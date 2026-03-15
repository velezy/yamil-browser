"""
ESL Auto-Mapper ML

ML-first approach to mapping JSON keys to HL7 segment.field positions.
Sends all keys + ESL field catalog directly to the LLM for mapping.
Falls back to deterministic fuzzy matching only if AI is unavailable.

All AI calls go through invoke_ai_with_phi_guard for HIPAA compliance.
"""

import json
import logging
from typing import Any, Optional
from uuid import uuid4

from assemblyline_common.ai import (
    invoke_ai_with_phi_guard,
    make_key,
    get_mapping_cache,
    get_feedback_store,
)
from assemblyline_common.hl7.esl_auto_mapper import ESLAutoMapper

logger = logging.getLogger(__name__)


MAPPER_SYSTEM_PROMPT = """You are an HL7 v2.x integration expert. Your task is to map JSON field names to HL7 segment.field positions.

You will receive:
1. A list of JSON keys from an input payload
2. A catalog of available HL7 segment fields from the ESL schema

For each JSON key, suggest the best matching HL7 segment.field position.

COMMON MAPPINGS (use these as reference):
- firstName, givenName → PID.5.2 (Given Name)
- lastName, familyName → PID.5.1 (Family Name)
- middleName, middleInitial → PID.5.3 (Middle Name)
- phone, phoneNumber, homePhone, mobilePhone → PID.13.1 (Phone Number - Home)
- workPhone, businessPhone → PID.14.1 (Phone Number - Business)
- email → PID.13.4 (Email in XTN)
- age → PID.7.1 or custom segment (no standard age field)
- gender, sex → PID.8 (Administrative Sex)
- dateOfBirth, dob, birthDate → PID.7.1 (Date/Time of Birth)
- address, streetAddress → PID.11.1 (Street Address)
- city → PID.11.3, state → PID.11.4, zip → PID.11.5
- mrn, patientId → PID.3.1 (Patient Identifier)
- ssn → PID.19 (SSN)

RULES:
- Use standard HL7 v2.x segment.field notation (e.g., PID.5, NK1.2, PV1.3)
- Map ALL keys - do not skip any field
- For phone numbers, ALWAYS map to PID.13 or PID.14
- Consider common healthcare data patterns and abbreviations
- If a key genuinely has no good HL7 mapping, set confidence to 0
- Provide a confidence score (0-1) for each mapping
- Include brief reasoning for each mapping

Respond in JSON format:
{
  "mappings": [
    {
      "inputKey": "the_json_key",
      "target": "PID.5",
      "fieldName": "Patient Name",
      "confidence": 0.85,
      "reasoning": "Brief explanation of why this mapping makes sense"
    }
  ]
}"""


REVERSE_MAPPER_SYSTEM_PROMPT = """You are an HL7 v2.x integration expert. Your task is to generate meaningful JSON key names for HL7 field positions.

You will receive:
1. A list of HL7 segment.field positions with their current values
2. The ESL field catalog showing field names for each position

For each HL7 field, suggest the best JSON key name (camelCase) that a developer would use to reference this data.

RULES:
- Use camelCase naming convention (e.g., patientLastName, admitDateTime)
- Keep names concise but descriptive
- Consider the field's data type and context within the segment
- For sub-components (e.g., PID.5.1), be specific (firstName vs lastName)
- Group related fields with common prefixes where logical (e.g., insurancePlanId, insuranceCompanyName)
- Provide a confidence score (0-1) for each naming suggestion
- Include brief reasoning for each name choice

Respond in JSON format:
{
  "mappings": [
    {
      "hl7Field": "PID.5.1",
      "jsonKey": "lastName",
      "fieldName": "Family Name",
      "confidence": 0.95,
      "reasoning": "PID.5 is Patient Name, component 1 is Family (Last) Name"
    }
  ]
}"""


class ESLAutoMapperML:
    """
    ML-first ESL auto-mapper.

    Sends all JSON keys directly to the LLM for mapping against the ESL
    field catalog. Falls back to deterministic fuzzy matching only when
    AI is unavailable.

    Usage:
        ml_mapper = ESLAutoMapperML(esl_source="epic", data_dir=DATA_DIR)
        result = await ml_mapper.map_payload(json_data=payload)
    """

    def __init__(self, esl_source: str = "epic", data_dir: str = None):
        self._base_mapper = ESLAutoMapper(esl_source=esl_source, data_dir=data_dir)
        self._cache = get_mapping_cache()
        self._esl_source = esl_source

    @property
    def base_mapper(self) -> ESLAutoMapper:
        return self._base_mapper

    async def map_payload(
        self,
        json_data: dict[str, Any],
        tenant_id: str = "default",
        ai_provider: str = "bedrock",
        secret_arn: str = None,
        credentials: dict = None,
        message_type: Optional[str] = None,
        skip_phi_masking: bool = False,
    ) -> dict[str, Any]:
        """
        Map JSON payload keys to HL7 fields using LLM.

        Sends all keys directly to the LLM. If AI is unavailable,
        falls back to deterministic fuzzy matching.

        Args:
            json_data: The JSON payload being mapped.
            tenant_id: Tenant ID for AI budget tracking.
            ai_provider: "bedrock" or "azure_openai".
            secret_arn: AWS Secrets Manager ARN for credential retrieval.
            credentials: Direct credentials dict (accessKeyId, secretAccessKey, region).
            message_type: Optional message type (e.g., 'ADT_A28') for structure-aware mapping.
            skip_phi_masking: Skip PHI masking (for HIPAA-compliant providers like AWS Bedrock).

        Returns:
            dict with:
                mappings: List of mapping results with confidence + reasoning
                mlAvailable: Whether ML was successfully used
                tokenUsage: AI token usage (if ML was invoked)
        """
        keys = list(json_data.keys())
        if not keys:
            return {"mappings": [], "mlAvailable": True, "tokenUsage": None}

        # Check cache (include message_type in cache key)
        cache_key = make_key("auto-map-ml", self._esl_source, sorted(keys), message_type or "")
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug(f"ML mapping cache hit for {len(keys)} keys")
            return {"mappings": cached, "mlAvailable": True, "tokenUsage": None}

        # Build field catalog for LLM context
        field_catalog = self._build_field_catalog()

        # Add message type context to field catalog if provided
        if message_type:
            field_catalog = f"Target Message Structure: {message_type}\nPrioritize segments relevant to this structure.\n\n{field_catalog}"

        # Invoke LLM with all keys
        ml_results = await self._invoke_llm(keys, field_catalog, tenant_id, ai_provider, secret_arn, credentials, skip_phi_masking)

        if ml_results["success"]:
            mappings = ml_results["mappings"]
            self._cache.set(cache_key, mappings)
            return {
                "mappings": mappings,
                "mlAvailable": True,
                "tokenUsage": ml_results.get("token_usage"),
            }

        # Fallback: deterministic fuzzy matching
        logger.info(f"AI unavailable ({ml_results.get('error')}), falling back to deterministic")
        fallback = self._base_mapper.get_mapping_preview(json_data)
        for item in fallback:
            item["source"] = "deterministic"
        return {
            "mappings": fallback,
            "mlAvailable": False,
            "tokenUsage": None,
            "mlError": ml_results.get("error"),
        }

    # Keep legacy method name for backward compatibility with endpoint
    async def enhance_mapping_preview(
        self,
        json_data: dict[str, Any],
        base_results: Optional[list[dict]] = None,
        confidence_threshold: float = 0.8,
        tenant_id: str = "default",
        ai_provider: str = "bedrock",
        secret_arn: str = None,
        credentials: dict = None,
    ) -> dict[str, Any]:
        """Legacy method - now delegates to map_payload (ML-first)."""
        return await self.map_payload(json_data, tenant_id, ai_provider, secret_arn, credentials)

    async def _invoke_llm(
        self,
        keys: list[str],
        field_catalog: list[dict],
        tenant_id: str,
        ai_provider: str,
        secret_arn: str = None,
        credentials: dict = None,
        skip_phi_masking: bool = False,
    ) -> dict[str, Any]:
        """Send all keys + field catalog to LLM for mapping."""
        conversation_id = f"auto-map-ml-{uuid4().hex[:8]}"
        user_content = self._build_prompt(keys, field_catalog, tenant_id)

        result = await invoke_ai_with_phi_guard(
            content=user_content,
            system_prompt=MAPPER_SYSTEM_PROMPT,
            conversation_id=conversation_id,
            tenant_id=tenant_id,
            temperature=0.3,
            max_tokens=4096,
            ai_provider=ai_provider,
            secret_arn=secret_arn,
            credentials=credentials,
            skip_phi_masking=skip_phi_masking,
        )

        if not result["success"]:
            return {"success": False, "mappings": [], "error": result["error"]}

        try:
            parsed = self._parse_llm_response(result["response"])
            return {
                "success": True,
                "mappings": parsed,
                "token_usage": result["token_usage"],
            }
        except Exception as e:
            logger.warning(f"Failed to parse LLM mapping response: {e}")
            return {"success": False, "mappings": [], "error": f"parse_error: {e}"}

    def _build_prompt(self, keys: list[str], field_catalog: list[dict], tenant_id: str = "default") -> str:
        """Build the user prompt for LLM mapping, including feedback context."""
        catalog_str = json.dumps(field_catalog[:200], indent=1)
        if len(catalog_str) > 8000:
            catalog_str = catalog_str[:8000] + "\n... (truncated)"

        # Include confirmed feedback as few-shot examples
        feedback_section = ""
        store = get_feedback_store()
        confirmed = store.get_confirmed_mappings(tenant_id, "auto-map", self._esl_source)
        rejected = store.get_rejected_mappings(tenant_id, "auto-map", self._esl_source)
        if confirmed or rejected:
            feedback_section = "\n\nPreviously confirmed mappings for this tenant:\n"
            for m in confirmed[:20]:
                feedback_section += f'- "{m["input_key"]}" → {m["target"]} (confirmed)\n'
            for m in rejected[:10]:
                feedback_section += f'- "{m["input_key"]}" → {m["target"]} (rejected - do NOT use)\n'

        return f"""Map these JSON keys to HL7 segment.field positions:

JSON Keys to Map:
{json.dumps(keys, indent=2)}

Available HL7 Fields (ESL Catalog):
{catalog_str}
{feedback_section}
Provide your best mapping suggestions in JSON format."""

    def _build_field_catalog(self) -> list[dict]:
        """Build a catalog of available ESL fields for LLM context."""
        catalog = []
        all_segments = self._base_mapper.schema_manager.get_all_segments(
            self._esl_source
        )

        for seg_id, segment in all_segments.items():
            for i, field in enumerate(segment.fields):
                catalog.append({
                    "position": f"{seg_id}.{i + 1}",
                    "name": field.name,
                    "type": field.data_type,
                    "segment": seg_id,
                })

        return catalog

    def _parse_llm_response(self, response_text: str) -> list[dict]:
        """Parse the LLM JSON response into mapping entries."""
        text = response_text.strip()

        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        # Try direct parse first
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # LLM may return JSON followed by extra text; find the JSON boundary
            start = -1
            brace = None
            for i, ch in enumerate(text):
                if ch == '{':
                    start = i
                    brace = '}'
                    break
                elif ch == '[':
                    start = i
                    brace = ']'
                    break

            if start >= 0 and brace:
                depth = 0
                open_ch = text[start]
                for i in range(start, len(text)):
                    if text[i] == open_ch:
                        depth += 1
                    elif text[i] == brace:
                        depth -= 1
                        if depth == 0:
                            parsed = json.loads(text[start:i + 1])
                            break
                else:
                    raise ValueError(f"Could not find matching {brace} in LLM response")
            else:
                raise ValueError("No JSON object or array found in LLM response")

        if isinstance(parsed, dict):
            mappings = parsed.get("mappings", [])
        elif isinstance(parsed, list):
            mappings = parsed
        else:
            mappings = []

        result = []
        for m in mappings:
            if not isinstance(m, dict):
                continue
            result.append({
                "inputKey": m.get("inputKey", ""),
                "target": m.get("target", "?"),
                "fieldName": m.get("fieldName", ""),
                "confidence": min(float(m.get("confidence", 0)), 1.0),
                "source": "ml",
                "reasoning": m.get("reasoning", ""),
            })

        return result

    async def reverse_map_payload(
        self,
        hl7_fields: list[dict],
        tenant_id: str = "default",
        ai_provider: str = "bedrock",
        secret_arn: str = None,
        credentials: dict = None,
    ) -> dict[str, Any]:
        """
        Reverse-map HL7 field positions to meaningful JSON key names using LLM.

        Takes a list of parsed HL7 fields (with positions and values) and uses
        the LLM to generate semantic JSON key names for each field.

        Args:
            hl7_fields: List of dicts with 'hl7Field' and 'value' keys.
            tenant_id: Tenant ID for AI budget tracking.
            ai_provider: "bedrock" or "azure_openai".
            secret_arn: AWS Secrets Manager ARN.
            credentials: Direct credentials dict.

        Returns:
            dict with mappings, mlAvailable, tokenUsage.
        """
        if not hl7_fields:
            return {"mappings": [], "mlAvailable": True, "tokenUsage": None}

        # Check cache
        field_keys = sorted([f["hl7Field"] for f in hl7_fields])
        cache_key = make_key("reverse-map-ml", self._esl_source, field_keys)
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Reverse ML mapping cache hit for {len(hl7_fields)} fields")
            return {"mappings": cached, "mlAvailable": True, "tokenUsage": None}

        # Build field catalog for context
        field_catalog = self._build_field_catalog()

        # Build reverse prompt
        prompt = self._build_reverse_prompt(hl7_fields, field_catalog)

        # Invoke LLM
        conversation_id = f"reverse-map-ml-{uuid4().hex[:8]}"
        result = await invoke_ai_with_phi_guard(
            content=prompt,
            system_prompt=REVERSE_MAPPER_SYSTEM_PROMPT,
            conversation_id=conversation_id,
            tenant_id=tenant_id,
            temperature=0.3,
            max_tokens=4096,
            ai_provider=ai_provider,
            secret_arn=secret_arn,
            credentials=credentials,
        )

        if not result["success"]:
            logger.info(f"AI unavailable for reverse map ({result.get('error')}), falling back to deterministic")
            return {
                "mappings": [],
                "mlAvailable": False,
                "tokenUsage": None,
                "mlError": result.get("error"),
            }

        try:
            parsed = self._parse_reverse_llm_response(result["response"])
            self._cache.set(cache_key, parsed)
            return {
                "mappings": parsed,
                "mlAvailable": True,
                "tokenUsage": result.get("token_usage"),
            }
        except Exception as e:
            logger.warning(f"Failed to parse reverse LLM response: {e}")
            return {
                "mappings": [],
                "mlAvailable": False,
                "tokenUsage": None,
                "mlError": f"parse_error: {e}",
            }

    def _build_reverse_prompt(self, hl7_fields: list[dict], field_catalog: list[dict]) -> str:
        """Build the user prompt for reverse LLM mapping."""
        # Truncate values for PHI safety (only send field structure, not full values)
        fields_for_prompt = []
        for f in hl7_fields[:100]:  # Limit to 100 fields
            fields_for_prompt.append({
                "hl7Field": f["hl7Field"],
                "sampleLength": len(str(f.get("value", ""))),
            })

        catalog_str = json.dumps(field_catalog[:200], indent=1)
        if len(catalog_str) > 8000:
            catalog_str = catalog_str[:8000] + "\n... (truncated)"

        # Include feedback context
        feedback_section = ""
        store = get_feedback_store()
        confirmed = store.get_confirmed_mappings(tenant_id="default", feature="reverse-map", context=self._esl_source)
        if confirmed:
            feedback_section = "\n\nPreviously confirmed reverse mappings:\n"
            for m in confirmed[:20]:
                feedback_section += f'- {m["input_key"]} → "{m["target"]}" (confirmed)\n'

        return f"""Generate meaningful JSON key names for these HL7 field positions:

HL7 Fields to Name:
{json.dumps(fields_for_prompt, indent=2)}

Available HL7 Fields (ESL Catalog - use field names as context):
{catalog_str}
{feedback_section}
Provide your best JSON key name suggestions in JSON format."""

    def _parse_reverse_llm_response(self, response_text: str) -> list[dict]:
        """Parse the reverse LLM response into mapping entries."""
        text = response_text.strip()

        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        # Try direct parse first
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            # LLM may return JSON followed by extra text; find the JSON boundary
            # Look for the outermost { ... } or [ ... ]
            start = -1
            brace = None
            for i, ch in enumerate(text):
                if ch == '{':
                    start = i
                    brace = '}'
                    break
                elif ch == '[':
                    start = i
                    brace = ']'
                    break

            if start >= 0 and brace:
                # Find matching close by counting nesting
                depth = 0
                open_ch = text[start]
                for i in range(start, len(text)):
                    if text[i] == open_ch:
                        depth += 1
                    elif text[i] == brace:
                        depth -= 1
                        if depth == 0:
                            parsed = json.loads(text[start:i + 1])
                            break
                else:
                    raise ValueError(f"Could not find matching {brace} in LLM response")
            else:
                raise ValueError("No JSON object or array found in LLM response")

        if isinstance(parsed, dict):
            mappings = parsed.get("mappings", [])
        elif isinstance(parsed, list):
            mappings = parsed
        else:
            mappings = []

        result = []
        for m in mappings:
            if not isinstance(m, dict):
                continue
            result.append({
                "hl7Field": m.get("hl7Field", ""),
                "jsonKey": m.get("jsonKey", ""),
                "fieldName": m.get("fieldName", ""),
                "confidence": min(float(m.get("confidence", 0)), 1.0),
                "source": "ml",
                "reasoning": m.get("reasoning", ""),
            })

        return result

    def reverse_map_deterministic(self, hl7_fields: list[dict]) -> list[dict]:
        """
        Deterministic reverse mapping using ESL schema field names.

        Looks up each HL7 field position in the ESL schema and generates
        a camelCase JSON key from the field name. No AI call needed.

        Args:
            hl7_fields: List of dicts with 'hl7Field' and 'value' keys.

        Returns:
            List of mapping dicts with hl7Field, jsonKey, confidence, source, reasoning.
        """
        result = []
        for field in hl7_fields:
            hl7_pos = field["hl7Field"]
            parts = hl7_pos.split(".")
            seg_id = parts[0]

            # Look up field name from ESL schema
            field_name = ""
            if len(parts) >= 2:
                try:
                    position = int(parts[1])
                except ValueError:
                    position = 0

                if seg_id in self._base_mapper._field_index:
                    for finfo in self._base_mapper._field_index[seg_id]:
                        if finfo["position"] == position:
                            field_name = finfo["name"]
                            break

            if field_name:
                # Generate camelCase key from ESL field name
                json_key = self._field_name_to_camel(field_name, seg_id, parts)
                # Confidence based on match quality:
                # - Sub-component with known data type pattern: 0.95 (exact naming)
                # - Base field or unknown sub-component: 0.90 (field identified, naming inferred)
                has_subcomponent = len(parts) >= 3
                if has_subcomponent:
                    # Check if we matched a known data type pattern
                    dt = ""
                    if seg_id in self._base_mapper._field_index:
                        for finfo in self._base_mapper._field_index[seg_id]:
                            if finfo["position"] == int(parts[1]):
                                dt = finfo.get("type", "")
                                break
                    known_types = {"XPN", "XAD", "XTN", "CX", "XCN"}
                    if dt in known_types:
                        confidence = 0.95
                        reasoning = f"ESL Schema: {field_name} ({dt}.{parts[2]})"
                    else:
                        confidence = 0.88
                        reasoning = f"ESL Schema: {field_name} (component {parts[2]})"
                else:
                    confidence = 0.90
                    reasoning = f"ESL Schema: {field_name}"
                source = "esl"
            else:
                # Fallback for unknown fields
                json_key = f"{seg_id.lower()}_{'_'.join(parts[1:])}"
                confidence = 0.4
                source = "generic"
                reasoning = "Field not found in ESL schema"

            result.append({
                "hl7Field": hl7_pos,
                "jsonKey": json_key,
                "value": field.get("value", ""),
                "confidence": confidence,
                "source": source,
                "reasoning": reasoning,
            })

        return result

    def _field_name_to_camel(self, field_name: str, seg_id: str, parts: list[str]) -> str:
        """Convert an ESL field name to a camelCase JSON key."""
        import re
        # Clean up the field name
        name = field_name.strip()

        # Handle sub-components: append component context
        if len(parts) >= 3:
            comp_idx = int(parts[2]) if parts[2].isdigit() else 0
            # Common component patterns
            comp_names = {
                "XPN": {1: "lastName", 2: "firstName", 3: "middleName", 4: "suffix", 5: "prefix"},
                "XAD": {1: "street", 2: "otherDesignation", 3: "city", 4: "state", 5: "zip", 6: "country"},
                "XTN": {1: "number", 2: "useCode", 3: "equipmentType"},
                "CX": {1: "id", 2: "checkDigit", 3: "codeId", 4: "assigningAuthority"},
                "XCN": {1: "id", 2: "lastName", 3: "firstName"},
            }
            # Try to find the field's data type for component naming
            if seg_id in self._base_mapper._field_index:
                for finfo in self._base_mapper._field_index[seg_id]:
                    if finfo["position"] == int(parts[1]):
                        dt = finfo.get("type", "")
                        if dt in comp_names and comp_idx in comp_names[dt]:
                            # Use segment-prefixed component name for clarity
                            base = re.sub(r'[^a-zA-Z0-9]', ' ', name).strip()
                            words = base.split()
                            comp_word = comp_names[dt][comp_idx]
                            # Build: segmentFieldComponent
                            camel = words[0].lower() + ''.join(w.capitalize() for w in words[1:]) + comp_word.capitalize()
                            return camel[:40]
                        break

        # Convert to camelCase: split on non-alphanumeric, join
        words = re.sub(r'[^a-zA-Z0-9]', ' ', name).split()
        if not words:
            return f"{seg_id.lower()}_{'_'.join(parts[1:])}"

        camel = words[0].lower() + ''.join(w.capitalize() for w in words[1:])
        return camel[:40]  # Limit length

    def map_deterministic(self, json_data: dict[str, Any]) -> list[dict]:
        """
        Deterministic forward mapping using ESL schema field names.

        Matches JSON keys to HL7 field positions by fuzzy-matching key names
        against ESL field names. No AI call needed.

        Args:
            json_data: The JSON payload to map.

        Returns:
            List of mapping dicts with inputKey, target, confidence, source, reasoning.
        """
        import re
        keys = list(json_data.keys())
        result = []

        # Build lookup: normalized field name → (position, original name, type)
        field_lookup = []
        for seg_id, fields in self._base_mapper._field_index.items():
            for finfo in fields:
                pos = finfo["position"]
                name = finfo["name"]
                dt = finfo.get("type", "")
                # Normalize: lowercase, strip non-alphanumeric
                norm = re.sub(r'[^a-z0-9]', '', name.lower())
                field_lookup.append({
                    "position": f"{seg_id}.{pos}",
                    "name": name,
                    "norm": norm,
                    "type": dt,
                    "seg": seg_id,
                })

        for key in keys:
            # Normalize key for matching
            key_norm = re.sub(r'[^a-z0-9]', '', key.lower())

            best_match = None
            best_score = 0.0

            for field in field_lookup:
                # Exact normalized match
                if key_norm == field["norm"]:
                    best_match = field
                    best_score = 0.95
                    break

                # Check if key contains field name or vice versa
                if len(key_norm) >= 3 and len(field["norm"]) >= 3:
                    if key_norm in field["norm"] or field["norm"] in key_norm:
                        score = min(len(key_norm), len(field["norm"])) / max(len(key_norm), len(field["norm"]))
                        if score > best_score:
                            best_score = score * 0.90  # Scale to max 0.90
                            best_match = field

            if best_match and best_score >= 0.50:
                result.append({
                    "inputKey": key,
                    "target": best_match["position"],
                    "fieldName": best_match["name"],
                    "confidence": round(best_score, 2),
                    "source": "esl",
                    "reasoning": f"ESL Schema: {best_match['name']}",
                })
            else:
                result.append({
                    "inputKey": key,
                    "target": "?",
                    "fieldName": "",
                    "confidence": 0.0,
                    "source": "generic",
                    "reasoning": "No matching ESL field found",
                })

        return result

"""
HL7 Smart Extractor - ML-Powered Extraction Configuration Suggestions

Analyzes HL7 message structure (without PHI) and uses LLM to suggest
optimal extraction configurations based on the use case.

Key features:
- Parses message to extract structure (segment names, field positions, data types)
- Creates PHI-free structural summary for LLM analysis
- LLM suggests extraction config based on structure + use case
- Returns recommended segments, output format, and relevant fields
"""

import json
import logging
import re
from typing import Any, Optional
from uuid import uuid4

from assemblyline_common.ai import (
    invoke_ai_with_phi_guard,
    make_key,
    get_suggestion_cache,
    get_feedback_store,
    get_ignore_list_store,
)
from assemblyline_common.hl7.esl_auto_mapper import ESLAutoMapper
from assemblyline_common.hl7.parser import HL7Parser, HL7Message

logger = logging.getLogger(__name__)


SMART_EXTRACT_SYSTEM_PROMPT = """You are an HL7 v2.x integration expert specializing in message parsing and data extraction.

You will receive:
1. A structural summary of an HL7 message (segment types, field counts, data types - NO actual PHI values)
2. An optional use case description (e.g., "patient registration to Salesforce")

Your task is to recommend the optimal extraction configuration:
- Which segments to extract
- What output format to use (json_flat, json_nested, key_value)
- Which specific fields are most relevant for the use case
- Clinical reasoning for your recommendations

RULES:
- Never include or reference actual patient data
- Focus on structural analysis only
- Consider common integration patterns
- Recommend the most efficient extraction for the stated use case
- If no use case is provided, suggest a general-purpose extraction

Respond in JSON format:
{
  "segments": ["PID", "PV1", "OBR"],
  "outputFormat": "json_nested",
  "relevantFields": [
    {
      "segment": "PID",
      "field": 5,
      "name": "Patient Name",
      "reason": "Required for patient matching in target system"
    }
  ],
  "reasoning": "Brief explanation of the overall extraction strategy",
  "confidence": 0.85
}"""


# Segment purpose dictionary for fuzzy matching relevance scoring
# Maps segment IDs to their clinical purpose keywords
SEGMENT_PURPOSE: dict[str, list[str]] = {
    "MSH": ["message", "header", "routing", "metadata", "version", "type"],
    "EVN": ["event", "trigger", "admission", "discharge", "transfer"],
    "PID": ["patient", "demographics", "name", "dob", "gender", "address", "identifier", "mrn", "ssn"],
    "PD1": ["patient", "additional", "demographics", "primary", "care", "provider", "living", "will"],
    "NK1": ["next", "of", "kin", "contact", "emergency", "relationship", "family"],
    "PV1": ["visit", "encounter", "admission", "location", "attending", "physician", "bed", "ward", "class"],
    "PV2": ["visit", "additional", "expected", "admit", "reason", "accommodation"],
    "IN1": ["insurance", "plan", "coverage", "policy", "group", "payer", "subscriber"],
    "IN2": ["insurance", "additional", "employer", "guarantor", "eligibility"],
    "GT1": ["guarantor", "billing", "financial", "responsibility"],
    "AL1": ["allergy", "allergen", "reaction", "severity", "clinical"],
    "DG1": ["diagnosis", "icd", "code", "clinical", "condition", "problem"],
    "PR1": ["procedure", "surgery", "intervention", "cpt"],
    "OBR": ["observation", "request", "order", "lab", "test", "radiology"],
    "OBX": ["observation", "result", "value", "lab", "vital", "sign", "measurement"],
    "ORC": ["order", "common", "control", "status", "placer", "filler"],
    "RXA": ["pharmacy", "administration", "vaccine", "immunization", "drug"],
    "RXE": ["pharmacy", "encoded", "order", "medication", "prescription"],
    "RXR": ["pharmacy", "route", "administration", "site"],
    "FT1": ["financial", "transaction", "charge", "billing", "cost"],
    "ACC": ["accident", "injury", "cause"],
    "UB1": ["uniform", "billing", "claim", "ub04"],
    "UB2": ["uniform", "billing", "additional"],
    "MRG": ["merge", "patient", "prior", "identifier"],
    "ZPD": ["epic", "custom", "extension", "mychart", "portal"],
    "SCH": ["scheduling", "appointment", "booking"],
    "AIS": ["appointment", "service", "resource"],
    "AIG": ["appointment", "group", "resource"],
    "AIL": ["appointment", "location"],
    "AIP": ["appointment", "personnel", "provider"],
    "TXA": ["document", "transcription", "report"],
    "ROL": ["role", "provider", "care", "team"],
}

# Use-case keyword mappings for relevance boosting
USE_CASE_KEYWORDS: dict[str, list[str]] = {
    "registration": ["PID", "PD1", "NK1", "IN1", "GT1"],
    "admission": ["PID", "PV1", "PV2", "EVN", "DG1"],
    "discharge": ["PID", "PV1", "PV2", "DG1", "PR1"],
    "transfer": ["PID", "PV1", "PV2", "EVN"],
    "billing": ["PID", "IN1", "IN2", "GT1", "FT1", "UB1", "UB2"],
    "insurance": ["PID", "IN1", "IN2", "GT1"],
    "clinical": ["PID", "PV1", "DG1", "OBR", "OBX", "AL1"],
    "lab": ["PID", "OBR", "OBX", "ORC"],
    "pharmacy": ["PID", "ORC", "RXA", "RXE", "RXR"],
    "allergy": ["PID", "AL1"],
    "diagnosis": ["PID", "DG1"],
    "scheduling": ["PID", "SCH", "AIS", "AIG", "AIL", "AIP"],
    "referral": ["PID", "PV1", "DG1", "ROL"],
    "salesforce": ["PID", "PV1", "NK1", "IN1", "DG1"],
    "fhir": ["PID", "PV1", "DG1", "AL1", "OBR", "OBX"],
    "adt": ["PID", "PV1", "PV2", "EVN", "NK1", "DG1"],
    "oru": ["PID", "OBR", "OBX", "ORC"],
    "orm": ["PID", "ORC", "OBR"],
    "immunization": ["PID", "RXA", "RXR", "OBX"],
    "vaccine": ["PID", "RXA", "RXR", "OBX"],
    "epic": ["PID", "PV1", "ZPD", "IN1"],
    "portal": ["PID", "ZPD", "PD1"],
    "mychart": ["PID", "ZPD"],
}

# NOTE: Feedback threshold removed — feedback signals are now integrated
# directly into the ESL ML tier rather than acting as a standalone gate.
# The _try_feedback_suggestion() method is retained for potential future use.


def _tokenize_for_matching(text: str) -> list[str]:
    """Tokenize text for fuzzy matching (same pattern as ESLAutoMapper)."""
    if not text:
        return []
    tokens = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    tokens = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', tokens)
    tokens = re.sub(r'[_\-/\s]+', ' ', tokens)
    tokens = re.sub(r"[^a-zA-Z0-9\s]", "", tokens)
    return [t.lower() for t in tokens.split() if t]


def _score_segment_relevance(
    segment_id: str,
    segment_fields: list[dict],
    use_case_tokens: list[str],
) -> float:
    """
    Score how relevant a segment is based on its purpose and field names
    fuzzy-matched against the use case.
    """
    score = 0.0

    # Base score from segment purpose
    purpose_tokens = SEGMENT_PURPOSE.get(segment_id, [])
    if purpose_tokens and use_case_tokens:
        overlap = len(set(use_case_tokens) & set(purpose_tokens))
        if overlap > 0:
            score += min(overlap * 0.2, 0.6)

    # Score from field names matching use case
    for field_info in segment_fields[:10]:
        field_tokens = _tokenize_for_matching(field_info.get("name", ""))
        if field_tokens and use_case_tokens:
            overlap = len(set(use_case_tokens) & set(field_tokens))
            if overlap > 0:
                score += min(overlap * 0.05, 0.15)

    # Bonus for having actual data
    fields_with_data = sum(1 for f in segment_fields if f.get("hasValue"))
    if fields_with_data > 0:
        score += min(fields_with_data * 0.02, 0.2)

    return min(score, 1.0)


class HL7SmartExtractor:
    """
    ML-powered HL7 extraction configuration suggester.

    Analyzes HL7 message structure and recommends extraction settings
    without exposing PHI to the LLM.

    Usage:
        extractor = HL7SmartExtractor()
        suggestion = await extractor.analyze_message(
            hl7_message="MSH|^~\\&|...",
            use_case_hint="patient demographics to Epic"
        )
    """

    def __init__(self):
        self._cache = get_suggestion_cache()
        self._parser = HL7Parser

    async def analyze_message(
        self,
        hl7_message: str,
        use_case_hint: Optional[str] = None,
        esl_source: str = "epic",
        tenant_id: str = "default",
        ai_provider: str = "bedrock",
        data_dir: Optional[str] = None,
        secret_arn: Optional[str] = None,
        credentials: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Analyze an HL7 message and suggest extraction configuration.

        Args:
            hl7_message: Raw HL7 v2.x message string.
            use_case_hint: Optional use case description.
            esl_source: ESL source for field name resolution.
            tenant_id: Tenant ID for budget tracking.
            data_dir: Path to ESL data directory (for schema loading).
            secret_arn: AWS Secrets Manager ARN for Bedrock credentials.
            credentials: Direct credentials dict fallback (when no secret_arn).

        Returns:
            dict with suggestion (segments, outputFormat, relevantFields,
            reasoning, confidence) or error info.
        """
        # Parse the message to get structure
        try:
            parsed = self._parser.parse(hl7_message)
        except Exception as e:
            logger.warning(f"Failed to parse HL7 message: {e}")
            return {
                "success": False,
                "error": f"parse_error: {e}",
                "suggestion": None,
            }

        # Create PHI-free structural summary
        structure_summary = self._summarize_message_structure(parsed, hl7_message)

        # Check cache
        cache_key = make_key(
            "smart-extract",
            structure_summary["messageType"],
            sorted(structure_summary["segments"]),
            use_case_hint or "",
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return {"success": True, "suggestion": cached, "cached": True, "source": cached.get("_source", "bedrock")}

        # Tier 1: ESL ML — always run schema-aware scoring first
        esl_result = self._try_esl_suggestion(structure_summary, use_case_hint, esl_source, tenant_id, data_dir)

        # If AI is off (ai_provider == "none") or ESL is the only tier needed, return ESL result
        if ai_provider == "none" or not ai_provider:
            if esl_result is not None:
                self._cache.set(cache_key, esl_result)
                return {"success": True, "suggestion": esl_result, "source": "esl_ml", "mlAvailable": False}
            # Fallback if ESL also fails
            fallback = self._get_fallback_suggestion(structure_summary, use_case_hint, tenant_id)
            return {"success": True, "suggestion": fallback, "source": "fallback", "mlAvailable": False}

        # Tier 2: Bedrock LLM — refine ESL ML results with semantic understanding
        # Include ESL ML scoring context so LLM can validate/enhance
        masked_content = self._mask_hl7_for_llm(structure_summary, use_case_hint, tenant_id)
        if esl_result:
            esl_context = (
                f"\nESL Schema Analysis (pre-scored):\n"
                f"  Ranked segments: {', '.join(esl_result.get('segments', []))}\n"
                f"  ESL confidence: {esl_result.get('confidence', 0):.0%}\n"
                f"  Top fields: {', '.join(str(f.get('field', '')) for f in esl_result.get('relevantFields', [])[:8])}\n"
                f"\nPlease validate and refine this ranking based on the use case. "
                f"Re-order segments if the use case suggests different priorities.\n"
            )
            masked_content += esl_context

        # Invoke LLM
        conversation_id = f"smart-extract-{uuid4().hex[:8]}"
        result = await invoke_ai_with_phi_guard(
            content=masked_content,
            system_prompt=SMART_EXTRACT_SYSTEM_PROMPT,
            conversation_id=conversation_id,
            tenant_id=tenant_id,
            temperature=0.5,
            max_tokens=2048,
            ai_provider=ai_provider,
            secret_arn=secret_arn,
            credentials=credentials,
        )

        if not result["success"]:
            # Bedrock failed — return ESL ML result if available, otherwise fallback
            if esl_result is not None:
                self._cache.set(cache_key, esl_result)
                return {"success": True, "suggestion": esl_result, "source": "esl_ml", "mlAvailable": False}
            fallback = self._get_fallback_suggestion(structure_summary, use_case_hint, tenant_id)
            return {
                "success": False,
                "error": result["error"],
                "suggestion": fallback,
                "source": "fallback",
                "mlAvailable": False,
            }

        # Parse LLM response and compare with ESL ML
        try:
            bedrock_suggestion = self._parse_suggestion(result["response"])
            bedrock_confidence = bedrock_suggestion.get("confidence", 0.0)
            esl_confidence = esl_result.get("confidence", 0.0) if esl_result else 0.0

            # Take the result with higher confidence
            if bedrock_confidence >= esl_confidence:
                bedrock_suggestion["_source"] = "bedrock+esl_ml"
                self._cache.set(cache_key, bedrock_suggestion)
                return {
                    "success": True,
                    "suggestion": bedrock_suggestion,
                    "source": "bedrock+esl_ml",
                    "tokenUsage": result["token_usage"],
                    "mlAvailable": True,
                }
            else:
                # ESL ML had higher confidence — use it but note Bedrock validated
                esl_result["_source"] = "esl_ml+bedrock"
                self._cache.set(cache_key, esl_result)
                return {
                    "success": True,
                    "suggestion": esl_result,
                    "source": "esl_ml+bedrock",
                    "tokenUsage": result["token_usage"],
                    "mlAvailable": True,
                }
        except Exception as e:
            logger.warning(f"Failed to parse Bedrock suggestion: {e}")
            # Bedrock parse failed — return ESL ML result
            if esl_result is not None:
                self._cache.set(cache_key, esl_result)
                return {"success": True, "suggestion": esl_result, "source": "esl_ml", "mlAvailable": True}
            fallback = self._get_fallback_suggestion(structure_summary, use_case_hint, tenant_id)
            return {
                "success": False,
                "error": f"parse_error: {e}",
                "suggestion": fallback,
                "source": "fallback",
                "mlAvailable": False,
            }

    def _try_esl_suggestion(
        self,
        structure_summary: dict[str, Any],
        use_case_hint: Optional[str],
        esl_source: str,
        tenant_id: str,
        data_dir: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Tier 1: ESL schema-aware segment scoring.

        Uses the ESL field catalog to intelligently score segments based on:
        - Mandatory field density (fields with usage='M' that have data)
        - ESL field name fuzzy matching against use case tokens
        - Data type richness (complex clinical types like XPN, XAD, CX, CE)
        - Data presence (fields with actual values)
        - Use-case keyword boosting (from USE_CASE_KEYWORDS)
        - Feedback confirmation boost (no threshold gate)

        Returns:
            A suggestion dict, or None if ESL schema cannot be loaded.
        """
        segments = structure_summary["segments"]
        segment_details = structure_summary["segmentDetails"]
        message_type = structure_summary.get("messageType", "unknown")

        # Load ESL schema
        try:
            mapper = ESLAutoMapper(esl_source=esl_source, data_dir=data_dir)
            all_esl_segments = mapper.schema_manager.get_all_segments(esl_source)
        except Exception as e:
            logger.warning(f"ESL schema unavailable for source '{esl_source}': {e}")
            return None

        if not all_esl_segments:
            logger.warning(f"No ESL segments found for source '{esl_source}'")
            return None

        # Tokenize use case
        use_case_tokens = _tokenize_for_matching(use_case_hint) if use_case_hint else []

        # Use-case keyword boosting
        boosted_segments: set[str] = set()
        if use_case_tokens:
            for keyword, boost_segs in USE_CASE_KEYWORDS.items():
                if keyword in " ".join(use_case_tokens):
                    boosted_segments.update(boost_segs)

        # Get feedback signals (no threshold required)
        store = get_feedback_store()
        confirmed = store.get_confirmed_mappings(tenant_id, "smart-extract", message_type)
        confirmed_set = {m["input_key"] for m in confirmed}

        # Get ignore list
        ignore_store = get_ignore_list_store()
        ignored = ignore_store.get_ignored(tenant_id, message_type)
        ignored_segments = set(ignored["segments"])

        # Score each segment using ESL field catalog
        segment_scores: list[tuple[str, float]] = []
        for seg_id in segments:
            if seg_id in ignored_segments:
                continue

            seg_detail = next((s for s in segment_details if s["id"] == seg_id), None)
            msg_fields = seg_detail.get("fields", []) if seg_detail else []

            # Get ESL definition for this segment
            esl_seg = all_esl_segments.get(seg_id)
            esl_fields = esl_seg.fields if esl_seg else []

            score = 0.0

            # 1. Mandatory/Required field density (segments with more M/R fields + data = more important)
            required_with_data = sum(
                1 for i, f in enumerate(esl_fields)
                if f.usage in ('M', 'R') and i < len(msg_fields) and msg_fields[i].get("hasValue")
            )
            score += min(required_with_data * 0.1, 0.3)

            # 1b. Base clinical priority (core segments are inherently important)
            base_priority = {"PID": 0.2, "PV1": 0.15, "MSH": 0.1, "OBR": 0.1, "OBX": 0.1, "IN1": 0.1}
            score += base_priority.get(seg_id, 0.0)

            # 2. ESL field name matching against use case
            if use_case_tokens:
                for f in esl_fields[:15]:
                    field_tokens = _tokenize_for_matching(f.name)
                    overlap = len(set(use_case_tokens) & set(field_tokens))
                    if overlap > 0:
                        score += min(overlap * 0.06, 0.15)

            # 3. Data type richness (complex clinical types)
            complex_types = {"XPN", "XAD", "XTN", "XCN", "XON", "CE", "CWE", "CX", "PL"}
            complex_count = sum(1 for f in esl_fields if f.data_type in complex_types)
            score += min(complex_count * 0.03, 0.15)

            # 4. Data presence (fields that actually have values)
            fields_with_data = sum(1 for f in msg_fields if f.get("hasValue"))
            score += min(fields_with_data * 0.015, 0.2)

            # 5. Use-case keyword boosting
            if seg_id in boosted_segments:
                score += 0.25

            # 6. Feedback confirmation boost
            if seg_id in confirmed_set:
                score += 0.2

            segment_scores.append((seg_id, min(score, 1.0)))

        if not segment_scores:
            return None

        segment_scores.sort(key=lambda x: x[1], reverse=True)
        recommended = [s[0] for s in segment_scores]

        # Build relevant fields from ESL definitions (top 5 segments)
        relevant_fields = []
        for seg_id, seg_score in segment_scores[:5]:
            seg_detail = next((s for s in segment_details if s["id"] == seg_id), None)
            if not seg_detail:
                continue
            esl_seg = all_esl_segments.get(seg_id)
            esl_fields = esl_seg.fields if esl_seg else []
            msg_fields = seg_detail.get("fields", [])

            for i, field_info in enumerate(msg_fields[:5]):
                if not field_info.get("hasValue"):
                    continue
                esl_name = esl_fields[i].name if i < len(esl_fields) else field_info.get("name", "")
                relevant_fields.append({
                    "segment": seg_id,
                    "field": field_info["position"],
                    "name": esl_name,
                    "reason": f"ESL schema match (score: {seg_score:.0%})",
                })

        top_score = segment_scores[0][1]
        confidence = min(0.5 + top_score * 0.45, 0.90)

        reasoning_parts = [f"ESL schema analysis for {message_type}"]
        if use_case_hint:
            reasoning_parts.append(f'matched against "{use_case_hint}"')
        if confirmed_set:
            reasoning_parts.append(f"with {len(confirmed_set)} confirmed preferences")

        return {
            "segments": recommended,
            "outputFormat": "json_nested",
            "relevantFields": relevant_fields[:15],
            "reasoning": " ".join(reasoning_parts),
            "confidence": round(confidence, 2),
            "_source": "esl_ml",
        }

    def _try_feedback_suggestion(
        self,
        structure_summary: dict[str, Any],
        use_case_hint: Optional[str],
        tenant_id: str,
    ) -> Optional[dict[str, Any]]:
        """
        Tier 1: Try to build a suggestion purely from confirmed feedback.

        If the feedback store has >= MIN_FEEDBACK_THRESHOLD confirmed segment
        preferences for this tenant + message type, build a suggestion without
        calling Bedrock.

        Returns:
            A suggestion dict if enough feedback exists, or None to proceed to Bedrock.
        """
        message_type = structure_summary.get("messageType", "unknown")
        available_segments = set(structure_summary.get("segments", []))

        # Fetch confirmed and rejected feedback
        store = get_feedback_store()
        confirmed = store.get_confirmed_mappings(tenant_id, "smart-extract", message_type)
        rejected = store.get_rejected_mappings(tenant_id, "smart-extract", message_type)

        # Require significant feedback volume before acting as standalone tier
        if len(confirmed) < 99:
            return None

        # Get ignore list
        ignore_store = get_ignore_list_store()
        ignored = ignore_store.get_ignored(tenant_id, message_type)
        ignored_segments = set(ignored["segments"])
        ignored_fields = set(ignored["fields"])

        # Build segment list from confirmed preferences (only segments present in this message)
        confirmed_segments = []
        confirmed_fields = []
        rejected_segments = set(m["input_key"] for m in rejected)

        for mapping in confirmed:
            seg_id = mapping["input_key"]
            if seg_id in available_segments and seg_id not in ignored_segments and seg_id not in rejected_segments:
                if seg_id not in confirmed_segments:
                    confirmed_segments.append(seg_id)
            # Also collect field-level preferences (format: "SEG.field_num")
            target = mapping.get("target", "")
            if "." in target and target not in ignored_fields:
                confirmed_fields.append(target)

        if not confirmed_segments:
            return None

        # Score and sort by use_case_hint relevance
        use_case_tokens = _tokenize_for_matching(use_case_hint) if use_case_hint else []
        segment_details = structure_summary.get("segmentDetails", [])

        scored_segments: list[tuple[str, float]] = []
        for seg_id in confirmed_segments:
            seg_detail = next((s for s in segment_details if s["id"] == seg_id), None)
            fields = seg_detail.get("fields", []) if seg_detail else []
            score = _score_segment_relevance(seg_id, fields, use_case_tokens)
            # Boost confirmed segments (they have explicit user preference)
            score += 0.4
            scored_segments.append((seg_id, min(score, 1.0)))

        scored_segments.sort(key=lambda x: x[1], reverse=True)
        ordered_segments = [s[0] for s in scored_segments]

        # Build relevant fields from confirmed field preferences + segment fields
        relevant_fields = []
        for seg_id, score in scored_segments[:5]:
            seg_detail = next((s for s in segment_details if s["id"] == seg_id), None)
            if not seg_detail:
                continue
            for field_info in seg_detail.get("fields", [])[:5]:
                field_key = f"{seg_id}.{field_info.get('position', 0)}"
                if field_key in ignored_fields:
                    continue
                if field_info.get("hasValue"):
                    relevant_fields.append({
                        "segment": seg_id,
                        "field": field_info["position"],
                        "name": field_info.get("name", ""),
                        "reason": f"Confirmed preference for {seg_id}",
                    })

        # Determine output format from confirmed targets (most common format hint)
        output_format = "json_nested"
        format_targets = [m["target"] for m in confirmed if m["target"] in ("json_flat", "json_nested", "key_value")]
        if format_targets:
            # Use most frequent format preference
            from collections import Counter
            format_counts = Counter(format_targets)
            output_format = format_counts.most_common(1)[0][0]

        # Confidence scales with feedback volume
        confidence = min(len(confirmed) / 8, 0.95)

        return {
            "segments": ordered_segments,
            "outputFormat": output_format,
            "relevantFields": relevant_fields[:15],
            "reasoning": f"Based on your previous extraction preferences for {message_type} messages",
            "confidence": round(confidence, 2),
            "_source": "feedback",
        }

    def _mask_hl7_for_llm(
        self, structure_summary: dict, use_case_hint: Optional[str], tenant_id: str = "default"
    ) -> str:
        """
        Build LLM prompt content from structure summary (no PHI).

        Only includes segment names, field counts, data types, and lengths.
        Includes user feedback context (segment preferences) if available.
        """
        content = f"""HL7 Message Structure Analysis:

Message Type: {structure_summary['messageType']}
HL7 Version: {structure_summary['version']}
Total Segments: {len(structure_summary['segments'])}

Segments Present:
"""
        for seg_info in structure_summary["segmentDetails"]:
            content += f"  {seg_info['id']}: {seg_info['fieldCount']} fields"
            if seg_info.get("repeats"):
                content += f" (repeats: {seg_info['repeats']})"
            content += "\n"
            # Add field type info (no values)
            for field_info in seg_info.get("fields", [])[:10]:
                content += f"    Field {field_info['position']}: "
                content += f"type={field_info['dataType']}, "
                content += f"length={field_info['valueLength']}, "
                content += f"name={field_info['name']}\n"

        if use_case_hint:
            content += f"\nUse Case: {use_case_hint}\n"

        # Include user segment preferences from feedback store
        msg_type = structure_summary.get("messageType", "unknown")
        store = get_feedback_store()
        confirmed = store.get_confirmed_mappings(tenant_id, "smart-extract", msg_type)
        rejected = store.get_rejected_mappings(tenant_id, "smart-extract", msg_type)
        if confirmed or rejected:
            content += f"\nUser preferences for {msg_type} messages:\n"
            if confirmed:
                included = [m["input_key"] for m in confirmed[:15]]
                content += f"- Always include: {', '.join(included)}\n"
            if rejected:
                excluded = [m["input_key"] for m in rejected[:10]]
                content += f"- Usually exclude: {', '.join(excluded)}\n"

        return content

    def _summarize_message_structure(
        self, parsed: HL7Message, raw_message: str
    ) -> dict[str, Any]:
        """
        Create a PHI-free structural summary of the parsed message.

        Includes segment names, field positions, data types, and value lengths
        but NOT actual field values.
        """
        # Known segment field names from the parser
        field_names = HL7Parser.SEGMENT_FIELDS

        segments = list(parsed.segments.keys())
        segment_details = []

        for seg_id, seg_list in parsed.segments.items():
            seg_info = {
                "id": seg_id,
                "fieldCount": len(seg_list[0].fields) if seg_list else 0,
                "repeats": len(seg_list) if len(seg_list) > 1 else 0,
                "fields": [],
            }

            if seg_list:
                first_seg = seg_list[0]
                known_names = field_names.get(seg_id, [])
                for i, field_val in enumerate(first_seg.fields):
                    field_info = {
                        "position": i + 1,
                        "dataType": self._infer_data_type(field_val),
                        "valueLength": len(str(field_val)) if field_val else 0,
                        "name": known_names[i] if i < len(known_names) else f"field_{i+1}",
                        "hasValue": bool(field_val),
                    }
                    seg_info["fields"].append(field_info)

            segment_details.append(seg_info)

        return {
            "messageType": parsed.message_type,
            "version": parsed.version,
            "segments": segments,
            "segmentDetails": segment_details,
        }

    def _infer_data_type(self, field_value: Any) -> str:
        """Infer the HL7 data type from a field value (structural, not content-based)."""
        if field_value is None or field_value == "":
            return "EMPTY"
        val_str = str(field_value)
        if "^" in val_str:
            component_count = len(val_str.split("^"))
            if component_count >= 5:
                return "XPN"  # Extended Person Name or similar
            elif component_count >= 3:
                return "CE"   # Coded Element
            else:
                return "CX"   # Composite ID
        if re.match(r"^\d{8}(\d{6})?$", val_str):
            return "TS"  # Timestamp
        if re.match(r"^\d+$", val_str):
            return "NM"  # Numeric
        return "ST"  # String

    def _parse_suggestion(self, response_text: str) -> dict[str, Any]:
        """Parse the LLM JSON response into a suggestion dict."""
        text = response_text.strip()

        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        parsed = json.loads(text)
        return {
            "segments": parsed.get("segments", []),
            "outputFormat": parsed.get("outputFormat", "json_nested"),
            "relevantFields": parsed.get("relevantFields", []),
            "reasoning": parsed.get("reasoning", ""),
            "confidence": float(parsed.get("confidence", 0.5)),
        }

    def _get_fallback_suggestion(
        self, structure: dict, use_case_hint: Optional[str] = None, tenant_id: str = "default"
    ) -> dict[str, Any]:
        """
        Provide a deterministic suggestion using fuzzy matching and ignore lists.

        Uses:
        1. Segment purpose fuzzy matching against use case keywords
        2. Use-case keyword boosting (known patterns like "billing", "lab", etc.)
        3. Ignore list filtering (per tenant + message type)
        4. Field-level scoring based on data presence
        """
        segments = structure.get("segments", [])
        segment_details = structure.get("segmentDetails", [])
        message_type = structure.get("messageType", "unknown")

        # Get ignore list
        ignore_store = get_ignore_list_store()
        ignored = ignore_store.get_ignored(tenant_id, message_type)
        ignored_segments = set(ignored["segments"])
        ignored_fields = set(ignored["fields"])

        # Tokenize use case for fuzzy matching
        use_case_tokens = []
        if use_case_hint:
            use_case_tokens = _tokenize_for_matching(use_case_hint)

        # Use-case keyword boosting: check if use case matches known patterns
        boosted_segments: set[str] = set()
        if use_case_tokens:
            for keyword, boost_segs in USE_CASE_KEYWORDS.items():
                if keyword in " ".join(use_case_tokens):
                    boosted_segments.update(boost_segs)

        # Score each segment
        segment_scores: list[tuple[str, float]] = []
        for seg_id in segments:
            if seg_id in ignored_segments:
                continue

            # Get field details for this segment
            seg_detail = next((s for s in segment_details if s["id"] == seg_id), None)
            fields = seg_detail.get("fields", []) if seg_detail else []

            # Filter out ignored fields
            fields = [f for f in fields if f"{seg_id}.{f.get('position', 0)}" not in ignored_fields]

            # Score via fuzzy matching
            score = _score_segment_relevance(seg_id, fields, use_case_tokens)

            # Boost for known use-case associations
            if seg_id in boosted_segments:
                score += 0.3

            # Base priority boost for core clinical segments
            base_priority = {"MSH": 0.5, "PID": 0.4, "PV1": 0.2, "OBR": 0.15, "OBX": 0.15}
            score += base_priority.get(seg_id, 0.0)

            segment_scores.append((seg_id, min(score, 1.0)))

        # Sort by score descending
        segment_scores.sort(key=lambda x: x[1], reverse=True)
        recommended = [s[0] for s in segment_scores]

        # Build relevant fields list (non-ignored fields from top segments)
        relevant_fields = []
        for seg_id, score in segment_scores[:5]:
            seg_detail = next((s for s in segment_details if s["id"] == seg_id), None)
            if not seg_detail:
                continue
            for field_info in seg_detail.get("fields", [])[:5]:
                field_key = f"{seg_id}.{field_info.get('position', 0)}"
                if field_key in ignored_fields:
                    continue
                if field_info.get("hasValue"):
                    relevant_fields.append({
                        "segment": seg_id,
                        "field": field_info["position"],
                        "name": field_info.get("name", ""),
                        "reason": f"Relevance score: {score:.0%} for {seg_id}",
                    })

        # Determine confidence based on how good the matching was
        top_score = segment_scores[0][1] if segment_scores else 0.0
        confidence = min(0.3 + top_score * 0.4, 0.75)  # Range: 0.3-0.75 for deterministic

        reasoning_parts = ["Fuzzy-matched segments by clinical relevance"]
        if use_case_hint:
            reasoning_parts.append(f'for use case "{use_case_hint}"')
        if ignored_segments:
            reasoning_parts.append(f"(excluded: {', '.join(sorted(ignored_segments))})")
        if ignored_fields:
            reasoning_parts.append(f"({len(ignored_fields)} fields ignored)")

        return {
            "segments": recommended,
            "outputFormat": "json_nested",
            "relevantFields": relevant_fields[:15],
            "reasoning": " ".join(reasoning_parts),
            "confidence": confidence,
            "messageType": message_type,
            "ignoredSegments": sorted(ignored_segments),
            "ignoredFields": sorted(ignored_fields),
        }

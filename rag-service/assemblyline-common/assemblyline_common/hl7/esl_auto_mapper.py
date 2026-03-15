"""
ESL Auto-Mapper for HL7 v2.x

Automatically maps JSON payloads (FHIR, plain JSON, or HL7 flat notation)
to HL7 segment.field positions using ESL schema definitions.

The mapper reads ESL files at runtime, so any schema updates are
immediately reflected without code changes.

Usage:
    from assemblyline_common.hl7 import ESLAutoMapper

    mapper = ESLAutoMapper(esl_source="epic")

    # Auto-detect format and map
    result = mapper.to_hl7(json_payload, message_type="ADT_A28")

    # Preview mapping
    preview = mapper.get_mapping_preview(json_payload)

    # Override specific mappings
    mapping = mapper.auto_map(json_payload)
    mapping["customField"] = "ZPD.20"
    result = mapper.to_hl7(json_payload, mapping=mapping)
"""

import re
import logging
from typing import Any, Optional
from datetime import datetime

from assemblyline_common.hl7.esl_schema_manager import (
    ESLSchemaManager,
    SegmentDefinition,
    FieldDefinition,
)

logger = logging.getLogger(__name__)


# Common abbreviations for fuzzy matching
# These map common JSON key names to ESL field name tokens for matching.
# Add new entries here to expand auto-mapping coverage.
ABBREVIATIONS: dict[str, list[str]] = {
    # === Patient Demographics ===
    "fullname": ["patient", "name"],
    "name": ["patient", "name"],
    "patientname": ["patient", "name"],
    "fname": ["patient", "name"],  # first+last as full name
    "firstname": ["patient", "name"],  # maps to XPN component 2
    "lastname": ["patient", "name"],  # maps to XPN component 1
    "givenname": ["patient", "name"],
    "familyname": ["patient", "name"],
    "middlename": ["patient", "name"],
    "prefix": ["patient", "name"],
    "suffix": ["patient", "name"],
    "mothermaidenname": ["mother", "maiden", "name"],
    "mothermaiden": ["mother", "maiden", "name"],

    # === Dates ===
    "dob": ["date", "time", "of", "birth"],
    "dateofbirth": ["date", "time", "of", "birth"],
    "birthdate": ["date", "time", "of", "birth"],
    "birthday": ["date", "time", "of", "birth"],
    "deathdate": ["patient", "death", "date", "time"],
    "dateofdeath": ["patient", "death", "date", "time"],
    "deceased": ["patient", "death", "indicator"],
    "deathindicator": ["patient", "death", "indicator"],

    # === Identifiers ===
    "ssn": ["ssn", "number", "patient"],
    "socialsecurity": ["ssn", "number", "patient"],
    "socialsecuritynumber": ["ssn", "number", "patient"],
    "mrn": ["patient", "identifier", "list"],
    "medicalrecordnumber": ["patient", "identifier", "list"],
    "patientid": ["patient", "identifier", "list"],
    "patientidentifier": ["patient", "identifier", "list"],
    "accountnumber": ["patient", "account", "number"],
    "accountno": ["patient", "account", "number"],
    "driverslicense": ["driver", "license", "number"],
    "dlnumber": ["driver", "license", "number"],

    # === Gender / Sex ===
    "gender": ["administrative", "sex"],
    "sex": ["administrative", "sex"],
    "administrativesex": ["administrative", "sex"],
    "genderidentity": ["gender", "identity"],
    "sexualorientation": ["sexual", "orientation"],
    "birthsex": ["sex", "assigned", "birth"],

    # === Contact Info ===
    "phone": ["phone", "number", "home"],
    "homephone": ["phone", "number", "home"],
    "phonehome": ["phone", "number", "home"],
    "mobilephone": ["phone", "number", "home"],
    "cellphone": ["phone", "number", "home"],
    "workphone": ["phone", "number", "business"],
    "phonework": ["phone", "number", "business"],
    "businessphone": ["phone", "number", "business"],
    "officephone": ["phone", "number", "business"],
    "email": ["phone", "number", "home"],  # email goes in XTN component 4
    "emailaddress": ["phone", "number", "home"],
    "fax": ["phone", "number", "business"],
    "faxnumber": ["phone", "number", "business"],
    "tel": ["phone", "number"],
    "telephone": ["phone", "number", "home"],

    # === Address ===
    "address": ["patient", "address"],
    "homeaddress": ["patient", "address"],
    "patientaddress": ["patient", "address"],
    "addr": ["patient", "address"],
    "streetaddress": ["patient", "address"],
    "mailingaddress": ["patient", "address"],

    # === Marital / Family ===
    "maritalstatus": ["marital", "status"],
    "marital": ["marital", "status"],
    "multiplebirth": ["multiple", "birth", "indicator"],
    "birthorder": ["birth", "order"],

    # === Cultural / Social ===
    "race": ["race"],
    "ethnicity": ["ethnic", "group"],
    "ethnicgroup": ["ethnic", "group"],
    "religion": ["religion"],
    "language": ["primary", "language"],
    "primarylanguage": ["primary", "language"],
    "nationality": ["nationality"],
    "citizenship": ["citizenship"],
    "veteranstatus": ["veterans", "military", "status"],

    # === Provider / Care ===
    "pcp": ["patient", "primary", "care", "provider"],
    "primarycareprovider": ["patient", "primary", "care", "provider"],
    "attendingdoctor": ["attending", "doctor"],
    "attendingphysician": ["attending", "doctor"],
    "referringdoctor": ["referring", "doctor"],
    "admittingdoctor": ["admitting", "doctor"],

    # === Visit / Encounter ===
    "patientclass": ["patient", "class"],
    "admissiontype": ["admission", "type"],
    "admitdate": ["admit", "date", "time"],
    "dischargedate": ["discharge", "date", "time"],
    "visitnumber": ["visit", "number"],
    "hospitalservice": ["hospital", "service"],
    "patientlocation": ["assigned", "patient", "location"],
    "room": ["assigned", "patient", "location"],
    "bed": ["assigned", "patient", "location"],
    "ward": ["assigned", "patient", "location"],

    # === Insurance ===
    "insuranceid": ["insurance", "plan", "id"],
    "insuranceplan": ["insurance", "plan", "id"],
    "groupnumber": ["group", "number"],
    "policynumber": ["policy", "number"],

    # === Clinical ===
    "diagnosis": ["diagnosis", "code"],
    "diagnosiscode": ["diagnosis", "code"],
    "allergy": ["allergen", "code"],
    "allergycode": ["allergen", "code"],

    # === Epic Z-Segment ===
    "mychartstatus": ["my", "chart", "status"],
    "epicmrn": ["patient", "identifier", "list"],
}

# Gender/Sex value mappings
GENDER_MAP: dict[str, str] = {
    "male": "M", "m": "M",
    "female": "F", "f": "F",
    "other": "O", "o": "O",
    "unknown": "U", "u": "U",
    "non-binary": "O", "nonbinary": "O",
}

# Marital status mappings
MARITAL_STATUS_MAP: dict[str, str] = {
    "single": "S", "s": "S",
    "married": "M", "m": "M",
    "divorced": "D", "d": "D",
    "widowed": "W", "w": "W",
    "separated": "A", "a": "A",
    "common law": "C", "common-law": "C",
    "domestic partner": "T",
    "unknown": "U", "u": "U",
}

# FHIR Patient resource → ESL field name mapping
FHIR_PATIENT_MAP: dict[str, str] = {
    # Direct field mappings (FHIR path → ESL field name)
    "identifier": "Patient Identifier List",
    "name": "Patient Name",
    "gender": "Administrative Sex",
    "birthDate": "Date/Time Of Birth",
    "deceasedBoolean": "Patient Death Indicator",
    "deceasedDateTime": "Patient Death Date and Time",
    "maritalStatus": "Marital Status",
    "address": "Patient Address",
    "telecom": "Phone Number - Home",
    "generalPractitioner": "Patient Primary Care Provider",
    "communication": "Primary Language",
    "multipleBirthBoolean": "Multiple Birth Indicator",
    "multipleBirthInteger": "Birth Order",
}

# FHIR Encounter → PV1 ESL field name mapping
FHIR_ENCOUNTER_MAP: dict[str, str] = {
    "class": "Patient Class",
    "type": "Admission Type",
    "location": "Assigned Patient Location",
    "period": "Admit Date/Time",
    "hospitalization": "Discharge Disposition",
    "participant": "Attending Doctor",
    "serviceType": "Hospital Service",
}


class MappingResult:
    """Result of a single field mapping."""

    def __init__(
        self,
        input_key: str,
        target: str,
        field_name: str,
        confidence: float,
        segment_id: str = "",
        field_position: int = 0,
        component: int = 0,
    ):
        self.input_key = input_key
        self.target = target  # e.g., "PID.5" or "PID.5.1"
        self.field_name = field_name  # ESL field name
        self.confidence = confidence  # 0.0 - 1.0
        self.segment_id = segment_id
        self.field_position = field_position
        self.component = component

    def to_dict(self) -> dict:
        return {
            "inputKey": self.input_key,
            "target": self.target,
            "fieldName": self.field_name,
            "confidence": self.confidence,
            "segmentId": self.segment_id,
            "fieldPosition": self.field_position,
            "component": self.component,
        }


class ESLAutoMapper:
    """
    Automatically maps JSON payloads to HL7 segment fields using ESL schemas.

    Supports three input formats:
    1. FHIR resources (detected by 'resourceType' key)
    2. HL7 flat JSON (detected by 'PID.x.y' key format)
    3. Plain JSON (fuzzy-matched against ESL field names)
    """

    # Minimum confidence score to consider a match valid
    CONFIDENCE_THRESHOLD = 0.5

    def __init__(self, esl_source: str = "epic", data_dir: str = None):
        """
        Initialize the auto-mapper with a specific ESL source.

        Args:
            esl_source: ESL source to use. One of:
                - "epic" for Epic Custom (basedefs + hssdefs)
                - "2.4", "2.5.1", etc. for standard HL7 versions
            data_dir: Override path to the data directory containing ESL files.
        """
        self.esl_source = esl_source
        self.schema_manager = ESLSchemaManager(data_dir=data_dir)
        self.schema_manager.set_active_source(esl_source)

        # Build the field index from ESL
        self._field_index: dict[str, list[dict]] = {}  # segment_id → [{position, name, type, tokens}]
        self._build_field_index()

    def _build_field_index(self):
        """Build searchable field index from ESL segment definitions."""
        segments = self.schema_manager.get_all_segments(self.esl_source)

        for seg_id, seg_def in segments.items():
            self._field_index[seg_id] = []
            for i, field_def in enumerate(seg_def.fields, start=1):
                tokens = self._tokenize(field_def.name)
                self._field_index[seg_id].append({
                    "position": i,
                    "name": field_def.name,
                    "type": field_def.data_type,
                    "usage": field_def.usage,
                    "repeating": field_def.count != "1",
                    "tokens": tokens,
                })

    def detect_format(self, json_data: dict) -> str:
        """
        Detect the input JSON format.

        Returns:
            'fhir' - FHIR resource (has 'resourceType')
            'hl7_flat' - HL7 flat notation (keys like 'PID.5.1')
            'plain_json' - Arbitrary JSON (fuzzy match needed)
        """
        if not isinstance(json_data, dict):
            return "plain_json"

        # Check for FHIR resource
        if "resourceType" in json_data:
            return "fhir"

        # Check for HL7 flat notation (at least 30% of keys match PID.x.y pattern)
        hl7_pattern = re.compile(r'^[A-Z]{2,3}\.\d+(\.\d+)?$')
        hl7_key_count = sum(1 for k in json_data.keys() if hl7_pattern.match(k))
        if hl7_key_count > 0 and hl7_key_count / len(json_data) >= 0.3:
            return "hl7_flat"

        return "plain_json"

    def auto_map(self, json_data: dict, format_hint: str = None, message_type: Optional[str] = None) -> dict[str, str]:
        """
        Generate field mapping from JSON keys to segment.field notation.

        Args:
            json_data: Input JSON payload
            format_hint: Override format detection ('fhir', 'hl7_flat', 'plain_json')
            message_type: Optional message type (e.g., 'ADT_A28') for field prioritization

        Returns:
            Dict mapping input keys to segment.field notation.
            e.g., {"fullName": "PID.5", "birthDate": "PID.7.1"}
        """
        fmt = format_hint or self.detect_format(json_data)

        if fmt == "hl7_flat":
            return self._map_hl7_flat(json_data)
        elif fmt == "fhir":
            return self._map_fhir(json_data)
        else:
            return self._map_plain_json(json_data)

    def apply_mapping(self, json_data: dict, mapping: dict[str, str]) -> dict[str, str]:
        """
        Apply a mapping to produce flat HL7 JSON (keys = segment.field notation).

        Args:
            json_data: Original JSON payload
            mapping: Field mapping (input_key → segment.field)

        Returns:
            Flat HL7 JSON ready for message generation.
        """
        fmt = self.detect_format(json_data)
        result: dict[str, str] = {}

        for input_key, target in mapping.items():
            if input_key.startswith("_static"):
                # Static values: {"_static": {"ZPD.4": "ACTIVE"}}
                if isinstance(target, dict):
                    result.update(target)
                continue

            value = json_data.get(input_key)
            if value is None:
                continue

            # Get the target field type from ESL for value transformation
            field_type = self._get_field_type(target)

            if fmt == "fhir":
                # FHIR values need special extraction
                transformed = self._transform_fhir_value(input_key, value, target, field_type)
            else:
                transformed = self._transform_value(value, field_type, target)

            if transformed is not None:
                if isinstance(transformed, dict):
                    # Multi-component result (e.g., name → PID.5.1, PID.5.2, etc.)
                    result.update(transformed)
                else:
                    result[target] = transformed

        return result

    def to_hl7(
        self,
        json_data: dict,
        mapping: dict[str, str] = None,
        message_type: str = "ADT_A28",
        config: dict = None,
    ) -> str:
        """
        Full pipeline: JSON → auto-map → flat HL7 JSON → HL7 message string.

        Args:
            json_data: Input JSON payload (any format)
            mapping: Optional pre-computed mapping (if None, auto_map is called)
            message_type: HL7 message type (e.g., "ADT_A28")
            config: Optional MSH config (sendingApp, sendingFacility, etc.)

        Returns:
            HL7 message string
        """
        config = config or {}

        if mapping is None:
            mapping = self.auto_map(json_data)

        # Apply mapping to get flat HL7 JSON
        flat_hl7 = self.apply_mapping(json_data, mapping)

        # Build HL7 message from flat JSON
        return self._build_message(flat_hl7, message_type, config)

    def get_mapping_preview(self, json_data: dict, format_hint: str = None, message_type: Optional[str] = None) -> list[dict]:
        """
        Return mapping preview for UI display.

        Returns list of mapping results with confidence scores.
        Each entry: {inputKey, target, fieldName, confidence, segmentId, fieldPosition}
        """
        fmt = format_hint or self.detect_format(json_data)
        results: list[MappingResult] = []

        if fmt == "hl7_flat":
            for key in json_data.keys():
                results.append(MappingResult(
                    input_key=key, target=key, field_name=self._get_field_name(key),
                    confidence=1.0, segment_id=key.split(".")[0],
                    field_position=int(key.split(".")[1]) if "." in key else 0,
                ))
        elif fmt == "fhir":
            resource_type = json_data.get("resourceType", "")
            fhir_map = self._get_fhir_map(resource_type)
            preferred = self._get_preferred_segments(resource_type)
            for fhir_key, esl_name in fhir_map.items():
                if fhir_key in json_data:
                    target = self._find_field_by_name(esl_name, preferred)
                    results.append(MappingResult(
                        input_key=fhir_key, target=target or "?",
                        field_name=esl_name, confidence=1.0 if target else 0.0,
                        segment_id=target.split(".")[0] if target else "",
                    ))
            # Check for unmapped keys
            for key in json_data.keys():
                if key not in fhir_map and key != "resourceType":
                    results.append(MappingResult(
                        input_key=key, target="?", field_name="(unmapped)",
                        confidence=0.0,
                    ))
        else:
            for key in json_data.keys():
                match_target, match_name, confidence = self._fuzzy_match_all_segments(key)
                results.append(MappingResult(
                    input_key=key, target=match_target or "?",
                    field_name=match_name or "(no match)",
                    confidence=confidence,
                    segment_id=match_target.split(".")[0] if match_target else "",
                ))

        return [r.to_dict() for r in results]

    # === Private: Format-specific mapping ===

    def _map_hl7_flat(self, json_data: dict) -> dict[str, str]:
        """Map HL7 flat notation — passthrough."""
        return {k: k for k in json_data.keys()}

    def _map_fhir(self, json_data: dict) -> dict[str, str]:
        """Map FHIR resource fields to HL7 segment.field notation."""
        resource_type = json_data.get("resourceType", "Patient")
        fhir_map = self._get_fhir_map(resource_type)
        preferred = self._get_preferred_segments(resource_type)

        result: dict[str, str] = {}
        for fhir_key, esl_name in fhir_map.items():
            if fhir_key in json_data:
                target = self._find_field_by_name(esl_name, preferred)
                if target:
                    result[fhir_key] = target

        return result

    def _get_preferred_segments(self, resource_type: str) -> list[str]:
        """Get preferred segment search order for a FHIR resource type."""
        if resource_type == "Patient":
            return ["PID", "PD1", "ZPD"]
        elif resource_type == "Encounter":
            return ["PV1", "PV2"]
        elif resource_type == "Condition":
            return ["DG1"]
        return []

    def _map_plain_json(self, json_data: dict) -> dict[str, str]:
        """Map plain JSON keys using fuzzy matching against ESL field names."""
        result: dict[str, str] = {}

        for key in json_data.keys():
            target, field_name, confidence = self._fuzzy_match_all_segments(key)
            if target and confidence >= self.CONFIDENCE_THRESHOLD:
                result[key] = target
                logger.debug(f"Mapped '{key}' → {target} ({field_name}, confidence={confidence:.2f})")
            else:
                logger.warning(f"No match for key '{key}' (best confidence={confidence:.2f})")

        return result

    def _get_fhir_map(self, resource_type: str) -> dict[str, str]:
        """Get the FHIR mapping template for a resource type."""
        if resource_type == "Patient":
            return FHIR_PATIENT_MAP
        elif resource_type == "Encounter":
            return FHIR_ENCOUNTER_MAP
        else:
            logger.warning(f"No FHIR mapping template for resource type: {resource_type}")
            return {}

    # === Private: Fuzzy matching ===

    def _fuzzy_match_all_segments(self, key: str) -> tuple[str, str, float]:
        """
        Match a JSON key against ALL segment field names.

        Returns: (target, field_name, confidence)
            target: e.g., "PID.5" or "PID.13"
            field_name: ESL field name
            confidence: 0.0-1.0
        """
        best_target = ""
        best_name = ""
        best_confidence = 0.0

        input_tokens = self._tokenize(key)

        # Expand abbreviations (try full key, then normalized)
        key_lower = key.lower().replace("_", "").replace("-", "")
        if key_lower in ABBREVIATIONS:
            input_tokens = ABBREVIATIONS[key_lower]

        # Priority segments get a small boost to prefer patient-level matches
        priority_segments = {"PID": 0.15, "PD1": 0.05, "PV1": 0.05}

        for seg_id, fields in self._field_index.items():
            boost = priority_segments.get(seg_id, 0.0)

            for field_info in fields:
                field_tokens = field_info["tokens"]
                confidence = self._token_overlap_score(input_tokens, field_tokens)
                # Apply boost for comparison (uncapped to break ties)
                comparison_score = (confidence + boost) if confidence >= 0.4 else confidence

                if comparison_score > best_confidence:
                    best_confidence = comparison_score
                    best_target = f"{seg_id}.{field_info['position']}"
                    best_name = field_info["name"]

        # Cap output confidence at 1.0
        return best_target, best_name, min(best_confidence, 1.0)

    def _token_overlap_score(self, input_tokens: list[str], field_tokens: list[str]) -> float:
        """
        Calculate token overlap between input key and ESL field name.

        Uses bidirectional matching: how many input tokens appear in field tokens
        AND how many field tokens appear in input tokens.
        """
        if not input_tokens or not field_tokens:
            return 0.0

        # Remove common stop words from field tokens for matching
        stop_words = {"of", "the", "a", "an", "for", "-", "id"}
        field_meaningful = [t for t in field_tokens if t not in stop_words]
        input_meaningful = [t for t in input_tokens if t not in stop_words]

        if not field_meaningful:
            field_meaningful = field_tokens
        if not input_meaningful:
            input_meaningful = input_tokens

        # Forward match: what % of input tokens are in field tokens
        forward = sum(1 for t in input_meaningful if t in field_meaningful) / len(input_meaningful)

        # Backward match: what % of field tokens are in input tokens
        backward = sum(1 for t in field_meaningful if t in input_meaningful) / len(field_meaningful)

        # Also check for partial token matches (e.g., "birth" matches "birthday")
        partial_forward = sum(
            1 for t in input_meaningful
            if any(t in ft or ft in t for ft in field_meaningful)
        ) / len(input_meaningful)

        # Weight: prioritize forward match, use partial as fallback
        score = max(
            (forward * 0.6 + backward * 0.4),  # Exact token overlap
            (partial_forward * 0.5 + backward * 0.3),  # Partial match
        )

        return min(score, 1.0)

    def _find_field_by_name(self, esl_name: str, preferred_segments: list[str] = None) -> Optional[str]:
        """Find segment.field notation by exact ESL field name.

        Args:
            esl_name: The ESL field name to search for
            preferred_segments: Segments to search first (e.g., ["PID"] for Patient)
        """
        # Search preferred segments first
        if preferred_segments:
            for seg_id in preferred_segments:
                if seg_id in self._field_index:
                    for field_info in self._field_index[seg_id]:
                        if field_info["name"].lower() == esl_name.lower():
                            return f"{seg_id}.{field_info['position']}"

        # Then search all segments
        for seg_id, fields in self._field_index.items():
            for field_info in fields:
                if field_info["name"].lower() == esl_name.lower():
                    return f"{seg_id}.{field_info['position']}"
        return None

    def _get_field_name(self, target: str) -> str:
        """Get ESL field name for a segment.field target."""
        parts = target.split(".")
        if len(parts) >= 2:
            seg_id = parts[0]
            try:
                position = int(parts[1])
            except ValueError:
                return ""
            if seg_id in self._field_index:
                for field_info in self._field_index[seg_id]:
                    if field_info["position"] == position:
                        return field_info["name"]
        return ""

    def _get_field_type(self, target: str) -> str:
        """Get the ESL data type for a segment.field target."""
        parts = target.split(".")
        if len(parts) >= 2:
            seg_id = parts[0]
            try:
                position = int(parts[1])
            except ValueError:
                return "ST"
            if seg_id in self._field_index:
                for field_info in self._field_index[seg_id]:
                    if field_info["position"] == position:
                        return field_info["type"]
        return "ST"

    # === Private: Value transformation ===

    def _transform_value(self, value: Any, field_type: str, target: str = "") -> Any:
        """
        Transform a value to HL7 format based on ESL data type.

        Args:
            value: Input value
            field_type: ESL data type (TS, IS, ID, XPN, XAD, XTN, etc.)
            target: Target segment.field for context
        """
        if value is None:
            return None

        # Handle boolean → Y/N
        if isinstance(value, bool):
            if field_type in ("ID",):
                return "Y" if value else "N"
            return str(value)

        # String values
        if isinstance(value, str):
            value_str = value.strip()

            # Timestamp fields
            if field_type in ("TS", "DT", "DTM"):
                return self._format_timestamp(value_str)

            # Sex/Gender fields
            if field_type == "IS" and target.endswith(".8"):  # PID.8 = Administrative Sex
                return GENDER_MAP.get(value_str.lower(), value_str)

            # Marital status
            if field_type.startswith("CE") and "16" in target:  # PID.16
                return MARITAL_STATUS_MAP.get(value_str.lower(), value_str)

            # Name fields (XPN): parse "Mr. John Michael Smith" → components
            if field_type == "XPN":
                name_parts = self._parse_full_name(value_str)
                # Remap internal keys to use actual target prefix
                result = {}
                for k, v in name_parts.items():
                    if k.startswith("_name."):
                        comp = k.split(".")[1]
                        result[f"{target}.{comp}"] = v
                    else:
                        result[k] = v
                return result

            # Address fields (XAD): parse "123 Main St, City, ST 12345, US"
            if field_type == "XAD":
                return self._parse_address(value_str, target)

            # Phone fields (XTN)
            if field_type == "XTN":
                return value_str

            return value_str

        # Numeric
        if isinstance(value, (int, float)):
            return str(value)

        # List/dict - complex types
        if isinstance(value, (list, dict)):
            return self._transform_complex_value(value, field_type, target)

        return str(value)

    def _transform_fhir_value(self, fhir_key: str, value: Any, target: str, field_type: str) -> Any:
        """Transform a FHIR resource field value to HL7 format."""

        if fhir_key == "name" and isinstance(value, list) and value:
            # FHIR name: [{"family": "Smith", "given": ["John", "Michael"], "prefix": ["Mr."]}]
            name = value[0] if isinstance(value[0], dict) else {}
            family = name.get("family", "")
            given = name.get("given", [])
            prefix = name.get("prefix", [])
            suffix = name.get("suffix", [])
            result = {}
            result[f"{target}.1"] = family
            if given:
                result[f"{target}.2"] = given[0]
            if len(given) > 1:
                result[f"{target}.3"] = given[1]
            if suffix:
                result[f"{target}.4"] = suffix[0]
            if prefix:
                result[f"{target}.5"] = prefix[0]
            return result

        if fhir_key == "identifier" and isinstance(value, list):
            # FHIR identifier: [{"system": "...", "value": "MRN-001"}]
            result = {}
            for i, ident in enumerate(value):
                if isinstance(ident, dict):
                    id_value = ident.get("value", "")
                    system = ident.get("system", "")
                    # First identifier → PID.3.1, additional → PID.3 repeating
                    if i == 0:
                        result[target + ".1"] = id_value
                        if system:
                            result[target + ".4"] = system
                    # SSN detection
                    if "2.16.840.1.113883.4.1" in system:
                        result["PID.19"] = id_value
            return result

        if fhir_key == "address" and isinstance(value, list) and value:
            addr = value[0] if isinstance(value[0], dict) else {}
            result = {}
            lines = addr.get("line", [])
            if lines:
                result[f"{target}.1"] = lines[0]
            if len(lines) > 1:
                result[f"{target}.2"] = lines[1]
            if addr.get("city"):
                result[f"{target}.3"] = addr["city"]
            if addr.get("state"):
                result[f"{target}.4"] = addr["state"]
            if addr.get("postalCode"):
                result[f"{target}.5"] = addr["postalCode"]
            if addr.get("country"):
                result[f"{target}.6"] = addr["country"]
            return result

        if fhir_key == "telecom" and isinstance(value, list):
            result = {}
            for contact in value:
                if not isinstance(contact, dict):
                    continue
                system = contact.get("system", "")
                use = contact.get("use", "")
                val = contact.get("value", "")
                if system == "phone" and use == "home":
                    result["PID.13.1"] = val
                elif system == "phone" and use == "work":
                    result["PID.14.1"] = val
                elif system == "email":
                    result["PID.13.4"] = val
                elif system == "phone":
                    result["PID.13.1"] = val  # default to home
            return result

        if fhir_key == "gender":
            return GENDER_MAP.get(str(value).lower(), str(value))

        if fhir_key == "birthDate":
            return self._format_timestamp(str(value))

        if fhir_key == "deceasedBoolean":
            return "Y" if value else "N"

        if fhir_key == "maritalStatus" and isinstance(value, dict):
            coding = value.get("coding", [])
            if coding and isinstance(coding[0], dict):
                return coding[0].get("code", "")
            return ""

        if fhir_key == "generalPractitioner" and isinstance(value, list) and value:
            ref = value[0]
            if isinstance(ref, dict):
                reference = ref.get("reference", "")
                # Extract ID from "Practitioner/eAB3mDIBBcyUKviyzrxsnAw3"
                if "/" in reference:
                    return reference.split("/")[-1]
                return reference
            return str(ref)

        # Default: use standard transform
        return self._transform_value(value, field_type, target)

    def _transform_complex_value(self, value: Any, field_type: str, target: str) -> Any:
        """Handle complex (list/dict) values."""
        if isinstance(value, list):
            # Take first element for non-repeating fields
            if value:
                return self._transform_value(value[0], field_type, target)
            return None

        if isinstance(value, dict):
            # Try common dict patterns
            if "value" in value:
                return self._transform_value(value["value"], field_type, target)
            if "code" in value:
                return str(value["code"])
            if "text" in value:
                return str(value["text"])

        return str(value)

    # === Private: Value format helpers ===

    def _format_timestamp(self, value: str) -> str:
        """Convert various date formats to HL7 timestamp (YYYYMMDD or YYYYMMDDHHmmss)."""
        if not value:
            return ""

        # Already in HL7 format
        if re.match(r'^\d{8,14}$', value):
            return value

        # ISO format: 2026-01-22T16:48:00Z or 2026-01-22
        try:
            # Try full datetime
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(value.replace("+00:00", "Z").rstrip("Z") if "T" in value else value,
                                           fmt.rstrip("Z").replace("%z", ""))
                    if "T" in value or " " in value:
                        return dt.strftime("%Y%m%d%H%M%S")
                    else:
                        return dt.strftime("%Y%m%d")
                except ValueError:
                    continue
        except Exception:
            pass

        # Try MM/DD/YYYY
        try:
            dt = datetime.strptime(value, "%m/%d/%Y")
            return dt.strftime("%Y%m%d")
        except ValueError:
            pass

        # Return as-is if can't parse
        return value.replace("-", "").replace("/", "").replace(":", "").replace("T", "").replace("Z", "")[:14]

    def _parse_full_name(self, name_str: str) -> dict[str, str]:
        """
        Parse a full name string into XPN components.

        Input: "Mr. John Michael Smith" or "Smith, John Michael"
        Output: {"PID.5.1": "Smith", "PID.5.2": "John", "PID.5.3": "Michael", "PID.5.5": "Mr."}
        """
        # Detect prefixes
        prefixes = ["Mr.", "Mrs.", "Ms.", "Dr.", "Prof.", "Rev."]
        prefix = ""
        for p in prefixes:
            if name_str.startswith(p + " "):
                prefix = p
                name_str = name_str[len(p):].strip()
                break
            elif name_str.lower().startswith(p.lower() + " "):
                prefix = p
                name_str = name_str[len(p):].strip()
                break

        # Detect suffixes
        suffixes = ["Jr.", "Sr.", "II", "III", "IV", "MD", "PhD", "DDS"]
        suffix = ""
        for s in suffixes:
            if name_str.endswith(" " + s):
                suffix = s
                name_str = name_str[:-len(s)].strip()
                break

        parts = name_str.split()

        # "Last, First Middle" format
        if "," in name_str:
            comma_parts = name_str.split(",", 1)
            family = comma_parts[0].strip()
            rest = comma_parts[1].strip().split() if len(comma_parts) > 1 else []
            given = rest[0] if rest else ""
            middle = " ".join(rest[1:]) if len(rest) > 1 else ""
        elif len(parts) >= 3:
            # "First Middle Last" (assume last word is family name)
            given = parts[0]
            middle = " ".join(parts[1:-1])
            family = parts[-1]
        elif len(parts) == 2:
            given = parts[0]
            family = parts[1]
            middle = ""
        elif len(parts) == 1:
            family = parts[0]
            given = ""
            middle = ""
        else:
            return {"PID.5": name_str}

        # Return as component dict — caller determines final target prefix
        result = {}
        if family:
            result["_name.1"] = family
        if given:
            result["_name.2"] = given
        if middle:
            result["_name.3"] = middle
        if suffix:
            result["_name.4"] = suffix
        if prefix:
            result["_name.5"] = prefix

        return result

    def _parse_address(self, addr_str: str, target: str) -> dict[str, str]:
        """
        Parse an address string into XAD components.

        Input: "123 Main Street, Apt 4B, Springfield, IL 62701, US"
        Output: {target+".1": "123 Main Street", target+".2": "Apt 4B", ...}
        """
        parts = [p.strip() for p in addr_str.split(",")]
        result = {}

        if len(parts) >= 5:
            # street, unit, city, state zip, country
            result[f"{target}.1"] = parts[0]
            result[f"{target}.2"] = parts[1]
            result[f"{target}.3"] = parts[2]
            # "IL 62701" → state + zip
            state_zip = parts[3].strip().split()
            if len(state_zip) >= 2:
                result[f"{target}.4"] = state_zip[0]
                result[f"{target}.5"] = state_zip[1]
            else:
                result[f"{target}.4"] = parts[3]
            result[f"{target}.6"] = parts[4] if len(parts) > 4 else ""
        elif len(parts) >= 4:
            result[f"{target}.1"] = parts[0]
            result[f"{target}.3"] = parts[1]
            state_zip = parts[2].strip().split()
            if len(state_zip) >= 2:
                result[f"{target}.4"] = state_zip[0]
                result[f"{target}.5"] = state_zip[1]
            else:
                result[f"{target}.4"] = parts[2]
            result[f"{target}.6"] = parts[3]
        elif len(parts) >= 2:
            result[f"{target}.1"] = parts[0]
            result[f"{target}.3"] = parts[1]
        else:
            result[f"{target}.1"] = addr_str

        return result

    # === Private: HL7 message building ===

    def _build_message(self, flat_hl7: dict[str, str], message_type: str, config: dict) -> str:
        """Build a complete HL7 message from flat JSON."""
        # Group fields by segment
        segment_fields: dict[str, dict[int, dict[int, str]]] = {}

        for key, value in flat_hl7.items():
            parts = key.split(".")
            if len(parts) < 2:
                continue

            seg_id = parts[0]
            try:
                field_pos = int(parts[1])
            except ValueError:
                continue
            component = int(parts[2]) if len(parts) > 2 else 0

            if seg_id not in segment_fields:
                segment_fields[seg_id] = {}
            if field_pos not in segment_fields[seg_id]:
                segment_fields[seg_id][field_pos] = {}

            segment_fields[seg_id][field_pos][component] = value

        # Determine segment order from message structure ESL
        msg_type_parts = message_type.split("_")
        segment_order = self._get_segment_order(message_type)

        # Build MSH segment
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        sending_app = config.get("sendingApp", "LOGICWEAVER")
        sending_fac = config.get("sendingFacility", "FACILITY")
        receiving_app = config.get("receivingApp", "")
        receiving_fac = config.get("receivingFacility", "")
        processing_id = config.get("processingId", "P")
        hl7_version = config.get("hl7Version", "2.4")
        msg_control_id = config.get("messageControlId", f"MSG{now}")

        # Message type field: ADT^A28^ADT_A05
        trigger = msg_type_parts[1] if len(msg_type_parts) > 1 else ""
        msg_type_field = f"{msg_type_parts[0]}^{trigger}^{message_type}"

        msh = (
            f"MSH|^~\\&|{sending_app}|{sending_fac}|{receiving_app}|{receiving_fac}|"
            f"{now}||{msg_type_field}|{msg_control_id}|{processing_id}|{hl7_version}"
        )

        # Build EVN segment
        evn = f"EVN|{trigger}|{now}"

        segments = [msh, evn]

        # Build remaining segments in order
        for seg_id in segment_order:
            if seg_id in ("MSH", "EVN"):
                continue
            if seg_id in segment_fields:
                seg_str = self._build_segment(seg_id, segment_fields[seg_id])
                segments.append(seg_str)
            elif seg_id == "PV1":
                # PV1 is often required even if empty
                segments.append("PV1||N")

        return "\r".join(segments) + "\r"

    def _build_segment(self, seg_id: str, fields: dict[int, dict[int, str]]) -> str:
        """Build a single segment string from field data."""
        if not fields:
            return seg_id

        max_field = max(fields.keys())
        parts = [seg_id]

        for pos in range(1, max_field + 1):
            if pos in fields:
                components = fields[pos]
                if 0 in components and len(components) == 1:
                    # Simple value (no sub-components)
                    parts.append(components[0])
                else:
                    # Multi-component field
                    max_comp = max(components.keys()) if components else 0
                    comp_parts = []
                    for c in range(1, max_comp + 1):
                        comp_parts.append(components.get(c, ""))
                    parts.append("^".join(comp_parts))
            else:
                parts.append("")

        return "|".join(parts)

    def _get_segment_order(self, message_type: str) -> list[str]:
        """Get segment order from message structure ESL."""
        # Try to load from schema manager
        structure = self.schema_manager.message_structures.get(message_type)
        if structure and hasattr(structure, "segments") and structure.segments:
            order = []
            for s in structure.segments:
                if isinstance(s, dict):
                    seg_id = s.get("idRef", s.get("id", ""))
                elif hasattr(s, "segment_id"):
                    seg_id = s.segment_id
                else:
                    continue
                if seg_id:
                    order.append(seg_id)
            if order:
                return order

        # Default order for ADT messages
        return ["MSH", "EVN", "PID", "ZPD", "PD1", "NK1", "PV1", "PV2", "AL1", "DG1"]

    # === Private: Tokenization ===

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """
        Convert a string to normalized tokens for comparison.

        Handles:
        - camelCase: "dateOfBirth" → ["date", "of", "birth"]
        - snake_case: "date_of_birth" → ["date", "of", "birth"]
        - PascalCase: "DateOfBirth" → ["date", "of", "birth"]
        - kebab-case: "date-of-birth" → ["date", "of", "birth"]
        - Spaces: "Date/Time Of Birth" → ["date", "time", "of", "birth"]
        """
        if not text:
            return []

        # Split camelCase/PascalCase
        tokens = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        tokens = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', tokens)

        # Replace separators with spaces
        tokens = re.sub(r'[_\-/\s]+', ' ', tokens)

        # Remove non-alphanumeric (except spaces)
        tokens = re.sub(r"[^a-zA-Z0-9\s]", "", tokens)

        # Split and lowercase
        return [t.lower() for t in tokens.split() if t]

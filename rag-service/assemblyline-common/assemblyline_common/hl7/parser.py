"""
HL7 v2.x Message Parser

Parses HL7 v2.x messages into structured Python dictionaries.
"""

import re
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class HL7Segment:
    """Represents a single HL7 segment."""
    name: str
    fields: list[Any]
    raw: str


@dataclass
class HL7Message:
    """Represents a parsed HL7 message."""
    raw: str
    message_type: str
    message_control_id: str
    version: str
    segments: dict[str, list[HL7Segment]] = field(default_factory=dict)
    encoding_characters: str = "^~\\&"
    parsed_data: dict[str, Any] = field(default_factory=dict)


class HL7Parser:
    """
    Parser for HL7 v2.x messages.
    
    Usage:
        parsed = HL7Parser.parse(hl7_message)
        patient_name = parsed.parsed_data["patient"]["name"]
    """
    
    # Segment terminator
    SEGMENT_TERMINATOR = "\r"
    
    # Common segment definitions for field names
    SEGMENT_FIELDS = {
        "MSH": [
            "encoding_characters", "sending_application", "sending_facility",
            "receiving_application", "receiving_facility", "datetime",
            "security", "message_type", "message_control_id", "processing_id",
            "version", "sequence_number", "continuation_pointer", "accept_ack_type",
            "application_ack_type", "country_code", "character_set"
        ],
        "PID": [
            "set_id", "patient_id", "patient_identifier_list", "alternate_patient_id",
            "patient_name", "mother_maiden_name", "datetime_of_birth", "sex",
            "patient_alias", "race", "patient_address", "county_code",
            "phone_number_home", "phone_number_business", "primary_language",
            "marital_status", "religion", "patient_account_number", "ssn",
            "drivers_license", "mothers_identifier", "ethnic_group",
            "birth_place", "multiple_birth_indicator", "birth_order", "citizenship",
            "veterans_military_status", "nationality", "patient_death_datetime",
            "patient_death_indicator"
        ],
        "PV1": [
            "set_id", "patient_class", "assigned_patient_location",
            "admission_type", "preadmit_number", "prior_patient_location",
            "attending_doctor", "referring_doctor", "consulting_doctor",
            "hospital_service", "temporary_location", "preadmit_test_indicator",
            "readmission_indicator", "admit_source", "ambulatory_status",
            "vip_indicator", "admitting_doctor", "patient_type",
            "visit_number", "financial_class", "charge_price_indicator",
            "courtesy_code", "credit_rating", "contract_code", "contract_effective_date",
            "contract_amount", "contract_period", "interest_code", "transfer_to_bad_debt_code",
            "transfer_to_bad_debt_date", "bad_debt_agency_code", "bad_debt_transfer_amount",
            "bad_debt_recovery_amount", "delete_account_indicator", "delete_account_date",
            "discharge_disposition", "discharged_to_location", "diet_type",
            "servicing_facility", "bed_status", "account_status", "pending_location",
            "prior_temporary_location", "admit_datetime", "discharge_datetime",
            "current_patient_balance", "total_charges", "total_adjustments",
            "total_payments", "alternate_visit_id", "visit_indicator",
            "other_healthcare_provider"
        ],
        "OBR": [
            "set_id", "placer_order_number", "filler_order_number",
            "universal_service_id", "priority", "requested_datetime",
            "observation_datetime", "observation_end_datetime",
            "collection_volume", "collector_identifier", "specimen_action_code",
            "danger_code", "relevant_clinical_info", "specimen_received_datetime",
            "specimen_source", "ordering_provider", "order_callback_phone_number",
            "placer_field_1", "placer_field_2", "filler_field_1", "filler_field_2",
            "results_status_change_datetime", "charge_to_practice",
            "diagnostic_service_section_id", "result_status", "parent_result",
            "quantity_timing", "result_copies_to", "parent", "transportation_mode",
            "reason_for_study", "principal_result_interpreter",
            "assistant_result_interpreter", "technician", "transcriptionist",
            "scheduled_datetime", "number_of_sample_containers",
            "transport_logistics_of_collected_sample",
            "collectors_comment", "transport_arrangement_responsibility",
            "transport_arranged", "escort_required", "planned_patient_transport_comment"
        ],
        "OBX": [
            "set_id", "value_type", "observation_identifier", "observation_sub_id",
            "observation_value", "units", "references_range", "abnormal_flags",
            "probability", "nature_of_abnormal_test", "observation_result_status",
            "date_last_obs_normal_values", "user_defined_access_checks",
            "datetime_of_observation", "producers_id", "responsible_observer",
            "observation_method"
        ],
        "IN1": [
            "set_id", "insurance_plan_id", "insurance_company_id",
            "insurance_company_name", "insurance_company_address",
            "insurance_company_contact_person", "insurance_company_phone",
            "group_number", "group_name", "insured_group_emp_id",
            "insured_group_emp_name", "plan_effective_date", "plan_expiration_date",
            "authorization_information", "plan_type", "name_of_insured",
            "insured_relationship_to_patient", "insured_dob", "insured_address",
            "assignment_of_benefits", "coordination_of_benefits",
            "coord_of_ben_priority", "notice_of_admission_flag",
            "notice_of_admission_date", "report_of_eligibility_flag",
            "report_of_eligibility_date", "release_information_code",
            "pre_admit_cert", "verification_datetime", "verification_by",
            "type_of_agreement_code", "billing_status", "lifetime_reserve_days",
            "delay_before_lr_day", "company_plan_code", "policy_number",
            "policy_deductible", "policy_limit_amount", "policy_limit_days",
            "room_rate_semi_private", "room_rate_private", "insured_employment_status",
            "insured_sex", "insured_employer_address", "verification_status",
            "prior_insurance_plan_id", "coverage_type", "handicap",
            "insured_id_number"
        ],
    }
    
    @classmethod
    def parse(cls, message: str) -> HL7Message:
        """
        Parse an HL7 v2.x message string into a structured HL7Message object.
        
        Args:
            message: Raw HL7 message string
            
        Returns:
            HL7Message object with parsed segments and data
        """
        # Normalize line endings
        message = message.replace("\r\n", "\r").replace("\n", "\r").strip()
        
        # Split into segments
        segments_raw = message.split(cls.SEGMENT_TERMINATOR)
        
        if not segments_raw or not segments_raw[0].startswith("MSH"):
            raise ValueError("Invalid HL7 message: must start with MSH segment")
        
        # Parse MSH to get encoding characters
        msh = segments_raw[0]
        field_separator = msh[3]  # Usually |
        encoding_chars = msh[4:8]  # Usually ^~\&
        
        # Initialize message
        hl7_msg = HL7Message(
            raw=message,
            message_type="",
            message_control_id="",
            version="",
            encoding_characters=encoding_chars,
        )
        
        # Parse each segment
        for segment_raw in segments_raw:
            if not segment_raw.strip():
                continue
                
            segment = cls._parse_segment(segment_raw, field_separator, encoding_chars)
            
            if segment.name not in hl7_msg.segments:
                hl7_msg.segments[segment.name] = []
            hl7_msg.segments[segment.name].append(segment)
        
        # Extract key fields from MSH
        if "MSH" in hl7_msg.segments:
            msh_segment = hl7_msg.segments["MSH"][0]
            if len(msh_segment.fields) > 8:
                msg_type = msh_segment.fields[8]
                if isinstance(msg_type, list) and len(msg_type) > 0:
                    hl7_msg.message_type = f"{msg_type[0]}^{msg_type[1]}" if len(msg_type) > 1 else str(msg_type[0])
                else:
                    hl7_msg.message_type = str(msg_type)
            if len(msh_segment.fields) > 9:
                hl7_msg.message_control_id = str(msh_segment.fields[9])
            if len(msh_segment.fields) > 11:
                hl7_msg.version = str(msh_segment.fields[11])
        
        # Build parsed data structure
        hl7_msg.parsed_data = cls._build_parsed_data(hl7_msg)
        
        return hl7_msg
    
    @classmethod
    def _parse_segment(cls, segment: str, field_sep: str, encoding_chars: str) -> HL7Segment:
        """Parse a single segment into fields."""
        component_sep = encoding_chars[0]  # ^
        repetition_sep = encoding_chars[1]  # ~
        escape_char = encoding_chars[2]  # \
        subcomponent_sep = encoding_chars[3]  # &
        
        segment_name = segment[:3]
        
        # Special handling for MSH segment
        if segment_name == "MSH":
            # MSH-1 is the field separator, MSH-2 is encoding characters
            rest = segment[4:]
            fields = [field_sep, encoding_chars] + rest.split(field_sep)
        else:
            fields = segment[4:].split(field_sep)  # Skip segment name and separator
        
        # Parse component and repetition structures
        parsed_fields = []
        for field in fields:
            if repetition_sep in field:
                # Handle repetitions
                repetitions = field.split(repetition_sep)
                parsed_reps = [cls._parse_components(rep, component_sep, subcomponent_sep) for rep in repetitions]
                parsed_fields.append(parsed_reps)
            else:
                parsed_fields.append(cls._parse_components(field, component_sep, subcomponent_sep))
        
        return HL7Segment(name=segment_name, fields=parsed_fields, raw=segment)
    
    @classmethod
    def _parse_components(cls, value: str, component_sep: str, subcomponent_sep: str) -> Any:
        """Parse component and subcomponent structures."""
        if component_sep in value:
            components = value.split(component_sep)
            parsed_components = []
            for comp in components:
                if subcomponent_sep in comp:
                    parsed_components.append(comp.split(subcomponent_sep))
                else:
                    parsed_components.append(comp)
            return parsed_components
        elif subcomponent_sep in value:
            return value.split(subcomponent_sep)
        return value
    
    @classmethod
    def _build_parsed_data(cls, msg: HL7Message) -> dict[str, Any]:
        """Build a friendly parsed data structure from segments."""
        data = {}
        
        # Patient data from PID
        if "PID" in msg.segments:
            pid = msg.segments["PID"][0]
            data["patient"] = cls._extract_patient_data(pid)
        
        # Visit data from PV1
        if "PV1" in msg.segments:
            pv1 = msg.segments["PV1"][0]
            data["visit"] = cls._extract_visit_data(pv1)
        
        # Observations from OBX
        if "OBX" in msg.segments:
            data["observations"] = [
                cls._extract_observation_data(obx) 
                for obx in msg.segments["OBX"]
            ]
        
        # Insurance from IN1
        if "IN1" in msg.segments:
            data["insurance"] = [
                cls._extract_insurance_data(in1)
                for in1 in msg.segments["IN1"]
            ]
        
        return data
    
    @classmethod
    def _extract_patient_data(cls, pid: HL7Segment) -> dict[str, Any]:
        """Extract patient data from PID segment."""
        data = {}
        
        # Patient ID (PID-3)
        if len(pid.fields) > 2:
            pid3 = pid.fields[2]
            if isinstance(pid3, list):
                if isinstance(pid3[0], list):
                    data["patient_id"] = pid3[0][0] if pid3[0] else ""
                else:
                    data["patient_id"] = pid3[0]
            else:
                data["patient_id"] = pid3
        
        # Patient name (PID-5)
        if len(pid.fields) > 4:
            name = pid.fields[4]
            if isinstance(name, list):
                data["name"] = {
                    "family": name[0] if len(name) > 0 else "",
                    "given": name[1] if len(name) > 1 else "",
                    "middle": name[2] if len(name) > 2 else "",
                    "suffix": name[3] if len(name) > 3 else "",
                    "prefix": name[4] if len(name) > 4 else "",
                }
            else:
                data["name"] = {"family": name, "given": "", "middle": ""}
        
        # Date of birth (PID-7)
        if len(pid.fields) > 6:
            dob = pid.fields[6]
            if isinstance(dob, list):
                dob = dob[0]
            data["date_of_birth"] = cls._parse_hl7_datetime(str(dob)) if dob else None
        
        # Sex (PID-8)
        if len(pid.fields) > 7:
            data["sex"] = str(pid.fields[7]) if pid.fields[7] else ""
        
        # Address (PID-11)
        if len(pid.fields) > 10:
            addr = pid.fields[10]
            if isinstance(addr, list):
                # Handle repetitions
                if isinstance(addr[0], list):
                    addr = addr[0]
                data["address"] = {
                    "street": addr[0] if len(addr) > 0 else "",
                    "city": addr[2] if len(addr) > 2 else "",
                    "state": addr[3] if len(addr) > 3 else "",
                    "zip": addr[4] if len(addr) > 4 else "",
                    "country": addr[5] if len(addr) > 5 else "",
                }
            else:
                data["address"] = {"street": addr}
        
        # Phone (PID-13)
        if len(pid.fields) > 12:
            phone = pid.fields[12]
            if isinstance(phone, list):
                data["phone"] = phone[0] if phone else ""
            else:
                data["phone"] = phone
        
        # SSN (PID-19)
        if len(pid.fields) > 18:
            data["ssn"] = str(pid.fields[18]) if pid.fields[18] else ""
        
        return data
    
    @classmethod
    def _extract_visit_data(cls, pv1: HL7Segment) -> dict[str, Any]:
        """Extract visit data from PV1 segment."""
        data = {}
        
        # Patient class (PV1-2)
        if len(pv1.fields) > 1:
            data["patient_class"] = str(pv1.fields[1]) if pv1.fields[1] else ""
        
        # Location (PV1-3)
        if len(pv1.fields) > 2:
            loc = pv1.fields[2]
            if isinstance(loc, list):
                data["location"] = {
                    "point_of_care": loc[0] if len(loc) > 0 else "",
                    "room": loc[1] if len(loc) > 1 else "",
                    "bed": loc[2] if len(loc) > 2 else "",
                    "facility": loc[3] if len(loc) > 3 else "",
                }
            else:
                data["location"] = {"point_of_care": loc}
        
        # Attending doctor (PV1-7)
        if len(pv1.fields) > 6:
            doc = pv1.fields[6]
            if isinstance(doc, list):
                data["attending_doctor"] = {
                    "id": doc[0] if len(doc) > 0 else "",
                    "family_name": doc[1] if len(doc) > 1 else "",
                    "given_name": doc[2] if len(doc) > 2 else "",
                }
            else:
                data["attending_doctor"] = {"id": doc}
        
        # Visit number (PV1-19)
        if len(pv1.fields) > 18:
            vn = pv1.fields[18]
            if isinstance(vn, list):
                data["visit_number"] = vn[0] if vn else ""
            else:
                data["visit_number"] = vn
        
        # Admit datetime (PV1-44)
        if len(pv1.fields) > 43:
            admit = pv1.fields[43]
            if isinstance(admit, list):
                admit = admit[0]
            data["admit_datetime"] = cls._parse_hl7_datetime(str(admit)) if admit else None
        
        # Discharge datetime (PV1-45)
        if len(pv1.fields) > 44:
            discharge = pv1.fields[44]
            if isinstance(discharge, list):
                discharge = discharge[0]
            data["discharge_datetime"] = cls._parse_hl7_datetime(str(discharge)) if discharge else None
        
        return data
    
    @classmethod
    def _extract_observation_data(cls, obx: HL7Segment) -> dict[str, Any]:
        """Extract observation data from OBX segment."""
        data = {}
        
        # Set ID (OBX-1)
        if len(obx.fields) > 0:
            data["set_id"] = str(obx.fields[0]) if obx.fields[0] else ""
        
        # Value type (OBX-2)
        if len(obx.fields) > 1:
            data["value_type"] = str(obx.fields[1]) if obx.fields[1] else ""
        
        # Observation identifier (OBX-3)
        if len(obx.fields) > 2:
            obs_id = obx.fields[2]
            if isinstance(obs_id, list):
                data["observation_identifier"] = {
                    "code": obs_id[0] if len(obs_id) > 0 else "",
                    "text": obs_id[1] if len(obs_id) > 1 else "",
                    "coding_system": obs_id[2] if len(obs_id) > 2 else "",
                }
            else:
                data["observation_identifier"] = {"code": obs_id}
        
        # Observation value (OBX-5)
        if len(obx.fields) > 4:
            value = obx.fields[4]
            if isinstance(value, list):
                data["value"] = value[0] if value else ""
            else:
                data["value"] = value
        
        # Units (OBX-6)
        if len(obx.fields) > 5:
            units = obx.fields[5]
            if isinstance(units, list):
                data["units"] = units[0] if units else ""
            else:
                data["units"] = units
        
        # Reference range (OBX-7)
        if len(obx.fields) > 6:
            data["reference_range"] = str(obx.fields[6]) if obx.fields[6] else ""
        
        # Abnormal flags (OBX-8)
        if len(obx.fields) > 7:
            data["abnormal_flags"] = str(obx.fields[7]) if obx.fields[7] else ""
        
        return data
    
    @classmethod
    def _extract_insurance_data(cls, in1: HL7Segment) -> dict[str, Any]:
        """Extract insurance data from IN1 segment."""
        data = {}
        
        # Insurance plan ID (IN1-2)
        if len(in1.fields) > 1:
            plan = in1.fields[1]
            if isinstance(plan, list):
                data["plan_id"] = plan[0] if plan else ""
            else:
                data["plan_id"] = plan
        
        # Insurance company name (IN1-4)
        if len(in1.fields) > 3:
            name = in1.fields[3]
            if isinstance(name, list):
                data["company_name"] = name[0] if name else ""
            else:
                data["company_name"] = name
        
        # Group number (IN1-8)
        if len(in1.fields) > 7:
            data["group_number"] = str(in1.fields[7]) if in1.fields[7] else ""
        
        # Policy number (IN1-36)
        if len(in1.fields) > 35:
            data["policy_number"] = str(in1.fields[35]) if in1.fields[35] else ""
        
        return data
    
    @classmethod
    def _parse_hl7_datetime(cls, value: str) -> Optional[str]:
        """Parse HL7 datetime format (YYYYMMDDHHMMSS) to ISO format."""
        if not value:
            return None
        
        value = value.strip()
        
        # Remove timezone offset if present
        if "+" in value:
            value = value.split("+")[0]
        elif "-" in value and len(value) > 8:
            # Check if it's a timezone, not part of date
            parts = value.rsplit("-", 1)
            if len(parts[1]) in [4, 2]:
                value = parts[0]
        
        try:
            if len(value) >= 14:
                dt = datetime.strptime(value[:14], "%Y%m%d%H%M%S")
            elif len(value) >= 12:
                dt = datetime.strptime(value[:12], "%Y%m%d%H%M")
            elif len(value) >= 8:
                dt = datetime.strptime(value[:8], "%Y%m%d")
            else:
                return value  # Return as-is if format unknown
            
            return dt.isoformat()
        except ValueError:
            return value  # Return as-is if parsing fails
    
    @classmethod
    def get_segment_field(cls, msg: HL7Message, segment_name: str, field_index: int, occurrence: int = 0) -> Any:
        """
        Get a specific field from a segment.
        
        Args:
            msg: The parsed HL7Message
            segment_name: Segment name (e.g., "PID")
            field_index: 1-based field index (as per HL7 spec)
            occurrence: Segment occurrence (0-based, for repeating segments)
            
        Returns:
            Field value or None if not found
        """
        if segment_name not in msg.segments:
            return None
        
        segments = msg.segments[segment_name]
        if occurrence >= len(segments):
            return None
        
        segment = segments[occurrence]
        
        # Adjust for 1-based indexing in HL7 spec
        # Also adjust for MSH which has special first 2 fields
        if segment_name == "MSH":
            idx = field_index - 1
        else:
            idx = field_index - 1
        
        if idx < 0 or idx >= len(segment.fields):
            return None
        
        return segment.fields[idx]
    
    @classmethod
    def to_dict(cls, msg: HL7Message) -> dict[str, Any]:
        """Convert HL7Message to a JSON-serializable dictionary."""
        return {
            "message_type": msg.message_type,
            "message_control_id": msg.message_control_id,
            "version": msg.version,
            "encoding_characters": msg.encoding_characters,
            "segments": {
                name: [
                    {"name": seg.name, "fields": seg.fields}
                    for seg in segments
                ]
                for name, segments in msg.segments.items()
            },
            "parsed_data": msg.parsed_data,
        }

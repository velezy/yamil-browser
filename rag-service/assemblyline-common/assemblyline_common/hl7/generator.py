"""
HL7 v2.x Message Generator

Generates HL7 v2.x messages from structured Python data.
"""

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4
import random
import string
import logging

logger = logging.getLogger(__name__)


class HL7Generator:
    """
    Generator for HL7 v2.x messages.
    
    Usage:
        message = HL7Generator.create_adt_a01(
            patient_data={...},
            visit_data={...},
            config={...}
        )
    """
    
    # Standard encoding characters
    FIELD_SEP = "|"
    ENCODING_CHARS = "^~\\&"
    SEGMENT_TERMINATOR = "\r"
    
    @classmethod
    def create_adt_a01(
        cls,
        patient_data: dict[str, Any],
        visit_data: dict[str, Any],
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an ADT^A01 (Admit/Visit Notification) message.
        
        Args:
            patient_data: Patient demographic information
            visit_data: Visit/encounter information
            config: Optional configuration (sending app, facility, etc.)
            
        Returns:
            HL7 message string
        """
        config = config or {}
        
        segments = []
        
        # MSH segment
        segments.append(cls._create_msh_segment(
            message_type="ADT",
            trigger_event="A01",
            config=config
        ))
        
        # EVN segment (Event Type)
        segments.append(cls._create_evn_segment(
            event_type="A01",
            recorded_datetime=visit_data.get("admit_datetime")
        ))
        
        # PID segment
        segments.append(cls._create_pid_segment(patient_data))
        
        # PV1 segment
        segments.append(cls._create_pv1_segment(visit_data))
        
        return cls.SEGMENT_TERMINATOR.join(segments) + cls.SEGMENT_TERMINATOR
    
    @classmethod
    def create_adt_a04(
        cls,
        patient_data: dict[str, Any],
        visit_data: dict[str, Any],
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an ADT^A04 (Register a Patient) message.

        Args:
            patient_data: Patient demographic information
            visit_data: Visit/encounter information
            config: Optional configuration

        Returns:
            HL7 message string
        """
        config = config or {}

        segments = []

        # MSH segment
        segments.append(cls._create_msh_segment(
            message_type="ADT",
            trigger_event="A04",
            config=config
        ))

        # EVN segment
        segments.append(cls._create_evn_segment(
            event_type="A04",
            recorded_datetime=visit_data.get("admit_datetime")
        ))

        # PID segment
        segments.append(cls._create_pid_segment(patient_data))

        # PV1 segment
        segments.append(cls._create_pv1_segment(visit_data))

        return cls.SEGMENT_TERMINATOR.join(segments) + cls.SEGMENT_TERMINATOR

    @classmethod
    def create_adt_a08(
        cls,
        patient_data: dict[str, Any],
        visit_data: dict[str, Any],
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an ADT^A08 (Update Patient Information) message.

        Args:
            patient_data: Patient demographic information
            visit_data: Visit/encounter information
            config: Optional configuration

        Returns:
            HL7 message string
        """
        config = config or {}

        segments = []

        # MSH segment
        segments.append(cls._create_msh_segment(
            message_type="ADT",
            trigger_event="A08",
            config=config
        ))

        # EVN segment
        segments.append(cls._create_evn_segment(event_type="A08"))

        # PID segment
        segments.append(cls._create_pid_segment(patient_data))

        # PV1 segment
        segments.append(cls._create_pv1_segment(visit_data))

        return cls.SEGMENT_TERMINATOR.join(segments) + cls.SEGMENT_TERMINATOR

    @classmethod
    def create_adt_a31(
        cls,
        patient_data: dict[str, Any],
        visit_data: dict[str, Any],
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an ADT^A31 (Update Person Information) message.

        Args:
            patient_data: Patient demographic information
            visit_data: Visit/encounter information (optional for A31)
            config: Optional configuration

        Returns:
            HL7 message string
        """
        config = config or {}
        visit_data = visit_data or {}

        segments = []

        # MSH segment
        segments.append(cls._create_msh_segment(
            message_type="ADT",
            trigger_event="A31",
            config=config
        ))

        # EVN segment
        segments.append(cls._create_evn_segment(event_type="A31"))

        # PID segment
        segments.append(cls._create_pid_segment(patient_data))

        # PV1 segment (optional but included for completeness)
        segments.append(cls._create_pv1_segment(visit_data))

        return cls.SEGMENT_TERMINATOR.join(segments) + cls.SEGMENT_TERMINATOR
    
    @classmethod
    def create_adt_a03(
        cls,
        patient_data: dict[str, Any],
        visit_data: dict[str, Any],
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an ADT^A03 (Discharge/End Visit) message.
        
        Args:
            patient_data: Patient demographic information
            visit_data: Visit/encounter information
            config: Optional configuration
            
        Returns:
            HL7 message string
        """
        config = config or {}
        
        segments = []
        
        # MSH segment
        segments.append(cls._create_msh_segment(
            message_type="ADT",
            trigger_event="A03",
            config=config
        ))
        
        # EVN segment
        segments.append(cls._create_evn_segment(
            event_type="A03",
            recorded_datetime=visit_data.get("discharge_datetime")
        ))
        
        # PID segment
        segments.append(cls._create_pid_segment(patient_data))
        
        # PV1 segment
        segments.append(cls._create_pv1_segment(visit_data))
        
        return cls.SEGMENT_TERMINATOR.join(segments) + cls.SEGMENT_TERMINATOR
    
    @classmethod
    def create_oru_r01(
        cls,
        patient_data: dict[str, Any],
        observations: list[dict[str, Any]],
        order_data: Optional[dict[str, Any]] = None,
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an ORU^R01 (Observation Result) message.
        
        Args:
            patient_data: Patient demographic information
            observations: List of observation data
            order_data: Order information (optional)
            config: Optional configuration
            
        Returns:
            HL7 message string
        """
        config = config or {}
        order_data = order_data or {}
        
        segments = []
        
        # MSH segment
        segments.append(cls._create_msh_segment(
            message_type="ORU",
            trigger_event="R01",
            config=config
        ))
        
        # PID segment
        segments.append(cls._create_pid_segment(patient_data))
        
        # OBR segment (Order)
        segments.append(cls._create_obr_segment(order_data))
        
        # OBX segments (Observations)
        for i, obs in enumerate(observations, start=1):
            segments.append(cls._create_obx_segment(obs, set_id=i))
        
        return cls.SEGMENT_TERMINATOR.join(segments) + cls.SEGMENT_TERMINATOR
    
    @classmethod
    def _create_msh_segment(
        cls,
        message_type: str,
        trigger_event: str,
        config: dict[str, Any]
    ) -> str:
        """Create MSH (Message Header) segment."""
        now = datetime.now(timezone.utc)
        
        fields = [
            "MSH",
            cls.ENCODING_CHARS,
            config.get("sending_application", "LOGIC_WEAVER"),
            config.get("sending_facility", "MW_FACILITY"),
            config.get("receiving_application", ""),
            config.get("receiving_facility", ""),
            now.strftime("%Y%m%d%H%M%S"),
            "",  # Security
            f"{message_type}^{trigger_event}",
            config.get("message_control_id", cls._generate_control_id()),
            config.get("processing_id", "P"),  # P=Production, D=Debug, T=Training
            config.get("version", "2.5"),
        ]
        
        return cls.FIELD_SEP.join(fields)
    
    @classmethod
    def _create_evn_segment(
        cls,
        event_type: str,
        recorded_datetime: Optional[str] = None
    ) -> str:
        """Create EVN (Event Type) segment."""
        now = datetime.now(timezone.utc)
        recorded = cls._format_datetime(recorded_datetime) if recorded_datetime else now.strftime("%Y%m%d%H%M%S")
        
        fields = [
            "EVN",
            event_type,
            recorded,
            "",  # Planned event date/time
            "",  # Event reason code
            "",  # Operator ID
            now.strftime("%Y%m%d%H%M%S"),  # Event occurred
        ]
        
        return cls.FIELD_SEP.join(fields)
    
    @classmethod
    def _create_pid_segment(cls, patient_data: dict[str, Any]) -> str:
        """Create PID (Patient Identification) segment."""
        name = patient_data.get("name", {})
        if isinstance(name, str):
            name = {"family": name, "given": ""}
        
        address = patient_data.get("address", {})
        if isinstance(address, str):
            address = {"street": address}
        
        # Format patient name: Family^Given^Middle^Suffix^Prefix
        name_str = f"{name.get('family', '')}^{name.get('given', '')}^{name.get('middle', '')}^{name.get('suffix', '')}^{name.get('prefix', '')}"
        
        # Format address: Street^Other^City^State^Zip^Country
        address_str = f"{address.get('street', '')}^^{address.get('city', '')}^{address.get('state', '')}^{address.get('zip', '')}^{address.get('country', '')}"
        
        # Format DOB
        dob = patient_data.get("date_of_birth", "")
        if dob and isinstance(dob, str):
            dob = cls._format_datetime(dob, date_only=True)
        
        fields = [
            "PID",
            "1",  # Set ID
            "",  # Patient ID (external)
            patient_data.get("patient_id", "") + "^^^MRN",  # Patient identifier list
            "",  # Alternate patient ID
            name_str,
            patient_data.get("mother_maiden_name", ""),
            dob,
            patient_data.get("sex", ""),
            "",  # Patient alias
            patient_data.get("race", ""),
            address_str,
            "",  # County code
            patient_data.get("phone", ""),
            "",  # Business phone
            "",  # Primary language
            patient_data.get("marital_status", ""),
            "",  # Religion
            patient_data.get("account_number", ""),
            patient_data.get("ssn", ""),
        ]
        
        return cls.FIELD_SEP.join(fields)
    
    @classmethod
    def _create_pv1_segment(cls, visit_data: dict[str, Any]) -> str:
        """Create PV1 (Patient Visit) segment."""
        location = visit_data.get("location", {})
        if isinstance(location, str):
            location = {"point_of_care": location}
        
        # Format location: PointOfCare^Room^Bed^Facility
        location_str = f"{location.get('point_of_care', '')}^{location.get('room', '')}^{location.get('bed', '')}^{location.get('facility', '')}"
        
        doctor = visit_data.get("attending_doctor", {})
        if isinstance(doctor, str):
            doctor = {"id": doctor}
        
        # Format doctor: ID^Family^Given
        doctor_str = f"{doctor.get('id', '')}^{doctor.get('family_name', '')}^{doctor.get('given_name', '')}"
        
        # Format datetimes
        admit_dt = cls._format_datetime(visit_data.get("admit_datetime", ""))
        discharge_dt = cls._format_datetime(visit_data.get("discharge_datetime", ""))
        
        fields = [
            "PV1",
            "1",  # Set ID
            visit_data.get("patient_class", "I"),  # I=Inpatient, O=Outpatient, E=Emergency
            location_str,
            visit_data.get("admission_type", ""),
            "",  # Preadmit number
            "",  # Prior patient location
            doctor_str,
            "",  # Referring doctor
            "",  # Consulting doctor
            visit_data.get("hospital_service", ""),
            "",  # Temporary location
            "",  # Preadmit test indicator
            "",  # Readmission indicator
            visit_data.get("admit_source", ""),
            "",  # Ambulatory status
            "",  # VIP indicator
            "",  # Admitting doctor
            visit_data.get("patient_type", ""),
            visit_data.get("visit_number", ""),
            "",  # Financial class
        ]
        
        # Pad to field 44 (admit datetime)
        while len(fields) < 44:
            fields.append("")
        
        fields.append(admit_dt)  # PV1-44: Admit Date/Time
        fields.append(discharge_dt)  # PV1-45: Discharge Date/Time
        
        return cls.FIELD_SEP.join(fields)
    
    @classmethod
    def _create_obr_segment(cls, order_data: dict[str, Any]) -> str:
        """Create OBR (Observation Request) segment."""
        service = order_data.get("universal_service_id", {})
        if isinstance(service, str):
            service = {"code": service}
        
        # Format service: Code^Text^CodingSystem
        service_str = f"{service.get('code', '')}^{service.get('text', '')}^{service.get('coding_system', '')}"
        
        fields = [
            "OBR",
            "1",  # Set ID
            order_data.get("placer_order_number", ""),
            order_data.get("filler_order_number", ""),
            service_str,
            "",  # Priority
            cls._format_datetime(order_data.get("requested_datetime", "")),
            cls._format_datetime(order_data.get("observation_datetime", "")),
        ]
        
        return cls.FIELD_SEP.join(fields)
    
    @classmethod
    def _create_obx_segment(cls, obs_data: dict[str, Any], set_id: int = 1) -> str:
        """Create OBX (Observation Result) segment."""
        obs_id = obs_data.get("observation_identifier", {})
        if isinstance(obs_id, str):
            obs_id = {"code": obs_id}
        
        # Format observation identifier: Code^Text^CodingSystem
        obs_id_str = f"{obs_id.get('code', '')}^{obs_id.get('text', '')}^{obs_id.get('coding_system', '')}"
        
        fields = [
            "OBX",
            str(set_id),
            obs_data.get("value_type", "ST"),  # ST=String, NM=Numeric, DT=Date
            obs_id_str,
            obs_data.get("sub_id", ""),
            str(obs_data.get("value", "")),
            obs_data.get("units", ""),
            obs_data.get("reference_range", ""),
            obs_data.get("abnormal_flags", ""),
            "",  # Probability
            "",  # Nature of abnormal test
            obs_data.get("result_status", "F"),  # F=Final, P=Preliminary
        ]
        
        return cls.FIELD_SEP.join(fields)
    
    @classmethod
    def _generate_control_id(cls) -> str:
        """Generate a unique message control ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        random_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        return f"MW{timestamp}{random_suffix}"
    
    @classmethod
    def _format_datetime(cls, value: Optional[str], date_only: bool = False) -> str:
        """Format a datetime string to HL7 format."""
        if not value:
            return ""
        
        try:
            # Try parsing ISO format
            if "T" in value:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif len(value) == 10:  # YYYY-MM-DD
                dt = datetime.strptime(value, "%Y-%m-%d")
            elif len(value) == 8:  # YYYYMMDD
                return value  # Already in HL7 format
            elif len(value) >= 14:  # YYYYMMDDHHMMSS
                return value[:14]  # Already in HL7 format
            else:
                return value
            
            if date_only:
                return dt.strftime("%Y%m%d")
            return dt.strftime("%Y%m%d%H%M%S")
        
        except (ValueError, AttributeError):
            return str(value)
    
    @classmethod
    def from_json(
        cls,
        message_type: str,
        data: dict[str, Any],
        config: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create an HL7 message from a JSON/dict structure.
        
        Args:
            message_type: Message type (e.g., "ADT^A01", "ORU^R01")
            data: Message data dictionary
            config: Optional configuration
            
        Returns:
            HL7 message string
        """
        msg_type, trigger = message_type.split("^") if "^" in message_type else (message_type, "")
        
        patient_data = data.get("patient", {})
        visit_data = data.get("visit", {})
        
        if msg_type == "ADT":
            if trigger == "A01":
                return cls.create_adt_a01(patient_data, visit_data, config)
            elif trigger == "A03":
                return cls.create_adt_a03(patient_data, visit_data, config)
            elif trigger == "A04":
                return cls.create_adt_a04(patient_data, visit_data, config)
            elif trigger == "A08":
                return cls.create_adt_a08(patient_data, visit_data, config)
            elif trigger == "A31":
                return cls.create_adt_a31(patient_data, visit_data, config)

        elif msg_type == "ORU" and trigger == "R01":
            observations = data.get("observations", [])
            order_data = data.get("order", {})
            return cls.create_oru_r01(patient_data, observations, order_data, config)
        
        raise ValueError(f"Unsupported message type: {message_type}")

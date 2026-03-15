"""
HL7 PHI Masking Service - Segment-Aware De-identification

Provides HL7-aware PHI masking that understands the structure of HL7 messages
and knows which segments and fields contain PHI.

Key PHI-containing segments:
- PID (Patient Identification)
- NK1 (Next of Kin)
- GT1 (Guarantor)
- IN1/IN2 (Insurance)
- PV1 (Patient Visit)
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set, Tuple
from enum import Enum

from pydantic import BaseModel, Field

from assemblyline_common.phi.masking import (
    PHIType,
    PHIDetection,
    PHIMaskingService,
    PHIConfig,
)

logger = logging.getLogger(__name__)


# ============================================================================
# HL7 Field Definitions
# ============================================================================

@dataclass
class HL7FieldInfo:
    """Information about an HL7 field."""

    segment: str
    field_index: int  # 1-based index
    name: str
    phi_type: PHIType
    component_indices: Optional[List[int]] = None  # If only certain components are PHI


# Known PHI fields in HL7 v2.x
HL7_PHI_FIELDS: List[HL7FieldInfo] = [
    # PID Segment (Patient Identification)
    HL7FieldInfo("PID", 3, "Patient ID", PHIType.MRN),
    HL7FieldInfo("PID", 4, "Alternate Patient ID", PHIType.MRN),
    HL7FieldInfo("PID", 5, "Patient Name", PHIType.NAME),
    HL7FieldInfo("PID", 6, "Mother's Maiden Name", PHIType.NAME),
    HL7FieldInfo("PID", 7, "Date of Birth", PHIType.DATE),
    HL7FieldInfo("PID", 9, "Patient Alias", PHIType.NAME),
    HL7FieldInfo("PID", 11, "Patient Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("PID", 13, "Phone Number - Home", PHIType.PHONE),
    HL7FieldInfo("PID", 14, "Phone Number - Business", PHIType.PHONE),
    HL7FieldInfo("PID", 19, "SSN Number", PHIType.SSN),
    HL7FieldInfo("PID", 20, "Driver's License", PHIType.LICENSE_NUMBER),
    HL7FieldInfo("PID", 21, "Mother's Identifier", PHIType.MRN),
    HL7FieldInfo("PID", 26, "Citizenship", PHIType.GEOGRAPHIC),

    # NK1 Segment (Next of Kin)
    HL7FieldInfo("NK1", 2, "NK Name", PHIType.NAME),
    HL7FieldInfo("NK1", 4, "NK Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("NK1", 5, "NK Phone", PHIType.PHONE),
    HL7FieldInfo("NK1", 6, "NK Business Phone", PHIType.PHONE),
    HL7FieldInfo("NK1", 12, "NK SSN", PHIType.SSN),
    HL7FieldInfo("NK1", 30, "NK Name", PHIType.NAME),
    HL7FieldInfo("NK1", 32, "NK Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("NK1", 33, "NK Phone", PHIType.PHONE),

    # GT1 Segment (Guarantor)
    HL7FieldInfo("GT1", 3, "Guarantor Name", PHIType.NAME),
    HL7FieldInfo("GT1", 5, "Guarantor Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("GT1", 6, "Guarantor Phone Home", PHIType.PHONE),
    HL7FieldInfo("GT1", 7, "Guarantor Phone Business", PHIType.PHONE),
    HL7FieldInfo("GT1", 8, "Guarantor DOB", PHIType.DATE),
    HL7FieldInfo("GT1", 12, "Guarantor SSN", PHIType.SSN),
    HL7FieldInfo("GT1", 16, "Guarantor Employer Name", PHIType.NAME),
    HL7FieldInfo("GT1", 17, "Guarantor Employer Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("GT1", 18, "Guarantor Employer Phone", PHIType.PHONE),

    # IN1 Segment (Insurance)
    HL7FieldInfo("IN1", 2, "Insurance Plan ID", PHIType.HEALTH_PLAN_ID),
    HL7FieldInfo("IN1", 4, "Insurance Company Name", PHIType.OTHER_ID),
    HL7FieldInfo("IN1", 5, "Insurance Company Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("IN1", 7, "Insurance Phone", PHIType.PHONE),
    HL7FieldInfo("IN1", 8, "Group Number", PHIType.HEALTH_PLAN_ID),
    HL7FieldInfo("IN1", 9, "Group Name", PHIType.OTHER_ID),
    HL7FieldInfo("IN1", 10, "Insured Group Emp ID", PHIType.OTHER_ID),
    HL7FieldInfo("IN1", 12, "Plan Effective Date", PHIType.DATE),
    HL7FieldInfo("IN1", 13, "Plan Expiration Date", PHIType.DATE),
    HL7FieldInfo("IN1", 16, "Insured Name", PHIType.NAME),
    HL7FieldInfo("IN1", 18, "Insured DOB", PHIType.DATE),
    HL7FieldInfo("IN1", 19, "Insured Address", PHIType.GEOGRAPHIC),
    HL7FieldInfo("IN1", 36, "Policy Number", PHIType.HEALTH_PLAN_ID),
    HL7FieldInfo("IN1", 49, "Insured ID", PHIType.HEALTH_PLAN_ID),

    # IN2 Segment (Insurance Additional)
    HL7FieldInfo("IN2", 1, "Insured Employee ID", PHIType.OTHER_ID),
    HL7FieldInfo("IN2", 2, "Insured SSN", PHIType.SSN),
    HL7FieldInfo("IN2", 6, "Medicare ID", PHIType.HEALTH_PLAN_ID),
    HL7FieldInfo("IN2", 8, "Medicaid ID", PHIType.HEALTH_PLAN_ID),
    HL7FieldInfo("IN2", 25, "Policy Holder Name", PHIType.NAME),
    HL7FieldInfo("IN2", 52, "Insured Phone Home", PHIType.PHONE),
    HL7FieldInfo("IN2", 61, "Patient Email", PHIType.EMAIL),
    HL7FieldInfo("IN2", 63, "Insured Phone Mobile", PHIType.PHONE),

    # PV1 Segment (Patient Visit)
    HL7FieldInfo("PV1", 7, "Attending Doctor", PHIType.NAME),
    HL7FieldInfo("PV1", 8, "Referring Doctor", PHIType.NAME),
    HL7FieldInfo("PV1", 9, "Consulting Doctor", PHIType.NAME),
    HL7FieldInfo("PV1", 17, "Admitting Doctor", PHIType.NAME),
    HL7FieldInfo("PV1", 19, "Visit Number", PHIType.ACCOUNT_NUMBER),
    HL7FieldInfo("PV1", 44, "Admit Date/Time", PHIType.DATE),
    HL7FieldInfo("PV1", 45, "Discharge Date/Time", PHIType.DATE),
    HL7FieldInfo("PV1", 52, "Other Provider", PHIType.NAME),

    # ACC Segment (Accident)
    HL7FieldInfo("ACC", 1, "Accident Date/Time", PHIType.DATE),

    # DG1 Segment (Diagnosis) - dates only, not codes
    HL7FieldInfo("DG1", 5, "Diagnosis Date/Time", PHIType.DATE),

    # OBR Segment (Observation Request)
    HL7FieldInfo("OBR", 7, "Observation Date/Time", PHIType.DATE),
    HL7FieldInfo("OBR", 8, "Observation End Date/Time", PHIType.DATE),
    HL7FieldInfo("OBR", 14, "Specimen Received Date/Time", PHIType.DATE),
    HL7FieldInfo("OBR", 16, "Ordering Provider", PHIType.NAME),
    HL7FieldInfo("OBR", 22, "Results Rpt/Status Chng", PHIType.DATE),

    # OBX Segment (Observation Result)
    HL7FieldInfo("OBX", 14, "Date/Time of Observation", PHIType.DATE),
    HL7FieldInfo("OBX", 16, "Responsible Observer", PHIType.NAME),
]


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class HL7SegmentConfig:
    """Configuration for HL7 segment masking."""

    # Field delimiter (default |)
    field_delimiter: str = "|"

    # Component delimiter (default ^)
    component_delimiter: str = "^"

    # Subcomponent delimiter (default &)
    subcomponent_delimiter: str = "&"

    # Repetition delimiter (default ~)
    repetition_delimiter: str = "~"

    # Escape character (default \)
    escape_char: str = "\\"

    # Segments to skip entirely (never mask)
    skip_segments: Set[str] = field(default_factory=lambda: {"MSH", "EVN"})

    # Additional PHI fields (beyond defaults)
    additional_phi_fields: List[HL7FieldInfo] = field(default_factory=list)

    # Fields to explicitly exclude from masking
    exclude_fields: Set[Tuple[str, int]] = field(default_factory=set)


# ============================================================================
# HL7 Masking Result
# ============================================================================

class HL7MaskingResult(BaseModel):
    """Result of HL7 message masking."""

    original_message: str
    masked_message: str
    segments_processed: int
    fields_masked: int
    detections: List[PHIDetection] = Field(default_factory=list)
    phi_types_found: List[PHIType] = Field(default_factory=list)


# ============================================================================
# HL7 PHI Masking Service
# ============================================================================

class HL7PHIMaskingService:
    """
    Service for masking PHI in HL7 messages.

    Understands HL7 message structure and masks PHI based on
    known field positions in standard HL7 segments.
    """

    def __init__(
        self,
        config: Optional[HL7SegmentConfig] = None,
        phi_config: Optional[PHIConfig] = None
    ):
        self.config = config or HL7SegmentConfig()
        self.phi_service = PHIMaskingService(phi_config)

        # Build lookup for PHI fields
        self._phi_field_map: Dict[str, Dict[int, HL7FieldInfo]] = {}
        for field_info in HL7_PHI_FIELDS + self.config.additional_phi_fields:
            if field_info.segment not in self._phi_field_map:
                self._phi_field_map[field_info.segment] = {}
            self._phi_field_map[field_info.segment][field_info.field_index] = field_info

    def mask_message(self, message: str) -> HL7MaskingResult:
        """
        Mask PHI in an HL7 message.

        Args:
            message: Raw HL7 message string

        Returns:
            HL7MaskingResult with masked message and detection details
        """
        if not message:
            return HL7MaskingResult(
                original_message=message,
                masked_message=message,
                segments_processed=0,
                fields_masked=0,
            )

        # Split into segments
        segments = message.strip().split("\r")
        if not segments:
            segments = message.strip().split("\n")

        masked_segments = []
        all_detections: List[PHIDetection] = []
        total_fields_masked = 0

        for segment in segments:
            if not segment.strip():
                masked_segments.append(segment)
                continue

            masked_segment, detections, fields_masked = self._mask_segment(segment)
            masked_segments.append(masked_segment)
            all_detections.extend(detections)
            total_fields_masked += fields_masked

        # Reconstruct message
        masked_message = "\r".join(masked_segments)
        phi_types_found = list(set(d.phi_type for d in all_detections))

        logger.info(
            "Masked HL7 message",
            extra={
                "event_type": "phi.hl7_masked",
                "segments_processed": len(segments),
                "fields_masked": total_fields_masked,
                "detection_count": len(all_detections),
            }
        )

        return HL7MaskingResult(
            original_message=message,
            masked_message=masked_message,
            segments_processed=len(segments),
            fields_masked=total_fields_masked,
            detections=all_detections,
            phi_types_found=phi_types_found,
        )

    def _mask_segment(
        self,
        segment: str
    ) -> Tuple[str, List[PHIDetection], int]:
        """
        Mask PHI in a single HL7 segment.

        Returns: (masked_segment, detections, fields_masked_count)
        """
        detections: List[PHIDetection] = []
        fields_masked = 0

        # Parse segment
        fields = segment.split(self.config.field_delimiter)
        if not fields:
            return segment, detections, fields_masked

        segment_type = fields[0][:3] if fields else ""

        # Skip certain segments
        if segment_type in self.config.skip_segments:
            return segment, detections, fields_masked

        # Check if we have PHI field definitions for this segment
        if segment_type not in self._phi_field_map:
            # Unknown segment - scan all fields for PHI
            return self._scan_unknown_segment(segment, segment_type)

        phi_fields = self._phi_field_map[segment_type]
        masked_fields = []

        for i, field_value in enumerate(fields):
            field_index = i  # 0-based for MSH, 1-based for others

            # MSH has special indexing (MSH-1 is the field separator)
            if segment_type == "MSH" and i == 0:
                masked_fields.append(field_value)
                continue

            # Check if this field should be excluded
            if (segment_type, field_index) in self.config.exclude_fields:
                masked_fields.append(field_value)
                continue

            # Check if this is a known PHI field
            if field_index in phi_fields:
                field_info = phi_fields[field_index]
                masked_value = self._mask_field_value(
                    field_value,
                    field_info,
                    f"{segment_type}.{field_index}"
                )

                if masked_value != field_value:
                    fields_masked += 1
                    detections.append(PHIDetection(
                        phi_type=field_info.phi_type,
                        original_value=field_value,
                        masked_value=masked_value,
                        confidence=1.0,  # High confidence for known fields
                        field_path=f"{segment_type}.{field_index}",
                    ))
                    masked_fields.append(masked_value)
                else:
                    masked_fields.append(field_value)
            else:
                masked_fields.append(field_value)

        return (
            self.config.field_delimiter.join(masked_fields),
            detections,
            fields_masked
        )

    def _mask_field_value(
        self,
        value: str,
        field_info: HL7FieldInfo,
        field_path: str
    ) -> str:
        """
        Mask a field value based on its PHI type.

        Handles repetitions and components.
        """
        if not value or value == '""':
            return value

        # Handle repetitions
        if self.config.repetition_delimiter in value:
            repetitions = value.split(self.config.repetition_delimiter)
            masked_reps = [
                self._mask_single_value(rep, field_info)
                for rep in repetitions
            ]
            return self.config.repetition_delimiter.join(masked_reps)

        return self._mask_single_value(value, field_info)

    def _mask_single_value(self, value: str, field_info: HL7FieldInfo) -> str:
        """Mask a single value (no repetitions)."""

        if not value:
            return value

        # If specific components should be masked
        if field_info.component_indices:
            components = value.split(self.config.component_delimiter)
            for idx in field_info.component_indices:
                if idx < len(components):
                    components[idx] = self.phi_service.config.mask_formats.get(
                        field_info.phi_type, "[REDACTED]"
                    )
            return self.config.component_delimiter.join(components)

        # Mask entire value
        mask_format = self.phi_service.config.mask_formats.get(
            field_info.phi_type, "[REDACTED]"
        )

        # For composite fields (with components), mask all non-empty components
        if self.config.component_delimiter in value:
            components = value.split(self.config.component_delimiter)
            masked_components = [
                mask_format if comp and comp.strip() else comp
                for comp in components
            ]
            return self.config.component_delimiter.join(masked_components)

        return mask_format

    def _scan_unknown_segment(
        self,
        segment: str,
        segment_type: str
    ) -> Tuple[str, List[PHIDetection], int]:
        """
        Scan an unknown segment for PHI patterns.

        For segments not in our known list, we scan all fields
        using pattern-based detection.
        """
        fields = segment.split(self.config.field_delimiter)
        masked_fields = []
        all_detections: List[PHIDetection] = []
        fields_masked = 0

        for i, field_value in enumerate(fields):
            if i == 0:
                # First field is segment type
                masked_fields.append(field_value)
                continue

            # Use pattern-based detection
            result = self.phi_service.mask_text(field_value)
            if result.detections:
                fields_masked += 1
                for d in result.detections:
                    d.field_path = f"{segment_type}.{i}"
                all_detections.extend(result.detections)
                masked_fields.append(result.masked_text)
            else:
                masked_fields.append(field_value)

        return (
            self.config.field_delimiter.join(masked_fields),
            all_detections,
            fields_masked
        )

    def get_phi_fields_for_segment(self, segment_type: str) -> List[HL7FieldInfo]:
        """Get list of known PHI fields for a segment type."""
        return [
            field_info
            for field_info in HL7_PHI_FIELDS
            if field_info.segment == segment_type
        ]


# ============================================================================
# Singleton Factory
# ============================================================================

_hl7_phi_masking_service: Optional[HL7PHIMaskingService] = None


async def get_hl7_phi_masking_service(
    config: Optional[HL7SegmentConfig] = None,
    phi_config: Optional[PHIConfig] = None
) -> HL7PHIMaskingService:
    """Get singleton instance of HL7 PHI masking service."""
    global _hl7_phi_masking_service

    if _hl7_phi_masking_service is None:
        _hl7_phi_masking_service = HL7PHIMaskingService(config, phi_config)
        logger.info(
            "Initialized HL7 PHI masking service",
            extra={
                "event_type": "phi.hl7_service_initialized",
                "known_segments": list(_hl7_phi_masking_service._phi_field_map.keys()),
            }
        )

    return _hl7_phi_masking_service

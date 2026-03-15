"""
PHI Masking Service - HIPAA Safe Harbor De-identification

Implements detection and masking of the 18 HIPAA identifier categories:
1.  Names
2.  Geographic subdivisions smaller than state
3.  Dates (except year) related to an individual
4.  Telephone numbers
5.  Fax numbers
6.  Email addresses
7.  Social Security numbers
8.  Medical record numbers
9.  Health plan beneficiary numbers
10. Account numbers
11. Certificate/license numbers
12. Vehicle identifiers and serial numbers
13. Device identifiers and serial numbers
14. Web URLs
15. IP addresses
16. Biometric identifiers
17. Full-face photographs
18. Any other unique identifying number/code
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Optional, List, Dict, Any, Pattern, Tuple, Set
from uuid import UUID

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ============================================================================
# PHI Types (HIPAA 18 Identifiers)
# ============================================================================

class PHIType(str, Enum):
    """HIPAA Safe Harbor 18 identifier categories."""

    NAME = "name"
    GEOGRAPHIC = "geographic"
    DATE = "date"
    PHONE = "phone"
    FAX = "fax"
    EMAIL = "email"
    SSN = "ssn"
    MRN = "mrn"  # Medical Record Number
    HEALTH_PLAN_ID = "health_plan_id"
    ACCOUNT_NUMBER = "account_number"
    LICENSE_NUMBER = "license_number"
    VEHICLE_ID = "vehicle_id"
    DEVICE_ID = "device_id"
    URL = "url"
    IP_ADDRESS = "ip_address"
    BIOMETRIC = "biometric"
    PHOTO = "photo"
    OTHER_ID = "other_id"


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class PHIConfig:
    """Configuration for PHI detection and masking."""

    # Masking characters
    mask_char: str = "*"
    mask_length: int = 8  # Fixed length for masked values

    # Masking formats per type
    mask_formats: Dict[PHIType, str] = field(default_factory=lambda: {
        PHIType.NAME: "[REDACTED_NAME]",
        PHIType.GEOGRAPHIC: "[REDACTED_ADDRESS]",
        PHIType.DATE: "[REDACTED_DATE]",
        PHIType.PHONE: "[REDACTED_PHONE]",
        PHIType.FAX: "[REDACTED_FAX]",
        PHIType.EMAIL: "[REDACTED_EMAIL]",
        PHIType.SSN: "[REDACTED_SSN]",
        PHIType.MRN: "[REDACTED_MRN]",
        PHIType.HEALTH_PLAN_ID: "[REDACTED_HPID]",
        PHIType.ACCOUNT_NUMBER: "[REDACTED_ACCT]",
        PHIType.LICENSE_NUMBER: "[REDACTED_LICENSE]",
        PHIType.VEHICLE_ID: "[REDACTED_VIN]",
        PHIType.DEVICE_ID: "[REDACTED_DEVICE]",
        PHIType.URL: "[REDACTED_URL]",
        PHIType.IP_ADDRESS: "[REDACTED_IP]",
        PHIType.BIOMETRIC: "[REDACTED_BIOMETRIC]",
        PHIType.PHOTO: "[REDACTED_PHOTO]",
        PHIType.OTHER_ID: "[REDACTED_ID]",
    })

    # Types to detect (all by default)
    enabled_types: Set[PHIType] = field(default_factory=lambda: set(PHIType))

    # Minimum confidence threshold (0.0 - 1.0)
    min_confidence: float = 0.7

    # Whether to include position info in detections
    include_positions: bool = True

    # Case sensitivity for name detection
    case_sensitive_names: bool = False


# ============================================================================
# Detection Result Models
# ============================================================================

class PHIDetection(BaseModel):
    """Single PHI detection result."""

    phi_type: PHIType
    original_value: str
    masked_value: str
    confidence: float = Field(ge=0.0, le=1.0)
    start_position: Optional[int] = None
    end_position: Optional[int] = None
    field_path: Optional[str] = None  # e.g., "patient.name" or "PID.5"
    context: Optional[str] = None  # Surrounding text for context


class PHIMaskingResult(BaseModel):
    """Result of masking operation."""

    original_text: str
    masked_text: str
    detections: List[PHIDetection] = Field(default_factory=list)
    phi_types_found: List[PHIType] = Field(default_factory=list)
    total_detections: int = 0


# ============================================================================
# PHI Detection Patterns
# ============================================================================

class PHIPatterns:
    """Regex patterns for PHI detection."""

    # SSN: XXX-XX-XXXX or XXXXXXXXX
    SSN = re.compile(
        r'\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b'
    )

    # Phone: Various formats
    PHONE = re.compile(
        r'\b(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'
    )

    # Email
    EMAIL = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        re.IGNORECASE
    )

    # IP Address (IPv4)
    IP_V4 = re.compile(
        r'\b(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
        r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'
    )

    # IP Address (IPv6) - simplified
    IP_V6 = re.compile(
        r'\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b|'
        r'\b(?:[0-9a-fA-F]{1,4}:){1,7}:\b|'
        r'\b(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}\b'
    )

    # URL
    URL = re.compile(
        r'https?://[^\s<>"{}|\\^`\[\]]+',
        re.IGNORECASE
    )

    # Date patterns (MM/DD/YYYY, YYYY-MM-DD, etc.)
    DATE_MDY = re.compile(
        r'\b(?:0?[1-9]|1[0-2])[/\-](?:0?[1-9]|[12]\d|3[01])[/\-](?:19|20)\d{2}\b'
    )
    DATE_YMD = re.compile(
        r'\b(?:19|20)\d{2}[/\-](?:0?[1-9]|1[0-2])[/\-](?:0?[1-9]|[12]\d|3[01])\b'
    )
    DATE_WRITTEN = re.compile(
        r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
        r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'\s+\d{1,2},?\s+(?:19|20)\d{2}\b',
        re.IGNORECASE
    )

    # Medical Record Number (various formats)
    MRN = re.compile(
        r'\b(?:MRN|MR#?|Medical Record|Patient ID)[:\s#]*([A-Z0-9]{4,15})\b',
        re.IGNORECASE
    )

    # Account Number (generic)
    ACCOUNT = re.compile(
        r'\b(?:Account|Acct|A/C)[:\s#]*([A-Z0-9]{6,20})\b',
        re.IGNORECASE
    )

    # Driver's License (state-agnostic pattern)
    LICENSE = re.compile(
        r'\b(?:DL|Driver\'?s?\s*License|License)[:\s#]*([A-Z0-9]{5,15})\b',
        re.IGNORECASE
    )

    # VIN (Vehicle Identification Number)
    VIN = re.compile(
        r'\b[A-HJ-NPR-Z0-9]{17}\b'
    )

    # ZIP Code (5 digit or ZIP+4)
    ZIP_CODE = re.compile(
        r'\b\d{5}(?:-\d{4})?\b'
    )

    # Street Address indicators
    STREET_ADDRESS = re.compile(
        r'\b\d{1,5}\s+(?:[A-Z][a-z]+\s+)+'
        r'(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|'
        r'Lane|Ln|Court|Ct|Way|Place|Pl|Circle|Cir)\b',
        re.IGNORECASE
    )

    # Health Plan ID (generic pattern)
    HEALTH_PLAN = re.compile(
        r'\b(?:Member\s*ID|Subscriber\s*ID|Policy\s*#?|Insurance\s*ID)[:\s#]*([A-Z0-9]{6,20})\b',
        re.IGNORECASE
    )


# ============================================================================
# Name Detection (Context-aware)
# ============================================================================

class NameDetector:
    """
    Detects names using contextual patterns.

    Names are hard to detect without context, so we look for:
    - Title prefixes (Mr., Mrs., Dr., etc.)
    - Label prefixes (Patient:, Name:, etc.)
    - Common name patterns in structured data
    """

    TITLE_PREFIXES = re.compile(
        r'\b(?:Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?|Rev\.?)\s+'
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\b'
    )

    LABEL_PREFIXES = re.compile(
        r'\b(?:Patient|Name|Full\s*Name|First\s*Name|Last\s*Name|'
        r'Contact|Guardian|Parent|Spouse|Emergency\s*Contact)'
        r'[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\b',
        re.IGNORECASE
    )

    # Names in formal format: LASTNAME, FIRSTNAME
    FORMAL_NAME = re.compile(
        r'\b([A-Z][A-Z]+),\s*([A-Z][a-z]+(?:\s+[A-Z]\.?)?)\b'
    )

    @classmethod
    def detect(cls, text: str) -> List[Tuple[str, int, int, float]]:
        """
        Detect potential names in text.

        Returns list of (name, start, end, confidence) tuples.
        """
        detections = []

        # Title-prefixed names (high confidence)
        for match in cls.TITLE_PREFIXES.finditer(text):
            detections.append((
                match.group(0),
                match.start(),
                match.end(),
                0.95
            ))

        # Label-prefixed names (high confidence)
        for match in cls.LABEL_PREFIXES.finditer(text):
            detections.append((
                match.group(0),
                match.start(),
                match.end(),
                0.90
            ))

        # Formal names (medium-high confidence)
        for match in cls.FORMAL_NAME.finditer(text):
            detections.append((
                match.group(0),
                match.start(),
                match.end(),
                0.85
            ))

        return detections


# ============================================================================
# PHI Masking Service
# ============================================================================

class PHIMaskingService:
    """
    Service for detecting and masking PHI in text.

    Implements HIPAA Safe Harbor de-identification by detecting
    the 18 identifier categories and replacing them with masked values.
    """

    def __init__(self, config: Optional[PHIConfig] = None):
        self.config = config or PHIConfig()
        self._pattern_map: Dict[PHIType, List[Pattern]] = self._build_pattern_map()

    def _build_pattern_map(self) -> Dict[PHIType, List[Pattern]]:
        """Build mapping of PHI types to detection patterns."""
        return {
            PHIType.SSN: [PHIPatterns.SSN],
            PHIType.PHONE: [PHIPatterns.PHONE],
            PHIType.FAX: [PHIPatterns.PHONE],  # Same pattern, context differs
            PHIType.EMAIL: [PHIPatterns.EMAIL],
            PHIType.IP_ADDRESS: [PHIPatterns.IP_V4, PHIPatterns.IP_V6],
            PHIType.URL: [PHIPatterns.URL],
            PHIType.DATE: [PHIPatterns.DATE_MDY, PHIPatterns.DATE_YMD, PHIPatterns.DATE_WRITTEN],
            PHIType.MRN: [PHIPatterns.MRN],
            PHIType.ACCOUNT_NUMBER: [PHIPatterns.ACCOUNT],
            PHIType.LICENSE_NUMBER: [PHIPatterns.LICENSE],
            PHIType.VEHICLE_ID: [PHIPatterns.VIN],
            PHIType.GEOGRAPHIC: [PHIPatterns.STREET_ADDRESS, PHIPatterns.ZIP_CODE],
            PHIType.HEALTH_PLAN_ID: [PHIPatterns.HEALTH_PLAN],
        }

    def detect_phi(self, text: str) -> List[PHIDetection]:
        """
        Detect all PHI in text.

        Returns list of PHIDetection objects with type, value, and position.
        """
        detections: List[PHIDetection] = []

        if not text:
            return detections

        # Pattern-based detection
        for phi_type, patterns in self._pattern_map.items():
            if phi_type not in self.config.enabled_types:
                continue

            for pattern in patterns:
                for match in pattern.finditer(text):
                    confidence = self._calculate_confidence(phi_type, match.group())

                    if confidence >= self.config.min_confidence:
                        detections.append(PHIDetection(
                            phi_type=phi_type,
                            original_value=match.group(),
                            masked_value=self.config.mask_formats.get(
                                phi_type, f"[REDACTED_{phi_type.value.upper()}]"
                            ),
                            confidence=confidence,
                            start_position=match.start() if self.config.include_positions else None,
                            end_position=match.end() if self.config.include_positions else None,
                        ))

        # Name detection (special handling)
        if PHIType.NAME in self.config.enabled_types:
            for name, start, end, confidence in NameDetector.detect(text):
                if confidence >= self.config.min_confidence:
                    detections.append(PHIDetection(
                        phi_type=PHIType.NAME,
                        original_value=name,
                        masked_value=self.config.mask_formats[PHIType.NAME],
                        confidence=confidence,
                        start_position=start if self.config.include_positions else None,
                        end_position=end if self.config.include_positions else None,
                    ))

        # Sort by position (if available) for proper masking order
        detections.sort(key=lambda d: (d.start_position or 0))

        return detections

    def _calculate_confidence(self, phi_type: PHIType, value: str) -> float:
        """
        Calculate confidence score for a detection.

        Higher confidence for more specific patterns.
        """
        # Base confidence by type
        base_confidence = {
            PHIType.SSN: 0.95,  # Very specific format
            PHIType.EMAIL: 0.98,  # Very specific format
            PHIType.IP_ADDRESS: 0.95,  # Specific format
            PHIType.URL: 0.95,  # Specific format
            PHIType.PHONE: 0.85,  # Could be other numbers
            PHIType.DATE: 0.80,  # Could be other dates
            PHIType.MRN: 0.90,  # Context-dependent
            PHIType.GEOGRAPHIC: 0.75,  # Less specific
            PHIType.VEHICLE_ID: 0.90,  # VIN is specific
        }.get(phi_type, 0.75)

        return base_confidence

    def mask_text(self, text: str) -> PHIMaskingResult:
        """
        Detect and mask all PHI in text.

        Returns result with masked text and detection details.
        """
        if not text:
            return PHIMaskingResult(
                original_text=text,
                masked_text=text,
                detections=[],
                phi_types_found=[],
                total_detections=0,
            )

        detections = self.detect_phi(text)
        masked_text = text
        offset = 0

        # Mask in reverse order to preserve positions
        for detection in reversed(detections):
            if detection.start_position is not None and detection.end_position is not None:
                start = detection.start_position
                end = detection.end_position
                masked_text = (
                    masked_text[:start] +
                    detection.masked_value +
                    masked_text[end:]
                )

        phi_types_found = list(set(d.phi_type for d in detections))

        logger.debug(
            "Masked PHI in text",
            extra={
                "event_type": "phi.masked",
                "detection_count": len(detections),
                "phi_types": [t.value for t in phi_types_found],
            }
        )

        return PHIMaskingResult(
            original_text=text,
            masked_text=masked_text,
            detections=detections,
            phi_types_found=phi_types_found,
            total_detections=len(detections),
        )

    def mask_dict(
        self,
        data: Dict[str, Any],
        sensitive_fields: Optional[Set[str]] = None,
        field_path: str = ""
    ) -> Tuple[Dict[str, Any], List[PHIDetection]]:
        """
        Recursively mask PHI in a dictionary.

        Args:
            data: Dictionary to mask
            sensitive_fields: Optional set of field names to always mask
            field_path: Current path for nested fields (internal use)

        Returns:
            Tuple of (masked_dict, detections)
        """
        # Default sensitive fields in healthcare data
        default_sensitive = {
            "name", "patient_name", "first_name", "last_name",
            "ssn", "social_security", "social_security_number",
            "dob", "date_of_birth", "birth_date", "birthdate",
            "phone", "telephone", "phone_number", "mobile",
            "email", "email_address",
            "address", "street", "street_address", "home_address",
            "mrn", "medical_record", "medical_record_number", "patient_id",
            "ip", "ip_address",
        }
        sensitive = sensitive_fields or default_sensitive

        all_detections: List[PHIDetection] = []
        masked_data = {}

        for key, value in data.items():
            current_path = f"{field_path}.{key}" if field_path else key
            key_lower = key.lower()

            if isinstance(value, dict):
                # Recurse into nested dicts
                masked_value, detections = self.mask_dict(
                    value, sensitive_fields, current_path
                )
                all_detections.extend(detections)
                masked_data[key] = masked_value

            elif isinstance(value, list):
                # Handle lists
                masked_list = []
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        masked_item, detections = self.mask_dict(
                            item, sensitive_fields, f"{current_path}[{i}]"
                        )
                        all_detections.extend(detections)
                        masked_list.append(masked_item)
                    elif isinstance(item, str):
                        result = self.mask_text(item)
                        for d in result.detections:
                            d.field_path = f"{current_path}[{i}]"
                        all_detections.extend(result.detections)
                        masked_list.append(result.masked_text)
                    else:
                        masked_list.append(item)
                masked_data[key] = masked_list

            elif isinstance(value, str):
                # Check if this is a known sensitive field
                if key_lower in sensitive:
                    # Force mask entire value
                    phi_type = self._infer_phi_type(key_lower)
                    masked_value = self.config.mask_formats.get(
                        phi_type, "[REDACTED]"
                    )
                    all_detections.append(PHIDetection(
                        phi_type=phi_type,
                        original_value=value,
                        masked_value=masked_value,
                        confidence=1.0,
                        field_path=current_path,
                    ))
                    masked_data[key] = masked_value
                else:
                    # Scan for PHI
                    result = self.mask_text(value)
                    for d in result.detections:
                        d.field_path = current_path
                    all_detections.extend(result.detections)
                    masked_data[key] = result.masked_text

            else:
                # Preserve non-string values
                masked_data[key] = value

        return masked_data, all_detections

    def _infer_phi_type(self, field_name: str) -> PHIType:
        """Infer PHI type from field name."""
        field_lower = field_name.lower()

        if any(n in field_lower for n in ["name", "first", "last"]):
            return PHIType.NAME
        elif any(n in field_lower for n in ["ssn", "social"]):
            return PHIType.SSN
        elif any(n in field_lower for n in ["dob", "birth", "date"]):
            return PHIType.DATE
        elif any(n in field_lower for n in ["phone", "tel", "mobile"]):
            return PHIType.PHONE
        elif any(n in field_lower for n in ["email"]):
            return PHIType.EMAIL
        elif any(n in field_lower for n in ["address", "street"]):
            return PHIType.GEOGRAPHIC
        elif any(n in field_lower for n in ["mrn", "medical_record", "patient_id"]):
            return PHIType.MRN
        elif any(n in field_lower for n in ["ip"]):
            return PHIType.IP_ADDRESS
        else:
            return PHIType.OTHER_ID


# ============================================================================
# Singleton Factory
# ============================================================================

_phi_masking_service: Optional[PHIMaskingService] = None


async def get_phi_masking_service(
    config: Optional[PHIConfig] = None
) -> PHIMaskingService:
    """Get singleton instance of PHI masking service."""
    global _phi_masking_service

    if _phi_masking_service is None:
        _phi_masking_service = PHIMaskingService(config)
        logger.info(
            "Initialized PHI masking service",
            extra={
                "event_type": "phi.service_initialized",
                "enabled_types": [t.value for t in _phi_masking_service.config.enabled_types],
            }
        )

    return _phi_masking_service

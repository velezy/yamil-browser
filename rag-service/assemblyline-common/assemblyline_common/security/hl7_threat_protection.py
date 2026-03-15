"""
HL7 Threat Protection.

Protects against HL7 message-based attacks:
- Maximum segment count
- Maximum field length
- Encoding character validation
- Required segment validation
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Set
from enum import Enum

logger = logging.getLogger(__name__)


class HL7ThreatType(Enum):
    """Types of HL7 threats detected."""
    MAX_SEGMENTS_EXCEEDED = "max_segments_exceeded"
    MAX_FIELDS_EXCEEDED = "max_fields_exceeded"
    MAX_FIELD_LENGTH_EXCEEDED = "max_field_length_exceeded"
    MAX_MESSAGE_SIZE_EXCEEDED = "max_message_size_exceeded"
    INVALID_ENCODING_CHARS = "invalid_encoding_chars"
    MISSING_REQUIRED_SEGMENT = "missing_required_segment"
    INVALID_MSH_SEGMENT = "invalid_msh_segment"
    INVALID_MESSAGE_TYPE = "invalid_message_type"
    INJECTION_DETECTED = "injection_detected"
    INVALID_CHARACTERS = "invalid_characters"
    DUPLICATE_SEGMENT = "duplicate_segment"


@dataclass
class HL7ThreatConfig:
    """Configuration for HL7 threat protection."""
    # Size limits
    max_message_size: int = 1000000  # 1MB
    max_segments: int = 500
    max_fields_per_segment: int = 100
    max_field_length: int = 65536  # 64KB
    max_repetitions: int = 50

    # Required segments (for all messages)
    required_segments: List[str] = field(default_factory=lambda: ["MSH"])

    # Allowed message types (empty = allow all)
    allowed_message_types: Set[str] = field(default_factory=set)

    # Segments that should only appear once
    unique_segments: Set[str] = field(default_factory=lambda: {"MSH", "MSA", "EVN"})

    # Character validation
    allow_binary_data: bool = False
    allowed_encoding_chars: str = r"^~\&"  # Default HL7 encoding characters

    # Injection protection
    check_injection: bool = True
    injection_patterns: List[str] = field(default_factory=lambda: [
        r"<script",           # XSS
        r"javascript:",       # XSS
        r"SELECT\s+.*FROM",   # SQL injection
        r"INSERT\s+INTO",     # SQL injection
        r"DROP\s+TABLE",      # SQL injection
        r";\s*--",            # SQL comment injection
        r"\|\s*\|",           # Command injection
        r"`.*`",              # Command substitution
    ])


@dataclass
class HL7ValidationResult:
    """Result of HL7 validation."""
    valid: bool
    threats: List[Tuple[HL7ThreatType, str]]
    stats: dict
    message_type: Optional[str] = None
    trigger_event: Optional[str] = None

    @property
    def threat_messages(self) -> List[str]:
        """Get list of threat messages."""
        return [msg for _, msg in self.threats]


class HL7ThreatProtection:
    """
    HL7 message threat protection validator.

    Validates HL7 v2.x messages against configurable limits to prevent:
    - Oversized messages (memory exhaustion)
    - Injection attacks via field values
    - Malformed messages
    - Denial of service via excessive segments/fields
    """

    def __init__(self, config: Optional[HL7ThreatConfig] = None):
        self.config = config or HL7ThreatConfig()
        self._injection_patterns = [
            re.compile(pattern, re.IGNORECASE)
            for pattern in self.config.injection_patterns
        ]

    def validate(
        self,
        message: str,
        content_type: Optional[str] = None
    ) -> HL7ValidationResult:
        """
        Validate HL7 message against threat protection rules.

        Args:
            message: Raw HL7 message string
            content_type: Optional content type header

        Returns:
            HL7ValidationResult with validation status and threats
        """
        threats: List[Tuple[HL7ThreatType, str]] = []
        stats = {
            "message_size": len(message),
            "segment_count": 0,
            "max_fields": 0,
            "max_field_length": 0,
            "segments_found": [],
        }

        # Check message size
        if len(message) > self.config.max_message_size:
            threats.append((
                HL7ThreatType.MAX_MESSAGE_SIZE_EXCEEDED,
                f"Message size {len(message)} exceeds maximum {self.config.max_message_size}"
            ))
            return HL7ValidationResult(valid=False, threats=threats, stats=stats)

        # Normalize line endings
        message = message.replace("\r\n", "\r").replace("\n", "\r")

        # Split into segments
        segments = [s for s in message.split("\r") if s.strip()]
        stats["segment_count"] = len(segments)

        # Check segment count
        if len(segments) > self.config.max_segments:
            threats.append((
                HL7ThreatType.MAX_SEGMENTS_EXCEEDED,
                f"Segment count {len(segments)} exceeds maximum {self.config.max_segments}"
            ))

        if not segments:
            threats.append((
                HL7ThreatType.INVALID_MSH_SEGMENT,
                "No segments found in message"
            ))
            return HL7ValidationResult(valid=False, threats=threats, stats=stats)

        # Validate MSH segment
        msh_segment = segments[0]
        if not msh_segment.startswith("MSH"):
            threats.append((
                HL7ThreatType.INVALID_MSH_SEGMENT,
                "Message must start with MSH segment"
            ))
            return HL7ValidationResult(valid=False, threats=threats, stats=stats)

        # Extract encoding characters
        if len(msh_segment) < 8:
            threats.append((
                HL7ThreatType.INVALID_MSH_SEGMENT,
                "MSH segment too short"
            ))
            return HL7ValidationResult(valid=False, threats=threats, stats=stats)

        field_separator = msh_segment[3]
        encoding_chars = msh_segment[4:8]

        # Validate encoding characters
        expected_encoding = self.config.allowed_encoding_chars
        if encoding_chars != expected_encoding:
            # Allow different encoding chars but log it
            logger.info(f"Non-standard encoding characters: {encoding_chars}")

        # Parse MSH fields to get message type
        msh_fields = msh_segment.split(field_separator)
        message_type = None
        trigger_event = None

        if len(msh_fields) >= 9:
            msg_type_field = msh_fields[8]  # MSH-9
            if "^" in msg_type_field:
                parts = msg_type_field.split("^")
                message_type = parts[0] if parts else None
                trigger_event = parts[1] if len(parts) > 1 else None
            else:
                message_type = msg_type_field

        # Check message type if whitelist is defined
        if self.config.allowed_message_types:
            if message_type and message_type not in self.config.allowed_message_types:
                threats.append((
                    HL7ThreatType.INVALID_MESSAGE_TYPE,
                    f"Message type '{message_type}' is not allowed"
                ))

        # Track segment occurrences
        segment_counts: dict = {}

        # Validate each segment
        for segment in segments:
            self._validate_segment(
                segment,
                field_separator,
                encoding_chars,
                segment_counts,
                stats,
                threats
            )

        # Check required segments
        for required in self.config.required_segments:
            if required not in segment_counts:
                threats.append((
                    HL7ThreatType.MISSING_REQUIRED_SEGMENT,
                    f"Required segment '{required}' is missing"
                ))

        # Check unique segments
        for unique_seg in self.config.unique_segments:
            if segment_counts.get(unique_seg, 0) > 1:
                threats.append((
                    HL7ThreatType.DUPLICATE_SEGMENT,
                    f"Segment '{unique_seg}' appears {segment_counts[unique_seg]} times but should be unique"
                ))

        stats["segments_found"] = list(segment_counts.keys())

        return HL7ValidationResult(
            valid=len(threats) == 0,
            threats=threats,
            stats=stats,
            message_type=message_type,
            trigger_event=trigger_event
        )

    def _validate_segment(
        self,
        segment: str,
        field_separator: str,
        encoding_chars: str,
        segment_counts: dict,
        stats: dict,
        threats: List[Tuple[HL7ThreatType, str]]
    ):
        """Validate a single HL7 segment."""
        if len(segment) < 3:
            return

        segment_id = segment[:3]
        segment_counts[segment_id] = segment_counts.get(segment_id, 0) + 1

        # Split into fields
        fields = segment.split(field_separator)
        field_count = len(fields)
        stats["max_fields"] = max(stats["max_fields"], field_count)

        # Check field count
        if field_count > self.config.max_fields_per_segment:
            threats.append((
                HL7ThreatType.MAX_FIELDS_EXCEEDED,
                f"Segment '{segment_id}' has {field_count} fields, exceeds maximum {self.config.max_fields_per_segment}"
            ))

        # Validate each field
        for i, field_value in enumerate(fields):
            # Check field length
            if len(field_value) > self.config.max_field_length:
                stats["max_field_length"] = max(stats["max_field_length"], len(field_value))
                threats.append((
                    HL7ThreatType.MAX_FIELD_LENGTH_EXCEEDED,
                    f"Field {segment_id}-{i+1} length {len(field_value)} exceeds maximum {self.config.max_field_length}"
                ))
                continue

            stats["max_field_length"] = max(stats["max_field_length"], len(field_value))

            # Check for binary data
            if not self.config.allow_binary_data:
                if self._contains_binary(field_value):
                    threats.append((
                        HL7ThreatType.INVALID_CHARACTERS,
                        f"Field {segment_id}-{i+1} contains binary/control characters"
                    ))

            # Check for injection patterns
            if self.config.check_injection:
                for pattern in self._injection_patterns:
                    if pattern.search(field_value):
                        threats.append((
                            HL7ThreatType.INJECTION_DETECTED,
                            f"Potential injection attack detected in {segment_id}-{i+1}"
                        ))
                        break

    def _contains_binary(self, value: str) -> bool:
        """Check if string contains binary/control characters."""
        for char in value:
            code = ord(char)
            # Allow printable ASCII, newlines, and high Unicode
            if code < 32 and code not in (9, 10, 13):  # Tab, LF, CR allowed
                return True
        return False


# Convenience function
def validate_hl7(
    message: str,
    config: Optional[HL7ThreatConfig] = None,
    content_type: Optional[str] = None
) -> HL7ValidationResult:
    """
    Validate HL7 message with default or custom configuration.

    Args:
        message: HL7 message string to validate
        config: Optional custom configuration
        content_type: Optional content type header

    Returns:
        HL7ValidationResult
    """
    protector = HL7ThreatProtection(config)
    return protector.validate(message, content_type)


# FastAPI dependency
from fastapi import Request, HTTPException


async def hl7_threat_protection(
    request: Request,
    config: Optional[HL7ThreatConfig] = None
):
    """FastAPI dependency for HL7 threat protection."""
    content_type = request.headers.get("content-type", "")

    # Check for HL7 content types
    hl7_types = ["x-application/hl7-v2", "application/hl7-v2", "text/plain"]
    if not any(t in content_type.lower() for t in hl7_types):
        return  # Not an HL7 request

    try:
        body = await request.body()
        message = body.decode("utf-8")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request body")

    protector = HL7ThreatProtection(config)
    result = protector.validate(message, content_type)

    if not result.valid:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "HL7 threat protection violation",
                "threats": result.threat_messages
            }
        )

    return result

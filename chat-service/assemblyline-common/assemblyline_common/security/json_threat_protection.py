"""
JSON Threat Protection.

Protects against JSON-based attacks:
- Maximum depth validation
- Maximum array/object size
- Maximum string length
- Maximum entry count
- Content type validation
"""

import json
import logging
from dataclasses import dataclass
from typing import Any, Optional, List, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class JSONThreatType(Enum):
    """Types of JSON threats detected."""
    MAX_DEPTH_EXCEEDED = "max_depth_exceeded"
    MAX_ARRAY_SIZE_EXCEEDED = "max_array_size_exceeded"
    MAX_OBJECT_SIZE_EXCEEDED = "max_object_size_exceeded"
    MAX_STRING_LENGTH_EXCEEDED = "max_string_length_exceeded"
    MAX_TOTAL_ENTRIES_EXCEEDED = "max_total_entries_exceeded"
    MAX_KEY_LENGTH_EXCEEDED = "max_key_length_exceeded"
    INVALID_JSON = "invalid_json"
    INVALID_CONTENT_TYPE = "invalid_content_type"


@dataclass
class JSONThreatConfig:
    """Configuration for JSON threat protection."""
    # Depth limits
    max_depth: int = 20

    # Size limits
    max_array_elements: int = 1000
    max_object_entries: int = 500
    max_total_entries: int = 10000

    # String limits
    max_string_length: int = 100000  # 100KB
    max_key_length: int = 256

    # Content type
    allowed_content_types: List[str] = None

    # Behavior
    strict_mode: bool = True  # Fail on first violation vs collect all

    def __post_init__(self):
        if self.allowed_content_types is None:
            self.allowed_content_types = [
                "application/json",
                "application/json; charset=utf-8",
                "text/json",
            ]


@dataclass
class JSONValidationResult:
    """Result of JSON validation."""
    valid: bool
    threats: List[Tuple[JSONThreatType, str]]  # (threat_type, message)
    stats: dict  # Statistics about the JSON

    @property
    def threat_messages(self) -> List[str]:
        """Get list of threat messages."""
        return [msg for _, msg in self.threats]


class JSONThreatProtection:
    """
    JSON threat protection validator.

    Validates JSON payloads against configurable limits to prevent:
    - Deeply nested structures (DoS via parsing)
    - Huge arrays/objects (memory exhaustion)
    - Excessively long strings (buffer attacks)
    - Malformed JSON (parser exploits)
    """

    def __init__(self, config: Optional[JSONThreatConfig] = None):
        self.config = config or JSONThreatConfig()

    def validate(
        self,
        data: Any,
        content_type: Optional[str] = None
    ) -> JSONValidationResult:
        """
        Validate JSON data against threat protection rules.

        Args:
            data: Parsed JSON data (dict, list, or primitive)
            content_type: Optional content type header to validate

        Returns:
            JSONValidationResult with validation status and threats
        """
        threats: List[Tuple[JSONThreatType, str]] = []
        stats = {
            "max_depth": 0,
            "total_entries": 0,
            "max_array_size": 0,
            "max_object_size": 0,
            "max_string_length": 0,
            "max_key_length": 0,
        }

        # Validate content type if provided
        if content_type:
            content_type_lower = content_type.lower().split(";")[0].strip()
            valid_types = [ct.lower().split(";")[0].strip()
                         for ct in self.config.allowed_content_types]
            if content_type_lower not in valid_types:
                threats.append((
                    JSONThreatType.INVALID_CONTENT_TYPE,
                    f"Invalid content type: {content_type}. "
                    f"Allowed: {', '.join(self.config.allowed_content_types)}"
                ))
                if self.config.strict_mode:
                    return JSONValidationResult(valid=False, threats=threats, stats=stats)

        # Validate structure
        try:
            self._validate_node(data, 0, stats, threats)
        except Exception as e:
            threats.append((
                JSONThreatType.INVALID_JSON,
                f"JSON validation error: {str(e)}"
            ))

        return JSONValidationResult(
            valid=len(threats) == 0,
            threats=threats,
            stats=stats
        )

    def _validate_node(
        self,
        node: Any,
        depth: int,
        stats: dict,
        threats: List[Tuple[JSONThreatType, str]]
    ):
        """Recursively validate a JSON node."""
        # Update max depth
        stats["max_depth"] = max(stats["max_depth"], depth)

        # Check depth limit
        if depth > self.config.max_depth:
            threats.append((
                JSONThreatType.MAX_DEPTH_EXCEEDED,
                f"JSON depth {depth} exceeds maximum {self.config.max_depth}"
            ))
            if self.config.strict_mode:
                return

        if isinstance(node, dict):
            self._validate_object(node, depth, stats, threats)
        elif isinstance(node, list):
            self._validate_array(node, depth, stats, threats)
        elif isinstance(node, str):
            self._validate_string(node, stats, threats)

    def _validate_object(
        self,
        obj: dict,
        depth: int,
        stats: dict,
        threats: List[Tuple[JSONThreatType, str]]
    ):
        """Validate a JSON object."""
        size = len(obj)
        stats["max_object_size"] = max(stats["max_object_size"], size)
        stats["total_entries"] += size

        # Check object size
        if size > self.config.max_object_entries:
            threats.append((
                JSONThreatType.MAX_OBJECT_SIZE_EXCEEDED,
                f"Object has {size} entries, exceeds maximum {self.config.max_object_entries}"
            ))
            if self.config.strict_mode:
                return

        # Check total entries
        if stats["total_entries"] > self.config.max_total_entries:
            threats.append((
                JSONThreatType.MAX_TOTAL_ENTRIES_EXCEEDED,
                f"Total entries {stats['total_entries']} exceeds maximum {self.config.max_total_entries}"
            ))
            if self.config.strict_mode:
                return

        # Validate keys and values
        for key, value in obj.items():
            # Check key length
            if len(key) > self.config.max_key_length:
                stats["max_key_length"] = max(stats["max_key_length"], len(key))
                threats.append((
                    JSONThreatType.MAX_KEY_LENGTH_EXCEEDED,
                    f"Key length {len(key)} exceeds maximum {self.config.max_key_length}"
                ))
                if self.config.strict_mode:
                    return

            stats["max_key_length"] = max(stats["max_key_length"], len(key))

            # Recurse into value
            self._validate_node(value, depth + 1, stats, threats)

            if self.config.strict_mode and threats:
                return

    def _validate_array(
        self,
        arr: list,
        depth: int,
        stats: dict,
        threats: List[Tuple[JSONThreatType, str]]
    ):
        """Validate a JSON array."""
        size = len(arr)
        stats["max_array_size"] = max(stats["max_array_size"], size)
        stats["total_entries"] += size

        # Check array size
        if size > self.config.max_array_elements:
            threats.append((
                JSONThreatType.MAX_ARRAY_SIZE_EXCEEDED,
                f"Array has {size} elements, exceeds maximum {self.config.max_array_elements}"
            ))
            if self.config.strict_mode:
                return

        # Check total entries
        if stats["total_entries"] > self.config.max_total_entries:
            threats.append((
                JSONThreatType.MAX_TOTAL_ENTRIES_EXCEEDED,
                f"Total entries {stats['total_entries']} exceeds maximum {self.config.max_total_entries}"
            ))
            if self.config.strict_mode:
                return

        # Validate elements
        for item in arr:
            self._validate_node(item, depth + 1, stats, threats)

            if self.config.strict_mode and threats:
                return

    def _validate_string(
        self,
        s: str,
        stats: dict,
        threats: List[Tuple[JSONThreatType, str]]
    ):
        """Validate a JSON string value."""
        length = len(s)
        stats["max_string_length"] = max(stats["max_string_length"], length)

        if length > self.config.max_string_length:
            threats.append((
                JSONThreatType.MAX_STRING_LENGTH_EXCEEDED,
                f"String length {length} exceeds maximum {self.config.max_string_length}"
            ))

    def validate_raw(
        self,
        raw_json: str,
        content_type: Optional[str] = None
    ) -> JSONValidationResult:
        """
        Validate raw JSON string.

        Args:
            raw_json: Raw JSON string to parse and validate
            content_type: Optional content type header

        Returns:
            JSONValidationResult with validation status
        """
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as e:
            return JSONValidationResult(
                valid=False,
                threats=[(
                    JSONThreatType.INVALID_JSON,
                    f"Invalid JSON: {str(e)}"
                )],
                stats={}
            )

        return self.validate(data, content_type)


# Convenience function
def validate_json(
    data: Any,
    config: Optional[JSONThreatConfig] = None,
    content_type: Optional[str] = None
) -> JSONValidationResult:
    """
    Validate JSON data with default or custom configuration.

    Args:
        data: JSON data to validate
        config: Optional custom configuration
        content_type: Optional content type header

    Returns:
        JSONValidationResult
    """
    protector = JSONThreatProtection(config)
    return protector.validate(data, content_type)


# FastAPI dependency
from fastapi import Request, HTTPException


async def json_threat_protection(
    request: Request,
    config: Optional[JSONThreatConfig] = None
):
    """FastAPI dependency for JSON threat protection."""
    content_type = request.headers.get("content-type", "")

    if "json" not in content_type.lower():
        return  # Not a JSON request

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    protector = JSONThreatProtection(config)
    result = protector.validate(body, content_type)

    if not result.valid:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "JSON threat protection violation",
                "threats": result.threat_messages
            }
        )

    return result

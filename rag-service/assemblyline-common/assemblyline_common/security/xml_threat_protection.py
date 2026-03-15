"""
XML Threat Protection.

Protects against XML-based attacks:
- DTD validation/rejection
- External entity prevention (XXE)
- Schema validation
- Maximum depth/elements
"""

import logging
from dataclasses import dataclass
from typing import Optional, List, Tuple
from enum import Enum
from io import StringIO
import re

logger = logging.getLogger(__name__)

# Try to import defusedxml for secure parsing
try:
    import defusedxml.ElementTree as ET
    from defusedxml import DefusedXmlException
    DEFUSED_AVAILABLE = True
except ImportError:
    import xml.etree.ElementTree as ET
    DefusedXmlException = Exception
    DEFUSED_AVAILABLE = False
    logger.warning("defusedxml not available, using standard library with manual protections")


class XMLThreatType(Enum):
    """Types of XML threats detected."""
    XXE_ATTACK = "xxe_attack"
    DTD_NOT_ALLOWED = "dtd_not_allowed"
    MAX_DEPTH_EXCEEDED = "max_depth_exceeded"
    MAX_ELEMENTS_EXCEEDED = "max_elements_exceeded"
    MAX_ATTRIBUTES_EXCEEDED = "max_attributes_exceeded"
    MAX_ATTRIBUTE_LENGTH_EXCEEDED = "max_attribute_length_exceeded"
    MAX_TEXT_LENGTH_EXCEEDED = "max_text_length_exceeded"
    MAX_ELEMENT_NAME_LENGTH_EXCEEDED = "max_element_name_length_exceeded"
    INVALID_XML = "invalid_xml"
    ENTITY_EXPANSION = "entity_expansion"
    PROCESSING_INSTRUCTION = "processing_instruction_detected"


@dataclass
class XMLThreatConfig:
    """Configuration for XML threat protection."""
    # DTD settings
    allow_dtd: bool = False
    allow_external_entities: bool = False

    # Depth and size limits
    max_depth: int = 50
    max_elements: int = 10000
    max_attributes_per_element: int = 100
    max_attribute_length: int = 10000
    max_text_length: int = 100000  # 100KB
    max_element_name_length: int = 256

    # Entity expansion limits
    max_entity_expansions: int = 100

    # Processing instructions
    allow_processing_instructions: bool = False

    # Content type
    allowed_content_types: List[str] = None

    def __post_init__(self):
        if self.allowed_content_types is None:
            self.allowed_content_types = [
                "application/xml",
                "text/xml",
                "application/xhtml+xml",
            ]


@dataclass
class XMLValidationResult:
    """Result of XML validation."""
    valid: bool
    threats: List[Tuple[XMLThreatType, str]]
    stats: dict

    @property
    def threat_messages(self) -> List[str]:
        """Get list of threat messages."""
        return [msg for _, msg in self.threats]


class XMLThreatProtection:
    """
    XML threat protection validator.

    Validates XML payloads against configurable limits to prevent:
    - XXE (XML External Entity) attacks
    - Billion laughs attack (entity expansion)
    - Deeply nested structures
    - Oversized elements
    """

    # Patterns for detecting threats in raw XML
    DTD_PATTERN = re.compile(r'<!DOCTYPE[^>]*>', re.IGNORECASE)
    ENTITY_PATTERN = re.compile(r'<!ENTITY[^>]*>', re.IGNORECASE)
    EXTERNAL_ENTITY_PATTERN = re.compile(
        r'<!ENTITY\s+\w+\s+SYSTEM\s+["\'][^"\']+["\']',
        re.IGNORECASE
    )
    PI_PATTERN = re.compile(r'<\?(?!xml\s)[^?]*\?>')

    def __init__(self, config: Optional[XMLThreatConfig] = None):
        self.config = config or XMLThreatConfig()

    def validate(
        self,
        xml_string: str,
        content_type: Optional[str] = None
    ) -> XMLValidationResult:
        """
        Validate XML string against threat protection rules.

        Args:
            xml_string: Raw XML string to validate
            content_type: Optional content type header

        Returns:
            XMLValidationResult with validation status and threats
        """
        threats: List[Tuple[XMLThreatType, str]] = []
        stats = {
            "max_depth": 0,
            "element_count": 0,
            "max_attributes": 0,
            "max_text_length": 0,
        }

        # Pre-parse checks (before parsing)
        self._check_raw_xml(xml_string, threats)

        if threats:
            return XMLValidationResult(valid=False, threats=threats, stats=stats)

        # Parse and validate structure
        try:
            if DEFUSED_AVAILABLE:
                # Use defusedxml for secure parsing
                root = ET.fromstring(xml_string)
            else:
                # Manual security checks with standard library
                root = ET.fromstring(xml_string)

            self._validate_element(root, 0, stats, threats)

        except DefusedXmlException as e:
            threats.append((
                XMLThreatType.XXE_ATTACK,
                f"Security violation detected: {str(e)}"
            ))
        except ET.ParseError as e:
            threats.append((
                XMLThreatType.INVALID_XML,
                f"XML parse error: {str(e)}"
            ))
        except Exception as e:
            threats.append((
                XMLThreatType.INVALID_XML,
                f"XML validation error: {str(e)}"
            ))

        return XMLValidationResult(
            valid=len(threats) == 0,
            threats=threats,
            stats=stats
        )

    def _check_raw_xml(
        self,
        xml_string: str,
        threats: List[Tuple[XMLThreatType, str]]
    ):
        """Check raw XML string for threats before parsing."""
        # Check for DTD
        if not self.config.allow_dtd:
            if self.DTD_PATTERN.search(xml_string):
                threats.append((
                    XMLThreatType.DTD_NOT_ALLOWED,
                    "DOCTYPE declarations are not allowed"
                ))

        # Check for external entities
        if not self.config.allow_external_entities:
            if self.EXTERNAL_ENTITY_PATTERN.search(xml_string):
                threats.append((
                    XMLThreatType.XXE_ATTACK,
                    "External entity references are not allowed"
                ))

        # Check for entity definitions
        entity_count = len(self.ENTITY_PATTERN.findall(xml_string))
        if entity_count > self.config.max_entity_expansions:
            threats.append((
                XMLThreatType.ENTITY_EXPANSION,
                f"Too many entity definitions: {entity_count}"
            ))

        # Check for processing instructions
        if not self.config.allow_processing_instructions:
            if self.PI_PATTERN.search(xml_string):
                threats.append((
                    XMLThreatType.PROCESSING_INSTRUCTION,
                    "Processing instructions are not allowed"
                ))

    def _validate_element(
        self,
        element,
        depth: int,
        stats: dict,
        threats: List[Tuple[XMLThreatType, str]]
    ):
        """Recursively validate an XML element."""
        # Update stats
        stats["element_count"] += 1
        stats["max_depth"] = max(stats["max_depth"], depth)

        # Check depth
        if depth > self.config.max_depth:
            threats.append((
                XMLThreatType.MAX_DEPTH_EXCEEDED,
                f"XML depth {depth} exceeds maximum {self.config.max_depth}"
            ))
            return

        # Check element count
        if stats["element_count"] > self.config.max_elements:
            threats.append((
                XMLThreatType.MAX_ELEMENTS_EXCEEDED,
                f"Element count {stats['element_count']} exceeds maximum {self.config.max_elements}"
            ))
            return

        # Check element name length
        tag_name = element.tag.split("}")[-1] if "}" in element.tag else element.tag
        if len(tag_name) > self.config.max_element_name_length:
            threats.append((
                XMLThreatType.MAX_ELEMENT_NAME_LENGTH_EXCEEDED,
                f"Element name '{tag_name[:50]}...' exceeds maximum length {self.config.max_element_name_length}"
            ))

        # Check attributes
        attr_count = len(element.attrib)
        stats["max_attributes"] = max(stats["max_attributes"], attr_count)

        if attr_count > self.config.max_attributes_per_element:
            threats.append((
                XMLThreatType.MAX_ATTRIBUTES_EXCEEDED,
                f"Element has {attr_count} attributes, exceeds maximum {self.config.max_attributes_per_element}"
            ))

        for attr_name, attr_value in element.attrib.items():
            if len(attr_value) > self.config.max_attribute_length:
                threats.append((
                    XMLThreatType.MAX_ATTRIBUTE_LENGTH_EXCEEDED,
                    f"Attribute '{attr_name}' value length exceeds maximum {self.config.max_attribute_length}"
                ))

        # Check text content
        if element.text:
            text_len = len(element.text)
            stats["max_text_length"] = max(stats["max_text_length"], text_len)
            if text_len > self.config.max_text_length:
                threats.append((
                    XMLThreatType.MAX_TEXT_LENGTH_EXCEEDED,
                    f"Text content length {text_len} exceeds maximum {self.config.max_text_length}"
                ))

        if element.tail:
            tail_len = len(element.tail)
            stats["max_text_length"] = max(stats["max_text_length"], tail_len)
            if tail_len > self.config.max_text_length:
                threats.append((
                    XMLThreatType.MAX_TEXT_LENGTH_EXCEEDED,
                    f"Tail text length {tail_len} exceeds maximum {self.config.max_text_length}"
                ))

        # Recursively validate children
        for child in element:
            self._validate_element(child, depth + 1, stats, threats)


# Convenience function
def validate_xml(
    xml_string: str,
    config: Optional[XMLThreatConfig] = None,
    content_type: Optional[str] = None
) -> XMLValidationResult:
    """
    Validate XML string with default or custom configuration.

    Args:
        xml_string: XML string to validate
        config: Optional custom configuration
        content_type: Optional content type header

    Returns:
        XMLValidationResult
    """
    protector = XMLThreatProtection(config)
    return protector.validate(xml_string, content_type)


# FastAPI dependency
from fastapi import Request, HTTPException


async def xml_threat_protection(
    request: Request,
    config: Optional[XMLThreatConfig] = None
):
    """FastAPI dependency for XML threat protection."""
    content_type = request.headers.get("content-type", "")

    if "xml" not in content_type.lower():
        return  # Not an XML request

    try:
        body = await request.body()
        xml_string = body.decode("utf-8")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request body")

    protector = XMLThreatProtection(config)
    result = protector.validate(xml_string, content_type)

    if not result.valid:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "XML threat protection violation",
                "threats": result.threat_messages
            }
        )

    return result

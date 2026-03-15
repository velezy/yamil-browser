"""
HL7 module for Logic Weaver.

Provides HL7 v2.x parsing and generation capabilities.
Includes ESL schema-based message generation and schema management.
"""

from assemblyline_common.hl7.parser import HL7Parser, HL7Message, HL7Segment
from assemblyline_common.hl7.generator import HL7Generator
from assemblyline_common.hl7.esl_schema_manager import (
    ESLSchemaManager,
    HL7MessageGenerator,
    SegmentDefinition,
    FieldDefinition,
    MessageStructure,
    generate_hl7_from_json,
    create_custom_segment,
)
from assemblyline_common.hl7.esl_auto_mapper import ESLAutoMapper, MappingResult
from assemblyline_common.hl7.esl_auto_mapper_ml import ESLAutoMapperML
from assemblyline_common.hl7.parser_ml import HL7SmartExtractor

__all__ = [
    "HL7Parser",
    "HL7Message",
    "HL7Segment",
    "HL7Generator",
    # ESL-based generation
    "ESLSchemaManager",
    "HL7MessageGenerator",
    "SegmentDefinition",
    "FieldDefinition",
    "MessageStructure",
    "generate_hl7_from_json",
    "create_custom_segment",
    # ESL Auto-Mapping
    "ESLAutoMapper",
    "MappingResult",
    # ESL ML-Enhanced Mapping
    "ESLAutoMapperML",
    # Smart Extractor
    "HL7SmartExtractor",
]

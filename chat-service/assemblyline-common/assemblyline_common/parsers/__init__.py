"""
Universal Payload Parsers for Logic Weaver

Enterprise-grade parsers for healthcare data interchange formats.
Positioned to surpass MuleSoft's DataWeave and Apigee's payload processing.

Supported Formats:
- X12 EDI: 835, 837, 270, 271, 276, 277 healthcare transactions
- HL7 v2.x: See assemblyline_common.hl7 module
- FHIR R4: JSON/XML resources
- CCDA/CDA: Clinical documents (CCD, Discharge Summary, etc.)
- Flat Files: Fixed-width, CSV, TSV, pipe-delimited
- PDF: Text, tables, form fields, OCR
- DICOM: Medical imaging metadata extraction
- Audio: Multi-vendor transcription (Whisper, AWS, Google, Azure)
- Video: Frame extraction via FFmpeg

Key Features:
- Automatic format detection
- Segment-level parsing with descriptions
- JSON/dict output for flow integration
- Async-ready for high throughput
- Service area determination for ERA processing
- FHIR R4 conversion support
- Flow node integration
- Microservice-oriented binary processing

Comparison with competitors:
| Feature                | MuleSoft | Apigee | Logic Weaver |
|------------------------|----------|--------|--------------|
| X12 Parsing            | Plugin   | No     | Native       |
| HL7 v2.x Parsing       | Plugin   | No     | Native       |
| CCDA/CDA Parsing       | Plugin   | No     | Native       |
| Healthcare Claims      | Limited  | No     | Full         |
| Service Area Detection | No       | No     | Yes          |
| Segment Descriptions   | No       | No     | Yes          |
| FHIR Conversion        | Limited  | No     | Yes          |
| Python Integration     | No       | No     | Full         |
| DICOM Parsing          | No       | No     | Native       |
| Audio Transcription    | No       | No     | Multi-vendor |
| Video Processing       | No       | No     | Native       |
"""

from assemblyline_common.parsers.x12_parser import (
    # Main parser
    X12Parser,
    X12ParseResult,

    # Data classes
    X12Segment,
    X12Element,
    X12Claim,
    X12ServiceLine,
    X12Adjustment,

    # Enums
    X12TransactionType,
    X12ClaimStatus,
    X12AdjustmentGroup,

    # Convenience functions
    parse_x12,
    parse_x12_file,
    x12_to_dict,
    x12_to_json,
)
from assemblyline_common.parsers.flatfile_parser import (
    FlatFileParser,
    FlatFileConfig,
    FlatFileField,
    FlatFileRecord,
    FlatFileResult,
    FlatFileNode,
    FlatFileNodeConfig,
    FlatFileNodeResult,
    get_flatfile_parser,
    get_flatfile_node_definition,
)
from assemblyline_common.parsers.pdf_extractor import (
    PDFExtractor,
    PDFConfig,
    PDFPage,
    PDFMetadata,
    PDFExtractionResult,
    PDFNode,
    PDFNodeConfig,
    PDFNodeResult,
    get_pdf_extractor,
    get_pdf_node_definition,
)
from assemblyline_common.parsers.ccda_parser import (
    # Main parser
    CCDAParser,
    CCDAParseResult,
    # Document types and section codes
    CCDADocumentType,
    CCDASectionCode,
    # Data classes
    CCDACode,
    CCDAName,
    CCDAAddress,
    CCDATelecom,
    CCDAPatient,
    CCDAAuthor,
    CCDAAllergy,
    CCDAMedication,
    CCDAProblem,
    CCDAProcedure,
    CCDAVitalSign,
    CCDALabResult,
    CCDAImmunization,
    CCDAEncounter,
    CCDASection,
    # Flow node
    CCDANode,
    CCDANodeConfig,
    CCDANodeResult,
    # Convenience functions
    parse_ccda,
    parse_ccda_file,
    ccda_to_dict,
    ccda_to_fhir_bundle,
    get_ccda_parser,
    get_ccda_node_definition,
)
from assemblyline_common.parsers.binary_streaming import (
    # DICOM Parser
    DICOMParser,
    DICOMConfig,
    DICOMParseResult,
    DICOMMetadata,
    DICOMPatient,
    DICOMStudy,
    DICOMSeries,
    DICOMImage,
    DICOMModality,
    DICOMTransferSyntax,
    DICOMNode,
    DICOMNodeConfig,
    DICOMNodeResult,
    get_dicom_parser,
    get_dicom_node_definition,
    # Audio Transcription
    AudioTranscriber,
    AudioTranscriptionConfig,
    TranscriptionResult,
    TranscriptionSegment,
    AudioMetadata,
    TranscriptionService,
    AudioFormat,
    AudioTranscriptionNode,
    AudioNodeConfig,
    AudioNodeResult,
    get_audio_transcriber,
    get_audio_node_definition,
    # Video Frame Extraction
    VideoFrameExtractor,
    VideoExtractionConfig,
    VideoExtractionResult,
    VideoMetadata,
    ExtractedFrame,
    VideoCodec,
    VideoFrameExtractionNode,
    VideoNodeConfig,
    VideoNodeResult,
    get_video_extractor,
    get_video_node_definition,
)

__all__ = [
    # X12 Parser
    "X12Parser",
    "X12ParseResult",
    "X12Segment",
    "X12Element",
    "X12Claim",
    "X12ServiceLine",
    "X12Adjustment",
    "X12TransactionType",
    "X12ClaimStatus",
    "X12AdjustmentGroup",
    "parse_x12",
    "parse_x12_file",
    "x12_to_dict",
    "x12_to_json",
    # Flat File Parser
    "FlatFileParser",
    "FlatFileConfig",
    "FlatFileField",
    "FlatFileRecord",
    "FlatFileResult",
    "FlatFileNode",
    "FlatFileNodeConfig",
    "FlatFileNodeResult",
    "get_flatfile_parser",
    "get_flatfile_node_definition",
    # PDF Extractor
    "PDFExtractor",
    "PDFConfig",
    "PDFPage",
    "PDFMetadata",
    "PDFExtractionResult",
    "PDFNode",
    "PDFNodeConfig",
    "PDFNodeResult",
    "get_pdf_extractor",
    "get_pdf_node_definition",
    # CCDA/CDA Parser
    "CCDAParser",
    "CCDAParseResult",
    "CCDADocumentType",
    "CCDASectionCode",
    "CCDACode",
    "CCDAName",
    "CCDAAddress",
    "CCDATelecom",
    "CCDAPatient",
    "CCDAAuthor",
    "CCDAAllergy",
    "CCDAMedication",
    "CCDAProblem",
    "CCDAProcedure",
    "CCDAVitalSign",
    "CCDALabResult",
    "CCDAImmunization",
    "CCDAEncounter",
    "CCDASection",
    "CCDANode",
    "CCDANodeConfig",
    "CCDANodeResult",
    "parse_ccda",
    "parse_ccda_file",
    "ccda_to_dict",
    "ccda_to_fhir_bundle",
    "get_ccda_parser",
    "get_ccda_node_definition",
    # DICOM Parser
    "DICOMParser",
    "DICOMConfig",
    "DICOMParseResult",
    "DICOMMetadata",
    "DICOMPatient",
    "DICOMStudy",
    "DICOMSeries",
    "DICOMImage",
    "DICOMModality",
    "DICOMTransferSyntax",
    "DICOMNode",
    "DICOMNodeConfig",
    "DICOMNodeResult",
    "get_dicom_parser",
    "get_dicom_node_definition",
    # Audio Transcription
    "AudioTranscriber",
    "AudioTranscriptionConfig",
    "TranscriptionResult",
    "TranscriptionSegment",
    "AudioMetadata",
    "TranscriptionService",
    "AudioFormat",
    "AudioTranscriptionNode",
    "AudioNodeConfig",
    "AudioNodeResult",
    "get_audio_transcriber",
    "get_audio_node_definition",
    # Video Frame Extraction
    "VideoFrameExtractor",
    "VideoExtractionConfig",
    "VideoExtractionResult",
    "VideoMetadata",
    "ExtractedFrame",
    "VideoCodec",
    "VideoFrameExtractionNode",
    "VideoNodeConfig",
    "VideoNodeResult",
    "get_video_extractor",
    "get_video_node_definition",
]

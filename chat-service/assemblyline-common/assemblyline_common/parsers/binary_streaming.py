"""
Binary & Streaming Parsers for Logic Weaver.

Microservice-oriented parsers for binary data formats:
- DICOM: Medical imaging metadata extraction
- Audio: Transcription via external services (Whisper, AWS Transcribe, Google STT)
- Video: Frame extraction and analysis

Architecture:
- Flow nodes are lightweight, handling metadata and coordination
- Heavy processing is delegated to external microservices
- All operations are async for high throughput
- Supports both local processing and cloud service integration

Comparison with competitors:
| Feature              | MuleSoft | Apigee | Logic Weaver |
|---------------------|----------|--------|--------------|
| DICOM Parsing       | No       | No     | Native       |
| Audio Transcription | No       | No     | Multi-vendor |
| Video Processing    | No       | No     | Native       |
| Healthcare Focus    | Limited  | No     | Full         |
| Microservice Ready  | Yes      | Yes    | Yes          |
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union, BinaryIO
import asyncio
import base64
import hashlib
import json
import logging
import struct
import io

logger = logging.getLogger(__name__)


# =============================================================================
# DICOM Parser
# =============================================================================

class DICOMTransferSyntax(str, Enum):
    """DICOM Transfer Syntax UIDs."""
    IMPLICIT_VR_LITTLE = "1.2.840.10008.1.2"
    EXPLICIT_VR_LITTLE = "1.2.840.10008.1.2.1"
    EXPLICIT_VR_BIG = "1.2.840.10008.1.2.2"
    JPEG_BASELINE = "1.2.840.10008.1.2.4.50"
    JPEG_LOSSLESS = "1.2.840.10008.1.2.4.70"
    JPEG_2000 = "1.2.840.10008.1.2.4.91"


class DICOMModality(str, Enum):
    """Common DICOM imaging modalities."""
    CT = "CT"  # Computed Tomography
    MR = "MR"  # Magnetic Resonance
    US = "US"  # Ultrasound
    XR = "CR"  # Computed Radiography (X-Ray)
    DX = "DX"  # Digital Radiography
    MG = "MG"  # Mammography
    NM = "NM"  # Nuclear Medicine
    PT = "PT"  # PET
    RF = "RF"  # Fluoroscopy
    OT = "OT"  # Other


@dataclass
class DICOMPatient:
    """DICOM patient information."""
    patient_id: str = ""
    patient_name: str = ""
    birth_date: Optional[str] = None
    sex: str = ""
    age: Optional[str] = None
    weight: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "patient_id": self.patient_id,
            "patient_name": self.patient_name,
            "birth_date": self.birth_date,
            "sex": self.sex,
            "age": self.age,
            "weight": self.weight,
        }


@dataclass
class DICOMStudy:
    """DICOM study information."""
    study_instance_uid: str = ""
    study_id: str = ""
    study_date: Optional[str] = None
    study_time: Optional[str] = None
    study_description: str = ""
    accession_number: str = ""
    referring_physician: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "study_instance_uid": self.study_instance_uid,
            "study_id": self.study_id,
            "study_date": self.study_date,
            "study_time": self.study_time,
            "study_description": self.study_description,
            "accession_number": self.accession_number,
            "referring_physician": self.referring_physician,
        }


@dataclass
class DICOMSeries:
    """DICOM series information."""
    series_instance_uid: str = ""
    series_number: Optional[int] = None
    series_description: str = ""
    modality: str = ""
    body_part: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "series_instance_uid": self.series_instance_uid,
            "series_number": self.series_number,
            "series_description": self.series_description,
            "modality": self.modality,
            "body_part": self.body_part,
        }


@dataclass
class DICOMImage:
    """DICOM image/instance information."""
    sop_instance_uid: str = ""
    sop_class_uid: str = ""
    instance_number: Optional[int] = None
    rows: Optional[int] = None
    columns: Optional[int] = None
    bits_allocated: Optional[int] = None
    bits_stored: Optional[int] = None
    pixel_spacing: Optional[List[float]] = None
    slice_thickness: Optional[float] = None
    window_center: Optional[float] = None
    window_width: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sop_instance_uid": self.sop_instance_uid,
            "sop_class_uid": self.sop_class_uid,
            "instance_number": self.instance_number,
            "rows": self.rows,
            "columns": self.columns,
            "bits_allocated": self.bits_allocated,
            "bits_stored": self.bits_stored,
            "pixel_spacing": self.pixel_spacing,
            "slice_thickness": self.slice_thickness,
            "window_center": self.window_center,
            "window_width": self.window_width,
        }


@dataclass
class DICOMMetadata:
    """Complete DICOM metadata."""
    file_meta: Dict[str, Any] = field(default_factory=dict)
    patient: DICOMPatient = field(default_factory=DICOMPatient)
    study: DICOMStudy = field(default_factory=DICOMStudy)
    series: DICOMSeries = field(default_factory=DICOMSeries)
    image: DICOMImage = field(default_factory=DICOMImage)
    transfer_syntax: str = ""
    manufacturer: str = ""
    institution: str = ""
    raw_tags: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_meta": self.file_meta,
            "patient": self.patient.to_dict(),
            "study": self.study.to_dict(),
            "series": self.series.to_dict(),
            "image": self.image.to_dict(),
            "transfer_syntax": self.transfer_syntax,
            "manufacturer": self.manufacturer,
            "institution": self.institution,
            "raw_tags": self.raw_tags,
        }


@dataclass
class DICOMConfig:
    """DICOM parser configuration."""
    # What to extract
    extract_patient: bool = True
    extract_study: bool = True
    extract_series: bool = True
    extract_image: bool = True
    extract_raw_tags: bool = False

    # PHI handling
    anonymize: bool = False
    phi_fields_to_mask: List[str] = field(default_factory=lambda: [
        "PatientName", "PatientID", "PatientBirthDate",
    ])

    # External service integration
    pacs_url: Optional[str] = None  # e.g., Orthanc DICOM server
    pacs_auth: Optional[Dict[str, str]] = None

    # Processing options
    include_pixel_data_hash: bool = False
    max_tag_value_length: int = 1024


@dataclass
class DICOMParseResult:
    """Result of DICOM parsing."""
    success: bool
    metadata: Optional[DICOMMetadata] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    parse_time_ms: float = 0.0
    file_size_bytes: int = 0
    pixel_data_hash: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "errors": self.errors,
            "warnings": self.warnings,
            "parse_time_ms": self.parse_time_ms,
            "file_size_bytes": self.file_size_bytes,
            "pixel_data_hash": self.pixel_data_hash,
        }


# Common DICOM tags (group, element) -> name mapping
DICOM_TAGS = {
    (0x0008, 0x0016): "SOPClassUID",
    (0x0008, 0x0018): "SOPInstanceUID",
    (0x0008, 0x0020): "StudyDate",
    (0x0008, 0x0021): "SeriesDate",
    (0x0008, 0x0030): "StudyTime",
    (0x0008, 0x0050): "AccessionNumber",
    (0x0008, 0x0060): "Modality",
    (0x0008, 0x0070): "Manufacturer",
    (0x0008, 0x0080): "InstitutionName",
    (0x0008, 0x0090): "ReferringPhysicianName",
    (0x0008, 0x1030): "StudyDescription",
    (0x0008, 0x103E): "SeriesDescription",
    (0x0010, 0x0010): "PatientName",
    (0x0010, 0x0020): "PatientID",
    (0x0010, 0x0030): "PatientBirthDate",
    (0x0010, 0x0040): "PatientSex",
    (0x0010, 0x1010): "PatientAge",
    (0x0010, 0x1030): "PatientWeight",
    (0x0018, 0x0015): "BodyPartExamined",
    (0x0018, 0x0050): "SliceThickness",
    (0x0020, 0x000D): "StudyInstanceUID",
    (0x0020, 0x000E): "SeriesInstanceUID",
    (0x0020, 0x0010): "StudyID",
    (0x0020, 0x0011): "SeriesNumber",
    (0x0020, 0x0013): "InstanceNumber",
    (0x0028, 0x0010): "Rows",
    (0x0028, 0x0011): "Columns",
    (0x0028, 0x0100): "BitsAllocated",
    (0x0028, 0x0101): "BitsStored",
    (0x0028, 0x0030): "PixelSpacing",
    (0x0028, 0x1050): "WindowCenter",
    (0x0028, 0x1051): "WindowWidth",
    (0x7FE0, 0x0010): "PixelData",
}


class DICOMParser:
    """
    DICOM metadata parser.

    Extracts metadata from DICOM files without full pixel data parsing.
    For full DICOM processing, integrate with PACS servers like Orthanc.

    Microservice integration:
    - Can query PACS via DICOMweb (WADO-RS, STOW-RS, QIDO-RS)
    - Supports async operations for high throughput
    - Lightweight metadata extraction for flow processing
    """

    def __init__(self, config: Optional[DICOMConfig] = None):
        self.config = config or DICOMConfig()

    def parse(self, data: Union[bytes, BinaryIO]) -> DICOMParseResult:
        """Parse DICOM file and extract metadata."""
        import time
        start_time = time.time()

        if isinstance(data, bytes):
            stream = io.BytesIO(data)
            file_size = len(data)
        else:
            stream = data
            stream.seek(0, 2)
            file_size = stream.tell()
            stream.seek(0)

        result = DICOMParseResult(
            success=False,
            file_size_bytes=file_size,
        )

        try:
            metadata = self._parse_dicom(stream)
            result.metadata = metadata
            result.success = True

            if self.config.include_pixel_data_hash:
                result.pixel_data_hash = self._compute_pixel_hash(stream)

        except Exception as e:
            result.errors.append(f"Parse error: {str(e)}")
            logger.error(f"DICOM parse error: {e}")

        result.parse_time_ms = (time.time() - start_time) * 1000
        return result

    def _parse_dicom(self, stream: BinaryIO) -> DICOMMetadata:
        """Parse DICOM stream and extract metadata."""
        metadata = DICOMMetadata()
        tags = {}

        # Check DICOM preamble (128 bytes) and magic number
        preamble = stream.read(128)
        magic = stream.read(4)

        if magic != b"DICM":
            # Try without preamble (some DICOM files lack it)
            stream.seek(0)

        # Try to use pydicom if available for robust parsing
        try:
            import pydicom
            stream.seek(0)
            ds = pydicom.dcmread(stream, stop_before_pixels=True)
            return self._extract_from_pydicom(ds)
        except ImportError:
            # Fall back to basic parsing
            pass
        except Exception as e:
            logger.warning(f"pydicom parsing failed, using basic parser: {e}")

        # Basic DICOM tag parsing (limited but works without pydicom)
        stream.seek(132 if magic == b"DICM" else 0)
        tags = self._parse_tags_basic(stream)

        # Map tags to metadata structure
        self._map_tags_to_metadata(tags, metadata)

        return metadata

    def _extract_from_pydicom(self, ds) -> DICOMMetadata:
        """Extract metadata from pydicom dataset."""
        metadata = DICOMMetadata()

        # File meta
        if hasattr(ds, 'file_meta'):
            metadata.transfer_syntax = str(getattr(ds.file_meta, 'TransferSyntaxUID', ''))
            metadata.file_meta = {
                "media_storage_sop_class": str(getattr(ds.file_meta, 'MediaStorageSOPClassUID', '')),
                "media_storage_sop_instance": str(getattr(ds.file_meta, 'MediaStorageSOPInstanceUID', '')),
            }

        # Patient
        if self.config.extract_patient:
            patient = DICOMPatient()
            patient.patient_id = self._get_value(ds, 'PatientID', '')
            patient.patient_name = self._anonymize_if_needed(
                str(self._get_value(ds, 'PatientName', '')), 'PatientName'
            )
            patient.birth_date = self._anonymize_if_needed(
                self._get_value(ds, 'PatientBirthDate', None), 'PatientBirthDate'
            )
            patient.sex = self._get_value(ds, 'PatientSex', '')
            patient.age = self._get_value(ds, 'PatientAge', None)
            patient.weight = self._get_value(ds, 'PatientWeight', None)
            metadata.patient = patient

        # Study
        if self.config.extract_study:
            study = DICOMStudy()
            study.study_instance_uid = self._get_value(ds, 'StudyInstanceUID', '')
            study.study_id = self._get_value(ds, 'StudyID', '')
            study.study_date = self._get_value(ds, 'StudyDate', None)
            study.study_time = self._get_value(ds, 'StudyTime', None)
            study.study_description = self._get_value(ds, 'StudyDescription', '')
            study.accession_number = self._get_value(ds, 'AccessionNumber', '')
            study.referring_physician = self._get_value(ds, 'ReferringPhysicianName', '')
            metadata.study = study

        # Series
        if self.config.extract_series:
            series = DICOMSeries()
            series.series_instance_uid = self._get_value(ds, 'SeriesInstanceUID', '')
            series.series_number = self._get_value(ds, 'SeriesNumber', None)
            series.series_description = self._get_value(ds, 'SeriesDescription', '')
            series.modality = self._get_value(ds, 'Modality', '')
            series.body_part = self._get_value(ds, 'BodyPartExamined', '')
            metadata.series = series

        # Image
        if self.config.extract_image:
            image = DICOMImage()
            image.sop_instance_uid = self._get_value(ds, 'SOPInstanceUID', '')
            image.sop_class_uid = self._get_value(ds, 'SOPClassUID', '')
            image.instance_number = self._get_value(ds, 'InstanceNumber', None)
            image.rows = self._get_value(ds, 'Rows', None)
            image.columns = self._get_value(ds, 'Columns', None)
            image.bits_allocated = self._get_value(ds, 'BitsAllocated', None)
            image.bits_stored = self._get_value(ds, 'BitsStored', None)

            pixel_spacing = self._get_value(ds, 'PixelSpacing', None)
            if pixel_spacing:
                image.pixel_spacing = [float(x) for x in pixel_spacing]

            image.slice_thickness = self._get_value(ds, 'SliceThickness', None)
            image.window_center = self._get_value(ds, 'WindowCenter', None)
            image.window_width = self._get_value(ds, 'WindowWidth', None)
            metadata.image = image

        # General info
        metadata.manufacturer = self._get_value(ds, 'Manufacturer', '')
        metadata.institution = self._get_value(ds, 'InstitutionName', '')

        return metadata

    def _get_value(self, ds, tag_name: str, default: Any) -> Any:
        """Get value from dataset with default."""
        try:
            value = getattr(ds, tag_name, default)
            if value == default:
                return default
            # Handle MultiValue
            if hasattr(value, '__iter__') and not isinstance(value, str):
                return list(value)
            return value
        except Exception:
            return default

    def _anonymize_if_needed(self, value: Any, field_name: str) -> Any:
        """Anonymize PHI field if configured."""
        if self.config.anonymize and field_name in self.config.phi_fields_to_mask:
            if value:
                return "***ANONYMIZED***"
        return value

    def _parse_tags_basic(self, stream: BinaryIO) -> Dict[str, Any]:
        """Basic DICOM tag parser without pydicom."""
        tags = {}
        max_bytes = 10000  # Only parse first 10KB for metadata

        try:
            while stream.tell() < max_bytes:
                # Read tag (group, element)
                tag_bytes = stream.read(4)
                if len(tag_bytes) < 4:
                    break

                group, element = struct.unpack('<HH', tag_bytes)
                tag_key = (group, element)

                # Read VR (Value Representation) for explicit VR
                vr = stream.read(2).decode('ascii', errors='ignore')

                # Determine value length
                if vr in ('OB', 'OW', 'OF', 'SQ', 'UC', 'UN', 'UR', 'UT'):
                    stream.read(2)  # Reserved
                    length = struct.unpack('<I', stream.read(4))[0]
                else:
                    length = struct.unpack('<H', stream.read(2))[0]

                # Skip pixel data
                if tag_key == (0x7FE0, 0x0010):
                    break

                # Read value
                if length > 0 and length < self.config.max_tag_value_length:
                    value = stream.read(length)
                    if tag_key in DICOM_TAGS:
                        tag_name = DICOM_TAGS[tag_key]
                        tags[tag_name] = self._decode_value(value, vr)
                elif length > 0:
                    stream.seek(length, 1)  # Skip long values

        except Exception as e:
            logger.debug(f"Basic parser ended: {e}")

        return tags

    def _decode_value(self, value: bytes, vr: str) -> Any:
        """Decode DICOM value based on VR."""
        try:
            if vr in ('AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT',
                      'PN', 'SH', 'ST', 'TM', 'UC', 'UI', 'UR', 'UT'):
                return value.decode('ascii', errors='ignore').strip('\x00 ')
            elif vr == 'US':
                return struct.unpack('<H', value)[0]
            elif vr == 'UL':
                return struct.unpack('<I', value)[0]
            elif vr == 'SS':
                return struct.unpack('<h', value)[0]
            elif vr == 'SL':
                return struct.unpack('<i', value)[0]
            elif vr == 'FL':
                return struct.unpack('<f', value)[0]
            elif vr == 'FD':
                return struct.unpack('<d', value)[0]
            else:
                return value.hex()
        except Exception:
            return value.hex() if value else None

    def _map_tags_to_metadata(self, tags: Dict[str, Any], metadata: DICOMMetadata) -> None:
        """Map parsed tags to metadata structure."""
        # Patient
        if self.config.extract_patient:
            metadata.patient.patient_id = tags.get('PatientID', '')
            metadata.patient.patient_name = self._anonymize_if_needed(
                tags.get('PatientName', ''), 'PatientName'
            )
            metadata.patient.birth_date = self._anonymize_if_needed(
                tags.get('PatientBirthDate'), 'PatientBirthDate'
            )
            metadata.patient.sex = tags.get('PatientSex', '')
            metadata.patient.age = tags.get('PatientAge')

        # Study
        if self.config.extract_study:
            metadata.study.study_instance_uid = tags.get('StudyInstanceUID', '')
            metadata.study.study_id = tags.get('StudyID', '')
            metadata.study.study_date = tags.get('StudyDate')
            metadata.study.study_time = tags.get('StudyTime')
            metadata.study.study_description = tags.get('StudyDescription', '')
            metadata.study.accession_number = tags.get('AccessionNumber', '')

        # Series
        if self.config.extract_series:
            metadata.series.series_instance_uid = tags.get('SeriesInstanceUID', '')
            metadata.series.series_number = tags.get('SeriesNumber')
            metadata.series.series_description = tags.get('SeriesDescription', '')
            metadata.series.modality = tags.get('Modality', '')
            metadata.series.body_part = tags.get('BodyPartExamined', '')

        # Image
        if self.config.extract_image:
            metadata.image.sop_instance_uid = tags.get('SOPInstanceUID', '')
            metadata.image.sop_class_uid = tags.get('SOPClassUID', '')
            metadata.image.instance_number = tags.get('InstanceNumber')
            metadata.image.rows = tags.get('Rows')
            metadata.image.columns = tags.get('Columns')
            metadata.image.bits_allocated = tags.get('BitsAllocated')
            metadata.image.bits_stored = tags.get('BitsStored')

        # Raw tags if requested
        if self.config.extract_raw_tags:
            metadata.raw_tags = tags

        metadata.manufacturer = tags.get('Manufacturer', '')
        metadata.institution = tags.get('InstitutionName', '')

    def _compute_pixel_hash(self, stream: BinaryIO) -> Optional[str]:
        """Compute hash of pixel data for integrity verification."""
        try:
            # Find pixel data tag
            stream.seek(0)
            content = stream.read()
            pixel_marker = b'\xe0\x7f\x10\x00'  # (7FE0, 0010) in little endian
            pos = content.find(pixel_marker)
            if pos > 0:
                pixel_data = content[pos + 8:]  # Skip tag + VR + length
                return hashlib.sha256(pixel_data[:4096]).hexdigest()[:16]
        except Exception:
            pass
        return None

    async def query_pacs(
        self,
        study_uid: Optional[str] = None,
        patient_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query PACS server via DICOMweb (QIDO-RS)."""
        if not self.config.pacs_url:
            raise ValueError("PACS URL not configured")

        try:
            import httpx

            params = {}
            if study_uid:
                params["StudyInstanceUID"] = study_uid
            if patient_id:
                params["PatientID"] = patient_id

            async with httpx.AsyncClient() as client:
                headers = {"Accept": "application/dicom+json"}
                if self.config.pacs_auth:
                    headers.update(self.config.pacs_auth)

                url = f"{self.config.pacs_url}/studies"
                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                return response.json()

        except ImportError:
            raise ImportError("httpx required for PACS integration")


# =============================================================================
# Audio Transcription
# =============================================================================

class TranscriptionService(str, Enum):
    """Supported transcription services."""
    LOCAL_WHISPER = "local_whisper"  # OpenAI Whisper local
    OPENAI_API = "openai_api"  # OpenAI Whisper API
    AWS_TRANSCRIBE = "aws_transcribe"
    GOOGLE_STT = "google_stt"
    AZURE_STT = "azure_stt"
    DEEPGRAM = "deepgram"


class AudioFormat(str, Enum):
    """Supported audio formats."""
    WAV = "wav"
    MP3 = "mp3"
    FLAC = "flac"
    OGG = "ogg"
    M4A = "m4a"
    WEBM = "webm"


@dataclass
class AudioMetadata:
    """Audio file metadata."""
    duration_seconds: float = 0.0
    sample_rate: int = 0
    channels: int = 0
    bit_depth: Optional[int] = None
    format: str = ""
    codec: str = ""
    file_size_bytes: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "duration_seconds": self.duration_seconds,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "bit_depth": self.bit_depth,
            "format": self.format,
            "codec": self.codec,
            "file_size_bytes": self.file_size_bytes,
        }


@dataclass
class TranscriptionSegment:
    """Single segment of transcription."""
    start_time: float
    end_time: float
    text: str
    confidence: float = 0.0
    speaker: Optional[str] = None
    language: Optional[str] = None
    words: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "text": self.text,
            "confidence": self.confidence,
            "speaker": self.speaker,
            "language": self.language,
            "words": self.words,
        }


@dataclass
class TranscriptionResult:
    """Complete transcription result."""
    success: bool
    text: str = ""
    segments: List[TranscriptionSegment] = field(default_factory=list)
    language: str = ""
    language_confidence: float = 0.0
    audio_metadata: Optional[AudioMetadata] = None
    service_used: str = ""
    processing_time_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "text": self.text,
            "segments": [s.to_dict() for s in self.segments],
            "language": self.language,
            "language_confidence": self.language_confidence,
            "audio_metadata": self.audio_metadata.to_dict() if self.audio_metadata else None,
            "service_used": self.service_used,
            "processing_time_seconds": self.processing_time_seconds,
            "errors": self.errors,
        }


@dataclass
class AudioTranscriptionConfig:
    """Audio transcription configuration."""
    # Service selection
    service: TranscriptionService = TranscriptionService.OPENAI_API
    fallback_services: List[TranscriptionService] = field(default_factory=list)

    # Service credentials
    openai_api_key: Optional[str] = None
    aws_region: str = "us-east-1"
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    google_credentials_path: Optional[str] = None
    azure_key: Optional[str] = None
    azure_region: str = "eastus"
    deepgram_api_key: Optional[str] = None

    # Transcription options
    language: Optional[str] = None  # Auto-detect if None
    enable_speaker_diarization: bool = False
    enable_word_timestamps: bool = True
    enable_punctuation: bool = True
    vocabulary_boost: List[str] = field(default_factory=list)  # Domain terms

    # Processing
    max_audio_duration_seconds: int = 7200  # 2 hours
    chunk_duration_seconds: int = 300  # 5 minute chunks for long audio

    # Microservice endpoint (for local Whisper server)
    whisper_service_url: Optional[str] = None


class AudioTranscriber:
    """
    Audio transcription with multi-vendor support.

    Microservice integration:
    - Calls external transcription APIs (OpenAI, AWS, Google, Azure)
    - Can integrate with local Whisper service via HTTP
    - Supports fallback between services
    - Handles chunking for long audio files
    """

    def __init__(self, config: Optional[AudioTranscriptionConfig] = None):
        self.config = config or AudioTranscriptionConfig()

    async def transcribe(
        self,
        audio_data: Union[bytes, str],  # bytes or file path
    ) -> TranscriptionResult:
        """Transcribe audio using configured service."""
        import time
        start_time = time.time()

        result = TranscriptionResult(
            success=False,
            service_used=self.config.service.value,
        )

        # Get audio metadata
        try:
            if isinstance(audio_data, str):
                with open(audio_data, 'rb') as f:
                    audio_bytes = f.read()
            else:
                audio_bytes = audio_data

            result.audio_metadata = self._extract_audio_metadata(audio_bytes)
        except Exception as e:
            result.errors.append(f"Failed to read audio: {e}")
            return result

        # Try primary service, then fallbacks
        services_to_try = [self.config.service] + self.config.fallback_services

        for service in services_to_try:
            try:
                if service == TranscriptionService.OPENAI_API:
                    text, segments = await self._transcribe_openai(audio_bytes)
                elif service == TranscriptionService.LOCAL_WHISPER:
                    text, segments = await self._transcribe_local_whisper(audio_bytes)
                elif service == TranscriptionService.AWS_TRANSCRIBE:
                    text, segments = await self._transcribe_aws(audio_bytes)
                elif service == TranscriptionService.GOOGLE_STT:
                    text, segments = await self._transcribe_google(audio_bytes)
                elif service == TranscriptionService.AZURE_STT:
                    text, segments = await self._transcribe_azure(audio_bytes)
                elif service == TranscriptionService.DEEPGRAM:
                    text, segments = await self._transcribe_deepgram(audio_bytes)
                else:
                    continue

                result.success = True
                result.text = text
                result.segments = segments
                result.service_used = service.value
                break

            except Exception as e:
                result.errors.append(f"{service.value}: {str(e)}")
                logger.warning(f"Transcription failed with {service.value}: {e}")
                continue

        result.processing_time_seconds = time.time() - start_time
        return result

    def _extract_audio_metadata(self, audio_bytes: bytes) -> AudioMetadata:
        """Extract audio file metadata."""
        metadata = AudioMetadata(file_size_bytes=len(audio_bytes))

        # Try to detect format from magic bytes
        if audio_bytes[:4] == b'RIFF':
            metadata.format = "wav"
            # Parse WAV header
            try:
                import struct
                channels = struct.unpack('<H', audio_bytes[22:24])[0]
                sample_rate = struct.unpack('<I', audio_bytes[24:28])[0]
                bit_depth = struct.unpack('<H', audio_bytes[34:36])[0]
                data_size = struct.unpack('<I', audio_bytes[40:44])[0]

                metadata.channels = channels
                metadata.sample_rate = sample_rate
                metadata.bit_depth = bit_depth
                metadata.duration_seconds = data_size / (sample_rate * channels * bit_depth / 8)
            except Exception:
                pass

        elif audio_bytes[:3] == b'ID3' or audio_bytes[:2] == b'\xff\xfb':
            metadata.format = "mp3"
        elif audio_bytes[:4] == b'fLaC':
            metadata.format = "flac"
        elif audio_bytes[:4] == b'OggS':
            metadata.format = "ogg"

        return metadata

    async def _transcribe_openai(
        self,
        audio_bytes: bytes,
    ) -> tuple[str, List[TranscriptionSegment]]:
        """Transcribe using OpenAI Whisper API."""
        if not self.config.openai_api_key:
            raise ValueError("OpenAI API key not configured")

        try:
            import httpx

            async with httpx.AsyncClient(timeout=300) as client:
                files = {"file": ("audio.wav", audio_bytes, "audio/wav")}
                data = {
                    "model": "whisper-1",
                    "response_format": "verbose_json",
                }
                if self.config.language:
                    data["language"] = self.config.language

                response = await client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {self.config.openai_api_key}"},
                    files=files,
                    data=data,
                )
                response.raise_for_status()
                result = response.json()

                text = result.get("text", "")
                segments = []

                for seg in result.get("segments", []):
                    segments.append(TranscriptionSegment(
                        start_time=seg.get("start", 0),
                        end_time=seg.get("end", 0),
                        text=seg.get("text", ""),
                        confidence=seg.get("avg_logprob", 0),
                    ))

                return text, segments

        except ImportError:
            raise ImportError("httpx required for OpenAI API")

    async def _transcribe_local_whisper(
        self,
        audio_bytes: bytes,
    ) -> tuple[str, List[TranscriptionSegment]]:
        """Transcribe using local Whisper service."""
        if not self.config.whisper_service_url:
            raise ValueError("Whisper service URL not configured")

        try:
            import httpx

            async with httpx.AsyncClient(timeout=600) as client:
                files = {"audio": ("audio.wav", audio_bytes, "audio/wav")}
                data = {}
                if self.config.language:
                    data["language"] = self.config.language
                if self.config.enable_word_timestamps:
                    data["word_timestamps"] = "true"

                response = await client.post(
                    f"{self.config.whisper_service_url}/transcribe",
                    files=files,
                    data=data,
                )
                response.raise_for_status()
                result = response.json()

                text = result.get("text", "")
                segments = []

                for seg in result.get("segments", []):
                    segments.append(TranscriptionSegment(
                        start_time=seg.get("start", 0),
                        end_time=seg.get("end", 0),
                        text=seg.get("text", ""),
                    ))

                return text, segments

        except ImportError:
            raise ImportError("httpx required for Whisper service")

    async def _transcribe_aws(
        self,
        audio_bytes: bytes,
    ) -> tuple[str, List[TranscriptionSegment]]:
        """Transcribe using AWS Transcribe."""
        try:
            import boto3
            import uuid

            # Upload to S3 first (AWS Transcribe requires S3 URI)
            s3_client = boto3.client(
                's3',
                region_name=self.config.aws_region,
                aws_access_key_id=self.config.aws_access_key,
                aws_secret_access_key=self.config.aws_secret_key,
            )

            bucket = "logic-weaver-temp"
            key = f"transcribe/{uuid.uuid4()}.wav"

            s3_client.put_object(Bucket=bucket, Key=key, Body=audio_bytes)

            transcribe_client = boto3.client(
                'transcribe',
                region_name=self.config.aws_region,
                aws_access_key_id=self.config.aws_access_key,
                aws_secret_access_key=self.config.aws_secret_key,
            )

            job_name = f"lw-{uuid.uuid4()}"
            transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': f"s3://{bucket}/{key}"},
                MediaFormat='wav',
                LanguageCode=self.config.language or 'en-US',
            )

            # Poll for completion
            while True:
                status = transcribe_client.get_transcription_job(
                    TranscriptionJobName=job_name
                )
                job_status = status['TranscriptionJob']['TranscriptionJobStatus']
                if job_status in ['COMPLETED', 'FAILED']:
                    break
                await asyncio.sleep(5)

            if job_status == 'FAILED':
                raise Exception("AWS Transcribe job failed")

            # Get results
            import httpx
            transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
            async with httpx.AsyncClient() as client:
                response = await client.get(transcript_uri)
                result = response.json()

            text = result['results']['transcripts'][0]['transcript']
            segments = []

            for item in result['results'].get('items', []):
                if item['type'] == 'pronunciation':
                    segments.append(TranscriptionSegment(
                        start_time=float(item.get('start_time', 0)),
                        end_time=float(item.get('end_time', 0)),
                        text=item['alternatives'][0]['content'],
                        confidence=float(item['alternatives'][0].get('confidence', 0)),
                    ))

            # Cleanup
            s3_client.delete_object(Bucket=bucket, Key=key)

            return text, segments

        except ImportError:
            raise ImportError("boto3 required for AWS Transcribe")

    async def _transcribe_google(
        self,
        audio_bytes: bytes,
    ) -> tuple[str, List[TranscriptionSegment]]:
        """Transcribe using Google Speech-to-Text."""
        try:
            from google.cloud import speech_v1

            client = speech_v1.SpeechClient()

            audio = speech_v1.RecognitionAudio(content=audio_bytes)
            config = speech_v1.RecognitionConfig(
                encoding=speech_v1.RecognitionConfig.AudioEncoding.LINEAR16,
                language_code=self.config.language or "en-US",
                enable_word_time_offsets=self.config.enable_word_timestamps,
                enable_automatic_punctuation=self.config.enable_punctuation,
            )

            response = client.recognize(config=config, audio=audio)

            text_parts = []
            segments = []

            for result in response.results:
                alt = result.alternatives[0]
                text_parts.append(alt.transcript)

                for word_info in alt.words:
                    segments.append(TranscriptionSegment(
                        start_time=word_info.start_time.total_seconds(),
                        end_time=word_info.end_time.total_seconds(),
                        text=word_info.word,
                        confidence=alt.confidence,
                    ))

            return " ".join(text_parts), segments

        except ImportError:
            raise ImportError("google-cloud-speech required for Google STT")

    async def _transcribe_azure(
        self,
        audio_bytes: bytes,
    ) -> tuple[str, List[TranscriptionSegment]]:
        """Transcribe using Azure Speech-to-Text."""
        try:
            import httpx

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"https://{self.config.azure_region}.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1",
                    headers={
                        "Ocp-Apim-Subscription-Key": self.config.azure_key,
                        "Content-Type": "audio/wav",
                    },
                    params={"language": self.config.language or "en-US"},
                    content=audio_bytes,
                )
                response.raise_for_status()
                result = response.json()

                text = result.get("DisplayText", "")
                return text, []

        except ImportError:
            raise ImportError("httpx required for Azure STT")

    async def _transcribe_deepgram(
        self,
        audio_bytes: bytes,
    ) -> tuple[str, List[TranscriptionSegment]]:
        """Transcribe using Deepgram."""
        if not self.config.deepgram_api_key:
            raise ValueError("Deepgram API key not configured")

        try:
            import httpx

            async with httpx.AsyncClient() as client:
                params = {
                    "punctuate": str(self.config.enable_punctuation).lower(),
                    "diarize": str(self.config.enable_speaker_diarization).lower(),
                }
                if self.config.language:
                    params["language"] = self.config.language

                response = await client.post(
                    "https://api.deepgram.com/v1/listen",
                    headers={
                        "Authorization": f"Token {self.config.deepgram_api_key}",
                        "Content-Type": "audio/wav",
                    },
                    params=params,
                    content=audio_bytes,
                )
                response.raise_for_status()
                result = response.json()

                channel = result['results']['channels'][0]
                alt = channel['alternatives'][0]

                text = alt.get('transcript', '')
                segments = []

                for word in alt.get('words', []):
                    segments.append(TranscriptionSegment(
                        start_time=word.get('start', 0),
                        end_time=word.get('end', 0),
                        text=word.get('word', ''),
                        confidence=word.get('confidence', 0),
                        speaker=word.get('speaker'),
                    ))

                return text, segments

        except ImportError:
            raise ImportError("httpx required for Deepgram")


# =============================================================================
# Video Frame Extraction
# =============================================================================

class VideoCodec(str, Enum):
    """Common video codecs."""
    H264 = "h264"
    H265 = "hevc"
    VP9 = "vp9"
    AV1 = "av1"
    MPEG4 = "mpeg4"
    MJPEG = "mjpeg"


@dataclass
class VideoMetadata:
    """Video file metadata."""
    duration_seconds: float = 0.0
    width: int = 0
    height: int = 0
    fps: float = 0.0
    codec: str = ""
    bitrate: int = 0
    total_frames: int = 0
    file_size_bytes: int = 0
    has_audio: bool = False
    audio_codec: str = ""
    creation_time: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "duration_seconds": self.duration_seconds,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "codec": self.codec,
            "bitrate": self.bitrate,
            "total_frames": self.total_frames,
            "file_size_bytes": self.file_size_bytes,
            "has_audio": self.has_audio,
            "audio_codec": self.audio_codec,
            "creation_time": self.creation_time,
        }


@dataclass
class ExtractedFrame:
    """Single extracted video frame."""
    frame_number: int
    timestamp_seconds: float
    width: int
    height: int
    format: str = "jpeg"
    data: Optional[bytes] = None
    data_base64: Optional[str] = None
    file_path: Optional[str] = None

    def to_dict(self, include_data: bool = False) -> Dict[str, Any]:
        result = {
            "frame_number": self.frame_number,
            "timestamp_seconds": self.timestamp_seconds,
            "width": self.width,
            "height": self.height,
            "format": self.format,
            "file_path": self.file_path,
        }
        if include_data and self.data_base64:
            result["data_base64"] = self.data_base64
        return result


@dataclass
class VideoExtractionResult:
    """Result of video frame extraction."""
    success: bool
    metadata: Optional[VideoMetadata] = None
    frames: List[ExtractedFrame] = field(default_factory=list)
    processing_time_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)

    def to_dict(self, include_frame_data: bool = False) -> Dict[str, Any]:
        return {
            "success": self.success,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "frames": [f.to_dict(include_frame_data) for f in self.frames],
            "processing_time_seconds": self.processing_time_seconds,
            "errors": self.errors,
        }


@dataclass
class VideoExtractionConfig:
    """Video frame extraction configuration."""
    # Extraction method
    use_ffmpeg_service: bool = True  # Use external FFmpeg microservice
    ffmpeg_service_url: Optional[str] = None
    ffmpeg_binary_path: str = "ffmpeg"  # Local ffmpeg path

    # Frame selection
    extract_mode: str = "interval"  # interval, keyframes, scene_change, specific
    interval_seconds: float = 1.0  # For interval mode
    max_frames: int = 100
    specific_timestamps: List[float] = field(default_factory=list)

    # Output format
    output_format: str = "jpeg"  # jpeg, png, webp
    output_quality: int = 85  # 1-100 for jpeg
    output_width: Optional[int] = None  # Resize width (maintains aspect)
    output_height: Optional[int] = None

    # Storage
    save_to_disk: bool = False
    output_directory: str = "/tmp/frames"
    include_base64: bool = True  # Include base64 in result


class VideoFrameExtractor:
    """
    Video frame extraction with FFmpeg integration.

    Microservice integration:
    - Can use external FFmpeg service via HTTP
    - Supports local FFmpeg for direct processing
    - Extracts metadata without full decode
    - Configurable frame selection strategies
    """

    def __init__(self, config: Optional[VideoExtractionConfig] = None):
        self.config = config or VideoExtractionConfig()

    async def extract(
        self,
        video_data: Union[bytes, str],  # bytes or file path
    ) -> VideoExtractionResult:
        """Extract frames from video."""
        import time
        start_time = time.time()

        result = VideoExtractionResult(success=False)

        try:
            # Handle input
            if isinstance(video_data, str):
                video_path = video_data
            else:
                # Write to temp file
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as f:
                    f.write(video_data)
                    video_path = f.name

            # Extract metadata first
            result.metadata = await self._extract_metadata(video_path)

            # Extract frames
            if self.config.use_ffmpeg_service and self.config.ffmpeg_service_url:
                frames = await self._extract_via_service(video_path, result.metadata)
            else:
                frames = await self._extract_local(video_path, result.metadata)

            result.frames = frames
            result.success = True

        except Exception as e:
            result.errors.append(str(e))
            logger.error(f"Video extraction error: {e}")

        result.processing_time_seconds = time.time() - start_time
        return result

    async def _extract_metadata(self, video_path: str) -> VideoMetadata:
        """Extract video metadata using ffprobe."""
        import subprocess
        import json

        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            video_path,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                raise Exception(f"ffprobe failed: {stderr.decode()}")

            data = json.loads(stdout.decode())
            metadata = VideoMetadata()

            # Parse format info
            fmt = data.get('format', {})
            metadata.duration_seconds = float(fmt.get('duration', 0))
            metadata.file_size_bytes = int(fmt.get('size', 0))
            metadata.bitrate = int(fmt.get('bit_rate', 0))
            metadata.creation_time = fmt.get('tags', {}).get('creation_time')

            # Parse streams
            for stream in data.get('streams', []):
                if stream['codec_type'] == 'video':
                    metadata.width = stream.get('width', 0)
                    metadata.height = stream.get('height', 0)
                    metadata.codec = stream.get('codec_name', '')

                    # Calculate FPS
                    fps_str = stream.get('r_frame_rate', '0/1')
                    if '/' in fps_str:
                        num, den = fps_str.split('/')
                        metadata.fps = float(num) / float(den) if float(den) > 0 else 0
                    else:
                        metadata.fps = float(fps_str)

                    metadata.total_frames = int(stream.get('nb_frames', 0))

                elif stream['codec_type'] == 'audio':
                    metadata.has_audio = True
                    metadata.audio_codec = stream.get('codec_name', '')

            return metadata

        except FileNotFoundError:
            logger.warning("ffprobe not found, returning minimal metadata")
            return VideoMetadata()

    async def _extract_local(
        self,
        video_path: str,
        metadata: VideoMetadata,
    ) -> List[ExtractedFrame]:
        """Extract frames using local FFmpeg."""
        import subprocess
        import tempfile
        import os

        frames = []
        output_dir = self.config.output_directory if self.config.save_to_disk else tempfile.mkdtemp()

        # Build FFmpeg command based on mode
        cmd = [self.config.ffmpeg_binary_path, "-y", "-i", video_path]

        if self.config.extract_mode == "interval":
            cmd.extend(["-vf", f"fps=1/{self.config.interval_seconds}"])
        elif self.config.extract_mode == "keyframes":
            cmd.extend(["-vf", "select=eq(pict_type\\,I)"])
        elif self.config.extract_mode == "scene_change":
            cmd.extend(["-vf", "select=gt(scene\\,0.3)"])

        # Resize if specified
        if self.config.output_width or self.config.output_height:
            w = self.config.output_width or -1
            h = self.config.output_height or -1
            cmd.extend(["-vf", f"scale={w}:{h}"])

        # Limit frames
        cmd.extend(["-frames:v", str(self.config.max_frames)])

        # Output format
        if self.config.output_format == "jpeg":
            cmd.extend(["-q:v", str((100 - self.config.output_quality) // 3 + 1)])

        output_pattern = os.path.join(output_dir, f"frame_%04d.{self.config.output_format}")
        cmd.append(output_pattern)

        # Run FFmpeg
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise Exception(f"FFmpeg failed: {stderr.decode()}")

        # Collect extracted frames
        for i in range(1, self.config.max_frames + 1):
            frame_path = os.path.join(output_dir, f"frame_{i:04d}.{self.config.output_format}")
            if not os.path.exists(frame_path):
                break

            frame = ExtractedFrame(
                frame_number=i,
                timestamp_seconds=(i - 1) * self.config.interval_seconds,
                width=self.config.output_width or metadata.width,
                height=self.config.output_height or metadata.height,
                format=self.config.output_format,
            )

            if self.config.save_to_disk:
                frame.file_path = frame_path
            else:
                with open(frame_path, 'rb') as f:
                    frame.data = f.read()
                if self.config.include_base64:
                    frame.data_base64 = base64.b64encode(frame.data).decode()
                os.unlink(frame_path)

            frames.append(frame)

        # Cleanup temp dir if not saving
        if not self.config.save_to_disk:
            import shutil
            shutil.rmtree(output_dir, ignore_errors=True)

        return frames

    async def _extract_via_service(
        self,
        video_path: str,
        metadata: VideoMetadata,
    ) -> List[ExtractedFrame]:
        """Extract frames using external FFmpeg service."""
        if not self.config.ffmpeg_service_url:
            raise ValueError("FFmpeg service URL not configured")

        try:
            import httpx

            with open(video_path, 'rb') as f:
                video_bytes = f.read()

            async with httpx.AsyncClient(timeout=300) as client:
                response = await client.post(
                    f"{self.config.ffmpeg_service_url}/extract-frames",
                    files={"video": ("video.mp4", video_bytes, "video/mp4")},
                    data={
                        "mode": self.config.extract_mode,
                        "interval": str(self.config.interval_seconds),
                        "max_frames": str(self.config.max_frames),
                        "format": self.config.output_format,
                        "quality": str(self.config.output_quality),
                    },
                )
                response.raise_for_status()
                result = response.json()

                frames = []
                for frame_data in result.get("frames", []):
                    frame = ExtractedFrame(
                        frame_number=frame_data.get("frame_number", 0),
                        timestamp_seconds=frame_data.get("timestamp", 0),
                        width=frame_data.get("width", 0),
                        height=frame_data.get("height", 0),
                        format=self.config.output_format,
                        data_base64=frame_data.get("data"),
                    )
                    frames.append(frame)

                return frames

        except ImportError:
            raise ImportError("httpx required for FFmpeg service")


# =============================================================================
# Flow Node Definitions
# =============================================================================

@dataclass
class DICOMNodeConfig:
    """Configuration for DICOM flow node."""
    extract_patient: bool = True
    extract_study: bool = True
    extract_series: bool = True
    extract_image: bool = True
    anonymize: bool = False
    pacs_url: Optional[str] = None


@dataclass
class DICOMNodeResult:
    """Result from DICOM flow node."""
    success: bool
    metadata: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)


class DICOMNode:
    """
    DICOM Parser Flow Node.

    Extracts metadata from DICOM medical images for use in integration flows.
    Lightweight processing - delegates heavy PACS operations to external services.
    """

    NODE_TYPE = "dicom_parser"
    NODE_CATEGORY = "parsers"
    NODE_LABEL = "DICOM Parser"
    NODE_DESCRIPTION = "Extract metadata from DICOM medical images"

    def __init__(self, config: Optional[DICOMNodeConfig] = None):
        self.config = config or DICOMNodeConfig()
        parser_config = DICOMConfig(
            extract_patient=self.config.extract_patient,
            extract_study=self.config.extract_study,
            extract_series=self.config.extract_series,
            extract_image=self.config.extract_image,
            anonymize=self.config.anonymize,
            pacs_url=self.config.pacs_url,
        )
        self.parser = DICOMParser(parser_config)

    async def execute(self, input_data: Dict[str, Any]) -> DICOMNodeResult:
        """Execute DICOM parsing."""
        try:
            # Get DICOM data from input
            dicom_data = input_data.get("data") or input_data.get("content")
            if isinstance(dicom_data, str):
                # Base64 encoded
                dicom_data = base64.b64decode(dicom_data)

            result = self.parser.parse(dicom_data)

            return DICOMNodeResult(
                success=result.success,
                metadata=result.to_dict(),
                errors=result.errors,
            )

        except Exception as e:
            return DICOMNodeResult(
                success=False,
                errors=[str(e)],
            )


@dataclass
class AudioNodeConfig:
    """Configuration for Audio Transcription flow node."""
    service: str = "openai_api"
    language: Optional[str] = None
    enable_timestamps: bool = True
    openai_api_key: Optional[str] = None
    whisper_service_url: Optional[str] = None


@dataclass
class AudioNodeResult:
    """Result from Audio Transcription flow node."""
    success: bool
    text: str = ""
    segments: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)


class AudioTranscriptionNode:
    """
    Audio Transcription Flow Node.

    Transcribes audio files using external services (Whisper, AWS, Google, etc.).
    Supports multiple vendors with automatic fallback.
    """

    NODE_TYPE = "audio_transcription"
    NODE_CATEGORY = "parsers"
    NODE_LABEL = "Audio Transcription"
    NODE_DESCRIPTION = "Transcribe audio files to text"

    def __init__(self, config: Optional[AudioNodeConfig] = None):
        self.config = config or AudioNodeConfig()
        transcriber_config = AudioTranscriptionConfig(
            service=TranscriptionService(self.config.service),
            language=self.config.language,
            enable_word_timestamps=self.config.enable_timestamps,
            openai_api_key=self.config.openai_api_key,
            whisper_service_url=self.config.whisper_service_url,
        )
        self.transcriber = AudioTranscriber(transcriber_config)

    async def execute(self, input_data: Dict[str, Any]) -> AudioNodeResult:
        """Execute audio transcription."""
        try:
            audio_data = input_data.get("data") or input_data.get("content")
            if isinstance(audio_data, str) and not audio_data.startswith("/"):
                # Base64 encoded
                audio_data = base64.b64decode(audio_data)

            result = await self.transcriber.transcribe(audio_data)

            return AudioNodeResult(
                success=result.success,
                text=result.text,
                segments=[s.to_dict() for s in result.segments],
                metadata=result.audio_metadata.to_dict() if result.audio_metadata else None,
                errors=result.errors,
            )

        except Exception as e:
            return AudioNodeResult(
                success=False,
                errors=[str(e)],
            )


@dataclass
class VideoNodeConfig:
    """Configuration for Video Frame Extraction flow node."""
    extract_mode: str = "interval"
    interval_seconds: float = 1.0
    max_frames: int = 10
    output_format: str = "jpeg"
    include_base64: bool = True
    ffmpeg_service_url: Optional[str] = None


@dataclass
class VideoNodeResult:
    """Result from Video Frame Extraction flow node."""
    success: bool
    metadata: Optional[Dict[str, Any]] = None
    frames: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class VideoFrameExtractionNode:
    """
    Video Frame Extraction Flow Node.

    Extracts frames from video files for analysis or processing.
    Uses FFmpeg (local or service) for video processing.
    """

    NODE_TYPE = "video_frame_extraction"
    NODE_CATEGORY = "parsers"
    NODE_LABEL = "Video Frame Extractor"
    NODE_DESCRIPTION = "Extract frames from video files"

    def __init__(self, config: Optional[VideoNodeConfig] = None):
        self.config = config or VideoNodeConfig()
        extractor_config = VideoExtractionConfig(
            extract_mode=self.config.extract_mode,
            interval_seconds=self.config.interval_seconds,
            max_frames=self.config.max_frames,
            output_format=self.config.output_format,
            include_base64=self.config.include_base64,
            ffmpeg_service_url=self.config.ffmpeg_service_url,
            use_ffmpeg_service=bool(self.config.ffmpeg_service_url),
        )
        self.extractor = VideoFrameExtractor(extractor_config)

    async def execute(self, input_data: Dict[str, Any]) -> VideoNodeResult:
        """Execute video frame extraction."""
        try:
            video_data = input_data.get("data") or input_data.get("content") or input_data.get("file_path")

            if isinstance(video_data, str) and not video_data.startswith("/"):
                # Base64 encoded
                video_data = base64.b64decode(video_data)

            result = await self.extractor.extract(video_data)

            return VideoNodeResult(
                success=result.success,
                metadata=result.metadata.to_dict() if result.metadata else None,
                frames=[f.to_dict(self.config.include_base64) for f in result.frames],
                errors=result.errors,
            )

        except Exception as e:
            return VideoNodeResult(
                success=False,
                errors=[str(e)],
            )


# =============================================================================
# Helper Functions
# =============================================================================

def get_dicom_parser(config: Optional[DICOMConfig] = None) -> DICOMParser:
    """Get a DICOM parser instance."""
    return DICOMParser(config)


def get_audio_transcriber(config: Optional[AudioTranscriptionConfig] = None) -> AudioTranscriber:
    """Get an audio transcriber instance."""
    return AudioTranscriber(config)


def get_video_extractor(config: Optional[VideoExtractionConfig] = None) -> VideoFrameExtractor:
    """Get a video frame extractor instance."""
    return VideoFrameExtractor(config)


def get_dicom_node_definition() -> Dict[str, Any]:
    """Get DICOM node definition for flow designer."""
    return {
        "type": DICOMNode.NODE_TYPE,
        "category": DICOMNode.NODE_CATEGORY,
        "label": DICOMNode.NODE_LABEL,
        "description": DICOMNode.NODE_DESCRIPTION,
        "inputs": [{"name": "data", "type": "binary", "label": "DICOM Data"}],
        "outputs": [
            {"name": "metadata", "type": "object", "label": "DICOM Metadata"},
            {"name": "patient", "type": "object", "label": "Patient Info"},
            {"name": "study", "type": "object", "label": "Study Info"},
        ],
        "config": [
            {"name": "extract_patient", "type": "boolean", "default": True},
            {"name": "extract_study", "type": "boolean", "default": True},
            {"name": "extract_series", "type": "boolean", "default": True},
            {"name": "extract_image", "type": "boolean", "default": True},
            {"name": "anonymize", "type": "boolean", "default": False},
            {"name": "pacs_url", "type": "string", "label": "PACS Server URL"},
        ],
    }


def get_audio_node_definition() -> Dict[str, Any]:
    """Get Audio Transcription node definition for flow designer."""
    return {
        "type": AudioTranscriptionNode.NODE_TYPE,
        "category": AudioTranscriptionNode.NODE_CATEGORY,
        "label": AudioTranscriptionNode.NODE_LABEL,
        "description": AudioTranscriptionNode.NODE_DESCRIPTION,
        "inputs": [{"name": "data", "type": "binary", "label": "Audio Data"}],
        "outputs": [
            {"name": "text", "type": "string", "label": "Transcribed Text"},
            {"name": "segments", "type": "array", "label": "Time-stamped Segments"},
        ],
        "config": [
            {
                "name": "service",
                "type": "select",
                "options": [s.value for s in TranscriptionService],
                "default": "openai_api",
            },
            {"name": "language", "type": "string", "label": "Language Code"},
            {"name": "enable_timestamps", "type": "boolean", "default": True},
            {"name": "openai_api_key", "type": "secret", "label": "OpenAI API Key"},
            {"name": "whisper_service_url", "type": "string", "label": "Whisper Service URL"},
        ],
    }


def get_video_node_definition() -> Dict[str, Any]:
    """Get Video Frame Extraction node definition for flow designer."""
    return {
        "type": VideoFrameExtractionNode.NODE_TYPE,
        "category": VideoFrameExtractionNode.NODE_CATEGORY,
        "label": VideoFrameExtractionNode.NODE_LABEL,
        "description": VideoFrameExtractionNode.NODE_DESCRIPTION,
        "inputs": [{"name": "data", "type": "binary", "label": "Video Data"}],
        "outputs": [
            {"name": "metadata", "type": "object", "label": "Video Metadata"},
            {"name": "frames", "type": "array", "label": "Extracted Frames"},
        ],
        "config": [
            {
                "name": "extract_mode",
                "type": "select",
                "options": ["interval", "keyframes", "scene_change"],
                "default": "interval",
            },
            {"name": "interval_seconds", "type": "number", "default": 1.0},
            {"name": "max_frames", "type": "number", "default": 10},
            {
                "name": "output_format",
                "type": "select",
                "options": ["jpeg", "png", "webp"],
                "default": "jpeg",
            },
            {"name": "include_base64", "type": "boolean", "default": True},
            {"name": "ffmpeg_service_url", "type": "string", "label": "FFmpeg Service URL"},
        ],
    }

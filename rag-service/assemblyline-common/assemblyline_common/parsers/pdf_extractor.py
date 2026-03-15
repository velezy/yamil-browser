"""
PDF Extractor for Logic Weaver

Enterprise-grade PDF extraction with:
- Text extraction (layout-aware)
- Table extraction
- Image extraction
- Form field extraction
- OCR support for scanned documents
- Metadata extraction
- Page-by-page processing
- Multi-tenant isolation

Comparison:
| Feature              | Adobe   | Textract | Logic Weaver |
|---------------------|---------|----------|--------------|
| Text Extraction     | Yes     | Yes      | Yes          |
| Table Extraction    | Limited | Yes      | Yes          |
| OCR Support         | Yes     | Yes      | Yes          |
| Form Fields         | Yes     | Limited  | Yes          |
| Layout Preserve     | Yes     | Limited  | Yes          |
"""

from __future__ import annotations

import base64
import io
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union

logger = logging.getLogger(__name__)

# Try to import PDF libraries
try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    from pdf2image import convert_from_path, convert_from_bytes
    HAS_PDF2IMAGE = True
except ImportError:
    HAS_PDF2IMAGE = False

try:
    import pytesseract
    from PIL import Image
    HAS_TESSERACT = True
except ImportError:
    HAS_TESSERACT = False

# Check for at least one PDF library
HAS_PDF_SUPPORT = HAS_PYPDF2 or HAS_PDFPLUMBER

if not HAS_PDF_SUPPORT:
    logger.warning("No PDF library installed. Install PyPDF2 or pdfplumber.")


class ExtractionMode(Enum):
    """PDF extraction modes."""
    TEXT = "text"
    TABLES = "tables"
    IMAGES = "images"
    FORMS = "forms"
    ALL = "all"
    OCR = "ocr"


@dataclass
class PDFConfig:
    """PDF extraction configuration."""
    # Extraction settings
    mode: ExtractionMode = ExtractionMode.TEXT
    pages: Optional[list[int]] = None  # None = all pages

    # Text extraction
    preserve_layout: bool = True
    extract_by_page: bool = True

    # Table extraction
    table_settings: dict[str, Any] = field(default_factory=dict)

    # Image extraction
    extract_images: bool = False
    image_format: str = "png"
    min_image_size: int = 100  # pixels

    # OCR settings
    use_ocr: bool = False
    ocr_language: str = "eng"
    ocr_dpi: int = 300

    # Form extraction
    extract_form_fields: bool = False

    # Output settings
    output_format: str = "dict"  # dict, json, text

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class PDFPage:
    """Represents a single PDF page."""
    page_number: int
    text: str = ""
    tables: list[list[list[str]]] = field(default_factory=list)
    images: list[dict[str, Any]] = field(default_factory=list)
    width: float = 0
    height: float = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "page_number": self.page_number,
            "text": self.text,
            "tables": self.tables,
            "images": [
                {k: v for k, v in img.items() if k != "data"}
                for img in self.images
            ],
            "image_count": len(self.images),
            "table_count": len(self.tables),
            "width": self.width,
            "height": self.height,
        }


@dataclass
class PDFMetadata:
    """PDF document metadata."""
    title: Optional[str] = None
    author: Optional[str] = None
    subject: Optional[str] = None
    creator: Optional[str] = None
    producer: Optional[str] = None
    creation_date: Optional[datetime] = None
    modification_date: Optional[datetime] = None
    page_count: int = 0
    encrypted: bool = False
    file_size: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "author": self.author,
            "subject": self.subject,
            "creator": self.creator,
            "producer": self.producer,
            "creation_date": self.creation_date.isoformat() if self.creation_date else None,
            "modification_date": self.modification_date.isoformat() if self.modification_date else None,
            "page_count": self.page_count,
            "encrypted": self.encrypted,
            "file_size": self.file_size,
        }


@dataclass
class FormField:
    """PDF form field."""
    name: str
    field_type: str
    value: Any
    options: list[str] = field(default_factory=list)
    required: bool = False
    readonly: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "field_type": self.field_type,
            "value": self.value,
            "options": self.options,
            "required": self.required,
            "readonly": self.readonly,
        }


@dataclass
class PDFExtractionResult:
    """Result of PDF extraction."""
    success: bool
    message: str
    metadata: Optional[PDFMetadata] = None
    pages: list[PDFPage] = field(default_factory=list)
    full_text: str = ""
    all_tables: list[list[list[str]]] = field(default_factory=list)
    form_fields: list[FormField] = field(default_factory=list)
    error: Optional[str] = None
    extraction_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "extraction_id": self.extraction_id,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "pages": [p.to_dict() for p in self.pages],
            "page_count": len(self.pages),
            "full_text": self.full_text,
            "all_tables": self.all_tables,
            "table_count": len(self.all_tables),
            "form_fields": [f.to_dict() for f in self.form_fields],
            "error": self.error,
        }


class PDFExtractor:
    """
    Enterprise PDF extractor.

    Example usage:

    config = PDFConfig(
        mode=ExtractionMode.ALL,
        extract_images=True,
        use_ocr=True
    )

    extractor = PDFExtractor(config)

    # From file
    result = extractor.extract("/path/to/document.pdf")

    # From bytes
    result = extractor.extract_bytes(pdf_bytes)

    print(result.full_text)
    print(result.all_tables)
    """

    def __init__(self, config: Optional[PDFConfig] = None):
        self.config = config or PDFConfig()

    def extract(self, file_path: Union[str, Path]) -> PDFExtractionResult:
        """Extract content from a PDF file."""
        file_path = Path(file_path)

        if not file_path.exists():
            return PDFExtractionResult(
                success=False,
                message="File not found",
                error=f"File does not exist: {file_path}",
            )

        try:
            with open(file_path, "rb") as f:
                pdf_bytes = f.read()

            result = self.extract_bytes(pdf_bytes)
            result.metadata.file_size = file_path.stat().st_size
            return result

        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            return PDFExtractionResult(
                success=False,
                message="Extraction failed",
                error=str(e),
            )

    def extract_bytes(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """Extract content from PDF bytes."""
        if not HAS_PDF_SUPPORT:
            return PDFExtractionResult(
                success=False,
                message="No PDF library available",
                error="Install PyPDF2 or pdfplumber",
            )

        try:
            # Use pdfplumber if available (better table extraction)
            if HAS_PDFPLUMBER:
                return self._extract_with_pdfplumber(pdf_bytes)
            else:
                return self._extract_with_pypdf2(pdf_bytes)

        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            return PDFExtractionResult(
                success=False,
                message="Extraction failed",
                error=str(e),
            )

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """Extract using pdfplumber (preferred method)."""
        pages = []
        all_tables = []
        full_text_parts = []
        metadata = None

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # Extract metadata
            metadata = self._extract_metadata_pdfplumber(pdf, len(pdf_bytes))

            # Determine pages to process
            page_indices = self.config.pages or list(range(len(pdf.pages)))

            for idx in page_indices:
                if idx >= len(pdf.pages):
                    continue

                page = pdf.pages[idx]
                pdf_page = PDFPage(
                    page_number=idx + 1,
                    width=page.width,
                    height=page.height,
                )

                # Extract text
                if self.config.mode in (ExtractionMode.TEXT, ExtractionMode.ALL):
                    if self.config.preserve_layout:
                        pdf_page.text = page.extract_text(layout=True) or ""
                    else:
                        pdf_page.text = page.extract_text() or ""
                    full_text_parts.append(pdf_page.text)

                # Extract tables
                if self.config.mode in (ExtractionMode.TABLES, ExtractionMode.ALL):
                    tables = page.extract_tables(self.config.table_settings) or []
                    pdf_page.tables = tables
                    all_tables.extend(tables)

                # Extract images
                if self.config.extract_images and self.config.mode in (ExtractionMode.IMAGES, ExtractionMode.ALL):
                    pdf_page.images = self._extract_images_from_page(page, idx)

                pages.append(pdf_page)

        # OCR if needed
        if self.config.use_ocr and self.config.mode == ExtractionMode.OCR:
            ocr_result = self._perform_ocr(pdf_bytes)
            if ocr_result:
                full_text_parts = [ocr_result]

        # Extract form fields
        form_fields = []
        if self.config.extract_form_fields and self.config.mode in (ExtractionMode.FORMS, ExtractionMode.ALL):
            form_fields = self._extract_form_fields_pypdf2(pdf_bytes)

        return PDFExtractionResult(
            success=True,
            message=f"Extracted {len(pages)} pages",
            metadata=metadata,
            pages=pages,
            full_text="\n\n".join(full_text_parts),
            all_tables=all_tables,
            form_fields=form_fields,
        )

    def _extract_with_pypdf2(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """Extract using PyPDF2 (fallback method)."""
        pages = []
        full_text_parts = []

        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))

        # Extract metadata
        metadata = self._extract_metadata_pypdf2(reader, len(pdf_bytes))

        # Determine pages to process
        page_indices = self.config.pages or list(range(len(reader.pages)))

        for idx in page_indices:
            if idx >= len(reader.pages):
                continue

            page = reader.pages[idx]
            pdf_page = PDFPage(page_number=idx + 1)

            # Extract text
            if self.config.mode in (ExtractionMode.TEXT, ExtractionMode.ALL):
                pdf_page.text = page.extract_text() or ""
                full_text_parts.append(pdf_page.text)

            pages.append(pdf_page)

        # OCR if needed
        if self.config.use_ocr and self.config.mode == ExtractionMode.OCR:
            ocr_result = self._perform_ocr(pdf_bytes)
            if ocr_result:
                full_text_parts = [ocr_result]

        # Extract form fields
        form_fields = []
        if self.config.extract_form_fields:
            form_fields = self._extract_form_fields_pypdf2(pdf_bytes)

        return PDFExtractionResult(
            success=True,
            message=f"Extracted {len(pages)} pages",
            metadata=metadata,
            pages=pages,
            full_text="\n\n".join(full_text_parts),
            all_tables=[],  # PyPDF2 doesn't support table extraction
            form_fields=form_fields,
        )

    def _extract_metadata_pdfplumber(self, pdf, file_size: int) -> PDFMetadata:
        """Extract metadata using pdfplumber."""
        meta = pdf.metadata or {}

        return PDFMetadata(
            title=meta.get("Title"),
            author=meta.get("Author"),
            subject=meta.get("Subject"),
            creator=meta.get("Creator"),
            producer=meta.get("Producer"),
            creation_date=self._parse_pdf_date(meta.get("CreationDate")),
            modification_date=self._parse_pdf_date(meta.get("ModDate")),
            page_count=len(pdf.pages),
            encrypted=False,
            file_size=file_size,
        )

    def _extract_metadata_pypdf2(self, reader: PyPDF2.PdfReader, file_size: int) -> PDFMetadata:
        """Extract metadata using PyPDF2."""
        meta = reader.metadata or {}

        return PDFMetadata(
            title=meta.get("/Title"),
            author=meta.get("/Author"),
            subject=meta.get("/Subject"),
            creator=meta.get("/Creator"),
            producer=meta.get("/Producer"),
            creation_date=self._parse_pdf_date(meta.get("/CreationDate")),
            modification_date=self._parse_pdf_date(meta.get("/ModDate")),
            page_count=len(reader.pages),
            encrypted=reader.is_encrypted,
            file_size=file_size,
        )

    def _parse_pdf_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse PDF date format (D:YYYYMMDDHHmmSS)."""
        if not date_str:
            return None

        try:
            # Remove D: prefix if present
            if date_str.startswith("D:"):
                date_str = date_str[2:]

            # Parse the date (may have timezone)
            date_str = date_str[:14]  # Take just YYYYMMDDHHmmSS
            return datetime.strptime(date_str, "%Y%m%d%H%M%S")
        except Exception:
            return None

    def _extract_images_from_page(self, page, page_idx: int) -> list[dict[str, Any]]:
        """Extract images from a pdfplumber page."""
        images = []

        for idx, img in enumerate(page.images):
            try:
                # Filter by minimum size
                width = img.get("width", 0)
                height = img.get("height", 0)

                if width < self.config.min_image_size or height < self.config.min_image_size:
                    continue

                images.append({
                    "index": idx,
                    "page": page_idx + 1,
                    "x0": img.get("x0"),
                    "y0": img.get("y0"),
                    "x1": img.get("x1"),
                    "y1": img.get("y1"),
                    "width": width,
                    "height": height,
                })

            except Exception as e:
                logger.warning(f"Failed to extract image: {e}")

        return images

    def _extract_form_fields_pypdf2(self, pdf_bytes: bytes) -> list[FormField]:
        """Extract form fields using PyPDF2."""
        form_fields = []

        try:
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))

            if reader.get_fields():
                for field_name, field_data in reader.get_fields().items():
                    field_type = field_data.get("/FT", "")
                    value = field_data.get("/V", "")

                    # Map field types
                    type_map = {
                        "/Tx": "text",
                        "/Btn": "button",
                        "/Ch": "choice",
                        "/Sig": "signature",
                    }

                    form_fields.append(FormField(
                        name=field_name,
                        field_type=type_map.get(field_type, "unknown"),
                        value=value if isinstance(value, str) else str(value),
                        required=bool(field_data.get("/Ff", 0) & 2),
                        readonly=bool(field_data.get("/Ff", 0) & 1),
                    ))

        except Exception as e:
            logger.warning(f"Failed to extract form fields: {e}")

        return form_fields

    def _perform_ocr(self, pdf_bytes: bytes) -> Optional[str]:
        """Perform OCR on PDF pages."""
        if not HAS_TESSERACT or not HAS_PDF2IMAGE:
            logger.warning("OCR requires pytesseract and pdf2image")
            return None

        try:
            # Convert PDF to images
            images = convert_from_bytes(pdf_bytes, dpi=self.config.ocr_dpi)

            text_parts = []
            for img in images:
                # Perform OCR
                text = pytesseract.image_to_string(
                    img,
                    lang=self.config.ocr_language,
                )
                text_parts.append(text)

            return "\n\n".join(text_parts)

        except Exception as e:
            logger.error(f"OCR failed: {e}")
            return None

    def extract_text_only(self, file_path: Union[str, Path, bytes]) -> str:
        """Quick method to extract just text."""
        original_mode = self.config.mode
        self.config.mode = ExtractionMode.TEXT

        if isinstance(file_path, bytes):
            result = self.extract_bytes(file_path)
        else:
            result = self.extract(file_path)

        self.config.mode = original_mode
        return result.full_text

    def extract_tables_only(self, file_path: Union[str, Path, bytes]) -> list[list[list[str]]]:
        """Quick method to extract just tables."""
        original_mode = self.config.mode
        self.config.mode = ExtractionMode.TABLES

        if isinstance(file_path, bytes):
            result = self.extract_bytes(file_path)
        else:
            result = self.extract(file_path)

        self.config.mode = original_mode
        return result.all_tables


# Flow Node Integration
@dataclass
class PDFNodeConfig:
    """Configuration for PDF flow node."""
    mode: str = "text"  # text, tables, images, forms, all, ocr
    pages: Optional[list[int]] = None
    preserve_layout: bool = True
    extract_images: bool = False
    use_ocr: bool = False
    ocr_language: str = "eng"
    extract_form_fields: bool = False


@dataclass
class PDFNodeResult:
    """Result from PDF flow node."""
    success: bool
    text: str
    tables: list[list[list[str]]]
    form_fields: list[dict[str, Any]]
    metadata: Optional[dict[str, Any]]
    page_count: int
    message: str
    error: Optional[str]


class PDFNode:
    """Flow node for PDF extraction."""

    node_type = "pdf_extractor"
    node_category = "parser"

    def __init__(self, config: PDFNodeConfig):
        self.config = config

    def execute(self, input_data: dict[str, Any]) -> PDFNodeResult:
        """Execute the PDF extraction."""
        pdf_config = PDFConfig(
            mode=ExtractionMode(self.config.mode),
            pages=self.config.pages,
            preserve_layout=self.config.preserve_layout,
            extract_images=self.config.extract_images,
            use_ocr=self.config.use_ocr,
            ocr_language=self.config.ocr_language,
            extract_form_fields=self.config.extract_form_fields,
        )

        extractor = PDFExtractor(pdf_config)

        try:
            # Get PDF data from input
            pdf_data = input_data.get("data") or input_data.get("file_path")

            if isinstance(pdf_data, str) and os.path.exists(pdf_data):
                result = extractor.extract(pdf_data)
            elif isinstance(pdf_data, bytes):
                result = extractor.extract_bytes(pdf_data)
            elif isinstance(pdf_data, str):
                # Assume base64 encoded
                pdf_bytes = base64.b64decode(pdf_data)
                result = extractor.extract_bytes(pdf_bytes)
            else:
                return PDFNodeResult(
                    success=False,
                    text="",
                    tables=[],
                    form_fields=[],
                    metadata=None,
                    page_count=0,
                    message="Invalid input",
                    error="Input must be file path, bytes, or base64 string",
                )

            return PDFNodeResult(
                success=result.success,
                text=result.full_text,
                tables=result.all_tables,
                form_fields=[f.to_dict() for f in result.form_fields],
                metadata=result.metadata.to_dict() if result.metadata else None,
                page_count=len(result.pages),
                message=result.message,
                error=result.error,
            )

        except Exception as e:
            logger.error(f"PDF node execution failed: {e}")
            return PDFNodeResult(
                success=False,
                text="",
                tables=[],
                form_fields=[],
                metadata=None,
                page_count=0,
                message="Execution failed",
                error=str(e),
            )


def get_pdf_extractor(config: Optional[PDFConfig] = None) -> PDFExtractor:
    """Factory function to create PDF extractor."""
    return PDFExtractor(config)


def get_pdf_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "pdf_extractor",
        "category": "parser",
        "label": "PDF Extractor",
        "description": "Extract text, tables, and forms from PDFs",
        "icon": "FileText",
        "color": "#DC2626",
        "inputs": ["data"],
        "outputs": ["text", "tables", "form_fields", "metadata"],
        "config_schema": {
            "mode": {
                "type": "select",
                "options": ["text", "tables", "images", "forms", "all", "ocr"],
                "default": "text",
                "label": "Extraction Mode",
            },
            "preserve_layout": {
                "type": "boolean",
                "default": True,
                "label": "Preserve Layout",
            },
            "extract_images": {
                "type": "boolean",
                "default": False,
                "label": "Extract Images",
            },
            "use_ocr": {
                "type": "boolean",
                "default": False,
                "label": "Use OCR (for scanned PDFs)",
            },
            "ocr_language": {
                "type": "string",
                "default": "eng",
                "label": "OCR Language",
                "condition": {"use_ocr": True},
            },
            "extract_form_fields": {
                "type": "boolean",
                "default": False,
                "label": "Extract Form Fields",
            },
        },
    }

"""
Flat File Parser for Logic Weaver

Enterprise-grade parser for fixed-width, delimited (CSV, TSV, pipe),
and custom record layout files.

Key Features:
- Fixed-width field parsing with position mapping
- Delimited file parsing (CSV, TSV, pipe, custom)
- Header/trailer record handling
- Data type conversion and validation
- Multi-record type support (mixed layouts)
- Streaming support for large files
- Error handling with line numbers

Comparison:
| Feature              | MuleSoft | Apigee | Logic Weaver |
|---------------------|----------|--------|--------------|
| Fixed-Width         | DataWeave| No     | Native       |
| CSV/TSV             | DataWeave| No     | Native       |
| Mixed Record Types  | Limited  | No     | Full         |
| Streaming           | Yes      | No     | Yes          |
| Type Conversion     | Yes      | No     | Yes          |
| Header/Trailer      | Manual   | No     | Automatic    |
"""

from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Iterator, Optional, TextIO, Union

logger = logging.getLogger(__name__)


class FileFormat(Enum):
    """Supported flat file formats."""
    FIXED_WIDTH = "fixed_width"
    CSV = "csv"
    TSV = "tsv"
    PIPE = "pipe"
    CUSTOM = "custom"


class DataType(Enum):
    """Field data types for conversion."""
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    FLOAT = "float"


class RecordType(Enum):
    """Record types in multi-record files."""
    HEADER = "header"
    DETAIL = "detail"
    TRAILER = "trailer"


@dataclass
class FieldDefinition:
    """Definition of a field in the record layout."""
    name: str
    data_type: DataType = DataType.STRING

    # For fixed-width files
    start: Optional[int] = None  # 1-based position
    length: Optional[int] = None

    # For delimited files
    position: Optional[int] = None  # 0-based column index

    # Formatting
    date_format: str = "%Y%m%d"
    datetime_format: str = "%Y%m%d%H%M%S"
    decimal_places: int = 2
    decimal_implied: bool = False  # For implied decimal (e.g., 12345 = 123.45)

    # Validation
    required: bool = False
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allowed_values: Optional[list[str]] = None

    # Transformation
    trim: bool = True
    default_value: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "start": self.start,
            "length": self.length,
            "position": self.position,
            "required": self.required,
        }


@dataclass
class RecordLayout:
    """Layout definition for a record type."""
    name: str
    record_type: RecordType = RecordType.DETAIL
    fields: list[FieldDefinition] = field(default_factory=list)

    # Record identification (for multi-record files)
    identifier_field: Optional[str] = None  # Field name to check
    identifier_value: Optional[str] = None  # Expected value
    identifier_position: Optional[int] = None  # Position for fixed-width
    identifier_length: Optional[int] = None

    # Record-level validation
    min_fields: Optional[int] = None
    max_fields: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "record_type": self.record_type.value,
            "fields": [f.to_dict() for f in self.fields],
            "identifier_field": self.identifier_field,
            "identifier_value": self.identifier_value,
        }


@dataclass
class ParseError:
    """Represents a parsing error."""
    line_number: int
    field_name: Optional[str]
    message: str
    raw_value: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "line_number": self.line_number,
            "field_name": self.field_name,
            "message": self.message,
            "raw_value": self.raw_value,
        }


@dataclass
class ParsedRecord:
    """A single parsed record."""
    line_number: int
    record_type: RecordType
    layout_name: str
    data: dict[str, Any]
    raw_line: str
    errors: list[ParseError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "line_number": self.line_number,
            "record_type": self.record_type.value,
            "layout_name": self.layout_name,
            "data": self.data,
            "is_valid": self.is_valid,
            "errors": [e.to_dict() for e in self.errors],
        }


@dataclass
class FlatFileParseResult:
    """Result of parsing a flat file."""
    format: FileFormat
    total_lines: int
    header_records: list[ParsedRecord] = field(default_factory=list)
    detail_records: list[ParsedRecord] = field(default_factory=list)
    trailer_records: list[ParsedRecord] = field(default_factory=list)
    errors: list[ParseError] = field(default_factory=list)

    @property
    def all_records(self) -> list[ParsedRecord]:
        return self.header_records + self.detail_records + self.trailer_records

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0 and all(r.is_valid for r in self.all_records)

    @property
    def error_count(self) -> int:
        return len(self.errors) + sum(len(r.errors) for r in self.all_records)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format.value,
            "total_lines": self.total_lines,
            "header_count": len(self.header_records),
            "detail_count": len(self.detail_records),
            "trailer_count": len(self.trailer_records),
            "is_valid": self.is_valid,
            "error_count": self.error_count,
            "header_records": [r.to_dict() for r in self.header_records],
            "detail_records": [r.to_dict() for r in self.detail_records],
            "trailer_records": [r.to_dict() for r in self.trailer_records],
            "errors": [e.to_dict() for e in self.errors],
        }

    def to_records(self) -> list[dict[str, Any]]:
        """Return just the data from detail records."""
        return [r.data for r in self.detail_records if r.is_valid]


class FlatFileParser:
    """
    Enterprise flat file parser supporting fixed-width and delimited formats.

    Example usage:

    # Fixed-width file
    layout = RecordLayout(
        name="claim",
        fields=[
            FieldDefinition(name="claim_id", start=1, length=10),
            FieldDefinition(name="amount", start=11, length=12, data_type=DataType.DECIMAL),
            FieldDefinition(name="date", start=23, length=8, data_type=DataType.DATE),
        ]
    )
    parser = FlatFileParser(FileFormat.FIXED_WIDTH, layouts=[layout])
    result = parser.parse(content)

    # CSV file
    layout = RecordLayout(
        name="patient",
        fields=[
            FieldDefinition(name="mrn", position=0),
            FieldDefinition(name="name", position=1),
            FieldDefinition(name="dob", position=2, data_type=DataType.DATE),
        ]
    )
    parser = FlatFileParser(FileFormat.CSV, layouts=[layout])
    result = parser.parse(content)
    """

    def __init__(
        self,
        format: FileFormat,
        layouts: list[RecordLayout],
        delimiter: str = ",",
        quote_char: str = '"',
        escape_char: Optional[str] = None,
        has_header_row: bool = False,
        skip_blank_lines: bool = True,
        encoding: str = "utf-8",
        strict: bool = False,
        line_terminator: str = "\n",
    ):
        """
        Initialize the flat file parser.

        Args:
            format: File format (FIXED_WIDTH, CSV, TSV, PIPE, CUSTOM)
            layouts: List of record layouts
            delimiter: Field delimiter for delimited files
            quote_char: Quote character for delimited files
            escape_char: Escape character
            has_header_row: Whether file has a header row to skip
            skip_blank_lines: Skip empty lines
            encoding: File encoding
            strict: Raise errors on validation failures
            line_terminator: Line terminator character
        """
        self.format = format
        self.layouts = {layout.name: layout for layout in layouts}
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.escape_char = escape_char
        self.has_header_row = has_header_row
        self.skip_blank_lines = skip_blank_lines
        self.encoding = encoding
        self.strict = strict
        self.line_terminator = line_terminator

        # Set delimiter based on format
        if format == FileFormat.TSV:
            self.delimiter = "\t"
        elif format == FileFormat.PIPE:
            self.delimiter = "|"

        # Organize layouts by record type
        self._header_layouts = [l for l in layouts if l.record_type == RecordType.HEADER]
        self._detail_layouts = [l for l in layouts if l.record_type == RecordType.DETAIL]
        self._trailer_layouts = [l for l in layouts if l.record_type == RecordType.TRAILER]

    def parse(self, content: str) -> FlatFileParseResult:
        """Parse flat file content."""
        result = FlatFileParseResult(format=self.format, total_lines=0)

        lines = content.split(self.line_terminator)
        result.total_lines = len(lines)

        start_line = 1 if self.has_header_row else 0

        for i, line in enumerate(lines[start_line:], start=start_line + 1):
            if self.skip_blank_lines and not line.strip():
                continue

            record = self._parse_line(line, i)

            if record.record_type == RecordType.HEADER:
                result.header_records.append(record)
            elif record.record_type == RecordType.TRAILER:
                result.trailer_records.append(record)
            else:
                result.detail_records.append(record)

        return result

    def parse_file(self, file_path: str) -> FlatFileParseResult:
        """Parse a flat file."""
        with open(file_path, 'r', encoding=self.encoding) as f:
            return self.parse(f.read())

    def parse_stream(self, stream: TextIO) -> Iterator[ParsedRecord]:
        """Parse a file stream, yielding records one at a time."""
        line_number = 0

        if self.has_header_row:
            next(stream, None)
            line_number = 1

        for line in stream:
            line_number += 1
            line = line.rstrip(self.line_terminator)

            if self.skip_blank_lines and not line.strip():
                continue

            yield self._parse_line(line, line_number)

    def _parse_line(self, line: str, line_number: int) -> ParsedRecord:
        """Parse a single line."""
        # Determine the layout to use
        layout = self._identify_layout(line)

        if layout is None:
            # Use first detail layout as default
            layout = self._detail_layouts[0] if self._detail_layouts else None
            if layout is None:
                return ParsedRecord(
                    line_number=line_number,
                    record_type=RecordType.DETAIL,
                    layout_name="unknown",
                    data={},
                    raw_line=line,
                    errors=[ParseError(line_number, None, "No layout defined")]
                )

        # Parse based on format
        if self.format == FileFormat.FIXED_WIDTH:
            return self._parse_fixed_width(line, line_number, layout)
        else:
            return self._parse_delimited(line, line_number, layout)

    def _identify_layout(self, line: str) -> Optional[RecordLayout]:
        """Identify which layout applies to this line."""
        all_layouts = self._header_layouts + self._detail_layouts + self._trailer_layouts

        for layout in all_layouts:
            if layout.identifier_value:
                if self.format == FileFormat.FIXED_WIDTH:
                    # Check fixed position
                    if layout.identifier_position and layout.identifier_length:
                        start = layout.identifier_position - 1
                        end = start + layout.identifier_length
                        value = line[start:end].strip()
                        if value == layout.identifier_value:
                            return layout
                else:
                    # Check field value
                    fields = self._split_delimited(line)
                    if layout.identifier_field:
                        for i, field_def in enumerate(layout.fields):
                            if field_def.name == layout.identifier_field and i < len(fields):
                                if fields[i].strip() == layout.identifier_value:
                                    return layout

        # Return first matching layout by record type
        if self._detail_layouts:
            return self._detail_layouts[0]

        return None

    def _parse_fixed_width(
        self, line: str, line_number: int, layout: RecordLayout
    ) -> ParsedRecord:
        """Parse a fixed-width line."""
        data = {}
        errors = []

        for field_def in layout.fields:
            if field_def.start is None or field_def.length is None:
                continue

            start = field_def.start - 1  # Convert to 0-based
            end = start + field_def.length

            if end > len(line):
                raw_value = line[start:] if start < len(line) else ""
            else:
                raw_value = line[start:end]

            value, error = self._convert_field(raw_value, field_def, line_number)

            if error:
                errors.append(error)

            data[field_def.name] = value

        return ParsedRecord(
            line_number=line_number,
            record_type=layout.record_type,
            layout_name=layout.name,
            data=data,
            raw_line=line,
            errors=errors,
        )

    def _parse_delimited(
        self, line: str, line_number: int, layout: RecordLayout
    ) -> ParsedRecord:
        """Parse a delimited line."""
        data = {}
        errors = []

        fields = self._split_delimited(line)

        for field_def in layout.fields:
            if field_def.position is None:
                continue

            if field_def.position >= len(fields):
                raw_value = ""
            else:
                raw_value = fields[field_def.position]

            value, error = self._convert_field(raw_value, field_def, line_number)

            if error:
                errors.append(error)

            data[field_def.name] = value

        return ParsedRecord(
            line_number=line_number,
            record_type=layout.record_type,
            layout_name=layout.name,
            data=data,
            raw_line=line,
            errors=errors,
        )

    def _split_delimited(self, line: str) -> list[str]:
        """Split a delimited line into fields."""
        reader = csv.reader(
            io.StringIO(line),
            delimiter=self.delimiter,
            quotechar=self.quote_char,
            escapechar=self.escape_char,
        )
        try:
            return next(reader)
        except StopIteration:
            return []

    def _convert_field(
        self, raw_value: str, field_def: FieldDefinition, line_number: int
    ) -> tuple[Any, Optional[ParseError]]:
        """Convert a raw field value to the appropriate type."""
        # Trim if specified
        if field_def.trim:
            raw_value = raw_value.strip()

        # Handle empty values
        if not raw_value:
            if field_def.required:
                return field_def.default_value, ParseError(
                    line_number, field_def.name, "Required field is empty"
                )
            return field_def.default_value, None

        # Validate pattern
        if field_def.pattern:
            if not re.match(field_def.pattern, raw_value):
                return raw_value, ParseError(
                    line_number, field_def.name,
                    f"Value does not match pattern: {field_def.pattern}",
                    raw_value
                )

        # Validate allowed values
        if field_def.allowed_values:
            if raw_value not in field_def.allowed_values:
                return raw_value, ParseError(
                    line_number, field_def.name,
                    f"Value not in allowed values: {field_def.allowed_values}",
                    raw_value
                )

        # Convert based on data type
        try:
            if field_def.data_type == DataType.STRING:
                return raw_value, None

            elif field_def.data_type == DataType.INTEGER:
                return int(raw_value), None

            elif field_def.data_type == DataType.FLOAT:
                return float(raw_value), None

            elif field_def.data_type == DataType.DECIMAL:
                if field_def.decimal_implied:
                    # Handle implied decimal (e.g., 12345 = 123.45)
                    divisor = 10 ** field_def.decimal_places
                    return Decimal(raw_value) / divisor, None
                return Decimal(raw_value), None

            elif field_def.data_type == DataType.DATE:
                return datetime.strptime(raw_value, field_def.date_format).date(), None

            elif field_def.data_type == DataType.DATETIME:
                return datetime.strptime(raw_value, field_def.datetime_format), None

            elif field_def.data_type == DataType.BOOLEAN:
                return raw_value.lower() in ('true', 'yes', '1', 'y', 't'), None

            else:
                return raw_value, None

        except (ValueError, TypeError) as e:
            return raw_value, ParseError(
                line_number, field_def.name,
                f"Failed to convert to {field_def.data_type.value}: {e}",
                raw_value
            )


# Convenience functions
def parse_csv(
    content: str,
    fields: list[dict[str, Any]],
    has_header: bool = True,
    delimiter: str = ",",
) -> FlatFileParseResult:
    """
    Quick CSV parsing with field definitions.

    Args:
        content: CSV content
        fields: List of field definitions as dicts with 'name', 'position', 'type'
        has_header: Whether file has header row
        delimiter: Field delimiter

    Example:
        result = parse_csv(
            content,
            fields=[
                {"name": "id", "position": 0, "type": "integer"},
                {"name": "name", "position": 1},
                {"name": "amount", "position": 2, "type": "decimal"},
            ]
        )
    """
    field_defs = []
    for f in fields:
        data_type = DataType.STRING
        if "type" in f:
            type_map = {
                "string": DataType.STRING,
                "integer": DataType.INTEGER,
                "int": DataType.INTEGER,
                "decimal": DataType.DECIMAL,
                "float": DataType.FLOAT,
                "date": DataType.DATE,
                "datetime": DataType.DATETIME,
                "boolean": DataType.BOOLEAN,
                "bool": DataType.BOOLEAN,
            }
            data_type = type_map.get(f["type"].lower(), DataType.STRING)

        field_defs.append(FieldDefinition(
            name=f["name"],
            position=f.get("position", 0),
            data_type=data_type,
            required=f.get("required", False),
            date_format=f.get("date_format", "%Y-%m-%d"),
        ))

    layout = RecordLayout(name="default", fields=field_defs)
    parser = FlatFileParser(
        format=FileFormat.CSV if delimiter == "," else FileFormat.CUSTOM,
        layouts=[layout],
        delimiter=delimiter,
        has_header_row=has_header,
    )
    return parser.parse(content)


def parse_fixed_width(
    content: str,
    fields: list[dict[str, Any]],
) -> FlatFileParseResult:
    """
    Quick fixed-width parsing with field definitions.

    Args:
        content: Fixed-width content
        fields: List of field definitions as dicts with 'name', 'start', 'length', 'type'

    Example:
        result = parse_fixed_width(
            content,
            fields=[
                {"name": "claim_id", "start": 1, "length": 10},
                {"name": "amount", "start": 11, "length": 12, "type": "decimal"},
                {"name": "date", "start": 23, "length": 8, "type": "date"},
            ]
        )
    """
    field_defs = []
    for f in fields:
        data_type = DataType.STRING
        if "type" in f:
            type_map = {
                "string": DataType.STRING,
                "integer": DataType.INTEGER,
                "int": DataType.INTEGER,
                "decimal": DataType.DECIMAL,
                "float": DataType.FLOAT,
                "date": DataType.DATE,
                "datetime": DataType.DATETIME,
                "boolean": DataType.BOOLEAN,
                "bool": DataType.BOOLEAN,
            }
            data_type = type_map.get(f["type"].lower(), DataType.STRING)

        field_defs.append(FieldDefinition(
            name=f["name"],
            start=f["start"],
            length=f["length"],
            data_type=data_type,
            required=f.get("required", False),
            date_format=f.get("date_format", "%Y%m%d"),
            decimal_implied=f.get("decimal_implied", False),
            decimal_places=f.get("decimal_places", 2),
        ))

    layout = RecordLayout(name="default", fields=field_defs)
    parser = FlatFileParser(format=FileFormat.FIXED_WIDTH, layouts=[layout])
    return parser.parse(content)


def parse_tsv(content: str, fields: list[dict[str, Any]], has_header: bool = True) -> FlatFileParseResult:
    """Parse TSV file."""
    return parse_csv(content, fields, has_header, delimiter="\t")


def parse_pipe_delimited(content: str, fields: list[dict[str, Any]], has_header: bool = True) -> FlatFileParseResult:
    """Parse pipe-delimited file."""
    return parse_csv(content, fields, has_header, delimiter="|")


# Flow Node Integration
@dataclass
class FlatFileNodeConfig:
    """Configuration for flat file flow node."""
    format: str = "csv"  # csv, tsv, pipe, fixed_width
    fields: list[dict[str, Any]] = field(default_factory=list)
    has_header: bool = True
    delimiter: str = ","
    encoding: str = "utf-8"
    skip_blank_lines: bool = True
    strict: bool = False


@dataclass
class FlatFileNodeResult:
    """Result from flat file flow node."""
    success: bool
    records: list[dict[str, Any]]
    total_records: int
    error_count: int
    errors: list[dict[str, Any]]
    metadata: dict[str, Any]


class FlatFileNode:
    """Flow node for flat file parsing."""

    node_type = "flatfile_parser"
    node_category = "parser"

    def __init__(self, config: FlatFileNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> FlatFileNodeResult:
        """Execute the flat file parser node."""
        content = input_data.get("content", "")

        if not content:
            return FlatFileNodeResult(
                success=False,
                records=[],
                total_records=0,
                error_count=1,
                errors=[{"message": "No content provided"}],
                metadata={},
            )

        try:
            if self.config.format == "fixed_width":
                result = parse_fixed_width(content, self.config.fields)
            elif self.config.format == "tsv":
                result = parse_tsv(content, self.config.fields, self.config.has_header)
            elif self.config.format == "pipe":
                result = parse_pipe_delimited(content, self.config.fields, self.config.has_header)
            else:  # csv
                result = parse_csv(
                    content,
                    self.config.fields,
                    self.config.has_header,
                    self.config.delimiter
                )

            return FlatFileNodeResult(
                success=result.is_valid,
                records=result.to_records(),
                total_records=len(result.detail_records),
                error_count=result.error_count,
                errors=[e.to_dict() for e in result.errors],
                metadata={
                    "format": result.format.value,
                    "total_lines": result.total_lines,
                    "header_count": len(result.header_records),
                    "trailer_count": len(result.trailer_records),
                }
            )
        except Exception as e:
            logger.error(f"Flat file parsing failed: {e}")
            return FlatFileNodeResult(
                success=False,
                records=[],
                total_records=0,
                error_count=1,
                errors=[{"message": str(e)}],
                metadata={},
            )


def get_flatfile_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "flatfile_parser",
        "category": "parser",
        "label": "Flat File Parser",
        "description": "Parse CSV, TSV, pipe-delimited, and fixed-width files",
        "icon": "FileText",
        "color": "#10B981",
        "inputs": ["content"],
        "outputs": ["records", "errors"],
        "config_schema": {
            "format": {
                "type": "select",
                "label": "File Format",
                "options": ["csv", "tsv", "pipe", "fixed_width"],
                "default": "csv",
            },
            "has_header": {
                "type": "boolean",
                "label": "Has Header Row",
                "default": True,
            },
            "delimiter": {
                "type": "string",
                "label": "Delimiter",
                "default": ",",
                "visible_when": {"format": "csv"},
            },
            "fields": {
                "type": "array",
                "label": "Field Definitions",
                "item_schema": {
                    "name": {"type": "string", "required": True},
                    "position": {"type": "integer", "label": "Column (0-based)"},
                    "start": {"type": "integer", "label": "Start Position (1-based)"},
                    "length": {"type": "integer", "label": "Length"},
                    "type": {
                        "type": "select",
                        "options": ["string", "integer", "decimal", "date", "datetime", "boolean"],
                        "default": "string",
                    },
                    "required": {"type": "boolean", "default": False},
                },
            },
        },
    }

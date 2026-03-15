"""
Visual Data Mapper for Logic Weaver

Drag-and-drop field mapping system similar to MuleSoft's DataWeave
but with Python-native execution and JSONPath support.

Features:
- Field-to-field mapping with visual editor support
- JSONPath source and target paths
- Built-in transform functions
- Custom Python transforms
- Type coercion
- Default values
- Conditional mapping
- Array/collection handling

Example:
    mapper = DataMapper(
        mappings=[
            FieldMapping("$.patient.name", "$.output.patientName"),
            FieldMapping("$.patient.dob", "$.output.birthDate", transform="date_format"),
            FieldMapping("$.patient.mrn", "$.output.mrn", transform=lambda x: f"MRN-{x}"),
        ]
    )
    result = mapper.map(input_data)
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import json

logger = logging.getLogger(__name__)


class TransformFunction(Enum):
    """Built-in transform functions"""
    # String transforms
    UPPER = "upper"
    LOWER = "lower"
    TRIM = "trim"
    CONCAT = "concat"
    SUBSTRING = "substring"
    REPLACE = "replace"
    SPLIT = "split"
    JOIN = "join"
    PAD_LEFT = "pad_left"
    PAD_RIGHT = "pad_right"

    # Numeric transforms
    ROUND = "round"
    FLOOR = "floor"
    CEILING = "ceiling"
    ABS = "abs"
    FORMAT_NUMBER = "format_number"
    TO_INT = "to_int"
    TO_FLOAT = "to_float"
    TO_DECIMAL = "to_decimal"

    # Date transforms
    DATE_FORMAT = "date_format"
    DATE_PARSE = "date_parse"
    DATE_NOW = "date_now"
    DATE_ADD = "date_add"

    # Type transforms
    TO_STRING = "to_string"
    TO_JSON = "to_json"
    FROM_JSON = "from_json"
    TO_BOOLEAN = "to_boolean"

    # Array transforms
    FIRST = "first"
    LAST = "last"
    SIZE = "size"
    FLATTEN = "flatten"
    DISTINCT = "distinct"
    SORT = "sort"
    FILTER = "filter"
    MAP = "map"

    # Conditional
    DEFAULT = "default"
    IF_ELSE = "if_else"
    COALESCE = "coalesce"
    NULL_TO_EMPTY = "null_to_empty"

    # Healthcare specific
    HL7_DATE = "hl7_date"
    FHIR_DATE = "fhir_date"
    NPI_FORMAT = "npi_format"
    SSN_MASK = "ssn_mask"
    PHI_MASK = "phi_mask"


@dataclass
class TransformConfig:
    """Configuration for a transform function"""
    function: Union[TransformFunction, str, Callable]
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingCondition:
    """Condition for conditional mapping"""
    expression: str  # e.g., "$.type == 'patient'"
    if_true_value: Optional[Any] = None
    if_false_value: Optional[Any] = None
    if_true_source: Optional[str] = None
    if_false_source: Optional[str] = None


@dataclass
class FieldMapping:
    """
    Single field mapping definition.

    Attributes:
        source: JSONPath to source field
        target: JSONPath to target field
        transform: Transform function or callable
        transform_args: Arguments for transform
        default: Default value if source is None
        condition: Conditional mapping
        required: Fail if source not found
        description: Human-readable description
    """
    source: str
    target: str
    transform: Optional[Union[TransformFunction, str, Callable]] = None
    transform_args: List[Any] = field(default_factory=list)
    transform_kwargs: Dict[str, Any] = field(default_factory=dict)
    default: Any = None
    condition: Optional[MappingCondition] = None
    required: bool = False
    description: str = ""
    data_type: Optional[str] = None  # Expected data type

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "source": self.source,
            "target": self.target,
            "transform": self.transform.value if isinstance(self.transform, TransformFunction) else str(self.transform) if self.transform else None,
            "transform_args": self.transform_args,
            "default": self.default,
            "required": self.required,
            "description": self.description,
            "data_type": self.data_type,
        }


@dataclass
class MappingRule:
    """
    Complex mapping rule with multiple transforms.

    Supports chaining multiple transforms.
    """
    source: str
    target: str
    transforms: List[TransformConfig] = field(default_factory=list)
    default: Any = None


@dataclass
class VisualMapperConfig:
    """
    Configuration for visual data mapper UI.

    This configuration is used by the React frontend
    to render the drag-and-drop mapping interface.
    """
    id: str
    name: str
    description: str = ""
    source_schema: Dict[str, Any] = field(default_factory=dict)
    target_schema: Dict[str, Any] = field(default_factory=dict)
    mappings: List[FieldMapping] = field(default_factory=list)
    sample_input: Optional[Dict[str, Any]] = None
    sample_output: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "source_schema": self.source_schema,
            "target_schema": self.target_schema,
            "mappings": [m.to_dict() for m in self.mappings],
            "sample_input": self.sample_input,
            "sample_output": self.sample_output,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class DataMapper:
    """
    Data Mapper with JSONPath support and transforms.

    Maps data from source structure to target structure
    using configurable field mappings and transforms.

    Example:
        mapper = DataMapper([
            FieldMapping("$.patient.firstName", "$.name.given"),
            FieldMapping("$.patient.lastName", "$.name.family"),
            FieldMapping("$.patient.dob", "$.birthDate", transform="hl7_date"),
        ])

        output = mapper.map(input_data)
    """

    # Built-in transform implementations
    TRANSFORMS: Dict[str, Callable] = {}

    def __init__(
        self,
        mappings: Optional[List[FieldMapping]] = None,
        strict: bool = False,
        preserve_unmapped: bool = False,
    ):
        """
        Initialize mapper.

        Args:
            mappings: List of field mappings
            strict: Fail on any mapping error
            preserve_unmapped: Copy unmapped fields to output
        """
        self.mappings = mappings or []
        self.strict = strict
        self.preserve_unmapped = preserve_unmapped
        self._setup_transforms()

    def _setup_transforms(self):
        """Setup built-in transform functions"""
        self.TRANSFORMS = {
            # String transforms
            "upper": lambda x, *a, **k: str(x).upper() if x else "",
            "lower": lambda x, *a, **k: str(x).lower() if x else "",
            "trim": lambda x, *a, **k: str(x).strip() if x else "",
            "concat": self._concat,
            "substring": self._substring,
            "replace": lambda x, old, new, *a, **k: str(x).replace(old, new) if x else "",
            "split": lambda x, sep=",", *a, **k: str(x).split(sep) if x else [],
            "join": lambda x, sep=",", *a, **k: sep.join(x) if isinstance(x, list) else str(x),
            "pad_left": lambda x, width, char=" ", *a, **k: str(x).rjust(width, char) if x else "",
            "pad_right": lambda x, width, char=" ", *a, **k: str(x).ljust(width, char) if x else "",

            # Numeric transforms
            "round": lambda x, decimals=0, *a, **k: round(float(x), decimals) if x else 0,
            "floor": lambda x, *a, **k: int(float(x)) if x else 0,
            "ceiling": lambda x, *a, **k: int(float(x)) + (1 if float(x) % 1 else 0) if x else 0,
            "abs": lambda x, *a, **k: abs(float(x)) if x else 0,
            "format_number": self._format_number,
            "to_int": lambda x, *a, **k: int(float(x)) if x else 0,
            "to_float": lambda x, *a, **k: float(x) if x else 0.0,
            "to_decimal": lambda x, *a, **k: Decimal(str(x)) if x else Decimal("0"),

            # Date transforms
            "date_format": self._date_format,
            "date_parse": self._date_parse,
            "date_now": lambda *a, **k: datetime.now().isoformat(),
            "date_add": self._date_add,

            # Type transforms
            "to_string": lambda x, *a, **k: str(x) if x is not None else "",
            "to_json": lambda x, *a, **k: json.dumps(x) if x else "null",
            "from_json": lambda x, *a, **k: json.loads(x) if x else None,
            "to_boolean": self._to_boolean,

            # Array transforms
            "first": lambda x, *a, **k: x[0] if x and isinstance(x, list) else x,
            "last": lambda x, *a, **k: x[-1] if x and isinstance(x, list) else x,
            "size": lambda x, *a, **k: len(x) if x else 0,
            "flatten": self._flatten,
            "distinct": lambda x, *a, **k: list(set(x)) if isinstance(x, list) else [x],
            "sort": lambda x, *a, **k: sorted(x) if isinstance(x, list) else x,
            "filter": self._filter,
            "map": self._map_array,

            # Conditional
            "default": lambda x, default_val, *a, **k: x if x is not None else default_val,
            "if_else": self._if_else,
            "coalesce": self._coalesce,
            "null_to_empty": lambda x, *a, **k: "" if x is None else x,

            # Healthcare specific
            "hl7_date": self._hl7_date_format,
            "fhir_date": self._fhir_date_format,
            "npi_format": self._npi_format,
            "ssn_mask": self._ssn_mask,
            "phi_mask": self._phi_mask,
        }

    def add_mapping(self, mapping: FieldMapping):
        """Add a field mapping"""
        self.mappings.append(mapping)

    def add_mappings(self, mappings: List[FieldMapping]):
        """Add multiple field mappings"""
        self.mappings.extend(mappings)

    def map(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map source data to target structure.

        Args:
            source: Input data

        Returns:
            Mapped output data
        """
        output = {}

        if self.preserve_unmapped:
            output = self._deep_copy(source)

        for mapping in self.mappings:
            try:
                value = self._apply_mapping(mapping, source)
                self._set_value(output, mapping.target, value)
            except Exception as e:
                if mapping.required or self.strict:
                    raise ValueError(f"Mapping failed for {mapping.source} -> {mapping.target}: {e}")
                logger.warning(f"Mapping failed for {mapping.source}: {e}")

        return output

    def _apply_mapping(
        self,
        mapping: FieldMapping,
        source: Dict[str, Any],
    ) -> Any:
        """Apply a single mapping"""
        # Check condition first
        if mapping.condition:
            return self._apply_conditional(mapping, source)

        # Get source value
        value = self._get_value(source, mapping.source)

        # Apply default if None
        if value is None:
            if mapping.required:
                raise ValueError(f"Required field not found: {mapping.source}")
            value = mapping.default

        # Apply transform
        if mapping.transform and value is not None:
            value = self._apply_transform(
                value,
                mapping.transform,
                mapping.transform_args,
                mapping.transform_kwargs,
            )

        return value

    def _apply_conditional(
        self,
        mapping: FieldMapping,
        source: Dict[str, Any],
    ) -> Any:
        """Apply conditional mapping"""
        condition = mapping.condition
        result = self._evaluate_condition(condition.expression, source)

        if result:
            if condition.if_true_source:
                return self._get_value(source, condition.if_true_source)
            return condition.if_true_value
        else:
            if condition.if_false_source:
                return self._get_value(source, condition.if_false_source)
            return condition.if_false_value

    def _evaluate_condition(self, expression: str, source: Dict[str, Any]) -> bool:
        """Evaluate condition expression"""
        try:
            # Simple condition parsing
            # Supports: $.field == value, $.field != value, $.field exists
            if "==" in expression:
                parts = expression.split("==")
                left = self._get_value(source, parts[0].strip())
                right = parts[1].strip().strip("'\"")
                return str(left) == right
            elif "!=" in expression:
                parts = expression.split("!=")
                left = self._get_value(source, parts[0].strip())
                right = parts[1].strip().strip("'\"")
                return str(left) != right
            elif "exists" in expression:
                path = expression.replace("exists", "").strip()
                return self._get_value(source, path) is not None

            return False

        except Exception as e:
            logger.warning(f"Condition evaluation failed: {e}")
            return False

    def _apply_transform(
        self,
        value: Any,
        transform: Union[TransformFunction, str, Callable],
        args: List[Any],
        kwargs: Dict[str, Any],
    ) -> Any:
        """Apply transform function to value"""
        if callable(transform):
            return transform(value, *args, **kwargs)

        transform_name = transform.value if isinstance(transform, TransformFunction) else transform

        if transform_name in self.TRANSFORMS:
            return self.TRANSFORMS[transform_name](value, *args, **kwargs)

        raise ValueError(f"Unknown transform: {transform_name}")

    def _get_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get value from data using JSONPath-like syntax"""
        if not path:
            return data

        # Handle root reference
        if path == "$":
            return data

        # Remove leading $.
        if path.startswith("$."):
            path = path[2:]

        parts = self._parse_path(path)
        current = data

        for part in parts:
            if current is None:
                return None

            if isinstance(part, int):
                # Array index
                if isinstance(current, list) and 0 <= part < len(current):
                    current = current[part]
                else:
                    return None
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current

    def _set_value(self, data: Dict[str, Any], path: str, value: Any):
        """Set value in data using JSONPath-like syntax"""
        if not path or path == "$":
            return

        # Remove leading $.
        if path.startswith("$."):
            path = path[2:]

        parts = self._parse_path(path)

        current = data
        for i, part in enumerate(parts[:-1]):
            if isinstance(part, int):
                # Array index
                while len(current) <= part:
                    current.append({})
                if not isinstance(current[part], (dict, list)):
                    current[part] = {}
                current = current[part]
            else:
                if part not in current:
                    # Check if next part is array index
                    next_part = parts[i + 1]
                    current[part] = [] if isinstance(next_part, int) else {}
                current = current[part]

        # Set final value
        final_part = parts[-1]
        if isinstance(final_part, int):
            while len(current) <= final_part:
                current.append(None)
            current[final_part] = value
        else:
            current[final_part] = value

    def _parse_path(self, path: str) -> List[Union[str, int]]:
        """Parse JSONPath into parts"""
        parts = []

        # Handle array notation [n]
        current = ""
        i = 0
        while i < len(path):
            char = path[i]

            if char == ".":
                if current:
                    parts.append(current)
                    current = ""
            elif char == "[":
                if current:
                    parts.append(current)
                    current = ""
                # Find closing bracket
                end = path.find("]", i)
                if end > i:
                    index_str = path[i + 1:end]
                    if index_str.isdigit():
                        parts.append(int(index_str))
                    else:
                        parts.append(index_str.strip("'\""))
                    i = end
            else:
                current += char

            i += 1

        if current:
            parts.append(current)

        return parts

    def _deep_copy(self, data: Any) -> Any:
        """Deep copy data structure"""
        if isinstance(data, dict):
            return {k: self._deep_copy(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._deep_copy(item) for item in data]
        return data

    # Transform implementations
    def _concat(self, value: Any, *parts, separator: str = "", **kwargs) -> str:
        """Concatenate strings"""
        all_parts = [str(value)] + [str(p) for p in parts]
        return separator.join(all_parts)

    def _substring(self, value: Any, start: int = 0, end: Optional[int] = None, **kwargs) -> str:
        """Extract substring"""
        s = str(value) if value else ""
        return s[start:end]

    def _format_number(self, value: Any, format_str: str = ",.2f", **kwargs) -> str:
        """Format number"""
        try:
            return format(float(value), format_str)
        except (ValueError, TypeError):
            return str(value)

    def _date_format(self, value: Any, output_format: str = "%Y-%m-%d", input_format: Optional[str] = None, **kwargs) -> str:
        """Format date"""
        if isinstance(value, datetime):
            return value.strftime(output_format)

        if isinstance(value, str):
            # Try common formats
            formats = [input_format] if input_format else [
                "%Y-%m-%d",
                "%Y%m%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime(output_format)
                except ValueError:
                    continue

        return str(value)

    def _date_parse(self, value: Any, input_format: str = "%Y-%m-%d", **kwargs) -> datetime:
        """Parse date string"""
        if isinstance(value, datetime):
            return value
        return datetime.strptime(str(value), input_format)

    def _date_add(self, value: Any, days: int = 0, hours: int = 0, **kwargs) -> str:
        """Add time to date"""
        from datetime import timedelta
        if isinstance(value, str):
            value = self._date_parse(value)
        result = value + timedelta(days=days, hours=hours)
        return result.isoformat()

    def _to_boolean(self, value: Any, **kwargs) -> bool:
        """Convert to boolean"""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "yes", "1", "y")
        return bool(value)

    def _flatten(self, value: Any, **kwargs) -> List[Any]:
        """Flatten nested arrays"""
        if not isinstance(value, list):
            return [value]

        result = []
        for item in value:
            if isinstance(item, list):
                result.extend(self._flatten(item))
            else:
                result.append(item)
        return result

    def _filter(self, value: Any, condition: str, **kwargs) -> List[Any]:
        """Filter array by condition"""
        if not isinstance(value, list):
            return [value] if value else []

        # Simple filter: field == value
        results = []
        for item in value:
            if self._evaluate_condition(condition, {"item": item}):
                results.append(item)
        return results

    def _map_array(self, value: Any, transform: str, **kwargs) -> List[Any]:
        """Map transform over array"""
        if not isinstance(value, list):
            return [value]

        if transform in self.TRANSFORMS:
            return [self.TRANSFORMS[transform](item) for item in value]
        return value

    def _if_else(self, value: Any, condition: bool, if_true: Any, if_false: Any, **kwargs) -> Any:
        """Conditional value"""
        return if_true if condition else if_false

    def _coalesce(self, value: Any, *alternatives, **kwargs) -> Any:
        """Return first non-None value"""
        if value is not None:
            return value
        for alt in alternatives:
            if alt is not None:
                return alt
        return None

    # Healthcare-specific transforms
    def _hl7_date_format(self, value: Any, **kwargs) -> str:
        """Convert to HL7 date format (YYYYMMDD or YYYYMMDDHHMMSS)"""
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d%H%M%S")
        if isinstance(value, str):
            # Try to parse and reformat
            dt = self._date_parse(value)
            return dt.strftime("%Y%m%d%H%M%S")
        return str(value)

    def _fhir_date_format(self, value: Any, **kwargs) -> str:
        """Convert to FHIR date format (YYYY-MM-DD)"""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, str):
            # Handle HL7 format
            if len(value) >= 8 and value.isdigit():
                return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
        return str(value)

    def _npi_format(self, value: Any, **kwargs) -> str:
        """Format NPI (National Provider Identifier)"""
        npi = str(value).replace("-", "").replace(" ", "")
        if len(npi) == 10:
            return npi
        return value

    def _ssn_mask(self, value: Any, **kwargs) -> str:
        """Mask SSN (show last 4 only)"""
        ssn = str(value).replace("-", "").replace(" ", "")
        if len(ssn) >= 4:
            return f"***-**-{ssn[-4:]}"
        return "***-**-****"

    def _phi_mask(self, value: Any, **kwargs) -> str:
        """Generic PHI masking"""
        if value is None:
            return "[REDACTED]"
        s = str(value)
        if len(s) <= 2:
            return "*" * len(s)
        return s[0] + "*" * (len(s) - 2) + s[-1]


# Convenience function
def create_mapper_from_config(config: VisualMapperConfig) -> DataMapper:
    """Create DataMapper from visual config"""
    return DataMapper(mappings=config.mappings)

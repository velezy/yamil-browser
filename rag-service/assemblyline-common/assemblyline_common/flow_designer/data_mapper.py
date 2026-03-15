"""
Visual Data Mapper for Logic Weaver Flow Designer.

Provides a comprehensive data mapping system:
- Schema Definition: Define source and target schemas
- Field Mapping: Map fields with transformations
- Transformation Functions: Built-in and custom transforms
- Visual Builder: Support for drag-and-drop UI

This is a LIGHTWEIGHT mapping engine - complex transformations
are delegated to the Python Transform node.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import re
import json
import copy
from datetime import datetime, date
from decimal import Decimal


# =============================================================================
# Schema Types
# =============================================================================


class SchemaType(Enum):
    """Supported schema field types."""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    DATE = "date"
    DATETIME = "datetime"
    NULL = "null"
    ANY = "any"


@dataclass
class SchemaField:
    """Definition of a schema field."""

    name: str
    type: SchemaType
    description: Optional[str] = None
    required: bool = False
    nullable: bool = False
    default_value: Any = None

    # For ARRAY type
    items_type: Optional["SchemaField"] = None

    # For OBJECT type
    properties: Optional[List["SchemaField"]] = None

    # Path for nested access
    path: str = ""

    # Metadata
    format: Optional[str] = None  # date-time, email, uri, etc.
    enum: Optional[List[Any]] = None
    examples: Optional[List[Any]] = None

    def __post_init__(self):
        if not self.path:
            self.path = self.name

    @classmethod
    def from_json_schema(
        cls,
        name: str,
        schema: Dict[str, Any],
        path: str = ""
    ) -> "SchemaField":
        """Create from JSON Schema."""
        schema_type = schema.get("type", "any")

        # Handle type unions
        if isinstance(schema_type, list):
            if "null" in schema_type:
                schema_type = [t for t in schema_type if t != "null"][0]
            else:
                schema_type = schema_type[0]

        type_mapping = {
            "string": SchemaType.STRING,
            "number": SchemaType.NUMBER,
            "integer": SchemaType.INTEGER,
            "boolean": SchemaType.BOOLEAN,
            "array": SchemaType.ARRAY,
            "object": SchemaType.OBJECT,
            "null": SchemaType.NULL,
        }

        field = cls(
            name=name,
            type=type_mapping.get(schema_type, SchemaType.ANY),
            description=schema.get("description"),
            required=False,  # Set from parent
            nullable="null" in (schema.get("type", []) if isinstance(schema.get("type"), list) else []),
            default_value=schema.get("default"),
            format=schema.get("format"),
            enum=schema.get("enum"),
            examples=schema.get("examples"),
            path=path or name
        )

        # Handle array items
        if field.type == SchemaType.ARRAY and "items" in schema:
            field.items_type = cls.from_json_schema(
                "items",
                schema["items"],
                f"{field.path}[]"
            )

        # Handle object properties
        if field.type == SchemaType.OBJECT and "properties" in schema:
            required_fields = set(schema.get("required", []))
            field.properties = [
                cls.from_json_schema(
                    prop_name,
                    prop_schema,
                    f"{field.path}.{prop_name}" if field.path else prop_name
                )
                for prop_name, prop_schema in schema["properties"].items()
            ]
            # Mark required fields
            for prop in field.properties:
                prop.required = prop.name in required_fields

        return field

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema."""
        schema: Dict[str, Any] = {}

        type_mapping = {
            SchemaType.STRING: "string",
            SchemaType.NUMBER: "number",
            SchemaType.INTEGER: "integer",
            SchemaType.BOOLEAN: "boolean",
            SchemaType.ARRAY: "array",
            SchemaType.OBJECT: "object",
            SchemaType.NULL: "null",
            SchemaType.ANY: {},  # No type constraint
        }

        if self.type != SchemaType.ANY:
            schema["type"] = type_mapping[self.type]

        if self.description:
            schema["description"] = self.description

        if self.default_value is not None:
            schema["default"] = self.default_value

        if self.format:
            schema["format"] = self.format

        if self.enum:
            schema["enum"] = self.enum

        # Array items
        if self.type == SchemaType.ARRAY and self.items_type:
            schema["items"] = self.items_type.to_json_schema()

        # Object properties
        if self.type == SchemaType.OBJECT and self.properties:
            schema["properties"] = {
                prop.name: prop.to_json_schema()
                for prop in self.properties
            }
            required = [p.name for p in self.properties if p.required]
            if required:
                schema["required"] = required

        return schema

    def flatten(self, prefix: str = "") -> List["SchemaField"]:
        """Flatten nested schema to list of leaf fields with paths."""
        current_path = f"{prefix}.{self.name}" if prefix else self.name

        if self.type == SchemaType.OBJECT and self.properties:
            result = []
            for prop in self.properties:
                result.extend(prop.flatten(current_path))
            return result
        elif self.type == SchemaType.ARRAY and self.items_type:
            items_path = f"{current_path}[]"
            if self.items_type.type == SchemaType.OBJECT:
                result = []
                for prop in (self.items_type.properties or []):
                    result.extend(prop.flatten(items_path))
                return result
            else:
                return [SchemaField(
                    name="item",
                    type=self.items_type.type,
                    path=items_path
                )]
        else:
            return [SchemaField(
                name=self.name,
                type=self.type,
                path=current_path,
                description=self.description,
                required=self.required
            )]


@dataclass
class SchemaDefinition:
    """Complete schema definition."""

    name: str
    fields: List[SchemaField] = field(default_factory=list)
    description: Optional[str] = None
    version: str = "1.0"

    @classmethod
    def from_json_schema(cls, schema: Dict[str, Any]) -> "SchemaDefinition":
        """Create from JSON Schema."""
        name = schema.get("title", "Schema")
        description = schema.get("description")

        fields = []
        if schema.get("type") == "object" and "properties" in schema:
            required_fields = set(schema.get("required", []))
            for prop_name, prop_schema in schema["properties"].items():
                field = SchemaField.from_json_schema(prop_name, prop_schema)
                field.required = prop_name in required_fields
                fields.append(field)

        return cls(name=name, fields=fields, description=description)

    @classmethod
    def from_sample(
        cls,
        sample: Dict[str, Any],
        name: str = "InferredSchema"
    ) -> "SchemaDefinition":
        """Infer schema from sample data."""

        def infer_type(value: Any) -> SchemaType:
            if value is None:
                return SchemaType.NULL
            elif isinstance(value, bool):
                return SchemaType.BOOLEAN
            elif isinstance(value, int):
                return SchemaType.INTEGER
            elif isinstance(value, float):
                return SchemaType.NUMBER
            elif isinstance(value, str):
                return SchemaType.STRING
            elif isinstance(value, list):
                return SchemaType.ARRAY
            elif isinstance(value, dict):
                return SchemaType.OBJECT
            else:
                return SchemaType.ANY

        def infer_field(key: str, value: Any, path: str = "") -> SchemaField:
            field_path = f"{path}.{key}" if path else key
            field_type = infer_type(value)

            field = SchemaField(
                name=key,
                type=field_type,
                path=field_path
            )

            if field_type == SchemaType.ARRAY and value:
                item_type = infer_type(value[0])
                if item_type == SchemaType.OBJECT:
                    field.items_type = SchemaField(
                        name="item",
                        type=SchemaType.OBJECT,
                        properties=[
                            infer_field(k, v, f"{field_path}[]")
                            for k, v in value[0].items()
                        ]
                    )
                else:
                    field.items_type = SchemaField(
                        name="item",
                        type=item_type
                    )

            elif field_type == SchemaType.OBJECT:
                field.properties = [
                    infer_field(k, v, field_path)
                    for k, v in value.items()
                ]

            return field

        fields = [
            infer_field(k, v)
            for k, v in sample.items()
        ]

        return cls(name=name, fields=fields)

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema."""
        properties = {}
        required = []

        for f in self.fields:
            properties[f.name] = f.to_json_schema()
            if f.required:
                required.append(f.name)

        schema = {
            "type": "object",
            "title": self.name,
            "properties": properties
        }

        if self.description:
            schema["description"] = self.description

        if required:
            schema["required"] = required

        return schema

    def get_flat_fields(self) -> List[SchemaField]:
        """Get flattened list of all fields with paths."""
        result = []
        for f in self.fields:
            result.extend(f.flatten())
        return result


# =============================================================================
# Transformation Functions
# =============================================================================


class TransformType(Enum):
    """Types of transformation functions."""
    # String transforms
    UPPERCASE = "uppercase"
    LOWERCASE = "lowercase"
    TRIM = "trim"
    SUBSTRING = "substring"
    REPLACE = "replace"
    SPLIT = "split"
    JOIN = "join"
    CONCAT = "concat"
    PAD_LEFT = "pad_left"
    PAD_RIGHT = "pad_right"
    REGEX_EXTRACT = "regex_extract"
    REGEX_REPLACE = "regex_replace"

    # Number transforms
    ROUND = "round"
    FLOOR = "floor"
    CEIL = "ceil"
    ABS = "abs"
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    MODULO = "modulo"

    # Date transforms
    FORMAT_DATE = "format_date"
    PARSE_DATE = "parse_date"
    ADD_DAYS = "add_days"
    ADD_HOURS = "add_hours"
    DATE_DIFF = "date_diff"
    NOW = "now"

    # Type conversions
    TO_STRING = "to_string"
    TO_NUMBER = "to_number"
    TO_INTEGER = "to_integer"
    TO_BOOLEAN = "to_boolean"
    TO_DATE = "to_date"
    TO_ARRAY = "to_array"
    TO_JSON = "to_json"
    FROM_JSON = "from_json"

    # Array transforms
    FIRST = "first"
    LAST = "last"
    NTH = "nth"
    LENGTH = "length"
    FLATTEN = "flatten"
    UNIQUE = "unique"
    SORT = "sort"
    REVERSE = "reverse"
    FILTER = "filter"
    MAP = "map"

    # Logic transforms
    IF_ELSE = "if_else"
    COALESCE = "coalesce"
    DEFAULT = "default"
    LOOKUP = "lookup"
    SWITCH = "switch"

    # Object transforms
    GET_FIELD = "get_field"
    SET_FIELD = "set_field"
    DELETE_FIELD = "delete_field"
    MERGE = "merge"
    PICK = "pick"
    OMIT = "omit"

    # Custom
    CUSTOM = "custom"
    EXPRESSION = "expression"


@dataclass
class TransformFunction:
    """Definition of a transformation function."""

    type: TransformType
    parameters: Dict[str, Any] = field(default_factory=dict)
    label: Optional[str] = None

    def apply(self, value: Any) -> Any:
        """Apply the transformation to a value."""
        try:
            return self._execute(value)
        except Exception as e:
            # Return original on error, log in production
            return value

    def _execute(self, value: Any) -> Any:
        """Execute the transformation."""
        t = self.type

        # String transforms
        if t == TransformType.UPPERCASE:
            return str(value).upper() if value else value
        elif t == TransformType.LOWERCASE:
            return str(value).lower() if value else value
        elif t == TransformType.TRIM:
            return str(value).strip() if value else value
        elif t == TransformType.SUBSTRING:
            start = self.parameters.get("start", 0)
            end = self.parameters.get("end")
            s = str(value) if value else ""
            return s[start:end]
        elif t == TransformType.REPLACE:
            old = self.parameters.get("old", "")
            new = self.parameters.get("new", "")
            return str(value).replace(old, new) if value else value
        elif t == TransformType.SPLIT:
            delimiter = self.parameters.get("delimiter", ",")
            return str(value).split(delimiter) if value else []
        elif t == TransformType.JOIN:
            delimiter = self.parameters.get("delimiter", ",")
            return delimiter.join(value) if isinstance(value, list) else str(value)
        elif t == TransformType.CONCAT:
            values = [value] + self.parameters.get("values", [])
            separator = self.parameters.get("separator", "")
            return separator.join(str(v) for v in values if v is not None)
        elif t == TransformType.PAD_LEFT:
            width = self.parameters.get("width", 0)
            char = self.parameters.get("char", " ")
            return str(value).rjust(width, char) if value else value
        elif t == TransformType.PAD_RIGHT:
            width = self.parameters.get("width", 0)
            char = self.parameters.get("char", " ")
            return str(value).ljust(width, char) if value else value
        elif t == TransformType.REGEX_EXTRACT:
            pattern = self.parameters.get("pattern", "")
            group = self.parameters.get("group", 0)
            if value:
                match = re.search(pattern, str(value))
                if match:
                    return match.group(group)
            return None
        elif t == TransformType.REGEX_REPLACE:
            pattern = self.parameters.get("pattern", "")
            replacement = self.parameters.get("replacement", "")
            return re.sub(pattern, replacement, str(value)) if value else value

        # Number transforms
        elif t == TransformType.ROUND:
            decimals = self.parameters.get("decimals", 0)
            return round(float(value), decimals) if value is not None else value
        elif t == TransformType.FLOOR:
            import math
            return math.floor(float(value)) if value is not None else value
        elif t == TransformType.CEIL:
            import math
            return math.ceil(float(value)) if value is not None else value
        elif t == TransformType.ABS:
            return abs(float(value)) if value is not None else value
        elif t == TransformType.ADD:
            operand = self.parameters.get("operand", 0)
            return float(value) + operand if value is not None else value
        elif t == TransformType.SUBTRACT:
            operand = self.parameters.get("operand", 0)
            return float(value) - operand if value is not None else value
        elif t == TransformType.MULTIPLY:
            operand = self.parameters.get("operand", 1)
            return float(value) * operand if value is not None else value
        elif t == TransformType.DIVIDE:
            operand = self.parameters.get("operand", 1)
            if operand == 0:
                return None
            return float(value) / operand if value is not None else value
        elif t == TransformType.MODULO:
            operand = self.parameters.get("operand", 1)
            return int(value) % operand if value is not None else value

        # Date transforms
        elif t == TransformType.FORMAT_DATE:
            fmt = self.parameters.get("format", "%Y-%m-%d")
            if isinstance(value, (datetime, date)):
                return value.strftime(fmt)
            return value
        elif t == TransformType.PARSE_DATE:
            fmt = self.parameters.get("format", "%Y-%m-%d")
            if isinstance(value, str):
                return datetime.strptime(value, fmt)
            return value
        elif t == TransformType.ADD_DAYS:
            from datetime import timedelta
            days = self.parameters.get("days", 0)
            if isinstance(value, (datetime, date)):
                return value + timedelta(days=days)
            return value
        elif t == TransformType.ADD_HOURS:
            from datetime import timedelta
            hours = self.parameters.get("hours", 0)
            if isinstance(value, datetime):
                return value + timedelta(hours=hours)
            return value
        elif t == TransformType.NOW:
            return datetime.utcnow()

        # Type conversions
        elif t == TransformType.TO_STRING:
            return str(value) if value is not None else ""
        elif t == TransformType.TO_NUMBER:
            if value is None or value == "":
                return None
            return float(value)
        elif t == TransformType.TO_INTEGER:
            if value is None or value == "":
                return None
            return int(float(value))
        elif t == TransformType.TO_BOOLEAN:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ("true", "yes", "1", "on")
            return bool(value)
        elif t == TransformType.TO_ARRAY:
            if isinstance(value, list):
                return value
            if value is None:
                return []
            return [value]
        elif t == TransformType.TO_JSON:
            return json.dumps(value)
        elif t == TransformType.FROM_JSON:
            return json.loads(value) if isinstance(value, str) else value

        # Array transforms
        elif t == TransformType.FIRST:
            return value[0] if isinstance(value, list) and value else None
        elif t == TransformType.LAST:
            return value[-1] if isinstance(value, list) and value else None
        elif t == TransformType.NTH:
            index = self.parameters.get("index", 0)
            if isinstance(value, list) and 0 <= index < len(value):
                return value[index]
            return None
        elif t == TransformType.LENGTH:
            return len(value) if value else 0
        elif t == TransformType.FLATTEN:
            if isinstance(value, list):
                result = []
                for item in value:
                    if isinstance(item, list):
                        result.extend(item)
                    else:
                        result.append(item)
                return result
            return value
        elif t == TransformType.UNIQUE:
            if isinstance(value, list):
                seen = set()
                result = []
                for item in value:
                    key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else item
                    if key not in seen:
                        seen.add(key)
                        result.append(item)
                return result
            return value
        elif t == TransformType.SORT:
            key_field = self.parameters.get("key")
            reverse = self.parameters.get("reverse", False)
            if isinstance(value, list):
                if key_field and value and isinstance(value[0], dict):
                    return sorted(value, key=lambda x: x.get(key_field, ""), reverse=reverse)
                return sorted(value, reverse=reverse)
            return value
        elif t == TransformType.REVERSE:
            if isinstance(value, list):
                return list(reversed(value))
            return value

        # Logic transforms
        elif t == TransformType.IF_ELSE:
            condition = self.parameters.get("condition")
            then_value = self.parameters.get("then")
            else_value = self.parameters.get("else")
            # Simple condition check - production would use expression evaluator
            if condition:
                return then_value
            return else_value
        elif t == TransformType.COALESCE:
            values = [value] + self.parameters.get("values", [])
            for v in values:
                if v is not None and v != "":
                    return v
            return None
        elif t == TransformType.DEFAULT:
            default = self.parameters.get("default")
            return value if value is not None and value != "" else default
        elif t == TransformType.LOOKUP:
            table = self.parameters.get("table", {})
            default = self.parameters.get("default")
            return table.get(value, default)
        elif t == TransformType.SWITCH:
            cases = self.parameters.get("cases", {})
            default = self.parameters.get("default")
            return cases.get(value, default)

        # Object transforms
        elif t == TransformType.GET_FIELD:
            path = self.parameters.get("path", "")
            default = self.parameters.get("default")
            if isinstance(value, dict):
                return _get_nested(value, path, default)
            return default
        elif t == TransformType.MERGE:
            other = self.parameters.get("other", {})
            if isinstance(value, dict):
                return {**value, **other}
            return value
        elif t == TransformType.PICK:
            fields = self.parameters.get("fields", [])
            if isinstance(value, dict):
                return {k: v for k, v in value.items() if k in fields}
            return value
        elif t == TransformType.OMIT:
            fields = self.parameters.get("fields", [])
            if isinstance(value, dict):
                return {k: v for k, v in value.items() if k not in fields}
            return value

        # Custom
        elif t == TransformType.EXPRESSION:
            # Expression would be evaluated in sandbox
            expr = self.parameters.get("expression", "value")
            # Simplified - production uses safe expression evaluator
            return value

        return value


# =============================================================================
# Field Mapping
# =============================================================================


@dataclass
class MappingRule:
    """A single mapping rule."""

    source_path: str
    target_path: str
    transforms: List[TransformFunction] = field(default_factory=list)
    condition: Optional[str] = None  # Optional condition expression
    enabled: bool = True

    def apply(self, source_value: Any) -> Any:
        """Apply all transforms to the source value."""
        result = source_value

        for transform in self.transforms:
            result = transform.apply(result)

        return result


@dataclass
class FieldMapping:
    """Complete field mapping between source and target."""

    source_field: str
    target_field: str
    transform_chain: List[TransformFunction] = field(default_factory=list)
    description: Optional[str] = None

    # Mapping mode
    mode: str = "map"  # map, constant, expression, array_map

    # For constant mode
    constant_value: Any = None

    # For expression mode
    expression: Optional[str] = None

    # For array mapping
    array_item_mapping: Optional[List["FieldMapping"]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source": self.source_field,
            "target": self.target_field,
            "mode": self.mode,
            "transforms": [
                {"type": t.type.value, "params": t.parameters}
                for t in self.transform_chain
            ],
            "constant": self.constant_value,
            "expression": self.expression,
            "description": self.description
        }


# =============================================================================
# Mapping Configuration
# =============================================================================


@dataclass
class MappingConfig:
    """Configuration for data mapping."""

    name: str
    source_schema: Optional[SchemaDefinition] = None
    target_schema: Optional[SchemaDefinition] = None
    mappings: List[FieldMapping] = field(default_factory=list)
    description: Optional[str] = None
    version: str = "1.0"

    # Behavior options
    ignore_unmapped: bool = True
    null_handling: str = "pass"  # pass, skip, default
    default_value: Any = None
    strict_types: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "source_schema": self.source_schema.to_json_schema() if self.source_schema else None,
            "target_schema": self.target_schema.to_json_schema() if self.target_schema else None,
            "mappings": [m.to_dict() for m in self.mappings],
            "options": {
                "ignore_unmapped": self.ignore_unmapped,
                "null_handling": self.null_handling,
                "default_value": self.default_value,
                "strict_types": self.strict_types
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MappingConfig":
        """Create from dictionary."""
        config = cls(
            name=data.get("name", "Mapping"),
            description=data.get("description"),
            version=data.get("version", "1.0")
        )

        if data.get("source_schema"):
            config.source_schema = SchemaDefinition.from_json_schema(data["source_schema"])

        if data.get("target_schema"):
            config.target_schema = SchemaDefinition.from_json_schema(data["target_schema"])

        for m in data.get("mappings", []):
            transforms = [
                TransformFunction(
                    type=TransformType(t["type"]),
                    parameters=t.get("params", {})
                )
                for t in m.get("transforms", [])
            ]

            config.mappings.append(FieldMapping(
                source_field=m["source"],
                target_field=m["target"],
                mode=m.get("mode", "map"),
                transform_chain=transforms,
                constant_value=m.get("constant"),
                expression=m.get("expression"),
                description=m.get("description")
            ))

        options = data.get("options", {})
        config.ignore_unmapped = options.get("ignore_unmapped", True)
        config.null_handling = options.get("null_handling", "pass")
        config.default_value = options.get("default_value")
        config.strict_types = options.get("strict_types", False)

        return config


# =============================================================================
# Data Mapper
# =============================================================================


def _get_nested(obj: Dict, path: str, default: Any = None) -> Any:
    """Get nested value by dot path."""
    if not path:
        return obj

    parts = path.replace("[]", "").split(".")
    current = obj

    for part in parts:
        if isinstance(current, dict):
            current = current.get(part, default)
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            current = current[idx] if 0 <= idx < len(current) else default
        else:
            return default

        if current is None:
            return default

    return current


def _set_nested(obj: Dict, path: str, value: Any) -> None:
    """Set nested value by dot path."""
    parts = path.split(".")
    current = obj

    for i, part in enumerate(parts[:-1]):
        if part not in current:
            # Check if next part is array index or object key
            next_part = parts[i + 1]
            if next_part.isdigit() or next_part == "[]":
                current[part] = []
            else:
                current[part] = {}
        current = current[part]

    final_key = parts[-1]
    if final_key.endswith("[]"):
        final_key = final_key[:-2]
        if final_key not in current:
            current[final_key] = []
        if isinstance(value, list):
            current[final_key].extend(value)
        else:
            current[final_key].append(value)
    else:
        current[final_key] = value


class DataMapper:
    """
    Main data mapper for transforming messages.

    Applies mapping configurations to transform source data
    into target format.
    """

    def __init__(self, config: Optional[MappingConfig] = None):
        """
        Initialize data mapper.

        Args:
            config: Mapping configuration
        """
        self.config = config

    def map(
        self,
        source: Dict[str, Any],
        config: Optional[MappingConfig] = None
    ) -> Dict[str, Any]:
        """
        Map source data to target format.

        Args:
            source: Source data dictionary
            config: Optional mapping config override

        Returns:
            Mapped target data
        """
        mapping_config = config or self.config
        if not mapping_config:
            return source

        target: Dict[str, Any] = {}

        for mapping in mapping_config.mappings:
            try:
                value = self._apply_mapping(source, mapping, mapping_config)

                # Handle null
                if value is None:
                    if mapping_config.null_handling == "skip":
                        continue
                    elif mapping_config.null_handling == "default":
                        value = mapping_config.default_value

                _set_nested(target, mapping.target_field, value)

            except Exception:
                # Log error in production, continue with other mappings
                continue

        return target

    def _apply_mapping(
        self,
        source: Dict[str, Any],
        mapping: FieldMapping,
        config: MappingConfig
    ) -> Any:
        """Apply a single field mapping."""

        if mapping.mode == "constant":
            return mapping.constant_value

        elif mapping.mode == "expression":
            # Expression evaluation - simplified
            # Production would use safe expression evaluator
            return mapping.constant_value

        elif mapping.mode == "array_map":
            # Map each item in source array
            source_array = _get_nested(source, mapping.source_field, [])
            if not isinstance(source_array, list):
                return []

            result = []
            for item in source_array:
                if mapping.array_item_mapping:
                    mapped_item = {}
                    for item_mapping in mapping.array_item_mapping:
                        item_value = _get_nested(item, item_mapping.source_field)
                        for transform in item_mapping.transform_chain:
                            item_value = transform.apply(item_value)
                        mapped_item[item_mapping.target_field] = item_value
                    result.append(mapped_item)
                else:
                    result.append(item)
            return result

        else:  # mode == "map"
            value = _get_nested(source, mapping.source_field)

            # Apply transforms
            for transform in mapping.transform_chain:
                value = transform.apply(value)

            return value

    def validate_source(
        self,
        source: Dict[str, Any]
    ) -> List[str]:
        """Validate source data against source schema."""
        if not self.config or not self.config.source_schema:
            return []

        # Simple validation - production would use jsonschema
        errors = []
        flat_fields = self.config.source_schema.get_flat_fields()

        for field in flat_fields:
            if field.required:
                value = _get_nested(source, field.path)
                if value is None:
                    errors.append(f"Required field missing: {field.path}")

        return errors

    def validate_target(
        self,
        target: Dict[str, Any]
    ) -> List[str]:
        """Validate target data against target schema."""
        if not self.config or not self.config.target_schema:
            return []

        errors = []
        flat_fields = self.config.target_schema.get_flat_fields()

        for field in flat_fields:
            if field.required:
                value = _get_nested(target, field.path)
                if value is None:
                    errors.append(f"Required field missing: {field.path}")

        return errors

    def preview(
        self,
        source: Dict[str, Any],
        config: Optional[MappingConfig] = None
    ) -> Dict[str, Any]:
        """
        Preview mapping result with detailed info.

        Returns mapping result along with diagnostic info.
        """
        mapping_config = config or self.config

        # Validate source
        source_errors = self.validate_source(source)

        # Perform mapping
        target = self.map(source, mapping_config)

        # Validate target
        target_errors = self.validate_target(target)

        return {
            "source": source,
            "target": target,
            "source_valid": len(source_errors) == 0,
            "source_errors": source_errors,
            "target_valid": len(target_errors) == 0,
            "target_errors": target_errors,
            "mappings_applied": len(mapping_config.mappings) if mapping_config else 0
        }


# =============================================================================
# Mapping Builder
# =============================================================================


class MappingBuilder:
    """
    Fluent builder for creating mapping configurations.

    Example:
        config = (
            MappingBuilder("Patient Mapping")
            .source_schema(source_schema)
            .target_schema(target_schema)
            .map_field("patient.id", "id")
            .map_field("patient.name.given", "firstName")
            .map_field("patient.name.family", "lastName", transforms=[
                TransformFunction(TransformType.UPPERCASE)
            ])
            .constant("version", "1.0")
            .build()
        )
    """

    def __init__(self, name: str):
        """Initialize builder with mapping name."""
        self._name = name
        self._description: Optional[str] = None
        self._version = "1.0"
        self._source_schema: Optional[SchemaDefinition] = None
        self._target_schema: Optional[SchemaDefinition] = None
        self._mappings: List[FieldMapping] = []
        self._ignore_unmapped = True
        self._null_handling = "pass"
        self._default_value: Any = None
        self._strict_types = False

    def description(self, desc: str) -> "MappingBuilder":
        """Set mapping description."""
        self._description = desc
        return self

    def version(self, version: str) -> "MappingBuilder":
        """Set mapping version."""
        self._version = version
        return self

    def source_schema(self, schema: SchemaDefinition) -> "MappingBuilder":
        """Set source schema."""
        self._source_schema = schema
        return self

    def target_schema(self, schema: SchemaDefinition) -> "MappingBuilder":
        """Set target schema."""
        self._target_schema = schema
        return self

    def source_from_sample(self, sample: Dict[str, Any]) -> "MappingBuilder":
        """Infer source schema from sample."""
        self._source_schema = SchemaDefinition.from_sample(sample, "SourceSchema")
        return self

    def target_from_sample(self, sample: Dict[str, Any]) -> "MappingBuilder":
        """Infer target schema from sample."""
        self._target_schema = SchemaDefinition.from_sample(sample, "TargetSchema")
        return self

    def map_field(
        self,
        source: str,
        target: str,
        transforms: Optional[List[TransformFunction]] = None,
        description: Optional[str] = None
    ) -> "MappingBuilder":
        """Add a field mapping."""
        self._mappings.append(FieldMapping(
            source_field=source,
            target_field=target,
            transform_chain=transforms or [],
            description=description,
            mode="map"
        ))
        return self

    def constant(self, target: str, value: Any) -> "MappingBuilder":
        """Add a constant value mapping."""
        self._mappings.append(FieldMapping(
            source_field="",
            target_field=target,
            mode="constant",
            constant_value=value
        ))
        return self

    def expression(self, target: str, expr: str) -> "MappingBuilder":
        """Add an expression mapping."""
        self._mappings.append(FieldMapping(
            source_field="",
            target_field=target,
            mode="expression",
            expression=expr
        ))
        return self

    def array_map(
        self,
        source: str,
        target: str,
        item_mappings: List[FieldMapping]
    ) -> "MappingBuilder":
        """Add an array mapping."""
        self._mappings.append(FieldMapping(
            source_field=source,
            target_field=target,
            mode="array_map",
            array_item_mapping=item_mappings
        ))
        return self

    def ignore_unmapped(self, ignore: bool = True) -> "MappingBuilder":
        """Set whether to ignore unmapped fields."""
        self._ignore_unmapped = ignore
        return self

    def null_handling(
        self,
        mode: str,
        default: Any = None
    ) -> "MappingBuilder":
        """Set null handling mode."""
        self._null_handling = mode
        self._default_value = default
        return self

    def strict_types(self, strict: bool = True) -> "MappingBuilder":
        """Set strict type checking."""
        self._strict_types = strict
        return self

    def build(self) -> MappingConfig:
        """Build the mapping configuration."""
        return MappingConfig(
            name=self._name,
            description=self._description,
            version=self._version,
            source_schema=self._source_schema,
            target_schema=self._target_schema,
            mappings=self._mappings,
            ignore_unmapped=self._ignore_unmapped,
            null_handling=self._null_handling,
            default_value=self._default_value,
            strict_types=self._strict_types
        )


# =============================================================================
# Mapping Validator
# =============================================================================


@dataclass
class MappingValidationError:
    """A mapping validation error."""
    path: str
    message: str
    severity: str = "error"


class MappingValidator:
    """Validates mapping configurations."""

    def validate(self, config: MappingConfig) -> List[MappingValidationError]:
        """
        Validate a mapping configuration.

        Returns list of validation errors.
        """
        errors = []

        if not config.name:
            errors.append(MappingValidationError(
                "name",
                "Mapping name is required"
            ))

        # Validate each mapping
        for i, mapping in enumerate(config.mappings):
            if mapping.mode == "map" and not mapping.source_field:
                errors.append(MappingValidationError(
                    f"mappings[{i}].source",
                    "Source field is required for map mode"
                ))

            if not mapping.target_field:
                errors.append(MappingValidationError(
                    f"mappings[{i}].target",
                    "Target field is required"
                ))

            if mapping.mode == "constant" and mapping.constant_value is None:
                errors.append(MappingValidationError(
                    f"mappings[{i}].constant",
                    "Constant value is required for constant mode",
                    "warning"
                ))

            # Validate transforms
            for j, transform in enumerate(mapping.transform_chain):
                # Check required parameters for certain transforms
                if transform.type == TransformType.SUBSTRING:
                    if "start" not in transform.parameters:
                        errors.append(MappingValidationError(
                            f"mappings[{i}].transforms[{j}]",
                            "Substring transform requires 'start' parameter"
                        ))

        # Validate against schemas
        if config.source_schema:
            source_paths = {f.path for f in config.source_schema.get_flat_fields()}
            for i, mapping in enumerate(config.mappings):
                if mapping.mode == "map" and mapping.source_field:
                    # Check if source path exists in schema
                    base_path = mapping.source_field.split("[")[0]
                    if base_path not in source_paths and not any(
                        sp.startswith(base_path) for sp in source_paths
                    ):
                        errors.append(MappingValidationError(
                            f"mappings[{i}].source",
                            f"Source field '{mapping.source_field}' not found in schema",
                            "warning"
                        ))

        if config.target_schema:
            target_paths = {f.path for f in config.target_schema.get_flat_fields()}
            mapped_targets = {m.target_field for m in config.mappings}

            # Check for required target fields not mapped
            for field in config.target_schema.get_flat_fields():
                if field.required and field.path not in mapped_targets:
                    errors.append(MappingValidationError(
                        f"target.{field.path}",
                        f"Required target field '{field.path}' is not mapped"
                    ))

        return errors

    def check_compatibility(
        self,
        source_type: SchemaType,
        target_type: SchemaType,
        transforms: List[TransformFunction]
    ) -> bool:
        """Check if source type can be transformed to target type."""
        current_type = source_type

        # Apply transform type changes
        for transform in transforms:
            if transform.type == TransformType.TO_STRING:
                current_type = SchemaType.STRING
            elif transform.type == TransformType.TO_NUMBER:
                current_type = SchemaType.NUMBER
            elif transform.type == TransformType.TO_INTEGER:
                current_type = SchemaType.INTEGER
            elif transform.type == TransformType.TO_BOOLEAN:
                current_type = SchemaType.BOOLEAN
            elif transform.type == TransformType.TO_ARRAY:
                current_type = SchemaType.ARRAY

        # Check compatibility
        if current_type == target_type:
            return True
        if target_type == SchemaType.ANY:
            return True
        if target_type == SchemaType.STRING:
            return True  # Everything can become string

        return False


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Mapper
    "DataMapper",
    "MappingConfig",
    "FieldMapping",
    "MappingRule",
    # Transformations
    "TransformFunction",
    "TransformType",
    # Schema
    "SchemaDefinition",
    "SchemaField",
    "SchemaType",
    # Builder
    "MappingBuilder",
    "MappingValidator",
    "MappingValidationError",
]

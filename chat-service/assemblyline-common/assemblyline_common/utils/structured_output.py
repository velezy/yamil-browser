"""
Structured Output Validation
JSON Schema enforcement for tool outputs and LLM responses.

Usage:
    from assemblyline_common.utils.structured_output import validate_output, OutputSchema

    # Define schema
    schema = OutputSchema(
        name="search_result",
        schema={
            "type": "object",
            "properties": {
                "results": {"type": "array"},
                "total": {"type": "integer"}
            },
            "required": ["results", "total"]
        }
    )

    # Validate output
    result = validate_output(data, schema)
"""

import json
import logging
from typing import Optional, Dict, Any, List, Union, Type
from dataclasses import dataclass, field
from enum import Enum
from pydantic import BaseModel, ValidationError, create_model
from pydantic.fields import FieldInfo

logger = logging.getLogger(__name__)


class ValidationMode(Enum):
    """How to handle validation failures"""
    STRICT = "strict"      # Raise exception on failure
    COERCE = "coerce"      # Try to coerce to correct type
    LENIENT = "lenient"    # Log warning but continue


@dataclass
class OutputSchema:
    """Schema definition for structured outputs"""
    name: str
    schema: Dict[str, Any]
    description: str = ""
    examples: List[Dict[str, Any]] = field(default_factory=list)

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema format"""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": self.name,
            "description": self.description,
            **self.schema
        }


@dataclass
class ValidationResult:
    """Result of validation"""
    valid: bool
    data: Any
    errors: List[str] = field(default_factory=list)
    coerced: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "data": self.data,
            "errors": self.errors,
            "coerced": self.coerced
        }


# =============================================================================
# PREDEFINED SCHEMAS FOR COMMON OUTPUTS
# =============================================================================

# RAG Search Result Schema
RAG_SEARCH_RESULT_SCHEMA = OutputSchema(
    name="RAGSearchResult",
    description="Search results from RAG retrieval",
    schema={
        "type": "object",
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "content": {"type": "string"},
                        "score": {"type": "number", "minimum": 0, "maximum": 1},
                        "metadata": {"type": "object"},
                        "source": {"type": "string"}
                    },
                    "required": ["content", "score"]
                }
            },
            "total": {"type": "integer", "minimum": 0},
            "query": {"type": "string"},
            "latency_ms": {"type": "number"}
        },
        "required": ["results", "total"]
    }
)

# Tool Execution Result Schema
TOOL_RESULT_SCHEMA = OutputSchema(
    name="ToolResult",
    description="Result from tool execution",
    schema={
        "type": "object",
        "properties": {
            "tool_name": {"type": "string"},
            "success": {"type": "boolean"},
            "result": {},  # Any type
            "error": {"type": ["string", "null"]},
            "execution_time_ms": {"type": "number"}
        },
        "required": ["tool_name", "success"]
    }
)

# LLM Response Schema
LLM_RESPONSE_SCHEMA = OutputSchema(
    name="LLMResponse",
    description="Response from LLM generation",
    schema={
        "type": "object",
        "properties": {
            "content": {"type": "string"},
            "model": {"type": "string"},
            "tokens_used": {"type": "integer", "minimum": 0},
            "finish_reason": {"type": "string", "enum": ["stop", "length", "error"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1}
        },
        "required": ["content", "model"]
    }
)

# Quality Score Schema
QUALITY_SCORE_SCHEMA = OutputSchema(
    name="QualityScore",
    description="Quality evaluation score",
    schema={
        "type": "object",
        "properties": {
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "grade": {"type": "string", "enum": ["A", "B", "C", "D", "F"]},
            "issues": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "category": {"type": "string"},
                        "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                        "description": {"type": "string"}
                    }
                }
            },
            "suggestions": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["score", "grade"]
    }
)

# Agent Pipeline Result Schema
AGENT_PIPELINE_RESULT_SCHEMA = OutputSchema(
    name="AgentPipelineResult",
    description="Complete result from agent pipeline",
    schema={
        "type": "object",
        "properties": {
            "response": {"type": "string"},
            "query": {"type": "string"},
            "success": {"type": "boolean"},
            "context_used": {"type": "boolean"},
            "sources": {"type": "array"},
            "model_used": {"type": "string"},
            "total_latency_ms": {"type": "number"},
            "quality_score": {"type": ["integer", "null"]},
            "reasoning_used": {"type": "boolean"},
            "tool_used": {"type": ["string", "null"]}
        },
        "required": ["response", "query", "success"]
    }
)


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_output(
    data: Any,
    schema: OutputSchema,
    mode: ValidationMode = ValidationMode.STRICT
) -> ValidationResult:
    """
    Validate data against a schema.

    Args:
        data: Data to validate
        schema: Schema to validate against
        mode: How to handle validation failures

    Returns:
        ValidationResult with validation status and potentially coerced data
    """
    try:
        import jsonschema
        from jsonschema import Draft7Validator, ValidationError as JsonSchemaError

        validator = Draft7Validator(schema.to_json_schema())
        errors = list(validator.iter_errors(data))

        if not errors:
            return ValidationResult(valid=True, data=data)

        error_messages = [f"{e.path}: {e.message}" for e in errors]

        if mode == ValidationMode.STRICT:
            return ValidationResult(valid=False, data=data, errors=error_messages)

        elif mode == ValidationMode.COERCE:
            coerced_data = _coerce_data(data, schema.schema)
            # Re-validate coerced data
            coerced_errors = list(validator.iter_errors(coerced_data))
            if not coerced_errors:
                return ValidationResult(valid=True, data=coerced_data, coerced=True)
            return ValidationResult(
                valid=False,
                data=coerced_data,
                errors=[f"{e.path}: {e.message}" for e in coerced_errors],
                coerced=True
            )

        else:  # LENIENT
            logger.warning(f"Validation warnings for {schema.name}: {error_messages}")
            return ValidationResult(valid=True, data=data, errors=error_messages)

    except ImportError:
        logger.warning("jsonschema not installed. Run: pip install jsonschema")
        return ValidationResult(valid=True, data=data)


def _coerce_data(data: Any, schema: Dict[str, Any]) -> Any:
    """Attempt to coerce data to match schema types."""
    if not isinstance(data, dict) or "properties" not in schema:
        return data

    coerced = dict(data)

    for prop, prop_schema in schema.get("properties", {}).items():
        if prop not in coerced:
            continue

        value = coerced[prop]
        expected_type = prop_schema.get("type")

        if expected_type == "string" and not isinstance(value, str):
            coerced[prop] = str(value)
        elif expected_type == "integer" and not isinstance(value, int):
            try:
                coerced[prop] = int(float(value))
            except (ValueError, TypeError):
                pass
        elif expected_type == "number" and not isinstance(value, (int, float)):
            try:
                coerced[prop] = float(value)
            except (ValueError, TypeError):
                pass
        elif expected_type == "boolean" and not isinstance(value, bool):
            if isinstance(value, str):
                coerced[prop] = value.lower() in ("true", "1", "yes")
            else:
                coerced[prop] = bool(value)
        elif expected_type == "array" and not isinstance(value, list):
            coerced[prop] = [value] if value is not None else []
        elif expected_type == "object" and isinstance(value, str):
            try:
                coerced[prop] = json.loads(value)
            except json.JSONDecodeError:
                pass

    return coerced


def validate_with_pydantic(
    data: Dict[str, Any],
    model_class: Type[BaseModel],
    mode: ValidationMode = ValidationMode.STRICT
) -> ValidationResult:
    """
    Validate data using a Pydantic model.

    Args:
        data: Data to validate
        model_class: Pydantic model class
        mode: How to handle validation failures

    Returns:
        ValidationResult with validation status
    """
    try:
        validated = model_class.model_validate(data)
        return ValidationResult(valid=True, data=validated.model_dump())
    except ValidationError as e:
        errors = [f"{err['loc']}: {err['msg']}" for err in e.errors()]

        if mode == ValidationMode.STRICT:
            return ValidationResult(valid=False, data=data, errors=errors)
        elif mode == ValidationMode.LENIENT:
            logger.warning(f"Pydantic validation warnings: {errors}")
            return ValidationResult(valid=True, data=data, errors=errors)
        else:
            # Try to coerce
            try:
                validated = model_class.model_validate(data, strict=False)
                return ValidationResult(valid=True, data=validated.model_dump(), coerced=True)
            except ValidationError as e2:
                return ValidationResult(
                    valid=False,
                    data=data,
                    errors=[f"{err['loc']}: {err['msg']}" for err in e2.errors()]
                )


# =============================================================================
# DECORATOR FOR AUTOMATIC VALIDATION
# =============================================================================

def validate_output_decorator(
    schema: OutputSchema,
    mode: ValidationMode = ValidationMode.STRICT
):
    """
    Decorator to automatically validate function output.

    Usage:
        @validate_output_decorator(TOOL_RESULT_SCHEMA)
        def my_tool():
            return {"tool_name": "calc", "success": True}
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            validation = validate_output(result, schema, mode)

            if not validation.valid and mode == ValidationMode.STRICT:
                raise ValueError(f"Output validation failed: {validation.errors}")

            return validation.data

        async def async_wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            validation = validate_output(result, schema, mode)

            if not validation.valid and mode == ValidationMode.STRICT:
                raise ValueError(f"Output validation failed: {validation.errors}")

            return validation.data

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper

    return decorator


# =============================================================================
# LLM STRUCTURED OUTPUT HELPERS
# =============================================================================

def create_json_schema_prompt(schema: OutputSchema) -> str:
    """
    Create a prompt instructing LLM to output in specified JSON format.

    Args:
        schema: Output schema to use

    Returns:
        Prompt string for the LLM
    """
    schema_json = json.dumps(schema.to_json_schema(), indent=2)

    examples_str = ""
    if schema.examples:
        examples_str = "\n\nExamples:\n"
        for i, example in enumerate(schema.examples, 1):
            examples_str += f"{i}. {json.dumps(example)}\n"

    return f"""Your response MUST be valid JSON matching this schema:

```json
{schema_json}
```
{examples_str}
Return ONLY the JSON object, no additional text or markdown."""


def extract_json_from_response(response: str) -> Optional[Dict[str, Any]]:
    """
    Extract JSON from LLM response that may contain markdown or extra text.

    Args:
        response: Raw LLM response

    Returns:
        Extracted JSON dict or None if extraction fails
    """
    import re

    # Try direct parse first
    try:
        return json.loads(response.strip())
    except json.JSONDecodeError:
        pass

    # Try extracting from code blocks
    code_block_pattern = r'```(?:json)?\s*([\s\S]*?)```'
    matches = re.findall(code_block_pattern, response)
    for match in matches:
        try:
            return json.loads(match.strip())
        except json.JSONDecodeError:
            continue

    # Try finding JSON object pattern
    json_pattern = r'\{[\s\S]*\}'
    matches = re.findall(json_pattern, response)
    for match in matches:
        try:
            return json.loads(match)
        except json.JSONDecodeError:
            continue

    return None


def validate_llm_json_output(
    response: str,
    schema: OutputSchema,
    mode: ValidationMode = ValidationMode.COERCE
) -> ValidationResult:
    """
    Extract and validate JSON from LLM response.

    Args:
        response: Raw LLM response
        schema: Expected output schema
        mode: Validation mode

    Returns:
        ValidationResult with extracted and validated data
    """
    extracted = extract_json_from_response(response)

    if extracted is None:
        return ValidationResult(
            valid=False,
            data=None,
            errors=["Failed to extract JSON from response"]
        )

    return validate_output(extracted, schema, mode)

"""
Expression Builder for Logic Weaver.

Provides a visual expression construction system:
- Expression types for comparisons, logic, math
- Visual builder with drag-and-drop support
- Safe expression evaluation
- Built-in and custom functions

This enables business users to create complex expressions
without writing code.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import re
import json
import operator
from datetime import datetime, date, timedelta
from decimal import Decimal


# =============================================================================
# Expression Types
# =============================================================================


class ExpressionType(Enum):
    """Types of expressions."""
    # Values
    LITERAL = "literal"
    FIELD = "field"
    VARIABLE = "variable"
    FUNCTION = "function"

    # Comparisons
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    GREATER_EQUAL = "greater_equal"
    LESS_THAN = "less_than"
    LESS_EQUAL = "less_equal"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    MATCHES = "matches"  # Regex
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"
    BETWEEN = "between"

    # Logic
    AND = "and"
    OR = "or"
    NOT = "not"
    IF = "if"

    # Math
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    MODULO = "modulo"
    POWER = "power"
    NEGATE = "negate"
    ABS = "abs"
    ROUND = "round"
    FLOOR = "floor"
    CEIL = "ceil"
    MIN = "min"
    MAX = "max"

    # String
    CONCAT = "concat"
    UPPER = "upper"
    LOWER = "lower"
    TRIM = "trim"
    SUBSTRING = "substring"
    LENGTH = "length"
    REPLACE = "replace"

    # Date
    NOW = "now"
    TODAY = "today"
    DATE_ADD = "date_add"
    DATE_DIFF = "date_diff"
    FORMAT_DATE = "format_date"

    # Collection
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    FIRST = "first"
    LAST = "last"
    FILTER = "filter"
    MAP = "map"

    # Special
    COALESCE = "coalesce"
    SWITCH = "switch"


class ExpressionOperator(Enum):
    """Operators for expressions."""
    # Comparison
    EQ = "=="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="

    # Logic
    AND = "&&"
    OR = "||"
    NOT = "!"

    # Math
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"
    POW = "**"


# =============================================================================
# Expression Values
# =============================================================================


@dataclass
class ExpressionValue:
    """A value in an expression."""

    type: str  # literal, field, variable, expression
    value: Any
    data_type: Optional[str] = None  # string, number, boolean, date, array, object

    @classmethod
    def literal(cls, value: Any) -> "ExpressionValue":
        """Create a literal value."""
        data_type = cls._infer_type(value)
        return cls(type="literal", value=value, data_type=data_type)

    @classmethod
    def field(cls, path: str, data_type: Optional[str] = None) -> "ExpressionValue":
        """Create a field reference."""
        return cls(type="field", value=path, data_type=data_type)

    @classmethod
    def variable(cls, name: str, data_type: Optional[str] = None) -> "ExpressionValue":
        """Create a variable reference."""
        return cls(type="variable", value=name, data_type=data_type)

    @staticmethod
    def _infer_type(value: Any) -> str:
        """Infer data type from value."""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, (int, float, Decimal)):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, (datetime, date)):
            return "date"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "unknown"


# =============================================================================
# Expression Definition
# =============================================================================


@dataclass
class Expression:
    """An expression node in the expression tree."""

    type: ExpressionType
    operands: List[Union["Expression", ExpressionValue]] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Metadata for visual builder
    id: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "type": self.type.value,
            "operands": [
                op.to_dict() if isinstance(op, Expression)
                else {"type": op.type, "value": op.value, "dataType": op.data_type}
                for op in self.operands
            ],
            "parameters": self.parameters,
            "id": self.id,
            "label": self.label,
            "description": self.description
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Expression":
        """Create from dictionary."""
        operands = []
        for op in data.get("operands", []):
            if "type" in op and op["type"] in [e.value for e in ExpressionType]:
                operands.append(cls.from_dict(op))
            else:
                operands.append(ExpressionValue(
                    type=op.get("type", "literal"),
                    value=op.get("value"),
                    data_type=op.get("dataType")
                ))

        return cls(
            type=ExpressionType(data["type"]),
            operands=operands,
            parameters=data.get("parameters", {}),
            id=data.get("id"),
            label=data.get("label"),
            description=data.get("description")
        )

    def to_string(self) -> str:
        """Convert to human-readable string."""
        return ExpressionStringifier().stringify(self)


# =============================================================================
# Expression Context
# =============================================================================


@dataclass
class ExpressionContext:
    """Context for expression evaluation."""

    # Data sources
    message: Dict[str, Any] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)

    # Environment
    tenant_id: str = ""
    flow_id: str = ""
    execution_id: str = ""
    timestamp: Optional[datetime] = None

    # Lookup function for external data
    lookup_fn: Optional[Callable[[str, str], Any]] = None

    def get_field(self, path: str, default: Any = None) -> Any:
        """Get a field value by path."""
        parts = path.split(".")
        current = self.message

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

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get a variable value."""
        return self.variables.get(name, default)


@dataclass
class ExpressionResult:
    """Result of expression evaluation."""

    success: bool
    value: Any = None
    error: Optional[str] = None
    type: Optional[str] = None

    @classmethod
    def ok(cls, value: Any) -> "ExpressionResult":
        """Create a successful result."""
        return cls(success=True, value=value, type=ExpressionValue._infer_type(value))

    @classmethod
    def fail(cls, error: str) -> "ExpressionResult":
        """Create a failed result."""
        return cls(success=False, error=error)


# =============================================================================
# Expression Functions
# =============================================================================


@dataclass
class ExpressionFunction:
    """Definition of an expression function."""

    name: str
    description: str
    category: str
    parameters: List[Dict[str, Any]]
    return_type: str
    handler: Callable[..., Any]
    examples: List[str] = field(default_factory=list)

    def call(self, *args, **kwargs) -> Any:
        """Call the function."""
        return self.handler(*args, **kwargs)


class FunctionRegistry:
    """Registry of expression functions."""

    def __init__(self):
        """Initialize with built-in functions."""
        self._functions: Dict[str, ExpressionFunction] = {}
        self._register_builtins()

    def register(self, func: ExpressionFunction) -> None:
        """Register a function."""
        self._functions[func.name] = func

    def get(self, name: str) -> Optional[ExpressionFunction]:
        """Get a function by name."""
        return self._functions.get(name)

    def list_functions(self, category: Optional[str] = None) -> List[ExpressionFunction]:
        """List all functions, optionally filtered by category."""
        funcs = list(self._functions.values())
        if category:
            funcs = [f for f in funcs if f.category == category]
        return sorted(funcs, key=lambda f: f.name)

    def _register_builtins(self) -> None:
        """Register built-in functions."""
        # String functions
        self.register(ExpressionFunction(
            name="CONCAT",
            description="Concatenate strings",
            category="string",
            parameters=[{"name": "values", "type": "string[]", "required": True}],
            return_type="string",
            handler=lambda *args: "".join(str(a) for a in args if a is not None),
            examples=["CONCAT('Hello', ' ', 'World') => 'Hello World'"]
        ))

        self.register(ExpressionFunction(
            name="UPPER",
            description="Convert to uppercase",
            category="string",
            parameters=[{"name": "text", "type": "string", "required": True}],
            return_type="string",
            handler=lambda x: str(x).upper() if x else ""
        ))

        self.register(ExpressionFunction(
            name="LOWER",
            description="Convert to lowercase",
            category="string",
            parameters=[{"name": "text", "type": "string", "required": True}],
            return_type="string",
            handler=lambda x: str(x).lower() if x else ""
        ))

        self.register(ExpressionFunction(
            name="TRIM",
            description="Remove leading/trailing whitespace",
            category="string",
            parameters=[{"name": "text", "type": "string", "required": True}],
            return_type="string",
            handler=lambda x: str(x).strip() if x else ""
        ))

        self.register(ExpressionFunction(
            name="LENGTH",
            description="Get string length",
            category="string",
            parameters=[{"name": "text", "type": "string", "required": True}],
            return_type="number",
            handler=lambda x: len(str(x)) if x else 0
        ))

        self.register(ExpressionFunction(
            name="SUBSTRING",
            description="Extract substring",
            category="string",
            parameters=[
                {"name": "text", "type": "string", "required": True},
                {"name": "start", "type": "number", "required": True},
                {"name": "length", "type": "number", "required": False}
            ],
            return_type="string",
            handler=lambda text, start, length=None: str(text)[start:start + length] if length else str(text)[start:]
        ))

        self.register(ExpressionFunction(
            name="REPLACE",
            description="Replace text",
            category="string",
            parameters=[
                {"name": "text", "type": "string", "required": True},
                {"name": "search", "type": "string", "required": True},
                {"name": "replace", "type": "string", "required": True}
            ],
            return_type="string",
            handler=lambda text, search, replace: str(text).replace(search, replace) if text else ""
        ))

        # Math functions
        self.register(ExpressionFunction(
            name="ABS",
            description="Absolute value",
            category="math",
            parameters=[{"name": "number", "type": "number", "required": True}],
            return_type="number",
            handler=lambda x: abs(float(x)) if x is not None else None
        ))

        self.register(ExpressionFunction(
            name="ROUND",
            description="Round to decimals",
            category="math",
            parameters=[
                {"name": "number", "type": "number", "required": True},
                {"name": "decimals", "type": "number", "required": False}
            ],
            return_type="number",
            handler=lambda x, d=0: round(float(x), int(d)) if x is not None else None
        ))

        self.register(ExpressionFunction(
            name="FLOOR",
            description="Round down",
            category="math",
            parameters=[{"name": "number", "type": "number", "required": True}],
            return_type="number",
            handler=lambda x: int(float(x)) if x is not None else None
        ))

        self.register(ExpressionFunction(
            name="CEIL",
            description="Round up",
            category="math",
            parameters=[{"name": "number", "type": "number", "required": True}],
            return_type="number",
            handler=lambda x: int(float(x)) + (1 if float(x) % 1 else 0) if x is not None else None
        ))

        self.register(ExpressionFunction(
            name="MIN",
            description="Minimum value",
            category="math",
            parameters=[{"name": "values", "type": "number[]", "required": True}],
            return_type="number",
            handler=lambda *args: min(args) if args else None
        ))

        self.register(ExpressionFunction(
            name="MAX",
            description="Maximum value",
            category="math",
            parameters=[{"name": "values", "type": "number[]", "required": True}],
            return_type="number",
            handler=lambda *args: max(args) if args else None
        ))

        self.register(ExpressionFunction(
            name="SUM",
            description="Sum of values",
            category="math",
            parameters=[{"name": "values", "type": "number[]", "required": True}],
            return_type="number",
            handler=lambda *args: sum(args) if args else 0
        ))

        self.register(ExpressionFunction(
            name="AVG",
            description="Average of values",
            category="math",
            parameters=[{"name": "values", "type": "number[]", "required": True}],
            return_type="number",
            handler=lambda *args: sum(args) / len(args) if args else None
        ))

        # Date functions
        self.register(ExpressionFunction(
            name="NOW",
            description="Current timestamp",
            category="date",
            parameters=[],
            return_type="date",
            handler=lambda: datetime.utcnow()
        ))

        self.register(ExpressionFunction(
            name="TODAY",
            description="Current date",
            category="date",
            parameters=[],
            return_type="date",
            handler=lambda: date.today()
        ))

        self.register(ExpressionFunction(
            name="DATE_ADD",
            description="Add time to date",
            category="date",
            parameters=[
                {"name": "date", "type": "date", "required": True},
                {"name": "amount", "type": "number", "required": True},
                {"name": "unit", "type": "string", "required": True}
            ],
            return_type="date",
            handler=self._date_add
        ))

        self.register(ExpressionFunction(
            name="DATE_DIFF",
            description="Difference between dates",
            category="date",
            parameters=[
                {"name": "date1", "type": "date", "required": True},
                {"name": "date2", "type": "date", "required": True},
                {"name": "unit", "type": "string", "required": False}
            ],
            return_type="number",
            handler=self._date_diff
        ))

        # Logic functions
        self.register(ExpressionFunction(
            name="IF",
            description="Conditional expression",
            category="logic",
            parameters=[
                {"name": "condition", "type": "boolean", "required": True},
                {"name": "then", "type": "any", "required": True},
                {"name": "else", "type": "any", "required": True}
            ],
            return_type="any",
            handler=lambda cond, then_val, else_val: then_val if cond else else_val
        ))

        self.register(ExpressionFunction(
            name="COALESCE",
            description="First non-null value",
            category="logic",
            parameters=[{"name": "values", "type": "any[]", "required": True}],
            return_type="any",
            handler=lambda *args: next((a for a in args if a is not None), None)
        ))

        self.register(ExpressionFunction(
            name="ISNULL",
            description="Check if null",
            category="logic",
            parameters=[{"name": "value", "type": "any", "required": True}],
            return_type="boolean",
            handler=lambda x: x is None
        ))

        self.register(ExpressionFunction(
            name="ISEMPTY",
            description="Check if empty",
            category="logic",
            parameters=[{"name": "value", "type": "any", "required": True}],
            return_type="boolean",
            handler=lambda x: x is None or x == "" or (isinstance(x, (list, dict)) and len(x) == 0)
        ))

        # Collection functions
        self.register(ExpressionFunction(
            name="COUNT",
            description="Count items",
            category="collection",
            parameters=[{"name": "array", "type": "array", "required": True}],
            return_type="number",
            handler=lambda x: len(x) if isinstance(x, list) else 0
        ))

        self.register(ExpressionFunction(
            name="FIRST",
            description="First item",
            category="collection",
            parameters=[{"name": "array", "type": "array", "required": True}],
            return_type="any",
            handler=lambda x: x[0] if isinstance(x, list) and x else None
        ))

        self.register(ExpressionFunction(
            name="LAST",
            description="Last item",
            category="collection",
            parameters=[{"name": "array", "type": "array", "required": True}],
            return_type="any",
            handler=lambda x: x[-1] if isinstance(x, list) and x else None
        ))

        self.register(ExpressionFunction(
            name="CONTAINS",
            description="Check if contains value",
            category="collection",
            parameters=[
                {"name": "array", "type": "array", "required": True},
                {"name": "value", "type": "any", "required": True}
            ],
            return_type="boolean",
            handler=lambda arr, val: val in arr if isinstance(arr, list) else False
        ))

    @staticmethod
    def _date_add(dt: datetime, amount: int, unit: str) -> datetime:
        """Add time to date."""
        if not isinstance(dt, datetime):
            return dt

        unit = unit.lower()
        if unit in ("day", "days"):
            return dt + timedelta(days=amount)
        elif unit in ("hour", "hours"):
            return dt + timedelta(hours=amount)
        elif unit in ("minute", "minutes"):
            return dt + timedelta(minutes=amount)
        elif unit in ("second", "seconds"):
            return dt + timedelta(seconds=amount)
        elif unit in ("week", "weeks"):
            return dt + timedelta(weeks=amount)
        return dt

    @staticmethod
    def _date_diff(dt1: datetime, dt2: datetime, unit: str = "days") -> float:
        """Calculate date difference."""
        if not isinstance(dt1, datetime) or not isinstance(dt2, datetime):
            return 0

        diff = dt1 - dt2
        unit = unit.lower()

        if unit in ("day", "days"):
            return diff.days
        elif unit in ("hour", "hours"):
            return diff.total_seconds() / 3600
        elif unit in ("minute", "minutes"):
            return diff.total_seconds() / 60
        elif unit in ("second", "seconds"):
            return diff.total_seconds()
        return diff.days


# Global function registry
_function_registry = FunctionRegistry()


def get_function_registry() -> FunctionRegistry:
    """Get the global function registry."""
    return _function_registry


# =============================================================================
# Expression Evaluator
# =============================================================================


class ExpressionEvaluator:
    """Evaluates expressions against a context."""

    def __init__(self, functions: Optional[FunctionRegistry] = None):
        """Initialize evaluator."""
        self.functions = functions or get_function_registry()

    def evaluate(
        self,
        expression: Expression,
        context: ExpressionContext
    ) -> ExpressionResult:
        """
        Evaluate an expression.

        Args:
            expression: The expression to evaluate
            context: Evaluation context

        Returns:
            Evaluation result
        """
        try:
            value = self._eval(expression, context)
            return ExpressionResult.ok(value)
        except Exception as e:
            return ExpressionResult.fail(str(e))

    def _eval(self, expr: Union[Expression, ExpressionValue], context: ExpressionContext) -> Any:
        """Internal evaluation."""
        if isinstance(expr, ExpressionValue):
            return self._eval_value(expr, context)

        t = expr.type
        ops = [self._eval(op, context) for op in expr.operands]

        # Comparisons
        if t == ExpressionType.EQUALS:
            return ops[0] == ops[1] if len(ops) >= 2 else False
        elif t == ExpressionType.NOT_EQUALS:
            return ops[0] != ops[1] if len(ops) >= 2 else True
        elif t == ExpressionType.GREATER_THAN:
            return ops[0] > ops[1] if len(ops) >= 2 else False
        elif t == ExpressionType.GREATER_EQUAL:
            return ops[0] >= ops[1] if len(ops) >= 2 else False
        elif t == ExpressionType.LESS_THAN:
            return ops[0] < ops[1] if len(ops) >= 2 else False
        elif t == ExpressionType.LESS_EQUAL:
            return ops[0] <= ops[1] if len(ops) >= 2 else False
        elif t == ExpressionType.CONTAINS:
            return ops[1] in ops[0] if len(ops) >= 2 and ops[0] else False
        elif t == ExpressionType.NOT_CONTAINS:
            return ops[1] not in ops[0] if len(ops) >= 2 and ops[0] else True
        elif t == ExpressionType.STARTS_WITH:
            return str(ops[0]).startswith(str(ops[1])) if len(ops) >= 2 and ops[0] else False
        elif t == ExpressionType.ENDS_WITH:
            return str(ops[0]).endswith(str(ops[1])) if len(ops) >= 2 and ops[0] else False
        elif t == ExpressionType.MATCHES:
            return bool(re.match(str(ops[1]), str(ops[0]))) if len(ops) >= 2 and ops[0] else False
        elif t == ExpressionType.IN:
            return ops[0] in ops[1] if len(ops) >= 2 else False
        elif t == ExpressionType.NOT_IN:
            return ops[0] not in ops[1] if len(ops) >= 2 else True
        elif t == ExpressionType.IS_NULL:
            return ops[0] is None if ops else True
        elif t == ExpressionType.IS_NOT_NULL:
            return ops[0] is not None if ops else False
        elif t == ExpressionType.IS_EMPTY:
            return not ops[0] or (isinstance(ops[0], (list, dict, str)) and len(ops[0]) == 0)
        elif t == ExpressionType.IS_NOT_EMPTY:
            return ops[0] and (not isinstance(ops[0], (list, dict, str)) or len(ops[0]) > 0)
        elif t == ExpressionType.BETWEEN:
            if len(ops) >= 3:
                return ops[1] <= ops[0] <= ops[2]
            return False

        # Logic
        elif t == ExpressionType.AND:
            return all(ops)
        elif t == ExpressionType.OR:
            return any(ops)
        elif t == ExpressionType.NOT:
            return not ops[0] if ops else True
        elif t == ExpressionType.IF:
            if len(ops) >= 3:
                return ops[1] if ops[0] else ops[2]
            return None

        # Math
        elif t == ExpressionType.ADD:
            return sum(ops) if ops else 0
        elif t == ExpressionType.SUBTRACT:
            return ops[0] - sum(ops[1:]) if len(ops) >= 2 else (ops[0] if ops else 0)
        elif t == ExpressionType.MULTIPLY:
            result = 1
            for op in ops:
                result *= op
            return result
        elif t == ExpressionType.DIVIDE:
            if len(ops) >= 2 and ops[1] != 0:
                return ops[0] / ops[1]
            return None
        elif t == ExpressionType.MODULO:
            if len(ops) >= 2 and ops[1] != 0:
                return ops[0] % ops[1]
            return None
        elif t == ExpressionType.POWER:
            return ops[0] ** ops[1] if len(ops) >= 2 else (ops[0] if ops else 0)
        elif t == ExpressionType.NEGATE:
            return -ops[0] if ops else 0
        elif t == ExpressionType.ABS:
            return abs(ops[0]) if ops else 0
        elif t == ExpressionType.ROUND:
            decimals = expr.parameters.get("decimals", 0)
            return round(ops[0], decimals) if ops else 0
        elif t == ExpressionType.FLOOR:
            return int(ops[0]) if ops else 0
        elif t == ExpressionType.CEIL:
            return int(ops[0]) + (1 if ops[0] % 1 else 0) if ops else 0
        elif t == ExpressionType.MIN:
            return min(ops) if ops else None
        elif t == ExpressionType.MAX:
            return max(ops) if ops else None

        # String
        elif t == ExpressionType.CONCAT:
            separator = expr.parameters.get("separator", "")
            return separator.join(str(op) for op in ops if op is not None)
        elif t == ExpressionType.UPPER:
            return str(ops[0]).upper() if ops else ""
        elif t == ExpressionType.LOWER:
            return str(ops[0]).lower() if ops else ""
        elif t == ExpressionType.TRIM:
            return str(ops[0]).strip() if ops else ""
        elif t == ExpressionType.SUBSTRING:
            start = expr.parameters.get("start", 0)
            length = expr.parameters.get("length")
            text = str(ops[0]) if ops else ""
            return text[start:start + length] if length else text[start:]
        elif t == ExpressionType.LENGTH:
            return len(str(ops[0])) if ops else 0
        elif t == ExpressionType.REPLACE:
            if len(ops) >= 3:
                return str(ops[0]).replace(str(ops[1]), str(ops[2]))
            return ops[0] if ops else ""

        # Date
        elif t == ExpressionType.NOW:
            return datetime.utcnow()
        elif t == ExpressionType.TODAY:
            return date.today()
        elif t == ExpressionType.DATE_ADD:
            if ops and isinstance(ops[0], datetime):
                amount = expr.parameters.get("amount", 0)
                unit = expr.parameters.get("unit", "days")
                return FunctionRegistry._date_add(ops[0], amount, unit)
            return ops[0] if ops else None
        elif t == ExpressionType.DATE_DIFF:
            if len(ops) >= 2:
                unit = expr.parameters.get("unit", "days")
                return FunctionRegistry._date_diff(ops[0], ops[1], unit)
            return 0

        # Collection
        elif t == ExpressionType.COUNT:
            return len(ops[0]) if ops and isinstance(ops[0], list) else 0
        elif t == ExpressionType.SUM:
            arr = ops[0] if ops and isinstance(ops[0], list) else ops
            return sum(arr)
        elif t == ExpressionType.AVERAGE:
            arr = ops[0] if ops and isinstance(ops[0], list) else ops
            return sum(arr) / len(arr) if arr else 0
        elif t == ExpressionType.FIRST:
            return ops[0][0] if ops and isinstance(ops[0], list) and ops[0] else None
        elif t == ExpressionType.LAST:
            return ops[0][-1] if ops and isinstance(ops[0], list) and ops[0] else None

        # Special
        elif t == ExpressionType.COALESCE:
            return next((op for op in ops if op is not None), None)
        elif t == ExpressionType.SWITCH:
            cases = expr.parameters.get("cases", {})
            default = expr.parameters.get("default")
            value = ops[0] if ops else None
            return cases.get(value, default)

        # Function call
        elif t == ExpressionType.FUNCTION:
            func_name = expr.parameters.get("name", "")
            func = self.functions.get(func_name)
            if func:
                return func.call(*ops)
            return None

        # Field/Variable access
        elif t == ExpressionType.FIELD:
            path = expr.parameters.get("path", "")
            return context.get_field(path)
        elif t == ExpressionType.VARIABLE:
            name = expr.parameters.get("name", "")
            return context.get_variable(name)
        elif t == ExpressionType.LITERAL:
            return expr.parameters.get("value")

        return None

    def _eval_value(self, value: ExpressionValue, context: ExpressionContext) -> Any:
        """Evaluate an expression value."""
        if value.type == "literal":
            return value.value
        elif value.type == "field":
            return context.get_field(value.value)
        elif value.type == "variable":
            return context.get_variable(value.value)
        return value.value


# =============================================================================
# Expression Parser
# =============================================================================


class ExpressionParser:
    """
    Parses expression strings into Expression objects.

    Supports a simple expression language (Jinja2-style):
    - Field access: {{ field.path }}
    - Variables: @variable or {{ vars.name }}
    - Literals: "string", 123, true, false, null
    - Operators: ==, !=, >, <, >=, <=, &&, ||, !, +, -, *, /
    - Functions: FUNCTION(arg1, arg2)
    - Filters: {{ value | upper }}, {{ value | default("N/A") }}
    """

    def __init__(self):
        """Initialize parser."""
        self.pos = 0
        self.text = ""
        self.length = 0

    def parse(self, text: str) -> Expression:
        """
        Parse expression string.

        Args:
            text: Expression string

        Returns:
            Parsed Expression
        """
        self.text = text.strip()
        self.length = len(self.text)
        self.pos = 0

        return self._parse_or()

    def _parse_or(self) -> Expression:
        """Parse OR expressions."""
        left = self._parse_and()

        while self._match("||") or self._match_keyword("OR"):
            right = self._parse_and()
            left = Expression(
                type=ExpressionType.OR,
                operands=[left, right]
            )

        return left

    def _parse_and(self) -> Expression:
        """Parse AND expressions."""
        left = self._parse_not()

        while self._match("&&") or self._match_keyword("AND"):
            right = self._parse_not()
            left = Expression(
                type=ExpressionType.AND,
                operands=[left, right]
            )

        return left

    def _parse_not(self) -> Expression:
        """Parse NOT expressions."""
        if self._match("!") or self._match_keyword("NOT"):
            operand = self._parse_not()
            return Expression(
                type=ExpressionType.NOT,
                operands=[operand]
            )
        return self._parse_comparison()

    def _parse_comparison(self) -> Expression:
        """Parse comparison expressions."""
        left = self._parse_additive()

        ops = {
            "==": ExpressionType.EQUALS,
            "!=": ExpressionType.NOT_EQUALS,
            ">=": ExpressionType.GREATER_EQUAL,
            "<=": ExpressionType.LESS_EQUAL,
            ">": ExpressionType.GREATER_THAN,
            "<": ExpressionType.LESS_THAN,
        }

        for op, expr_type in ops.items():
            if self._match(op):
                right = self._parse_additive()
                return Expression(
                    type=expr_type,
                    operands=[left, right]
                )

        # Keyword comparisons
        if self._match_keyword("CONTAINS"):
            right = self._parse_additive()
            return Expression(type=ExpressionType.CONTAINS, operands=[left, right])
        elif self._match_keyword("STARTS WITH"):
            right = self._parse_additive()
            return Expression(type=ExpressionType.STARTS_WITH, operands=[left, right])
        elif self._match_keyword("ENDS WITH"):
            right = self._parse_additive()
            return Expression(type=ExpressionType.ENDS_WITH, operands=[left, right])
        elif self._match_keyword("IN"):
            right = self._parse_additive()
            return Expression(type=ExpressionType.IN, operands=[left, right])
        elif self._match_keyword("IS NULL"):
            return Expression(type=ExpressionType.IS_NULL, operands=[left])
        elif self._match_keyword("IS NOT NULL"):
            return Expression(type=ExpressionType.IS_NOT_NULL, operands=[left])

        return left

    def _parse_additive(self) -> Expression:
        """Parse additive expressions (+, -)."""
        left = self._parse_multiplicative()

        while True:
            if self._match("+"):
                right = self._parse_multiplicative()
                left = Expression(type=ExpressionType.ADD, operands=[left, right])
            elif self._match("-"):
                right = self._parse_multiplicative()
                left = Expression(type=ExpressionType.SUBTRACT, operands=[left, right])
            else:
                break

        return left

    def _parse_multiplicative(self) -> Expression:
        """Parse multiplicative expressions (*, /, %)."""
        left = self._parse_unary()

        while True:
            if self._match("*"):
                right = self._parse_unary()
                left = Expression(type=ExpressionType.MULTIPLY, operands=[left, right])
            elif self._match("/"):
                right = self._parse_unary()
                left = Expression(type=ExpressionType.DIVIDE, operands=[left, right])
            elif self._match("%"):
                right = self._parse_unary()
                left = Expression(type=ExpressionType.MODULO, operands=[left, right])
            else:
                break

        return left

    def _parse_unary(self) -> Expression:
        """Parse unary expressions."""
        if self._match("-"):
            operand = self._parse_unary()
            return Expression(type=ExpressionType.NEGATE, operands=[operand])

        return self._parse_primary()

    def _parse_primary(self) -> Expression:
        """Parse primary expressions."""
        self._skip_whitespace()

        # Parentheses
        if self._match("("):
            expr = self._parse_or()
            self._expect(")")
            return expr

        # Field reference {{ ... }} (Jinja2 syntax)
        if self._match("{{"):
            path = self._read_until("}}")
            self._expect("}}")
            return Expression(
                type=ExpressionType.FIELD,
                parameters={"path": path.strip()}
            )

        # Variable @name
        if self._match("@"):
            name = self._read_identifier()
            return Expression(
                type=ExpressionType.VARIABLE,
                parameters={"name": name}
            )

        # String literal
        if self._peek() == '"' or self._peek() == "'":
            quote = self._advance()
            value = self._read_until(quote)
            self._expect(quote)
            return Expression(
                type=ExpressionType.LITERAL,
                operands=[ExpressionValue.literal(value)]
            )

        # Number
        if self._peek() and (self._peek().isdigit() or self._peek() == "."):
            num_str = self._read_number()
            value = float(num_str) if "." in num_str else int(num_str)
            return Expression(
                type=ExpressionType.LITERAL,
                operands=[ExpressionValue.literal(value)]
            )

        # Boolean/null keywords
        if self._match_keyword("true"):
            return Expression(type=ExpressionType.LITERAL, operands=[ExpressionValue.literal(True)])
        if self._match_keyword("false"):
            return Expression(type=ExpressionType.LITERAL, operands=[ExpressionValue.literal(False)])
        if self._match_keyword("null"):
            return Expression(type=ExpressionType.LITERAL, operands=[ExpressionValue.literal(None)])

        # Function call or field
        identifier = self._read_identifier()
        if identifier:
            if self._match("("):
                # Function call
                args = self._parse_arguments()
                self._expect(")")
                return Expression(
                    type=ExpressionType.FUNCTION,
                    operands=args,
                    parameters={"name": identifier}
                )
            else:
                # Treat as field reference
                return Expression(
                    type=ExpressionType.FIELD,
                    parameters={"path": identifier}
                )

        raise ValueError(f"Unexpected character at position {self.pos}: {self._peek()}")

    def _parse_arguments(self) -> List[Expression]:
        """Parse function arguments."""
        args = []
        self._skip_whitespace()

        if self._peek() != ")":
            args.append(self._parse_or())

            while self._match(","):
                args.append(self._parse_or())

        return args

    def _skip_whitespace(self) -> None:
        """Skip whitespace."""
        while self.pos < self.length and self.text[self.pos].isspace():
            self.pos += 1

    def _peek(self) -> Optional[str]:
        """Peek at current character."""
        self._skip_whitespace()
        return self.text[self.pos] if self.pos < self.length else None

    def _advance(self) -> str:
        """Advance and return current character."""
        self._skip_whitespace()
        char = self.text[self.pos]
        self.pos += 1
        return char

    def _match(self, expected: str) -> bool:
        """Try to match a string."""
        self._skip_whitespace()
        if self.text[self.pos:self.pos + len(expected)] == expected:
            self.pos += len(expected)
            return True
        return False

    def _match_keyword(self, keyword: str) -> bool:
        """Try to match a keyword (case-insensitive)."""
        self._skip_whitespace()
        end = self.pos + len(keyword)
        if self.text[self.pos:end].upper() == keyword.upper():
            # Make sure it's not part of a longer identifier
            if end >= self.length or not self.text[end].isalnum():
                self.pos = end
                return True
        return False

    def _expect(self, expected: str) -> None:
        """Expect a specific string."""
        if not self._match(expected):
            raise ValueError(f"Expected '{expected}' at position {self.pos}")

    def _read_until(self, delimiter: str) -> str:
        """Read until a delimiter (supports multi-character delimiters like '}}')."""
        start = self.pos
        delimiter_len = len(delimiter)
        while self.pos < self.length:
            if self.text[self.pos:self.pos + delimiter_len] == delimiter:
                break
            self.pos += 1
        return self.text[start:self.pos]

    def _read_identifier(self) -> str:
        """Read an identifier."""
        self._skip_whitespace()
        start = self.pos
        while self.pos < self.length and (self.text[self.pos].isalnum() or self.text[self.pos] in "_$."):
            self.pos += 1
        return self.text[start:self.pos]

    def _read_number(self) -> str:
        """Read a number."""
        start = self.pos
        has_dot = False
        while self.pos < self.length:
            char = self.text[self.pos]
            if char.isdigit():
                self.pos += 1
            elif char == "." and not has_dot:
                has_dot = True
                self.pos += 1
            else:
                break
        return self.text[start:self.pos]


# =============================================================================
# Expression Builder (Visual)
# =============================================================================


class ExpressionBuilder:
    """
    Builder for constructing expressions programmatically.

    Designed to be used by visual expression builder UI.
    """

    def __init__(self):
        """Initialize builder."""
        self._expr_stack: List[Expression] = []

    # Value builders
    def literal(self, value: Any) -> "ExpressionBuilder":
        """Add a literal value."""
        self._expr_stack.append(Expression(
            type=ExpressionType.LITERAL,
            operands=[ExpressionValue.literal(value)]
        ))
        return self

    def field(self, path: str) -> "ExpressionBuilder":
        """Add a field reference."""
        self._expr_stack.append(Expression(
            type=ExpressionType.FIELD,
            parameters={"path": path}
        ))
        return self

    def variable(self, name: str) -> "ExpressionBuilder":
        """Add a variable reference."""
        self._expr_stack.append(Expression(
            type=ExpressionType.VARIABLE,
            parameters={"name": name}
        ))
        return self

    # Comparison builders
    def equals(self) -> "ExpressionBuilder":
        """Create equals expression from last two values."""
        return self._binary_op(ExpressionType.EQUALS)

    def not_equals(self) -> "ExpressionBuilder":
        """Create not equals expression."""
        return self._binary_op(ExpressionType.NOT_EQUALS)

    def greater_than(self) -> "ExpressionBuilder":
        """Create greater than expression."""
        return self._binary_op(ExpressionType.GREATER_THAN)

    def less_than(self) -> "ExpressionBuilder":
        """Create less than expression."""
        return self._binary_op(ExpressionType.LESS_THAN)

    def contains(self) -> "ExpressionBuilder":
        """Create contains expression."""
        return self._binary_op(ExpressionType.CONTAINS)

    def is_null(self) -> "ExpressionBuilder":
        """Create is null expression."""
        return self._unary_op(ExpressionType.IS_NULL)

    def is_not_null(self) -> "ExpressionBuilder":
        """Create is not null expression."""
        return self._unary_op(ExpressionType.IS_NOT_NULL)

    # Logic builders
    def and_(self) -> "ExpressionBuilder":
        """Create AND expression."""
        return self._binary_op(ExpressionType.AND)

    def or_(self) -> "ExpressionBuilder":
        """Create OR expression."""
        return self._binary_op(ExpressionType.OR)

    def not_(self) -> "ExpressionBuilder":
        """Create NOT expression."""
        return self._unary_op(ExpressionType.NOT)

    # Math builders
    def add(self) -> "ExpressionBuilder":
        """Create add expression."""
        return self._binary_op(ExpressionType.ADD)

    def subtract(self) -> "ExpressionBuilder":
        """Create subtract expression."""
        return self._binary_op(ExpressionType.SUBTRACT)

    def multiply(self) -> "ExpressionBuilder":
        """Create multiply expression."""
        return self._binary_op(ExpressionType.MULTIPLY)

    def divide(self) -> "ExpressionBuilder":
        """Create divide expression."""
        return self._binary_op(ExpressionType.DIVIDE)

    # Function builder
    def function(self, name: str, arg_count: int) -> "ExpressionBuilder":
        """Create function call expression."""
        if len(self._expr_stack) < arg_count:
            raise ValueError(f"Not enough operands for function {name}")

        args = self._expr_stack[-arg_count:]
        self._expr_stack = self._expr_stack[:-arg_count]

        self._expr_stack.append(Expression(
            type=ExpressionType.FUNCTION,
            operands=args,
            parameters={"name": name}
        ))
        return self

    # Build
    def build(self) -> Expression:
        """Build the final expression."""
        if not self._expr_stack:
            raise ValueError("No expression built")
        if len(self._expr_stack) > 1:
            raise ValueError("Incomplete expression - multiple values on stack")
        return self._expr_stack[0]

    def _binary_op(self, op_type: ExpressionType) -> "ExpressionBuilder":
        """Create binary operation."""
        if len(self._expr_stack) < 2:
            raise ValueError("Need two operands for binary operation")
        right = self._expr_stack.pop()
        left = self._expr_stack.pop()
        self._expr_stack.append(Expression(type=op_type, operands=[left, right]))
        return self

    def _unary_op(self, op_type: ExpressionType) -> "ExpressionBuilder":
        """Create unary operation."""
        if not self._expr_stack:
            raise ValueError("Need one operand for unary operation")
        operand = self._expr_stack.pop()
        self._expr_stack.append(Expression(type=op_type, operands=[operand]))
        return self


# =============================================================================
# Expression Stringifier
# =============================================================================


class ExpressionStringifier:
    """Converts expressions to human-readable strings."""

    def stringify(self, expr: Expression) -> str:
        """Convert expression to string."""
        t = expr.type

        # Values
        if t == ExpressionType.LITERAL:
            val = expr.operands[0].value if expr.operands else expr.parameters.get("value")
            if isinstance(val, str):
                return f'"{val}"'
            return str(val)
        elif t == ExpressionType.FIELD:
            return f"{{{{ {expr.parameters.get('path', '')} }}}}"
        elif t == ExpressionType.VARIABLE:
            return f"{{{{ vars.{expr.parameters.get('name', '')} }}}}"

        # Get operand strings
        ops = [self.stringify(op) if isinstance(op, Expression) else str(op.value) for op in expr.operands]

        # Comparisons
        op_map = {
            ExpressionType.EQUALS: "==",
            ExpressionType.NOT_EQUALS: "!=",
            ExpressionType.GREATER_THAN: ">",
            ExpressionType.GREATER_EQUAL: ">=",
            ExpressionType.LESS_THAN: "<",
            ExpressionType.LESS_EQUAL: "<=",
            ExpressionType.AND: " AND ",
            ExpressionType.OR: " OR ",
            ExpressionType.ADD: " + ",
            ExpressionType.SUBTRACT: " - ",
            ExpressionType.MULTIPLY: " * ",
            ExpressionType.DIVIDE: " / ",
        }

        if t in op_map:
            return f"({ops[0]} {op_map[t].strip()} {ops[1]})" if len(ops) >= 2 else ""

        if t == ExpressionType.NOT:
            return f"NOT {ops[0]}" if ops else "NOT"
        if t == ExpressionType.IS_NULL:
            return f"{ops[0]} IS NULL" if ops else "IS NULL"
        if t == ExpressionType.IS_NOT_NULL:
            return f"{ops[0]} IS NOT NULL" if ops else "IS NOT NULL"
        if t == ExpressionType.CONTAINS:
            return f"{ops[0]} CONTAINS {ops[1]}" if len(ops) >= 2 else ""
        if t == ExpressionType.FUNCTION:
            func_name = expr.parameters.get("name", "FUNC")
            return f"{func_name}({', '.join(ops)})"

        return str(expr.type.value)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Expression types
    "Expression",
    "ExpressionType",
    "ExpressionOperator",
    "ExpressionValue",
    # Builder
    "ExpressionBuilder",
    "ExpressionParser",
    "ExpressionEvaluator",
    # Context
    "ExpressionContext",
    "ExpressionResult",
    # Functions
    "ExpressionFunction",
    "FunctionRegistry",
    "get_function_registry",
]

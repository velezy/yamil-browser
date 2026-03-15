"""
Business Rules Engine for Logic Weaver.

Provides rule-based decision making:
- Rule conditions with operators
- Rule actions (set value, route, validate, etc.)
- Rule sets with priorities
- Decision tables

This enables business users to define complex business logic
without programming.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import re
import json
from datetime import datetime


# =============================================================================
# Condition Operators
# =============================================================================


class ConditionOperator(Enum):
    """Operators for rule conditions."""
    # Equality
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"

    # Comparison
    GREATER_THAN = "greater_than"
    GREATER_EQUAL = "greater_equal"
    LESS_THAN = "less_than"
    LESS_EQUAL = "less_equal"
    BETWEEN = "between"

    # String
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    MATCHES = "matches"  # Regex

    # Membership
    IN = "in"
    NOT_IN = "not_in"

    # Null checks
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"

    # Type checks
    IS_STRING = "is_string"
    IS_NUMBER = "is_number"
    IS_BOOLEAN = "is_boolean"
    IS_ARRAY = "is_array"
    IS_OBJECT = "is_object"


# =============================================================================
# Rule Conditions
# =============================================================================


@dataclass
class RuleCondition:
    """A condition in a business rule."""

    field: str
    operator: ConditionOperator
    value: Any = None
    second_value: Any = None  # For BETWEEN

    # Nested conditions for AND/OR
    operator_logic: str = "and"  # and, or
    sub_conditions: List["RuleCondition"] = field(default_factory=list)

    def evaluate(self, data: Dict[str, Any]) -> bool:
        """
        Evaluate condition against data.

        Args:
            data: Data to evaluate against

        Returns:
            Whether condition is met
        """
        # Handle sub-conditions
        if self.sub_conditions:
            results = [cond.evaluate(data) for cond in self.sub_conditions]
            if self.operator_logic == "or":
                return any(results)
            return all(results)

        # Get field value
        field_value = self._get_field_value(data, self.field)

        # Evaluate operator
        return self._evaluate_operator(field_value)

    def _get_field_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested field value by path."""
        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if 0 <= idx < len(current) else None
            else:
                return None

            if current is None:
                return None

        return current

    def _evaluate_operator(self, field_value: Any) -> bool:
        """Evaluate the operator."""
        op = self.operator
        val = self.value

        # Null/Empty checks
        if op == ConditionOperator.IS_NULL:
            return field_value is None
        elif op == ConditionOperator.IS_NOT_NULL:
            return field_value is not None
        elif op == ConditionOperator.IS_EMPTY:
            return field_value is None or field_value == "" or (
                isinstance(field_value, (list, dict)) and len(field_value) == 0
            )
        elif op == ConditionOperator.IS_NOT_EMPTY:
            return field_value is not None and field_value != "" and (
                not isinstance(field_value, (list, dict)) or len(field_value) > 0
            )

        # Type checks
        elif op == ConditionOperator.IS_STRING:
            return isinstance(field_value, str)
        elif op == ConditionOperator.IS_NUMBER:
            return isinstance(field_value, (int, float))
        elif op == ConditionOperator.IS_BOOLEAN:
            return isinstance(field_value, bool)
        elif op == ConditionOperator.IS_ARRAY:
            return isinstance(field_value, list)
        elif op == ConditionOperator.IS_OBJECT:
            return isinstance(field_value, dict)

        # Handle None for comparison operators
        if field_value is None:
            return False

        # Equality
        if op == ConditionOperator.EQUALS:
            return field_value == val
        elif op == ConditionOperator.NOT_EQUALS:
            return field_value != val

        # Comparison
        elif op == ConditionOperator.GREATER_THAN:
            return field_value > val
        elif op == ConditionOperator.GREATER_EQUAL:
            return field_value >= val
        elif op == ConditionOperator.LESS_THAN:
            return field_value < val
        elif op == ConditionOperator.LESS_EQUAL:
            return field_value <= val
        elif op == ConditionOperator.BETWEEN:
            return val <= field_value <= self.second_value

        # String operations
        elif op == ConditionOperator.CONTAINS:
            return str(val) in str(field_value)
        elif op == ConditionOperator.NOT_CONTAINS:
            return str(val) not in str(field_value)
        elif op == ConditionOperator.STARTS_WITH:
            return str(field_value).startswith(str(val))
        elif op == ConditionOperator.ENDS_WITH:
            return str(field_value).endswith(str(val))
        elif op == ConditionOperator.MATCHES:
            return bool(re.match(str(val), str(field_value)))

        # Membership
        elif op == ConditionOperator.IN:
            if isinstance(val, (list, set, tuple)):
                return field_value in val
            return field_value in str(val).split(",")
        elif op == ConditionOperator.NOT_IN:
            if isinstance(val, (list, set, tuple)):
                return field_value not in val
            return field_value not in str(val).split(",")

        return False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field": self.field,
            "operator": self.operator.value,
            "value": self.value,
            "secondValue": self.second_value,
            "logic": self.operator_logic,
            "subConditions": [c.to_dict() for c in self.sub_conditions]
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleCondition":
        """Create from dictionary."""
        return cls(
            field=data.get("field", ""),
            operator=ConditionOperator(data.get("operator", "equals")),
            value=data.get("value"),
            second_value=data.get("secondValue"),
            operator_logic=data.get("logic", "and"),
            sub_conditions=[
                cls.from_dict(c) for c in data.get("subConditions", [])
            ]
        )


# =============================================================================
# Rule Actions
# =============================================================================


class ActionType(Enum):
    """Types of rule actions."""
    # Data modification
    SET_VALUE = "set_value"
    COPY_VALUE = "copy_value"
    DELETE_FIELD = "delete_field"
    RENAME_FIELD = "rename_field"
    APPEND_VALUE = "append_value"
    INCREMENT = "increment"
    DECREMENT = "decrement"

    # Flow control
    ROUTE = "route"
    STOP = "stop"
    CONTINUE = "continue"
    SKIP = "skip"

    # Validation
    VALIDATE = "validate"
    REQUIRE = "require"
    REJECT = "reject"

    # Lookup
    LOOKUP = "lookup"
    ENRICH = "enrich"

    # Output
    EMIT = "emit"
    EMIT_ERROR = "emit_error"
    LOG = "log"

    # Custom
    EXECUTE = "execute"  # Execute custom code


@dataclass
class RuleAction:
    """An action to perform when a rule matches."""

    type: ActionType
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None

    def execute(
        self,
        data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Execute the action.

        Args:
            data: Data to act on
            context: Execution context

        Returns:
            (success, modified_data or error)
        """
        try:
            result = self._execute_action(data, context)
            return True, result
        except Exception as e:
            return False, {"error": str(e)}

    def _execute_action(
        self,
        data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Internal action execution."""
        t = self.type
        params = self.parameters

        if t == ActionType.SET_VALUE:
            field = params.get("field", "")
            value = params.get("value")
            self._set_field(data, field, value)
            return data

        elif t == ActionType.COPY_VALUE:
            source = params.get("source", "")
            target = params.get("target", "")
            value = self._get_field(data, source)
            self._set_field(data, target, value)
            return data

        elif t == ActionType.DELETE_FIELD:
            field = params.get("field", "")
            self._delete_field(data, field)
            return data

        elif t == ActionType.RENAME_FIELD:
            old_name = params.get("oldName", "")
            new_name = params.get("newName", "")
            value = self._get_field(data, old_name)
            self._delete_field(data, old_name)
            self._set_field(data, new_name, value)
            return data

        elif t == ActionType.APPEND_VALUE:
            field = params.get("field", "")
            value = params.get("value")
            current = self._get_field(data, field) or []
            if isinstance(current, list):
                current.append(value)
                self._set_field(data, field, current)
            return data

        elif t == ActionType.INCREMENT:
            field = params.get("field", "")
            amount = params.get("amount", 1)
            current = self._get_field(data, field) or 0
            self._set_field(data, field, current + amount)
            return data

        elif t == ActionType.DECREMENT:
            field = params.get("field", "")
            amount = params.get("amount", 1)
            current = self._get_field(data, field) or 0
            self._set_field(data, field, current - amount)
            return data

        elif t == ActionType.ROUTE:
            target = params.get("target", "")
            context["route_to"] = target
            return data

        elif t == ActionType.STOP:
            context["stop"] = True
            return data

        elif t == ActionType.SKIP:
            context["skip"] = True
            return data

        elif t == ActionType.VALIDATE:
            field = params.get("field", "")
            pattern = params.get("pattern", "")
            message = params.get("message", f"Validation failed for {field}")
            value = self._get_field(data, field)
            if not re.match(pattern, str(value or "")):
                context.setdefault("validation_errors", []).append(message)
            return data

        elif t == ActionType.REQUIRE:
            field = params.get("field", "")
            message = params.get("message", f"Required field missing: {field}")
            value = self._get_field(data, field)
            if value is None or value == "":
                context.setdefault("validation_errors", []).append(message)
            return data

        elif t == ActionType.REJECT:
            reason = params.get("reason", "Rejected by rule")
            context["rejected"] = True
            context["rejection_reason"] = reason
            return data

        elif t == ActionType.LOOKUP:
            table = params.get("table", "")
            key_field = params.get("keyField", "")
            value_field = params.get("valueField", "")
            target_field = params.get("targetField", "")
            lookup_fn = context.get("lookup_fn")
            if lookup_fn:
                key_value = self._get_field(data, key_field)
                result = lookup_fn(table, key_value, value_field)
                self._set_field(data, target_field, result)
            return data

        elif t == ActionType.EMIT:
            message = params.get("message", data)
            target = params.get("target")
            context.setdefault("emitted", []).append({
                "message": message,
                "target": target
            })
            return data

        elif t == ActionType.EMIT_ERROR:
            error = params.get("error", "Error")
            code = params.get("code", "RULE_ERROR")
            context.setdefault("errors", []).append({
                "error": error,
                "code": code
            })
            return data

        elif t == ActionType.LOG:
            level = params.get("level", "info")
            message = params.get("message", "")
            context.setdefault("logs", []).append({
                "level": level,
                "message": message,
                "data": data
            })
            return data

        return data

    def _get_field(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested field value."""
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    def _set_field(self, data: Dict[str, Any], path: str, value: Any) -> None:
        """Set nested field value."""
        parts = path.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def _delete_field(self, data: Dict[str, Any], path: str) -> None:
        """Delete nested field."""
        parts = path.split(".")
        current = data
        for part in parts[:-1]:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return
        if parts[-1] in current:
            del current[parts[-1]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.type.value,
            "parameters": self.parameters,
            "description": self.description
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleAction":
        """Create from dictionary."""
        return cls(
            type=ActionType(data.get("type", "set_value")),
            parameters=data.get("parameters", {}),
            description=data.get("description")
        )


# =============================================================================
# Business Rule
# =============================================================================


@dataclass
class BusinessRule:
    """A business rule with conditions and actions."""

    id: str
    name: str
    conditions: List[RuleCondition] = field(default_factory=list)
    actions: List[RuleAction] = field(default_factory=list)
    description: Optional[str] = None

    # Configuration
    condition_logic: str = "all"  # all, any
    priority: int = 0
    enabled: bool = True
    stop_on_match: bool = False

    # Metadata
    tenant_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    version: int = 1

    def matches(self, data: Dict[str, Any]) -> bool:
        """Check if rule conditions match the data."""
        if not self.enabled:
            return False

        if not self.conditions:
            return True  # No conditions = always match

        results = [cond.evaluate(data) for cond in self.conditions]

        if self.condition_logic == "any":
            return any(results)
        return all(results)

    def execute(
        self,
        data: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Execute rule actions.

        Returns (modified_data, execution_context)
        """
        context = context or {}
        current_data = data.copy()

        for action in self.actions:
            success, result = action.execute(current_data, context)
            if success and result:
                current_data = result

            # Check for stop
            if context.get("stop"):
                break

        return current_data, context

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "conditions": [c.to_dict() for c in self.conditions],
            "actions": [a.to_dict() for a in self.actions],
            "conditionLogic": self.condition_logic,
            "priority": self.priority,
            "enabled": self.enabled,
            "stopOnMatch": self.stop_on_match,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BusinessRule":
        """Create from dictionary."""
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description"),
            conditions=[RuleCondition.from_dict(c) for c in data.get("conditions", [])],
            actions=[RuleAction.from_dict(a) for a in data.get("actions", [])],
            condition_logic=data.get("conditionLogic", "all"),
            priority=data.get("priority", 0),
            enabled=data.get("enabled", True),
            stop_on_match=data.get("stopOnMatch", False),
            version=data.get("version", 1)
        )


# =============================================================================
# Rule Set
# =============================================================================


@dataclass
class RuleSet:
    """A set of business rules."""

    id: str
    name: str
    rules: List[BusinessRule] = field(default_factory=list)
    description: Optional[str] = None

    # Configuration
    evaluation_mode: str = "all"  # all, first_match
    continue_on_error: bool = True

    # Metadata
    tenant_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    version: int = 1

    def add_rule(self, rule: BusinessRule) -> None:
        """Add a rule to the set."""
        self.rules.append(rule)
        self._sort_rules()

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a rule by ID."""
        for i, rule in enumerate(self.rules):
            if rule.id == rule_id:
                self.rules.pop(i)
                return True
        return False

    def _sort_rules(self) -> None:
        """Sort rules by priority (higher priority first)."""
        self.rules.sort(key=lambda r: r.priority, reverse=True)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "rules": [r.to_dict() for r in self.rules],
            "evaluationMode": self.evaluation_mode,
            "continueOnError": self.continue_on_error,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleSet":
        """Create from dictionary."""
        rule_set = cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description"),
            rules=[BusinessRule.from_dict(r) for r in data.get("rules", [])],
            evaluation_mode=data.get("evaluationMode", "all"),
            continue_on_error=data.get("continueOnError", True),
            version=data.get("version", 1)
        )
        rule_set._sort_rules()
        return rule_set


# =============================================================================
# Decision Table
# =============================================================================


@dataclass
class DecisionTableRow:
    """A row in a decision table."""

    conditions: Dict[str, Any]  # Column name -> condition value
    actions: Dict[str, Any]  # Action name -> value
    priority: int = 0
    enabled: bool = True

    def matches(self, data: Dict[str, Any]) -> bool:
        """Check if row matches data."""
        for col_name, condition in self.conditions.items():
            value = data.get(col_name)

            # Handle special condition values
            if condition == "*" or condition == "ANY":
                continue  # Wildcard match
            elif condition == "-" or condition == "NONE":
                if value is not None and value != "":
                    return False
            elif isinstance(condition, str) and condition.startswith(">="):
                threshold = float(condition[2:])
                if not (value is not None and float(value) >= threshold):
                    return False
            elif isinstance(condition, str) and condition.startswith("<="):
                threshold = float(condition[2:])
                if not (value is not None and float(value) <= threshold):
                    return False
            elif isinstance(condition, str) and condition.startswith(">"):
                threshold = float(condition[1:])
                if not (value is not None and float(value) > threshold):
                    return False
            elif isinstance(condition, str) and condition.startswith("<"):
                threshold = float(condition[1:])
                if not (value is not None and float(value) < threshold):
                    return False
            elif isinstance(condition, str) and ".." in condition:
                # Range: "10..20"
                parts = condition.split("..")
                low, high = float(parts[0]), float(parts[1])
                if not (value is not None and low <= float(value) <= high):
                    return False
            elif isinstance(condition, list):
                # List of allowed values
                if value not in condition:
                    return False
            else:
                # Exact match
                if value != condition:
                    return False

        return True


@dataclass
class DecisionTable:
    """
    A decision table for structured rules.

    Decision tables provide a tabular way to define rules:
    - Columns define conditions and actions
    - Rows define rule entries
    - Easy for business users to understand and maintain
    """

    id: str
    name: str
    condition_columns: List[str] = field(default_factory=list)
    action_columns: List[str] = field(default_factory=list)
    rows: List[DecisionTableRow] = field(default_factory=list)
    description: Optional[str] = None

    # Configuration
    hit_policy: str = "first"  # first, all, priority, unique
    missing_action: str = "skip"  # skip, error, default

    # Default values for actions
    default_actions: Dict[str, Any] = field(default_factory=dict)

    # Metadata
    tenant_id: Optional[str] = None
    version: int = 1

    def evaluate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate data against decision table.

        Returns action values for matching rows.
        """
        matching_rows = []

        # Sort by priority for priority hit policy
        sorted_rows = sorted(
            [r for r in self.rows if r.enabled],
            key=lambda r: r.priority,
            reverse=True
        )

        for row in sorted_rows:
            if row.matches(data):
                matching_rows.append(row)

                if self.hit_policy == "first":
                    break
                elif self.hit_policy == "unique":
                    # Only one row should match
                    if len(matching_rows) > 1:
                        raise ValueError("Multiple rows matched in unique hit policy")
                    break

        if not matching_rows:
            if self.missing_action == "error":
                raise ValueError("No matching row found")
            elif self.missing_action == "default":
                return self.default_actions.copy()
            return {}

        # Combine actions from matching rows
        if self.hit_policy == "all":
            result = {}
            for row in matching_rows:
                result.update(row.actions)
            return result
        else:
            return matching_rows[0].actions.copy()

    def add_row(
        self,
        conditions: Dict[str, Any],
        actions: Dict[str, Any],
        priority: int = 0
    ) -> None:
        """Add a row to the table."""
        self.rows.append(DecisionTableRow(
            conditions=conditions,
            actions=actions,
            priority=priority
        ))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "conditionColumns": self.condition_columns,
            "actionColumns": self.action_columns,
            "rows": [
                {
                    "conditions": row.conditions,
                    "actions": row.actions,
                    "priority": row.priority,
                    "enabled": row.enabled
                }
                for row in self.rows
            ],
            "hitPolicy": self.hit_policy,
            "missingAction": self.missing_action,
            "defaultActions": self.default_actions,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DecisionTable":
        """Create from dictionary."""
        table = cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description"),
            condition_columns=data.get("conditionColumns", []),
            action_columns=data.get("actionColumns", []),
            hit_policy=data.get("hitPolicy", "first"),
            missing_action=data.get("missingAction", "skip"),
            default_actions=data.get("defaultActions", {}),
            version=data.get("version", 1)
        )

        for row_data in data.get("rows", []):
            table.rows.append(DecisionTableRow(
                conditions=row_data.get("conditions", {}),
                actions=row_data.get("actions", {}),
                priority=row_data.get("priority", 0),
                enabled=row_data.get("enabled", True)
            ))

        return table


# =============================================================================
# Rule Engine
# =============================================================================


@dataclass
class RuleResult:
    """Result of rule evaluation."""

    rule_id: str
    rule_name: str
    matched: bool
    actions_executed: int = 0
    output_data: Optional[Dict[str, Any]] = None
    context: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0
    errors: List[str] = field(default_factory=list)


class RuleEvaluator:
    """Evaluates a single rule."""

    def evaluate(
        self,
        rule: BusinessRule,
        data: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> RuleResult:
        """Evaluate a rule against data."""
        import time
        start = time.time()

        context = context or {}
        result = RuleResult(
            rule_id=rule.id,
            rule_name=rule.name,
            matched=False
        )

        try:
            if rule.matches(data):
                result.matched = True
                output_data, exec_context = rule.execute(data, context)
                result.output_data = output_data
                result.context = exec_context
                result.actions_executed = len(rule.actions)
        except Exception as e:
            result.errors.append(str(e))

        result.execution_time_ms = (time.time() - start) * 1000
        return result


class RuleEngine:
    """
    Business Rules Engine.

    Evaluates rule sets against data and returns results.
    """

    def __init__(self):
        """Initialize engine."""
        self._rule_sets: Dict[str, RuleSet] = {}
        self._decision_tables: Dict[str, DecisionTable] = {}
        self._evaluator = RuleEvaluator()

    def register_rule_set(self, rule_set: RuleSet) -> None:
        """Register a rule set."""
        self._rule_sets[rule_set.id] = rule_set

    def register_decision_table(self, table: DecisionTable) -> None:
        """Register a decision table."""
        self._decision_tables[table.id] = table

    def get_rule_set(self, rule_set_id: str) -> Optional[RuleSet]:
        """Get a rule set by ID."""
        return self._rule_sets.get(rule_set_id)

    def get_decision_table(self, table_id: str) -> Optional[DecisionTable]:
        """Get a decision table by ID."""
        return self._decision_tables.get(table_id)

    def evaluate_rule_set(
        self,
        rule_set_id: str,
        data: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> List[RuleResult]:
        """
        Evaluate a rule set against data.

        Args:
            rule_set_id: Rule set ID
            data: Data to evaluate
            context: Optional execution context

        Returns:
            List of rule results
        """
        rule_set = self.get_rule_set(rule_set_id)
        if not rule_set:
            return []

        results = []
        current_data = data.copy()
        context = context or {}

        for rule in rule_set.rules:
            if not rule.enabled:
                continue

            try:
                result = self._evaluator.evaluate(rule, current_data, context)
                results.append(result)

                if result.matched:
                    # Update data for next rule
                    if result.output_data:
                        current_data = result.output_data

                    # Check for stop
                    if rule.stop_on_match or context.get("stop"):
                        break

                    # Check for first_match mode
                    if rule_set.evaluation_mode == "first_match":
                        break

            except Exception as e:
                if not rule_set.continue_on_error:
                    raise
                results.append(RuleResult(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    matched=False,
                    errors=[str(e)]
                ))

        return results

    def evaluate_decision_table(
        self,
        table_id: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Evaluate a decision table.

        Args:
            table_id: Decision table ID
            data: Data to evaluate

        Returns:
            Action values
        """
        table = self.get_decision_table(table_id)
        if not table:
            return {}

        return table.evaluate(data)

    def execute(
        self,
        data: Dict[str, Any],
        rule_set_ids: Optional[List[str]] = None,
        decision_table_ids: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute all specified rules and decision tables.

        Returns final data with all modifications applied.
        """
        context = context or {}
        current_data = data.copy()

        # Evaluate rule sets
        for rule_set_id in (rule_set_ids or []):
            results = self.evaluate_rule_set(rule_set_id, current_data, context)
            for result in results:
                if result.matched and result.output_data:
                    current_data = result.output_data

                if context.get("stop") or context.get("rejected"):
                    break

            if context.get("stop") or context.get("rejected"):
                break

        # Evaluate decision tables
        for table_id in (decision_table_ids or []):
            try:
                actions = self.evaluate_decision_table(table_id, current_data)
                current_data.update(actions)
            except Exception:
                pass

        return current_data


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Rule types
    "BusinessRule",
    "RuleCondition",
    "RuleAction",
    "RuleSet",
    # Operators
    "ConditionOperator",
    "ActionType",
    # Engine
    "RuleEngine",
    "RuleEvaluator",
    "RuleResult",
    # Decision tables
    "DecisionTable",
    "DecisionTableRow",
]

"""
Business User Features for Logic Weaver.

Provides low-code/no-code features for business users:
- Expression Builder: Visual expression construction
- Lookup Tables: Reference data management
- Business Rules: Rule-based decision logic
- CEP: Complex Event Processing / Real-time Dashboard

These components enable business users to configure integrations
without requiring programming knowledge.
"""

from assemblyline_common.business_user.expression_builder import (
    # Expression types
    Expression,
    ExpressionType,
    ExpressionOperator,
    ExpressionValue,
    # Builder
    ExpressionBuilder,
    ExpressionParser,
    ExpressionEvaluator,
    # Context
    ExpressionContext,
    ExpressionResult,
    # Functions
    ExpressionFunction,
    FunctionRegistry,
)

from assemblyline_common.business_user.lookup_tables import (
    # Table types
    LookupTable,
    LookupColumn,
    ColumnType,
    LookupRow,
    # Operations
    LookupQuery,
    LookupResult,
    # Manager
    LookupTableManager,
    LookupCache,
    # Import/Export
    TableImporter,
    TableExporter,
)

from assemblyline_common.business_user.business_rules import (
    # Rule types
    BusinessRule,
    RuleCondition,
    RuleAction,
    RuleSet,
    # Operators
    ConditionOperator,
    ActionType,
    # Engine
    RuleEngine,
    RuleEvaluator,
    RuleResult,
    # Decision tables
    DecisionTable,
    DecisionTableRow,
)

from assemblyline_common.business_user.cep import (
    # Events
    Event,
    EventPattern,
    EventStream,
    # Windows
    TimeWindow,
    CountWindow,
    SessionWindow,
    # Aggregations
    Aggregation,
    AggregationType,
    # Engine
    CEPEngine,
    CEPRule,
    CEPResult,
    # Dashboard
    DashboardMetric,
    DashboardWidget,
    MetricType,
)

__all__ = [
    # Expression Builder
    "Expression",
    "ExpressionType",
    "ExpressionOperator",
    "ExpressionValue",
    "ExpressionBuilder",
    "ExpressionParser",
    "ExpressionEvaluator",
    "ExpressionContext",
    "ExpressionResult",
    "ExpressionFunction",
    "FunctionRegistry",
    # Lookup Tables
    "LookupTable",
    "LookupColumn",
    "ColumnType",
    "LookupRow",
    "LookupQuery",
    "LookupResult",
    "LookupTableManager",
    "LookupCache",
    "TableImporter",
    "TableExporter",
    # Business Rules
    "BusinessRule",
    "RuleCondition",
    "RuleAction",
    "RuleSet",
    "ConditionOperator",
    "ActionType",
    "RuleEngine",
    "RuleEvaluator",
    "RuleResult",
    "DecisionTable",
    "DecisionTableRow",
    # CEP
    "Event",
    "EventPattern",
    "EventStream",
    "TimeWindow",
    "CountWindow",
    "SessionWindow",
    "Aggregation",
    "AggregationType",
    "CEPEngine",
    "CEPRule",
    "CEPResult",
    "DashboardMetric",
    "DashboardWidget",
    "MetricType",
]

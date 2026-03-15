"""
Logic Weaver Flow Engine

Advanced flow execution engine with subflows, error handling, and parallel execution.
Surpasses MuleSoft Anypoint and Apigee in flexibility and Python integration.

Features:
- Subflows: Reusable flow components that can be called from other flows
- Error Handling: Dedicated error paths with retry, fallback, and dead letter
- Scatter-Gather: Parallel execution with aggregation
- For-Each: Iterative processing with batching
- Choice Router: Conditional routing with expression evaluation
- Visual Data Mapper: Drag-and-drop field mapping

Comparison:
| Feature           | MuleSoft | Apigee | Kong | Logic Weaver |
|-------------------|----------|--------|------|--------------|
| Subflows          | Yes      | No     | No   | Yes          |
| Error Handlers    | Yes      | Yes    | No   | Yes          |
| Scatter-Gather    | Yes      | No     | No   | Yes          |
| Python Scripts    | No       | No     | No   | Yes          |
| Visual Mapper     | Yes      | No     | No   | Yes          |
| Hot Reload        | No       | No     | No   | Yes          |
"""

from assemblyline_common.flows.subflows import (
    SubflowReference,
    SubflowExecutor,
    SubflowRegistry,
    get_subflow_registry,
)

from assemblyline_common.flows.error_handling import (
    ErrorHandlerType,
    ErrorHandler,
    ErrorHandlerConfig,
    ErrorHandlingStrategy,
    OnErrorContinue,
    OnErrorPropagate,
    DeadLetterHandler,
    RetryHandler,
    FallbackHandler,
    FlowErrorContext,
)

from assemblyline_common.flows.patterns import (
    ScatterGatherNode,
    ForEachNode,
    ChoiceRouter,
    RouterCondition,
    AggregationStrategy,
    BatchProcessor,
)

from assemblyline_common.flows.data_mapper import (
    FieldMapping,
    DataMapper,
    MappingRule,
    TransformFunction,
    VisualMapperConfig,
)

__all__ = [
    # Subflows
    "SubflowReference",
    "SubflowExecutor",
    "SubflowRegistry",
    "get_subflow_registry",

    # Error Handling
    "ErrorHandlerType",
    "ErrorHandler",
    "ErrorHandlerConfig",
    "ErrorHandlingStrategy",
    "OnErrorContinue",
    "OnErrorPropagate",
    "DeadLetterHandler",
    "RetryHandler",
    "FallbackHandler",
    "FlowErrorContext",

    # Patterns
    "ScatterGatherNode",
    "ForEachNode",
    "ChoiceRouter",
    "RouterCondition",
    "AggregationStrategy",
    "BatchProcessor",

    # Data Mapper
    "FieldMapping",
    "DataMapper",
    "MappingRule",
    "TransformFunction",
    "VisualMapperConfig",
]

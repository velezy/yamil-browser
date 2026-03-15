"""
Flow Policy Engine for Logic Weaver

Enterprise-grade policy management superior to Kong, Apigee, and MuleSoft:

| Feature                    | Kong | Apigee | MuleSoft | Logic Weaver |
|---------------------------|------|--------|----------|--------------|
| Hierarchy Levels          | 4    | 3      | 3        | 6            |
| Execution Phases          | 1    | 3      | 3        | 7            |
| Node-Level Policies       | No   | No     | Limited  | Yes          |
| Consumer Policies         | Yes  | No     | No       | Yes          |
| Hot Reload                | DB   | Deploy | Deploy   | Instant      |
| Shadow Mode               | No   | No     | No       | Yes          |
| Policy Dependencies       | No   | No     | No       | Yes          |
| Per-Policy Circuit Breaker| No   | No     | No       | Yes          |
| Fault Injection           | No   | Yes    | No       | Yes          |
| A/B Testing               | No   | Yes    | Yes      | Yes          |
| Analytics                 | Basic| Basic  | Basic    | Advanced     |

6-Level Hierarchy:
- Global: Apply to all flows and routes
- Tenant: Apply to specific tenant (multi-tenancy)
- Route: Apply to specific API endpoints (Kong-style)
- Flow: Apply to specific message processing flows (Apigee-style)
- Node: Apply to specific nodes within a flow
- Consumer: Apply to specific API consumer (Kong-style)

7 Execution Phases:
- PRE_ROUTE: Before route matching (IP filter, early auth)
- PRE_FLOW: Before flow starts (validation, rate limiting)
- PRE_NODE: Before each node (node-specific policies)
- POST_NODE: After each node (transform, enrich)
- POST_FLOW: After flow completes (logging, audit)
- POST_CLIENT: After response sent (async analytics) - Apigee feature
- ON_ERROR: On any error (error handling, notification)
"""

from .engine import (
    # Main Engine
    FlowPolicyEngine,
    get_flow_policy_engine,

    # Enums
    PolicyPhase,
    PolicyLevel,
    PolicyAction,
    PolicyMode,
    TrafficSplit,

    # Configuration Classes
    PolicyAttachment,
    ConsumerConfig,
    FlowPolicyConfig,
    NodePolicyConfig,
    TrafficSplitConfig,
    FaultInjectionConfig,

    # Execution Classes
    PolicyExecutionContext,
    PolicyExecutionResult,

    # Version Control
    PolicyVersion,

    # Circuit Breaker
    PolicyCircuitBreaker,

    # Analytics
    PolicyAnalytics,

    # Expression Engine
    ExpressionEngine,
)

from .flow_policies import (
    # Base Class
    FlowPolicy,
    FlowPolicyContext,
    FlowPolicyResult,

    # Built-in Flow Policies
    PHIMaskingPolicy,
    MessageValidationPolicy,
    MessageTransformPolicy,
    RetryPolicy,
    TimeoutPolicy,
    CircuitBreakerFlowPolicy,
    AuditLogPolicy,
    ContentRoutingPolicy,
    CachingPolicy,
    CompressionPolicy,
    CorrelationIdPolicy,
    DeduplicationPolicy,
)

__all__ = [
    # Main Engine
    "FlowPolicyEngine",
    "get_flow_policy_engine",

    # Enums
    "PolicyPhase",
    "PolicyLevel",
    "PolicyAction",
    "PolicyMode",
    "TrafficSplit",

    # Configuration Classes
    "PolicyAttachment",
    "ConsumerConfig",
    "FlowPolicyConfig",
    "NodePolicyConfig",
    "TrafficSplitConfig",
    "FaultInjectionConfig",

    # Execution Classes
    "PolicyExecutionContext",
    "PolicyExecutionResult",

    # Version Control
    "PolicyVersion",

    # Circuit Breaker
    "PolicyCircuitBreaker",

    # Analytics
    "PolicyAnalytics",

    # Expression Engine
    "ExpressionEngine",

    # Flow Policy Base
    "FlowPolicy",
    "FlowPolicyContext",
    "FlowPolicyResult",

    # Built-in Flow Policies
    "PHIMaskingPolicy",
    "MessageValidationPolicy",
    "MessageTransformPolicy",
    "RetryPolicy",
    "TimeoutPolicy",
    "CircuitBreakerFlowPolicy",
    "AuditLogPolicy",
    "ContentRoutingPolicy",
    "CachingPolicy",
    "CompressionPolicy",
    "CorrelationIdPolicy",
    "DeduplicationPolicy",
]

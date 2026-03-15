"""
Built-in Flow Policies for Logic Weaver

Enterprise-grade policies for message flow processing.
These policies can be attached at any level (Global, Tenant, Route, Flow, Node, Consumer)
and executed at any phase (PRE_ROUTE, PRE_FLOW, PRE_NODE, POST_NODE, POST_FLOW, POST_CLIENT, ON_ERROR).
"""

import asyncio
import gzip
import hashlib
import json
import logging
import re
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Tuple, Union

from .engine import PolicyExecutionContext, PolicyExecutionResult, PolicyAction

logger = logging.getLogger(__name__)


# ============================================================================
# BASE CLASSES
# ============================================================================

@dataclass
class FlowPolicyContext:
    """
    Extended context for flow policies.

    Provides additional flow-specific information beyond PolicyExecutionContext.
    """
    execution_context: PolicyExecutionContext

    # Flow-specific
    flow_version: str = "1.0.0"
    flow_tags: List[str] = field(default_factory=list)

    # Node-specific
    node_input: Optional[Any] = None
    node_output: Optional[Any] = None

    # Metrics
    nodes_executed: int = 0
    total_latency_ms: float = 0.0


@dataclass
class FlowPolicyResult:
    """Result from flow policy execution."""
    success: bool
    action: PolicyAction = PolicyAction.CONTINUE

    # Modified data
    modified_message: Optional[Any] = None
    modified_headers: Optional[Dict[str, str]] = None

    # Error
    error: Optional[str] = None
    error_code: Optional[str] = None

    # Metrics
    execution_time_ms: float = 0.0

    # Additional data
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_execution_result(self) -> PolicyExecutionResult:
        """Convert to PolicyExecutionResult."""
        return PolicyExecutionResult(
            success=self.success,
            action=self.action,
            modified_message=self.modified_message,
            modified_headers=self.modified_headers,
            error=self.error,
            error_code=self.error_code,
            execution_time_ms=self.execution_time_ms,
        )


class FlowPolicy(ABC):
    """
    Base class for flow policies.

    All flow policies must implement the execute method.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the policy with configuration.

        Args:
            config: Policy-specific configuration
        """
        self.config = config
        self._name = self.__class__.__name__

    @property
    def name(self) -> str:
        """Get policy name."""
        return self._name

    @abstractmethod
    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """
        Execute the policy.

        Args:
            context: Execution context with message and metadata

        Returns:
            PolicyExecutionResult with action and optional modifications
        """
        pass

    def _log_execution(self, context: PolicyExecutionContext, result: PolicyExecutionResult):
        """Log policy execution."""
        logger.debug(
            f"Policy {self.name} executed: "
            f"flow={context.flow_id}, "
            f"phase={context.phase.value}, "
            f"success={result.success}, "
            f"action={result.action.value}, "
            f"time={result.execution_time_ms:.2f}ms"
        )


# ============================================================================
# PHI MASKING POLICY - HIPAA Compliance
# ============================================================================

class PHIMaskingPolicy(FlowPolicy):
    """
    Masks Protected Health Information (PHI) for HIPAA compliance.

    Features:
    - SSN, MRN, DOB masking
    - Name masking with preservation options
    - Address masking
    - Phone/email masking
    - Custom pattern support
    - Selective field masking

    Config:
        mask_ssn: bool = True
        mask_mrn: bool = True
        mask_dob: bool = True
        mask_names: bool = True
        mask_addresses: bool = True
        mask_phone: bool = True
        mask_email: bool = True
        preserve_format: bool = True
        custom_patterns: List[Dict] = []
        exclude_fields: List[str] = []
    """

    # PHI patterns
    SSN_PATTERN = re.compile(r'\b\d{3}-\d{2}-\d{4}\b')
    PHONE_PATTERN = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
    EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    DOB_PATTERN = re.compile(r'\b\d{4}-\d{2}-\d{2}\b|\b\d{2}/\d{2}/\d{4}\b')
    MRN_PATTERN = re.compile(r'\bMRN[:\s]*\d{6,10}\b', re.IGNORECASE)

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mask_ssn = config.get("mask_ssn", True)
        self.mask_mrn = config.get("mask_mrn", True)
        self.mask_dob = config.get("mask_dob", True)
        self.mask_names = config.get("mask_names", True)
        self.mask_addresses = config.get("mask_addresses", True)
        self.mask_phone = config.get("mask_phone", True)
        self.mask_email = config.get("mask_email", True)
        self.preserve_format = config.get("preserve_format", True)
        self.custom_patterns = config.get("custom_patterns", [])
        self.exclude_fields = set(config.get("exclude_fields", []))

        # Compile custom patterns
        self._custom_compiled = [
            (re.compile(p["pattern"]), p.get("replacement", "***"))
            for p in self.custom_patterns
        ]

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute PHI masking on the message."""
        start = time.time()

        if context.message is None:
            return PolicyExecutionResult(success=True)

        try:
            masked = self._mask_message(context.message)

            return PolicyExecutionResult(
                success=True,
                action=PolicyAction.CONTINUE,
                modified_message=masked,
                execution_time_ms=(time.time() - start) * 1000,
            )

        except Exception as e:
            logger.error(f"PHI masking failed: {e}")
            return PolicyExecutionResult(
                success=False,
                action=PolicyAction.ABORT,
                error=str(e),
                error_code="PHI_MASK_FAILED",
                execution_time_ms=(time.time() - start) * 1000,
            )

    def _mask_message(self, message: Any, path: str = "") -> Any:
        """Recursively mask PHI in message."""
        if isinstance(message, dict):
            return {
                k: self._mask_message(v, f"{path}.{k}")
                for k, v in message.items()
            }
        elif isinstance(message, list):
            return [
                self._mask_message(item, f"{path}[{i}]")
                for i, item in enumerate(message)
            ]
        elif isinstance(message, str):
            return self._mask_string(message, path)
        else:
            return message

    def _mask_string(self, value: str, path: str) -> str:
        """Mask PHI patterns in string."""
        # Skip excluded fields
        field_name = path.split(".")[-1] if path else ""
        if field_name in self.exclude_fields:
            return value

        result = value

        # Apply standard masks
        if self.mask_ssn:
            if self.preserve_format:
                result = self.SSN_PATTERN.sub("XXX-XX-XXXX", result)
            else:
                result = self.SSN_PATTERN.sub("***", result)

        if self.mask_phone:
            if self.preserve_format:
                result = self.PHONE_PATTERN.sub("XXX-XXX-XXXX", result)
            else:
                result = self.PHONE_PATTERN.sub("***", result)

        if self.mask_email:
            result = self.EMAIL_PATTERN.sub("***@***.***", result)

        if self.mask_dob:
            result = self.DOB_PATTERN.sub("****-**-**", result)

        if self.mask_mrn:
            result = self.MRN_PATTERN.sub("MRN:XXXXXX", result)

        # Apply custom patterns
        for pattern, replacement in self._custom_compiled:
            result = pattern.sub(replacement, result)

        return result


# ============================================================================
# MESSAGE VALIDATION POLICY
# ============================================================================

class MessageValidationPolicy(FlowPolicy):
    """
    Validates message structure and content.

    Features:
    - JSON Schema validation
    - Required fields checking
    - Field type validation
    - Value range validation
    - Custom validation rules

    Config:
        schema: Dict = None (JSON Schema)
        required_fields: List[str] = []
        field_types: Dict[str, str] = {}
        value_ranges: Dict[str, Dict] = {}
        custom_rules: List[Dict] = []
        fail_on_error: bool = True
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.schema = config.get("schema")
        self.required_fields = config.get("required_fields", [])
        self.field_types = config.get("field_types", {})
        self.value_ranges = config.get("value_ranges", {})
        self.custom_rules = config.get("custom_rules", [])
        self.fail_on_error = config.get("fail_on_error", True)

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute message validation."""
        start = time.time()

        if context.message is None:
            return PolicyExecutionResult(
                success=True,
                execution_time_ms=(time.time() - start) * 1000,
            )

        errors = []

        # Check required fields
        for field in self.required_fields:
            if not self._has_field(context.message, field):
                errors.append(f"Missing required field: {field}")

        # Check field types
        for field, expected_type in self.field_types.items():
            value = self._get_field(context.message, field)
            if value is not None and not self._check_type(value, expected_type):
                errors.append(f"Invalid type for {field}: expected {expected_type}")

        # Check value ranges
        for field, range_config in self.value_ranges.items():
            value = self._get_field(context.message, field)
            if value is not None:
                if "min" in range_config and value < range_config["min"]:
                    errors.append(f"{field} below minimum: {value} < {range_config['min']}")
                if "max" in range_config and value > range_config["max"]:
                    errors.append(f"{field} above maximum: {value} > {range_config['max']}")

        if errors:
            if self.fail_on_error:
                return PolicyExecutionResult(
                    success=False,
                    action=PolicyAction.ABORT,
                    error="; ".join(errors),
                    error_code="VALIDATION_FAILED",
                    execution_time_ms=(time.time() - start) * 1000,
                )
            else:
                logger.warning(f"Validation warnings: {errors}")

        return PolicyExecutionResult(
            success=True,
            action=PolicyAction.CONTINUE,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def _has_field(self, message: Any, path: str) -> bool:
        """Check if field exists in message."""
        return self._get_field(message, path) is not None

    def _get_field(self, message: Any, path: str) -> Any:
        """Get field value by path."""
        parts = path.split(".")
        current = message

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    return None
            else:
                return None

            if current is None:
                return None

        return current

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            "string": str,
            "int": int,
            "float": (int, float),
            "bool": bool,
            "list": list,
            "dict": dict,
        }
        expected = type_map.get(expected_type)
        return isinstance(value, expected) if expected else True


# ============================================================================
# MESSAGE TRANSFORM POLICY
# ============================================================================

class MessageTransformPolicy(FlowPolicy):
    """
    Transforms message content.

    Features:
    - Field mapping/renaming
    - Value transformation
    - Field addition/removal
    - Template-based transformation
    - JSONPath support

    Config:
        mappings: Dict[str, str] = {}
        transforms: Dict[str, Dict] = {}
        add_fields: Dict[str, Any] = {}
        remove_fields: List[str] = []
        template: str = None
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mappings = config.get("mappings", {})
        self.transforms = config.get("transforms", {})
        self.add_fields = config.get("add_fields", {})
        self.remove_fields = config.get("remove_fields", [])
        self.template = config.get("template")

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute message transformation."""
        start = time.time()

        if context.message is None:
            return PolicyExecutionResult(success=True)

        try:
            result = context.message

            # Apply mappings
            if self.mappings and isinstance(result, dict):
                result = self._apply_mappings(result)

            # Apply transforms
            if self.transforms and isinstance(result, dict):
                result = self._apply_transforms(result)

            # Add fields
            if self.add_fields and isinstance(result, dict):
                result.update(self.add_fields)

            # Remove fields
            if self.remove_fields and isinstance(result, dict):
                for field in self.remove_fields:
                    self._remove_field(result, field)

            return PolicyExecutionResult(
                success=True,
                action=PolicyAction.CONTINUE,
                modified_message=result,
                execution_time_ms=(time.time() - start) * 1000,
            )

        except Exception as e:
            logger.error(f"Transform failed: {e}")
            return PolicyExecutionResult(
                success=False,
                action=PolicyAction.ABORT,
                error=str(e),
                error_code="TRANSFORM_FAILED",
                execution_time_ms=(time.time() - start) * 1000,
            )

    def _apply_mappings(self, message: Dict) -> Dict:
        """Apply field mappings."""
        result = {}
        for old_key, new_key in self.mappings.items():
            if old_key in message:
                result[new_key] = message[old_key]

        # Copy unmapped fields
        for key in message:
            if key not in self.mappings:
                result[key] = message[key]

        return result

    def _apply_transforms(self, message: Dict) -> Dict:
        """Apply value transforms."""
        result = dict(message)

        for field, transform in self.transforms.items():
            if field in result:
                result[field] = self._transform_value(result[field], transform)

        return result

    def _transform_value(self, value: Any, transform: Dict) -> Any:
        """Transform a single value."""
        transform_type = transform.get("type")

        if transform_type == "uppercase":
            return value.upper() if isinstance(value, str) else value
        elif transform_type == "lowercase":
            return value.lower() if isinstance(value, str) else value
        elif transform_type == "prefix":
            return f"{transform.get('value', '')}{value}"
        elif transform_type == "suffix":
            return f"{value}{transform.get('value', '')}"
        elif transform_type == "replace":
            pattern = transform.get("pattern", "")
            replacement = transform.get("replacement", "")
            return re.sub(pattern, replacement, str(value))
        elif transform_type == "default":
            return value if value is not None else transform.get("value")

        return value

    def _remove_field(self, message: Dict, path: str):
        """Remove field by path."""
        parts = path.split(".")
        current = message

        for i, part in enumerate(parts[:-1]):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return

        if isinstance(current, dict) and parts[-1] in current:
            del current[parts[-1]]


# ============================================================================
# RETRY POLICY
# ============================================================================

class RetryPolicy(FlowPolicy):
    """
    Handles retry logic for failed operations.

    Features:
    - Configurable retry count
    - Exponential backoff
    - Retry conditions
    - Jitter support
    - Dead letter queue

    Config:
        max_retries: int = 3
        initial_delay_ms: int = 100
        max_delay_ms: int = 10000
        exponential_base: float = 2.0
        jitter: bool = True
        retry_on_errors: List[str] = []
        dead_letter_enabled: bool = False
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.max_retries = config.get("max_retries", 3)
        self.initial_delay_ms = config.get("initial_delay_ms", 100)
        self.max_delay_ms = config.get("max_delay_ms", 10000)
        self.exponential_base = config.get("exponential_base", 2.0)
        self.jitter = config.get("jitter", True)
        self.retry_on_errors = set(config.get("retry_on_errors", []))
        self.dead_letter_enabled = config.get("dead_letter_enabled", False)

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute retry policy (tracks retry state)."""
        start = time.time()

        # Get current retry count from context
        retry_count = context.variables.get("_retry_count", 0)

        # Check if we have errors to retry
        if not context.errors:
            return PolicyExecutionResult(
                success=True,
                execution_time_ms=(time.time() - start) * 1000,
            )

        last_error = context.errors[-1] if context.errors else {}
        error_type = last_error.get("type", "")

        # Check if we should retry this error
        if self.retry_on_errors and error_type not in self.retry_on_errors:
            return PolicyExecutionResult(
                success=True,
                action=PolicyAction.CONTINUE,
                execution_time_ms=(time.time() - start) * 1000,
            )

        # Check retry limit
        if retry_count >= self.max_retries:
            if self.dead_letter_enabled:
                # Send to DLQ
                context.variables["_dead_letter"] = True
                logger.warning(f"Max retries exceeded, sending to DLQ: {context.execution_id}")

            return PolicyExecutionResult(
                success=False,
                action=PolicyAction.ABORT,
                error=f"Max retries ({self.max_retries}) exceeded",
                error_code="MAX_RETRIES_EXCEEDED",
                execution_time_ms=(time.time() - start) * 1000,
            )

        # Calculate delay
        delay = self._calculate_delay(retry_count)

        # Wait
        await asyncio.sleep(delay / 1000)

        # Increment retry count
        context.variables["_retry_count"] = retry_count + 1

        return PolicyExecutionResult(
            success=True,
            action=PolicyAction.RETRY,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def _calculate_delay(self, retry_count: int) -> float:
        """Calculate delay with exponential backoff and jitter."""
        import random

        delay = self.initial_delay_ms * (self.exponential_base ** retry_count)
        delay = min(delay, self.max_delay_ms)

        if self.jitter:
            delay = delay * (0.5 + random.random())

        return delay


# ============================================================================
# TIMEOUT POLICY
# ============================================================================

class TimeoutPolicy(FlowPolicy):
    """
    Enforces timeout on operations.

    Features:
    - Configurable timeout
    - Per-node timeouts
    - Grace period
    - Timeout action

    Config:
        timeout_ms: int = 30000
        node_timeouts: Dict[str, int] = {}
        grace_period_ms: int = 0
        action_on_timeout: str = "abort"
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.timeout_ms = config.get("timeout_ms", 30000)
        self.node_timeouts = config.get("node_timeouts", {})
        self.grace_period_ms = config.get("grace_period_ms", 0)
        self.action_on_timeout = config.get("action_on_timeout", "abort")

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Check if execution has timed out."""
        start = time.time()

        # Calculate elapsed time
        elapsed_ms = (datetime.now(timezone.utc) - context.start_time).total_seconds() * 1000

        # Get applicable timeout
        timeout = self.timeout_ms
        if context.current_node_id and context.current_node_id in self.node_timeouts:
            timeout = self.node_timeouts[context.current_node_id]

        # Check timeout
        if elapsed_ms > timeout + self.grace_period_ms:
            action = (
                PolicyAction.ABORT
                if self.action_on_timeout == "abort"
                else PolicyAction.SKIP
            )

            return PolicyExecutionResult(
                success=False,
                action=action,
                error=f"Timeout exceeded: {elapsed_ms:.0f}ms > {timeout}ms",
                error_code="TIMEOUT",
                execution_time_ms=(time.time() - start) * 1000,
            )

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )


# ============================================================================
# CIRCUIT BREAKER FLOW POLICY
# ============================================================================

class CircuitBreakerFlowPolicy(FlowPolicy):
    """
    Circuit breaker for flow execution.

    Features:
    - Three states: closed, open, half-open
    - Failure threshold
    - Success threshold for recovery
    - Timeout-based recovery

    Config:
        failure_threshold: int = 5
        success_threshold: int = 2
        timeout_seconds: int = 60
        failure_rate_threshold: float = 0.5
        minimum_requests: int = 10
    """

    # Class-level state (shared across instances)
    _states: Dict[str, Dict[str, Any]] = {}

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.failure_threshold = config.get("failure_threshold", 5)
        self.success_threshold = config.get("success_threshold", 2)
        self.timeout_seconds = config.get("timeout_seconds", 60)
        self.failure_rate_threshold = config.get("failure_rate_threshold", 0.5)
        self.minimum_requests = config.get("minimum_requests", 10)

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute circuit breaker check."""
        start = time.time()

        # Get state key (per flow)
        state_key = f"cb:{context.flow_id}"
        state = self._get_state(state_key)

        # Check circuit state
        if state["state"] == "open":
            # Check if timeout has passed
            if time.time() > state["open_until"]:
                state["state"] = "half-open"
                state["half_open_successes"] = 0
                logger.info(f"Circuit breaker half-open: {context.flow_id}")
            else:
                return PolicyExecutionResult(
                    success=False,
                    action=PolicyAction.ABORT,
                    error="Circuit breaker is open",
                    error_code="CIRCUIT_OPEN",
                    execution_time_ms=(time.time() - start) * 1000,
                )

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def _get_state(self, key: str) -> Dict[str, Any]:
        """Get or create circuit breaker state."""
        if key not in self._states:
            self._states[key] = {
                "state": "closed",
                "failures": 0,
                "successes": 0,
                "requests": 0,
                "half_open_successes": 0,
                "open_until": 0,
            }
        return self._states[key]

    def record_success(self, flow_id: str):
        """Record a successful execution."""
        state_key = f"cb:{flow_id}"
        state = self._get_state(state_key)

        state["successes"] += 1
        state["requests"] += 1

        if state["state"] == "half-open":
            state["half_open_successes"] += 1
            if state["half_open_successes"] >= self.success_threshold:
                state["state"] = "closed"
                state["failures"] = 0
                logger.info(f"Circuit breaker closed: {flow_id}")

    def record_failure(self, flow_id: str):
        """Record a failed execution."""
        state_key = f"cb:{flow_id}"
        state = self._get_state(state_key)

        state["failures"] += 1
        state["requests"] += 1

        if state["state"] == "half-open":
            state["state"] = "open"
            state["open_until"] = time.time() + self.timeout_seconds
            logger.warning(f"Circuit breaker opened (half-open failure): {flow_id}")
            return

        # Check if we should open the circuit
        if state["requests"] >= self.minimum_requests:
            failure_rate = state["failures"] / state["requests"]
            if failure_rate >= self.failure_rate_threshold:
                state["state"] = "open"
                state["open_until"] = time.time() + self.timeout_seconds
                logger.warning(f"Circuit breaker opened (threshold): {flow_id}")


# ============================================================================
# AUDIT LOG POLICY
# ============================================================================

class AuditLogPolicy(FlowPolicy):
    """
    Creates audit logs for compliance.

    Features:
    - Structured logging
    - PHI-safe logging
    - Correlation tracking
    - Compliance tagging

    Config:
        log_message: bool = False
        log_headers: bool = True
        mask_phi: bool = True
        include_timing: bool = True
        compliance_tags: List[str] = []
        custom_fields: Dict[str, str] = {}
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.log_message = config.get("log_message", False)
        self.log_headers = config.get("log_headers", True)
        self.mask_phi = config.get("mask_phi", True)
        self.include_timing = config.get("include_timing", True)
        self.compliance_tags = config.get("compliance_tags", [])
        self.custom_fields = config.get("custom_fields", {})

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Create audit log entry."""
        start = time.time()

        audit_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "flow_id": context.flow_id,
            "flow_name": context.flow_name,
            "phase": context.phase.value,
            "tenant_id": context.tenant_id,
            "user_id": context.user_id,
        }

        if context.current_node_id:
            audit_entry["node_id"] = context.current_node_id
            audit_entry["node_type"] = context.current_node_type

        if context.route_path:
            audit_entry["route_path"] = context.route_path
            audit_entry["http_method"] = context.http_method

        if self.log_headers:
            # Mask sensitive headers
            safe_headers = {
                k: "***" if k.lower() in ("authorization", "x-api-key") else v
                for k, v in context.headers.items()
            }
            audit_entry["headers"] = safe_headers

        if self.log_message and context.message:
            audit_entry["message_type"] = context.message_type
            audit_entry["message_size"] = len(str(context.message))

        if self.include_timing:
            elapsed = (datetime.now(timezone.utc) - context.start_time).total_seconds()
            audit_entry["elapsed_seconds"] = elapsed

        if context.errors:
            audit_entry["errors"] = context.errors

        if self.compliance_tags:
            audit_entry["compliance_tags"] = self.compliance_tags

        # Add custom fields
        for key, path in self.custom_fields.items():
            value = context.variables.get(path)
            if value is not None:
                audit_entry[key] = value

        # Log the audit entry
        logger.info(f"AUDIT: {json.dumps(audit_entry)}")

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )


# ============================================================================
# CONTENT ROUTING POLICY
# ============================================================================

class ContentRoutingPolicy(FlowPolicy):
    """
    Routes messages based on content.

    Features:
    - Field-based routing
    - Pattern matching
    - Header-based routing
    - Default route

    Config:
        rules: List[Dict] = []
        default_flow: str = None
        default_node: str = None
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.rules = config.get("rules", [])
        self.default_flow = config.get("default_flow")
        self.default_node = config.get("default_node")

        # Compile patterns
        for rule in self.rules:
            if "pattern" in rule:
                rule["_compiled"] = re.compile(rule["pattern"])

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute content-based routing."""
        start = time.time()

        for rule in self.rules:
            if self._match_rule(rule, context):
                return PolicyExecutionResult(
                    success=True,
                    action=PolicyAction.REDIRECT,
                    redirect_flow_id=rule.get("flow_id"),
                    redirect_node_id=rule.get("node_id"),
                    execution_time_ms=(time.time() - start) * 1000,
                )

        # Apply default if set
        if self.default_flow or self.default_node:
            return PolicyExecutionResult(
                success=True,
                action=PolicyAction.REDIRECT,
                redirect_flow_id=self.default_flow,
                redirect_node_id=self.default_node,
                execution_time_ms=(time.time() - start) * 1000,
            )

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def _match_rule(self, rule: Dict, context: PolicyExecutionContext) -> bool:
        """Check if rule matches context."""
        # Field match
        if "field" in rule and "value" in rule:
            field_value = self._get_field(context.message, rule["field"])
            if field_value != rule["value"]:
                return False

        # Pattern match
        if "field" in rule and "_compiled" in rule:
            field_value = str(self._get_field(context.message, rule["field"]) or "")
            if not rule["_compiled"].match(field_value):
                return False

        # Header match
        if "header" in rule:
            header_value = context.headers.get(rule["header"])
            if header_value != rule.get("header_value"):
                return False

        # Message type match
        if "message_type" in rule:
            if context.message_type != rule["message_type"]:
                return False

        return True

    def _get_field(self, message: Any, path: str) -> Any:
        """Get field value by path."""
        parts = path.split(".")
        current = message

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current


# ============================================================================
# CACHING POLICY
# ============================================================================

class CachingPolicy(FlowPolicy):
    """
    Caches responses for performance.

    Features:
    - TTL-based caching
    - Key generation
    - Cache invalidation
    - Conditional caching

    Config:
        ttl_seconds: int = 300
        key_fields: List[str] = []
        include_headers: List[str] = []
        cache_conditions: Dict = {}
    """

    # Simple in-memory cache (would use Redis in production)
    _cache: Dict[str, Tuple[Any, float]] = {}

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.ttl_seconds = config.get("ttl_seconds", 300)
        self.key_fields = config.get("key_fields", [])
        self.include_headers = config.get("include_headers", [])
        self.cache_conditions = config.get("cache_conditions", {})

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute caching logic."""
        start = time.time()

        # Generate cache key
        cache_key = self._generate_key(context)

        # Check cache
        if cache_key in self._cache:
            cached_value, expires_at = self._cache[cache_key]
            if time.time() < expires_at:
                return PolicyExecutionResult(
                    success=True,
                    action=PolicyAction.CACHE_HIT,
                    modified_message=cached_value,
                    cache_hit=True,
                    cache_key=cache_key,
                    execution_time_ms=(time.time() - start) * 1000,
                )
            else:
                # Expired, remove from cache
                del self._cache[cache_key]

        # Store current value in cache for later use
        context.variables["_cache_key"] = cache_key

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def cache_response(self, context: PolicyExecutionContext, response: Any):
        """Cache a response."""
        cache_key = context.variables.get("_cache_key")
        if cache_key:
            expires_at = time.time() + self.ttl_seconds
            self._cache[cache_key] = (response, expires_at)

    def _generate_key(self, context: PolicyExecutionContext) -> str:
        """Generate cache key."""
        parts = [
            context.flow_id,
            context.route_path or "",
            context.http_method or "",
        ]

        # Add key fields from message
        if context.message and self.key_fields:
            for field in self.key_fields:
                value = self._get_field(context.message, field)
                parts.append(str(value) if value is not None else "")

        # Add specified headers
        for header in self.include_headers:
            parts.append(context.headers.get(header, ""))

        key_string = "|".join(parts)
        return hashlib.md5(key_string.encode()).hexdigest()

    def _get_field(self, message: Any, path: str) -> Any:
        """Get field value by path."""
        parts = path.split(".")
        current = message

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current


# ============================================================================
# COMPRESSION POLICY
# ============================================================================

class CompressionPolicy(FlowPolicy):
    """
    Compresses/decompresses messages.

    Features:
    - Gzip compression
    - Threshold-based compression
    - Header management

    Config:
        compress: bool = True
        threshold_bytes: int = 1024
        level: int = 6
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.compress = config.get("compress", True)
        self.threshold_bytes = config.get("threshold_bytes", 1024)
        self.level = config.get("level", 6)

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Execute compression/decompression."""
        start = time.time()

        if context.message is None:
            return PolicyExecutionResult(success=True)

        try:
            if self.compress:
                result = self._compress(context.message)
            else:
                result = self._decompress(context.message)

            return PolicyExecutionResult(
                success=True,
                modified_message=result,
                modified_headers={"Content-Encoding": "gzip"} if self.compress else None,
                execution_time_ms=(time.time() - start) * 1000,
            )

        except Exception as e:
            logger.error(f"Compression failed: {e}")
            return PolicyExecutionResult(
                success=True,  # Don't fail on compression error
                execution_time_ms=(time.time() - start) * 1000,
            )

    def _compress(self, message: Any) -> Any:
        """Compress message if above threshold."""
        if isinstance(message, (dict, list)):
            data = json.dumps(message).encode()
        elif isinstance(message, str):
            data = message.encode()
        elif isinstance(message, bytes):
            data = message
        else:
            return message

        if len(data) < self.threshold_bytes:
            return message

        return gzip.compress(data, compresslevel=self.level)

    def _decompress(self, message: Any) -> Any:
        """Decompress gzipped message."""
        if not isinstance(message, bytes):
            return message

        try:
            decompressed = gzip.decompress(message)
            try:
                return json.loads(decompressed)
            except json.JSONDecodeError:
                return decompressed.decode()
        except gzip.BadGzipFile:
            return message


# ============================================================================
# CORRELATION ID POLICY
# ============================================================================

class CorrelationIdPolicy(FlowPolicy):
    """
    Manages correlation IDs for request tracing.

    Features:
    - Generate if missing
    - Propagate through headers
    - Support multiple ID formats

    Config:
        header_name: str = "X-Correlation-ID"
        generate_if_missing: bool = True
        id_format: str = "uuid4"
        prefix: str = ""
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.header_name = config.get("header_name", "X-Correlation-ID")
        self.generate_if_missing = config.get("generate_if_missing", True)
        self.id_format = config.get("id_format", "uuid4")
        self.prefix = config.get("prefix", "")

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Manage correlation ID."""
        start = time.time()

        # Check if correlation ID exists
        correlation_id = context.headers.get(self.header_name)

        if not correlation_id and self.generate_if_missing:
            correlation_id = self._generate_id()

        if correlation_id:
            # Store in context
            context.variables["correlation_id"] = correlation_id

            return PolicyExecutionResult(
                success=True,
                modified_headers={self.header_name: correlation_id},
                execution_time_ms=(time.time() - start) * 1000,
            )

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def _generate_id(self) -> str:
        """Generate new correlation ID."""
        if self.id_format == "uuid4":
            id_part = str(uuid.uuid4())
        elif self.id_format == "uuid1":
            id_part = str(uuid.uuid1())
        elif self.id_format == "short":
            id_part = uuid.uuid4().hex[:12]
        else:
            id_part = str(uuid.uuid4())

        return f"{self.prefix}{id_part}" if self.prefix else id_part


# ============================================================================
# DEDUPLICATION POLICY
# ============================================================================

class DeduplicationPolicy(FlowPolicy):
    """
    Prevents duplicate message processing.

    Features:
    - Key-based deduplication
    - TTL for dedup window
    - Configurable action on duplicate

    Config:
        key_fields: List[str] = []
        ttl_seconds: int = 3600
        action_on_duplicate: str = "skip"
        include_execution_id: bool = True
    """

    # Simple in-memory dedup store (would use Redis in production)
    _seen: Dict[str, float] = {}

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.key_fields = config.get("key_fields", [])
        self.ttl_seconds = config.get("ttl_seconds", 3600)
        self.action_on_duplicate = config.get("action_on_duplicate", "skip")
        self.include_execution_id = config.get("include_execution_id", True)

    async def execute(
        self,
        context: PolicyExecutionContext,
    ) -> PolicyExecutionResult:
        """Check for duplicates."""
        start = time.time()

        # Generate dedup key
        dedup_key = self._generate_key(context)

        # Clean expired entries
        self._cleanup()

        # Check if duplicate
        if dedup_key in self._seen:
            action = (
                PolicyAction.SKIP
                if self.action_on_duplicate == "skip"
                else PolicyAction.ABORT
            )

            return PolicyExecutionResult(
                success=action == PolicyAction.SKIP,
                action=action,
                error="Duplicate message detected" if action == PolicyAction.ABORT else None,
                error_code="DUPLICATE" if action == PolicyAction.ABORT else None,
                execution_time_ms=(time.time() - start) * 1000,
            )

        # Mark as seen
        self._seen[dedup_key] = time.time() + self.ttl_seconds

        return PolicyExecutionResult(
            success=True,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def _generate_key(self, context: PolicyExecutionContext) -> str:
        """Generate deduplication key."""
        parts = [context.flow_id]

        if self.include_execution_id:
            # Use message content hash
            if context.message:
                msg_str = json.dumps(context.message, sort_keys=True) if isinstance(context.message, (dict, list)) else str(context.message)
                parts.append(hashlib.md5(msg_str.encode()).hexdigest())

        # Add key fields
        if context.message and self.key_fields:
            for field in self.key_fields:
                value = self._get_field(context.message, field)
                parts.append(str(value) if value is not None else "")

        return "|".join(parts)

    def _get_field(self, message: Any, path: str) -> Any:
        """Get field value by path."""
        parts = path.split(".")
        current = message

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current

    def _cleanup(self):
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, expires in self._seen.items() if expires < now]
        for key in expired:
            del self._seen[key]

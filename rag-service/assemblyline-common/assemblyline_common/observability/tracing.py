"""
OpenTelemetry Tracing Wrapper (Advanced)

Wraps the existing tracing module with additional features for the
unified observability setup.

Advanced Features (upgraded from Required):
- Distributed tracing: Cross-service correlation with trace propagation
- Causality analysis: Root cause detection, dependency mapping
- Service dependency graph: Automatic service topology discovery
- Error correlation: Link errors across service boundaries

Usage:
    from services.shared.observability import get_tracer, trace_function

    tracer = get_tracer()
    with tracer.start_as_current_span("my_operation") as span:
        span.set_attribute("key", "value")
        # Your code
"""

import os
import logging
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar, Tuple, Set
from functools import wraps
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
from datetime import datetime, timedelta
import asyncio
import hashlib

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])

# Re-export from existing module
try:
    from services.shared.utils.opentelemetry_tracing import (
        init_telemetry,
        get_tracer as _get_tracer,
        get_meter,
        trace_function,
        inject_context,
        extract_context,
        Metrics,
        NoOpTracer,
        NoOpSpan,
    )
    TRACING_AVAILABLE = True
except ImportError:
    TRACING_AVAILABLE = False
    logger.debug("OpenTelemetry tracing module not available")


# =============================================================================
# ADVANCED: DISTRIBUTED TRACING & CAUSALITY ANALYSIS
# =============================================================================

class SpanStatus(str, Enum):
    """Status of a span for causality analysis."""
    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class SpanInfo:
    """Information about a span for correlation analysis."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    service_name: str
    operation_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    status: SpanStatus = SpanStatus.OK
    error_message: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ServiceDependency:
    """Represents a dependency between two services."""
    caller_service: str
    callee_service: str
    operation: str
    call_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    avg_duration_ms: float = 0.0
    p99_duration_ms: float = 0.0
    last_seen: Optional[datetime] = None

    def record_call(self, duration_ms: float, is_error: bool = False):
        """Record a call between services."""
        self.call_count += 1
        self.total_duration_ms += duration_ms
        self.avg_duration_ms = self.total_duration_ms / self.call_count
        if is_error:
            self.error_count += 1
        self.last_seen = datetime.utcnow()


@dataclass
class CausalityChain:
    """Chain of spans showing causality for an error or anomaly."""
    root_span: SpanInfo
    chain: List[SpanInfo]
    root_cause: Optional[SpanInfo] = None
    root_cause_confidence: float = 0.0
    affected_services: List[str] = field(default_factory=list)
    error_propagation_path: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root_trace_id": self.root_span.trace_id,
            "chain_length": len(self.chain),
            "root_cause_service": self.root_cause.service_name if self.root_cause else None,
            "root_cause_operation": self.root_cause.operation_name if self.root_cause else None,
            "root_cause_error": self.root_cause.error_message if self.root_cause else None,
            "confidence": self.root_cause_confidence,
            "affected_services": self.affected_services,
            "propagation_path": self.error_propagation_path,
        }


class ServiceDependencyGraph:
    """
    Tracks service dependencies discovered from distributed traces.

    Automatically builds a service topology map from observed traces.
    """

    def __init__(self, retention_hours: int = 24):
        self.dependencies: Dict[Tuple[str, str, str], ServiceDependency] = {}
        self.services: Set[str] = set()
        self.retention_hours = retention_hours
        self._last_cleanup = datetime.utcnow()

    def record_call(
        self,
        caller_service: str,
        callee_service: str,
        operation: str,
        duration_ms: float,
        is_error: bool = False
    ):
        """Record a call between services."""
        self.services.add(caller_service)
        self.services.add(callee_service)

        key = (caller_service, callee_service, operation)
        if key not in self.dependencies:
            self.dependencies[key] = ServiceDependency(
                caller_service=caller_service,
                callee_service=callee_service,
                operation=operation
            )

        self.dependencies[key].record_call(duration_ms, is_error)
        self._maybe_cleanup()

    def _maybe_cleanup(self):
        """Remove stale dependencies periodically."""
        now = datetime.utcnow()
        if (now - self._last_cleanup).total_seconds() < 3600:  # Every hour
            return

        cutoff = now - timedelta(hours=self.retention_hours)
        stale_keys = [
            key for key, dep in self.dependencies.items()
            if dep.last_seen and dep.last_seen < cutoff
        ]
        for key in stale_keys:
            del self.dependencies[key]
        self._last_cleanup = now

    def get_dependencies_for_service(self, service_name: str) -> Dict[str, List[ServiceDependency]]:
        """Get all dependencies for a service (callers and callees)."""
        callers = []
        callees = []

        for (caller, callee, _), dep in self.dependencies.items():
            if callee == service_name:
                callers.append(dep)
            if caller == service_name:
                callees.append(dep)

        return {"callers": callers, "callees": callees}

    def get_service_topology(self) -> Dict[str, Any]:
        """Get the full service topology graph."""
        nodes = [{"id": svc, "label": svc} for svc in self.services]
        edges = []

        for (caller, callee, op), dep in self.dependencies.items():
            edges.append({
                "source": caller,
                "target": callee,
                "operation": op,
                "call_count": dep.call_count,
                "error_rate": dep.error_count / dep.call_count if dep.call_count > 0 else 0,
                "avg_latency_ms": dep.avg_duration_ms,
            })

        return {
            "nodes": nodes,
            "edges": edges,
            "service_count": len(self.services),
            "edge_count": len(edges),
        }

    def get_critical_paths(self, min_calls: int = 10) -> List[Dict[str, Any]]:
        """Identify critical paths (high traffic, high error rate, or high latency)."""
        critical = []

        for (caller, callee, op), dep in self.dependencies.items():
            if dep.call_count < min_calls:
                continue

            error_rate = dep.error_count / dep.call_count
            is_critical = error_rate > 0.05 or dep.avg_duration_ms > 1000  # 5% errors or >1s latency

            if is_critical:
                critical.append({
                    "path": f"{caller} -> {callee}",
                    "operation": op,
                    "call_count": dep.call_count,
                    "error_rate": error_rate,
                    "avg_latency_ms": dep.avg_duration_ms,
                    "criticality": "high" if error_rate > 0.1 else "medium",
                })

        return sorted(critical, key=lambda x: x["error_rate"], reverse=True)


class CausalityAnalyzer:
    """
    Analyzes distributed traces to determine root cause of errors.

    Uses temporal ordering and error propagation patterns to identify
    the most likely root cause of failures in distributed systems.
    """

    def __init__(self, max_trace_depth: int = 50):
        self.max_trace_depth = max_trace_depth
        # Store recent traces for analysis
        self._trace_store: Dict[str, List[SpanInfo]] = defaultdict(list)
        self._error_patterns: Dict[str, int] = defaultdict(int)  # Error signature -> count
        self._max_traces = 10000

    def record_span(self, span_info: SpanInfo):
        """Record a span for later analysis."""
        self._trace_store[span_info.trace_id].append(span_info)

        # Cleanup old traces if needed
        if len(self._trace_store) > self._max_traces:
            # Remove oldest 20%
            to_remove = list(self._trace_store.keys())[:self._max_traces // 5]
            for trace_id in to_remove:
                del self._trace_store[trace_id]

        # Track error patterns
        if span_info.status == SpanStatus.ERROR and span_info.error_message:
            signature = self._get_error_signature(span_info)
            self._error_patterns[signature] += 1

    def _get_error_signature(self, span: SpanInfo) -> str:
        """Generate a signature for an error pattern."""
        parts = [span.service_name, span.operation_name, span.error_message or "unknown"]
        return hashlib.md5(":".join(parts).encode()).hexdigest()[:16]

    def analyze_trace(self, trace_id: str) -> Optional[CausalityChain]:
        """
        Analyze a trace to determine root cause of any errors.

        Args:
            trace_id: The trace ID to analyze

        Returns:
            CausalityChain if errors found, None otherwise
        """
        spans = self._trace_store.get(trace_id, [])
        if not spans:
            return None

        # Sort spans by start time
        spans = sorted(spans, key=lambda s: s.start_time)

        # Find root span (no parent or first span)
        root_span = None
        for span in spans:
            if span.parent_span_id is None:
                root_span = span
                break
        if not root_span:
            root_span = spans[0]

        # Find all error spans
        error_spans = [s for s in spans if s.status == SpanStatus.ERROR]
        if not error_spans:
            return None

        # Build span tree for causality analysis
        span_by_id = {s.span_id: s for s in spans}
        children = defaultdict(list)
        for span in spans:
            if span.parent_span_id and span.parent_span_id in span_by_id:
                children[span.parent_span_id].append(span)

        # Find root cause: earliest error that isn't caused by a child error
        root_cause = self._find_root_cause(error_spans, span_by_id, children)

        # Build error propagation path
        propagation_path = self._build_propagation_path(root_cause, error_spans, span_by_id)

        # Get affected services
        affected = list(set(s.service_name for s in error_spans))

        return CausalityChain(
            root_span=root_span,
            chain=spans,
            root_cause=root_cause,
            root_cause_confidence=self._calculate_confidence(root_cause, error_spans),
            affected_services=affected,
            error_propagation_path=propagation_path,
        )

    def _find_root_cause(
        self,
        error_spans: List[SpanInfo],
        span_by_id: Dict[str, SpanInfo],
        children: Dict[str, List[SpanInfo]]
    ) -> Optional[SpanInfo]:
        """Find the root cause error span."""
        # Sort by time - earliest error is likely root cause
        error_spans = sorted(error_spans, key=lambda s: s.start_time)

        for span in error_spans:
            # Check if any children also have errors
            child_spans = children.get(span.span_id, [])
            has_child_errors = any(c.status == SpanStatus.ERROR for c in child_spans)

            if not has_child_errors:
                # This is a leaf error - likely the root cause
                return span

        # If all errors have child errors, return the earliest
        return error_spans[0] if error_spans else None

    def _build_propagation_path(
        self,
        root_cause: Optional[SpanInfo],
        error_spans: List[SpanInfo],
        span_by_id: Dict[str, SpanInfo]
    ) -> List[str]:
        """Build the error propagation path from root cause."""
        if not root_cause:
            return []

        path = [f"{root_cause.service_name}:{root_cause.operation_name}"]

        # Find spans that happened after root cause and might be affected
        for span in sorted(error_spans, key=lambda s: s.start_time):
            if span.span_id == root_cause.span_id:
                continue
            if span.start_time >= root_cause.start_time:
                path.append(f"{span.service_name}:{span.operation_name}")

        return path

    def _calculate_confidence(
        self,
        root_cause: Optional[SpanInfo],
        error_spans: List[SpanInfo]
    ) -> float:
        """Calculate confidence in the root cause identification."""
        if not root_cause or not error_spans:
            return 0.0

        # Higher confidence if:
        # 1. Root cause is the earliest error
        # 2. Root cause has a clear error message
        # 3. This error pattern has been seen before

        confidence = 0.5  # Base confidence

        # Check if earliest
        earliest = min(error_spans, key=lambda s: s.start_time)
        if root_cause.span_id == earliest.span_id:
            confidence += 0.2

        # Check for clear error message
        if root_cause.error_message and len(root_cause.error_message) > 10:
            confidence += 0.15

        # Check if pattern seen before
        signature = self._get_error_signature(root_cause)
        if self._error_patterns.get(signature, 0) > 5:
            confidence += 0.15

        return min(confidence, 1.0)

    def get_common_errors(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Get most common error patterns across all traces."""
        sorted_patterns = sorted(
            self._error_patterns.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]

        return [
            {"signature": sig, "count": count}
            for sig, count in sorted_patterns
        ]

    def correlate_errors(
        self,
        service_name: str,
        time_window_minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Find correlated errors across services within a time window.

        Helps identify cascading failures originating from one service.
        """
        cutoff = datetime.utcnow() - timedelta(minutes=time_window_minutes)

        # Find errors in the target service
        service_errors = []
        for trace_id, spans in self._trace_store.items():
            for span in spans:
                if (span.service_name == service_name and
                    span.status == SpanStatus.ERROR and
                    span.start_time >= cutoff):
                    service_errors.append((trace_id, span))

        # Find errors in other services that happened in the same traces
        correlated = defaultdict(int)
        for trace_id, _ in service_errors:
            for span in self._trace_store.get(trace_id, []):
                if span.service_name != service_name and span.status == SpanStatus.ERROR:
                    correlated[span.service_name] += 1

        return {
            "source_service": service_name,
            "error_count": len(service_errors),
            "time_window_minutes": time_window_minutes,
            "correlated_services": dict(correlated),
            "likely_cascade": len(correlated) > 0,
        }


class DistributedTraceCorrelator:
    """
    Correlates traces across services for end-to-end visibility.

    Provides cross-service trace correlation and context propagation.
    """

    def __init__(self):
        self.dependency_graph = ServiceDependencyGraph()
        self.causality_analyzer = CausalityAnalyzer()
        self._current_trace_context: Dict[str, Dict[str, Any]] = {}

    def start_trace_context(
        self,
        trace_id: str,
        service_name: str,
        operation: str,
        parent_service: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Start a new trace context with correlation metadata.

        Returns headers to propagate to downstream services.
        """
        context = {
            "trace_id": trace_id,
            "service_name": service_name,
            "operation": operation,
            "parent_service": parent_service,
            "start_time": datetime.utcnow().isoformat(),
        }
        self._current_trace_context[trace_id] = context

        # Return headers for propagation
        return {
            "x-trace-id": trace_id,
            "x-parent-service": service_name,
            "x-parent-operation": operation,
        }

    def extract_trace_context(self, headers: Dict[str, str]) -> Optional[Dict[str, str]]:
        """Extract trace context from incoming request headers."""
        trace_id = headers.get("x-trace-id")
        if not trace_id:
            return None

        return {
            "trace_id": trace_id,
            "parent_service": headers.get("x-parent-service"),
            "parent_operation": headers.get("x-parent-operation"),
        }

    def record_span_completion(
        self,
        span_info: SpanInfo,
        parent_service: Optional[str] = None
    ):
        """Record span completion for analysis."""
        # Record in causality analyzer
        self.causality_analyzer.record_span(span_info)

        # Update dependency graph if we have parent info
        if parent_service and span_info.duration_ms is not None:
            self.dependency_graph.record_call(
                caller_service=parent_service,
                callee_service=span_info.service_name,
                operation=span_info.operation_name,
                duration_ms=span_info.duration_ms,
                is_error=span_info.status == SpanStatus.ERROR
            )

    def analyze_request(self, trace_id: str) -> Dict[str, Any]:
        """
        Analyze a complete request across all services.

        Returns comprehensive analysis including causality chain if errors exist.
        """
        causality = self.causality_analyzer.analyze_trace(trace_id)

        result = {
            "trace_id": trace_id,
            "has_errors": causality is not None,
        }

        if causality:
            result["causality_analysis"] = causality.to_dict()

        return result

    def get_service_health(self) -> Dict[str, Any]:
        """Get health metrics for all tracked services."""
        topology = self.dependency_graph.get_service_topology()
        critical_paths = self.dependency_graph.get_critical_paths()
        common_errors = self.causality_analyzer.get_common_errors()

        return {
            "topology": topology,
            "critical_paths": critical_paths,
            "common_errors": common_errors,
        }


# Global correlator instance
_trace_correlator: Optional[DistributedTraceCorrelator] = None


def get_trace_correlator() -> DistributedTraceCorrelator:
    """Get the global trace correlator instance."""
    global _trace_correlator
    if _trace_correlator is None:
        _trace_correlator = DistributedTraceCorrelator()
    return _trace_correlator


# Enhanced tracer wrapper
class TracerWrapper:
    """
    Enhanced tracer wrapper with additional convenience methods.
    """

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._tracer = None
        self._initialized = False

    def initialize(
        self,
        otlp_endpoint: Optional[str] = None,
        enable_console_export: bool = False,
        sample_rate: float = 1.0
    ) -> bool:
        """Initialize the tracer."""
        if not TRACING_AVAILABLE:
            logger.info("OpenTelemetry not available, using no-op tracer")
            return False

        try:
            success = init_telemetry(
                service_name=self.service_name,
                otlp_endpoint=otlp_endpoint,
                enable_console_export=enable_console_export,
                sample_rate=sample_rate
            )
            self._initialized = success
            if success:
                self._tracer = _get_tracer()
            return success
        except Exception as e:
            logger.error(f"Failed to initialize tracer: {e}")
            return False

    def get_tracer(self):
        """Get the underlying tracer."""
        if self._tracer:
            return self._tracer
        if TRACING_AVAILABLE:
            return _get_tracer()
        return NoOpTracerLocal()

    def start_span(self, name: str, **kwargs):
        """Start a new span."""
        return self.get_tracer().start_as_current_span(name, **kwargs)

    def trace(
        self,
        span_name: Optional[str] = None,
        record_args: bool = False,
        record_result: bool = False,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Callable[[F], F]:
        """
        Decorator to trace a function.

        Args:
            span_name: Custom span name (defaults to function name)
            record_args: Record function arguments as span attributes
            record_result: Record function result as span attribute
            attributes: Additional attributes to add to the span

        Usage:
            @tracer.trace("process_query", record_args=True)
            async def process_query(query: str):
                return "result"
        """
        def decorator(func: F) -> F:
            name = span_name or func.__name__

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                tracer = self.get_tracer()
                with tracer.start_as_current_span(name) as span:
                    if attributes:
                        for k, v in attributes.items():
                            span.set_attribute(k, v)
                    if record_args:
                        span.set_attribute("args", str(args)[:500])
                        span.set_attribute("kwargs", str(kwargs)[:500])
                    try:
                        result = func(*args, **kwargs)
                        if record_result:
                            span.set_attribute("result", str(result)[:500])
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        raise

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                tracer = self.get_tracer()
                with tracer.start_as_current_span(name) as span:
                    if attributes:
                        for k, v in attributes.items():
                            span.set_attribute(k, v)
                    if record_args:
                        span.set_attribute("args", str(args)[:500])
                        span.set_attribute("kwargs", str(kwargs)[:500])
                    try:
                        result = await func(*args, **kwargs)
                        if record_result:
                            span.set_attribute("result", str(result)[:500])
                        return result
                    except Exception as e:
                        span.record_exception(e)
                        raise

            if asyncio.iscoroutinefunction(func):
                return async_wrapper  # type: ignore
            return sync_wrapper  # type: ignore

        return decorator


# Local no-op implementations if main module not available
class NoOpTracerLocal:
    def start_as_current_span(self, name: str, **kwargs):
        return NoOpSpanLocal()

    def start_span(self, name: str, **kwargs):
        return NoOpSpanLocal()


class NoOpSpanLocal:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def set_attribute(self, key: str, value: Any):
        pass

    def add_event(self, name: str, attributes: Optional[Dict] = None):
        pass

    def set_status(self, status):
        pass

    def record_exception(self, exception):
        pass

    def end(self):
        pass


# Global tracer instance
_tracer_wrapper: Optional[TracerWrapper] = None


def get_tracer(service_name: Optional[str] = None):
    """
    Get a tracer instance.

    Args:
        service_name: Service name for the tracer

    Returns:
        Tracer instance
    """
    global _tracer_wrapper

    if _tracer_wrapper is None:
        _tracer_wrapper = TracerWrapper(service_name or "drivesentinel")

    return _tracer_wrapper.get_tracer()


def initialize_tracing(
    service_name: str,
    otlp_endpoint: Optional[str] = None,
    enable_console_export: bool = False,
    sample_rate: float = 1.0
) -> bool:
    """
    Initialize tracing for a service.

    Args:
        service_name: Name of the service
        otlp_endpoint: OTLP exporter endpoint
        enable_console_export: Enable console export for debugging
        sample_rate: Trace sampling rate (0.0 to 1.0)

    Returns:
        True if initialization succeeded
    """
    global _tracer_wrapper

    _tracer_wrapper = TracerWrapper(service_name)
    return _tracer_wrapper.initialize(
        otlp_endpoint=otlp_endpoint,
        enable_console_export=enable_console_export,
        sample_rate=sample_rate
    )


# Utility functions
def create_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """
    Create a new span.

    Args:
        name: Span name
        attributes: Span attributes

    Returns:
        Span context manager
    """
    tracer = get_tracer()
    span = tracer.start_as_current_span(name)
    if attributes and hasattr(span, "set_attribute"):
        for k, v in attributes.items():
            span.set_attribute(k, v)
    return span


def add_span_attributes(attributes: Dict[str, Any]):
    """
    Add attributes to the current span.

    Args:
        attributes: Attributes to add
    """
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            for k, v in attributes.items():
                span.set_attribute(k, v)
    except ImportError:
        pass


def add_span_event(name: str, attributes: Optional[Dict[str, Any]] = None):
    """
    Add an event to the current span.

    Args:
        name: Event name
        attributes: Event attributes
    """
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            span.add_event(name, attributes=attributes)
    except ImportError:
        pass


# FastAPI Tracing Middleware
class TracingMiddleware:
    """
    FastAPI middleware for distributed tracing.

    Creates a span for each incoming request with method, path, and status.
    Propagates trace context from incoming headers.
    """

    def __init__(self, app, service_name: str = "unknown"):
        self.app = app
        self.service_name = service_name

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        import time
        start_time = time.time()

        # Extract request info
        method = scope.get("method", "UNKNOWN")
        path = scope.get("path", "/")

        # Extract trace context from headers
        headers = dict(scope.get("headers", []))
        headers = {k.decode(): v.decode() for k, v in headers.items() if isinstance(k, bytes)}

        # Extract parent context if available
        parent_context = None
        try:
            parent_context = extract_context(headers)
        except Exception:
            pass

        # Create span
        tracer = get_tracer(self.service_name)
        span_name = f"{method} {path}"

        # Track status code
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 500)
            await send(message)

        try:
            with tracer.start_as_current_span(span_name) as span:
                # Set span attributes
                if hasattr(span, 'set_attribute'):
                    span.set_attribute("http.method", method)
                    span.set_attribute("http.url", path)
                    span.set_attribute("http.route", path)
                    span.set_attribute("service.name", self.service_name)

                await self.app(scope, receive, send_wrapper)

                # Set response attributes
                if hasattr(span, 'set_attribute'):
                    span.set_attribute("http.status_code", status_code)
                    duration_ms = (time.time() - start_time) * 1000
                    span.set_attribute("http.duration_ms", duration_ms)

                    # Set span status based on HTTP status
                    if status_code >= 400:
                        try:
                            from opentelemetry.trace import StatusCode
                            span.set_status(StatusCode.ERROR if status_code >= 500 else StatusCode.OK)
                        except ImportError:
                            pass
        except Exception as e:
            if hasattr(span, 'record_exception'):
                span.record_exception(e)
            raise


# =============================================================================
# CUTTING EDGE: AUTOMATIC BOTTLENECK DETECTION
# =============================================================================

class BottleneckType(str, Enum):
    """Types of bottlenecks."""
    LATENCY_SPIKE = "latency_spike"
    HIGH_ERROR_RATE = "high_error_rate"
    RESOURCE_CONTENTION = "resource_contention"
    CASCADING_FAILURE = "cascading_failure"
    THROUGHPUT_DEGRADATION = "throughput_degradation"
    TIMEOUT_CLUSTER = "timeout_cluster"


class BottleneckSeverity(str, Enum):
    """Severity levels for bottlenecks."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class LatencyProfile:
    """Latency statistics for an operation."""
    operation: str
    service: str
    sample_count: int
    mean_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    std_dev_ms: float
    trend: str  # increasing, stable, decreasing
    last_updated: datetime


@dataclass
class DetectedBottleneck:
    """A detected performance bottleneck."""
    bottleneck_id: str
    bottleneck_type: BottleneckType
    severity: BottleneckSeverity
    service: str
    operation: str
    description: str
    impact_score: float  # 0-1
    confidence: float
    evidence: List[str]
    suggested_actions: List[str]
    detected_at: datetime
    resolved: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bottleneck_id": self.bottleneck_id,
            "type": self.bottleneck_type.value,
            "severity": self.severity.value,
            "service": self.service,
            "operation": self.operation,
            "description": self.description,
            "impact_score": self.impact_score,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "suggested_actions": self.suggested_actions,
            "detected_at": self.detected_at.isoformat(),
            "resolved": self.resolved,
        }


class AutomaticBottleneckDetector:
    """
    ML-based automatic bottleneck detection.

    Features:
    - Real-time latency analysis with statistical methods
    - Anomaly detection using Z-scores and IQR
    - Pattern recognition for cascading failures
    - Throughput monitoring and degradation detection
    - Automatic severity assessment
    """

    def __init__(
        self,
        latency_threshold_ms: float = 500,
        error_rate_threshold: float = 0.05,
        anomaly_z_threshold: float = 2.5,
        window_size: int = 100
    ):
        self.latency_threshold_ms = latency_threshold_ms
        self.error_rate_threshold = error_rate_threshold
        self.anomaly_z_threshold = anomaly_z_threshold
        self.window_size = window_size

        # Latency tracking per operation
        self._latency_windows: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)
        self._error_windows: Dict[str, List[Tuple[datetime, bool]]] = defaultdict(list)

        # Latency profiles cache
        self._latency_profiles: Dict[str, LatencyProfile] = {}

        # Detected bottlenecks
        self._bottlenecks: List[DetectedBottleneck] = []

        # Baseline statistics (learned over time)
        self._baselines: Dict[str, Dict[str, float]] = {}

        # Cascade detection
        self._service_error_times: Dict[str, List[datetime]] = defaultdict(list)

    def record_span_metrics(
        self,
        service: str,
        operation: str,
        duration_ms: float,
        is_error: bool = False,
        timestamp: Optional[datetime] = None
    ):
        """Record span metrics for bottleneck detection."""
        ts = timestamp or datetime.utcnow()
        key = f"{service}:{operation}"

        # Update latency window
        self._latency_windows[key].append((ts, duration_ms))
        if len(self._latency_windows[key]) > self.window_size * 2:
            self._latency_windows[key] = self._latency_windows[key][-self.window_size:]

        # Update error window
        self._error_windows[key].append((ts, is_error))
        if len(self._error_windows[key]) > self.window_size * 2:
            self._error_windows[key] = self._error_windows[key][-self.window_size:]

        # Track service error times for cascade detection
        if is_error:
            self._service_error_times[service].append(ts)
            # Cleanup old error times
            cutoff = ts - timedelta(minutes=5)
            self._service_error_times[service] = [
                t for t in self._service_error_times[service] if t > cutoff
            ]

        # Update latency profile
        self._update_latency_profile(key, service, operation)

        # Check for bottlenecks
        self._detect_bottlenecks(key, service, operation, duration_ms, is_error, ts)

    def _update_latency_profile(self, key: str, service: str, operation: str):
        """Update latency profile for an operation."""
        latencies = [l for _, l in self._latency_windows[key]]
        if len(latencies) < 10:
            return

        import statistics

        sorted_latencies = sorted(latencies)
        n = len(sorted_latencies)

        mean = statistics.mean(latencies)
        median = statistics.median(latencies)
        std_dev = statistics.stdev(latencies) if n > 1 else 0

        p95_idx = int(n * 0.95)
        p99_idx = int(n * 0.99)

        # Calculate trend
        if n >= 20:
            first_half = statistics.mean(latencies[:n//2])
            second_half = statistics.mean(latencies[n//2:])
            if second_half > first_half * 1.2:
                trend = "increasing"
            elif second_half < first_half * 0.8:
                trend = "decreasing"
            else:
                trend = "stable"
        else:
            trend = "stable"

        self._latency_profiles[key] = LatencyProfile(
            operation=operation,
            service=service,
            sample_count=n,
            mean_ms=mean,
            median_ms=median,
            p95_ms=sorted_latencies[p95_idx] if p95_idx < n else sorted_latencies[-1],
            p99_ms=sorted_latencies[p99_idx] if p99_idx < n else sorted_latencies[-1],
            min_ms=min(latencies),
            max_ms=max(latencies),
            std_dev_ms=std_dev,
            trend=trend,
            last_updated=datetime.utcnow(),
        )

        # Update baseline
        if key not in self._baselines:
            self._baselines[key] = {
                "mean": mean,
                "std_dev": std_dev,
                "samples": n,
            }
        else:
            # Exponential moving average for baseline
            alpha = 0.1
            self._baselines[key]["mean"] = alpha * mean + (1 - alpha) * self._baselines[key]["mean"]
            self._baselines[key]["std_dev"] = alpha * std_dev + (1 - alpha) * self._baselines[key]["std_dev"]
            self._baselines[key]["samples"] += 1

    def _detect_bottlenecks(
        self,
        key: str,
        service: str,
        operation: str,
        duration_ms: float,
        is_error: bool,
        timestamp: datetime
    ):
        """Detect bottlenecks based on current metrics."""
        bottlenecks = []

        # 1. Latency spike detection using Z-score
        if key in self._baselines and self._baselines[key]["samples"] > 20:
            baseline = self._baselines[key]
            if baseline["std_dev"] > 0:
                z_score = (duration_ms - baseline["mean"]) / baseline["std_dev"]
                if z_score > self.anomaly_z_threshold:
                    bottlenecks.append(self._create_bottleneck(
                        bottleneck_type=BottleneckType.LATENCY_SPIKE,
                        service=service,
                        operation=operation,
                        description=f"Latency spike detected: {duration_ms:.0f}ms (Z-score: {z_score:.2f})",
                        impact_score=min(1.0, z_score / 5),
                        confidence=0.7 + min(0.25, baseline["samples"] / 1000),
                        evidence=[
                            f"Current: {duration_ms:.0f}ms",
                            f"Baseline mean: {baseline['mean']:.0f}ms",
                            f"Z-score: {z_score:.2f} (threshold: {self.anomaly_z_threshold})",
                        ],
                        suggested_actions=[
                            "Check for resource contention",
                            "Review recent deployments",
                            "Examine downstream service health",
                        ],
                    ))

        # 2. Error rate detection
        errors = [e for _, e in self._error_windows[key]]
        if len(errors) >= 10:
            error_rate = sum(errors) / len(errors)
            if error_rate > self.error_rate_threshold:
                bottlenecks.append(self._create_bottleneck(
                    bottleneck_type=BottleneckType.HIGH_ERROR_RATE,
                    service=service,
                    operation=operation,
                    description=f"High error rate: {error_rate*100:.1f}%",
                    impact_score=min(1.0, error_rate * 2),
                    confidence=0.8,
                    evidence=[
                        f"Error rate: {error_rate*100:.1f}%",
                        f"Threshold: {self.error_rate_threshold*100:.1f}%",
                        f"Sample size: {len(errors)}",
                    ],
                    suggested_actions=[
                        "Review error logs for root cause",
                        "Check service dependencies",
                        "Consider circuit breaker activation",
                    ],
                ))

        # 3. Cascading failure detection
        cascade = self._detect_cascade(service, timestamp)
        if cascade:
            bottlenecks.append(cascade)

        # 4. Throughput degradation (if profile exists and trend is increasing)
        if key in self._latency_profiles:
            profile = self._latency_profiles[key]
            if profile.trend == "increasing" and profile.mean_ms > self.latency_threshold_ms:
                bottlenecks.append(self._create_bottleneck(
                    bottleneck_type=BottleneckType.THROUGHPUT_DEGRADATION,
                    service=service,
                    operation=operation,
                    description=f"Throughput degradation: latency increasing trend",
                    impact_score=min(1.0, profile.mean_ms / (self.latency_threshold_ms * 2)),
                    confidence=0.65,
                    evidence=[
                        f"Mean latency: {profile.mean_ms:.0f}ms",
                        f"P99 latency: {profile.p99_ms:.0f}ms",
                        f"Trend: {profile.trend}",
                    ],
                    suggested_actions=[
                        "Scale up resources",
                        "Optimize hot paths",
                        "Add caching layer",
                    ],
                ))

        # Store new bottlenecks (deduplicate by type+service+operation)
        for b in bottlenecks:
            existing = next(
                (eb for eb in self._bottlenecks
                 if eb.bottleneck_type == b.bottleneck_type
                 and eb.service == b.service
                 and eb.operation == b.operation
                 and not eb.resolved
                 and (datetime.utcnow() - eb.detected_at).total_seconds() < 300),
                None
            )
            if not existing:
                self._bottlenecks.append(b)

    def _detect_cascade(self, service: str, timestamp: datetime) -> Optional[DetectedBottleneck]:
        """Detect cascading failure patterns."""
        window = timedelta(minutes=2)
        cutoff = timestamp - window

        # Check if multiple services have errors in a short time window
        services_with_errors = []
        for svc, error_times in self._service_error_times.items():
            recent_errors = [t for t in error_times if t > cutoff]
            if len(recent_errors) >= 3:
                services_with_errors.append((svc, len(recent_errors)))

        if len(services_with_errors) >= 3:
            # Likely cascading failure
            services_with_errors.sort(key=lambda x: x[1], reverse=True)
            primary_service = services_with_errors[0][0]

            return self._create_bottleneck(
                bottleneck_type=BottleneckType.CASCADING_FAILURE,
                service=primary_service,
                operation="*",
                description=f"Cascading failure detected across {len(services_with_errors)} services",
                impact_score=min(1.0, len(services_with_errors) / 5),
                confidence=0.75,
                evidence=[
                    f"Affected services: {', '.join(s[0] for s in services_with_errors)}",
                    f"Time window: {window.total_seconds():.0f}s",
                    f"Primary source: {primary_service}",
                ],
                suggested_actions=[
                    f"Investigate {primary_service} first",
                    "Check shared dependencies",
                    "Consider activating circuit breakers",
                    "Review service mesh health",
                ],
            )

        return None

    def _create_bottleneck(
        self,
        bottleneck_type: BottleneckType,
        service: str,
        operation: str,
        description: str,
        impact_score: float,
        confidence: float,
        evidence: List[str],
        suggested_actions: List[str],
    ) -> DetectedBottleneck:
        """Create a bottleneck detection."""
        # Determine severity based on impact
        if impact_score >= 0.8:
            severity = BottleneckSeverity.CRITICAL
        elif impact_score >= 0.5:
            severity = BottleneckSeverity.HIGH
        elif impact_score >= 0.3:
            severity = BottleneckSeverity.MEDIUM
        else:
            severity = BottleneckSeverity.LOW

        return DetectedBottleneck(
            bottleneck_id=f"btn_{hashlib.md5(f'{bottleneck_type.value}_{service}_{operation}_{datetime.utcnow().isoformat()}'.encode()).hexdigest()[:12]}",
            bottleneck_type=bottleneck_type,
            severity=severity,
            service=service,
            operation=operation,
            description=description,
            impact_score=impact_score,
            confidence=confidence,
            evidence=evidence,
            suggested_actions=suggested_actions,
            detected_at=datetime.utcnow(),
        )

    def get_active_bottlenecks(self, max_age_minutes: int = 30) -> List[Dict[str, Any]]:
        """Get active (unresolved) bottlenecks."""
        cutoff = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        active = [
            b for b in self._bottlenecks
            if not b.resolved and b.detected_at > cutoff
        ]
        return [b.to_dict() for b in sorted(active, key=lambda x: x.impact_score, reverse=True)]

    def get_latency_profiles(self) -> List[Dict[str, Any]]:
        """Get all latency profiles."""
        return [
            {
                "key": key,
                "service": p.service,
                "operation": p.operation,
                "mean_ms": p.mean_ms,
                "median_ms": p.median_ms,
                "p95_ms": p.p95_ms,
                "p99_ms": p.p99_ms,
                "std_dev_ms": p.std_dev_ms,
                "trend": p.trend,
                "sample_count": p.sample_count,
            }
            for key, p in self._latency_profiles.items()
        ]

    def mark_resolved(self, bottleneck_id: str):
        """Mark a bottleneck as resolved."""
        for b in self._bottlenecks:
            if b.bottleneck_id == bottleneck_id:
                b.resolved = True
                break


# =============================================================================
# CUTTING EDGE: SELF-OPTIMIZING INSTRUMENTATION
# =============================================================================

class InstrumentationDecision(str, Enum):
    """Decisions about instrumentation."""
    ADD_TRACE_POINT = "add_trace_point"
    REMOVE_TRACE_POINT = "remove_trace_point"
    INCREASE_SAMPLING = "increase_sampling"
    DECREASE_SAMPLING = "decrease_sampling"
    ADD_ATTRIBUTES = "add_attributes"
    REDUCE_ATTRIBUTES = "reduce_attributes"


@dataclass
class TracePoint:
    """A trace point configuration."""
    trace_point_id: str
    service: str
    operation: str
    enabled: bool
    sampling_rate: float  # 0.0 to 1.0
    attribute_level: str  # minimal, standard, verbose
    added_at: datetime
    usage_count: int = 0
    useful_count: int = 0  # times it helped identify issues
    overhead_ms: float = 0.0
    last_decision: Optional[InstrumentationDecision] = None
    auto_managed: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_point_id": self.trace_point_id,
            "service": self.service,
            "operation": self.operation,
            "enabled": self.enabled,
            "sampling_rate": self.sampling_rate,
            "attribute_level": self.attribute_level,
            "usage_count": self.usage_count,
            "useful_count": self.useful_count,
            "overhead_ms": self.overhead_ms,
            "usefulness_ratio": self.useful_count / max(self.usage_count, 1),
            "auto_managed": self.auto_managed,
        }


@dataclass
class InstrumentationChange:
    """A record of an instrumentation change."""
    change_id: str
    trace_point_id: str
    decision: InstrumentationDecision
    reason: str
    old_value: Any
    new_value: Any
    timestamp: datetime
    impact_assessment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "change_id": self.change_id,
            "trace_point_id": self.trace_point_id,
            "decision": self.decision.value,
            "reason": self.reason,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "timestamp": self.timestamp.isoformat(),
            "impact_assessment": self.impact_assessment,
        }


class SelfOptimizingInstrumentation:
    """
    Self-optimizing instrumentation system.

    Features:
    - Automatic trace point management
    - Dynamic sampling rate adjustment
    - Overhead-aware instrumentation
    - Usefulness tracking and optimization
    - Cold/hot path detection
    """

    def __init__(
        self,
        max_overhead_percent: float = 1.0,
        min_sampling_rate: float = 0.001,
        max_sampling_rate: float = 1.0,
        optimization_interval_seconds: int = 300
    ):
        self.max_overhead_percent = max_overhead_percent
        self.min_sampling_rate = min_sampling_rate
        self.max_sampling_rate = max_sampling_rate
        self.optimization_interval_seconds = optimization_interval_seconds

        # Trace points
        self._trace_points: Dict[str, TracePoint] = {}

        # Change history
        self._changes: List[InstrumentationChange] = []

        # Performance tracking
        self._operation_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {
            "total_calls": 0,
            "traced_calls": 0,
            "total_overhead_ms": 0,
            "errors_detected": 0,
            "bottlenecks_detected": 0,
        })

        # Hot paths (frequently called operations)
        self._hot_paths: Set[str] = set()

        # Last optimization time
        self._last_optimization = datetime.utcnow()

    def get_or_create_trace_point(
        self,
        service: str,
        operation: str,
        initial_sampling_rate: float = 0.1
    ) -> TracePoint:
        """Get or create a trace point for an operation."""
        key = f"{service}:{operation}"

        if key not in self._trace_points:
            self._trace_points[key] = TracePoint(
                trace_point_id=f"tp_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                service=service,
                operation=operation,
                enabled=True,
                sampling_rate=initial_sampling_rate,
                attribute_level="standard",
                added_at=datetime.utcnow(),
            )

            self._record_change(
                trace_point_id=self._trace_points[key].trace_point_id,
                decision=InstrumentationDecision.ADD_TRACE_POINT,
                reason="New operation discovered",
                old_value=None,
                new_value=initial_sampling_rate,
            )

        return self._trace_points[key]

    def should_trace(self, service: str, operation: str) -> Tuple[bool, TracePoint]:
        """
        Determine if a call should be traced based on current configuration.

        Returns:
            Tuple of (should_trace, trace_point)
        """
        tp = self.get_or_create_trace_point(service, operation)

        if not tp.enabled:
            return False, tp

        # Random sampling
        import random
        should = random.random() < tp.sampling_rate

        # Update stats
        key = f"{service}:{operation}"
        self._operation_stats[key]["total_calls"] += 1
        if should:
            self._operation_stats[key]["traced_calls"] += 1
            tp.usage_count += 1

        return should, tp

    def record_trace_outcome(
        self,
        service: str,
        operation: str,
        overhead_ms: float,
        detected_issue: bool = False,
        issue_type: Optional[str] = None
    ):
        """Record the outcome of a trace for optimization."""
        key = f"{service}:{operation}"
        tp = self._trace_points.get(key)

        if not tp:
            return

        # Update overhead tracking
        self._operation_stats[key]["total_overhead_ms"] += overhead_ms
        tp.overhead_ms = (tp.overhead_ms * 0.9) + (overhead_ms * 0.1)  # EMA

        # Track if trace was useful (detected an issue)
        if detected_issue:
            tp.useful_count += 1
            if issue_type == "error":
                self._operation_stats[key]["errors_detected"] += 1
            elif issue_type == "bottleneck":
                self._operation_stats[key]["bottlenecks_detected"] += 1

        # Check if we need to run optimization
        self._maybe_optimize()

    def _maybe_optimize(self):
        """Run optimization if enough time has passed."""
        now = datetime.utcnow()
        if (now - self._last_optimization).total_seconds() < self.optimization_interval_seconds:
            return

        self._last_optimization = now
        self._run_optimization()

    def _run_optimization(self):
        """Run the optimization algorithm."""
        # 1. Identify hot paths
        self._identify_hot_paths()

        # 2. Optimize each trace point
        for key, tp in self._trace_points.items():
            if not tp.auto_managed:
                continue

            stats = self._operation_stats[key]
            self._optimize_trace_point(tp, stats)

    def _identify_hot_paths(self):
        """Identify hot paths (frequently called operations)."""
        if not self._operation_stats:
            return

        # Get total calls across all operations
        total_calls = sum(s["total_calls"] for s in self._operation_stats.values())
        if total_calls == 0:
            return

        # Hot paths are operations that account for >5% of total calls
        self._hot_paths = set()
        for key, stats in self._operation_stats.items():
            if stats["total_calls"] / total_calls > 0.05:
                self._hot_paths.add(key)

    def _optimize_trace_point(self, tp: TracePoint, stats: Dict[str, float]):
        """Optimize a single trace point."""
        key = f"{tp.service}:{tp.operation}"
        is_hot_path = key in self._hot_paths

        # Calculate usefulness ratio
        usefulness_ratio = tp.useful_count / max(tp.usage_count, 1)

        # Calculate overhead ratio
        avg_overhead = stats["total_overhead_ms"] / max(stats["traced_calls"], 1)

        decisions = []

        # Decision 1: Adjust sampling rate based on usefulness
        if usefulness_ratio < 0.01 and tp.usage_count > 100:
            # Low usefulness, reduce sampling
            new_rate = max(self.min_sampling_rate, tp.sampling_rate * 0.5)
            if new_rate != tp.sampling_rate:
                decisions.append((
                    InstrumentationDecision.DECREASE_SAMPLING,
                    f"Low usefulness ratio ({usefulness_ratio:.3f})",
                    tp.sampling_rate,
                    new_rate,
                ))
                tp.sampling_rate = new_rate

        elif usefulness_ratio > 0.1 and tp.usage_count > 50:
            # High usefulness, increase sampling
            new_rate = min(self.max_sampling_rate, tp.sampling_rate * 1.5)
            if new_rate != tp.sampling_rate:
                decisions.append((
                    InstrumentationDecision.INCREASE_SAMPLING,
                    f"High usefulness ratio ({usefulness_ratio:.3f})",
                    tp.sampling_rate,
                    new_rate,
                ))
                tp.sampling_rate = new_rate

        # Decision 2: Adjust for hot paths
        if is_hot_path and tp.sampling_rate > 0.1:
            # Hot paths should have lower sampling to reduce overhead
            new_rate = max(self.min_sampling_rate, tp.sampling_rate * 0.3)
            decisions.append((
                InstrumentationDecision.DECREASE_SAMPLING,
                f"Hot path detected, reducing overhead",
                tp.sampling_rate,
                new_rate,
            ))
            tp.sampling_rate = new_rate

        # Decision 3: Adjust attribute level based on overhead
        if avg_overhead > 10 and tp.attribute_level == "verbose":
            decisions.append((
                InstrumentationDecision.REDUCE_ATTRIBUTES,
                f"High overhead ({avg_overhead:.1f}ms), reducing attributes",
                "verbose",
                "standard",
            ))
            tp.attribute_level = "standard"

        elif avg_overhead < 1 and usefulness_ratio > 0.05 and tp.attribute_level == "minimal":
            decisions.append((
                InstrumentationDecision.ADD_ATTRIBUTES,
                f"Low overhead, increasing attributes for better diagnostics",
                "minimal",
                "standard",
            ))
            tp.attribute_level = "standard"

        # Decision 4: Disable low-value trace points
        if tp.usage_count > 1000 and tp.useful_count == 0:
            decisions.append((
                InstrumentationDecision.REMOVE_TRACE_POINT,
                f"No useful data after {tp.usage_count} traces",
                tp.enabled,
                False,
            ))
            tp.enabled = False

        # Record all decisions
        for decision, reason, old_val, new_val in decisions:
            self._record_change(
                trace_point_id=tp.trace_point_id,
                decision=decision,
                reason=reason,
                old_value=old_val,
                new_value=new_val,
            )
            tp.last_decision = decision

    def _record_change(
        self,
        trace_point_id: str,
        decision: InstrumentationDecision,
        reason: str,
        old_value: Any,
        new_value: Any
    ):
        """Record an instrumentation change."""
        change = InstrumentationChange(
            change_id=f"chg_{hashlib.md5(f'{trace_point_id}_{datetime.utcnow().isoformat()}'.encode()).hexdigest()[:12]}",
            trace_point_id=trace_point_id,
            decision=decision,
            reason=reason,
            old_value=old_value,
            new_value=new_value,
            timestamp=datetime.utcnow(),
        )
        self._changes.append(change)

        # Keep only recent changes
        if len(self._changes) > 1000:
            self._changes = self._changes[-500:]

    def get_trace_points(self) -> List[Dict[str, Any]]:
        """Get all trace points."""
        return [tp.to_dict() for tp in self._trace_points.values()]

    def get_recent_changes(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent instrumentation changes."""
        return [c.to_dict() for c in self._changes[-limit:]]

    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        total_trace_points = len(self._trace_points)
        enabled_points = sum(1 for tp in self._trace_points.values() if tp.enabled)
        auto_managed = sum(1 for tp in self._trace_points.values() if tp.auto_managed)

        avg_sampling = sum(tp.sampling_rate for tp in self._trace_points.values()) / max(total_trace_points, 1)
        total_overhead = sum(s["total_overhead_ms"] for s in self._operation_stats.values())
        total_traced = sum(s["traced_calls"] for s in self._operation_stats.values())

        return {
            "total_trace_points": total_trace_points,
            "enabled_trace_points": enabled_points,
            "auto_managed_points": auto_managed,
            "average_sampling_rate": avg_sampling,
            "total_overhead_ms": total_overhead,
            "total_traced_calls": total_traced,
            "avg_overhead_per_trace_ms": total_overhead / max(total_traced, 1),
            "hot_paths": list(self._hot_paths),
            "total_changes": len(self._changes),
        }

    def force_enable(self, service: str, operation: str, sampling_rate: float = 1.0):
        """Forcefully enable a trace point (disable auto-management)."""
        tp = self.get_or_create_trace_point(service, operation, sampling_rate)
        tp.enabled = True
        tp.sampling_rate = sampling_rate
        tp.auto_managed = False

        self._record_change(
            trace_point_id=tp.trace_point_id,
            decision=InstrumentationDecision.ADD_TRACE_POINT,
            reason="Manual override",
            old_value=None,
            new_value=sampling_rate,
        )

    def force_disable(self, service: str, operation: str):
        """Forcefully disable a trace point."""
        key = f"{service}:{operation}"
        if key in self._trace_points:
            tp = self._trace_points[key]
            tp.enabled = False
            tp.auto_managed = False

            self._record_change(
                trace_point_id=tp.trace_point_id,
                decision=InstrumentationDecision.REMOVE_TRACE_POINT,
                reason="Manual disable",
                old_value=True,
                new_value=False,
            )


# =============================================================================
# CUTTING EDGE: INTEGRATED TRACING SYSTEM
# =============================================================================

class CuttingEdgeTracing:
    """
    Cutting-edge tracing system with automatic optimization.

    Combines:
    - Distributed trace correlation
    - Automatic bottleneck detection
    - Self-optimizing instrumentation
    """

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.correlator = get_trace_correlator()
        self.bottleneck_detector = AutomaticBottleneckDetector()
        self.instrumentation = SelfOptimizingInstrumentation()
        self._tracer = TracerWrapper(service_name)

    def initialize(
        self,
        otlp_endpoint: Optional[str] = None,
        enable_console_export: bool = False,
        sample_rate: float = 1.0
    ) -> bool:
        """Initialize the cutting-edge tracing system."""
        return self._tracer.initialize(
            otlp_endpoint=otlp_endpoint,
            enable_console_export=enable_console_export,
            sample_rate=sample_rate,
        )

    def trace(
        self,
        operation: str,
        parent_service: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """
        Context manager for tracing an operation.

        Automatically handles:
        - Sampling decisions
        - Bottleneck detection
        - Overhead tracking
        """
        return CuttingEdgeTraceContext(
            tracing_system=self,
            operation=operation,
            parent_service=parent_service,
            attributes=attributes,
        )

    def get_diagnostics(self) -> Dict[str, Any]:
        """Get comprehensive diagnostics."""
        return {
            "service": self.service_name,
            "bottlenecks": self.bottleneck_detector.get_active_bottlenecks(),
            "latency_profiles": self.bottleneck_detector.get_latency_profiles(),
            "instrumentation": {
                "trace_points": self.instrumentation.get_trace_points(),
                "stats": self.instrumentation.get_optimization_stats(),
                "recent_changes": self.instrumentation.get_recent_changes(20),
            },
            "service_health": self.correlator.get_service_health(),
        }


class CuttingEdgeTraceContext:
    """Context manager for cutting-edge tracing."""

    def __init__(
        self,
        tracing_system: CuttingEdgeTracing,
        operation: str,
        parent_service: Optional[str],
        attributes: Optional[Dict[str, Any]]
    ):
        self.tracing_system = tracing_system
        self.operation = operation
        self.parent_service = parent_service
        self.attributes = attributes or {}

        self._span = None
        self._should_trace = False
        self._trace_point = None
        self._start_time = None
        self._is_error = False

    def __enter__(self):
        # Check if we should trace
        self._should_trace, self._trace_point = self.tracing_system.instrumentation.should_trace(
            self.tracing_system.service_name,
            self.operation,
        )

        self._start_time = time.time()

        if self._should_trace:
            tracer = self.tracing_system._tracer.get_tracer()
            self._span = tracer.start_as_current_span(self.operation).__enter__()

            if self._span and hasattr(self._span, 'set_attribute'):
                self._span.set_attribute("service.name", self.tracing_system.service_name)
                self._span.set_attribute("sampling.rate", self._trace_point.sampling_rate)
                for k, v in self.attributes.items():
                    self._span.set_attribute(k, v)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self._start_time) * 1000
        self._is_error = exc_type is not None

        # Record metrics for bottleneck detection
        self.tracing_system.bottleneck_detector.record_span_metrics(
            service=self.tracing_system.service_name,
            operation=self.operation,
            duration_ms=duration_ms,
            is_error=self._is_error,
        )

        # Record trace outcome for instrumentation optimization
        if self._should_trace:
            # Check if this trace detected any issues
            bottlenecks = self.tracing_system.bottleneck_detector.get_active_bottlenecks(max_age_minutes=1)
            detected_issue = self._is_error or any(
                b["operation"] == self.operation for b in bottlenecks
            )

            self.tracing_system.instrumentation.record_trace_outcome(
                service=self.tracing_system.service_name,
                operation=self.operation,
                overhead_ms=0.1,  # Estimated overhead
                detected_issue=detected_issue,
                issue_type="error" if self._is_error else "bottleneck" if detected_issue else None,
            )

            if self._span:
                if self._is_error and hasattr(self._span, 'record_exception'):
                    self._span.record_exception(exc_val)
                if hasattr(self._span, '__exit__'):
                    self._span.__exit__(exc_type, exc_val, exc_tb)

        return False

    def set_attribute(self, key: str, value: Any):
        """Set an attribute on the span."""
        if self._span and hasattr(self._span, 'set_attribute'):
            self._span.set_attribute(key, value)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Add an event to the span."""
        if self._span and hasattr(self._span, 'add_event'):
            self._span.add_event(name, attributes=attributes)


# Factory functions
_cutting_edge_tracing: Dict[str, CuttingEdgeTracing] = {}


def get_cutting_edge_tracing(service_name: str) -> CuttingEdgeTracing:
    """Get or create a cutting-edge tracing instance."""
    if service_name not in _cutting_edge_tracing:
        _cutting_edge_tracing[service_name] = CuttingEdgeTracing(service_name)
    return _cutting_edge_tracing[service_name]


# =============================================================================
# GENAI OTEL SEMANTIC CONVENTIONS
# =============================================================================
# Follows OpenTelemetry GenAI semantic conventions:
# https://opentelemetry.io/docs/specs/semconv/gen-ai/

def trace_llm_call(
    system: str = "ollama",
    operation: str = "chat",
):
    """
    Decorator that adds GenAI semantic convention attributes to LLM calls.

    Attributes added:
    - gen_ai.system: The AI system (ollama, openai, anthropic)
    - gen_ai.request.model: Model name requested
    - gen_ai.request.max_tokens: Max tokens requested
    - gen_ai.request.temperature: Temperature setting
    - gen_ai.usage.input_tokens: Prompt tokens used
    - gen_ai.usage.output_tokens: Completion tokens used
    - gen_ai.response.finish_reason: Why generation stopped

    Usage:
        @trace_llm_call(system="ollama", operation="generate")
        async def generate(self, prompt, model_override=None, ...):
            ...
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer("genai")

            # Extract model name from kwargs or args
            model = kwargs.get("model_override") or kwargs.get("model")
            max_tokens = kwargs.get("max_tokens_override") or kwargs.get("max_tokens")
            temperature = kwargs.get("temperature_override") or kwargs.get("temperature")

            # Try to get model from self.config if available
            self_arg = args[0] if args else None
            if model is None and self_arg and hasattr(self_arg, "config"):
                model = getattr(self_arg.config, "model_id", None)
            if max_tokens is None and self_arg and hasattr(self_arg, "config"):
                max_tokens = getattr(self_arg.config, "max_tokens", None)
            if temperature is None and self_arg and hasattr(self_arg, "config"):
                temperature = getattr(self_arg.config, "temperature", None)

            span_name = f"gen_ai.{operation}"
            try:
                with tracer.start_as_current_span(span_name) as span:
                    # Request attributes
                    span.set_attribute("gen_ai.system", system)
                    span.set_attribute("gen_ai.operation.name", operation)
                    if model:
                        span.set_attribute("gen_ai.request.model", str(model))
                    if max_tokens is not None:
                        span.set_attribute("gen_ai.request.max_tokens", int(max_tokens))
                    if temperature is not None:
                        span.set_attribute("gen_ai.request.temperature", float(temperature))

                    start = time.time()
                    result = await func(*args, **kwargs)
                    duration_ms = (time.time() - start) * 1000

                    # Response attributes from result metadata
                    if hasattr(result, "metadata") and isinstance(result.metadata, dict):
                        meta = result.metadata
                        if "eval_count" in meta:
                            span.set_attribute("gen_ai.usage.output_tokens", meta["eval_count"])
                        if "prompt_eval_count" in meta:
                            span.set_attribute("gen_ai.usage.input_tokens", meta["prompt_eval_count"])
                        if "provider" in meta:
                            span.set_attribute("gen_ai.system", meta["provider"])

                    if hasattr(result, "model_used") and result.model_used:
                        span.set_attribute("gen_ai.response.model", result.model_used)

                    if hasattr(result, "success"):
                        span.set_attribute("gen_ai.response.finish_reason",
                                           "stop" if result.success else "error")

                    span.set_attribute("gen_ai.response.duration_ms", duration_ms)

                    return result

            except Exception as e:
                # Record error on span if available
                logger.error(f"GenAI call failed: {e}")
                raise

        return wrapper  # type: ignore
    return decorator

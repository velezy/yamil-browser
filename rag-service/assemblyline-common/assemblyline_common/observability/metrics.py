"""
Prometheus Metrics Service

Provides application metrics for monitoring:
- Request rate, latency, error rate (RED metrics)
- Connection pool utilization
- Circuit breaker state
- Custom business metrics
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class MetricsConfig:
    """Configuration for metrics collection."""

    # Service identification
    service_name: str = "logic-weaver"
    environment: str = "development"

    # Prometheus settings
    enable_prometheus: bool = True
    metrics_port: int = 9090
    metrics_path: str = "/metrics"

    # Histogram buckets for latency
    latency_buckets: tuple = (
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0
    )

    # Default labels
    default_labels: Dict[str, str] = field(default_factory=dict)


# ============================================================================
# Metric Types
# ============================================================================

class MetricType(str, Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


# ============================================================================
# In-Memory Metrics (Fallback when Prometheus not installed)
# ============================================================================

class InMemoryMetrics:
    """Simple in-memory metrics storage."""

    def __init__(self):
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}

    def inc_counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        key = self._make_key(name, labels)
        self._counters[key] = self._counters.get(key, 0) + value

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        key = self._make_key(name, labels)
        self._gauges[key] = value

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        key = self._make_key(name, labels)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)

    def _make_key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        if labels:
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{name}{{{label_str}}}"
        return name

    def get_all(self) -> Dict[str, Any]:
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {k: {"count": len(v), "sum": sum(v)} for k, v in self._histograms.items()},
        }


# ============================================================================
# Metrics Service
# ============================================================================

class MetricsService:
    """
    Service for collecting and exposing Prometheus metrics.

    Provides standard RED metrics (Rate, Errors, Duration) plus
    custom business metrics.
    """

    def __init__(self, config: Optional[MetricsConfig] = None):
        self.config = config or MetricsConfig()
        self._prometheus_available = False
        self._metrics = InMemoryMetrics()
        self._prometheus_metrics: Dict[str, Any] = {}

        self._initialize_prometheus()

    def _initialize_prometheus(self):
        """Initialize Prometheus metrics if available."""
        if not self.config.enable_prometheus:
            return

        try:
            from prometheus_client import Counter, Gauge, Histogram, REGISTRY

            # Request metrics
            self._prometheus_metrics["request_total"] = Counter(
                "http_requests_total",
                "Total HTTP requests",
                ["service", "method", "endpoint", "status"],
            )

            self._prometheus_metrics["request_duration"] = Histogram(
                "http_request_duration_seconds",
                "HTTP request duration in seconds",
                ["service", "method", "endpoint"],
                buckets=self.config.latency_buckets,
            )

            self._prometheus_metrics["request_in_progress"] = Gauge(
                "http_requests_in_progress",
                "HTTP requests currently in progress",
                ["service", "method", "endpoint"],
            )

            # Error metrics
            self._prometheus_metrics["errors_total"] = Counter(
                "errors_total",
                "Total errors",
                ["service", "error_type"],
            )

            # Connection pool metrics
            self._prometheus_metrics["pool_connections"] = Gauge(
                "connection_pool_connections",
                "Number of connections in pool",
                ["service", "pool_name", "state"],
            )

            # Circuit breaker metrics
            self._prometheus_metrics["circuit_breaker_state"] = Gauge(
                "circuit_breaker_state",
                "Circuit breaker state (0=closed, 1=open, 2=half_open)",
                ["service", "circuit_name"],
            )

            self._prometheus_metrics["circuit_breaker_failures"] = Counter(
                "circuit_breaker_failures_total",
                "Total circuit breaker failures",
                ["service", "circuit_name"],
            )

            # Business metrics
            self._prometheus_metrics["messages_processed"] = Counter(
                "messages_processed_total",
                "Total messages processed",
                ["service", "message_type", "status"],
            )

            self._prometheus_metrics["flow_executions"] = Counter(
                "flow_executions_total",
                "Total flow executions",
                ["service", "flow_id", "status"],
            )

            self._prometheus_metrics["phi_access"] = Counter(
                "phi_access_total",
                "Total PHI access events",
                ["service", "action", "phi_type"],
            )

            # Flow execution duration (histogram for p50/p95/p99 SLOs)
            self._prometheus_metrics["flow_execution_duration"] = Histogram(
                "flow_execution_duration_seconds",
                "Flow execution duration in seconds",
                ["service", "flow_name", "status"],
                buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
            )

            # Connector calls (by type for per-connector visibility)
            self._prometheus_metrics["connector_calls"] = Counter(
                "connector_calls_total",
                "Total outbound connector calls",
                ["service", "connector_type", "connector_name", "status"],
            )

            # DB pool active connections (real-time gauge)
            self._prometheus_metrics["db_pool_active"] = Gauge(
                "db_pool_active_connections",
                "Active database pool connections",
                ["service"],
            )

            self._prometheus_available = True
            logger.info(
                "Prometheus metrics initialized",
                extra={"event_type": "metrics.initialized"}
            )

        except ImportError:
            logger.warning(
                "prometheus_client not installed, using in-memory metrics",
                extra={"event_type": "metrics.fallback"}
            )

    # =========================================================================
    # Request Metrics
    # =========================================================================

    def track_request_start(self, method: str, endpoint: str):
        """Track the start of an HTTP request."""
        labels = {"service": self.config.service_name, "method": method, "endpoint": endpoint}

        if self._prometheus_available:
            self._prometheus_metrics["request_in_progress"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("request_in_progress", 1, labels)

    def track_request_end(
        self,
        method: str,
        endpoint: str,
        status_code: int,
        duration_seconds: float,
    ):
        """Track the end of an HTTP request."""
        labels = {"service": self.config.service_name, "method": method, "endpoint": endpoint}

        if self._prometheus_available:
            self._prometheus_metrics["request_in_progress"].labels(**labels).dec()
            self._prometheus_metrics["request_total"].labels(
                **labels, status=str(status_code)
            ).inc()
            self._prometheus_metrics["request_duration"].labels(**labels).observe(duration_seconds)
        else:
            self._metrics.inc_counter("request_in_progress", -1, labels)
            self._metrics.inc_counter("request_total", 1, {**labels, "status": str(status_code)})
            self._metrics.observe_histogram("request_duration", duration_seconds, labels)

    # =========================================================================
    # Error Metrics
    # =========================================================================

    def track_error(self, error_type: str):
        """Track an error occurrence."""
        labels = {"service": self.config.service_name, "error_type": error_type}

        if self._prometheus_available:
            self._prometheus_metrics["errors_total"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("errors_total", 1, labels)

    # =========================================================================
    # Connection Pool Metrics
    # =========================================================================

    def set_pool_connections(self, pool_name: str, active: int, idle: int, total: int):
        """Set connection pool metrics."""
        base_labels = {"service": self.config.service_name, "pool_name": pool_name}

        if self._prometheus_available:
            self._prometheus_metrics["pool_connections"].labels(**base_labels, state="active").set(active)
            self._prometheus_metrics["pool_connections"].labels(**base_labels, state="idle").set(idle)
            self._prometheus_metrics["pool_connections"].labels(**base_labels, state="total").set(total)
        else:
            self._metrics.set_gauge("pool_connections_active", active, base_labels)
            self._metrics.set_gauge("pool_connections_idle", idle, base_labels)
            self._metrics.set_gauge("pool_connections_total", total, base_labels)

    # =========================================================================
    # Circuit Breaker Metrics
    # =========================================================================

    def set_circuit_breaker_state(self, circuit_name: str, state: str):
        """Set circuit breaker state (closed=0, open=1, half_open=2)."""
        state_map = {"closed": 0, "open": 1, "half_open": 2}
        state_value = state_map.get(state.lower(), 0)

        labels = {"service": self.config.service_name, "circuit_name": circuit_name}

        if self._prometheus_available:
            self._prometheus_metrics["circuit_breaker_state"].labels(**labels).set(state_value)
        else:
            self._metrics.set_gauge("circuit_breaker_state", state_value, labels)

    def track_circuit_breaker_failure(self, circuit_name: str):
        """Track a circuit breaker failure."""
        labels = {"service": self.config.service_name, "circuit_name": circuit_name}

        if self._prometheus_available:
            self._prometheus_metrics["circuit_breaker_failures"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("circuit_breaker_failures", 1, labels)

    # =========================================================================
    # Business Metrics
    # =========================================================================

    def track_message_processed(self, message_type: str, status: str = "success"):
        """Track a message being processed."""
        labels = {
            "service": self.config.service_name,
            "message_type": message_type,
            "status": status,
        }

        if self._prometheus_available:
            self._prometheus_metrics["messages_processed"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("messages_processed", 1, labels)

    def track_flow_execution(self, flow_id: str, status: str):
        """Track a flow execution."""
        labels = {
            "service": self.config.service_name,
            "flow_id": flow_id,
            "status": status,
        }

        if self._prometheus_available:
            self._prometheus_metrics["flow_executions"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("flow_executions", 1, labels)

    def track_phi_access(self, action: str, phi_type: str):
        """Track PHI access for compliance."""
        labels = {
            "service": self.config.service_name,
            "action": action,
            "phi_type": phi_type,
        }

        if self._prometheus_available:
            self._prometheus_metrics["phi_access"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("phi_access", 1, labels)

    def track_flow_execution_duration(
        self, flow_name: str, duration_seconds: float, status: str = "success"
    ):
        """Track flow execution duration for SLO monitoring."""
        labels = {
            "service": self.config.service_name,
            "flow_name": flow_name,
            "status": status,
        }

        if self._prometheus_available:
            self._prometheus_metrics["flow_execution_duration"].labels(**labels).observe(
                duration_seconds
            )
        else:
            self._metrics.observe_histogram(
                "flow_execution_duration", duration_seconds, labels
            )

    def track_connector_call(
        self, connector_type: str, connector_name: str, status: str = "success"
    ):
        """Track an outbound connector call."""
        labels = {
            "service": self.config.service_name,
            "connector_type": connector_type,
            "connector_name": connector_name,
            "status": status,
        }

        if self._prometheus_available:
            self._prometheus_metrics["connector_calls"].labels(**labels).inc()
        else:
            self._metrics.inc_counter("connector_calls", 1, labels)

    def set_db_pool_active(self, active_count: int):
        """Set the current number of active DB pool connections."""
        labels = {"service": self.config.service_name}

        if self._prometheus_available:
            self._prometheus_metrics["db_pool_active"].labels(**labels).set(active_count)
        else:
            self._metrics.set_gauge("db_pool_active_connections", active_count, labels)

    # =========================================================================
    # Custom Metrics
    # =========================================================================

    def increment(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        self._metrics.inc_counter(name, value, labels)

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        self._metrics.set_gauge(name, value, labels)

    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe a histogram value."""
        self._metrics.observe_histogram(name, value, labels)

    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics (for non-Prometheus export)."""
        return self._metrics.get_all()


# ============================================================================
# Decorator for timing functions
# ============================================================================

def timed(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """
    Decorator to time function execution.

    Usage:
        @timed("process_order_duration")
        async def process_order(order_id: str):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                duration = time.time() - start
                if _metrics_service:
                    _metrics_service.observe(metric_name, duration, labels)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.time() - start
                if _metrics_service:
                    _metrics_service.observe(metric_name, duration, labels)

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# ============================================================================
# Singleton Factory
# ============================================================================

_metrics_service: Optional[MetricsService] = None


async def get_metrics_service(
    config: Optional[MetricsConfig] = None
) -> MetricsService:
    """Get singleton instance of metrics service."""
    global _metrics_service

    if _metrics_service is None:
        _metrics_service = MetricsService(config)

    return _metrics_service

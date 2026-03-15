"""
Distributed Tracing with OpenTelemetry

Provides end-to-end request tracing across microservices:
- Automatic trace context propagation
- Span creation and management
- Integration with FastAPI, SQLAlchemy, Redis, Kafka
- Export to Jaeger, Zipkin, or OTLP collectors
- TraceLogFilter: injects trace_id/span_id into every log record
"""

import functools
import logging
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable, TypeVar, ParamSpec
from uuid import UUID, uuid4

logger = logging.getLogger(__name__)


# ============================================================================
# Trace-correlated Log Filter
# ============================================================================

class TraceLogFilter(logging.Filter):
    """
    Logging filter that injects trace_id and span_id into every log record.

    Reads from OpenTelemetry context if available, falls back to SpanContext.
    This allows log aggregation tools (CloudWatch, Datadog, Grafana) to
    correlate log entries with distributed traces.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        trace_id = None
        span_id = None

        # Try OpenTelemetry first
        try:
            from opentelemetry import trace as otel_trace
            span = otel_trace.get_current_span()
            ctx = span.get_span_context()
            if ctx and ctx.trace_id:
                trace_id = format(ctx.trace_id, "032x")
                span_id = format(ctx.span_id, "016x")
        except Exception:
            pass

        # Fall back to SpanContext
        if not trace_id:
            trace_id = SpanContext.get_trace_id() or ""
            span_id = SpanContext.get_span_id() or ""

        record.trace_id = trace_id
        record.span_id = span_id
        return True


def install_trace_log_filter(logger_name: str = "") -> None:
    """
    Install the trace log filter on a logger (default: root logger).

    Call once at service startup, after install_phi_log_filter().
    """
    target = logging.getLogger(logger_name)
    for existing in target.filters:
        if isinstance(existing, TraceLogFilter):
            return
    target.addFilter(TraceLogFilter())

# Type variables for decorators
P = ParamSpec('P')
T = TypeVar('T')


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class TelemetryConfig:
    """Configuration for distributed tracing."""

    # Service identification
    service_name: str = "logic-weaver"
    service_version: str = "1.0.0"
    environment: str = "development"

    # OTLP exporter configuration
    otlp_endpoint: Optional[str] = None  # e.g., "http://jaeger:4317"
    otlp_insecure: bool = True

    # Sampling
    sample_rate: float = 1.0  # 1.0 = 100% sampling

    # Headers to propagate
    propagate_headers: list = field(default_factory=lambda: [
        "traceparent",
        "tracestate",
        "x-correlation-id",
        "x-request-id",
    ])

    # Enable/disable instrumentation
    instrument_fastapi: bool = True
    instrument_sqlalchemy: bool = True
    instrument_redis: bool = True
    instrument_httpx: bool = True
    instrument_kafka: bool = True


# ============================================================================
# Span Context
# ============================================================================

class SpanContext:
    """
    Context holder for current span and trace information.

    Thread-local storage for the current trace/span context.
    """

    _current_trace_id: Optional[str] = None
    _current_span_id: Optional[str] = None
    _current_correlation_id: Optional[UUID] = None
    _spans: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def get_trace_id(cls) -> Optional[str]:
        return cls._current_trace_id

    @classmethod
    def get_span_id(cls) -> Optional[str]:
        return cls._current_span_id

    @classmethod
    def get_correlation_id(cls) -> Optional[UUID]:
        return cls._current_correlation_id

    @classmethod
    def set_correlation_id(cls, correlation_id: UUID):
        cls._current_correlation_id = correlation_id

    @classmethod
    def start_span(
        cls,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Start a new span and return its ID."""
        span_id = str(uuid4())[:16]

        if cls._current_trace_id is None:
            cls._current_trace_id = str(uuid4()).replace("-", "")

        cls._spans[span_id] = {
            "name": name,
            "trace_id": cls._current_trace_id,
            "parent_span_id": cls._current_span_id,
            "start_time": datetime.now(timezone.utc),
            "attributes": attributes or {},
            "events": [],
            "status": "OK",
        }

        cls._current_span_id = span_id
        return span_id

    @classmethod
    def end_span(cls, span_id: str, status: str = "OK", error: Optional[str] = None):
        """End a span."""
        if span_id in cls._spans:
            span = cls._spans[span_id]
            span["end_time"] = datetime.now(timezone.utc)
            span["duration_ms"] = int(
                (span["end_time"] - span["start_time"]).total_seconds() * 1000
            )
            span["status"] = status
            if error:
                span["error"] = error

            # Restore parent span
            cls._current_span_id = span.get("parent_span_id")

    @classmethod
    def add_event(cls, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Add an event to the current span."""
        if cls._current_span_id and cls._current_span_id in cls._spans:
            cls._spans[cls._current_span_id]["events"].append({
                "name": name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "attributes": attributes or {},
            })

    @classmethod
    def set_attribute(cls, key: str, value: Any):
        """Set an attribute on the current span."""
        if cls._current_span_id and cls._current_span_id in cls._spans:
            cls._spans[cls._current_span_id]["attributes"][key] = value

    @classmethod
    def get_current_span(cls) -> Optional[Dict[str, Any]]:
        """Get the current span data."""
        if cls._current_span_id and cls._current_span_id in cls._spans:
            return cls._spans[cls._current_span_id]
        return None

    @classmethod
    def clear(cls):
        """Clear all trace context."""
        cls._current_trace_id = None
        cls._current_span_id = None
        cls._current_correlation_id = None
        cls._spans.clear()


# ============================================================================
# Telemetry Service
# ============================================================================

class TelemetryService:
    """
    Service for distributed tracing using OpenTelemetry.

    Provides:
    - Trace context propagation
    - Span creation and management
    - Integration with various libraries
    """

    def __init__(self, config: Optional[TelemetryConfig] = None):
        self.config = config or TelemetryConfig()
        self._initialized = False
        self._tracer = None

    async def initialize(self):
        """Initialize OpenTelemetry tracing."""
        if self._initialized:
            return

        try:
            # Try to import opentelemetry
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.resources import Resource

            # Create resource
            resource = Resource.create({
                "service.name": self.config.service_name,
                "service.version": self.config.service_version,
                "deployment.environment": self.config.environment,
            })

            # Create tracer provider
            provider = TracerProvider(resource=resource)

            # Add OTLP exporter if configured
            if self.config.otlp_endpoint:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                exporter = OTLPSpanExporter(
                    endpoint=self.config.otlp_endpoint,
                    insecure=self.config.otlp_insecure,
                )
                provider.add_span_processor(BatchSpanProcessor(exporter))

            # Set global tracer provider
            trace.set_tracer_provider(provider)
            self._tracer = trace.get_tracer(self.config.service_name)

            # Instrument libraries
            await self._instrument_libraries()

            self._initialized = True
            logger.info(
                "OpenTelemetry tracing initialized",
                extra={
                    "event_type": "telemetry.initialized",
                    "service_name": self.config.service_name,
                    "otlp_endpoint": self.config.otlp_endpoint,
                }
            )

        except ImportError:
            logger.warning(
                "OpenTelemetry not installed, using basic tracing",
                extra={"event_type": "telemetry.fallback"}
            )
            self._initialized = True

    async def _instrument_libraries(self):
        """Instrument common libraries for automatic tracing."""
        if self.config.instrument_fastapi:
            try:
                from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
                FastAPIInstrumentor().instrument()
            except ImportError:
                pass

        if self.config.instrument_sqlalchemy:
            try:
                from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
                SQLAlchemyInstrumentor().instrument()
            except ImportError:
                pass

        if self.config.instrument_redis:
            try:
                from opentelemetry.instrumentation.redis import RedisInstrumentor
                RedisInstrumentor().instrument()
            except ImportError:
                pass

        if self.config.instrument_httpx:
            try:
                from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
                HTTPXClientInstrumentor().instrument()
            except ImportError:
                pass

    @contextmanager
    def start_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """
        Start a new span as a context manager.

        Usage:
            with telemetry.start_span("process_message") as span:
                span.set_attribute("message_id", msg_id)
                process_message(msg)
        """
        span_id = SpanContext.start_span(name, attributes)

        try:
            if self._tracer:
                from opentelemetry import trace
                with self._tracer.start_as_current_span(name) as otel_span:
                    if attributes:
                        for k, v in attributes.items():
                            otel_span.set_attribute(k, str(v))
                    yield otel_span
            else:
                yield SpanContext.get_current_span()
        except Exception as e:
            SpanContext.end_span(span_id, "ERROR", str(e))
            raise
        else:
            SpanContext.end_span(span_id)

    @asynccontextmanager
    async def start_span_async(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Async version of start_span."""
        with self.start_span(name, attributes) as span:
            yield span

    def inject_context(self, headers: Dict[str, str]) -> Dict[str, str]:
        """
        Inject trace context into outgoing headers.

        Use when making HTTP requests to propagate trace context.
        """
        if self._tracer:
            from opentelemetry.propagate import inject
            inject(headers)
        else:
            # Fallback: inject correlation ID
            if SpanContext.get_correlation_id():
                headers["x-correlation-id"] = str(SpanContext.get_correlation_id())
            if SpanContext.get_trace_id():
                headers["x-trace-id"] = SpanContext.get_trace_id()

        return headers

    def extract_context(self, headers: Dict[str, str]):
        """
        Extract trace context from incoming headers.

        Use when receiving HTTP requests to continue the trace.
        """
        if self._tracer:
            from opentelemetry.propagate import extract
            return extract(headers)
        else:
            # Fallback: extract correlation ID
            correlation_id = headers.get("x-correlation-id")
            if correlation_id:
                try:
                    SpanContext.set_correlation_id(UUID(correlation_id))
                except ValueError:
                    pass

        return None


# ============================================================================
# Decorators
# ============================================================================

def trace_async(
    name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
):
    """
    Decorator to trace an async function.

    Usage:
        @trace_async("process_order")
        async def process_order(order_id: str):
            ...
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        span_name = name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            span_id = SpanContext.start_span(span_name, attributes)
            try:
                result = await func(*args, **kwargs)
                SpanContext.end_span(span_id)
                return result
            except Exception as e:
                SpanContext.end_span(span_id, "ERROR", str(e))
                raise

        return wrapper
    return decorator


def trace_sync(
    name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
):
    """
    Decorator to trace a sync function.

    Usage:
        @trace_sync("validate_input")
        def validate_input(data: dict):
            ...
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        span_name = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            span_id = SpanContext.start_span(span_name, attributes)
            try:
                result = func(*args, **kwargs)
                SpanContext.end_span(span_id)
                return result
            except Exception as e:
                SpanContext.end_span(span_id, "ERROR", str(e))
                raise

        return wrapper
    return decorator


# ============================================================================
# Singleton Factory
# ============================================================================

_telemetry_service: Optional[TelemetryService] = None


async def get_telemetry_service(
    config: Optional[TelemetryConfig] = None
) -> TelemetryService:
    """Get singleton instance of telemetry service."""
    global _telemetry_service

    if _telemetry_service is None:
        _telemetry_service = TelemetryService(config)
        await _telemetry_service.initialize()

    return _telemetry_service

"""
OpenTelemetry Distributed Tracing
Provides distributed tracing across all DriveSentinel microservices.

Usage:
    from assemblyline_common.utils.opentelemetry_tracing import init_telemetry, get_tracer

    # Initialize at service startup
    init_telemetry(service_name="orchestrator")

    # Use tracer
    tracer = get_tracer()
    with tracer.start_as_current_span("operation_name") as span:
        span.set_attribute("key", "value")
        # Your code here
"""

import os
import logging
from typing import Optional, Dict, Any
from functools import wraps
import asyncio

logger = logging.getLogger(__name__)

# OpenTelemetry components
_tracer = None
_meter = None
_initialized = False


def init_telemetry(
    service_name: str,
    otlp_endpoint: Optional[str] = None,
    enable_console_export: bool = False,
    sample_rate: float = 1.0
) -> bool:
    """
    Initialize OpenTelemetry tracing and metrics.

    Args:
        service_name: Name of the service (e.g., "orchestrator", "rag", "chat")
        otlp_endpoint: OTLP exporter endpoint (e.g., "http://localhost:4317")
        enable_console_export: Also export to console for debugging
        sample_rate: Sampling rate (0.0 to 1.0)

    Returns:
        True if initialization succeeded
    """
    global _tracer, _meter, _initialized

    if _initialized:
        return True

    try:
        from opentelemetry import trace, metrics
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME

        # Create resource with service info
        resource = Resource.create({
            SERVICE_NAME: service_name,
            "service.version": "2.0.0",
            "deployment.environment": os.getenv("ENVIRONMENT", "development")
        })

        # Set up tracer provider with sampling
        sampler = TraceIdRatioBased(sample_rate)
        tracer_provider = TracerProvider(resource=resource, sampler=sampler)

        # Set up exporters
        exporters = []

        # OTLP exporter (for Jaeger, Zipkin, etc.)
        endpoint = otlp_endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        if endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                otlp_exporter = OTLPSpanExporter(endpoint=endpoint)
                tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
                exporters.append("OTLP")
            except ImportError:
                logger.warning("OTLP exporter not available. Install: pip install opentelemetry-exporter-otlp")

        # Console exporter for debugging
        if enable_console_export:
            try:
                from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
                tracer_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
                exporters.append("Console")
            except ImportError:
                pass

        # Langfuse LLM observability (opt-in via USE_LANGFUSE=true)
        try:
            from assemblyline_common.observability.langfuse_integration import init_langfuse_tracing
            if init_langfuse_tracing(tracer_provider):
                exporters.append("Langfuse")
        except ImportError:
            pass  # langfuse not installed
        except Exception as e:
            logger.debug(f"Langfuse integration skipped: {e}")

        # Register the tracer provider
        trace.set_tracer_provider(tracer_provider)
        _tracer = trace.get_tracer(service_name)

        # Set up meter provider for metrics
        meter_provider = MeterProvider(resource=resource)
        metrics.set_meter_provider(meter_provider)
        _meter = metrics.get_meter(service_name)

        _initialized = True
        logger.info(f"OpenTelemetry initialized for {service_name} with exporters: {exporters}")
        return True

    except ImportError as e:
        logger.warning(f"OpenTelemetry not available: {e}. Install: pip install opentelemetry-sdk")
        return False
    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry: {e}")
        return False


def get_tracer():
    """Get the configured tracer, or a no-op tracer if not initialized."""
    global _tracer

    if _tracer:
        return _tracer

    try:
        from opentelemetry import trace
        return trace.get_tracer(__name__)
    except ImportError:
        return NoOpTracer()


def get_meter():
    """Get the configured meter, or a no-op meter if not initialized."""
    global _meter

    if _meter:
        return _meter

    try:
        from opentelemetry import metrics
        return metrics.get_meter(__name__)
    except ImportError:
        return NoOpMeter()


class NoOpTracer:
    """No-op tracer for when OpenTelemetry is not available."""

    def start_as_current_span(self, name: str, **kwargs):
        return NoOpSpan()

    def start_span(self, name: str, **kwargs):
        return NoOpSpan()


class NoOpSpan:
    """No-op span for when OpenTelemetry is not available."""

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


class NoOpMeter:
    """No-op meter for when OpenTelemetry is not available."""

    def create_counter(self, name: str, **kwargs):
        return NoOpCounter()

    def create_histogram(self, name: str, **kwargs):
        return NoOpHistogram()

    def create_up_down_counter(self, name: str, **kwargs):
        return NoOpCounter()


class NoOpCounter:
    def add(self, value: int, attributes: Optional[Dict] = None):
        pass


class NoOpHistogram:
    def record(self, value: float, attributes: Optional[Dict] = None):
        pass


def trace_function(
    span_name: Optional[str] = None,
    record_args: bool = False,
    record_result: bool = False
):
    """
    Decorator to trace function execution.

    Args:
        span_name: Custom span name (defaults to function name)
        record_args: Whether to record function arguments as attributes
        record_result: Whether to record function result as attribute

    Usage:
        @trace_function("process_query", record_args=True)
        async def process_query(query: str):
            return "result"
    """
    def decorator(func):
        name = span_name or func.__name__

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            with tracer.start_as_current_span(name) as span:
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
            tracer = get_tracer()
            with tracer.start_as_current_span(name) as span:
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
            return async_wrapper
        return sync_wrapper

    return decorator


# Pre-defined metrics for common operations
class Metrics:
    """Common metrics for DriveSentinel services."""

    _counters = {}
    _histograms = {}

    @classmethod
    def get_counter(cls, name: str, description: str = "") -> Any:
        if name not in cls._counters:
            meter = get_meter()
            cls._counters[name] = meter.create_counter(
                name,
                description=description
            )
        return cls._counters[name]

    @classmethod
    def get_histogram(cls, name: str, description: str = "", unit: str = "ms") -> Any:
        if name not in cls._histograms:
            meter = get_meter()
            cls._histograms[name] = meter.create_histogram(
                name,
                description=description,
                unit=unit
            )
        return cls._histograms[name]

    @classmethod
    def record_request(cls, service: str, endpoint: str, status: str = "success"):
        """Record an API request."""
        counter = cls.get_counter(
            "http_requests_total",
            "Total HTTP requests"
        )
        counter.add(1, {"service": service, "endpoint": endpoint, "status": status})

    @classmethod
    def record_latency(cls, service: str, operation: str, duration_ms: float):
        """Record operation latency."""
        histogram = cls.get_histogram(
            "operation_duration_ms",
            "Operation duration in milliseconds"
        )
        histogram.record(duration_ms, {"service": service, "operation": operation})

    @classmethod
    def record_rag_retrieval(cls, num_results: int, latency_ms: float):
        """Record RAG retrieval metrics."""
        cls.get_counter("rag_retrievals_total", "Total RAG retrievals").add(1)
        cls.get_histogram("rag_retrieval_latency_ms", "RAG retrieval latency").record(latency_ms)
        cls.get_histogram("rag_results_count", "Number of RAG results").record(num_results)

    @classmethod
    def record_llm_call(cls, model: str, tokens: int, latency_ms: float):
        """Record LLM call metrics."""
        cls.get_counter("llm_calls_total", "Total LLM calls").add(1, {"model": model})
        cls.get_histogram("llm_tokens_used", "Tokens used per call").record(tokens, {"model": model})
        cls.get_histogram("llm_latency_ms", "LLM call latency").record(latency_ms, {"model": model})


# Context propagation helpers
def inject_context(headers: Dict[str, str]) -> Dict[str, str]:
    """Inject trace context into HTTP headers for propagation."""
    try:
        from opentelemetry.propagate import inject
        inject(headers)
    except ImportError:
        pass
    return headers


def extract_context(headers: Dict[str, str]):
    """Extract trace context from incoming HTTP headers."""
    try:
        from opentelemetry.propagate import extract
        return extract(headers)
    except ImportError:
        return None

"""
Observability Module for ECS

Works with AWS managed services:
- CloudWatch Logs (always - via stdout)
- Amazon Managed Prometheus (optional - install prometheus-client)
- AWS X-Ray (optional - install opentelemetry packages)

Usage:
    from assemblyline_common.observability import setup_observability

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await setup_observability(app, "my-service", 8001)
        yield

    app = FastAPI(lifespan=lifespan)

For Prometheus/OpenTelemetry, install with:
    pip install -e ../../shared/python[observability]
"""

from assemblyline_common.observability.setup import (
    ObservabilityConfig,
    setup_observability,
    add_metrics_middleware,
    add_health_endpoint,
)

# Optional exports - only available if packages installed
try:
    from assemblyline_common.observability.telemetry import (
        TelemetryConfig,
        TelemetryService,
        get_telemetry_service,
        trace_async,
        trace_sync,
        TraceLogFilter,
        install_trace_log_filter,
    )
except ImportError:
    TelemetryConfig = None
    TelemetryService = None
    get_telemetry_service = None
    trace_async = None
    trace_sync = None
    TraceLogFilter = None
    install_trace_log_filter = None

try:
    from assemblyline_common.observability.metrics import (
        MetricsConfig,
        MetricsService,
        get_metrics_service,
    )
except ImportError:
    MetricsConfig = None
    MetricsService = None
    get_metrics_service = None

# Structured logging (from legacy services/shared/observability)
try:
    from assemblyline_common.observability.structured_logging import (
        get_logger,
        configure_logging,
    )
except ImportError:
    import logging as _logging
    get_logger = _logging.getLogger
    configure_logging = None

__all__ = [
    # Always available
    "ObservabilityConfig",
    "setup_observability",
    "add_metrics_middleware",
    "add_health_endpoint",
    "get_logger",
    "configure_logging",
    # Optional - Telemetry (for X-Ray)
    "TelemetryConfig",
    "TelemetryService",
    "get_telemetry_service",
    "trace_async",
    "trace_sync",
    # Optional - Metrics (for AMP)
    "MetricsConfig",
    "MetricsService",
    "get_metrics_service",
]

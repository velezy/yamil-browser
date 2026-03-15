"""
Observability Setup for ECS

Works with AWS managed services:
- Standard Python logging (stdout → CloudWatch Logs)
- Health endpoints for ECS health checks
- Optional: Prometheus metrics (for Amazon Managed Prometheus)
- Optional: OpenTelemetry tracing (for AWS X-Ray via ADOT)

The optional features are enabled when their packages are installed.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from fastapi import FastAPI, Response

logger = logging.getLogger(__name__)


@dataclass
class ObservabilityConfig:
    """Configuration for observability components."""

    # Service identification
    service_name: str = "logic-weaver"
    service_version: str = "1.0.0"
    environment: str = "development"
    service_port: int = 8000

    # Logging
    log_level: str = "INFO"

    # Optional: OpenTelemetry (for X-Ray via ADOT)
    enable_tracing: bool = True
    otlp_endpoint: Optional[str] = None  # e.g., "http://localhost:4317"

    # Optional: Prometheus (for Amazon Managed Prometheus)
    enable_metrics: bool = True
    metrics_path: str = "/metrics"

    # Request logging
    enable_request_logging: bool = True
    log_request_body: bool = False
    log_response_body: bool = False

    # PHI Masking
    enable_phi_masking: bool = True
    enable_structured_logging: bool = True


async def setup_observability(
    app: FastAPI,
    service_name: str,
    service_port: int = 8000,
    config: Optional[ObservabilityConfig] = None,
) -> None:
    """
    Initialize observability for a FastAPI service.

    Works in ECS with:
    - CloudWatch Logs (always - via stdout)
    - Amazon Managed Prometheus (optional - if prometheus-client installed)
    - AWS X-Ray (optional - if opentelemetry installed)

    Args:
        app: FastAPI application instance
        service_name: Name of the service (e.g., "auth-service")
        service_port: Port the service runs on
        config: Optional configuration override
    """
    if config is None:
        config = ObservabilityConfig(
            service_name=service_name,
            service_port=service_port,
        )

    # Store config on app state
    app.state.observability_config = config

    # 1. Setup logging (always works - CloudWatch Logs)
    _setup_logging(config)

    # 2. Setup OpenTelemetry tracing (optional - for X-Ray)
    if config.enable_tracing:
        await _setup_tracing(config)

    # 3. Setup Prometheus metrics (optional - for AMP)
    if config.enable_metrics:
        await _setup_metrics(app, config)

    # 4. Add health endpoints (always - for ECS health checks)
    _add_health_endpoints(app, service_name)

    logger.info(f"Observability initialized for {service_name}")


def _setup_logging(config: ObservabilityConfig) -> None:
    """Setup standard Python logging for CloudWatch."""
    log_format = (
        f"%(asctime)s | {config.service_name} | %(levelname)s | %(name)s"
        f" | trace=%(trace_id)s span=%(span_id)s | %(message)s"
    )

    # Set defaults so records without trace filter don't crash
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        if not hasattr(record, "trace_id"):
            record.trace_id = ""
        if not hasattr(record, "span_id"):
            record.span_id = ""
        return record

    logging.setLogRecordFactory(record_factory)

    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Reduce uvicorn noise
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


async def _setup_tracing(config: ObservabilityConfig) -> None:
    """Setup OpenTelemetry tracing (optional - for X-Ray via ADOT)."""
    try:
        from assemblyline_common.observability.telemetry import (
            TelemetryConfig,
            get_telemetry_service,
        )

        telemetry_config = TelemetryConfig(
            service_name=config.service_name,
            service_version=config.service_version,
            environment=config.environment,
            otlp_endpoint=config.otlp_endpoint,
        )

        await get_telemetry_service(telemetry_config)
        logger.info("OpenTelemetry tracing initialized (X-Ray compatible)")
    except ImportError:
        logger.debug("OpenTelemetry not installed - tracing disabled")
    except Exception as e:
        logger.warning(f"OpenTelemetry setup failed: {e}")


async def _setup_metrics(app: FastAPI, config: ObservabilityConfig) -> None:
    """Setup Prometheus metrics (optional - for Amazon Managed Prometheus)."""
    try:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from assemblyline_common.observability.metrics import (
            MetricsConfig,
            MetricsService,
        )

        metrics_config = MetricsConfig(
            service_name=config.service_name,
            environment=config.environment,
        )

        metrics_service = MetricsService(metrics_config)
        app.state.metrics_service = metrics_service

        @app.get(config.metrics_path, include_in_schema=False)
        async def metrics_endpoint():
            """Prometheus metrics endpoint for AMP scraping."""
            return Response(
                content=generate_latest(),
                media_type=CONTENT_TYPE_LATEST,
            )

        logger.info(f"Prometheus metrics enabled at {config.metrics_path}")
    except ImportError:
        # Prometheus not installed - add simple endpoint
        @app.get(config.metrics_path, include_in_schema=False)
        async def metrics_endpoint():
            return {"message": "Install prometheus-client for metrics"}

        logger.debug("prometheus-client not installed - metrics disabled")
    except Exception as e:
        logger.warning(f"Prometheus setup failed: {e}")


def _add_health_endpoints(app: FastAPI, service_name: str) -> None:
    """Add health check endpoints for ECS."""

    @app.get("/health", include_in_schema=False)
    async def health_check():
        return {"status": "healthy", "service": service_name}

    @app.get("/ready", include_in_schema=False)
    async def readiness_check():
        return {"status": "ready", "service": service_name}


# Backward compatibility
async def add_metrics_middleware(app: FastAPI) -> None:
    """Add request metrics middleware (if prometheus installed)."""
    try:
        import time
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.requests import Request

        class MetricsMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                start_time = time.time()
                metrics_service = getattr(request.app.state, "metrics_service", None)

                if metrics_service:
                    metrics_service.track_request_start(
                        method=request.method,
                        endpoint=request.url.path,
                    )

                try:
                    response = await call_next(request)
                    status_code = response.status_code
                except Exception:
                    status_code = 500
                    raise
                finally:
                    duration = time.time() - start_time
                    if metrics_service:
                        metrics_service.track_request_end(
                            method=request.method,
                            endpoint=request.url.path,
                            status_code=status_code,
                            duration_seconds=duration,
                        )

                return response

        app.add_middleware(MetricsMiddleware)
    except Exception:
        pass


def add_health_endpoint(app: FastAPI, service_name: str) -> None:
    """Add health endpoints."""
    _add_health_endpoints(app, service_name)

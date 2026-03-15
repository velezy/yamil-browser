"""
Trace Platform Integration
Supports LangSmith, Phoenix (Arize), and Weights & Biases for observability.

Usage:
    from assemblyline_common.utils.trace_platform import get_tracer, trace_span

    tracer = get_tracer()
    with tracer.trace("my_operation", metadata={"key": "value"}):
        # Your code here
        pass
"""

import os
import logging
import time
import json
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from contextlib import contextmanager
import asyncio

logger = logging.getLogger(__name__)


class TracePlatform(Enum):
    """Supported trace platforms"""
    LANGSMITH = "langsmith"
    PHOENIX = "phoenix"
    WANDB = "wandb"
    LOCAL = "local"  # Fallback to local logging


@dataclass
class TraceSpan:
    """A single trace span"""
    name: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    status: str = "running"
    metadata: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": (self.end_time - self.start_time) * 1000 if self.end_time else None,
            "status": self.status,
            "metadata": self.metadata,
            "events": self.events
        }


class BaseTracer:
    """Base tracer interface"""

    def __init__(self):
        self.current_trace_id: Optional[str] = None
        self.current_span_id: Optional[str] = None
        self.spans: List[TraceSpan] = []

    def _generate_id(self) -> str:
        import uuid
        return str(uuid.uuid4())[:16]

    @contextmanager
    def trace(self, name: str, metadata: Optional[Dict[str, Any]] = None):
        """Context manager for tracing a span"""
        span = self.start_span(name, metadata)
        try:
            yield span
            self.end_span(span, status="success")
        except Exception as e:
            self.end_span(span, status="error", error=str(e))
            raise

    def start_span(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> TraceSpan:
        """Start a new span"""
        if not self.current_trace_id:
            self.current_trace_id = self._generate_id()

        span = TraceSpan(
            name=name,
            trace_id=self.current_trace_id,
            span_id=self._generate_id(),
            parent_span_id=self.current_span_id,
            metadata=metadata or {}
        )
        self.current_span_id = span.span_id
        self.spans.append(span)
        return span

    def end_span(self, span: TraceSpan, status: str = "success", error: Optional[str] = None):
        """End a span"""
        span.end_time = time.time()
        span.status = status
        if error:
            span.metadata["error"] = error
        self._send_span(span)
        self.current_span_id = span.parent_span_id

    def add_event(self, span: TraceSpan, name: str, data: Dict[str, Any]):
        """Add an event to a span"""
        span.events.append({
            "name": name,
            "timestamp": time.time(),
            "data": data
        })

    def _send_span(self, span: TraceSpan):
        """Send span to the trace platform - override in subclasses"""
        pass


class LangSmithTracer(BaseTracer):
    """LangSmith integration for LangChain tracing"""

    def __init__(self, api_key: Optional[str] = None, project: str = "drivesentinel"):
        super().__init__()
        self.api_key = api_key or os.getenv("LANGCHAIN_API_KEY")
        self.project = project
        self.client = None

        if self.api_key:
            try:
                from langsmith import Client
                self.client = Client(api_key=self.api_key)
                logger.info(f"LangSmith tracer initialized for project: {project}")
            except ImportError:
                logger.warning("LangSmith not installed. Run: pip install langsmith")
        else:
            logger.warning("LANGCHAIN_API_KEY not set - LangSmith tracing disabled")

    def _send_span(self, span: TraceSpan):
        """Send span to LangSmith"""
        if self.client:
            try:
                self.client.create_run(
                    name=span.name,
                    run_type="chain",
                    inputs=span.metadata.get("inputs", {}),
                    outputs=span.metadata.get("outputs", {}),
                    error=span.metadata.get("error"),
                    start_time=datetime.fromtimestamp(span.start_time),
                    end_time=datetime.fromtimestamp(span.end_time) if span.end_time else None,
                    extra={"span_id": span.span_id, "trace_id": span.trace_id},
                    project_name=self.project
                )
            except Exception as e:
                logger.warning(f"Failed to send span to LangSmith: {e}")


class PhoenixTracer(BaseTracer):
    """Arize Phoenix integration for ML observability"""

    def __init__(self, endpoint: Optional[str] = None):
        super().__init__()
        self.endpoint = endpoint or os.getenv("PHOENIX_ENDPOINT", "http://localhost:6006")
        self.session = None

        try:
            import phoenix as px
            self.px = px
            logger.info(f"Phoenix tracer initialized at: {self.endpoint}")
        except ImportError:
            self.px = None
            logger.warning("Phoenix not installed. Run: pip install arize-phoenix")

    def _send_span(self, span: TraceSpan):
        """Send span to Phoenix"""
        if self.px:
            try:
                # Phoenix uses OpenTelemetry under the hood
                from opentelemetry import trace as otel_trace
                tracer = otel_trace.get_tracer(__name__)
                with tracer.start_as_current_span(span.name) as otel_span:
                    for key, value in span.metadata.items():
                        otel_span.set_attribute(key, str(value))
            except Exception as e:
                logger.warning(f"Failed to send span to Phoenix: {e}")


class WandBTracer(BaseTracer):
    """Weights & Biases integration for experiment tracking"""

    def __init__(self, project: str = "drivesentinel", entity: Optional[str] = None):
        super().__init__()
        self.project = project
        self.entity = entity
        self.run = None

        try:
            import wandb
            self.wandb = wandb

            # Initialize run if not already active
            if wandb.run is None:
                self.run = wandb.init(
                    project=project,
                    entity=entity,
                    job_type="inference",
                    reinit=True
                )
            else:
                self.run = wandb.run
            logger.info(f"W&B tracer initialized for project: {project}")
        except ImportError:
            self.wandb = None
            logger.warning("W&B not installed. Run: pip install wandb")

    def _send_span(self, span: TraceSpan):
        """Send span to W&B"""
        if self.wandb and self.run:
            try:
                # Log as a table row
                self.run.log({
                    f"trace/{span.name}": {
                        "duration_ms": (span.end_time - span.start_time) * 1000 if span.end_time else 0,
                        "status": span.status,
                        **span.metadata
                    }
                })
            except Exception as e:
                logger.warning(f"Failed to send span to W&B: {e}")


class LocalTracer(BaseTracer):
    """Local file-based tracer for development"""

    def __init__(self, log_file: str = "logs/traces.jsonl"):
        super().__init__()
        self.log_file = log_file

        # Ensure directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        logger.info(f"Local tracer initialized: {log_file}")

    def _send_span(self, span: TraceSpan):
        """Write span to local file"""
        try:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(span.to_dict()) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write span to file: {e}")


class CompositeTracer(BaseTracer):
    """Tracer that sends to multiple platforms"""

    def __init__(self, tracers: List[BaseTracer]):
        super().__init__()
        self.tracers = tracers

    def _send_span(self, span: TraceSpan):
        """Send span to all configured tracers"""
        for tracer in self.tracers:
            try:
                tracer._send_span(span)
            except Exception as e:
                logger.warning(f"Failed to send to {tracer.__class__.__name__}: {e}")


# Global tracer instance
_tracer: Optional[BaseTracer] = None


def get_tracer(
    platform: Optional[TracePlatform] = None,
    **kwargs
) -> BaseTracer:
    """
    Get or create the global tracer instance.

    Args:
        platform: Which platform to use (auto-detected if not specified)
        **kwargs: Platform-specific configuration

    Returns:
        Configured tracer instance
    """
    global _tracer

    if _tracer is not None:
        return _tracer

    # Auto-detect platform from environment
    if platform is None:
        if os.getenv("LANGCHAIN_API_KEY"):
            platform = TracePlatform.LANGSMITH
        elif os.getenv("PHOENIX_ENDPOINT"):
            platform = TracePlatform.PHOENIX
        elif os.getenv("WANDB_API_KEY"):
            platform = TracePlatform.WANDB
        else:
            platform = TracePlatform.LOCAL

    # Create tracer based on platform
    if platform == TracePlatform.LANGSMITH:
        _tracer = LangSmithTracer(**kwargs)
    elif platform == TracePlatform.PHOENIX:
        _tracer = PhoenixTracer(**kwargs)
    elif platform == TracePlatform.WANDB:
        _tracer = WandBTracer(**kwargs)
    else:
        _tracer = LocalTracer(**kwargs)

    return _tracer


def trace_span(name: str, metadata: Optional[Dict[str, Any]] = None):
    """
    Decorator to trace a function call.

    Usage:
        @trace_span("my_function")
        def my_function():
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            with tracer.trace(name, metadata=metadata):
                return func(*args, **kwargs)

        async def async_wrapper(*args, **kwargs):
            tracer = get_tracer()
            with tracer.trace(name, metadata=metadata):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    return decorator


# Convenience function for quick tracing
def trace(name: str, **metadata):
    """Context manager shortcut for tracing"""
    return get_tracer().trace(name, metadata=metadata)

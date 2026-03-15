"""
Langfuse LLM Observability Integration

Bridges the existing OpenTelemetry tracing into Langfuse for LLM-specific
observability: trace visualization, token counts, cost tracking, evals.

Usage:
    # At service startup (after init_telemetry):
    from services.shared.observability.langfuse_integration import (
        init_langfuse_tracing, instrument_pydantic_ai
    )
    init_langfuse_tracing()      # Attach LangfuseSpanProcessor to TracerProvider
    instrument_pydantic_ai()     # Auto-trace all Pydantic AI agent runs

Part of doc 125-LangfuseIntegration.md
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_langfuse_client = None
_initialized = False


def is_enabled() -> bool:
    """Check if Langfuse is enabled via feature flag."""
    return os.getenv("USE_LANGFUSE", "").lower() == "true"


def is_available() -> bool:
    """Check if Langfuse is enabled AND credentials are configured."""
    return (
        is_enabled()
        and bool(os.getenv("LANGFUSE_PUBLIC_KEY"))
        and bool(os.getenv("LANGFUSE_SECRET_KEY"))
    )


def get_langfuse_client():
    """
    Get or create the Langfuse client singleton.

    Reads configuration from environment variables:
    - LANGFUSE_PUBLIC_KEY
    - LANGFUSE_SECRET_KEY
    - LANGFUSE_HOST (default: http://localhost:3000)

    Returns:
        Langfuse client instance

    Raises:
        ImportError: If langfuse is not installed
        ValueError: If required env vars are missing
    """
    global _langfuse_client
    if _langfuse_client is not None:
        return _langfuse_client

    from langfuse import Langfuse

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY", "")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY", "")
    host = os.getenv("LANGFUSE_HOST", "http://localhost:3000")

    if not public_key or not secret_key:
        raise ValueError(
            "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are required. "
            "Generate keys in Langfuse UI: Project > Settings."
        )

    _langfuse_client = Langfuse(
        public_key=public_key,
        secret_key=secret_key,
        host=host,
    )
    logger.info(f"Langfuse client initialized (host={host})")
    return _langfuse_client


def init_langfuse_tracing(tracer_provider=None) -> bool:
    """
    Attach LangfuseSpanProcessor to the existing TracerProvider.

    This makes all existing @trace_function and @trace_llm_call spans
    flow to Langfuse automatically, alongside the existing OTLP exporter.

    Infrastructure spans (FastAPI, SQLAlchemy, httpx) are blocked so
    Langfuse only receives LLM/agent-relevant traces.

    Args:
        tracer_provider: Existing TracerProvider. If None, gets the global one.

    Returns:
        True if successfully initialized
    """
    global _initialized

    if _initialized:
        return True

    if not is_available():
        logger.debug("Langfuse not available (USE_LANGFUSE not set or missing keys)")
        return False

    try:
        from langfuse.opentelemetry import LangfuseSpanProcessor

        # Get the global tracer provider if none supplied
        if tracer_provider is None:
            from opentelemetry import trace
            tracer_provider = trace.get_tracer_provider()

        # Block infrastructure scopes — only LLM/agent spans go to Langfuse
        processor = LangfuseSpanProcessor(
            blocked_instrumentation_scopes=[
                "fastapi",
                "sqlalchemy",
                "psycopg2",
                "asyncpg",
                "httpx",
                "aiohttp",
                "redis",
                "uvicorn",
            ]
        )

        # The SDK TracerProvider has add_span_processor
        if hasattr(tracer_provider, "add_span_processor"):
            tracer_provider.add_span_processor(processor)
        else:
            # Wrapped provider — try to reach the underlying one
            underlying = getattr(tracer_provider, "_real_tracer_provider", None)
            if underlying and hasattr(underlying, "add_span_processor"):
                underlying.add_span_processor(processor)
            else:
                logger.warning("Could not add LangfuseSpanProcessor — TracerProvider type not supported")
                return False

        _initialized = True
        logger.info("Langfuse span processor attached to TracerProvider")
        return True

    except ImportError:
        logger.debug("langfuse package not installed")
        return False
    except Exception as e:
        logger.error(f"Failed to initialize Langfuse tracing: {e}")
        return False


def instrument_pydantic_ai() -> bool:
    """
    Enable automatic tracing of all Pydantic AI agent runs.

    Calls Agent.instrument_all() which instruments every agent globally —
    every agent.run() emits OTel spans that flow to both Tempo and Langfuse.

    Returns:
        True if successfully instrumented
    """
    if not is_available():
        return False

    try:
        from pydantic_ai import Agent
        Agent.instrument_all()
        logger.info("Pydantic AI agents instrumented for Langfuse tracing")
        return True
    except ImportError:
        logger.debug("pydantic-ai not available for instrumentation")
        return False
    except Exception as e:
        logger.error(f"Failed to instrument Pydantic AI: {e}")
        return False


def set_trace_metadata(
    user_id: Optional[int] = None,
    session_id: Optional[str] = None,
    org_id: Optional[int] = None,
    tags: Optional[list[str]] = None,
):
    """
    Set Langfuse trace metadata for the current context.

    Maps Y.A.M.I.L identifiers to Langfuse conventions:
    - user_id → langfuse.user.id
    - conversation_id → langfuse.session.id
    - org_id → metadata

    Must be called within an active trace context (e.g., inside a request handler).

    Args:
        user_id: Y.A.M.I.L user ID
        session_id: Conversation/session ID
        org_id: Organization ID
        tags: Optional trace tags
    """
    if not is_available():
        return

    try:
        from langfuse import propagate_attributes

        attrs = {}
        if user_id is not None:
            attrs["user_id"] = str(user_id)
        if session_id is not None:
            attrs["session_id"] = session_id
        if tags:
            attrs["tags"] = tags

        metadata = {}
        if org_id is not None:
            metadata["org_id"] = org_id

        if metadata:
            attrs["metadata"] = metadata

        # propagate_attributes is a context manager but can also be called
        # to set attributes on the current trace
        with propagate_attributes(**attrs):
            pass  # Attributes are set on the current context

    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"Failed to set Langfuse trace metadata: {e}")


def flush():
    """Flush pending Langfuse data (call before shutdown)."""
    if _langfuse_client is not None:
        try:
            _langfuse_client.flush()
            logger.info("Langfuse data flushed")
        except Exception as e:
            logger.error(f"Langfuse flush failed: {e}")


def shutdown():
    """Gracefully shut down the Langfuse client."""
    global _langfuse_client, _initialized
    if _langfuse_client is not None:
        try:
            _langfuse_client.flush()
            _langfuse_client.shutdown()
            logger.info("Langfuse client shut down")
        except Exception as e:
            logger.error(f"Langfuse shutdown failed: {e}")
        finally:
            _langfuse_client = None
            _initialized = False

"""
RAG Service — Hybrid search, pgvector, HyDE, reranking, RAGAS evaluation.

Port: 8022

Enterprise middleware: observability, structured logging, PHI masking.
"""

import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from app.main import app  # noqa: E402, F401

try:
    from assemblyline_common.observability import setup_observability, ObservabilityConfig
    from assemblyline_common.phi.log_filter import install_phi_log_filter

    @app.on_event("startup")
    async def _enterprise_startup():
        try:
            install_phi_log_filter()
        except Exception:
            pass
        try:
            config = ObservabilityConfig(
                service_name="rag-service",
                service_version="1.0.0",
                service_port=8022,
                enable_tracing=True,
                enable_metrics=True,
                enable_structured_logging=True,
                enable_request_logging=True,
                enable_phi_masking=True,
                otlp_endpoint=os.getenv("OTLP_ENDPOINT"),
            )
            await setup_observability(
                app=app,
                service_name="rag-service",
                service_port=8022,
                config=config,
            )
        except Exception as e:
            logging.getLogger(__name__).warning(f"Observability setup skipped: {e}")
except ImportError:
    pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8022")), reload=True)

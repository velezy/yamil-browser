"""
RAG Service — Hybrid search, pgvector, HyDE, reranking, RAGAS evaluation.

Port: 8022 (AssemblyLine) / 17002 (DriveSentinel origin)

Promoted from services-drivesentinel/rag/ as a reusable building block.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from app.main import app  # noqa: E402, F401

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8022")), reload=True)

"""
YAMIL Browser Chat Service — Standalone AI backend.

Powers the YAMIL Browser AI sidebar with multi-provider LLM routing,
voice I/O, and agentic RAG for external apps.

Port: 8020
"""

import sys
import os

# Add app directory to path for module resolution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from app.main import app  # noqa: E402, F401

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8020")), reload=True)

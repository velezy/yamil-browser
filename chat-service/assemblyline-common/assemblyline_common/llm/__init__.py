"""
T.A.L.O.S. Multi-Provider LLM Module
=====================================
Unified interface for multiple LLM providers with smart routing.

Supports:
- Local: Ollama, vLLM, llama.cpp
- Cloud: OpenAI, Anthropic (Claude), Google (Gemini), Azure OpenAI

Usage:
    from assemblyline_common.llm import get_llm_router, LLMRequest

    router = await get_llm_router()
    response = await router.generate(LLMRequest(
        prompt="What is 2+2?",
        provider="auto"  # Smart routing
    ))
"""

from .provider_interface import (
    LLMProvider,
    LLMRequest,
    LLMResponse,
    BaseLLMProvider,
    ProviderCapabilities,
)
from .router import (
    LLMRouter,
    get_llm_router,
    route_request,
)

__all__ = [
    # Enums and types
    "LLMProvider",
    "LLMRequest",
    "LLMResponse",
    "ProviderCapabilities",
    # Base class
    "BaseLLMProvider",
    # Router
    "LLMRouter",
    "get_llm_router",
    "route_request",
]

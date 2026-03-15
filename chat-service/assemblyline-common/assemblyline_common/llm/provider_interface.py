"""
Unified LLM Provider Interface
==============================
Abstract base class and types for all LLM providers.
"""

import os
import time
import logging
from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Supported LLM providers."""
    # Local providers
    OLLAMA = "ollama"
    VLLM = "vllm"
    LLAMA_CPP = "llama_cpp"

    # Cloud providers
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GEMINI = "gemini"
    AZURE = "azure"

    # Auto-select
    AUTO = "auto"


@dataclass
class ProviderCapabilities:
    """Capabilities of an LLM provider."""
    vision: bool = False
    function_calling: bool = False
    streaming: bool = True
    embeddings: bool = False
    max_context: int = 4096
    supports_system_prompt: bool = True
    cost_per_1m_input: float = 0.0  # USD per 1M input tokens
    cost_per_1m_output: float = 0.0  # USD per 1M output tokens


# Provider capabilities registry
PROVIDER_CAPABILITIES: Dict[LLMProvider, Dict[str, ProviderCapabilities]] = {
    LLMProvider.OLLAMA: {
        "default": ProviderCapabilities(
            vision=False, function_calling=False, streaming=True,
            max_context=8192, cost_per_1m_input=0.0, cost_per_1m_output=0.0
        ),
        "qwen2.5-vl:3b": ProviderCapabilities(
            vision=True, function_calling=False, streaming=True,
            max_context=8192, cost_per_1m_input=0.0, cost_per_1m_output=0.0
        ),
        "llama3.1:8b": ProviderCapabilities(
            vision=False, function_calling=True, streaming=True,
            max_context=128000, cost_per_1m_input=0.0, cost_per_1m_output=0.0
        ),
    },
    LLMProvider.OPENAI: {
        "gpt-4o": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=128000, cost_per_1m_input=2.50, cost_per_1m_output=10.00
        ),
        "gpt-4o-mini": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=128000, cost_per_1m_input=0.15, cost_per_1m_output=0.60
        ),
        "gpt-4-turbo": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=128000, cost_per_1m_input=10.00, cost_per_1m_output=30.00
        ),
    },
    LLMProvider.ANTHROPIC: {
        "claude-3-5-sonnet-20241022": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=200000, cost_per_1m_input=3.00, cost_per_1m_output=15.00
        ),
        "claude-3-5-haiku-20241022": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=200000, cost_per_1m_input=0.80, cost_per_1m_output=4.00
        ),
        "claude-3-opus-20240229": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=200000, cost_per_1m_input=15.00, cost_per_1m_output=75.00
        ),
    },
    LLMProvider.GEMINI: {
        "gemini-1.5-flash": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=1000000, cost_per_1m_input=0.075, cost_per_1m_output=0.30
        ),
        "gemini-1.5-pro": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=1000000, cost_per_1m_input=1.25, cost_per_1m_output=5.00
        ),
        "gemini-2.0-flash": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=1000000, cost_per_1m_input=0.10, cost_per_1m_output=0.40
        ),
    },
    LLMProvider.AZURE: {
        "default": ProviderCapabilities(
            vision=True, function_calling=True, streaming=True,
            max_context=128000, cost_per_1m_input=2.50, cost_per_1m_output=10.00
        ),
    },
    LLMProvider.VLLM: {
        "default": ProviderCapabilities(
            vision=False, function_calling=False, streaming=True,
            max_context=8192, cost_per_1m_input=0.0, cost_per_1m_output=0.0
        ),
    },
    LLMProvider.LLAMA_CPP: {
        "default": ProviderCapabilities(
            vision=False, function_calling=False, streaming=True,
            max_context=4096, cost_per_1m_input=0.0, cost_per_1m_output=0.0
        ),
    },
}


@dataclass
class LLMRequest:
    """Unified request format for all providers."""
    prompt: str
    system_prompt: Optional[str] = None
    model: Optional[str] = None
    provider: str = "auto"  # Provider name or "auto"
    temperature: float = 0.7
    max_tokens: int = 4096
    messages: Optional[List[Dict[str, str]]] = None  # Chat format
    images: Optional[List[str]] = None  # Base64 images for vision
    tools: Optional[List[Dict]] = None  # Function calling
    stream: bool = False
    context: Optional[List[str]] = None  # RAG context

    # Routing hints
    task_type: Optional[str] = None  # "vision", "code", "reasoning", "general"
    prefer_local: bool = False  # Prefer local over cloud
    max_cost: Optional[float] = None  # Max cost in USD for this request

    # User/org context for routing
    user_id: Optional[int] = None
    org_id: Optional[int] = None


@dataclass
class LLMResponse:
    """Unified response format from all providers."""
    content: str
    provider: LLMProvider
    model: str
    tokens_used: int = 0
    tokens_prompt: int = 0
    tokens_completion: int = 0
    latency_ms: float = 0.0
    cost_usd: float = 0.0
    finish_reason: str = "stop"
    tool_calls: Optional[List[Dict]] = None

    # Metadata
    cached: bool = False
    fallback_used: bool = False
    original_provider: Optional[LLMProvider] = None  # If fallback was used
    pii_redacted: bool = False
    pii_count: int = 0


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""

    provider_type: LLMProvider = LLMProvider.AUTO

    def __init__(self, **kwargs):
        self.config = kwargs
        self._healthy: Optional[bool] = None
        self._last_health_check: float = 0
        self._health_check_interval: float = 30.0  # seconds

    @abstractmethod
    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response."""
        pass

    @abstractmethod
    async def generate_stream(self, request: LLMRequest) -> AsyncIterator[str]:
        """Stream a response token by token."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if provider is available."""
        pass

    @abstractmethod
    def get_available_models(self) -> List[str]:
        """List available models for this provider."""
        pass

    def get_capabilities(self, model: Optional[str] = None) -> ProviderCapabilities:
        """Get capabilities for a model."""
        caps = PROVIDER_CAPABILITIES.get(self.provider_type, {})
        if model and model in caps:
            return caps[model]
        return caps.get("default", ProviderCapabilities())

    async def is_healthy(self) -> bool:
        """Check health with caching."""
        now = time.time()
        if self._healthy is None or (now - self._last_health_check) > self._health_check_interval:
            self._healthy = await self.health_check()
            self._last_health_check = now
        return self._healthy

    def calculate_cost(self, input_tokens: int, output_tokens: int, model: Optional[str] = None) -> float:
        """Calculate cost for a request."""
        caps = self.get_capabilities(model)
        input_cost = (input_tokens / 1_000_000) * caps.cost_per_1m_input
        output_cost = (output_tokens / 1_000_000) * caps.cost_per_1m_output
        return input_cost + output_cost

    def supports_vision(self, model: Optional[str] = None) -> bool:
        """Check if vision is supported."""
        return self.get_capabilities(model).vision

    def supports_function_calling(self, model: Optional[str] = None) -> bool:
        """Check if function calling is supported."""
        return self.get_capabilities(model).function_calling

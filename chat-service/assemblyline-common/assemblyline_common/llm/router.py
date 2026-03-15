"""
Smart LLM Router
================
Intelligent routing between multiple LLM providers with fallback support.
"""

import os
import asyncio
import logging
from typing import Dict, List, Optional, AsyncIterator, Tuple
from dataclasses import dataclass, field

from .provider_interface import (
    LLMProvider,
    LLMRequest,
    LLMResponse,
    BaseLLMProvider,
)
from .providers import (
    OllamaProvider,
    OpenAIProvider,
    AnthropicProvider,
    GeminiProvider,
    AzureOpenAIProvider,
)

logger = logging.getLogger(__name__)


@dataclass
class RouterConfig:
    """Configuration for the LLM router."""
    # Default provider preference
    default_provider: str = "auto"

    # Fallback chain (order of providers to try)
    fallback_chain: List[str] = field(default_factory=lambda: ["ollama", "gemini", "openai", "anthropic"])

    # Task-based routing preferences
    vision_provider: str = "gemini"
    code_provider: str = "openai"
    reasoning_provider: str = "anthropic"
    fast_provider: str = "ollama"

    # Cost management
    max_monthly_cost: float = 100.0  # USD
    prefer_local_when_possible: bool = True

    # Performance
    health_check_interval: float = 30.0  # seconds
    request_timeout: float = 120.0

    @classmethod
    def from_env(cls) -> "RouterConfig":
        """Create config from environment variables."""
        fallback_str = os.getenv("LLM_FALLBACK_CHAIN", "ollama,gemini,openai,anthropic")
        fallback_chain = [p.strip() for p in fallback_str.split(",")]

        return cls(
            default_provider=os.getenv("DEFAULT_LLM_PROVIDER", "auto"),
            fallback_chain=fallback_chain,
            vision_provider=os.getenv("LLM_VISION_PROVIDER", "gemini"),
            code_provider=os.getenv("LLM_CODE_PROVIDER", "openai"),
            reasoning_provider=os.getenv("LLM_REASONING_PROVIDER", "anthropic"),
            fast_provider=os.getenv("LLM_FAST_PROVIDER", "ollama"),
            max_monthly_cost=float(os.getenv("LLM_MAX_MONTHLY_COST", "100")),
            prefer_local_when_possible=os.getenv("LLM_PREFER_LOCAL", "true").lower() == "true",
        )


class LLMRouter:
    """
    Intelligent LLM routing with fallback and task-based selection.

    Features:
    - Multi-provider support (Ollama, OpenAI, Anthropic, Gemini, Azure)
    - Automatic fallback on failure
    - Task-based routing (vision, code, reasoning)
    - Cost tracking and limits
    - Health monitoring
    """

    # Provider name to enum mapping
    PROVIDER_MAP: Dict[str, LLMProvider] = {
        "ollama": LLMProvider.OLLAMA,
        "openai": LLMProvider.OPENAI,
        "anthropic": LLMProvider.ANTHROPIC,
        "claude": LLMProvider.ANTHROPIC,
        "gemini": LLMProvider.GEMINI,
        "google": LLMProvider.GEMINI,
        "azure": LLMProvider.AZURE,
    }

    def __init__(self, config: Optional[RouterConfig] = None):
        self.config = config or RouterConfig.from_env()
        self.providers: Dict[LLMProvider, BaseLLMProvider] = {}
        self._monthly_cost: float = 0.0
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize all providers and check availability."""
        if self._initialized:
            return

        # Initialize providers
        self.providers = {
            LLMProvider.OLLAMA: OllamaProvider(),
            LLMProvider.OPENAI: OpenAIProvider(),
            LLMProvider.ANTHROPIC: AnthropicProvider(),
            LLMProvider.GEMINI: GeminiProvider(),
            LLMProvider.AZURE: AzureOpenAIProvider(),
        }

        # Check which providers are available
        available = []
        for name, provider in self.providers.items():
            if await provider.is_healthy():
                available.append(name.value)

        logger.info(f"LLM Router initialized. Available providers: {available}")
        self._initialized = True

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """
        Route request to appropriate provider.

        Priority:
        1. User's specified provider (if available and healthy)
        2. Task-based routing (vision → multimodal provider)
        3. Organization/user preference
        4. Fallback chain
        """
        await self.initialize()

        # Determine target provider
        provider, reason = await self._select_provider(request)
        logger.info(f"Routing to {provider.value}: {reason}")

        # Try primary provider, then fallbacks
        providers_to_try = self._build_provider_chain(provider, request)
        last_error = None
        original_provider = provider

        for try_provider in providers_to_try:
            if try_provider not in self.providers:
                continue

            provider_instance = self.providers[try_provider]

            # Check health
            if not await provider_instance.is_healthy():
                logger.warning(f"{try_provider.value} is not healthy, trying next")
                continue

            # Check cost limit
            if not self._check_cost_limit(try_provider, request):
                logger.warning(f"Cost limit would be exceeded with {try_provider.value}")
                continue

            try:
                response = await provider_instance.generate(request)

                # Track cost
                self._monthly_cost += response.cost_usd

                # Mark if fallback was used
                if try_provider != original_provider:
                    response.fallback_used = True
                    response.original_provider = original_provider

                return response

            except Exception as e:
                logger.error(f"Provider {try_provider.value} failed: {e}")
                last_error = e
                continue

        raise RuntimeError(f"All LLM providers failed. Last error: {last_error}")

    async def generate_stream(self, request: LLMRequest) -> AsyncIterator[str]:
        """Stream response from appropriate provider."""
        await self.initialize()

        provider, reason = await self._select_provider(request)
        providers_to_try = self._build_provider_chain(provider, request)

        for try_provider in providers_to_try:
            if try_provider not in self.providers:
                continue

            provider_instance = self.providers[try_provider]

            if not await provider_instance.is_healthy():
                continue

            try:
                async for chunk in provider_instance.generate_stream(request):
                    yield chunk
                return
            except Exception as e:
                logger.error(f"Streaming from {try_provider.value} failed: {e}")
                continue

        raise RuntimeError("All LLM providers failed for streaming")

    async def _select_provider(self, request: LLMRequest) -> Tuple[LLMProvider, str]:
        """Select the best provider for a request."""

        # 1. User explicitly specified provider
        if request.provider and request.provider != "auto":
            provider_name = request.provider.lower()
            if provider_name in self.PROVIDER_MAP:
                return self.PROVIDER_MAP[provider_name], "user specified"

        # 2. Task-based routing
        if request.task_type == "vision" or request.images:
            provider_name = self.config.vision_provider
            if provider_name in self.PROVIDER_MAP:
                return self.PROVIDER_MAP[provider_name], "vision task"

        if request.task_type == "code":
            provider_name = self.config.code_provider
            if provider_name in self.PROVIDER_MAP:
                return self.PROVIDER_MAP[provider_name], "code task"

        if request.task_type == "reasoning":
            provider_name = self.config.reasoning_provider
            if provider_name in self.PROVIDER_MAP:
                return self.PROVIDER_MAP[provider_name], "reasoning task"

        if request.task_type == "fast":
            provider_name = self.config.fast_provider
            if provider_name in self.PROVIDER_MAP:
                return self.PROVIDER_MAP[provider_name], "fast task"

        # 3. Prefer local if configured
        if request.prefer_local or self.config.prefer_local_when_possible:
            if LLMProvider.OLLAMA in self.providers:
                if await self.providers[LLMProvider.OLLAMA].is_healthy():
                    return LLMProvider.OLLAMA, "prefer local"

        # 4. Default provider from config
        if self.config.default_provider != "auto":
            provider_name = self.config.default_provider.lower()
            if provider_name in self.PROVIDER_MAP:
                return self.PROVIDER_MAP[provider_name], "default config"

        # 5. First available in fallback chain
        for provider_name in self.config.fallback_chain:
            if provider_name in self.PROVIDER_MAP:
                provider = self.PROVIDER_MAP[provider_name]
                if provider in self.providers and await self.providers[provider].is_healthy():
                    return provider, "fallback chain"

        # Fallback to Ollama
        return LLMProvider.OLLAMA, "fallback default"

    def _build_provider_chain(self, primary: LLMProvider, request: LLMRequest) -> List[LLMProvider]:
        """Build ordered list of providers to try."""
        chain = [primary]

        # Add fallback chain
        for provider_name in self.config.fallback_chain:
            if provider_name in self.PROVIDER_MAP:
                provider = self.PROVIDER_MAP[provider_name]
                if provider not in chain:
                    chain.append(provider)

        # If vision required, filter to vision-capable
        if request.images:
            vision_capable = [LLMProvider.GEMINI, LLMProvider.OPENAI, LLMProvider.ANTHROPIC]
            # Also check Ollama vision models
            if LLMProvider.OLLAMA in self.providers:
                ollama = self.providers[LLMProvider.OLLAMA]
                if hasattr(ollama, '_is_vision_model'):
                    model = request.model or "qwen2.5-vl:3b"
                    if ollama._is_vision_model(model):
                        vision_capable.append(LLMProvider.OLLAMA)

            chain = [p for p in chain if p in vision_capable]

        return chain

    def _check_cost_limit(self, provider: LLMProvider, request: LLMRequest) -> bool:
        """Check if using this provider would exceed cost limits."""
        # Local providers have no cost
        if provider == LLMProvider.OLLAMA:
            return True

        # Check request-specific limit
        if request.max_cost is not None:
            # Estimate cost (rough approximation)
            provider_instance = self.providers[provider]
            caps = provider_instance.get_capabilities(request.model)
            estimated_cost = (request.max_tokens / 1_000_000) * caps.cost_per_1m_output
            if estimated_cost > request.max_cost:
                return False

        # Check monthly limit
        if self._monthly_cost >= self.config.max_monthly_cost:
            return False

        return True

    def get_available_providers(self) -> List[str]:
        """Get list of configured providers."""
        return [p.value for p in self.providers.keys()]

    async def get_healthy_providers(self) -> List[str]:
        """Get list of healthy providers."""
        await self.initialize()
        healthy = []
        for name, provider in self.providers.items():
            if await provider.is_healthy():
                healthy.append(name.value)
        return healthy

    def get_provider_models(self, provider_name: str) -> List[str]:
        """Get available models for a provider."""
        provider_name = provider_name.lower()
        if provider_name in self.PROVIDER_MAP:
            provider = self.PROVIDER_MAP[provider_name]
            if provider in self.providers:
                return self.providers[provider].get_available_models()
        return []

    def get_monthly_cost(self) -> float:
        """Get current monthly cost."""
        return self._monthly_cost

    def reset_monthly_cost(self) -> None:
        """Reset monthly cost counter (call at start of month)."""
        self._monthly_cost = 0.0

    async def test_provider(self, provider_name: str, prompt: str = "What is 2+2?") -> Dict:
        """Test a specific provider."""
        await self.initialize()

        provider_name = provider_name.lower()
        if provider_name not in self.PROVIDER_MAP:
            return {"success": False, "error": f"Unknown provider: {provider_name}"}

        provider = self.PROVIDER_MAP[provider_name]
        if provider not in self.providers:
            return {"success": False, "error": f"Provider not configured: {provider_name}"}

        provider_instance = self.providers[provider]

        try:
            # Health check
            is_healthy = await provider_instance.health_check()
            if not is_healthy:
                return {"success": False, "error": "Provider health check failed"}

            # Test generation
            request = LLMRequest(prompt=prompt, max_tokens=100)
            response = await provider_instance.generate(request)

            return {
                "success": True,
                "provider": provider_name,
                "model": response.model,
                "response": response.content[:200],
                "latency_ms": response.latency_ms,
                "tokens_used": response.tokens_used,
                "cost_usd": response.cost_usd,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}


# =============================================================================
# GLOBAL ROUTER INSTANCE
# =============================================================================

_router: Optional[LLMRouter] = None


async def get_llm_router() -> LLMRouter:
    """Get or create the global LLM router."""
    global _router
    if _router is None:
        _router = LLMRouter()
        await _router.initialize()
    return _router


async def route_request(request: LLMRequest) -> LLMResponse:
    """Convenience function to route a request."""
    router = await get_llm_router()
    return await router.generate(request)


async def route_stream(request: LLMRequest) -> AsyncIterator[str]:
    """Convenience function to stream a request."""
    router = await get_llm_router()
    async for chunk in router.generate_stream(request):
        yield chunk

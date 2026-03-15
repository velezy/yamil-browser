"""
Ollama Optimization Service

Background service for continuous model optimization.
Manages warm-up, eviction, and performance monitoring.
"""

import asyncio
import logging
from typing import Optional
from contextlib import asynccontextmanager

from .config import PRIORITY_MODELS
from .client import OllamaOptimizedClient
from .model_warmer import ModelWarmer
from .model_router import ModelRouter
from .token_counter import TokenCounter
from .context_optimizer import ContextOptimizer

logger = logging.getLogger(__name__)


class OllamaOptimizationService:
    """
    Continuous background optimization for Ollama.

    Features:
    - Background model warm-up loop
    - Stale model eviction
    - Priority model pre-warming
    - Performance statistics
    - Unified access to all optimization components
    """

    def __init__(self):
        """Initialize optimization service"""
        self.client = OllamaOptimizedClient()
        self.warmer = ModelWarmer()
        self.router = ModelRouter(self.warmer)
        self.token_counter = TokenCounter()
        self.context_optimizer = ContextOptimizer()

        self.running = False
        self._optimization_task: Optional[asyncio.Task] = None
        self._stats = {
            'loop_iterations': 0,
            'models_warmed': 0,
            'models_evicted': 0,
            'errors': 0,
        }
        # Track failed models to avoid repeated retry loops
        self._failed_models: dict[str, int] = {}  # model -> failure count
        self._max_failures = 3  # Stop trying after this many failures

    async def start(self) -> None:
        """Start background optimization"""
        if self.running:
            logger.warning("Optimization service already running")
            return

        # Check if Ollama has any models before starting the optimization loop
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{self.client.base_url}/api/tags")
                if resp.status_code == 200:
                    data = resp.json()
                    if not data.get("models"):
                        logger.warning("⚠️ No Ollama models installed - optimization service disabled")
                        return
        except Exception as e:
            logger.warning(f"⚠️ Could not check Ollama models: {e} - optimization service disabled")
            return

        self.running = True
        self._optimization_task = asyncio.create_task(self._optimization_loop())
        logger.info("🚀 Ollama optimization service started")

    async def stop(self) -> None:
        """Stop background optimization"""
        self.running = False

        if self._optimization_task:
            self._optimization_task.cancel()
            try:
                await self._optimization_task
            except asyncio.CancelledError:
                pass
            self._optimization_task = None

        logger.info("🛑 Ollama optimization service stopped")

    async def _optimization_loop(self) -> None:
        """
        Background optimization loop.

        Runs every 1 second to:
        1. Evict stale models
        2. Pre-warm priority models
        3. Update performance stats
        """
        while self.running:
            try:
                self._stats['loop_iterations'] += 1

                # 1. Evict stale models (unused for 10+ minutes)
                evicted = await self.warmer.evict_stale_models()
                if evicted:
                    self._stats['models_evicted'] += len(evicted)
                    logger.debug(f"Evicted stale models: {evicted}")

                # 2. Pre-warm priority models (skip failed models)
                for model in PRIORITY_MODELS:
                    # Skip models that have failed too many times
                    if self._failed_models.get(model, 0) >= self._max_failures:
                        continue
                    if model not in self.warmer.warm_models:
                        success = await self.warmer.warm_up(model)
                        if success:
                            self._stats['models_warmed'] += 1
                            self._failed_models.pop(model, None)  # Clear failure count
                        else:
                            self._failed_models[model] = self._failed_models.get(model, 0) + 1
                            if self._failed_models[model] >= self._max_failures:
                                logger.warning(f"⚠️ Model {model} unavailable after {self._max_failures} attempts, skipping")

                # 3. Update stats (could add more metrics here)
                self._update_performance_stats()

            except Exception as e:
                self._stats['errors'] += 1
                logger.error(f"Optimization loop error: {e}")

            await asyncio.sleep(1.0)

    def _update_performance_stats(self) -> None:
        """Update internal performance statistics"""
        # Could add more sophisticated tracking here
        pass

    async def generate(
        self,
        prompt: str,
        task_type: str = 'synthesis',
        system: Optional[str] = None,
        use_cache: bool = True,
        **options
    ) -> dict:
        """
        Generate response with automatic model selection.

        Args:
            prompt: User prompt
            task_type: Task type for model routing
            system: System prompt
            use_cache: Whether to use response caching
            **options: Additional model options

        Returns:
            Response dictionary
        """
        # Select optimal model
        model = self.router.select_model(task_type)

        # Update last used
        self.warmer.touch(model)

        # Generate response
        response = await self.client.generate(
            model=model,
            prompt=prompt,
            system=system,
            options=options,
            use_cache=use_cache,
        )

        return response

    async def generate_stream(
        self,
        prompt: str,
        task_type: str = 'synthesis',
        system: Optional[str] = None,
        **options
    ):
        """
        Stream response with automatic model selection.

        Args:
            prompt: User prompt
            task_type: Task type for model routing
            system: System prompt
            **options: Additional model options

        Yields:
            Response tokens
        """
        model = self.router.select_model(task_type)
        self.warmer.touch(model)

        async for token in self.client.generate_stream(
            model=model,
            prompt=prompt,
            system=system,
            options=options,
        ):
            yield token

    async def chat(
        self,
        messages: list[dict],
        task_type: str = 'chat',
        use_cache: bool = True,
        **options
    ) -> dict:
        """
        Chat completion with automatic model selection.

        Args:
            messages: List of message dicts
            task_type: Task type for model routing
            use_cache: Whether to use response caching
            **options: Additional model options

        Returns:
            Response dictionary
        """
        model = self.router.select_model(task_type)
        self.warmer.touch(model)

        return await self.client.chat(
            model=model,
            messages=messages,
            use_cache=use_cache,
            options=options,
        )

    def count_tokens(self, text: str) -> int:
        """Count tokens in text"""
        return self.token_counter.count_tokens(text)

    def optimize_context(
        self,
        documents: list[str],
        max_tokens: int,
        quality_scores: Optional[list[float]] = None
    ) -> list[str]:
        """Optimize document selection for context"""
        return self.context_optimizer.optimize_context(
            documents, max_tokens, quality_scores
        )

    def get_stats(self) -> dict:
        """Get comprehensive statistics"""
        return {
            'service': self._stats.copy(),
            'cache': self.client.get_cache_stats(),
            'warmer': self.warmer.get_stats(),
            'router': self.router.get_stats(),
            'context_optimizer': self.context_optimizer.get_average_metrics(),
            'running': self.running,
        }

    def clear_cache(self) -> int:
        """Clear response cache"""
        return self.client.clear_cache()


# Singleton instance
_service: Optional[OllamaOptimizationService] = None


def get_ollama_service() -> OllamaOptimizationService:
    """Get or create singleton optimization service"""
    global _service
    if _service is None:
        _service = OllamaOptimizationService()
    return _service


@asynccontextmanager
async def ollama_service_lifespan():
    """
    Context manager for service lifecycle.

    Usage:
        async with ollama_service_lifespan():
            # Service is running
            pass
    """
    service = get_ollama_service()
    await service.start()
    try:
        yield service
    finally:
        await service.stop()

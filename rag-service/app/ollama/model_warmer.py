"""
Model Warmer

Pre-load models to avoid cold starts and maintain keep-alive.
Reduces first-request latency from ~5-10s to <1s.
"""

import time
import logging
from typing import Optional, Set

import httpx

from .config import OLLAMA_URL, WARMUP_CONFIG, PRIORITY_MODELS

logger = logging.getLogger(__name__)


class ModelWarmer:
    """
    Pre-load and maintain warm models in Ollama.

    Features:
    - Minimal prompt warm-up (1 token generation)
    - Keep-alive management
    - Stale model eviction
    - Priority model pre-warming
    """

    def __init__(
        self,
        base_url: str = OLLAMA_URL,
        max_warm_models: int = WARMUP_CONFIG['max_warm_models'],
        keep_alive_seconds: int = WARMUP_CONFIG['keep_alive_seconds'],
        stale_threshold: int = WARMUP_CONFIG['stale_threshold_seconds'],
    ):
        self.base_url = base_url
        self.max_warm_models = max_warm_models
        self.keep_alive_seconds = keep_alive_seconds
        self.stale_threshold = stale_threshold

        self.warm_models: Set[str] = set()
        self.last_used: dict[str, float] = {}
        self.warmup_times: dict[str, float] = {}

    async def warm_up(self, model_name: str, force: bool = False) -> bool:
        """
        Load model with minimal request.

        Args:
            model_name: Ollama model name (e.g., 'gemma3:4b')
            force: Force warm-up even if already warm

        Returns:
            True if warm-up successful
        """
        # Skip if already warm (unless forced)
        if not force and model_name in self.warm_models:
            self.last_used[model_name] = time.time()
            logger.debug(f"Model {model_name} already warm")
            return True

        # Check capacity
        if len(self.warm_models) >= self.max_warm_models:
            # Evict least recently used
            await self._evict_lru_model()

        # Minimal warm-up request
        payload = {
            'model': model_name,
            'prompt': 'Hello',  # Minimal prompt
            'options': {'num_predict': 1},  # 1 token output
            'keep_alive': f'{self.keep_alive_seconds}s',
            'stream': False,
        }

        start_time = time.time()

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json=payload
                )
                response.raise_for_status()

            warmup_time = time.time() - start_time
            self.warm_models.add(model_name)
            self.last_used[model_name] = time.time()
            self.warmup_times[model_name] = warmup_time

            logger.info(f"🔥 Model warmed: {model_name} ({warmup_time:.2f}s)")
            return True

        except httpx.HTTPError as e:
            logger.error(f"Failed to warm model {model_name}: {e}")
            return False

    async def refresh_keep_alive(self, model_name: str) -> bool:
        """
        Refresh keep-alive timer for a model.

        Args:
            model_name: Model to refresh

        Returns:
            True if refresh successful
        """
        if model_name not in self.warm_models:
            return await self.warm_up(model_name)

        payload = {
            'model': model_name,
            'prompt': '',  # Empty prompt
            'options': {'num_predict': 0},
            'keep_alive': f'{self.keep_alive_seconds}s',
            'stream': False,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.post(
                    f"{self.base_url}/api/generate",
                    json=payload
                )

            self.last_used[model_name] = time.time()
            logger.debug(f"Refreshed keep-alive: {model_name}")
            return True

        except httpx.HTTPError:
            # Model might have been evicted
            self.warm_models.discard(model_name)
            return False

    async def evict_stale_models(self) -> list[str]:
        """
        Remove models unused for too long.

        Returns:
            List of evicted model names
        """
        current_time = time.time()
        evicted = []

        stale = [
            model for model in list(self.warm_models)
            if current_time - self.last_used.get(model, 0) > self.stale_threshold
        ]

        for model in stale:
            await self._unload_model(model)
            evicted.append(model)

        return evicted

    async def _evict_lru_model(self) -> Optional[str]:
        """Evict least recently used model"""
        if not self.warm_models:
            return None

        # Find LRU model (not in priority list)
        sorted_models = sorted(
            self.warm_models,
            key=lambda m: self.last_used.get(m, 0)
        )

        # Prefer evicting non-priority models
        for model in sorted_models:
            if model not in PRIORITY_MODELS:
                await self._unload_model(model)
                return model

        # If all are priority, evict oldest anyway
        if sorted_models:
            await self._unload_model(sorted_models[0])
            return sorted_models[0]

        return None

    async def _unload_model(self, model_name: str) -> bool:
        """
        Unload model from Ollama memory.

        Args:
            model_name: Model to unload

        Returns:
            True if unload successful
        """
        payload = {
            'model': model_name,
            'prompt': '',
            'keep_alive': '0',  # Immediate unload
            'stream': False,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.post(
                    f"{self.base_url}/api/generate",
                    json=payload
                )

            self.warm_models.discard(model_name)
            self.last_used.pop(model_name, None)
            logger.info(f"🗑️ Evicted model: {model_name}")
            return True

        except httpx.HTTPError as e:
            logger.warning(f"Failed to unload {model_name}: {e}")
            self.warm_models.discard(model_name)
            return False

    async def warm_priority_models(self) -> dict[str, bool]:
        """
        Warm up priority models.

        Returns:
            Dict of model -> success status
        """
        results = {}
        for model in PRIORITY_MODELS:
            if model not in self.warm_models:
                results[model] = await self.warm_up(model)
            else:
                results[model] = True
        return results

    def is_warm(self, model_name: str) -> bool:
        """Check if model is currently warm"""
        return model_name in self.warm_models

    def get_warm_models(self) -> list[str]:
        """Get list of currently warm models"""
        return list(self.warm_models)

    def get_stats(self) -> dict:
        """Get warmer statistics"""
        return {
            'warm_models': list(self.warm_models),
            'warm_count': len(self.warm_models),
            'max_warm': self.max_warm_models,
            'warmup_times': self.warmup_times.copy(),
            'last_used': {
                k: time.time() - v
                for k, v in self.last_used.items()
            },
        }

    def touch(self, model_name: str) -> None:
        """Update last used time for a model"""
        if model_name in self.warm_models:
            self.last_used[model_name] = time.time()

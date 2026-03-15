"""
Optimized Ollama Client

Drop-in replacement for Ollama client with response caching and warm-up.
Target: 85-90% latency reduction through intelligent caching.
"""

import hashlib
import time
import logging
from typing import Optional, Any, AsyncIterator
from dataclasses import dataclass, field

import httpx

from .config import OLLAMA_URL, CACHE_CONFIG, OLLAMA_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cached response with metadata"""
    response: Any
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    hit_count: int = 0


class OllamaOptimizedClient:
    """
    Ollama client with response caching and optimization.

    Features:
    - In-memory response caching with LRU eviction
    - TTL-based cache expiration
    - Deterministic cache keys from request parameters
    - Async request handling with streaming support
    """

    def __init__(
        self,
        base_url: str = OLLAMA_URL,
        cache_max_size: int = CACHE_CONFIG['max_size'],
        cache_ttl: int = CACHE_CONFIG['ttl_seconds'],
    ):
        self.base_url = base_url
        self.response_cache: dict[str, CacheEntry] = {}
        self.cache_max_size = cache_max_size
        self.cache_ttl = cache_ttl
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
        }

    def _generate_cache_key(
        self,
        model: str,
        prompt: str,
        options: Optional[dict] = None
    ) -> str:
        """Generate deterministic cache key from request parameters"""
        cache_data = {
            'model': model,
            'prompt': prompt.strip(),
            'options': sorted((options or {}).items())
        }
        return hashlib.md5(str(cache_data).encode()).hexdigest()

    def _get_cached_response(self, cache_key: str) -> Optional[CacheEntry]:
        """Get cached response if valid"""
        entry = self.response_cache.get(cache_key)

        if entry is None:
            return None

        # Check TTL
        if time.time() - entry.created_at > self.cache_ttl:
            del self.response_cache[cache_key]
            return None

        # Update access time and hit count
        entry.last_accessed = time.time()
        entry.hit_count += 1

        return entry

    def _cache_response(self, cache_key: str, response: Any) -> None:
        """Cache response with LRU eviction"""
        # Evict if at capacity
        if len(self.response_cache) >= self.cache_max_size:
            self._evict_oldest_entries()

        self.response_cache[cache_key] = CacheEntry(response=response)

    def _evict_oldest_entries(self) -> None:
        """Remove oldest entries using LRU strategy"""
        if not self.response_cache:
            return

        # Sort by last accessed time
        sorted_entries = sorted(
            self.response_cache.items(),
            key=lambda x: x[1].last_accessed
        )

        # Remove oldest 20%
        eviction_count = max(1, int(len(sorted_entries) * CACHE_CONFIG['eviction_percent']))
        for i in range(eviction_count):
            key = sorted_entries[i][0]
            del self.response_cache[key]
            self.stats['evictions'] += 1

        logger.debug(f"Evicted {eviction_count} cache entries")

    async def generate(
        self,
        model: str,
        prompt: str,
        system: Optional[str] = None,
        options: Optional[dict] = None,
        keep_alive: Optional[str] = None,
        use_cache: bool = True,
    ) -> dict:
        """
        Generate response from Ollama with caching.

        Args:
            model: Model name (e.g., 'gemma3:4b')
            prompt: User prompt
            system: System prompt (optional)
            options: Model options (temperature, num_predict, etc.)
            keep_alive: How long to keep model in memory
            use_cache: Whether to use response caching

        Returns:
            Response dictionary with 'response' key
        """
        options = options or {}

        # Check cache first
        if use_cache:
            cache_key = self._generate_cache_key(model, prompt, options)
            cached = self._get_cached_response(cache_key)
            if cached:
                self.stats['hits'] += 1
                logger.info(f"🚀 CACHE HIT: {model} (hits: {cached.hit_count})")
                return cached.response

        self.stats['misses'] += 1

        # Build request payload
        payload = {
            'model': model,
            'prompt': prompt,
            'stream': False,
            'options': options,
        }

        if system:
            payload['system'] = system

        if keep_alive:
            payload['keep_alive'] = keep_alive
        else:
            payload['keep_alive'] = OLLAMA_CONFIG['OLLAMA_KEEP_ALIVE']

        # Make request
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self.base_url}/api/generate",
                json=payload
            )
            response.raise_for_status()
            result = response.json()

        # Cache successful response
        if use_cache:
            self._cache_response(cache_key, result)
            logger.info(f"📝 Cached response for {model}")

        return result

    async def generate_stream(
        self,
        model: str,
        prompt: str,
        system: Optional[str] = None,
        options: Optional[dict] = None,
        keep_alive: Optional[str] = None,
    ) -> AsyncIterator[str]:
        """
        Stream response from Ollama (no caching for streams).

        Yields:
            Response tokens as they arrive
        """
        options = options or {}

        payload = {
            'model': model,
            'prompt': prompt,
            'stream': True,
            'options': options,
        }

        if system:
            payload['system'] = system

        if keep_alive:
            payload['keep_alive'] = keep_alive
        else:
            payload['keep_alive'] = OLLAMA_CONFIG['OLLAMA_KEEP_ALIVE']

        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream(
                'POST',
                f"{self.base_url}/api/generate",
                json=payload
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line:
                        import json
                        data = json.loads(line)
                        if 'response' in data:
                            yield data['response']

    async def chat(
        self,
        model: str,
        messages: list[dict],
        options: Optional[dict] = None,
        keep_alive: Optional[str] = None,
        use_cache: bool = True,
    ) -> dict:
        """
        Chat completion with caching.

        Args:
            model: Model name
            messages: List of message dicts with 'role' and 'content'
            options: Model options
            keep_alive: How long to keep model in memory
            use_cache: Whether to use response caching

        Returns:
            Response dictionary with 'message' key
        """
        options = options or {}

        # Check cache
        if use_cache:
            # Create cache key from messages
            messages_str = str([(m['role'], m['content']) for m in messages])
            cache_key = self._generate_cache_key(model, messages_str, options)
            cached = self._get_cached_response(cache_key)
            if cached:
                self.stats['hits'] += 1
                logger.info(f"🚀 CACHE HIT: {model} chat")
                return cached.response

        self.stats['misses'] += 1

        payload = {
            'model': model,
            'messages': messages,
            'stream': False,
            'options': options,
        }

        if keep_alive:
            payload['keep_alive'] = keep_alive
        else:
            payload['keep_alive'] = OLLAMA_CONFIG['OLLAMA_KEEP_ALIVE']

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self.base_url}/api/chat",
                json=payload
            )
            response.raise_for_status()
            result = response.json()

        # Cache successful response
        if use_cache:
            self._cache_response(cache_key, result)

        return result

    async def embeddings(
        self,
        model: str,
        input_text: str,
        use_cache: bool = True,
    ) -> list[float]:
        """
        Get embeddings with caching.

        Args:
            model: Embedding model name
            input_text: Text to embed
            use_cache: Whether to use response caching

        Returns:
            List of embedding floats
        """
        if use_cache:
            cache_key = self._generate_cache_key(model, input_text, {})
            cached = self._get_cached_response(cache_key)
            if cached:
                self.stats['hits'] += 1
                return cached.response

        self.stats['misses'] += 1

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{self.base_url}/api/embed",
                json={'model': model, 'input': input_text}
            )
            response.raise_for_status()
            result = response.json()

        embeddings = result.get('embeddings', [[]])[0]

        if use_cache:
            self._cache_response(cache_key, embeddings)

        return embeddings

    def get_cache_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0

        return {
            'total_requests': total_requests,
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'hit_rate_percent': round(hit_rate, 2),
            'evictions': self.stats['evictions'],
            'cache_size': len(self.response_cache),
            'cache_max_size': self.cache_max_size,
        }

    def clear_cache(self) -> int:
        """Clear all cached responses, returns count cleared"""
        count = len(self.response_cache)
        self.response_cache.clear()
        logger.info(f"Cleared {count} cached responses")
        return count

"""
Idempotency Enforcement for outbound connector calls.

Prevents duplicate processing on retries by generating a deterministic
idempotency key per flow execution step and caching the result in Redis.

Enterprise Use Cases:
- Prevents duplicate patient records in Epic on flow retry
- Prevents duplicate financial transactions on retry
- Prevents duplicate SFTP file uploads on retry

Usage:
    from assemblyline_common.retry.idempotency import IdempotencyGuard

    guard = IdempotencyGuard(redis_client)

    # Check before making outbound call
    cached = await guard.check(key)
    if cached is not None:
        return cached  # Already processed

    result = await make_outbound_call()
    await guard.store(key, result)
"""

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Optional, Any, Dict

import redis.asyncio as aioredis

from assemblyline_common.config import settings

logger = logging.getLogger(__name__)


@dataclass
class IdempotencyConfig:
    """Configuration for idempotency enforcement."""
    # Default TTL for cached results (matches typical retry window)
    default_ttl_seconds: int = 3600  # 1 hour
    # Redis key prefix
    key_prefix: str = "idempotency"
    # Maximum cached response size in bytes (prevent Redis bloat)
    max_response_size: int = 1_048_576  # 1 MB


class IdempotencyGuard:
    """
    Redis-backed idempotency guard for outbound calls.

    Generates deterministic keys from flow execution context and caches
    results so that retried operations return the same response without
    re-executing the outbound call.
    """

    def __init__(
        self,
        redis_client: Optional[aioredis.Redis] = None,
        config: Optional[IdempotencyConfig] = None,
    ):
        self.redis = redis_client
        self.config = config or IdempotencyConfig()

    @staticmethod
    def generate_key(
        flow_execution_id: str,
        step_index: int,
        step_type: str,
        target_url: str = "",
    ) -> str:
        """
        Generate a deterministic idempotency key for a flow execution step.

        The key is derived from the execution context so that the same step
        in the same execution always produces the same key, but different
        executions produce different keys.
        """
        raw = f"{flow_execution_id}:{step_index}:{step_type}:{target_url}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_key_from_header(idempotency_key: str) -> str:
        """Use a client-provided Idempotency-Key header value directly."""
        return hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()

    async def check(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Check if a result is already cached for this idempotency key.

        Returns:
            Cached result dict if found, None if not found or Redis unavailable.
        """
        if not self.redis:
            return None

        redis_key = f"{self.config.key_prefix}:{key}"
        try:
            cached = await self.redis.get(redis_key)
            if cached:
                result = json.loads(cached)
                logger.info(
                    "Idempotency cache hit",
                    extra={
                        "event_type": "idempotency.cache_hit",
                        "idempotency_key": key[:16],
                    }
                )
                return result
        except Exception as e:
            logger.warning(f"Idempotency check failed: {e}")

        return None

    async def store(
        self,
        key: str,
        result: Dict[str, Any],
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """
        Store the result of an outbound call for idempotency.

        Returns True if stored successfully.
        """
        if not self.redis:
            return False

        ttl = ttl_seconds or self.config.default_ttl_seconds
        redis_key = f"{self.config.key_prefix}:{key}"

        try:
            serialized = json.dumps(result, default=str)
            if len(serialized) > self.config.max_response_size:
                logger.warning(
                    f"Idempotency response too large ({len(serialized)} bytes), not caching"
                )
                return False

            await self.redis.setex(redis_key, ttl, serialized)
            logger.info(
                "Idempotency result stored",
                extra={
                    "event_type": "idempotency.stored",
                    "idempotency_key": key[:16],
                    "ttl_seconds": ttl,
                }
            )
            return True
        except Exception as e:
            logger.warning(f"Idempotency store failed: {e}")
            return False

    async def invalidate(self, key: str) -> bool:
        """Remove a cached idempotency result (for manual retry forcing)."""
        if not self.redis:
            return False

        redis_key = f"{self.config.key_prefix}:{key}"
        try:
            await self.redis.delete(redis_key)
            return True
        except Exception as e:
            logger.warning(f"Idempotency invalidate failed: {e}")
            return False


# Singleton
_idempotency_guard: Optional[IdempotencyGuard] = None


async def get_idempotency_guard(
    config: Optional[IdempotencyConfig] = None,
) -> IdempotencyGuard:
    """Get singleton idempotency guard with Redis connection."""
    global _idempotency_guard

    if _idempotency_guard is None:
        redis_client = None
        try:
            redis_client = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
            )
            await redis_client.ping()
        except Exception as e:
            logger.warning(f"Idempotency Redis unavailable: {e}")
            redis_client = None

        _idempotency_guard = IdempotencyGuard(
            redis_client=redis_client,
            config=config or IdempotencyConfig(),
        )

    return _idempotency_guard

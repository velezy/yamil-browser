"""
Per-Connector Rate Limiter with Redis backend.

Enforces external API rate limits per {tenant_id}:{connector_id} using
a distributed token bucket in Redis. This prevents any single tenant
from exceeding vendor API limits (e.g., Epic: 100 req/min).

Usage:
    from assemblyline_common.connectors.rate_limiter import (
        ConnectorRateLimiter, ConnectorRateLimitExceeded,
    )

    limiter = ConnectorRateLimiter(redis_url="redis://localhost:6379/0")

    # Before making a connector call:
    result = await limiter.acquire("tenant-uuid", "connector-uuid", "epic-fhir")
    if not result.allowed:
        raise ConnectorRateLimitExceeded(result.retry_after)
"""

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)

# Default rate limits per connector type (requests, window_seconds)
# These match known vendor API limits with a safety margin.
CONNECTOR_TYPE_LIMITS: Dict[str, tuple[int, int]] = {
    "epic-fhir": (90, 60),       # Epic: ~100/min, we use 90 for safety
    "cerner-fhir": (90, 60),
    "fhir": (100, 60),
    "salesforce": (100, 60),      # Salesforce: 100 concurrent, we use per-min
    "databricks": (30, 60),       # Databricks: varies by tier
    "http": (200, 60),            # Generic HTTP: generous default
    "database": (500, 60),        # Database connectors: high throughput
    "dynamodb": (200, 60),        # DynamoDB: depends on capacity
    "cosmosdb": (200, 60),
    "s3": (300, 60),              # S3: 5500 GET/s, but we limit writes
    "kafka": (1000, 60),          # Kafka: high throughput
    "sqs": (300, 60),             # SQS: 300 SendMessage/s standard
    "rabbitmq": (500, 60),
    "sftp": (50, 60),             # SFTP: conservative
    "email": (30, 60),            # Email: prevent spam
    "sendgrid": (30, 60),
    "mailgun": (30, 60),
    "ai": (20, 60),               # AI/LLM: expensive, conservative
    "bedrock": (20, 60),
    "azure-openai": (20, 60),
    "grpc": (200, 60),
    "websocket": (100, 60),
    "mllp": (100, 60),
    "document-ai": (30, 60),
    "jira_cloud": (60, 60),          # Jira Cloud: 60 req/min conservative
}

# Lua script for atomic token bucket in Redis.
# Returns: [allowed (0/1), tokens_remaining, retry_after_ms]
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_refill = now
end

-- Refill tokens based on elapsed time
local elapsed = now - last_refill
local refill_rate = capacity / window
local new_tokens = tokens + (elapsed * refill_rate)
if new_tokens > capacity then
    new_tokens = capacity
end

if new_tokens >= requested then
    new_tokens = new_tokens - requested
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
    redis.call('EXPIRE', key, window * 2)
    return {1, math.floor(new_tokens), 0}
else
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
    redis.call('EXPIRE', key, window * 2)
    local wait_ms = math.ceil((requested - new_tokens) / refill_rate * 1000)
    return {0, math.floor(new_tokens), wait_ms}
end
"""


class ConnectorRateLimitExceeded(Exception):
    """Raised when a connector rate limit is exceeded."""

    def __init__(self, retry_after: int, connector_type: str = ""):
        self.retry_after = retry_after
        self.connector_type = connector_type
        super().__init__(
            f"Rate limit exceeded for connector type '{connector_type}'. "
            f"Retry after {retry_after}s."
        )


@dataclass
class ConnectorRateLimitResult:
    """Result of a connector rate limit check."""
    allowed: bool
    remaining: int
    retry_after: int  # seconds (0 if allowed)
    connector_type: str = ""
    tenant_id: str = ""
    connector_id: str = ""


class ConnectorRateLimiter:
    """
    Distributed per-connector rate limiter using Redis token bucket.

    Rate limits are enforced per {tenant_id}:{connector_id} so each
    tenant's connector instance has its own bucket. Limits default to
    the connector type's vendor limit but can be overridden per connector.
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        key_prefix: str = "conn_rl",
        custom_limits: Optional[Dict[str, tuple[int, int]]] = None,
    ):
        self._redis_url = redis_url or os.getenv(
            "REDIS_URL", "redis://localhost:6379/0"
        )
        self._key_prefix = key_prefix
        self._redis: Optional[redis.Redis] = None
        self._lua_sha: Optional[str] = None
        self._custom_limits = custom_limits or {}

    async def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.from_url(
                self._redis_url, decode_responses=True
            )
        return self._redis

    async def _ensure_lua(self, r: redis.Redis) -> str:
        if self._lua_sha is None:
            self._lua_sha = await r.script_load(TOKEN_BUCKET_LUA)
        return self._lua_sha

    def get_limits(
        self, connector_type: str, override: Optional[tuple[int, int]] = None
    ) -> tuple[int, int]:
        """Get (requests, window_seconds) for a connector type."""
        if override:
            return override
        if connector_type in self._custom_limits:
            return self._custom_limits[connector_type]
        return CONNECTOR_TYPE_LIMITS.get(connector_type, (100, 60))

    async def acquire(
        self,
        tenant_id: str,
        connector_id: str,
        connector_type: str,
        override_limit: Optional[tuple[int, int]] = None,
        tokens: int = 1,
    ) -> ConnectorRateLimitResult:
        """
        Attempt to acquire tokens for a connector call.

        Args:
            tenant_id: Tenant UUID
            connector_id: Connector UUID
            connector_type: Type key (e.g., "epic-fhir", "http")
            override_limit: Optional (requests, window) override
            tokens: Number of tokens to consume (default 1)

        Returns:
            ConnectorRateLimitResult with allowed/remaining/retry_after
        """
        capacity, window = self.get_limits(connector_type, override_limit)
        key = f"{self._key_prefix}:{tenant_id}:{connector_id}"

        try:
            r = await self._get_redis()
            sha = await self._ensure_lua(r)
            now = time.time()

            result = await r.evalsha(
                sha, 1, key, capacity, window, now, tokens
            )
            allowed = bool(result[0])
            remaining = int(result[1])
            retry_after_ms = int(result[2])
            retry_after = max(1, retry_after_ms // 1000) if not allowed else 0

            return ConnectorRateLimitResult(
                allowed=allowed,
                remaining=remaining,
                retry_after=retry_after,
                connector_type=connector_type,
                tenant_id=tenant_id,
                connector_id=connector_id,
            )
        except Exception as e:
            # Fail open: if Redis is unavailable, allow the request
            logger.warning(
                "Connector rate limiter failed (allowing request): %s", e
            )
            return ConnectorRateLimitResult(
                allowed=True,
                remaining=capacity,
                retry_after=0,
                connector_type=connector_type,
                tenant_id=tenant_id,
                connector_id=connector_id,
            )

    async def check_or_raise(
        self,
        tenant_id: str,
        connector_id: str,
        connector_type: str,
        override_limit: Optional[tuple[int, int]] = None,
    ) -> ConnectorRateLimitResult:
        """
        Check rate limit and raise ConnectorRateLimitExceeded if denied.

        Convenience wrapper around acquire() for use in endpoint handlers.
        """
        result = await self.acquire(
            tenant_id, connector_id, connector_type, override_limit
        )
        if not result.allowed:
            raise ConnectorRateLimitExceeded(
                retry_after=result.retry_after,
                connector_type=connector_type,
            )
        return result

    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            self._lua_sha = None


# Singleton instance
_limiter: Optional[ConnectorRateLimiter] = None


def get_connector_rate_limiter() -> ConnectorRateLimiter:
    """Get or create the singleton connector rate limiter."""
    global _limiter
    if _limiter is None:
        _limiter = ConnectorRateLimiter()
    return _limiter

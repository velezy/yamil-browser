"""
Global Rate Limiter with Redis backend.

Implements token bucket algorithm with support for:
- Per-tenant limits
- Per-user limits
- Per-endpoint limits
- Burst allowance
"""

import asyncio
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from enum import Enum

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RateLimitScope(Enum):
    """Scope for rate limiting."""
    GLOBAL = "global"
    TENANT = "tenant"
    USER = "user"
    ENDPOINT = "endpoint"
    IP = "ip"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    # Token bucket settings
    requests_per_second: float = 100.0
    burst_size: int = 200  # Maximum tokens in bucket

    # Scope-specific limits (requests per second)
    tenant_rps: float = 1000.0
    user_rps: float = 100.0
    endpoint_rps: float = 500.0
    ip_rps: float = 50.0

    # Burst multipliers
    tenant_burst_multiplier: float = 2.0
    user_burst_multiplier: float = 1.5
    endpoint_burst_multiplier: float = 2.0
    ip_burst_multiplier: float = 1.5

    # Redis settings
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    key_prefix: str = "ratelimit"

    # Behavior
    block_duration_seconds: int = 60  # How long to block after limit exceeded
    enable_headers: bool = True  # Include rate limit headers in response


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    limit: int
    reset_at: float  # Unix timestamp
    retry_after: Optional[int] = None  # Seconds until retry allowed
    scope: RateLimitScope = RateLimitScope.GLOBAL

    def to_headers(self) -> Dict[str, str]:
        """Convert to HTTP headers."""
        headers = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(int(self.reset_at)),
        }
        if self.retry_after:
            headers["Retry-After"] = str(self.retry_after)
        return headers


class TokenBucket:
    """
    Token bucket implementation for rate limiting.

    Uses Redis for distributed state with Lua script for atomicity.
    """

    # Lua script for atomic token bucket operation
    LUA_SCRIPT = """
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local rate = tonumber(ARGV[2])
    local burst = tonumber(ARGV[3])
    local requested = tonumber(ARGV[4])
    local ttl = tonumber(ARGV[5])

    -- Get current bucket state
    local bucket = redis.call('HMGET', key, 'tokens', 'last_update')
    local tokens = tonumber(bucket[1])
    local last_update = tonumber(bucket[2])

    -- Initialize if not exists
    if tokens == nil then
        tokens = burst
        last_update = now
    end

    -- Calculate tokens to add based on time elapsed
    local elapsed = now - last_update
    local new_tokens = elapsed * rate
    tokens = math.min(burst, tokens + new_tokens)

    -- Check if we have enough tokens
    local allowed = 0
    local remaining = tokens

    if tokens >= requested then
        tokens = tokens - requested
        remaining = tokens
        allowed = 1
    end

    -- Update bucket state
    redis.call('HMSET', key, 'tokens', tokens, 'last_update', now)
    redis.call('EXPIRE', key, ttl)

    return {allowed, math.floor(remaining), burst}
    """

    def __init__(self, redis_client: redis.Redis, config: RateLimitConfig):
        self.redis = redis_client
        self.config = config
        self._script_sha: Optional[str] = None

    async def _ensure_script(self) -> str:
        """Load Lua script if not already loaded."""
        if self._script_sha is None:
            self._script_sha = await self.redis.script_load(self.LUA_SCRIPT)
        return self._script_sha

    async def consume(
        self,
        key: str,
        rate: float,
        burst: int,
        tokens: int = 1
    ) -> RateLimitResult:
        """
        Try to consume tokens from the bucket.

        Args:
            key: Unique identifier for this bucket
            rate: Tokens per second to add
            burst: Maximum tokens in bucket
            tokens: Number of tokens to consume

        Returns:
            RateLimitResult with allowed status and remaining tokens
        """
        now = time.time()
        ttl = max(3600, int(burst / rate * 2))  # TTL based on refill time

        try:
            script_sha = await self._ensure_script()
            result = await self.redis.evalsha(
                script_sha,
                1,
                key,
                now,
                rate,
                burst,
                tokens,
                ttl
            )

            allowed = bool(result[0])
            remaining = int(result[1])
            limit = int(result[2])

            # Calculate reset time (when bucket will be full again)
            tokens_needed = limit - remaining
            reset_at = now + (tokens_needed / rate) if tokens_needed > 0 else now

            return RateLimitResult(
                allowed=allowed,
                remaining=remaining,
                limit=limit,
                reset_at=reset_at,
                retry_after=int(1 / rate) if not allowed else None
            )

        except redis.RedisError as e:
            logger.warning(f"Redis error in rate limiter, allowing request: {e}")
            # Fail open - allow request if Redis is unavailable
            return RateLimitResult(
                allowed=True,
                remaining=burst,
                limit=burst,
                reset_at=now + 1
            )


class RateLimiter:
    """
    Multi-scope rate limiter with Redis backend.

    Supports hierarchical rate limiting:
    - Global limits
    - Per-tenant limits
    - Per-user limits
    - Per-endpoint limits
    - Per-IP limits
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._redis: Optional[redis.Redis] = None
        self._bucket: Optional[TokenBucket] = None
        self._initialized = False

    async def initialize(self):
        """Initialize Redis connection."""
        if self._initialized:
            return

        try:
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            await self._redis.ping()
            self._bucket = TokenBucket(self._redis, self.config)
            self._initialized = True
            logger.info("Rate limiter initialized with Redis backend")
        except redis.RedisError as e:
            logger.warning(f"Failed to connect to Redis for rate limiting: {e}")
            self._initialized = True  # Mark as initialized to avoid retry loops

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            self._bucket = None
            self._initialized = False

    def _make_key(self, scope: RateLimitScope, identifier: str) -> str:
        """Create Redis key for rate limit bucket."""
        return f"{self.config.key_prefix}:{scope.value}:{identifier}"

    async def check(
        self,
        tenant_id: Optional[str] = None,
        user_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        ip_address: Optional[str] = None,
        tokens: int = 1
    ) -> RateLimitResult:
        """
        Check rate limits across all applicable scopes.

        Returns the most restrictive result.

        Args:
            tenant_id: Tenant identifier
            user_id: User identifier
            endpoint: API endpoint path
            ip_address: Client IP address
            tokens: Number of tokens to consume

        Returns:
            RateLimitResult with the most restrictive limit
        """
        await self.initialize()

        if not self._bucket:
            # Redis not available, allow all requests
            return RateLimitResult(
                allowed=True,
                remaining=self.config.burst_size,
                limit=self.config.burst_size,
                reset_at=time.time() + 1
            )

        results: list[RateLimitResult] = []

        # Check each scope
        if tenant_id:
            result = await self._bucket.consume(
                self._make_key(RateLimitScope.TENANT, tenant_id),
                self.config.tenant_rps,
                int(self.config.tenant_rps * self.config.tenant_burst_multiplier),
                tokens
            )
            result.scope = RateLimitScope.TENANT
            results.append(result)

        if user_id:
            result = await self._bucket.consume(
                self._make_key(RateLimitScope.USER, user_id),
                self.config.user_rps,
                int(self.config.user_rps * self.config.user_burst_multiplier),
                tokens
            )
            result.scope = RateLimitScope.USER
            results.append(result)

        if endpoint:
            result = await self._bucket.consume(
                self._make_key(RateLimitScope.ENDPOINT, endpoint),
                self.config.endpoint_rps,
                int(self.config.endpoint_rps * self.config.endpoint_burst_multiplier),
                tokens
            )
            result.scope = RateLimitScope.ENDPOINT
            results.append(result)

        if ip_address:
            result = await self._bucket.consume(
                self._make_key(RateLimitScope.IP, ip_address),
                self.config.ip_rps,
                int(self.config.ip_rps * self.config.ip_burst_multiplier),
                tokens
            )
            result.scope = RateLimitScope.IP
            results.append(result)

        # If no specific scope, use global
        if not results:
            result = await self._bucket.consume(
                self._make_key(RateLimitScope.GLOBAL, "default"),
                self.config.requests_per_second,
                self.config.burst_size,
                tokens
            )
            result.scope = RateLimitScope.GLOBAL
            results.append(result)

        # Return the most restrictive result
        denied_results = [r for r in results if not r.allowed]
        if denied_results:
            # Return the first denied result
            return denied_results[0]

        # All allowed - return the one with lowest remaining
        return min(results, key=lambda r: r.remaining)

    async def reset(
        self,
        scope: RateLimitScope,
        identifier: str
    ):
        """Reset rate limit for a specific scope and identifier."""
        await self.initialize()
        if self._redis:
            key = self._make_key(scope, identifier)
            await self._redis.delete(key)
            logger.info(f"Reset rate limit for {scope.value}:{identifier}")

    async def get_status(
        self,
        scope: RateLimitScope,
        identifier: str
    ) -> Optional[Dict[str, Any]]:
        """Get current rate limit status for a scope."""
        await self.initialize()
        if not self._redis:
            return None

        key = self._make_key(scope, identifier)
        data = await self._redis.hgetall(key)

        if not data:
            return None

        return {
            "tokens": float(data.get("tokens", 0)),
            "last_update": float(data.get("last_update", 0)),
            "scope": scope.value,
            "identifier": identifier
        }


# Singleton instance
_rate_limiter: Optional[RateLimiter] = None
_rate_limiter_lock = asyncio.Lock()


async def get_rate_limiter(config: Optional[RateLimitConfig] = None) -> RateLimiter:
    """Get or create the singleton rate limiter instance."""
    global _rate_limiter

    async with _rate_limiter_lock:
        if _rate_limiter is None:
            _rate_limiter = RateLimiter(config)
            await _rate_limiter.initialize()
        return _rate_limiter


# FastAPI middleware helper
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for rate limiting."""

    def __init__(self, app, config: Optional[RateLimitConfig] = None):
        super().__init__(app)
        self.config = config or RateLimitConfig()

    async def dispatch(self, request: Request, call_next) -> Response:
        """Check rate limits before processing request."""
        limiter = await get_rate_limiter(self.config)

        # Extract identifiers from request
        tenant_id = getattr(request.state, "tenant_id", None)
        user_id = getattr(request.state, "user_id", None)
        endpoint = request.url.path
        ip_address = request.client.host if request.client else None

        result = await limiter.check(
            tenant_id=tenant_id,
            user_id=user_id,
            endpoint=endpoint,
            ip_address=ip_address
        )

        if not result.allowed:
            response = Response(
                content='{"detail": "Rate limit exceeded"}',
                status_code=429,
                media_type="application/json"
            )
            if self.config.enable_headers:
                for key, value in result.to_headers().items():
                    response.headers[key] = value
            return response

        # Process request
        response = await call_next(request)

        # Add rate limit headers to response
        if self.config.enable_headers:
            for key, value in result.to_headers().items():
                response.headers[key] = value

        return response

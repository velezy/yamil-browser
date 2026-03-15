"""
T.A.L.O.S. DragonflyDB Cache
Drop-in Redis replacement - 25x faster (25M vs 1M ops/sec)

Features:
- Multi-threaded (uses all CPU cores)
- Redis-compatible API
- No code changes from Redis
- Memory efficient with shared-nothing architecture
"""

import os
import logging
from typing import Optional, Any, Dict, List
from datetime import timedelta
import json

logger = logging.getLogger(__name__)

# =============================================================================
# REDIS/DRAGONFLY AVAILABILITY CHECK
# =============================================================================

DRAGONFLY_AVAILABLE = False

try:
    import redis.asyncio as aioredis
    import redis
    DRAGONFLY_AVAILABLE = True
    logger.info("Redis/DragonflyDB client loaded successfully")
except ImportError:
    logger.warning("Redis not installed. Run: pip install redis")


# =============================================================================
# CONFIGURATION
# =============================================================================

class DragonflyConfig:
    """DragonflyDB configuration"""

    def __init__(self):
        # DragonflyDB is Redis-compatible, use same env vars with fallback
        self.host = os.getenv("DRAGONFLY_HOST", os.getenv("REDIS_HOST", "localhost"))
        self.port = int(os.getenv("DRAGONFLY_PORT", os.getenv("REDIS_PORT", "6379")))
        self.password = os.getenv("DRAGONFLY_PASSWORD", os.getenv("REDIS_PASSWORD"))
        self.db = int(os.getenv("DRAGONFLY_DB", os.getenv("REDIS_DB", "0")))

        # Connection pool settings
        self.max_connections = int(os.getenv("DRAGONFLY_MAX_CONNECTIONS", "100"))
        self.socket_timeout = float(os.getenv("DRAGONFLY_SOCKET_TIMEOUT", "5.0"))
        self.socket_connect_timeout = float(os.getenv("DRAGONFLY_CONNECT_TIMEOUT", "5.0"))

        # DragonflyDB specific settings
        self.decode_responses = True

    @property
    def url(self) -> str:
        """Get connection URL"""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


# =============================================================================
# ASYNC DRAGONFLY CLIENT
# =============================================================================

class AsyncDragonflyCache:
    """
    Async DragonflyDB/Redis cache client.

    DragonflyDB is 25x faster than Redis:
    - 25 million ops/sec vs 1 million
    - Multi-threaded (uses all CPU cores)
    - Same Redis API, no code changes
    """

    def __init__(self, config: Optional[DragonflyConfig] = None):
        self.config = config or DragonflyConfig()
        self._pool: Optional[aioredis.ConnectionPool] = None
        self._client: Optional[aioredis.Redis] = None

    async def connect(self):
        """Connect to DragonflyDB"""
        if not DRAGONFLY_AVAILABLE:
            logger.warning("Redis/DragonflyDB client not available")
            return False

        try:
            self._pool = aioredis.ConnectionPool.from_url(
                self.config.url,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                decode_responses=self.config.decode_responses
            )
            self._client = aioredis.Redis(connection_pool=self._pool)

            # Test connection
            await self._client.ping()
            logger.info(f"Connected to DragonflyDB at {self.config.host}:{self.config.port}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to DragonflyDB: {e}")
            return False

    async def close(self):
        """Close connection"""
        if self._client:
            await self._client.close()
        if self._pool:
            await self._pool.disconnect()
        self._client = None
        self._pool = None

    async def _ensure_connected(self):
        """Ensure connection is established"""
        if not self._client:
            await self.connect()

    # =========================================================================
    # BASIC OPERATIONS
    # =========================================================================

    async def get(self, key: str) -> Optional[str]:
        """Get value by key"""
        await self._ensure_connected()
        if not self._client:
            return None
        return await self._client.get(key)

    async def set(
        self,
        key: str,
        value: str,
        ttl: Optional[int] = None
    ) -> bool:
        """Set value with optional TTL in seconds"""
        await self._ensure_connected()
        if not self._client:
            return False

        try:
            if ttl:
                await self._client.setex(key, ttl, value)
            else:
                await self._client.set(key, value)
            return True
        except Exception as e:
            logger.error(f"Set failed: {e}")
            return False

    async def delete(self, *keys: str) -> int:
        """Delete one or more keys"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.delete(*keys)

    async def exists(self, *keys: str) -> int:
        """Check if keys exist"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.exists(*keys)

    async def expire(self, key: str, seconds: int) -> bool:
        """Set TTL on a key"""
        await self._ensure_connected()
        if not self._client:
            return False
        return await self._client.expire(key, seconds)

    async def ttl(self, key: str) -> int:
        """Get TTL of a key"""
        await self._ensure_connected()
        if not self._client:
            return -2
        return await self._client.ttl(key)

    # =========================================================================
    # JSON OPERATIONS
    # =========================================================================

    async def get_json(self, key: str) -> Optional[Any]:
        """Get JSON value"""
        value = await self.get(key)
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return None

    async def set_json(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """Set JSON value"""
        try:
            json_str = json.dumps(value)
            return await self.set(key, json_str, ttl)
        except (TypeError, ValueError) as e:
            logger.error(f"JSON serialization failed: {e}")
            return False

    # =========================================================================
    # HASH OPERATIONS
    # =========================================================================

    async def hget(self, name: str, key: str) -> Optional[str]:
        """Get hash field"""
        await self._ensure_connected()
        if not self._client:
            return None
        return await self._client.hget(name, key)

    async def hset(self, name: str, key: str, value: str) -> int:
        """Set hash field"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.hset(name, key, value)

    async def hgetall(self, name: str) -> Dict[str, str]:
        """Get all hash fields"""
        await self._ensure_connected()
        if not self._client:
            return {}
        return await self._client.hgetall(name)

    async def hdel(self, name: str, *keys: str) -> int:
        """Delete hash fields"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.hdel(name, *keys)

    # =========================================================================
    # LIST OPERATIONS
    # =========================================================================

    async def lpush(self, name: str, *values: str) -> int:
        """Push to left of list"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.lpush(name, *values)

    async def rpush(self, name: str, *values: str) -> int:
        """Push to right of list"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.rpush(name, *values)

    async def lrange(self, name: str, start: int, end: int) -> List[str]:
        """Get list range"""
        await self._ensure_connected()
        if not self._client:
            return []
        return await self._client.lrange(name, start, end)

    async def llen(self, name: str) -> int:
        """Get list length"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.llen(name)

    # =========================================================================
    # SET OPERATIONS
    # =========================================================================

    async def sadd(self, name: str, *values: str) -> int:
        """Add to set"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.sadd(name, *values)

    async def smembers(self, name: str) -> set:
        """Get all set members"""
        await self._ensure_connected()
        if not self._client:
            return set()
        return await self._client.smembers(name)

    async def sismember(self, name: str, value: str) -> bool:
        """Check if value is in set"""
        await self._ensure_connected()
        if not self._client:
            return False
        return await self._client.sismember(name, value)

    # =========================================================================
    # SORTED SET OPERATIONS (useful for leaderboards, rate limiting)
    # =========================================================================

    async def zadd(
        self,
        name: str,
        mapping: Dict[str, float],
        nx: bool = False,
        xx: bool = False
    ) -> int:
        """Add to sorted set"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.zadd(name, mapping, nx=nx, xx=xx)

    async def zrange(
        self,
        name: str,
        start: int,
        end: int,
        withscores: bool = False
    ) -> List:
        """Get sorted set range"""
        await self._ensure_connected()
        if not self._client:
            return []
        return await self._client.zrange(name, start, end, withscores=withscores)

    async def zrem(self, name: str, *values: str) -> int:
        """Remove from sorted set"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.zrem(name, *values)

    # =========================================================================
    # PUBSUB (for real-time features)
    # =========================================================================

    async def publish(self, channel: str, message: str) -> int:
        """Publish message to channel"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.publish(channel, message)

    def pubsub(self):
        """Get pubsub instance"""
        if not self._client:
            return None
        return self._client.pubsub()

    # =========================================================================
    # PIPELINE (batch operations)
    # =========================================================================

    def pipeline(self):
        """Get pipeline for batch operations"""
        if not self._client:
            return None
        return self._client.pipeline()

    # =========================================================================
    # UTILITY
    # =========================================================================

    async def info(self) -> Dict[str, Any]:
        """Get server info"""
        await self._ensure_connected()
        if not self._client:
            return {}
        return await self._client.info()

    async def dbsize(self) -> int:
        """Get number of keys"""
        await self._ensure_connected()
        if not self._client:
            return 0
        return await self._client.dbsize()

    async def flushdb(self) -> bool:
        """Flush current database"""
        await self._ensure_connected()
        if not self._client:
            return False
        return await self._client.flushdb()

    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern"""
        await self._ensure_connected()
        if not self._client:
            return []
        return await self._client.keys(pattern)


# =============================================================================
# SPECIALIZED CACHES
# =============================================================================

class AIResponseCache:
    """Cache for AI responses - avoid re-generating identical queries"""

    def __init__(self, cache: AsyncDragonflyCache, prefix: str = "ai:response"):
        self.cache = cache
        self.prefix = prefix
        self.default_ttl = 3600  # 1 hour

    def _make_key(self, query_hash: str, model: str) -> str:
        return f"{self.prefix}:{model}:{query_hash}"

    async def get_response(
        self,
        query_hash: str,
        model: str
    ) -> Optional[Dict[str, Any]]:
        """Get cached AI response"""
        key = self._make_key(query_hash, model)
        return await self.cache.get_json(key)

    async def cache_response(
        self,
        query_hash: str,
        model: str,
        response: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """Cache AI response"""
        key = self._make_key(query_hash, model)
        return await self.cache.set_json(key, response, ttl or self.default_ttl)


class EmbeddingCache:
    """Cache for embeddings - avoid re-computing"""

    def __init__(self, cache: AsyncDragonflyCache, prefix: str = "embedding"):
        self.cache = cache
        self.prefix = prefix
        self.default_ttl = 86400  # 24 hours

    def _make_key(self, text_hash: str, model: str) -> str:
        return f"{self.prefix}:{model}:{text_hash}"

    async def get_embedding(
        self,
        text_hash: str,
        model: str
    ) -> Optional[List[float]]:
        """Get cached embedding"""
        key = self._make_key(text_hash, model)
        return await self.cache.get_json(key)

    async def cache_embedding(
        self,
        text_hash: str,
        model: str,
        embedding: List[float],
        ttl: Optional[int] = None
    ) -> bool:
        """Cache embedding"""
        key = self._make_key(text_hash, model)
        return await self.cache.set_json(key, embedding, ttl or self.default_ttl)


class RateLimiter:
    """Rate limiting using DragonflyDB sorted sets"""

    def __init__(self, cache: AsyncDragonflyCache, prefix: str = "ratelimit"):
        self.cache = cache
        self.prefix = prefix

    async def is_allowed(
        self,
        identifier: str,
        max_requests: int,
        window_seconds: int
    ) -> tuple[bool, int]:
        """
        Check if request is allowed.

        Returns:
            (is_allowed, remaining_requests)
        """
        import time
        key = f"{self.prefix}:{identifier}"
        now = time.time()
        window_start = now - window_seconds

        await self.cache._ensure_connected()
        if not self.cache._client:
            return True, max_requests

        pipe = self.cache.pipeline()

        # Remove old entries
        pipe.zremrangebyscore(key, 0, window_start)
        # Count current entries
        pipe.zcard(key)
        # Add new entry
        pipe.zadd(key, {str(now): now})
        # Set expiry
        pipe.expire(key, window_seconds)

        results = await pipe.execute()
        current_count = results[1]

        remaining = max(0, max_requests - current_count - 1)
        allowed = current_count < max_requests

        return allowed, remaining

    async def is_allowed_for_user(
        self,
        user_id: int,
        endpoint: str,
        max_requests: int,
        window_seconds: int = 60
    ) -> tuple[bool, int, int]:
        """
        Check if request is allowed for a specific user on an endpoint.

        Args:
            user_id: The user's ID
            endpoint: The endpoint path (e.g., "/send/stream")
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds (default 60)

        Returns:
            (is_allowed, remaining_requests, retry_after_seconds)
        """
        import time
        key = f"{self.prefix}:user:{user_id}:{endpoint}"
        now = time.time()
        window_start = now - window_seconds

        await self.cache._ensure_connected()
        if not self.cache._client:
            return True, max_requests, 0

        pipe = self.cache.pipeline()

        # Remove old entries
        pipe.zremrangebyscore(key, 0, window_start)
        # Count current entries
        pipe.zcard(key)
        # Get oldest entry to calculate retry-after
        pipe.zrange(key, 0, 0, withscores=True)
        # Add new entry
        pipe.zadd(key, {str(now): now})
        # Set expiry
        pipe.expire(key, window_seconds)

        results = await pipe.execute()
        current_count = results[1]
        oldest_entry = results[2]

        remaining = max(0, max_requests - current_count - 1)
        allowed = current_count < max_requests

        # Calculate retry-after (when the oldest request will expire)
        retry_after = 0
        if not allowed and oldest_entry:
            oldest_time = oldest_entry[0][1]
            retry_after = int(oldest_time + window_seconds - now) + 1

        return allowed, remaining, retry_after

    async def is_allowed_for_ip(
        self,
        ip_address: str,
        endpoint: str,
        max_requests: int,
        window_seconds: int = 60
    ) -> tuple[bool, int, int]:
        """
        Check if request is allowed for a specific IP on an endpoint.

        Args:
            ip_address: The client's IP address
            endpoint: The endpoint path
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds

        Returns:
            (is_allowed, remaining_requests, retry_after_seconds)
        """
        import time
        key = f"{self.prefix}:ip:{ip_address}:{endpoint}"
        now = time.time()
        window_start = now - window_seconds

        await self.cache._ensure_connected()
        if not self.cache._client:
            return True, max_requests, 0

        pipe = self.cache.pipeline()

        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zcard(key)
        pipe.zrange(key, 0, 0, withscores=True)
        pipe.zadd(key, {str(now): now})
        pipe.expire(key, window_seconds)

        results = await pipe.execute()
        current_count = results[1]
        oldest_entry = results[2]

        remaining = max(0, max_requests - current_count - 1)
        allowed = current_count < max_requests

        retry_after = 0
        if not allowed and oldest_entry:
            oldest_time = oldest_entry[0][1]
            retry_after = int(oldest_time + window_seconds - now) + 1

        return allowed, remaining, retry_after


# Default rate limits per endpoint (requests per window)
DEFAULT_RATE_LIMITS = {
    "/send/stream": 30,
    "/send": 60,
    "/query": 30,
    "/process": 30,
    "/voice/transcribe": 10,
    "/documents/upload": 10,
    "/evaluate/ragas": 20,
    "/quality/evaluate": 30,
}

# Custom rate limit windows per endpoint (seconds, default is 60)
DEFAULT_RATE_WINDOWS = {
    "/documents/upload": 3600,  # 10 uploads per hour
}


# Global rate limiter instance
_rate_limiter: Optional[RateLimiter] = None


async def get_rate_limiter() -> Optional[RateLimiter]:
    """Get or create rate limiter singleton"""
    global _rate_limiter
    if _rate_limiter is None:
        cache = await get_cache()
        if cache:
            _rate_limiter = RateLimiter(cache)
    return _rate_limiter


async def check_rate_limit(
    user_id: Optional[int],
    ip_address: str,
    endpoint: str,
    max_requests: Optional[int] = None
) -> tuple[bool, dict]:
    """
    Check rate limit for a request.

    Args:
        user_id: User ID (preferred for rate limiting)
        ip_address: Client IP (fallback if no user_id)
        endpoint: The endpoint path
        max_requests: Override default rate limit

    Returns:
        (is_allowed, headers_dict)
        headers_dict contains X-RateLimit-* headers for the response
    """
    rate_limiter = await get_rate_limiter()

    if not rate_limiter:
        # No rate limiter available, allow all
        return True, {}

    # Get rate limit for endpoint
    limit = max_requests or DEFAULT_RATE_LIMITS.get(endpoint, 60)
    window = DEFAULT_RATE_WINDOWS.get(endpoint, 60)

    # Prefer user-based rate limiting
    if user_id:
        allowed, remaining, retry_after = await rate_limiter.is_allowed_for_user(
            user_id=user_id,
            endpoint=endpoint,
            max_requests=limit,
            window_seconds=window
        )
    else:
        allowed, remaining, retry_after = await rate_limiter.is_allowed_for_ip(
            ip_address=ip_address,
            endpoint=endpoint,
            max_requests=limit,
            window_seconds=window
        )

    headers = {
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": str(remaining),
        "X-RateLimit-Reset": str(retry_after) if not allowed else "0",
    }

    if not allowed:
        headers["Retry-After"] = str(retry_after)

    return allowed, headers


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_cache: Optional[AsyncDragonflyCache] = None


async def get_cache() -> AsyncDragonflyCache:
    """Get or create cache singleton"""
    global _cache
    if _cache is None:
        _cache = AsyncDragonflyCache()
        await _cache.connect()
    return _cache


def is_dragonfly_available() -> bool:
    """Check if DragonflyDB/Redis client is available"""
    return DRAGONFLY_AVAILABLE


# =============================================================================
# DRAGONFLY SETUP INSTRUCTIONS
# =============================================================================
"""
DragonflyDB Installation (macOS):
    brew install dragonflydb/dragonfly/dragonfly

Start DragonflyDB:
    dragonfly --logtostderr

Or with Docker:
    docker run -p 6379:6379 docker.dragonflydb.io/dragonflydb/dragonfly

Performance Comparison:
    Redis:      ~1 million ops/sec (single-threaded)
    DragonflyDB: ~25 million ops/sec (multi-threaded)

No code changes needed - it's a drop-in replacement!
"""

"""
T.A.L.O.S. Redis Cache Layer
Based on Memobytes patterns

Features:
- AI response caching
- Session management
- OCR results cache
- Rate limiting
- Real-time chat state
"""

import os
import json
import hashlib
import pickle
import logging
from typing import Optional, Any, Dict, List, Union
from datetime import timedelta
from functools import wraps

logger = logging.getLogger(__name__)

# =============================================================================
# REDIS AVAILABILITY CHECK
# =============================================================================

REDIS_AVAILABLE = False

try:
    import redis
    from redis import asyncio as aioredis
    REDIS_AVAILABLE = True
    logger.info("Redis library loaded successfully")
except ImportError:
    logger.warning("Redis not installed. Run: pip install redis")


# =============================================================================
# CONFIGURATION
# =============================================================================

class RedisConfig:
    """Redis configuration"""
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", 6379))
    password: Optional[str] = os.getenv("REDIS_PASSWORD")

    # Database assignments (like Memobytes)
    DB_SESSION = 0
    DB_AI_CACHE = 1
    DB_OCR_CACHE = 2
    DB_RATE_LIMIT = 3
    DB_CHAT_STATE = 4

    # TTL defaults
    TTL_SESSION = 3600 * 24  # 24 hours
    TTL_AI_RESPONSE = 3600  # 1 hour
    TTL_OCR_RESULT = 3600 * 24 * 7  # 7 days
    TTL_RATE_LIMIT = 60  # 1 minute
    TTL_CHAT_STATE = 3600 * 2  # 2 hours


# =============================================================================
# SYNC REDIS CLIENT
# =============================================================================

class RedisCache:
    """
    Synchronous Redis cache client.

    Usage:
        cache = RedisCache()
        cache.set("key", {"data": "value"})
        data = cache.get("key")
    """

    def __init__(self, db: int = RedisConfig.DB_AI_CACHE):
        self.config = RedisConfig()
        self.db = db
        self._client = None

    @property
    def client(self):
        """Lazy connection to Redis"""
        if self._client is None:
            if not REDIS_AVAILABLE:
                return None
            try:
                self._client = redis.Redis(
                    host=self.config.host,
                    port=self.config.port,
                    password=self.config.password,
                    db=self.db,
                    decode_responses=False
                )
                self._client.ping()
                logger.info(f"Connected to Redis db={self.db}")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")
                self._client = None
        return self._client

    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage"""
        try:
            return json.dumps(value).encode('utf-8')
        except (TypeError, ValueError):
            return pickle.dumps(value)

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize value from storage"""
        if data is None:
            return None
        try:
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return pickle.loads(data)

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set a value with optional TTL"""
        if self.client is None:
            return False
        try:
            data = self._serialize(value)
            if ttl:
                return self.client.setex(key, ttl, data)
            return self.client.set(key, data)
        except Exception as e:
            logger.error(f"Redis SET error: {e}")
            return False

    def get(self, key: str) -> Any:
        """Get a value"""
        if self.client is None:
            return None
        try:
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None

    def delete(self, key: str) -> bool:
        """Delete a key"""
        if self.client is None:
            return False
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            logger.error(f"Redis DELETE error: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        if self.client is None:
            return False
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Redis EXISTS error: {e}")
            return False

    def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on existing key"""
        if self.client is None:
            return False
        try:
            return bool(self.client.expire(key, ttl))
        except Exception as e:
            logger.error(f"Redis EXPIRE error: {e}")
            return False

    def incr(self, key: str) -> int:
        """Increment a counter"""
        if self.client is None:
            return 0
        try:
            return self.client.incr(key)
        except Exception as e:
            logger.error(f"Redis INCR error: {e}")
            return 0

    def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern"""
        if self.client is None:
            return []
        try:
            return [k.decode() for k in self.client.keys(pattern)]
        except Exception as e:
            logger.error(f"Redis KEYS error: {e}")
            return []


# =============================================================================
# ASYNC REDIS CLIENT
# =============================================================================

class AsyncRedisCache:
    """
    Asynchronous Redis cache client.

    Usage:
        cache = AsyncRedisCache()
        await cache.set("key", {"data": "value"})
        data = await cache.get("key")
    """

    def __init__(self, db: int = RedisConfig.DB_AI_CACHE):
        self.config = RedisConfig()
        self.db = db
        self._client = None

    async def connect(self):
        """Connect to Redis"""
        if not REDIS_AVAILABLE:
            return None
        if self._client is None:
            try:
                self._client = await aioredis.from_url(
                    f"redis://{self.config.host}:{self.config.port}/{self.db}",
                    password=self.config.password,
                    decode_responses=False
                )
                await self._client.ping()
                logger.info(f"Async Redis connected db={self.db}")
            except Exception as e:
                logger.warning(f"Async Redis connection failed: {e}")
                self._client = None
        return self._client

    async def close(self):
        """Close connection"""
        if self._client:
            await self._client.close()
            self._client = None

    def _serialize(self, value: Any) -> bytes:
        try:
            return json.dumps(value).encode('utf-8')
        except (TypeError, ValueError):
            return pickle.dumps(value)

    def _deserialize(self, data: bytes) -> Any:
        if data is None:
            return None
        try:
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return pickle.loads(data)

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set a value with optional TTL"""
        client = await self.connect()
        if client is None:
            return False
        try:
            data = self._serialize(value)
            if ttl:
                await client.setex(key, ttl, data)
            else:
                await client.set(key, data)
            return True
        except Exception as e:
            logger.error(f"Async Redis SET error: {e}")
            return False

    async def get(self, key: str) -> Any:
        """Get a value"""
        client = await self.connect()
        if client is None:
            return None
        try:
            data = await client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Async Redis GET error: {e}")
            return None

    async def delete(self, key: str) -> bool:
        """Delete a key"""
        client = await self.connect()
        if client is None:
            return False
        try:
            return bool(await client.delete(key))
        except Exception as e:
            logger.error(f"Async Redis DELETE error: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        client = await self.connect()
        if client is None:
            return False
        try:
            return bool(await client.exists(key))
        except Exception as e:
            logger.error(f"Async Redis EXISTS error: {e}")
            return False


# =============================================================================
# SPECIALIZED CACHES
# =============================================================================

class AIResponseCache(AsyncRedisCache):
    """
    Cache for AI responses to avoid regeneration.

    Usage:
        cache = AIResponseCache()

        # Check cache before AI call
        cached = await cache.get_response(query, context)
        if cached:
            return cached

        # Generate and cache
        response = await generate_ai_response(query)
        await cache.cache_response(query, context, response)
    """

    def __init__(self):
        super().__init__(db=RedisConfig.DB_AI_CACHE)
        self.default_ttl = RedisConfig.TTL_AI_RESPONSE

    def _hash_query(
        self,
        query: str,
        context: str = "",
        use_rag: bool = False,
        user_id: int = None,
        conversation_id: str = None
    ) -> str:
        """Create hash of query + context + RAG mode + user + conversation"""
        # Include RAG mode in cache key to avoid returning RAG responses for non-RAG queries
        combined = f"{query}:{context}:rag={use_rag}:user={user_id}:conv={conversation_id}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    async def cache_response(
        self,
        query: str,
        response: Dict[str, Any],
        context: str = "",
        use_rag: bool = False,
        user_id: int = None,
        conversation_id: str = None,
        ttl: int = None
    ) -> bool:
        """Cache an AI response"""
        key = f"ai_response:{self._hash_query(query, context, use_rag, user_id, conversation_id)}"
        return await self.set(key, response, ttl or self.default_ttl)

    async def get_response(
        self,
        query: str,
        context: str = "",
        use_rag: bool = False,
        user_id: int = None,
        conversation_id: str = None
    ) -> Optional[Dict[str, Any]]:
        """Get cached AI response"""
        key = f"ai_response:{self._hash_query(query, context, use_rag, user_id, conversation_id)}"
        return await self.get(key)

    async def invalidate(self, query: str, context: str = "") -> bool:
        """Invalidate cached response"""
        key = f"ai_response:{self._hash_query(query, context)}"
        return await self.delete(key)


class OCRCache(AsyncRedisCache):
    """
    Cache for OCR processing results.

    Usage:
        cache = OCRCache()

        # Check if already processed
        cached = await cache.get_ocr_result(file_hash)
        if cached:
            return cached

        # Process and cache
        result = await process_ocr(file)
        await cache.cache_ocr_result(file_hash, result)
    """

    def __init__(self):
        super().__init__(db=RedisConfig.DB_OCR_CACHE)
        self.default_ttl = RedisConfig.TTL_OCR_RESULT

    async def cache_ocr_result(
        self,
        file_hash: str,
        result: Dict[str, Any],
        ttl: int = None
    ) -> bool:
        """Cache OCR result"""
        key = f"ocr:{file_hash}"
        return await self.set(key, result, ttl or self.default_ttl)

    async def get_ocr_result(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached OCR result"""
        key = f"ocr:{file_hash}"
        return await self.get(key)


class SessionCache(AsyncRedisCache):
    """
    Session management cache.

    Usage:
        cache = SessionCache()
        await cache.set_session(session_id, user_data)
        user = await cache.get_session(session_id)
    """

    def __init__(self):
        super().__init__(db=RedisConfig.DB_SESSION)
        self.default_ttl = RedisConfig.TTL_SESSION

    async def set_session(
        self,
        session_id: str,
        data: Dict[str, Any],
        ttl: int = None
    ) -> bool:
        """Set session data"""
        key = f"session:{session_id}"
        return await self.set(key, data, ttl or self.default_ttl)

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data"""
        key = f"session:{session_id}"
        return await self.get(key)

    async def delete_session(self, session_id: str) -> bool:
        """Delete session"""
        key = f"session:{session_id}"
        return await self.delete(key)

    async def refresh_session(self, session_id: str, ttl: int = None) -> bool:
        """Refresh session TTL"""
        client = await self.connect()
        if client is None:
            return False
        key = f"session:{session_id}"
        try:
            return bool(await client.expire(key, ttl or self.default_ttl))
        except Exception:
            return False


class RateLimiter(AsyncRedisCache):
    """
    Rate limiting using Redis.

    Usage:
        limiter = RateLimiter()

        # Check rate limit (100 requests per minute)
        allowed = await limiter.check_rate_limit(user_id, limit=100, window=60)
        if not allowed:
            raise HTTPException(429, "Rate limit exceeded")
    """

    def __init__(self):
        super().__init__(db=RedisConfig.DB_RATE_LIMIT)

    async def check_rate_limit(
        self,
        identifier: str,
        limit: int = 100,
        window: int = 60
    ) -> bool:
        """
        Check if request is within rate limit.

        Args:
            identifier: User ID, IP, or other identifier
            limit: Maximum requests allowed
            window: Time window in seconds

        Returns:
            True if allowed, False if rate limited
        """
        client = await self.connect()
        if client is None:
            return True  # Allow if Redis unavailable

        key = f"rate_limit:{identifier}"

        try:
            current = await client.incr(key)

            if current == 1:
                await client.expire(key, window)

            return current <= limit
        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            return True

    async def get_remaining(self, identifier: str, limit: int = 100) -> int:
        """Get remaining requests"""
        client = await self.connect()
        if client is None:
            return limit

        key = f"rate_limit:{identifier}"
        try:
            current = await client.get(key)
            if current is None:
                return limit
            return max(0, limit - int(current))
        except Exception:
            return limit


class ChatStateCache(AsyncRedisCache):
    """
    Real-time chat state management.

    Usage:
        cache = ChatStateCache()
        await cache.set_typing(conversation_id, user_id, True)
        await cache.add_message(conversation_id, message)
    """

    def __init__(self):
        super().__init__(db=RedisConfig.DB_CHAT_STATE)
        self.default_ttl = RedisConfig.TTL_CHAT_STATE

    async def set_typing(
        self,
        conversation_id: str,
        user_id: str,
        is_typing: bool
    ) -> bool:
        """Set user typing status"""
        key = f"typing:{conversation_id}:{user_id}"
        if is_typing:
            return await self.set(key, True, ttl=10)
        else:
            return await self.delete(key)

    async def get_typing_users(self, conversation_id: str) -> List[str]:
        """Get users currently typing"""
        client = await self.connect()
        if client is None:
            return []

        pattern = f"typing:{conversation_id}:*"
        try:
            keys = await client.keys(pattern)
            return [k.decode().split(":")[-1] for k in keys]
        except Exception:
            return []

    async def add_message(
        self,
        conversation_id: str,
        message: Dict[str, Any]
    ) -> bool:
        """Add message to conversation cache"""
        key = f"messages:{conversation_id}"
        client = await self.connect()
        if client is None:
            return False

        try:
            data = json.dumps(message).encode()
            await client.rpush(key, data)
            await client.expire(key, self.default_ttl)
            # Keep only last 100 messages in cache
            await client.ltrim(key, -100, -1)
            return True
        except Exception as e:
            logger.error(f"Add message error: {e}")
            return False

    async def get_recent_messages(
        self,
        conversation_id: str,
        count: int = 20
    ) -> List[Dict[str, Any]]:
        """Get recent messages from cache"""
        key = f"messages:{conversation_id}"
        client = await self.connect()
        if client is None:
            return []

        try:
            messages = await client.lrange(key, -count, -1)
            return [json.loads(m.decode()) for m in messages]
        except Exception:
            return []


# =============================================================================
# CACHE DECORATOR
# =============================================================================

def cached(ttl: int = 3600, key_prefix: str = "cache"):
    """
    Decorator to cache async function results.

    Usage:
        @cached(ttl=3600, key_prefix="search")
        async def search_documents(query: str) -> List[Dict]:
            ...
    """
    def decorator(func):
        cache = AsyncRedisCache()

        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Build cache key from args
            key_parts = [key_prefix, func.__name__]
            key_parts.extend(str(a) for a in args)
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            cache_key = ":".join(key_parts)
            cache_key = hashlib.md5(cache_key.encode()).hexdigest()

            # Check cache
            cached_result = await cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit: {func.__name__}")
                return cached_result

            # Execute function
            result = await func(*args, **kwargs)

            # Cache result
            await cache.set(cache_key, result, ttl)
            logger.debug(f"Cache miss: {func.__name__}")

            return result

        return wrapper
    return decorator


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_ai_cache: Optional[AIResponseCache] = None
_ocr_cache: Optional[OCRCache] = None
_session_cache: Optional[SessionCache] = None
_rate_limiter: Optional[RateLimiter] = None
_chat_cache: Optional[ChatStateCache] = None


def get_ai_cache() -> AIResponseCache:
    """Get AI response cache singleton"""
    global _ai_cache
    if _ai_cache is None:
        _ai_cache = AIResponseCache()
    return _ai_cache


def get_ocr_cache() -> OCRCache:
    """Get OCR cache singleton"""
    global _ocr_cache
    if _ocr_cache is None:
        _ocr_cache = OCRCache()
    return _ocr_cache


def get_session_cache() -> SessionCache:
    """Get session cache singleton"""
    global _session_cache
    if _session_cache is None:
        _session_cache = SessionCache()
    return _session_cache


def get_rate_limiter() -> RateLimiter:
    """Get rate limiter singleton"""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter


def get_chat_cache() -> ChatStateCache:
    """Get chat state cache singleton"""
    global _chat_cache
    if _chat_cache is None:
        _chat_cache = ChatStateCache()
    return _chat_cache


def is_redis_available() -> bool:
    """Check if Redis is available"""
    return REDIS_AVAILABLE


_redis_cache: Optional[RedisCache] = None
_async_redis_cache: Optional[AsyncRedisCache] = None


def get_redis_cache(db: int = RedisConfig.DB_AI_CACHE) -> RedisCache:
    """Get sync Redis cache singleton"""
    global _redis_cache
    if _redis_cache is None:
        _redis_cache = RedisCache(db=db)
    return _redis_cache


def get_async_redis_cache(db: int = RedisConfig.DB_AI_CACHE) -> AsyncRedisCache:
    """Get async Redis cache singleton"""
    global _async_redis_cache
    if _async_redis_cache is None:
        _async_redis_cache = AsyncRedisCache(db=db)
    return _async_redis_cache

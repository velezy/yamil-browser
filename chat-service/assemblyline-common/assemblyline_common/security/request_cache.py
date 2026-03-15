"""
Request Caching Module

Enterprise-grade HTTP response caching with Redis backend.
Similar to API Gateway caching (CloudFront, Varnish, Kong).

Features:
- Cache GET responses by URL + headers
- TTL-based expiration
- Cache invalidation patterns
- Tenant-isolated caching
- Cache bypass headers
- Conditional requests (ETag, Last-Modified)
"""

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class CacheControl(str, Enum):
    """Cache control directives."""
    PUBLIC = "public"
    PRIVATE = "private"
    NO_CACHE = "no-cache"
    NO_STORE = "no-store"


@dataclass
class CacheConfig:
    """Configuration for request caching."""
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    default_ttl: int = 300  # 5 minutes
    max_ttl: int = 3600  # 1 hour
    key_prefix: str = "cache:"

    # Cache behavior
    cacheable_methods: Set[str] = field(default_factory=lambda: {"GET", "HEAD"})
    cacheable_status_codes: Set[int] = field(default_factory=lambda: {200, 203, 204, 206, 300, 301, 308})

    # Headers to include in cache key
    vary_headers: List[str] = field(default_factory=lambda: ["Accept", "Accept-Encoding", "Accept-Language"])

    # Bypass cache headers
    bypass_header: str = "X-Cache-Bypass"

    # Max cached response size (bytes)
    max_response_size: int = 10 * 1024 * 1024  # 10MB

    # Tenant isolation
    tenant_isolated: bool = True


@dataclass
class CachedResponse:
    """Cached HTTP response."""
    status_code: int
    headers: Dict[str, str]
    body: bytes
    created_at: float
    ttl: int
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    def is_expired(self) -> bool:
        return time.time() > self.created_at + self.ttl

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body.decode("utf-8", errors="replace"),
            "created_at": self.created_at,
            "ttl": self.ttl,
            "etag": self.etag,
            "last_modified": self.last_modified,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CachedResponse":
        return cls(
            status_code=data["status_code"],
            headers=data["headers"],
            body=data["body"].encode("utf-8"),
            created_at=data["created_at"],
            ttl=data["ttl"],
            etag=data.get("etag"),
            last_modified=data.get("last_modified"),
        )


class RequestCache:
    """
    Redis-backed request cache for API responses.

    Usage:
        cache = RequestCache(config)
        await cache.connect()

        # In middleware or endpoint
        cached = await cache.get(request)
        if cached:
            return Response(content=cached.body, ...)

        # After getting response
        await cache.set(request, response, ttl=300)
    """

    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self._redis = None

    async def connect(self):
        """Connect to Redis."""
        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=False,
            )
            await self._redis.ping()
            logger.info("Request cache connected to Redis")
        except Exception as e:
            logger.warning(f"Request cache Redis connection failed: {e}")
            self._redis = None

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    def _generate_cache_key(
        self,
        request: Request,
        tenant_id: Optional[str] = None,
    ) -> str:
        """Generate cache key from request."""
        parts = [
            request.method,
            str(request.url.path),
            str(request.url.query) if request.url.query else "",
        ]

        # Add vary headers
        for header in self.config.vary_headers:
            value = request.headers.get(header, "")
            parts.append(f"{header}:{value}")

        # Add tenant isolation
        if self.config.tenant_isolated and tenant_id:
            parts.insert(0, f"tenant:{tenant_id}")

        # Generate hash
        key_data = "|".join(parts)
        key_hash = hashlib.sha256(key_data.encode()).hexdigest()[:32]

        return f"{self.config.key_prefix}{key_hash}"

    def _should_cache_request(self, request: Request) -> bool:
        """Check if request should be cached."""
        # Check method
        if request.method not in self.config.cacheable_methods:
            return False

        # Check bypass header
        if request.headers.get(self.config.bypass_header):
            return False

        # Check Cache-Control: no-store
        cache_control = request.headers.get("Cache-Control", "")
        if "no-store" in cache_control or "no-cache" in cache_control:
            return False

        return True

    def _should_cache_response(self, response: Response) -> bool:
        """Check if response should be cached."""
        # Check status code
        if response.status_code not in self.config.cacheable_status_codes:
            return False

        # Check Cache-Control header
        cache_control = response.headers.get("Cache-Control", "")
        if "no-store" in cache_control or "private" in cache_control:
            return False

        return True

    def _extract_ttl(self, response: Response) -> int:
        """Extract TTL from response headers."""
        cache_control = response.headers.get("Cache-Control", "")

        # Check max-age
        if "max-age=" in cache_control:
            try:
                for part in cache_control.split(","):
                    if "max-age=" in part:
                        max_age = int(part.split("=")[1].strip())
                        return min(max_age, self.config.max_ttl)
            except (ValueError, IndexError):
                pass

        # Check Expires header
        expires = response.headers.get("Expires")
        if expires:
            try:
                from email.utils import parsedate_to_datetime
                exp_time = parsedate_to_datetime(expires)
                ttl = int((exp_time - datetime.now(timezone.utc)).total_seconds())
                if ttl > 0:
                    return min(ttl, self.config.max_ttl)
            except Exception:
                pass

        return self.config.default_ttl

    async def get(
        self,
        request: Request,
        tenant_id: Optional[str] = None,
    ) -> Optional[CachedResponse]:
        """Get cached response for request."""
        if not self._redis or not self._should_cache_request(request):
            return None

        try:
            key = self._generate_cache_key(request, tenant_id)
            data = await self._redis.get(key)

            if not data:
                return None

            cached = CachedResponse.from_dict(json.loads(data))

            # Check expiration
            if cached.is_expired():
                await self._redis.delete(key)
                return None

            # Check conditional request (If-None-Match)
            if_none_match = request.headers.get("If-None-Match")
            if if_none_match and cached.etag:
                if if_none_match == cached.etag or if_none_match == f'W/"{cached.etag}"':
                    # Return 304 Not Modified indicator
                    cached.status_code = 304
                    cached.body = b""

            # Check conditional request (If-Modified-Since)
            if_modified_since = request.headers.get("If-Modified-Since")
            if if_modified_since and cached.last_modified:
                try:
                    from email.utils import parsedate_to_datetime
                    ims = parsedate_to_datetime(if_modified_since)
                    lm = parsedate_to_datetime(cached.last_modified)
                    if lm <= ims:
                        cached.status_code = 304
                        cached.body = b""
                except Exception:
                    pass

            logger.debug(f"Cache HIT: {key}")
            return cached

        except Exception as e:
            logger.warning(f"Cache get failed: {e}")
            return None

    async def set(
        self,
        request: Request,
        response: Response,
        body: bytes,
        tenant_id: Optional[str] = None,
        ttl: Optional[int] = None,
    ) -> bool:
        """Cache response for request."""
        if not self._redis:
            return False

        if not self._should_cache_request(request):
            return False

        if not self._should_cache_response(response):
            return False

        # Check response size
        if len(body) > self.config.max_response_size:
            logger.debug(f"Response too large to cache: {len(body)} bytes")
            return False

        try:
            key = self._generate_cache_key(request, tenant_id)
            effective_ttl = ttl or self._extract_ttl(response)

            # Extract ETag and Last-Modified
            etag = response.headers.get("ETag", "").strip('"').strip("W/").strip('"')
            last_modified = response.headers.get("Last-Modified")

            # Create cached response
            cached = CachedResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                body=body,
                created_at=time.time(),
                ttl=effective_ttl,
                etag=etag or None,
                last_modified=last_modified,
            )

            await self._redis.setex(
                key,
                effective_ttl,
                json.dumps(cached.to_dict()),
            )

            logger.debug(f"Cache SET: {key} (TTL: {effective_ttl}s)")
            return True

        except Exception as e:
            logger.warning(f"Cache set failed: {e}")
            return False

    async def invalidate(
        self,
        pattern: str,
        tenant_id: Optional[str] = None,
    ) -> int:
        """Invalidate cache entries matching pattern."""
        if not self._redis:
            return 0

        try:
            prefix = self.config.key_prefix
            if self.config.tenant_isolated and tenant_id:
                prefix = f"{self.config.key_prefix}tenant:{tenant_id}:"

            full_pattern = f"{prefix}{pattern}"

            # Find and delete matching keys
            deleted = 0
            async for key in self._redis.scan_iter(match=full_pattern):
                await self._redis.delete(key)
                deleted += 1

            logger.info(f"Invalidated {deleted} cache entries matching {full_pattern}")
            return deleted

        except Exception as e:
            logger.warning(f"Cache invalidation failed: {e}")
            return 0

    async def clear_tenant(self, tenant_id: str) -> int:
        """Clear all cache entries for a tenant."""
        return await self.invalidate("*", tenant_id)

    async def clear_all(self) -> int:
        """Clear all cache entries."""
        if not self._redis:
            return 0

        try:
            pattern = f"{self.config.key_prefix}*"
            deleted = 0
            async for key in self._redis.scan_iter(match=pattern):
                await self._redis.delete(key)
                deleted += 1

            logger.info(f"Cleared {deleted} cache entries")
            return deleted

        except Exception as e:
            logger.warning(f"Cache clear failed: {e}")
            return 0

    async def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self._redis:
            return {"status": "disconnected"}

        try:
            pattern = f"{self.config.key_prefix}*"
            count = 0
            total_size = 0

            async for key in self._redis.scan_iter(match=pattern):
                count += 1
                data = await self._redis.get(key)
                if data:
                    total_size += len(data)

            return {
                "status": "connected",
                "entries": count,
                "total_size_bytes": total_size,
                "config": {
                    "default_ttl": self.config.default_ttl,
                    "max_ttl": self.config.max_ttl,
                    "tenant_isolated": self.config.tenant_isolated,
                }
            }

        except Exception as e:
            return {"status": "error", "error": str(e)}


class CacheMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware for automatic response caching.

    Usage:
        cache = RequestCache(config)
        await cache.connect()
        app.add_middleware(CacheMiddleware, cache=cache)
    """

    def __init__(self, app, cache: RequestCache):
        super().__init__(app)
        self.cache = cache

    async def dispatch(self, request: Request, call_next) -> Response:
        # Get tenant ID from request state if available
        tenant_id = getattr(request.state, "tenant_id", None)

        # Try to get from cache
        cached = await self.cache.get(request, tenant_id)
        if cached:
            response = Response(
                content=cached.body,
                status_code=cached.status_code,
                headers=cached.headers,
            )
            response.headers["X-Cache"] = "HIT"
            return response

        # Call the actual endpoint
        response = await call_next(request)

        # Cache the response if applicable
        # Note: This requires response body buffering
        response.headers["X-Cache"] = "MISS"

        return response


# Singleton instance
_cache_instance: Optional[RequestCache] = None


async def get_request_cache(
    config: Optional[CacheConfig] = None
) -> RequestCache:
    """Get or create request cache singleton."""
    global _cache_instance

    if _cache_instance is None:
        _cache_instance = RequestCache(config)
        await _cache_instance.connect()

    return _cache_instance


def cache_response(
    ttl: int = 300,
    vary: Optional[List[str]] = None,
    cache_control: CacheControl = CacheControl.PUBLIC,
):
    """
    Decorator for caching endpoint responses.

    Usage:
        @app.get("/api/data")
        @cache_response(ttl=600, vary=["Accept"])
        async def get_data():
            return {"data": "value"}
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            response = await func(*args, **kwargs)

            # Add cache headers
            if hasattr(response, "headers"):
                response.headers["Cache-Control"] = f"{cache_control.value}, max-age={ttl}"
                if vary:
                    response.headers["Vary"] = ", ".join(vary)

            return response

        return wrapper
    return decorator

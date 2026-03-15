"""
PostgreSQL-based Distributed Rate Limiter

Implements sliding window rate limiting using PostgreSQL, eliminating
the need for Redis while maintaining accuracy and performance.

This is an alternative to the Redis-based rate_limiter.py for environments
where Redis is not available.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from asyncpg import Pool, Connection

logger = logging.getLogger(__name__)


@dataclass
class PostgresRateLimitResult:
    """Result of a rate limit check"""
    allowed: bool
    current_count: int
    remaining: int
    reset_at: datetime
    limit: int

    def to_headers(self) -> dict[str, str]:
        """Generate standard rate limit headers"""
        return {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(int(self.reset_at.timestamp())),
        }


class PostgresRateLimiter:
    """
    PostgreSQL-based rate limiter using sliding window algorithm.

    Features:
    - No Redis dependency - uses existing PostgreSQL
    - Atomic operations via PostgreSQL functions
    - Per-key configurable limits
    - Automatic cleanup of old entries
    - Burst protection

    Usage:
        rate_limiter = PostgresRateLimiter(db_pool)
        result = await rate_limiter.check_limit(api_key_id)
        if not result.allowed:
            raise HTTPException(429, headers=result.to_headers())
    """

    def __init__(
        self,
        db_pool: Pool,
        default_limit_per_minute: int = 1000,
        default_limit_per_hour: int = 50000,
        cleanup_interval_seconds: int = 60,
    ):
        self.pool = db_pool
        self.default_limit_per_minute = default_limit_per_minute
        self.default_limit_per_hour = default_limit_per_hour
        self.cleanup_interval = cleanup_interval_seconds
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start_cleanup_task(self) -> None:
        """Start background task to clean up old rate limit entries"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("PostgreSQL rate limiter cleanup task started")

    async def stop_cleanup_task(self) -> None:
        """Stop the cleanup background task"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            logger.info("PostgreSQL rate limiter cleanup task stopped")

    async def _cleanup_loop(self) -> None:
        """Background loop to periodically clean up old entries"""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                deleted = await self.cleanup_old_entries()
                if deleted > 0:
                    logger.debug(f"Cleaned up {deleted} old rate limit entries")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Rate limit cleanup error: {e}")

    async def check_limit(
        self,
        api_key_id: str,
        endpoint: str = "*",
        limit_override: Optional[int] = None,
    ) -> PostgresRateLimitResult:
        """
        Check if request is within rate limit.

        Args:
            api_key_id: UUID of the API key
            endpoint: Optional endpoint for per-endpoint limiting
            limit_override: Override the default/configured limit

        Returns:
            PostgresRateLimitResult with allowed status and metadata
        """
        limit = limit_override or self.default_limit_per_minute

        try:
            async with self.pool.acquire() as conn:
                # Use PostgreSQL function for atomic check
                row = await conn.fetchrow(
                    "SELECT * FROM common.check_rate_limit($1, $2, $3)",
                    api_key_id,
                    endpoint,
                    limit,
                )

                if row:
                    return PostgresRateLimitResult(
                        allowed=row["allowed"],
                        current_count=row["current_count"],
                        remaining=row["remaining"],
                        reset_at=row["reset_at"],
                        limit=limit,
                    )
        except Exception as e:
            logger.warning(f"Rate limit check failed, allowing request: {e}")

        # Fallback if function fails or doesn't exist
        return PostgresRateLimitResult(
            allowed=True,
            current_count=0,
            remaining=limit,
            reset_at=datetime.utcnow() + timedelta(minutes=1),
            limit=limit,
        )

    async def check_limit_simple(
        self,
        api_key_id: str,
        limit: int = 1000,
        window_seconds: int = 60,
    ) -> PostgresRateLimitResult:
        """
        Simple rate limit check without using PostgreSQL function.
        Useful if migration hasn't been run yet.
        """
        window_start = datetime.utcnow() - timedelta(seconds=window_seconds)

        try:
            async with self.pool.acquire() as conn:
                # Count requests in window
                count = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM common.rate_limit_log
                    WHERE api_key_id = $1 AND timestamp > $2
                    """,
                    api_key_id,
                    window_start,
                )

                if count is None:
                    count = 0

                allowed = count < limit

                if allowed:
                    # Log this request
                    await conn.execute(
                        """
                        INSERT INTO common.rate_limit_log (api_key_id, endpoint, timestamp)
                        VALUES ($1, '*', NOW())
                        """,
                        api_key_id,
                    )

                return PostgresRateLimitResult(
                    allowed=allowed,
                    current_count=count + (1 if allowed else 0),
                    remaining=max(0, limit - count - 1) if allowed else 0,
                    reset_at=datetime.utcnow() + timedelta(seconds=window_seconds),
                    limit=limit,
                )
        except Exception as e:
            logger.warning(f"Simple rate limit check failed: {e}")
            return PostgresRateLimitResult(
                allowed=True,
                current_count=0,
                remaining=limit,
                reset_at=datetime.utcnow() + timedelta(seconds=window_seconds),
                limit=limit,
            )

    async def get_key_config(self, api_key_id: str) -> Optional[dict]:
        """Get rate limit configuration for a specific API key"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT requests_per_minute, requests_per_hour, requests_per_day,
                           burst_limit, enabled
                    FROM common.rate_limit_config
                    WHERE api_key_id = $1
                    """,
                    api_key_id,
                )
                return dict(row) if row else None
        except Exception:
            return None

    async def set_key_config(
        self,
        api_key_id: str,
        requests_per_minute: int = 1000,
        requests_per_hour: int = 50000,
        requests_per_day: int = 500000,
        burst_limit: int = 100,
    ) -> None:
        """Set custom rate limit configuration for an API key"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO common.rate_limit_config
                    (api_key_id, requests_per_minute, requests_per_hour,
                     requests_per_day, burst_limit, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (api_key_id) DO UPDATE SET
                    requests_per_minute = EXCLUDED.requests_per_minute,
                    requests_per_hour = EXCLUDED.requests_per_hour,
                    requests_per_day = EXCLUDED.requests_per_day,
                    burst_limit = EXCLUDED.burst_limit,
                    updated_at = NOW()
                """,
                api_key_id,
                requests_per_minute,
                requests_per_hour,
                requests_per_day,
                burst_limit,
            )

    async def cleanup_old_entries(self, retention_minutes: int = 5) -> int:
        """Remove rate limit entries older than retention period"""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(
                    "SELECT common.cleanup_rate_limit_log($1)",
                    retention_minutes,
                )
                return result or 0
        except Exception:
            return 0

    async def get_usage_stats(self, api_key_id: str) -> dict:
        """Get current usage statistics for an API key"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 minute') as last_minute,
                        COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') as last_hour,
                        COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 day') as last_day,
                        MAX(timestamp) as last_request
                    FROM common.rate_limit_log
                    WHERE api_key_id = $1
                    """,
                    api_key_id,
                )

                config = await self.get_key_config(api_key_id)
                limits = config or {
                    "requests_per_minute": self.default_limit_per_minute,
                    "requests_per_hour": self.default_limit_per_hour,
                }

                return {
                    "usage": {
                        "last_minute": row["last_minute"] or 0 if row else 0,
                        "last_hour": row["last_hour"] or 0 if row else 0,
                        "last_day": row["last_day"] or 0 if row else 0,
                        "last_request": row["last_request"] if row else None,
                    },
                    "limits": limits,
                }
        except Exception as e:
            logger.error(f"Failed to get usage stats: {e}")
            return {"usage": {}, "limits": {}}


# Convenience function for middleware
async def check_postgres_rate_limit(
    conn: Connection,
    api_key_id: str,
    limit: int = 1000,
) -> tuple[bool, dict]:
    """
    Simple rate limit check for use in middleware.

    Returns:
        (allowed, headers_dict)
    """
    window_start = datetime.utcnow() - timedelta(minutes=1)

    try:
        count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM common.rate_limit_log
            WHERE api_key_id = $1 AND timestamp > $2
            """,
            api_key_id,
            window_start,
        )

        if count is None:
            count = 0

        allowed = count < limit

        if allowed:
            await conn.execute(
                "INSERT INTO common.rate_limit_log (api_key_id) VALUES ($1)",
                api_key_id,
            )

        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(max(0, limit - count - 1)),
            "X-RateLimit-Reset": str(int((datetime.utcnow() + timedelta(minutes=1)).timestamp())),
        }

        return allowed, headers
    except Exception as e:
        logger.warning(f"Rate limit check failed: {e}")
        return True, {}

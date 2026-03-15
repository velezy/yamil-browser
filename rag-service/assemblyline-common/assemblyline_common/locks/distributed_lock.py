"""
Redis-backed Distributed Lock implementation.

Features:
- Atomic lock acquisition with Lua scripts
- Automatic expiration (prevents deadlocks)
- Lock extension for long operations
- Fencing tokens for correctness
- Async context manager support

Usage:
    from assemblyline_common.locks import acquire_lock, DistributedLockConfig

    # Simple usage
    async with acquire_lock("resource:123", owner="user-abc") as lock:
        # Do exclusive work
        if lock.acquired:
            await process_resource()

    # With custom config
    config = DistributedLockConfig(
        ttl_seconds=60,
        retry_count=5,
        retry_delay_ms=200
    )
    async with acquire_lock("flow:uuid", owner="session-id", config=config) as lock:
        if lock.acquired:
            await edit_flow()
"""

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

import redis.asyncio as redis

from assemblyline_common.config import settings

logger = logging.getLogger(__name__)


class LockAcquisitionError(Exception):
    """Raised when lock acquisition fails."""
    pass


class LockReleaseError(Exception):
    """Raised when lock release fails."""
    pass


@dataclass
class DistributedLockConfig:
    """Configuration for distributed locks."""
    # Lock TTL in seconds (auto-expires to prevent deadlocks)
    ttl_seconds: int = 300  # 5 minutes default

    # Retry settings for lock acquisition
    retry_count: int = 3
    retry_delay_ms: int = 100

    # Key prefix for Redis
    key_prefix: str = "lock"

    # Redis connection (uses settings if not provided)
    redis_url: Optional[str] = None

    # Whether to block waiting for lock or return immediately
    blocking: bool = True

    # Maximum time to wait for lock acquisition (seconds)
    acquire_timeout: float = 30.0


@dataclass
class LockInfo:
    """Information about an acquired lock."""
    key: str
    owner: str
    acquired: bool
    fencing_token: int = 0
    expires_at: float = 0.0
    acquired_at: float = 0.0

    @property
    def remaining_ttl(self) -> float:
        """Remaining TTL in seconds."""
        if not self.acquired:
            return 0.0
        return max(0.0, self.expires_at - time.time())


class DistributedLock:
    """
    Redis-backed distributed lock with fencing tokens.

    Uses Redis SET NX with expiration for safe distributed locking.
    Fencing tokens ensure operations are correctly ordered even in
    cases of lock expiration during processing.
    """

    # Lua script for atomic lock acquisition
    ACQUIRE_SCRIPT = """
    local key = KEYS[1]
    local owner = ARGV[1]
    local ttl_ms = tonumber(ARGV[2])
    local fencing_key = KEYS[2]

    -- Try to acquire lock
    local acquired = redis.call('SET', key, owner, 'NX', 'PX', ttl_ms)

    if acquired then
        -- Increment fencing token
        local token = redis.call('INCR', fencing_key)
        redis.call('EXPIRE', fencing_key, 86400)  -- 24 hour TTL for fencing tokens
        return {1, token}
    else
        -- Check if we already own the lock
        local current_owner = redis.call('GET', key)
        if current_owner == owner then
            -- Refresh TTL
            redis.call('PEXPIRE', key, ttl_ms)
            local token = redis.call('GET', fencing_key) or 0
            return {2, tonumber(token)}  -- Already owned
        end
        return {0, 0}  -- Not acquired
    end
    """

    # Lua script for atomic lock release
    RELEASE_SCRIPT = """
    local key = KEYS[1]
    local owner = ARGV[1]

    local current_owner = redis.call('GET', key)
    if current_owner == owner then
        redis.call('DEL', key)
        return 1
    end
    return 0
    """

    # Lua script for lock extension
    EXTEND_SCRIPT = """
    local key = KEYS[1]
    local owner = ARGV[1]
    local ttl_ms = tonumber(ARGV[2])

    local current_owner = redis.call('GET', key)
    if current_owner == owner then
        redis.call('PEXPIRE', key, ttl_ms)
        return 1
    end
    return 0
    """

    def __init__(self, config: Optional[DistributedLockConfig] = None):
        self.config = config or DistributedLockConfig()
        self._redis: Optional[redis.Redis] = None
        self._acquire_sha: Optional[str] = None
        self._release_sha: Optional[str] = None
        self._extend_sha: Optional[str] = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize Redis connection and load Lua scripts."""
        if self._initialized:
            return

        redis_url = self.config.redis_url or settings.REDIS_URL

        try:
            self._redis = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=settings.REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=settings.REDIS_SOCKET_CONNECT_TIMEOUT,
            )
            await self._redis.ping()

            # Load Lua scripts
            self._acquire_sha = await self._redis.script_load(self.ACQUIRE_SCRIPT)
            self._release_sha = await self._redis.script_load(self.RELEASE_SCRIPT)
            self._extend_sha = await self._redis.script_load(self.EXTEND_SCRIPT)

            self._initialized = True
            logger.info("Distributed lock manager initialized")
        except redis.RedisError as e:
            logger.error(f"Failed to initialize distributed lock manager: {e}")
            raise

    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None
            self._initialized = False

    def _make_key(self, resource: str) -> str:
        """Create Redis key for lock."""
        return f"{self.config.key_prefix}:{resource}"

    def _make_fencing_key(self, resource: str) -> str:
        """Create Redis key for fencing token."""
        return f"{self.config.key_prefix}:fence:{resource}"

    async def acquire(
        self,
        resource: str,
        owner: str,
        ttl_seconds: Optional[int] = None
    ) -> LockInfo:
        """
        Attempt to acquire a distributed lock.

        Args:
            resource: The resource identifier to lock
            owner: Unique identifier for the lock owner (e.g., session ID)
            ttl_seconds: Lock TTL, defaults to config value

        Returns:
            LockInfo with acquisition status and fencing token
        """
        await self.initialize()

        ttl = ttl_seconds or self.config.ttl_seconds
        ttl_ms = ttl * 1000
        key = self._make_key(resource)
        fencing_key = self._make_fencing_key(resource)

        for attempt in range(self.config.retry_count):
            try:
                result = await self._redis.evalsha(
                    self._acquire_sha,
                    2,  # Number of keys
                    key, fencing_key,
                    owner, ttl_ms
                )

                status, token = result
                now = time.time()

                if status == 1:
                    # Newly acquired
                    logger.info(
                        f"Lock acquired",
                        extra={
                            "event_type": "lock_acquired",
                            "resource": resource,
                            "owner": owner,
                            "fencing_token": token,
                            "ttl_seconds": ttl,
                        }
                    )
                    return LockInfo(
                        key=key,
                        owner=owner,
                        acquired=True,
                        fencing_token=token,
                        expires_at=now + ttl,
                        acquired_at=now,
                    )
                elif status == 2:
                    # Already owned, refreshed
                    logger.debug(f"Lock refreshed for {resource} by {owner}")
                    return LockInfo(
                        key=key,
                        owner=owner,
                        acquired=True,
                        fencing_token=token,
                        expires_at=now + ttl,
                        acquired_at=now,
                    )
                else:
                    # Not acquired
                    if not self.config.blocking:
                        return LockInfo(
                            key=key,
                            owner=owner,
                            acquired=False,
                        )

                    # Wait before retry
                    if attempt < self.config.retry_count - 1:
                        delay = self.config.retry_delay_ms / 1000.0
                        await asyncio.sleep(delay * (attempt + 1))

            except redis.RedisError as e:
                logger.warning(f"Redis error during lock acquisition: {e}")
                if attempt < self.config.retry_count - 1:
                    await asyncio.sleep(0.1)

        # All retries exhausted
        logger.warning(
            f"Failed to acquire lock",
            extra={
                "event_type": "lock_acquisition_failed",
                "resource": resource,
                "owner": owner,
            }
        )
        return LockInfo(key=key, owner=owner, acquired=False)

    async def release(self, resource: str, owner: str) -> bool:
        """
        Release a distributed lock.

        Args:
            resource: The resource identifier
            owner: The lock owner (must match acquisition owner)

        Returns:
            True if lock was released, False if not owned
        """
        await self.initialize()

        key = self._make_key(resource)

        try:
            result = await self._redis.evalsha(
                self._release_sha,
                1,
                key,
                owner
            )

            released = bool(result)

            if released:
                logger.info(
                    f"Lock released",
                    extra={
                        "event_type": "lock_released",
                        "resource": resource,
                        "owner": owner,
                    }
                )
            else:
                logger.warning(f"Lock release failed - not owned by {owner}")

            return released

        except redis.RedisError as e:
            logger.error(f"Redis error during lock release: {e}")
            raise LockReleaseError(f"Failed to release lock: {e}")

    async def extend(
        self,
        resource: str,
        owner: str,
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """
        Extend the TTL of an owned lock.

        Args:
            resource: The resource identifier
            owner: The lock owner
            ttl_seconds: New TTL, defaults to config value

        Returns:
            True if lock was extended, False if not owned
        """
        await self.initialize()

        ttl = ttl_seconds or self.config.ttl_seconds
        ttl_ms = ttl * 1000
        key = self._make_key(resource)

        try:
            result = await self._redis.evalsha(
                self._extend_sha,
                1,
                key,
                owner, ttl_ms
            )

            extended = bool(result)

            if extended:
                logger.debug(f"Lock extended for {resource} by {owner}")
            else:
                logger.warning(f"Lock extend failed - not owned by {owner}")

            return extended

        except redis.RedisError as e:
            logger.error(f"Redis error during lock extend: {e}")
            return False

    async def get_lock_info(self, resource: str) -> Optional[Dict[str, Any]]:
        """Get information about a lock."""
        await self.initialize()

        key = self._make_key(resource)
        fencing_key = self._make_fencing_key(resource)

        try:
            pipe = self._redis.pipeline()
            pipe.get(key)
            pipe.pttl(key)
            pipe.get(fencing_key)
            results = await pipe.execute()

            owner, ttl_ms, fencing_token = results

            if owner is None:
                return None

            return {
                "resource": resource,
                "owner": owner,
                "remaining_ttl_ms": max(0, ttl_ms),
                "fencing_token": int(fencing_token) if fencing_token else 0,
            }

        except redis.RedisError as e:
            logger.error(f"Redis error getting lock info: {e}")
            return None

    async def force_release(self, resource: str) -> bool:
        """
        Force release a lock regardless of owner.

        Use with caution - this bypasses ownership checks.
        Intended for admin operations only.
        """
        await self.initialize()

        key = self._make_key(resource)

        try:
            result = await self._redis.delete(key)
            logger.warning(
                f"Lock force released",
                extra={
                    "event_type": "lock_force_released",
                    "resource": resource,
                }
            )
            return bool(result)
        except redis.RedisError as e:
            logger.error(f"Redis error during force release: {e}")
            return False


# Singleton lock manager
_lock_manager: Optional[DistributedLock] = None
_lock_manager_lock = asyncio.Lock()


async def get_lock_manager(
    config: Optional[DistributedLockConfig] = None
) -> DistributedLock:
    """Get or create the singleton lock manager."""
    global _lock_manager

    async with _lock_manager_lock:
        if _lock_manager is None:
            _lock_manager = DistributedLock(config)
            await _lock_manager.initialize()
        return _lock_manager


@asynccontextmanager
async def acquire_lock(
    resource: str,
    owner: Optional[str] = None,
    config: Optional[DistributedLockConfig] = None,
):
    """
    Async context manager for distributed lock acquisition.

    Args:
        resource: Resource identifier to lock
        owner: Lock owner ID (auto-generated if not provided)
        config: Lock configuration

    Yields:
        LockInfo with acquisition status

    Example:
        async with acquire_lock("flow:123", owner="user-abc") as lock:
            if lock.acquired:
                await do_exclusive_work()
    """
    if owner is None:
        owner = str(uuid.uuid4())

    manager = await get_lock_manager(config)
    lock_info = await manager.acquire(resource, owner)

    try:
        yield lock_info
    finally:
        if lock_info.acquired:
            await manager.release(resource, owner)


async def release_lock(resource: str, owner: str) -> bool:
    """Release a lock directly (without context manager)."""
    manager = await get_lock_manager()
    return await manager.release(resource, owner)

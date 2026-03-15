"""
Generic Connection Pool implementation.

Enterprise Features:
- Health checks with configurable intervals
- Automatic reconnection on failure
- Pool size limits per tenant
- Connection lifecycle management
- Metrics and monitoring

Usage:
    from assemblyline_common.pool import ConnectionPool, ConnectionPoolConfig

    # Define a connection factory
    class MyConnectionFactory:
        async def create(self) -> MyConnection:
            return await MyConnection.connect(host, port)

        async def validate(self, conn: MyConnection) -> bool:
            return conn.is_connected()

        async def close(self, conn: MyConnection) -> None:
            await conn.close()

    # Create and use pool
    pool = ConnectionPool(
        factory=MyConnectionFactory(),
        config=ConnectionPoolConfig(max_size=10)
    )

    async with pool.acquire() as conn:
        await conn.send(data)
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Generic, TypeVar, Optional, Dict, List, Set, Any
from uuid import UUID

logger = logging.getLogger(__name__)

T = TypeVar('T')  # Connection type


class PoolExhaustedError(Exception):
    """Raised when pool is exhausted and no connections available."""
    pass


class ConnectionHealthError(Exception):
    """Raised when connection fails health check."""
    pass


@dataclass
class ConnectionPoolConfig:
    """Configuration for connection pool.

    Sized for 200K+ API calls/day across all services.
    Each worker process gets its own pool instance.
    """
    # Maximum connections in the pool (per worker process)
    max_size: int = 25
    # Minimum connections to maintain (pre-warmed for burst traffic)
    min_size: int = 5
    # Maximum time to wait for a connection (seconds)
    acquire_timeout: float = 30.0
    # Maximum time a connection can be idle before removal (seconds)
    max_idle_time: float = 300.0
    # Maximum lifetime of a connection (seconds), 0 = unlimited
    max_lifetime: float = 3600.0
    # Health check interval (seconds)
    health_check_interval: float = 30.0
    # Enable background health checks
    enable_health_checks: bool = True
    # Number of retries for creating new connections
    connection_retries: int = 3
    # Delay between connection retries (seconds)
    retry_delay: float = 1.0


@dataclass
class PooledConnection(Generic[T]):
    """Wrapper around a pooled connection with metadata."""
    connection: T
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    last_health_check: float = field(default_factory=time.time)
    use_count: int = 0
    is_healthy: bool = True

    def mark_used(self) -> None:
        """Mark connection as used."""
        self.last_used_at = time.time()
        self.use_count += 1

    def is_expired(self, max_lifetime: float) -> bool:
        """Check if connection has exceeded its lifetime."""
        if max_lifetime <= 0:
            return False
        return (time.time() - self.created_at) > max_lifetime

    def is_idle_too_long(self, max_idle_time: float) -> bool:
        """Check if connection has been idle too long."""
        return (time.time() - self.last_used_at) > max_idle_time


class ConnectionFactory(ABC, Generic[T]):
    """Abstract factory for creating and managing connections."""

    @abstractmethod
    async def create(self) -> T:
        """Create a new connection."""
        pass

    @abstractmethod
    async def validate(self, connection: T) -> bool:
        """Validate a connection is healthy."""
        pass

    @abstractmethod
    async def close(self, connection: T) -> None:
        """Close a connection."""
        pass


class ConnectionPool(Generic[T]):
    """
    Generic async connection pool with health checks.

    Features:
    - Async context manager for safe connection acquisition
    - Background health check task
    - Automatic cleanup of stale connections
    - Connection reuse and lifecycle management
    """

    def __init__(
        self,
        factory: ConnectionFactory[T],
        config: Optional[ConnectionPoolConfig] = None,
        name: str = "default",
    ):
        self.factory = factory
        self.config = config or ConnectionPoolConfig()
        self.name = name

        self._available: asyncio.Queue[PooledConnection[T]] = asyncio.Queue()
        self._in_use: Set[PooledConnection[T]] = set()
        self._all_connections: List[PooledConnection[T]] = []
        self._lock = asyncio.Lock()
        self._closed = False
        self._health_check_task: Optional[asyncio.Task] = None
        self._initialized = False

    @property
    def size(self) -> int:
        """Current total pool size."""
        return len(self._all_connections)

    @property
    def available_count(self) -> int:
        """Number of available connections."""
        return self._available.qsize()

    @property
    def in_use_count(self) -> int:
        """Number of connections currently in use."""
        return len(self._in_use)

    async def initialize(self) -> None:
        """Initialize the pool with minimum connections."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            # Pre-warm the pool with min_size connections
            for _ in range(self.config.min_size):
                try:
                    conn = await self._create_connection()
                    if conn:
                        await self._available.put(conn)
                except Exception as e:
                    logger.warning(f"Failed to pre-warm connection: {e}")

            # Start health check task
            if self.config.enable_health_checks:
                self._health_check_task = asyncio.create_task(
                    self._health_check_loop()
                )

            self._initialized = True
            logger.info(
                f"Connection pool initialized",
                extra={
                    "event_type": "pool_initialized",
                    "pool_name": self.name,
                    "initial_size": self.size,
                }
            )

    async def _create_connection(self) -> Optional[PooledConnection[T]]:
        """Create a new pooled connection with retry logic."""
        last_error = None

        for attempt in range(self.config.connection_retries):
            try:
                conn = await self.factory.create()
                pooled = PooledConnection(connection=conn)
                self._all_connections.append(pooled)

                logger.debug(
                    f"Created new connection",
                    extra={
                        "event_type": "connection_created",
                        "pool_name": self.name,
                        "pool_size": self.size,
                    }
                )
                return pooled

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Connection creation failed (attempt {attempt + 1}): {e}",
                    extra={
                        "event_type": "connection_create_failed",
                        "pool_name": self.name,
                        "attempt": attempt + 1,
                    }
                )
                if attempt < self.config.connection_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))

        logger.error(f"Failed to create connection after {self.config.connection_retries} attempts")
        return None

    async def _destroy_connection(self, pooled: PooledConnection[T]) -> None:
        """Destroy a pooled connection."""
        try:
            await self.factory.close(pooled.connection)
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

        if pooled in self._all_connections:
            self._all_connections.remove(pooled)

        logger.debug(
            f"Destroyed connection",
            extra={
                "event_type": "connection_destroyed",
                "pool_name": self.name,
                "pool_size": self.size,
                "use_count": pooled.use_count,
            }
        )

    async def _validate_connection(self, pooled: PooledConnection[T]) -> bool:
        """Validate a connection is healthy."""
        try:
            is_valid = await self.factory.validate(pooled.connection)
            pooled.is_healthy = is_valid
            pooled.last_health_check = time.time()
            return is_valid
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            pooled.is_healthy = False
            return False

    async def acquire(self, timeout: Optional[float] = None) -> PooledConnection[T]:
        """
        Acquire a connection from the pool.

        Args:
            timeout: Maximum time to wait, defaults to config.acquire_timeout

        Returns:
            A pooled connection

        Raises:
            PoolExhaustedError: If no connection available within timeout
        """
        if not self._initialized:
            await self.initialize()

        if self._closed:
            raise PoolExhaustedError("Pool is closed")

        timeout = timeout or self.config.acquire_timeout
        deadline = time.time() + timeout

        while time.time() < deadline:
            # Try to get an available connection
            try:
                pooled = self._available.get_nowait()

                # Check if connection is still valid
                if pooled.is_expired(self.config.max_lifetime):
                    await self._destroy_connection(pooled)
                    continue

                if pooled.is_idle_too_long(self.config.max_idle_time):
                    # Validate before reuse
                    if not await self._validate_connection(pooled):
                        await self._destroy_connection(pooled)
                        continue

                pooled.mark_used()
                self._in_use.add(pooled)
                return pooled

            except asyncio.QueueEmpty:
                pass

            # Try to create a new connection if under limit
            async with self._lock:
                if self.size < self.config.max_size:
                    pooled = await self._create_connection()
                    if pooled:
                        pooled.mark_used()
                        self._in_use.add(pooled)
                        return pooled

            # Wait for a connection to become available
            remaining = deadline - time.time()
            if remaining <= 0:
                break

            try:
                pooled = await asyncio.wait_for(
                    self._available.get(),
                    timeout=min(remaining, 1.0)
                )

                if pooled.is_expired(self.config.max_lifetime):
                    await self._destroy_connection(pooled)
                    continue

                pooled.mark_used()
                self._in_use.add(pooled)
                return pooled

            except asyncio.TimeoutError:
                continue

        raise PoolExhaustedError(
            f"Could not acquire connection from pool '{self.name}' within {timeout}s. "
            f"Pool size: {self.size}/{self.config.max_size}, in use: {self.in_use_count}"
        )

    async def release(self, pooled: PooledConnection[T], force_close: bool = False) -> None:
        """
        Release a connection back to the pool.

        Args:
            pooled: The pooled connection to release
            force_close: If True, destroy the connection instead of returning to pool
        """
        if pooled in self._in_use:
            self._in_use.remove(pooled)

        if force_close or not pooled.is_healthy:
            await self._destroy_connection(pooled)
            return

        if pooled.is_expired(self.config.max_lifetime):
            await self._destroy_connection(pooled)
            return

        # Return to available pool
        await self._available.put(pooled)

    @asynccontextmanager
    async def connection(self, timeout: Optional[float] = None):
        """
        Context manager for acquiring a connection.

        Usage:
            async with pool.connection() as conn:
                await conn.send(data)
        """
        pooled = await self.acquire(timeout)
        try:
            yield pooled.connection
            await self.release(pooled)
        except Exception as e:
            # Mark connection as unhealthy on error
            pooled.is_healthy = False
            await self.release(pooled, force_close=True)
            raise

    async def _health_check_loop(self) -> None:
        """Background task for periodic health checks."""
        while not self._closed:
            try:
                await asyncio.sleep(self.config.health_check_interval)

                if self._closed:
                    break

                # Check available connections
                connections_to_check: List[PooledConnection[T]] = []
                while True:
                    try:
                        conn = self._available.get_nowait()
                        connections_to_check.append(conn)
                    except asyncio.QueueEmpty:
                        break

                for pooled in connections_to_check:
                    if self._closed:
                        break

                    # Remove expired connections
                    if pooled.is_expired(self.config.max_lifetime):
                        await self._destroy_connection(pooled)
                        continue

                    # Remove stale connections beyond min_size
                    if (
                        pooled.is_idle_too_long(self.config.max_idle_time)
                        and self.size > self.config.min_size
                    ):
                        await self._destroy_connection(pooled)
                        continue

                    # Validate connection health
                    if await self._validate_connection(pooled):
                        await self._available.put(pooled)
                    else:
                        await self._destroy_connection(pooled)

                # Ensure minimum pool size
                async with self._lock:
                    while self.size < self.config.min_size and not self._closed:
                        pooled = await self._create_connection()
                        if pooled:
                            await self._available.put(pooled)
                        else:
                            break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(5)

    async def close(self) -> None:
        """Close the pool and all connections."""
        self._closed = True

        # Cancel health check task
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Close all connections
        for pooled in list(self._all_connections):
            await self._destroy_connection(pooled)

        self._in_use.clear()

        logger.info(
            f"Connection pool closed",
            extra={
                "event_type": "pool_closed",
                "pool_name": self.name,
            }
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            "name": self.name,
            "size": self.size,
            "max_size": self.config.max_size,
            "min_size": self.config.min_size,
            "available": self.available_count,
            "in_use": self.in_use_count,
            "health_checks_enabled": self.config.enable_health_checks,
        }


class TenantConnectionPoolManager(Generic[T]):
    """
    Manages connection pools per tenant with limits.

    Ensures each tenant has their own pool and enforces
    tenant-specific size limits.
    """

    def __init__(
        self,
        factory_creator: ConnectionFactory[T],
        default_config: Optional[ConnectionPoolConfig] = None,
        max_pools: int = 100,
    ):
        self.factory_creator = factory_creator
        self.default_config = default_config or ConnectionPoolConfig()
        self.max_pools = max_pools
        self._pools: Dict[str, ConnectionPool[T]] = {}
        self._lock = asyncio.Lock()

    async def get_pool(
        self,
        tenant_id: str,
        config: Optional[ConnectionPoolConfig] = None
    ) -> ConnectionPool[T]:
        """Get or create a pool for a tenant."""
        if tenant_id in self._pools:
            return self._pools[tenant_id]

        async with self._lock:
            if tenant_id in self._pools:
                return self._pools[tenant_id]

            if len(self._pools) >= self.max_pools:
                # Evict least recently used pool
                await self._evict_oldest_pool()

            pool = ConnectionPool(
                factory=self.factory_creator,
                config=config or self.default_config,
                name=f"tenant-{tenant_id}",
            )
            await pool.initialize()
            self._pools[tenant_id] = pool

            logger.info(
                f"Created pool for tenant",
                extra={
                    "event_type": "tenant_pool_created",
                    "tenant_id": tenant_id,
                    "total_pools": len(self._pools),
                }
            )

            return pool

    async def _evict_oldest_pool(self) -> None:
        """Evict the pool with least recent activity."""
        if not self._pools:
            return

        # Find pool with oldest last_used connection
        oldest_tenant = None
        oldest_time = float('inf')

        for tenant_id, pool in self._pools.items():
            if pool.in_use_count > 0:
                continue  # Don't evict pools with active connections

            for conn in pool._all_connections:
                if conn.last_used_at < oldest_time:
                    oldest_time = conn.last_used_at
                    oldest_tenant = tenant_id

        if oldest_tenant:
            pool = self._pools.pop(oldest_tenant)
            await pool.close()
            logger.info(
                f"Evicted pool for tenant",
                extra={
                    "event_type": "tenant_pool_evicted",
                    "tenant_id": oldest_tenant,
                }
            )

    async def close_all(self) -> None:
        """Close all tenant pools."""
        for tenant_id, pool in list(self._pools.items()):
            await pool.close()
        self._pools.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all pools."""
        return {
            "total_pools": len(self._pools),
            "max_pools": self.max_pools,
            "pools": {
                tenant_id: pool.get_stats()
                for tenant_id, pool in self._pools.items()
            }
        }


# Singleton manager for application-wide pool management
_pool_managers: Dict[str, TenantConnectionPoolManager] = {}
_pool_manager_lock = asyncio.Lock()


async def get_connection_pool_manager(
    name: str,
    factory: ConnectionFactory,
    config: Optional[ConnectionPoolConfig] = None,
) -> TenantConnectionPoolManager:
    """
    Get or create a tenant connection pool manager.

    Args:
        name: Unique name for this pool manager (e.g., "http", "mllp", "database")
        factory: Connection factory for creating connections
        config: Default pool configuration

    Returns:
        TenantConnectionPoolManager instance
    """
    if name in _pool_managers:
        return _pool_managers[name]

    async with _pool_manager_lock:
        if name in _pool_managers:
            return _pool_managers[name]

        manager = TenantConnectionPoolManager(
            factory_creator=factory,
            default_config=config,
        )
        _pool_managers[name] = manager

        return manager


async def close_all_pool_managers() -> None:
    """Close all pool managers."""
    for manager in _pool_managers.values():
        await manager.close_all()
    _pool_managers.clear()

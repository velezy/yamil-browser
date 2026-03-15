"""
Enterprise Database Connectors.

Supports:
- PostgreSQL with asyncpg
- Redis with cluster and sentinel support
- Connection pooling with health checks
- Read replica routing
- Query timeout enforcement
- TLS encryption
- Tenant isolation

Usage:
    from assemblyline_common.connectors import (
        get_postgres_connector,
        get_redis_connector,
        PostgresConfig,
        RedisConfig,
    )

    # PostgreSQL
    pg = await get_postgres_connector(PostgresConfig(
        host="localhost",
        database="mydb",
    ))
    async with pg.connection() as conn:
        result = await conn.fetch("SELECT * FROM users")

    # Redis
    redis = await get_redis_connector(RedisConfig(
        host="localhost",
        cluster_mode=False,
    ))
    await redis.set("key", "value")
"""

import asyncio
import logging
import ssl
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, List, Union, AsyncIterator

import asyncpg
import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
)
from assemblyline_common.retry import RetryHandler, DATABASE_RETRY_CONFIG

logger = logging.getLogger(__name__)


class ReplicaMode(Enum):
    """Read replica routing modes."""
    PRIMARY_ONLY = "primary"
    REPLICA_PREFERRED = "replica_preferred"
    REPLICA_ONLY = "replica"


@dataclass
class PostgresConfig:
    """Configuration for PostgreSQL connector."""
    # Connection settings
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""

    # Read replicas
    replica_hosts: List[str] = field(default_factory=list)
    replica_mode: ReplicaMode = ReplicaMode.PRIMARY_ONLY
    max_replica_lag_seconds: int = 10

    # Pool settings
    min_pool_size: int = 5
    max_pool_size: int = 20
    max_idle_time: float = 300.0
    max_lifetime: float = 3600.0

    # Query settings
    statement_timeout: int = 30000  # ms
    command_timeout: float = 30.0  # seconds

    # TLS
    ssl_mode: str = "prefer"  # disable, prefer, require, verify-ca, verify-full
    ssl_ca_file: Optional[str] = None
    ssl_cert_file: Optional[str] = None
    ssl_key_file: Optional[str] = None

    # Circuit breaker
    enable_circuit_breaker: bool = True

    # Tenant ID for namespacing
    tenant_id: Optional[str] = None
    schema_name: Optional[str] = None


@dataclass
class RedisConfig:
    """Configuration for Redis connector."""
    # Connection settings
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    username: Optional[str] = None

    # Cluster mode
    cluster_mode: bool = False
    cluster_nodes: List[Dict[str, Any]] = field(default_factory=list)

    # Sentinel mode
    sentinel_mode: bool = False
    sentinel_hosts: List[tuple] = field(default_factory=list)
    sentinel_master: str = "mymaster"

    # Pool settings
    max_connections: int = 50
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0

    # TLS
    ssl_enabled: bool = False
    ssl_ca_file: Optional[str] = None
    ssl_cert_file: Optional[str] = None
    ssl_key_file: Optional[str] = None

    # Circuit breaker
    enable_circuit_breaker: bool = True

    # Key namespacing
    key_prefix: str = ""
    tenant_id: Optional[str] = None


class PostgresConnector:
    """
    Enterprise PostgreSQL connector.

    Features:
    - Connection pooling with asyncpg
    - Read replica routing
    - Query timeout enforcement
    - TLS support
    - Circuit breaker
    - Tenant schema isolation
    """

    def __init__(
        self,
        config: PostgresConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._primary_pool: Optional[asyncpg.Pool] = None
        self._replica_pools: List[asyncpg.Pool] = []
        self._retry_handler: Optional[RetryHandler] = None
        self._replica_index = 0
        self._metrics: Dict[str, int] = {
            "queries": 0,
            "primary_queries": 0,
            "replica_queries": 0,
            "errors": 0,
        }
        self._closed = False

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context if needed."""
        if self.config.ssl_mode == "disable":
            return None

        if self.config.ssl_mode in ("require", "verify-ca", "verify-full"):
            ctx = ssl.create_default_context()

            if self.config.ssl_ca_file:
                ctx.load_verify_locations(self.config.ssl_ca_file)

            if self.config.ssl_cert_file:
                ctx.load_cert_chain(
                    self.config.ssl_cert_file,
                    self.config.ssl_key_file,
                )

            if self.config.ssl_mode == "verify-full":
                ctx.check_hostname = True
            else:
                ctx.check_hostname = False

            return ctx

        return True  # Let asyncpg handle SSL negotiation

    async def initialize(self) -> None:
        """Initialize connection pools."""
        ssl_context = self._get_ssl_context()

        # Create primary pool
        self._primary_pool = await asyncpg.create_pool(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            min_size=self.config.min_pool_size,
            max_size=self.config.max_pool_size,
            max_inactive_connection_lifetime=self.config.max_idle_time,
            command_timeout=self.config.command_timeout,
            ssl=ssl_context,
        )

        # Set statement timeout on pool init
        async def init_connection(conn):
            await conn.execute(
                f"SET statement_timeout = {self.config.statement_timeout}"
            )
            if self.config.schema_name:
                await conn.execute(
                    f"SET search_path TO {self.config.schema_name}, public"
                )

        # Note: asyncpg doesn't have init callback, we'll set per-connection

        # Create replica pools
        for replica_host in self.config.replica_hosts:
            replica_pool = await asyncpg.create_pool(
                host=replica_host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=self.config.min_pool_size // 2 or 1,
                max_size=self.config.max_pool_size // 2 or 5,
                max_inactive_connection_lifetime=self.config.max_idle_time,
                command_timeout=self.config.command_timeout,
                ssl=ssl_context,
            )
            self._replica_pools.append(replica_pool)

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        self._retry_handler = RetryHandler(config=DATABASE_RETRY_CONFIG)

        logger.info(
            "PostgreSQL connector initialized",
            extra={
                "event_type": "postgres_initialized",
                "host": self.config.host,
                "database": self.config.database,
                "replicas": len(self.config.replica_hosts),
            }
        )

    def _get_replica_pool(self) -> Optional[asyncpg.Pool]:
        """Get next replica pool using round-robin."""
        if not self._replica_pools:
            return None

        pool = self._replica_pools[self._replica_index]
        self._replica_index = (self._replica_index + 1) % len(self._replica_pools)
        return pool

    def _should_use_replica(self, read_only: bool) -> bool:
        """Determine if query should go to replica."""
        if not read_only:
            return False

        if self.config.replica_mode == ReplicaMode.PRIMARY_ONLY:
            return False

        if not self._replica_pools:
            return False

        return True

    @asynccontextmanager
    async def connection(
        self,
        read_only: bool = False,
    ) -> AsyncIterator[asyncpg.Connection]:
        """
        Get a connection from the pool.

        Args:
            read_only: If True, may route to read replica
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._primary_pool:
            await self.initialize()

        # Determine which pool to use
        use_replica = self._should_use_replica(read_only)
        pool = self._get_replica_pool() if use_replica else self._primary_pool

        if pool is None:
            pool = self._primary_pool

        circuit_name = f"postgres:{self.config.host}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        try:
            async with pool.acquire() as conn:
                # Set session parameters
                await conn.execute(
                    f"SET statement_timeout = {self.config.statement_timeout}"
                )
                if self.config.schema_name:
                    await conn.execute(
                        f"SET search_path TO {self.config.schema_name}, public"
                    )

                yield conn

                if self._circuit_breaker:
                    await self._circuit_breaker.record_success(circuit_name)

                # Update metrics
                self._metrics["queries"] += 1
                if use_replica:
                    self._metrics["replica_queries"] += 1
                else:
                    self._metrics["primary_queries"] += 1

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    async def execute(
        self,
        query: str,
        *args,
        read_only: bool = False,
    ) -> str:
        """Execute a query and return status."""
        async with self.connection(read_only=read_only) as conn:
            return await conn.execute(query, *args)

    async def fetch(
        self,
        query: str,
        *args,
        read_only: bool = True,
    ) -> List[asyncpg.Record]:
        """Execute a query and fetch all results."""
        async with self.connection(read_only=read_only) as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(
        self,
        query: str,
        *args,
        read_only: bool = True,
    ) -> Optional[asyncpg.Record]:
        """Execute a query and fetch one row."""
        async with self.connection(read_only=read_only) as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(
        self,
        query: str,
        *args,
        read_only: bool = True,
    ) -> Any:
        """Execute a query and fetch a single value."""
        async with self.connection(read_only=read_only) as conn:
            return await conn.fetchval(query, *args)

    @asynccontextmanager
    async def transaction(self):
        """Start a transaction (always on primary)."""
        async with self.connection(read_only=False) as conn:
            async with conn.transaction():
                yield conn

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return {
            **self._metrics,
            "pool_size": self._primary_pool.get_size() if self._primary_pool else 0,
            "pool_free": self._primary_pool.get_idle_size() if self._primary_pool else 0,
        }

    async def close(self) -> None:
        """Close all connection pools."""
        self._closed = True

        if self._primary_pool:
            await self._primary_pool.close()
            self._primary_pool = None

        for pool in self._replica_pools:
            await pool.close()
        self._replica_pools.clear()

        logger.info("PostgreSQL connector closed")


class RedisConnector:
    """
    Enterprise Redis connector.

    Features:
    - Standalone, Cluster, and Sentinel modes
    - Connection pooling
    - TLS support
    - Key namespacing per tenant
    - Pipeline optimization
    - Circuit breaker
    """

    def __init__(
        self,
        config: RedisConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._client: Optional[aioredis.Redis] = None
        self._sentinel: Optional[Sentinel] = None
        self._metrics: Dict[str, int] = {
            "commands": 0,
            "errors": 0,
        }
        self._closed = False

    def _get_key(self, key: str) -> str:
        """Add namespace prefix to key."""
        prefix = self.config.key_prefix
        if self.config.tenant_id:
            prefix = f"{prefix}:{self.config.tenant_id}" if prefix else self.config.tenant_id
        return f"{prefix}:{key}" if prefix else key

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context if needed."""
        if not self.config.ssl_enabled:
            return None

        ctx = ssl.create_default_context()

        if self.config.ssl_ca_file:
            ctx.load_verify_locations(self.config.ssl_ca_file)

        if self.config.ssl_cert_file:
            ctx.load_cert_chain(
                self.config.ssl_cert_file,
                self.config.ssl_key_file,
            )

        return ctx

    async def initialize(self) -> None:
        """Initialize Redis client."""
        ssl_context = self._get_ssl_context()

        if self.config.sentinel_mode:
            # Sentinel mode
            self._sentinel = Sentinel(
                self.config.sentinel_hosts,
                socket_timeout=self.config.socket_timeout,
                password=self.config.password,
                ssl=ssl_context,
            )
            self._client = self._sentinel.master_for(
                self.config.sentinel_master,
                socket_timeout=self.config.socket_timeout,
                db=self.config.db,
            )

        elif self.config.cluster_mode:
            # Cluster mode
            from redis.asyncio.cluster import RedisCluster

            startup_nodes = self.config.cluster_nodes or [
                {"host": self.config.host, "port": self.config.port}
            ]

            self._client = RedisCluster(
                startup_nodes=startup_nodes,
                password=self.config.password,
                ssl=ssl_context,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
            )

        else:
            # Standalone mode
            self._client = aioredis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                username=self.config.username,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                max_connections=self.config.max_connections,
                ssl=ssl_context,
            )

        # Test connection
        await self._client.ping()

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        logger.info(
            "Redis connector initialized",
            extra={
                "event_type": "redis_initialized",
                "host": self.config.host,
                "cluster_mode": self.config.cluster_mode,
                "sentinel_mode": self.config.sentinel_mode,
            }
        )

    async def _execute(self, method: str, *args, **kwargs) -> Any:
        """Execute a Redis command with circuit breaker."""
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._client:
            await self.initialize()

        circuit_name = f"redis:{self.config.host}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        try:
            func = getattr(self._client, method)
            result = await func(*args, **kwargs)

            self._metrics["commands"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            return result

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    # String operations
    async def get(self, key: str) -> Optional[bytes]:
        """Get a value."""
        return await self._execute("get", self._get_key(key))

    async def set(
        self,
        key: str,
        value: Union[str, bytes],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set a value."""
        return await self._execute(
            "set",
            self._get_key(key),
            value,
            ex=ex,
            px=px,
            nx=nx,
            xx=xx,
        )

    async def delete(self, *keys: str) -> int:
        """Delete keys."""
        prefixed_keys = [self._get_key(k) for k in keys]
        return await self._execute("delete", *prefixed_keys)

    async def exists(self, *keys: str) -> int:
        """Check if keys exist."""
        prefixed_keys = [self._get_key(k) for k in keys]
        return await self._execute("exists", *prefixed_keys)

    async def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration."""
        return await self._execute("expire", self._get_key(key), seconds)

    async def ttl(self, key: str) -> int:
        """Get key TTL."""
        return await self._execute("ttl", self._get_key(key))

    # Hash operations
    async def hget(self, key: str, field: str) -> Optional[bytes]:
        """Get hash field."""
        return await self._execute("hget", self._get_key(key), field)

    async def hset(
        self,
        key: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        mapping: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Set hash field(s)."""
        if mapping:
            return await self._execute("hset", self._get_key(key), mapping=mapping)
        return await self._execute("hset", self._get_key(key), field, value)

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        """Get all hash fields."""
        return await self._execute("hgetall", self._get_key(key))

    async def hdel(self, key: str, *fields: str) -> int:
        """Delete hash fields."""
        return await self._execute("hdel", self._get_key(key), *fields)

    # List operations
    async def lpush(self, key: str, *values: Any) -> int:
        """Push to list head."""
        return await self._execute("lpush", self._get_key(key), *values)

    async def rpush(self, key: str, *values: Any) -> int:
        """Push to list tail."""
        return await self._execute("rpush", self._get_key(key), *values)

    async def lpop(self, key: str, count: Optional[int] = None) -> Any:
        """Pop from list head."""
        return await self._execute("lpop", self._get_key(key), count)

    async def rpop(self, key: str, count: Optional[int] = None) -> Any:
        """Pop from list tail."""
        return await self._execute("rpop", self._get_key(key), count)

    async def lrange(self, key: str, start: int, end: int) -> List[bytes]:
        """Get list range."""
        return await self._execute("lrange", self._get_key(key), start, end)

    async def llen(self, key: str) -> int:
        """Get list length."""
        return await self._execute("llen", self._get_key(key))

    # Set operations
    async def sadd(self, key: str, *members: Any) -> int:
        """Add to set."""
        return await self._execute("sadd", self._get_key(key), *members)

    async def srem(self, key: str, *members: Any) -> int:
        """Remove from set."""
        return await self._execute("srem", self._get_key(key), *members)

    async def smembers(self, key: str) -> set:
        """Get all set members."""
        return await self._execute("smembers", self._get_key(key))

    async def sismember(self, key: str, member: Any) -> bool:
        """Check set membership."""
        return await self._execute("sismember", self._get_key(key), member)

    # Sorted set operations
    async def zadd(
        self,
        key: str,
        mapping: Dict[str, float],
        nx: bool = False,
        xx: bool = False,
    ) -> int:
        """Add to sorted set."""
        return await self._execute(
            "zadd",
            self._get_key(key),
            mapping,
            nx=nx,
            xx=xx,
        )

    async def zrange(
        self,
        key: str,
        start: int,
        end: int,
        withscores: bool = False,
    ) -> List:
        """Get sorted set range."""
        return await self._execute(
            "zrange",
            self._get_key(key),
            start,
            end,
            withscores=withscores,
        )

    async def zrem(self, key: str, *members: Any) -> int:
        """Remove from sorted set."""
        return await self._execute("zrem", self._get_key(key), *members)

    # Pipeline for batch operations
    def pipeline(self):
        """Get a pipeline for batch operations."""
        return self._client.pipeline()

    # Pub/Sub
    async def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel."""
        return await self._execute("publish", self._get_key(channel), message)

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return {
            **self._metrics,
            "mode": "cluster" if self.config.cluster_mode else (
                "sentinel" if self.config.sentinel_mode else "standalone"
            ),
        }

    async def close(self) -> None:
        """Close Redis connection."""
        self._closed = True

        if self._client:
            await self._client.close()
            self._client = None

        logger.info("Redis connector closed")


# Singleton instances
_postgres_connectors: Dict[str, PostgresConnector] = {}
_redis_connectors: Dict[str, RedisConnector] = {}
_db_lock = asyncio.Lock()


async def get_postgres_connector(
    config: Optional[PostgresConfig] = None,
    name: Optional[str] = None,
) -> PostgresConnector:
    """Get or create a PostgreSQL connector."""
    config = config or PostgresConfig()
    connector_name = name or f"pg-{config.host}-{config.database}"

    if connector_name in _postgres_connectors:
        return _postgres_connectors[connector_name]

    async with _db_lock:
        if connector_name in _postgres_connectors:
            return _postgres_connectors[connector_name]

        connector = PostgresConnector(config)
        await connector.initialize()
        _postgres_connectors[connector_name] = connector

        return connector


async def get_redis_connector(
    config: Optional[RedisConfig] = None,
    name: Optional[str] = None,
) -> RedisConnector:
    """Get or create a Redis connector."""
    config = config or RedisConfig()
    connector_name = name or f"redis-{config.host}-{config.db}"

    if connector_name in _redis_connectors:
        return _redis_connectors[connector_name]

    async with _db_lock:
        if connector_name in _redis_connectors:
            return _redis_connectors[connector_name]

        connector = RedisConnector(config)
        await connector.initialize()
        _redis_connectors[connector_name] = connector

        return connector


async def close_all_database_connectors() -> None:
    """Close all database connectors."""
    for connector in _postgres_connectors.values():
        await connector.close()
    _postgres_connectors.clear()

    for connector in _redis_connectors.values():
        await connector.close()
    _redis_connectors.clear()

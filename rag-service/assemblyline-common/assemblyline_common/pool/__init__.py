"""
Connection Pool module for Logic Weaver.

Provides unified connection pooling with health checks, automatic reconnection,
and tenant-based pool size limits.
"""

from assemblyline_common.pool.connection_pool import (
    ConnectionPool,
    ConnectionPoolConfig,
    PooledConnection,
    ConnectionFactory,
    get_connection_pool_manager,
    PoolExhaustedError,
    ConnectionHealthError,
)

__all__ = [
    "ConnectionPool",
    "ConnectionPoolConfig",
    "PooledConnection",
    "ConnectionFactory",
    "get_connection_pool_manager",
    "PoolExhaustedError",
    "ConnectionHealthError",
]

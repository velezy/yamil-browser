"""
PostgreSQL Connection Pool Manager
Async connection handling with pgvector support

Connection pool limits are automatically configured based on LICENSE_TIER:
- free/consumer: 1-5 connections
- consumer_plus: 2-10 connections
- pro: 2-15 connections
- enterprise_s: 5-50 connections
- enterprise_m: 10-100 connections
- enterprise_l: 15-150 connections
- enterprise_unlimited: 20-200 connections
- developer: 2-20 connections

Override with DB_MIN_CONN and DB_MAX_CONN environment variables if needed.
"""

import os
import logging
from typing import Optional, AsyncIterator
from dataclasses import dataclass
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


def _get_connection_limits_for_license() -> tuple:
    """Get connection pool limits based on LICENSE_TIER environment variable

    Formula: min = max(2, users * 0.1), max = max(5, users * 0.5)
    Assumes ~50% peak concurrent users, each needing 1 connection
    """
    license_tier = os.getenv("LICENSE_TIER", "developer").lower()

    # License tier to connection limits mapping
    # Format: (min_connections, max_connections)
    tier_limits = {
        "free": (2, 5),              # 1 user
        "consumer": (2, 5),          # 1 user
        "consumer_plus": (2, 5),     # 5 users
        "pro": (2, 5),               # 10 users
        "enterprise_s": (10, 50),    # 100 users
        "enterprise_m": (50, 250),   # 500 users
        "enterprise_l": (100, 500),  # 1000 users
        "enterprise_unlimited": (100, 1000),  # unlimited
        "developer": (2, 20),        # dev/testing
        # Legacy aliases
        "enterprise": (10, 50),      # Maps to enterprise_s
    }

    return tier_limits.get(license_tier, (2, 10))


@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = "localhost"
    port: int = 5432  # Default PostgreSQL port
    database: str = "drivesentinel"
    user: str = "postgres"
    password: str = "postgres"
    min_connections: int = 2
    max_connections: int = 10

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create config from environment variables

        Connection limits are determined by:
        1. Explicit DB_MIN_CONN/DB_MAX_CONN env vars (highest priority)
        2. LICENSE_TIER env var (automatic scaling)
        3. Default values (fallback)
        """
        database_url = os.getenv("DATABASE_URL")

        # Get license-based limits as defaults
        license_min, license_max = _get_connection_limits_for_license()

        if database_url:
            # Parse DATABASE_URL format: postgresql://user:pass@host:port/db
            from urllib.parse import urlparse
            parsed = urlparse(database_url)
            return cls(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                database=parsed.path.lstrip("/") or "drivesentinel",
                user=parsed.username or "postgres",
                password=parsed.password or "postgres",
                min_connections=int(os.getenv("DB_MIN_CONN", str(license_min))),
                max_connections=int(os.getenv("DB_MAX_CONN", str(license_max))),
            )

        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "drivesentinel"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            min_connections=int(os.getenv("DB_MIN_CONN", str(license_min))),
            max_connections=int(os.getenv("DB_MAX_CONN", str(license_max))),
        )

    @property
    def dsn(self) -> str:
        """Get connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


# Global connection pool
_pool = None
_config: Optional[DatabaseConfig] = None


async def get_db_pool(config: Optional[DatabaseConfig] = None):
    """Get or create the database connection pool"""
    global _pool, _config

    if _pool is not None:
        return _pool

    try:
        import asyncpg
    except ImportError:
        raise ImportError("asyncpg is required. Install with: pip install asyncpg")

    _config = config or DatabaseConfig.from_env()

    try:
        _pool = await asyncpg.create_pool(
            host=_config.host,
            port=_config.port,
            database=_config.database,
            user=_config.user,
            password=_config.password,
            min_size=_config.min_connections,
            max_size=_config.max_connections,
            command_timeout=60,
        )

        # Initialize pgvector extension
        async with _pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            logger.info("pgvector extension enabled")

        logger.info(f"✅ Database pool created: {_config.host}:{_config.port}/{_config.database}")
        return _pool

    except Exception as e:
        logger.error(f"❌ Failed to create database pool: {e}")
        raise


async def close_db_pool():
    """Close the database connection pool"""
    global _pool

    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


@asynccontextmanager
async def get_connection():
    """Get a connection from the pool"""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        yield conn


async def execute(query: str, *args):
    """Execute a query"""
    async with get_connection() as conn:
        return await conn.execute(query, *args)


async def fetch(query: str, *args):
    """Fetch all rows"""
    async with get_connection() as conn:
        return await conn.fetch(query, *args)


async def fetchrow(query: str, *args):
    """Fetch a single row"""
    async with get_connection() as conn:
        return await conn.fetchrow(query, *args)


async def fetchval(query: str, *args):
    """Fetch a single value"""
    async with get_connection() as conn:
        return await conn.fetchval(query, *args)

"""
T.A.L.O.S. PostgreSQL Connection Pooling
pgbouncer + asyncpg - <1ms vs 5-10ms connection overhead

Features:
- Connection pooling at application level
- pgbouncer configuration for external pooling
- Handles 1000s of concurrent clients
- Optimized for asyncpg
"""

import os
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# =============================================================================
# ASYNCPG AVAILABILITY CHECK
# =============================================================================

ASYNCPG_AVAILABLE = False

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
    logger.info("asyncpg library loaded successfully")
except ImportError:
    logger.warning("asyncpg not installed. Run: pip install asyncpg")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration"""
    # Direct connection (or pgbouncer)
    host: str = os.getenv("POSTGRES_HOST", os.getenv("PGBOUNCER_HOST", "localhost"))
    port: int = int(os.getenv("POSTGRES_PORT", os.getenv("PGBOUNCER_PORT", "5432")))
    database: str = os.getenv("POSTGRES_DB", "talos")
    user: str = os.getenv("POSTGRES_USER", "talos")
    password: str = os.getenv("POSTGRES_PASSWORD", "talos")

    # Connection pool settings
    min_pool_size: int = int(os.getenv("PG_MIN_POOL_SIZE", "5"))
    max_pool_size: int = int(os.getenv("PG_MAX_POOL_SIZE", "20"))

    # Connection settings
    command_timeout: float = float(os.getenv("PG_COMMAND_TIMEOUT", "60.0"))
    statement_cache_size: int = int(os.getenv("PG_STATEMENT_CACHE", "1024"))

    # SSL settings
    ssl: Optional[str] = os.getenv("POSTGRES_SSL")  # require, prefer, disable

    @property
    def dsn(self) -> str:
        """Get connection DSN"""
        ssl_param = f"?sslmode={self.ssl}" if self.ssl else ""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}{ssl_param}"


# =============================================================================
# CONNECTION POOL
# =============================================================================

class AsyncPGPool:
    """
    High-performance async PostgreSQL connection pool.

    When using with pgbouncer:
    - Connect to pgbouncer port (6432) instead of PostgreSQL (5432)
    - pgbouncer handles connection reuse at proxy level
    - Additional ~1ms saved per connection

    Performance:
    - Without pooling: 5-10ms per connection
    - With asyncpg pool: ~1ms per connection
    - With pgbouncer: <0.5ms per connection
    """

    def __init__(self, config: Optional[PostgresConfig] = None):
        self.config = config or PostgresConfig()
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> bool:
        """Create connection pool"""
        if not ASYNCPG_AVAILABLE:
            logger.error("asyncpg not available")
            return False

        try:
            self._pool = await asyncpg.create_pool(
                dsn=self.config.dsn,
                min_size=self.config.min_pool_size,
                max_size=self.config.max_pool_size,
                command_timeout=self.config.command_timeout,
                statement_cache_size=self.config.statement_cache_size,
            )
            logger.info(
                f"PostgreSQL pool created: {self.config.host}:{self.config.port}/{self.config.database} "
                f"(min={self.config.min_pool_size}, max={self.config.max_pool_size})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to create PostgreSQL pool: {e}")
            return False

    async def close(self):
        """Close connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL pool closed")

    @asynccontextmanager
    async def acquire(self):
        """Acquire connection from pool"""
        if not self._pool:
            await self.connect()

        if not self._pool:
            raise RuntimeError("PostgreSQL pool not available")

        async with self._pool.acquire() as conn:
            yield conn

    async def execute(self, query: str, *args, timeout: float = None) -> str:
        """Execute a query"""
        async with self.acquire() as conn:
            return await conn.execute(query, *args, timeout=timeout)

    async def fetch(self, query: str, *args, timeout: float = None) -> List[asyncpg.Record]:
        """Fetch multiple rows"""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args, timeout=timeout)

    async def fetchrow(self, query: str, *args, timeout: float = None) -> Optional[asyncpg.Record]:
        """Fetch single row"""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)

    async def fetchval(self, query: str, *args, column: int = 0, timeout: float = None) -> Any:
        """Fetch single value"""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args, column=column, timeout=timeout)

    async def executemany(self, query: str, args: List, timeout: float = None) -> None:
        """Execute query for each args tuple"""
        async with self.acquire() as conn:
            await conn.executemany(query, args, timeout=timeout)

    @asynccontextmanager
    async def transaction(self):
        """Context manager for transactions"""
        async with self.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def copy_to_table(
        self,
        table_name: str,
        records: List[tuple],
        columns: List[str] = None,
        timeout: float = None
    ) -> str:
        """Bulk insert using COPY"""
        async with self.acquire() as conn:
            return await conn.copy_records_to_table(
                table_name,
                records=records,
                columns=columns,
                timeout=timeout
            )

    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        if not self._pool:
            return {"status": "not_connected"}

        return {
            "status": "connected",
            "size": self._pool.get_size(),
            "min_size": self._pool.get_min_size(),
            "max_size": self._pool.get_max_size(),
            "free_size": self._pool.get_idle_size(),
        }


# =============================================================================
# VECTOR OPERATIONS (pgvector)
# =============================================================================

class VectorPool(AsyncPGPool):
    """
    Extended pool with pgvector operations.

    Optimized for:
    - Vector similarity search
    - Embedding storage
    - Hybrid search (vector + keyword)
    """

    async def create_vector_extension(self):
        """Ensure pgvector extension is installed"""
        await self.execute("CREATE EXTENSION IF NOT EXISTS vector")

    async def store_embedding(
        self,
        table: str,
        id_column: str,
        id_value: Any,
        embedding_column: str,
        embedding: List[float],
        extra_columns: Dict[str, Any] = None
    ) -> None:
        """Store an embedding vector"""
        extra_columns = extra_columns or {}

        columns = [id_column, embedding_column] + list(extra_columns.keys())
        placeholders = ["$1", "$2"] + [f"${i+3}" for i in range(len(extra_columns))]
        values = [id_value, embedding] + list(extra_columns.values())

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({id_column}) DO UPDATE
            SET {embedding_column} = EXCLUDED.{embedding_column}
        """

        await self.execute(query, *values)

    async def similarity_search(
        self,
        table: str,
        embedding_column: str,
        query_embedding: List[float],
        limit: int = 10,
        threshold: float = None,
        select_columns: List[str] = None,
        where_clause: str = None
    ) -> List[Dict[str, Any]]:
        """
        Perform vector similarity search.

        Args:
            table: Table name
            embedding_column: Column containing embeddings
            query_embedding: Query vector
            limit: Maximum results
            threshold: Minimum similarity (0-1)
            select_columns: Columns to return
            where_clause: Additional WHERE conditions

        Returns:
            List of matching rows with similarity score
        """
        select_cols = ", ".join(select_columns) if select_columns else "*"

        query = f"""
            SELECT {select_cols},
                   1 - ({embedding_column} <=> $1::vector) as similarity
            FROM {table}
        """

        conditions = []
        if where_clause:
            conditions.append(where_clause)
        if threshold:
            conditions.append(f"1 - ({embedding_column} <=> $1::vector) >= {threshold}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f"""
            ORDER BY {embedding_column} <=> $1::vector
            LIMIT {limit}
        """

        rows = await self.fetch(query, query_embedding)
        return [dict(row) for row in rows]

    async def hybrid_search(
        self,
        table: str,
        embedding_column: str,
        text_column: str,
        query_embedding: List[float],
        query_text: str,
        limit: int = 10,
        vector_weight: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Hybrid search combining vector similarity and full-text search.

        Args:
            table: Table name
            embedding_column: Vector column
            text_column: Text column for FTS
            query_embedding: Query vector
            query_text: Query text for FTS
            limit: Maximum results
            vector_weight: Weight for vector score (0-1)

        Returns:
            List of matching rows with combined score
        """
        text_weight = 1 - vector_weight

        query = f"""
            WITH vector_search AS (
                SELECT id, 1 - ({embedding_column} <=> $1::vector) as vector_score
                FROM {table}
                ORDER BY {embedding_column} <=> $1::vector
                LIMIT {limit * 2}
            ),
            text_search AS (
                SELECT id, ts_rank_cd(to_tsvector({text_column}), plainto_tsquery($2)) as text_score
                FROM {table}
                WHERE to_tsvector({text_column}) @@ plainto_tsquery($2)
                LIMIT {limit * 2}
            )
            SELECT t.*,
                   COALESCE(v.vector_score, 0) * {vector_weight} +
                   COALESCE(ts.text_score, 0) * {text_weight} as combined_score
            FROM {table} t
            LEFT JOIN vector_search v ON t.id = v.id
            LEFT JOIN text_search ts ON t.id = ts.id
            WHERE v.id IS NOT NULL OR ts.id IS NOT NULL
            ORDER BY combined_score DESC
            LIMIT {limit}
        """

        rows = await self.fetch(query, query_embedding, query_text)
        return [dict(row) for row in rows]


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_pool: Optional[AsyncPGPool] = None
_vector_pool: Optional[VectorPool] = None


async def get_pool() -> AsyncPGPool:
    """Get or create pool singleton"""
    global _pool
    if _pool is None:
        _pool = AsyncPGPool()
        await _pool.connect()
    return _pool


async def get_vector_pool() -> VectorPool:
    """Get or create vector pool singleton"""
    global _vector_pool
    if _vector_pool is None:
        _vector_pool = VectorPool()
        await _vector_pool.connect()
    return _vector_pool


def is_asyncpg_available() -> bool:
    """Check if asyncpg is available"""
    return ASYNCPG_AVAILABLE


# =============================================================================
# PGBOUNCER CONFIGURATION
# =============================================================================

PGBOUNCER_INI = """
;; T.A.L.O.S. PgBouncer Configuration
;; Save as /etc/pgbouncer/pgbouncer.ini

[databases]
talos = host=localhost port=5432 dbname=talos

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt

;; Pool settings
pool_mode = transaction
default_pool_size = 20
min_pool_size = 5
max_client_conn = 1000
max_db_connections = 50

;; Connection settings
server_idle_timeout = 600
server_lifetime = 3600
client_idle_timeout = 0

;; Query settings
query_timeout = 120
query_wait_timeout = 120

;; Logging
log_connections = 0
log_disconnections = 0
log_pooler_errors = 1

;; Stats
stats_period = 60
"""

PGBOUNCER_USERLIST = """
;; Save as /etc/pgbouncer/userlist.txt
;; Format: "username" "password_hash"
;; Generate hash: echo -n "md5$(echo -n 'passwordusername' | md5sum | cut -d' ' -f1)"

"talos" "md5..."
"""

# =============================================================================
# SETUP INSTRUCTIONS
# =============================================================================
"""
PgBouncer Installation (macOS):
    brew install pgbouncer

PgBouncer Installation (Ubuntu):
    sudo apt install pgbouncer

Configuration:
    1. Create /etc/pgbouncer/pgbouncer.ini (see PGBOUNCER_INI above)
    2. Create /etc/pgbouncer/userlist.txt with user credentials
    3. Start: pgbouncer /etc/pgbouncer/pgbouncer.ini

Connect through pgbouncer:
    psql -h localhost -p 6432 -U talos talos

Performance Comparison:
    Direct connection:     5-10ms overhead per query
    asyncpg pool:          ~1ms overhead per query
    pgbouncer + asyncpg:   <0.5ms overhead per query

Pool modes:
    - session: Connection per session (like direct)
    - transaction: Connection per transaction (recommended)
    - statement: Connection per statement (most efficient, some limitations)

Environment variables for T.A.L.O.S.:
    PGBOUNCER_HOST=localhost
    PGBOUNCER_PORT=6432
    POSTGRES_DB=talos
    POSTGRES_USER=talos
    POSTGRES_PASSWORD=talos
"""

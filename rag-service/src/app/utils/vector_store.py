"""
T.A.L.O.S. Vector Store with pgvector
Fast vector similarity search using PostgreSQL + pgvector

Features:
- HNSW indexing for fast approximate nearest neighbor
- Async streaming for large result sets
- Batch upsert operations
- Hybrid search (vector + keyword)
- Uses shared database connection pool
"""

import os
import sys
import logging
from typing import List, Dict, Any, Optional, AsyncIterator, Tuple
from dataclasses import dataclass

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

logger = logging.getLogger(__name__)

# Try to import shared database module
try:
    from assemblyline_common.database import get_db_pool, get_connection, close_db_pool
    SHARED_DB_AVAILABLE = True
except ImportError:
    SHARED_DB_AVAILABLE = False
    logger.warning("Shared database module not available")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class VectorStoreConfig:
    """Configuration for vector store"""
    embedding_dim: int = int(os.getenv("EMBEDDING_DIM", "768"))  # nomic-embed-text default
    index_type: str = "hnsw"  # hnsw or ivfflat
    distance_metric: str = "cosine"  # cosine, l2, inner_product
    ef_construction: int = 128  # HNSW build parameter
    ef_search: int = 64  # HNSW search parameter
    m: int = 16  # HNSW connections per layer


# =============================================================================
# VECTOR STORE
# =============================================================================

class PgVectorStore:
    """
    PostgreSQL vector store using pgvector extension.

    Uses the shared database connection pool for efficient resource sharing.

    Provides:
    - Fast HNSW-based similarity search
    - Async streaming for large results
    - Batch operations
    - Hybrid search
    """

    def __init__(self, config: Optional[VectorStoreConfig] = None):
        self.config = config or VectorStoreConfig()
        self._initialized = False

    async def initialize(self):
        """Initialize using shared database pool and ensure tables exist"""
        if self._initialized:
            return

        if not SHARED_DB_AVAILABLE:
            raise RuntimeError("Shared database module not available")

        try:
            # Get shared connection pool
            await get_db_pool()

            async with get_connection() as conn:
                # Enable pgvector extension
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")

                # Create embeddings table (uses document_chunks from shared schema)
                # But we also maintain a local embeddings table for RAG-specific needs
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS embeddings (
                        id SERIAL PRIMARY KEY,
                        document_id INTEGER NOT NULL,
                        chunk_id INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        embedding vector({self.config.embedding_dim}),
                        metadata JSONB DEFAULT '{{}}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(document_id, chunk_id)
                    )
                """)

                # Create HNSW index for fast similarity search
                if self.config.index_type == "hnsw":
                    distance_ops = {
                        "cosine": "vector_cosine_ops",
                        "l2": "vector_l2_ops",
                        "inner_product": "vector_ip_ops"
                    }
                    ops = distance_ops.get(self.config.distance_metric, "vector_cosine_ops")

                    await conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS embeddings_hnsw_idx
                        ON embeddings
                        USING hnsw (embedding {ops})
                        WITH (m = {self.config.m}, ef_construction = {self.config.ef_construction})
                    """)

                # Create index for document lookups
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS embeddings_document_idx
                    ON embeddings(document_id)
                """)

                # Create GIN index for metadata search
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS embeddings_metadata_idx
                    ON embeddings USING gin(metadata)
                """)

            self._initialized = True
            logger.info("✅ PgVectorStore initialized with HNSW index (shared pool)")

        except Exception as e:
            logger.error(f"Failed to initialize vector store: {e}")
            raise

    async def close(self):
        """Close is handled by shared database module"""
        self._initialized = False

    async def upsert(
        self,
        document_id: int,
        chunks: List[Dict[str, Any]],
        embeddings: List[List[float]]
    ) -> int:
        """
        Upsert document chunks with embeddings.

        Args:
            document_id: Document ID
            chunks: List of chunk dicts with 'content' and optional 'metadata'
            embeddings: List of embedding vectors

        Returns:
            Number of chunks upserted
        """
        if not self._initialized:
            await self.initialize()

        if len(chunks) != len(embeddings):
            raise ValueError("Number of chunks must match number of embeddings")

        import json

        async with get_connection() as conn:
            # Use batch insert with ON CONFLICT
            count = 0
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                await conn.execute("""
                    INSERT INTO embeddings (document_id, chunk_id, content, embedding, metadata)
                    VALUES ($1, $2, $3, $4::vector, $5::jsonb)
                    ON CONFLICT (document_id, chunk_id)
                    DO UPDATE SET
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata
                """,
                    document_id,
                    i,
                    chunk.get("content", ""),
                    str(embedding),
                    json.dumps(chunk.get("metadata", {}))
                )
                count += 1

            logger.info(f"📥 Upserted {count} chunks for document {document_id}")
            return count

    async def search(
        self,
        query_embedding: List[float],
        user_id: Optional[int] = None,
        limit: int = 10,
        threshold: float = 0.0,
        document_ids: Optional[List[int]] = None,
        metadata_filter: Optional[Dict[str, Any]] = None,
        organization_id: Optional[int] = None,
        department: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar vectors.

        Args:
            query_embedding: Query vector
            user_id: User ID for multi-tenant isolation (required for security)
            limit: Maximum results
            threshold: Minimum similarity score (0-1 for cosine)
            document_ids: Optional filter by document IDs
            metadata_filter: Optional metadata filter
            organization_id: Optional organization ID for org-wide search (enterprise)
            department: Optional department for department-scoped search (enterprise)

        Returns:
            List of results with content, score, and metadata
        """
        if not self._initialized:
            await self.initialize()

        import json

        # Build query based on distance metric
        if self.config.distance_metric == "cosine":
            distance_sql = "1 - (e.embedding <=> $1::vector)"
        elif self.config.distance_metric == "l2":
            distance_sql = "1 / (1 + (e.embedding <-> $1::vector))"
        else:  # inner_product
            distance_sql = "(e.embedding <#> $1::vector) * -1"

        # Build WHERE clause
        where_clauses = []
        params = [str(query_embedding)]

        # Multi-tenant isolation with department support
        if department and organization_id:
            # Department isolation: search documents from users in the same department
            where_clauses.append(f"""
                d.user_id IN (
                    SELECT id FROM users
                    WHERE organization_id = ${len(params) + 1} AND department = ${len(params) + 2}
                )
            """)
            params.append(organization_id)
            params.append(department)
            where_clauses.append("d.deleted_at IS NULL")
            where_clauses.append("d.flagged_at IS NULL")  # Exclude flagged documents from AI
        elif organization_id:
            # Organization-wide search (admin feature)
            where_clauses.append(f"""
                d.user_id IN (
                    SELECT id FROM users WHERE organization_id = ${len(params) + 1}
                )
            """)
            params.append(organization_id)
            where_clauses.append("d.deleted_at IS NULL")
            where_clauses.append("d.flagged_at IS NULL")  # Exclude flagged documents from AI
        elif user_id is not None:
            # Default: user's own documents only
            where_clauses.append(f"d.user_id = ${len(params) + 1}")
            params.append(user_id)
            where_clauses.append("d.deleted_at IS NULL")
            where_clauses.append("d.flagged_at IS NULL")  # Exclude flagged documents from AI

        if threshold > 0:
            where_clauses.append(f"{distance_sql} >= ${len(params) + 1}")
            params.append(threshold)

        if document_ids:
            where_clauses.append(f"e.document_id = ANY(${len(params) + 1}::int[])")
            params.append(document_ids)

        if metadata_filter:
            where_clauses.append(f"e.metadata @> ${len(params) + 1}::jsonb")
            params.append(json.dumps(metadata_filter))

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        # Join with documents table for user isolation
        query = f"""
            SELECT
                e.document_id,
                e.chunk_id,
                e.content,
                e.metadata,
                {distance_sql} as score
            FROM embeddings e
            JOIN documents d ON d.id = e.document_id
            WHERE {where_sql}
            ORDER BY e.embedding <=> $1::vector
            LIMIT ${len(params) + 1}
        """
        params.append(limit)

        async with get_connection() as conn:
            # Set HNSW search parameter
            await conn.execute(f"SET hnsw.ef_search = {self.config.ef_search}")

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                # Handle metadata - can be dict, string, or None
                metadata = row["metadata"]
                if metadata is None:
                    metadata = {}
                elif isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                else:
                    metadata = dict(metadata)

                results.append({
                    "document_id": row["document_id"],
                    "chunk_id": row["chunk_id"],
                    "content": row["content"],
                    "metadata": metadata,
                    "score": float(row["score"])
                })

            return results

    async def stream_search(
        self,
        query_embedding: List[float],
        user_id: Optional[int] = None,
        limit: int = 100,
        batch_size: int = 10,
        organization_id: Optional[int] = None,
        department: Optional[str] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream search results for large result sets with user isolation.

        Yields results in batches to reduce memory usage.

        Args:
            query_embedding: Query vector
            user_id: User ID for multi-tenant isolation
            limit: Maximum total results
            batch_size: Results per batch
            organization_id: Optional organization ID for org-wide search (enterprise)
            department: Optional department for department-scoped search (enterprise)
        """
        if not self._initialized:
            await self.initialize()

        offset = 0
        while True:
            results = await self.search(
                query_embedding,
                user_id=user_id,
                limit=batch_size,
                threshold=0.0,
                organization_id=organization_id,
                department=department
            )

            if not results:
                break

            for result in results:
                yield result

            offset += batch_size
            if offset >= limit:
                break

    async def add_vectors(
        self,
        vectors: List[List[float]],
        metadata: List[Dict[str, Any]]
    ) -> int:
        """
        Add vectors with metadata (for web crawl content without document_id).

        Uses document_id = 0 and auto-incrementing chunk_id for web content.

        Args:
            vectors: List of embedding vectors
            metadata: List of metadata dicts with 'content' key

        Returns:
            Number of vectors added
        """
        if not self._initialized:
            await self.initialize()

        if len(vectors) != len(metadata):
            raise ValueError("Number of vectors must match number of metadata entries")

        import json
        import hashlib

        async with get_connection() as conn:
            # Get next chunk_id for web content (document_id = 0)
            max_chunk = await conn.fetchval(
                "SELECT COALESCE(MAX(chunk_id), -1) FROM embeddings WHERE document_id = 0"
            )

            count = 0
            for i, (vector, meta) in enumerate(zip(vectors, metadata)):
                chunk_id = max_chunk + 1 + i
                content = meta.get("content", "")

                # Create a unique hash for deduplication
                content_hash = hashlib.md5(content.encode()).hexdigest()

                # Check for duplicate content
                existing = await conn.fetchval(
                    "SELECT 1 FROM embeddings WHERE metadata->>'content_hash' = $1",
                    content_hash
                )

                if existing:
                    logger.debug(f"Skipping duplicate content: {content[:50]}...")
                    continue

                # Add content_hash to metadata
                meta["content_hash"] = content_hash

                await conn.execute("""
                    INSERT INTO embeddings (document_id, chunk_id, content, embedding, metadata)
                    VALUES ($1, $2, $3, $4::vector, $5::jsonb)
                """,
                    0,  # document_id = 0 for web content
                    chunk_id,
                    content,
                    str(vector),
                    json.dumps(meta)
                )
                count += 1

            logger.info(f"📥 Added {count} web content vectors")
            return count

    async def delete_document(self, document_id: int) -> int:
        """Delete all chunks for a document"""
        if not self._initialized:
            await self.initialize()

        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM embeddings WHERE document_id = $1",
                document_id
            )
            count = int(result.split()[-1])
            logger.info(f"🗑️ Deleted {count} chunks for document {document_id}")
            return count

    async def get_stats(self) -> Dict[str, Any]:
        """Get vector store statistics"""
        if not self._initialized:
            await self.initialize()

        async with get_connection() as conn:
            total_chunks = await conn.fetchval(
                "SELECT COUNT(*) FROM embeddings"
            )
            total_documents = await conn.fetchval(
                "SELECT COUNT(DISTINCT document_id) FROM embeddings"
            )

            return {
                "total_chunks": total_chunks,
                "total_documents": total_documents,
                "embedding_dim": self.config.embedding_dim,
                "index_type": self.config.index_type,
                "distance_metric": self.config.distance_metric,
                "backend": "postgresql"
            }

    async def get_document_chunks(self, document_id: int) -> List[Dict[str, Any]]:
        """
        Get all chunks for a document.

        Args:
            document_id: Document ID

        Returns:
            List of chunk dicts with content, metadata, and embedding
        """
        if not self._initialized:
            await self.initialize()

        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    chunk_id,
                    content,
                    metadata,
                    embedding::text as embedding
                FROM embeddings
                WHERE document_id = $1
                ORDER BY chunk_id
            """, document_id)

            results = []
            for row in rows:
                # Parse embedding from string format
                embedding = None
                if row["embedding"]:
                    try:
                        # pgvector returns embedding as '[0.1,0.2,...]' format
                        emb_str = row["embedding"].strip("[]")
                        embedding = [float(x) for x in emb_str.split(",")]
                    except Exception:
                        pass

                # Handle metadata - can be dict, string, or None
                metadata = row["metadata"]
                if metadata is None:
                    metadata = {}
                elif isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                else:
                    metadata = dict(metadata)

                results.append({
                    "chunk_id": row["chunk_id"],
                    "content": row["content"],
                    "metadata": metadata,
                    "embedding": embedding
                })

            return results


# =============================================================================
# ADVANCED: MULTI-INDEX SUPPORT
# =============================================================================

@dataclass
class IndexConfig:
    """Configuration for a specific index"""
    name: str
    embedding_dim: int
    index_type: str = "hnsw"
    distance_metric: str = "cosine"
    ef_construction: int = 128
    ef_search: int = 64
    m: int = 16
    description: str = ""


@dataclass
class IndexPerformanceStats:
    """Performance statistics for an index"""
    index_name: str
    total_queries: int = 0
    total_query_time_ms: float = 0.0
    avg_query_time_ms: float = 0.0
    p95_query_time_ms: float = 0.0
    total_results_returned: int = 0
    avg_results_per_query: float = 0.0
    last_optimized: Optional[str] = None
    fragmentation_ratio: float = 0.0


class MultiIndexManager:
    """
    Manages multiple vector indexes for different embedding models.

    Enables:
    - Different embedding dimensions for different content types
    - Specialized indexes for different use cases (e.g., code vs. text)
    - A/B testing between embedding models
    - Gradual migration between embedding models
    """

    def __init__(self):
        self.indexes: Dict[str, IndexConfig] = {}
        self.performance_stats: Dict[str, IndexPerformanceStats] = {}
        self._query_times: Dict[str, List[float]] = {}  # For percentile calculations
        self._initialized = False

    async def initialize(self):
        """Initialize the multi-index manager"""
        if self._initialized:
            return

        if not SHARED_DB_AVAILABLE:
            raise RuntimeError("Shared database module not available")

        async with get_connection() as conn:
            # Enable pgvector extension
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Create index registry table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS vector_index_registry (
                    name VARCHAR(255) PRIMARY KEY,
                    embedding_dim INTEGER NOT NULL,
                    index_type VARCHAR(50) DEFAULT 'hnsw',
                    distance_metric VARCHAR(50) DEFAULT 'cosine',
                    ef_construction INTEGER DEFAULT 128,
                    ef_search INTEGER DEFAULT 64,
                    m INTEGER DEFAULT 16,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_optimized TIMESTAMP
                )
            """)

            # Create index performance tracking table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS vector_index_stats (
                    id SERIAL PRIMARY KEY,
                    index_name VARCHAR(255) REFERENCES vector_index_registry(name),
                    query_time_ms FLOAT,
                    results_count INTEGER,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Load existing indexes
            rows = await conn.fetch("SELECT * FROM vector_index_registry")
            for row in rows:
                config = IndexConfig(
                    name=row["name"],
                    embedding_dim=row["embedding_dim"],
                    index_type=row["index_type"],
                    distance_metric=row["distance_metric"],
                    ef_construction=row["ef_construction"],
                    ef_search=row["ef_search"],
                    m=row["m"],
                    description=row["description"] or ""
                )
                self.indexes[config.name] = config
                self.performance_stats[config.name] = IndexPerformanceStats(
                    index_name=config.name,
                    last_optimized=str(row["last_optimized"]) if row["last_optimized"] else None
                )
                self._query_times[config.name] = []

        self._initialized = True
        logger.info(f"MultiIndexManager initialized with {len(self.indexes)} indexes")

    async def create_index(self, config: IndexConfig) -> bool:
        """
        Create a new vector index with the specified configuration.

        Args:
            config: Index configuration

        Returns:
            True if created successfully
        """
        if not self._initialized:
            await self.initialize()

        if config.name in self.indexes:
            logger.warning(f"Index {config.name} already exists")
            return False

        async with get_connection() as conn:
            # Create embeddings table for this index
            table_name = f"embeddings_{config.name}"

            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    document_id INTEGER NOT NULL,
                    chunk_id INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    embedding vector({config.embedding_dim}),
                    metadata JSONB DEFAULT '{{}}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(document_id, chunk_id)
                )
            """)

            # Create HNSW index
            distance_ops = {
                "cosine": "vector_cosine_ops",
                "l2": "vector_l2_ops",
                "inner_product": "vector_ip_ops"
            }
            ops = distance_ops.get(config.distance_metric, "vector_cosine_ops")

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {table_name}_hnsw_idx
                ON {table_name}
                USING hnsw (embedding {ops})
                WITH (m = {config.m}, ef_construction = {config.ef_construction})
            """)

            # Create supporting indexes
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {table_name}_document_idx
                ON {table_name}(document_id)
            """)

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {table_name}_metadata_idx
                ON {table_name} USING gin(metadata)
            """)

            # Register in registry
            await conn.execute("""
                INSERT INTO vector_index_registry
                (name, embedding_dim, index_type, distance_metric, ef_construction, ef_search, m, description)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, config.name, config.embedding_dim, config.index_type,
               config.distance_metric, config.ef_construction, config.ef_search,
               config.m, config.description)

            self.indexes[config.name] = config
            self.performance_stats[config.name] = IndexPerformanceStats(index_name=config.name)
            self._query_times[config.name] = []

            logger.info(f"Created index {config.name} with dimension {config.embedding_dim}")
            return True

    async def search_index(
        self,
        index_name: str,
        query_embedding: List[float],
        limit: int = 10,
        threshold: float = 0.0,
        metadata_filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search a specific index.

        Args:
            index_name: Name of the index to search
            query_embedding: Query vector
            limit: Maximum results
            threshold: Minimum similarity score
            metadata_filter: Optional metadata filter

        Returns:
            List of results with content, score, and metadata
        """
        if not self._initialized:
            await self.initialize()

        if index_name not in self.indexes:
            raise ValueError(f"Index {index_name} not found")

        import time
        import json

        start_time = time.time()
        config = self.indexes[index_name]
        table_name = f"embeddings_{index_name}"

        # Build distance SQL
        if config.distance_metric == "cosine":
            distance_sql = f"1 - (embedding <=> $1::vector)"
        elif config.distance_metric == "l2":
            distance_sql = f"1 / (1 + (embedding <-> $1::vector))"
        else:
            distance_sql = f"(embedding <#> $1::vector) * -1"

        # Build WHERE clause
        where_clauses = []
        params = [str(query_embedding)]

        if threshold > 0:
            where_clauses.append(f"{distance_sql} >= ${len(params) + 1}")
            params.append(threshold)

        if metadata_filter:
            where_clauses.append(f"metadata @> ${len(params) + 1}::jsonb")
            params.append(json.dumps(metadata_filter))

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        query = f"""
            SELECT document_id, chunk_id, content, metadata, {distance_sql} as score
            FROM {table_name}
            WHERE {where_sql}
            ORDER BY embedding <=> $1::vector
            LIMIT ${len(params) + 1}
        """
        params.append(limit)

        async with get_connection() as conn:
            await conn.execute(f"SET hnsw.ef_search = {config.ef_search}")
            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                metadata = row["metadata"]
                if metadata is None:
                    metadata = {}
                elif isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                else:
                    metadata = dict(metadata)

                results.append({
                    "document_id": row["document_id"],
                    "chunk_id": row["chunk_id"],
                    "content": row["content"],
                    "metadata": metadata,
                    "score": float(row["score"]),
                    "index": index_name
                })

        # Record performance stats
        query_time_ms = (time.time() - start_time) * 1000
        await self._record_query_stats(index_name, query_time_ms, len(results))

        return results

    async def search_multiple_indexes(
        self,
        index_names: List[str],
        query_embeddings: Dict[str, List[float]],
        limit: int = 10,
        merge_strategy: str = "interleave"
    ) -> List[Dict[str, Any]]:
        """
        Search multiple indexes and merge results.

        Args:
            index_names: List of index names to search
            query_embeddings: Dict mapping index name to query embedding
            limit: Maximum total results
            merge_strategy: How to merge results ("interleave" or "score_based")

        Returns:
            Merged list of results
        """
        import asyncio

        # Search all indexes in parallel
        tasks = []
        for name in index_names:
            if name in query_embeddings:
                tasks.append(self.search_index(name, query_embeddings[name], limit))

        all_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten and merge results
        merged = []
        for results in all_results:
            if isinstance(results, Exception):
                logger.warning(f"Index search failed: {results}")
                continue
            merged.extend(results)

        # Merge based on strategy
        if merge_strategy == "score_based":
            # Sort by score and take top results
            merged.sort(key=lambda x: x["score"], reverse=True)
            return merged[:limit]
        else:
            # Interleave results from different indexes
            from itertools import zip_longest
            results_by_index = {}
            for r in merged:
                idx = r.get("index", "default")
                if idx not in results_by_index:
                    results_by_index[idx] = []
                results_by_index[idx].append(r)

            interleaved = []
            for items in zip_longest(*results_by_index.values()):
                for item in items:
                    if item is not None:
                        interleaved.append(item)

            return interleaved[:limit]

    async def _record_query_stats(
        self,
        index_name: str,
        query_time_ms: float,
        results_count: int
    ):
        """Record query performance statistics"""
        if index_name not in self.performance_stats:
            return

        stats = self.performance_stats[index_name]
        stats.total_queries += 1
        stats.total_query_time_ms += query_time_ms
        stats.avg_query_time_ms = stats.total_query_time_ms / stats.total_queries
        stats.total_results_returned += results_count
        stats.avg_results_per_query = stats.total_results_returned / stats.total_queries

        # Track for percentile calculations
        self._query_times[index_name].append(query_time_ms)
        if len(self._query_times[index_name]) > 1000:
            self._query_times[index_name] = self._query_times[index_name][-1000:]

        # Calculate P95
        times = sorted(self._query_times[index_name])
        if times:
            p95_idx = int(len(times) * 0.95)
            stats.p95_query_time_ms = times[min(p95_idx, len(times) - 1)]

        # Persist to database (async, don't block)
        try:
            async with get_connection() as conn:
                await conn.execute("""
                    INSERT INTO vector_index_stats (index_name, query_time_ms, results_count)
                    VALUES ($1, $2, $3)
                """, index_name, query_time_ms, results_count)
        except Exception as e:
            logger.debug(f"Failed to persist query stats: {e}")

    def get_performance_stats(self, index_name: str) -> Optional[IndexPerformanceStats]:
        """Get performance statistics for an index"""
        return self.performance_stats.get(index_name)

    def get_all_stats(self) -> Dict[str, IndexPerformanceStats]:
        """Get performance statistics for all indexes"""
        return self.performance_stats.copy()

    async def list_indexes(self) -> List[IndexConfig]:
        """List all registered indexes"""
        if not self._initialized:
            await self.initialize()
        return list(self.indexes.values())


class IndexOptimizer:
    """
    Automatically optimizes vector indexes based on query patterns.

    Provides:
    - Automatic ef_search tuning based on query latency targets
    - Index fragmentation detection and reindex recommendations
    - Unused index detection
    - Index size and memory usage tracking
    """

    def __init__(self, multi_index_manager: MultiIndexManager):
        self.manager = multi_index_manager
        self.latency_target_ms: float = 50.0  # Target P95 latency
        self.fragmentation_threshold: float = 0.3  # 30% fragmentation triggers alert
        self._optimization_history: List[Dict[str, Any]] = []

    async def analyze_index(self, index_name: str) -> Dict[str, Any]:
        """
        Analyze an index and provide optimization recommendations.

        Args:
            index_name: Name of the index to analyze

        Returns:
            Analysis results with recommendations
        """
        if index_name not in self.manager.indexes:
            raise ValueError(f"Index {index_name} not found")

        config = self.manager.indexes[index_name]
        stats = self.manager.get_performance_stats(index_name)

        analysis = {
            "index_name": index_name,
            "current_config": {
                "embedding_dim": config.embedding_dim,
                "ef_search": config.ef_search,
                "ef_construction": config.ef_construction,
                "m": config.m,
                "distance_metric": config.distance_metric
            },
            "performance": {
                "total_queries": stats.total_queries if stats else 0,
                "avg_query_time_ms": stats.avg_query_time_ms if stats else 0,
                "p95_query_time_ms": stats.p95_query_time_ms if stats else 0,
                "avg_results_per_query": stats.avg_results_per_query if stats else 0
            },
            "recommendations": [],
            "health_score": 100.0
        }

        # Get index size and fragmentation from database
        async with get_connection() as conn:
            table_name = f"embeddings_{index_name}"

            # Get row count
            try:
                row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                analysis["row_count"] = row_count
            except Exception:
                analysis["row_count"] = 0

            # Get table size
            try:
                size_result = await conn.fetchrow(f"""
                    SELECT pg_size_pretty(pg_total_relation_size('{table_name}')) as size,
                           pg_total_relation_size('{table_name}') as bytes
                """)
                analysis["table_size"] = size_result["size"] if size_result else "unknown"
                analysis["table_size_bytes"] = size_result["bytes"] if size_result else 0
            except Exception:
                analysis["table_size"] = "unknown"
                analysis["table_size_bytes"] = 0

        # Generate recommendations
        if stats and stats.p95_query_time_ms > self.latency_target_ms:
            # Latency is too high
            if config.ef_search < 128:
                analysis["recommendations"].append({
                    "type": "increase_ef_search",
                    "reason": f"P95 latency ({stats.p95_query_time_ms:.1f}ms) exceeds target ({self.latency_target_ms}ms)",
                    "action": f"Consider increasing ef_search from {config.ef_search} to {min(config.ef_search * 2, 256)}",
                    "priority": "high"
                })
                analysis["health_score"] -= 20
            else:
                analysis["recommendations"].append({
                    "type": "review_queries",
                    "reason": "ef_search is already high, latency issues may be due to other factors",
                    "action": "Review query patterns and consider adding filters",
                    "priority": "medium"
                })
                analysis["health_score"] -= 10

        if stats and stats.total_queries == 0:
            analysis["recommendations"].append({
                "type": "unused_index",
                "reason": "Index has not been queried",
                "action": "Consider removing if not needed",
                "priority": "low"
            })
            analysis["health_score"] -= 5

        if analysis["row_count"] > 1000000 and config.m < 32:
            analysis["recommendations"].append({
                "type": "increase_m",
                "reason": f"Large index ({analysis['row_count']:,} rows) may benefit from higher m",
                "action": f"Consider increasing m from {config.m} to 32 for better recall",
                "priority": "medium"
            })
            analysis["health_score"] -= 10

        if not analysis["recommendations"]:
            analysis["recommendations"].append({
                "type": "healthy",
                "reason": "Index is performing well",
                "action": "No changes recommended",
                "priority": "info"
            })

        return analysis

    async def auto_tune_ef_search(self, index_name: str) -> Optional[int]:
        """
        Automatically tune ef_search based on query performance.

        Args:
            index_name: Name of the index to tune

        Returns:
            New ef_search value if changed, None otherwise
        """
        stats = self.manager.get_performance_stats(index_name)
        if not stats or stats.total_queries < 100:
            logger.info(f"Not enough queries ({stats.total_queries if stats else 0}) to tune {index_name}")
            return None

        config = self.manager.indexes[index_name]
        current_ef = config.ef_search

        # Determine optimal ef_search based on latency
        if stats.p95_query_time_ms > self.latency_target_ms * 1.5:
            # Significantly over target - decrease ef_search (trade recall for speed)
            new_ef = max(32, int(current_ef * 0.75))
        elif stats.p95_query_time_ms > self.latency_target_ms:
            # Slightly over target - small decrease
            new_ef = max(32, int(current_ef * 0.9))
        elif stats.p95_query_time_ms < self.latency_target_ms * 0.5:
            # Well under target - can increase ef_search for better recall
            new_ef = min(256, int(current_ef * 1.25))
        else:
            # Within acceptable range
            return None

        if new_ef == current_ef:
            return None

        # Update configuration
        config.ef_search = new_ef
        self.manager.indexes[index_name] = config

        # Update in database
        async with get_connection() as conn:
            await conn.execute("""
                UPDATE vector_index_registry SET ef_search = $1 WHERE name = $2
            """, new_ef, index_name)

        self._optimization_history.append({
            "timestamp": str(asyncio.get_event_loop().time()),
            "index_name": index_name,
            "action": "tune_ef_search",
            "old_value": current_ef,
            "new_value": new_ef,
            "reason": f"P95 latency: {stats.p95_query_time_ms:.1f}ms, target: {self.latency_target_ms}ms"
        })

        logger.info(f"Auto-tuned {index_name} ef_search: {current_ef} -> {new_ef}")
        return new_ef

    async def optimize_all_indexes(self) -> Dict[str, Any]:
        """
        Run optimization analysis on all indexes.

        Returns:
            Summary of all optimizations performed
        """
        results = {
            "analyzed_indexes": [],
            "optimizations_applied": [],
            "recommendations": []
        }

        for index_name in self.manager.indexes:
            # Analyze
            analysis = await self.analyze_index(index_name)
            results["analyzed_indexes"].append(analysis)

            # Auto-tune if needed
            new_ef = await self.auto_tune_ef_search(index_name)
            if new_ef:
                results["optimizations_applied"].append({
                    "index": index_name,
                    "action": "ef_search_tuned",
                    "new_value": new_ef
                })

            # Collect high-priority recommendations
            for rec in analysis["recommendations"]:
                if rec["priority"] in ("high", "medium"):
                    results["recommendations"].append({
                        "index": index_name,
                        **rec
                    })

        return results

    def get_optimization_history(self) -> List[Dict[str, Any]]:
        """Get history of automatic optimizations"""
        return self._optimization_history.copy()

    def set_latency_target(self, target_ms: float):
        """Set the target P95 latency for optimization"""
        self.latency_target_ms = target_ms


# =============================================================================
# CUTTING EDGE: Learned Index Structures & Query-Adaptive Selection
# =============================================================================

from enum import Enum
from datetime import datetime


class QueryType(str, Enum):
    """Types of queries for adaptive routing."""
    SEMANTIC = "semantic"  # Conceptual/meaning-based
    LEXICAL = "lexical"  # Keyword-focused
    HYBRID = "hybrid"  # Mix of both
    EXACT = "exact"  # Looking for specific item
    EXPLORATORY = "exploratory"  # Browsing/discovery
    COMPARATIVE = "comparative"  # Comparing items


class IndexSelectionStrategy(str, Enum):
    """Strategies for selecting indexes."""
    LEARNED = "learned"  # ML-based selection
    RULE_BASED = "rule_based"  # Heuristic rules
    ENSEMBLE = "ensemble"  # Combine multiple indexes
    PERFORMANCE = "performance"  # Based on historical performance


@dataclass
class QueryFeatures:
    """Features extracted from a query for routing."""
    query_id: str
    query_text: str
    query_type: QueryType
    embedding_norm: float
    term_count: int
    avg_term_length: float
    has_technical_terms: bool
    has_named_entities: bool
    estimated_specificity: float  # 0=broad, 1=very specific
    domain_hint: Optional[str] = None
    extracted_at: datetime = None

    def __post_init__(self):
        if self.extracted_at is None:
            self.extracted_at = datetime.now()

    def to_feature_vector(self) -> List[float]:
        """Convert to numeric feature vector for ML."""
        return [
            self.embedding_norm,
            self.term_count / 20.0,  # Normalize
            self.avg_term_length / 10.0,
            1.0 if self.has_technical_terms else 0.0,
            1.0 if self.has_named_entities else 0.0,
            self.estimated_specificity,
            {"semantic": 0.0, "lexical": 0.25, "hybrid": 0.5,
             "exact": 0.75, "exploratory": 0.85, "comparative": 1.0}.get(self.query_type.value, 0.5)
        ]


@dataclass
class IndexRoutingDecision:
    """Decision on which index(es) to use."""
    decision_id: str
    query_features: QueryFeatures
    selected_indexes: List[str]
    confidence: float
    strategy_used: IndexSelectionStrategy
    reasoning: str
    weights: Dict[str, float]  # Index name -> weight for ensemble
    decided_at: datetime = None

    def __post_init__(self):
        if self.decided_at is None:
            self.decided_at = datetime.now()


@dataclass
class IndexPerformanceFeedback:
    """Feedback on index performance for learning."""
    query_features: QueryFeatures
    index_name: str
    latency_ms: float
    results_count: int
    avg_relevance_score: float
    user_clicked_result: bool = False
    click_position: Optional[int] = None
    recorded_at: datetime = None

    def __post_init__(self):
        if self.recorded_at is None:
            self.recorded_at = datetime.now()


@dataclass
class LearnedIndexConfig:
    """Configuration for learned index structures."""
    enable_query_classification: bool = True
    enable_adaptive_routing: bool = True
    enable_online_learning: bool = True
    min_samples_for_learning: int = 100
    learning_rate: float = 0.01
    exploration_rate: float = 0.1  # Epsilon for exploration
    feedback_window_size: int = 1000


@dataclass
class CuttingEdgeVectorStoreConfig:
    """Configuration for Cutting Edge vector store."""
    enable_learned_index: bool = True
    enable_adaptive_selection: bool = True
    learned_config: LearnedIndexConfig = None

    def __post_init__(self):
        if self.learned_config is None:
            self.learned_config = LearnedIndexConfig()


class QueryClassifier:
    """
    Classifies queries to determine optimal retrieval strategy.

    Uses lightweight heuristics and learned patterns to classify queries.
    """

    def __init__(self):
        self._technical_terms = {
            "api", "function", "class", "method", "error", "exception",
            "database", "query", "index", "schema", "table", "column",
            "algorithm", "implementation", "interface", "module", "package"
        }
        self._entity_patterns = [
            r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b',  # Proper names
            r'\b[A-Z]{2,}\b',  # Acronyms
            r'\b\d{4}-\d{2}-\d{2}\b',  # Dates
        ]

    def classify(self, query: str, embedding: Optional[List[float]] = None) -> QueryFeatures:
        """Classify a query and extract features."""
        import re
        import uuid
        import math

        query_lower = query.lower()
        terms = query_lower.split()

        # Calculate embedding norm
        embedding_norm = 0.0
        if embedding:
            embedding_norm = math.sqrt(sum(x*x for x in embedding))

        # Check for technical terms
        has_technical = any(term in self._technical_terms for term in terms)

        # Check for named entities
        has_entities = any(re.search(pattern, query) for pattern in self._entity_patterns)

        # Estimate specificity
        specificity = min(1.0, len(terms) / 10.0)
        if any(c in query for c in ['"', "'", ":"]):
            specificity = min(1.0, specificity + 0.2)

        # Determine query type
        if '"' in query or "exact" in query_lower:
            query_type = QueryType.EXACT
        elif any(kw in query_lower for kw in ["compare", "versus", "vs", "difference"]):
            query_type = QueryType.COMPARATIVE
        elif any(kw in query_lower for kw in ["browse", "explore", "show me", "list"]):
            query_type = QueryType.EXPLORATORY
        elif len(terms) <= 2 and not has_technical:
            query_type = QueryType.LEXICAL
        elif has_technical or specificity > 0.7:
            query_type = QueryType.SEMANTIC
        else:
            query_type = QueryType.HYBRID

        return QueryFeatures(
            query_id=f"q_{uuid.uuid4().hex[:8]}",
            query_text=query,
            query_type=query_type,
            embedding_norm=embedding_norm,
            term_count=len(terms),
            avg_term_length=sum(len(t) for t in terms) / max(1, len(terms)),
            has_technical_terms=has_technical,
            has_named_entities=has_entities,
            estimated_specificity=specificity
        )


class LearnedIndexRouter:
    """
    Routes queries to optimal indexes using learned patterns.

    Combines:
    - Query classification
    - Historical performance data
    - Online learning from feedback
    """

    def __init__(self, config: Optional[LearnedIndexConfig] = None):
        self.config = config or LearnedIndexConfig()
        self.classifier = QueryClassifier()

        # Index performance models (simple weighted averages)
        self._index_weights: Dict[str, Dict[QueryType, float]] = {}
        self._feedback_history: List[IndexPerformanceFeedback] = []
        self._routing_history: List[IndexRoutingDecision] = []

        # Default weights per query type
        self._default_weights = {
            QueryType.SEMANTIC: {"hnsw_cosine": 0.9, "hnsw_l2": 0.7, "ivfflat": 0.5},
            QueryType.LEXICAL: {"hnsw_cosine": 0.6, "hnsw_l2": 0.5, "ivfflat": 0.7},
            QueryType.HYBRID: {"hnsw_cosine": 0.8, "hnsw_l2": 0.6, "ivfflat": 0.6},
            QueryType.EXACT: {"hnsw_cosine": 0.5, "hnsw_l2": 0.5, "ivfflat": 0.8},
            QueryType.EXPLORATORY: {"hnsw_cosine": 0.7, "hnsw_l2": 0.6, "ivfflat": 0.5},
            QueryType.COMPARATIVE: {"hnsw_cosine": 0.8, "hnsw_l2": 0.7, "ivfflat": 0.6}
        }

    def register_index(self, index_name: str, index_type: str = "hnsw_cosine"):
        """Register an index for routing."""
        if index_name not in self._index_weights:
            self._index_weights[index_name] = {}
            # Initialize with default weights based on type
            for qt in QueryType:
                default = self._default_weights.get(qt, {}).get(index_type, 0.5)
                self._index_weights[index_name][qt] = default

    def route_query(
        self,
        query: str,
        embedding: Optional[List[float]] = None,
        available_indexes: Optional[List[str]] = None,
        strategy: IndexSelectionStrategy = IndexSelectionStrategy.LEARNED
    ) -> IndexRoutingDecision:
        """
        Route a query to the optimal index(es).

        Args:
            query: The search query
            embedding: Optional query embedding
            available_indexes: List of available index names
            strategy: Selection strategy to use

        Returns:
            Routing decision with selected indexes and weights
        """
        import uuid
        import random

        # Classify query
        features = self.classifier.classify(query, embedding)

        # Get available indexes
        if available_indexes is None:
            available_indexes = list(self._index_weights.keys())

        if not available_indexes:
            # Fallback to default
            return IndexRoutingDecision(
                decision_id=f"d_{uuid.uuid4().hex[:8]}",
                query_features=features,
                selected_indexes=["default"],
                confidence=0.5,
                strategy_used=strategy,
                reasoning="No indexes available, using default",
                weights={"default": 1.0}
            )

        # Exploration: occasionally try random index
        if self.config.enable_online_learning and random.random() < self.config.exploration_rate:
            selected = random.choice(available_indexes)
            return IndexRoutingDecision(
                decision_id=f"d_{uuid.uuid4().hex[:8]}",
                query_features=features,
                selected_indexes=[selected],
                confidence=0.5,
                strategy_used=IndexSelectionStrategy.LEARNED,
                reasoning="Exploration: trying random index for learning",
                weights={selected: 1.0}
            )

        # Calculate scores for each index
        index_scores = {}
        for index_name in available_indexes:
            if index_name in self._index_weights:
                weights = self._index_weights[index_name]
                score = weights.get(features.query_type, 0.5)

                # Boost based on feature match
                if features.has_technical_terms and "code" in index_name.lower():
                    score *= 1.2
                if features.estimated_specificity > 0.7:
                    score *= 1.1

                index_scores[index_name] = min(1.0, score)
            else:
                index_scores[index_name] = 0.5

        # Select based on strategy
        if strategy == IndexSelectionStrategy.ENSEMBLE:
            # Use top indexes with weights
            sorted_indexes = sorted(index_scores.items(), key=lambda x: x[1], reverse=True)
            top_indexes = sorted_indexes[:3]
            total_score = sum(s for _, s in top_indexes)

            selected = [idx for idx, _ in top_indexes]
            weights = {idx: score / total_score for idx, score in top_indexes}
            confidence = top_indexes[0][1] if top_indexes else 0.5
            reasoning = f"Ensemble of top {len(selected)} indexes by learned scores"

        elif strategy == IndexSelectionStrategy.PERFORMANCE:
            # Select based purely on historical latency
            best_idx = min(index_scores.keys(), key=lambda x: self._get_avg_latency(x))
            selected = [best_idx]
            weights = {best_idx: 1.0}
            confidence = 0.7
            reasoning = f"Selected {best_idx} for best historical latency"

        else:  # LEARNED or RULE_BASED
            # Select highest scoring index
            best_idx = max(index_scores.items(), key=lambda x: x[1])
            selected = [best_idx[0]]
            weights = {best_idx[0]: 1.0}
            confidence = best_idx[1]
            reasoning = f"Selected {best_idx[0]} with score {best_idx[1]:.2f} for {features.query_type.value} query"

        decision = IndexRoutingDecision(
            decision_id=f"d_{uuid.uuid4().hex[:8]}",
            query_features=features,
            selected_indexes=selected,
            confidence=confidence,
            strategy_used=strategy,
            reasoning=reasoning,
            weights=weights
        )

        self._routing_history.append(decision)
        if len(self._routing_history) > 1000:
            self._routing_history = self._routing_history[-500:]

        return decision

    def _get_avg_latency(self, index_name: str) -> float:
        """Get average latency for an index from feedback history."""
        relevant = [f for f in self._feedback_history if f.index_name == index_name]
        if not relevant:
            return 100.0  # Default assumption
        return sum(f.latency_ms for f in relevant[-100:]) / len(relevant[-100:])

    def record_feedback(self, feedback: IndexPerformanceFeedback):
        """
        Record performance feedback for online learning.

        Updates index weights based on observed performance.
        """
        self._feedback_history.append(feedback)

        # Keep limited history
        if len(self._feedback_history) > self.config.feedback_window_size:
            self._feedback_history = self._feedback_history[-self.config.feedback_window_size:]

        # Online weight update
        if not self.config.enable_online_learning:
            return

        index_name = feedback.index_name
        query_type = feedback.query_features.query_type

        if index_name not in self._index_weights:
            self.register_index(index_name)

        # Calculate reward signal
        reward = 0.0

        # Reward for relevance
        reward += feedback.avg_relevance_score * 0.5

        # Reward for user clicks (strong signal)
        if feedback.user_clicked_result:
            position_bonus = 1.0 / (1.0 + (feedback.click_position or 0) * 0.1)
            reward += position_bonus * 0.3

        # Penalty for high latency
        latency_penalty = min(0.2, feedback.latency_ms / 1000.0)
        reward -= latency_penalty

        # Reward for returning results
        if feedback.results_count > 0:
            reward += 0.1

        reward = max(0.0, min(1.0, reward))

        # Update weight with learning rate
        current_weight = self._index_weights[index_name].get(query_type, 0.5)
        new_weight = current_weight + self.config.learning_rate * (reward - current_weight)
        self._index_weights[index_name][query_type] = max(0.1, min(1.0, new_weight))

        logger.debug(f"Updated weight for {index_name}/{query_type.value}: {current_weight:.3f} -> {new_weight:.3f}")

    def get_routing_stats(self) -> Dict[str, Any]:
        """Get routing statistics."""
        if not self._routing_history:
            return {"total_routings": 0}

        by_type = {}
        for decision in self._routing_history:
            qt = decision.query_features.query_type.value
            if qt not in by_type:
                by_type[qt] = 0
            by_type[qt] += 1

        by_index = {}
        for decision in self._routing_history:
            for idx in decision.selected_indexes:
                if idx not in by_index:
                    by_index[idx] = 0
                by_index[idx] += 1

        return {
            "total_routings": len(self._routing_history),
            "by_query_type": by_type,
            "by_index": by_index,
            "avg_confidence": sum(d.confidence for d in self._routing_history) / len(self._routing_history),
            "feedback_samples": len(self._feedback_history),
            "learned_weights": {
                idx: {qt.value: w for qt, w in weights.items()}
                for idx, weights in self._index_weights.items()
            }
        }


class AdaptiveIndexSelector:
    """
    Selects the best index for each query using adaptive strategies.

    Combines multiple signals:
    - Query characteristics
    - Historical performance
    - Current load
    - Index health
    """

    def __init__(
        self,
        multi_index_manager: 'MultiIndexManager',
        router: Optional[LearnedIndexRouter] = None
    ):
        self.manager = multi_index_manager
        self.router = router or LearnedIndexRouter()
        self._selection_history: List[Dict[str, Any]] = []

    async def select_index(
        self,
        query: str,
        embedding: Optional[List[float]] = None,
        prefer_low_latency: bool = False,
        prefer_high_recall: bool = False
    ) -> Tuple[str, float]:
        """
        Select the best index for a query.

        Args:
            query: Search query
            embedding: Query embedding
            prefer_low_latency: Prioritize speed over recall
            prefer_high_recall: Prioritize recall over speed

        Returns:
            Tuple of (index_name, confidence)
        """
        available = list(self.manager.indexes.keys())

        if not available:
            return ("default", 0.5)

        # Get routing decision
        strategy = IndexSelectionStrategy.LEARNED
        if prefer_low_latency:
            strategy = IndexSelectionStrategy.PERFORMANCE
        elif prefer_high_recall:
            strategy = IndexSelectionStrategy.ENSEMBLE

        decision = self.router.route_query(
            query=query,
            embedding=embedding,
            available_indexes=available,
            strategy=strategy
        )

        # Consider index health
        best_index = decision.selected_indexes[0]
        best_confidence = decision.confidence

        # Check if selected index has poor recent performance
        stats = self.manager.get_performance_stats(best_index)
        if stats and stats.p95_query_time_ms > 200:  # High latency threshold
            # Try to find a faster alternative
            for idx in decision.selected_indexes[1:]:
                alt_stats = self.manager.get_performance_stats(idx)
                if alt_stats and alt_stats.p95_query_time_ms < stats.p95_query_time_ms:
                    logger.info(f"Switching from {best_index} to {idx} due to latency")
                    best_index = idx
                    best_confidence *= 0.9  # Slight confidence reduction
                    break

        self._selection_history.append({
            "query": query[:50],
            "selected": best_index,
            "confidence": best_confidence,
            "strategy": strategy.value,
            "timestamp": datetime.now().isoformat()
        })

        if len(self._selection_history) > 500:
            self._selection_history = self._selection_history[-250:]

        return (best_index, best_confidence)

    async def search_with_adaptive_selection(
        self,
        query: str,
        embedding: List[float],
        limit: int = 10,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Search using adaptively selected index.

        Automatically selects the best index and records feedback.
        """
        import time

        # Select index
        index_name, confidence = await self.select_index(query, embedding)

        # Perform search
        start_time = time.time()

        if index_name in self.manager.indexes:
            results = await self.manager.search_index(
                index_name=index_name,
                query_embedding=embedding,
                limit=limit,
                **kwargs
            )
        else:
            # Fallback to default search
            results = []

        latency_ms = (time.time() - start_time) * 1000

        # Record feedback for learning
        features = self.router.classifier.classify(query, embedding)
        avg_score = sum(r.get("score", 0) for r in results) / max(1, len(results))

        feedback = IndexPerformanceFeedback(
            query_features=features,
            index_name=index_name,
            latency_ms=latency_ms,
            results_count=len(results),
            avg_relevance_score=avg_score
        )
        self.router.record_feedback(feedback)

        return results

    def record_click_feedback(
        self,
        query: str,
        embedding: Optional[List[float]],
        index_name: str,
        click_position: int
    ):
        """Record user click feedback for learning."""
        features = self.router.classifier.classify(query, embedding)

        feedback = IndexPerformanceFeedback(
            query_features=features,
            index_name=index_name,
            latency_ms=0,  # Not relevant for click feedback
            results_count=1,
            avg_relevance_score=1.0,  # Click implies relevance
            user_clicked_result=True,
            click_position=click_position
        )
        self.router.record_feedback(feedback)

    def get_selection_stats(self) -> Dict[str, Any]:
        """Get adaptive selection statistics."""
        return {
            "total_selections": len(self._selection_history),
            "routing_stats": self.router.get_routing_stats(),
            "recent_selections": self._selection_history[-10:]
        }


class CuttingEdgeVectorStore:
    """
    Cutting Edge vector store with learned index structures.

    Combines:
    - PgVectorStore for storage
    - MultiIndexManager for multi-index support
    - LearnedIndexRouter for adaptive query routing
    - AdaptiveIndexSelector for intelligent index selection
    """

    def __init__(
        self,
        base_store: PgVectorStore,
        multi_index_manager: MultiIndexManager,
        config: Optional[CuttingEdgeVectorStoreConfig] = None
    ):
        self.base_store = base_store
        self.manager = multi_index_manager
        self.config = config or CuttingEdgeVectorStoreConfig()

        self.router = LearnedIndexRouter(self.config.learned_config)
        self.selector = AdaptiveIndexSelector(multi_index_manager, self.router)

        self._initialized = False

    async def initialize(self):
        """Initialize the cutting edge vector store."""
        if self._initialized:
            return

        # Ensure base components are initialized
        if not self.base_store._initialized:
            await self.base_store.initialize()

        if not self.manager._initialized:
            await self.manager.initialize()

        # Register existing indexes with router
        for index_name in self.manager.indexes:
            config = self.manager.indexes[index_name]
            index_type = f"{config.index_type}_{config.distance_metric}"
            self.router.register_index(index_name, index_type)

        self._initialized = True
        logger.info("CuttingEdgeVectorStore initialized with learned routing")

    async def search(
        self,
        query_embedding: List[float],
        query_text: Optional[str] = None,
        limit: int = 10,
        use_adaptive_selection: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Search with optional adaptive index selection.

        Args:
            query_embedding: Query vector
            query_text: Original query text (for classification)
            limit: Maximum results
            use_adaptive_selection: Whether to use learned routing
            **kwargs: Additional search parameters

        Returns:
            List of search results
        """
        if not self._initialized:
            await self.initialize()

        if use_adaptive_selection and self.config.enable_adaptive_selection and query_text:
            # Use adaptive selection
            return await self.selector.search_with_adaptive_selection(
                query=query_text,
                embedding=query_embedding,
                limit=limit,
                **kwargs
            )
        else:
            # Use base store directly
            return await self.base_store.search(
                query_embedding=query_embedding,
                limit=limit,
                **kwargs
            )

    async def search_with_routing(
        self,
        query_text: str,
        query_embedding: List[float],
        limit: int = 10,
        strategy: IndexSelectionStrategy = IndexSelectionStrategy.LEARNED
    ) -> Tuple[List[Dict[str, Any]], IndexRoutingDecision]:
        """
        Search with explicit routing decision returned.

        Returns both results and the routing decision for transparency.
        """
        if not self._initialized:
            await self.initialize()

        # Get routing decision
        available_indexes = list(self.manager.indexes.keys())
        decision = self.router.route_query(
            query=query_text,
            embedding=query_embedding,
            available_indexes=available_indexes,
            strategy=strategy
        )

        # Search selected indexes
        if strategy == IndexSelectionStrategy.ENSEMBLE and len(decision.selected_indexes) > 1:
            # Ensemble: search multiple and merge
            results = await self.manager.search_multiple_indexes(
                index_names=decision.selected_indexes,
                query_embeddings={idx: query_embedding for idx in decision.selected_indexes},
                limit=limit,
                merge_strategy="score_based"
            )
        else:
            # Single index search
            index_name = decision.selected_indexes[0]
            if index_name in self.manager.indexes:
                results = await self.manager.search_index(
                    index_name=index_name,
                    query_embedding=query_embedding,
                    limit=limit
                )
            else:
                results = await self.base_store.search(
                    query_embedding=query_embedding,
                    limit=limit
                )

        return results, decision

    def record_click(
        self,
        query_text: str,
        query_embedding: Optional[List[float]],
        clicked_result_index: int
    ):
        """Record a user click for learning."""
        # Get the most recent routing decision for this query
        for decision in reversed(self.router._routing_history[-50:]):
            if decision.query_features.query_text == query_text:
                for idx in decision.selected_indexes:
                    self.selector.record_click_feedback(
                        query=query_text,
                        embedding=query_embedding,
                        index_name=idx,
                        click_position=clicked_result_index
                    )
                break

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get comprehensive cutting edge statistics."""
        return {
            "initialized": self._initialized,
            "config": {
                "learned_index_enabled": self.config.enable_learned_index,
                "adaptive_selection_enabled": self.config.enable_adaptive_selection
            },
            "routing": self.router.get_routing_stats(),
            "selection": self.selector.get_selection_stats(),
            "indexes_registered": len(self.manager.indexes)
        }


# -----------------------------------------------------------------------------
# Factory Functions for Cutting Edge Features
# -----------------------------------------------------------------------------

_learned_router: Optional[LearnedIndexRouter] = None
_cutting_edge_store: Optional[CuttingEdgeVectorStore] = None


async def get_learned_index_router() -> LearnedIndexRouter:
    """Get or create the global learned index router."""
    global _learned_router
    if _learned_router is None:
        _learned_router = LearnedIndexRouter()
    return _learned_router


async def get_cutting_edge_vector_store() -> CuttingEdgeVectorStore:
    """Get or create the global cutting edge vector store."""
    global _cutting_edge_store
    if _cutting_edge_store is None:
        base_store = await get_vector_store()
        manager = await get_multi_index_manager()
        _cutting_edge_store = CuttingEdgeVectorStore(base_store, manager)
        await _cutting_edge_store.initialize()
        logger.info("CuttingEdgeVectorStore created with learned routing")
    return _cutting_edge_store


def reset_cutting_edge_vector_store():
    """Reset cutting edge vector store (for testing)."""
    global _cutting_edge_store, _learned_router
    _cutting_edge_store = None
    _learned_router = None


# Singleton instances for multi-index support
_multi_index_manager: Optional[MultiIndexManager] = None
_index_optimizer: Optional[IndexOptimizer] = None


async def get_multi_index_manager() -> MultiIndexManager:
    """Get or create the global multi-index manager"""
    global _multi_index_manager
    if _multi_index_manager is None:
        _multi_index_manager = MultiIndexManager()
        await _multi_index_manager.initialize()
    return _multi_index_manager


async def get_index_optimizer() -> IndexOptimizer:
    """Get or create the global index optimizer"""
    global _index_optimizer
    if _index_optimizer is None:
        manager = await get_multi_index_manager()
        _index_optimizer = IndexOptimizer(manager)
    return _index_optimizer


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

_vector_store = None


async def get_vector_store(
    config: Optional[VectorStoreConfig] = None
) -> PgVectorStore:
    """
    Get or create vector store instance.

    PostgreSQL with pgvector is required - no in-memory fallback.
    This ensures data persistence and production-ready vector search.

    Raises:
        RuntimeError: If PostgreSQL is not available
    """
    global _vector_store

    if _vector_store is None:
        if not SHARED_DB_AVAILABLE:
            raise RuntimeError(
                "PostgreSQL is required for RAG service. "
                "Please ensure PostgreSQL is running and the shared database module is available."
            )

        _vector_store = PgVectorStore(config)
        await _vector_store.initialize()
        logger.info("✅ Vector store initialized with PostgreSQL + pgvector")

    return _vector_store


async def close_vector_store():
    """Close the vector store connection"""
    global _vector_store
    if _vector_store:
        await _vector_store.close()
        _vector_store = None

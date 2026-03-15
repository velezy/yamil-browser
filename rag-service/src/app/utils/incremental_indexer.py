"""
T.A.L.O.S. Incremental Indexer
Real-time chunk indexing as documents are processed

Features:
- Stream chunks directly to vector store as they're created
- Batch accumulation for efficient embedding
- Progress tracking for large documents
- Rollback support on failure
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# DATA MODELS
# =============================================================================

class IndexingStatus(str, Enum):
    """Incremental indexing status"""
    IDLE = "idle"
    RECEIVING = "receiving"
    BATCHING = "batching"
    EMBEDDING = "embedding"
    UPSERTING = "upserting"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"


@dataclass
class IndexingProgress:
    """Progress tracking for incremental indexing"""
    document_id: int
    status: IndexingStatus
    chunks_received: int = 0
    chunks_embedded: int = 0
    chunks_indexed: int = 0
    current_batch: int = 0
    total_batches: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

    @property
    def progress_percent(self) -> float:
        if self.chunks_received == 0:
            return 0.0
        return (self.chunks_indexed / self.chunks_received) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "document_id": self.document_id,
            "status": self.status.value,
            "chunks_received": self.chunks_received,
            "chunks_embedded": self.chunks_embedded,
            "chunks_indexed": self.chunks_indexed,
            "progress_percent": self.progress_percent,
            "current_batch": self.current_batch,
            "total_batches": self.total_batches,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error
        }


@dataclass
class ChunkBatch:
    """Batch of chunks ready for embedding"""
    chunks: List[Dict[str, Any]] = field(default_factory=list)
    batch_number: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)


# =============================================================================
# INCREMENTAL INDEXER
# =============================================================================

class IncrementalIndexer:
    """
    Indexes document chunks incrementally as they stream in.

    Benefits:
    - Faster perceived indexing (results available before full processing)
    - Memory efficient (doesn't hold all chunks in memory)
    - Progressive updates to search index
    - Better user feedback during processing
    """

    def __init__(
        self,
        vector_store,
        embedder,
        batch_size: int = 32,
        flush_interval: float = 2.0
    ):
        """
        Args:
            vector_store: Vector store instance (PgVectorStore or InMemoryVectorStore)
            embedder: SentenceTransformer or compatible embedder
            batch_size: Number of chunks to batch before embedding
            flush_interval: Max seconds to wait before flushing partial batch
        """
        self.vector_store = vector_store
        self.embedder = embedder
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        # Active indexing sessions
        self._sessions: Dict[int, IndexingProgress] = {}
        self._buffers: Dict[int, List[Dict[str, Any]]] = {}
        self._indexed_ids: Dict[int, List[int]] = {}  # For rollback
        self._locks: Dict[int, asyncio.Lock] = {}

    async def start_indexing(self, document_id: int) -> IndexingProgress:
        """Start an incremental indexing session for a document"""
        if document_id in self._sessions:
            # Clean up existing session
            await self._cleanup_session(document_id)

        self._sessions[document_id] = IndexingProgress(
            document_id=document_id,
            status=IndexingStatus.IDLE,
            started_at=datetime.utcnow()
        )
        self._buffers[document_id] = []
        self._indexed_ids[document_id] = []
        self._locks[document_id] = asyncio.Lock()

        logger.info(f"Started incremental indexing for document {document_id}")
        return self._sessions[document_id]

    async def add_chunk(
        self,
        document_id: int,
        chunk: Dict[str, Any]
    ) -> bool:
        """
        Add a chunk to be indexed.

        Args:
            document_id: Document ID
            chunk: Chunk dict with 'content' and optional 'metadata'

        Returns:
            True if chunk was accepted
        """
        if document_id not in self._sessions:
            logger.warning(f"No active session for document {document_id}")
            return False

        progress = self._sessions[document_id]
        progress.status = IndexingStatus.RECEIVING
        progress.chunks_received += 1

        # Add to buffer
        self._buffers[document_id].append(chunk)

        # Check if we should flush
        if len(self._buffers[document_id]) >= self.batch_size:
            await self._flush_buffer(document_id)

        return True

    async def add_chunks_stream(
        self,
        document_id: int,
        chunks: AsyncIterator[Dict[str, Any]],
        on_progress: Optional[Callable[[IndexingProgress], None]] = None
    ) -> IndexingProgress:
        """
        Stream chunks for indexing.

        Args:
            document_id: Document ID
            chunks: Async iterator of chunk dicts
            on_progress: Optional progress callback

        Returns:
            Final indexing progress
        """
        progress = await self.start_indexing(document_id)

        try:
            async for chunk in chunks:
                await self.add_chunk(document_id, chunk)

                if on_progress:
                    on_progress(self._sessions[document_id])

            # Flush remaining chunks
            await self.finish_indexing(document_id)

            return self._sessions[document_id]

        except Exception as e:
            logger.error(f"Stream indexing failed for document {document_id}: {e}")
            await self.rollback(document_id)
            raise

    async def _flush_buffer(self, document_id: int):
        """Flush the buffer and index accumulated chunks"""
        if document_id not in self._buffers:
            return

        async with self._locks[document_id]:
            chunks = self._buffers[document_id]
            if not chunks:
                return

            progress = self._sessions[document_id]
            progress.status = IndexingStatus.BATCHING
            progress.current_batch += 1

            try:
                # Extract content for embedding
                contents = [c.get("content", "") for c in chunks]
                valid_indices = [i for i, c in enumerate(contents) if c.strip()]

                if not valid_indices:
                    self._buffers[document_id] = []
                    return

                valid_contents = [contents[i] for i in valid_indices]
                valid_chunks = [chunks[i] for i in valid_indices]

                # Generate embeddings
                progress.status = IndexingStatus.EMBEDDING
                embeddings = self.embedder.encode(
                    valid_contents,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                    batch_size=self.batch_size
                ).tolist()

                progress.chunks_embedded += len(embeddings)

                # Upsert to vector store
                progress.status = IndexingStatus.UPSERTING
                count = await self.vector_store.upsert(
                    document_id=document_id,
                    chunks=valid_chunks,
                    embeddings=embeddings
                )

                progress.chunks_indexed += count

                # Track indexed IDs for potential rollback
                # (Assuming vector store returns or we can track chunk IDs)

                logger.debug(
                    f"Flushed batch {progress.current_batch} for doc {document_id}: "
                    f"{count} chunks indexed"
                )

            except Exception as e:
                progress.status = IndexingStatus.FAILED
                progress.error = str(e)
                raise

            finally:
                # Clear buffer
                self._buffers[document_id] = []

    async def finish_indexing(self, document_id: int) -> IndexingProgress:
        """Finish indexing and flush any remaining chunks"""
        if document_id not in self._sessions:
            raise ValueError(f"No active session for document {document_id}")

        # Flush remaining buffer
        if self._buffers.get(document_id):
            await self._flush_buffer(document_id)

        progress = self._sessions[document_id]
        progress.status = IndexingStatus.COMPLETED
        progress.completed_at = datetime.utcnow()

        duration = (progress.completed_at - progress.started_at).total_seconds()

        logger.info(
            f"Completed incremental indexing for document {document_id}: "
            f"{progress.chunks_indexed} chunks in {duration:.2f}s"
        )

        return progress

    async def rollback(self, document_id: int) -> bool:
        """
        Rollback indexed chunks for a failed document.

        This removes all chunks indexed during this session.
        """
        if document_id not in self._sessions:
            return False

        progress = self._sessions[document_id]
        progress.status = IndexingStatus.ROLLING_BACK

        try:
            # Delete all chunks for this document
            count = await self.vector_store.delete_document(document_id)
            logger.info(f"Rolled back {count} chunks for document {document_id}")

            progress.status = IndexingStatus.FAILED
            return True

        except Exception as e:
            logger.error(f"Rollback failed for document {document_id}: {e}")
            return False

        finally:
            await self._cleanup_session(document_id)

    async def _cleanup_session(self, document_id: int):
        """Clean up session data"""
        self._sessions.pop(document_id, None)
        self._buffers.pop(document_id, None)
        self._indexed_ids.pop(document_id, None)
        self._locks.pop(document_id, None)

    def get_progress(self, document_id: int) -> Optional[IndexingProgress]:
        """Get current indexing progress for a document"""
        return self._sessions.get(document_id)

    def get_all_active_sessions(self) -> List[IndexingProgress]:
        """Get all active indexing sessions"""
        return list(self._sessions.values())


# =============================================================================
# STREAMING INDEX ENDPOINT HELPER
# =============================================================================

async def index_chunks_incrementally(
    vector_store,
    embedder,
    document_id: int,
    chunks: List[Dict[str, Any]],
    batch_size: int = 32,
    on_progress: Optional[Callable[[IndexingProgress], None]] = None
) -> Dict[str, Any]:
    """
    Index chunks incrementally with progress tracking.

    This is a convenience function for use in endpoints.

    Args:
        vector_store: Vector store instance
        embedder: Embedding model
        document_id: Document ID
        chunks: List of chunk dicts
        batch_size: Batch size for embedding
        on_progress: Progress callback

    Returns:
        Indexing result with metrics
    """
    indexer = IncrementalIndexer(
        vector_store=vector_store,
        embedder=embedder,
        batch_size=batch_size
    )

    start_time = time.time()

    await indexer.start_indexing(document_id)

    for i, chunk in enumerate(chunks):
        await indexer.add_chunk(document_id, chunk)

        if on_progress and (i + 1) % batch_size == 0:
            on_progress(indexer.get_progress(document_id))

    progress = await indexer.finish_indexing(document_id)

    total_time = (time.time() - start_time) * 1000

    return {
        "success": True,
        "document_id": document_id,
        "chunks_indexed": progress.chunks_indexed,
        "batches_processed": progress.current_batch,
        "total_time_ms": total_time,
        "avg_ms_per_chunk": total_time / max(progress.chunks_indexed, 1),
        "status": progress.status.value
    }


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_GLOBAL_INDEXER: Optional[IncrementalIndexer] = None


def get_incremental_indexer(
    vector_store,
    embedder,
    batch_size: int = 32
) -> IncrementalIndexer:
    """Get or create global incremental indexer"""
    global _GLOBAL_INDEXER

    if _GLOBAL_INDEXER is None:
        _GLOBAL_INDEXER = IncrementalIndexer(
            vector_store=vector_store,
            embedder=embedder,
            batch_size=batch_size
        )

    return _GLOBAL_INDEXER

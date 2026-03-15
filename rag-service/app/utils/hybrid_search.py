"""
Hybrid Search: pgvector + OpenSearch

Combines semantic search (pgvector) with BM25 full-text search (OpenSearch)
using Reciprocal Rank Fusion (RRF) for optimal retrieval.

Features:
- Configurable search backend (pgvector, hybrid, opensearch)
- RRF fusion of semantic + lexical results
- Score normalization across backends
- Graceful fallback when OpenSearch unavailable
"""

import os
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

SEARCH_BACKEND = os.getenv("SEARCH_BACKEND", "pgvector")


@dataclass
class HybridResult:
    """Result from hybrid search"""
    chunk_id: int
    document_id: int
    content: str
    semantic_score: float
    bm25_score: float
    fused_score: float
    metadata: Dict[str, Any]
    highlights: List[str]


class HybridSearchEngine:
    """
    Combines pgvector semantic search with OpenSearch BM25.

    Uses Reciprocal Rank Fusion (RRF) to merge results:
    RRF(d) = sum(1 / (k + rank_i(d))) for each ranker i

    Where k=60 (standard constant).
    """

    def __init__(
        self,
        vector_store=None,
        opensearch_store=None,
        k: int = 60,
        semantic_weight: float = 0.6,
        bm25_weight: float = 0.4,
    ):
        """
        Initialize hybrid search.

        Args:
            vector_store: PgVectorStore instance
            opensearch_store: OpenSearchStore instance
            k: RRF constant (default 60)
            semantic_weight: Weight for semantic results in weighted fusion
            bm25_weight: Weight for BM25 results in weighted fusion
        """
        self.vector_store = vector_store
        self.opensearch_store = opensearch_store
        self.k = k
        self.semantic_weight = semantic_weight
        self.bm25_weight = bm25_weight

    async def search(
        self,
        query: str,
        query_embedding: Optional[List[float]] = None,
        top_k: int = 10,
        tenant_id: Optional[str] = None,
        document_ids: Optional[List[int]] = None,
    ) -> List[HybridResult]:
        """
        Execute hybrid search combining semantic + BM25 results.

        Args:
            query: Search query text
            query_embedding: Pre-computed query embedding (for semantic search)
            top_k: Number of results to return
            tenant_id: Filter by tenant
            document_ids: Filter by document IDs

        Returns:
            List of HybridResult sorted by fused score
        """
        backend = SEARCH_BACKEND.lower()

        if backend == "opensearch":
            return await self._opensearch_only(query, top_k, tenant_id, document_ids)
        elif backend == "hybrid":
            return await self._hybrid_rrf(query, query_embedding, top_k, tenant_id, document_ids)
        else:  # pgvector
            return await self._pgvector_only(query_embedding, top_k, document_ids)

    async def _pgvector_only(
        self,
        query_embedding: Optional[List[float]],
        top_k: int,
        document_ids: Optional[List[int]],
    ) -> List[HybridResult]:
        """Semantic search only via pgvector"""
        if not self.vector_store or query_embedding is None:
            return []

        results = await self.vector_store.search(
            embedding=query_embedding,
            limit=top_k,
            document_ids=document_ids,
        )

        return [
            HybridResult(
                chunk_id=r.get("chunk_id", 0),
                document_id=r.get("document_id", 0),
                content=r.get("content", ""),
                semantic_score=r.get("score", 0.0),
                bm25_score=0.0,
                fused_score=r.get("score", 0.0),
                metadata=r.get("metadata", {}),
                highlights=[],
            )
            for r in results
        ]

    async def _opensearch_only(
        self,
        query: str,
        top_k: int,
        tenant_id: Optional[str],
        document_ids: Optional[List[int]],
    ) -> List[HybridResult]:
        """BM25 search only via OpenSearch"""
        if not self.opensearch_store:
            return []

        results = self.opensearch_store.search(
            query=query,
            top_k=top_k,
            tenant_id=tenant_id,
            document_ids=document_ids,
        )

        return [
            HybridResult(
                chunk_id=r.chunk_id,
                document_id=r.document_id,
                content=r.content,
                semantic_score=0.0,
                bm25_score=r.score,
                fused_score=r.score,
                metadata=r.metadata,
                highlights=r.highlights,
            )
            for r in results
        ]

    async def _hybrid_rrf(
        self,
        query: str,
        query_embedding: Optional[List[float]],
        top_k: int,
        tenant_id: Optional[str],
        document_ids: Optional[List[int]],
    ) -> List[HybridResult]:
        """
        Hybrid search with RRF fusion.

        Retrieves from both pgvector and OpenSearch, then fuses using RRF.
        """
        # Fetch wider result set for fusion
        fetch_k = top_k * 3

        # Semantic results
        semantic_results = {}
        if self.vector_store and query_embedding is not None:
            try:
                raw = await self.vector_store.search(
                    embedding=query_embedding,
                    limit=fetch_k,
                    document_ids=document_ids,
                )
                for rank, r in enumerate(raw):
                    key = (r.get("document_id", 0), r.get("chunk_id", 0))
                    semantic_results[key] = {
                        "rank": rank,
                        "score": r.get("score", 0.0),
                        "content": r.get("content", ""),
                        "metadata": r.get("metadata", {}),
                    }
            except Exception as e:
                logger.warning(f"Semantic search failed: {e}")

        # BM25 results
        bm25_results = {}
        if self.opensearch_store:
            try:
                raw = self.opensearch_store.search(
                    query=query,
                    top_k=fetch_k,
                    tenant_id=tenant_id,
                    document_ids=document_ids,
                )
                for rank, r in enumerate(raw):
                    key = (r.document_id, r.chunk_id)
                    bm25_results[key] = {
                        "rank": rank,
                        "score": r.score,
                        "content": r.content,
                        "metadata": r.metadata,
                        "highlights": r.highlights,
                    }
            except Exception as e:
                logger.warning(f"BM25 search failed: {e}")

        # RRF Fusion
        all_keys = set(semantic_results.keys()) | set(bm25_results.keys())
        fused = []

        for key in all_keys:
            doc_id, chunk_id = key

            sem = semantic_results.get(key)
            bm25 = bm25_results.get(key)

            # RRF score: sum of 1/(k + rank) for each ranker
            rrf_score = 0.0
            if sem:
                rrf_score += self.semantic_weight / (self.k + sem["rank"])
            if bm25:
                rrf_score += self.bm25_weight / (self.k + bm25["rank"])

            fused.append(HybridResult(
                chunk_id=chunk_id,
                document_id=doc_id,
                content=(sem or bm25 or {}).get("content", ""),
                semantic_score=sem["score"] if sem else 0.0,
                bm25_score=bm25["score"] if bm25 else 0.0,
                fused_score=rrf_score,
                metadata=(sem or bm25 or {}).get("metadata", {}),
                highlights=(bm25 or {}).get("highlights", []),
            ))

        # Sort by fused score descending
        fused.sort(key=lambda x: x.fused_score, reverse=True)

        logger.debug(
            f"Hybrid search: {len(semantic_results)} semantic + "
            f"{len(bm25_results)} BM25 → {len(fused)} fused results"
        )

        return fused[:top_k]

    def get_info(self) -> Dict[str, Any]:
        """Get search engine info"""
        return {
            "backend": SEARCH_BACKEND,
            "vector_store": self.vector_store is not None,
            "opensearch": self.opensearch_store is not None,
            "k": self.k,
            "weights": {
                "semantic": self.semantic_weight,
                "bm25": self.bm25_weight,
            },
        }


# Singleton
_hybrid_engine: Optional[HybridSearchEngine] = None


def get_hybrid_search(vector_store=None, opensearch_store=None) -> HybridSearchEngine:
    """Get or create singleton hybrid search engine"""
    global _hybrid_engine
    if _hybrid_engine is None:
        _hybrid_engine = HybridSearchEngine(
            vector_store=vector_store,
            opensearch_store=opensearch_store,
        )
    return _hybrid_engine

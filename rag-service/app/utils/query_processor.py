"""
T.A.L.O.S. RAG Query Processor
End-to-end query processing with hybrid search, reranking, and context building

Integrates:
- Document processing
- Embedding generation
- Hybrid search (vector + full-text)
- Result reranking
- Context building for LLM
"""

import os
import logging
import time
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from app.utils.context_compressor import get_context_compressor

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RetrievedChunk:
    """A chunk retrieved from search"""
    chunk_id: int
    document_id: int
    content: str
    score: float
    semantic_score: float
    keyword_score: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    rerank_score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "document_id": self.document_id,
            "content": self.content,
            "score": self.score,
            "semantic_score": self.semantic_score,
            "keyword_score": self.keyword_score,
            "metadata": self.metadata,
            "rerank_score": self.rerank_score
        }


@dataclass
class QueryResult:
    """Complete result from query processing"""
    query: str
    chunks: List[RetrievedChunk]
    context: str
    total_chunks: int
    processing_time_ms: float
    search_method: str
    semantic_weight: float
    keyword_weight: float
    success: bool
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "chunks": [c.to_dict() for c in self.chunks],
            "context": self.context,
            "total_chunks": self.total_chunks,
            "processing_time_ms": self.processing_time_ms,
            "search_method": self.search_method,
            "weights": {
                "semantic": self.semantic_weight,
                "keyword": self.keyword_weight
            },
            "success": self.success,
            "error": self.error
        }


@dataclass
class ProcessorConfig:
    """Configuration for RAG Query Processor"""
    # Search settings
    top_k: int = 10
    min_score: float = 0.3  # Lowered to allow filename boosting to surface relevant docs
    semantic_weight: float = 0.7
    keyword_weight: float = 0.3

    # Reranking
    enable_reranking: bool = True
    rerank_top_n: int = 50  # Increased to allow filename boosting to surface relevant docs

    # Context building
    max_context_length: int = 4000
    context_separator: str = "\n\n---\n\n"
    include_metadata: bool = True

    # Context compression
    enable_compression: bool = True

    def __post_init__(self):
        # Environment overrides
        self.semantic_weight = float(os.getenv("RAG_SEMANTIC_WEIGHT", str(self.semantic_weight)))
        self.keyword_weight = float(os.getenv("RAG_KEYWORD_WEIGHT", str(self.keyword_weight)))
        self.max_context_length = int(os.getenv("RAG_MAX_CONTEXT", str(self.max_context_length)))


# =============================================================================
# RAG QUERY PROCESSOR
# =============================================================================

class RAGQueryProcessor:
    """
    End-to-end RAG query processor.

    Pipeline:
    1. Generate query embedding
    2. Retrieve relevant chunks (hybrid search)
    3. Rerank by relevance
    4. Build context for LLM
    5. Return structured result
    """

    def __init__(
        self,
        vector_store,
        embedder,
        config: Optional[ProcessorConfig] = None
    ):
        self.vector_store = vector_store
        self.embedder = embedder
        self.config = config or ProcessorConfig()

        logger.info("RAG Query Processor initialized")

    async def process_query(
        self,
        query: str,
        document_ids: Optional[List[int]] = None,
        top_k: Optional[int] = None,
        min_score: Optional[float] = None,
        metadata_filter: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """
        Process a query through the full RAG pipeline.

        Args:
            query: User's query
            document_ids: Optional filter by document IDs
            top_k: Override number of results
            min_score: Override minimum score threshold
            metadata_filter: Optional metadata filter

        Returns:
            QueryResult with retrieved chunks and built context
        """
        start_time = time.time()
        top_k = top_k or self.config.top_k
        min_score = min_score or self.config.min_score

        try:
            # Step 1: Generate query embedding
            query_embedding = await self._generate_embedding(query)

            # Step 2: Hybrid search (vector + keyword)
            raw_results = await self._hybrid_search(
                query=query,
                query_embedding=query_embedding,
                limit=self.config.rerank_top_n if self.config.enable_reranking else top_k,
                threshold=min_score * 0.5,  # Much lower threshold to allow filename boosting to work
                document_ids=document_ids,
                metadata_filter=metadata_filter
            )

            # Step 3: Convert to RetrievedChunk objects
            chunks = self._create_chunks(raw_results)

            # Step 4: Rerank if enabled
            if self.config.enable_reranking and chunks:
                chunks = await self._rerank_chunks(query, chunks)

            # Step 5: Filter by score and limit
            chunks = [c for c in chunks if c.score >= min_score][:top_k]

            # Step 6: Build context
            context = self._build_context(chunks)

            # Step 7: Compress context if enabled
            if self.config.enable_compression:
                compressor = get_context_compressor()
                if compressor.is_available:
                    result = compressor.compress(context, query=query)
                    context = result.compressed_text
                    logger.debug(f"Context compressed: {result.saving_percent:.1f}% saved")

            processing_time = (time.time() - start_time) * 1000

            logger.info(
                f"Query processed: {len(chunks)} chunks, "
                f"{len(context)} chars context, {processing_time:.1f}ms"
            )

            return QueryResult(
                query=query,
                chunks=chunks,
                context=context,
                total_chunks=len(chunks),
                processing_time_ms=processing_time,
                search_method="hybrid",
                semantic_weight=self.config.semantic_weight,
                keyword_weight=self.config.keyword_weight,
                success=True
            )

        except Exception as e:
            logger.error(f"Query processing error: {e}")
            return QueryResult(
                query=query,
                chunks=[],
                context="",
                total_chunks=0,
                processing_time_ms=(time.time() - start_time) * 1000,
                search_method="hybrid",
                semantic_weight=self.config.semantic_weight,
                keyword_weight=self.config.keyword_weight,
                success=False,
                error=str(e)
            )

    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate normalized embedding for text"""
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        embedding = await loop.run_in_executor(
            None,
            lambda: self.embedder.encode(text, normalize_embeddings=True).tolist()
        )
        return embedding

    async def _hybrid_search(
        self,
        query: str,
        query_embedding: List[float],
        limit: int,
        threshold: float,
        document_ids: Optional[List[int]],
        metadata_filter: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining vector and keyword matching.

        Returns results with both semantic and keyword scores.
        """
        # Vector search
        vector_results = await self.vector_store.search(
            query_embedding=query_embedding,
            limit=limit,
            threshold=threshold,
            document_ids=document_ids,
            metadata_filter=metadata_filter
        )

        # Calculate keyword overlap scores
        query_words = set(query.lower().split())
        query_words = {w for w in query_words if len(w) > 2}  # Filter short words

        for result in vector_results:
            content_lower = result["content"].lower()
            content_words = set(content_lower.split())

            # Calculate keyword overlap
            overlap = len(query_words & content_words)
            keyword_score = overlap / max(len(query_words), 1)

            # Store scores
            result["semantic_score"] = result["score"]
            result["keyword_score"] = keyword_score

            # Calculate hybrid score
            result["score"] = (
                result["semantic_score"] * self.config.semantic_weight +
                keyword_score * self.config.keyword_weight
            )

        # Sort by hybrid score
        vector_results.sort(key=lambda x: x["score"], reverse=True)

        return vector_results

    def _create_chunks(self, raw_results: List[Dict[str, Any]]) -> List[RetrievedChunk]:
        """Convert raw search results to RetrievedChunk objects"""
        chunks = []
        for result in raw_results:
            chunk = RetrievedChunk(
                chunk_id=result["chunk_id"],
                document_id=result["document_id"],
                content=result["content"],
                score=result["score"],
                semantic_score=result.get("semantic_score", result["score"]),
                keyword_score=result.get("keyword_score", 0.0),
                metadata=result.get("metadata", {})
            )
            chunks.append(chunk)
        return chunks

    async def _rerank_chunks(
        self,
        query: str,
        chunks: List[RetrievedChunk]
    ) -> List[RetrievedChunk]:
        """
        Rerank chunks based on query relevance.

        Uses a simple but effective heuristic-based reranking:
        - Filename matches boost score (for document-specific queries)
        - Exact phrase matches boost score
        - Query term density in content
        - Position of query terms (earlier is better)
        """
        query_lower = query.lower()
        query_terms = [t for t in query_lower.split() if len(t) > 2]

        for chunk in chunks:
            content_lower = chunk.content.lower()
            rerank_score = chunk.score  # Start with hybrid score

            # Filename boosting: if query mentions document name, boost matching files
            source = chunk.metadata.get('source', '') or chunk.metadata.get('filename', '')
            if source:
                source_lower = source.lower().replace('_', ' ').replace('-', ' ').replace('.', ' ')
                filename_matches = sum(1 for term in query_terms if term in source_lower)
                if filename_matches > 0:
                    # Significant boost for filename matches (0.3 per term, max 0.6)
                    filename_boost = min(filename_matches * 0.3, 0.6)
                    rerank_score += filename_boost
                    logger.debug(f"Filename boost +{filename_boost:.2f} for '{source}'")

            # Exact phrase match bonus
            if query_lower in content_lower:
                rerank_score += 0.2

            # Query term density
            term_count = sum(1 for term in query_terms if term in content_lower)
            term_density = term_count / max(len(query_terms), 1)
            rerank_score += term_density * 0.1

            # Early occurrence bonus
            first_positions = []
            for term in query_terms:
                pos = content_lower.find(term)
                if pos >= 0:
                    first_positions.append(pos)

            if first_positions:
                avg_position = sum(first_positions) / len(first_positions)
                # Normalize: earlier position = higher bonus
                position_bonus = max(0, 1 - (avg_position / len(content_lower)))
                rerank_score += position_bonus * 0.05

            chunk.rerank_score = min(rerank_score, 1.0)  # Cap at 1.0

        # Sort by rerank score
        chunks.sort(key=lambda c: c.rerank_score or c.score, reverse=True)

        return chunks

    def _build_context(self, chunks: List[RetrievedChunk]) -> str:
        """
        Build context string from retrieved chunks.

        Formats chunks with optional metadata and respects max length.
        """
        if not chunks:
            return ""

        context_parts = []
        current_length = 0

        for i, chunk in enumerate(chunks):
            # Format chunk
            if self.config.include_metadata and chunk.metadata:
                # Include source info if available
                source = chunk.metadata.get("source", f"Document {chunk.document_id}")
                chunk_text = f"[Source: {source}]\n{chunk.content}"
            else:
                chunk_text = chunk.content

            # Check length
            separator_length = len(self.config.context_separator)
            if current_length + len(chunk_text) + separator_length > self.config.max_context_length:
                # Try to fit partial content
                remaining = self.config.max_context_length - current_length - separator_length
                if remaining > 200:  # Only include if meaningful
                    chunk_text = chunk_text[:remaining] + "..."
                    context_parts.append(chunk_text)
                break

            context_parts.append(chunk_text)
            current_length += len(chunk_text) + separator_length

        return self.config.context_separator.join(context_parts)

    async def get_context_for_llm(
        self,
        query: str,
        document_ids: Optional[List[int]] = None,
        max_chunks: int = 5
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Convenience method to get context and sources for LLM.

        Returns:
            Tuple of (context_string, list_of_sources)
        """
        result = await self.process_query(
            query=query,
            document_ids=document_ids,
            top_k=max_chunks
        )

        sources = [
            {
                "document_id": c.document_id,
                "chunk_id": c.chunk_id,
                "score": c.score,
                "preview": c.content[:100] + "..." if len(c.content) > 100 else c.content
            }
            for c in result.chunks
        ]

        return result.context, sources


# =============================================================================
# CUTTING EDGE: Neural-Symbolic Fusion & Learned Retrieval Routing
# =============================================================================

from datetime import datetime


class RetrievalMethod(str, Enum):
    """Available retrieval methods."""
    SEMANTIC = "semantic"  # Pure vector search
    LEXICAL = "lexical"  # Pure keyword/BM25
    HYBRID = "hybrid"  # Weighted combination
    NEURAL_SYMBOLIC = "neural_symbolic"  # Learned fusion
    ADAPTIVE = "adaptive"  # Query-dependent routing


class FusionStrategy(str, Enum):
    """Strategies for neural-symbolic fusion."""
    WEIGHTED_SUM = "weighted_sum"  # Fixed weight combination
    LEARNED_WEIGHTS = "learned_weights"  # Learned per-query weights
    RECIPROCAL_RANK = "reciprocal_rank"  # RRF fusion
    ATTENTION = "attention"  # Attention-based fusion
    CASCADE = "cascade"  # Sequential filtering


@dataclass
class RetrievalSignal:
    """A retrieval signal for learning."""
    signal_id: str
    query: str
    method_used: RetrievalMethod
    semantic_weight: float
    lexical_weight: float
    result_count: int
    avg_score: float
    user_clicked: bool = False
    click_position: Optional[int] = None
    dwell_time_ms: Optional[int] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def get_reward(self) -> float:
        """Calculate reward for learning."""
        reward = 0.0

        # Base reward from result quality
        reward += min(self.avg_score, 0.3)

        # Reward for user clicks
        if self.user_clicked:
            position_factor = 1.0 / (1.0 + (self.click_position or 0) * 0.1)
            reward += 0.4 * position_factor

        # Dwell time bonus
        if self.dwell_time_ms and self.dwell_time_ms > 3000:
            reward += min(0.2, self.dwell_time_ms / 30000)

        return min(1.0, reward)


@dataclass
class RoutingDecision:
    """Decision on retrieval method and weights."""
    method: RetrievalMethod
    semantic_weight: float
    lexical_weight: float
    fusion_strategy: FusionStrategy
    confidence: float
    reasoning: str


@dataclass
class FusionResult:
    """Result of neural-symbolic fusion."""
    chunks: List[RetrievedChunk]
    fusion_strategy: FusionStrategy
    semantic_contribution: float
    lexical_contribution: float
    attention_weights: Optional[Dict[int, float]] = None


@dataclass
class LearnedRoutingConfig:
    """Configuration for learned retrieval routing."""
    enable_routing_learning: bool = True
    learning_rate: float = 0.01
    exploration_rate: float = 0.1
    min_samples_for_learning: int = 50
    default_semantic_weight: float = 0.7
    default_lexical_weight: float = 0.3


@dataclass
class CuttingEdgeSearchConfig:
    """Configuration for Cutting Edge search."""
    enable_neural_symbolic: bool = True
    enable_learned_routing: bool = True
    enable_attention_fusion: bool = True
    routing_config: LearnedRoutingConfig = None
    base_config: ProcessorConfig = None

    def __post_init__(self):
        if self.routing_config is None:
            self.routing_config = LearnedRoutingConfig()
        if self.base_config is None:
            self.base_config = ProcessorConfig()


class QueryRouter:
    """
    Routes queries to optimal retrieval methods.

    Learns from feedback to improve routing decisions over time.
    """

    def __init__(self, config: Optional[LearnedRoutingConfig] = None):
        self.config = config or LearnedRoutingConfig()

        # Learned weights per query pattern
        self._pattern_weights: Dict[str, Dict[str, float]] = {}
        self._signal_history: List[RetrievalSignal] = []

        # Default method weights
        self._method_scores = {
            RetrievalMethod.SEMANTIC: 0.5,
            RetrievalMethod.LEXICAL: 0.5,
            RetrievalMethod.HYBRID: 0.6,
            RetrievalMethod.NEURAL_SYMBOLIC: 0.7,
            RetrievalMethod.ADAPTIVE: 0.65
        }

    def classify_query(self, query: str) -> str:
        """Classify query into a pattern for routing."""
        query_lower = query.lower()

        # Pattern detection
        if any(kw in query_lower for kw in ['"', 'exact', 'specific']):
            return "exact_match"
        elif any(kw in query_lower for kw in ['how', 'why', 'explain', 'what is']):
            return "conceptual"
        elif any(kw in query_lower for kw in ['list', 'examples', 'show me']):
            return "exploratory"
        elif len(query.split()) <= 3:
            return "keyword"
        elif any(kw in query_lower for kw in ['compare', 'difference', 'vs']):
            return "comparative"
        else:
            return "general"

    def route(self, query: str) -> RoutingDecision:
        """
        Route a query to optimal retrieval method and weights.

        Args:
            query: The search query

        Returns:
            RoutingDecision with method and weights
        """
        import random

        pattern = self.classify_query(query)

        # Exploration: try different methods occasionally
        if self.config.enable_routing_learning and random.random() < self.config.exploration_rate:
            method = random.choice(list(RetrievalMethod))
            semantic_w = random.uniform(0.3, 0.9)
            return RoutingDecision(
                method=method,
                semantic_weight=semantic_w,
                lexical_weight=1.0 - semantic_w,
                fusion_strategy=FusionStrategy.WEIGHTED_SUM,
                confidence=0.5,
                reasoning="Exploration: trying alternative routing"
            )

        # Check learned patterns
        if pattern in self._pattern_weights:
            weights = self._pattern_weights[pattern]
            semantic_w = weights.get("semantic", self.config.default_semantic_weight)
            lexical_w = weights.get("lexical", self.config.default_lexical_weight)

            # Determine best method from scores
            best_method = max(self._method_scores.items(), key=lambda x: x[1])[0]

            return RoutingDecision(
                method=best_method,
                semantic_weight=semantic_w,
                lexical_weight=lexical_w,
                fusion_strategy=FusionStrategy.LEARNED_WEIGHTS,
                confidence=0.8,
                reasoning=f"Learned routing for pattern: {pattern}"
            )

        # Pattern-based heuristics
        if pattern == "exact_match":
            return RoutingDecision(
                method=RetrievalMethod.LEXICAL,
                semantic_weight=0.3,
                lexical_weight=0.7,
                fusion_strategy=FusionStrategy.WEIGHTED_SUM,
                confidence=0.85,
                reasoning="Exact match query favors lexical search"
            )
        elif pattern == "conceptual":
            return RoutingDecision(
                method=RetrievalMethod.SEMANTIC,
                semantic_weight=0.85,
                lexical_weight=0.15,
                fusion_strategy=FusionStrategy.WEIGHTED_SUM,
                confidence=0.85,
                reasoning="Conceptual query favors semantic search"
            )
        elif pattern == "keyword":
            return RoutingDecision(
                method=RetrievalMethod.HYBRID,
                semantic_weight=0.5,
                lexical_weight=0.5,
                fusion_strategy=FusionStrategy.RECIPROCAL_RANK,
                confidence=0.75,
                reasoning="Short keyword query uses balanced hybrid"
            )
        elif pattern == "comparative":
            return RoutingDecision(
                method=RetrievalMethod.NEURAL_SYMBOLIC,
                semantic_weight=0.6,
                lexical_weight=0.4,
                fusion_strategy=FusionStrategy.ATTENTION,
                confidence=0.8,
                reasoning="Comparative query uses neural-symbolic fusion"
            )
        else:
            return RoutingDecision(
                method=RetrievalMethod.HYBRID,
                semantic_weight=self.config.default_semantic_weight,
                lexical_weight=self.config.default_lexical_weight,
                fusion_strategy=FusionStrategy.WEIGHTED_SUM,
                confidence=0.7,
                reasoning="Default hybrid routing"
            )

    def record_signal(self, signal: RetrievalSignal):
        """Record a retrieval signal for learning."""
        self._signal_history.append(signal)

        # Keep limited history
        if len(self._signal_history) > 1000:
            self._signal_history = self._signal_history[-500:]

        # Update learned weights
        if self.config.enable_routing_learning:
            self._update_weights(signal)

    def _update_weights(self, signal: RetrievalSignal):
        """Update weights based on signal."""
        pattern = self.classify_query(signal.query)
        reward = signal.get_reward()

        if pattern not in self._pattern_weights:
            self._pattern_weights[pattern] = {
                "semantic": self.config.default_semantic_weight,
                "lexical": self.config.default_lexical_weight
            }

        # Update weights toward the successful configuration
        if reward > 0.5:
            lr = self.config.learning_rate * reward
            self._pattern_weights[pattern]["semantic"] = (
                self._pattern_weights[pattern]["semantic"] * (1 - lr) +
                signal.semantic_weight * lr
            )
            self._pattern_weights[pattern]["lexical"] = (
                self._pattern_weights[pattern]["lexical"] * (1 - lr) +
                signal.lexical_weight * lr
            )

        # Update method scores
        method = signal.method_used
        current_score = self._method_scores.get(method, 0.5)
        self._method_scores[method] = current_score * 0.95 + reward * 0.05

    def get_routing_stats(self) -> Dict[str, Any]:
        """Get routing statistics."""
        return {
            "learned_patterns": len(self._pattern_weights),
            "signal_count": len(self._signal_history),
            "method_scores": {m.value: s for m, s in self._method_scores.items()},
            "pattern_weights": self._pattern_weights
        }


class NeuralSymbolicFuser:
    """
    Fuses neural (semantic) and symbolic (lexical) retrieval results.

    Implements multiple fusion strategies including attention-based fusion.
    """

    def __init__(self):
        self._fusion_history: List[Dict[str, Any]] = []

    def fuse(
        self,
        semantic_chunks: List[RetrievedChunk],
        lexical_chunks: List[RetrievedChunk],
        strategy: FusionStrategy,
        semantic_weight: float = 0.7,
        lexical_weight: float = 0.3
    ) -> FusionResult:
        """
        Fuse semantic and lexical retrieval results.

        Args:
            semantic_chunks: Results from semantic search
            lexical_chunks: Results from lexical search
            strategy: Fusion strategy to use
            semantic_weight: Weight for semantic results
            lexical_weight: Weight for lexical results

        Returns:
            FusionResult with fused chunks
        """
        if strategy == FusionStrategy.WEIGHTED_SUM:
            return self._weighted_sum_fusion(
                semantic_chunks, lexical_chunks, semantic_weight, lexical_weight
            )
        elif strategy == FusionStrategy.RECIPROCAL_RANK:
            return self._rrf_fusion(semantic_chunks, lexical_chunks)
        elif strategy == FusionStrategy.ATTENTION:
            return self._attention_fusion(
                semantic_chunks, lexical_chunks, semantic_weight
            )
        elif strategy == FusionStrategy.CASCADE:
            return self._cascade_fusion(
                semantic_chunks, lexical_chunks, semantic_weight
            )
        else:
            return self._weighted_sum_fusion(
                semantic_chunks, lexical_chunks, semantic_weight, lexical_weight
            )

    def _weighted_sum_fusion(
        self,
        semantic: List[RetrievedChunk],
        lexical: List[RetrievedChunk],
        sem_w: float,
        lex_w: float
    ) -> FusionResult:
        """Simple weighted sum fusion."""
        # Build chunk map
        chunk_map: Dict[int, RetrievedChunk] = {}

        for chunk in semantic:
            chunk_map[chunk.chunk_id] = RetrievedChunk(
                chunk_id=chunk.chunk_id,
                document_id=chunk.document_id,
                content=chunk.content,
                score=chunk.score * sem_w,
                semantic_score=chunk.semantic_score,
                keyword_score=0.0,
                metadata=chunk.metadata
            )

        for chunk in lexical:
            if chunk.chunk_id in chunk_map:
                existing = chunk_map[chunk.chunk_id]
                existing.score += chunk.score * lex_w
                existing.keyword_score = chunk.keyword_score
            else:
                chunk_map[chunk.chunk_id] = RetrievedChunk(
                    chunk_id=chunk.chunk_id,
                    document_id=chunk.document_id,
                    content=chunk.content,
                    score=chunk.score * lex_w,
                    semantic_score=0.0,
                    keyword_score=chunk.keyword_score,
                    metadata=chunk.metadata
                )

        chunks = sorted(chunk_map.values(), key=lambda x: x.score, reverse=True)

        return FusionResult(
            chunks=chunks,
            fusion_strategy=FusionStrategy.WEIGHTED_SUM,
            semantic_contribution=sem_w,
            lexical_contribution=lex_w
        )

    def _rrf_fusion(
        self,
        semantic: List[RetrievedChunk],
        lexical: List[RetrievedChunk],
        k: int = 60
    ) -> FusionResult:
        """Reciprocal Rank Fusion."""
        chunk_map: Dict[int, float] = {}
        chunk_data: Dict[int, RetrievedChunk] = {}

        # Semantic ranks
        for i, chunk in enumerate(semantic):
            rrf_score = 1.0 / (k + i + 1)
            chunk_map[chunk.chunk_id] = chunk_map.get(chunk.chunk_id, 0) + rrf_score
            chunk_data[chunk.chunk_id] = chunk

        # Lexical ranks
        for i, chunk in enumerate(lexical):
            rrf_score = 1.0 / (k + i + 1)
            chunk_map[chunk.chunk_id] = chunk_map.get(chunk.chunk_id, 0) + rrf_score
            if chunk.chunk_id not in chunk_data:
                chunk_data[chunk.chunk_id] = chunk

        # Sort by RRF score
        sorted_ids = sorted(chunk_map.keys(), key=lambda x: chunk_map[x], reverse=True)

        chunks = []
        for cid in sorted_ids:
            chunk = chunk_data[cid]
            chunk.score = chunk_map[cid]
            chunks.append(chunk)

        return FusionResult(
            chunks=chunks,
            fusion_strategy=FusionStrategy.RECIPROCAL_RANK,
            semantic_contribution=0.5,
            lexical_contribution=0.5
        )

    def _attention_fusion(
        self,
        semantic: List[RetrievedChunk],
        lexical: List[RetrievedChunk],
        base_weight: float
    ) -> FusionResult:
        """
        Attention-based fusion that weights results based on relevance signals.
        """
        chunk_map: Dict[int, RetrievedChunk] = {}
        attention_weights: Dict[int, float] = {}

        # Calculate attention weights based on score distribution
        sem_scores = [c.score for c in semantic] if semantic else [0]
        lex_scores = [c.score for c in lexical] if lexical else [0]

        sem_max = max(sem_scores) if sem_scores else 1
        lex_max = max(lex_scores) if lex_scores else 1

        for chunk in semantic:
            norm_score = chunk.score / sem_max if sem_max > 0 else 0
            attention = norm_score * base_weight
            attention_weights[chunk.chunk_id] = attention

            chunk_map[chunk.chunk_id] = RetrievedChunk(
                chunk_id=chunk.chunk_id,
                document_id=chunk.document_id,
                content=chunk.content,
                score=chunk.score * attention,
                semantic_score=chunk.semantic_score,
                keyword_score=0.0,
                metadata=chunk.metadata
            )

        for chunk in lexical:
            norm_score = chunk.score / lex_max if lex_max > 0 else 0
            attention = norm_score * (1 - base_weight)

            if chunk.chunk_id in chunk_map:
                existing = chunk_map[chunk.chunk_id]
                existing.score += chunk.score * attention
                existing.keyword_score = chunk.keyword_score
                attention_weights[chunk.chunk_id] += attention
            else:
                attention_weights[chunk.chunk_id] = attention
                chunk_map[chunk.chunk_id] = RetrievedChunk(
                    chunk_id=chunk.chunk_id,
                    document_id=chunk.document_id,
                    content=chunk.content,
                    score=chunk.score * attention,
                    semantic_score=0.0,
                    keyword_score=chunk.keyword_score,
                    metadata=chunk.metadata
                )

        chunks = sorted(chunk_map.values(), key=lambda x: x.score, reverse=True)

        return FusionResult(
            chunks=chunks,
            fusion_strategy=FusionStrategy.ATTENTION,
            semantic_contribution=base_weight,
            lexical_contribution=1 - base_weight,
            attention_weights=attention_weights
        )

    def _cascade_fusion(
        self,
        semantic: List[RetrievedChunk],
        lexical: List[RetrievedChunk],
        threshold: float
    ) -> FusionResult:
        """
        Cascade fusion: use semantic first, add lexical for low-confidence results.
        """
        chunks = []

        # Add high-confidence semantic results
        for chunk in semantic:
            if chunk.score >= threshold:
                chunks.append(chunk)

        # Fill with lexical results
        existing_ids = {c.chunk_id for c in chunks}
        for chunk in lexical:
            if chunk.chunk_id not in existing_ids:
                chunks.append(chunk)
                existing_ids.add(chunk.chunk_id)

        # Sort by score
        chunks.sort(key=lambda x: x.score, reverse=True)

        return FusionResult(
            chunks=chunks,
            fusion_strategy=FusionStrategy.CASCADE,
            semantic_contribution=threshold,
            lexical_contribution=1 - threshold
        )


class CuttingEdgeQueryProcessor:
    """
    Cutting Edge query processor with neural-symbolic fusion.

    Combines:
    - RAGQueryProcessor for base functionality
    - QueryRouter for learned retrieval routing
    - NeuralSymbolicFuser for advanced result fusion
    """

    def __init__(
        self,
        base_processor: RAGQueryProcessor,
        config: Optional[CuttingEdgeSearchConfig] = None
    ):
        self.base_processor = base_processor
        self.config = config or CuttingEdgeSearchConfig()

        self.router = QueryRouter(self.config.routing_config)
        self.fuser = NeuralSymbolicFuser()

        self._search_history: List[Dict[str, Any]] = []

    async def process(
        self,
        query: str,
        use_adaptive_routing: bool = True,
        **kwargs
    ) -> QueryResult:
        """
        Process query with cutting edge features.

        Args:
            query: Search query
            use_adaptive_routing: Use learned routing
            **kwargs: Additional parameters

        Returns:
            QueryResult with fused results
        """
        start_time = time.time()

        # Get routing decision
        if use_adaptive_routing and self.config.enable_learned_routing:
            routing = self.router.route(query)
        else:
            routing = RoutingDecision(
                method=RetrievalMethod.HYBRID,
                semantic_weight=self.config.base_config.semantic_weight,
                lexical_weight=self.config.base_config.keyword_weight,
                fusion_strategy=FusionStrategy.WEIGHTED_SUM,
                confidence=0.7,
                reasoning="Default hybrid"
            )

        # Perform search based on routing
        if routing.method == RetrievalMethod.SEMANTIC:
            result = await self.base_processor.process(
                query,
                semantic_weight=0.95,
                keyword_weight=0.05,
                **kwargs
            )
        elif routing.method == RetrievalMethod.LEXICAL:
            result = await self.base_processor.process(
                query,
                semantic_weight=0.2,
                keyword_weight=0.8,
                **kwargs
            )
        elif routing.method == RetrievalMethod.NEURAL_SYMBOLIC:
            # Get both result sets and fuse
            result = await self._neural_symbolic_search(query, routing, **kwargs)
        else:
            # Hybrid with routed weights
            result = await self.base_processor.process(
                query,
                semantic_weight=routing.semantic_weight,
                keyword_weight=routing.lexical_weight,
                **kwargs
            )

        processing_time = (time.time() - start_time) * 1000

        # Record search
        self._search_history.append({
            "query": query[:50],
            "method": routing.method.value,
            "fusion": routing.fusion_strategy.value,
            "semantic_weight": routing.semantic_weight,
            "result_count": len(result.chunks),
            "timestamp": datetime.now().isoformat()
        })

        if len(self._search_history) > 500:
            self._search_history = self._search_history[-250:]

        return QueryResult(
            query=result.query,
            chunks=result.chunks,
            context=result.context,
            total_chunks=result.total_chunks,
            processing_time_ms=processing_time,
            search_method=routing.method.value,
            semantic_weight=routing.semantic_weight,
            keyword_weight=routing.lexical_weight,
            success=result.success,
            error=result.error
        )

    async def _neural_symbolic_search(
        self,
        query: str,
        routing: RoutingDecision,
        **kwargs
    ) -> QueryResult:
        """Perform neural-symbolic fusion search."""
        # Get semantic results
        semantic_result = await self.base_processor.process(
            query,
            semantic_weight=1.0,
            keyword_weight=0.0,
            **kwargs
        )

        # Get lexical results
        lexical_result = await self.base_processor.process(
            query,
            semantic_weight=0.0,
            keyword_weight=1.0,
            **kwargs
        )

        # Fuse results
        fusion_result = self.fuser.fuse(
            semantic_chunks=semantic_result.chunks,
            lexical_chunks=lexical_result.chunks,
            strategy=routing.fusion_strategy,
            semantic_weight=routing.semantic_weight,
            lexical_weight=routing.lexical_weight
        )

        # Build context from fused results
        context = self.base_processor._build_context(
            fusion_result.chunks[:self.config.base_config.top_k]
        )

        return QueryResult(
            query=query,
            chunks=fusion_result.chunks[:self.config.base_config.top_k],
            context=context,
            total_chunks=len(fusion_result.chunks),
            processing_time_ms=0,  # Will be set by caller
            search_method="neural_symbolic",
            semantic_weight=fusion_result.semantic_contribution,
            keyword_weight=fusion_result.lexical_contribution,
            success=True
        )

    def record_click(
        self,
        query: str,
        click_position: int,
        dwell_time_ms: Optional[int] = None
    ):
        """Record user click for learning."""
        # Find recent search for this query
        recent = None
        for h in reversed(self._search_history[-50:]):
            if h["query"][:50] == query[:50]:
                recent = h
                break

        if recent:
            import uuid
            signal = RetrievalSignal(
                signal_id=f"sig_{uuid.uuid4().hex[:8]}",
                query=query,
                method_used=RetrievalMethod(recent["method"]),
                semantic_weight=recent["semantic_weight"],
                lexical_weight=1.0 - recent["semantic_weight"],
                result_count=recent["result_count"],
                avg_score=0.7,  # Approximate
                user_clicked=True,
                click_position=click_position,
                dwell_time_ms=dwell_time_ms
            )
            self.router.record_signal(signal)

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return {
            "config": {
                "neural_symbolic": self.config.enable_neural_symbolic,
                "learned_routing": self.config.enable_learned_routing
            },
            "routing": self.router.get_routing_stats(),
            "search_history_size": len(self._search_history),
            "method_distribution": {
                method.value: len([h for h in self._search_history if h.get("method") == method.value])
                for method in RetrievalMethod
            }
        }


# -----------------------------------------------------------------------------
# Factory Functions for Cutting Edge Features
# -----------------------------------------------------------------------------

_cutting_edge_processor: Optional[CuttingEdgeQueryProcessor] = None


def get_cutting_edge_query_processor(
    vector_store,
    embedder,
    config: Optional[CuttingEdgeSearchConfig] = None
) -> CuttingEdgeQueryProcessor:
    """Get or create the global cutting edge query processor."""
    global _cutting_edge_processor
    if _cutting_edge_processor is None:
        base = RAGQueryProcessor(vector_store, embedder, config.base_config if config else None)
        _cutting_edge_processor = CuttingEdgeQueryProcessor(base, config)
        logger.info("CuttingEdgeQueryProcessor created with neural-symbolic fusion")
    return _cutting_edge_processor


def reset_cutting_edge_query_processor():
    """Reset cutting edge query processor (for testing)."""
    global _cutting_edge_processor
    _cutting_edge_processor = None


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

_query_processor: Optional[RAGQueryProcessor] = None


def get_query_processor(
    vector_store,
    embedder,
    config: Optional[ProcessorConfig] = None
) -> RAGQueryProcessor:
    """Get or create query processor instance"""
    global _query_processor
    if _query_processor is None:
        _query_processor = RAGQueryProcessor(vector_store, embedder, config)
    return _query_processor


def reset_query_processor():
    """Reset query processor (for testing)"""
    global _query_processor
    _query_processor = None

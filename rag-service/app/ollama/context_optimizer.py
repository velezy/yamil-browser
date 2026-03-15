"""
Context Optimizer

Adaptive context window sizing based on quality feedback.
Optimizes context utilization for better responses.
"""

import logging
from typing import Optional
from dataclasses import dataclass, field

from .token_counter import TokenCounter

logger = logging.getLogger(__name__)


@dataclass
class ContextMetrics:
    """Metrics for context optimization"""
    tokens_used: int = 0
    tokens_available: int = 0
    utilization: float = 0.0
    quality_score: float = 0.0
    response_time: float = 0.0


@dataclass
class OptimizationResult:
    """Result of context optimization"""
    recommended_size: int
    action: str  # 'increase', 'decrease', 'maintain'
    reason: str
    confidence: float


class ContextOptimizer:
    """
    Adaptive context window sizing.

    Features:
    - Quality-aware context sizing
    - Utilization tracking
    - Automatic adjustment recommendations
    - History-based optimization
    """

    def __init__(
        self,
        max_context_tokens: int = 4000,
        target_utilization: float = 0.5,
        min_context_tokens: int = 512,
    ):
        """
        Initialize optimizer.

        Args:
            max_context_tokens: Maximum context size
            target_utilization: Target utilization (0.0-1.0)
            min_context_tokens: Minimum context size
        """
        self.max_context_tokens = max_context_tokens
        self.target_utilization = target_utilization
        self.min_context_tokens = min_context_tokens

        self.token_counter = TokenCounter()
        self.history: list[ContextMetrics] = []
        self.max_history = 100

    def calculate_utilization(
        self,
        context: str,
        max_tokens: int
    ) -> float:
        """
        Calculate context utilization.

        Args:
            context: RAG context text
            max_tokens: Maximum tokens available

        Returns:
            Utilization ratio (0.0-1.0)
        """
        tokens_used = self.token_counter.count_tokens(context)
        return min(tokens_used / max_tokens, 1.0) if max_tokens > 0 else 0.0

    def recommend_context_size(
        self,
        current_tokens: int,
        utilization: float,
        quality_score: float
    ) -> OptimizationResult:
        """
        Recommend context size adjustment.

        Args:
            current_tokens: Current context size
            utilization: Current utilization (0.0-1.0)
            quality_score: Response quality (0.0-1.0)

        Returns:
            OptimizationResult with recommendation
        """
        # Record metrics
        self.history.append(ContextMetrics(
            tokens_used=current_tokens,
            utilization=utilization,
            quality_score=quality_score,
        ))

        # Trim history
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history:]

        # Decision logic based on utilization and quality
        if utilization < 0.3 and quality_score < 0.5:
            # Underutilization with low quality → increase context
            new_size = min(
                int(current_tokens * 1.5),
                self.max_context_tokens
            )
            return OptimizationResult(
                recommended_size=new_size,
                action='increase',
                reason='Low utilization with poor quality - need more context',
                confidence=0.8
            )

        if utilization > 0.5 and quality_score > 0.7:
            # Good utilization with good quality → optimal
            return OptimizationResult(
                recommended_size=current_tokens,
                action='maintain',
                reason='Optimal utilization and quality',
                confidence=0.9
            )

        if utilization > 0.7 and quality_score < 0.7:
            # High utilization with low quality → increase (might need more)
            new_size = min(
                int(current_tokens * 1.3),
                self.max_context_tokens
            )
            return OptimizationResult(
                recommended_size=new_size,
                action='increase',
                reason='High utilization but quality still low - try more context',
                confidence=0.6
            )

        if utilization > 0.8 and quality_score > 0.8:
            # Very high utilization with good quality → could be optimal or could reduce
            return OptimizationResult(
                recommended_size=current_tokens,
                action='maintain',
                reason='High utilization with good quality',
                confidence=0.85
            )

        if utilization < 0.3 and quality_score > 0.7:
            # Low utilization with good quality → could reduce to save tokens
            new_size = max(
                int(current_tokens * 0.8),
                self.min_context_tokens
            )
            return OptimizationResult(
                recommended_size=new_size,
                action='decrease',
                reason='Good quality with low utilization - can reduce context',
                confidence=0.7
            )

        # Default: maintain current size
        return OptimizationResult(
            recommended_size=current_tokens,
            action='maintain',
            reason='No clear optimization signal',
            confidence=0.5
        )

    def optimize_context(
        self,
        documents: list[str],
        max_tokens: int,
        quality_scores: Optional[list[float]] = None
    ) -> list[str]:
        """
        Optimize document selection for context.

        Args:
            documents: List of document chunks
            max_tokens: Maximum tokens to use
            quality_scores: Optional relevance scores per document

        Returns:
            Optimized list of documents that fit within limit
        """
        if not documents:
            return []

        # Calculate tokens for each document
        doc_tokens = [
            self.token_counter.count_tokens(doc)
            for doc in documents
        ]

        # If scores provided, sort by score
        if quality_scores and len(quality_scores) == len(documents):
            indexed = list(zip(documents, doc_tokens, quality_scores))
            indexed.sort(key=lambda x: x[2], reverse=True)
            documents = [x[0] for x in indexed]
            doc_tokens = [x[1] for x in indexed]

        # Greedily select documents within limit
        selected = []
        total_tokens = 0

        for doc, tokens in zip(documents, doc_tokens):
            if total_tokens + tokens <= max_tokens:
                selected.append(doc)
                total_tokens += tokens
            elif total_tokens == 0:
                # First doc too large, truncate it
                truncated = self.token_counter.truncate_to_tokens(doc, max_tokens)
                selected.append(truncated)
                break

        return selected

    def get_optimal_chunk_size(
        self,
        query_tokens: int,
        num_chunks: int = 5,
        reserve_for_response: int = 512
    ) -> int:
        """
        Calculate optimal chunk size for retrieval.

        Args:
            query_tokens: Tokens in the query
            num_chunks: Expected number of chunks
            reserve_for_response: Tokens to reserve for response

        Returns:
            Optimal tokens per chunk
        """
        available = self.max_context_tokens - query_tokens - reserve_for_response
        optimal_per_chunk = max(
            available // num_chunks,
            self.min_context_tokens // num_chunks
        )
        return optimal_per_chunk

    def get_average_metrics(self) -> dict:
        """Get average metrics from history"""
        if not self.history:
            return {
                'avg_utilization': 0.0,
                'avg_quality': 0.0,
                'sample_count': 0,
            }

        return {
            'avg_utilization': sum(m.utilization for m in self.history) / len(self.history),
            'avg_quality': sum(m.quality_score for m in self.history) / len(self.history),
            'sample_count': len(self.history),
        }

    def clear_history(self) -> None:
        """Clear optimization history"""
        self.history.clear()

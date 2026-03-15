"""
Neural Re-ranking Module

Uses cross-encoder models to re-rank retrieved chunks for better relevance.
Industry standard: BGE-reranker, Cohere Rerank

Ensemble Reranking (Advanced):
- Combines cross-encoder (neural) + BM25 (lexical) scores
- Configurable weights with adaptive learning
- Multiple scoring signals: semantic, lexical, filename, metadata
"""

import logging
import math
import json
import os
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import Counter
import asyncio

logger = logging.getLogger(__name__)


# =============================================================================
# BM25 SCORER - Lexical Relevance
# =============================================================================

class BM25Scorer:
    """
    BM25 (Best Matching 25) scoring for lexical relevance.

    BM25 is a ranking function used by search engines to estimate relevance
    of documents to a query based on term frequency and inverse document frequency.

    Formula: score = IDF(qi) * (f(qi,D) * (k1 + 1)) / (f(qi,D) + k1 * (1 - b + b * |D|/avgdl))

    Where:
    - f(qi,D) = frequency of term qi in document D
    - |D| = length of document D
    - avgdl = average document length
    - k1 = term frequency saturation parameter (typically 1.2-2.0)
    - b = document length normalization (typically 0.75)
    """

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        """
        Initialize BM25 scorer.

        Args:
            k1: Term frequency saturation (1.2-2.0). Higher = more weight to term frequency.
            b: Length normalization (0-1). Higher = more penalty for long documents.
        """
        self.k1 = k1
        self.b = b
        self._corpus_stats: Dict[str, Any] = {}

    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenization - lowercase and split on non-alphanumeric"""
        import re
        return [t.lower() for t in re.findall(r'\w+', text) if len(t) > 1]

    def _compute_idf(self, term: str, doc_freqs: Dict[str, int], n_docs: int) -> float:
        """Compute IDF for a term"""
        df = doc_freqs.get(term, 0)
        if df == 0:
            return 0.0
        # Smoothed IDF: log((N - df + 0.5) / (df + 0.5) + 1)
        return math.log((n_docs - df + 0.5) / (df + 0.5) + 1)

    def score(
        self,
        query: str,
        documents: List[str],
        return_details: bool = False
    ) -> List[float]:
        """
        Score documents against a query using BM25.

        Args:
            query: The search query
            documents: List of document texts
            return_details: If True, return (scores, details) tuple

        Returns:
            List of BM25 scores (higher = more relevant)
        """
        if not documents:
            return []

        # Tokenize query and documents
        query_terms = self._tokenize(query)
        doc_tokens = [self._tokenize(doc) for doc in documents]

        # Compute corpus statistics
        n_docs = len(documents)
        doc_lengths = [len(tokens) for tokens in doc_tokens]
        avgdl = sum(doc_lengths) / n_docs if n_docs > 0 else 1

        # Document frequency for each term
        doc_freqs: Dict[str, int] = Counter()
        for tokens in doc_tokens:
            for term in set(tokens):
                doc_freqs[term] += 1

        # Score each document
        scores = []
        details = []

        for i, (doc, tokens) in enumerate(zip(documents, doc_tokens)):
            doc_len = doc_lengths[i]
            term_freqs = Counter(tokens)

            score = 0.0
            term_scores = {}

            for term in query_terms:
                if term not in term_freqs:
                    continue

                tf = term_freqs[term]
                idf = self._compute_idf(term, doc_freqs, n_docs)

                # BM25 formula
                numerator = tf * (self.k1 + 1)
                denominator = tf + self.k1 * (1 - self.b + self.b * doc_len / avgdl)
                term_score = idf * numerator / denominator

                score += term_score
                term_scores[term] = term_score

            scores.append(score)
            if return_details:
                details.append({
                    'doc_length': doc_len,
                    'term_scores': term_scores,
                    'total_score': score
                })

        # Normalize scores to 0-1 range
        if scores:
            max_score = max(scores) if max(scores) > 0 else 1
            scores = [s / max_score for s in scores]

        if return_details:
            return scores, details
        return scores

    def score_single(self, query: str, document: str) -> float:
        """Score a single document against query"""
        scores = self.score(query, [document])
        return scores[0] if scores else 0.0


# =============================================================================
# ENSEMBLE CONFIGURATION
# =============================================================================

@dataclass
class EnsembleConfig:
    """
    Configuration for ensemble reranking weights.

    Total should sum to ~1.0 for interpretable scores.
    Weights can be learned from user feedback.
    """
    # Core scoring weights
    cross_encoder_weight: float = 0.45  # Neural semantic relevance
    bm25_weight: float = 0.25           # Lexical/keyword relevance
    original_score_weight: float = 0.15 # Original embedding similarity
    filename_weight: float = 0.15       # Document name matching

    # Boost parameters
    exact_phrase_boost: float = 0.1     # Bonus for exact query match
    recency_weight: float = 0.0         # Weight for document recency (if available)

    # BM25 parameters
    bm25_k1: float = 1.5
    bm25_b: float = 0.75

    # Learning parameters
    learning_rate: float = 0.1          # For weight updates
    enable_learning: bool = True        # Enable weight adaptation

    # File to persist learned weights
    weights_file: Optional[str] = None

    def __post_init__(self):
        """Normalize weights to sum to 1.0"""
        total = (self.cross_encoder_weight + self.bm25_weight +
                 self.original_score_weight + self.filename_weight)
        if total > 0 and abs(total - 1.0) > 0.01:
            self.cross_encoder_weight /= total
            self.bm25_weight /= total
            self.original_score_weight /= total
            self.filename_weight /= total

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'cross_encoder_weight': self.cross_encoder_weight,
            'bm25_weight': self.bm25_weight,
            'original_score_weight': self.original_score_weight,
            'filename_weight': self.filename_weight,
            'exact_phrase_boost': self.exact_phrase_boost,
            'recency_weight': self.recency_weight,
            'bm25_k1': self.bm25_k1,
            'bm25_b': self.bm25_b,
            'learning_rate': self.learning_rate,
            'enable_learning': self.enable_learning
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EnsembleConfig':
        """Create from dictionary"""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def save(self, filepath: str):
        """Save weights to file"""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Saved ensemble weights to {filepath}")

    @classmethod
    def load(cls, filepath: str) -> 'EnsembleConfig':
        """Load weights from file"""
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded ensemble weights from {filepath}")
            return cls.from_dict(data)
        return cls()


@dataclass
class EnsembleScore:
    """Detailed scoring breakdown from ensemble reranker"""
    final_score: float
    cross_encoder_score: float
    bm25_score: float
    original_score: float
    filename_score: float
    exact_phrase_bonus: float
    component_weights: Dict[str, float] = field(default_factory=dict)

@dataclass
class RankedChunk:
    """A chunk with reranking score"""
    content: str
    original_score: float
    rerank_score: float
    metadata: Dict[str, Any]
    document_id: Optional[int] = None
    chunk_index: Optional[int] = None
    ensemble_details: Optional[EnsembleScore] = None  # Detailed breakdown


# =============================================================================
# ENSEMBLE RERANKER - Advanced Multi-Signal Reranking
# =============================================================================

class EnsembleReranker:
    """
    Advanced ensemble reranker combining multiple scoring signals.

    Combines:
    - Cross-encoder scores (neural semantic relevance)
    - BM25 scores (lexical/keyword relevance)
    - Original embedding similarity
    - Filename matching
    - Exact phrase matching

    Features:
    - Configurable weights for each signal
    - Adaptive weight learning from user feedback
    - Detailed scoring breakdown for debugging
    """

    def __init__(
        self,
        config: Optional[EnsembleConfig] = None,
        cross_encoder_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    ):
        """
        Initialize ensemble reranker.

        Args:
            config: Ensemble configuration with weights
            cross_encoder_model: Model for neural scoring
        """
        self.config = config or EnsembleConfig()
        self.cross_encoder_model = cross_encoder_model

        # Initialize components
        self._cross_encoder = None
        self._bm25 = BM25Scorer(k1=self.config.bm25_k1, b=self.config.bm25_b)
        self._initialized = False

        # Feedback history for learning
        self._feedback_history: List[Dict[str, Any]] = []
        self._max_feedback_history = 1000

        # Load persisted weights if available
        if self.config.weights_file and os.path.exists(self.config.weights_file):
            self.config = EnsembleConfig.load(self.config.weights_file)

    async def initialize(self):
        """Lazy load the cross-encoder model"""
        if self._initialized:
            return

        try:
            from sentence_transformers import CrossEncoder
            logger.info(f"Loading ensemble cross-encoder: {self.cross_encoder_model}")

            loop = asyncio.get_event_loop()
            self._cross_encoder = await loop.run_in_executor(
                None,
                lambda: CrossEncoder(self.cross_encoder_model, max_length=512)
            )
            self._initialized = True
            logger.info("Ensemble reranker initialized successfully")

        except ImportError:
            logger.warning("sentence-transformers not installed. Cross-encoder disabled.")
            self._initialized = True
        except Exception as e:
            logger.error(f"Failed to load cross-encoder: {e}")
            self._initialized = True

    def _compute_filename_score(self, query: str, metadata: Dict[str, Any]) -> float:
        """Compute filename matching score"""
        source = metadata.get('source', '') or metadata.get('filename', '')
        if not source:
            return 0.0

        query_lower = query.lower()
        query_terms = [t for t in query_lower.split() if len(t) > 2]
        source_lower = source.lower().replace('_', ' ').replace('-', ' ').replace('.', ' ')

        matches = sum(1 for term in query_terms if term in source_lower)
        if matches == 0:
            return 0.0

        # Score based on matches (0.3 per term, max 1.0)
        return min(matches * 0.3, 1.0)

    def _compute_exact_phrase_bonus(self, query: str, content: str) -> float:
        """Check for exact phrase match"""
        if query.lower() in content.lower():
            return self.config.exact_phrase_boost
        return 0.0

    async def _get_cross_encoder_scores(
        self,
        query: str,
        documents: List[str]
    ) -> List[float]:
        """Get cross-encoder scores for documents"""
        if self._cross_encoder is None:
            # Fallback: return zeros
            return [0.0] * len(documents)

        try:
            pairs = [[query, doc] for doc in documents]
            loop = asyncio.get_event_loop()
            scores = await loop.run_in_executor(
                None,
                lambda: self._cross_encoder.predict(pairs)
            )

            # Sigmoid normalize to 0-1
            def sigmoid(x):
                return 1 / (1 + math.exp(-x))

            return [sigmoid(float(s)) for s in scores]

        except Exception as e:
            logger.error(f"Cross-encoder scoring failed: {e}")
            return [0.0] * len(documents)

    async def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int = 10,
        return_details: bool = False
    ) -> List[RankedChunk]:
        """
        Re-rank chunks using ensemble scoring.

        Args:
            query: Search query
            chunks: List of chunks with 'content', 'score', 'metadata'
            top_k: Number of results to return
            return_details: Include detailed score breakdown

        Returns:
            List of RankedChunk sorted by ensemble score
        """
        await self.initialize()

        if not chunks:
            return []

        # Extract documents for scoring
        documents = [chunk.get('content', '') for chunk in chunks]

        # Compute all scores in parallel where possible
        cross_encoder_scores = await self._get_cross_encoder_scores(query, documents)
        bm25_scores = self._bm25.score(query, documents)

        # Build ranked chunks with ensemble scores
        ranked = []
        for i, chunk in enumerate(chunks):
            content = chunk.get('content', '')
            metadata = chunk.get('metadata', {})
            original_score = chunk.get('score', 0.0)

            # Component scores
            ce_score = cross_encoder_scores[i]
            bm25_score = bm25_scores[i]
            filename_score = self._compute_filename_score(query, metadata)
            phrase_bonus = self._compute_exact_phrase_bonus(query, content)

            # Weighted ensemble score
            ensemble_score = (
                self.config.cross_encoder_weight * ce_score +
                self.config.bm25_weight * bm25_score +
                self.config.original_score_weight * original_score +
                self.config.filename_weight * filename_score +
                phrase_bonus
            )

            # Clamp to 0-1
            ensemble_score = min(max(ensemble_score, 0.0), 1.0)

            # Create detailed breakdown if requested
            details = None
            if return_details:
                details = EnsembleScore(
                    final_score=ensemble_score,
                    cross_encoder_score=ce_score,
                    bm25_score=bm25_score,
                    original_score=original_score,
                    filename_score=filename_score,
                    exact_phrase_bonus=phrase_bonus,
                    component_weights={
                        'cross_encoder': self.config.cross_encoder_weight,
                        'bm25': self.config.bm25_weight,
                        'original': self.config.original_score_weight,
                        'filename': self.config.filename_weight
                    }
                )

            ranked.append(RankedChunk(
                content=content,
                original_score=original_score,
                rerank_score=ensemble_score,
                metadata=metadata,
                document_id=chunk.get('document_id'),
                chunk_index=chunk.get('chunk_index'),
                ensemble_details=details
            ))

        # Sort by ensemble score
        ranked.sort(key=lambda x: x.rerank_score, reverse=True)

        logger.debug(f"Ensemble reranked {len(chunks)} chunks, returning top {top_k}")
        return ranked[:top_k]

    def record_feedback(
        self,
        query: str,
        selected_chunk_id: int,
        ranked_chunks: List[RankedChunk],
        feedback_type: str = "click"  # click, thumbs_up, thumbs_down
    ):
        """
        Record user feedback for weight learning.

        Args:
            query: The search query
            selected_chunk_id: ID of chunk user selected/liked
            ranked_chunks: The ranked results shown to user
            feedback_type: Type of feedback
        """
        if not self.config.enable_learning:
            return

        # Find position of selected chunk
        selected_idx = None
        for i, chunk in enumerate(ranked_chunks):
            if chunk.document_id == selected_chunk_id:
                selected_idx = i
                break

        if selected_idx is None:
            return

        # Store feedback
        self._feedback_history.append({
            'query': query,
            'selected_idx': selected_idx,
            'total_results': len(ranked_chunks),
            'feedback_type': feedback_type,
            'selected_details': ranked_chunks[selected_idx].ensemble_details
        })

        # Trim history
        if len(self._feedback_history) > self._max_feedback_history:
            self._feedback_history = self._feedback_history[-self._max_feedback_history:]

    def update_weights_from_feedback(self):
        """
        Update ensemble weights based on accumulated feedback.

        Uses simple gradient-like updates based on which components
        contributed most to correctly ranked results.
        """
        if not self.config.enable_learning or not self._feedback_history:
            return

        # Analyze feedback to adjust weights
        # If selected items were not top-ranked, increase weights of
        # components that scored them higher

        ce_adjustment = 0.0
        bm25_adjustment = 0.0
        orig_adjustment = 0.0
        filename_adjustment = 0.0

        for feedback in self._feedback_history[-100:]:  # Last 100 feedbacks
            idx = feedback['selected_idx']
            details = feedback.get('selected_details')

            if details is None:
                continue

            # If selected wasn't #1, see which components scored it well
            if idx > 0:
                # Reward components that gave high scores to the selected item
                if details.cross_encoder_score > 0.5:
                    ce_adjustment += self.config.learning_rate * (1 - idx/10)
                if details.bm25_score > 0.5:
                    bm25_adjustment += self.config.learning_rate * (1 - idx/10)
                if details.original_score > 0.5:
                    orig_adjustment += self.config.learning_rate * (1 - idx/10)
                if details.filename_score > 0.3:
                    filename_adjustment += self.config.learning_rate * (1 - idx/10)

        # Apply adjustments
        n = len(self._feedback_history[-100:])
        if n > 10:  # Only update with enough data
            self.config.cross_encoder_weight += ce_adjustment / n
            self.config.bm25_weight += bm25_adjustment / n
            self.config.original_score_weight += orig_adjustment / n
            self.config.filename_weight += filename_adjustment / n

            # Normalize
            self.config.__post_init__()

            # Persist if configured
            if self.config.weights_file:
                self.config.save(self.config.weights_file)

            logger.info(f"Updated ensemble weights: CE={self.config.cross_encoder_weight:.3f}, "
                       f"BM25={self.config.bm25_weight:.3f}, "
                       f"Orig={self.config.original_score_weight:.3f}, "
                       f"File={self.config.filename_weight:.3f}")

    def get_config(self) -> Dict[str, Any]:
        """Get current configuration as dictionary"""
        return {
            'weights': self.config.to_dict(),
            'cross_encoder_model': self.cross_encoder_model,
            'initialized': self._initialized,
            'feedback_count': len(self._feedback_history)
        }


class NeuralReranker:
    """
    Neural re-ranking using cross-encoder models.

    Cross-encoders score query-document pairs together (more accurate than bi-encoders)
    but slower. Use after initial retrieval to re-score top candidates.
    """

    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        """
        Initialize the reranker.

        Args:
            model_name: Cross-encoder model to use
                - "cross-encoder/ms-marco-MiniLM-L-6-v2" (fast, good quality)
                - "BAAI/bge-reranker-base" (better quality, slower)
                - "BAAI/bge-reranker-large" (best quality, slowest)
        """
        self.model_name = model_name
        self.model = None
        self._initialized = False

    async def initialize(self):
        """Lazy load the model"""
        if self._initialized:
            return

        try:
            from sentence_transformers import CrossEncoder
            logger.info(f"Loading reranker model: {self.model_name}")

            # Load in thread pool to not block
            loop = asyncio.get_event_loop()
            self.model = await loop.run_in_executor(
                None,
                lambda: CrossEncoder(self.model_name, max_length=512)
            )
            self._initialized = True
            logger.info("Reranker model loaded successfully")

        except ImportError:
            logger.warning("sentence-transformers not installed. Using fallback reranker.")
            self._initialized = True
        except Exception as e:
            logger.error(f"Failed to load reranker model: {e}")
            self._initialized = True

    async def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int = 5
    ) -> List[RankedChunk]:
        """
        Re-rank chunks by relevance to query.

        Args:
            query: The search query
            chunks: List of chunks with 'content' and optionally 'score', 'metadata'
            top_k: Number of top results to return

        Returns:
            List of RankedChunk sorted by rerank_score (descending)
        """
        await self.initialize()

        if not chunks:
            return []

        # If model not available, use fallback
        if self.model is None:
            return await self._fallback_rerank(query, chunks, top_k)

        try:
            # Create query-document pairs
            pairs = [[query, chunk.get('content', '')] for chunk in chunks]

            # Score all pairs
            loop = asyncio.get_event_loop()
            scores = await loop.run_in_executor(
                None,
                lambda: self.model.predict(pairs)
            )

            # Normalize scores using sigmoid to convert to 0-1 range
            # Cross-encoder scores can be negative; sigmoid maps any value to (0, 1)
            import math
            def sigmoid(x):
                return 1 / (1 + math.exp(-x))

            # Check if query mentions a filename/document name
            query_lower = query.lower()
            query_terms = [t for t in query_lower.split() if len(t) > 2]

            # Create ranked chunks with normalized scores and filename boosting
            ranked = []
            for chunk, score in zip(chunks, scores):
                normalized_score = sigmoid(float(score))

                # Filename boosting: if query mentions document name, boost matching files
                metadata = chunk.get('metadata', {})
                source = metadata.get('source', '') or metadata.get('filename', '')
                if source:
                    source_lower = source.lower().replace('_', ' ').replace('-', ' ').replace('.', ' ')
                    # Check if query terms match filename
                    filename_matches = sum(1 for term in query_terms if term in source_lower)
                    if filename_matches > 0:
                        # Significant boost for filename matches (0.3 per matching term, max 0.6)
                        filename_boost = min(filename_matches * 0.3, 0.6)
                        normalized_score = min(normalized_score + filename_boost, 1.0)
                        logger.debug(f"Filename boost +{filename_boost:.2f} for '{source}' (matches: {filename_matches})")

                ranked.append(RankedChunk(
                    content=chunk.get('content', ''),
                    original_score=chunk.get('score', 0.0),
                    rerank_score=normalized_score,  # Normalized to 0-1 with filename boost
                    metadata=metadata,
                    document_id=chunk.get('document_id'),
                    chunk_index=chunk.get('chunk_index')
                ))

            # Sort by rerank score (descending)
            ranked.sort(key=lambda x: x.rerank_score, reverse=True)

            logger.debug(f"Reranked {len(chunks)} chunks, returning top {top_k}")
            return ranked[:top_k]

        except Exception as e:
            logger.error(f"Reranking failed: {e}")
            return await self._fallback_rerank(query, chunks, top_k)

    async def _fallback_rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int
    ) -> List[RankedChunk]:
        """
        Fallback reranking using heuristics when model unavailable.
        Includes filename boosting for document-specific queries.
        """
        query_lower = query.lower()
        query_terms = [t for t in query_lower.split() if len(t) > 2]

        ranked = []
        for chunk in chunks:
            content = chunk.get('content', '')
            content_lower = content.lower()
            original_score = chunk.get('score', 0.0)
            metadata = chunk.get('metadata', {})

            # Calculate heuristic rerank score
            rerank_score = original_score

            # Filename boosting: if query mentions document name, boost matching files
            source = metadata.get('source', '') or metadata.get('filename', '')
            if source:
                source_lower = source.lower().replace('_', ' ').replace('-', ' ').replace('.', ' ')
                filename_matches = sum(1 for term in query_terms if term in source_lower)
                if filename_matches > 0:
                    # Significant boost for filename matches (0.3 per matching term, max 0.6)
                    filename_boost = min(filename_matches * 0.3, 0.6)
                    rerank_score += filename_boost
                    logger.debug(f"Filename boost +{filename_boost:.2f} for '{source}'")

            # Exact phrase match bonus
            if query_lower in content_lower:
                rerank_score += 0.3

            # Query term coverage
            term_matches = sum(1 for term in query_terms if term in content_lower)
            term_coverage = term_matches / max(len(query_terms), 1)
            rerank_score += term_coverage * 0.2

            # Term density (terms per 100 chars)
            if content:
                density = (term_matches * 100) / len(content)
                rerank_score += min(density * 0.05, 0.1)

            ranked.append(RankedChunk(
                content=content,
                original_score=original_score,
                rerank_score=rerank_score,
                metadata=metadata,
                document_id=chunk.get('document_id'),
                chunk_index=chunk.get('chunk_index')
            ))

        ranked.sort(key=lambda x: x.rerank_score, reverse=True)
        return ranked[:top_k]


class OllamaReranker:
    """
    Re-ranking using Ollama LLM for scoring relevance.

    Useful when you don't want to install sentence-transformers
    or want to use local models only.
    """

    def __init__(self, model: str = "gemma2:2b", ollama_url: str = "http://localhost:11434"):
        self.model = model
        self.ollama_url = ollama_url

    async def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int = 5
    ) -> List[RankedChunk]:
        """
        Re-rank using LLM scoring.

        Asks the LLM to rate relevance of each chunk to the query.
        """
        import aiohttp

        if not chunks:
            return []

        ranked = []

        async with aiohttp.ClientSession() as session:
            for chunk in chunks:
                content = chunk.get('content', '')[:500]  # Limit content length

                prompt = f"""Rate how relevant this text is to the query on a scale of 0-10.
Only respond with a single number.

Query: {query}

Text: {content}

Relevance score (0-10):"""

                try:
                    async with session.post(
                        f"{self.ollama_url}/api/generate",
                        json={
                            "model": self.model,
                            "prompt": prompt,
                            "stream": False,
                            "options": {"temperature": 0, "num_predict": 5}
                        },
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            response = data.get('response', '5').strip()
                            # Extract number from response
                            score = float(''.join(c for c in response if c.isdigit() or c == '.') or '5')
                            score = min(max(score, 0), 10) / 10  # Normalize to 0-1
                        else:
                            score = chunk.get('score', 0.5)

                except Exception as e:
                    logger.warning(f"Ollama rerank failed for chunk: {e}")
                    score = chunk.get('score', 0.5)

                ranked.append(RankedChunk(
                    content=chunk.get('content', ''),
                    original_score=chunk.get('score', 0.0),
                    rerank_score=score,
                    metadata=chunk.get('metadata', {}),
                    document_id=chunk.get('document_id'),
                    chunk_index=chunk.get('chunk_index')
                ))

        ranked.sort(key=lambda x: x.rerank_score, reverse=True)
        return ranked[:top_k]


# =============================================================================
# CUTTING EDGE: Online Learning & Personalized Ranking
# =============================================================================

from enum import Enum
from datetime import datetime, timedelta


class ClickType(str, Enum):
    """Types of user clicks for learning."""
    VIEW = "view"  # User viewed the result
    CLICK = "click"  # User clicked the result
    DWELL = "dwell"  # User spent time on result
    COPY = "copy"  # User copied content
    THUMBS_UP = "thumbs_up"  # Explicit positive
    THUMBS_DOWN = "thumbs_down"  # Explicit negative
    SKIP = "skip"  # User skipped this result


class PersonalizationLevel(str, Enum):
    """Levels of personalization."""
    NONE = "none"  # No personalization
    LIGHT = "light"  # Slight adjustments
    MODERATE = "moderate"  # Noticeable personalization
    HEAVY = "heavy"  # Strong personalization


@dataclass
class ClickSignal:
    """A click signal for learning."""
    signal_id: str
    user_id: str
    query: str
    result_position: int
    result_id: str
    click_type: ClickType
    dwell_time_ms: Optional[int] = None
    result_scores: Optional[Dict[str, float]] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def get_reward(self) -> float:
        """Get reward value for this signal."""
        rewards = {
            ClickType.VIEW: 0.1,
            ClickType.CLICK: 0.4,
            ClickType.DWELL: 0.5,
            ClickType.COPY: 0.7,
            ClickType.THUMBS_UP: 1.0,
            ClickType.THUMBS_DOWN: -0.5,
            ClickType.SKIP: -0.1
        }
        base_reward = rewards.get(self.click_type, 0.0)

        # Position discount - clicks at lower positions are more valuable
        position_factor = 1.0 / (1.0 + self.result_position * 0.1)

        # Dwell time bonus
        dwell_bonus = 0.0
        if self.dwell_time_ms and self.dwell_time_ms > 5000:  # 5 seconds
            dwell_bonus = min(0.2, self.dwell_time_ms / 50000)

        return base_reward * position_factor + dwell_bonus


@dataclass
class UserPreferences:
    """User-specific ranking preferences."""
    user_id: str
    weight_adjustments: Dict[str, float]  # Component -> adjustment
    domain_preferences: Dict[str, float]  # Domain/topic -> preference
    recency_preference: float  # How much to weight recent docs
    format_preferences: Dict[str, float]  # File type preferences
    interaction_count: int = 0
    last_updated: datetime = None

    def __post_init__(self):
        if self.last_updated is None:
            self.last_updated = datetime.now()

    def apply_to_config(self, base_config: EnsembleConfig) -> EnsembleConfig:
        """Apply user preferences to base config."""
        adjusted = EnsembleConfig(
            cross_encoder_weight=base_config.cross_encoder_weight + self.weight_adjustments.get('cross_encoder', 0.0),
            bm25_weight=base_config.bm25_weight + self.weight_adjustments.get('bm25', 0.0),
            original_score_weight=base_config.original_score_weight + self.weight_adjustments.get('original', 0.0),
            filename_weight=base_config.filename_weight + self.weight_adjustments.get('filename', 0.0),
            recency_weight=self.recency_preference,
            enable_learning=base_config.enable_learning
        )
        return adjusted


@dataclass
class OnlineLearningConfig:
    """Configuration for online learning."""
    learning_rate: float = 0.01
    momentum: float = 0.9
    min_signals_for_update: int = 5
    max_signal_age_hours: int = 168  # 1 week
    enable_personalization: bool = True
    personalization_level: PersonalizationLevel = PersonalizationLevel.MODERATE
    exploration_rate: float = 0.05  # Epsilon for exploration


@dataclass
class CuttingEdgeRerankerConfig:
    """Configuration for Cutting Edge reranker."""
    enable_online_learning: bool = True
    enable_personalization: bool = True
    enable_click_learning: bool = True
    online_config: OnlineLearningConfig = None
    base_config: EnsembleConfig = None

    def __post_init__(self):
        if self.online_config is None:
            self.online_config = OnlineLearningConfig()
        if self.base_config is None:
            self.base_config = EnsembleConfig()


class OnlineLearningEngine:
    """
    Online learning engine for real-time weight updates from clicks.

    Updates ranking weights based on user interaction signals in real-time.
    """

    def __init__(self, config: Optional[OnlineLearningConfig] = None):
        self.config = config or OnlineLearningConfig()
        self._signal_buffer: List[ClickSignal] = []
        self._weight_gradients: Dict[str, float] = {
            'cross_encoder': 0.0,
            'bm25': 0.0,
            'original': 0.0,
            'filename': 0.0
        }
        self._momentum: Dict[str, float] = {
            'cross_encoder': 0.0,
            'bm25': 0.0,
            'original': 0.0,
            'filename': 0.0
        }
        self._update_count = 0

    def add_signal(self, signal: ClickSignal):
        """Add a click signal for learning."""
        self._signal_buffer.append(signal)

        # Remove old signals
        cutoff = datetime.now() - timedelta(hours=self.config.max_signal_age_hours)
        self._signal_buffer = [s for s in self._signal_buffer if s.timestamp > cutoff]

        # Update gradients immediately for online learning
        self._update_gradients(signal)

    def _update_gradients(self, signal: ClickSignal):
        """Update gradients based on a single signal."""
        reward = signal.get_reward()

        if signal.result_scores:
            # Calculate gradient based on which components contributed to the result
            total_score = sum(signal.result_scores.values())
            if total_score > 0:
                for component, score in signal.result_scores.items():
                    contribution = score / total_score
                    gradient = reward * contribution
                    self._weight_gradients[component] = (
                        self._weight_gradients.get(component, 0.0) + gradient
                    )

    def get_weight_updates(self) -> Dict[str, float]:
        """
        Get weight updates from accumulated signals.

        Returns adjustment values to apply to base weights.
        """
        if len(self._signal_buffer) < self.config.min_signals_for_update:
            return {}

        updates = {}
        for component, gradient in self._weight_gradients.items():
            # Apply momentum
            self._momentum[component] = (
                self.config.momentum * self._momentum[component] +
                (1 - self.config.momentum) * gradient
            )

            # Calculate update
            update = self.config.learning_rate * self._momentum[component]

            # Clip update to prevent extreme changes
            update = max(-0.1, min(0.1, update))
            updates[component] = update

        # Reset gradients after update
        for component in self._weight_gradients:
            self._weight_gradients[component] *= 0.5  # Decay gradients

        self._update_count += 1
        return updates

    def apply_updates_to_config(self, config: EnsembleConfig) -> EnsembleConfig:
        """Apply learned updates to a config."""
        updates = self.get_weight_updates()
        if not updates:
            return config

        return EnsembleConfig(
            cross_encoder_weight=config.cross_encoder_weight + updates.get('cross_encoder', 0.0),
            bm25_weight=config.bm25_weight + updates.get('bm25', 0.0),
            original_score_weight=config.original_score_weight + updates.get('original', 0.0),
            filename_weight=config.filename_weight + updates.get('filename', 0.0),
            exact_phrase_boost=config.exact_phrase_boost,
            recency_weight=config.recency_weight,
            enable_learning=config.enable_learning
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get learning statistics."""
        return {
            "total_signals": len(self._signal_buffer),
            "update_count": self._update_count,
            "current_gradients": self._weight_gradients.copy(),
            "current_momentum": self._momentum.copy(),
            "signals_by_type": {
                ct.value: len([s for s in self._signal_buffer if s.click_type == ct])
                for ct in ClickType
            }
        }


class PersonalizedRankingModel:
    """
    Per-user personalized ranking model.

    Learns user preferences from interaction history and adjusts
    ranking weights accordingly.
    """

    def __init__(self, config: Optional[OnlineLearningConfig] = None):
        self.config = config or OnlineLearningConfig()
        self._user_preferences: Dict[str, UserPreferences] = {}
        self._user_signals: Dict[str, List[ClickSignal]] = {}

    def get_user_preferences(self, user_id: str) -> UserPreferences:
        """Get or create user preferences."""
        if user_id not in self._user_preferences:
            self._user_preferences[user_id] = UserPreferences(
                user_id=user_id,
                weight_adjustments={},
                domain_preferences={},
                recency_preference=0.0,
                format_preferences={}
            )
        return self._user_preferences[user_id]

    def record_signal(self, signal: ClickSignal):
        """Record a signal for a user."""
        user_id = signal.user_id

        if user_id not in self._user_signals:
            self._user_signals[user_id] = []

        self._user_signals[user_id].append(signal)

        # Keep limited history per user
        if len(self._user_signals[user_id]) > 500:
            self._user_signals[user_id] = self._user_signals[user_id][-500:]

        # Update preferences
        self._update_preferences(user_id)

    def _update_preferences(self, user_id: str):
        """Update user preferences based on signals."""
        signals = self._user_signals.get(user_id, [])
        if len(signals) < 10:
            return

        prefs = self.get_user_preferences(user_id)
        prefs.interaction_count = len(signals)
        prefs.last_updated = datetime.now()

        # Analyze signals to determine preferences
        weight_scores = {
            'cross_encoder': 0.0,
            'bm25': 0.0,
            'original': 0.0,
            'filename': 0.0
        }

        for signal in signals[-100:]:  # Last 100 signals
            reward = signal.get_reward()
            if signal.result_scores:
                for component, score in signal.result_scores.items():
                    if component in weight_scores:
                        weight_scores[component] += reward * score

        # Normalize to small adjustments
        total = sum(abs(v) for v in weight_scores.values())
        if total > 0:
            adjustment_scale = {
                PersonalizationLevel.NONE: 0.0,
                PersonalizationLevel.LIGHT: 0.05,
                PersonalizationLevel.MODERATE: 0.1,
                PersonalizationLevel.HEAVY: 0.2
            }
            scale = adjustment_scale.get(self.config.personalization_level, 0.1)

            for component, score in weight_scores.items():
                prefs.weight_adjustments[component] = (score / total) * scale

        # Analyze format preferences
        format_signals: Dict[str, List[float]] = {}
        for signal in signals[-100:]:
            if signal.result_scores:
                # Try to extract format from result_id
                result_id = signal.result_id.lower()
                for ext in ['.pdf', '.doc', '.txt', '.md', '.py', '.js']:
                    if ext in result_id:
                        if ext not in format_signals:
                            format_signals[ext] = []
                        format_signals[ext].append(signal.get_reward())
                        break

        for fmt, rewards in format_signals.items():
            if len(rewards) >= 5:
                avg_reward = sum(rewards) / len(rewards)
                prefs.format_preferences[fmt] = avg_reward

    def get_personalized_config(
        self,
        user_id: str,
        base_config: EnsembleConfig
    ) -> EnsembleConfig:
        """Get personalized config for a user."""
        prefs = self.get_user_preferences(user_id)
        return prefs.apply_to_config(base_config)

    def get_stats(self) -> Dict[str, Any]:
        """Get personalization statistics."""
        return {
            "total_users": len(self._user_preferences),
            "users_with_preferences": len([
                u for u in self._user_preferences.values()
                if u.interaction_count >= 10
            ]),
            "total_signals": sum(len(s) for s in self._user_signals.values())
        }


class CuttingEdgeReranker:
    """
    Cutting Edge reranker with online learning and personalization.

    Combines:
    - EnsembleReranker for multi-signal scoring
    - OnlineLearningEngine for real-time weight updates
    - PersonalizedRankingModel for per-user preferences
    """

    def __init__(self, config: Optional[CuttingEdgeRerankerConfig] = None):
        self.config = config or CuttingEdgeRerankerConfig()

        # Initialize components
        self.base_reranker = EnsembleReranker(config=self.config.base_config)
        self.online_learner = OnlineLearningEngine(self.config.online_config)
        self.personalization = PersonalizedRankingModel(self.config.online_config)

        self._initialized = False
        self._rerank_history: List[Dict[str, Any]] = []

    async def initialize(self):
        """Initialize the reranker."""
        if self._initialized:
            return

        await self.base_reranker.initialize()
        self._initialized = True
        logger.info("CuttingEdgeReranker initialized with online learning")

    async def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        user_id: Optional[str] = None,
        top_k: int = 10,
        return_details: bool = True
    ) -> List[RankedChunk]:
        """
        Rerank with online learning and personalization.

        Args:
            query: Search query
            chunks: Chunks to rerank
            user_id: Optional user ID for personalization
            top_k: Number of results to return
            return_details: Include detailed scoring

        Returns:
            List of ranked chunks
        """
        await self.initialize()

        # Get effective config
        effective_config = self.config.base_config

        # Apply online learning updates
        if self.config.enable_online_learning:
            effective_config = self.online_learner.apply_updates_to_config(effective_config)

        # Apply personalization
        if self.config.enable_personalization and user_id:
            effective_config = self.personalization.get_personalized_config(
                user_id, effective_config
            )

        # Create temporary reranker with effective config
        reranker = EnsembleReranker(config=effective_config)
        reranker._cross_encoder = self.base_reranker._cross_encoder
        reranker._initialized = True

        # Perform reranking
        results = await reranker.rerank(
            query=query,
            chunks=chunks,
            top_k=top_k,
            return_details=return_details
        )

        # Record for history
        self._rerank_history.append({
            "query": query[:50],
            "user_id": user_id,
            "num_chunks": len(chunks),
            "num_results": len(results),
            "timestamp": datetime.now().isoformat()
        })

        if len(self._rerank_history) > 1000:
            self._rerank_history = self._rerank_history[-500:]

        return results

    def record_click(
        self,
        user_id: str,
        query: str,
        result_position: int,
        result_id: str,
        click_type: ClickType,
        dwell_time_ms: Optional[int] = None,
        result_scores: Optional[Dict[str, float]] = None
    ):
        """
        Record a user click for learning.

        Args:
            user_id: User who clicked
            query: The search query
            result_position: Position of clicked result (0-indexed)
            result_id: ID of the clicked result
            click_type: Type of click/interaction
            dwell_time_ms: Time spent on result (optional)
            result_scores: Score components of the result (optional)
        """
        import uuid

        signal = ClickSignal(
            signal_id=f"sig_{uuid.uuid4().hex[:8]}",
            user_id=user_id,
            query=query,
            result_position=result_position,
            result_id=result_id,
            click_type=click_type,
            dwell_time_ms=dwell_time_ms,
            result_scores=result_scores
        )

        # Feed to online learner
        if self.config.enable_online_learning:
            self.online_learner.add_signal(signal)

        # Feed to personalization
        if self.config.enable_personalization:
            self.personalization.record_signal(signal)

        logger.debug(f"Recorded click: {click_type.value} at position {result_position}")

    def record_batch_feedback(
        self,
        user_id: str,
        query: str,
        results: List[RankedChunk],
        clicked_positions: List[int]
    ):
        """
        Record feedback for a batch of results.

        Automatically infers skips for non-clicked results.
        """
        for i, result in enumerate(results):
            if i in clicked_positions:
                click_type = ClickType.CLICK
            else:
                click_type = ClickType.SKIP

            scores = {}
            if result.ensemble_details:
                scores = {
                    'cross_encoder': result.ensemble_details.cross_encoder_score,
                    'bm25': result.ensemble_details.bm25_score,
                    'original': result.ensemble_details.original_score,
                    'filename': result.ensemble_details.filename_score
                }

            self.record_click(
                user_id=user_id,
                query=query,
                result_position=i,
                result_id=str(result.document_id or i),
                click_type=click_type,
                result_scores=scores
            )

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return {
            "initialized": self._initialized,
            "config": {
                "online_learning": self.config.enable_online_learning,
                "personalization": self.config.enable_personalization,
                "click_learning": self.config.enable_click_learning
            },
            "online_learning": self.online_learner.get_stats(),
            "personalization": self.personalization.get_stats(),
            "rerank_history_size": len(self._rerank_history),
            "base_weights": self.config.base_config.to_dict()
        }


# -----------------------------------------------------------------------------
# Factory Functions for Cutting Edge Features
# -----------------------------------------------------------------------------

_cutting_edge_reranker: Optional[CuttingEdgeReranker] = None


def get_cutting_edge_reranker(
    config: Optional[CuttingEdgeRerankerConfig] = None
) -> CuttingEdgeReranker:
    """Get or create the global cutting edge reranker."""
    global _cutting_edge_reranker
    if _cutting_edge_reranker is None:
        _cutting_edge_reranker = CuttingEdgeReranker(config)
        logger.info("CuttingEdgeReranker created with online learning")
    return _cutting_edge_reranker


async def cutting_edge_rerank(
    query: str,
    chunks: List[Dict[str, Any]],
    user_id: Optional[str] = None,
    top_k: int = 10
) -> List[RankedChunk]:
    """
    Convenience function for cutting edge reranking.

    Includes online learning and personalization.
    """
    reranker = get_cutting_edge_reranker()
    return await reranker.rerank(query, chunks, user_id, top_k)


def reset_cutting_edge_reranker():
    """Reset cutting edge reranker (for testing)."""
    global _cutting_edge_reranker
    _cutting_edge_reranker = None


# Singleton instance
_reranker: Optional[NeuralReranker] = None

def get_reranker() -> NeuralReranker:
    """Get or create the global reranker instance"""
    global _reranker
    if _reranker is None:
        _reranker = NeuralReranker()
    return _reranker


async def rerank_chunks(
    query: str,
    chunks: List[Dict[str, Any]],
    top_k: int = 5
) -> List[RankedChunk]:
    """
    Convenience function to rerank chunks.

    Usage:
        results = await rerank_chunks(query, chunks, top_k=5)
        for r in results:
            print(f"Score: {r.rerank_score:.3f} - {r.content[:100]}")
    """
    reranker = get_reranker()
    return await reranker.rerank(query, chunks, top_k)


# =============================================================================
# ENSEMBLE RERANKER SINGLETON
# =============================================================================

_ensemble_reranker: Optional[EnsembleReranker] = None
_ensemble_config: Optional[EnsembleConfig] = None


def get_ensemble_reranker(
    config: Optional[EnsembleConfig] = None,
    weights_file: Optional[str] = None
) -> EnsembleReranker:
    """
    Get or create the global ensemble reranker instance.

    Args:
        config: Optional config to use (only on first call)
        weights_file: Path to persist learned weights

    Returns:
        EnsembleReranker instance
    """
    global _ensemble_reranker, _ensemble_config

    if _ensemble_reranker is None:
        # Use provided config or create default
        if config:
            _ensemble_config = config
        else:
            _ensemble_config = EnsembleConfig(
                weights_file=weights_file or os.getenv('ENSEMBLE_WEIGHTS_FILE')
            )
        _ensemble_reranker = EnsembleReranker(config=_ensemble_config)

    return _ensemble_reranker


async def ensemble_rerank_chunks(
    query: str,
    chunks: List[Dict[str, Any]],
    top_k: int = 10,
    return_details: bool = False
) -> List[RankedChunk]:
    """
    Convenience function for ensemble reranking.

    Combines cross-encoder, BM25, and other signals for better relevance.

    Usage:
        results = await ensemble_rerank_chunks(query, chunks, top_k=10, return_details=True)
        for r in results:
            print(f"Score: {r.rerank_score:.3f}")
            if r.ensemble_details:
                print(f"  CE: {r.ensemble_details.cross_encoder_score:.3f}")
                print(f"  BM25: {r.ensemble_details.bm25_score:.3f}")
    """
    reranker = get_ensemble_reranker()
    return await reranker.rerank(query, chunks, top_k, return_details)


def configure_ensemble_weights(
    cross_encoder_weight: float = 0.45,
    bm25_weight: float = 0.25,
    original_score_weight: float = 0.15,
    filename_weight: float = 0.15,
    enable_learning: bool = True
) -> EnsembleConfig:
    """
    Configure ensemble reranking weights.

    Call this before first use of get_ensemble_reranker() to customize weights.

    Args:
        cross_encoder_weight: Weight for neural semantic scoring (0-1)
        bm25_weight: Weight for BM25 lexical scoring (0-1)
        original_score_weight: Weight for original embedding score (0-1)
        filename_weight: Weight for filename matching (0-1)
        enable_learning: Enable weight learning from feedback

    Returns:
        EnsembleConfig with the specified weights (normalized)
    """
    config = EnsembleConfig(
        cross_encoder_weight=cross_encoder_weight,
        bm25_weight=bm25_weight,
        original_score_weight=original_score_weight,
        filename_weight=filename_weight,
        enable_learning=enable_learning
    )

    # Force re-creation of reranker with new config
    global _ensemble_reranker, _ensemble_config
    _ensemble_config = config
    _ensemble_reranker = None

    return config


# =============================================================================
# COLBERT RERANKER (Optional)
# =============================================================================

# Try to import ColBERT
try:
    from colbert.infra import ColBERTConfig
    from colbert.modeling.checkpoint import Checkpoint
    COLBERT_AVAILABLE = True
except ImportError:
    COLBERT_AVAILABLE = False


class ColBERTReranker:
    """
    ColBERT-based late interaction reranker.

    Uses token-level MaxSim scoring for fine-grained query-document matching.
    Requires: pip install colbert-ai
    """

    def __init__(
        self,
        model_name: str = "colbert-ir/colbertv2.0",
        max_length: int = 512,
    ):
        self.model_name = model_name
        self.max_length = max_length
        self._checkpoint = None

    def _load_model(self):
        """Lazy-load ColBERT checkpoint"""
        if self._checkpoint is None:
            if not COLBERT_AVAILABLE:
                raise RuntimeError("colbert-ai not installed. pip install colbert-ai")
            config = ColBERTConfig(
                doc_maxlen=self.max_length,
                query_maxlen=128,
            )
            self._checkpoint = Checkpoint(self.model_name, colbert_config=config)
            logger.info(f"ColBERT checkpoint loaded: {self.model_name}")

    async def rerank(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        top_k: int = 10,
    ) -> List[RankedChunk]:
        """
        Rerank chunks using ColBERT late interaction scoring.

        Args:
            query: User query
            chunks: List of chunks with 'content' key
            top_k: Number of results to return

        Returns:
            List of RankedChunk sorted by ColBERT score
        """
        if not chunks:
            return []

        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, self._score_chunks, query, chunks
        )

        # Sort by score descending and take top_k
        results.sort(key=lambda x: x.rerank_score or 0, reverse=True)
        return results[:top_k]

    def _score_chunks(
        self, query: str, chunks: List[Dict[str, Any]]
    ) -> List[RankedChunk]:
        """Score chunks using ColBERT MaxSim (blocking)"""
        self._load_model()

        documents = [c.get("content", "") for c in chunks]
        scores = self._checkpoint.queryFromText(
            [query], bsize=32, to_cpu=True
        )

        results = []
        for i, chunk in enumerate(chunks):
            score = float(scores[0][i]) if i < len(scores[0]) else 0.0
            results.append(RankedChunk(
                chunk_id=chunk.get("chunk_id", chunk.get("id", i)),
                document_id=chunk.get("document_id", 0),
                content=chunk.get("content", ""),
                original_score=chunk.get("score", 0.0),
                rerank_score=score,
                metadata=chunk.get("metadata", {}),
            ))

        return results


# =============================================================================
# RERANKER FACTORY
# =============================================================================

RERANKER_TYPE = os.getenv("RERANKER_TYPE", "ensemble")


def get_reranker_by_type(reranker_type: Optional[str] = None):
    """
    Factory function to get the appropriate reranker by type.

    Args:
        reranker_type: One of 'ensemble', 'colbert', 'neural', 'cutting_edge'
                       Defaults to RERANKER_TYPE env var

    Returns:
        Reranker instance
    """
    rtype = (reranker_type or RERANKER_TYPE).lower()

    if rtype == "colbert":
        if not COLBERT_AVAILABLE:
            logger.warning("ColBERT not available, falling back to ensemble")
            return get_ensemble_reranker()
        return ColBERTReranker()
    elif rtype == "neural":
        return get_reranker()
    elif rtype == "cutting_edge":
        return get_cutting_edge_reranker()
    else:  # default: ensemble
        return get_ensemble_reranker()

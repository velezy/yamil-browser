"""
Synonym Expander Module
========================

Expands queries using embedding-based semantic similarity to find synonyms
and related terms. This improves retrieval recall by matching documents
that use different terminology for the same concepts.

Unlike traditional thesaurus approaches, this uses embedding similarity
to find contextually relevant synonyms based on actual usage patterns.
"""

import logging
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class SynonymResult:
    """Result of synonym expansion for a term"""
    original_term: str
    synonyms: List[str] = field(default_factory=list)
    similarity_scores: Dict[str, float] = field(default_factory=dict)


@dataclass
class ExpandedQuery:
    """Result of expanding a full query"""
    original_query: str
    expanded_terms: List[SynonymResult] = field(default_factory=list)
    expanded_queries: List[str] = field(default_factory=list)


class SynonymExpander:
    """
    Embedding-based synonym expansion for improved retrieval.

    Uses semantic similarity between word embeddings to find synonyms
    and related terms, providing better coverage than static thesauruses.
    """

    # Common technical vocabulary for computer science / AI domain
    # Maps terms to their semantic equivalents
    DOMAIN_SYNONYMS: Dict[str, List[str]] = {
        # Programming
        "function": ["method", "procedure", "routine", "subroutine"],
        "variable": ["parameter", "argument", "field", "attribute"],
        "class": ["type", "object", "struct", "model"],
        "array": ["list", "collection", "sequence", "vector"],
        "dictionary": ["map", "hash", "hashmap", "object", "dict"],
        "loop": ["iteration", "cycle", "repetition"],
        "error": ["exception", "fault", "bug", "issue", "problem"],
        "debug": ["troubleshoot", "diagnose", "fix"],

        # Database
        "database": ["db", "datastore", "repository", "storage"],
        "query": ["request", "search", "lookup", "fetch"],
        "table": ["collection", "relation", "entity"],
        "index": ["key", "lookup", "hash"],
        "join": ["merge", "combine", "link"],

        # AI/ML
        "model": ["network", "classifier", "predictor", "algorithm"],
        "train": ["fit", "learn", "optimize"],
        "predict": ["infer", "forecast", "estimate", "classify"],
        "embedding": ["vector", "representation", "encoding"],
        "accuracy": ["precision", "performance", "score"],
        "loss": ["error", "cost", "objective"],

        # Web
        "api": ["endpoint", "service", "interface", "rest"],
        "request": ["call", "fetch", "query"],
        "response": ["reply", "result", "output"],
        "authenticate": ["login", "authorize", "verify"],
        "cache": ["store", "buffer", "memoize"],

        # General
        "create": ["make", "generate", "build", "construct", "add"],
        "delete": ["remove", "drop", "destroy", "clear"],
        "update": ["modify", "change", "edit", "alter"],
        "get": ["fetch", "retrieve", "obtain", "read"],
        "set": ["assign", "configure", "define", "establish"],
        "optimize": ["improve", "enhance", "tune", "speed up"],
        "analyze": ["examine", "inspect", "evaluate", "assess"],
    }

    def __init__(
        self,
        embedder: Any = None,
        similarity_threshold: float = 0.7,
        max_synonyms_per_term: int = 3,
        use_domain_synonyms: bool = True
    ):
        """
        Initialize synonym expander.

        Args:
            embedder: Embedding model with encode() method
            similarity_threshold: Minimum similarity for synonym candidates
            max_synonyms_per_term: Maximum synonyms to return per term
            use_domain_synonyms: Whether to use built-in domain synonyms
        """
        self.embedder = embedder
        self.similarity_threshold = similarity_threshold
        self.max_synonyms_per_term = max_synonyms_per_term
        self.use_domain_synonyms = use_domain_synonyms

        # Cache for term embeddings
        self._embedding_cache: Dict[str, np.ndarray] = {}

        # Build reverse lookup for domain synonyms
        self._reverse_synonyms: Dict[str, str] = {}
        if use_domain_synonyms:
            for main_term, synonyms in self.DOMAIN_SYNONYMS.items():
                for syn in synonyms:
                    self._reverse_synonyms[syn.lower()] = main_term

    def _get_embedding(self, text: str) -> Optional[np.ndarray]:
        """Get embedding for a text, using cache when available."""
        if text in self._embedding_cache:
            return self._embedding_cache[text]

        if self.embedder is None:
            return None

        try:
            embedding = self.embedder.encode(text, normalize_embeddings=True)
            self._embedding_cache[text] = embedding
            return embedding
        except Exception as e:
            logger.warning(f"Failed to get embedding for '{text}': {e}")
            return None

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors."""
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

    def expand_term(self, term: str) -> SynonymResult:
        """
        Find synonyms for a single term.

        Args:
            term: The term to expand

        Returns:
            SynonymResult with synonyms and scores
        """
        result = SynonymResult(original_term=term)
        term_lower = term.lower()

        # First check domain synonyms
        if self.use_domain_synonyms:
            if term_lower in self.DOMAIN_SYNONYMS:
                synonyms = self.DOMAIN_SYNONYMS[term_lower][:self.max_synonyms_per_term]
                result.synonyms = synonyms
                result.similarity_scores = {s: 1.0 for s in synonyms}
                logger.debug(f"Found domain synonyms for '{term}': {synonyms}")
                return result

            # Check reverse lookup
            if term_lower in self._reverse_synonyms:
                main_term = self._reverse_synonyms[term_lower]
                all_synonyms = [main_term] + [
                    s for s in self.DOMAIN_SYNONYMS[main_term]
                    if s.lower() != term_lower
                ]
                synonyms = all_synonyms[:self.max_synonyms_per_term]
                result.synonyms = synonyms
                result.similarity_scores = {s: 1.0 for s in synonyms}
                logger.debug(f"Found reverse domain synonyms for '{term}': {synonyms}")
                return result

        # Fall back to embedding-based similarity if embedder available
        if self.embedder is not None:
            term_embedding = self._get_embedding(term)
            if term_embedding is not None:
                # Check against all domain terms
                candidates = []
                for main_term, synonyms in self.DOMAIN_SYNONYMS.items():
                    for candidate in [main_term] + synonyms:
                        if candidate.lower() == term_lower:
                            continue
                        cand_embedding = self._get_embedding(candidate)
                        if cand_embedding is not None:
                            sim = self._cosine_similarity(term_embedding, cand_embedding)
                            if sim >= self.similarity_threshold:
                                candidates.append((candidate, sim))

                # Sort by similarity and take top N
                candidates.sort(key=lambda x: x[1], reverse=True)
                for candidate, sim in candidates[:self.max_synonyms_per_term]:
                    result.synonyms.append(candidate)
                    result.similarity_scores[candidate] = sim

        return result

    def expand_query(
        self,
        query: str,
        expand_all_terms: bool = False
    ) -> ExpandedQuery:
        """
        Expand a query by finding synonyms for key terms.

        Args:
            query: The query to expand
            expand_all_terms: If True, expand all terms; otherwise only nouns/verbs

        Returns:
            ExpandedQuery with expanded terms and alternative queries
        """
        result = ExpandedQuery(original_query=query)

        # Simple tokenization (split on whitespace and punctuation)
        import re
        tokens = re.findall(r'\b\w+\b', query.lower())

        # Filter stopwords
        stopwords = {
            'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'been',
            'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'could', 'should', 'may', 'might', 'must', 'shall',
            'can', 'need', 'to', 'of', 'in', 'for', 'on', 'with', 'at',
            'by', 'from', 'as', 'into', 'through', 'during', 'before',
            'after', 'above', 'below', 'between', 'under', 'again',
            'further', 'then', 'once', 'here', 'there', 'when', 'where',
            'why', 'how', 'all', 'each', 'few', 'more', 'most', 'other',
            'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
            'so', 'than', 'too', 'very', 'just', 'and', 'but', 'if', 'or',
            'because', 'until', 'while', 'what', 'which', 'who', 'whom',
            'this', 'that', 'these', 'those', 'am', 'i', 'you', 'he', 'she',
            'it', 'we', 'they', 'my', 'your', 'his', 'her', 'its', 'our',
            'their', 'me', 'him', 'us', 'them'
        }

        # Expand meaningful terms
        expanded_terms: List[SynonymResult] = []
        for token in tokens:
            if token in stopwords or len(token) < 2:
                continue

            term_result = self.expand_term(token)
            if term_result.synonyms:
                expanded_terms.append(term_result)

        result.expanded_terms = expanded_terms

        # Generate expanded query variants
        if expanded_terms:
            # Create query variants by substituting synonyms
            expanded_queries: Set[str] = {query}

            for term_result in expanded_terms:
                new_queries: Set[str] = set()
                for eq in expanded_queries:
                    for synonym in term_result.synonyms:
                        # Case-insensitive replacement
                        import re
                        pattern = re.compile(re.escape(term_result.original_term), re.IGNORECASE)
                        new_query = pattern.sub(synonym, eq)
                        if new_query != eq:
                            new_queries.add(new_query)
                expanded_queries.update(new_queries)

            # Remove original and limit to reasonable number
            expanded_queries.discard(query)
            result.expanded_queries = list(expanded_queries)[:5]

        return result

    async def expand_query_async(
        self,
        query: str,
        expand_all_terms: bool = False
    ) -> ExpandedQuery:
        """Async wrapper for expand_query (for consistency with other async modules)."""
        return self.expand_query(query, expand_all_terms)


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

_synonym_expander: Optional[SynonymExpander] = None


def get_synonym_expander(
    embedder: Any = None,
    similarity_threshold: float = 0.7,
    max_synonyms_per_term: int = 3
) -> SynonymExpander:
    """Get or create the global synonym expander instance."""
    global _synonym_expander
    if _synonym_expander is None:
        _synonym_expander = SynonymExpander(
            embedder=embedder,
            similarity_threshold=similarity_threshold,
            max_synonyms_per_term=max_synonyms_per_term
        )
    return _synonym_expander


async def expand_synonyms(
    query: str,
    embedder: Any = None
) -> ExpandedQuery:
    """
    Convenience function to expand synonyms in a query.

    Usage:
        expanded = await expand_synonyms("How to optimize database queries?")
        for eq in expanded.expanded_queries:
            # Search with each variant
    """
    expander = get_synonym_expander(embedder=embedder)
    return await expander.expand_query_async(query)

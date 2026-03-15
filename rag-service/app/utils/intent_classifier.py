"""
T.A.L.O.S. Query Intent Classifier
Classify user queries to optimize retrieval strategy

Features:
- Intent classification (factual, analytical, comparison, etc.)
- Query complexity scoring
- Retrieval strategy recommendation
- Query expansion for better recall
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# INTENT TYPES
# =============================================================================

class QueryIntent(str, Enum):
    """Types of query intents"""
    FACTUAL = "factual"           # "What is X?", "Who founded X?"
    DEFINITION = "definition"     # "Define X", "What does X mean?"
    PROCEDURAL = "procedural"     # "How do I X?", "Steps to X"
    COMPARISON = "comparison"     # "Compare X and Y", "Difference between"
    ANALYTICAL = "analytical"     # "Why does X happen?", "Analyze X"
    SUMMARY = "summary"           # "Summarize X", "Overview of X"
    LIST = "list"                 # "List all X", "What are the X?"
    OPINION = "opinion"           # "What do you think about X?"
    CLARIFICATION = "clarification"  # Follow-up questions
    NAVIGATION = "navigation"     # "Where can I find X?"
    UNKNOWN = "unknown"


class QueryComplexity(str, Enum):
    """Query complexity levels"""
    SIMPLE = "simple"       # Single concept, direct answer
    MODERATE = "moderate"   # Multiple concepts, needs context
    COMPLEX = "complex"     # Deep analysis, multiple sources


class RetrievalStrategy(str, Enum):
    """Recommended retrieval strategy"""
    PRECISE = "precise"         # High precision, fewer results
    BROAD = "broad"             # High recall, more results
    HYBRID = "hybrid"           # Balance precision and recall
    MULTI_HOP = "multi_hop"     # Chain multiple searches
    AGGREGATE = "aggregate"     # Combine from multiple docs


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class IntentResult:
    """Result of intent classification"""
    primary_intent: QueryIntent
    secondary_intent: Optional[QueryIntent]
    confidence: float
    complexity: QueryComplexity
    strategy: RetrievalStrategy
    query_type_scores: Dict[QueryIntent, float]
    keywords: List[str]
    entities: List[str]
    expanded_queries: List[str]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "primary_intent": self.primary_intent.value,
            "secondary_intent": self.secondary_intent.value if self.secondary_intent else None,
            "confidence": self.confidence,
            "complexity": self.complexity.value,
            "strategy": self.strategy.value,
            "keywords": self.keywords,
            "entities": self.entities,
            "expanded_queries": self.expanded_queries,
            "metadata": self.metadata
        }


# =============================================================================
# INTENT PATTERNS
# =============================================================================

INTENT_PATTERNS = {
    QueryIntent.FACTUAL: [
        r"^what is\b",
        r"^who (is|was|are|were)\b",
        r"^when (is|was|did)\b",
        r"^where (is|was|are|were)\b",
        r"^which\b",
        r"\bfact(s)?\b",
    ],
    QueryIntent.DEFINITION: [
        r"^define\b",
        r"\bdefinition of\b",
        r"\bmeaning of\b",
        r"^what does .+ mean",
        r"\bexplain (the term|what)\b",
    ],
    QueryIntent.PROCEDURAL: [
        r"^how (do|can|to|should)\b",
        r"\bstep(s)? (to|for)\b",
        r"\bprocess (of|for)\b",
        r"\bprocedure\b",
        r"\binstruction(s)?\b",
        r"\btutorial\b",
        r"\bguide (to|for)\b",
    ],
    QueryIntent.COMPARISON: [
        r"\bcompare\b",
        r"\bcomparison\b",
        r"\bdifference(s)? between\b",
        r"\bvs\.?\b",
        r"\bversus\b",
        r"\bbetter than\b",
        r"\bpros and cons\b",
    ],
    QueryIntent.ANALYTICAL: [
        r"^why\b",
        r"\banalyze\b",
        r"\banalysis\b",
        r"\bexplain why\b",
        r"\breason(s)? (for|why)\b",
        r"\bcause(s)?\b",
        r"\bimpact\b",
        r"\bimplication(s)?\b",
    ],
    QueryIntent.SUMMARY: [
        r"\bsummar(y|ize)\b",
        r"\boverview\b",
        r"\bbrief\b",
        r"\bhighlight(s)?\b",
        r"\bkey points\b",
        r"\bmain idea(s)?\b",
        r"\btl;?dr\b",
    ],
    QueryIntent.LIST: [
        r"^list\b",
        r"\ball (the|of the)\b",
        r"\bevery\b",
        r"\beach\b",
        r"^what are (the|all)\b",
        r"\benumerate\b",
        r"\bexamples of\b",
    ],
    QueryIntent.OPINION: [
        r"\bopinion\b",
        r"\bthink about\b",
        r"\bfeel about\b",
        r"\bshould (I|we)\b",
        r"\brecommend\b",
        r"\badvice\b",
        r"\bsuggestion\b",
    ],
    QueryIntent.CLARIFICATION: [
        r"^can you (explain|clarify)\b",
        r"\bmore (details|info|information)\b",
        r"\belaborate\b",
        r"\bwhat do you mean\b",
        r"\bcould you\b",
    ],
    QueryIntent.NAVIGATION: [
        r"\bwhere (can|do) (I|we) find\b",
        r"\blocate\b",
        r"\bfind (the|a)\b",
        r"\bshow me\b",
        r"\bpoint me to\b",
    ],
}

# Complexity indicators
COMPLEXITY_INDICATORS = {
    QueryComplexity.COMPLEX: [
        r"\band\b.*\band\b",  # Multiple "and"s
        r"\brelationship between\b",
        r"\bhow does .+ affect\b",
        r"\bimplications of\b",
        r"\bconsequences\b",
        r"\bmultiple\b",
        r"\bvarious\b",
        r"\bcomprehensive\b",
        r"\bin-depth\b",
        r"\bdetailed\b",
    ],
    QueryComplexity.MODERATE: [
        r"\bexplain\b",
        r"\bdescribe\b",
        r"\bcontext\b",
        r"\bexample(s)?\b",
        r"\bspecific(ally)?\b",
    ],
}


# =============================================================================
# INTENT CLASSIFIER
# =============================================================================

class QueryIntentClassifier:
    """
    Classifies user queries to optimize retrieval strategy.

    Uses pattern matching and heuristics for fast, reliable classification
    without requiring ML models.
    """

    def __init__(self, embedder=None):
        """
        Args:
            embedder: Optional embedder for semantic similarity (SentenceTransformer)
        """
        self.embedder = embedder
        self._compile_patterns()

    def _compile_patterns(self):
        """Pre-compile regex patterns for performance"""
        self.compiled_patterns = {
            intent: [re.compile(p, re.IGNORECASE) for p in patterns]
            for intent, patterns in INTENT_PATTERNS.items()
        }
        self.complexity_patterns = {
            level: [re.compile(p, re.IGNORECASE) for p in patterns]
            for level, patterns in COMPLEXITY_INDICATORS.items()
        }

    def classify(self, query: str) -> IntentResult:
        """
        Classify a query's intent.

        Args:
            query: User query string

        Returns:
            IntentResult with classification details
        """
        query_lower = query.lower().strip()

        # Score each intent
        intent_scores = self._score_intents(query_lower)

        # Get primary and secondary intents
        sorted_intents = sorted(intent_scores.items(), key=lambda x: x[1], reverse=True)
        primary_intent = sorted_intents[0][0] if sorted_intents[0][1] > 0 else QueryIntent.UNKNOWN
        secondary_intent = sorted_intents[1][0] if len(sorted_intents) > 1 and sorted_intents[1][1] > 0.3 else None

        # Calculate confidence
        confidence = sorted_intents[0][1] if sorted_intents else 0.0
        if confidence == 0:
            confidence = 0.5  # Default confidence for unknown

        # Determine complexity
        complexity = self._assess_complexity(query_lower)

        # Determine retrieval strategy
        strategy = self._recommend_strategy(primary_intent, complexity)

        # Extract keywords and entities
        keywords = self._extract_keywords(query)
        entities = self._extract_entities(query)

        # Generate expanded queries
        expanded = self._expand_query(query, primary_intent, keywords)

        return IntentResult(
            primary_intent=primary_intent,
            secondary_intent=secondary_intent,
            confidence=confidence,
            complexity=complexity,
            strategy=strategy,
            query_type_scores={k: v for k, v in intent_scores.items() if v > 0},
            keywords=keywords,
            entities=entities,
            expanded_queries=expanded,
            metadata={
                "word_count": len(query.split()),
                "has_question_mark": "?" in query,
                "is_question": query_lower.startswith(("what", "who", "where", "when", "why", "how", "which"))
            }
        )

    def _score_intents(self, query: str) -> Dict[QueryIntent, float]:
        """Score each intent based on pattern matches"""
        scores = {intent: 0.0 for intent in QueryIntent}

        for intent, patterns in self.compiled_patterns.items():
            match_count = sum(1 for p in patterns if p.search(query))
            if match_count > 0:
                # Normalize score (max 1.0)
                scores[intent] = min(match_count / len(patterns) * 2, 1.0)

        return scores

    def _assess_complexity(self, query: str) -> QueryComplexity:
        """Assess query complexity"""
        word_count = len(query.split())

        # Check complex patterns
        for pattern in self.complexity_patterns[QueryComplexity.COMPLEX]:
            if pattern.search(query):
                return QueryComplexity.COMPLEX

        # Check moderate patterns
        for pattern in self.complexity_patterns[QueryComplexity.MODERATE]:
            if pattern.search(query):
                return QueryComplexity.MODERATE

        # Word count heuristics
        if word_count > 20:
            return QueryComplexity.COMPLEX
        elif word_count > 10:
            return QueryComplexity.MODERATE

        return QueryComplexity.SIMPLE

    def _recommend_strategy(
        self,
        intent: QueryIntent,
        complexity: QueryComplexity
    ) -> RetrievalStrategy:
        """Recommend retrieval strategy based on intent and complexity"""
        strategy_map = {
            QueryIntent.FACTUAL: RetrievalStrategy.PRECISE,
            QueryIntent.DEFINITION: RetrievalStrategy.PRECISE,
            QueryIntent.PROCEDURAL: RetrievalStrategy.HYBRID,
            QueryIntent.COMPARISON: RetrievalStrategy.MULTI_HOP,
            QueryIntent.ANALYTICAL: RetrievalStrategy.BROAD,
            QueryIntent.SUMMARY: RetrievalStrategy.AGGREGATE,
            QueryIntent.LIST: RetrievalStrategy.BROAD,
            QueryIntent.OPINION: RetrievalStrategy.HYBRID,
            QueryIntent.CLARIFICATION: RetrievalStrategy.PRECISE,
            QueryIntent.NAVIGATION: RetrievalStrategy.PRECISE,
            QueryIntent.UNKNOWN: RetrievalStrategy.HYBRID,
        }

        base_strategy = strategy_map.get(intent, RetrievalStrategy.HYBRID)

        # Adjust for complexity
        if complexity == QueryComplexity.COMPLEX:
            if base_strategy == RetrievalStrategy.PRECISE:
                return RetrievalStrategy.HYBRID
            elif base_strategy == RetrievalStrategy.HYBRID:
                return RetrievalStrategy.MULTI_HOP

        return base_strategy

    def _extract_keywords(self, query: str) -> List[str]:
        """Extract important keywords from query"""
        # Remove common stop words
        stop_words = {
            "a", "an", "the", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "must", "shall",
            "can", "need", "dare", "ought", "used", "to", "of", "in",
            "for", "on", "with", "at", "by", "from", "as", "into",
            "through", "during", "before", "after", "above", "below",
            "between", "under", "again", "further", "then", "once",
            "here", "there", "when", "where", "why", "how", "all",
            "each", "few", "more", "most", "other", "some", "such",
            "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "just", "also", "now", "what", "which",
            "who", "whom", "this", "that", "these", "those", "am",
            "and", "but", "if", "or", "because", "until", "while"
        }

        # Tokenize and filter
        words = re.findall(r'\b[a-zA-Z]{2,}\b', query.lower())
        keywords = [w for w in words if w not in stop_words]

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for k in keywords:
            if k not in seen:
                seen.add(k)
                unique.append(k)

        return unique[:10]  # Limit to 10 keywords

    def _extract_entities(self, query: str) -> List[str]:
        """Extract potential named entities (simple heuristic)"""
        # Find capitalized words (potential proper nouns)
        entities = re.findall(r'\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\b', query)

        # Filter out sentence starters
        if entities and query.startswith(entities[0]):
            # Check if it's just a sentence starter
            if entities[0].lower() in ["what", "who", "where", "when", "why", "how", "which", "can", "could", "would", "should", "is", "are", "do", "does"]:
                entities = entities[1:]

        return list(set(entities))[:5]

    def _expand_query(
        self,
        query: str,
        intent: QueryIntent,
        keywords: List[str]
    ) -> List[str]:
        """Generate expanded queries for better recall"""
        expanded = []

        # Intent-based expansions
        if intent == QueryIntent.DEFINITION:
            for kw in keywords[:3]:
                expanded.append(f"{kw} definition meaning")
                expanded.append(f"what is {kw}")

        elif intent == QueryIntent.PROCEDURAL:
            for kw in keywords[:3]:
                expanded.append(f"how to {kw}")
                expanded.append(f"{kw} steps guide")

        elif intent == QueryIntent.COMPARISON:
            if len(keywords) >= 2:
                expanded.append(f"{keywords[0]} vs {keywords[1]}")
                expanded.append(f"difference {keywords[0]} {keywords[1]}")

        elif intent == QueryIntent.LIST:
            for kw in keywords[:2]:
                expanded.append(f"list of {kw}")
                expanded.append(f"types of {kw}")

        # Keyword-based expansion (always include)
        if keywords:
            expanded.append(" ".join(keywords[:5]))

        return expanded[:5]  # Limit expansions


# =============================================================================
# RETRIEVAL CONFIG BUILDER
# =============================================================================

def build_retrieval_config(intent_result: IntentResult) -> Dict[str, Any]:
    """
    Build retrieval configuration based on intent classification.

    Returns configuration for the RAG search endpoint.
    """
    strategy = intent_result.strategy

    # Base configuration
    config = {
        "top_k": 10,
        "min_score": 0.5,
        "use_hybrid": True,
        "semantic_weight": 0.7,
        "keyword_weight": 0.3,
        "rerank": True,
        "expand_query": len(intent_result.expanded_queries) > 0
    }

    # Adjust based on strategy
    if strategy == RetrievalStrategy.PRECISE:
        config.update({
            "top_k": 5,
            "min_score": 0.7,
            "semantic_weight": 0.8,
            "keyword_weight": 0.2
        })

    elif strategy == RetrievalStrategy.BROAD:
        config.update({
            "top_k": 20,
            "min_score": 0.4,
            "semantic_weight": 0.6,
            "keyword_weight": 0.4
        })

    elif strategy == RetrievalStrategy.MULTI_HOP:
        config.update({
            "top_k": 15,
            "min_score": 0.5,
            "multi_hop": True,
            "hop_limit": 2
        })

    elif strategy == RetrievalStrategy.AGGREGATE:
        config.update({
            "top_k": 25,
            "min_score": 0.4,
            "aggregate_sources": True,
            "max_sources": 5
        })

    # Add expanded queries if available
    if intent_result.expanded_queries:
        config["expanded_queries"] = intent_result.expanded_queries

    return config


# =============================================================================
# QUERY WEIGHT LEARNER - Adaptive Weight Learning
# =============================================================================

@dataclass
class HybridWeights:
    """Weights for hybrid search scoring"""
    semantic_weight: float = 0.7
    keyword_weight: float = 0.3
    bm25_weight: float = 0.0  # For ensemble reranker
    filename_weight: float = 0.0  # For ensemble reranker

    def __post_init__(self):
        """Normalize weights"""
        total = self.semantic_weight + self.keyword_weight
        if total > 0 and abs(total - 1.0) > 0.01:
            self.semantic_weight /= total
            self.keyword_weight /= total

    def to_dict(self) -> Dict[str, float]:
        return {
            "semantic_weight": self.semantic_weight,
            "keyword_weight": self.keyword_weight,
            "bm25_weight": self.bm25_weight,
            "filename_weight": self.filename_weight
        }


class QueryWeightLearner:
    """
    Learns optimal hybrid search weights per query intent type.

    Each query type (factual, procedural, comparison, etc.) may benefit
    from different semantic/keyword weight ratios. This class:
    1. Maintains per-intent weight profiles
    2. Learns from user feedback (clicks, thumbs up/down)
    3. Persists learned weights to disk
    4. Provides optimal weights for any query

    Usage:
        learner = get_weight_learner()
        weights = learner.get_weights_for_query(query)
        # Use weights.semantic_weight, weights.keyword_weight in search
    """

    # Default weights per intent type (based on research/intuition)
    DEFAULT_WEIGHTS: Dict[QueryIntent, HybridWeights] = {
        # Factual queries: higher semantic (understand meaning)
        QueryIntent.FACTUAL: HybridWeights(semantic_weight=0.75, keyword_weight=0.25),

        # Definition queries: balanced (need exact terms + meaning)
        QueryIntent.DEFINITION: HybridWeights(semantic_weight=0.65, keyword_weight=0.35),

        # Procedural: higher semantic (understand steps)
        QueryIntent.PROCEDURAL: HybridWeights(semantic_weight=0.70, keyword_weight=0.30),

        # Comparison: high semantic (understand both concepts)
        QueryIntent.COMPARISON: HybridWeights(semantic_weight=0.80, keyword_weight=0.20),

        # Analytical: very high semantic (deep understanding)
        QueryIntent.ANALYTICAL: HybridWeights(semantic_weight=0.85, keyword_weight=0.15),

        # Summary: high semantic (comprehensive understanding)
        QueryIntent.SUMMARY: HybridWeights(semantic_weight=0.75, keyword_weight=0.25),

        # List queries: higher keyword (specific items)
        QueryIntent.LIST: HybridWeights(semantic_weight=0.55, keyword_weight=0.45),

        # Opinion: very high semantic (nuanced understanding)
        QueryIntent.OPINION: HybridWeights(semantic_weight=0.80, keyword_weight=0.20),

        # Clarification: balanced (context + specific terms)
        QueryIntent.CLARIFICATION: HybridWeights(semantic_weight=0.65, keyword_weight=0.35),

        # Navigation: higher keyword (specific names/paths)
        QueryIntent.NAVIGATION: HybridWeights(semantic_weight=0.50, keyword_weight=0.50),

        # Unknown: default balanced
        QueryIntent.UNKNOWN: HybridWeights(semantic_weight=0.70, keyword_weight=0.30),
    }

    def __init__(
        self,
        weights_file: Optional[str] = None,
        classifier: Optional['QueryIntentClassifier'] = None,
        learning_rate: float = 0.05,
        enable_learning: bool = True
    ):
        """
        Initialize weight learner.

        Args:
            weights_file: Path to persist learned weights (JSON)
            classifier: Intent classifier instance (will create if None)
            learning_rate: Rate for weight updates (0.01-0.1)
            enable_learning: Whether to update weights from feedback
        """
        self.weights_file = weights_file
        self._classifier = classifier
        self.learning_rate = learning_rate
        self.enable_learning = enable_learning

        # Per-intent learned weights (starts from defaults)
        self._weights: Dict[QueryIntent, HybridWeights] = {}
        for intent, default in self.DEFAULT_WEIGHTS.items():
            self._weights[intent] = HybridWeights(
                semantic_weight=default.semantic_weight,
                keyword_weight=default.keyword_weight
            )

        # Feedback history per intent
        self._feedback: Dict[QueryIntent, List[Dict[str, Any]]] = {
            intent: [] for intent in QueryIntent
        }
        self._max_feedback_per_intent = 500

        # Load persisted weights if available
        if weights_file:
            self._load_weights()

    @property
    def classifier(self) -> 'QueryIntentClassifier':
        """Get or create intent classifier"""
        if self._classifier is None:
            self._classifier = get_intent_classifier()
        return self._classifier

    def _load_weights(self):
        """Load weights from file"""
        import json
        import os

        if not self.weights_file or not os.path.exists(self.weights_file):
            return

        try:
            with open(self.weights_file, 'r') as f:
                data = json.load(f)

            for intent_str, weight_data in data.get('weights', {}).items():
                try:
                    intent = QueryIntent(intent_str)
                    self._weights[intent] = HybridWeights(
                        semantic_weight=weight_data.get('semantic_weight', 0.7),
                        keyword_weight=weight_data.get('keyword_weight', 0.3)
                    )
                except ValueError:
                    continue

            logger.info(f"Loaded learned weights from {self.weights_file}")

        except Exception as e:
            logger.warning(f"Failed to load weights: {e}")

    def _save_weights(self):
        """Save weights to file"""
        import json

        if not self.weights_file:
            return

        try:
            data = {
                'weights': {
                    intent.value: weights.to_dict()
                    for intent, weights in self._weights.items()
                },
                'feedback_counts': {
                    intent.value: len(fb_list)
                    for intent, fb_list in self._feedback.items()
                }
            }

            with open(self.weights_file, 'w') as f:
                json.dump(data, f, indent=2)

            logger.debug(f"Saved learned weights to {self.weights_file}")

        except Exception as e:
            logger.warning(f"Failed to save weights: {e}")

    def get_weights_for_intent(self, intent: QueryIntent) -> HybridWeights:
        """Get learned weights for a specific intent type"""
        return self._weights.get(intent, self.DEFAULT_WEIGHTS[QueryIntent.UNKNOWN])

    def get_weights_for_query(
        self,
        query: str,
        return_intent: bool = False
    ) -> HybridWeights | Tuple[HybridWeights, IntentResult]:
        """
        Get optimal weights for a query by classifying its intent.

        Args:
            query: The search query
            return_intent: If True, also return the intent classification

        Returns:
            HybridWeights (or tuple with IntentResult if return_intent=True)
        """
        # Classify query intent
        intent_result = self.classifier.classify(query)
        intent = intent_result.primary_intent

        # Get learned weights for this intent
        weights = self.get_weights_for_intent(intent)

        # Adjust based on confidence - blend toward default if low confidence
        if intent_result.confidence < 0.5:
            default = self.DEFAULT_WEIGHTS[QueryIntent.UNKNOWN]
            blend = intent_result.confidence * 2  # 0-1 range from 0-0.5 confidence
            weights = HybridWeights(
                semantic_weight=weights.semantic_weight * blend + default.semantic_weight * (1 - blend),
                keyword_weight=weights.keyword_weight * blend + default.keyword_weight * (1 - blend)
            )

        if return_intent:
            return weights, intent_result
        return weights

    def record_feedback(
        self,
        query: str,
        intent: QueryIntent,
        selected_rank: int,
        total_results: int,
        feedback_type: str = "click",  # click, thumbs_up, thumbs_down
        weights_used: Optional[HybridWeights] = None
    ):
        """
        Record user feedback for weight learning.

        Args:
            query: The search query
            intent: Classified intent of the query
            selected_rank: Rank of the result user selected (0-indexed)
            total_results: Total results shown
            feedback_type: Type of feedback
            weights_used: Weights that were used for this search
        """
        if not self.enable_learning:
            return

        # Calculate quality signal
        # If user selected top result, weights were good
        # If they selected lower, weights may need adjustment
        quality = 1.0 - (selected_rank / max(total_results, 1))

        # For thumbs down, invert quality
        if feedback_type == "thumbs_down":
            quality = -quality

        # Store feedback
        self._feedback[intent].append({
            'query': query[:100],  # Truncate for storage
            'selected_rank': selected_rank,
            'quality': quality,
            'weights_used': weights_used.to_dict() if weights_used else None,
            'feedback_type': feedback_type
        })

        # Trim old feedback
        if len(self._feedback[intent]) > self._max_feedback_per_intent:
            self._feedback[intent] = self._feedback[intent][-self._max_feedback_per_intent:]

    def update_weights(self, min_feedback: int = 20):
        """
        Update weights based on accumulated feedback.

        Uses a simple approach:
        - If users tend to select lower-ranked results, adjust weights
        - toward what those results would have scored higher with.

        Args:
            min_feedback: Minimum feedback samples before updating
        """
        if not self.enable_learning:
            return

        updated = False

        for intent, feedback_list in self._feedback.items():
            if len(feedback_list) < min_feedback:
                continue

            # Analyze recent feedback
            recent = feedback_list[-100:]  # Last 100

            # Calculate average quality signal
            avg_quality = sum(fb['quality'] for fb in recent) / len(recent)

            # If average quality is low, users aren't finding what they want
            # Try shifting weights slightly
            if avg_quality < 0.6:  # Threshold for "needs improvement"
                current = self._weights[intent]

                # Analyze which direction to shift
                # If keyword-heavy queries perform better, increase keyword weight
                keyword_bias_quality = sum(
                    fb['quality'] for fb in recent
                    if fb.get('weights_used', {}).get('keyword_weight', 0.3) > 0.35
                ) / max(sum(1 for fb in recent if fb.get('weights_used', {}).get('keyword_weight', 0.3) > 0.35), 1)

                semantic_bias_quality = sum(
                    fb['quality'] for fb in recent
                    if fb.get('weights_used', {}).get('semantic_weight', 0.7) > 0.75
                ) / max(sum(1 for fb in recent if fb.get('weights_used', {}).get('semantic_weight', 0.7) > 0.75), 1)

                # Shift toward better performing ratio
                if keyword_bias_quality > semantic_bias_quality + 0.1:
                    # Increase keyword weight
                    new_kw = min(current.keyword_weight + self.learning_rate, 0.6)
                    self._weights[intent] = HybridWeights(
                        semantic_weight=1.0 - new_kw,
                        keyword_weight=new_kw
                    )
                    updated = True
                    logger.info(f"Increased keyword_weight for {intent.value} to {new_kw:.2f}")

                elif semantic_bias_quality > keyword_bias_quality + 0.1:
                    # Increase semantic weight
                    new_sem = min(current.semantic_weight + self.learning_rate, 0.9)
                    self._weights[intent] = HybridWeights(
                        semantic_weight=new_sem,
                        keyword_weight=1.0 - new_sem
                    )
                    updated = True
                    logger.info(f"Increased semantic_weight for {intent.value} to {new_sem:.2f}")

        if updated:
            self._save_weights()

    def get_all_weights(self) -> Dict[str, Dict[str, float]]:
        """Get all current weights for debugging/monitoring"""
        return {
            intent.value: weights.to_dict()
            for intent, weights in self._weights.items()
        }

    def get_statistics(self) -> Dict[str, Any]:
        """Get learning statistics"""
        return {
            'weights': self.get_all_weights(),
            'feedback_counts': {
                intent.value: len(fb_list)
                for intent, fb_list in self._feedback.items()
            },
            'learning_enabled': self.enable_learning,
            'learning_rate': self.learning_rate
        }


# Singleton instance
_WEIGHT_LEARNER: Optional[QueryWeightLearner] = None


def get_weight_learner(
    weights_file: Optional[str] = None,
    enable_learning: bool = True
) -> QueryWeightLearner:
    """
    Get or create global weight learner instance.

    Args:
        weights_file: Path to persist weights (uses env var if not specified)
        enable_learning: Enable weight learning from feedback

    Returns:
        QueryWeightLearner instance
    """
    global _WEIGHT_LEARNER
    import os

    if _WEIGHT_LEARNER is None:
        file_path = weights_file or os.getenv('HYBRID_WEIGHTS_FILE')
        _WEIGHT_LEARNER = QueryWeightLearner(
            weights_file=file_path,
            enable_learning=enable_learning
        )

    return _WEIGHT_LEARNER


def get_optimal_weights(query: str) -> HybridWeights:
    """
    Convenience function to get optimal weights for a query.

    Usage:
        from services.rag.app.utils.intent_classifier import get_optimal_weights

        weights = get_optimal_weights("What is machine learning?")
        # weights.semantic_weight = 0.75 (factual query)
        # weights.keyword_weight = 0.25
    """
    learner = get_weight_learner()
    return learner.get_weights_for_query(query)


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_GLOBAL_CLASSIFIER: Optional[QueryIntentClassifier] = None


def get_intent_classifier(embedder=None) -> QueryIntentClassifier:
    """Get or create global intent classifier"""
    global _GLOBAL_CLASSIFIER

    if _GLOBAL_CLASSIFIER is None:
        _GLOBAL_CLASSIFIER = QueryIntentClassifier(embedder=embedder)

    return _GLOBAL_CLASSIFIER

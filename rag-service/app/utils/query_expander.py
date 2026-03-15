"""
Query Expander Module
======================

Unified orchestrator for query expansion combining:
- Synonym expansion (embedding-based)
- Paraphrase generation (LLM-based)

This complements existing HyDE and query decomposition to provide
comprehensive query understanding for improved retrieval recall.

Pipeline position:
    Query -> [Query Expansion] -> [Decomposition] -> [HyDE] -> [Embedding] -> Search
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field

from .synonym_expander import (
    SynonymExpander,
    ExpandedQuery as SynonymExpandedQuery,
    get_synonym_expander
)
from .paraphrase_generator import (
    ParaphraseGenerator,
    ParaphraseResult,
    get_paraphrase_generator
)

logger = logging.getLogger(__name__)


@dataclass
class ExpansionConfig:
    """Configuration for query expansion"""
    use_synonyms: bool = True
    use_paraphrases: bool = True
    max_synonym_queries: int = 3
    max_paraphrases: int = 3
    max_total_queries: int = 6  # Cap on total expanded queries
    deduplicate: bool = True
    include_original: bool = True


@dataclass
class QueryExpansionResult:
    """Result of unified query expansion"""
    original_query: str
    expanded_queries: List[str] = field(default_factory=list)
    synonym_expansions: Optional[SynonymExpandedQuery] = None
    paraphrase_results: Optional[ParaphraseResult] = None
    config_used: Optional[ExpansionConfig] = None
    expansion_metadata: Dict[str, Any] = field(default_factory=dict)


class QueryExpander:
    """
    Unified query expansion orchestrator.

    Combines multiple expansion strategies:
    1. Synonym expansion - fast, deterministic term substitution
    2. Paraphrase generation - LLM-generated alternative phrasings

    Results are deduplicated and ranked for optimal retrieval coverage.
    """

    def __init__(
        self,
        synonym_expander: Optional[SynonymExpander] = None,
        paraphrase_generator: Optional[ParaphraseGenerator] = None,
        embedder: Any = None,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        """
        Initialize query expander.

        Args:
            synonym_expander: Optional pre-configured synonym expander
            paraphrase_generator: Optional pre-configured paraphrase generator
            embedder: Embedding model for synonym similarity
            ollama_url: Ollama API URL for paraphrase generation
            ollama_model: LLM model for paraphrase generation
        """
        self.synonym_expander = synonym_expander or get_synonym_expander(embedder=embedder)
        self.paraphrase_generator = paraphrase_generator or get_paraphrase_generator(
            ollama_url=ollama_url,
            model=ollama_model
        )
        self.embedder = embedder

    async def expand(
        self,
        query: str,
        config: Optional[ExpansionConfig] = None
    ) -> QueryExpansionResult:
        """
        Expand a query using configured strategies.

        Args:
            query: The query to expand
            config: Expansion configuration (uses defaults if not provided)

        Returns:
            QueryExpansionResult with all expanded query variants
        """
        if config is None:
            config = ExpansionConfig()

        result = QueryExpansionResult(
            original_query=query,
            config_used=config
        )

        # Collect all expanded queries
        all_queries: Set[str] = set()
        if config.include_original:
            all_queries.add(query)

        # Run expansion strategies in parallel
        tasks = []

        if config.use_synonyms:
            tasks.append(self._expand_with_synonyms(query))

        if config.use_paraphrases:
            tasks.append(self._expand_with_paraphrases(query, config.max_paraphrases))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, expansion_result in enumerate(results):
                if isinstance(expansion_result, Exception):
                    logger.warning(f"Expansion task failed: {expansion_result}")
                    continue

                if isinstance(expansion_result, SynonymExpandedQuery):
                    result.synonym_expansions = expansion_result
                    for eq in expansion_result.expanded_queries[:config.max_synonym_queries]:
                        all_queries.add(eq)
                    result.expansion_metadata["synonym_count"] = len(expansion_result.expanded_queries)

                elif isinstance(expansion_result, ParaphraseResult):
                    result.paraphrase_results = expansion_result
                    for p in expansion_result.paraphrases[:config.max_paraphrases]:
                        all_queries.add(p)
                    result.expansion_metadata["paraphrase_count"] = len(expansion_result.paraphrases)

        # Convert to list and limit total
        expanded_list = list(all_queries)

        # Put original first if included
        if config.include_original and query in expanded_list:
            expanded_list.remove(query)
            expanded_list.insert(0, query)

        # Cap total queries
        result.expanded_queries = expanded_list[:config.max_total_queries]
        result.expansion_metadata["total_variants"] = len(result.expanded_queries)

        logger.info(f"Query expanded from 1 to {len(result.expanded_queries)} variants")

        return result

    async def _expand_with_synonyms(self, query: str) -> SynonymExpandedQuery:
        """Expand query using synonym substitution."""
        return await self.synonym_expander.expand_query_async(query)

    async def _expand_with_paraphrases(self, query: str, num: int) -> ParaphraseResult:
        """Expand query using LLM paraphrasing."""
        return await self.paraphrase_generator.generate_paraphrases(query, num)

    async def expand_for_search(
        self,
        query: str,
        use_synonyms: bool = True,
        use_paraphrases: bool = True,
        max_queries: int = 5
    ) -> List[str]:
        """
        Convenience method to get expanded queries ready for search.

        Args:
            query: The query to expand
            use_synonyms: Whether to use synonym expansion
            use_paraphrases: Whether to use paraphrase generation
            max_queries: Maximum number of queries to return

        Returns:
            List of query variants for searching
        """
        config = ExpansionConfig(
            use_synonyms=use_synonyms,
            use_paraphrases=use_paraphrases,
            max_total_queries=max_queries,
            include_original=True
        )

        result = await self.expand(query, config)
        return result.expanded_queries


# =============================================================================
# ADVANCED: CONTEXTUAL & USER-HISTORY AWARE EXPANSION
# =============================================================================

@dataclass
class ConversationContext:
    """Context from the current conversation for contextual expansion"""
    previous_queries: List[str] = field(default_factory=list)
    previous_responses: List[str] = field(default_factory=list)
    mentioned_entities: List[str] = field(default_factory=list)
    topic_keywords: List[str] = field(default_factory=list)
    session_id: Optional[str] = None


@dataclass
class UserQueryPattern:
    """Pattern learned from user's query history"""
    pattern_type: str  # e.g., "question_style", "domain_preference", "term_preference"
    value: str
    frequency: int = 1
    last_seen: Optional[str] = None
    confidence: float = 0.5


@dataclass
class ContextualExpansionResult:
    """Result of contextual query expansion"""
    original_query: str
    expanded_queries: List[str]
    context_terms_added: List[str] = field(default_factory=list)
    history_terms_added: List[str] = field(default_factory=list)
    expansion_reasoning: str = ""


class ContextualExpander:
    """
    Expands queries using conversation context.

    Provides:
    - Coreference resolution (pronouns -> entities)
    - Topic continuation (adding implicit context from conversation)
    - Entity disambiguation based on previous mentions
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.ollama_model = ollama_model
        self._entity_cache: Dict[str, List[str]] = {}  # session_id -> entities

    async def expand_with_context(
        self,
        query: str,
        context: ConversationContext,
        max_expansions: int = 3
    ) -> ContextualExpansionResult:
        """
        Expand query using conversation context.

        Args:
            query: The query to expand
            context: Conversation context
            max_expansions: Maximum expansion variants

        Returns:
            ContextualExpansionResult with context-aware expansions
        """
        result = ContextualExpansionResult(
            original_query=query,
            expanded_queries=[query]
        )

        # Step 1: Resolve coreferences (pronouns to entities)
        resolved_query, resolved_entities = await self._resolve_coreferences(
            query, context
        )
        if resolved_query != query:
            result.expanded_queries.append(resolved_query)
            result.context_terms_added.extend(resolved_entities)
            result.expansion_reasoning += f"Resolved coreferences: {resolved_entities}. "

        # Step 2: Add implicit topic context
        topic_expanded = await self._add_topic_context(
            resolved_query, context
        )
        if topic_expanded and topic_expanded not in result.expanded_queries:
            result.expanded_queries.append(topic_expanded)
            result.expansion_reasoning += "Added topic context. "

        # Step 3: Generate context-aware paraphrases
        if len(result.expanded_queries) < max_expansions:
            paraphrases = await self._generate_contextual_paraphrases(
                resolved_query,
                context,
                max_expansions - len(result.expanded_queries)
            )
            for p in paraphrases:
                if p not in result.expanded_queries:
                    result.expanded_queries.append(p)

        result.expanded_queries = result.expanded_queries[:max_expansions]
        return result

    async def _resolve_coreferences(
        self,
        query: str,
        context: ConversationContext
    ) -> tuple:
        """Resolve pronouns and references to specific entities"""

        # Quick check for pronouns
        pronouns = ['it', 'they', 'them', 'this', 'that', 'these', 'those', 'he', 'she', 'its']
        has_pronoun = any(f" {p} " in f" {query.lower()} " for p in pronouns)

        if not has_pronoun or not context.mentioned_entities:
            return query, []

        # Use LLM to resolve coreferences
        entities_str = ", ".join(context.mentioned_entities[-5:])  # Last 5 entities
        prev_queries_str = " | ".join(context.previous_queries[-3:])  # Last 3 queries

        prompt = f"""Given the conversation context, resolve any pronouns or references in the query.

Previous queries: {prev_queries_str}
Mentioned entities: {entities_str}
Current query: {query}

Return ONLY the resolved query with pronouns replaced by specific entities. If no resolution needed, return the original query.

Resolved query:"""

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": 100}
                    },
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        resolved = data.get("response", "").strip()
                        if resolved and resolved != query:
                            # Extract which entities were used
                            used_entities = [
                                e for e in context.mentioned_entities
                                if e.lower() in resolved.lower()
                            ]
                            return resolved, used_entities
        except Exception as e:
            logger.warning(f"Coreference resolution failed: {e}")

        return query, []

    async def _add_topic_context(
        self,
        query: str,
        context: ConversationContext
    ) -> Optional[str]:
        """Add implicit topic context from conversation"""

        if not context.topic_keywords:
            return None

        # Check if query is missing obvious context
        query_lower = query.lower()
        missing_topics = [
            t for t in context.topic_keywords[-3:]
            if t.lower() not in query_lower
        ]

        if not missing_topics:
            return None

        # Only add context if query seems incomplete or ambiguous
        ambiguous_indicators = [
            query_lower.startswith("what about"),
            query_lower.startswith("how about"),
            query_lower.startswith("and "),
            query_lower.startswith("also "),
            len(query.split()) < 5  # Short queries might need context
        ]

        if not any(ambiguous_indicators):
            return None

        # Add most relevant topic
        topic = missing_topics[0]
        if query_lower.startswith(("what about", "how about", "and ", "also ")):
            return f"{query} (regarding {topic})"
        else:
            return f"{query} in the context of {topic}"

    async def _generate_contextual_paraphrases(
        self,
        query: str,
        context: ConversationContext,
        num_paraphrases: int
    ) -> List[str]:
        """Generate paraphrases that maintain conversation context"""

        if not context.previous_queries:
            return []

        prev_queries_str = " | ".join(context.previous_queries[-3:])

        prompt = f"""Given this conversation context, generate {num_paraphrases} alternative ways to ask the current question that maintain the same intent and context.

Previous questions in conversation: {prev_queries_str}
Current question: {query}

Return ONLY a JSON array of {num_paraphrases} paraphrased questions. No explanation.

JSON array:"""

        try:
            import aiohttp
            import json
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.5, "num_predict": 200}
                    },
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "").strip()
                        # Try to parse JSON
                        import re
                        match = re.search(r'\[.*?\]', response, re.DOTALL)
                        if match:
                            paraphrases = json.loads(match.group())
                            return [str(p).strip() for p in paraphrases if p]
        except Exception as e:
            logger.warning(f"Contextual paraphrase generation failed: {e}")

        return []

    def extract_entities(self, text: str) -> List[str]:
        """Extract entities from text for context tracking"""
        import re

        entities = []

        # Extract capitalized phrases (potential proper nouns)
        cap_pattern = r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b'
        entities.extend(re.findall(cap_pattern, text))

        # Extract quoted terms
        quoted_pattern = r'"([^"]+)"|\'([^\']+)\''
        for match in re.findall(quoted_pattern, text):
            entities.extend([m for m in match if m])

        # Extract technical terms (camelCase, snake_case)
        tech_pattern = r'\b[a-z]+(?:[A-Z][a-z]+)+\b|\b[a-z]+(?:_[a-z]+)+\b'
        entities.extend(re.findall(tech_pattern, text))

        return list(set(entities))

    def extract_topic_keywords(self, text: str) -> List[str]:
        """Extract topic keywords from text"""
        import re

        # Common stopwords to filter
        stopwords = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'could', 'should', 'may', 'might', 'must', 'can', 'to', 'of',
            'in', 'for', 'on', 'with', 'at', 'by', 'from', 'as', 'into',
            'through', 'during', 'before', 'after', 'above', 'below',
            'between', 'under', 'again', 'further', 'then', 'once', 'here',
            'there', 'when', 'where', 'why', 'how', 'all', 'each', 'few',
            'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
            'only', 'own', 'same', 'so', 'than', 'too', 'very', 'just',
            'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those',
            'am', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs',
            'we', 'us', 'our', 'ours', 'you', 'your', 'yours', 'he', 'him',
            'his', 'she', 'her', 'hers', 'i', 'me', 'my', 'mine', 'and', 'but',
            'or', 'if', 'because', 'about', 'get', 'make', 'like', 'know'
        }

        # Tokenize and filter
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        keywords = [w for w in words if w not in stopwords]

        # Count frequency and return top keywords
        from collections import Counter
        counts = Counter(keywords)
        return [word for word, _ in counts.most_common(10)]


class UserHistoryExpander:
    """
    Expands queries based on user's query history and preferences.

    Provides:
    - Personalized term expansion based on past queries
    - Domain-specific vocabulary addition
    - Query style adaptation
    """

    def __init__(self):
        self._user_patterns: Dict[str, List[UserQueryPattern]] = {}  # user_id -> patterns
        self._user_vocabulary: Dict[str, Dict[str, int]] = {}  # user_id -> term -> count
        self._max_history_size = 100

    def record_query(
        self,
        user_id: str,
        query: str,
        successful: bool = True
    ):
        """
        Record a user query for learning patterns.

        Args:
            user_id: User identifier
            query: The query text
            successful: Whether the query returned good results
        """
        if user_id not in self._user_vocabulary:
            self._user_vocabulary[user_id] = {}
            self._user_patterns[user_id] = []

        # Extract and count terms
        terms = self._extract_significant_terms(query)
        for term in terms:
            self._user_vocabulary[user_id][term] = \
                self._user_vocabulary[user_id].get(term, 0) + (2 if successful else 1)

        # Trim vocabulary if too large
        if len(self._user_vocabulary[user_id]) > self._max_history_size * 5:
            # Keep top terms
            sorted_terms = sorted(
                self._user_vocabulary[user_id].items(),
                key=lambda x: x[1],
                reverse=True
            )[:self._max_history_size * 3]
            self._user_vocabulary[user_id] = dict(sorted_terms)

        # Learn patterns
        self._learn_patterns(user_id, query, successful)

    def _extract_significant_terms(self, query: str) -> List[str]:
        """Extract significant terms from query"""
        import re

        stopwords = {
            'what', 'how', 'why', 'when', 'where', 'who', 'which', 'is', 'are',
            'the', 'a', 'an', 'to', 'of', 'in', 'for', 'on', 'with', 'can',
            'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may',
            'might', 'must', 'have', 'has', 'had', 'be', 'been', 'being',
            'and', 'or', 'but', 'if', 'then', 'so', 'because', 'about', 'i',
            'me', 'my', 'we', 'our', 'you', 'your', 'it', 'its', 'this', 'that'
        }

        words = re.findall(r'\b[a-zA-Z]{2,}\b', query.lower())
        return [w for w in words if w not in stopwords]

    def _learn_patterns(
        self,
        user_id: str,
        query: str,
        successful: bool
    ):
        """Learn patterns from user query"""
        import datetime

        query_lower = query.lower()
        now = datetime.datetime.now().isoformat()

        patterns = self._user_patterns[user_id]

        # Pattern: Question style
        if query_lower.startswith("how to"):
            self._update_pattern(patterns, "question_style", "how_to", now, successful)
        elif query_lower.startswith("what is"):
            self._update_pattern(patterns, "question_style", "what_is", now, successful)
        elif query_lower.startswith("why"):
            self._update_pattern(patterns, "question_style", "why", now, successful)
        elif "?" not in query:
            self._update_pattern(patterns, "question_style", "keyword_search", now, successful)

        # Pattern: Query length preference
        word_count = len(query.split())
        if word_count <= 3:
            self._update_pattern(patterns, "length_preference", "short", now, successful)
        elif word_count <= 7:
            self._update_pattern(patterns, "length_preference", "medium", now, successful)
        else:
            self._update_pattern(patterns, "length_preference", "long", now, successful)

    def _update_pattern(
        self,
        patterns: List[UserQueryPattern],
        pattern_type: str,
        value: str,
        timestamp: str,
        successful: bool
    ):
        """Update or add a pattern"""
        for p in patterns:
            if p.pattern_type == pattern_type and p.value == value:
                p.frequency += 1 if successful else 0
                p.last_seen = timestamp
                # Update confidence based on success
                p.confidence = min(1.0, p.confidence + (0.05 if successful else -0.02))
                return

        # Add new pattern
        patterns.append(UserQueryPattern(
            pattern_type=pattern_type,
            value=value,
            frequency=1,
            last_seen=timestamp,
            confidence=0.6 if successful else 0.4
        ))

    async def expand_with_history(
        self,
        query: str,
        user_id: str,
        max_expansions: int = 3
    ) -> ContextualExpansionResult:
        """
        Expand query using user's history.

        Args:
            query: The query to expand
            user_id: User identifier
            max_expansions: Maximum expansion variants

        Returns:
            ContextualExpansionResult with history-aware expansions
        """
        result = ContextualExpansionResult(
            original_query=query,
            expanded_queries=[query]
        )

        if user_id not in self._user_vocabulary:
            return result

        # Get user's preferred vocabulary
        user_vocab = self._user_vocabulary[user_id]
        query_terms = set(self._extract_significant_terms(query))

        # Find related terms from user's vocabulary
        related_terms = []
        for term, count in sorted(user_vocab.items(), key=lambda x: x[1], reverse=True):
            if term not in query_terms and count >= 2:
                # Check if term is semantically related (simple heuristic)
                for qt in query_terms:
                    if (qt in term or term in qt or
                        self._terms_related(qt, term)):
                        related_terms.append(term)
                        break
            if len(related_terms) >= 5:
                break

        # Add expansions with user-preferred terms
        if related_terms:
            for term in related_terms[:2]:
                expanded = f"{query} {term}"
                if expanded not in result.expanded_queries:
                    result.expanded_queries.append(expanded)
                    result.history_terms_added.append(term)

        # Adapt query style based on user patterns
        patterns = self._user_patterns.get(user_id, [])
        style_pattern = next(
            (p for p in patterns if p.pattern_type == "question_style" and p.confidence > 0.6),
            None
        )

        if style_pattern and len(result.expanded_queries) < max_expansions:
            adapted = self._adapt_query_style(query, style_pattern.value)
            if adapted and adapted not in result.expanded_queries:
                result.expanded_queries.append(adapted)
                result.expansion_reasoning += f"Adapted to preferred style: {style_pattern.value}. "

        result.expanded_queries = result.expanded_queries[:max_expansions]
        return result

    def _terms_related(self, term1: str, term2: str) -> bool:
        """Simple check if two terms might be related"""
        # Check for common prefix/suffix
        min_len = min(len(term1), len(term2))
        if min_len < 4:
            return False

        common_prefix = 0
        for i in range(min_len):
            if term1[i] == term2[i]:
                common_prefix += 1
            else:
                break

        return common_prefix >= 4

    def _adapt_query_style(self, query: str, style: str) -> Optional[str]:
        """Adapt query to user's preferred style"""
        query_lower = query.lower().strip()

        if style == "how_to" and not query_lower.startswith("how to"):
            # Convert to "how to" format
            if query_lower.startswith(("what is", "what are")):
                return query.replace("What is", "How to understand").replace("What are", "How to understand")
            elif "?" not in query:
                return f"How to {query.lower()}"

        elif style == "what_is" and not query_lower.startswith(("what is", "what are")):
            # Convert to "what is" format
            if query_lower.startswith("how to"):
                return query.replace("How to", "What is the best way to").replace("how to", "what is the best way to")

        elif style == "keyword_search" and "?" in query:
            # Convert to keyword format
            keywords = self._extract_significant_terms(query)
            if len(keywords) >= 2:
                return " ".join(keywords[:5])

        return None

    def get_user_preferences(self, user_id: str) -> Dict[str, Any]:
        """Get user's learned preferences"""
        return {
            "vocabulary_size": len(self._user_vocabulary.get(user_id, {})),
            "top_terms": sorted(
                self._user_vocabulary.get(user_id, {}).items(),
                key=lambda x: x[1],
                reverse=True
            )[:20],
            "patterns": [
                {"type": p.pattern_type, "value": p.value, "confidence": p.confidence}
                for p in self._user_patterns.get(user_id, [])
            ]
        }

    def clear_user_history(self, user_id: str):
        """Clear user's query history"""
        self._user_vocabulary.pop(user_id, None)
        self._user_patterns.pop(user_id, None)


class AdvancedQueryExpander(QueryExpander):
    """
    Enhanced query expander with contextual and user-history awareness.

    Combines:
    - Basic synonym expansion
    - LLM paraphrasing
    - Conversation context awareness
    - User history personalization
    """

    def __init__(
        self,
        synonym_expander: Optional[SynonymExpander] = None,
        paraphrase_generator: Optional[ParaphraseGenerator] = None,
        embedder: Any = None,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        super().__init__(
            synonym_expander=synonym_expander,
            paraphrase_generator=paraphrase_generator,
            embedder=embedder,
            ollama_url=ollama_url,
            ollama_model=ollama_model
        )
        self.contextual_expander = ContextualExpander(ollama_url, ollama_model)
        self.history_expander = UserHistoryExpander()

    async def expand_with_context_and_history(
        self,
        query: str,
        user_id: Optional[str] = None,
        context: Optional[ConversationContext] = None,
        config: Optional[ExpansionConfig] = None
    ) -> QueryExpansionResult:
        """
        Expand query using all available information.

        Args:
            query: The query to expand
            user_id: Optional user identifier for personalization
            context: Optional conversation context
            config: Expansion configuration

        Returns:
            QueryExpansionResult with all expansions
        """
        if config is None:
            config = ExpansionConfig()

        # Start with basic expansion
        result = await self.expand(query, config)

        # Add contextual expansions
        if context:
            ctx_result = await self.contextual_expander.expand_with_context(
                query, context, max_expansions=3
            )
            for exp in ctx_result.expanded_queries:
                if exp not in result.expanded_queries:
                    result.expanded_queries.append(exp)
            result.expansion_metadata["context_terms"] = ctx_result.context_terms_added

        # Add user history expansions
        if user_id:
            hist_result = await self.history_expander.expand_with_history(
                query, user_id, max_expansions=2
            )
            for exp in hist_result.expanded_queries:
                if exp not in result.expanded_queries:
                    result.expanded_queries.append(exp)
            result.expansion_metadata["history_terms"] = hist_result.history_terms_added

            # Record this query for future learning
            self.history_expander.record_query(user_id, query)

        # Cap total and deduplicate
        seen = set()
        unique = []
        for q in result.expanded_queries:
            q_lower = q.lower().strip()
            if q_lower not in seen:
                seen.add(q_lower)
                unique.append(q)

        result.expanded_queries = unique[:config.max_total_queries]
        result.expansion_metadata["total_variants"] = len(result.expanded_queries)

        return result

    def update_context(
        self,
        context: ConversationContext,
        query: str,
        response: str
    ) -> ConversationContext:
        """
        Update conversation context with new query/response.

        Args:
            context: Current context
            query: New query
            response: Response to the query

        Returns:
            Updated context
        """
        # Add to history
        context.previous_queries.append(query)
        context.previous_responses.append(response)

        # Keep history bounded
        if len(context.previous_queries) > 10:
            context.previous_queries = context.previous_queries[-10:]
            context.previous_responses = context.previous_responses[-10:]

        # Extract entities from query and response
        new_entities = self.contextual_expander.extract_entities(query)
        new_entities.extend(self.contextual_expander.extract_entities(response))
        context.mentioned_entities.extend(new_entities)

        # Keep entities bounded
        if len(context.mentioned_entities) > 50:
            context.mentioned_entities = context.mentioned_entities[-50:]

        # Extract topic keywords
        new_keywords = self.contextual_expander.extract_topic_keywords(query)
        new_keywords.extend(self.contextual_expander.extract_topic_keywords(response))
        context.topic_keywords.extend(new_keywords)

        # Keep keywords bounded
        if len(context.topic_keywords) > 30:
            context.topic_keywords = context.topic_keywords[-30:]

        return context


# Singleton instances for advanced expansion
_contextual_expander: Optional[ContextualExpander] = None
_history_expander: Optional[UserHistoryExpander] = None
_advanced_expander: Optional[AdvancedQueryExpander] = None


def get_contextual_expander(
    ollama_url: str = "http://localhost:11434",
    ollama_model: str = "gemma2:2b"
) -> ContextualExpander:
    """Get or create the global contextual expander"""
    global _contextual_expander
    if _contextual_expander is None:
        _contextual_expander = ContextualExpander(ollama_url, ollama_model)
    return _contextual_expander


def get_history_expander() -> UserHistoryExpander:
    """Get or create the global history expander"""
    global _history_expander
    if _history_expander is None:
        _history_expander = UserHistoryExpander()
    return _history_expander


def get_advanced_query_expander(
    embedder: Any = None,
    ollama_url: str = "http://localhost:11434",
    ollama_model: str = "gemma2:2b"
) -> AdvancedQueryExpander:
    """Get or create the global advanced query expander"""
    global _advanced_expander
    if _advanced_expander is None:
        _advanced_expander = AdvancedQueryExpander(
            embedder=embedder,
            ollama_url=ollama_url,
            ollama_model=ollama_model
        )
    return _advanced_expander


# =============================================================================
# CUTTING EDGE: LLM-Driven Semantic Expansion & Contrastive Generation
# =============================================================================

from enum import Enum
from datetime import datetime


class ExpansionStrategy(str, Enum):
    """Strategies for query expansion."""
    SEMANTIC = "semantic"  # Deep semantic understanding
    CONTRASTIVE = "contrastive"  # Generate diverse alternatives
    NEGATIVE = "negative"  # Generate negative/exclusion queries
    HYPOTHETICAL = "hypothetical"  # Generate hypothetical document queries
    MULTI_PERSPECTIVE = "multi_perspective"  # Different viewpoints


class ContrastType(str, Enum):
    """Types of contrastive expansions."""
    SYNONYM_CONTRAST = "synonym_contrast"  # Different words, same meaning
    SCOPE_CONTRAST = "scope_contrast"  # Broader/narrower scope
    PERSPECTIVE_CONTRAST = "perspective_contrast"  # Different viewpoints
    TEMPORAL_CONTRAST = "temporal_contrast"  # Different time frames
    DOMAIN_CONTRAST = "domain_contrast"  # Different domains/fields


@dataclass
class SemanticExpansion:
    """Result of semantic expansion."""
    expansion_id: str
    original_query: str
    expanded_query: str
    strategy: ExpansionStrategy
    semantic_similarity: float
    reasoning: str
    generated_at: datetime = None

    def __post_init__(self):
        if self.generated_at is None:
            self.generated_at = datetime.now()


@dataclass
class ContrastiveExpansion:
    """A contrastive query expansion."""
    expansion_id: str
    original_query: str
    contrastive_query: str
    contrast_type: ContrastType
    diversity_score: float  # How different from original
    relevance_score: float  # How relevant to original intent
    explanation: str


@dataclass
class NegativeQuery:
    """A negative query for filtering irrelevant results."""
    query: str
    exclusion_terms: List[str]
    reasoning: str


@dataclass
class SemanticExpansionConfig:
    """Configuration for semantic expansion."""
    enable_deep_semantics: bool = True
    enable_contrastive: bool = True
    enable_negative_mining: bool = True
    max_semantic_expansions: int = 3
    max_contrastive_expansions: int = 3
    min_diversity_score: float = 0.3
    max_diversity_score: float = 0.8


@dataclass
class CuttingEdgeExpansionConfig:
    """Configuration for Cutting Edge expansion."""
    enable_semantic_expansion: bool = True
    enable_contrastive_generation: bool = True
    semantic_config: SemanticExpansionConfig = None
    base_config: ExpansionConfig = None

    def __post_init__(self):
        if self.semantic_config is None:
            self.semantic_config = SemanticExpansionConfig()
        if self.base_config is None:
            self.base_config = ExpansionConfig()


class SemanticExpansionEngine:
    """
    LLM-driven semantic expansion engine.

    Uses deep semantic understanding to generate meaningful query variations
    that capture the true intent behind the query.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.ollama_model = ollama_model
        self._expansion_cache: Dict[str, List[SemanticExpansion]] = {}

    async def expand_semantically(
        self,
        query: str,
        max_expansions: int = 3
    ) -> List[SemanticExpansion]:
        """
        Generate semantically rich expansions using LLM understanding.

        Args:
            query: The query to expand
            max_expansions: Maximum number of expansions

        Returns:
            List of semantic expansions
        """
        import uuid

        # Check cache
        cache_key = f"{query}:{max_expansions}"
        if cache_key in self._expansion_cache:
            return self._expansion_cache[cache_key]

        prompt = f"""Analyze the following query and generate {max_expansions} semantically equivalent but differently phrased queries.

Query: {query}

For each expansion:
1. Maintain the exact same intent and meaning
2. Use different vocabulary and phrasing
3. Consider different ways a user might ask the same question

Return a JSON array of objects with "query" and "reasoning" fields.
Example: [{{"query": "...", "reasoning": "..."}}]

JSON array:"""

        expansions = []

        try:
            import aiohttp
            import json as json_lib
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.7, "num_predict": 500}
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "")

                        # Parse JSON from response
                        import re
                        match = re.search(r'\[.*?\]', response, re.DOTALL)
                        if match:
                            items = json_lib.loads(match.group())
                            for item in items[:max_expansions]:
                                if isinstance(item, dict) and "query" in item:
                                    expansions.append(SemanticExpansion(
                                        expansion_id=f"sem_{uuid.uuid4().hex[:8]}",
                                        original_query=query,
                                        expanded_query=item["query"],
                                        strategy=ExpansionStrategy.SEMANTIC,
                                        semantic_similarity=0.9,  # High similarity by design
                                        reasoning=item.get("reasoning", "Semantic paraphrase")
                                    ))
        except Exception as e:
            logger.warning(f"Semantic expansion failed: {e}")

        # Cache results
        self._expansion_cache[cache_key] = expansions

        # Limit cache size
        if len(self._expansion_cache) > 500:
            # Remove oldest entries
            keys = list(self._expansion_cache.keys())
            for k in keys[:250]:
                del self._expansion_cache[k]

        return expansions

    async def extract_query_intent(self, query: str) -> Dict[str, Any]:
        """
        Extract deep semantic intent from query.

        Returns structured intent including:
        - Main topic/subject
        - Action/goal
        - Constraints/filters
        - Expected answer type
        """
        prompt = f"""Analyze this query and extract its semantic components:

Query: {query}

Return a JSON object with:
- "main_topic": The primary subject of the query
- "action_goal": What the user wants to achieve
- "constraints": Any filters or limitations
- "answer_type": Expected type of answer (fact, explanation, list, comparison, etc.)
- "implicit_context": Any implied but unstated context

JSON:"""

        try:
            import aiohttp
            import json as json_lib
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.3, "num_predict": 300}
                    },
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "")

                        import re
                        match = re.search(r'\{.*?\}', response, re.DOTALL)
                        if match:
                            return json_lib.loads(match.group())
        except Exception as e:
            logger.warning(f"Intent extraction failed: {e}")

        return {
            "main_topic": query,
            "action_goal": "unknown",
            "constraints": [],
            "answer_type": "unknown",
            "implicit_context": ""
        }


class ContrastiveQueryGenerator:
    """
    Generates semantically diverse query variations.

    Creates queries that explore different aspects of the same topic,
    helping to retrieve more comprehensive results.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.ollama_model = ollama_model

    async def generate_contrastive_queries(
        self,
        query: str,
        contrast_types: Optional[List[ContrastType]] = None,
        max_per_type: int = 2
    ) -> List[ContrastiveExpansion]:
        """
        Generate contrastive query variations.

        Args:
            query: Original query
            contrast_types: Types of contrast to generate
            max_per_type: Maximum expansions per contrast type

        Returns:
            List of contrastive expansions
        """
        import uuid

        if contrast_types is None:
            contrast_types = [
                ContrastType.SYNONYM_CONTRAST,
                ContrastType.SCOPE_CONTRAST,
                ContrastType.PERSPECTIVE_CONTRAST
            ]

        all_expansions = []

        for contrast_type in contrast_types:
            expansions = await self._generate_for_type(query, contrast_type, max_per_type)
            all_expansions.extend(expansions)

        return all_expansions

    async def _generate_for_type(
        self,
        query: str,
        contrast_type: ContrastType,
        max_expansions: int
    ) -> List[ContrastiveExpansion]:
        """Generate expansions for a specific contrast type."""
        import uuid

        type_prompts = {
            ContrastType.SYNONYM_CONTRAST: f"Generate {max_expansions} queries using different words but keeping the same meaning as: {query}",
            ContrastType.SCOPE_CONTRAST: f"Generate {max_expansions} queries that are broader or narrower in scope than: {query}",
            ContrastType.PERSPECTIVE_CONTRAST: f"Generate {max_expansions} queries asking about the same topic from different perspectives than: {query}",
            ContrastType.TEMPORAL_CONTRAST: f"Generate {max_expansions} queries about the same topic but different time frames than: {query}",
            ContrastType.DOMAIN_CONTRAST: f"Generate {max_expansions} queries about similar concepts in different domains than: {query}"
        }

        prompt = f"""{type_prompts.get(contrast_type, type_prompts[ContrastType.SYNONYM_CONTRAST])}

Return a JSON array of objects with "query", "diversity_score" (0-1, how different), and "explanation".
Example: [{{"query": "...", "diversity_score": 0.5, "explanation": "..."}}]

JSON array:"""

        expansions = []

        try:
            import aiohttp
            import json as json_lib
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.8, "num_predict": 400}
                    },
                    timeout=aiohttp.ClientTimeout(total=25)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "")

                        import re
                        match = re.search(r'\[.*?\]', response, re.DOTALL)
                        if match:
                            items = json_lib.loads(match.group())
                            for item in items[:max_expansions]:
                                if isinstance(item, dict) and "query" in item:
                                    diversity = float(item.get("diversity_score", 0.5))
                                    expansions.append(ContrastiveExpansion(
                                        expansion_id=f"con_{uuid.uuid4().hex[:8]}",
                                        original_query=query,
                                        contrastive_query=item["query"],
                                        contrast_type=contrast_type,
                                        diversity_score=diversity,
                                        relevance_score=1.0 - diversity * 0.3,  # Higher diversity = slightly lower relevance
                                        explanation=item.get("explanation", "")
                                    ))
        except Exception as e:
            logger.warning(f"Contrastive generation failed for {contrast_type}: {e}")

        return expansions

    async def generate_negative_queries(
        self,
        query: str,
        max_negatives: int = 2
    ) -> List[NegativeQuery]:
        """
        Generate negative queries for filtering irrelevant results.

        Identifies what the query is NOT asking about to help exclude
        irrelevant documents.
        """
        prompt = f"""Analyze this query and identify what it is NOT asking about. Generate {max_negatives} negative queries that represent topics to exclude from search results.

Query: {query}

Return a JSON array with "query" (the negative/exclusion query), "exclusion_terms" (list of terms to exclude), and "reasoning".

JSON array:"""

        negatives = []

        try:
            import aiohttp
            import json as json_lib
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.5, "num_predict": 300}
                    },
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "")

                        import re
                        match = re.search(r'\[.*?\]', response, re.DOTALL)
                        if match:
                            items = json_lib.loads(match.group())
                            for item in items[:max_negatives]:
                                if isinstance(item, dict):
                                    negatives.append(NegativeQuery(
                                        query=item.get("query", ""),
                                        exclusion_terms=item.get("exclusion_terms", []),
                                        reasoning=item.get("reasoning", "")
                                    ))
        except Exception as e:
            logger.warning(f"Negative query generation failed: {e}")

        return negatives


class CuttingEdgeQueryExpander:
    """
    Cutting Edge query expander with LLM-driven semantic expansion.

    Combines:
    - AdvancedQueryExpander for context/history awareness
    - SemanticExpansionEngine for deep semantic understanding
    - ContrastiveQueryGenerator for diverse query generation
    """

    def __init__(
        self,
        base_expander: Optional[AdvancedQueryExpander] = None,
        config: Optional[CuttingEdgeExpansionConfig] = None,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.config = config or CuttingEdgeExpansionConfig()

        self.base_expander = base_expander or AdvancedQueryExpander(
            ollama_url=ollama_url,
            ollama_model=ollama_model
        )
        self.semantic_engine = SemanticExpansionEngine(ollama_url, ollama_model)
        self.contrastive_generator = ContrastiveQueryGenerator(ollama_url, ollama_model)

        self._expansion_history: List[Dict[str, Any]] = []

    async def expand(
        self,
        query: str,
        user_id: Optional[str] = None,
        context: Optional[ConversationContext] = None,
        include_contrastive: bool = True,
        include_negative: bool = False
    ) -> QueryExpansionResult:
        """
        Comprehensive query expansion with cutting edge features.

        Args:
            query: Query to expand
            user_id: Optional user ID for personalization
            context: Optional conversation context
            include_contrastive: Include contrastive variations
            include_negative: Include negative queries for filtering

        Returns:
            QueryExpansionResult with all expansions
        """
        # Start with advanced expansion (context + history)
        result = await self.base_expander.expand_with_context_and_history(
            query=query,
            user_id=user_id,
            context=context,
            config=self.config.base_config
        )

        all_queries = set(result.expanded_queries)

        # Add semantic expansions
        if self.config.enable_semantic_expansion:
            semantic_expansions = await self.semantic_engine.expand_semantically(
                query,
                max_expansions=self.config.semantic_config.max_semantic_expansions
            )
            for exp in semantic_expansions:
                all_queries.add(exp.expanded_query)
            result.expansion_metadata["semantic_expansions"] = len(semantic_expansions)

        # Add contrastive expansions
        if self.config.enable_contrastive_generation and include_contrastive:
            contrastive_expansions = await self.contrastive_generator.generate_contrastive_queries(
                query,
                max_per_type=self.config.semantic_config.max_contrastive_expansions
            )

            # Filter by diversity score
            for exp in contrastive_expansions:
                if (self.config.semantic_config.min_diversity_score <=
                    exp.diversity_score <=
                    self.config.semantic_config.max_diversity_score):
                    all_queries.add(exp.contrastive_query)

            result.expansion_metadata["contrastive_expansions"] = len(contrastive_expansions)

        # Generate negative queries if requested
        if include_negative:
            negative_queries = await self.contrastive_generator.generate_negative_queries(
                query, max_negatives=2
            )
            result.expansion_metadata["negative_queries"] = [
                {"query": nq.query, "exclusion_terms": nq.exclusion_terms}
                for nq in negative_queries
            ]

        # Finalize expanded queries
        result.expanded_queries = list(all_queries)[:self.config.base_config.max_total_queries]

        # Put original first
        if query in result.expanded_queries:
            result.expanded_queries.remove(query)
        result.expanded_queries.insert(0, query)

        result.expansion_metadata["total_variants"] = len(result.expanded_queries)

        # Record history
        self._expansion_history.append({
            "query": query[:50],
            "user_id": user_id,
            "num_expansions": len(result.expanded_queries),
            "timestamp": datetime.now().isoformat()
        })

        if len(self._expansion_history) > 500:
            self._expansion_history = self._expansion_history[-250:]

        logger.info(f"Cutting edge expansion: {query[:30]}... -> {len(result.expanded_queries)} variants")
        return result

    async def expand_with_intent_analysis(
        self,
        query: str
    ) -> Tuple[QueryExpansionResult, Dict[str, Any]]:
        """
        Expand query with full intent analysis.

        Returns both expansions and detailed intent analysis.
        """
        # Get intent analysis
        intent = await self.semantic_engine.extract_query_intent(query)

        # Expand
        result = await self.expand(query)

        return result, intent

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return {
            "config": {
                "semantic_expansion": self.config.enable_semantic_expansion,
                "contrastive_generation": self.config.enable_contrastive_generation
            },
            "expansion_history_size": len(self._expansion_history),
            "semantic_cache_size": len(self.semantic_engine._expansion_cache),
            "recent_expansions": self._expansion_history[-10:]
        }


# -----------------------------------------------------------------------------
# Factory Functions for Cutting Edge Features
# -----------------------------------------------------------------------------

_cutting_edge_expander: Optional[CuttingEdgeQueryExpander] = None


def get_cutting_edge_query_expander(
    ollama_url: str = "http://localhost:11434",
    ollama_model: str = "gemma2:2b"
) -> CuttingEdgeQueryExpander:
    """Get or create the global cutting edge query expander."""
    global _cutting_edge_expander
    if _cutting_edge_expander is None:
        _cutting_edge_expander = CuttingEdgeQueryExpander(
            ollama_url=ollama_url,
            ollama_model=ollama_model
        )
        logger.info("CuttingEdgeQueryExpander created with semantic expansion")
    return _cutting_edge_expander


async def cutting_edge_expand_query(
    query: str,
    user_id: Optional[str] = None,
    context: Optional[ConversationContext] = None,
    include_contrastive: bool = True
) -> List[str]:
    """
    Convenience function for cutting edge query expansion.

    Returns list of expanded queries including semantic and contrastive variations.
    """
    expander = get_cutting_edge_query_expander()
    result = await expander.expand(
        query=query,
        user_id=user_id,
        context=context,
        include_contrastive=include_contrastive
    )
    return result.expanded_queries


def reset_cutting_edge_query_expander():
    """Reset cutting edge query expander (for testing)."""
    global _cutting_edge_expander
    _cutting_edge_expander = None


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

_query_expander: Optional[QueryExpander] = None


def get_query_expander(
    embedder: Any = None,
    ollama_url: str = "http://localhost:11434",
    ollama_model: str = "gemma2:2b"
) -> QueryExpander:
    """Get or create the global query expander instance."""
    global _query_expander
    if _query_expander is None:
        _query_expander = QueryExpander(
            embedder=embedder,
            ollama_url=ollama_url,
            ollama_model=ollama_model
        )
    return _query_expander


def reset_query_expander() -> None:
    """Reset the global query expander (useful for testing)."""
    global _query_expander
    _query_expander = None


async def expand_query(
    query: str,
    use_synonyms: bool = True,
    use_paraphrases: bool = True,
    max_queries: int = 5
) -> List[str]:
    """
    Convenience function to expand a query for search.

    Usage:
        queries = await expand_query("How to optimize database performance?")
        # Returns: ["How to optimize database performance?",
        #           "How to improve database performance?",
        #           "What are ways to tune database queries?", ...]

        for q in queries:
            results = await search(q)
    """
    expander = get_query_expander()
    return await expander.expand_for_search(
        query,
        use_synonyms=use_synonyms,
        use_paraphrases=use_paraphrases,
        max_queries=max_queries
    )

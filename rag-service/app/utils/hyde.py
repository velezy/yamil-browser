"""
HyDE (Hypothetical Document Embeddings) Module

HyDE improves retrieval by generating a hypothetical answer first,
then using that to search for similar real documents.

Research paper: "Precise Zero-Shot Dense Retrieval without Relevance Labels"
"""

import logging
import aiohttp
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class HyDEGenerator:
    """
    Generates hypothetical documents/answers for improved retrieval.

    The idea: Instead of directly embedding the query, generate what
    an ideal answer might look like, then embed THAT to find similar
    real documents.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.model = model

    async def generate_hypothetical_document(
        self,
        query: str,
        context_hint: Optional[str] = None,
        num_hypotheses: int = 1
    ) -> List[str]:
        """
        Generate hypothetical document(s) that would answer the query.

        Args:
            query: The user's query
            context_hint: Optional hint about the domain/topic
            num_hypotheses: Number of hypothetical documents to generate

        Returns:
            List of hypothetical document texts
        """
        hypotheses = []

        prompt = self._build_hyde_prompt(query, context_hint)

        async with aiohttp.ClientSession() as session:
            for _ in range(num_hypotheses):
                try:
                    async with session.post(
                        f"{self.ollama_url}/api/generate",
                        json={
                            "model": self.model,
                            "prompt": prompt,
                            "stream": False,
                            "options": {
                                "temperature": 0.7,
                                "num_predict": 256,
                            }
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            hypothesis = data.get('response', '').strip()
                            if hypothesis:
                                hypotheses.append(hypothesis)
                        else:
                            logger.warning(f"HyDE generation failed: {resp.status}")

                except Exception as e:
                    logger.error(f"HyDE generation error: {e}")

        # Fallback to original query if no hypotheses generated
        if not hypotheses:
            logger.warning("No hypotheses generated, using original query")
            hypotheses = [query]

        return hypotheses

    def _build_hyde_prompt(self, query: str, context_hint: Optional[str] = None) -> str:
        """Build the prompt for generating hypothetical documents."""

        context_part = ""
        if context_hint:
            context_part = f"\nContext: This is about {context_hint}.\n"

        return f"""Write a detailed, factual passage that would be found in a document answering this question.
Write as if you are quoting directly from a relevant source document.
Be specific and informative. Do not include phrases like "According to..." or "The document states...".
Just write the content itself.
{context_part}
Question: {query}

Passage:"""

    async def generate_multi_perspective(
        self,
        query: str,
        perspectives: List[str] = None
    ) -> List[str]:
        """
        Generate hypothetical documents from multiple perspectives.

        Args:
            query: The user's query
            perspectives: List of perspectives (e.g., ["technical", "simple", "historical"])

        Returns:
            List of hypothetical documents from different angles
        """
        if perspectives is None:
            perspectives = ["detailed technical", "simple explanation", "practical example"]

        hypotheses = []

        async with aiohttp.ClientSession() as session:
            for perspective in perspectives:
                prompt = f"""Write a {perspective} passage that answers this question.
Be factual and specific. Write directly without meta-commentary.

Question: {query}

Passage:"""

                try:
                    async with session.post(
                        f"{self.ollama_url}/api/generate",
                        json={
                            "model": self.model,
                            "prompt": prompt,
                            "stream": False,
                            "options": {
                                "temperature": 0.8,
                                "num_predict": 200,
                            }
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            hypothesis = data.get('response', '').strip()
                            if hypothesis:
                                hypotheses.append(hypothesis)

                except Exception as e:
                    logger.error(f"Multi-perspective HyDE error: {e}")

        return hypotheses if hypotheses else [query]


class HyDERetriever:
    """
    Combines HyDE with vector search for improved retrieval.
    """

    def __init__(
        self,
        hyde_generator: HyDEGenerator,
        embedding_model: Any = None
    ):
        self.hyde = hyde_generator
        self.embedding_model = embedding_model

    async def retrieve_with_hyde(
        self,
        query: str,
        vector_store: Any,
        top_k: int = 5,
        use_multi_perspective: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Retrieve documents using HyDE-enhanced search.

        Args:
            query: User's query
            vector_store: Vector store for similarity search
            top_k: Number of results to return
            use_multi_perspective: Use multiple hypothetical perspectives

        Returns:
            List of retrieved documents with scores
        """
        # Generate hypothetical document(s)
        if use_multi_perspective:
            hypotheses = await self.hyde.generate_multi_perspective(query)
        else:
            hypotheses = await self.hyde.generate_hypothetical_document(query)

        logger.debug(f"Generated {len(hypotheses)} hypothetical documents")

        # Search using each hypothesis and aggregate results
        all_results = []
        seen_chunks = set()

        for hypothesis in hypotheses:
            # Search with the hypothetical document
            results = await vector_store.search(
                query=hypothesis,
                top_k=top_k * 2,  # Get more to allow for deduplication
            )

            for result in results:
                # Deduplicate by content hash
                content_key = hash(result.get('content', ''))
                if content_key not in seen_chunks:
                    seen_chunks.add(content_key)
                    all_results.append(result)

        # Sort by score and return top_k
        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return all_results[:top_k]


# Singleton instance
_hyde_generator: Optional[HyDEGenerator] = None


def get_hyde_generator(
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b"
) -> HyDEGenerator:
    """Get or create the global HyDE generator instance"""
    global _hyde_generator
    if _hyde_generator is None:
        _hyde_generator = HyDEGenerator(ollama_url=ollama_url, model=model)
    return _hyde_generator


async def generate_hyde_query(
    query: str,
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b"
) -> str:
    """
    Convenience function to generate a single hypothetical document.

    Usage:
        hyde_query = await generate_hyde_query("What is machine learning?")
        # Use hyde_query for vector search instead of original query
    """
    generator = get_hyde_generator(ollama_url, model)
    hypotheses = await generator.generate_hypothetical_document(query)
    return hypotheses[0] if hypotheses else query


async def generate_hyde_embeddings(
    query: str,
    embedder,
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b",
    num_hypotheses: int = 1
) -> List[List[float]]:
    """
    Generate embeddings from hypothetical documents.

    This is the core HyDE operation: generate hypothetical answer,
    then embed it for vector search.

    Args:
        query: The user's query
        embedder: Embedding model with .encode() method
        ollama_url: Ollama API URL
        model: LLM model to use for hypothesis generation
        num_hypotheses: Number of hypothetical documents to generate

    Returns:
        List of embedding vectors for hypothetical documents
    """
    generator = get_hyde_generator(ollama_url, model)
    hypotheses = await generator.generate_hypothetical_document(
        query=query,
        num_hypotheses=num_hypotheses
    )

    embeddings = []
    for hypothesis in hypotheses:
        if hypothesis:
            embedding = embedder.encode(hypothesis).tolist()
            embeddings.append(embedding)

    return embeddings


# =============================================================================
# CUTTING-EDGE: ITERATIVE HyDE WITH FEEDBACK-DRIVEN REFINEMENT
# =============================================================================

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple
from collections import defaultdict


class RefinementStrategy(str, Enum):
    """Strategies for refining hypotheses based on retrieval feedback."""
    INCORPORATE = "incorporate"  # Incorporate retrieved content into hypothesis
    CONTRAST = "contrast"  # Generate hypothesis that contrasts with poor results
    EXPAND = "expand"  # Expand hypothesis with missing aspects
    FOCUS = "focus"  # Focus hypothesis on high-scoring aspects
    DIVERSIFY = "diversify"  # Generate diverse alternatives


class StoppingReason(str, Enum):
    """Reasons for stopping the iterative process."""
    QUALITY_THRESHOLD = "quality_threshold"  # Reached quality threshold
    MAX_ITERATIONS = "max_iterations"  # Hit iteration limit
    NO_IMPROVEMENT = "no_improvement"  # Quality not improving
    CONVERGENCE = "convergence"  # Hypotheses converged


@dataclass
class RetrievalQuality:
    """Quality metrics for retrieval results."""
    avg_score: float  # Average similarity score
    max_score: float  # Maximum similarity score
    min_score: float  # Minimum similarity score
    coverage: float  # How much of query is covered (0-1)
    relevance: float  # Estimated relevance (0-1)
    diversity: float  # Diversity of results (0-1)
    confidence: float  # Overall confidence (0-1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "avg_score": round(self.avg_score, 3),
            "max_score": round(self.max_score, 3),
            "min_score": round(self.min_score, 3),
            "coverage": round(self.coverage, 3),
            "relevance": round(self.relevance, 3),
            "diversity": round(self.diversity, 3),
            "confidence": round(self.confidence, 3)
        }

    @property
    def overall_quality(self) -> float:
        """Calculate overall quality score."""
        return (
            self.avg_score * 0.3 +
            self.relevance * 0.3 +
            self.coverage * 0.2 +
            self.confidence * 0.2
        )


@dataclass
class IterationResult:
    """Result from a single iteration of the HyDE loop."""
    iteration: int
    hypothesis: str
    retrieved_docs: List[Dict[str, Any]]
    quality: RetrievalQuality
    refinement_strategy: Optional[RefinementStrategy]
    refinement_reason: str
    duration_ms: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "iteration": self.iteration,
            "hypothesis": self.hypothesis[:200] + "..." if len(self.hypothesis) > 200 else self.hypothesis,
            "num_docs": len(self.retrieved_docs),
            "quality": self.quality.to_dict(),
            "refinement_strategy": self.refinement_strategy.value if self.refinement_strategy else None,
            "refinement_reason": self.refinement_reason,
            "duration_ms": round(self.duration_ms, 2)
        }


@dataclass
class IterativeHyDEResult:
    """Complete result from iterative HyDE process."""
    query: str
    final_hypothesis: str
    final_results: List[Dict[str, Any]]
    final_quality: RetrievalQuality
    iterations: List[IterationResult]
    total_iterations: int
    stopping_reason: StoppingReason
    total_duration_ms: float
    improvement_trajectory: List[float]  # Quality scores over iterations

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "final_hypothesis": self.final_hypothesis[:300] + "..." if len(self.final_hypothesis) > 300 else self.final_hypothesis,
            "num_results": len(self.final_results),
            "final_quality": self.final_quality.to_dict(),
            "iterations": [it.to_dict() for it in self.iterations],
            "total_iterations": self.total_iterations,
            "stopping_reason": self.stopping_reason.value,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "improvement_trajectory": [round(q, 3) for q in self.improvement_trajectory]
        }


class QualityEstimator:
    """
    Estimates the quality of retrieval results.

    Uses multiple signals to assess how well the retrieved
    documents match the query intent.
    """

    def __init__(self, llm_generator: Optional[HyDEGenerator] = None):
        self.llm_generator = llm_generator

    def estimate_quality(
        self,
        query: str,
        hypothesis: str,
        results: List[Dict[str, Any]]
    ) -> RetrievalQuality:
        """
        Estimate quality of retrieval results.

        Args:
            query: Original query
            hypothesis: Hypothesis used for retrieval
            results: Retrieved documents

        Returns:
            RetrievalQuality metrics
        """
        if not results:
            return RetrievalQuality(
                avg_score=0.0, max_score=0.0, min_score=0.0,
                coverage=0.0, relevance=0.0, diversity=0.0, confidence=0.0
            )

        # Score-based metrics
        scores = [r.get('score', 0.0) for r in results]
        avg_score = sum(scores) / len(scores)
        max_score = max(scores)
        min_score = min(scores)

        # Coverage: how many query terms appear in results
        query_terms = set(query.lower().split())
        result_text = " ".join(r.get('content', '') for r in results).lower()
        result_terms = set(result_text.split())
        coverage = len(query_terms & result_terms) / max(len(query_terms), 1)

        # Relevance: estimate from hypothesis overlap with results
        hyp_terms = set(hypothesis.lower().split())
        relevance = len(hyp_terms & result_terms) / max(len(hyp_terms), 1)

        # Diversity: unique content ratio
        unique_contents = set()
        for r in results:
            content = r.get('content', '')[:100]  # First 100 chars
            unique_contents.add(content)
        diversity = len(unique_contents) / max(len(results), 1)

        # Confidence: based on score distribution
        score_std = (sum((s - avg_score) ** 2 for s in scores) / len(scores)) ** 0.5
        confidence = max(0.0, min(1.0, avg_score - score_std))

        return RetrievalQuality(
            avg_score=avg_score,
            max_score=max_score,
            min_score=min_score,
            coverage=min(1.0, coverage),
            relevance=min(1.0, relevance),
            diversity=diversity,
            confidence=confidence
        )

    async def estimate_with_llm(
        self,
        query: str,
        results: List[Dict[str, Any]]
    ) -> float:
        """Use LLM to estimate result relevance (0-1)."""
        if not self.llm_generator or not results:
            return 0.5

        # Build context from top results
        result_summaries = []
        for r in results[:3]:
            content = r.get('content', '')[:200]
            result_summaries.append(content)

        context = "\n---\n".join(result_summaries)

        prompt = f"""Rate how well these search results answer the query.

QUERY: {query}

RESULTS:
{context}

Rate from 0 to 10 where:
0 = Completely irrelevant
5 = Somewhat relevant
10 = Perfectly relevant

Respond with just a number:"""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.llm_generator.ollama_url}/api/generate",
                    json={
                        "model": self.llm_generator.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": 10}
                    },
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get('response', '5').strip()
                        # Extract number
                        import re
                        match = re.search(r'(\d+(?:\.\d+)?)', response)
                        if match:
                            return min(1.0, float(match.group(1)) / 10)
        except Exception as e:
            logger.debug(f"LLM quality estimation failed: {e}")

        return 0.5


class HypothesisRefiner:
    """
    Refines hypotheses based on retrieval feedback.

    Uses retrieved documents to improve the hypothesis
    for better retrieval in the next iteration.
    """

    def __init__(self, hyde_generator: HyDEGenerator):
        self.generator = hyde_generator

    async def refine(
        self,
        query: str,
        current_hypothesis: str,
        results: List[Dict[str, Any]],
        quality: RetrievalQuality,
        strategy: RefinementStrategy
    ) -> str:
        """
        Refine hypothesis based on retrieval results.

        Args:
            query: Original query
            current_hypothesis: Current hypothesis
            results: Retrieved documents
            quality: Quality metrics
            strategy: Refinement strategy to use

        Returns:
            Refined hypothesis
        """
        if strategy == RefinementStrategy.INCORPORATE:
            return await self._refine_incorporate(query, current_hypothesis, results)
        elif strategy == RefinementStrategy.CONTRAST:
            return await self._refine_contrast(query, current_hypothesis, results)
        elif strategy == RefinementStrategy.EXPAND:
            return await self._refine_expand(query, current_hypothesis, results, quality)
        elif strategy == RefinementStrategy.FOCUS:
            return await self._refine_focus(query, current_hypothesis, results)
        elif strategy == RefinementStrategy.DIVERSIFY:
            return await self._refine_diversify(query, current_hypothesis, results)
        else:
            return current_hypothesis

    async def _refine_incorporate(
        self,
        query: str,
        hypothesis: str,
        results: List[Dict[str, Any]]
    ) -> str:
        """Incorporate retrieved content into hypothesis."""
        # Get high-scoring result snippets
        snippets = []
        for r in results[:3]:
            if r.get('score', 0) > 0.5:
                content = r.get('content', '')[:150]
                snippets.append(content)

        if not snippets:
            return hypothesis

        context = "\n".join(snippets)

        prompt = f"""Improve this hypothetical passage by incorporating relevant information from these retrieved documents.

QUERY: {query}

CURRENT PASSAGE:
{hypothesis}

RETRIEVED INFORMATION:
{context}

Write an improved, more accurate passage that incorporates relevant details from the retrieved documents.
Keep it focused on answering the query. Be specific and factual.

IMPROVED PASSAGE:"""

        return await self._generate_refinement(prompt, hypothesis)

    async def _refine_contrast(
        self,
        query: str,
        hypothesis: str,
        results: List[Dict[str, Any]]
    ) -> str:
        """Generate hypothesis that contrasts with poor results."""
        # Get low-scoring results to avoid
        low_scoring = [r for r in results if r.get('score', 0) < 0.5]
        if not low_scoring:
            return hypothesis

        avoid_content = "\n".join(r.get('content', '')[:100] for r in low_scoring[:2])

        prompt = f"""Write a passage that better answers this query by avoiding the irrelevant aspects found in poor search results.

QUERY: {query}

CONTENT TO AVOID (not relevant):
{avoid_content}

Write a focused passage that directly answers the query without including the irrelevant aspects above.

PASSAGE:"""

        return await self._generate_refinement(prompt, hypothesis)

    async def _refine_expand(
        self,
        query: str,
        hypothesis: str,
        results: List[Dict[str, Any]],
        quality: RetrievalQuality
    ) -> str:
        """Expand hypothesis with missing aspects."""
        # Identify query terms not covered
        query_terms = set(query.lower().split())
        result_text = " ".join(r.get('content', '') for r in results).lower()
        missing_terms = [t for t in query_terms if t not in result_text and len(t) > 3]

        if not missing_terms:
            return hypothesis

        prompt = f"""Expand this passage to better cover aspects of the query that are currently missing.

QUERY: {query}

CURRENT PASSAGE:
{hypothesis}

MISSING ASPECTS: The following query terms are not well covered: {', '.join(missing_terms[:5])}

Write an expanded passage that covers these missing aspects while still answering the main query.

EXPANDED PASSAGE:"""

        return await self._generate_refinement(prompt, hypothesis)

    async def _refine_focus(
        self,
        query: str,
        hypothesis: str,
        results: List[Dict[str, Any]]
    ) -> str:
        """Focus hypothesis on high-scoring aspects."""
        # Get terms from high-scoring results
        high_scoring = [r for r in results if r.get('score', 0) > 0.7]
        if not high_scoring:
            return hypothesis

        high_content = " ".join(r.get('content', '')[:100] for r in high_scoring)

        prompt = f"""Refine this passage to focus more on the aspects that retrieved high-quality results.

QUERY: {query}

CURRENT PASSAGE:
{hypothesis}

HIGH-QUALITY ASPECTS (from best results):
{high_content[:300]}

Write a refined passage that focuses on these high-quality aspects.

FOCUSED PASSAGE:"""

        return await self._generate_refinement(prompt, hypothesis)

    async def _refine_diversify(
        self,
        query: str,
        hypothesis: str,
        results: List[Dict[str, Any]]
    ) -> str:
        """Generate a diverse alternative hypothesis."""
        prompt = f"""Write a different passage that answers this query from a new angle.

QUERY: {query}

PREVIOUS ATTEMPT:
{hypothesis[:200]}

Write a passage that answers the same query but from a completely different angle or perspective.
Use different terminology and examples.

ALTERNATIVE PASSAGE:"""

        return await self._generate_refinement(prompt, hypothesis)

    async def _generate_refinement(self, prompt: str, fallback: str) -> str:
        """Generate refined hypothesis using LLM."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.generator.ollama_url}/api/generate",
                    json={
                        "model": self.generator.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.6, "num_predict": 300}
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        refined = data.get('response', '').strip()
                        if refined and len(refined) > 20:
                            return refined
        except Exception as e:
            logger.error(f"Hypothesis refinement failed: {e}")

        return fallback

    def select_strategy(
        self,
        quality: RetrievalQuality,
        iteration: int
    ) -> Tuple[RefinementStrategy, str]:
        """
        Select refinement strategy based on quality metrics.

        Args:
            quality: Current retrieval quality
            iteration: Current iteration number

        Returns:
            Tuple of (strategy, reason)
        """
        # Early iterations: try incorporation
        if iteration <= 1 and quality.avg_score > 0.4:
            return RefinementStrategy.INCORPORATE, "Incorporating high-quality retrieved content"

        # Low coverage: expand
        if quality.coverage < 0.5:
            return RefinementStrategy.EXPAND, f"Low coverage ({quality.coverage:.1%}), expanding hypothesis"

        # Low relevance but decent scores: focus
        if quality.relevance < 0.5 and quality.avg_score > 0.5:
            return RefinementStrategy.FOCUS, f"Low relevance ({quality.relevance:.1%}), focusing on high-scoring aspects"

        # Low diversity: diversify
        if quality.diversity < 0.5:
            return RefinementStrategy.DIVERSIFY, f"Low diversity ({quality.diversity:.1%}), trying new angle"

        # Very low scores: contrast
        if quality.avg_score < 0.4:
            return RefinementStrategy.CONTRAST, f"Low scores ({quality.avg_score:.2f}), contrasting with poor results"

        # Default: incorporate
        return RefinementStrategy.INCORPORATE, "Standard refinement with retrieved content"


class IterativeHyDE:
    """
    Iterative HyDE with feedback-driven refinement.

    Implements the Generate → Retrieve → Refine → Repeat cycle
    with quality-aware stopping criteria.
    """

    def __init__(
        self,
        hyde_generator: HyDEGenerator,
        max_iterations: int = 3,
        quality_threshold: float = 0.7,
        improvement_threshold: float = 0.05,
        min_iterations: int = 1
    ):
        self.generator = hyde_generator
        self.max_iterations = max_iterations
        self.quality_threshold = quality_threshold
        self.improvement_threshold = improvement_threshold
        self.min_iterations = min_iterations

        self.quality_estimator = QualityEstimator(hyde_generator)
        self.refiner = HypothesisRefiner(hyde_generator)

    async def retrieve_iteratively(
        self,
        query: str,
        vector_store: Any,
        top_k: int = 5,
        context_hint: Optional[str] = None
    ) -> IterativeHyDEResult:
        """
        Perform iterative HyDE retrieval with refinement.

        Args:
            query: User's query
            vector_store: Vector store for similarity search
            top_k: Number of results to return
            context_hint: Optional domain hint

        Returns:
            IterativeHyDEResult with final results and iteration history
        """
        start_time = time.time()
        iterations: List[IterationResult] = []
        improvement_trajectory: List[float] = []

        # Generate initial hypothesis
        hypotheses = await self.generator.generate_hypothetical_document(
            query=query,
            context_hint=context_hint
        )
        current_hypothesis = hypotheses[0] if hypotheses else query

        best_hypothesis = current_hypothesis
        best_results: List[Dict[str, Any]] = []
        best_quality: Optional[RetrievalQuality] = None
        previous_quality: Optional[RetrievalQuality] = None

        for iteration in range(self.max_iterations):
            iter_start = time.time()

            # Retrieve with current hypothesis
            results = await vector_store.search(
                query=current_hypothesis,
                top_k=top_k * 2  # Get more for quality estimation
            )

            # Estimate quality
            quality = self.quality_estimator.estimate_quality(
                query=query,
                hypothesis=current_hypothesis,
                results=results
            )

            # Track improvement
            improvement_trajectory.append(quality.overall_quality)

            # Determine refinement strategy
            strategy, reason = self.refiner.select_strategy(quality, iteration)

            # Record iteration
            iter_result = IterationResult(
                iteration=iteration + 1,
                hypothesis=current_hypothesis,
                retrieved_docs=results[:top_k],
                quality=quality,
                refinement_strategy=strategy if iteration < self.max_iterations - 1 else None,
                refinement_reason=reason,
                duration_ms=(time.time() - iter_start) * 1000
            )
            iterations.append(iter_result)

            # Update best if improved
            if best_quality is None or quality.overall_quality > best_quality.overall_quality:
                best_hypothesis = current_hypothesis
                best_results = results[:top_k]
                best_quality = quality

            # Check stopping conditions
            stopping_reason = self._check_stopping(
                quality=quality,
                previous_quality=previous_quality,
                iteration=iteration
            )

            if stopping_reason:
                return IterativeHyDEResult(
                    query=query,
                    final_hypothesis=best_hypothesis,
                    final_results=best_results,
                    final_quality=best_quality,
                    iterations=iterations,
                    total_iterations=iteration + 1,
                    stopping_reason=stopping_reason,
                    total_duration_ms=(time.time() - start_time) * 1000,
                    improvement_trajectory=improvement_trajectory
                )

            # Refine hypothesis for next iteration
            current_hypothesis = await self.refiner.refine(
                query=query,
                current_hypothesis=current_hypothesis,
                results=results,
                quality=quality,
                strategy=strategy
            )

            previous_quality = quality

        # Reached max iterations
        return IterativeHyDEResult(
            query=query,
            final_hypothesis=best_hypothesis,
            final_results=best_results,
            final_quality=best_quality or quality,
            iterations=iterations,
            total_iterations=self.max_iterations,
            stopping_reason=StoppingReason.MAX_ITERATIONS,
            total_duration_ms=(time.time() - start_time) * 1000,
            improvement_trajectory=improvement_trajectory
        )

    def _check_stopping(
        self,
        quality: RetrievalQuality,
        previous_quality: Optional[RetrievalQuality],
        iteration: int
    ) -> Optional[StoppingReason]:
        """Check if iteration should stop."""
        # Must complete minimum iterations
        if iteration < self.min_iterations - 1:
            return None

        # Quality threshold reached
        if quality.overall_quality >= self.quality_threshold:
            return StoppingReason.QUALITY_THRESHOLD

        # No improvement
        if previous_quality:
            improvement = quality.overall_quality - previous_quality.overall_quality
            if improvement < self.improvement_threshold and iteration > 0:
                return StoppingReason.NO_IMPROVEMENT

            # Convergence (very small change)
            if abs(improvement) < 0.01:
                return StoppingReason.CONVERGENCE

        return None


class HyDEOptimizer:
    """
    Learns which HyDE strategies work best for different query types.

    Tracks performance metrics and adapts strategy selection over time.
    """

    def __init__(self):
        # Track performance by query characteristics
        self.strategy_performance: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.query_type_strategies: Dict[str, RefinementStrategy] = {}

    def record_result(
        self,
        query: str,
        strategy: RefinementStrategy,
        initial_quality: float,
        final_quality: float
    ):
        """Record the performance of a strategy."""
        query_type = self._classify_query(query)
        improvement = final_quality - initial_quality

        self.strategy_performance[query_type][strategy.value].append(improvement)

        # Update best strategy for query type
        self._update_best_strategy(query_type)

    def get_recommended_strategy(self, query: str) -> Optional[RefinementStrategy]:
        """Get recommended starting strategy for a query type."""
        query_type = self._classify_query(query)
        return self.query_type_strategies.get(query_type)

    def _classify_query(self, query: str) -> str:
        """Classify query into a type."""
        query_lower = query.lower()

        if any(w in query_lower for w in ["how to", "how do", "steps", "guide"]):
            return "procedural"
        elif any(w in query_lower for w in ["what is", "define", "meaning"]):
            return "definitional"
        elif any(w in query_lower for w in ["why", "reason", "cause"]):
            return "explanatory"
        elif any(w in query_lower for w in ["compare", "difference", "versus"]):
            return "comparative"
        elif any(w in query_lower for w in ["best", "recommend", "should"]):
            return "advisory"
        else:
            return "general"

    def _update_best_strategy(self, query_type: str):
        """Update best strategy based on accumulated performance."""
        if query_type not in self.strategy_performance:
            return

        best_strategy = None
        best_avg = float('-inf')

        for strategy_name, improvements in self.strategy_performance[query_type].items():
            if len(improvements) >= 3:  # Need enough data
                avg_improvement = sum(improvements) / len(improvements)
                if avg_improvement > best_avg:
                    best_avg = avg_improvement
                    best_strategy = RefinementStrategy(strategy_name)

        if best_strategy and best_avg > 0:
            self.query_type_strategies[query_type] = best_strategy

    def get_stats(self) -> Dict[str, Any]:
        """Get optimizer statistics."""
        stats = {
            "query_type_strategies": {
                qt: s.value for qt, s in self.query_type_strategies.items()
            },
            "performance_data": {}
        }

        for query_type, strategies in self.strategy_performance.items():
            stats["performance_data"][query_type] = {
                strategy: {
                    "count": len(improvements),
                    "avg_improvement": sum(improvements) / len(improvements) if improvements else 0
                }
                for strategy, improvements in strategies.items()
            }

        return stats


# =============================================================================
# ENHANCED HYDE RETRIEVER (CUTTING-EDGE)
# =============================================================================

class EnhancedHyDERetriever:
    """
    Enhanced HyDE Retriever with iterative refinement.

    Combines all cutting-edge HyDE features:
    - Iterative Generate → Retrieve → Refine → Repeat
    - Feedback-driven hypothesis refinement
    - Quality-aware stopping
    - Strategy optimization
    """

    def __init__(
        self,
        hyde_generator: HyDEGenerator,
        max_iterations: int = 3,
        quality_threshold: float = 0.7,
        enable_optimization: bool = True
    ):
        self.generator = hyde_generator
        self.iterative_hyde = IterativeHyDE(
            hyde_generator=hyde_generator,
            max_iterations=max_iterations,
            quality_threshold=quality_threshold
        )
        self.optimizer = HyDEOptimizer() if enable_optimization else None
        self.enable_optimization = enable_optimization

    async def retrieve(
        self,
        query: str,
        vector_store: Any,
        top_k: int = 5,
        context_hint: Optional[str] = None,
        use_iterative: bool = True
    ) -> Dict[str, Any]:
        """
        Retrieve documents using enhanced HyDE.

        Args:
            query: User's query
            vector_store: Vector store for search
            top_k: Number of results
            context_hint: Optional domain hint
            use_iterative: Whether to use iterative refinement

        Returns:
            Dict with results and metadata
        """
        if not use_iterative:
            # Fall back to standard HyDE
            hypotheses = await self.generator.generate_hypothetical_document(
                query=query,
                context_hint=context_hint
            )
            hypothesis = hypotheses[0] if hypotheses else query

            results = await vector_store.search(query=hypothesis, top_k=top_k)

            return {
                "results": results,
                "hypothesis": hypothesis,
                "method": "standard_hyde",
                "iterations": 1
            }

        # Use iterative HyDE
        result = await self.iterative_hyde.retrieve_iteratively(
            query=query,
            vector_store=vector_store,
            top_k=top_k,
            context_hint=context_hint
        )

        # Record for optimization
        if self.optimizer and result.iterations:
            initial_quality = result.iterations[0].quality.overall_quality
            final_quality = result.final_quality.overall_quality
            strategy = result.iterations[-1].refinement_strategy

            if strategy:
                self.optimizer.record_result(
                    query=query,
                    strategy=strategy,
                    initial_quality=initial_quality,
                    final_quality=final_quality
                )

        return {
            "results": result.final_results,
            "hypothesis": result.final_hypothesis,
            "method": "iterative_hyde",
            "iterations": result.total_iterations,
            "stopping_reason": result.stopping_reason.value,
            "quality": result.final_quality.to_dict(),
            "improvement_trajectory": result.improvement_trajectory,
            "iteration_details": [it.to_dict() for it in result.iterations]
        }

    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        if not self.optimizer:
            return {"optimization_enabled": False}

        return {
            "optimization_enabled": True,
            **self.optimizer.get_stats()
        }


# =============================================================================
# ENHANCED FACTORY FUNCTIONS
# =============================================================================

_iterative_hyde: Optional[IterativeHyDE] = None
_enhanced_retriever: Optional[EnhancedHyDERetriever] = None


def get_iterative_hyde(
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b",
    max_iterations: int = 3,
    quality_threshold: float = 0.7
) -> IterativeHyDE:
    """Get or create iterative HyDE instance."""
    global _iterative_hyde
    if _iterative_hyde is None:
        generator = get_hyde_generator(ollama_url, model)
        _iterative_hyde = IterativeHyDE(
            hyde_generator=generator,
            max_iterations=max_iterations,
            quality_threshold=quality_threshold
        )
        logger.info("Iterative HyDE initialized with feedback-driven refinement")
    return _iterative_hyde


def get_enhanced_hyde_retriever(
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b",
    max_iterations: int = 3,
    quality_threshold: float = 0.7,
    enable_optimization: bool = True
) -> EnhancedHyDERetriever:
    """Get or create enhanced HyDE retriever instance."""
    global _enhanced_retriever
    if _enhanced_retriever is None:
        generator = get_hyde_generator(ollama_url, model)
        _enhanced_retriever = EnhancedHyDERetriever(
            hyde_generator=generator,
            max_iterations=max_iterations,
            quality_threshold=quality_threshold,
            enable_optimization=enable_optimization
        )
        logger.info("Enhanced HyDE Retriever initialized with iterative refinement and optimization")
    return _enhanced_retriever

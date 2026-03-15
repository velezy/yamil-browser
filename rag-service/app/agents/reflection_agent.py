"""
Reflection Agent

Evaluates response quality and determines whether to iterate.
Provides structured feedback for improvement.
"""

import logging
import time
from typing import Optional, TYPE_CHECKING
from dataclasses import dataclass

from .base_agent import BaseAgent, AgentResult, AgentContext, AgentRole

if TYPE_CHECKING:
    from ..ollama.client import OllamaOptimizedClient
    from ..ollama.model_router import ModelRouter

logger = logging.getLogger(__name__)


@dataclass
class EvaluationResult:
    """Structured evaluation result"""
    overall_score: float  # 0.0 - 1.0
    passes_threshold: bool
    should_iterate: bool

    # Individual scores
    completeness: float  # Did it answer all parts?
    accuracy: float  # Is it grounded in context?
    relevance: float  # Is it relevant to the query?
    clarity: float  # Is it clear and well-structured?

    # Feedback
    strengths: list[str]
    weaknesses: list[str]
    suggestions: list[str]
    missing_aspects: list[str]

    def to_dict(self) -> dict:
        return {
            "overall_score": self.overall_score,
            "passes_threshold": self.passes_threshold,
            "should_iterate": self.should_iterate,
            "scores": {
                "completeness": self.completeness,
                "accuracy": self.accuracy,
                "relevance": self.relevance,
                "clarity": self.clarity,
            },
            "feedback": {
                "strengths": self.strengths,
                "weaknesses": self.weaknesses,
                "suggestions": self.suggestions,
                "missing_aspects": self.missing_aspects,
            },
        }


class ReflectionAgent(BaseAgent):
    """
    Evaluates response quality and determines next steps.

    Evaluation criteria:
    - Completeness: Did it answer all sub-queries?
    - Accuracy: Is it grounded in retrieved context?
    - Relevance: Is it directly relevant to the query?
    - Clarity: Is it well-structured and clear?

    Decision logic:
    - Score >= threshold: Return response
    - Score < threshold AND iterations < max: Loop back
    - Score < threshold AND iterations >= max: Return with warning
    """

    def __init__(
        self,
        client: 'OllamaOptimizedClient',
        router: 'ModelRouter',
        default_model: Optional[str] = None,
        quality_threshold: float = 0.75,
        max_iterations: int = 3,
    ):
        super().__init__(
            client=client,
            router=router,
            role=AgentRole.REFLECTION,
            default_model=default_model,
        )
        self.quality_threshold = quality_threshold
        self.max_iterations = max_iterations

    def _get_default_task_type(self) -> str:
        return "reflection"  # Quality model for evaluation

    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Evaluate the current response.

        Updates context with:
        - quality_score: Overall quality score
        - feedback: Structured feedback for improvement
        """
        self._log_start(context)
        start_time = time.time()

        try:
            # Perform evaluation
            evaluation = await self._evaluate_response(context)

            # Update context
            context.quality_score = evaluation.overall_score
            context.feedback = self._format_feedback(evaluation)

            # Determine if we should iterate
            should_iterate = (
                not evaluation.passes_threshold
                and context.iteration < self.max_iterations
            )

            duration_ms = (time.time() - start_time) * 1000

            result = AgentResult(
                success=True,
                data={
                    "evaluation": evaluation.to_dict(),
                    "should_iterate": should_iterate,
                    "iteration": context.iteration,
                    "max_iterations": self.max_iterations,
                },
                duration_ms=duration_ms,
                model_used=self.get_model(),
                metadata={
                    "score": evaluation.overall_score,
                    "passes": evaluation.passes_threshold,
                },
            )

            self._log_complete(result, context)

            # Log decision
            if should_iterate:
                logger.info(
                    f"🔄 Reflection: Score {evaluation.overall_score:.2f} < {self.quality_threshold}, "
                    f"iterating (attempt {context.iteration + 1}/{self.max_iterations})"
                )
            else:
                logger.info(
                    f"✅ Reflection: Score {evaluation.overall_score:.2f}, "
                    f"{'passed' if evaluation.passes_threshold else 'max iterations reached'}"
                )

            return result

        except Exception as e:
            logger.error(f"{self.name} error: {e}")
            # On error, pass through without iteration
            context.quality_score = 0.5
            context.feedback = f"Evaluation error: {str(e)}"

            return AgentResult(
                success=False,
                data={"should_iterate": False},
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )

    async def _evaluate_response(self, context: AgentContext) -> EvaluationResult:
        """
        Use LLM to evaluate response quality.
        """
        system = """You are a response quality evaluator. Assess AI responses for accuracy, completeness, and clarity.

Be strict but fair. A score of 0.75+ means the response is good enough to return.

Evaluation criteria:
- Completeness (0-1): Does it address all parts of the query?
- Accuracy (0-1): Is it factually grounded in the provided context?
- Relevance (0-1): Does it directly answer what was asked?
- Clarity (0-1): Is it well-organized and easy to understand?"""

        # Build evaluation prompt
        prompt = self._build_evaluation_prompt(context)

        parsed, _ = await self._call_llm_json(
            prompt=prompt,
            system=system,
            options={
                "num_predict": 512,
                "temperature": 0.2,  # More consistent evaluation
            },
        )

        # Parse scores with defaults
        scores = parsed.get('scores', {})
        completeness = min(1.0, max(0.0, float(scores.get('completeness', 0.5))))
        accuracy = min(1.0, max(0.0, float(scores.get('accuracy', 0.5))))
        relevance = min(1.0, max(0.0, float(scores.get('relevance', 0.5))))
        clarity = min(1.0, max(0.0, float(scores.get('clarity', 0.5))))

        # Calculate overall score (weighted average)
        overall = (
            0.30 * completeness +
            0.30 * accuracy +
            0.25 * relevance +
            0.15 * clarity
        )

        # Parse feedback
        feedback = parsed.get('feedback', {})
        strengths = feedback.get('strengths', [])
        weaknesses = feedback.get('weaknesses', [])
        suggestions = feedback.get('suggestions', [])
        missing = feedback.get('missing_aspects', [])

        return EvaluationResult(
            overall_score=overall,
            passes_threshold=overall >= self.quality_threshold,
            should_iterate=overall < self.quality_threshold,
            completeness=completeness,
            accuracy=accuracy,
            relevance=relevance,
            clarity=clarity,
            strengths=strengths if isinstance(strengths, list) else [],
            weaknesses=weaknesses if isinstance(weaknesses, list) else [],
            suggestions=suggestions if isinstance(suggestions, list) else [],
            missing_aspects=missing if isinstance(missing, list) else [],
        )

    def _build_evaluation_prompt(self, context: AgentContext) -> str:
        """Build prompt for response evaluation"""
        parts = []

        # Original query
        parts.append(f"## Original Query\n{context.query}")

        # Sub-queries that needed answering
        if len(context.sub_queries) > 1:
            parts.append("\n## Sub-Questions to Address")
            for i, sq in enumerate(context.sub_queries, 1):
                parts.append(f"{i}. {sq}")

        # Retrieved context (summarized)
        if context.retrieved_chunks:
            parts.append(f"\n## Retrieved Context ({len(context.retrieved_chunks)} chunks)")
            context_preview = []
            for i, chunk in enumerate(context.retrieved_chunks[:3], 1):
                content = chunk.get('content', '')[:200]
                context_preview.append(f"[{i}] {content}...")
            parts.append("\n".join(context_preview))

        # Response to evaluate
        parts.append("\n## Response to Evaluate")
        parts.append(context.current_response)

        # Previous iteration feedback if any
        if context.iteration > 0:
            parts.append(f"\n## Previous Iteration")
            parts.append(f"This is iteration {context.iteration + 1}")
            parts.append(f"Previous score: {context.quality_score:.2f}")

        parts.append("""
## Evaluation Task

Evaluate this response and provide scores and feedback.

Respond with JSON:
{
    "scores": {
        "completeness": 0.0-1.0,
        "accuracy": 0.0-1.0,
        "relevance": 0.0-1.0,
        "clarity": 0.0-1.0
    },
    "feedback": {
        "strengths": ["list of strengths"],
        "weaknesses": ["list of weaknesses"],
        "suggestions": ["specific improvements"],
        "missing_aspects": ["what's missing from the response"]
    }
}""")

        return "\n".join(parts)

    def _format_feedback(self, evaluation: EvaluationResult) -> str:
        """Format evaluation result into actionable feedback"""
        parts = []

        if evaluation.weaknesses:
            parts.append("Weaknesses:")
            for w in evaluation.weaknesses[:3]:
                parts.append(f"- {w}")

        if evaluation.missing_aspects:
            parts.append("\nMissing:")
            for m in evaluation.missing_aspects[:3]:
                parts.append(f"- {m}")

        if evaluation.suggestions:
            parts.append("\nSuggestions:")
            for s in evaluation.suggestions[:3]:
                parts.append(f"- {s}")

        return "\n".join(parts) if parts else "No specific feedback"

    async def quick_evaluate(
        self,
        query: str,
        response: str,
        context_chunks: list[dict],
    ) -> float:
        """
        Quick evaluation returning just a score.
        Useful for testing or simple checks.
        """
        # Create minimal context
        ctx = AgentContext(
            query=query,
            current_response=response,
            retrieved_chunks=context_chunks,
        )

        result = await self.execute(ctx)

        if result.success:
            return result.data.get('evaluation', {}).get('overall_score', 0.5)
        return 0.5

    def check_factual_grounding(
        self,
        response: str,
        chunks: list[dict],
    ) -> dict:
        """
        Simple heuristic check for factual grounding.
        Checks if key terms from response appear in context.
        """
        # Extract key terms from response (simple approach)
        import re
        response_words = set(re.findall(r'\b\w{4,}\b', response.lower()))

        # Extract terms from chunks
        chunk_text = ' '.join(c.get('content', '') for c in chunks)
        chunk_words = set(re.findall(r'\b\w{4,}\b', chunk_text.lower()))

        # Calculate overlap
        common_words = response_words & chunk_words
        grounding_ratio = len(common_words) / len(response_words) if response_words else 0

        return {
            "grounding_ratio": grounding_ratio,
            "is_grounded": grounding_ratio > 0.3,
            "common_terms": len(common_words),
            "response_terms": len(response_words),
        }

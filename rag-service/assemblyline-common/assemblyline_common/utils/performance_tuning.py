"""
Performance Tuning & Industry Benchmarks
Tracks latency against industry standards and auto-adjusts temperature.

Industry Benchmarks (2025-2026):
- Simple queries: < 500ms (target), < 1s (acceptable)
- Complex RAG queries: < 2s (target), < 5s (acceptable)
- Multi-step reasoning: < 5s (target), < 10s (acceptable)

Temperature Guidelines:
- Factual/RAG queries: 0.1-0.3 (low creativity, high accuracy)
- General chat: 0.5-0.7 (balanced)
- Creative tasks: 0.8-1.0 (high creativity)
"""

import os
import logging
import time
import json
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# INDUSTRY BENCHMARKS (2025-2026 Standards)
# =============================================================================

class QueryComplexity(Enum):
    SIMPLE = "simple"
    STANDARD = "standard"
    COMPLEX = "complex"
    REASONING = "reasoning"


@dataclass
class LatencyBenchmark:
    """Industry-standard latency targets"""
    target_ms: int      # Target latency (ideal)
    acceptable_ms: int  # Acceptable latency (maximum)
    excellent_ms: int   # Excellent performance


# Industry benchmarks based on 2025-2026 enterprise RAG standards
LATENCY_BENCHMARKS: Dict[QueryComplexity, LatencyBenchmark] = {
    QueryComplexity.SIMPLE: LatencyBenchmark(
        excellent_ms=200,
        target_ms=500,
        acceptable_ms=1000
    ),
    QueryComplexity.STANDARD: LatencyBenchmark(
        excellent_ms=500,
        target_ms=1500,
        acceptable_ms=3000
    ),
    QueryComplexity.COMPLEX: LatencyBenchmark(
        excellent_ms=1000,
        target_ms=3000,
        acceptable_ms=5000
    ),
    QueryComplexity.REASONING: LatencyBenchmark(
        excellent_ms=2000,
        target_ms=5000,
        acceptable_ms=10000
    ),
}


@dataclass
class PerformanceRating:
    """Performance rating against benchmarks"""
    rating: str  # "excellent", "good", "acceptable", "slow", "critical"
    latency_ms: float
    benchmark: LatencyBenchmark
    percentile_rank: float  # 0-100, higher is better
    improvement_needed_ms: float  # 0 if meeting target


def rate_latency(
    latency_ms: float,
    complexity: QueryComplexity
) -> PerformanceRating:
    """
    Rate a query's latency against industry benchmarks.

    Returns:
        PerformanceRating with rating and improvement suggestions
    """
    benchmark = LATENCY_BENCHMARKS[complexity]

    if latency_ms <= benchmark.excellent_ms:
        rating = "excellent"
        percentile = 95.0
    elif latency_ms <= benchmark.target_ms:
        rating = "good"
        percentile = 75.0 + (25.0 * (benchmark.target_ms - latency_ms) / (benchmark.target_ms - benchmark.excellent_ms))
    elif latency_ms <= benchmark.acceptable_ms:
        rating = "acceptable"
        percentile = 50.0 + (25.0 * (benchmark.acceptable_ms - latency_ms) / (benchmark.acceptable_ms - benchmark.target_ms))
    elif latency_ms <= benchmark.acceptable_ms * 1.5:
        rating = "slow"
        percentile = 25.0
    else:
        rating = "critical"
        percentile = 10.0

    improvement_needed = max(0, latency_ms - benchmark.target_ms)

    return PerformanceRating(
        rating=rating,
        latency_ms=latency_ms,
        benchmark=benchmark,
        percentile_rank=percentile,
        improvement_needed_ms=improvement_needed
    )


# =============================================================================
# ADAPTIVE TEMPERATURE CONTROL
# =============================================================================

@dataclass
class TemperatureConfig:
    """Temperature configuration for different query types"""
    base_temperature: float
    min_temperature: float
    max_temperature: float

    def adjust(self, modifier: float) -> float:
        """Apply modifier and clamp to range"""
        adjusted = self.base_temperature + modifier
        return max(self.min_temperature, min(self.max_temperature, adjusted))


# Recommended temperatures by query type
TEMPERATURE_CONFIGS: Dict[str, TemperatureConfig] = {
    "factual": TemperatureConfig(base_temperature=0.1, min_temperature=0.0, max_temperature=0.3),
    "rag": TemperatureConfig(base_temperature=0.2, min_temperature=0.1, max_temperature=0.4),
    "reasoning": TemperatureConfig(base_temperature=0.3, min_temperature=0.1, max_temperature=0.5),
    "chat": TemperatureConfig(base_temperature=0.6, min_temperature=0.4, max_temperature=0.8),
    "creative": TemperatureConfig(base_temperature=0.9, min_temperature=0.7, max_temperature=1.0),
    "code": TemperatureConfig(base_temperature=0.2, min_temperature=0.0, max_temperature=0.4),
}


class AdaptiveTemperatureController:
    """
    Automatically adjusts temperature based on:
    1. Query type detection
    2. Historical quality scores
    3. User feedback
    """

    def __init__(self):
        self.history: List[Dict[str, Any]] = []
        self.quality_scores: Dict[str, List[float]] = {}
        self.feedback_scores: Dict[str, List[float]] = {}

    def get_temperature(
        self,
        query_type: str,
        quality_history: Optional[List[float]] = None,
        user_feedback: Optional[float] = None
    ) -> Tuple[float, str]:
        """
        Get recommended temperature with explanation.

        Args:
            query_type: Type of query (factual, rag, chat, creative, code)
            quality_history: Recent quality scores for this type
            user_feedback: Recent user satisfaction (0-1)

        Returns:
            (temperature, reasoning)
        """
        config = TEMPERATURE_CONFIGS.get(query_type, TEMPERATURE_CONFIGS["chat"])
        modifier = 0.0
        reasons = []

        # Adjust based on quality history
        if quality_history and len(quality_history) >= 3:
            avg_quality = sum(quality_history[-5:]) / len(quality_history[-5:])

            if avg_quality < 0.5:
                # Low quality - reduce temperature for more deterministic outputs
                modifier -= 0.1
                reasons.append(f"lowering due to low quality ({avg_quality:.1%})")
            elif avg_quality > 0.85:
                # High quality - can allow slightly more creativity
                modifier += 0.05
                reasons.append(f"slight increase due to high quality ({avg_quality:.1%})")

        # Adjust based on user feedback
        if user_feedback is not None:
            if user_feedback < 0.4:
                # Users unhappy - try lower temperature
                modifier -= 0.15
                reasons.append(f"lowering due to poor feedback ({user_feedback:.1%})")
            elif user_feedback > 0.8:
                # Users happy - maintain or slightly increase
                modifier += 0.02
                reasons.append(f"maintaining due to positive feedback")

        final_temp = config.adjust(modifier)

        if not reasons:
            reasons.append(f"default for {query_type} queries")

        reasoning = f"Temperature {final_temp:.2f}: {', '.join(reasons)}"

        return final_temp, reasoning

    def detect_query_type(self, query: str, has_context: bool = False) -> str:
        """Detect query type for temperature selection."""
        query_lower = query.lower()

        # Code-related
        if any(kw in query_lower for kw in ["code", "function", "implement", "debug", "syntax", "programming"]):
            return "code"

        # Factual/lookup
        if any(kw in query_lower for kw in ["what is", "define", "when did", "who is", "how many"]):
            return "factual"

        # Creative
        if any(kw in query_lower for kw in ["write a story", "creative", "imagine", "poem", "fiction"]):
            return "creative"

        # Reasoning
        if any(kw in query_lower for kw in ["analyze", "compare", "explain why", "reason", "evaluate"]):
            return "reasoning"

        # RAG (has document context)
        if has_context:
            return "rag"

        return "chat"

    def record_result(
        self,
        query_type: str,
        temperature: float,
        quality_score: float,
        user_feedback: Optional[float] = None
    ):
        """Record result for future tuning."""
        self.history.append({
            "timestamp": datetime.utcnow().isoformat(),
            "query_type": query_type,
            "temperature": temperature,
            "quality_score": quality_score,
            "user_feedback": user_feedback
        })

        # Update quality tracking
        if query_type not in self.quality_scores:
            self.quality_scores[query_type] = []
        self.quality_scores[query_type].append(quality_score)

        # Keep only last 100 scores per type
        if len(self.quality_scores[query_type]) > 100:
            self.quality_scores[query_type] = self.quality_scores[query_type][-100:]


# =============================================================================
# FINE-TUNING TRACKING
# =============================================================================

@dataclass
class FineTuningCandidate:
    """A query-response pair suitable for fine-tuning"""
    query: str
    response: str
    quality_score: float
    user_feedback: Optional[float]
    context_used: Optional[str]
    model_used: str
    temperature: float
    timestamp: str
    is_positive_example: bool  # High quality = positive, low = negative


class FineTuningTracker:
    """
    Tracks high/low quality examples for potential fine-tuning.

    Exports in formats compatible with:
    - OpenAI fine-tuning (JSONL)
    - Hugging Face TRL (preference pairs)
    - Ollama custom models
    """

    def __init__(self, storage_path: str = "data/fine_tuning"):
        self.storage_path = storage_path
        self.positive_examples: List[FineTuningCandidate] = []
        self.negative_examples: List[FineTuningCandidate] = []
        self.preference_pairs: List[Tuple[FineTuningCandidate, FineTuningCandidate]] = []

        os.makedirs(storage_path, exist_ok=True)

    def record_example(
        self,
        query: str,
        response: str,
        quality_score: float,
        model_used: str,
        temperature: float = 0.7,
        user_feedback: Optional[float] = None,
        context: Optional[str] = None
    ):
        """Record a query-response pair for fine-tuning consideration."""

        # Determine if this is a positive or negative example
        effective_score = quality_score
        if user_feedback is not None:
            effective_score = (quality_score * 0.6) + (user_feedback * 0.4)

        is_positive = effective_score >= 0.75

        candidate = FineTuningCandidate(
            query=query,
            response=response,
            quality_score=quality_score,
            user_feedback=user_feedback,
            context_used=context[:1000] if context else None,
            model_used=model_used,
            temperature=temperature,
            timestamp=datetime.utcnow().isoformat(),
            is_positive_example=is_positive
        )

        if is_positive:
            self.positive_examples.append(candidate)
            logger.info(f"Recorded positive fine-tuning example (score: {effective_score:.2f})")
        elif effective_score < 0.4:
            self.negative_examples.append(candidate)
            logger.info(f"Recorded negative fine-tuning example (score: {effective_score:.2f})")

    def create_preference_pair(
        self,
        query: str,
        chosen_response: str,
        rejected_response: str,
        chosen_score: float,
        rejected_score: float,
        model_used: str
    ):
        """Create a preference pair for DPO/RLHF training."""
        chosen = FineTuningCandidate(
            query=query,
            response=chosen_response,
            quality_score=chosen_score,
            user_feedback=None,
            context_used=None,
            model_used=model_used,
            temperature=0.7,
            timestamp=datetime.utcnow().isoformat(),
            is_positive_example=True
        )

        rejected = FineTuningCandidate(
            query=query,
            response=rejected_response,
            quality_score=rejected_score,
            user_feedback=None,
            context_used=None,
            model_used=model_used,
            temperature=0.7,
            timestamp=datetime.utcnow().isoformat(),
            is_positive_example=False
        )

        self.preference_pairs.append((chosen, rejected))

    def export_openai_format(self, output_file: Optional[str] = None) -> str:
        """Export positive examples in OpenAI fine-tuning JSONL format."""
        output_file = output_file or f"{self.storage_path}/openai_finetune.jsonl"

        with open(output_file, "w") as f:
            for example in self.positive_examples:
                entry = {
                    "messages": [
                        {"role": "system", "content": "You are a helpful AI assistant with access to a document knowledge base."},
                        {"role": "user", "content": example.query},
                        {"role": "assistant", "content": example.response}
                    ]
                }
                f.write(json.dumps(entry) + "\n")

        logger.info(f"Exported {len(self.positive_examples)} examples to {output_file}")
        return output_file

    def export_dpo_format(self, output_file: Optional[str] = None) -> str:
        """Export preference pairs for DPO training (TRL format)."""
        output_file = output_file or f"{self.storage_path}/dpo_pairs.jsonl"

        with open(output_file, "w") as f:
            for chosen, rejected in self.preference_pairs:
                entry = {
                    "prompt": chosen.query,
                    "chosen": chosen.response,
                    "rejected": rejected.response
                }
                f.write(json.dumps(entry) + "\n")

        logger.info(f"Exported {len(self.preference_pairs)} preference pairs to {output_file}")
        return output_file

    def get_stats(self) -> Dict[str, Any]:
        """Get fine-tuning tracking statistics."""
        return {
            "positive_examples": len(self.positive_examples),
            "negative_examples": len(self.negative_examples),
            "preference_pairs": len(self.preference_pairs),
            "avg_positive_score": sum(e.quality_score for e in self.positive_examples) / len(self.positive_examples) if self.positive_examples else 0,
            "avg_negative_score": sum(e.quality_score for e in self.negative_examples) / len(self.negative_examples) if self.negative_examples else 0,
            "ready_for_fine_tuning": len(self.positive_examples) >= 100
        }


# =============================================================================
# PERFORMANCE MONITORING
# =============================================================================

class PerformanceMonitor:
    """
    Centralized performance monitoring with:
    - Latency tracking vs benchmarks
    - Adaptive temperature control
    - Fine-tuning candidate collection
    """

    def __init__(self):
        self.temperature_controller = AdaptiveTemperatureController()
        self.fine_tuning_tracker = FineTuningTracker()
        self.latency_history: List[Dict[str, Any]] = []

    def record_query(
        self,
        query: str,
        response: str,
        latency_ms: float,
        complexity: QueryComplexity,
        quality_score: float,
        model_used: str,
        temperature: float,
        has_context: bool = False,
        user_feedback: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Record a complete query cycle and get recommendations.

        Returns:
            Performance report with ratings and suggestions
        """
        # Rate latency
        latency_rating = rate_latency(latency_ms, complexity)

        # Detect query type and record for temperature tuning
        query_type = self.temperature_controller.detect_query_type(query, has_context)
        self.temperature_controller.record_result(query_type, temperature, quality_score, user_feedback)

        # Record for fine-tuning if notable
        if quality_score >= 0.75 or quality_score < 0.4:
            self.fine_tuning_tracker.record_example(
                query=query,
                response=response,
                quality_score=quality_score,
                model_used=model_used,
                temperature=temperature,
                user_feedback=user_feedback
            )

        # Store latency record
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "complexity": complexity.value,
            "latency_ms": latency_ms,
            "rating": latency_rating.rating,
            "percentile": latency_rating.percentile_rank,
            "quality_score": quality_score,
            "query_type": query_type,
            "temperature": temperature
        }
        self.latency_history.append(record)

        # Generate recommendations
        recommendations = []

        if latency_rating.rating in ["slow", "critical"]:
            recommendations.append(f"Latency {latency_ms:.0f}ms exceeds target {latency_rating.benchmark.target_ms}ms")
            recommendations.append("Consider: smaller model, caching, or reduced context")

        if quality_score < 0.6:
            recommendations.append(f"Quality score {quality_score:.1%} below threshold")
            new_temp, reasoning = self.temperature_controller.get_temperature(
                query_type,
                self.temperature_controller.quality_scores.get(query_type, [])
            )
            if abs(new_temp - temperature) > 0.05:
                recommendations.append(f"Suggested temperature adjustment: {temperature:.2f} -> {new_temp:.2f}")

        return {
            "latency": {
                "ms": latency_ms,
                "rating": latency_rating.rating,
                "percentile": latency_rating.percentile_rank,
                "benchmark_target_ms": latency_rating.benchmark.target_ms,
                "improvement_needed_ms": latency_rating.improvement_needed_ms
            },
            "quality": {
                "score": quality_score,
                "user_feedback": user_feedback
            },
            "temperature": {
                "used": temperature,
                "query_type": query_type
            },
            "recommendations": recommendations,
            "fine_tuning_stats": self.fine_tuning_tracker.get_stats()
        }

    def get_summary(self, days: int = 7) -> Dict[str, Any]:
        """Get performance summary for the last N days."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        recent = [r for r in self.latency_history
                  if datetime.fromisoformat(r["timestamp"]) > cutoff]

        if not recent:
            return {"message": "No data available"}

        latencies = [r["latency_ms"] for r in recent]
        qualities = [r["quality_score"] for r in recent]

        ratings_count = {}
        for r in recent:
            ratings_count[r["rating"]] = ratings_count.get(r["rating"], 0) + 1

        return {
            "period_days": days,
            "total_queries": len(recent),
            "latency": {
                "avg_ms": sum(latencies) / len(latencies),
                "min_ms": min(latencies),
                "max_ms": max(latencies),
                "p50_ms": sorted(latencies)[len(latencies) // 2],
                "p95_ms": sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) >= 20 else max(latencies)
            },
            "quality": {
                "avg_score": sum(qualities) / len(qualities),
                "min_score": min(qualities),
                "max_score": max(qualities)
            },
            "ratings_distribution": ratings_count,
            "fine_tuning": self.fine_tuning_tracker.get_stats(),
            "industry_comparison": {
                "simple_target_ms": LATENCY_BENCHMARKS[QueryComplexity.SIMPLE].target_ms,
                "standard_target_ms": LATENCY_BENCHMARKS[QueryComplexity.STANDARD].target_ms,
                "complex_target_ms": LATENCY_BENCHMARKS[QueryComplexity.COMPLEX].target_ms
            }
        }


# Global instance
_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get or create the global performance monitor."""
    global _monitor
    if _monitor is None:
        _monitor = PerformanceMonitor()
    return _monitor

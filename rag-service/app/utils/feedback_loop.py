"""
Self-Improvement Feedback Loop System

Implements online learning and self-improvement for the agentic RAG system.
Based on RLHF/RLAIF concepts from HuggingFace TRL.

Features:
- User feedback collection (thumbs up/down, ratings)
- Preference pair generation for reward modeling
- Response quality tracking and metrics
- Automatic prompt optimization
- A/B testing for retrieval strategies
- Reflection-based self-assessment
- Fine-tuning data preparation (DPO/ORPO compatible)

References:
- HuggingFace TRL: https://huggingface.co/docs/trl
- RLAIF: https://arxiv.org/abs/2309.00267
- DeepSeekMath GRPO: Group Relative Policy Optimization
"""

import asyncio
import json
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Callable
from collections import defaultdict
import random

logger = logging.getLogger(__name__)


# =============================================================================
# DATA MODELS
# =============================================================================

class FeedbackType(str, Enum):
    """Types of user feedback"""
    THUMBS_UP = "thumbs_up"
    THUMBS_DOWN = "thumbs_down"
    RATING = "rating"  # 1-5 scale
    CORRECTION = "correction"  # User provided correct answer
    REGENERATE = "regenerate"  # User asked for regeneration
    COPY = "copy"  # User copied the response
    IGNORE = "ignore"  # User ignored the response


class ResponseQuality(str, Enum):
    """Quality levels for responses"""
    EXCELLENT = "excellent"  # Score >= 0.9
    GOOD = "good"  # Score >= 0.7
    ACCEPTABLE = "acceptable"  # Score >= 0.5
    POOR = "poor"  # Score < 0.5


@dataclass
class FeedbackEntry:
    """A single feedback entry"""
    id: str
    query: str
    response: str
    context: List[str]
    feedback_type: FeedbackType
    feedback_value: Optional[Any]  # Rating value, correction text, etc.
    reflection_score: float  # From reflection agent
    user_id: Optional[str]
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "query": self.query,
            "response": self.response[:500],  # Truncate for storage
            "context_count": len(self.context),
            "feedback_type": self.feedback_type.value,
            "feedback_value": self.feedback_value,
            "reflection_score": self.reflection_score,
            "user_id": self.user_id,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class PreferencePair:
    """
    A preference pair for reward model training.

    Format compatible with HuggingFace TRL DPO/ORPO trainers.
    """
    prompt: str
    chosen: str  # Preferred response
    rejected: str  # Non-preferred response
    chosen_score: float
    rejected_score: float
    margin: float  # Score difference

    def to_trl_format(self) -> Dict[str, str]:
        """Convert to TRL DPO training format"""
        return {
            "prompt": self.prompt,
            "chosen": self.chosen,
            "rejected": self.rejected,
        }


@dataclass
class QualityMetrics:
    """Aggregated quality metrics"""
    total_responses: int
    positive_feedback_rate: float
    average_reflection_score: float
    regeneration_rate: float
    copy_rate: float
    quality_distribution: Dict[str, int]
    trend: str  # "improving", "stable", "declining"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_responses": self.total_responses,
            "positive_feedback_rate": round(self.positive_feedback_rate, 3),
            "average_reflection_score": round(self.average_reflection_score, 3),
            "regeneration_rate": round(self.regeneration_rate, 3),
            "copy_rate": round(self.copy_rate, 3),
            "quality_distribution": self.quality_distribution,
            "trend": self.trend,
        }


@dataclass
class ABTestResult:
    """Results from A/B testing"""
    test_name: str
    variant_a: str
    variant_b: str
    a_count: int
    b_count: int
    a_success_rate: float
    b_success_rate: float
    winner: str
    confidence: float
    start_time: datetime
    end_time: Optional[datetime] = None


# =============================================================================
# FEEDBACK STORE
# =============================================================================

class FeedbackStore:
    """
    In-memory feedback store with persistence hooks.

    Can be extended to use PostgreSQL or other persistent storage.
    """

    def __init__(self, max_entries: int = 10000):
        self.entries: Dict[str, FeedbackEntry] = {}
        self.by_query: Dict[str, List[str]] = defaultdict(list)  # query_hash -> entry_ids
        self.by_user: Dict[str, List[str]] = defaultdict(list)
        self.max_entries = max_entries

    def add(self, entry: FeedbackEntry) -> None:
        """Add a feedback entry"""
        self.entries[entry.id] = entry
        query_hash = hashlib.md5(entry.query.encode()).hexdigest()[:16]
        self.by_query[query_hash].append(entry.id)
        if entry.user_id:
            self.by_user[entry.user_id].append(entry.id)

        # Evict old entries if needed
        if len(self.entries) > self.max_entries:
            self._evict_oldest()

    def get(self, entry_id: str) -> Optional[FeedbackEntry]:
        """Get a feedback entry by ID"""
        return self.entries.get(entry_id)

    def get_for_query(self, query: str) -> List[FeedbackEntry]:
        """Get all feedback entries for a query"""
        query_hash = hashlib.md5(query.encode()).hexdigest()[:16]
        entry_ids = self.by_query.get(query_hash, [])
        return [self.entries[eid] for eid in entry_ids if eid in self.entries]

    def get_recent(self, hours: int = 24) -> List[FeedbackEntry]:
        """Get recent feedback entries"""
        cutoff = datetime.now() - timedelta(hours=hours)
        return [e for e in self.entries.values() if e.timestamp > cutoff]

    def _evict_oldest(self) -> None:
        """Remove oldest 10% of entries"""
        sorted_entries = sorted(self.entries.items(), key=lambda x: x[1].timestamp)
        to_remove = len(sorted_entries) // 10
        for entry_id, _ in sorted_entries[:to_remove]:
            del self.entries[entry_id]


# =============================================================================
# FEEDBACK LOOP MANAGER
# =============================================================================

class FeedbackLoopManager:
    """
    Manages the self-improvement feedback loop.

    Collects feedback, generates preference pairs, tracks metrics,
    and provides data for fine-tuning.
    """

    def __init__(
        self,
        store: Optional[FeedbackStore] = None,
        reflection_threshold: float = 0.7,
        min_pairs_for_training: int = 100,
    ):
        self.store = store or FeedbackStore()
        self.reflection_threshold = reflection_threshold
        self.min_pairs_for_training = min_pairs_for_training
        self.ab_tests: Dict[str, Dict[str, Any]] = {}
        self._preference_cache: List[PreferencePair] = []

    async def record_feedback(
        self,
        query: str,
        response: str,
        context: List[str],
        feedback_type: FeedbackType,
        feedback_value: Optional[Any] = None,
        reflection_score: float = 0.0,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FeedbackEntry:
        """
        Record user feedback for a response.

        Args:
            query: The user's query
            response: The generated response
            context: Retrieved context chunks
            feedback_type: Type of feedback
            feedback_value: Optional value (rating, correction text)
            reflection_score: Score from reflection agent
            user_id: Optional user identifier
            metadata: Additional metadata

        Returns:
            The created FeedbackEntry
        """
        entry_id = hashlib.md5(
            f"{query}{response}{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]

        entry = FeedbackEntry(
            id=entry_id,
            query=query,
            response=response,
            context=context,
            feedback_type=feedback_type,
            feedback_value=feedback_value,
            reflection_score=reflection_score,
            user_id=user_id,
            timestamp=datetime.now(),
            metadata=metadata or {},
        )

        self.store.add(entry)
        logger.info(f"Recorded feedback: {feedback_type.value} for query '{query[:50]}...'")

        # Check if we can generate preference pairs
        await self._try_generate_pairs(query)

        return entry

    async def _try_generate_pairs(self, query: str) -> None:
        """Try to generate preference pairs from feedback"""
        entries = self.store.get_for_query(query)
        if len(entries) < 2:
            return

        # Find positive and negative examples
        positive = [e for e in entries if self._is_positive(e)]
        negative = [e for e in entries if self._is_negative(e)]

        if positive and negative:
            # Generate pairs
            for pos in positive:
                for neg in negative:
                    if pos.response != neg.response:
                        pair = PreferencePair(
                            prompt=query,
                            chosen=pos.response,
                            rejected=neg.response,
                            chosen_score=pos.reflection_score,
                            rejected_score=neg.reflection_score,
                            margin=pos.reflection_score - neg.reflection_score,
                        )
                        self._preference_cache.append(pair)
                        logger.info(f"Generated preference pair (margin: {pair.margin:.2f})")

    def _is_positive(self, entry: FeedbackEntry) -> bool:
        """Determine if feedback is positive"""
        if entry.feedback_type == FeedbackType.THUMBS_UP:
            return True
        if entry.feedback_type == FeedbackType.COPY:
            return True
        if entry.feedback_type == FeedbackType.RATING:
            return entry.feedback_value and entry.feedback_value >= 4
        if entry.reflection_score >= self.reflection_threshold:
            return True
        return False

    def _is_negative(self, entry: FeedbackEntry) -> bool:
        """Determine if feedback is negative"""
        if entry.feedback_type == FeedbackType.THUMBS_DOWN:
            return True
        if entry.feedback_type == FeedbackType.REGENERATE:
            return True
        if entry.feedback_type == FeedbackType.RATING:
            return entry.feedback_value and entry.feedback_value <= 2
        if entry.reflection_score < 0.5:
            return True
        return False

    def get_preference_pairs(self, min_margin: float = 0.1) -> List[PreferencePair]:
        """
        Get preference pairs for reward model training.

        Args:
            min_margin: Minimum score difference between chosen/rejected

        Returns:
            List of PreferencePair for training
        """
        return [p for p in self._preference_cache if p.margin >= min_margin]

    def export_for_trl(self, output_path: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Export preference pairs in TRL DPO format.

        Args:
            output_path: Optional path to save JSON

        Returns:
            List of training examples
        """
        pairs = self.get_preference_pairs()
        data = [p.to_trl_format() for p in pairs]

        if output_path:
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Exported {len(data)} preference pairs to {output_path}")

        return data

    def get_quality_metrics(self, hours: int = 24) -> QualityMetrics:
        """
        Get aggregated quality metrics.

        Args:
            hours: Lookback period in hours

        Returns:
            QualityMetrics
        """
        entries = self.store.get_recent(hours)
        if not entries:
            return QualityMetrics(
                total_responses=0,
                positive_feedback_rate=0.0,
                average_reflection_score=0.0,
                regeneration_rate=0.0,
                copy_rate=0.0,
                quality_distribution={},
                trend="stable",
            )

        total = len(entries)
        positive = sum(1 for e in entries if self._is_positive(e))
        regenerations = sum(1 for e in entries if e.feedback_type == FeedbackType.REGENERATE)
        copies = sum(1 for e in entries if e.feedback_type == FeedbackType.COPY)
        avg_score = sum(e.reflection_score for e in entries) / total

        # Quality distribution
        distribution = defaultdict(int)
        for e in entries:
            if e.reflection_score >= 0.9:
                distribution["excellent"] += 1
            elif e.reflection_score >= 0.7:
                distribution["good"] += 1
            elif e.reflection_score >= 0.5:
                distribution["acceptable"] += 1
            else:
                distribution["poor"] += 1

        # Calculate trend
        trend = self._calculate_trend(entries)

        return QualityMetrics(
            total_responses=total,
            positive_feedback_rate=positive / total if total else 0,
            average_reflection_score=avg_score,
            regeneration_rate=regenerations / total if total else 0,
            copy_rate=copies / total if total else 0,
            quality_distribution=dict(distribution),
            trend=trend,
        )

    def _calculate_trend(self, entries: List[FeedbackEntry]) -> str:
        """Calculate trend from entries"""
        if len(entries) < 10:
            return "stable"

        # Split into first and second half
        sorted_entries = sorted(entries, key=lambda e: e.timestamp)
        mid = len(sorted_entries) // 2
        first_half = sorted_entries[:mid]
        second_half = sorted_entries[mid:]

        first_avg = sum(e.reflection_score for e in first_half) / len(first_half)
        second_avg = sum(e.reflection_score for e in second_half) / len(second_half)

        diff = second_avg - first_avg
        if diff > 0.05:
            return "improving"
        elif diff < -0.05:
            return "declining"
        return "stable"

    # =========================================================================
    # A/B TESTING
    # =========================================================================

    def start_ab_test(
        self,
        test_name: str,
        variant_a: str,
        variant_b: str,
    ) -> None:
        """
        Start an A/B test.

        Args:
            test_name: Unique test identifier
            variant_a: Description of variant A
            variant_b: Description of variant B
        """
        self.ab_tests[test_name] = {
            "variant_a": variant_a,
            "variant_b": variant_b,
            "a_results": [],
            "b_results": [],
            "start_time": datetime.now(),
        }
        logger.info(f"Started A/B test: {test_name}")

    def record_ab_result(
        self,
        test_name: str,
        variant: str,  # "a" or "b"
        success: bool,
        score: float = 0.0,
    ) -> None:
        """Record a result for an A/B test"""
        if test_name not in self.ab_tests:
            return

        result = {"success": success, "score": score}
        if variant.lower() == "a":
            self.ab_tests[test_name]["a_results"].append(result)
        else:
            self.ab_tests[test_name]["b_results"].append(result)

    def get_ab_variant(self, test_name: str) -> str:
        """Get which variant to use for a test (random selection)"""
        return random.choice(["a", "b"])

    def get_ab_results(self, test_name: str) -> Optional[ABTestResult]:
        """Get results of an A/B test"""
        if test_name not in self.ab_tests:
            return None

        test = self.ab_tests[test_name]
        a_results = test["a_results"]
        b_results = test["b_results"]

        if not a_results or not b_results:
            return None

        a_success = sum(1 for r in a_results if r["success"]) / len(a_results)
        b_success = sum(1 for r in b_results if r["success"]) / len(b_results)

        # Simple winner determination
        if a_success > b_success + 0.05:
            winner = "a"
            confidence = min(len(a_results), len(b_results)) / 100  # Simplified
        elif b_success > a_success + 0.05:
            winner = "b"
            confidence = min(len(a_results), len(b_results)) / 100
        else:
            winner = "tie"
            confidence = 0.0

        return ABTestResult(
            test_name=test_name,
            variant_a=test["variant_a"],
            variant_b=test["variant_b"],
            a_count=len(a_results),
            b_count=len(b_results),
            a_success_rate=a_success,
            b_success_rate=b_success,
            winner=winner,
            confidence=min(confidence, 1.0),
            start_time=test["start_time"],
        )

    # =========================================================================
    # PROMPT OPTIMIZATION
    # =========================================================================

    def get_prompt_suggestions(self) -> List[Dict[str, Any]]:
        """
        Analyze feedback to suggest prompt improvements.

        Returns:
            List of suggestions with context
        """
        suggestions = []
        entries = self.store.get_recent(hours=168)  # Last week

        # Find common issues
        poor_responses = [e for e in entries if e.reflection_score < 0.5]
        corrections = [e for e in entries if e.feedback_type == FeedbackType.CORRECTION]

        if len(poor_responses) > len(entries) * 0.2:
            suggestions.append({
                "type": "quality_issue",
                "message": f"{len(poor_responses)} responses had low quality scores",
                "recommendation": "Consider adding more specific instructions to the system prompt",
            })

        if corrections:
            suggestions.append({
                "type": "factual_errors",
                "message": f"{len(corrections)} responses were corrected by users",
                "recommendation": "Review retrieval quality and add fact-checking instructions",
                "examples": [c.feedback_value for c in corrections[:3]],
            })

        # Analyze by query patterns
        regenerate_queries = [e.query for e in entries if e.feedback_type == FeedbackType.REGENERATE]
        if regenerate_queries:
            suggestions.append({
                "type": "regeneration_patterns",
                "message": f"{len(regenerate_queries)} queries led to regeneration",
                "sample_queries": regenerate_queries[:5],
            })

        return suggestions

    def is_ready_for_training(self) -> bool:
        """Check if enough data is available for fine-tuning"""
        pairs = self.get_preference_pairs()
        return len(pairs) >= self.min_pairs_for_training


# =============================================================================
# SINGLETON & CONVENIENCE
# =============================================================================

_feedback_manager: Optional[FeedbackLoopManager] = None


def get_feedback_manager() -> FeedbackLoopManager:
    """Get or create singleton feedback manager"""
    global _feedback_manager
    if _feedback_manager is None:
        _feedback_manager = FeedbackLoopManager()
    return _feedback_manager


async def record_user_feedback(
    query: str,
    response: str,
    context: List[str],
    feedback_type: str,
    feedback_value: Optional[Any] = None,
    reflection_score: float = 0.0,
    user_id: Optional[str] = None,
) -> FeedbackEntry:
    """Convenience function to record feedback"""
    manager = get_feedback_manager()
    return await manager.record_feedback(
        query=query,
        response=response,
        context=context,
        feedback_type=FeedbackType(feedback_type),
        feedback_value=feedback_value,
        reflection_score=reflection_score,
        user_id=user_id,
    )


# =============================================================================
# QUALITY CALIBRATOR - Cutting Edge User Feedback Calibration
# =============================================================================

@dataclass
class CalibrationPoint:
    """A single calibration data point mapping automatic score to user feedback"""
    automatic_score: float  # Raw automatic quality score (0-100)
    user_satisfied: bool  # Whether user gave positive feedback
    feedback_type: FeedbackType
    feedback_value: Optional[Any]
    timestamp: datetime
    query_type: str = "general"  # Query classification for per-type calibration

    def to_dict(self) -> Dict[str, Any]:
        return {
            "automatic_score": self.automatic_score,
            "user_satisfied": self.user_satisfied,
            "feedback_type": self.feedback_type.value,
            "feedback_value": self.feedback_value,
            "timestamp": self.timestamp.isoformat(),
            "query_type": self.query_type,
        }


@dataclass
class UserCalibrationProfile:
    """Per-user calibration profile learned from feedback history"""
    user_id: str

    # Learned thresholds - automatic score that maps to user satisfaction
    satisfaction_threshold: float = 70.0  # Default: 70+ = satisfied
    high_quality_threshold: float = 85.0  # Default: 85+ = high quality

    # Calibration curve parameters (linear transformation: calibrated = a * raw + b)
    calibration_slope: float = 1.0  # How much user cares about score differences
    calibration_intercept: float = 0.0  # User's baseline offset

    # Per-dimension weights (what user values most)
    relevance_weight: float = 0.25
    accuracy_weight: float = 0.25
    completeness_weight: float = 0.20
    citation_weight: float = 0.15
    language_weight: float = 0.15

    # Confidence metrics
    calibration_points: int = 0
    confidence: float = 0.0  # 0-1, increases with more data
    last_updated: Optional[datetime] = None

    # Per-query-type thresholds (user may have different standards for different query types)
    query_type_thresholds: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "satisfaction_threshold": self.satisfaction_threshold,
            "high_quality_threshold": self.high_quality_threshold,
            "calibration_slope": self.calibration_slope,
            "calibration_intercept": self.calibration_intercept,
            "dimension_weights": {
                "relevance": self.relevance_weight,
                "accuracy": self.accuracy_weight,
                "completeness": self.completeness_weight,
                "citation": self.citation_weight,
                "language": self.language_weight,
            },
            "calibration_points": self.calibration_points,
            "confidence": self.confidence,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "query_type_thresholds": self.query_type_thresholds,
        }


@dataclass
class CalibratedScore:
    """A quality score calibrated for a specific user"""
    raw_score: float  # Original automatic score
    calibrated_score: float  # User-calibrated score
    predicted_satisfaction: float  # Probability user will be satisfied (0-1)
    confidence: float  # Confidence in this prediction (0-1)
    calibration_applied: str  # Type of calibration: "personalized", "global", "default"
    dimension_scores: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "raw_score": round(self.raw_score, 2),
            "calibrated_score": round(self.calibrated_score, 2),
            "predicted_satisfaction": round(self.predicted_satisfaction, 3),
            "confidence": round(self.confidence, 3),
            "calibration_applied": self.calibration_applied,
            "dimension_scores": {k: round(v, 2) for k, v in self.dimension_scores.items()},
        }


class QualityCalibrator:
    """
    User Feedback Calibration System (Cutting Edge)

    Learns individual user preferences and calibrates automatic quality scores
    to predict user satisfaction. Supports:

    - Per-user calibration profiles
    - Per-query-type thresholds
    - Dimension weight learning (what aspects users care about)
    - A/B testing of calibration methods
    - Global fallback when user data insufficient

    Based on concepts from:
    - Preference learning and reward modeling
    - Bayesian calibration methods
    - Online learning with cold-start handling
    """

    def __init__(
        self,
        min_points_for_calibration: int = 5,
        learning_rate: float = 0.1,
        decay_factor: float = 0.95,  # Weight decay for older feedback
        persistence_path: Optional[str] = None,
    ):
        self.min_points = min_points_for_calibration
        self.learning_rate = learning_rate
        self.decay_factor = decay_factor
        self.persistence_path = persistence_path

        # User profiles
        self.user_profiles: Dict[str, UserCalibrationProfile] = {}

        # Calibration data per user
        self.calibration_data: Dict[str, List[CalibrationPoint]] = defaultdict(list)

        # Global calibration (fallback for new users)
        self.global_profile = UserCalibrationProfile(user_id="__global__")
        self.global_calibration_data: List[CalibrationPoint] = []

        # A/B testing for calibration methods
        self.calibration_ab_tests: Dict[str, Dict[str, Any]] = {}

        # Load persisted data
        if persistence_path:
            self._load_from_disk()

    def record_calibration_feedback(
        self,
        user_id: str,
        automatic_score: float,
        feedback_type: FeedbackType,
        feedback_value: Optional[Any] = None,
        dimension_scores: Optional[Dict[str, float]] = None,
        query_type: str = "general",
    ) -> None:
        """
        Record a calibration data point from user feedback.

        Args:
            user_id: The user who provided feedback
            automatic_score: Raw automatic quality score (0-100)
            feedback_type: Type of user feedback
            feedback_value: Optional value (e.g., rating 1-5)
            dimension_scores: Optional breakdown by dimension
            query_type: Classification of the query
        """
        # Determine if user was satisfied
        user_satisfied = self._is_user_satisfied(feedback_type, feedback_value)

        point = CalibrationPoint(
            automatic_score=automatic_score,
            user_satisfied=user_satisfied,
            feedback_type=feedback_type,
            feedback_value=feedback_value,
            timestamp=datetime.now(),
            query_type=query_type,
        )

        # Add to user's calibration data
        self.calibration_data[user_id].append(point)

        # Add to global data
        self.global_calibration_data.append(point)

        # Update calibration if enough data
        if len(self.calibration_data[user_id]) >= self.min_points:
            self._update_user_calibration(user_id)

        # Update global calibration periodically
        if len(self.global_calibration_data) % 10 == 0:
            self._update_global_calibration()

        # Persist if configured
        if self.persistence_path:
            self._save_to_disk()

        logger.info(
            f"Recorded calibration point for user {user_id}: "
            f"score={automatic_score:.1f}, satisfied={user_satisfied}"
        )

    def get_calibrated_score(
        self,
        user_id: str,
        raw_score: float,
        dimension_scores: Optional[Dict[str, float]] = None,
        query_type: str = "general",
    ) -> CalibratedScore:
        """
        Get a user-calibrated quality score.

        Args:
            user_id: The user to calibrate for
            raw_score: Raw automatic quality score (0-100)
            dimension_scores: Optional breakdown by dimension
            query_type: Classification of the query

        Returns:
            CalibratedScore with calibrated score and satisfaction prediction
        """
        # Get user profile or use global
        profile = self.user_profiles.get(user_id)
        calibration_type = "default"

        if profile and profile.confidence >= 0.3:
            calibration_type = "personalized"
        elif self.global_profile.confidence >= 0.2:
            profile = self.global_profile
            calibration_type = "global"
        else:
            # Use default calibration
            profile = UserCalibrationProfile(user_id=user_id)

        # Apply linear calibration transformation
        calibrated = profile.calibration_slope * raw_score + profile.calibration_intercept
        calibrated = max(0, min(100, calibrated))  # Clamp to 0-100

        # Apply per-query-type adjustment if available
        if query_type in profile.query_type_thresholds:
            type_threshold = profile.query_type_thresholds[query_type]
            # Adjust based on type-specific threshold
            threshold_diff = type_threshold - profile.satisfaction_threshold
            calibrated += threshold_diff * 0.5
            calibrated = max(0, min(100, calibrated))

        # Calculate weighted score if dimensions provided
        if dimension_scores:
            weighted_score = (
                dimension_scores.get("relevance", 0) * profile.relevance_weight +
                dimension_scores.get("accuracy", 0) * profile.accuracy_weight +
                dimension_scores.get("completeness", 0) * profile.completeness_weight +
                dimension_scores.get("citation", 0) * profile.citation_weight +
                dimension_scores.get("language", 0) * profile.language_weight
            )
            # Blend raw and weighted
            calibrated = 0.7 * calibrated + 0.3 * weighted_score

        # Predict satisfaction probability using sigmoid
        threshold = profile.query_type_thresholds.get(query_type, profile.satisfaction_threshold)
        satisfaction_prob = self._sigmoid(calibrated - threshold, steepness=0.1)

        return CalibratedScore(
            raw_score=raw_score,
            calibrated_score=calibrated,
            predicted_satisfaction=satisfaction_prob,
            confidence=profile.confidence,
            calibration_applied=calibration_type,
            dimension_scores=dimension_scores or {},
        )

    def get_user_profile(self, user_id: str) -> Optional[UserCalibrationProfile]:
        """Get calibration profile for a user"""
        return self.user_profiles.get(user_id)

    def get_calibration_stats(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get calibration statistics.

        Args:
            user_id: Optional user ID, or None for global stats

        Returns:
            Statistics dictionary
        """
        if user_id:
            profile = self.user_profiles.get(user_id)
            data = self.calibration_data.get(user_id, [])
        else:
            profile = self.global_profile
            data = self.global_calibration_data

        if not data:
            return {"calibration_points": 0, "status": "insufficient_data"}

        # Calculate satisfaction rate by score band
        bands = {"0-40": [], "40-60": [], "60-80": [], "80-100": []}
        for point in data:
            score = point.automatic_score
            if score < 40:
                bands["0-40"].append(point.user_satisfied)
            elif score < 60:
                bands["40-60"].append(point.user_satisfied)
            elif score < 80:
                bands["60-80"].append(point.user_satisfied)
            else:
                bands["80-100"].append(point.user_satisfied)

        satisfaction_by_band = {
            band: sum(satisfactions) / len(satisfactions) if satisfactions else None
            for band, satisfactions in bands.items()
        }

        return {
            "calibration_points": len(data),
            "confidence": profile.confidence if profile else 0.0,
            "satisfaction_threshold": profile.satisfaction_threshold if profile else 70.0,
            "satisfaction_by_score_band": satisfaction_by_band,
            "recent_satisfaction_rate": (
                sum(1 for p in data[-20:] if p.user_satisfied) / min(len(data), 20)
                if data else 0.0
            ),
        }

    def start_calibration_ab_test(
        self,
        test_name: str,
        variant_a_params: Dict[str, float],
        variant_b_params: Dict[str, float],
    ) -> None:
        """
        Start an A/B test for calibration parameters.

        Args:
            test_name: Unique test identifier
            variant_a_params: Parameters for variant A
            variant_b_params: Parameters for variant B
        """
        self.calibration_ab_tests[test_name] = {
            "variant_a": variant_a_params,
            "variant_b": variant_b_params,
            "a_results": [],
            "b_results": [],
            "start_time": datetime.now(),
        }
        logger.info(f"Started calibration A/B test: {test_name}")

    def get_ab_variant_params(self, test_name: str) -> Tuple[str, Dict[str, float]]:
        """Get which variant to use for a test"""
        if test_name not in self.calibration_ab_tests:
            return "default", {}

        variant = random.choice(["a", "b"])
        test = self.calibration_ab_tests[test_name]
        params = test["variant_a"] if variant == "a" else test["variant_b"]
        return variant, params

    def record_ab_test_result(
        self,
        test_name: str,
        variant: str,
        predicted_satisfaction: float,
        actual_satisfied: bool,
    ) -> None:
        """Record a result for calibration A/B test"""
        if test_name not in self.calibration_ab_tests:
            return

        result = {
            "predicted": predicted_satisfaction,
            "actual": actual_satisfied,
            "error": abs(predicted_satisfaction - (1.0 if actual_satisfied else 0.0)),
        }

        test = self.calibration_ab_tests[test_name]
        if variant == "a":
            test["a_results"].append(result)
        else:
            test["b_results"].append(result)

    def get_ab_test_winner(self, test_name: str) -> Optional[Dict[str, Any]]:
        """Get winner of calibration A/B test based on prediction accuracy"""
        if test_name not in self.calibration_ab_tests:
            return None

        test = self.calibration_ab_tests[test_name]
        a_results = test["a_results"]
        b_results = test["b_results"]

        if len(a_results) < 10 or len(b_results) < 10:
            return {"status": "insufficient_data", "a_count": len(a_results), "b_count": len(b_results)}

        a_mae = sum(r["error"] for r in a_results) / len(a_results)
        b_mae = sum(r["error"] for r in b_results) / len(b_results)

        winner = "a" if a_mae < b_mae else "b"
        improvement = abs(a_mae - b_mae) / max(a_mae, b_mae)

        return {
            "winner": winner,
            "a_mae": round(a_mae, 4),
            "b_mae": round(b_mae, 4),
            "improvement": round(improvement, 4),
            "a_params": test["variant_a"],
            "b_params": test["variant_b"],
            "confidence": min(len(a_results), len(b_results)) / 50,  # Simplified confidence
        }

    # =========================================================================
    # PRIVATE METHODS
    # =========================================================================

    def _is_user_satisfied(self, feedback_type: FeedbackType, feedback_value: Optional[Any]) -> bool:
        """Determine if user was satisfied based on feedback"""
        if feedback_type == FeedbackType.THUMBS_UP:
            return True
        if feedback_type == FeedbackType.THUMBS_DOWN:
            return False
        if feedback_type == FeedbackType.COPY:
            return True
        if feedback_type == FeedbackType.REGENERATE:
            return False
        if feedback_type == FeedbackType.RATING:
            return feedback_value is not None and feedback_value >= 4
        if feedback_type == FeedbackType.CORRECTION:
            return False  # User corrected = not satisfied with original
        return False  # Default: ignore = not satisfied

    def _update_user_calibration(self, user_id: str) -> None:
        """Update calibration profile for a user based on their feedback history"""
        data = self.calibration_data[user_id]
        if len(data) < self.min_points:
            return

        # Get or create profile
        if user_id not in self.user_profiles:
            self.user_profiles[user_id] = UserCalibrationProfile(user_id=user_id)
        profile = self.user_profiles[user_id]

        # Apply decay to older data points
        now = datetime.now()
        weighted_data = []
        for point in data:
            age_hours = (now - point.timestamp).total_seconds() / 3600
            weight = self.decay_factor ** (age_hours / 24)  # Decay per day
            weighted_data.append((point, weight))

        # Find satisfaction threshold using weighted binary search
        # This is the score where user transitions from unsatisfied to satisfied
        satisfied_scores = [p.automatic_score for p, w in weighted_data if p.user_satisfied]
        unsatisfied_scores = [p.automatic_score for p, w in weighted_data if not p.user_satisfied]

        if satisfied_scores and unsatisfied_scores:
            # Threshold is between max unsatisfied and min satisfied
            max_unsatisfied = max(unsatisfied_scores)
            min_satisfied = min(satisfied_scores)

            # Use weighted average of these boundary scores
            old_threshold = profile.satisfaction_threshold
            new_threshold = (max_unsatisfied + min_satisfied) / 2

            # Smooth update with learning rate
            profile.satisfaction_threshold = (
                (1 - self.learning_rate) * old_threshold +
                self.learning_rate * new_threshold
            )

        # Estimate calibration slope and intercept using simple linear regression
        # Map scores to satisfaction (0 or 1) and find best linear fit
        X = [p.automatic_score for p, _ in weighted_data]
        Y = [1.0 if p.user_satisfied else 0.0 for p, _ in weighted_data]
        weights = [w for _, w in weighted_data]

        if len(set(X)) > 1:  # Need variance in X
            # Weighted linear regression
            n = len(X)
            sum_w = sum(weights)
            sum_wx = sum(w * x for w, x in zip(weights, X))
            sum_wy = sum(w * y for w, y in zip(weights, Y))
            sum_wxy = sum(w * x * y for w, x, y in zip(weights, X, Y))
            sum_wx2 = sum(w * x * x for w, x in zip(weights, X))

            denom = sum_w * sum_wx2 - sum_wx ** 2
            if abs(denom) > 1e-10:
                slope = (sum_w * sum_wxy - sum_wx * sum_wy) / denom
                intercept = (sum_wy - slope * sum_wx) / sum_w

                # Scale to score space (slope should map satisfaction to score change)
                profile.calibration_slope = 1.0 + slope * 0.5  # Moderate adjustment
                profile.calibration_intercept = intercept * 10  # Scale intercept

        # Update per-query-type thresholds
        type_data = defaultdict(list)
        for point, weight in weighted_data:
            type_data[point.query_type].append((point, weight))

        for query_type, type_points in type_data.items():
            if len(type_points) >= 3:
                type_satisfied = [p.automatic_score for p, _ in type_points if p.user_satisfied]
                type_unsatisfied = [p.automatic_score for p, _ in type_points if not p.user_satisfied]

                if type_satisfied and type_unsatisfied:
                    profile.query_type_thresholds[query_type] = (
                        max(type_unsatisfied) + min(type_satisfied)
                    ) / 2

        # Update confidence based on data quantity and recency
        recent_count = sum(1 for p in data if (now - p.timestamp).total_seconds() < 7 * 24 * 3600)
        profile.calibration_points = len(data)
        profile.confidence = min(1.0, len(data) / 50 * 0.5 + recent_count / 20 * 0.5)
        profile.last_updated = now

        logger.info(
            f"Updated calibration for user {user_id}: "
            f"threshold={profile.satisfaction_threshold:.1f}, "
            f"confidence={profile.confidence:.2f}"
        )

    def _update_global_calibration(self) -> None:
        """Update global calibration from all user data"""
        if len(self.global_calibration_data) < 10:
            return

        # Aggregate all data
        all_satisfied = [p for p in self.global_calibration_data if p.user_satisfied]
        all_unsatisfied = [p for p in self.global_calibration_data if not p.user_satisfied]

        if all_satisfied and all_unsatisfied:
            satisfied_scores = [p.automatic_score for p in all_satisfied]
            unsatisfied_scores = [p.automatic_score for p in all_unsatisfied]

            old_threshold = self.global_profile.satisfaction_threshold
            new_threshold = (max(unsatisfied_scores) + min(satisfied_scores)) / 2

            self.global_profile.satisfaction_threshold = (
                0.9 * old_threshold + 0.1 * new_threshold
            )

        self.global_profile.calibration_points = len(self.global_calibration_data)
        self.global_profile.confidence = min(1.0, len(self.global_calibration_data) / 100)
        self.global_profile.last_updated = datetime.now()

    def _sigmoid(self, x: float, steepness: float = 0.1) -> float:
        """Sigmoid function for satisfaction probability"""
        import math
        try:
            return 1 / (1 + math.exp(-steepness * x))
        except OverflowError:
            return 0.0 if x < 0 else 1.0

    def _save_to_disk(self) -> None:
        """Persist calibration data to disk"""
        if not self.persistence_path:
            return

        try:
            data = {
                "user_profiles": {
                    uid: profile.to_dict()
                    for uid, profile in self.user_profiles.items()
                },
                "global_profile": self.global_profile.to_dict(),
                "calibration_data": {
                    uid: [p.to_dict() for p in points[-100:]]  # Keep last 100 per user
                    for uid, points in self.calibration_data.items()
                },
                "global_calibration_data": [
                    p.to_dict() for p in self.global_calibration_data[-500:]
                ],
            }

            with open(self.persistence_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save calibration data: {e}")

    def _load_from_disk(self) -> None:
        """Load calibration data from disk"""
        if not self.persistence_path:
            return

        try:
            import os
            if not os.path.exists(self.persistence_path):
                return

            with open(self.persistence_path, 'r') as f:
                data = json.load(f)

            # Restore user profiles
            for uid, profile_data in data.get("user_profiles", {}).items():
                profile = UserCalibrationProfile(
                    user_id=uid,
                    satisfaction_threshold=profile_data.get("satisfaction_threshold", 70.0),
                    high_quality_threshold=profile_data.get("high_quality_threshold", 85.0),
                    calibration_slope=profile_data.get("calibration_slope", 1.0),
                    calibration_intercept=profile_data.get("calibration_intercept", 0.0),
                    calibration_points=profile_data.get("calibration_points", 0),
                    confidence=profile_data.get("confidence", 0.0),
                )
                # Restore dimension weights
                if "dimension_weights" in profile_data:
                    dw = profile_data["dimension_weights"]
                    profile.relevance_weight = dw.get("relevance", 0.25)
                    profile.accuracy_weight = dw.get("accuracy", 0.25)
                    profile.completeness_weight = dw.get("completeness", 0.20)
                    profile.citation_weight = dw.get("citation", 0.15)
                    profile.language_weight = dw.get("language", 0.15)

                profile.query_type_thresholds = profile_data.get("query_type_thresholds", {})
                self.user_profiles[uid] = profile

            # Restore global profile
            global_data = data.get("global_profile", {})
            self.global_profile.satisfaction_threshold = global_data.get("satisfaction_threshold", 70.0)
            self.global_profile.calibration_points = global_data.get("calibration_points", 0)
            self.global_profile.confidence = global_data.get("confidence", 0.0)

            logger.info(f"Loaded calibration data: {len(self.user_profiles)} user profiles")
        except Exception as e:
            logger.error(f"Failed to load calibration data: {e}")


# =============================================================================
# QUALITY CALIBRATOR SINGLETON
# =============================================================================

_quality_calibrator: Optional[QualityCalibrator] = None


def get_quality_calibrator() -> QualityCalibrator:
    """Get or create singleton quality calibrator"""
    global _quality_calibrator
    if _quality_calibrator is None:
        _quality_calibrator = QualityCalibrator(
            persistence_path="data/quality_calibration.json"
        )
    return _quality_calibrator


async def record_quality_feedback(
    user_id: str,
    automatic_score: float,
    feedback_type: str,
    feedback_value: Optional[Any] = None,
    dimension_scores: Optional[Dict[str, float]] = None,
    query_type: str = "general",
) -> None:
    """
    Convenience function to record quality feedback for calibration.

    Args:
        user_id: The user who provided feedback
        automatic_score: Raw automatic quality score (0-100)
        feedback_type: Type of feedback (thumbs_up, thumbs_down, rating, etc.)
        feedback_value: Optional value (e.g., rating 1-5)
        dimension_scores: Optional breakdown by dimension
        query_type: Classification of the query
    """
    calibrator = get_quality_calibrator()
    calibrator.record_calibration_feedback(
        user_id=user_id,
        automatic_score=automatic_score,
        feedback_type=FeedbackType(feedback_type),
        feedback_value=feedback_value,
        dimension_scores=dimension_scores,
        query_type=query_type,
    )


def get_calibrated_quality_score(
    user_id: str,
    raw_score: float,
    dimension_scores: Optional[Dict[str, float]] = None,
    query_type: str = "general",
) -> CalibratedScore:
    """
    Convenience function to get a calibrated quality score.

    Args:
        user_id: The user to calibrate for
        raw_score: Raw automatic quality score (0-100)
        dimension_scores: Optional breakdown by dimension
        query_type: Classification of the query

    Returns:
        CalibratedScore with calibrated score and satisfaction prediction
    """
    calibrator = get_quality_calibrator()
    return calibrator.get_calibrated_score(
        user_id=user_id,
        raw_score=raw_score,
        dimension_scores=dimension_scores,
        query_type=query_type,
    )


# =============================================================================
# CUTTING EDGE: META-META LEARNING & SELF-REFLECTION
# =============================================================================

class StrategyType(str, Enum):
    """Types of learning strategies."""
    RETRIEVAL = "retrieval"  # How to retrieve context
    GENERATION = "generation"  # How to generate responses
    REFLECTION = "reflection"  # How to self-assess
    FEEDBACK = "feedback"  # How to learn from feedback
    CALIBRATION = "calibration"  # How to calibrate scores
    TRANSFER = "transfer"  # How to transfer knowledge


class MutationType(str, Enum):
    """Types of strategy mutations."""
    PARAMETER_TWEAK = "parameter_tweak"
    WEIGHT_ADJUSTMENT = "weight_adjustment"
    THRESHOLD_SHIFT = "threshold_shift"
    COMPONENT_SWAP = "component_swap"
    HYBRID_MERGE = "hybrid_merge"


@dataclass
class LearningStrategy:
    """A strategy for a specific learning aspect."""
    strategy_id: str
    strategy_type: StrategyType
    name: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    weights: Dict[str, float] = field(default_factory=dict)
    thresholds: Dict[str, float] = field(default_factory=dict)
    parent_ids: List[str] = field(default_factory=list)
    generation: int = 0
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_type": self.strategy_type.value,
            "name": self.name,
            "parameters": self.parameters,
            "weights": self.weights,
            "thresholds": self.thresholds,
            "generation": self.generation,
            "created_at": self.created_at.isoformat(),
        }

    def clone(self, new_id: str) -> "LearningStrategy":
        """Create a clone of this strategy."""
        return LearningStrategy(
            strategy_id=new_id,
            strategy_type=self.strategy_type,
            name=f"{self.name}_clone",
            parameters=self.parameters.copy(),
            weights=self.weights.copy(),
            thresholds=self.thresholds.copy(),
            parent_ids=[self.strategy_id],
            generation=self.generation + 1,
        )


@dataclass
class StrategyPerformance:
    """Performance metrics for a learning strategy."""
    strategy_id: str
    total_uses: int = 0
    successful_uses: int = 0
    cumulative_score: float = 0.0
    avg_score: float = 0.0
    trend: str = "stable"  # improving, stable, declining
    last_updated: datetime = field(default_factory=datetime.now)
    score_history: List[float] = field(default_factory=list)

    def record_outcome(self, score: float, success: bool):
        """Record an outcome for this strategy."""
        self.total_uses += 1
        if success:
            self.successful_uses += 1
        self.cumulative_score += score
        self.avg_score = self.cumulative_score / self.total_uses
        self.score_history.append(score)
        if len(self.score_history) > 100:
            self.score_history = self.score_history[-100:]
        self.last_updated = datetime.now()
        self._update_trend()

    def _update_trend(self):
        """Update trend based on score history."""
        if len(self.score_history) < 10:
            self.trend = "stable"
            return
        first_half = self.score_history[:len(self.score_history)//2]
        second_half = self.score_history[len(self.score_history)//2:]
        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)
        diff = second_avg - first_avg
        if diff > 0.05:
            self.trend = "improving"
        elif diff < -0.05:
            self.trend = "declining"
        else:
            self.trend = "stable"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "total_uses": self.total_uses,
            "successful_uses": self.successful_uses,
            "success_rate": self.successful_uses / max(1, self.total_uses),
            "avg_score": round(self.avg_score, 3),
            "trend": self.trend,
            "last_updated": self.last_updated.isoformat(),
        }


@dataclass
class MetaLearningOutcome:
    """Outcome of a meta-learning decision."""
    decision_id: str
    strategy_id: str
    context: Dict[str, Any]
    outcome_score: float
    was_optimal: bool  # Whether this was the best choice in hindsight
    alternatives_considered: List[str]
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ReflectionImprovement:
    """A proposed improvement to the reflection process."""
    improvement_id: str
    improvement_type: str
    description: str
    parameters: Dict[str, Any]
    predicted_benefit: float
    actual_benefit: Optional[float] = None
    applied: bool = False
    timestamp: datetime = field(default_factory=datetime.now)


class MetaLearningEngine:
    """
    Meta-learning engine that tracks which strategies work.

    Monitors strategy performance across different contexts and
    automatically selects the best strategy for each situation.
    """

    def __init__(self):
        self._strategies: Dict[str, LearningStrategy] = {}
        self._performance: Dict[str, StrategyPerformance] = {}
        self._context_strategy_map: Dict[str, str] = {}  # context_hash -> best_strategy_id
        self._outcomes: List[MetaLearningOutcome] = []

        # Initialize default strategies
        self._initialize_default_strategies()

    def _initialize_default_strategies(self):
        """Initialize default learning strategies."""
        default_strategies = [
            LearningStrategy(
                strategy_id="retrieval_dense",
                strategy_type=StrategyType.RETRIEVAL,
                name="Dense Retrieval",
                parameters={"method": "dense", "top_k": 10},
                weights={"semantic": 0.8, "keyword": 0.2},
            ),
            LearningStrategy(
                strategy_id="retrieval_hybrid",
                strategy_type=StrategyType.RETRIEVAL,
                name="Hybrid Retrieval",
                parameters={"method": "hybrid", "top_k": 15},
                weights={"semantic": 0.5, "keyword": 0.5},
            ),
            LearningStrategy(
                strategy_id="generation_standard",
                strategy_type=StrategyType.GENERATION,
                name="Standard Generation",
                parameters={"temperature": 0.7, "max_tokens": 1000},
            ),
            LearningStrategy(
                strategy_id="generation_precise",
                strategy_type=StrategyType.GENERATION,
                name="Precise Generation",
                parameters={"temperature": 0.3, "max_tokens": 500},
            ),
            LearningStrategy(
                strategy_id="reflection_standard",
                strategy_type=StrategyType.REFLECTION,
                name="Standard Reflection",
                parameters={"depth": "standard", "criteria": ["relevance", "accuracy"]},
            ),
            LearningStrategy(
                strategy_id="reflection_deep",
                strategy_type=StrategyType.REFLECTION,
                name="Deep Reflection",
                parameters={"depth": "deep", "criteria": ["relevance", "accuracy", "completeness", "coherence"]},
            ),
        ]

        for strategy in default_strategies:
            self._strategies[strategy.strategy_id] = strategy
            self._performance[strategy.strategy_id] = StrategyPerformance(
                strategy_id=strategy.strategy_id
            )

    def select_strategy(
        self,
        strategy_type: StrategyType,
        context: Dict[str, Any],
        exploration_rate: float = 0.1
    ) -> LearningStrategy:
        """
        Select the best strategy for the given context.

        Uses epsilon-greedy exploration to balance exploitation
        with discovering better strategies.
        """
        # Get strategies of this type
        candidates = [
            s for s in self._strategies.values()
            if s.strategy_type == strategy_type
        ]

        if not candidates:
            raise ValueError(f"No strategies available for type {strategy_type}")

        # Epsilon-greedy exploration
        if random.random() < exploration_rate:
            return random.choice(candidates)

        # Check for context-specific preference
        context_hash = self._hash_context(context)
        if context_hash in self._context_strategy_map:
            preferred_id = self._context_strategy_map[context_hash]
            if preferred_id in self._strategies:
                return self._strategies[preferred_id]

        # Select best performing strategy
        best_strategy = max(
            candidates,
            key=lambda s: self._performance.get(s.strategy_id, StrategyPerformance(s.strategy_id)).avg_score
        )

        return best_strategy

    def record_strategy_outcome(
        self,
        strategy_id: str,
        context: Dict[str, Any],
        score: float,
        success: bool,
        alternatives: Optional[List[str]] = None
    ):
        """Record the outcome of using a strategy."""
        if strategy_id not in self._performance:
            self._performance[strategy_id] = StrategyPerformance(strategy_id=strategy_id)

        self._performance[strategy_id].record_outcome(score, success)

        # Update context mapping if this was successful
        if success:
            context_hash = self._hash_context(context)
            current_best = self._context_strategy_map.get(context_hash)
            if current_best:
                current_perf = self._performance.get(current_best)
                new_perf = self._performance[strategy_id]
                if new_perf.avg_score > (current_perf.avg_score if current_perf else 0):
                    self._context_strategy_map[context_hash] = strategy_id
            else:
                self._context_strategy_map[context_hash] = strategy_id

        # Record outcome for meta-meta learning
        outcome = MetaLearningOutcome(
            decision_id=hashlib.md5(f"{strategy_id}{datetime.now().isoformat()}".encode()).hexdigest()[:12],
            strategy_id=strategy_id,
            context=context,
            outcome_score=score,
            was_optimal=success and score >= 0.8,
            alternatives_considered=alternatives or [],
        )
        self._outcomes.append(outcome)
        if len(self._outcomes) > 1000:
            self._outcomes = self._outcomes[-500:]

    def _hash_context(self, context: Dict[str, Any]) -> str:
        """Create a hash for context matching."""
        # Create a simplified context representation
        key_features = []
        if "query_type" in context:
            key_features.append(f"qt:{context['query_type']}")
        if "complexity" in context:
            key_features.append(f"cx:{context['complexity']}")
        if "domain" in context:
            key_features.append(f"dm:{context['domain']}")
        return hashlib.md5("|".join(sorted(key_features)).encode()).hexdigest()[:8]

    def get_strategy_rankings(self, strategy_type: Optional[StrategyType] = None) -> List[Dict[str, Any]]:
        """Get strategy rankings by performance."""
        strategies = self._strategies.values()
        if strategy_type:
            strategies = [s for s in strategies if s.strategy_type == strategy_type]

        rankings = []
        for strategy in strategies:
            perf = self._performance.get(strategy.strategy_id, StrategyPerformance(strategy.strategy_id))
            rankings.append({
                "strategy_id": strategy.strategy_id,
                "name": strategy.name,
                "type": strategy.strategy_type.value,
                "avg_score": perf.avg_score,
                "success_rate": perf.successful_uses / max(1, perf.total_uses),
                "total_uses": perf.total_uses,
                "trend": perf.trend,
            })

        return sorted(rankings, key=lambda x: x["avg_score"], reverse=True)

    def get_meta_learning_stats(self) -> Dict[str, Any]:
        """Get statistics about meta-learning performance."""
        total_outcomes = len(self._outcomes)
        optimal_decisions = sum(1 for o in self._outcomes if o.was_optimal)

        return {
            "total_strategies": len(self._strategies),
            "total_decisions": total_outcomes,
            "optimal_decision_rate": optimal_decisions / max(1, total_outcomes),
            "context_mappings": len(self._context_strategy_map),
            "by_type": {
                t.value: len([s for s in self._strategies.values() if s.strategy_type == t])
                for t in StrategyType
            },
        }


class MetaMetaLearningEngine:
    """
    Meta-meta learning: Learning how to learn better.

    This engine optimizes the learning process itself through:
    - Strategy evolution (genetic algorithm-style)
    - Reflection process optimization
    - Learning rate adaptation
    - Recursive self-improvement
    """

    def __init__(self, meta_engine: MetaLearningEngine):
        self.meta_engine = meta_engine
        self._improvements: List[ReflectionImprovement] = []
        self._evolution_history: List[Dict[str, Any]] = []
        self._learning_rate_history: List[Tuple[datetime, float]] = []
        self._current_learning_rate: float = 0.1

        # Meta-meta parameters (parameters about how we learn about learning)
        self._meta_meta_params = {
            "mutation_rate": 0.2,
            "crossover_rate": 0.3,
            "elite_fraction": 0.2,
            "exploration_decay": 0.995,
            "improvement_threshold": 0.05,
        }

        # Track effectiveness of meta-learning decisions
        self._meta_learning_effectiveness: List[float] = []

    def evolve_strategies(self, strategy_type: StrategyType) -> List[LearningStrategy]:
        """
        Evolve strategies using genetic algorithm principles.

        Creates new strategy variants through mutation and crossover,
        keeping the best performers and replacing poor performers.
        """
        strategies = [
            s for s in self.meta_engine._strategies.values()
            if s.strategy_type == strategy_type
        ]

        if len(strategies) < 2:
            return strategies

        # Get performances
        performances = [
            (s, self.meta_engine._performance.get(s.strategy_id, StrategyPerformance(s.strategy_id)))
            for s in strategies
        ]

        # Sort by performance
        performances.sort(key=lambda x: x[1].avg_score, reverse=True)

        # Keep elite strategies
        elite_count = max(1, int(len(strategies) * self._meta_meta_params["elite_fraction"]))
        elite = [s for s, _ in performances[:elite_count]]

        new_strategies = list(elite)

        # Generate new strategies through mutation and crossover
        while len(new_strategies) < len(strategies):
            if random.random() < self._meta_meta_params["crossover_rate"] and len(elite) >= 2:
                # Crossover
                parent1, parent2 = random.sample(elite, 2)
                child = self._crossover_strategies(parent1, parent2)
            else:
                # Mutation
                parent = random.choice(elite)
                child = self._mutate_strategy(parent)

            new_strategies.append(child)
            self.meta_engine._strategies[child.strategy_id] = child
            self.meta_engine._performance[child.strategy_id] = StrategyPerformance(child.strategy_id)

        # Record evolution
        self._evolution_history.append({
            "timestamp": datetime.now().isoformat(),
            "strategy_type": strategy_type.value,
            "elite_count": elite_count,
            "new_count": len(new_strategies) - elite_count,
            "best_score": performances[0][1].avg_score if performances else 0,
        })

        return new_strategies

    def _mutate_strategy(self, strategy: LearningStrategy) -> LearningStrategy:
        """Create a mutated version of a strategy."""
        new_id = hashlib.md5(f"{strategy.strategy_id}_{datetime.now().isoformat()}".encode()).hexdigest()[:12]
        child = strategy.clone(new_id)
        child.name = f"{strategy.name}_mut{child.generation}"

        # Decide mutation type
        mutation_type = random.choice(list(MutationType))

        if mutation_type == MutationType.PARAMETER_TWEAK:
            # Tweak a random parameter
            if child.parameters:
                key = random.choice(list(child.parameters.keys()))
                value = child.parameters[key]
                if isinstance(value, (int, float)):
                    child.parameters[key] = value * random.uniform(0.8, 1.2)

        elif mutation_type == MutationType.WEIGHT_ADJUSTMENT:
            # Adjust weights
            if child.weights:
                key = random.choice(list(child.weights.keys()))
                child.weights[key] = max(0.1, min(1.0, child.weights[key] + random.uniform(-0.1, 0.1)))
                # Renormalize
                total = sum(child.weights.values())
                child.weights = {k: v / total for k, v in child.weights.items()}

        elif mutation_type == MutationType.THRESHOLD_SHIFT:
            # Shift thresholds
            if child.thresholds:
                key = random.choice(list(child.thresholds.keys()))
                child.thresholds[key] = child.thresholds[key] + random.uniform(-0.05, 0.05)

        return child

    def _crossover_strategies(self, parent1: LearningStrategy, parent2: LearningStrategy) -> LearningStrategy:
        """Create a child strategy by crossing over two parents."""
        new_id = hashlib.md5(f"{parent1.strategy_id}_{parent2.strategy_id}_{datetime.now().isoformat()}".encode()).hexdigest()[:12]

        # Combine parameters from both parents
        combined_params = {}
        for key in set(parent1.parameters.keys()) | set(parent2.parameters.keys()):
            if key in parent1.parameters and key in parent2.parameters:
                # Average numeric values, random choice otherwise
                v1, v2 = parent1.parameters[key], parent2.parameters[key]
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    combined_params[key] = (v1 + v2) / 2
                else:
                    combined_params[key] = random.choice([v1, v2])
            elif key in parent1.parameters:
                combined_params[key] = parent1.parameters[key]
            else:
                combined_params[key] = parent2.parameters[key]

        # Combine weights (average)
        combined_weights = {}
        all_weight_keys = set(parent1.weights.keys()) | set(parent2.weights.keys())
        for key in all_weight_keys:
            w1 = parent1.weights.get(key, 0)
            w2 = parent2.weights.get(key, 0)
            combined_weights[key] = (w1 + w2) / 2

        # Normalize weights
        if combined_weights:
            total = sum(combined_weights.values())
            combined_weights = {k: v / total for k, v in combined_weights.items()}

        return LearningStrategy(
            strategy_id=new_id,
            strategy_type=parent1.strategy_type,
            name=f"{parent1.name}_x_{parent2.name}",
            parameters=combined_params,
            weights=combined_weights,
            thresholds={**parent1.thresholds, **parent2.thresholds},
            parent_ids=[parent1.strategy_id, parent2.strategy_id],
            generation=max(parent1.generation, parent2.generation) + 1,
        )

    def optimize_reflection_process(self) -> Optional[ReflectionImprovement]:
        """
        Analyze reflection outcomes and propose improvements.

        This is the core of meta-meta learning: improving how we reflect.
        """
        # Analyze recent outcomes
        outcomes = self.meta_engine._outcomes[-100:]
        if len(outcomes) < 20:
            return None

        # Calculate effectiveness of current reflection
        optimal_rate = sum(1 for o in outcomes if o.was_optimal) / len(outcomes)
        avg_score = sum(o.outcome_score for o in outcomes) / len(outcomes)

        # Identify patterns in suboptimal decisions
        suboptimal = [o for o in outcomes if not o.was_optimal]

        improvement = None

        if optimal_rate < 0.6:
            # Need to improve strategy selection
            if len(suboptimal) > len(outcomes) * 0.5:
                # Too many suboptimal decisions - increase exploration
                improvement = ReflectionImprovement(
                    improvement_id=hashlib.md5(f"imp_{datetime.now().isoformat()}".encode()).hexdigest()[:12],
                    improvement_type="exploration_increase",
                    description="Increase exploration rate to discover better strategies",
                    parameters={"exploration_rate_delta": 0.05},
                    predicted_benefit=0.1,
                )

        elif avg_score < 0.7:
            # Need to improve strategies themselves
            # Find worst performing strategy type
            type_scores = defaultdict(list)
            for o in outcomes:
                strategy = self.meta_engine._strategies.get(o.strategy_id)
                if strategy:
                    type_scores[strategy.strategy_type].append(o.outcome_score)

            worst_type = min(type_scores.keys(), key=lambda t: sum(type_scores[t]) / len(type_scores[t]))

            improvement = ReflectionImprovement(
                improvement_id=hashlib.md5(f"imp_{datetime.now().isoformat()}".encode()).hexdigest()[:12],
                improvement_type="strategy_evolution",
                description=f"Evolve {worst_type.value} strategies to improve performance",
                parameters={"strategy_type": worst_type.value},
                predicted_benefit=0.15,
            )

        if improvement:
            self._improvements.append(improvement)

        return improvement

    def apply_improvement(self, improvement: ReflectionImprovement) -> bool:
        """Apply a proposed improvement to the learning process."""
        if improvement.applied:
            return False

        if improvement.improvement_type == "exploration_increase":
            delta = improvement.parameters.get("exploration_rate_delta", 0.05)
            self._meta_meta_params["mutation_rate"] = min(0.5, self._meta_meta_params["mutation_rate"] + delta)
            improvement.applied = True
            return True

        elif improvement.improvement_type == "strategy_evolution":
            strategy_type = StrategyType(improvement.parameters.get("strategy_type"))
            self.evolve_strategies(strategy_type)
            improvement.applied = True
            return True

        return False

    def adapt_learning_rate(self):
        """
        Adapt the learning rate based on recent performance.

        Implements learning rate scheduling based on meta-learning effectiveness.
        """
        recent_outcomes = self.meta_engine._outcomes[-50:]
        if len(recent_outcomes) < 10:
            return

        recent_scores = [o.outcome_score for o in recent_outcomes]
        avg_recent = sum(recent_scores) / len(recent_scores)

        # Compare to older performance
        older_outcomes = self.meta_engine._outcomes[-100:-50]
        if older_outcomes:
            avg_older = sum(o.outcome_score for o in older_outcomes) / len(older_outcomes)
        else:
            avg_older = avg_recent

        # Adjust learning rate
        if avg_recent > avg_older + 0.05:
            # Improving - increase learning rate
            self._current_learning_rate = min(0.3, self._current_learning_rate * 1.1)
        elif avg_recent < avg_older - 0.05:
            # Declining - decrease learning rate
            self._current_learning_rate = max(0.01, self._current_learning_rate * 0.9)

        self._learning_rate_history.append((datetime.now(), self._current_learning_rate))
        if len(self._learning_rate_history) > 100:
            self._learning_rate_history = self._learning_rate_history[-100:]

        # Track effectiveness
        self._meta_learning_effectiveness.append(avg_recent)
        if len(self._meta_learning_effectiveness) > 100:
            self._meta_learning_effectiveness = self._meta_learning_effectiveness[-100:]

    def recursive_improve(self) -> Dict[str, Any]:
        """
        Perform recursive self-improvement cycle.

        This is the core meta-meta learning loop:
        1. Analyze current learning effectiveness
        2. Identify areas for improvement
        3. Apply improvements
        4. Measure impact
        5. Learn from the improvement process itself
        """
        results = {
            "cycle_time": datetime.now().isoformat(),
            "improvements_proposed": 0,
            "improvements_applied": 0,
            "strategies_evolved": 0,
            "learning_rate_adjusted": False,
        }

        # Step 1: Optimize reflection process
        improvement = self.optimize_reflection_process()
        if improvement:
            results["improvements_proposed"] += 1
            if self.apply_improvement(improvement):
                results["improvements_applied"] += 1

        # Step 2: Evolve underperforming strategy types
        rankings = self.meta_engine.get_strategy_rankings()
        for strategy_type in StrategyType:
            type_rankings = [r for r in rankings if r["type"] == strategy_type.value]
            if type_rankings:
                best_score = type_rankings[0]["avg_score"]
                if best_score < 0.7:  # Threshold for evolution
                    evolved = self.evolve_strategies(strategy_type)
                    results["strategies_evolved"] += len(evolved)

        # Step 3: Adapt learning rate
        old_rate = self._current_learning_rate
        self.adapt_learning_rate()
        if self._current_learning_rate != old_rate:
            results["learning_rate_adjusted"] = True
            results["new_learning_rate"] = self._current_learning_rate

        # Step 4: Record this cycle for meta-meta-meta learning
        self._evolution_history.append({
            "type": "recursive_improvement",
            **results,
        })

        return results

    def get_meta_meta_stats(self) -> Dict[str, Any]:
        """Get statistics about meta-meta learning."""
        return {
            "improvements_proposed": len(self._improvements),
            "improvements_applied": sum(1 for i in self._improvements if i.applied),
            "evolution_cycles": len(self._evolution_history),
            "current_learning_rate": self._current_learning_rate,
            "meta_meta_params": self._meta_meta_params,
            "learning_effectiveness_trend": (
                "improving" if len(self._meta_learning_effectiveness) >= 10 and
                sum(self._meta_learning_effectiveness[-5:]) / 5 >
                sum(self._meta_learning_effectiveness[-10:-5]) / 5
                else "stable"
            ),
        }


class CuttingEdgeSelfReflection:
    """
    Cutting-edge self-reflection system with meta-meta learning.

    Integrates:
    - Meta-learning: Which strategies work for which contexts
    - Meta-meta learning: Optimizing the learning process itself
    - Recursive self-improvement: Learning how to learn better
    - Adaptive reflection depth and criteria
    """

    def __init__(self):
        self.meta_engine = MetaLearningEngine()
        self.meta_meta_engine = MetaMetaLearningEngine(self.meta_engine)
        self._reflection_count = 0
        self._improvement_cycles = 0

    def select_learning_strategy(
        self,
        strategy_type: StrategyType,
        context: Dict[str, Any]
    ) -> LearningStrategy:
        """Select the best learning strategy for the context."""
        # Use meta-meta learning's adaptive exploration rate
        exploration_rate = self.meta_meta_engine._meta_meta_params.get("mutation_rate", 0.1) * 0.5

        return self.meta_engine.select_strategy(
            strategy_type=strategy_type,
            context=context,
            exploration_rate=exploration_rate
        )

    def record_learning_outcome(
        self,
        strategy_id: str,
        context: Dict[str, Any],
        score: float,
        success: bool
    ):
        """Record the outcome of a learning attempt."""
        self.meta_engine.record_strategy_outcome(
            strategy_id=strategy_id,
            context=context,
            score=score,
            success=success
        )

        self._reflection_count += 1

        # Trigger meta-meta learning periodically
        if self._reflection_count % 20 == 0:
            self.meta_meta_engine.adapt_learning_rate()

        # Trigger recursive improvement periodically
        if self._reflection_count % 50 == 0:
            self.meta_meta_engine.recursive_improve()
            self._improvement_cycles += 1

    def get_optimal_reflection_depth(self, context: Dict[str, Any]) -> str:
        """Determine optimal reflection depth based on context and learning."""
        strategy = self.select_learning_strategy(StrategyType.REFLECTION, context)
        return strategy.parameters.get("depth", "standard")

    def get_optimal_reflection_criteria(self, context: Dict[str, Any]) -> List[str]:
        """Determine optimal reflection criteria based on context and learning."""
        strategy = self.select_learning_strategy(StrategyType.REFLECTION, context)
        return strategy.parameters.get("criteria", ["relevance", "accuracy"])

    def trigger_recursive_improvement(self) -> Dict[str, Any]:
        """Manually trigger a recursive improvement cycle."""
        return self.meta_meta_engine.recursive_improve()

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics about self-reflection."""
        return {
            "reflection_count": self._reflection_count,
            "improvement_cycles": self._improvement_cycles,
            "meta_learning_stats": self.meta_engine.get_meta_learning_stats(),
            "meta_meta_stats": self.meta_meta_engine.get_meta_meta_stats(),
            "strategy_rankings": self.meta_engine.get_strategy_rankings(),
        }


# Global cutting-edge instance
_cutting_edge_self_reflection: Optional[CuttingEdgeSelfReflection] = None


def get_cutting_edge_self_reflection() -> CuttingEdgeSelfReflection:
    """Get the global cutting-edge self-reflection instance."""
    global _cutting_edge_self_reflection
    if _cutting_edge_self_reflection is None:
        _cutting_edge_self_reflection = CuttingEdgeSelfReflection()
    return _cutting_edge_self_reflection


def reset_cutting_edge_self_reflection():
    """Reset the global instance (for testing)."""
    global _cutting_edge_self_reflection
    _cutting_edge_self_reflection = None

"""
AI Feedback System

Collects and processes user feedback on AI responses for continuous improvement.
Enterprise features include:
- Thumbs up/down feedback
- Detailed feedback with corrections
- Response quality metrics
- Learning signals for prompt improvement
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from uuid import UUID, uuid4
from enum import Enum

logger = logging.getLogger(__name__)


class FeedbackRating(str, Enum):
    """Feedback rating types."""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class FeedbackCategory(str, Enum):
    """Categories of feedback for classification."""
    ACCURACY = "accuracy"
    HELPFULNESS = "helpfulness"
    RELEVANCE = "relevance"
    CLARITY = "clarity"
    COMPLETENESS = "completeness"
    SAFETY = "safety"
    SPEED = "speed"


@dataclass
class AIFeedback:
    """User feedback on an AI response."""
    id: UUID
    conversation_id: UUID
    message_id: UUID
    tenant_id: UUID
    user_id: UUID
    rating: FeedbackRating
    categories: List[FeedbackCategory]
    comment: Optional[str]
    correction: Optional[str]
    metadata: Dict[str, Any]
    created_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "conversation_id": str(self.conversation_id),
            "message_id": str(self.message_id),
            "tenant_id": str(self.tenant_id),
            "user_id": str(self.user_id),
            "rating": self.rating.value,
            "categories": [c.value for c in self.categories],
            "comment": self.comment,
            "correction": self.correction,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class FeedbackStats:
    """Aggregated feedback statistics."""
    total_responses: int
    positive_count: int
    negative_count: int
    neutral_count: int
    positive_rate: float
    category_breakdown: Dict[str, Dict[str, int]]
    avg_response_time_ms: float
    common_issues: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_responses": self.total_responses,
            "positive_count": self.positive_count,
            "negative_count": self.negative_count,
            "neutral_count": self.neutral_count,
            "positive_rate": self.positive_rate,
            "category_breakdown": self.category_breakdown,
            "avg_response_time_ms": self.avg_response_time_ms,
            "common_issues": self.common_issues,
        }


class FeedbackService:
    """
    Manages AI feedback collection and analysis.

    Features:
    - Collect user feedback on AI responses
    - Track response quality metrics
    - Identify common issues and patterns
    - Generate improvement signals
    """

    def __init__(self, db_session=None, redis_client=None):
        self._db = db_session
        self._redis = redis_client
        # In-memory store for development
        self._feedback_store: List[AIFeedback] = []

    async def submit_feedback(
        self,
        conversation_id: UUID,
        message_id: UUID,
        tenant_id: UUID,
        user_id: UUID,
        rating: FeedbackRating,
        categories: Optional[List[FeedbackCategory]] = None,
        comment: Optional[str] = None,
        correction: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AIFeedback:
        """Submit feedback for an AI response."""
        feedback = AIFeedback(
            id=uuid4(),
            conversation_id=conversation_id,
            message_id=message_id,
            tenant_id=tenant_id,
            user_id=user_id,
            rating=rating,
            categories=categories or [],
            comment=comment,
            correction=correction,
            metadata=metadata or {},
            created_at=datetime.now(timezone.utc),
        )

        # Store feedback
        self._feedback_store.append(feedback)

        # Update Redis counters for quick stats
        if self._redis:
            await self._update_counters(tenant_id, rating)

        logger.info(
            f"Feedback submitted: {rating.value}",
            extra={
                "event_type": "ai.feedback_submitted",
                "conversation_id": str(conversation_id),
                "message_id": str(message_id),
                "rating": rating.value,
                "categories": [c.value for c in (categories or [])],
            }
        )

        return feedback

    async def get_feedback(
        self,
        tenant_id: UUID,
        conversation_id: Optional[UUID] = None,
        rating: Optional[FeedbackRating] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 50,
    ) -> List[AIFeedback]:
        """Get feedback with optional filters."""
        results = []

        for fb in self._feedback_store:
            if fb.tenant_id != tenant_id:
                continue
            if conversation_id and fb.conversation_id != conversation_id:
                continue
            if rating and fb.rating != rating:
                continue
            if start_date and fb.created_at < start_date:
                continue
            if end_date and fb.created_at > end_date:
                continue

            results.append(fb)

            if len(results) >= limit:
                break

        return results

    async def get_stats(
        self,
        tenant_id: UUID,
        days: int = 7,
    ) -> FeedbackStats:
        """Get aggregated feedback statistics."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        # Filter feedback for time period
        feedback = [
            fb for fb in self._feedback_store
            if fb.tenant_id == tenant_id and fb.created_at >= cutoff
        ]

        total = len(feedback)
        positive = sum(1 for fb in feedback if fb.rating == FeedbackRating.POSITIVE)
        negative = sum(1 for fb in feedback if fb.rating == FeedbackRating.NEGATIVE)
        neutral = sum(1 for fb in feedback if fb.rating == FeedbackRating.NEUTRAL)

        # Category breakdown
        category_breakdown: Dict[str, Dict[str, int]] = {}
        for fb in feedback:
            for cat in fb.categories:
                if cat.value not in category_breakdown:
                    category_breakdown[cat.value] = {"positive": 0, "negative": 0, "neutral": 0}
                category_breakdown[cat.value][fb.rating.value] += 1

        # Common issues (negative feedback with categories)
        common_issues: Dict[str, int] = {}
        for fb in feedback:
            if fb.rating == FeedbackRating.NEGATIVE:
                for cat in fb.categories:
                    common_issues[cat.value] = common_issues.get(cat.value, 0) + 1

        sorted_issues = sorted(
            common_issues.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        return FeedbackStats(
            total_responses=total,
            positive_count=positive,
            negative_count=negative,
            neutral_count=neutral,
            positive_rate=positive / total if total > 0 else 0.0,
            category_breakdown=category_breakdown,
            avg_response_time_ms=0.0,  # Would calculate from metadata
            common_issues=[{"category": k, "count": v} for k, v in sorted_issues],
        )

    async def get_improvement_signals(
        self,
        tenant_id: UUID,
    ) -> List[Dict[str, Any]]:
        """
        Analyze feedback to generate improvement signals.

        Returns actionable insights for prompt improvement.
        """
        stats = await self.get_stats(tenant_id, days=30)
        signals = []

        # Low overall satisfaction
        if stats.positive_rate < 0.8:
            signals.append({
                "type": "low_satisfaction",
                "severity": "high" if stats.positive_rate < 0.6 else "medium",
                "message": f"Positive feedback rate is {stats.positive_rate:.1%}",
                "recommendation": "Review negative feedback for common patterns",
            })

        # Category-specific issues
        for category, counts in stats.category_breakdown.items():
            total_cat = counts.get("positive", 0) + counts.get("negative", 0)
            if total_cat > 5:
                neg_rate = counts.get("negative", 0) / total_cat
                if neg_rate > 0.3:
                    signals.append({
                        "type": "category_issue",
                        "category": category,
                        "severity": "high" if neg_rate > 0.5 else "medium",
                        "message": f"{category} has {neg_rate:.1%} negative feedback",
                        "recommendation": f"Improve {category} in AI responses",
                    })

        # Common issues
        for issue in stats.common_issues[:3]:
            signals.append({
                "type": "common_issue",
                "category": issue["category"],
                "count": issue["count"],
                "recommendation": f"Address {issue['category']} issues in prompts",
            })

        return signals

    async def _update_counters(self, tenant_id: UUID, rating: FeedbackRating):
        """Update Redis counters for quick stats."""
        if not self._redis:
            return

        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            key = f"feedback:{tenant_id}:{today}:{rating.value}"
            await self._redis.incr(key)
            await self._redis.expire(key, 86400 * 30)  # 30 day TTL
        except Exception as e:
            logger.warning(f"Failed to update feedback counter: {e}")


# ============================================================================
# Response Quality Tracker
# ============================================================================

class ResponseQualityTracker:
    """
    Tracks AI response quality metrics over time.

    Metrics tracked:
    - Response time
    - Token usage efficiency
    - Tool usage patterns
    - Error rates
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client
        self._metrics: List[Dict[str, Any]] = []

    async def record_response(
        self,
        conversation_id: UUID,
        message_id: UUID,
        tenant_id: UUID,
        agent_type: str,
        response_time_ms: int,
        input_tokens: int,
        output_tokens: int,
        tools_used: List[str],
        error: Optional[str] = None,
    ):
        """Record metrics for an AI response."""
        metric = {
            "conversation_id": str(conversation_id),
            "message_id": str(message_id),
            "tenant_id": str(tenant_id),
            "agent_type": agent_type,
            "response_time_ms": response_time_ms,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "tools_used": tools_used,
            "tool_count": len(tools_used),
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self._metrics.append(metric)

        # Update Redis for real-time metrics
        if self._redis:
            await self._update_metrics(tenant_id, agent_type, metric)

        logger.info(
            f"Response quality recorded",
            extra={
                "event_type": "ai.response_quality",
                **metric,
            }
        )

    async def get_quality_summary(
        self,
        tenant_id: UUID,
        days: int = 7,
    ) -> Dict[str, Any]:
        """Get quality summary for tenant."""
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        relevant = [
            m for m in self._metrics
            if m["tenant_id"] == str(tenant_id)
            and datetime.fromisoformat(m["timestamp"]) >= cutoff
        ]

        if not relevant:
            return {
                "total_responses": 0,
                "avg_response_time_ms": 0,
                "avg_tokens": 0,
                "error_rate": 0,
            }

        total = len(relevant)
        errors = sum(1 for m in relevant if m.get("error"))

        return {
            "total_responses": total,
            "avg_response_time_ms": sum(m["response_time_ms"] for m in relevant) / total,
            "avg_input_tokens": sum(m["input_tokens"] for m in relevant) / total,
            "avg_output_tokens": sum(m["output_tokens"] for m in relevant) / total,
            "avg_total_tokens": sum(m["total_tokens"] for m in relevant) / total,
            "error_rate": errors / total if total > 0 else 0,
            "most_used_tools": self._get_top_tools(relevant),
            "by_agent_type": self._group_by_agent(relevant),
        }

    def _get_top_tools(self, metrics: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
        """Get most frequently used tools."""
        tool_counts: Dict[str, int] = {}
        for m in metrics:
            for tool in m.get("tools_used", []):
                tool_counts[tool] = tool_counts.get(tool, 0) + 1

        sorted_tools = sorted(tool_counts.items(), key=lambda x: x[1], reverse=True)
        return [{"tool": t, "count": c} for t, c in sorted_tools[:limit]]

    def _group_by_agent(self, metrics: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Group metrics by agent type."""
        by_agent: Dict[str, List[Dict[str, Any]]] = {}
        for m in metrics:
            agent = m.get("agent_type", "unknown")
            if agent not in by_agent:
                by_agent[agent] = []
            by_agent[agent].append(m)

        result = {}
        for agent, agent_metrics in by_agent.items():
            total = len(agent_metrics)
            result[agent] = {
                "count": total,
                "avg_response_time_ms": sum(m["response_time_ms"] for m in agent_metrics) / total,
                "avg_tokens": sum(m["total_tokens"] for m in agent_metrics) / total,
            }

        return result

    async def _update_metrics(
        self,
        tenant_id: UUID,
        agent_type: str,
        metric: Dict[str, Any],
    ):
        """Update Redis metrics for real-time dashboard."""
        if not self._redis:
            return

        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # Increment response count
            await self._redis.incr(f"quality:{tenant_id}:{today}:count")

            # Add to response time list for percentile calculation
            await self._redis.lpush(
                f"quality:{tenant_id}:{today}:response_times",
                metric["response_time_ms"],
            )
            await self._redis.ltrim(f"quality:{tenant_id}:{today}:response_times", 0, 999)

            # Set TTL
            await self._redis.expire(f"quality:{tenant_id}:{today}:count", 86400 * 7)
            await self._redis.expire(f"quality:{tenant_id}:{today}:response_times", 86400 * 7)
        except Exception as e:
            logger.warning(f"Failed to update quality metrics: {e}")


# ============================================================================
# Singleton Factories
# ============================================================================

from datetime import timedelta

_feedback_service: Optional[FeedbackService] = None
_quality_tracker: Optional[ResponseQualityTracker] = None


async def get_feedback_service(db_session=None, redis_client=None) -> FeedbackService:
    """Get singleton instance of feedback service."""
    global _feedback_service
    if _feedback_service is None:
        _feedback_service = FeedbackService(
            db_session=db_session,
            redis_client=redis_client,
        )
    return _feedback_service


async def get_quality_tracker(redis_client=None) -> ResponseQualityTracker:
    """Get singleton instance of quality tracker."""
    global _quality_tracker
    if _quality_tracker is None:
        _quality_tracker = ResponseQualityTracker(redis_client=redis_client)
    return _quality_tracker

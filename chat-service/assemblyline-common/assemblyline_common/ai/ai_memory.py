"""
AI Memory Service - Long-term learning memory for the AI assistant.

Stores and recalls learned patterns, corrections, and domain knowledge
across conversations. All methods catch exceptions internally and never
block the AI response pipeline (same pattern as chat_history.py).
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import select, update, desc
from sqlalchemy.ext.asyncio import AsyncSession

from assemblyline_common.models.ai_chat import AILearnedPattern

logger = logging.getLogger(__name__)


class AIMemoryService:
    """Long-term learning memory for the AI assistant."""

    async def store_pattern(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        pattern_type: str,
        learned_content: str,
        agent_type: Optional[str] = None,
        trigger_context: Optional[str] = None,
        source: str = "self_correction",
        conversation_id: Optional[UUID] = None,
    ) -> Optional[UUID]:
        """
        Store a learned pattern.

        Args:
            db: Database session
            tenant_id: Tenant ID for isolation
            pattern_type: One of 'correction', 'successful_flow', 'user_preference', 'domain_knowledge'
            learned_content: The actual learned information
            agent_type: Which agent learned this (e.g., 'flow-builder')
            trigger_context: What situation triggered this learning
            source: How it was learned ('user_feedback', 'self_correction', 'prompt_update', 'explicit_teach')
            conversation_id: Which conversation this came from

        Returns:
            Pattern ID on success, None on failure
        """
        try:
            pattern_id = uuid4()
            pattern = AILearnedPattern(
                id=pattern_id,
                tenant_id=tenant_id,
                pattern_type=pattern_type,
                agent_type=agent_type,
                trigger_context=trigger_context,
                learned_content=learned_content,
                source=source,
                conversation_id=conversation_id,
            )
            db.add(pattern)
            await db.flush()
            logger.info(f"Stored learned pattern: {pattern_type} ({source}) - {learned_content[:80]}")
            return pattern_id
        except Exception as e:
            logger.error(f"Failed to store learned pattern: {e}", exc_info=True)
            return None

    async def recall_patterns(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        agent_type: Optional[str] = None,
        pattern_type: Optional[str] = None,
        limit: int = 10,
    ) -> List[dict]:
        """
        Recall relevant learned patterns for the current context.

        Returns patterns ordered by confidence and recency, filtered by
        tenant, agent type, and optionally pattern type.

        Args:
            db: Database session
            tenant_id: Tenant ID for isolation
            agent_type: Filter by agent type (optional)
            pattern_type: Filter by pattern type (optional)
            limit: Max patterns to return (default 10)

        Returns:
            List of pattern dicts with keys: id, pattern_type, learned_content, source, confidence
        """
        try:
            query = (
                select(AILearnedPattern)
                .where(
                    AILearnedPattern.tenant_id == tenant_id,
                    AILearnedPattern.is_active == True,
                )
                .order_by(
                    desc(AILearnedPattern.confidence),
                    desc(AILearnedPattern.updated_at),
                )
                .limit(limit)
            )

            if agent_type:
                query = query.where(
                    (AILearnedPattern.agent_type == agent_type)
                    | (AILearnedPattern.agent_type == None)
                )

            if pattern_type:
                query = query.where(AILearnedPattern.pattern_type == pattern_type)

            result = await db.execute(query)
            patterns = result.scalars().all()

            return [
                {
                    "id": str(p.id),
                    "pattern_type": p.pattern_type,
                    "learned_content": p.learned_content,
                    "source": p.source,
                    "confidence": p.confidence,
                    "usage_count": p.usage_count,
                }
                for p in patterns
            ]
        except Exception as e:
            logger.error(f"Failed to recall patterns: {e}", exc_info=True)
            return []

    async def record_feedback(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        conversation_id: UUID,
        message_id: str,
        rating: int,
        comment: Optional[str] = None,
    ) -> bool:
        """
        Record user feedback (thumbs up/down) on a message.

        Negative feedback creates a correction pattern for learning.

        Args:
            db: Database session
            tenant_id: Tenant ID
            conversation_id: Conversation this feedback relates to
            message_id: Specific message being rated
            rating: 1 for positive, -1 for negative
            comment: Optional user comment

        Returns:
            True on success, False on failure
        """
        try:
            if rating < 0:
                content = f"User disliked response (message {message_id})"
                if comment:
                    content += f": {comment}"
                await self.store_pattern(
                    db=db,
                    tenant_id=tenant_id,
                    pattern_type="correction",
                    learned_content=content,
                    source="user_feedback",
                    conversation_id=conversation_id,
                )
            logger.info(f"Recorded feedback: rating={rating} for message {message_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to record feedback: {e}", exc_info=True)
            return False

    async def increment_usage(self, db: AsyncSession, pattern_id: UUID) -> None:
        """Track pattern usage for relevance ranking."""
        try:
            now = datetime.now(timezone.utc)
            await db.execute(
                update(AILearnedPattern)
                .where(AILearnedPattern.id == pattern_id)
                .values(
                    usage_count=AILearnedPattern.usage_count + 1,
                    last_used_at=now,
                )
            )
            await db.flush()
        except Exception as e:
            logger.error(f"Failed to increment pattern usage: {e}", exc_info=True)


# Singleton
_ai_memory_service: Optional[AIMemoryService] = None


def get_ai_memory_service() -> AIMemoryService:
    """Get or create the singleton AIMemoryService."""
    global _ai_memory_service
    if _ai_memory_service is None:
        _ai_memory_service = AIMemoryService()
    return _ai_memory_service

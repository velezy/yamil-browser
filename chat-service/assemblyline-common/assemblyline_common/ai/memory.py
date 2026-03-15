"""
Conversation Memory System

Provides persistent memory and context management for AI conversations.
Enterprise features include:
- Short-term memory (current conversation)
- Long-term memory (cross-conversation patterns)
- Entity extraction and tracking
- Token budget management
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any
from uuid import UUID, uuid4
from enum import Enum
import json
import hashlib

logger = logging.getLogger(__name__)


class MemoryType(str, Enum):
    """Types of memory entries."""
    MESSAGE = "message"
    ENTITY = "entity"
    PREFERENCE = "preference"
    CONTEXT = "context"
    SUMMARY = "summary"


@dataclass
class MemoryEntry:
    """A single memory entry."""
    id: UUID
    conversation_id: UUID
    tenant_id: UUID
    memory_type: MemoryType
    content: str
    metadata: Dict[str, Any]
    relevance_score: float
    created_at: datetime
    expires_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "conversation_id": str(self.conversation_id),
            "tenant_id": str(self.tenant_id),
            "memory_type": self.memory_type.value,
            "content": self.content,
            "metadata": self.metadata,
            "relevance_score": self.relevance_score,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }


@dataclass
class ConversationContext:
    """Current conversation context."""
    conversation_id: UUID
    tenant_id: UUID
    user_id: UUID
    agent_type: str
    messages: List[Dict[str, Any]] = field(default_factory=list)
    entities: Dict[str, Any] = field(default_factory=dict)
    preferences: Dict[str, Any] = field(default_factory=dict)
    token_count: int = 0
    max_tokens: int = 100000  # Conversation token budget
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def add_message(self, role: str, content: str, tokens: int = 0):
        """Add a message to the conversation."""
        self.messages.append({
            "role": role,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.token_count += tokens
        self.last_activity = datetime.now(timezone.utc)

    def get_recent_messages(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent messages for context."""
        return self.messages[-limit:]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conversation_id": str(self.conversation_id),
            "tenant_id": str(self.tenant_id),
            "user_id": str(self.user_id),
            "agent_type": self.agent_type,
            "message_count": len(self.messages),
            "token_count": self.token_count,
            "entities": self.entities,
            "preferences": self.preferences,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
        }


class ConversationMemory:
    """
    Manages conversation memory with automatic summarization and entity tracking.

    Features:
    - Maintains message history within token budget
    - Extracts and tracks entities (flows, connectors, users)
    - Summarizes old context to save tokens
    - Provides relevant context for AI calls
    """

    def __init__(self, redis_client=None, db_session=None):
        self._redis = redis_client
        self._db = db_session
        self._contexts: Dict[UUID, ConversationContext] = {}

    async def get_or_create_context(
        self,
        conversation_id: UUID,
        tenant_id: UUID,
        user_id: UUID,
        agent_type: str,
    ) -> ConversationContext:
        """Get existing context or create new one."""
        # Try memory cache first
        if conversation_id in self._contexts:
            return self._contexts[conversation_id]

        # Try Redis cache
        if self._redis:
            try:
                cached = await self._redis.get(f"conv:{conversation_id}")
                if cached:
                    data = json.loads(cached)
                    context = self._deserialize_context(data)
                    self._contexts[conversation_id] = context
                    return context
            except Exception as e:
                logger.warning(f"Redis cache miss: {e}")

        # Create new context
        context = ConversationContext(
            conversation_id=conversation_id,
            tenant_id=tenant_id,
            user_id=user_id,
            agent_type=agent_type,
        )
        self._contexts[conversation_id] = context

        return context

    async def save_context(self, context: ConversationContext):
        """Save context to cache and database."""
        self._contexts[context.conversation_id] = context

        # Save to Redis with TTL
        if self._redis:
            try:
                data = self._serialize_context(context)
                await self._redis.setex(
                    f"conv:{context.conversation_id}",
                    3600 * 24,  # 24 hour TTL
                    json.dumps(data),
                )
            except Exception as e:
                logger.warning(f"Redis save failed: {e}")

    async def add_message(
        self,
        conversation_id: UUID,
        role: str,
        content: str,
        tokens: int,
        tenant_id: UUID,
        user_id: UUID,
        agent_type: str,
    ) -> ConversationContext:
        """Add a message and manage context window."""
        context = await self.get_or_create_context(
            conversation_id, tenant_id, user_id, agent_type
        )

        # Check if we need to summarize old messages
        if context.token_count + tokens > context.max_tokens * 0.8:
            await self._summarize_old_messages(context)

        context.add_message(role, content, tokens)
        await self.save_context(context)

        return context

    async def extract_entities(
        self,
        content: str,
        context: ConversationContext,
    ) -> Dict[str, Any]:
        """Extract entities from content and update context."""
        entities = {}

        # Simple pattern matching for common entities
        import re

        # Flow IDs
        flow_matches = re.findall(r'flow[_-]?([a-f0-9-]{36}|[a-f0-9-]{8})', content, re.I)
        if flow_matches:
            entities["flows"] = list(set(flow_matches))

        # Connector names
        connector_patterns = [
            r'(epic|cerner|meditech|allscripts|athena)\s*(fhir|hl7)?',
            r'(s3|kafka|postgres|dynamodb)\s*connector',
        ]
        for pattern in connector_patterns:
            matches = re.findall(pattern, content, re.I)
            if matches:
                if "connectors" not in entities:
                    entities["connectors"] = []
                entities["connectors"].extend([" ".join(m) for m in matches])

        # HL7 message types
        hl7_matches = re.findall(r'\b(ADT|ORM|ORU|MDM|BAR|DFT)[_^][A-Z]\d{2}\b', content)
        if hl7_matches:
            entities["hl7_types"] = list(set(hl7_matches))

        # Update context
        context.entities.update(entities)

        return entities

    async def get_relevant_context(
        self,
        conversation_id: UUID,
        query: str,
        max_messages: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get relevant context for the current query."""
        if conversation_id not in self._contexts:
            return []

        context = self._contexts[conversation_id]
        messages = context.get_recent_messages(max_messages)

        # Add entity context if relevant
        if context.entities:
            messages.insert(0, {
                "role": "system",
                "content": f"Known entities: {json.dumps(context.entities)}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        return messages

    async def _summarize_old_messages(self, context: ConversationContext):
        """Summarize old messages to save tokens."""
        if len(context.messages) < 10:
            return

        # Keep last 5 messages, summarize the rest
        old_messages = context.messages[:-5]
        context.messages = context.messages[-5:]

        # Create summary (in production, use AI for this)
        summary_content = f"Previous conversation summary: {len(old_messages)} messages exchanged. "
        if context.entities:
            summary_content += f"Entities discussed: {list(context.entities.keys())}"

        # Prepend summary
        context.messages.insert(0, {
            "role": "system",
            "content": summary_content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # Estimate token reduction (rough estimate)
        context.token_count = context.token_count // 3

        logger.info(
            f"Summarized conversation {context.conversation_id}",
            extra={
                "event_type": "ai.memory_summarized",
                "messages_summarized": len(old_messages),
            }
        )

    def _serialize_context(self, context: ConversationContext) -> Dict[str, Any]:
        """Serialize context for storage."""
        return {
            "conversation_id": str(context.conversation_id),
            "tenant_id": str(context.tenant_id),
            "user_id": str(context.user_id),
            "agent_type": context.agent_type,
            "messages": context.messages,
            "entities": context.entities,
            "preferences": context.preferences,
            "token_count": context.token_count,
            "max_tokens": context.max_tokens,
            "created_at": context.created_at.isoformat(),
            "last_activity": context.last_activity.isoformat(),
        }

    def _deserialize_context(self, data: Dict[str, Any]) -> ConversationContext:
        """Deserialize context from storage."""
        return ConversationContext(
            conversation_id=UUID(data["conversation_id"]),
            tenant_id=UUID(data["tenant_id"]),
            user_id=UUID(data["user_id"]),
            agent_type=data["agent_type"],
            messages=data.get("messages", []),
            entities=data.get("entities", {}),
            preferences=data.get("preferences", {}),
            token_count=data.get("token_count", 0),
            max_tokens=data.get("max_tokens", 100000),
            created_at=datetime.fromisoformat(data["created_at"]),
            last_activity=datetime.fromisoformat(data["last_activity"]),
        )


# ============================================================================
# Token Budget Manager
# ============================================================================

class TokenBudgetManager:
    """
    Manages token budgets across tenants and users.

    Enterprise features:
    - Per-tenant daily/monthly limits
    - Per-user rate limiting
    - Cost tracking and alerts
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client

    async def check_budget(
        self,
        tenant_id: UUID,
        user_id: UUID,
        estimated_tokens: int,
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Check if request is within budget.

        Returns:
            Tuple of (allowed, budget_info)
        """
        # Default limits (in production, load from settings)
        daily_limit = 1_000_000  # 1M tokens/day
        user_daily_limit = 100_000  # 100k tokens/user/day

        current_usage = await self._get_usage(tenant_id, user_id)

        budget_info = {
            "tenant_daily_limit": daily_limit,
            "tenant_daily_used": current_usage.get("tenant_daily", 0),
            "user_daily_limit": user_daily_limit,
            "user_daily_used": current_usage.get("user_daily", 0),
            "estimated_tokens": estimated_tokens,
        }

        # Check tenant limit
        if current_usage.get("tenant_daily", 0) + estimated_tokens > daily_limit:
            budget_info["reason"] = "Tenant daily limit exceeded"
            return False, budget_info

        # Check user limit
        if current_usage.get("user_daily", 0) + estimated_tokens > user_daily_limit:
            budget_info["reason"] = "User daily limit exceeded"
            return False, budget_info

        return True, budget_info

    async def record_usage(
        self,
        tenant_id: UUID,
        user_id: UUID,
        tokens_used: int,
        model: str,
    ):
        """Record token usage."""
        if not self._redis:
            return

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            # Increment tenant daily usage
            await self._redis.incrby(
                f"budget:tenant:{tenant_id}:{today}",
                tokens_used,
            )
            await self._redis.expire(f"budget:tenant:{tenant_id}:{today}", 86400 * 2)

            # Increment user daily usage
            await self._redis.incrby(
                f"budget:user:{user_id}:{today}",
                tokens_used,
            )
            await self._redis.expire(f"budget:user:{user_id}:{today}", 86400 * 2)

            logger.info(
                f"Recorded {tokens_used} tokens for user {user_id}",
                extra={
                    "event_type": "ai.tokens_used",
                    "tenant_id": str(tenant_id),
                    "user_id": str(user_id),
                    "tokens": tokens_used,
                    "model": model,
                }
            )
        except Exception as e:
            logger.warning(f"Failed to record token usage: {e}")

    async def _get_usage(
        self,
        tenant_id: UUID,
        user_id: UUID,
    ) -> Dict[str, int]:
        """Get current usage for tenant and user."""
        if not self._redis:
            return {"tenant_daily": 0, "user_daily": 0}

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            tenant_usage = await self._redis.get(f"budget:tenant:{tenant_id}:{today}")
            user_usage = await self._redis.get(f"budget:user:{user_id}:{today}")

            return {
                "tenant_daily": int(tenant_usage or 0),
                "user_daily": int(user_usage or 0),
            }
        except Exception:
            return {"tenant_daily": 0, "user_daily": 0}


# ============================================================================
# Singleton Factories
# ============================================================================

_memory_instance: Optional[ConversationMemory] = None
_budget_manager: Optional[TokenBudgetManager] = None


async def get_conversation_memory(redis_client=None) -> ConversationMemory:
    """Get singleton instance of conversation memory."""
    global _memory_instance
    if _memory_instance is None:
        _memory_instance = ConversationMemory(redis_client=redis_client)
    return _memory_instance


async def get_token_budget_manager(redis_client=None) -> TokenBudgetManager:
    """Get singleton instance of token budget manager."""
    global _budget_manager
    if _budget_manager is None:
        _budget_manager = TokenBudgetManager(redis_client=redis_client)
    return _budget_manager

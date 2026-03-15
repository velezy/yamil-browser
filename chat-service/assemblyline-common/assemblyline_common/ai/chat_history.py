"""
Chat History Service - Non-blocking persistence for AI conversations.

Provides CRUD operations for storing and retrieving AI chat history.
All methods catch exceptions internally and never block the AI response pipeline.
Compatible with PostgreSQL (local + AWS RDS) and SQLite.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from assemblyline_common.models.ai_chat import AIConversation, AIMessage

logger = logging.getLogger(__name__)


class ChatHistoryService:
    """
    Non-blocking persistence layer for AI conversations.

    All public methods catch exceptions and log errors rather than
    raising - AI responses must never be blocked by persistence failures.
    """

    async def create_conversation(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        user_id: UUID,
        agent_type: str,
        title: Optional[str] = None,
        conversation_id: Optional[UUID] = None,
    ) -> Optional[UUID]:
        """
        Create a new conversation record.

        Returns the conversation ID, or None on failure.
        """
        try:
            conv_id = conversation_id or uuid4()
            conversation = AIConversation(
                id=conv_id,
                tenant_id=tenant_id,
                user_id=user_id,
                agent_type=agent_type,
                title=title,
                status="active",
                message_count=0,
                prompt_tokens=0,
                completion_tokens=0,
            )
            db.add(conversation)
            await db.flush()
            return conv_id
        except Exception as e:
            logger.error(f"Failed to create conversation: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return None

    async def add_message(
        self,
        db: AsyncSession,
        conversation_id: UUID,
        role: str,
        content: str,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        contained_phi: bool = False,
        phi_was_masked: bool = False,
        phi_types: Optional[List[str]] = None,
        actions: Optional[List[dict]] = None,
        extra_data: Optional[dict] = None,
    ) -> Optional[UUID]:
        """
        Add a message to an existing conversation and update stats.

        Returns the message ID, or None on failure.
        """
        try:
            msg_id = uuid4()
            message = AIMessage(
                id=msg_id,
                conversation_id=conversation_id,
                role=role,
                content=content,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                contained_phi=contained_phi,
                phi_was_masked=phi_was_masked,
                phi_types=phi_types or [],
                actions=actions or [],
                extra_data=extra_data or {},
            )
            db.add(message)

            # Update conversation stats
            now = datetime.now(timezone.utc)
            await db.execute(
                update(AIConversation)
                .where(AIConversation.id == conversation_id)
                .values(
                    message_count=AIConversation.message_count + 1,
                    prompt_tokens=AIConversation.prompt_tokens + prompt_tokens,
                    completion_tokens=AIConversation.completion_tokens + completion_tokens,
                    last_message_at=now,
                    updated_at=now,
                )
            )
            await db.flush()
            return msg_id
        except Exception as e:
            logger.error(f"Failed to add message to conversation {conversation_id}: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return None

    async def update_title(
        self,
        db: AsyncSession,
        conversation_id: UUID,
        title: str,
    ) -> bool:
        """
        Update conversation title (typically from first user message, truncated to 100 chars).

        Returns True on success, False on failure.
        """
        try:
            truncated = title[:100] if len(title) > 100 else title
            await db.execute(
                update(AIConversation)
                .where(AIConversation.id == conversation_id)
                .values(title=truncated)
            )
            await db.flush()
            return True
        except Exception as e:
            logger.error(f"Failed to update title for conversation {conversation_id}: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return False

    async def list_conversations(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
        status: str = "active",
    ) -> List[AIConversation]:
        """
        List conversations for a user, ordered by last_message_at DESC.

        Returns list of AIConversation objects, or empty list on failure.
        """
        try:
            stmt = (
                select(AIConversation)
                .where(
                    AIConversation.tenant_id == tenant_id,
                    AIConversation.user_id == user_id,
                    AIConversation.status == status,
                )
                .order_by(AIConversation.last_message_at.desc())
                .limit(limit)
                .offset(offset)
            )
            result = await db.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Failed to list conversations for user {user_id}: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return []

    async def get_conversation(
        self,
        db: AsyncSession,
        conversation_id: UUID,
    ) -> Optional[AIConversation]:
        """
        Get a single conversation by ID.

        Returns AIConversation or None on failure/not found.
        """
        try:
            stmt = select(AIConversation).where(AIConversation.id == conversation_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Failed to get conversation {conversation_id}: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return None

    async def get_messages(
        self,
        db: AsyncSession,
        conversation_id: UUID,
        limit: int = 200,
        offset: int = 0,
    ) -> List[AIMessage]:
        """
        Get messages for a conversation, ordered by created_at ASC.

        Returns list of AIMessage objects, or empty list on failure.
        """
        try:
            stmt = (
                select(AIMessage)
                .where(AIMessage.conversation_id == conversation_id)
                .order_by(AIMessage.created_at.asc())
                .limit(limit)
                .offset(offset)
            )
            result = await db.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Failed to get messages for conversation {conversation_id}: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return []

    async def close_conversation(
        self,
        db: AsyncSession,
        conversation_id: UUID,
    ) -> bool:
        """Mark a conversation as closed."""
        try:
            await db.execute(
                update(AIConversation)
                .where(AIConversation.id == conversation_id)
                .values(status="closed", updated_at=datetime.now(timezone.utc))
            )
            await db.flush()
            return True
        except Exception as e:
            logger.error(f"Failed to close conversation {conversation_id}: {e}")
            try:
                await db.rollback()
            except Exception:
                pass
            return False


    async def ensure_tables(self, engine) -> None:
        """
        Create tables if they don't exist (for SQLite and local dev).

        For PostgreSQL (local + RDS), use Flyway migration V063__ai_chat_history.sql.
        For SQLite, call this at startup to create tables via SQLAlchemy DDL.
        Uses schema_translate_map to handle SQLite's lack of schema support.
        """
        try:
            from sqlalchemy import inspect
            dialect_name = engine.dialect.name

            if dialect_name == "sqlite":
                # SQLite: create tables with schema mapped to None
                async with engine.execution_options(
                    schema_translate_map={"common": None}
                ).begin() as conn:
                    def _create_sqlite(sync_conn):
                        AIConversation.__table__.create(sync_conn, checkfirst=True)
                        AIMessage.__table__.create(sync_conn, checkfirst=True)
                        logger.info("Created ai_conversations and ai_messages tables (SQLite mode)")
                    await conn.run_sync(_create_sqlite)
            else:
                # PostgreSQL: verify tables exist (migration should have created them)
                async with engine.begin() as conn:
                    def _check_pg(sync_conn):
                        inspector = inspect(sync_conn)
                        existing = inspector.get_table_names(schema="common")
                        if "ai_conversations" not in existing:
                            logger.warning(
                                "ai_conversations table missing in PostgreSQL - "
                                "run migration V063__ai_chat_history.sql"
                            )
                    await conn.run_sync(_check_pg)
        except Exception as e:
            logger.error(f"Failed to ensure chat history tables: {e}", exc_info=True)


# Singleton
_chat_history_service: Optional[ChatHistoryService] = None


def get_chat_history_service() -> ChatHistoryService:
    """Get singleton ChatHistoryService instance."""
    global _chat_history_service
    if _chat_history_service is None:
        _chat_history_service = ChatHistoryService()
    return _chat_history_service

"""
SQLAlchemy models for AI Chat History.

Multi-database support: PostgreSQL (local + AWS RDS) and SQLite.
Uses TypeDecorators to handle dialect differences transparently.
"""

import json
from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import (
    String, Integer, Boolean, Text, DateTime, Float, ForeignKey,
    Index, TypeDecorator,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from assemblyline_common.models.common import Base


# ============================================================================
# Dialect-Aware Type Decorators (PostgreSQL + SQLite)
# ============================================================================

class PortableUUID(TypeDecorator):
    """UUID type that works on PostgreSQL (native UUID) and SQLite (CHAR(36))."""

    impl = String(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import UUID as PGUUID
            return dialect.type_descriptor(PGUUID(as_uuid=True))
        return dialect.type_descriptor(String(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value if isinstance(value, UUID) else UUID(str(value))
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, UUID):
            return value
        return UUID(str(value))


class PortableJSONB(TypeDecorator):
    """JSONB type that works on PostgreSQL (native JSONB) and SQLite (TEXT with JSON serialization)."""

    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import JSONB
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(Text())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value  # PostgreSQL handles JSONB natively
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value  # Already parsed by psycopg
        if isinstance(value, str):
            return json.loads(value)
        return value


# ============================================================================
# Models
# ============================================================================

class AIConversation(Base):
    """Persistent AI conversation record."""

    __tablename__ = "ai_conversations"
    __table_args__ = (
        Index("idx_ai_conversations_tenant_user", "tenant_id", "user_id"),
        Index("idx_ai_conversations_user_last_message", "user_id", "last_message_at"),
        {"schema": "common"},
    )

    id: Mapped[UUID] = mapped_column(PortableUUID(), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PortableUUID(), ForeignKey("common.tenants.id"), nullable=False)
    user_id: Mapped[UUID] = mapped_column(PortableUUID(), ForeignKey("common.users.id"), nullable=False)
    agent_type: Mapped[Optional[str]] = mapped_column(String(50))
    title: Mapped[Optional[str]] = mapped_column(String(255))
    status: Mapped[Optional[str]] = mapped_column(String(20), default="active")
    message_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    prompt_tokens: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    extra_data: Mapped[Optional[dict]] = mapped_column("metadata", PortableJSONB(), default=dict)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    last_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    messages: Mapped[List["AIMessage"]] = relationship(
        "AIMessage", back_populates="conversation", cascade="all, delete-orphan",
        order_by="AIMessage.created_at",
    )


class AIMessage(Base):
    """Individual message within an AI conversation."""

    __tablename__ = "ai_messages"
    __table_args__ = (
        Index("idx_ai_messages_conversation_created", "conversation_id", "created_at"),
        {"schema": "common"},
    )

    id: Mapped[UUID] = mapped_column(PortableUUID(), primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(
        PortableUUID(), ForeignKey("common.ai_conversations.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_tokens: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    contained_phi: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    phi_was_masked: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    phi_types: Mapped[Optional[list]] = mapped_column(PortableJSONB(), default=list)
    actions: Mapped[Optional[list]] = mapped_column(PortableJSONB(), default=list)
    extra_data: Mapped[Optional[dict]] = mapped_column("metadata", PortableJSONB(), default=dict)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    conversation: Mapped["AIConversation"] = relationship("AIConversation", back_populates="messages")


class AILearnedPattern(Base):
    """Long-term learned pattern from AI interactions."""

    __tablename__ = "ai_learned_patterns"
    __table_args__ = (
        Index("idx_ai_learned_patterns_tenant", "tenant_id", "is_active"),
        Index("idx_ai_learned_patterns_type", "pattern_type", "agent_type"),
        {"schema": "common"},
    )

    id: Mapped[UUID] = mapped_column(PortableUUID(), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PortableUUID(), nullable=False)
    pattern_type: Mapped[str] = mapped_column(String(50), nullable=False)
    agent_type: Mapped[Optional[str]] = mapped_column(String(50))
    trigger_context: Mapped[Optional[str]] = mapped_column(Text)
    learned_content: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    confidence: Mapped[Optional[float]] = mapped_column(Float, default=1.0)
    usage_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    conversation_id: Mapped[Optional[UUID]] = mapped_column(PortableUUID())
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)

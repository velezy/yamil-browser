"""
SQLAlchemy models for Gateway persistence.

These models persist gateway routes and consumers to the database,
making them survive container restarts. The database serves as
the source of truth, with routes synced to APIsix when not in
simulation mode.
"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import (
    String, Integer, Boolean, Text, DateTime, ForeignKey, BigInteger
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from assemblyline_common.models.common import Base


class GatewayRoute(Base):
    """
    Persisted API Gateway route for published Logic Weaver flows.

    Routes are stored in the database first, then synced to APIsix
    when not in simulation mode. This ensures routes survive
    container restarts.
    """

    __tablename__ = "gateway_routes"
    __table_args__ = {"schema": "common"}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # APIsix route identification
    route_id: Mapped[str] = mapped_column(String(100), nullable=False)  # APIsix route ID (lw-{flow_id})
    flow_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.logic_weaver_flows.id", ondelete="CASCADE"), nullable=False)
    flow_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Route configuration
    path: Mapped[str] = mapped_column(String(500), nullable=False)
    methods: Mapped[list] = mapped_column(JSONB, default=lambda: ["POST"], nullable=False)
    upstream_url: Mapped[str] = mapped_column(String(500), nullable=False)

    # Security
    security_policy: Mapped[str] = mapped_column(String(50), default="api_key", nullable=False)  # public, api_key, jwt, api_key_and_jwt

    # Rate limiting
    rate_limit_tier: Mapped[str] = mapped_column(String(20), default="standard", nullable=False)  # standard, premium, unlimited, custom
    rate_limit_requests: Mapped[Optional[int]] = mapped_column(Integer)
    rate_limit_window: Mapped[int] = mapped_column(Integer, default=60)

    # Plugin configuration
    plugins_config: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sync_status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, synced, sync_failed
    sync_error: Mapped[Optional[str]] = mapped_column(Text)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Audit
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deleted_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")  # type: ignore
    flow: Mapped["LogicWeaverFlow"] = relationship("LogicWeaverFlow")  # type: ignore
    route_access: Mapped[List["GatewayRouteAccess"]] = relationship("GatewayRouteAccess", back_populates="route", cascade="all, delete-orphan")


class GatewayConsumer(Base):
    """
    API consumer with API key for gateway authentication.

    Consumers are stored in the database and optionally synced
    to APIsix for key-auth plugin verification.
    """

    __tablename__ = "gateway_consumers"
    __table_args__ = {"schema": "common"}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Consumer identification
    username: Mapped[str] = mapped_column(String(100), nullable=False)
    apisix_username: Mapped[Optional[str]] = mapped_column(String(100))  # APIsix-compatible username (hyphens replaced with underscores)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # API Key (stored securely)
    api_key_prefix: Mapped[str] = mapped_column(String(20), nullable=False)  # First 8 chars for display
    api_key_hash: Mapped[str] = mapped_column(String(64), nullable=False)    # SHA-256 hash
    api_key_encrypted: Mapped[Optional[str]] = mapped_column(Text)           # Encrypted full key

    # Multi-auth support
    auth_type: Mapped[str] = mapped_column(String(50), default="key-auth", nullable=False)
    credentials: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Rate limiting
    rate_limit_requests: Mapped[Optional[int]] = mapped_column(Integer)
    rate_limit_window: Mapped[int] = mapped_column(Integer, default=60)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sync_status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, synced, sync_failed
    sync_error: Mapped[Optional[str]] = mapped_column(Text)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Usage tracking
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    usage_count: Mapped[int] = mapped_column(BigInteger, default=0)

    # Audit
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deleted_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")  # type: ignore
    route_access: Mapped[List["GatewayRouteAccess"]] = relationship("GatewayRouteAccess", back_populates="consumer", cascade="all, delete-orphan")


class GatewayRouteAccess(Base):
    """
    Per-route access control for consumers.

    Optionally restricts which consumers can access specific routes.
    If no access records exist for a route, all consumers can access it.
    """

    __tablename__ = "gateway_route_access"
    __table_args__ = {"schema": "common"}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    route_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.gateway_routes.id", ondelete="CASCADE"), nullable=False)
    consumer_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.gateway_consumers.id", ondelete="CASCADE"), nullable=False)

    # Access level
    can_access: Mapped[bool] = mapped_column(Boolean, default=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    route: Mapped["GatewayRoute"] = relationship("GatewayRoute", back_populates="route_access")
    consumer: Mapped["GatewayConsumer"] = relationship("GatewayConsumer", back_populates="route_access")

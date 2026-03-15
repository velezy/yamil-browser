"""
SQLAlchemy models for the common schema.

These models are shared across all tenants.
"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import (
    String, Integer, BigInteger, Boolean, Text, DateTime, ForeignKey,
    Index, UniqueConstraint, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB, INET, ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class Role(Base):
    """Application role definition stored in the database."""

    __tablename__ = "roles"
    __table_args__ = (
        Index("idx_roles_display_order", "display_order"),
        Index("idx_roles_active", "is_active"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="")
    display_order: Mapped[int] = mapped_column(Integer, default=0)
    requires_super_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    requires_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Tenant(Base):
    """Multi-tenant organization."""
    
    __tablename__ = "tenants"
    __table_args__ = {"schema": "common"}
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    schema_name: Mapped[str] = mapped_column(String(63), unique=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    tier: Mapped[str] = mapped_column(String(20), default="standard")  # free, standard, enterprise
    
    # Rate limiting
    rate_limit_requests: Mapped[int] = mapped_column(Integer, default=1000)
    rate_limit_window_seconds: Mapped[int] = mapped_column(Integer, default=3600)
    
    # MFA enforcement policy: off = no MFA required, optional = user can enable, required = must enroll
    mfa_policy: Mapped[str] = mapped_column(String(20), default="optional")

    # Configuration
    config: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deleted_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Relationships
    api_keys: Mapped[List["APIKey"]] = relationship("APIKey", back_populates="tenant")
    users: Mapped[List["User"]] = relationship("User", back_populates="tenant", primaryjoin="Tenant.id == User.tenant_id")


class User(Base):
    """User account within a tenant."""

    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="users_tenant_id_email_key"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))

    role: Mapped[str] = mapped_column(String(50), default="user")  # admin, user, viewer
    permissions: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False)

    # MFA fields
    mfa_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    mfa_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text)
    mfa_backup_codes_encrypted: Mapped[Optional[str]] = mapped_column(Text)
    mfa_enrolled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Account lockout (for brute force protection persistence)
    failed_login_attempts: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Manual lock (separate from brute force)
    status: Mapped[str] = mapped_column(String(20), default="active")  # pending, active, locked, disabled
    locked_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    locked_reason: Mapped[Optional[str]] = mapped_column(Text)
    unlock_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # Auto-unlock time

    # Password management
    must_change_password: Mapped[bool] = mapped_column(Boolean, default=False)
    password_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_password_change: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Registration tracking
    registration_source: Mapped[str] = mapped_column(String(50), default="admin")  # admin, self, invite, ldap, saml, scim, system

    # SAML/SSO
    saml_provider_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.saml_identity_providers.id"))
    external_id: Mapped[Optional[str]] = mapped_column(String(255))  # External user ID from SAML IdP

    # OIDC/SSO
    oidc_provider_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.oidc_identity_providers.id"))

    # LDAP/AD
    ldap_provider_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.ldap_providers.id"))

    # Timestamps
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # User metadata (preferences, settings)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSONB, default=dict)

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="users", primaryjoin="User.tenant_id == Tenant.id")
    saml_provider: Mapped[Optional["SAMLIdentityProvider"]] = relationship(
        "SAMLIdentityProvider",
        back_populates="users",
        foreign_keys=[saml_provider_id],
    )
    oidc_provider: Mapped[Optional["OIDCIdentityProvider"]] = relationship(
        "OIDCIdentityProvider",
        back_populates="users",
        foreign_keys=[oidc_provider_id],
    )


class UserInvitation(Base):
    """Pending user invitation."""

    __tablename__ = "user_invitations"
    __table_args__ = {"schema": "common"}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    invited_by_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"), nullable=False)

    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(50), default="user")
    token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    # Status: pending, accepted, expired, revoked
    status: Mapped[str] = mapped_column(String(20), default="pending")

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    accepted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Created user (after acceptance)
    user_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")
    invited_by: Mapped["User"] = relationship("User", foreign_keys=[invited_by_id])
    user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[user_id])


class UserRegistrationRequest(Base):
    """Self-registration request pending admin approval."""

    __tablename__ = "user_registration_requests"
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="user_registration_requests_tenant_email_key"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Applicant information
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))
    organization: Mapped[Optional[str]] = mapped_column(String(255))
    justification: Mapped[Optional[str]] = mapped_column(Text)
    requested_role: Mapped[str] = mapped_column(String(50), default="user")

    # Status tracking
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, approved, rejected
    reviewed_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    review_notes: Mapped[Optional[str]] = mapped_column(Text)

    # Metadata for security/audit
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    user_agent: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")
    reviewer: Mapped[Optional["User"]] = relationship("User", foreign_keys=[reviewed_by])


class APIKey(Base):
    """API key for programmatic access."""
    
    __tablename__ = "api_keys"
    __table_args__ = {"schema": "common"}
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # mw_live_xxxx
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)  # HMAC-SHA256 hash
    key_salt: Mapped[Optional[str]] = mapped_column(String(32))  # Salt for HMAC-SHA256 (NULL for legacy keys)

    environment: Mapped[str] = mapped_column(String(20), default="live")  # live, test, dev
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Permissions
    scopes: Mapped[list] = mapped_column(JSONB, default=list)  # ["read", "write", "admin"]

    # IP restrictions
    ip_allowlist: Mapped[list] = mapped_column(JSONB, default=list)  # ["192.168.1.0/24", "10.0.0.0/8"]

    # Rate limiting (can override tenant defaults)
    rate_limit_requests: Mapped[Optional[int]] = mapped_column(Integer)
    rate_limit_window_seconds: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Usage tracking
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    usage_count: Mapped[int] = mapped_column(Integer, default=0)
    
    # Expiration
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Revocation tracking
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    revoked_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    rotated_from_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.api_keys.id"))

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="api_keys")
    rotated_from: Mapped[Optional["APIKey"]] = relationship("APIKey", remote_side=[id], foreign_keys=[rotated_from_id])


class OAuthToken(Base):
    """OAuth2 tokens for external integrations."""
    
    __tablename__ = "oauth_tokens"
    __table_args__ = {"schema": "common"}
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    
    provider: Mapped[str] = mapped_column(String(50), nullable=False)  # epic, azure_ad, auth0
    provider_user_id: Mapped[Optional[str]] = mapped_column(String(255))
    
    access_token: Mapped[str] = mapped_column(Text, nullable=False)  # Encrypted
    refresh_token: Mapped[Optional[str]] = mapped_column(Text)  # Encrypted
    token_type: Mapped[str] = mapped_column(String(50), default="Bearer")
    
    scopes: Mapped[list] = mapped_column(JSONB, default=list)
    
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    refresh_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Extra data
    extra_data: Mapped[dict] = mapped_column(JSONB, default=dict)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class LogicWeaverFlow(Base):
    """Data transformation flow definition."""
    
    __tablename__ = "logic_weaver_flows"
    __table_args__ = {"schema": "common"}
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, default=1)
    category: Mapped[str] = mapped_column(String(100), default="general")
    
    # Flow definition (React Flow nodes/edges as JSON)
    flow_definition: Mapped[dict] = mapped_column(JSONB, default=lambda: {"nodes": [], "edges": []})
    
    # Trigger configuration
    trigger_type: Mapped[str] = mapped_column(String(50), default="api")  # api, kafka, schedule, mllp
    trigger_config: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Test payload - persisted sample input for testing the flow
    test_payload: Mapped[Optional[str]] = mapped_column(Text)
    test_payload_format: Mapped[Optional[str]] = mapped_column(String(20))  # json, hl7, xml, yaml, csv, x12, text

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    is_published: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Tags and metadata
    tags: Mapped[list] = mapped_column(JSONB, default=list)
    error_handling: Mapped[dict] = mapped_column(JSONB, default=dict)
    
    # Statistics
    execution_count: Mapped[int] = mapped_column(Integer, default=0)
    last_execution_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    avg_execution_ms: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    last_edited_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deleted_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Flow locking (pessimistic lock for concurrent editing prevention)
    locked_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    locked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    lock_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    lock_session_id: Mapped[Optional[str]] = mapped_column(String(100))

    # Collaboration mode: 'concurrent' (Git-like merge, default) or 'locked' (single editor)
    collaboration_mode: Mapped[str] = mapped_column(String(20), default="concurrent")

    # Per-flow policy configuration (Enterprise API Gateway)
    # Supports: rate_limit, spike_arrest, required_scopes, ip_filter,
    # transformations, circuit_breaker, api_version, audit, timeout_ms, retry, caching
    policies: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Policy bundle reference (V031) - bundle policies + flow.policies merged at runtime
    policy_bundle_id: Mapped[Optional[UUID]] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("common.policy_bundles.id", ondelete="SET NULL"),
        nullable=True
    )


class FlowExecution(Base):
    """Execution history for LogicWeaver flows."""
    
    __tablename__ = "flow_executions"
    __table_args__ = (
        Index("idx_flow_executions_flow", "flow_id"),
        Index("idx_flow_executions_tenant", "tenant_id"),
        Index("idx_flow_executions_status", "status"),
        Index("idx_flow_executions_correlation", "correlation_id"),
        Index("idx_flow_executions_started", "started_at"),
        {"schema": "common"}
    )
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    flow_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.logic_weaver_flows.id"), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    
    # Execution details
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")  # pending, running, completed, failed
    correlation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), default=uuid4)
    
    # Input/Output
    input_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    output_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Steps executed
    steps_executed: Mapped[int] = mapped_column(Integer, default=0)
    step_logs: Mapped[list] = mapped_column(JSONB, default=list)
    
    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    error_step_id: Mapped[Optional[str]] = mapped_column(String(100))
    error_details: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # Triggered by
    triggered_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    trigger_source: Mapped[str] = mapped_column(String(50), default="manual")  # manual, api, schedule, kafka


class FlowVersion(Base):
    """Version history for LogicWeaver flows."""
    
    __tablename__ = "flow_versions"
    __table_args__ = (
        Index("idx_flow_versions_flow", "flow_id"),
        {"schema": "common"}
    )
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    flow_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.logic_weaver_flows.id"), nullable=False)
    
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Snapshot of flow at this version
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    flow_definition: Mapped[dict] = mapped_column(JSONB, nullable=False)
    trigger_type: Mapped[Optional[str]] = mapped_column(String(50))
    trigger_config: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # Change tracking
    changed_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    change_message: Mapped[Optional[str]] = mapped_column(Text)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AuditTrail(Base):
    """
    Compliance audit trail for PHI access with immutable hash chain.

    Each entry contains a cryptographic hash linking it to the previous entry,
    creating a tamper-evident chain similar to a blockchain. Any modification
    to historical entries will break the hash chain and be detected during
    verification.

    Hash computation: SHA-256(prev_hash + timestamp + tenant_id + sequence + data)
    """

    __tablename__ = "audit_trail"
    __table_args__ = (
        Index("idx_audit_tenant_created", "tenant_id", "created_at"),
        Index("idx_audit_tenant_sequence", "tenant_id", "sequence_number"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Hash chain fields for immutability
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    prev_hash: Mapped[str] = mapped_column(String(64), nullable=False, default="0" * 64)  # SHA-256 hex
    entry_hash: Mapped[str] = mapped_column(String(64), nullable=False, default="")  # SHA-256 hex

    # Actor
    user_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    api_key_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.api_keys.id"))
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    user_agent: Mapped[Optional[str]] = mapped_column(String(500))

    # Action
    action: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    resource_type: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_id: Mapped[Optional[str]] = mapped_column(String(255))

    # PHI flags
    contains_phi: Mapped[bool] = mapped_column(Boolean, default=False)
    phi_types: Mapped[list] = mapped_column(JSONB, default=list)  # ["name", "mrn", "dob"]

    # Details
    details: Mapped[dict] = mapped_column(JSONB, default=dict)
    encrypted_details: Mapped[Optional[str]] = mapped_column(Text)  # AES-256-GCM encrypted JSON when contains_phi=True
    correlation_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), index=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class APICallLog(Base):
    """Log of all inbound API calls."""
    
    __tablename__ = "api_call_log"
    __table_args__ = (
        Index("idx_api_call_tenant_created", "tenant_id", "created_at"),
        Index("idx_api_call_correlation", "correlation_id"),
        {"schema": "common"}
    )
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"))
    correlation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, default=uuid4)
    
    # Request
    method: Mapped[str] = mapped_column(String(10), nullable=False)
    path: Mapped[str] = mapped_column(String(2000), nullable=False)
    query_params: Mapped[dict] = mapped_column(JSONB, default=dict)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)  # Sanitized
    request_body: Mapped[Optional[str]] = mapped_column(Text)
    
    # Response
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[Optional[str]] = mapped_column(Text)
    
    # Metadata
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    user_agent: Mapped[Optional[str]] = mapped_column(String(500))
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # PHI flag
    contains_phi: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Authentication
    auth_type: Mapped[Optional[str]] = mapped_column(String(50))  # api_key, jwt, oauth
    user_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True))
    api_key_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True))
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OutboundAPICallLog(Base):
    """Log of all outbound API calls to external systems."""
    
    __tablename__ = "outbound_api_call_log"
    __table_args__ = (
        Index("idx_outbound_api_tenant_created", "tenant_id", "created_at"),
        Index("idx_outbound_api_correlation", "correlation_id"),
        {"schema": "common"}
    )
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    
    # Destination
    destination: Mapped[str] = mapped_column(String(100), nullable=False)  # epic, databricks, webhook
    destination_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    
    # Request
    method: Mapped[str] = mapped_column(String(10), nullable=False)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)  # Sanitized
    request_body: Mapped[Optional[str]] = mapped_column(Text)
    
    # Response
    status_code: Mapped[Optional[int]] = mapped_column(Integer)
    response_body: Mapped[Optional[str]] = mapped_column(Text)
    response_headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    
    # Timing
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Retry tracking
    retry_attempt: Mapped[int] = mapped_column(Integer, default=0)
    is_success: Mapped[bool] = mapped_column(Boolean, default=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class KafkaMessageLog(Base):
    """Log of all Kafka messages produced and consumed."""
    
    __tablename__ = "kafka_message_log"
    __table_args__ = (
        Index("idx_kafka_tenant_created", "tenant_id", "created_at"),
        Index("idx_kafka_correlation", "correlation_id"),
        {"schema": "common"}
    )
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    
    # Kafka metadata
    topic: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    partition: Mapped[Optional[int]] = mapped_column(Integer)
    offset: Mapped[Optional[int]] = mapped_column(Integer)
    key: Mapped[Optional[str]] = mapped_column(String(500))
    
    # Message
    message_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    
    # Direction
    direction: Mapped[str] = mapped_column(String(20), nullable=False)  # produced, consumed
    
    # Processing status
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, processed, failed
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Connector(Base):
    """Reusable connector configurations for Logic Weaver flows."""
    
    __tablename__ = "connectors"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_connectors_tenant_name"),
        Index("idx_connectors_tenant", "tenant_id"),
        Index("idx_connectors_type", "connector_type"),
        Index("idx_connectors_category", "category"),
        {"schema": "common"}
    )
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    
    # Connector identity
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(255))  # Custom display name shown on nodes
    description: Mapped[Optional[str]] = mapped_column(Text)
    
    # Connector type (s3-read, s3-write, http-output, kafka-producer, mllp-output, email-send, etc.)
    connector_type: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(100), default="general")  # Connectors, Output, Input, etc.
    
    # Configuration stored as JSONB
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    
    # Security - sensitive fields are encrypted
    encrypted_fields: Mapped[list] = mapped_column(JSONB, default=list)  # List of field names that are encrypted
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_shared: Mapped[bool] = mapped_column(Boolean, default=False)  # Can be used by all flows in tenant
    connection_status: Mapped[Optional[str]] = mapped_column(String(20), default="disconnected")  # connected, disconnected, error, unknown
    last_connected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Validation
    last_tested_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_test_status: Mapped[Optional[str]] = mapped_column(String(20))  # success, failed, pending
    last_test_error: Mapped[Optional[str]] = mapped_column(Text)
    
    # Metadata
    icon: Mapped[Optional[str]] = mapped_column(String(10))  # Emoji or icon identifier
    color: Mapped[Optional[str]] = mapped_column(String(20))  # Hex color for UI
    tags: Mapped[list] = mapped_column(JSONB, default=list)
    
    # Audit fields
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deleted_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))


class EmailTemplate(Base):
    """Reusable email templates selectable from the Email Send node."""

    __tablename__ = "email_templates"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_email_templates_tenant_name"),
        Index("idx_email_templates_tenant", "tenant_id"),
        Index("idx_email_templates_category", "category"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(255))
    description: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[str] = mapped_column(String(100), default="general")
    subject: Mapped[str] = mapped_column(String(500), default="")
    body_type: Mapped[str] = mapped_column(String(10), default="html")  # html, text
    body: Mapped[str] = mapped_column(Text, default="")
    default_from: Mapped[Optional[str]] = mapped_column(String(255))
    default_cc: Mapped[Optional[str]] = mapped_column(String(500))
    variables: Mapped[list] = mapped_column(JSONB, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_shared: Mapped[bool] = mapped_column(Boolean, default=True)
    tags: Mapped[list] = mapped_column(JSONB, default=list)

    # Audit fields
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class ConnectorUsage(Base):
    """Tracks which flows use which connectors for impact analysis."""

    __tablename__ = "connector_usage"
    __table_args__ = (
        UniqueConstraint("connector_id", "flow_id", "node_id", name="uq_connector_usage"),
        Index("idx_connector_usage_connector", "connector_id"),
        Index("idx_connector_usage_flow", "flow_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    connector_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.connectors.id"), nullable=False)
    flow_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.logic_weaver_flows.id"), nullable=False)
    node_id: Mapped[str] = mapped_column(String(100), nullable=False)  # The node ID within the flow that uses this connector

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SAMLIdentityProvider(Base):
    """SAML 2.0 Identity Provider configuration for SSO."""

    __tablename__ = "saml_identity_providers"
    __table_args__ = (
        UniqueConstraint("tenant_id", "entity_id", name="uq_saml_provider_tenant_entity"),
        Index("idx_saml_providers_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Provider identity
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_type: Mapped[str] = mapped_column(String(50), nullable=False)  # azure_ad, okta, generic
    entity_id: Mapped[str] = mapped_column(String(500), nullable=False)  # IdP Entity ID

    # SAML endpoints
    sso_url: Mapped[str] = mapped_column(String(500), nullable=False)  # IdP SSO URL
    slo_url: Mapped[Optional[str]] = mapped_column(String(500))  # IdP SLO URL (optional)

    # Certificates (encrypted)
    x509_certificate_encrypted: Mapped[str] = mapped_column(Text, nullable=False)  # IdP's public certificate
    sp_private_key_encrypted: Mapped[Optional[str]] = mapped_column(Text)  # SP's private key for signing

    # Attribute mapping (how to extract user info from SAML assertions)
    attribute_mapping: Mapped[dict] = mapped_column(JSONB, default=lambda: {
        "email": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
        "first_name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
        "last_name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname",
        "groups": "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups"
    })

    # Configuration
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    auto_provision_users: Mapped[bool] = mapped_column(Boolean, default=True)  # Create users on first login
    default_role: Mapped[str] = mapped_column(String(50), default="user")  # Role for auto-provisioned users
    allowed_domains: Mapped[list] = mapped_column(JSONB, default=list)  # Restrict to specific email domains
    group_role_mapping: Mapped[dict] = mapped_column(JSONB, default=dict)  # Map IdP groups to roles

    # SP metadata
    sp_entity_id: Mapped[Optional[str]] = mapped_column(String(500))  # Our Entity ID
    sp_acs_url: Mapped[Optional[str]] = mapped_column(String(500))  # Assertion Consumer Service URL

    # Audit fields
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")
    users: Mapped[List["User"]] = relationship(
        "User",
        back_populates="saml_provider",
        foreign_keys="[User.saml_provider_id]",
    )


class OIDCIdentityProvider(Base):
    """OpenID Connect Identity Provider configuration for SSO (Entra ID, Okta, Auth0, etc.)."""

    __tablename__ = "oidc_identity_providers"
    __table_args__ = (
        UniqueConstraint("tenant_id", "issuer_url", name="uq_oidc_provider_tenant_issuer"),
        Index("idx_oidc_providers_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Provider identity
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_type: Mapped[str] = mapped_column(String(50), nullable=False, default="entra_id")  # entra_id, okta, auth0, keycloak, generic

    # OIDC Discovery
    issuer_url: Mapped[str] = mapped_column(String(500), nullable=False)
    discovery_url: Mapped[Optional[str]] = mapped_column(String(500))

    # Client credentials (encrypted)
    client_id: Mapped[str] = mapped_column(String(255), nullable=False)
    client_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text)

    # Endpoints (auto-populated from discovery, overridable)
    authorization_endpoint: Mapped[Optional[str]] = mapped_column(String(500))
    token_endpoint: Mapped[Optional[str]] = mapped_column(String(500))
    userinfo_endpoint: Mapped[Optional[str]] = mapped_column(String(500))
    jwks_uri: Mapped[Optional[str]] = mapped_column(String(500))
    end_session_endpoint: Mapped[Optional[str]] = mapped_column(String(500))

    # Configuration
    scopes: Mapped[str] = mapped_column(String(500), default="openid profile email")
    response_type: Mapped[str] = mapped_column(String(50), default="code")
    use_pkce: Mapped[bool] = mapped_column(Boolean, default=True)

    # Claim mapping
    claim_mapping: Mapped[dict] = mapped_column(JSONB, default=lambda: {
        "email": "email",
        "first_name": "given_name",
        "last_name": "family_name",
        "groups": "groups"
    })

    # User provisioning
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    auto_provision_users: Mapped[bool] = mapped_column(Boolean, default=True)
    default_role: Mapped[str] = mapped_column(String(50), default="user")
    allowed_domains: Mapped[list] = mapped_column(JSONB, default=list)
    group_role_mapping: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Audit fields
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")
    users: Mapped[List["User"]] = relationship(
        "User",
        back_populates="oidc_provider",
        foreign_keys="[User.oidc_provider_id]",
    )


class LDAPProvider(Base):
    """LDAP / Active Directory provider configuration for SSO."""

    __tablename__ = "ldap_providers"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_ldap_provider_tenant_name"),
        Index("idx_ldap_providers_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Provider identity
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_type: Mapped[str] = mapped_column(String(50), default="active_directory")  # active_directory, openldap, generic

    # Connection settings
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=389)
    use_ssl: Mapped[bool] = mapped_column(Boolean, default=False)
    use_starttls: Mapped[bool] = mapped_column(Boolean, default=False)

    # Bind credentials (encrypted)
    bind_dn: Mapped[Optional[str]] = mapped_column(Text)
    bind_password_encrypted: Mapped[Optional[str]] = mapped_column(Text)

    # Search settings
    base_dn: Mapped[str] = mapped_column(Text, nullable=False)
    user_search_filter: Mapped[str] = mapped_column(Text, default="(sAMAccountName={username})")
    user_dn_pattern: Mapped[Optional[str]] = mapped_column(Text)

    # Attribute mapping
    attribute_mapping: Mapped[dict] = mapped_column(JSONB, default=lambda: {
        "email": "mail",
        "first_name": "givenName",
        "last_name": "sn",
        "display_name": "displayName"
    })

    # Group settings
    group_search_base: Mapped[Optional[str]] = mapped_column(Text)
    group_search_filter: Mapped[Optional[str]] = mapped_column(Text)
    group_role_mapping: Mapped[dict] = mapped_column(JSONB, default=dict)

    # User provisioning
    auto_provision_users: Mapped[bool] = mapped_column(Boolean, default=True)
    default_role: Mapped[str] = mapped_column(String(50), default="user")
    allowed_domains: Mapped[list] = mapped_column(ARRAY(String), default=list)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Audit fields
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")


# ============================================================================
# API Builder Models
# ============================================================================

class ApiCollection(Base):
    """Collection of API requests for API Builder."""

    __tablename__ = "api_collections"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_api_collections_tenant_name"),
        Index("idx_api_collections_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Collection-level variables (e.g., {{base_url}})
    variables: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))

    # Relationships
    requests: Mapped[List["ApiRequest"]] = relationship("ApiRequest", back_populates="collection", cascade="all, delete-orphan")


class ApiRequest(Base):
    """Individual API request within a collection."""

    __tablename__ = "api_requests"
    __table_args__ = (
        Index("idx_api_requests_collection", "collection_id"),
        Index("idx_api_requests_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    collection_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.api_collections.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    method: Mapped[str] = mapped_column(String(10), nullable=False, default="GET")  # GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS
    url: Mapped[str] = mapped_column(String(2000), nullable=False, default="")

    # Request configuration
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    query_params: Mapped[dict] = mapped_column(JSONB, default=dict)
    body: Mapped[Optional[str]] = mapped_column(Text)
    body_type: Mapped[str] = mapped_column(String(30), default="none")  # none, json, form-data, x-www-form-urlencoded, raw, binary

    # Authentication
    auth_type: Mapped[str] = mapped_column(String(30), default="none")  # none, basic, bearer, api-key, oauth2, aws-sig
    auth_credentials_encrypted: Mapped[Optional[str]] = mapped_column(Text)  # Encrypted JSON

    # Direction (inbound = receive, outbound = send)
    direction: Mapped[str] = mapped_column(String(20), default="outbound")

    # Environment (for variable substitution)
    environment: Mapped[str] = mapped_column(String(20), default="DEV")  # DEV, STAGING, PROD

    # Test status
    test_status: Mapped[str] = mapped_column(String(20), default="untested")  # untested, success, failed
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_status_code: Mapped[Optional[int]] = mapped_column(Integer)

    # Folder for organization
    folder: Mapped[Optional[str]] = mapped_column(String(255))

    # Documentation (Phase 4.1)
    description: Mapped[Optional[str]] = mapped_column(Text)  # Markdown description
    example_responses: Mapped[Optional[list]] = mapped_column(JSONB)  # [{"name": "Success", "status": 200, "body": {...}}, ...]

    # Mock Server (Phase 4.2)
    mock_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    mock_response: Mapped[Optional[dict]] = mapped_column(JSONB)  # {"status": 200, "headers": {}, "body": "", "delay_ms": 0}
    mock_error_rate: Mapped[int] = mapped_column(Integer, default=0)  # 0-100%
    mock_error_status: Mapped[int] = mapped_column(Integer, default=500)

    # Test Assertions (Phase 5.1)
    assertions: Mapped[list] = mapped_column(JSONB, default=list)  # [{"type": "status_code", "operator": "eq", "value": 200}, ...]

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    collection: Mapped["ApiCollection"] = relationship("ApiCollection", back_populates="requests")


class ApiTestResult(Base):
    """Test execution result for API requests."""

    __tablename__ = "api_test_results"
    __table_args__ = (
        Index("idx_api_test_results_request", "request_id"),
        Index("idx_api_test_results_collection", "collection_id"),
        Index("idx_api_test_results_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    request_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.api_requests.id", ondelete="CASCADE"), nullable=False)
    collection_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.api_collections.id", ondelete="CASCADE"), nullable=False)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")  # pending, passed, failed
    response_status: Mapped[Optional[int]] = mapped_column(Integer)
    response_time_ms: Mapped[Optional[int]] = mapped_column(Integer)
    response_size: Mapped[Optional[int]] = mapped_column(Integer)
    response_headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    response_body: Mapped[Optional[str]] = mapped_column(Text)
    assertions_results: Mapped[list] = mapped_column(JSONB, default=list)  # [{"type": "...", "passed": true, "message": "..."}, ...]

    environment: Mapped[str] = mapped_column(String(20), default="DEV")
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))


class SftpConnection(Base):
    """SFTP connection configuration for API Builder."""

    __tablename__ = "sftp_connections"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_sftp_connections_tenant_name"),
        Index("idx_sftp_connections_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=22)
    username: Mapped[str] = mapped_column(String(255), nullable=False)

    # Authentication (password or key stored encrypted)
    auth_type: Mapped[str] = mapped_column(String(30), default="password")  # password, key, key-passphrase
    password_encrypted: Mapped[Optional[str]] = mapped_column(Text)
    private_key_encrypted: Mapped[Optional[str]] = mapped_column(Text)
    passphrase_encrypted: Mapped[Optional[str]] = mapped_column(Text)

    # Paths
    remote_path: Mapped[str] = mapped_column(String(500), default="/")
    local_path: Mapped[Optional[str]] = mapped_column(String(500))

    # Security
    encryption: Mapped[str] = mapped_column(String(20), default="none")  # none, aes-256, aes-128, 3des

    # PGP encryption/decryption
    pgp_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    pgp_public_key_encrypted: Mapped[Optional[str]] = mapped_column(Text)
    pgp_private_key_encrypted: Mapped[Optional[str]] = mapped_column(Text)

    # Direction (inbound = GET files, outbound = PUT files)
    direction: Mapped[str] = mapped_column(String(20), default="inbound")

    # Status
    status: Mapped[str] = mapped_column(String(20), default="disconnected")  # connected, disconnected, error
    test_status: Mapped[str] = mapped_column(String(20), default="untested")  # untested, success, failed
    last_connected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))


class WebSocketConnection(Base):
    """WebSocket connection for API Builder testing (Phase 4.3)."""

    __tablename__ = "websocket_connections"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_websocket_connections_tenant_name"),
        Index("idx_websocket_connections_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(2000), nullable=False)

    # Connection configuration
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    subprotocol: Mapped[Optional[str]] = mapped_column(String(100))

    # Connection state
    status: Mapped[str] = mapped_column(String(20), default="disconnected")  # disconnected, connecting, connected, error
    last_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Message history (last N messages stored)
    message_history: Mapped[Optional[list]] = mapped_column(JSONB)  # [{"type": "sent"|"received", "data": "...", "timestamp": "..."}]

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))


class GraphQLEndpoint(Base):
    """GraphQL endpoint for API Builder testing (Phase 4.4)."""

    __tablename__ = "graphql_endpoints"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_graphql_endpoints_tenant_name"),
        Index("idx_graphql_endpoints_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(2000), nullable=False)

    # Connection configuration
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Cached schema from introspection
    schema_cached: Mapped[Optional[dict]] = mapped_column(JSONB)
    schema_cached_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Saved queries
    saved_queries: Mapped[Optional[list]] = mapped_column(JSONB)  # [{"name": "...", "query": "...", "variables": {...}}]

    # Last used variables
    last_variables: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))


class ApiRequestHistory(Base):
    """Request execution history for API Builder."""

    __tablename__ = "api_request_history"
    __table_args__ = (
        Index("idx_api_request_history_tenant", "tenant_id"),
        Index("idx_api_request_history_request", "request_id"),
        Index("idx_api_request_history_executed", "tenant_id", "executed_at"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    request_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.api_requests.id", ondelete="SET NULL"))

    method: Mapped[str] = mapped_column(String(10), nullable=False)
    url: Mapped[str] = mapped_column(String(4000), nullable=False)
    request_headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    request_body: Mapped[Optional[str]] = mapped_column(Text)

    response_status: Mapped[Optional[int]] = mapped_column(Integer)
    response_status_text: Mapped[Optional[str]] = mapped_column(String(100))
    response_headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    response_body: Mapped[Optional[str]] = mapped_column(Text)
    response_time_ms: Mapped[Optional[int]] = mapped_column(Integer)
    response_size: Mapped[Optional[int]] = mapped_column(Integer)

    environment: Mapped[str] = mapped_column(String(20), default="DEV")
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    executed_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))


# ============================================================================
# AI Prompt Management Models
# ============================================================================

class AIPrompt(Base):
    """Configurable AI prompts for various integration tasks."""

    __tablename__ = "ai_prompts"
    __table_args__ = (
        UniqueConstraint("tenant_id", "key", "version", name="uq_ai_prompts_tenant_key_version"),
        Index("idx_ai_prompts_tenant", "tenant_id"),
        Index("idx_ai_prompts_category", "tenant_id", "category"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Prompt identification
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    key: Mapped[str] = mapped_column(String(100), nullable=False)  # Unique key for programmatic access
    category: Mapped[str] = mapped_column(String(100), nullable=False)  # flow-builder, data-transform, etc.
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Prompt content
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    user_prompt_template: Mapped[Optional[str]] = mapped_column(Text)  # Template with {{placeholders}}
    example_input: Mapped[dict] = mapped_column(JSONB, default=dict)
    example_output: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Model configuration
    model_id: Mapped[str] = mapped_column(String(100), default="us.anthropic.claude-3-5-sonnet-20241022-v2:0")
    temperature: Mapped[float] = mapped_column(default=0.7)
    max_tokens: Mapped[int] = mapped_column(Integer, default=4096)
    top_p: Mapped[float] = mapped_column(default=0.9)

    # Versioning
    version: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)

    # Guardrails
    guardrails: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    guardrail_template_key: Mapped[Optional[str]] = mapped_column(String(100), default="general-security")

    # Testing
    last_test_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_test_status: Mapped[Optional[str]] = mapped_column(String(20))
    last_test_result: Mapped[Optional[dict]] = mapped_column(JSONB)
    last_test_latency_ms: Mapped[Optional[int]] = mapped_column(Integer)

    # Audit
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tests: Mapped[List["AIPromptTest"]] = relationship("AIPromptTest", back_populates="prompt", cascade="all, delete-orphan")


class AIPromptTest(Base):
    """History of prompt tests against Bedrock AI."""

    __tablename__ = "ai_prompt_tests"
    __table_args__ = (
        Index("idx_ai_prompt_tests_prompt", "prompt_id", "tested_at"),
        Index("idx_ai_prompt_tests_tenant", "tenant_id", "tested_at"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    prompt_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.ai_prompts.id"), nullable=False)

    # Test input/output
    test_input: Mapped[dict] = mapped_column(JSONB, nullable=False)
    test_output: Mapped[Optional[dict]] = mapped_column(JSONB)
    raw_response: Mapped[Optional[str]] = mapped_column(Text)

    # Metrics
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # success, failed, error
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer)
    prompt_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    completion_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    total_tokens: Mapped[Optional[int]] = mapped_column(Integer)

    # Error details
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Audit
    tested_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    tested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    prompt: Mapped["AIPrompt"] = relationship("AIPrompt", back_populates="tests")


# ============================================================================
# Environment & Promotion Models
# ============================================================================

class Environment(Base):
    """Environment configuration for flow deployments."""

    __tablename__ = "environments"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_environment_tenant_name"),
        Index("idx_environments_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Environment identity
    name: Mapped[str] = mapped_column(String(50), nullable=False)  # development, testing, staging, production, sandbox
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Environment type
    env_type: Mapped[str] = mapped_column(String(20), nullable=False)  # development, testing, staging, production, sandbox
    base_url: Mapped[Optional[str]] = mapped_column(String(500))

    # Policies (rate limiting, quotas, security)
    policy: Mapped[dict] = mapped_column(JSONB, default=lambda: {
        "rate_limit_requests": 1000,
        "rate_limit_period_seconds": 60,
        "require_api_key": True,
        "require_oauth": False,
        "cache_enabled": True,
        "cache_ttl_seconds": 300,
        "log_level": "INFO"
    })

    # Variables and secrets for this environment
    variables: Mapped[dict] = mapped_column(JSONB, default=dict)
    secrets_encrypted: Mapped[dict] = mapped_column(JSONB, default=dict)  # Encrypted secret values

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Currently deployed flow versions
    deployed_flows: Mapped[dict] = mapped_column(JSONB, default=dict)  # {flow_id: version}

    # Audit
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")


class PromotionRule(Base):
    """Rules for promoting flows between environments."""

    __tablename__ = "promotion_rules"
    __table_args__ = (
        UniqueConstraint("tenant_id", "source_env", "target_env", name="uq_promotion_rule"),
        Index("idx_promotion_rules_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Source and target environments
    source_env: Mapped[str] = mapped_column(String(50), nullable=False)  # development, staging, etc.
    target_env: Mapped[str] = mapped_column(String(50), nullable=False)  # staging, production, etc.

    # Approval requirements
    approval_type: Mapped[str] = mapped_column(String(20), default="single")  # none, single, multi, automatic
    required_approvers: Mapped[int] = mapped_column(Integer, default=1)
    allowed_approvers: Mapped[list] = mapped_column(JSONB, default=list)  # List of user IDs or role names

    # Validation requirements
    require_tests_pass: Mapped[bool] = mapped_column(Boolean, default=True)
    require_no_critical_issues: Mapped[bool] = mapped_column(Boolean, default=True)
    require_documentation: Mapped[bool] = mapped_column(Boolean, default=False)

    # Automatic checks
    run_smoke_tests: Mapped[bool] = mapped_column(Boolean, default=True)
    run_integration_tests: Mapped[bool] = mapped_column(Boolean, default=False)
    run_security_scan: Mapped[bool] = mapped_column(Boolean, default=False)

    # Rollback settings
    auto_rollback_on_failure: Mapped[bool] = mapped_column(Boolean, default=True)
    rollback_timeout_seconds: Mapped[int] = mapped_column(Integer, default=300)

    # Custom validators (list of validator names)
    custom_validators: Mapped[list] = mapped_column(JSONB, default=list)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")


class PromotionRequest(Base):
    """Request to promote one or more flows between environments."""

    __tablename__ = "promotion_requests"
    __table_args__ = (
        Index("idx_promotion_requests_tenant", "tenant_id"),
        Index("idx_promotion_requests_status", "status"),
        Index("idx_promotion_requests_source", "source_env"),
        Index("idx_promotion_requests_target", "target_env"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Flows to promote (can be multiple)
    flow_ids: Mapped[list] = mapped_column(JSONB, nullable=False)  # List of flow UUIDs
    flow_versions: Mapped[dict] = mapped_column(JSONB, nullable=False)  # {flow_id: version}

    # Source and target
    source_env: Mapped[str] = mapped_column(String(50), nullable=False)
    target_env: Mapped[str] = mapped_column(String(50), nullable=False)

    # Status: pending, approved, rejected, in_progress, completed, failed, rolled_back
    status: Mapped[str] = mapped_column(String(20), default="pending")

    # Requester
    requested_by: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"), nullable=False)
    request_notes: Mapped[Optional[str]] = mapped_column(Text)

    # Validation results
    validation_results: Mapped[dict] = mapped_column(JSONB, default=dict)  # {check_name: passed}
    validation_messages: Mapped[dict] = mapped_column(JSONB, default=dict)  # {check_name: message}

    # Deployment info
    deployment_log: Mapped[list] = mapped_column(JSONB, default=list)  # List of log entries
    rollback_versions: Mapped[dict] = mapped_column(JSONB, default=dict)  # {flow_id: previous_version}

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")
    requester: Mapped["User"] = relationship("User", foreign_keys=[requested_by])
    approvals: Mapped[List["PromotionApproval"]] = relationship("PromotionApproval", back_populates="promotion_request")


class PromotionApproval(Base):
    """Approval or rejection for a promotion request."""

    __tablename__ = "promotion_approvals"
    __table_args__ = (
        UniqueConstraint("promotion_request_id", "approver_id", name="uq_promotion_approval"),
        Index("idx_promotion_approvals_request", "promotion_request_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    promotion_request_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.promotion_requests.id"), nullable=False)
    approver_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"), nullable=False)

    # Decision: approved, rejected
    decision: Mapped[str] = mapped_column(String(20), nullable=False)
    comments: Mapped[Optional[str]] = mapped_column(Text)

    # Timestamp
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    promotion_request: Mapped["PromotionRequest"] = relationship("PromotionRequest", back_populates="approvals")
    approver: Mapped["User"] = relationship("User")


# ============================================================================
# Cross-Tenant Flow Transfer Models
# ============================================================================

class FlowTransfer(Base):
    """Transfer one or more flows between tenants."""

    __tablename__ = "flow_transfers"
    __table_args__ = (
        Index("idx_flow_transfers_source_tenant", "source_tenant_id"),
        Index("idx_flow_transfers_target_tenant", "target_tenant_id"),
        Index("idx_flow_transfers_status", "status"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Source and target tenants
    source_tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    target_tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Flows to transfer
    flow_ids: Mapped[list] = mapped_column(JSONB, nullable=False)  # List of source flow UUIDs

    # Transfer options
    transfer_type: Mapped[str] = mapped_column(String(20), default="copy")  # copy, move
    include_connectors: Mapped[bool] = mapped_column(Boolean, default=False)  # Copy connector configs too
    include_variables: Mapped[bool] = mapped_column(Boolean, default=False)  # Copy environment variables
    include_versions: Mapped[bool] = mapped_column(Boolean, default=False)  # Copy version history

    # Connector mapping (if transferring to tenant with different connectors)
    connector_mapping: Mapped[dict] = mapped_column(JSONB, default=dict)  # {source_connector_id: target_connector_id}

    # Status: pending, approved, in_progress, completed, failed, cancelled
    status: Mapped[str] = mapped_column(String(20), default="pending")

    # Results
    transferred_flows: Mapped[dict] = mapped_column(JSONB, default=dict)  # {source_flow_id: new_flow_id}
    transfer_log: Mapped[list] = mapped_column(JSONB, default=list)  # List of log entries
    error_details: Mapped[Optional[str]] = mapped_column(Text)

    # Requester and approvals
    requested_by: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"), nullable=False)
    approved_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    request_notes: Mapped[Optional[str]] = mapped_column(Text)
    approval_notes: Mapped[Optional[str]] = mapped_column(Text)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Relationships
    source_tenant: Mapped["Tenant"] = relationship("Tenant", foreign_keys=[source_tenant_id])
    target_tenant: Mapped["Tenant"] = relationship("Tenant", foreign_keys=[target_tenant_id])
    requester: Mapped["User"] = relationship("User", foreign_keys=[requested_by])
    approver: Mapped["User"] = relationship("User", foreign_keys=[approved_by])


# ============================================================================
# Policy Bundle Models (V031)
# ============================================================================

class PolicyBundle(Base):
    """
    Reusable policy configurations that can be applied to multiple flows.

    Supports inheritance: Tenant defaults → Bundle → Flow overrides.
    Bundle policies are merged with flow-level policies at execution time.
    """

    __tablename__ = "policy_bundles"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_policy_bundles_tenant_name"),
        Index("idx_policy_bundles_tenant", "tenant_id"),
        Index("idx_policy_bundles_default", "tenant_id", "is_default"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Bundle identity
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # The actual policy configuration (same structure as flow.policies)
    policies: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Bundle metadata
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)  # Tenant's default bundle
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)  # System-provided (read-only)
    category: Mapped[Optional[str]] = mapped_column(String(100))  # security, performance, compliance
    tags: Mapped[List[str]] = mapped_column(ARRAY(Text), default=list)

    # AI recommendation matching rules (V045)
    matching_rules: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Usage tracking
    usage_count: Mapped[int] = mapped_column(Integer, default=0)

    # Audit
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Soft delete (V032)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    deleted_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"), nullable=True)

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")


# ============================================================================
# Policy Type Definition Models (V034)
# ============================================================================

class PolicyTypeDefinition(Base):
    """
    Defines available policy types for the Flow Builder.

    Each policy type has:
    - A unique key (e.g., "rate_limit", "geo_blocking")
    - A JSON schema for configuration validation
    - UI hints for rendering in the Flow Builder

    System policy types are read-only. Tenants can create custom policy types.
    """

    __tablename__ = "policy_type_definitions"
    __table_args__ = (
        UniqueConstraint("key", "tenant_id", name="policy_type_unique_key"),
        Index("idx_policy_type_definitions_tenant", "tenant_id"),
        Index("idx_policy_type_definitions_active", "is_active", "display_order"),
        Index("idx_policy_type_definitions_category", "category"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Policy type identity
    key: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "rate_limit", "geo_blocking"
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # Display name
    description: Mapped[Optional[str]] = mapped_column(Text)
    icon: Mapped[str] = mapped_column(String(50), default="Shield")  # Lucide icon name
    phase: Mapped[Optional[str]] = mapped_column(String(50))  # e.g., "Phase 1", "General"
    category: Mapped[str] = mapped_column(String(50), default="general")  # security, performance, compliance

    # Configuration schema and defaults
    config_schema: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    default_config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ui_config: Mapped[dict] = mapped_column(JSONB, default=dict)  # UI hints for rendering

    # Flags
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)  # System-provided (read-only)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    display_order: Mapped[int] = mapped_column(Integer, default=100)

    # Optional tenant scope (NULL = global)
    tenant_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=True)

    # Audit
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    updated_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped[Optional["Tenant"]] = relationship("Tenant")


# ============================================================================
# RPA Browser Automation Models
# ============================================================================

class RPACredential(Base):
    """Stored credentials for RPA browser automation."""

    __tablename__ = "rpa_credentials"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_rpa_credentials_tenant_name"),
        Index("idx_rpa_credentials_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)

    # Credential identity
    name: Mapped[str] = mapped_column(String(255), nullable=False)  # "Epic Sandbox Login"
    site_url: Mapped[str] = mapped_column(String(2000), nullable=False)  # "https://vendorservices.epic.com"

    # Encrypted credentials (never exposed to LLM or API responses)
    username_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)

    # MFA configuration
    mfa_type: Mapped[str] = mapped_column(String(20), default="none")  # none, totp, sms, email
    totp_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text)  # For auto-TOTP generation

    # Custom login flow (optional override for non-standard login pages)
    login_steps: Mapped[Optional[dict]] = mapped_column(JSONB)  # Custom step sequence

    # Notes
    notes: Mapped[Optional[str]] = mapped_column(Text)

    # Audit
    created_by: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant")


# ============================================================================
# AI Model Catalog (V102)
# ============================================================================

class AIModel(Base):
    """Available AI models for Bedrock — replaces hardcoded model lists."""

    __tablename__ = "ai_models"
    __table_args__ = (
        Index("idx_ai_models_active", "is_active", "sort_order"),
        Index("idx_ai_models_provider", "provider"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    model_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False, default="bedrock")
    vendor: Mapped[Optional[str]] = mapped_column(String(100))
    category: Mapped[str] = mapped_column(String(50), nullable=False, default="general")
    max_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=4096)
    supports_tools: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    supports_vision: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ============================================================================
# CDC Subscriptions (V103) — Databricks Change Data Feed at Enterprise Scale
# ============================================================================

class CDCSubscription(Base):
    """CDC subscription — monitors a Databricks Delta table for changes."""

    __tablename__ = "cdc_subscriptions"
    __table_args__ = (
        UniqueConstraint("tenant_id", "connector_id", "catalog", "schema_name", "table_name",
                         name="uq_cdc_sub_table"),
        Index("idx_cdc_subs_active", "is_active", "is_paused"),
        Index("idx_cdc_subs_connector", "connector_id"),
        Index("idx_cdc_subs_tenant", "tenant_id"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.tenants.id"), nullable=False)
    connector_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.connectors.id"), nullable=False)

    # What to watch
    catalog: Mapped[str] = mapped_column(String(255), nullable=False, default="hive_metastore")
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False, default="default")
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    change_types: Mapped[list] = mapped_column(ARRAY(String), nullable=False, default=lambda: ["insert", "update_postimage", "delete"])
    filter_expression: Mapped[Optional[str]] = mapped_column(Text)

    # Polling config
    poll_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    batch_size: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=5)

    # Where to route changes
    target_flow_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.logic_weaver_flows.id"))
    target_webhook_url: Mapped[Optional[str]] = mapped_column(Text)
    target_kafka_topic: Mapped[Optional[str]] = mapped_column(Text)

    # State
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    last_error_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    consecutive_errors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Metadata
    created_by: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", foreign_keys=[tenant_id])
    connector: Mapped["Connector"] = relationship("Connector", foreign_keys=[connector_id])


class CDCExecutionLog(Base):
    """Log entry for each CDC poll cycle — observability at scale."""

    __tablename__ = "cdc_execution_log"
    __table_args__ = (
        Index("idx_cdc_log_sub", "subscription_id", "created_at"),
        Index("idx_cdc_log_tenant", "tenant_id", "created_at"),
        {"schema": "common"}
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    subscription_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("common.cdc_subscriptions.id"), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    table_fqn: Mapped[str] = mapped_column(String(768), nullable=False)
    from_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    to_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    flow_execution_id: Mapped[Optional[UUID]] = mapped_column(PGUUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

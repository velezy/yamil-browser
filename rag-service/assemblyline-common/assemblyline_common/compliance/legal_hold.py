"""
Legal Hold Service - Prevent Deletion During Legal Proceedings

Implements legal hold functionality to:
- Prevent deletion of records during litigation
- Track hold status per record or category
- Support multiple concurrent holds
- Audit hold creation and release
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Set
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy import select, update, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ============================================================================
# Legal Hold Status
# ============================================================================

class HoldStatus(str, Enum):
    """Status of a legal hold."""
    ACTIVE = "active"
    RELEASED = "released"
    EXPIRED = "expired"


class HoldScope(str, Enum):
    """Scope of a legal hold."""
    TENANT = "tenant"  # All data for a tenant
    CATEGORY = "category"  # Specific data category
    RECORD = "record"  # Specific record(s)
    DATE_RANGE = "date_range"  # Records within date range


# ============================================================================
# Models
# ============================================================================

class LegalHold(BaseModel):
    """Legal hold definition."""

    id: UUID = Field(default_factory=uuid4)
    tenant_id: UUID

    # Hold details
    name: str
    description: Optional[str] = None
    case_number: Optional[str] = None  # External case reference
    scope: HoldScope = HoldScope.TENANT

    # Scope details
    category: Optional[str] = None  # If scope is CATEGORY
    record_ids: List[UUID] = Field(default_factory=list)  # If scope is RECORD
    start_date: Optional[datetime] = None  # If scope is DATE_RANGE
    end_date: Optional[datetime] = None  # If scope is DATE_RANGE

    # Status
    status: HoldStatus = HoldStatus.ACTIVE
    expires_at: Optional[datetime] = None

    # Audit
    created_by: Optional[UUID] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    released_by: Optional[UUID] = None
    released_at: Optional[datetime] = None
    release_reason: Optional[str] = None


class LegalHoldSummary(BaseModel):
    """Summary of legal holds for a tenant."""

    tenant_id: UUID
    total_holds: int = 0
    active_holds: int = 0
    released_holds: int = 0
    expired_holds: int = 0
    holds: List[LegalHold] = Field(default_factory=list)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class LegalHoldConfig:
    """Configuration for legal hold service."""

    # Default hold expiration (None = no expiration)
    default_expiration_days: Optional[int] = None

    # Require case number for holds
    require_case_number: bool = False

    # Notify on hold creation/release
    notify_on_change: bool = True


# ============================================================================
# Legal Hold Service
# ============================================================================

class LegalHoldService:
    """
    Service for managing legal holds.

    Legal holds prevent data from being deleted during retention cleanup,
    ensuring compliance with litigation and regulatory requirements.
    """

    def __init__(self, config: Optional[LegalHoldConfig] = None):
        self.config = config or LegalHoldConfig()
        self._holds: Dict[UUID, Dict[UUID, LegalHold]] = {}  # tenant_id -> hold_id -> hold

    async def create_hold(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        name: str,
        scope: HoldScope = HoldScope.TENANT,
        description: Optional[str] = None,
        case_number: Optional[str] = None,
        category: Optional[str] = None,
        record_ids: Optional[List[UUID]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        expires_at: Optional[datetime] = None,
        created_by: Optional[UUID] = None,
    ) -> LegalHold:
        """
        Create a new legal hold.

        Args:
            db: Database session
            tenant_id: Tenant to hold
            name: Name of the hold
            scope: Scope of the hold
            description: Optional description
            case_number: External case reference
            category: Data category (if scope is CATEGORY)
            record_ids: Specific records (if scope is RECORD)
            start_date: Start of date range (if scope is DATE_RANGE)
            end_date: End of date range (if scope is DATE_RANGE)
            expires_at: Optional expiration date
            created_by: User creating the hold

        Returns:
            Created LegalHold
        """
        if self.config.require_case_number and not case_number:
            raise ValueError("Case number is required for legal holds")

        hold = LegalHold(
            tenant_id=tenant_id,
            name=name,
            description=description,
            case_number=case_number,
            scope=scope,
            category=category,
            record_ids=record_ids or [],
            start_date=start_date,
            end_date=end_date,
            status=HoldStatus.ACTIVE,
            expires_at=expires_at,
            created_by=created_by,
        )

        # Store in memory (in production, this would be in database)
        if tenant_id not in self._holds:
            self._holds[tenant_id] = {}
        self._holds[tenant_id][hold.id] = hold

        # Create audit log entry
        from assemblyline_common.audit import get_immutable_audit_service
        try:
            audit_service = await get_immutable_audit_service()
            await audit_service.create_entry(
                db=db,
                tenant_id=tenant_id,
                action="legal_hold_created",
                resource_type="legal_hold",
                resource_id=str(hold.id),
                user_id=created_by,
                details={
                    "name": name,
                    "scope": scope.value,
                    "case_number": case_number,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to create audit entry: {e}")

        logger.info(
            "Created legal hold",
            extra={
                "event_type": "legal_hold.created",
                "hold_id": str(hold.id),
                "tenant_id": str(tenant_id),
                "name": name,
                "scope": scope.value,
            }
        )

        return hold

    async def release_hold(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        hold_id: UUID,
        released_by: Optional[UUID] = None,
        reason: Optional[str] = None,
    ) -> LegalHold:
        """
        Release a legal hold.

        Args:
            db: Database session
            tenant_id: Tenant ID
            hold_id: Hold to release
            released_by: User releasing the hold
            reason: Reason for release

        Returns:
            Updated LegalHold
        """
        if tenant_id not in self._holds or hold_id not in self._holds[tenant_id]:
            raise ValueError(f"Legal hold {hold_id} not found")

        hold = self._holds[tenant_id][hold_id]
        hold.status = HoldStatus.RELEASED
        hold.released_by = released_by
        hold.released_at = datetime.now(timezone.utc)
        hold.release_reason = reason

        # Create audit log entry
        from assemblyline_common.audit import get_immutable_audit_service
        try:
            audit_service = await get_immutable_audit_service()
            await audit_service.create_entry(
                db=db,
                tenant_id=tenant_id,
                action="legal_hold_released",
                resource_type="legal_hold",
                resource_id=str(hold_id),
                user_id=released_by,
                details={
                    "name": hold.name,
                    "reason": reason,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to create audit entry: {e}")

        logger.info(
            "Released legal hold",
            extra={
                "event_type": "legal_hold.released",
                "hold_id": str(hold_id),
                "tenant_id": str(tenant_id),
                "reason": reason,
            }
        )

        return hold

    async def get_hold(
        self,
        tenant_id: UUID,
        hold_id: UUID,
    ) -> Optional[LegalHold]:
        """Get a specific legal hold."""
        if tenant_id in self._holds and hold_id in self._holds[tenant_id]:
            return self._holds[tenant_id][hold_id]
        return None

    async def list_holds(
        self,
        tenant_id: UUID,
        status: Optional[HoldStatus] = None,
    ) -> List[LegalHold]:
        """
        List legal holds for a tenant.

        Args:
            tenant_id: Tenant ID
            status: Optional filter by status

        Returns:
            List of LegalHold objects
        """
        if tenant_id not in self._holds:
            return []

        holds = list(self._holds[tenant_id].values())

        if status:
            holds = [h for h in holds if h.status == status]

        return holds

    async def get_summary(self, tenant_id: UUID) -> LegalHoldSummary:
        """Get summary of legal holds for a tenant."""
        holds = await self.list_holds(tenant_id)

        return LegalHoldSummary(
            tenant_id=tenant_id,
            total_holds=len(holds),
            active_holds=len([h for h in holds if h.status == HoldStatus.ACTIVE]),
            released_holds=len([h for h in holds if h.status == HoldStatus.RELEASED]),
            expired_holds=len([h for h in holds if h.status == HoldStatus.EXPIRED]),
            holds=holds,
        )

    async def is_record_on_hold(
        self,
        tenant_id: UUID,
        record_id: UUID,
        category: Optional[str] = None,
        record_date: Optional[datetime] = None,
    ) -> bool:
        """
        Check if a record is under legal hold.

        Args:
            tenant_id: Tenant ID
            record_id: Record ID to check
            category: Data category
            record_date: Record creation date

        Returns:
            True if record is under hold
        """
        active_holds = await self.list_holds(tenant_id, HoldStatus.ACTIVE)

        for hold in active_holds:
            # Check expiration
            if hold.expires_at and hold.expires_at < datetime.now(timezone.utc):
                continue

            # Check scope
            if hold.scope == HoldScope.TENANT:
                return True

            if hold.scope == HoldScope.CATEGORY and hold.category == category:
                return True

            if hold.scope == HoldScope.RECORD and record_id in hold.record_ids:
                return True

            if hold.scope == HoldScope.DATE_RANGE and record_date:
                if hold.start_date and hold.end_date:
                    if hold.start_date <= record_date <= hold.end_date:
                        return True

        return False

    async def check_expired_holds(self, tenant_id: UUID) -> List[LegalHold]:
        """
        Check and update expired holds.

        Returns list of holds that were expired.
        """
        now = datetime.now(timezone.utc)
        expired = []

        if tenant_id not in self._holds:
            return expired

        for hold in self._holds[tenant_id].values():
            if (
                hold.status == HoldStatus.ACTIVE
                and hold.expires_at
                and hold.expires_at < now
            ):
                hold.status = HoldStatus.EXPIRED
                expired.append(hold)

        return expired


# ============================================================================
# Singleton Factory
# ============================================================================

_legal_hold_service: Optional[LegalHoldService] = None


async def get_legal_hold_service(
    config: Optional[LegalHoldConfig] = None
) -> LegalHoldService:
    """Get singleton instance of legal hold service."""
    global _legal_hold_service

    if _legal_hold_service is None:
        _legal_hold_service = LegalHoldService(config)
        logger.info(
            "Initialized legal hold service",
            extra={"event_type": "legal_hold.service_initialized"}
        )

    return _legal_hold_service

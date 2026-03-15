"""
Approval Workflow Service

Manages pending approvals for sensitive AI actions:
- Create pending approval requests
- Approve/reject pending actions
- Execute approved actions
- Expiration handling
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, List, Any, Callable, Awaitable
from uuid import UUID, uuid4

logger = logging.getLogger(__name__)


# ============================================================================
# Enums
# ============================================================================

class ApprovalStatus(str, Enum):
    """Status of a pending approval."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    EXECUTED = "executed"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class PendingApproval:
    """Represents a pending approval request."""
    id: UUID
    tenant_id: UUID
    conversation_id: Optional[UUID]
    requested_by: UUID
    requester_email: str

    # Action details
    action_type: str
    action_payload: Dict[str, Any]
    action_summary: str

    # Authorization
    required_role: str

    # Status
    status: ApprovalStatus = ApprovalStatus.PENDING

    # Approval handling
    approved_by: Optional[UUID] = None
    approver_email: Optional[str] = None
    approved_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None

    # Timestamps
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc) + timedelta(hours=24))

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "tenant_id": str(self.tenant_id),
            "conversation_id": str(self.conversation_id) if self.conversation_id else None,
            "requested_by": str(self.requested_by),
            "requester_email": self.requester_email,
            "action_type": self.action_type,
            "action_summary": self.action_summary,
            "required_role": self.required_role,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "approved_by": str(self.approved_by) if self.approved_by else None,
            "approver_email": self.approver_email,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "rejection_reason": self.rejection_reason,
        }


# ============================================================================
# Approval Service
# ============================================================================

class ApprovalService:
    """
    Service for managing approval workflows.
    
    Usage:
        approval_service = ApprovalService()
        
        # Create approval request
        approval = await approval_service.create_approval(
            tenant_id=tenant_id,
            requested_by=user_id,
            requester_email=email,
            action_type="delete_flow",
            action_payload={"flow_id": flow_id},
            action_summary="Delete flow 'My Flow'",
            required_role="admin",
        )
        
        # Approve the request
        await approval_service.approve(
            approval_id=approval.id,
            approved_by=admin_id,
            approver_email=admin_email,
        )
    """

    def __init__(self):
        # In-memory storage (in production, use database)
        self._pending_approvals: Dict[UUID, PendingApproval] = {}
        self._action_handlers: Dict[str, Callable[[Dict[str, Any]], Awaitable[Any]]] = {}

    def register_action_handler(
        self,
        action_type: str,
        handler: Callable[[Dict[str, Any]], Awaitable[Any]],
    ):
        """Register a handler for executing approved actions."""
        self._action_handlers[action_type] = handler

    async def create_approval(
        self,
        tenant_id: UUID,
        requested_by: UUID,
        requester_email: str,
        action_type: str,
        action_payload: Dict[str, Any],
        action_summary: str,
        required_role: str,
        conversation_id: Optional[UUID] = None,
        expires_in_hours: int = 24,
    ) -> PendingApproval:
        """Create a new pending approval request."""
        approval = PendingApproval(
            id=uuid4(),
            tenant_id=tenant_id,
            conversation_id=conversation_id,
            requested_by=requested_by,
            requester_email=requester_email,
            action_type=action_type,
            action_payload=action_payload,
            action_summary=action_summary,
            required_role=required_role,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=expires_in_hours),
        )

        self._pending_approvals[approval.id] = approval

        logger.info(
            f"Created approval request: {action_type}",
            extra={
                "event_type": "approval.created",
                "approval_id": str(approval.id),
                "action_type": action_type,
                "requested_by": str(requested_by),
                "required_role": required_role,
            }
        )

        return approval

    async def approve(
        self,
        approval_id: UUID,
        approved_by: UUID,
        approver_email: str,
        approver_role: str,
    ) -> PendingApproval:
        """Approve a pending request."""
        approval = self._pending_approvals.get(approval_id)
        if not approval:
            raise ValueError(f"Approval {approval_id} not found")

        if approval.status != ApprovalStatus.PENDING:
            raise ValueError(f"Approval is not pending: {approval.status.value}")

        if approval.is_expired():
            approval.status = ApprovalStatus.EXPIRED
            raise ValueError("Approval has expired")

        # Check if approver has required role
        # (In production, use proper role hierarchy check)
        role_hierarchy = ["viewer", "operator", "developer", "admin", "super_admin"]
        approver_level = role_hierarchy.index(approver_role.lower()) if approver_role.lower() in role_hierarchy else -1
        required_level = role_hierarchy.index(approval.required_role.lower()) if approval.required_role.lower() in role_hierarchy else 99

        if approver_level < required_level:
            raise ValueError(f"Insufficient role. Required: {approval.required_role}, has: {approver_role}")

        # Update approval
        approval.status = ApprovalStatus.APPROVED
        approval.approved_by = approved_by
        approval.approver_email = approver_email
        approval.approved_at = datetime.now(timezone.utc)

        logger.info(
            f"Approval granted: {approval.action_type}",
            extra={
                "event_type": "approval.approved",
                "approval_id": str(approval_id),
                "action_type": approval.action_type,
                "approved_by": str(approved_by),
            }
        )

        return approval

    async def reject(
        self,
        approval_id: UUID,
        rejected_by: UUID,
        rejection_reason: str,
    ) -> PendingApproval:
        """Reject a pending request."""
        approval = self._pending_approvals.get(approval_id)
        if not approval:
            raise ValueError(f"Approval {approval_id} not found")

        if approval.status != ApprovalStatus.PENDING:
            raise ValueError(f"Approval is not pending: {approval.status.value}")

        approval.status = ApprovalStatus.REJECTED
        approval.approved_by = rejected_by
        approval.rejection_reason = rejection_reason
        approval.approved_at = datetime.now(timezone.utc)

        logger.info(
            f"Approval rejected: {approval.action_type}",
            extra={
                "event_type": "approval.rejected",
                "approval_id": str(approval_id),
                "action_type": approval.action_type,
                "rejected_by": str(rejected_by),
                "reason": rejection_reason,
            }
        )

        return approval

    async def execute_approved(
        self,
        approval_id: UUID,
    ) -> Any:
        """Execute an approved action."""
        approval = self._pending_approvals.get(approval_id)
        if not approval:
            raise ValueError(f"Approval {approval_id} not found")

        if approval.status != ApprovalStatus.APPROVED:
            raise ValueError(f"Approval not approved: {approval.status.value}")

        handler = self._action_handlers.get(approval.action_type)
        if not handler:
            raise ValueError(f"No handler for action type: {approval.action_type}")

        try:
            result = await handler(approval.action_payload)
            approval.status = ApprovalStatus.EXECUTED

            logger.info(
                f"Executed approved action: {approval.action_type}",
                extra={
                    "event_type": "approval.executed",
                    "approval_id": str(approval_id),
                    "action_type": approval.action_type,
                }
            )

            return result

        except Exception as e:
            logger.error(
                f"Failed to execute approved action: {e}",
                extra={
                    "event_type": "approval.execution_failed",
                    "approval_id": str(approval_id),
                    "error": str(e),
                }
            )
            raise

    def get_pending(
        self,
        tenant_id: UUID,
        approver_role: Optional[str] = None,
    ) -> List[PendingApproval]:
        """Get pending approvals for a tenant."""
        pending = [
            a for a in self._pending_approvals.values()
            if a.tenant_id == tenant_id
            and a.status == ApprovalStatus.PENDING
            and not a.is_expired()
        ]

        # Filter by approver role if specified
        if approver_role:
            role_hierarchy = ["viewer", "operator", "developer", "admin", "super_admin"]
            approver_level = role_hierarchy.index(approver_role.lower()) if approver_role.lower() in role_hierarchy else -1

            pending = [
                a for a in pending
                if role_hierarchy.index(a.required_role.lower()) <= approver_level
            ]

        return sorted(pending, key=lambda a: a.created_at, reverse=True)

    def get_by_id(self, approval_id: UUID) -> Optional[PendingApproval]:
        """Get approval by ID."""
        return self._pending_approvals.get(approval_id)

    def cleanup_expired(self):
        """Mark expired approvals."""
        now = datetime.now(timezone.utc)
        expired_count = 0

        for approval in self._pending_approvals.values():
            if approval.status == ApprovalStatus.PENDING and approval.is_expired():
                approval.status = ApprovalStatus.EXPIRED
                expired_count += 1

        if expired_count > 0:
            logger.info(
                f"Cleaned up {expired_count} expired approvals",
                extra={
                    "event_type": "approval.cleanup",
                    "expired_count": expired_count,
                }
            )


# ============================================================================
# Singleton Factory
# ============================================================================

_approval_service: Optional[ApprovalService] = None


async def get_approval_service() -> ApprovalService:
    """Get singleton instance of approval service."""
    global _approval_service
    if _approval_service is None:
        _approval_service = ApprovalService()
    return _approval_service

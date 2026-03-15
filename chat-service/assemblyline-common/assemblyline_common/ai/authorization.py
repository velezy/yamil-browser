"""
AI Authorization Service

Handles permission checking for AI agent actions:
- Role-based access control
- Action-specific permissions
- PHI access authorization
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List, Set, Any
from uuid import UUID

logger = logging.getLogger(__name__)


# ============================================================================
# Permissions
# ============================================================================

class Permission(str, Enum):
    """Available permissions for AI actions."""
    # Query permissions
    QUERY_MESSAGES = "query:messages"
    QUERY_AUDIT_LOGS = "query:audit_logs"
    QUERY_STATISTICS = "query:statistics"

    # Flow permissions
    VIEW_FLOWS = "flow:view"
    CREATE_FLOWS = "flow:create"
    MODIFY_FLOWS = "flow:modify"
    DELETE_FLOWS = "flow:delete"
    DEPLOY_FLOWS = "flow:deploy"

    # API key permissions
    VIEW_API_KEYS = "api_key:view"
    CREATE_API_KEYS = "api_key:create"
    REVOKE_API_KEYS = "api_key:revoke"

    # User management
    VIEW_USERS = "user:view"
    INVITE_USERS = "user:invite"
    MODIFY_USERS = "user:modify"
    DEACTIVATE_USERS = "user:deactivate"

    # Connector permissions
    VIEW_CONNECTORS = "connector:view"
    CREATE_CONNECTORS = "connector:create"
    MODIFY_CONNECTORS = "connector:modify"
    TEST_CONNECTORS = "connector:test"

    # PHI access
    VIEW_PHI = "phi:view"
    EXPORT_PHI = "phi:export"

    # System config
    VIEW_CONFIG = "config:view"
    MODIFY_CONFIG = "config:modify"

    # AI specific
    USE_AI = "ai:use"
    VIEW_AI_HISTORY = "ai:view_history"


# ============================================================================
# Role Definitions
# ============================================================================

class Role(str, Enum):
    """User roles with associated permissions."""
    VIEWER = "viewer"
    OPERATOR = "operator"
    DEVELOPER = "developer"
    ADMIN = "admin"
    COMPLIANCE = "compliance"
    SUPER_ADMIN = "super_admin"


# Role to permissions mapping
ROLE_PERMISSIONS: Dict[Role, Set[Permission]] = {
    Role.VIEWER: {
        Permission.QUERY_MESSAGES,
        Permission.VIEW_FLOWS,
        Permission.VIEW_CONNECTORS,
        Permission.USE_AI,
        Permission.VIEW_AI_HISTORY,
    },
    Role.OPERATOR: {
        Permission.QUERY_MESSAGES,
        Permission.QUERY_STATISTICS,
        Permission.VIEW_FLOWS,
        Permission.VIEW_CONNECTORS,
        Permission.TEST_CONNECTORS,
        Permission.VIEW_PHI,  # With logging
        Permission.USE_AI,
        Permission.VIEW_AI_HISTORY,
    },
    Role.DEVELOPER: {
        Permission.QUERY_MESSAGES,
        Permission.QUERY_STATISTICS,
        Permission.VIEW_FLOWS,
        Permission.CREATE_FLOWS,
        Permission.MODIFY_FLOWS,
        Permission.DEPLOY_FLOWS,
        Permission.VIEW_CONNECTORS,
        Permission.CREATE_CONNECTORS,
        Permission.MODIFY_CONNECTORS,
        Permission.TEST_CONNECTORS,
        Permission.VIEW_PHI,
        Permission.USE_AI,
        Permission.VIEW_AI_HISTORY,
    },
    Role.ADMIN: {
        Permission.QUERY_MESSAGES,
        Permission.QUERY_STATISTICS,
        Permission.VIEW_FLOWS,
        Permission.CREATE_FLOWS,
        Permission.MODIFY_FLOWS,
        Permission.DELETE_FLOWS,
        Permission.DEPLOY_FLOWS,
        Permission.VIEW_API_KEYS,
        Permission.CREATE_API_KEYS,
        Permission.REVOKE_API_KEYS,
        Permission.VIEW_USERS,
        Permission.INVITE_USERS,
        Permission.MODIFY_USERS,
        Permission.DEACTIVATE_USERS,
        Permission.VIEW_CONNECTORS,
        Permission.CREATE_CONNECTORS,
        Permission.MODIFY_CONNECTORS,
        Permission.TEST_CONNECTORS,
        Permission.VIEW_PHI,
        Permission.VIEW_CONFIG,
        Permission.MODIFY_CONFIG,
        Permission.USE_AI,
        Permission.VIEW_AI_HISTORY,
    },
    Role.COMPLIANCE: {
        Permission.QUERY_MESSAGES,
        Permission.QUERY_AUDIT_LOGS,
        Permission.QUERY_STATISTICS,
        Permission.VIEW_FLOWS,
        Permission.VIEW_USERS,
        Permission.VIEW_PHI,
        Permission.EXPORT_PHI,
        Permission.VIEW_CONFIG,
        Permission.USE_AI,
        Permission.VIEW_AI_HISTORY,
    },
    Role.SUPER_ADMIN: set(Permission),  # All permissions
}


# ============================================================================
# Actions Requiring Approval
# ============================================================================

@dataclass
class ApprovalRequirement:
    """Defines when an action requires approval."""
    action: str
    required_approver_role: Role
    description: str


APPROVAL_REQUIREMENTS: List[ApprovalRequirement] = [
    ApprovalRequirement(
        action="delete_flow_with_messages",
        required_approver_role=Role.ADMIN,
        description="Delete a flow that has processed messages",
    ),
    ApprovalRequirement(
        action="bulk_api_key_revocation",
        required_approver_role=Role.SUPER_ADMIN,
        description="Revoke multiple API keys at once",
    ),
    ApprovalRequirement(
        action="export_phi",
        required_approver_role=Role.COMPLIANCE,
        description="Export PHI data",
    ),
    ApprovalRequirement(
        action="deploy_to_production",
        required_approver_role=Role.ADMIN,
        description="Deploy flow to production",
    ),
    ApprovalRequirement(
        action="modify_connector_credentials",
        required_approver_role=Role.ADMIN,
        description="Change connector credentials",
    ),
    ApprovalRequirement(
        action="deactivate_user",
        required_approver_role=Role.ADMIN,
        description="Deactivate a user account",
    ),
]


# ============================================================================
# Authorization Service
# ============================================================================

@dataclass
class AuthorizationContext:
    """Context for authorization checks."""
    user_id: UUID
    tenant_id: UUID
    role: Role
    email: str
    permissions: Set[Permission]
    is_mfa_verified: bool = False


class AuthorizationService:
    """
    Service for checking permissions and authorization.
    
    Usage:
        auth = AuthorizationService()
        
        # Check if user can perform action
        if auth.can_perform(context, Permission.CREATE_FLOWS):
            create_flow(...)
        
        # Check if action requires approval
        if auth.requires_approval(context, "deploy_to_production"):
            submit_for_approval(...)
    """

    def __init__(self):
        self._approval_requirements = {
            req.action: req for req in APPROVAL_REQUIREMENTS
        }

    def get_permissions_for_role(self, role: Role) -> Set[Permission]:
        """Get all permissions for a role."""
        return ROLE_PERMISSIONS.get(role, set())

    def create_context(
        self,
        user_id: UUID,
        tenant_id: UUID,
        role: str,
        email: str,
        is_mfa_verified: bool = False,
    ) -> AuthorizationContext:
        """Create authorization context for a user."""
        # Map role string to enum, fallback to VIEWER for unknown roles (e.g., "user")
        if isinstance(role, str):
            try:
                role_enum = Role(role.lower())
            except ValueError:
                # Unknown role (e.g., "user") defaults to viewer permissions
                role_enum = Role.VIEWER
        else:
            role_enum = role
        permissions = self.get_permissions_for_role(role_enum)

        return AuthorizationContext(
            user_id=user_id,
            tenant_id=tenant_id,
            role=role_enum,
            email=email,
            permissions=permissions,
            is_mfa_verified=is_mfa_verified,
        )

    def can_perform(
        self,
        context: AuthorizationContext,
        permission: Permission,
    ) -> bool:
        """Check if user can perform an action."""
        has_permission = permission in context.permissions

        logger.debug(
            f"Permission check: {permission.value}",
            extra={
                "event_type": "auth.permission_check",
                "user_id": str(context.user_id),
                "permission": permission.value,
                "has_permission": has_permission,
            }
        )

        return has_permission

    def can_access_phi(self, context: AuthorizationContext) -> bool:
        """Check if user can access PHI."""
        return Permission.VIEW_PHI in context.permissions

    def can_export_phi(self, context: AuthorizationContext) -> bool:
        """Check if user can export PHI."""
        return Permission.EXPORT_PHI in context.permissions

    def requires_approval(
        self,
        context: AuthorizationContext,
        action: str,
    ) -> Optional[ApprovalRequirement]:
        """Check if action requires approval."""
        requirement = self._approval_requirements.get(action)
        if not requirement:
            return None

        # Super admin doesn't need approval
        if context.role == Role.SUPER_ADMIN:
            return None

        # Check if user's role is sufficient to self-approve
        role_order = [Role.VIEWER, Role.OPERATOR, Role.DEVELOPER, Role.ADMIN, Role.SUPER_ADMIN]
        user_role_index = role_order.index(context.role) if context.role in role_order else 0
        required_role_index = role_order.index(requirement.required_approver_role)

        if user_role_index >= required_role_index:
            return None

        return requirement

    def get_required_permissions_for_agent(self, agent_type: str) -> Set[Permission]:
        """Get permissions required for using an agent type."""
        agent_permissions = {
            "flow-builder": {Permission.USE_AI, Permission.VIEW_FLOWS},
            "admin": {Permission.USE_AI, Permission.VIEW_API_KEYS},
            "analysis": {Permission.USE_AI, Permission.QUERY_MESSAGES},
            "query": {Permission.USE_AI, Permission.QUERY_MESSAGES},
        }
        return agent_permissions.get(agent_type, {Permission.USE_AI})

    def can_use_agent(
        self,
        context: AuthorizationContext,
        agent_type: str,
    ) -> bool:
        """Check if user can use a specific agent."""
        required = self.get_required_permissions_for_agent(agent_type)
        return required.issubset(context.permissions)

    def log_authorization(
        self,
        context: AuthorizationContext,
        action: str,
        resource_type: str,
        resource_id: str,
        authorized: bool,
        reason: Optional[str] = None,
    ):
        """Log an authorization decision."""
        logger.info(
            f"Authorization: {action} on {resource_type}",
            extra={
                "event_type": "auth.decision",
                "user_id": str(context.user_id),
                "tenant_id": str(context.tenant_id),
                "role": context.role.value,
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "authorized": authorized,
                "reason": reason,
            }
        )


# ============================================================================
# Singleton Factory
# ============================================================================

_authorization_service: Optional[AuthorizationService] = None


def get_authorization_service() -> AuthorizationService:
    """Get singleton instance of authorization service."""
    global _authorization_service
    if _authorization_service is None:
        _authorization_service = AuthorizationService()
    return _authorization_service

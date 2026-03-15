"""
Permission Definitions

Defines all system permissions in resource:action format.
Permissions are organized by scope/resource type.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class PermissionScope(str, Enum):
    """Permission scope/resource categories."""
    DOCUMENTS = "documents"
    CONVERSATIONS = "conversations"
    USERS = "users"
    ROLES = "roles"
    PROMPTS = "prompts"
    MODELS = "models"
    INTEGRATIONS = "integrations"
    SETTINGS = "settings"
    AUDIT = "audit"
    SYSTEM = "system"


@dataclass
class Permission:
    """
    Represents a single permission.

    Attributes:
        code: Unique permission code (e.g., "documents:read")
        name: Human-readable name
        description: Detailed description
        scope: Resource category
        is_dangerous: Whether this permission can cause data loss
    """
    code: str
    name: str
    description: str
    scope: PermissionScope
    is_dangerous: bool = False

    def __hash__(self):
        return hash(self.code)

    def __eq__(self, other):
        if isinstance(other, Permission):
            return self.code == other.code
        return self.code == other


# =============================================================================
# SYSTEM PERMISSIONS
# =============================================================================

SYSTEM_PERMISSIONS: Dict[str, Permission] = {}


def _register(*permissions: Permission):
    """Register permissions in the global registry."""
    for p in permissions:
        SYSTEM_PERMISSIONS[p.code] = p


# -----------------------------------------------------------------------------
# DOCUMENT PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="documents:read",
        name="View Documents",
        description="View and search documents",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:read_own",
        name="View Own Documents",
        description="View only documents uploaded by the user",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:upload",
        name="Upload Documents",
        description="Upload new documents to the system",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:delete",
        name="Delete Documents",
        description="Delete documents from the system",
        scope=PermissionScope.DOCUMENTS,
        is_dangerous=True,
    ),
    Permission(
        code="documents:delete_own",
        name="Delete Own Documents",
        description="Delete only documents uploaded by the user",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:manage",
        name="Manage All Documents",
        description="Full access to all document operations",
        scope=PermissionScope.DOCUMENTS,
        is_dangerous=True,
    ),
    # Enterprise document consumption features
    Permission(
        code="documents:flag",
        name="Flag Documents",
        description="Flag documents for review (hides from AI and most users)",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:unflag",
        name="Unflag Documents",
        description="Remove flags from documents",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:view_flagged",
        name="View Flagged Documents",
        description="View documents that have been flagged",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:protect",
        name="Protect Documents",
        description="Protect documents from deletion",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:unprotect",
        name="Unprotect Documents",
        description="Remove protection from documents",
        scope=PermissionScope.DOCUMENTS,
    ),
    Permission(
        code="documents:manage_visibility",
        name="Manage Document Visibility",
        description="Change document visibility (private/department/organization)",
        scope=PermissionScope.DOCUMENTS,
    ),
)

# -----------------------------------------------------------------------------
# CONVERSATION PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="conversations:read",
        name="View Conversations",
        description="View all conversations",
        scope=PermissionScope.CONVERSATIONS,
    ),
    Permission(
        code="conversations:read_own",
        name="View Own Conversations",
        description="View only user's own conversations",
        scope=PermissionScope.CONVERSATIONS,
    ),
    Permission(
        code="conversations:create",
        name="Create Conversations",
        description="Start new conversations with AI",
        scope=PermissionScope.CONVERSATIONS,
    ),
    Permission(
        code="conversations:delete",
        name="Delete Conversations",
        description="Delete any conversation",
        scope=PermissionScope.CONVERSATIONS,
        is_dangerous=True,
    ),
    Permission(
        code="conversations:delete_own",
        name="Delete Own Conversations",
        description="Delete only user's own conversations",
        scope=PermissionScope.CONVERSATIONS,
    ),
    Permission(
        code="conversations:export",
        name="Export Conversations",
        description="Export conversation history",
        scope=PermissionScope.CONVERSATIONS,
    ),
)

# -----------------------------------------------------------------------------
# USER MANAGEMENT PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="users:read",
        name="View Users",
        description="View user list and profiles",
        scope=PermissionScope.USERS,
    ),
    Permission(
        code="users:create",
        name="Create Users",
        description="Create new user accounts",
        scope=PermissionScope.USERS,
    ),
    Permission(
        code="users:update",
        name="Update Users",
        description="Modify user profiles and settings",
        scope=PermissionScope.USERS,
    ),
    Permission(
        code="users:delete",
        name="Delete Users",
        description="Delete user accounts",
        scope=PermissionScope.USERS,
        is_dangerous=True,
    ),
    Permission(
        code="users:manage_roles",
        name="Manage User Roles",
        description="Assign and modify user roles",
        scope=PermissionScope.USERS,
        is_dangerous=True,
    ),
    Permission(
        code="users:impersonate",
        name="Impersonate Users",
        description="Act as another user (for support)",
        scope=PermissionScope.USERS,
        is_dangerous=True,
    ),
)

# -----------------------------------------------------------------------------
# ROLE MANAGEMENT PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="roles:read",
        name="View Roles",
        description="View role definitions and permissions",
        scope=PermissionScope.ROLES,
    ),
    Permission(
        code="roles:create",
        name="Create Roles",
        description="Create new custom roles",
        scope=PermissionScope.ROLES,
    ),
    Permission(
        code="roles:update",
        name="Update Roles",
        description="Modify role permissions",
        scope=PermissionScope.ROLES,
        is_dangerous=True,
    ),
    Permission(
        code="roles:delete",
        name="Delete Roles",
        description="Delete custom roles",
        scope=PermissionScope.ROLES,
        is_dangerous=True,
    ),
)

# -----------------------------------------------------------------------------
# PROMPT MANAGEMENT PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="prompts:read",
        name="View Prompts",
        description="View system prompts",
        scope=PermissionScope.PROMPTS,
    ),
    Permission(
        code="prompts:create",
        name="Create Prompts",
        description="Create new prompts",
        scope=PermissionScope.PROMPTS,
    ),
    Permission(
        code="prompts:update",
        name="Update Prompts",
        description="Modify existing prompts",
        scope=PermissionScope.PROMPTS,
    ),
    Permission(
        code="prompts:delete",
        name="Delete Prompts",
        description="Delete prompts",
        scope=PermissionScope.PROMPTS,
    ),
    Permission(
        code="prompts:activate",
        name="Activate Prompts",
        description="Set prompts as active/default",
        scope=PermissionScope.PROMPTS,
    ),
)

# -----------------------------------------------------------------------------
# MODEL MANAGEMENT PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="models:read",
        name="View Models",
        description="View available AI models",
        scope=PermissionScope.MODELS,
    ),
    Permission(
        code="models:configure",
        name="Configure Models",
        description="Modify model settings and defaults",
        scope=PermissionScope.MODELS,
    ),
    Permission(
        code="models:pull",
        name="Pull Models",
        description="Download new models from Ollama",
        scope=PermissionScope.MODELS,
    ),
)

# -----------------------------------------------------------------------------
# INTEGRATION PERMISSIONS (MCP)
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="integrations:read",
        name="View Integrations",
        description="View available integrations/MCPs",
        scope=PermissionScope.INTEGRATIONS,
    ),
    Permission(
        code="integrations:configure",
        name="Configure Integrations",
        description="Enable/disable and configure integrations",
        scope=PermissionScope.INTEGRATIONS,
    ),
    Permission(
        code="integrations:install",
        name="Install Integrations",
        description="Install new MCP integrations",
        scope=PermissionScope.INTEGRATIONS,
    ),
    Permission(
        code="integrations:use",
        name="Use Integrations",
        description="Use enabled integrations in conversations",
        scope=PermissionScope.INTEGRATIONS,
    ),
)

# -----------------------------------------------------------------------------
# SETTINGS PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="settings:read",
        name="View Settings",
        description="View system settings",
        scope=PermissionScope.SETTINGS,
    ),
    Permission(
        code="settings:update",
        name="Update Settings",
        description="Modify system settings",
        scope=PermissionScope.SETTINGS,
    ),
    Permission(
        code="settings:backup",
        name="Backup System",
        description="Create and restore backups",
        scope=PermissionScope.SETTINGS,
        is_dangerous=True,
    ),
)

# -----------------------------------------------------------------------------
# AUDIT PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="audit:read",
        name="View Audit Logs",
        description="View system audit logs",
        scope=PermissionScope.AUDIT,
    ),
    Permission(
        code="audit:export",
        name="Export Audit Logs",
        description="Export audit logs for compliance",
        scope=PermissionScope.AUDIT,
    ),
)

# -----------------------------------------------------------------------------
# SYSTEM PERMISSIONS
# -----------------------------------------------------------------------------
_register(
    Permission(
        code="system:admin",
        name="System Administration",
        description="Full system administration access",
        scope=PermissionScope.SYSTEM,
        is_dangerous=True,
    ),
    Permission(
        code="system:metrics",
        name="View Metrics",
        description="View system metrics and health",
        scope=PermissionScope.SYSTEM,
    ),
    Permission(
        code="system:api_keys",
        name="Manage API Keys",
        description="Create and manage API keys",
        scope=PermissionScope.SYSTEM,
    ),
)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_permission_by_code(code: str) -> Optional[Permission]:
    """Get a permission by its code."""
    return SYSTEM_PERMISSIONS.get(code)


def get_permissions_by_scope(scope: PermissionScope) -> List[Permission]:
    """Get all permissions for a given scope."""
    return [p for p in SYSTEM_PERMISSIONS.values() if p.scope == scope]


def get_all_permissions() -> List[Permission]:
    """Get all system permissions."""
    return list(SYSTEM_PERMISSIONS.values())


def get_dangerous_permissions() -> List[Permission]:
    """Get all permissions marked as dangerous."""
    return [p for p in SYSTEM_PERMISSIONS.values() if p.is_dangerous]


def validate_permission_code(code: str) -> bool:
    """Check if a permission code is valid."""
    return code in SYSTEM_PERMISSIONS

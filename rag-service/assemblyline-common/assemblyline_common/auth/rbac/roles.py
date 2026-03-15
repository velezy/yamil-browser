"""
Role Definitions

Defines system roles and their default permissions.
Roles can be customized via the database.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional
from enum import Enum


class RoleType(str, Enum):
    """Role types - system roles cannot be deleted."""
    SYSTEM = "system"  # Built-in, cannot be deleted
    CUSTOM = "custom"  # User-created, can be deleted


@dataclass
class Role:
    """
    Represents a role with associated permissions.

    Attributes:
        code: Unique role code (e.g., "admin")
        name: Human-readable name
        description: Role description
        permissions: Set of permission codes
        role_type: Whether this is a system or custom role
        priority: Role priority for inheritance (higher = more powerful)
    """
    code: str
    name: str
    description: str
    permissions: Set[str] = field(default_factory=set)
    role_type: RoleType = RoleType.CUSTOM
    priority: int = 0

    def has_permission(self, permission_code: str) -> bool:
        """Check if role has a specific permission."""
        # system:admin grants all permissions
        if "system:admin" in self.permissions:
            return True
        return permission_code in self.permissions

    def add_permission(self, permission_code: str):
        """Add a permission to the role."""
        self.permissions.add(permission_code)

    def remove_permission(self, permission_code: str):
        """Remove a permission from the role."""
        self.permissions.discard(permission_code)


@dataclass
class RolePermission:
    """Represents the role-permission relationship."""
    role_code: str
    permission_code: str


# =============================================================================
# DEFAULT SYSTEM ROLES
# =============================================================================

DEFAULT_ROLES: Dict[str, Role] = {
    # Basic user - can use the system but limited management
    "user": Role(
        code="user",
        name="User",
        description="Standard user with basic access",
        role_type=RoleType.SYSTEM,
        priority=10,
        permissions={
            # Documents - own only
            "documents:read_own",
            "documents:upload",
            "documents:delete_own",
            # Conversations - own only
            "conversations:read_own",
            "conversations:create",
            "conversations:delete_own",
            # Integrations - use only
            "integrations:read",
            "integrations:use",
            # Models - read only
            "models:read",
        },
    ),

    # Manager - can manage team resources
    "manager": Role(
        code="manager",
        name="Manager",
        description="Team manager with extended access",
        role_type=RoleType.SYSTEM,
        priority=50,
        permissions={
            # Documents - full team access
            "documents:read",
            "documents:upload",
            "documents:delete",
            "documents:flag",  # Can flag documents for review
            "documents:manage_visibility",  # Can change document visibility
            # Conversations - full team access
            "conversations:read",
            "conversations:create",
            "conversations:delete",
            "conversations:export",
            # Users - read only
            "users:read",
            # Prompts - manage
            "prompts:read",
            "prompts:create",
            "prompts:update",
            # Integrations - configure
            "integrations:read",
            "integrations:configure",
            "integrations:use",
            # Models
            "models:read",
            "models:configure",
            # Audit - read
            "audit:read",
        },
    ),

    # Admin - system administration
    "admin": Role(
        code="admin",
        name="Administrator",
        description="System administrator with full management access",
        role_type=RoleType.SYSTEM,
        priority=90,
        permissions={
            # Documents - full including enterprise features
            "documents:read",
            "documents:upload",
            "documents:delete",
            "documents:manage",
            "documents:flag",
            "documents:unflag",
            "documents:view_flagged",
            "documents:protect",
            "documents:unprotect",
            "documents:manage_visibility",
            # Conversations - full
            "conversations:read",
            "conversations:create",
            "conversations:delete",
            "conversations:export",
            # Users - full except impersonation
            "users:read",
            "users:create",
            "users:update",
            "users:delete",
            "users:manage_roles",
            # Roles - read and update
            "roles:read",
            "roles:update",
            # Prompts - full
            "prompts:read",
            "prompts:create",
            "prompts:update",
            "prompts:delete",
            "prompts:activate",
            # Models - full
            "models:read",
            "models:configure",
            "models:pull",
            # Integrations - full
            "integrations:read",
            "integrations:configure",
            "integrations:install",
            "integrations:use",
            # Settings - full except backup
            "settings:read",
            "settings:update",
            # Audit - full
            "audit:read",
            "audit:export",
            # System
            "system:metrics",
            "system:api_keys",
        },
    ),

    # Superadmin - unrestricted access
    "superadmin": Role(
        code="superadmin",
        name="Super Administrator",
        description="Unrestricted system access",
        role_type=RoleType.SYSTEM,
        priority=100,
        permissions={
            # Full system access
            "system:admin",
            # Explicit dangerous permissions
            "users:impersonate",
            "roles:create",
            "roles:delete",
            "settings:backup",
        },
    ),
}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_role(code: str) -> Optional[Role]:
    """Get a role by its code."""
    return DEFAULT_ROLES.get(code)


def get_role_permissions(role_code: str) -> Set[str]:
    """Get all permission codes for a role."""
    role = DEFAULT_ROLES.get(role_code)
    if role:
        return role.permissions.copy()
    return set()


def get_all_roles() -> List[Role]:
    """Get all system roles."""
    return list(DEFAULT_ROLES.values())


def get_system_roles() -> List[Role]:
    """Get only system (non-deletable) roles."""
    return [r for r in DEFAULT_ROLES.values() if r.role_type == RoleType.SYSTEM]


def role_has_permission(role_code: str, permission_code: str) -> bool:
    """Check if a role has a specific permission."""
    role = DEFAULT_ROLES.get(role_code)
    if role:
        return role.has_permission(permission_code)
    return False


def get_role_hierarchy() -> List[str]:
    """Get roles sorted by priority (lowest to highest)."""
    return sorted(DEFAULT_ROLES.keys(), key=lambda r: DEFAULT_ROLES[r].priority)


def role_includes_role(higher_role: str, lower_role: str) -> bool:
    """Check if higher_role includes all permissions of lower_role."""
    hr = DEFAULT_ROLES.get(higher_role)
    lr = DEFAULT_ROLES.get(lower_role)
    if not hr or not lr:
        return False
    if hr.priority < lr.priority:
        return False
    # Superadmin includes all
    if "system:admin" in hr.permissions:
        return True
    return lr.permissions.issubset(hr.permissions)

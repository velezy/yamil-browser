"""
DriveSentinel RBAC (Role-Based Access Control)

Enterprise-grade permission system with:
- Fine-grained permissions (resource:action format)
- Customizable roles with permission sets
- Resource-level access control
- FastAPI integration with decorators

Usage:
    from assemblyline_common.rbac import require_permission, has_permission

    # FastAPI endpoint protection
    @app.get("/documents")
    @require_permission("documents:read")
    async def list_documents(user: dict = Depends(get_current_user)):
        ...

    # Programmatic permission check
    if await has_permission(user_id, "documents:delete"):
        ...
"""

from .permissions import (
    Permission,
    PermissionScope,
    SYSTEM_PERMISSIONS,
    get_permission_by_code,
)
from .roles import (
    Role,
    RolePermission,
    DEFAULT_ROLES,
    get_role_permissions,
)
from .checker import (
    PermissionChecker,
    has_permission,
    has_any_permission,
    has_all_permissions,
    get_user_permissions,
)
from .decorators import (
    require_permission,
    require_any_permission,
    require_all_permissions,
    require_role,
)
from .repository import RBACRepository

__all__ = [
    # Permissions
    "Permission",
    "PermissionScope",
    "SYSTEM_PERMISSIONS",
    "get_permission_by_code",
    # Roles
    "Role",
    "RolePermission",
    "DEFAULT_ROLES",
    "get_role_permissions",
    # Checker
    "PermissionChecker",
    "has_permission",
    "has_any_permission",
    "has_all_permissions",
    "get_user_permissions",
    # Decorators
    "require_permission",
    "require_any_permission",
    "require_all_permissions",
    "require_role",
    # Repository
    "RBACRepository",
]

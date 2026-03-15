"""
Permission Checker

Runtime permission checking utilities.
Checks user permissions against required permissions.
"""

import logging
from typing import List, Optional, Set, Union
from functools import lru_cache

from .roles import DEFAULT_ROLES, get_role_permissions, role_has_permission

logger = logging.getLogger(__name__)


class PermissionChecker:
    """
    Permission checking engine.

    Handles permission evaluation including:
    - Role-based permissions
    - Custom user permissions (from database)
    - Resource-level permissions
    """

    def __init__(self, db_pool=None):
        """
        Initialize the permission checker.

        Args:
            db_pool: Optional database pool for loading custom permissions
        """
        self._db_pool = db_pool
        self._permission_cache = {}

    async def get_user_permissions(
        self,
        user_id: int,
        user_role: str,
        include_custom: bool = True
    ) -> Set[str]:
        """
        Get all permissions for a user.

        Combines:
        1. Role-based permissions (from DEFAULT_ROLES)
        2. Custom permissions assigned to user (from database)

        Args:
            user_id: User ID
            user_role: User's role code
            include_custom: Whether to include custom DB permissions

        Returns:
            Set of permission codes
        """
        # Start with role permissions
        permissions = get_role_permissions(user_role)

        # Add custom permissions from database if available
        if include_custom and self._db_pool:
            custom = await self._load_custom_permissions(user_id)
            permissions.update(custom)

        return permissions

    async def _load_custom_permissions(self, user_id: int) -> Set[str]:
        """Load custom permissions assigned to a specific user."""
        # Check cache first
        cache_key = f"user:{user_id}"
        if cache_key in self._permission_cache:
            return self._permission_cache[cache_key]

        permissions = set()

        if self._db_pool:
            try:
                async with self._db_pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT permission_code
                        FROM user_permissions
                        WHERE user_id = $1 AND is_granted = TRUE
                    """, user_id)
                    permissions = {row["permission_code"] for row in rows}

                    # Also check for denied permissions (explicit denials)
                    denied = await conn.fetch("""
                        SELECT permission_code
                        FROM user_permissions
                        WHERE user_id = $1 AND is_granted = FALSE
                    """, user_id)
                    # Store denied for later use
                    self._permission_cache[f"denied:{user_id}"] = {
                        row["permission_code"] for row in denied
                    }

            except Exception as e:
                logger.warning(f"Could not load custom permissions: {e}")

        self._permission_cache[cache_key] = permissions
        return permissions

    async def has_permission(
        self,
        user_id: int,
        user_role: str,
        permission_code: str,
        resource_id: Optional[int] = None,
        resource_owner_id: Optional[int] = None
    ) -> bool:
        """
        Check if a user has a specific permission.

        Args:
            user_id: User ID
            user_role: User's role code
            permission_code: Permission to check
            resource_id: Optional resource ID for resource-level checks
            resource_owner_id: Optional owner ID for "own" permission checks

        Returns:
            True if user has the permission
        """
        # Get all user permissions (this also loads denied permissions into cache)
        permissions = await self.get_user_permissions(user_id, user_role)

        # Check for explicit denial AFTER loading custom permissions
        denied = self._permission_cache.get(f"denied:{user_id}", set())
        if permission_code in denied:
            return False

        # Check for system:admin (superuser)
        if "system:admin" in permissions:
            return True

        # Direct permission check
        if permission_code in permissions:
            return True

        # Check for "own" variant permissions
        if permission_code.endswith("_own") is False:
            own_variant = permission_code.replace(":", ":") + "_own"
            if own_variant.replace(":_own", ":").replace(":", ":") != own_variant:
                # Try resource:action_own format
                parts = permission_code.split(":")
                if len(parts) == 2:
                    own_variant = f"{parts[0]}:{parts[1]}_own"
                    if own_variant in permissions:
                        # Check if user owns the resource
                        if resource_owner_id is not None and resource_owner_id == user_id:
                            return True

        # Check for broader "manage" permission
        parts = permission_code.split(":")
        if len(parts) == 2:
            manage_permission = f"{parts[0]}:manage"
            if manage_permission in permissions:
                return True

        return False

    async def has_any_permission(
        self,
        user_id: int,
        user_role: str,
        permission_codes: List[str]
    ) -> bool:
        """Check if user has ANY of the specified permissions."""
        for code in permission_codes:
            if await self.has_permission(user_id, user_role, code):
                return True
        return False

    async def has_all_permissions(
        self,
        user_id: int,
        user_role: str,
        permission_codes: List[str]
    ) -> bool:
        """Check if user has ALL of the specified permissions."""
        for code in permission_codes:
            if not await self.has_permission(user_id, user_role, code):
                return False
        return True

    def clear_cache(self, user_id: Optional[int] = None):
        """Clear permission cache."""
        if user_id:
            self._permission_cache.pop(f"user:{user_id}", None)
            self._permission_cache.pop(f"denied:{user_id}", None)
        else:
            self._permission_cache.clear()


# =============================================================================
# GLOBAL CHECKER INSTANCE
# =============================================================================

_checker: Optional[PermissionChecker] = None


def get_checker(db_pool=None) -> PermissionChecker:
    """Get or create the global permission checker."""
    global _checker
    if _checker is None or db_pool is not None:
        _checker = PermissionChecker(db_pool)
    return _checker


def set_checker(checker: PermissionChecker):
    """Set the global permission checker."""
    global _checker
    _checker = checker


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def has_permission(
    user_id: int,
    user_role: str,
    permission_code: str,
    resource_owner_id: Optional[int] = None,
    db_pool=None
) -> bool:
    """
    Check if a user has a specific permission.

    Convenience function using the global checker.

    Args:
        user_id: User ID
        user_role: User's role code (e.g., "admin", "user")
        permission_code: Permission to check (e.g., "documents:read")
        resource_owner_id: Optional owner ID for "own" permission checks
        db_pool: Optional database pool

    Returns:
        True if user has the permission
    """
    checker = get_checker(db_pool)
    return await checker.has_permission(
        user_id=user_id,
        user_role=user_role,
        permission_code=permission_code,
        resource_owner_id=resource_owner_id
    )


async def has_any_permission(
    user_id: int,
    user_role: str,
    permission_codes: List[str],
    db_pool=None
) -> bool:
    """Check if user has ANY of the specified permissions."""
    checker = get_checker(db_pool)
    return await checker.has_any_permission(user_id, user_role, permission_codes)


async def has_all_permissions(
    user_id: int,
    user_role: str,
    permission_codes: List[str],
    db_pool=None
) -> bool:
    """Check if user has ALL of the specified permissions."""
    checker = get_checker(db_pool)
    return await checker.has_all_permissions(user_id, user_role, permission_codes)


async def get_user_permissions(
    user_id: int,
    user_role: str,
    db_pool=None
) -> Set[str]:
    """Get all permissions for a user."""
    checker = get_checker(db_pool)
    return await checker.get_user_permissions(user_id, user_role)


def check_role_permission(role_code: str, permission_code: str) -> bool:
    """
    Synchronous check if a role has a permission.

    Uses only DEFAULT_ROLES, no database lookup.
    """
    return role_has_permission(role_code, permission_code)

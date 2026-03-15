"""
RBAC Repository

Database operations for managing roles and permissions.
Handles custom roles, user-specific permissions, and role assignments.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

from .permissions import SYSTEM_PERMISSIONS, Permission
from .roles import DEFAULT_ROLES, Role, RoleType

logger = logging.getLogger(__name__)


@dataclass
class UserPermission:
    """Represents a custom permission granted/denied to a user."""
    user_id: int
    permission_code: str
    is_granted: bool
    granted_by: Optional[int] = None
    granted_at: Optional[datetime] = None
    reason: Optional[str] = None


@dataclass
class CustomRole:
    """Represents a custom role stored in database."""
    id: int
    code: str
    name: str
    description: str
    permissions: Set[str]
    created_by: Optional[int] = None
    created_at: Optional[datetime] = None


class RBACRepository:
    """
    Database repository for RBAC operations.

    Handles:
    - Custom role management
    - User permission overrides
    - Role assignments
    """

    def __init__(self, db_pool):
        """
        Initialize with database connection pool.

        Args:
            db_pool: asyncpg connection pool
        """
        self._pool = db_pool

    # =========================================================================
    # ROLE OPERATIONS
    # =========================================================================

    async def get_all_roles(self) -> List[Role]:
        """Get all roles (system + custom)."""
        roles = list(DEFAULT_ROLES.values())

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, code, name, description, created_by, created_at
                FROM custom_roles
                WHERE is_active = TRUE
                ORDER BY name
            """)

            for row in rows:
                # Get permissions for this custom role
                perm_rows = await conn.fetch("""
                    SELECT permission_code
                    FROM role_permissions
                    WHERE role_id = $1
                """, row["id"])

                permissions = {r["permission_code"] for r in perm_rows}

                roles.append(Role(
                    code=row["code"],
                    name=row["name"],
                    description=row["description"],
                    permissions=permissions,
                    role_type=RoleType.CUSTOM,
                    priority=25,  # Custom roles between user and manager
                ))

        return roles

    async def get_role(self, role_code: str) -> Optional[Role]:
        """Get a role by code."""
        # Check system roles first
        if role_code in DEFAULT_ROLES:
            return DEFAULT_ROLES[role_code]

        # Check custom roles
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, code, name, description
                FROM custom_roles
                WHERE code = $1 AND is_active = TRUE
            """, role_code)

            if row:
                perm_rows = await conn.fetch("""
                    SELECT permission_code
                    FROM role_permissions
                    WHERE role_id = $1
                """, row["id"])

                return Role(
                    code=row["code"],
                    name=row["name"],
                    description=row["description"],
                    permissions={r["permission_code"] for r in perm_rows},
                    role_type=RoleType.CUSTOM,
                    priority=25,
                )

        return None

    async def create_custom_role(
        self,
        code: str,
        name: str,
        description: str,
        permissions: List[str],
        created_by: int
    ) -> CustomRole:
        """
        Create a new custom role.

        Args:
            code: Unique role code
            name: Display name
            description: Role description
            permissions: List of permission codes
            created_by: User ID creating the role

        Returns:
            Created CustomRole

        Raises:
            ValueError: If role code already exists or permissions invalid
        """
        # Validate code doesn't exist
        if code in DEFAULT_ROLES:
            raise ValueError(f"Cannot use system role code: {code}")

        # Validate permissions
        invalid = [p for p in permissions if p not in SYSTEM_PERMISSIONS]
        if invalid:
            raise ValueError(f"Invalid permission codes: {invalid}")

        async with self._pool.acquire() as conn:
            # Check for existing custom role
            existing = await conn.fetchval("""
                SELECT id FROM custom_roles WHERE code = $1
            """, code)
            if existing:
                raise ValueError(f"Role code already exists: {code}")

            # Create role
            async with conn.transaction():
                role_id = await conn.fetchval("""
                    INSERT INTO custom_roles (code, name, description, created_by, is_active)
                    VALUES ($1, $2, $3, $4, TRUE)
                    RETURNING id
                """, code, name, description, created_by)

                # Add permissions
                for perm in permissions:
                    await conn.execute("""
                        INSERT INTO role_permissions (role_id, permission_code)
                        VALUES ($1, $2)
                    """, role_id, perm)

                logger.info(f"Created custom role: {code} with {len(permissions)} permissions")

                return CustomRole(
                    id=role_id,
                    code=code,
                    name=name,
                    description=description,
                    permissions=set(permissions),
                    created_by=created_by,
                    created_at=datetime.utcnow(),
                )

    async def update_custom_role(
        self,
        role_code: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        permissions: Optional[List[str]] = None
    ) -> bool:
        """
        Update a custom role.

        Args:
            role_code: Role code to update
            name: New name (optional)
            description: New description (optional)
            permissions: New permission list (optional, replaces existing)

        Returns:
            True if updated, False if not found

        Raises:
            ValueError: If trying to update system role
        """
        if role_code in DEFAULT_ROLES:
            raise ValueError(f"Cannot modify system role: {role_code}")

        if permissions:
            invalid = [p for p in permissions if p not in SYSTEM_PERMISSIONS]
            if invalid:
                raise ValueError(f"Invalid permission codes: {invalid}")

        async with self._pool.acquire() as conn:
            role_id = await conn.fetchval("""
                SELECT id FROM custom_roles WHERE code = $1 AND is_active = TRUE
            """, role_code)

            if not role_id:
                return False

            async with conn.transaction():
                # Update role details
                if name or description:
                    await conn.execute("""
                        UPDATE custom_roles
                        SET name = COALESCE($2, name),
                            description = COALESCE($3, description),
                            updated_at = NOW()
                        WHERE id = $1
                    """, role_id, name, description)

                # Update permissions
                if permissions is not None:
                    await conn.execute("""
                        DELETE FROM role_permissions WHERE role_id = $1
                    """, role_id)

                    for perm in permissions:
                        await conn.execute("""
                            INSERT INTO role_permissions (role_id, permission_code)
                            VALUES ($1, $2)
                        """, role_id, perm)

                logger.info(f"Updated custom role: {role_code}")
                return True

    async def delete_custom_role(self, role_code: str) -> bool:
        """
        Delete (soft) a custom role.

        Args:
            role_code: Role code to delete

        Returns:
            True if deleted, False if not found

        Raises:
            ValueError: If trying to delete system role
        """
        if role_code in DEFAULT_ROLES:
            raise ValueError(f"Cannot delete system role: {role_code}")

        async with self._pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE custom_roles
                SET is_active = FALSE, updated_at = NOW()
                WHERE code = $1 AND is_active = TRUE
            """, role_code)

            deleted = result == "UPDATE 1"
            if deleted:
                logger.info(f"Deleted custom role: {role_code}")
            return deleted

    # =========================================================================
    # USER PERMISSION OPERATIONS
    # =========================================================================

    async def get_user_permissions(self, user_id: int) -> Dict[str, bool]:
        """
        Get all custom permissions for a user.

        Returns:
            Dict mapping permission_code to is_granted (True=granted, False=denied)
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT permission_code, is_granted
                FROM user_permissions
                WHERE user_id = $1
            """, user_id)

            return {row["permission_code"]: row["is_granted"] for row in rows}

    async def grant_permission(
        self,
        user_id: int,
        permission_code: str,
        granted_by: int,
        reason: Optional[str] = None
    ) -> bool:
        """
        Grant a permission to a user.

        Args:
            user_id: User to grant permission to
            permission_code: Permission to grant
            granted_by: Admin user granting the permission
            reason: Optional reason for the grant

        Returns:
            True if granted (or already granted)
        """
        if permission_code not in SYSTEM_PERMISSIONS:
            raise ValueError(f"Invalid permission code: {permission_code}")

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO user_permissions (user_id, permission_code, is_granted, granted_by, reason)
                VALUES ($1, $2, TRUE, $3, $4)
                ON CONFLICT (user_id, permission_code)
                DO UPDATE SET is_granted = TRUE, granted_by = $3, reason = $4, updated_at = NOW()
            """, user_id, permission_code, granted_by, reason)

            logger.info(f"Granted permission {permission_code} to user {user_id}")
            return True

    async def deny_permission(
        self,
        user_id: int,
        permission_code: str,
        granted_by: int,
        reason: Optional[str] = None
    ) -> bool:
        """
        Explicitly deny a permission to a user.

        This overrides role-based permissions.

        Args:
            user_id: User to deny permission from
            permission_code: Permission to deny
            granted_by: Admin user denying the permission
            reason: Optional reason for the denial

        Returns:
            True if denied
        """
        if permission_code not in SYSTEM_PERMISSIONS:
            raise ValueError(f"Invalid permission code: {permission_code}")

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO user_permissions (user_id, permission_code, is_granted, granted_by, reason)
                VALUES ($1, $2, FALSE, $3, $4)
                ON CONFLICT (user_id, permission_code)
                DO UPDATE SET is_granted = FALSE, granted_by = $3, reason = $4, updated_at = NOW()
            """, user_id, permission_code, granted_by, reason)

            logger.info(f"Denied permission {permission_code} for user {user_id}")
            return True

    async def revoke_permission(self, user_id: int, permission_code: str) -> bool:
        """
        Remove a custom permission override (grant or deny).

        User will fall back to their role's default permissions.

        Args:
            user_id: User ID
            permission_code: Permission to revoke

        Returns:
            True if removed, False if didn't exist
        """
        async with self._pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM user_permissions
                WHERE user_id = $1 AND permission_code = $2
            """, user_id, permission_code)

            revoked = result == "DELETE 1"
            if revoked:
                logger.info(f"Revoked permission {permission_code} from user {user_id}")
            return revoked

    async def bulk_grant_permissions(
        self,
        user_id: int,
        permission_codes: List[str],
        granted_by: int,
        reason: Optional[str] = None
    ) -> int:
        """
        Grant multiple permissions at once.

        Returns:
            Number of permissions granted
        """
        count = 0
        for perm in permission_codes:
            if await self.grant_permission(user_id, perm, granted_by, reason):
                count += 1
        return count

    async def bulk_revoke_permissions(
        self,
        user_id: int,
        permission_codes: List[str]
    ) -> int:
        """
        Revoke multiple permissions at once.

        Returns:
            Number of permissions revoked
        """
        count = 0
        for perm in permission_codes:
            if await self.revoke_permission(user_id, perm):
                count += 1
        return count

    # =========================================================================
    # USER ROLE OPERATIONS
    # =========================================================================

    async def assign_role(
        self,
        user_id: int,
        role_code: str,
        assigned_by: int
    ) -> bool:
        """
        Assign a role to a user.

        Args:
            user_id: User to assign role to
            role_code: Role code to assign
            assigned_by: Admin making the assignment

        Returns:
            True if assigned
        """
        # Validate role exists
        role = await self.get_role(role_code)
        if not role:
            raise ValueError(f"Invalid role code: {role_code}")

        async with self._pool.acquire() as conn:
            await conn.execute("""
                UPDATE users
                SET role = $2, updated_at = NOW()
                WHERE id = $1
            """, user_id, role_code)

            logger.info(f"Assigned role {role_code} to user {user_id} by {assigned_by}")
            return True

    async def get_users_with_role(self, role_code: str) -> List[int]:
        """Get all user IDs with a specific role."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id FROM users WHERE role = $1
            """, role_code)
            return [row["id"] for row in rows]

    async def get_users_with_permission(self, permission_code: str) -> List[Tuple[int, str]]:
        """
        Get all users who have a permission (via role or custom grant).

        Returns:
            List of (user_id, source) tuples where source is "role" or "custom"
        """
        results = []

        async with self._pool.acquire() as conn:
            # Users with custom permission grant
            custom_rows = await conn.fetch("""
                SELECT user_id FROM user_permissions
                WHERE permission_code = $1 AND is_granted = TRUE
            """, permission_code)
            for row in custom_rows:
                results.append((row["user_id"], "custom"))

            # Users with role that includes permission
            for role_code, role in DEFAULT_ROLES.items():
                if role.has_permission(permission_code):
                    user_rows = await conn.fetch("""
                        SELECT id FROM users WHERE role = $1
                    """, role_code)
                    for row in user_rows:
                        if (row["id"], "custom") not in results:
                            results.append((row["id"], "role"))

        return results

    # =========================================================================
    # PERMISSION INFO
    # =========================================================================

    async def get_all_permissions(self) -> List[Permission]:
        """Get all system permissions."""
        return list(SYSTEM_PERMISSIONS.values())

    async def get_effective_permissions(
        self,
        user_id: int,
        user_role: str
    ) -> Dict[str, dict]:
        """
        Get user's effective permissions with source info.

        Returns:
            Dict mapping permission_code to {granted: bool, source: str}
        """
        from .roles import get_role_permissions

        result = {}

        # Start with role permissions
        role_perms = get_role_permissions(user_role)
        for perm in role_perms:
            result[perm] = {"granted": True, "source": "role"}

        # Apply custom permissions
        custom_perms = await self.get_user_permissions(user_id)
        for perm, granted in custom_perms.items():
            if perm in result:
                # Override role permission
                result[perm] = {"granted": granted, "source": "custom_override"}
            else:
                # Additional permission
                result[perm] = {"granted": granted, "source": "custom"}

        return result

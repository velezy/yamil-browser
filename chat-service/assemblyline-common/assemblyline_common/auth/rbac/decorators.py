"""
FastAPI Permission Decorators

Decorators for protecting endpoints with permission checks.
Works with FastAPI's dependency injection system.
"""

import logging
from functools import wraps
from typing import Callable, List, Optional, Union

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .checker import get_checker, has_permission as check_permission
from .roles import DEFAULT_ROLES

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


class PermissionDenied(HTTPException):
    """Permission denied exception with details."""

    def __init__(
        self,
        detail: str = "Permission denied",
        required_permission: Optional[str] = None
    ):
        super().__init__(
            status_code=403,
            detail={
                "error": "permission_denied",
                "message": detail,
                "required_permission": required_permission,
            }
        )


class RoleRequired(HTTPException):
    """Role required exception."""

    def __init__(self, required_role: str):
        super().__init__(
            status_code=403,
            detail={
                "error": "insufficient_role",
                "message": f"Role '{required_role}' or higher is required",
                "required_role": required_role,
            }
        )


def _get_user_from_request(request: Request) -> Optional[dict]:
    """
    Extract user information from request state.

    The user should be set by an authentication middleware.
    Expected format: {"id": int, "role": str, ...}
    """
    return getattr(request.state, "user", None)


def require_permission(permission_code: str):
    """
    Decorator to require a specific permission for an endpoint.

    Usage:
        @app.get("/documents")
        @require_permission("documents:read")
        async def list_documents(request: Request):
            ...

    Args:
        permission_code: The permission code required (e.g., "documents:read")

    Raises:
        HTTPException 401: If user is not authenticated
        HTTPException 403: If user lacks the required permission
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Find request object in kwargs or args
            request = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                raise HTTPException(
                    status_code=500,
                    detail="Request object not found in endpoint"
                )

            user = _get_user_from_request(request)
            if user is None:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )

            user_id = user.get("id") or user.get("user_id")
            user_role = user.get("role", "user")

            # Get db_pool from app state if available
            db_pool = getattr(request.app.state, "db_pool", None)

            # Check permission
            has_perm = await check_permission(
                user_id=user_id,
                user_role=user_role,
                permission_code=permission_code,
                db_pool=db_pool
            )

            if not has_perm:
                logger.warning(
                    f"Permission denied: user={user_id}, role={user_role}, "
                    f"required={permission_code}"
                )
                raise PermissionDenied(
                    detail=f"Permission '{permission_code}' is required",
                    required_permission=permission_code
                )

            return await func(*args, **kwargs)

        return wrapper
    return decorator


def require_any_permission(permission_codes: List[str]):
    """
    Decorator to require ANY of the specified permissions.

    Usage:
        @app.get("/reports")
        @require_any_permission(["reports:read", "admin:read"])
        async def view_reports(request: Request):
            ...

    Args:
        permission_codes: List of permission codes (user needs at least one)
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                raise HTTPException(
                    status_code=500,
                    detail="Request object not found in endpoint"
                )

            user = _get_user_from_request(request)
            if user is None:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )

            user_id = user.get("id") or user.get("user_id")
            user_role = user.get("role", "user")
            db_pool = getattr(request.app.state, "db_pool", None)

            # Check if user has any of the permissions
            for perm in permission_codes:
                has_perm = await check_permission(
                    user_id=user_id,
                    user_role=user_role,
                    permission_code=perm,
                    db_pool=db_pool
                )
                if has_perm:
                    return await func(*args, **kwargs)

            logger.warning(
                f"Permission denied: user={user_id}, role={user_role}, "
                f"required_any={permission_codes}"
            )
            raise PermissionDenied(
                detail=f"One of these permissions is required: {', '.join(permission_codes)}",
                required_permission=permission_codes[0]
            )

        return wrapper
    return decorator


def require_all_permissions(permission_codes: List[str]):
    """
    Decorator to require ALL of the specified permissions.

    Usage:
        @app.delete("/users/{user_id}")
        @require_all_permissions(["users:read", "users:delete"])
        async def delete_user(request: Request, user_id: int):
            ...

    Args:
        permission_codes: List of permission codes (user needs all)
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                raise HTTPException(
                    status_code=500,
                    detail="Request object not found in endpoint"
                )

            user = _get_user_from_request(request)
            if user is None:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )

            user_id = user.get("id") or user.get("user_id")
            user_role = user.get("role", "user")
            db_pool = getattr(request.app.state, "db_pool", None)

            # Check if user has all permissions
            missing = []
            for perm in permission_codes:
                has_perm = await check_permission(
                    user_id=user_id,
                    user_role=user_role,
                    permission_code=perm,
                    db_pool=db_pool
                )
                if not has_perm:
                    missing.append(perm)

            if missing:
                logger.warning(
                    f"Permission denied: user={user_id}, role={user_role}, "
                    f"missing={missing}"
                )
                raise PermissionDenied(
                    detail=f"Missing required permissions: {', '.join(missing)}",
                    required_permission=missing[0]
                )

            return await func(*args, **kwargs)

        return wrapper
    return decorator


def require_role(role_code: str):
    """
    Decorator to require a minimum role level.

    Uses role priority to determine if user's role is sufficient.

    Usage:
        @app.get("/admin/settings")
        @require_role("admin")
        async def admin_settings(request: Request):
            ...

    Args:
        role_code: Minimum required role code
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                raise HTTPException(
                    status_code=500,
                    detail="Request object not found in endpoint"
                )

            user = _get_user_from_request(request)
            if user is None:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )

            user_role = user.get("role", "user")

            # Get role priorities
            required_role = DEFAULT_ROLES.get(role_code)
            actual_role = DEFAULT_ROLES.get(user_role)

            if required_role is None:
                raise HTTPException(
                    status_code=500,
                    detail=f"Invalid required role: {role_code}"
                )

            if actual_role is None:
                # Unknown role - treat as lowest priority
                logger.warning(f"Unknown role '{user_role}' for user")
                raise RoleRequired(role_code)

            if actual_role.priority < required_role.priority:
                logger.warning(
                    f"Role check failed: user_role={user_role} "
                    f"(priority={actual_role.priority}), "
                    f"required={role_code} (priority={required_role.priority})"
                )
                raise RoleRequired(role_code)

            return await func(*args, **kwargs)

        return wrapper
    return decorator


def require_own_resource(
    permission_code: str,
    owner_param: str = "user_id",
    allow_admin: bool = True
):
    """
    Decorator for "own resource" permission checks.

    Checks if user has either:
    1. Full permission (e.g., "documents:delete")
    2. Own permission (e.g., "documents:delete_own") AND owns the resource

    Usage:
        @app.delete("/documents/{document_id}")
        @require_own_resource("documents:delete", owner_param="owner_id")
        async def delete_document(request: Request, document_id: int, owner_id: int):
            ...

    Args:
        permission_code: Base permission code (without "_own" suffix)
        owner_param: Name of the parameter containing resource owner ID
        allow_admin: Whether to allow users with manage permission
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                raise HTTPException(
                    status_code=500,
                    detail="Request object not found in endpoint"
                )

            user = _get_user_from_request(request)
            if user is None:
                raise HTTPException(
                    status_code=401,
                    detail="Authentication required"
                )

            user_id = user.get("id") or user.get("user_id")
            user_role = user.get("role", "user")
            db_pool = getattr(request.app.state, "db_pool", None)

            # Check full permission first
            has_full = await check_permission(
                user_id=user_id,
                user_role=user_role,
                permission_code=permission_code,
                db_pool=db_pool
            )

            if has_full:
                return await func(*args, **kwargs)

            # Check own permission with resource ownership
            own_permission = f"{permission_code}_own"
            resource_owner_id = kwargs.get(owner_param)

            if resource_owner_id is not None:
                has_own = await check_permission(
                    user_id=user_id,
                    user_role=user_role,
                    permission_code=own_permission,
                    resource_owner_id=resource_owner_id,
                    db_pool=db_pool
                )

                if has_own and resource_owner_id == user_id:
                    return await func(*args, **kwargs)

            # Check manage permission if allowed
            if allow_admin:
                parts = permission_code.split(":")
                if len(parts) == 2:
                    manage_perm = f"{parts[0]}:manage"
                    has_manage = await check_permission(
                        user_id=user_id,
                        user_role=user_role,
                        permission_code=manage_perm,
                        db_pool=db_pool
                    )
                    if has_manage:
                        return await func(*args, **kwargs)

            raise PermissionDenied(
                detail=f"Permission '{permission_code}' or '{own_permission}' (as resource owner) is required",
                required_permission=permission_code
            )

        return wrapper
    return decorator


# =============================================================================
# DEPENDENCY INJECTION HELPERS
# =============================================================================

class PermissionDependency:
    """
    FastAPI dependency for permission checking.

    Usage:
        @app.get("/documents")
        async def list_documents(
            request: Request,
            _: None = Depends(PermissionDependency("documents:read"))
        ):
            ...
    """

    def __init__(self, permission_code: str):
        self.permission_code = permission_code

    async def __call__(self, request: Request):
        user = _get_user_from_request(request)
        if user is None:
            raise HTTPException(
                status_code=401,
                detail="Authentication required"
            )

        user_id = user.get("id") or user.get("user_id")
        user_role = user.get("role", "user")
        db_pool = getattr(request.app.state, "db_pool", None)

        has_perm = await check_permission(
            user_id=user_id,
            user_role=user_role,
            permission_code=self.permission_code,
            db_pool=db_pool
        )

        if not has_perm:
            raise PermissionDenied(
                detail=f"Permission '{self.permission_code}' is required",
                required_permission=self.permission_code
            )


class RoleDependency:
    """
    FastAPI dependency for role checking.

    Usage:
        @app.get("/admin/dashboard")
        async def admin_dashboard(
            request: Request,
            _: None = Depends(RoleDependency("admin"))
        ):
            ...
    """

    def __init__(self, role_code: str):
        self.role_code = role_code

    async def __call__(self, request: Request):
        user = _get_user_from_request(request)
        if user is None:
            raise HTTPException(
                status_code=401,
                detail="Authentication required"
            )

        user_role = user.get("role", "user")

        required_role = DEFAULT_ROLES.get(self.role_code)
        actual_role = DEFAULT_ROLES.get(user_role)

        if required_role is None:
            raise HTTPException(
                status_code=500,
                detail=f"Invalid required role: {self.role_code}"
            )

        if actual_role is None or actual_role.priority < required_role.priority:
            raise RoleRequired(self.role_code)

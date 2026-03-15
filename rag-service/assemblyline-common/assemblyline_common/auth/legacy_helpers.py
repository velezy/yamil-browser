"""
Legacy auth helpers from services/shared/auth.

These provide simple JWT extraction from request headers,
used by services that don't need the full CurrentUser/CurrentTenant
dependency injection system.
"""

import os
from typing import Optional

from fastapi import Request, HTTPException
import jwt

# JWT configuration from environment
SECRET_KEY = os.getenv("JWT_SECRET", "drivesentinel-jwt-secret-change-in-production")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


def get_user_id_from_request(request: Request) -> Optional[int]:
    """
    Extract user_id from JWT token in Authorization header.

    Returns None if no valid token found (for optional auth endpoints).
    Use require_user_id() for endpoints that MUST have a user.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token = auth_header.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str = payload.get("sub")
        if user_id_str is None:
            return None
        return int(user_id_str)
    except (jwt.InvalidTokenError, ValueError):
        return None


def require_user_id(request: Request) -> int:
    """
    Extract user_id from JWT token, raising HTTPException if not found.

    Use this for endpoints that require authentication.
    """
    user_id = get_user_id_from_request(request)
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return user_id


def get_user_info_from_request(request: Request) -> Optional[dict]:
    """
    Extract full user info from JWT token in Authorization header.

    Returns dict with user_id, organization_id, department, etc.
    Returns None if no valid token found.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token = auth_header.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str = payload.get("sub")
        if user_id_str is None:
            return None
        return {
            "user_id": int(user_id_str),
            "email": payload.get("email"),
            "organization_id": payload.get("organization_id"),
            "department": payload.get("department"),
            "is_admin": payload.get("is_admin", False),
            "is_superadmin": payload.get("is_superadmin", False),
            "license_tier": payload.get("license_tier"),
        }
    except (jwt.InvalidTokenError, ValueError):
        return None


def require_user_info(request: Request) -> dict:
    """
    Extract full user info from JWT token, raising HTTPException if not found.

    Use this for endpoints that need organization_id, department, etc.
    """
    user_info = get_user_info_from_request(request)
    if user_info is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return user_info


def get_user_permissions_from_request(request: Request) -> set:
    """
    Extract user permissions from JWT token.

    Returns a set of permission codes.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return set()

    token = auth_header.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        permissions = payload.get("permissions", [])
        return set(permissions) if permissions else set()
    except (jwt.InvalidTokenError, ValueError):
        return set()


def require_permission(request: Request, permission: str) -> dict:
    """
    Require a specific permission, raising HTTPException if not granted.

    Returns user_info if permission is granted.
    """
    user_info = require_user_info(request)

    # Superadmin has all permissions
    if user_info.get("is_superadmin"):
        return user_info

    permissions = get_user_permissions_from_request(request)

    # system:admin grants all permissions
    if "system:admin" in permissions:
        return user_info

    if permission not in permissions:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: {permission} required"
        )

    return user_info


def has_permission(request: Request, permission: str) -> bool:
    """
    Check if user has a specific permission.

    Returns True if granted, False otherwise.
    Does not raise exceptions.
    """
    user_info = get_user_info_from_request(request)
    if not user_info:
        return False

    # Superadmin has all permissions
    if user_info.get("is_superadmin"):
        return True

    permissions = get_user_permissions_from_request(request)

    # system:admin grants all permissions
    if "system:admin" in permissions:
        return True

    return permission in permissions


def is_enterprise_tier(request: Request) -> bool:
    """
    Check if user's organization has an enterprise license tier.
    """
    user_info = get_user_info_from_request(request)
    if not user_info:
        return False

    license_tier = user_info.get("license_tier", "")
    enterprise_tiers = {
        "enterprise", "enterprise_s", "enterprise_m",
        "enterprise_l", "enterprise_unlimited", "developer"
    }
    return license_tier in enterprise_tiers


__all__ = [
    "get_user_id_from_request",
    "require_user_id",
    "get_user_info_from_request",
    "require_user_info",
    "get_user_permissions_from_request",
    "require_permission",
    "has_permission",
    "is_enterprise_tier",
    "SECRET_KEY",
    "ALGORITHM",
]

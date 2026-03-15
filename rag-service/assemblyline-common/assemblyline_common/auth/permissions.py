"""
DriveSentinel Permission System
Backend permission checks for defense in depth security

This module provides FastAPI dependencies that verify user permissions
before allowing access to sensitive endpoints. Even if frontend is bypassed,
these checks ensure unauthorized users cannot access admin functionality.

Usage:
    from assemblyline_common.permissions import require_superadmin, require_admin, require_org_admin

    @app.get("/admin/sensitive")
    async def sensitive_endpoint(user: dict = Depends(require_superadmin)):
        # Only superadmins can reach this code
        pass
"""

from functools import wraps
from typing import Optional, List, Callable
from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
import logging

logger = logging.getLogger(__name__)

# OAuth2 scheme - reused from auth service
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


class PermissionDenied(HTTPException):
    """Custom exception for permission denied errors"""
    def __init__(self, detail: str = "Permission denied"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail
        )


class InsufficientTier(HTTPException):
    """Custom exception for license tier restrictions"""
    def __init__(self, required_tier: str):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"This feature requires {required_tier} license tier or higher"
        )


# =============================================================================
# PERMISSION DEPENDENCY FACTORIES
# =============================================================================

def require_superadmin(current_user: dict) -> dict:
    """
    Dependency that requires superadmin role.
    Use for: System settings, prompts, models, knowledge graph, memory, company admin

    Usage:
        @app.get("/admin/endpoint")
        async def endpoint(user: dict = Depends(require_superadmin)):
            pass
    """
    if not current_user.get("is_superadmin", False):
        logger.warning(f"Permission denied: User {current_user.get('email')} attempted superadmin action")
        raise PermissionDenied("This action requires system administrator privileges")
    return current_user


def require_admin(current_user: dict) -> dict:
    """
    Dependency that requires admin or superadmin role.
    Use for: Organization management, user management within org

    Usage:
        @app.get("/org/endpoint")
        async def endpoint(user: dict = Depends(require_admin)):
            pass
    """
    if not current_user.get("is_admin", False):
        logger.warning(f"Permission denied: User {current_user.get('email')} attempted admin action")
        raise PermissionDenied("This action requires administrator privileges")
    return current_user


def require_manager(current_user: dict) -> dict:
    """
    Dependency that requires manager, admin, or superadmin role.
    Use for: Team management, report access
    """
    if not current_user.get("is_manager", False):
        logger.warning(f"Permission denied: User {current_user.get('email')} attempted manager action")
        raise PermissionDenied("This action requires manager privileges")
    return current_user


def require_active(current_user: dict) -> dict:
    """
    Dependency that requires active user status.
    Use for: Any authenticated endpoint
    """
    if not current_user.get("is_active", True):
        logger.warning(f"Permission denied: Inactive user {current_user.get('email')} attempted action")
        raise PermissionDenied("Your account has been deactivated")
    return current_user


# =============================================================================
# LICENSE TIER CHECKS
# =============================================================================

class LicenseTierRequired:
    """
    Dependency class for checking license tier requirements.

    Usage:
        @app.get("/enterprise/feature")
        async def feature(user: dict = Depends(LicenseTierRequired("enterprise"))):
            pass
    """

    TIER_HIERARCHY = {
        "free": 0,
        "consumer": 1,
        "consumer_plus": 2,
        "pro": 3,
        "enterprise_s": 4,
        "enterprise_m": 5,
        "enterprise_l": 6,
        "enterprise_unlimited": 7,
        "developer": 10,  # Developer has access to everything
    }

    def __init__(self, required_tier: str):
        self.required_tier = required_tier.lower()
        self.required_level = self.TIER_HIERARCHY.get(self.required_tier, 0)

    def __call__(self, current_user: dict) -> dict:
        user_tier = current_user.get("license_tier", "free").lower()
        user_level = self.TIER_HIERARCHY.get(user_tier, 0)

        if user_level < self.required_level:
            logger.warning(
                f"License tier denied: User {current_user.get('email')} "
                f"(tier={user_tier}) attempted to access {self.required_tier} feature"
            )
            raise InsufficientTier(self.required_tier)

        return current_user


# Convenience instances for common tier checks
require_enterprise = LicenseTierRequired("enterprise_s")
require_pro = LicenseTierRequired("pro")


# =============================================================================
# FEATURE FLAG CHECKS
# =============================================================================

class FeatureRequired:
    """
    Dependency class for checking if a feature is enabled in the license.

    Usage:
        @app.get("/ai/claude")
        async def claude_endpoint(user: dict = Depends(FeatureRequired("anthropic_api"))):
            pass
    """

    def __init__(self, feature: str):
        self.feature = feature

    def __call__(self, current_user: dict) -> dict:
        features = current_user.get("license_features", [])

        # Superadmins bypass feature checks
        if current_user.get("is_superadmin", False):
            return current_user

        if self.feature not in features:
            logger.warning(
                f"Feature denied: User {current_user.get('email')} "
                f"attempted to access feature '{self.feature}'"
            )
            raise PermissionDenied(f"Your license does not include the '{self.feature}' feature")

        return current_user


# Common feature checks
require_cloud_ai = FeatureRequired("cloud_ai")
require_ollama = FeatureRequired("ollama")
require_aws_bedrock = FeatureRequired("aws_bedrock")
require_azure_ai = FeatureRequired("azure_ai")
require_anthropic = FeatureRequired("anthropic_api")
require_openai = FeatureRequired("openai_api")


# =============================================================================
# ORGANIZATION CHECKS
# =============================================================================

class SameOrganization:
    """
    Dependency that ensures the target resource belongs to the user's organization.
    Prevents cross-organization data access.

    Usage:
        @app.get("/org/{org_id}/data")
        async def get_org_data(
            org_id: int,
            user: dict = Depends(SameOrganization())
        ):
            pass
    """

    def __init__(self, org_id_param: str = "org_id"):
        self.org_id_param = org_id_param

    async def __call__(self, current_user: dict, **kwargs) -> dict:
        user_org_id = current_user.get("organization_id")
        target_org_id = kwargs.get(self.org_id_param)

        # Superadmins can access any organization
        if current_user.get("is_superadmin", False):
            return current_user

        if user_org_id != target_org_id:
            logger.warning(
                f"Cross-org access denied: User {current_user.get('email')} "
                f"(org={user_org_id}) attempted to access org={target_org_id}"
            )
            raise PermissionDenied("You cannot access resources from another organization")

        return current_user


# =============================================================================
# COMBINED PERMISSION CHECKS
# =============================================================================

def require_superadmin_check(current_user: dict) -> dict:
    """Direct check function for use after get_current_user"""
    require_active(current_user)
    return require_superadmin(current_user)


def require_admin_check(current_user: dict) -> dict:
    """Direct check function for use after get_current_user"""
    require_active(current_user)
    return require_admin(current_user)


def require_manager_check(current_user: dict) -> dict:
    """Direct check function for use after get_current_user"""
    require_active(current_user)
    return require_manager(current_user)


# =============================================================================
# AUDIT LOGGING
# =============================================================================

def log_admin_action(action: str):
    """
    Decorator to log admin actions for audit trail.

    Usage:
        @app.post("/admin/delete-user")
        @log_admin_action("delete_user")
        async def delete_user(user_id: int, admin: dict = Depends(require_superadmin)):
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract user from kwargs (from Depends)
            user = None
            for key, value in kwargs.items():
                if isinstance(value, dict) and "email" in value:
                    user = value
                    break

            logger.info(
                f"ADMIN ACTION: {action} by {user.get('email') if user else 'unknown'} "
                f"- args: {args}, kwargs: {list(kwargs.keys())}"
            )

            return await func(*args, **kwargs)
        return wrapper
    return decorator

"""
OAuth 2.0 Scopes Enforcement.

Provides:
- Scope-based access control
- Dynamic scope resolution
- Token introspection
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, List, Set, Dict, Any, Callable
from functools import wraps
from enum import Enum

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

logger = logging.getLogger(__name__)


class ScopeType(Enum):
    """Types of scopes."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"


@dataclass
class ScopeConfig:
    """Configuration for OAuth scope enforcement."""
    # Scope hierarchy (higher scopes include lower ones)
    scope_hierarchy: Dict[str, Set[str]] = field(default_factory=lambda: {
        "admin": {"admin", "write", "read", "delete"},
        "write": {"write", "read"},
        "read": {"read"},
        "delete": {"delete", "read"},
    })

    # Resource-specific scopes
    resource_scopes: Dict[str, Dict[str, Set[str]]] = field(default_factory=lambda: {
        "flows": {
            "flows:read": {"read"},
            "flows:write": {"write", "read"},
            "flows:delete": {"delete", "read"},
            "flows:admin": {"admin", "write", "read", "delete"},
        },
        "messages": {
            "messages:read": {"read"},
            "messages:write": {"write", "read"},
        },
        "users": {
            "users:read": {"read"},
            "users:write": {"write", "read"},
            "users:admin": {"admin", "write", "read", "delete"},
        },
        "tenants": {
            "tenants:read": {"read"},
            "tenants:admin": {"admin", "write", "read", "delete"},
        },
        "api_keys": {
            "api_keys:read": {"read"},
            "api_keys:write": {"write", "read"},
            "api_keys:delete": {"delete", "read"},
        },
    })

    # Token introspection endpoint (optional)
    introspection_endpoint: Optional[str] = None
    introspection_client_id: Optional[str] = None
    introspection_client_secret: Optional[str] = None

    # Caching
    cache_ttl_seconds: int = 300

    # Behavior
    require_all_scopes: bool = False  # If True, require ALL scopes. If False, require ANY.


@dataclass
class ScopeValidationResult:
    """Result of scope validation."""
    valid: bool
    missing_scopes: Set[str] = field(default_factory=set)
    granted_scopes: Set[str] = field(default_factory=set)
    message: Optional[str] = None


class OAuthScopeEnforcer:
    """
    OAuth 2.0 scope enforcement.

    Features:
    - Hierarchical scope resolution
    - Resource-specific scopes
    - Token introspection support
    - Scope caching
    """

    def __init__(self, config: Optional[ScopeConfig] = None):
        self.config = config or ScopeConfig()
        self._scope_cache: Dict[str, Set[str]] = {}

    def expand_scopes(self, scopes: Set[str]) -> Set[str]:
        """
        Expand scopes based on hierarchy.

        For example, if user has 'admin' scope, they implicitly have 'read' and 'write'.

        Args:
            scopes: Set of scope strings

        Returns:
            Expanded set of scopes
        """
        expanded = set()

        for scope in scopes:
            # Add the scope itself
            expanded.add(scope)

            # Check if it's a hierarchical scope
            if scope in self.config.scope_hierarchy:
                expanded.update(self.config.scope_hierarchy[scope])

            # Check resource-specific scopes
            for resource, resource_scopes in self.config.resource_scopes.items():
                if scope in resource_scopes:
                    for implied_scope in resource_scopes[scope]:
                        expanded.add(f"{resource}:{implied_scope}")
                        expanded.add(implied_scope)

        return expanded

    def validate_scopes(
        self,
        granted_scopes: Set[str],
        required_scopes: Set[str],
        require_all: Optional[bool] = None
    ) -> ScopeValidationResult:
        """
        Validate if granted scopes satisfy required scopes.

        Args:
            granted_scopes: Scopes the user has
            required_scopes: Scopes required for the action
            require_all: If True, require ALL scopes. If None, use config default.

        Returns:
            ScopeValidationResult with validation status
        """
        if require_all is None:
            require_all = self.config.require_all_scopes

        # Expand granted scopes
        expanded_granted = self.expand_scopes(granted_scopes)

        # Check required scopes
        missing = set()
        matched = set()

        for required in required_scopes:
            if required in expanded_granted:
                matched.add(required)
            else:
                missing.add(required)

        # Determine validity
        if require_all:
            valid = len(missing) == 0
        else:
            valid = len(matched) > 0 or len(required_scopes) == 0

        message = None
        if not valid:
            if require_all:
                message = f"Missing required scopes: {', '.join(missing)}"
            else:
                message = f"None of the required scopes found: {', '.join(required_scopes)}"

        return ScopeValidationResult(
            valid=valid,
            missing_scopes=missing,
            granted_scopes=expanded_granted,
            message=message
        )

    def check_resource_access(
        self,
        granted_scopes: Set[str],
        resource: str,
        action: str
    ) -> ScopeValidationResult:
        """
        Check if user has access to perform an action on a resource.

        Args:
            granted_scopes: User's granted scopes
            resource: Resource name (e.g., 'flows', 'messages')
            action: Action name (e.g., 'read', 'write', 'delete')

        Returns:
            ScopeValidationResult with validation status
        """
        # Build required scope
        required_scope = f"{resource}:{action}"

        # Also accept generic scopes
        required_scopes = {
            required_scope,
            action,
            f"{resource}:admin",
            "admin"
        }

        return self.validate_scopes(granted_scopes, required_scopes, require_all=False)

    async def introspect_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Introspect a token using the configured introspection endpoint.

        Args:
            token: Access token to introspect

        Returns:
            Token metadata if valid, None if invalid
        """
        if not self.config.introspection_endpoint:
            return None

        try:
            import httpx

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.config.introspection_endpoint,
                    data={"token": token},
                    auth=(
                        self.config.introspection_client_id,
                        self.config.introspection_client_secret
                    ) if self.config.introspection_client_id else None
                )

                if response.status_code == 200:
                    data = response.json()
                    if data.get("active"):
                        return data

        except Exception as e:
            logger.error(f"Token introspection failed: {e}")

        return None


# Global instance
_enforcer: Optional[OAuthScopeEnforcer] = None


def get_scope_enforcer(config: Optional[ScopeConfig] = None) -> OAuthScopeEnforcer:
    """Get or create the scope enforcer instance."""
    global _enforcer
    if _enforcer is None:
        _enforcer = OAuthScopeEnforcer(config)
    return _enforcer


# FastAPI dependency decorator
def require_scopes(
    *required_scopes: str,
    require_all: bool = False,
    config: Optional[ScopeConfig] = None
):
    """
    FastAPI dependency to require specific OAuth scopes.

    Usage:
        @app.get("/flows")
        async def list_flows(
            _: None = Depends(require_scopes("flows:read"))
        ):
            ...

    Args:
        *required_scopes: Required scope strings
        require_all: If True, require ALL scopes. If False, require ANY.
        config: Optional custom configuration

    Returns:
        FastAPI dependency
    """
    async def scope_checker(request: Request):
        # Get scopes from request state (set by auth middleware)
        granted_scopes = getattr(request.state, "scopes", set())

        if isinstance(granted_scopes, list):
            granted_scopes = set(granted_scopes)

        # Validate scopes
        enforcer = get_scope_enforcer(config)
        result = enforcer.validate_scopes(
            granted_scopes,
            set(required_scopes),
            require_all=require_all
        )

        if not result.valid:
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "insufficient_scope",
                    "message": result.message,
                    "required_scopes": list(required_scopes),
                    "missing_scopes": list(result.missing_scopes)
                }
            )

        return result

    return scope_checker  # Return callable, caller wraps with Depends()


def require_resource_access(
    resource: str,
    action: str,
    config: Optional[ScopeConfig] = None
):
    """
    FastAPI dependency to require resource access.

    Usage:
        @app.delete("/flows/{flow_id}")
        async def delete_flow(
            flow_id: str,
            _: None = Depends(require_resource_access("flows", "delete"))
        ):
            ...

    Args:
        resource: Resource name
        action: Action name
        config: Optional custom configuration

    Returns:
        FastAPI dependency
    """
    async def resource_checker(request: Request):
        granted_scopes = getattr(request.state, "scopes", set())

        if isinstance(granted_scopes, list):
            granted_scopes = set(granted_scopes)

        enforcer = get_scope_enforcer(config)
        result = enforcer.check_resource_access(granted_scopes, resource, action)

        if not result.valid:
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "insufficient_scope",
                    "message": f"Access denied: {action} on {resource}",
                    "resource": resource,
                    "action": action
                }
            )

        return result

    return resource_checker  # Return callable, caller wraps with Depends()

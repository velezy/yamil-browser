"""
ACL (Access Control List) Policy

Kong ACL plugin equivalent for fine-grained access control.
Control API access based on groups, roles, and permissions.

Features:
- Group-based access control
- Role hierarchy
- Resource-level permissions
- Whitelist/blacklist modes
- Per-route ACL rules
- Integration with JWT claims
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

from fastapi import FastAPI, Request, HTTPException, Depends, status
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class ACLMode(str, Enum):
    """ACL enforcement mode."""
    WHITELIST = "whitelist"  # Only allow listed groups
    BLACKLIST = "blacklist"  # Allow all except listed groups


class PermissionType(str, Enum):
    """Types of permissions."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    EXECUTE = "execute"
    ALL = "*"


@dataclass
class ACLRule:
    """Single ACL rule."""
    # Route pattern (supports * wildcard)
    route_pattern: str

    # Allowed groups (for whitelist) or denied groups (for blacklist)
    groups: List[str] = field(default_factory=list)

    # Required permissions
    permissions: List[PermissionType] = field(default_factory=list)

    # HTTP methods this rule applies to (empty = all)
    methods: List[str] = field(default_factory=list)

    # Mode for this specific rule
    mode: ACLMode = ACLMode.WHITELIST

    # Require all groups (AND) or any group (OR)
    require_all_groups: bool = False

    # Require all permissions (AND) or any permission (OR)
    require_all_permissions: bool = True

    # Priority (lower = checked first)
    priority: int = 100


@dataclass
class ACLConfig:
    """Configuration for ACL policy."""
    # Default mode
    mode: ACLMode = ACLMode.WHITELIST

    # Global rules (applied to all routes)
    global_rules: List[ACLRule] = field(default_factory=list)

    # Route-specific rules
    route_rules: List[ACLRule] = field(default_factory=list)

    # Group hierarchy (group -> parent groups)
    group_hierarchy: Dict[str, List[str]] = field(default_factory=dict)

    # Role definitions (role -> permissions)
    roles: Dict[str, List[PermissionType]] = field(default_factory=lambda: {
        "viewer": [PermissionType.READ],
        "editor": [PermissionType.READ, PermissionType.WRITE],
        "admin": [PermissionType.READ, PermissionType.WRITE, PermissionType.DELETE, PermissionType.ADMIN],
        "super_admin": [PermissionType.ALL],
    })

    # Claim name in JWT for groups
    groups_claim: str = "groups"

    # Claim name in JWT for roles
    roles_claim: str = "roles"

    # Allow unauthenticated access by default
    allow_anonymous: bool = False

    # Anonymous group name
    anonymous_group: str = "anonymous"

    # Log access decisions
    log_decisions: bool = True


@dataclass
class ACLCheckResult:
    """Result of ACL check."""
    allowed: bool
    matched_rule: Optional[ACLRule] = None
    user_groups: List[str] = field(default_factory=list)
    user_permissions: List[PermissionType] = field(default_factory=list)
    reason: Optional[str] = None


class ACLPolicy:
    """
    Kong-style ACL policy.

    Usage:
        acl = ACLPolicy(ACLConfig(
            mode=ACLMode.WHITELIST,
            route_rules=[
                ACLRule(
                    route_pattern="/api/v1/admin/*",
                    groups=["admins", "super_admins"],
                    permissions=[PermissionType.ADMIN],
                ),
                ACLRule(
                    route_pattern="/api/v1/flows/*",
                    groups=["developers", "admins"],
                    permissions=[PermissionType.READ, PermissionType.WRITE],
                ),
            ],
        ))

        # Check access
        result = await acl.check(request)
        if not result.allowed:
            raise HTTPException(403, result.reason)

        # Use as dependency
        @app.get("/admin/users")
        async def get_users(_: ACLCheckResult = Depends(acl.dependency)):
            return {"users": [...]}
    """

    def __init__(self, config: Optional[ACLConfig] = None):
        self.config = config or ACLConfig()
        self._sorted_rules: List[ACLRule] = []
        self._compile_rules()

    def _compile_rules(self):
        """Compile and sort rules by priority."""
        all_rules = self.config.global_rules + self.config.route_rules
        self._sorted_rules = sorted(all_rules, key=lambda r: r.priority)

    def _expand_groups(self, groups: List[str]) -> Set[str]:
        """
        Expand groups with hierarchy.

        If user is in 'admins' and 'admins' has parent 'users',
        then user effectively has both groups.
        """
        expanded = set(groups)

        for group in groups:
            if group in self.config.group_hierarchy:
                parent_groups = self.config.group_hierarchy[group]
                expanded.update(parent_groups)
                # Recursively expand
                expanded.update(self._expand_groups(parent_groups))

        return expanded

    def _get_role_permissions(self, roles: List[str]) -> Set[PermissionType]:
        """Get all permissions for given roles."""
        permissions = set()

        for role in roles:
            if role in self.config.roles:
                role_perms = self.config.roles[role]
                if PermissionType.ALL in role_perms:
                    return {PermissionType.ALL}
                permissions.update(role_perms)

        return permissions

    def _match_route(self, pattern: str, path: str) -> bool:
        """Check if route pattern matches path."""
        import re

        # Convert glob pattern to regex
        # * matches any single path segment
        # ** matches multiple path segments
        regex = pattern.replace("**", "§§")  # Temp placeholder
        regex = regex.replace("*", "[^/]+")
        regex = regex.replace("§§", ".*")
        regex = f"^{regex}$"

        return bool(re.match(regex, path))

    def _check_rule(
        self,
        rule: ACLRule,
        path: str,
        method: str,
        user_groups: Set[str],
        user_permissions: Set[PermissionType],
    ) -> Optional[bool]:
        """
        Check if rule matches and returns access decision.

        Returns:
            True = allow, False = deny, None = rule doesn't apply
        """
        # Check route pattern
        if not self._match_route(rule.route_pattern, path):
            return None

        # Check method
        if rule.methods and method.upper() not in [m.upper() for m in rule.methods]:
            return None

        # Check groups
        rule_groups = set(rule.groups)

        if rule.require_all_groups:
            # User must have ALL specified groups
            group_match = rule_groups.issubset(user_groups)
        else:
            # User must have ANY specified group
            group_match = bool(rule_groups & user_groups) or not rule_groups

        # Check permissions
        rule_permissions = set(rule.permissions)

        if PermissionType.ALL in user_permissions:
            permission_match = True
        elif rule.require_all_permissions:
            # User must have ALL specified permissions
            permission_match = rule_permissions.issubset(user_permissions) or not rule_permissions
        else:
            # User must have ANY specified permission
            permission_match = bool(rule_permissions & user_permissions) or not rule_permissions

        # Determine access based on mode
        if rule.mode == ACLMode.WHITELIST:
            # Must have matching groups AND permissions
            return group_match and permission_match
        else:
            # If matching groups, deny; otherwise allow
            return not group_match

    async def check(
        self,
        request: Request,
        groups: Optional[List[str]] = None,
        roles: Optional[List[str]] = None,
    ) -> ACLCheckResult:
        """
        Check ACL for request.

        Args:
            request: FastAPI request
            groups: User groups (if not provided, extracted from request.state)
            roles: User roles (if not provided, extracted from request.state)

        Returns:
            ACLCheckResult
        """
        path = request.url.path
        method = request.method

        # Get user groups from various sources
        if groups is None:
            # Try request.state (set by auth middleware)
            groups = getattr(request.state, "groups", None)

            # Try JWT claims
            if groups is None:
                user = getattr(request.state, "user", None)
                if user and isinstance(user, dict):
                    groups = user.get(self.config.groups_claim, [])

            # Default to anonymous
            if groups is None:
                if self.config.allow_anonymous:
                    groups = [self.config.anonymous_group]
                else:
                    return ACLCheckResult(
                        allowed=False,
                        reason="Authentication required",
                    )

        # Get user roles
        if roles is None:
            roles = getattr(request.state, "roles", None)

            if roles is None:
                user = getattr(request.state, "user", None)
                if user and isinstance(user, dict):
                    roles = user.get(self.config.roles_claim, [])

            if roles is None:
                roles = []

        # Expand groups with hierarchy
        user_groups = self._expand_groups(groups)

        # Get permissions from roles
        user_permissions = self._get_role_permissions(roles)

        # Also check for explicit permissions in request.state
        explicit_perms = getattr(request.state, "permissions", [])
        if explicit_perms:
            user_permissions.update(
                PermissionType(p) if isinstance(p, str) else p
                for p in explicit_perms
            )

        # Check rules in order
        for rule in self._sorted_rules:
            result = self._check_rule(
                rule, path, method, user_groups, user_permissions
            )

            if result is not None:
                if self.config.log_decisions:
                    logger.debug(
                        f"ACL decision: {'allow' if result else 'deny'}",
                        extra={
                            "event_type": "acl_decision",
                            "allowed": result,
                            "path": path,
                            "method": method,
                            "rule": rule.route_pattern,
                            "user_groups": list(user_groups),
                        }
                    )

                return ACLCheckResult(
                    allowed=result,
                    matched_rule=rule,
                    user_groups=list(user_groups),
                    user_permissions=list(user_permissions),
                    reason=None if result else f"Access denied by rule: {rule.route_pattern}",
                )

        # No rule matched - use default mode
        if self.config.mode == ACLMode.WHITELIST:
            # Whitelist mode: deny by default
            return ACLCheckResult(
                allowed=False,
                user_groups=list(user_groups),
                user_permissions=list(user_permissions),
                reason="No matching ACL rule - access denied",
            )
        else:
            # Blacklist mode: allow by default
            return ACLCheckResult(
                allowed=True,
                user_groups=list(user_groups),
                user_permissions=list(user_permissions),
            )

    async def dependency(self, request: Request) -> ACLCheckResult:
        """FastAPI dependency for ACL checking."""
        result = await self.check(request)

        if not result.allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result.reason or "Access denied",
            )

        return result

    def require_groups(self, *groups: str):
        """
        Decorator to require specific groups.

        Usage:
            @app.get("/admin")
            @acl.require_groups("admins", "super_admins")
            async def admin_endpoint():
                return {"status": "ok"}
        """
        def decorator(func):
            async def wrapper(request: Request, *args, **kwargs):
                result = await self.check(request)

                required = set(groups)
                user_groups = set(result.user_groups)

                if not (required & user_groups):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Requires one of groups: {', '.join(groups)}",
                    )

                return await func(request, *args, **kwargs)
            return wrapper
        return decorator

    def require_permissions(self, *permissions: PermissionType):
        """
        Decorator to require specific permissions.

        Usage:
            @app.delete("/resource/{id}")
            @acl.require_permissions(PermissionType.DELETE)
            async def delete_resource(id: str):
                return {"deleted": id}
        """
        def decorator(func):
            async def wrapper(request: Request, *args, **kwargs):
                result = await self.check(request)

                required = set(permissions)
                user_perms = set(result.user_permissions)

                if PermissionType.ALL not in user_perms:
                    if not (required & user_perms):
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Requires permissions: {', '.join(p.value for p in permissions)}",
                        )

                return await func(request, *args, **kwargs)
            return wrapper
        return decorator

    def attach(self, app: FastAPI):
        """Attach ACL as middleware."""
        acl = self

        class ACLMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Skip health endpoints
                if request.url.path in ("/health", "/health/live", "/health/ready"):
                    return await call_next(request)

                result = await acl.check(request)

                if not result.allowed:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=result.reason,
                    )

                # Store result in request state
                request.state.acl_result = result

                return await call_next(request)

        app.add_middleware(ACLMiddleware)
        logger.info("ACL middleware attached")


# Preset configurations
ACL_PRESETS = {
    "basic": ACLConfig(
        mode=ACLMode.WHITELIST,
        route_rules=[
            ACLRule(route_pattern="/api/v1/**", groups=["users", "admins"]),
            ACLRule(route_pattern="/api/v1/admin/**", groups=["admins"]),
        ],
    ),
    "rbac": ACLConfig(
        mode=ACLMode.WHITELIST,
        roles={
            "viewer": [PermissionType.READ],
            "operator": [PermissionType.READ, PermissionType.EXECUTE],
            "developer": [PermissionType.READ, PermissionType.WRITE, PermissionType.EXECUTE],
            "admin": [PermissionType.READ, PermissionType.WRITE, PermissionType.DELETE, PermissionType.ADMIN],
            "super_admin": [PermissionType.ALL],
        },
        group_hierarchy={
            "admins": ["developers"],
            "developers": ["operators"],
            "operators": ["viewers"],
        },
    ),
    "healthcare": ACLConfig(
        mode=ACLMode.WHITELIST,
        route_rules=[
            ACLRule(
                route_pattern="/api/v1/phi/**",
                groups=["clinical", "compliance"],
                permissions=[PermissionType.READ],
            ),
            ACLRule(
                route_pattern="/api/v1/phi/**",
                groups=["clinical"],
                permissions=[PermissionType.WRITE],
                methods=["POST", "PUT", "PATCH"],
            ),
            ACLRule(
                route_pattern="/api/v1/admin/**",
                groups=["admins", "compliance"],
            ),
            ACLRule(
                route_pattern="/api/v1/audit/**",
                groups=["compliance", "admins"],
                permissions=[PermissionType.READ],
            ),
        ],
        group_hierarchy={
            "super_admins": ["admins", "compliance"],
            "admins": ["clinical"],
        },
        log_decisions=True,
    ),
}


# Singleton
_acl_policy: Optional[ACLPolicy] = None


def get_acl_policy(config: Optional[ACLConfig] = None) -> ACLPolicy:
    """Get or create ACL policy singleton."""
    global _acl_policy

    if _acl_policy is None:
        _acl_policy = ACLPolicy(config)

    return _acl_policy


def acl_from_preset(preset_name: str, **overrides) -> ACLPolicy:
    """Create ACL policy from preset."""
    if preset_name not in ACL_PRESETS:
        raise ValueError(f"Unknown preset: {preset_name}")

    config = ACL_PRESETS[preset_name]

    for key, value in overrides.items():
        if hasattr(config, key):
            setattr(config, key, value)

    return ACLPolicy(config)

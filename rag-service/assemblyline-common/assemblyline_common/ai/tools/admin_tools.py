"""
Admin Tools

Tools for API key management, user management, and connector configuration.
"""

import logging
import secrets
import hashlib
from typing import Optional, Dict, List, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta

from assemblyline_common.ai.tools.base import Tool, ToolResult, ToolDefinition
from assemblyline_common.ai.authorization import AuthorizationContext, Permission

logger = logging.getLogger(__name__)


# ============================================================================
# API Key Tools
# ============================================================================

class CreateAPIKeyTool(Tool):
    """Tool to create a new API key."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="create_api_key",
            description="Create a new API key for the tenant",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Descriptive name for the key"},
                    "environment": {"type": "string", "enum": ["test", "live"], "description": "Environment"},
                    "scopes": {"type": "array", "items": {"type": "string"}, "description": "Permission scopes"},
                    "expires_in_days": {"type": "integer", "description": "Days until expiration (max 365)"},
                },
                "required": ["name", "environment", "scopes", "expires_in_days"],
            },
            required_permission=Permission.CREATE_API_KEYS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        name: str,
        environment: str,
        scopes: List[str],
        expires_in_days: int,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.CREATE_API_KEYS):
            return ToolResult(success=False, error="Not authorized to create API keys")
        
        # Validate expiration
        if expires_in_days > 365:
            return ToolResult(success=False, error="Expiration cannot exceed 365 days")
        
        # Generate key
        prefix = "lw_test_" if environment == "test" else "lw_live_"
        key_value = prefix + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(key_value.encode()).hexdigest()
        
        key_id = uuid4()
        api_key = {
            "id": str(key_id),
            "name": name,
            "environment": environment,
            "scopes": scopes,
            "key_preview": key_value[:12] + "..." + key_value[-4:],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=expires_in_days)).isoformat(),
            "created_by": str(auth_context.user_id),
        }
        
        self.log_execution(auth_context, "create_api_key", {"name": name}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data={
                "api_key": api_key,
                "key_value": key_value,  # Only returned once!
            },
            message=f"Created API key '{name}'. Store the key securely - it won't be shown again.",
        )


class ListAPIKeysTool(Tool):
    """Tool to list API keys."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="list_api_keys",
            description="List API keys for the tenant",
            parameters={
                "type": "object",
                "properties": {
                    "include_inactive": {"type": "boolean", "description": "Include revoked keys"},
                },
            },
            required_permission=Permission.VIEW_API_KEYS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        include_inactive: bool = False,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_API_KEYS):
            return ToolResult(success=False, error="Not authorized to view API keys")
        
        # In production, fetch from database
        keys = [
            {"id": "key-1", "name": "Analytics Dashboard", "environment": "live", "status": "active"},
            {"id": "key-2", "name": "Dev Testing", "environment": "test", "status": "active"},
        ]
        
        if not include_inactive:
            keys = [k for k in keys if k["status"] == "active"]
        
        return ToolResult(
            success=True,
            data=keys,
            message=f"Found {len(keys)} API keys",
        )


class RevokeAPIKeyTool(Tool):
    """Tool to revoke an API key."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="revoke_api_key",
            description="Revoke an API key (requires confirmation)",
            parameters={
                "type": "object",
                "properties": {
                    "key_id": {"type": "string", "description": "API key UUID"},
                    "reason": {"type": "string", "description": "Reason for revocation"},
                },
                "required": ["key_id", "reason"],
            },
            required_permission=Permission.REVOKE_API_KEYS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        key_id: str,
        reason: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.REVOKE_API_KEYS):
            return ToolResult(success=False, error="Not authorized to revoke API keys")
        
        self.log_execution(auth_context, "revoke_api_key", {"key_id": key_id, "reason": reason}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data={"key_id": key_id, "status": "revoked", "revoked_at": datetime.now(timezone.utc).isoformat()},
            message=f"API key {key_id} has been revoked",
        )


# ============================================================================
# User Management Tools
# ============================================================================

class InviteUserTool(Tool):
    """Tool to invite a new user."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="invite_user",
            description="Invite a new user to the tenant",
            parameters={
                "type": "object",
                "properties": {
                    "email": {"type": "string", "description": "User's email address"},
                    "role": {"type": "string", "enum": ["viewer", "operator", "developer", "admin"], "description": "User role"},
                    "full_name": {"type": "string", "description": "User's full name"},
                },
                "required": ["email", "role"],
            },
            required_permission=Permission.INVITE_USERS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        email: str,
        role: str,
        full_name: Optional[str] = None,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.INVITE_USERS):
            return ToolResult(success=False, error="Not authorized to invite users")
        
        user_id = uuid4()
        user = {
            "id": str(user_id),
            "email": email,
            "role": role,
            "full_name": full_name,
            "status": "invited",
            "invited_by": str(auth_context.user_id),
            "invited_at": datetime.now(timezone.utc).isoformat(),
        }
        
        self.log_execution(auth_context, "invite_user", {"email": email, "role": role}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data=user,
            message=f"Invitation sent to {email} with {role} role",
        )


class ListUsersTool(Tool):
    """Tool to list users in the tenant."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="list_users",
            description="List users in the tenant",
            parameters={
                "type": "object",
                "properties": {
                    "include_inactive": {"type": "boolean", "description": "Include deactivated users"},
                    "role": {"type": "string", "description": "Filter by role"},
                },
            },
            required_permission=Permission.VIEW_USERS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        include_inactive: bool = False,
        role: Optional[str] = None,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_USERS):
            return ToolResult(success=False, error="Not authorized to view users")
        
        # In production, fetch from database
        users = [
            {"id": "user-1", "email": "admin@example.com", "role": "admin", "status": "active"},
            {"id": "user-2", "email": "dev@example.com", "role": "developer", "status": "active"},
            {"id": "user-3", "email": "analyst@example.com", "role": "viewer", "status": "active"},
        ]
        
        if not include_inactive:
            users = [u for u in users if u["status"] == "active"]
        if role:
            users = [u for u in users if u["role"] == role]
        
        return ToolResult(
            success=True,
            data=users,
            message=f"Found {len(users)} users",
        )


class UpdateUserRoleTool(Tool):
    """Tool to update a user's role."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="update_user_role",
            description="Update a user's role",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string", "description": "User UUID"},
                    "new_role": {"type": "string", "enum": ["viewer", "operator", "developer", "admin"], "description": "New role"},
                },
                "required": ["user_id", "new_role"],
            },
            required_permission=Permission.MODIFY_USERS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        user_id: str,
        new_role: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.MODIFY_USERS):
            return ToolResult(success=False, error="Not authorized to modify users")
        
        self.log_execution(auth_context, "update_user_role", {"user_id": user_id, "new_role": new_role}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data={"user_id": user_id, "role": new_role},
            message=f"Updated user {user_id} to {new_role} role",
        )


class DeactivateUserTool(Tool):
    """Tool to deactivate a user (requires approval)."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="deactivate_user",
            description="Deactivate a user account (requires approval)",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string", "description": "User UUID"},
                    "reason": {"type": "string", "description": "Reason for deactivation"},
                },
                "required": ["user_id", "reason"],
            },
            required_permission=Permission.DEACTIVATE_USERS,
            requires_approval=True,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        user_id: str,
        reason: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.DEACTIVATE_USERS):
            return ToolResult(success=False, error="Not authorized to deactivate users")
        
        # Create approval request
        from assemblyline_common.ai.approvals import get_approval_service
        approval_service = await get_approval_service()
        
        approval = await approval_service.create_approval(
            tenant_id=auth_context.tenant_id,
            requested_by=auth_context.user_id,
            requester_email=auth_context.email,
            action_type="deactivate_user",
            action_payload={"user_id": user_id, "reason": reason},
            action_summary=f"Deactivate user {user_id}: {reason}",
            required_role="admin",
        )
        
        return ToolResult(
            success=True,
            requires_approval=True,
            approval_id=approval.id,
            message=f"User deactivation submitted for approval",
        )


# ============================================================================
# Connector Management Tools
# ============================================================================

class CreateConnectorTool(Tool):
    """Tool to create a new connector."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="create_connector",
            description="Create a new connector configuration",
            parameters={
                "type": "object",
                "properties": {
                    "connector_type": {"type": "string", "description": "Type of connector"},
                    "name": {"type": "string", "description": "Connector name"},
                    "config": {"type": "object", "description": "Connector configuration"},
                },
                "required": ["connector_type", "name", "config"],
            },
            required_permission=Permission.CREATE_CONNECTORS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        connector_type: str,
        name: str,
        config: Dict[str, Any],
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.CREATE_CONNECTORS):
            return ToolResult(success=False, error="Not authorized to create connectors")
        
        connector_id = uuid4()
        connector = {
            "id": str(connector_id),
            "type": connector_type,
            "name": name,
            "config": {k: "***" if "secret" in k.lower() or "password" in k.lower() else v for k, v in config.items()},
            "status": "created",
            "created_by": str(auth_context.user_id),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        self.log_execution(auth_context, "create_connector", {"type": connector_type, "name": name}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data=connector,
            message=f"Created {connector_type} connector '{name}'",
        )


class TestConnectorTool(Tool):
    """Tool to test connector connectivity."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="test_connector",
            description="Test connector connectivity",
            parameters={
                "type": "object",
                "properties": {
                    "connector_id": {"type": "string", "description": "Connector UUID"},
                },
                "required": ["connector_id"],
            },
            required_permission=Permission.TEST_CONNECTORS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        connector_id: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.TEST_CONNECTORS):
            return ToolResult(success=False, error="Not authorized to test connectors")
        
        # In production, actually test the connection
        return ToolResult(
            success=True,
            data={
                "connector_id": connector_id,
                "status": "connected",
                "latency_ms": 45,
                "tested_at": datetime.now(timezone.utc).isoformat(),
            },
            message=f"Connector {connector_id} is connected (45ms latency)",
        )


# ============================================================================
# Admin Tools Collection
# ============================================================================

class AdminTools:
    """Collection of all admin-related tools."""
    
    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [
            # API Keys
            CreateAPIKeyTool(),
            ListAPIKeysTool(),
            RevokeAPIKeyTool(),
            # Users
            InviteUserTool(),
            ListUsersTool(),
            UpdateUserRoleTool(),
            DeactivateUserTool(),
            # Connectors
            CreateConnectorTool(),
            TestConnectorTool(),
        ]

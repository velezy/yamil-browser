"""
Base Tool Classes

Provides the foundation for all AI agent tools with:
- Authorization checking
- Audit logging
- Error handling
- Result formatting
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Type
from uuid import UUID

from assemblyline_common.ai.authorization import AuthorizationContext, Permission

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ToolResult:
    """Result from a tool execution."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    requires_approval: bool = False
    approval_id: Optional[UUID] = None
    message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "requires_approval": self.requires_approval,
            "approval_id": str(self.approval_id) if self.approval_id else None,
            "message": self.message,
        }


@dataclass
class ToolDefinition:
    """Definition of a tool for AI model."""
    name: str
    description: str
    parameters: Dict[str, Any]
    required_permission: Optional[Permission] = None
    requires_approval: bool = False


# ============================================================================
# Base Tool Class
# ============================================================================

class Tool(ABC):
    """
    Base class for AI agent tools.
    
    All tools must implement:
    - get_definition(): Returns tool schema for AI model
    - execute(): Performs the tool action
    """
    
    @abstractmethod
    def get_definition(self) -> ToolDefinition:
        """Get the tool definition for the AI model."""
        pass
    
    @abstractmethod
    async def execute(
        self,
        auth_context: AuthorizationContext,
        **kwargs,
    ) -> ToolResult:
        """Execute the tool with the given parameters."""
        pass
    
    def check_authorization(
        self,
        auth_context: AuthorizationContext,
        required_permission: Permission,
    ) -> bool:
        """Check if user is authorized to use this tool."""
        from assemblyline_common.ai.authorization import get_authorization_service
        auth_service = get_authorization_service()
        return auth_service.can_perform(auth_context, required_permission)
    
    def log_execution(
        self,
        auth_context: AuthorizationContext,
        tool_name: str,
        parameters: Dict[str, Any],
        result: ToolResult,
    ):
        """Log tool execution for audit."""
        logger.info(
            f"Tool executed: {tool_name}",
            extra={
                "event_type": "ai.tool_executed",
                "tool_name": tool_name,
                "user_id": str(auth_context.user_id),
                "tenant_id": str(auth_context.tenant_id),
                "success": result.success,
                "requires_approval": result.requires_approval,
            }
        )


# ============================================================================
# Tool Registry
# ============================================================================

class ToolRegistry:
    """
    Registry of available tools for AI agents.
    
    Manages tool registration and lookup by agent type.
    """
    
    def __init__(self):
        self._tools: Dict[str, Dict[str, Tool]] = {
            "flow-builder": {},
            "admin": {},
            "analysis": {},
            "query": {},
            "general": {},
        }
    
    def register(
        self,
        agent_type: str,
        tool_name: str,
        tool: Tool,
    ):
        """Register a tool for an agent type."""
        if agent_type not in self._tools:
            self._tools[agent_type] = {}
        self._tools[agent_type][tool_name] = tool
    
    def get_tools(self, agent_type: str) -> Dict[str, Tool]:
        """Get all tools for an agent type."""
        return self._tools.get(agent_type, {})
    
    def get_tool(self, agent_type: str, tool_name: str) -> Optional[Tool]:
        """Get a specific tool."""
        return self._tools.get(agent_type, {}).get(tool_name)
    
    def get_definitions(self, agent_type: str) -> List[ToolDefinition]:
        """Get tool definitions for AI model."""
        tools = self.get_tools(agent_type)
        return [tool.get_definition() for tool in tools.values()]


# ============================================================================
# Singleton Factory
# ============================================================================

_tool_registry: Optional[ToolRegistry] = None


def get_tool_registry() -> ToolRegistry:
    """Get singleton instance of tool registry."""
    global _tool_registry
    if _tool_registry is None:
        _tool_registry = ToolRegistry()
    return _tool_registry

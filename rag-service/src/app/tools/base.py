"""
Base Tool Class

Abstract base for all tools in the agent pipeline.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext

logger = logging.getLogger(__name__)


@dataclass
class ToolResult:
    """Result from a tool execution"""
    success: bool
    data: Any
    error: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    # For visualizations
    html: Optional[str] = None
    image_base64: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "metadata": self.metadata,
            "has_html": self.html is not None,
            "has_image": self.image_base64 is not None,
        }


class BaseTool(ABC):
    """
    Abstract base class for all tools.

    Tools are invoked by the agent pipeline when specific capabilities
    are needed (calculations, visualizations, data queries, etc.)
    """

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.logger = logging.getLogger(f"tool.{name}")

    @abstractmethod
    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Execute the tool with the given context.

        Args:
            context: Agent context with query and retrieved data

        Returns:
            ToolResult with execution output
        """
        pass

    def get_info(self) -> dict:
        """Get tool information"""
        return {
            "name": self.name,
            "description": self.description,
        }

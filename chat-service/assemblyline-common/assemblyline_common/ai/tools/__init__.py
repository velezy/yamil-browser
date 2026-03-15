"""
AI Tools Module - Tools available to AI agents

Each tool represents an action an AI agent can perform,
with proper authorization and audit logging.
"""

from assemblyline_common.ai.tools.base import (
    Tool,
    ToolResult,
    ToolDefinition,
    ToolRegistry,
    get_tool_registry,
)
from assemblyline_common.ai.tools.flow_tools import FlowTools, NODE_TYPES
from assemblyline_common.ai.tools.admin_tools import AdminTools
from assemblyline_common.ai.tools.analysis_tools import AnalysisTools
from assemblyline_common.ai.tools.query_tools import QueryTools
from assemblyline_common.ai.tools.general_tools import GeneralTools
from assemblyline_common.ai.tools.browser_rpa_tool import BrowserRPATools
from assemblyline_common.ai.tools.canvas_vision_tool import CanvasVisionTools
from assemblyline_common.ai.tools.learn_ui_tool import LearnUITools
from assemblyline_common.ai.tools.jira_tool import JiraTools
from assemblyline_common.ai.tools.cdc_tools import CDCTools


def initialize_tool_registry() -> ToolRegistry:
    """Initialize the tool registry with all available tools."""
    registry = get_tool_registry()

    # Register Flow Builder Agent tools
    for tool in FlowTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("flow-builder", definition.name, tool)

    # Register Admin Agent tools
    for tool in AdminTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("admin", definition.name, tool)

    # Register Analysis Agent tools
    for tool in AnalysisTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("analysis", definition.name, tool)

    # Register Query Agent tools
    for tool in QueryTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("query", definition.name, tool)

    # Register General Agent tools (URL fetching, etc.)
    for tool in GeneralTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("general", definition.name, tool)

    # Register Browser RPA tools (browser automation for the General agent)
    for tool in BrowserRPATools.get_all_tools():
        definition = tool.get_definition()
        registry.register("general", definition.name, tool)

    # Register Canvas Vision tools (AI's internal browser for viewing the app)
    for tool in CanvasVisionTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("general", definition.name, tool)
        registry.register("flow-builder", definition.name, tool)

    # Register Learn UI tools (AI's draft learning journal)
    for tool in LearnUITools.get_all_tools():
        definition = tool.get_definition()
        registry.register("general", definition.name, tool)
        registry.register("flow-builder", definition.name, tool)

    # Register Jira tools (project management, issue tracking)
    for tool in JiraTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("general", definition.name, tool)
        registry.register("flow-builder", definition.name, tool)

    # Register CDC tools (CDC Hub monitoring and delivery tracking)
    for tool in CDCTools.get_all_tools():
        definition = tool.get_definition()
        registry.register("query", definition.name, tool)
        registry.register("general", definition.name, tool)

    # Register general tools (subset available to all agents)
    general_tools = [
        # Include search and view tools for all agents
        *[t for t in QueryTools.get_all_tools() if "search" in t.get_definition().name or "get" in t.get_definition().name],
    ]
    for tool in general_tools:
        definition = tool.get_definition()
        registry.register("general", definition.name, tool)

    return registry


__all__ = [
    "Tool",
    "ToolResult",
    "ToolDefinition",
    "ToolRegistry",
    "get_tool_registry",
    "initialize_tool_registry",
    "FlowTools",
    "AdminTools",
    "AnalysisTools",
    "QueryTools",
    "GeneralTools",
    "BrowserRPATools",
    "CanvasVisionTools",
    "LearnUITools",
    "JiraTools",
    "CDCTools",
    "NODE_TYPES",
]

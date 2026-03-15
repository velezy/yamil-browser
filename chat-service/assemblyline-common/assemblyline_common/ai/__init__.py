"""
AI Orchestra Module - Enterprise Healthcare AI Agent System

Provides AI-powered automation with HIPAA compliance:
- PHI masking before AI model calls
- Authorization and approval workflows
- Multiple specialized agents (Flow Builder, Admin, Analysis, Query)
- Token budget management
- Multi-agent orchestration with handoffs
- Conversation memory and context
- Feedback collection and quality tracking
"""

from assemblyline_common.ai.phi_guard import (
    PHIGuard,
    PHIToken,
    MaskedContent,
    get_phi_guard,
)
from assemblyline_common.ai.authorization import (
    Permission,
    Role,
    AuthorizationContext,
    AuthorizationService,
    get_authorization_service,
)
from assemblyline_common.ai.approvals import (
    ApprovalStatus,
    PendingApproval,
    ApprovalService,
    get_approval_service,
)
from assemblyline_common.ai.orchestrator import (
    AgentType,
    AIOrchestrator,
    get_orchestrator,
)
from assemblyline_common.ai.memory import (
    ConversationMemory,
    ConversationContext,
    TokenBudgetManager,
    get_conversation_memory,
    get_token_budget_manager,
)
from assemblyline_common.ai.feedback import (
    FeedbackRating,
    FeedbackCategory,
    AIFeedback,
    FeedbackService,
    ResponseQualityTracker,
    get_feedback_service,
    get_quality_tracker,
)
from assemblyline_common.ai.multi_agent import (
    AgentRole,
    HandoffReason,
    AgentHandoff,
    AgentTask,
    AgentResult,
    MultiAgentOrchestrator,
    CollaborationPlanner,
    CollaborationPattern,
    get_multi_agent_orchestrator,
    get_collaboration_planner,
)
from assemblyline_common.ai.tools import (
    Tool,
    ToolResult,
    ToolRegistry,
    get_tool_registry,
    initialize_tool_registry,
    FlowTools,
    AdminTools,
    AnalysisTools,
    QueryTools,
)
from assemblyline_common.ai.chat_history import (
    ChatHistoryService,
    get_chat_history_service,
)
from assemblyline_common.ai.ai_memory import (
    AIMemoryService,
    get_ai_memory_service,
)
from assemblyline_common.ai.audit_client import (
    AIAuditClient,
    get_ai_audit_client,
)
from assemblyline_common.ai.ml_cache import (
    MLResultCache,
    make_key,
    get_suggestion_cache,
    get_mapping_cache,
    MappingFeedbackStore,
    get_feedback_store,
    IgnoreListStore,
    get_ignore_list_store,
    DEFAULT_SUGGESTION_TTL,
    DEFAULT_FIELD_MAPPING_TTL,
)

__all__ = [
    # PHI Guard
    "PHIGuard",
    "PHIToken",
    "MaskedContent",
    "get_phi_guard",
    # Authorization
    "Permission",
    "Role",
    "AuthorizationContext",
    "AuthorizationService",
    "get_authorization_service",
    # Approvals
    "ApprovalStatus",
    "PendingApproval",
    "ApprovalService",
    "get_approval_service",
    # Orchestrator
    "AgentType",
    "AIOrchestrator",
    "get_orchestrator",
    # Memory
    "ConversationMemory",
    "ConversationContext",
    "TokenBudgetManager",
    "get_conversation_memory",
    "get_token_budget_manager",
    # Feedback
    "FeedbackRating",
    "FeedbackCategory",
    "AIFeedback",
    "FeedbackService",
    "ResponseQualityTracker",
    "get_feedback_service",
    "get_quality_tracker",
    # Multi-Agent
    "AgentRole",
    "HandoffReason",
    "AgentHandoff",
    "AgentTask",
    "AgentResult",
    "MultiAgentOrchestrator",
    "CollaborationPlanner",
    "CollaborationPattern",
    "get_multi_agent_orchestrator",
    "get_collaboration_planner",
    # Tools
    "Tool",
    "ToolResult",
    "ToolRegistry",
    "get_tool_registry",
    "initialize_tool_registry",
    "FlowTools",
    "AdminTools",
    "AnalysisTools",
    "QueryTools",
    # Chat History
    "ChatHistoryService",
    "get_chat_history_service",
    # AI Memory
    "AIMemoryService",
    "get_ai_memory_service",
    # Audit Client
    "AIAuditClient",
    "get_ai_audit_client",
    # ML Cache
    "MLResultCache",
    "make_key",
    "get_suggestion_cache",
    "get_mapping_cache",
    "MappingFeedbackStore",
    "get_feedback_store",
    "IgnoreListStore",
    "get_ignore_list_store",
    "DEFAULT_SUGGESTION_TTL",
    "DEFAULT_FIELD_MAPPING_TTL",
    # ML Helper
    "invoke_ai_with_phi_guard",
    # Tool Definitions
    "AI_TOOLS",
]


import json
import logging as _logging
from typing import List, Dict, Any, Optional

_ml_logger = _logging.getLogger(__name__ + ".ml")


# ============================================================================
# Tool Definitions for Bedrock Tool Calling
# ============================================================================

AI_TOOLS: List[Dict[str, Any]] = [
    {
        "toolSpec": {
            "name": "fetch_url",
            "description": "Fetch content from a URL. Use to read OpenAPI/Swagger specs, FHIR IGs, or API documentation pages.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "URL to fetch"},
                        "format_hint": {
                            "type": "string",
                            "enum": ["auto", "json", "yaml", "html"],
                            "description": "Expected format (default: auto-detect)",
                        },
                    },
                    "required": ["url"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "browser_rpa",
            "description": "Open a browser, navigate pages, fill forms, click buttons, and extract content. Use for JS-rendered pages or sites requiring login. When fetch_url returns only an HTML shell without meaningful content, use this tool instead.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "session_action": {
                            "type": "string",
                            "enum": ["start", "continue", "close"],
                            "description": "start=new browser session, continue=use existing session, close=end session"
                        },
                        "steps": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "enum": ["navigate", "click", "fill", "select", "wait", "screenshot", "extract", "scroll"]
                                    },
                                    "selector": {"type": "string", "description": "CSS selector or text to find element"},
                                    "value": {"type": "string", "description": "Value for fill/select, or direction for scroll"},
                                    "url": {"type": "string", "description": "URL for navigate action"},
                                    "wait_for": {"type": "string", "description": "Text or selector to wait for"},
                                    "timeout": {"type": "number", "description": "Timeout in seconds (default 10)"}
                                },
                                "required": ["action"]
                            },
                            "description": "Sequence of browser actions to execute"
                        },
                        "credential_id": {
                            "type": "string",
                            "description": "ID of stored RPA credential for auto-login"
                        }
                    },
                    "required": ["session_action", "steps"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "create_api_request",
            "description": "Create a new API request in the API Builder. Use this when the user provides API details, OpenAPI specs, or asks to create an endpoint.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Name of the API request"},
                        "method": {"type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"], "description": "HTTP method"},
                        "url": {"type": "string", "description": "The API endpoint URL"},
                        "headers": {"type": "object", "description": "HTTP headers as key-value pairs"},
                        "queryParams": {"type": "object", "description": "Query parameters as key-value pairs"},
                        "body": {"type": "string", "description": "Request body (JSON string for POST/PUT/PATCH)"},
                        "bodyType": {"type": "string", "enum": ["none", "json", "form", "raw"], "description": "Type of request body"},
                        "description": {"type": "string", "description": "Description of what this API does"},
                        "auth": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["none", "bearer", "basic", "api_key"]},
                                "credentials": {"type": "object"}
                            }
                        }
                    },
                    "required": ["name", "method", "url"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "navigate",
            "description": (
                "Navigate to any page in the application. Use when user asks to go to, open, or navigate to a page. "
                "Common paths: / (dashboard), /messages (executions), /flows (flow list), /api-builder, "
                "/ai-prompts, /libraries, /schema-editor, /yql-engine, /ytl-engine, /connectors (settings), "
                "/database-inspector, /api-keys, /users, /tenants, /activity. "
                "You can also navigate to any valid path — check the sidebar links in the page snapshot "
                "or use discover_capabilities to find available routes."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The path to navigate to (e.g., '/flows', '/connectors', '/users')"
                        }
                    },
                    "required": ["path"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "create_flow",
            "description": "Create an integration flow with connected nodes. Use when user asks to create, build, or add a flow with nodes.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "nodes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Array of node IDs to add in sequence (e.g., ['http-trigger', 'hl7-write', 'http-output'])"
                        }
                    },
                    "required": ["nodes"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "clear_canvas",
            "description": "Clear all nodes from the flow canvas. Use when user asks to clear, remove all, or start fresh.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {}
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "update_node_properties",
            "description": "Update properties/configuration of a node on the canvas.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "node_label": {"type": "string", "description": "Display label of the node (e.g., 'HTTP Listener')"},
                        "properties": {"type": "object", "description": "Key-value pairs of properties to set"}
                    },
                    "required": ["node_label", "properties"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "compile_yql",
            "description": "Compile a YQL (YAMIL Query Language) query to SQL for a specific database dialect.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "yql": {"type": "object", "description": "The YQL query object with SELECT, FROM, WHERE, etc."},
                        "dialect": {"type": "string", "enum": ["databricks", "postgresql", "snowflake", "mysql"], "description": "Target SQL dialect"}
                    },
                    "required": ["yql", "dialect"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "transform_ytl",
            "description": "Transform data using YTL (YAMIL Transform Language).",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "data": {"type": "object", "description": "Input data to transform"},
                        "spec": {"type": "object", "description": "YTL transformation specification"}
                    },
                    "required": ["data", "spec"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "test_run",
            "description": "Run a test on the current flow with sample payload data.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "payload": {"type": "object", "description": "Sample JSON payload for testing"}
                    },
                    "required": ["payload"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "save_flow",
            "description": "Save the current flow.",
            "inputSchema": {
                "json": {"type": "object", "properties": {}}
            }
        }
    },
    {
        "toolSpec": {
            "name": "delete_flow",
            "description": "Delete the current flow. Use when the user asks to delete, remove, or trash the flow they are currently viewing. Always confirm with the user before calling this tool.",
            "inputSchema": {
                "json": {"type": "object", "properties": {}}
            }
        }
    },
    {
        "toolSpec": {
            "name": "add_node",
            "description": "Add one or more nodes to the EXISTING flow on the canvas without clearing it. Use when the user asks to add nodes to a flow that already has nodes.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "nodes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Array of node type IDs to add (e.g., ['hl7-write', 'email-send'])"
                        }
                    },
                    "required": ["nodes"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "ui_action",
            "description": "Interact with UI elements on the current page: click buttons, fill inputs, select dropdowns, toggle checkboxes. Use this instead of text-based [UI_ACTION] blocks.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "actions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "enum": ["click", "fill", "select", "check", "uncheck"],
                                        "description": "Action to perform"
                                    },
                                    "selector_type": {
                                        "type": "string",
                                        "enum": ["text", "label", "id", "css", "placeholder", "role"],
                                        "description": "How to find the element"
                                    },
                                    "selector": {
                                        "type": "string",
                                        "description": "The selector value (button text, input label, element ID, CSS selector, etc.)"
                                    },
                                    "value": {
                                        "type": "string",
                                        "description": "Value for fill/select actions"
                                    }
                                },
                                "required": ["action", "selector_type", "selector"]
                            },
                            "description": "Sequence of UI actions to perform"
                        }
                    },
                    "required": ["actions"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "publish_to_gateway",
            "description": "Publish the current flow as an API endpoint on the API Gateway. Sets security policy and rate limiting.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "security_policy": {
                            "type": "string",
                            "enum": ["jwt", "api_key", "none"],
                            "description": "Authentication method for the API endpoint"
                        },
                        "rate_limit_tier": {
                            "type": "string",
                            "enum": ["standard", "premium", "unlimited"],
                            "description": "Rate limiting tier"
                        }
                    }
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "create_consumer",
            "description": "Create an API consumer who can access published API endpoints.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "username": {"type": "string", "description": "Consumer username"},
                        "description": {"type": "string", "description": "Description of the consumer"},
                        "auth_type": {
                            "type": "string",
                            "enum": ["api_key", "jwt"],
                            "description": "Authentication type for this consumer"
                        }
                    },
                    "required": ["username", "auth_type"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "grant_route_access",
            "description": "Grant an API consumer access to a specific published route.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "route_name": {"type": "string", "description": "Name of the published route"},
                        "consumer_username": {"type": "string", "description": "Username of the consumer to grant access"}
                    },
                    "required": ["route_name", "consumer_username"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "set_test_input",
            "description": "Set the test panel JSON input for flow testing. Use before test_run to provide sample data.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "json": {"type": "string", "description": "JSON string to set as test input"}
                    },
                    "required": ["json"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "delete_node",
            "description": "Delete a node from the current flow by its label. The label must match a node currently on the canvas.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "node_label": {"type": "string", "description": "Display label of the node to delete (must match exactly)"}
                    },
                    "required": ["node_label"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "connect_nodes",
            "description": "Connect two nodes on the canvas with an edge. Source and target must match node labels exactly.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string", "description": "Label of the source node"},
                        "target": {"type": "string", "description": "Label of the target node"},
                        "connectionType": {
                            "type": "string",
                            "enum": ["default", "success", "error"],
                            "description": "Type of connection. Use 'error' to connect a node's red error handle to an Error Handler node. Default: 'default'"
                        }
                    },
                    "required": ["source", "target"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "disconnect_node",
            "description": "Remove all edges (connections) from a node on the canvas.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "node_label": {"type": "string", "description": "Label of the node to disconnect"}
                    },
                    "required": ["node_label"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "rename_flow",
            "description": "Rename the current flow.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "New name for the flow"}
                    },
                    "required": ["name"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "auto_arrange",
            "description": "Auto-arrange/layout nodes on the canvas for better visual organization.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "layout": {
                            "type": "string",
                            "enum": ["horizontal", "vertical", "tree", "grid", "radial"],
                            "description": "Layout style to apply"
                        }
                    },
                    "required": ["layout"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "suggest_prompt_update",
            "description": "Suggest an update to AI prompt knowledge in Settings > AI Prompts. Use when you learn something new that should be remembered.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "prompt_key": {"type": "string", "description": "The AI prompt key to update (e.g., 'agent-flow-builder-connectors')"},
                        "addition": {"type": "string", "description": "The text to add to the prompt"},
                        "reason": {"type": "string", "description": "Why this update is needed"}
                    },
                    "required": ["prompt_key", "addition", "reason"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "email_template",
            "description": "Create or modify an email template. Templates can be applied to email-send nodes.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["create", "update", "preview"],
                            "description": "Operation type"
                        },
                        "payload": {
                            "type": "object",
                            "description": "Template data: name, subject, body, bodyType, category, variables, etc.",
                            "properties": {
                                "id": {"type": "string", "description": "Template ID (required for update)"},
                                "name": {"type": "string", "description": "Template name"},
                                "subject": {"type": "string", "description": "Email subject line"},
                                "body": {"type": "string", "description": "Email body (HTML or plain text)"},
                                "bodyType": {"type": "string", "enum": ["html", "text"]},
                                "category": {"type": "string", "description": "Template category"},
                                "variables": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Template variables (e.g., ['patientName', 'appointmentDate'])"
                                }
                            }
                        }
                    },
                    "required": ["type", "payload"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "build_flow_from_api",
            "description": "Build a complete flow from an API request created in the API Builder. Generates trigger, transform, and output nodes automatically.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Name for the new flow"},
                        "nodes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Node type IDs for the flow"
                        },
                        "api_request_id": {"type": "string", "description": "ID of the API request to build from (optional)"}
                    },
                    "required": ["name", "nodes"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "list_connectors",
            "description": (
                "Query the database for all saved connectors in the tenant. Returns each connector's name, type, "
                "status, and non-secret config fields (host, port, bucket, region, etc.). Use this to verify what "
                "connectors exist and what their actual configuration is before configuring nodes. "
                "Passwords and encrypted fields are redacted."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "connector_type": {
                            "type": "string",
                            "description": "Optional filter by type (e.g., 'sftp', 's3', 'smtp', 'databricks')"
                        }
                    }
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "audit_flow_connectors",
            "description": (
                "Audit the current flow's connector nodes against the database. Reads the flow_definition from "
                "the database, extracts every node that uses authMethod='connector', then cross-references each "
                "node's connectorName with the actual connector records. Reports: missing connectors, mismatched "
                "types, empty required fields, stale credential fields that should be empty, and duplicate/conflicting "
                "config values. Use this when a user reports connector issues or asks you to verify flow properties."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "flow_id": {
                            "type": "string",
                            "description": "UUID of the flow to audit. If omitted, uses the current flow from context."
                        }
                    }
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "discover_capabilities",
            "description": (
                "Discover what the system can do right now. Returns the current catalog of available node types "
                "(triggers, processing, outputs, connectors), available connector types, available AI prompts, "
                "and available tools. Use this when you encounter an unfamiliar node type, need to know what "
                "nodes are available for building flows, or want to check if a feature exists. This queries the "
                "live system — results reflect the actual current state, not hardcoded documentation."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "enum": ["all", "node_types", "connector_types", "ai_prompts", "tools", "routes"],
                            "description": "What to discover. Default: 'all'"
                        }
                    }
                }
            }
        }
    },
    # ==========================================================================
    # Phase 3 — CRUD API, Verification, and Schema Discovery Tools
    # ==========================================================================
    {
        "toolSpec": {
            "name": "create_connector",
            "description": (
                "Create a new connector in the database. Replaces the 5-step UI flow "
                "(navigate → click New → select type → fill form → save). "
                "Requires admin role or above. Sensitive fields in config are auto-encrypted."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Unique connector name (e.g., 'Hospital_SFTP')"},
                        "connector_type": {
                            "type": "string",
                            "description": "Connector type: sftp, s3, smtp, http, kafka, databricks, epic_fhir, database"
                        },
                        "config": {
                            "type": "object",
                            "description": "Connection configuration (host, port, username, password, bucket, region, etc.)"
                        },
                        "description": {"type": "string", "description": "Human-readable description"},
                        "category": {"type": "string", "description": "Category (default: 'general')"},
                        "encrypted_fields": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Field names in config that should be encrypted (e.g., ['password', 'secretAccessKey'])"
                        },
                        "is_shared": {"type": "boolean", "description": "Whether the connector is shared across flows (default: true)"},
                    },
                    "required": ["name", "connector_type", "config"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "update_connector",
            "description": (
                "Update an existing connector's configuration. Use connector ID (UUID) or name. "
                "Only provided fields are updated — omitted fields are unchanged. "
                "Requires admin role or above."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "connector_id": {"type": "string", "description": "UUID of the connector to update"},
                        "connector_name": {"type": "string", "description": "Name of the connector (alternative to ID)"},
                        "name": {"type": "string", "description": "New name (optional)"},
                        "config": {"type": "object", "description": "Updated config fields (merged with existing)"},
                        "description": {"type": "string", "description": "Updated description"},
                        "encrypted_fields": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Updated list of fields to encrypt"
                        },
                        "is_active": {"type": "boolean", "description": "Enable/disable the connector"},
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "test_connector",
            "description": (
                "Test connectivity for a saved connector. Attempts a real connection "
                "(socket connect for SFTP, list-buckets for S3, EHLO for SMTP, etc.) "
                "and returns success/failure with details."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "connector_id": {"type": "string", "description": "UUID of the connector to test"},
                        "connector_name": {"type": "string", "description": "Name of the connector (alternative to ID)"},
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "query_executions",
            "description": (
                "Query flow execution history. Filter by flow ID, status, date range. "
                "Returns execution ID, status, duration, error message, and step logs. "
                "Use when users ask 'why did my flow fail?' or 'show me recent executions'."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "flow_id": {"type": "string", "description": "UUID of the flow to query executions for"},
                        "status": {
                            "type": "string",
                            "enum": ["pending", "running", "completed", "failed"],
                            "description": "Filter by execution status"
                        },
                        "limit": {"type": "integer", "description": "Max results to return (default: 10, max: 50)"},
                        "correlation_id": {"type": "string", "description": "Filter by correlation ID for tracing"},
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "verify_action",
            "description": (
                "Read-back verification: after performing an action (save flow, create connector, "
                "update node), verify the result by reading from the database. Returns the current "
                "persisted state so you can confirm your changes were saved correctly."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "entity_type": {
                            "type": "string",
                            "enum": ["flow", "connector", "execution"],
                            "description": "Type of entity to verify"
                        },
                        "entity_id": {"type": "string", "description": "UUID of the entity to verify"},
                        "entity_name": {"type": "string", "description": "Name of the entity (alternative to ID, for connectors)"},
                    },
                    "required": ["entity_type"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_connector_schema",
            "description": (
                "Get the required and optional fields for a connector type. Use this to discover "
                "what configuration a connector needs before creating one. Works for any connector "
                "type including custom ones added by developers."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "connector_type": {
                            "type": "string",
                            "description": "Connector type to get schema for (e.g., 'sftp', 's3', 'smtp', 'kafka', 'epic_fhir', 'databricks', 'http', 'database')"
                        }
                    },
                    "required": ["connector_type"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "manage_user",
            "description": (
                "Create, update, lock, or unlock a user account. Use for user management tasks "
                "like creating new users, changing roles, resetting passwords, or locking/unlocking accounts. "
                "Requires admin role."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["create", "update", "lock", "unlock", "reset_password", "list"],
                            "description": (
                                "create: add a new user. update: change user fields (role, name, status). "
                                "lock: lock a user account. unlock: unlock a locked account. "
                                "reset_password: force password reset on next login. "
                                "list: list all users in the tenant."
                            )
                        },
                        "email": {
                            "type": "string",
                            "description": "User email (required for create, used as identifier for other actions)"
                        },
                        "user_id": {
                            "type": "string",
                            "description": "User UUID (alternative to email for update/lock/unlock)"
                        },
                        "first_name": {"type": "string", "description": "User's first name"},
                        "last_name": {"type": "string", "description": "User's last name"},
                        "role": {
                            "type": "string",
                            "enum": ["viewer", "user", "editor", "developer", "admin"],
                            "description": "User role to assign"
                        },
                        "reason": {
                            "type": "string",
                            "description": "Reason for lock/unlock (for audit trail)"
                        }
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "manage_api_key",
            "description": (
                "Generate, revoke, or rotate API keys. Use when users need programmatic access "
                "to the platform. Requires admin role. The full key is only shown once at creation."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["generate", "revoke", "rotate", "list"],
                            "description": (
                                "generate: create a new API key. revoke: deactivate an existing key. "
                                "rotate: revoke old key and generate a new one. list: show all active keys."
                            )
                        },
                        "name": {
                            "type": "string",
                            "description": "Name/label for the API key (required for generate)"
                        },
                        "key_id": {
                            "type": "string",
                            "description": "API key UUID (required for revoke/rotate)"
                        },
                        "scopes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Permission scopes: 'read', 'write', 'admin'. Default: ['read', 'write']"
                        },
                        "environment": {
                            "type": "string",
                            "enum": ["live", "test", "dev"],
                            "description": "Environment for the key. Default: 'live'"
                        }
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "manage_tenant",
            "description": (
                "Create, update, activate, or deactivate tenants. Requires super_admin role. "
                "Use for multi-tenant management: creating new organizations, changing tier, "
                "or toggling tenant status."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["create", "update", "activate", "deactivate", "list"],
                            "description": (
                                "create: provision a new tenant. update: change tenant properties. "
                                "activate: re-enable a deactivated tenant. deactivate: soft-disable a tenant. "
                                "list: show all tenants."
                            )
                        },
                        "tenant_id": {
                            "type": "string",
                            "description": "Tenant UUID (for update/activate/deactivate)"
                        },
                        "name": {"type": "string", "description": "Tenant display name"},
                        "key": {"type": "string", "description": "Unique tenant key (short identifier)"},
                        "tier": {
                            "type": "string",
                            "enum": ["free", "standard", "enterprise"],
                            "description": "Tenant tier"
                        },
                        "mfa_policy": {
                            "type": "string",
                            "enum": ["off", "optional", "required"],
                            "description": "MFA enforcement policy"
                        }
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "manage_permissions",
            "description": (
                "View or update role permissions. Shows what each role (viewer, user, editor, developer, admin) "
                "can do, and allows admins to modify permission assignments. Requires admin role."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["view", "update"],
                            "description": "view: show current role→permission mapping. update: change permissions for a role."
                        },
                        "role": {
                            "type": "string",
                            "description": "Role name to view or update permissions for"
                        },
                        "permissions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of permission strings to assign (for update action)"
                        }
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "deploy_flow",
            "description": (
                "Deploy, activate, or deactivate a flow. Publishes a flow to make it live, "
                "or pauses/stops a running flow. Requires developer or admin role."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["publish", "activate", "deactivate", "status"],
                            "description": (
                                "publish: mark flow as published and active. "
                                "activate: turn on an already published flow. "
                                "deactivate: pause a running flow (keeps published state). "
                                "status: check current deployment status."
                            )
                        },
                        "flow_id": {
                            "type": "string",
                            "description": "Flow UUID to deploy/activate/deactivate"
                        },
                        "flow_name": {
                            "type": "string",
                            "description": "Flow name (alternative to flow_id — will resolve by name)"
                        }
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "export_flow",
            "description": (
                "Export a flow definition as JSON. Returns the complete flow (nodes, edges, "
                "trigger config, metadata) that can be saved as a file or imported into another tenant. "
                "Optionally saves to the file staging area."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "flow_id": {
                            "type": "string",
                            "description": "Flow UUID to export"
                        },
                        "flow_name": {
                            "type": "string",
                            "description": "Flow name (alternative to flow_id)"
                        },
                        "save_to_file": {
                            "type": "boolean",
                            "description": "If true, saves the export to the file staging area as {flow_name}.json. Default: false (returns inline)"
                        },
                        "include_connectors": {
                            "type": "boolean",
                            "description": "If true, includes connector references (names + types, not secrets). Default: true"
                        }
                    },
                    "required": []
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "import_flow",
            "description": (
                "Import a flow from a JSON definition. Creates a new flow with the provided "
                "nodes, edges, and trigger config. Use after uploading a flow JSON via file_operation, "
                "or provide the flow definition directly. Connector references are preserved by name."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Name for the imported flow (required)"
                        },
                        "filename": {
                            "type": "string",
                            "description": "Filename in the staging area to import from (e.g., 'my_flow.json'). Alternative to flow_definition."
                        },
                        "flow_definition": {
                            "type": "object",
                            "description": "The flow definition JSON object with 'nodes' and 'edges' arrays. Alternative to filename."
                        },
                        "description": {
                            "type": "string",
                            "description": "Description for the imported flow"
                        },
                        "trigger_type": {
                            "type": "string",
                            "enum": ["api", "kafka", "schedule", "mllp"],
                            "description": "Trigger type. Default: inherited from the imported definition or 'api'"
                        }
                    },
                    "required": ["name"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "file_operation",
            "description": (
                "Upload, download, or inspect files. Use for HL7, JSON, CSV, XML, or any file "
                "the user wants to process through a flow or inspect. Upload accepts base64-encoded "
                "content. Download returns file content (text) or base64 (binary). "
                "Inspect returns file metadata (size, type, line count, encoding)."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["upload", "download", "inspect", "list"],
                            "description": (
                                "upload: save content to a staging area for flow testing. "
                                "download: retrieve a file's content. "
                                "inspect: get file metadata without full content. "
                                "list: list files in the staging area."
                            )
                        },
                        "filename": {
                            "type": "string",
                            "description": "Filename for upload/download/inspect (e.g., 'patient_adt.hl7', 'claims.csv')"
                        },
                        "content": {
                            "type": "string",
                            "description": "File content for upload — plain text for text files, base64-encoded for binary"
                        },
                        "encoding": {
                            "type": "string",
                            "enum": ["text", "base64"],
                            "description": "Content encoding: 'text' (default) or 'base64' for binary files"
                        },
                        "flow_id": {
                            "type": "string",
                            "description": "Optional flow ID to associate the file with for test_run"
                        },
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "canvas_vision",
            "description": (
                "View AND interact with the app's UI through an internal headless browser. "
                "Vision actions: take screenshots (with element map), verify canvases, extract page structure. "
                "CanvasTouch actions: click buttons, fill forms, select dropdowns, hover elements, scroll, drag nodes. "
                "Screenshots include a structured element map — use element_id to target elements precisely. "
                "Set highlight=true to overlay numbered labels [1], [2], [3] on the screenshot for visual targeting."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": [
                                "view_flow", "view_page", "screenshot", "verify_canvas", "learn_page", "close",
                                "click", "fill", "select", "hover", "scroll", "drag"
                            ],
                            "description": (
                                "view_flow: navigate to flow by ID, screenshot + element map + accessibility snapshot. "
                                "view_page: navigate to any app page with element map. "
                                "screenshot: capture current page with element map. "
                                "verify_canvas: check node connections and layout via JS. "
                                "learn_page: extract comprehensive UI structure. "
                                "close: close browser session. "
                                "--- CanvasTouch --- "
                                "click: click a button, link, tab, or element (supports element_id). "
                                "fill: type text into an input or textarea. "
                                "select: choose option from a dropdown. "
                                "hover: hover to reveal tooltips or menus. "
                                "scroll: scroll page or container. "
                                "drag: drag a node/element by dx/dy pixels."
                            )
                        },
                        "flow_id": {
                            "type": "string",
                            "description": "Flow UUID to view (required for view_flow and verify_canvas actions)"
                        },
                        "path": {
                            "type": "string",
                            "description": "App path to navigate to, e.g. '/settings', '/flows' (for view_page/learn_page)"
                        },
                        "selector": {
                            "type": "string",
                            "description": "CSS selector to target element (e.g., 'button.save', '#email'). For CanvasTouch actions."
                        },
                        "text": {
                            "type": "string",
                            "description": "Visible text to find element (e.g., 'Save', 'Submit'). Supports fuzzy matching. For CanvasTouch actions."
                        },
                        "element_id": {
                            "type": "integer",
                            "description": "Element ID from the element map (e.g., 3 to target element [3]). Alternative to selector/text."
                        },
                        "value": {
                            "type": "string",
                            "description": "Value to type (fill) or option text to select (select)."
                        },
                        "highlight": {
                            "type": "boolean",
                            "description": "Overlay numbered labels [1],[2],[3] on interactive elements in screenshot. Default: false."
                        },
                        "dry_run": {
                            "type": "boolean",
                            "description": "Describe what the action would do without executing. For safety review."
                        },
                        "scroll_direction": {
                            "type": "string",
                            "enum": ["up", "down", "left", "right"],
                            "description": "Scroll direction (default: down). For scroll action."
                        },
                        "scroll_amount": {
                            "type": "number",
                            "description": "Pixels to scroll, 50-5000 (default: 500). For scroll action."
                        },
                        "dx": {
                            "type": "number",
                            "description": "Horizontal pixels to drag (positive=right, negative=left). For drag action."
                        },
                        "dy": {
                            "type": "number",
                            "description": "Vertical pixels to drag (positive=down, negative=up). For drag action."
                        },
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "learn_ui",
            "description": (
                "Save UI discoveries and learned patterns to the draft orchestrator layer. "
                "Learnings are saved as drafts and must be approved by a user before they "
                "become part of your knowledge base. Use after browsing pages with canvas_vision "
                "to record what you learned."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["save_learning", "suggest_prompt", "list_learnings", "query_learnings"],
                            "description": (
                                "save_learning: write a new UI discovery. "
                                "suggest_prompt: create a draft AI prompt entry. "
                                "list_learnings: show pending drafts for review. "
                                "query_learnings: search approved knowledge by category/route (like git log)."
                            )
                        },
                        "category": {
                            "type": "string",
                            "enum": ["page_description", "ui_pattern", "flow_template", "node_behavior"],
                            "description": "Category of the learning (for save_learning)"
                        },
                        "page_route": {
                            "type": "string",
                            "description": "The page route this learning applies to, e.g. '/flows', '/settings' (also used as filter for query_learnings)"
                        },
                        "status_filter": {
                            "type": "string",
                            "enum": ["draft", "approved", "rejected"],
                            "description": "Filter by status (for query_learnings, default: 'approved')"
                        },
                        "title": {
                            "type": "string",
                            "description": "Short descriptive title for the learning"
                        },
                        "content": {
                            "type": "string",
                            "description": "The learned knowledge in prompt format — write as if giving instructions to another AI"
                        },
                        "prompt_key": {
                            "type": "string",
                            "description": "Key for the draft AI prompt (for suggest_prompt action)"
                        },
                        "prompt_content": {
                            "type": "string",
                            "description": "Content for the draft AI prompt (for suggest_prompt action)"
                        },
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "list_flows",
            "description": (
                "Query the database for all flows in the tenant. Returns each flow's name, ID, status "
                "(mounted/unmounted/draft), version, trigger type, node/edge count, execution count, "
                "and last update time. Use this to find flows by name, check what flows exist, or get "
                "a flow's ID before opening or deleting it."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "name_contains": {
                            "type": "string",
                            "description": "Optional filter: only return flows whose name contains this text (case-insensitive)"
                        },
                        "status": {
                            "type": "string",
                            "enum": ["active", "draft", "all"],
                            "description": "Optional filter by status: 'active' (mounted), 'draft' (not active), 'all' (default)"
                        },
                        "limit": {
                            "type": "number",
                            "description": "Max flows to return (default 50, max 100)"
                        }
                    }
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "ai_builder",
            "description": (
                "Delegate code tasks to the AI Builder service. The Builder can plan, generate, "
                "modify, test, and deploy code across any YAMIL service. Use when the user asks "
                "to write code, add features, fix bugs, or modify any service's source code. "
                "The Builder has its own memory and understands the full stack."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["plan", "generate", "status", "list_services"],
                            "description": (
                                "plan: create a code change plan (returns plan for user review). "
                                "generate: create plan + generate code in one step (auto-approve). "
                                "status: check workspace/PR status for a session. "
                                "list_services: list available services the builder can modify."
                            )
                        },
                        "prompt": {
                            "type": "string",
                            "description": "Description of what code changes to make (required for plan/generate)"
                        },
                        "service_name": {
                            "type": "string",
                            "description": "Target service name (e.g., 'auth-service', 'flow-execution-service')"
                        },
                        "file_path": {
                            "type": "string",
                            "description": "Specific file to focus on (optional)"
                        },
                        "auto_approve": {
                            "type": "boolean",
                            "description": "If true, auto-approve the plan and generate code. Default: false for plan, true for generate."
                        }
                    },
                    "required": ["action"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "delete_flow_by_name",
            "description": (
                "Delete (soft-delete) a flow by its name or ID. The flow is moved to trash and can be "
                "restored from the Trash page. ALWAYS confirm with the user before deleting. Requires "
                "admin or developer role."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "flow_name": {
                            "type": "string",
                            "description": "Exact name of the flow to delete (case-insensitive match)"
                        },
                        "flow_id": {
                            "type": "string",
                            "description": "UUID of the flow to delete (use if name is ambiguous)"
                        }
                    }
                }
            }
        }
    },
]


async def invoke_ai_with_phi_guard(
    content: str,
    system_prompt: str,
    conversation_id: str,
    tenant_id: str = "default",
    temperature: float = 0.3,
    max_tokens: int = 4096,
    model: str = None,
    ai_provider: str = "bedrock",
    secret_arn: str = None,
    credentials: dict = None,
    tools: Optional[List[Dict[str, Any]]] = None,
    enable_tool_calling: bool = True,
    skip_phi_masking: bool = False,
    conversation_messages: Optional[List[Dict[str, Any]]] = None,
) -> dict:
    """
    Standard pattern for invoking AI with PHI protection and optional tool calling.

    Steps:
    1. Mask PHI in content (unless skip_phi_masking=True)
    2. Fetch credentials from Secrets Manager (if secret_arn provided)
    3. Invoke AWS Bedrock (Claude) with system prompt and optional tools
    4. Return response with text and tool calls (still masked - caller decides on unmasking)

    Handles:
    - Circuit breaker (returns graceful degradation)
    - Budget exceeded (returns error with reason)
    - Timeout (returns partial results)
    - Tool calling via Bedrock Converse API

    Args:
        content: User content to send (will be PHI-masked)
        system_prompt: System prompt for the AI model
        conversation_id: Unique conversation/request ID
        tenant_id: Tenant identifier for budget tracking
        temperature: Model temperature (default 0.3)
        max_tokens: Max response tokens (default 4096)
        model: Model ID override (default: Claude Sonnet on Bedrock)
        ai_provider: AI provider to use - "bedrock" (default) or "azure_openai"
        secret_arn: AWS Secrets Manager ARN for credential retrieval (HIPAA)
        credentials: Direct credentials dict (accessKeyId, secretAccessKey, region)
        tools: Optional list of tool definitions for Bedrock tool calling
        enable_tool_calling: Whether to enable tool calling (default True)
        skip_phi_masking: Skip PHI masking (for HIPAA-compliant providers like AWS Bedrock)

    Returns:
        dict with keys: success, response, tool_calls, masked_content, token_usage, error
        - response: Text response from the model
        - tool_calls: List of tool use blocks [{name, input}] if tools were used
    """
    phi_guard = get_phi_guard()
    masked = None

    # Step 1: Mask PHI (unless explicitly skipped for HIPAA-compliant providers)
    _ml_logger.info(f"[PHI Guard] skip_phi_masking={skip_phi_masking}, content_length={len(content)}")
    if skip_phi_masking:
        _ml_logger.info(f"PHI masking skipped (HIPAA-compliant provider: {ai_provider})")
        masked_text = content
    else:
        try:
            masked = phi_guard.mask_for_ai(content, conversation_id)
            masked_text = masked.masked_text
            _ml_logger.info(f"[PHI Guard] Masked {masked.token_count} tokens, types={[t.value for t in masked.phi_types_found]}")
        except Exception as e:
            _ml_logger.warning(f"PHI masking failed, using raw content: {e}")
            masked_text = content

    # Log a snippet of the masked text for debugging (truncate to avoid logging PHI)
    _ml_logger.debug(f"[PHI Guard] Sending to AI (first 200 chars): {masked_text[:200]}...")

    # Step 2: Fetch credentials from Secrets Manager if ARN provided
    sm_credentials = None
    if secret_arn:
        try:
            from assemblyline_common.secrets_manager import get_secrets_manager
            sm_client = get_secrets_manager()
            sm_credentials = await sm_client.get_credentials(secret_arn)
            if not sm_credentials:
                _ml_logger.warning(f"No credentials found in SM for ARN: {secret_arn}")
        except Exception as e:
            _ml_logger.warning(f"Secrets Manager fetch failed, falling back to env: {e}")

    # Step 3: Invoke AI Provider
    try:
        if ai_provider == "azure_openai":
            # Azure OpenAI path
            import os
            try:
                from openai import AzureOpenAI
            except ImportError:
                return {
                    "success": False,
                    "response": None,
                    "tool_calls": [],
                    "masked_content": None,
                    "token_usage": None,
                    "error": "ai_unavailable: openai package not installed",
                }

            # Use SM credentials if available, otherwise fall back to env
            if sm_credentials and sm_credentials.get("provider") == "azure":
                azure_key = sm_credentials.get("apiKey")
                azure_endpoint = sm_credentials.get("endpoint")
                azure_version = sm_credentials.get("apiVersion", "2024-02-15-preview")
                azure_deployment = sm_credentials.get("deploymentName", "gpt-4")
            else:
                azure_key = os.getenv("AZURE_OPENAI_API_KEY")
                azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
                azure_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
                azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4")

            client = AzureOpenAI(
                api_key=azure_key,
                api_version=azure_version,
                azure_endpoint=azure_endpoint,
            )

            response = client.chat.completions.create(
                model=model or azure_deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": masked_text},
                ],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            ai_response = response.choices[0].message.content
            token_usage = {
                "input_tokens": response.usage.prompt_tokens if response.usage else 0,
                "output_tokens": response.usage.completion_tokens if response.usage else 0,
            }

            return {
                "success": True,
                "response": ai_response,
                "tool_calls": [],  # Azure OpenAI tool calling not yet implemented
                "masked_content": masked.to_dict() if masked else None,
                "token_usage": token_usage,
                "error": None,
            }
        else:
            # AWS Bedrock path (default) - using Converse API for tool calling
            import boto3

            # Use SM credentials if available, then direct credentials, then default chain
            if sm_credentials and sm_credentials.get("provider") == "bedrock":
                client = boto3.client(
                    "bedrock-runtime",
                    region_name=sm_credentials.get("region", "us-east-1"),
                    aws_access_key_id=sm_credentials.get("accessKeyId"),
                    aws_secret_access_key=sm_credentials.get("secretAccessKey"),
                )
            elif credentials and credentials.get("accessKeyId"):
                client = boto3.client(
                    "bedrock-runtime",
                    region_name=credentials.get("region", "us-east-1"),
                    aws_access_key_id=credentials.get("accessKeyId"),
                    aws_secret_access_key=credentials.get("secretAccessKey"),
                )
            else:
                client = boto3.client("bedrock-runtime")

            model_id = model or "us.anthropic.claude-3-5-sonnet-20241022-v2:0"

            # Use tools if provided and enabled
            use_tools = tools or (AI_TOOLS if enable_tool_calling else None)

            if use_tools:
                # Use Converse API for tool calling (more reliable structured output)
                # Multi-turn: use full conversation history when provided
                # Also use conversation_messages when they contain image blocks (even single-turn)
                _has_images = conversation_messages and any(
                    any("image" in block for block in msg.get("content", []))
                    for msg in conversation_messages
                )
                if conversation_messages and (len(conversation_messages) > 1 or _has_images):
                    api_messages = conversation_messages
                else:
                    api_messages = [{"role": "user", "content": [{"text": masked_text}]}]

                converse_params = {
                    "modelId": model_id,
                    "messages": api_messages,
                    "system": [{"text": system_prompt}],
                    "inferenceConfig": {
                        "maxTokens": max_tokens,
                        "temperature": temperature,
                    },
                    "toolConfig": {
                        "tools": use_tools,
                    }
                }

                response = client.converse(**converse_params)

                # Debug logging for Converse API response
                _ml_logger.info(f"Converse API response stopReason: {response.get('stopReason')}")
                _ml_logger.info(f"Converse API response keys: {list(response.keys())}")

                # Parse response - can contain both text and tool_use blocks
                ai_response = ""
                tool_calls = []
                output = response.get("output", {})
                message = output.get("message", {})
                _ml_logger.info(f"Converse content blocks: {len(message.get('content', []))}")

                for content_block in message.get("content", []):
                    if "text" in content_block:
                        ai_response += content_block["text"]
                    elif "toolUse" in content_block:
                        tool_use = content_block["toolUse"]
                        tool_calls.append({
                            "id": tool_use.get("toolUseId", ""),
                            "name": tool_use.get("name", ""),
                            "input": tool_use.get("input", {}),
                        })
                        _ml_logger.info(f"Tool call detected: {tool_use.get('name')}")

                # If the model only returned tool calls with no text, add a default message
                if tool_calls and not ai_response.strip():
                    tool_names = [tc.get("name", "action") for tc in tool_calls]
                    ai_response = f"Done! Executed: {', '.join(tool_names)}."

                token_usage = {
                    "input_tokens": response.get("usage", {}).get("inputTokens", 0),
                    "output_tokens": response.get("usage", {}).get("outputTokens", 0),
                }

                return {
                    "success": True,
                    "response": ai_response,
                    "tool_calls": tool_calls,
                    "masked_content": masked.to_dict() if masked else None,
                    "token_usage": token_usage,
                    "error": None,
                }
            else:
                # Fallback to invoke_model without tools
                body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": masked_text}],
                }

                response = client.invoke_model(
                    modelId=model_id,
                    body=json.dumps(body),
                )
                response_body = json.loads(response["body"].read())
                ai_response = response_body["content"][0]["text"]
                token_usage = {
                    "input_tokens": response_body.get("usage", {}).get("input_tokens", 0),
                    "output_tokens": response_body.get("usage", {}).get("output_tokens", 0),
                }

                return {
                    "success": True,
                    "response": ai_response,
                    "tool_calls": [],
                    "masked_content": masked.to_dict() if masked else None,
                    "token_usage": token_usage,
                    "error": None,
                }

    except ImportError:
        _ml_logger.error(f"Required package not available for {ai_provider}")
        return {
            "success": False,
            "response": None,
            "tool_calls": [],
            "masked_content": None,
            "token_usage": None,
            "error": f"ai_unavailable: required package for {ai_provider} not installed",
        }
    except Exception as e:
        error_msg = str(e)
        # Detect circuit breaker / budget issues
        if "throttl" in error_msg.lower() or "rate" in error_msg.lower():
            reason = "rate_limited"
        elif "budget" in error_msg.lower():
            reason = "budget_exceeded"
        elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            reason = "timeout"
        else:
            reason = "ai_error"

        _ml_logger.error(f"AI invocation failed ({reason}): {error_msg}")
        return {
            "success": False,
            "response": None,
            "tool_calls": [],
            "masked_content": None,
            "token_usage": None,
            "error": f"{reason}: {error_msg}",
        }

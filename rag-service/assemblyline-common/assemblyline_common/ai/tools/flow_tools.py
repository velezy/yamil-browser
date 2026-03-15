"""
Flow Builder Tools

Tools for creating, modifying, and validating integration flows.
"""

import logging
from typing import Optional, Dict, List, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone

from assemblyline_common.ai.tools.base import Tool, ToolResult, ToolDefinition
from assemblyline_common.ai.authorization import AuthorizationContext, Permission

logger = logging.getLogger(__name__)


# ============================================================================
# Node Type Registry
# ============================================================================

NODE_TYPES = {
    # ========================================================================
    # Triggers
    # ========================================================================
    "http-trigger": {
        "category": "trigger",
        "description": "Receive HTTP/REST requests on a configured path and method",
        "config": ["path", "method", "contentType", "authType", "timeout", "rateLimit", "corsEnabled"],
    },
    "http-polling": {
        "category": "trigger",
        "description": "Poll an external HTTP endpoint on an interval",
        "config": ["url", "method", "bearerToken", "intervalValue", "intervalUnit", "authType"],
    },
    "json-input": {
        "category": "trigger",
        "description": "Accept a static or variable JSON payload as flow input",
        "config": ["inputSource", "inputValue", "validationSchema", "variableName"],
    },
    "kafka-consumer": {
        "category": "trigger",
        "description": "Consume messages from a Kafka topic",
        "config": ["topic", "consumerGroup", "startOffset", "autoCommit", "maxPollRecords"],
    },
    "file-input": {
        "category": "trigger",
        "description": "Watch a directory or SFTP server for new files",
        "config": ["sourceType", "directory", "pattern", "pollInterval", "afterProcess"],
    },
    "schedule-trigger": {
        "category": "trigger",
        "description": "Trigger on a cron schedule or fixed interval",
        "config": ["scheduleType", "cronExpression", "intervalValue", "intervalUnit", "timezone"],
    },
    "flow-trigger": {
        "category": "trigger",
        "description": "Entry point for a subflow invoked by call-flow",
        "config": ["inputSchema", "outputSchema", "description"],
    },
    "hl7-receiver": {
        "category": "trigger",
        "description": "Receive HL7 v2.x messages via MLLP",
        "config": ["port", "tls_enabled", "ack_mode"],
    },
    # ========================================================================
    # HL7 / X12
    # ========================================================================
    "hl7-read": {
        "category": "processing",
        "description": "Parse raw HL7 v2.x messages into structured JSON",
        "config": ["messageType", "outputFormat", "strictParsing", "includeMetadata", "onError"],
    },
    "hl7-write": {
        "category": "processing",
        "description": "Generate HL7 v2.x messages from structured JSON",
        "config": ["messageStructure", "hl7Version", "sendingApp", "sendingFacility", "fieldMapping", "validateOutput"],
    },
    "hl7-parser": {
        "category": "processing",
        "description": "Parse HL7 v2.x messages (alias for hl7-read)",
        "config": ["version", "strict_mode"],
    },
    "hl7-generator": {
        "category": "processing",
        "description": "Generate HL7 v2.x messages (alias for hl7-write)",
        "config": ["message_type", "template"],
    },
    "x12-read": {
        "category": "processing",
        "description": "Parse X12 EDI transactions (835, 837, 270/271) into JSON",
        "config": ["transactionType", "x12Version", "outputFormat", "splitTransactions"],
    },
    "x12-write": {
        "category": "processing",
        "description": "Generate X12 EDI transactions from structured data",
        "config": ["transactionType", "x12Version", "senderId", "receiverId", "fieldMapping", "validateOutput"],
    },
    "mllp": {
        "category": "processing",
        "description": "Send/receive HL7 messages over MLLP transport",
        "config": ["mode", "host", "port", "useTls", "timeout", "ackMode"],
    },
    # ========================================================================
    # Processing
    # ========================================================================
    "transform": {
        "category": "processing",
        "description": "Map and transform data fields using JSONPath or visual mapper",
        "config": ["language", "expression", "fieldMappings", "useVisualMapper"],
    },
    "filter": {
        "category": "processing",
        "description": "Evaluate a condition and pass or reject the message",
        "config": ["condition", "onReject", "useVisualBuilder"],
    },
    "router": {
        "category": "processing",
        "description": "Route messages to different downstream paths based on content",
        "config": ["routingMode", "routingKey", "routeACondition", "routeBCondition", "defaultRoute"],
    },
    "scatter": {
        "category": "processing",
        "description": "Send a message to multiple downstream nodes in parallel (fan-out)",
        "config": ["targets", "waitForAll", "timeout"],
    },
    "http-request": {
        "category": "processing",
        "description": "Make an outbound HTTP call mid-flow and capture the response",
        "config": ["url", "method", "authType", "headers", "body", "timeout", "retryCount", "responseVariable"],
    },
    "set-variable": {
        "category": "processing",
        "description": "Set a flow variable that persists across nodes",
        "config": ["variableName", "valueType", "value"],
    },
    "logger": {
        "category": "processing",
        "description": "Log a message to the flow execution log",
        "config": ["level", "message", "includePayload", "category"],
    },
    "collection-processor": {
        "category": "processing",
        "description": "Iterate over an array/collection and process each item",
        "config": ["collectionField", "itemVariable", "batchSize", "parallel"],
    },
    "rate-limiter": {
        "category": "processing",
        "description": "Throttle message throughput to protect downstream systems",
        "config": ["ratePerSecond", "ratePerMinute", "burstSize", "onExceeded"],
    },
    "python-transform": {
        "category": "processing",
        "description": "Execute custom Python code for complex transformations",
        "config": ["pythonCode", "timeout", "testPayload"],
    },
    "fhir-mapper": {
        "category": "processing",
        "description": "Map between FHIR resources",
        "config": ["source_type", "target_type", "mapping"],
    },
    # ========================================================================
    # Flow Control
    # ========================================================================
    "call-flow": {
        "category": "flow_control",
        "description": "Invoke another flow by ID, passing input and receiving output",
        "config": ["flowId", "inputMapping", "outputMapping", "async", "timeout"],
    },
    "flow-async": {
        "category": "flow_control",
        "description": "Fire another flow asynchronously (fire-and-forget)",
        "config": ["flowId", "inputMapping", "fireAndForget"],
    },
    "flow-parallel": {
        "category": "flow_control",
        "description": "Execute multiple downstream branches in parallel and merge results",
        "config": ["branches", "mergeStrategy", "timeout"],
    },
    "subflow": {
        "category": "flow_control",
        "description": "Embed a reusable flow inline as a single node",
        "config": ["flowId", "inputMapping", "outputMapping"],
    },
    # ========================================================================
    # Security
    # ========================================================================
    "crypto": {
        "category": "security",
        "description": "Encrypt or decrypt fields using AES/RSA",
        "config": ["operation", "algorithm", "fields", "keyManagement"],
    },
    "tokenize": {
        "category": "security",
        "description": "Replace sensitive values with tokens (de-identification)",
        "config": ["fields", "tokenFormat", "reversible"],
    },
    "de-identify": {
        "category": "security",
        "description": "Remove or mask PHI fields per HIPAA Safe Harbor",
        "config": ["fields", "method", "maskCharacter"],
    },
    "audit-phi-access": {
        "category": "security",
        "description": "Log an audit record when PHI fields are accessed",
        "config": ["fields", "purpose", "requester"],
    },
    # ========================================================================
    # AI / ML
    # ========================================================================
    "bedrock-chat": {
        "category": "ai",
        "description": "Send a prompt to Amazon Bedrock (Claude, Titan) and return the response",
        "config": ["model", "systemPrompt", "temperature", "maxTokens", "inputField", "outputField"],
    },
    "bedrock-embeddings": {
        "category": "ai",
        "description": "Generate vector embeddings via Amazon Bedrock",
        "config": ["model", "inputField", "outputField", "dimensions"],
    },
    "azure-ai-chat": {
        "category": "ai",
        "description": "Send a prompt to Azure OpenAI",
        "config": ["deploymentName", "systemPrompt", "temperature", "maxTokens", "inputField", "outputField"],
    },
    "azure-ai-vision": {
        "category": "ai",
        "description": "Analyze images using Azure Computer Vision",
        "config": ["endpoint", "operation", "inputSource", "outputField"],
    },
    "ai-transform": {
        "category": "ai",
        "description": "Transform data using a natural language instruction (AI-powered mapping)",
        "config": ["instruction", "inputFields", "outputFields", "model", "temperature"],
    },
    "ai-classifier": {
        "category": "ai",
        "description": "Classify messages into categories using AI",
        "config": ["categories", "inputField", "outputField", "model", "confidenceThreshold"],
    },
    "ai-summarizer": {
        "category": "ai",
        "description": "Summarize long text or documents using AI",
        "config": ["inputField", "outputField", "model", "maxLength", "style"],
    },
    "ai-agent": {
        "category": "ai",
        "description": "Multi-step AI agent that can reason and use tools",
        "config": ["model", "systemPrompt", "tools", "maxSteps", "inputField", "outputField"],
    },
    # ========================================================================
    # Document AI
    # ========================================================================
    "pdf-extract": {
        "category": "document_ai",
        "description": "Extract text, tables, and structured data from PDF documents",
        "config": ["inputSource", "extractionMode", "outputFormat", "ocrEnabled", "pages"],
    },
    "pdf-to-json": {
        "category": "document_ai",
        "description": "Convert PDF content into structured JSON using AI extraction",
        "config": ["inputSource", "schemaMapping", "ocrEnabled", "outputField"],
    },
    "pdf-to-markdown": {
        "category": "document_ai",
        "description": "Convert PDF to markdown format",
        "config": ["inputSource", "includeImages", "outputField"],
    },
    "table-extract": {
        "category": "document_ai",
        "description": "Extract tabular data from images or PDFs",
        "config": ["inputSource", "tableDetection", "outputFormat", "headerRow"],
    },
    # ========================================================================
    # Output
    # ========================================================================
    "http-output": {
        "category": "output",
        "description": "Send HTTP response or make a final outbound HTTP call",
        "config": ["url", "method", "contentType", "authType", "headers", "timeout", "retryCount"],
    },
    "kafka-producer": {
        "category": "output",
        "description": "Publish a message to a Kafka topic",
        "config": ["topic", "partitionKey", "compression"],
    },
    "file-output": {
        "category": "output",
        "description": "Write data to a local file, S3, or SFTP destination",
        "config": ["destinationType", "filePath", "filenamePattern", "overwriteMode", "createDirs"],
    },
    "mllp-output": {
        "category": "output",
        "description": "Send HL7 messages via MLLP transport",
        "config": ["host", "port", "tls_enabled"],
    },
    "email-send": {
        "category": "output",
        "description": "Send email via SMTP or SES for notifications and alerts",
        "config": ["provider", "to", "from", "subject", "body", "bodyType"],
    },
    # ========================================================================
    # Connectors
    # ========================================================================
    "epic-fhir": {
        "category": "connector",
        "description": "Perform FHIR R4 operations against Epic EHR",
        "config": ["fhirBaseUrl", "resourceType", "operation", "clientId", "tokenUrl"],
    },
    "databricks": {
        "category": "connector",
        "description": "Execute Spark SQL queries against Databricks via HTTP connector",
        "config": ["connectorName", "httpPath", "catalog", "schema", "operation", "sqlQuery", "timeout"],
    },
    "database": {
        "category": "connector",
        "description": "Execute SQL queries against PostgreSQL, MySQL, SQL Server, or Oracle",
        "config": ["operation", "dbType", "connectionString", "query", "parameterMapping", "resultVariable", "timeout"],
    },
    "s3-read": {
        "category": "connector",
        "description": "Read objects from an S3 bucket",
        "config": ["bucket", "key", "region", "authType", "outputFormat"],
    },
    "s3-write": {
        "category": "connector",
        "description": "Write objects to an S3 bucket",
        "config": ["bucket", "key", "region", "authType", "contentType"],
    },
    "http-connector": {
        "category": "connector",
        "description": "Generic HTTP connector with saved credentials for REST API integrations",
        "config": ["connectorName", "baseUrl", "authType", "headers", "timeout"],
    },
    "sftp-connector": {
        "category": "connector",
        "description": "Connect to SFTP server for secure file transfer",
        "config": ["host", "port", "username", "authMethod", "remoteDir"],
    },
    "kafka-connector": {
        "category": "connector",
        "description": "Managed Kafka connection with schema registry support",
        "config": ["brokers", "schemaRegistryUrl", "securityProtocol", "topic"],
    },
    "database-query": {
        "category": "connector",
        "description": "Execute database query (alias for database)",
        "config": ["connector_id", "query", "parameters"],
    },
    "jira-connector": {
        "category": "connector",
        "description": "Create, search, update, or transition Jira issues via Jira Cloud REST API",
        "config": [
            "connectorName", "connectorId", "jiraAction",
            "projectKey", "summary", "description", "issueType", "priority", "labels",
            "issueKey", "jql", "transitionName", "comment",
        ],
    },
    # ========================================================================
    # Error Handling
    # ========================================================================
    "error-handler": {
        "category": "error_handling",
        "description": "Catch errors and execute recovery logic (retry, redirect, notify)",
        "config": ["errorType", "retryCount", "retryDelay", "retryBackoff", "sendToDeadLetter", "notifyOnFailure"],
    },
    "circuit-breaker": {
        "category": "error_handling",
        "description": "Prevent cascading failures by opening circuit after N failures",
        "config": ["failureThreshold", "resetTimeout", "halfOpenRequests", "monitoredErrors", "fallbackAction"],
    },
    "dead-letter-queue": {
        "category": "error_handling",
        "description": "Store failed messages for later reprocessing",
        "config": ["destination", "includeOriginal", "includeError", "maxAge", "alertThreshold"],
    },
    # ========================================================================
    # Audit / Observability
    # ========================================================================
    "audit-log": {
        "category": "audit",
        "description": "Write a structured audit record to the compliance log",
        "config": ["eventType", "severity", "includePayload", "retentionDays", "complianceStandard"],
    },
    "phi-detector": {
        "category": "audit",
        "description": "Scan payload for PHI (names, SSNs, MRNs) and flag or mask them",
        "config": ["scanFields", "action", "maskCharacter", "allowedTypes", "logDetections"],
    },
    "cloudwatch-logger": {
        "category": "audit",
        "description": "Send metrics and logs to AWS CloudWatch",
        "config": ["logGroup", "logStream", "metricNamespace", "metricName"],
    },
    "azure-monitor": {
        "category": "audit",
        "description": "Send telemetry to Azure Monitor / Application Insights",
        "config": ["instrumentationKey", "eventName", "severity", "customDimensions"],
    },
}


# ============================================================================
# Flow Tools
# ============================================================================

class CreateFlowTool(Tool):
    """Tool to create a new integration flow."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="create_flow",
            description="Create a new integration flow from a specification",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Flow name"},
                    "description": {"type": "string", "description": "Flow description"},
                    "nodes": {"type": "array", "description": "List of nodes"},
                    "edges": {"type": "array", "description": "List of edges connecting nodes"},
                    "is_draft": {"type": "boolean", "description": "Create as draft", "default": True},
                },
                "required": ["name", "nodes", "edges"],
            },
            required_permission=Permission.CREATE_FLOWS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        name: str,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        description: str = "",
        is_draft: bool = True,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.CREATE_FLOWS):
            return ToolResult(success=False, error="Not authorized to create flows")
        
        # Validate nodes
        for node in nodes:
            node_type = node.get("type")
            if node_type not in NODE_TYPES:
                return ToolResult(
                    success=False,
                    error=f"Unknown node type: {node_type}",
                )
        
        # Create flow (in production, save to database)
        flow_id = uuid4()
        flow = {
            "id": str(flow_id),
            "name": name,
            "description": description,
            "nodes": nodes,
            "edges": edges,
            "is_draft": is_draft,
            "status": "draft" if is_draft else "active",
            "created_by": str(auth_context.user_id),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        self.log_execution(auth_context, "create_flow", {"name": name}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data=flow,
            message=f"Created {'draft ' if is_draft else ''}flow '{name}' (ID: {flow_id})",
        )


class GetFlowTool(Tool):
    """Tool to get a flow by ID."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_flow",
            description="Get a flow definition by ID",
            parameters={
                "type": "object",
                "properties": {
                    "flow_id": {"type": "string", "description": "Flow UUID"},
                },
                "required": ["flow_id"],
            },
            required_permission=Permission.VIEW_FLOWS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        flow_id: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_FLOWS):
            return ToolResult(success=False, error="Not authorized to view flows")
        
        # In production, fetch from database
        return ToolResult(
            success=True,
            data={"id": flow_id, "name": "Sample Flow", "nodes": [], "edges": []},
            message=f"Retrieved flow {flow_id}",
        )


class UpdateFlowTool(Tool):
    """Tool to update an existing flow."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="update_flow",
            description="Update an existing flow",
            parameters={
                "type": "object",
                "properties": {
                    "flow_id": {"type": "string", "description": "Flow UUID"},
                    "updates": {"type": "object", "description": "Fields to update"},
                    "create_version": {"type": "boolean", "description": "Create new version", "default": True},
                },
                "required": ["flow_id", "updates"],
            },
            required_permission=Permission.MODIFY_FLOWS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        flow_id: str,
        updates: Dict[str, Any],
        create_version: bool = True,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.MODIFY_FLOWS):
            return ToolResult(success=False, error="Not authorized to modify flows")
        
        self.log_execution(auth_context, "update_flow", {"flow_id": flow_id}, ToolResult(success=True))
        
        return ToolResult(
            success=True,
            data={"flow_id": flow_id, "version": 2 if create_version else 1},
            message=f"Updated flow {flow_id}" + (" (new version created)" if create_version else ""),
        )


class ValidateFlowTool(Tool):
    """Tool to validate a flow configuration."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="validate_flow",
            description="Validate a flow configuration",
            parameters={
                "type": "object",
                "properties": {
                    "flow_definition": {"type": "object", "description": "Flow definition to validate"},
                },
                "required": ["flow_definition"],
            },
            required_permission=Permission.VIEW_FLOWS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        flow_definition: Dict[str, Any],
        **kwargs,
    ) -> ToolResult:
        errors = []
        warnings = []
        
        nodes = flow_definition.get("nodes", [])
        edges = flow_definition.get("edges", [])
        
        # Check for trigger
        triggers = [n for n in nodes if NODE_TYPES.get(n.get("type", ""), {}).get("category") == "trigger"]
        if not triggers:
            errors.append("Flow must have at least one trigger node")
        
        # Check for outputs
        outputs = [n for n in nodes if NODE_TYPES.get(n.get("type", ""), {}).get("category") == "output"]
        if not outputs:
            warnings.append("Flow has no output nodes")
        
        # Check node types
        for node in nodes:
            node_type = node.get("type")
            if node_type not in NODE_TYPES:
                errors.append(f"Unknown node type: {node_type}")
        
        # Check edge connections
        node_ids = {n.get("id") for n in nodes}
        for edge in edges:
            if edge.get("source") not in node_ids:
                errors.append(f"Edge source {edge.get('source')} not found")
            if edge.get("target") not in node_ids:
                errors.append(f"Edge target {edge.get('target')} not found")
        
        is_valid = len(errors) == 0
        
        return ToolResult(
            success=True,
            data={
                "is_valid": is_valid,
                "errors": errors,
                "warnings": warnings,
            },
            message="Flow is valid" if is_valid else f"Flow has {len(errors)} error(s)",
        )


class ListConnectorsTool(Tool):
    """Tool to list available connectors."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="list_connectors",
            description="List available connectors for flow nodes",
            parameters={
                "type": "object",
                "properties": {
                    "connector_type": {"type": "string", "description": "Filter by type"},
                },
            },
            required_permission=Permission.VIEW_CONNECTORS,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        connector_type: Optional[str] = None,
        **kwargs,
    ) -> ToolResult:
        # In production, fetch from database
        connectors = [
            {"id": "conn-1", "name": "Epic Production", "type": "epic-fhir", "status": "active"},
            {"id": "conn-2", "name": "S3 Data Lake", "type": "s3", "status": "active"},
            {"id": "conn-3", "name": "Analytics DB", "type": "postgresql", "status": "active"},
        ]
        
        if connector_type:
            connectors = [c for c in connectors if c["type"] == connector_type]
        
        return ToolResult(
            success=True,
            data=connectors,
            message=f"Found {len(connectors)} connectors",
        )


class DeployFlowTool(Tool):
    """Tool to deploy a flow to production."""
    
    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="deploy_flow",
            description="Deploy a flow to production (requires approval)",
            parameters={
                "type": "object",
                "properties": {
                    "flow_id": {"type": "string", "description": "Flow UUID"},
                },
                "required": ["flow_id"],
            },
            required_permission=Permission.DEPLOY_FLOWS,
            requires_approval=True,
        )
    
    async def execute(
        self,
        auth_context: AuthorizationContext,
        flow_id: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.DEPLOY_FLOWS):
            return ToolResult(success=False, error="Not authorized to deploy flows")
        
        # This would create an approval request
        from assemblyline_common.ai.approvals import get_approval_service
        approval_service = await get_approval_service()
        
        approval = await approval_service.create_approval(
            tenant_id=auth_context.tenant_id,
            requested_by=auth_context.user_id,
            requester_email=auth_context.email,
            action_type="deploy_flow",
            action_payload={"flow_id": flow_id},
            action_summary=f"Deploy flow {flow_id} to production",
            required_role="admin",
        )
        
        return ToolResult(
            success=True,
            requires_approval=True,
            approval_id=approval.id,
            message=f"Deployment of flow {flow_id} submitted for approval",
        )


# ============================================================================
# Flow Tools Collection
# ============================================================================

class FlowTools:
    """Collection of all flow-related tools."""
    
    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [
            CreateFlowTool(),
            GetFlowTool(),
            UpdateFlowTool(),
            ValidateFlowTool(),
            ListConnectorsTool(),
            DeployFlowTool(),
        ]
    
    @staticmethod
    def get_node_types() -> Dict[str, Any]:
        return NODE_TYPES

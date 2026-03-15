"""
Flow Templates for Logic Weaver.

Pre-built integration patterns that can be customized:
- Healthcare: HL7 to FHIR, ADT processing, Claims
- Integration: API to Queue, File to Database
- Data: ETL, Sync, Migration

Templates provide:
- Pre-configured node arrangements
- Parameter placeholders for customization
- Best-practice configurations
- Documentation and examples
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import json
import uuid


class TemplateCategory(str, Enum):
    """Template categories."""
    HEALTHCARE = "healthcare"
    INTEGRATION = "integration"
    DATA = "data"
    MESSAGING = "messaging"
    API = "api"
    FILE = "file"
    CUSTOM = "custom"


class ParameterType(str, Enum):
    """Template parameter types."""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    SELECT = "select"
    MULTI_SELECT = "multi_select"
    SECRET = "secret"
    CONNECTION = "connection"
    EXPRESSION = "expression"
    CODE = "code"
    JSON = "json"


@dataclass
class TemplateParameter:
    """Parameter definition for template customization."""
    name: str
    label: str
    type: ParameterType
    required: bool = True
    default: Any = None
    description: str = ""
    placeholder: str = ""
    options: List[Dict[str, str]] = field(default_factory=list)  # For select types
    validation: Optional[str] = None  # Regex or expression
    group: str = "General"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "type": self.type.value,
            "required": self.required,
            "default": self.default,
            "description": self.description,
            "placeholder": self.placeholder,
            "options": self.options,
            "validation": self.validation,
            "group": self.group,
        }


@dataclass
class TemplateNode:
    """Node definition within a template."""
    id: str
    type: str
    label: str
    position: Dict[str, int] = field(default_factory=lambda: {"x": 0, "y": 0})
    config: Dict[str, Any] = field(default_factory=dict)
    parameter_bindings: Dict[str, str] = field(default_factory=dict)  # config key -> parameter name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "label": self.label,
            "position": self.position,
            "config": self.config,
            "parameter_bindings": self.parameter_bindings,
        }


@dataclass
class TemplateConnection:
    """Connection between nodes in a template."""
    id: str
    source_node: str
    source_port: str
    target_node: str
    target_port: str
    label: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source_node": self.source_node,
            "source_port": self.source_port,
            "target_node": self.target_node,
            "target_port": self.target_port,
            "label": self.label,
        }


@dataclass
class FlowTemplate:
    """
    Complete flow template definition.

    Templates are pre-built integration patterns that users can
    customize through parameters without understanding the underlying
    node configuration.
    """
    id: str
    name: str
    description: str
    category: TemplateCategory
    version: str = "1.0.0"

    # Visual
    icon: str = ""
    color: str = "#3B82F6"
    preview_image: str = ""

    # Structure
    nodes: List[TemplateNode] = field(default_factory=list)
    connections: List[TemplateConnection] = field(default_factory=list)
    parameters: List[TemplateParameter] = field(default_factory=list)

    # Metadata
    tags: List[str] = field(default_factory=list)
    author: str = "Logic Weaver"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    # Documentation
    documentation: str = ""
    example_config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "version": self.version,
            "icon": self.icon,
            "color": self.color,
            "preview_image": self.preview_image,
            "nodes": [n.to_dict() for n in self.nodes],
            "connections": [c.to_dict() for c in self.connections],
            "parameters": [p.to_dict() for p in self.parameters],
            "tags": self.tags,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "documentation": self.documentation,
            "example_config": self.example_config,
        }

    def instantiate(self, parameter_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a flow instance from this template.

        Args:
            parameter_values: Values for template parameters

        Returns:
            Flow definition ready for saving
        """
        # Validate required parameters
        for param in self.parameters:
            if param.required and param.name not in parameter_values:
                if param.default is not None:
                    parameter_values[param.name] = param.default
                else:
                    raise ValueError(f"Missing required parameter: {param.name}")

        # Build nodes with parameter values applied
        flow_nodes = []
        for node in self.nodes:
            node_config = dict(node.config)

            # Apply parameter bindings
            for config_key, param_name in node.parameter_bindings.items():
                if param_name in parameter_values:
                    node_config[config_key] = parameter_values[param_name]

            flow_nodes.append({
                "id": f"{node.id}_{uuid.uuid4().hex[:8]}",
                "type": node.type,
                "label": node.label,
                "position": node.position,
                "config": node_config,
            })

        # Build connections with updated node IDs
        id_map = {node.id: flow_nodes[i]["id"] for i, node in enumerate(self.nodes)}
        flow_connections = []
        for conn in self.connections:
            flow_connections.append({
                "id": f"conn_{uuid.uuid4().hex[:8]}",
                "source_node": id_map.get(conn.source_node, conn.source_node),
                "source_port": conn.source_port,
                "target_node": id_map.get(conn.target_node, conn.target_node),
                "target_port": conn.target_port,
            })

        return {
            "id": str(uuid.uuid4()),
            "name": f"{self.name} - Instance",
            "description": f"Created from template: {self.name}",
            "nodes": flow_nodes,
            "connections": flow_connections,
            "template_id": self.id,
            "template_version": self.version,
            "created_at": datetime.utcnow().isoformat(),
        }


class TemplateRegistry:
    """
    Registry for flow templates.

    Provides:
    - Template registration and discovery
    - Category-based filtering
    - Search functionality
    """

    def __init__(self):
        self._templates: Dict[str, FlowTemplate] = {}

    def register(self, template: FlowTemplate) -> None:
        """Register a template."""
        self._templates[template.id] = template

    def get(self, template_id: str) -> Optional[FlowTemplate]:
        """Get template by ID."""
        return self._templates.get(template_id)

    def list(
        self,
        category: Optional[TemplateCategory] = None,
        tags: Optional[List[str]] = None,
        search: Optional[str] = None,
    ) -> List[FlowTemplate]:
        """List templates with optional filtering."""
        templates = list(self._templates.values())

        if category:
            templates = [t for t in templates if t.category == category]

        if tags:
            templates = [
                t for t in templates
                if any(tag in t.tags for tag in tags)
            ]

        if search:
            search_lower = search.lower()
            templates = [
                t for t in templates
                if search_lower in t.name.lower()
                or search_lower in t.description.lower()
            ]

        return templates

    def get_categories(self) -> List[Dict[str, Any]]:
        """Get categories with template counts."""
        counts = {}
        for template in self._templates.values():
            cat = template.category.value
            counts[cat] = counts.get(cat, 0) + 1

        return [
            {"category": cat, "count": count}
            for cat, count in counts.items()
        ]


# =============================================================================
# Built-in Templates
# =============================================================================

HEALTHCARE_TEMPLATES = [
    FlowTemplate(
        id="hl7-to-fhir",
        name="HL7 v2 to FHIR R4",
        description="Transform HL7 v2.x messages to FHIR R4 resources",
        category=TemplateCategory.HEALTHCARE,
        icon="transform",
        color="#10B981",
        tags=["hl7", "fhir", "transform", "interoperability"],
        parameters=[
            TemplateParameter(
                name="mllp_host",
                label="MLLP Host",
                type=ParameterType.STRING,
                default="localhost",
                description="HL7 MLLP server host",
            ),
            TemplateParameter(
                name="mllp_port",
                label="MLLP Port",
                type=ParameterType.NUMBER,
                default=2575,
            ),
            TemplateParameter(
                name="fhir_server",
                label="FHIR Server URL",
                type=ParameterType.STRING,
                placeholder="https://fhir.example.com/r4",
            ),
            TemplateParameter(
                name="message_types",
                label="Message Types",
                type=ParameterType.MULTI_SELECT,
                options=[
                    {"value": "ADT", "label": "ADT - Admit/Discharge/Transfer"},
                    {"value": "ORM", "label": "ORM - Orders"},
                    {"value": "ORU", "label": "ORU - Results"},
                    {"value": "SIU", "label": "SIU - Scheduling"},
                ],
                default=["ADT", "ORU"],
            ),
        ],
        nodes=[
            TemplateNode(
                id="mllp_receiver",
                type="mllp_adapter",
                label="MLLP Receiver",
                position={"x": 100, "y": 200},
                config={"server_mode": True},
                parameter_bindings={
                    "host": "mllp_host",
                    "port": "mllp_port",
                },
            ),
            TemplateNode(
                id="hl7_parser",
                type="hl7_parser",
                label="Parse HL7",
                position={"x": 300, "y": 200},
                config={"validate": True},
            ),
            TemplateNode(
                id="fhir_transform",
                type="python_transform",
                label="Transform to FHIR",
                position={"x": 500, "y": 200},
                config={
                    "code": """
# Transform HL7 to FHIR
from assemblyline_common.hl7 import hl7_to_fhir
bundle = hl7_to_fhir(input_data['parsed'])
return {"bundle": bundle}
""",
                },
            ),
            TemplateNode(
                id="fhir_sender",
                type="http_adapter",
                label="Send to FHIR",
                position={"x": 700, "y": 200},
                config={"method": "POST"},
                parameter_bindings={"base_url": "fhir_server"},
            ),
        ],
        connections=[
            TemplateConnection(
                id="c1",
                source_node="mllp_receiver",
                source_port="output",
                target_node="hl7_parser",
                target_port="input",
            ),
            TemplateConnection(
                id="c2",
                source_node="hl7_parser",
                source_port="output",
                target_node="fhir_transform",
                target_port="input",
            ),
            TemplateConnection(
                id="c3",
                source_node="fhir_transform",
                source_port="output",
                target_node="fhir_sender",
                target_port="input",
            ),
        ],
        documentation="""
# HL7 v2 to FHIR R4 Template

This template receives HL7 v2.x messages via MLLP and transforms them to FHIR R4 resources.

## Configuration

1. **MLLP Host/Port**: Where to receive HL7 messages
2. **FHIR Server**: Target FHIR server URL
3. **Message Types**: Which HL7 message types to process

## Supported Transformations

- ADT → Patient, Encounter
- ORM → ServiceRequest
- ORU → DiagnosticReport, Observation
- SIU → Appointment

## Notes

- Ensure FHIR server accepts the generated resource types
- Consider adding error handling for validation failures
""",
    ),
    FlowTemplate(
        id="claims-processing",
        name="X12 835/837 Claims Processing",
        description="Process healthcare claims and remittance files",
        category=TemplateCategory.HEALTHCARE,
        icon="file-text",
        color="#8B5CF6",
        tags=["x12", "claims", "835", "837", "era", "edi"],
        parameters=[
            TemplateParameter(
                name="sftp_host",
                label="SFTP Host",
                type=ParameterType.STRING,
            ),
            TemplateParameter(
                name="sftp_username",
                label="SFTP Username",
                type=ParameterType.STRING,
            ),
            TemplateParameter(
                name="sftp_password",
                label="SFTP Password",
                type=ParameterType.SECRET,
            ),
            TemplateParameter(
                name="input_directory",
                label="Input Directory",
                type=ParameterType.STRING,
                default="/inbound",
            ),
            TemplateParameter(
                name="database_connection",
                label="Database Connection",
                type=ParameterType.CONNECTION,
            ),
        ],
        nodes=[
            TemplateNode(
                id="sftp_poll",
                type="sftp_adapter",
                label="Poll SFTP",
                position={"x": 100, "y": 200},
                config={"poll_pattern": "*.835,*.837"},
                parameter_bindings={
                    "host": "sftp_host",
                    "username": "sftp_username",
                    "password": "sftp_password",
                    "poll_directory": "input_directory",
                },
            ),
            TemplateNode(
                id="x12_parser",
                type="x12_parser",
                label="Parse X12",
                position={"x": 300, "y": 200},
            ),
            TemplateNode(
                id="claims_processor",
                type="python_transform",
                label="Process Claims",
                position={"x": 500, "y": 200},
                config={
                    "code": """
# Process claims data
claims = input_data['claims']
processed = []
for claim in claims:
    processed.append({
        'claim_id': claim['claim_id'],
        'amount': claim['amount'],
        'status': 'PROCESSED',
    })
return {'claims': processed}
""",
                },
            ),
            TemplateNode(
                id="db_writer",
                type="database_adapter",
                label="Save to Database",
                position={"x": 700, "y": 200},
                config={"operation": "upsert", "table": "claims"},
                parameter_bindings={"connection": "database_connection"},
            ),
        ],
        connections=[
            TemplateConnection(id="c1", source_node="sftp_poll", source_port="output", target_node="x12_parser", target_port="input"),
            TemplateConnection(id="c2", source_node="x12_parser", source_port="output", target_node="claims_processor", target_port="input"),
            TemplateConnection(id="c3", source_node="claims_processor", source_port="output", target_node="db_writer", target_port="input"),
        ],
    ),
]

INTEGRATION_TEMPLATES = [
    FlowTemplate(
        id="api-to-queue",
        name="API to Message Queue",
        description="Receive API requests and publish to message queue",
        category=TemplateCategory.INTEGRATION,
        icon="server",
        color="#F59E0B",
        tags=["api", "queue", "webhook", "async"],
        parameters=[
            TemplateParameter(
                name="webhook_path",
                label="Webhook Path",
                type=ParameterType.STRING,
                default="/webhook",
            ),
            TemplateParameter(
                name="queue_type",
                label="Queue Type",
                type=ParameterType.SELECT,
                options=[
                    {"value": "rabbitmq", "label": "RabbitMQ"},
                    {"value": "sqs", "label": "AWS SQS"},
                    {"value": "kafka", "label": "Kafka"},
                ],
                default="rabbitmq",
            ),
            TemplateParameter(
                name="queue_name",
                label="Queue Name",
                type=ParameterType.STRING,
            ),
        ],
        nodes=[
            TemplateNode(
                id="http_receiver",
                type="http_adapter",
                label="HTTP Webhook",
                position={"x": 100, "y": 200},
                config={"server_mode": True},
                parameter_bindings={"webhook_path": "webhook_path"},
            ),
            TemplateNode(
                id="validator",
                type="json_validator",
                label="Validate Request",
                position={"x": 300, "y": 200},
            ),
            TemplateNode(
                id="queue_publisher",
                type="queue_adapter",
                label="Publish to Queue",
                position={"x": 500, "y": 200},
                parameter_bindings={
                    "queue_type": "queue_type",
                    "queue_name": "queue_name",
                },
            ),
        ],
        connections=[
            TemplateConnection(id="c1", source_node="http_receiver", source_port="output", target_node="validator", target_port="input"),
            TemplateConnection(id="c2", source_node="validator", source_port="output", target_node="queue_publisher", target_port="input"),
        ],
    ),
    FlowTemplate(
        id="database-sync",
        name="Database Synchronization",
        description="Sync data between two databases using CDC",
        category=TemplateCategory.INTEGRATION,
        icon="refresh-cw",
        color="#06B6D4",
        tags=["database", "sync", "cdc", "replication"],
        parameters=[
            TemplateParameter(
                name="source_db",
                label="Source Database",
                type=ParameterType.CONNECTION,
                group="Source",
            ),
            TemplateParameter(
                name="source_tables",
                label="Tables to Sync",
                type=ParameterType.STRING,
                description="Comma-separated list of tables",
                group="Source",
            ),
            TemplateParameter(
                name="target_db",
                label="Target Database",
                type=ParameterType.CONNECTION,
                group="Target",
            ),
        ],
        nodes=[
            TemplateNode(
                id="cdc_source",
                type="cdc_adapter",
                label="CDC Source",
                position={"x": 100, "y": 200},
                parameter_bindings={"connection": "source_db"},
            ),
            TemplateNode(
                id="transformer",
                type="python_transform",
                label="Transform Data",
                position={"x": 300, "y": 200},
                config={
                    "code": """
# Transform CDC event
event = input_data
return {
    'operation': event['operation'],
    'table': event['table'],
    'data': event['after'] or event['before'],
}
""",
                },
            ),
            TemplateNode(
                id="target_writer",
                type="database_adapter",
                label="Write to Target",
                position={"x": 500, "y": 200},
                config={"operation": "upsert"},
                parameter_bindings={"connection": "target_db"},
            ),
        ],
        connections=[
            TemplateConnection(id="c1", source_node="cdc_source", source_port="output", target_node="transformer", target_port="input"),
            TemplateConnection(id="c2", source_node="transformer", source_port="output", target_node="target_writer", target_port="input"),
        ],
    ),
]

DATA_TEMPLATES = [
    FlowTemplate(
        id="csv-to-json",
        name="CSV to JSON Transformation",
        description="Convert CSV files to JSON with field mapping",
        category=TemplateCategory.DATA,
        icon="file-spreadsheet",
        color="#EC4899",
        tags=["csv", "json", "transform", "etl"],
        parameters=[
            TemplateParameter(
                name="source_path",
                label="Source Path",
                type=ParameterType.STRING,
            ),
            TemplateParameter(
                name="destination_path",
                label="Destination Path",
                type=ParameterType.STRING,
            ),
            TemplateParameter(
                name="delimiter",
                label="CSV Delimiter",
                type=ParameterType.SELECT,
                options=[
                    {"value": ",", "label": "Comma (,)"},
                    {"value": ";", "label": "Semicolon (;)"},
                    {"value": "\t", "label": "Tab"},
                    {"value": "|", "label": "Pipe (|)"},
                ],
                default=",",
            ),
        ],
        nodes=[
            TemplateNode(
                id="file_reader",
                type="file_adapter",
                label="Read CSV",
                position={"x": 100, "y": 200},
                parameter_bindings={"path": "source_path"},
            ),
            TemplateNode(
                id="csv_parser",
                type="flatfile_parser",
                label="Parse CSV",
                position={"x": 300, "y": 200},
                parameter_bindings={"delimiter": "delimiter"},
            ),
            TemplateNode(
                id="json_writer",
                type="file_adapter",
                label="Write JSON",
                position={"x": 500, "y": 200},
                config={"format": "json"},
                parameter_bindings={"path": "destination_path"},
            ),
        ],
        connections=[
            TemplateConnection(id="c1", source_node="file_reader", source_port="output", target_node="csv_parser", target_port="input"),
            TemplateConnection(id="c2", source_node="csv_parser", source_port="output", target_node="json_writer", target_port="input"),
        ],
    ),
]


# =============================================================================
# Registry Singleton
# =============================================================================

_template_registry: Optional[TemplateRegistry] = None


def get_template_registry() -> TemplateRegistry:
    """Get the global template registry."""
    global _template_registry

    if _template_registry is None:
        _template_registry = TemplateRegistry()

        # Register built-in templates
        for template in HEALTHCARE_TEMPLATES:
            _template_registry.register(template)
        for template in INTEGRATION_TEMPLATES:
            _template_registry.register(template)
        for template in DATA_TEMPLATES:
            _template_registry.register(template)

    return _template_registry

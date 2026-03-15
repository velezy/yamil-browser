"""
Flow Designer Enhancements for Logic Weaver.

Advanced tooling for the visual flow designer:
- Flow Templates: Pre-built integration patterns
- Python Transform IDE: In-browser Python editor with autocomplete
- Custom Connector Builder: SDK for building custom connectors
- Visual Data Mapper: Drag-and-drop field mapping

These components work together to provide a low-code/no-code
experience for building integrations.
"""

from assemblyline_common.flow_designer.templates import (
    # Template classes
    FlowTemplate,
    TemplateCategory,
    TemplateParameter,
    TemplateNode,
    TemplateConnection,
    # Template registry
    TemplateRegistry,
    get_template_registry,
    # Built-in templates
    HEALTHCARE_TEMPLATES,
    INTEGRATION_TEMPLATES,
    DATA_TEMPLATES,
)

from assemblyline_common.flow_designer.python_ide import (
    # IDE components
    PythonTransformIDE,
    IDEConfig,
    CodeCompletion,
    CompletionItem,
    DiagnosticMessage,
    DiagnosticSeverity,
    # Runtime
    TransformRuntime,
    RuntimeContext,
    RuntimeResult,
    # Sandbox
    PythonSandbox,
    SandboxConfig,
)

from assemblyline_common.flow_designer.connector_builder import (
    # SDK
    ConnectorSDK,
    ConnectorDefinition,
    ConnectorConfig,
    ConnectorAction,
    ConnectorTrigger,
    # Field types
    FieldDefinition,
    FieldType,
    FieldValidation,
    # Auth
    AuthDefinition,
    AuthType,
    # Builder
    ConnectorBuilder,
    ConnectorValidator,
)

from assemblyline_common.flow_designer.data_mapper import (
    # Mapper
    DataMapper,
    MappingConfig,
    FieldMapping,
    MappingRule,
    # Transformations
    TransformFunction,
    TransformType,
    # Schema
    SchemaDefinition,
    SchemaField,
    SchemaType,
    # Builder
    MappingBuilder,
    MappingValidator,
)

__all__ = [
    # Templates
    "FlowTemplate",
    "TemplateCategory",
    "TemplateParameter",
    "TemplateNode",
    "TemplateConnection",
    "TemplateRegistry",
    "get_template_registry",
    "HEALTHCARE_TEMPLATES",
    "INTEGRATION_TEMPLATES",
    "DATA_TEMPLATES",
    # Python IDE
    "PythonTransformIDE",
    "IDEConfig",
    "CodeCompletion",
    "CompletionItem",
    "DiagnosticMessage",
    "DiagnosticSeverity",
    "TransformRuntime",
    "RuntimeContext",
    "RuntimeResult",
    "PythonSandbox",
    "SandboxConfig",
    # Connector Builder
    "ConnectorSDK",
    "ConnectorDefinition",
    "ConnectorConfig",
    "ConnectorAction",
    "ConnectorTrigger",
    "FieldDefinition",
    "FieldType",
    "FieldValidation",
    "AuthDefinition",
    "AuthType",
    "ConnectorBuilder",
    "ConnectorValidator",
    # Data Mapper
    "DataMapper",
    "MappingConfig",
    "FieldMapping",
    "MappingRule",
    "TransformFunction",
    "TransformType",
    "SchemaDefinition",
    "SchemaField",
    "SchemaType",
    "MappingBuilder",
    "MappingValidator",
]

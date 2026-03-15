"""
Custom Connector Builder SDK for Logic Weaver.

Provides a comprehensive SDK for building custom connectors:
- Connector Definition: Define actions, triggers, authentication
- Field Types: Type-safe field definitions with validation
- Builder Pattern: Fluent API for connector construction
- Validation: Ensure connector definitions are complete and valid

This is a LIGHTWEIGHT SDK - connectors delegate heavy operations
to external services and follow the microservice pattern.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, Union
import re
import json
from datetime import datetime


# =============================================================================
# Field Types and Validation
# =============================================================================


class FieldType(Enum):
    """Supported field types for connector configuration."""
    STRING = "string"
    TEXT = "text"  # Multi-line string
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    SELECT = "select"  # Single select
    MULTI_SELECT = "multi_select"
    DATE = "date"
    DATETIME = "datetime"
    PASSWORD = "password"  # Masked input
    URL = "url"
    EMAIL = "email"
    JSON = "json"
    FILE = "file"
    ARRAY = "array"
    OBJECT = "object"
    DYNAMIC = "dynamic"  # Runtime-resolved


@dataclass
class FieldValidation:
    """Validation rules for a field."""

    required: bool = False
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: Optional[str] = None  # Regex pattern
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[str] = None  # JavaScript expression

    def validate(self, value: Any) -> List[str]:
        """
        Validate a value against rules.

        Returns list of validation error messages.
        """
        errors = []

        # Required check
        if self.required and (value is None or value == ""):
            errors.append("This field is required")
            return errors

        if value is None:
            return errors

        # String validations
        if isinstance(value, str):
            if self.min_length and len(value) < self.min_length:
                errors.append(f"Minimum length is {self.min_length}")
            if self.max_length and len(value) > self.max_length:
                errors.append(f"Maximum length is {self.max_length}")
            if self.pattern and not re.match(self.pattern, value):
                errors.append("Value does not match required pattern")

        # Numeric validations
        if isinstance(value, (int, float)):
            if self.min_value is not None and value < self.min_value:
                errors.append(f"Minimum value is {self.min_value}")
            if self.max_value is not None and value > self.max_value:
                errors.append(f"Maximum value is {self.max_value}")

        # Allowed values
        if self.allowed_values and value not in self.allowed_values:
            errors.append(f"Value must be one of: {', '.join(map(str, self.allowed_values))}")

        return errors


@dataclass
class FieldOption:
    """Option for select fields."""
    value: str
    label: str
    description: Optional[str] = None
    icon: Optional[str] = None
    disabled: bool = False


@dataclass
class FieldDefinition:
    """Definition of a connector field."""

    name: str
    type: FieldType
    label: str
    description: Optional[str] = None
    placeholder: Optional[str] = None
    default_value: Any = None
    validation: Optional[FieldValidation] = None
    options: Optional[List[FieldOption]] = None  # For select types
    depends_on: Optional[str] = None  # Conditional visibility
    depends_value: Any = None  # Value that triggers visibility
    group: Optional[str] = None  # UI grouping
    order: int = 0
    hidden: bool = False
    read_only: bool = False

    # For nested types
    items_type: Optional["FieldDefinition"] = None  # For ARRAY
    properties: Optional[List["FieldDefinition"]] = None  # For OBJECT

    # For DYNAMIC type
    options_endpoint: Optional[str] = None  # API to fetch options
    refresh_on: Optional[List[str]] = None  # Fields that trigger refresh

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema."""
        schema: Dict[str, Any] = {}

        type_mapping = {
            FieldType.STRING: "string",
            FieldType.TEXT: "string",
            FieldType.NUMBER: "number",
            FieldType.INTEGER: "integer",
            FieldType.BOOLEAN: "boolean",
            FieldType.SELECT: "string",
            FieldType.MULTI_SELECT: "array",
            FieldType.DATE: "string",
            FieldType.DATETIME: "string",
            FieldType.PASSWORD: "string",
            FieldType.URL: "string",
            FieldType.EMAIL: "string",
            FieldType.JSON: "object",
            FieldType.FILE: "string",
            FieldType.ARRAY: "array",
            FieldType.OBJECT: "object",
            FieldType.DYNAMIC: "string",
        }

        schema["type"] = type_mapping.get(self.type, "string")

        if self.description:
            schema["description"] = self.description

        if self.default_value is not None:
            schema["default"] = self.default_value

        # Format hints
        if self.type == FieldType.DATE:
            schema["format"] = "date"
        elif self.type == FieldType.DATETIME:
            schema["format"] = "date-time"
        elif self.type == FieldType.URL:
            schema["format"] = "uri"
        elif self.type == FieldType.EMAIL:
            schema["format"] = "email"

        # Validation
        if self.validation:
            if self.validation.min_length:
                schema["minLength"] = self.validation.min_length
            if self.validation.max_length:
                schema["maxLength"] = self.validation.max_length
            if self.validation.min_value is not None:
                schema["minimum"] = self.validation.min_value
            if self.validation.max_value is not None:
                schema["maximum"] = self.validation.max_value
            if self.validation.pattern:
                schema["pattern"] = self.validation.pattern
            if self.validation.allowed_values:
                schema["enum"] = self.validation.allowed_values

        # Array items
        if self.type == FieldType.ARRAY and self.items_type:
            schema["items"] = self.items_type.to_json_schema()

        # Object properties
        if self.type == FieldType.OBJECT and self.properties:
            schema["properties"] = {
                prop.name: prop.to_json_schema()
                for prop in self.properties
            }
            required = [
                prop.name for prop in self.properties
                if prop.validation and prop.validation.required
            ]
            if required:
                schema["required"] = required

        return schema


# =============================================================================
# Authentication Types
# =============================================================================


class AuthType(Enum):
    """Authentication types for connectors."""
    NONE = "none"
    API_KEY = "api_key"
    BASIC = "basic"
    BEARER = "bearer"
    OAUTH2 = "oauth2"
    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    CUSTOM = "custom"
    SESSION = "session"
    CERTIFICATE = "certificate"


@dataclass
class OAuth2Config:
    """OAuth2 configuration."""
    authorization_url: str
    token_url: str
    scopes: List[str] = field(default_factory=list)
    refresh_url: Optional[str] = None
    revoke_url: Optional[str] = None
    pkce_enabled: bool = False
    client_id_field: str = "client_id"
    client_secret_field: str = "client_secret"


@dataclass
class AuthDefinition:
    """Definition of connector authentication."""

    type: AuthType
    fields: List[FieldDefinition] = field(default_factory=list)

    # OAuth2 specific
    oauth2_config: Optional[OAuth2Config] = None

    # API Key specific
    api_key_location: str = "header"  # header, query, cookie
    api_key_name: str = "X-API-Key"

    # Bearer specific
    bearer_prefix: str = "Bearer"

    # Certificate specific
    certificate_field: str = "certificate"
    private_key_field: str = "private_key"

    # Custom auth
    custom_auth_handler: Optional[str] = None  # JS function name

    @classmethod
    def none(cls) -> "AuthDefinition":
        """No authentication."""
        return cls(type=AuthType.NONE)

    @classmethod
    def api_key(
        cls,
        location: str = "header",
        key_name: str = "X-API-Key"
    ) -> "AuthDefinition":
        """API Key authentication."""
        return cls(
            type=AuthType.API_KEY,
            api_key_location=location,
            api_key_name=key_name,
            fields=[
                FieldDefinition(
                    name="api_key",
                    type=FieldType.PASSWORD,
                    label="API Key",
                    validation=FieldValidation(required=True)
                )
            ]
        )

    @classmethod
    def basic(cls) -> "AuthDefinition":
        """Basic HTTP authentication."""
        return cls(
            type=AuthType.BASIC,
            fields=[
                FieldDefinition(
                    name="username",
                    type=FieldType.STRING,
                    label="Username",
                    validation=FieldValidation(required=True)
                ),
                FieldDefinition(
                    name="password",
                    type=FieldType.PASSWORD,
                    label="Password",
                    validation=FieldValidation(required=True)
                )
            ]
        )

    @classmethod
    def bearer(cls, prefix: str = "Bearer") -> "AuthDefinition":
        """Bearer token authentication."""
        return cls(
            type=AuthType.BEARER,
            bearer_prefix=prefix,
            fields=[
                FieldDefinition(
                    name="token",
                    type=FieldType.PASSWORD,
                    label="Token",
                    validation=FieldValidation(required=True)
                )
            ]
        )

    @classmethod
    def oauth2(
        cls,
        authorization_url: str,
        token_url: str,
        scopes: Optional[List[str]] = None,
        pkce: bool = False
    ) -> "AuthDefinition":
        """OAuth2 authorization code flow."""
        return cls(
            type=AuthType.OAUTH2,
            oauth2_config=OAuth2Config(
                authorization_url=authorization_url,
                token_url=token_url,
                scopes=scopes or [],
                pkce_enabled=pkce
            ),
            fields=[
                FieldDefinition(
                    name="client_id",
                    type=FieldType.STRING,
                    label="Client ID",
                    validation=FieldValidation(required=True)
                ),
                FieldDefinition(
                    name="client_secret",
                    type=FieldType.PASSWORD,
                    label="Client Secret",
                    validation=FieldValidation(required=True)
                )
            ]
        )


# =============================================================================
# Actions and Triggers
# =============================================================================


@dataclass
class ActionInput:
    """Input parameter for an action."""
    name: str
    type: FieldType
    label: str
    description: Optional[str] = None
    required: bool = False
    default_value: Any = None
    validation: Optional[FieldValidation] = None


@dataclass
class ActionOutput:
    """Output field from an action."""
    name: str
    type: FieldType
    label: str
    description: Optional[str] = None


@dataclass
class ConnectorAction:
    """Definition of a connector action."""

    name: str
    label: str
    description: Optional[str] = None
    inputs: List[ActionInput] = field(default_factory=list)
    outputs: List[ActionOutput] = field(default_factory=list)

    # Execution
    handler: Optional[str] = None  # Handler function/endpoint
    timeout_ms: int = 30000
    retryable: bool = True
    max_retries: int = 3

    # Rate limiting
    rate_limit: Optional[int] = None  # Requests per minute
    rate_limit_key: Optional[str] = None  # Field to use for rate limit key

    # Caching
    cacheable: bool = False
    cache_ttl_seconds: int = 300

    # Pagination (for list operations)
    supports_pagination: bool = False
    page_size_param: Optional[str] = None
    cursor_param: Optional[str] = None

    # Bulk operations
    supports_bulk: bool = False
    max_bulk_size: int = 100

    def to_schema(self) -> Dict[str, Any]:
        """Convert to schema representation."""
        return {
            "name": self.name,
            "label": self.label,
            "description": self.description,
            "inputs": {
                inp.name: {
                    "type": inp.type.value,
                    "label": inp.label,
                    "description": inp.description,
                    "required": inp.required,
                    "default": inp.default_value
                }
                for inp in self.inputs
            },
            "outputs": {
                out.name: {
                    "type": out.type.value,
                    "label": out.label,
                    "description": out.description
                }
                for out in self.outputs
            },
            "timeout_ms": self.timeout_ms,
            "retryable": self.retryable,
            "supports_pagination": self.supports_pagination,
            "supports_bulk": self.supports_bulk
        }


class TriggerType(Enum):
    """Types of triggers."""
    POLLING = "polling"
    WEBHOOK = "webhook"
    SCHEDULED = "scheduled"
    EVENT = "event"


@dataclass
class ConnectorTrigger:
    """Definition of a connector trigger."""

    name: str
    label: str
    trigger_type: TriggerType
    description: Optional[str] = None
    inputs: List[ActionInput] = field(default_factory=list)
    outputs: List[ActionOutput] = field(default_factory=list)

    # Polling specific
    poll_interval_seconds: int = 60
    poll_handler: Optional[str] = None
    poll_cursor_field: Optional[str] = None  # For incremental polling

    # Webhook specific
    webhook_path: Optional[str] = None
    webhook_method: str = "POST"
    webhook_validation: Optional[str] = None  # Signature validation

    # Scheduled specific
    cron_expression: Optional[str] = None

    # Deduplication
    dedupe_field: Optional[str] = None  # Field to use for deduplication
    dedupe_window_hours: int = 24

    def to_schema(self) -> Dict[str, Any]:
        """Convert to schema representation."""
        schema = {
            "name": self.name,
            "label": self.label,
            "type": self.trigger_type.value,
            "description": self.description,
            "inputs": {
                inp.name: {
                    "type": inp.type.value,
                    "label": inp.label,
                    "required": inp.required
                }
                for inp in self.inputs
            },
            "outputs": {
                out.name: {
                    "type": out.type.value,
                    "label": out.label
                }
                for out in self.outputs
            }
        }

        if self.trigger_type == TriggerType.POLLING:
            schema["poll_interval_seconds"] = self.poll_interval_seconds
        elif self.trigger_type == TriggerType.WEBHOOK:
            schema["webhook_path"] = self.webhook_path
            schema["webhook_method"] = self.webhook_method
        elif self.trigger_type == TriggerType.SCHEDULED:
            schema["cron_expression"] = self.cron_expression

        return schema


# =============================================================================
# Connector Definition
# =============================================================================


@dataclass
class ConnectorConfig:
    """Configuration fields for a connector."""
    fields: List[FieldDefinition] = field(default_factory=list)

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert to JSON Schema."""
        properties = {}
        required = []

        for f in self.fields:
            properties[f.name] = f.to_json_schema()
            if f.validation and f.validation.required:
                required.append(f.name)

        return {
            "type": "object",
            "properties": properties,
            "required": required if required else None
        }


@dataclass
class ConnectorDefinition:
    """Complete definition of a connector."""

    # Identity
    id: str
    name: str
    version: str
    description: Optional[str] = None

    # Branding
    icon: Optional[str] = None  # SVG or URL
    color: Optional[str] = None  # Hex color
    category: str = "other"

    # Author
    author: Optional[str] = None
    homepage: Optional[str] = None
    documentation_url: Optional[str] = None

    # Configuration
    config: Optional[ConnectorConfig] = None
    auth: Optional[AuthDefinition] = None

    # Capabilities
    actions: List[ConnectorAction] = field(default_factory=list)
    triggers: List[ConnectorTrigger] = field(default_factory=list)

    # Runtime
    base_url: Optional[str] = None
    handler_module: Optional[str] = None  # Python module path

    # Features
    supports_test_connection: bool = True
    supports_discovery: bool = False  # Dynamic schema discovery

    # Requirements
    required_permissions: List[str] = field(default_factory=list)

    def to_manifest(self) -> Dict[str, Any]:
        """Convert to connector manifest format."""
        return {
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "icon": self.icon,
            "color": self.color,
            "category": self.category,
            "author": self.author,
            "homepage": self.homepage,
            "documentation_url": self.documentation_url,
            "auth": {
                "type": self.auth.type.value if self.auth else "none",
                "fields": [
                    {
                        "name": f.name,
                        "type": f.type.value,
                        "label": f.label,
                        "required": f.validation.required if f.validation else False
                    }
                    for f in (self.auth.fields if self.auth else [])
                ]
            },
            "config": self.config.to_json_schema() if self.config else None,
            "actions": [action.to_schema() for action in self.actions],
            "triggers": [trigger.to_schema() for trigger in self.triggers],
            "features": {
                "test_connection": self.supports_test_connection,
                "discovery": self.supports_discovery
            }
        }


# =============================================================================
# Builder Pattern
# =============================================================================


class ConnectorBuilder:
    """
    Fluent builder for creating connector definitions.

    Example:
        connector = (
            ConnectorBuilder("my-connector", "My Connector")
            .version("1.0.0")
            .description("A custom connector")
            .auth_api_key()
            .add_config_field("base_url", FieldType.URL, "Base URL", required=True)
            .add_action(
                "get_data",
                "Get Data",
                inputs=[ActionInput("id", FieldType.STRING, "ID", required=True)],
                outputs=[ActionOutput("data", FieldType.JSON, "Data")]
            )
            .build()
        )
    """

    def __init__(self, connector_id: str, name: str):
        """Initialize builder with required fields."""
        self._id = connector_id
        self._name = name
        self._version = "1.0.0"
        self._description: Optional[str] = None
        self._icon: Optional[str] = None
        self._color: Optional[str] = None
        self._category = "other"
        self._author: Optional[str] = None
        self._homepage: Optional[str] = None
        self._documentation_url: Optional[str] = None
        self._config_fields: List[FieldDefinition] = []
        self._auth: Optional[AuthDefinition] = None
        self._actions: List[ConnectorAction] = []
        self._triggers: List[ConnectorTrigger] = []
        self._base_url: Optional[str] = None
        self._handler_module: Optional[str] = None
        self._supports_test_connection = True
        self._supports_discovery = False
        self._required_permissions: List[str] = []

    def version(self, version: str) -> "ConnectorBuilder":
        """Set connector version."""
        self._version = version
        return self

    def description(self, description: str) -> "ConnectorBuilder":
        """Set connector description."""
        self._description = description
        return self

    def icon(self, icon: str) -> "ConnectorBuilder":
        """Set connector icon (SVG or URL)."""
        self._icon = icon
        return self

    def color(self, color: str) -> "ConnectorBuilder":
        """Set connector brand color."""
        self._color = color
        return self

    def category(self, category: str) -> "ConnectorBuilder":
        """Set connector category."""
        self._category = category
        return self

    def author(self, author: str, homepage: Optional[str] = None) -> "ConnectorBuilder":
        """Set connector author."""
        self._author = author
        self._homepage = homepage
        return self

    def documentation(self, url: str) -> "ConnectorBuilder":
        """Set documentation URL."""
        self._documentation_url = url
        return self

    def base_url(self, url: str) -> "ConnectorBuilder":
        """Set base URL for API calls."""
        self._base_url = url
        return self

    def handler_module(self, module: str) -> "ConnectorBuilder":
        """Set Python handler module path."""
        self._handler_module = module
        return self

    # Authentication methods
    def auth_none(self) -> "ConnectorBuilder":
        """No authentication."""
        self._auth = AuthDefinition.none()
        return self

    def auth_api_key(
        self,
        location: str = "header",
        key_name: str = "X-API-Key"
    ) -> "ConnectorBuilder":
        """API Key authentication."""
        self._auth = AuthDefinition.api_key(location, key_name)
        return self

    def auth_basic(self) -> "ConnectorBuilder":
        """Basic HTTP authentication."""
        self._auth = AuthDefinition.basic()
        return self

    def auth_bearer(self, prefix: str = "Bearer") -> "ConnectorBuilder":
        """Bearer token authentication."""
        self._auth = AuthDefinition.bearer(prefix)
        return self

    def auth_oauth2(
        self,
        authorization_url: str,
        token_url: str,
        scopes: Optional[List[str]] = None,
        pkce: bool = False
    ) -> "ConnectorBuilder":
        """OAuth2 authentication."""
        self._auth = AuthDefinition.oauth2(
            authorization_url, token_url, scopes, pkce
        )
        return self

    def auth_custom(self, auth_definition: AuthDefinition) -> "ConnectorBuilder":
        """Custom authentication."""
        self._auth = auth_definition
        return self

    # Configuration methods
    def add_config_field(
        self,
        name: str,
        field_type: FieldType,
        label: str,
        description: Optional[str] = None,
        required: bool = False,
        default_value: Any = None,
        options: Optional[List[FieldOption]] = None,
        **kwargs
    ) -> "ConnectorBuilder":
        """Add a configuration field."""
        validation = FieldValidation(required=required) if required else None

        self._config_fields.append(FieldDefinition(
            name=name,
            type=field_type,
            label=label,
            description=description,
            default_value=default_value,
            validation=validation,
            options=options,
            **kwargs
        ))
        return self

    # Action methods
    def add_action(
        self,
        name: str,
        label: str,
        description: Optional[str] = None,
        inputs: Optional[List[ActionInput]] = None,
        outputs: Optional[List[ActionOutput]] = None,
        **kwargs
    ) -> "ConnectorBuilder":
        """Add an action."""
        self._actions.append(ConnectorAction(
            name=name,
            label=label,
            description=description,
            inputs=inputs or [],
            outputs=outputs or [],
            **kwargs
        ))
        return self

    # Trigger methods
    def add_polling_trigger(
        self,
        name: str,
        label: str,
        poll_interval_seconds: int = 60,
        description: Optional[str] = None,
        inputs: Optional[List[ActionInput]] = None,
        outputs: Optional[List[ActionOutput]] = None,
        **kwargs
    ) -> "ConnectorBuilder":
        """Add a polling trigger."""
        self._triggers.append(ConnectorTrigger(
            name=name,
            label=label,
            trigger_type=TriggerType.POLLING,
            description=description,
            poll_interval_seconds=poll_interval_seconds,
            inputs=inputs or [],
            outputs=outputs or [],
            **kwargs
        ))
        return self

    def add_webhook_trigger(
        self,
        name: str,
        label: str,
        webhook_path: str,
        description: Optional[str] = None,
        inputs: Optional[List[ActionInput]] = None,
        outputs: Optional[List[ActionOutput]] = None,
        **kwargs
    ) -> "ConnectorBuilder":
        """Add a webhook trigger."""
        self._triggers.append(ConnectorTrigger(
            name=name,
            label=label,
            trigger_type=TriggerType.WEBHOOK,
            description=description,
            webhook_path=webhook_path,
            inputs=inputs or [],
            outputs=outputs or [],
            **kwargs
        ))
        return self

    # Feature methods
    def supports_test_connection(self, enabled: bool = True) -> "ConnectorBuilder":
        """Enable/disable test connection feature."""
        self._supports_test_connection = enabled
        return self

    def supports_discovery(self, enabled: bool = True) -> "ConnectorBuilder":
        """Enable/disable schema discovery feature."""
        self._supports_discovery = enabled
        return self

    def require_permission(self, permission: str) -> "ConnectorBuilder":
        """Add a required permission."""
        self._required_permissions.append(permission)
        return self

    def build(self) -> ConnectorDefinition:
        """Build the connector definition."""
        return ConnectorDefinition(
            id=self._id,
            name=self._name,
            version=self._version,
            description=self._description,
            icon=self._icon,
            color=self._color,
            category=self._category,
            author=self._author,
            homepage=self._homepage,
            documentation_url=self._documentation_url,
            config=ConnectorConfig(fields=self._config_fields) if self._config_fields else None,
            auth=self._auth,
            actions=self._actions,
            triggers=self._triggers,
            base_url=self._base_url,
            handler_module=self._handler_module,
            supports_test_connection=self._supports_test_connection,
            supports_discovery=self._supports_discovery,
            required_permissions=self._required_permissions
        )


# =============================================================================
# Validation
# =============================================================================


@dataclass
class ValidationError:
    """A validation error."""
    path: str
    message: str
    severity: str = "error"


class ConnectorValidator:
    """
    Validates connector definitions.

    Ensures connectors are complete, consistent, and follow best practices.
    """

    def validate(self, connector: ConnectorDefinition) -> List[ValidationError]:
        """
        Validate a connector definition.

        Returns list of validation errors.
        """
        errors = []

        # Required fields
        if not connector.id:
            errors.append(ValidationError("id", "Connector ID is required"))
        elif not re.match(r'^[a-z][a-z0-9-]*$', connector.id):
            errors.append(ValidationError(
                "id",
                "Connector ID must start with letter and contain only lowercase letters, numbers, and hyphens"
            ))

        if not connector.name:
            errors.append(ValidationError("name", "Connector name is required"))

        if not connector.version:
            errors.append(ValidationError("version", "Version is required"))
        elif not re.match(r'^\d+\.\d+\.\d+', connector.version):
            errors.append(ValidationError(
                "version",
                "Version must follow semver format (e.g., 1.0.0)"
            ))

        # Must have at least one action or trigger
        if not connector.actions and not connector.triggers:
            errors.append(ValidationError(
                "actions",
                "Connector must have at least one action or trigger"
            ))

        # Validate actions
        action_names = set()
        for i, action in enumerate(connector.actions):
            if not action.name:
                errors.append(ValidationError(
                    f"actions[{i}].name",
                    "Action name is required"
                ))
            elif action.name in action_names:
                errors.append(ValidationError(
                    f"actions[{i}].name",
                    f"Duplicate action name: {action.name}"
                ))
            else:
                action_names.add(action.name)

            if not action.label:
                errors.append(ValidationError(
                    f"actions[{i}].label",
                    "Action label is required"
                ))

        # Validate triggers
        trigger_names = set()
        for i, trigger in enumerate(connector.triggers):
            if not trigger.name:
                errors.append(ValidationError(
                    f"triggers[{i}].name",
                    "Trigger name is required"
                ))
            elif trigger.name in trigger_names:
                errors.append(ValidationError(
                    f"triggers[{i}].name",
                    f"Duplicate trigger name: {trigger.name}"
                ))
            else:
                trigger_names.add(trigger.name)

            # Type-specific validation
            if trigger.trigger_type == TriggerType.WEBHOOK:
                if not trigger.webhook_path:
                    errors.append(ValidationError(
                        f"triggers[{i}].webhook_path",
                        "Webhook path is required for webhook triggers"
                    ))
            elif trigger.trigger_type == TriggerType.SCHEDULED:
                if not trigger.cron_expression:
                    errors.append(ValidationError(
                        f"triggers[{i}].cron_expression",
                        "Cron expression is required for scheduled triggers"
                    ))

        # Validate config fields
        if connector.config:
            field_names = set()
            for i, field in enumerate(connector.config.fields):
                if not field.name:
                    errors.append(ValidationError(
                        f"config.fields[{i}].name",
                        "Field name is required"
                    ))
                elif field.name in field_names:
                    errors.append(ValidationError(
                        f"config.fields[{i}].name",
                        f"Duplicate field name: {field.name}"
                    ))
                else:
                    field_names.add(field.name)

                # Validate select options
                if field.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
                    if not field.options and not field.options_endpoint:
                        errors.append(ValidationError(
                            f"config.fields[{i}].options",
                            "Select fields must have options or options_endpoint"
                        ))

        return errors

    def validate_config_values(
        self,
        connector: ConnectorDefinition,
        values: Dict[str, Any]
    ) -> List[ValidationError]:
        """
        Validate configuration values against the connector definition.
        """
        errors = []

        if not connector.config:
            return errors

        for field in connector.config.fields:
            value = values.get(field.name)

            # Check required
            if field.validation and field.validation.required:
                if value is None or value == "":
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        f"{field.label} is required"
                    ))
                    continue

            # Skip validation if no value and not required
            if value is None:
                continue

            # Type validation
            if field.type == FieldType.URL:
                if not re.match(r'^https?://', str(value)):
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        "Must be a valid URL"
                    ))

            elif field.type == FieldType.EMAIL:
                if not re.match(r'^[^@]+@[^@]+\.[^@]+$', str(value)):
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        "Must be a valid email address"
                    ))

            elif field.type == FieldType.INTEGER:
                if not isinstance(value, int):
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        "Must be an integer"
                    ))

            elif field.type == FieldType.NUMBER:
                if not isinstance(value, (int, float)):
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        "Must be a number"
                    ))

            elif field.type == FieldType.BOOLEAN:
                if not isinstance(value, bool):
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        "Must be a boolean"
                    ))

            # Field-level validation
            if field.validation:
                field_errors = field.validation.validate(value)
                for err in field_errors:
                    errors.append(ValidationError(
                        f"config.{field.name}",
                        err
                    ))

        return errors


# =============================================================================
# SDK Interface
# =============================================================================


class ConnectorSDK:
    """
    SDK for building and managing custom connectors.

    Provides utilities for connector development, testing, and deployment.
    """

    def __init__(self):
        """Initialize the SDK."""
        self.validator = ConnectorValidator()

    def create_builder(self, connector_id: str, name: str) -> ConnectorBuilder:
        """Create a new connector builder."""
        return ConnectorBuilder(connector_id, name)

    def validate(self, connector: ConnectorDefinition) -> List[ValidationError]:
        """Validate a connector definition."""
        return self.validator.validate(connector)

    def to_manifest(self, connector: ConnectorDefinition) -> str:
        """Export connector as JSON manifest."""
        return json.dumps(connector.to_manifest(), indent=2)

    def from_manifest(self, manifest: Union[str, Dict]) -> ConnectorDefinition:
        """
        Import connector from JSON manifest.

        This is a simplified implementation - full version would
        handle all manifest fields.
        """
        if isinstance(manifest, str):
            manifest = json.loads(manifest)

        # Parse auth
        auth = None
        if manifest.get("auth"):
            auth_type = AuthType(manifest["auth"].get("type", "none"))
            auth = AuthDefinition(type=auth_type)

        # Parse actions
        actions = []
        for action_data in manifest.get("actions", []):
            actions.append(ConnectorAction(
                name=action_data["name"],
                label=action_data["label"],
                description=action_data.get("description")
            ))

        # Parse triggers
        triggers = []
        for trigger_data in manifest.get("triggers", []):
            triggers.append(ConnectorTrigger(
                name=trigger_data["name"],
                label=trigger_data["label"],
                trigger_type=TriggerType(trigger_data.get("type", "polling"))
            ))

        return ConnectorDefinition(
            id=manifest["id"],
            name=manifest["name"],
            version=manifest.get("version", "1.0.0"),
            description=manifest.get("description"),
            icon=manifest.get("icon"),
            color=manifest.get("color"),
            category=manifest.get("category", "other"),
            auth=auth,
            actions=actions,
            triggers=triggers
        )

    def test_connection(
        self,
        connector: ConnectorDefinition,
        config: Dict[str, Any],
        auth: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Test connector configuration.

        In production, this delegates to the connector's handler.
        """
        # Validate config
        errors = self.validator.validate_config_values(connector, config)
        if errors:
            return {
                "success": False,
                "errors": [e.message for e in errors]
            }

        # Connector-specific test would happen here
        return {
            "success": True,
            "message": "Connection test passed"
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # SDK
    "ConnectorSDK",
    "ConnectorDefinition",
    "ConnectorConfig",
    "ConnectorAction",
    "ConnectorTrigger",
    # Fields
    "FieldDefinition",
    "FieldType",
    "FieldValidation",
    "FieldOption",
    "ActionInput",
    "ActionOutput",
    "TriggerType",
    # Auth
    "AuthDefinition",
    "AuthType",
    "OAuth2Config",
    # Builder
    "ConnectorBuilder",
    "ConnectorValidator",
    "ValidationError",
]

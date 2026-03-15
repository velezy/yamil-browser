"""
Models module for Logic Weaver.
"""

from assemblyline_common.models.common import (
    Base,
    Role,
    Tenant,
    User,
    UserInvitation,
    UserRegistrationRequest,
    APIKey,
    OAuthToken,
    LogicWeaverFlow,
    FlowExecution,
    FlowVersion,
    AuditTrail,
    APICallLog,
    OutboundAPICallLog,
    KafkaMessageLog,
    Connector,
    EmailTemplate,
    ConnectorUsage,
    SAMLIdentityProvider,
    OIDCIdentityProvider,
    LDAPProvider,
    # API Builder models
    ApiCollection,
    ApiRequest,
    ApiTestResult,
    SftpConnection,
    WebSocketConnection,
    GraphQLEndpoint,
    ApiRequestHistory,
    # AI Prompt models
    AIPrompt,
    AIPromptTest,
    # Policy Bundle models (V031)
    PolicyBundle,
    # Policy Type Definition models (V034)
    PolicyTypeDefinition,
    # RPA Browser Automation models (V076)
    RPACredential,
    # AI Model Catalog (V102)
    AIModel,
    # CDC Subscriptions (V103)
    CDCSubscription,
    CDCExecutionLog,
)
from assemblyline_common.models.ai_chat import (
    AIConversation,
    AIMessage,
    PortableUUID,
    PortableJSONB,
)
from assemblyline_common.models.gateway import (
    GatewayRoute,
    GatewayConsumer,
    GatewayRouteAccess,
)

__all__ = [
    "Base",
    "Role",
    "Tenant",
    "User",
    "UserInvitation",
    "UserRegistrationRequest",
    "APIKey",
    "OAuthToken",
    "LogicWeaverFlow",
    "FlowExecution",
    "FlowVersion",
    "AuditTrail",
    "APICallLog",
    "OutboundAPICallLog",
    "KafkaMessageLog",
    "Connector",
    "EmailTemplate",
    "ConnectorUsage",
    "SAMLIdentityProvider",
    "OIDCIdentityProvider",
    "LDAPProvider",
    # API Builder models
    "ApiCollection",
    "ApiRequest",
    "ApiTestResult",
    "SftpConnection",
    "WebSocketConnection",
    "GraphQLEndpoint",
    "ApiRequestHistory",
    # AI Prompt models
    "AIPrompt",
    "AIPromptTest",
    # Policy Bundle models (V031)
    "PolicyBundle",
    # Policy Type Definition models (V034)
    "PolicyTypeDefinition",
    # RPA Browser Automation models (V076)
    "RPACredential",
    # AI Model Catalog (V102)
    "AIModel",
    # CDC Subscriptions (V103)
    "CDCSubscription",
    "CDCExecutionLog",
    # AI Chat History models (V063)
    "AIConversation",
    "AIMessage",
    "PortableUUID",
    "PortableJSONB",
    # Gateway models (V065)
    "GatewayRoute",
    "GatewayConsumer",
    "GatewayRouteAccess",
]

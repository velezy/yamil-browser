"""
Universal Input/Output Adapters for Logic Weaver.

Enterprise-grade adapters for receiving and sending data across protocols,
queues, and databases. Designed for high-throughput, multi-tenant environments.

Adapter Categories:
- Protocol Adapters: HTTP/REST, MLLP, WebSocket, gRPC, SFTP, Email
- Queue Adapters: RabbitMQ, AWS SQS, Azure Service Bus, Google Pub/Sub, IBM MQ
- Database CDC: PostgreSQL, MySQL, Oracle change data capture

Key Features:
- Unified adapter interface
- Flow node integration
- Multi-tenant isolation
- Automatic retry and circuit breaking
- Metrics and observability built-in
"""

from assemblyline_common.adapters.protocol_adapters import (
    # Base
    ProtocolAdapter,
    AdapterConfig,
    AdapterMessage,
    AdapterResult,
    # HTTP
    HTTPAdapter,
    HTTPAdapterConfig,
    HTTPMethod,
    # WebSocket
    WebSocketAdapter,
    WebSocketAdapterConfig,
    WebSocketMessage,
    # gRPC
    GRPCAdapter,
    GRPCAdapterConfig,
    # SFTP
    SFTPAdapter,
    SFTPAdapterConfig,
    SFTPOperation,
    # Email
    EmailAdapter,
    EmailAdapterConfig,
    EmailMessage,
    EmailProtocol,
    # MLLP
    MLLPAdapter,
    MLLPAdapterConfig,
    # Factory
    get_protocol_adapter,
    PROTOCOL_ADAPTERS,
)

from assemblyline_common.adapters.queue_adapters import (
    # Base
    QueueAdapter,
    QueueConfig,
    QueueMessage,
    QueueResult,
    # RabbitMQ
    RabbitMQAdapter,
    RabbitMQConfig,
    # AWS SQS
    SQSAdapter,
    SQSConfig,
    # Azure Service Bus
    AzureServiceBusAdapter,
    AzureServiceBusConfig,
    # Google Pub/Sub
    PubSubAdapter,
    PubSubConfig,
    # IBM MQ
    IBMMQAdapter,
    IBMMQConfig,
    # Factory
    get_queue_adapter,
    QUEUE_ADAPTERS,
)

from assemblyline_common.adapters.cdc_adapters import (
    # Base
    CDCAdapter,
    CDCConfig,
    CDCEvent,
    CDCOperation,
    CDCPosition,
    # PostgreSQL
    PostgreSQLCDCAdapter,
    PostgreSQLCDCConfig,
    # MySQL
    MySQLCDCAdapter,
    MySQLCDCConfig,
    # Oracle
    OracleCDCAdapter,
    OracleCDCConfig,
    # Factory
    get_cdc_adapter,
    CDC_ADAPTERS,
)

__all__ = [
    # Protocol Adapters
    "ProtocolAdapter",
    "AdapterConfig",
    "AdapterMessage",
    "AdapterResult",
    "HTTPAdapter",
    "HTTPAdapterConfig",
    "HTTPMethod",
    "WebSocketAdapter",
    "WebSocketAdapterConfig",
    "WebSocketMessage",
    "GRPCAdapter",
    "GRPCAdapterConfig",
    "SFTPAdapter",
    "SFTPAdapterConfig",
    "SFTPOperation",
    "EmailAdapter",
    "EmailAdapterConfig",
    "EmailMessage",
    "EmailProtocol",
    "MLLPAdapter",
    "MLLPAdapterConfig",
    "get_protocol_adapter",
    "PROTOCOL_ADAPTERS",
    # Queue Adapters
    "QueueAdapter",
    "QueueConfig",
    "QueueMessage",
    "QueueResult",
    "RabbitMQAdapter",
    "RabbitMQConfig",
    "SQSAdapter",
    "SQSConfig",
    "AzureServiceBusAdapter",
    "AzureServiceBusConfig",
    "PubSubAdapter",
    "PubSubConfig",
    "IBMMQAdapter",
    "IBMMQConfig",
    "get_queue_adapter",
    "QUEUE_ADAPTERS",
    # CDC Adapters
    "CDCAdapter",
    "CDCConfig",
    "CDCEvent",
    "CDCOperation",
    "CDCPosition",
    "PostgreSQLCDCAdapter",
    "PostgreSQLCDCConfig",
    "MySQLCDCAdapter",
    "MySQLCDCConfig",
    "OracleCDCAdapter",
    "OracleCDCConfig",
    "get_cdc_adapter",
    "CDC_ADAPTERS",
]

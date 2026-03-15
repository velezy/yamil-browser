"""
Connectors module for Logic Weaver.

Provides enterprise-grade connectors with:
- Circuit breaker integration
- Connection pooling
- Retry with exponential backoff
- mTLS support
- Rate limiting
- KMS encryption
- Multi-tenant isolation

Uses lazy imports to avoid loading all connectors (and their heavy deps)
when only one is needed. Import directly from submodules for best performance:
    from assemblyline_common.connectors.http_connector import HTTPConnector
"""

import importlib as _importlib

# Map of attribute name → (module_path, attribute_name)
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {}

def _register(module: str, *names: str) -> None:
    for name in names:
        _LAZY_IMPORTS[name] = (module, name)

# HTTP
_register("assemblyline_common.connectors.http_connector",
    "HTTPConnector", "HTTPConnectorConfig", "HTTPResponse", "get_http_connector")
# MLLP
_register("assemblyline_common.connectors.mllp_connector",
    "MLLPConnector", "MLLPConnectorConfig", "MLLPMessage", "get_mllp_connector")
# Kafka
_register("assemblyline_common.connectors.kafka_connector",
    "KafkaProducer", "KafkaConsumer", "KafkaConfig", "get_kafka_producer", "get_kafka_consumer")
# S3
_register("assemblyline_common.connectors.s3_connector",
    "S3Connector", "S3ConnectorConfig", "S3Object", "EncryptionType", "get_s3_connector")
# Database
_register("assemblyline_common.connectors.database_connector",
    "PostgresConnector", "PostgresConfig", "RedisConnector", "RedisConfig", "ReplicaMode",
    "get_postgres_connector", "get_redis_connector")
# Email
_register("assemblyline_common.connectors.email_connector",
    "SMTPConnector", "SMTPConfig", "SESConnector", "SESConfig", "EmailMessage", "EmailAttachment",
    "get_smtp_connector", "get_ses_connector")
# AI
_register("assemblyline_common.connectors.ai_connector",
    "BedrockConnector", "BedrockConfig", "AzureOpenAIConnector", "AzureOpenAIConfig", "AIResponse",
    "get_bedrock_connector", "get_azure_openai_connector")
# FHIR
_register("assemblyline_common.connectors.fhir_connector",
    "FHIRConnector", "FHIRConnectorConfig", "FHIRResource", "FHIRBundle", "get_fhir_connector")
# Schema Registry
_register("assemblyline_common.connectors.schema_registry_connector",
    "SchemaRegistryConnector", "SchemaRegistryConfig", "SchemaType", "CompatibilityLevel",
    "get_schema_registry_connector")
# DynamoDB
_register("assemblyline_common.connectors.dynamodb_connector",
    "DynamoDBConnector", "DynamoDBConfig", "CapacityMode", "get_dynamodb_connector")
# Email Providers
_register("assemblyline_common.connectors.email_provider_connector",
    "SendGridConnector", "SendGridConfig", "MailgunConnector", "MailgunConfig",
    "get_sendgrid_connector", "get_mailgun_connector")
# X12 EDI
_register("assemblyline_common.connectors.x12_connector",
    "X12ParserNode", "X12ValidatorNode", "X12SplitterNode", "X12ServiceAreaNode",
    "X12NodeConfig", "X12NodeResult", "get_x12_node", "get_x12_node_definitions")
# Universal Query
_register("assemblyline_common.connectors.query_connector",
    "UniversalQueryConnector", "QueryConfig", "QueryResult", "DatabaseType",
    "QueryNode", "QueryNodeConfig", "QueryNodeResult", "get_query_connector", "get_query_node_definition")
# Salesforce
_register("assemblyline_common.connectors.salesforce_connector",
    "SalesforceConnector", "SalesforceConfig", "SalesforceAuthType", "SalesforceRecord",
    "BulkJobResult", "BulkOperation", "SalesforceNode", "SalesforceNodeConfig", "SalesforceNodeResult",
    "get_salesforce_connector", "get_salesforce_node_definition")
# SFTP
_register("assemblyline_common.connectors.sftp_connector",
    "SFTPConnector", "SFTPConfig", "SFTPFile", "SFTPResult", "SFTPNode", "SFTPNodeConfig",
    "SFTPNodeResult", "get_sftp_connector", "get_sftp_node_definition")
# SQS
_register("assemblyline_common.connectors.sqs_connector",
    "SQSConnector", "SQSConfig", "SQSMessage", "SQSResult", "SQSNode", "SQSNodeConfig",
    "SQSNodeResult", "get_sqs_connector", "get_sqs_node_definition")
# Azure Service Bus
_register("assemblyline_common.connectors.servicebus_connector",
    "ServiceBusConnector", "ServiceBusConfig", "ServiceBusMessage", "ServiceBusResult",
    "ServiceBusNode", "ServiceBusNodeConfig", "ServiceBusNodeResult",
    "get_servicebus_connector", "get_servicebus_node_definition")
# RabbitMQ
_register("assemblyline_common.connectors.rabbitmq_connector",
    "RabbitMQConnector", "RabbitMQConfig", "RabbitMQMessage", "RabbitMQResult",
    "RabbitMQNode", "RabbitMQNodeConfig", "RabbitMQNodeResult",
    "get_rabbitmq_connector", "get_rabbitmq_node_definition")
# WebSocket
_register("assemblyline_common.connectors.websocket_connector",
    "WebSocketConnector", "WebSocketConfig", "WebSocketMessage", "WebSocketResult",
    "WebSocketNode", "WebSocketNodeConfig", "WebSocketNodeResult",
    "get_websocket_connector", "get_websocket_node_definition")
# gRPC
_register("assemblyline_common.connectors.grpc_connector",
    "GrpcConnector", "GrpcConfig", "GrpcMessage", "GrpcResult",
    "GrpcNode", "GrpcNodeConfig", "GrpcNodeResult", "get_grpc_connector", "get_grpc_node_definition")
# PostgreSQL CDC
_register("assemblyline_common.connectors.postgres_cdc_connector",
    "PostgresCDCConnector", "get_postgres_cdc_connector")
# Google Pub/Sub
_register("assemblyline_common.connectors.pubsub_connector",
    "PubSubConnector", "PubSubConfig", "PubSubMessage", "PublishResult",
    "PubSubNode", "PubSubNodeConfig", "PubSubNodeResult", "get_pubsub_connector", "get_pubsub_node_definition")
# IMAP
_register("assemblyline_common.connectors.imap_connector",
    "IMAPConnector", "IMAPConfig", "IMAPResult", "IMAPNode", "IMAPNodeConfig", "IMAPNodeResult",
    "get_imap_connector", "get_imap_node_definition")
# IBM MQ
_register("assemblyline_common.connectors.ibmmq_connector",
    "IBMMQConnector", "MQConfig", "MQMessage", "MQResult",
    "MQNode", "MQNodeConfig", "MQNodeResult", "get_ibmmq_connector", "get_mq_node_definition")
# MySQL CDC
_register("assemblyline_common.connectors.mysql_cdc_connector",
    "MySQLCDCConnector", "MySQLCDCConfig", "MySQLCDCChange", "MySQLCDCResult",
    "MySQLCDCNode", "MySQLCDCNodeConfig", "MySQLCDCNodeResult",
    "get_mysql_cdc_connector", "get_mysql_cdc_node_definition")
# MongoDB CDC
_register("assemblyline_common.connectors.mongodb_cdc_connector",
    "MongoDBCDCConnector", "MongoDBCDCConfig", "MongoDBCDCChange", "MongoDBCDCResult",
    "MongoDBCDCNode", "MongoDBCDCNodeConfig", "MongoDBCDCNodeResult",
    "get_mongodb_cdc_connector", "get_mongodb_cdc_node_definition")

# Rate Limiter
_register("assemblyline_common.connectors.rate_limiter",
    "ConnectorRateLimiter", "ConnectorRateLimitExceeded", "ConnectorRateLimitResult",
    "get_connector_rate_limiter", "CONNECTOR_TYPE_LIMITS")

# Aliases with different names
_LAZY_IMPORTS["QueryOutputFormat"] = ("assemblyline_common.connectors.query_connector", "OutputFormat")
_LAZY_IMPORTS["SalesforceQueryResult"] = ("assemblyline_common.connectors.salesforce_connector", "QueryResult")
_LAZY_IMPORTS["IMAPEmailMessage"] = ("assemblyline_common.connectors.imap_connector", "EmailMessage")
_LAZY_IMPORTS["IMAPEmailAttachment"] = ("assemblyline_common.connectors.imap_connector", "EmailAttachment")
_LAZY_IMPORTS["PostgresCDCConfig"] = ("assemblyline_common.connectors.postgres_cdc_connector", "CDCConfig")
_LAZY_IMPORTS["PostgresCDCChange"] = ("assemblyline_common.connectors.postgres_cdc_connector", "CDCChange")
_LAZY_IMPORTS["PostgresCDCResult"] = ("assemblyline_common.connectors.postgres_cdc_connector", "CDCResult")
_LAZY_IMPORTS["PostgresCDCNode"] = ("assemblyline_common.connectors.postgres_cdc_connector", "CDCNode")
_LAZY_IMPORTS["PostgresCDCNodeConfig"] = ("assemblyline_common.connectors.postgres_cdc_connector", "CDCNodeConfig")
_LAZY_IMPORTS["PostgresCDCNodeResult"] = ("assemblyline_common.connectors.postgres_cdc_connector", "CDCNodeResult")
_LAZY_IMPORTS["get_postgres_cdc_node_definition"] = ("assemblyline_common.connectors.postgres_cdc_connector", "get_cdc_node_definition")


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module_path, attr_name = _LAZY_IMPORTS[name]
        module = _importlib.import_module(module_path)
        return getattr(module, attr_name)
    raise AttributeError(f"module 'assemblyline_common.connectors' has no attribute {name!r}")


__all__ = list(_LAZY_IMPORTS.keys())

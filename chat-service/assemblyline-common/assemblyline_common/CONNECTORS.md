# Logic Weaver Connectors & Parsers

Enterprise-grade connectors and parsers for universal payload processing. Built to surpass MuleSoft and Apigee in flexibility, performance, and healthcare integration capabilities.

## Overview

| Category | Connectors | Status |
|----------|------------|--------|
| **Message Queues** | SQS, Azure Service Bus, RabbitMQ, IBM MQ, Google Pub/Sub | Complete |
| **Protocols** | HTTP, MLLP, gRPC, WebSocket, SFTP | Complete |
| **Databases** | PostgreSQL, Redis, DynamoDB | Complete |
| **CDC (Change Data Capture)** | PostgreSQL CDC, MySQL CDC, MongoDB CDC | Complete |
| **Email** | SMTP, SES, SendGrid, Mailgun, IMAP | Complete |
| **Cloud Storage** | S3 | Complete |
| **Healthcare** | FHIR, HL7 v2, X12 EDI, CCDA | Complete |
| **AI/ML** | AWS Bedrock, Azure OpenAI | Complete |
| **CRM** | Salesforce | Complete |
| **Parsers** | Flat File (CSV, Fixed-width), PDF, X12 | Complete |

---

## Message Queue Connectors

### AWS SQS Connector
Enterprise SQS integration with FIFO support, dead-letter queues, and batch operations.

```python
from logic_weaver_common.connectors import SQSConnector, SQSConfig

config = SQSConfig(
    queue_url="https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
    region="us-east-1",
    batch_size=10,
    visibility_timeout=30
)

async with SQSConnector(config) as sqs:
    # Send message
    result = await sqs.send({"order_id": "123", "status": "new"})

    # Receive messages
    messages = await sqs.receive(max_messages=10)
    for msg in messages:
        process(msg)
        await sqs.delete(msg)
```

**Features:**
- Standard and FIFO queue support
- Message batching (up to 10 messages)
- Dead letter queue integration
- Long polling
- Message deduplication (FIFO)
- Visibility timeout management

### Azure Service Bus Connector
Full Azure Service Bus support with queues, topics, sessions, and scheduled messages.

```python
from logic_weaver_common.connectors import ServiceBusConnector, ServiceBusConfig

config = ServiceBusConfig(
    connection_string="Endpoint=sb://...",
    queue_name="orders",
    session_enabled=True
)

async with ServiceBusConnector(config) as sb:
    # Send to queue
    await sb.send({"event": "order_created"})

    # Receive with sessions
    async for msg in sb.receive():
        process(msg)
        await sb.complete(msg)
```

**Features:**
- Queue and Topic/Subscription support
- Session-based processing for ordered delivery
- Scheduled message delivery
- Dead letter handling
- Managed Identity authentication

### RabbitMQ Connector
AMQP 0-9-1 connector with exchange support, consumer prefetch, and publisher confirms.

```python
from logic_weaver_common.connectors import RabbitMQConnector, RabbitMQConfig

config = RabbitMQConfig(
    host="rabbitmq.example.com",
    queue_name="orders",
    exchange_name="events",
    exchange_type="topic"
)

async with RabbitMQConnector(config) as rmq:
    # Publish with routing key
    await rmq.publish(
        data={"order_id": "123"},
        routing_key="orders.new"
    )

    # Consume messages
    async for msg in rmq.consume():
        process(msg)
        await rmq.ack(msg)
```

**Features:**
- Queue and Exchange support (direct, fanout, topic, headers)
- Publisher confirms
- Consumer prefetch
- Dead letter exchanges
- TLS support

### IBM MQ Connector
Enterprise IBM MQ integration with message groups and transaction support.

```python
from logic_weaver_common.connectors import IBMMQConnector, MQConfig

config = MQConfig(
    queue_manager="QM1",
    channel="DEV.APP.SVRCONN",
    host="mq.example.com",
    queue_name="DEV.QUEUE.1"
)

async with IBMMQConnector(config) as mq:
    # Put message
    await mq.put({"data": "payload"})

    # Get message
    msg = await mq.get(wait=5000)

    # Request/Reply
    reply = await mq.request_reply(
        data={"request": "data"},
        reply_queue="REPLY.QUEUE"
    )
```

**Features:**
- Queue put/get operations
- Message browsing
- Message groups and sequences
- Request/Reply pattern
- SSL/TLS authentication

### Google Pub/Sub Connector
GCP Pub/Sub with ordering, filtering, and exactly-once delivery.

```python
from logic_weaver_common.connectors import PubSubConnector, PubSubConfig

config = PubSubConfig(
    project_id="my-project",
    topic_id="orders",
    subscription_id="orders-sub"
)

async with PubSubConnector(config) as pubsub:
    # Publish
    await pubsub.publish(
        data={"order_id": "123"},
        attributes={"type": "new_order"}
    )

    # Subscribe
    async for msg in pubsub.subscribe():
        process(msg)
        await pubsub.ack(msg)
```

**Features:**
- Topic and Subscription management
- Message ordering with ordering keys
- Dead letter handling
- Message filtering
- Batch publishing

---

## Protocol Connectors

### WebSocket Connector
Real-time bidirectional communication with auto-reconnect.

```python
from logic_weaver_common.connectors import WebSocketConnector, WebSocketConfig

config = WebSocketConfig(
    url="wss://api.example.com/ws",
    auth_token="my-token",
    auto_reconnect=True
)

async with WebSocketConnector(config) as ws:
    # Send
    await ws.send({"type": "subscribe", "channel": "orders"})

    # Receive stream
    async for msg in ws.receive():
        process(msg)
```

**Features:**
- Client mode with auto-reconnect
- Heartbeat/ping-pong
- SSL/TLS support
- Message compression
- Binary and text messages

### gRPC Connector
High-performance RPC with all streaming modes.

```python
from logic_weaver_common.connectors import GrpcConnector, GrpcConfig

config = GrpcConfig(
    host="api.example.com",
    port=50051,
    auth_type="mtls",
    ssl_cert="/path/to/cert.pem"
)

async with GrpcConnector(config) as grpc:
    # Unary call
    result = await grpc.call(
        service="UserService",
        method="GetUser",
        request={"user_id": "123"}
    )

    # Server streaming
    async for msg in grpc.stream(
        service="EventService",
        method="Subscribe",
        request={"channel": "orders"}
    ):
        process(msg)
```

**Features:**
- Unary, client/server/bidirectional streaming
- TLS/mTLS authentication
- Interceptors for auth and logging
- Health checking
- Retry policies

### SFTP Connector
Secure file transfer with PGP encryption support.

```python
from logic_weaver_common.connectors import SFTPConnector, SFTPConfig

config = SFTPConfig(
    host="sftp.example.com",
    username="user",
    private_key_path="/path/to/key",
    remote_path="/incoming"
)

async with SFTPConnector(config) as sftp:
    # List files
    files = await sftp.list_files(pattern="*.csv")

    # Download with PGP decryption
    content = await sftp.download("data.csv.pgp", decrypt=True)

    # Upload with PGP encryption
    await sftp.upload("report.csv", content, encrypt=True)
```

**Features:**
- SFTP/FTPS/FTP support
- SSH key and password auth
- PGP encryption/decryption
- File pattern matching
- Recursive operations

---

## CDC (Change Data Capture) Connectors

### PostgreSQL CDC
Real-time change capture using logical replication.

```python
from logic_weaver_common.connectors import PostgresCDCConnector, PostgresCDCConfig

config = PostgresCDCConfig(
    host="localhost",
    database="mydb",
    tables=["orders", "customers"],
    slot_name="logic_weaver_cdc"
)

async with PostgresCDCConnector(config) as cdc:
    async for change in cdc.stream_changes():
        print(f"{change.operation} on {change.table}")
        print(f"Data: {change.data}")
        print(f"Old: {change.old_data}")
```

**Features:**
- Logical replication (pgoutput)
- INSERT/UPDATE/DELETE tracking
- Resume from LSN position
- Transaction boundaries
- Schema change detection

### MySQL CDC
Binlog-based change capture with GTID support.

```python
from logic_weaver_common.connectors import MySQLCDCConnector, MySQLCDCConfig

config = MySQLCDCConfig(
    host="localhost",
    user="replication_user",
    tables=["mydb.orders", "mydb.customers"]
)

async with MySQLCDCConnector(config) as cdc:
    async for change in cdc.stream_changes():
        print(f"{change.operation}: {change.data}")
```

**Features:**
- Binary log streaming
- GTID-based positioning
- Row-based replication
- Multi-table filtering

### MongoDB CDC
Change streams for real-time document tracking.

```python
from logic_weaver_common.connectors import MongoDBCDCConnector, MongoDBCDCConfig

config = MongoDBCDCConfig(
    uri="mongodb://localhost:27017",
    database="mydb",
    collection="orders"
)

async with MongoDBCDCConnector(config) as cdc:
    async for change in cdc.stream_changes():
        print(f"{change.operation}: {change.full_document}")
```

**Features:**
- Change streams API
- Full document lookup
- Resume token support
- Pipeline filtering

---

## Email Connectors

### IMAP Connector
Full IMAP support with IDLE for real-time notifications.

```python
from logic_weaver_common.connectors import IMAPConnector, IMAPConfig

config = IMAPConfig(
    host="imap.gmail.com",
    username="user@gmail.com",
    password="app-password"
)

async with IMAPConnector(config) as imap:
    # Fetch unread emails
    emails = await imap.fetch_emails(
        criteria="UNSEEN",
        limit=10
    )

    for email in emails:
        print(f"From: {email.from_address}")
        print(f"Subject: {email.subject}")
        await imap.mark_read(email.uid)

    # Real-time notifications
    async for email in imap.idle():
        process_new_email(email)
```

**Features:**
- IMAP/IMAPS support
- OAuth2 authentication
- Folder management
- Advanced search
- IDLE for push notifications
- Attachment extraction

---

## Parsers

### Flat File Parser
Universal parser for CSV, TSV, fixed-width, and pipe-delimited files.

```python
from logic_weaver_common.parsers import FlatFileParser, FlatFileConfig, FlatFileField

# Fixed-width parsing
config = FlatFileConfig(
    file_type="fixed_width",
    fields=[
        FlatFileField(name="id", start=0, length=10, data_type="integer"),
        FlatFileField(name="name", start=10, length=30, data_type="string"),
        FlatFileField(name="amount", start=40, length=12, data_type="decimal"),
    ]
)

parser = FlatFileParser(config)
result = parser.parse("/path/to/data.txt")

# CSV with auto-detection
config = FlatFileConfig(file_type="csv", has_header=True)
parser = FlatFileParser(config)
result = parser.parse("/path/to/data.csv")

for record in result.records:
    print(record.data)
```

**Features:**
- Fixed-width, CSV, TSV, pipe-delimited
- Header/trailer record handling
- Data type conversion
- Field validation
- Auto-delimiter detection

### PDF Extractor
Comprehensive PDF extraction with OCR support.

```python
from logic_weaver_common.parsers import PDFExtractor, PDFConfig, ExtractionMode

config = PDFConfig(
    mode=ExtractionMode.ALL,
    extract_images=True,
    use_ocr=True
)

extractor = PDFExtractor(config)
result = extractor.extract("/path/to/document.pdf")

print(f"Text: {result.full_text}")
print(f"Tables: {result.all_tables}")
print(f"Form fields: {result.form_fields}")
```

**Features:**
- Text extraction (layout-aware)
- Table extraction
- Form field extraction
- Image extraction
- OCR for scanned documents
- Metadata extraction

---

## Flow Node Integration

All connectors include flow node integration for the visual workflow builder:

```python
# Get node definition for UI
from logic_weaver_common.connectors import get_sqs_node_definition
definition = get_sqs_node_definition()

# Execute node in flow
from logic_weaver_common.connectors import SQSNode, SQSNodeConfig

node = SQSNode(SQSNodeConfig(
    queue_url="https://sqs...",
    operation="receive"
))
result = await node.execute({})
```

Each node provides:
- `config_schema` - UI configuration schema
- `inputs` - Input ports
- `outputs` - Output ports
- `execute()` - Async execution method

---

## Dependencies

Install optional dependencies based on your needs:

```bash
# Message Queues
pip install aiobotocore aio-pika azure-servicebus google-cloud-pubsub pymqi

# Protocols
pip install websockets grpcio paramiko

# CDC
pip install psycopg pymysqlreplication motor

# Email
pip install aioimaplib

# Parsers
pip install pdfplumber PyPDF2 pytesseract pdf2image
```

---

## Enterprise Features

All connectors include:

- **Circuit Breaker**: Automatic failure handling with configurable thresholds
- **Connection Pooling**: Efficient resource management
- **Retry with Backoff**: Exponential backoff for transient failures
- **mTLS Support**: Mutual TLS authentication
- **Multi-tenant Isolation**: Tenant-aware operations
- **Async/Await**: Full async support for high throughput
- **Flow Node Integration**: Ready for visual workflow builder

---

## Comparison with Competitors

| Feature | MuleSoft | Apigee | Logic Weaver |
|---------|----------|--------|--------------|
| Message Queues | Plugin-based | Limited | Native |
| CDC Support | No | No | PostgreSQL, MySQL, MongoDB |
| WebSocket | Limited | No | Full |
| gRPC | Limited | Yes | Full streaming |
| SFTP + PGP | Plugin | No | Native |
| Healthcare (HL7, X12, FHIR) | Plugin | No | Native |
| PDF Extraction | No | No | Native |
| Flow Builder Integration | Yes | Limited | Native |
| Python Native | No | No | Yes |
| Open Source | No | No | Yes |

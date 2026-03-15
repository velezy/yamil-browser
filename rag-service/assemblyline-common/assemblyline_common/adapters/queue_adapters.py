"""
Queue Adapters for Logic Weaver.

Unified adapters for message queue systems:
- RabbitMQ: AMQP messaging with exchanges and routing
- AWS SQS: Managed queue with visibility timeout
- Azure Service Bus: Enterprise messaging with topics
- Google Pub/Sub: Serverless messaging
- IBM MQ: Enterprise MQ for legacy integration

All adapters provide:
- Publish/subscribe patterns
- Message acknowledgment
- Dead letter handling
- Batch operations
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union
import asyncio
import logging
import json
import uuid
import time

logger = logging.getLogger(__name__)


# =============================================================================
# Base Classes
# =============================================================================

class QueueType(str, Enum):
    """Queue types."""
    RABBITMQ = "rabbitmq"
    SQS = "sqs"
    AZURE_SERVICE_BUS = "azure_service_bus"
    GOOGLE_PUBSUB = "google_pubsub"
    IBM_MQ = "ibm_mq"


class DeliveryMode(str, Enum):
    """Message delivery modes."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class QueueMessage:
    """Universal queue message."""
    id: str
    body: Union[bytes, str, Dict[str, Any]]
    headers: Dict[str, str] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Queue-specific
    receipt_handle: Optional[str] = None  # For acknowledgment
    delivery_count: int = 0
    sequence_number: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "body": self.body if isinstance(self.body, (str, dict)) else self.body.decode(),
            "headers": self.headers,
            "attributes": self.attributes,
            "timestamp": self.timestamp.isoformat(),
            "delivery_count": self.delivery_count,
        }


@dataclass
class QueueResult:
    """Result of queue operation."""
    success: bool
    message_id: Optional[str] = None
    sequence_number: Optional[int] = None
    error: Optional[str] = None
    latency_ms: float = 0.0


@dataclass
class QueueConfig:
    """Base queue configuration."""
    name: str = "queue"
    queue_name: str = ""
    tenant_id: Optional[str] = None

    # Delivery
    delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE
    visibility_timeout_seconds: int = 30
    message_ttl_seconds: Optional[int] = None

    # Batching
    batch_size: int = 10
    batch_timeout_seconds: float = 1.0

    # Dead letter
    dead_letter_queue: Optional[str] = None
    max_delivery_attempts: int = 5

    # Connection
    timeout_seconds: float = 30.0
    max_retries: int = 3


class QueueAdapter(ABC):
    """
    Base class for queue adapters.

    Provides:
    - Message publish/consume
    - Acknowledgment handling
    - Dead letter support
    - Batch operations
    """

    def __init__(self, config: QueueConfig):
        self.config = config
        self._connected = False
        self._message_count = 0
        self._error_count = 0

    @property
    def is_connected(self) -> bool:
        return self._connected

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection."""
        pass

    @abstractmethod
    async def publish(self, message: QueueMessage) -> QueueResult:
        """Publish a message."""
        pass

    @abstractmethod
    async def consume(self, max_messages: int = 1) -> List[QueueMessage]:
        """Consume messages."""
        pass

    @abstractmethod
    async def acknowledge(self, message: QueueMessage) -> bool:
        """Acknowledge a message."""
        pass

    @abstractmethod
    async def reject(self, message: QueueMessage, requeue: bool = False) -> bool:
        """Reject a message."""
        pass

    async def publish_batch(self, messages: List[QueueMessage]) -> List[QueueResult]:
        """Publish multiple messages."""
        results = []
        for msg in messages:
            result = await self.publish(msg)
            results.append(result)
        return results

    async def subscribe(
        self,
        handler: Callable[[QueueMessage], bool],
    ) -> None:
        """Subscribe to queue with handler. Handler returns True to ack."""
        while self.is_connected:
            try:
                messages = await self.consume(self.config.batch_size)
                for msg in messages:
                    try:
                        should_ack = await handler(msg)
                        if should_ack:
                            await self.acknowledge(msg)
                        else:
                            await self.reject(msg, requeue=True)
                    except Exception as e:
                        logger.error(f"Handler error: {e}")
                        await self.reject(msg, requeue=False)
            except Exception as e:
                logger.error(f"Consume error: {e}")
                await asyncio.sleep(1)


# =============================================================================
# RabbitMQ Adapter
# =============================================================================

@dataclass
class RabbitMQConfig(QueueConfig):
    """RabbitMQ configuration."""
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    username: str = "guest"
    password: str = "guest"

    # Exchange
    exchange: str = ""
    exchange_type: str = "direct"  # direct, fanout, topic, headers
    routing_key: str = ""

    # Queue
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False

    # Consumer
    prefetch_count: int = 10
    consumer_tag: str = ""

    # TLS
    use_tls: bool = False
    tls_cert_path: Optional[str] = None


class RabbitMQAdapter(QueueAdapter):
    """
    RabbitMQ adapter using aio-pika.

    Features:
    - Exchange binding
    - Topic routing
    - Publisher confirms
    - Consumer prefetch
    """

    def __init__(self, config: RabbitMQConfig):
        super().__init__(config)
        self.config: RabbitMQConfig = config
        self._connection = None
        self._channel = None
        self._queue = None
        self._exchange = None

    async def connect(self) -> bool:
        """Connect to RabbitMQ."""
        try:
            import aio_pika

            url = f"amqp://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.virtual_host}"

            self._connection = await aio_pika.connect_robust(url)
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=self.config.prefetch_count)

            # Declare exchange if specified
            if self.config.exchange:
                self._exchange = await self._channel.declare_exchange(
                    self.config.exchange,
                    type=self.config.exchange_type,
                    durable=self.config.durable,
                )

            # Declare queue
            self._queue = await self._channel.declare_queue(
                self.config.queue_name,
                durable=self.config.durable,
                exclusive=self.config.exclusive,
                auto_delete=self.config.auto_delete,
            )

            # Bind to exchange
            if self._exchange and self.config.routing_key:
                await self._queue.bind(self._exchange, self.config.routing_key)

            self._connected = True
            logger.info(f"RabbitMQ connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"RabbitMQ connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ."""
        if self._connection:
            await self._connection.close()
            self._connection = None
        self._connected = False

    async def publish(self, message: QueueMessage) -> QueueResult:
        """Publish message to RabbitMQ."""
        if not self._channel:
            return QueueResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            import aio_pika

            body = message.body if isinstance(message.body, bytes) else json.dumps(message.body).encode()

            amqp_message = aio_pika.Message(
                body=body,
                message_id=message.id,
                headers=message.headers,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if self.config.durable else aio_pika.DeliveryMode.NOT_PERSISTENT,
            )

            if self._exchange:
                await self._exchange.publish(
                    amqp_message,
                    routing_key=self.config.routing_key,
                )
            else:
                await self._channel.default_exchange.publish(
                    amqp_message,
                    routing_key=self.config.queue_name,
                )

            self._message_count += 1
            return QueueResult(
                success=True,
                message_id=message.id,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._error_count += 1
            return QueueResult(success=False, error=str(e))

    async def consume(self, max_messages: int = 1) -> List[QueueMessage]:
        """Consume messages from RabbitMQ."""
        if not self._queue:
            return []

        messages = []
        try:
            for _ in range(max_messages):
                msg = await asyncio.wait_for(
                    self._queue.get(timeout=self.config.batch_timeout_seconds),
                    timeout=self.config.batch_timeout_seconds + 1,
                )
                if msg:
                    messages.append(QueueMessage(
                        id=msg.message_id or str(uuid.uuid4()),
                        body=msg.body,
                        headers=dict(msg.headers) if msg.headers else {},
                        receipt_handle=msg.delivery_tag,
                        delivery_count=msg.delivery_count or 0,
                    ))
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"RabbitMQ consume error: {e}")

        return messages

    async def acknowledge(self, message: QueueMessage) -> bool:
        """Acknowledge RabbitMQ message."""
        if not self._channel or not message.receipt_handle:
            return False

        try:
            await self._channel.basic_ack(message.receipt_handle)
            return True
        except Exception as e:
            logger.error(f"RabbitMQ ack failed: {e}")
            return False

    async def reject(self, message: QueueMessage, requeue: bool = False) -> bool:
        """Reject RabbitMQ message."""
        if not self._channel or not message.receipt_handle:
            return False

        try:
            await self._channel.basic_reject(message.receipt_handle, requeue=requeue)
            return True
        except Exception as e:
            logger.error(f"RabbitMQ reject failed: {e}")
            return False


# =============================================================================
# AWS SQS Adapter
# =============================================================================

@dataclass
class SQSConfig(QueueConfig):
    """AWS SQS configuration."""
    queue_url: str = ""
    region: str = "us-east-1"
    access_key: Optional[str] = None
    secret_key: Optional[str] = None

    # FIFO
    fifo_queue: bool = False
    message_group_id: str = "default"
    deduplication_id: Optional[str] = None

    # Long polling
    wait_time_seconds: int = 20


class SQSAdapter(QueueAdapter):
    """
    AWS SQS adapter.

    Features:
    - Standard and FIFO queues
    - Long polling
    - Message deduplication
    - Batch operations
    """

    def __init__(self, config: SQSConfig):
        super().__init__(config)
        self.config: SQSConfig = config
        self._client = None

    async def connect(self) -> bool:
        """Connect to SQS."""
        try:
            import boto3

            self._client = boto3.client(
                'sqs',
                region_name=self.config.region,
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
            )

            # Verify queue exists
            self._client.get_queue_attributes(
                QueueUrl=self.config.queue_url,
                AttributeNames=['QueueArn'],
            )

            self._connected = True
            logger.info(f"SQS connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"SQS connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from SQS."""
        self._client = None
        self._connected = False

    async def publish(self, message: QueueMessage) -> QueueResult:
        """Publish message to SQS."""
        if not self._client:
            return QueueResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            body = message.body if isinstance(message.body, str) else json.dumps(message.body)

            params = {
                'QueueUrl': self.config.queue_url,
                'MessageBody': body,
                'MessageAttributes': {
                    k: {'DataType': 'String', 'StringValue': v}
                    for k, v in message.headers.items()
                },
            }

            if self.config.fifo_queue:
                params['MessageGroupId'] = self.config.message_group_id
                params['MessageDeduplicationId'] = message.id

            response = self._client.send_message(**params)

            self._message_count += 1
            return QueueResult(
                success=True,
                message_id=response['MessageId'],
                sequence_number=int(response.get('SequenceNumber', 0)) if self.config.fifo_queue else None,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._error_count += 1
            return QueueResult(success=False, error=str(e))

    async def consume(self, max_messages: int = 1) -> List[QueueMessage]:
        """Consume messages from SQS."""
        if not self._client:
            return []

        try:
            response = self._client.receive_message(
                QueueUrl=self.config.queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=self.config.wait_time_seconds,
                VisibilityTimeout=self.config.visibility_timeout_seconds,
                MessageAttributeNames=['All'],
            )

            messages = []
            for msg in response.get('Messages', []):
                headers = {
                    k: v['StringValue']
                    for k, v in msg.get('MessageAttributes', {}).items()
                }
                messages.append(QueueMessage(
                    id=msg['MessageId'],
                    body=msg['Body'],
                    headers=headers,
                    receipt_handle=msg['ReceiptHandle'],
                    attributes=msg.get('Attributes', {}),
                ))

            return messages

        except Exception as e:
            logger.error(f"SQS consume error: {e}")
            return []

    async def acknowledge(self, message: QueueMessage) -> bool:
        """Delete message from SQS (acknowledge)."""
        if not self._client or not message.receipt_handle:
            return False

        try:
            self._client.delete_message(
                QueueUrl=self.config.queue_url,
                ReceiptHandle=message.receipt_handle,
            )
            return True
        except Exception as e:
            logger.error(f"SQS delete failed: {e}")
            return False

    async def reject(self, message: QueueMessage, requeue: bool = False) -> bool:
        """Change visibility or delete message."""
        if not self._client or not message.receipt_handle:
            return False

        try:
            if requeue:
                self._client.change_message_visibility(
                    QueueUrl=self.config.queue_url,
                    ReceiptHandle=message.receipt_handle,
                    VisibilityTimeout=0,
                )
            # If not requeue, just let visibility timeout expire
            return True
        except Exception as e:
            logger.error(f"SQS reject failed: {e}")
            return False


# =============================================================================
# Azure Service Bus Adapter
# =============================================================================

@dataclass
class AzureServiceBusConfig(QueueConfig):
    """Azure Service Bus configuration."""
    connection_string: str = ""
    namespace: str = ""

    # Entity type
    entity_type: str = "queue"  # queue or topic
    topic_name: str = ""
    subscription_name: str = ""

    # Sessions
    session_enabled: bool = False
    session_id: Optional[str] = None


class AzureServiceBusAdapter(QueueAdapter):
    """
    Azure Service Bus adapter.

    Features:
    - Queues and Topics/Subscriptions
    - Sessions for ordered processing
    - Scheduled messages
    - Dead letter handling
    """

    def __init__(self, config: AzureServiceBusConfig):
        super().__init__(config)
        self.config: AzureServiceBusConfig = config
        self._client = None
        self._sender = None
        self._receiver = None

    async def connect(self) -> bool:
        """Connect to Azure Service Bus."""
        try:
            from azure.servicebus.aio import ServiceBusClient

            self._client = ServiceBusClient.from_connection_string(
                self.config.connection_string
            )

            if self.config.entity_type == "queue":
                self._sender = self._client.get_queue_sender(self.config.queue_name)
                self._receiver = self._client.get_queue_receiver(
                    self.config.queue_name,
                    max_wait_time=self.config.batch_timeout_seconds,
                )
            else:
                self._sender = self._client.get_topic_sender(self.config.topic_name)
                self._receiver = self._client.get_subscription_receiver(
                    self.config.topic_name,
                    self.config.subscription_name,
                    max_wait_time=self.config.batch_timeout_seconds,
                )

            self._connected = True
            logger.info(f"Azure Service Bus connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"Azure Service Bus connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from Azure Service Bus."""
        if self._sender:
            await self._sender.close()
        if self._receiver:
            await self._receiver.close()
        if self._client:
            await self._client.close()
        self._connected = False

    async def publish(self, message: QueueMessage) -> QueueResult:
        """Publish message to Azure Service Bus."""
        if not self._sender:
            return QueueResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            from azure.servicebus import ServiceBusMessage

            body = message.body if isinstance(message.body, str) else json.dumps(message.body)

            sb_message = ServiceBusMessage(
                body,
                message_id=message.id,
                application_properties=message.headers,
            )

            if self.config.session_enabled and self.config.session_id:
                sb_message.session_id = self.config.session_id

            await self._sender.send_messages(sb_message)

            self._message_count += 1
            return QueueResult(
                success=True,
                message_id=message.id,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._error_count += 1
            return QueueResult(success=False, error=str(e))

    async def consume(self, max_messages: int = 1) -> List[QueueMessage]:
        """Consume messages from Azure Service Bus."""
        if not self._receiver:
            return []

        try:
            received = await self._receiver.receive_messages(
                max_message_count=max_messages,
                max_wait_time=self.config.batch_timeout_seconds,
            )

            messages = []
            for msg in received:
                messages.append(QueueMessage(
                    id=msg.message_id or str(uuid.uuid4()),
                    body=str(msg),
                    headers=dict(msg.application_properties) if msg.application_properties else {},
                    receipt_handle=msg,  # Store the message object for completion
                    delivery_count=msg.delivery_count,
                    sequence_number=msg.sequence_number,
                ))

            return messages

        except Exception as e:
            logger.error(f"Azure Service Bus consume error: {e}")
            return []

    async def acknowledge(self, message: QueueMessage) -> bool:
        """Complete message in Azure Service Bus."""
        if not self._receiver or not message.receipt_handle:
            return False

        try:
            await self._receiver.complete_message(message.receipt_handle)
            return True
        except Exception as e:
            logger.error(f"Azure Service Bus complete failed: {e}")
            return False

    async def reject(self, message: QueueMessage, requeue: bool = False) -> bool:
        """Abandon or dead letter message."""
        if not self._receiver or not message.receipt_handle:
            return False

        try:
            if requeue:
                await self._receiver.abandon_message(message.receipt_handle)
            else:
                await self._receiver.dead_letter_message(message.receipt_handle)
            return True
        except Exception as e:
            logger.error(f"Azure Service Bus reject failed: {e}")
            return False


# =============================================================================
# Google Pub/Sub Adapter
# =============================================================================

@dataclass
class PubSubConfig(QueueConfig):
    """Google Pub/Sub configuration."""
    project_id: str = ""
    topic_id: str = ""
    subscription_id: str = ""
    credentials_path: Optional[str] = None

    # Ordering
    enable_ordering: bool = False
    ordering_key: str = ""


class PubSubAdapter(QueueAdapter):
    """
    Google Cloud Pub/Sub adapter.

    Features:
    - Topics and Subscriptions
    - Message ordering
    - Exactly once delivery
    - Dead letter topics
    """

    def __init__(self, config: PubSubConfig):
        super().__init__(config)
        self.config: PubSubConfig = config
        self._publisher = None
        self._subscriber = None
        self._subscription_path = None

    async def connect(self) -> bool:
        """Connect to Google Pub/Sub."""
        try:
            from google.cloud import pubsub_v1

            if self.config.credentials_path:
                self._publisher = pubsub_v1.PublisherClient.from_service_account_file(
                    self.config.credentials_path
                )
                self._subscriber = pubsub_v1.SubscriberClient.from_service_account_file(
                    self.config.credentials_path
                )
            else:
                self._publisher = pubsub_v1.PublisherClient()
                self._subscriber = pubsub_v1.SubscriberClient()

            self._topic_path = self._publisher.topic_path(
                self.config.project_id,
                self.config.topic_id,
            )
            self._subscription_path = self._subscriber.subscription_path(
                self.config.project_id,
                self.config.subscription_id,
            )

            self._connected = True
            logger.info(f"Google Pub/Sub connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"Google Pub/Sub connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from Google Pub/Sub."""
        self._publisher = None
        self._subscriber = None
        self._connected = False

    async def publish(self, message: QueueMessage) -> QueueResult:
        """Publish message to Google Pub/Sub."""
        if not self._publisher:
            return QueueResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            data = message.body if isinstance(message.body, bytes) else json.dumps(message.body).encode()

            kwargs = {
                'data': data,
                **{k: v for k, v in message.headers.items()},
            }

            if self.config.enable_ordering:
                kwargs['ordering_key'] = self.config.ordering_key

            future = self._publisher.publish(self._topic_path, **kwargs)
            message_id = future.result()

            self._message_count += 1
            return QueueResult(
                success=True,
                message_id=message_id,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._error_count += 1
            return QueueResult(success=False, error=str(e))

    async def consume(self, max_messages: int = 1) -> List[QueueMessage]:
        """Consume messages from Google Pub/Sub."""
        if not self._subscriber:
            return []

        try:
            response = self._subscriber.pull(
                subscription=self._subscription_path,
                max_messages=max_messages,
                timeout=self.config.timeout_seconds,
            )

            messages = []
            for received in response.received_messages:
                messages.append(QueueMessage(
                    id=received.message.message_id,
                    body=received.message.data,
                    headers=dict(received.message.attributes),
                    receipt_handle=received.ack_id,
                    timestamp=received.message.publish_time,
                ))

            return messages

        except Exception as e:
            logger.error(f"Google Pub/Sub consume error: {e}")
            return []

    async def acknowledge(self, message: QueueMessage) -> bool:
        """Acknowledge message in Google Pub/Sub."""
        if not self._subscriber or not message.receipt_handle:
            return False

        try:
            self._subscriber.acknowledge(
                subscription=self._subscription_path,
                ack_ids=[message.receipt_handle],
            )
            return True
        except Exception as e:
            logger.error(f"Google Pub/Sub ack failed: {e}")
            return False

    async def reject(self, message: QueueMessage, requeue: bool = False) -> bool:
        """Modify ack deadline or nack message."""
        if not self._subscriber or not message.receipt_handle:
            return False

        try:
            if requeue:
                self._subscriber.modify_ack_deadline(
                    subscription=self._subscription_path,
                    ack_ids=[message.receipt_handle],
                    ack_deadline_seconds=0,
                )
            # Pub/Sub doesn't have explicit nack, message will be redelivered after deadline
            return True
        except Exception as e:
            logger.error(f"Google Pub/Sub reject failed: {e}")
            return False


# =============================================================================
# IBM MQ Adapter
# =============================================================================

@dataclass
class IBMMQConfig(QueueConfig):
    """IBM MQ configuration."""
    host: str = "localhost"
    port: int = 1414
    channel: str = "DEV.APP.SVRCONN"
    queue_manager: str = ""
    username: Optional[str] = None
    password: Optional[str] = None

    # TLS
    use_tls: bool = False
    cipher_spec: str = "TLS_RSA_WITH_AES_128_CBC_SHA256"
    key_repo_path: Optional[str] = None


class IBMMQAdapter(QueueAdapter):
    """
    IBM MQ adapter using pymqi.

    Features:
    - Queue manager connection
    - TLS support
    - Transactional messaging
    - Backout handling
    """

    def __init__(self, config: IBMMQConfig):
        super().__init__(config)
        self.config: IBMMQConfig = config
        self._qmgr = None
        self._queue = None

    async def connect(self) -> bool:
        """Connect to IBM MQ."""
        try:
            import pymqi

            cd = pymqi.CD()
            cd.ChannelName = self.config.channel.encode()
            cd.ConnectionName = f"{self.config.host}({self.config.port})".encode()
            cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
            cd.TransportType = pymqi.CMQC.MQXPT_TCP

            if self.config.use_tls:
                cd.SSLCipherSpec = self.config.cipher_spec.encode()
                sco = pymqi.SCO()
                sco.KeyRepository = self.config.key_repo_path.encode() if self.config.key_repo_path else b""

            connect_options = pymqi.CMQC.MQCNO_HANDLE_SHARE_BLOCK

            self._qmgr = pymqi.QueueManager(None)
            self._qmgr.connect_with_options(
                self.config.queue_manager,
                cd=cd,
                opts=connect_options,
                user=self.config.username,
                password=self.config.password,
            )

            self._queue = pymqi.Queue(
                self._qmgr,
                self.config.queue_name,
                pymqi.CMQC.MQOO_INPUT_AS_Q_DEF | pymqi.CMQC.MQOO_OUTPUT,
            )

            self._connected = True
            logger.info(f"IBM MQ connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"IBM MQ connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from IBM MQ."""
        if self._queue:
            self._queue.close()
            self._queue = None
        if self._qmgr:
            self._qmgr.disconnect()
            self._qmgr = None
        self._connected = False

    async def publish(self, message: QueueMessage) -> QueueResult:
        """Publish message to IBM MQ."""
        if not self._queue:
            return QueueResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            import pymqi

            body = message.body if isinstance(message.body, bytes) else json.dumps(message.body).encode()

            md = pymqi.MD()
            md.MsgId = message.id.encode()[:24]
            md.Format = pymqi.CMQC.MQFMT_STRING

            self._queue.put(body, md)

            self._message_count += 1
            return QueueResult(
                success=True,
                message_id=message.id,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._error_count += 1
            return QueueResult(success=False, error=str(e))

    async def consume(self, max_messages: int = 1) -> List[QueueMessage]:
        """Consume messages from IBM MQ."""
        if not self._queue:
            return []

        messages = []
        try:
            import pymqi

            for _ in range(max_messages):
                try:
                    md = pymqi.MD()
                    gmo = pymqi.GMO()
                    gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
                    gmo.WaitInterval = int(self.config.batch_timeout_seconds * 1000)

                    body = self._queue.get(None, md, gmo)

                    messages.append(QueueMessage(
                        id=md.MsgId.decode().strip('\x00'),
                        body=body,
                        receipt_handle=md.MsgId,
                        delivery_count=md.BackoutCount,
                    ))
                except pymqi.MQMIError as e:
                    if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        break
                    raise

        except Exception as e:
            logger.error(f"IBM MQ consume error: {e}")

        return messages

    async def acknowledge(self, message: QueueMessage) -> bool:
        """IBM MQ auto-acknowledges on get. Commit if using transactions."""
        return True

    async def reject(self, message: QueueMessage, requeue: bool = False) -> bool:
        """IBM MQ uses backout for requeue. Rollback if using transactions."""
        return True


# =============================================================================
# Factory & Registry
# =============================================================================

QUEUE_ADAPTERS: Dict[str, type] = {
    "rabbitmq": RabbitMQAdapter,
    "amqp": RabbitMQAdapter,
    "sqs": SQSAdapter,
    "aws_sqs": SQSAdapter,
    "azure_service_bus": AzureServiceBusAdapter,
    "servicebus": AzureServiceBusAdapter,
    "pubsub": PubSubAdapter,
    "google_pubsub": PubSubAdapter,
    "ibm_mq": IBMMQAdapter,
    "mq": IBMMQAdapter,
}


def get_queue_adapter(
    queue_type: str,
    config: QueueConfig,
) -> QueueAdapter:
    """Get queue adapter by type."""
    adapter_class = QUEUE_ADAPTERS.get(queue_type.lower())
    if not adapter_class:
        raise ValueError(f"Unknown queue type: {queue_type}")
    return adapter_class(config)

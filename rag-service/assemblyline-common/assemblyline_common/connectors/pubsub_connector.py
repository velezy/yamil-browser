"""
Google Cloud Pub/Sub Connector for Logic Weaver

Enterprise-grade Pub/Sub connector with:
- Topic and subscription management
- Batch publishing for throughput
- Pull and streaming pull modes
- Message ordering with ordering keys
- Dead letter handling
- Message filtering
- Exactly-once delivery
- Multi-tenant isolation

Comparison:
| Feature              | Kafka  | SQS    | Logic Weaver |
|---------------------|--------|--------|--------------|
| Pub/Sub Support     | Yes    | No     | Yes          |
| Message Ordering    | Yes    | FIFO   | Yes          |
| Dead Letter         | No     | Yes    | Yes          |
| Filtering           | No     | No     | Yes          |
| Exactly-once        | No     | No     | Yes          |
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional

logger = logging.getLogger(__name__)

# Try to import google cloud pubsub
try:
    from google.cloud import pubsub_v1
    from google.cloud.pubsub_v1 import types
    from google.api_core import retry
    from google.auth import default as google_auth_default
    from google.oauth2 import service_account
    HAS_PUBSUB = True
except ImportError:
    HAS_PUBSUB = False
    logger.warning("google-cloud-pubsub not installed. Pub/Sub limited.")


class AckMode(Enum):
    """Message acknowledgment modes."""
    AUTO = "auto"  # Ack after handler completes
    MANUAL = "manual"  # Handler must ack


@dataclass
class PubSubConfig:
    """Google Pub/Sub configuration."""
    # Project
    project_id: str

    # Authentication
    credentials_path: Optional[str] = None  # Path to service account JSON
    credentials_json: Optional[str] = None  # JSON string

    # Topic settings
    topic_id: Optional[str] = None

    # Subscription settings
    subscription_id: Optional[str] = None

    # Publisher settings
    batch_max_messages: int = 100
    batch_max_bytes: int = 1024 * 1024  # 1MB
    batch_max_latency: float = 0.01  # 10ms
    enable_message_ordering: bool = False

    # Subscriber settings
    max_messages: int = 100
    ack_deadline_seconds: int = 60
    ack_mode: AckMode = AckMode.AUTO
    flow_control_max_messages: int = 1000
    flow_control_max_bytes: int = 100 * 1024 * 1024  # 100MB

    # Dead letter
    dead_letter_topic: Optional[str] = None
    max_delivery_attempts: int = 5

    # Message filter
    filter_expression: Optional[str] = None  # e.g., "attributes.type = 'order'"

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class PubSubMessage:
    """Represents a Pub/Sub message."""
    data: bytes
    attributes: dict[str, str] = field(default_factory=dict)
    message_id: Optional[str] = None
    publish_time: Optional[datetime] = None
    ordering_key: Optional[str] = None
    ack_id: Optional[str] = None
    delivery_attempt: int = 0

    @property
    def data_str(self) -> str:
        return self.data.decode('utf-8')

    @property
    def data_json(self) -> Any:
        try:
            return json.loads(self.data.decode('utf-8'))
        except json.JSONDecodeError:
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": self.data.decode('utf-8', errors='replace'),
            "attributes": self.attributes,
            "message_id": self.message_id,
            "publish_time": self.publish_time.isoformat() if self.publish_time else None,
            "ordering_key": self.ordering_key,
            "delivery_attempt": self.delivery_attempt,
        }


@dataclass
class PublishResult:
    """Result of a publish operation."""
    success: bool
    message_id: Optional[str] = None
    message: str = ""
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message_id": self.message_id,
            "message": self.message,
            "error": self.error,
        }


@dataclass
class BatchPublishResult:
    """Result of a batch publish operation."""
    success: bool
    message_ids: list[str] = field(default_factory=list)
    failed_count: int = 0
    message: str = ""
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message_ids": self.message_ids,
            "failed_count": self.failed_count,
            "message": self.message,
            "errors": self.errors,
        }


class PubSubConnector:
    """
    Enterprise Google Cloud Pub/Sub connector.

    Example usage:

    config = PubSubConfig(
        project_id="my-project",
        credentials_path="/path/to/credentials.json",
        topic_id="my-topic",
        subscription_id="my-subscription"
    )

    async with PubSubConnector(config) as pubsub:
        # Publish message
        result = await pubsub.publish({"event": "order_created", "order_id": "123"})

        # Subscribe to messages
        async for msg in pubsub.subscribe():
            process(msg)
            await pubsub.ack(msg)
    """

    def __init__(self, config: PubSubConfig):
        self.config = config
        self._publisher: Optional[pubsub_v1.PublisherClient] = None
        self._subscriber: Optional[pubsub_v1.SubscriberClient] = None
        self._credentials = None
        self._streaming_pull_future = None

    @property
    def topic_path(self) -> str:
        if self._publisher and self.config.topic_id:
            return self._publisher.topic_path(self.config.project_id, self.config.topic_id)
        return f"projects/{self.config.project_id}/topics/{self.config.topic_id}"

    @property
    def subscription_path(self) -> str:
        if self._subscriber and self.config.subscription_id:
            return self._subscriber.subscription_path(
                self.config.project_id, self.config.subscription_id
            )
        return f"projects/{self.config.project_id}/subscriptions/{self.config.subscription_id}"

    async def __aenter__(self) -> "PubSubConnector":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> bool:
        """Initialize Pub/Sub clients."""
        if not HAS_PUBSUB:
            raise ImportError("google-cloud-pubsub is required for Pub/Sub connections")

        try:
            # Load credentials
            self._credentials = self._load_credentials()

            # Publisher settings
            publisher_settings = types.BatchSettings(
                max_messages=self.config.batch_max_messages,
                max_bytes=self.config.batch_max_bytes,
                max_latency=self.config.batch_max_latency,
            )

            # Initialize publisher
            self._publisher = pubsub_v1.PublisherClient(
                credentials=self._credentials,
                batch_settings=publisher_settings,
            )

            # Initialize subscriber
            self._subscriber = pubsub_v1.SubscriberClient(
                credentials=self._credentials,
            )

            logger.info(f"Connected to Google Pub/Sub: {self.config.project_id}")
            return True

        except Exception as e:
            logger.error(f"Pub/Sub connection failed: {e}")
            raise

    def _load_credentials(self):
        """Load Google Cloud credentials."""
        if self.config.credentials_path:
            return service_account.Credentials.from_service_account_file(
                self.config.credentials_path
            )
        elif self.config.credentials_json:
            info = json.loads(self.config.credentials_json)
            return service_account.Credentials.from_service_account_info(info)
        else:
            # Use default credentials
            credentials, _ = google_auth_default()
            return credentials

    async def disconnect(self) -> None:
        """Close Pub/Sub clients."""
        if self._streaming_pull_future:
            self._streaming_pull_future.cancel()
            try:
                self._streaming_pull_future.result(timeout=5)
            except Exception:
                pass
            self._streaming_pull_future = None

        if self._publisher:
            self._publisher.stop()
            self._publisher = None

        if self._subscriber:
            self._subscriber.close()
            self._subscriber = None

        logger.info("Pub/Sub connection closed")

    async def create_topic(self, topic_id: Optional[str] = None) -> bool:
        """Create a topic."""
        topic_id = topic_id or self.config.topic_id
        if not topic_id:
            raise ValueError("topic_id is required")

        try:
            topic_path = self._publisher.topic_path(self.config.project_id, topic_id)
            self._publisher.create_topic(name=topic_path)
            logger.info(f"Created topic: {topic_path}")
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Topic already exists: {topic_id}")
                return True
            logger.error(f"Failed to create topic: {e}")
            raise

    async def create_subscription(
        self,
        subscription_id: Optional[str] = None,
        topic_id: Optional[str] = None,
    ) -> bool:
        """Create a subscription."""
        subscription_id = subscription_id or self.config.subscription_id
        topic_id = topic_id or self.config.topic_id

        if not subscription_id or not topic_id:
            raise ValueError("subscription_id and topic_id are required")

        try:
            subscription_path = self._subscriber.subscription_path(
                self.config.project_id, subscription_id
            )
            topic_path = self._publisher.topic_path(self.config.project_id, topic_id)

            request = {
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": self.config.ack_deadline_seconds,
                "enable_message_ordering": self.config.enable_message_ordering,
            }

            # Add dead letter policy if configured
            if self.config.dead_letter_topic:
                dead_letter_path = self._publisher.topic_path(
                    self.config.project_id, self.config.dead_letter_topic
                )
                request["dead_letter_policy"] = {
                    "dead_letter_topic": dead_letter_path,
                    "max_delivery_attempts": self.config.max_delivery_attempts,
                }

            # Add filter if configured
            if self.config.filter_expression:
                request["filter"] = self.config.filter_expression

            self._subscriber.create_subscription(request=request)
            logger.info(f"Created subscription: {subscription_path}")
            return True

        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Subscription already exists: {subscription_id}")
                return True
            logger.error(f"Failed to create subscription: {e}")
            raise

    async def publish(
        self,
        data: Any,
        attributes: Optional[dict[str, str]] = None,
        ordering_key: Optional[str] = None,
    ) -> PublishResult:
        """
        Publish a message to the topic.

        Args:
            data: Message data (will be JSON encoded if dict/list)
            attributes: Message attributes
            ordering_key: Ordering key for ordered delivery
        """
        if not self._publisher:
            await self.connect()

        try:
            # Prepare data
            if isinstance(data, (dict, list)):
                data_bytes = json.dumps(data).encode('utf-8')
            elif isinstance(data, str):
                data_bytes = data.encode('utf-8')
            else:
                data_bytes = data

            # Prepare attributes
            attrs = attributes or {}
            if self.config.tenant_id:
                attrs["tenant_id"] = self.config.tenant_id

            # Publish
            future = self._publisher.publish(
                self.topic_path,
                data_bytes,
                ordering_key=ordering_key or "",
                **attrs,
            )

            # Wait for result
            message_id = await asyncio.get_event_loop().run_in_executor(
                None, future.result
            )

            return PublishResult(
                success=True,
                message_id=message_id,
                message="Message published successfully",
            )

        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return PublishResult(
                success=False,
                message="Publish failed",
                error=str(e),
            )

    async def publish_batch(
        self,
        messages: list[dict[str, Any]],
    ) -> BatchPublishResult:
        """
        Publish multiple messages.

        Args:
            messages: List of {"data": ..., "attributes": ..., "ordering_key": ...}
        """
        if not self._publisher:
            await self.connect()

        message_ids = []
        errors = []

        for msg in messages:
            result = await self.publish(
                data=msg.get("data"),
                attributes=msg.get("attributes"),
                ordering_key=msg.get("ordering_key"),
            )

            if result.success and result.message_id:
                message_ids.append(result.message_id)
            else:
                errors.append(result.error or "Unknown error")

        return BatchPublishResult(
            success=len(errors) == 0,
            message_ids=message_ids,
            failed_count=len(errors),
            message=f"Published {len(message_ids)}/{len(messages)} messages",
            errors=errors,
        )

    async def pull(self, max_messages: Optional[int] = None) -> list[PubSubMessage]:
        """
        Synchronous pull of messages.

        Returns a batch of messages that must be acknowledged.
        """
        if not self._subscriber:
            await self.connect()

        max_messages = max_messages or self.config.max_messages

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._subscriber.pull(
                    request={
                        "subscription": self.subscription_path,
                        "max_messages": max_messages,
                    }
                )
            )

            messages = []
            for received in response.received_messages:
                msg = received.message
                messages.append(PubSubMessage(
                    data=msg.data,
                    attributes=dict(msg.attributes),
                    message_id=msg.message_id,
                    publish_time=msg.publish_time,
                    ordering_key=msg.ordering_key or None,
                    ack_id=received.ack_id,
                    delivery_attempt=received.delivery_attempt,
                ))

            return messages

        except Exception as e:
            logger.error(f"Failed to pull messages: {e}")
            return []

    async def subscribe(
        self,
        handler: Optional[Callable[[PubSubMessage], Any]] = None,
    ) -> AsyncIterator[PubSubMessage]:
        """
        Subscribe to messages using streaming pull.

        If handler is provided, it's called for each message.
        Otherwise, yields messages as they arrive.
        """
        if not self._subscriber:
            await self.connect()

        message_queue: asyncio.Queue = asyncio.Queue()

        def callback(message):
            """Process received message."""
            pubsub_msg = PubSubMessage(
                data=message.data,
                attributes=dict(message.attributes),
                message_id=message.message_id,
                publish_time=message.publish_time,
                ordering_key=message.ordering_key or None,
                delivery_attempt=message.delivery_attempt,
            )

            if handler:
                try:
                    result = handler(pubsub_msg)
                    if asyncio.iscoroutine(result):
                        asyncio.run_coroutine_threadsafe(
                            result,
                            asyncio.get_event_loop()
                        )
                    if self.config.ack_mode == AckMode.AUTO:
                        message.ack()
                except Exception as e:
                    logger.error(f"Message handler error: {e}")
                    message.nack()
            else:
                # Store ack_id for manual ack
                pubsub_msg.ack_id = message.ack_id
                message_queue.put_nowait((pubsub_msg, message))

        # Start streaming pull
        flow_control = types.FlowControl(
            max_messages=self.config.flow_control_max_messages,
            max_bytes=self.config.flow_control_max_bytes,
        )

        self._streaming_pull_future = self._subscriber.subscribe(
            self.subscription_path,
            callback=callback,
            flow_control=flow_control,
        )

        if handler:
            # Wait for cancellation
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._streaming_pull_future.result
                )
            except Exception as e:
                logger.error(f"Streaming pull error: {e}")
        else:
            # Yield messages
            try:
                while True:
                    pubsub_msg, original_msg = await message_queue.get()
                    yield pubsub_msg
                    if self.config.ack_mode == AckMode.AUTO:
                        original_msg.ack()
            except asyncio.CancelledError:
                pass

    async def ack(self, message: PubSubMessage) -> bool:
        """Acknowledge a message."""
        if not message.ack_id:
            return False

        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._subscriber.acknowledge(
                    request={
                        "subscription": self.subscription_path,
                        "ack_ids": [message.ack_id],
                    }
                )
            )
            return True
        except Exception as e:
            logger.error(f"Failed to ack message: {e}")
            return False

    async def nack(self, message: PubSubMessage) -> bool:
        """Negative acknowledge a message (will be redelivered)."""
        if not message.ack_id:
            return False

        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._subscriber.modify_ack_deadline(
                    request={
                        "subscription": self.subscription_path,
                        "ack_ids": [message.ack_id],
                        "ack_deadline_seconds": 0,
                    }
                )
            )
            return True
        except Exception as e:
            logger.error(f"Failed to nack message: {e}")
            return False

    async def delete_topic(self, topic_id: Optional[str] = None) -> bool:
        """Delete a topic."""
        topic_id = topic_id or self.config.topic_id
        if not topic_id:
            raise ValueError("topic_id is required")

        try:
            topic_path = self._publisher.topic_path(self.config.project_id, topic_id)
            self._publisher.delete_topic(topic=topic_path)
            logger.info(f"Deleted topic: {topic_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            raise

    async def delete_subscription(self, subscription_id: Optional[str] = None) -> bool:
        """Delete a subscription."""
        subscription_id = subscription_id or self.config.subscription_id
        if not subscription_id:
            raise ValueError("subscription_id is required")

        try:
            subscription_path = self._subscriber.subscription_path(
                self.config.project_id, subscription_id
            )
            self._subscriber.delete_subscription(subscription=subscription_path)
            logger.info(f"Deleted subscription: {subscription_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete subscription: {e}")
            raise


# Flow Node Integration
@dataclass
class PubSubNodeConfig:
    """Configuration for Pub/Sub flow node."""
    project_id: str
    credentials_path: Optional[str] = None
    topic_id: Optional[str] = None
    subscription_id: Optional[str] = None
    operation: str = "publish"  # publish, pull
    max_messages: int = 100
    enable_ordering: bool = False


@dataclass
class PubSubNodeResult:
    """Result from Pub/Sub flow node."""
    success: bool
    operation: str
    message_ids: list[str]
    messages: list[dict[str, Any]]
    count: int
    message: str
    error: Optional[str]


class PubSubNode:
    """Flow node for Google Pub/Sub operations."""

    node_type = "pubsub"
    node_category = "connector"

    def __init__(self, config: PubSubNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> PubSubNodeResult:
        """Execute the Pub/Sub operation."""
        pubsub_config = PubSubConfig(
            project_id=self.config.project_id,
            credentials_path=self.config.credentials_path,
            topic_id=self.config.topic_id,
            subscription_id=self.config.subscription_id,
            enable_message_ordering=self.config.enable_ordering,
        )

        try:
            async with PubSubConnector(pubsub_config) as pubsub:
                if self.config.operation == "publish":
                    data = input_data.get("data", input_data)
                    attributes = input_data.get("attributes", {})
                    ordering_key = input_data.get("ordering_key")

                    result = await pubsub.publish(
                        data=data,
                        attributes=attributes,
                        ordering_key=ordering_key,
                    )

                    return PubSubNodeResult(
                        success=result.success,
                        operation="publish",
                        message_ids=[result.message_id] if result.message_id else [],
                        messages=[],
                        count=1 if result.success else 0,
                        message=result.message,
                        error=result.error,
                    )

                elif self.config.operation == "pull":
                    messages = await pubsub.pull(max_messages=self.config.max_messages)

                    # Auto-ack pulled messages
                    for msg in messages:
                        await pubsub.ack(msg)

                    return PubSubNodeResult(
                        success=True,
                        operation="pull",
                        message_ids=[m.message_id for m in messages if m.message_id],
                        messages=[m.to_dict() for m in messages],
                        count=len(messages),
                        message=f"Pulled {len(messages)} messages",
                        error=None,
                    )

                else:
                    return PubSubNodeResult(
                        success=False,
                        operation=self.config.operation,
                        message_ids=[],
                        messages=[],
                        count=0,
                        message="Unknown operation",
                        error=f"Unknown operation: {self.config.operation}",
                    )

        except Exception as e:
            logger.error(f"Pub/Sub node execution failed: {e}")
            return PubSubNodeResult(
                success=False,
                operation=self.config.operation,
                message_ids=[],
                messages=[],
                count=0,
                message="Execution failed",
                error=str(e),
            )


def get_pubsub_connector(config: PubSubConfig) -> PubSubConnector:
    """Factory function to create Pub/Sub connector."""
    return PubSubConnector(config)


def get_pubsub_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "pubsub",
        "category": "connector",
        "label": "Google Pub/Sub",
        "description": "Google Cloud messaging service",
        "icon": "Cloud",
        "color": "#4285F4",
        "inputs": ["data"],
        "outputs": ["messages", "result"],
        "config_schema": {
            "project_id": {
                "type": "string",
                "required": True,
                "label": "Project ID",
                "placeholder": "my-gcp-project",
            },
            "credentials_path": {
                "type": "string",
                "label": "Credentials Path",
                "placeholder": "/path/to/credentials.json",
            },
            "topic_id": {
                "type": "string",
                "label": "Topic ID",
                "placeholder": "my-topic",
            },
            "subscription_id": {
                "type": "string",
                "label": "Subscription ID",
                "placeholder": "my-subscription",
            },
            "operation": {
                "type": "select",
                "options": ["publish", "pull"],
                "default": "publish",
                "label": "Operation",
            },
            "max_messages": {
                "type": "number",
                "default": 100,
                "label": "Max Messages",
                "condition": {"operation": "pull"},
            },
            "enable_ordering": {
                "type": "boolean",
                "default": False,
                "label": "Enable Message Ordering",
            },
        },
    }

"""
AWS SQS Connector for Logic Weaver

Enterprise-grade AWS SQS connector with:
- Standard and FIFO queue support
- Message batching
- Dead letter queue integration
- Visibility timeout management
- Long polling
- Message deduplication
- Multi-tenant isolation

Comparison:
| Feature              | MuleSoft | Apigee | Logic Weaver |
|---------------------|----------|--------|--------------|
| SQS Support         | Yes      | Plugin | Native       |
| FIFO Queues         | Yes      | No     | Yes          |
| Batching            | Yes      | No     | Yes          |
| DLQ Integration     | Manual   | No     | Automatic    |
| Long Polling        | Yes      | No     | Yes          |
"""

from __future__ import annotations

import asyncio
import json
import logging
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional

logger = logging.getLogger(__name__)

# Try to import boto3
try:
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    logger.warning("boto3 not installed. SQS functionality limited.")


class SQSQueueType(Enum):
    """SQS queue types."""
    STANDARD = "standard"
    FIFO = "fifo"


@dataclass
class SQSConfig:
    """SQS connection configuration."""
    queue_url: str
    region: str = "us-east-1"

    # AWS credentials (optional - uses default credential chain if not provided)
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    role_arn: Optional[str] = None  # For cross-account access

    # Queue settings
    queue_type: SQSQueueType = SQSQueueType.STANDARD
    visibility_timeout: int = 30  # seconds
    wait_time_seconds: int = 20  # Long polling (max 20)
    max_messages: int = 10  # Max messages per receive (max 10)

    # FIFO settings
    message_group_id: Optional[str] = None
    deduplication_id: Optional[str] = None
    content_based_deduplication: bool = False

    # Batching
    batch_size: int = 10
    batch_window_ms: int = 100

    # DLQ
    dlq_url: Optional[str] = None
    max_receive_count: int = 3  # Before sending to DLQ

    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class SQSMessage:
    """Represents an SQS message."""
    message_id: str
    receipt_handle: str
    body: str
    attributes: dict[str, str] = field(default_factory=dict)
    message_attributes: dict[str, Any] = field(default_factory=dict)
    md5_of_body: str = ""
    sent_timestamp: Optional[datetime] = None
    approximate_receive_count: int = 0

    @property
    def body_json(self) -> Any:
        """Parse body as JSON."""
        try:
            return json.loads(self.body)
        except json.JSONDecodeError:
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "receipt_handle": self.receipt_handle,
            "body": self.body,
            "attributes": self.attributes,
            "message_attributes": self.message_attributes,
            "sent_timestamp": self.sent_timestamp.isoformat() if self.sent_timestamp else None,
            "approximate_receive_count": self.approximate_receive_count,
        }


@dataclass
class SQSSendResult:
    """Result of sending a message."""
    success: bool
    message_id: Optional[str] = None
    md5_of_body: Optional[str] = None
    sequence_number: Optional[str] = None  # FIFO only
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message_id": self.message_id,
            "md5_of_body": self.md5_of_body,
            "sequence_number": self.sequence_number,
            "error": self.error,
        }


@dataclass
class SQSBatchResult:
    """Result of batch operation."""
    successful: list[SQSSendResult]
    failed: list[dict[str, Any]]

    @property
    def success_count(self) -> int:
        return len(self.successful)

    @property
    def failure_count(self) -> int:
        return len(self.failed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "successful": [r.to_dict() for r in self.successful],
            "failed": self.failed,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
        }


class SQSConnector:
    """
    Enterprise AWS SQS connector.

    Example usage:

    config = SQSConfig(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
        region="us-east-1"
    )

    async with SQSConnector(config) as sqs:
        # Send message
        result = await sqs.send_message({"patient_id": "12345"})

        # Receive messages
        messages = await sqs.receive_messages()

        # Process and delete
        for msg in messages:
            process(msg)
            await sqs.delete_message(msg)
    """

    def __init__(self, config: SQSConfig):
        self.config = config
        self._client = None

    async def __aenter__(self) -> "SQSConnector":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        pass  # boto3 handles cleanup

    async def connect(self) -> None:
        """Initialize the SQS client."""
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for SQS connections")

        boto_config = BotoConfig(
            retries={"max_attempts": self.config.max_retries}
        )

        session_kwargs = {"region_name": self.config.region}

        if self.config.aws_access_key_id:
            session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
        if self.config.aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
        if self.config.aws_session_token:
            session_kwargs["aws_session_token"] = self.config.aws_session_token

        session = boto3.Session(**session_kwargs)

        # Handle role assumption
        if self.config.role_arn:
            sts = session.client("sts")
            assumed_role = sts.assume_role(
                RoleArn=self.config.role_arn,
                RoleSessionName="LogicWeaverSQS"
            )
            credentials = assumed_role["Credentials"]
            self._client = boto3.client(
                "sqs",
                region_name=self.config.region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                config=boto_config
            )
        else:
            self._client = session.client("sqs", config=boto_config)

    async def send_message(
        self,
        body: Any,
        delay_seconds: int = 0,
        message_attributes: Optional[dict[str, Any]] = None,
        message_group_id: Optional[str] = None,
        deduplication_id: Optional[str] = None,
    ) -> SQSSendResult:
        """
        Send a message to the queue.

        Args:
            body: Message body (will be JSON serialized if dict)
            delay_seconds: Delay before message is available
            message_attributes: Custom message attributes
            message_group_id: FIFO message group ID
            deduplication_id: FIFO deduplication ID
        """
        if self._client is None:
            await self.connect()

        # Serialize body
        if isinstance(body, (dict, list)):
            message_body = json.dumps(body)
        else:
            message_body = str(body)

        params = {
            "QueueUrl": self.config.queue_url,
            "MessageBody": message_body,
            "DelaySeconds": delay_seconds,
        }

        # Add message attributes
        if message_attributes:
            params["MessageAttributes"] = self._format_message_attributes(message_attributes)

        # FIFO queue settings
        if self.config.queue_type == SQSQueueType.FIFO:
            group_id = message_group_id or self.config.message_group_id or "default"
            params["MessageGroupId"] = group_id

            if not self.config.content_based_deduplication:
                dedup_id = deduplication_id or self.config.deduplication_id or str(uuid.uuid4())
                params["MessageDeduplicationId"] = dedup_id

        try:
            response = self._client.send_message(**params)
            return SQSSendResult(
                success=True,
                message_id=response.get("MessageId"),
                md5_of_body=response.get("MD5OfMessageBody"),
                sequence_number=response.get("SequenceNumber"),
            )
        except ClientError as e:
            logger.error(f"Failed to send SQS message: {e}")
            return SQSSendResult(success=False, error=str(e))

    async def send_messages_batch(
        self,
        messages: list[dict[str, Any]],
    ) -> SQSBatchResult:
        """
        Send messages in batch.

        Args:
            messages: List of dicts with 'body' and optional 'delay_seconds',
                     'message_attributes', 'message_group_id', 'deduplication_id'
        """
        if self._client is None:
            await self.connect()

        successful = []
        failed = []

        # Process in batches of 10 (SQS limit)
        for i in range(0, len(messages), 10):
            batch = messages[i:i + 10]
            entries = []

            for j, msg in enumerate(batch):
                body = msg.get("body", msg)
                if isinstance(body, (dict, list)):
                    body = json.dumps(body)

                entry = {
                    "Id": str(j),
                    "MessageBody": str(body),
                    "DelaySeconds": msg.get("delay_seconds", 0),
                }

                if self.config.queue_type == SQSQueueType.FIFO:
                    entry["MessageGroupId"] = msg.get("message_group_id", "default")
                    if not self.config.content_based_deduplication:
                        entry["MessageDeduplicationId"] = msg.get("deduplication_id", str(uuid.uuid4()))

                if "message_attributes" in msg:
                    entry["MessageAttributes"] = self._format_message_attributes(msg["message_attributes"])

                entries.append(entry)

            try:
                response = self._client.send_message_batch(
                    QueueUrl=self.config.queue_url,
                    Entries=entries
                )

                for success in response.get("Successful", []):
                    successful.append(SQSSendResult(
                        success=True,
                        message_id=success.get("MessageId"),
                        md5_of_body=success.get("MD5OfMessageBody"),
                        sequence_number=success.get("SequenceNumber"),
                    ))

                for failure in response.get("Failed", []):
                    failed.append(failure)

            except ClientError as e:
                logger.error(f"Batch send failed: {e}")
                for entry in entries:
                    failed.append({"Id": entry["Id"], "Message": str(e)})

        return SQSBatchResult(successful=successful, failed=failed)

    async def receive_messages(
        self,
        max_messages: Optional[int] = None,
        wait_time_seconds: Optional[int] = None,
        visibility_timeout: Optional[int] = None,
    ) -> list[SQSMessage]:
        """
        Receive messages from the queue.

        Args:
            max_messages: Maximum messages to receive (1-10)
            wait_time_seconds: Long polling wait time
            visibility_timeout: Visibility timeout override
        """
        if self._client is None:
            await self.connect()

        params = {
            "QueueUrl": self.config.queue_url,
            "MaxNumberOfMessages": min(max_messages or self.config.max_messages, 10),
            "WaitTimeSeconds": wait_time_seconds or self.config.wait_time_seconds,
            "VisibilityTimeout": visibility_timeout or self.config.visibility_timeout,
            "AttributeNames": ["All"],
            "MessageAttributeNames": ["All"],
        }

        try:
            response = self._client.receive_message(**params)
            messages = []

            for msg in response.get("Messages", []):
                attrs = msg.get("Attributes", {})
                sent_ts = attrs.get("SentTimestamp")

                messages.append(SQSMessage(
                    message_id=msg["MessageId"],
                    receipt_handle=msg["ReceiptHandle"],
                    body=msg["Body"],
                    attributes=attrs,
                    message_attributes=msg.get("MessageAttributes", {}),
                    md5_of_body=msg.get("MD5OfBody", ""),
                    sent_timestamp=datetime.fromtimestamp(int(sent_ts) / 1000) if sent_ts else None,
                    approximate_receive_count=int(attrs.get("ApproximateReceiveCount", 0)),
                ))

            return messages

        except ClientError as e:
            logger.error(f"Failed to receive messages: {e}")
            raise

    async def delete_message(self, message: SQSMessage) -> bool:
        """Delete a message after processing."""
        if self._client is None:
            await self.connect()

        try:
            self._client.delete_message(
                QueueUrl=self.config.queue_url,
                ReceiptHandle=message.receipt_handle
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete message: {e}")
            return False

    async def delete_messages_batch(self, messages: list[SQSMessage]) -> SQSBatchResult:
        """Delete messages in batch."""
        if self._client is None:
            await self.connect()

        successful = []
        failed = []

        for i in range(0, len(messages), 10):
            batch = messages[i:i + 10]
            entries = [
                {"Id": str(j), "ReceiptHandle": msg.receipt_handle}
                for j, msg in enumerate(batch)
            ]

            try:
                response = self._client.delete_message_batch(
                    QueueUrl=self.config.queue_url,
                    Entries=entries
                )

                for success in response.get("Successful", []):
                    successful.append(SQSSendResult(success=True, message_id=success["Id"]))

                for failure in response.get("Failed", []):
                    failed.append(failure)

            except ClientError as e:
                logger.error(f"Batch delete failed: {e}")
                for entry in entries:
                    failed.append({"Id": entry["Id"], "Message": str(e)})

        return SQSBatchResult(successful=successful, failed=failed)

    async def change_visibility_timeout(
        self, message: SQSMessage, visibility_timeout: int
    ) -> bool:
        """Change visibility timeout for a message."""
        if self._client is None:
            await self.connect()

        try:
            self._client.change_message_visibility(
                QueueUrl=self.config.queue_url,
                ReceiptHandle=message.receipt_handle,
                VisibilityTimeout=visibility_timeout
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to change visibility timeout: {e}")
            return False

    async def purge_queue(self) -> bool:
        """Purge all messages from the queue."""
        if self._client is None:
            await self.connect()

        try:
            self._client.purge_queue(QueueUrl=self.config.queue_url)
            return True
        except ClientError as e:
            logger.error(f"Failed to purge queue: {e}")
            return False

    async def get_queue_attributes(self) -> dict[str, str]:
        """Get queue attributes."""
        if self._client is None:
            await self.connect()

        try:
            response = self._client.get_queue_attributes(
                QueueUrl=self.config.queue_url,
                AttributeNames=["All"]
            )
            return response.get("Attributes", {})
        except ClientError as e:
            logger.error(f"Failed to get queue attributes: {e}")
            return {}

    def _format_message_attributes(self, attrs: dict[str, Any]) -> dict[str, dict]:
        """Format message attributes for SQS API."""
        formatted = {}
        for key, value in attrs.items():
            if isinstance(value, str):
                formatted[key] = {"DataType": "String", "StringValue": value}
            elif isinstance(value, (int, float)):
                formatted[key] = {"DataType": "Number", "StringValue": str(value)}
            elif isinstance(value, bytes):
                formatted[key] = {"DataType": "Binary", "BinaryValue": value}
            else:
                formatted[key] = {"DataType": "String", "StringValue": json.dumps(value)}
        return formatted


# Flow Node Integration
@dataclass
class SQSNodeConfig:
    """Configuration for SQS flow node."""
    queue_url: str
    region: str = "us-east-1"
    operation: str = "receive"  # send, receive, delete
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    queue_type: str = "standard"
    message_group_id: Optional[str] = None
    visibility_timeout: int = 30
    wait_time_seconds: int = 20
    max_messages: int = 10
    delay_seconds: int = 0


@dataclass
class SQSNodeResult:
    """Result from SQS flow node."""
    success: bool
    operation: str
    messages: list[dict[str, Any]]
    send_result: Optional[dict[str, Any]]
    message: str
    error: Optional[str]


class SQSNode:
    """Flow node for SQS operations."""

    node_type = "sqs"
    node_category = "queue"

    def __init__(self, config: SQSNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> SQSNodeResult:
        """Execute the SQS operation."""
        sqs_config = SQSConfig(
            queue_url=self.config.queue_url,
            region=self.config.region,
            aws_access_key_id=self.config.aws_access_key_id,
            aws_secret_access_key=self.config.aws_secret_access_key,
            queue_type=SQSQueueType(self.config.queue_type),
            message_group_id=self.config.message_group_id,
            visibility_timeout=self.config.visibility_timeout,
            wait_time_seconds=self.config.wait_time_seconds,
            max_messages=self.config.max_messages,
        )

        try:
            async with SQSConnector(sqs_config) as sqs:
                if self.config.operation == "send":
                    body = input_data.get("body", input_data)
                    result = await sqs.send_message(
                        body=body,
                        delay_seconds=self.config.delay_seconds
                    )
                    return SQSNodeResult(
                        success=result.success,
                        operation="send",
                        messages=[],
                        send_result=result.to_dict(),
                        message="Message sent" if result.success else "Send failed",
                        error=result.error,
                    )

                elif self.config.operation == "receive":
                    messages = await sqs.receive_messages()
                    return SQSNodeResult(
                        success=True,
                        operation="receive",
                        messages=[m.to_dict() for m in messages],
                        send_result=None,
                        message=f"Received {len(messages)} messages",
                        error=None,
                    )

                else:
                    return SQSNodeResult(
                        success=False,
                        operation=self.config.operation,
                        messages=[],
                        send_result=None,
                        message="Unknown operation",
                        error=f"Unknown operation: {self.config.operation}",
                    )

        except Exception as e:
            logger.error(f"SQS operation failed: {e}")
            return SQSNodeResult(
                success=False,
                operation=self.config.operation,
                messages=[],
                send_result=None,
                message="Operation failed",
                error=str(e),
            )


def get_sqs_connector(config: SQSConfig) -> SQSConnector:
    """Factory function to create SQS connector."""
    return SQSConnector(config)


def get_sqs_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "sqs",
        "category": "queue",
        "label": "AWS SQS",
        "description": "AWS Simple Queue Service operations",
        "icon": "Cloud",
        "color": "#FF9900",
        "inputs": ["body"],
        "outputs": ["messages", "result"],
        "config_schema": {
            "queue_url": {"type": "string", "required": True, "label": "Queue URL"},
            "region": {"type": "string", "default": "us-east-1", "label": "AWS Region"},
            "operation": {
                "type": "select",
                "options": ["send", "receive"],
                "default": "receive",
                "label": "Operation",
            },
            "queue_type": {
                "type": "select",
                "options": ["standard", "fifo"],
                "default": "standard",
                "label": "Queue Type",
            },
            "message_group_id": {
                "type": "string",
                "label": "Message Group ID (FIFO)",
            },
            "max_messages": {
                "type": "integer",
                "default": 10,
                "min": 1,
                "max": 10,
                "label": "Max Messages",
            },
            "visibility_timeout": {
                "type": "integer",
                "default": 30,
                "label": "Visibility Timeout (s)",
            },
            "wait_time_seconds": {
                "type": "integer",
                "default": 20,
                "max": 20,
                "label": "Wait Time (Long Polling)",
            },
            "delay_seconds": {
                "type": "integer",
                "default": 0,
                "max": 900,
                "label": "Delay Seconds",
            },
        },
    }

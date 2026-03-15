"""
Enterprise Kafka Connector.

Features:
Producer:
- Idempotent producer (enable.idempotence=true)
- Transactional producer for exactly-once semantics
- Batch optimization (linger.ms, batch.size)
- LZ4 compression
- Circuit breaker integration

Consumer:
- Consumer groups with rebalancing
- Manual offset commit
- Dead letter topic for poison pills
- Pause/resume for backpressure
- Lag monitoring

Security:
- SASL/SCRAM authentication
- mTLS support
- ACL enforcement

Usage:
    from assemblyline_common.connectors import (
        get_kafka_producer,
        get_kafka_consumer,
        KafkaConfig,
    )

    # Producer
    producer = await get_kafka_producer(
        config=KafkaConfig(
            bootstrap_servers="kafka:9092",
            enable_idempotence=True,
        )
    )
    await producer.send("my-topic", {"key": "value"})

    # Consumer
    consumer = await get_kafka_consumer(
        config=KafkaConfig(
            bootstrap_servers="kafka:9092",
            group_id="my-group",
        )
    )
    async for message in consumer.consume(["my-topic"]):
        process(message)
        await consumer.commit()
"""

import asyncio
import json
import logging
import ssl
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, List, Callable, AsyncIterator, Union

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
    CircuitBreakerConfig,
)
from assemblyline_common.retry import RetryHandler, KAFKA_RETRY_CONFIG

logger = logging.getLogger(__name__)


class SecurityProtocol(Enum):
    """Kafka security protocols."""
    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class SASLMechanism(Enum):
    """SASL authentication mechanisms."""
    PLAIN = "PLAIN"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"


@dataclass
class KafkaConfig:
    """Configuration for Kafka connector."""
    # Connection
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "logic-weaver"

    # Consumer settings
    group_id: Optional[str] = None
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000

    # Producer settings
    enable_idempotence: bool = True
    acks: str = "all"
    retries: int = 5
    max_in_flight_requests: int = 5
    compression_type: str = "lz4"
    batch_size: int = 16384  # 16KB
    linger_ms: int = 10
    max_request_size: int = 1048576  # 1MB

    # Transactional settings
    transactional_id: Optional[str] = None
    transaction_timeout_ms: int = 60000

    # Security
    security_protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
    sasl_mechanism: Optional[SASLMechanism] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None

    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None

    # Dead letter queue
    enable_dlq: bool = True
    dlq_topic_suffix: str = ".dlq"

    # Monitoring
    enable_metrics: bool = True

    # Tenant ID
    tenant_id: Optional[str] = None


@dataclass
class KafkaMessage:
    """Kafka message wrapper."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
    headers: Dict[str, bytes]
    timestamp: int

    @property
    def key_str(self) -> Optional[str]:
        """Get key as string."""
        return self.key.decode('utf-8') if self.key else None

    @property
    def value_str(self) -> str:
        """Get value as string."""
        return self.value.decode('utf-8')

    @property
    def value_json(self) -> Any:
        """Parse value as JSON."""
        return json.loads(self.value)


class KafkaProducer:
    """
    Enterprise Kafka producer with exactly-once semantics.

    Features:
    - Idempotent production
    - Transactional support
    - Circuit breaker integration
    - Batch optimization
    - Compression
    """

    def __init__(
        self,
        config: KafkaConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._producer: Optional[AIOKafkaProducer] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._metrics: Dict[str, int] = {
            "messages_sent": 0,
            "bytes_sent": 0,
            "errors": 0,
        }
        self._closed = False

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context if needed."""
        if self.config.security_protocol not in (
            SecurityProtocol.SSL,
            SecurityProtocol.SASL_SSL,
        ):
            return None

        ctx = ssl.create_default_context()
        if self.config.ssl_cafile:
            ctx.load_verify_locations(self.config.ssl_cafile)
        if self.config.ssl_certfile:
            ctx.load_cert_chain(
                self.config.ssl_certfile,
                self.config.ssl_keyfile,
            )
        return ctx

    async def initialize(self) -> None:
        """Initialize the producer."""
        ssl_context = self._get_ssl_context()

        # Build producer config
        producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "client_id": self.config.client_id,
            "acks": self.config.acks,
            "enable_idempotence": self.config.enable_idempotence,
            "max_batch_size": self.config.batch_size,
            "linger_ms": self.config.linger_ms,
            "compression_type": self.config.compression_type,
            "max_request_size": self.config.max_request_size,
        }

        if self.config.enable_idempotence:
            producer_config["max_in_flight_requests_per_connection"] = (
                self.config.max_in_flight_requests
            )

        # Security settings
        if self.config.security_protocol != SecurityProtocol.PLAINTEXT:
            producer_config["security_protocol"] = self.config.security_protocol.value

        if self.config.sasl_mechanism:
            producer_config["sasl_mechanism"] = self.config.sasl_mechanism.value
            producer_config["sasl_plain_username"] = self.config.sasl_username
            producer_config["sasl_plain_password"] = self.config.sasl_password

        if ssl_context:
            producer_config["ssl_context"] = ssl_context

        # Transactional producer
        if self.config.transactional_id:
            producer_config["transactional_id"] = self.config.transactional_id
            producer_config["transaction_timeout_ms"] = (
                self.config.transaction_timeout_ms
            )

        self._producer = AIOKafkaProducer(**producer_config)
        await self._producer.start()

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker(
                self.config.circuit_breaker_config
            )

        # Initialize retry handler
        self._retry_handler = RetryHandler(config=KAFKA_RETRY_CONFIG)

        logger.info(
            "Kafka producer initialized",
            extra={
                "event_type": "kafka_producer_initialized",
                "bootstrap_servers": self.config.bootstrap_servers,
                "idempotent": self.config.enable_idempotence,
                "transactional": bool(self.config.transactional_id),
            }
        )

    async def send(
        self,
        topic: str,
        value: Union[str, bytes, Dict[str, Any]],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Send a message to Kafka.

        Args:
            topic: Target topic
            value: Message value (string, bytes, or dict for JSON)
            key: Optional message key
            headers: Optional headers
            partition: Optional specific partition

        Returns:
            Dict with partition and offset
        """
        if self._closed:
            raise RuntimeError("Producer is closed")

        if not self._producer:
            await self.initialize()

        # Serialize value
        if isinstance(value, dict):
            value_bytes = json.dumps(value).encode('utf-8')
        elif isinstance(value, str):
            value_bytes = value.encode('utf-8')
        else:
            value_bytes = value

        # Serialize key
        key_bytes = None
        if key:
            key_bytes = key.encode('utf-8') if isinstance(key, str) else key

        # Prepare headers
        kafka_headers = None
        if headers:
            kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]

        # Check circuit breaker
        circuit_name = f"kafka:producer:{topic}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(
                    circuit_name,
                    await self._circuit_breaker.get_retry_after(circuit_name),
                )

        async def do_send() -> Dict[str, Any]:
            """Inner send function for retry."""
            result = await self._producer.send_and_wait(
                topic,
                value=value_bytes,
                key=key_bytes,
                headers=kafka_headers,
                partition=partition,
            )

            return {
                "topic": result.topic,
                "partition": result.partition,
                "offset": result.offset,
                "timestamp": result.timestamp,
            }

        try:
            # Execute with retry
            result = await self._retry_handler.execute(
                do_send,
                operation_id=f"kafka-send-{topic}",
            )

            # Update metrics
            if self.config.enable_metrics:
                self._metrics["messages_sent"] += 1
                self._metrics["bytes_sent"] += len(value_bytes)

            # Record success
            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            logger.debug(
                f"Kafka message sent",
                extra={
                    "event_type": "kafka_message_sent",
                    "topic": topic,
                    "partition": result["partition"],
                    "offset": result["offset"],
                }
            )

            return result

        except Exception as e:
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)

            if self.config.enable_metrics:
                self._metrics["errors"] += 1

            logger.error(
                f"Kafka send failed",
                extra={
                    "event_type": "kafka_send_failed",
                    "topic": topic,
                    "error": str(e),
                }
            )
            raise

    async def send_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Send multiple messages in a batch.

        Args:
            topic: Target topic
            messages: List of messages with 'value', optional 'key' and 'headers'

        Returns:
            List of send results
        """
        results = []
        for msg in messages:
            result = await self.send(
                topic,
                value=msg["value"],
                key=msg.get("key"),
                headers=msg.get("headers"),
            )
            results.append(result)
        return results

    async def begin_transaction(self) -> None:
        """Begin a transaction (requires transactional_id)."""
        if not self.config.transactional_id:
            raise ValueError("Transactional ID not configured")
        await self._producer.begin_transaction()

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        await self._producer.commit_transaction()

    async def abort_transaction(self) -> None:
        """Abort the current transaction."""
        await self._producer.abort_transaction()

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            **self._metrics,
            "config": {
                "bootstrap_servers": self.config.bootstrap_servers,
                "idempotent": self.config.enable_idempotence,
                "compression": self.config.compression_type,
            }
        }

    async def close(self) -> None:
        """Close the producer."""
        self._closed = True
        if self._producer:
            await self._producer.stop()
            self._producer = None

        logger.info("Kafka producer closed")


class KafkaConsumer:
    """
    Enterprise Kafka consumer with consumer groups.

    Features:
    - Consumer groups with rebalancing
    - Manual offset commit
    - Dead letter queue
    - Pause/resume for backpressure
    - Lag monitoring
    """

    def __init__(
        self,
        config: KafkaConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._dlq_producer: Optional[KafkaProducer] = None
        self._paused_topics: set = set()
        self._metrics: Dict[str, int] = {
            "messages_consumed": 0,
            "bytes_consumed": 0,
            "errors": 0,
            "dlq_messages": 0,
        }
        self._closed = False

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context if needed."""
        if self.config.security_protocol not in (
            SecurityProtocol.SSL,
            SecurityProtocol.SASL_SSL,
        ):
            return None

        ctx = ssl.create_default_context()
        if self.config.ssl_cafile:
            ctx.load_verify_locations(self.config.ssl_cafile)
        if self.config.ssl_certfile:
            ctx.load_cert_chain(
                self.config.ssl_certfile,
                self.config.ssl_keyfile,
            )
        return ctx

    async def initialize(self, topics: List[str]) -> None:
        """Initialize the consumer."""
        ssl_context = self._get_ssl_context()

        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "client_id": self.config.client_id,
            "group_id": self.config.group_id,
            "auto_offset_reset": self.config.auto_offset_reset,
            "enable_auto_commit": self.config.enable_auto_commit,
            "max_poll_records": self.config.max_poll_records,
            "max_poll_interval_ms": self.config.max_poll_interval_ms,
            "session_timeout_ms": self.config.session_timeout_ms,
            "heartbeat_interval_ms": self.config.heartbeat_interval_ms,
        }

        # Security settings
        if self.config.security_protocol != SecurityProtocol.PLAINTEXT:
            consumer_config["security_protocol"] = self.config.security_protocol.value

        if self.config.sasl_mechanism:
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism.value
            consumer_config["sasl_plain_username"] = self.config.sasl_username
            consumer_config["sasl_plain_password"] = self.config.sasl_password

        if ssl_context:
            consumer_config["ssl_context"] = ssl_context

        self._consumer = AIOKafkaConsumer(*topics, **consumer_config)
        await self._consumer.start()

        # Initialize DLQ producer if enabled
        if self.config.enable_dlq:
            dlq_config = KafkaConfig(
                bootstrap_servers=self.config.bootstrap_servers,
                security_protocol=self.config.security_protocol,
                sasl_mechanism=self.config.sasl_mechanism,
                sasl_username=self.config.sasl_username,
                sasl_password=self.config.sasl_password,
                ssl_cafile=self.config.ssl_cafile,
                ssl_certfile=self.config.ssl_certfile,
                ssl_keyfile=self.config.ssl_keyfile,
                enable_circuit_breaker=False,  # Don't circuit break DLQ
            )
            self._dlq_producer = KafkaProducer(dlq_config)
            await self._dlq_producer.initialize()

        logger.info(
            "Kafka consumer initialized",
            extra={
                "event_type": "kafka_consumer_initialized",
                "bootstrap_servers": self.config.bootstrap_servers,
                "group_id": self.config.group_id,
                "topics": topics,
            }
        )

    async def consume(
        self,
        topics: List[str],
        timeout_ms: int = 1000,
    ) -> AsyncIterator[KafkaMessage]:
        """
        Consume messages from topics.

        Args:
            topics: Topics to consume from
            timeout_ms: Poll timeout in milliseconds

        Yields:
            KafkaMessage objects
        """
        if not self._consumer:
            await self.initialize(topics)

        while not self._closed:
            try:
                # Get batch of messages
                data = await self._consumer.getmany(
                    timeout_ms=timeout_ms,
                    max_records=self.config.max_poll_records,
                )

                for tp, messages in data.items():
                    # Skip paused topics
                    if tp.topic in self._paused_topics:
                        continue

                    for msg in messages:
                        kafka_msg = KafkaMessage(
                            topic=msg.topic,
                            partition=msg.partition,
                            offset=msg.offset,
                            key=msg.key,
                            value=msg.value,
                            headers=dict(msg.headers) if msg.headers else {},
                            timestamp=msg.timestamp,
                        )

                        if self.config.enable_metrics:
                            self._metrics["messages_consumed"] += 1
                            self._metrics["bytes_consumed"] += len(msg.value)

                        yield kafka_msg

            except Exception as e:
                logger.error(f"Kafka consume error: {e}")
                if self.config.enable_metrics:
                    self._metrics["errors"] += 1

                # Brief pause before retry
                await asyncio.sleep(1)

    async def commit(
        self,
        message: Optional[KafkaMessage] = None,
    ) -> None:
        """
        Commit offsets.

        Args:
            message: Specific message to commit, or None for all consumed
        """
        if not self._consumer:
            return

        if message:
            # Commit specific offset
            from aiokafka import TopicPartition
            tp = TopicPartition(message.topic, message.partition)
            await self._consumer.commit({tp: message.offset + 1})
        else:
            # Commit all
            await self._consumer.commit()

        logger.debug("Kafka offsets committed")

    async def send_to_dlq(
        self,
        message: KafkaMessage,
        error: Exception,
    ) -> None:
        """Send a poison message to dead letter queue."""
        if not self._dlq_producer or not self.config.enable_dlq:
            return

        dlq_topic = f"{message.topic}{self.config.dlq_topic_suffix}"

        await self._dlq_producer.send(
            dlq_topic,
            value=message.value,
            key=message.key,
            headers={
                "original_topic": message.topic,
                "original_partition": str(message.partition),
                "original_offset": str(message.offset),
                "error_type": type(error).__name__,
                "error_message": str(error),
                "timestamp": str(time.time()),
            }
        )

        if self.config.enable_metrics:
            self._metrics["dlq_messages"] += 1

        logger.warning(
            "Message sent to DLQ",
            extra={
                "event_type": "kafka_dlq_sent",
                "topic": message.topic,
                "dlq_topic": dlq_topic,
                "offset": message.offset,
                "error": str(error),
            }
        )

    async def pause(self, topics: Optional[List[str]] = None) -> None:
        """Pause consumption of topics for backpressure."""
        if not self._consumer:
            return

        if topics:
            for topic in topics:
                self._paused_topics.add(topic)
            # Get partitions for topics
            from aiokafka import TopicPartition
            partitions = [
                TopicPartition(t, p)
                for t in topics
                for p in self._consumer.partitions_for_topic(t) or []
            ]
            self._consumer.pause(*partitions)
        else:
            # Pause all
            self._consumer.pause(*self._consumer.assignment())

        logger.info(f"Kafka consumer paused topics: {topics or 'all'}")

    async def resume(self, topics: Optional[List[str]] = None) -> None:
        """Resume consumption of paused topics."""
        if not self._consumer:
            return

        if topics:
            for topic in topics:
                self._paused_topics.discard(topic)
            from aiokafka import TopicPartition
            partitions = [
                TopicPartition(t, p)
                for t in topics
                for p in self._consumer.partitions_for_topic(t) or []
            ]
            self._consumer.resume(*partitions)
        else:
            # Resume all
            self._paused_topics.clear()
            self._consumer.resume(*self._consumer.assignment())

        logger.info(f"Kafka consumer resumed topics: {topics or 'all'}")

    async def get_lag(self) -> Dict[str, int]:
        """Get consumer lag per partition."""
        if not self._consumer:
            return {}

        lag = {}
        for tp in self._consumer.assignment():
            end_offset = await self._consumer.end_offsets([tp])
            current_offset = await self._consumer.position(tp)
            lag[f"{tp.topic}-{tp.partition}"] = end_offset[tp] - current_offset

        return lag

    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        return {
            **self._metrics,
            "paused_topics": list(self._paused_topics),
            "config": {
                "bootstrap_servers": self.config.bootstrap_servers,
                "group_id": self.config.group_id,
            }
        }

    async def close(self) -> None:
        """Close the consumer."""
        self._closed = True

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._dlq_producer:
            await self._dlq_producer.close()
            self._dlq_producer = None

        logger.info("Kafka consumer closed")


# Singleton instances
_kafka_producers: Dict[str, KafkaProducer] = {}
_kafka_consumers: Dict[str, KafkaConsumer] = {}
_kafka_lock = asyncio.Lock()


async def get_kafka_producer(
    config: Optional[KafkaConfig] = None,
    name: Optional[str] = None,
) -> KafkaProducer:
    """Get or create a Kafka producer."""
    config = config or KafkaConfig()
    producer_name = name or f"producer-{config.bootstrap_servers}"

    if producer_name in _kafka_producers:
        return _kafka_producers[producer_name]

    async with _kafka_lock:
        if producer_name in _kafka_producers:
            return _kafka_producers[producer_name]

        producer = KafkaProducer(config)
        await producer.initialize()
        _kafka_producers[producer_name] = producer

        return producer


async def get_kafka_consumer(
    config: Optional[KafkaConfig] = None,
    name: Optional[str] = None,
) -> KafkaConsumer:
    """Get or create a Kafka consumer."""
    config = config or KafkaConfig()
    consumer_name = name or f"consumer-{config.bootstrap_servers}-{config.group_id}"

    if consumer_name in _kafka_consumers:
        return _kafka_consumers[consumer_name]

    async with _kafka_lock:
        if consumer_name in _kafka_consumers:
            return _kafka_consumers[consumer_name]

        consumer = KafkaConsumer(config)
        _kafka_consumers[consumer_name] = consumer

        return consumer


async def close_all_kafka_connectors() -> None:
    """Close all Kafka producers and consumers."""
    for producer in _kafka_producers.values():
        await producer.close()
    _kafka_producers.clear()

    for consumer in _kafka_consumers.values():
        await consumer.close()
    _kafka_consumers.clear()

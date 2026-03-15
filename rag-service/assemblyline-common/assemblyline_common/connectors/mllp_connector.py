"""
Enterprise MLLP Connector for HL7 messaging.

MLLP (Minimal Lower Layer Protocol) is the standard transport for HL7 v2 messages.

Features:
- Persistent TCP connections with pooling
- TLS 1.2+ support with certificate validation
- Client certificate authentication (mTLS)
- ACK/NAK handling with configurable retry
- Duplicate message detection
- Flow control and backpressure handling
- Primary/secondary failover
- Health-aware routing

Message Format:
    <VT> message <FS><CR>
    VT = 0x0B (vertical tab)
    FS = 0x1C (file separator)
    CR = 0x0D (carriage return)

Usage:
    from assemblyline_common.connectors import get_mllp_connector, MLLPConnectorConfig

    connector = await get_mllp_connector(
        config=MLLPConnectorConfig(
            primary_host="hl7.hospital.com",
            primary_port=2575,
            enable_tls=True,
        )
    )

    # Send message and wait for ACK
    ack = await connector.send(hl7_message)
    if ack.is_ack:
        print("Message accepted")
"""

import asyncio
import hashlib
import logging
import ssl
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, List, Set, Tuple
from collections import deque

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
    CircuitBreakerConfig,
)
from assemblyline_common.retry import RetryHandler, RetryConfig, MLLP_RETRY_CONFIG
from assemblyline_common.pool import ConnectionPool, ConnectionPoolConfig, ConnectionFactory

logger = logging.getLogger(__name__)

# MLLP Frame characters
MLLP_START = b'\x0b'  # VT (Vertical Tab)
MLLP_END = b'\x1c\x0d'  # FS + CR


class AckType(Enum):
    """HL7 acknowledgment types."""
    AA = "AA"  # Application Accept
    AE = "AE"  # Application Error
    AR = "AR"  # Application Reject
    CA = "CA"  # Commit Accept
    CE = "CE"  # Commit Error
    CR = "CR"  # Commit Reject


@dataclass
class MLLPMessage:
    """HL7 message wrapper."""
    raw_message: bytes
    message_id: str = ""
    message_type: str = ""
    sending_facility: str = ""
    receiving_facility: str = ""
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def from_bytes(cls, data: bytes) -> "MLLPMessage":
        """Parse MLLP-framed message."""
        # Strip MLLP framing if present
        if data.startswith(MLLP_START):
            data = data[1:]
        if data.endswith(MLLP_END):
            data = data[:-2]

        # Parse MSH segment for metadata
        try:
            message_str = data.decode('utf-8', errors='replace')
            segments = message_str.split('\r')
            msh = segments[0] if segments else ""

            if msh.startswith('MSH'):
                fields = msh.split('|')
                message_type = fields[8] if len(fields) > 8 else ""
                message_id = fields[9] if len(fields) > 9 else ""
                sending_facility = fields[3] if len(fields) > 3 else ""
                receiving_facility = fields[5] if len(fields) > 5 else ""
            else:
                message_type = ""
                message_id = ""
                sending_facility = ""
                receiving_facility = ""
        except Exception:
            message_type = ""
            message_id = ""
            sending_facility = ""
            receiving_facility = ""

        return cls(
            raw_message=data,
            message_id=message_id,
            message_type=message_type,
            sending_facility=sending_facility,
            receiving_facility=receiving_facility,
        )

    def to_mllp_frame(self) -> bytes:
        """Wrap message in MLLP framing."""
        return MLLP_START + self.raw_message + MLLP_END

    @property
    def content_hash(self) -> str:
        """Get hash of message content for deduplication."""
        return hashlib.sha256(self.raw_message).hexdigest()[:16]


@dataclass
class MLLPAck:
    """HL7 acknowledgment message."""
    raw_message: bytes
    ack_type: AckType
    message_id: str
    error_message: str = ""

    @property
    def is_ack(self) -> bool:
        """Check if this is a positive acknowledgment."""
        return self.ack_type in (AckType.AA, AckType.CA)

    @property
    def is_error(self) -> bool:
        """Check if this indicates an error."""
        return self.ack_type in (AckType.AE, AckType.CE)

    @property
    def is_reject(self) -> bool:
        """Check if this is a rejection."""
        return self.ack_type in (AckType.AR, AckType.CR)

    @classmethod
    def from_bytes(cls, data: bytes) -> "MLLPAck":
        """Parse ACK message."""
        # Strip MLLP framing if present
        if data.startswith(MLLP_START):
            data = data[1:]
        if data.endswith(MLLP_END):
            data = data[:-2]

        try:
            message_str = data.decode('utf-8', errors='replace')
            segments = message_str.split('\r')

            # Parse MSA segment
            msa = next((s for s in segments if s.startswith('MSA')), None)
            if msa:
                msa_fields = msa.split('|')
                ack_code = msa_fields[1] if len(msa_fields) > 1 else "AE"
                message_id = msa_fields[2] if len(msa_fields) > 2 else ""
                error_message = msa_fields[3] if len(msa_fields) > 3 else ""
            else:
                ack_code = "AE"
                message_id = ""
                error_message = "No MSA segment in response"

            try:
                ack_type = AckType(ack_code)
            except ValueError:
                ack_type = AckType.AE

        except Exception as e:
            ack_type = AckType.AE
            message_id = ""
            error_message = str(e)

        return cls(
            raw_message=data,
            ack_type=ack_type,
            message_id=message_id,
            error_message=error_message,
        )


@dataclass
class MLLPConnectorConfig:
    """Configuration for MLLP connector."""
    # Primary endpoint
    primary_host: str = "localhost"
    primary_port: int = 2575

    # Secondary endpoint (failover)
    secondary_host: Optional[str] = None
    secondary_port: int = 2575

    # Connection settings
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 30.0

    # TLS settings
    enable_tls: bool = False
    tls_version: str = "TLSv1_2"
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None
    ca_cert_path: Optional[str] = None
    verify_hostname: bool = True

    # Pool settings
    pool_min_size: int = 1
    pool_max_size: int = 10
    pool_max_idle_time: float = 300.0

    # Retry settings
    enable_retry: bool = True
    retry_config: Optional[RetryConfig] = None
    ack_timeout: float = 30.0  # Time to wait for ACK

    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None

    # Duplicate detection
    enable_duplicate_detection: bool = True
    duplicate_window_seconds: int = 300  # 5 minutes

    # Flow control
    max_pending_messages: int = 100
    enable_backpressure: bool = True

    # Tenant ID
    tenant_id: Optional[str] = None


class MLLPConnection:
    """Single MLLP connection."""

    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: Optional[ssl.SSLContext] = None,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Establish connection."""
        async with self._lock:
            if self._connected:
                return

            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        self.host,
                        self.port,
                        ssl=self.ssl_context,
                    ),
                    timeout=self.connect_timeout,
                )
                self._connected = True

                logger.info(
                    f"MLLP connected",
                    extra={
                        "event_type": "mllp_connected",
                        "host": self.host,
                        "port": self.port,
                        "tls": self.ssl_context is not None,
                    }
                )

            except Exception as e:
                logger.error(f"MLLP connection failed: {e}")
                raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    async def send_and_receive(
        self,
        message: MLLPMessage,
        ack_timeout: float = 30.0,
    ) -> MLLPAck:
        """Send message and wait for ACK."""
        if not self._connected:
            await self.connect()

        async with self._lock:
            try:
                # Send message
                frame = message.to_mllp_frame()
                self._writer.write(frame)
                await self._writer.drain()

                # Read response
                response_data = await asyncio.wait_for(
                    self._read_mllp_frame(),
                    timeout=ack_timeout,
                )

                return MLLPAck.from_bytes(response_data)

            except asyncio.TimeoutError:
                logger.warning(f"MLLP ACK timeout for message {message.message_id}")
                raise TimeoutError(f"ACK timeout for message {message.message_id}")
            except Exception as e:
                logger.error(f"MLLP send error: {e}")
                self._connected = False
                raise

    async def _read_mllp_frame(self) -> bytes:
        """Read a complete MLLP frame."""
        buffer = b""

        # Read until we find the MLLP end sequence
        while True:
            chunk = await self._reader.read(4096)
            if not chunk:
                raise ConnectionError("Connection closed by remote")

            buffer += chunk

            # Check for complete frame
            if buffer.endswith(MLLP_END):
                return buffer

    async def close(self) -> None:
        """Close the connection."""
        async with self._lock:
            if self._writer:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except Exception:
                    pass
            self._reader = None
            self._writer = None
            self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self._connected


class MLLPConnectionFactory(ConnectionFactory[MLLPConnection]):
    """Factory for MLLP connections."""

    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: Optional[ssl.SSLContext] = None,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

    async def create(self) -> MLLPConnection:
        """Create a new connection."""
        conn = MLLPConnection(
            self.host,
            self.port,
            self.ssl_context,
            self.connect_timeout,
            self.read_timeout,
        )
        await conn.connect()
        return conn

    async def validate(self, connection: MLLPConnection) -> bool:
        """Validate connection is healthy."""
        return connection.is_connected

    async def close(self, connection: MLLPConnection) -> None:
        """Close a connection."""
        await connection.close()


class MLLPConnector:
    """
    Enterprise MLLP connector with resilience patterns.

    Features:
    - Connection pooling
    - TLS support
    - Circuit breaker
    - Retry with backoff
    - Duplicate detection
    - Flow control
    - Primary/secondary failover
    """

    def __init__(self, config: MLLPConnectorConfig):
        self.config = config

        self._primary_pool: Optional[ConnectionPool[MLLPConnection]] = None
        self._secondary_pool: Optional[ConnectionPool[MLLPConnection]] = None
        self._circuit_breaker: Optional[CircuitBreaker] = None
        self._retry_handler: Optional[RetryHandler] = None

        # Duplicate detection
        self._seen_messages: Dict[str, float] = {}
        self._duplicate_cleanup_task: Optional[asyncio.Task] = None

        # Flow control
        self._pending_count = 0
        self._pending_condition = asyncio.Condition()

        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        # Create SSL context if TLS enabled
        ssl_context = None
        if self.config.enable_tls:
            ssl_context = ssl.create_default_context()
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

            if self.config.client_cert_path:
                ssl_context.load_cert_chain(
                    self.config.client_cert_path,
                    self.config.client_key_path,
                )

            if self.config.ca_cert_path:
                ssl_context.load_verify_locations(self.config.ca_cert_path)

            ssl_context.check_hostname = self.config.verify_hostname

        # Create primary connection pool
        primary_factory = MLLPConnectionFactory(
            self.config.primary_host,
            self.config.primary_port,
            ssl_context,
            self.config.connect_timeout,
            self.config.read_timeout,
        )

        self._primary_pool = ConnectionPool(
            factory=primary_factory,
            config=ConnectionPoolConfig(
                min_size=self.config.pool_min_size,
                max_size=self.config.pool_max_size,
                max_idle_time=self.config.pool_max_idle_time,
            ),
            name=f"mllp-primary-{self.config.primary_host}",
        )
        await self._primary_pool.initialize()

        # Create secondary pool if configured
        if self.config.secondary_host:
            secondary_factory = MLLPConnectionFactory(
                self.config.secondary_host,
                self.config.secondary_port,
                ssl_context,
                self.config.connect_timeout,
                self.config.read_timeout,
            )

            self._secondary_pool = ConnectionPool(
                factory=secondary_factory,
                config=ConnectionPoolConfig(
                    min_size=1,
                    max_size=self.config.pool_max_size // 2,
                    max_idle_time=self.config.pool_max_idle_time,
                ),
                name=f"mllp-secondary-{self.config.secondary_host}",
            )
            await self._secondary_pool.initialize()

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker(
                self.config.circuit_breaker_config
            )

        # Initialize retry handler
        if self.config.enable_retry:
            self._retry_handler = RetryHandler(
                config=self.config.retry_config or MLLP_RETRY_CONFIG
            )

        # Start duplicate cleanup task
        if self.config.enable_duplicate_detection:
            self._duplicate_cleanup_task = asyncio.create_task(
                self._cleanup_duplicates_loop()
            )

        logger.info(
            "MLLP connector initialized",
            extra={
                "event_type": "mllp_connector_initialized",
                "primary": f"{self.config.primary_host}:{self.config.primary_port}",
                "secondary": f"{self.config.secondary_host}:{self.config.secondary_port}"
                            if self.config.secondary_host else None,
                "tls": self.config.enable_tls,
            }
        )

    def _get_circuit_name(self, is_primary: bool) -> str:
        """Get circuit breaker name."""
        if is_primary:
            return f"mllp:{self.config.primary_host}:{self.config.primary_port}"
        return f"mllp:{self.config.secondary_host}:{self.config.secondary_port}"

    def _is_duplicate(self, message: MLLPMessage) -> bool:
        """Check if message is a duplicate."""
        if not self.config.enable_duplicate_detection:
            return False

        content_hash = message.content_hash
        if content_hash in self._seen_messages:
            logger.warning(
                f"Duplicate message detected",
                extra={
                    "event_type": "mllp_duplicate_detected",
                    "message_id": message.message_id,
                    "content_hash": content_hash,
                }
            )
            return True

        self._seen_messages[content_hash] = time.time()
        return False

    async def _cleanup_duplicates_loop(self) -> None:
        """Background task to cleanup old duplicate entries."""
        while not self._closed:
            try:
                await asyncio.sleep(60)  # Check every minute

                cutoff = time.time() - self.config.duplicate_window_seconds
                to_remove = [
                    h for h, t in self._seen_messages.items() if t < cutoff
                ]
                for h in to_remove:
                    del self._seen_messages[h]

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Duplicate cleanup error: {e}")

    async def _wait_for_capacity(self) -> None:
        """Wait for capacity if backpressure is enabled."""
        if not self.config.enable_backpressure:
            return

        async with self._pending_condition:
            while self._pending_count >= self.config.max_pending_messages:
                await self._pending_condition.wait()

    async def _acquire_capacity(self) -> None:
        """Acquire a slot for sending."""
        await self._wait_for_capacity()
        async with self._pending_condition:
            self._pending_count += 1

    async def _release_capacity(self) -> None:
        """Release a sending slot."""
        async with self._pending_condition:
            self._pending_count -= 1
            self._pending_condition.notify()

    async def send(
        self,
        message: MLLPMessage,
        require_ack: bool = True,
    ) -> MLLPAck:
        """
        Send an HL7 message and wait for acknowledgment.

        Args:
            message: The HL7 message to send
            require_ack: Whether to wait for ACK (default True)

        Returns:
            MLLPAck response

        Raises:
            CircuitOpenError: If circuit breaker is open
            ConnectionError: If connection fails
            TimeoutError: If ACK timeout
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        # Check for duplicates
        if self._is_duplicate(message):
            # Return synthetic ACK for duplicates
            return MLLPAck(
                raw_message=b"",
                ack_type=AckType.AA,
                message_id=message.message_id,
                error_message="Duplicate message (already processed)",
            )

        # Wait for capacity
        await self._acquire_capacity()

        try:
            return await self._send_with_failover(message, require_ack)
        finally:
            await self._release_capacity()

    async def _send_with_failover(
        self,
        message: MLLPMessage,
        require_ack: bool,
    ) -> MLLPAck:
        """Send with primary/secondary failover."""
        # Try primary first
        primary_circuit = self._get_circuit_name(True)

        if self._circuit_breaker:
            can_use_primary = await self._circuit_breaker.can_execute(primary_circuit)
        else:
            can_use_primary = True

        if can_use_primary:
            try:
                return await self._send_to_pool(
                    self._primary_pool,
                    message,
                    require_ack,
                    is_primary=True,
                )
            except Exception as e:
                logger.warning(f"Primary MLLP failed: {e}")
                if self._circuit_breaker:
                    await self._circuit_breaker.record_failure(primary_circuit, e)

                # Fall through to secondary
                if not self._secondary_pool:
                    raise

        # Try secondary
        if self._secondary_pool:
            secondary_circuit = self._get_circuit_name(False)

            if self._circuit_breaker:
                can_use_secondary = await self._circuit_breaker.can_execute(secondary_circuit)
            else:
                can_use_secondary = True

            if can_use_secondary:
                try:
                    return await self._send_to_pool(
                        self._secondary_pool,
                        message,
                        require_ack,
                        is_primary=False,
                    )
                except Exception as e:
                    if self._circuit_breaker:
                        await self._circuit_breaker.record_failure(secondary_circuit, e)
                    raise

        raise CircuitOpenError(
            "mllp:all",
            await self._circuit_breaker.get_retry_after(primary_circuit)
            if self._circuit_breaker else 0
        )

    async def _send_to_pool(
        self,
        pool: ConnectionPool[MLLPConnection],
        message: MLLPMessage,
        require_ack: bool,
        is_primary: bool,
    ) -> MLLPAck:
        """Send to a specific pool."""
        async def do_send() -> MLLPAck:
            pooled = await pool.acquire()
            try:
                ack = await pooled.connection.send_and_receive(
                    message,
                    self.config.ack_timeout,
                )

                # Check ACK type
                if ack.is_reject:
                    raise ValueError(f"Message rejected: {ack.error_message}")

                await pool.release(pooled)
                return ack

            except Exception:
                await pool.release(pooled, force_close=True)
                raise

        # Execute with retry
        if self._retry_handler:
            ack = await self._retry_handler.execute(
                do_send,
                operation_id=f"mllp-send-{message.message_id}",
            )
        else:
            ack = await do_send()

        # Record success
        circuit = self._get_circuit_name(is_primary)
        if self._circuit_breaker:
            await self._circuit_breaker.record_success(circuit)

        logger.info(
            f"MLLP message sent",
            extra={
                "event_type": "mllp_message_sent",
                "message_id": message.message_id,
                "message_type": message.message_type,
                "ack_type": ack.ack_type.value,
                "is_primary": is_primary,
            }
        )

        return ack

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True

        if self._duplicate_cleanup_task:
            self._duplicate_cleanup_task.cancel()
            try:
                await self._duplicate_cleanup_task
            except asyncio.CancelledError:
                pass

        if self._primary_pool:
            await self._primary_pool.close()

        if self._secondary_pool:
            await self._secondary_pool.close()

        logger.info("MLLP connector closed")

    def get_stats(self) -> Dict[str, Any]:
        """Get connector statistics."""
        return {
            "primary_pool": self._primary_pool.get_stats() if self._primary_pool else None,
            "secondary_pool": self._secondary_pool.get_stats() if self._secondary_pool else None,
            "pending_messages": self._pending_count,
            "duplicate_cache_size": len(self._seen_messages),
        }


# Singleton connectors
_mllp_connectors: Dict[str, MLLPConnector] = {}
_mllp_lock = asyncio.Lock()


async def get_mllp_connector(
    config: Optional[MLLPConnectorConfig] = None,
    name: Optional[str] = None,
) -> MLLPConnector:
    """Get or create an MLLP connector."""
    config = config or MLLPConnectorConfig()
    connector_name = name or f"{config.primary_host}:{config.primary_port}"

    if connector_name in _mllp_connectors:
        return _mllp_connectors[connector_name]

    async with _mllp_lock:
        if connector_name in _mllp_connectors:
            return _mllp_connectors[connector_name]

        connector = MLLPConnector(config)
        await connector.initialize()
        _mllp_connectors[connector_name] = connector

        return connector


async def close_all_mllp_connectors() -> None:
    """Close all MLLP connectors."""
    for connector in _mllp_connectors.values():
        await connector.close()
    _mllp_connectors.clear()

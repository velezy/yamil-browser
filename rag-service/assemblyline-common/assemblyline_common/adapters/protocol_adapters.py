"""
Protocol Adapters for Logic Weaver.

Unified adapters for various network protocols:
- HTTP/REST: Inbound webhooks, outbound API calls
- WebSocket: Real-time bidirectional communication
- gRPC: High-performance RPC with streaming
- SFTP/FTP: File transfer polling and pushing
- Email: IMAP polling, SMTP sending
- MLLP: HL7 v2.x healthcare messaging

All adapters implement a common interface for:
- Connection management
- Message receive/send
- Error handling with retry
- Metrics collection
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union
import asyncio
import logging
import ssl
import time

logger = logging.getLogger(__name__)


# =============================================================================
# Base Classes
# =============================================================================

class AdapterDirection(str, Enum):
    """Adapter direction."""
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    BIDIRECTIONAL = "bidirectional"


class AdapterStatus(str, Enum):
    """Adapter connection status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"
    RECONNECTING = "reconnecting"


@dataclass
class AdapterMessage:
    """Universal message container."""
    id: str
    payload: Union[bytes, str, Dict[str, Any]]
    headers: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source: str = ""
    content_type: str = "application/octet-stream"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "payload": self.payload if isinstance(self.payload, (str, dict)) else self.payload.hex(),
            "headers": self.headers,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "content_type": self.content_type,
        }


@dataclass
class AdapterResult:
    """Result of adapter operation."""
    success: bool
    message_id: Optional[str] = None
    response: Optional[Any] = None
    error: Optional[str] = None
    latency_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AdapterConfig:
    """Base adapter configuration."""
    name: str = "adapter"
    tenant_id: Optional[str] = None
    direction: AdapterDirection = AdapterDirection.BIDIRECTIONAL

    # Connection settings
    timeout_seconds: float = 30.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.0

    # TLS/SSL
    use_tls: bool = False
    tls_verify: bool = True
    tls_cert_path: Optional[str] = None
    tls_key_path: Optional[str] = None
    tls_ca_path: Optional[str] = None

    # Circuit breaker
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0


class ProtocolAdapter(ABC):
    """
    Base class for all protocol adapters.

    Provides unified interface for:
    - Connection lifecycle (connect, disconnect, reconnect)
    - Message operations (send, receive, subscribe)
    - Health checks
    - Metrics
    """

    def __init__(self, config: AdapterConfig):
        self.config = config
        self._status = AdapterStatus.DISCONNECTED
        self._connected_at: Optional[datetime] = None
        self._message_count = 0
        self._error_count = 0
        self._last_error: Optional[str] = None
        self._circuit_open = False
        self._circuit_failures = 0

    @property
    def status(self) -> AdapterStatus:
        return self._status

    @property
    def is_connected(self) -> bool:
        return self._status == AdapterStatus.CONNECTED

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection."""
        pass

    @abstractmethod
    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Send a message."""
        pass

    @abstractmethod
    async def receive(self) -> Optional[AdapterMessage]:
        """Receive a single message."""
        pass

    async def subscribe(
        self,
        handler: Callable[[AdapterMessage], None],
    ) -> None:
        """Subscribe to incoming messages with handler."""
        while self.is_connected:
            try:
                message = await self.receive()
                if message:
                    await handler(message)
            except Exception as e:
                logger.error(f"Error in subscription handler: {e}")
                await asyncio.sleep(1)

    async def health_check(self) -> Dict[str, Any]:
        """Check adapter health."""
        return {
            "name": self.config.name,
            "status": self._status.value,
            "connected": self.is_connected,
            "connected_at": self._connected_at.isoformat() if self._connected_at else None,
            "message_count": self._message_count,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "circuit_open": self._circuit_open,
        }

    def _check_circuit(self) -> bool:
        """Check if circuit breaker allows operation."""
        if not self.config.circuit_breaker_enabled:
            return True
        return not self._circuit_open

    def _record_success(self) -> None:
        """Record successful operation."""
        self._message_count += 1
        self._circuit_failures = 0
        if self._circuit_open:
            self._circuit_open = False
            logger.info(f"Circuit closed for {self.config.name}")

    def _record_failure(self, error: str) -> None:
        """Record failed operation."""
        self._error_count += 1
        self._last_error = error
        self._circuit_failures += 1

        if self._circuit_failures >= self.config.circuit_breaker_threshold:
            self._circuit_open = True
            logger.warning(f"Circuit opened for {self.config.name}")


# =============================================================================
# HTTP Adapter
# =============================================================================

class HTTPMethod(str, Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class HTTPAdapterConfig(AdapterConfig):
    """HTTP adapter configuration."""
    base_url: str = ""
    default_method: HTTPMethod = HTTPMethod.POST
    default_headers: Dict[str, str] = field(default_factory=dict)

    # Authentication
    auth_type: str = ""  # basic, bearer, api_key, oauth2
    auth_username: Optional[str] = None
    auth_password: Optional[str] = None
    auth_token: Optional[str] = None
    auth_api_key: Optional[str] = None
    auth_api_key_header: str = "X-API-Key"

    # OAuth2
    oauth2_token_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[str] = None
    oauth2_scopes: List[str] = field(default_factory=list)

    # HTTP/2
    http2_enabled: bool = True

    # Webhook (inbound)
    webhook_path: str = "/webhook"
    webhook_secret: Optional[str] = None


class HTTPAdapter(ProtocolAdapter):
    """
    HTTP/REST protocol adapter.

    Supports:
    - Outbound REST API calls
    - Inbound webhooks
    - Multiple auth methods (Basic, Bearer, API Key, OAuth2)
    - HTTP/2 multiplexing
    """

    def __init__(self, config: HTTPAdapterConfig):
        super().__init__(config)
        self.config: HTTPAdapterConfig = config
        self._client = None
        self._oauth_token: Optional[str] = None
        self._oauth_expires: Optional[datetime] = None

    async def connect(self) -> bool:
        """Initialize HTTP client."""
        try:
            import httpx

            self._status = AdapterStatus.CONNECTING

            # Build SSL context if needed
            ssl_context = None
            if self.config.use_tls:
                ssl_context = ssl.create_default_context()
                if not self.config.tls_verify:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                if self.config.tls_ca_path:
                    ssl_context.load_verify_locations(self.config.tls_ca_path)
                if self.config.tls_cert_path and self.config.tls_key_path:
                    ssl_context.load_cert_chain(
                        self.config.tls_cert_path,
                        self.config.tls_key_path,
                    )

            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=self.config.timeout_seconds,
                http2=self.config.http2_enabled,
                verify=ssl_context if ssl_context else self.config.tls_verify,
            )

            # Get OAuth token if configured
            if self.config.auth_type == "oauth2":
                await self._refresh_oauth_token()

            self._status = AdapterStatus.CONNECTED
            self._connected_at = datetime.utcnow()
            logger.info(f"HTTP adapter connected: {self.config.name}")
            return True

        except Exception as e:
            self._status = AdapterStatus.ERROR
            self._record_failure(str(e))
            logger.error(f"HTTP adapter connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._status = AdapterStatus.DISCONNECTED

    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Send HTTP request."""
        if not self._check_circuit():
            return AdapterResult(success=False, error="Circuit breaker open")

        if not self._client:
            return AdapterResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            # Build headers
            headers = {**self.config.default_headers, **message.headers}
            headers["Content-Type"] = message.content_type

            # Add authentication
            await self._add_auth_headers(headers)

            # Determine method and path
            method = message.metadata.get("method", self.config.default_method)
            path = message.metadata.get("path", "")

            # Build request body
            if isinstance(message.payload, dict):
                import json
                body = json.dumps(message.payload)
            elif isinstance(message.payload, str):
                body = message.payload
            else:
                body = message.payload

            # Send request
            response = await self._client.request(
                method=method if isinstance(method, str) else method.value,
                url=path,
                headers=headers,
                content=body,
            )

            latency = (time.time() - start_time) * 1000

            if response.is_success:
                self._record_success()
                return AdapterResult(
                    success=True,
                    message_id=message.id,
                    response=response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
                    latency_ms=latency,
                    metadata={"status_code": response.status_code},
                )
            else:
                self._record_failure(f"HTTP {response.status_code}")
                return AdapterResult(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                    latency_ms=latency,
                )

        except Exception as e:
            self._record_failure(str(e))
            return AdapterResult(success=False, error=str(e))

    async def receive(self) -> Optional[AdapterMessage]:
        """HTTP adapter doesn't support polling receive. Use webhook handler instead."""
        return None

    async def _add_auth_headers(self, headers: Dict[str, str]) -> None:
        """Add authentication headers."""
        if self.config.auth_type == "basic":
            import base64
            credentials = base64.b64encode(
                f"{self.config.auth_username}:{self.config.auth_password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {credentials}"

        elif self.config.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

        elif self.config.auth_type == "api_key":
            headers[self.config.auth_api_key_header] = self.config.auth_api_key

        elif self.config.auth_type == "oauth2":
            if self._should_refresh_token():
                await self._refresh_oauth_token()
            headers["Authorization"] = f"Bearer {self._oauth_token}"

    def _should_refresh_token(self) -> bool:
        """Check if OAuth token needs refresh."""
        if not self._oauth_token or not self._oauth_expires:
            return True
        return datetime.utcnow() >= self._oauth_expires

    async def _refresh_oauth_token(self) -> None:
        """Refresh OAuth2 token."""
        if not self._client:
            return

        response = await self._client.post(
            self.config.oauth2_token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.oauth2_client_id,
                "client_secret": self.config.oauth2_client_secret,
                "scope": " ".join(self.config.oauth2_scopes),
            },
        )
        response.raise_for_status()
        data = response.json()

        self._oauth_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)
        from datetime import timedelta
        self._oauth_expires = datetime.utcnow() + timedelta(seconds=expires_in - 60)


# =============================================================================
# WebSocket Adapter
# =============================================================================

@dataclass
class WebSocketMessage:
    """WebSocket message types."""
    TEXT = "text"
    BINARY = "binary"
    PING = "ping"
    PONG = "pong"


@dataclass
class WebSocketAdapterConfig(AdapterConfig):
    """WebSocket adapter configuration."""
    url: str = ""
    subprotocols: List[str] = field(default_factory=list)
    headers: Dict[str, str] = field(default_factory=dict)

    # Heartbeat
    ping_interval: float = 30.0
    ping_timeout: float = 10.0

    # Reconnection
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0
    max_reconnect_attempts: int = 10


class WebSocketAdapter(ProtocolAdapter):
    """
    WebSocket protocol adapter.

    Features:
    - Automatic reconnection
    - Ping/pong heartbeat
    - Text and binary messages
    - Subprotocol negotiation
    """

    def __init__(self, config: WebSocketAdapterConfig):
        super().__init__(config)
        self.config: WebSocketAdapterConfig = config
        self._ws = None
        self._receive_queue: asyncio.Queue = asyncio.Queue()
        self._reconnect_attempts = 0

    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        try:
            import websockets

            self._status = AdapterStatus.CONNECTING

            ssl_context = None
            if self.config.use_tls or self.config.url.startswith("wss://"):
                ssl_context = ssl.create_default_context()
                if not self.config.tls_verify:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE

            self._ws = await websockets.connect(
                self.config.url,
                subprotocols=self.config.subprotocols,
                extra_headers=self.config.headers,
                ssl=ssl_context,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
            )

            self._status = AdapterStatus.CONNECTED
            self._connected_at = datetime.utcnow()
            self._reconnect_attempts = 0
            logger.info(f"WebSocket connected: {self.config.name}")

            # Start receive loop
            asyncio.create_task(self._receive_loop())

            return True

        except Exception as e:
            self._status = AdapterStatus.ERROR
            self._record_failure(str(e))
            logger.error(f"WebSocket connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._status = AdapterStatus.DISCONNECTED

    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Send WebSocket message."""
        if not self._check_circuit():
            return AdapterResult(success=False, error="Circuit breaker open")

        if not self._ws:
            return AdapterResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            if isinstance(message.payload, bytes):
                await self._ws.send(message.payload)
            else:
                import json
                payload = message.payload if isinstance(message.payload, str) else json.dumps(message.payload)
                await self._ws.send(payload)

            self._record_success()
            return AdapterResult(
                success=True,
                message_id=message.id,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._record_failure(str(e))
            if self.config.auto_reconnect:
                asyncio.create_task(self._auto_reconnect())
            return AdapterResult(success=False, error=str(e))

    async def receive(self) -> Optional[AdapterMessage]:
        """Receive WebSocket message from queue."""
        try:
            return await asyncio.wait_for(
                self._receive_queue.get(),
                timeout=self.config.timeout_seconds,
            )
        except asyncio.TimeoutError:
            return None

    async def _receive_loop(self) -> None:
        """Background loop to receive messages."""
        import uuid

        while self._ws and self.is_connected:
            try:
                data = await self._ws.recv()

                message = AdapterMessage(
                    id=str(uuid.uuid4()),
                    payload=data,
                    source=self.config.url,
                    content_type="application/octet-stream" if isinstance(data, bytes) else "text/plain",
                )

                await self._receive_queue.put(message)
                self._record_success()

            except Exception as e:
                logger.error(f"WebSocket receive error: {e}")
                self._record_failure(str(e))
                if self.config.auto_reconnect:
                    await self._auto_reconnect()
                break

    async def _auto_reconnect(self) -> None:
        """Attempt automatic reconnection."""
        if self._reconnect_attempts >= self.config.max_reconnect_attempts:
            logger.error(f"Max reconnection attempts reached for {self.config.name}")
            return

        self._status = AdapterStatus.RECONNECTING
        self._reconnect_attempts += 1

        await asyncio.sleep(self.config.reconnect_delay)
        await self.connect()


# =============================================================================
# gRPC Adapter
# =============================================================================

@dataclass
class GRPCAdapterConfig(AdapterConfig):
    """gRPC adapter configuration."""
    host: str = "localhost"
    port: int = 50051
    service_name: str = ""
    method_name: str = ""

    # Proto
    proto_file: Optional[str] = None
    proto_include_dirs: List[str] = field(default_factory=list)

    # Channel options
    max_message_length: int = 4 * 1024 * 1024  # 4MB
    keepalive_time_ms: int = 30000
    keepalive_timeout_ms: int = 10000


class GRPCAdapter(ProtocolAdapter):
    """
    gRPC protocol adapter.

    Features:
    - Unary and streaming calls
    - Automatic retry
    - Channel pooling
    - Reflection support
    """

    def __init__(self, config: GRPCAdapterConfig):
        super().__init__(config)
        self.config: GRPCAdapterConfig = config
        self._channel = None
        self._stub = None

    async def connect(self) -> bool:
        """Establish gRPC channel."""
        try:
            import grpc

            self._status = AdapterStatus.CONNECTING

            target = f"{self.config.host}:{self.config.port}"

            options = [
                ("grpc.max_receive_message_length", self.config.max_message_length),
                ("grpc.max_send_message_length", self.config.max_message_length),
                ("grpc.keepalive_time_ms", self.config.keepalive_time_ms),
                ("grpc.keepalive_timeout_ms", self.config.keepalive_timeout_ms),
            ]

            if self.config.use_tls:
                if self.config.tls_cert_path and self.config.tls_key_path:
                    with open(self.config.tls_cert_path, "rb") as f:
                        cert = f.read()
                    with open(self.config.tls_key_path, "rb") as f:
                        key = f.read()
                    ca = None
                    if self.config.tls_ca_path:
                        with open(self.config.tls_ca_path, "rb") as f:
                            ca = f.read()
                    credentials = grpc.ssl_channel_credentials(ca, key, cert)
                else:
                    credentials = grpc.ssl_channel_credentials()

                self._channel = grpc.aio.secure_channel(target, credentials, options=options)
            else:
                self._channel = grpc.aio.insecure_channel(target, options=options)

            self._status = AdapterStatus.CONNECTED
            self._connected_at = datetime.utcnow()
            logger.info(f"gRPC channel connected: {self.config.name}")
            return True

        except Exception as e:
            self._status = AdapterStatus.ERROR
            self._record_failure(str(e))
            logger.error(f"gRPC connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close gRPC channel."""
        if self._channel:
            await self._channel.close()
            self._channel = None
        self._status = AdapterStatus.DISCONNECTED

    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Send gRPC request."""
        if not self._check_circuit():
            return AdapterResult(success=False, error="Circuit breaker open")

        if not self._channel:
            return AdapterResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            import grpc

            # Dynamic method invocation
            method_name = message.metadata.get("method", self.config.method_name)
            service = message.metadata.get("service", self.config.service_name)

            full_method = f"/{service}/{method_name}"

            # Serialize request
            import json
            request_data = message.payload if isinstance(message.payload, bytes) else json.dumps(message.payload).encode()

            # Make unary call
            response = await self._channel.unary_unary(
                full_method,
                request_serializer=lambda x: x,
                response_deserializer=lambda x: x,
            )(request_data, timeout=self.config.timeout_seconds)

            self._record_success()
            return AdapterResult(
                success=True,
                message_id=message.id,
                response=response,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._record_failure(str(e))
            return AdapterResult(success=False, error=str(e))

    async def receive(self) -> Optional[AdapterMessage]:
        """gRPC uses request/response pattern. Use streaming for continuous receive."""
        return None


# =============================================================================
# SFTP Adapter
# =============================================================================

class SFTPOperation(str, Enum):
    """SFTP operations."""
    LIST = "list"
    GET = "get"
    PUT = "put"
    DELETE = "delete"
    RENAME = "rename"
    MKDIR = "mkdir"


@dataclass
class SFTPAdapterConfig(AdapterConfig):
    """SFTP adapter configuration."""
    host: str = "localhost"
    port: int = 22
    username: str = ""
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None

    # Polling
    poll_directory: str = "/"
    poll_pattern: str = "*"
    poll_interval_seconds: float = 60.0
    delete_after_download: bool = False
    move_after_download: Optional[str] = None

    # Upload
    upload_directory: str = "/"


class SFTPAdapter(ProtocolAdapter):
    """
    SFTP/FTP protocol adapter.

    Features:
    - Directory polling
    - File pattern matching
    - Upload/download
    - Post-processing (delete, move)
    """

    def __init__(self, config: SFTPAdapterConfig):
        super().__init__(config)
        self.config: SFTPAdapterConfig = config
        self._sftp = None
        self._transport = None
        self._processed_files: set = set()

    async def connect(self) -> bool:
        """Establish SFTP connection."""
        try:
            import paramiko

            self._status = AdapterStatus.CONNECTING

            self._transport = paramiko.Transport((self.config.host, self.config.port))

            if self.config.private_key_path:
                key = paramiko.RSAKey.from_private_key_file(
                    self.config.private_key_path,
                    password=self.config.private_key_passphrase,
                )
                self._transport.connect(username=self.config.username, pkey=key)
            else:
                self._transport.connect(
                    username=self.config.username,
                    password=self.config.password,
                )

            self._sftp = paramiko.SFTPClient.from_transport(self._transport)

            self._status = AdapterStatus.CONNECTED
            self._connected_at = datetime.utcnow()
            logger.info(f"SFTP connected: {self.config.name}")
            return True

        except Exception as e:
            self._status = AdapterStatus.ERROR
            self._record_failure(str(e))
            logger.error(f"SFTP connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close SFTP connection."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
        self._status = AdapterStatus.DISCONNECTED

    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Upload file via SFTP."""
        if not self._check_circuit():
            return AdapterResult(success=False, error="Circuit breaker open")

        if not self._sftp:
            return AdapterResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            filename = message.metadata.get("filename", f"{message.id}.dat")
            remote_path = f"{self.config.upload_directory}/{filename}"

            # Write to temp file and upload
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(delete=False) as f:
                if isinstance(message.payload, bytes):
                    f.write(message.payload)
                else:
                    f.write(str(message.payload).encode())
                temp_path = f.name

            self._sftp.put(temp_path, remote_path)
            os.unlink(temp_path)

            self._record_success()
            return AdapterResult(
                success=True,
                message_id=message.id,
                metadata={"remote_path": remote_path},
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._record_failure(str(e))
            return AdapterResult(success=False, error=str(e))

    async def receive(self) -> Optional[AdapterMessage]:
        """Poll for new files."""
        if not self._sftp:
            return None

        try:
            import fnmatch
            import uuid

            files = self._sftp.listdir(self.config.poll_directory)

            for filename in files:
                if not fnmatch.fnmatch(filename, self.config.poll_pattern):
                    continue

                remote_path = f"{self.config.poll_directory}/{filename}"

                if remote_path in self._processed_files:
                    continue

                # Download file
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False) as f:
                    self._sftp.get(remote_path, f.name)
                    with open(f.name, "rb") as rf:
                        content = rf.read()

                # Post-processing
                if self.config.delete_after_download:
                    self._sftp.remove(remote_path)
                elif self.config.move_after_download:
                    new_path = f"{self.config.move_after_download}/{filename}"
                    self._sftp.rename(remote_path, new_path)

                self._processed_files.add(remote_path)
                self._record_success()

                return AdapterMessage(
                    id=str(uuid.uuid4()),
                    payload=content,
                    source=remote_path,
                    metadata={"filename": filename},
                )

        except Exception as e:
            self._record_failure(str(e))

        return None


# =============================================================================
# Email Adapter
# =============================================================================

class EmailProtocol(str, Enum):
    """Email protocols."""
    IMAP = "imap"
    SMTP = "smtp"
    POP3 = "pop3"


@dataclass
class EmailMessage:
    """Email message container."""
    subject: str = ""
    from_addr: str = ""
    to_addrs: List[str] = field(default_factory=list)
    cc_addrs: List[str] = field(default_factory=list)
    body_text: str = ""
    body_html: str = ""
    attachments: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class EmailAdapterConfig(AdapterConfig):
    """Email adapter configuration."""
    # IMAP (receive)
    imap_host: str = ""
    imap_port: int = 993
    imap_use_ssl: bool = True

    # SMTP (send)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_use_tls: bool = True

    # Authentication
    username: str = ""
    password: str = ""

    # Polling
    poll_folder: str = "INBOX"
    poll_interval_seconds: float = 60.0
    mark_as_read: bool = True
    delete_after_read: bool = False


class EmailAdapter(ProtocolAdapter):
    """
    Email protocol adapter (IMAP/SMTP).

    Features:
    - IMAP polling for incoming emails
    - SMTP sending with attachments
    - HTML and plain text support
    - Folder management
    """

    def __init__(self, config: EmailAdapterConfig):
        super().__init__(config)
        self.config: EmailAdapterConfig = config
        self._imap = None
        self._smtp = None
        self._seen_uids: set = set()

    async def connect(self) -> bool:
        """Connect to email servers."""
        try:
            import imaplib
            import smtplib

            self._status = AdapterStatus.CONNECTING

            # Connect IMAP
            if self.config.imap_host:
                if self.config.imap_use_ssl:
                    self._imap = imaplib.IMAP4_SSL(
                        self.config.imap_host,
                        self.config.imap_port,
                    )
                else:
                    self._imap = imaplib.IMAP4(
                        self.config.imap_host,
                        self.config.imap_port,
                    )
                self._imap.login(self.config.username, self.config.password)
                self._imap.select(self.config.poll_folder)

            # Connect SMTP
            if self.config.smtp_host:
                self._smtp = smtplib.SMTP(
                    self.config.smtp_host,
                    self.config.smtp_port,
                )
                if self.config.smtp_use_tls:
                    self._smtp.starttls()
                self._smtp.login(self.config.username, self.config.password)

            self._status = AdapterStatus.CONNECTED
            self._connected_at = datetime.utcnow()
            logger.info(f"Email adapter connected: {self.config.name}")
            return True

        except Exception as e:
            self._status = AdapterStatus.ERROR
            self._record_failure(str(e))
            logger.error(f"Email connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close email connections."""
        if self._imap:
            try:
                self._imap.logout()
            except:
                pass
            self._imap = None

        if self._smtp:
            try:
                self._smtp.quit()
            except:
                pass
            self._smtp = None

        self._status = AdapterStatus.DISCONNECTED

    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Send email via SMTP."""
        if not self._check_circuit():
            return AdapterResult(success=False, error="Circuit breaker open")

        if not self._smtp:
            return AdapterResult(success=False, error="SMTP not connected")

        start_time = time.time()

        try:
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            # Build email
            email_data = message.payload if isinstance(message.payload, dict) else {}

            msg = MIMEMultipart("alternative")
            msg["Subject"] = email_data.get("subject", "")
            msg["From"] = email_data.get("from", self.config.username)
            msg["To"] = ", ".join(email_data.get("to", []))

            if email_data.get("body_text"):
                msg.attach(MIMEText(email_data["body_text"], "plain"))
            if email_data.get("body_html"):
                msg.attach(MIMEText(email_data["body_html"], "html"))

            # Send
            self._smtp.send_message(msg)

            self._record_success()
            return AdapterResult(
                success=True,
                message_id=message.id,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._record_failure(str(e))
            return AdapterResult(success=False, error=str(e))

    async def receive(self) -> Optional[AdapterMessage]:
        """Poll for new emails via IMAP."""
        if not self._imap:
            return None

        try:
            import email
            import uuid

            # Search for unseen messages
            status, data = self._imap.search(None, "UNSEEN")
            if status != "OK":
                return None

            uids = data[0].split()
            for uid in uids:
                if uid in self._seen_uids:
                    continue

                status, msg_data = self._imap.fetch(uid, "(RFC822)")
                if status != "OK":
                    continue

                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                # Extract content
                body_text = ""
                body_html = ""
                attachments = []

                if msg.is_multipart():
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        if content_type == "text/plain":
                            body_text = part.get_payload(decode=True).decode()
                        elif content_type == "text/html":
                            body_html = part.get_payload(decode=True).decode()
                        elif part.get_filename():
                            attachments.append({
                                "filename": part.get_filename(),
                                "content_type": content_type,
                                "data": part.get_payload(decode=True),
                            })
                else:
                    body_text = msg.get_payload(decode=True).decode()

                # Mark as read
                if self.config.mark_as_read:
                    self._imap.store(uid, "+FLAGS", "\\Seen")

                # Delete if configured
                if self.config.delete_after_read:
                    self._imap.store(uid, "+FLAGS", "\\Deleted")
                    self._imap.expunge()

                self._seen_uids.add(uid)
                self._record_success()

                return AdapterMessage(
                    id=str(uuid.uuid4()),
                    payload={
                        "subject": msg["Subject"],
                        "from": msg["From"],
                        "to": msg["To"],
                        "date": msg["Date"],
                        "body_text": body_text,
                        "body_html": body_html,
                        "attachments": [{"filename": a["filename"], "size": len(a["data"])} for a in attachments],
                    },
                    source=msg["From"],
                    content_type="message/rfc822",
                )

        except Exception as e:
            self._record_failure(str(e))

        return None


# =============================================================================
# MLLP Adapter
# =============================================================================

@dataclass
class MLLPAdapterConfig(AdapterConfig):
    """MLLP adapter configuration."""
    host: str = "localhost"
    port: int = 2575

    # Server mode
    server_mode: bool = False
    max_connections: int = 100

    # Message framing
    start_block: bytes = b"\x0b"  # VT
    end_block: bytes = b"\x1c\x0d"  # FS + CR


class MLLPAdapter(ProtocolAdapter):
    """
    MLLP (Minimal Lower Layer Protocol) adapter for HL7 v2.x.

    Features:
    - Client and server modes
    - Automatic ACK/NAK handling
    - Connection pooling
    - HL7 message framing
    """

    def __init__(self, config: MLLPAdapterConfig):
        super().__init__(config)
        self.config: MLLPAdapterConfig = config
        self._reader = None
        self._writer = None
        self._server = None
        self._receive_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> bool:
        """Connect to MLLP endpoint or start server."""
        try:
            self._status = AdapterStatus.CONNECTING

            if self.config.server_mode:
                self._server = await asyncio.start_server(
                    self._handle_client,
                    self.config.host,
                    self.config.port,
                )
                logger.info(f"MLLP server started on {self.config.host}:{self.config.port}")
            else:
                self._reader, self._writer = await asyncio.open_connection(
                    self.config.host,
                    self.config.port,
                )
                logger.info(f"MLLP client connected to {self.config.host}:{self.config.port}")

            self._status = AdapterStatus.CONNECTED
            self._connected_at = datetime.utcnow()
            return True

        except Exception as e:
            self._status = AdapterStatus.ERROR
            self._record_failure(str(e))
            logger.error(f"MLLP connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Close MLLP connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        self._status = AdapterStatus.DISCONNECTED

    async def send(self, message: AdapterMessage) -> AdapterResult:
        """Send HL7 message via MLLP."""
        if not self._check_circuit():
            return AdapterResult(success=False, error="Circuit breaker open")

        if not self._writer:
            return AdapterResult(success=False, error="Not connected")

        start_time = time.time()

        try:
            # Frame the message
            payload = message.payload if isinstance(message.payload, bytes) else str(message.payload).encode()
            framed = self.config.start_block + payload + self.config.end_block

            self._writer.write(framed)
            await self._writer.drain()

            # Wait for ACK
            response = await self._read_mllp_message()

            self._record_success()
            return AdapterResult(
                success=True,
                message_id=message.id,
                response=response.decode() if response else None,
                latency_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._record_failure(str(e))
            return AdapterResult(success=False, error=str(e))

    async def receive(self) -> Optional[AdapterMessage]:
        """Receive MLLP message."""
        try:
            return await asyncio.wait_for(
                self._receive_queue.get(),
                timeout=self.config.timeout_seconds,
            )
        except asyncio.TimeoutError:
            return None

    async def _read_mllp_message(self) -> Optional[bytes]:
        """Read a single MLLP-framed message."""
        if not self._reader:
            return None

        buffer = b""
        while True:
            chunk = await self._reader.read(1024)
            if not chunk:
                break
            buffer += chunk

            # Check for end of message
            end_pos = buffer.find(self.config.end_block)
            if end_pos >= 0:
                # Remove framing
                start_pos = buffer.find(self.config.start_block)
                if start_pos >= 0:
                    return buffer[start_pos + len(self.config.start_block):end_pos]

        return None

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming MLLP client connection."""
        import uuid

        try:
            while True:
                buffer = b""
                while True:
                    chunk = await reader.read(4096)
                    if not chunk:
                        return
                    buffer += chunk

                    end_pos = buffer.find(self.config.end_block)
                    if end_pos >= 0:
                        break

                # Extract message
                start_pos = buffer.find(self.config.start_block)
                if start_pos >= 0:
                    hl7_message = buffer[start_pos + len(self.config.start_block):end_pos]

                    message = AdapterMessage(
                        id=str(uuid.uuid4()),
                        payload=hl7_message,
                        source=f"{writer.get_extra_info('peername')}",
                        content_type="application/hl7-v2",
                    )

                    await self._receive_queue.put(message)
                    self._record_success()

                    # Send ACK
                    ack = self._generate_ack(hl7_message)
                    framed_ack = self.config.start_block + ack + self.config.end_block
                    writer.write(framed_ack)
                    await writer.drain()

        except Exception as e:
            logger.error(f"MLLP client handler error: {e}")
        finally:
            writer.close()

    def _generate_ack(self, message: bytes) -> bytes:
        """Generate HL7 ACK for received message."""
        # Parse MSH to get message control ID
        segments = message.decode().split("\r")
        msh = segments[0] if segments else ""
        fields = msh.split("|")

        control_id = fields[9] if len(fields) > 9 else ""
        sending_app = fields[2] if len(fields) > 2 else ""
        sending_fac = fields[3] if len(fields) > 3 else ""
        receiving_app = fields[4] if len(fields) > 4 else ""
        receiving_fac = fields[5] if len(fields) > 5 else ""

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        ack = (
            f"MSH|^~\\&|{receiving_app}|{receiving_fac}|{sending_app}|{sending_fac}|"
            f"{timestamp}||ACK|{control_id}|P|2.5\r"
            f"MSA|AA|{control_id}\r"
        )

        return ack.encode()


# =============================================================================
# Factory & Registry
# =============================================================================

PROTOCOL_ADAPTERS: Dict[str, type] = {
    "http": HTTPAdapter,
    "https": HTTPAdapter,
    "websocket": WebSocketAdapter,
    "ws": WebSocketAdapter,
    "wss": WebSocketAdapter,
    "grpc": GRPCAdapter,
    "sftp": SFTPAdapter,
    "ftp": SFTPAdapter,
    "email": EmailAdapter,
    "imap": EmailAdapter,
    "smtp": EmailAdapter,
    "mllp": MLLPAdapter,
    "hl7": MLLPAdapter,
}


def get_protocol_adapter(
    protocol: str,
    config: AdapterConfig,
) -> ProtocolAdapter:
    """Get protocol adapter by name."""
    adapter_class = PROTOCOL_ADAPTERS.get(protocol.lower())
    if not adapter_class:
        raise ValueError(f"Unknown protocol: {protocol}")
    return adapter_class(config)

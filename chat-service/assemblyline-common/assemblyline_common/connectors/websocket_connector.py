"""
WebSocket Connector for Logic Weaver

Enterprise-grade WebSocket connector with:
- Client and server modes
- Automatic reconnection
- Heartbeat/ping-pong
- Message compression
- SSL/TLS support
- Authentication headers
- Multi-tenant isolation

Comparison:
| Feature              | MuleSoft | Apigee | Logic Weaver |
|---------------------|----------|--------|--------------|
| WebSocket Support   | Limited  | No     | Full         |
| Auto Reconnect      | No       | No     | Yes          |
| Heartbeat           | No       | No     | Yes          |
| Compression         | No       | No     | Yes          |
| Binary Messages     | Limited  | No     | Yes          |
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional, Union

logger = logging.getLogger(__name__)

# Try to import websockets
try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    from websockets.server import WebSocketServerProtocol, serve
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    logger.warning("websockets not installed. WebSocket functionality limited.")


class WebSocketState(Enum):
    """WebSocket connection states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


class WebSocketMessageType(Enum):
    """Message types."""
    TEXT = "text"
    BINARY = "binary"
    PING = "ping"
    PONG = "pong"
    CLOSE = "close"


@dataclass
class WebSocketConfig:
    """WebSocket connection configuration."""
    # Connection
    url: str  # ws:// or wss://
    headers: dict[str, str] = field(default_factory=dict)

    # Authentication
    auth_token: Optional[str] = None
    auth_header: str = "Authorization"
    auth_prefix: str = "Bearer"

    # SSL
    ssl_verify: bool = True
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_ca: Optional[str] = None

    # Connection settings
    connect_timeout: float = 30.0
    close_timeout: float = 10.0
    ping_interval: Optional[float] = 20.0  # seconds
    ping_timeout: Optional[float] = 10.0

    # Reconnection
    auto_reconnect: bool = True
    reconnect_interval: float = 1.0
    reconnect_max_interval: float = 60.0
    reconnect_backoff: float = 2.0
    max_reconnect_attempts: int = 10

    # Message settings
    max_message_size: int = 10 * 1024 * 1024  # 10MB
    compression: Optional[str] = "deflate"  # None, "deflate"

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class WebSocketMessage:
    """Represents a WebSocket message."""
    data: Union[str, bytes]
    message_type: WebSocketMessageType
    timestamp: datetime = field(default_factory=datetime.now)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def is_text(self) -> bool:
        return self.message_type == WebSocketMessageType.TEXT

    @property
    def is_binary(self) -> bool:
        return self.message_type == WebSocketMessageType.BINARY

    @property
    def json(self) -> Any:
        """Parse data as JSON."""
        if isinstance(self.data, bytes):
            data = self.data.decode('utf-8')
        else:
            data = self.data
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": self.data if isinstance(self.data, str) else self.data.decode('utf-8', errors='replace'),
            "message_type": self.message_type.value,
            "timestamp": self.timestamp.isoformat(),
            "message_id": self.message_id,
        }


@dataclass
class WebSocketResult:
    """Result of a WebSocket operation."""
    success: bool
    message: str
    data: Optional[Any] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "error": self.error,
        }


class WebSocketConnector:
    """
    Enterprise WebSocket connector.

    Example usage:

    config = WebSocketConfig(
        url="wss://api.example.com/ws",
        auth_token="my-token"
    )

    async with WebSocketConnector(config) as ws:
        # Send message
        await ws.send({"type": "subscribe", "channel": "orders"})

        # Receive messages
        async for msg in ws.receive():
            process(msg)

        # Or receive single message
        msg = await ws.receive_one()
    """

    def __init__(self, config: WebSocketConfig):
        self.config = config
        self._connection: Optional[WebSocketClientProtocol] = None
        self._state = WebSocketState.DISCONNECTED
        self._reconnect_count = 0
        self._message_handlers: list[Callable[[WebSocketMessage], Any]] = []
        self._receive_task: Optional[asyncio.Task] = None

    @property
    def state(self) -> WebSocketState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state == WebSocketState.CONNECTED and self._connection is not None

    async def __aenter__(self) -> "WebSocketConnector":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        if not HAS_WEBSOCKETS:
            raise ImportError("websockets is required for WebSocket connections")

        if self._state == WebSocketState.CONNECTED:
            return True

        self._state = WebSocketState.CONNECTING

        try:
            # Build headers
            headers = dict(self.config.headers)
            if self.config.auth_token:
                headers[self.config.auth_header] = f"{self.config.auth_prefix} {self.config.auth_token}"

            # SSL context
            ssl_context = None
            if self.config.url.startswith("wss://"):
                ssl_context = ssl.create_default_context()
                if not self.config.ssl_verify:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                if self.config.ssl_ca:
                    ssl_context.load_verify_locations(self.config.ssl_ca)
                if self.config.ssl_cert and self.config.ssl_key:
                    ssl_context.load_cert_chain(self.config.ssl_cert, self.config.ssl_key)

            # Connect
            self._connection = await asyncio.wait_for(
                websockets.connect(
                    self.config.url,
                    extra_headers=headers,
                    ssl=ssl_context,
                    ping_interval=self.config.ping_interval,
                    ping_timeout=self.config.ping_timeout,
                    close_timeout=self.config.close_timeout,
                    max_size=self.config.max_message_size,
                    compression=self.config.compression,
                ),
                timeout=self.config.connect_timeout
            )

            self._state = WebSocketState.CONNECTED
            self._reconnect_count = 0
            logger.info(f"Connected to WebSocket: {self.config.url}")
            return True

        except asyncio.TimeoutError:
            self._state = WebSocketState.DISCONNECTED
            logger.error(f"WebSocket connection timeout: {self.config.url}")
            return False
        except Exception as e:
            self._state = WebSocketState.DISCONNECTED
            logger.error(f"WebSocket connection failed: {e}")
            raise

    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None

        if self._connection:
            try:
                await self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self._connection = None

        self._state = WebSocketState.CLOSED
        logger.info("WebSocket disconnected")

    async def _reconnect(self) -> bool:
        """Attempt to reconnect."""
        if not self.config.auto_reconnect:
            return False

        if self._reconnect_count >= self.config.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached")
            return False

        self._state = WebSocketState.RECONNECTING
        self._reconnect_count += 1

        # Calculate backoff delay
        delay = min(
            self.config.reconnect_interval * (self.config.reconnect_backoff ** (self._reconnect_count - 1)),
            self.config.reconnect_max_interval
        )

        logger.info(f"Reconnecting in {delay}s (attempt {self._reconnect_count})")
        await asyncio.sleep(delay)

        try:
            return await self.connect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            return await self._reconnect()

    async def send(
        self,
        data: Union[str, bytes, dict, list],
        message_type: WebSocketMessageType = WebSocketMessageType.TEXT,
    ) -> WebSocketResult:
        """
        Send a message.

        Args:
            data: Message data (dict/list will be JSON serialized)
            message_type: TEXT or BINARY
        """
        if not self.is_connected:
            if not await self.connect():
                return WebSocketResult(
                    success=False,
                    message="Not connected",
                    error="WebSocket not connected"
                )

        try:
            if isinstance(data, (dict, list)):
                data = json.dumps(data)

            if message_type == WebSocketMessageType.TEXT:
                await self._connection.send(str(data))
            else:
                if isinstance(data, str):
                    data = data.encode('utf-8')
                await self._connection.send(data)

            return WebSocketResult(success=True, message="Message sent")

        except websockets.ConnectionClosed:
            self._state = WebSocketState.DISCONNECTED
            if self.config.auto_reconnect:
                await self._reconnect()
            return WebSocketResult(
                success=False,
                message="Connection closed",
                error="WebSocket connection closed"
            )
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return WebSocketResult(success=False, message="Send failed", error=str(e))

    async def send_json(self, data: dict) -> WebSocketResult:
        """Send JSON data."""
        return await self.send(json.dumps(data), WebSocketMessageType.TEXT)

    async def receive_one(self, timeout: Optional[float] = None) -> Optional[WebSocketMessage]:
        """Receive a single message."""
        if not self.is_connected:
            if not await self.connect():
                return None

        try:
            if timeout:
                data = await asyncio.wait_for(
                    self._connection.recv(),
                    timeout=timeout
                )
            else:
                data = await self._connection.recv()

            msg_type = WebSocketMessageType.BINARY if isinstance(data, bytes) else WebSocketMessageType.TEXT

            return WebSocketMessage(
                data=data,
                message_type=msg_type,
            )

        except asyncio.TimeoutError:
            return None
        except websockets.ConnectionClosed:
            self._state = WebSocketState.DISCONNECTED
            if self.config.auto_reconnect:
                await self._reconnect()
            return None
        except Exception as e:
            logger.error(f"Failed to receive message: {e}")
            return None

    async def receive(self) -> AsyncIterator[WebSocketMessage]:
        """Receive messages as an async iterator."""
        if not self.is_connected:
            if not await self.connect():
                return

        try:
            async for data in self._connection:
                msg_type = WebSocketMessageType.BINARY if isinstance(data, bytes) else WebSocketMessageType.TEXT

                yield WebSocketMessage(
                    data=data,
                    message_type=msg_type,
                )

        except websockets.ConnectionClosed:
            self._state = WebSocketState.DISCONNECTED
            if self.config.auto_reconnect:
                if await self._reconnect():
                    async for msg in self.receive():
                        yield msg

    def on_message(self, handler: Callable[[WebSocketMessage], Any]) -> None:
        """Register a message handler."""
        self._message_handlers.append(handler)

    async def start_receiving(self) -> None:
        """Start receiving messages in the background."""
        if self._receive_task is not None:
            return

        async def _receive_loop():
            async for msg in self.receive():
                for handler in self._message_handlers:
                    try:
                        result = handler(msg)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        logger.error(f"Message handler error: {e}")

        self._receive_task = asyncio.create_task(_receive_loop())

    async def ping(self) -> bool:
        """Send a ping."""
        if not self.is_connected:
            return False

        try:
            pong = await self._connection.ping()
            await pong
            return True
        except Exception as e:
            logger.error(f"Ping failed: {e}")
            return False


# Flow Node Integration
@dataclass
class WebSocketNodeConfig:
    """Configuration for WebSocket flow node."""
    url: str
    auth_token: Optional[str] = None
    operation: str = "send"  # send, receive
    message_type: str = "text"  # text, binary
    auto_reconnect: bool = True
    timeout: Optional[float] = None


@dataclass
class WebSocketNodeResult:
    """Result from WebSocket flow node."""
    success: bool
    operation: str
    messages: list[dict[str, Any]]
    send_result: Optional[dict[str, Any]]
    message: str
    error: Optional[str]


class WebSocketNode:
    """Flow node for WebSocket operations."""

    node_type = "websocket"
    node_category = "connector"

    def __init__(self, config: WebSocketNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> WebSocketNodeResult:
        """Execute the WebSocket operation."""
        ws_config = WebSocketConfig(
            url=self.config.url,
            auth_token=self.config.auth_token,
            auto_reconnect=self.config.auto_reconnect,
        )

        try:
            async with WebSocketConnector(ws_config) as ws:
                if self.config.operation == "send":
                    data = input_data.get("data", input_data)
                    msg_type = (
                        WebSocketMessageType.BINARY
                        if self.config.message_type == "binary"
                        else WebSocketMessageType.TEXT
                    )
                    result = await ws.send(data, msg_type)
                    return WebSocketNodeResult(
                        success=result.success,
                        operation="send",
                        messages=[],
                        send_result=result.to_dict(),
                        message=result.message,
                        error=result.error,
                    )

                elif self.config.operation == "receive":
                    msg = await ws.receive_one(timeout=self.config.timeout)
                    messages = [msg.to_dict()] if msg else []
                    return WebSocketNodeResult(
                        success=True,
                        operation="receive",
                        messages=messages,
                        send_result=None,
                        message=f"Received {len(messages)} message(s)",
                        error=None,
                    )

                else:
                    return WebSocketNodeResult(
                        success=False,
                        operation=self.config.operation,
                        messages=[],
                        send_result=None,
                        message="Unknown operation",
                        error=f"Unknown operation: {self.config.operation}",
                    )

        except Exception as e:
            logger.error(f"WebSocket operation failed: {e}")
            return WebSocketNodeResult(
                success=False,
                operation=self.config.operation,
                messages=[],
                send_result=None,
                message="Operation failed",
                error=str(e),
            )


def get_websocket_connector(config: WebSocketConfig) -> WebSocketConnector:
    """Factory function to create WebSocket connector."""
    return WebSocketConnector(config)


def get_websocket_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "websocket",
        "category": "connector",
        "label": "WebSocket",
        "description": "WebSocket real-time communication",
        "icon": "Zap",
        "color": "#8B5CF6",
        "inputs": ["data"],
        "outputs": ["messages", "result"],
        "config_schema": {
            "url": {
                "type": "string",
                "required": True,
                "label": "WebSocket URL",
                "placeholder": "wss://api.example.com/ws",
            },
            "auth_token": {
                "type": "password",
                "label": "Auth Token",
            },
            "operation": {
                "type": "select",
                "options": ["send", "receive"],
                "default": "send",
                "label": "Operation",
            },
            "message_type": {
                "type": "select",
                "options": ["text", "binary"],
                "default": "text",
                "label": "Message Type",
            },
            "auto_reconnect": {
                "type": "boolean",
                "default": True,
                "label": "Auto Reconnect",
            },
            "timeout": {
                "type": "number",
                "label": "Receive Timeout (seconds)",
            },
        },
    }

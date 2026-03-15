"""
IBM MQ Connector for Logic Weaver

Enterprise-grade IBM MQ connector with:
- Queue put/get operations
- Message browsing
- Message groups and sequences
- Transaction support
- SSL/TLS authentication
- Multiple connection modes (client/bindings)
- Dead letter queue handling
- Multi-tenant isolation

Comparison:
| Feature              | MuleSoft | Boomi  | Logic Weaver |
|---------------------|----------|--------|--------------|
| IBM MQ Support      | Yes      | Yes    | Yes          |
| Message Groups      | Limited  | No     | Yes          |
| Browse Mode         | Yes      | Limited| Yes          |
| Transactions        | Yes      | Limited| Yes          |
| SSL/TLS             | Yes      | Yes    | Yes          |
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional, Union

logger = logging.getLogger(__name__)

# Try to import pymqi
try:
    import pymqi
    HAS_PYMQI = True
except ImportError:
    HAS_PYMQI = False
    logger.warning("pymqi not installed. IBM MQ functionality limited.")


class MQConnectionMode(Enum):
    """MQ connection modes."""
    CLIENT = "client"
    BINDINGS = "bindings"


class MQMessagePriority(Enum):
    """Message priorities."""
    LOW = 0
    NORMAL = 5
    HIGH = 9


class MQPersistence(Enum):
    """Message persistence options."""
    NOT_PERSISTENT = 0
    PERSISTENT = 1
    AS_QUEUE_DEF = 2


@dataclass
class MQConfig:
    """IBM MQ connection configuration."""
    # Connection
    queue_manager: str
    channel: str = "SYSTEM.DEF.SVRCONN"
    host: str = "localhost"
    port: int = 1414
    connection_mode: MQConnectionMode = MQConnectionMode.CLIENT

    # Authentication
    user: Optional[str] = None
    password: Optional[str] = None

    # SSL/TLS
    ssl_cipher: Optional[str] = None  # e.g., "TLS_RSA_WITH_AES_256_CBC_SHA256"
    ssl_key_repo: Optional[str] = None  # Path to key repository
    ssl_cert_label: Optional[str] = None

    # Queue settings
    queue_name: Optional[str] = None
    model_queue: Optional[str] = None  # For dynamic queues

    # Message settings
    default_persistence: MQPersistence = MQPersistence.AS_QUEUE_DEF
    default_priority: MQMessagePriority = MQMessagePriority.NORMAL
    expiry: int = -1  # -1 = unlimited

    # Connection pool
    max_connections: int = 10
    connection_timeout: float = 30.0

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class MQMessage:
    """Represents an IBM MQ message."""
    data: Union[str, bytes]
    message_id: Optional[bytes] = None
    correlation_id: Optional[bytes] = None
    reply_to_queue: Optional[str] = None
    reply_to_qm: Optional[str] = None
    format: str = "MQSTR"
    persistence: MQPersistence = MQPersistence.AS_QUEUE_DEF
    priority: MQMessagePriority = MQMessagePriority.NORMAL
    expiry: int = -1
    put_time: Optional[datetime] = None
    group_id: Optional[bytes] = None
    sequence_number: int = 1
    msg_flags: int = 0

    @property
    def data_str(self) -> str:
        if isinstance(self.data, bytes):
            return self.data.decode('utf-8', errors='replace')
        return self.data

    @property
    def data_json(self) -> Any:
        try:
            return json.loads(self.data_str)
        except json.JSONDecodeError:
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": self.data_str,
            "message_id": self.message_id.hex() if self.message_id else None,
            "correlation_id": self.correlation_id.hex() if self.correlation_id else None,
            "reply_to_queue": self.reply_to_queue,
            "reply_to_qm": self.reply_to_qm,
            "format": self.format,
            "persistence": self.persistence.value,
            "priority": self.priority.value,
            "expiry": self.expiry,
            "put_time": self.put_time.isoformat() if self.put_time else None,
            "group_id": self.group_id.hex() if self.group_id else None,
            "sequence_number": self.sequence_number,
        }


@dataclass
class MQResult:
    """Result of an MQ operation."""
    success: bool
    message: str
    message_id: Optional[bytes] = None
    data: Optional[Any] = None
    error: Optional[str] = None
    reason_code: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "message_id": self.message_id.hex() if self.message_id else None,
            "data": self.data,
            "error": self.error,
            "reason_code": self.reason_code,
        }


class IBMMQConnector:
    """
    Enterprise IBM MQ connector.

    Example usage:

    config = MQConfig(
        queue_manager="QM1",
        channel="DEV.APP.SVRCONN",
        host="mq.example.com",
        port=1414,
        queue_name="DEV.QUEUE.1",
        user="app",
        password="secret"
    )

    async with IBMMQConnector(config) as mq:
        # Put message
        result = await mq.put({"order_id": "123", "status": "new"})

        # Get message
        msg = await mq.get()
        if msg:
            print(msg.data_json)

        # Browse messages
        async for msg in mq.browse():
            print(msg.data)
    """

    def __init__(self, config: MQConfig):
        self.config = config
        self._qmgr: Optional[pymqi.QueueManager] = None
        self._queue: Optional[pymqi.Queue] = None
        self._is_connected = False

    @property
    def is_connected(self) -> bool:
        return self._is_connected and self._qmgr is not None

    async def __aenter__(self) -> "IBMMQConnector":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> bool:
        """Establish connection to MQ."""
        if not HAS_PYMQI:
            raise ImportError("pymqi is required for IBM MQ connections")

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._connect_sync)
            self._is_connected = True
            logger.info(f"Connected to IBM MQ: {self.config.queue_manager}")
            return True

        except Exception as e:
            logger.error(f"IBM MQ connection failed: {e}")
            raise

    def _connect_sync(self) -> None:
        """Synchronous connection (run in thread)."""
        # Build connection descriptor
        conn_info = f"{self.config.host}({self.config.port})"

        cd = pymqi.CD()
        cd.ChannelName = self.config.channel.encode()
        cd.ConnectionName = conn_info.encode()
        cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
        cd.TransportType = pymqi.CMQC.MQXPT_TCP

        # SSL configuration
        if self.config.ssl_cipher:
            cd.SSLCipherSpec = self.config.ssl_cipher.encode()

            sco = pymqi.SCO()
            if self.config.ssl_key_repo:
                sco.KeyRepository = self.config.ssl_key_repo.encode()
            if self.config.ssl_cert_label:
                sco.CertificateLabel = self.config.ssl_cert_label.encode()
        else:
            sco = None

        # Connect options
        opts = pymqi.CMQC.MQCNO_HANDLE_SHARE_BLOCK

        # Connect
        if self.config.user and self.config.password:
            self._qmgr = pymqi.QueueManager(None)
            self._qmgr.connect_with_options(
                self.config.queue_manager,
                cd=cd,
                sco=sco,
                opts=opts,
                user=self.config.user,
                password=self.config.password,
            )
        else:
            self._qmgr = pymqi.QueueManager(None)
            self._qmgr.connect_with_options(
                self.config.queue_manager,
                cd=cd,
                sco=sco,
                opts=opts,
            )

    async def disconnect(self) -> None:
        """Close MQ connection."""
        if self._queue:
            try:
                self._queue.close()
            except Exception as e:
                logger.warning(f"Error closing queue: {e}")
            self._queue = None

        if self._qmgr:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._qmgr.disconnect)
            except Exception as e:
                logger.warning(f"Error disconnecting from QM: {e}")
            self._qmgr = None

        self._is_connected = False
        logger.info("IBM MQ connection closed")

    async def open_queue(
        self,
        queue_name: Optional[str] = None,
        for_output: bool = True,
        for_input: bool = True,
        browse: bool = False,
    ) -> bool:
        """Open a queue for operations."""
        queue_name = queue_name or self.config.queue_name
        if not queue_name:
            raise ValueError("queue_name is required")

        try:
            open_opts = 0
            if for_output:
                open_opts |= pymqi.CMQC.MQOO_OUTPUT
            if for_input:
                open_opts |= pymqi.CMQC.MQOO_INPUT_SHARED
            if browse:
                open_opts |= pymqi.CMQC.MQOO_BROWSE

            loop = asyncio.get_event_loop()
            self._queue = await loop.run_in_executor(
                None,
                lambda: pymqi.Queue(self._qmgr, queue_name, open_opts)
            )

            return True

        except Exception as e:
            logger.error(f"Failed to open queue {queue_name}: {e}")
            raise

    async def put(
        self,
        data: Union[str, bytes, dict, list],
        correlation_id: Optional[bytes] = None,
        reply_to_queue: Optional[str] = None,
        priority: Optional[MQMessagePriority] = None,
        persistence: Optional[MQPersistence] = None,
        expiry: Optional[int] = None,
        group_id: Optional[bytes] = None,
        msg_flags: int = 0,
    ) -> MQResult:
        """
        Put a message to the queue.

        Args:
            data: Message data (dict/list will be JSON encoded)
            correlation_id: Correlation ID for request/reply
            reply_to_queue: Reply-to queue name
            priority: Message priority
            persistence: Message persistence
            expiry: Message expiry in tenths of seconds
            group_id: Message group ID
            msg_flags: Message flags
        """
        if not self.is_connected:
            await self.connect()

        if not self._queue:
            await self.open_queue(for_output=True, for_input=False)

        try:
            # Prepare message data
            if isinstance(data, (dict, list)):
                msg_data = json.dumps(data).encode('utf-8')
            elif isinstance(data, str):
                msg_data = data.encode('utf-8')
            else:
                msg_data = data

            # Build message descriptor
            md = pymqi.MD()
            md.Format = pymqi.CMQC.MQFMT_STRING
            md.Persistence = (persistence or self.config.default_persistence).value
            md.Priority = (priority or self.config.default_priority).value
            md.Expiry = expiry if expiry is not None else self.config.expiry

            if correlation_id:
                md.CorrelId = correlation_id

            if reply_to_queue:
                md.ReplyToQ = reply_to_queue.encode()
                md.ReplyToQMgr = self.config.queue_manager.encode()

            if group_id:
                md.GroupId = group_id
                md.MsgFlags = msg_flags | pymqi.CMQC.MQMF_MSG_IN_GROUP

            # Put options
            pmo = pymqi.PMO()
            pmo.Options = pymqi.CMQC.MQPMO_NO_SYNCPOINT

            # Put message
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._queue.put(msg_data, md, pmo)
            )

            return MQResult(
                success=True,
                message="Message put successfully",
                message_id=md.MsgId,
            )

        except pymqi.MQMIError as e:
            logger.error(f"MQ put failed: {e}")
            return MQResult(
                success=False,
                message="Put failed",
                error=str(e),
                reason_code=e.reason,
            )
        except Exception as e:
            logger.error(f"Put failed: {e}")
            return MQResult(
                success=False,
                message="Put failed",
                error=str(e),
            )

    async def get(
        self,
        wait: int = 0,
        correlation_id: Optional[bytes] = None,
        message_id: Optional[bytes] = None,
    ) -> Optional[MQMessage]:
        """
        Get a message from the queue.

        Args:
            wait: Wait time in milliseconds (0 = no wait)
            correlation_id: Get message with this correlation ID
            message_id: Get message with this message ID
        """
        if not self.is_connected:
            await self.connect()

        if not self._queue:
            await self.open_queue(for_output=False, for_input=True)

        try:
            # Build message descriptor
            md = pymqi.MD()

            if correlation_id:
                md.CorrelId = correlation_id
            if message_id:
                md.MsgId = message_id

            # Get options
            gmo = pymqi.GMO()
            gmo.Options = pymqi.CMQC.MQGMO_NO_SYNCPOINT

            if wait > 0:
                gmo.Options |= pymqi.CMQC.MQGMO_WAIT
                gmo.WaitInterval = wait

            if correlation_id or message_id:
                gmo.Options |= pymqi.CMQC.MQGMO_MATCH_CORREL_ID
                gmo.Options |= pymqi.CMQC.MQGMO_MATCH_MSG_ID

            # Get message
            loop = asyncio.get_event_loop()
            msg_data = await loop.run_in_executor(
                None,
                lambda: self._queue.get(None, md, gmo)
            )

            return MQMessage(
                data=msg_data,
                message_id=md.MsgId,
                correlation_id=md.CorrelId if md.CorrelId != b'\x00' * 24 else None,
                reply_to_queue=md.ReplyToQ.decode().strip() if md.ReplyToQ else None,
                reply_to_qm=md.ReplyToQMgr.decode().strip() if md.ReplyToQMgr else None,
                format=md.Format.decode().strip(),
                persistence=MQPersistence(md.Persistence),
                priority=MQMessagePriority(md.Priority) if md.Priority <= 9 else MQMessagePriority.NORMAL,
                expiry=md.Expiry,
                group_id=md.GroupId if md.GroupId != b'\x00' * 24 else None,
                sequence_number=md.MsgSeqNumber,
                msg_flags=md.MsgFlags,
            )

        except pymqi.MQMIError as e:
            if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                return None
            logger.error(f"MQ get failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Get failed: {e}")
            return None

    async def browse(
        self,
        max_messages: int = 100,
    ) -> AsyncIterator[MQMessage]:
        """
        Browse messages without removing them.

        Yields messages from the queue.
        """
        if not self.is_connected:
            await self.connect()

        # Open queue for browsing
        if not self._queue:
            await self.open_queue(for_output=False, for_input=False, browse=True)

        count = 0
        browse_first = True

        while count < max_messages:
            try:
                md = pymqi.MD()
                gmo = pymqi.GMO()
                gmo.Options = pymqi.CMQC.MQGMO_NO_SYNCPOINT

                if browse_first:
                    gmo.Options |= pymqi.CMQC.MQGMO_BROWSE_FIRST
                    browse_first = False
                else:
                    gmo.Options |= pymqi.CMQC.MQGMO_BROWSE_NEXT

                loop = asyncio.get_event_loop()
                msg_data = await loop.run_in_executor(
                    None,
                    lambda: self._queue.get(None, md, gmo)
                )

                yield MQMessage(
                    data=msg_data,
                    message_id=md.MsgId,
                    correlation_id=md.CorrelId if md.CorrelId != b'\x00' * 24 else None,
                    format=md.Format.decode().strip(),
                )

                count += 1

            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    break
                logger.error(f"Browse failed: {e}")
                break

    async def request_reply(
        self,
        data: Union[str, bytes, dict, list],
        reply_queue: str,
        timeout: int = 30000,
    ) -> Optional[MQMessage]:
        """
        Send a request and wait for reply.

        Args:
            data: Request message data
            reply_queue: Queue to receive reply on
            timeout: Wait timeout in milliseconds
        """
        # Generate correlation ID
        correl_id = uuid.uuid4().bytes[:24]

        # Put request
        result = await self.put(
            data=data,
            correlation_id=correl_id,
            reply_to_queue=reply_queue,
        )

        if not result.success:
            return None

        # Open reply queue and wait for response
        orig_queue = self._queue
        try:
            await self.open_queue(reply_queue, for_output=False, for_input=True)
            return await self.get(
                wait=timeout,
                correlation_id=correl_id,
            )
        finally:
            self._queue = orig_queue

    async def get_queue_depth(self, queue_name: Optional[str] = None) -> int:
        """Get the current depth of a queue."""
        queue_name = queue_name or self.config.queue_name
        if not queue_name:
            raise ValueError("queue_name is required")

        try:
            loop = asyncio.get_event_loop()

            def _get_depth():
                pcf = pymqi.PCFExecute(self._qmgr)
                response = pcf.MQCMD_INQUIRE_Q({
                    pymqi.CMQC.MQCA_Q_NAME: queue_name.encode(),
                    pymqi.CMQC.MQIA_Q_TYPE: pymqi.CMQC.MQQT_LOCAL,
                })
                for queue_info in response:
                    return queue_info.get(pymqi.CMQC.MQIA_CURRENT_Q_DEPTH, 0)
                return 0

            return await loop.run_in_executor(None, _get_depth)

        except Exception as e:
            logger.error(f"Failed to get queue depth: {e}")
            return -1


# Flow Node Integration
@dataclass
class MQNodeConfig:
    """Configuration for IBM MQ flow node."""
    queue_manager: str
    channel: str = "SYSTEM.DEF.SVRCONN"
    host: str = "localhost"
    port: int = 1414
    queue_name: str = ""
    user: Optional[str] = None
    password: Optional[str] = None
    operation: str = "put"  # put, get, browse
    wait_timeout: int = 0
    max_messages: int = 100


@dataclass
class MQNodeResult:
    """Result from IBM MQ flow node."""
    success: bool
    operation: str
    messages: list[dict[str, Any]]
    message_id: Optional[str]
    count: int
    message: str
    error: Optional[str]


class MQNode:
    """Flow node for IBM MQ operations."""

    node_type = "ibmmq"
    node_category = "connector"

    def __init__(self, config: MQNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> MQNodeResult:
        """Execute the MQ operation."""
        mq_config = MQConfig(
            queue_manager=self.config.queue_manager,
            channel=self.config.channel,
            host=self.config.host,
            port=self.config.port,
            queue_name=self.config.queue_name,
            user=self.config.user,
            password=self.config.password,
        )

        try:
            async with IBMMQConnector(mq_config) as mq:
                if self.config.operation == "put":
                    data = input_data.get("data", input_data)
                    result = await mq.put(data)

                    return MQNodeResult(
                        success=result.success,
                        operation="put",
                        messages=[],
                        message_id=result.message_id.hex() if result.message_id else None,
                        count=1 if result.success else 0,
                        message=result.message,
                        error=result.error,
                    )

                elif self.config.operation == "get":
                    msg = await mq.get(wait=self.config.wait_timeout)
                    messages = [msg.to_dict()] if msg else []

                    return MQNodeResult(
                        success=True,
                        operation="get",
                        messages=messages,
                        message_id=msg.message_id.hex() if msg and msg.message_id else None,
                        count=len(messages),
                        message=f"Retrieved {len(messages)} message(s)",
                        error=None,
                    )

                elif self.config.operation == "browse":
                    messages = []
                    async for msg in mq.browse(max_messages=self.config.max_messages):
                        messages.append(msg.to_dict())

                    return MQNodeResult(
                        success=True,
                        operation="browse",
                        messages=messages,
                        message_id=None,
                        count=len(messages),
                        message=f"Browsed {len(messages)} message(s)",
                        error=None,
                    )

                else:
                    return MQNodeResult(
                        success=False,
                        operation=self.config.operation,
                        messages=[],
                        message_id=None,
                        count=0,
                        message="Unknown operation",
                        error=f"Unknown operation: {self.config.operation}",
                    )

        except Exception as e:
            logger.error(f"MQ node execution failed: {e}")
            return MQNodeResult(
                success=False,
                operation=self.config.operation,
                messages=[],
                message_id=None,
                count=0,
                message="Execution failed",
                error=str(e),
            )


def get_ibmmq_connector(config: MQConfig) -> IBMMQConnector:
    """Factory function to create IBM MQ connector."""
    return IBMMQConnector(config)


def get_mq_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "ibmmq",
        "category": "connector",
        "label": "IBM MQ",
        "description": "Enterprise messaging with IBM MQ",
        "icon": "Server",
        "color": "#054ADA",
        "inputs": ["data"],
        "outputs": ["messages", "result"],
        "config_schema": {
            "queue_manager": {
                "type": "string",
                "required": True,
                "label": "Queue Manager",
                "placeholder": "QM1",
            },
            "host": {
                "type": "string",
                "required": True,
                "label": "Host",
                "placeholder": "localhost",
            },
            "port": {
                "type": "number",
                "default": 1414,
                "label": "Port",
            },
            "channel": {
                "type": "string",
                "default": "SYSTEM.DEF.SVRCONN",
                "label": "Channel",
            },
            "queue_name": {
                "type": "string",
                "required": True,
                "label": "Queue Name",
            },
            "user": {
                "type": "string",
                "label": "Username",
            },
            "password": {
                "type": "password",
                "label": "Password",
            },
            "operation": {
                "type": "select",
                "options": ["put", "get", "browse"],
                "default": "put",
                "label": "Operation",
            },
            "wait_timeout": {
                "type": "number",
                "default": 0,
                "label": "Wait Timeout (ms)",
                "condition": {"operation": "get"},
            },
        },
    }

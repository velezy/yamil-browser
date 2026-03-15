"""
MongoDB CDC (Change Data Capture) Connector for Logic Weaver

Enterprise-grade MongoDB CDC connector with:
- Change streams API
- Insert, Update, Delete, Replace tracking
- Resume token support
- Full document lookup
- Pipeline filtering
- Multi-tenant isolation

Comparison:
| Feature              | Debezium | Mongo Compass | Logic Weaver |
|---------------------|----------|---------------|--------------|
| Change Streams      | Yes      | Yes           | Yes          |
| Resume Token        | Yes      | No            | Yes          |
| Pipeline Filter     | Limited  | No            | Yes          |
| Multi-tenant        | No       | No            | Yes          |
| Real-time           | Yes      | Yes           | Yes          |
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

# Try to import motor (async MongoDB)
try:
    from motor.motor_asyncio import AsyncIOMotorClient
    from bson import ObjectId
    from bson.json_util import dumps as bson_dumps
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False
    logger.warning("motor not installed. MongoDB CDC limited.")


class MongoDBCDCOperation(Enum):
    """CDC operation types."""
    INSERT = "insert"
    UPDATE = "update"
    REPLACE = "replace"
    DELETE = "delete"
    DROP = "drop"
    RENAME = "rename"
    DROP_DATABASE = "dropDatabase"
    INVALIDATE = "invalidate"


@dataclass
class MongoDBCDCConfig:
    """MongoDB CDC configuration."""
    # Connection
    uri: str = "mongodb://localhost:27017"
    database: Optional[str] = None  # None = watch all databases
    collection: Optional[str] = None  # None = watch all collections

    # Authentication
    username: Optional[str] = None
    password: Optional[str] = None
    auth_source: str = "admin"

    # Change stream options
    full_document: str = "updateLookup"  # "default", "updateLookup", "whenAvailable", "required"
    full_document_before_change: Optional[str] = None  # "off", "whenAvailable", "required"
    resume_token: Optional[str] = None
    start_at_operation_time: Optional[datetime] = None

    # Pipeline filter
    pipeline: list[dict] = field(default_factory=list)

    # Options
    batch_size: int = 100
    max_await_time_ms: int = 1000

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class MongoDBCDCChange:
    """Represents a MongoDB change event."""
    operation: MongoDBCDCOperation
    database: str
    collection: str
    document_key: Optional[dict[str, Any]] = None
    full_document: Optional[dict[str, Any]] = None
    full_document_before_change: Optional[dict[str, Any]] = None
    update_description: Optional[dict[str, Any]] = None
    cluster_time: Optional[datetime] = None
    resume_token: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    change_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        result = {
            "operation": self.operation.value,
            "database": self.database,
            "collection": self.collection,
            "document_key": self._serialize(self.document_key),
            "full_document": self._serialize(self.full_document),
            "resume_token": self.resume_token,
            "timestamp": self.timestamp.isoformat(),
            "change_id": self.change_id,
        }

        if self.full_document_before_change:
            result["full_document_before_change"] = self._serialize(
                self.full_document_before_change
            )

        if self.update_description:
            result["update_description"] = self._serialize(self.update_description)

        return result

    def _serialize(self, obj: Any) -> Any:
        """Serialize MongoDB objects to JSON-compatible format."""
        if obj is None:
            return None
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, dict):
            return {k: self._serialize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._serialize(v) for v in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj


@dataclass
class MongoDBCDCResult:
    """Result of a CDC operation."""
    success: bool
    message: str
    changes: list[MongoDBCDCChange] = field(default_factory=list)
    last_resume_token: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "changes": [c.to_dict() for c in self.changes],
            "last_resume_token": self.last_resume_token,
            "error": self.error,
        }


class MongoDBCDCConnector:
    """
    Enterprise MongoDB CDC connector.

    Example usage:

    config = MongoDBCDCConfig(
        uri="mongodb://localhost:27017",
        database="mydb",
        collection="orders"
    )

    async with MongoDBCDCConnector(config) as cdc:
        # Stream changes
        async for change in cdc.stream_changes():
            print(f"{change.operation} on {change.collection}")
            print(f"Document: {change.full_document}")

        # Or poll for changes
        changes = await cdc.poll_changes(limit=100)
    """

    def __init__(self, config: MongoDBCDCConfig):
        self.config = config
        self._client: Optional[AsyncIOMotorClient] = None
        self._change_stream = None
        self._is_streaming = False
        self._last_resume_token: Optional[str] = None
        self._handlers: list[Callable[[MongoDBCDCChange], Any]] = []

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    async def __aenter__(self) -> "MongoDBCDCConnector":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> bool:
        """Connect to MongoDB."""
        if not HAS_MOTOR:
            raise ImportError("motor is required for MongoDB CDC")

        try:
            # Build connection URI with auth
            uri = self.config.uri
            if self.config.username and self.config.password:
                # Insert credentials into URI
                if "://" in uri:
                    proto, rest = uri.split("://", 1)
                    uri = f"{proto}://{self.config.username}:{self.config.password}@{rest}"
                    if "?" in uri:
                        uri = uri.replace("?", f"?authSource={self.config.auth_source}&")
                    else:
                        uri = f"{uri}?authSource={self.config.auth_source}"

            self._client = AsyncIOMotorClient(uri)

            # Verify connection
            await self._client.admin.command("ping")

            logger.info(f"Connected to MongoDB: {self.config.uri}")
            return True

        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise

    async def disconnect(self) -> None:
        """Close MongoDB connection."""
        self._is_streaming = False

        if self._change_stream:
            await self._change_stream.close()
            self._change_stream = None

        if self._client:
            self._client.close()
            self._client = None

        logger.info("MongoDB CDC disconnected")

    async def stream_changes(self) -> AsyncIterator[MongoDBCDCChange]:
        """
        Stream changes in real-time using change streams.

        Yields change events as they occur.
        """
        if not self.is_connected:
            await self.connect()

        self._is_streaming = True

        try:
            # Get the target for change stream
            target = self._get_watch_target()

            # Build change stream options
            options = {
                "full_document": self.config.full_document,
                "batch_size": self.config.batch_size,
                "max_await_time_ms": self.config.max_await_time_ms,
            }

            if self.config.full_document_before_change:
                options["full_document_before_change"] = self.config.full_document_before_change

            if self.config.resume_token:
                options["resume_after"] = {"_data": self.config.resume_token}
            elif self._last_resume_token:
                options["resume_after"] = {"_data": self._last_resume_token}

            if self.config.start_at_operation_time:
                options["start_at_operation_time"] = self.config.start_at_operation_time

            # Build pipeline
            pipeline = self.config.pipeline.copy()

            # Add tenant filter if configured
            if self.config.tenant_id:
                pipeline.insert(0, {
                    "$match": {
                        "$or": [
                            {"fullDocument.tenant_id": self.config.tenant_id},
                            {"fullDocumentBeforeChange.tenant_id": self.config.tenant_id},
                        ]
                    }
                })

            # Open change stream
            async with target.watch(pipeline, **options) as stream:
                self._change_stream = stream

                async for event in stream:
                    if not self._is_streaming:
                        break

                    change = self._parse_change_event(event)
                    if change:
                        self._last_resume_token = change.resume_token
                        yield change

        except Exception as e:
            logger.error(f"Change stream error: {e}")
            raise
        finally:
            self._is_streaming = False

    def _get_watch_target(self):
        """Get the MongoDB target to watch."""
        if self.config.collection and self.config.database:
            return self._client[self.config.database][self.config.collection]
        elif self.config.database:
            return self._client[self.config.database]
        else:
            return self._client

    def _parse_change_event(self, event: dict) -> Optional[MongoDBCDCChange]:
        """Parse a change stream event."""
        try:
            operation = MongoDBCDCOperation(event.get("operationType", ""))

            # Extract namespace
            ns = event.get("ns", {})
            database = ns.get("db", "")
            collection = ns.get("coll", "")

            # Extract resume token
            resume_token = None
            if "_id" in event:
                token_data = event["_id"]
                if isinstance(token_data, dict) and "_data" in token_data:
                    resume_token = token_data["_data"]

            # Extract cluster time
            cluster_time = None
            if "clusterTime" in event:
                cluster_time = event["clusterTime"].as_datetime()

            return MongoDBCDCChange(
                operation=operation,
                database=database,
                collection=collection,
                document_key=event.get("documentKey"),
                full_document=event.get("fullDocument"),
                full_document_before_change=event.get("fullDocumentBeforeChange"),
                update_description=event.get("updateDescription"),
                cluster_time=cluster_time,
                resume_token=resume_token,
            )

        except Exception as e:
            logger.warning(f"Failed to parse change event: {e}")
            return None

    async def poll_changes(
        self,
        limit: int = 100,
        timeout: float = 5.0,
    ) -> list[MongoDBCDCChange]:
        """
        Poll for changes with timeout.

        Returns a batch of changes.
        """
        changes = []

        try:
            async with asyncio.timeout(timeout):
                async for change in self.stream_changes():
                    changes.append(change)
                    if len(changes) >= limit:
                        break
        except asyncio.TimeoutError:
            pass

        return changes

    def on_change(self, handler: Callable[[MongoDBCDCChange], Any]) -> None:
        """Register a change handler."""
        self._handlers.append(handler)

    async def start_streaming(self) -> None:
        """Start streaming changes in the background."""
        async def _stream_loop():
            async for change in self.stream_changes():
                for handler in self._handlers:
                    try:
                        result = handler(change)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        logger.error(f"Change handler error: {e}")

        asyncio.create_task(_stream_loop())

    def stop_streaming(self) -> None:
        """Stop streaming changes."""
        self._is_streaming = False

    def get_resume_token(self) -> Optional[str]:
        """Get last resume token."""
        return self._last_resume_token

    async def get_collection_count(
        self,
        database: Optional[str] = None,
        collection: Optional[str] = None,
    ) -> int:
        """Get document count in a collection."""
        db = database or self.config.database
        coll = collection or self.config.collection

        if not db or not coll:
            raise ValueError("database and collection are required")

        return await self._client[db][coll].count_documents({})


# Flow Node Integration
@dataclass
class MongoDBCDCNodeConfig:
    """Configuration for MongoDB CDC flow node."""
    uri: str = "mongodb://localhost:27017"
    database: Optional[str] = None
    collection: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    operation: str = "poll"  # poll
    limit: int = 100
    full_document: str = "updateLookup"


@dataclass
class MongoDBCDCNodeResult:
    """Result from MongoDB CDC flow node."""
    success: bool
    operation: str
    changes: list[dict[str, Any]]
    count: int
    resume_token: Optional[str]
    message: str
    error: Optional[str]


class MongoDBCDCNode:
    """Flow node for MongoDB CDC operations."""

    node_type = "mongodb_cdc"
    node_category = "connector"

    def __init__(self, config: MongoDBCDCNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> MongoDBCDCNodeResult:
        """Execute the CDC operation."""
        cdc_config = MongoDBCDCConfig(
            uri=self.config.uri,
            database=self.config.database,
            collection=self.config.collection,
            username=self.config.username,
            password=self.config.password,
            full_document=self.config.full_document,
        )

        try:
            async with MongoDBCDCConnector(cdc_config) as cdc:
                if self.config.operation == "poll":
                    changes = await cdc.poll_changes(limit=self.config.limit)
                    resume_token = cdc.get_resume_token()

                    return MongoDBCDCNodeResult(
                        success=True,
                        operation="poll",
                        changes=[c.to_dict() for c in changes],
                        count=len(changes),
                        resume_token=resume_token,
                        message=f"Retrieved {len(changes)} changes",
                        error=None,
                    )

                else:
                    return MongoDBCDCNodeResult(
                        success=False,
                        operation=self.config.operation,
                        changes=[],
                        count=0,
                        resume_token=None,
                        message="Streaming not supported in node",
                        error="Use poll operation for node execution",
                    )

        except Exception as e:
            logger.error(f"MongoDB CDC node execution failed: {e}")
            return MongoDBCDCNodeResult(
                success=False,
                operation=self.config.operation,
                changes=[],
                count=0,
                resume_token=None,
                message="Execution failed",
                error=str(e),
            )


def get_mongodb_cdc_connector(config: MongoDBCDCConfig) -> MongoDBCDCConnector:
    """Factory function to create MongoDB CDC connector."""
    return MongoDBCDCConnector(config)


def get_mongodb_cdc_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "mongodb_cdc",
        "category": "connector",
        "label": "MongoDB CDC",
        "description": "Real-time MongoDB change streams",
        "icon": "Database",
        "color": "#4DB33D",
        "inputs": ["trigger"],
        "outputs": ["changes", "result"],
        "config_schema": {
            "uri": {
                "type": "string",
                "required": True,
                "label": "MongoDB URI",
                "placeholder": "mongodb://localhost:27017",
            },
            "database": {
                "type": "string",
                "label": "Database",
            },
            "collection": {
                "type": "string",
                "label": "Collection",
            },
            "username": {
                "type": "string",
                "label": "Username",
            },
            "password": {
                "type": "password",
                "label": "Password",
            },
            "full_document": {
                "type": "select",
                "options": ["default", "updateLookup", "whenAvailable", "required"],
                "default": "updateLookup",
                "label": "Full Document Mode",
            },
            "limit": {
                "type": "number",
                "default": 100,
                "label": "Max Changes",
            },
        },
    }

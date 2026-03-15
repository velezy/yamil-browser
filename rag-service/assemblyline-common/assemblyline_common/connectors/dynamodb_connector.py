"""
DynamoDB Connector for Logic Weaver.

Provides enterprise-grade DynamoDB integration with:
- On-demand and provisioned capacity modes
- Auto-scaling configuration
- Batch operations with error handling
- Conditional writes for optimistic locking
- Retry on throttling with circuit breaker
- Transaction support
- Multi-tenant isolation
- Health checks for monitoring
- TLS/SSL enforcement
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional, TypeVar, Union

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CapacityMode(str, Enum):
    """DynamoDB capacity modes."""
    ON_DEMAND = "PAY_PER_REQUEST"
    PROVISIONED = "PROVISIONED"


class ReturnValues(str, Enum):
    """Return values options for write operations."""
    NONE = "NONE"
    ALL_OLD = "ALL_OLD"
    UPDATED_OLD = "UPDATED_OLD"
    ALL_NEW = "ALL_NEW"
    UPDATED_NEW = "UPDATED_NEW"


@dataclass
class DynamoDBConfig:
    """Configuration for DynamoDB connector."""
    # AWS settings
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # For local development
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    role_arn: Optional[str] = None  # For role assumption

    # Connection settings
    max_pool_connections: int = 50
    connect_timeout: float = 5.0
    read_timeout: float = 30.0

    # TLS/SSL settings
    use_ssl: bool = True
    verify_ssl: bool = True
    ssl_ca_bundle: Optional[str] = None  # Custom CA bundle path

    # Retry settings
    max_retries: int = 10
    retry_mode: str = "adaptive"  # standard, adaptive

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    circuit_breaker_success_threshold: int = 3

    # Capacity settings
    default_capacity_mode: CapacityMode = CapacityMode.ON_DEMAND
    default_read_capacity: int = 5
    default_write_capacity: int = 5

    # Tenant isolation
    tenant_id: Optional[str] = None
    table_prefix: Optional[str] = None


@dataclass
class TableDefinition:
    """Table definition for creating tables."""
    table_name: str
    partition_key: str
    partition_key_type: str = "S"  # S=String, N=Number, B=Binary
    sort_key: Optional[str] = None
    sort_key_type: str = "S"
    capacity_mode: CapacityMode = CapacityMode.ON_DEMAND
    read_capacity: int = 5
    write_capacity: int = 5
    gsi: list[dict] = field(default_factory=list)
    lsi: list[dict] = field(default_factory=list)
    ttl_attribute: Optional[str] = None
    stream_enabled: bool = False
    stream_view_type: str = "NEW_AND_OLD_IMAGES"


@dataclass
class QueryResult:
    """Result from a query or scan operation."""
    items: list[dict]
    count: int
    scanned_count: int
    last_evaluated_key: Optional[dict] = None
    consumed_capacity: Optional[dict] = None

    @property
    def has_more(self) -> bool:
        """Check if there are more results to fetch."""
        return self.last_evaluated_key is not None


@dataclass
class BatchWriteResult:
    """Result from batch write operation."""
    success_count: int
    failed_count: int
    unprocessed_items: dict
    consumed_capacity: Optional[dict] = None


@dataclass
class HealthCheckResult:
    """Result from health check."""
    healthy: bool
    latency_ms: float
    message: str
    details: dict = field(default_factory=dict)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    def __init__(self, message: str, retry_after: float = 0):
        super().__init__(message)
        self.retry_after = retry_after


class DynamoDBConnector:
    """
    Enterprise DynamoDB connector.

    Features:
    - Automatic retry with exponential backoff
    - Circuit breaker for fault tolerance
    - Batch operations with unprocessed item handling
    - Conditional expressions for optimistic locking
    - Transaction support
    - TTL management
    - Multi-tenant table namespacing
    - Health checks for monitoring
    - TLS/SSL enforcement
    """

    def __init__(self, config: DynamoDBConfig):
        self.config = config
        self._client = None
        self._resource = None
        self._session = None
        self._metrics = {
            "read_units_consumed": 0,
            "write_units_consumed": 0,
            "throttled_requests": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "circuit_breaker_trips": 0,
        }
        # Circuit breaker state
        self._circuit_state = CircuitState.CLOSED
        self._circuit_failure_count = 0
        self._circuit_success_count = 0
        self._circuit_last_failure_time: Optional[float] = None
        self._circuit_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Initialize DynamoDB client."""
        # Run boto3 setup in thread pool (it's synchronous)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._setup_client)

        logger.info(
            "DynamoDB connector initialized",
            extra={
                "event_type": "dynamodb.connected",
                "region": self.config.region,
                "tenant_id": self.config.tenant_id,
            },
        )

    def _setup_client(self) -> None:
        """Set up boto3 client (synchronous)."""
        boto_config = BotoConfig(
            region_name=self.config.region,
            retries={
                "max_attempts": self.config.max_retries,
                "mode": self.config.retry_mode,
            },
            max_pool_connections=self.config.max_pool_connections,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            # Signature version for security
            signature_version="v4",
        )

        session_kwargs = {}
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

        self._session = boto3.Session(**session_kwargs)

        # Handle role assumption
        if self.config.role_arn:
            sts_client = self._session.client("sts")
            assumed_role = sts_client.assume_role(
                RoleArn=self.config.role_arn,
                RoleSessionName=f"logic-weaver-{self.config.tenant_id or 'default'}",
            )
            credentials = assumed_role["Credentials"]
            self._session = boto3.Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
            )

        client_kwargs = {"config": boto_config}
        if self.config.endpoint_url:
            client_kwargs["endpoint_url"] = self.config.endpoint_url

        # SSL/TLS configuration
        client_kwargs["use_ssl"] = self.config.use_ssl
        client_kwargs["verify"] = self.config.verify_ssl
        if self.config.ssl_ca_bundle:
            client_kwargs["verify"] = self.config.ssl_ca_bundle

        self._client = self._session.client("dynamodb", **client_kwargs)
        self._resource = self._session.resource("dynamodb", **client_kwargs)

    async def close(self) -> None:
        """Close the connector."""
        self._client = None
        self._resource = None
        self._session = None

    # -------------------------------------------------------------------------
    # Circuit Breaker Methods
    # -------------------------------------------------------------------------

    async def _check_circuit(self) -> None:
        """Check circuit breaker state and raise if open."""
        if not self.config.circuit_breaker_enabled:
            return

        async with self._circuit_lock:
            if self._circuit_state == CircuitState.OPEN:
                # Check if timeout has elapsed
                if self._circuit_last_failure_time:
                    elapsed = time.time() - self._circuit_last_failure_time
                    if elapsed >= self.config.circuit_breaker_timeout_seconds:
                        self._circuit_state = CircuitState.HALF_OPEN
                        self._circuit_success_count = 0
                        logger.info(
                            "Circuit breaker transitioning to HALF_OPEN",
                            extra={
                                "event_type": "dynamodb.circuit_half_open",
                                "tenant_id": self.config.tenant_id,
                            },
                        )
                    else:
                        retry_after = self.config.circuit_breaker_timeout_seconds - elapsed
                        raise CircuitBreakerError(
                            "DynamoDB circuit breaker is open",
                            retry_after=retry_after,
                        )

    async def _record_success(self) -> None:
        """Record successful operation for circuit breaker."""
        if not self.config.circuit_breaker_enabled:
            return

        async with self._circuit_lock:
            if self._circuit_state == CircuitState.HALF_OPEN:
                self._circuit_success_count += 1
                if self._circuit_success_count >= self.config.circuit_breaker_success_threshold:
                    self._circuit_state = CircuitState.CLOSED
                    self._circuit_failure_count = 0
                    logger.info(
                        "Circuit breaker closed after successful recovery",
                        extra={
                            "event_type": "dynamodb.circuit_closed",
                            "tenant_id": self.config.tenant_id,
                        },
                    )
            elif self._circuit_state == CircuitState.CLOSED:
                # Reset failure count on success
                self._circuit_failure_count = 0

    async def _record_failure(self) -> None:
        """Record failed operation for circuit breaker."""
        if not self.config.circuit_breaker_enabled:
            return

        async with self._circuit_lock:
            self._circuit_failure_count += 1
            self._circuit_last_failure_time = time.time()

            if self._circuit_state == CircuitState.HALF_OPEN:
                # Immediate trip on failure in half-open
                self._circuit_state = CircuitState.OPEN
                self._metrics["circuit_breaker_trips"] += 1
                logger.warning(
                    "Circuit breaker tripped from HALF_OPEN",
                    extra={
                        "event_type": "dynamodb.circuit_open",
                        "tenant_id": self.config.tenant_id,
                    },
                )
            elif self._circuit_state == CircuitState.CLOSED:
                if self._circuit_failure_count >= self.config.circuit_breaker_failure_threshold:
                    self._circuit_state = CircuitState.OPEN
                    self._metrics["circuit_breaker_trips"] += 1
                    logger.warning(
                        "Circuit breaker tripped",
                        extra={
                            "event_type": "dynamodb.circuit_open",
                            "failure_count": self._circuit_failure_count,
                            "tenant_id": self.config.tenant_id,
                        },
                    )

    def get_circuit_state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self._circuit_state

    # -------------------------------------------------------------------------
    # Health Check
    # -------------------------------------------------------------------------

    async def health_check(self) -> HealthCheckResult:
        """
        Perform health check on DynamoDB connection.

        Returns:
            HealthCheckResult with status and latency
        """
        start_time = time.time()

        try:
            # Check circuit breaker first
            if self._circuit_state == CircuitState.OPEN:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Circuit breaker is open",
                    details={
                        "circuit_state": self._circuit_state.value,
                        "retry_after": self.config.circuit_breaker_timeout_seconds,
                    },
                )

            # Try to list tables (lightweight operation)
            await self._run_sync(self._client.list_tables, Limit=1)

            latency_ms = (time.time() - start_time) * 1000

            return HealthCheckResult(
                healthy=True,
                latency_ms=latency_ms,
                message="DynamoDB connection healthy",
                details={
                    "region": self.config.region,
                    "circuit_state": self._circuit_state.value,
                    "tenant_id": self.config.tenant_id,
                },
            )

        except EndpointConnectionError as e:
            latency_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                healthy=False,
                latency_ms=latency_ms,
                message=f"Connection error: {str(e)}",
                details={"error_type": "connection"},
            )

        except ClientError as e:
            latency_ms = (time.time() - start_time) * 1000
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            return HealthCheckResult(
                healthy=False,
                latency_ms=latency_ms,
                message=f"DynamoDB error: {error_code}",
                details={"error_code": error_code},
            )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                healthy=False,
                latency_ms=latency_ms,
                message=f"Unexpected error: {str(e)}",
                details={"error_type": type(e).__name__},
            )

    def _get_table_name(self, table: str) -> str:
        """Get full table name with prefix."""
        if self.config.table_prefix:
            return f"{self.config.table_prefix}_{table}"
        elif self.config.tenant_id:
            return f"{self.config.tenant_id}_{table}"
        return table

    def _serialize_item(self, item: dict) -> dict:
        """Convert Python types to DynamoDB format."""
        from boto3.dynamodb.types import TypeSerializer
        serializer = TypeSerializer()

        def serialize_value(value):
            if isinstance(value, float):
                # DynamoDB doesn't support float, convert to Decimal
                return serializer.serialize(Decimal(str(value)))
            elif isinstance(value, dict):
                return {"M": {k: serialize_value(v) for k, v in value.items()}}
            elif isinstance(value, list):
                return {"L": [serialize_value(v) for v in value]}
            else:
                return serializer.serialize(value)

        return {k: serialize_value(v) for k, v in item.items()}

    def _deserialize_item(self, item: dict) -> dict:
        """Convert DynamoDB format to Python types."""
        from boto3.dynamodb.types import TypeDeserializer
        deserializer = TypeDeserializer()

        def deserialize_value(value):
            deserialized = deserializer.deserialize(value)
            if isinstance(deserialized, Decimal):
                # Convert Decimal back to float or int
                if deserialized % 1 == 0:
                    return int(deserialized)
                return float(deserialized)
            return deserialized

        return {k: deserialize_value(v) for k, v in item.items()}

    async def _run_sync(self, func, *args, **kwargs) -> Any:
        """Run synchronous boto3 call in thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    async def _execute_with_circuit_breaker(self, func, *args, **kwargs) -> Any:
        """Execute operation with circuit breaker protection."""
        await self._check_circuit()

        try:
            result = await self._run_sync(func, *args, **kwargs)
            await self._record_success()
            return result
        except (ClientError, EndpointConnectionError) as e:
            # Only trip circuit on connection/infrastructure errors, not business logic errors
            if isinstance(e, ClientError):
                error_code = e.response.get("Error", {}).get("Code", "")
                # Don't trip circuit for validation errors or conditional check failures
                if error_code not in [
                    "ConditionalCheckFailedException",
                    "ValidationException",
                    "ResourceNotFoundException",
                    "TransactionCanceledException",
                ]:
                    await self._record_failure()
            else:
                await self._record_failure()
            raise

    # -------------------------------------------------------------------------
    # Table Operations
    # -------------------------------------------------------------------------

    async def create_table(self, definition: TableDefinition) -> dict:
        """Create a new DynamoDB table."""
        table_name = self._get_table_name(definition.table_name)

        key_schema = [
            {"AttributeName": definition.partition_key, "KeyType": "HASH"}
        ]
        attribute_definitions = [
            {"AttributeName": definition.partition_key, "AttributeType": definition.partition_key_type}
        ]

        if definition.sort_key:
            key_schema.append(
                {"AttributeName": definition.sort_key, "KeyType": "RANGE"}
            )
            attribute_definitions.append(
                {"AttributeName": definition.sort_key, "AttributeType": definition.sort_key_type}
            )

        create_params = {
            "TableName": table_name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attribute_definitions,
            "BillingMode": definition.capacity_mode.value,
        }

        if definition.capacity_mode == CapacityMode.PROVISIONED:
            create_params["ProvisionedThroughput"] = {
                "ReadCapacityUnits": definition.read_capacity,
                "WriteCapacityUnits": definition.write_capacity,
            }

        # Add GSIs
        if definition.gsi:
            create_params["GlobalSecondaryIndexes"] = definition.gsi
            for gsi in definition.gsi:
                for key in gsi["KeySchema"]:
                    attr_name = key["AttributeName"]
                    if not any(a["AttributeName"] == attr_name for a in attribute_definitions):
                        attribute_definitions.append(
                            {"AttributeName": attr_name, "AttributeType": "S"}
                        )

        # Add LSIs
        if definition.lsi:
            create_params["LocalSecondaryIndexes"] = definition.lsi

        # Add streams
        if definition.stream_enabled:
            create_params["StreamSpecification"] = {
                "StreamEnabled": True,
                "StreamViewType": definition.stream_view_type,
            }

        result = await self._run_sync(self._client.create_table, **create_params)

        # Enable TTL if specified
        if definition.ttl_attribute:
            await self._run_sync(
                self._client.update_time_to_live,
                TableName=table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": definition.ttl_attribute,
                },
            )

        logger.info(
            "DynamoDB table created",
            extra={
                "event_type": "dynamodb.table_created",
                "table_name": table_name,
                "capacity_mode": definition.capacity_mode.value,
                "tenant_id": self.config.tenant_id,
            },
        )

        return result["TableDescription"]

    async def delete_table(self, table: str) -> None:
        """Delete a DynamoDB table."""
        table_name = self._get_table_name(table)
        await self._run_sync(self._client.delete_table, TableName=table_name)

        logger.info(
            "DynamoDB table deleted",
            extra={
                "event_type": "dynamodb.table_deleted",
                "table_name": table_name,
                "tenant_id": self.config.tenant_id,
            },
        )

    async def describe_table(self, table: str) -> dict:
        """Get table description and status."""
        table_name = self._get_table_name(table)
        result = await self._run_sync(self._client.describe_table, TableName=table_name)
        return result["Table"]

    async def wait_for_table(self, table: str, timeout: float = 300) -> None:
        """Wait for table to become active."""
        table_name = self._get_table_name(table)
        start = time.time()

        while time.time() - start < timeout:
            try:
                result = await self.describe_table(table)
                if result["TableStatus"] == "ACTIVE":
                    return
            except ClientError:
                pass
            await asyncio.sleep(2)

        raise TimeoutError(f"Table {table_name} did not become active within {timeout}s")

    # -------------------------------------------------------------------------
    # Item Operations
    # -------------------------------------------------------------------------

    async def put_item(
        self,
        table: str,
        item: dict,
        condition_expression: Optional[str] = None,
        expression_attribute_names: Optional[dict] = None,
        expression_attribute_values: Optional[dict] = None,
        return_values: ReturnValues = ReturnValues.NONE,
    ) -> Optional[dict]:
        """
        Put an item into a table.

        Supports conditional writes for optimistic locking.
        """
        table_name = self._get_table_name(table)

        params = {
            "TableName": table_name,
            "Item": self._serialize_item(item),
            "ReturnValues": return_values.value,
            "ReturnConsumedCapacity": "TOTAL",
        }

        if condition_expression:
            params["ConditionExpression"] = condition_expression
        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            params["ExpressionAttributeValues"] = self._serialize_item(expression_attribute_values)

        try:
            result = await self._execute_with_circuit_breaker(self._client.put_item, **params)
            self._metrics["successful_operations"] += 1

            if result.get("ConsumedCapacity"):
                self._metrics["write_units_consumed"] += result["ConsumedCapacity"].get("CapacityUnits", 0)

            if return_values != ReturnValues.NONE and "Attributes" in result:
                return self._deserialize_item(result["Attributes"])
            return None

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConditionalCheckFailedError(
                    f"Condition check failed for put_item on {table}"
                )
            self._metrics["failed_operations"] += 1
            raise

    async def get_item(
        self,
        table: str,
        key: dict,
        consistent_read: bool = False,
        projection_expression: Optional[str] = None,
        expression_attribute_names: Optional[dict] = None,
    ) -> Optional[dict]:
        """Get a single item by key."""
        table_name = self._get_table_name(table)

        params = {
            "TableName": table_name,
            "Key": self._serialize_item(key),
            "ConsistentRead": consistent_read,
            "ReturnConsumedCapacity": "TOTAL",
        }

        if projection_expression:
            params["ProjectionExpression"] = projection_expression
        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names

        result = await self._execute_with_circuit_breaker(self._client.get_item, **params)
        self._metrics["successful_operations"] += 1

        if result.get("ConsumedCapacity"):
            self._metrics["read_units_consumed"] += result["ConsumedCapacity"].get("CapacityUnits", 0)

        if "Item" in result:
            return self._deserialize_item(result["Item"])
        return None

    async def update_item(
        self,
        table: str,
        key: dict,
        update_expression: str,
        expression_attribute_names: Optional[dict] = None,
        expression_attribute_values: Optional[dict] = None,
        condition_expression: Optional[str] = None,
        return_values: ReturnValues = ReturnValues.ALL_NEW,
    ) -> Optional[dict]:
        """Update an item with update expressions."""
        table_name = self._get_table_name(table)

        params = {
            "TableName": table_name,
            "Key": self._serialize_item(key),
            "UpdateExpression": update_expression,
            "ReturnValues": return_values.value,
            "ReturnConsumedCapacity": "TOTAL",
        }

        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            params["ExpressionAttributeValues"] = self._serialize_item(expression_attribute_values)
        if condition_expression:
            params["ConditionExpression"] = condition_expression

        try:
            result = await self._execute_with_circuit_breaker(self._client.update_item, **params)
            self._metrics["successful_operations"] += 1

            if result.get("ConsumedCapacity"):
                self._metrics["write_units_consumed"] += result["ConsumedCapacity"].get("CapacityUnits", 0)

            if "Attributes" in result:
                return self._deserialize_item(result["Attributes"])
            return None

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConditionalCheckFailedError(
                    f"Condition check failed for update_item on {table}"
                )
            self._metrics["failed_operations"] += 1
            raise

    async def delete_item(
        self,
        table: str,
        key: dict,
        condition_expression: Optional[str] = None,
        expression_attribute_names: Optional[dict] = None,
        expression_attribute_values: Optional[dict] = None,
        return_values: ReturnValues = ReturnValues.NONE,
    ) -> Optional[dict]:
        """Delete an item by key."""
        table_name = self._get_table_name(table)

        params = {
            "TableName": table_name,
            "Key": self._serialize_item(key),
            "ReturnValues": return_values.value,
            "ReturnConsumedCapacity": "TOTAL",
        }

        if condition_expression:
            params["ConditionExpression"] = condition_expression
        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            params["ExpressionAttributeValues"] = self._serialize_item(expression_attribute_values)

        try:
            result = await self._execute_with_circuit_breaker(self._client.delete_item, **params)
            self._metrics["successful_operations"] += 1

            if result.get("ConsumedCapacity"):
                self._metrics["write_units_consumed"] += result["ConsumedCapacity"].get("CapacityUnits", 0)

            if return_values != ReturnValues.NONE and "Attributes" in result:
                return self._deserialize_item(result["Attributes"])
            return None

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConditionalCheckFailedError(
                    f"Condition check failed for delete_item on {table}"
                )
            self._metrics["failed_operations"] += 1
            raise

    # -------------------------------------------------------------------------
    # Query and Scan Operations
    # -------------------------------------------------------------------------

    async def query(
        self,
        table: str,
        key_condition_expression: str,
        expression_attribute_names: Optional[dict] = None,
        expression_attribute_values: Optional[dict] = None,
        filter_expression: Optional[str] = None,
        projection_expression: Optional[str] = None,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        scan_index_forward: bool = True,
        consistent_read: bool = False,
        exclusive_start_key: Optional[dict] = None,
    ) -> QueryResult:
        """Query items by partition key."""
        table_name = self._get_table_name(table)

        params = {
            "TableName": table_name,
            "KeyConditionExpression": key_condition_expression,
            "ScanIndexForward": scan_index_forward,
            "ConsistentRead": consistent_read,
            "ReturnConsumedCapacity": "TOTAL",
        }

        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            params["ExpressionAttributeValues"] = self._serialize_item(expression_attribute_values)
        if filter_expression:
            params["FilterExpression"] = filter_expression
        if projection_expression:
            params["ProjectionExpression"] = projection_expression
        if index_name:
            params["IndexName"] = index_name
        if limit:
            params["Limit"] = limit
        if exclusive_start_key:
            params["ExclusiveStartKey"] = self._serialize_item(exclusive_start_key)

        result = await self._execute_with_circuit_breaker(self._client.query, **params)
        self._metrics["successful_operations"] += 1

        if result.get("ConsumedCapacity"):
            self._metrics["read_units_consumed"] += result["ConsumedCapacity"].get("CapacityUnits", 0)

        return QueryResult(
            items=[self._deserialize_item(item) for item in result.get("Items", [])],
            count=result.get("Count", 0),
            scanned_count=result.get("ScannedCount", 0),
            last_evaluated_key=result.get("LastEvaluatedKey"),
            consumed_capacity=result.get("ConsumedCapacity"),
        )

    async def query_all(
        self,
        table: str,
        key_condition_expression: str,
        **kwargs,
    ) -> AsyncIterator[dict]:
        """Query all items with automatic pagination."""
        exclusive_start_key = None

        while True:
            result = await self.query(
                table,
                key_condition_expression,
                exclusive_start_key=exclusive_start_key,
                **kwargs,
            )

            for item in result.items:
                yield item

            if not result.has_more:
                break

            exclusive_start_key = result.last_evaluated_key

    async def scan(
        self,
        table: str,
        filter_expression: Optional[str] = None,
        expression_attribute_names: Optional[dict] = None,
        expression_attribute_values: Optional[dict] = None,
        projection_expression: Optional[str] = None,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        consistent_read: bool = False,
        exclusive_start_key: Optional[dict] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
    ) -> QueryResult:
        """Scan all items in a table (expensive, use sparingly)."""
        table_name = self._get_table_name(table)

        params = {
            "TableName": table_name,
            "ConsistentRead": consistent_read,
            "ReturnConsumedCapacity": "TOTAL",
        }

        if filter_expression:
            params["FilterExpression"] = filter_expression
        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values:
            params["ExpressionAttributeValues"] = self._serialize_item(expression_attribute_values)
        if projection_expression:
            params["ProjectionExpression"] = projection_expression
        if index_name:
            params["IndexName"] = index_name
        if limit:
            params["Limit"] = limit
        if exclusive_start_key:
            params["ExclusiveStartKey"] = self._serialize_item(exclusive_start_key)
        if segment is not None and total_segments:
            params["Segment"] = segment
            params["TotalSegments"] = total_segments

        result = await self._execute_with_circuit_breaker(self._client.scan, **params)
        self._metrics["successful_operations"] += 1

        if result.get("ConsumedCapacity"):
            self._metrics["read_units_consumed"] += result["ConsumedCapacity"].get("CapacityUnits", 0)

        return QueryResult(
            items=[self._deserialize_item(item) for item in result.get("Items", [])],
            count=result.get("Count", 0),
            scanned_count=result.get("ScannedCount", 0),
            last_evaluated_key=result.get("LastEvaluatedKey"),
            consumed_capacity=result.get("ConsumedCapacity"),
        )

    # -------------------------------------------------------------------------
    # Batch Operations
    # -------------------------------------------------------------------------

    async def batch_get_items(
        self,
        table: str,
        keys: list[dict],
        consistent_read: bool = False,
        projection_expression: Optional[str] = None,
        expression_attribute_names: Optional[dict] = None,
    ) -> list[dict]:
        """
        Batch get up to 100 items.

        Automatically handles unprocessed keys with retry.
        """
        table_name = self._get_table_name(table)
        all_items = []

        # Process in chunks of 100
        for i in range(0, len(keys), 100):
            chunk = keys[i:i + 100]

            request_items = {
                table_name: {
                    "Keys": [self._serialize_item(k) for k in chunk],
                    "ConsistentRead": consistent_read,
                }
            }

            if projection_expression:
                request_items[table_name]["ProjectionExpression"] = projection_expression
            if expression_attribute_names:
                request_items[table_name]["ExpressionAttributeNames"] = expression_attribute_names

            # Retry loop for unprocessed keys
            max_retries = 5
            for attempt in range(max_retries):
                result = await self._run_sync(
                    self._client.batch_get_item,
                    RequestItems=request_items,
                    ReturnConsumedCapacity="TOTAL",
                )

                # Collect retrieved items
                if table_name in result.get("Responses", {}):
                    for item in result["Responses"][table_name]:
                        all_items.append(self._deserialize_item(item))

                # Check for unprocessed keys
                unprocessed = result.get("UnprocessedKeys", {})
                if not unprocessed or table_name not in unprocessed:
                    break

                # Exponential backoff
                await asyncio.sleep(2 ** attempt * 0.1)
                request_items = unprocessed

        self._metrics["successful_operations"] += 1
        return all_items

    async def batch_write_items(
        self,
        table: str,
        items: list[dict],
        operation: str = "put",  # put or delete
    ) -> BatchWriteResult:
        """
        Batch write up to 25 items.

        Automatically handles unprocessed items with retry.
        """
        table_name = self._get_table_name(table)

        success_count = 0
        failed_count = 0
        all_unprocessed = {}

        # Process in chunks of 25
        for i in range(0, len(items), 25):
            chunk = items[i:i + 25]

            if operation == "put":
                write_requests = [
                    {"PutRequest": {"Item": self._serialize_item(item)}}
                    for item in chunk
                ]
            else:  # delete
                write_requests = [
                    {"DeleteRequest": {"Key": self._serialize_item(item)}}
                    for item in chunk
                ]

            request_items = {table_name: write_requests}

            # Retry loop for unprocessed items
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    result = await self._run_sync(
                        self._client.batch_write_item,
                        RequestItems=request_items,
                        ReturnConsumedCapacity="TOTAL",
                    )

                    if result.get("ConsumedCapacity"):
                        for cap in result["ConsumedCapacity"]:
                            self._metrics["write_units_consumed"] += cap.get("CapacityUnits", 0)

                    # Check for unprocessed items
                    unprocessed = result.get("UnprocessedItems", {})
                    if not unprocessed or table_name not in unprocessed:
                        success_count += len(chunk)
                        break

                    # Update for retry
                    processed = len(chunk) - len(unprocessed.get(table_name, []))
                    success_count += processed

                    if attempt == max_retries - 1:
                        # Final attempt, record failures
                        failed_count += len(unprocessed.get(table_name, []))
                        all_unprocessed = unprocessed
                    else:
                        # Exponential backoff and retry
                        await asyncio.sleep(2 ** attempt * 0.1)
                        request_items = unprocessed

                except ClientError as e:
                    if e.response["Error"]["Code"] == "ProvisionedThroughputExceededException":
                        self._metrics["throttled_requests"] += 1
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt * 0.5)
                            continue
                    raise

        self._metrics["successful_operations"] += 1
        return BatchWriteResult(
            success_count=success_count,
            failed_count=failed_count,
            unprocessed_items=all_unprocessed,
        )

    # -------------------------------------------------------------------------
    # Transaction Operations
    # -------------------------------------------------------------------------

    async def transact_write_items(
        self,
        items: list[dict],
    ) -> None:
        """
        Execute transactional write of up to 25 items.

        Each item should be a dict with one of:
        - Put: {TableName, Item, ConditionExpression?}
        - Update: {TableName, Key, UpdateExpression, ...}
        - Delete: {TableName, Key, ConditionExpression?}
        - ConditionCheck: {TableName, Key, ConditionExpression}
        """
        transact_items = []

        for item in items:
            if "Put" in item:
                put_item = item["Put"].copy()
                put_item["TableName"] = self._get_table_name(put_item["TableName"])
                put_item["Item"] = self._serialize_item(put_item["Item"])
                if "ExpressionAttributeValues" in put_item:
                    put_item["ExpressionAttributeValues"] = self._serialize_item(
                        put_item["ExpressionAttributeValues"]
                    )
                transact_items.append({"Put": put_item})

            elif "Update" in item:
                update_item = item["Update"].copy()
                update_item["TableName"] = self._get_table_name(update_item["TableName"])
                update_item["Key"] = self._serialize_item(update_item["Key"])
                if "ExpressionAttributeValues" in update_item:
                    update_item["ExpressionAttributeValues"] = self._serialize_item(
                        update_item["ExpressionAttributeValues"]
                    )
                transact_items.append({"Update": update_item})

            elif "Delete" in item:
                delete_item = item["Delete"].copy()
                delete_item["TableName"] = self._get_table_name(delete_item["TableName"])
                delete_item["Key"] = self._serialize_item(delete_item["Key"])
                transact_items.append({"Delete": delete_item})

            elif "ConditionCheck" in item:
                check_item = item["ConditionCheck"].copy()
                check_item["TableName"] = self._get_table_name(check_item["TableName"])
                check_item["Key"] = self._serialize_item(check_item["Key"])
                if "ExpressionAttributeValues" in check_item:
                    check_item["ExpressionAttributeValues"] = self._serialize_item(
                        check_item["ExpressionAttributeValues"]
                    )
                transact_items.append({"ConditionCheck": check_item})

        try:
            await self._run_sync(
                self._client.transact_write_items,
                TransactItems=transact_items,
                ReturnConsumedCapacity="TOTAL",
            )
            self._metrics["successful_operations"] += 1

        except ClientError as e:
            if e.response["Error"]["Code"] == "TransactionCanceledException":
                reasons = e.response.get("CancellationReasons", [])
                raise TransactionCancelledError(
                    f"Transaction cancelled: {reasons}"
                )
            self._metrics["failed_operations"] += 1
            raise

    async def transact_get_items(
        self,
        items: list[dict],
    ) -> list[Optional[dict]]:
        """
        Execute transactional read of up to 25 items.

        Each item should be: {TableName, Key, ProjectionExpression?}
        Returns list in same order as input (None for not found).
        """
        transact_items = []

        for item in items:
            get_item = {
                "Get": {
                    "TableName": self._get_table_name(item["TableName"]),
                    "Key": self._serialize_item(item["Key"]),
                }
            }
            if "ProjectionExpression" in item:
                get_item["Get"]["ProjectionExpression"] = item["ProjectionExpression"]
            if "ExpressionAttributeNames" in item:
                get_item["Get"]["ExpressionAttributeNames"] = item["ExpressionAttributeNames"]
            transact_items.append(get_item)

        result = await self._run_sync(
            self._client.transact_get_items,
            TransactItems=transact_items,
            ReturnConsumedCapacity="TOTAL",
        )

        self._metrics["successful_operations"] += 1

        responses = []
        for response in result.get("Responses", []):
            if "Item" in response:
                responses.append(self._deserialize_item(response["Item"]))
            else:
                responses.append(None)

        return responses

    # -------------------------------------------------------------------------
    # Capacity Management
    # -------------------------------------------------------------------------

    async def update_table_capacity(
        self,
        table: str,
        capacity_mode: Optional[CapacityMode] = None,
        read_capacity: Optional[int] = None,
        write_capacity: Optional[int] = None,
    ) -> dict:
        """Update table capacity settings."""
        table_name = self._get_table_name(table)

        params = {"TableName": table_name}

        if capacity_mode:
            params["BillingMode"] = capacity_mode.value

        if capacity_mode == CapacityMode.PROVISIONED or (
            not capacity_mode and (read_capacity or write_capacity)
        ):
            params["ProvisionedThroughput"] = {}
            if read_capacity:
                params["ProvisionedThroughput"]["ReadCapacityUnits"] = read_capacity
            if write_capacity:
                params["ProvisionedThroughput"]["WriteCapacityUnits"] = write_capacity

        result = await self._run_sync(self._client.update_table, **params)

        logger.info(
            "DynamoDB table capacity updated",
            extra={
                "event_type": "dynamodb.capacity_updated",
                "table_name": table_name,
                "capacity_mode": capacity_mode.value if capacity_mode else None,
                "read_capacity": read_capacity,
                "write_capacity": write_capacity,
                "tenant_id": self.config.tenant_id,
            },
        )

        return result["TableDescription"]

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------

    def get_metrics(self) -> dict:
        """Get connector metrics."""
        return {
            **self._metrics,
            "circuit_state": self._circuit_state.value,
            "circuit_failure_count": self._circuit_failure_count,
            "tenant_id": self.config.tenant_id,
        }

    def reset_metrics(self) -> None:
        """Reset connector metrics."""
        self._metrics = {
            "read_units_consumed": 0,
            "write_units_consumed": 0,
            "throttled_requests": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "circuit_breaker_trips": 0,
        }


class ConditionalCheckFailedError(Exception):
    """Conditional check failed for write operation."""
    pass


class TransactionCancelledError(Exception):
    """Transaction was cancelled."""
    pass


# Singleton instance management
_dynamodb_instances: dict[str, DynamoDBConnector] = {}
_lock = asyncio.Lock()


async def get_dynamodb_connector(
    config: Optional[DynamoDBConfig] = None,
    instance_key: str = "default",
) -> DynamoDBConnector:
    """
    Get or create a DynamoDB connector instance.

    Args:
        config: Configuration (required for first call)
        instance_key: Key for managing multiple instances
    """
    async with _lock:
        if instance_key not in _dynamodb_instances:
            if config is None:
                raise ValueError("Config required for first initialization")

            connector = DynamoDBConnector(config)
            await connector.connect()
            _dynamodb_instances[instance_key] = connector

        return _dynamodb_instances[instance_key]


async def close_dynamodb_connector(instance_key: str = "default") -> None:
    """Close and remove a connector instance."""
    async with _lock:
        if instance_key in _dynamodb_instances:
            await _dynamodb_instances[instance_key].close()
            del _dynamodb_instances[instance_key]

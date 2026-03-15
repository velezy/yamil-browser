"""
Kafka Schema Registry Connector for Logic Weaver.

Provides enterprise-grade schema registry integration with:
- Confluent Schema Registry support
- Avro schema validation and serialization
- JSON Schema validation
- Schema evolution and compatibility checking
- Caching for performance
- Multi-tenant isolation
- Circuit breaker for fault tolerance
- Health checks for monitoring
- TLS/SSL enforcement
"""

import asyncio
import hashlib
import json
import logging
import struct
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Union

import httpx

logger = logging.getLogger(__name__)


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


@dataclass
class HealthCheckResult:
    """Result from health check."""
    healthy: bool
    latency_ms: float
    message: str
    details: dict = field(default_factory=dict)


class SchemaType(str, Enum):
    """Supported schema types."""
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


class CompatibilityLevel(str, Enum):
    """Schema compatibility levels."""
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


@dataclass
class SchemaRegistryConfig:
    """Configuration for Schema Registry connector."""
    # Connection settings
    url: str = "http://localhost:8081"
    username: Optional[str] = None
    password: Optional[str] = None

    # SSL settings
    ssl_ca_cert: Optional[str] = None
    ssl_client_cert: Optional[str] = None
    ssl_client_key: Optional[str] = None
    verify_ssl: bool = True

    # Cache settings
    cache_ttl_seconds: int = 300
    cache_max_size: int = 1000

    # Request settings
    timeout_seconds: float = 30.0
    max_retries: int = 3

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    circuit_breaker_success_threshold: int = 3

    # Tenant isolation
    tenant_id: Optional[str] = None
    subject_prefix: Optional[str] = None


@dataclass
class Schema:
    """Represents a schema in the registry."""
    schema_id: int
    schema_type: SchemaType
    schema_str: str
    subject: str
    version: int
    references: list = field(default_factory=list)

    @property
    def schema_dict(self) -> dict:
        """Parse schema string to dict."""
        return json.loads(self.schema_str)


@dataclass
class SchemaVersion:
    """Schema version information."""
    subject: str
    version: int
    schema_id: int
    schema_str: str
    schema_type: SchemaType


class SchemaCache:
    """LRU cache for schemas with TTL."""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[Any, float]] = {}
        self._access_order: list[str] = []
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """Get item from cache if not expired."""
        async with self._lock:
            if key not in self._cache:
                return None

            value, timestamp = self._cache[key]
            if time.time() - timestamp > self.ttl_seconds:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                return None

            # Update access order
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

            return value

    async def set(self, key: str, value: Any) -> None:
        """Set item in cache with current timestamp."""
        async with self._lock:
            # Evict oldest if at capacity
            while len(self._cache) >= self.max_size and self._access_order:
                oldest = self._access_order.pop(0)
                self._cache.pop(oldest, None)

            self._cache[key] = (value, time.time())
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

    async def invalidate(self, key: str) -> None:
        """Remove item from cache."""
        async with self._lock:
            self._cache.pop(key, None)
            if key in self._access_order:
                self._access_order.remove(key)

    async def clear(self) -> None:
        """Clear all cached items."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()


class AvroSerializer:
    """Avro serialization with schema registry wire format."""

    MAGIC_BYTE = 0

    @staticmethod
    def encode_message(schema_id: int, data: bytes) -> bytes:
        """
        Encode message with Confluent wire format.
        Format: [Magic Byte (1)] [Schema ID (4)] [Data (N)]
        """
        return struct.pack(">bI", AvroSerializer.MAGIC_BYTE, schema_id) + data

    @staticmethod
    def decode_message(data: bytes) -> tuple[int, bytes]:
        """
        Decode message with Confluent wire format.
        Returns: (schema_id, payload)
        """
        if len(data) < 5:
            raise ValueError("Message too short for wire format")

        magic, schema_id = struct.unpack(">bI", data[:5])
        if magic != AvroSerializer.MAGIC_BYTE:
            raise ValueError(f"Invalid magic byte: {magic}")

        return schema_id, data[5:]


class JsonSchemaValidator:
    """JSON Schema validation."""

    def __init__(self):
        self._validators: dict[str, Any] = {}

    def validate(self, schema_str: str, data: dict) -> tuple[bool, Optional[str]]:
        """
        Validate data against JSON Schema.
        Returns: (is_valid, error_message)
        """
        try:
            import jsonschema

            schema = json.loads(schema_str)
            schema_hash = hashlib.md5(schema_str.encode()).hexdigest()

            if schema_hash not in self._validators:
                self._validators[schema_hash] = jsonschema.Draft7Validator(schema)

            validator = self._validators[schema_hash]
            errors = list(validator.iter_errors(data))

            if errors:
                error_messages = [f"{e.path}: {e.message}" for e in errors[:5]]
                return False, "; ".join(error_messages)

            return True, None

        except ImportError:
            logger.warning("jsonschema not installed, skipping validation")
            return True, None
        except Exception as e:
            return False, str(e)


class SchemaRegistryConnector:
    """
    Enterprise Schema Registry connector.

    Features:
    - Confluent Schema Registry API
    - Avro and JSON Schema support
    - Schema caching with TTL
    - Compatibility checking
    - Multi-tenant subject namespacing
    - Circuit breaker for fault tolerance
    - Health checks for monitoring
    - TLS/SSL enforcement
    """

    def __init__(self, config: SchemaRegistryConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._cache = SchemaCache(
            max_size=config.cache_max_size,
            ttl_seconds=config.cache_ttl_seconds,
        )
        self._json_validator = JsonSchemaValidator()
        self._id_to_schema: dict[int, Schema] = {}
        self._metrics = {
            "schemas_registered": 0,
            "schemas_fetched": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "circuit_breaker_trips": 0,
        }
        # Circuit breaker state
        self._circuit_state = CircuitState.CLOSED
        self._circuit_failure_count = 0
        self._circuit_success_count = 0
        self._circuit_last_failure_time: Optional[float] = None
        self._circuit_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Initialize HTTP client."""
        auth = None
        if self.config.username and self.config.password:
            auth = httpx.BasicAuth(self.config.username, self.config.password)

        # SSL/TLS configuration
        verify = self.config.verify_ssl
        if self.config.ssl_ca_cert:
            import ssl
            ssl_context = ssl.create_default_context(cafile=self.config.ssl_ca_cert)
            if self.config.ssl_client_cert and self.config.ssl_client_key:
                ssl_context.load_cert_chain(
                    self.config.ssl_client_cert,
                    self.config.ssl_client_key,
                )
            verify = ssl_context

        self._client = httpx.AsyncClient(
            base_url=self.config.url,
            auth=auth,
            verify=verify,
            timeout=self.config.timeout_seconds,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            http2=True,  # Enable HTTP/2 for better performance
        )

        logger.info(
            "Schema Registry connector initialized",
            extra={
                "event_type": "schema_registry.connected",
                "url": self.config.url,
                "tenant_id": self.config.tenant_id,
            },
        )

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
        await self._cache.clear()

    # -------------------------------------------------------------------------
    # Circuit Breaker Methods
    # -------------------------------------------------------------------------

    async def _check_circuit(self) -> None:
        """Check circuit breaker state and raise if open."""
        if not self.config.circuit_breaker_enabled:
            return

        async with self._circuit_lock:
            if self._circuit_state == CircuitState.OPEN:
                if self._circuit_last_failure_time:
                    elapsed = time.time() - self._circuit_last_failure_time
                    if elapsed >= self.config.circuit_breaker_timeout_seconds:
                        self._circuit_state = CircuitState.HALF_OPEN
                        self._circuit_success_count = 0
                        logger.info(
                            "Schema Registry circuit breaker transitioning to HALF_OPEN",
                            extra={"event_type": "schema_registry.circuit_half_open"},
                        )
                    else:
                        retry_after = self.config.circuit_breaker_timeout_seconds - elapsed
                        raise CircuitBreakerError(
                            "Schema Registry circuit breaker is open",
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
                        "Schema Registry circuit breaker closed",
                        extra={"event_type": "schema_registry.circuit_closed"},
                    )
            elif self._circuit_state == CircuitState.CLOSED:
                self._circuit_failure_count = 0

    async def _record_failure(self) -> None:
        """Record failed operation for circuit breaker."""
        if not self.config.circuit_breaker_enabled:
            return

        async with self._circuit_lock:
            self._circuit_failure_count += 1
            self._circuit_last_failure_time = time.time()

            if self._circuit_state == CircuitState.HALF_OPEN:
                self._circuit_state = CircuitState.OPEN
                self._metrics["circuit_breaker_trips"] += 1
                logger.warning(
                    "Schema Registry circuit breaker tripped from HALF_OPEN",
                    extra={"event_type": "schema_registry.circuit_open"},
                )
            elif self._circuit_state == CircuitState.CLOSED:
                if self._circuit_failure_count >= self.config.circuit_breaker_failure_threshold:
                    self._circuit_state = CircuitState.OPEN
                    self._metrics["circuit_breaker_trips"] += 1
                    logger.warning(
                        "Schema Registry circuit breaker tripped",
                        extra={
                            "event_type": "schema_registry.circuit_open",
                            "failure_count": self._circuit_failure_count,
                        },
                    )

    def get_circuit_state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self._circuit_state

    # -------------------------------------------------------------------------
    # Health Check
    # -------------------------------------------------------------------------

    async def health_check(self) -> HealthCheckResult:
        """Perform health check on Schema Registry connection."""
        start_time = time.time()

        try:
            if self._circuit_state == CircuitState.OPEN:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Circuit breaker is open",
                    details={"circuit_state": self._circuit_state.value},
                )

            if not self._client:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Client not connected",
                )

            # Check API access by getting subjects list (lightweight)
            response = await self._client.get("/subjects")
            latency_ms = (time.time() - start_time) * 1000

            if response.status_code == 200:
                return HealthCheckResult(
                    healthy=True,
                    latency_ms=latency_ms,
                    message="Schema Registry connection healthy",
                    details={
                        "url": self.config.url,
                        "circuit_state": self._circuit_state.value,
                        "cache_size": len(self._cache._cache),
                    },
                )
            else:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=latency_ms,
                    message=f"Schema Registry returned {response.status_code}",
                    details={"status_code": response.status_code},
                )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                healthy=False,
                latency_ms=latency_ms,
                message=f"Health check failed: {str(e)}",
                details={"error_type": type(e).__name__},
            )

    def _get_subject(self, topic: str, is_key: bool = False) -> str:
        """Get subject name with optional prefix."""
        suffix = "key" if is_key else "value"
        subject = f"{topic}-{suffix}"

        if self.config.subject_prefix:
            subject = f"{self.config.subject_prefix}.{subject}"
        elif self.config.tenant_id:
            subject = f"{self.config.tenant_id}.{subject}"

        return subject

    async def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[dict] = None,
    ) -> dict:
        """Make HTTP request with retry logic and circuit breaker."""
        if not self._client:
            raise RuntimeError("Connector not connected")

        await self._check_circuit()

        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                response = await self._client.request(
                    method,
                    path,
                    json=json_data,
                )

                if response.status_code == 404:
                    raise SchemaNotFoundError(f"Not found: {path}")

                if response.status_code == 409:
                    error_data = response.json()
                    raise IncompatibleSchemaError(
                        error_data.get("message", "Schema incompatible")
                    )

                # Record failure for 5xx errors (server-side)
                if response.status_code >= 500:
                    await self._record_failure()
                    response.raise_for_status()

                response.raise_for_status()
                await self._record_success()
                return response.json() if response.content else {}

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                await self._record_failure()
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue

        raise ConnectionError(f"Failed after {self.config.max_retries} attempts: {last_error}")

    # -------------------------------------------------------------------------
    # Schema Operations
    # -------------------------------------------------------------------------

    async def register_schema(
        self,
        topic: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
        is_key: bool = False,
        references: Optional[list[dict]] = None,
    ) -> int:
        """
        Register a new schema for a topic.

        Returns the schema ID.
        """
        subject = self._get_subject(topic, is_key)

        payload = {
            "schema": schema_str,
            "schemaType": schema_type.value,
        }
        if references:
            payload["references"] = references

        result = await self._request("POST", f"/subjects/{subject}/versions", payload)
        schema_id = result["id"]

        # Cache the schema
        schema = Schema(
            schema_id=schema_id,
            schema_type=schema_type,
            schema_str=schema_str,
            subject=subject,
            version=result.get("version", -1),
            references=references or [],
        )
        self._id_to_schema[schema_id] = schema
        await self._cache.set(f"id:{schema_id}", schema)

        logger.info(
            "Schema registered",
            extra={
                "event_type": "schema_registry.schema_registered",
                "subject": subject,
                "schema_id": schema_id,
                "schema_type": schema_type.value,
                "tenant_id": self.config.tenant_id,
            },
        )

        return schema_id

    async def get_schema_by_id(self, schema_id: int) -> Schema:
        """Get schema by its global ID."""
        # Check cache
        cached = await self._cache.get(f"id:{schema_id}")
        if cached:
            return cached

        # Check local map
        if schema_id in self._id_to_schema:
            return self._id_to_schema[schema_id]

        # Fetch from registry
        result = await self._request("GET", f"/schemas/ids/{schema_id}")

        schema = Schema(
            schema_id=schema_id,
            schema_type=SchemaType(result.get("schemaType", "AVRO")),
            schema_str=result["schema"],
            subject=result.get("subject", ""),
            version=result.get("version", -1),
            references=result.get("references", []),
        )

        # Cache
        self._id_to_schema[schema_id] = schema
        await self._cache.set(f"id:{schema_id}", schema)

        return schema

    async def get_latest_schema(
        self,
        topic: str,
        is_key: bool = False,
    ) -> Schema:
        """Get the latest schema version for a topic."""
        subject = self._get_subject(topic, is_key)

        # Check cache
        cached = await self._cache.get(f"latest:{subject}")
        if cached:
            return cached

        # Fetch from registry
        result = await self._request("GET", f"/subjects/{subject}/versions/latest")

        schema = Schema(
            schema_id=result["id"],
            schema_type=SchemaType(result.get("schemaType", "AVRO")),
            schema_str=result["schema"],
            subject=subject,
            version=result["version"],
            references=result.get("references", []),
        )

        # Cache
        await self._cache.set(f"latest:{subject}", schema)
        await self._cache.set(f"id:{schema.schema_id}", schema)
        self._id_to_schema[schema.schema_id] = schema

        return schema

    async def get_schema_version(
        self,
        topic: str,
        version: int,
        is_key: bool = False,
    ) -> Schema:
        """Get a specific schema version."""
        subject = self._get_subject(topic, is_key)
        cache_key = f"version:{subject}:{version}"

        # Check cache
        cached = await self._cache.get(cache_key)
        if cached:
            return cached

        # Fetch from registry
        result = await self._request("GET", f"/subjects/{subject}/versions/{version}")

        schema = Schema(
            schema_id=result["id"],
            schema_type=SchemaType(result.get("schemaType", "AVRO")),
            schema_str=result["schema"],
            subject=subject,
            version=version,
            references=result.get("references", []),
        )

        # Cache
        await self._cache.set(cache_key, schema)
        await self._cache.set(f"id:{schema.schema_id}", schema)

        return schema

    async def get_all_versions(
        self,
        topic: str,
        is_key: bool = False,
    ) -> list[int]:
        """Get all version numbers for a subject."""
        subject = self._get_subject(topic, is_key)
        return await self._request("GET", f"/subjects/{subject}/versions")

    async def delete_schema(
        self,
        topic: str,
        version: Optional[int] = None,
        is_key: bool = False,
        permanent: bool = False,
    ) -> list[int]:
        """
        Delete schema version(s).

        Args:
            topic: Topic name
            version: Specific version to delete (None = all versions)
            is_key: Key schema vs value schema
            permanent: Hard delete (requires soft delete first)
        """
        subject = self._get_subject(topic, is_key)

        if version:
            path = f"/subjects/{subject}/versions/{version}"
        else:
            path = f"/subjects/{subject}"

        if permanent:
            path += "?permanent=true"

        result = await self._request("DELETE", path)

        # Invalidate cache
        await self._cache.invalidate(f"latest:{subject}")
        if version:
            await self._cache.invalidate(f"version:{subject}:{version}")

        logger.info(
            "Schema deleted",
            extra={
                "event_type": "schema_registry.schema_deleted",
                "subject": subject,
                "version": version,
                "permanent": permanent,
                "tenant_id": self.config.tenant_id,
            },
        )

        return result if isinstance(result, list) else [result]

    # -------------------------------------------------------------------------
    # Compatibility Operations
    # -------------------------------------------------------------------------

    async def check_compatibility(
        self,
        topic: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
        is_key: bool = False,
        version: str = "latest",
    ) -> tuple[bool, Optional[list[str]]]:
        """
        Check if a schema is compatible with existing versions.

        Returns: (is_compatible, error_messages)
        """
        subject = self._get_subject(topic, is_key)

        payload = {
            "schema": schema_str,
            "schemaType": schema_type.value,
        }

        try:
            result = await self._request(
                "POST",
                f"/compatibility/subjects/{subject}/versions/{version}",
                payload,
            )
            is_compatible = result.get("is_compatible", False)
            messages = result.get("messages", [])
            return is_compatible, messages if not is_compatible else None

        except SchemaNotFoundError:
            # No existing schema, so compatible
            return True, None

    async def get_compatibility_level(
        self,
        topic: Optional[str] = None,
        is_key: bool = False,
    ) -> CompatibilityLevel:
        """Get compatibility level for subject or global."""
        if topic:
            subject = self._get_subject(topic, is_key)
            path = f"/config/{subject}"
        else:
            path = "/config"

        try:
            result = await self._request("GET", path)
            return CompatibilityLevel(result.get("compatibilityLevel", "BACKWARD"))
        except SchemaNotFoundError:
            return CompatibilityLevel.BACKWARD

    async def set_compatibility_level(
        self,
        level: CompatibilityLevel,
        topic: Optional[str] = None,
        is_key: bool = False,
    ) -> None:
        """Set compatibility level for subject or global."""
        if topic:
            subject = self._get_subject(topic, is_key)
            path = f"/config/{subject}"
        else:
            path = "/config"

        await self._request("PUT", path, {"compatibility": level.value})

        logger.info(
            "Compatibility level set",
            extra={
                "event_type": "schema_registry.compatibility_set",
                "subject": topic,
                "level": level.value,
                "tenant_id": self.config.tenant_id,
            },
        )

    # -------------------------------------------------------------------------
    # Validation Operations
    # -------------------------------------------------------------------------

    async def validate_avro(
        self,
        schema_id: int,
        data: dict,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate data against an Avro schema.

        Returns: (is_valid, error_message)
        """
        try:
            import fastavro
            from io import BytesIO

            schema = await self.get_schema_by_id(schema_id)
            parsed_schema = fastavro.parse_schema(json.loads(schema.schema_str))

            # Try to serialize (validates the data)
            buffer = BytesIO()
            fastavro.schemaless_writer(buffer, parsed_schema, data)

            return True, None

        except ImportError:
            logger.warning("fastavro not installed, skipping Avro validation")
            return True, None
        except Exception as e:
            return False, str(e)

    async def validate_json_schema(
        self,
        schema_id: int,
        data: dict,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate data against a JSON Schema.

        Returns: (is_valid, error_message)
        """
        schema = await self.get_schema_by_id(schema_id)
        if schema.schema_type != SchemaType.JSON:
            return False, f"Schema {schema_id} is not JSON Schema type"

        return self._json_validator.validate(schema.schema_str, data)

    async def validate(
        self,
        schema_id: int,
        data: dict,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate data against schema (auto-detects type).

        Returns: (is_valid, error_message)
        """
        schema = await self.get_schema_by_id(schema_id)

        if schema.schema_type == SchemaType.AVRO:
            return await self.validate_avro(schema_id, data)
        elif schema.schema_type == SchemaType.JSON:
            return await self.validate_json_schema(schema_id, data)
        else:
            return False, f"Unsupported schema type: {schema.schema_type}"

    # -------------------------------------------------------------------------
    # Serialization Operations
    # -------------------------------------------------------------------------

    async def serialize_avro(
        self,
        topic: str,
        data: dict,
        is_key: bool = False,
    ) -> bytes:
        """
        Serialize data with Avro schema and wire format.

        Returns bytes in Confluent wire format.
        """
        try:
            import fastavro
            from io import BytesIO

            schema = await self.get_latest_schema(topic, is_key)
            parsed_schema = fastavro.parse_schema(json.loads(schema.schema_str))

            buffer = BytesIO()
            fastavro.schemaless_writer(buffer, parsed_schema, data)
            payload = buffer.getvalue()

            return AvroSerializer.encode_message(schema.schema_id, payload)

        except ImportError:
            raise RuntimeError("fastavro required for Avro serialization")

    async def deserialize_avro(
        self,
        data: bytes,
    ) -> tuple[int, dict]:
        """
        Deserialize Avro data with wire format.

        Returns: (schema_id, deserialized_data)
        """
        try:
            import fastavro
            from io import BytesIO

            schema_id, payload = AvroSerializer.decode_message(data)
            schema = await self.get_schema_by_id(schema_id)
            parsed_schema = fastavro.parse_schema(json.loads(schema.schema_str))

            buffer = BytesIO(payload)
            result = fastavro.schemaless_reader(buffer, parsed_schema)

            return schema_id, result

        except ImportError:
            raise RuntimeError("fastavro required for Avro deserialization")

    # -------------------------------------------------------------------------
    # Subject Operations
    # -------------------------------------------------------------------------

    async def list_subjects(self) -> list[str]:
        """List all subjects in the registry."""
        subjects = await self._request("GET", "/subjects")

        # Filter by tenant prefix if configured
        if self.config.tenant_id:
            prefix = f"{self.config.tenant_id}."
            subjects = [s for s in subjects if s.startswith(prefix)]
        elif self.config.subject_prefix:
            prefix = f"{self.config.subject_prefix}."
            subjects = [s for s in subjects if s.startswith(prefix)]

        return subjects

    async def get_subjects_for_schema(self, schema_id: int) -> list[str]:
        """Get all subjects using a schema ID."""
        return await self._request("GET", f"/schemas/ids/{schema_id}/subjects")

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------

    def get_metrics(self) -> dict:
        """Get connector metrics."""
        return {
            **self._metrics,
            "cache_size": len(self._cache._cache),
            "schemas_cached": len(self._id_to_schema),
            "circuit_state": self._circuit_state.value,
            "tenant_id": self.config.tenant_id,
        }


class SchemaNotFoundError(Exception):
    """Schema or subject not found."""
    pass


class IncompatibleSchemaError(Exception):
    """Schema is incompatible with existing versions."""
    pass


# Singleton instance management
_schema_registry_instances: dict[str, SchemaRegistryConnector] = {}
_lock = asyncio.Lock()


async def get_schema_registry_connector(
    config: Optional[SchemaRegistryConfig] = None,
    instance_key: str = "default",
) -> SchemaRegistryConnector:
    """
    Get or create a Schema Registry connector instance.

    Args:
        config: Configuration (required for first call)
        instance_key: Key for managing multiple instances
    """
    async with _lock:
        if instance_key not in _schema_registry_instances:
            if config is None:
                raise ValueError("Config required for first initialization")

            connector = SchemaRegistryConnector(config)
            await connector.connect()
            _schema_registry_instances[instance_key] = connector

        return _schema_registry_instances[instance_key]


async def close_schema_registry_connector(instance_key: str = "default") -> None:
    """Close and remove a connector instance."""
    async with _lock:
        if instance_key in _schema_registry_instances:
            await _schema_registry_instances[instance_key].close()
            del _schema_registry_instances[instance_key]

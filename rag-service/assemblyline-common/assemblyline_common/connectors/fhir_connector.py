"""
Enterprise Epic FHIR Connector.

Features:
- JWT assertion authentication with rotating keys
- Token caching with automatic refresh
- Batch/transaction support
- Pagination handling
- SMART on FHIR scopes
- PHI access audit logging
- Circuit breaker

Usage:
    from assemblyline_common.connectors import (
        get_fhir_connector,
        FHIRConnectorConfig,
    )

    connector = await get_fhir_connector(FHIRConnectorConfig(
        base_url="https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4",
        client_id="my-app-id",
        private_key_path="/path/to/private_key.pem",
    ))

    # Read a patient
    patient = await connector.read("Patient", "12345")

    # Search for patients
    patients = await connector.search("Patient", {"family": "Smith"})

    # Create a resource
    result = await connector.create("Observation", observation_data)
"""

import asyncio
import base64
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, AsyncIterator
from uuid import uuid4

import httpx
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
import jwt

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
)
from assemblyline_common.retry import RetryHandler, HTTP_RETRY_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class FHIRConnectorConfig:
    """Configuration for FHIR connector."""
    # Epic FHIR server settings
    base_url: str = ""
    token_url: Optional[str] = None  # OAuth token endpoint

    # Client credentials
    client_id: str = ""
    private_key_path: Optional[str] = None
    private_key_content: Optional[str] = None  # PEM content directly

    # SMART on FHIR scopes
    scopes: List[str] = field(default_factory=lambda: [
        "system/Patient.read",
        "system/Observation.read",
        "system/Observation.write",
    ])

    # Token settings
    token_lifetime_seconds: int = 300  # 5 minutes
    token_refresh_margin_seconds: int = 60

    # Request settings
    timeout: float = 30.0
    max_retries: int = 3

    # Pagination
    default_page_size: int = 100
    max_page_size: int = 1000

    # Circuit breaker
    enable_circuit_breaker: bool = True

    # Audit logging
    enable_audit_logging: bool = True

    # Tenant settings
    tenant_id: Optional[str] = None

    # JWKS managed key (for JKU-based Epic Backend Services)
    jwks_kid: Optional[str] = None  # Key ID from managed JWKS key pair
    jwks_url: Optional[str] = None  # JKU URL for JWT header


@dataclass
class FHIRResource:
    """FHIR resource wrapper."""
    resource_type: str
    id: Optional[str]
    data: Dict[str, Any]
    meta: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FHIRResource":
        """Create from dictionary."""
        return cls(
            resource_type=data.get("resourceType", ""),
            id=data.get("id"),
            data=data,
            meta=data.get("meta"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.data


@dataclass
class FHIRBundle:
    """FHIR bundle for batch operations."""
    type: str  # searchset, batch, transaction, etc.
    entries: List[FHIRResource]
    total: Optional[int] = None
    link: List[Dict[str, str]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FHIRBundle":
        """Create from dictionary."""
        entries = [
            FHIRResource.from_dict(e.get("resource", {}))
            for e in data.get("entry", [])
            if "resource" in e
        ]
        return cls(
            type=data.get("type", ""),
            entries=entries,
            total=data.get("total"),
            link=data.get("link", []),
        )

    def get_next_link(self) -> Optional[str]:
        """Get URL for next page."""
        for link in self.link:
            if link.get("relation") == "next":
                return link.get("url")
        return None


class FHIRTokenManager:
    """Manages JWT tokens for Epic FHIR authentication."""

    def __init__(
        self,
        client_id: str,
        token_url: str,
        private_key_path: Optional[str] = None,
        private_key_content: Optional[str] = None,
        scopes: Optional[List[str]] = None,
        token_lifetime: int = 300,
        refresh_margin: int = 60,
        kid: Optional[str] = None,
        jku: Optional[str] = None,
    ):
        self.client_id = client_id
        self.token_url = token_url
        self.scopes = scopes or []
        self.token_lifetime = token_lifetime
        self.refresh_margin = refresh_margin
        self.kid = kid    # Key ID for JWT header (JWKS managed keys)
        self.jku = jku    # JWK Set URL for JWT header

        # Load private key
        if private_key_content:
            self._private_key = serialization.load_pem_private_key(
                private_key_content.encode(),
                password=None,
                backend=default_backend(),
            )
        elif private_key_path:
            with open(private_key_path, "rb") as f:
                self._private_key = serialization.load_pem_private_key(
                    f.read(),
                    password=None,
                    backend=default_backend(),
                )
        else:
            self._private_key = None

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0
        self._lock = asyncio.Lock()

    def _create_jwt_assertion(self) -> str:
        """Create a signed JWT assertion for client credentials flow."""
        now = datetime.now(timezone.utc)
        # Epic requires exp no more than 5 minutes from now
        lifetime = min(self.token_lifetime, 300)
        exp = now.timestamp() + lifetime

        payload = {
            "iss": self.client_id,
            "sub": self.client_id,
            "aud": self.token_url,
            "jti": str(uuid4()),
            "nbf": int(now.timestamp()),
            "iat": int(now.timestamp()),
            "exp": int(exp),
        }

        # Sign with RS384 (required by Epic)
        # Include typ/kid/jku headers (Epic Backend Services OAuth 2.0 spec)
        jwt_headers = {"typ": "JWT"}
        if self.kid:
            jwt_headers["kid"] = self.kid
        if self.jku:
            jwt_headers["jku"] = self.jku

        token = jwt.encode(
            payload,
            self._private_key,
            algorithm="RS384",
            headers=jwt_headers,
        )

        return token

    async def get_token(self, client: httpx.AsyncClient) -> str:
        """Get a valid access token, refreshing if needed."""
        async with self._lock:
            # Check if token is still valid
            if self._access_token and time.time() < (self._token_expires_at - self.refresh_margin):
                return self._access_token

            # Create JWT assertion
            assertion = self._create_jwt_assertion()

            # Request access token
            data = {
                "grant_type": "client_credentials",
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": assertion,
            }

            if self.scopes:
                data["scope"] = " ".join(self.scopes)

            response = await client.post(
                self.token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

            token_data = response.json()
            self._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", self.token_lifetime)
            self._token_expires_at = time.time() + expires_in

            logger.info(
                "FHIR token refreshed",
                extra={
                    "event_type": "fhir_token_refreshed",
                    "expires_in": expires_in,
                }
            )

            return self._access_token


class FHIRConnector:
    """
    Enterprise Epic FHIR connector.

    Features:
    - JWT authentication
    - Automatic token refresh
    - Pagination handling
    - Batch/transaction support
    - Circuit breaker
    - PHI audit logging
    """

    def __init__(
        self,
        config: FHIRConnectorConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._client: Optional[httpx.AsyncClient] = None
        self._token_manager: Optional[FHIRTokenManager] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._metrics: Dict[str, int] = {
            "reads": 0,
            "searches": 0,
            "creates": 0,
            "updates": 0,
            "errors": 0,
        }
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout, connect=10.0),
            headers={
                "Accept": "application/fhir+json",
                "Content-Type": "application/fhir+json",
            },
        )

        # Initialize token manager
        token_url = self.config.token_url or f"{self.config.base_url.rstrip('/')}/oauth2/token"
        self._token_manager = FHIRTokenManager(
            client_id=self.config.client_id,
            token_url=token_url,
            private_key_path=self.config.private_key_path,
            private_key_content=self.config.private_key_content,
            scopes=self.config.scopes,
            token_lifetime=self.config.token_lifetime_seconds,
            refresh_margin=self.config.token_refresh_margin_seconds,
            kid=self.config.jwks_kid,
            jku=self.config.jwks_url,
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        self._retry_handler = RetryHandler(config=HTTP_RETRY_CONFIG)

        logger.info(
            "FHIR connector initialized",
            extra={
                "event_type": "fhir_initialized",
                "base_url": self.config.base_url,
                "client_id": self.config.client_id,
            }
        )

    async def _get_auth_headers(self) -> Dict[str, str]:
        """Get authorization headers with valid token."""
        token = await self._token_manager.get_token(self._client)
        return {"Authorization": f"Bearer {token}"}

    def _get_url(self, resource_type: str, resource_id: Optional[str] = None) -> str:
        """Build FHIR URL."""
        base = self.config.base_url.rstrip("/")
        if resource_id:
            return f"{base}/{resource_type}/{resource_id}"
        return f"{base}/{resource_type}"

    async def _request(
        self,
        method: str,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make authenticated FHIR request."""
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._client:
            await self.initialize()

        circuit_name = f"fhir:{self.config.base_url}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        headers = await self._get_auth_headers()

        async def do_request() -> Dict[str, Any]:
            response = await self._client.request(
                method=method,
                url=url,
                json=data,
                params=params,
                headers=headers,
            )
            response.raise_for_status()

            if response.status_code == 204:
                return {}

            return response.json()

        try:
            if self._retry_handler:
                result = await self._retry_handler.execute(
                    do_request,
                    operation_id=f"fhir-{method}-{url}",
                )
            else:
                result = await do_request()

            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            return result

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    def _log_phi_access(
        self,
        operation: str,
        resource_type: str,
        resource_id: Optional[str] = None,
        patient_id: Optional[str] = None,
    ) -> None:
        """Log PHI access for audit purposes."""
        if not self.config.enable_audit_logging:
            return

        logger.info(
            f"PHI access: {operation} {resource_type}",
            extra={
                "event_type": "phi_access",
                "operation": operation,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "patient_id": patient_id,
                "tenant_id": self.config.tenant_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    async def read(
        self,
        resource_type: str,
        resource_id: str,
    ) -> FHIRResource:
        """
        Read a FHIR resource by ID.

        Args:
            resource_type: FHIR resource type (Patient, Observation, etc.)
            resource_id: Resource ID

        Returns:
            FHIRResource object
        """
        url = self._get_url(resource_type, resource_id)
        data = await self._request("GET", url)

        self._metrics["reads"] += 1

        # Extract patient ID for audit
        patient_id = None
        if resource_type == "Patient":
            patient_id = resource_id
        elif "subject" in data:
            ref = data["subject"].get("reference", "")
            if ref.startswith("Patient/"):
                patient_id = ref.split("/")[1]

        self._log_phi_access("read", resource_type, resource_id, patient_id)

        return FHIRResource.from_dict(data)

    async def search(
        self,
        resource_type: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: Optional[int] = None,
    ) -> FHIRBundle:
        """
        Search for FHIR resources.

        Args:
            resource_type: FHIR resource type
            params: Search parameters
            page_size: Results per page

        Returns:
            FHIRBundle with results
        """
        url = self._get_url(resource_type)
        search_params = params or {}
        search_params["_count"] = page_size or self.config.default_page_size

        data = await self._request("GET", url, params=search_params)

        self._metrics["searches"] += 1
        self._log_phi_access("search", resource_type)

        return FHIRBundle.from_dict(data)

    async def search_all(
        self,
        resource_type: str,
        params: Optional[Dict[str, Any]] = None,
        max_results: Optional[int] = None,
    ) -> AsyncIterator[FHIRResource]:
        """
        Search with automatic pagination.

        Yields resources across all pages.
        """
        page_size = self.config.default_page_size
        search_params = params or {}
        search_params["_count"] = page_size

        url = self._get_url(resource_type)
        total_yielded = 0

        while url:
            if url.startswith("http"):
                # Absolute URL from pagination
                data = await self._request("GET", url)
            else:
                data = await self._request("GET", url, params=search_params)

            bundle = FHIRBundle.from_dict(data)

            for entry in bundle.entries:
                yield entry
                total_yielded += 1

                if max_results and total_yielded >= max_results:
                    return

            # Get next page URL
            url = bundle.get_next_link()
            search_params = {}  # Clear params for subsequent requests

    async def create(
        self,
        resource_type: str,
        data: Dict[str, Any],
    ) -> FHIRResource:
        """
        Create a new FHIR resource.

        Args:
            resource_type: FHIR resource type
            data: Resource data

        Returns:
            Created FHIRResource with ID
        """
        url = self._get_url(resource_type)
        data["resourceType"] = resource_type

        result = await self._request("POST", url, data=data)

        self._metrics["creates"] += 1
        self._log_phi_access("create", resource_type, result.get("id"))

        return FHIRResource.from_dict(result)

    async def update(
        self,
        resource_type: str,
        resource_id: str,
        data: Dict[str, Any],
    ) -> FHIRResource:
        """
        Update an existing FHIR resource.

        Args:
            resource_type: FHIR resource type
            resource_id: Resource ID
            data: Updated resource data

        Returns:
            Updated FHIRResource
        """
        url = self._get_url(resource_type, resource_id)
        data["resourceType"] = resource_type
        data["id"] = resource_id

        result = await self._request("PUT", url, data=data)

        self._metrics["updates"] += 1
        self._log_phi_access("update", resource_type, resource_id)

        return FHIRResource.from_dict(result)

    async def delete(
        self,
        resource_type: str,
        resource_id: str,
    ) -> None:
        """Delete a FHIR resource."""
        url = self._get_url(resource_type, resource_id)
        await self._request("DELETE", url)

        self._log_phi_access("delete", resource_type, resource_id)

    async def batch(
        self,
        entries: List[Dict[str, Any]],
    ) -> FHIRBundle:
        """
        Execute a batch of operations.

        Args:
            entries: List of bundle entries with request info

        Returns:
            FHIRBundle with results
        """
        bundle = {
            "resourceType": "Bundle",
            "type": "batch",
            "entry": entries,
        }

        url = self.config.base_url.rstrip("/")
        result = await self._request("POST", url, data=bundle)

        return FHIRBundle.from_dict(result)

    async def transaction(
        self,
        entries: List[Dict[str, Any]],
    ) -> FHIRBundle:
        """
        Execute a transaction (atomic batch).

        Args:
            entries: List of bundle entries with request info

        Returns:
            FHIRBundle with results
        """
        bundle = {
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": entries,
        }

        url = self.config.base_url.rstrip("/")
        result = await self._request("POST", url, data=bundle)

        return FHIRBundle.from_dict(result)

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return self._metrics

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("FHIR connector closed")


# Singleton instances
_fhir_connectors: Dict[str, FHIRConnector] = {}
_fhir_lock = asyncio.Lock()


async def get_fhir_connector(
    config: Optional[FHIRConnectorConfig] = None,
    name: Optional[str] = None,
) -> FHIRConnector:
    """Get or create a FHIR connector."""
    config = config or FHIRConnectorConfig()
    connector_name = name or f"fhir-{config.client_id}"

    if connector_name in _fhir_connectors:
        return _fhir_connectors[connector_name]

    async with _fhir_lock:
        if connector_name in _fhir_connectors:
            return _fhir_connectors[connector_name]

        connector = FHIRConnector(config)
        await connector.initialize()
        _fhir_connectors[connector_name] = connector

        return connector


async def close_all_fhir_connectors() -> None:
    """Close all FHIR connectors."""
    for connector in _fhir_connectors.values():
        await connector.close()
    _fhir_connectors.clear()

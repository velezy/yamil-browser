"""
Enterprise HTTP/REST Connector.

Features:
- HTTP/2 multiplexing and connection pooling
- Circuit breaker integration
- Retry with exponential backoff
- mTLS (mutual TLS) support
- Request signing (AWS Sig V4, HMAC)
- Client-side rate limiting
- OAuth 2.0 client credentials flow

Usage:
    from assemblyline_common.connectors import get_http_connector, HTTPConnectorConfig

    connector = await get_http_connector(
        config=HTTPConnectorConfig(
            base_url="https://api.example.com",
            enable_circuit_breaker=True,
        )
    )

    response = await connector.get("/users/123")
"""

import asyncio
import base64
import hashlib
import hmac
import logging
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Union
from urllib.parse import urlparse, urlencode

import httpx

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
    CircuitBreakerConfig,
)
from assemblyline_common.retry import (
    RetryHandler,
    RetryConfig,
    HTTP_RETRY_CONFIG,
)

logger = logging.getLogger(__name__)


@dataclass
class HTTPConnectorConfig:
    """Configuration for HTTP connector."""
    # Base URL for all requests
    base_url: str = ""
    # Request timeout in seconds
    timeout: float = 30.0
    # Connection timeout in seconds
    connect_timeout: float = 10.0
    # Maximum connections per host
    max_connections: int = 100
    # Maximum keepalive connections
    max_keepalive_connections: int = 20
    # Keepalive expiry in seconds
    keepalive_expiry: float = 60.0
    # Enable HTTP/2
    http2: bool = True

    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None

    # Retry configuration
    enable_retry: bool = True
    retry_config: Optional[RetryConfig] = None

    # mTLS configuration
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None
    ca_cert_path: Optional[str] = None
    verify_ssl: bool = True

    # Authentication
    bearer_token: Optional[str] = None
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"
    basic_auth: Optional[tuple[str, str]] = None

    # OAuth 2.0 client credentials
    oauth_token_url: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_scopes: List[str] = field(default_factory=list)

    # Request signing
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_region: str = "us-east-1"
    aws_service: str = "execute-api"

    # Rate limiting
    enable_rate_limiting: bool = False
    rate_limit_requests: int = 100
    rate_limit_window_seconds: int = 60

    # Default headers
    default_headers: Dict[str, str] = field(default_factory=dict)

    # Tenant ID for isolation
    tenant_id: Optional[str] = None


@dataclass
class HTTPResponse:
    """HTTP response wrapper."""
    status_code: int
    headers: Dict[str, str]
    body: bytes
    elapsed_ms: float
    request_id: Optional[str] = None

    @property
    def text(self) -> str:
        """Get response body as text."""
        return self.body.decode('utf-8')

    @property
    def json(self) -> Any:
        """Parse response body as JSON."""
        import json
        return json.loads(self.body)

    @property
    def ok(self) -> bool:
        """Check if response was successful (2xx)."""
        return 200 <= self.status_code < 300


class TokenBucket:
    """Simple token bucket rate limiter."""

    def __init__(self, tokens: int, refill_seconds: int):
        self.capacity = tokens
        self.tokens = tokens
        self.refill_rate = tokens / refill_seconds
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens. Returns True if successful."""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last_refill = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    async def wait_and_acquire(self, tokens: int = 1, timeout: float = 30.0) -> bool:
        """Wait for tokens to become available."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if await self.acquire(tokens):
                return True
            await asyncio.sleep(0.1)
        return False


class OAuthTokenManager:
    """Manages OAuth 2.0 tokens with automatic refresh."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scopes: List[str],
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes

        self._access_token: Optional[str] = None
        self._expires_at: float = 0
        self._lock = asyncio.Lock()

    async def get_token(self, client: httpx.AsyncClient) -> str:
        """Get a valid access token, refreshing if needed."""
        async with self._lock:
            # Check if token is still valid (with 60s buffer)
            if self._access_token and time.time() < (self._expires_at - 60):
                return self._access_token

            # Request new token
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
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
            expires_in = token_data.get("expires_in", 3600)
            self._expires_at = time.time() + expires_in

            logger.debug("OAuth token refreshed", extra={"expires_in": expires_in})

            return self._access_token


class AWSSignatureV4:
    """AWS Signature Version 4 signing."""

    def __init__(self, access_key: str, secret_key: str, region: str, service: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def sign_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: bytes = b"",
    ) -> Dict[str, str]:
        """Sign a request and return updated headers."""
        parsed = urlparse(url)
        host = parsed.netloc
        path = parsed.path or "/"
        query = parsed.query

        # Get current time
        t = datetime.now(timezone.utc)
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')

        # Create canonical request
        signed_headers = "host;x-amz-date"
        canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"

        payload_hash = hashlib.sha256(body).hexdigest()

        canonical_request = '\n'.join([
            method.upper(),
            path,
            query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])

        # Create string to sign
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = '\n'.join([
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ])

        # Calculate signature
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode(), hashlib.sha256).digest()

        k_date = sign(f"AWS4{self.secret_key}".encode(), date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        k_signing = sign(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

        # Create authorization header
        authorization = (
            f"{algorithm} Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        return {
            **headers,
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            "Authorization": authorization,
        }


class HTTPConnector:
    """
    Enterprise HTTP connector with resilience patterns.

    Integrates:
    - Circuit breaker for fault tolerance
    - Retry with exponential backoff
    - Connection pooling with HTTP/2
    - mTLS support
    - OAuth 2.0 and AWS Sig V4
    - Client-side rate limiting
    """

    def __init__(
        self,
        config: HTTPConnectorConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._client: Optional[httpx.AsyncClient] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._rate_limiter: Optional[TokenBucket] = None
        self._oauth_manager: Optional[OAuthTokenManager] = None
        self._aws_signer: Optional[AWSSignatureV4] = None
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        # Create SSL context if mTLS configured
        ssl_context = None
        if self.config.client_cert_path:
            ssl_context = ssl.create_default_context()
            ssl_context.load_cert_chain(
                self.config.client_cert_path,
                self.config.client_key_path,
            )
            if self.config.ca_cert_path:
                ssl_context.load_verify_locations(self.config.ca_cert_path)

        # Create HTTP client
        limits = httpx.Limits(
            max_connections=self.config.max_connections,
            max_keepalive_connections=self.config.max_keepalive_connections,
            keepalive_expiry=self.config.keepalive_expiry,
        )

        timeout = httpx.Timeout(
            timeout=self.config.timeout,
            connect=self.config.connect_timeout,
        )

        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            limits=limits,
            timeout=timeout,
            http2=self.config.http2,
            verify=ssl_context if ssl_context else self.config.verify_ssl,
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker(
                self.config.circuit_breaker_config
            )

        # Initialize retry handler
        if self.config.enable_retry:
            self._retry_handler = RetryHandler(
                config=self.config.retry_config or HTTP_RETRY_CONFIG
            )

        # Initialize rate limiter
        if self.config.enable_rate_limiting:
            self._rate_limiter = TokenBucket(
                self.config.rate_limit_requests,
                self.config.rate_limit_window_seconds,
            )

        # Initialize OAuth manager
        if self.config.oauth_token_url:
            self._oauth_manager = OAuthTokenManager(
                token_url=self.config.oauth_token_url,
                client_id=self.config.oauth_client_id or "",
                client_secret=self.config.oauth_client_secret or "",
                scopes=self.config.oauth_scopes,
            )

        # Initialize AWS signer
        if self.config.aws_access_key:
            self._aws_signer = AWSSignatureV4(
                access_key=self.config.aws_access_key,
                secret_key=self.config.aws_secret_key or "",
                region=self.config.aws_region,
                service=self.config.aws_service,
            )

        logger.info(
            "HTTP connector initialized",
            extra={
                "event_type": "http_connector_initialized",
                "base_url": self.config.base_url,
                "http2": self.config.http2,
                "circuit_breaker": self.config.enable_circuit_breaker,
                "retry": self.config.enable_retry,
                "mtls": bool(self.config.client_cert_path),
            }
        )

    async def _prepare_headers(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]],
        body: bytes,
    ) -> Dict[str, str]:
        """Prepare request headers with auth and signing."""
        result = {**self.config.default_headers}

        if headers:
            result.update(headers)

        # Bearer token
        if self.config.bearer_token:
            result["Authorization"] = f"Bearer {self.config.bearer_token}"

        # API key
        if self.config.api_key:
            result[self.config.api_key_header] = self.config.api_key

        # Basic auth
        if self.config.basic_auth:
            credentials = base64.b64encode(
                f"{self.config.basic_auth[0]}:{self.config.basic_auth[1]}".encode()
            ).decode()
            result["Authorization"] = f"Basic {credentials}"

        # OAuth 2.0
        if self._oauth_manager and self._client:
            token = await self._oauth_manager.get_token(self._client)
            result["Authorization"] = f"Bearer {token}"

        # AWS Signature V4
        if self._aws_signer:
            full_url = f"{self.config.base_url}{url}" if not url.startswith("http") else url
            result = self._aws_signer.sign_request(method, full_url, result, body)

        return result

    def _get_circuit_name(self, url: str) -> str:
        """Get circuit breaker name for URL."""
        parsed = urlparse(url if url.startswith("http") else f"{self.config.base_url}{url}")
        return f"http:{parsed.netloc}{parsed.path}"

    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Union[str, bytes, Dict[str, Any]]] = None,
        timeout: Optional[float] = None,
    ) -> HTTPResponse:
        """
        Make an HTTP request with all resilience patterns.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            url: URL path (relative to base_url or absolute)
            headers: Additional headers
            params: Query parameters
            json: JSON body
            data: Form data or raw body
            timeout: Request-specific timeout

        Returns:
            HTTPResponse object

        Raises:
            CircuitOpenError: If circuit breaker is open
            httpx.HTTPError: On request failure after retries
        """
        if not self._client:
            await self.initialize()

        # Rate limiting
        if self._rate_limiter:
            if not await self._rate_limiter.wait_and_acquire():
                raise httpx.HTTPStatusError(
                    "Rate limit exceeded",
                    request=None,
                    response=None,
                )

        # Get circuit name
        circuit_name = self._get_circuit_name(url)

        # Check circuit breaker
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                retry_after = await self._circuit_breaker.get_retry_after(circuit_name)
                raise CircuitOpenError(circuit_name, retry_after)

        # Prepare body
        body = b""
        if json:
            import json as json_lib
            body = json_lib.dumps(json).encode()
        elif data and isinstance(data, (str, bytes)):
            body = data.encode() if isinstance(data, str) else data

        # Prepare headers
        request_headers = await self._prepare_headers(method, url, headers, body)

        async def make_request() -> HTTPResponse:
            """Inner request function for retry."""
            start_time = time.time()

            response = await self._client.request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                json=json if json else None,
                data=data if data and not json else None,
                timeout=timeout,
            )

            elapsed_ms = (time.time() - start_time) * 1000

            return HTTPResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                body=response.content,
                elapsed_ms=elapsed_ms,
                request_id=response.headers.get("x-request-id"),
            )

        try:
            # Execute with retry
            if self._retry_handler:
                result = await self._retry_handler.execute(
                    make_request,
                    operation_id=f"{method}:{circuit_name}",
                )
            else:
                result = await make_request()

            # Record success
            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            logger.debug(
                f"HTTP request completed",
                extra={
                    "event_type": "http_request_completed",
                    "method": method,
                    "url": url,
                    "status_code": result.status_code,
                    "elapsed_ms": result.elapsed_ms,
                }
            )

            return result

        except Exception as e:
            # Record failure
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)

            logger.warning(
                f"HTTP request failed",
                extra={
                    "event_type": "http_request_failed",
                    "method": method,
                    "url": url,
                    "error": str(e),
                }
            )
            raise

    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Make a GET request."""
        return await self.request("GET", url, headers=headers, params=params, **kwargs)

    async def post(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Union[str, bytes, Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Make a POST request."""
        return await self.request("POST", url, headers=headers, json=json, data=data, **kwargs)

    async def put(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Union[str, bytes, Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Make a PUT request."""
        return await self.request("PUT", url, headers=headers, json=json, data=data, **kwargs)

    async def delete(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Make a DELETE request."""
        return await self.request("DELETE", url, headers=headers, **kwargs)

    async def patch(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Make a PATCH request."""
        return await self.request("PATCH", url, headers=headers, json=json, **kwargs)

    async def close(self) -> None:
        """Close the connector and release resources."""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._closed = True

        logger.info("HTTP connector closed")

    async def __aenter__(self) -> "HTTPConnector":
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()


# Singleton connectors per configuration
_http_connectors: Dict[str, HTTPConnector] = {}
_connector_lock = asyncio.Lock()


async def get_http_connector(
    config: Optional[HTTPConnectorConfig] = None,
    name: Optional[str] = None,
) -> HTTPConnector:
    """
    Get or create an HTTP connector.

    Args:
        config: Connector configuration
        name: Unique name for this connector (for caching)

    Returns:
        HTTPConnector instance
    """
    config = config or HTTPConnectorConfig()
    connector_name = name or config.base_url or "default"

    if connector_name in _http_connectors:
        return _http_connectors[connector_name]

    async with _connector_lock:
        if connector_name in _http_connectors:
            return _http_connectors[connector_name]

        connector = HTTPConnector(config)
        await connector.initialize()
        _http_connectors[connector_name] = connector

        return connector


async def close_all_http_connectors() -> None:
    """Close all HTTP connectors."""
    for connector in _http_connectors.values():
        await connector.close()
    _http_connectors.clear()

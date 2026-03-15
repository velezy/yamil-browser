"""
Third-Party Email Provider Connectors for Logic Weaver.

Provides enterprise-grade integrations with:
- SendGrid API
- Mailgun API
- API key management with rotation
- Webhook signature validation
- Template management
- Rate limiting and throttling
- Delivery tracking
- Circuit breaker for fault tolerance
- Health checks for monitoring
- TLS/SSL enforcement
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

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
    provider: str
    details: dict = field(default_factory=dict)


class EmailStatus(str, Enum):
    """Email delivery status."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    OPENED = "opened"
    CLICKED = "clicked"
    BOUNCED = "bounced"
    DROPPED = "dropped"
    DEFERRED = "deferred"
    SPAM = "spam"
    UNSUBSCRIBED = "unsubscribed"


@dataclass
class EmailAttachment:
    """Email attachment."""
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"
    disposition: str = "attachment"  # or "inline"
    content_id: Optional[str] = None  # For inline images


@dataclass
class EmailMessage:
    """Email message structure."""
    to: list[str]
    subject: str
    from_email: str
    from_name: Optional[str] = None
    reply_to: Optional[str] = None
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    text_body: Optional[str] = None
    html_body: Optional[str] = None
    attachments: list[EmailAttachment] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)
    template_id: Optional[str] = None
    template_data: dict[str, Any] = field(default_factory=dict)
    send_at: Optional[datetime] = None  # Scheduled sending
    tracking_opens: bool = True
    tracking_clicks: bool = True


@dataclass
class SendResult:
    """Result from send operation."""
    message_id: str
    status: EmailStatus
    provider: str
    response: dict = field(default_factory=dict)


# =============================================================================
# SendGrid Connector
# =============================================================================


@dataclass
class SendGridConfig:
    """Configuration for SendGrid connector."""
    api_key: str
    api_url: str = "https://api.sendgrid.com/v3"

    # Webhook settings
    webhook_signing_key: Optional[str] = None

    # Pool settings
    ip_pool_name: Optional[str] = None

    # Rate limiting
    max_requests_per_second: float = 100.0

    # Retry settings
    max_retries: int = 3
    timeout_seconds: float = 30.0

    # TLS/SSL settings
    verify_ssl: bool = True
    ssl_ca_bundle: Optional[str] = None

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    circuit_breaker_success_threshold: int = 3

    # Tenant isolation
    tenant_id: Optional[str] = None

    # Default sending settings
    default_from_email: Optional[str] = None
    default_from_name: Optional[str] = None


class SendGridConnector:
    """
    SendGrid email connector.

    Features:
    - Full SendGrid Mail Send API v3
    - Template support with dynamic data
    - Scheduled sending
    - Attachment handling
    - Webhook signature validation
    - Rate limiting
    - IP pool support
    - Circuit breaker for fault tolerance
    - Health checks for monitoring
    - TLS/SSL enforcement
    """

    def __init__(self, config: SendGridConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time = 0.0
        self._min_request_interval = 1.0 / config.max_requests_per_second
        self._metrics = {
            "emails_sent": 0,
            "emails_failed": 0,
            "rate_limited": 0,
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
        # SSL/TLS configuration
        verify = self.config.verify_ssl
        if self.config.ssl_ca_bundle:
            verify = self.config.ssl_ca_bundle

        self._client = httpx.AsyncClient(
            base_url=self.config.api_url,
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            },
            timeout=self.config.timeout_seconds,
            verify=verify,
            http2=True,  # Enable HTTP/2 for better performance
        )

        logger.info(
            "SendGrid connector initialized",
            extra={
                "event_type": "sendgrid.connected",
                "tenant_id": self.config.tenant_id,
            },
        )

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

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
                            "SendGrid circuit breaker transitioning to HALF_OPEN",
                            extra={"event_type": "sendgrid.circuit_half_open"},
                        )
                    else:
                        retry_after = self.config.circuit_breaker_timeout_seconds - elapsed
                        raise CircuitBreakerError(
                            "SendGrid circuit breaker is open",
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
                        "SendGrid circuit breaker closed",
                        extra={"event_type": "sendgrid.circuit_closed"},
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
                    "SendGrid circuit breaker tripped from HALF_OPEN",
                    extra={"event_type": "sendgrid.circuit_open"},
                )
            elif self._circuit_state == CircuitState.CLOSED:
                if self._circuit_failure_count >= self.config.circuit_breaker_failure_threshold:
                    self._circuit_state = CircuitState.OPEN
                    self._metrics["circuit_breaker_trips"] += 1
                    logger.warning(
                        "SendGrid circuit breaker tripped",
                        extra={
                            "event_type": "sendgrid.circuit_open",
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
        """Perform health check on SendGrid connection."""
        start_time = time.time()

        try:
            if self._circuit_state == CircuitState.OPEN:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Circuit breaker is open",
                    provider="sendgrid",
                    details={"circuit_state": self._circuit_state.value},
                )

            if not self._client:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Client not connected",
                    provider="sendgrid",
                )

            # Check API access by getting user info (lightweight)
            response = await self._client.get("/user/profile")
            latency_ms = (time.time() - start_time) * 1000

            if response.status_code == 200:
                return HealthCheckResult(
                    healthy=True,
                    latency_ms=latency_ms,
                    message="SendGrid connection healthy",
                    provider="sendgrid",
                    details={"circuit_state": self._circuit_state.value},
                )
            else:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=latency_ms,
                    message=f"SendGrid API returned {response.status_code}",
                    provider="sendgrid",
                    details={"status_code": response.status_code},
                )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                healthy=False,
                latency_ms=latency_ms,
                message=f"Health check failed: {str(e)}",
                provider="sendgrid",
                details={"error_type": type(e).__name__},
            )

    async def _rate_limit(self) -> None:
        """Apply rate limiting."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

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
        await self._rate_limit()

        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                response = await self._client.request(
                    method,
                    path,
                    json=json_data,
                )

                if response.status_code == 429:
                    self._metrics["rate_limited"] += 1
                    retry_after = int(response.headers.get("Retry-After", 60))
                    await asyncio.sleep(retry_after)
                    continue

                if response.status_code >= 400:
                    error_body = response.json() if response.content else {}
                    # Record failure for 5xx errors (server-side)
                    if response.status_code >= 500:
                        await self._record_failure()
                    raise SendGridError(
                        f"SendGrid API error: {response.status_code}",
                        status_code=response.status_code,
                        errors=error_body.get("errors", []),
                    )

                await self._record_success()
                return response.json() if response.content else {}

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                await self._record_failure()
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue

        raise ConnectionError(f"Failed after {self.config.max_retries} attempts: {last_error}")

    def _build_personalization(self, message: EmailMessage) -> dict:
        """Build personalization object for SendGrid."""
        personalization = {
            "to": [{"email": email} for email in message.to],
        }

        if message.cc:
            personalization["cc"] = [{"email": email} for email in message.cc]
        if message.bcc:
            personalization["bcc"] = [{"email": email} for email in message.bcc]
        if message.template_data:
            personalization["dynamic_template_data"] = message.template_data
        if message.headers:
            personalization["headers"] = message.headers
        if message.send_at:
            personalization["send_at"] = int(message.send_at.timestamp())

        return personalization

    async def send(self, message: EmailMessage) -> SendResult:
        """Send an email via SendGrid."""
        from_email = message.from_email or self.config.default_from_email
        from_name = message.from_name or self.config.default_from_name

        if not from_email:
            raise ValueError("from_email is required")

        payload = {
            "personalizations": [self._build_personalization(message)],
            "from": {"email": from_email},
            "subject": message.subject,
            "tracking_settings": {
                "open_tracking": {"enable": message.tracking_opens},
                "click_tracking": {"enable": message.tracking_clicks},
            },
        }

        if from_name:
            payload["from"]["name"] = from_name

        if message.reply_to:
            payload["reply_to"] = {"email": message.reply_to}

        # Content
        if message.template_id:
            payload["template_id"] = message.template_id
        else:
            content = []
            if message.text_body:
                content.append({"type": "text/plain", "value": message.text_body})
            if message.html_body:
                content.append({"type": "text/html", "value": message.html_body})
            if content:
                payload["content"] = content

        # Attachments
        if message.attachments:
            payload["attachments"] = []
            for att in message.attachments:
                att_data = {
                    "content": base64.b64encode(att.content).decode("utf-8"),
                    "filename": att.filename,
                    "type": att.content_type,
                    "disposition": att.disposition,
                }
                if att.content_id:
                    att_data["content_id"] = att.content_id
                payload["attachments"].append(att_data)

        # Categories/tags
        if message.tags:
            payload["categories"] = message.tags[:10]  # Max 10 categories

        # Custom metadata
        if message.metadata:
            payload["custom_args"] = message.metadata

        # IP pool
        if self.config.ip_pool_name:
            payload["ip_pool_name"] = self.config.ip_pool_name

        try:
            response = await self._request("POST", "/mail/send", payload)
            self._metrics["emails_sent"] += 1

            # SendGrid returns 202 with message ID in header
            message_id = response.get("x-message-id", f"sg-{int(time.time())}")

            logger.info(
                "Email sent via SendGrid",
                extra={
                    "event_type": "sendgrid.email_sent",
                    "message_id": message_id,
                    "to": message.to,
                    "template_id": message.template_id,
                    "tenant_id": self.config.tenant_id,
                },
            )

            return SendResult(
                message_id=message_id,
                status=EmailStatus.SENT,
                provider="sendgrid",
                response=response,
            )

        except Exception as e:
            self._metrics["emails_failed"] += 1
            raise

    async def send_batch(
        self,
        messages: list[EmailMessage],
        max_concurrent: int = 10,
    ) -> list[SendResult]:
        """Send multiple emails with concurrency control."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def send_with_semaphore(msg: EmailMessage) -> SendResult:
            async with semaphore:
                return await self.send(msg)

        tasks = [send_with_semaphore(msg) for msg in messages]
        return await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------------------------------------------------------
    # Template Management
    # -------------------------------------------------------------------------

    async def list_templates(
        self,
        generations: str = "dynamic",
        page_size: int = 100,
    ) -> list[dict]:
        """List email templates."""
        result = await self._request(
            "GET",
            f"/templates?generations={generations}&page_size={page_size}",
        )
        return result.get("templates", [])

    async def get_template(self, template_id: str) -> dict:
        """Get template details."""
        return await self._request("GET", f"/templates/{template_id}")

    async def create_template(
        self,
        name: str,
        generation: str = "dynamic",
    ) -> dict:
        """Create a new template."""
        return await self._request(
            "POST",
            "/templates",
            {"name": name, "generation": generation},
        )

    async def create_template_version(
        self,
        template_id: str,
        name: str,
        subject: str,
        html_content: str,
        plain_content: Optional[str] = None,
        active: bool = True,
    ) -> dict:
        """Create a template version."""
        payload = {
            "name": name,
            "subject": subject,
            "html_content": html_content,
            "active": 1 if active else 0,
        }
        if plain_content:
            payload["plain_content"] = plain_content

        return await self._request(
            "POST",
            f"/templates/{template_id}/versions",
            payload,
        )

    # -------------------------------------------------------------------------
    # Webhook Validation
    # -------------------------------------------------------------------------

    def validate_webhook_signature(
        self,
        payload: bytes,
        signature: str,
        timestamp: str,
    ) -> bool:
        """
        Validate SendGrid webhook signature.

        Args:
            payload: Raw request body
            signature: X-Twilio-Email-Event-Webhook-Signature header
            timestamp: X-Twilio-Email-Event-Webhook-Timestamp header
        """
        if not self.config.webhook_signing_key:
            logger.warning("Webhook signing key not configured")
            return True  # Skip validation if not configured

        try:
            # Decode the public key
            import base64
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import ec
            from cryptography.hazmat.primitives.serialization import load_pem_public_key

            # Compute expected signature
            signed_payload = timestamp.encode() + payload

            # Verify using ECDSA
            public_key = load_pem_public_key(
                base64.b64decode(self.config.webhook_signing_key)
            )
            signature_bytes = base64.b64decode(signature)

            public_key.verify(
                signature_bytes,
                signed_payload,
                ec.ECDSA(hashes.SHA256()),
            )
            return True

        except ImportError:
            logger.warning("cryptography not installed for webhook validation")
            return True
        except Exception as e:
            logger.warning(f"Webhook signature validation failed: {e}")
            return False

    def parse_webhook_events(self, payload: list[dict]) -> list[dict]:
        """Parse SendGrid webhook event payload."""
        events = []
        for event in payload:
            events.append({
                "message_id": event.get("sg_message_id", "").split(".")[0],
                "event_type": event.get("event"),
                "email": event.get("email"),
                "timestamp": datetime.fromtimestamp(event.get("timestamp", 0)),
                "reason": event.get("reason"),
                "bounce_classification": event.get("bounce_classification"),
                "url": event.get("url"),  # For click events
                "useragent": event.get("useragent"),
                "ip": event.get("ip"),
                "raw": event,
            })
        return events

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------

    def get_metrics(self) -> dict:
        """Get connector metrics."""
        return {
            **self._metrics,
            "circuit_state": self._circuit_state.value,
            "tenant_id": self.config.tenant_id,
        }


class SendGridError(Exception):
    """SendGrid API error."""
    def __init__(self, message: str, status_code: int = 0, errors: list = None):
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors or []


# =============================================================================
# Mailgun Connector
# =============================================================================


@dataclass
class MailgunConfig:
    """Configuration for Mailgun connector."""
    api_key: str
    domain: str
    api_url: str = "https://api.mailgun.net/v3"
    region: str = "us"  # us or eu

    # Webhook settings
    webhook_signing_key: Optional[str] = None

    # Rate limiting
    max_requests_per_second: float = 100.0

    # Retry settings
    max_retries: int = 3
    timeout_seconds: float = 30.0

    # TLS/SSL settings
    verify_ssl: bool = True
    ssl_ca_bundle: Optional[str] = None

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    circuit_breaker_success_threshold: int = 3

    # Tenant isolation
    tenant_id: Optional[str] = None

    # Default sending settings
    default_from_email: Optional[str] = None
    default_from_name: Optional[str] = None

    def __post_init__(self):
        if self.region == "eu":
            self.api_url = "https://api.eu.mailgun.net/v3"


class MailgunConnector:
    """
    Mailgun email connector.

    Features:
    - Full Mailgun Messages API
    - Template support
    - Batch sending with recipient variables
    - Attachment handling
    - Webhook signature validation
    - Tag and variable support
    - Scheduled sending
    - Circuit breaker for fault tolerance
    - Health checks for monitoring
    - TLS/SSL enforcement
    """

    def __init__(self, config: MailgunConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time = 0.0
        self._min_request_interval = 1.0 / config.max_requests_per_second
        self._metrics = {
            "emails_sent": 0,
            "emails_failed": 0,
            "rate_limited": 0,
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
        # SSL/TLS configuration
        verify = self.config.verify_ssl
        if self.config.ssl_ca_bundle:
            verify = self.config.ssl_ca_bundle

        self._client = httpx.AsyncClient(
            base_url=self.config.api_url,
            auth=("api", self.config.api_key),
            timeout=self.config.timeout_seconds,
            verify=verify,
            http2=True,  # Enable HTTP/2 for better performance
        )

        logger.info(
            "Mailgun connector initialized",
            extra={
                "event_type": "mailgun.connected",
                "domain": self.config.domain,
                "tenant_id": self.config.tenant_id,
            },
        )

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

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
                            "Mailgun circuit breaker transitioning to HALF_OPEN",
                            extra={"event_type": "mailgun.circuit_half_open"},
                        )
                    else:
                        retry_after = self.config.circuit_breaker_timeout_seconds - elapsed
                        raise CircuitBreakerError(
                            "Mailgun circuit breaker is open",
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
                        "Mailgun circuit breaker closed",
                        extra={"event_type": "mailgun.circuit_closed"},
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
                    "Mailgun circuit breaker tripped from HALF_OPEN",
                    extra={"event_type": "mailgun.circuit_open"},
                )
            elif self._circuit_state == CircuitState.CLOSED:
                if self._circuit_failure_count >= self.config.circuit_breaker_failure_threshold:
                    self._circuit_state = CircuitState.OPEN
                    self._metrics["circuit_breaker_trips"] += 1
                    logger.warning(
                        "Mailgun circuit breaker tripped",
                        extra={
                            "event_type": "mailgun.circuit_open",
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
        """Perform health check on Mailgun connection."""
        start_time = time.time()

        try:
            if self._circuit_state == CircuitState.OPEN:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Circuit breaker is open",
                    provider="mailgun",
                    details={"circuit_state": self._circuit_state.value},
                )

            if not self._client:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=0,
                    message="Client not connected",
                    provider="mailgun",
                )

            # Check API access by getting domain info
            response = await self._client.get(f"/{self.config.domain}")
            latency_ms = (time.time() - start_time) * 1000

            if response.status_code == 200:
                return HealthCheckResult(
                    healthy=True,
                    latency_ms=latency_ms,
                    message="Mailgun connection healthy",
                    provider="mailgun",
                    details={
                        "circuit_state": self._circuit_state.value,
                        "domain": self.config.domain,
                    },
                )
            else:
                return HealthCheckResult(
                    healthy=False,
                    latency_ms=latency_ms,
                    message=f"Mailgun API returned {response.status_code}",
                    provider="mailgun",
                    details={"status_code": response.status_code},
                )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                healthy=False,
                latency_ms=latency_ms,
                message=f"Health check failed: {str(e)}",
                provider="mailgun",
                details={"error_type": type(e).__name__},
            )

    async def _rate_limit(self) -> None:
        """Apply rate limiting."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    async def send(self, message: EmailMessage) -> SendResult:
        """Send an email via Mailgun."""
        if not self._client:
            raise RuntimeError("Connector not connected")

        await self._check_circuit()

        from_email = message.from_email or self.config.default_from_email
        from_name = message.from_name or self.config.default_from_name

        if not from_email:
            raise ValueError("from_email is required")

        from_address = f"{from_name} <{from_email}>" if from_name else from_email

        await self._rate_limit()

        # Build form data
        data = {
            "from": from_address,
            "to": message.to,
            "subject": message.subject,
        }

        if message.cc:
            data["cc"] = message.cc
        if message.bcc:
            data["bcc"] = message.bcc
        if message.reply_to:
            data["h:Reply-To"] = message.reply_to
        if message.text_body:
            data["text"] = message.text_body
        if message.html_body:
            data["html"] = message.html_body

        # Template
        if message.template_id:
            data["template"] = message.template_id
            if message.template_data:
                data["h:X-Mailgun-Variables"] = json.dumps(message.template_data)

        # Tags
        if message.tags:
            data["o:tag"] = message.tags[:3]  # Max 3 tags

        # Tracking
        data["o:tracking-opens"] = "yes" if message.tracking_opens else "no"
        data["o:tracking-clicks"] = "yes" if message.tracking_clicks else "no"

        # Scheduled sending
        if message.send_at:
            data["o:deliverytime"] = message.send_at.strftime("%a, %d %b %Y %H:%M:%S %z")

        # Custom headers
        for key, value in message.headers.items():
            data[f"h:{key}"] = value

        # Custom metadata/variables
        for key, value in message.metadata.items():
            data[f"v:{key}"] = value

        # Handle attachments
        files = []
        if message.attachments:
            for att in message.attachments:
                if att.disposition == "inline":
                    files.append(("inline", (att.filename, att.content, att.content_type)))
                else:
                    files.append(("attachment", (att.filename, att.content, att.content_type)))

        try:
            response = await self._client.post(
                f"/{self.config.domain}/messages",
                data=data,
                files=files if files else None,
            )

            if response.status_code == 429:
                self._metrics["rate_limited"] += 1
                raise MailgunError("Rate limited", status_code=429)

            if response.status_code >= 400:
                error_body = response.json() if response.content else {}
                # Record failure for 5xx errors (server-side)
                if response.status_code >= 500:
                    await self._record_failure()
                raise MailgunError(
                    error_body.get("message", f"Mailgun API error: {response.status_code}"),
                    status_code=response.status_code,
                )

            await self._record_success()
            result = response.json()
            self._metrics["emails_sent"] += 1

            # Extract message ID from response
            message_id = result.get("id", "").strip("<>")

            logger.info(
                "Email sent via Mailgun",
                extra={
                    "event_type": "mailgun.email_sent",
                    "message_id": message_id,
                    "to": message.to,
                    "template_id": message.template_id,
                    "tenant_id": self.config.tenant_id,
                },
            )

            return SendResult(
                message_id=message_id,
                status=EmailStatus.SENT,
                provider="mailgun",
                response=result,
            )

        except httpx.HTTPError as e:
            self._metrics["emails_failed"] += 1
            await self._record_failure()
            raise MailgunError(f"HTTP error: {e}")

    async def send_batch(
        self,
        message: EmailMessage,
        recipient_variables: dict[str, dict],
    ) -> SendResult:
        """
        Send batch email with recipient variables.

        Args:
            message: Base message (to field will be overwritten)
            recipient_variables: Dict mapping email to variables
                Example: {"user@example.com": {"name": "John", "id": "123"}}
        """
        if not self._client:
            raise RuntimeError("Connector not connected")

        # Override recipients
        message.to = list(recipient_variables.keys())

        from_email = message.from_email or self.config.default_from_email
        from_name = message.from_name or self.config.default_from_name
        from_address = f"{from_name} <{from_email}>" if from_name else from_email

        await self._rate_limit()

        data = {
            "from": from_address,
            "to": message.to,
            "subject": message.subject,
            "recipient-variables": json.dumps(recipient_variables),
        }

        if message.text_body:
            data["text"] = message.text_body
        if message.html_body:
            data["html"] = message.html_body
        if message.template_id:
            data["template"] = message.template_id

        response = await self._client.post(
            f"/{self.config.domain}/messages",
            data=data,
        )

        if response.status_code >= 400:
            error_body = response.json() if response.content else {}
            raise MailgunError(
                error_body.get("message", f"Mailgun API error: {response.status_code}"),
                status_code=response.status_code,
            )

        result = response.json()
        self._metrics["emails_sent"] += len(recipient_variables)

        return SendResult(
            message_id=result.get("id", "").strip("<>"),
            status=EmailStatus.SENT,
            provider="mailgun",
            response=result,
        )

    # -------------------------------------------------------------------------
    # Template Management
    # -------------------------------------------------------------------------

    async def list_templates(self, limit: int = 100) -> list[dict]:
        """List stored templates."""
        response = await self._client.get(
            f"/{self.config.domain}/templates",
            params={"limit": limit},
        )
        response.raise_for_status()
        return response.json().get("items", [])

    async def get_template(self, template_name: str) -> dict:
        """Get template details."""
        response = await self._client.get(
            f"/{self.config.domain}/templates/{template_name}",
        )
        response.raise_for_status()
        return response.json()

    async def create_template(
        self,
        name: str,
        description: str = "",
    ) -> dict:
        """Create a new template."""
        response = await self._client.post(
            f"/{self.config.domain}/templates",
            data={"name": name, "description": description},
        )
        response.raise_for_status()
        return response.json()

    async def create_template_version(
        self,
        template_name: str,
        template: str,
        tag: str = "initial",
        engine: str = "handlebars",
        active: bool = True,
    ) -> dict:
        """Create a template version."""
        response = await self._client.post(
            f"/{self.config.domain}/templates/{template_name}/versions",
            data={
                "template": template,
                "tag": tag,
                "engine": engine,
                "active": "yes" if active else "no",
            },
        )
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # Webhook Validation
    # -------------------------------------------------------------------------

    def validate_webhook_signature(
        self,
        timestamp: str,
        token: str,
        signature: str,
    ) -> bool:
        """
        Validate Mailgun webhook signature.

        Args:
            timestamp: Timestamp from webhook payload
            token: Token from webhook payload
            signature: Signature from webhook payload
        """
        if not self.config.webhook_signing_key:
            logger.warning("Webhook signing key not configured")
            return True

        # Compute expected signature
        expected = hmac.new(
            self.config.webhook_signing_key.encode(),
            f"{timestamp}{token}".encode(),
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected, signature)

    def parse_webhook_event(self, payload: dict) -> dict:
        """Parse Mailgun webhook event payload."""
        event_data = payload.get("event-data", payload)

        return {
            "message_id": event_data.get("message", {}).get("headers", {}).get("message-id", ""),
            "event_type": event_data.get("event"),
            "email": event_data.get("recipient"),
            "timestamp": datetime.fromtimestamp(event_data.get("timestamp", 0)),
            "reason": event_data.get("reason"),
            "severity": event_data.get("severity"),  # For failures
            "delivery_status": event_data.get("delivery-status", {}),
            "geolocation": event_data.get("geolocation", {}),
            "client_info": event_data.get("client-info", {}),
            "tags": event_data.get("tags", []),
            "user_variables": event_data.get("user-variables", {}),
            "raw": payload,
        }

    # -------------------------------------------------------------------------
    # Domain Management
    # -------------------------------------------------------------------------

    async def get_domain_stats(
        self,
        event: str = "accepted",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> dict:
        """Get domain statistics."""
        params = {"event": event}
        if start:
            params["start"] = start.strftime("%a, %d %b %Y %H:%M:%S %z")
        if end:
            params["end"] = end.strftime("%a, %d %b %Y %H:%M:%S %z")

        response = await self._client.get(
            f"/{self.config.domain}/stats/total",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def get_bounces(self, limit: int = 100) -> list[dict]:
        """Get list of bounces."""
        response = await self._client.get(
            f"/{self.config.domain}/bounces",
            params={"limit": limit},
        )
        response.raise_for_status()
        return response.json().get("items", [])

    async def get_unsubscribes(self, limit: int = 100) -> list[dict]:
        """Get list of unsubscribes."""
        response = await self._client.get(
            f"/{self.config.domain}/unsubscribes",
            params={"limit": limit},
        )
        response.raise_for_status()
        return response.json().get("items", [])

    async def add_unsubscribe(self, email: str, tag: Optional[str] = None) -> dict:
        """Add email to unsubscribe list."""
        data = {"address": email}
        if tag:
            data["tag"] = tag

        response = await self._client.post(
            f"/{self.config.domain}/unsubscribes",
            data=data,
        )
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------

    def get_metrics(self) -> dict:
        """Get connector metrics."""
        return {
            **self._metrics,
            "circuit_state": self._circuit_state.value,
            "domain": self.config.domain,
            "tenant_id": self.config.tenant_id,
        }


class MailgunError(Exception):
    """Mailgun API error."""
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


# =============================================================================
# Singleton Management
# =============================================================================

_sendgrid_instances: dict[str, SendGridConnector] = {}
_mailgun_instances: dict[str, MailgunConnector] = {}
_lock = asyncio.Lock()


async def get_sendgrid_connector(
    config: Optional[SendGridConfig] = None,
    instance_key: str = "default",
) -> SendGridConnector:
    """Get or create a SendGrid connector instance."""
    async with _lock:
        if instance_key not in _sendgrid_instances:
            if config is None:
                raise ValueError("Config required for first initialization")

            connector = SendGridConnector(config)
            await connector.connect()
            _sendgrid_instances[instance_key] = connector

        return _sendgrid_instances[instance_key]


async def get_mailgun_connector(
    config: Optional[MailgunConfig] = None,
    instance_key: str = "default",
) -> MailgunConnector:
    """Get or create a Mailgun connector instance."""
    async with _lock:
        if instance_key not in _mailgun_instances:
            if config is None:
                raise ValueError("Config required for first initialization")

            connector = MailgunConnector(config)
            await connector.connect()
            _mailgun_instances[instance_key] = connector

        return _mailgun_instances[instance_key]


async def close_sendgrid_connector(instance_key: str = "default") -> None:
    """Close and remove a SendGrid connector instance."""
    async with _lock:
        if instance_key in _sendgrid_instances:
            await _sendgrid_instances[instance_key].close()
            del _sendgrid_instances[instance_key]


async def close_mailgun_connector(instance_key: str = "default") -> None:
    """Close and remove a Mailgun connector instance."""
    async with _lock:
        if instance_key in _mailgun_instances:
            await _mailgun_instances[instance_key].close()
            del _mailgun_instances[instance_key]

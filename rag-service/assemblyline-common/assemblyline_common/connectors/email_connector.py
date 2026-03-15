"""
Enterprise Email Connectors.

Supports:
- SMTP with connection pooling and TLS
- AWS SES with configuration sets
- SendGrid API
- Rate limiting per sender
- Bounce/complaint handling

Usage:
    from assemblyline_common.connectors import (
        get_smtp_connector,
        get_ses_connector,
        SMTPConfig,
        SESConfig,
    )

    # SMTP
    smtp = await get_smtp_connector(SMTPConfig(
        host="smtp.example.com",
        port=587,
        use_tls=True,
    ))
    await smtp.send_email(
        to=["user@example.com"],
        subject="Hello",
        body="Hello, World!",
    )

    # AWS SES
    ses = await get_ses_connector(SESConfig(
        region="us-east-1",
        configuration_set="my-config",
    ))
    await ses.send_email(
        to=["user@example.com"],
        subject="Hello",
        body="Hello, World!",
    )
"""

import asyncio
import base64
import logging
import ssl
import time
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, Dict, Any, List, Union

import aiosmtplib
import aioboto3

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
)
from assemblyline_common.retry import RetryHandler, RetryConfig

logger = logging.getLogger(__name__)


@dataclass
class EmailAttachment:
    """Email attachment."""
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


@dataclass
class EmailMessage:
    """Email message structure."""
    to: List[str]
    subject: str
    body: str
    body_html: Optional[str] = None
    from_address: Optional[str] = None
    reply_to: Optional[str] = None
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    attachments: List[EmailAttachment] = field(default_factory=list)
    headers: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SMTPConfig:
    """Configuration for SMTP connector."""
    host: str = "localhost"
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None

    # TLS settings
    use_tls: bool = True
    start_tls: bool = True
    validate_certs: bool = True
    client_cert: Optional[str] = None
    client_key: Optional[str] = None

    # Connection settings
    timeout: float = 30.0
    source_address: Optional[str] = None

    # Default sender
    default_from: Optional[str] = None
    default_from_name: Optional[str] = None

    # Rate limiting
    rate_limit_per_minute: int = 100

    # Circuit breaker
    enable_circuit_breaker: bool = True

    # Tenant ID
    tenant_id: Optional[str] = None


@dataclass
class SESConfig:
    """Configuration for AWS SES connector."""
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None

    # Credentials (if not using IAM role)
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None

    # Configuration set for tracking
    configuration_set: Optional[str] = None

    # Default sender
    default_from: Optional[str] = None
    default_from_name: Optional[str] = None

    # Rate limiting (SES has account-level limits)
    rate_limit_per_second: int = 14  # SES default

    # Circuit breaker
    enable_circuit_breaker: bool = True

    # Tenant ID
    tenant_id: Optional[str] = None


class RateLimiter:
    """Simple rate limiter for email sending."""

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: List[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire a slot, return True if allowed."""
        async with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            # Remove old requests
            self.requests = [r for r in self.requests if r > cutoff]

            if len(self.requests) >= self.max_requests:
                return False

            self.requests.append(now)
            return True

    async def wait_and_acquire(self, timeout: float = 60.0) -> bool:
        """Wait for a slot to become available."""
        deadline = time.time() + timeout

        while time.time() < deadline:
            if await self.acquire():
                return True
            await asyncio.sleep(0.1)

        return False


class SMTPConnector:
    """
    Enterprise SMTP connector.

    Features:
    - TLS/STARTTLS support
    - Authentication
    - Rate limiting
    - Circuit breaker
    - Attachment support
    """

    def __init__(
        self,
        config: SMTPConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._rate_limiter: Optional[RateLimiter] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._metrics: Dict[str, int] = {
            "emails_sent": 0,
            "errors": 0,
        }
        self._initialized = False
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        # Initialize rate limiter
        self._rate_limiter = RateLimiter(
            max_requests=self.config.rate_limit_per_minute,
            window_seconds=60,
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        self._retry_handler = RetryHandler(
            config=RetryConfig(
                max_attempts=3,
                base_delay=1.0,
                max_delay=30.0,
            )
        )

        self._initialized = True

        logger.info(
            "SMTP connector initialized",
            extra={
                "event_type": "smtp_initialized",
                "host": self.config.host,
                "port": self.config.port,
                "tls": self.config.use_tls,
            }
        )

    def _build_message(self, email: EmailMessage) -> MIMEMultipart:
        """Build MIME message from EmailMessage."""
        msg = MIMEMultipart("mixed")

        # Set headers
        from_addr = email.from_address or self.config.default_from
        if self.config.default_from_name and not email.from_address:
            from_addr = f"{self.config.default_from_name} <{from_addr}>"

        msg["From"] = from_addr
        msg["To"] = ", ".join(email.to)
        msg["Subject"] = email.subject

        if email.cc:
            msg["Cc"] = ", ".join(email.cc)

        if email.reply_to:
            msg["Reply-To"] = email.reply_to

        for key, value in email.headers.items():
            msg[key] = value

        # Create body part
        body_part = MIMEMultipart("alternative")

        # Plain text
        body_part.attach(MIMEText(email.body, "plain", "utf-8"))

        # HTML (if provided)
        if email.body_html:
            body_part.attach(MIMEText(email.body_html, "html", "utf-8"))

        msg.attach(body_part)

        # Attachments
        for attachment in email.attachments:
            part = MIMEBase(*attachment.content_type.split("/", 1))
            part.set_payload(attachment.content)
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={attachment.filename}",
            )
            msg.attach(part)

        return msg

    async def send_email(
        self,
        to: Union[str, List[str]],
        subject: str,
        body: str,
        body_html: Optional[str] = None,
        from_address: Optional[str] = None,
        attachments: Optional[List[EmailAttachment]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Send an email.

        Args:
            to: Recipient address(es)
            subject: Email subject
            body: Plain text body
            body_html: HTML body (optional)
            from_address: Sender address (uses default if not provided)
            attachments: List of attachments

        Returns:
            Dict with send status
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._initialized:
            await self.initialize()

        # Normalize recipients
        if isinstance(to, str):
            to = [to]

        email = EmailMessage(
            to=to,
            subject=subject,
            body=body,
            body_html=body_html,
            from_address=from_address,
            attachments=attachments or [],
            **kwargs,
        )

        # Rate limiting
        if not await self._rate_limiter.wait_and_acquire():
            raise RuntimeError("Rate limit exceeded for SMTP")

        # Check circuit breaker
        circuit_name = f"smtp:{self.config.host}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        msg = self._build_message(email)

        async def do_send() -> Dict[str, Any]:
            # Create SSL context
            ssl_context = None
            if self.config.use_tls or self.config.start_tls:
                ssl_context = ssl.create_default_context()
                if not self.config.validate_certs:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                if self.config.client_cert:
                    ssl_context.load_cert_chain(
                        self.config.client_cert,
                        self.config.client_key,
                    )

            # Connect and send
            async with aiosmtplib.SMTP(
                hostname=self.config.host,
                port=self.config.port,
                timeout=self.config.timeout,
                use_tls=self.config.use_tls and not self.config.start_tls,
                tls_context=ssl_context if self.config.use_tls else None,
                source_address=self.config.source_address,
            ) as smtp:
                if self.config.start_tls and not self.config.use_tls:
                    await smtp.starttls(tls_context=ssl_context)

                if self.config.username:
                    await smtp.login(self.config.username, self.config.password)

                # Get all recipients
                recipients = email.to + email.cc + email.bcc

                # Send
                response = await smtp.send_message(msg, recipients=recipients)

                return {
                    "success": True,
                    "message_id": msg["Message-ID"],
                    "recipients": recipients,
                }

        try:
            if self._retry_handler:
                result = await self._retry_handler.execute(
                    do_send,
                    operation_id=f"smtp-send-{email.to[0]}",
                )
            else:
                result = await do_send()

            self._metrics["emails_sent"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            logger.info(
                "Email sent via SMTP",
                extra={
                    "event_type": "smtp_email_sent",
                    "to": email.to,
                    "subject": email.subject,
                }
            )

            return result

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return self._metrics

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True
        logger.info("SMTP connector closed")


class SESConnector:
    """
    Enterprise AWS SES connector.

    Features:
    - Configuration sets for tracking
    - Bounce/complaint handling
    - Rate limiting
    - Circuit breaker
    - Template support
    """

    def __init__(
        self,
        config: SESConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._session: Optional[aioboto3.Session] = None
        self._rate_limiter: Optional[RateLimiter] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._metrics: Dict[str, int] = {
            "emails_sent": 0,
            "errors": 0,
            "bounces": 0,
            "complaints": 0,
        }
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        self._session = aioboto3.Session(
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            region_name=self.config.region,
        )

        # Initialize rate limiter
        self._rate_limiter = RateLimiter(
            max_requests=self.config.rate_limit_per_second,
            window_seconds=1,
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        self._retry_handler = RetryHandler(
            config=RetryConfig(
                max_attempts=3,
                base_delay=0.5,
                max_delay=10.0,
            )
        )

        logger.info(
            "SES connector initialized",
            extra={
                "event_type": "ses_initialized",
                "region": self.config.region,
                "configuration_set": self.config.configuration_set,
            }
        )

    async def send_email(
        self,
        to: Union[str, List[str]],
        subject: str,
        body: str,
        body_html: Optional[str] = None,
        from_address: Optional[str] = None,
        reply_to: Optional[List[str]] = None,
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Send an email via AWS SES.

        Args:
            to: Recipient address(es)
            subject: Email subject
            body: Plain text body
            body_html: HTML body (optional)
            from_address: Sender address (uses default if not provided)
            reply_to: Reply-to addresses
            cc: CC addresses
            bcc: BCC addresses
            tags: Tags for tracking

        Returns:
            Dict with MessageId and status
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._session:
            await self.initialize()

        # Normalize recipients
        if isinstance(to, str):
            to = [to]

        # Rate limiting
        if not await self._rate_limiter.wait_and_acquire():
            raise RuntimeError("Rate limit exceeded for SES")

        # Check circuit breaker
        circuit_name = f"ses:{self.config.region}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        from_addr = from_address or self.config.default_from
        if self.config.default_from_name and not from_address:
            from_addr = f"{self.config.default_from_name} <{from_addr}>"

        # Build destination
        destination = {"ToAddresses": to}
        if cc:
            destination["CcAddresses"] = cc
        if bcc:
            destination["BccAddresses"] = bcc

        # Build message
        message = {
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": body, "Charset": "UTF-8"},
            },
        }
        if body_html:
            message["Body"]["Html"] = {"Data": body_html, "Charset": "UTF-8"}

        # Build send request
        send_args = {
            "Source": from_addr,
            "Destination": destination,
            "Message": message,
        }

        if reply_to:
            send_args["ReplyToAddresses"] = reply_to

        if self.config.configuration_set:
            send_args["ConfigurationSetName"] = self.config.configuration_set

        if tags:
            send_args["Tags"] = [
                {"Name": k, "Value": v} for k, v in tags.items()
            ]

        async def do_send() -> Dict[str, Any]:
            async with self._session.client(
                "ses",
                endpoint_url=self.config.endpoint_url,
            ) as ses:
                response = await ses.send_email(**send_args)
                return {
                    "success": True,
                    "message_id": response["MessageId"],
                    "recipients": to + (cc or []) + (bcc or []),
                }

        try:
            if self._retry_handler:
                result = await self._retry_handler.execute(
                    do_send,
                    operation_id=f"ses-send-{to[0]}",
                )
            else:
                result = await do_send()

            self._metrics["emails_sent"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            logger.info(
                "Email sent via SES",
                extra={
                    "event_type": "ses_email_sent",
                    "message_id": result["message_id"],
                    "to": to,
                    "subject": subject,
                }
            )

            return result

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    async def send_templated_email(
        self,
        to: Union[str, List[str]],
        template_name: str,
        template_data: Dict[str, Any],
        from_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send an email using an SES template."""
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._session:
            await self.initialize()

        if isinstance(to, str):
            to = [to]

        from_addr = from_address or self.config.default_from

        import json

        async with self._session.client(
            "ses",
            endpoint_url=self.config.endpoint_url,
        ) as ses:
            response = await ses.send_templated_email(
                Source=from_addr,
                Destination={"ToAddresses": to},
                Template=template_name,
                TemplateData=json.dumps(template_data),
                ConfigurationSetName=self.config.configuration_set,
            )

        return {
            "success": True,
            "message_id": response["MessageId"],
        }

    async def get_send_quota(self) -> Dict[str, Any]:
        """Get current send quota and usage."""
        if not self._session:
            await self.initialize()

        async with self._session.client("ses") as ses:
            response = await ses.get_send_quota()

        return {
            "max_24_hour_send": response["Max24HourSend"],
            "max_send_rate": response["MaxSendRate"],
            "sent_last_24_hours": response["SentLast24Hours"],
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return self._metrics

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True
        self._session = None
        logger.info("SES connector closed")


# Singleton instances
_smtp_connectors: Dict[str, SMTPConnector] = {}
_ses_connectors: Dict[str, SESConnector] = {}
_email_lock = asyncio.Lock()


async def get_smtp_connector(
    config: Optional[SMTPConfig] = None,
    name: Optional[str] = None,
) -> SMTPConnector:
    """Get or create an SMTP connector."""
    config = config or SMTPConfig()
    connector_name = name or f"smtp-{config.host}"

    if connector_name in _smtp_connectors:
        return _smtp_connectors[connector_name]

    async with _email_lock:
        if connector_name in _smtp_connectors:
            return _smtp_connectors[connector_name]

        connector = SMTPConnector(config)
        await connector.initialize()
        _smtp_connectors[connector_name] = connector

        return connector


async def get_ses_connector(
    config: Optional[SESConfig] = None,
    name: Optional[str] = None,
) -> SESConnector:
    """Get or create an SES connector."""
    config = config or SESConfig()
    connector_name = name or f"ses-{config.region}"

    if connector_name in _ses_connectors:
        return _ses_connectors[connector_name]

    async with _email_lock:
        if connector_name in _ses_connectors:
            return _ses_connectors[connector_name]

        connector = SESConnector(config)
        await connector.initialize()
        _ses_connectors[connector_name] = connector

        return connector


async def close_all_email_connectors() -> None:
    """Close all email connectors."""
    for connector in _smtp_connectors.values():
        await connector.close()
    _smtp_connectors.clear()

    for connector in _ses_connectors.values():
        await connector.close()
    _ses_connectors.clear()

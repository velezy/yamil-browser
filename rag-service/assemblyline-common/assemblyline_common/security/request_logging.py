"""
Request Logging Policy

MuleSoft Message Logging policy equivalent with HIPAA-compliant PHI masking.
Log API requests and responses with automatic sensitive data redaction.

Features:
- Request/response logging
- PHI masking for HIPAA compliance
- Configurable log levels
- Structured JSON logging
- Field-level redaction
- Header filtering
- Body size limits
- Correlation ID tracking
"""

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Set

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class LogLevel(str, Enum):
    """Log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class LogTarget(str, Enum):
    """Log targets."""
    LOGGER = "logger"  # Python logger
    STDOUT = "stdout"  # Print to stdout
    FILE = "file"  # Write to file
    KAFKA = "kafka"  # Send to Kafka
    WEBHOOK = "webhook"  # Send to webhook


@dataclass
class PHIMaskingConfig:
    """Configuration for PHI masking."""
    # Enable PHI masking
    enabled: bool = True

    # Mask character
    mask_char: str = "*"

    # Number of mask characters
    mask_length: int = 8

    # Show last N characters
    show_last: int = 0

    # Patterns to mask (regex)
    patterns: Dict[str, str] = field(default_factory=lambda: {
        # SSN: 123-45-6789
        "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
        # MRN: Various formats
        "mrn": r"\b[A-Z]{0,3}\d{6,10}\b",
        # Date of birth: MM/DD/YYYY, YYYY-MM-DD
        "dob": r"\b(?:\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})\b",
        # Phone: (123) 456-7890, 123-456-7890
        "phone": r"\b(?:\(\d{3}\)\s*|\d{3}[-.])\d{3}[-.]?\d{4}\b",
        # Email
        "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        # Credit card: 16 digits with optional separators
        "credit_card": r"\b(?:\d{4}[-\s]?){3}\d{4}\b",
        # IP address
        "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    })

    # JSON field names to always mask
    sensitive_fields: Set[str] = field(default_factory=lambda: {
        "password", "secret", "token", "api_key", "apikey", "api-key",
        "authorization", "auth", "credential", "credentials",
        "ssn", "social_security", "dob", "date_of_birth", "birth_date",
        "mrn", "medical_record", "patient_id",
        "credit_card", "card_number", "cvv", "expiry",
        "pin", "secret_key", "private_key",
    })

    # Headers to mask
    sensitive_headers: Set[str] = field(default_factory=lambda: {
        "authorization", "x-api-key", "cookie", "set-cookie",
        "x-auth-token", "x-access-token", "x-secret",
    })


@dataclass
class RequestLoggingConfig:
    """Configuration for request logging."""
    # Log request details
    log_request: bool = True

    # Log response details
    log_response: bool = True

    # Log level
    log_level: LogLevel = LogLevel.INFO

    # Log target
    log_target: LogTarget = LogTarget.LOGGER

    # PHI masking configuration
    phi_masking: PHIMaskingConfig = field(default_factory=PHIMaskingConfig)

    # Max body size to log (bytes)
    max_body_size: int = 10240  # 10KB

    # Truncate body if larger
    truncate_body: bool = True

    # Include request headers
    include_headers: bool = True

    # Include request body
    include_body: bool = True

    # Include response body
    include_response_body: bool = False

    # Include query parameters
    include_query_params: bool = True

    # Exclude paths from logging
    exclude_paths: List[str] = field(default_factory=lambda: [
        "/health",
        "/health/live",
        "/health/ready",
        "/metrics",
    ])

    # Only log specific paths
    include_paths: List[str] = field(default_factory=list)

    # Correlation ID header name
    correlation_id_header: str = "X-Correlation-ID"

    # Generate correlation ID if not present
    generate_correlation_id: bool = True

    # Log file path (for FILE target)
    log_file_path: Optional[str] = None

    # Kafka topic (for KAFKA target)
    kafka_topic: Optional[str] = None

    # Webhook URL (for WEBHOOK target)
    webhook_url: Optional[str] = None


class PHIMasker:
    """Mask PHI and sensitive data."""

    def __init__(self, config: PHIMaskingConfig):
        self.config = config
        self._patterns: Dict[str, Pattern] = {}
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns."""
        for name, pattern in self.config.patterns.items():
            try:
                self._patterns[name] = re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                logger.warning(f"Invalid PHI pattern '{name}': {e}")

    def _mask_value(self, value: str) -> str:
        """Mask a value."""
        if not value:
            return value

        if self.config.show_last > 0 and len(value) > self.config.show_last:
            masked_part = self.config.mask_char * self.config.mask_length
            visible_part = value[-self.config.show_last:]
            return masked_part + visible_part
        else:
            return self.config.mask_char * self.config.mask_length

    def mask_text(self, text: str) -> str:
        """Mask PHI patterns in text."""
        if not self.config.enabled:
            return text

        for name, pattern in self._patterns.items():
            text = pattern.sub(
                lambda m: self._mask_value(m.group()),
                text
            )

        return text

    def mask_dict(self, data: Dict[str, Any], depth: int = 0) -> Dict[str, Any]:
        """Recursively mask sensitive fields in a dictionary."""
        if not self.config.enabled or depth > 10:
            return data

        result = {}

        for key, value in data.items():
            key_lower = key.lower().replace("-", "_").replace(" ", "_")

            if key_lower in self.config.sensitive_fields:
                if isinstance(value, str):
                    result[key] = self._mask_value(value)
                else:
                    result[key] = self._mask_value(str(value))
            elif isinstance(value, dict):
                result[key] = self.mask_dict(value, depth + 1)
            elif isinstance(value, list):
                result[key] = [
                    self.mask_dict(item, depth + 1) if isinstance(item, dict)
                    else self.mask_text(str(item)) if isinstance(item, str)
                    else item
                    for item in value
                ]
            elif isinstance(value, str):
                result[key] = self.mask_text(value)
            else:
                result[key] = value

        return result

    def mask_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Mask sensitive headers."""
        if not self.config.enabled:
            return headers

        result = {}

        for key, value in headers.items():
            key_lower = key.lower()
            if key_lower in self.config.sensitive_headers:
                result[key] = self._mask_value(value)
            else:
                result[key] = self.mask_text(value)

        return result


@dataclass
class RequestLogEntry:
    """Structured request log entry."""
    timestamp: str
    correlation_id: str
    method: str
    path: str
    query_params: Optional[Dict[str, str]]
    headers: Optional[Dict[str, str]]
    body: Optional[str]
    client_ip: Optional[str]
    user_agent: Optional[str]
    content_type: Optional[str]
    content_length: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "request",
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
            "method": self.method,
            "path": self.path,
            "query_params": self.query_params,
            "headers": self.headers,
            "body": self.body,
            "client_ip": self.client_ip,
            "user_agent": self.user_agent,
            "content_type": self.content_type,
            "content_length": self.content_length,
        }


@dataclass
class ResponseLogEntry:
    """Structured response log entry."""
    timestamp: str
    correlation_id: str
    status_code: int
    headers: Optional[Dict[str, str]]
    body: Optional[str]
    content_type: Optional[str]
    content_length: Optional[int]
    duration_ms: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "response",
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body,
            "content_type": self.content_type,
            "content_length": self.content_length,
            "duration_ms": self.duration_ms,
        }


class RequestLogger:
    """
    Request logging policy with PHI masking.

    Usage:
        request_logger = RequestLogger(RequestLoggingConfig(
            log_request=True,
            log_response=True,
            phi_masking=PHIMaskingConfig(enabled=True),
        ))

        # Attach as middleware
        request_logger.attach(app)
    """

    def __init__(self, config: Optional[RequestLoggingConfig] = None):
        self.config = config or RequestLoggingConfig()
        self.masker = PHIMasker(self.config.phi_masking)
        self._file_handler = None

    def _should_log(self, path: str) -> bool:
        """Check if path should be logged."""
        # Check exclusions
        for exclude in self.config.exclude_paths:
            if path.startswith(exclude):
                return False

        # Check inclusions
        if self.config.include_paths:
            for include in self.config.include_paths:
                if path.startswith(include):
                    return True
            return False

        return True

    def _get_correlation_id(self, request: Request) -> str:
        """Get or generate correlation ID."""
        correlation_id = request.headers.get(self.config.correlation_id_header)

        if not correlation_id and self.config.generate_correlation_id:
            correlation_id = str(uuid.uuid4())

        return correlation_id or "unknown"

    async def _get_request_body(self, request: Request) -> Optional[str]:
        """Get and mask request body."""
        if not self.config.include_body:
            return None

        try:
            body = await request.body()

            if not body:
                return None

            # Check size
            if len(body) > self.config.max_body_size:
                if self.config.truncate_body:
                    body = body[:self.config.max_body_size]
                else:
                    return f"[Body too large: {len(body)} bytes]"

            # Try to parse as JSON for better masking
            content_type = request.headers.get("content-type", "")
            if "json" in content_type:
                try:
                    data = json.loads(body)
                    masked = self.masker.mask_dict(data)
                    return json.dumps(masked)
                except json.JSONDecodeError:
                    pass

            # Mask as text
            return self.masker.mask_text(body.decode("utf-8", errors="replace"))

        except Exception as e:
            logger.warning(f"Failed to get request body: {e}")
            return None

    def _log_entry(self, entry: Dict[str, Any]):
        """Log an entry to the configured target."""
        log_msg = json.dumps(entry, default=str)

        if self.config.log_target == LogTarget.LOGGER:
            level = getattr(logging, self.config.log_level.value.upper())
            logger.log(level, log_msg)

        elif self.config.log_target == LogTarget.STDOUT:
            print(log_msg)

        elif self.config.log_target == LogTarget.FILE:
            if self.config.log_file_path:
                with open(self.config.log_file_path, "a") as f:
                    f.write(log_msg + "\n")

    async def log_request(self, request: Request) -> str:
        """Log incoming request."""
        if not self.config.log_request:
            return self._get_correlation_id(request)

        correlation_id = self._get_correlation_id(request)

        # Get headers
        headers = None
        if self.config.include_headers:
            headers = self.masker.mask_headers(dict(request.headers))

        # Get query params
        query_params = None
        if self.config.include_query_params:
            query_params = dict(request.query_params)

        # Get body
        body = await self._get_request_body(request)

        entry = RequestLogEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            correlation_id=correlation_id,
            method=request.method,
            path=request.url.path,
            query_params=query_params,
            headers=headers,
            body=body,
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            content_type=request.headers.get("content-type"),
            content_length=int(request.headers.get("content-length", 0)),
        )

        self._log_entry(entry.to_dict())
        return correlation_id

    async def log_response(
        self,
        response: Response,
        correlation_id: str,
        duration_ms: float,
        body: Optional[bytes] = None,
    ):
        """Log outgoing response."""
        if not self.config.log_response:
            return

        # Get headers
        headers = None
        if self.config.include_headers:
            headers = self.masker.mask_headers(dict(response.headers))

        # Get body
        body_str = None
        if self.config.include_response_body and body:
            if len(body) <= self.config.max_body_size:
                content_type = response.headers.get("content-type", "")
                if "json" in content_type:
                    try:
                        data = json.loads(body)
                        masked = self.masker.mask_dict(data)
                        body_str = json.dumps(masked)
                    except json.JSONDecodeError:
                        body_str = self.masker.mask_text(
                            body.decode("utf-8", errors="replace")
                        )
                else:
                    body_str = self.masker.mask_text(
                        body.decode("utf-8", errors="replace")
                    )

        entry = ResponseLogEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            correlation_id=correlation_id,
            status_code=response.status_code,
            headers=headers,
            body=body_str,
            content_type=response.headers.get("content-type"),
            content_length=int(response.headers.get("content-length", 0)),
            duration_ms=duration_ms,
        )

        self._log_entry(entry.to_dict())

    def attach(self, app: FastAPI):
        """Attach request logging as middleware."""
        request_logger = self

        class RequestLoggingMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Check if should log
                if not request_logger._should_log(request.url.path):
                    return await call_next(request)

                # Log request
                start_time = time.time()
                correlation_id = await request_logger.log_request(request)

                # Store correlation ID
                request.state.correlation_id = correlation_id

                # Process request
                response = await call_next(request)

                # Calculate duration
                duration_ms = (time.time() - start_time) * 1000

                # Read response body if needed
                body = None
                if request_logger.config.include_response_body:
                    body = b""
                    async for chunk in response.body_iterator:
                        body += chunk

                    # Re-create response with body
                    from starlette.responses import Response as StarletteResponse
                    response = StarletteResponse(
                        content=body,
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        media_type=response.media_type,
                    )

                # Log response
                await request_logger.log_response(
                    response, correlation_id, duration_ms, body
                )

                # Add correlation ID header
                response.headers[request_logger.config.correlation_id_header] = correlation_id

                return response

        app.add_middleware(RequestLoggingMiddleware)
        logger.info("Request logging middleware attached")


# Preset configurations
LOGGING_PRESETS = {
    "standard": RequestLoggingConfig(
        log_request=True,
        log_response=True,
        include_body=True,
        include_response_body=False,
    ),
    "verbose": RequestLoggingConfig(
        log_request=True,
        log_response=True,
        include_body=True,
        include_response_body=True,
        log_level=LogLevel.DEBUG,
    ),
    "hipaa": RequestLoggingConfig(
        log_request=True,
        log_response=True,
        include_body=True,
        include_response_body=True,
        phi_masking=PHIMaskingConfig(
            enabled=True,
            mask_length=10,
        ),
    ),
    "minimal": RequestLoggingConfig(
        log_request=True,
        log_response=True,
        include_body=False,
        include_headers=False,
        include_response_body=False,
    ),
}


# Singleton
_request_logger: Optional[RequestLogger] = None


def get_request_logger(
    config: Optional[RequestLoggingConfig] = None
) -> RequestLogger:
    """Get or create request logger singleton."""
    global _request_logger

    if _request_logger is None:
        _request_logger = RequestLogger(config)

    return _request_logger

"""
Request Size Limit Policy

Protect against large payload attacks by limiting request body size.
Comparable to Kong Request Size Limiting plugin and MuleSoft Payload Size Limit policy.

Features:
- Configurable size limits (bytes, KB, MB)
- Per-content-type limits
- Per-route overrides
- Streaming detection
- Memory protection
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class SizeUnit(str, Enum):
    """Size units."""
    BYTES = "bytes"
    KB = "kb"
    MB = "mb"
    GB = "gb"


def parse_size(value: str) -> int:
    """
    Parse size string to bytes.

    Examples:
        "1024" -> 1024
        "1KB" -> 1024
        "10MB" -> 10485760
        "1GB" -> 1073741824
    """
    value = value.strip().upper()

    if value.endswith("GB"):
        return int(float(value[:-2]) * 1073741824)
    elif value.endswith("MB"):
        return int(float(value[:-2]) * 1048576)
    elif value.endswith("KB"):
        return int(float(value[:-2]) * 1024)
    elif value.endswith("B"):
        return int(float(value[:-1]))
    else:
        return int(value)


def format_size(bytes_value: int) -> str:
    """Format bytes to human-readable string."""
    if bytes_value >= 1073741824:
        return f"{bytes_value / 1073741824:.2f} GB"
    elif bytes_value >= 1048576:
        return f"{bytes_value / 1048576:.2f} MB"
    elif bytes_value >= 1024:
        return f"{bytes_value / 1024:.2f} KB"
    else:
        return f"{bytes_value} bytes"


@dataclass
class RequestSizeConfig:
    """Configuration for request size limiting."""
    # Default max size (bytes) - 10MB default
    max_body_size: int = 10485760

    # Max header size (bytes) - 8KB default
    max_header_size: int = 8192

    # Max URL length - 2KB default
    max_url_length: int = 2048

    # Per content-type limits (content-type pattern -> max bytes)
    content_type_limits: Dict[str, int] = field(default_factory=lambda: {
        "application/json": 10485760,  # 10MB
        "application/xml": 10485760,  # 10MB
        "text/xml": 10485760,  # 10MB
        "application/x-hl7-v2": 1048576,  # 1MB for HL7
        "multipart/form-data": 52428800,  # 50MB for file uploads
        "application/octet-stream": 104857600,  # 100MB for binary
    })

    # Per-route limits (path pattern -> max bytes)
    route_limits: Dict[str, int] = field(default_factory=dict)

    # Block or allow on limit exceeded
    block_on_exceed: bool = True

    # Log oversized requests
    log_oversized: bool = True

    # Allow streaming (chunked transfer)
    allow_streaming: bool = True

    # Max streaming chunk size
    max_chunk_size: int = 1048576  # 1MB

    # Methods to check (empty = all)
    check_methods: Set[str] = field(default_factory=lambda: {"POST", "PUT", "PATCH"})


@dataclass
class SizeLimitResult:
    """Result of size limit check."""
    allowed: bool
    actual_size: Optional[int]
    max_size: int
    exceeded_by: int = 0
    reason: Optional[str] = None
    limit_source: str = "default"  # default, content_type, route


class RequestSizeLimit:
    """
    Request size limiting policy.

    Usage:
        limiter = RequestSizeLimit(RequestSizeConfig(
            max_body_size=10485760,  # 10MB
            content_type_limits={
                "application/json": 5242880,  # 5MB for JSON
            }
        ))

        # Check size before processing
        result = await limiter.check(request)
        if not result.allowed:
            raise HTTPException(413, result.reason)

        # Use as middleware
        limiter.attach(app)
    """

    def __init__(self, config: Optional[RequestSizeConfig] = None):
        self.config = config or RequestSizeConfig()

    def _get_content_type(self, request: Request) -> str:
        """Get content type from request."""
        content_type = request.headers.get("content-type", "")
        # Strip charset and other parameters
        return content_type.split(";")[0].strip().lower()

    def _get_limit_for_request(self, request: Request) -> tuple[int, str]:
        """
        Get size limit for request.

        Returns:
            Tuple of (limit_bytes, limit_source)
        """
        path = request.url.path

        # Check route-specific limits first
        for pattern, limit in self.config.route_limits.items():
            if path.startswith(pattern) or pattern == "*":
                return limit, f"route:{pattern}"

        # Check content-type limits
        content_type = self._get_content_type(request)
        for ct_pattern, limit in self.config.content_type_limits.items():
            if content_type.startswith(ct_pattern.lower()):
                return limit, f"content_type:{ct_pattern}"

        # Return default
        return self.config.max_body_size, "default"

    async def check(self, request: Request) -> SizeLimitResult:
        """
        Check if request size is within limits.

        Args:
            request: FastAPI request

        Returns:
            SizeLimitResult
        """
        # Skip methods that don't have body
        if request.method not in self.config.check_methods:
            return SizeLimitResult(
                allowed=True,
                actual_size=0,
                max_size=self.config.max_body_size,
            )

        # Check URL length
        url_length = len(str(request.url))
        if url_length > self.config.max_url_length:
            return SizeLimitResult(
                allowed=False,
                actual_size=url_length,
                max_size=self.config.max_url_length,
                exceeded_by=url_length - self.config.max_url_length,
                reason=f"URL too long: {url_length} bytes (max: {self.config.max_url_length})",
                limit_source="url",
            )

        # Check header size
        header_size = sum(
            len(k) + len(v) + 4  # key: value\r\n
            for k, v in request.headers.items()
        )
        if header_size > self.config.max_header_size:
            return SizeLimitResult(
                allowed=False,
                actual_size=header_size,
                max_size=self.config.max_header_size,
                exceeded_by=header_size - self.config.max_header_size,
                reason=f"Headers too large: {format_size(header_size)} (max: {format_size(self.config.max_header_size)})",
                limit_source="headers",
            )

        # Get content length
        content_length = request.headers.get("content-length")
        max_size, limit_source = self._get_limit_for_request(request)

        # Check Content-Length header
        if content_length:
            try:
                size = int(content_length)
                if size > max_size:
                    if self.config.log_oversized:
                        logger.warning(
                            f"Request body too large: {format_size(size)} (max: {format_size(max_size)})",
                            extra={
                                "event_type": "request_size_exceeded",
                                "size": size,
                                "max_size": max_size,
                                "path": request.url.path,
                                "limit_source": limit_source,
                            }
                        )
                    return SizeLimitResult(
                        allowed=False,
                        actual_size=size,
                        max_size=max_size,
                        exceeded_by=size - max_size,
                        reason=f"Request body too large: {format_size(size)} (max: {format_size(max_size)})",
                        limit_source=limit_source,
                    )

                return SizeLimitResult(
                    allowed=True,
                    actual_size=size,
                    max_size=max_size,
                    limit_source=limit_source,
                )
            except ValueError:
                pass

        # Check for chunked transfer (streaming)
        transfer_encoding = request.headers.get("transfer-encoding", "")
        if "chunked" in transfer_encoding.lower():
            if not self.config.allow_streaming:
                return SizeLimitResult(
                    allowed=False,
                    actual_size=None,
                    max_size=max_size,
                    reason="Chunked transfer encoding not allowed",
                    limit_source="streaming",
                )

            # For streaming, we can't check size upfront
            return SizeLimitResult(
                allowed=True,
                actual_size=None,
                max_size=max_size,
                limit_source=limit_source,
            )

        # No content length - assume empty or will be checked later
        return SizeLimitResult(
            allowed=True,
            actual_size=0,
            max_size=max_size,
            limit_source=limit_source,
        )

    async def check_body(self, body: bytes, request: Request) -> SizeLimitResult:
        """
        Check actual body size after reading.

        Args:
            body: Request body bytes
            request: FastAPI request

        Returns:
            SizeLimitResult
        """
        max_size, limit_source = self._get_limit_for_request(request)
        size = len(body)

        if size > max_size:
            if self.config.log_oversized:
                logger.warning(
                    f"Request body too large: {format_size(size)} (max: {format_size(max_size)})",
                    extra={
                        "event_type": "request_size_exceeded",
                        "size": size,
                        "max_size": max_size,
                        "path": request.url.path,
                        "limit_source": limit_source,
                    }
                )
            return SizeLimitResult(
                allowed=False,
                actual_size=size,
                max_size=max_size,
                exceeded_by=size - max_size,
                reason=f"Request body too large: {format_size(size)} (max: {format_size(max_size)})",
                limit_source=limit_source,
            )

        return SizeLimitResult(
            allowed=True,
            actual_size=size,
            max_size=max_size,
            limit_source=limit_source,
        )

    def attach(self, app: FastAPI):
        """
        Attach size limit as middleware.

        Args:
            app: FastAPI application
        """
        limiter = self

        class SizeLimitMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Check size before processing
                result = await limiter.check(request)

                if not result.allowed and limiter.config.block_on_exceed:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=result.reason,
                        headers={
                            "X-Max-Request-Size": str(result.max_size),
                        }
                    )

                return await call_next(request)

        app.add_middleware(SizeLimitMiddleware)
        logger.info(
            f"Request size limit attached: max {format_size(self.config.max_body_size)}"
        )


# Preset configurations
SIZE_LIMIT_PRESETS = {
    "strict": RequestSizeConfig(
        max_body_size=1048576,  # 1MB
        max_header_size=4096,  # 4KB
        max_url_length=1024,  # 1KB
        allow_streaming=False,
    ),
    "standard": RequestSizeConfig(
        max_body_size=10485760,  # 10MB
        max_header_size=8192,  # 8KB
        max_url_length=2048,  # 2KB
    ),
    "file_upload": RequestSizeConfig(
        max_body_size=104857600,  # 100MB
        content_type_limits={
            "multipart/form-data": 104857600,
            "application/octet-stream": 104857600,
        },
    ),
    "healthcare": RequestSizeConfig(
        max_body_size=10485760,  # 10MB
        content_type_limits={
            "application/x-hl7-v2": 1048576,  # 1MB for HL7
            "application/fhir+json": 10485760,  # 10MB for FHIR
            "application/fhir+xml": 10485760,
            "text/xml": 10485760,
        },
    ),
}


# Singleton
_size_limiter: Optional[RequestSizeLimit] = None


def get_request_size_limiter(
    config: Optional[RequestSizeConfig] = None
) -> RequestSizeLimit:
    """Get or create request size limiter singleton."""
    global _size_limiter

    if _size_limiter is None:
        _size_limiter = RequestSizeLimit(config)

    return _size_limiter


def size_limit_from_preset(preset_name: str, **overrides) -> RequestSizeLimit:
    """Create size limiter from preset."""
    if preset_name not in SIZE_LIMIT_PRESETS:
        raise ValueError(f"Unknown preset: {preset_name}")

    config = SIZE_LIMIT_PRESETS[preset_name]

    for key, value in overrides.items():
        if hasattr(config, key):
            setattr(config, key, value)

    return RequestSizeLimit(config)

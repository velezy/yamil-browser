"""
CORS (Cross-Origin Resource Sharing) Policy

Enterprise-grade CORS configuration comparable to Kong CORS plugin and MuleSoft CORS policy.

Features:
- Configurable allowed origins (exact, wildcard, regex)
- Preflight caching (Access-Control-Max-Age)
- Credential support
- Custom headers exposure
- Per-route and per-tenant CORS rules
- Origin validation with allowlists
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Union

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import PlainTextResponse

logger = logging.getLogger(__name__)


class OriginMatchMode(str, Enum):
    """How to match origins."""
    EXACT = "exact"  # Exact string match
    WILDCARD = "wildcard"  # Simple wildcard (*.example.com)
    REGEX = "regex"  # Full regex pattern


@dataclass
class CORSConfig:
    """Configuration for CORS policy."""
    # Allowed origins
    allowed_origins: List[str] = field(default_factory=lambda: ["*"])

    # Allow credentials (cookies, auth headers)
    allow_credentials: bool = False

    # Allowed HTTP methods
    allowed_methods: List[str] = field(default_factory=lambda: [
        "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"
    ])

    # Allowed request headers
    allowed_headers: List[str] = field(default_factory=lambda: ["*"])

    # Headers to expose to browser
    expose_headers: List[str] = field(default_factory=list)

    # Preflight cache duration (seconds)
    max_age: int = 86400  # 24 hours

    # Origin matching mode
    match_mode: OriginMatchMode = OriginMatchMode.EXACT

    # Block requests with invalid origin
    strict_mode: bool = False

    # Log CORS violations
    log_violations: bool = True

    # Per-path overrides: path pattern -> CORSConfig
    path_overrides: Dict[str, "CORSConfig"] = field(default_factory=dict)


@dataclass
class CORSResult:
    """Result of CORS check."""
    allowed: bool
    origin: Optional[str]
    matched_rule: Optional[str] = None
    reason: Optional[str] = None


class CORSPolicy:
    """
    Enterprise CORS policy implementation.

    Usage:
        policy = CORSPolicy(CORSConfig(
            allowed_origins=["https://example.com", "https://*.example.com"],
            allow_credentials=True,
            max_age=3600,
        ))

        # Check if origin is allowed
        result = policy.check_origin(request)

        # Get CORS headers
        headers = policy.get_cors_headers(request)

        # Use as middleware
        policy.attach(app)
    """

    def __init__(self, config: Optional[CORSConfig] = None):
        self.config = config or CORSConfig()
        self._origin_patterns: List[Pattern] = []
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile origin patterns for matching."""
        self._origin_patterns = []

        for origin in self.config.allowed_origins:
            if origin == "*":
                # Match all origins
                self._origin_patterns.append(re.compile(r".*"))
            elif self.config.match_mode == OriginMatchMode.REGEX:
                # Use as-is for regex mode
                self._origin_patterns.append(re.compile(origin))
            elif self.config.match_mode == OriginMatchMode.WILDCARD:
                # Convert wildcard to regex
                # *.example.com -> ^https?://[^/]+\.example\.com$
                pattern = origin.replace(".", r"\.")
                pattern = pattern.replace("*", r"[^/]+")
                if not pattern.startswith("http"):
                    pattern = r"https?://" + pattern
                self._origin_patterns.append(re.compile(f"^{pattern}$"))
            else:
                # Exact match
                self._origin_patterns.append(re.compile(f"^{re.escape(origin)}$"))

    def check_origin(self, request: Request) -> CORSResult:
        """
        Check if request origin is allowed.

        Args:
            request: FastAPI request

        Returns:
            CORSResult with allowed status
        """
        origin = request.headers.get("origin")

        if not origin:
            # No origin header (same-origin request)
            return CORSResult(allowed=True, origin=None)

        # Check for wildcard
        if "*" in self.config.allowed_origins:
            return CORSResult(
                allowed=True,
                origin=origin,
                matched_rule="*"
            )

        # Check against patterns
        for i, pattern in enumerate(self._origin_patterns):
            if pattern.match(origin):
                return CORSResult(
                    allowed=True,
                    origin=origin,
                    matched_rule=self.config.allowed_origins[i]
                )

        # Origin not allowed
        if self.config.log_violations:
            logger.warning(
                f"CORS violation: origin {origin} not allowed",
                extra={
                    "event_type": "cors_violation",
                    "origin": origin,
                    "path": request.url.path,
                }
            )

        return CORSResult(
            allowed=False,
            origin=origin,
            reason=f"Origin {origin} not allowed"
        )

    def is_preflight(self, request: Request) -> bool:
        """Check if request is a CORS preflight."""
        return (
            request.method == "OPTIONS" and
            "origin" in request.headers and
            "access-control-request-method" in request.headers
        )

    def get_cors_headers(
        self,
        request: Request,
        response_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        Get CORS headers for response.

        Args:
            request: FastAPI request
            response_headers: Optional existing response headers

        Returns:
            Dict of CORS headers
        """
        headers = response_headers or {}
        origin = request.headers.get("origin")

        if not origin:
            return headers

        # Check origin
        result = self.check_origin(request)
        if not result.allowed:
            return headers

        # Set Access-Control-Allow-Origin
        if "*" in self.config.allowed_origins and not self.config.allow_credentials:
            headers["Access-Control-Allow-Origin"] = "*"
        else:
            headers["Access-Control-Allow-Origin"] = origin

        # Set credentials
        if self.config.allow_credentials:
            headers["Access-Control-Allow-Credentials"] = "true"

        # Set Vary header (important for caching)
        headers["Vary"] = "Origin"

        # For preflight requests
        if self.is_preflight(request):
            # Allowed methods
            headers["Access-Control-Allow-Methods"] = ", ".join(self.config.allowed_methods)

            # Allowed headers
            request_headers = request.headers.get("access-control-request-headers", "")
            if "*" in self.config.allowed_headers:
                headers["Access-Control-Allow-Headers"] = request_headers
            else:
                headers["Access-Control-Allow-Headers"] = ", ".join(self.config.allowed_headers)

            # Max age
            headers["Access-Control-Max-Age"] = str(self.config.max_age)

        # Expose headers (for actual requests)
        if self.config.expose_headers:
            headers["Access-Control-Expose-Headers"] = ", ".join(self.config.expose_headers)

        return headers

    def handle_preflight(self, request: Request) -> Response:
        """
        Handle CORS preflight request.

        Args:
            request: FastAPI request

        Returns:
            Preflight response
        """
        result = self.check_origin(request)

        if not result.allowed:
            if self.config.strict_mode:
                raise HTTPException(status_code=403, detail="CORS origin not allowed")
            return PlainTextResponse(status_code=204)

        headers = self.get_cors_headers(request)
        return PlainTextResponse(status_code=204, headers=headers)

    def attach(self, app: FastAPI):
        """
        Attach CORS policy as middleware.

        Args:
            app: FastAPI application
        """
        policy = self

        class CORSPolicyMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Handle preflight
                if policy.is_preflight(request):
                    return policy.handle_preflight(request)

                # Check origin for strict mode
                if policy.config.strict_mode:
                    result = policy.check_origin(request)
                    if not result.allowed:
                        raise HTTPException(status_code=403, detail="CORS origin not allowed")

                # Process request
                response = await call_next(request)

                # Add CORS headers
                cors_headers = policy.get_cors_headers(request)
                for key, value in cors_headers.items():
                    response.headers[key] = value

                return response

        app.add_middleware(CORSPolicyMiddleware)
        logger.info("CORS policy attached to FastAPI app")


# Preset configurations
CORS_PRESETS = {
    "development": CORSConfig(
        allowed_origins=["*"],
        allow_credentials=True,
        allowed_methods=["*"],
        allowed_headers=["*"],
        max_age=0,
    ),
    "production": CORSConfig(
        allowed_origins=[],  # Must be configured
        allow_credentials=True,
        allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
        allowed_headers=[
            "Content-Type",
            "Authorization",
            "X-Request-ID",
            "X-Tenant-ID",
        ],
        expose_headers=[
            "X-Request-ID",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
        ],
        max_age=86400,
        strict_mode=True,
    ),
    "public_api": CORSConfig(
        allowed_origins=["*"],
        allow_credentials=False,
        allowed_methods=["GET", "POST"],
        allowed_headers=["Content-Type", "Authorization", "X-API-Key"],
        expose_headers=["X-RateLimit-Limit", "X-RateLimit-Remaining"],
        max_age=3600,
    ),
    "internal_api": CORSConfig(
        allowed_origins=[],  # Internal only - no CORS
        allow_credentials=False,
        strict_mode=True,
    ),
}


# Singleton
_cors_policy: Optional[CORSPolicy] = None


def get_cors_policy(config: Optional[CORSConfig] = None) -> CORSPolicy:
    """Get or create CORS policy singleton."""
    global _cors_policy

    if _cors_policy is None:
        _cors_policy = CORSPolicy(config)

    return _cors_policy


def cors_from_preset(preset_name: str, **overrides) -> CORSPolicy:
    """
    Create CORS policy from preset.

    Args:
        preset_name: Name of preset (development, production, public_api, internal_api)
        **overrides: Config overrides

    Returns:
        CORSPolicy instance
    """
    if preset_name not in CORS_PRESETS:
        raise ValueError(f"Unknown preset: {preset_name}. Available: {list(CORS_PRESETS.keys())}")

    config = CORS_PRESETS[preset_name]

    # Apply overrides
    for key, value in overrides.items():
        if hasattr(config, key):
            setattr(config, key, value)

    return CORSPolicy(config)

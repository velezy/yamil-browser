"""
Production Security Headers Middleware

Adds essential security headers to all HTTP responses:
- X-Frame-Options: Prevents clickjacking
- X-Content-Type-Options: Prevents MIME sniffing
- X-XSS-Protection: Legacy XSS protection
- Strict-Transport-Security: Enforces HTTPS
- Content-Security-Policy: Controls resource loading
- Referrer-Policy: Controls referrer information
- Permissions-Policy: Controls browser features

Usage:
    from assemblyline_common.middleware.security_headers import SecurityHeadersMiddleware

    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware)
"""

import os
from typing import Optional, Dict, List
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware that adds security headers to all responses.

    Configuration via environment variables:
    - SECURITY_HSTS_ENABLED: Enable HSTS (default: true in production)
    - SECURITY_HSTS_MAX_AGE: HSTS max-age in seconds (default: 31536000 = 1 year)
    - SECURITY_FRAME_OPTIONS: X-Frame-Options value (default: DENY)
    - SECURITY_CSP_ENABLED: Enable CSP (default: true)
    - SECURITY_CSP_REPORT_ONLY: Use report-only mode (default: false)
    """

    def __init__(
        self,
        app,
        # Frame options
        frame_options: str = "DENY",
        # HSTS
        hsts_enabled: bool = True,
        hsts_max_age: int = 31536000,  # 1 year
        hsts_include_subdomains: bool = True,
        hsts_preload: bool = False,
        # CSP
        csp_enabled: bool = True,
        csp_report_only: bool = False,
        csp_directives: Optional[Dict[str, List[str]]] = None,
        # Additional headers
        referrer_policy: str = "strict-origin-when-cross-origin",
        permissions_policy: Optional[Dict[str, List[str]]] = None,
        # Paths to exclude (e.g., health checks)
        exclude_paths: Optional[List[str]] = None,
    ):
        super().__init__(app)

        # Read from environment or use defaults
        self.frame_options = os.getenv("SECURITY_FRAME_OPTIONS", frame_options)
        self.hsts_enabled = os.getenv("SECURITY_HSTS_ENABLED", str(hsts_enabled)).lower() == "true"
        self.hsts_max_age = int(os.getenv("SECURITY_HSTS_MAX_AGE", str(hsts_max_age)))
        self.hsts_include_subdomains = hsts_include_subdomains
        self.hsts_preload = hsts_preload
        self.csp_enabled = os.getenv("SECURITY_CSP_ENABLED", str(csp_enabled)).lower() == "true"
        self.csp_report_only = os.getenv("SECURITY_CSP_REPORT_ONLY", str(csp_report_only)).lower() == "true"
        self.referrer_policy = referrer_policy
        self.exclude_paths = exclude_paths or ["/health", "/ready", "/live", "/metrics"]

        # Build CSP directive
        self.csp_directives = csp_directives or self._default_csp_directives()
        self.csp_header = self._build_csp_header()

        # Build Permissions-Policy
        self.permissions_policy = permissions_policy or self._default_permissions_policy()
        self.permissions_header = self._build_permissions_header()

        # Build HSTS header
        self.hsts_header = self._build_hsts_header()

    def _default_csp_directives(self) -> Dict[str, List[str]]:
        """Default Content Security Policy for API services."""
        return {
            "default-src": ["'self'"],
            "script-src": ["'self'"],
            "style-src": ["'self'", "'unsafe-inline'"],  # Allow inline styles for Mermaid
            "img-src": ["'self'", "data:", "blob:"],
            "font-src": ["'self'"],
            "connect-src": ["'self'"],
            "frame-ancestors": ["'none'"],
            "form-action": ["'self'"],
            "base-uri": ["'self'"],
            "object-src": ["'none'"],
        }

    def _default_permissions_policy(self) -> Dict[str, List[str]]:
        """Default Permissions Policy (formerly Feature Policy)."""
        return {
            "accelerometer": [],
            "camera": [],
            "geolocation": [],
            "gyroscope": [],
            "magnetometer": [],
            "microphone": [],
            "payment": [],
            "usb": [],
        }

    def _build_csp_header(self) -> str:
        """Build the CSP header value."""
        directives = []
        for directive, sources in self.csp_directives.items():
            if sources:
                directives.append(f"{directive} {' '.join(sources)}")
            else:
                directives.append(directive)
        return "; ".join(directives)

    def _build_permissions_header(self) -> str:
        """Build the Permissions-Policy header value."""
        policies = []
        for feature, allowlist in self.permissions_policy.items():
            if not allowlist:
                policies.append(f"{feature}=()")
            else:
                policies.append(f"{feature}=({' '.join(allowlist)})")
        return ", ".join(policies)

    def _build_hsts_header(self) -> str:
        """Build the HSTS header value."""
        value = f"max-age={self.hsts_max_age}"
        if self.hsts_include_subdomains:
            value += "; includeSubDomains"
        if self.hsts_preload:
            value += "; preload"
        return value

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        # Skip security headers for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return response

        # Core security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = self.frame_options
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = self.referrer_policy

        # HSTS (only for HTTPS or when explicitly enabled)
        if self.hsts_enabled:
            response.headers["Strict-Transport-Security"] = self.hsts_header

        # Content Security Policy
        if self.csp_enabled:
            header_name = "Content-Security-Policy-Report-Only" if self.csp_report_only else "Content-Security-Policy"
            response.headers[header_name] = self.csp_header

        # Permissions Policy
        if self.permissions_header:
            response.headers["Permissions-Policy"] = self.permissions_header

        # Remove potentially dangerous headers
        if "Server" in response.headers:
            del response.headers["Server"]
        if "X-Powered-By" in response.headers:
            del response.headers["X-Powered-By"]

        return response


class CORSSecurityMiddleware(BaseHTTPMiddleware):
    """
    Production-ready CORS middleware with security focus.

    Unlike permissive CORS, this enforces:
    - Explicit allowed origins (no wildcards in production)
    - Credential handling with origin validation
    - Preflight caching
    """

    def __init__(
        self,
        app,
        allowed_origins: Optional[List[str]] = None,
        allowed_methods: Optional[List[str]] = None,
        allowed_headers: Optional[List[str]] = None,
        expose_headers: Optional[List[str]] = None,
        allow_credentials: bool = True,
        max_age: int = 86400,  # 24 hours
    ):
        super().__init__(app)

        # Get origins from environment or parameter
        env_origins = os.getenv("CORS_ALLOWED_ORIGINS", "")
        if env_origins:
            self.allowed_origins = [o.strip() for o in env_origins.split(",")]
        else:
            self.allowed_origins = allowed_origins or []

        self.allowed_methods = allowed_methods or [
            "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"
        ]
        self.allowed_headers = allowed_headers or [
            "Authorization",
            "Content-Type",
            "X-Request-ID",
            "X-Correlation-ID",
            "Accept",
            "Origin",
        ]
        self.expose_headers = expose_headers or [
            "X-Request-ID",
            "X-Correlation-ID",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
        ]
        self.allow_credentials = allow_credentials
        self.max_age = max_age

    def _is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is in allowed list."""
        if not origin:
            return False

        # Exact match
        if origin in self.allowed_origins:
            return True

        # Pattern match (e.g., *.example.com)
        for allowed in self.allowed_origins:
            if allowed.startswith("*."):
                domain = allowed[2:]
                if origin.endswith(domain) or origin.endswith(f".{domain}"):
                    return True

        return False

    async def dispatch(self, request: Request, call_next) -> Response:
        origin = request.headers.get("origin", "")

        # Handle preflight
        if request.method == "OPTIONS":
            if self._is_origin_allowed(origin):
                response = Response(status_code=204)
                response.headers["Access-Control-Allow-Origin"] = origin
                response.headers["Access-Control-Allow-Methods"] = ", ".join(self.allowed_methods)
                response.headers["Access-Control-Allow-Headers"] = ", ".join(self.allowed_headers)
                response.headers["Access-Control-Max-Age"] = str(self.max_age)
                if self.allow_credentials:
                    response.headers["Access-Control-Allow-Credentials"] = "true"
                return response
            else:
                return Response(status_code=403, content="Origin not allowed")

        # Handle actual request
        response = await call_next(request)

        if self._is_origin_allowed(origin):
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Expose-Headers"] = ", ".join(self.expose_headers)
            if self.allow_credentials:
                response.headers["Access-Control-Allow-Credentials"] = "true"

        return response


def get_security_middleware_stack(app, production: bool = None):
    """
    Apply recommended security middleware stack to a FastAPI app.

    Args:
        app: FastAPI application instance
        production: Whether in production mode (auto-detected from DEBUG env var)

    Returns:
        The app with middleware applied
    """
    if production is None:
        production = os.getenv("DEBUG", "false").lower() != "true"

    # Security headers (always apply)
    app.add_middleware(
        SecurityHeadersMiddleware,
        hsts_enabled=production,  # Only enable HSTS in production
        csp_report_only=not production,  # Report-only in development
    )

    # CORS (with proper origins)
    allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", "").split(",")
    if not production:
        # More permissive in development
        allowed_origins = [
            "http://localhost:3570",
            "http://localhost:3580",
            "http://localhost:5173",
            "http://127.0.0.1:3570",
            "http://127.0.0.1:5173",
        ]

    if allowed_origins and allowed_origins[0]:
        app.add_middleware(
            CORSSecurityMiddleware,
            allowed_origins=allowed_origins,
        )

    return app

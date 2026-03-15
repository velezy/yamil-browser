"""
Shared Middleware for Production Hardening

Provides:
- SecurityHeadersMiddleware: Adds security headers (CSP, HSTS, etc.)
- CORSSecurityMiddleware: Production-ready CORS with origin validation
- get_security_middleware_stack: Apply all security middleware at once
"""

from .security_headers import (
    SecurityHeadersMiddleware,
    CORSSecurityMiddleware,
    get_security_middleware_stack,
)

__all__ = [
    "SecurityHeadersMiddleware",
    "CORSSecurityMiddleware",
    "get_security_middleware_stack",
]

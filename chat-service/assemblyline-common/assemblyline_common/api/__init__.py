"""
Shared API Utilities

Provides:
- API versioning (URL and header-based)
- Standardized response formats
- Error handling utilities
"""

from .versioning import (
    VersionedAPIRouter,
    APIVersionMiddleware,
    version,
    require_version,
    get_api_version,
    get_version_info,
    create_versioned_app,
    SUPPORTED_VERSIONS,
    DEFAULT_VERSION,
    LATEST_VERSION,
)

__all__ = [
    # Versioning
    "VersionedAPIRouter",
    "APIVersionMiddleware",
    "version",
    "require_version",
    "get_api_version",
    "get_version_info",
    "create_versioned_app",
    "SUPPORTED_VERSIONS",
    "DEFAULT_VERSION",
    "LATEST_VERSION",
]

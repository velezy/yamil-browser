"""
API Versioning & Lifecycle Management for Logic Weaver

Enterprise-grade API versioning with:
- Multiple versioning strategies (URL path, header, query param)
- Deprecation management with sunset headers
- Migration guides and backward compatibility
- Version negotiation

Comparison:
| Feature                | Kong | Apigee | MuleSoft | Logic Weaver |
|-----------------------|------|--------|----------|--------------|
| URL Path Versioning   | Yes  | Yes    | Yes      | Yes          |
| Header Versioning     | Yes  | Yes    | Yes      | Yes          |
| Query Versioning      | No   | Yes    | No       | Yes          |
| Sunset Headers        | No   | No     | No       | Yes (RFC 8594) |
| Deprecation Warnings  | No   | Yes    | Yes      | Yes          |
| Migration Guides      | No   | No     | No       | Yes          |
| Version Negotiation   | No   | No     | No       | Yes          |
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

logger = logging.getLogger(__name__)


class VersioningStrategy(Enum):
    """API versioning strategies."""
    URL_PATH = "url_path"         # /api/v1/resource, /api/v2/resource
    HEADER = "header"             # Accept-Version: v1, X-API-Version: 2
    QUERY_PARAM = "query_param"   # ?api-version=1, ?version=v1
    MEDIA_TYPE = "media_type"     # Accept: application/vnd.api.v1+json


class VersionStatus(Enum):
    """API version lifecycle status."""
    CURRENT = "current"           # Active, recommended version
    SUPPORTED = "supported"       # Still supported, not latest
    DEPRECATED = "deprecated"     # Working but scheduled for removal
    SUNSET = "sunset"             # Removed, returns 410 Gone
    BETA = "beta"                 # Preview version


@dataclass
class APIVersion:
    """Represents an API version."""
    version: str                  # "v1", "v2", "2.0", etc.
    status: VersionStatus = VersionStatus.CURRENT
    release_date: Optional[datetime] = None
    deprecation_date: Optional[datetime] = None
    sunset_date: Optional[datetime] = None
    successor: Optional[str] = None  # Version that replaces this one
    changelog_url: Optional[str] = None
    migration_guide_url: Optional[str] = None
    min_client_version: Optional[str] = None

    def __post_init__(self):
        # Normalize version string
        if not self.version.startswith("v"):
            self.version = f"v{self.version}"

    @property
    def numeric_version(self) -> tuple[int, ...]:
        """Extract numeric version for comparison."""
        match = re.search(r'(\d+)(?:\.(\d+))?(?:\.(\d+))?', self.version)
        if match:
            return tuple(int(g) if g else 0 for g in match.groups())
        return (0,)

    @property
    def is_deprecated(self) -> bool:
        return self.status == VersionStatus.DEPRECATED

    @property
    def is_sunset(self) -> bool:
        return self.status == VersionStatus.SUNSET

    @property
    def days_until_sunset(self) -> Optional[int]:
        if self.sunset_date:
            delta = self.sunset_date - datetime.now()
            return max(0, delta.days)
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status.value,
            "release_date": self.release_date.isoformat() if self.release_date else None,
            "deprecation_date": self.deprecation_date.isoformat() if self.deprecation_date else None,
            "sunset_date": self.sunset_date.isoformat() if self.sunset_date else None,
            "successor": self.successor,
            "changelog_url": self.changelog_url,
            "migration_guide_url": self.migration_guide_url,
        }


@dataclass
class DeprecatedEndpoint:
    """Represents a deprecated endpoint."""
    path: str
    method: str
    deprecated_in: str            # Version where deprecation started
    removed_in: Optional[str] = None  # Version where it will be removed
    sunset_date: Optional[datetime] = None
    replacement_path: Optional[str] = None
    replacement_method: Optional[str] = None
    migration_notes: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "method": self.method,
            "deprecated_in": self.deprecated_in,
            "removed_in": self.removed_in,
            "sunset_date": self.sunset_date.isoformat() if self.sunset_date else None,
            "replacement_path": self.replacement_path,
            "replacement_method": self.replacement_method,
            "migration_notes": self.migration_notes,
        }


@dataclass
class VersioningConfig:
    """Configuration for API versioning."""
    # Versioning strategy (can use multiple)
    strategies: list[VersioningStrategy] = field(default_factory=lambda: [
        VersioningStrategy.URL_PATH,
        VersioningStrategy.HEADER,
    ])

    # Strategy priority (first match wins)
    strategy_priority: list[VersioningStrategy] = field(default_factory=lambda: [
        VersioningStrategy.URL_PATH,
        VersioningStrategy.QUERY_PARAM,
        VersioningStrategy.HEADER,
        VersioningStrategy.MEDIA_TYPE,
    ])

    # Default version if none specified
    default_version: str = "v1"

    # Header names for header-based versioning
    version_headers: list[str] = field(default_factory=lambda: [
        "Accept-Version",
        "X-API-Version",
        "API-Version",
    ])

    # Query parameter names
    version_query_params: list[str] = field(default_factory=lambda: [
        "api-version",
        "version",
        "v",
    ])

    # URL path pattern (regex)
    url_version_pattern: str = r"/api/(v\d+(?:\.\d+)?)"

    # Media type pattern
    media_type_pattern: str = r"application/vnd\.[\w\.]+\.(v\d+)\+json"

    # Deprecation settings
    deprecation_warning_header: str = "Deprecation"  # RFC 8594
    sunset_header: str = "Sunset"  # RFC 8594
    link_header: str = "Link"  # For migration guide links

    # Response headers to add
    add_version_header: bool = True
    version_response_header: str = "X-API-Version"

    # Strict mode - reject unknown versions
    strict_version_check: bool = False


class VersionRegistry:
    """
    Registry for API versions and deprecated endpoints.

    Maintains the lifecycle of all API versions.
    """

    def __init__(self, config: Optional[VersioningConfig] = None):
        self.config = config or VersioningConfig()
        self._versions: dict[str, APIVersion] = {}
        self._deprecated_endpoints: list[DeprecatedEndpoint] = []
        self._current_version: Optional[str] = None

        # Register default v1
        self.register_version(APIVersion(
            version="v1",
            status=VersionStatus.CURRENT,
            release_date=datetime(2024, 1, 1),
        ))

    def register_version(self, version: APIVersion) -> None:
        """Register an API version."""
        self._versions[version.version] = version

        # Track current version
        if version.status == VersionStatus.CURRENT:
            self._current_version = version.version

        logger.info(f"Registered API version: {version.version} ({version.status.value})")

    def get_version(self, version_str: str) -> Optional[APIVersion]:
        """Get version info."""
        # Normalize version string
        if not version_str.startswith("v"):
            version_str = f"v{version_str}"
        return self._versions.get(version_str)

    def get_current_version(self) -> Optional[APIVersion]:
        """Get the current (recommended) version."""
        if self._current_version:
            return self._versions.get(self._current_version)
        return None

    def get_all_versions(self) -> list[APIVersion]:
        """Get all registered versions."""
        return list(self._versions.values())

    def get_supported_versions(self) -> list[APIVersion]:
        """Get all non-sunset versions."""
        return [v for v in self._versions.values()
                if v.status != VersionStatus.SUNSET]

    def deprecate_version(
        self,
        version: str,
        sunset_date: Optional[datetime] = None,
        successor: Optional[str] = None,
        migration_guide_url: Optional[str] = None,
    ) -> None:
        """Mark a version as deprecated."""
        v = self.get_version(version)
        if v:
            v.status = VersionStatus.DEPRECATED
            v.deprecation_date = datetime.now()
            v.sunset_date = sunset_date
            v.successor = successor
            v.migration_guide_url = migration_guide_url
            logger.warning(f"API version {version} marked as deprecated")

    def sunset_version(self, version: str) -> None:
        """Mark a version as sunset (removed)."""
        v = self.get_version(version)
        if v:
            v.status = VersionStatus.SUNSET
            logger.warning(f"API version {version} is now sunset")

    def register_deprecated_endpoint(self, endpoint: DeprecatedEndpoint) -> None:
        """Register a deprecated endpoint."""
        self._deprecated_endpoints.append(endpoint)
        logger.warning(f"Deprecated endpoint: {endpoint.method} {endpoint.path}")

    def get_deprecated_endpoint(
        self,
        path: str,
        method: str,
    ) -> Optional[DeprecatedEndpoint]:
        """Check if an endpoint is deprecated."""
        for ep in self._deprecated_endpoints:
            # Match path pattern
            if re.match(ep.path.replace("{", "(?P<").replace("}", ">[^/]+)"), path):
                if ep.method.upper() == method.upper() or ep.method == "*":
                    return ep
        return None

    def is_version_valid(self, version: str) -> bool:
        """Check if a version exists and is not sunset."""
        v = self.get_version(version)
        return v is not None and v.status != VersionStatus.SUNSET


class VersionDetector:
    """
    Detects API version from requests.

    Supports multiple detection strategies with configurable priority.
    """

    def __init__(self, config: VersioningConfig):
        self.config = config

    def detect(self, request: Request) -> Optional[str]:
        """Detect API version from request using configured strategies."""
        for strategy in self.config.strategy_priority:
            if strategy not in self.config.strategies:
                continue

            version = self._detect_by_strategy(request, strategy)
            if version:
                return version

        return self.config.default_version

    def _detect_by_strategy(
        self,
        request: Request,
        strategy: VersioningStrategy,
    ) -> Optional[str]:
        """Detect version using a specific strategy."""
        if strategy == VersioningStrategy.URL_PATH:
            return self._detect_from_url(request)
        elif strategy == VersioningStrategy.HEADER:
            return self._detect_from_header(request)
        elif strategy == VersioningStrategy.QUERY_PARAM:
            return self._detect_from_query(request)
        elif strategy == VersioningStrategy.MEDIA_TYPE:
            return self._detect_from_media_type(request)
        return None

    def _detect_from_url(self, request: Request) -> Optional[str]:
        """Extract version from URL path."""
        match = re.search(self.config.url_version_pattern, str(request.url.path))
        if match:
            return match.group(1)
        return None

    def _detect_from_header(self, request: Request) -> Optional[str]:
        """Extract version from HTTP headers."""
        for header in self.config.version_headers:
            value = request.headers.get(header)
            if value:
                # Normalize: "v1", "1", "1.0" -> "v1", "v1", "v1.0"
                if not value.startswith("v"):
                    value = f"v{value}"
                return value
        return None

    def _detect_from_query(self, request: Request) -> Optional[str]:
        """Extract version from query parameters."""
        for param in self.config.version_query_params:
            value = request.query_params.get(param)
            if value:
                if not value.startswith("v"):
                    value = f"v{value}"
                return value
        return None

    def _detect_from_media_type(self, request: Request) -> Optional[str]:
        """Extract version from Accept header media type."""
        accept = request.headers.get("Accept", "")
        match = re.search(self.config.media_type_pattern, accept)
        if match:
            return match.group(1)
        return None


class DeprecationManager:
    """
    Manages deprecation warnings and sunset headers.

    Implements RFC 8594 for HTTP Deprecation and Sunset headers.
    """

    def __init__(
        self,
        registry: VersionRegistry,
        config: VersioningConfig,
    ):
        self.registry = registry
        self.config = config

    def add_deprecation_headers(
        self,
        response: Response,
        version: APIVersion,
        deprecated_endpoint: Optional[DeprecatedEndpoint] = None,
    ) -> None:
        """Add deprecation-related headers to response."""
        # Add version header
        if self.config.add_version_header:
            response.headers[self.config.version_response_header] = version.version

        # Add deprecation warning
        if version.is_deprecated or deprecated_endpoint:
            self._add_deprecation_header(response, version, deprecated_endpoint)

        # Add sunset header
        sunset_date = None
        if deprecated_endpoint and deprecated_endpoint.sunset_date:
            sunset_date = deprecated_endpoint.sunset_date
        elif version.sunset_date:
            sunset_date = version.sunset_date

        if sunset_date:
            self._add_sunset_header(response, sunset_date)

        # Add link to migration guide
        migration_url = None
        if deprecated_endpoint and deprecated_endpoint.migration_notes:
            # Could be a URL or inline text
            if deprecated_endpoint.migration_notes.startswith("http"):
                migration_url = deprecated_endpoint.migration_notes
        elif version.migration_guide_url:
            migration_url = version.migration_guide_url

        if migration_url:
            self._add_link_header(response, migration_url)

    def _add_deprecation_header(
        self,
        response: Response,
        version: APIVersion,
        deprecated_endpoint: Optional[DeprecatedEndpoint],
    ) -> None:
        """Add RFC 8594 Deprecation header."""
        # Deprecation: true
        # or Deprecation: @1735689600  (Unix timestamp)
        if version.deprecation_date:
            timestamp = int(version.deprecation_date.timestamp())
            response.headers[self.config.deprecation_warning_header] = f"@{timestamp}"
        else:
            response.headers[self.config.deprecation_warning_header] = "true"

        # Add Warning header for compatibility
        warning_msg = "299 - \"This API version is deprecated"
        if version.successor:
            warning_msg += f". Please migrate to {version.successor}"
        if deprecated_endpoint and deprecated_endpoint.replacement_path:
            warning_msg += f". Use {deprecated_endpoint.replacement_path} instead"
        warning_msg += "\""
        response.headers["Warning"] = warning_msg

    def _add_sunset_header(self, response: Response, sunset_date: datetime) -> None:
        """Add RFC 8594 Sunset header."""
        # Format: Sat, 31 Dec 2025 23:59:59 GMT
        sunset_str = sunset_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
        response.headers[self.config.sunset_header] = sunset_str

    def _add_link_header(self, response: Response, migration_url: str) -> None:
        """Add Link header pointing to migration guide."""
        # Link: <https://api.example.com/migration/v1-to-v2>; rel="deprecation"
        link_value = f'<{migration_url}>; rel="deprecation"'

        # Append to existing Link header if present
        existing = response.headers.get(self.config.link_header, "")
        if existing:
            link_value = f"{existing}, {link_value}"

        response.headers[self.config.link_header] = link_value

    def create_sunset_response(
        self,
        version: APIVersion,
        request: Request,
    ) -> JSONResponse:
        """Create a 410 Gone response for sunset versions."""
        current = self.registry.get_current_version()

        return JSONResponse(
            status_code=410,
            content={
                "error": "Gone",
                "message": f"API version {version.version} has been sunset and is no longer available.",
                "sunset_date": version.sunset_date.isoformat() if version.sunset_date else None,
                "current_version": current.version if current else None,
                "migration_guide": version.migration_guide_url,
            },
            headers={
                self.config.version_response_header: version.version,
            }
        )


class APIVersionMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware for API versioning.

    Features:
    - Automatic version detection
    - Deprecation warnings
    - Sunset handling
    - Version negotiation
    """

    def __init__(
        self,
        app,
        registry: Optional[VersionRegistry] = None,
        config: Optional[VersioningConfig] = None,
    ):
        super().__init__(app)
        self.config = config or VersioningConfig()
        self.registry = registry or VersionRegistry(self.config)
        self.detector = VersionDetector(self.config)
        self.deprecation_manager = DeprecationManager(self.registry, self.config)

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request with version handling."""
        # Detect API version
        version_str = self.detector.detect(request)

        # Get version info
        version = self.registry.get_version(version_str)

        # Handle unknown version
        if version is None:
            if self.config.strict_version_check:
                return JSONResponse(
                    status_code=400,
                    content={
                        "error": "Bad Request",
                        "message": f"Unknown API version: {version_str}",
                        "supported_versions": [v.version for v in self.registry.get_supported_versions()],
                    }
                )
            # Fall back to default version
            version = self.registry.get_version(self.config.default_version)

        # Handle sunset version
        if version and version.is_sunset:
            return self.deprecation_manager.create_sunset_response(version, request)

        # Store version in request state for access in endpoints
        request.state.api_version = version
        request.state.api_version_str = version.version if version else self.config.default_version

        # Check for deprecated endpoint
        deprecated_endpoint = self.registry.get_deprecated_endpoint(
            str(request.url.path),
            request.method,
        )

        # Process request
        response = await call_next(request)

        # Add version and deprecation headers
        if version:
            self.deprecation_manager.add_deprecation_headers(
                response,
                version,
                deprecated_endpoint,
            )

        return response


# Decorator for version-specific endpoints
F = TypeVar('F', bound=Callable[..., Any])


def api_version(
    min_version: Optional[str] = None,
    max_version: Optional[str] = None,
    deprecated_in: Optional[str] = None,
    removed_in: Optional[str] = None,
    replacement: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator to mark endpoint version constraints.

    Usage:
        @app.get("/users")
        @api_version(min_version="v1", deprecated_in="v2", replacement="/api/v2/accounts")
        async def get_users():
            pass
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get request from args (FastAPI dependency injection)
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break

            if request and hasattr(request.state, 'api_version'):
                version = request.state.api_version

                # Check version constraints
                if min_version and version:
                    min_v = APIVersion(min_version)
                    if version.numeric_version < min_v.numeric_version:
                        return JSONResponse(
                            status_code=400,
                            content={
                                "error": "Version Mismatch",
                                "message": f"This endpoint requires API version {min_version} or later",
                            }
                        )

                if max_version and version:
                    max_v = APIVersion(max_version)
                    if version.numeric_version > max_v.numeric_version:
                        return JSONResponse(
                            status_code=400,
                            content={
                                "error": "Version Mismatch",
                                "message": f"This endpoint is not available in API version {version.version}",
                                "max_version": max_version,
                                "replacement": replacement,
                            }
                        )

            return await func(*args, **kwargs)

        # Store version metadata on function
        wrapper._api_version = {
            "min_version": min_version,
            "max_version": max_version,
            "deprecated_in": deprecated_in,
            "removed_in": removed_in,
            "replacement": replacement,
        }

        return wrapper  # type: ignore
    return decorator


def deprecated(
    since: str,
    sunset: Optional[str] = None,
    replacement: Optional[str] = None,
    message: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator to mark an endpoint as deprecated.

    Usage:
        @app.get("/old-endpoint")
        @deprecated(since="v2", sunset="2025-12-31", replacement="/api/v2/new-endpoint")
        async def old_endpoint():
            pass
    """
    return api_version(deprecated_in=since, replacement=replacement)


# Factory function
def create_versioning_middleware(
    app,
    versions: Optional[list[APIVersion]] = None,
    deprecated_endpoints: Optional[list[DeprecatedEndpoint]] = None,
    config: Optional[VersioningConfig] = None,
) -> APIVersionMiddleware:
    """
    Factory to create configured versioning middleware.

    Usage:
        middleware = create_versioning_middleware(
            app,
            versions=[
                APIVersion("v1", status=VersionStatus.DEPRECATED, sunset_date=datetime(2025, 12, 31)),
                APIVersion("v2", status=VersionStatus.CURRENT),
            ],
            deprecated_endpoints=[
                DeprecatedEndpoint(
                    path="/api/v1/users",
                    method="GET",
                    deprecated_in="v1",
                    replacement_path="/api/v2/accounts",
                ),
            ],
        )
    """
    config = config or VersioningConfig()
    registry = VersionRegistry(config)

    if versions:
        for version in versions:
            registry.register_version(version)

    if deprecated_endpoints:
        for endpoint in deprecated_endpoints:
            registry.register_deprecated_endpoint(endpoint)

    return APIVersionMiddleware(app, registry, config)


# Utility functions
def get_api_version(request: Request) -> Optional[str]:
    """Get API version from request state."""
    if hasattr(request.state, 'api_version_str'):
        return request.state.api_version_str
    return None


def get_api_version_info(request: Request) -> Optional[APIVersion]:
    """Get full API version info from request state."""
    if hasattr(request.state, 'api_version'):
        return request.state.api_version
    return None


# Version info endpoint helper
def create_versions_endpoint(registry: VersionRegistry):
    """
    Create an endpoint that returns version information.

    Usage:
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/api/versions")
        async def get_versions():
            return create_versions_endpoint(registry)()
    """
    def versions_endpoint():
        current = registry.get_current_version()
        return {
            "current_version": current.version if current else None,
            "versions": [v.to_dict() for v in registry.get_all_versions()],
            "supported_versions": [v.version for v in registry.get_supported_versions()],
        }
    return versions_endpoint

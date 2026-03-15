"""
API Versioning Module

Provides URL-based and header-based API versioning for FastAPI applications.

Versioning Strategies:
1. URL Path: /api/v1/users, /api/v2/users
2. Header: X-API-Version: 1, X-API-Version: 2
3. Query Parameter: /api/users?version=1

Usage:
    from fastapi import FastAPI
    from assemblyline_common.api.versioning import (
        VersionedAPIRouter,
        APIVersionMiddleware,
        version,
    )

    app = FastAPI()

    # Add versioning middleware
    app.add_middleware(APIVersionMiddleware, default_version="1")

    # Create versioned routers
    v1_router = VersionedAPIRouter(version="1", prefix="/api/v1")
    v2_router = VersionedAPIRouter(version="2", prefix="/api/v2")

    @v1_router.get("/users")
    async def get_users_v1():
        return {"version": 1, "users": [...]}

    @v2_router.get("/users")
    async def get_users_v2():
        return {"version": 2, "users": [...], "pagination": {...}}

    app.include_router(v1_router)
    app.include_router(v2_router)
"""

import os
import re
from typing import Optional, List, Callable, Dict, Any
from functools import wraps
from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.routing import APIRoute
from starlette.middleware.base import BaseHTTPMiddleware


# =============================================================================
# VERSION CONFIGURATION
# =============================================================================

# Supported API versions (newest first)
SUPPORTED_VERSIONS = ["2", "1"]
DEFAULT_VERSION = os.getenv("API_DEFAULT_VERSION", "1")
LATEST_VERSION = SUPPORTED_VERSIONS[0]

# Deprecated versions (still work but return warning header)
DEPRECATED_VERSIONS = []

# Sunset versions (will be removed)
SUNSET_VERSIONS: Dict[str, str] = {}  # version -> sunset date


# =============================================================================
# VERSIONED ROUTER
# =============================================================================

class VersionedAPIRouter(APIRouter):
    """
    APIRouter with version metadata.

    Example:
        v1 = VersionedAPIRouter(version="1", prefix="/api/v1")
        v2 = VersionedAPIRouter(version="2", prefix="/api/v2")

        @v1.get("/users")
        def get_users_v1(): ...

        @v2.get("/users")
        def get_users_v2(): ...
    """

    def __init__(
        self,
        version: str,
        prefix: str = "",
        deprecated: bool = False,
        sunset_date: Optional[str] = None,
        **kwargs
    ):
        # Ensure prefix includes version
        if not prefix:
            prefix = f"/api/v{version}"
        elif not re.search(r'/v\d+', prefix):
            prefix = f"{prefix}/v{version}"

        super().__init__(prefix=prefix, **kwargs)

        self.api_version = version
        self.is_deprecated = deprecated or version in DEPRECATED_VERSIONS
        self.sunset_date = sunset_date or SUNSET_VERSIONS.get(version)

    def api_route(self, path: str, **kwargs):
        """Override to add version metadata to routes."""
        def decorator(func):
            # Add version info to function
            func._api_version = self.api_version
            func._deprecated = self.is_deprecated
            func._sunset_date = self.sunset_date

            return super(VersionedAPIRouter, self).api_route(path, **kwargs)(func)
        return decorator


# =============================================================================
# VERSION MIDDLEWARE
# =============================================================================

class APIVersionMiddleware(BaseHTTPMiddleware):
    """
    Middleware that handles API versioning via headers.

    Supports:
    - X-API-Version header for version selection
    - Adds X-API-Version response header
    - Adds deprecation warnings for old versions
    - Adds sunset headers for versions being removed

    Usage:
        app.add_middleware(
            APIVersionMiddleware,
            default_version="1",
            supported_versions=["1", "2"],
        )
    """

    def __init__(
        self,
        app,
        default_version: str = DEFAULT_VERSION,
        supported_versions: Optional[List[str]] = None,
        version_header: str = "X-API-Version",
        version_param: str = "api_version",
    ):
        super().__init__(app)
        self.default_version = default_version
        self.supported_versions = supported_versions or SUPPORTED_VERSIONS
        self.version_header = version_header
        self.version_param = version_param

    async def dispatch(self, request: Request, call_next) -> Response:
        # Extract version from various sources
        version = self._extract_version(request)

        # Validate version
        if version and version not in self.supported_versions:
            return Response(
                content=f"Unsupported API version: {version}. Supported: {', '.join(self.supported_versions)}",
                status_code=400,
                headers={"Content-Type": "text/plain"},
            )

        # Store version in request state
        request.state.api_version = version or self.default_version

        # Call next middleware/handler
        response = await call_next(request)

        # Add version headers to response
        response.headers[self.version_header] = request.state.api_version
        response.headers["X-API-Supported-Versions"] = ", ".join(self.supported_versions)

        # Add deprecation warning if applicable
        if request.state.api_version in DEPRECATED_VERSIONS:
            response.headers["Deprecation"] = "true"
            response.headers["X-API-Deprecation-Warning"] = (
                f"API version {request.state.api_version} is deprecated. "
                f"Please upgrade to version {LATEST_VERSION}."
            )

        # Add sunset header if applicable
        sunset_date = SUNSET_VERSIONS.get(request.state.api_version)
        if sunset_date:
            response.headers["Sunset"] = sunset_date
            response.headers["X-API-Sunset-Warning"] = (
                f"API version {request.state.api_version} will be removed on {sunset_date}."
            )

        return response

    def _extract_version(self, request: Request) -> Optional[str]:
        """Extract API version from request."""
        # 1. Check header
        header_version = request.headers.get(self.version_header)
        if header_version:
            return header_version.strip()

        # 2. Check URL path (e.g., /api/v1/...)
        path = request.url.path
        match = re.search(r'/v(\d+)/', path)
        if match:
            return match.group(1)

        # 3. Check query parameter
        query_version = request.query_params.get(self.version_param)
        if query_version:
            return query_version.strip()

        return None


# =============================================================================
# DECORATORS
# =============================================================================

def version(
    api_version: str,
    deprecated: bool = False,
    sunset_date: Optional[str] = None,
):
    """
    Decorator to mark an endpoint with version metadata.

    Example:
        @app.get("/api/v1/users")
        @version("1", deprecated=True, sunset_date="2025-01-01")
        async def get_users():
            return {"users": [...]}
    """
    def decorator(func: Callable) -> Callable:
        func._api_version = api_version
        func._deprecated = deprecated or api_version in DEPRECATED_VERSIONS
        func._sunset_date = sunset_date or SUNSET_VERSIONS.get(api_version)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        return wrapper
    return decorator


def require_version(min_version: str, max_version: Optional[str] = None):
    """
    Decorator to require a specific API version range.

    Example:
        @app.get("/api/users")
        @require_version("2")  # Requires v2 or higher
        async def get_users_v2_only(request: Request):
            ...

        @require_version("1", "2")  # Requires v1 or v2
        async def get_users_legacy(request: Request):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, request: Request = None, **kwargs):
            # Find request in args if not in kwargs
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                raise HTTPException(500, "Request object not found")

            current_version = getattr(request.state, 'api_version', DEFAULT_VERSION)

            # Check minimum version
            if int(current_version) < int(min_version):
                raise HTTPException(
                    400,
                    f"This endpoint requires API version {min_version} or higher. "
                    f"Current version: {current_version}"
                )

            # Check maximum version
            if max_version and int(current_version) > int(max_version):
                raise HTTPException(
                    400,
                    f"This endpoint is not available in API version {current_version}. "
                    f"Maximum supported version: {max_version}"
                )

            return await func(*args, **kwargs)

        return wrapper
    return decorator


# =============================================================================
# UTILITIES
# =============================================================================

def get_api_version(request: Request) -> str:
    """Get the API version from request state."""
    return getattr(request.state, 'api_version', DEFAULT_VERSION)


def is_deprecated_version(version: str) -> bool:
    """Check if a version is deprecated."""
    return version in DEPRECATED_VERSIONS


def get_version_info() -> Dict[str, Any]:
    """Get API version information."""
    return {
        "supported_versions": SUPPORTED_VERSIONS,
        "default_version": DEFAULT_VERSION,
        "latest_version": LATEST_VERSION,
        "deprecated_versions": DEPRECATED_VERSIONS,
        "sunset_schedule": SUNSET_VERSIONS,
    }


# =============================================================================
# VERSION ROUTER FACTORY
# =============================================================================

def create_versioned_app(
    title: str,
    description: str = "",
    versions: Optional[List[str]] = None,
) -> Dict[str, VersionedAPIRouter]:
    """
    Create versioned routers for an application.

    Returns a dict of version -> router that can be used like:

        routers = create_versioned_app("My API", versions=["1", "2"])

        @routers["1"].get("/users")
        async def get_users_v1(): ...

        @routers["2"].get("/users")
        async def get_users_v2(): ...

        for router in routers.values():
            app.include_router(router)
    """
    versions = versions or SUPPORTED_VERSIONS
    routers = {}

    for v in versions:
        deprecated = v in DEPRECATED_VERSIONS
        sunset = SUNSET_VERSIONS.get(v)

        routers[v] = VersionedAPIRouter(
            version=v,
            deprecated=deprecated,
            sunset_date=sunset,
            tags=[f"v{v}"],
        )

    return routers


# Exports
__all__ = [
    "VersionedAPIRouter",
    "APIVersionMiddleware",
    "version",
    "require_version",
    "get_api_version",
    "is_deprecated_version",
    "get_version_info",
    "create_versioned_app",
    "SUPPORTED_VERSIONS",
    "DEFAULT_VERSION",
    "LATEST_VERSION",
]

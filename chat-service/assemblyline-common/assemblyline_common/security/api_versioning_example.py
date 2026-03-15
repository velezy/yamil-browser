"""
API Versioning Integration Example

Demonstrates how to use the API versioning middleware with FastAPI.
Run with: python -m uvicorn api_versioning_example:app --port 8080
"""

from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from assemblyline_common.security.api_versioning import (
    APIVersion,
    VersionStatus,
    VersioningConfig,
    VersionRegistry,
    DeprecatedEndpoint,
    APIVersionMiddleware,
    create_versioning_middleware,
    get_api_version,
    get_api_version_info,
    api_version,
    deprecated,
)

# Create FastAPI app
app = FastAPI(
    title="API Versioning Demo",
    description="Demonstrates API versioning with deprecation management",
)

# Configure versioning
config = VersioningConfig(
    default_version="v2",
    strict_version_check=False,
)

# Create version registry with lifecycle
registry = VersionRegistry(config)

# Register versions
registry.register_version(APIVersion(
    version="v1",
    status=VersionStatus.DEPRECATED,
    release_date=datetime(2024, 1, 1),
    deprecation_date=datetime(2024, 6, 1),
    sunset_date=datetime(2025, 12, 31),
    successor="v2",
    migration_guide_url="https://api.example.com/docs/migration/v1-to-v2",
    changelog_url="https://api.example.com/docs/changelog/v1",
))

registry.register_version(APIVersion(
    version="v2",
    status=VersionStatus.CURRENT,
    release_date=datetime(2024, 6, 1),
    changelog_url="https://api.example.com/docs/changelog/v2",
))

registry.register_version(APIVersion(
    version="v3",
    status=VersionStatus.BETA,
    release_date=datetime(2025, 1, 1),
))

# Register deprecated endpoints
registry.register_deprecated_endpoint(DeprecatedEndpoint(
    path="/api/v1/users",
    method="GET",
    deprecated_in="v1",
    removed_in="v3",
    sunset_date=datetime(2025, 12, 31),
    replacement_path="/api/v2/accounts",
    replacement_method="GET",
    migration_notes="https://api.example.com/docs/migration/users-to-accounts",
))

# Add versioning middleware
app.add_middleware(
    APIVersionMiddleware,
    registry=registry,
    config=config,
)


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/api/versions")
async def get_versions():
    """Get all API versions and their status."""
    current = registry.get_current_version()
    return {
        "current_version": current.version if current else None,
        "versions": [v.to_dict() for v in registry.get_all_versions()],
        "supported_versions": [v.version for v in registry.get_supported_versions()],
    }


@app.get("/api/v1/users")
async def get_users_v1(request: Request):
    """V1 users endpoint (deprecated)."""
    version = get_api_version(request)
    return {
        "api_version": version,
        "message": "This is the deprecated v1 users endpoint",
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ],
        "deprecation_notice": "Please migrate to /api/v2/accounts",
    }


@app.get("/api/v2/accounts")
async def get_accounts_v2(request: Request):
    """V2 accounts endpoint (current)."""
    version = get_api_version(request)
    return {
        "api_version": version,
        "message": "This is the current v2 accounts endpoint",
        "accounts": [
            {"id": "acc-001", "name": "Alice", "email": "alice@example.com"},
            {"id": "acc-002", "name": "Bob", "email": "bob@example.com"},
        ],
    }


@app.get("/api/v3/accounts")
async def get_accounts_v3(request: Request):
    """V3 accounts endpoint (beta)."""
    version = get_api_version(request)
    return {
        "api_version": version,
        "message": "This is the beta v3 accounts endpoint",
        "accounts": [
            {
                "id": "acc-001",
                "name": "Alice",
                "email": "alice@example.com",
                "metadata": {"created": "2024-01-01"},
            },
        ],
        "beta_notice": "This API version is in beta and may change",
    }


@app.get("/api/v1/health")
@app.get("/api/v2/health")
@app.get("/api/v3/health")
async def health(request: Request):
    """Health endpoint available in all versions."""
    version = get_api_version(request)
    version_info = get_api_version_info(request)

    return {
        "status": "healthy",
        "api_version": version,
        "version_status": version_info.status.value if version_info else None,
        "is_deprecated": version_info.is_deprecated if version_info else False,
    }


# Example with version decorator
@app.get("/api/v2/features")
@api_version(min_version="v2")
async def get_features_v2(request: Request):
    """Features endpoint only available in v2+."""
    return {
        "features": ["feature1", "feature2", "feature3"],
    }


# ============================================================================
# Test Script
# ============================================================================

if __name__ == "__main__":
    import requests

    BASE_URL = "http://localhost:8080"

    print("=" * 60)
    print("API Versioning Test Script")
    print("=" * 60)

    # Test 1: Get versions
    print("\n1. GET /api/versions")
    resp = requests.get(f"{BASE_URL}/api/versions")
    print(f"   Status: {resp.status_code}")
    print(f"   Response: {resp.json()}")

    # Test 2: V1 deprecated endpoint (URL path versioning)
    print("\n2. GET /api/v1/users (deprecated)")
    resp = requests.get(f"{BASE_URL}/api/v1/users")
    print(f"   Status: {resp.status_code}")
    print(f"   X-API-Version: {resp.headers.get('X-API-Version')}")
    print(f"   Deprecation: {resp.headers.get('Deprecation')}")
    print(f"   Sunset: {resp.headers.get('Sunset')}")
    print(f"   Warning: {resp.headers.get('Warning')}")
    print(f"   Link: {resp.headers.get('Link')}")

    # Test 3: V2 current endpoint
    print("\n3. GET /api/v2/accounts (current)")
    resp = requests.get(f"{BASE_URL}/api/v2/accounts")
    print(f"   Status: {resp.status_code}")
    print(f"   X-API-Version: {resp.headers.get('X-API-Version')}")

    # Test 4: Header-based versioning
    print("\n4. GET /api/v1/health with Accept-Version: v2")
    resp = requests.get(
        f"{BASE_URL}/api/v1/health",
        headers={"Accept-Version": "v2"}
    )
    print(f"   Status: {resp.status_code}")
    print(f"   X-API-Version: {resp.headers.get('X-API-Version')}")
    print(f"   Response: {resp.json()}")

    # Test 5: Query parameter versioning
    print("\n5. GET /api/v1/health?api-version=v3")
    resp = requests.get(f"{BASE_URL}/api/v1/health?api-version=v3")
    print(f"   Status: {resp.status_code}")
    print(f"   X-API-Version: {resp.headers.get('X-API-Version')}")
    print(f"   Response: {resp.json()}")

    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)

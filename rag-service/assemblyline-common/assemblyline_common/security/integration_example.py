"""
Integration Example: How to use Phase 7 security modules with your microservices.

This file shows how to integrate the security modules with your existing FastAPI services.
Copy the relevant sections to your service's main.py file.
"""

# ============================================================================
# Example: auth-service/main.py integration
# ============================================================================

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware

# Import Phase 7 security modules
from assemblyline_common.security import (
    # Rate Limiting
    RateLimiter,
    RateLimitConfig,
    RateLimitMiddleware,
    get_rate_limiter,
    RateLimitScope,

    # Adaptive Throttling
    AdaptiveThrottler,
    get_adaptive_throttler,

    # Threat Protection
    JSONThreatProtection,
    JSONThreatConfig,
    validate_json,
    HL7ThreatProtection,
    validate_hl7,

    # Injection Prevention
    SQLInjectionProtection,

    # OAuth Scopes
    require_scopes,
    require_resource_access,

    # mTLS (optional)
    MTLSValidator,
    mtls_required,
)


# ============================================================================
# 1. RATE LIMITING - Add to app startup
# ============================================================================

app = FastAPI(title="Example Service")


# Option A: Use middleware (applies to ALL requests)
@app.on_event("startup")
async def setup_rate_limiting():
    """Configure rate limiting on startup."""
    rate_limiter = await get_rate_limiter(
        RateLimitConfig(
            redis_url="redis://localhost:6379",
            default_requests_per_second=100,
            default_burst_size=200,
        )
    )

    # Add middleware
    app.add_middleware(
        RateLimitMiddleware,
        rate_limiter=rate_limiter,
        scope=RateLimitScope.TENANT,  # Rate limit per tenant
    )


# Option B: Use as dependency (applies to specific endpoints)
async def rate_limit_dependency(request: Request):
    """Rate limit dependency for specific endpoints."""
    rate_limiter = await get_rate_limiter()

    # Get tenant ID from request state (set by auth middleware)
    tenant_id = getattr(request.state, "tenant_id", None)

    allowed, result = await rate_limiter.check(
        scope=RateLimitScope.TENANT,
        key=str(tenant_id) if tenant_id else "anonymous"
    )

    if not allowed:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Retry after {result.retry_after_seconds}s",
            headers={"Retry-After": str(result.retry_after_seconds)}
        )


# ============================================================================
# 2. OAUTH SCOPES - Protect endpoints with scope requirements
# ============================================================================

# Require specific scopes
@app.get("/api/v1/flows")
async def list_flows(
    _: None = Depends(require_scopes("flows:read"))
):
    """List flows - requires 'flows:read' scope."""
    return {"flows": []}


# Require resource access (checks hierarchical scopes)
@app.delete("/api/v1/flows/{flow_id}")
async def delete_flow(
    flow_id: str,
    _: None = Depends(require_resource_access("flows", "delete"))
):
    """Delete flow - requires flows:delete or admin scope."""
    return {"deleted": flow_id}


# Require multiple scopes (any or all)
@app.post("/api/v1/admin/users")
async def create_user(
    _: None = Depends(require_scopes("users:write", "admin", require_all=False))
):
    """Create user - requires users:write OR admin scope."""
    return {"user": "created"}


# ============================================================================
# 3. JSON THREAT PROTECTION - Validate incoming JSON payloads
# ============================================================================

# Use as dependency
@app.post("/api/v1/flows")
async def create_flow(
    request: Request,
    _: None = Depends(validate_json(
        max_depth=10,
        max_string_length=50000,
        max_array_size=100,
        max_entries=200
    ))
):
    """Create flow with JSON validation."""
    body = await request.json()
    return {"flow_id": "new-flow"}


# Or configure globally
json_protection = JSONThreatProtection(JSONThreatConfig(
    max_depth=10,
    max_string_length=100000,
    max_array_size=1000,
    max_object_size=500,
    max_entries=5000,
))


@app.post("/api/v1/messages")
async def receive_message(request: Request):
    """Receive message with manual JSON validation."""
    body = await request.json()
    result = json_protection.validate(body)

    if not result.safe:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid payload: {result.message}")

    return {"received": True}


# ============================================================================
# 4. HL7 THREAT PROTECTION - For inbound-service
# ============================================================================

@app.post("/api/v1/hl7")
async def receive_hl7(
    request: Request,
    _: None = Depends(validate_hl7(
        max_segments=500,
        max_field_length=10000,
        required_segments=["MSH", "PID"]
    ))
):
    """Receive HL7 message with validation."""
    body = await request.body()
    return {"ack": "AA"}


# ============================================================================
# 5. SQL INJECTION PROTECTION - Validate search parameters
# ============================================================================

sql_protection = SQLInjectionProtection()


@app.get("/api/v1/search")
async def search(q: str):
    """Search with SQL injection protection."""
    result = sql_protection.check(q)

    if not result.safe:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"Potentially malicious input detected: {result.details}"
        )

    # Safe to use in query
    return {"query": q, "results": []}


# ============================================================================
# 6. ADAPTIVE THROTTLING - For high-load scenarios
# ============================================================================

@app.on_event("startup")
async def setup_throttling():
    """Configure adaptive throttling."""
    throttler = await get_adaptive_throttler()
    app.state.throttler = throttler


@app.post("/api/v1/process")
async def process_message(request: Request):
    """Process message with adaptive throttling."""
    throttler = request.app.state.throttler

    # Get tenant ID for prioritization
    tenant_id = getattr(request.state, "tenant_id", "unknown")

    allowed, result = await throttler.should_allow(tenant_id)

    if not allowed:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=503,
            detail="System under high load. Please retry.",
            headers={"Retry-After": "30"}
        )

    # Process the message
    return {"processed": True}


# ============================================================================
# 7. mTLS - For services requiring client certificates
# ============================================================================

# Use as dependency for endpoints requiring client certs
@app.get("/api/v1/secure-data")
async def get_secure_data(
    cert_info = Depends(mtls_required)
):
    """Endpoint requiring client certificate."""
    return {
        "subject": cert_info.subject_dn,
        "issuer": cert_info.issuer_dn,
        "data": "sensitive"
    }


# ============================================================================
# FULL INTEGRATION EXAMPLE
# ============================================================================

def create_secure_app() -> FastAPI:
    """Create a fully secured FastAPI application."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup: Initialize security components
        app.state.rate_limiter = await get_rate_limiter()
        app.state.throttler = await get_adaptive_throttler()
        app.state.json_protection = JSONThreatProtection()
        app.state.sql_protection = SQLInjectionProtection()

        yield

        # Shutdown: Cleanup if needed
        pass

    app = FastAPI(
        title="Secure Service",
        lifespan=lifespan
    )

    # Add CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Rate limiting is per-tenant by default
    # Individual endpoints can override with dependencies

    return app


# ============================================================================
# NOTE: To use these in your services, copy the relevant sections to your
# service's main.py file. The imports are from the shared library:
#
#   from assemblyline_common.security import (
#       RateLimiter, get_rate_limiter, RateLimitMiddleware,
#       validate_json, validate_hl7,
#       require_scopes, require_resource_access,
#       SQLInjectionProtection, CommandInjectionProtection,
#       mtls_required,
#   )
#
# Redis URL comes from your settings:
#   from assemblyline_common.config import settings
#   redis_url = settings.REDIS_URL
# ============================================================================

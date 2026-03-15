"""
Authentication dependencies for FastAPI.

Enterprise Features:
- JWT with JTI claim for revocation support
- Token blacklist integration
- Brute force protection integration
- Multi-tenant support with concurrent users
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Annotated
from uuid import UUID, uuid4
import hashlib
import hmac
import secrets
import logging

from fastapi import Depends, HTTPException, status, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
import jwt
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from assemblyline_common.config import settings
from assemblyline_common.database import get_db
from assemblyline_common.models import Tenant, User, APIKey
from assemblyline_common.models.gateway import GatewayConsumer

logger = logging.getLogger(__name__)

# Gateway authentication headers
GATEWAY_CONSUMER_HEADER = "X-Gateway-Consumer"
GATEWAY_SECRET_HEADER = "X-Gateway-Secret"

# Security schemes
bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class TokenPayload(BaseModel):
    """JWT token payload with enterprise security claims."""
    jti: str  # JWT ID for revocation tracking
    sub: str  # user_id
    tenant_id: str
    exp: datetime
    iat: datetime
    type: str = "access"  # access, refresh
    mfa_pending: bool = False  # True if MFA verification is still required
    scopes: list[str] = []  # OAuth 2.0 scopes for fine-grained authorization


class CurrentUser(BaseModel):
    """Current authenticated user context."""
    user_id: UUID
    tenant_id: UUID
    tenant_key: str
    email: str
    role: str
    auth_type: str  # jwt, api_key
    scopes: list[str] = []  # OAuth 2.0 scopes from token or API key


class CurrentTenant(BaseModel):
    """Current tenant context."""
    id: UUID
    key: str
    name: str
    schema_name: str
    is_active: bool


def get_default_scopes_for_role(role: str) -> list[str]:
    """Get default OAuth scopes based on user role."""
    # Full access scopes for admin roles
    admin_scopes = [
        "flows:read", "flows:write", "flows:execute", "flows:delete",
        "connectors:read", "connectors:write",
        "users:read", "users:write",
        "tenants:read", "tenants:write",
        "api-keys:read", "api-keys:write",
        "audit:read", "settings:write"
    ]
    role_scopes = {
        "super_admin": admin_scopes,  # Full access
        "admin": admin_scopes,
        "developer": [
            "flows:read", "flows:write", "flows:execute",
            "connectors:read", "connectors:write",
            "api-keys:read", "api-keys:write"
        ],
        "user": [
            "flows:read", "flows:execute",
            "connectors:read"
        ],
        "viewer": [
            "flows:read",
            "connectors:read"
        ],
        "api": [  # API key default scopes
            "flows:read", "flows:execute"
        ]
    }
    return role_scopes.get(role, ["flows:read"])


def create_access_token(
    user_id: UUID,
    tenant_id: UUID,
    expires_delta: Optional[timedelta] = None,
    additional_claims: Optional[dict] = None,
    expires_minutes: Optional[int] = None,
    scopes: Optional[list[str]] = None,
    role: Optional[str] = None
) -> tuple[str, str, int]:
    """
    Create a new JWT access token with JTI for revocation support.

    Args:
        user_id: User's UUID
        tenant_id: Tenant's UUID
        expires_delta: Optional custom expiration timedelta
        additional_claims: Optional additional claims to add (e.g., mfa_pending)
        expires_minutes: Optional expiration in minutes (overrides expires_delta)
        scopes: Optional list of OAuth scopes (defaults based on role)
        role: User role for default scope assignment

    Returns:
        Tuple of (token, jti, exp_timestamp) for token registration
    """
    now = datetime.now(timezone.utc)

    # Determine expiration
    if expires_minutes is not None:
        expire = now + timedelta(minutes=expires_minutes)
    elif expires_delta is not None:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)

    jti = str(uuid4())  # Unique token identifier for revocation

    # Determine scopes: explicit > role-based > default
    token_scopes = scopes if scopes is not None else get_default_scopes_for_role(role or "user")

    payload = {
        "jti": jti,
        "sub": str(user_id),
        "key": str(user_id),  # APIsix jwt-auth looks for 'key' claim to identify consumer
        "tenant_id": str(tenant_id),
        "exp": expire,
        "iat": now,
        "type": "access",
        "scopes": token_scopes  # Include OAuth scopes in token
    }

    # Add any additional claims (e.g., mfa_pending)
    if additional_claims:
        payload.update(additional_claims)

    token = jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return token, jti, int(expire.timestamp())


def create_refresh_token(user_id: UUID, tenant_id: UUID) -> tuple[str, str, int]:
    """
    Create a new JWT refresh token with JTI for revocation support.

    Returns:
        Tuple of (token, jti, exp_timestamp) for token registration
    """
    now = datetime.now(timezone.utc)
    expire = now + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
    jti = str(uuid4())  # Unique token identifier for revocation

    payload = {
        "jti": jti,
        "sub": str(user_id),
        "tenant_id": str(tenant_id),
        "exp": expire,
        "iat": now,
        "type": "refresh"
    }

    token = jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return token, jti, int(expire.timestamp())


def verify_token(token: str, expected_type: str = "access") -> TokenPayload:
    """Verify and decode a JWT token."""
    try:
        payload = jwt.decode(
            token, 
            settings.JWT_SECRET_KEY, 
            algorithms=[settings.JWT_ALGORITHM]
        )
        
        if payload.get("type") != expected_type:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid token type. Expected {expected_type}."
            )
        
        return TokenPayload(**payload)
    
    except InvalidTokenError as e:
        logger.warning(f"JWT verification failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )


def generate_api_key(environment: str = "live") -> tuple[str, str, str, str]:
    """
    Generate a new API key with HMAC-SHA256 and salt.

    Returns:
        Tuple of (full_key, prefix, key_hash, salt)

    Example:
        mw_live_a8f3j2k9x7m4n5p2_4a9c

    Security: Uses HMAC-SHA256 with unique salt for each key.
    """
    random_part = secrets.token_hex(16)  # 32 characters for better entropy
    checksum = hashlib.sha256(random_part.encode()).hexdigest()[:4]

    prefix = f"{settings.API_KEY_PREFIX}_{environment}_{random_part[:4]}"
    full_key = f"{settings.API_KEY_PREFIX}_{environment}_{random_part}_{checksum}"

    # Generate unique salt for this key
    salt = secrets.token_hex(16)  # 32 character salt

    # HMAC-SHA256 with salt
    key_hash = hmac.new(
        key=salt.encode(),
        msg=full_key.encode(),
        digestmod=hashlib.sha256
    ).hexdigest()

    return full_key, prefix, key_hash, salt


def hash_api_key(api_key: str, salt: str) -> str:
    """
    Hash an API key using HMAC-SHA256 with salt.

    Args:
        api_key: The API key to hash
        salt: The salt used when generating the key

    Returns:
        HMAC-SHA256 hash of the key
    """
    return hmac.new(
        key=salt.encode(),
        msg=api_key.encode(),
        digestmod=hashlib.sha256
    ).hexdigest()


def verify_api_key(api_key: str, stored_hash: str, stored_salt: str) -> bool:
    """
    Verify an API key against stored hash and salt.

    Uses constant-time comparison to prevent timing attacks.

    Args:
        api_key: The API key to verify
        stored_hash: The stored HMAC hash
        stored_salt: The stored salt

    Returns:
        True if the key is valid
    """
    computed_hash = hash_api_key(api_key, stored_salt)
    return hmac.compare_digest(computed_hash, stored_hash)


async def get_user_from_token(
    credentials: HTTPAuthorizationCredentials,
    db: AsyncSession
) -> tuple[User, Tenant]:
    """Get user and tenant from JWT token."""
    payload = verify_token(credentials.credentials)
    
    # Get user
    result = await db.execute(
        select(User).where(User.id == UUID(payload.sub))
    )
    user = result.scalar_one_or_none()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    # Get tenant
    result = await db.execute(
        select(Tenant).where(Tenant.id == UUID(payload.tenant_id))
    )
    tenant = result.scalar_one_or_none()
    
    if not tenant or not tenant.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tenant not found or inactive"
        )
    
    return user, tenant


async def get_tenant_from_api_key(
    api_key: str,
    db: AsyncSession
) -> tuple[APIKey, Tenant]:
    """
    Get API key and tenant from API key string.

    Supports both legacy (SHA-256) and new (HMAC-SHA256 with salt) keys.
    New keys use prefix lookup + HMAC verification for security.
    """
    # Extract prefix from key (format: mw_env_XXXX...)
    key_parts = api_key.split("_")
    if len(key_parts) >= 3:
        # Build prefix for lookup (e.g., "mw_live_a8f3")
        prefix = f"{key_parts[0]}_{key_parts[1]}_{key_parts[2][:4]}"
    else:
        prefix = None

    api_key_obj = None

    # Try prefix-based lookup first (for salted keys)
    if prefix:
        result = await db.execute(
            select(APIKey).where(
                APIKey.key_prefix == prefix,
                APIKey.is_active == True
            )
        )
        candidates = result.scalars().all()

        # Verify with HMAC if salt exists
        for candidate in candidates:
            if hasattr(candidate, 'key_salt') and candidate.key_salt:
                if verify_api_key(api_key, candidate.key_hash, candidate.key_salt):
                    api_key_obj = candidate
                    break
            else:
                # Legacy key without salt - use direct hash comparison
                legacy_hash = hashlib.sha256(api_key.encode()).hexdigest()
                if hmac.compare_digest(legacy_hash, candidate.key_hash):
                    api_key_obj = candidate
                    break

    # Fallback: legacy hash lookup (for migration period)
    if not api_key_obj:
        legacy_hash = hashlib.sha256(api_key.encode()).hexdigest()
        result = await db.execute(
            select(APIKey).where(APIKey.key_hash == legacy_hash)
        )
        api_key_obj = result.scalar_one_or_none()

    if not api_key_obj or not api_key_obj.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key"
        )

    # Check expiration
    if api_key_obj.expires_at and api_key_obj.expires_at < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key has expired"
        )

    # Get tenant
    result = await db.execute(
        select(Tenant).where(Tenant.id == api_key_obj.tenant_id)
    )
    tenant = result.scalar_one_or_none()

    if not tenant or not tenant.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tenant not found or inactive"
        )

    # Update usage tracking
    api_key_obj.last_used_at = datetime.now(timezone.utc)
    api_key_obj.usage_count += 1
    await db.commit()

    return api_key_obj, tenant


async def get_gateway_consumer(
    consumer_username: str,
    db: AsyncSession
) -> tuple[GatewayConsumer, Tenant]:
    """
    Get gateway consumer and tenant from consumer username.

    Used when APIsix has already validated the consumer's credentials
    and is forwarding the request with X-Gateway-Consumer header.

    APIsix may pass either the original username or the sanitized apisix_username,
    so we check both fields.
    """
    from sqlalchemy import or_

    # Look up consumer by username OR apisix_username
    # APIsix uses the sanitized username, but some routes may use the original
    result = await db.execute(
        select(GatewayConsumer).where(
            or_(
                GatewayConsumer.username == consumer_username,
                GatewayConsumer.apisix_username == consumer_username
            ),
            GatewayConsumer.is_active == True,
            GatewayConsumer.deleted_at.is_(None)
        )
    )
    consumer = result.scalar_one_or_none()

    if not consumer:
        logger.warning(f"Gateway consumer not found: {consumer_username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Gateway consumer not found"
        )

    # Get tenant
    result = await db.execute(
        select(Tenant).where(Tenant.id == consumer.tenant_id)
    )
    tenant = result.scalar_one_or_none()

    if not tenant or not tenant.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tenant not found or inactive"
        )

    # Update usage tracking
    consumer.last_used_at = datetime.now(timezone.utc)
    consumer.usage_count += 1
    await db.commit()

    return consumer, tenant


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    api_key: Optional[str] = Depends(api_key_header),
    db: AsyncSession = Depends(get_db)
) -> CurrentUser:
    """
    FastAPI dependency to get the current authenticated user.

    Supports Gateway-authenticated requests, JWT Bearer tokens, and API keys.
    Sets scopes on request.state for OAuth scope enforcement.

    Authentication priority:
    1. Gateway headers (X-Gateway-Consumer + X-Gateway-Secret) - for APIsix-proxied requests
    2. API Key (X-API-Key header)
    3. JWT Bearer token (Authorization header)
    """
    # Check for Gateway authentication first (APIsix-proxied requests)
    gateway_consumer = request.headers.get(GATEWAY_CONSUMER_HEADER)
    gateway_secret = request.headers.get(GATEWAY_SECRET_HEADER)

    if gateway_consumer and gateway_secret:
        # Validate the gateway secret
        if gateway_secret != settings.GATEWAY_INTERNAL_SECRET:
            logger.warning(f"Invalid gateway secret for consumer: {gateway_consumer}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid gateway credentials"
            )

        # Look up the gateway consumer
        consumer, tenant = await get_gateway_consumer(gateway_consumer, db)

        # Gateway consumers get flow execution scopes by default
        gateway_scopes = ["flows:read", "flows:execute"]

        # Set scopes on request.state for require_scopes() dependency
        request.state.scopes = set(gateway_scopes)
        request.state.user_id = str(consumer.id)
        request.state.tenant_id = str(tenant.id)

        logger.info(f"Gateway authentication successful for consumer: {gateway_consumer}")

        return CurrentUser(
            user_id=consumer.id,
            tenant_id=tenant.id,
            tenant_key=tenant.key,
            email=f"{consumer.username}@gateway",
            role="api",
            auth_type="gateway",
            scopes=gateway_scopes
        )

    # Try API key
    if api_key:
        api_key_obj, tenant = await get_tenant_from_api_key(api_key, db)
        # Get scopes from API key
        api_scopes = getattr(api_key_obj, 'scopes', None) or ["flows:read", "flows:execute"]

        # Set scopes on request.state for require_scopes() dependency
        request.state.scopes = set(api_scopes)
        request.state.user_id = str(api_key_obj.created_by or api_key_obj.id)
        request.state.tenant_id = str(tenant.id)

        return CurrentUser(
            user_id=api_key_obj.created_by or api_key_obj.id,  # Use creator or key ID
            tenant_id=tenant.id,
            tenant_key=tenant.key,
            email="api@" + tenant.key,
            role="api",
            auth_type="api_key",
            scopes=api_scopes
        )

    # Try JWT token
    if credentials:
        user, tenant = await get_user_from_token(credentials, db)
        # Extract scopes from token payload
        payload = verify_token(credentials.credentials)
        token_scopes = payload.scopes or get_default_scopes_for_role(user.role)

        # Set scopes on request.state for require_scopes() dependency
        request.state.scopes = set(token_scopes)
        request.state.user_id = str(user.id)
        request.state.tenant_id = str(tenant.id)

        return CurrentUser(
            user_id=user.id,
            tenant_id=tenant.id,
            tenant_key=tenant.key,
            email=user.email,
            role=user.role,
            auth_type="jwt",
            scopes=token_scopes
        )

    # No authentication provided
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required"
    )


async def get_current_tenant(
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> CurrentTenant:
    """FastAPI dependency to get the current tenant."""
    result = await db.execute(
        select(Tenant).where(Tenant.id == current_user.tenant_id)
    )
    tenant = result.scalar_one_or_none()
    
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found"
        )
    
    return CurrentTenant(
        id=tenant.id,
        key=tenant.key,
        name=tenant.name,
        schema_name=tenant.schema_name,
        is_active=tenant.is_active
    )


async def get_optional_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    api_key: Optional[str] = Depends(api_key_header),
    db: AsyncSession = Depends(get_db)
) -> Optional[CurrentUser]:
    """FastAPI dependency that returns None if not authenticated (instead of raising)."""
    try:
        return await get_current_user(request, credentials, api_key, db)
    except HTTPException:
        return None


def require_role(allowed_roles: list[str]):
    """Dependency factory to require specific roles."""
    async def role_checker(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role {current_user.role} not authorized. Required: {allowed_roles}"
            )
        return current_user
    return role_checker

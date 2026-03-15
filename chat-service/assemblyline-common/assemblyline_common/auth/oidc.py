"""
OpenID Connect (OIDC) Service for Logic Weaver.

Provides OIDC-based Single Sign-On with:
- Microsoft Entra ID (Azure AD), Okta, Auth0, Keycloak integration
- Generic OIDC Identity Provider support
- PKCE (Proof Key for Code Exchange) for enhanced security
- JWKS-based token validation
- Automatic user provisioning from ID token claims

HIPAA Requirement: Secure authentication for PHI access.

Usage:
    oidc = get_oidc_service()

    # Create authorization URL
    auth_url, state, nonce, code_verifier = oidc.create_authorization_url(provider, tenant_id, redirect_uri)

    # Exchange code for tokens
    tokens = await oidc.exchange_code(provider, code, redirect_uri, code_verifier)

    # Validate and extract user info
    user_info = await oidc.validate_and_extract(tokens["id_token"], provider)
"""

import base64
import hashlib
import json
import logging
import os
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from jwt.algorithms import RSAAlgorithm, ECAlgorithm

logger = logging.getLogger(__name__)


class OIDCConfig:
    """Configuration for OIDC service."""

    # Default paths
    DEFAULT_CALLBACK_PATH: str = "/api/v1/oidc/{tenant_id}/callback"

    # PKCE settings
    CODE_VERIFIER_LENGTH: int = 128
    CODE_CHALLENGE_METHOD: str = "S256"

    # Timeouts
    HTTP_TIMEOUT_SECONDS: int = 10
    STATE_TTL_SECONDS: int = 600  # 10 minutes for login flow
    JWKS_CACHE_TTL_SECONDS: int = 3600  # 1 hour JWKS cache

    # Token validation
    CLOCK_SKEW_SECONDS: int = 120  # 2 minutes clock skew tolerance

    # Provider type mappings
    ENTRA_ID_ISSUER_PATTERN: str = "https://login.microsoftonline.com/"
    OKTA_ISSUER_PATTERN: str = ".okta.com"
    AUTH0_ISSUER_PATTERN: str = ".auth0.com"


@dataclass
class OIDCProviderConfig:
    """OIDC Identity Provider configuration."""
    issuer_url: str
    client_id: str
    client_secret: Optional[str] = None
    name: str = "OIDC Provider"
    provider_type: str = "generic"  # entra_id, okta, auth0, keycloak, generic

    # Endpoints (auto-populated from discovery)
    authorization_endpoint: Optional[str] = None
    token_endpoint: Optional[str] = None
    userinfo_endpoint: Optional[str] = None
    jwks_uri: Optional[str] = None
    end_session_endpoint: Optional[str] = None

    # Configuration
    scopes: str = "openid profile email"
    response_type: str = "code"
    use_pkce: bool = True

    # Claim mapping
    claim_mapping: Dict[str, str] = field(default_factory=lambda: {
        "email": "email",
        "first_name": "given_name",
        "last_name": "family_name",
        "groups": "groups"
    })

    # Auto-provisioning settings
    auto_provision_users: bool = True
    default_role: str = "user"
    allowed_domains: List[str] = field(default_factory=list)
    group_role_mapping: Dict[str, str] = field(default_factory=dict)


@dataclass
class OIDCUserInfo:
    """User information extracted from OIDC claims."""
    email: str
    sub: str  # Subject identifier from IdP
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    groups: List[str] = field(default_factory=list)
    claims: Dict[str, Any] = field(default_factory=dict)
    issuer: Optional[str] = None


class OIDCService:
    """
    OpenID Connect Service implementation.

    Handles OIDC SSO with:
    - Discovery document fetching
    - Authorization URL generation with PKCE
    - Code exchange for tokens
    - ID token validation via JWKS
    - User info extraction from claims

    Supports Entra ID, Okta, Auth0, Keycloak, and generic OIDC providers.

    HIPAA Technical Safeguard: 164.312(d) Person or Entity Authentication
    """

    def __init__(
        self,
        base_url: str,
        config: Optional[OIDCConfig] = None
    ):
        self.base_url = base_url.rstrip("/")
        self.config = config or OIDCConfig()
        self._discovery_cache: Dict[str, Tuple[Dict, float]] = {}  # issuer -> (doc, timestamp)
        self._jwks_cache: Dict[str, Tuple[Dict, float]] = {}  # jwks_uri -> (keys, timestamp)

    async def discover(self, issuer_url: str) -> Dict[str, Any]:
        """
        Fetch OIDC discovery document from .well-known/openid-configuration.

        Args:
            issuer_url: The issuer URL (e.g., https://login.microsoftonline.com/{tenant}/v2.0)

        Returns:
            Discovery document as dict
        """
        # Check cache
        cached = self._discovery_cache.get(issuer_url)
        if cached:
            doc, ts = cached
            if time.time() - ts < self.config.JWKS_CACHE_TTL_SECONDS:
                return doc

        discovery_url = f"{issuer_url.rstrip('/')}/.well-known/openid-configuration"

        async with httpx.AsyncClient(timeout=self.config.HTTP_TIMEOUT_SECONDS) as client:
            response = await client.get(discovery_url)
            response.raise_for_status()
            doc = response.json()

        self._discovery_cache[issuer_url] = (doc, time.time())

        logger.info(
            "OIDC discovery document fetched",
            extra={
                "issuer": issuer_url,
                "event_type": "oidc_discovery_fetched"
            }
        )

        return doc

    def create_authorization_url(
        self,
        provider: OIDCProviderConfig,
        tenant_id: str,
        redirect_uri: str,
        state: Optional[str] = None,
        nonce: Optional[str] = None,
    ) -> Tuple[str, str, str, Optional[str]]:
        """
        Create OIDC authorization URL with optional PKCE.

        Args:
            provider: OIDC provider configuration
            tenant_id: Tenant ID
            redirect_uri: Callback URI
            state: Optional state parameter (generated if None)
            nonce: Optional nonce parameter (generated if None)

        Returns:
            Tuple of (authorization_url, state, nonce, code_verifier)
        """
        if not provider.authorization_endpoint:
            raise ValueError("Authorization endpoint not configured. Run discovery first.")

        state = state or uuid4().hex
        nonce = nonce or uuid4().hex

        params = {
            "client_id": provider.client_id,
            "response_type": provider.response_type,
            "redirect_uri": redirect_uri,
            "scope": provider.scopes,
            "state": state,
            "nonce": nonce,
            "response_mode": "query",
        }

        code_verifier = None
        if provider.use_pkce:
            code_verifier = self._generate_code_verifier()
            code_challenge = self._generate_code_challenge(code_verifier)
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = self.config.CODE_CHALLENGE_METHOD

        # Request MFA from Entra ID via claims parameter
        if provider.provider_type == "entra_id":
            import json as _json
            params["claims"] = _json.dumps({
                "id_token": {
                    "acrs": {
                        "essential": True,
                        "values": ["c1"]
                    },
                    "amr": {
                        "essential": True,
                        "values": ["mfa"]
                    }
                }
            })

        authorization_url = f"{provider.authorization_endpoint}?{urllib.parse.urlencode(params)}"

        logger.info(
            "OIDC authorization URL created",
            extra={
                "provider": provider.name,
                "tenant_id": tenant_id,
                "use_pkce": provider.use_pkce,
                "event_type": "oidc_auth_url_created"
            }
        )

        return authorization_url, state, nonce, code_verifier

    async def exchange_code(
        self,
        provider: OIDCProviderConfig,
        code: str,
        redirect_uri: str,
        code_verifier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Exchange authorization code for tokens.

        Args:
            provider: OIDC provider configuration
            code: Authorization code
            redirect_uri: Same redirect URI used in authorization request
            code_verifier: PKCE code verifier (if PKCE was used)

        Returns:
            Token response dict with access_token, id_token, refresh_token, etc.
        """
        if not provider.token_endpoint:
            raise ValueError("Token endpoint not configured. Run discovery first.")

        data = {
            "grant_type": "authorization_code",
            "client_id": provider.client_id,
            "code": code,
            "redirect_uri": redirect_uri,
        }

        if provider.client_secret:
            data["client_secret"] = provider.client_secret

        if code_verifier:
            data["code_verifier"] = code_verifier

        async with httpx.AsyncClient(timeout=self.config.HTTP_TIMEOUT_SECONDS) as client:
            response = await client.post(
                provider.token_endpoint,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if response.status_code != 200:
                error_detail = response.text
                logger.error(
                    f"OIDC token exchange failed: {error_detail}",
                    extra={
                        "provider": provider.name,
                        "status_code": response.status_code,
                        "event_type": "oidc_token_exchange_error"
                    }
                )
                raise ValueError(f"Token exchange failed: {error_detail}")

            tokens = response.json()

        logger.info(
            "OIDC code exchanged for tokens",
            extra={
                "provider": provider.name,
                "has_id_token": "id_token" in tokens,
                "has_access_token": "access_token" in tokens,
                "event_type": "oidc_token_exchange_success"
            }
        )

        return tokens

    async def validate_id_token(
        self,
        id_token: str,
        provider: OIDCProviderConfig,
        nonce: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate ID token signature and claims.

        Args:
            id_token: JWT ID token
            provider: OIDC provider configuration
            nonce: Expected nonce value

        Returns:
            Decoded token claims

        Raises:
            ValueError: If validation fails
        """
        if not provider.jwks_uri:
            raise ValueError("JWKS URI not configured. Run discovery first.")

        # Get JWKS keys
        jwks = await self._get_jwks(provider.jwks_uri)

        # Decode header to find key ID
        try:
            unverified_header = jwt.get_unverified_header(id_token)
        except jwt.exceptions.DecodeError as e:
            raise ValueError(f"Invalid ID token format: {e}")

        kid = unverified_header.get("kid")
        alg = unverified_header.get("alg", "RS256")

        # Find matching key
        signing_key = None
        for key_data in jwks.get("keys", []):
            if key_data.get("kid") == kid:
                signing_key = key_data
                break

        if not signing_key:
            # Refresh JWKS cache and retry
            self._jwks_cache.pop(provider.jwks_uri, None)
            jwks = await self._get_jwks(provider.jwks_uri)
            for key_data in jwks.get("keys", []):
                if key_data.get("kid") == kid:
                    signing_key = key_data
                    break

        if not signing_key:
            raise ValueError(f"No matching key found for kid: {kid}")

        # Build public key from JWK
        try:
            if alg.startswith("RS"):
                public_key = RSAAlgorithm.from_jwk(json.dumps(signing_key))
            elif alg.startswith("ES"):
                public_key = ECAlgorithm.from_jwk(json.dumps(signing_key))
            else:
                raise ValueError(f"Unsupported algorithm: {alg}")
        except Exception as e:
            raise ValueError(f"Failed to construct public key: {e}")

        # Validate token
        try:
            claims = jwt.decode(
                id_token,
                public_key,
                algorithms=[alg],
                audience=provider.client_id,
                issuer=provider.issuer_url,
                leeway=self.config.CLOCK_SKEW_SECONDS,
            )
        except jwt.ExpiredSignatureError:
            raise ValueError("ID token has expired")
        except jwt.InvalidAudienceError:
            raise ValueError("ID token audience mismatch")
        except jwt.InvalidIssuerError:
            raise ValueError("ID token issuer mismatch")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"ID token validation failed: {e}")

        # Validate nonce
        if nonce and claims.get("nonce") != nonce:
            raise ValueError("ID token nonce mismatch")

        logger.info(
            "OIDC ID token validated",
            extra={
                "provider": provider.name,
                "sub": claims.get("sub"),
                "event_type": "oidc_token_validated"
            }
        )

        return claims

    async def get_user_info(
        self,
        access_token: str,
        provider: OIDCProviderConfig,
    ) -> Dict[str, Any]:
        """
        Fetch user info from the userinfo endpoint.

        Args:
            access_token: OAuth2 access token
            provider: OIDC provider configuration

        Returns:
            User info claims dict
        """
        if not provider.userinfo_endpoint:
            raise ValueError("Userinfo endpoint not configured")

        async with httpx.AsyncClient(timeout=self.config.HTTP_TIMEOUT_SECONDS) as client:
            response = await client.get(
                provider.userinfo_endpoint,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            response.raise_for_status()
            return response.json()

    def extract_user_info(
        self,
        claims: Dict[str, Any],
        provider: OIDCProviderConfig,
    ) -> OIDCUserInfo:
        """
        Extract user info from ID token claims using claim mapping.

        Args:
            claims: Decoded ID token claims
            provider: OIDC provider configuration

        Returns:
            OIDCUserInfo with mapped user data
        """
        mapping = provider.claim_mapping

        email = claims.get(mapping.get("email", "email"))
        if not email:
            # Fallback: try preferred_username or upn (common in Entra ID)
            email = claims.get("preferred_username") or claims.get("upn")

        if not email:
            raise ValueError("No email claim found in ID token")

        first_name = claims.get(mapping.get("first_name", "given_name"))
        last_name = claims.get(mapping.get("last_name", "family_name"))

        # Fallback: split "name" claim if given_name/family_name are missing
        if not first_name and not last_name:
            full_name = claims.get("name", "")
            if full_name:
                parts = full_name.split(" ", 1)
                first_name = parts[0]
                last_name = parts[1] if len(parts) > 1 else None

        # Extract groups (may be a list or single value)
        groups_claim = mapping.get("groups", "groups")
        groups = claims.get(groups_claim, [])
        if isinstance(groups, str):
            groups = [groups]

        return OIDCUserInfo(
            email=email,
            sub=claims.get("sub", ""),
            first_name=first_name,
            last_name=last_name,
            groups=groups,
            claims=claims,
            issuer=claims.get("iss"),
        )

    def validate_email_domain(
        self,
        email: str,
        allowed_domains: List[str]
    ) -> bool:
        """
        Validate email domain against allowed list.

        Args:
            email: User's email address
            allowed_domains: List of allowed domains (empty = all allowed)

        Returns:
            True if email domain is allowed
        """
        if not allowed_domains:
            return True

        domain = email.split("@")[-1].lower()
        allowed_lower = [d.lower() for d in allowed_domains]

        return domain in allowed_lower

    def map_groups_to_role(
        self,
        groups: List[str],
        group_role_mapping: Dict[str, str],
        default_role: str = "user"
    ) -> str:
        """
        Map IdP groups to application role.

        Args:
            groups: User's groups from OIDC claims
            group_role_mapping: Dict mapping group names to roles
            default_role: Role if no mapping matches

        Returns:
            Role name
        """
        for group in groups:
            if group in group_role_mapping:
                return group_role_mapping[group]

        return default_role

    async def populate_endpoints(self, provider: OIDCProviderConfig) -> OIDCProviderConfig:
        """
        Populate provider endpoints from discovery document.

        Args:
            provider: OIDC provider config (modified in place)

        Returns:
            Updated provider config
        """
        doc = await self.discover(provider.issuer_url)

        if not provider.authorization_endpoint:
            provider.authorization_endpoint = doc.get("authorization_endpoint")
        if not provider.token_endpoint:
            provider.token_endpoint = doc.get("token_endpoint")
        if not provider.userinfo_endpoint:
            provider.userinfo_endpoint = doc.get("userinfo_endpoint")
        if not provider.jwks_uri:
            provider.jwks_uri = doc.get("jwks_uri")
        if not provider.end_session_endpoint:
            provider.end_session_endpoint = doc.get("end_session_endpoint")

        return provider

    # ========================================================================
    # Private helpers
    # ========================================================================

    def _generate_code_verifier(self) -> str:
        """Generate a PKCE code verifier (128 bytes, base64url-encoded)."""
        return base64.urlsafe_b64encode(os.urandom(96)).decode("ascii").rstrip("=")

    def _generate_code_challenge(self, code_verifier: str) -> str:
        """Generate a PKCE code challenge from a verifier (S256)."""
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    async def _get_jwks(self, jwks_uri: str) -> Dict[str, Any]:
        """Fetch JWKS keys with caching."""
        cached = self._jwks_cache.get(jwks_uri)
        if cached:
            keys, ts = cached
            if time.time() - ts < self.config.JWKS_CACHE_TTL_SECONDS:
                return keys

        async with httpx.AsyncClient(timeout=self.config.HTTP_TIMEOUT_SECONDS) as client:
            response = await client.get(jwks_uri)
            response.raise_for_status()
            jwks = response.json()

        self._jwks_cache[jwks_uri] = (jwks, time.time())

        logger.debug(
            "JWKS keys fetched",
            extra={
                "jwks_uri": jwks_uri,
                "key_count": len(jwks.get("keys", [])),
                "event_type": "oidc_jwks_fetched"
            }
        )

        return jwks


# Singleton helper
_oidc_service: Optional[OIDCService] = None


def get_oidc_service(base_url: Optional[str] = None) -> OIDCService:
    """
    Get or create OIDC service singleton.

    Usage in FastAPI:
        @app.get("/oidc/{tenant_id}/login")
        async def oidc_login(
            oidc: OIDCService = Depends(get_oidc_service)
        ):
            url, state, nonce, verifier = oidc.create_authorization_url(provider, tenant_id, redirect_uri)
            return RedirectResponse(url)
    """
    global _oidc_service

    if _oidc_service is None:
        if base_url is None:
            try:
                from assemblyline_common.config import settings
                origins = settings.cors_origins_list
                base_url = origins[0] if origins else "http://localhost:8001"
            except Exception:
                base_url = "http://localhost:8001"

        _oidc_service = OIDCService(base_url=base_url)

    return _oidc_service

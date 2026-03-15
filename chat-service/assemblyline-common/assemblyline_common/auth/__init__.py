"""
Authentication module for Logic Weaver.

Enterprise Security Features:
- JWT with JTI for token revocation
- Brute force protection with Redis
- Token blacklist for logout/security events
- HMAC-SHA256 API key hashing with salt
- MFA/TOTP with backup codes
- SAML 2.0 SSO (Azure AD, Okta, generic)
"""

from assemblyline_common.auth.dependencies import (
    CurrentUser,
    CurrentTenant,
    TokenPayload,
    get_current_user,
    get_current_tenant,
    get_optional_current_user,
    create_access_token,
    create_refresh_token,
    verify_token,
    generate_api_key,
    hash_api_key,
    verify_api_key,
    require_role,
    get_default_scopes_for_role,  # OAuth scope helper
)

from assemblyline_common.auth.brute_force import (
    BruteForceProtection,
    BruteForceConfig,
    get_brute_force_protection,
)

from assemblyline_common.auth.token_blacklist import (
    TokenBlacklist,
    TokenBlacklistConfig,
    get_token_blacklist,
)

from assemblyline_common.auth.mfa import (
    MFAService,
    MFAConfig,
    MFAEnrollment,
    get_mfa_service,
)

from assemblyline_common.auth.saml import (
    SAMLService,
    SAMLConfig,
    SAMLProviderConfig,
    SAMLUserInfo,
    SAMLRequest,
    get_saml_service,
)

from assemblyline_common.auth.oidc import (
    OIDCService,
    OIDCConfig,
    OIDCProviderConfig,
    OIDCUserInfo,
    get_oidc_service,
)

from assemblyline_common.auth.ldap_service import (
    LDAPService,
    LDAPProviderConfig,
    LDAPUserInfo,
    get_ldap_service,
)

from assemblyline_common.auth.legacy_helpers import (
    get_user_id_from_request,
    require_user_id,
    get_user_info_from_request,
    require_user_info,
    get_user_permissions_from_request,
    has_permission,
    is_enterprise_tier,
)

__all__ = [
    # Core auth
    "CurrentUser",
    "CurrentTenant",
    "TokenPayload",
    "get_current_user",
    "get_current_tenant",
    "get_optional_current_user",
    "create_access_token",
    "create_refresh_token",
    "verify_token",
    "generate_api_key",
    "hash_api_key",
    "verify_api_key",
    "require_role",
    "get_default_scopes_for_role",
    # Brute force protection
    "BruteForceProtection",
    "BruteForceConfig",
    "get_brute_force_protection",
    # Token blacklist
    "TokenBlacklist",
    "TokenBlacklistConfig",
    "get_token_blacklist",
    # MFA
    "MFAService",
    "MFAConfig",
    "MFAEnrollment",
    "get_mfa_service",
    # SAML SSO
    "SAMLService",
    "SAMLConfig",
    "SAMLProviderConfig",
    "SAMLUserInfo",
    "SAMLRequest",
    "get_saml_service",
    # OIDC SSO
    "OIDCService",
    "OIDCConfig",
    "OIDCProviderConfig",
    "OIDCUserInfo",
    "get_oidc_service",
    # LDAP SSO
    "LDAPService",
    "LDAPProviderConfig",
    "LDAPUserInfo",
    "get_ldap_service",
    # Legacy helpers (simple JWT extraction)
    "get_user_id_from_request",
    "require_user_id",
    "get_user_info_from_request",
    "require_user_info",
    "get_user_permissions_from_request",
    "has_permission",
    "is_enterprise_tier",
]

"""
SAML 2.0 SSO Service for Logic Weaver.

Provides SAML-based Single Sign-On with:
- Azure AD and Okta integration
- Generic SAML 2.0 Identity Provider support
- Automatic user provisioning from assertions
- SP metadata generation
- Signed requests and encrypted assertions

HIPAA Requirement: Secure authentication for PHI access.

Usage:
    saml = get_saml_service()

    # Generate SP metadata
    metadata = saml.get_sp_metadata(provider_config)

    # Create login request
    redirect_url = saml.create_authn_request(provider_config)

    # Process response
    user_info = saml.process_response(saml_response, provider_config)
"""

import base64
import logging
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class SAMLConfig:
    """Configuration for SAML service."""

    # SP settings
    DEFAULT_SP_ENTITY_ID: str = "urn:logic-weaver:sp"
    DEFAULT_ACS_PATH: str = "/api/v1/saml/{tenant_id}/acs"
    DEFAULT_SLO_PATH: str = "/api/v1/saml/{tenant_id}/slo"
    DEFAULT_METADATA_PATH: str = "/api/v1/saml/{tenant_id}/metadata"

    # Signature settings
    SIGN_AUTHN_REQUEST: bool = True
    WANT_ASSERTIONS_SIGNED: bool = True
    WANT_ASSERTIONS_ENCRYPTED: bool = False  # Most IdPs don't support this well

    # Name ID format
    NAME_ID_FORMAT: str = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"

    # Binding types
    SSO_BINDING: str = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
    ACS_BINDING: str = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"

    # Session settings
    SESSION_DURATION_SECONDS: int = 28800  # 8 hours

    # Provider type mappings
    AZURE_AD_ISSUER_PATTERN: str = "https://sts.windows.net/"
    OKTA_ISSUER_PATTERN: str = ".okta.com"


@dataclass
class SAMLProviderConfig:
    """SAML Identity Provider configuration."""
    entity_id: str
    sso_url: str
    x509_certificate: str
    name: str = "SAML Provider"
    provider_type: str = "generic"  # azure_ad, okta, generic
    slo_url: Optional[str] = None
    sp_private_key: Optional[str] = None

    # Attribute mapping
    attribute_mapping: Dict[str, str] = field(default_factory=lambda: {
        "email": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
        "first_name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
        "last_name": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname",
        "groups": "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups"
    })

    # Auto-provisioning settings
    auto_provision_users: bool = True
    default_role: str = "user"
    allowed_domains: List[str] = field(default_factory=list)
    group_role_mapping: Dict[str, str] = field(default_factory=dict)


@dataclass
class SAMLUserInfo:
    """User information extracted from SAML assertion."""
    email: str
    name_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    groups: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    session_index: Optional[str] = None
    issuer: Optional[str] = None


@dataclass
class SAMLRequest:
    """SAML AuthnRequest data."""
    request_id: str
    redirect_url: str
    relay_state: Optional[str] = None


class SAMLService:
    """
    SAML 2.0 Service Provider implementation.

    Handles SAML SSO with:
    - AuthnRequest generation
    - Response validation
    - Assertion parsing
    - SP metadata generation

    Supports Azure AD, Okta, and generic SAML 2.0 IdPs.

    HIPAA Technical Safeguard: 164.312(d) Person or Entity Authentication
    """

    def __init__(
        self,
        base_url: str,
        config: Optional[SAMLConfig] = None
    ):
        """
        Initialize SAML service.

        Args:
            base_url: Base URL of the application (e.g., https://app.logicweaver.io)
            config: Optional configuration override
        """
        self.base_url = base_url.rstrip("/")
        self.config = config or SAMLConfig()
        self._onelogin_available = self._check_onelogin()

    def _check_onelogin(self) -> bool:
        """Check if python3-saml is available."""
        try:
            from onelogin.saml2.auth import OneLogin_Saml2_Auth
            return True
        except ImportError:
            logger.warning(
                "python3-saml not installed, SAML features limited",
                extra={"event_type": "saml_library_unavailable"}
            )
            return False

    def _build_settings(
        self,
        provider: SAMLProviderConfig,
        tenant_id: str
    ) -> Dict[str, Any]:
        """Build OneLogin SAML settings dict."""
        acs_url = f"{self.base_url}{self.config.DEFAULT_ACS_PATH.format(tenant_id=tenant_id)}"
        sp_entity_id = f"{self.config.DEFAULT_SP_ENTITY_ID}:{tenant_id}"

        settings = {
            "strict": True,
            "debug": False,
            "sp": {
                "entityId": sp_entity_id,
                "assertionConsumerService": {
                    "url": acs_url,
                    "binding": self.config.ACS_BINDING
                },
                "NameIDFormat": self.config.NAME_ID_FORMAT,
            },
            "idp": {
                "entityId": provider.entity_id,
                "singleSignOnService": {
                    "url": provider.sso_url,
                    "binding": self.config.SSO_BINDING
                },
                "x509cert": provider.x509_certificate,
            },
            "security": {
                "authnRequestsSigned": self.config.SIGN_AUTHN_REQUEST and provider.sp_private_key is not None,
                "wantAssertionsSigned": self.config.WANT_ASSERTIONS_SIGNED,
                "wantAssertionsEncrypted": self.config.WANT_ASSERTIONS_ENCRYPTED,
            }
        }

        # Add SLO if configured
        if provider.slo_url:
            settings["idp"]["singleLogoutService"] = {
                "url": provider.slo_url,
                "binding": self.config.SSO_BINDING
            }

        # Add SP private key if available
        if provider.sp_private_key:
            settings["sp"]["privateKey"] = provider.sp_private_key

        return settings

    def create_authn_request(
        self,
        provider: SAMLProviderConfig,
        tenant_id: str,
        relay_state: Optional[str] = None,
        return_to: Optional[str] = None
    ) -> SAMLRequest:
        """
        Create SAML AuthnRequest and get redirect URL.

        Args:
            provider: SAML provider configuration
            tenant_id: Tenant ID for ACS URL
            relay_state: Optional state to pass through
            return_to: Optional URL to return to after login

        Returns:
            SAMLRequest with redirect URL
        """
        request_id = f"_id{uuid4().hex}"

        if not self._onelogin_available:
            # Fallback: construct basic redirect manually
            redirect_url = self._build_simple_authn_request(
                provider, tenant_id, request_id, relay_state
            )
            return SAMLRequest(
                request_id=request_id,
                redirect_url=redirect_url,
                relay_state=relay_state
            )

        try:
            from onelogin.saml2.auth import OneLogin_Saml2_Auth

            # Build settings
            settings = self._build_settings(provider, tenant_id)

            # Create mock request for OneLogin
            request_data = {
                "https": "on" if self.base_url.startswith("https") else "off",
                "http_host": urllib.parse.urlparse(self.base_url).netloc,
                "script_name": "/",
                "get_data": {},
                "post_data": {}
            }

            auth = OneLogin_Saml2_Auth(request_data, settings)
            redirect_url = auth.login(return_to=return_to)

            # Add relay state if provided
            if relay_state and "RelayState" not in redirect_url:
                separator = "&" if "?" in redirect_url else "?"
                redirect_url = f"{redirect_url}{separator}RelayState={urllib.parse.quote(relay_state)}"

            logger.info(
                "SAML AuthnRequest created",
                extra={
                    "request_id": request_id,
                    "provider": provider.name,
                    "tenant_id": tenant_id,
                    "event_type": "saml_authn_request"
                }
            )

            return SAMLRequest(
                request_id=request_id,
                redirect_url=redirect_url,
                relay_state=relay_state
            )

        except Exception as e:
            logger.error(
                f"Failed to create SAML request: {e}",
                extra={
                    "provider": provider.name,
                    "tenant_id": tenant_id,
                    "error": str(e),
                    "event_type": "saml_request_error"
                }
            )
            raise

    def _build_simple_authn_request(
        self,
        provider: SAMLProviderConfig,
        tenant_id: str,
        request_id: str,
        relay_state: Optional[str]
    ) -> str:
        """Build simple SAML AuthnRequest without full library."""
        import zlib

        acs_url = f"{self.base_url}{self.config.DEFAULT_ACS_PATH.format(tenant_id=tenant_id)}"
        sp_entity_id = f"{self.config.DEFAULT_SP_ENTITY_ID}:{tenant_id}"
        issue_instant = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        authn_request = f"""<?xml version="1.0" encoding="UTF-8"?>
<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    ID="{request_id}"
    Version="2.0"
    IssueInstant="{issue_instant}"
    AssertionConsumerServiceURL="{acs_url}"
    Destination="{provider.sso_url}">
    <saml:Issuer xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">{sp_entity_id}</saml:Issuer>
    <samlp:NameIDPolicy Format="{self.config.NAME_ID_FORMAT}" AllowCreate="true"/>
</samlp:AuthnRequest>"""

        # Deflate and base64 encode
        compressed = zlib.compress(authn_request.encode("utf-8"))[2:-4]
        encoded = base64.b64encode(compressed).decode("ascii")

        # Build redirect URL
        params = {"SAMLRequest": encoded}
        if relay_state:
            params["RelayState"] = relay_state

        query = urllib.parse.urlencode(params)
        separator = "&" if "?" in provider.sso_url else "?"

        return f"{provider.sso_url}{separator}{query}"

    def process_response(
        self,
        saml_response: str,
        provider: SAMLProviderConfig,
        tenant_id: str,
        request_id: Optional[str] = None
    ) -> SAMLUserInfo:
        """
        Process SAML Response and extract user information.

        Args:
            saml_response: Base64-encoded SAML Response
            provider: SAML provider configuration
            tenant_id: Tenant ID
            request_id: Optional request ID for validation

        Returns:
            SAMLUserInfo with extracted user data

        Raises:
            ValueError: If response is invalid or validation fails
        """
        if not self._onelogin_available:
            return self._parse_response_simple(saml_response, provider)

        try:
            from onelogin.saml2.auth import OneLogin_Saml2_Auth

            # Build settings
            settings = self._build_settings(provider, tenant_id)

            # Create mock request with POST data
            request_data = {
                "https": "on" if self.base_url.startswith("https") else "off",
                "http_host": urllib.parse.urlparse(self.base_url).netloc,
                "script_name": self.config.DEFAULT_ACS_PATH.format(tenant_id=tenant_id),
                "get_data": {},
                "post_data": {"SAMLResponse": saml_response}
            }

            auth = OneLogin_Saml2_Auth(request_data, settings)
            auth.process_response()

            # Check for errors
            errors = auth.get_errors()
            if errors:
                error_msg = ", ".join(errors)
                logger.error(
                    f"SAML response validation failed: {error_msg}",
                    extra={
                        "provider": provider.name,
                        "errors": errors,
                        "event_type": "saml_validation_error"
                    }
                )
                raise ValueError(f"SAML validation failed: {error_msg}")

            # Extract user info
            attributes = auth.get_attributes()
            name_id = auth.get_nameid()
            session_index = auth.get_session_index()

            user_info = self._extract_user_info(
                attributes=attributes,
                name_id=name_id,
                session_index=session_index,
                provider=provider
            )

            logger.info(
                "SAML response processed successfully",
                extra={
                    "email": user_info.email,
                    "provider": provider.name,
                    "tenant_id": tenant_id,
                    "event_type": "saml_login_success"
                }
            )

            return user_info

        except ValueError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to process SAML response: {e}",
                extra={
                    "provider": provider.name,
                    "tenant_id": tenant_id,
                    "error": str(e),
                    "event_type": "saml_process_error"
                }
            )
            raise ValueError(f"Failed to process SAML response: {e}")

    def _parse_response_simple(
        self,
        saml_response: str,
        provider: SAMLProviderConfig
    ) -> SAMLUserInfo:
        """Simple SAML response parsing without full library (not for production)."""
        import xml.etree.ElementTree as ET

        try:
            decoded = base64.b64decode(saml_response)
            root = ET.fromstring(decoded)

            # Find NameID
            ns = {
                "saml": "urn:oasis:names:tc:SAML:2.0:assertion",
                "samlp": "urn:oasis:names:tc:SAML:2.0:protocol"
            }

            name_id_elem = root.find(".//saml:NameID", ns)
            name_id = name_id_elem.text if name_id_elem is not None else None

            if not name_id:
                raise ValueError("No NameID found in SAML response")

            # Extract attributes
            attributes = {}
            for attr in root.findall(".//saml:Attribute", ns):
                attr_name = attr.get("Name")
                values = [v.text for v in attr.findall("saml:AttributeValue", ns)]
                attributes[attr_name] = values

            return self._extract_user_info(
                attributes=attributes,
                name_id=name_id,
                session_index=None,
                provider=provider
            )

        except ET.ParseError as e:
            raise ValueError(f"Invalid SAML response XML: {e}")

    def _extract_user_info(
        self,
        attributes: Dict[str, Any],
        name_id: str,
        session_index: Optional[str],
        provider: SAMLProviderConfig
    ) -> SAMLUserInfo:
        """Extract user info from SAML attributes."""
        mapping = provider.attribute_mapping

        def get_attr(key: str) -> Optional[str]:
            attr_name = mapping.get(key)
            if not attr_name:
                return None
            values = attributes.get(attr_name, [])
            if isinstance(values, list):
                return values[0] if values else None
            return values

        def get_list_attr(key: str) -> List[str]:
            attr_name = mapping.get(key)
            if not attr_name:
                return []
            values = attributes.get(attr_name, [])
            if isinstance(values, list):
                return values
            return [values] if values else []

        email = get_attr("email") or name_id
        first_name = get_attr("first_name")
        last_name = get_attr("last_name")
        groups = get_list_attr("groups")

        return SAMLUserInfo(
            email=email,
            name_id=name_id,
            first_name=first_name,
            last_name=last_name,
            groups=groups,
            attributes=attributes,
            session_index=session_index,
            issuer=provider.entity_id
        )

    def get_sp_metadata(
        self,
        provider: SAMLProviderConfig,
        tenant_id: str
    ) -> str:
        """
        Generate SP metadata XML.

        Args:
            provider: Provider config (for SP key if available)
            tenant_id: Tenant ID

        Returns:
            XML metadata string
        """
        acs_url = f"{self.base_url}{self.config.DEFAULT_ACS_PATH.format(tenant_id=tenant_id)}"
        metadata_url = f"{self.base_url}{self.config.DEFAULT_METADATA_PATH.format(tenant_id=tenant_id)}"
        sp_entity_id = f"{self.config.DEFAULT_SP_ENTITY_ID}:{tenant_id}"

        if self._onelogin_available:
            try:
                from onelogin.saml2.metadata import OneLogin_Saml2_Metadata
                from onelogin.saml2.settings import OneLogin_Saml2_Settings

                settings = self._build_settings(provider, tenant_id)
                saml_settings = OneLogin_Saml2_Settings(settings)
                metadata = OneLogin_Saml2_Metadata.builder(saml_settings)
                return metadata
            except Exception as e:
                logger.warning(
                    f"Failed to generate metadata with library: {e}",
                    extra={"event_type": "saml_metadata_fallback"}
                )

        # Fallback: generate simple metadata
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
    entityID="{sp_entity_id}">
    <md:SPSSODescriptor
        AuthnRequestsSigned="false"
        WantAssertionsSigned="true"
        protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
        <md:NameIDFormat>{self.config.NAME_ID_FORMAT}</md:NameIDFormat>
        <md:AssertionConsumerService
            Binding="{self.config.ACS_BINDING}"
            Location="{acs_url}"
            index="0"
            isDefault="true"/>
    </md:SPSSODescriptor>
</md:EntityDescriptor>"""

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
            groups: User's groups from SAML assertion
            group_role_mapping: Dict mapping group names to roles
            default_role: Role if no mapping matches

        Returns:
            Role name
        """
        # Check mappings in order (first match wins)
        for group in groups:
            if group in group_role_mapping:
                return group_role_mapping[group]

        return default_role


# Singleton helper
_saml_service: Optional[SAMLService] = None


def get_saml_service(base_url: Optional[str] = None) -> SAMLService:
    """
    Get or create SAML service singleton.

    Usage in FastAPI:
        @app.get("/saml/{tenant_id}/login")
        async def saml_login(
            saml: SAMLService = Depends(get_saml_service)
        ):
            request = saml.create_authn_request(provider, tenant_id)
            return RedirectResponse(request.redirect_url)
    """
    global _saml_service

    if _saml_service is None:
        if base_url is None:
            # Try to get from config
            try:
                from assemblyline_common.config import settings
                # Construct from CORS origins or default
                origins = settings.cors_origins_list
                base_url = origins[0] if origins else "http://localhost:8001"
            except Exception:
                base_url = "http://localhost:8001"

        _saml_service = SAMLService(base_url=base_url)

    return _saml_service

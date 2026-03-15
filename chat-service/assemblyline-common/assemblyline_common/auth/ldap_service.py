"""
LDAP / Active Directory Authentication Service for Logic Weaver.

Provides LDAP-based authentication with:
- Active Directory and OpenLDAP support
- Connection pooling with SSL/STARTTLS
- Attribute mapping for user provisioning
- Group-based role mapping
- Connection testing and user sync

HIPAA Requirement: Secure authentication for PHI access.

Usage:
    ldap_svc = get_ldap_service()

    # Authenticate user
    user_info = await ldap_svc.authenticate(provider_config, "username", "password")

    # Test connection
    result = await ldap_svc.test_connection(provider_config)

    # Search for user
    user_info = await ldap_svc.search_user(provider_config, "username")
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Try to import ldap3 — gracefully degrade if not installed
try:
    import ldap3
    from ldap3 import Server, Connection, ALL, SUBTREE, Tls
    from ldap3.core.exceptions import LDAPException, LDAPBindError, LDAPSocketOpenError
    _ldap3_available = True
except ImportError:
    _ldap3_available = False
    logger.info("ldap3 not installed — LDAP authentication unavailable")


@dataclass
class LDAPProviderConfig:
    """LDAP provider configuration."""
    host: str
    port: int = 389
    use_ssl: bool = False
    use_starttls: bool = False
    bind_dn: Optional[str] = None
    bind_password: Optional[str] = None
    base_dn: str = ""
    user_search_filter: str = "(sAMAccountName={username})"
    user_dn_pattern: Optional[str] = None
    attribute_mapping: Dict[str, str] = field(default_factory=lambda: {
        "email": "mail",
        "first_name": "givenName",
        "last_name": "sn",
        "display_name": "displayName"
    })
    group_search_base: Optional[str] = None
    group_search_filter: Optional[str] = None
    group_role_mapping: Dict[str, str] = field(default_factory=dict)
    auto_provision_users: bool = True
    default_role: str = "user"
    allowed_domains: List[str] = field(default_factory=list)
    name: str = "LDAP Provider"
    provider_type: str = "active_directory"


@dataclass
class LDAPUserInfo:
    """User information extracted from LDAP directory."""
    username: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    display_name: Optional[str] = None
    groups: List[str] = field(default_factory=list)
    dn: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)


class LDAPService:
    """
    LDAP authentication service.

    Supports Active Directory, OpenLDAP, and generic LDAP directories.
    Uses ldap3 library for cross-platform LDAP connectivity.
    """

    def __init__(self):
        if not _ldap3_available:
            logger.warning("LDAPService initialized without ldap3 — all operations will fail gracefully")

    def _create_server(self, config: LDAPProviderConfig) -> Any:
        """Create an ldap3 Server object from config."""
        if not _ldap3_available:
            raise RuntimeError("ldap3 library is not installed")

        tls = None
        if config.use_ssl or config.use_starttls:
            import ssl
            tls = Tls(validate=ssl.CERT_OPTIONAL)

        return Server(
            config.host,
            port=config.port,
            use_ssl=config.use_ssl,
            tls=tls,
            get_info=ALL,
            connect_timeout=10,
        )

    def _create_bind_connection(self, config: LDAPProviderConfig) -> Any:
        """Create a bound connection using service account credentials."""
        server = self._create_server(config)
        conn = Connection(
            server,
            user=config.bind_dn,
            password=config.bind_password,
            auto_bind=False,
            raise_exceptions=True,
            receive_timeout=15,
        )

        if config.use_starttls and not config.use_ssl:
            conn.start_tls()

        conn.bind()
        return conn

    def authenticate(
        self, config: LDAPProviderConfig, username: str, password: str
    ) -> Optional[LDAPUserInfo]:
        """
        Authenticate a user against LDAP.

        Two-step process:
        1. Bind with service account to search for the user DN
        2. Re-bind with the user's DN + password to verify credentials

        Returns LDAPUserInfo on success, None on failure.
        """
        if not _ldap3_available:
            logger.error("LDAP authentication unavailable — ldap3 not installed")
            return None

        try:
            # Step 1: Search for the user using service account
            user_entry = self._search_user_entry(config, username)
            if not user_entry:
                logger.warning(
                    "LDAP user not found",
                    extra={"username": username, "event_type": "ldap_user_not_found"}
                )
                return None

            user_dn = user_entry.entry_dn

            # Step 2: Bind as the user to verify password
            server = self._create_server(config)
            user_conn = Connection(
                server,
                user=user_dn,
                password=password,
                auto_bind=False,
                raise_exceptions=True,
                receive_timeout=15,
            )

            if config.use_starttls and not config.use_ssl:
                user_conn.start_tls()

            user_conn.bind()
            user_conn.unbind()

            # Extract user info from search results
            user_info = self._extract_user_info(user_entry, config, username)

            # Fetch groups if configured
            if config.group_search_base:
                user_info.groups = self._get_user_groups(config, user_dn)

            logger.info(
                "LDAP authentication successful",
                extra={
                    "username": username,
                    "email": user_info.email,
                    "event_type": "ldap_auth_success"
                }
            )

            return user_info

        except LDAPBindError:
            logger.warning(
                "LDAP authentication failed — invalid credentials",
                extra={"username": username, "event_type": "ldap_auth_failed"}
            )
            return None
        except LDAPSocketOpenError as e:
            logger.error(
                f"LDAP connection failed: {e}",
                extra={"host": config.host, "event_type": "ldap_connection_failed"}
            )
            return None
        except LDAPException as e:
            logger.error(
                f"LDAP error during authentication: {e}",
                extra={"username": username, "event_type": "ldap_auth_error"}
            )
            return None

    def search_user(
        self, config: LDAPProviderConfig, username: str
    ) -> Optional[LDAPUserInfo]:
        """Search for a user without authenticating (uses service account)."""
        if not _ldap3_available:
            return None

        try:
            entry = self._search_user_entry(config, username)
            if not entry:
                return None

            user_info = self._extract_user_info(entry, config, username)

            if config.group_search_base:
                user_info.groups = self._get_user_groups(config, entry.entry_dn)

            return user_info
        except LDAPException as e:
            logger.error(f"LDAP search failed: {e}")
            return None

    def test_connection(self, config: LDAPProviderConfig) -> Dict[str, Any]:
        """
        Test LDAP connection and return diagnostic info.

        Returns dict with success status and details.
        """
        if not _ldap3_available:
            return {
                "success": False,
                "error": "ldap3 library is not installed on this server",
            }

        try:
            conn = self._create_bind_connection(config)

            # Get server info
            info = {
                "success": True,
                "server": config.host,
                "port": config.port,
                "ssl": config.use_ssl,
                "starttls": config.use_starttls,
                "base_dn": config.base_dn,
            }

            # Try a simple search to validate base_dn
            conn.search(
                search_base=config.base_dn,
                search_filter="(objectClass=*)",
                search_scope=ldap3.BASE,
                attributes=["namingContexts"],
            )
            info["base_dn_valid"] = True

            conn.unbind()
            return info

        except LDAPBindError as e:
            return {
                "success": False,
                "error": f"Bind failed — check bind DN and password: {e}",
            }
        except LDAPSocketOpenError as e:
            return {
                "success": False,
                "error": f"Cannot connect to {config.host}:{config.port} — {e}",
            }
        except LDAPException as e:
            return {
                "success": False,
                "error": f"LDAP error: {e}",
            }

    def sync_users(
        self, config: LDAPProviderConfig, page_size: int = 500
    ) -> List[LDAPUserInfo]:
        """
        Sync all users from LDAP directory.

        Returns list of LDAPUserInfo for all matching users.
        """
        if not _ldap3_available:
            return []

        try:
            conn = self._create_bind_connection(config)

            # Build attributes list from mapping
            attrs = list(config.attribute_mapping.values())
            if "memberOf" not in attrs:
                attrs.append("memberOf")

            # Search for all users
            search_filter = config.user_search_filter.replace("{username}", "*")
            conn.search(
                search_base=config.base_dn,
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=attrs,
                paged_size=page_size,
            )

            users = []
            for entry in conn.entries:
                user_info = self._extract_user_info(entry, config, "")
                if user_info.email:  # Only include users with email
                    users.append(user_info)

            conn.unbind()

            logger.info(
                f"LDAP sync completed",
                extra={"user_count": len(users), "event_type": "ldap_sync_complete"}
            )

            return users

        except LDAPException as e:
            logger.error(f"LDAP sync failed: {e}")
            return []

    def map_groups_to_role(
        self,
        groups: List[str],
        group_role_mapping: Dict[str, str],
        default_role: str = "user",
    ) -> str:
        """Map LDAP groups to application role. Highest privilege wins."""
        if not groups or not group_role_mapping:
            return default_role

        role_priority = {
            "super_admin": 6,
            "admin": 5,
            "architecture_admin": 4,
            "editor": 3,
            "user": 2,
            "viewer": 1,
        }

        best_role = default_role
        best_priority = role_priority.get(default_role, 0)

        for group in groups:
            # Check both CN and full DN
            group_lower = group.lower()
            for mapping_key, role in group_role_mapping.items():
                if mapping_key.lower() in group_lower:
                    priority = role_priority.get(role, 0)
                    if priority > best_priority:
                        best_role = role
                        best_priority = priority

        return best_role

    def validate_email_domain(self, email: str, allowed_domains: List[str]) -> bool:
        """Check if email domain is in the allowed list."""
        if not allowed_domains:
            return True
        domain = email.split("@")[1].lower() if "@" in email else ""
        return domain in [d.lower() for d in allowed_domains]

    # ── Private helpers ──────────────────────────────────────────────────

    def _search_user_entry(self, config: LDAPProviderConfig, username: str) -> Any:
        """Search for a user entry by username."""
        conn = self._create_bind_connection(config)

        # Build attributes to request
        attrs = list(config.attribute_mapping.values())
        if "memberOf" not in attrs:
            attrs.append("memberOf")

        search_filter = config.user_search_filter.replace("{username}", username)

        conn.search(
            search_base=config.base_dn,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=attrs,
        )

        if not conn.entries:
            conn.unbind()
            return None

        entry = conn.entries[0]
        conn.unbind()
        return entry

    def _extract_user_info(
        self, entry: Any, config: LDAPProviderConfig, username: str
    ) -> LDAPUserInfo:
        """Extract user info from an LDAP entry using attribute mapping."""
        mapping = config.attribute_mapping

        def get_attr(attr_name: str) -> Optional[str]:
            try:
                val = getattr(entry, attr_name, None)
                if val is not None:
                    return str(val)
            except Exception:
                pass
            return None

        email = get_attr(mapping.get("email", "mail")) or ""
        first_name = get_attr(mapping.get("first_name", "givenName"))
        last_name = get_attr(mapping.get("last_name", "sn"))
        display_name = get_attr(mapping.get("display_name", "displayName"))

        # Extract groups from memberOf
        groups = []
        try:
            member_of = getattr(entry, "memberOf", None)
            if member_of:
                groups = [str(g) for g in member_of]
        except Exception:
            pass

        return LDAPUserInfo(
            username=username or email.split("@")[0] if email else "",
            email=email,
            first_name=first_name,
            last_name=last_name,
            display_name=display_name,
            groups=groups,
            dn=str(entry.entry_dn) if hasattr(entry, "entry_dn") else "",
            attributes={str(k): str(v) for k, v in entry.entry_attributes_as_dict.items()}
            if hasattr(entry, "entry_attributes_as_dict") else {},
        )

    def _get_user_groups(
        self, config: LDAPProviderConfig, user_dn: str
    ) -> List[str]:
        """Fetch groups for a user DN."""
        if not config.group_search_base:
            return []

        try:
            conn = self._create_bind_connection(config)

            group_filter = config.group_search_filter or f"(member={user_dn})"

            conn.search(
                search_base=config.group_search_base,
                search_filter=group_filter,
                search_scope=SUBTREE,
                attributes=["cn", "distinguishedName"],
            )

            groups = []
            for entry in conn.entries:
                cn = getattr(entry, "cn", None)
                if cn:
                    groups.append(str(cn))

            conn.unbind()
            return groups

        except LDAPException as e:
            logger.warning(f"Failed to fetch LDAP groups: {e}")
            return []


# ── Singleton ────────────────────────────────────────────────────────────

_ldap_service: Optional[LDAPService] = None


def get_ldap_service() -> LDAPService:
    """Get or create LDAP service singleton."""
    global _ldap_service

    if _ldap_service is None:
        _ldap_service = LDAPService()

    return _ldap_service

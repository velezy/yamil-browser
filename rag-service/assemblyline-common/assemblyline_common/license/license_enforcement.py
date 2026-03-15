"""
DriveSentinel License Enforcement
Controls user limits, feature access, and license validation

License Tiers and User Limits:
- consumer: 1 user (personal use)
- consumer_plus: 2-5 users (small team)
- enterprise_s: 100 users
- enterprise_m: 500 users
- enterprise_l: 1000 users
- enterprise_unlimited: Unlimited users
- developer: Unlimited (development/testing)
"""

import os
import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# LICENSE TIER DEFINITIONS
# =============================================================================

class LicenseTier(str, Enum):
    """License tiers with user limits"""
    FREE = "free"
    CONSUMER = "consumer"
    CONSUMER_PLUS = "consumer_plus"
    PRO = "pro"
    ENTERPRISE_S = "enterprise_s"
    ENTERPRISE_M = "enterprise_m"
    ENTERPRISE_L = "enterprise_l"
    ENTERPRISE_UNLIMITED = "enterprise_unlimited"
    DEVELOPER = "developer"


@dataclass
class TierConfig:
    """Configuration for each license tier"""
    max_users: int
    max_documents: int
    max_storage_gb: float
    features: List[str]
    can_invite_users: bool
    has_departments: bool
    has_cross_dept_access: bool
    cloud_ai_enabled: bool
    has_sso: bool = False  # SSO (OIDC/SAML2) - enterprise only
    # Database connection pool limits (scaled by user capacity)
    db_min_connections: int = 2
    db_max_connections: int = 10


# Tier configurations
TIER_CONFIGS: Dict[LicenseTier, TierConfig] = {
    # Connection formula: min = max(2, users * 0.1), max = max(5, users * 0.5)
    # Assumes ~50% peak concurrent users, each needing 1 connection
    LicenseTier.FREE: TierConfig(
        max_users=1,
        max_documents=10,
        max_storage_gb=0.5,
        features=["ollama"],
        can_invite_users=False,
        has_departments=False,
        has_cross_dept_access=False,
        cloud_ai_enabled=False,
        db_min_connections=2,      # min floor
        db_max_connections=5       # 1 * 0.5 = 0.5, floor to 5
    ),
    LicenseTier.CONSUMER: TierConfig(
        max_users=1,
        max_documents=100,
        max_storage_gb=5.0,
        features=["ollama"],
        can_invite_users=False,
        has_departments=False,
        has_cross_dept_access=False,
        cloud_ai_enabled=False,
        db_min_connections=2,      # 1 * 0.1 = 0.1, floor to 2
        db_max_connections=5       # 1 * 0.5 = 0.5, floor to 5
    ),
    LicenseTier.CONSUMER_PLUS: TierConfig(
        max_users=5,
        max_documents=500,
        max_storage_gb=25.0,
        features=["ollama"],
        can_invite_users=True,
        has_departments=False,
        has_cross_dept_access=False,
        cloud_ai_enabled=False,
        db_min_connections=2,      # 5 * 0.1 = 0.5, floor to 2
        db_max_connections=5       # 5 * 0.5 = 2.5, floor to 5
    ),
    LicenseTier.PRO: TierConfig(
        max_users=10,
        max_documents=1000,
        max_storage_gb=50.0,
        features=["ollama"],
        can_invite_users=True,
        has_departments=True,
        has_cross_dept_access=False,
        cloud_ai_enabled=False,
        db_min_connections=2,      # 10 * 0.1 = 1, floor to 2
        db_max_connections=5       # 10 * 0.5 = 5
    ),
    LicenseTier.ENTERPRISE_S: TierConfig(
        max_users=100,
        max_documents=10000,
        max_storage_gb=500.0,
        features=["ollama", "aws_bedrock", "azure_ai", "google_vertex", "anthropic_api", "openai_api", "sso"],
        can_invite_users=True,
        has_departments=True,
        has_cross_dept_access=True,
        cloud_ai_enabled=True,
        has_sso=True,
        db_min_connections=10,     # 100 * 0.1 = 10
        db_max_connections=50      # 100 * 0.5 = 50
    ),
    LicenseTier.ENTERPRISE_M: TierConfig(
        max_users=500,
        max_documents=50000,
        max_storage_gb=2000.0,
        features=["ollama", "aws_bedrock", "azure_ai", "google_vertex", "anthropic_api", "openai_api", "sso"],
        can_invite_users=True,
        has_departments=True,
        has_cross_dept_access=True,
        cloud_ai_enabled=True,
        has_sso=True,
        db_min_connections=50,     # 500 * 0.1 = 50
        db_max_connections=250     # 500 * 0.5 = 250
    ),
    LicenseTier.ENTERPRISE_L: TierConfig(
        max_users=1000,
        max_documents=100000,
        max_storage_gb=5000.0,
        features=["ollama", "aws_bedrock", "azure_ai", "google_vertex", "anthropic_api", "openai_api", "sso"],
        can_invite_users=True,
        has_departments=True,
        has_cross_dept_access=True,
        cloud_ai_enabled=True,
        has_sso=True,
        db_min_connections=100,    # 1000 * 0.1 = 100
        db_max_connections=500     # 1000 * 0.5 = 500
    ),
    LicenseTier.ENTERPRISE_UNLIMITED: TierConfig(
        max_users=999999,  # Effectively unlimited
        max_documents=999999,
        max_storage_gb=999999.0,
        features=["ollama", "aws_bedrock", "azure_ai", "google_vertex", "anthropic_api", "openai_api", "sso"],
        can_invite_users=True,
        has_departments=True,
        has_cross_dept_access=True,
        cloud_ai_enabled=True,
        has_sso=True,
        db_min_connections=100,    # Reasonable default for "unlimited"
        db_max_connections=1000    # Scale as needed
    ),
    LicenseTier.DEVELOPER: TierConfig(
        max_users=999999,
        max_documents=999999,
        max_storage_gb=999999.0,
        features=["ollama", "aws_bedrock", "azure_ai", "google_vertex", "anthropic_api", "openai_api", "sso", "developer_tools"],
        can_invite_users=True,
        has_departments=True,
        has_cross_dept_access=True,
        cloud_ai_enabled=True,
        has_sso=True,
        db_min_connections=2,      # Low for dev machines
        db_max_connections=20      # Enough for testing
    ),
}


# =============================================================================
# LICENSE ENFORCEMENT CLASS
# =============================================================================

class LicenseEnforcer:
    """
    Enforces license limits for organizations.

    Usage:
        enforcer = LicenseEnforcer()

        # Check if can add user
        can_add, message = await enforcer.can_add_user(org_id)

        # Get remaining user slots
        remaining = await enforcer.get_remaining_user_slots(org_id)

        # Validate feature access
        has_feature = enforcer.has_feature(tier, "aws_bedrock")
    """

    def __init__(self):
        self._db_available = False
        self._initialize_db()

    def _initialize_db(self):
        """Check if database is available"""
        try:
            from assemblyline_common.database import get_connection
            self._db_available = True
        except ImportError:
            logger.warning("Database not available for license enforcement")
            self._db_available = False

    def get_tier_config(self, tier: str) -> TierConfig:
        """Get configuration for a license tier"""
        tier_lower = tier.lower()

        # Map legacy/shorthand tier names to specific tiers
        tier_aliases = {
            "enterprise": "enterprise_s",  # Default enterprise to enterprise_s
        }
        tier_lower = tier_aliases.get(tier_lower, tier_lower)

        try:
            license_tier = LicenseTier(tier_lower)
            return TIER_CONFIGS.get(license_tier, TIER_CONFIGS[LicenseTier.FREE])
        except ValueError:
            logger.warning(f"Unknown license tier: {tier}, defaulting to FREE")
            return TIER_CONFIGS[LicenseTier.FREE]

    def get_max_users(self, tier: str) -> int:
        """Get maximum users allowed for a tier"""
        config = self.get_tier_config(tier)
        return config.max_users

    def has_feature(self, tier: str, feature: str) -> bool:
        """Check if a tier has access to a feature"""
        config = self.get_tier_config(tier)
        return feature in config.features

    def can_invite_users(self, tier: str) -> bool:
        """Check if tier allows inviting users"""
        config = self.get_tier_config(tier)
        return config.can_invite_users

    def has_departments(self, tier: str) -> bool:
        """Check if tier supports department isolation"""
        config = self.get_tier_config(tier)
        return config.has_departments

    def has_cross_dept_access(self, tier: str) -> bool:
        """Check if tier supports cross-department access grants"""
        config = self.get_tier_config(tier)
        return config.has_cross_dept_access

    def has_cloud_ai(self, tier: str) -> bool:
        """Check if tier supports cloud AI providers"""
        config = self.get_tier_config(tier)
        return config.cloud_ai_enabled

    def has_sso(self, tier: str) -> bool:
        """Check if tier supports SSO (OIDC/SAML2)"""
        config = self.get_tier_config(tier)
        return config.has_sso

    def get_db_connection_limits(self, tier: str) -> tuple:
        """Get database connection pool limits for a tier

        Returns:
            tuple: (min_connections, max_connections)
        """
        config = self.get_tier_config(tier)
        return (config.db_min_connections, config.db_max_connections)

    async def get_organization_info(self, org_id: int) -> Optional[Dict[str, Any]]:
        """Get organization details including license tier and user count"""
        if not self._db_available:
            return None

        try:
            from assemblyline_common.database import get_connection

            async with get_connection() as conn:
                org = await conn.fetchrow("""
                    SELECT
                        o.id,
                        o.name,
                        o.license_tier,
                        o.max_users,
                        o.settings,
                        COUNT(u.id) as current_users
                    FROM organizations o
                    LEFT JOIN users u ON u.organization_id = o.id AND u.is_active = TRUE
                    WHERE o.id = $1
                    GROUP BY o.id
                """, org_id)

                if org:
                    return dict(org)
                return None
        except Exception as e:
            logger.error(f"Failed to get organization info: {e}")
            return None

    async def can_add_user(self, org_id: int) -> tuple[bool, str]:
        """
        Check if organization can add another user.

        Returns:
            tuple: (can_add: bool, message: str)
        """
        org_info = await self.get_organization_info(org_id)

        if not org_info:
            return False, "Organization not found"

        tier = org_info.get("license_tier", "free")
        config = self.get_tier_config(tier)
        current_users = org_info.get("current_users", 0)
        max_users = config.max_users

        # Check if at user limit
        if current_users >= max_users:
            return False, f"User limit reached ({current_users}/{max_users}). Upgrade your license to add more users."

        # Check if tier allows inviting users
        if current_users > 0 and not config.can_invite_users:
            return False, f"Your {tier} license only supports a single user. Upgrade to add team members."

        return True, f"Can add user ({current_users}/{max_users} used)"

    async def get_remaining_user_slots(self, org_id: int) -> int:
        """Get number of remaining user slots for organization"""
        org_info = await self.get_organization_info(org_id)

        if not org_info:
            return 0

        tier = org_info.get("license_tier", "free")
        config = self.get_tier_config(tier)
        current_users = org_info.get("current_users", 0)

        return max(0, config.max_users - current_users)

    async def validate_user_addition(self, org_id: int, requesting_user_id: int) -> tuple[bool, str]:
        """
        Validate that a user can be added to the organization.
        Checks license limits and requesting user's permissions.

        Returns:
            tuple: (valid: bool, message: str)
        """
        # Check if can add user based on license
        can_add, message = await self.can_add_user(org_id)
        if not can_add:
            return False, message

        # Check if requesting user is admin of the organization
        if not self._db_available:
            return False, "Database not available"

        try:
            from assemblyline_common.database import get_connection

            async with get_connection() as conn:
                user = await conn.fetchrow("""
                    SELECT id, role, organization_id
                    FROM users
                    WHERE id = $1
                """, requesting_user_id)

                if not user:
                    return False, "Requesting user not found"

                if user["organization_id"] != org_id:
                    return False, "Cannot add users to another organization"

                if user["role"] not in ("admin", "superadmin", "org_admin"):
                    return False, "Only admins can invite users"

                return True, "User addition validated"

        except Exception as e:
            logger.error(f"Failed to validate user addition: {e}")
            return False, f"Validation failed: {str(e)}"

    async def update_organization_license(
        self,
        org_id: int,
        new_tier: str,
        activated_by: int
    ) -> tuple[bool, str]:
        """
        Update organization's license tier.

        This is called when a license is activated/upgraded.
        """
        if not self._db_available:
            return False, "Database not available"

        try:
            config = self.get_tier_config(new_tier)

            from assemblyline_common.database import get_connection

            async with get_connection() as conn:
                await conn.execute("""
                    UPDATE organizations
                    SET
                        license_tier = $1,
                        max_users = $2,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = $3
                """, new_tier, config.max_users, org_id)

                # Log the license change
                logger.info(f"Organization {org_id} license updated to {new_tier} by user {activated_by}")

                return True, f"License updated to {new_tier} (max {config.max_users} users)"

        except Exception as e:
            logger.error(f"Failed to update organization license: {e}")
            return False, f"Failed to update license: {str(e)}"

    async def get_license_status(self, org_id: int) -> Dict[str, Any]:
        """Get complete license status for an organization"""
        org_info = await self.get_organization_info(org_id)

        if not org_info:
            return {
                "valid": False,
                "error": "Organization not found"
            }

        tier = org_info.get("license_tier", "free")
        config = self.get_tier_config(tier)
        current_users = org_info.get("current_users", 0)

        return {
            "valid": True,
            "organization_id": org_id,
            "organization_name": org_info.get("name"),
            "license_tier": tier,
            "tier_display": tier.replace("_", " ").title(),
            "limits": {
                "max_users": config.max_users,
                "current_users": current_users,
                "remaining_users": max(0, config.max_users - current_users),
                "max_documents": config.max_documents,
                "max_storage_gb": config.max_storage_gb
            },
            "features": {
                "can_invite_users": config.can_invite_users,
                "has_departments": config.has_departments,
                "has_cross_dept_access": config.has_cross_dept_access,
                "cloud_ai_enabled": config.cloud_ai_enabled,
                "has_sso": config.has_sso,
                "available_ai_providers": config.features
            },
            "at_user_limit": current_users >= config.max_users
        }


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_enforcer_instance: Optional[LicenseEnforcer] = None


def get_license_enforcer() -> LicenseEnforcer:
    """Get singleton license enforcer instance"""
    global _enforcer_instance
    if _enforcer_instance is None:
        _enforcer_instance = LicenseEnforcer()
    return _enforcer_instance


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def check_user_limit(org_id: int) -> tuple[bool, str]:
    """Check if organization can add another user"""
    enforcer = get_license_enforcer()
    return await enforcer.can_add_user(org_id)


async def get_org_license_status(org_id: int) -> Dict[str, Any]:
    """Get license status for organization"""
    enforcer = get_license_enforcer()
    return await enforcer.get_license_status(org_id)


def get_tier_max_users(tier: str) -> int:
    """Get max users for a tier"""
    enforcer = get_license_enforcer()
    return enforcer.get_max_users(tier)


def tier_has_feature(tier: str, feature: str) -> bool:
    """Check if tier has a feature"""
    enforcer = get_license_enforcer()
    return enforcer.has_feature(tier, feature)


def get_db_connection_limits_for_tier(tier: str) -> tuple:
    """Get database connection pool limits for a license tier

    Args:
        tier: License tier name (e.g., 'consumer', 'enterprise_s')

    Returns:
        tuple: (min_connections, max_connections)
    """
    enforcer = get_license_enforcer()
    return enforcer.get_db_connection_limits(tier)


def tier_has_sso(tier: str) -> bool:
    """Check if tier has SSO (OIDC/SAML2) support"""
    enforcer = get_license_enforcer()
    return enforcer.has_sso(tier)

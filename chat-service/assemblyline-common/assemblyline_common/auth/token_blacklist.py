"""
JWT Token Blacklist/Revocation for Logic Weaver.

Provides Redis-backed token revocation supporting:
- Individual token revocation (logout)
- Bulk revocation by user (password change, security event)
- Bulk revocation by tenant (security breach)

All operations are O(1) and support high concurrency.

Usage:
    blacklist = TokenBlacklist(redis_client)

    # On logout - revoke specific token
    await blacklist.revoke_token(jti, exp_timestamp)

    # On password change - revoke all user tokens
    await blacklist.revoke_all_user_tokens(user_id)

    # On token verification - check if revoked
    if await blacklist.is_revoked(jti):
        raise HTTPException(401, "Token revoked")
"""

from datetime import datetime, timezone
from typing import Optional, List
from uuid import UUID
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class TokenBlacklistConfig:
    """Configuration for token blacklist."""

    # Redis key prefix
    KEY_PREFIX: str = "token_blacklist"

    # Maximum time to keep revoked tokens (should match max token lifetime)
    # Set to 8 days to cover refresh tokens (7 days) with buffer
    MAX_TTL_SECONDS: int = 691200  # 8 days

    # User tokens set TTL (for tracking user's active token JTIs)
    USER_TOKENS_TTL_SECONDS: int = 691200


class TokenBlacklist:
    """
    Redis-backed JWT token blacklist for revocation.

    Uses Redis for O(1) lookup performance at scale.
    Tokens auto-expire from blacklist when their natural expiration passes.

    Supports:
    - Per-token revocation (logout)
    - Per-user bulk revocation (password change, account compromise)
    - Per-tenant bulk revocation (security incident)
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        config: Optional[TokenBlacklistConfig] = None
    ):
        self.redis = redis_client
        self.config = config or TokenBlacklistConfig()

    def _token_key(self, jti: str) -> str:
        """Generate Redis key for a specific token."""
        return f"{self.config.KEY_PREFIX}:token:{jti}"

    def _user_tokens_key(self, user_id: UUID) -> str:
        """Generate Redis key for user's token tracking set."""
        return f"{self.config.KEY_PREFIX}:user_tokens:{user_id}"

    def _tenant_tokens_key(self, tenant_id: UUID) -> str:
        """Generate Redis key for tenant's token tracking set."""
        return f"{self.config.KEY_PREFIX}:tenant_tokens:{tenant_id}"

    def _global_revoke_key(self, user_id: UUID) -> str:
        """Generate Redis key for user's global revoke timestamp."""
        return f"{self.config.KEY_PREFIX}:global_revoke:{user_id}"

    async def register_token(
        self,
        jti: str,
        user_id: UUID,
        tenant_id: UUID,
        exp_timestamp: int,
        token_type: str = "access"
    ):
        """
        Register a newly issued token for tracking.

        Call this when issuing a new token to enable bulk revocation later.

        Args:
            jti: JWT ID (unique token identifier)
            user_id: User who owns this token
            tenant_id: Tenant the token belongs to
            exp_timestamp: Token expiration timestamp
            token_type: "access" or "refresh"
        """
        now = int(datetime.now(timezone.utc).timestamp())
        ttl = max(1, exp_timestamp - now)

        # Add to user's token set
        user_key = self._user_tokens_key(user_id)
        await self.redis.sadd(user_key, jti)
        await self.redis.expire(user_key, self.config.USER_TOKENS_TTL_SECONDS)

        # Add to tenant's token set
        tenant_key = self._tenant_tokens_key(tenant_id)
        await self.redis.sadd(tenant_key, jti)
        await self.redis.expire(tenant_key, self.config.USER_TOKENS_TTL_SECONDS)

        logger.debug(
            f"Registered token {jti[:8]}... for user {user_id}",
            extra={
                "jti": jti,
                "user_id": str(user_id),
                "tenant_id": str(tenant_id),
                "token_type": token_type,
                "event_type": "token_registered"
            }
        )

    async def revoke_token(
        self,
        jti: str,
        exp_timestamp: int,
        reason: str = "logout",
        revoked_by: Optional[str] = None
    ):
        """
        Revoke a specific token.

        The token is blacklisted until its natural expiration.

        Args:
            jti: JWT ID to revoke
            exp_timestamp: Token's expiration timestamp
            reason: Reason for revocation (logout, password_change, security)
            revoked_by: User/system that initiated revocation
        """
        token_key = self._token_key(jti)

        # Calculate TTL - only need to blacklist until token expires naturally
        now = int(datetime.now(timezone.utc).timestamp())
        ttl = max(1, exp_timestamp - now)

        # Store revocation info
        revocation_data = {
            "revoked_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "revoked_by": revoked_by or "system"
        }

        await self.redis.hset(token_key, mapping=revocation_data)
        await self.redis.expire(token_key, ttl)

        logger.info(
            f"Token revoked: {jti[:8]}...",
            extra={
                "jti": jti,
                "reason": reason,
                "revoked_by": revoked_by,
                "ttl_seconds": ttl,
                "event_type": "token_revoked"
            }
        )

    async def is_revoked(self, jti: str) -> bool:
        """
        Check if a token has been revoked.

        O(1) operation - safe for use in hot path.

        Args:
            jti: JWT ID to check

        Returns:
            True if token is revoked
        """
        token_key = self._token_key(jti)
        return await self.redis.exists(token_key) > 0

    async def is_token_valid_for_user(
        self,
        jti: str,
        user_id: UUID,
        issued_at: int
    ) -> bool:
        """
        Check if token is valid considering global revocation.

        Tokens issued before a global revoke timestamp are invalid.

        Args:
            jti: JWT ID
            user_id: Token owner
            issued_at: Token issued-at timestamp

        Returns:
            True if token is valid (not individually or globally revoked)
        """
        # Check individual revocation
        if await self.is_revoked(jti):
            return False

        # Check global revocation timestamp
        global_key = self._global_revoke_key(user_id)
        revoke_timestamp = await self.redis.get(global_key)

        if revoke_timestamp:
            revoke_ts = int(revoke_timestamp)
            if issued_at < revoke_ts:
                logger.debug(
                    f"Token {jti[:8]}... rejected due to global revocation",
                    extra={
                        "jti": jti,
                        "user_id": str(user_id),
                        "issued_at": issued_at,
                        "revoke_timestamp": revoke_ts,
                        "event_type": "token_globally_revoked"
                    }
                )
                return False

        return True

    async def revoke_all_user_tokens(
        self,
        user_id: UUID,
        reason: str = "password_change",
        revoked_by: Optional[str] = None
    ) -> int:
        """
        Revoke all tokens for a user.

        Uses global revoke timestamp for efficiency - all tokens issued
        before this timestamp are considered invalid.

        Args:
            user_id: User whose tokens to revoke
            reason: Reason for revocation
            revoked_by: User/admin who initiated

        Returns:
            Approximate number of tokens affected
        """
        # Set global revoke timestamp
        global_key = self._global_revoke_key(user_id)
        now = int(datetime.now(timezone.utc).timestamp())
        await self.redis.set(
            global_key,
            str(now),
            ex=self.config.MAX_TTL_SECONDS
        )

        # Get count of tracked tokens (approximate)
        user_key = self._user_tokens_key(user_id)
        token_count = await self.redis.scard(user_key)

        # Clear tracking set
        await self.redis.delete(user_key)

        logger.warning(
            f"All tokens revoked for user {user_id}",
            extra={
                "user_id": str(user_id),
                "reason": reason,
                "revoked_by": revoked_by,
                "token_count": token_count,
                "event_type": "user_tokens_revoked"
            }
        )

        return int(token_count)

    async def revoke_all_tenant_tokens(
        self,
        tenant_id: UUID,
        reason: str = "security_incident",
        revoked_by: Optional[str] = None
    ) -> int:
        """
        Revoke all tokens for a tenant (security incident response).

        Args:
            tenant_id: Tenant whose tokens to revoke
            reason: Reason for revocation
            revoked_by: Admin who initiated

        Returns:
            Number of tokens revoked
        """
        tenant_key = self._tenant_tokens_key(tenant_id)

        # Get all token JTIs
        token_jtis = await self.redis.smembers(tenant_key)
        count = len(token_jtis)

        # Revoke each token
        now = int(datetime.now(timezone.utc).timestamp())
        for jti_bytes in token_jtis:
            jti = jti_bytes.decode() if isinstance(jti_bytes, bytes) else jti_bytes
            # Use max TTL since we don't know individual expiration
            await self.revoke_token(
                jti,
                now + self.config.MAX_TTL_SECONDS,
                reason=reason,
                revoked_by=revoked_by
            )

        # Clear tracking set
        await self.redis.delete(tenant_key)

        logger.warning(
            f"All tokens revoked for tenant {tenant_id}",
            extra={
                "tenant_id": str(tenant_id),
                "reason": reason,
                "revoked_by": revoked_by,
                "token_count": count,
                "event_type": "tenant_tokens_revoked"
            }
        )

        return count

    async def get_revocation_info(self, jti: str) -> Optional[dict]:
        """
        Get revocation details for a token.

        Args:
            jti: JWT ID

        Returns:
            Dict with revocation info or None if not revoked
        """
        token_key = self._token_key(jti)

        if not await self.redis.exists(token_key):
            return None

        data = await self.redis.hgetall(token_key)
        ttl = await self.redis.ttl(token_key)

        return {
            "jti": jti,
            "revoked_at": data.get(b"revoked_at", b"").decode(),
            "reason": data.get(b"reason", b"").decode(),
            "revoked_by": data.get(b"revoked_by", b"").decode(),
            "ttl_remaining": max(0, ttl)
        }

    async def get_user_active_tokens(self, user_id: UUID) -> List[str]:
        """
        Get list of active (non-revoked) token JTIs for a user.

        Args:
            user_id: User ID

        Returns:
            List of JTI strings
        """
        user_key = self._user_tokens_key(user_id)
        token_jtis = await self.redis.smembers(user_key)

        active_tokens = []
        for jti_bytes in token_jtis:
            jti = jti_bytes.decode() if isinstance(jti_bytes, bytes) else jti_bytes
            if not await self.is_revoked(jti):
                active_tokens.append(jti)

        return active_tokens


# Singleton helper for FastAPI dependency injection
_token_blacklist_instance: Optional[TokenBlacklist] = None


async def get_token_blacklist(
    redis_url: Optional[str] = None
) -> TokenBlacklist:
    """
    Get or create token blacklist instance.

    Usage in FastAPI:
        @app.post("/logout")
        async def logout(
            blacklist: TokenBlacklist = Depends(get_token_blacklist)
        ):
            ...
    """
    global _token_blacklist_instance

    if _token_blacklist_instance is None:
        from assemblyline_common.config import settings
        url = redis_url or settings.REDIS_URL
        redis_client = redis.from_url(url, decode_responses=False)
        _token_blacklist_instance = TokenBlacklist(redis_client)

    return _token_blacklist_instance

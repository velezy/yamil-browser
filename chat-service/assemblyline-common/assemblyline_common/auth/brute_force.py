"""
Brute Force Protection for Logic Weaver Authentication.

Provides Redis-backed tracking of failed login attempts with automatic lockout.
HIPAA Requirement: Account lockout after failed authentication attempts.

Usage:
    brute_force = BruteForceProtection(redis_client)

    # Check before login
    if await brute_force.is_locked(email, ip_address):
        raise HTTPException(429, "Account locked")

    # On failed login
    await brute_force.record_failed_attempt(email, ip_address)

    # On successful login
    await brute_force.clear_attempts(email, ip_address)
"""

from datetime import datetime, timezone
from typing import Optional, Tuple
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class BruteForceConfig:
    """Configuration for brute force protection."""

    # Maximum failed attempts before lockout
    MAX_ATTEMPTS: int = 5

    # Lockout duration in seconds (30 minutes)
    LOCKOUT_DURATION_SECONDS: int = 1800

    # Time window for counting attempts in seconds (15 minutes)
    ATTEMPT_WINDOW_SECONDS: int = 900

    # Redis key prefix
    KEY_PREFIX: str = "brute_force"

    # Whether to track by IP in addition to email
    TRACK_BY_IP: bool = True

    # Maximum failed attempts per IP (higher than per-email)
    MAX_ATTEMPTS_PER_IP: int = 20


class BruteForceProtection:
    """
    Redis-backed brute force protection.

    Tracks failed login attempts per email and IP address.
    Implements automatic lockout after threshold exceeded.

    HIPAA Technical Safeguard: 164.312(d) Person or Entity Authentication
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        config: Optional[BruteForceConfig] = None
    ):
        self.redis = redis_client
        self.config = config or BruteForceConfig()

    def _email_key(self, email: str) -> str:
        """Generate Redis key for email-based tracking."""
        email_normalized = email.lower().strip()
        return f"{self.config.KEY_PREFIX}:email:{email_normalized}"

    def _ip_key(self, ip_address: str) -> str:
        """Generate Redis key for IP-based tracking."""
        return f"{self.config.KEY_PREFIX}:ip:{ip_address}"

    def _lockout_key(self, email: str) -> str:
        """Generate Redis key for lockout status."""
        email_normalized = email.lower().strip()
        return f"{self.config.KEY_PREFIX}:lockout:{email_normalized}"

    async def record_failed_attempt(
        self,
        email: str,
        ip_address: Optional[str] = None
    ) -> Tuple[int, bool]:
        """
        Record a failed login attempt.

        Args:
            email: User email address
            ip_address: Client IP address (optional)

        Returns:
            Tuple of (current_attempts, is_now_locked)
        """
        now = datetime.now(timezone.utc).timestamp()
        email_key = self._email_key(email)

        # Add attempt to sorted set with timestamp as score
        await self.redis.zadd(email_key, {f"{now}:{ip_address or 'unknown'}": now})

        # Remove old attempts outside window
        window_start = now - self.config.ATTEMPT_WINDOW_SECONDS
        await self.redis.zremrangebyscore(email_key, 0, window_start)

        # Set expiry on the key
        await self.redis.expire(email_key, self.config.ATTEMPT_WINDOW_SECONDS)

        # Count current attempts
        current_attempts = await self.redis.zcard(email_key)

        # Track IP separately if enabled
        if self.config.TRACK_BY_IP and ip_address:
            ip_key = self._ip_key(ip_address)
            await self.redis.zadd(ip_key, {f"{now}:{email}": now})
            await self.redis.zremrangebyscore(ip_key, 0, window_start)
            await self.redis.expire(ip_key, self.config.ATTEMPT_WINDOW_SECONDS)

        # Check if should lock
        is_now_locked = current_attempts >= self.config.MAX_ATTEMPTS

        if is_now_locked:
            await self._set_lockout(email, ip_address)
            logger.warning(
                f"Account locked due to {current_attempts} failed attempts",
                extra={
                    "email": email,
                    "ip_address": ip_address,
                    "attempts": current_attempts,
                    "event_type": "account_lockout"
                }
            )

        return int(current_attempts), is_now_locked

    async def _set_lockout(self, email: str, ip_address: Optional[str] = None):
        """Set lockout status in Redis."""
        lockout_key = self._lockout_key(email)
        lockout_data = {
            "locked_at": datetime.now(timezone.utc).isoformat(),
            "ip_address": ip_address or "unknown",
            "reason": "max_attempts_exceeded"
        }

        await self.redis.hset(lockout_key, mapping=lockout_data)
        await self.redis.expire(lockout_key, self.config.LOCKOUT_DURATION_SECONDS)

    async def is_locked(
        self,
        email: str,
        ip_address: Optional[str] = None
    ) -> bool:
        """
        Check if an account is currently locked.

        Args:
            email: User email address
            ip_address: Client IP address (optional)

        Returns:
            True if account is locked
        """
        # Check email lockout
        lockout_key = self._lockout_key(email)
        if await self.redis.exists(lockout_key):
            return True

        # Check IP-based lockout if enabled
        if self.config.TRACK_BY_IP and ip_address:
            ip_key = self._ip_key(ip_address)
            ip_attempts = await self.redis.zcard(ip_key)
            if ip_attempts >= self.config.MAX_ATTEMPTS_PER_IP:
                logger.warning(
                    f"IP address blocked due to {ip_attempts} failed attempts",
                    extra={
                        "ip_address": ip_address,
                        "attempts": ip_attempts,
                        "event_type": "ip_blocked"
                    }
                )
                return True

        return False

    async def get_lockout_info(self, email: str) -> Optional[dict]:
        """
        Get lockout information for an account.

        Returns:
            Dict with lockout details or None if not locked
        """
        lockout_key = self._lockout_key(email)

        if not await self.redis.exists(lockout_key):
            return None

        data = await self.redis.hgetall(lockout_key)
        ttl = await self.redis.ttl(lockout_key)

        return {
            "email": email,
            "locked_at": data.get(b"locked_at", b"").decode(),
            "ip_address": data.get(b"ip_address", b"").decode(),
            "reason": data.get(b"reason", b"").decode(),
            "remaining_seconds": max(0, ttl),
            "unlocks_at": datetime.now(timezone.utc).timestamp() + max(0, ttl)
        }

    async def get_remaining_lockout_seconds(self, email: str) -> int:
        """
        Get remaining lockout time in seconds.

        Returns:
            Seconds until lockout expires, 0 if not locked
        """
        lockout_key = self._lockout_key(email)
        ttl = await self.redis.ttl(lockout_key)
        return max(0, ttl)

    async def get_attempt_count(self, email: str) -> int:
        """
        Get current failed attempt count for an email.

        Returns:
            Number of failed attempts in the current window
        """
        email_key = self._email_key(email)

        # Clean up old attempts first
        now = datetime.now(timezone.utc).timestamp()
        window_start = now - self.config.ATTEMPT_WINDOW_SECONDS
        await self.redis.zremrangebyscore(email_key, 0, window_start)

        count = await self.redis.zcard(email_key)
        return int(count)

    async def clear_attempts(
        self,
        email: str,
        ip_address: Optional[str] = None
    ):
        """
        Clear failed attempts on successful login.

        Args:
            email: User email address
            ip_address: Client IP address (optional)
        """
        email_key = self._email_key(email)
        lockout_key = self._lockout_key(email)

        # Clear attempt history
        await self.redis.delete(email_key)

        # Clear lockout if exists
        await self.redis.delete(lockout_key)

        logger.info(
            f"Cleared failed attempts for account",
            extra={
                "email": email,
                "ip_address": ip_address,
                "event_type": "attempts_cleared"
            }
        )

    async def unlock_account(self, email: str, admin_user: str):
        """
        Manually unlock an account (admin action).

        Args:
            email: User email to unlock
            admin_user: Admin user performing the action
        """
        await self.clear_attempts(email)

        logger.warning(
            f"Account manually unlocked by admin",
            extra={
                "email": email,
                "admin_user": admin_user,
                "event_type": "admin_unlock"
            }
        )

    async def get_ip_attempt_count(self, ip_address: str) -> int:
        """
        Get failed attempt count for an IP address.

        Returns:
            Number of failed attempts from this IP
        """
        ip_key = self._ip_key(ip_address)

        now = datetime.now(timezone.utc).timestamp()
        window_start = now - self.config.ATTEMPT_WINDOW_SECONDS
        await self.redis.zremrangebyscore(ip_key, 0, window_start)

        count = await self.redis.zcard(ip_key)
        return int(count)


# Singleton helper for FastAPI dependency injection
_brute_force_instance: Optional[BruteForceProtection] = None


async def get_brute_force_protection(
    redis_url: Optional[str] = None
) -> BruteForceProtection:
    """
    Get or create brute force protection instance.

    Usage in FastAPI:
        @app.post("/login")
        async def login(
            brute_force: BruteForceProtection = Depends(get_brute_force_protection)
        ):
            ...
    """
    global _brute_force_instance

    if _brute_force_instance is None:
        from assemblyline_common.config import settings
        url = redis_url or settings.REDIS_URL
        redis_client = redis.from_url(url, decode_responses=False)
        _brute_force_instance = BruteForceProtection(redis_client)

    return _brute_force_instance

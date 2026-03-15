"""
Multi-Factor Authentication (MFA) Service for Logic Weaver.

Provides TOTP-based MFA with:
- TOTP secret generation and verification
- QR code generation for authenticator apps
- Backup codes for account recovery
- Redis-based rate limiting for verification attempts

HIPAA Requirement: Multi-factor authentication for PHI access.

Usage:
    mfa = await get_mfa_service()

    # Start enrollment
    enrollment = await mfa.create_enrollment("user@example.com")
    # Returns: secret, qr_code_uri, qr_code_base64

    # Verify code to complete enrollment
    is_valid = await mfa.verify_code(secret, "123456")

    # Generate backup codes
    codes = mfa.generate_backup_codes()
"""

import base64
import hashlib
import io
import logging
import secrets
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pyotp
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class MFAConfig:
    """Configuration for MFA service."""

    # TOTP settings
    ISSUER_NAME: str = "Y.A.M.I.L"
    TOTP_DIGITS: int = 6
    TOTP_INTERVAL: int = 30  # seconds
    TOTP_ALGORITHM: str = "SHA1"  # Most compatible with authenticator apps

    # Allowed time drift for verification (number of intervals)
    TOTP_VALID_WINDOW: int = 1  # Allow 1 interval before/after current

    # Backup codes
    BACKUP_CODE_COUNT: int = 10
    BACKUP_CODE_LENGTH: int = 8

    # Rate limiting
    RATE_LIMIT_MAX_ATTEMPTS: int = 5
    RATE_LIMIT_WINDOW_SECONDS: int = 300  # 5 minutes
    RATE_LIMIT_KEY_PREFIX: str = "mfa_rate"

    # Redis key prefixes
    USED_CODE_PREFIX: str = "mfa_used"
    USED_CODE_TTL_SECONDS: int = 60  # Prevent code reuse within TOTP interval


@dataclass
class MFAEnrollment:
    """MFA enrollment data returned to user."""
    secret: str
    qr_code_uri: str
    qr_code_base64: str
    issuer: str
    account: str


class MFAService:
    """
    TOTP-based Multi-Factor Authentication service.

    Provides secure MFA with:
    - TOTP generation compatible with Google Authenticator, Authy, etc.
    - QR codes for easy enrollment
    - Backup codes for recovery
    - Rate limiting to prevent brute force attacks
    - Code reuse prevention

    HIPAA Technical Safeguard: 164.312(d) Person or Entity Authentication
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        config: Optional[MFAConfig] = None
    ):
        """
        Initialize MFA service.

        Args:
            redis_client: Optional Redis client for rate limiting and code reuse prevention
            config: Optional configuration override
        """
        self.redis = redis_client
        self.config = config or MFAConfig()

    def generate_secret(self) -> str:
        """
        Generate a new TOTP secret.

        Returns:
            Base32-encoded secret string
        """
        return pyotp.random_base32()

    async def create_enrollment(
        self,
        account_email: str,
        issuer: Optional[str] = None
    ) -> MFAEnrollment:
        """
        Create MFA enrollment for a user.

        Args:
            account_email: User's email address (shown in authenticator)
            issuer: Issuer name (defaults to config)

        Returns:
            MFAEnrollment with secret and QR code
        """
        secret = self.generate_secret()
        issuer = issuer or self.config.ISSUER_NAME

        # Create TOTP object
        totp = pyotp.TOTP(secret, digits=self.config.TOTP_DIGITS)

        # Generate provisioning URI (for QR code)
        provisioning_uri = totp.provisioning_uri(
            name=account_email,
            issuer_name=issuer
        )

        # Generate QR code
        qr_code_base64 = self._generate_qr_code(provisioning_uri)

        logger.info(
            "MFA enrollment created",
            extra={
                "account": account_email,
                "issuer": issuer,
                "event_type": "mfa_enrollment_created"
            }
        )

        return MFAEnrollment(
            secret=secret,
            qr_code_uri=provisioning_uri,
            qr_code_base64=qr_code_base64,
            issuer=issuer,
            account=account_email
        )

    def _generate_qr_code(self, data: str) -> str:
        """Generate QR code as base64-encoded PNG."""
        try:
            import qrcode

            qr = qrcode.QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=10,
                border=4,
            )
            qr.add_data(data)
            qr.make(fit=True)

            # Create image using default Pillow backend
            img = qr.make_image(fill_color="black", back_color="white")

            # Convert to base64
            buffer = io.BytesIO()
            img.save(buffer, format="PNG")
            buffer.seek(0)
            base64_data = base64.b64encode(buffer.getvalue()).decode("ascii")

            return f"data:image/png;base64,{base64_data}"

        except ImportError:
            logger.warning(
                "qrcode package not installed, returning empty QR",
                extra={"event_type": "qr_generation_skipped"}
            )
            return ""

    async def verify_code(
        self,
        secret: str,
        code: str,
        user_id: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Verify a TOTP code.

        Args:
            secret: User's TOTP secret
            code: 6-digit code from authenticator
            user_id: Optional user ID for rate limiting

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Rate limit check
        if user_id and self.redis:
            is_limited, remaining = await self._check_rate_limit(user_id)
            if is_limited:
                logger.warning(
                    "MFA verification rate limited",
                    extra={
                        "user_id": user_id,
                        "event_type": "mfa_rate_limited"
                    }
                )
                return False, f"Too many attempts. Try again in {remaining} seconds."

        # Verify code
        totp = pyotp.TOTP(secret, digits=self.config.TOTP_DIGITS)
        is_valid = totp.verify(code, valid_window=self.config.TOTP_VALID_WINDOW)

        if not is_valid:
            # Record failed attempt
            if user_id and self.redis:
                await self._record_failed_attempt(user_id)

            logger.warning(
                "Invalid MFA code",
                extra={
                    "user_id": user_id,
                    "event_type": "mfa_code_invalid"
                }
            )
            return False, "Invalid code"

        # Check for code reuse
        if self.redis:
            code_hash = self._hash_code(secret, code)
            if await self._is_code_used(code_hash):
                logger.warning(
                    "MFA code reuse attempt",
                    extra={
                        "user_id": user_id,
                        "event_type": "mfa_code_reuse"
                    }
                )
                return False, "Code already used"

            # Mark code as used
            await self._mark_code_used(code_hash)

        # Clear rate limit on success
        if user_id and self.redis:
            await self._clear_rate_limit(user_id)

        logger.info(
            "MFA code verified",
            extra={
                "user_id": user_id,
                "event_type": "mfa_code_verified"
            }
        )

        return True, None

    def generate_backup_codes(self) -> List[str]:
        """
        Generate backup codes for account recovery.

        Returns:
            List of backup codes
        """
        codes = []
        for _ in range(self.config.BACKUP_CODE_COUNT):
            # Generate random code
            code = secrets.token_hex(self.config.BACKUP_CODE_LENGTH // 2)
            # Format as XXXX-XXXX
            formatted = f"{code[:4]}-{code[4:]}"
            codes.append(formatted)

        logger.info(
            f"Generated {len(codes)} backup codes",
            extra={"event_type": "mfa_backup_codes_generated"}
        )

        return codes

    def verify_backup_code(
        self,
        provided_code: str,
        stored_codes: List[str]
    ) -> Tuple[bool, List[str]]:
        """
        Verify and consume a backup code.

        Args:
            provided_code: Code provided by user
            stored_codes: List of valid backup codes

        Returns:
            Tuple of (is_valid, remaining_codes)
        """
        # Normalize code
        normalized = provided_code.replace("-", "").lower()

        for stored in stored_codes:
            stored_normalized = stored.replace("-", "").lower()
            if secrets.compare_digest(normalized, stored_normalized):
                # Remove used code
                remaining = [c for c in stored_codes if c != stored]

                logger.info(
                    "Backup code used",
                    extra={
                        "remaining_codes": len(remaining),
                        "event_type": "mfa_backup_code_used"
                    }
                )

                return True, remaining

        logger.warning(
            "Invalid backup code",
            extra={"event_type": "mfa_backup_code_invalid"}
        )

        return False, stored_codes

    def get_current_code(self, secret: str) -> str:
        """
        Get current TOTP code (for testing/debugging only).

        Args:
            secret: TOTP secret

        Returns:
            Current 6-digit code
        """
        totp = pyotp.TOTP(secret, digits=self.config.TOTP_DIGITS)
        return totp.now()

    # Rate limiting helpers

    def _rate_limit_key(self, user_id: str) -> str:
        """Generate rate limit key for user."""
        return f"{self.config.RATE_LIMIT_KEY_PREFIX}:{user_id}"

    async def _check_rate_limit(self, user_id: str) -> Tuple[bool, int]:
        """
        Check if user is rate limited.

        Returns:
            Tuple of (is_limited, remaining_seconds)
        """
        if not self.redis:
            return False, 0

        key = self._rate_limit_key(user_id)
        attempts = await self.redis.get(key)

        if attempts and int(attempts) >= self.config.RATE_LIMIT_MAX_ATTEMPTS:
            ttl = await self.redis.ttl(key)
            return True, max(0, ttl)

        return False, 0

    async def _record_failed_attempt(self, user_id: str):
        """Record a failed verification attempt."""
        if not self.redis:
            return

        key = self._rate_limit_key(user_id)
        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, self.config.RATE_LIMIT_WINDOW_SECONDS)
        await pipe.execute()

    async def _clear_rate_limit(self, user_id: str):
        """Clear rate limit on successful verification."""
        if not self.redis:
            return

        key = self._rate_limit_key(user_id)
        await self.redis.delete(key)

    # Code reuse prevention

    def _hash_code(self, secret: str, code: str) -> str:
        """Hash secret+code for reuse detection."""
        combined = f"{secret}:{code}:{int(time.time()) // self.config.TOTP_INTERVAL}"
        return hashlib.sha256(combined.encode()).hexdigest()

    async def _is_code_used(self, code_hash: str) -> bool:
        """Check if code was already used."""
        if not self.redis:
            return False

        key = f"{self.config.USED_CODE_PREFIX}:{code_hash}"
        return await self.redis.exists(key) > 0

    async def _mark_code_used(self, code_hash: str):
        """Mark code as used."""
        if not self.redis:
            return

        key = f"{self.config.USED_CODE_PREFIX}:{code_hash}"
        await self.redis.setex(key, self.config.USED_CODE_TTL_SECONDS, "1")


# Singleton helper
_mfa_service: Optional[MFAService] = None


async def get_mfa_service(
    redis_url: Optional[str] = None
) -> MFAService:
    """
    Get or create MFA service singleton.

    Usage in FastAPI:
        @app.post("/mfa/verify")
        async def verify_mfa(
            mfa: MFAService = Depends(get_mfa_service)
        ):
            is_valid, error = await mfa.verify_code(secret, code)
            ...
    """
    global _mfa_service

    if _mfa_service is None:
        redis_client = None
        if redis_url:
            redis_client = redis.from_url(redis_url, decode_responses=True)
        else:
            try:
                from assemblyline_common.config import settings
                redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
            except Exception:
                logger.warning(
                    "Redis not available, MFA rate limiting disabled",
                    extra={"event_type": "mfa_redis_unavailable"}
                )

        _mfa_service = MFAService(redis_client=redis_client)

    return _mfa_service

"""
AES-256-GCM Encryption for Logic Weaver.

Provides encryption for sensitive data including:
- MFA secrets and backup codes
- SAML certificates and private keys
- OAuth tokens
- Connector credentials

HIPAA Requirement: Encryption of PHI at rest.

Usage:
    from assemblyline_common.crypto import encrypt, decrypt

    # Encrypt sensitive data
    encrypted = encrypt("my-secret-mfa-key")

    # Decrypt when needed
    plaintext = decrypt(encrypted)
"""

import base64
import logging
import os
import secrets
from typing import Optional, Union

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)


class EncryptionConfig:
    """Configuration for encryption service."""

    # AES key size in bytes (256 bits)
    KEY_SIZE_BYTES: int = 32

    # Nonce size for AES-GCM (96 bits recommended by NIST)
    NONCE_SIZE_BYTES: int = 12

    # Prefix for encrypted data (to identify encrypted strings)
    ENCRYPTED_PREFIX: str = "enc:v1:"

    # Environment variable for encryption key
    KEY_ENV_VAR: str = "ENCRYPTION_KEY"


class EncryptionService:
    """
    AES-256-GCM encryption service.

    Uses authenticated encryption to provide confidentiality and integrity.
    The nonce is prepended to the ciphertext and both are base64-encoded.

    Format: enc:v1:<base64(nonce + ciphertext + tag)>

    HIPAA Technical Safeguard: 164.312(a)(2)(iv) Encryption and decryption
    """

    def __init__(self, key: Optional[bytes] = None, config: Optional[EncryptionConfig] = None):
        """
        Initialize encryption service.

        Args:
            key: 32-byte encryption key. If not provided, loads from environment.
            config: Optional configuration override.
        """
        self.config = config or EncryptionConfig()

        if key is None:
            key = self._load_key_from_env()

        if len(key) != self.config.KEY_SIZE_BYTES:
            raise ValueError(f"Encryption key must be {self.config.KEY_SIZE_BYTES} bytes")

        self._aesgcm = AESGCM(key)
        self._key = key

    def _load_key_from_env(self) -> bytes:
        """Load encryption key from environment variable or AWS Secrets Manager."""
        key_str = os.environ.get(self.config.KEY_ENV_VAR)

        if not key_str:
            # Try AWS Secrets Manager before falling back to ephemeral key
            sm_key = self._fetch_key_from_secrets_manager()
            if sm_key:
                logger.info(
                    "Encryption key loaded from AWS Secrets Manager",
                    extra={"event_type": "encryption_key_from_sm"}
                )
                return sm_key

            logger.warning(
                f"No encryption key found in {self.config.KEY_ENV_VAR} or Secrets Manager, "
                "generating ephemeral key",
                extra={"event_type": "encryption_key_generated"}
            )
            # Generate a random key for development (data won't persist across restarts)
            return secrets.token_bytes(self.config.KEY_SIZE_BYTES)

        # Key can be hex-encoded or base64-encoded
        try:
            # Try hex first
            if len(key_str) == self.config.KEY_SIZE_BYTES * 2:
                return bytes.fromhex(key_str)
            # Try base64
            decoded = base64.b64decode(key_str)
            if len(decoded) == self.config.KEY_SIZE_BYTES:
                return decoded
        except (ValueError, Exception):
            pass

        raise ValueError(
            f"Invalid encryption key in {self.config.KEY_ENV_VAR}. "
            f"Must be {self.config.KEY_SIZE_BYTES} bytes (hex or base64 encoded)."
        )

    def _fetch_key_from_secrets_manager(self) -> Optional[bytes]:
        """Fetch encryption key from AWS Secrets Manager."""
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError

            prefix = os.environ.get("AWS_SECRET_PREFIX", "yamil")
            region = os.environ.get("AWS_REGION", "us-east-1")
            secret_name = f"{prefix}/encryption-key"

            client = boto3.client('secretsmanager', region_name=region)
            response = client.get_secret_value(SecretId=secret_name)
            key_str = response['SecretString']

            # Support hex or base64 encoded keys
            if len(key_str) == self.config.KEY_SIZE_BYTES * 2:
                return bytes.fromhex(key_str)
            decoded = base64.b64decode(key_str)
            if len(decoded) == self.config.KEY_SIZE_BYTES:
                return decoded

            logger.warning("Encryption key from Secrets Manager has invalid size")
            return None
        except ImportError:
            return None
        except Exception as e:
            logger.debug(f"Secrets Manager not available for encryption key: {e}")
            return None

    def encrypt(self, plaintext: Union[str, bytes], associated_data: Optional[bytes] = None) -> str:
        """
        Encrypt plaintext using AES-256-GCM.

        Args:
            plaintext: Data to encrypt (string or bytes)
            associated_data: Optional authenticated but not encrypted data

        Returns:
            Encrypted string in format: enc:v1:<base64(nonce + ciphertext)>
        """
        if isinstance(plaintext, str):
            plaintext = plaintext.encode("utf-8")

        # Generate random nonce
        nonce = secrets.token_bytes(self.config.NONCE_SIZE_BYTES)

        # Encrypt with authentication
        ciphertext = self._aesgcm.encrypt(nonce, plaintext, associated_data)

        # Combine nonce + ciphertext and base64 encode
        combined = nonce + ciphertext
        encoded = base64.b64encode(combined).decode("ascii")

        return f"{self.config.ENCRYPTED_PREFIX}{encoded}"

    def decrypt(self, encrypted: str, associated_data: Optional[bytes] = None) -> str:
        """
        Decrypt ciphertext using AES-256-GCM.

        Args:
            encrypted: Encrypted string from encrypt()
            associated_data: Optional authenticated data (must match encryption)

        Returns:
            Decrypted plaintext as string

        Raises:
            ValueError: If data is not encrypted or invalid format
            cryptography.exceptions.InvalidTag: If authentication fails
        """
        if not self.is_encrypted(encrypted):
            raise ValueError("Data is not encrypted or has invalid format")

        # Remove prefix and decode
        encoded = encrypted[len(self.config.ENCRYPTED_PREFIX):]
        combined = base64.b64decode(encoded)

        # Split nonce and ciphertext
        nonce = combined[:self.config.NONCE_SIZE_BYTES]
        ciphertext = combined[self.config.NONCE_SIZE_BYTES:]

        # Decrypt and verify authentication
        plaintext = self._aesgcm.decrypt(nonce, ciphertext, associated_data)

        return plaintext.decode("utf-8")

    def is_encrypted(self, value: str) -> bool:
        """Check if a string is encrypted (has our prefix)."""
        return value.startswith(self.config.ENCRYPTED_PREFIX)

    def encrypt_if_needed(self, value: str, associated_data: Optional[bytes] = None) -> str:
        """Encrypt value only if not already encrypted."""
        if self.is_encrypted(value):
            return value
        return self.encrypt(value, associated_data)

    def decrypt_if_encrypted(self, value: str, associated_data: Optional[bytes] = None) -> str:
        """Decrypt value only if encrypted."""
        if not self.is_encrypted(value):
            return value
        return self.decrypt(value, associated_data)

    @staticmethod
    def generate_key() -> str:
        """Generate a new random encryption key (hex-encoded)."""
        key = secrets.token_bytes(EncryptionConfig.KEY_SIZE_BYTES)
        return key.hex()


# Singleton instance
_encryption_service: Optional[EncryptionService] = None


def get_encryption_service(key: Optional[bytes] = None) -> EncryptionService:
    """
    Get or create encryption service singleton.

    Usage in FastAPI:
        @app.post("/mfa/enroll")
        async def enroll_mfa(
            encryption: EncryptionService = Depends(get_encryption_service)
        ):
            encrypted_secret = encryption.encrypt(mfa_secret)
            ...
    """
    global _encryption_service

    if _encryption_service is None:
        _encryption_service = EncryptionService(key)

    return _encryption_service


def encrypt(plaintext: Union[str, bytes], associated_data: Optional[bytes] = None) -> str:
    """Convenience function to encrypt using the singleton service."""
    return get_encryption_service().encrypt(plaintext, associated_data)


def decrypt(encrypted: str, associated_data: Optional[bytes] = None) -> str:
    """Convenience function to decrypt using the singleton service."""
    return get_encryption_service().decrypt(encrypted, associated_data)

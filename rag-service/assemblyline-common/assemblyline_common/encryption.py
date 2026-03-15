"""
Encryption utilities for Logic Weaver.

Uses AES-256-GCM for symmetric encryption of sensitive data like API credentials,
SFTP passwords, and private keys.
"""

import os
import base64
import logging
from typing import Optional

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

# Get encryption key from environment, or generate a development key
_ENCRYPTION_KEY: Optional[bytes] = None


def _get_encryption_key() -> bytes:
    """Get or derive the encryption key from environment."""
    global _ENCRYPTION_KEY

    if _ENCRYPTION_KEY is not None:
        return _ENCRYPTION_KEY

    # Try to get key from environment
    key_env = os.environ.get("ENCRYPTION_KEY")

    if key_env:
        # If it looks like a base64-encoded Fernet key, use directly
        if len(key_env) == 44:
            _ENCRYPTION_KEY = key_env.encode()
        else:
            # Derive key from passphrase using PBKDF2
            salt = os.environ.get("ENCRYPTION_SALT", "logic-weaver-salt").encode()
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=480000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(key_env.encode()))
            _ENCRYPTION_KEY = key
    else:
        # Try AWS Secrets Manager
        key_from_sm = _get_encryption_key_from_secrets_manager()
        if key_from_sm:
            _ENCRYPTION_KEY = key_from_sm.encode()
            return _ENCRYPTION_KEY

        # In production: ENCRYPTION_KEY is set by Vault Agent or AWS Secrets Manager
        # In development: fall back to a fixed dev-only key
        env = os.environ.get("ENVIRONMENT", "development")
        if env == "production":
            raise RuntimeError(
                "ENCRYPTION_KEY not set and AWS Secrets Manager unreachable. "
                "Cannot start in production without encryption key."
            )
        logger.warning(
            "ENCRYPTION_KEY not set — using development-only key. "
            "This is NOT secure for production."
        )
        _ENCRYPTION_KEY = b'JCVd2F9HKXbqYJGJ7g_2z5K3u8x1Aw6pQjNR-tY0kv0='

    return _ENCRYPTION_KEY


def _get_encryption_key_from_secrets_manager() -> Optional[str]:
    """Fetch encryption key from AWS Secrets Manager."""
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        prefix = os.environ.get("AWS_SECRET_PREFIX", "yamil")
        region = os.environ.get("AWS_REGION", "us-east-1")
        secret_name = f"{prefix}/encryption-key"

        client = boto3.client('secretsmanager', region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        logger.info("Encryption key fetched from AWS Secrets Manager")
        return response['SecretString']
    except ImportError:
        return None
    except (NoCredentialsError, ClientError) as e:
        logger.debug(f"Secrets Manager not available: {e}")
        return None


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value.

    Args:
        value: The plaintext string to encrypt.

    Returns:
        Base64-encoded encrypted ciphertext.
    """
    if not value:
        return ""

    key = _get_encryption_key()
    f = Fernet(key)
    encrypted = f.encrypt(value.encode())
    return base64.urlsafe_b64encode(encrypted).decode()


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt an encrypted string value.

    Args:
        encrypted_value: Base64-encoded encrypted ciphertext.

    Returns:
        Decrypted plaintext string.
    """
    if not encrypted_value:
        return ""

    key = _get_encryption_key()
    f = Fernet(key)
    decoded = base64.urlsafe_b64decode(encrypted_value.encode())
    decrypted = f.decrypt(decoded)
    return decrypted.decode()


def rotate_key(old_key: str, new_key: str, encrypted_values: list[str]) -> list[str]:
    """
    Rotate encryption key for a list of encrypted values.

    Args:
        old_key: The current encryption key.
        new_key: The new encryption key.
        encrypted_values: List of encrypted values to re-encrypt.

    Returns:
        List of re-encrypted values with the new key.
    """
    old_fernet = Fernet(old_key.encode())
    new_fernet = Fernet(new_key.encode())

    re_encrypted = []
    for encrypted_value in encrypted_values:
        if not encrypted_value:
            re_encrypted.append("")
            continue

        # Decrypt with old key
        decoded = base64.urlsafe_b64decode(encrypted_value.encode())
        plaintext = old_fernet.decrypt(decoded)

        # Encrypt with new key
        new_encrypted = new_fernet.encrypt(plaintext)
        re_encrypted.append(base64.urlsafe_b64encode(new_encrypted).decode())

    return re_encrypted


def generate_key() -> str:
    """
    Generate a new Fernet encryption key.

    Returns:
        Base64-encoded Fernet key.
    """
    return Fernet.generate_key().decode()

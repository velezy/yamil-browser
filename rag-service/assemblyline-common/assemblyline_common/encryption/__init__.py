"""
Encryption module for Logic Weaver.

Provides field-level encryption for connector credentials
with key rotation support.
"""

import base64
import logging
import os
from typing import Optional

from cryptography.fernet import Fernet

from assemblyline_common.encryption.credentials import (
    CredentialEncryption,
    EncryptedCredential,
    KeyRotationManager,
    get_credential_encryption,
)

logger = logging.getLogger(__name__)

# Simple encryption key - in production, use proper key management
_SIMPLE_KEY: bytes | None = None


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
    except Exception as e:
        logger.debug(f"Secrets Manager not available: {e}")
        return None


def _get_simple_key() -> bytes:
    """Get or generate a simple encryption key for basic value encryption."""
    global _SIMPLE_KEY
    if _SIMPLE_KEY is not None:
        return _SIMPLE_KEY

    # 1. Try environment variable
    key_env = os.environ.get("SIMPLE_ENCRYPTION_KEY")
    if key_env:
        _SIMPLE_KEY = key_env.encode('utf-8')
        return _SIMPLE_KEY

    # 2. Try AWS Secrets Manager
    key_from_sm = _get_encryption_key_from_secrets_manager()
    if key_from_sm:
        _SIMPLE_KEY = key_from_sm.encode('utf-8')
        return _SIMPLE_KEY

    # 3. Fall back to machine-generated key (dev only)
    logger.warning(
        "SIMPLE_ENCRYPTION_KEY not set. Using dev fallback key. "
        "DO NOT USE IN PRODUCTION!"
    )
    import hashlib
    seed = f"logic-weaver-simple-key-{os.environ.get('USER', 'default')}"
    _SIMPLE_KEY = base64.urlsafe_b64encode(
        hashlib.sha256(seed.encode()).digest()
    )
    return _SIMPLE_KEY


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value using Fernet (AES-128-CBC).

    This is a simple synchronous encryption for basic use cases.
    For credential management with key rotation, use CredentialEncryption.

    Args:
        value: The string to encrypt

    Returns:
        Base64-encoded encrypted string
    """
    if not value:
        return ""
    key = _get_simple_key()
    f = Fernet(key)
    encrypted = f.encrypt(value.encode('utf-8'))
    return base64.urlsafe_b64encode(encrypted).decode('utf-8')


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt a string value encrypted with encrypt_value.

    Args:
        encrypted_value: The base64-encoded encrypted string

    Returns:
        Decrypted string
    """
    if not encrypted_value:
        return ""
    key = _get_simple_key()
    f = Fernet(key)
    decoded = base64.urlsafe_b64decode(encrypted_value.encode('utf-8'))
    decrypted = f.decrypt(decoded)
    return decrypted.decode('utf-8')


__all__ = [
    "CredentialEncryption",
    "EncryptedCredential",
    "KeyRotationManager",
    "get_credential_encryption",
    "encrypt_value",
    "decrypt_value",
]

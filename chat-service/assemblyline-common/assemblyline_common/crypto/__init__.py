"""
Cryptographic utilities for Logic Weaver.

Provides AES-256-GCM encryption for sensitive data like MFA secrets and SAML certificates.
"""

from assemblyline_common.crypto.encryption import (
    EncryptionService,
    EncryptionConfig,
    get_encryption_service,
    encrypt,
    decrypt,
)

__all__ = [
    "EncryptionService",
    "EncryptionConfig",
    "get_encryption_service",
    "encrypt",
    "decrypt",
]

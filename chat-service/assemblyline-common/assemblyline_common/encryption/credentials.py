"""
Field-level encryption for connector credentials.

Enterprise Features:
- AES-256-GCM encryption for credentials
- Key rotation support with version tracking
- Multiple key support for gradual rotation
- HSM integration hooks (optional)
- Tenant-isolated encryption keys

Usage:
    from assemblyline_common.encryption import get_credential_encryption

    cred_enc = await get_credential_encryption()

    # Encrypt a credential
    encrypted = await cred_enc.encrypt_credential(
        tenant_id="tenant-123",
        credential_type="api_key",
        credential_value="secret-value",
    )

    # Decrypt a credential
    value = await cred_enc.decrypt_credential(encrypted)

    # Rotate keys
    await cred_enc.rotate_key(tenant_id="tenant-123")
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from assemblyline_common.config import settings
from assemblyline_common.secrets import get_secrets_manager

logger = logging.getLogger(__name__)

# Constants
KEY_SIZE = 32  # 256 bits
NONCE_SIZE = 12  # 96 bits for GCM
CURRENT_VERSION = "v1"


@dataclass
class EncryptedCredential:
    """Encrypted credential with metadata."""
    # Encrypted value (base64 encoded)
    encrypted_value: str
    # Encryption version
    version: str = CURRENT_VERSION
    # Key ID used for encryption
    key_id: str = ""
    # Tenant ID
    tenant_id: str = ""
    # Credential type (e.g., api_key, password, certificate)
    credential_type: str = ""
    # Timestamp when encrypted
    encrypted_at: float = field(default_factory=time.time)
    # Additional authenticated data (not encrypted, but authenticated)
    aad: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "encrypted_value": self.encrypted_value,
            "version": self.version,
            "key_id": self.key_id,
            "tenant_id": self.tenant_id,
            "credential_type": self.credential_type,
            "encrypted_at": self.encrypted_at,
            "aad": self.aad,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EncryptedCredential":
        """Create from dictionary."""
        return cls(
            encrypted_value=data["encrypted_value"],
            version=data.get("version", CURRENT_VERSION),
            key_id=data.get("key_id", ""),
            tenant_id=data.get("tenant_id", ""),
            credential_type=data.get("credential_type", ""),
            encrypted_at=data.get("encrypted_at", time.time()),
            aad=data.get("aad"),
        )

    def to_json(self) -> str:
        """Serialize to JSON string for database storage."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "EncryptedCredential":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class EncryptionKey:
    """Encryption key with metadata."""
    key_id: str
    key_bytes: bytes
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    is_active: bool = True
    rotation_count: int = 0

    def is_expired(self) -> bool:
        """Check if key has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


class KeyRotationManager:
    """
    Manages encryption key rotation.

    Supports:
    - Multiple active keys for gradual rotation
    - Key versioning
    - Automatic key expiration
    - Key derivation from master key
    """

    def __init__(
        self,
        master_key: Optional[bytes] = None,
        key_rotation_days: int = 90,
    ):
        self._master_key = master_key
        self._key_rotation_days = key_rotation_days
        self._keys: Dict[str, EncryptionKey] = {}
        self._current_key_ids: Dict[str, str] = {}  # tenant_id -> key_id
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize with master key from secrets manager."""
        if self._master_key is None:
            secrets = await get_secrets_manager()
            master_key_b64 = await secrets.get_secret("encryption/master-key")

            if master_key_b64:
                self._master_key = base64.b64decode(master_key_b64)
            else:
                # Fallback to config
                if settings.ENCRYPTION_KEY:
                    self._master_key = base64.b64decode(settings.ENCRYPTION_KEY)
                else:
                    # Generate ephemeral key (development only)
                    logger.warning(
                        "No encryption master key configured, using ephemeral key. "
                        "Set ENCRYPTION_KEY in production!"
                    )
                    self._master_key = os.urandom(KEY_SIZE)

    def _derive_tenant_key(self, tenant_id: str, key_version: int = 0) -> bytes:
        """Derive a tenant-specific key from master key."""
        if not self._master_key:
            raise ValueError("Master key not initialized")

        # Use HKDF-like derivation
        info = f"credential-key:{tenant_id}:v{key_version}".encode()
        derived = hashlib.pbkdf2_hmac(
            'sha256',
            self._master_key,
            info,
            iterations=100000,
            dklen=KEY_SIZE
        )
        return derived

    async def get_current_key(self, tenant_id: str) -> EncryptionKey:
        """Get the current encryption key for a tenant."""
        async with self._lock:
            # Check if we have a current key
            if tenant_id in self._current_key_ids:
                key_id = self._current_key_ids[tenant_id]
                if key_id in self._keys:
                    key = self._keys[key_id]
                    if key.is_active and not key.is_expired():
                        return key

            # Create new key
            return await self._create_key(tenant_id)

    async def _create_key(self, tenant_id: str) -> EncryptionKey:
        """Create a new encryption key for a tenant."""
        # Get current version count
        version = 0
        for key in self._keys.values():
            if key.key_id.startswith(f"{tenant_id}:"):
                version = max(version, key.rotation_count + 1)

        key_bytes = self._derive_tenant_key(tenant_id, version)
        key_id = f"{tenant_id}:v{version}:{int(time.time())}"

        expires_at = None
        if self._key_rotation_days > 0:
            expires_at = time.time() + (self._key_rotation_days * 86400)

        key = EncryptionKey(
            key_id=key_id,
            key_bytes=key_bytes,
            expires_at=expires_at,
            rotation_count=version,
        )

        self._keys[key_id] = key
        self._current_key_ids[tenant_id] = key_id

        logger.info(
            f"Created new encryption key",
            extra={
                "event_type": "encryption_key_created",
                "tenant_id": tenant_id,
                "key_id": key_id,
                "version": version,
            }
        )

        return key

    async def get_key(self, key_id: str) -> Optional[EncryptionKey]:
        """Get a specific key by ID."""
        if key_id in self._keys:
            return self._keys[key_id]

        # Try to reconstruct key from ID
        # Format: tenant_id:v{version}:timestamp
        parts = key_id.split(":")
        if len(parts) >= 2:
            tenant_id = parts[0]
            version_str = parts[1]
            if version_str.startswith("v"):
                version = int(version_str[1:])
                key_bytes = self._derive_tenant_key(tenant_id, version)

                key = EncryptionKey(
                    key_id=key_id,
                    key_bytes=key_bytes,
                    is_active=False,  # Old key, not active
                )
                self._keys[key_id] = key
                return key

        return None

    async def rotate_key(self, tenant_id: str) -> EncryptionKey:
        """Rotate the encryption key for a tenant."""
        async with self._lock:
            # Mark current key as inactive
            if tenant_id in self._current_key_ids:
                old_key_id = self._current_key_ids[tenant_id]
                if old_key_id in self._keys:
                    self._keys[old_key_id].is_active = False

            # Create new key
            new_key = await self._create_key(tenant_id)

            logger.info(
                f"Rotated encryption key",
                extra={
                    "event_type": "encryption_key_rotated",
                    "tenant_id": tenant_id,
                    "new_key_id": new_key.key_id,
                }
            )

            return new_key

    def get_active_keys(self, tenant_id: str) -> List[EncryptionKey]:
        """Get all active (non-expired) keys for a tenant."""
        return [
            key for key in self._keys.values()
            if key.key_id.startswith(f"{tenant_id}:")
            and not key.is_expired()
        ]


class CredentialEncryption:
    """
    Field-level encryption service for connector credentials.

    Features:
    - AES-256-GCM encryption
    - Key rotation with version tracking
    - Tenant isolation
    - Additional authenticated data (AAD) support
    """

    def __init__(self, key_manager: Optional[KeyRotationManager] = None):
        self.key_manager = key_manager or KeyRotationManager()
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize the encryption service."""
        if self._initialized:
            return

        await self.key_manager.initialize()
        self._initialized = True

        logger.info("Credential encryption service initialized")

    async def encrypt_credential(
        self,
        tenant_id: str,
        credential_type: str,
        credential_value: str,
        aad: Optional[Dict[str, str]] = None,
    ) -> EncryptedCredential:
        """
        Encrypt a credential value.

        Args:
            tenant_id: Tenant identifier
            credential_type: Type of credential (api_key, password, certificate, etc.)
            credential_value: The secret value to encrypt
            aad: Additional authenticated data (optional)

        Returns:
            EncryptedCredential object
        """
        if not self._initialized:
            await self.initialize()

        # Get current key
        key = await self.key_manager.get_current_key(tenant_id)

        # Create AESGCM cipher
        aesgcm = AESGCM(key.key_bytes)

        # Generate random nonce
        nonce = os.urandom(NONCE_SIZE)

        # Prepare AAD
        aad_bytes = None
        if aad:
            aad_bytes = json.dumps(aad, sort_keys=True).encode()

        # Encrypt
        plaintext = credential_value.encode('utf-8')
        ciphertext = aesgcm.encrypt(nonce, plaintext, aad_bytes)

        # Combine nonce + ciphertext and encode
        encrypted_bytes = nonce + ciphertext
        encrypted_b64 = base64.b64encode(encrypted_bytes).decode('utf-8')

        # Create encrypted credential object
        encrypted = EncryptedCredential(
            encrypted_value=encrypted_b64,
            version=CURRENT_VERSION,
            key_id=key.key_id,
            tenant_id=tenant_id,
            credential_type=credential_type,
            encrypted_at=time.time(),
            aad=aad,
        )

        logger.debug(
            f"Encrypted credential",
            extra={
                "event_type": "credential_encrypted",
                "tenant_id": tenant_id,
                "credential_type": credential_type,
                "key_id": key.key_id,
            }
        )

        return encrypted

    async def decrypt_credential(
        self,
        encrypted: EncryptedCredential,
    ) -> str:
        """
        Decrypt an encrypted credential.

        Args:
            encrypted: EncryptedCredential object

        Returns:
            Decrypted credential value

        Raises:
            ValueError: If decryption fails
        """
        if not self._initialized:
            await self.initialize()

        # Get the key used for encryption
        key = await self.key_manager.get_key(encrypted.key_id)
        if not key:
            raise ValueError(f"Encryption key not found: {encrypted.key_id}")

        # Decode
        encrypted_bytes = base64.b64decode(encrypted.encrypted_value)

        # Extract nonce and ciphertext
        nonce = encrypted_bytes[:NONCE_SIZE]
        ciphertext = encrypted_bytes[NONCE_SIZE:]

        # Create cipher
        aesgcm = AESGCM(key.key_bytes)

        # Prepare AAD
        aad_bytes = None
        if encrypted.aad:
            aad_bytes = json.dumps(encrypted.aad, sort_keys=True).encode()

        # Decrypt
        try:
            plaintext = aesgcm.decrypt(nonce, ciphertext, aad_bytes)
            return plaintext.decode('utf-8')
        except Exception as e:
            logger.error(
                f"Credential decryption failed",
                extra={
                    "event_type": "credential_decrypt_failed",
                    "tenant_id": encrypted.tenant_id,
                    "key_id": encrypted.key_id,
                    "error": str(e),
                }
            )
            raise ValueError(f"Failed to decrypt credential: {e}")

    async def rotate_key(self, tenant_id: str) -> str:
        """
        Rotate the encryption key for a tenant.

        Returns the new key ID.
        """
        if not self._initialized:
            await self.initialize()

        new_key = await self.key_manager.rotate_key(tenant_id)
        return new_key.key_id

    async def re_encrypt_credential(
        self,
        encrypted: EncryptedCredential,
    ) -> EncryptedCredential:
        """
        Re-encrypt a credential with the current key.

        Useful for key rotation - decrypt with old key, encrypt with new.
        """
        # Decrypt with current key
        value = await self.decrypt_credential(encrypted)

        # Re-encrypt with new current key
        return await self.encrypt_credential(
            tenant_id=encrypted.tenant_id,
            credential_type=encrypted.credential_type,
            credential_value=value,
            aad=encrypted.aad,
        )

    async def encrypt_dict(
        self,
        tenant_id: str,
        credentials: Dict[str, str],
        credential_type: str = "config",
    ) -> Dict[str, EncryptedCredential]:
        """
        Encrypt multiple credentials in a dictionary.

        Useful for encrypting connector configuration with multiple secrets.
        """
        result = {}
        for key, value in credentials.items():
            result[key] = await self.encrypt_credential(
                tenant_id=tenant_id,
                credential_type=f"{credential_type}:{key}",
                credential_value=value,
            )
        return result

    async def decrypt_dict(
        self,
        encrypted_credentials: Dict[str, EncryptedCredential],
    ) -> Dict[str, str]:
        """
        Decrypt multiple credentials from a dictionary.
        """
        result = {}
        for key, encrypted in encrypted_credentials.items():
            result[key] = await self.decrypt_credential(encrypted)
        return result


# Singleton instance
_credential_encryption: Optional[CredentialEncryption] = None
_encryption_lock = asyncio.Lock()


async def get_credential_encryption() -> CredentialEncryption:
    """Get singleton credential encryption service."""
    global _credential_encryption

    if _credential_encryption is None:
        async with _encryption_lock:
            if _credential_encryption is None:
                _credential_encryption = CredentialEncryption()
                await _credential_encryption.initialize()

    return _credential_encryption

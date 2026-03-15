"""
Master Credentials Manager - Encrypted Database Credential Storage

This module provides secure storage and retrieval of database credentials,
encrypted with a master password that the user enters at runtime.

Features:
- AES-256-GCM encryption for credentials
- PBKDF2 key derivation from master password
- Secure local file storage (not in .env)
- Runtime decryption with master password

Usage:
    # First time setup (CLI)
    python -m assemblyline_common.credentials.setup_credentials

    # In application
    from assemblyline_common.credentials import get_database_url
    database_url = await get_database_url(master_password)
"""

import base64
import hashlib
import json
import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# Constants
KEY_SIZE = 32  # 256 bits
NONCE_SIZE = 12  # 96 bits for GCM
SALT_SIZE = 32  # 256 bits
PBKDF2_ITERATIONS = 100000

# Default credentials file location
DEFAULT_CREDENTIALS_FILE = Path.home() / ".logic-weaver" / "credentials.enc"


@dataclass
class EncryptedCredentials:
    """Encrypted credentials container."""
    # Encrypted data (base64)
    encrypted_data: str
    # Salt for key derivation (base64)
    salt: str
    # Version for future compatibility
    version: str = "v1"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EncryptedCredentials":
        return cls(
            encrypted_data=data["encrypted_data"],
            salt=data["salt"],
            version=data.get("version", "v1"),
        )


def derive_key(master_password: str, salt: bytes) -> bytes:
    """Derive an encryption key from the master password using PBKDF2."""
    return hashlib.pbkdf2_hmac(
        "sha256",
        master_password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
        dklen=KEY_SIZE,
    )


def encrypt_credentials(
    credentials: dict,
    master_password: str,
) -> EncryptedCredentials:
    """
    Encrypt credentials dictionary with master password.

    Args:
        credentials: Dictionary of credentials (e.g., {"database_url": "...", "database_password": "..."})
        master_password: Master password for encryption

    Returns:
        EncryptedCredentials object
    """
    # Generate random salt
    salt = os.urandom(SALT_SIZE)

    # Derive key from master password
    key = derive_key(master_password, salt)

    # Create AESGCM cipher
    aesgcm = AESGCM(key)

    # Generate random nonce
    nonce = os.urandom(NONCE_SIZE)

    # Encrypt credentials as JSON
    plaintext = json.dumps(credentials).encode("utf-8")
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)

    # Combine nonce + ciphertext
    encrypted_bytes = nonce + ciphertext
    encrypted_b64 = base64.b64encode(encrypted_bytes).decode("utf-8")

    return EncryptedCredentials(
        encrypted_data=encrypted_b64,
        salt=base64.b64encode(salt).decode("utf-8"),
    )


def decrypt_credentials(
    encrypted: EncryptedCredentials,
    master_password: str,
) -> dict:
    """
    Decrypt credentials with master password.

    Args:
        encrypted: EncryptedCredentials object
        master_password: Master password for decryption

    Returns:
        Dictionary of decrypted credentials

    Raises:
        ValueError: If decryption fails (wrong password or corrupted data)
    """
    # Decode salt
    salt = base64.b64decode(encrypted.salt)

    # Derive key from master password
    key = derive_key(master_password, salt)

    # Decode encrypted data
    encrypted_bytes = base64.b64decode(encrypted.encrypted_data)

    # Extract nonce and ciphertext
    nonce = encrypted_bytes[:NONCE_SIZE]
    ciphertext = encrypted_bytes[NONCE_SIZE:]

    # Create cipher and decrypt
    aesgcm = AESGCM(key)

    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to decrypt credentials. Check your master password. Error: {e}")


def save_credentials(
    encrypted: EncryptedCredentials,
    filepath: Optional[Path] = None,
) -> Path:
    """
    Save encrypted credentials to file.

    Args:
        encrypted: EncryptedCredentials object
        filepath: Optional custom filepath (defaults to ~/.logic-weaver/credentials.enc)

    Returns:
        Path where credentials were saved
    """
    filepath = filepath or DEFAULT_CREDENTIALS_FILE

    # Ensure directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Save as JSON
    with open(filepath, "w") as f:
        json.dump(encrypted.to_dict(), f, indent=2)

    # Set restrictive permissions (owner read/write only)
    os.chmod(filepath, 0o600)

    return filepath


def load_credentials(
    filepath: Optional[Path] = None,
) -> Optional[EncryptedCredentials]:
    """
    Load encrypted credentials from file.

    Args:
        filepath: Optional custom filepath (defaults to ~/.logic-weaver/credentials.enc)

    Returns:
        EncryptedCredentials object or None if file doesn't exist
    """
    filepath = filepath or DEFAULT_CREDENTIALS_FILE

    if not filepath.exists():
        return None

    with open(filepath, "r") as f:
        data = json.load(f)

    return EncryptedCredentials.from_dict(data)


def credentials_exist(filepath: Optional[Path] = None) -> bool:
    """Check if encrypted credentials file exists."""
    filepath = filepath or DEFAULT_CREDENTIALS_FILE
    return filepath.exists()


def get_database_url(
    master_password: str,
    filepath: Optional[Path] = None,
    environment: str = "local"
) -> str:
    """
    Get the decrypted database URL for the specified environment.

    Args:
        master_password: Master password for decryption
        filepath: Optional custom filepath
        environment: Database environment ('local' or 'aws')

    Returns:
        Decrypted database URL for the specified environment

    Raises:
        FileNotFoundError: If credentials file doesn't exist
        ValueError: If decryption fails or environment not configured
    """
    encrypted = load_credentials(filepath)
    if encrypted is None:
        raise FileNotFoundError(
            f"Credentials file not found. Run 'python -m assemblyline_common.credentials.setup' to set up credentials."
        )

    credentials = decrypt_credentials(encrypted, master_password)

    # Support both old format (single database_url) and new format (local_database_url, aws_database_url)
    if environment == "aws":
        url = credentials.get("aws_database_url", credentials.get("database_url", ""))
    else:
        url = credentials.get("local_database_url", credentials.get("database_url", ""))

    if not url:
        raise ValueError(f"No database URL configured for environment '{environment}'")

    return url


def get_all_credentials(master_password: str, filepath: Optional[Path] = None) -> dict:
    """
    Get all decrypted credentials.

    Args:
        master_password: Master password for decryption
        filepath: Optional custom filepath

    Returns:
        Dictionary of all decrypted credentials
    """
    encrypted = load_credentials(filepath)
    if encrypted is None:
        raise FileNotFoundError(
            f"Credentials file not found. Run 'python -m assemblyline_common.credentials.setup' to set up credentials."
        )

    return decrypt_credentials(encrypted, master_password)

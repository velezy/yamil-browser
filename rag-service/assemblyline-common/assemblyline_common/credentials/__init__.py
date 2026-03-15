"""
Credentials module for secure credential storage and retrieval.

This module provides encrypted storage for sensitive credentials like
database passwords, using a master password for encryption/decryption.
"""

from .master_credentials import (
    EncryptedCredentials,
    encrypt_credentials,
    decrypt_credentials,
    save_credentials,
    load_credentials,
    credentials_exist,
    get_database_url,
    get_all_credentials,
    DEFAULT_CREDENTIALS_FILE,
)

__all__ = [
    "EncryptedCredentials",
    "encrypt_credentials",
    "decrypt_credentials",
    "save_credentials",
    "load_credentials",
    "credentials_exist",
    "get_database_url",
    "get_all_credentials",
    "DEFAULT_CREDENTIALS_FILE",
]

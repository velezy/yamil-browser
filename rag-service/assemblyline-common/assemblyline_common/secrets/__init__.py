"""
Secrets management for Logic Weaver.

Provides a unified interface for accessing secrets from different backends:
- AWS Secrets Manager (production)
- Azure Key Vault (Azure environments)
- HashiCorp Vault (self-hosted/enterprise)
- Local environment variables (development)

HIPAA Requirement: Secure storage and access control for credentials.
"""

from assemblyline_common.secrets.manager import (
    SecretsBackend,
    LocalSecretsBackend,
    AWSSecretsManagerBackend,
    SecretsManager,
    get_secrets_manager,
)
from assemblyline_common.secrets.azure_keyvault import (
    AzureKeyVaultBackend,
    AzureKeyVaultConfig,
    AuthMethod as AzureAuthMethod,
    get_azure_keyvault_backend,
)
from assemblyline_common.secrets.hashicorp_vault import (
    HashiCorpVaultBackend,
    VaultConfig,
    AuthMethod as VaultAuthMethod,
    KVVersion,
    get_hashicorp_vault_backend,
)

__all__ = [
    # AWS
    "SecretsBackend",
    "LocalSecretsBackend",
    "AWSSecretsManagerBackend",
    "SecretsManager",
    "get_secrets_manager",
    # Azure Key Vault
    "AzureKeyVaultBackend",
    "AzureKeyVaultConfig",
    "AzureAuthMethod",
    "get_azure_keyvault_backend",
    # HashiCorp Vault
    "HashiCorpVaultBackend",
    "VaultConfig",
    "VaultAuthMethod",
    "KVVersion",
    "get_hashicorp_vault_backend",
]

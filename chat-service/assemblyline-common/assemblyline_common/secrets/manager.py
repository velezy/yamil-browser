"""
Secrets Manager for Logic Weaver.

Provides abstracted access to secrets with multiple backend support:
- AWS Secrets Manager for production deployments
- Local environment variables for development

Features:
- TTL-based caching to reduce API calls
- Graceful fallback to local secrets
- Structured logging for audit trails

HIPAA Requirement: Secure credential management with audit logging.

Usage:
    from assemblyline_common.secrets import get_secrets_manager

    secrets = await get_secrets_manager()

    # Get a single secret
    db_password = await secrets.get_secret("database/password")

    # Get JSON secret (parsed)
    db_config = await secrets.get_secret_json("database/config")
"""

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class SecretsConfig:
    """Configuration for secrets manager."""

    # Backend type: "local" or "aws"
    BACKEND_ENV_VAR: str = "SECRETS_BACKEND"
    DEFAULT_BACKEND: str = "local"

    # AWS configuration
    AWS_REGION_ENV_VAR: str = "AWS_REGION"
    AWS_SECRET_PREFIX_ENV_VAR: str = "AWS_SECRET_PREFIX"
    DEFAULT_AWS_REGION: str = "us-east-1"
    DEFAULT_AWS_SECRET_PREFIX: str = "yamil"

    # Cache TTL in seconds (5 minutes)
    CACHE_TTL_SECONDS: int = 300

    # Max retries for AWS calls
    MAX_RETRIES: int = 3


@dataclass
class CachedSecret:
    """Cached secret with expiration."""
    value: str
    expires_at: float


class SecretsBackend(ABC):
    """Abstract base class for secrets backends."""

    @abstractmethod
    async def get_secret(self, key: str) -> Optional[str]:
        """Get a secret value by key."""
        pass

    @abstractmethod
    async def set_secret(self, key: str, value: str) -> bool:
        """Set a secret value (if supported)."""
        pass

    @abstractmethod
    async def delete_secret(self, key: str) -> bool:
        """Delete a secret (if supported)."""
        pass

    @abstractmethod
    async def list_secrets(self, prefix: Optional[str] = None) -> list[str]:
        """List available secret keys."""
        pass


class LocalSecretsBackend(SecretsBackend):
    """
    Local environment-based secrets backend.

    Maps secret keys to environment variables:
    - "database/password" -> DATABASE_PASSWORD
    - "mfa/encryption_key" -> MFA_ENCRYPTION_KEY

    Useful for local development and testing.
    """

    def __init__(self, env_prefix: str = ""):
        """
        Initialize local secrets backend.

        Args:
            env_prefix: Optional prefix for environment variables
        """
        self.env_prefix = env_prefix

    def _key_to_env_var(self, key: str) -> str:
        """Convert secret key to environment variable name."""
        # Replace / and - with _
        env_var = key.replace("/", "_").replace("-", "_").upper()
        if self.env_prefix:
            env_var = f"{self.env_prefix}_{env_var}"
        return env_var

    async def get_secret(self, key: str) -> Optional[str]:
        """Get secret from environment variable."""
        env_var = self._key_to_env_var(key)
        value = os.environ.get(env_var)

        if value is None:
            logger.debug(
                f"Secret not found in environment",
                extra={"key": key, "env_var": env_var, "event_type": "secret_not_found"}
            )
        else:
            logger.debug(
                f"Secret retrieved from environment",
                extra={"key": key, "env_var": env_var, "event_type": "secret_retrieved"}
            )

        return value

    async def set_secret(self, key: str, value: str) -> bool:
        """Set secret as environment variable (in-memory only)."""
        env_var = self._key_to_env_var(key)
        os.environ[env_var] = value
        logger.info(
            f"Secret set in environment",
            extra={"key": key, "env_var": env_var, "event_type": "secret_set"}
        )
        return True

    async def delete_secret(self, key: str) -> bool:
        """Delete secret from environment."""
        env_var = self._key_to_env_var(key)
        if env_var in os.environ:
            del os.environ[env_var]
            logger.info(
                f"Secret deleted from environment",
                extra={"key": key, "env_var": env_var, "event_type": "secret_deleted"}
            )
            return True
        return False

    async def list_secrets(self, prefix: Optional[str] = None) -> list[str]:
        """List secrets that match the prefix pattern."""
        secrets = []
        prefix_upper = (prefix or "").replace("/", "_").replace("-", "_").upper()

        for env_var in os.environ:
            if self.env_prefix and not env_var.startswith(self.env_prefix):
                continue
            if prefix_upper and prefix_upper not in env_var:
                continue
            secrets.append(env_var)

        return secrets


class AWSSecretsManagerBackend(SecretsBackend):
    """
    AWS Secrets Manager backend.

    Uses boto3 to access secrets stored in AWS Secrets Manager.
    Supports JSON secrets and automatic secret rotation.

    Secret naming convention:
    - Prefix: {prefix}/{environment}/
    - Example: logic-weaver/production/database/password
    """

    def __init__(
        self,
        region: Optional[str] = None,
        secret_prefix: Optional[str] = None,
        config: Optional[SecretsConfig] = None
    ):
        """
        Initialize AWS Secrets Manager backend.

        Args:
            region: AWS region (defaults to AWS_REGION env var or us-east-1)
            secret_prefix: Prefix for all secret names
            config: Optional configuration override
        """
        self.config = config or SecretsConfig()
        self.region = region or os.environ.get(
            self.config.AWS_REGION_ENV_VAR,
            self.config.DEFAULT_AWS_REGION
        )
        self.secret_prefix = secret_prefix or os.environ.get(
            self.config.AWS_SECRET_PREFIX_ENV_VAR,
            self.config.DEFAULT_AWS_SECRET_PREFIX
        )
        self._client = None

    @property
    def client(self):
        """Lazy-load boto3 client."""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    "secretsmanager",
                    region_name=self.region
                )
            except ImportError:
                raise ImportError("boto3 is required for AWS Secrets Manager backend")
        return self._client

    def _full_secret_name(self, key: str) -> str:
        """Get full secret name with prefix."""
        return f"{self.secret_prefix}/{key}"

    async def get_secret(self, key: str) -> Optional[str]:
        """Get secret from AWS Secrets Manager."""
        secret_name = self._full_secret_name(key)

        try:
            # Run boto3 call in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.get_secret_value(SecretId=secret_name)
            )

            # Handle string or binary secrets
            if "SecretString" in response:
                value = response["SecretString"]
            else:
                import base64
                value = base64.b64decode(response["SecretBinary"]).decode("utf-8")

            logger.info(
                f"Secret retrieved from AWS",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "event_type": "aws_secret_retrieved"
                }
            )
            return value

        except self.client.exceptions.ResourceNotFoundException:
            logger.warning(
                f"Secret not found in AWS",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "event_type": "aws_secret_not_found"
                }
            )
            return None

        except Exception as e:
            logger.error(
                f"Failed to retrieve secret from AWS: {e}",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "error": str(e),
                    "event_type": "aws_secret_error"
                }
            )
            raise

    async def set_secret(self, key: str, value: str) -> bool:
        """Create or update secret in AWS Secrets Manager."""
        secret_name = self._full_secret_name(key)

        try:
            loop = asyncio.get_event_loop()

            # Try to update existing secret
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.client.put_secret_value(
                        SecretId=secret_name,
                        SecretString=value
                    )
                )
            except self.client.exceptions.ResourceNotFoundException:
                # Create new secret
                await loop.run_in_executor(
                    None,
                    lambda: self.client.create_secret(
                        Name=secret_name,
                        SecretString=value
                    )
                )

            logger.info(
                f"Secret stored in AWS",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "event_type": "aws_secret_stored"
                }
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to store secret in AWS: {e}",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "error": str(e),
                    "event_type": "aws_secret_store_error"
                }
            )
            return False

    async def delete_secret(self, key: str) -> bool:
        """Delete secret from AWS Secrets Manager."""
        secret_name = self._full_secret_name(key)

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.client.delete_secret(
                    SecretId=secret_name,
                    ForceDeleteWithoutRecovery=False  # Allow recovery for 30 days
                )
            )

            logger.info(
                f"Secret deleted from AWS",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "event_type": "aws_secret_deleted"
                }
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to delete secret from AWS: {e}",
                extra={
                    "key": key,
                    "secret_name": secret_name,
                    "error": str(e),
                    "event_type": "aws_secret_delete_error"
                }
            )
            return False

    async def list_secrets(self, prefix: Optional[str] = None) -> list[str]:
        """List secrets matching prefix."""
        full_prefix = self._full_secret_name(prefix or "")

        try:
            loop = asyncio.get_event_loop()
            secrets = []
            paginator = self.client.get_paginator("list_secrets")

            # Paginate through all secrets
            async def paginate():
                for page in paginator.paginate(
                    Filters=[{"Key": "name", "Values": [full_prefix]}]
                ):
                    for secret in page.get("SecretList", []):
                        secrets.append(secret["Name"])
                return secrets

            await loop.run_in_executor(None, lambda: list(paginate()))
            return secrets

        except Exception as e:
            logger.error(
                f"Failed to list secrets from AWS: {e}",
                extra={
                    "prefix": prefix,
                    "error": str(e),
                    "event_type": "aws_secret_list_error"
                }
            )
            return []


class SecretsManager:
    """
    High-level secrets manager with caching and fallback.

    Provides:
    - TTL-based caching to reduce backend calls
    - Automatic fallback from AWS to local backend
    - JSON secret parsing
    - Audit logging

    Usage:
        secrets = await get_secrets_manager()
        password = await secrets.get_secret("database/password")
    """

    def __init__(
        self,
        backend: SecretsBackend,
        fallback_backend: Optional[SecretsBackend] = None,
        config: Optional[SecretsConfig] = None
    ):
        """
        Initialize secrets manager.

        Args:
            backend: Primary secrets backend
            fallback_backend: Optional fallback backend if primary fails
            config: Optional configuration override
        """
        self.backend = backend
        self.fallback_backend = fallback_backend
        self.config = config or SecretsConfig()
        self._cache: Dict[str, CachedSecret] = {}
        self._cache_lock = asyncio.Lock()

    async def get_secret(self, key: str, use_cache: bool = True) -> Optional[str]:
        """
        Get a secret value.

        Args:
            key: Secret key/name
            use_cache: Whether to use cached value if available

        Returns:
            Secret value or None if not found
        """
        # Check cache first
        if use_cache:
            cached = await self._get_from_cache(key)
            if cached is not None:
                return cached

        # Try primary backend
        value = None
        try:
            value = await self.backend.get_secret(key)
        except Exception as e:
            logger.warning(
                f"Primary backend failed, trying fallback: {e}",
                extra={"key": key, "error": str(e), "event_type": "backend_fallback"}
            )

        # Try fallback if primary failed
        if value is None and self.fallback_backend is not None:
            try:
                value = await self.fallback_backend.get_secret(key)
            except Exception as e:
                logger.error(
                    f"Fallback backend also failed: {e}",
                    extra={"key": key, "error": str(e), "event_type": "fallback_failed"}
                )

        # Cache the result
        if value is not None and use_cache:
            await self._set_cache(key, value)

        return value

    async def get_secret_json(self, key: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get a JSON secret and parse it.

        Args:
            key: Secret key/name
            use_cache: Whether to use cached value

        Returns:
            Parsed JSON as dict, or None if not found
        """
        value = await self.get_secret(key, use_cache)
        if value is None:
            return None

        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to parse secret as JSON: {e}",
                extra={"key": key, "error": str(e), "event_type": "json_parse_error"}
            )
            return None

    async def set_secret(self, key: str, value: str) -> bool:
        """Set a secret value."""
        result = await self.backend.set_secret(key, value)
        if result:
            await self._set_cache(key, value)
        return result

    async def delete_secret(self, key: str) -> bool:
        """Delete a secret."""
        result = await self.backend.delete_secret(key)
        if result:
            await self._invalidate_cache(key)
        return result

    async def list_secrets(self, prefix: Optional[str] = None) -> list[str]:
        """List available secrets."""
        return await self.backend.list_secrets(prefix)

    async def clear_cache(self):
        """Clear the entire cache."""
        async with self._cache_lock:
            self._cache.clear()
            logger.info("Secret cache cleared", extra={"event_type": "cache_cleared"})

    async def _get_from_cache(self, key: str) -> Optional[str]:
        """Get value from cache if not expired."""
        async with self._cache_lock:
            cached = self._cache.get(key)
            if cached is None:
                return None

            if time.time() > cached.expires_at:
                del self._cache[key]
                return None

            return cached.value

    async def _set_cache(self, key: str, value: str):
        """Store value in cache with TTL."""
        async with self._cache_lock:
            self._cache[key] = CachedSecret(
                value=value,
                expires_at=time.time() + self.config.CACHE_TTL_SECONDS
            )

    async def _invalidate_cache(self, key: str):
        """Remove key from cache."""
        async with self._cache_lock:
            self._cache.pop(key, None)


# Singleton instance
_secrets_manager: Optional[SecretsManager] = None


async def get_secrets_manager(
    backend_type: Optional[str] = None,
    **kwargs
) -> SecretsManager:
    """
    Get or create secrets manager singleton.

    Args:
        backend_type: "local" or "aws" (defaults to SECRETS_BACKEND env var)
        **kwargs: Additional arguments passed to backend constructor

    Usage in FastAPI:
        @app.get("/config")
        async def get_config(
            secrets: SecretsManager = Depends(get_secrets_manager)
        ):
            db_password = await secrets.get_secret("database/password")
            ...
    """
    global _secrets_manager

    if _secrets_manager is None:
        config = SecretsConfig()
        backend_type = backend_type or os.environ.get(
            config.BACKEND_ENV_VAR,
            config.DEFAULT_BACKEND
        )

        # Create primary backend
        if backend_type == "aws":
            primary = AWSSecretsManagerBackend(**kwargs)
            fallback = LocalSecretsBackend()  # Local as fallback
        else:
            primary = LocalSecretsBackend(**kwargs)
            fallback = None

        _secrets_manager = SecretsManager(
            backend=primary,
            fallback_backend=fallback,
            config=config
        )

        logger.info(
            f"Secrets manager initialized",
            extra={
                "backend_type": backend_type,
                "has_fallback": fallback is not None,
                "event_type": "secrets_manager_initialized"
            }
        )

    return _secrets_manager

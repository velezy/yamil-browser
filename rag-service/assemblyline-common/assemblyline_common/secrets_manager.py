"""
AWS Secrets Manager Client - HIPAA-Compliant Credential Storage

Provides secure storage and retrieval of AI provider credentials
using AWS Secrets Manager instead of plaintext database storage.

Features:
- In-memory cache with configurable TTL (default 5 minutes)
- Async-friendly with sync boto3 wrapped
- Tenant-isolated secret naming (yamil/{tenant_id}/bedrock-credentials)
- Graceful degradation on SM unavailability
"""

import copy
import json
import time
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

CACHE_TTL = 300  # 5 minutes


class SecretsManagerClient:
    """
    Client for storing and retrieving AI credentials from AWS Secrets Manager.

    Secrets are stored under the path: yamil/{tenant_id}/bedrock-credentials
    with a 5-minute in-memory cache to reduce API calls.
    """

    def __init__(self, region: str = "us-east-1"):
        self._region = region
        self._cache: dict[str, tuple[dict, float]] = {}  # secret_name -> (value, expiry)
        self._client = None

    def _get_client(self, region: Optional[str] = None):
        """Get or create boto3 Secrets Manager client."""
        target_region = region or self._region
        if self._client is None or target_region != self._region:
            self._client = boto3.client(
                "secretsmanager",
                region_name=target_region,
            )
            self._region = target_region
        return self._client

    async def store_credentials(
        self,
        tenant_id: str,
        access_key_id: str,
        secret_access_key: str,
        region: str,
        provider: str = "bedrock",
    ) -> str:
        """
        Store AI provider credentials in Secrets Manager.

        Args:
            tenant_id: Tenant identifier
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            region: AWS region for Bedrock
            provider: AI provider name (bedrock/azure)

        Returns:
            Secret ARN for reference storage in DB
        """
        secret_name = f"yamil/{tenant_id}/{provider}-credentials"
        secret_value = json.dumps({
            "accessKeyId": access_key_id,
            "secretAccessKey": secret_access_key,
            "region": region,
            "provider": provider,
        })

        client = self._get_client(region)

        try:
            # Try to create new secret
            response = client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"AI provider credentials for tenant {tenant_id}",
                Tags=[
                    {"Key": "tenant_id", "Value": tenant_id},
                    {"Key": "provider", "Value": provider},
                    {"Key": "managed_by", "Value": "logic-weaver"},
                ],
            )
            arn = response["ARN"]
            logger.info(f"Created secret for tenant {tenant_id}: {secret_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                # Update existing secret
                response = client.update_secret(
                    SecretId=secret_name,
                    SecretString=secret_value,
                )
                arn = response["ARN"]
                logger.info(f"Updated secret for tenant {tenant_id}: {secret_name}")
            else:
                logger.error(f"Failed to store credentials: {e}")
                raise

        # Invalidate cache for this secret
        self._cache.pop(secret_name, None)

        return arn

    async def store_azure_credentials(
        self,
        tenant_id: str,
        endpoint: str,
        api_key: str,
        deployment_name: str,
        api_version: str,
    ) -> str:
        """
        Store Azure AI credentials in Secrets Manager.

        Args:
            tenant_id: Tenant identifier
            endpoint: Azure OpenAI endpoint URL
            api_key: Azure API key
            deployment_name: Model deployment name
            api_version: API version string

        Returns:
            Secret ARN for reference storage in DB
        """
        secret_name = f"yamil/{tenant_id}/azure-credentials"
        secret_value = json.dumps({
            "endpoint": endpoint,
            "apiKey": api_key,
            "deploymentName": deployment_name,
            "apiVersion": api_version,
            "provider": "azure",
        })

        client = self._get_client()

        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"Azure AI credentials for tenant {tenant_id}",
                Tags=[
                    {"Key": "tenant_id", "Value": tenant_id},
                    {"Key": "provider", "Value": "azure"},
                    {"Key": "managed_by", "Value": "logic-weaver"},
                ],
            )
            arn = response["ARN"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                response = client.update_secret(
                    SecretId=secret_name,
                    SecretString=secret_value,
                )
                arn = response["ARN"]
            else:
                raise

        self._cache.pop(secret_name, None)
        return arn

    async def get_credentials(self, secret_arn: str) -> Optional[dict]:
        """
        Fetch credentials from Secrets Manager with caching.

        Args:
            secret_arn: ARN or name of the secret

        Returns:
            Dict with credential fields, or None if unavailable
        """
        # Check cache first
        now = time.time()
        if secret_arn in self._cache:
            value, expiry = self._cache[secret_arn]
            if now < expiry:
                return value

        # Fetch from Secrets Manager
        try:
            client = self._get_client()
            response = client.get_secret_value(SecretId=secret_arn)
            secret_data = json.loads(response["SecretString"])

            # Cache the result
            self._cache[secret_arn] = (secret_data, now + CACHE_TTL)

            return secret_data
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("ResourceNotFoundException", "InvalidRequestException"):
                logger.warning(f"Secret not found: {secret_arn}")
                return None
            logger.error(f"Failed to fetch secret {secret_arn}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching secret: {e}")
            return None

    async def delete_credentials(self, secret_arn: str) -> bool:
        """
        Delete a secret from Secrets Manager.

        Uses 7-day recovery window for safety.

        Args:
            secret_arn: ARN or name of the secret to delete

        Returns:
            True if deleted, False if not found
        """
        try:
            client = self._get_client()
            client.delete_secret(
                SecretId=secret_arn,
                RecoveryWindowInDays=7,
            )
            # Remove from cache
            self._cache.pop(secret_arn, None)
            logger.info(f"Scheduled secret deletion: {secret_arn}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise

    async def store_rpa_credentials(
        self,
        tenant_id: str,
        credential_id: str,
        secrets: dict,
    ) -> str:
        """
        Store RPA credential secrets (username, password, totp_secret) in AWS SM.

        Args:
            tenant_id: Tenant identifier
            credential_id: RPA credential UUID
            secrets: Dict with username, password, and optionally totp_secret

        Returns:
            Secret ARN for reference
        """
        secret_name = f"yamil/{tenant_id}/rpa-credentials/{credential_id}"
        secret_value = json.dumps({
            "username": secrets.get("username", ""),
            "password": secrets.get("password", ""),
            "totp_secret": secrets.get("totp_secret", ""),
        })

        client = self._get_client()

        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"RPA credentials for tenant {tenant_id}, credential {credential_id}",
                Tags=[
                    {"Key": "tenant_id", "Value": tenant_id},
                    {"Key": "type", "Value": "rpa-credentials"},
                    {"Key": "managed_by", "Value": "logic-weaver"},
                ],
            )
            arn = response["ARN"]
            logger.info(f"Created RPA secret for tenant {tenant_id}: {secret_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                response = client.update_secret(
                    SecretId=secret_name,
                    SecretString=secret_value,
                )
                arn = response["ARN"]
                logger.info(f"Updated RPA secret for tenant {tenant_id}: {secret_name}")
            else:
                logger.error(f"Failed to store RPA credentials: {e}")
                raise

        self._cache.pop(secret_name, None)
        return arn

    async def store_connector_credentials(
        self,
        tenant_id: str,
        connector_id: str,
        secrets: dict,
    ) -> str:
        """
        Store connector credential secrets in AWS SM.

        Args:
            tenant_id: Tenant identifier
            connector_id: Connector UUID
            secrets: Dict with sensitive field values (e.g., password, token, header_value)

        Returns:
            Secret ARN for reference
        """
        secret_name = f"yamil/{tenant_id}/connector-credentials/{connector_id}"
        secret_value = json.dumps(secrets)

        client = self._get_client()

        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"Connector credentials for tenant {tenant_id}, connector {connector_id}",
                Tags=[
                    {"Key": "tenant_id", "Value": tenant_id},
                    {"Key": "type", "Value": "connector-credentials"},
                    {"Key": "managed_by", "Value": "logic-weaver"},
                ],
            )
            arn = response["ARN"]
            logger.info(f"Created connector secret for tenant {tenant_id}: {secret_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                response = client.update_secret(
                    SecretId=secret_name,
                    SecretString=secret_value,
                )
                arn = response["ARN"]
                logger.info(f"Updated connector secret for tenant {tenant_id}: {secret_name}")
            else:
                logger.error(f"Failed to store connector credentials: {e}")
                raise

        self._cache.pop(secret_name, None)
        return arn

    async def get_connector_credentials(
        self,
        tenant_id: str,
        connector_id: str,
    ) -> Optional[dict]:
        """
        Get connector credentials from AWS SM.

        Args:
            tenant_id: Tenant identifier
            connector_id: Connector UUID

        Returns:
            Dict with credential fields, or None if unavailable
        """
        secret_name = f"yamil/{tenant_id}/connector-credentials/{connector_id}"
        return await self.get_credentials(secret_name)

    async def delete_connector_credentials(
        self,
        tenant_id: str,
        connector_id: str,
    ) -> bool:
        """
        Delete connector credential secrets from AWS SM.

        Uses 7-day recovery window for safety.

        Args:
            tenant_id: Tenant identifier
            connector_id: Connector UUID

        Returns:
            True if deleted, False if not found
        """
        secret_name = f"yamil/{tenant_id}/connector-credentials/{connector_id}"
        return await self.delete_credentials(secret_name)

    async def store_auth_provider_credentials(
        self,
        tenant_id: str,
        provider_type: str,
        provider_id: str,
        secrets: dict,
    ) -> str:
        """
        Store auth provider credential secrets in AWS SM.

        Args:
            tenant_id: Tenant identifier
            provider_type: Provider type ("oidc", "saml", "ldap")
            provider_id: Provider UUID
            secrets: Dict with sensitive field values (e.g., client_secret, x509_certificate, bind_password)

        Returns:
            Secret ARN for reference
        """
        secret_name = f"yamil/{tenant_id}/{provider_type}-provider/{provider_id}"
        secret_value = json.dumps(secrets)

        client = self._get_client()

        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"{provider_type.upper()} auth provider credentials for tenant {tenant_id}, provider {provider_id}",
                Tags=[
                    {"Key": "tenant_id", "Value": tenant_id},
                    {"Key": "type", "Value": f"{provider_type}-provider"},
                    {"Key": "managed_by", "Value": "logic-weaver"},
                ],
            )
            arn = response["ARN"]
            logger.info(f"Created {provider_type} provider secret for tenant {tenant_id}: {secret_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                response = client.update_secret(
                    SecretId=secret_name,
                    SecretString=secret_value,
                )
                arn = response["ARN"]
                logger.info(f"Updated {provider_type} provider secret for tenant {tenant_id}: {secret_name}")
            else:
                logger.error(f"Failed to store {provider_type} provider credentials: {e}")
                raise

        self._cache.pop(secret_name, None)
        return arn

    async def delete_auth_provider_credentials(
        self,
        tenant_id: str,
        provider_type: str,
        provider_id: str,
    ) -> bool:
        """
        Delete auth provider credential secrets from AWS SM.

        Uses 7-day recovery window for safety.

        Args:
            tenant_id: Tenant identifier
            provider_type: Provider type ("oidc", "saml", "ldap")
            provider_id: Provider UUID

        Returns:
            True if deleted, False if not found
        """
        secret_name = f"yamil/{tenant_id}/{provider_type}-provider/{provider_id}"
        return await self.delete_credentials(secret_name)

    async def delete_rpa_credentials(
        self,
        tenant_id: str,
        credential_id: str,
    ) -> bool:
        """
        Delete RPA credential secrets from AWS SM.

        Uses 7-day recovery window for safety.

        Args:
            tenant_id: Tenant identifier
            credential_id: RPA credential UUID

        Returns:
            True if deleted, False if not found
        """
        secret_name = f"yamil/{tenant_id}/rpa-credentials/{credential_id}"
        return await self.delete_credentials(secret_name)

    async def store_jwks_keypair(
        self,
        tenant_id: str,
        kid: str,
        secrets: dict,
    ) -> str:
        """
        Store JWKS key pair (private key, public key, JWK) in AWS SM.

        Args:
            tenant_id: Tenant identifier
            kid: Key ID (UUID)
            secrets: Dict with private_key, public_key, jwk, and metadata

        Returns:
            Secret ARN for reference
        """
        secret_name = f"yamil/{tenant_id}/jwks-keypairs/{kid}"
        secret_value = json.dumps(secrets)

        client = self._get_client()

        try:
            response = client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                Description=f"JWKS key pair for tenant {tenant_id}, kid {kid}",
                Tags=[
                    {"Key": "tenant_id", "Value": tenant_id},
                    {"Key": "type", "Value": "jwks-keypair"},
                    {"Key": "kid", "Value": kid},
                    {"Key": "managed_by", "Value": "logic-weaver"},
                ],
            )
            arn = response["ARN"]
            logger.info(f"Created JWKS keypair secret for tenant {tenant_id}: {secret_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceExistsException":
                response = client.update_secret(
                    SecretId=secret_name,
                    SecretString=secret_value,
                )
                arn = response["ARN"]
                logger.info(f"Updated JWKS keypair secret for tenant {tenant_id}: {secret_name}")
            else:
                logger.error(f"Failed to store JWKS keypair: {e}")
                raise

        self._cache.pop(secret_name, None)
        return arn

    async def get_jwks_keypair(
        self,
        tenant_id: str,
        kid: str,
    ) -> Optional[dict]:
        """
        Get JWKS key pair from AWS SM.

        Args:
            tenant_id: Tenant identifier
            kid: Key ID (UUID)

        Returns:
            Dict with key pair data, or None if unavailable
        """
        secret_name = f"yamil/{tenant_id}/jwks-keypairs/{kid}"
        return await self.get_credentials(secret_name)

    async def delete_jwks_keypair(
        self,
        tenant_id: str,
        kid: str,
    ) -> bool:
        """
        Delete JWKS key pair from AWS SM.

        Uses 7-day recovery window for safety.

        Args:
            tenant_id: Tenant identifier
            kid: Key ID (UUID)

        Returns:
            True if deleted, False if not found
        """
        secret_name = f"yamil/{tenant_id}/jwks-keypairs/{kid}"
        return await self.delete_credentials(secret_name)

    def invalidate_cache(self, secret_arn: Optional[str] = None):
        """Clear cached credentials."""
        if secret_arn:
            self._cache.pop(secret_arn, None)
        else:
            self._cache.clear()


# Singleton instance
_secrets_client: Optional[SecretsManagerClient] = None


def get_secrets_manager(region: str = "us-east-1") -> SecretsManagerClient:
    """Get singleton SecretsManagerClient instance."""
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = SecretsManagerClient(region=region)
    return _secrets_client


# Sentinel value stored in DB config for SM-managed fields
SM_MANAGED_SENTINEL = "__SM_MANAGED__"


async def resolve_connector_config(
    connector_config: dict,
    encrypted_fields: list,
    tenant_id: str,
    connector_id: str,
) -> dict:
    """
    Resolve a connector's full config, fetching sensitive fields from AWS SM.

    Primary path: fetch from AWS Secrets Manager.
    Fallback: decrypt from DB (for legacy connectors not yet migrated).

    Args:
        connector_config: Raw config dict from the DB
        encrypted_fields: List of field names that are sensitive
        tenant_id: Tenant UUID string
        connector_id: Connector UUID string

    Returns:
        dict with all fields resolved (sensitive ones from SM)
    """
    result = copy.deepcopy(connector_config)
    result.pop("secret_arn", None)

    if not encrypted_fields:
        return result

    # Primary: fetch from AWS Secrets Manager
    if tenant_id and connector_id:
        try:
            sm = get_secrets_manager()
            sm_creds = await sm.get_connector_credentials(
                tenant_id=tenant_id,
                connector_id=connector_id,
            )
            if sm_creds:
                for field in encrypted_fields:
                    if field in sm_creds and sm_creds[field]:
                        result[field] = sm_creds[field]
                    elif result.get(field) == SM_MANAGED_SENTINEL:
                        result[field] = ""
                return result
        except Exception as e:
            logger.warning(f"SM fetch failed for connector {connector_id}: {e}")

    # Fallback: decrypt from DB (legacy connectors)
    try:
        from assemblyline_common.encryption import decrypt_value
        for field in encrypted_fields:
            val = result.get(field)
            if val and isinstance(val, str) and val != SM_MANAGED_SENTINEL:
                try:
                    result[field] = decrypt_value(val)
                except Exception:
                    pass
    except ImportError:
        pass

    return result

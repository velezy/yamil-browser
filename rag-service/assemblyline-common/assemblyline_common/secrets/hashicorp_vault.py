"""
HashiCorp Vault Secrets Backend for Logic Weaver.

Provides enterprise-grade HashiCorp Vault integration with:
- Multiple authentication methods (Token, AppRole, Kubernetes, AWS)
- KV secrets engine (v1 and v2)
- Dynamic secrets (database, AWS, etc.)
- Lease renewal and revocation
- Namespace isolation
- Caching with TTL
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


class AuthMethod(str, Enum):
    """Vault authentication methods."""
    TOKEN = "token"
    APPROLE = "approle"
    KUBERNETES = "kubernetes"
    AWS_IAM = "aws"
    LDAP = "ldap"
    USERPASS = "userpass"


class KVVersion(int, Enum):
    """KV secrets engine version."""
    V1 = 1
    V2 = 2


@dataclass
class VaultConfig:
    """Configuration for HashiCorp Vault."""
    url: str = "http://localhost:8200"

    # Authentication
    auth_method: AuthMethod = AuthMethod.TOKEN
    token: Optional[str] = None

    # AppRole auth
    role_id: Optional[str] = None
    secret_id: Optional[str] = None

    # Kubernetes auth
    kubernetes_role: Optional[str] = None
    kubernetes_jwt_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    # AWS IAM auth
    aws_role: Optional[str] = None
    aws_region: str = "us-east-1"

    # LDAP/Userpass auth
    username: Optional[str] = None
    password: Optional[str] = None

    # Namespace (Enterprise feature)
    namespace: Optional[str] = None

    # KV settings
    kv_mount_point: str = "secret"
    kv_version: KVVersion = KVVersion.V2

    # Cache settings
    cache_ttl_seconds: int = 300
    cache_max_size: int = 1000

    # Connection settings
    timeout_seconds: float = 30.0
    max_retries: int = 3
    verify_ssl: bool = True
    ca_cert: Optional[str] = None

    # Tenant isolation
    tenant_prefix: Optional[str] = None


@dataclass
class Secret:
    """Secret data with metadata."""
    path: str
    data: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    version: Optional[int] = None
    created_time: Optional[datetime] = None
    deletion_time: Optional[datetime] = None
    destroyed: bool = False


@dataclass
class Lease:
    """Lease information for dynamic secrets."""
    lease_id: str
    lease_duration: int
    renewable: bool
    data: dict[str, Any]
    expires_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if self.lease_duration > 0:
            self.expires_at = datetime.now() + timedelta(seconds=self.lease_duration)


class SecretCache:
    """TTL cache for secrets."""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[Any, float]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._cache:
                return None
            value, timestamp = self._cache[key]
            if time.time() - timestamp > self.ttl_seconds:
                del self._cache[key]
                return None
            return value

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        async with self._lock:
            if len(self._cache) >= self.max_size:
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
                del self._cache[oldest_key]
            self._cache[key] = (value, time.time())

    async def invalidate(self, key: str) -> None:
        async with self._lock:
            self._cache.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()


class HashiCorpVaultBackend:
    """
    HashiCorp Vault secrets backend.

    Features:
    - Multiple auth methods
    - KV v1 and v2 support
    - Dynamic secrets with lease management
    - Automatic token renewal
    - Namespace isolation
    - Caching
    """

    def __init__(self, config: VaultConfig):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._cache = SecretCache(
            max_size=config.cache_max_size,
            ttl_seconds=config.cache_ttl_seconds,
        )
        self._active_leases: dict[str, Lease] = {}
        self._renewal_task: Optional[asyncio.Task] = None
        self._metrics = {
            "cache_hits": 0,
            "cache_misses": 0,
            "api_calls": 0,
            "errors": 0,
            "leases_renewed": 0,
        }

    async def connect(self) -> None:
        """Initialize client and authenticate."""
        ssl_context = None
        if self.config.ca_cert:
            import ssl
            ssl_context = ssl.create_default_context(cafile=self.config.ca_cert)

        headers = {}
        if self.config.namespace:
            headers["X-Vault-Namespace"] = self.config.namespace

        self._client = httpx.AsyncClient(
            base_url=self.config.url,
            timeout=self.config.timeout_seconds,
            verify=ssl_context if ssl_context else self.config.verify_ssl,
            headers=headers,
        )

        # Authenticate
        await self._authenticate()

        # Start lease renewal task
        self._renewal_task = asyncio.create_task(self._lease_renewal_loop())

        logger.info(
            "HashiCorp Vault backend initialized",
            extra={
                "event_type": "vault.connected",
                "url": self.config.url,
                "auth_method": self.config.auth_method.value,
                "namespace": self.config.namespace,
            },
        )

    async def close(self) -> None:
        """Close client and revoke leases."""
        # Cancel renewal task
        if self._renewal_task:
            self._renewal_task.cancel()
            try:
                await self._renewal_task
            except asyncio.CancelledError:
                pass

        # Revoke all active leases
        for lease_id in list(self._active_leases.keys()):
            try:
                await self.revoke_lease(lease_id)
            except Exception as e:
                logger.warning(f"Failed to revoke lease {lease_id}: {e}")

        if self._client:
            await self._client.aclose()
            self._client = None

        await self._cache.clear()

    async def _authenticate(self) -> None:
        """Authenticate based on configured method."""
        if self.config.auth_method == AuthMethod.TOKEN:
            if not self.config.token:
                raise ValueError("Token required for token auth")
            self._token = self.config.token

        elif self.config.auth_method == AuthMethod.APPROLE:
            await self._auth_approle()

        elif self.config.auth_method == AuthMethod.KUBERNETES:
            await self._auth_kubernetes()

        elif self.config.auth_method == AuthMethod.AWS_IAM:
            await self._auth_aws_iam()

        elif self.config.auth_method == AuthMethod.LDAP:
            await self._auth_ldap()

        elif self.config.auth_method == AuthMethod.USERPASS:
            await self._auth_userpass()

        else:
            raise ValueError(f"Unsupported auth method: {self.config.auth_method}")

    async def _auth_approle(self) -> None:
        """Authenticate using AppRole."""
        if not self.config.role_id or not self.config.secret_id:
            raise ValueError("role_id and secret_id required for AppRole auth")

        response = await self._client.post(
            "/v1/auth/approle/login",
            json={
                "role_id": self.config.role_id,
                "secret_id": self.config.secret_id,
            },
        )
        response.raise_for_status()

        data = response.json()
        auth = data.get("auth", {})
        self._token = auth.get("client_token")
        ttl = auth.get("lease_duration", 3600)
        self._token_expires_at = datetime.now() + timedelta(seconds=ttl)

    async def _auth_kubernetes(self) -> None:
        """Authenticate using Kubernetes service account."""
        if not self.config.kubernetes_role:
            raise ValueError("kubernetes_role required for Kubernetes auth")

        # Read JWT from service account
        try:
            with open(self.config.kubernetes_jwt_path) as f:
                jwt = f.read().strip()
        except FileNotFoundError:
            raise ValueError(f"Kubernetes JWT not found at {self.config.kubernetes_jwt_path}")

        response = await self._client.post(
            "/v1/auth/kubernetes/login",
            json={
                "role": self.config.kubernetes_role,
                "jwt": jwt,
            },
        )
        response.raise_for_status()

        data = response.json()
        auth = data.get("auth", {})
        self._token = auth.get("client_token")
        ttl = auth.get("lease_duration", 3600)
        self._token_expires_at = datetime.now() + timedelta(seconds=ttl)

    async def _auth_aws_iam(self) -> None:
        """Authenticate using AWS IAM."""
        if not self.config.aws_role:
            raise ValueError("aws_role required for AWS IAM auth")

        import boto3
        import base64
        import json

        # Get AWS credentials and create signed request
        session = boto3.Session()
        credentials = session.get_credentials()
        frozen_credentials = credentials.get_frozen_credentials()

        # Create the signed request for sts:GetCallerIdentity
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        request = AWSRequest(
            method="POST",
            url=f"https://sts.{self.config.aws_region}.amazonaws.com/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            },
        )
        SigV4Auth(frozen_credentials, "sts", self.config.aws_region).add_auth(request)

        # Prepare Vault login payload
        headers_json = json.dumps(dict(request.headers))
        response = await self._client.post(
            "/v1/auth/aws/login",
            json={
                "role": self.config.aws_role,
                "iam_http_request_method": "POST",
                "iam_request_url": base64.b64encode(
                    f"https://sts.{self.config.aws_region}.amazonaws.com/".encode()
                ).decode(),
                "iam_request_body": base64.b64encode(request.data.encode()).decode(),
                "iam_request_headers": base64.b64encode(headers_json.encode()).decode(),
            },
        )
        response.raise_for_status()

        data = response.json()
        auth = data.get("auth", {})
        self._token = auth.get("client_token")
        ttl = auth.get("lease_duration", 3600)
        self._token_expires_at = datetime.now() + timedelta(seconds=ttl)

    async def _auth_ldap(self) -> None:
        """Authenticate using LDAP."""
        if not self.config.username or not self.config.password:
            raise ValueError("username and password required for LDAP auth")

        response = await self._client.post(
            f"/v1/auth/ldap/login/{self.config.username}",
            json={"password": self.config.password},
        )
        response.raise_for_status()

        data = response.json()
        auth = data.get("auth", {})
        self._token = auth.get("client_token")
        ttl = auth.get("lease_duration", 3600)
        self._token_expires_at = datetime.now() + timedelta(seconds=ttl)

    async def _auth_userpass(self) -> None:
        """Authenticate using username/password."""
        if not self.config.username or not self.config.password:
            raise ValueError("username and password required for userpass auth")

        response = await self._client.post(
            f"/v1/auth/userpass/login/{self.config.username}",
            json={"password": self.config.password},
        )
        response.raise_for_status()

        data = response.json()
        auth = data.get("auth", {})
        self._token = auth.get("client_token")
        ttl = auth.get("lease_duration", 3600)
        self._token_expires_at = datetime.now() + timedelta(seconds=ttl)

    async def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[dict] = None,
    ) -> dict:
        """Make authenticated request to Vault."""
        if not self._client or not self._token:
            raise RuntimeError("Not connected")

        # Check if token needs renewal
        if (
            self._token_expires_at
            and datetime.now() > self._token_expires_at - timedelta(minutes=5)
        ):
            await self._renew_token()

        headers = {"X-Vault-Token": self._token}

        self._metrics["api_calls"] += 1

        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                response = await self._client.request(
                    method,
                    path,
                    json=json_data,
                    headers=headers,
                )

                if response.status_code == 403:
                    # Token might be expired, try to re-authenticate
                    await self._authenticate()
                    headers["X-Vault-Token"] = self._token
                    continue

                if response.status_code == 404:
                    return {}

                response.raise_for_status()
                return response.json() if response.content else {}

            except httpx.HTTPError as e:
                last_error = e
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue

        self._metrics["errors"] += 1
        raise ConnectionError(f"Request failed after {self.config.max_retries} attempts: {last_error}")

    async def _renew_token(self) -> None:
        """Renew the current token."""
        try:
            response = await self._client.post(
                "/v1/auth/token/renew-self",
                headers={"X-Vault-Token": self._token},
            )
            response.raise_for_status()

            data = response.json()
            auth = data.get("auth", {})
            ttl = auth.get("lease_duration", 3600)
            self._token_expires_at = datetime.now() + timedelta(seconds=ttl)

            logger.debug("Vault token renewed")

        except Exception as e:
            logger.warning(f"Token renewal failed, re-authenticating: {e}")
            await self._authenticate()

    async def _lease_renewal_loop(self) -> None:
        """Background task to renew leases."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                now = datetime.now()
                for lease_id, lease in list(self._active_leases.items()):
                    # Renew if expiring within 5 minutes
                    if lease.renewable and lease.expires_at - now < timedelta(minutes=5):
                        try:
                            await self.renew_lease(lease_id)
                            self._metrics["leases_renewed"] += 1
                        except Exception as e:
                            logger.warning(f"Failed to renew lease {lease_id}: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Lease renewal error: {e}")

    def _get_path(self, path: str) -> str:
        """Get full path with tenant prefix."""
        if self.config.tenant_prefix:
            return f"{self.config.tenant_prefix}/{path}"
        return path

    # -------------------------------------------------------------------------
    # KV Secrets Engine
    # -------------------------------------------------------------------------

    async def get_secret(
        self,
        path: str,
        version: Optional[int] = None,
        use_cache: bool = True,
    ) -> Optional[dict[str, Any]]:
        """
        Get a secret from KV engine.

        Args:
            path: Secret path (relative to mount point)
            version: Specific version (KV v2 only)
            use_cache: Whether to use cache
        """
        full_path = self._get_path(path)
        cache_key = f"kv:{full_path}:{version or 'latest'}"

        if use_cache:
            cached = await self._cache.get(cache_key)
            if cached is not None:
                self._metrics["cache_hits"] += 1
                return cached

        self._metrics["cache_misses"] += 1

        if self.config.kv_version == KVVersion.V2:
            api_path = f"/v1/{self.config.kv_mount_point}/data/{full_path}"
            if version:
                api_path += f"?version={version}"
        else:
            api_path = f"/v1/{self.config.kv_mount_point}/{full_path}"

        result = await self._request("GET", api_path)

        if not result:
            return None

        if self.config.kv_version == KVVersion.V2:
            data = result.get("data", {}).get("data", {})
        else:
            data = result.get("data", {})

        await self._cache.set(cache_key, data)
        return data

    async def set_secret(
        self,
        path: str,
        data: dict[str, Any],
        cas: Optional[int] = None,
    ) -> Optional[dict]:
        """
        Set a secret in KV engine.

        Args:
            path: Secret path
            data: Secret data
            cas: Check-and-set version (KV v2 only, for optimistic locking)
        """
        full_path = self._get_path(path)

        if self.config.kv_version == KVVersion.V2:
            api_path = f"/v1/{self.config.kv_mount_point}/data/{full_path}"
            payload = {"data": data}
            if cas is not None:
                payload["options"] = {"cas": cas}
        else:
            api_path = f"/v1/{self.config.kv_mount_point}/{full_path}"
            payload = data

        result = await self._request("POST", api_path, payload)

        # Invalidate cache
        await self._cache.invalidate(f"kv:{full_path}:latest")

        logger.info(
            "Secret written to Vault",
            extra={
                "event_type": "vault.secret_written",
                "path": full_path,
            },
        )

        return result.get("data") if self.config.kv_version == KVVersion.V2 else result

    async def delete_secret(
        self,
        path: str,
        versions: Optional[list[int]] = None,
    ) -> None:
        """
        Delete a secret.

        For KV v2, this soft-deletes. Use destroy_secret for permanent deletion.
        """
        full_path = self._get_path(path)

        if self.config.kv_version == KVVersion.V2:
            if versions:
                api_path = f"/v1/{self.config.kv_mount_point}/delete/{full_path}"
                await self._request("POST", api_path, {"versions": versions})
            else:
                api_path = f"/v1/{self.config.kv_mount_point}/data/{full_path}"
                await self._request("DELETE", api_path)
        else:
            api_path = f"/v1/{self.config.kv_mount_point}/{full_path}"
            await self._request("DELETE", api_path)

        await self._cache.invalidate(f"kv:{full_path}:latest")

        logger.info(
            "Secret deleted from Vault",
            extra={
                "event_type": "vault.secret_deleted",
                "path": full_path,
            },
        )

    async def destroy_secret(
        self,
        path: str,
        versions: list[int],
    ) -> None:
        """Permanently destroy secret versions (KV v2 only)."""
        if self.config.kv_version != KVVersion.V2:
            raise ValueError("destroy_secret only available for KV v2")

        full_path = self._get_path(path)
        api_path = f"/v1/{self.config.kv_mount_point}/destroy/{full_path}"

        await self._request("POST", api_path, {"versions": versions})

        logger.info(
            "Secret versions destroyed",
            extra={
                "event_type": "vault.secret_destroyed",
                "path": full_path,
                "versions": versions,
            },
        )

    async def list_secrets(self, path: str = "") -> list[str]:
        """List secrets at a path."""
        full_path = self._get_path(path)

        if self.config.kv_version == KVVersion.V2:
            api_path = f"/v1/{self.config.kv_mount_point}/metadata/{full_path}"
        else:
            api_path = f"/v1/{self.config.kv_mount_point}/{full_path}"

        result = await self._request("LIST", api_path)
        return result.get("data", {}).get("keys", [])

    async def get_secret_metadata(self, path: str) -> dict:
        """Get secret metadata (KV v2 only)."""
        if self.config.kv_version != KVVersion.V2:
            raise ValueError("Metadata only available for KV v2")

        full_path = self._get_path(path)
        api_path = f"/v1/{self.config.kv_mount_point}/metadata/{full_path}"

        result = await self._request("GET", api_path)
        return result.get("data", {})

    # -------------------------------------------------------------------------
    # Dynamic Secrets
    # -------------------------------------------------------------------------

    async def get_dynamic_secret(
        self,
        engine: str,
        role: str,
    ) -> Lease:
        """
        Get a dynamic secret (database creds, AWS creds, etc.).

        Args:
            engine: Secrets engine mount point (e.g., "database", "aws")
            role: Role name to get credentials for
        """
        api_path = f"/v1/{engine}/creds/{role}"
        result = await self._request("GET", api_path)

        lease = Lease(
            lease_id=result.get("lease_id", ""),
            lease_duration=result.get("lease_duration", 0),
            renewable=result.get("renewable", False),
            data=result.get("data", {}),
        )

        # Track the lease
        if lease.lease_id:
            self._active_leases[lease.lease_id] = lease

        logger.info(
            "Dynamic secret obtained",
            extra={
                "event_type": "vault.dynamic_secret_obtained",
                "engine": engine,
                "role": role,
                "lease_id": lease.lease_id,
                "lease_duration": lease.lease_duration,
            },
        )

        return lease

    async def renew_lease(
        self,
        lease_id: str,
        increment: Optional[int] = None,
    ) -> Lease:
        """Renew a lease."""
        payload = {"lease_id": lease_id}
        if increment:
            payload["increment"] = increment

        result = await self._request("POST", "/v1/sys/leases/renew", payload)

        lease = Lease(
            lease_id=result.get("lease_id", lease_id),
            lease_duration=result.get("lease_duration", 0),
            renewable=result.get("renewable", False),
            data=self._active_leases.get(lease_id, Lease("", 0, False, {})).data,
        )

        self._active_leases[lease_id] = lease

        logger.debug(
            f"Lease renewed: {lease_id}",
            extra={
                "event_type": "vault.lease_renewed",
                "lease_id": lease_id,
                "new_duration": lease.lease_duration,
            },
        )

        return lease

    async def revoke_lease(self, lease_id: str) -> None:
        """Revoke a lease."""
        await self._request("POST", "/v1/sys/leases/revoke", {"lease_id": lease_id})

        self._active_leases.pop(lease_id, None)

        logger.info(
            "Lease revoked",
            extra={
                "event_type": "vault.lease_revoked",
                "lease_id": lease_id,
            },
        )

    # -------------------------------------------------------------------------
    # Transit Secrets Engine (Encryption as a Service)
    # -------------------------------------------------------------------------

    async def encrypt(
        self,
        key_name: str,
        plaintext: str,
        mount_point: str = "transit",
    ) -> str:
        """
        Encrypt data using transit engine.

        Args:
            key_name: Name of the encryption key
            plaintext: Base64-encoded plaintext
            mount_point: Transit engine mount point
        """
        api_path = f"/v1/{mount_point}/encrypt/{key_name}"
        result = await self._request("POST", api_path, {"plaintext": plaintext})
        return result.get("data", {}).get("ciphertext", "")

    async def decrypt(
        self,
        key_name: str,
        ciphertext: str,
        mount_point: str = "transit",
    ) -> str:
        """
        Decrypt data using transit engine.

        Returns base64-encoded plaintext.
        """
        api_path = f"/v1/{mount_point}/decrypt/{key_name}"
        result = await self._request("POST", api_path, {"ciphertext": ciphertext})
        return result.get("data", {}).get("plaintext", "")

    async def create_transit_key(
        self,
        key_name: str,
        key_type: str = "aes256-gcm96",
        mount_point: str = "transit",
    ) -> None:
        """Create a new transit encryption key."""
        api_path = f"/v1/{mount_point}/keys/{key_name}"
        await self._request("POST", api_path, {"type": key_type})

        logger.info(
            "Transit key created",
            extra={
                "event_type": "vault.transit_key_created",
                "key_name": key_name,
                "key_type": key_type,
            },
        )

    async def rotate_transit_key(
        self,
        key_name: str,
        mount_point: str = "transit",
    ) -> None:
        """Rotate a transit encryption key."""
        api_path = f"/v1/{mount_point}/keys/{key_name}/rotate"
        await self._request("POST", api_path)

        logger.info(
            "Transit key rotated",
            extra={
                "event_type": "vault.transit_key_rotated",
                "key_name": key_name,
            },
        )

    # -------------------------------------------------------------------------
    # PKI Secrets Engine
    # -------------------------------------------------------------------------

    async def generate_certificate(
        self,
        role: str,
        common_name: str,
        mount_point: str = "pki",
        ttl: Optional[str] = None,
        alt_names: Optional[list[str]] = None,
        ip_sans: Optional[list[str]] = None,
    ) -> dict:
        """
        Generate a certificate from PKI engine.

        Returns dict with certificate, private_key, ca_chain, etc.
        """
        api_path = f"/v1/{mount_point}/issue/{role}"
        payload = {"common_name": common_name}

        if ttl:
            payload["ttl"] = ttl
        if alt_names:
            payload["alt_names"] = ",".join(alt_names)
        if ip_sans:
            payload["ip_sans"] = ",".join(ip_sans)

        result = await self._request("POST", api_path, payload)

        logger.info(
            "Certificate generated",
            extra={
                "event_type": "vault.certificate_generated",
                "role": role,
                "common_name": common_name,
            },
        )

        return result.get("data", {})

    # -------------------------------------------------------------------------
    # Health & Status
    # -------------------------------------------------------------------------

    async def health_check(self) -> dict:
        """Check Vault health status."""
        try:
            response = await self._client.get("/v1/sys/health")
            return {
                "healthy": response.status_code in [200, 429, 472, 473, 501, 503],
                "initialized": response.json().get("initialized", False),
                "sealed": response.json().get("sealed", True),
                "standby": response.json().get("standby", False),
            }
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e),
            }

    def get_metrics(self) -> dict:
        """Get backend metrics."""
        return {
            **self._metrics,
            "cache_size": len(self._cache._cache),
            "active_leases": len(self._active_leases),
            "url": self.config.url,
            "namespace": self.config.namespace,
        }


# Singleton instance management
_instances: dict[str, HashiCorpVaultBackend] = {}
_lock = asyncio.Lock()


async def get_hashicorp_vault_backend(
    config: Optional[VaultConfig] = None,
    instance_key: str = "default",
) -> HashiCorpVaultBackend:
    """Get or create a HashiCorp Vault backend instance."""
    async with _lock:
        if instance_key not in _instances:
            if config is None:
                raise ValueError("Config required for first initialization")

            backend = HashiCorpVaultBackend(config)
            await backend.connect()
            _instances[instance_key] = backend

        return _instances[instance_key]


async def close_hashicorp_vault_backend(instance_key: str = "default") -> None:
    """Close and remove a backend instance."""
    async with _lock:
        if instance_key in _instances:
            await _instances[instance_key].close()
            del _instances[instance_key]

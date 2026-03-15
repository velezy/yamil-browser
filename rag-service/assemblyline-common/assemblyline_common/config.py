"""
Configuration settings for Logic Weaver services.

Loaded from environment variables with sensible defaults for local development.

Supports multiple credential backends:
1. AWS Secrets Manager (USE_SECRETS_MANAGER=true) - Recommended for production
2. Encrypted credentials file (USE_ENCRYPTED_CREDENTIALS=true) - Good for dev
3. Direct DATABASE_URL in .env - Least secure, fallback only
"""

import getpass
import os
import sys
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
from functools import lru_cache

# Compute the project root (logic-weaver directory)
# This file is at: shared/python/assemblyline_common/config.py
# Project root is 4 levels up
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application
    APP_NAME: str = "Logic Weaver"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = Field(default="development", description="development, staging, production")
    
    # Database (PostgreSQL) - Sized for 200K+ API calls/day
    # Pool math: 11 services × pool_size = total DB connections (pool is per-service, not per-worker)
    # EC2 (.env.prod): 11 services × 5 pool + 2 overflow = 77 connections (fits RDS db.t3.micro ~79 max)
    # For larger RDS: increase pool_size/overflow via env vars
    DATABASE_URL: str = Field(
        default="",
        description="PostgreSQL connection string - must be set via environment variable"
    )
    DATABASE_POOL_SIZE: int = 15  # Per worker process — 15 × 4 workers = 60 per service
    DATABASE_MAX_OVERFLOW: int = 10  # Burst headroom per worker
    DATABASE_POOL_TIMEOUT: int = 30  # Seconds to wait for connection
    DATABASE_POOL_RECYCLE: int = 1800  # Recycle connections every 30 min
    DATABASE_ECHO: bool = False
    DATABASE_STATEMENT_TIMEOUT: int = Field(
        default=30000,
        description="PostgreSQL statement_timeout in milliseconds (0=disabled). Prevents runaway queries."
    )
    DATABASE_IDLE_IN_TRANSACTION_TIMEOUT: int = Field(
        default=60000,
        description="PostgreSQL idle_in_transaction_session_timeout in ms. Kills idle transactions."
    )

    # Flow execution
    FLOW_EXECUTION_TIMEOUT_MS: int = Field(
        default=30000,
        description="Default flow execution timeout in milliseconds. Overridden by per-flow policy."
    )

    # Redis - Sized for 200K+ API calls/day (rate limiting, caching, sessions)
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection string"
    )
    REDIS_POOL_SIZE: int = 100  # Shared across rate limit checks, cache, sessions
    REDIS_POOL_MIN_IDLE: int = 20  # Pre-warm for burst traffic
    REDIS_SOCKET_TIMEOUT: float = 5.0
    REDIS_SOCKET_CONNECT_TIMEOUT: float = 5.0

    # Worker processes per service (gunicorn/uvicorn)
    UVICORN_WORKERS: int = Field(default=4, description="Workers per service — 4 handles ~50 req/sec per service")
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    KAFKA_CONSUMER_GROUP: str = "logic_weaver"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    
    # JWT Authentication
    JWT_SECRET_KEY: str = Field(
        default="change-me-in-production-use-strong-secret",
        description="Secret key for JWT tokens"
    )
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    
    # API Keys
    API_KEY_PREFIX: str = "mw"
    API_KEY_HASH_ALGORITHM: str = "sha256"
    
    # Rate Limiting — defaults for FastAPI middleware (separate from APIsix gateway tiers)
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_DEFAULT_REQUESTS: int = 10000  # 10K/hour per API key for direct service access
    RATE_LIMIT_DEFAULT_WINDOW_SECONDS: int = 3600
    
    # Epic FHIR Integration
    EPIC_FHIR_BASE_URL: Optional[str] = None
    EPIC_CLIENT_ID: Optional[str] = None
    EPIC_PRIVATE_KEY_PATH: Optional[str] = None
    EPIC_TOKEN_ENDPOINT: Optional[str] = None
    
    # Databricks
    DATABRICKS_HOST: Optional[str] = None
    DATABRICKS_HTTP_PATH: Optional[str] = None
    DATABRICKS_TOKEN: Optional[str] = None
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    LOG_INCLUDE_TIMESTAMP: bool = True

    # Observability (Prometheus + Jaeger)
    OTLP_ENDPOINT: Optional[str] = Field(
        default=None,
        description="OTLP endpoint for distributed tracing (e.g., http://localhost:4317)"
    )
    PROMETHEUS_URL: Optional[str] = Field(
        default=None,
        description="Prometheus URL (e.g., http://localhost:9099)"
    )

    # CORS
    CORS_ORIGINS: str = Field(
        default="http://localhost:3000,http://localhost:3030,http://localhost:3031,http://localhost:3032,http://localhost:3033,http://localhost:3034,http://localhost:3035,http://localhost:3036,http://localhost:3037,http://localhost:3038,http://localhost:3039,http://localhost:3040,http://localhost:5173",
        description="Comma-separated list of allowed origins"
    )
    
    # PHI/Security
    PHI_AUDIT_ENABLED: bool = True
    ENCRYPTION_KEY: Optional[str] = None

    # OIDC / Entra ID (per-provider config is in DB, these are global defaults)
    OIDC_STATE_TTL_SECONDS: int = 600  # 10 minutes for login flow
    OIDC_JWKS_CACHE_TTL_SECONDS: int = 3600  # 1 hour JWKS cache

    # Database Environment (local or aws)
    DB_ENVIRONMENT: str = Field(
        default="local",
        description="Database environment: 'local' for local PostgreSQL, 'aws' for AWS RDS"
    )

    # AWS Secrets Manager (Priority 1 - Default for all environments)
    USE_SECRETS_MANAGER: bool = Field(
        default=True,
        description="Use AWS Secrets Manager to fetch database credentials. Disable only for local dev without AWS access."
    )
    AWS_REGION: str = Field(default="us-east-1", description="AWS region for Secrets Manager")
    AWS_SECRET_PREFIX: str = Field(default="yamil", description="Prefix for AWS secrets")

    # Credential Backend for Gateway consumers (where to store API keys/credentials)
    CREDENTIAL_BACKEND: str = Field(
        default="aws_secrets_manager",
        description="Where to store gateway consumer credentials: 'local' (encrypted in DB) or 'aws_secrets_manager'"
    )

    # RDS IAM Authentication (Priority 0 - Best for production on AWS)
    USE_IAM_AUTH: bool = Field(
        default=False,
        description="Use RDS IAM authentication instead of password. Requires IAM role with rds-db:connect."
    )
    RDS_IAM_HOST: str = Field(
        default="",
        description="RDS hostname for IAM auth (e.g., yamil-dev.xxxx.us-east-1.rds.amazonaws.com)"
    )
    RDS_IAM_PORT: int = Field(default=5432, description="RDS port for IAM auth")
    RDS_IAM_USER: str = Field(default="yamil_app", description="PostgreSQL user with rds_iam grant")
    RDS_IAM_DBNAME: str = Field(default="message_weaver", description="Database name for IAM auth")

    # Encrypted Credentials File (Priority 2 - Good for dev)
    USE_ENCRYPTED_CREDENTIALS: bool = Field(
        default=False,
        description="Use encrypted credentials file instead of .env DATABASE_URL"
    )
    MASTER_PASSWORD: Optional[str] = Field(
        default=None,
        description="Master password for encrypted credentials (set via env var or prompt)"
    )

    # Legacy setting (kept for compatibility)
    SECRETS_BACKEND: str = Field(
        default="local",
        description="Deprecated: Use USE_SECRETS_MANAGER instead"
    )
    
    # Service Ports (for local development)
    AUTH_SERVICE_PORT: int = 8001
    INBOUND_SERVICE_PORT: int = 8002
    LOGICWEAVER_SERVICE_PORT: int = 8003
    OUTBOUND_SERVICE_PORT: int = 8004
    CONNECTOR_SERVICE_PORT: int = 8005
    ERROR_HANDLER_SERVICE_PORT: int = 8006
    AUDIT_SERVICE_PORT: int = 8007
    AI_ORCHESTRA_SERVICE_PORT: int = 8008
    AI_POLICY_SERVICE_PORT: int = 8009
    THREAT_PROTECTION_SERVICE_PORT: int = 8010
    FLOW_EXECUTION_SERVICE_PORT: int = 8011
    POLICY_SERVICE_PORT: int = 8012
    DSL_ENGINES_SERVICE_PORT: int = 8013
    GATEWAY_SERVICE_PORT: int = 9091

    # Service URLs (for inter-service communication)
    FLOW_EXECUTION_SERVICE_URL: str = "http://localhost:8011"

    # Gateway / APIsix
    GATEWAY_SIMULATION_MODE: Optional[bool] = Field(default=None, description="None = auto-detect APIsix")
    LOGICWEAVER_SERVICE_URL: str = "http://localhost:8003"
    APISIX_ADMIN_URL: str = "http://localhost:9180"
    APISIX_ADMIN_KEY: str = Field(
        default="",
        description="APIsix admin API key — must be set via Vault or environment variable"
    )
    APISIX_GATEWAY_HTTP: str = Field(
        default="http://localhost:9080",
        description="Public URL for APIsix gateway (used in curl examples)"
    )
    APISIX_GATEWAY_EXTERNAL: str = Field(
        default="",
        description="External/production URL for APIsix gateway (e.g. http://23.21.224.117:9082). If empty, APISIX_GATEWAY_HTTP is used."
    )
    GATEWAY_INTERNAL_SECRET: str = Field(
        default="",
        description="Shared secret for APIsix to authenticate with internal services — must be set via Vault or environment variable"
    )

    @property
    def cors_origins_list(self) -> list[str]:
        """Return CORS origins as a list."""
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]

    @property
    def cors_origin_regex(self) -> str:
        """Return regex pattern for CORS origins - allows any localhost port (Mac/Windows/Linux)."""
        return r"^https?://(localhost|127\.0\.0\.1|0\.0\.0\.0)(:\d+)?$"
    
    class Config:
        env_file = str(_ENV_FILE)
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Singleton instance
settings = get_settings()


# Cached values (so we only fetch once per session)
_cached_master_password: Optional[str] = None
_cached_secrets_manager_url: Optional[str] = None


def get_database_url_from_secrets_manager() -> Optional[str]:
    """
    Fetch database URL from AWS Secrets Manager.

    Secret names follow the pattern: {prefix}/db/{environment}
    e.g., logic-weaver/db/local or logic-weaver/db/aws

    Returns:
        Database URL string if successful, None if failed
    """
    global _cached_secrets_manager_url

    # Return cached value if available
    if _cached_secrets_manager_url:
        return _cached_secrets_manager_url

    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        # Build secret name based on environment
        secret_name = f"{settings.AWS_SECRET_PREFIX}/db/{settings.DB_ENVIRONMENT}"

        # Create Secrets Manager client
        client = boto3.client(
            service_name='secretsmanager',
            region_name=settings.AWS_REGION
        )

        # Fetch the secret
        response = client.get_secret_value(SecretId=secret_name)
        database_url = response['SecretString']

        # Cache the result
        _cached_secrets_manager_url = database_url

        env_label = "AWS RDS" if settings.DB_ENVIRONMENT == "aws" else "Local PostgreSQL"
        print(f"✓ Database URL fetched from Secrets Manager ({env_label})\n")

        return database_url

    except ImportError:
        print("✗ boto3 not installed. Run: pip install boto3")
        return None
    except NoCredentialsError:
        print("✗ AWS credentials not configured")
        return None
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'ResourceNotFoundException':
            print(f"✗ Secret not found: {settings.AWS_SECRET_PREFIX}/db/{settings.DB_ENVIRONMENT}")
        else:
            print(f"✗ Secrets Manager error: {e}")
        return None
    except Exception as e:
        print(f"✗ Failed to fetch from Secrets Manager: {e}")
        return None


def clear_secrets_manager_cache() -> None:
    """Clear the cached Secrets Manager URL (useful when switching environments)."""
    global _cached_secrets_manager_url
    _cached_secrets_manager_url = None


def get_database_url_with_encryption() -> str:
    """
    Get database URL using the configured credential backend.

    Priority order:
    1. AWS Secrets Manager (if USE_SECRETS_MANAGER=true)
    2. Encrypted credentials file (if USE_ENCRYPTED_CREDENTIALS=true)
    3. Direct DATABASE_URL from .env (fallback)

    The DB_ENVIRONMENT setting determines which database to connect to:
    - 'local': Local PostgreSQL database
    - 'aws': AWS RDS database

    Returns:
        Database URL string
    """
    global _cached_master_password

    # Priority 1: AWS Secrets Manager
    if settings.USE_SECRETS_MANAGER:
        url = get_database_url_from_secrets_manager()
        if url:
            return url
        print("Falling back to next credential backend...")

    # Priority 2: Encrypted credentials file
    if not settings.USE_ENCRYPTED_CREDENTIALS:
        return settings.DATABASE_URL

    # Import here to avoid circular imports
    from assemblyline_common.credentials import (
        credentials_exist,
        get_database_url,
    )

    if not credentials_exist():
        print("\n" + "=" * 60)
        print("  Encrypted credentials not found!")
        print("=" * 60)
        print("\nRun the following to set up encrypted credentials:")
        print("  python -m assemblyline_common.credentials.setup")
        print("\nFalling back to .env DATABASE_URL...")
        return settings.DATABASE_URL

    # Get master password
    master_password = _cached_master_password or settings.MASTER_PASSWORD

    if not master_password:
        # Prompt for password
        print("\n" + "=" * 60)
        print("  Logic Weaver - Enter Master Password")
        print("=" * 60)
        try:
            master_password = getpass.getpass("\nMaster password: ")
        except (EOFError, KeyboardInterrupt):
            print("\nNo password provided, falling back to .env DATABASE_URL...")
            return settings.DATABASE_URL

    try:
        # Pass the environment to get the correct database URL
        database_url = get_database_url(master_password, environment=settings.DB_ENVIRONMENT)
        # Cache the password for this session
        _cached_master_password = master_password
        env_label = "AWS RDS" if settings.DB_ENVIRONMENT == "aws" else "Local PostgreSQL"
        print(f"✓ Credentials decrypted successfully ({env_label})\n")
        return database_url
    except ValueError as e:
        print(f"\n✗ Failed to decrypt credentials: {e}")
        print("Falling back to .env DATABASE_URL...")
        return settings.DATABASE_URL
    except FileNotFoundError:
        print("\n✗ Encrypted credentials file not found")
        print("Run: python -m assemblyline_common.credentials.setup")
        print("Falling back to .env DATABASE_URL...")
        return settings.DATABASE_URL


def set_master_password(password: str) -> None:
    """
    Set the master password programmatically (for use in tests or scripts).

    Args:
        password: The master password
    """
    global _cached_master_password
    _cached_master_password = password


def clear_master_password() -> None:
    """Clear the cached master password."""
    global _cached_master_password
    _cached_master_password = None


# =============================================================================
# Legacy ServiceConfig (used by chat, document, rag, orchestrator services)
# Provides service ports, URLs, JWT config, Redis config, etc.
# =============================================================================
from assemblyline_common.service_config import ServiceConfig, config

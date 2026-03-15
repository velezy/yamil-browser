"""
DriveSentinel Central Configuration

All service URLs, ports, and configuration values are defined here.
Environment variables can override any default value.

Usage:
    from shared.config import config

    # Get service URL
    auth_url = config.AUTH_SERVICE_URL

    # Get port
    gateway_port = config.GATEWAY_PORT
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ServiceConfig:
    """Central configuration for all DriveSentinel services."""

    # ==========================================================================
    # Service Ports (17xxx range to avoid system conflicts)
    # ==========================================================================
    GATEWAY_PORT: int = field(default_factory=lambda: int(os.getenv("GATEWAY_PORT", "17080")))
    CHAT_PORT: int = field(default_factory=lambda: int(os.getenv("CHAT_PORT", "17001")))
    RAG_PORT: int = field(default_factory=lambda: int(os.getenv("RAG_PORT", "17002")))
    ORCHESTRATOR_PORT: int = field(default_factory=lambda: int(os.getenv("ORCHESTRATOR_PORT", "17003")))
    DOCUMENTS_PORT: int = field(default_factory=lambda: int(os.getenv("DOCUMENTS_PORT", "17004")))
    AUTH_PORT: int = field(default_factory=lambda: int(os.getenv("AUTH_PORT", "17005")))
    AUDIT_PORT: int = field(default_factory=lambda: int(os.getenv("AUDIT_PORT", "17006")))
    ERRORS_PORT: int = field(default_factory=lambda: int(os.getenv("ERRORS_PORT", "17007")))
    LICENSE_PORT: int = field(default_factory=lambda: int(os.getenv("LICENSE_PORT", "17008")))
    FRONTEND_PORT: int = field(default_factory=lambda: int(os.getenv("FRONTEND_PORT", "3570")))

    # ==========================================================================
    # Service Base URL (for external access)
    # ==========================================================================
    SERVICE_HOST: str = field(default_factory=lambda: os.getenv("SERVICE_HOST", "localhost"))

    # ==========================================================================
    # Service URLs (computed from host and ports)
    # ==========================================================================
    @property
    def GATEWAY_URL(self) -> str:
        return os.getenv("GATEWAY_URL", f"http://{self.SERVICE_HOST}:{self.GATEWAY_PORT}")

    @property
    def CHAT_SERVICE_URL(self) -> str:
        return os.getenv("CHAT_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.CHAT_PORT}")

    @property
    def RAG_SERVICE_URL(self) -> str:
        return os.getenv("RAG_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.RAG_PORT}")

    @property
    def ORCHESTRATOR_URL(self) -> str:
        return os.getenv("ORCHESTRATOR_URL", f"http://{self.SERVICE_HOST}:{self.ORCHESTRATOR_PORT}")

    @property
    def DOCUMENTS_SERVICE_URL(self) -> str:
        return os.getenv("DOCUMENTS_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.DOCUMENTS_PORT}")

    @property
    def AUTH_SERVICE_URL(self) -> str:
        return os.getenv("AUTH_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.AUTH_PORT}")

    @property
    def AUDIT_SERVICE_URL(self) -> str:
        return os.getenv("AUDIT_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.AUDIT_PORT}")

    @property
    def ERRORS_SERVICE_URL(self) -> str:
        return os.getenv("ERRORS_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.ERRORS_PORT}")

    @property
    def LICENSE_SERVICE_URL(self) -> str:
        return os.getenv("LICENSE_SERVICE_URL", f"http://{self.SERVICE_HOST}:{self.LICENSE_PORT}")

    @property
    def FRONTEND_URL(self) -> str:
        return os.getenv("FRONTEND_URL", f"http://{self.SERVICE_HOST}:{self.FRONTEND_PORT}")

    # ==========================================================================
    # Database Configuration
    # ==========================================================================
    DATABASE_HOST: str = field(default_factory=lambda: os.getenv("DATABASE_HOST", "localhost"))
    DATABASE_PORT: int = field(default_factory=lambda: int(os.getenv("DATABASE_PORT", "5450")))
    DATABASE_NAME: str = field(default_factory=lambda: os.getenv("DATABASE_NAME", "drivesentinel"))
    DATABASE_USER: str = field(default_factory=lambda: os.getenv("DATABASE_USER", "yaml"))
    DATABASE_PASSWORD: str = field(default_factory=lambda: os.getenv("DATABASE_PASSWORD", ""))

    @property
    def DATABASE_URL(self) -> str:
        """Construct database URL from components."""
        password_part = f":{self.DATABASE_PASSWORD}" if self.DATABASE_PASSWORD else ""
        return os.getenv(
            "DATABASE_URL",
            f"postgresql+asyncpg://{self.DATABASE_USER}{password_part}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )

    @property
    def DATABASE_URL_SYNC(self) -> str:
        """Synchronous database URL (for alembic, etc.)."""
        password_part = f":{self.DATABASE_PASSWORD}" if self.DATABASE_PASSWORD else ""
        return os.getenv(
            "DATABASE_URL_SYNC",
            f"postgresql://{self.DATABASE_USER}{password_part}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )

    # ==========================================================================
    # Redis Configuration
    # ==========================================================================
    REDIS_HOST: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    REDIS_PORT: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    REDIS_PASSWORD: Optional[str] = field(default_factory=lambda: os.getenv("REDIS_PASSWORD"))

    @property
    def REDIS_URL(self) -> str:
        password_part = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        return os.getenv("REDIS_URL", f"redis://{password_part}{self.REDIS_HOST}:{self.REDIS_PORT}")

    # ==========================================================================
    # API Keys and Secrets
    # ==========================================================================
    JWT_SECRET: str = field(default_factory=lambda: os.getenv("JWT_SECRET", "drivesentinel-jwt-secret-change-in-production"))
    JWT_ALGORITHM: str = field(default_factory=lambda: os.getenv("JWT_ALGORITHM", "HS256"))
    JWT_EXPIRATION_HOURS: int = field(default_factory=lambda: int(os.getenv("JWT_EXPIRATION_HOURS", "24")))

    # ==========================================================================
    # External API Keys
    # ==========================================================================
    OPENAI_API_KEY: Optional[str] = field(default_factory=lambda: os.getenv("OPENAI_API_KEY"))
    ANTHROPIC_API_KEY: Optional[str] = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY"))

    # ==========================================================================
    # External AI Services (not part of DriveSentinel, but used by services)
    # ==========================================================================
    @property
    def OLLAMA_URL(self) -> str:
        return os.getenv("OLLAMA_URL", "http://localhost:11434")

    @property
    def INFINITY_URL(self) -> str:
        return os.getenv("INFINITY_URL", "http://localhost:7997")

    @property
    def VLLM_URL(self) -> str:
        return os.getenv("VLLM_URL", "http://localhost:8080")

    @property
    def LLAMA_CPP_URL(self) -> str:
        return os.getenv("LLAMA_CPP_URL", "http://localhost:8081")

    # ==========================================================================
    # Application Settings
    # ==========================================================================
    DEBUG: bool = field(default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true")
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    ENVIRONMENT: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))

    # ==========================================================================
    # Helper Methods
    # ==========================================================================
    def get_service_url(self, service_name: str) -> str:
        """Get URL for a service by name."""
        service_map = {
            "gateway": self.GATEWAY_URL,
            "chat": self.CHAT_SERVICE_URL,
            "rag": self.RAG_SERVICE_URL,
            "orchestrator": self.ORCHESTRATOR_URL,
            "documents": self.DOCUMENTS_SERVICE_URL,
            "auth": self.AUTH_SERVICE_URL,
            "audit": self.AUDIT_SERVICE_URL,
            "errors": self.ERRORS_SERVICE_URL,
            "license": self.LICENSE_SERVICE_URL,
            "frontend": self.FRONTEND_URL,
        }
        return service_map.get(service_name.lower(), "")

    def get_service_port(self, service_name: str) -> int:
        """Get port for a service by name."""
        port_map = {
            "gateway": self.GATEWAY_PORT,
            "chat": self.CHAT_PORT,
            "rag": self.RAG_PORT,
            "orchestrator": self.ORCHESTRATOR_PORT,
            "documents": self.DOCUMENTS_PORT,
            "auth": self.AUTH_PORT,
            "audit": self.AUDIT_PORT,
            "errors": self.ERRORS_PORT,
            "license": self.LICENSE_PORT,
            "frontend": self.FRONTEND_PORT,
        }
        return port_map.get(service_name.lower(), 0)

    def to_dict(self) -> dict:
        """Export configuration as dictionary (for debugging/logging)."""
        return {
            "services": {
                "gateway": {"port": self.GATEWAY_PORT, "url": self.GATEWAY_URL},
                "chat": {"port": self.CHAT_PORT, "url": self.CHAT_SERVICE_URL},
                "rag": {"port": self.RAG_PORT, "url": self.RAG_SERVICE_URL},
                "orchestrator": {"port": self.ORCHESTRATOR_PORT, "url": self.ORCHESTRATOR_URL},
                "documents": {"port": self.DOCUMENTS_PORT, "url": self.DOCUMENTS_SERVICE_URL},
                "auth": {"port": self.AUTH_PORT, "url": self.AUTH_SERVICE_URL},
                "audit": {"port": self.AUDIT_PORT, "url": self.AUDIT_SERVICE_URL},
                "errors": {"port": self.ERRORS_PORT, "url": self.ERRORS_SERVICE_URL},
                "license": {"port": self.LICENSE_PORT, "url": self.LICENSE_SERVICE_URL},
                "frontend": {"port": self.FRONTEND_PORT, "url": self.FRONTEND_URL},
            },
            "database": {
                "host": self.DATABASE_HOST,
                "port": self.DATABASE_PORT,
                "name": self.DATABASE_NAME,
            },
            "environment": self.ENVIRONMENT,
            "debug": self.DEBUG,
        }


# Singleton instance - import this in other modules
config = ServiceConfig()

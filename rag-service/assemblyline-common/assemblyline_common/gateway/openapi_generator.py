"""
OpenAPI 3.0 Specification Generator.

Generates OpenAPI specs from FastAPI applications for API Gateway deployment.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set
from enum import Enum

logger = logging.getLogger(__name__)


class SecuritySchemeType(Enum):
    """OpenAPI security scheme types."""
    API_KEY = "apiKey"
    HTTP = "http"
    OAUTH2 = "oauth2"
    OPENID_CONNECT = "openIdConnect"


@dataclass
class OpenAPIConfig:
    """Configuration for OpenAPI spec generation."""
    # API info
    title: str = "Logic Weaver API"
    description: str = "Enterprise HL7/FHIR Integration Platform"
    version: str = "1.0.0"
    terms_of_service: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_url: Optional[str] = None
    license_name: Optional[str] = "Proprietary"
    license_url: Optional[str] = None

    # Servers
    servers: List[Dict[str, str]] = field(default_factory=lambda: [
        {"url": "https://api.logicweaver.io/v1", "description": "Production"},
        {"url": "https://staging-api.logicweaver.io/v1", "description": "Staging"},
    ])

    # Security schemes
    enable_api_key_auth: bool = True
    enable_jwt_auth: bool = True
    enable_oauth2: bool = False
    oauth2_authorization_url: Optional[str] = None
    oauth2_token_url: Optional[str] = None
    oauth2_scopes: Dict[str, str] = field(default_factory=lambda: {
        "read": "Read access",
        "write": "Write access",
        "admin": "Admin access",
    })

    # Tags
    tags: List[Dict[str, str]] = field(default_factory=lambda: [
        {"name": "Authentication", "description": "Auth endpoints"},
        {"name": "Flows", "description": "Logic Weaver flow management"},
        {"name": "Messages", "description": "Message processing"},
        {"name": "Connectors", "description": "Connector management"},
        {"name": "Tenants", "description": "Multi-tenant management"},
        {"name": "Users", "description": "User management"},
        {"name": "Audit", "description": "Audit logging"},
    ])

    # Extensions for API Gateway
    include_aws_extensions: bool = False
    include_azure_extensions: bool = False

    # Filtering
    include_paths: Set[str] = field(default_factory=set)
    exclude_paths: Set[str] = field(default_factory=lambda: {"/health", "/docs", "/openapi.json", "/redoc"})


class OpenAPIGenerator:
    """
    OpenAPI 3.0 specification generator.

    Features:
    - Generate from FastAPI app
    - Add security schemes
    - Add API Gateway extensions
    - Export to JSON/YAML
    """

    def __init__(self, config: Optional[OpenAPIConfig] = None):
        self.config = config or OpenAPIConfig()

    def generate_from_fastapi(self, app) -> Dict[str, Any]:
        """
        Generate OpenAPI spec from a FastAPI application.

        Args:
            app: FastAPI application instance

        Returns:
            OpenAPI 3.0 specification as dictionary
        """
        # Get base spec from FastAPI
        base_spec = app.openapi()

        # Enhance with our configuration
        spec = self._enhance_spec(base_spec)

        return spec

    def generate_base_spec(self) -> Dict[str, Any]:
        """
        Generate a base OpenAPI spec without an app.

        Returns:
            OpenAPI 3.0 specification skeleton
        """
        spec = {
            "openapi": "3.0.3",
            "info": self._build_info(),
            "servers": self.config.servers,
            "tags": self.config.tags,
            "paths": {},
            "components": {
                "schemas": {},
                "securitySchemes": self._build_security_schemes(),
                "responses": self._build_common_responses(),
            },
            "security": self._build_security_requirements(),
        }

        return spec

    def _enhance_spec(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance an existing OpenAPI spec."""
        # Update info
        spec["info"] = {**spec.get("info", {}), **self._build_info()}

        # Update servers
        if self.config.servers:
            spec["servers"] = self.config.servers

        # Add/update tags
        existing_tags = {t["name"] for t in spec.get("tags", [])}
        for tag in self.config.tags:
            if tag["name"] not in existing_tags:
                spec.setdefault("tags", []).append(tag)

        # Update security schemes
        spec.setdefault("components", {})
        spec["components"]["securitySchemes"] = self._build_security_schemes()

        # Add security requirements
        spec["security"] = self._build_security_requirements()

        # Filter paths
        if self.config.exclude_paths:
            spec["paths"] = {
                path: ops for path, ops in spec.get("paths", {}).items()
                if path not in self.config.exclude_paths
            }

        if self.config.include_paths:
            spec["paths"] = {
                path: ops for path, ops in spec.get("paths", {}).items()
                if path in self.config.include_paths
            }

        # Add common responses
        spec["components"]["responses"] = self._build_common_responses()

        # Add API Gateway extensions
        if self.config.include_aws_extensions:
            spec = self._add_aws_extensions(spec)

        if self.config.include_azure_extensions:
            spec = self._add_azure_extensions(spec)

        return spec

    def _build_info(self) -> Dict[str, Any]:
        """Build OpenAPI info object."""
        info = {
            "title": self.config.title,
            "description": self.config.description,
            "version": self.config.version,
        }

        if self.config.terms_of_service:
            info["termsOfService"] = self.config.terms_of_service

        if self.config.contact_name or self.config.contact_email:
            info["contact"] = {}
            if self.config.contact_name:
                info["contact"]["name"] = self.config.contact_name
            if self.config.contact_email:
                info["contact"]["email"] = self.config.contact_email
            if self.config.contact_url:
                info["contact"]["url"] = self.config.contact_url

        if self.config.license_name:
            info["license"] = {"name": self.config.license_name}
            if self.config.license_url:
                info["license"]["url"] = self.config.license_url

        return info

    def _build_security_schemes(self) -> Dict[str, Any]:
        """Build OpenAPI security schemes."""
        schemes = {}

        if self.config.enable_api_key_auth:
            schemes["ApiKeyAuth"] = {
                "type": "apiKey",
                "in": "header",
                "name": "X-API-Key",
                "description": "API key for authentication"
            }

        if self.config.enable_jwt_auth:
            schemes["BearerAuth"] = {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT",
                "description": "JWT token authentication"
            }

        if self.config.enable_oauth2 and self.config.oauth2_authorization_url:
            schemes["OAuth2"] = {
                "type": "oauth2",
                "flows": {
                    "authorizationCode": {
                        "authorizationUrl": self.config.oauth2_authorization_url,
                        "tokenUrl": self.config.oauth2_token_url or "",
                        "scopes": self.config.oauth2_scopes
                    }
                }
            }

        return schemes

    def _build_security_requirements(self) -> List[Dict[str, List[str]]]:
        """Build global security requirements."""
        requirements = []

        if self.config.enable_jwt_auth:
            requirements.append({"BearerAuth": []})

        if self.config.enable_api_key_auth:
            requirements.append({"ApiKeyAuth": []})

        return requirements

    def _build_common_responses(self) -> Dict[str, Any]:
        """Build common response definitions."""
        return {
            "BadRequest": {
                "description": "Bad Request",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "detail": {"type": "string"}
                            }
                        }
                    }
                }
            },
            "Unauthorized": {
                "description": "Unauthorized",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "detail": {"type": "string"}
                            }
                        }
                    }
                }
            },
            "Forbidden": {
                "description": "Forbidden",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "detail": {"type": "string"}
                            }
                        }
                    }
                }
            },
            "NotFound": {
                "description": "Not Found",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "detail": {"type": "string"}
                            }
                        }
                    }
                }
            },
            "TooManyRequests": {
                "description": "Too Many Requests",
                "headers": {
                    "X-RateLimit-Limit": {
                        "schema": {"type": "integer"},
                        "description": "Request limit per window"
                    },
                    "X-RateLimit-Remaining": {
                        "schema": {"type": "integer"},
                        "description": "Remaining requests in window"
                    },
                    "X-RateLimit-Reset": {
                        "schema": {"type": "integer"},
                        "description": "Unix timestamp when window resets"
                    },
                    "Retry-After": {
                        "schema": {"type": "integer"},
                        "description": "Seconds to wait before retry"
                    }
                },
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "detail": {"type": "string"}
                            }
                        }
                    }
                }
            },
            "InternalServerError": {
                "description": "Internal Server Error",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "detail": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }

    def _add_aws_extensions(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Add AWS API Gateway extensions to spec."""
        # Add x-amazon-apigateway-integration to each operation
        for path, methods in spec.get("paths", {}).items():
            for method, operation in methods.items():
                if method in ("get", "post", "put", "patch", "delete"):
                    operation["x-amazon-apigateway-integration"] = {
                        "type": "http_proxy",
                        "httpMethod": method.upper(),
                        "uri": f"{{stageVariables.backendUrl}}{path}",
                        "passthroughBehavior": "when_no_match",
                        "timeoutInMillis": 29000
                    }

        # Add CORS configuration
        spec["x-amazon-apigateway-cors"] = {
            "allowOrigins": ["*"],
            "allowMethods": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            "allowHeaders": ["Content-Type", "Authorization", "X-API-Key"],
            "maxAge": 300
        }

        return spec

    def _add_azure_extensions(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Add Azure API Management extensions to spec."""
        # Add x-ms-paths for URL template parameters
        spec["x-ms-paths"] = {}

        # Add policies placeholder
        spec["x-ms-apim-policies"] = {
            "inbound": [
                {"name": "rate-limit", "calls": 1000, "renewal-period": 60},
                {"name": "quota", "calls": 10000, "renewal-period": 86400}
            ],
            "backend": [],
            "outbound": [],
            "on-error": []
        }

        return spec

    def export_json(self, spec: Dict[str, Any], pretty: bool = True) -> str:
        """Export spec to JSON string."""
        if pretty:
            return json.dumps(spec, indent=2)
        return json.dumps(spec)

    def export_yaml(self, spec: Dict[str, Any]) -> str:
        """Export spec to YAML string."""
        try:
            import yaml
            return yaml.dump(spec, default_flow_style=False, sort_keys=False)
        except ImportError:
            logger.warning("PyYAML not available, falling back to JSON")
            return self.export_json(spec)


# Convenience function
def generate_openapi_spec(
    app=None,
    config: Optional[OpenAPIConfig] = None
) -> Dict[str, Any]:
    """
    Generate OpenAPI spec.

    Args:
        app: Optional FastAPI application
        config: Optional configuration

    Returns:
        OpenAPI 3.0 specification
    """
    generator = OpenAPIGenerator(config)

    if app:
        return generator.generate_from_fastapi(app)
    return generator.generate_base_spec()

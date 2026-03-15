"""
API Gateway Integration for Logic Weaver.

Provides:
- OpenAPI 3.0 specification generation
- AWS API Gateway integration
- Azure API Management integration
"""

from .openapi_generator import (
    OpenAPIGenerator,
    OpenAPIConfig,
    generate_openapi_spec,
)
from .aws_api_gateway import (
    AWSAPIGatewayIntegration,
    AWSGatewayConfig,
)
from .azure_apim import (
    AzureAPIMIntegration,
    AzureAPIMConfig,
)

__all__ = [
    # OpenAPI
    "OpenAPIGenerator",
    "OpenAPIConfig",
    "generate_openapi_spec",
    # AWS
    "AWSAPIGatewayIntegration",
    "AWSGatewayConfig",
    # Azure
    "AzureAPIMIntegration",
    "AzureAPIMConfig",
]

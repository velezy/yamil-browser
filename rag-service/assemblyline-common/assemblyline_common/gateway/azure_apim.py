"""
Azure API Management (APIM) Integration.

Provides:
- APIM API configuration
- Policy management
- Subscription and product management
- Multi-region deployment support
"""

import json
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)

# Try to import Azure SDK
try:
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.apimanagement import ApiManagementClient
    from azure.mgmt.apimanagement.models import (
        ApiCreateOrUpdateParameter,
        AuthenticationSettingsContract,
        SubscriptionKeyParameterNamesContract,
    )
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logger.warning("Azure SDK not available, APIM features disabled")


class APIProtocol(Enum):
    """API protocols supported by APIM."""
    HTTPS = "https"
    HTTP = "http"
    WS = "ws"
    WSS = "wss"


class SubscriptionRequired(Enum):
    """Subscription requirement options."""
    REQUIRED = True
    NOT_REQUIRED = False


class ProductState(Enum):
    """Product state options."""
    PUBLISHED = "published"
    NOT_PUBLISHED = "notPublished"


@dataclass
class APIMPolicy:
    """APIM policy configuration."""
    name: str
    scope: str = "api"  # api, operation, product, global
    inbound: List[str] = field(default_factory=list)
    backend: List[str] = field(default_factory=list)
    outbound: List[str] = field(default_factory=list)
    on_error: List[str] = field(default_factory=list)


@dataclass
class RateLimitPolicy:
    """Rate limit policy settings."""
    calls: int = 1000
    renewal_period: int = 60  # seconds
    counter_key: str = "@(context.Subscription.Id)"


@dataclass
class QuotaPolicy:
    """Quota policy settings."""
    calls: int = 100000
    renewal_period: int = 604800  # 1 week in seconds
    counter_key: str = "@(context.Subscription.Id)"


@dataclass
class CachePolicy:
    """Caching policy settings."""
    duration: int = 3600  # seconds
    vary_by_developer: bool = False
    vary_by_developer_groups: bool = False
    vary_by_headers: List[str] = field(default_factory=lambda: ["Accept", "Accept-Charset"])
    vary_by_query: List[str] = field(default_factory=list)


@dataclass
class JWTValidationPolicy:
    """JWT validation policy settings."""
    header_name: str = "Authorization"
    scheme: str = "Bearer"
    issuers: List[str] = field(default_factory=list)
    audiences: List[str] = field(default_factory=list)
    openid_config_url: Optional[str] = None
    required_claims: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class ProductConfig:
    """APIM product configuration."""
    name: str
    display_name: str
    description: str = ""
    subscription_required: bool = True
    approval_required: bool = True
    subscriptions_limit: Optional[int] = None
    state: ProductState = ProductState.PUBLISHED
    terms: str = ""


@dataclass
class BackendConfig:
    """Backend service configuration."""
    name: str
    url: str
    protocol: str = "http"
    validate_certificate: bool = True
    credentials: Optional[Dict[str, str]] = None  # header, query, or certificate


@dataclass
class AzureAPIMConfig:
    """Configuration for Azure API Management."""
    # APIM instance
    subscription_id: str = ""
    resource_group: str = ""
    service_name: str = ""

    # API settings
    api_id: str = "logic-weaver-api"
    api_display_name: str = "Logic Weaver API"
    api_description: str = "Enterprise HL7/FHIR Integration Platform"
    api_path: str = "v1"
    api_protocols: List[APIProtocol] = field(default_factory=lambda: [APIProtocol.HTTPS])

    # Backend
    backend_url: str = "https://api.logicweaver.internal"
    backends: List[BackendConfig] = field(default_factory=list)

    # Authentication
    subscription_required: bool = True
    subscription_key_header: str = "Ocp-Apim-Subscription-Key"
    subscription_key_query: str = "subscription-key"
    jwt_validation: Optional[JWTValidationPolicy] = None

    # Products
    products: List[ProductConfig] = field(default_factory=lambda: [
        ProductConfig(
            name="starter",
            display_name="Starter",
            description="Starter tier with limited rate limits",
            subscription_required=True,
            approval_required=False,
        ),
        ProductConfig(
            name="enterprise",
            display_name="Enterprise",
            description="Enterprise tier with higher limits",
            subscription_required=True,
            approval_required=True,
        ),
    ])

    # Policies
    rate_limit: RateLimitPolicy = field(default_factory=RateLimitPolicy)
    quota: QuotaPolicy = field(default_factory=QuotaPolicy)
    cache: Optional[CachePolicy] = None

    # Multi-region
    regions: List[str] = field(default_factory=lambda: ["eastus"])

    # Networking
    vnet_type: str = "None"  # None, External, Internal
    vnet_resource_id: Optional[str] = None


class PolicyBuilder:
    """Builder for APIM XML policies."""

    def __init__(self):
        self.inbound = []
        self.backend = []
        self.outbound = []
        self.on_error = []

    def add_rate_limit(self, config: RateLimitPolicy) -> "PolicyBuilder":
        """Add rate limiting policy."""
        self.inbound.append(
            f'<rate-limit calls="{config.calls}" '
            f'renewal-period="{config.renewal_period}" '
            f'counter-key="{config.counter_key}" />'
        )
        return self

    def add_quota(self, config: QuotaPolicy) -> "PolicyBuilder":
        """Add quota policy."""
        self.inbound.append(
            f'<quota calls="{config.calls}" '
            f'renewal-period="{config.renewal_period}" '
            f'counter-key="{config.counter_key}" />'
        )
        return self

    def add_jwt_validation(self, config: JWTValidationPolicy) -> "PolicyBuilder":
        """Add JWT validation policy."""
        policy = ['<validate-jwt header-name="' + config.header_name + '" '
                  'failed-validation-httpcode="401" '
                  'failed-validation-error-message="Unauthorized">']

        if config.openid_config_url:
            policy.append(f'<openid-config url="{config.openid_config_url}" />')

        if config.issuers:
            policy.append('<issuers>')
            for issuer in config.issuers:
                policy.append(f'<issuer>{issuer}</issuer>')
            policy.append('</issuers>')

        if config.audiences:
            policy.append('<audiences>')
            for audience in config.audiences:
                policy.append(f'<audience>{audience}</audience>')
            policy.append('</audiences>')

        if config.required_claims:
            policy.append('<required-claims>')
            for claim_name, values in config.required_claims.items():
                policy.append(f'<claim name="{claim_name}" match="any">')
                for value in values:
                    policy.append(f'<value>{value}</value>')
                policy.append('</claim>')
            policy.append('</required-claims>')

        policy.append('</validate-jwt>')

        self.inbound.append('\n'.join(policy))
        return self

    def add_cors(
        self,
        origins: List[str],
        methods: List[str],
        headers: List[str],
        max_age: int = 3600
    ) -> "PolicyBuilder":
        """Add CORS policy."""
        origin_elements = '\n'.join(f'<origin>{o}</origin>' for o in origins)
        method_elements = '\n'.join(f'<method>{m}</method>' for m in methods)
        header_elements = '\n'.join(f'<header>{h}</header>' for h in headers)

        self.inbound.append(f'''
<cors allow-credentials="true">
    <allowed-origins>
        {origin_elements}
    </allowed-origins>
    <allowed-methods preflight-result-max-age="{max_age}">
        {method_elements}
    </allowed-methods>
    <allowed-headers>
        {header_elements}
    </allowed-headers>
</cors>''')
        return self

    def add_cache_lookup(self, config: CachePolicy) -> "PolicyBuilder":
        """Add cache lookup policy."""
        vary_headers = ' '.join(f'<header>{h}</header>' for h in config.vary_by_headers)
        vary_query = ' '.join(f'<query-parameter>{q}</query-parameter>' for q in config.vary_by_query)

        self.inbound.append(f'''
<cache-lookup vary-by-developer="{str(config.vary_by_developer).lower()}"
              vary-by-developer-groups="{str(config.vary_by_developer_groups).lower()}">
    <vary-by-header>{vary_headers}</vary-by-header>
    <vary-by-query-parameter>{vary_query}</vary-by-query-parameter>
</cache-lookup>''')
        return self

    def add_cache_store(self, duration: int) -> "PolicyBuilder":
        """Add cache store policy."""
        self.outbound.append(f'<cache-store duration="{duration}" />')
        return self

    def add_set_header(
        self,
        name: str,
        value: str,
        exists_action: str = "override"
    ) -> "PolicyBuilder":
        """Add set-header policy."""
        self.inbound.append(
            f'<set-header name="{name}" exists-action="{exists_action}">'
            f'<value>{value}</value></set-header>'
        )
        return self

    def add_set_backend_service(self, url: str) -> "PolicyBuilder":
        """Add set-backend-service policy."""
        self.backend.append(f'<set-backend-service base-url="{url}" />')
        return self

    def add_rewrite_uri(self, template: str) -> "PolicyBuilder":
        """Add rewrite-uri policy."""
        self.backend.append(f'<rewrite-uri template="{template}" />')
        return self

    def add_log_to_eventhub(
        self,
        logger_id: str,
        partition_id: str = "0"
    ) -> "PolicyBuilder":
        """Add log-to-eventhub policy."""
        self.outbound.append(f'''
<log-to-eventhub logger-id="{logger_id}" partition-id="{partition_id}">
    @{{
        var responseBody = context.Response.Body.As<string>();
        return new JObject(
            new JProperty("timestamp", DateTime.UtcNow.ToString("o")),
            new JProperty("method", context.Request.Method),
            new JProperty("url", context.Request.Url.ToString()),
            new JProperty("status", context.Response.StatusCode),
            new JProperty("responseBody", responseBody)
        ).ToString();
    }}
</log-to-eventhub>''')
        return self

    def add_retry(
        self,
        count: int = 3,
        interval: int = 10,
        max_interval: int = 90,
        delta: int = 10,
        first_fast_retry: bool = True
    ) -> "PolicyBuilder":
        """Add retry policy for backend."""
        self.backend.append(f'''
<retry condition="@(context.Response.StatusCode >= 500)"
       count="{count}"
       interval="{interval}"
       max-interval="{max_interval}"
       delta="{delta}"
       first-fast-retry="{str(first_fast_retry).lower()}">
    <forward-request buffer-request-body="true" />
</retry>''')
        return self

    def add_error_response(
        self,
        status_code: int = 500,
        reason: str = "Internal Server Error"
    ) -> "PolicyBuilder":
        """Add error response handling."""
        self.on_error.append(f'''
<return-response>
    <set-status code="{status_code}" reason="{reason}" />
    <set-header name="Content-Type" exists-action="override">
        <value>application/json</value>
    </set-header>
    <set-body>@{{
        return new JObject(
            new JProperty("error", context.LastError.Message),
            new JProperty("source", context.LastError.Source),
            new JProperty("reason", context.LastError.Reason)
        ).ToString();
    }}</set-body>
</return-response>''')
        return self

    def build(self) -> str:
        """Build the complete policy XML."""
        inbound_policies = '\n        '.join(self.inbound) if self.inbound else '<base />'
        backend_policies = '\n        '.join(self.backend) if self.backend else '<base />'
        outbound_policies = '\n        '.join(self.outbound) if self.outbound else '<base />'
        on_error_policies = '\n        '.join(self.on_error) if self.on_error else '<base />'

        return f'''<policies>
    <inbound>
        {inbound_policies}
    </inbound>
    <backend>
        {backend_policies}
    </backend>
    <outbound>
        {outbound_policies}
    </outbound>
    <on-error>
        {on_error_policies}
    </on-error>
</policies>'''


class AzureAPIMIntegration:
    """
    Azure API Management integration manager.

    Features:
    - Import APIs from OpenAPI specs
    - Configure policies (rate limiting, JWT validation, CORS)
    - Manage products and subscriptions
    - Multi-region deployment
    """

    def __init__(self, config: Optional[AzureAPIMConfig] = None):
        self.config = config or AzureAPIMConfig()
        self._client = None

    def _get_client(self):
        """Get or create APIM client."""
        if not AZURE_AVAILABLE:
            raise RuntimeError("Azure SDK is required for APIM integration")

        if self._client is None:
            credential = DefaultAzureCredential()
            self._client = ApiManagementClient(
                credential,
                self.config.subscription_id
            )
        return self._client

    def import_api_from_openapi(
        self,
        openapi_spec: Dict[str, Any],
        api_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Import an API from OpenAPI specification.

        Args:
            openapi_spec: OpenAPI 3.0 specification
            api_id: Optional API ID (uses config default if not provided)

        Returns:
            API details
        """
        client = self._get_client()
        api_id = api_id or self.config.api_id

        # Add APIM-specific extensions
        spec = self._add_apim_extensions(openapi_spec)

        try:
            # Import API
            result = client.api.create_or_update(
                resource_group_name=self.config.resource_group,
                service_name=self.config.service_name,
                api_id=api_id,
                parameters={
                    "properties": {
                        "format": "openapi+json",
                        "value": json.dumps(spec),
                        "path": self.config.api_path,
                        "displayName": self.config.api_display_name,
                        "description": self.config.api_description,
                        "protocols": [p.value for p in self.config.api_protocols],
                        "serviceUrl": self.config.backend_url,
                        "subscriptionRequired": self.config.subscription_required,
                        "subscriptionKeyParameterNames": {
                            "header": self.config.subscription_key_header,
                            "query": self.config.subscription_key_query,
                        },
                    }
                }
            )

            logger.info(f"Imported API: {api_id}")
            return {
                "id": result.id,
                "name": result.name,
                "path": result.path,
                "service_url": result.service_url,
            }

        except Exception as e:
            logger.error(f"Failed to import API: {e}")
            raise

    def _add_apim_extensions(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Add Azure APIM extensions to OpenAPI spec."""
        spec = spec.copy()

        # Add x-ms-paths for complex URL patterns if needed
        if "x-ms-paths" not in spec:
            spec["x-ms-paths"] = {}

        return spec

    def set_api_policy(
        self,
        policy_xml: str,
        api_id: Optional[str] = None,
        operation_id: Optional[str] = None
    ) -> None:
        """
        Set policy for an API or operation.

        Args:
            policy_xml: Policy XML string
            api_id: API ID (uses config default if not provided)
            operation_id: Optional operation ID for operation-level policies
        """
        client = self._get_client()
        api_id = api_id or self.config.api_id

        try:
            if operation_id:
                client.api_operation_policy.create_or_update(
                    resource_group_name=self.config.resource_group,
                    service_name=self.config.service_name,
                    api_id=api_id,
                    operation_id=operation_id,
                    policy_id="policy",
                    parameters={
                        "properties": {
                            "format": "rawxml",
                            "value": policy_xml,
                        }
                    }
                )
            else:
                client.api_policy.create_or_update(
                    resource_group_name=self.config.resource_group,
                    service_name=self.config.service_name,
                    api_id=api_id,
                    policy_id="policy",
                    parameters={
                        "properties": {
                            "format": "rawxml",
                            "value": policy_xml,
                        }
                    }
                )

            logger.info(f"Set policy for API: {api_id}")

        except Exception as e:
            logger.error(f"Failed to set policy: {e}")
            raise

    def create_product(self, config: ProductConfig) -> Dict[str, Any]:
        """
        Create an APIM product.

        Args:
            config: Product configuration

        Returns:
            Product details
        """
        client = self._get_client()

        try:
            result = client.product.create_or_update(
                resource_group_name=self.config.resource_group,
                service_name=self.config.service_name,
                product_id=config.name,
                parameters={
                    "properties": {
                        "displayName": config.display_name,
                        "description": config.description,
                        "subscriptionRequired": config.subscription_required,
                        "approvalRequired": config.approval_required,
                        "subscriptionsLimit": config.subscriptions_limit,
                        "state": config.state.value,
                        "terms": config.terms,
                    }
                }
            )

            logger.info(f"Created product: {config.name}")
            return {
                "id": result.id,
                "name": result.name,
                "display_name": result.display_name,
            }

        except Exception as e:
            logger.error(f"Failed to create product: {e}")
            raise

    def add_api_to_product(
        self,
        product_id: str,
        api_id: Optional[str] = None
    ) -> None:
        """
        Add an API to a product.

        Args:
            product_id: Product ID
            api_id: API ID (uses config default if not provided)
        """
        client = self._get_client()
        api_id = api_id or self.config.api_id

        try:
            client.product_api.create_or_update(
                resource_group_name=self.config.resource_group,
                service_name=self.config.service_name,
                product_id=product_id,
                api_id=api_id,
            )

            logger.info(f"Added API {api_id} to product {product_id}")

        except Exception as e:
            logger.error(f"Failed to add API to product: {e}")
            raise

    def create_subscription(
        self,
        product_id: str,
        subscription_name: str,
        display_name: str,
        user_id: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Create a subscription to a product.

        Args:
            product_id: Product ID
            subscription_name: Subscription name
            display_name: Display name
            user_id: Optional user ID

        Returns:
            Subscription details including keys
        """
        client = self._get_client()

        try:
            result = client.subscription.create_or_update(
                resource_group_name=self.config.resource_group,
                service_name=self.config.service_name,
                sid=subscription_name,
                parameters={
                    "properties": {
                        "scope": f"/products/{product_id}",
                        "displayName": display_name,
                        "ownerId": f"/users/{user_id}" if user_id else None,
                        "state": "active",
                    }
                }
            )

            # Get subscription keys
            keys = client.subscription.list_secrets(
                resource_group_name=self.config.resource_group,
                service_name=self.config.service_name,
                sid=subscription_name,
            )

            logger.info(f"Created subscription: {subscription_name}")
            return {
                "id": result.id,
                "name": result.name,
                "primary_key": keys.primary_key,
                "secondary_key": keys.secondary_key,
            }

        except Exception as e:
            logger.error(f"Failed to create subscription: {e}")
            raise

    def create_backend(self, config: BackendConfig) -> Dict[str, Any]:
        """
        Create a backend service configuration.

        Args:
            config: Backend configuration

        Returns:
            Backend details
        """
        client = self._get_client()

        try:
            params = {
                "properties": {
                    "url": config.url,
                    "protocol": config.protocol,
                    "tls": {
                        "validateCertificateChain": config.validate_certificate,
                        "validateCertificateName": config.validate_certificate,
                    }
                }
            }

            if config.credentials:
                params["properties"]["credentials"] = config.credentials

            result = client.backend.create_or_update(
                resource_group_name=self.config.resource_group,
                service_name=self.config.service_name,
                backend_id=config.name,
                parameters=params
            )

            logger.info(f"Created backend: {config.name}")
            return {
                "id": result.id,
                "name": result.name,
                "url": result.url,
            }

        except Exception as e:
            logger.error(f"Failed to create backend: {e}")
            raise

    def build_default_policy(self) -> str:
        """
        Build default API policy based on configuration.

        Returns:
            Policy XML string
        """
        builder = PolicyBuilder()

        # Add rate limiting
        builder.add_rate_limit(self.config.rate_limit)

        # Add quota
        builder.add_quota(self.config.quota)

        # Add JWT validation if configured
        if self.config.jwt_validation:
            builder.add_jwt_validation(self.config.jwt_validation)

        # Add CORS
        builder.add_cors(
            origins=["*"],
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            headers=["Content-Type", "Authorization", self.config.subscription_key_header],
        )

        # Add caching if configured
        if self.config.cache:
            builder.add_cache_lookup(self.config.cache)
            builder.add_cache_store(self.config.cache.duration)

        # Add retry for backend
        builder.add_retry()

        # Add error handling
        builder.add_error_response()

        return builder.build()

    def deploy_full_api(
        self,
        openapi_spec: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Full deployment workflow: import API, set policies, create products.

        Args:
            openapi_spec: OpenAPI 3.0 specification

        Returns:
            Deployment details
        """
        result = {
            "api": None,
            "products": [],
            "subscriptions": [],
            "backends": [],
            "policy_applied": False,
        }

        # Import API
        api_details = self.import_api_from_openapi(openapi_spec)
        result["api"] = api_details

        # Set default policy
        policy = self.build_default_policy()
        self.set_api_policy(policy)
        result["policy_applied"] = True

        # Create products
        for product_config in self.config.products:
            product = self.create_product(product_config)
            result["products"].append(product)

            # Add API to product
            self.add_api_to_product(product_config.name)

        # Create backends
        for backend_config in self.config.backends:
            backend = self.create_backend(backend_config)
            result["backends"].append(backend)

        logger.info(f"Full APIM deployment complete: {result}")
        return result


# Convenience function
def create_apim_api(
    openapi_spec: Dict[str, Any],
    config: Optional[AzureAPIMConfig] = None
) -> Dict[str, Any]:
    """
    Create and configure an API in Azure APIM.

    Args:
        openapi_spec: OpenAPI 3.0 specification
        config: Optional configuration

    Returns:
        Deployment details
    """
    apim = AzureAPIMIntegration(config)
    return apim.deploy_full_api(openapi_spec)

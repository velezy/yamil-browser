"""
AWS API Gateway Integration.

Provides:
- REST API deployment
- Lambda authorizer configuration
- Stage management
- Usage plans and API keys
- CloudWatch logging
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)

# Try to import boto3
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 not available, AWS API Gateway features disabled")


class AuthorizerType(Enum):
    """API Gateway authorizer types."""
    TOKEN = "TOKEN"
    REQUEST = "REQUEST"
    COGNITO = "COGNITO_USER_POOLS"
    JWT = "JWT"


class EndpointType(Enum):
    """API Gateway endpoint types."""
    REGIONAL = "REGIONAL"
    EDGE = "EDGE"
    PRIVATE = "PRIVATE"


class IntegrationType(Enum):
    """API Gateway integration types."""
    HTTP = "HTTP"
    HTTP_PROXY = "HTTP_PROXY"
    AWS = "AWS"
    AWS_PROXY = "AWS_PROXY"
    MOCK = "MOCK"


@dataclass
class LambdaAuthorizerConfig:
    """Configuration for Lambda authorizer."""
    name: str
    function_arn: str
    authorizer_type: AuthorizerType = AuthorizerType.TOKEN
    identity_source: str = "method.request.header.Authorization"
    identity_validation_expression: Optional[str] = None
    result_ttl_seconds: int = 300


@dataclass
class CognitoAuthorizerConfig:
    """Configuration for Cognito authorizer."""
    name: str
    user_pool_arns: List[str] = field(default_factory=list)
    identity_source: str = "method.request.header.Authorization"


@dataclass
class ThrottleSettings:
    """Throttle settings for usage plans."""
    rate_limit: float = 1000.0  # requests per second
    burst_limit: int = 2000


@dataclass
class QuotaSettings:
    """Quota settings for usage plans."""
    limit: int = 100000
    period: str = "MONTH"  # DAY, WEEK, MONTH


@dataclass
class UsagePlanConfig:
    """Configuration for API Gateway usage plan."""
    name: str
    description: str = ""
    throttle: ThrottleSettings = field(default_factory=ThrottleSettings)
    quota: QuotaSettings = field(default_factory=QuotaSettings)
    api_stages: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class StageConfig:
    """Configuration for API Gateway stage."""
    name: str
    description: str = ""
    variables: Dict[str, str] = field(default_factory=dict)
    cache_enabled: bool = False
    cache_size: str = "0.5"  # GB
    cache_ttl: int = 300
    throttle_rate: float = 10000.0
    throttle_burst: int = 5000
    logging_level: str = "INFO"  # OFF, ERROR, INFO
    data_trace_enabled: bool = False
    metrics_enabled: bool = True
    xray_tracing_enabled: bool = True


@dataclass
class AWSGatewayConfig:
    """Configuration for AWS API Gateway integration."""
    # API settings
    api_name: str = "LogicWeaver-API"
    api_description: str = "Logic Weaver Enterprise Integration Platform"
    endpoint_type: EndpointType = EndpointType.REGIONAL

    # Regional settings
    region: str = "us-east-1"

    # Backend
    backend_url: str = "https://api.logicweaver.internal"
    vpc_link_id: Optional[str] = None

    # Stages
    stages: List[StageConfig] = field(default_factory=lambda: [
        StageConfig(
            name="prod",
            description="Production",
            variables={"backendUrl": "https://api.logicweaver.io"},
        ),
        StageConfig(
            name="staging",
            description="Staging",
            variables={"backendUrl": "https://staging-api.logicweaver.io"},
        ),
    ])

    # Authentication
    lambda_authorizer: Optional[LambdaAuthorizerConfig] = None
    cognito_authorizer: Optional[CognitoAuthorizerConfig] = None

    # Usage plans
    usage_plans: List[UsagePlanConfig] = field(default_factory=list)

    # CORS
    cors_enabled: bool = True
    cors_allow_origins: List[str] = field(default_factory=lambda: ["*"])
    cors_allow_methods: List[str] = field(default_factory=lambda: [
        "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"
    ])
    cors_allow_headers: List[str] = field(default_factory=lambda: [
        "Content-Type", "Authorization", "X-API-Key", "X-Amz-Date",
        "X-Amz-Security-Token", "X-Requested-With"
    ])

    # Logging
    access_log_format: str = '{ "requestId":"$context.requestId", "ip": "$context.identity.sourceIp", "caller":"$context.identity.caller", "user":"$context.identity.user","requestTime":"$context.requestTime", "httpMethod":"$context.httpMethod","resourcePath":"$context.resourcePath", "status":"$context.status","protocol":"$context.protocol", "responseLength":"$context.responseLength" }'


class AWSAPIGatewayIntegration:
    """
    AWS API Gateway integration manager.

    Features:
    - Create and update REST APIs
    - Configure Lambda and Cognito authorizers
    - Manage stages and deployments
    - Set up usage plans and API keys
    - Configure CloudWatch logging
    """

    def __init__(self, config: Optional[AWSGatewayConfig] = None):
        self.config = config or AWSGatewayConfig()
        self._client = None
        self._api_id: Optional[str] = None

    def _get_client(self):
        """Get or create API Gateway client."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 is required for AWS API Gateway integration")

        if self._client is None:
            self._client = boto3.client(
                "apigateway",
                region_name=self.config.region
            )
        return self._client

    def create_rest_api(self, openapi_spec: Dict[str, Any]) -> str:
        """
        Create a REST API from OpenAPI specification.

        Args:
            openapi_spec: OpenAPI 3.0 specification

        Returns:
            API ID
        """
        client = self._get_client()

        # Add API Gateway extensions if not present
        spec = self._add_gateway_extensions(openapi_spec)

        try:
            response = client.import_rest_api(
                failOnWarnings=False,
                parameters={
                    "endpointConfigurationTypes": self.config.endpoint_type.value
                },
                body=json.dumps(spec).encode()
            )

            self._api_id = response["id"]
            logger.info(f"Created REST API: {self._api_id}")

            return self._api_id

        except ClientError as e:
            logger.error(f"Failed to create REST API: {e}")
            raise

    def update_rest_api(self, api_id: str, openapi_spec: Dict[str, Any]) -> None:
        """
        Update an existing REST API.

        Args:
            api_id: Existing API ID
            openapi_spec: Updated OpenAPI specification
        """
        client = self._get_client()
        self._api_id = api_id

        spec = self._add_gateway_extensions(openapi_spec)

        try:
            client.put_rest_api(
                restApiId=api_id,
                mode="overwrite",
                failOnWarnings=False,
                body=json.dumps(spec).encode()
            )

            logger.info(f"Updated REST API: {api_id}")

        except ClientError as e:
            logger.error(f"Failed to update REST API: {e}")
            raise

    def _add_gateway_extensions(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Add AWS API Gateway extensions to OpenAPI spec."""
        spec = spec.copy()

        # Add x-amazon-apigateway-request-validators
        spec["x-amazon-apigateway-request-validators"] = {
            "all": {
                "validateRequestBody": True,
                "validateRequestParameters": True
            },
            "params-only": {
                "validateRequestBody": False,
                "validateRequestParameters": True
            }
        }
        spec["x-amazon-apigateway-request-validator"] = "all"

        # Add CORS gateway responses
        if self.config.cors_enabled:
            spec["x-amazon-apigateway-gateway-responses"] = {
                "DEFAULT_4XX": {
                    "responseParameters": {
                        "gatewayresponse.header.Access-Control-Allow-Origin": f"'{self.config.cors_allow_origins[0]}'",
                        "gatewayresponse.header.Access-Control-Allow-Headers": f"'{','.join(self.config.cors_allow_headers)}'"
                    }
                },
                "DEFAULT_5XX": {
                    "responseParameters": {
                        "gatewayresponse.header.Access-Control-Allow-Origin": f"'{self.config.cors_allow_origins[0]}'",
                    }
                }
            }

        # Add integrations to each path/method
        for path, methods in spec.get("paths", {}).items():
            for method, operation in methods.items():
                if method.lower() in ("get", "post", "put", "patch", "delete"):
                    # HTTP proxy integration
                    integration = {
                        "type": "HTTP_PROXY",
                        "httpMethod": method.upper(),
                        "uri": f"${{stageVariables.backendUrl}}{path}",
                        "passthroughBehavior": "when_no_match",
                        "timeoutInMillis": 29000,
                        "connectionType": "INTERNET"
                    }

                    # Use VPC Link if configured
                    if self.config.vpc_link_id:
                        integration["connectionType"] = "VPC_LINK"
                        integration["connectionId"] = self.config.vpc_link_id

                    operation["x-amazon-apigateway-integration"] = integration

                elif method.lower() == "options" and self.config.cors_enabled:
                    # CORS preflight
                    operation["x-amazon-apigateway-integration"] = {
                        "type": "MOCK",
                        "requestTemplates": {
                            "application/json": '{"statusCode": 200}'
                        },
                        "responses": {
                            "default": {
                                "statusCode": "200",
                                "responseParameters": {
                                    "method.response.header.Access-Control-Allow-Headers": f"'{','.join(self.config.cors_allow_headers)}'",
                                    "method.response.header.Access-Control-Allow-Methods": f"'{','.join(self.config.cors_allow_methods)}'",
                                    "method.response.header.Access-Control-Allow-Origin": f"'{self.config.cors_allow_origins[0]}'"
                                }
                            }
                        }
                    }

        return spec

    def create_lambda_authorizer(
        self,
        api_id: str,
        config: LambdaAuthorizerConfig
    ) -> str:
        """
        Create a Lambda authorizer.

        Args:
            api_id: API Gateway REST API ID
            config: Authorizer configuration

        Returns:
            Authorizer ID
        """
        client = self._get_client()

        try:
            params = {
                "restApiId": api_id,
                "name": config.name,
                "type": config.authorizer_type.value,
                "authorizerUri": f"arn:aws:apigateway:{self.config.region}:lambda:path/2015-03-31/functions/{config.function_arn}/invocations",
                "identitySource": config.identity_source,
                "authorizerResultTtlInSeconds": config.result_ttl_seconds,
            }

            if config.identity_validation_expression:
                params["identityValidationExpression"] = config.identity_validation_expression

            response = client.create_authorizer(**params)

            authorizer_id = response["id"]
            logger.info(f"Created Lambda authorizer: {authorizer_id}")

            return authorizer_id

        except ClientError as e:
            logger.error(f"Failed to create Lambda authorizer: {e}")
            raise

    def create_cognito_authorizer(
        self,
        api_id: str,
        config: CognitoAuthorizerConfig
    ) -> str:
        """
        Create a Cognito User Pools authorizer.

        Args:
            api_id: API Gateway REST API ID
            config: Authorizer configuration

        Returns:
            Authorizer ID
        """
        client = self._get_client()

        try:
            response = client.create_authorizer(
                restApiId=api_id,
                name=config.name,
                type="COGNITO_USER_POOLS",
                providerARNs=config.user_pool_arns,
                identitySource=config.identity_source,
            )

            authorizer_id = response["id"]
            logger.info(f"Created Cognito authorizer: {authorizer_id}")

            return authorizer_id

        except ClientError as e:
            logger.error(f"Failed to create Cognito authorizer: {e}")
            raise

    def create_deployment(
        self,
        api_id: str,
        stage_config: StageConfig,
        description: str = ""
    ) -> str:
        """
        Create a deployment and stage.

        Args:
            api_id: API Gateway REST API ID
            stage_config: Stage configuration
            description: Deployment description

        Returns:
            Deployment ID
        """
        client = self._get_client()

        try:
            # Create deployment
            deploy_response = client.create_deployment(
                restApiId=api_id,
                stageName=stage_config.name,
                stageDescription=stage_config.description,
                description=description or f"Deployment to {stage_config.name}",
                variables=stage_config.variables,
                cacheClusterEnabled=stage_config.cache_enabled,
                cacheClusterSize=stage_config.cache_size if stage_config.cache_enabled else None,
                tracingEnabled=stage_config.xray_tracing_enabled,
            )

            deployment_id = deploy_response["id"]

            # Update stage settings
            self._update_stage_settings(api_id, stage_config)

            logger.info(f"Created deployment {deployment_id} for stage {stage_config.name}")

            return deployment_id

        except ClientError as e:
            logger.error(f"Failed to create deployment: {e}")
            raise

    def _update_stage_settings(self, api_id: str, stage_config: StageConfig) -> None:
        """Update stage settings."""
        client = self._get_client()

        patch_operations = [
            {
                "op": "replace",
                "path": "/*/*/throttling/rateLimit",
                "value": str(stage_config.throttle_rate)
            },
            {
                "op": "replace",
                "path": "/*/*/throttling/burstLimit",
                "value": str(stage_config.throttle_burst)
            },
            {
                "op": "replace",
                "path": "/*/*/logging/loglevel",
                "value": stage_config.logging_level
            },
            {
                "op": "replace",
                "path": "/*/*/logging/dataTrace",
                "value": str(stage_config.data_trace_enabled).lower()
            },
            {
                "op": "replace",
                "path": "/*/*/metrics/enabled",
                "value": str(stage_config.metrics_enabled).lower()
            },
        ]

        if stage_config.cache_enabled:
            patch_operations.append({
                "op": "replace",
                "path": "/*/*/caching/ttlInSeconds",
                "value": str(stage_config.cache_ttl)
            })

        try:
            client.update_stage(
                restApiId=api_id,
                stageName=stage_config.name,
                patchOperations=patch_operations
            )
        except ClientError as e:
            logger.warning(f"Failed to update stage settings: {e}")

    def create_usage_plan(
        self,
        api_id: str,
        stage_name: str,
        config: UsagePlanConfig
    ) -> str:
        """
        Create a usage plan.

        Args:
            api_id: API Gateway REST API ID
            stage_name: Stage name to associate
            config: Usage plan configuration

        Returns:
            Usage plan ID
        """
        client = self._get_client()

        try:
            response = client.create_usage_plan(
                name=config.name,
                description=config.description,
                apiStages=[
                    {
                        "apiId": api_id,
                        "stage": stage_name
                    }
                ],
                throttle={
                    "rateLimit": config.throttle.rate_limit,
                    "burstLimit": config.throttle.burst_limit
                },
                quota={
                    "limit": config.quota.limit,
                    "period": config.quota.period
                }
            )

            usage_plan_id = response["id"]
            logger.info(f"Created usage plan: {usage_plan_id}")

            return usage_plan_id

        except ClientError as e:
            logger.error(f"Failed to create usage plan: {e}")
            raise

    def create_api_key(
        self,
        name: str,
        description: str = "",
        enabled: bool = True,
        usage_plan_id: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Create an API key.

        Args:
            name: Key name
            description: Key description
            enabled: Whether key is enabled
            usage_plan_id: Usage plan to associate with

        Returns:
            Dictionary with key ID and value
        """
        client = self._get_client()

        try:
            response = client.create_api_key(
                name=name,
                description=description,
                enabled=enabled,
                generateDistinctId=True
            )

            key_id = response["id"]
            key_value = response["value"]

            # Associate with usage plan if specified
            if usage_plan_id:
                client.create_usage_plan_key(
                    usagePlanId=usage_plan_id,
                    keyId=key_id,
                    keyType="API_KEY"
                )

            logger.info(f"Created API key: {key_id}")

            return {
                "id": key_id,
                "value": key_value,
                "name": name
            }

        except ClientError as e:
            logger.error(f"Failed to create API key: {e}")
            raise

    def setup_cloudwatch_logging(
        self,
        api_id: str,
        stage_name: str,
        log_group_arn: str
    ) -> None:
        """
        Configure CloudWatch access logging.

        Args:
            api_id: API Gateway REST API ID
            stage_name: Stage name
            log_group_arn: CloudWatch Log Group ARN
        """
        client = self._get_client()

        try:
            client.update_stage(
                restApiId=api_id,
                stageName=stage_name,
                patchOperations=[
                    {
                        "op": "replace",
                        "path": "/accessLogSettings/destinationArn",
                        "value": log_group_arn
                    },
                    {
                        "op": "replace",
                        "path": "/accessLogSettings/format",
                        "value": self.config.access_log_format
                    }
                ]
            )

            logger.info(f"Configured CloudWatch logging for stage {stage_name}")

        except ClientError as e:
            logger.error(f"Failed to configure CloudWatch logging: {e}")
            raise

    def get_api_endpoint(self, api_id: str, stage_name: str) -> str:
        """
        Get the API endpoint URL.

        Args:
            api_id: API Gateway REST API ID
            stage_name: Stage name

        Returns:
            API endpoint URL
        """
        return f"https://{api_id}.execute-api.{self.config.region}.amazonaws.com/{stage_name}"

    def deploy_full_api(
        self,
        openapi_spec: Dict[str, Any],
        deploy_stages: bool = True
    ) -> Dict[str, Any]:
        """
        Full deployment workflow: create API, authorizers, stages, and usage plans.

        Args:
            openapi_spec: OpenAPI 3.0 specification
            deploy_stages: Whether to create deployments for stages

        Returns:
            Deployment details
        """
        result = {
            "api_id": None,
            "authorizers": [],
            "stages": [],
            "usage_plans": [],
            "endpoints": {}
        }

        # Create REST API
        api_id = self.create_rest_api(openapi_spec)
        result["api_id"] = api_id

        # Create Lambda authorizer if configured
        if self.config.lambda_authorizer:
            auth_id = self.create_lambda_authorizer(
                api_id,
                self.config.lambda_authorizer
            )
            result["authorizers"].append({
                "id": auth_id,
                "type": "lambda",
                "name": self.config.lambda_authorizer.name
            })

        # Create Cognito authorizer if configured
        if self.config.cognito_authorizer:
            auth_id = self.create_cognito_authorizer(
                api_id,
                self.config.cognito_authorizer
            )
            result["authorizers"].append({
                "id": auth_id,
                "type": "cognito",
                "name": self.config.cognito_authorizer.name
            })

        # Create deployments for each stage
        if deploy_stages:
            for stage_config in self.config.stages:
                deployment_id = self.create_deployment(
                    api_id,
                    stage_config,
                    f"Initial deployment to {stage_config.name}"
                )

                result["stages"].append({
                    "name": stage_config.name,
                    "deployment_id": deployment_id
                })

                result["endpoints"][stage_config.name] = self.get_api_endpoint(
                    api_id,
                    stage_config.name
                )

        # Create usage plans
        for plan_config in self.config.usage_plans:
            for stage in result["stages"]:
                plan_id = self.create_usage_plan(
                    api_id,
                    stage["name"],
                    plan_config
                )
                result["usage_plans"].append({
                    "id": plan_id,
                    "name": plan_config.name,
                    "stage": stage["name"]
                })

        logger.info(f"Full API deployment complete: {result}")
        return result


# Convenience function
def create_api_gateway(
    openapi_spec: Dict[str, Any],
    config: Optional[AWSGatewayConfig] = None,
    deploy: bool = True
) -> Dict[str, Any]:
    """
    Create and deploy an API Gateway.

    Args:
        openapi_spec: OpenAPI 3.0 specification
        config: Optional configuration
        deploy: Whether to create deployments

    Returns:
        Deployment details
    """
    gateway = AWSAPIGatewayIntegration(config)
    return gateway.deploy_full_api(openapi_spec, deploy)

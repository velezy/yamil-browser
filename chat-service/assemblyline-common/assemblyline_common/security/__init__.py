"""
Security module for Logic Weaver.

Enterprise-grade API security comparable to Kong, MuleSoft, and AWS API Gateway.

Features:
- Rate Limiting & Adaptive Throttling
- Threat Protection (JSON, XML, HL7, SQL injection)
- IP Filtering & Geo-blocking
- Bot Detection
- Quota Management
- Request Caching
- API Analytics
- Policy Orchestration (Kong-style declarative config)
- API Versioning & Lifecycle Management (RFC 8594)
"""

# Rate Limiting
from .rate_limiter import (
    RateLimiter,
    RateLimitConfig,
    RateLimitResult,
    RateLimitScope,
    RateLimitMiddleware,
    get_rate_limiter,
)

# Adaptive Throttling
from .adaptive_throttler import (
    AdaptiveThrottler,
    ThrottleConfig,
    LoadLevel,
    get_adaptive_throttler,
)

# Threat Protection - JSON
from .json_threat_protection import (
    JSONThreatProtection,
    JSONThreatConfig,
    validate_json,
)

# Threat Protection - XML
from .xml_threat_protection import (
    XMLThreatProtection,
    XMLThreatConfig,
    validate_xml,
)

# Threat Protection - HL7
from .hl7_threat_protection import (
    HL7ThreatProtection,
    HL7ThreatConfig,
    validate_hl7,
)

# Injection Protection
from .injection_protection import (
    SQLInjectionProtection,
    CommandInjectionProtection,
    NoSQLInjectionProtection,
)

# OAuth Scopes
from .oauth_scopes import (
    OAuthScopeEnforcer,
    ScopeConfig,
    require_scopes,
    require_resource_access,
)

# mTLS
from .mtls import (
    MTLSValidator,
    MTLSConfig,
    validate_client_certificate,
    mtls_required,
)

# Request Caching
from .request_cache import (
    RequestCache,
    CacheConfig,
    CacheMiddleware,
    CacheControl,
    get_request_cache,
    cache_response,
)

# IP Filtering
from .ip_filter import (
    IPFilter,
    IPFilterConfig,
    IPFilterMode,
    IPFilterResult,
    get_ip_filter,
    ip_filter_dependency,
)

# Geo-blocking
from .geo_blocking import (
    GeoBlocker,
    GeoBlockConfig,
    GeoBlockMode,
    GeoBlockResult,
    get_geo_blocker,
    geo_block_dependency,
    OFAC_SANCTIONED,
    EU_COUNTRIES,
)

# Bot Detection
from .bot_detection import (
    BotDetector,
    BotDetectionConfig,
    BotDetectionResult,
    BotCategory,
    BotAction,
    get_bot_detector,
    bot_detection_dependency,
)

# Quota Management
from .quota_manager import (
    QuotaManager,
    QuotaConfig,
    QuotaTier,
    QuotaLimit,
    QuotaPeriod,
    QuotaUsage,
    OverageAction,
    get_quota_manager,
    quota_check_dependency,
    QUOTA_TIERS,
)

# API Analytics
from .api_analytics import (
    APIAnalytics,
    AnalyticsConfig,
    AnalyticsMiddleware,
    RequestMetrics,
    AggregatedMetrics,
    TimeGranularity,
    get_api_analytics,
)

# Policy Orchestrator
from .policy_orchestrator import (
    PolicyOrchestrator,
    PolicyConfig,
    PolicyType,
    PolicyScope,
    GatewayConfig,
    ServiceConfig,
    PRESETS,
)

# CORS Policy
from .cors_policy import (
    CORSPolicy,
    CORSConfig,
    OriginMatchMode,
    CORSResult,
    get_cors_policy,
    cors_from_preset,
    CORS_PRESETS,
)

# Request Size Limit
from .request_size_limit import (
    RequestSizeLimit,
    RequestSizeConfig,
    SizeLimitResult,
    get_request_size_limiter,
    size_limit_from_preset,
    SIZE_LIMIT_PRESETS,
)

# Request/Response Transform
from .request_transform import (
    RequestTransformer,
    ResponseTransformer,
    RequestTransformConfig,
    ResponseTransformConfig,
    TransformAction,
    HeaderTransform,
    BodyTransform,
    PathTransform,
    TransformMiddleware,
    get_request_transformer,
    get_response_transformer,
    TRANSFORM_PRESETS,
)

# Spike Control
from .spike_control import (
    SpikeController,
    SpikeControlConfig,
    SpikeAction,
    SpikeState,
    SpikeMetrics,
    SpikeCheckResult,
    get_spike_controller,
    SPIKE_CONTROL_PRESETS,
)

# ACL Policy
from .acl_policy import (
    ACLPolicy,
    ACLConfig,
    ACLRule,
    ACLMode,
    PermissionType,
    ACLCheckResult,
    get_acl_policy,
    acl_from_preset,
    ACL_PRESETS,
)

# Circuit Breaker Policy
from .circuit_breaker_policy import (
    CircuitBreakerPolicy,
    CircuitBreakerConfig,
    CircuitBreaker,
    CircuitState,
    FailureType,
    CircuitMetrics,
    CircuitCheckResult,
    get_circuit_breaker_policy,
    CIRCUIT_BREAKER_PRESETS,
)

# Request Termination
from .request_termination import (
    RequestTermination,
    TerminationConfig,
    TerminationRule,
    TerminationReason,
    TerminationResult,
    get_request_termination,
    TERMINATION_PRESETS,
)

# Request Logging
from .request_logging import (
    RequestLogger,
    RequestLoggingConfig,
    PHIMaskingConfig,
    PHIMasker,
    LogLevel,
    LogTarget,
    RequestLogEntry,
    ResponseLogEntry,
    get_request_logger,
    LOGGING_PRESETS,
)

# API Versioning & Lifecycle
from .api_versioning import (
    APIVersion,
    VersionStatus,
    VersioningStrategy,
    VersioningConfig,
    VersionRegistry,
    VersionDetector,
    DeprecatedEndpoint,
    DeprecationManager,
    APIVersionMiddleware,
    create_versioning_middleware,
    get_api_version,
    get_api_version_info,
    api_version,
    deprecated,
    create_versions_endpoint,
)

# API Environments
from .api_environments import (
    EnvironmentType,
    PromotionStatus,
    ApprovalType,
    EnvironmentPolicy,
    Environment,
    PromotionRule,
    PromotionRequest,
    EnvironmentManager,
    get_environment_manager,
    ENVIRONMENT_PRESETS,
)

# API Documentation
from .api_documentation import (
    OpenAPIVersion,
    SDKLanguage,
    DocumentationFormat,
    APIContact,
    APILicense,
    APIInfo,
    Server,
    ServerVariable,
    SecurityScheme,
    Tag,
    CodeSample,
    EndpointDoc,
    OpenAPIGenerator,
    CodeSampleGenerator,
    SDKGenerator,
    DocumentationExporter,
    create_openapi_generator,
    generate_code_samples,
    generate_sdk,
    export_documentation,
)

# PostgreSQL Rate Limiter (alternative to Redis)
from .postgres_rate_limiter import (
    PostgresRateLimiter,
    PostgresRateLimitResult,
    check_postgres_rate_limit,
)

# Token Scanner
from .token_scanner import (
    TokenScanner,
    LeakedKeyAlert,
    ScanResult,
    scan_string_for_keys,
    mask_keys_in_text,
)

__all__ = [
    # Rate Limiting
    "RateLimiter",
    "RateLimitConfig",
    "RateLimitResult",
    "RateLimitScope",
    "RateLimitMiddleware",
    "get_rate_limiter",

    # Adaptive Throttling
    "AdaptiveThrottler",
    "ThrottleConfig",
    "LoadLevel",
    "get_adaptive_throttler",

    # JSON Protection
    "JSONThreatProtection",
    "JSONThreatConfig",
    "validate_json",

    # XML Protection
    "XMLThreatProtection",
    "XMLThreatConfig",
    "validate_xml",

    # HL7 Protection
    "HL7ThreatProtection",
    "HL7ThreatConfig",
    "validate_hl7",

    # Injection Protection
    "SQLInjectionProtection",
    "CommandInjectionProtection",
    "NoSQLInjectionProtection",

    # OAuth Scopes
    "OAuthScopeEnforcer",
    "ScopeConfig",
    "require_scopes",
    "require_resource_access",

    # mTLS
    "MTLSValidator",
    "MTLSConfig",
    "validate_client_certificate",
    "mtls_required",

    # Request Caching
    "RequestCache",
    "CacheConfig",
    "CacheMiddleware",
    "CacheControl",
    "get_request_cache",
    "cache_response",

    # IP Filtering
    "IPFilter",
    "IPFilterConfig",
    "IPFilterMode",
    "IPFilterResult",
    "get_ip_filter",
    "ip_filter_dependency",

    # Geo-blocking
    "GeoBlocker",
    "GeoBlockConfig",
    "GeoBlockMode",
    "GeoBlockResult",
    "get_geo_blocker",
    "geo_block_dependency",
    "OFAC_SANCTIONED",
    "EU_COUNTRIES",

    # Bot Detection
    "BotDetector",
    "BotDetectionConfig",
    "BotDetectionResult",
    "BotCategory",
    "BotAction",
    "get_bot_detector",
    "bot_detection_dependency",

    # Quota Management
    "QuotaManager",
    "QuotaConfig",
    "QuotaTier",
    "QuotaLimit",
    "QuotaPeriod",
    "QuotaUsage",
    "OverageAction",
    "get_quota_manager",
    "quota_check_dependency",
    "QUOTA_TIERS",

    # API Analytics
    "APIAnalytics",
    "AnalyticsConfig",
    "AnalyticsMiddleware",
    "RequestMetrics",
    "AggregatedMetrics",
    "TimeGranularity",
    "get_api_analytics",

    # Policy Orchestrator
    "PolicyOrchestrator",
    "PolicyConfig",
    "PolicyType",
    "PolicyScope",
    "GatewayConfig",
    "ServiceConfig",
    "PRESETS",

    # CORS Policy
    "CORSPolicy",
    "CORSConfig",
    "OriginMatchMode",
    "CORSResult",
    "get_cors_policy",
    "cors_from_preset",
    "CORS_PRESETS",

    # Request Size Limit
    "RequestSizeLimit",
    "RequestSizeConfig",
    "SizeLimitResult",
    "get_request_size_limiter",
    "size_limit_from_preset",
    "SIZE_LIMIT_PRESETS",

    # Request/Response Transform
    "RequestTransformer",
    "ResponseTransformer",
    "RequestTransformConfig",
    "ResponseTransformConfig",
    "TransformAction",
    "HeaderTransform",
    "BodyTransform",
    "PathTransform",
    "TransformMiddleware",
    "get_request_transformer",
    "get_response_transformer",
    "TRANSFORM_PRESETS",

    # Spike Control
    "SpikeController",
    "SpikeControlConfig",
    "SpikeAction",
    "SpikeState",
    "SpikeMetrics",
    "SpikeCheckResult",
    "get_spike_controller",
    "SPIKE_CONTROL_PRESETS",

    # ACL Policy
    "ACLPolicy",
    "ACLConfig",
    "ACLRule",
    "ACLMode",
    "PermissionType",
    "ACLCheckResult",
    "get_acl_policy",
    "acl_from_preset",
    "ACL_PRESETS",

    # Circuit Breaker Policy
    "CircuitBreakerPolicy",
    "CircuitBreakerConfig",
    "CircuitBreaker",
    "CircuitState",
    "FailureType",
    "CircuitMetrics",
    "CircuitCheckResult",
    "get_circuit_breaker_policy",
    "CIRCUIT_BREAKER_PRESETS",

    # Request Termination
    "RequestTermination",
    "TerminationConfig",
    "TerminationRule",
    "TerminationReason",
    "TerminationResult",
    "get_request_termination",
    "TERMINATION_PRESETS",

    # Request Logging
    "RequestLogger",
    "RequestLoggingConfig",
    "PHIMaskingConfig",
    "PHIMasker",
    "LogLevel",
    "LogTarget",
    "RequestLogEntry",
    "ResponseLogEntry",
    "get_request_logger",
    "LOGGING_PRESETS",

    # API Versioning & Lifecycle
    "APIVersion",
    "VersionStatus",
    "VersioningStrategy",
    "VersioningConfig",
    "VersionRegistry",
    "VersionDetector",
    "DeprecatedEndpoint",
    "DeprecationManager",
    "APIVersionMiddleware",
    "create_versioning_middleware",
    "get_api_version",
    "get_api_version_info",
    "api_version",
    "deprecated",
    "create_versions_endpoint",

    # API Environments
    "EnvironmentType",
    "PromotionStatus",
    "ApprovalType",
    "EnvironmentPolicy",
    "Environment",
    "PromotionRule",
    "PromotionRequest",
    "EnvironmentManager",
    "get_environment_manager",
    "ENVIRONMENT_PRESETS",

    # API Documentation
    "OpenAPIVersion",
    "SDKLanguage",
    "DocumentationFormat",
    "APIContact",
    "APILicense",
    "APIInfo",
    "Server",
    "ServerVariable",
    "SecurityScheme",
    "Tag",
    "CodeSample",
    "EndpointDoc",
    "OpenAPIGenerator",
    "CodeSampleGenerator",
    "SDKGenerator",
    "DocumentationExporter",
    "create_openapi_generator",
    "generate_code_samples",
    "generate_sdk",
    "export_documentation",

    # PostgreSQL Rate Limiter
    "PostgresRateLimiter",
    "PostgresRateLimitResult",
    "check_postgres_rate_limit",

    # Token Scanner
    "TokenScanner",
    "LeakedKeyAlert",
    "ScanResult",
    "scan_string_for_keys",
    "mask_keys_in_text",
]

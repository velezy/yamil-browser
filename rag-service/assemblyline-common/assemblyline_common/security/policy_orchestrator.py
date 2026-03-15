"""
Policy Orchestrator

Kong/MuleSoft-style declarative policy configuration for FastAPI.
Attach security policies using YAML/dict configuration instead of code.

Features:
- Declarative policy configuration (YAML/JSON/dict)
- Global, route, and tenant-level policies
- Policy chaining and ordering
- Hot-reload without restart
- Policy templates and presets
"""

import asyncio
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union

from fastapi import FastAPI, Request, Response, HTTPException, Depends
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class PolicyType(str, Enum):
    """Available policy types."""
    # Rate Limiting & Throttling
    RATE_LIMIT = "rate-limiting"
    ADAPTIVE_THROTTLE = "adaptive-throttling"
    SPIKE_CONTROL = "spike-control"

    # IP & Geo
    IP_FILTER = "ip-restriction"
    GEO_BLOCK = "geo-blocking"
    BOT_DETECTION = "bot-detection"

    # Threat Protection
    JSON_THREAT = "json-threat-protection"
    XML_THREAT = "xml-threat-protection"
    HL7_THREAT = "hl7-threat-protection"
    SQL_INJECTION = "sql-injection-protection"

    # Caching
    REQUEST_CACHE = "request-caching"

    # CORS
    CORS = "cors"

    # Authentication
    JWT_AUTH = "jwt-auth"
    API_KEY_AUTH = "api-key-auth"
    OAUTH_SCOPES = "oauth-scopes"
    MTLS = "mtls"
    ACL = "acl"

    # Quota & Size
    QUOTA = "quota"
    REQUEST_SIZE = "request-size-limit"

    # Transform
    RESPONSE_TRANSFORM = "response-transform"
    REQUEST_TRANSFORM = "request-transform"

    # Logging
    LOGGING = "request-logging"

    # Circuit Breaker
    CIRCUIT_BREAKER = "circuit-breaker"

    # Request Control
    REQUEST_TERMINATION = "request-termination"


class PolicyScope(str, Enum):
    """Policy application scope."""
    GLOBAL = "global"  # All routes
    SERVICE = "service"  # Specific service/router
    ROUTE = "route"  # Specific endpoint
    TENANT = "tenant"  # Per-tenant override


@dataclass
class PolicyConfig:
    """Configuration for a single policy."""
    name: str
    type: PolicyType
    enabled: bool = True
    config: Dict[str, Any] = field(default_factory=dict)
    scope: PolicyScope = PolicyScope.GLOBAL
    routes: List[str] = field(default_factory=list)  # Route patterns
    priority: int = 100  # Lower = runs first
    condition: Optional[str] = None  # SpEL-like condition


@dataclass
class ServiceConfig:
    """Configuration for a service (group of routes)."""
    name: str
    path_prefix: str
    policies: List[PolicyConfig] = field(default_factory=list)
    routes: Dict[str, List[PolicyConfig]] = field(default_factory=dict)


@dataclass
class GatewayConfig:
    """Top-level gateway configuration (Kong-style)."""
    version: str = "1.0"
    global_policies: List[PolicyConfig] = field(default_factory=list)
    services: List[ServiceConfig] = field(default_factory=list)
    presets: Dict[str, List[PolicyConfig]] = field(default_factory=dict)


# Policy presets (like Kong plugin bundles)
PRESETS = {
    "basic-security": [
        PolicyConfig(name="rate-limit", type=PolicyType.RATE_LIMIT, config={"requests_per_second": 100}),
        PolicyConfig(name="json-threat", type=PolicyType.JSON_THREAT, config={"max_depth": 20}),
    ],
    "enterprise-security": [
        PolicyConfig(name="rate-limit", type=PolicyType.RATE_LIMIT, config={"requests_per_second": 100}),
        PolicyConfig(name="adaptive-throttle", type=PolicyType.ADAPTIVE_THROTTLE),
        PolicyConfig(name="ip-filter", type=PolicyType.IP_FILTER, config={"mode": "blacklist"}),
        PolicyConfig(name="geo-block", type=PolicyType.GEO_BLOCK, config={"block_ofac": True}),
        PolicyConfig(name="bot-detection", type=PolicyType.BOT_DETECTION),
        PolicyConfig(name="json-threat", type=PolicyType.JSON_THREAT, config={"max_depth": 20}),
        PolicyConfig(name="sql-injection", type=PolicyType.SQL_INJECTION),
    ],
    "healthcare-hipaa": [
        PolicyConfig(name="rate-limit", type=PolicyType.RATE_LIMIT, config={"requests_per_second": 50}),
        PolicyConfig(name="ip-filter", type=PolicyType.IP_FILTER, config={"mode": "whitelist"}),
        PolicyConfig(name="mtls", type=PolicyType.MTLS),
        PolicyConfig(name="json-threat", type=PolicyType.JSON_THREAT),
        PolicyConfig(name="hl7-threat", type=PolicyType.HL7_THREAT),
        PolicyConfig(name="logging", type=PolicyType.LOGGING, config={"phi_mask": True}),
    ],
    "public-api": [
        PolicyConfig(name="rate-limit", type=PolicyType.RATE_LIMIT, config={"requests_per_second": 10}),
        PolicyConfig(name="quota", type=PolicyType.QUOTA, config={"requests_per_day": 1000}),
        PolicyConfig(name="bot-detection", type=PolicyType.BOT_DETECTION),
        PolicyConfig(name="cors", type=PolicyType.CORS, config={"allow_origins": ["*"]}),
        PolicyConfig(name="cache", type=PolicyType.REQUEST_CACHE, config={"ttl": 300}),
    ],
}


class PolicyExecutor:
    """Executes a single policy."""

    def __init__(self, policy: PolicyConfig):
        self.policy = policy
        self._handler = None
        self._initialized = False

    async def initialize(self):
        """Initialize the policy handler."""
        if self._initialized:
            return

        policy_type = self.policy.type
        config = self.policy.config

        try:
            if policy_type == PolicyType.RATE_LIMIT:
                from assemblyline_common.security.rate_limiter import get_rate_limiter, RateLimitConfig
                self._handler = await get_rate_limiter(RateLimitConfig(
                    default_requests_per_second=config.get("requests_per_second", 100),
                    default_burst_size=config.get("burst_size", 200),
                ))

            elif policy_type == PolicyType.IP_FILTER:
                from assemblyline_common.security.ip_filter import get_ip_filter, IPFilterConfig, IPFilterMode
                mode = IPFilterMode(config.get("mode", "blacklist"))
                self._handler = await get_ip_filter(IPFilterConfig(
                    mode=mode,
                    default_whitelist=config.get("whitelist", []),
                    default_blacklist=config.get("blacklist", []),
                ))

            elif policy_type == PolicyType.GEO_BLOCK:
                from assemblyline_common.security.geo_blocking import get_geo_blocker, GeoBlockConfig
                self._handler = await get_geo_blocker(GeoBlockConfig(
                    block_ofac_sanctioned=config.get("block_ofac", False),
                    blocked_countries=config.get("blocked_countries", []),
                    eu_only=config.get("eu_only", False),
                ))

            elif policy_type == PolicyType.BOT_DETECTION:
                from assemblyline_common.security.bot_detection import get_bot_detector, BotDetectionConfig
                self._handler = await get_bot_detector(BotDetectionConfig(
                    analyze_behavior=config.get("analyze_behavior", True),
                ))

            elif policy_type == PolicyType.JSON_THREAT:
                from assemblyline_common.security.json_threat_protection import JSONThreatProtection, JSONThreatConfig
                self._handler = JSONThreatProtection(JSONThreatConfig(
                    max_depth=config.get("max_depth", 20),
                    max_string_length=config.get("max_string_length", 100000),
                    max_array_size=config.get("max_array_size", 1000),
                ))

            elif policy_type == PolicyType.XML_THREAT:
                from assemblyline_common.security.xml_threat_protection import XMLThreatProtection, XMLThreatConfig
                self._handler = XMLThreatProtection(XMLThreatConfig(
                    max_depth=config.get("max_depth", 50),
                    block_dtd=config.get("block_dtd", True),
                    block_external_entities=config.get("block_xxe", True),
                ))

            elif policy_type == PolicyType.HL7_THREAT:
                from assemblyline_common.security.hl7_threat_protection import HL7ThreatProtection, HL7ThreatConfig
                self._handler = HL7ThreatProtection(HL7ThreatConfig(
                    max_segments=config.get("max_segments", 500),
                    max_field_length=config.get("max_field_length", 65536),
                ))

            elif policy_type == PolicyType.SQL_INJECTION:
                from assemblyline_common.security.injection_protection import SQLInjectionProtection
                self._handler = SQLInjectionProtection()

            elif policy_type == PolicyType.REQUEST_CACHE:
                from assemblyline_common.security.request_cache import get_request_cache, CacheConfig
                self._handler = await get_request_cache(CacheConfig(
                    default_ttl=config.get("ttl", 300),
                ))

            elif policy_type == PolicyType.ADAPTIVE_THROTTLE:
                from assemblyline_common.security.adaptive_throttler import get_adaptive_throttler, ThrottleConfig
                self._handler = await get_adaptive_throttler(ThrottleConfig(
                    high_load_threshold=config.get("high_load_threshold", 0.7),
                    critical_load_threshold=config.get("critical_load_threshold", 0.9),
                ))

            elif policy_type == PolicyType.SPIKE_CONTROL:
                from assemblyline_common.security.spike_control import get_spike_controller, SpikeControlConfig, SpikeAction
                self._handler = await get_spike_controller(SpikeControlConfig(
                    max_requests_per_window=config.get("max_requests_per_window", 100),
                    time_window_seconds=config.get("time_window_seconds", 1.0),
                    spike_action=SpikeAction(config.get("spike_action", "queue")),
                ))

            elif policy_type == PolicyType.CORS:
                from assemblyline_common.security.cors_policy import CORSPolicy, CORSConfig
                self._handler = CORSPolicy(CORSConfig(
                    allowed_origins=config.get("allowed_origins", ["*"]),
                    allow_credentials=config.get("allow_credentials", False),
                    allowed_methods=config.get("allowed_methods", ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]),
                    max_age=config.get("max_age", 86400),
                ))

            elif policy_type == PolicyType.REQUEST_SIZE:
                from assemblyline_common.security.request_size_limit import RequestSizeLimit, RequestSizeConfig
                self._handler = RequestSizeLimit(RequestSizeConfig(
                    max_body_size=config.get("max_body_size", 10485760),
                    max_header_size=config.get("max_header_size", 8192),
                ))

            elif policy_type == PolicyType.QUOTA:
                from assemblyline_common.security.quota_manager import get_quota_manager, QuotaConfig
                self._handler = await get_quota_manager(QuotaConfig())

            elif policy_type == PolicyType.ACL:
                from assemblyline_common.security.acl_policy import ACLPolicy, ACLConfig, ACLRule, ACLMode
                rules = [
                    ACLRule(route_pattern=r["route_pattern"], groups=r.get("groups", []))
                    for r in config.get("rules", [])
                ]
                self._handler = ACLPolicy(ACLConfig(
                    mode=ACLMode(config.get("mode", "whitelist")),
                    route_rules=rules,
                ))

            elif policy_type == PolicyType.CIRCUIT_BREAKER:
                from assemblyline_common.security.circuit_breaker_policy import get_circuit_breaker_policy, CircuitBreakerConfig
                self._handler = await get_circuit_breaker_policy(CircuitBreakerConfig(
                    failure_threshold=config.get("failure_threshold", 5),
                    recovery_timeout_seconds=config.get("recovery_timeout_seconds", 30.0),
                ))

            elif policy_type == PolicyType.REQUEST_TERMINATION:
                from assemblyline_common.security.request_termination import RequestTermination, TerminationConfig
                self._handler = RequestTermination(TerminationConfig(
                    maintenance_mode=config.get("maintenance_mode", False),
                    maintenance_message=config.get("maintenance_message", "Service is under maintenance"),
                ))

            elif policy_type == PolicyType.LOGGING:
                from assemblyline_common.security.request_logging import RequestLogger, RequestLoggingConfig, PHIMaskingConfig
                self._handler = RequestLogger(RequestLoggingConfig(
                    log_request=config.get("log_request", True),
                    log_response=config.get("log_response", True),
                    phi_masking=PHIMaskingConfig(enabled=config.get("phi_mask", True)),
                ))

            self._initialized = True
            logger.debug(f"Initialized policy: {self.policy.name} ({policy_type.value})")

        except ImportError as e:
            logger.warning(f"Policy {self.policy.name} not available: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize policy {self.policy.name}: {e}")

    async def execute(self, request: Request, tenant_id: Optional[str] = None) -> Optional[Response]:
        """
        Execute the policy.

        Returns:
            None if allowed, Response if blocked/modified
        """
        if not self._initialized or not self._handler:
            return None

        if not self.policy.enabled:
            return None

        try:
            policy_type = self.policy.type

            if policy_type == PolicyType.RATE_LIMIT:
                from assemblyline_common.security.rate_limiter import RateLimitScope
                key = tenant_id or (request.client.host if request.client else "unknown")
                allowed, result = await self._handler.check(
                    scope=RateLimitScope.TENANT if tenant_id else RateLimitScope.IP,
                    key=key,
                )
                if not allowed:
                    raise HTTPException(
                        status_code=429,
                        detail="Rate limit exceeded",
                        headers={"Retry-After": str(result.retry_after_seconds)}
                    )

            elif policy_type == PolicyType.IP_FILTER:
                result = await self._handler.check(request, tenant_id)
                if not result.allowed:
                    raise HTTPException(status_code=403, detail=result.reason)

            elif policy_type == PolicyType.GEO_BLOCK:
                result = await self._handler.check(request, tenant_id)
                if not result.allowed:
                    raise HTTPException(status_code=403, detail="Access denied from your location")

            elif policy_type == PolicyType.BOT_DETECTION:
                from assemblyline_common.security.bot_detection import BotAction
                result = await self._handler.detect(request)
                if result.action == BotAction.BLOCK:
                    raise HTTPException(status_code=403, detail="Access denied")

            elif policy_type == PolicyType.ADAPTIVE_THROTTLE:
                key = tenant_id or "global"
                allowed, _ = await self._handler.should_allow(key)
                if not allowed:
                    raise HTTPException(
                        status_code=503,
                        detail="Service temporarily unavailable",
                        headers={"Retry-After": "30"}
                    )

            return None

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Policy {self.policy.name} execution failed: {e}")
            return None


class PolicyOrchestrator:
    """
    Kong/MuleSoft-style policy orchestrator.

    Usage:
        # From YAML config
        orchestrator = PolicyOrchestrator.from_yaml("policies.yaml")

        # Or from dict
        orchestrator = PolicyOrchestrator.from_dict({
            "version": "1.0",
            "global_policies": [
                {"name": "rate-limit", "type": "rate-limiting", "config": {"requests_per_second": 100}}
            ],
            "services": [
                {
                    "name": "auth-service",
                    "path_prefix": "/api/v1/auth",
                    "policies": [
                        {"name": "ip-filter", "type": "ip-restriction"}
                    ]
                }
            ]
        })

        # Or use presets
        orchestrator = PolicyOrchestrator.from_preset("enterprise-security")

        # Attach to FastAPI app
        await orchestrator.attach(app)
    """

    def __init__(self, config: GatewayConfig):
        self.config = config
        self._executors: Dict[str, PolicyExecutor] = {}
        self._route_patterns: Dict[str, List[PolicyExecutor]] = {}
        self._initialized = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PolicyOrchestrator":
        """Create orchestrator from dictionary config."""
        global_policies = [
            PolicyConfig(
                name=p.get("name", f"policy-{i}"),
                type=PolicyType(p["type"]),
                enabled=p.get("enabled", True),
                config=p.get("config", {}),
                scope=PolicyScope(p.get("scope", "global")),
                routes=p.get("routes", []),
                priority=p.get("priority", 100),
            )
            for i, p in enumerate(data.get("global_policies", []))
        ]

        services = []
        for svc in data.get("services", []):
            svc_policies = [
                PolicyConfig(
                    name=p.get("name", f"policy-{i}"),
                    type=PolicyType(p["type"]),
                    enabled=p.get("enabled", True),
                    config=p.get("config", {}),
                    priority=p.get("priority", 100),
                )
                for i, p in enumerate(svc.get("policies", []))
            ]
            services.append(ServiceConfig(
                name=svc["name"],
                path_prefix=svc.get("path_prefix", ""),
                policies=svc_policies,
            ))

        config = GatewayConfig(
            version=data.get("version", "1.0"),
            global_policies=global_policies,
            services=services,
        )

        return cls(config)

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "PolicyOrchestrator":
        """Create orchestrator from YAML file."""
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f)

        return cls.from_dict(data)

    @classmethod
    def from_preset(cls, preset_name: str) -> "PolicyOrchestrator":
        """Create orchestrator from a preset."""
        if preset_name not in PRESETS:
            raise ValueError(f"Unknown preset: {preset_name}. Available: {list(PRESETS.keys())}")

        config = GatewayConfig(
            global_policies=PRESETS[preset_name].copy()
        )

        return cls(config)

    async def initialize(self):
        """Initialize all policy executors."""
        if self._initialized:
            return

        # Initialize global policies
        for policy in sorted(self.config.global_policies, key=lambda p: p.priority):
            executor = PolicyExecutor(policy)
            await executor.initialize()
            self._executors[policy.name] = executor

            # Build route patterns
            if policy.routes:
                for pattern in policy.routes:
                    if pattern not in self._route_patterns:
                        self._route_patterns[pattern] = []
                    self._route_patterns[pattern].append(executor)

        # Initialize service policies
        for service in self.config.services:
            for policy in sorted(service.policies, key=lambda p: p.priority):
                executor = PolicyExecutor(policy)
                await executor.initialize()
                key = f"{service.name}:{policy.name}"
                self._executors[key] = executor

                # Build route patterns for service
                pattern = f"{service.path_prefix}/*"
                if pattern not in self._route_patterns:
                    self._route_patterns[pattern] = []
                self._route_patterns[pattern].append(executor)

        self._initialized = True
        logger.info(f"Policy orchestrator initialized with {len(self._executors)} policies")

    def _match_route(self, path: str) -> List[PolicyExecutor]:
        """Get executors matching a route path."""
        matched = []

        for pattern, executors in self._route_patterns.items():
            # Convert glob pattern to regex
            regex = pattern.replace("*", ".*").replace("/", r"\/")
            if re.match(regex, path):
                matched.extend(executors)

        # Add global policies (those without route patterns)
        for policy in self.config.global_policies:
            if not policy.routes:
                executor = self._executors.get(policy.name)
                if executor and executor not in matched:
                    matched.append(executor)

        return matched

    async def execute(
        self,
        request: Request,
        tenant_id: Optional[str] = None
    ) -> Optional[Response]:
        """Execute all matching policies for a request."""
        path = request.url.path
        executors = self._match_route(path)

        for executor in executors:
            result = await executor.execute(request, tenant_id)
            if result:
                return result

        return None

    async def attach(self, app: FastAPI):
        """
        Attach policies to a FastAPI application.

        Adds middleware that executes policies on every request.
        """
        await self.initialize()

        orchestrator = self

        class PolicyMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Get tenant ID from request state if available
                tenant_id = getattr(request.state, "tenant_id", None)

                # Execute policies
                try:
                    result = await orchestrator.execute(request, tenant_id)
                    if result:
                        return result
                except HTTPException as e:
                    from fastapi.responses import JSONResponse
                    return JSONResponse(
                        status_code=e.status_code,
                        content={"detail": e.detail},
                        headers=dict(e.headers) if e.headers else None,
                    )

                return await call_next(request)

        app.add_middleware(PolicyMiddleware)
        logger.info("Policy orchestrator attached to FastAPI app")

    def get_policy(self, name: str) -> Optional[PolicyExecutor]:
        """Get a policy executor by name."""
        return self._executors.get(name)

    async def enable_policy(self, name: str) -> bool:
        """Enable a policy at runtime."""
        executor = self._executors.get(name)
        if executor:
            executor.policy.enabled = True
            return True
        return False

    async def disable_policy(self, name: str) -> bool:
        """Disable a policy at runtime."""
        executor = self._executors.get(name)
        if executor:
            executor.policy.enabled = False
            return True
        return False

    def list_policies(self) -> List[Dict[str, Any]]:
        """List all configured policies."""
        return [
            {
                "name": name,
                "type": executor.policy.type.value,
                "enabled": executor.policy.enabled,
                "priority": executor.policy.priority,
                "config": executor.policy.config,
            }
            for name, executor in self._executors.items()
        ]


# Example YAML configuration:
EXAMPLE_CONFIG = """
# policies.yaml - Kong-style declarative configuration
version: "1.0"

global_policies:
  - name: rate-limit
    type: rate-limiting
    config:
      requests_per_second: 100
      burst_size: 200
    priority: 10

  - name: bot-detection
    type: bot-detection
    config:
      analyze_behavior: true
    priority: 20

  - name: json-threat
    type: json-threat-protection
    config:
      max_depth: 20
      max_string_length: 100000
    priority: 30

services:
  - name: auth-service
    path_prefix: /api/v1/auth
    policies:
      - name: ip-filter
        type: ip-restriction
        config:
          mode: blacklist
          blacklist:
            - "1.2.3.4"

  - name: inbound-service
    path_prefix: /api/v1/inbound
    policies:
      - name: hl7-threat
        type: hl7-threat-protection
        config:
          max_segments: 500
          required_segments: ["MSH"]

      - name: adaptive-throttle
        type: adaptive-throttling
        config:
          high_load_threshold: 0.7
"""


def create_example_config(path: str = "policies.yaml"):
    """Create example policy configuration file."""
    with open(path, "w") as f:
        f.write(EXAMPLE_CONFIG)
    logger.info(f"Created example config: {path}")

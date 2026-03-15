"""
Request Termination Policy

Kong Request Termination plugin equivalent.
Terminate requests with a specific status code and message.

Features:
- Custom status codes and messages
- Route-based termination
- Condition-based termination
- Maintenance mode
- Scheduled downtime
- A/B testing support
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Union

from fastapi import FastAPI, Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, PlainTextResponse

logger = logging.getLogger(__name__)


class TerminationReason(str, Enum):
    """Reason for termination."""
    MAINTENANCE = "maintenance"
    DEPRECATED = "deprecated"
    UNAUTHORIZED = "unauthorized"
    BLOCKED = "blocked"
    RATE_LIMITED = "rate_limited"
    CUSTOM = "custom"
    SCHEDULED = "scheduled"


@dataclass
class TerminationRule:
    """Rule for request termination."""
    # Name for identification
    name: str

    # Route pattern (supports * wildcard, regex)
    route_pattern: Optional[str] = None

    # HTTP methods to match (empty = all)
    methods: List[str] = field(default_factory=list)

    # Status code to return
    status_code: int = 503

    # Response body (string or dict for JSON)
    body: Union[str, Dict[str, Any], None] = None

    # Content-Type header
    content_type: str = "application/json"

    # Additional headers
    headers: Dict[str, str] = field(default_factory=dict)

    # Termination reason
    reason: TerminationReason = TerminationReason.CUSTOM

    # Condition expression (evaluated at runtime)
    condition: Optional[str] = None

    # Enable/disable
    enabled: bool = True

    # Priority (lower = checked first)
    priority: int = 100

    # Start time (for scheduled maintenance)
    start_time: Optional[datetime] = None

    # End time (for scheduled maintenance)
    end_time: Optional[datetime] = None

    # Percentage of requests to terminate (for A/B testing)
    percentage: float = 100.0


@dataclass
class TerminationConfig:
    """Configuration for request termination."""
    # Global maintenance mode
    maintenance_mode: bool = False

    # Maintenance message
    maintenance_message: str = "Service is under maintenance. Please try again later."

    # Maintenance status code
    maintenance_status_code: int = 503

    # Termination rules
    rules: List[TerminationRule] = field(default_factory=list)

    # Default headers for terminated requests
    default_headers: Dict[str, str] = field(default_factory=lambda: {
        "Cache-Control": "no-store",
    })

    # Log terminated requests
    log_terminations: bool = True

    # Bypass paths (never terminate)
    bypass_paths: List[str] = field(default_factory=lambda: [
        "/health",
        "/health/live",
        "/health/ready",
    ])


@dataclass
class TerminationResult:
    """Result of termination check."""
    terminate: bool
    rule: Optional[TerminationRule] = None
    status_code: int = 200
    body: Optional[Any] = None
    headers: Dict[str, str] = field(default_factory=dict)
    reason: Optional[str] = None


class RequestTermination:
    """
    Kong-style request termination policy.

    Usage:
        termination = RequestTermination(TerminationConfig(
            maintenance_mode=False,
            rules=[
                TerminationRule(
                    name="deprecate-v1",
                    route_pattern="/api/v1/*",
                    status_code=410,
                    body={"error": "API v1 is deprecated. Please use v2."},
                    reason=TerminationReason.DEPRECATED,
                ),
                TerminationRule(
                    name="block-country",
                    condition="request.headers.get('X-Country') == 'XX'",
                    status_code=403,
                    body={"error": "Access denied from your region"},
                    reason=TerminationReason.BLOCKED,
                ),
            ]
        ))

        # Check termination
        result = termination.check(request)
        if result.terminate:
            return Response(result.body, status_code=result.status_code)

        # Enable maintenance mode
        termination.enable_maintenance()

        # Attach as middleware
        termination.attach(app)
    """

    def __init__(self, config: Optional[TerminationConfig] = None):
        self.config = config or TerminationConfig()
        self._sorted_rules: List[TerminationRule] = []
        self._route_patterns: Dict[str, Pattern] = {}
        self._compile_rules()

    def _compile_rules(self):
        """Compile and sort rules."""
        self._sorted_rules = sorted(
            self.config.rules,
            key=lambda r: r.priority
        )

        for rule in self._sorted_rules:
            if rule.route_pattern:
                # Convert glob to regex
                pattern = rule.route_pattern.replace("**", "§§")
                pattern = pattern.replace("*", "[^/]+")
                pattern = pattern.replace("§§", ".*")
                pattern = f"^{pattern}$"
                self._route_patterns[rule.name] = re.compile(pattern)

    def _match_route(self, rule: TerminationRule, path: str) -> bool:
        """Check if path matches rule pattern."""
        if not rule.route_pattern:
            return True

        pattern = self._route_patterns.get(rule.name)
        if pattern:
            return bool(pattern.match(path))

        return False

    def _match_method(self, rule: TerminationRule, method: str) -> bool:
        """Check if method matches rule."""
        if not rule.methods:
            return True
        return method.upper() in [m.upper() for m in rule.methods]

    def _match_time(self, rule: TerminationRule) -> bool:
        """Check if current time is within scheduled window."""
        now = datetime.now(timezone.utc)

        if rule.start_time and now < rule.start_time:
            return False

        if rule.end_time and now > rule.end_time:
            return False

        return True

    def _match_percentage(self, rule: TerminationRule) -> bool:
        """Check if request falls within percentage."""
        import random
        return random.random() * 100 < rule.percentage

    def _evaluate_condition(
        self,
        rule: TerminationRule,
        request: Request,
    ) -> bool:
        """Evaluate condition expression."""
        if not rule.condition:
            return True

        # Build context for evaluation
        context = {
            "request": request,
            "path": request.url.path,
            "method": request.method,
            "headers": dict(request.headers),
            "query": dict(request.query_params),
            "True": True,
            "False": False,
            "None": None,
        }

        try:
            # Simple expression evaluation
            # Only allow safe operations
            result = eval(rule.condition, {"__builtins__": {}}, context)
            return bool(result)
        except Exception as e:
            logger.warning(f"Failed to evaluate condition '{rule.condition}': {e}")
            return False

    def check(self, request: Request) -> TerminationResult:
        """
        Check if request should be terminated.

        Args:
            request: FastAPI request

        Returns:
            TerminationResult
        """
        path = request.url.path
        method = request.method

        # Check bypass paths
        for bypass_path in self.config.bypass_paths:
            if path.startswith(bypass_path):
                return TerminationResult(terminate=False)

        # Check maintenance mode
        if self.config.maintenance_mode:
            return TerminationResult(
                terminate=True,
                status_code=self.config.maintenance_status_code,
                body={"error": self.config.maintenance_message},
                headers=self.config.default_headers.copy(),
                reason=TerminationReason.MAINTENANCE.value,
            )

        # Check rules
        for rule in self._sorted_rules:
            if not rule.enabled:
                continue

            # Check route pattern
            if not self._match_route(rule, path):
                continue

            # Check method
            if not self._match_method(rule, method):
                continue

            # Check time window
            if not self._match_time(rule):
                continue

            # Check percentage
            if not self._match_percentage(rule):
                continue

            # Check condition
            if not self._evaluate_condition(rule, request):
                continue

            # Rule matches - terminate
            headers = {**self.config.default_headers, **rule.headers}

            body = rule.body
            if body is None:
                body = {"error": f"Request terminated: {rule.reason.value}"}

            if self.config.log_terminations:
                logger.info(
                    f"Request terminated by rule '{rule.name}'",
                    extra={
                        "event_type": "request_terminated",
                        "rule": rule.name,
                        "path": path,
                        "method": method,
                        "status_code": rule.status_code,
                        "reason": rule.reason.value,
                    }
                )

            return TerminationResult(
                terminate=True,
                rule=rule,
                status_code=rule.status_code,
                body=body,
                headers=headers,
                reason=rule.reason.value,
            )

        return TerminationResult(terminate=False)

    def enable_maintenance(self, message: Optional[str] = None):
        """Enable maintenance mode."""
        self.config.maintenance_mode = True
        if message:
            self.config.maintenance_message = message
        logger.info("Maintenance mode enabled")

    def disable_maintenance(self):
        """Disable maintenance mode."""
        self.config.maintenance_mode = False
        logger.info("Maintenance mode disabled")

    def add_rule(self, rule: TerminationRule):
        """Add a termination rule."""
        self.config.rules.append(rule)
        self._compile_rules()

    def remove_rule(self, name: str) -> bool:
        """Remove a termination rule by name."""
        for i, rule in enumerate(self.config.rules):
            if rule.name == name:
                del self.config.rules[i]
                self._compile_rules()
                return True
        return False

    def enable_rule(self, name: str) -> bool:
        """Enable a rule by name."""
        for rule in self.config.rules:
            if rule.name == name:
                rule.enabled = True
                return True
        return False

    def disable_rule(self, name: str) -> bool:
        """Disable a rule by name."""
        for rule in self.config.rules:
            if rule.name == name:
                rule.enabled = False
                return True
        return False

    def attach(self, app: FastAPI):
        """Attach termination as middleware."""
        termination = self

        class TerminationMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                result = termination.check(request)

                if result.terminate:
                    body = result.body
                    content_type = result.rule.content_type if result.rule else "application/json"

                    if isinstance(body, dict):
                        return JSONResponse(
                            content=body,
                            status_code=result.status_code,
                            headers=result.headers,
                        )
                    else:
                        return PlainTextResponse(
                            content=str(body) if body else "",
                            status_code=result.status_code,
                            headers=result.headers,
                            media_type=content_type,
                        )

                return await call_next(request)

        app.add_middleware(TerminationMiddleware)
        logger.info("Request termination middleware attached")


# Preset rules
TERMINATION_PRESETS = {
    "maintenance": TerminationConfig(
        maintenance_mode=True,
        maintenance_message="Service is temporarily unavailable for scheduled maintenance.",
        maintenance_status_code=503,
    ),
    "deprecation": TerminationConfig(
        rules=[
            TerminationRule(
                name="deprecate-old-api",
                route_pattern="/api/v1/*",
                status_code=410,
                body={
                    "error": "Gone",
                    "message": "API v1 has been deprecated. Please migrate to API v2.",
                    "documentation": "https://docs.example.com/migration",
                },
                reason=TerminationReason.DEPRECATED,
                headers={"Sunset": "Sat, 01 Jan 2025 00:00:00 GMT"},
            ),
        ],
    ),
    "geo_block": TerminationConfig(
        rules=[
            TerminationRule(
                name="block-restricted-countries",
                condition="request.headers.get('CF-IPCountry', '') in ['XX', 'YY', 'ZZ']",
                status_code=403,
                body={"error": "Access denied from your region"},
                reason=TerminationReason.BLOCKED,
            ),
        ],
    ),
}


# Singleton
_termination: Optional[RequestTermination] = None


def get_request_termination(
    config: Optional[TerminationConfig] = None
) -> RequestTermination:
    """Get or create request termination singleton."""
    global _termination

    if _termination is None:
        _termination = RequestTermination(config)

    return _termination

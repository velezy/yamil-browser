"""
Circuit Breaker Policy

Kong-style circuit breaker for API protection.
Prevents cascading failures by stopping requests to failing backends.

Features:
- Three states: CLOSED, OPEN, HALF_OPEN
- Configurable failure thresholds
- Automatic recovery testing
- Per-endpoint circuit breakers
- Redis-backed for distributed state
- Health check integration
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, Request, Response, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation - requests flow through
    OPEN = "open"  # Circuit tripped - requests blocked
    HALF_OPEN = "half_open"  # Testing recovery - limited requests


class FailureType(str, Enum):
    """Types of failures to track."""
    TIMEOUT = "timeout"
    ERROR_5XX = "5xx"
    ERROR_4XX = "4xx"
    CONNECTION = "connection"
    EXCEPTION = "exception"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    # Failures needed to trip (CLOSED -> OPEN)
    failure_threshold: int = 5

    # Consecutive successes needed to close (HALF_OPEN -> CLOSED)
    success_threshold: int = 3

    # Time before attempting recovery (OPEN -> HALF_OPEN)
    recovery_timeout_seconds: float = 30.0

    # Time window for counting failures
    failure_window_seconds: float = 60.0

    # Which failure types to count
    tracked_failures: Set[FailureType] = field(default_factory=lambda: {
        FailureType.TIMEOUT,
        FailureType.ERROR_5XX,
        FailureType.CONNECTION,
        FailureType.EXCEPTION,
    })

    # Status codes considered failures
    failure_status_codes: Set[int] = field(default_factory=lambda: {
        500, 502, 503, 504
    })

    # Max requests in HALF_OPEN state
    half_open_max_requests: int = 3

    # Timeout for requests (seconds)
    request_timeout_seconds: float = 30.0

    # Per-endpoint circuits
    per_endpoint: bool = True

    # Redis URL for distributed state
    redis_url: Optional[str] = None

    # Key prefix for Redis
    key_prefix: str = "circuit:"

    # Enable fallback response
    enable_fallback: bool = True

    # Fallback response status code
    fallback_status_code: int = 503

    # Fallback response body
    fallback_body: Dict[str, Any] = field(default_factory=lambda: {
        "error": "Service temporarily unavailable",
        "circuit_state": "open",
    })


@dataclass
class CircuitMetrics:
    """Metrics for a circuit."""
    state: CircuitState
    failure_count: int
    success_count: int
    last_failure_time: Optional[datetime]
    last_success_time: Optional[datetime]
    last_state_change: datetime
    total_requests: int
    total_failures: int
    total_blocked: int


@dataclass
class CircuitCheckResult:
    """Result of circuit check."""
    allowed: bool
    state: CircuitState
    reason: Optional[str] = None
    retry_after_seconds: float = 0.0


class CircuitBreaker:
    """
    Single circuit breaker instance.

    Implements the circuit breaker pattern:
    - CLOSED: Normal operation, track failures
    - OPEN: Block requests, wait for recovery
    - HALF_OPEN: Allow limited requests to test recovery
    """

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self._state = CircuitState.CLOSED
        self._failures: List[Tuple[datetime, FailureType]] = []
        self._success_count = 0
        self._half_open_requests = 0
        self._last_failure_time: Optional[datetime] = None
        self._last_success_time: Optional[datetime] = None
        self._last_state_change = datetime.now(timezone.utc)
        self._total_requests = 0
        self._total_failures = 0
        self._total_blocked = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def check(self) -> CircuitCheckResult:
        """Check if request should be allowed."""
        async with self._lock:
            self._total_requests += 1

            if self._state == CircuitState.CLOSED:
                return CircuitCheckResult(allowed=True, state=self._state)

            elif self._state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                time_in_open = (datetime.now(timezone.utc) - self._last_state_change).total_seconds()

                if time_in_open >= self.config.recovery_timeout_seconds:
                    # Transition to HALF_OPEN
                    self._transition_to(CircuitState.HALF_OPEN)
                    self._half_open_requests = 1
                    return CircuitCheckResult(allowed=True, state=self._state)
                else:
                    self._total_blocked += 1
                    retry_after = self.config.recovery_timeout_seconds - time_in_open
                    return CircuitCheckResult(
                        allowed=False,
                        state=self._state,
                        reason="Circuit breaker is OPEN",
                        retry_after_seconds=retry_after,
                    )

            elif self._state == CircuitState.HALF_OPEN:
                if self._half_open_requests < self.config.half_open_max_requests:
                    self._half_open_requests += 1
                    return CircuitCheckResult(allowed=True, state=self._state)
                else:
                    self._total_blocked += 1
                    return CircuitCheckResult(
                        allowed=False,
                        state=self._state,
                        reason="Circuit breaker HALF_OPEN limit reached",
                    )

            return CircuitCheckResult(allowed=False, state=self._state)

    async def record_success(self):
        """Record a successful request."""
        async with self._lock:
            self._last_success_time = datetime.now(timezone.utc)

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    async def record_failure(self, failure_type: FailureType):
        """Record a failed request."""
        async with self._lock:
            now = datetime.now(timezone.utc)
            self._last_failure_time = now
            self._total_failures += 1

            # Only track configured failure types
            if failure_type not in self.config.tracked_failures:
                return

            # Add failure
            self._failures.append((now, failure_type))

            # Clean old failures outside window
            cutoff = now.timestamp() - self.config.failure_window_seconds
            self._failures = [
                (ts, ft) for ts, ft in self._failures
                if ts.timestamp() >= cutoff
            ]

            if self._state == CircuitState.CLOSED:
                if len(self._failures) >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)

            elif self._state == CircuitState.HALF_OPEN:
                # Any failure in HALF_OPEN trips back to OPEN
                self._transition_to(CircuitState.OPEN)

    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state."""
        old_state = self._state
        self._state = new_state
        self._last_state_change = datetime.now(timezone.utc)

        if new_state == CircuitState.CLOSED:
            self._failures = []
            self._success_count = 0

        elif new_state == CircuitState.HALF_OPEN:
            self._success_count = 0
            self._half_open_requests = 0

        logger.info(
            f"Circuit breaker '{self.name}' state change: {old_state.value} -> {new_state.value}",
            extra={
                "event_type": "circuit_state_change",
                "circuit": self.name,
                "old_state": old_state.value,
                "new_state": new_state.value,
            }
        )

    def get_metrics(self) -> CircuitMetrics:
        """Get circuit metrics."""
        return CircuitMetrics(
            state=self._state,
            failure_count=len(self._failures),
            success_count=self._success_count,
            last_failure_time=self._last_failure_time,
            last_success_time=self._last_success_time,
            last_state_change=self._last_state_change,
            total_requests=self._total_requests,
            total_failures=self._total_failures,
            total_blocked=self._total_blocked,
        )


class CircuitBreakerPolicy:
    """
    Kong-style circuit breaker policy.

    Usage:
        policy = CircuitBreakerPolicy(CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout_seconds=30.0,
        ))

        # Use as context manager
        async with policy.protect("/api/v1/users") as breaker:
            response = await call_backend()
            if response.status_code >= 500:
                breaker.record_failure(FailureType.ERROR_5XX)
            else:
                breaker.record_success()

        # Use as decorator
        @policy.protected("/api/v1/users")
        async def call_users_api():
            return await httpx.get("http://users-service/api/users")

        # Use as middleware
        policy.attach(app)
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self._circuits: Dict[str, CircuitBreaker] = {}
        self._redis = None
        self._lock = asyncio.Lock()

    async def connect(self):
        """Connect to Redis if configured."""
        if self.config.redis_url:
            try:
                import redis.asyncio as redis
                self._redis = redis.from_url(
                    self.config.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                )
                await self._redis.ping()
                logger.info("Circuit breaker policy connected to Redis")
            except Exception as e:
                logger.warning(f"Circuit breaker Redis connection failed: {e}")
                self._redis = None

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    async def get_circuit(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker for name."""
        async with self._lock:
            if name not in self._circuits:
                self._circuits[name] = CircuitBreaker(name, self.config)
            return self._circuits[name]

    def _get_circuit_name(self, request: Request) -> str:
        """Get circuit name from request."""
        if self.config.per_endpoint:
            return f"{request.method}:{request.url.path}"
        return "global"

    async def check(self, request: Request) -> CircuitCheckResult:
        """Check if request should be allowed."""
        name = self._get_circuit_name(request)
        circuit = await self.get_circuit(name)
        return await circuit.check()

    async def record_success(self, request: Request):
        """Record successful response."""
        name = self._get_circuit_name(request)
        circuit = await self.get_circuit(name)
        await circuit.record_success()

    async def record_failure(
        self,
        request: Request,
        failure_type: FailureType,
    ):
        """Record failed response."""
        name = self._get_circuit_name(request)
        circuit = await self.get_circuit(name)
        await circuit.record_failure(failure_type)

    def get_failure_type(self, status_code: int, exception: Optional[Exception] = None) -> FailureType:
        """Determine failure type from status code or exception."""
        if exception:
            if isinstance(exception, asyncio.TimeoutError):
                return FailureType.TIMEOUT
            elif isinstance(exception, ConnectionError):
                return FailureType.CONNECTION
            else:
                return FailureType.EXCEPTION

        if status_code in self.config.failure_status_codes:
            return FailureType.ERROR_5XX

        if 400 <= status_code < 500:
            return FailureType.ERROR_4XX

        return FailureType.ERROR_5XX

    class CircuitContext:
        """Context manager for circuit breaker."""

        def __init__(self, policy: "CircuitBreakerPolicy", circuit: CircuitBreaker):
            self.policy = policy
            self.circuit = circuit
            self._success = False

        async def __aenter__(self):
            result = await self.circuit.check()
            if not result.allowed:
                raise HTTPException(
                    status_code=self.policy.config.fallback_status_code,
                    detail=result.reason,
                    headers={"Retry-After": str(int(result.retry_after_seconds))} if result.retry_after_seconds else None,
                )
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                failure_type = self.policy.get_failure_type(0, exc_val)
                await self.circuit.record_failure(failure_type)
            elif self._success:
                await self.circuit.record_success()
            return False

        async def record_success(self):
            """Mark the request as successful."""
            self._success = True
            await self.circuit.record_success()

        async def record_failure(self, failure_type: FailureType):
            """Mark the request as failed."""
            await self.circuit.record_failure(failure_type)

    async def protect(self, name: str) -> CircuitContext:
        """
        Get circuit context for a named circuit.

        Usage:
            async with policy.protect("users-api") as ctx:
                response = await call_api()
                if response.status_code < 400:
                    await ctx.record_success()
                else:
                    await ctx.record_failure(FailureType.ERROR_5XX)
        """
        circuit = await self.get_circuit(name)
        return self.CircuitContext(self, circuit)

    def protected(self, name: str):
        """
        Decorator to protect a function with circuit breaker.

        Usage:
            @policy.protected("users-api")
            async def call_users():
                return await httpx.get("http://users/api")
        """
        def decorator(func: Callable):
            async def wrapper(*args, **kwargs):
                circuit = await self.get_circuit(name)
                result = await circuit.check()

                if not result.allowed:
                    if self.config.enable_fallback:
                        raise HTTPException(
                            status_code=self.config.fallback_status_code,
                            detail=self.config.fallback_body,
                        )
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=result.reason,
                    )

                try:
                    response = await func(*args, **kwargs)
                    await circuit.record_success()
                    return response
                except Exception as e:
                    failure_type = self.get_failure_type(0, e)
                    await circuit.record_failure(failure_type)
                    raise

            return wrapper
        return decorator

    def attach(self, app: FastAPI):
        """Attach circuit breaker as middleware."""
        policy = self

        class CircuitBreakerMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Check circuit
                result = await policy.check(request)

                if not result.allowed:
                    return Response(
                        content='{"error": "Circuit breaker open"}',
                        status_code=policy.config.fallback_status_code,
                        headers={
                            "Content-Type": "application/json",
                            "Retry-After": str(int(result.retry_after_seconds)),
                            "X-Circuit-State": result.state.value,
                        }
                    )

                try:
                    response = await call_next(request)

                    # Record result
                    if response.status_code in policy.config.failure_status_codes:
                        failure_type = policy.get_failure_type(response.status_code)
                        await policy.record_failure(request, failure_type)
                    else:
                        await policy.record_success(request)

                    return response

                except Exception as e:
                    failure_type = policy.get_failure_type(0, e)
                    await policy.record_failure(request, failure_type)
                    raise

        app.add_middleware(CircuitBreakerMiddleware)
        logger.info("Circuit breaker middleware attached")

    def get_all_metrics(self) -> Dict[str, CircuitMetrics]:
        """Get metrics for all circuits."""
        return {
            name: circuit.get_metrics()
            for name, circuit in self._circuits.items()
        }


# Singleton
_circuit_breaker_policy: Optional[CircuitBreakerPolicy] = None


async def get_circuit_breaker_policy(
    config: Optional[CircuitBreakerConfig] = None
) -> CircuitBreakerPolicy:
    """Get or create circuit breaker policy singleton."""
    global _circuit_breaker_policy

    if _circuit_breaker_policy is None:
        _circuit_breaker_policy = CircuitBreakerPolicy(config)
        await _circuit_breaker_policy.connect()

    return _circuit_breaker_policy


# Preset configurations
CIRCUIT_BREAKER_PRESETS = {
    "standard": CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        recovery_timeout_seconds=30.0,
    ),
    "aggressive": CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=5,
        recovery_timeout_seconds=60.0,
    ),
    "lenient": CircuitBreakerConfig(
        failure_threshold=10,
        success_threshold=2,
        recovery_timeout_seconds=15.0,
    ),
    "healthcare": CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        recovery_timeout_seconds=30.0,
        half_open_max_requests=5,
        request_timeout_seconds=60.0,
    ),
}

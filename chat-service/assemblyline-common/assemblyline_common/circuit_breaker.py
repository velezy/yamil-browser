"""
Circuit Breaker implementation for connector resilience.

Enterprise Features:
- Three states: CLOSED, OPEN, HALF_OPEN
- Redis-backed for distributed state across service replicas
- Configurable failure thresholds and timeouts
- Per-endpoint circuit breakers
- Metrics and event logging

Usage:
    from assemblyline_common.circuit_breaker import get_circuit_breaker, CircuitBreakerConfig

    cb = await get_circuit_breaker()

    # Check if circuit allows request
    if await cb.can_execute("api.example.com/endpoint"):
        try:
            result = await make_request()
            await cb.record_success("api.example.com/endpoint")
        except Exception as e:
            await cb.record_failure("api.example.com/endpoint")
            raise
    else:
        raise CircuitOpenError("Circuit is open")
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, Callable, TypeVar, Awaitable
from functools import wraps

import redis.asyncio as redis

from assemblyline_common.config import settings

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests allowed
    OPEN = "open"          # Failures exceeded threshold, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitOpenError(Exception):
    """Raised when circuit is open and request cannot proceed."""
    def __init__(self, circuit_name: str, retry_after: float = 0):
        self.circuit_name = circuit_name
        self.retry_after = retry_after
        super().__init__(f"Circuit '{circuit_name}' is open. Retry after {retry_after:.1f}s")


# ============================================================================
# Per-Connector Type Configurations (C4: Enterprise Readiness)
# ============================================================================

# Connector-type-specific thresholds tuned for healthcare integration patterns.
# Key insight: Epic/FHIR can be flaky under load but recovers quickly;
# SFTP connections are slow to establish; databases should fail fast.
CONNECTOR_TYPE_CONFIGS: Dict[str, "CircuitBreakerConfig"] = {}  # populated after class definition


def get_connector_circuit_name(tenant_id: str, connector_id: str) -> str:
    """Build a deterministic circuit name for a connector instance."""
    return f"connector:{tenant_id}:{connector_id}"


def get_connector_config(connector_type: str) -> "CircuitBreakerConfig":
    """Get circuit breaker config tuned for a specific connector type."""
    return CONNECTOR_TYPE_CONFIGS.get(connector_type, CircuitBreakerConfig())


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    # Failure threshold to trip the circuit
    failure_threshold: int = 5
    # Number of successes needed to close circuit from half-open
    success_threshold: int = 3
    # Time in seconds before attempting recovery (half-open)
    timeout_seconds: float = 60.0
    # Time window in seconds for counting failures
    failure_window_seconds: float = 60.0
    # Maximum concurrent requests in half-open state
    half_open_max_requests: int = 3
    # Redis key prefix
    key_prefix: str = "circuit_breaker"
    # Redis TTL for circuit state (should be > timeout_seconds)
    redis_ttl_seconds: int = 300


# Populate connector-type configs now that CircuitBreakerConfig is defined
CONNECTOR_TYPE_CONFIGS.update({
    # Epic FHIR: tolerant — can be flaky under load but recovers quickly
    "epic_fhir": CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        timeout_seconds=30.0,
        failure_window_seconds=60.0,
    ),
    # Epic Vendor Services: same as FHIR
    "epic_vendor_services": CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        timeout_seconds=30.0,
        failure_window_seconds=60.0,
    ),
    # SFTP: connections are slow to establish, fail faster but wait longer to recover
    "sftp": CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout_seconds=60.0,
        failure_window_seconds=30.0,
    ),
    # Database connectors: fail fast, recover fast
    "database": CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout_seconds=15.0,
        failure_window_seconds=10.0,
    ),
    # HTTP/REST APIs: balanced defaults
    "http": CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        timeout_seconds=30.0,
        failure_window_seconds=60.0,
    ),
    "rest_api": CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        timeout_seconds=30.0,
        failure_window_seconds=60.0,
    ),
})


@dataclass
class CircuitStats:
    """Statistics for a circuit breaker."""
    state: CircuitState
    failure_count: int
    success_count: int
    last_failure_time: Optional[float]
    last_success_time: Optional[float]
    last_state_change: float
    half_open_requests: int = 0


class CircuitBreaker:
    """
    Distributed circuit breaker with Redis backend.

    State Machine:
    - CLOSED: Normal operation. Count failures in sliding window.
              If failures >= threshold, transition to OPEN.
    - OPEN: All requests fail immediately with CircuitOpenError.
            After timeout_seconds, transition to HALF_OPEN.
    - HALF_OPEN: Allow limited requests to test recovery.
                 If success_threshold reached, transition to CLOSED.
                 If any failure, transition back to OPEN.
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        config: Optional[CircuitBreakerConfig] = None
    ):
        self.redis = redis_client
        self.config = config or CircuitBreakerConfig()
        self._local_cache: Dict[str, CircuitStats] = {}
        self._cache_ttl = 1.0  # Local cache TTL in seconds
        self._last_cache_update: Dict[str, float] = {}

    def _get_redis_keys(self, circuit_name: str) -> Dict[str, str]:
        """Get Redis keys for a circuit."""
        prefix = f"{self.config.key_prefix}:{circuit_name}"
        return {
            "state": f"{prefix}:state",
            "failures": f"{prefix}:failures",
            "successes": f"{prefix}:successes",
            "last_failure": f"{prefix}:last_failure",
            "last_success": f"{prefix}:last_success",
            "state_change": f"{prefix}:state_change",
            "half_open_requests": f"{prefix}:half_open_requests",
        }

    async def _get_state(self, circuit_name: str) -> CircuitStats:
        """Get current circuit state from Redis."""
        # Check local cache first
        cache_time = self._last_cache_update.get(circuit_name, 0)
        if time.time() - cache_time < self._cache_ttl and circuit_name in self._local_cache:
            return self._local_cache[circuit_name]

        keys = self._get_redis_keys(circuit_name)

        if self.redis:
            try:
                pipe = self.redis.pipeline()
                pipe.get(keys["state"])
                pipe.get(keys["failures"])
                pipe.get(keys["successes"])
                pipe.get(keys["last_failure"])
                pipe.get(keys["last_success"])
                pipe.get(keys["state_change"])
                pipe.get(keys["half_open_requests"])
                results = await pipe.execute()

                state_str = results[0]
                state = CircuitState(state_str.decode() if state_str else "closed")

                stats = CircuitStats(
                    state=state,
                    failure_count=int(results[1] or 0),
                    success_count=int(results[2] or 0),
                    last_failure_time=float(results[3]) if results[3] else None,
                    last_success_time=float(results[4]) if results[4] else None,
                    last_state_change=float(results[5]) if results[5] else time.time(),
                    half_open_requests=int(results[6] or 0),
                )
            except Exception as e:
                logger.warning(f"Redis error getting circuit state: {e}, using local state")
                stats = self._local_cache.get(circuit_name, CircuitStats(
                    state=CircuitState.CLOSED,
                    failure_count=0,
                    success_count=0,
                    last_failure_time=None,
                    last_success_time=None,
                    last_state_change=time.time(),
                ))
        else:
            # Fallback to local state
            stats = self._local_cache.get(circuit_name, CircuitStats(
                state=CircuitState.CLOSED,
                failure_count=0,
                success_count=0,
                last_failure_time=None,
                last_success_time=None,
                last_state_change=time.time(),
            ))

        self._local_cache[circuit_name] = stats
        self._last_cache_update[circuit_name] = time.time()
        return stats

    async def _set_state(
        self,
        circuit_name: str,
        state: CircuitState,
        failure_count: Optional[int] = None,
        success_count: Optional[int] = None,
        half_open_requests: Optional[int] = None,
    ) -> None:
        """Update circuit state in Redis."""
        keys = self._get_redis_keys(circuit_name)
        now = time.time()

        if self.redis:
            try:
                pipe = self.redis.pipeline()
                pipe.set(keys["state"], state.value, ex=self.config.redis_ttl_seconds)
                pipe.set(keys["state_change"], str(now), ex=self.config.redis_ttl_seconds)

                if failure_count is not None:
                    pipe.set(keys["failures"], str(failure_count), ex=self.config.redis_ttl_seconds)
                if success_count is not None:
                    pipe.set(keys["successes"], str(success_count), ex=self.config.redis_ttl_seconds)
                if half_open_requests is not None:
                    pipe.set(keys["half_open_requests"], str(half_open_requests), ex=self.config.redis_ttl_seconds)

                await pipe.execute()
            except Exception as e:
                logger.warning(f"Redis error setting circuit state: {e}")

        # Update local cache
        if circuit_name in self._local_cache:
            stats = self._local_cache[circuit_name]
            stats.state = state
            stats.last_state_change = now
            if failure_count is not None:
                stats.failure_count = failure_count
            if success_count is not None:
                stats.success_count = success_count
            if half_open_requests is not None:
                stats.half_open_requests = half_open_requests

        logger.info(
            f"Circuit breaker state change",
            extra={
                "event_type": "circuit_breaker_state_change",
                "circuit_name": circuit_name,
                "new_state": state.value,
                "failure_count": failure_count,
                "success_count": success_count,
            }
        )

    async def can_execute(self, circuit_name: str) -> bool:
        """
        Check if a request can proceed through the circuit.

        Returns True if the circuit allows the request.
        For HALF_OPEN state, may return False if max concurrent requests reached.
        """
        stats = await self._get_state(circuit_name)
        now = time.time()

        if stats.state == CircuitState.CLOSED:
            return True

        if stats.state == CircuitState.OPEN:
            # Check if timeout has elapsed
            time_in_open = now - stats.last_state_change
            if time_in_open >= self.config.timeout_seconds:
                # Transition to half-open
                await self._set_state(
                    circuit_name,
                    CircuitState.HALF_OPEN,
                    success_count=0,
                    half_open_requests=0,
                )
                return True
            return False

        if stats.state == CircuitState.HALF_OPEN:
            # Allow limited requests in half-open state
            if stats.half_open_requests < self.config.half_open_max_requests:
                return True
            return False

        return True

    async def get_retry_after(self, circuit_name: str) -> float:
        """Get seconds until circuit might allow requests."""
        stats = await self._get_state(circuit_name)
        if stats.state != CircuitState.OPEN:
            return 0

        time_in_open = time.time() - stats.last_state_change
        return max(0, self.config.timeout_seconds - time_in_open)

    async def record_success(self, circuit_name: str) -> None:
        """Record a successful request."""
        stats = await self._get_state(circuit_name)
        now = time.time()

        if self.redis:
            try:
                keys = self._get_redis_keys(circuit_name)
                await self.redis.set(keys["last_success"], str(now), ex=self.config.redis_ttl_seconds)
            except Exception as e:
                logger.warning(f"Redis error recording success: {e}")

        if stats.state == CircuitState.HALF_OPEN:
            new_success_count = stats.success_count + 1
            new_half_open = stats.half_open_requests + 1

            if new_success_count >= self.config.success_threshold:
                # Recovery confirmed, close the circuit
                await self._set_state(
                    circuit_name,
                    CircuitState.CLOSED,
                    failure_count=0,
                    success_count=0,
                    half_open_requests=0,
                )
                logger.info(
                    f"Circuit breaker recovered",
                    extra={
                        "event_type": "circuit_breaker_recovered",
                        "circuit_name": circuit_name,
                    }
                )
            else:
                await self._set_state(
                    circuit_name,
                    CircuitState.HALF_OPEN,
                    success_count=new_success_count,
                    half_open_requests=new_half_open,
                )
        elif stats.state == CircuitState.CLOSED:
            # In closed state, successes reset failure count
            if stats.failure_count > 0:
                await self._set_state(circuit_name, CircuitState.CLOSED, failure_count=0)

    async def record_failure(self, circuit_name: str, error: Optional[Exception] = None) -> None:
        """Record a failed request."""
        stats = await self._get_state(circuit_name)
        now = time.time()

        if self.redis:
            try:
                keys = self._get_redis_keys(circuit_name)
                await self.redis.set(keys["last_failure"], str(now), ex=self.config.redis_ttl_seconds)
            except Exception as e:
                logger.warning(f"Redis error recording failure: {e}")

        if stats.state == CircuitState.HALF_OPEN:
            # Any failure in half-open trips the circuit back to open
            await self._set_state(
                circuit_name,
                CircuitState.OPEN,
                failure_count=stats.failure_count + 1,
                success_count=0,
                half_open_requests=0,
            )
            logger.warning(
                f"Circuit breaker tripped (half-open failure)",
                extra={
                    "event_type": "circuit_breaker_tripped",
                    "circuit_name": circuit_name,
                    "error": str(error) if error else None,
                }
            )
        elif stats.state == CircuitState.CLOSED:
            # Check if failure is within the sliding window
            new_failure_count = stats.failure_count + 1

            # Clean up old failures (simple approach: reset if last failure is old)
            if stats.last_failure_time:
                time_since_last = now - stats.last_failure_time
                if time_since_last > self.config.failure_window_seconds:
                    new_failure_count = 1

            if new_failure_count >= self.config.failure_threshold:
                # Trip the circuit
                await self._set_state(
                    circuit_name,
                    CircuitState.OPEN,
                    failure_count=new_failure_count,
                    success_count=0,
                )
                logger.warning(
                    f"Circuit breaker tripped",
                    extra={
                        "event_type": "circuit_breaker_tripped",
                        "circuit_name": circuit_name,
                        "failure_count": new_failure_count,
                        "error": str(error) if error else None,
                    }
                )
            else:
                await self._set_state(
                    circuit_name,
                    CircuitState.CLOSED,
                    failure_count=new_failure_count,
                )

    async def get_stats(self, circuit_name: str) -> CircuitStats:
        """Get current statistics for a circuit."""
        return await self._get_state(circuit_name)

    async def reset(self, circuit_name: str) -> None:
        """Manually reset a circuit to closed state."""
        await self._set_state(
            circuit_name,
            CircuitState.CLOSED,
            failure_count=0,
            success_count=0,
            half_open_requests=0,
        )
        logger.info(
            f"Circuit breaker manually reset",
            extra={
                "event_type": "circuit_breaker_reset",
                "circuit_name": circuit_name,
            }
        )

    async def get_all_circuit_stats(self, prefix: str = "connector:") -> Dict[str, Dict[str, Any]]:
        """
        Get stats for all tracked circuits matching a prefix.

        Returns dict keyed by circuit name with state, failure count, etc.
        Useful for the dashboard / connectors API.
        """
        results: Dict[str, Dict[str, Any]] = {}

        # Check Redis for all matching keys
        if self.redis:
            try:
                pattern = f"{self.config.key_prefix}:{prefix}*:state"
                cursor = 0
                keys = []
                while True:
                    cursor, batch = await self.redis.scan(cursor, match=pattern, count=100)
                    keys.extend(batch)
                    if cursor == 0:
                        break

                for key in keys:
                    # Extract circuit name from key: circuit_breaker:connector:tenant:id:state
                    key_str = key.decode() if isinstance(key, bytes) else key
                    # Remove prefix and :state suffix
                    circuit_name = key_str[len(self.config.key_prefix) + 1:-6]  # strip ":state"
                    stats = await self._get_state(circuit_name)
                    results[circuit_name] = {
                        "state": stats.state.value,
                        "failure_count": stats.failure_count,
                        "success_count": stats.success_count,
                        "last_failure_time": stats.last_failure_time,
                        "last_state_change": stats.last_state_change,
                    }
            except Exception as e:
                logger.warning(f"Failed to scan circuit stats from Redis: {e}")

        # Also include local cache entries
        for name, stats in self._local_cache.items():
            if name.startswith(prefix) and name not in results:
                results[name] = {
                    "state": stats.state.value,
                    "failure_count": stats.failure_count,
                    "success_count": stats.success_count,
                    "last_failure_time": stats.last_failure_time,
                    "last_state_change": stats.last_state_change,
                }

        return results

    def decorator(
        self,
        circuit_name: str,
        fallback: Optional[Callable[..., Awaitable[T]]] = None,
    ) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
        """
        Decorator to wrap async functions with circuit breaker.

        Usage:
            cb = await get_circuit_breaker()

            @cb.decorator("my-service")
            async def call_service():
                return await http_client.get("...")
        """
        def decorator_inner(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> T:
                if not await self.can_execute(circuit_name):
                    retry_after = await self.get_retry_after(circuit_name)
                    if fallback:
                        return await fallback(*args, **kwargs)
                    raise CircuitOpenError(circuit_name, retry_after)

                try:
                    result = await func(*args, **kwargs)
                    await self.record_success(circuit_name)
                    return result
                except Exception as e:
                    await self.record_failure(circuit_name, e)
                    raise

            return wrapper
        return decorator_inner


# Singleton instance
_circuit_breaker: Optional[CircuitBreaker] = None
_circuit_breaker_lock = asyncio.Lock()


async def get_circuit_breaker(
    config: Optional[CircuitBreakerConfig] = None
) -> CircuitBreaker:
    """Get singleton circuit breaker instance."""
    global _circuit_breaker

    if _circuit_breaker is None:
        async with _circuit_breaker_lock:
            if _circuit_breaker is None:
                redis_client = None
                try:
                    redis_client = redis.from_url(
                        settings.REDIS_URL,
                        encoding="utf-8",
                        decode_responses=False,
                    )
                    # Test connection
                    await redis_client.ping()
                    logger.info("Circuit breaker connected to Redis")
                except Exception as e:
                    logger.warning(f"Circuit breaker Redis unavailable: {e}, using local state")
                    redis_client = None

                _circuit_breaker = CircuitBreaker(
                    redis_client=redis_client,
                    config=config or CircuitBreakerConfig(),
                )

    return _circuit_breaker


async def close_circuit_breaker() -> None:
    """Close the circuit breaker and its Redis connection."""
    global _circuit_breaker
    if _circuit_breaker and _circuit_breaker.redis:
        await _circuit_breaker.redis.close()
    _circuit_breaker = None

"""
Adaptive Throttling with load-based adjustments.

Features:
- Load-based throttling
- Circuit breaker integration
- Graceful degradation
- Priority queues
"""

import asyncio
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Callable, Any
from enum import Enum
import statistics

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class LoadLevel(Enum):
    """System load levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class Priority(Enum):
    """Request priority levels."""
    CRITICAL = 0  # Always allowed
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4  # First to be throttled


@dataclass
class ThrottleConfig:
    """Configuration for adaptive throttling."""
    # Load thresholds (percentage)
    low_load_threshold: float = 30.0
    normal_load_threshold: float = 60.0
    high_load_threshold: float = 80.0
    critical_load_threshold: float = 95.0

    # Throttle percentages by load level
    # Percentage of requests to allow at each priority level
    throttle_rates: Dict[LoadLevel, Dict[Priority, float]] = field(default_factory=lambda: {
        LoadLevel.LOW: {
            Priority.CRITICAL: 100.0,
            Priority.HIGH: 100.0,
            Priority.NORMAL: 100.0,
            Priority.LOW: 100.0,
            Priority.BACKGROUND: 100.0,
        },
        LoadLevel.NORMAL: {
            Priority.CRITICAL: 100.0,
            Priority.HIGH: 100.0,
            Priority.NORMAL: 100.0,
            Priority.LOW: 90.0,
            Priority.BACKGROUND: 80.0,
        },
        LoadLevel.HIGH: {
            Priority.CRITICAL: 100.0,
            Priority.HIGH: 95.0,
            Priority.NORMAL: 80.0,
            Priority.LOW: 50.0,
            Priority.BACKGROUND: 20.0,
        },
        LoadLevel.CRITICAL: {
            Priority.CRITICAL: 100.0,
            Priority.HIGH: 80.0,
            Priority.NORMAL: 50.0,
            Priority.LOW: 10.0,
            Priority.BACKGROUND: 0.0,
        },
    })

    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_requests: int = 3

    # Metrics collection
    metrics_window_seconds: int = 60
    metrics_bucket_count: int = 12  # 5-second buckets

    # Redis settings
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    key_prefix: str = "throttle"

    # Graceful degradation
    degradation_enabled: bool = True
    degraded_response_cache_seconds: int = 30


@dataclass
class ThrottleResult:
    """Result of throttle check."""
    allowed: bool
    load_level: LoadLevel
    priority: Priority
    throttle_rate: float
    queue_position: Optional[int] = None
    retry_after: Optional[int] = None
    degraded: bool = False


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker for a specific endpoint/service."""
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    last_state_change: float = 0.0


class LoadMetrics:
    """Collect and analyze load metrics."""

    def __init__(self, window_seconds: int = 60, bucket_count: int = 12):
        self.window_seconds = window_seconds
        self.bucket_count = bucket_count
        self.bucket_duration = window_seconds / bucket_count
        self._buckets: Dict[int, Dict[str, int]] = {}
        self._lock = asyncio.Lock()

    def _current_bucket(self) -> int:
        """Get current time bucket index."""
        return int(time.time() / self.bucket_duration)

    async def record_request(self, success: bool = True, latency_ms: float = 0):
        """Record a request metric."""
        async with self._lock:
            bucket = self._current_bucket()
            if bucket not in self._buckets:
                self._buckets[bucket] = {
                    "total": 0,
                    "success": 0,
                    "failure": 0,
                    "latency_sum": 0,
                }

            self._buckets[bucket]["total"] += 1
            if success:
                self._buckets[bucket]["success"] += 1
            else:
                self._buckets[bucket]["failure"] += 1
            self._buckets[bucket]["latency_sum"] += latency_ms

            # Clean old buckets
            min_bucket = bucket - self.bucket_count
            self._buckets = {
                k: v for k, v in self._buckets.items()
                if k >= min_bucket
            }

    async def get_metrics(self) -> Dict[str, float]:
        """Get aggregated metrics for the window."""
        async with self._lock:
            current = self._current_bucket()
            min_bucket = current - self.bucket_count

            total = 0
            success = 0
            failure = 0
            latency_sum = 0

            for bucket_id, data in self._buckets.items():
                if bucket_id >= min_bucket:
                    total += data["total"]
                    success += data["success"]
                    failure += data["failure"]
                    latency_sum += data["latency_sum"]

            success_rate = (success / total * 100) if total > 0 else 100.0
            avg_latency = (latency_sum / total) if total > 0 else 0.0
            rps = total / self.window_seconds if self.window_seconds > 0 else 0.0

            return {
                "total_requests": total,
                "success_count": success,
                "failure_count": failure,
                "success_rate": success_rate,
                "failure_rate": 100.0 - success_rate,
                "avg_latency_ms": avg_latency,
                "requests_per_second": rps,
            }


class AdaptiveThrottler:
    """
    Adaptive throttling system with load-based adjustments.

    Features:
    - Dynamic load detection
    - Priority-based request handling
    - Circuit breaker integration
    - Graceful degradation
    """

    def __init__(self, config: Optional[ThrottleConfig] = None):
        self.config = config or ThrottleConfig()
        self._redis: Optional[redis.Redis] = None
        self._metrics = LoadMetrics(
            self.config.metrics_window_seconds,
            self.config.metrics_bucket_count
        )
        self._circuits: Dict[str, CircuitBreaker] = {}
        self._circuits_lock = asyncio.Lock()
        self._initialized = False
        self._load_level = LoadLevel.NORMAL
        self._external_load: Optional[float] = None

    async def initialize(self):
        """Initialize Redis connection."""
        if self._initialized:
            return

        try:
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            await self._redis.ping()
            self._initialized = True
            logger.info("Adaptive throttler initialized")
        except redis.RedisError as e:
            logger.warning(f"Redis not available for throttler: {e}")
            self._initialized = True

    async def close(self):
        """Close connections."""
        if self._redis:
            await self._redis.close()
            self._redis = None
        self._initialized = False

    def set_external_load(self, load_percentage: float):
        """Set external load metric (e.g., from system monitoring)."""
        self._external_load = max(0.0, min(100.0, load_percentage))
        self._update_load_level()

    def _update_load_level(self):
        """Update load level based on metrics."""
        load = self._external_load or 50.0  # Default to normal

        if load >= self.config.critical_load_threshold:
            self._load_level = LoadLevel.CRITICAL
        elif load >= self.config.high_load_threshold:
            self._load_level = LoadLevel.HIGH
        elif load >= self.config.normal_load_threshold:
            self._load_level = LoadLevel.NORMAL
        else:
            self._load_level = LoadLevel.LOW

    async def _get_circuit(self, circuit_id: str) -> CircuitBreaker:
        """Get or create circuit breaker."""
        async with self._circuits_lock:
            if circuit_id not in self._circuits:
                self._circuits[circuit_id] = CircuitBreaker()
            return self._circuits[circuit_id]

    async def check_circuit(self, circuit_id: str) -> bool:
        """Check if circuit allows requests."""
        if not self.config.circuit_breaker_enabled:
            return True

        circuit = await self._get_circuit(circuit_id)
        now = time.time()

        if circuit.state == CircuitState.CLOSED:
            return True

        if circuit.state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if now - circuit.last_state_change >= self.config.recovery_timeout:
                async with self._circuits_lock:
                    circuit.state = CircuitState.HALF_OPEN
                    circuit.success_count = 0
                    circuit.last_state_change = now
                logger.info(f"Circuit {circuit_id} transitioning to HALF_OPEN")
                return True
            return False

        # HALF_OPEN - allow limited requests
        return circuit.success_count < self.config.half_open_requests

    async def record_success(self, circuit_id: str):
        """Record successful request for circuit breaker."""
        if not self.config.circuit_breaker_enabled:
            return

        circuit = await self._get_circuit(circuit_id)

        async with self._circuits_lock:
            if circuit.state == CircuitState.HALF_OPEN:
                circuit.success_count += 1
                if circuit.success_count >= self.config.half_open_requests:
                    circuit.state = CircuitState.CLOSED
                    circuit.failure_count = 0
                    circuit.last_state_change = time.time()
                    logger.info(f"Circuit {circuit_id} CLOSED after recovery")
            elif circuit.state == CircuitState.CLOSED:
                circuit.failure_count = max(0, circuit.failure_count - 1)

    async def record_failure(self, circuit_id: str):
        """Record failed request for circuit breaker."""
        if not self.config.circuit_breaker_enabled:
            return

        circuit = await self._get_circuit(circuit_id)
        now = time.time()

        async with self._circuits_lock:
            circuit.failure_count += 1
            circuit.last_failure_time = now

            if circuit.state == CircuitState.HALF_OPEN:
                # Immediate trip back to OPEN
                circuit.state = CircuitState.OPEN
                circuit.last_state_change = now
                logger.warning(f"Circuit {circuit_id} OPEN after half-open failure")
            elif circuit.state == CircuitState.CLOSED:
                if circuit.failure_count >= self.config.failure_threshold:
                    circuit.state = CircuitState.OPEN
                    circuit.last_state_change = now
                    logger.warning(f"Circuit {circuit_id} OPEN after {circuit.failure_count} failures")

    async def check(
        self,
        priority: Priority = Priority.NORMAL,
        circuit_id: Optional[str] = None
    ) -> ThrottleResult:
        """
        Check if request should be allowed based on load and priority.

        Args:
            priority: Request priority level
            circuit_id: Optional circuit breaker identifier

        Returns:
            ThrottleResult with allow/deny decision
        """
        await self.initialize()

        # Check circuit breaker first
        if circuit_id:
            circuit_allowed = await self.check_circuit(circuit_id)
            if not circuit_allowed:
                return ThrottleResult(
                    allowed=False,
                    load_level=self._load_level,
                    priority=priority,
                    throttle_rate=0.0,
                    retry_after=int(self.config.recovery_timeout)
                )

        # Get throttle rate for current load and priority
        throttle_rates = self.config.throttle_rates.get(self._load_level, {})
        throttle_rate = throttle_rates.get(priority, 100.0)

        # Probabilistic throttling
        import random
        allowed = random.random() * 100 <= throttle_rate

        return ThrottleResult(
            allowed=allowed,
            load_level=self._load_level,
            priority=priority,
            throttle_rate=throttle_rate,
            retry_after=1 if not allowed else None
        )

    async def record_request(
        self,
        success: bool = True,
        latency_ms: float = 0,
        circuit_id: Optional[str] = None
    ):
        """Record request outcome for metrics and circuit breaker."""
        await self._metrics.record_request(success, latency_ms)

        if circuit_id:
            if success:
                await self.record_success(circuit_id)
            else:
                await self.record_failure(circuit_id)

        # Update load based on metrics
        metrics = await self._metrics.get_metrics()
        # Use failure rate as a proxy for load
        failure_rate = metrics.get("failure_rate", 0)
        self.set_external_load(failure_rate)

    async def get_status(self) -> Dict[str, Any]:
        """Get current throttler status."""
        metrics = await self._metrics.get_metrics()

        circuits_status = {}
        async with self._circuits_lock:
            for cid, circuit in self._circuits.items():
                circuits_status[cid] = {
                    "state": circuit.state.value,
                    "failure_count": circuit.failure_count,
                    "success_count": circuit.success_count,
                }

        return {
            "load_level": self._load_level.value,
            "external_load": self._external_load,
            "metrics": metrics,
            "circuits": circuits_status,
            "config": {
                "circuit_breaker_enabled": self.config.circuit_breaker_enabled,
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
            }
        }


# Singleton instance
_throttler: Optional[AdaptiveThrottler] = None
_throttler_lock = asyncio.Lock()


async def get_adaptive_throttler(config: Optional[ThrottleConfig] = None) -> AdaptiveThrottler:
    """Get or create the singleton throttler instance."""
    global _throttler

    async with _throttler_lock:
        if _throttler is None:
            _throttler = AdaptiveThrottler(config)
            await _throttler.initialize()
        return _throttler

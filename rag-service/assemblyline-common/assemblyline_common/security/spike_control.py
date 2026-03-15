"""
Spike Control Policy

MuleSoft-style spike control to protect against sudden traffic bursts.
Different from rate limiting - focuses on smoothing traffic spikes.

Features:
- Time-window based spike detection
- Request queuing during spikes
- Gradual request release
- Configurable spike thresholds
- Per-tenant spike isolation
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class SpikeAction(str, Enum):
    """Action to take during spike."""
    QUEUE = "queue"  # Queue requests and process gradually
    REJECT = "reject"  # Reject excess requests immediately
    DELAY = "delay"  # Add artificial delay
    THROTTLE = "throttle"  # Reduce throughput


class SpikeState(str, Enum):
    """Current spike state."""
    NORMAL = "normal"  # Normal traffic
    WARNING = "warning"  # Approaching spike threshold
    SPIKE = "spike"  # Spike detected
    RECOVERY = "recovery"  # Recovering from spike


@dataclass
class SpikeControlConfig:
    """Configuration for spike control."""
    # Maximum requests per time window
    max_requests_per_window: int = 100

    # Time window in seconds
    time_window_seconds: float = 1.0

    # Spike threshold multiplier (e.g., 2.0 = 2x normal rate = spike)
    spike_threshold_multiplier: float = 2.0

    # Warning threshold multiplier
    warning_threshold_multiplier: float = 1.5

    # Action to take during spike
    spike_action: SpikeAction = SpikeAction.QUEUE

    # Maximum queue size (for QUEUE action)
    max_queue_size: int = 1000

    # Maximum queue wait time in seconds
    max_queue_wait_seconds: float = 30.0

    # Delay to add during spike (for DELAY action) in milliseconds
    spike_delay_ms: int = 100

    # Recovery time in seconds
    recovery_time_seconds: float = 10.0

    # Enable per-tenant spike control
    per_tenant: bool = True

    # Redis URL for distributed spike control
    redis_url: Optional[str] = None


@dataclass
class SpikeMetrics:
    """Metrics for spike control."""
    current_rate: float  # Requests per second
    window_count: int  # Requests in current window
    state: SpikeState
    queue_size: int
    rejected_count: int
    delayed_count: int
    last_spike_time: Optional[datetime]


@dataclass
class SpikeCheckResult:
    """Result of spike check."""
    allowed: bool
    state: SpikeState
    action: Optional[SpikeAction]
    wait_time_seconds: float = 0.0
    position_in_queue: int = 0
    reason: Optional[str] = None


class SlidingWindowCounter:
    """Sliding window counter for accurate rate measurement."""

    def __init__(self, window_seconds: float, precision: int = 10):
        """
        Initialize sliding window counter.

        Args:
            window_seconds: Window size in seconds
            precision: Number of sub-windows for precision
        """
        self.window_seconds = window_seconds
        self.precision = precision
        self.bucket_seconds = window_seconds / precision
        self.buckets: Deque[Tuple[float, int]] = deque(maxlen=precision)
        self._lock = asyncio.Lock()

    async def increment(self) -> int:
        """Increment counter and return current count."""
        async with self._lock:
            now = time.time()
            self._cleanup(now)

            # Add to current bucket
            if self.buckets and (now - self.buckets[-1][0]) < self.bucket_seconds:
                # Same bucket - increment
                timestamp, count = self.buckets[-1]
                self.buckets[-1] = (timestamp, count + 1)
            else:
                # New bucket
                self.buckets.append((now, 1))

            return self._get_count(now)

    def _cleanup(self, now: float):
        """Remove expired buckets."""
        cutoff = now - self.window_seconds
        while self.buckets and self.buckets[0][0] < cutoff:
            self.buckets.popleft()

    def _get_count(self, now: float) -> int:
        """Get total count in window."""
        cutoff = now - self.window_seconds
        total = 0
        for timestamp, count in self.buckets:
            if timestamp >= cutoff:
                total += count
        return total

    async def get_count(self) -> int:
        """Get current count."""
        async with self._lock:
            now = time.time()
            self._cleanup(now)
            return self._get_count(now)

    async def get_rate(self) -> float:
        """Get current rate (requests per second)."""
        count = await self.get_count()
        return count / self.window_seconds


class RequestQueue:
    """Queue for holding requests during spikes."""

    def __init__(self, max_size: int, max_wait_seconds: float):
        self.max_size = max_size
        self.max_wait_seconds = max_wait_seconds
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self._processing = False
        self._release_rate: float = 0.0

    async def enqueue(self) -> Tuple[bool, int, float]:
        """
        Try to enqueue a request.

        Returns:
            Tuple of (success, position, estimated_wait)
        """
        if self._queue.full():
            return False, 0, 0.0

        try:
            event = asyncio.Event()
            position = self._queue.qsize() + 1
            estimated_wait = position / max(self._release_rate, 1.0)

            if estimated_wait > self.max_wait_seconds:
                return False, 0, 0.0

            self._queue.put_nowait(event)
            return True, position, estimated_wait
        except asyncio.QueueFull:
            return False, 0, 0.0

    async def wait(self, event: asyncio.Event, timeout: float) -> bool:
        """Wait for queue position."""
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def set_release_rate(self, rate: float):
        """Set the release rate (requests per second)."""
        self._release_rate = rate

    @property
    def size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()


class SpikeController:
    """
    MuleSoft-style spike control.

    Usage:
        controller = SpikeController(SpikeControlConfig(
            max_requests_per_window=100,
            time_window_seconds=1.0,
            spike_action=SpikeAction.QUEUE,
        ))

        # Check for spike
        result = await controller.check("tenant-123")
        if not result.allowed:
            if result.action == SpikeAction.QUEUE:
                await asyncio.sleep(result.wait_time_seconds)
            else:
                raise HTTPException(429, result.reason)

        # After request completes
        await controller.complete("tenant-123")
    """

    def __init__(self, config: Optional[SpikeControlConfig] = None):
        self.config = config or SpikeControlConfig()
        self._counters: Dict[str, SlidingWindowCounter] = {}
        self._queues: Dict[str, RequestQueue] = {}
        self._states: Dict[str, SpikeState] = {}
        self._metrics: Dict[str, Dict[str, int]] = {}
        self._last_spike: Dict[str, datetime] = {}
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
                logger.info("Spike controller connected to Redis")
            except Exception as e:
                logger.warning(f"Spike controller Redis connection failed: {e}")
                self._redis = None

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    async def disconnect(self):
        """Alias for close() - disconnect from Redis."""
        await self.close()

    def _get_key(self, tenant_id: Optional[str]) -> str:
        """Get key for tenant or global."""
        if self.config.per_tenant and tenant_id:
            return f"tenant:{tenant_id}"
        return "global"

    async def _get_counter(self, key: str) -> SlidingWindowCounter:
        """Get or create counter for key."""
        if key not in self._counters:
            self._counters[key] = SlidingWindowCounter(
                self.config.time_window_seconds
            )
        return self._counters[key]

    async def _get_queue(self, key: str) -> RequestQueue:
        """Get or create queue for key."""
        if key not in self._queues:
            self._queues[key] = RequestQueue(
                self.config.max_queue_size,
                self.config.max_queue_wait_seconds,
            )
        return self._queues[key]

    def _determine_state(self, rate: float) -> SpikeState:
        """Determine current state based on rate."""
        normal_rate = self.config.max_requests_per_window / self.config.time_window_seconds
        warning_threshold = normal_rate * self.config.warning_threshold_multiplier
        spike_threshold = normal_rate * self.config.spike_threshold_multiplier

        if rate >= spike_threshold:
            return SpikeState.SPIKE
        elif rate >= warning_threshold:
            return SpikeState.WARNING
        else:
            return SpikeState.NORMAL

    async def check(
        self,
        tenant_id: Optional[str] = None,
        request: Optional[Request] = None,
    ) -> SpikeCheckResult:
        """
        Check if request should be allowed or controlled.

        Args:
            tenant_id: Optional tenant identifier
            request: Optional FastAPI request

        Returns:
            SpikeCheckResult
        """
        key = self._get_key(tenant_id)
        counter = await self._get_counter(key)

        # Get current rate
        current_count = await counter.get_count()
        current_rate = current_count / self.config.time_window_seconds

        # Determine state
        state = self._determine_state(current_rate)
        old_state = self._states.get(key, SpikeState.NORMAL)
        self._states[key] = state

        # Check for recovery
        if state == SpikeState.NORMAL and old_state in (SpikeState.SPIKE, SpikeState.RECOVERY):
            last_spike = self._last_spike.get(key)
            if last_spike:
                recovery_elapsed = (datetime.now(timezone.utc) - last_spike).total_seconds()
                if recovery_elapsed < self.config.recovery_time_seconds:
                    state = SpikeState.RECOVERY
                    self._states[key] = state

        # Log state transition
        if state != old_state:
            logger.info(
                f"Spike state transition: {old_state.value} -> {state.value}",
                extra={
                    "event_type": "spike_state_change",
                    "key": key,
                    "old_state": old_state.value,
                    "new_state": state.value,
                    "rate": current_rate,
                }
            )

            if state == SpikeState.SPIKE:
                self._last_spike[key] = datetime.now(timezone.utc)

        # Check if under limit
        if current_count < self.config.max_requests_per_window:
            # Increment counter
            await counter.increment()
            return SpikeCheckResult(allowed=True, state=state, action=None)

        # Apply spike control action
        if state in (SpikeState.SPIKE, SpikeState.RECOVERY):
            # Initialize metrics
            if key not in self._metrics:
                self._metrics[key] = {"rejected": 0, "delayed": 0, "queued": 0}

            if self.config.spike_action == SpikeAction.REJECT:
                self._metrics[key]["rejected"] += 1
                return SpikeCheckResult(
                    allowed=False,
                    state=state,
                    action=SpikeAction.REJECT,
                    reason="Traffic spike detected - request rejected",
                )

            elif self.config.spike_action == SpikeAction.DELAY:
                self._metrics[key]["delayed"] += 1
                delay = self.config.spike_delay_ms / 1000.0

                # Increment counter after delay
                await counter.increment()
                return SpikeCheckResult(
                    allowed=True,
                    state=state,
                    action=SpikeAction.DELAY,
                    wait_time_seconds=delay,
                )

            elif self.config.spike_action == SpikeAction.QUEUE:
                queue = await self._get_queue(key)
                success, position, wait_time = await queue.enqueue()

                if not success:
                    self._metrics[key]["rejected"] += 1
                    return SpikeCheckResult(
                        allowed=False,
                        state=state,
                        action=SpikeAction.REJECT,
                        reason="Queue full - request rejected",
                    )

                self._metrics[key]["queued"] += 1
                await counter.increment()
                return SpikeCheckResult(
                    allowed=True,
                    state=state,
                    action=SpikeAction.QUEUE,
                    wait_time_seconds=wait_time,
                    position_in_queue=position,
                )

            elif self.config.spike_action == SpikeAction.THROTTLE:
                # Reduce effective rate
                throttle_factor = 0.5  # 50% throughput
                effective_limit = int(self.config.max_requests_per_window * throttle_factor)

                if current_count < effective_limit:
                    await counter.increment()
                    return SpikeCheckResult(allowed=True, state=state, action=SpikeAction.THROTTLE)
                else:
                    self._metrics[key]["rejected"] += 1
                    return SpikeCheckResult(
                        allowed=False,
                        state=state,
                        action=SpikeAction.THROTTLE,
                        reason="Throttling active - request rejected",
                    )

        # Normal operation - allow if under limit
        await counter.increment()
        return SpikeCheckResult(allowed=True, state=state, action=None)

    async def get_metrics(self, tenant_id: Optional[str] = None) -> SpikeMetrics:
        """Get spike control metrics."""
        key = self._get_key(tenant_id)
        counter = await self._get_counter(key)
        queue = await self._get_queue(key)

        rate = await counter.get_rate()
        count = await counter.get_count()
        state = self._states.get(key, SpikeState.NORMAL)
        metrics = self._metrics.get(key, {"rejected": 0, "delayed": 0, "queued": 0})

        return SpikeMetrics(
            current_rate=rate,
            window_count=count,
            state=state,
            queue_size=queue.size,
            rejected_count=metrics["rejected"],
            delayed_count=metrics["delayed"],
            last_spike_time=self._last_spike.get(key),
        )

    def attach(self, app: FastAPI):
        """Attach spike control as middleware."""
        controller = self

        class SpikeControlMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # Get tenant ID
                tenant_id = getattr(request.state, "tenant_id", None)

                # Check spike control
                result = await controller.check(tenant_id, request)

                if not result.allowed:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=result.reason,
                        headers={
                            "X-Spike-State": result.state.value,
                            "Retry-After": "5",
                        }
                    )

                # Apply delay if needed
                if result.wait_time_seconds > 0:
                    await asyncio.sleep(result.wait_time_seconds)

                return await call_next(request)

        app.add_middleware(SpikeControlMiddleware)
        logger.info("Spike control middleware attached")


# Singleton
_spike_controller: Optional[SpikeController] = None


async def get_spike_controller(
    config: Optional[SpikeControlConfig] = None
) -> SpikeController:
    """Get or create spike controller singleton."""
    global _spike_controller

    if _spike_controller is None:
        _spike_controller = SpikeController(config)
        await _spike_controller.connect()

    return _spike_controller


# Preset configurations
SPIKE_CONTROL_PRESETS = {
    "standard": SpikeControlConfig(
        max_requests_per_window=100,
        time_window_seconds=1.0,
        spike_action=SpikeAction.QUEUE,
    ),
    "strict": SpikeControlConfig(
        max_requests_per_window=50,
        time_window_seconds=1.0,
        spike_action=SpikeAction.REJECT,
        spike_threshold_multiplier=1.5,
    ),
    "lenient": SpikeControlConfig(
        max_requests_per_window=200,
        time_window_seconds=1.0,
        spike_action=SpikeAction.DELAY,
        spike_delay_ms=50,
    ),
    "healthcare": SpikeControlConfig(
        max_requests_per_window=100,
        time_window_seconds=1.0,
        spike_action=SpikeAction.QUEUE,
        max_queue_size=500,
        max_queue_wait_seconds=60.0,
    ),
}

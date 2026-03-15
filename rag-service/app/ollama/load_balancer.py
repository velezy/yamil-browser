"""
Ollama Multi-Instance Load Balancer

Routes requests across multiple Ollama instances with health checking
and backpressure support.

Features:
- Round-robin routing across instances
- Health check with automatic failover
- Request queue with configurable backpressure
- Per-instance metrics tracking
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

import httpx

from .config import OLLAMA_INSTANCES, OLLAMA_MAX_QUEUE_DEPTH

logger = logging.getLogger(__name__)


class InstanceStatus(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"


@dataclass
class InstanceMetrics:
    """Metrics for a single Ollama instance"""
    url: str
    status: InstanceStatus = InstanceStatus.HEALTHY
    total_requests: int = 0
    failed_requests: int = 0
    active_requests: int = 0
    avg_latency_ms: float = 0.0
    total_latency_ms: float = 0.0
    last_health_check: float = 0.0
    consecutive_failures: int = 0

    def record_request(self, latency_ms: float, success: bool):
        self.total_requests += 1
        self.total_latency_ms += latency_ms
        self.avg_latency_ms = self.total_latency_ms / self.total_requests
        if not success:
            self.failed_requests += 1
            self.consecutive_failures += 1
        else:
            self.consecutive_failures = 0


class OllamaLoadBalancer:
    """
    Load balancer for multiple Ollama instances.

    Routes requests using round-robin with health checking.
    Supports backpressure via configurable queue depth.
    """

    def __init__(
        self,
        instances: Optional[List[str]] = None,
        max_queue_depth: int = 50,
        health_check_interval: float = 30.0,
        unhealthy_threshold: int = 3,
    ):
        """
        Initialize load balancer.

        Args:
            instances: List of Ollama instance URLs
            max_queue_depth: Max pending requests before backpressure
            health_check_interval: Seconds between health checks
            unhealthy_threshold: Consecutive failures before marking unhealthy
        """
        self._instances = instances or OLLAMA_INSTANCES
        self._max_queue_depth = max_queue_depth or OLLAMA_MAX_QUEUE_DEPTH
        self._health_check_interval = health_check_interval
        self._unhealthy_threshold = unhealthy_threshold

        self._metrics: Dict[str, InstanceMetrics] = {
            url: InstanceMetrics(url=url) for url in self._instances
        }
        self._current_index = 0
        self._queue_depth = 0
        self._lock = asyncio.Lock()
        self._health_task: Optional[asyncio.Task] = None
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self):
        """Start health checking"""
        self._client = httpx.AsyncClient(timeout=10.0)
        self._health_task = asyncio.create_task(self._health_check_loop())
        logger.info(
            f"Load balancer started with {len(self._instances)} instances: "
            f"{', '.join(self._instances)}"
        )

    async def stop(self):
        """Stop health checking"""
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()

    async def get_instance(self) -> Optional[str]:
        """
        Get the next healthy instance (round-robin).

        Returns:
            Instance URL or None if all unhealthy or queue full
        """
        async with self._lock:
            if self._queue_depth >= self._max_queue_depth:
                logger.warning(
                    f"Backpressure: queue depth {self._queue_depth} >= "
                    f"max {self._max_queue_depth}"
                )
                return None

            # Find next healthy instance
            healthy = [
                url for url, m in self._metrics.items()
                if m.status == InstanceStatus.HEALTHY
            ]

            if not healthy:
                logger.error("No healthy Ollama instances available")
                return None

            # Round-robin among healthy instances
            self._current_index = self._current_index % len(healthy)
            instance = healthy[self._current_index]
            self._current_index += 1

            self._queue_depth += 1
            self._metrics[instance].active_requests += 1

            return instance

    async def release_instance(self, url: str, latency_ms: float, success: bool):
        """Release an instance back to the pool after request completion"""
        async with self._lock:
            self._queue_depth = max(0, self._queue_depth - 1)
            if url in self._metrics:
                self._metrics[url].active_requests = max(
                    0, self._metrics[url].active_requests - 1
                )
                self._metrics[url].record_request(latency_ms, success)

                # Mark unhealthy if too many consecutive failures
                if self._metrics[url].consecutive_failures >= self._unhealthy_threshold:
                    self._metrics[url].status = InstanceStatus.UNHEALTHY
                    logger.warning(f"Instance {url} marked unhealthy")

    async def request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> Optional[httpx.Response]:
        """
        Route a request to a healthy instance.

        Args:
            method: HTTP method (GET, POST)
            path: API path (e.g., /api/generate)
            **kwargs: Additional httpx request kwargs

        Returns:
            Response or None if unavailable
        """
        instance = await self.get_instance()
        if instance is None:
            return None

        start = time.time()
        success = False
        try:
            url = f"{instance}{path}"
            response = await self._client.request(method, url, **kwargs)
            success = response.status_code < 500
            return response
        except Exception as e:
            logger.error(f"Request to {instance}{path} failed: {e}")
            return None
        finally:
            latency_ms = (time.time() - start) * 1000
            await self.release_instance(instance, latency_ms, success)

    async def _health_check_loop(self):
        """Periodically check instance health"""
        while True:
            try:
                await asyncio.sleep(self._health_check_interval)
                await self._check_all_instances()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")

    async def _check_all_instances(self):
        """Check health of all instances"""
        for url in self._instances:
            try:
                response = await self._client.get(
                    f"{url}/api/tags",
                    timeout=5.0,
                )
                if response.status_code == 200:
                    if self._metrics[url].status == InstanceStatus.UNHEALTHY:
                        logger.info(f"Instance {url} recovered")
                    self._metrics[url].status = InstanceStatus.HEALTHY
                    self._metrics[url].consecutive_failures = 0
                else:
                    self._metrics[url].consecutive_failures += 1
                    if self._metrics[url].consecutive_failures >= self._unhealthy_threshold:
                        self._metrics[url].status = InstanceStatus.UNHEALTHY
            except Exception:
                self._metrics[url].consecutive_failures += 1
                if self._metrics[url].consecutive_failures >= self._unhealthy_threshold:
                    self._metrics[url].status = InstanceStatus.UNHEALTHY

            self._metrics[url].last_health_check = time.time()

    def get_status(self) -> Dict[str, Any]:
        """Get load balancer status"""
        return {
            "instances": {
                url: {
                    "status": m.status.value,
                    "total_requests": m.total_requests,
                    "active_requests": m.active_requests,
                    "avg_latency_ms": round(m.avg_latency_ms, 2),
                    "error_rate": m.failed_requests / max(m.total_requests, 1),
                }
                for url, m in self._metrics.items()
            },
            "queue_depth": self._queue_depth,
            "max_queue_depth": self._max_queue_depth,
            "healthy_count": sum(
                1 for m in self._metrics.values()
                if m.status == InstanceStatus.HEALTHY
            ),
            "total_count": len(self._instances),
        }


# Singleton
_load_balancer: Optional[OllamaLoadBalancer] = None


def get_load_balancer() -> OllamaLoadBalancer:
    """Get or create singleton load balancer"""
    global _load_balancer
    if _load_balancer is None:
        _load_balancer = OllamaLoadBalancer()
    return _load_balancer

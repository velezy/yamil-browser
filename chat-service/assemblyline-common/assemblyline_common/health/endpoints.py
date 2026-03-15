"""
Health Check Endpoints for Kubernetes and ECS

Provides standardized health endpoints compatible with:
- Kubernetes liveness/readiness/startup probes
- AWS ECS health checks
- AWS ALB target group health checks
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional

from fastapi import APIRouter, Response, status

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health status values"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class DependencyHealth:
    """Health status for a single dependency"""
    name: str
    healthy: bool
    latency_ms: float = 0.0
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "latency_ms": round(self.latency_ms, 2),
            "message": self.message,
            "details": self.details,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass
class HealthCheckResult:
    """Overall health check result"""
    status: HealthStatus
    service_name: str
    version: str
    uptime_seconds: float
    dependencies: List[DependencyHealth]
    is_ready: bool = True
    is_draining: bool = False
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "version": self.version,
            "uptime_seconds": round(self.uptime_seconds, 2),
            "is_ready": self.is_ready,
            "is_draining": self.is_draining,
            "dependencies": [d.to_dict() for d in self.dependencies],
            "details": self.details,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


class HealthRouter:
    """
    Health check router for FastAPI services.

    Provides three endpoints:
    - /health/live - Always returns 200 if process is running (liveness)
    - /health/ready - Returns 200 only if all dependencies are healthy (readiness)
    - /health/startup - Returns 200 once initial startup is complete

    Usage:
        health_router = HealthRouter(
            service_name="auth-service",
            version="1.0.0"
        )
        health_router.add_dependency_check("postgres", check_postgres)
        health_router.add_dependency_check("redis", check_redis)

        app.include_router(health_router.router)

        # On startup complete
        health_router.mark_startup_complete()

        # On shutdown signal
        health_router.start_draining()
    """

    def __init__(
        self,
        service_name: str,
        version: str = "0.0.0",
        startup_timeout: float = 120.0,
        drain_timeout: float = 30.0,
    ):
        self.service_name = service_name
        self.version = version
        self.startup_timeout = startup_timeout
        self.drain_timeout = drain_timeout

        self._start_time = time.time()
        self._startup_complete = False
        self._is_draining = False
        self._drain_start_time: Optional[float] = None

        # Dependency health check functions
        self._dependency_checks: Dict[
            str, Callable[[], Coroutine[Any, Any, DependencyHealth]]
        ] = {}

        # Cached health results
        self._last_check_time: float = 0
        self._cache_ttl: float = 5.0  # Cache results for 5 seconds
        self._cached_dependencies: List[DependencyHealth] = []

        self.router = self._create_router()

    def _create_router(self) -> APIRouter:
        """Create FastAPI router with health endpoints"""
        router = APIRouter(prefix="/health", tags=["health"])

        @router.get("/live")
        async def liveness():
            """
            Kubernetes liveness probe.
            Returns 200 if the process is running.
            Used to detect deadlocks or hung processes.
            """
            return {
                "status": "alive",
                "service": self.service_name,
                "uptime_seconds": round(time.time() - self._start_time, 2),
            }

        @router.get("/ready")
        async def readiness(response: Response):
            """
            Kubernetes readiness probe.
            Returns 200 only if the service can accept traffic.
            Returns 503 if draining or dependencies unhealthy.
            """
            if self._is_draining:
                response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
                return {
                    "status": "draining",
                    "service": self.service_name,
                    "message": "Service is shutting down",
                    "drain_elapsed_seconds": round(
                        time.time() - (self._drain_start_time or time.time()), 2
                    ),
                }

            result = await self._check_health()

            if result.status != HealthStatus.HEALTHY:
                response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

            return result.to_dict()

        @router.get("/startup")
        async def startup(response: Response):
            """
            Kubernetes startup probe.
            Returns 200 once initial startup is complete.
            Used for slow-starting containers.
            """
            if not self._startup_complete:
                elapsed = time.time() - self._start_time
                if elapsed > self.startup_timeout:
                    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
                    return {
                        "status": "timeout",
                        "service": self.service_name,
                        "message": f"Startup timeout after {elapsed:.2f}s",
                    }

                response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
                return {
                    "status": "starting",
                    "service": self.service_name,
                    "elapsed_seconds": round(elapsed, 2),
                }

            return {
                "status": "started",
                "service": self.service_name,
                "startup_time_seconds": round(self._start_time, 2),
            }

        @router.get("/")
        async def health_summary(response: Response):
            """
            Comprehensive health check with all dependency statuses.
            Useful for debugging and monitoring dashboards.
            """
            result = await self._check_health()

            if result.status != HealthStatus.HEALTHY:
                response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

            return result.to_dict()

        return router

    def add_dependency_check(
        self,
        name: str,
        check_func: Callable[[], Coroutine[Any, Any, DependencyHealth]],
    ) -> None:
        """
        Add a dependency health check function.

        Args:
            name: Name of the dependency (e.g., "postgres", "redis")
            check_func: Async function that returns DependencyHealth
        """
        self._dependency_checks[name] = check_func
        logger.info(f"Added health check for dependency: {name}")

    def remove_dependency_check(self, name: str) -> None:
        """Remove a dependency health check"""
        if name in self._dependency_checks:
            del self._dependency_checks[name]
            logger.info(f"Removed health check for dependency: {name}")

    def mark_startup_complete(self) -> None:
        """Mark the service startup as complete"""
        self._startup_complete = True
        startup_time = time.time() - self._start_time
        logger.info(
            f"Service {self.service_name} startup complete",
            extra={
                "event_type": "startup_complete",
                "startup_time_seconds": startup_time,
            }
        )

    def start_draining(self) -> None:
        """Start draining connections (graceful shutdown)"""
        self._is_draining = True
        self._drain_start_time = time.time()
        logger.info(
            f"Service {self.service_name} starting drain",
            extra={
                "event_type": "drain_started",
                "drain_timeout": self.drain_timeout,
            }
        )

    def stop_draining(self) -> None:
        """Stop draining (cancel shutdown)"""
        self._is_draining = False
        self._drain_start_time = None
        logger.info(f"Service {self.service_name} drain cancelled")

    @property
    def is_draining(self) -> bool:
        """Check if service is draining"""
        return self._is_draining

    @property
    def is_ready(self) -> bool:
        """Check if service is ready to accept traffic"""
        return self._startup_complete and not self._is_draining

    async def _check_health(self) -> HealthCheckResult:
        """Run all dependency health checks"""
        # Use cached results if fresh
        now = time.time()
        if now - self._last_check_time < self._cache_ttl and self._cached_dependencies:
            dependencies = self._cached_dependencies
        else:
            # Run all checks in parallel
            if self._dependency_checks:
                tasks = [
                    self._run_check(name, func)
                    for name, func in self._dependency_checks.items()
                ]
                dependencies = await asyncio.gather(*tasks)
            else:
                dependencies = []

            self._cached_dependencies = dependencies
            self._last_check_time = now

        # Determine overall status
        all_healthy = all(d.healthy for d in dependencies)
        any_unhealthy = any(not d.healthy for d in dependencies)

        if self._is_draining:
            overall_status = HealthStatus.UNHEALTHY
        elif all_healthy:
            overall_status = HealthStatus.HEALTHY
        elif any_unhealthy:
            overall_status = HealthStatus.UNHEALTHY
        else:
            overall_status = HealthStatus.DEGRADED

        return HealthCheckResult(
            status=overall_status,
            service_name=self.service_name,
            version=self.version,
            uptime_seconds=time.time() - self._start_time,
            dependencies=list(dependencies),
            is_ready=self.is_ready,
            is_draining=self._is_draining,
        )

    async def _run_check(
        self,
        name: str,
        func: Callable[[], Coroutine[Any, Any, DependencyHealth]],
    ) -> DependencyHealth:
        """Run a single health check with error handling"""
        start = time.time()
        try:
            result = await asyncio.wait_for(func(), timeout=5.0)
            return result
        except asyncio.TimeoutError:
            return DependencyHealth(
                name=name,
                healthy=False,
                latency_ms=(time.time() - start) * 1000,
                message="Health check timed out",
            )
        except Exception as e:
            logger.error(f"Health check failed for {name}: {e}")
            return DependencyHealth(
                name=name,
                healthy=False,
                latency_ms=(time.time() - start) * 1000,
                message=str(e),
            )


# Singleton instance
_health_router: Optional[HealthRouter] = None


def get_health_router(
    service_name: str = "unknown",
    version: str = "0.0.0",
) -> HealthRouter:
    """Get or create the health router singleton"""
    global _health_router
    if _health_router is None:
        _health_router = HealthRouter(service_name=service_name, version=version)
    return _health_router

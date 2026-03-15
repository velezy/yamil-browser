"""
Health Check Module for Logic Weaver Services

Provides Kubernetes-compatible health endpoints:
- /health/live - Liveness probe (is the process running?)
- /health/ready - Readiness probe (can we accept traffic?)
- /health/startup - Startup probe (has initialization completed?)

Works with both k3s (local) and AWS ECS.
"""

from assemblyline_common.health.endpoints import (
    HealthRouter,
    HealthStatus,
    DependencyHealth,
    get_health_router,
)
from assemblyline_common.health.graceful_shutdown import (
    GracefulShutdownManager,
    get_shutdown_manager,
)
from assemblyline_common.health.dependency_checks import (
    DependencyChecker,
    get_dependency_checker,
)

__all__ = [
    "HealthRouter",
    "HealthStatus",
    "DependencyHealth",
    "get_health_router",
    "GracefulShutdownManager",
    "get_shutdown_manager",
    "DependencyChecker",
    "get_dependency_checker",
]

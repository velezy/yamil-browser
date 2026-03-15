"""
Service Registry Abstraction

Provides a unified interface for service discovery across different backends:
- Kubernetes native DNS
- HashiCorp Consul
- AWS Cloud Map
- Static configuration
"""

import asyncio
import logging
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ServiceStatus(str, Enum):
    """Service instance status"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"
    UNKNOWN = "unknown"


class LoadBalanceStrategy(str, Enum):
    """Load balancing strategies"""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"


@dataclass
class ServiceInstance:
    """Represents a single service instance"""
    service_name: str
    instance_id: str
    host: str
    port: int
    status: ServiceStatus = ServiceStatus.HEALTHY
    weight: int = 100
    zone: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_check: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def address(self) -> str:
        """Get host:port address"""
        return f"{self.host}:{self.port}"

    @property
    def url(self) -> str:
        """Get HTTP URL"""
        scheme = self.metadata.get("scheme", "http")
        return f"{scheme}://{self.host}:{self.port}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service_name": self.service_name,
            "instance_id": self.instance_id,
            "host": self.host,
            "port": self.port,
            "status": self.status.value,
            "weight": self.weight,
            "zone": self.zone,
            "metadata": self.metadata,
            "last_check": self.last_check.isoformat(),
        }


@dataclass
class ServiceRegistration:
    """Registration details for a service"""
    service_name: str
    instance_id: str
    host: str
    port: int
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    health_check_url: str = ""
    health_check_interval: int = 10
    deregister_critical_after: int = 60


class ServiceRegistry(ABC):
    """
    Abstract base class for service registries.

    Usage:
        registry = await get_service_registry()

        # Register this service
        await registry.register(ServiceRegistration(
            service_name="auth-service",
            instance_id="auth-service-1",
            host="10.0.0.1",
            port=8001,
            health_check_url="http://10.0.0.1:8001/health/live",
        ))

        # Discover other services
        instances = await registry.get_instances("flow-service")

        # Get a single healthy instance (load balanced)
        instance = await registry.get_instance("flow-service")

        # Deregister on shutdown
        await registry.deregister("auth-service", "auth-service-1")
    """

    def __init__(
        self,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        prefer_same_zone: bool = True,
        cache_ttl: float = 30.0,
    ):
        self.strategy = strategy
        self.prefer_same_zone = prefer_same_zone
        self.cache_ttl = cache_ttl
        self._current_zone = os.getenv("AVAILABILITY_ZONE", "")

        # Round-robin state
        self._rr_index: Dict[str, int] = {}

        # Instance cache
        self._cache: Dict[str, List[ServiceInstance]] = {}
        self._cache_time: Dict[str, float] = {}

        # Connection tracking for least-connections
        self._connections: Dict[str, int] = {}

    @abstractmethod
    async def register(self, registration: ServiceRegistration) -> bool:
        """Register a service instance"""
        pass

    @abstractmethod
    async def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance"""
        pass

    @abstractmethod
    async def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> List[ServiceInstance]:
        """Get all instances of a service"""
        pass

    async def get_instance(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> Optional[ServiceInstance]:
        """
        Get a single service instance using configured load balancing strategy.

        Returns None if no healthy instances available.
        """
        instances = await self.get_instances(service_name, healthy_only)

        if not instances:
            return None

        # Filter by zone preference if enabled
        if self.prefer_same_zone and self._current_zone:
            same_zone = [i for i in instances if i.zone == self._current_zone]
            if same_zone:
                instances = same_zone

        # Apply load balancing strategy
        if self.strategy == LoadBalanceStrategy.RANDOM:
            return random.choice(instances)

        elif self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            idx = self._rr_index.get(service_name, 0)
            instance = instances[idx % len(instances)]
            self._rr_index[service_name] = idx + 1
            return instance

        elif self.strategy == LoadBalanceStrategy.LEAST_CONNECTIONS:
            return min(
                instances,
                key=lambda i: self._connections.get(i.instance_id, 0)
            )

        elif self.strategy == LoadBalanceStrategy.WEIGHTED:
            # Weighted random selection
            total_weight = sum(i.weight for i in instances)
            r = random.randint(0, total_weight)
            cumulative = 0
            for instance in instances:
                cumulative += instance.weight
                if r <= cumulative:
                    return instance
            return instances[-1]

        return instances[0]

    def track_connection_start(self, instance_id: str) -> None:
        """Track connection started for least-connections balancing"""
        self._connections[instance_id] = self._connections.get(instance_id, 0) + 1

    def track_connection_end(self, instance_id: str) -> None:
        """Track connection ended for least-connections balancing"""
        self._connections[instance_id] = max(
            0, self._connections.get(instance_id, 0) - 1
        )

    @abstractmethod
    async def watch_service(
        self,
        service_name: str,
        callback,
    ) -> None:
        """
        Watch for changes to a service.

        Callback receives list of current instances when changes occur.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close registry connections"""
        pass


# Registry implementations will be loaded dynamically
_registry_instance: Optional[ServiceRegistry] = None


async def get_service_registry(
    backend: Optional[str] = None,
    **kwargs,
) -> ServiceRegistry:
    """
    Get or create the service registry singleton.

    Args:
        backend: Registry backend ("kubernetes", "consul", "cloudmap", "static")
                 Defaults to DISCOVERY_BACKEND env var or "kubernetes"
        **kwargs: Backend-specific configuration

    Returns:
        ServiceRegistry instance
    """
    global _registry_instance

    if _registry_instance is not None:
        return _registry_instance

    backend = backend or os.getenv("DISCOVERY_BACKEND", "kubernetes")

    if backend == "kubernetes":
        from assemblyline_common.discovery.kubernetes import KubernetesServiceRegistry
        _registry_instance = KubernetesServiceRegistry(**kwargs)

    elif backend == "consul":
        from assemblyline_common.discovery.consul import ConsulServiceRegistry
        _registry_instance = ConsulServiceRegistry(**kwargs)

    elif backend == "cloudmap":
        from assemblyline_common.discovery.cloudmap import CloudMapServiceRegistry
        _registry_instance = CloudMapServiceRegistry(**kwargs)

    elif backend == "static":
        from assemblyline_common.discovery.static import StaticServiceRegistry
        _registry_instance = StaticServiceRegistry(**kwargs)

    else:
        raise ValueError(f"Unknown discovery backend: {backend}")

    logger.info(f"Initialized service registry: {backend}")
    return _registry_instance

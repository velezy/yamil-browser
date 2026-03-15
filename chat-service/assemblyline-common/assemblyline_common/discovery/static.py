"""
Static Service Discovery

Simple service discovery for development and testing environments.
Services are configured via environment variables or constructor arguments.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from assemblyline_common.discovery.registry import (
    LoadBalanceStrategy,
    ServiceInstance,
    ServiceRegistration,
    ServiceRegistry,
    ServiceStatus,
)

logger = logging.getLogger(__name__)


# Default service configuration for local development
DEFAULT_SERVICES = {
    "auth-service": [("localhost", 8001)],
    "flow-service": [("localhost", 8002)],
    "inbound-service": [("localhost", 8003)],
    "outbound-service": [("localhost", 8004)],
    "transform-service": [("localhost", 8005)],
    "mapping-service": [("localhost", 8006)],
    "status-service": [("localhost", 8007)],
    "ai-orchestra-service": [("localhost", 8008)],
}


class StaticServiceRegistry(ServiceRegistry):
    """
    Static service discovery for development and testing.

    Services can be configured via:
    1. Constructor argument
    2. Environment variables (SERVICE_<NAME>_URL)
    3. Default localhost configuration

    Usage:
        # Default localhost configuration
        registry = StaticServiceRegistry()

        # Custom configuration
        registry = StaticServiceRegistry(
            services={
                "auth-service": [("10.0.0.1", 8001), ("10.0.0.2", 8001)],
                "flow-service": [("10.0.0.1", 8002)],
            }
        )

        # Or via environment:
        # SERVICE_AUTH_SERVICE_URL=http://10.0.0.1:8001,http://10.0.0.2:8001
        # SERVICE_FLOW_SERVICE_URL=http://10.0.0.1:8002
    """

    def __init__(
        self,
        services: Optional[Dict[str, List[tuple]]] = None,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        prefer_same_zone: bool = False,
        cache_ttl: float = 0.0,  # No caching for static config
    ):
        super().__init__(
            strategy=strategy,
            prefer_same_zone=prefer_same_zone,
            cache_ttl=cache_ttl,
        )

        # Initialize service map
        self._services: Dict[str, List[ServiceInstance]] = {}

        # Load from constructor, env vars, or defaults
        if services:
            self._load_from_dict(services)
        else:
            self._load_from_env()
            if not self._services:
                self._load_from_dict(DEFAULT_SERVICES)

        logger.info(
            f"Static service registry initialized",
            extra={
                "services": list(self._services.keys()),
                "instance_count": sum(len(v) for v in self._services.values()),
            }
        )

    def _load_from_dict(self, services: Dict[str, List[tuple]]) -> None:
        """Load services from dictionary"""
        for service_name, endpoints in services.items():
            instances = []
            for i, (host, port) in enumerate(endpoints):
                instances.append(
                    ServiceInstance(
                        service_name=service_name,
                        instance_id=f"{service_name}-{i}",
                        host=host,
                        port=port,
                        status=ServiceStatus.HEALTHY,
                        metadata={"discovery_method": "static"},
                    )
                )
            self._services[service_name] = instances

    def _load_from_env(self) -> None:
        """
        Load services from environment variables.

        Format: SERVICE_<NAME>_URL=http://host:port,http://host2:port2
        Example: SERVICE_AUTH_SERVICE_URL=http://localhost:8001
        """
        prefix = "SERVICE_"
        suffix = "_URL"

        for key, value in os.environ.items():
            if key.startswith(prefix) and key.endswith(suffix):
                # Extract service name
                service_name = key[len(prefix):-len(suffix)].lower().replace("_", "-")

                # Parse URLs
                instances = []
                for i, url in enumerate(value.split(",")):
                    url = url.strip()
                    if "://" in url:
                        # Parse http://host:port
                        _, hostport = url.split("://", 1)
                    else:
                        hostport = url

                    if ":" in hostport:
                        host, port_str = hostport.rsplit(":", 1)
                        port = int(port_str)
                    else:
                        host = hostport
                        port = 80

                    instances.append(
                        ServiceInstance(
                            service_name=service_name,
                            instance_id=f"{service_name}-{i}",
                            host=host,
                            port=port,
                            status=ServiceStatus.HEALTHY,
                            metadata={
                                "discovery_method": "static",
                                "env_var": key,
                            },
                        )
                    )

                if instances:
                    self._services[service_name] = instances

    async def register(self, registration: ServiceRegistration) -> bool:
        """
        Register a service instance.

        Adds the instance to the in-memory service map.
        """
        service_name = registration.service_name

        instance = ServiceInstance(
            service_name=service_name,
            instance_id=registration.instance_id,
            host=registration.host,
            port=registration.port,
            status=ServiceStatus.HEALTHY,
            metadata={
                "discovery_method": "static",
                **registration.metadata,
            },
        )

        if service_name not in self._services:
            self._services[service_name] = []

        # Check for duplicate instance ID
        existing_ids = {i.instance_id for i in self._services[service_name]}
        if registration.instance_id not in existing_ids:
            self._services[service_name].append(instance)

        logger.info(
            f"Registered static service",
            extra={
                "service_name": service_name,
                "instance_id": registration.instance_id,
                "address": f"{registration.host}:{registration.port}",
            }
        )
        return True

    async def deregister(self, service_name: str, instance_id: str) -> bool:
        """Remove a service instance from the registry"""
        if service_name not in self._services:
            return False

        original_count = len(self._services[service_name])
        self._services[service_name] = [
            i for i in self._services[service_name]
            if i.instance_id != instance_id
        ]

        removed = len(self._services[service_name]) < original_count

        if removed:
            logger.info(
                f"Deregistered static service",
                extra={
                    "service_name": service_name,
                    "instance_id": instance_id,
                }
            )

        return removed

    async def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> List[ServiceInstance]:
        """Get all instances of a service"""
        instances = self._services.get(service_name, [])

        if healthy_only:
            instances = [i for i in instances if i.status == ServiceStatus.HEALTHY]

        return instances

    async def set_instance_status(
        self,
        service_name: str,
        instance_id: str,
        status: ServiceStatus,
    ) -> bool:
        """Set the status of a service instance"""
        if service_name not in self._services:
            return False

        for instance in self._services[service_name]:
            if instance.instance_id == instance_id:
                instance.status = status
                instance.last_check = datetime.now(timezone.utc)
                return True

        return False

    async def watch_service(
        self,
        service_name: str,
        callback: Callable[[List[ServiceInstance]], Any],
    ) -> None:
        """
        Watch for changes to a service.

        For static registry, this just calls the callback once immediately.
        Changes only happen through register/deregister calls.
        """
        instances = await self.get_instances(service_name)
        await callback(instances)

    def add_service(
        self,
        service_name: str,
        host: str,
        port: int,
        instance_id: Optional[str] = None,
    ) -> ServiceInstance:
        """Convenience method to add a service instance"""
        if instance_id is None:
            count = len(self._services.get(service_name, []))
            instance_id = f"{service_name}-{count}"

        instance = ServiceInstance(
            service_name=service_name,
            instance_id=instance_id,
            host=host,
            port=port,
            status=ServiceStatus.HEALTHY,
            metadata={"discovery_method": "static"},
        )

        if service_name not in self._services:
            self._services[service_name] = []

        self._services[service_name].append(instance)
        return instance

    async def close(self) -> None:
        """Close registry (no-op for static)"""
        logger.info("Static service registry closed")

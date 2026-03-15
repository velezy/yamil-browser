"""
HashiCorp Consul Service Discovery

Provides service registration and discovery using Consul's HTTP API.
Suitable for VM deployments or mixed environments.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import httpx

from assemblyline_common.discovery.registry import (
    LoadBalanceStrategy,
    ServiceInstance,
    ServiceRegistration,
    ServiceRegistry,
    ServiceStatus,
)

logger = logging.getLogger(__name__)


class ConsulServiceRegistry(ServiceRegistry):
    """
    HashiCorp Consul service discovery.

    Provides full service registration and discovery using Consul's HTTP API.

    Usage:
        registry = ConsulServiceRegistry(
            consul_url="http://consul.service.consul:8500",
            datacenter="dc1",
        )

        # Register this service
        await registry.register(ServiceRegistration(
            service_name="auth-service",
            instance_id="auth-service-1",
            host="10.0.0.1",
            port=8001,
            tags=["v1", "production"],
            health_check_url="http://10.0.0.1:8001/health/live",
        ))

        # Discover services
        instances = await registry.get_instances("flow-service")

        # Deregister on shutdown
        await registry.deregister("auth-service", "auth-service-1")
    """

    def __init__(
        self,
        consul_url: Optional[str] = None,
        datacenter: str = "",
        token: Optional[str] = None,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        prefer_same_zone: bool = True,
        cache_ttl: float = 30.0,
    ):
        super().__init__(
            strategy=strategy,
            prefer_same_zone=prefer_same_zone,
            cache_ttl=cache_ttl,
        )

        self.consul_url = consul_url or os.getenv(
            "CONSUL_HTTP_ADDR", "http://localhost:8500"
        )
        self.datacenter = datacenter or os.getenv("CONSUL_DATACENTER", "")
        self.token = token or os.getenv("CONSUL_HTTP_TOKEN")

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        # Watch indices for long polling
        self._watch_indices: Dict[str, int] = {}
        self._watch_tasks: Dict[str, asyncio.Task] = {}

        logger.info(
            f"Consul service registry initialized",
            extra={
                "consul_url": self.consul_url,
                "datacenter": self.datacenter,
            }
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._client is None:
            headers = {}
            if self.token:
                headers["X-Consul-Token"] = self.token

            self._client = httpx.AsyncClient(
                base_url=self.consul_url,
                headers=headers,
                timeout=30.0,
            )
        return self._client

    async def register(self, registration: ServiceRegistration) -> bool:
        """
        Register a service instance with Consul.

        Creates the service registration and optional health check.
        """
        client = await self._get_client()

        # Build service definition
        service_def: Dict[str, Any] = {
            "ID": registration.instance_id,
            "Name": registration.service_name,
            "Address": registration.host,
            "Port": registration.port,
            "Tags": registration.tags or [],
            "Meta": registration.metadata or {},
        }

        # Add health check if URL provided
        if registration.health_check_url:
            service_def["Check"] = {
                "HTTP": registration.health_check_url,
                "Interval": f"{registration.health_check_interval}s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": f"{registration.deregister_critical_after}s",
            }

        try:
            response = await client.put(
                "/v1/agent/service/register",
                json=service_def,
                params={"dc": self.datacenter} if self.datacenter else {},
            )
            response.raise_for_status()

            logger.info(
                f"Registered service with Consul",
                extra={
                    "service_name": registration.service_name,
                    "instance_id": registration.instance_id,
                    "address": f"{registration.host}:{registration.port}",
                }
            )
            return True

        except httpx.HTTPError as e:
            logger.error(f"Failed to register service with Consul: {e}")
            return False

    async def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance from Consul"""
        client = await self._get_client()

        try:
            response = await client.put(
                f"/v1/agent/service/deregister/{instance_id}",
                params={"dc": self.datacenter} if self.datacenter else {},
            )
            response.raise_for_status()

            logger.info(
                f"Deregistered service from Consul",
                extra={
                    "service_name": service_name,
                    "instance_id": instance_id,
                }
            )
            return True

        except httpx.HTTPError as e:
            logger.error(f"Failed to deregister service from Consul: {e}")
            return False

    async def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> List[ServiceInstance]:
        """
        Get all instances of a service from Consul.

        Uses the health endpoint to get both service info and health status.
        """
        client = await self._get_client()

        # Use health endpoint for healthy_only, catalog otherwise
        if healthy_only:
            endpoint = f"/v1/health/service/{service_name}"
            params = {"passing": "true"}
        else:
            endpoint = f"/v1/health/service/{service_name}"
            params = {}

        if self.datacenter:
            params["dc"] = self.datacenter

        try:
            response = await client.get(endpoint, params=params)
            response.raise_for_status()

            instances = []
            for entry in response.json():
                service = entry.get("Service", {})
                checks = entry.get("Checks", [])
                node = entry.get("Node", {})

                # Determine health status from checks
                status = ServiceStatus.HEALTHY
                for check in checks:
                    check_status = check.get("Status", "passing")
                    if check_status == "critical":
                        status = ServiceStatus.UNHEALTHY
                        break
                    elif check_status == "warning":
                        status = ServiceStatus.DEGRADED

                # Get zone from node metadata if available
                node_meta = node.get("Meta", {})
                zone = node_meta.get("zone", node_meta.get("availability_zone", ""))

                instances.append(
                    ServiceInstance(
                        service_name=service_name,
                        instance_id=service.get("ID", ""),
                        host=service.get("Address") or node.get("Address", ""),
                        port=service.get("Port", 0),
                        status=status,
                        weight=service.get("Weights", {}).get("Passing", 100),
                        zone=zone,
                        metadata={
                            **service.get("Meta", {}),
                            "tags": service.get("Tags", []),
                            "node": node.get("Node", ""),
                            "datacenter": node.get("Datacenter", ""),
                        },
                        last_check=datetime.now(timezone.utc),
                    )
                )

            return instances

        except httpx.HTTPError as e:
            logger.error(f"Failed to get service instances from Consul: {e}")
            return []

    async def watch_service(
        self,
        service_name: str,
        callback: Callable[[List[ServiceInstance]], Any],
    ) -> None:
        """
        Watch for changes to a service using Consul's blocking queries.

        Uses long-polling with the X-Consul-Index header for efficient updates.
        """
        async def _watch():
            index = self._watch_indices.get(service_name, 0)

            while True:
                try:
                    client = await self._get_client()

                    params = {
                        "passing": "true",
                        "index": str(index),
                        "wait": "30s",
                    }
                    if self.datacenter:
                        params["dc"] = self.datacenter

                    response = await client.get(
                        f"/v1/health/service/{service_name}",
                        params=params,
                        timeout=35.0,  # Slightly longer than wait time
                    )
                    response.raise_for_status()

                    # Get new index from header
                    new_index = int(response.headers.get("X-Consul-Index", 0))

                    if new_index != index:
                        index = new_index
                        self._watch_indices[service_name] = index

                        # Parse instances and call callback
                        instances = await self.get_instances(service_name)
                        await callback(instances)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error watching service {service_name}: {e}")
                    await asyncio.sleep(5)

        task = asyncio.create_task(_watch())
        self._watch_tasks[service_name] = task

    async def set_maintenance(
        self,
        instance_id: str,
        enable: bool,
        reason: str = "",
    ) -> bool:
        """
        Put a service instance in/out of maintenance mode.

        Maintenance mode prevents the service from receiving traffic.
        """
        client = await self._get_client()

        try:
            response = await client.put(
                f"/v1/agent/service/maintenance/{instance_id}",
                params={
                    "enable": str(enable).lower(),
                    "reason": reason,
                },
            )
            response.raise_for_status()

            logger.info(
                f"{'Enabled' if enable else 'Disabled'} maintenance mode",
                extra={
                    "instance_id": instance_id,
                    "reason": reason,
                }
            )
            return True

        except httpx.HTTPError as e:
            logger.error(f"Failed to set maintenance mode: {e}")
            return False

    async def close(self) -> None:
        """Close registry and cancel watch tasks"""
        # Cancel watch tasks
        for task in self._watch_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._watch_tasks.clear()

        # Close HTTP client
        if self._client:
            await self._client.aclose()
            self._client = None

        logger.info("Consul service registry closed")

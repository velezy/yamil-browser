"""
AWS Cloud Map Service Discovery

Provides service registration and discovery using AWS Cloud Map.
Designed for AWS ECS/EKS deployments.
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


class CloudMapServiceRegistry(ServiceRegistry):
    """
    AWS Cloud Map service discovery.

    Provides service registration and discovery using AWS Cloud Map API.
    Works with both DNS-based and API-based discovery.

    Usage:
        registry = CloudMapServiceRegistry(
            namespace_id="ns-xxxxx",  # Or namespace_name="logic-weaver"
            region="us-east-1",
        )

        # Register service (usually handled by ECS/EKS)
        await registry.register(ServiceRegistration(
            service_name="auth-service",
            instance_id="i-xxxxx",
            host="10.0.0.1",
            port=8001,
        ))

        # Discover services
        instances = await registry.get_instances("flow-service")
    """

    def __init__(
        self,
        namespace_id: Optional[str] = None,
        namespace_name: Optional[str] = None,
        region: Optional[str] = None,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        prefer_same_zone: bool = True,
        cache_ttl: float = 30.0,
    ):
        super().__init__(
            strategy=strategy,
            prefer_same_zone=prefer_same_zone,
            cache_ttl=cache_ttl,
        )

        self.namespace_id = namespace_id or os.getenv("CLOUDMAP_NAMESPACE_ID")
        self.namespace_name = namespace_name or os.getenv(
            "CLOUDMAP_NAMESPACE_NAME", "logic-weaver"
        )
        self.region = region or os.getenv("AWS_REGION", "us-east-1")

        # Service ID cache (service_name -> service_id)
        self._service_ids: Dict[str, str] = {}

        # AWS clients (lazy init)
        self._sd_client = None
        self._loop = None

        logger.info(
            f"Cloud Map service registry initialized",
            extra={
                "namespace_id": self.namespace_id,
                "namespace_name": self.namespace_name,
                "region": self.region,
            }
        )

    def _get_client(self):
        """Get or create Cloud Map (servicediscovery) client"""
        if self._sd_client is None:
            import boto3
            self._sd_client = boto3.client(
                "servicediscovery",
                region_name=self.region,
            )
        return self._sd_client

    async def _run_sync(self, func):
        """Run synchronous boto3 call in executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, func)

    async def _resolve_namespace_id(self) -> Optional[str]:
        """Resolve namespace ID from name if not provided"""
        if self.namespace_id:
            return self.namespace_id

        client = self._get_client()
        try:
            response = await self._run_sync(
                lambda: client.list_namespaces(
                    Filters=[
                        {"Name": "NAME", "Values": [self.namespace_name]},
                    ]
                )
            )

            namespaces = response.get("Namespaces", [])
            if namespaces:
                self.namespace_id = namespaces[0]["Id"]
                return self.namespace_id

            logger.error(f"Namespace not found: {self.namespace_name}")
            return None

        except Exception as e:
            logger.error(f"Failed to resolve namespace: {e}")
            return None

    async def _get_service_id(self, service_name: str) -> Optional[str]:
        """Get Cloud Map service ID for a service name"""
        if service_name in self._service_ids:
            return self._service_ids[service_name]

        namespace_id = await self._resolve_namespace_id()
        if not namespace_id:
            return None

        client = self._get_client()
        try:
            response = await self._run_sync(
                lambda: client.list_services(
                    Filters=[
                        {"Name": "NAMESPACE_ID", "Values": [namespace_id]},
                    ]
                )
            )

            for service in response.get("Services", []):
                if service["Name"] == service_name:
                    self._service_ids[service_name] = service["Id"]
                    return service["Id"]

            return None

        except Exception as e:
            logger.error(f"Failed to get service ID: {e}")
            return None

    async def register(self, registration: ServiceRegistration) -> bool:
        """
        Register a service instance with Cloud Map.

        Note: In ECS/EKS, registration is usually automatic via the platform.
        This method is for manual registration or custom deployments.
        """
        service_id = await self._get_service_id(registration.service_name)
        if not service_id:
            logger.error(f"Service not found in Cloud Map: {registration.service_name}")
            return False

        client = self._get_client()

        attributes = {
            "AWS_INSTANCE_IPV4": registration.host,
            "AWS_INSTANCE_PORT": str(registration.port),
        }
        if registration.metadata:
            for key, value in registration.metadata.items():
                if isinstance(value, str) and len(key) <= 256:
                    attributes[key] = value[:1024]

        try:
            await self._run_sync(
                lambda: client.register_instance(
                    ServiceId=service_id,
                    InstanceId=registration.instance_id,
                    Attributes=attributes,
                )
            )

            logger.info(
                f"Registered instance with Cloud Map",
                extra={
                    "service_name": registration.service_name,
                    "instance_id": registration.instance_id,
                }
            )
            return True

        except Exception as e:
            logger.error(f"Failed to register with Cloud Map: {e}")
            return False

    async def deregister(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance from Cloud Map"""
        service_id = await self._get_service_id(service_name)
        if not service_id:
            return False

        client = self._get_client()

        try:
            await self._run_sync(
                lambda: client.deregister_instance(
                    ServiceId=service_id,
                    InstanceId=instance_id,
                )
            )

            logger.info(
                f"Deregistered instance from Cloud Map",
                extra={
                    "service_name": service_name,
                    "instance_id": instance_id,
                }
            )
            return True

        except Exception as e:
            logger.error(f"Failed to deregister from Cloud Map: {e}")
            return False

    async def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> List[ServiceInstance]:
        """
        Get all instances of a service from Cloud Map.

        Uses discover_instances for efficient lookup.
        """
        namespace_id = await self._resolve_namespace_id()
        if not namespace_id:
            return []

        client = self._get_client()

        try:
            params = {
                "NamespaceName": self.namespace_name,
                "ServiceName": service_name,
            }

            if healthy_only:
                params["HealthStatus"] = "HEALTHY"

            response = await self._run_sync(
                lambda: client.discover_instances(**params)
            )

            instances = []
            for inst in response.get("Instances", []):
                attrs = inst.get("Attributes", {})

                host = attrs.get("AWS_INSTANCE_IPV4", "")
                port_str = attrs.get("AWS_INSTANCE_PORT", "8000")
                port = int(port_str) if port_str.isdigit() else 8000

                # Determine status
                health = inst.get("HealthStatus", "UNKNOWN")
                if health == "HEALTHY":
                    status = ServiceStatus.HEALTHY
                elif health == "UNHEALTHY":
                    status = ServiceStatus.UNHEALTHY
                else:
                    status = ServiceStatus.UNKNOWN

                # Get zone from attributes
                zone = attrs.get("AVAILABILITY_ZONE", attrs.get("AWS_AVAILABILITY_ZONE", ""))

                instances.append(
                    ServiceInstance(
                        service_name=service_name,
                        instance_id=inst.get("InstanceId", ""),
                        host=host,
                        port=port,
                        status=status,
                        zone=zone,
                        metadata={
                            k: v for k, v in attrs.items()
                            if not k.startswith("AWS_")
                        },
                        last_check=datetime.now(timezone.utc),
                    )
                )

            return instances

        except Exception as e:
            logger.error(f"Failed to discover instances from Cloud Map: {e}")
            return []

    async def watch_service(
        self,
        service_name: str,
        callback: Callable[[List[ServiceInstance]], Any],
        poll_interval: float = 10.0,
    ) -> None:
        """
        Watch for changes to a service.

        Cloud Map doesn't support push notifications, so we poll.
        """
        previous_instance_ids = set()

        while True:
            try:
                instances = await self.get_instances(service_name)
                current_ids = {i.instance_id for i in instances}

                if current_ids != previous_instance_ids:
                    await callback(instances)
                    previous_instance_ids = current_ids

                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching service {service_name}: {e}")
                await asyncio.sleep(poll_interval)

    async def create_service(
        self,
        service_name: str,
        dns_ttl: int = 10,
        health_check_path: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create a new service in Cloud Map.

        Returns the service ID if successful.
        """
        namespace_id = await self._resolve_namespace_id()
        if not namespace_id:
            return None

        client = self._get_client()

        try:
            dns_config = {
                "NamespaceId": namespace_id,
                "DnsRecords": [
                    {"Type": "A", "TTL": dns_ttl},
                    {"Type": "SRV", "TTL": dns_ttl},
                ],
            }

            params = {
                "Name": service_name,
                "DnsConfig": dns_config,
            }

            if health_check_path:
                params["HealthCheckCustomConfig"] = {
                    "FailureThreshold": 1,
                }

            response = await self._run_sync(
                lambda: client.create_service(**params)
            )

            service_id = response.get("Service", {}).get("Id")
            if service_id:
                self._service_ids[service_name] = service_id

            logger.info(
                f"Created Cloud Map service",
                extra={
                    "service_name": service_name,
                    "service_id": service_id,
                }
            )
            return service_id

        except Exception as e:
            logger.error(f"Failed to create Cloud Map service: {e}")
            return None

    async def close(self) -> None:
        """Close registry"""
        self._sd_client = None
        logger.info("Cloud Map service registry closed")

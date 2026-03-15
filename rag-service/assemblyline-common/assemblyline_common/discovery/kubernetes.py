"""
Kubernetes Service Discovery

Uses Kubernetes native DNS and endpoints API for service discovery.
Designed for services running in k3s/EKS clusters.
"""

import asyncio
import logging
import os
import socket
from typing import Any, Callable, Dict, List, Optional

from assemblyline_common.discovery.registry import (
    LoadBalanceStrategy,
    ServiceInstance,
    ServiceRegistration,
    ServiceRegistry,
    ServiceStatus,
)

logger = logging.getLogger(__name__)


class KubernetesServiceRegistry(ServiceRegistry):
    """
    Kubernetes-native service discovery.

    In Kubernetes, services are automatically discovered via DNS.
    Service names resolve to cluster IPs which load balance across pods.

    For more advanced routing (zone-aware, weighted), use the Kubernetes
    Endpoints API to get individual pod IPs.

    Usage:
        registry = KubernetesServiceRegistry(
            namespace="logic-weaver",
            use_endpoints_api=True,  # For pod-level discovery
        )

        # Get service URL (uses Kubernetes DNS)
        instances = await registry.get_instances("auth-service")

        # The returned instances use Kubernetes DNS names like:
        # auth-service.logic-weaver.svc.cluster.local:8001
    """

    def __init__(
        self,
        namespace: Optional[str] = None,
        cluster_domain: str = "cluster.local",
        use_endpoints_api: bool = False,
        strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
        prefer_same_zone: bool = True,
        cache_ttl: float = 30.0,
    ):
        super().__init__(
            strategy=strategy,
            prefer_same_zone=prefer_same_zone,
            cache_ttl=cache_ttl,
        )

        # Detect namespace from pod or env var
        self.namespace = namespace or self._detect_namespace()
        self.cluster_domain = cluster_domain
        self.use_endpoints_api = use_endpoints_api

        # Service port mappings (service_name -> default_port)
        self._service_ports: Dict[str, int] = {
            "auth-service": 8001,
            "flow-service": 8002,
            "inbound-service": 8003,
            "outbound-service": 8004,
            "transform-service": 8005,
            "mapping-service": 8006,
            "status-service": 8007,
            "ai-orchestra-service": 8008,
        }

        # Watch tasks
        self._watch_tasks: Dict[str, asyncio.Task] = {}

        logger.info(
            f"Kubernetes service registry initialized",
            extra={
                "namespace": self.namespace,
                "cluster_domain": self.cluster_domain,
                "use_endpoints_api": use_endpoints_api,
            }
        )

    def _detect_namespace(self) -> str:
        """Detect Kubernetes namespace from pod environment"""
        # Try environment variable first
        ns = os.getenv("POD_NAMESPACE", os.getenv("KUBERNETES_NAMESPACE"))
        if ns:
            return ns

        # Try to read from mounted namespace file
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
                return f.read().strip()
        except FileNotFoundError:
            pass

        # Default to "default" namespace
        return os.getenv("DEFAULT_NAMESPACE", "logic-weaver")

    def _get_service_dns(self, service_name: str) -> str:
        """Get fully qualified DNS name for a service"""
        return f"{service_name}.{self.namespace}.svc.{self.cluster_domain}"

    async def register(self, registration: ServiceRegistration) -> bool:
        """
        Register a service instance.

        In Kubernetes, registration is handled by the Kubernetes control plane.
        Pods are automatically registered when they start and pass health checks.

        This method is a no-op but logs for consistency with other registries.
        """
        logger.info(
            f"Service registration (Kubernetes handles this automatically)",
            extra={
                "service_name": registration.service_name,
                "instance_id": registration.instance_id,
                "host": registration.host,
                "port": registration.port,
            }
        )
        return True

    async def deregister(self, service_name: str, instance_id: str) -> bool:
        """
        Deregister a service instance.

        In Kubernetes, deregistration happens when pods terminate.
        This method is a no-op but logs for consistency.
        """
        logger.info(
            f"Service deregistration (Kubernetes handles this automatically)",
            extra={
                "service_name": service_name,
                "instance_id": instance_id,
            }
        )
        return True

    async def get_instances(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> List[ServiceInstance]:
        """
        Get all instances of a service.

        If use_endpoints_api is True, uses Kubernetes API to get individual pods.
        Otherwise, returns the service's DNS name which Kubernetes load balances.
        """
        if self.use_endpoints_api:
            return await self._get_instances_from_api(service_name, healthy_only)
        else:
            return await self._get_instances_from_dns(service_name)

    async def _get_instances_from_dns(
        self,
        service_name: str,
    ) -> List[ServiceInstance]:
        """
        Get service instance using Kubernetes DNS.

        Returns a single "virtual" instance representing the Kubernetes service.
        Kubernetes handles load balancing across pods.
        """
        dns_name = self._get_service_dns(service_name)
        port = self._service_ports.get(service_name, 8000)

        try:
            # Verify DNS resolves
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: socket.gethostbyname(dns_name)
            )

            return [
                ServiceInstance(
                    service_name=service_name,
                    instance_id=f"{service_name}-k8s-service",
                    host=dns_name,
                    port=port,
                    status=ServiceStatus.HEALTHY,
                    metadata={
                        "discovery_method": "dns",
                        "load_balanced": True,
                    },
                )
            ]
        except socket.gaierror as e:
            logger.warning(f"DNS resolution failed for {dns_name}: {e}")
            return []

    async def _get_instances_from_api(
        self,
        service_name: str,
        healthy_only: bool = True,
    ) -> List[ServiceInstance]:
        """
        Get individual pod instances using Kubernetes Endpoints API.

        Requires kubernetes Python client and appropriate RBAC permissions.
        """
        try:
            # Lazy import kubernetes client
            from kubernetes import client, config
            from kubernetes.client.rest import ApiException

            # Try in-cluster config first, fall back to kubeconfig
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()

            v1 = client.CoreV1Api()

            # Get endpoints for the service
            loop = asyncio.get_event_loop()
            endpoints = await loop.run_in_executor(
                None,
                lambda: v1.read_namespaced_endpoints(service_name, self.namespace)
            )

            instances = []
            default_port = self._service_ports.get(service_name, 8000)

            for subset in endpoints.subsets or []:
                # Get port number
                port = default_port
                if subset.ports:
                    port = subset.ports[0].port

                # Healthy addresses
                for address in subset.addresses or []:
                    zone = ""
                    if address.node_name:
                        # Try to get zone from node labels
                        zone = await self._get_node_zone(v1, address.node_name)

                    instances.append(
                        ServiceInstance(
                            service_name=service_name,
                            instance_id=address.target_ref.name if address.target_ref else address.ip,
                            host=address.ip,
                            port=port,
                            status=ServiceStatus.HEALTHY,
                            zone=zone,
                            metadata={
                                "discovery_method": "endpoints_api",
                                "pod_name": address.target_ref.name if address.target_ref else None,
                                "node_name": address.node_name,
                            },
                        )
                    )

                # Unhealthy addresses (not ready)
                if not healthy_only:
                    for address in subset.not_ready_addresses or []:
                        instances.append(
                            ServiceInstance(
                                service_name=service_name,
                                instance_id=address.target_ref.name if address.target_ref else address.ip,
                                host=address.ip,
                                port=port,
                                status=ServiceStatus.UNHEALTHY,
                                metadata={
                                    "discovery_method": "endpoints_api",
                                    "pod_name": address.target_ref.name if address.target_ref else None,
                                },
                            )
                        )

            return instances

        except ImportError:
            logger.warning(
                "kubernetes package not installed, falling back to DNS discovery"
            )
            return await self._get_instances_from_dns(service_name)

        except Exception as e:
            logger.error(f"Failed to get endpoints from Kubernetes API: {e}")
            return await self._get_instances_from_dns(service_name)

    async def _get_node_zone(self, v1, node_name: str) -> str:
        """Get availability zone from node labels"""
        try:
            loop = asyncio.get_event_loop()
            node = await loop.run_in_executor(
                None,
                lambda: v1.read_node(node_name)
            )

            labels = node.metadata.labels or {}
            # Standard Kubernetes zone labels
            zone = labels.get(
                "topology.kubernetes.io/zone",
                labels.get("failure-domain.beta.kubernetes.io/zone", "")
            )
            return zone

        except Exception as e:
            logger.debug(f"Could not get zone for node {node_name}: {e}")
            return ""

    async def watch_service(
        self,
        service_name: str,
        callback: Callable[[List[ServiceInstance]], Any],
    ) -> None:
        """
        Watch for changes to a service's endpoints.

        Uses Kubernetes watch API for real-time updates.
        """
        if not self.use_endpoints_api:
            logger.warning(
                "watch_service requires use_endpoints_api=True, using polling instead"
            )
            # Fall back to polling
            await self._poll_service(service_name, callback)
            return

        try:
            from kubernetes import client, config, watch
            from kubernetes.client.rest import ApiException

            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()

            v1 = client.CoreV1Api()
            w = watch.Watch()

            async def _watch():
                loop = asyncio.get_event_loop()
                for event in await loop.run_in_executor(
                    None,
                    lambda: w.stream(
                        v1.list_namespaced_endpoints,
                        self.namespace,
                        field_selector=f"metadata.name={service_name}",
                    )
                ):
                    instances = await self.get_instances(service_name)
                    await callback(instances)

            task = asyncio.create_task(_watch())
            self._watch_tasks[service_name] = task

        except ImportError:
            await self._poll_service(service_name, callback)

    async def _poll_service(
        self,
        service_name: str,
        callback: Callable[[List[ServiceInstance]], Any],
        interval: float = 10.0,
    ) -> None:
        """Poll for service changes when watch is not available"""
        previous_instances = []

        while True:
            try:
                instances = await self.get_instances(service_name)

                # Check if instances changed
                current_ids = {i.instance_id for i in instances}
                previous_ids = {i.instance_id for i in previous_instances}

                if current_ids != previous_ids:
                    await callback(instances)
                    previous_instances = instances

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error polling service {service_name}: {e}")
                await asyncio.sleep(interval)

    async def close(self) -> None:
        """Close registry and cancel watch tasks"""
        for task in self._watch_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._watch_tasks.clear()
        logger.info("Kubernetes service registry closed")

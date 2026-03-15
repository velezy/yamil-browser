"""
Service Discovery Module for Logic Weaver

Provides abstraction for service discovery supporting:
- Kubernetes native DNS (default)
- HashiCorp Consul
- AWS Cloud Map
- Static configuration (development)
"""

from assemblyline_common.discovery.registry import (
    ServiceInstance,
    ServiceRegistry,
    get_service_registry,
)
from assemblyline_common.discovery.kubernetes import KubernetesServiceRegistry
from assemblyline_common.discovery.consul import ConsulServiceRegistry
from assemblyline_common.discovery.static import StaticServiceRegistry

__all__ = [
    "ServiceInstance",
    "ServiceRegistry",
    "get_service_registry",
    "KubernetesServiceRegistry",
    "ConsulServiceRegistry",
    "StaticServiceRegistry",
]

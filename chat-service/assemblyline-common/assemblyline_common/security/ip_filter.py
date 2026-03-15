"""
IP Filtering Module

Enterprise-grade IP whitelist/blacklist with CIDR support.
Similar to AWS WAF, Cloudflare Access Rules, Kong IP Restriction.

Features:
- IP whitelist/blacklist
- CIDR notation support
- Tenant-specific rules
- Dynamic rule updates via Redis
- X-Forwarded-For handling
- GeoIP blocking integration
"""

import ipaddress
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union

from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)


class IPFilterAction(str, Enum):
    """Filter action."""
    ALLOW = "allow"
    DENY = "deny"


class IPFilterMode(str, Enum):
    """Filter mode."""
    WHITELIST = "whitelist"  # Deny all except whitelist
    BLACKLIST = "blacklist"  # Allow all except blacklist
    HYBRID = "hybrid"  # Check both lists


@dataclass
class IPFilterConfig:
    """Configuration for IP filtering."""
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    mode: IPFilterMode = IPFilterMode.BLACKLIST
    key_prefix: str = "ipfilter:"

    # Default lists (can be overridden per tenant)
    default_whitelist: List[str] = field(default_factory=list)
    default_blacklist: List[str] = field(default_factory=list)

    # Trusted proxy headers
    trust_proxy_headers: bool = True
    proxy_header: str = "X-Forwarded-For"
    trusted_proxies: List[str] = field(default_factory=lambda: ["127.0.0.1", "10.0.0.0/8"])

    # Built-in protection
    block_private_ranges: bool = False  # Block RFC1918 addresses
    block_loopback: bool = False  # Block 127.0.0.0/8

    # Rate limiting for denied IPs
    track_denied: bool = True
    denied_ttl: int = 3600  # Track denied IPs for 1 hour


@dataclass
class IPFilterResult:
    """Result of IP filter check."""
    allowed: bool
    action: IPFilterAction
    matched_rule: Optional[str] = None
    reason: Optional[str] = None
    client_ip: str = ""


class IPFilter:
    """
    IP whitelist/blacklist filter.

    Usage:
        filter = IPFilter(config)
        await filter.connect()

        # Check IP
        result = await filter.check(request)
        if not result.allowed:
            raise HTTPException(403, "IP blocked")

        # Manage lists
        await filter.add_to_blacklist("1.2.3.4")
        await filter.add_to_whitelist("10.0.0.0/8", tenant_id="tenant1")
    """

    # Private IP ranges
    PRIVATE_RANGES = [
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("172.16.0.0/12"),
        ipaddress.ip_network("192.168.0.0/16"),
    ]
    LOOPBACK_RANGE = ipaddress.ip_network("127.0.0.0/8")

    def __init__(self, config: Optional[IPFilterConfig] = None):
        self.config = config or IPFilterConfig()
        self._redis = None

        # Parse default lists
        self._default_whitelist = self._parse_networks(self.config.default_whitelist)
        self._default_blacklist = self._parse_networks(self.config.default_blacklist)
        self._trusted_proxies = self._parse_networks(self.config.trusted_proxies)

    def _parse_networks(
        self,
        entries: List[str]
    ) -> List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]:
        """Parse IP/CIDR strings into network objects."""
        networks = []
        for entry in entries:
            try:
                # Try as network first
                networks.append(ipaddress.ip_network(entry, strict=False))
            except ValueError:
                try:
                    # Try as single IP
                    ip = ipaddress.ip_address(entry)
                    if isinstance(ip, ipaddress.IPv4Address):
                        networks.append(ipaddress.ip_network(f"{entry}/32"))
                    else:
                        networks.append(ipaddress.ip_network(f"{entry}/128"))
                except ValueError:
                    logger.warning(f"Invalid IP/CIDR: {entry}")
        return networks

    async def connect(self):
        """Connect to Redis for dynamic rules."""
        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info("IP filter connected to Redis")
        except Exception as e:
            logger.warning(f"IP filter Redis connection failed: {e}")
            self._redis = None

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    async def disconnect(self):
        """Alias for close() - disconnect from Redis."""
        await self.close()

    def _get_client_ip(self, request: Request) -> str:
        """Extract real client IP from request."""
        # Direct connection IP
        client_ip = request.client.host if request.client else "127.0.0.1"

        if not self.config.trust_proxy_headers:
            return client_ip

        # Check if direct connection is from trusted proxy
        try:
            direct_ip = ipaddress.ip_address(client_ip)
            is_trusted_proxy = any(
                direct_ip in network for network in self._trusted_proxies
            )
        except ValueError:
            is_trusted_proxy = False

        if not is_trusted_proxy:
            return client_ip

        # Parse X-Forwarded-For header
        forwarded = request.headers.get(self.config.proxy_header)
        if forwarded:
            # Get the first (leftmost) IP, which is the original client
            ips = [ip.strip() for ip in forwarded.split(",")]
            if ips:
                return ips[0]

        return client_ip

    def _check_network(
        self,
        ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
        networks: List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]
    ) -> Optional[str]:
        """Check if IP matches any network."""
        for network in networks:
            if ip in network:
                return str(network)
        return None

    async def _get_dynamic_list(
        self,
        list_type: str,
        tenant_id: Optional[str] = None
    ) -> List[str]:
        """Get dynamic list from Redis."""
        if not self._redis:
            return []

        try:
            if tenant_id:
                key = f"{self.config.key_prefix}{list_type}:{tenant_id}"
            else:
                key = f"{self.config.key_prefix}{list_type}:global"

            return list(await self._redis.smembers(key))
        except Exception as e:
            logger.warning(f"Failed to get dynamic {list_type}: {e}")
            return []

    async def check(
        self,
        request: Request,
        tenant_id: Optional[str] = None
    ) -> IPFilterResult:
        """
        Check if request IP is allowed.

        Args:
            request: FastAPI request
            tenant_id: Optional tenant ID for tenant-specific rules

        Returns:
            IPFilterResult with allowed status and reason
        """
        client_ip_str = self._get_client_ip(request)

        try:
            client_ip = ipaddress.ip_address(client_ip_str)
        except ValueError:
            return IPFilterResult(
                allowed=False,
                action=IPFilterAction.DENY,
                reason="Invalid IP address",
                client_ip=client_ip_str,
            )

        # Check built-in blocks first
        if self.config.block_loopback and client_ip in self.LOOPBACK_RANGE:
            return IPFilterResult(
                allowed=False,
                action=IPFilterAction.DENY,
                matched_rule="loopback",
                reason="Loopback addresses blocked",
                client_ip=client_ip_str,
            )

        if self.config.block_private_ranges:
            for network in self.PRIVATE_RANGES:
                if client_ip in network:
                    return IPFilterResult(
                        allowed=False,
                        action=IPFilterAction.DENY,
                        matched_rule=str(network),
                        reason="Private IP ranges blocked",
                        client_ip=client_ip_str,
                    )

        # Get dynamic lists
        dynamic_whitelist = await self._get_dynamic_list("whitelist", tenant_id)
        dynamic_blacklist = await self._get_dynamic_list("blacklist", tenant_id)

        # Combine with defaults
        all_whitelist = self._default_whitelist + self._parse_networks(dynamic_whitelist)
        all_blacklist = self._default_blacklist + self._parse_networks(dynamic_blacklist)

        # Check based on mode
        if self.config.mode == IPFilterMode.WHITELIST:
            # Whitelist mode: deny unless in whitelist
            match = self._check_network(client_ip, all_whitelist)
            if match:
                return IPFilterResult(
                    allowed=True,
                    action=IPFilterAction.ALLOW,
                    matched_rule=match,
                    reason="IP in whitelist",
                    client_ip=client_ip_str,
                )
            return IPFilterResult(
                allowed=False,
                action=IPFilterAction.DENY,
                reason="IP not in whitelist",
                client_ip=client_ip_str,
            )

        elif self.config.mode == IPFilterMode.BLACKLIST:
            # Blacklist mode: allow unless in blacklist
            match = self._check_network(client_ip, all_blacklist)
            if match:
                await self._track_denied(client_ip_str, tenant_id)
                return IPFilterResult(
                    allowed=False,
                    action=IPFilterAction.DENY,
                    matched_rule=match,
                    reason="IP in blacklist",
                    client_ip=client_ip_str,
                )
            return IPFilterResult(
                allowed=True,
                action=IPFilterAction.ALLOW,
                reason="IP not in blacklist",
                client_ip=client_ip_str,
            )

        else:  # HYBRID mode
            # Check whitelist first (takes precedence)
            match = self._check_network(client_ip, all_whitelist)
            if match:
                return IPFilterResult(
                    allowed=True,
                    action=IPFilterAction.ALLOW,
                    matched_rule=match,
                    reason="IP in whitelist",
                    client_ip=client_ip_str,
                )

            # Then check blacklist
            match = self._check_network(client_ip, all_blacklist)
            if match:
                await self._track_denied(client_ip_str, tenant_id)
                return IPFilterResult(
                    allowed=False,
                    action=IPFilterAction.DENY,
                    matched_rule=match,
                    reason="IP in blacklist",
                    client_ip=client_ip_str,
                )

            # Default allow in hybrid mode
            return IPFilterResult(
                allowed=True,
                action=IPFilterAction.ALLOW,
                reason="IP not in any list",
                client_ip=client_ip_str,
            )

    async def _track_denied(
        self,
        ip: str,
        tenant_id: Optional[str] = None
    ):
        """Track denied IP for analytics."""
        if not self._redis or not self.config.track_denied:
            return

        try:
            key = f"{self.config.key_prefix}denied:{ip}"
            await self._redis.incr(key)
            await self._redis.expire(key, self.config.denied_ttl)
        except Exception as e:
            logger.debug(f"Failed to track denied IP: {e}")

    async def add_to_whitelist(
        self,
        ip_or_cidr: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Add IP or CIDR to whitelist."""
        if not self._redis:
            return False

        try:
            # Validate IP/CIDR
            try:
                ipaddress.ip_network(ip_or_cidr, strict=False)
            except ValueError:
                logger.error(f"Invalid IP/CIDR: {ip_or_cidr}")
                return False

            if tenant_id:
                key = f"{self.config.key_prefix}whitelist:{tenant_id}"
            else:
                key = f"{self.config.key_prefix}whitelist:global"

            await self._redis.sadd(key, ip_or_cidr)
            logger.info(f"Added to whitelist: {ip_or_cidr}")
            return True

        except Exception as e:
            logger.error(f"Failed to add to whitelist: {e}")
            return False

    async def add_to_blacklist(
        self,
        ip_or_cidr: str,
        tenant_id: Optional[str] = None,
        reason: Optional[str] = None
    ) -> bool:
        """Add IP or CIDR to blacklist."""
        if not self._redis:
            return False

        try:
            # Validate IP/CIDR
            try:
                ipaddress.ip_network(ip_or_cidr, strict=False)
            except ValueError:
                logger.error(f"Invalid IP/CIDR: {ip_or_cidr}")
                return False

            if tenant_id:
                key = f"{self.config.key_prefix}blacklist:{tenant_id}"
            else:
                key = f"{self.config.key_prefix}blacklist:global"

            await self._redis.sadd(key, ip_or_cidr)

            # Store reason if provided
            if reason:
                reason_key = f"{self.config.key_prefix}reason:{ip_or_cidr}"
                await self._redis.set(reason_key, reason)

            logger.info(f"Added to blacklist: {ip_or_cidr} ({reason})")
            return True

        except Exception as e:
            logger.error(f"Failed to add to blacklist: {e}")
            return False

    async def remove_from_whitelist(
        self,
        ip_or_cidr: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Remove IP or CIDR from whitelist."""
        if not self._redis:
            return False

        try:
            if tenant_id:
                key = f"{self.config.key_prefix}whitelist:{tenant_id}"
            else:
                key = f"{self.config.key_prefix}whitelist:global"

            removed = await self._redis.srem(key, ip_or_cidr)
            return removed > 0

        except Exception as e:
            logger.error(f"Failed to remove from whitelist: {e}")
            return False

    async def remove_from_blacklist(
        self,
        ip_or_cidr: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Remove IP or CIDR from blacklist."""
        if not self._redis:
            return False

        try:
            if tenant_id:
                key = f"{self.config.key_prefix}blacklist:{tenant_id}"
            else:
                key = f"{self.config.key_prefix}blacklist:global"

            removed = await self._redis.srem(key, ip_or_cidr)

            # Remove reason
            reason_key = f"{self.config.key_prefix}reason:{ip_or_cidr}"
            await self._redis.delete(reason_key)

            return removed > 0

        except Exception as e:
            logger.error(f"Failed to remove from blacklist: {e}")
            return False

    async def list_whitelist(
        self,
        tenant_id: Optional[str] = None
    ) -> List[str]:
        """Get whitelist entries."""
        dynamic = await self._get_dynamic_list("whitelist", tenant_id)
        static = [str(n) for n in self._default_whitelist]
        return list(set(static + dynamic))

    async def list_blacklist(
        self,
        tenant_id: Optional[str] = None
    ) -> List[str]:
        """Get blacklist entries."""
        dynamic = await self._get_dynamic_list("blacklist", tenant_id)
        static = [str(n) for n in self._default_blacklist]
        return list(set(static + dynamic))

    async def get_denied_stats(self) -> Dict[str, int]:
        """Get denied IP statistics."""
        if not self._redis:
            return {}

        try:
            stats = {}
            pattern = f"{self.config.key_prefix}denied:*"

            async for key in self._redis.scan_iter(match=pattern):
                ip = key.split(":")[-1]
                count = await self._redis.get(key)
                if count:
                    stats[ip] = int(count)

            return stats

        except Exception as e:
            logger.error(f"Failed to get denied stats: {e}")
            return {}


# Singleton instance
_filter_instance: Optional[IPFilter] = None


async def get_ip_filter(
    config: Optional[IPFilterConfig] = None
) -> IPFilter:
    """Get or create IP filter singleton."""
    global _filter_instance

    if _filter_instance is None:
        _filter_instance = IPFilter(config)
        await _filter_instance.connect()

    return _filter_instance


async def ip_filter_dependency(
    request: Request,
    tenant_id: Optional[str] = None
):
    """
    FastAPI dependency for IP filtering.

    Usage:
        @app.get("/api/data")
        async def get_data(_: None = Depends(ip_filter_dependency)):
            return {"data": "value"}
    """
    filter = await get_ip_filter()
    result = await filter.check(request, tenant_id)

    if not result.allowed:
        logger.warning(
            f"IP blocked: {result.client_ip} - {result.reason}",
            extra={
                "event_type": "ip_blocked",
                "client_ip": result.client_ip,
                "reason": result.reason,
                "matched_rule": result.matched_rule,
            }
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied: {result.reason}"
        )

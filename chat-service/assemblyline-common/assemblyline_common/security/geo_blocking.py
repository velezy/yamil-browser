"""
Geo-blocking Module

Enterprise-grade geographic IP blocking.
Similar to Cloudflare Geo-blocking, AWS WAF Geographic Rules.

Features:
- Block/allow by country code (ISO 3166-1 alpha-2)
- Block/allow by continent
- GDPR compliance (EU blocking option)
- OFAC sanctioned countries list
- MaxMind GeoIP2 integration
- Redis caching for lookups
"""

import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)


class GeoBlockMode(str, Enum):
    """Geo-blocking mode."""
    ALLOW_LIST = "allow_list"  # Only allow listed countries
    BLOCK_LIST = "block_list"  # Block listed countries


@dataclass
class GeoBlockConfig:
    """Configuration for geo-blocking."""
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    mode: GeoBlockMode = GeoBlockMode.BLOCK_LIST
    key_prefix: str = "geoblock:"

    # MaxMind GeoIP2 database path
    geoip_db_path: str = "/var/lib/GeoIP/GeoLite2-Country.mmdb"

    # Alternative: IP-API.com (free tier: 45 req/min)
    use_ip_api: bool = True
    ip_api_timeout: float = 2.0

    # Cache settings
    cache_ttl: int = 86400  # 24 hours
    cache_lookups: bool = True

    # Default lists
    blocked_countries: List[str] = field(default_factory=list)
    allowed_countries: List[str] = field(default_factory=list)
    blocked_continents: List[str] = field(default_factory=list)

    # Presets
    block_ofac_sanctioned: bool = False  # Iran, North Korea, Cuba, Syria, etc.
    block_high_risk: bool = False  # High fraud/attack origin countries
    eu_only: bool = False  # GDPR compliance - only allow EU


# OFAC sanctioned countries (as of 2024)
OFAC_SANCTIONED = {
    "CU",  # Cuba
    "IR",  # Iran
    "KP",  # North Korea
    "SY",  # Syria
    "RU",  # Russia (partial)
    "BY",  # Belarus
}

# High-risk countries (common attack origins - adjust based on your threat model)
HIGH_RISK_COUNTRIES = {
    # Add based on your specific threat intelligence
}

# EU member states (for GDPR compliance)
EU_COUNTRIES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}

# EEA (EU + Iceland, Liechtenstein, Norway)
EEA_COUNTRIES = EU_COUNTRIES | {"IS", "LI", "NO"}

# Continent codes
CONTINENTS = {
    "AF": "Africa",
    "AN": "Antarctica",
    "AS": "Asia",
    "EU": "Europe",
    "NA": "North America",
    "OC": "Oceania",
    "SA": "South America",
}


@dataclass
class GeoLookupResult:
    """Result of geo lookup."""
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    continent_code: Optional[str] = None
    continent_name: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    is_eu: bool = False
    is_proxy: bool = False
    is_hosting: bool = False
    cached: bool = False


@dataclass
class GeoBlockResult:
    """Result of geo-blocking check."""
    allowed: bool
    country_code: Optional[str] = None
    reason: Optional[str] = None
    client_ip: str = ""


class GeoBlocker:
    """
    Geographic IP blocking service.

    Usage:
        blocker = GeoBlocker(config)
        await blocker.connect()

        # Check IP
        result = await blocker.check(request)
        if not result.allowed:
            raise HTTPException(403, f"Access denied from {result.country_code}")

        # Manage lists
        await blocker.add_blocked_country("XX")
        await blocker.add_allowed_country("US")
    """

    def __init__(self, config: Optional[GeoBlockConfig] = None):
        self.config = config or GeoBlockConfig()
        self._redis = None
        self._geoip_reader = None

        # Initialize lists
        self._blocked_countries: Set[str] = set(self.config.blocked_countries)
        self._allowed_countries: Set[str] = set(self.config.allowed_countries)
        self._blocked_continents: Set[str] = set(self.config.blocked_continents)

        # Apply presets
        if self.config.block_ofac_sanctioned:
            self._blocked_countries.update(OFAC_SANCTIONED)

        if self.config.block_high_risk:
            self._blocked_countries.update(HIGH_RISK_COUNTRIES)

        if self.config.eu_only:
            self._allowed_countries = EEA_COUNTRIES.copy()

    async def connect(self):
        """Connect to Redis and initialize GeoIP."""
        # Connect to Redis
        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info("Geo-blocker connected to Redis")
        except Exception as e:
            logger.warning(f"Geo-blocker Redis connection failed: {e}")
            self._redis = None

        # Try to load MaxMind database
        if os.path.exists(self.config.geoip_db_path):
            try:
                import geoip2.database
                self._geoip_reader = geoip2.database.Reader(self.config.geoip_db_path)
                logger.info(f"Loaded GeoIP database: {self.config.geoip_db_path}")
            except ImportError:
                logger.info("geoip2 not installed, using IP-API fallback")
            except Exception as e:
                logger.warning(f"Failed to load GeoIP database: {e}")

    async def close(self):
        """Close connections."""
        if self._redis:
            await self._redis.close()
        if self._geoip_reader:
            self._geoip_reader.close()

    async def _lookup_ip_cache(self, ip: str) -> Optional[GeoLookupResult]:
        """Check cache for IP lookup."""
        if not self._redis or not self.config.cache_lookups:
            return None

        try:
            key = f"{self.config.key_prefix}lookup:{ip}"
            data = await self._redis.hgetall(key)

            if data:
                return GeoLookupResult(
                    country_code=data.get("country_code"),
                    country_name=data.get("country_name"),
                    continent_code=data.get("continent_code"),
                    is_eu=data.get("is_eu") == "1",
                    cached=True,
                )
            return None

        except Exception as e:
            logger.debug(f"Cache lookup failed: {e}")
            return None

    async def _cache_lookup(self, ip: str, result: GeoLookupResult):
        """Cache IP lookup result."""
        if not self._redis or not self.config.cache_lookups:
            return

        try:
            key = f"{self.config.key_prefix}lookup:{ip}"
            await self._redis.hset(key, mapping={
                "country_code": result.country_code or "",
                "country_name": result.country_name or "",
                "continent_code": result.continent_code or "",
                "is_eu": "1" if result.is_eu else "0",
            })
            await self._redis.expire(key, self.config.cache_ttl)

        except Exception as e:
            logger.debug(f"Cache write failed: {e}")

    async def _lookup_maxmind(self, ip: str) -> Optional[GeoLookupResult]:
        """Lookup IP using MaxMind GeoIP2."""
        if not self._geoip_reader:
            return None

        try:
            response = self._geoip_reader.country(ip)
            return GeoLookupResult(
                country_code=response.country.iso_code,
                country_name=response.country.name,
                continent_code=response.continent.code,
                continent_name=response.continent.name,
                is_eu=response.country.is_in_european_union,
            )
        except Exception as e:
            logger.debug(f"MaxMind lookup failed: {e}")
            return None

    async def _lookup_ip_api(self, ip: str) -> Optional[GeoLookupResult]:
        """Lookup IP using IP-API.com (free tier)."""
        if not self.config.use_ip_api:
            return None

        try:
            import httpx

            async with httpx.AsyncClient(timeout=self.config.ip_api_timeout) as client:
                response = await client.get(
                    f"http://ip-api.com/json/{ip}",
                    params={"fields": "status,country,countryCode,continent,continentCode,city,region,proxy,hosting"}
                )

                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "success":
                        country_code = data.get("countryCode")
                        return GeoLookupResult(
                            country_code=country_code,
                            country_name=data.get("country"),
                            continent_code=data.get("continentCode"),
                            continent_name=data.get("continent"),
                            city=data.get("city"),
                            region=data.get("region"),
                            is_eu=country_code in EU_COUNTRIES if country_code else False,
                            is_proxy=data.get("proxy", False),
                            is_hosting=data.get("hosting", False),
                        )
        except Exception as e:
            logger.debug(f"IP-API lookup failed: {e}")

        return None

    async def lookup_ip(self, ip: str) -> GeoLookupResult:
        """
        Lookup geographic information for an IP.

        Tries in order:
        1. Redis cache
        2. MaxMind GeoIP2 database
        3. IP-API.com service
        """
        # Check cache first
        cached = await self._lookup_ip_cache(ip)
        if cached:
            return cached

        # Try MaxMind
        result = await self._lookup_maxmind(ip)

        # Fall back to IP-API
        if not result:
            result = await self._lookup_ip_api(ip)

        # Cache result
        if result:
            await self._cache_lookup(ip, result)
        else:
            result = GeoLookupResult()  # Unknown location

        return result

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request."""
        # Check X-Forwarded-For
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        # Direct connection
        return request.client.host if request.client else "127.0.0.1"

    async def check(
        self,
        request: Request,
        tenant_id: Optional[str] = None
    ) -> GeoBlockResult:
        """
        Check if request is allowed based on geographic location.

        Args:
            request: FastAPI request
            tenant_id: Optional tenant for tenant-specific rules

        Returns:
            GeoBlockResult with allowed status
        """
        client_ip = self._get_client_ip(request)

        # Skip for private IPs
        import ipaddress
        try:
            ip_obj = ipaddress.ip_address(client_ip)
            if ip_obj.is_private or ip_obj.is_loopback:
                return GeoBlockResult(
                    allowed=True,
                    reason="Private/loopback IP",
                    client_ip=client_ip,
                )
        except ValueError:
            pass

        # Lookup IP location
        geo = await self.lookup_ip(client_ip)

        if not geo.country_code:
            # Unknown location - allow or deny based on config
            return GeoBlockResult(
                allowed=True,  # Default allow unknown
                reason="Unknown location",
                client_ip=client_ip,
            )

        # Get tenant-specific lists
        tenant_blocked, tenant_allowed = await self._get_tenant_lists(tenant_id)

        # Combine with global lists
        blocked_countries = self._blocked_countries | tenant_blocked
        allowed_countries = self._allowed_countries | tenant_allowed

        # Check based on mode
        if self.config.mode == GeoBlockMode.ALLOW_LIST or self.config.eu_only:
            # Only allow listed countries
            if allowed_countries and geo.country_code not in allowed_countries:
                return GeoBlockResult(
                    allowed=False,
                    country_code=geo.country_code,
                    reason=f"Country {geo.country_code} not in allow list",
                    client_ip=client_ip,
                )

        elif self.config.mode == GeoBlockMode.BLOCK_LIST:
            # Block listed countries
            if geo.country_code in blocked_countries:
                return GeoBlockResult(
                    allowed=False,
                    country_code=geo.country_code,
                    reason=f"Country {geo.country_code} is blocked",
                    client_ip=client_ip,
                )

            # Check blocked continents
            if geo.continent_code in self._blocked_continents:
                return GeoBlockResult(
                    allowed=False,
                    country_code=geo.country_code,
                    reason=f"Continent {geo.continent_code} is blocked",
                    client_ip=client_ip,
                )

        return GeoBlockResult(
            allowed=True,
            country_code=geo.country_code,
            reason="Allowed",
            client_ip=client_ip,
        )

    async def _get_tenant_lists(
        self,
        tenant_id: Optional[str]
    ) -> tuple[Set[str], Set[str]]:
        """Get tenant-specific geo lists from Redis."""
        blocked: Set[str] = set()
        allowed: Set[str] = set()

        if not self._redis or not tenant_id:
            return blocked, allowed

        try:
            blocked_key = f"{self.config.key_prefix}blocked:{tenant_id}"
            allowed_key = f"{self.config.key_prefix}allowed:{tenant_id}"

            blocked = set(await self._redis.smembers(blocked_key))
            allowed = set(await self._redis.smembers(allowed_key))

        except Exception as e:
            logger.debug(f"Failed to get tenant geo lists: {e}")

        return blocked, allowed

    async def add_blocked_country(
        self,
        country_code: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Add country to block list."""
        country_code = country_code.upper()

        if tenant_id and self._redis:
            key = f"{self.config.key_prefix}blocked:{tenant_id}"
            await self._redis.sadd(key, country_code)
        else:
            self._blocked_countries.add(country_code)

        logger.info(f"Added blocked country: {country_code}")
        return True

    async def add_allowed_country(
        self,
        country_code: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Add country to allow list."""
        country_code = country_code.upper()

        if tenant_id and self._redis:
            key = f"{self.config.key_prefix}allowed:{tenant_id}"
            await self._redis.sadd(key, country_code)
        else:
            self._allowed_countries.add(country_code)

        logger.info(f"Added allowed country: {country_code}")
        return True

    async def remove_blocked_country(
        self,
        country_code: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Remove country from block list."""
        country_code = country_code.upper()

        if tenant_id and self._redis:
            key = f"{self.config.key_prefix}blocked:{tenant_id}"
            await self._redis.srem(key, country_code)
        else:
            self._blocked_countries.discard(country_code)

        return True

    async def get_stats(self) -> Dict:
        """Get geo-blocking statistics."""
        return {
            "mode": self.config.mode.value,
            "blocked_countries": list(self._blocked_countries),
            "allowed_countries": list(self._allowed_countries),
            "blocked_continents": list(self._blocked_continents),
            "presets": {
                "ofac_sanctioned": self.config.block_ofac_sanctioned,
                "high_risk": self.config.block_high_risk,
                "eu_only": self.config.eu_only,
            },
            "geoip_database": self._geoip_reader is not None,
            "ip_api_enabled": self.config.use_ip_api,
        }


# Singleton
_blocker_instance: Optional[GeoBlocker] = None


async def get_geo_blocker(
    config: Optional[GeoBlockConfig] = None
) -> GeoBlocker:
    """Get or create geo-blocker singleton."""
    global _blocker_instance

    if _blocker_instance is None:
        _blocker_instance = GeoBlocker(config)
        await _blocker_instance.connect()

    return _blocker_instance


async def geo_block_dependency(request: Request):
    """
    FastAPI dependency for geo-blocking.

    Usage:
        @app.get("/api/data")
        async def get_data(_: None = Depends(geo_block_dependency)):
            return {"data": "value"}
    """
    blocker = await get_geo_blocker()
    result = await blocker.check(request)

    if not result.allowed:
        logger.warning(
            f"Geo-blocked: {result.client_ip} ({result.country_code}) - {result.reason}",
            extra={
                "event_type": "geo_blocked",
                "client_ip": result.client_ip,
                "country_code": result.country_code,
                "reason": result.reason,
            }
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied from your location"
        )

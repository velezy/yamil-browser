"""
Quota Management Module

Enterprise-grade API quota enforcement.
Similar to AWS API Gateway Usage Plans, Apigee Quota Policy.

Features:
- Per-tenant/user/API key quotas
- Multiple time windows (minute/hour/day/month)
- Quota tiers (Bronze/Silver/Gold)
- Quota reset schedules
- Overage handling (block/throttle/charge)
- Usage tracking and alerts
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)


class QuotaPeriod(str, Enum):
    """Quota time periods."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class OverageAction(str, Enum):
    """Action when quota exceeded."""
    BLOCK = "block"  # Return 429
    THROTTLE = "throttle"  # Slow down requests
    ALLOW = "allow"  # Allow but log
    CHARGE = "charge"  # Allow and bill for overage


@dataclass
class QuotaLimit:
    """Single quota limit."""
    limit: int
    period: QuotaPeriod
    overage_action: OverageAction = OverageAction.BLOCK


@dataclass
class QuotaTier:
    """Quota tier (usage plan)."""
    name: str
    limits: List[QuotaLimit] = field(default_factory=list)
    description: str = ""
    price_monthly: float = 0.0


@dataclass
class QuotaConfig:
    """Configuration for quota manager."""
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    key_prefix: str = "quota:"

    # Default quotas (can be overridden per tenant/tier)
    default_limits: List[QuotaLimit] = field(default_factory=lambda: [
        QuotaLimit(limit=1000, period=QuotaPeriod.MINUTE),
        QuotaLimit(limit=10000, period=QuotaPeriod.HOUR),
        QuotaLimit(limit=100000, period=QuotaPeriod.DAY),
    ])

    # Alert thresholds (percentage of quota)
    warning_threshold: float = 0.8  # 80%
    critical_threshold: float = 0.95  # 95%

    # Grace period for quota reset (seconds)
    reset_grace_period: int = 60


# Predefined tiers
QUOTA_TIERS = {
    "free": QuotaTier(
        name="Free",
        description="Free tier with limited access",
        limits=[
            QuotaLimit(limit=100, period=QuotaPeriod.MINUTE),
            QuotaLimit(limit=1000, period=QuotaPeriod.DAY),
        ],
    ),
    "bronze": QuotaTier(
        name="Bronze",
        description="Basic paid tier",
        price_monthly=49.0,
        limits=[
            QuotaLimit(limit=500, period=QuotaPeriod.MINUTE),
            QuotaLimit(limit=10000, period=QuotaPeriod.DAY),
            QuotaLimit(limit=100000, period=QuotaPeriod.MONTH),
        ],
    ),
    "silver": QuotaTier(
        name="Silver",
        description="Standard business tier",
        price_monthly=199.0,
        limits=[
            QuotaLimit(limit=2000, period=QuotaPeriod.MINUTE),
            QuotaLimit(limit=50000, period=QuotaPeriod.DAY),
            QuotaLimit(limit=500000, period=QuotaPeriod.MONTH),
        ],
    ),
    "gold": QuotaTier(
        name="Gold",
        description="Premium enterprise tier",
        price_monthly=499.0,
        limits=[
            QuotaLimit(limit=10000, period=QuotaPeriod.MINUTE),
            QuotaLimit(limit=500000, period=QuotaPeriod.DAY),
            QuotaLimit(limit=5000000, period=QuotaPeriod.MONTH),
        ],
    ),
    "unlimited": QuotaTier(
        name="Unlimited",
        description="No quota limits",
        price_monthly=999.0,
        limits=[],  # No limits
    ),
}


@dataclass
class QuotaUsage:
    """Current quota usage."""
    used: int
    limit: int
    remaining: int
    reset_at: datetime
    period: QuotaPeriod
    percentage: float


@dataclass
class QuotaCheckResult:
    """Result of quota check."""
    allowed: bool
    usage: List[QuotaUsage]
    exceeded_limit: Optional[QuotaLimit] = None
    action: OverageAction = OverageAction.BLOCK
    retry_after_seconds: int = 0


class QuotaManager:
    """
    API quota management service.

    Usage:
        manager = QuotaManager(config)
        await manager.connect()

        # Check quota
        result = await manager.check("tenant-123")
        if not result.allowed:
            raise HTTPException(429, "Quota exceeded")

        # Increment usage
        await manager.increment("tenant-123")

        # Set tier for tenant
        await manager.set_tier("tenant-123", "gold")

        # Get usage
        usage = await manager.get_usage("tenant-123")
    """

    def __init__(self, config: Optional[QuotaConfig] = None):
        self.config = config or QuotaConfig()
        self._redis = None

    async def connect(self):
        """Connect to Redis."""
        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info("Quota manager connected to Redis")
        except Exception as e:
            logger.warning(f"Quota manager Redis connection failed: {e}")
            self._redis = None

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    def _get_period_seconds(self, period: QuotaPeriod) -> int:
        """Get period duration in seconds."""
        return {
            QuotaPeriod.SECOND: 1,
            QuotaPeriod.MINUTE: 60,
            QuotaPeriod.HOUR: 3600,
            QuotaPeriod.DAY: 86400,
            QuotaPeriod.WEEK: 604800,
            QuotaPeriod.MONTH: 2592000,  # 30 days
        }[period]

    def _get_period_key(self, identifier: str, period: QuotaPeriod) -> str:
        """Generate Redis key for a quota period."""
        now = datetime.now(timezone.utc)

        if period == QuotaPeriod.SECOND:
            window = now.strftime("%Y%m%d%H%M%S")
        elif period == QuotaPeriod.MINUTE:
            window = now.strftime("%Y%m%d%H%M")
        elif period == QuotaPeriod.HOUR:
            window = now.strftime("%Y%m%d%H")
        elif period == QuotaPeriod.DAY:
            window = now.strftime("%Y%m%d")
        elif period == QuotaPeriod.WEEK:
            window = now.strftime("%Y%W")
        elif period == QuotaPeriod.MONTH:
            window = now.strftime("%Y%m")
        else:
            window = now.strftime("%Y%m%d%H%M")

        return f"{self.config.key_prefix}{identifier}:{period.value}:{window}"

    def _get_reset_time(self, period: QuotaPeriod) -> datetime:
        """Get next reset time for a period."""
        now = datetime.now(timezone.utc)

        if period == QuotaPeriod.SECOND:
            return now.replace(microsecond=0) + timedelta(seconds=1)
        elif period == QuotaPeriod.MINUTE:
            return now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        elif period == QuotaPeriod.HOUR:
            return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif period == QuotaPeriod.DAY:
            return now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        elif period == QuotaPeriod.WEEK:
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0:
                days_until_monday = 7
            return now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_until_monday)
        elif period == QuotaPeriod.MONTH:
            if now.month == 12:
                return now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                return now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            return now + timedelta(minutes=1)

    async def get_tier(self, identifier: str) -> QuotaTier:
        """Get quota tier for an identifier."""
        if not self._redis:
            return QUOTA_TIERS.get("bronze", QuotaTier(name="default", limits=self.config.default_limits))

        try:
            tier_key = f"{self.config.key_prefix}tier:{identifier}"
            tier_name = await self._redis.get(tier_key)

            if tier_name and tier_name in QUOTA_TIERS:
                return QUOTA_TIERS[tier_name]

            return QUOTA_TIERS.get("bronze", QuotaTier(name="default", limits=self.config.default_limits))

        except Exception as e:
            logger.warning(f"Failed to get tier: {e}")
            return QuotaTier(name="default", limits=self.config.default_limits)

    async def set_tier(self, identifier: str, tier_name: str) -> bool:
        """Set quota tier for an identifier."""
        if not self._redis:
            return False

        if tier_name not in QUOTA_TIERS:
            logger.error(f"Unknown tier: {tier_name}")
            return False

        try:
            tier_key = f"{self.config.key_prefix}tier:{identifier}"
            await self._redis.set(tier_key, tier_name)
            logger.info(f"Set tier for {identifier}: {tier_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to set tier: {e}")
            return False

    async def check(
        self,
        identifier: str,
        cost: int = 1
    ) -> QuotaCheckResult:
        """
        Check if request is within quota.

        Args:
            identifier: Tenant/user/API key identifier
            cost: Number of quota units to consume

        Returns:
            QuotaCheckResult with allowed status and usage info
        """
        tier = await self.get_tier(identifier)

        # Unlimited tier
        if not tier.limits:
            return QuotaCheckResult(allowed=True, usage=[])

        usage_list = []
        exceeded_limit = None
        action = OverageAction.BLOCK

        for limit in tier.limits:
            current_usage = await self._get_current_usage(identifier, limit.period)
            remaining = max(0, limit.limit - current_usage)
            reset_at = self._get_reset_time(limit.period)
            percentage = (current_usage / limit.limit * 100) if limit.limit > 0 else 0

            usage = QuotaUsage(
                used=current_usage,
                limit=limit.limit,
                remaining=remaining,
                reset_at=reset_at,
                period=limit.period,
                percentage=percentage,
            )
            usage_list.append(usage)

            # Check if this limit is exceeded
            if current_usage + cost > limit.limit:
                if exceeded_limit is None:
                    exceeded_limit = limit
                    action = limit.overage_action

            # Check warning/critical thresholds
            if percentage >= self.config.critical_threshold * 100:
                logger.warning(
                    f"Quota critical: {identifier} at {percentage:.1f}% of {limit.period.value} limit",
                    extra={
                        "event_type": "quota_critical",
                        "identifier": identifier,
                        "period": limit.period.value,
                        "percentage": percentage,
                    }
                )
            elif percentage >= self.config.warning_threshold * 100:
                logger.info(
                    f"Quota warning: {identifier} at {percentage:.1f}% of {limit.period.value} limit",
                    extra={
                        "event_type": "quota_warning",
                        "identifier": identifier,
                        "period": limit.period.value,
                        "percentage": percentage,
                    }
                )

        # Calculate retry-after
        retry_after = 0
        if exceeded_limit:
            reset_at = self._get_reset_time(exceeded_limit.period)
            retry_after = int((reset_at - datetime.now(timezone.utc)).total_seconds())

        allowed = exceeded_limit is None or action == OverageAction.ALLOW or action == OverageAction.CHARGE

        return QuotaCheckResult(
            allowed=allowed,
            usage=usage_list,
            exceeded_limit=exceeded_limit,
            action=action,
            retry_after_seconds=max(0, retry_after),
        )

    async def _get_current_usage(self, identifier: str, period: QuotaPeriod) -> int:
        """Get current usage for a period."""
        if not self._redis:
            return 0

        try:
            key = self._get_period_key(identifier, period)
            value = await self._redis.get(key)
            return int(value) if value else 0

        except Exception as e:
            logger.warning(f"Failed to get usage: {e}")
            return 0

    async def increment(
        self,
        identifier: str,
        cost: int = 1
    ) -> Dict[QuotaPeriod, int]:
        """
        Increment usage counters.

        Returns:
            Dict of period -> new count
        """
        if not self._redis:
            return {}

        tier = await self.get_tier(identifier)
        results = {}

        try:
            for limit in tier.limits:
                key = self._get_period_key(identifier, limit.period)
                ttl = self._get_period_seconds(limit.period) + self.config.reset_grace_period

                new_count = await self._redis.incrby(key, cost)
                await self._redis.expire(key, ttl)
                results[limit.period] = new_count

            return results

        except Exception as e:
            logger.error(f"Failed to increment usage: {e}")
            return {}

    async def get_usage(self, identifier: str) -> List[QuotaUsage]:
        """Get current usage across all periods."""
        tier = await self.get_tier(identifier)
        usage_list = []

        for limit in tier.limits:
            current = await self._get_current_usage(identifier, limit.period)
            reset_at = self._get_reset_time(limit.period)
            percentage = (current / limit.limit * 100) if limit.limit > 0 else 0

            usage_list.append(QuotaUsage(
                used=current,
                limit=limit.limit,
                remaining=max(0, limit.limit - current),
                reset_at=reset_at,
                period=limit.period,
                percentage=percentage,
            ))

        return usage_list

    async def reset(self, identifier: str, period: Optional[QuotaPeriod] = None) -> bool:
        """Reset usage for an identifier."""
        if not self._redis:
            return False

        try:
            if period:
                key = self._get_period_key(identifier, period)
                await self._redis.delete(key)
            else:
                # Reset all periods
                pattern = f"{self.config.key_prefix}{identifier}:*"
                async for key in self._redis.scan_iter(match=pattern):
                    await self._redis.delete(key)

            logger.info(f"Reset quota for {identifier}")
            return True

        except Exception as e:
            logger.error(f"Failed to reset quota: {e}")
            return False


# Import for period calculations
from datetime import timedelta

# Singleton
_quota_manager: Optional[QuotaManager] = None


async def get_quota_manager(
    config: Optional[QuotaConfig] = None
) -> QuotaManager:
    """Get or create quota manager singleton."""
    global _quota_manager

    if _quota_manager is None:
        _quota_manager = QuotaManager(config)
        await _quota_manager.connect()

    return _quota_manager


async def quota_check_dependency(request: Request):
    """
    FastAPI dependency for quota checking.

    Usage:
        @app.get("/api/data")
        async def get_data(_: None = Depends(quota_check_dependency)):
            return {"data": "value"}
    """
    # Get identifier from request (tenant_id, user_id, or API key)
    identifier = getattr(request.state, "tenant_id", None)
    if not identifier:
        identifier = getattr(request.state, "user_id", None)
    if not identifier:
        identifier = request.client.host if request.client else "unknown"

    manager = await get_quota_manager()
    result = await manager.check(identifier)

    if not result.allowed:
        # Add quota headers
        usage = result.usage[0] if result.usage else None
        headers = {}
        if usage:
            headers["X-RateLimit-Limit"] = str(usage.limit)
            headers["X-RateLimit-Remaining"] = str(usage.remaining)
            headers["X-RateLimit-Reset"] = str(int(usage.reset_at.timestamp()))
        if result.retry_after_seconds > 0:
            headers["Retry-After"] = str(result.retry_after_seconds)

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Quota exceeded",
            headers=headers,
        )

    # Increment usage
    await manager.increment(identifier)

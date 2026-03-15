"""
API Analytics Module

Enterprise-grade API analytics and metrics collection.
Similar to Kong Analytics, Apigee Analytics, AWS API Gateway Metrics.

Features:
- Request/response metrics (latency, size, status)
- Endpoint-level aggregation
- Tenant/user analytics
- Error tracking and patterns
- Geographic distribution
- Time-series data
- Real-time dashboards
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class TimeGranularity(str, Enum):
    """Time aggregation granularity."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


@dataclass
class AnalyticsConfig:
    """Configuration for API analytics."""
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    key_prefix: str = "analytics:"

    # Data retention
    retention_seconds: int = 86400  # 24 hours for second granularity
    retention_minutes: int = 604800  # 7 days for minute granularity
    retention_hours: int = 2592000  # 30 days for hour granularity
    retention_days: int = 31536000  # 365 days for day granularity

    # Sampling (for high-volume APIs)
    sample_rate: float = 1.0  # 1.0 = 100% of requests
    sample_slow_requests: bool = True  # Always sample slow requests
    slow_request_threshold_ms: int = 1000  # What's considered slow

    # Histogram buckets for latency (ms)
    latency_buckets: List[int] = field(default_factory=lambda: [
        10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000
    ])


@dataclass
class RequestMetrics:
    """Metrics for a single request."""
    timestamp: datetime
    method: str
    path: str
    status_code: int
    latency_ms: float
    request_size: int
    response_size: int
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    client_ip: str = ""
    user_agent: str = ""
    country_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class AggregatedMetrics:
    """Aggregated metrics for a time period."""
    timestamp: datetime
    granularity: TimeGranularity
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    total_request_bytes: int = 0
    total_response_bytes: int = 0

    # Status code distribution
    status_2xx: int = 0
    status_3xx: int = 0
    status_4xx: int = 0
    status_5xx: int = 0

    # Latency histogram
    latency_histogram: Dict[int, int] = field(default_factory=dict)

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.total_requests if self.total_requests > 0 else 0

    @property
    def success_rate(self) -> float:
        return self.successful_requests / self.total_requests * 100 if self.total_requests > 0 else 0

    @property
    def error_rate(self) -> float:
        return self.failed_requests / self.total_requests * 100 if self.total_requests > 0 else 0


class APIAnalytics:
    """
    API analytics collection and aggregation.

    Usage:
        analytics = APIAnalytics(config)
        await analytics.connect()

        # Record a request (usually via middleware)
        await analytics.record(RequestMetrics(
            timestamp=datetime.now(timezone.utc),
            method="GET",
            path="/api/v1/users",
            status_code=200,
            latency_ms=45.2,
            request_size=0,
            response_size=1024,
        ))

        # Get analytics
        metrics = await analytics.get_metrics(
            start_time=datetime.now() - timedelta(hours=1),
            granularity=TimeGranularity.MINUTE,
        )

        # Get endpoint stats
        stats = await analytics.get_endpoint_stats("/api/v1/users")
    """

    def __init__(self, config: Optional[AnalyticsConfig] = None):
        self.config = config or AnalyticsConfig()
        self._redis = None

        # In-memory buffer for batching
        self._buffer: List[RequestMetrics] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

    async def connect(self):
        """Connect to Redis and start background flush task."""
        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info("API analytics connected to Redis")

            # Start background flush task
            self._flush_task = asyncio.create_task(self._flush_loop())

        except Exception as e:
            logger.warning(f"API analytics Redis connection failed: {e}")
            self._redis = None

    async def close(self):
        """Close connections and flush remaining data."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush_buffer()

        if self._redis:
            await self._redis.close()

    async def _flush_loop(self, interval: float = 5.0):
        """Background task to periodically flush buffer."""
        while True:
            try:
                await asyncio.sleep(interval)
                await self._flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Flush error: {e}")

    async def _flush_buffer(self):
        """Flush buffered metrics to Redis."""
        if not self._redis:
            return

        async with self._buffer_lock:
            if not self._buffer:
                return

            metrics_to_flush = self._buffer.copy()
            self._buffer.clear()

        try:
            pipe = self._redis.pipeline()

            for metrics in metrics_to_flush:
                await self._record_to_redis(metrics, pipe)

            await pipe.execute()

        except Exception as e:
            logger.error(f"Failed to flush analytics: {e}")
            # Re-add to buffer on failure
            async with self._buffer_lock:
                self._buffer.extend(metrics_to_flush)

    def _should_sample(self, metrics: RequestMetrics) -> bool:
        """Determine if request should be sampled."""
        # Always sample slow requests
        if self.config.sample_slow_requests:
            if metrics.latency_ms >= self.config.slow_request_threshold_ms:
                return True

        # Always sample errors
        if metrics.status_code >= 500:
            return True

        # Random sampling
        import random
        return random.random() < self.config.sample_rate

    async def record(self, metrics: RequestMetrics):
        """
        Record request metrics.

        Buffers metrics and flushes periodically.
        """
        if not self._should_sample(metrics):
            return

        async with self._buffer_lock:
            self._buffer.append(metrics)

            # Flush if buffer is large
            if len(self._buffer) >= 100:
                asyncio.create_task(self._flush_buffer())

    async def _record_to_redis(self, metrics: RequestMetrics, pipe):
        """Record metrics to Redis via pipeline."""
        timestamp = metrics.timestamp
        prefix = self.config.key_prefix

        # Generate time-based keys
        minute_key = timestamp.strftime("%Y%m%d%H%M")
        hour_key = timestamp.strftime("%Y%m%d%H")
        day_key = timestamp.strftime("%Y%m%d")

        # Global counters
        pipe.hincrby(f"{prefix}global:minute:{minute_key}", "requests", 1)
        pipe.hincrby(f"{prefix}global:minute:{minute_key}", "latency_total", int(metrics.latency_ms))
        pipe.hincrby(f"{prefix}global:minute:{minute_key}", f"status_{metrics.status_code // 100}xx", 1)
        pipe.expire(f"{prefix}global:minute:{minute_key}", self.config.retention_minutes)

        # Endpoint-level metrics
        endpoint_key = f"{prefix}endpoint:{metrics.method}:{metrics.path}:minute:{minute_key}"
        pipe.hincrby(endpoint_key, "requests", 1)
        pipe.hincrby(endpoint_key, "latency_total", int(metrics.latency_ms))
        pipe.hincrby(endpoint_key, f"status_{metrics.status_code}", 1)
        pipe.expire(endpoint_key, self.config.retention_minutes)

        # Latency histogram
        bucket = self._get_latency_bucket(metrics.latency_ms)
        pipe.hincrby(f"{prefix}histogram:minute:{minute_key}", f"le_{bucket}", 1)
        pipe.expire(f"{prefix}histogram:minute:{minute_key}", self.config.retention_minutes)

        # Tenant-level metrics
        if metrics.tenant_id:
            tenant_key = f"{prefix}tenant:{metrics.tenant_id}:minute:{minute_key}"
            pipe.hincrby(tenant_key, "requests", 1)
            pipe.hincrby(tenant_key, "latency_total", int(metrics.latency_ms))
            pipe.expire(tenant_key, self.config.retention_minutes)

        # Error tracking
        if metrics.status_code >= 400:
            error_key = f"{prefix}errors:minute:{minute_key}"
            error_data = f"{metrics.status_code}:{metrics.path}:{metrics.error_message or ''}"
            pipe.lpush(error_key, error_data)
            pipe.ltrim(error_key, 0, 999)  # Keep last 1000 errors
            pipe.expire(error_key, self.config.retention_minutes)

        # Top endpoints (sorted set)
        pipe.zincrby(f"{prefix}top_endpoints:hour:{hour_key}", 1, f"{metrics.method}:{metrics.path}")
        pipe.expire(f"{prefix}top_endpoints:hour:{hour_key}", self.config.retention_hours)

        # Geographic distribution
        if metrics.country_code:
            pipe.hincrby(f"{prefix}geo:day:{day_key}", metrics.country_code, 1)
            pipe.expire(f"{prefix}geo:day:{day_key}", self.config.retention_days)

    def _get_latency_bucket(self, latency_ms: float) -> int:
        """Get histogram bucket for latency."""
        for bucket in self.config.latency_buckets:
            if latency_ms <= bucket:
                return bucket
        return self.config.latency_buckets[-1] + 1  # +Inf bucket

    async def get_metrics(
        self,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        granularity: TimeGranularity = TimeGranularity.MINUTE,
        tenant_id: Optional[str] = None,
        endpoint: Optional[str] = None,
    ) -> List[AggregatedMetrics]:
        """
        Get aggregated metrics for a time range.

        Args:
            start_time: Start of time range
            end_time: End of time range (default: now)
            granularity: Time aggregation level
            tenant_id: Filter by tenant
            endpoint: Filter by endpoint (e.g., "GET:/api/v1/users")

        Returns:
            List of AggregatedMetrics for each time bucket
        """
        if not self._redis:
            return []

        end_time = end_time or datetime.now(timezone.utc)
        prefix = self.config.key_prefix
        results = []

        try:
            # Generate time keys based on granularity
            current = start_time
            while current <= end_time:
                if granularity == TimeGranularity.MINUTE:
                    time_key = current.strftime("%Y%m%d%H%M")
                    delta = timedelta(minutes=1)
                elif granularity == TimeGranularity.HOUR:
                    time_key = current.strftime("%Y%m%d%H")
                    delta = timedelta(hours=1)
                elif granularity == TimeGranularity.DAY:
                    time_key = current.strftime("%Y%m%d")
                    delta = timedelta(days=1)
                else:
                    time_key = current.strftime("%Y%m%d%H%M%S")
                    delta = timedelta(seconds=1)

                # Build key
                if tenant_id:
                    key = f"{prefix}tenant:{tenant_id}:{granularity.value}:{time_key}"
                elif endpoint:
                    key = f"{prefix}endpoint:{endpoint}:{granularity.value}:{time_key}"
                else:
                    key = f"{prefix}global:{granularity.value}:{time_key}"

                # Fetch data
                data = await self._redis.hgetall(key)

                if data:
                    metrics = AggregatedMetrics(
                        timestamp=current,
                        granularity=granularity,
                        total_requests=int(data.get("requests", 0)),
                        total_latency_ms=float(data.get("latency_total", 0)),
                        status_2xx=int(data.get("status_2xx", 0)),
                        status_3xx=int(data.get("status_3xx", 0)),
                        status_4xx=int(data.get("status_4xx", 0)),
                        status_5xx=int(data.get("status_5xx", 0)),
                    )
                    metrics.successful_requests = metrics.status_2xx + metrics.status_3xx
                    metrics.failed_requests = metrics.status_4xx + metrics.status_5xx
                    results.append(metrics)

                current += delta

            return results

        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return []

    async def get_endpoint_stats(
        self,
        endpoint: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get statistics for a specific endpoint."""
        if not self._redis:
            return {}

        start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        metrics = await self.get_metrics(
            start_time=start_time,
            granularity=TimeGranularity.HOUR,
            endpoint=endpoint,
        )

        if not metrics:
            return {"endpoint": endpoint, "no_data": True}

        total_requests = sum(m.total_requests for m in metrics)
        total_latency = sum(m.total_latency_ms for m in metrics)
        total_errors = sum(m.failed_requests for m in metrics)

        return {
            "endpoint": endpoint,
            "period_hours": hours,
            "total_requests": total_requests,
            "avg_latency_ms": total_latency / total_requests if total_requests > 0 else 0,
            "error_count": total_errors,
            "error_rate": total_errors / total_requests * 100 if total_requests > 0 else 0,
            "requests_per_hour": total_requests / hours,
        }

    async def get_top_endpoints(
        self,
        hours: int = 1,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get top endpoints by request count."""
        if not self._redis:
            return []

        prefix = self.config.key_prefix
        now = datetime.now(timezone.utc)

        try:
            # Aggregate across hours
            endpoint_counts: Dict[str, int] = defaultdict(int)

            for h in range(hours):
                hour_time = now - timedelta(hours=h)
                hour_key = hour_time.strftime("%Y%m%d%H")
                key = f"{prefix}top_endpoints:hour:{hour_key}"

                data = await self._redis.zrevrange(key, 0, 99, withscores=True)
                for endpoint, count in data:
                    endpoint_counts[endpoint] += int(count)

            # Sort and return top
            sorted_endpoints = sorted(
                endpoint_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:limit]

            return [
                {"endpoint": ep, "requests": count}
                for ep, count in sorted_endpoints
            ]

        except Exception as e:
            logger.error(f"Failed to get top endpoints: {e}")
            return []

    async def get_error_summary(
        self,
        minutes: int = 60
    ) -> Dict[str, Any]:
        """Get summary of recent errors."""
        if not self._redis:
            return {}

        prefix = self.config.key_prefix
        now = datetime.now(timezone.utc)
        errors: Dict[int, int] = defaultdict(int)
        error_endpoints: Dict[str, int] = defaultdict(int)

        try:
            for m in range(minutes):
                minute_time = now - timedelta(minutes=m)
                minute_key = minute_time.strftime("%Y%m%d%H%M")
                key = f"{prefix}errors:minute:{minute_key}"

                error_list = await self._redis.lrange(key, 0, -1)
                for error_str in error_list:
                    parts = error_str.split(":", 2)
                    if len(parts) >= 2:
                        status_code = int(parts[0])
                        path = parts[1]
                        errors[status_code] += 1
                        error_endpoints[path] += 1

            return {
                "period_minutes": minutes,
                "total_errors": sum(errors.values()),
                "by_status_code": dict(errors),
                "top_error_endpoints": sorted(
                    error_endpoints.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10],
            }

        except Exception as e:
            logger.error(f"Failed to get error summary: {e}")
            return {}


class AnalyticsMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware for automatic analytics collection.

    Usage:
        analytics = APIAnalytics(config)
        await analytics.connect()
        app.add_middleware(AnalyticsMiddleware, analytics=analytics)
    """

    def __init__(self, app, analytics: APIAnalytics):
        super().__init__(app)
        self.analytics = analytics

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.time()

        # Get request size
        request_size = int(request.headers.get("content-length", 0))

        # Process request
        response = await call_next(request)

        # Calculate metrics
        latency_ms = (time.time() - start_time) * 1000

        # Get response size
        response_size = int(response.headers.get("content-length", 0))

        # Get tenant/user from request state
        tenant_id = getattr(request.state, "tenant_id", None)
        user_id = getattr(request.state, "user_id", None)
        country_code = getattr(request.state, "country_code", None)

        # Record metrics
        metrics = RequestMetrics(
            timestamp=datetime.now(timezone.utc),
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            latency_ms=latency_ms,
            request_size=request_size,
            response_size=response_size,
            tenant_id=tenant_id,
            user_id=user_id,
            client_ip=request.client.host if request.client else "",
            user_agent=request.headers.get("user-agent", ""),
            country_code=country_code,
        )

        await self.analytics.record(metrics)

        # Add analytics headers
        response.headers["X-Response-Time"] = f"{latency_ms:.2f}ms"

        return response


# Singleton
_analytics_instance: Optional[APIAnalytics] = None


async def get_api_analytics(
    config: Optional[AnalyticsConfig] = None
) -> APIAnalytics:
    """Get or create API analytics singleton."""
    global _analytics_instance

    if _analytics_instance is None:
        _analytics_instance = APIAnalytics(config)
        await _analytics_instance.connect()

    return _analytics_instance

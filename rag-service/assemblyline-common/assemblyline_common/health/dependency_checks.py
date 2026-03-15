"""
Dependency Health Checks for Logic Weaver Services

Provides reusable health check functions for common dependencies:
- PostgreSQL (via asyncpg/SQLAlchemy)
- Redis (via redis-py)
- Kafka (via aiokafka)
- External HTTP services
- AWS services (S3, Secrets Manager, etc.)
"""

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional

from assemblyline_common.health.endpoints import DependencyHealth

logger = logging.getLogger(__name__)


class DependencyChecker:
    """
    Factory for creating dependency health check functions.

    Usage:
        checker = DependencyChecker()

        # Add checks to health router
        health_router.add_dependency_check(
            "postgres",
            checker.check_postgres(database_url)
        )
        health_router.add_dependency_check(
            "redis",
            checker.check_redis(redis_url)
        )
    """

    def __init__(self):
        self._check_cache: Dict[str, DependencyHealth] = {}
        self._cache_ttl: float = 5.0
        self._last_check: Dict[str, float] = {}

    def check_postgres(
        self,
        database_url: Optional[str] = None,
        pool: Optional[Any] = None,
    ):
        """
        Create a PostgreSQL health check function.

        Args:
            database_url: PostgreSQL connection string (supports SQLAlchemy format)
            pool: Existing SQLAlchemy async engine or asyncpg pool
        """
        url = database_url or os.getenv("DATABASE_URL")
        # Convert SQLAlchemy URL to asyncpg format
        if url and "+asyncpg" in url:
            url = url.replace("postgresql+asyncpg://", "postgresql://")

        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                if pool is not None:
                    # Use existing pool
                    async with pool.connect() as conn:
                        result = await conn.execute("SELECT 1")
                        await result.fetchone()
                else:
                    # Create temporary connection
                    import asyncpg
                    conn = await asyncpg.connect(url, timeout=5.0)
                    try:
                        await conn.fetchval("SELECT 1")
                    finally:
                        await conn.close()

                latency = (time.time() - start) * 1000
                return DependencyHealth(
                    name="postgres",
                    healthy=True,
                    latency_ms=latency,
                    message="Connected",
                    details={"query": "SELECT 1"},
                )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"PostgreSQL health check failed: {e}")
                return DependencyHealth(
                    name="postgres",
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                )

        return _check

    def check_redis(
        self,
        redis_url: Optional[str] = None,
        client: Optional[Any] = None,
        sentinel_master: Optional[str] = None,
    ):
        """
        Create a Redis health check function.

        Args:
            redis_url: Redis connection string
            client: Existing Redis client
            sentinel_master: Sentinel master name (for HA setups)
        """
        url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")

        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                if client is not None:
                    # Use existing client
                    info = await client.ping()
                else:
                    # Create temporary connection
                    import redis.asyncio as redis
                    r = redis.from_url(url, socket_timeout=5.0)
                    try:
                        await r.ping()
                        info = await r.info("server")
                    finally:
                        await r.close()

                latency = (time.time() - start) * 1000
                return DependencyHealth(
                    name="redis",
                    healthy=True,
                    latency_ms=latency,
                    message="Connected",
                    details={
                        "sentinel_master": sentinel_master,
                        "mode": "sentinel" if sentinel_master else "standalone",
                    },
                )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"Redis health check failed: {e}")
                return DependencyHealth(
                    name="redis",
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                )

        return _check

    def check_kafka(
        self,
        bootstrap_servers: Optional[str] = None,
        producer: Optional[Any] = None,
    ):
        """
        Create a Kafka health check function.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            producer: Existing AIOKafkaProducer
        """
        servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )

        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                if producer is not None:
                    # Check producer is connected
                    if producer._sender is None:
                        raise RuntimeError("Producer not started")
                    # Get cluster metadata
                    metadata = await producer.client.fetch_all_metadata()
                    broker_count = len(metadata.brokers())
                else:
                    # Create temporary admin client
                    from aiokafka.admin import AIOKafkaAdminClient
                    admin = AIOKafkaAdminClient(
                        bootstrap_servers=servers,
                        request_timeout_ms=5000,
                    )
                    try:
                        await admin.start()
                        cluster = admin._client.cluster
                        broker_count = len(cluster.brokers())
                    finally:
                        await admin.close()

                latency = (time.time() - start) * 1000
                return DependencyHealth(
                    name="kafka",
                    healthy=True,
                    latency_ms=latency,
                    message="Connected",
                    details={
                        "brokers": broker_count,
                        "bootstrap_servers": servers,
                    },
                )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"Kafka health check failed: {e}")
                return DependencyHealth(
                    name="kafka",
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                )

        return _check

    def check_http(
        self,
        name: str,
        url: str,
        expected_status: int = 200,
        timeout: float = 5.0,
    ):
        """
        Create an HTTP endpoint health check function.

        Args:
            name: Name for this dependency
            url: URL to check
            expected_status: Expected HTTP status code
            timeout: Request timeout in seconds
        """
        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                import httpx
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url)
                    healthy = response.status_code == expected_status

                    latency = (time.time() - start) * 1000
                    return DependencyHealth(
                        name=name,
                        healthy=healthy,
                        latency_ms=latency,
                        message=f"Status: {response.status_code}",
                        details={
                            "url": url,
                            "status_code": response.status_code,
                            "expected_status": expected_status,
                        },
                    )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"HTTP health check failed for {name}: {e}")
                return DependencyHealth(
                    name=name,
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                    details={"url": url},
                )

        return _check

    def check_s3(
        self,
        bucket_name: str,
        region: Optional[str] = None,
    ):
        """
        Create an S3 bucket health check function.

        Args:
            bucket_name: S3 bucket name
            region: AWS region
        """
        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                import boto3
                s3 = boto3.client("s3", region_name=region)

                # Run in executor since boto3 is sync
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: s3.head_bucket(Bucket=bucket_name)
                )

                latency = (time.time() - start) * 1000
                return DependencyHealth(
                    name=f"s3:{bucket_name}",
                    healthy=True,
                    latency_ms=latency,
                    message="Bucket accessible",
                    details={"bucket": bucket_name, "region": region},
                )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"S3 health check failed for {bucket_name}: {e}")
                return DependencyHealth(
                    name=f"s3:{bucket_name}",
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                )

        return _check

    def check_secrets_manager(
        self,
        secret_name: str,
        region: Optional[str] = None,
    ):
        """
        Create an AWS Secrets Manager health check function.

        Args:
            secret_name: Secret name to check access to
            region: AWS region
        """
        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                import boto3
                client = boto3.client("secretsmanager", region_name=region)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: client.describe_secret(SecretId=secret_name)
                )

                latency = (time.time() - start) * 1000
                return DependencyHealth(
                    name="secrets_manager",
                    healthy=True,
                    latency_ms=latency,
                    message="Access verified",
                    details={"secret_name": secret_name},
                )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"Secrets Manager health check failed: {e}")
                return DependencyHealth(
                    name="secrets_manager",
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                )

        return _check

    def check_msk(
        self,
        cluster_arn: str,
        region: Optional[str] = None,
    ):
        """
        Create an AWS MSK (Managed Kafka) health check function.

        Args:
            cluster_arn: MSK cluster ARN
            region: AWS region
        """
        async def _check() -> DependencyHealth:
            start = time.time()
            try:
                import boto3
                client = boto3.client("kafka", region_name=region)

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: client.describe_cluster(ClusterArn=cluster_arn)
                )

                cluster_info = response.get("ClusterInfo", {})
                state = cluster_info.get("State", "UNKNOWN")
                healthy = state == "ACTIVE"

                latency = (time.time() - start) * 1000
                return DependencyHealth(
                    name="msk",
                    healthy=healthy,
                    latency_ms=latency,
                    message=f"Cluster state: {state}",
                    details={
                        "cluster_arn": cluster_arn,
                        "state": state,
                        "broker_count": cluster_info.get("NumberOfBrokerNodes", 0),
                    },
                )
            except Exception as e:
                latency = (time.time() - start) * 1000
                logger.error(f"MSK health check failed: {e}")
                return DependencyHealth(
                    name="msk",
                    healthy=False,
                    latency_ms=latency,
                    message=str(e),
                )

        return _check


# Singleton instance
_dependency_checker: Optional[DependencyChecker] = None


def get_dependency_checker() -> DependencyChecker:
    """Get or create the dependency checker singleton"""
    global _dependency_checker
    if _dependency_checker is None:
        _dependency_checker = DependencyChecker()
    return _dependency_checker

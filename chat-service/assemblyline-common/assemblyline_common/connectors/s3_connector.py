"""
Enterprise AWS S3 Connector.

Features:
- Connection pooling with region-aware routing
- IAM role assumption with session tokens
- KMS encryption (SSE-KMS, SSE-S3, SSE-C)
- Multipart upload for large files
- Automatic retry on throttling
- Checksum validation (CRC32C, SHA256)
- Transfer acceleration support
- Parallel downloads with range requests
- VPC endpoint support

Usage:
    from assemblyline_common.connectors import get_s3_connector, S3ConnectorConfig

    connector = await get_s3_connector(
        config=S3ConnectorConfig(
            region="us-east-1",
            enable_kms=True,
            kms_key_id="alias/my-key",
        )
    )

    # Upload file
    await connector.upload_file("my-bucket", "key.txt", data)

    # Download file
    data = await connector.download_file("my-bucket", "key.txt")

    # Multipart upload for large files
    async with connector.multipart_upload("my-bucket", "large-file.zip") as upload:
        for chunk in chunks:
            await upload.upload_part(chunk)
"""

import asyncio
import base64
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, List, AsyncIterator, BinaryIO, Union
from io import BytesIO

import aioboto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
)
from assemblyline_common.retry import RetryHandler, RetryConfig

logger = logging.getLogger(__name__)


class EncryptionType(Enum):
    """S3 server-side encryption types."""
    NONE = "none"
    SSE_S3 = "AES256"  # S3-managed keys
    SSE_KMS = "aws:kms"  # KMS-managed keys
    SSE_C = "customer"  # Customer-provided keys


@dataclass
class S3ConnectorConfig:
    """Configuration for S3 connector."""
    # AWS settings
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # For VPC endpoints or LocalStack

    # Credentials (if not using IAM role)
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None

    # IAM role assumption
    role_arn: Optional[str] = None
    role_session_name: str = "logic-weaver-s3"
    role_duration_seconds: int = 3600

    # Encryption
    encryption_type: EncryptionType = EncryptionType.SSE_S3
    kms_key_id: Optional[str] = None  # For SSE-KMS
    customer_key: Optional[bytes] = None  # For SSE-C (32 bytes)

    # Transfer settings
    multipart_threshold: int = 8 * 1024 * 1024  # 8MB
    multipart_chunksize: int = 8 * 1024 * 1024  # 8MB
    max_concurrency: int = 10
    use_transfer_acceleration: bool = False

    # Retry and circuit breaker
    enable_retry: bool = True
    max_retries: int = 3
    enable_circuit_breaker: bool = True

    # Checksum validation
    enable_checksum: bool = True
    checksum_algorithm: str = "SHA256"  # SHA256, CRC32C

    # Connection settings
    connect_timeout: int = 10
    read_timeout: int = 60
    max_pool_connections: int = 50

    # Tenant ID
    tenant_id: Optional[str] = None


@dataclass
class S3Object:
    """S3 object metadata."""
    bucket: str
    key: str
    size: int
    etag: str
    last_modified: Any
    content_type: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    encryption: Optional[str] = None
    kms_key_id: Optional[str] = None


class MultipartUpload:
    """Context manager for multipart uploads."""

    def __init__(
        self,
        connector: "S3Connector",
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        storage_class: Optional[str] = None,
    ):
        self.connector = connector
        self.bucket = bucket
        self.key = key
        self.content_type = content_type
        self.metadata = metadata or {}
        self.storage_class = storage_class
        self._upload_id: Optional[str] = None
        self._parts: List[Dict[str, Any]] = []
        self._part_number = 0

    async def __aenter__(self) -> "MultipartUpload":
        """Start multipart upload."""
        extra_args = self.connector._get_encryption_args()
        extra_args["ContentType"] = self.content_type
        if self.metadata:
            extra_args["Metadata"] = self.metadata
        if self.storage_class and self.storage_class != "STANDARD":
            extra_args["StorageClass"] = self.storage_class

        async with self.connector._get_client() as client:
            response = await client.create_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                **extra_args,
            )
            self._upload_id = response["UploadId"]

        logger.info(
            f"Started multipart upload",
            extra={
                "event_type": "s3_multipart_started",
                "bucket": self.bucket,
                "key": self.key,
                "upload_id": self._upload_id,
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Complete or abort multipart upload."""
        if exc_type is not None:
            # Abort on error
            await self.abort()
        else:
            # Complete upload
            await self.complete()

    async def upload_part(self, data: bytes) -> Dict[str, Any]:
        """Upload a part."""
        self._part_number += 1

        async with self.connector._get_client() as client:
            response = await client.upload_part(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self._upload_id,
                PartNumber=self._part_number,
                Body=data,
            )

        part_info = {
            "PartNumber": self._part_number,
            "ETag": response["ETag"],
        }
        self._parts.append(part_info)

        logger.debug(
            f"Uploaded part {self._part_number}",
            extra={
                "event_type": "s3_part_uploaded",
                "bucket": self.bucket,
                "key": self.key,
                "part_number": self._part_number,
                "size": len(data),
            }
        )
        return part_info

    async def complete(self) -> Dict[str, Any]:
        """Complete the multipart upload."""
        async with self.connector._get_client() as client:
            response = await client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self._upload_id,
                MultipartUpload={"Parts": self._parts},
            )

        logger.info(
            f"Completed multipart upload",
            extra={
                "event_type": "s3_multipart_completed",
                "bucket": self.bucket,
                "key": self.key,
                "parts": len(self._parts),
                "etag": response.get("ETag"),
            }
        )
        return response

    async def abort(self) -> None:
        """Abort the multipart upload."""
        if not self._upload_id:
            return

        try:
            async with self.connector._get_client() as client:
                await client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self._upload_id,
                )

            logger.warning(
                f"Aborted multipart upload",
                extra={
                    "event_type": "s3_multipart_aborted",
                    "bucket": self.bucket,
                    "key": self.key,
                    "upload_id": self._upload_id,
                }
            )
        except Exception as e:
            logger.error(f"Failed to abort multipart upload: {e}")


class S3Connector:
    """
    Enterprise S3 connector with resilience patterns.

    Features:
    - IAM role assumption
    - KMS encryption
    - Multipart upload/download
    - Circuit breaker integration
    - Checksum validation
    """

    def __init__(
        self,
        config: S3ConnectorConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._session: Optional[aioboto3.Session] = None
        self._assumed_credentials: Optional[Dict[str, Any]] = None
        self._credentials_expire_at: float = 0
        self._retry_handler: Optional[RetryHandler] = None
        self._metrics: Dict[str, int] = {
            "uploads": 0,
            "downloads": 0,
            "bytes_uploaded": 0,
            "bytes_downloaded": 0,
            "errors": 0,
        }
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        self._session = aioboto3.Session(
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            aws_session_token=self.config.session_token,
            region_name=self.config.region,
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        if self.config.enable_retry:
            self._retry_handler = RetryHandler(
                config=RetryConfig(
                    max_attempts=self.config.max_retries,
                    base_delay=1.0,
                    max_delay=30.0,
                    retryable_exceptions={ClientError},
                )
            )

        # Assume IAM role if configured
        if self.config.role_arn:
            await self._assume_role()

        logger.info(
            "S3 connector initialized",
            extra={
                "event_type": "s3_connector_initialized",
                "region": self.config.region,
                "encryption": self.config.encryption_type.value,
                "transfer_acceleration": self.config.use_transfer_acceleration,
                "role_arn": self.config.role_arn or "none",
            }
        )

    async def _assume_role(self) -> Dict[str, Any]:
        """Assume IAM role and get temporary credentials."""
        if not self.config.role_arn:
            return {}

        # Check if credentials are still valid
        if self._assumed_credentials and time.time() < self._credentials_expire_at - 300:
            return self._assumed_credentials

        async with self._session.client("sts") as sts:
            response = await sts.assume_role(
                RoleArn=self.config.role_arn,
                RoleSessionName=self.config.role_session_name,
                DurationSeconds=self.config.role_duration_seconds,
            )

        creds = response["Credentials"]
        self._assumed_credentials = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
        self._credentials_expire_at = creds["Expiration"].timestamp()

        logger.info(
            "Assumed IAM role",
            extra={
                "event_type": "s3_role_assumed",
                "role_arn": self.config.role_arn,
                "expires_at": self._credentials_expire_at,
            }
        )

        return self._assumed_credentials

    def _get_client(self):
        """Get S3 client context manager.

        If role_arn is configured, creates a new session with assumed role
        credentials. Otherwise uses the session initialized in initialize().
        """
        boto_config = BotoConfig(
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            max_pool_connections=self.config.max_pool_connections,
            retries={"max_attempts": 0},  # We handle retries ourselves
            s3={
                "use_accelerate_endpoint": self.config.use_transfer_acceleration,
            }
        )

        kwargs = {
            "config": boto_config,
            "region_name": self.config.region,
        }

        if self.config.endpoint_url:
            kwargs["endpoint_url"] = self.config.endpoint_url

        # Use assumed role credentials if available
        if self._assumed_credentials:
            session = aioboto3.Session(
                aws_access_key_id=self._assumed_credentials["aws_access_key_id"],
                aws_secret_access_key=self._assumed_credentials["aws_secret_access_key"],
                aws_session_token=self._assumed_credentials["aws_session_token"],
                region_name=self.config.region,
            )
            return session.client("s3", **kwargs)

        return self._session.client("s3", **kwargs)

    def _get_encryption_args(self) -> Dict[str, Any]:
        """Get encryption arguments for S3 operations."""
        args = {}

        if self.config.encryption_type == EncryptionType.SSE_S3:
            args["ServerSideEncryption"] = "AES256"

        elif self.config.encryption_type == EncryptionType.SSE_KMS:
            args["ServerSideEncryption"] = "aws:kms"
            if self.config.kms_key_id:
                args["SSEKMSKeyId"] = self.config.kms_key_id

        elif self.config.encryption_type == EncryptionType.SSE_C:
            if self.config.customer_key:
                key_b64 = base64.b64encode(self.config.customer_key).decode()
                key_md5 = base64.b64encode(
                    hashlib.md5(self.config.customer_key).digest()
                ).decode()
                args["SSECustomerAlgorithm"] = "AES256"
                args["SSECustomerKey"] = key_b64
                args["SSECustomerKeyMD5"] = key_md5

        return args

    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate checksum for data."""
        if self.config.checksum_algorithm == "SHA256":
            return base64.b64encode(hashlib.sha256(data).digest()).decode()
        elif self.config.checksum_algorithm == "CRC32C":
            import crc32c
            return base64.b64encode(crc32c.crc32c(data).to_bytes(4, 'big')).decode()
        return ""

    async def upload_file(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, BinaryIO],
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        storage_class: Optional[str] = None,
    ) -> S3Object:
        """
        Upload a file to S3.

        Uses multipart upload for files larger than multipart_threshold.
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._session:
            await self.initialize()

        # Get data as bytes
        if hasattr(data, 'read'):
            data = data.read()

        # Use multipart for large files
        if len(data) > self.config.multipart_threshold:
            return await self._multipart_upload(bucket, key, data, content_type, metadata, storage_class)

        # Check circuit breaker
        circuit_name = f"s3:{bucket}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        extra_args = self._get_encryption_args()
        extra_args["ContentType"] = content_type
        if metadata:
            extra_args["Metadata"] = metadata
        if storage_class and storage_class != "STANDARD":
            extra_args["StorageClass"] = storage_class

        # Add checksum
        if self.config.enable_checksum:
            checksum = self._calculate_checksum(data)
            if self.config.checksum_algorithm == "SHA256":
                extra_args["ChecksumSHA256"] = checksum

        async def do_upload() -> S3Object:
            async with self._get_client() as client:
                response = await client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data,
                    **extra_args,
                )

            return S3Object(
                bucket=bucket,
                key=key,
                size=len(data),
                etag=response.get("ETag", "").strip('"'),
                last_modified=None,
                content_type=content_type,
                metadata=metadata or {},
                encryption=response.get("ServerSideEncryption"),
                kms_key_id=response.get("SSEKMSKeyId"),
            )

        try:
            if self._retry_handler:
                result = await self._retry_handler.execute(
                    do_upload,
                    operation_id=f"s3-upload-{bucket}/{key}",
                )
            else:
                result = await do_upload()

            # Update metrics
            self._metrics["uploads"] += 1
            self._metrics["bytes_uploaded"] += len(data)

            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            logger.info(
                f"Uploaded file to S3",
                extra={
                    "event_type": "s3_upload_completed",
                    "bucket": bucket,
                    "key": key,
                    "size": len(data),
                    "etag": result.etag,
                }
            )

            return result

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    async def _multipart_upload(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str,
        metadata: Optional[Dict[str, str]],
        storage_class: Optional[str] = None,
    ) -> S3Object:
        """Perform multipart upload for large files."""
        async with self.multipart_upload(bucket, key, content_type, metadata, storage_class) as upload:
            offset = 0
            while offset < len(data):
                chunk = data[offset:offset + self.config.multipart_chunksize]
                await upload.upload_part(chunk)
                offset += self.config.multipart_chunksize

        return S3Object(
            bucket=bucket,
            key=key,
            size=len(data),
            etag="",  # Will be composite ETag
            last_modified=None,
            content_type=content_type,
            metadata=metadata or {},
        )

    def multipart_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        storage_class: Optional[str] = None,
    ) -> MultipartUpload:
        """Get a multipart upload context manager."""
        return MultipartUpload(self, bucket, key, content_type, metadata, storage_class)

    async def download_file(
        self,
        bucket: str,
        key: str,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
    ) -> bytes:
        """
        Download a file from S3.

        Supports range requests for partial downloads.
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._session:
            await self.initialize()

        circuit_name = f"s3:{bucket}"
        if self._circuit_breaker:
            if not await self._circuit_breaker.can_execute(circuit_name):
                raise CircuitOpenError(circuit_name, 0)

        extra_args = {}

        # Add range header for partial downloads
        if range_start is not None or range_end is not None:
            range_str = f"bytes={range_start or 0}-{range_end or ''}"
            extra_args["Range"] = range_str

        # Add SSE-C args if needed
        if self.config.encryption_type == EncryptionType.SSE_C and self.config.customer_key:
            key_b64 = base64.b64encode(self.config.customer_key).decode()
            key_md5 = base64.b64encode(
                hashlib.md5(self.config.customer_key).digest()
            ).decode()
            extra_args["SSECustomerAlgorithm"] = "AES256"
            extra_args["SSECustomerKey"] = key_b64
            extra_args["SSECustomerKeyMD5"] = key_md5

        async def do_download() -> bytes:
            async with self._get_client() as client:
                response = await client.get_object(
                    Bucket=bucket,
                    Key=key,
                    **extra_args,
                )
                return await response["Body"].read()

        try:
            if self._retry_handler:
                data = await self._retry_handler.execute(
                    do_download,
                    operation_id=f"s3-download-{bucket}/{key}",
                )
            else:
                data = await do_download()

            # Update metrics
            self._metrics["downloads"] += 1
            self._metrics["bytes_downloaded"] += len(data)

            if self._circuit_breaker:
                await self._circuit_breaker.record_success(circuit_name)

            logger.debug(
                f"Downloaded file from S3",
                extra={
                    "event_type": "s3_download_completed",
                    "bucket": bucket,
                    "key": key,
                    "size": len(data),
                }
            )

            return data

        except Exception as e:
            self._metrics["errors"] += 1
            if self._circuit_breaker:
                await self._circuit_breaker.record_failure(circuit_name, e)
            raise

    async def delete_file(self, bucket: str, key: str) -> None:
        """Delete a file from S3."""
        if not self._session:
            await self.initialize()

        async with self._get_client() as client:
            await client.delete_object(Bucket=bucket, Key=key)

        logger.info(
            f"Deleted file from S3",
            extra={
                "event_type": "s3_delete_completed",
                "bucket": bucket,
                "key": key,
            }
        )

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> AsyncIterator[S3Object]:
        """List objects in a bucket with pagination."""
        if not self._session:
            await self.initialize()

        continuation_token = None

        while True:
            kwargs = {
                "Bucket": bucket,
                "Prefix": prefix,
                "MaxKeys": max_keys,
            }
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            async with self._get_client() as client:
                response = await client.list_objects_v2(**kwargs)

            for obj in response.get("Contents", []):
                yield S3Object(
                    bucket=bucket,
                    key=obj["Key"],
                    size=obj["Size"],
                    etag=obj["ETag"].strip('"'),
                    last_modified=obj["LastModified"],
                )

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

    async def get_presigned_url(
        self,
        bucket: str,
        key: str,
        operation: str = "get_object",
        expires_in: int = 3600,
    ) -> str:
        """Generate a presigned URL."""
        if not self._session:
            await self.initialize()

        async with self._get_client() as client:
            url = await client.generate_presigned_url(
                operation,
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires_in,
            )

        return url

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return {
            **self._metrics,
            "config": {
                "region": self.config.region,
                "encryption": self.config.encryption_type.value,
            }
        }

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True
        self._session = None
        logger.info("S3 connector closed")


# Singleton instances
_s3_connectors: Dict[str, S3Connector] = {}
_s3_lock = asyncio.Lock()


async def get_s3_connector(
    config: Optional[S3ConnectorConfig] = None,
    name: Optional[str] = None,
) -> S3Connector:
    """Get or create an S3 connector."""
    config = config or S3ConnectorConfig()
    connector_name = name or f"s3-{config.region}"

    if connector_name in _s3_connectors:
        return _s3_connectors[connector_name]

    async with _s3_lock:
        if connector_name in _s3_connectors:
            return _s3_connectors[connector_name]

        connector = S3Connector(config)
        await connector.initialize()
        _s3_connectors[connector_name] = connector

        return connector


async def close_all_s3_connectors() -> None:
    """Close all S3 connectors."""
    for connector in _s3_connectors.values():
        await connector.close()
    _s3_connectors.clear()

"""
Retry module for Logic Weaver.

Provides unified retry handling with exponential backoff and jitter,
configurable per connector type, and dead letter queue integration.
"""

from assemblyline_common.retry.handler import (
    RetryConfig,
    RetryContext,
    RetryHandler,
    RetryExhaustedError,
    retry,
    get_retry_handler,
    HTTP_RETRY_CONFIG,
    MLLP_RETRY_CONFIG,
    KAFKA_RETRY_CONFIG,
    DATABASE_RETRY_CONFIG,
)

from assemblyline_common.retry.idempotency import (
    IdempotencyGuard,
    IdempotencyConfig,
    get_idempotency_guard,
)

__all__ = [
    "RetryConfig",
    "RetryContext",
    "RetryHandler",
    "RetryExhaustedError",
    "retry",
    "get_retry_handler",
    "HTTP_RETRY_CONFIG",
    "MLLP_RETRY_CONFIG",
    "KAFKA_RETRY_CONFIG",
    "DATABASE_RETRY_CONFIG",
    "IdempotencyGuard",
    "IdempotencyConfig",
    "get_idempotency_guard",
]

"""
Unified Retry Handler with exponential backoff and jitter.

Enterprise Features:
- Exponential backoff with configurable base and max delay
- Jitter to prevent thundering herd
- Configurable max attempts per connector type
- Dead letter queue integration for exhausted retries
- Retry-After header support
- Idempotency tracking

Usage:
    from assemblyline_common.retry import retry, RetryConfig

    @retry(RetryConfig(max_attempts=3))
    async def call_api():
        return await http_client.get(url)

    # Or with context manager
    handler = RetryHandler(RetryConfig())
    async for attempt in handler.attempts():
        try:
            result = await call_api()
            break
        except RetryableError as e:
            if not await handler.should_retry(e):
                raise
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import (
    Optional, Set, Type, Callable, TypeVar, Awaitable, Any,
    AsyncIterator, Dict, Tuple, List
)

import redis.asyncio as redis

from assemblyline_common.config import settings

logger = logging.getLogger(__name__)

T = TypeVar('T')


class RetryExhaustedError(Exception):
    """Raised when all retry attempts have been exhausted."""
    def __init__(
        self,
        message: str,
        attempts: int,
        last_error: Optional[Exception] = None,
        context: Optional["RetryContext"] = None
    ):
        self.attempts = attempts
        self.last_error = last_error
        self.context = context
        super().__init__(message)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    # Maximum number of retry attempts (including initial attempt)
    max_attempts: int = 3
    # Base delay in seconds for exponential backoff
    base_delay: float = 1.0
    # Maximum delay in seconds
    max_delay: float = 60.0
    # Exponential multiplier (delay = base_delay * multiplier^attempt)
    multiplier: float = 2.0
    # Jitter factor (0.0 to 1.0) - randomness added to delay
    jitter: float = 0.1
    # Exception types that should trigger a retry
    retryable_exceptions: Set[Type[Exception]] = field(
        default_factory=lambda: {
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
        }
    )
    # HTTP status codes that should trigger a retry
    retryable_status_codes: Set[int] = field(
        default_factory=lambda: {408, 429, 500, 502, 503, 504}
    )
    # Whether to respect Retry-After header
    respect_retry_after: bool = True
    # Maximum time to wait from Retry-After header
    max_retry_after: float = 300.0
    # Enable dead letter queue for exhausted retries
    enable_dlq: bool = False
    # Dead letter queue topic/key prefix
    dlq_prefix: str = "dlq"
    # Whether to track idempotency
    track_idempotency: bool = False
    # Idempotency key TTL in seconds
    idempotency_ttl: int = 86400  # 24 hours


@dataclass
class RetryContext:
    """Context information for a retry operation."""
    operation_id: str
    attempt: int = 0
    total_attempts: int = 0
    first_attempt_at: float = field(default_factory=time.time)
    last_attempt_at: Optional[float] = None
    last_error: Optional[Exception] = None
    last_delay: float = 0.0
    total_delay: float = 0.0
    idempotency_key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def record_attempt(self, error: Optional[Exception] = None, delay: float = 0.0) -> None:
        """Record an attempt."""
        self.attempt += 1
        self.total_attempts += 1
        self.last_attempt_at = time.time()
        self.last_error = error
        self.last_delay = delay
        self.total_delay += delay


class RetryHandler:
    """
    Unified retry handler with exponential backoff.

    Supports:
    - Exponential backoff with jitter
    - Retry-After header handling
    - Dead letter queue for failed messages
    - Idempotency tracking
    """

    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        redis_client: Optional[redis.Redis] = None,
    ):
        self.config = config or RetryConfig()
        self.redis = redis_client
        self._context: Optional[RetryContext] = None

    def calculate_delay(self, attempt: int, retry_after: Optional[float] = None) -> float:
        """
        Calculate delay for the next retry attempt.

        Uses exponential backoff with jitter:
        delay = min(base_delay * multiplier^attempt, max_delay) * (1 + random(0, jitter))
        """
        # Calculate base exponential delay
        delay = self.config.base_delay * (self.config.multiplier ** attempt)

        # Cap at maximum delay
        delay = min(delay, self.config.max_delay)

        # Add jitter
        if self.config.jitter > 0:
            jitter_amount = delay * self.config.jitter * random.random()
            delay += jitter_amount

        # Respect Retry-After header if provided
        if retry_after and self.config.respect_retry_after:
            retry_after = min(retry_after, self.config.max_retry_after)
            delay = max(delay, retry_after)

        return delay

    def is_retryable(self, error: Exception) -> bool:
        """Check if an exception should trigger a retry."""
        # Check if exception type is retryable
        for exc_type in self.config.retryable_exceptions:
            if isinstance(error, exc_type):
                return True

        # Check for HTTP status code in exception
        status_code = getattr(error, 'status_code', None) or getattr(error, 'status', None)
        if status_code and status_code in self.config.retryable_status_codes:
            return True

        return False

    def get_retry_after(self, error: Exception) -> Optional[float]:
        """Extract Retry-After value from exception or response."""
        # Try to get from exception attributes
        retry_after = getattr(error, 'retry_after', None)
        if retry_after:
            return float(retry_after)

        # Try to get from response headers
        response = getattr(error, 'response', None)
        if response:
            headers = getattr(response, 'headers', {})
            retry_after_header = headers.get('Retry-After')
            if retry_after_header:
                try:
                    return float(retry_after_header)
                except ValueError:
                    # Could be a date string, skip for now
                    pass

        return None

    async def should_retry(
        self,
        error: Exception,
        context: Optional[RetryContext] = None
    ) -> Tuple[bool, float]:
        """
        Determine if operation should be retried.

        Returns:
            Tuple of (should_retry, delay_seconds)
        """
        ctx = context or self._context

        if not ctx:
            return False, 0.0

        # Check if max attempts reached
        if ctx.attempt >= self.config.max_attempts:
            return False, 0.0

        # Check if error is retryable
        if not self.is_retryable(error):
            return False, 0.0

        # Calculate delay
        retry_after = self.get_retry_after(error)
        delay = self.calculate_delay(ctx.attempt, retry_after)

        return True, delay

    async def attempts(
        self,
        operation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> AsyncIterator[RetryContext]:
        """
        Async iterator for retry attempts.

        Usage:
            async for attempt in handler.attempts("my-operation"):
                try:
                    result = await do_something()
                    break
                except Exception as e:
                    should_retry, delay = await handler.should_retry(e, attempt)
                    if not should_retry:
                        raise
                    await asyncio.sleep(delay)
        """
        ctx = RetryContext(
            operation_id=operation_id or f"op-{time.time()}",
            idempotency_key=idempotency_key,
        )
        self._context = ctx

        # Check idempotency if enabled
        if idempotency_key and self.config.track_idempotency and self.redis:
            cached = await self._get_cached_result(idempotency_key)
            if cached is not None:
                logger.info(
                    f"Returning cached result for idempotent operation",
                    extra={
                        "event_type": "retry_idempotent_hit",
                        "operation_id": ctx.operation_id,
                        "idempotency_key": idempotency_key,
                    }
                )
                return

        for attempt in range(self.config.max_attempts):
            ctx.attempt = attempt
            yield ctx

    async def _get_cached_result(self, idempotency_key: str) -> Optional[Any]:
        """Get cached result for idempotent operation."""
        if not self.redis:
            return None

        try:
            key = f"idempotency:{idempotency_key}"
            result = await self.redis.get(key)
            return result
        except Exception as e:
            logger.warning(f"Error checking idempotency cache: {e}")
            return None

    async def cache_result(
        self,
        idempotency_key: str,
        result: Any,
    ) -> None:
        """Cache result for idempotent operation."""
        if not self.redis or not self.config.track_idempotency:
            return

        try:
            key = f"idempotency:{idempotency_key}"
            await self.redis.set(
                key,
                str(result),
                ex=self.config.idempotency_ttl
            )
        except Exception as e:
            logger.warning(f"Error caching idempotency result: {e}")

    async def send_to_dlq(
        self,
        context: RetryContext,
        error: Exception,
        payload: Any,
    ) -> bool:
        """
        Send failed message to dead letter queue.

        Returns True if successfully sent to DLQ.
        """
        if not self.config.enable_dlq:
            return False

        if not self.redis:
            logger.warning("DLQ enabled but Redis not available")
            return False

        try:
            dlq_key = f"{self.config.dlq_prefix}:{context.operation_id}"

            dlq_entry = {
                "operation_id": context.operation_id,
                "payload": str(payload),
                "error": str(error),
                "error_type": type(error).__name__,
                "attempts": context.total_attempts,
                "first_attempt_at": context.first_attempt_at,
                "last_attempt_at": context.last_attempt_at,
                "total_delay": context.total_delay,
                "metadata": context.metadata,
                "timestamp": time.time(),
            }

            await self.redis.lpush(dlq_key, str(dlq_entry))
            await self.redis.expire(dlq_key, 86400 * 7)  # 7 day retention

            logger.warning(
                f"Message sent to dead letter queue",
                extra={
                    "event_type": "retry_dlq_sent",
                    "operation_id": context.operation_id,
                    "attempts": context.total_attempts,
                    "error_type": type(error).__name__,
                }
            )
            return True

        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
            return False

    async def execute(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        operation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        dlq_payload: Optional[Any] = None,
        **kwargs: Any,
    ) -> T:
        """
        Execute a function with retry logic.

        Args:
            func: Async function to execute
            *args: Arguments for the function
            operation_id: Unique operation identifier
            idempotency_key: Key for idempotency tracking
            dlq_payload: Payload to send to DLQ if all retries exhausted
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function

        Raises:
            RetryExhaustedError: If all retries are exhausted
        """
        ctx = RetryContext(
            operation_id=operation_id or f"op-{time.time()}",
            idempotency_key=idempotency_key,
        )

        last_error: Optional[Exception] = None

        for attempt in range(self.config.max_attempts):
            try:
                ctx.record_attempt()

                logger.debug(
                    f"Retry attempt {attempt + 1}/{self.config.max_attempts}",
                    extra={
                        "event_type": "retry_attempt",
                        "operation_id": ctx.operation_id,
                        "attempt": attempt + 1,
                        "max_attempts": self.config.max_attempts,
                    }
                )

                result = await func(*args, **kwargs)

                # Cache result if idempotent
                if idempotency_key:
                    await self.cache_result(idempotency_key, result)

                if attempt > 0:
                    logger.info(
                        f"Retry succeeded after {attempt + 1} attempts",
                        extra={
                            "event_type": "retry_succeeded",
                            "operation_id": ctx.operation_id,
                            "attempts": attempt + 1,
                            "total_delay": ctx.total_delay,
                        }
                    )

                return result

            except Exception as e:
                last_error = e
                ctx.record_attempt(error=e)

                should_retry, delay = await self.should_retry(e, ctx)

                if not should_retry:
                    logger.warning(
                        f"Non-retryable error or max attempts reached",
                        extra={
                            "event_type": "retry_exhausted",
                            "operation_id": ctx.operation_id,
                            "attempts": ctx.total_attempts,
                            "error_type": type(e).__name__,
                            "error": str(e),
                        }
                    )

                    # Send to DLQ if configured
                    if dlq_payload:
                        await self.send_to_dlq(ctx, e, dlq_payload)

                    raise RetryExhaustedError(
                        f"All {ctx.total_attempts} retry attempts exhausted",
                        attempts=ctx.total_attempts,
                        last_error=e,
                        context=ctx,
                    )

                ctx.record_attempt(delay=delay)

                logger.warning(
                    f"Retrying after {delay:.2f}s",
                    extra={
                        "event_type": "retry_scheduled",
                        "operation_id": ctx.operation_id,
                        "attempt": attempt + 1,
                        "delay": delay,
                        "error_type": type(e).__name__,
                    }
                )

                await asyncio.sleep(delay)

        # Should not reach here, but handle edge case
        raise RetryExhaustedError(
            f"All {ctx.total_attempts} retry attempts exhausted",
            attempts=ctx.total_attempts,
            last_error=last_error,
            context=ctx,
        )


def retry(
    config: Optional[RetryConfig] = None,
    operation_id: Optional[str] = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """
    Decorator for adding retry logic to async functions.

    Usage:
        @retry(RetryConfig(max_attempts=5))
        async def call_api():
            return await http_client.get(url)
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            handler = RetryHandler(config=config)
            return await handler.execute(
                func,
                *args,
                operation_id=operation_id or func.__name__,
                **kwargs,
            )
        return wrapper
    return decorator


# Singleton handler
_retry_handler: Optional[RetryHandler] = None
_retry_handler_lock = asyncio.Lock()


async def get_retry_handler(config: Optional[RetryConfig] = None) -> RetryHandler:
    """Get singleton retry handler with Redis support."""
    global _retry_handler

    if _retry_handler is None:
        async with _retry_handler_lock:
            if _retry_handler is None:
                redis_client = None
                try:
                    redis_client = redis.from_url(
                        settings.REDIS_URL,
                        encoding="utf-8",
                        decode_responses=True,
                    )
                    await redis_client.ping()
                except Exception as e:
                    logger.warning(f"Retry handler Redis unavailable: {e}")
                    redis_client = None

                _retry_handler = RetryHandler(
                    config=config,
                    redis_client=redis_client,
                )

    return _retry_handler


# Predefined configs for common connector types
HTTP_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    retryable_status_codes={408, 429, 500, 502, 503, 504},
    respect_retry_after=True,
)

MLLP_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=60.0,
    retryable_exceptions={ConnectionError, TimeoutError, asyncio.TimeoutError},
)

KAFKA_RETRY_CONFIG = RetryConfig(
    max_attempts=10,
    base_delay=0.5,
    max_delay=30.0,
    enable_dlq=True,
)

DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=10.0,
    multiplier=1.5,
)

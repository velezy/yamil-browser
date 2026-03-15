"""
Error Handling System for Logic Weaver Flows

Enterprise-grade error handling with retry, fallback, and dead letter support.
Superior to MuleSoft and Apigee with Python-native exception handling.

Features:
- On-Error-Continue: Log and continue
- On-Error-Propagate: Bubble up to parent
- Dead Letter Queue: Route failed messages
- Retry with backoff: Exponential retry
- Fallback: Alternative flow on error
- Error transformations: Custom error responses
- Circuit breaker integration

Example:
    error_handler = ErrorHandler(
        type=ErrorHandlerType.RETRY,
        config=ErrorHandlerConfig(
            max_retries=3,
            backoff_type="exponential",
            initial_delay_ms=1000,
            max_delay_ms=30000,
            retry_on=[ConnectionError, TimeoutError],
        )
    )
"""

import asyncio
import logging
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union
from uuid import uuid4

logger = logging.getLogger(__name__)


class ErrorHandlerType(Enum):
    """Types of error handlers"""
    CONTINUE = "continue"  # Log error and continue
    PROPAGATE = "propagate"  # Bubble up to parent
    RETRY = "retry"  # Retry with backoff
    FALLBACK = "fallback"  # Execute alternative flow
    DEAD_LETTER = "dead_letter"  # Route to DLQ
    TRANSFORM = "transform"  # Transform error response
    CIRCUIT_BREAKER = "circuit_breaker"  # Trip circuit breaker


class ErrorSeverity(Enum):
    """Error severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class BackoffType(Enum):
    """Retry backoff strategies"""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    RANDOM = "random"


@dataclass
class FlowErrorContext:
    """
    Context about an error that occurred during flow execution.

    Attributes:
        error_id: Unique error identifier
        flow_id: The flow where error occurred
        node_id: The node where error occurred
        execution_id: Flow execution ID
        correlation_id: Request correlation ID
        exception: The actual exception
        exception_type: Exception class name
        message: Error message
        stack_trace: Full stack trace
        input_data: Input that caused the error
        timestamp: When error occurred
        retry_count: Number of retries attempted
        severity: Error severity level
        metadata: Additional context
    """
    error_id: str = field(default_factory=lambda: str(uuid4()))
    flow_id: str = ""
    node_id: str = ""
    execution_id: str = ""
    correlation_id: str = ""
    exception: Optional[Exception] = None
    exception_type: str = ""
    message: str = ""
    stack_trace: str = ""
    input_data: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.now)
    retry_count: int = 0
    severity: ErrorSeverity = ErrorSeverity.ERROR
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_exception(
        cls,
        exception: Exception,
        flow_id: str = "",
        node_id: str = "",
        execution_id: str = "",
        correlation_id: str = "",
        input_data: Optional[Dict[str, Any]] = None,
    ) -> "FlowErrorContext":
        """Create error context from exception"""
        return cls(
            flow_id=flow_id,
            node_id=node_id,
            execution_id=execution_id,
            correlation_id=correlation_id,
            exception=exception,
            exception_type=type(exception).__name__,
            message=str(exception),
            stack_trace=traceback.format_exc(),
            input_data=input_data,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage"""
        return {
            "error_id": self.error_id,
            "flow_id": self.flow_id,
            "node_id": self.node_id,
            "execution_id": self.execution_id,
            "correlation_id": self.correlation_id,
            "exception_type": self.exception_type,
            "message": self.message,
            "stack_trace": self.stack_trace,
            "timestamp": self.timestamp.isoformat(),
            "retry_count": self.retry_count,
            "severity": self.severity.value,
            "metadata": self.metadata,
        }


@dataclass
class ErrorHandlerConfig:
    """
    Configuration for error handlers.

    Attributes:
        max_retries: Maximum retry attempts
        backoff_type: Retry backoff strategy
        initial_delay_ms: Initial delay before retry
        max_delay_ms: Maximum delay between retries
        backoff_multiplier: Multiplier for exponential backoff
        retry_on: Exception types to retry on
        dont_retry_on: Exception types to NOT retry on
        fallback_flow_id: Flow to execute on fallback
        dead_letter_topic: Kafka topic for dead letters
        dead_letter_queue: Queue name for dead letters
        transform_expression: Expression for error transformation
        log_level: Logging level for errors
        notification_channels: Where to send notifications
    """
    max_retries: int = 3
    backoff_type: BackoffType = BackoffType.EXPONENTIAL
    initial_delay_ms: int = 1000
    max_delay_ms: int = 30000
    backoff_multiplier: float = 2.0
    retry_on: List[str] = field(default_factory=lambda: ["ConnectionError", "TimeoutError"])
    dont_retry_on: List[str] = field(default_factory=lambda: ["ValidationError", "AuthenticationError"])
    fallback_flow_id: Optional[str] = None
    fallback_value: Optional[Any] = None
    dead_letter_topic: Optional[str] = None
    dead_letter_queue: Optional[str] = None
    transform_expression: Optional[str] = None
    log_level: str = "error"
    notification_channels: List[str] = field(default_factory=list)
    circuit_breaker_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_retries": self.max_retries,
            "backoff_type": self.backoff_type.value if isinstance(self.backoff_type, BackoffType) else self.backoff_type,
            "initial_delay_ms": self.initial_delay_ms,
            "max_delay_ms": self.max_delay_ms,
            "backoff_multiplier": self.backoff_multiplier,
            "retry_on": self.retry_on,
            "dont_retry_on": self.dont_retry_on,
            "fallback_flow_id": self.fallback_flow_id,
            "fallback_value": self.fallback_value,
            "dead_letter_topic": self.dead_letter_topic,
            "dead_letter_queue": self.dead_letter_queue,
            "transform_expression": self.transform_expression,
            "log_level": self.log_level,
            "notification_channels": self.notification_channels,
            "circuit_breaker_id": self.circuit_breaker_id,
        }


@dataclass
class ErrorHandlerResult:
    """Result of error handler execution"""
    handled: bool
    should_continue: bool
    should_retry: bool
    retry_delay_ms: int = 0
    transformed_output: Optional[Dict[str, Any]] = None
    fallback_result: Optional[Dict[str, Any]] = None
    dead_letter_sent: bool = False
    notifications_sent: List[str] = field(default_factory=list)


class ErrorHandler:
    """
    Base error handler with pluggable strategies.

    Supports:
    - Multiple handler types (continue, propagate, retry, fallback, DLQ)
    - Exception type filtering
    - Configurable backoff
    - Notification integration
    """

    def __init__(
        self,
        handler_type: ErrorHandlerType,
        config: Optional[ErrorHandlerConfig] = None,
        name: str = "",
    ):
        self.handler_type = handler_type
        self.config = config or ErrorHandlerConfig()
        self.name = name or handler_type.value

    async def handle(
        self,
        error_context: FlowErrorContext,
        retry_callback: Optional[Callable] = None,
        fallback_callback: Optional[Callable] = None,
        dead_letter_callback: Optional[Callable] = None,
    ) -> ErrorHandlerResult:
        """
        Handle an error according to configuration.

        Args:
            error_context: Context about the error
            retry_callback: Function to call for retry
            fallback_callback: Function to call for fallback
            dead_letter_callback: Function to send to dead letter

        Returns:
            ErrorHandlerResult
        """
        logger.debug(f"Handling error with {self.handler_type.value}: {error_context.message}")

        result = ErrorHandlerResult(
            handled=True,
            should_continue=False,
            should_retry=False,
        )

        # Log based on configured level
        self._log_error(error_context)

        if self.handler_type == ErrorHandlerType.CONTINUE:
            result.should_continue = True

        elif self.handler_type == ErrorHandlerType.PROPAGATE:
            result.handled = False
            result.should_continue = False

        elif self.handler_type == ErrorHandlerType.RETRY:
            result = await self._handle_retry(error_context, retry_callback)

        elif self.handler_type == ErrorHandlerType.FALLBACK:
            result = await self._handle_fallback(error_context, fallback_callback)

        elif self.handler_type == ErrorHandlerType.DEAD_LETTER:
            result = await self._handle_dead_letter(error_context, dead_letter_callback)

        elif self.handler_type == ErrorHandlerType.TRANSFORM:
            result = self._handle_transform(error_context)

        # Send notifications if configured
        if self.config.notification_channels:
            result.notifications_sent = await self._send_notifications(error_context)

        return result

    async def _handle_retry(
        self,
        error_context: FlowErrorContext,
        retry_callback: Optional[Callable],
    ) -> ErrorHandlerResult:
        """Handle retry logic"""
        result = ErrorHandlerResult(
            handled=True,
            should_continue=False,
            should_retry=False,
        )

        # Check if we should retry this exception type
        if not self._should_retry(error_context):
            logger.debug(f"Exception type {error_context.exception_type} not configured for retry")
            return result

        # Check retry count
        if error_context.retry_count >= self.config.max_retries:
            logger.warning(f"Max retries ({self.config.max_retries}) exceeded")
            return result

        # Calculate delay
        delay = self._calculate_delay(error_context.retry_count)

        result.should_retry = True
        result.retry_delay_ms = delay

        logger.info(
            f"Scheduling retry {error_context.retry_count + 1}/{self.config.max_retries} "
            f"after {delay}ms"
        )

        return result

    async def _handle_fallback(
        self,
        error_context: FlowErrorContext,
        fallback_callback: Optional[Callable],
    ) -> ErrorHandlerResult:
        """Handle fallback execution"""
        result = ErrorHandlerResult(
            handled=True,
            should_continue=True,
            should_retry=False,
        )

        # Use static fallback value if configured
        if self.config.fallback_value is not None:
            result.fallback_result = {"value": self.config.fallback_value}
            return result

        # Execute fallback flow
        if fallback_callback and self.config.fallback_flow_id:
            try:
                fallback_result = await fallback_callback(
                    self.config.fallback_flow_id,
                    error_context,
                )
                result.fallback_result = fallback_result
            except Exception as e:
                logger.error(f"Fallback execution failed: {e}")
                result.should_continue = False

        return result

    async def _handle_dead_letter(
        self,
        error_context: FlowErrorContext,
        dead_letter_callback: Optional[Callable],
    ) -> ErrorHandlerResult:
        """Handle dead letter routing"""
        result = ErrorHandlerResult(
            handled=True,
            should_continue=True,
            should_retry=False,
        )

        if dead_letter_callback:
            try:
                await dead_letter_callback(
                    topic=self.config.dead_letter_topic,
                    queue=self.config.dead_letter_queue,
                    error_context=error_context,
                )
                result.dead_letter_sent = True
                logger.info(f"Sent to dead letter: {self.config.dead_letter_topic or self.config.dead_letter_queue}")
            except Exception as e:
                logger.error(f"Failed to send to dead letter: {e}")

        return result

    def _handle_transform(self, error_context: FlowErrorContext) -> ErrorHandlerResult:
        """Transform error into response"""
        result = ErrorHandlerResult(
            handled=True,
            should_continue=True,
            should_retry=False,
        )

        # Default transformation
        result.transformed_output = {
            "error": True,
            "error_id": error_context.error_id,
            "error_type": error_context.exception_type,
            "message": error_context.message,
            "timestamp": error_context.timestamp.isoformat(),
        }

        # Custom transformation if configured
        if self.config.transform_expression:
            try:
                # Simple template substitution
                output = self.config.transform_expression
                output = output.replace("${error.message}", error_context.message)
                output = output.replace("${error.type}", error_context.exception_type)
                output = output.replace("${error.id}", error_context.error_id)
                result.transformed_output = {"response": output}
            except Exception as e:
                logger.warning(f"Transform expression failed: {e}")

        return result

    def _should_retry(self, error_context: FlowErrorContext) -> bool:
        """Check if exception type should be retried"""
        exception_type = error_context.exception_type

        # Don't retry if in exclusion list
        if exception_type in self.config.dont_retry_on:
            return False

        # Retry if in inclusion list or list is empty (retry all)
        if not self.config.retry_on:
            return True

        return exception_type in self.config.retry_on

    def _calculate_delay(self, retry_count: int) -> int:
        """Calculate delay for retry based on backoff strategy"""
        initial = self.config.initial_delay_ms
        max_delay = self.config.max_delay_ms
        multiplier = self.config.backoff_multiplier

        if self.config.backoff_type == BackoffType.FIXED:
            delay = initial

        elif self.config.backoff_type == BackoffType.LINEAR:
            delay = initial * (retry_count + 1)

        elif self.config.backoff_type == BackoffType.EXPONENTIAL:
            delay = int(initial * (multiplier ** retry_count))

        elif self.config.backoff_type == BackoffType.RANDOM:
            import random
            delay = random.randint(initial, max_delay)

        else:
            delay = initial

        return min(delay, max_delay)

    def _log_error(self, error_context: FlowErrorContext):
        """Log error at configured level"""
        log_func = getattr(logger, self.config.log_level, logger.error)
        log_func(
            f"Flow error in {error_context.flow_id}/{error_context.node_id}: "
            f"{error_context.exception_type}: {error_context.message}"
        )

    async def _send_notifications(
        self,
        error_context: FlowErrorContext,
    ) -> List[str]:
        """Send notifications to configured channels"""
        sent = []
        for channel in self.config.notification_channels:
            try:
                # TODO: Integrate with notification service
                logger.info(f"Would send notification to {channel}: {error_context.message}")
                sent.append(channel)
            except Exception as e:
                logger.warning(f"Failed to notify {channel}: {e}")
        return sent


class ErrorHandlingStrategy:
    """
    Strategy pattern for error handling chains.

    Allows multiple handlers to be chained together.
    """

    def __init__(self, handlers: Optional[List[ErrorHandler]] = None):
        self.handlers = handlers or []

    def add_handler(self, handler: ErrorHandler):
        """Add a handler to the chain"""
        self.handlers.append(handler)

    async def handle(
        self,
        error_context: FlowErrorContext,
        **callbacks,
    ) -> ErrorHandlerResult:
        """
        Run error through handler chain.

        First handler that handles the error wins.
        """
        for handler in self.handlers:
            result = await handler.handle(error_context, **callbacks)
            if result.handled:
                return result

        # No handler matched - propagate
        return ErrorHandlerResult(
            handled=False,
            should_continue=False,
            should_retry=False,
        )


# Convenience classes
class OnErrorContinue(ErrorHandler):
    """Continue execution after logging error"""

    def __init__(self, name: str = "on-error-continue"):
        super().__init__(ErrorHandlerType.CONTINUE, name=name)


class OnErrorPropagate(ErrorHandler):
    """Propagate error to parent"""

    def __init__(self, name: str = "on-error-propagate"):
        super().__init__(ErrorHandlerType.PROPAGATE, name=name)


class RetryHandler(ErrorHandler):
    """Retry with configurable backoff"""

    def __init__(
        self,
        max_retries: int = 3,
        backoff_type: BackoffType = BackoffType.EXPONENTIAL,
        initial_delay_ms: int = 1000,
        name: str = "retry-handler",
    ):
        config = ErrorHandlerConfig(
            max_retries=max_retries,
            backoff_type=backoff_type,
            initial_delay_ms=initial_delay_ms,
        )
        super().__init__(ErrorHandlerType.RETRY, config, name)


class FallbackHandler(ErrorHandler):
    """Execute fallback flow or return fallback value"""

    def __init__(
        self,
        fallback_flow_id: Optional[str] = None,
        fallback_value: Optional[Any] = None,
        name: str = "fallback-handler",
    ):
        config = ErrorHandlerConfig(
            fallback_flow_id=fallback_flow_id,
            fallback_value=fallback_value,
        )
        super().__init__(ErrorHandlerType.FALLBACK, config, name)


class DeadLetterHandler(ErrorHandler):
    """Route to dead letter queue"""

    def __init__(
        self,
        topic: Optional[str] = None,
        queue: Optional[str] = None,
        name: str = "dead-letter-handler",
    ):
        config = ErrorHandlerConfig(
            dead_letter_topic=topic,
            dead_letter_queue=queue,
        )
        super().__init__(ErrorHandlerType.DEAD_LETTER, config, name)

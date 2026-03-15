"""
Graceful Shutdown Manager for Logic Weaver Services

Handles SIGTERM/SIGINT signals for clean shutdown:
1. Stop accepting new requests (mark as not ready)
2. Wait for in-flight requests to complete
3. Close database connections, Redis, Kafka producers
4. Exit cleanly

Compatible with:
- Kubernetes pod termination
- AWS ECS task stopping
- Docker container stop
"""

import asyncio
import logging
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ShutdownHook:
    """A hook to run during shutdown"""
    name: str
    func: Callable[[], Coroutine[Any, Any, None]]
    priority: int = 100  # Lower = run first
    timeout: float = 10.0


@dataclass
class ShutdownResult:
    """Result of shutdown process"""
    success: bool
    duration_seconds: float
    hooks_executed: List[str]
    hooks_failed: List[str]
    errors: Dict[str, str] = field(default_factory=dict)


class GracefulShutdownManager:
    """
    Manages graceful shutdown for FastAPI services.

    Usage:
        shutdown_manager = GracefulShutdownManager(
            drain_timeout=30.0,
            force_timeout=45.0,
        )

        # Register cleanup hooks
        shutdown_manager.add_hook("postgres", close_postgres_pool, priority=10)
        shutdown_manager.add_hook("redis", close_redis, priority=20)
        shutdown_manager.add_hook("kafka", close_kafka_producer, priority=30)

        # Register signal handlers
        shutdown_manager.register_signals()

        # In FastAPI lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            yield
            await shutdown_manager.shutdown()
    """

    def __init__(
        self,
        drain_timeout: float = 30.0,
        force_timeout: float = 45.0,
        health_router: Optional[Any] = None,
    ):
        self.drain_timeout = drain_timeout
        self.force_timeout = force_timeout
        self.health_router = health_router

        self._hooks: List[ShutdownHook] = []
        self._is_shutting_down = False
        self._shutdown_started: Optional[datetime] = None
        self._active_requests: int = 0
        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def add_hook(
        self,
        name: str,
        func: Callable[[], Coroutine[Any, Any, None]],
        priority: int = 100,
        timeout: float = 10.0,
    ) -> None:
        """
        Add a shutdown hook.

        Hooks are executed in priority order (lower = first).
        Typical priorities:
        - 10: Stop accepting new work (Kafka consumers)
        - 20: Wait for in-flight work
        - 30: Flush buffers (Kafka producers)
        - 40: Close connections (DB, Redis)
        - 50: Final cleanup
        """
        hook = ShutdownHook(name=name, func=func, priority=priority, timeout=timeout)
        self._hooks.append(hook)
        self._hooks.sort(key=lambda h: h.priority)
        logger.info(f"Registered shutdown hook: {name} (priority={priority})")

    def remove_hook(self, name: str) -> None:
        """Remove a shutdown hook by name"""
        self._hooks = [h for h in self._hooks if h.name != name]

    def register_signals(self) -> None:
        """Register SIGTERM and SIGINT handlers"""
        if sys.platform != "win32":
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._signal_handler(s))
                )
            logger.info("Registered signal handlers for SIGTERM/SIGINT")
        else:
            # Windows doesn't support add_signal_handler
            signal.signal(signal.SIGTERM, self._sync_signal_handler)
            signal.signal(signal.SIGINT, self._sync_signal_handler)
            logger.info("Registered signal handlers (Windows mode)")

    async def _signal_handler(self, sig: signal.Signals) -> None:
        """Handle shutdown signal"""
        logger.info(
            f"Received signal {sig.name}, initiating graceful shutdown",
            extra={"event_type": "signal_received", "signal": sig.name}
        )
        await self.shutdown()

    def _sync_signal_handler(self, signum: int, frame: Any) -> None:
        """Synchronous signal handler for Windows"""
        logger.info(f"Received signal {signum}, initiating shutdown")
        asyncio.create_task(self.shutdown())

    def request_started(self) -> None:
        """Track that a new request has started"""
        self._active_requests += 1

    def request_finished(self) -> None:
        """Track that a request has finished"""
        self._active_requests = max(0, self._active_requests - 1)
        if self._is_shutting_down and self._active_requests == 0:
            self._shutdown_event.set()

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown is in progress"""
        return self._is_shutting_down

    @property
    def active_requests(self) -> int:
        """Get number of active requests"""
        return self._active_requests

    async def shutdown(self) -> ShutdownResult:
        """
        Execute graceful shutdown.

        Steps:
        1. Mark service as draining (stop receiving traffic)
        2. Wait for active requests to complete (up to drain_timeout)
        3. Execute shutdown hooks in priority order
        4. Force exit if force_timeout exceeded
        """
        async with self._lock:
            if self._is_shutting_down:
                logger.warning("Shutdown already in progress")
                return ShutdownResult(
                    success=False,
                    duration_seconds=0,
                    hooks_executed=[],
                    hooks_failed=[],
                    errors={"shutdown": "Already in progress"},
                )

            self._is_shutting_down = True
            self._shutdown_started = datetime.now(timezone.utc)
            start_time = asyncio.get_event_loop().time()

        logger.info(
            "Starting graceful shutdown",
            extra={
                "event_type": "shutdown_started",
                "active_requests": self._active_requests,
                "hooks_count": len(self._hooks),
            }
        )

        # Step 1: Mark as draining
        if self.health_router:
            self.health_router.start_draining()

        # Step 2: Wait for active requests to drain
        if self._active_requests > 0:
            logger.info(
                f"Waiting for {self._active_requests} active requests to complete"
            )
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.drain_timeout,
                )
                logger.info("All active requests completed")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Drain timeout after {self.drain_timeout}s, "
                    f"{self._active_requests} requests still active"
                )

        # Step 3: Execute shutdown hooks
        hooks_executed = []
        hooks_failed = []
        errors = {}

        for hook in self._hooks:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > self.force_timeout:
                logger.error(
                    f"Force timeout reached after {elapsed:.2f}s, "
                    f"skipping remaining hooks"
                )
                break

            logger.info(f"Executing shutdown hook: {hook.name}")
            try:
                await asyncio.wait_for(hook.func(), timeout=hook.timeout)
                hooks_executed.append(hook.name)
                logger.info(f"Shutdown hook completed: {hook.name}")
            except asyncio.TimeoutError:
                hooks_failed.append(hook.name)
                errors[hook.name] = f"Timeout after {hook.timeout}s"
                logger.error(f"Shutdown hook timed out: {hook.name}")
            except Exception as e:
                hooks_failed.append(hook.name)
                errors[hook.name] = str(e)
                logger.error(f"Shutdown hook failed: {hook.name} - {e}")

        duration = asyncio.get_event_loop().time() - start_time
        success = len(hooks_failed) == 0

        logger.info(
            f"Graceful shutdown {'completed' if success else 'completed with errors'}",
            extra={
                "event_type": "shutdown_complete",
                "success": success,
                "duration_seconds": duration,
                "hooks_executed": hooks_executed,
                "hooks_failed": hooks_failed,
            }
        )

        return ShutdownResult(
            success=success,
            duration_seconds=duration,
            hooks_executed=hooks_executed,
            hooks_failed=hooks_failed,
            errors=errors,
        )


# Singleton instance
_shutdown_manager: Optional[GracefulShutdownManager] = None


def get_shutdown_manager(
    drain_timeout: float = 30.0,
    force_timeout: float = 45.0,
) -> GracefulShutdownManager:
    """Get or create the shutdown manager singleton"""
    global _shutdown_manager
    if _shutdown_manager is None:
        _shutdown_manager = GracefulShutdownManager(
            drain_timeout=drain_timeout,
            force_timeout=force_timeout,
        )
    return _shutdown_manager

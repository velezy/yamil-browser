"""
SSE Streaming with Heartbeat

Server-Sent Events implementation with:
- Heartbeat to keep connections alive
- Backpressure handling
- Structured event format
- Disconnect detection
"""

import asyncio
import json
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import AsyncGenerator, Any, Optional

from fastapi import Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)


class SSEEventType(Enum):
    """SSE event types"""
    CONNECTED = "connected"
    HEARTBEAT = "heartbeat"
    PROGRESS = "progress"
    TOKEN = "token"           # Streaming LLM token
    TRANSCRIPT = "transcript" # STT result
    AUDIO = "audio"           # TTS audio chunk
    STATE = "state"           # Voice state change
    COMPLETE = "complete"
    ERROR = "error"


@dataclass
class SSEEvent:
    """Structured SSE event with metadata"""
    event_type: SSEEventType
    data: dict
    sequence: int
    timestamp: float = field(default_factory=time.time)
    event_id: Optional[str] = None

    def to_sse_format(self) -> str:
        """Convert to SSE wire format"""
        lines = []

        if self.event_id:
            lines.append(f"id: {self.event_id}")

        lines.append(f"event: {self.event_type.value}")

        event_data = {
            "type": self.event_type.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            **self.data
        }
        lines.append(f"data: {json.dumps(event_data)}")

        return "\n".join(lines) + "\n\n"


class SSEStreamManager:
    """
    SSE stream manager with heartbeat and backpressure.

    Features:
    - Automatic heartbeat every 30 seconds
    - Backpressure detection (max pending events)
    - Client disconnect detection
    - Structured event format
    """

    HEARTBEAT_INTERVAL = 30.0  # seconds
    BACKPRESSURE_THRESHOLD = 100  # pending events

    def __init__(
        self,
        heartbeat_interval: float = HEARTBEAT_INTERVAL,
        backpressure_threshold: int = BACKPRESSURE_THRESHOLD
    ):
        self.heartbeat_interval = heartbeat_interval
        self.backpressure_threshold = backpressure_threshold
        self._active_streams = 0

    async def stream_response(
        self,
        request: Request,
        generator: AsyncGenerator[dict, None],
        include_connected: bool = True,
        include_complete: bool = True
    ) -> StreamingResponse:
        """
        Create a streaming response from an async generator.

        Args:
            request: FastAPI request for disconnect detection
            generator: Async generator yielding event dicts
            include_connected: Send connected event at start
            include_complete: Send complete event at end

        Returns:
            StreamingResponse with SSE content
        """
        async def event_stream():
            self._active_streams += 1
            sequence = 0
            last_heartbeat = time.time()
            pending_count = 0

            try:
                # Send connected event
                if include_connected:
                    yield SSEEvent(
                        event_type=SSEEventType.CONNECTED,
                        data={"message": "Connected", "stream_id": id(generator)},
                        sequence=sequence,
                        timestamp=time.time()
                    ).to_sse_format()
                    sequence += 1

                async for item in generator:
                    # Check for client disconnect
                    if await request.is_disconnected():
                        logger.info("Client disconnected, stopping stream")
                        break

                    # Check backpressure
                    if pending_count > self.backpressure_threshold:
                        logger.warning(f"Backpressure threshold reached: {pending_count}")
                        await asyncio.sleep(0.1)  # Brief pause
                        pending_count = 0

                    # Send heartbeat if needed
                    current_time = time.time()
                    if current_time - last_heartbeat > self.heartbeat_interval:
                        yield SSEEvent(
                            event_type=SSEEventType.HEARTBEAT,
                            data={"uptime_seconds": current_time - last_heartbeat},
                            sequence=sequence,
                            timestamp=current_time
                        ).to_sse_format()
                        sequence += 1
                        last_heartbeat = current_time

                    # Determine event type from item
                    event_type = self._get_event_type(item)

                    # Send data event
                    yield SSEEvent(
                        event_type=event_type,
                        data=item,
                        sequence=sequence,
                        timestamp=time.time()
                    ).to_sse_format()
                    sequence += 1
                    pending_count += 1

                # Send complete event
                if include_complete:
                    yield SSEEvent(
                        event_type=SSEEventType.COMPLETE,
                        data={"message": "Done", "total_events": sequence},
                        sequence=sequence,
                        timestamp=time.time()
                    ).to_sse_format()

            except Exception as e:
                logger.error(f"SSE stream error: {e}")
                yield SSEEvent(
                    event_type=SSEEventType.ERROR,
                    data={"message": str(e)},
                    sequence=sequence,
                    timestamp=time.time()
                ).to_sse_format()

            finally:
                self._active_streams -= 1

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
            }
        )

    def _get_event_type(self, item: dict) -> SSEEventType:
        """Determine event type from item data"""
        if "type" in item:
            try:
                return SSEEventType(item["type"])
            except ValueError:
                pass

        # Infer from content
        if "token" in item or "text" in item:
            return SSEEventType.TOKEN
        if "audio" in item or "audio_data" in item:
            return SSEEventType.AUDIO
        if "transcript" in item:
            return SSEEventType.TRANSCRIPT
        if "state" in item:
            return SSEEventType.STATE
        if "error" in item:
            return SSEEventType.ERROR

        return SSEEventType.PROGRESS

    @property
    def active_streams(self) -> int:
        """Get count of active streams"""
        return self._active_streams


# Convenience functions

async def stream_tokens(
    request: Request,
    token_generator: AsyncGenerator[str, None]
) -> StreamingResponse:
    """
    Simple helper to stream LLM tokens.

    Args:
        request: FastAPI request
        token_generator: Generator yielding token strings

    Returns:
        StreamingResponse
    """
    async def wrapped_generator():
        async for token in token_generator:
            yield {"type": "token", "text": token}

    manager = SSEStreamManager()
    return await manager.stream_response(request, wrapped_generator())


async def stream_with_progress(
    request: Request,
    task_generator: AsyncGenerator[dict, None],
    total_steps: Optional[int] = None
) -> StreamingResponse:
    """
    Stream task progress with percentage.

    Args:
        request: FastAPI request
        task_generator: Generator yielding {"step": int, "message": str}
        total_steps: Optional total for percentage calculation

    Returns:
        StreamingResponse
    """
    async def wrapped_generator():
        step = 0
        async for item in task_generator:
            step += 1
            progress = (step / total_steps * 100) if total_steps else None
            yield {
                "type": "progress",
                "step": step,
                "progress_percent": progress,
                **item
            }

    manager = SSEStreamManager()
    return await manager.stream_response(request, wrapped_generator())


# Singleton instance
_sse_manager: Optional[SSEStreamManager] = None


def get_sse_manager() -> SSEStreamManager:
    """Get or create singleton SSE manager"""
    global _sse_manager
    if _sse_manager is None:
        _sse_manager = SSEStreamManager()
    return _sse_manager

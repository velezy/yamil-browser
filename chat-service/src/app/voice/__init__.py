"""
Voice & Streaming Module

Real-time voice chat with STT, TTS, and streaming responses.

Components:
- SSE streaming with heartbeat and backpressure
- Faster-Whisper STT (6x faster than standard Whisper)
- Edge TTS (Microsoft neural voices - high quality, no API key)
- Parallel sentence TTS for fast perceived latency
- WebSocket voice chat protocol
"""

from .sse import SSEEventType, SSEEvent, SSEStreamManager, get_sse_manager
from .stt import FasterWhisperSTT, get_stt_service, TranscriptionResult
from .tts import EdgeTTS, get_tts_service, AVAILABLE_VOICES, VOICE_DISPLAY_NAMES, TTSResult
from .pipeline import StreamingTTSPipeline, get_streaming_pipeline, StreamEvent
from .text_utils import (
    strip_markdown,
    clean_for_tts,
    split_sentences_for_streaming,
    is_markdown_clean_available
)

__all__ = [
    # SSE
    'SSEEventType',
    'SSEEvent',
    'SSEStreamManager',
    'get_sse_manager',
    # STT
    'FasterWhisperSTT',
    'get_stt_service',
    'TranscriptionResult',
    # TTS
    'EdgeTTS',
    'get_tts_service',
    'AVAILABLE_VOICES',
    'VOICE_DISPLAY_NAMES',
    'TTSResult',
    # Pipeline
    'StreamingTTSPipeline',
    'get_streaming_pipeline',
    'StreamEvent',
    # Text Utils
    'strip_markdown',
    'clean_for_tts',
    'split_sentences_for_streaming',
    'is_markdown_clean_available',
]

"""
Streaming TTS Service for WebSocket

Ultra-low latency TTS streaming:
- Processes LLM tokens as they arrive
- Extracts speakable phrases/sentences
- Streams audio chunks immediately via WebSocket

Architecture:
LLM tokens → Phrase extraction → Streaming Provider → Audio chunks → WebSocket → Browser

Providers (in order of latency):
1. ElevenLabs - WebSocket streaming, ~200ms latency (commercial)
2. Coqui XTTS - HTTP streaming, ~400ms latency (open source, self-hosted)
3. Edge TTS - HTTP chunked, ~600ms+ latency (free, always available)
"""

import asyncio
import base64
import os
import re
import logging
from typing import AsyncGenerator, Optional, Callable, Any
from dataclasses import dataclass

from .tts import EdgeTTS, get_tts_service, AVAILABLE_VOICES, DEFAULT_VOICE
from .text_utils import clean_for_tts
from .streaming_providers import (
    StreamingConfig,
    TTSProvider,
    StreamingTTSProvider,
    get_streaming_provider,
    auto_select_provider,
)

# Production detection - Docker sets this environment variable
IS_PRODUCTION = os.getenv("PRODUCTION", "false").lower() == "true"

# Import SpeechAgent for intelligent content conversion
try:
    from services.orchestrator.app.agents.speech_agent import convert_to_speech
    SPEECH_AGENT_AVAILABLE = True
except ImportError:
    # Fallback to local speech analyzer if orchestrator not available
    from .speech_analyzer import convert_to_speech
    SPEECH_AGENT_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class AudioChunk:
    """Audio chunk for streaming"""
    data: bytes           # Raw MP3 audio bytes
    index: int            # Chunk sequence number
    is_final: bool        # Last chunk for this phrase
    phrase_index: int     # Which phrase this belongs to


@dataclass
class StreamingTTSConfig:
    """Configuration for streaming TTS"""
    voice: str = DEFAULT_VOICE
    speed: float = 1.0
    # Minimum characters before synthesizing (higher = more natural, lower = faster response)
    # Production uses much lower values for minimal delay
    min_phrase_length: int = 20 if IS_PRODUCTION else 100
    # Maximum characters to buffer before forcing synthesis
    max_buffer_length: int = 80 if IS_PRODUCTION else 300
    # Enable phrase-level streaming (vs sentence-level)
    # False = only break on sentence endings (.!?) for smoother audio
    phrase_streaming: bool = False  # Disable comma breaks for natural flow
    # Provider selection
    provider: TTSProvider = TTSProvider.EDGE
    # Auto-select best available provider
    auto_select: bool = True
    # Kokoro-82M settings (open source, fast, high quality)
    kokoro_url: str = "http://localhost:8880"
    # Coqui XTTS settings (open source option)
    coqui_url: str = "http://localhost:5002"
    # ElevenLabs settings (commercial option)
    elevenlabs_api_key: Optional[str] = None
    elevenlabs_voice_id: str = "21m00Tcm4TlvDq8ikWAM"


class StreamingTTSService:
    """
    Real-time TTS streaming service.

    Processes LLM tokens immediately, extracts phrases, and streams
    audio chunks with ultra-low latency.

    Usage:
        service = StreamingTTSService()
        async for chunk in service.stream_tts(llm_token_generator):
            # Send chunk.data (bytes) via WebSocket
    """

    # Phrase break patterns - where it's natural to pause speech
    PHRASE_BREAKS = re.compile(
        r'[.!?;:]\s+|'     # Sentence endings
        r',\s+|'           # Commas
        r'\s+[-–—]\s+|'    # Dashes
        r'\n\n+'           # Paragraph breaks
    )

    # Sentence endings for priority breaks
    SENTENCE_ENDINGS = re.compile(r'[.!?]\s*$')

    def __init__(self, config: Optional[StreamingTTSConfig] = None):
        """
        Initialize streaming TTS service.

        Args:
            config: Streaming configuration
        """
        self.config = config or StreamingTTSConfig()
        # Legacy Edge TTS for backwards compatibility
        self.tts = get_tts_service(self.config.voice)
        # Streaming provider (will be initialized on first use)
        self._provider: Optional[StreamingTTSProvider] = None
        self._provider_initialized = False
        self._phrase_index = 0
        self._chunk_index = 0

    async def _get_provider(self) -> StreamingTTSProvider:
        """Get or initialize the streaming provider"""
        if not self._provider_initialized:
            provider_config = StreamingConfig(
                provider=self.config.provider,
                voice=self.config.voice,
                speed=self.config.speed,
                kokoro_url=self.config.kokoro_url,
                kokoro_voice=self.config.voice,  # Use same voice ID
                coqui_url=self.config.coqui_url,
                chatterbox_url=os.getenv("CHATTERBOX_URL", "http://localhost:4123"),
                chatterbox_voice=self.config.voice,  # Pass user's voice selection for cloning
                higgs_audio_url=os.getenv("HIGGS_AUDIO_URL", "http://localhost:8000"),
                higgs_audio_voice=self.config.voice,
                elevenlabs_api_key=self.config.elevenlabs_api_key or os.getenv("ELEVENLABS_API_KEY"),
                elevenlabs_voice_id=self.config.elevenlabs_voice_id,
            )

            if self.config.auto_select:
                self._provider = await auto_select_provider(provider_config)
            else:
                self._provider = get_streaming_provider(provider_config)

            self._provider_initialized = True
            logger.info(f"TTS provider initialized: {self._provider.get_info()}")

        return self._provider

    def set_voice(self, voice: str) -> bool:
        """Change the TTS voice"""
        if voice in AVAILABLE_VOICES:
            self.config.voice = voice
            self.tts.set_voice(voice)
            if self._provider:
                self._provider.set_voice(voice)
            return True
        return False

    def set_speed(self, speed: float):
        """Change the speech speed"""
        self.config.speed = max(0.5, min(2.0, speed))

    def set_provider(self, provider_name: str):
        """Change the TTS provider, forcing re-initialization"""
        try:
            new_provider = TTSProvider(provider_name)
            if new_provider != self.config.provider:
                self.config.provider = new_provider
                self.config.auto_select = False  # Explicit user choice overrides auto
                self._provider = None
                self._provider_initialized = False
                logger.info(f"TTS provider set to: {provider_name}")
        except ValueError:
            logger.warning(f"Unknown TTS provider: {provider_name}")

    async def stream_tts(
        self,
        token_stream: AsyncGenerator[str, None],
        on_text: Optional[Callable[[str], Any]] = None
    ) -> AsyncGenerator[AudioChunk, None]:
        """
        Stream TTS audio from LLM token stream.

        This is the main entry point. Processes tokens as they arrive,
        extracts speakable phrases, and yields audio chunks immediately.

        Args:
            token_stream: Async generator yielding LLM tokens
            on_text: Optional callback for each phrase extracted

        Yields:
            AudioChunk with MP3 audio data
        """
        self._phrase_index = 0
        self._chunk_index = 0
        text_buffer = ""

        # Queue for parallel TTS generation
        tts_queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
        audio_queue: asyncio.Queue[AudioChunk | None] = asyncio.Queue()

        # Start TTS worker
        tts_task = asyncio.create_task(
            self._tts_worker(tts_queue, audio_queue)
        )

        try:
            async for token in token_stream:
                text_buffer += token

                # Check if we should synthesize now
                phrases, text_buffer = self._extract_phrases(text_buffer)

                for phrase in phrases:
                    clean_phrase = clean_for_tts(phrase)
                    if clean_phrase and len(clean_phrase) >= 3:
                        if on_text:
                            on_text(phrase)
                        await tts_queue.put((self._phrase_index, clean_phrase))
                        self._phrase_index += 1

            # Process remaining buffer
            if text_buffer.strip():
                clean_phrase = clean_for_tts(text_buffer.strip())
                if clean_phrase and len(clean_phrase) >= 3:
                    if on_text:
                        on_text(text_buffer.strip())
                    await tts_queue.put((self._phrase_index, clean_phrase))
                    self._phrase_index += 1

            # Signal end of input
            await tts_queue.put((-1, ""))

            # Yield audio chunks as they become available
            while True:
                chunk = await audio_queue.get()
                if chunk is None:
                    break
                yield chunk

        except asyncio.CancelledError:
            logger.info("TTS streaming cancelled")
            tts_task.cancel()
            raise

        finally:
            if not tts_task.done():
                tts_task.cancel()
                try:
                    await tts_task
                except asyncio.CancelledError:
                    pass

    async def _tts_worker(
        self,
        tts_queue: asyncio.Queue[tuple[int, str]],
        audio_queue: asyncio.Queue[AudioChunk | None]
    ):
        """
        Worker that processes TTS requests from queue.

        Runs synthesis in parallel with token processing for
        minimal latency. Uses the best available streaming provider.
        """
        try:
            # Get the streaming provider
            provider = await self._get_provider()
            use_provider = provider.supports_true_streaming

            while True:
                phrase_idx, text = await tts_queue.get()

                if phrase_idx == -1:  # End signal
                    await audio_queue.put(None)
                    break

                # Stream audio chunks for this phrase
                chunk_count = 0

                # Use streaming provider if it supports true streaming
                if use_provider:
                    async for audio_bytes in provider.stream_audio(
                        text, speed=self.config.speed
                    ):
                        chunk = AudioChunk(
                            data=audio_bytes,
                            index=self._chunk_index,
                            is_final=False,
                            phrase_index=phrase_idx
                        )
                        self._chunk_index += 1
                        chunk_count += 1
                        await audio_queue.put(chunk)
                else:
                    # Fallback to Edge TTS for non-streaming providers
                    async for audio_bytes in self.tts.synthesize_streaming(
                        text, speed=self.config.speed
                    ):
                        chunk = AudioChunk(
                            data=audio_bytes,
                            index=self._chunk_index,
                            is_final=False,
                            phrase_index=phrase_idx
                        )
                        self._chunk_index += 1
                        chunk_count += 1
                        await audio_queue.put(chunk)

                # Mark the end of this phrase
                if chunk_count > 0:
                    logger.debug(f"Phrase {phrase_idx} complete: {chunk_count} chunks")

        except asyncio.CancelledError:
            await audio_queue.put(None)
            raise

        except Exception as e:
            logger.error(f"TTS worker error: {e}")
            await audio_queue.put(None)

    def _extract_phrases(self, text: str) -> tuple[list[str], str]:
        """
        Extract speakable phrases from text buffer.

        Optimized for natural speech flow - waits for complete sentences
        rather than breaking on every comma for smoother audio.

        Args:
            text: Current text buffer

        Returns:
            (list of phrases to synthesize, remaining buffer)
        """
        if not text:
            return [], ""

        phrases = []

        # Force synthesis if buffer is too long
        if len(text) >= self.config.max_buffer_length:
            # Find best break point - prefer sentence endings
            match = None
            # First try to find a sentence ending
            for m in re.finditer(r'[.!?]\s+', text):
                if m.end() <= self.config.max_buffer_length:
                    match = m
            # If no sentence ending, try other breaks
            if not match:
                for m in self.PHRASE_BREAKS.finditer(text):
                    if m.end() <= self.config.max_buffer_length:
                        match = m

            if match:
                phrases.append(text[:match.end()].strip())
                return phrases, text[match.end():]
            else:
                # No good break point, force at max length
                phrases.append(text[:self.config.max_buffer_length].strip())
                return phrases, text[self.config.max_buffer_length:]

        # ALWAYS check for complete sentences (natural speech boundaries)
        # This ensures we synthesize complete thoughts for better audio quality
        sentence_match = re.search(r'[.!?]\s+', text)
        if sentence_match and sentence_match.end() >= self.config.min_phrase_length:
            phrases.append(text[:sentence_match.end()].strip())
            return phrases, text[sentence_match.end():]

        # Only break on commas/dashes if phrase_streaming is enabled
        # Otherwise wait for sentence endings for smoother, more natural audio
        if self.config.phrase_streaming and len(text) >= self.config.min_phrase_length:
            # Look for phrase breaks (commas, dashes, etc.)
            for match in self.PHRASE_BREAKS.finditer(text):
                if match.end() >= self.config.min_phrase_length:
                    phrases.append(text[:match.end()].strip())
                    return phrases, text[match.end():]

        return phrases, text


class StreamingTTSWebSocket:
    """
    WebSocket handler for streaming TTS.

    Protocol:
    Client → Server:
    - {"type": "config", "voice": "af_heart", "speed": 1.0}
    - {"type": "start", "session_id": "abc123"}  # session_id for audio sync
    - {"type": "token", "text": "Hello "}
    - {"type": "end"}
    - {"type": "stop"}

    Server → Client:
    - {"type": "ready"}
    - {"type": "audio", "data": "base64...", "index": 0, "phrase": 0, "session_id": "abc123"}
    - {"type": "phrase_end", "phrase": 0}
    - {"type": "complete", "session_id": "abc123"}
    - {"type": "error", "message": "..."}
    """

    def __init__(self):
        self.service = StreamingTTSService()
        self._token_queue: Optional[asyncio.Queue[str | None]] = None
        self._streaming = False
        self._session_id: Optional[str] = None  # Track current session for audio sync
        self._stream_task: Optional[asyncio.Task] = None  # Track streaming task for cancellation

    async def handle(self, websocket):
        """Handle WebSocket connection"""
        from fastapi import WebSocket

        await websocket.accept()
        await websocket.send_json({"type": "ready"})

        try:
            while True:
                message = await websocket.receive_json()
                await self._handle_message(websocket, message)

        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            try:
                await websocket.send_json({"type": "error", "message": str(e)})
            except Exception:
                pass

    async def _handle_message(self, websocket, message: dict):
        """Handle incoming WebSocket message"""
        msg_type = message.get("type")
        logger.info(f"WebSocket TTS received message type: {msg_type}")

        if msg_type == "config":
            voice = message.get("voice")
            speed = message.get("speed", 1.0)
            provider = message.get("provider")

            if voice:
                self.service.set_voice(voice)
            self.service.set_speed(speed)
            if provider:
                self.service.set_provider(provider)

            await websocket.send_json({
                "type": "config_updated",
                "voice": self.service.config.voice,
                "speed": self.service.config.speed,
                "provider": self.service.config.provider.value
            })

        elif msg_type == "start":
            # Cancel any existing streaming task to prevent audio from old session
            if self._stream_task and not self._stream_task.done():
                logger.info("WebSocket TTS: Cancelling previous streaming session")
                self._streaming = False
                if self._token_queue:
                    await self._token_queue.put(None)  # Signal end to old task
                self._stream_task.cancel()
                try:
                    await self._stream_task
                except asyncio.CancelledError:
                    pass

            # Start new streaming session with session ID
            session_id = message.get("session_id", str(id(asyncio.current_task())))
            logger.info(f"WebSocket TTS: Starting streaming session {session_id}")
            self._session_id = session_id
            self._token_queue = asyncio.Queue()
            self._streaming = True

            # Start streaming task
            self._stream_task = asyncio.create_task(
                self._stream_audio(websocket, session_id)
            )

        elif msg_type == "token":
            # Add token to queue
            if self._token_queue and self._streaming:
                text = message.get("text", "")
                logger.debug(f"WebSocket TTS: Received token: {text[:20] if text else ''}")
                await self._token_queue.put(text)

        elif msg_type == "end":
            # End of tokens
            if self._token_queue:
                await self._token_queue.put(None)

        elif msg_type == "stop":
            # Stop streaming
            self._streaming = False
            if self._token_queue:
                await self._token_queue.put(None)

        elif msg_type == "synthesize":
            # Synthesize complete text (for "Read Aloud" on existing messages)
            text = message.get("text", "")
            if text:
                asyncio.create_task(
                    self._synthesize_full_text(websocket, text)
                )

        elif msg_type == "get_voices":
            await websocket.send_json({
                "type": "voices",
                "voices": list(AVAILABLE_VOICES.keys())
            })

    async def _stream_audio(self, websocket, session_id: str):
        """Stream audio chunks to WebSocket with session tracking"""
        if not self._token_queue:
            return

        token_gen = None
        tts_gen = None
        # Capture the session_id at task start to prevent mixing audio from different sessions
        current_session = session_id

        async def token_generator():
            while self._streaming and self._session_id == current_session:
                try:
                    # 120 second timeout to handle slow LLM responses
                    token = await asyncio.wait_for(self._token_queue.get(), timeout=120.0)
                    if token is None:
                        break
                    yield token
                except asyncio.TimeoutError:
                    logger.warning("TTS token_generator: Timeout waiting for tokens")
                    break
                except asyncio.CancelledError:
                    break

        try:
            token_gen = token_generator()
            tts_gen = self.service.stream_tts(token_gen)

            async for chunk in tts_gen:
                # Stop if streaming was cancelled or session changed
                if not self._streaming or self._session_id != current_session:
                    logger.info(f"TTS: Session {current_session} cancelled, stopping audio")
                    break

                # Send audio chunk as base64 with session_id for client-side filtering
                await websocket.send_json({
                    "type": "audio",
                    "data": base64.b64encode(chunk.data).decode(),
                    "index": chunk.index,
                    "phrase": chunk.phrase_index,
                    "session_id": current_session
                })

            # Only send complete if this session is still active
            if self._session_id == current_session:
                await websocket.send_json({"type": "complete", "session_id": current_session})

        except asyncio.CancelledError:
            pass
        except GeneratorExit:
            pass
        except Exception as e:
            if "GeneratorExit" not in str(e):
                logger.error(f"Audio streaming error: {e}")
            try:
                await websocket.send_json({"type": "error", "message": str(e)})
            except Exception:
                pass
        finally:
            self._streaming = False
            # Properly close async generators
            if tts_gen is not None:
                try:
                    await tts_gen.aclose()
                except Exception:
                    pass
            if token_gen is not None:
                try:
                    await token_gen.aclose()
                except Exception:
                    pass

    async def _synthesize_full_text(self, websocket, text: str):
        """Synthesize complete text and stream audio (for Read Aloud)"""
        try:
            # Use LLM-based speech analyzer for complete messages
            # Falls back to regex if LLM unavailable
            clean_text = await convert_to_speech(text)
            if not clean_text or len(clean_text) < 3:
                await websocket.send_json({"type": "complete"})
                return

            chunk_index = 0
            async for audio_bytes in self.service.tts.synthesize_streaming(
                clean_text, speed=self.service.config.speed
            ):
                await websocket.send_json({
                    "type": "audio",
                    "data": base64.b64encode(audio_bytes).decode(),
                    "index": chunk_index,
                    "phrase": 0
                })
                chunk_index += 1

            await websocket.send_json({"type": "complete"})

        except Exception as e:
            logger.error(f"Full text synthesis error: {e}")
            await websocket.send_json({"type": "error", "message": str(e)})


# Singleton
_streaming_service: Optional[StreamingTTSService] = None


def get_streaming_tts_service(
    voice: str = DEFAULT_VOICE,
    speed: float = 1.0
) -> StreamingTTSService:
    """Get or create streaming TTS service"""
    global _streaming_service
    if _streaming_service is None:
        config = StreamingTTSConfig(voice=voice, speed=speed)
        _streaming_service = StreamingTTSService(config)
    return _streaming_service

"""
WebSocket Voice Chat

Real-time voice chat over WebSocket with:
- Audio streaming from browser
- Speech-to-text transcription
- LLM response generation
- Text-to-speech synthesis
- Interrupt support
"""

import asyncio
import base64
import logging
from typing import Optional, Callable, AsyncGenerator
from dataclasses import dataclass
from enum import Enum

from fastapi import WebSocket, WebSocketDisconnect

from .stt import FasterWhisperSTT, get_stt_service
from .tts import EdgeTTS, get_tts_service, AVAILABLE_VOICES
from .pipeline import StreamingTTSPipeline

logger = logging.getLogger(__name__)


class VoiceState(Enum):
    """Voice chat states"""
    IDLE = "idle"
    LISTENING = "listening"
    PROCESSING = "processing"
    SPEAKING = "speaking"
    ERROR = "error"


@dataclass
class VoiceConfig:
    """Voice chat configuration"""
    voice: str = "af_heart"
    language: str = "en"
    speed: float = 1.0
    auto_play: bool = True


class VoiceChatHandler:
    """
    WebSocket handler for voice chat.

    Protocol:
    Client → Server:
    - {"type": "config", "voice": "af_heart", "language": "en", "speed": 1.0}
    - {"type": "start_listening"}
    - {"type": "audio_chunk", "audio": "base64...", "format": "webm"}
    - {"type": "stop_listening"}
    - {"type": "text_input", "text": "Question?"}
    - {"type": "interrupt"}

    Server → Client:
    - {"type": "state", "state": "idle|listening|processing|speaking"}
    - {"type": "transcript", "text": "User speech...", "confidence": 0.95}
    - {"type": "token", "text": "AI"} (streaming)
    - {"type": "response", "text": "Full response", "has_audio": true}
    - {"type": "audio", "data": "base64...", "sample_rate": 24000}
    - {"type": "error", "message": "..."}
    """

    def __init__(
        self,
        llm_generator: Optional[Callable[[str], AsyncGenerator[str, None]]] = None
    ):
        """
        Initialize voice chat handler.

        Args:
            llm_generator: Function that takes text and returns token stream
        """
        self.stt = get_stt_service()
        self.tts = get_tts_service()
        self.llm_generator = llm_generator
        self.config = VoiceConfig()
        self._state = VoiceState.IDLE
        self._audio_buffer = bytearray()
        self._current_task: Optional[asyncio.Task] = None
        self._interrupted = False

    async def handle(self, websocket: WebSocket):
        """
        Handle WebSocket connection for voice chat.

        Args:
            websocket: FastAPI WebSocket connection
        """
        await websocket.accept()

        # Initialize services
        await self._initialize_services()

        # Send initial state
        await self._send_state(websocket, VoiceState.IDLE)

        try:
            while True:
                message = await websocket.receive_json()
                await self._handle_message(websocket, message)

        except WebSocketDisconnect:
            logger.info("Voice chat client disconnected")

        except Exception as e:
            logger.error(f"Voice chat error: {e}")
            await self._send_error(websocket, str(e))

        finally:
            # Cancel any running task
            if self._current_task:
                self._current_task.cancel()

    async def _initialize_services(self):
        """Initialize STT and TTS services"""
        await self.stt.initialize()
        await self.tts.initialize()

    async def _handle_message(self, websocket: WebSocket, message: dict):
        """Handle incoming WebSocket message"""
        msg_type = message.get("type")

        if msg_type == "config":
            await self._handle_config(websocket, message)

        elif msg_type == "start_listening":
            await self._handle_start_listening(websocket)

        elif msg_type == "audio_chunk":
            await self._handle_audio_chunk(websocket, message)

        elif msg_type == "stop_listening":
            await self._handle_stop_listening(websocket)

        elif msg_type == "text_input":
            await self._handle_text_input(websocket, message)

        elif msg_type == "interrupt":
            await self._handle_interrupt(websocket)

        elif msg_type == "get_voices":
            await websocket.send_json({
                "type": "voices",
                "voices": AVAILABLE_VOICES
            })

        elif msg_type == "get_state":
            await self._send_state(websocket, self._state)

        else:
            logger.warning(f"Unknown message type: {msg_type}")

    async def _handle_config(self, websocket: WebSocket, message: dict):
        """Handle configuration update"""
        self.config.voice = message.get("voice", self.config.voice)
        self.config.language = message.get("language", self.config.language)
        self.config.speed = message.get("speed", self.config.speed)
        self.config.auto_play = message.get("auto_play", self.config.auto_play)

        # Update TTS voice
        self.tts.set_voice(self.config.voice)

        await websocket.send_json({
            "type": "config_updated",
            "config": {
                "voice": self.config.voice,
                "language": self.config.language,
                "speed": self.config.speed,
                "auto_play": self.config.auto_play
            }
        })

    async def _handle_start_listening(self, websocket: WebSocket):
        """Start listening for audio"""
        self._audio_buffer.clear()
        self._interrupted = False
        await self._send_state(websocket, VoiceState.LISTENING)

    async def _handle_audio_chunk(self, websocket: WebSocket, message: dict):
        """Handle incoming audio chunk"""
        if self._state != VoiceState.LISTENING:
            return

        try:
            audio_bytes = base64.b64decode(message["audio"])
            self._audio_buffer.extend(audio_bytes)
        except Exception as e:
            logger.error(f"Failed to decode audio chunk: {e}")

    async def _handle_stop_listening(self, websocket: WebSocket):
        """Stop listening and process audio"""
        if not self._audio_buffer:
            await self._send_error(websocket, "No audio recorded")
            await self._send_state(websocket, VoiceState.IDLE)
            return

        await self._send_state(websocket, VoiceState.PROCESSING)

        try:
            # Transcribe audio
            result = await self.stt.transcribe(
                bytes(self._audio_buffer),
                language=self.config.language
            )

            # Send transcript
            await websocket.send_json({
                "type": "transcript",
                "text": result.text,
                "language": result.language,
                "confidence": result.confidence,
                "duration": result.duration
            })

            if not result.text.strip():
                await self._send_error(websocket, "No speech detected")
                await self._send_state(websocket, VoiceState.IDLE)
                return

            # Generate response
            await self._generate_response(websocket, result.text)

        except Exception as e:
            logger.error(f"Processing error: {e}")
            await self._send_error(websocket, str(e))
            await self._send_state(websocket, VoiceState.IDLE)

        finally:
            self._audio_buffer.clear()

    async def _handle_text_input(self, websocket: WebSocket, message: dict):
        """Handle text input (instead of voice)"""
        text = message.get("text", "").strip()

        if not text:
            await self._send_error(websocket, "No text provided")
            return

        await self._send_state(websocket, VoiceState.PROCESSING)
        await self._generate_response(websocket, text)

    async def _handle_interrupt(self, websocket: WebSocket):
        """Handle interrupt request"""
        self._interrupted = True

        if self._current_task:
            self._current_task.cancel()
            self._current_task = None

        await self._send_state(websocket, VoiceState.IDLE)

    async def _generate_response(self, websocket: WebSocket, text: str):
        """Generate LLM response with streaming and TTS"""
        if not self.llm_generator:
            await self._send_error(websocket, "LLM not configured")
            await self._send_state(websocket, VoiceState.IDLE)
            return

        full_response = ""
        tts_sentences = []
        sentence_buffer = ""

        try:
            # Stream LLM response
            async for token in self.llm_generator(text):
                if self._interrupted:
                    break

                # Send token
                await websocket.send_json({
                    "type": "token",
                    "text": token
                })
                full_response += token
                sentence_buffer += token

                # Check for complete sentences
                if sentence_buffer.rstrip().endswith(('.', '!', '?')):
                    tts_sentences.append(sentence_buffer.strip())
                    sentence_buffer = ""

            # Add remaining buffer
            if sentence_buffer.strip():
                tts_sentences.append(sentence_buffer.strip())

            if self._interrupted:
                await self._send_state(websocket, VoiceState.IDLE)
                return

            # Send complete response
            await websocket.send_json({
                "type": "response",
                "text": full_response,
                "has_audio": bool(tts_sentences)
            })

            # Generate and send TTS
            if tts_sentences:
                await self._send_state(websocket, VoiceState.SPEAKING)

                # Generate TTS for all sentences
                pipeline = StreamingTTSPipeline(
                    tts=self.tts,
                    speed=self.config.speed,
                    combine_audio=True
                )

                # Create a simple generator from full text
                async def text_generator():
                    yield full_response

                async for event in pipeline.stream_with_tts(
                    text_generator(),
                    include_audio=True
                ):
                    if event.type == "audio":
                        await websocket.send_json({
                            "type": "audio",
                            "data": event.data["data"],
                            "sample_rate": event.data["sample_rate"],
                            "format": event.data["format"]
                        })

            await self._send_state(websocket, VoiceState.IDLE)

        except asyncio.CancelledError:
            logger.info("Response generation cancelled")
            await self._send_state(websocket, VoiceState.IDLE)

        except Exception as e:
            logger.error(f"Response generation error: {e}")
            await self._send_error(websocket, str(e))
            await self._send_state(websocket, VoiceState.IDLE)

    async def _send_state(self, websocket: WebSocket, state: VoiceState):
        """Send state update"""
        self._state = state
        await websocket.send_json({
            "type": "state",
            "state": state.value
        })

    async def _send_error(self, websocket: WebSocket, message: str):
        """Send error message"""
        await websocket.send_json({
            "type": "error",
            "message": message
        })


def create_voice_chat_handler(
    llm_generator: Optional[Callable[[str], AsyncGenerator[str, None]]] = None
) -> VoiceChatHandler:
    """
    Create a voice chat handler.

    Args:
        llm_generator: Function that takes text and returns token stream

    Returns:
        VoiceChatHandler instance
    """
    return VoiceChatHandler(llm_generator=llm_generator)

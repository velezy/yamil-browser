"""
Speech-to-Text with Provider Abstraction

Supports multiple STT backends via a common STTProvider interface.
Default: Faster-Whisper (6x faster than standard Whisper).

Features:
- Provider ABC for pluggable STT backends
- FasterWhisperSTT: distil-large-v3 model (best speed/accuracy)
- int8 quantization (~0.8GB RAM)
- Voice Activity Detection (VAD)
- Streaming transcription support
- Factory function for provider selection via STT_PROVIDER env var
"""

import asyncio
import logging
import io
import os
import wave
from abc import ABC, abstractmethod
from typing import Optional, List, AsyncGenerator, Dict, Any
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)

# STT provider selection from env
STT_PROVIDER = os.getenv("STT_PROVIDER", "faster-whisper")

# Try to import faster_whisper
try:
    from faster_whisper import WhisperModel
    FASTER_WHISPER_AVAILABLE = True
except ImportError:
    FASTER_WHISPER_AVAILABLE = False
    logger.warning("faster-whisper not available. Install with: pip install faster-whisper")


@dataclass
class TranscriptionResult:
    """Result of speech transcription"""
    text: str
    language: str
    duration: float
    confidence: float
    segments: List[dict]


class STTProvider(ABC):
    """
    Abstract base class for Speech-to-Text providers.

    All STT providers must implement this interface.
    """

    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize the provider. Returns True if successful."""
        pass

    @abstractmethod
    async def transcribe(
        self,
        audio_data: bytes,
        language: Optional[str] = None,
        task: str = "transcribe",
    ) -> TranscriptionResult:
        """Transcribe audio bytes to text."""
        pass

    @abstractmethod
    async def transcribe_stream(
        self,
        audio_chunks: asyncio.Queue,
        language: Optional[str] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream transcription for real-time audio."""
        pass

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the provider is available."""
        pass

    @property
    @abstractmethod
    def is_loaded(self) -> bool:
        """Check if the model is loaded."""
        pass

    @abstractmethod
    def get_info(self) -> dict:
        """Get provider info."""
        pass


class FasterWhisperSTT(STTProvider):
    """
    Real-time Speech-to-Text using Faster-Whisper.

    6x faster than standard Whisper with comparable accuracy.

    Models:
    - tiny: Fastest, lowest accuracy
    - base: Fast, decent accuracy
    - small: Balanced
    - medium: Good accuracy
    - large-v3: Best accuracy
    - distil-large-v3: Best speed/accuracy tradeoff (recommended)
    """

    DEFAULT_MODEL = "distil-large-v3"
    FALLBACK_MODEL = "base"  # Lighter fallback

    def __init__(
        self,
        model_name: Optional[str] = None,
        device: str = "auto",
        compute_type: str = "int8",
        download_root: Optional[str] = None
    ):
        """
        Initialize STT service.

        Args:
            model_name: Whisper model name (default: distil-large-v3)
            device: Device to use (auto, cpu, cuda)
            compute_type: Quantization type (int8, float16, float32)
            download_root: Directory to download models to
        """
        self.model_name = model_name or self.DEFAULT_MODEL
        self.device = device
        self.compute_type = compute_type
        self._model: Optional['WhisperModel'] = None
        self._download_root = download_root

        if not FASTER_WHISPER_AVAILABLE:
            logger.error("faster-whisper not installed")

    async def initialize(self) -> bool:
        """
        Initialize the Whisper model.

        Returns:
            True if initialization successful
        """
        if not FASTER_WHISPER_AVAILABLE:
            return False

        if self._model is not None:
            return True

        try:
            # Load model in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            self._model = await loop.run_in_executor(
                None,
                self._load_model
            )
            logger.info(f"Loaded Whisper model: {self.model_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to load Whisper model: {e}")

            # Try fallback model
            if self.model_name != self.FALLBACK_MODEL:
                logger.info(f"Trying fallback model: {self.FALLBACK_MODEL}")
                self.model_name = self.FALLBACK_MODEL
                try:
                    self._model = await loop.run_in_executor(
                        None,
                        self._load_model
                    )
                    return True
                except Exception as e2:
                    logger.error(f"Fallback model also failed: {e2}")

            return False

    def _load_model(self) -> 'WhisperModel':
        """Load the Whisper model (blocking)"""
        return WhisperModel(
            self.model_name,
            device=self.device if self.device != "auto" else "cpu",
            compute_type=self.compute_type,
            download_root=self._download_root
        )

    async def transcribe(
        self,
        audio_data: bytes,
        language: Optional[str] = None,
        task: str = "transcribe"
    ) -> TranscriptionResult:
        """
        Transcribe audio bytes to text.

        Args:
            audio_data: Raw audio bytes (PCM or WAV)
            language: Language code (e.g., 'en', 'es') or None for auto-detect
            task: 'transcribe' or 'translate' (to English)

        Returns:
            TranscriptionResult with text and metadata
        """
        if self._model is None:
            if not await self.initialize():
                return TranscriptionResult(
                    text="",
                    language="unknown",
                    duration=0.0,
                    confidence=0.0,
                    segments=[]
                )

        # Convert audio bytes to numpy array
        audio_float = self._audio_bytes_to_float(audio_data)

        # Run transcription in thread pool
        loop = asyncio.get_event_loop()
        segments, info = await loop.run_in_executor(
            None,
            lambda: self._model.transcribe(
                audio_float,
                language=language,
                task=task,
                beam_size=5,
                best_of=3,
                temperature=0.0,           # Greedy decoding for speed
                vad_filter=True,           # Voice Activity Detection
                vad_parameters=dict(
                    min_silence_duration_ms=300,
                    speech_pad_ms=150
                ),
                condition_on_previous_text=True,
                no_speech_threshold=0.6
            )
        )

        # Collect segments
        segment_list = []
        full_text_parts = []

        for segment in segments:
            segment_list.append({
                "start": segment.start,
                "end": segment.end,
                "text": segment.text,
                "confidence": segment.avg_logprob
            })
            full_text_parts.append(segment.text)

        text = ' '.join(full_text_parts).strip()

        # Calculate average confidence
        avg_confidence = 0.0
        if segment_list:
            avg_confidence = sum(s["confidence"] for s in segment_list) / len(segment_list)

        return TranscriptionResult(
            text=text,
            language=info.language,
            duration=info.duration,
            confidence=avg_confidence,
            segments=segment_list
        )

    async def transcribe_stream(
        self,
        audio_chunks: asyncio.Queue,
        language: Optional[str] = None
    ):
        """
        Stream transcription for real-time audio.

        Args:
            audio_chunks: Queue of audio bytes
            language: Language code or None

        Yields:
            Partial transcription results
        """
        buffer = bytearray()
        chunk_duration_ms = 0
        min_chunk_ms = 500  # Minimum 500ms of audio before transcribing

        while True:
            try:
                chunk = await asyncio.wait_for(
                    audio_chunks.get(),
                    timeout=5.0
                )

                if chunk is None:  # End signal
                    break

                buffer.extend(chunk)
                # Approximate duration (16kHz, 16-bit = 32 bytes/ms)
                chunk_duration_ms = len(buffer) / 32

                # Transcribe when we have enough audio
                if chunk_duration_ms >= min_chunk_ms:
                    result = await self.transcribe(bytes(buffer), language)
                    if result.text:
                        yield {
                            "type": "partial",
                            "text": result.text,
                            "confidence": result.confidence
                        }
                    buffer.clear()

            except asyncio.TimeoutError:
                # Transcribe remaining buffer on timeout
                if buffer:
                    result = await self.transcribe(bytes(buffer), language)
                    if result.text:
                        yield {
                            "type": "final",
                            "text": result.text,
                            "confidence": result.confidence
                        }
                break

    def _audio_bytes_to_float(self, audio_data: bytes) -> np.ndarray:
        """
        Convert audio bytes to float32 numpy array.

        Handles both raw PCM and WAV formats.
        """
        # Check if it's a WAV file
        if audio_data[:4] == b'RIFF':
            # Parse WAV
            with io.BytesIO(audio_data) as f:
                with wave.open(f, 'rb') as wav:
                    frames = wav.readframes(wav.getnframes())
                    sample_width = wav.getsampwidth()

                    if sample_width == 2:  # 16-bit
                        audio_np = np.frombuffer(frames, dtype=np.int16)
                        audio_float = audio_np.astype(np.float32) / 32768.0
                    elif sample_width == 4:  # 32-bit
                        audio_np = np.frombuffer(frames, dtype=np.int32)
                        audio_float = audio_np.astype(np.float32) / 2147483648.0
                    else:  # 8-bit
                        audio_np = np.frombuffer(frames, dtype=np.uint8)
                        audio_float = (audio_np.astype(np.float32) - 128) / 128.0

                    return audio_float
        else:
            # Assume raw 16-bit PCM
            audio_np = np.frombuffer(audio_data, dtype=np.int16)
            return audio_np.astype(np.float32) / 32768.0

    @property
    def is_available(self) -> bool:
        """Check if STT is available"""
        return FASTER_WHISPER_AVAILABLE

    @property
    def is_loaded(self) -> bool:
        """Check if model is loaded"""
        return self._model is not None

    def get_info(self) -> dict:
        """Get service info"""
        return {
            "available": FASTER_WHISPER_AVAILABLE,
            "loaded": self._model is not None,
            "model": self.model_name,
            "device": self.device,
            "compute_type": self.compute_type
        }


# Singleton instance
_stt_service: Optional[STTProvider] = None


def get_stt_service() -> FasterWhisperSTT:
    """Get or create singleton STT service (legacy, returns FasterWhisperSTT)"""
    global _stt_service
    if _stt_service is None:
        _stt_service = FasterWhisperSTT()
    return _stt_service


def get_stt_provider(provider_name: Optional[str] = None) -> STTProvider:
    """
    Factory function to get the appropriate STT provider.

    Args:
        provider_name: Provider name override. Options:
            - 'faster-whisper' (default): Local Faster-Whisper
            - Future: 'deepgram', 'whisper-api', 'azure'

    Returns:
        STTProvider instance
    """
    global _stt_service
    provider = (provider_name or STT_PROVIDER).lower()

    if _stt_service is not None:
        return _stt_service

    if provider == "faster-whisper":
        _stt_service = FasterWhisperSTT()
    else:
        logger.warning(f"Unknown STT provider '{provider}', using faster-whisper")
        _stt_service = FasterWhisperSTT()

    return _stt_service

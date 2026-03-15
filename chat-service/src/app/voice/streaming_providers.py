"""
Streaming TTS Providers

Provider-based architecture for true streaming TTS.
Supports multiple backends with WebSocket-based audio streaming.

Providers:
- Edge TTS: Free Microsoft voices (HTTP-based, sentence-level latency)
- Coqui XTTS: Open source, self-hosted (true streaming via HTTP streaming)
- ElevenLabs: Commercial option (lowest latency WebSocket streaming)
"""

import asyncio
import os
import logging
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class TTSProvider(Enum):
    """Available TTS providers"""
    EDGE = "edge"           # Microsoft Edge TTS (free, HTTP-based)
    COQUI = "coqui"         # Coqui XTTS (open source, self-hosted)
    KOKORO = "kokoro"       # Kokoro-82M (open source, fast, high quality)
    PIPER = "piper"         # Piper (open source, fastest synthesis)
    CHATTERBOX = "chatterbox"  # Chatterbox (SoTA open source, sub-200ms latency)
    HIGGS_AUDIO = "higgs_audio"  # Higgs Audio V2 (BosonAI, #1 HF TTS, GPU)
    ELEVENLABS = "elevenlabs"  # ElevenLabs (commercial, lowest latency)


@dataclass
class StreamingConfig:
    """Configuration for streaming TTS"""
    provider: TTSProvider = TTSProvider.EDGE
    voice: str = "af_heart"
    speed: float = 1.0
    # Provider-specific settings
    coqui_url: str = "http://localhost:5002"  # Coqui TTS server URL
    coqui_speaker: str = "default"
    coqui_language: str = "en"
    # Kokoro-82M settings (open source, fast, high quality)
    kokoro_url: str = "http://localhost:8880"  # Kokoro API server
    kokoro_voice: str = "af_heart"  # Kokoro voice preset
    # Piper settings (open source, fastest synthesis)
    piper_url: str = "http://localhost:8881"  # Piper API server
    piper_voice: str = "amy"  # Piper voice
    # Chatterbox settings (SoTA open source, sub-200ms latency)
    chatterbox_url: str = field(default_factory=lambda: os.getenv("CHATTERBOX_URL", "http://localhost:4123"))
    chatterbox_voice: str = "default"  # Chatterbox voice preset
    chatterbox_exaggeration: float = 0.5  # Emotion exaggeration (0-1)
    chatterbox_cfg: float = 0.5  # CFG scale
    # Higgs Audio V2 settings (GPU, #1 on HuggingFace)
    higgs_audio_url: str = field(default_factory=lambda: os.getenv("HIGGS_AUDIO_URL", "http://localhost:8000"))
    higgs_audio_voice: str = "af_heart"
    # ElevenLabs settings (commercial option)
    elevenlabs_api_key: Optional[str] = None
    elevenlabs_voice_id: str = "21m00Tcm4TlvDq8ikWAM"  # Rachel - default
    elevenlabs_model: str = "eleven_turbo_v2_5"  # Low latency model


class StreamingTTSProvider(ABC):
    """Base class for streaming TTS providers"""

    @abstractmethod
    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream audio bytes for the given text.

        Args:
            text: Text to synthesize
            speed: Speech speed (0.5 - 2.0)

        Yields:
            Audio bytes (MP3 format preferred)
        """
        pass

    @abstractmethod
    def set_voice(self, voice: str) -> bool:
        """Set the voice to use"""
        pass

    @abstractmethod
    def get_info(self) -> dict:
        """Get provider info"""
        pass

    @property
    @abstractmethod
    def supports_true_streaming(self) -> bool:
        """Whether provider supports true streaming (not just chunked HTTP)"""
        pass


class CoquiStreamingProvider(StreamingTTSProvider):
    """
    Coqui TTS / XTTS streaming provider.

    Self-hosted, open source TTS with excellent quality.
    Supports streaming via chunked HTTP response.

    To run Coqui TTS server:
        docker run --rm -it -p 5002:5002 --gpus all ghcr.io/coqui-ai/tts:latest

    Or install locally:
        pip install TTS
        tts-server --model_name tts_models/multilingual/multi-dataset/xtts_v2
    """

    # Coqui XTTS voices (speaker embeddings)
    COQUI_VOICES = {
        "default": {"name": "Default", "speaker": None},
        "female_1": {"name": "Female 1", "speaker": "female-en-5"},
        "female_2": {"name": "Female 2", "speaker": "female-en-6"},
        "male_1": {"name": "Male 1", "speaker": "male-en-7"},
        "male_2": {"name": "Male 2", "speaker": "male-en-8"},
    }

    def __init__(self, config: StreamingConfig):
        self.config = config
        self._voice = config.coqui_speaker
        self._available = None  # Will be checked on first use

    async def _check_availability(self) -> bool:
        """Check if Coqui TTS server is available"""
        if self._available is not None:
            return self._available

        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.config.coqui_url}/")
                self._available = response.status_code == 200
        except Exception as e:
            logger.warning(f"Coqui TTS server not available: {e}")
            self._available = False

        return self._available

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Stream audio from Coqui TTS server"""
        if not await self._check_availability():
            logger.error("Coqui TTS server not available")
            return

        try:
            import httpx

            # Use OpenAI-compatible endpoint with MP3 format for browser compatibility
            url = f"{self.config.coqui_url}/v1/audio/speech"

            payload = {
                "input": text,
                "voice": self._voice if self._voice != "default" else "af_heart",
                "response_format": "mp3",  # Request MP3 for browser compatibility
                "speed": speed,
            }

            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("POST", url, json=payload) as response:
                    if response.status_code != 200:
                        logger.error(f"Coqui TTS error: {response.status_code}")
                        return

                    # Stream audio chunks as they arrive
                    buffer = bytearray()
                    min_chunk_size = 4096  # 4KB chunks for smooth playback

                    async for chunk in response.aiter_bytes():
                        buffer.extend(chunk)
                        while len(buffer) >= min_chunk_size:
                            yield bytes(buffer[:min_chunk_size])
                            buffer = buffer[min_chunk_size:]

                    # Yield remaining data
                    if buffer:
                        yield bytes(buffer)

        except Exception as e:
            logger.error(f"Coqui streaming error: {e}")

    def set_voice(self, voice: str) -> bool:
        # Accept any voice - the Coqui server will map it to a VCTK speaker
        self._voice = voice
        return True

    def get_info(self) -> dict:
        return {
            "provider": "coqui",
            "available": self._available or False,
            "voice": self._voice,
            "voices": list(self.COQUI_VOICES.keys()),
            "server_url": self.config.coqui_url,
            "streaming": True,
        }

    @property
    def supports_true_streaming(self) -> bool:
        return True  # Coqui streams audio as it generates


class KokoroStreamingProvider(StreamingTTSProvider):
    """
    Kokoro-82M direct provider using kokoro-onnx.

    Open source (Apache 2.0), fast, and high quality TTS.
    No server required - runs directly in process.

    Install: pip install kokoro-onnx soundfile
    """

    # Kokoro v1.0 voices - 54 voices across 9 languages
    # voices-v1.0.bin includes: American, British, European, French, Hindi, Italian, Japanese, Portuguese, Chinese
    KOKORO_VOICES = {
        # American Female (11 voices)
        "af_heart": {"name": "Heart (American Female)", "id": "af_heart"},
        "af_alloy": {"name": "Alloy (American Female)", "id": "af_alloy"},
        "af_aoede": {"name": "Aoede (American Female)", "id": "af_aoede"},
        "af_bella": {"name": "Bella (American Female)", "id": "af_bella"},
        "af_jessica": {"name": "Jessica (American Female)", "id": "af_jessica"},
        "af_kore": {"name": "Kore (American Female)", "id": "af_kore"},
        "af_nicole": {"name": "Nicole (American Female)", "id": "af_nicole"},
        "af_nova": {"name": "Nova (American Female)", "id": "af_nova"},
        "af_river": {"name": "River (American Female)", "id": "af_river"},
        "af_sarah": {"name": "Sarah (American Female)", "id": "af_sarah"},
        "af_sky": {"name": "Sky (American Female)", "id": "af_sky"},
        # American Male (9 voices)
        "am_adam": {"name": "Adam (American Male)", "id": "am_adam"},
        "am_echo": {"name": "Echo (American Male)", "id": "am_echo"},
        "am_eric": {"name": "Eric (American Male)", "id": "am_eric"},
        "am_fenrir": {"name": "Fenrir (American Male)", "id": "am_fenrir"},
        "am_liam": {"name": "Liam (American Male)", "id": "am_liam"},
        "am_michael": {"name": "Michael (American Male)", "id": "am_michael"},
        "am_onyx": {"name": "Onyx (American Male)", "id": "am_onyx"},
        "am_puck": {"name": "Puck (American Male)", "id": "am_puck"},
        "am_santa": {"name": "Santa (American Male)", "id": "am_santa"},
        # British Female (4 voices)
        "bf_alice": {"name": "Alice (British Female)", "id": "bf_alice"},
        "bf_emma": {"name": "Emma (British Female)", "id": "bf_emma"},
        "bf_isabella": {"name": "Isabella (British Female)", "id": "bf_isabella"},
        "bf_lily": {"name": "Lily (British Female)", "id": "bf_lily"},
        # British Male (4 voices)
        "bm_daniel": {"name": "Daniel (British Male)", "id": "bm_daniel"},
        "bm_fable": {"name": "Fable (British Male)", "id": "bm_fable"},
        "bm_george": {"name": "George (British Male)", "id": "bm_george"},
        "bm_lewis": {"name": "Lewis (British Male)", "id": "bm_lewis"},
        # European (3 voices)
        "ef_dora": {"name": "Dora (European Female)", "id": "ef_dora"},
        "em_alex": {"name": "Alex (European Male)", "id": "em_alex"},
        "em_santa": {"name": "Santa (European Male)", "id": "em_santa"},
        # French (1 voice)
        "ff_siwis": {"name": "Siwis (French Female)", "id": "ff_siwis"},
        # Hindi (4 voices)
        "hf_alpha": {"name": "Alpha (Hindi Female)", "id": "hf_alpha"},
        "hf_beta": {"name": "Beta (Hindi Female)", "id": "hf_beta"},
        "hm_omega": {"name": "Omega (Hindi Male)", "id": "hm_omega"},
        "hm_psi": {"name": "Psi (Hindi Male)", "id": "hm_psi"},
        # Italian (2 voices)
        "if_sara": {"name": "Sara (Italian Female)", "id": "if_sara"},
        "im_nicola": {"name": "Nicola (Italian Male)", "id": "im_nicola"},
        # Japanese (5 voices)
        "jf_alpha": {"name": "Alpha (Japanese Female)", "id": "jf_alpha"},
        "jf_gongitsune": {"name": "Gongitsune (Japanese Female)", "id": "jf_gongitsune"},
        "jf_nezumi": {"name": "Nezumi (Japanese Female)", "id": "jf_nezumi"},
        "jf_tebukuro": {"name": "Tebukuro (Japanese Female)", "id": "jf_tebukuro"},
        "jm_kumo": {"name": "Kumo (Japanese Male)", "id": "jm_kumo"},
        # Portuguese (3 voices)
        "pf_dora": {"name": "Dora (Portuguese Female)", "id": "pf_dora"},
        "pm_alex": {"name": "Alex (Portuguese Male)", "id": "pm_alex"},
        "pm_santa": {"name": "Santa (Portuguese Male)", "id": "pm_santa"},
        # Chinese/Mandarin (8 voices)
        "zf_xiaobei": {"name": "Xiaobei (Chinese Female)", "id": "zf_xiaobei"},
        "zf_xiaoni": {"name": "Xiaoni (Chinese Female)", "id": "zf_xiaoni"},
        "zf_xiaoxiao": {"name": "Xiaoxiao (Chinese Female)", "id": "zf_xiaoxiao"},
        "zf_xiaoyi": {"name": "Xiaoyi (Chinese Female)", "id": "zf_xiaoyi"},
        "zm_yunjian": {"name": "Yunjian (Chinese Male)", "id": "zm_yunjian"},
        "zm_yunxi": {"name": "Yunxi (Chinese Male)", "id": "zm_yunxi"},
        "zm_yunxia": {"name": "Yunxia (Chinese Male)", "id": "zm_yunxia"},
        "zm_yunyang": {"name": "Yunyang (Chinese Male)", "id": "zm_yunyang"},
    }

    # Singleton instance for model (lazy loaded)
    _kokoro_instance = None
    _kokoro_available = None

    def __init__(self, config: StreamingConfig):
        self.config = config
        self._voice = config.kokoro_voice

    @classmethod
    def _get_kokoro(cls):
        """Lazy load Kokoro model (singleton)"""
        if cls._kokoro_instance is None:
            try:
                from kokoro_onnx import Kokoro
                import os
                # Get model directory (relative to this file)
                model_dir = os.path.join(os.path.dirname(__file__), 'models')

                # Prefer v1.0 model files (54 voices), fallback to legacy files
                model_path_v1 = os.path.join(model_dir, 'kokoro-v1.0.int8.onnx')
                voices_path_v1 = os.path.join(model_dir, 'voices-v1.0.bin')
                model_path_legacy = os.path.join(model_dir, 'kokoro-quant.onnx')
                voices_path_legacy = os.path.join(model_dir, 'voices.bin')

                # Use v1.0 files if available, otherwise fallback to legacy
                if os.path.exists(model_path_v1) and os.path.exists(voices_path_v1):
                    model_path = model_path_v1
                    voices_path = voices_path_v1
                    logger.info("Using Kokoro v1.0 model with 54 voices")
                elif os.path.exists(model_path_legacy) and os.path.exists(voices_path_legacy):
                    model_path = model_path_legacy
                    voices_path = voices_path_legacy
                    logger.info("Using legacy Kokoro model with 11 voices")
                else:
                    logger.warning(f"Kokoro model files not found in {model_dir}")
                    cls._kokoro_available = False
                    return None

                cls._kokoro_instance = Kokoro(model_path, voices_path)
                cls._kokoro_available = True
                logger.info("Kokoro-82M model loaded successfully")
            except Exception as e:
                logger.warning(f"Failed to load Kokoro model: {e}")
                cls._kokoro_available = False
        return cls._kokoro_instance

    async def _check_availability(self) -> bool:
        """Check if Kokoro is available"""
        if KokoroStreamingProvider._kokoro_available is not None:
            return KokoroStreamingProvider._kokoro_available

        # Try to load the model
        self._get_kokoro()
        return KokoroStreamingProvider._kokoro_available or False

    def _preprocess_text_for_kokoro(self, text: str) -> str:
        """
        Preprocess text to avoid Kokoro phonemizer errors.

        Handles:
        - Newlines causing line count mismatch
        - Special characters causing UTF-8 encoding issues
        - Empty or whitespace-only text
        """
        import re

        if not text or not text.strip():
            return ""

        # Replace newlines with spaces (prevents line count mismatch)
        text = text.replace('\n', ' ').replace('\r', ' ')

        # Remove or replace problematic characters that can cause UTF-8 issues
        # These include certain Unicode characters that the phonemizer can't handle
        problematic_chars = {
            '"': '"',  # Smart quotes
            '"': '"',
            ''': "'",
            ''': "'",
            '–': '-',  # En dash
            '—': '-',  # Em dash
            '…': '...',  # Ellipsis
            '•': ',',  # Bullet
            '©': '',
            '®': '',
            '™': '',
            '°': ' degrees ',
            '±': ' plus or minus ',
            '×': ' times ',
            '÷': ' divided by ',
            '≈': ' approximately ',
            '≠': ' not equal to ',
            '≤': ' less than or equal to ',
            '≥': ' greater than or equal to ',
            '→': ' to ',
            '←': ' from ',
            '↑': ' up ',
            '↓': ' down ',
        }

        for char, replacement in problematic_chars.items():
            text = text.replace(char, replacement)

        # Remove any remaining non-ASCII characters that might cause issues
        # Keep only printable ASCII and common punctuation
        text = re.sub(r'[^\x20-\x7E]', '', text)

        # Replace tabs and other whitespace with spaces
        text = text.replace('\t', ' ').replace('\f', ' ').replace('\v', ' ')

        # Remove or simplify potential multi-line triggers
        # Replace semicolons and colons that might cause phonemizer issues
        text = re.sub(r'[;:]', ',', text)

        # Remove URLs (they cause phonemizer issues)
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'www\.\S+', '', text)

        # Remove code-like content (backticks, brackets with content)
        text = re.sub(r'`[^`]*`', '', text)
        text = re.sub(r'\[[^\]]*\]', '', text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        # If text is too short after cleanup, skip it
        if len(text) < 3:
            return ""

        # Ensure text ends with punctuation for natural TTS
        if text and text[-1] not in '.!?':
            text += '.'

        return text

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Generate audio using Kokoro-82M directly"""
        kokoro = self._get_kokoro()
        if kokoro is None:
            logger.error("Kokoro model not available")
            return

        # Preprocess text to avoid phonemizer errors
        text = self._preprocess_text_for_kokoro(text)
        if not text:
            logger.warning("Kokoro: Empty text after preprocessing, skipping")
            return

        try:
            import io
            import soundfile as sf

            # Map frontend voice ID to actual Kokoro model voice ID
            voice_config = self.KOKORO_VOICES.get(self._voice)
            actual_voice = voice_config["id"] if voice_config else "af"  # Default to 'af'
            logger.debug(f"Kokoro synthesis: voice={self._voice} -> actual={actual_voice}, text_len={len(text)}")

            # Generate audio (runs in thread pool to not block)
            loop = asyncio.get_event_loop()
            audio, sample_rate = await loop.run_in_executor(
                None,
                lambda: kokoro.create(text, voice=actual_voice, speed=speed)
            )

            # Convert to MP3 for browser compatibility
            # First write to WAV buffer, then convert
            wav_buffer = io.BytesIO()
            sf.write(wav_buffer, audio, sample_rate, format='WAV')
            wav_buffer.seek(0)

            # Try to convert to MP3 using pydub if available
            try:
                from pydub import AudioSegment
                audio_segment = AudioSegment.from_wav(wav_buffer)
                mp3_buffer = io.BytesIO()
                audio_segment.export(mp3_buffer, format='mp3', bitrate='128k')
                mp3_buffer.seek(0)
                audio_data = mp3_buffer.read()
            except ImportError:
                # Fallback to WAV if pydub not available
                logger.warning("pydub not available, using WAV format")
                wav_buffer.seek(0)
                audio_data = wav_buffer.read()

            # Yield complete audio as single chunk for proper decoding
            # MP3 files must be complete for browser decodeAudioData() to work
            yield audio_data

        except Exception as e:
            error_msg = str(e)
            # If phonemizer line count error, try with simplified text
            if "lines in input and output must be equal" in error_msg or "phonemizer" in error_msg.lower():
                logger.warning(f"Kokoro phonemizer error, retrying with simplified text: {e}")
                try:
                    import re
                    # Ultra-simple text: only letters, numbers, spaces, and periods
                    simple_text = re.sub(r'[^a-zA-Z0-9\s.]', '', text)
                    simple_text = re.sub(r'\s+', ' ', simple_text).strip()
                    if simple_text and len(simple_text) >= 3:
                        if not simple_text.endswith('.'):
                            simple_text += '.'
                        voice_config = self.KOKORO_VOICES.get(self._voice)
                        actual_voice = voice_config["id"] if voice_config else "af"
                        loop = asyncio.get_event_loop()
                        audio, sample_rate = await loop.run_in_executor(
                            None,
                            lambda: kokoro.create(simple_text, voice=actual_voice, speed=speed)
                        )
                        wav_buffer = io.BytesIO()
                        sf.write(wav_buffer, audio, sample_rate, format='WAV')
                        wav_buffer.seek(0)
                        try:
                            from pydub import AudioSegment
                            audio_segment = AudioSegment.from_wav(wav_buffer)
                            mp3_buffer = io.BytesIO()
                            audio_segment.export(mp3_buffer, format='mp3', bitrate='128k')
                            mp3_buffer.seek(0)
                            audio_data = mp3_buffer.read()
                        except ImportError:
                            wav_buffer.seek(0)
                            audio_data = wav_buffer.read()
                        # Yield complete audio for proper decoding
                        yield audio_data
                        return
                except Exception as retry_error:
                    logger.error(f"Kokoro retry also failed: {retry_error}")
            else:
                logger.error(f"Kokoro synthesis error: {e}")

    def set_voice(self, voice: str) -> bool:
        if voice in self.KOKORO_VOICES:
            self._voice = voice
            return True
        # Also accept edge TTS voice IDs that match
        if voice.startswith(("af_", "am_", "bf_", "bm_")):
            self._voice = voice
            return True
        return False

    def get_info(self) -> dict:
        return {
            "provider": "kokoro",
            "available": KokoroStreamingProvider._kokoro_available or False,
            "voice": self._voice,
            "voices": list(self.KOKORO_VOICES.keys()),
            "streaming": False,  # Not true streaming, generates full audio then chunks
            "model": "kokoro-82m-onnx",
        }

    @property
    def supports_true_streaming(self) -> bool:
        return False  # Generates full audio then streams chunks


class PiperStreamingProvider(StreamingTTSProvider):
    """
    Piper TTS streaming provider.

    Open source (MIT), extremely fast TTS.
    Synthesis typically completes in <200ms for most sentences.

    To run Piper server:
        python -m app.voice.piper_server

    API compatible with OpenAI TTS format.
    Supports Edge TTS voice ID mapping for seamless settings integration.
    """

    # Piper voices with Edge TTS ID mapping
    # The server maps Edge TTS IDs (af_heart, am_adam, etc.) to installed Piper voices
    PIPER_VOICES = {
        # Direct Piper voices
        "en_US-amy-medium": {"name": "Amy (US Female)", "id": "en_US-amy-medium"},
        "en_US-lessac-medium": {"name": "Lessac (US Female)", "id": "en_US-lessac-medium"},
        "en_US-ryan-medium": {"name": "Ryan (US Male)", "id": "en_US-ryan-medium"},
        "en_US-joe-medium": {"name": "Joe (US Male)", "id": "en_US-joe-medium"},
        "en_GB-alba-medium": {"name": "Alba (British Female)", "id": "en_GB-alba-medium"},
        "en_GB-semaine-medium": {"name": "Semaine (British Male)", "id": "en_GB-semaine-medium"},
        # Edge TTS ID mappings (frontend uses these)
        "af_heart": {"name": "Jenny (Heart)", "id": "af_heart", "maps_to": "en_US-amy-medium"},
        "af_bella": {"name": "Aria (Bella)", "id": "af_bella", "maps_to": "en_US-lessac-medium"},
        "af_sarah": {"name": "Ava (Sarah)", "id": "af_sarah", "maps_to": "en_US-amy-medium"},
        "am_adam": {"name": "Guy (Adam)", "id": "am_adam", "maps_to": "en_US-ryan-medium"},
        "am_michael": {"name": "Christopher (Michael)", "id": "am_michael", "maps_to": "en_US-joe-medium"},
        "am_brian": {"name": "Brian", "id": "am_brian", "maps_to": "en_US-joe-medium"},
        "bf_emma": {"name": "Sonia (Emma)", "id": "bf_emma", "maps_to": "en_GB-alba-medium"},
        "bf_isabella": {"name": "Libby (Isabella)", "id": "bf_isabella", "maps_to": "en_GB-alba-medium"},
        "bm_george": {"name": "Ryan (George)", "id": "bm_george", "maps_to": "en_GB-semaine-medium"},
        "bm_lewis": {"name": "Thomas (Lewis)", "id": "bm_lewis", "maps_to": "en_GB-semaine-medium"},
        "default": {"name": "Default", "id": "amy", "maps_to": "en_US-amy-medium"},
    }

    def __init__(self, config: StreamingConfig):
        self.config = config
        self._voice = config.piper_voice
        self._available = None

    async def _check_availability(self) -> bool:
        """Check if Piper server is available"""
        if self._available is not None:
            return self._available

        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.config.piper_url}/")
                self._available = response.status_code == 200
        except Exception as e:
            logger.warning(f"Piper server not available: {e}")
            self._available = False

        return self._available

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Stream audio from Piper server using OpenAI-compatible API"""
        if not await self._check_availability():
            logger.error("Piper server not available")
            return

        try:
            import httpx

            url = f"{self.config.piper_url}/v1/audio/speech"

            payload = {
                "model": "piper",
                "input": text,
                "voice": self._voice,
                "response_format": "mp3",
                "speed": speed,
            }

            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("POST", url, json=payload) as response:
                    if response.status_code != 200:
                        logger.error(f"Piper error: {response.status_code}")
                        return

                    # Stream audio chunks
                    buffer = bytearray()
                    min_chunk_size = 4096  # 4KB for smooth playback

                    async for chunk in response.aiter_bytes():
                        buffer.extend(chunk)
                        while len(buffer) >= min_chunk_size:
                            yield bytes(buffer[:min_chunk_size])
                            buffer = buffer[min_chunk_size:]

                    # Yield remaining
                    if buffer:
                        yield bytes(buffer)

        except Exception as e:
            logger.error(f"Piper streaming error: {e}")

    def set_voice(self, voice: str) -> bool:
        """Set voice - accepts both Edge TTS IDs and Piper voice names"""
        if voice in self.PIPER_VOICES:
            self._voice = voice
            return True
        # Accept Edge TTS voice patterns (af_, am_, bf_, bm_, etc.)
        if voice.startswith(("af_", "am_", "bf_", "bm_", "au_", "ca_", "in_", "ie_", "nz_", "sg_", "za_")):
            self._voice = voice  # Server will handle the mapping
            return True
        # Accept Piper voice names directly
        if voice.startswith("en_"):
            self._voice = voice
            return True
        # Default fallback
        self._voice = "af_heart"  # Will map to amy on the server
        return True

    def get_info(self) -> dict:
        return {
            "provider": "piper",
            "available": self._available or False,
            "voice": self._voice,
            "voices": list(self.PIPER_VOICES.keys()),
            "server_url": self.config.piper_url,
            "streaming": True,
        }

    @property
    def supports_true_streaming(self) -> bool:
        return True  # Piper is so fast it feels like streaming


class ChatterboxStreamingProvider(StreamingTTSProvider):
    """
    Chatterbox TTS streaming provider with voice cloning.

    State-of-the-art open source (MIT) TTS from Resemble AI.
    Features emotion control and voice cloning via reference audio.

    Voice cloning flow:
    1. User selects a voice (e.g., af_heart → Jenny)
    2. A reference audio clip is generated via Edge TTS and cached
    3. The cached clip is sent to Chatterbox's /v1/audio/speech/upload
       multipart endpoint for per-request voice cloning
    4. Result: Chatterbox speaks with the identity of the selected voice

    Server: travisvn/chatterbox-tts-api:cpu on port 4123
    """

    def __init__(self, config: StreamingConfig):
        self.config = config
        self._voice = config.chatterbox_voice
        self._available = None
        self._exaggeration = config.chatterbox_exaggeration
        self._cfg = config.chatterbox_cfg

    async def _check_availability(self) -> bool:
        """Check if Chatterbox server is available and running on GPU"""
        if self._available is not None:
            return self._available

        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.config.chatterbox_url}/health")
                if response.status_code == 200:
                    data = response.json()
                    device = data.get("device", "cpu")
                    if device == "cpu":
                        logger.warning("Chatterbox is running on CPU — too slow for real-time TTS, skipping")
                        self._available = False
                    else:
                        logger.info(f"Chatterbox available on {device}")
                        self._available = True
                else:
                    self._available = False
        except Exception as e:
            logger.warning(f"Chatterbox server not available: {e}")
            self._available = False

        return self._available

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Stream audio from Chatterbox with voice cloning via upload endpoint"""
        if not await self._check_availability():
            logger.error("Chatterbox server not available")
            return

        try:
            import httpx
            from .sample_generator import get_voice_sample

            sample_path = await get_voice_sample(self._voice)

            if sample_path:
                # PRIMARY: Upload endpoint with voice cloning
                url = f"{self.config.chatterbox_url}/v1/audio/speech/upload"
                logger.info(f"Chatterbox: Using voice sample '{self._voice}' via upload endpoint")

                form_data = {
                    "input": text,
                    "exaggeration": str(self._exaggeration),
                    "cfg_weight": str(self._cfg),
                }

                async with httpx.AsyncClient(timeout=120.0) as client:
                    with open(sample_path, "rb") as f:
                        files = {"voice_file": ("voice.mp3", f, "audio/mpeg")}
                        async with client.stream(
                            "POST", url, data=form_data, files=files
                        ) as response:
                            if response.status_code != 200:
                                error_text = await response.aread()
                                logger.error(f"Chatterbox upload error: {response.status_code} - {error_text}")
                                return

                            # Stream audio chunks (4KB buffered)
                            buffer = bytearray()
                            min_chunk_size = 4096

                            async for chunk in response.aiter_bytes():
                                buffer.extend(chunk)
                                while len(buffer) >= min_chunk_size:
                                    yield bytes(buffer[:min_chunk_size])
                                    buffer = buffer[min_chunk_size:]

                            if buffer:
                                yield bytes(buffer)
            else:
                # FALLBACK: JSON endpoint with default voice (no sample available)
                url = f"{self.config.chatterbox_url}/v1/audio/speech"
                logger.info(f"Chatterbox: No voice sample for '{self._voice}', using default voice")

                payload = {
                    "model": "chatterbox",
                    "input": text,
                    "voice": self._voice,
                    "response_format": "wav",
                    "speed": speed,
                    "exaggeration": self._exaggeration,
                    "cfg_weight": self._cfg,
                }

                async with httpx.AsyncClient(timeout=120.0) as client:
                    async with client.stream("POST", url, json=payload) as response:
                        if response.status_code != 200:
                            error_text = await response.aread()
                            logger.error(f"Chatterbox error: {response.status_code} - {error_text}")
                            return

                        buffer = bytearray()
                        min_chunk_size = 4096

                        async for chunk in response.aiter_bytes():
                            buffer.extend(chunk)
                            while len(buffer) >= min_chunk_size:
                                yield bytes(buffer[:min_chunk_size])
                                buffer = buffer[min_chunk_size:]

                        if buffer:
                            yield bytes(buffer)

        except Exception as e:
            logger.error(f"Chatterbox streaming error: {e}")

    def set_voice(self, voice: str) -> bool:
        """Set voice - accepts any voice ID that has an Edge TTS mapping"""
        self._voice = voice
        return True

    def set_emotion(self, exaggeration: float = 0.5) -> None:
        """Set emotion exaggeration level (0-1)"""
        self._exaggeration = max(0.0, min(1.0, exaggeration))

    def get_info(self) -> dict:
        from .tts import AVAILABLE_VOICES
        return {
            "provider": "chatterbox",
            "available": self._available or False,
            "voice": self._voice,
            "voices": list(AVAILABLE_VOICES.keys()),
            "server_url": self.config.chatterbox_url,
            "streaming": True,
            "exaggeration": self._exaggeration,
            "cfg_weight": self._cfg,
            "features": ["emotion_control", "voice_cloning"],
        }

    @property
    def supports_true_streaming(self) -> bool:
        return True  # Chatterbox supports true streaming


class HiggsAudioStreamingProvider(StreamingTTSProvider):
    """
    Higgs Audio V2 streaming provider (BosonAI).

    #1 trending TTS model on HuggingFace.
    Built on Llama 3.2 3B with 10M+ hours of training data.
    OpenAI-compatible /v1/audio/speech endpoint.

    Voice cloning flow (same as Chatterbox):
    1. Edge TTS voice samples are pre-generated at container startup
    2. vLLM loads them as voice presets via --voice-presets-dir
    3. Requests use the voice ID directly (e.g. "af_heart")

    Output: raw PCM (s16le, 24kHz, mono) — converted to MP3 in-process.

    Requires NVIDIA GPU (24GB VRAM full, 8GB quantized).
    """

    # 33 built-in Higgs Audio character voices
    BUILTIN_VOICES = {
        "en_man", "en_woman", "belinda", "chadwick", "mabel", "vex", "shrek",
        "carla", "curt", "mona", "reggie", "tilly", "brock", "daphne",
        "fern", "gus", "hazel", "ivy", "jasper", "kira", "leo",
        "nina", "otto", "pearl", "quinn", "rex", "sage", "tara",
        "una", "vince", "wren", "xena", "yuri",
    }

    def __init__(self, config: StreamingConfig):
        self.config = config
        self._voice = config.higgs_audio_voice
        self._available = None

    async def _check_availability(self) -> bool:
        """Check if Higgs Audio vLLM server is available"""
        if self._available is not None:
            return self._available

        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.config.higgs_audio_url}/health")
                self._available = response.status_code == 200
                if self._available:
                    logger.info("Higgs Audio V2 server available")
        except Exception as e:
            logger.warning(f"Higgs Audio server not available: {e}")
            self._available = False

        return self._available

    @staticmethod
    def _pcm_to_wav(pcm_data: bytes, sample_rate: int = 24000, channels: int = 1, sample_width: int = 2) -> bytes:
        """Convert raw PCM bytes to WAV by prepending a 44-byte header."""
        import struct
        data_size = len(pcm_data)
        header = struct.pack(
            '<4sI4s4sIHHIIHH4sI',
            b'RIFF',
            36 + data_size,
            b'WAVE',
            b'fmt ',
            16,                                    # chunk size
            1,                                     # PCM format
            channels,
            sample_rate,
            sample_rate * channels * sample_width,  # byte rate
            channels * sample_width,                # block align
            sample_width * 8,                       # bits per sample
            b'data',
            data_size,
        )
        return header + pcm_data

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Synthesize audio via Higgs Audio V2 OpenAI-compatible endpoint."""
        if not await self._check_availability():
            logger.error("Higgs Audio server not available")
            return

        try:
            import httpx

            url = f"{self.config.higgs_audio_url}/v1/audio/speech"
            payload = {
                "input": text,
                "voice": self._voice,
                "response_format": "pcm",
                "speed": speed,
            }

            # Collect full PCM response (vLLM generates all at once)
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(url, json=payload)

                if response.status_code != 200:
                    logger.error(f"Higgs Audio error: {response.status_code} - {response.text[:200]}")
                    return

                pcm_data = response.content

            if not pcm_data:
                logger.warning("Higgs Audio returned empty response")
                return

            # Convert PCM → WAV → MP3
            wav_data = self._pcm_to_wav(pcm_data)

            try:
                import io
                from pydub import AudioSegment
                wav_buffer = io.BytesIO(wav_data)
                audio_segment = AudioSegment.from_wav(wav_buffer)
                mp3_buffer = io.BytesIO()
                audio_segment.export(mp3_buffer, format='mp3', bitrate='128k')
                mp3_buffer.seek(0)
                audio_data = mp3_buffer.read()
            except ImportError:
                logger.warning("pydub not available, serving WAV instead of MP3")
                audio_data = wav_data

            # Yield complete audio as single chunk for proper browser decoding
            yield audio_data

        except Exception as e:
            logger.error(f"Higgs Audio streaming error: {e}")

    def set_voice(self, voice: str) -> bool:
        """Accept any voice ID — built-in or Edge TTS preset."""
        self._voice = voice
        return True

    def get_info(self) -> dict:
        from .tts import AVAILABLE_VOICES
        return {
            "provider": "higgs_audio",
            "available": self._available or False,
            "voice": self._voice,
            "voices": list(AVAILABLE_VOICES.keys()) + list(self.BUILTIN_VOICES),
            "server_url": self.config.higgs_audio_url,
            "streaming": False,
            "model": "higgs-audio-v2-generation-3B-base",
            "features": ["voice_cloning", "builtin_characters"],
        }

    @property
    def supports_true_streaming(self) -> bool:
        return False  # Generates full audio then returns


class ElevenLabsStreamingProvider(StreamingTTSProvider):
    """
    ElevenLabs streaming provider.

    Commercial API with lowest latency WebSocket streaming.
    Requires API key (paid plans).

    Features:
    - True WebSocket streaming with ~200ms latency
    - High quality neural voices
    - Voice cloning capabilities
    """

    ELEVENLABS_VOICES = {
        "rachel": {"id": "21m00Tcm4TlvDq8ikWAM", "name": "Rachel"},
        "domi": {"id": "AZnzlk1XvdvUeBnXmlld", "name": "Domi"},
        "bella": {"id": "EXAVITQu4vr4xnSDxMaL", "name": "Bella"},
        "antoni": {"id": "ErXwobaYiN019PkySvjV", "name": "Antoni"},
        "elli": {"id": "MF3mGyEYCl7XYWbV9V6O", "name": "Elli"},
        "josh": {"id": "TxGEqnHWrfWFTfGW9XjX", "name": "Josh"},
        "arnold": {"id": "VR6AewLTigWG4xSOukaG", "name": "Arnold"},
        "adam": {"id": "pNInz6obpgDQGcFmaJgB", "name": "Adam"},
        "sam": {"id": "yoZ06aMxZJJ28mfd3POQ", "name": "Sam"},
    }

    def __init__(self, config: StreamingConfig):
        self.config = config
        self._voice_id = config.elevenlabs_voice_id
        self._api_key = config.elevenlabs_api_key or os.getenv("ELEVENLABS_API_KEY")

    @property
    def is_available(self) -> bool:
        return bool(self._api_key)

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Stream audio from ElevenLabs WebSocket API"""
        if not self._api_key:
            logger.error("ElevenLabs API key not configured")
            return

        try:
            import websockets
            import json

            # ElevenLabs streaming WebSocket URL
            url = f"wss://api.elevenlabs.io/v1/text-to-speech/{self._voice_id}/stream-input?model_id={self.config.elevenlabs_model}"

            async with websockets.connect(
                url,
                additional_headers={"xi-api-key": self._api_key}
            ) as ws:
                # Send initial config
                await ws.send(json.dumps({
                    "text": " ",  # Initial text to start the stream
                    "voice_settings": {
                        "stability": 0.5,
                        "similarity_boost": 0.75,
                    },
                    "generation_config": {
                        "chunk_length_schedule": [120, 160, 250, 290]
                    }
                }))

                # Send the actual text
                await ws.send(json.dumps({
                    "text": text,
                    "try_trigger_generation": True,
                }))

                # Signal end of input
                await ws.send(json.dumps({
                    "text": "",
                }))

                # Receive audio chunks
                async for message in ws:
                    try:
                        data = json.loads(message)
                        if "audio" in data:
                            import base64
                            audio_bytes = base64.b64decode(data["audio"])
                            yield audio_bytes
                        elif data.get("isFinal"):
                            break
                    except json.JSONDecodeError:
                        # Binary data
                        yield message

        except ImportError:
            logger.error("websockets package required for ElevenLabs streaming")
        except Exception as e:
            logger.error(f"ElevenLabs streaming error: {e}")

    def set_voice(self, voice: str) -> bool:
        if voice in self.ELEVENLABS_VOICES:
            self._voice_id = self.ELEVENLABS_VOICES[voice]["id"]
            return True
        # Also accept voice IDs directly
        if len(voice) > 10:  # Looks like a voice ID
            self._voice_id = voice
            return True
        return False

    def get_info(self) -> dict:
        return {
            "provider": "elevenlabs",
            "available": self.is_available,
            "voice_id": self._voice_id,
            "voices": list(self.ELEVENLABS_VOICES.keys()),
            "streaming": True,
            "model": self.config.elevenlabs_model,
        }

    @property
    def supports_true_streaming(self) -> bool:
        return True  # WebSocket-based true streaming


class EdgeStreamingProvider(StreamingTTSProvider):
    """
    Edge TTS streaming provider.

    Wraps the existing Edge TTS with streaming interface.
    Not true streaming - synthesizes full text then streams chunks.
    """

    def __init__(self, config: StreamingConfig):
        self.config = config
        from .tts import get_tts_service, AVAILABLE_VOICES
        self._tts = get_tts_service(config.voice)
        self._voices = AVAILABLE_VOICES

    async def stream_audio(
        self,
        text: str,
        speed: float = 1.0
    ) -> AsyncGenerator[bytes, None]:
        """Stream audio from Edge TTS"""
        async for chunk in self._tts.synthesize_streaming(text, speed=speed):
            yield chunk

    def set_voice(self, voice: str) -> bool:
        return self._tts.set_voice(voice)

    def get_info(self) -> dict:
        return {
            "provider": "edge",
            "available": self._tts.is_available,
            "voice": self._tts.voice,
            "voices": list(self._voices.keys()),
            "streaming": True,  # True streaming with buffered 4KB chunks
        }

    @property
    def supports_true_streaming(self) -> bool:
        return True  # Now streams 4KB chunks as they arrive from edge-tts


def get_streaming_provider(config: Optional[StreamingConfig] = None) -> StreamingTTSProvider:
    """
    Get the appropriate streaming TTS provider.

    Priority:
    1. ElevenLabs (if API key available) - lowest latency
    2. Kokoro-82M (if server available) - open source, fast, high quality
    3. Coqui XTTS (if server available) - open source, highest quality
    4. Edge TTS (fallback) - free, always available
    """
    if config is None:
        config = StreamingConfig()

    # Check explicit provider preference
    if config.provider == TTSProvider.ELEVENLABS:
        provider = ElevenLabsStreamingProvider(config)
        if provider.is_available:
            logger.info("Using ElevenLabs streaming provider")
            return provider
        logger.warning("ElevenLabs not available (no API key), falling back")

    if config.provider == TTSProvider.KOKORO:
        provider = KokoroStreamingProvider(config)
        logger.info("Using Kokoro-82M streaming provider")
        return provider

    if config.provider == TTSProvider.PIPER:
        provider = PiperStreamingProvider(config)
        logger.info("Using Piper streaming provider")
        return provider

    if config.provider == TTSProvider.CHATTERBOX:
        provider = ChatterboxStreamingProvider(config)
        logger.info("Using Chatterbox streaming provider (SoTA, sub-200ms)")
        return provider

    if config.provider == TTSProvider.HIGGS_AUDIO:
        provider = HiggsAudioStreamingProvider(config)
        logger.info("Using Higgs Audio V2 streaming provider (GPU, #1 HF)")
        return provider

    if config.provider == TTSProvider.COQUI:
        provider = CoquiStreamingProvider(config)
        logger.info("Using Coqui XTTS streaming provider")
        return provider

    # Default to Edge TTS
    logger.info("Using Edge TTS streaming provider")
    return EdgeStreamingProvider(config)


async def auto_select_provider(config: Optional[StreamingConfig] = None) -> StreamingTTSProvider:
    """
    Automatically select the best available provider.

    Priority order (free providers only):
    1. Higgs Audio V2 (GPU, #1 on HuggingFace, best quality)
    2. Chatterbox (SoTA, sub-200ms, true streaming)
    3. Kokoro-82M (offline, excellent neural quality)
    4. Edge TTS (always available with internet, true streaming)

    Note: ElevenLabs (paid) is only used if explicitly requested via provider parameter.
    """
    if config is None:
        config = StreamingConfig()

    # 1. Higgs Audio V2 (GPU, #1 on HuggingFace, best quality)
    higgs = HiggsAudioStreamingProvider(config)
    if await higgs._check_availability():
        logger.info("Auto-selected: Higgs Audio V2 (GPU, #1 HF)")
        return higgs

    # 2. Chatterbox (SoTA, sub-200ms, true streaming)
    chatterbox = ChatterboxStreamingProvider(config)
    if await chatterbox._check_availability():
        logger.info("Auto-selected: Chatterbox (SoTA, sub-200ms)")
        return chatterbox

    # 2. Kokoro (offline, excellent neural quality)
    kokoro = KokoroStreamingProvider(config)
    if await kokoro._check_availability():
        logger.info("Auto-selected: Kokoro-82M (offline neural)")
        return kokoro

    # 3. Edge TTS (always available with internet)
    logger.info("Auto-selected: Edge TTS (streaming fallback)")
    return EdgeStreamingProvider(config)

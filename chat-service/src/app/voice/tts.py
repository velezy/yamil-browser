"""
Text-to-Speech with Microsoft Edge Neural Voices

High-quality neural TTS using edge-tts (Microsoft's free neural voices).

Features:
- Multiple natural-sounding voice options
- Speed control
- MP3 output for browser compatibility
- No API keys required
"""

import asyncio
import io
import logging
from typing import Optional, AsyncGenerator
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Try to import edge-tts
try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    EDGE_TTS_AVAILABLE = False
    logger.warning("edge-tts not available. Install with: pip install edge-tts")


# Available voices - Microsoft Edge Neural voices (74 languages, 322+ voices)
AVAILABLE_VOICES = {
    # ==================== ENGLISH ====================
    # American Female (7)
    'af_heart': 'en-US-JennyNeural',      # Friendly, warm (default)
    'af_bella': 'en-US-AriaNeural',       # Professional
    'af_sarah': 'en-US-AvaNeural',        # Natural, expressive
    'af_sky': 'en-US-MichelleNeural',     # Clear
    'af_emma': 'en-US-EmmaNeural',        # Conversational
    'af_ana': 'en-US-AnaNeural',          # Young
    'af_nicole': 'en-US-AvaMultilingualNeural',  # Multilingual
    # American Male (6)
    'am_adam': 'en-US-GuyNeural',         # Casual
    'am_michael': 'en-US-ChristopherNeural',  # Professional
    'am_echo': 'en-US-EricNeural',        # Clear
    'am_brian': 'en-US-BrianNeural',      # Natural
    'am_andrew': 'en-US-AndrewNeural',    # Warm
    'am_roger': 'en-US-RogerNeural',      # Mature
    # British Female (4)
    'bf_emma': 'en-GB-SoniaNeural',       # British accent
    'bf_isabella': 'en-GB-LibbyNeural',   # Young British
    'bf_maisie': 'en-GB-MaisieNeural',    # Friendly British
    # British Male (2)
    'bm_george': 'en-GB-RyanNeural',      # British male
    'bm_lewis': 'en-GB-ThomasNeural',     # Formal British
    # Australian (2)
    'au_natasha': 'en-AU-NatashaNeural',  # Australian Female
    'au_william': 'en-AU-WilliamMultilingualNeural',  # Australian Male
    # Canadian (2)
    'ca_clara': 'en-CA-ClaraNeural',      # Canadian Female
    'ca_liam': 'en-CA-LiamNeural',        # Canadian Male
    # Indian (3)
    'in_neerja': 'en-IN-NeerjaNeural',    # Indian Female
    'in_neerja_exp': 'en-IN-NeerjaExpressiveNeural',  # Indian Female Expressive
    'in_prabhat': 'en-IN-PrabhatNeural',  # Indian Male
    # Irish (2)
    'ie_emily': 'en-IE-EmilyNeural',      # Irish Female
    'ie_connor': 'en-IE-ConnorNeural',    # Irish Male
    # New Zealand (2)
    'nz_molly': 'en-NZ-MollyNeural',      # NZ Female
    'nz_mitchell': 'en-NZ-MitchellNeural',  # NZ Male
    # Singapore (2)
    'sg_luna': 'en-SG-LunaNeural',        # Singapore Female
    'sg_wayne': 'en-SG-WayneNeural',      # Singapore Male
    # South African (2)
    'za_leah': 'en-ZA-LeahNeural',        # SA Female
    'za_luke': 'en-ZA-LukeNeural',        # SA Male

    # ==================== SPANISH ====================
    # Spanish (Spain)
    'es_elvira': 'es-ES-ElviraNeural',    # Spanish Female
    'es_alvaro': 'es-ES-AlvaroNeural',    # Spanish Male
    # Spanish (Mexico)
    'es_mx_dalia': 'es-MX-DaliaNeural',   # Mexican Female
    'es_mx_jorge': 'es-MX-JorgeNeural',   # Mexican Male
    # Spanish (Argentina)
    'es_ar_elena': 'es-AR-ElenaNeural',   # Argentine Female
    'es_ar_tomas': 'es-AR-TomasNeural',   # Argentine Male
    # Spanish (Colombia)
    'es_co_salome': 'es-CO-SalomeNeural', # Colombian Female
    'es_co_gonzalo': 'es-CO-GonzaloNeural', # Colombian Male

    # ==================== FRENCH ====================
    # French (France)
    'fr_denise': 'fr-FR-DeniseNeural',    # French Female
    'fr_henri': 'fr-FR-HenriNeural',      # French Male
    'fr_vivienne': 'fr-FR-VivienneMultilingualNeural', # French Female Multilingual
    # French (Canada)
    'fr_ca_sylvie': 'fr-CA-SylvieNeural', # Quebec Female
    'fr_ca_antoine': 'fr-CA-AntoineNeural', # Quebec Male
    'fr_ca_jean': 'fr-CA-JeanNeural',     # Quebec Male 2

    # ==================== GERMAN ====================
    'de_katja': 'de-DE-KatjaNeural',      # German Female
    'de_conrad': 'de-DE-ConradNeural',    # German Male
    'de_amala': 'de-DE-AmalaNeural',      # German Female 2
    'de_seraphina': 'de-DE-SeraphinaMultilingualNeural', # German Female Multilingual

    # ==================== ITALIAN ====================
    'it_elsa': 'it-IT-ElsaNeural',        # Italian Female
    'it_diego': 'it-IT-DiegoNeural',      # Italian Male
    'it_isabella': 'it-IT-IsabellaNeural', # Italian Female 2
    'it_giuseppe': 'it-IT-GiuseppeNeural', # Italian Male 2

    # ==================== PORTUGUESE ====================
    # Portuguese (Brazil)
    'pt_br_francisca': 'pt-BR-FranciscaNeural', # Brazilian Female
    'pt_br_antonio': 'pt-BR-AntonioNeural', # Brazilian Male
    'pt_br_thalita': 'pt-BR-ThalitaMultilingualNeural', # Brazilian Female Multilingual
    # Portuguese (Portugal)
    'pt_pt_raquel': 'pt-PT-RaquelNeural', # Portuguese Female
    'pt_pt_duarte': 'pt-PT-DuarteNeural', # Portuguese Male

    # ==================== CHINESE ====================
    # Chinese (Mandarin)
    'zh_xiaoxiao': 'zh-CN-XiaoxiaoNeural', # Chinese Female
    'zh_yunyang': 'zh-CN-YunyangNeural',   # Chinese Male
    'zh_xiaoyi': 'zh-CN-XiaoyiNeural',     # Chinese Female 2
    # Chinese (Cantonese)
    'zh_hk_hiugaai': 'zh-HK-HiuGaaiNeural', # Cantonese Female
    'zh_hk_wanlung': 'zh-HK-WanLungNeural', # Cantonese Male

    # ==================== JAPANESE ====================
    'ja_nanami': 'ja-JP-NanamiNeural',    # Japanese Female
    'ja_keita': 'ja-JP-KeitaNeural',      # Japanese Male
    'ja_aoi': 'ja-JP-AoiNeural',          # Japanese Female 2
    'ja_daichi': 'ja-JP-DaichiNeural',    # Japanese Male 2

    # ==================== KOREAN ====================
    'ko_sunhi': 'ko-KR-SunHiNeural',      # Korean Female
    'ko_injoon': 'ko-KR-InJoonNeural',    # Korean Male
    'ko_bongji': 'ko-KR-BongJinNeural',   # Korean Male 2

    # ==================== RUSSIAN ====================
    'ru_svetlana': 'ru-RU-SvetlanaNeural', # Russian Female
    'ru_dmitry': 'ru-RU-DmitryNeural',     # Russian Male

    # ==================== ARABIC ====================
    'ar_zariyah': 'ar-SA-ZariyahNeural',  # Arabic Female
    'ar_hamed': 'ar-SA-HamedNeural',      # Arabic Male

    # ==================== HINDI ====================
    'hi_swara': 'hi-IN-SwaraNeural',      # Hindi Female
    'hi_madhur': 'hi-IN-MadhurNeural',    # Hindi Male
}

# Display names for UI - auto-generated from AVAILABLE_VOICES
# Format: voice_id -> "Name (Language)"
VOICE_DISPLAY_NAMES = {
    # English - American
    'af_heart': 'Jenny (American)', 'af_bella': 'Aria (American)', 'af_sarah': 'Ava (American)',
    'af_sky': 'Michelle (American)', 'af_emma': 'Emma (American)', 'af_ana': 'Ana (American)',
    'af_nicole': 'Ava Multilingual', 'am_adam': 'Guy (American)', 'am_michael': 'Christopher (American)',
    'am_echo': 'Eric (American)', 'am_brian': 'Brian (American)', 'am_andrew': 'Andrew (American)',
    'am_roger': 'Roger (American)',
    # English - British
    'bf_emma': 'Sonia (British)', 'bf_isabella': 'Libby (British)', 'bf_maisie': 'Maisie (British)',
    'bm_george': 'Ryan (British)', 'bm_lewis': 'Thomas (British)',
    # English - Other
    'au_natasha': 'Natasha (Australian)', 'au_william': 'William (Australian)',
    'ca_clara': 'Clara (Canadian)', 'ca_liam': 'Liam (Canadian)',
    'in_neerja': 'Neerja (Indian)', 'in_neerja_exp': 'Neerja Expressive (Indian)', 'in_prabhat': 'Prabhat (Indian)',
    'ie_emily': 'Emily (Irish)', 'ie_connor': 'Connor (Irish)',
    'nz_molly': 'Molly (NZ)', 'nz_mitchell': 'Mitchell (NZ)',
    'sg_luna': 'Luna (Singapore)', 'sg_wayne': 'Wayne (Singapore)',
    'za_leah': 'Leah (South African)', 'za_luke': 'Luke (South African)',
    # Spanish
    'es_elvira': 'Elvira (Spanish)', 'es_alvaro': 'Alvaro (Spanish)',
    'es_mx_dalia': 'Dalia (Mexican)', 'es_mx_jorge': 'Jorge (Mexican)',
    'es_ar_elena': 'Elena (Argentine)', 'es_ar_tomas': 'Tomas (Argentine)',
    'es_co_salome': 'Salome (Colombian)', 'es_co_gonzalo': 'Gonzalo (Colombian)',
    # French
    'fr_denise': 'Denise (French)', 'fr_henri': 'Henri (French)', 'fr_vivienne': 'Vivienne (French)',
    'fr_ca_sylvie': 'Sylvie (Quebec)', 'fr_ca_antoine': 'Antoine (Quebec)', 'fr_ca_jean': 'Jean (Quebec)',
    # German
    'de_katja': 'Katja (German)', 'de_conrad': 'Conrad (German)',
    'de_amala': 'Amala (German)', 'de_seraphina': 'Seraphina (German)',
    # Italian
    'it_elsa': 'Elsa (Italian)', 'it_diego': 'Diego (Italian)',
    'it_isabella': 'Isabella (Italian)', 'it_giuseppe': 'Giuseppe (Italian)',
    # Portuguese
    'pt_br_francisca': 'Francisca (Brazilian)', 'pt_br_antonio': 'Antonio (Brazilian)',
    'pt_br_thalita': 'Thalita (Brazilian)', 'pt_pt_raquel': 'Raquel (Portuguese)', 'pt_pt_duarte': 'Duarte (Portuguese)',
    # Chinese
    'zh_xiaoxiao': 'Xiaoxiao (Chinese)', 'zh_yunyang': 'Yunyang (Chinese)', 'zh_xiaoyi': 'Xiaoyi (Chinese)',
    'zh_hk_hiugaai': 'HiuGaai (Cantonese)', 'zh_hk_wanlung': 'WanLung (Cantonese)',
    # Japanese
    'ja_nanami': 'Nanami (Japanese)', 'ja_keita': 'Keita (Japanese)',
    'ja_aoi': 'Aoi (Japanese)', 'ja_daichi': 'Daichi (Japanese)',
    # Korean
    'ko_sunhi': 'SunHi (Korean)', 'ko_injoon': 'InJoon (Korean)', 'ko_bongji': 'BongJin (Korean)',
    # Russian
    'ru_svetlana': 'Svetlana (Russian)', 'ru_dmitry': 'Dmitry (Russian)',
    # Arabic
    'ar_zariyah': 'Zariyah (Arabic)', 'ar_hamed': 'Hamed (Arabic)',
    # Hindi
    'hi_swara': 'Swara (Hindi)', 'hi_madhur': 'Madhur (Hindi)',
}

DEFAULT_VOICE = 'af_heart'
DEFAULT_SAMPLE_RATE = 24000


@dataclass
class TTSResult:
    """Result of text-to-speech synthesis"""
    audio_data: bytes
    sample_rate: int
    duration_seconds: float
    format: str  # 'mp3' or 'wav'


class EdgeTTS:
    """
    High-quality neural TTS using Microsoft Edge voices.

    Uses edge-tts for natural-sounding speech synthesis.
    """

    def __init__(self, voice: str = DEFAULT_VOICE):
        """
        Initialize TTS service.

        Args:
            voice: Voice ID from AVAILABLE_VOICES
        """
        self.voice = voice if voice in AVAILABLE_VOICES else DEFAULT_VOICE
        self._sample_rate = DEFAULT_SAMPLE_RATE

    async def initialize(self) -> bool:
        """Initialize TTS service (no-op for EdgeTTS)"""
        return True

    def _get_edge_voice(self, voice_id: str) -> str:
        """Convert our voice ID to Edge voice name"""
        return AVAILABLE_VOICES.get(voice_id, AVAILABLE_VOICES[DEFAULT_VOICE])

    async def synthesize(
        self,
        text: str,
        speed: float = 1.0,
        output_format: str = "mp3"
    ) -> TTSResult:
        """
        Generate audio from text (buffered - waits for completion).

        Args:
            text: Text to synthesize
            speed: Speech speed multiplier (0.5 - 2.0)
            output_format: 'mp3' (default)

        Returns:
            TTSResult with audio data
        """
        if not EDGE_TTS_AVAILABLE:
            return TTSResult(
                audio_data=b'',
                sample_rate=self._sample_rate,
                duration_seconds=0.0,
                format=output_format
            )

        try:
            # Convert speed to rate string (e.g., +50% or -25%)
            speed = max(0.5, min(2.0, speed))
            rate_percent = int((speed - 1.0) * 100)
            rate_str = f"+{rate_percent}%" if rate_percent >= 0 else f"{rate_percent}%"

            edge_voice = self._get_edge_voice(self.voice)
            logger.info(f"TTS synthesizing: voice_id={self.voice}, edge_voice={edge_voice}, rate={rate_str}")

            # Create communicate instance
            communicate = edge_tts.Communicate(text, edge_voice, rate=rate_str)

            # Collect audio data
            audio_data = io.BytesIO()
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    audio_data.write(chunk["data"])

            audio_bytes = audio_data.getvalue()

            # Estimate duration (MP3 at ~128kbps)
            duration = len(audio_bytes) / (128 * 1024 / 8) if audio_bytes else 0.0

            return TTSResult(
                audio_data=audio_bytes,
                sample_rate=self._sample_rate,
                duration_seconds=duration,
                format="mp3"
            )

        except Exception as e:
            logger.error(f"Edge TTS synthesis failed: {e}")
            return TTSResult(
                audio_data=b'',
                sample_rate=self._sample_rate,
                duration_seconds=0.0,
                format=output_format
            )

    async def synthesize_streaming(
        self,
        text: str,
        speed: float = 1.0,
        min_chunk_size: int = 4096
    ) -> AsyncGenerator[bytes, None]:
        """
        Generate audio from text with true streaming.

        Yields buffered 4KB chunks as they arrive from Edge TTS,
        matching the same pattern used by Chatterbox/Coqui providers.
        Frontend audio queue already handles incremental MP3 chunks.

        Args:
            text: Text to synthesize
            speed: Speech speed multiplier (0.5 - 2.0)
            min_chunk_size: Minimum bytes before yielding a chunk (default 4KB)

        Yields:
            bytes: MP3 audio chunks (4KB buffered)
        """
        if not EDGE_TTS_AVAILABLE:
            logger.warning("Edge TTS not available for streaming")
            return

        try:
            # Convert speed to rate string
            speed = max(0.5, min(2.0, speed))
            rate_percent = int((speed - 1.0) * 100)
            rate_str = f"+{rate_percent}%" if rate_percent >= 0 else f"{rate_percent}%"

            edge_voice = self._get_edge_voice(self.voice)
            logger.debug(f"TTS streaming: voice={edge_voice}, rate={rate_str}, text_len={len(text)}")

            # Create communicate instance
            communicate = edge_tts.Communicate(text, edge_voice, rate=rate_str)

            # Stream audio chunks as they arrive with 4KB buffering
            buffer = bytearray()
            async for chunk in communicate.stream():
                if chunk["type"] == "audio" and chunk["data"]:
                    buffer.extend(chunk["data"])
                    while len(buffer) >= min_chunk_size:
                        yield bytes(buffer[:min_chunk_size])
                        buffer = buffer[min_chunk_size:]

            # Yield remaining data
            if buffer:
                yield bytes(buffer)

        except Exception as e:
            logger.error(f"Edge TTS streaming failed: {e}")

    def set_voice(self, voice: str) -> bool:
        """
        Change the active voice.

        Args:
            voice: Voice ID from AVAILABLE_VOICES

        Returns:
            True if voice was changed
        """
        if voice in AVAILABLE_VOICES:
            self.voice = voice
            return True
        return False

    @property
    def is_available(self) -> bool:
        """Check if TTS is available"""
        return EDGE_TTS_AVAILABLE

    @property
    def is_loaded(self) -> bool:
        """Check if TTS is ready"""
        return EDGE_TTS_AVAILABLE

    @property
    def sample_rate(self) -> int:
        """Get the audio sample rate"""
        return self._sample_rate

    def get_info(self) -> dict:
        """Get service info"""
        return {
            "available": EDGE_TTS_AVAILABLE,
            "loaded": EDGE_TTS_AVAILABLE,
            "voice": self.voice,
            "sample_rate": self._sample_rate,
            "voices": list(AVAILABLE_VOICES.keys())
        }


# Singleton instance
_tts_service: Optional[EdgeTTS] = None


def get_tts_service(voice: str = DEFAULT_VOICE) -> EdgeTTS:
    """Get or create singleton TTS service, updating voice if different"""
    global _tts_service
    if _tts_service is None:
        logger.info(f"Creating new TTS service with voice={voice}")
        _tts_service = EdgeTTS(voice=voice)
    elif _tts_service.voice != voice:
        # Update voice if it changed
        logger.info(f"Updating TTS service voice from {_tts_service.voice} to {voice}")
        _tts_service.set_voice(voice)
    return _tts_service

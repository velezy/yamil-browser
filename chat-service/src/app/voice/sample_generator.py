"""
Voice Sample Generator for Chatterbox TTS Voice Cloning

Generates reference audio clips from Edge TTS (Microsoft neural voices)
and caches them on disk. Chatterbox uses these cached clips via its
/v1/audio/speech/upload multipart endpoint to clone voice identity.

This gives Chatterbox 50+ distinct voices without requiring manual
reference audio recordings.

Flow:
1. User selects voice (e.g., af_heart → en-US-JennyNeural)
2. First request: generate ~8s reference clip via Edge TTS, cache as MP3
3. Subsequent requests: return cached MP3 path instantly
4. Chatterbox receives the MP3 via multipart upload for voice cloning
"""

import asyncio
import os
import logging
import tempfile
import shutil
from typing import Optional

logger = logging.getLogger(__name__)

# Reference text for voice sample generation
# ~8 seconds of varied phonemes and intonation for good voice cloning
REFERENCE_TEXT = (
    "The quick brown fox jumps over the lazy dog. "
    "She sells seashells by the seashore on a beautiful sunny morning. "
    "How wonderful it is to explore new places and discover amazing things!"
)

# Directory for cached voice samples
VOICE_SAMPLES_DIR = os.getenv("VOICE_SAMPLES_DIR", "/app/voice_samples")

# Per-voice locks to prevent concurrent generation of the same voice
_generation_locks: dict[str, asyncio.Lock] = {}
_global_lock = asyncio.Lock()


async def _get_voice_lock(voice_id: str) -> asyncio.Lock:
    """Get or create a per-voice lock to prevent duplicate generation."""
    async with _global_lock:
        if voice_id not in _generation_locks:
            _generation_locks[voice_id] = asyncio.Lock()
        return _generation_locks[voice_id]


async def get_voice_sample(voice_id: str) -> Optional[str]:
    """
    Get the cached voice sample path for a voice ID, generating if needed.

    Args:
        voice_id: Internal voice ID (e.g., 'af_heart', 'am_adam')

    Returns:
        Absolute path to cached MP3 file, or None if generation failed
    """
    from .tts import AVAILABLE_VOICES

    # Look up Edge TTS voice name
    edge_voice = AVAILABLE_VOICES.get(voice_id)
    if not edge_voice:
        logger.warning(f"No Edge TTS mapping for voice '{voice_id}', cannot generate sample")
        return None

    # Ensure cache directory exists
    os.makedirs(VOICE_SAMPLES_DIR, exist_ok=True)

    # Check cache
    sample_path = os.path.join(VOICE_SAMPLES_DIR, f"{voice_id}.mp3")
    if os.path.exists(sample_path) and os.path.getsize(sample_path) > 0:
        logger.debug(f"Voice sample cache hit: {voice_id}")
        return sample_path

    # Generate with per-voice lock (prevent concurrent generation of same voice)
    lock = await _get_voice_lock(voice_id)
    async with lock:
        # Double-check after acquiring lock (another coroutine may have generated it)
        if os.path.exists(sample_path) and os.path.getsize(sample_path) > 0:
            return sample_path

        logger.info(f"Generating voice sample for '{voice_id}' using Edge TTS voice '{edge_voice}'")

        try:
            import edge_tts

            communicate = edge_tts.Communicate(REFERENCE_TEXT, edge_voice)

            # Write to temp file first, then atomic rename to prevent corruption
            fd, tmp_path = tempfile.mkstemp(suffix=".mp3", dir=VOICE_SAMPLES_DIR)
            os.close(fd)

            try:
                await communicate.save(tmp_path)

                # Verify the file has content
                if os.path.getsize(tmp_path) < 100:
                    logger.error(f"Generated sample too small for '{voice_id}' ({os.path.getsize(tmp_path)} bytes)")
                    os.unlink(tmp_path)
                    return None

                # Atomic move
                shutil.move(tmp_path, sample_path)
                logger.info(f"Voice sample generated and cached: {voice_id} ({os.path.getsize(sample_path)} bytes)")
                return sample_path

            except Exception:
                # Clean up temp file on error
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise

        except ImportError:
            logger.error("edge_tts not installed, cannot generate voice samples")
            return None
        except Exception as e:
            logger.error(f"Failed to generate voice sample for '{voice_id}': {e}")
            return None

"""
Streaming TTS Pipeline

Stream LLM tokens immediately while generating TTS for complete sentences in parallel.
This provides fast perceived latency by showing text immediately while audio generates.

Features:
- Immediate token streaming for text display
- Sentence extraction from token stream
- Parallel TTS generation per sentence
- Audio chunk combination and streaming
"""

import asyncio
import base64
import io
import os
import wave
import re
import logging
from typing import AsyncGenerator, Optional, List, Tuple, Union
from dataclasses import dataclass

from .tts import EdgeTTS, get_tts_service, TTSResult
from .streaming_providers import (
    StreamingTTSProvider,
    StreamingConfig,
    TTSProvider as TTSProviderEnum,
    get_streaming_provider,
    auto_select_provider,
)
from .text_utils import clean_for_tts

# TTS provider override from environment
TTS_PROVIDER = os.getenv("TTS_PROVIDER", "auto")

logger = logging.getLogger(__name__)


@dataclass
class StreamEvent:
    """Event from the streaming pipeline"""
    type: str  # 'token', 'sentence', 'audio', 'complete', 'error'
    data: dict


class StreamingTTSPipeline:
    """
    Stream LLM tokens + Generate TTS per sentence in parallel.

    Flow:
    1. Receive tokens from LLM stream
    2. Yield each token immediately for text display
    3. Buffer tokens and extract complete sentences
    4. Generate TTS for each sentence in background
    5. Combine and yield audio when complete

    This provides fast perceived latency:
    - User sees text immediately
    - Audio plays shortly after text completes
    """

    # Sentence ending patterns
    SENTENCE_ENDINGS = re.compile(r'(?<=[.!?])\s+(?=[A-Z])|(?<=[.!?])$')

    # Minimum sentence length for TTS (chars)
    MIN_SENTENCE_LENGTH = 5

    def __init__(
        self,
        tts: Optional[Union[EdgeTTS, StreamingTTSProvider]] = None,
        speed: float = 1.0,
        combine_audio: bool = True,
        strip_markdown: bool = True,
        provider_name: Optional[str] = None,
    ):
        """
        Initialize pipeline.

        Args:
            tts: TTS service instance (EdgeTTS or StreamingTTSProvider)
            speed: Speech speed multiplier
            combine_audio: Whether to combine all audio into single chunk
            strip_markdown: Whether to strip markdown formatting for natural TTS
            provider_name: Override TTS provider (auto, edge, kokoro, piper, coqui, chatterbox, elevenlabs)
        """
        self._streaming_provider = None
        provider = provider_name or TTS_PROVIDER

        if tts is not None:
            self.tts = tts
        elif provider != "auto" and provider != "edge":
            # Use streaming provider from streaming_providers.py
            try:
                provider_enum = TTSProviderEnum(provider)
                config = StreamingConfig(provider=provider_enum)
                self._streaming_provider = get_streaming_provider(config)
                self.tts = get_tts_service()  # Keep as fallback
                logger.info(f"Pipeline using streaming provider: {provider}")
            except (ValueError, Exception) as e:
                logger.warning(f"Failed to load provider '{provider}', falling back to EdgeTTS: {e}")
                self.tts = get_tts_service()
        else:
            self.tts = get_tts_service()

        self.speed = speed
        self.combine_audio = combine_audio
        self.strip_markdown = strip_markdown

    async def stream_with_tts(
        self,
        llm_stream: AsyncGenerator[str, None],
        include_audio: bool = True
    ) -> AsyncGenerator[StreamEvent, None]:
        """
        Stream text tokens immediately, generate TTS for sentences in parallel.

        Args:
            llm_stream: Async generator yielding LLM tokens
            include_audio: Whether to generate and include audio

        Yields:
            StreamEvent with token, sentence, or audio data
        """
        sentence_buffer = ""
        tts_tasks: List[asyncio.Task] = []
        sentence_index = 0
        full_response = ""

        try:
            async for token in llm_stream:
                # Yield token immediately for text display
                yield StreamEvent(
                    type="token",
                    data={"text": token, "index": len(full_response)}
                )

                full_response += token
                sentence_buffer += token

                # Extract complete sentences
                sentences, sentence_buffer = self._extract_sentences(sentence_buffer)

                for sentence in sentences:
                    if len(sentence.strip()) >= self.MIN_SENTENCE_LENGTH:
                        # Notify about complete sentence
                        yield StreamEvent(
                            type="sentence",
                            data={
                                "text": sentence,
                                "index": sentence_index
                            }
                        )

                        # Start TTS generation in background
                        if include_audio:
                            task = asyncio.create_task(
                                self._generate_tts(sentence, sentence_index)
                            )
                            tts_tasks.append(task)

                        sentence_index += 1

            # Process remaining buffer as final sentence
            if sentence_buffer.strip() and len(sentence_buffer.strip()) >= self.MIN_SENTENCE_LENGTH:
                yield StreamEvent(
                    type="sentence",
                    data={
                        "text": sentence_buffer.strip(),
                        "index": sentence_index,
                        "final": True
                    }
                )

                if include_audio:
                    task = asyncio.create_task(
                        self._generate_tts(sentence_buffer.strip(), sentence_index)
                    )
                    tts_tasks.append(task)

            # Wait for all TTS to complete
            if tts_tasks and include_audio:
                tts_results = await asyncio.gather(*tts_tasks, return_exceptions=True)

                # Filter successful results
                successful_results = []
                for result in tts_results:
                    if isinstance(result, Exception):
                        logger.error(f"TTS generation failed: {result}")
                    else:
                        successful_results.append(result)

                if successful_results:
                    # Sort by index
                    sorted_results = sorted(successful_results, key=lambda x: x[0])

                    if self.combine_audio:
                        # Combine all audio chunks
                        combined_audio = self._combine_wav_chunks(
                            [r[1] for r in sorted_results]
                        )
                        yield StreamEvent(
                            type="audio",
                            data={
                                "data": base64.b64encode(combined_audio).decode(),
                                "format": "wav",
                                "sample_rate": self.tts.sample_rate,
                                "chunks": len(sorted_results)
                            }
                        )
                    else:
                        # Yield individual audio chunks
                        for idx, audio_data in sorted_results:
                            yield StreamEvent(
                                type="audio",
                                data={
                                    "data": base64.b64encode(audio_data).decode(),
                                    "format": "wav",
                                    "sample_rate": self.tts.sample_rate,
                                    "index": idx
                                }
                            )

            # Send completion event
            yield StreamEvent(
                type="complete",
                data={
                    "full_text": full_response,
                    "sentence_count": sentence_index + 1,
                    "has_audio": bool(tts_tasks) and include_audio
                }
            )

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            yield StreamEvent(
                type="error",
                data={"message": str(e)}
            )

    def _extract_sentences(self, text: str) -> Tuple[List[str], str]:
        """
        Extract complete sentences from buffer.

        Args:
            text: Text buffer

        Returns:
            (list of complete sentences, remaining text)
        """
        # Split on sentence endings followed by space and capital letter
        parts = self.SENTENCE_ENDINGS.split(text)

        if len(parts) > 1:
            # All but last are complete sentences
            sentences = [p.strip() for p in parts[:-1] if p.strip()]
            remaining = parts[-1] if parts[-1] else ""
        else:
            # Check if text ends with sentence terminator
            if text.rstrip().endswith(('.', '!', '?')):
                sentences = [text.strip()]
                remaining = ""
            else:
                sentences = []
                remaining = text

        return sentences, remaining

    async def _generate_tts(
        self,
        text: str,
        index: int
    ) -> Tuple[int, bytes]:
        """
        Generate TTS for a sentence.

        Uses StreamingTTSProvider if available, falls back to EdgeTTS.

        Args:
            text: Sentence text
            index: Sentence index for ordering

        Returns:
            (index, audio_bytes)
        """
        # Clean text for natural TTS (strip markdown like **bold**, *italic*, etc.)
        clean_text = clean_for_tts(text, strip_md=self.strip_markdown)

        if not clean_text:
            return (index, b'')

        # Use streaming provider if available
        if self._streaming_provider is not None:
            try:
                audio_chunks = []
                async for chunk in self._streaming_provider.stream_audio(clean_text, speed=self.speed):
                    audio_chunks.append(chunk)
                return (index, b''.join(audio_chunks))
            except Exception as e:
                logger.warning(f"Streaming provider failed, falling back to EdgeTTS: {e}")

        # Fallback to EdgeTTS
        result = await self.tts.synthesize(clean_text, speed=self.speed)
        return (index, result.audio_data)

    def _combine_wav_chunks(self, chunks: List[bytes]) -> bytes:
        """
        Combine multiple WAV chunks into one.

        Args:
            chunks: List of WAV bytes

        Returns:
            Combined WAV bytes
        """
        if not chunks:
            return b''

        if len(chunks) == 1:
            return chunks[0]

        # Extract PCM data from each WAV
        pcm_data = []
        sample_rate = 24000
        sample_width = 2

        for chunk in chunks:
            if len(chunk) < 44:  # Minimum WAV header size
                continue

            try:
                with io.BytesIO(chunk) as f:
                    with wave.open(f, 'rb') as wav:
                        sample_rate = wav.getframerate()
                        sample_width = wav.getsampwidth()
                        pcm_data.append(wav.readframes(wav.getnframes()))
            except Exception as e:
                logger.warning(f"Failed to read WAV chunk: {e}")
                continue

        if not pcm_data:
            return b''

        # Combine PCM data
        combined_pcm = b''.join(pcm_data)

        # Create combined WAV
        output = io.BytesIO()
        with wave.open(output, 'wb') as wav:
            wav.setnchannels(1)
            wav.setsampwidth(sample_width)
            wav.setframerate(sample_rate)
            wav.writeframes(combined_pcm)

        return output.getvalue()


class VoiceChatPipeline:
    """
    Complete voice chat pipeline: STT -> LLM -> TTS

    Integrates:
    - Speech-to-text transcription
    - LLM response generation
    - Text-to-speech synthesis
    """

    def __init__(
        self,
        stt=None,
        tts=None,
        llm_generator=None,
        tts_provider: Optional[str] = None,
        stt_provider: Optional[str] = None,
    ):
        """
        Initialize voice chat pipeline.

        Args:
            stt: STT service (optional, will use singleton)
            tts: TTS service (optional, will use singleton)
            llm_generator: Async function(text) -> AsyncGenerator[str]
            tts_provider: Override TTS provider name
            stt_provider: Override STT provider name
        """
        from .stt import get_stt_provider

        self.stt = stt or get_stt_provider(stt_provider)
        self.tts = tts or get_tts_service()
        self.llm_generator = llm_generator
        self.streaming_pipeline = StreamingTTSPipeline(
            tts=self.tts,
            provider_name=tts_provider,
        )

    async def process_audio(
        self,
        audio_data: bytes,
        language: Optional[str] = None,
        include_audio: bool = True
    ) -> AsyncGenerator[StreamEvent, None]:
        """
        Process audio through full pipeline.

        Args:
            audio_data: Audio bytes from user
            language: Language code for STT
            include_audio: Whether to generate response audio

        Yields:
            StreamEvent with transcript, tokens, and audio
        """
        # Step 1: Transcribe audio
        yield StreamEvent(
            type="state",
            data={"state": "transcribing"}
        )

        transcript = await self.stt.transcribe(audio_data, language)

        yield StreamEvent(
            type="transcript",
            data={
                "text": transcript.text,
                "language": transcript.language,
                "confidence": transcript.confidence
            }
        )

        if not transcript.text.strip():
            yield StreamEvent(
                type="error",
                data={"message": "No speech detected"}
            )
            return

        # Step 2: Generate LLM response with streaming
        yield StreamEvent(
            type="state",
            data={"state": "generating"}
        )

        if self.llm_generator:
            llm_stream = self.llm_generator(transcript.text)

            # Step 3: Stream tokens and generate TTS
            async for event in self.streaming_pipeline.stream_with_tts(
                llm_stream,
                include_audio=include_audio
            ):
                yield event

        yield StreamEvent(
            type="state",
            data={"state": "idle"}
        )


# Singleton instance
_pipeline: Optional[StreamingTTSPipeline] = None


def get_streaming_pipeline(provider_name: Optional[str] = None) -> StreamingTTSPipeline:
    """Get or create singleton streaming pipeline"""
    global _pipeline
    if _pipeline is None:
        _pipeline = StreamingTTSPipeline(provider_name=provider_name)
    return _pipeline

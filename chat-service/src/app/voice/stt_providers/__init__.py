"""
STT Provider Backends

Pluggable speech-to-text provider implementations.
Add new providers here and register them in stt.get_stt_provider().

Available providers:
- FasterWhisperSTT (default): Local Faster-Whisper with distil-large-v3

Planned:
- DeepgramSTT: Cloud-based, low latency streaming
- WhisperAPISTT: OpenAI Whisper API
- AzureSTT: Azure Cognitive Services Speech
"""

from ..stt import STTProvider, TranscriptionResult, FasterWhisperSTT

__all__ = ["STTProvider", "TranscriptionResult", "FasterWhisperSTT"]

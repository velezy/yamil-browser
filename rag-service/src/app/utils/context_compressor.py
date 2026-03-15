"""
Context Compression with LLMLingua

Compresses retrieved context before sending to LLM to reduce token usage
and improve answer quality by removing irrelevant content.

Features:
- LLMLingua-based prompt compression
- Configurable compression ratio
- Query-aware compression (preserves query-relevant content)
- Graceful fallback when LLMLingua unavailable
"""

import os
import logging
from typing import List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Try to import LLMLingua
try:
    from llmlingua import PromptCompressor
    LLMLINGUA_AVAILABLE = True
except ImportError:
    LLMLINGUA_AVAILABLE = False
    logger.info("LLMLingua not available — context compression disabled")


@dataclass
class CompressionResult:
    """Result of context compression"""
    compressed_text: str
    original_tokens: int
    compressed_tokens: int
    compression_ratio: float
    saving_percent: float


class ContextCompressor:
    """
    Compresses RAG context using LLMLingua to reduce token usage
    while preserving query-relevant information.

    Typical savings: 2-5x token reduction with <5% quality loss.
    """

    def __init__(
        self,
        model_name: str = "microsoft/llmlingua-2-bert-base-multilingual-cased-meetingbank",
        target_ratio: float = 0.5,
        force_tokens: Optional[List[str]] = None,
        use_llmlingua2: bool = True,
    ):
        """
        Initialize context compressor.

        Args:
            model_name: LLMLingua model to use
            target_ratio: Target compression ratio (0.5 = keep 50% of tokens)
            force_tokens: Tokens to always preserve
            use_llmlingua2: Use LLMLingua-2 (faster, better quality)
        """
        self.model_name = model_name
        self.target_ratio = float(os.getenv("COMPRESSION_RATIO", str(target_ratio)))
        self.force_tokens = force_tokens or []
        self.use_llmlingua2 = use_llmlingua2
        self._compressor: Optional['PromptCompressor'] = None
        self._enabled = LLMLINGUA_AVAILABLE and os.getenv("ENABLE_COMPRESSION", "true").lower() == "true"

    def _get_compressor(self) -> Optional['PromptCompressor']:
        """Lazy-load the compressor model"""
        if not self._enabled:
            return None

        if self._compressor is None:
            try:
                self._compressor = PromptCompressor(
                    model_name=self.model_name,
                    use_llmlingua2=self.use_llmlingua2,
                )
                logger.info(f"LLMLingua compressor loaded: {self.model_name}")
            except Exception as e:
                logger.error(f"Failed to load LLMLingua compressor: {e}")
                self._enabled = False
                return None

        return self._compressor

    def compress(
        self,
        context: str,
        query: str = "",
        target_ratio: Optional[float] = None,
    ) -> CompressionResult:
        """
        Compress context text, preserving query-relevant information.

        Args:
            context: The retrieved context to compress
            query: The user query (for query-aware compression)
            target_ratio: Override default compression ratio

        Returns:
            CompressionResult with compressed text and statistics
        """
        ratio = target_ratio or self.target_ratio

        # Estimate original tokens (rough: 1 token ≈ 4 chars)
        original_tokens = len(context) // 4

        compressor = self._get_compressor()
        if compressor is None:
            # Fallback: return uncompressed
            return CompressionResult(
                compressed_text=context,
                original_tokens=original_tokens,
                compressed_tokens=original_tokens,
                compression_ratio=1.0,
                saving_percent=0.0,
            )

        try:
            result = compressor.compress_prompt(
                context,
                question=query if query else None,
                rate=ratio,
                force_tokens=self.force_tokens if self.force_tokens else None,
            )

            compressed_text = result.get("compressed_prompt", context)
            compressed_tokens = len(compressed_text) // 4

            actual_ratio = compressed_tokens / max(original_tokens, 1)
            saving = (1 - actual_ratio) * 100

            logger.debug(
                f"Compressed context: {original_tokens} → {compressed_tokens} tokens "
                f"({saving:.1f}% saved)"
            )

            return CompressionResult(
                compressed_text=compressed_text,
                original_tokens=original_tokens,
                compressed_tokens=compressed_tokens,
                compression_ratio=actual_ratio,
                saving_percent=saving,
            )

        except Exception as e:
            logger.warning(f"Compression failed, returning original: {e}")
            return CompressionResult(
                compressed_text=context,
                original_tokens=original_tokens,
                compressed_tokens=original_tokens,
                compression_ratio=1.0,
                saving_percent=0.0,
            )

    def compress_chunks(
        self,
        chunks: List[str],
        query: str = "",
        target_ratio: Optional[float] = None,
    ) -> List[CompressionResult]:
        """
        Compress a list of context chunks individually.

        Args:
            chunks: List of context strings
            query: The user query
            target_ratio: Override default compression ratio

        Returns:
            List of CompressionResult for each chunk
        """
        return [
            self.compress(chunk, query=query, target_ratio=target_ratio)
            for chunk in chunks
        ]

    @property
    def is_available(self) -> bool:
        """Check if compression is available"""
        return self._enabled

    def get_info(self) -> dict:
        """Get compressor info"""
        return {
            "available": LLMLINGUA_AVAILABLE,
            "enabled": self._enabled,
            "model": self.model_name,
            "target_ratio": self.target_ratio,
            "loaded": self._compressor is not None,
        }


# Singleton
_compressor: Optional[ContextCompressor] = None


def get_context_compressor() -> ContextCompressor:
    """Get or create singleton context compressor"""
    global _compressor
    if _compressor is None:
        _compressor = ContextCompressor()
    return _compressor

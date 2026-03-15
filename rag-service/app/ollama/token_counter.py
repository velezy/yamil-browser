"""
Token Counter

Accurate token counting for context window management.
Uses tiktoken with fallback estimation.
"""

import logging
from typing import Optional, Union
from functools import lru_cache

logger = logging.getLogger(__name__)

# Try to import tiktoken
try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    logger.warning("tiktoken not available, using fallback token estimation")


class TokenCounter:
    """
    Accurate token counting with tiktoken.

    Features:
    - tiktoken for accurate counting
    - Fallback estimation when tiktoken unavailable
    - Cached encoding for performance
    - Multiple encoding support
    """

    # Default encoding (works well for most models)
    DEFAULT_ENCODING = "cl100k_base"

    # Fallback ratio (tokens per word)
    FALLBACK_RATIO = 1.3

    def __init__(self, encoding_name: str = DEFAULT_ENCODING):
        """
        Initialize token counter.

        Args:
            encoding_name: tiktoken encoding name
        """
        self.encoding_name = encoding_name
        self._encoding = None

        if TIKTOKEN_AVAILABLE:
            self._encoding = self._get_encoding(encoding_name)

    @lru_cache(maxsize=8)
    def _get_encoding(self, encoding_name: str):
        """Get tiktoken encoding (cached)"""
        if not TIKTOKEN_AVAILABLE:
            return None
        try:
            return tiktoken.get_encoding(encoding_name)
        except Exception as e:
            logger.warning(f"Failed to load encoding {encoding_name}: {e}")
            return None

    def count_tokens(self, text: str) -> int:
        """
        Count tokens in text.

        Args:
            text: Input text

        Returns:
            Token count
        """
        if not text:
            return 0

        if self._encoding:
            try:
                return len(self._encoding.encode(text))
            except Exception as e:
                logger.warning(f"tiktoken encoding failed: {e}")

        # Fallback: estimate ~1.3 tokens per word
        return int(len(text.split()) * self.FALLBACK_RATIO)

    def count_tokens_batch(self, texts: list[str]) -> list[int]:
        """
        Count tokens for multiple texts.

        Args:
            texts: List of input texts

        Returns:
            List of token counts
        """
        return [self.count_tokens(text) for text in texts]

    def truncate_to_tokens(
        self,
        text: str,
        max_tokens: int,
        truncation_strategy: str = "end"
    ) -> str:
        """
        Truncate text to fit within token limit.

        Args:
            text: Input text
            max_tokens: Maximum tokens allowed
            truncation_strategy: 'end', 'start', or 'middle'

        Returns:
            Truncated text
        """
        if not text:
            return text

        current_tokens = self.count_tokens(text)
        if current_tokens <= max_tokens:
            return text

        if self._encoding:
            # Use tiktoken for precise truncation
            tokens = self._encoding.encode(text)

            if truncation_strategy == "end":
                tokens = tokens[:max_tokens]
            elif truncation_strategy == "start":
                tokens = tokens[-max_tokens:]
            else:  # middle
                half = max_tokens // 2
                tokens = tokens[:half] + tokens[-(max_tokens - half):]

            return self._encoding.decode(tokens)

        # Fallback: word-based truncation
        words = text.split()
        estimated_words = int(max_tokens / self.FALLBACK_RATIO)

        if truncation_strategy == "end":
            return " ".join(words[:estimated_words])
        elif truncation_strategy == "start":
            return " ".join(words[-estimated_words:])
        else:  # middle
            half = estimated_words // 2
            return " ".join(words[:half]) + " ... " + " ".join(words[-half:])

    def estimate_tokens_for_model(
        self,
        text: str,
        model: str
    ) -> int:
        """
        Estimate tokens for a specific model.

        Different models may have different tokenizers.

        Args:
            text: Input text
            model: Model name (e.g., 'llama3.1:8b')

        Returns:
            Estimated token count
        """
        # Most local models use similar tokenization to cl100k_base
        # This is a reasonable approximation
        return self.count_tokens(text)

    def fits_context(
        self,
        prompt: str,
        context: str,
        max_context: int,
        reserve_for_response: int = 512
    ) -> bool:
        """
        Check if prompt + context fits within limits.

        Args:
            prompt: User prompt
            context: RAG context
            max_context: Maximum context tokens
            reserve_for_response: Tokens reserved for response

        Returns:
            True if fits
        """
        total_tokens = self.count_tokens(prompt) + self.count_tokens(context)
        available = max_context - reserve_for_response
        return total_tokens <= available

    def split_into_chunks(
        self,
        text: str,
        chunk_size: int,
        overlap: int = 0
    ) -> list[str]:
        """
        Split text into token-based chunks.

        Args:
            text: Input text
            chunk_size: Target tokens per chunk
            overlap: Token overlap between chunks

        Returns:
            List of text chunks
        """
        if not text:
            return []

        if self._encoding:
            tokens = self._encoding.encode(text)
            chunks = []
            start = 0

            while start < len(tokens):
                end = min(start + chunk_size, len(tokens))
                chunk_tokens = tokens[start:end]
                chunks.append(self._encoding.decode(chunk_tokens))
                start = end - overlap if overlap > 0 else end

            return chunks

        # Fallback: word-based chunking
        words = text.split()
        estimated_words_per_chunk = int(chunk_size / self.FALLBACK_RATIO)
        overlap_words = int(overlap / self.FALLBACK_RATIO)

        chunks = []
        start = 0

        while start < len(words):
            end = min(start + estimated_words_per_chunk, len(words))
            chunks.append(" ".join(words[start:end]))
            start = end - overlap_words if overlap_words > 0 else end

        return chunks


# Convenience function
def count_tokens(text: str) -> int:
    """Quick token count using default counter"""
    return TokenCounter().count_tokens(text)

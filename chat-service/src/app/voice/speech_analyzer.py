"""
Speech Content Analyzer (Local Ollama)

Converts AI responses into natural spoken format using local Ollama.
Runs entirely on your machine - no external API calls.

Transforms:
- Code execution results -> natural language
- Charts/diagrams -> descriptive phrases
- Technical content -> conversational speech
- Numbers -> readable format
"""

import re
import logging
import os
import httpx
from typing import Optional

logger = logging.getLogger(__name__)

# Get Ollama settings from environment
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")

# System prompt for speech conversion
SPEECH_PROMPT = """Convert this AI response to natural spoken language for text-to-speech.

Rules:
- Make it conversational, like you're speaking to someone
- For code results, just say the result naturally (e.g., "The result is four hundred thirty-seven thousand, seven hundred ninety-eight")
- For charts/diagrams, say "I've created a visual chart/diagram for you"
- Remove all code, markdown, technical symbols
- Keep it concise - about 50% of original length
- Use natural pauses with commas and periods
- Don't say "here's the response" or meta-commentary

Content to convert:
"""


class SpeechAnalyzer:
    """
    Local speech analyzer using Ollama.

    Uses a small fast model (llama3.2:1b or similar) running locally
    for quick content-to-speech conversion.
    """

    def __init__(
        self,
        ollama_host: str = OLLAMA_HOST,
        model: str = "llama3.2:1b",  # Small, fast model
        timeout: float = 5.0,  # Quick timeout for responsiveness
    ):
        self.ollama_host = ollama_host
        self.model = model
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._fallback = RegexFallback()

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def convert_to_speech(self, text: str) -> str:
        """
        Convert AI response to natural speech using local Ollama.

        Falls back to regex if Ollama is unavailable or slow.
        """
        if not text or len(text) < 30:
            return self._fallback.convert(text)

        # Quick check - if text is simple, use regex
        if not self._needs_conversion(text):
            return self._fallback.convert(text)

        try:
            client = await self._get_client()

            response = await client.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.model,
                    "prompt": SPEECH_PROMPT + text,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 300,  # Keep output short
                    }
                }
            )

            if response.status_code == 200:
                result = response.json()
                converted = result.get("response", "").strip()
                if converted and len(converted) > 10:
                    logger.debug(f"Ollama speech conversion: {len(text)} -> {len(converted)}")
                    return converted

        except httpx.TimeoutException:
            logger.debug("Ollama speech conversion timed out")
        except httpx.ConnectError:
            logger.debug("Ollama not available for speech conversion")
        except Exception as e:
            logger.warning(f"Ollama speech conversion failed: {e}")

        # Fallback to regex
        return self._fallback.convert(text)

    def _needs_conversion(self, text: str) -> bool:
        """Check if text needs LLM conversion."""
        indicators = [
            "```",           # Code blocks
            "**Output:**",   # Code execution
            "**Code Executed:**",
            "mermaid",       # Diagrams
            "|---",          # Tables
        ]
        return any(ind in text for ind in indicators)

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


class RegexFallback:
    """Fast regex-based fallback when Ollama is unavailable."""

    def convert(self, text: str) -> str:
        """Convert using regex rules."""
        if not text:
            return ""

        result = text

        # Code execution -> result only
        result = self._convert_code(result)

        # Charts/diagrams -> description
        result = self._convert_visuals(result)

        # Strip markdown
        result = self._strip_markdown(result)

        # Cleanup
        result = self._cleanup(result)

        return result

    def _convert_code(self, text: str) -> str:
        """Extract code results."""

        def replace_code(match):
            full = match.group(0)
            output_match = re.search(r'\*\*Output:\*\*\s*```[^\n]*\n([\s\S]*?)```', full)
            if output_match:
                output = output_match.group(1).strip().split('\n')[0]
                try:
                    num = float(output.replace(',', ''))
                    if num == int(num):
                        return f" The result is {int(num):,}."
                    return f" The result is {num:,.2f}."
                except ValueError:
                    if len(output) < 80:
                        return f" The result is: {output}."
            return " The code ran successfully."

        pattern = r'\*\*Code Executed:\*\*[\s\S]*?```[\s\S]*?```(?:\s*\*Execution time:[^*]*\*)?(?:\s*\*\*Output:\*\*\s*```[\s\S]*?```)?'
        text = re.sub(pattern, replace_code, text)
        text = re.sub(r'```[a-z]*\n[\s\S]*?```', '', text)
        text = re.sub(r'`[^`]+`', '', text)
        return text

    def _convert_visuals(self, text: str) -> str:
        """Convert charts and diagrams."""
        text = re.sub(r'```chartjs[\s\S]*?```', "I've created a chart for you.", text)
        text = re.sub(r'```mermaid[\s\S]*?```', "I've created a diagram for you.", text)
        return text

    def _strip_markdown(self, text: str) -> str:
        """Remove markdown formatting."""
        text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        text = re.sub(r'!\[([^\]]*)\]\([^)]+\)', r'\1', text)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        text = re.sub(r'^\s*[-*+]\s+', '', text, flags=re.MULTILINE)
        return text

    def _cleanup(self, text: str) -> str:
        """Final cleanup."""
        text = re.sub(r'\{[^}]*\}', '', text)
        text = re.sub(r'\[[^\]]*\]', '', text)
        text = re.sub(r'[/\\][\w./\\-]+\.\w+', '', text)
        text = re.sub(r'\b\w+_\w+\b', '', text)
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'[#$^*<>|\\`~]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


# Singleton
_analyzer: Optional[SpeechAnalyzer] = None


def get_speech_analyzer() -> SpeechAnalyzer:
    """Get speech analyzer instance."""
    global _analyzer
    if _analyzer is None:
        _analyzer = SpeechAnalyzer()
    return _analyzer


async def convert_to_speech(text: str) -> str:
    """Convert text to speech-ready format using local Ollama."""
    analyzer = get_speech_analyzer()
    return await analyzer.convert_to_speech(text)

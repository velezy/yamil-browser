"""
Paraphrase Generator Module
============================

Uses LLM to generate paraphrases of queries for improved retrieval recall.
Different phrasings of the same question can match different documents
that use varied terminology or writing styles.

Follows the same async pattern as hyde.py for consistency.
"""

import logging
import aiohttp
from typing import List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ParaphraseResult:
    """Result of paraphrase generation"""
    original_query: str
    paraphrases: List[str] = field(default_factory=list)
    generation_model: str = ""


class ParaphraseGenerator:
    """
    LLM-based query paraphrase generation for improved retrieval.

    Generates semantically equivalent but differently phrased versions
    of queries to improve retrieval recall. Documents may use different
    terminology or phrasing than the user's query.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "gemma2:2b"
    ):
        """
        Initialize paraphrase generator.

        Args:
            ollama_url: URL of Ollama API
            model: LLM model to use for paraphrase generation
        """
        self.ollama_url = ollama_url
        self.model = model

    def _build_paraphrase_prompt(self, query: str, num_paraphrases: int = 3) -> str:
        """Build the prompt for generating paraphrases."""
        return f"""Generate {num_paraphrases} different ways to ask the same question.
Each paraphrase should:
- Keep the exact same meaning and intent
- Use different words or sentence structure
- Be a complete, standalone question
- Not add new information or change the scope

Original question: {query}

Provide exactly {num_paraphrases} paraphrases, one per line, numbered 1-{num_paraphrases}.
Do not include any other text or explanation.

Paraphrases:"""

    async def generate_paraphrases(
        self,
        query: str,
        num_paraphrases: int = 3,
        temperature: float = 0.7
    ) -> ParaphraseResult:
        """
        Generate paraphrases of a query.

        Args:
            query: The query to paraphrase
            num_paraphrases: Number of paraphrases to generate
            temperature: LLM temperature for generation

        Returns:
            ParaphraseResult with generated paraphrases
        """
        result = ParaphraseResult(
            original_query=query,
            generation_model=self.model
        )

        prompt = self._build_paraphrase_prompt(query, num_paraphrases)

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": temperature,
                            "num_predict": 256,
                        }
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response_text = data.get('response', '').strip()

                        # Parse numbered paraphrases
                        paraphrases = self._parse_paraphrases(response_text)
                        result.paraphrases = paraphrases[:num_paraphrases]

                        logger.debug(f"Generated {len(result.paraphrases)} paraphrases for query")
                    else:
                        logger.warning(f"Paraphrase generation failed: HTTP {resp.status}")

            except aiohttp.ClientError as e:
                logger.error(f"Paraphrase generation connection error: {e}")
            except Exception as e:
                logger.error(f"Paraphrase generation error: {e}")

        return result

    def _parse_paraphrases(self, response: str) -> List[str]:
        """Parse numbered paraphrases from LLM response."""
        paraphrases = []
        lines = response.strip().split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Remove numbering patterns like "1.", "1)", "1:", etc.
            import re
            cleaned = re.sub(r'^[\d]+[.):]\s*', '', line)
            cleaned = cleaned.strip()

            # Skip if it looks like a header or explanation
            if cleaned and len(cleaned) > 10:
                # Avoid duplicates
                if cleaned not in paraphrases:
                    paraphrases.append(cleaned)

        return paraphrases

    async def generate_contextual_paraphrases(
        self,
        query: str,
        context_hint: Optional[str] = None,
        num_paraphrases: int = 3
    ) -> ParaphraseResult:
        """
        Generate paraphrases with optional context hint.

        Args:
            query: The query to paraphrase
            context_hint: Optional domain/topic hint
            num_paraphrases: Number of paraphrases to generate

        Returns:
            ParaphraseResult with generated paraphrases
        """
        result = ParaphraseResult(
            original_query=query,
            generation_model=self.model
        )

        context_part = ""
        if context_hint:
            context_part = f"\nDomain/Context: {context_hint}\n"

        prompt = f"""Generate {num_paraphrases} different ways to phrase the same technical question.
Each paraphrase should maintain the exact intent but use different terminology.
{context_part}
Original: {query}

Paraphrases (one per line, numbered):"""

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.8,
                            "num_predict": 256,
                        }
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response_text = data.get('response', '').strip()
                        paraphrases = self._parse_paraphrases(response_text)
                        result.paraphrases = paraphrases[:num_paraphrases]

            except Exception as e:
                logger.error(f"Contextual paraphrase generation error: {e}")

        return result


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

_paraphrase_generator: Optional[ParaphraseGenerator] = None


def get_paraphrase_generator(
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b"
) -> ParaphraseGenerator:
    """Get or create the global paraphrase generator instance."""
    global _paraphrase_generator
    if _paraphrase_generator is None:
        _paraphrase_generator = ParaphraseGenerator(
            ollama_url=ollama_url,
            model=model
        )
    return _paraphrase_generator


async def generate_paraphrases(
    query: str,
    num_paraphrases: int = 3,
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b"
) -> List[str]:
    """
    Convenience function to generate query paraphrases.

    Usage:
        paraphrases = await generate_paraphrases("How do I optimize SQL queries?")
        for p in paraphrases:
            # Search with each paraphrase
    """
    generator = get_paraphrase_generator(ollama_url, model)
    result = await generator.generate_paraphrases(query, num_paraphrases)
    return result.paraphrases

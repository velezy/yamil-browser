"""
Synthesis Agent

Generates coherent responses from retrieved context and tool outputs.
Uses a quality model for accurate, well-structured responses.
"""

import logging
import time
from typing import Optional, AsyncIterator, TYPE_CHECKING

from .base_agent import BaseAgent, AgentResult, AgentContext, AgentRole

if TYPE_CHECKING:
    from ..ollama.client import OllamaOptimizedClient
    from ..ollama.model_router import ModelRouter

logger = logging.getLogger(__name__)


class SynthesisAgent(BaseAgent):
    """
    Synthesizes a coherent response from:
    - Retrieved document chunks
    - Tool outputs (calculations, charts, etc.)
    - Conversation history

    Features:
    - Citation generation
    - Structured response formatting
    - Streaming support
    """

    def __init__(
        self,
        client: 'OllamaOptimizedClient',
        router: 'ModelRouter',
        default_model: Optional[str] = None,
        include_citations: bool = True,
        max_response_tokens: int = 2048,
    ):
        super().__init__(
            client=client,
            router=router,
            role=AgentRole.SYNTHESIS,
            default_model=default_model,
        )
        self.include_citations = include_citations
        self.max_response_tokens = max_response_tokens

    def _get_default_task_type(self) -> str:
        return "synthesis"  # Quality model for generation

    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Generate response from context.

        Updates context with:
        - current_response: Generated response text
        - citations: List of source citations
        """
        self._log_start(context)
        start_time = time.time()

        try:
            # Build the synthesis prompt
            prompt = self._build_synthesis_prompt(context)
            system = self._build_system_prompt(context)

            # Generate response
            response_text, full_result = await self._call_llm(
                prompt=prompt,
                system=system,
                options={
                    "num_predict": self.max_response_tokens,
                    "temperature": 0.7,
                },
                use_cache=False,  # Don't cache synthesis responses
            )

            # Extract citations if enabled
            citations = []
            if self.include_citations:
                citations = self._extract_citations(response_text, context)

            # Update context
            context.current_response = response_text
            context.citations = citations

            duration_ms = (time.time() - start_time) * 1000

            result = AgentResult(
                success=True,
                data={
                    "response": response_text,
                    "citations": citations,
                    "response_length": len(response_text),
                },
                duration_ms=duration_ms,
                model_used=self.get_model(),
                tokens_used=full_result.get('eval_count', 0),
            )

            self._log_complete(result, context)
            return result

        except Exception as e:
            logger.error(f"{self.name} error: {e}")
            return AgentResult(
                success=False,
                data={},
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )

    async def execute_stream(
        self,
        context: AgentContext,
    ) -> AsyncIterator[str]:
        """
        Stream response generation token by token.

        Yields response tokens as they're generated.
        """
        self._log_start(context)

        prompt = self._build_synthesis_prompt(context)
        system = self._build_system_prompt(context)

        model = self.get_model()
        full_response = ""

        try:
            async for token in self.client.generate_stream(
                model=model,
                prompt=prompt,
                system=system,
                options={
                    "num_predict": self.max_response_tokens,
                    "temperature": 0.7,
                },
            ):
                full_response += token
                yield token

            # Update context after streaming completes
            context.current_response = full_response
            if self.include_citations:
                context.citations = self._extract_citations(full_response, context)

            context.add_trace(self.name, "stream_complete", {
                "length": len(full_response),
            })

        except Exception as e:
            logger.error(f"Streaming synthesis error: {e}")
            yield f"\n\n[Error: {str(e)}]"

    def _build_system_prompt(self, context: AgentContext) -> str:
        """Build system prompt based on context"""
        base_prompt = """You are a helpful AI assistant that answers questions based on provided documents.

Guidelines:
1. Answer based ONLY on the provided context
2. If the context doesn't contain enough information, say so clearly
3. Be concise but comprehensive
4. Use clear, professional language"""

        if self.include_citations:
            base_prompt += """
5. When referencing specific information, cite the source using [1], [2], etc.
6. Include a "Sources" section at the end listing cited documents"""

        if context.required_tools:
            base_prompt += f"""
7. The following tools were used: {', '.join(context.required_tools)}
   Incorporate their outputs naturally into your response."""

        return base_prompt

    def _build_synthesis_prompt(self, context: AgentContext) -> str:
        """Build the synthesis prompt with context"""
        parts = []

        # Add original query
        parts.append(f"## User Question\n{context.query}")

        # Add sub-queries if decomposed
        if len(context.sub_queries) > 1:
            parts.append("\n## Sub-Questions to Address")
            for i, sq in enumerate(context.sub_queries, 1):
                parts.append(f"{i}. {sq}")

        # Add retrieved context
        if context.retrieved_chunks:
            parts.append("\n## Retrieved Documents")
            for i, chunk in enumerate(context.retrieved_chunks, 1):
                content = chunk.get('content', '')
                source = chunk.get('metadata', {}).get('source', 'Unknown')
                title = chunk.get('metadata', {}).get('title', '')

                source_info = f"[{i}] {title}" if title else f"[{i}] {source}"
                parts.append(f"\n### {source_info}")
                parts.append(content[:1500])  # Limit chunk size

        # Add tool outputs
        if context.tool_outputs:
            parts.append("\n## Tool Outputs")
            for tool_name, output in context.tool_outputs.items():
                parts.append(f"\n### {tool_name.title()}")
                parts.append(str(output)[:500])

        # Add previous response if iterating
        if context.iteration > 0 and context.feedback:
            parts.append("\n## Previous Attempt Feedback")
            parts.append(f"Previous response quality score: {context.quality_score:.2f}")
            parts.append(f"Feedback: {context.feedback}")
            parts.append("\nPlease improve your response based on this feedback.")

        # Final instruction
        parts.append("\n## Instructions")
        parts.append("Based on the above context, provide a comprehensive answer to the user's question.")

        return "\n".join(parts)

    def _extract_citations(
        self,
        response: str,
        context: AgentContext,
    ) -> list[dict]:
        """
        Extract citations from response and match to source documents.
        """
        citations = []
        import re

        # Find citation markers like [1], [2], etc.
        citation_pattern = r'\[(\d+)\]'
        found_citations = set(re.findall(citation_pattern, response))

        for citation_num in found_citations:
            idx = int(citation_num) - 1
            if 0 <= idx < len(context.retrieved_chunks):
                chunk = context.retrieved_chunks[idx]
                citations.append({
                    "index": int(citation_num),
                    "source": chunk.get('metadata', {}).get('source', 'Unknown'),
                    "title": chunk.get('metadata', {}).get('title', ''),
                    "content_preview": chunk.get('content', '')[:100],
                    "document_id": chunk.get('document_id'),
                })

        return sorted(citations, key=lambda x: x['index'])

    async def synthesize_with_feedback(
        self,
        context: AgentContext,
        feedback: str,
    ) -> AgentResult:
        """
        Re-synthesize response incorporating reflection feedback.
        """
        # Store feedback in context for prompt building
        context.feedback = feedback

        # Re-execute synthesis
        return await self.execute(context)

    async def summarize_chunks(
        self,
        chunks: list[dict],
        max_length: int = 500,
    ) -> str:
        """
        Summarize a list of chunks into a concise overview.
        Useful for very long contexts.
        """
        if not chunks:
            return ""

        combined_text = "\n\n".join(
            c.get('content', '')[:500] for c in chunks[:5]
        )

        prompt = f"""Summarize these document excerpts in {max_length} characters or less:

{combined_text}

Summary:"""

        try:
            summary, _ = await self._call_llm(
                prompt=prompt,
                options={"num_predict": max_length // 4, "temperature": 0.3},
            )
            return summary.strip()

        except Exception as e:
            logger.warning(f"Summarization failed: {e}")
            return combined_text[:max_length]

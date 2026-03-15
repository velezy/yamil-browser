"""
Retrieval Agent

Executes semantic search for each sub-query, deduplicates results,
and scores chunks by relevance.
"""

import logging
import time
import hashlib
from typing import Any, Optional, TYPE_CHECKING

from .base_agent import BaseAgent, AgentResult, AgentContext, AgentRole

if TYPE_CHECKING:
    from ..ollama.client import OllamaOptimizedClient
    from ..ollama.model_router import ModelRouter

logger = logging.getLogger(__name__)


class RetrievalAgent(BaseAgent):
    """
    Retrieves relevant document chunks for answering queries.

    Features:
    - Multi-query search (searches each sub-query)
    - Result deduplication
    - Relevance scoring
    - Context window optimization
    """

    def __init__(
        self,
        client: 'OllamaOptimizedClient',
        router: 'ModelRouter',
        vector_store: Any = None,
        embedding_model: Any = None,
        default_model: Optional[str] = None,
        top_k: int = 5,
        reranker: Any = None,
    ):
        super().__init__(
            client=client,
            router=router,
            role=AgentRole.RETRIEVAL,
            default_model=default_model,
        )
        self.vector_store = vector_store
        self.embedding_model = embedding_model
        self.top_k = top_k
        self.reranker = reranker

    def _get_default_task_type(self) -> str:
        return "simple_retrieval"

    def set_vector_store(self, vector_store: Any, embedding_model: Any = None):
        """Set vector store and optional embedding model"""
        self.vector_store = vector_store
        if embedding_model:
            self.embedding_model = embedding_model

    def set_reranker(self, reranker: Any):
        """Set reranker for improved scoring"""
        self.reranker = reranker

    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Retrieve relevant chunks for all sub-queries.

        Updates context with:
        - retrieved_chunks: List of deduplicated chunks
        - chunk_scores: Mapping of chunk IDs to relevance scores
        """
        self._log_start(context)
        start_time = time.time()

        if not self.vector_store:
            logger.error("Vector store not configured")
            return AgentResult(
                success=False,
                data={},
                error="Vector store not configured",
                duration_ms=0,
            )

        try:
            all_chunks = []
            seen_hashes = set()

            # Search for each sub-query
            for i, query in enumerate(context.sub_queries):
                context.add_trace(self.name, f"search_query_{i}", query)

                # Get more chunks per query to allow for deduplication
                chunks = await self._search_query(
                    query=query,
                    top_k=self.top_k * 2,
                    user_id=context.user_id,
                )

                # Deduplicate by content hash
                for chunk in chunks:
                    content = chunk.get('content', '')
                    content_hash = hashlib.md5(content.encode()).hexdigest()

                    if content_hash not in seen_hashes:
                        seen_hashes.add(content_hash)
                        chunk['source_query'] = query
                        chunk['query_index'] = i
                        all_chunks.append(chunk)

            logger.info(f"Retrieved {len(all_chunks)} unique chunks from {len(context.sub_queries)} queries")

            # Rerank if available
            if self.reranker and all_chunks:
                all_chunks = await self._rerank_chunks(
                    query=context.query,  # Use original query for reranking
                    chunks=all_chunks,
                )
                context.add_trace(self.name, "rerank", f"{len(all_chunks)} chunks")

            # Score and filter chunks
            scored_chunks = self._score_chunks(all_chunks)

            # Keep top_k chunks
            final_chunks = scored_chunks[:self.top_k]

            # Update context
            context.retrieved_chunks = final_chunks
            context.chunk_scores = {
                chunk.get('id', str(i)): chunk.get('score', 0.0)
                for i, chunk in enumerate(final_chunks)
            }

            duration_ms = (time.time() - start_time) * 1000

            result = AgentResult(
                success=True,
                data={
                    "chunks_retrieved": len(final_chunks),
                    "queries_searched": len(context.sub_queries),
                    "dedup_count": len(seen_hashes) - len(final_chunks),
                },
                duration_ms=duration_ms,
                model_used=None,  # No LLM used in basic retrieval
                metadata={
                    "top_scores": [c.get('score', 0) for c in final_chunks[:3]],
                },
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

    async def _search_query(
        self,
        query: str,
        top_k: int,
        user_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Execute semantic search for a single query.
        """
        try:
            # Generate embedding for query
            if self.embedding_model is None:
                logger.error("Embedding model not configured")
                return []

            query_embedding = self.embedding_model.encode(query).tolist()

            # Search vector store with embedding
            results = await self.vector_store.search(
                query_embedding=query_embedding,
                limit=top_k,
            )

            return results

        except Exception as e:
            logger.error(f"Search failed for query: {e}")
            return []

    async def _rerank_chunks(
        self,
        query: str,
        chunks: list[dict],
    ) -> list[dict]:
        """
        Rerank chunks using neural reranker for better relevance.
        """
        try:
            if hasattr(self.reranker, 'rerank'):
                # Use reranker module
                documents = [c.get('content', '') for c in chunks]
                reranked = await self.reranker.rerank(
                    query=query,
                    documents=documents,
                    top_k=len(chunks),
                )

                # Merge rerank scores back to chunks
                for i, score in enumerate(reranked.get('scores', [])):
                    if i < len(chunks):
                        chunks[i]['rerank_score'] = score
                        # Combine with original score
                        orig_score = chunks[i].get('score', 0.5)
                        chunks[i]['score'] = 0.4 * orig_score + 0.6 * score

                # Sort by new combined score
                chunks.sort(key=lambda x: x.get('score', 0), reverse=True)

            return chunks

        except Exception as e:
            logger.warning(f"Reranking failed: {e}")
            return chunks

    def _score_chunks(self, chunks: list[dict]) -> list[dict]:
        """
        Score and sort chunks by relevance.

        Scoring factors:
        - Vector similarity score
        - Rerank score (if available)
        - Recency (if timestamps available)
        - Source diversity
        """
        for chunk in chunks:
            score = chunk.get('score', 0.5)

            # Boost recent documents
            if 'created_at' in chunk.get('metadata', {}):
                # Simple recency boost (could be more sophisticated)
                score *= 1.05

            # Boost documents with good metadata
            if chunk.get('metadata', {}).get('title'):
                score *= 1.02

            chunk['final_score'] = score

        # Sort by final score
        chunks.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        return chunks

    async def retrieve_with_feedback(
        self,
        context: AgentContext,
        feedback: str,
    ) -> AgentResult:
        """
        Re-retrieve with improved strategy based on feedback.

        Called when reflection agent indicates retrieval was insufficient.
        """
        self._log_start(context)
        start_time = time.time()

        # Analyze feedback to adjust strategy
        adjustments = await self._analyze_feedback(feedback)

        # Apply adjustments
        original_top_k = self.top_k

        if adjustments.get('need_more_context'):
            self.top_k = min(self.top_k * 2, 20)

        if adjustments.get('different_queries'):
            # Generate alternative queries
            alt_queries = adjustments.get('suggested_queries', [])
            if alt_queries:
                context.sub_queries = context.sub_queries + alt_queries

        # Re-execute search
        result = await self.execute(context)

        # Restore original settings
        self.top_k = original_top_k

        return result

    async def _analyze_feedback(self, feedback: str) -> dict:
        """
        Analyze reflection feedback to determine retrieval adjustments.
        """
        prompt = f"""Analyze this feedback about retrieved documents and suggest improvements:

Feedback: {feedback}

What adjustments should be made to the retrieval strategy?

Respond with JSON:
{{
    "need_more_context": true/false,
    "different_queries": true/false,
    "suggested_queries": ["alternative query 1", "..."],
    "reasoning": "explanation"
}}"""

        try:
            parsed, _ = await self._call_llm_json(
                prompt=prompt,
                options={"num_predict": 256, "temperature": 0.1},
            )
            return parsed

        except Exception as e:
            logger.warning(f"Feedback analysis failed: {e}")
            return {"need_more_context": True}

    def get_context_window_usage(self, chunks: list[dict]) -> dict:
        """
        Calculate token usage of retrieved chunks.
        """
        total_chars = sum(len(c.get('content', '')) for c in chunks)
        # Rough token estimate: ~4 chars per token
        estimated_tokens = total_chars // 4

        return {
            "chunks": len(chunks),
            "total_chars": total_chars,
            "estimated_tokens": estimated_tokens,
            "utilization_percent": min(100, estimated_tokens / 4000 * 100),
        }

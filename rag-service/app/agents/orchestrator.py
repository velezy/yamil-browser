"""
Agent Pipeline Orchestrator

Coordinates the multi-agent RAG pipeline:
Query Planner → Retrieval → Synthesis → Reflection → [Loop or Return]
"""

import logging
import time
from typing import Any, Optional, AsyncIterator
from dataclasses import dataclass, field

from .base_agent import AgentContext, AgentResult
from .query_planner import QueryPlannerAgent
from .retrieval_agent import RetrievalAgent
from .synthesis_agent import SynthesisAgent
from .reflection_agent import ReflectionAgent

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Final result from the agentic pipeline"""
    success: bool
    response: str
    citations: list[dict]

    # Metrics
    total_duration_ms: float
    iterations: int
    quality_score: float

    # Agent results
    agent_results: dict[str, AgentResult] = field(default_factory=dict)

    # Debugging
    context: Optional[AgentContext] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "response": self.response,
            "citations": self.citations,
            "metrics": {
                "total_duration_ms": self.total_duration_ms,
                "iterations": self.iterations,
                "quality_score": self.quality_score,
            },
            "error": self.error,
        }


@dataclass
class StreamingUpdate:
    """Update during streaming execution"""
    type: str  # 'status', 'token', 'complete', 'error'
    agent: Optional[str] = None
    data: Any = None
    timestamp: float = field(default_factory=time.time)


class AgentOrchestrator:
    """
    Orchestrates the multi-agent RAG pipeline.

    Pipeline flow:
    1. Query Planner: Decompose query, identify tools
    2. Retrieval Agent: Search for relevant chunks
    3. (Optional) Tool Execution: Run calculator, charts, etc.
    4. Synthesis Agent: Generate response
    5. Reflection Agent: Evaluate quality
    6. Decision: Return OR loop back with feedback

    The pipeline iterates until:
    - Quality score >= threshold
    - Max iterations reached
    """

    def __init__(
        self,
        query_planner: QueryPlannerAgent,
        retrieval_agent: RetrievalAgent,
        synthesis_agent: SynthesisAgent,
        reflection_agent: ReflectionAgent,
        max_iterations: int = 3,
        quality_threshold: float = 0.75,
        enable_tools: bool = True,
    ):
        self.query_planner = query_planner
        self.retrieval_agent = retrieval_agent
        self.synthesis_agent = synthesis_agent
        self.reflection_agent = reflection_agent

        self.max_iterations = max_iterations
        self.quality_threshold = quality_threshold
        self.enable_tools = enable_tools

        # Tool handlers (to be set externally)
        self.tool_handlers: dict[str, Any] = {}

    def register_tool(self, name: str, handler: Any):
        """Register a tool handler"""
        self.tool_handlers[name] = handler
        logger.info(f"Registered tool: {name}")

    async def execute(
        self,
        query: str,
        user_id: Optional[int] = None,
        conversation_id: Optional[str] = None,
        config: Optional[dict] = None,
    ) -> PipelineResult:
        """
        Execute the full agentic pipeline.

        Args:
            query: User query
            user_id: Optional user ID for personalization
            conversation_id: Optional conversation context
            config: Optional pipeline configuration

        Returns:
            PipelineResult with response and metadata
        """
        start_time = time.time()
        config = config or {}

        # Initialize context
        context = AgentContext(
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
            max_iterations=self.max_iterations,
            config=config,
        )

        agent_results = {}

        try:
            # Main pipeline loop
            while context.iteration < self.max_iterations:
                logger.info(f"🔄 Pipeline iteration {context.iteration + 1}/{self.max_iterations}")

                # Step 1: Query Planning (only on first iteration or if replanning)
                if context.iteration == 0:
                    planner_result = await self.query_planner.execute(context)
                    agent_results['query_planner'] = planner_result

                    if not planner_result.success:
                        logger.warning("Query planning failed, using original query")

                # Step 2: Retrieval
                retrieval_result = await self.retrieval_agent.execute(context)
                agent_results['retrieval'] = retrieval_result

                if not retrieval_result.success:
                    logger.warning("Retrieval failed")
                    # Continue with empty context if retrieval fails

                # Step 3: Tool Execution (if needed)
                if self.enable_tools and context.required_tools:
                    await self._execute_tools(context)

                # Step 4: Synthesis
                synthesis_result = await self.synthesis_agent.execute(context)
                agent_results['synthesis'] = synthesis_result

                if not synthesis_result.success:
                    raise Exception(f"Synthesis failed: {synthesis_result.error}")

                # Step 5: Reflection
                reflection_result = await self.reflection_agent.execute(context)
                agent_results['reflection'] = reflection_result

                # Check if we should continue
                should_iterate = reflection_result.data.get('should_iterate', False)

                if not should_iterate:
                    # Quality threshold met or max iterations
                    break

                # Prepare for next iteration
                context.iteration += 1

                # Optionally replan based on feedback
                if context.iteration < self.max_iterations:
                    await self._prepare_next_iteration(context, agent_results)

            # Build final result
            total_duration = (time.time() - start_time) * 1000

            return PipelineResult(
                success=True,
                response=context.current_response,
                citations=context.citations,
                total_duration_ms=total_duration,
                iterations=context.iteration + 1,
                quality_score=context.quality_score,
                agent_results=agent_results,
                context=context,
            )

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            total_duration = (time.time() - start_time) * 1000

            return PipelineResult(
                success=False,
                response=context.current_response or f"Error: {str(e)}",
                citations=[],
                total_duration_ms=total_duration,
                iterations=context.iteration + 1,
                quality_score=context.quality_score,
                agent_results=agent_results,
                context=context,
                error=str(e),
            )

    async def execute_stream(
        self,
        query: str,
        user_id: Optional[int] = None,
        conversation_id: Optional[str] = None,
        config: Optional[dict] = None,
    ) -> AsyncIterator[StreamingUpdate]:
        """
        Execute pipeline with streaming updates.

        Yields StreamingUpdate objects for real-time feedback.
        """
        start_time = time.time()
        config = config or {}

        context = AgentContext(
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
            max_iterations=self.max_iterations,
            config=config,
        )

        yield StreamingUpdate(type='status', agent='orchestrator', data='Starting pipeline')

        try:
            # Step 1: Query Planning
            yield StreamingUpdate(type='status', agent='query_planner', data='Planning query')
            planner_result = await self.query_planner.execute(context)

            yield StreamingUpdate(
                type='status',
                agent='query_planner',
                data=f"Decomposed into {len(context.sub_queries)} sub-queries"
            )

            # Step 2: Retrieval
            yield StreamingUpdate(type='status', agent='retrieval', data='Searching documents')
            await self.retrieval_agent.execute(context)

            yield StreamingUpdate(
                type='status',
                agent='retrieval',
                data=f"Found {len(context.retrieved_chunks)} relevant chunks"
            )

            # Step 3: Tools (if needed)
            if self.enable_tools and context.required_tools:
                yield StreamingUpdate(
                    type='status',
                    agent='tools',
                    data=f"Executing tools: {', '.join(context.required_tools)}"
                )
                await self._execute_tools(context)

            # Step 4: Streaming Synthesis
            yield StreamingUpdate(type='status', agent='synthesis', data='Generating response')

            async for token in self.synthesis_agent.execute_stream(context):
                yield StreamingUpdate(type='token', agent='synthesis', data=token)

            # Step 5: Reflection (quick for streaming)
            yield StreamingUpdate(type='status', agent='reflection', data='Evaluating response')
            await self.reflection_agent.execute(context)

            # Complete
            total_duration = (time.time() - start_time) * 1000

            yield StreamingUpdate(
                type='complete',
                agent='orchestrator',
                data={
                    'response': context.current_response,
                    'citations': context.citations,
                    'quality_score': context.quality_score,
                    'duration_ms': total_duration,
                }
            )

        except Exception as e:
            logger.error(f"Streaming pipeline error: {e}")
            yield StreamingUpdate(type='error', agent='orchestrator', data=str(e))

    async def _execute_tools(self, context: AgentContext):
        """Execute required tools and store outputs"""
        for tool_name in context.required_tools:
            if tool_name in self.tool_handlers:
                try:
                    handler = self.tool_handlers[tool_name]
                    output = await handler.execute(context)
                    context.tool_outputs[tool_name] = output
                    context.add_trace('tools', f'execute_{tool_name}', output)
                    logger.info(f"Tool {tool_name} executed successfully")
                except Exception as e:
                    logger.error(f"Tool {tool_name} failed: {e}")
                    context.tool_outputs[tool_name] = f"Error: {str(e)}"
            else:
                logger.warning(f"Tool {tool_name} not registered")

    async def _prepare_next_iteration(
        self,
        context: AgentContext,
        agent_results: dict,
    ):
        """Prepare for next iteration based on feedback"""
        # Get feedback from reflection
        feedback = context.feedback

        # Optionally re-retrieve with different strategy
        if 'insufficient context' in feedback.lower():
            logger.info("Re-retrieving with expanded search")
            await self.retrieval_agent.retrieve_with_feedback(context, feedback)

        # Optionally replan queries
        if 'missing' in feedback.lower() or 'incomplete' in feedback.lower():
            logger.info("Replanning queries based on feedback")
            await self.query_planner.replan_with_feedback(context, feedback)

    def get_pipeline_stats(self) -> dict:
        """Get pipeline configuration and stats"""
        return {
            "max_iterations": self.max_iterations,
            "quality_threshold": self.quality_threshold,
            "enable_tools": self.enable_tools,
            "registered_tools": list(self.tool_handlers.keys()),
            "agents": {
                "query_planner": self.query_planner.name,
                "retrieval": self.retrieval_agent.name,
                "synthesis": self.synthesis_agent.name,
                "reflection": self.reflection_agent.name,
            },
        }


# Factory function for easy setup
def create_agent_pipeline(
    ollama_client: Any,
    model_router: Any,
    vector_store: Any = None,
    embedding_model: Any = None,
    reranker: Any = None,
    max_iterations: int = 3,
    quality_threshold: float = 0.75,
) -> AgentOrchestrator:
    """
    Create a fully configured agent pipeline.

    Args:
        ollama_client: OllamaOptimizedClient instance
        model_router: ModelRouter instance
        vector_store: Vector store for retrieval
        embedding_model: Embedding model
        reranker: Optional reranker for improved retrieval
        max_iterations: Maximum pipeline iterations
        quality_threshold: Minimum quality score to pass

    Returns:
        Configured AgentOrchestrator
    """
    # Create agents
    query_planner = QueryPlannerAgent(
        client=ollama_client,
        router=model_router,
    )

    retrieval_agent = RetrievalAgent(
        client=ollama_client,
        router=model_router,
        vector_store=vector_store,
        embedding_model=embedding_model,
        reranker=reranker,
    )

    synthesis_agent = SynthesisAgent(
        client=ollama_client,
        router=model_router,
    )

    reflection_agent = ReflectionAgent(
        client=ollama_client,
        router=model_router,
        quality_threshold=quality_threshold,
        max_iterations=max_iterations,
    )

    # Create orchestrator
    orchestrator = AgentOrchestrator(
        query_planner=query_planner,
        retrieval_agent=retrieval_agent,
        synthesis_agent=synthesis_agent,
        reflection_agent=reflection_agent,
        max_iterations=max_iterations,
        quality_threshold=quality_threshold,
    )

    logger.info("Agent pipeline created successfully")
    return orchestrator

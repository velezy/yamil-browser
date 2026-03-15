"""
Base Agent Class

Abstract base for all agents in the multi-agent RAG pipeline.
Each agent has a specific role and can communicate with the LLM.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING
from enum import Enum

if TYPE_CHECKING:
    from ..ollama.client import OllamaOptimizedClient
    from ..ollama.model_router import ModelRouter
    from ..ollama.task_router import AITaskRouter

logger = logging.getLogger(__name__)

# Import task router for FlashCards-style routing
try:
    from ..ollama.task_router import get_task_router, get_model_for_task
    TASK_ROUTER_AVAILABLE = True
except ImportError:
    TASK_ROUTER_AVAILABLE = False
    logger.debug("Task router not available, using legacy routing")


class AgentRole(Enum):
    """Roles for different agents in the pipeline"""
    QUERY_PLANNER = "query_planner"
    RETRIEVAL = "retrieval"
    SYNTHESIS = "synthesis"
    REFLECTION = "reflection"
    TOOL = "tool"


@dataclass
class AgentResult:
    """Result from an agent's execution"""
    success: bool
    data: Any
    error: Optional[str] = None
    duration_ms: float = 0.0
    model_used: Optional[str] = None
    tokens_used: int = 0
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
            "metadata": self.metadata,
        }


@dataclass
class AgentContext:
    """
    Shared context passed between agents in the pipeline.

    Contains the original query, intermediate results, and configuration.
    """
    # Original user query
    query: str

    # User context
    user_id: Optional[int] = None
    conversation_id: Optional[str] = None

    # Query planning results
    sub_queries: list[str] = field(default_factory=list)
    required_tools: list[str] = field(default_factory=list)
    query_complexity: str = "medium"  # simple, medium, complex

    # Retrieval results
    retrieved_chunks: list[dict] = field(default_factory=list)
    chunk_scores: dict[str, float] = field(default_factory=dict)

    # Synthesis results
    current_response: str = ""
    citations: list[dict] = field(default_factory=list)

    # Reflection feedback
    quality_score: float = 0.0
    feedback: str = ""
    iteration: int = 0
    max_iterations: int = 3

    # Tool outputs
    tool_outputs: dict[str, Any] = field(default_factory=dict)

    # Configuration
    config: dict = field(default_factory=dict)

    # Execution trace for debugging
    trace: list[dict] = field(default_factory=list)

    def add_trace(self, agent: str, action: str, details: Any = None):
        """Add execution trace entry"""
        self.trace.append({
            "agent": agent,
            "action": action,
            "details": details,
            "timestamp": time.time(),
        })

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "user_id": self.user_id,
            "conversation_id": self.conversation_id,
            "sub_queries": self.sub_queries,
            "required_tools": self.required_tools,
            "query_complexity": self.query_complexity,
            "retrieved_chunks_count": len(self.retrieved_chunks),
            "current_response": self.current_response[:200] + "..." if len(self.current_response) > 200 else self.current_response,
            "citations_count": len(self.citations),
            "quality_score": self.quality_score,
            "iteration": self.iteration,
            "tool_outputs": list(self.tool_outputs.keys()),
        }


class BaseAgent(ABC):
    """
    Abstract base class for all agents.

    Each agent:
    - Has a specific role (query planning, retrieval, synthesis, reflection)
    - Uses an LLM for reasoning
    - Produces structured output
    - Can access shared context
    """

    def __init__(
        self,
        client: 'OllamaOptimizedClient',
        router: 'ModelRouter',
        role: AgentRole,
        default_model: Optional[str] = None,
    ):
        """
        Initialize agent.

        Args:
            client: Ollama client for LLM calls
            router: Model router for task-based model selection
            role: Agent's role in the pipeline
            default_model: Override model selection
        """
        self.client = client
        self.router = router
        self.role = role
        self.default_model = default_model
        self.name = self.__class__.__name__

    def get_model(self, task_type: Optional[str] = None) -> str:
        """
        Get appropriate model for this agent's task.

        Uses FlashCards-style task router for explicit mappings,
        falls back to legacy router if not available.
        """
        if self.default_model:
            return self.default_model

        task = task_type or self._get_default_task_type()

        # Use FlashCards-style task router (preferred)
        if TASK_ROUTER_AVAILABLE:
            model = get_model_for_task(task)
            logger.debug(f"{self.name}: Task '{task}' → Model '{model}' (task router)")
            return model

        # Fallback to legacy router
        return self.router.select_model(task)

    @abstractmethod
    def _get_default_task_type(self) -> str:
        """Return the default task type for model selection"""
        pass

    @abstractmethod
    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Execute the agent's main logic.

        Args:
            context: Shared pipeline context

        Returns:
            AgentResult with success status and data
        """
        pass

    async def _call_llm(
        self,
        prompt: str,
        system: Optional[str] = None,
        model: Optional[str] = None,
        options: Optional[dict] = None,
        use_cache: bool = True,
    ) -> tuple[str, dict]:
        """
        Call the LLM with the given prompt.

        Args:
            prompt: User prompt
            system: System prompt
            model: Model override
            options: Model options
            use_cache: Whether to cache response

        Returns:
            Tuple of (response_text, full_response_dict)
        """
        model = model or self.get_model()
        options = options or {}

        # Set sensible defaults based on role
        if 'temperature' not in options:
            if self.role in [AgentRole.QUERY_PLANNER, AgentRole.RETRIEVAL]:
                options['temperature'] = 0.1  # More deterministic
            elif self.role == AgentRole.REFLECTION:
                options['temperature'] = 0.3  # Balanced
            else:
                options['temperature'] = 0.7  # More creative

        start_time = time.time()

        result = await self.client.generate(
            model=model,
            prompt=prompt,
            system=system,
            options=options,
            use_cache=use_cache,
        )

        duration = (time.time() - start_time) * 1000
        response_text = result.get('response', '')

        logger.debug(
            f"{self.name} LLM call: model={model}, "
            f"duration={duration:.0f}ms, tokens={result.get('eval_count', 0)}"
        )

        return response_text, result

    async def _call_llm_json(
        self,
        prompt: str,
        system: Optional[str] = None,
        model: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> tuple[dict, dict]:
        """
        Call LLM expecting JSON response.

        Returns:
            Tuple of (parsed_json, full_response_dict)
        """
        import json

        # Add JSON instruction to prompt
        json_prompt = f"""{prompt}

IMPORTANT: Respond with valid JSON only. No explanations or markdown."""

        response_text, full_result = await self._call_llm(
            prompt=json_prompt,
            system=system,
            model=model,
            options=options,
            use_cache=False,  # JSON responses should be fresh
        )

        # Parse JSON from response
        try:
            # Try to extract JSON from response
            text = response_text.strip()

            # Handle markdown code blocks
            if text.startswith('```'):
                lines = text.split('\n')
                json_lines = []
                in_block = False
                for line in lines:
                    if line.startswith('```'):
                        in_block = not in_block
                        continue
                    if in_block:
                        json_lines.append(line)
                text = '\n'.join(json_lines)

            parsed = json.loads(text)
            return parsed, full_result

        except json.JSONDecodeError as e:
            logger.warning(f"{self.name} JSON parse error: {e}")
            # Return empty dict on parse failure
            return {}, full_result

    def _log_start(self, context: AgentContext):
        """Log agent execution start"""
        logger.info(f"🤖 {self.name} starting (iteration {context.iteration})")
        context.add_trace(self.name, "start")

    def _log_complete(self, result: AgentResult, context: AgentContext):
        """Log agent execution complete"""
        status = "✅" if result.success else "❌"
        logger.info(
            f"{status} {self.name} complete: "
            f"duration={result.duration_ms:.0f}ms, "
            f"model={result.model_used}"
        )
        context.add_trace(self.name, "complete", {
            "success": result.success,
            "duration_ms": result.duration_ms,
        })

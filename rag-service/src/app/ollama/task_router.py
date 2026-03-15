"""
AI Task Router - Multi-Model Orchestra (Ported from FlashCards)
================================================================

Intelligent task routing with explicit task-to-model mappings,
fallback chains, and performance tracking.

Model Strategy:
- gemma2:2b (1.6 GB) - Ultra Fast: Classification, simple extraction
- llama3.2:3b (2.0 GB) - Fast Agent: Quick responses, routing
- gemma3:4b (3.3 GB) - Conductor: Orchestration, summarization
- llama3.1:8b (4.9 GB) - Deep Agent: Synthesis, reasoning, reflection
"""

import os
import logging
import asyncio
import time
from typing import Dict, Optional, List, Any, Callable
from enum import Enum
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class AIRole(str, Enum):
    """AI role types for specialized task handling"""
    CONDUCTOR = "conductor"           # Orchestrates agents, quality control
    FAST_OPS = "fast_ops"            # Quick responses, classification
    DEEP_REASONING = "deep_reasoning" # Complex analysis, synthesis
    RETRIEVAL = "retrieval"          # Document retrieval, search
    ULTRA_FAST = "ultra_fast"        # Fastest possible responses


class AIAgent(str, Enum):
    """Available AI agents in the orchestra"""
    ULTRA_FAST = "ultra_fast_agent"  # gemma2:2b - Speed demon
    FAST = "fast_agent"              # llama3.2:3b - Quick responses
    CONDUCTOR = "conductor_agent"    # gemma3:4b - Orchestrator
    DEEP = "deep_agent"              # llama3.1:8b - Accuracy specialist


@dataclass
class ExecutionResult:
    """Result of an AI task execution with performance metadata"""
    success: bool
    model: str
    output: Any
    task: str
    execution_time_ms: float
    fallback_used: bool = False
    fallback_model: Optional[str] = None
    confidence: float = 1.0
    error: Optional[str] = None


@dataclass
class ModelPerformance:
    """Track model performance metrics"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests


class AITaskRouter:
    """
    Intelligent AI task routing system - Multi-Model Orchestra

    Ported from FlashCards with enhancements for RAG pipeline.
    """

    # Agent-to-model mapping
    AGENT_MODEL_MAP = {
        AIAgent.ULTRA_FAST: {
            'model': 'gemma2:2b',
            'description': 'Ultra-fast responses, classification, simple extraction',
            'strengths': ['speed', 'classification', 'simple_qa', 'extraction'],
            'ram_gb': 1.6,
            'avg_time': 1.5,
            'speed': 'ultra_fast',
            'accuracy': 'good'
        },
        AIAgent.FAST: {
            'model': 'llama3.2:3b',
            'description': 'Fast responses, chat, routing decisions',
            'strengths': ['chat', 'routing', 'simple_qa', 'classification'],
            'ram_gb': 2.0,
            'avg_time': 3.0,
            'speed': 'fast',
            'accuracy': 'good'
        },
        AIAgent.CONDUCTOR: {
            'model': 'gemma3:4b',
            'description': 'Orchestration, summarization, tool selection',
            'strengths': ['orchestration', 'summarization', 'tool_selection', 'code_gen'],
            'ram_gb': 3.3,
            'avg_time': 4.0,
            'speed': 'medium',
            'accuracy': 'high'
        },
        AIAgent.DEEP: {
            'model': 'llama3.1:8b',
            'description': 'Complex analysis, synthesis, reasoning, reflection',
            'strengths': ['synthesis', 'reasoning', 'reflection', 'complex_analysis'],
            'ram_gb': 4.9,
            'avg_time': 8.0,
            'speed': 'slow',
            'accuracy': 'excellent'
        },
    }

    # Direct task-to-model mapping for fine-grained control
    TASK_TO_MODEL_MAP = {
        # ═══════════════════════════════════════════════════════════════
        # ULTRA FAST AGENT (gemma2:2b) - Speed-critical tasks
        # ═══════════════════════════════════════════════════════════════
        'intent_classification': 'gemma2:2b',
        'query_classification': 'gemma2:2b',
        'simple_extraction': 'gemma2:2b',
        'yes_no_question': 'gemma2:2b',
        'entity_extraction': 'gemma2:2b',
        'sentiment_analysis': 'gemma2:2b',
        'language_detection': 'gemma2:2b',

        # ═══════════════════════════════════════════════════════════════
        # FAST AGENT (llama3.2:3b) - Quick but capable tasks
        # ═══════════════════════════════════════════════════════════════
        'chat': 'llama3.2:3b',
        'simple_qa': 'llama3.2:3b',
        'query_planning': 'llama3.2:3b',
        'query_decomposition': 'llama3.2:3b',
        'tool_detection': 'llama3.2:3b',
        'quick_response': 'llama3.2:3b',
        'classification': 'llama3.2:3b',
        'routing': 'llama3.2:3b',
        'simple_retrieval': 'llama3.2:3b',  # Retrieval feedback analysis

        # ═══════════════════════════════════════════════════════════════
        # CONDUCTOR AGENT (gemma3:4b) - Orchestration tasks
        # ═══════════════════════════════════════════════════════════════
        'summarization': 'gemma3:4b',
        'tool_selection': 'gemma3:4b',
        'code_generation': 'gemma3:4b',
        'orchestration': 'gemma3:4b',
        'response_formatting': 'gemma3:4b',
        'context_building': 'gemma3:4b',

        # ═══════════════════════════════════════════════════════════════
        # DEEP AGENT (llama3.1:8b) - Accuracy-critical tasks
        # ═══════════════════════════════════════════════════════════════
        'synthesis': 'llama3.1:8b',
        'reasoning': 'llama3.1:8b',
        'reflection': 'llama3.1:8b',
        'quality_evaluation': 'llama3.1:8b',
        'complex_analysis': 'llama3.1:8b',
        'document_analysis': 'llama3.1:8b',
        'rag_synthesis': 'llama3.1:8b',
        'answer_generation': 'llama3.1:8b',
        'citation_extraction': 'llama3.1:8b',
        'feedback_analysis': 'llama3.1:8b',
    }

    # Task-specific fallback chains
    TASK_FALLBACK_CHAINS = {
        'synthesis': ['llama3.1:8b', 'gemma3:4b', 'llama3.2:3b'],
        'reflection': ['llama3.1:8b', 'gemma3:4b', 'llama3.2:3b'],
        'reasoning': ['llama3.1:8b', 'gemma3:4b', 'llama3.2:3b'],
        'classification': ['gemma2:2b', 'llama3.2:3b', 'gemma3:4b'],
        'query_planning': ['llama3.2:3b', 'gemma3:4b', 'gemma2:2b'],
        'chat': ['llama3.2:3b', 'gemma3:4b', 'llama3.1:8b'],
        'summarization': ['gemma3:4b', 'llama3.1:8b', 'llama3.2:3b'],
    }

    # Confidence thresholds for retry decisions
    CONFIDENCE_THRESHOLDS = {
        'synthesis': 0.75,
        'reflection': 0.80,
        'reasoning': 0.75,
        'quality_evaluation': 0.85,
        'classification': 0.60,
        'default': 0.70
    }

    def __init__(self):
        """Initialize the AI task router"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self._performance: Dict[str, Dict[str, ModelPerformance]] = {}
        self._model_last_used: Dict[str, float] = {}
        self._loaded_models: set = set()
        self._warmup_complete = False

    def get_model_for_task(self, task: str) -> str:
        """
        Get the optimal AI model for a specific task.
        Uses direct task-to-model mapping.
        """
        # Priority 1: Direct task-to-model mapping
        if task in self.TASK_TO_MODEL_MAP:
            model = self.TASK_TO_MODEL_MAP[task]
            self.logger.debug(f"Task '{task}' → Direct mapping → {model}")
            return model

        # Priority 2: Check if task matches any strength
        for agent, config in self.AGENT_MODEL_MAP.items():
            if task in config.get('strengths', []):
                model = config['model']
                self.logger.debug(f"Task '{task}' → Strength match → {model}")
                return model

        # Priority 3: Default to conductor model
        self.logger.warning(f"Unknown task '{task}', using conductor model")
        return 'gemma3:4b'

    def get_agent_for_task(self, task: str) -> AIAgent:
        """Get the AI agent responsible for a specific task"""
        model = self.get_model_for_task(task)

        for agent, config in self.AGENT_MODEL_MAP.items():
            if config['model'] == model:
                return agent

        return AIAgent.CONDUCTOR

    def get_fallback_chain(self, task: str) -> List[str]:
        """Get fallback chain for a specific task"""
        if task in self.TASK_FALLBACK_CHAINS:
            return self.TASK_FALLBACK_CHAINS[task].copy()

        # Default chain: primary model first, then others by speed
        primary = self.get_model_for_task(task)
        all_models = ['gemma2:2b', 'llama3.2:3b', 'gemma3:4b', 'llama3.1:8b']
        chain = [primary] + [m for m in all_models if m != primary]
        return chain

    def get_confidence_threshold(self, task: str) -> float:
        """Get confidence threshold for a task"""
        return self.CONFIDENCE_THRESHOLDS.get(task, self.CONFIDENCE_THRESHOLDS['default'])

    def get_task_info(self, task: str) -> Dict:
        """Get detailed information about a task's routing"""
        model = self.get_model_for_task(task)
        agent = self.get_agent_for_task(task)
        agent_config = self.AGENT_MODEL_MAP.get(agent, {})

        return {
            'task': task,
            'model': model,
            'agent': agent.value,
            'description': agent_config.get('description', 'Unknown'),
            'strengths': agent_config.get('strengths', []),
            'speed': agent_config.get('speed', 'medium'),
            'accuracy': agent_config.get('accuracy', 'good'),
            'ram_gb': agent_config.get('ram_gb', 0),
            'avg_time': agent_config.get('avg_time', 5.0),
            'fallback_chain': self.get_fallback_chain(task),
            'confidence_threshold': self.get_confidence_threshold(task),
        }

    def record_success(self, task: str, model: str, latency_ms: float):
        """Record successful execution"""
        if task not in self._performance:
            self._performance[task] = {}
        if model not in self._performance[task]:
            self._performance[task][model] = ModelPerformance()

        perf = self._performance[task][model]
        perf.total_requests += 1
        perf.successful_requests += 1
        perf.total_latency_ms += latency_ms

        self._model_last_used[model] = time.time()

    def record_failure(self, task: str, model: str, error: str):
        """Record failed execution"""
        if task not in self._performance:
            self._performance[task] = {}
        if model not in self._performance[task]:
            self._performance[task][model] = ModelPerformance()

        perf = self._performance[task][model]
        perf.total_requests += 1
        perf.failed_requests += 1

        self.logger.warning(f"Task '{task}' failed on model '{model}': {error}")

    def get_best_model_for_task(self, task: str) -> str:
        """
        Get best performing model for a task based on historical data.
        Falls back to configured model if no data available.
        """
        if task in self._performance:
            task_perf = self._performance[task]
            best_model = None
            best_score = -1

            for model, perf in task_perf.items():
                if perf.total_requests >= 3:  # Need at least 3 requests
                    # Score = success_rate * (1 / avg_latency_normalized)
                    score = perf.success_rate * (1000 / max(perf.avg_latency_ms, 100))
                    if score > best_score:
                        best_score = score
                        best_model = model

            if best_model:
                self.logger.debug(f"Performance tracker suggests {best_model} for {task}")
                return best_model

        return self.get_model_for_task(task)

    def is_model_warm(self, model: str) -> bool:
        """Check if a model has been used recently"""
        if model not in self._model_last_used:
            return False
        warmup_window = 300  # 5 minutes
        return (time.time() - self._model_last_used[model]) < warmup_window

    async def execute_with_fallback(
        self,
        task: str,
        executor: Callable,
        *args,
        use_fallback: bool = True,
        **kwargs
    ) -> ExecutionResult:
        """
        Execute a task with fallback chain on failure.

        Args:
            task: Task type identifier
            executor: Async function(model, *args, **kwargs) that performs the task
            use_fallback: Whether to try fallback models on failure
        """
        start_time = time.perf_counter()
        primary_model = self.get_best_model_for_task(task)
        fallback_chain = self.get_fallback_chain(task) if use_fallback else [primary_model]

        fallback_used = False
        last_error = None

        for i, model in enumerate(fallback_chain):
            try:
                self._model_last_used[model] = time.time()

                output = await executor(model, *args, **kwargs)

                exec_time = (time.perf_counter() - start_time) * 1000
                self.record_success(task, model, exec_time)

                return ExecutionResult(
                    success=True,
                    model=model,
                    output=output,
                    task=task,
                    execution_time_ms=exec_time,
                    fallback_used=fallback_used,
                    fallback_model=model if fallback_used else None,
                )

            except Exception as e:
                last_error = str(e)
                self.record_failure(task, model, last_error)
                self.logger.warning(f"Model {model} failed for {task}: {e}")

                if i > 0:
                    fallback_used = True

        exec_time = (time.perf_counter() - start_time) * 1000
        return ExecutionResult(
            success=False,
            model=fallback_chain[-1] if fallback_chain else 'unknown',
            output=None,
            task=task,
            execution_time_ms=exec_time,
            fallback_used=fallback_used,
            error=last_error,
        )

    async def warmup_models(self, models: Optional[List[str]] = None) -> Dict[str, bool]:
        """Warm up models with a simple prompt"""
        import httpx

        if models is None:
            models = ['gemma2:2b', 'llama3.2:3b', 'gemma3:4b']  # Don't warm 8b by default

        results = {}
        ollama_url = os.environ.get('OLLAMA_URL', 'http://localhost:11434')

        async with httpx.AsyncClient(timeout=60.0) as client:
            for model in models:
                try:
                    self.logger.info(f"Warming up model: {model}")

                    response = await client.post(
                        f'{ollama_url}/api/generate',
                        json={
                            'model': model,
                            'prompt': 'Hi',
                            'stream': False,
                            'options': {'num_predict': 5}
                        }
                    )

                    if response.status_code == 200:
                        results[model] = True
                        self._loaded_models.add(model)
                        self._model_last_used[model] = time.time()
                        self.logger.info(f"Model {model} warmed up successfully")
                    else:
                        results[model] = False
                        self.logger.warning(f"Model {model} warmup failed: {response.status_code}")

                except Exception as e:
                    results[model] = False
                    self.logger.warning(f"Model {model} warmup error: {e}")

        self._warmup_complete = True
        return results

    def get_stats(self) -> Dict:
        """Get router statistics"""
        model_stats = {}
        for task, task_perf in self._performance.items():
            for model, perf in task_perf.items():
                if model not in model_stats:
                    model_stats[model] = {
                        'total_requests': 0,
                        'successful': 0,
                        'failed': 0,
                        'total_latency_ms': 0,
                    }
                model_stats[model]['total_requests'] += perf.total_requests
                model_stats[model]['successful'] += perf.successful_requests
                model_stats[model]['failed'] += perf.failed_requests
                model_stats[model]['total_latency_ms'] += perf.total_latency_ms

        return {
            'warmup_complete': self._warmup_complete,
            'loaded_models': list(self._loaded_models),
            'warm_models': [m for m in self._model_last_used if self.is_model_warm(m)],
            'model_stats': model_stats,
            'task_mappings': len(self.TASK_TO_MODEL_MAP),
        }

    def get_all_tasks_by_model(self) -> Dict[str, List[str]]:
        """Get all tasks grouped by model"""
        tasks_by_model: Dict[str, List[str]] = {}

        for task, model in self.TASK_TO_MODEL_MAP.items():
            if model not in tasks_by_model:
                tasks_by_model[model] = []
            tasks_by_model[model].append(task)

        return tasks_by_model


# Global instance
_router: Optional[AITaskRouter] = None


def get_task_router() -> AITaskRouter:
    """Get or create the global AI task router instance"""
    global _router
    if _router is None:
        _router = AITaskRouter()
    return _router


def get_model_for_task(task: str) -> str:
    """Convenience function to get model for a task"""
    return get_task_router().get_model_for_task(task)


def get_task_info(task: str) -> Dict:
    """Convenience function to get task info"""
    return get_task_router().get_task_info(task)

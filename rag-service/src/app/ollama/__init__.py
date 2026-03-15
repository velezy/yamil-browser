"""
Ollama Token Optimization Module

Provides caching, model warm-up, task-based routing, and token optimization
for local LLM inference via Ollama.

Performance Target: 85-90% latency reduction (35-56s → 5-8s)
"""

from .config import OLLAMA_CONFIG, OLLAMA_URL, MODEL_PROFILES
from .client import OllamaOptimizedClient
from .model_warmer import ModelWarmer
from .model_router import ModelRouter
from .token_counter import TokenCounter
from .context_optimizer import ContextOptimizer
from .task_router import (
    AITaskRouter,
    AIRole,
    AIAgent,
    ExecutionResult,
    get_task_router,
    get_model_for_task,
    get_task_info,
)
from .service import (
    OllamaOptimizationService,
    get_ollama_service,
    ollama_service_lifespan,
)

__all__ = [
    # Configuration
    'OLLAMA_CONFIG',
    'OLLAMA_URL',
    'MODEL_PROFILES',
    # Core Components
    'OllamaOptimizedClient',
    'ModelWarmer',
    'ModelRouter',
    'TokenCounter',
    'ContextOptimizer',
    # Task Router (FlashCards-style)
    'AITaskRouter',
    'AIRole',
    'AIAgent',
    'ExecutionResult',
    'get_task_router',
    'get_model_for_task',
    'get_task_info',
    # Service
    'OllamaOptimizationService',
    'get_ollama_service',
    'ollama_service_lifespan',
]

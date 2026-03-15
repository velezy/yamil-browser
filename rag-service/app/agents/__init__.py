"""
Multi-Agent RAG Pipeline

A modular agentic system for complex question answering:
- Query Planner: Decomposes queries into sub-queries
- Retrieval Agent: Semantic search with scoring
- Synthesis Agent: Generates responses from context
- Reflection Agent: Evaluates and iterates on quality
"""

from .base_agent import BaseAgent, AgentResult, AgentContext
from .query_planner import QueryPlannerAgent
from .retrieval_agent import RetrievalAgent
from .synthesis_agent import SynthesisAgent
from .reflection_agent import ReflectionAgent
from .orchestrator import AgentOrchestrator, create_agent_pipeline

__all__ = [
    'BaseAgent',
    'AgentResult',
    'AgentContext',
    'QueryPlannerAgent',
    'RetrievalAgent',
    'SynthesisAgent',
    'ReflectionAgent',
    'AgentOrchestrator',
    'create_agent_pipeline',
]

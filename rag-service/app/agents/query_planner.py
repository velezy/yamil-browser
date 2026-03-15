"""
Query Planner Agent

Decomposes complex queries into sub-queries and identifies required tools.
Uses a fast model for quick planning.
"""

import logging
import time
from typing import Optional, TYPE_CHECKING

from .base_agent import BaseAgent, AgentResult, AgentContext, AgentRole

if TYPE_CHECKING:
    from ..ollama.client import OllamaOptimizedClient
    from ..ollama.model_router import ModelRouter

logger = logging.getLogger(__name__)


# Keywords indicating different tool needs
TOOL_INDICATORS = {
    "calculator": [
        "calculate", "compute", "percentage", "sum", "average", "total",
        "difference", "multiply", "divide", "add", "subtract", "ratio",
        "growth", "increase", "decrease", "margin", "profit"
    ],
    "chart": [
        "chart", "graph", "visualize", "plot", "trend", "diagram",
        "bar chart", "line chart", "pie chart", "histogram", "scatter",
        "distribution", "breakdown"
    ],
    "sql": [
        "show me all", "list all", "find all", "query", "filter by",
        "group by", "sort by", "order by", "where", "count of",
        "how many", "statistics", "my documents", "my data", "my files"
    ],
    "insights": [
        "insights", "analyze", "analysis", "recommend", "decision",
        "patterns", "trends", "summary", "overview", "statistics"
    ],
    "web_search": [
        "current", "latest", "recent", "today", "news", "update"
    ],
}

# Keywords indicating query complexity
COMPLEXITY_INDICATORS = {
    "simple": [
        "what is", "who is", "define", "explain", "describe"
    ],
    "medium": [
        "how", "why", "compare", "difference between", "summarize"
    ],
    "complex": [
        "analyze", "evaluate", "recommend", "predict", "multi-step",
        "and also", "as well as", "in addition", "furthermore"
    ],
}


class QueryPlannerAgent(BaseAgent):
    """
    Plans query execution by:
    1. Classifying query complexity
    2. Decomposing into sub-queries if needed
    3. Identifying required tools
    4. Setting execution order
    """

    def __init__(
        self,
        client: 'OllamaOptimizedClient',
        router: 'ModelRouter',
        default_model: Optional[str] = None,
    ):
        super().__init__(
            client=client,
            router=router,
            role=AgentRole.QUERY_PLANNER,
            default_model=default_model,
        )

    def _get_default_task_type(self) -> str:
        return "query_planning"  # llama3.2:3b - Fast but capable

    async def execute(self, context: AgentContext) -> AgentResult:
        """
        Plan the query execution.

        Updates context with:
        - sub_queries: List of decomposed queries
        - required_tools: List of tools needed
        - query_complexity: simple/medium/complex
        """
        self._log_start(context)
        start_time = time.time()

        try:
            # Step 1: Classify complexity
            complexity = self._classify_complexity(context.query)
            context.query_complexity = complexity
            context.add_trace(self.name, "classify_complexity", complexity)

            # Step 2: Identify tools from keywords
            tools = self._identify_tools_heuristic(context.query)
            context.add_trace(self.name, "identify_tools", tools)

            # Step 3: Decompose query if complex
            if complexity == "simple":
                # Simple queries don't need decomposition
                sub_queries = [context.query]
            else:
                # Use LLM to decompose complex queries
                sub_queries = await self._decompose_query(context.query)
                context.add_trace(self.name, "decompose_query", sub_queries)

            # Step 4: Refine tool identification with LLM for complex queries
            if complexity == "complex" or len(sub_queries) > 1:
                llm_tools = await self._identify_tools_llm(context.query, sub_queries)
                tools = list(set(tools + llm_tools))

            context.sub_queries = sub_queries
            context.required_tools = tools

            duration_ms = (time.time() - start_time) * 1000

            result = AgentResult(
                success=True,
                data={
                    "sub_queries": sub_queries,
                    "required_tools": tools,
                    "complexity": complexity,
                },
                duration_ms=duration_ms,
                model_used=self.get_model(),
                metadata={"query_length": len(context.query)},
            )

            self._log_complete(result, context)
            return result

        except Exception as e:
            logger.error(f"{self.name} error: {e}")
            duration_ms = (time.time() - start_time) * 1000

            # Fallback: treat as simple query
            context.sub_queries = [context.query]
            context.required_tools = []
            context.query_complexity = "simple"

            return AgentResult(
                success=False,
                data={"sub_queries": [context.query]},
                error=str(e),
                duration_ms=duration_ms,
            )

    def _classify_complexity(self, query: str) -> str:
        """
        Classify query complexity using heuristics.

        Returns: 'simple', 'medium', or 'complex'
        """
        query_lower = query.lower()

        # Check for complex indicators first
        for indicator in COMPLEXITY_INDICATORS["complex"]:
            if indicator in query_lower:
                return "complex"

        # Check for multiple questions
        question_count = query.count("?")
        if question_count > 1:
            return "complex"

        # Check word count
        word_count = len(query.split())
        if word_count > 30:
            return "complex"
        elif word_count > 15:
            return "medium"

        # Check for medium indicators
        for indicator in COMPLEXITY_INDICATORS["medium"]:
            if indicator in query_lower:
                return "medium"

        # Check for simple indicators
        for indicator in COMPLEXITY_INDICATORS["simple"]:
            if indicator in query_lower:
                return "simple"

        # Default to medium
        return "medium"

    def _identify_tools_heuristic(self, query: str) -> list[str]:
        """Identify required tools using keyword matching"""
        query_lower = query.lower()
        tools = []

        for tool, keywords in TOOL_INDICATORS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    tools.append(tool)
                    break

        return tools

    async def _decompose_query(self, query: str) -> list[str]:
        """
        Use LLM to decompose a complex query into sub-queries.

        Returns list of focused sub-queries.
        """
        system = """You are a query decomposition expert. Your job is to break down complex questions into simpler, focused sub-questions that can be answered independently.

Rules:
1. Create 2-4 sub-queries maximum
2. Each sub-query should be self-contained
3. Order sub-queries logically (dependencies first)
4. Keep sub-queries concise and specific"""

        prompt = f"""Decompose this query into focused sub-queries:

Query: {query}

Respond with a JSON object:
{{
    "sub_queries": ["sub-query 1", "sub-query 2", ...],
    "reasoning": "brief explanation of decomposition"
}}"""

        try:
            parsed, _ = await self._call_llm_json(
                prompt=prompt,
                system=system,
                options={"num_predict": 256, "temperature": 0.1},
            )

            sub_queries = parsed.get("sub_queries", [])

            # Validate and clean
            if not sub_queries or not isinstance(sub_queries, list):
                return [query]

            # Filter empty strings and limit to 4
            sub_queries = [sq.strip() for sq in sub_queries if sq.strip()][:4]

            return sub_queries if sub_queries else [query]

        except Exception as e:
            logger.warning(f"Query decomposition failed: {e}")
            return [query]

    async def _identify_tools_llm(
        self,
        original_query: str,
        sub_queries: list[str]
    ) -> list[str]:
        """
        Use LLM to identify required tools for complex queries.
        """
        available_tools = list(TOOL_INDICATORS.keys())

        system = """You are a tool selection expert. Identify which tools are needed to answer a query.

Available tools:
- calculator: For mathematical calculations, percentages, comparisons
- chart: For data visualization, trends, graphs, pie charts
- sql: For querying user's data, documents, conversations, statistics
- insights: For data analysis, patterns, recommendations, decision support
- web_search: For current events, recent information"""

        all_queries = [original_query] + sub_queries
        queries_text = "\n".join(f"- {q}" for q in all_queries)

        prompt = f"""What tools are needed to answer these queries?

Queries:
{queries_text}

Respond with JSON:
{{
    "tools": ["tool1", "tool2"],
    "reasoning": "why each tool is needed"
}}

Only include tools from: {available_tools}"""

        try:
            parsed, _ = await self._call_llm_json(
                prompt=prompt,
                system=system,
                options={"num_predict": 128, "temperature": 0.1},
            )

            tools = parsed.get("tools", [])

            # Validate tools
            valid_tools = [t for t in tools if t in available_tools]
            return valid_tools

        except Exception as e:
            logger.warning(f"Tool identification failed: {e}")
            return []

    async def replan_with_feedback(
        self,
        context: AgentContext,
        feedback: str
    ) -> AgentResult:
        """
        Re-plan based on reflection feedback.

        Called when the reflection agent determines the response is inadequate.
        """
        self._log_start(context)
        start_time = time.time()

        system = """You are a query replanning expert. Based on feedback about a previous attempt to answer a query, create an improved execution plan."""

        prompt = f"""Original query: {context.query}

Previous sub-queries: {context.sub_queries}

Feedback from evaluation:
{feedback}

Create an improved plan with better sub-queries that address the feedback.

Respond with JSON:
{{
    "sub_queries": ["improved sub-query 1", "..."],
    "additional_tools": ["any new tools needed"],
    "strategy_change": "how the approach should change"
}}"""

        try:
            parsed, _ = await self._call_llm_json(
                prompt=prompt,
                system=system,
                options={"num_predict": 256, "temperature": 0.2},
            )

            new_sub_queries = parsed.get("sub_queries", context.sub_queries)
            additional_tools = parsed.get("additional_tools", [])

            context.sub_queries = new_sub_queries
            context.required_tools = list(set(context.required_tools + additional_tools))

            duration_ms = (time.time() - start_time) * 1000

            result = AgentResult(
                success=True,
                data={
                    "sub_queries": new_sub_queries,
                    "additional_tools": additional_tools,
                    "strategy_change": parsed.get("strategy_change", ""),
                },
                duration_ms=duration_ms,
                model_used=self.get_model(),
            )

            self._log_complete(result, context)
            return result

        except Exception as e:
            logger.error(f"Replanning failed: {e}")
            return AgentResult(
                success=False,
                data={},
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )

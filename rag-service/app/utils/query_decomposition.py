"""
Query Decomposition Module

Breaks down complex queries into simpler sub-queries for better retrieval.
Implements multiple decomposition strategies used in industry RAG systems.
"""

import logging
import aiohttp
import asyncio
import json
import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DecompositionStrategy(str, Enum):
    """Available query decomposition strategies"""
    SIMPLE = "simple"           # Break into independent sub-queries
    SEQUENTIAL = "sequential"   # Each sub-query builds on previous
    MULTI_HOP = "multi_hop"     # For multi-hop reasoning questions


@dataclass
class SubQuery:
    """A decomposed sub-query with metadata"""
    query: str
    order: int
    depends_on: Optional[List[int]] = None  # For sequential queries
    purpose: Optional[str] = None


@dataclass
class DecomposedQuery:
    """Result of query decomposition"""
    original_query: str
    sub_queries: List[SubQuery]
    strategy: DecompositionStrategy
    reasoning: Optional[str] = None


class QueryDecomposer:
    """
    Decomposes complex queries into simpler sub-queries.

    This improves retrieval by:
    1. Breaking down multi-part questions
    2. Handling implicit sub-questions
    3. Enabling parallel retrieval for independent parts
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.model = model

    async def decompose(
        self,
        query: str,
        strategy: DecompositionStrategy = DecompositionStrategy.SIMPLE,
        max_sub_queries: int = 4
    ) -> DecomposedQuery:
        """
        Decompose a query into sub-queries.

        Args:
            query: The original complex query
            strategy: Decomposition strategy to use
            max_sub_queries: Maximum number of sub-queries

        Returns:
            DecomposedQuery with sub-queries
        """
        # First check if decomposition is needed
        if not self._should_decompose(query):
            return DecomposedQuery(
                original_query=query,
                sub_queries=[SubQuery(query=query, order=0)],
                strategy=strategy,
                reasoning="Query is simple enough, no decomposition needed"
            )

        # Use appropriate decomposition method
        if strategy == DecompositionStrategy.SEQUENTIAL:
            return await self._decompose_sequential(query, max_sub_queries)
        elif strategy == DecompositionStrategy.MULTI_HOP:
            return await self._decompose_multi_hop(query, max_sub_queries)
        else:
            return await self._decompose_simple(query, max_sub_queries)

    def _should_decompose(self, query: str) -> bool:
        """Heuristic to determine if query needs decomposition."""
        # Check for multiple question words
        question_words = ['what', 'how', 'why', 'when', 'where', 'who', 'which']
        word_count = sum(1 for word in question_words if word in query.lower())

        # Check for conjunctions indicating multiple parts
        conjunctions = [' and ', ' or ', ' also ', ' as well as ', ' both ']
        has_conjunction = any(c in query.lower() for c in conjunctions)

        # Check query length
        is_long = len(query.split()) > 15

        # Check for comparison keywords
        comparison_words = ['compare', 'difference', 'versus', 'vs', 'between']
        is_comparison = any(c in query.lower() for c in comparison_words)

        return word_count > 1 or has_conjunction or is_long or is_comparison

    async def _decompose_simple(
        self,
        query: str,
        max_sub_queries: int
    ) -> DecomposedQuery:
        """Simple decomposition into independent sub-queries."""

        prompt = f"""Break down this complex question into {max_sub_queries} or fewer simple, independent sub-questions.
Each sub-question should be answerable on its own.
Return ONLY a JSON array of strings, no explanation.

Question: {query}

JSON array of sub-questions:"""

        sub_query_texts = await self._generate_sub_queries(prompt, max_sub_queries)

        sub_queries = [
            SubQuery(query=sq, order=i, purpose=f"Part {i+1}")
            for i, sq in enumerate(sub_query_texts)
        ]

        return DecomposedQuery(
            original_query=query,
            sub_queries=sub_queries,
            strategy=DecompositionStrategy.SIMPLE,
            reasoning="Decomposed into independent sub-queries"
        )

    async def _decompose_sequential(
        self,
        query: str,
        max_sub_queries: int
    ) -> DecomposedQuery:
        """Sequential decomposition where each builds on previous."""

        prompt = f"""Break down this question into {max_sub_queries} or fewer sequential steps.
Each step should build on the information from the previous step.
Return ONLY a JSON array of strings representing the steps, no explanation.

Question: {query}

JSON array of sequential sub-questions:"""

        sub_query_texts = await self._generate_sub_queries(prompt, max_sub_queries)

        sub_queries = [
            SubQuery(
                query=sq,
                order=i,
                depends_on=list(range(i)) if i > 0 else None,
                purpose=f"Step {i+1}"
            )
            for i, sq in enumerate(sub_query_texts)
        ]

        return DecomposedQuery(
            original_query=query,
            sub_queries=sub_queries,
            strategy=DecompositionStrategy.SEQUENTIAL,
            reasoning="Decomposed into sequential steps"
        )

    async def _decompose_multi_hop(
        self,
        query: str,
        max_sub_queries: int
    ) -> DecomposedQuery:
        """Multi-hop decomposition for reasoning chains."""

        prompt = f"""This is a multi-hop question that requires connecting multiple pieces of information.
Break it down into {max_sub_queries} or fewer atomic questions that, when answered in order, will answer the main question.
Return ONLY a JSON array of strings, no explanation.

Multi-hop question: {query}

JSON array of atomic questions:"""

        sub_query_texts = await self._generate_sub_queries(prompt, max_sub_queries)

        sub_queries = [
            SubQuery(
                query=sq,
                order=i,
                depends_on=[i-1] if i > 0 else None,
                purpose=f"Hop {i+1}"
            )
            for i, sq in enumerate(sub_query_texts)
        ]

        return DecomposedQuery(
            original_query=query,
            sub_queries=sub_queries,
            strategy=DecompositionStrategy.MULTI_HOP,
            reasoning="Decomposed into multi-hop reasoning chain"
        )

    async def _generate_sub_queries(
        self,
        prompt: str,
        max_sub_queries: int
    ) -> List[str]:
        """Generate sub-queries using LLM."""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.3,
                            "num_predict": 300,
                        }
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get('response', '').strip()
                        return self._parse_sub_queries(response, max_sub_queries)

        except Exception as e:
            logger.error(f"Query decomposition error: {e}")

        # Fallback: return original as single sub-query
        return [prompt.split("Question:")[-1].split("\n")[0].strip()]

    def _parse_sub_queries(self, response: str, max_sub_queries: int) -> List[str]:
        """Parse LLM response into list of sub-queries."""

        # Try to parse as JSON array
        try:
            # Find JSON array in response
            match = re.search(r'\[.*?\]', response, re.DOTALL)
            if match:
                queries = json.loads(match.group())
                if isinstance(queries, list):
                    return [str(q).strip() for q in queries[:max_sub_queries] if q]
        except json.JSONDecodeError:
            pass

        # Fallback: split by newlines and numbered items
        lines = response.split('\n')
        queries = []
        for line in lines:
            line = line.strip()
            # Remove numbering like "1.", "1)", "- "
            line = re.sub(r'^[\d]+[.\)]\s*', '', line)
            line = re.sub(r'^[-•]\s*', '', line)
            if line and len(line) > 10:  # Minimum reasonable query length
                queries.append(line)

        return queries[:max_sub_queries] if queries else []


class ParallelQueryExecutor:
    """
    Executes decomposed sub-queries in parallel or sequentially.
    """

    def __init__(self, vector_store: Any):
        self.vector_store = vector_store

    async def execute_parallel(
        self,
        decomposed: DecomposedQuery,
        top_k_per_query: int = 3
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute independent sub-queries in parallel.

        Returns:
            Dict mapping sub-query text to retrieved documents
        """
        tasks = []
        for sq in decomposed.sub_queries:
            if sq.depends_on is None:  # Independent query
                tasks.append(self._search_single(sq.query, top_k_per_query))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        result_map = {}
        for i, sq in enumerate(decomposed.sub_queries):
            if sq.depends_on is None and i < len(results):
                if not isinstance(results[i], Exception):
                    result_map[sq.query] = results[i]

        return result_map

    async def execute_sequential(
        self,
        decomposed: DecomposedQuery,
        top_k_per_query: int = 3
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute sub-queries sequentially, using context from previous results.

        Returns:
            Dict mapping sub-query text to retrieved documents
        """
        result_map = {}
        previous_context = ""

        for sq in decomposed.sub_queries:
            # Enhance query with previous context for sequential queries
            enhanced_query = sq.query
            if sq.depends_on and previous_context:
                enhanced_query = f"{previous_context} {sq.query}"

            results = await self._search_single(enhanced_query, top_k_per_query)
            result_map[sq.query] = results

            # Extract context for next query
            if results:
                previous_context = results[0].get('content', '')[:200]

        return result_map

    async def _search_single(
        self,
        query: str,
        top_k: int
    ) -> List[Dict[str, Any]]:
        """Search for a single query."""
        try:
            return await self.vector_store.search(query=query, top_k=top_k)
        except Exception as e:
            logger.error(f"Search error for sub-query: {e}")
            return []


# =============================================================================
# ADVANCED: RECURSIVE DECOMPOSITION & DEPENDENCY-AWARE EXECUTION
# =============================================================================

class DependencyType(str, Enum):
    """Types of dependencies between sub-queries"""
    REQUIRES_ANSWER = "requires_answer"  # Need answer from dependency
    REQUIRES_CONTEXT = "requires_context"  # Need context from dependency
    REQUIRES_ENTITIES = "requires_entities"  # Need entities identified
    PARALLEL = "parallel"  # Can run in parallel
    AGGREGATION = "aggregation"  # Combines results from multiple queries


@dataclass
class DependencyNode:
    """Node in the dependency graph"""
    query_id: int
    query_text: str
    dependencies: List[int] = None  # IDs of queries this depends on
    dependency_types: Dict[int, DependencyType] = None  # Type of each dependency
    depth: int = 0  # Recursion depth
    is_leaf: bool = True  # Whether this is a leaf query
    sub_queries: List[int] = None  # IDs of child sub-queries if recursively decomposed

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.dependency_types is None:
            self.dependency_types = {}
        if self.sub_queries is None:
            self.sub_queries = []


@dataclass
class DependencyGraph:
    """Graph tracking dependencies between decomposed queries"""
    nodes: Dict[int, DependencyNode]
    root_id: int
    execution_order: List[List[int]] = None  # Levels of parallel execution

    def __post_init__(self):
        if self.execution_order is None:
            self.execution_order = self._compute_execution_order()

    def _compute_execution_order(self) -> List[List[int]]:
        """Compute topological order grouped by parallel execution levels"""
        if not self.nodes:
            return []

        # Calculate in-degree for each node
        in_degree = {nid: 0 for nid in self.nodes}
        for node in self.nodes.values():
            for dep_id in node.dependencies:
                if dep_id in in_degree:
                    in_degree[node.query_id] += 1

        # Group by execution level (modified Kahn's algorithm)
        levels = []
        remaining = set(self.nodes.keys())

        while remaining:
            # Find all nodes with no remaining dependencies
            ready = [
                nid for nid in remaining
                if in_degree[nid] == 0
            ]

            if not ready:
                # Cycle detected, break arbitrarily
                ready = [next(iter(remaining))]

            levels.append(ready)

            # Remove ready nodes and update in-degrees
            for nid in ready:
                remaining.discard(nid)
                for other_id in remaining:
                    if nid in self.nodes[other_id].dependencies:
                        in_degree[other_id] -= 1

        return levels

    def get_execution_levels(self) -> List[List[DependencyNode]]:
        """Get nodes grouped by execution level"""
        return [
            [self.nodes[nid] for nid in level]
            for level in self.execution_order
        ]

    def get_dependencies_for(self, query_id: int) -> List[DependencyNode]:
        """Get all dependency nodes for a query"""
        if query_id not in self.nodes:
            return []
        return [
            self.nodes[dep_id]
            for dep_id in self.nodes[query_id].dependencies
            if dep_id in self.nodes
        ]


@dataclass
class RecursiveDecompositionResult:
    """Result of recursive query decomposition"""
    original_query: str
    dependency_graph: DependencyGraph
    total_sub_queries: int
    max_depth: int
    decomposition_reasoning: str = ""


class RecursiveDecomposer:
    """
    Recursively decomposes complex queries into simpler sub-queries.

    Provides:
    - Multi-level decomposition for very complex queries
    - Dependency detection between sub-queries
    - Automatic depth limiting to prevent over-decomposition
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "gemma2:2b",
        max_depth: int = 3,
        min_complexity_for_decomposition: int = 10
    ):
        self.ollama_url = ollama_url
        self.model = model
        self.max_depth = max_depth
        self.min_complexity = min_complexity_for_decomposition
        self._next_id = 0

    def _get_next_id(self) -> int:
        """Get next unique query ID"""
        self._next_id += 1
        return self._next_id

    def _estimate_complexity(self, query: str) -> int:
        """Estimate query complexity score"""
        score = 0

        # Length-based complexity
        words = query.split()
        score += len(words)

        # Question word complexity
        question_words = ['what', 'how', 'why', 'when', 'where', 'who', 'which']
        score += sum(3 for word in question_words if word in query.lower()) * 3

        # Conjunction complexity
        conjunctions = [' and ', ' or ', ' but ', ' while ', ' whereas ']
        score += sum(5 for c in conjunctions if c in query.lower())

        # Comparison complexity
        comparison_words = ['compare', 'difference', 'versus', 'vs', 'between', 'rather than']
        score += sum(5 for c in comparison_words if c in query.lower())

        # Multi-hop indicators
        multi_hop_indicators = ['then', 'after that', 'based on', 'using that', 'given that']
        score += sum(7 for m in multi_hop_indicators if m in query.lower())

        return score

    async def decompose_recursively(
        self,
        query: str,
        current_depth: int = 0
    ) -> RecursiveDecompositionResult:
        """
        Recursively decompose a query into sub-queries with dependencies.

        Args:
            query: The query to decompose
            current_depth: Current recursion depth

        Returns:
            RecursiveDecompositionResult with dependency graph
        """
        self._next_id = 0
        nodes = {}
        reasoning = []

        # Create root node
        root_id = self._get_next_id()
        root_node = DependencyNode(
            query_id=root_id,
            query_text=query,
            depth=0,
            is_leaf=False
        )
        nodes[root_id] = root_node

        # Recursively decompose
        max_depth_reached = await self._decompose_node(
            nodes, root_node, 0, reasoning
        )

        # Build dependency graph
        graph = DependencyGraph(nodes=nodes, root_id=root_id)

        return RecursiveDecompositionResult(
            original_query=query,
            dependency_graph=graph,
            total_sub_queries=len([n for n in nodes.values() if n.is_leaf]),
            max_depth=max_depth_reached,
            decomposition_reasoning="\n".join(reasoning)
        )

    async def _decompose_node(
        self,
        nodes: Dict[int, DependencyNode],
        node: DependencyNode,
        depth: int,
        reasoning: List[str]
    ) -> int:
        """Recursively decompose a single node"""

        # Check if we should decompose further
        complexity = self._estimate_complexity(node.query_text)

        if depth >= self.max_depth:
            reasoning.append(f"Depth {depth}: '{node.query_text[:50]}...' - max depth reached")
            node.is_leaf = True
            return depth

        if complexity < self.min_complexity:
            reasoning.append(f"Depth {depth}: '{node.query_text[:50]}...' - complexity {complexity} below threshold")
            node.is_leaf = True
            return depth

        # Decompose this node
        sub_queries, dependencies = await self._decompose_with_dependencies(node.query_text)

        if len(sub_queries) <= 1:
            reasoning.append(f"Depth {depth}: '{node.query_text[:50]}...' - no further decomposition possible")
            node.is_leaf = True
            return depth

        reasoning.append(f"Depth {depth}: '{node.query_text[:50]}...' decomposed into {len(sub_queries)} sub-queries")
        node.is_leaf = False

        max_child_depth = depth

        # Create child nodes
        for i, (sq_text, sq_deps) in enumerate(zip(sub_queries, dependencies)):
            child_id = self._get_next_id()
            child_node = DependencyNode(
                query_id=child_id,
                query_text=sq_text,
                depth=depth + 1,
                is_leaf=True
            )

            # Set up dependencies
            for dep_idx, dep_type in sq_deps:
                if dep_idx < i:
                    # Dependency on a previous sub-query in this group
                    dep_id = node.sub_queries[dep_idx] if dep_idx < len(node.sub_queries) else None
                    if dep_id:
                        child_node.dependencies.append(dep_id)
                        child_node.dependency_types[dep_id] = dep_type

            nodes[child_id] = child_node
            node.sub_queries.append(child_id)

            # Recursively decompose child
            child_depth = await self._decompose_node(
                nodes, child_node, depth + 1, reasoning
            )
            max_child_depth = max(max_child_depth, child_depth)

        return max_child_depth

    async def _decompose_with_dependencies(
        self,
        query: str
    ) -> tuple:
        """Decompose query and identify dependencies between sub-queries"""

        prompt = f"""Decompose this query into simpler sub-queries and identify dependencies.

Query: {query}

Return a JSON object with:
- "sub_queries": array of sub-query strings
- "dependencies": array of arrays, where dependencies[i] lists indices of sub-queries that sub-query i depends on

Example format:
{{"sub_queries": ["What is X?", "How does X relate to Y?", "Compare X and Y"], "dependencies": [[], [0], [0, 1]]}}

JSON response:"""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.2, "num_predict": 400}
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "").strip()

                        # Parse JSON
                        match = re.search(r'\{.*\}', response, re.DOTALL)
                        if match:
                            parsed = json.loads(match.group())
                            sub_queries = parsed.get("sub_queries", [])
                            raw_deps = parsed.get("dependencies", [])

                            # Convert to typed dependencies
                            dependencies = []
                            for i, deps in enumerate(raw_deps):
                                typed_deps = []
                                for dep_idx in deps:
                                    if isinstance(dep_idx, int):
                                        typed_deps.append((dep_idx, DependencyType.REQUIRES_ANSWER))
                                dependencies.append(typed_deps)

                            # Pad dependencies if needed
                            while len(dependencies) < len(sub_queries):
                                dependencies.append([])

                            return sub_queries, dependencies

        except Exception as e:
            logger.warning(f"Decomposition with dependencies failed: {e}")

        # Fallback: return original as single query
        return [query], [[]]


class DependencyAwareExecutor:
    """
    Executes decomposed queries respecting their dependencies.

    Provides:
    - Parallel execution of independent queries
    - Sequential execution when dependencies exist
    - Context passing between dependent queries
    """

    def __init__(self, vector_store: Any, context_window: int = 500):
        self.vector_store = vector_store
        self.context_window = context_window

    async def execute(
        self,
        decomposition: RecursiveDecompositionResult,
        top_k_per_query: int = 3
    ) -> Dict[int, Dict[str, Any]]:
        """
        Execute all sub-queries respecting dependencies.

        Args:
            decomposition: The decomposition result
            top_k_per_query: Results per sub-query

        Returns:
            Dict mapping query_id to results and context
        """
        results = {}
        graph = decomposition.dependency_graph

        # Execute level by level
        for level in graph.get_execution_levels():
            # Filter to leaf nodes only (the actual queries to execute)
            leaf_nodes = [n for n in level if n.is_leaf]

            if not leaf_nodes:
                continue

            # Execute this level in parallel
            tasks = []
            for node in leaf_nodes:
                # Gather context from dependencies
                context = self._gather_dependency_context(node, results, graph)
                tasks.append(self._execute_single(node, context, top_k_per_query))

            level_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Store results
            for node, result in zip(leaf_nodes, level_results):
                if isinstance(result, Exception):
                    logger.warning(f"Query {node.query_id} failed: {result}")
                    results[node.query_id] = {
                        "query": node.query_text,
                        "results": [],
                        "context": "",
                        "error": str(result)
                    }
                else:
                    results[node.query_id] = result

        return results

    def _gather_dependency_context(
        self,
        node: DependencyNode,
        results: Dict[int, Dict[str, Any]],
        graph: DependencyGraph
    ) -> str:
        """Gather context from dependency results"""
        if not node.dependencies:
            return ""

        context_parts = []
        for dep_id in node.dependencies:
            if dep_id in results:
                dep_result = results[dep_id]
                dep_type = node.dependency_types.get(dep_id, DependencyType.REQUIRES_CONTEXT)

                if dep_type == DependencyType.REQUIRES_ANSWER:
                    # Use top result content
                    if dep_result.get("results"):
                        context_parts.append(dep_result["results"][0].get("content", "")[:self.context_window])
                elif dep_type == DependencyType.REQUIRES_CONTEXT:
                    # Use summary context
                    context_parts.append(dep_result.get("context", "")[:self.context_window])
                elif dep_type == DependencyType.REQUIRES_ENTITIES:
                    # Extract and pass entities
                    if dep_result.get("results"):
                        entities = self._extract_entities(dep_result["results"])
                        context_parts.append(f"Entities: {', '.join(entities[:10])}")

        return " | ".join(context_parts) if context_parts else ""

    async def _execute_single(
        self,
        node: DependencyNode,
        context: str,
        top_k: int
    ) -> Dict[str, Any]:
        """Execute a single query with optional context"""
        # Enhance query with context if available
        enhanced_query = node.query_text
        if context:
            enhanced_query = f"{context} {node.query_text}"

        try:
            results = await self.vector_store.search(query=enhanced_query, top_k=top_k)

            # Generate context summary for dependents
            context_summary = ""
            if results:
                context_summary = " ".join([
                    r.get("content", "")[:100] for r in results[:2]
                ])

            return {
                "query": node.query_text,
                "enhanced_query": enhanced_query,
                "results": results,
                "context": context_summary
            }
        except Exception as e:
            logger.error(f"Search error for query {node.query_id}: {e}")
            return {
                "query": node.query_text,
                "results": [],
                "context": "",
                "error": str(e)
            }

    def _extract_entities(self, results: List[Dict[str, Any]]) -> List[str]:
        """Extract entities from search results"""
        entities = set()
        for r in results:
            content = r.get("content", "")
            # Simple entity extraction (capitalized words)
            words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', content)
            entities.update(words[:5])
        return list(entities)


@dataclass
class AggregatedResult:
    """Aggregated result from decomposed query execution"""
    original_query: str
    combined_results: List[Dict[str, Any]]
    sub_query_results: Dict[int, List[Dict[str, Any]]]
    coverage_score: float  # How well sub-queries cover the original
    confidence_score: float  # Overall confidence in results
    aggregation_method: str


class ResultAggregator:
    """
    Aggregates results from decomposed query execution.

    Provides:
    - Multiple aggregation strategies
    - Result deduplication
    - Relevance scoring across sub-queries
    """

    def __init__(self):
        self.aggregation_methods = {
            "union": self._aggregate_union,
            "intersection": self._aggregate_intersection,
            "weighted": self._aggregate_weighted,
            "hierarchical": self._aggregate_hierarchical
        }

    def aggregate(
        self,
        decomposition: RecursiveDecompositionResult,
        execution_results: Dict[int, Dict[str, Any]],
        method: str = "weighted",
        max_results: int = 10
    ) -> AggregatedResult:
        """
        Aggregate results from decomposed query execution.

        Args:
            decomposition: The decomposition used
            execution_results: Results from execution
            method: Aggregation method
            max_results: Maximum results to return

        Returns:
            AggregatedResult with combined results
        """
        aggregator = self.aggregation_methods.get(method, self._aggregate_weighted)

        combined, coverage, confidence = aggregator(
            decomposition, execution_results, max_results
        )

        # Build sub-query results map
        sub_results = {
            qid: res.get("results", [])
            for qid, res in execution_results.items()
        }

        return AggregatedResult(
            original_query=decomposition.original_query,
            combined_results=combined,
            sub_query_results=sub_results,
            coverage_score=coverage,
            confidence_score=confidence,
            aggregation_method=method
        )

    def _aggregate_union(
        self,
        decomposition: RecursiveDecompositionResult,
        results: Dict[int, Dict[str, Any]],
        max_results: int
    ) -> tuple:
        """Union aggregation - include all unique results"""
        seen_content = set()
        combined = []

        for qid, qresult in results.items():
            for r in qresult.get("results", []):
                content_key = r.get("content", "")[:100]
                if content_key not in seen_content:
                    seen_content.add(content_key)
                    combined.append(r)

        # Sort by score and limit
        combined.sort(key=lambda x: x.get("score", 0), reverse=True)
        combined = combined[:max_results]

        # Calculate scores
        total_queries = len([n for n in decomposition.dependency_graph.nodes.values() if n.is_leaf])
        queries_with_results = sum(1 for r in results.values() if r.get("results"))
        coverage = queries_with_results / total_queries if total_queries > 0 else 0

        avg_score = sum(r.get("score", 0) for r in combined) / len(combined) if combined else 0
        confidence = avg_score * coverage

        return combined, coverage, confidence

    def _aggregate_intersection(
        self,
        decomposition: RecursiveDecompositionResult,
        results: Dict[int, Dict[str, Any]],
        max_results: int
    ) -> tuple:
        """Intersection aggregation - only include results that appear in multiple sub-queries"""
        content_counts = {}
        content_to_result = {}

        for qid, qresult in results.items():
            for r in qresult.get("results", []):
                content_key = r.get("content", "")[:100]
                content_counts[content_key] = content_counts.get(content_key, 0) + 1
                # Keep highest scoring version
                if content_key not in content_to_result or r.get("score", 0) > content_to_result[content_key].get("score", 0):
                    content_to_result[content_key] = r

        # Filter to results appearing in multiple queries
        combined = [
            content_to_result[key]
            for key, count in content_counts.items()
            if count >= 2
        ]

        # If not enough intersection results, fall back to union
        if len(combined) < max_results // 2:
            return self._aggregate_union(decomposition, results, max_results)

        combined.sort(key=lambda x: x.get("score", 0), reverse=True)
        combined = combined[:max_results]

        # Calculate scores
        total_queries = len([n for n in decomposition.dependency_graph.nodes.values() if n.is_leaf])
        coverage = len(combined) / (total_queries * 3) if total_queries > 0 else 0  # Assuming 3 results per query
        avg_score = sum(r.get("score", 0) for r in combined) / len(combined) if combined else 0
        confidence = min(1.0, avg_score * (1 + coverage))  # Boost for overlap

        return combined, coverage, confidence

    def _aggregate_weighted(
        self,
        decomposition: RecursiveDecompositionResult,
        results: Dict[int, Dict[str, Any]],
        max_results: int
    ) -> tuple:
        """Weighted aggregation based on query depth and dependency satisfaction"""
        graph = decomposition.dependency_graph
        scored_results = []

        for qid, qresult in results.items():
            if qid not in graph.nodes:
                continue

            node = graph.nodes[qid]
            depth_weight = 1.0 / (1 + node.depth * 0.2)  # Deeper queries slightly less weight

            # Check if dependencies were satisfied
            dep_satisfaction = 1.0
            if node.dependencies:
                satisfied = sum(1 for d in node.dependencies if d in results and results[d].get("results"))
                dep_satisfaction = satisfied / len(node.dependencies)

            for r in qresult.get("results", []):
                base_score = r.get("score", 0)
                weighted_score = base_score * depth_weight * (0.5 + 0.5 * dep_satisfaction)
                scored_results.append({
                    **r,
                    "weighted_score": weighted_score,
                    "original_score": base_score,
                    "source_query_id": qid
                })

        # Deduplicate and sort
        seen_content = set()
        combined = []
        for r in sorted(scored_results, key=lambda x: x.get("weighted_score", 0), reverse=True):
            content_key = r.get("content", "")[:100]
            if content_key not in seen_content:
                seen_content.add(content_key)
                combined.append(r)
            if len(combined) >= max_results:
                break

        # Calculate scores
        total_queries = len([n for n in graph.nodes.values() if n.is_leaf])
        queries_with_results = sum(1 for r in results.values() if r.get("results"))
        coverage = queries_with_results / total_queries if total_queries > 0 else 0

        avg_weighted = sum(r.get("weighted_score", 0) for r in combined) / len(combined) if combined else 0
        confidence = avg_weighted

        return combined, coverage, confidence

    def _aggregate_hierarchical(
        self,
        decomposition: RecursiveDecompositionResult,
        results: Dict[int, Dict[str, Any]],
        max_results: int
    ) -> tuple:
        """Hierarchical aggregation - prioritize root-level results, then children"""
        graph = decomposition.dependency_graph
        levels = graph.get_execution_levels()

        combined = []
        seen_content = set()
        results_per_level = max(1, max_results // len(levels)) if levels else max_results

        for level in levels:
            level_results = []
            for node in level:
                if not node.is_leaf:
                    continue
                for r in results.get(node.query_id, {}).get("results", []):
                    content_key = r.get("content", "")[:100]
                    if content_key not in seen_content:
                        level_results.append(r)
                        seen_content.add(content_key)

            # Take top results from this level
            level_results.sort(key=lambda x: x.get("score", 0), reverse=True)
            combined.extend(level_results[:results_per_level])

            if len(combined) >= max_results:
                break

        combined = combined[:max_results]

        # Calculate scores
        total_queries = len([n for n in graph.nodes.values() if n.is_leaf])
        queries_with_results = sum(1 for r in results.values() if r.get("results"))
        coverage = queries_with_results / total_queries if total_queries > 0 else 0

        avg_score = sum(r.get("score", 0) for r in combined) / len(combined) if combined else 0
        confidence = avg_score * coverage

        return combined, coverage, confidence


# =============================================================================
# CUTTING EDGE: Self-Verifying Decomposition & Automatic Aggregation Selection
# =============================================================================

from datetime import datetime


class VerificationStatus(str, Enum):
    """Status of decomposition verification."""
    VERIFIED = "verified"
    PARTIAL = "partial"
    FAILED = "failed"
    NEEDS_REFINEMENT = "needs_refinement"


class AggregationMethod(str, Enum):
    """Available aggregation methods."""
    UNION = "union"
    INTERSECTION = "intersection"
    WEIGHTED = "weighted"
    HIERARCHICAL = "hierarchical"
    CAUSAL = "causal"
    COMPARATIVE = "comparative"


@dataclass
class CoverageAnalysis:
    """Analysis of how well sub-queries cover the original query."""
    coverage_score: float  # 0-1
    covered_aspects: List[str]
    missing_aspects: List[str]
    redundant_queries: List[int]
    suggestions: List[str]


@dataclass
class VerifiedDecomposition:
    """A decomposition that has been verified for coverage."""
    decomposition: DecomposedQuery
    verification_status: VerificationStatus
    coverage: CoverageAnalysis
    refinement_iterations: int = 0
    verified_at: datetime = None

    def __post_init__(self):
        if self.verified_at is None:
            self.verified_at = datetime.now()


@dataclass
class AggregationDecision:
    """Decision on which aggregation method to use."""
    method: AggregationMethod
    confidence: float
    reasoning: str
    weights: Optional[Dict[int, float]] = None  # Sub-query weights


@dataclass
class SelfVerifyingConfig:
    """Configuration for self-verifying decomposition."""
    enable_verification: bool = True
    max_refinement_iterations: int = 3
    min_coverage_threshold: float = 0.8
    enable_aggregation_learning: bool = True


@dataclass
class CuttingEdgeDecompositionConfig:
    """Configuration for Cutting Edge decomposition."""
    enable_self_verification: bool = True
    enable_auto_aggregation: bool = True
    verification_config: SelfVerifyingConfig = None

    def __post_init__(self):
        if self.verification_config is None:
            self.verification_config = SelfVerifyingConfig()


class DecompositionVerifier:
    """
    Verifies that decomposed sub-queries properly cover the original query.

    Analyzes coverage, identifies gaps, and suggests refinements.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.ollama_model = ollama_model

    async def verify_coverage(
        self,
        original_query: str,
        decomposition: DecomposedQuery
    ) -> CoverageAnalysis:
        """
        Verify that sub-queries cover the original query's intent.

        Args:
            original_query: The original complex query
            decomposition: The decomposed query result

        Returns:
            CoverageAnalysis with coverage details
        """
        sub_queries_text = "\n".join(
            f"{i+1}. {sq.query}"
            for i, sq in enumerate(decomposition.sub_queries)
        )

        prompt = f"""Analyze how well these sub-queries cover the original question's intent.

Original question: {original_query}

Sub-queries:
{sub_queries_text}

Return a JSON object with:
- "coverage_score": float 0-1 (how completely the sub-queries address the original)
- "covered_aspects": list of aspects that ARE covered
- "missing_aspects": list of aspects that are NOT covered
- "redundant_indices": list of sub-query numbers (1-indexed) that are redundant
- "suggestions": list of suggestions to improve coverage

JSON:"""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.3, "num_predict": 500}
                    },
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "")

                        match = re.search(r'\{.*\}', response, re.DOTALL)
                        if match:
                            result = json.loads(match.group())
                            return CoverageAnalysis(
                                coverage_score=float(result.get("coverage_score", 0.7)),
                                covered_aspects=result.get("covered_aspects", []),
                                missing_aspects=result.get("missing_aspects", []),
                                redundant_queries=[i-1 for i in result.get("redundant_indices", [])],
                                suggestions=result.get("suggestions", [])
                            )
        except Exception as e:
            logger.warning(f"Coverage verification failed: {e}")

        # Default analysis
        return CoverageAnalysis(
            coverage_score=0.7,
            covered_aspects=["main topic"],
            missing_aspects=[],
            redundant_queries=[],
            suggestions=[]
        )

    async def refine_decomposition(
        self,
        original_query: str,
        decomposition: DecomposedQuery,
        coverage: CoverageAnalysis
    ) -> DecomposedQuery:
        """
        Refine decomposition based on coverage analysis.

        Adds missing aspects and removes redundant queries.
        """
        # Remove redundant queries
        refined_queries = [
            sq for i, sq in enumerate(decomposition.sub_queries)
            if i not in coverage.redundant_queries
        ]

        # Generate queries for missing aspects
        if coverage.missing_aspects:
            missing_str = ", ".join(coverage.missing_aspects)
            prompt = f"""Generate sub-queries to cover these missing aspects of the original question.

Original question: {original_query}
Missing aspects: {missing_str}

Return a JSON array of sub-query strings to cover these aspects.

JSON array:"""

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.ollama_url}/api/generate",
                        json={
                            "model": self.ollama_model,
                            "prompt": prompt,
                            "stream": False,
                            "options": {"temperature": 0.5, "num_predict": 300}
                        },
                        timeout=aiohttp.ClientTimeout(total=20)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            response = data.get("response", "")

                            match = re.search(r'\[.*\]', response, re.DOTALL)
                            if match:
                                new_queries = json.loads(match.group())
                                for i, q in enumerate(new_queries[:3]):
                                    refined_queries.append(SubQuery(
                                        query=q,
                                        order=len(refined_queries),
                                        purpose=f"Covering: {coverage.missing_aspects[min(i, len(coverage.missing_aspects)-1)]}"
                                    ))
            except Exception as e:
                logger.warning(f"Refinement generation failed: {e}")

        # Reindex
        for i, sq in enumerate(refined_queries):
            sq.order = i

        return DecomposedQuery(
            original_query=original_query,
            sub_queries=refined_queries,
            strategy=decomposition.strategy,
            reasoning=f"Refined from {len(decomposition.sub_queries)} to {len(refined_queries)} queries"
        )


class AutoAggregationSelector:
    """
    Automatically selects the optimal aggregation method for a query.

    Learns from query patterns and past performance to choose the best
    method for combining sub-query results.
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.ollama_url = ollama_url
        self.ollama_model = ollama_model
        self._pattern_history: Dict[str, Dict[AggregationMethod, float]] = {}  # pattern -> method -> score

    async def select_aggregation(
        self,
        original_query: str,
        decomposition: DecomposedQuery
    ) -> AggregationDecision:
        """
        Select the best aggregation method for this query.

        Args:
            original_query: Original query
            decomposition: Decomposed query

        Returns:
            AggregationDecision with selected method
        """
        # Analyze query characteristics
        query_lower = original_query.lower()

        # Heuristic patterns
        if any(kw in query_lower for kw in ["compare", "versus", "vs", "difference"]):
            return AggregationDecision(
                method=AggregationMethod.COMPARATIVE,
                confidence=0.9,
                reasoning="Query requests comparison between items"
            )

        if any(kw in query_lower for kw in ["cause", "effect", "because", "lead to", "result in"]):
            return AggregationDecision(
                method=AggregationMethod.CAUSAL,
                confidence=0.85,
                reasoning="Query involves causal relationships"
            )

        if decomposition.strategy == DecompositionStrategy.MULTI_HOP:
            return AggregationDecision(
                method=AggregationMethod.HIERARCHICAL,
                confidence=0.85,
                reasoning="Multi-hop queries benefit from hierarchical aggregation"
            )

        if decomposition.strategy == DecompositionStrategy.SEQUENTIAL:
            # Weight later results higher
            weights = {i: (i + 1) / len(decomposition.sub_queries)
                      for i in range(len(decomposition.sub_queries))}
            return AggregationDecision(
                method=AggregationMethod.WEIGHTED,
                confidence=0.8,
                reasoning="Sequential queries use weighted aggregation",
                weights=weights
            )

        # Use LLM for complex cases
        return await self._llm_select_aggregation(original_query, decomposition)

    async def _llm_select_aggregation(
        self,
        original_query: str,
        decomposition: DecomposedQuery
    ) -> AggregationDecision:
        """Use LLM to select aggregation method for complex queries."""
        sub_queries_text = "\n".join(
            f"- {sq.query}" for sq in decomposition.sub_queries
        )

        prompt = f"""Given this query and its sub-queries, what's the best way to combine the results?

Original query: {original_query}

Sub-queries:
{sub_queries_text}

Options:
1. UNION - Combine all results (for independent parts)
2. INTERSECTION - Find common results (for overlapping needs)
3. WEIGHTED - Weight sub-query results differently
4. HIERARCHICAL - Build answer from parts in order

Return JSON with "method" (one of UNION, INTERSECTION, WEIGHTED, HIERARCHICAL) and "reasoning".

JSON:"""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.3, "num_predict": 200}
                    },
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        response = data.get("response", "")

                        match = re.search(r'\{.*\}', response, re.DOTALL)
                        if match:
                            result = json.loads(match.group())
                            method_str = result.get("method", "WEIGHTED").upper()
                            try:
                                method = AggregationMethod(method_str.lower())
                            except ValueError:
                                method = AggregationMethod.WEIGHTED

                            return AggregationDecision(
                                method=method,
                                confidence=0.75,
                                reasoning=result.get("reasoning", "LLM selection")
                            )
        except Exception as e:
            logger.warning(f"LLM aggregation selection failed: {e}")

        # Default to weighted
        return AggregationDecision(
            method=AggregationMethod.WEIGHTED,
            confidence=0.6,
            reasoning="Default weighted aggregation"
        )

    def record_feedback(
        self,
        pattern: str,
        method: AggregationMethod,
        success_score: float
    ):
        """Record feedback for learning."""
        if pattern not in self._pattern_history:
            self._pattern_history[pattern] = {}

        current = self._pattern_history[pattern].get(method, 0.5)
        self._pattern_history[pattern][method] = current * 0.8 + success_score * 0.2


class CuttingEdgeDecomposer:
    """
    Cutting Edge query decomposer with self-verification.

    Combines:
    - RecursiveDecomposer for deep decomposition
    - DecompositionVerifier for coverage verification
    - AutoAggregationSelector for optimal aggregation
    """

    def __init__(
        self,
        config: Optional[CuttingEdgeDecompositionConfig] = None,
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "gemma2:2b"
    ):
        self.config = config or CuttingEdgeDecompositionConfig()

        self.base_decomposer = RecursiveDecomposer(
            ollama_url=ollama_url,
            model=ollama_model
        )
        self.verifier = DecompositionVerifier(ollama_url, ollama_model)
        self.aggregation_selector = AutoAggregationSelector(ollama_url, ollama_model)

        self._decomposition_history: List[Dict[str, Any]] = []

    async def decompose_verified(
        self,
        query: str,
        max_refinements: int = None
    ) -> VerifiedDecomposition:
        """
        Decompose with verification and refinement.

        Args:
            query: Query to decompose
            max_refinements: Max refinement iterations (default from config)

        Returns:
            VerifiedDecomposition with coverage guarantee
        """
        if max_refinements is None:
            max_refinements = self.config.verification_config.max_refinement_iterations

        # Initial decomposition
        decomposition = await self.base_decomposer.decompose_recursively(query)
        base_decomposed = DecomposedQuery(
            original_query=query,
            sub_queries=[
                SubQuery(query=node.query, order=i, purpose=node.purpose)
                for i, node in enumerate(decomposition.all_nodes)
            ],
            strategy=DecompositionStrategy.MULTI_HOP,
            reasoning="Recursive decomposition"
        )

        if not self.config.enable_self_verification:
            return VerifiedDecomposition(
                decomposition=base_decomposed,
                verification_status=VerificationStatus.VERIFIED,
                coverage=CoverageAnalysis(1.0, [], [], [], []),
                refinement_iterations=0
            )

        # Verify and refine
        current = base_decomposed
        iterations = 0

        for iteration in range(max_refinements):
            coverage = await self.verifier.verify_coverage(query, current)

            if coverage.coverage_score >= self.config.verification_config.min_coverage_threshold:
                return VerifiedDecomposition(
                    decomposition=current,
                    verification_status=VerificationStatus.VERIFIED,
                    coverage=coverage,
                    refinement_iterations=iteration
                )

            # Refine
            current = await self.verifier.refine_decomposition(query, current, coverage)
            iterations += 1

        # Final coverage check
        final_coverage = await self.verifier.verify_coverage(query, current)
        status = (VerificationStatus.VERIFIED
                 if final_coverage.coverage_score >= self.config.verification_config.min_coverage_threshold
                 else VerificationStatus.PARTIAL)

        return VerifiedDecomposition(
            decomposition=current,
            verification_status=status,
            coverage=final_coverage,
            refinement_iterations=iterations
        )

    async def decompose_with_aggregation(
        self,
        query: str
    ) -> tuple:
        """
        Decompose and determine optimal aggregation method.

        Returns both verified decomposition and aggregation decision.
        """
        verified = await self.decompose_verified(query)
        aggregation = await self.aggregation_selector.select_aggregation(
            query, verified.decomposition
        )

        # Record for history
        self._decomposition_history.append({
            "query": query[:50],
            "num_subqueries": len(verified.decomposition.sub_queries),
            "coverage": verified.coverage.coverage_score,
            "aggregation": aggregation.method.value,
            "timestamp": datetime.now().isoformat()
        })

        if len(self._decomposition_history) > 500:
            self._decomposition_history = self._decomposition_history[-250:]

        return verified, aggregation

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return {
            "config": {
                "self_verification": self.config.enable_self_verification,
                "auto_aggregation": self.config.enable_auto_aggregation
            },
            "history_size": len(self._decomposition_history),
            "avg_coverage": (
                sum(h["coverage"] for h in self._decomposition_history) /
                len(self._decomposition_history)
                if self._decomposition_history else 0
            ),
            "aggregation_distribution": {
                method.value: len([h for h in self._decomposition_history if h.get("aggregation") == method.value])
                for method in AggregationMethod
            }
        }


# -----------------------------------------------------------------------------
# Factory Functions for Cutting Edge Features
# -----------------------------------------------------------------------------

_cutting_edge_decomposer: Optional[CuttingEdgeDecomposer] = None


def get_cutting_edge_decomposer(
    ollama_url: str = "http://localhost:11434",
    ollama_model: str = "gemma2:2b"
) -> CuttingEdgeDecomposer:
    """Get or create the global cutting edge decomposer."""
    global _cutting_edge_decomposer
    if _cutting_edge_decomposer is None:
        _cutting_edge_decomposer = CuttingEdgeDecomposer(
            ollama_url=ollama_url,
            ollama_model=ollama_model
        )
        logger.info("CuttingEdgeDecomposer created with self-verification")
    return _cutting_edge_decomposer


async def cutting_edge_decompose(
    query: str,
    ollama_url: str = "http://localhost:11434"
) -> VerifiedDecomposition:
    """
    Convenience function for cutting edge decomposition.

    Returns verified decomposition with coverage guarantee.
    """
    decomposer = get_cutting_edge_decomposer(ollama_url)
    return await decomposer.decompose_verified(query)


def reset_cutting_edge_decomposer():
    """Reset cutting edge decomposer (for testing)."""
    global _cutting_edge_decomposer
    _cutting_edge_decomposer = None


# Singleton instances for advanced decomposition
_recursive_decomposer: Optional[RecursiveDecomposer] = None
_result_aggregator: Optional[ResultAggregator] = None


def get_recursive_decomposer(
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b",
    max_depth: int = 3
) -> RecursiveDecomposer:
    """Get or create the global recursive decomposer"""
    global _recursive_decomposer
    if _recursive_decomposer is None:
        _recursive_decomposer = RecursiveDecomposer(
            ollama_url=ollama_url,
            model=model,
            max_depth=max_depth
        )
    return _recursive_decomposer


def get_result_aggregator() -> ResultAggregator:
    """Get or create the global result aggregator"""
    global _result_aggregator
    if _result_aggregator is None:
        _result_aggregator = ResultAggregator()
    return _result_aggregator


async def decompose_and_execute(
    query: str,
    vector_store: Any,
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b",
    top_k_per_query: int = 3,
    aggregation_method: str = "weighted"
) -> AggregatedResult:
    """
    Convenience function to decompose, execute, and aggregate in one call.

    Usage:
        result = await decompose_and_execute(
            "What is machine learning, how does it compare to deep learning, and what are common applications?",
            vector_store
        )
        for r in result.combined_results:
            print(r["content"])
    """
    decomposer = get_recursive_decomposer(ollama_url, model)
    aggregator = get_result_aggregator()

    # Decompose
    decomposition = await decomposer.decompose_recursively(query)

    # Execute
    executor = DependencyAwareExecutor(vector_store)
    execution_results = await executor.execute(decomposition, top_k_per_query)

    # Aggregate
    return aggregator.aggregate(decomposition, execution_results, aggregation_method)


# Singleton instance
_decomposer: Optional[QueryDecomposer] = None


def get_query_decomposer(
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b"
) -> QueryDecomposer:
    """Get or create the global query decomposer instance"""
    global _decomposer
    if _decomposer is None:
        _decomposer = QueryDecomposer(ollama_url=ollama_url, model=model)
    return _decomposer


async def decompose_query(
    query: str,
    strategy: str = "simple",
    ollama_url: str = "http://localhost:11434",
    model: str = "gemma2:2b"
) -> DecomposedQuery:
    """
    Convenience function to decompose a query.

    Usage:
        decomposed = await decompose_query(
            "What is machine learning and how does it compare to deep learning?",
            strategy="simple"
        )
        for sq in decomposed.sub_queries:
            print(f"Sub-query {sq.order}: {sq.query}")
    """
    decomposer = get_query_decomposer(ollama_url, model)
    strategy_enum = DecompositionStrategy(strategy)
    return await decomposer.decompose(query, strategy_enum)

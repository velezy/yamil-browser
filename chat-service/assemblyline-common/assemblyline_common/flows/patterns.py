"""
Flow Patterns for Logic Weaver

Enterprise integration patterns for parallel processing, iteration, and routing.
Implementation of MuleSoft-like patterns with Python async support.

Patterns:
- Scatter-Gather: Parallel execution with aggregation
- For-Each: Iterative processing with batching
- Choice Router: Conditional routing
- Batch Processor: Bulk processing with checkpointing

Example:
    # Scatter-Gather
    scatter = ScatterGatherNode(
        routes=[route1, route2, route3],
        aggregation=AggregationStrategy.MERGE,
        timeout_seconds=30,
    )
    result = await scatter.execute(payload)

    # For-Each
    foreach = ForEachNode(
        collection_path="$.items",
        batch_size=10,
        parallel=True,
    )
    results = await foreach.execute(payload, processor)
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from uuid import uuid4

logger = logging.getLogger(__name__)


class AggregationStrategy(Enum):
    """How to aggregate scatter-gather results"""
    MERGE = "merge"  # Merge all results into one dict
    LIST = "list"  # Return list of results
    FIRST = "first"  # Return first successful result
    LAST = "last"  # Return last result
    CUSTOM = "custom"  # Custom aggregation function


class RoutingStrategy(Enum):
    """How choice router selects routes"""
    FIRST_MATCH = "first_match"  # First matching route
    ALL_MATCH = "all_match"  # All matching routes (parallel)
    ROUND_ROBIN = "round_robin"  # Distribute across routes


@dataclass
class RouteResult:
    """Result from a single route execution"""
    route_id: str
    success: bool
    output: Any
    execution_time_ms: int
    error: Optional[str] = None


@dataclass
class RouterCondition:
    """
    Condition for choice router.

    Attributes:
        expression: Python expression or JSONPath
        route_id: Target route ID
        description: Human-readable description
        priority: Higher priority evaluated first
    """
    expression: str
    route_id: str
    description: str = ""
    priority: int = 0

    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate condition against context"""
        try:
            # Simple expression evaluation
            # Supports: $.field == value, $.field > value, etc.
            expr = self.expression

            # JSONPath shorthand
            if expr.startswith("$."):
                # Extract field and operator
                parts = expr.split(" ", 2)
                if len(parts) >= 3:
                    field_path = parts[0]
                    operator = parts[1]
                    value = parts[2]

                    # Get field value
                    field_value = self._get_field(field_path, context)

                    # Evaluate
                    return self._compare(field_value, operator, value)

            # Direct Python expression
            return eval(expr, {"__builtins__": {}}, context)

        except Exception as e:
            logger.warning(f"Condition evaluation failed: {e}")
            return False

    def _get_field(self, path: str, data: Dict[str, Any]) -> Any:
        """Extract field value from JSONPath"""
        if path.startswith("$."):
            path = path[2:]

        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)]
            else:
                return None

        return current

    def _compare(self, field_value: Any, operator: str, value: str) -> bool:
        """Compare field value with operator"""
        # Parse value
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        elif value.isdigit():
            value = int(value)
        else:
            try:
                value = float(value)
            except ValueError:
                pass

        if operator == "==":
            return field_value == value
        elif operator == "!=":
            return field_value != value
        elif operator == ">":
            return field_value > value
        elif operator == ">=":
            return field_value >= value
        elif operator == "<":
            return field_value < value
        elif operator == "<=":
            return field_value <= value
        elif operator == "contains":
            return value in field_value if field_value else False
        elif operator == "startswith":
            return str(field_value).startswith(str(value)) if field_value else False
        elif operator == "endswith":
            return str(field_value).endswith(str(value)) if field_value else False
        elif operator == "matches":
            import re
            return bool(re.match(str(value), str(field_value))) if field_value else False

        return False


@dataclass
class ScatterGatherRoute:
    """A route in scatter-gather"""
    id: str
    name: str
    flow_id: Optional[str] = None
    processor: Optional[Callable] = None
    timeout_seconds: int = 30
    required: bool = True  # If false, errors don't fail the whole operation


class ScatterGatherNode:
    """
    Scatter-Gather pattern for parallel execution.

    Executes multiple routes in parallel and aggregates results.

    Example:
        scatter = ScatterGatherNode(
            routes=[
                ScatterGatherRoute("route1", "Call API 1", processor=api1_call),
                ScatterGatherRoute("route2", "Call API 2", processor=api2_call),
            ],
            aggregation=AggregationStrategy.MERGE,
            timeout_seconds=30,
        )
        result = await scatter.execute(payload)
    """

    def __init__(
        self,
        routes: List[ScatterGatherRoute],
        aggregation: AggregationStrategy = AggregationStrategy.LIST,
        timeout_seconds: int = 60,
        fail_fast: bool = False,
        max_concurrency: Optional[int] = None,
        custom_aggregator: Optional[Callable] = None,
    ):
        self.routes = routes
        self.aggregation = aggregation
        self.timeout_seconds = timeout_seconds
        self.fail_fast = fail_fast
        self.max_concurrency = max_concurrency
        self.custom_aggregator = custom_aggregator

    async def execute(
        self,
        payload: Dict[str, Any],
        flow_executor: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Execute all routes in parallel.

        Args:
            payload: Input data for all routes
            flow_executor: Function to execute flow IDs

        Returns:
            Aggregated results
        """
        start_time = datetime.now()
        results: List[RouteResult] = []

        # Create tasks for each route
        tasks = []
        semaphore = asyncio.Semaphore(self.max_concurrency or len(self.routes))

        for route in self.routes:
            task = self._execute_route(route, payload, flow_executor, semaphore)
            tasks.append(task)

        # Execute with timeout
        try:
            if self.fail_fast:
                # Stop on first error
                done, pending = await asyncio.wait(
                    [asyncio.create_task(t) for t in tasks],
                    timeout=self.timeout_seconds,
                    return_when=asyncio.FIRST_EXCEPTION,
                )
                for task in pending:
                    task.cancel()
                results = [t.result() for t in done if not t.cancelled()]
            else:
                # Wait for all
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.timeout_seconds,
                )
                # Convert exceptions to RouteResult
                results = [
                    r if isinstance(r, RouteResult) else RouteResult(
                        route_id="unknown",
                        success=False,
                        output=None,
                        execution_time_ms=0,
                        error=str(r),
                    )
                    for r in results
                ]

        except asyncio.TimeoutError:
            logger.warning(f"Scatter-gather timeout after {self.timeout_seconds}s")
            return {"error": "timeout", "partial_results": results}

        execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

        # Check for required route failures
        for route, result in zip(self.routes, results):
            if route.required and not result.success:
                logger.error(f"Required route {route.id} failed: {result.error}")
                if self.fail_fast:
                    return {
                        "error": f"Required route {route.id} failed",
                        "results": [r.output for r in results if r.success],
                    }

        # Aggregate results
        return self._aggregate(results, execution_time)

    async def _execute_route(
        self,
        route: ScatterGatherRoute,
        payload: Dict[str, Any],
        flow_executor: Optional[Callable],
        semaphore: asyncio.Semaphore,
    ) -> RouteResult:
        """Execute a single route"""
        async with semaphore:
            start = datetime.now()
            try:
                if route.processor:
                    result = await route.processor(payload)
                elif route.flow_id and flow_executor:
                    result = await flow_executor(route.flow_id, payload)
                else:
                    result = payload  # Pass-through

                execution_time = int((datetime.now() - start).total_seconds() * 1000)

                return RouteResult(
                    route_id=route.id,
                    success=True,
                    output=result,
                    execution_time_ms=execution_time,
                )

            except Exception as e:
                execution_time = int((datetime.now() - start).total_seconds() * 1000)
                logger.error(f"Route {route.id} failed: {e}")

                return RouteResult(
                    route_id=route.id,
                    success=False,
                    output=None,
                    execution_time_ms=execution_time,
                    error=str(e),
                )

    def _aggregate(
        self,
        results: List[RouteResult],
        total_time_ms: int,
    ) -> Dict[str, Any]:
        """Aggregate results based on strategy"""

        if self.aggregation == AggregationStrategy.CUSTOM and self.custom_aggregator:
            return self.custom_aggregator(results)

        successful = [r for r in results if r.success]

        if self.aggregation == AggregationStrategy.LIST:
            return {
                "results": [r.output for r in results],
                "successful": len(successful),
                "failed": len(results) - len(successful),
                "execution_time_ms": total_time_ms,
            }

        elif self.aggregation == AggregationStrategy.MERGE:
            merged = {}
            for r in successful:
                if isinstance(r.output, dict):
                    merged.update(r.output)
            return {
                "merged": merged,
                "execution_time_ms": total_time_ms,
            }

        elif self.aggregation == AggregationStrategy.FIRST:
            return {
                "result": successful[0].output if successful else None,
                "execution_time_ms": total_time_ms,
            }

        elif self.aggregation == AggregationStrategy.LAST:
            return {
                "result": successful[-1].output if successful else None,
                "execution_time_ms": total_time_ms,
            }

        return {"results": [r.output for r in results]}


class ForEachNode:
    """
    For-Each pattern for iterative processing.

    Processes a collection of items with optional batching and parallelism.

    Example:
        foreach = ForEachNode(
            collection_path="$.items",
            batch_size=10,
            parallel=True,
            max_concurrency=5,
        )
        results = await foreach.execute(payload, processor)
    """

    def __init__(
        self,
        collection_path: str = "$.items",
        batch_size: int = 1,
        parallel: bool = False,
        max_concurrency: int = 10,
        continue_on_error: bool = True,
        root_message_variable: str = "rootMessage",
        counter_variable: str = "counter",
    ):
        self.collection_path = collection_path
        self.batch_size = batch_size
        self.parallel = parallel
        self.max_concurrency = max_concurrency
        self.continue_on_error = continue_on_error
        self.root_message_variable = root_message_variable
        self.counter_variable = counter_variable

    async def execute(
        self,
        payload: Dict[str, Any],
        processor: Callable,
    ) -> Dict[str, Any]:
        """
        Execute processor for each item in collection.

        Args:
            payload: Input containing collection
            processor: Async function to process each item

        Returns:
            Results dict with processed items and stats
        """
        # Extract collection
        collection = self._extract_collection(payload)
        if not collection:
            return {"results": [], "processed": 0, "errors": 0}

        # Process in batches
        results = []
        errors = []
        batches = self._create_batches(collection)

        for batch_index, batch in enumerate(batches):
            batch_results = await self._process_batch(
                batch,
                processor,
                payload,
                batch_index * self.batch_size,
            )
            for r in batch_results:
                if r.get("error"):
                    errors.append(r)
                    if not self.continue_on_error:
                        return {
                            "results": results,
                            "errors": errors,
                            "processed": len(results),
                            "failed": len(errors),
                            "aborted": True,
                        }
                else:
                    results.append(r.get("output"))

        return {
            "results": results,
            "errors": errors,
            "processed": len(results),
            "failed": len(errors),
            "total": len(collection),
        }

    def _extract_collection(self, payload: Dict[str, Any]) -> List[Any]:
        """Extract collection from payload using path"""
        path = self.collection_path
        if path.startswith("$."):
            path = path[2:]

        parts = path.split(".")
        current = payload

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)]
            else:
                return []

        return current if isinstance(current, list) else []

    def _create_batches(self, collection: List[Any]) -> List[List[Any]]:
        """Split collection into batches"""
        return [
            collection[i:i + self.batch_size]
            for i in range(0, len(collection), self.batch_size)
        ]

    async def _process_batch(
        self,
        batch: List[Any],
        processor: Callable,
        root_payload: Dict[str, Any],
        start_index: int,
    ) -> List[Dict[str, Any]]:
        """Process a batch of items"""

        async def process_item(item: Any, index: int) -> Dict[str, Any]:
            context = {
                "payload": item,
                self.root_message_variable: root_payload,
                self.counter_variable: index,
            }
            try:
                result = await processor(context)
                return {"output": result}
            except Exception as e:
                logger.error(f"Item {index} processing failed: {e}")
                return {"error": str(e), "index": index}

        if self.parallel:
            semaphore = asyncio.Semaphore(self.max_concurrency)

            async def limited_process(item, idx):
                async with semaphore:
                    return await process_item(item, idx)

            tasks = [
                limited_process(item, start_index + i)
                for i, item in enumerate(batch)
            ]
            return await asyncio.gather(*tasks)
        else:
            results = []
            for i, item in enumerate(batch):
                result = await process_item(item, start_index + i)
                results.append(result)
            return results


class ChoiceRouter:
    """
    Choice Router pattern for conditional routing.

    Routes messages to different flows based on conditions.

    Example:
        router = ChoiceRouter(
            conditions=[
                RouterCondition("$.type == 'order'", "order-flow"),
                RouterCondition("$.type == 'refund'", "refund-flow"),
            ],
            default_route="default-flow",
        )
        result = await router.route(payload, flow_executor)
    """

    def __init__(
        self,
        conditions: List[RouterCondition],
        default_route: Optional[str] = None,
        strategy: RoutingStrategy = RoutingStrategy.FIRST_MATCH,
    ):
        self.conditions = sorted(conditions, key=lambda c: -c.priority)
        self.default_route = default_route
        self.strategy = strategy

    async def route(
        self,
        payload: Dict[str, Any],
        flow_executor: Callable,
    ) -> Dict[str, Any]:
        """
        Route payload to matching flow(s).

        Args:
            payload: Input payload
            flow_executor: Function to execute flows

        Returns:
            Result from executed flow(s)
        """
        matching_routes = self._evaluate_conditions(payload)

        if not matching_routes:
            if self.default_route:
                logger.debug(f"No conditions matched, using default route")
                return await flow_executor(self.default_route, payload)
            else:
                return {"error": "No matching route", "payload": payload}

        if self.strategy == RoutingStrategy.FIRST_MATCH:
            route_id = matching_routes[0]
            return await flow_executor(route_id, payload)

        elif self.strategy == RoutingStrategy.ALL_MATCH:
            tasks = [flow_executor(route_id, payload) for route_id in matching_routes]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return {
                "routes_executed": matching_routes,
                "results": results,
            }

        elif self.strategy == RoutingStrategy.ROUND_ROBIN:
            # Use hash of payload for deterministic distribution
            import hashlib
            payload_hash = hashlib.md5(str(payload).encode()).hexdigest()
            index = int(payload_hash, 16) % len(matching_routes)
            route_id = matching_routes[index]
            return await flow_executor(route_id, payload)

        return {"error": "Unknown routing strategy"}

    def _evaluate_conditions(self, payload: Dict[str, Any]) -> List[str]:
        """Evaluate all conditions and return matching route IDs"""
        matching = []
        for condition in self.conditions:
            if condition.evaluate(payload):
                matching.append(condition.route_id)
                if self.strategy == RoutingStrategy.FIRST_MATCH:
                    break
        return matching


@dataclass
class BatchProcessorConfig:
    """Configuration for batch processor"""
    batch_size: int = 100
    max_records: int = 0  # 0 = unlimited
    checkpoint_interval: int = 10  # Checkpoint every N batches
    parallel_batches: int = 1
    on_complete_flow: Optional[str] = None
    on_error_flow: Optional[str] = None


class BatchProcessor:
    """
    Batch Processor for large-scale data processing.

    Features:
    - Configurable batch sizes
    - Checkpointing for fault tolerance
    - Parallel batch processing
    - On-complete and on-error hooks

    Example:
        processor = BatchProcessor(
            config=BatchProcessorConfig(
                batch_size=1000,
                checkpoint_interval=5,
                parallel_batches=4,
            )
        )
        result = await processor.process(records, handler)
    """

    def __init__(self, config: Optional[BatchProcessorConfig] = None):
        self.config = config or BatchProcessorConfig()
        self._checkpoint_data: Dict[str, Any] = {}

    async def process(
        self,
        records: List[Any],
        handler: Callable,
        checkpoint_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Process records in batches.

        Args:
            records: List of records to process
            handler: Async function to handle each batch
            checkpoint_callback: Function to save checkpoint

        Returns:
            Processing results and statistics
        """
        start_time = datetime.now()
        total_records = len(records)

        if self.config.max_records > 0:
            records = records[:self.config.max_records]

        batches = self._create_batches(records)
        total_batches = len(batches)

        processed = 0
        failed = 0
        batch_results = []

        for batch_index, batch in enumerate(batches):
            try:
                result = await handler(batch, batch_index)
                batch_results.append(result)
                processed += len(batch)

                # Checkpoint
                if (batch_index + 1) % self.config.checkpoint_interval == 0:
                    if checkpoint_callback:
                        await checkpoint_callback({
                            "batch_index": batch_index,
                            "processed": processed,
                            "total": total_records,
                        })

            except Exception as e:
                logger.error(f"Batch {batch_index} failed: {e}")
                failed += len(batch)
                if not self.config.on_error_flow:
                    raise

        execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

        return {
            "total_records": total_records,
            "processed": processed,
            "failed": failed,
            "batches": total_batches,
            "execution_time_ms": execution_time,
            "records_per_second": processed / (execution_time / 1000) if execution_time > 0 else 0,
        }

    def _create_batches(self, records: List[Any]) -> List[List[Any]]:
        """Split records into batches"""
        return [
            records[i:i + self.config.batch_size]
            for i in range(0, len(records), self.config.batch_size)
        ]

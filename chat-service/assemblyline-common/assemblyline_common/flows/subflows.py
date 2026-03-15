"""
Subflow System for Logic Weaver

Enables reusable flow components that can be called from parent flows.
Superior to MuleSoft's subflow with Python-native execution and hot reload.

Features:
- Subflow references by ID or name
- Input/output variable mapping
- Execution isolation (separate context)
- Circular dependency detection
- Hot reload support
- Version pinning

Example:
    # Register a subflow
    registry = get_subflow_registry()
    registry.register("validate-patient", flow_definition)

    # Use in parent flow
    subflow_node = {
        "type": "subflow",
        "data": {
            "subflow_id": "validate-patient",
            "input_mapping": {"patient": "$.payload.patient"},
            "output_mapping": {"validated": "$.result"}
        }
    }
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import UUID, uuid4
import copy

logger = logging.getLogger(__name__)


class SubflowExecutionMode(Enum):
    """How subflow execution affects parent flow"""
    SYNC = "sync"  # Wait for completion
    ASYNC = "async"  # Fire and forget
    ASYNC_WAIT = "async_wait"  # Async but wait at end


@dataclass
class SubflowReference:
    """
    Reference to a subflow from a parent flow.

    Attributes:
        subflow_id: UUID or name of the subflow
        version: Optional version to pin (None = latest)
        input_mapping: Map parent variables to subflow input
        output_mapping: Map subflow output to parent variables
        execution_mode: Sync, async, or async with wait
        timeout_seconds: Maximum execution time
        error_handler: How to handle subflow errors
    """
    subflow_id: str
    version: Optional[int] = None
    input_mapping: Dict[str, str] = field(default_factory=dict)
    output_mapping: Dict[str, str] = field(default_factory=dict)
    execution_mode: SubflowExecutionMode = SubflowExecutionMode.SYNC
    timeout_seconds: int = 300
    error_handler: str = "propagate"  # propagate, continue, fallback

    def to_dict(self) -> Dict[str, Any]:
        return {
            "subflow_id": self.subflow_id,
            "version": self.version,
            "input_mapping": self.input_mapping,
            "output_mapping": self.output_mapping,
            "execution_mode": self.execution_mode.value,
            "timeout_seconds": self.timeout_seconds,
            "error_handler": self.error_handler,
        }


@dataclass
class SubflowDefinition:
    """
    A registered subflow definition.

    Attributes:
        id: Unique identifier
        name: Human-readable name
        description: What this subflow does
        version: Version number
        input_schema: Expected input structure
        output_schema: Output structure
        flow_definition: The actual flow nodes/edges
        is_public: Can be used by other tenants
        tenant_id: Owner tenant
    """
    id: str
    name: str
    description: str
    version: int
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    flow_definition: Dict[str, Any]
    is_public: bool = False
    tenant_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
            "is_public": self.is_public,
            "tenant_id": self.tenant_id,
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class SubflowExecutionContext:
    """Context for subflow execution"""
    execution_id: str = field(default_factory=lambda: str(uuid4()))
    parent_execution_id: Optional[str] = None
    parent_flow_id: Optional[str] = None
    depth: int = 0
    call_stack: List[str] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    timeout_seconds: int = 300


@dataclass
class SubflowExecutionResult:
    """Result of subflow execution"""
    success: bool
    output: Dict[str, Any]
    execution_time_ms: int
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None


class SubflowRegistry:
    """
    Registry for subflow definitions.

    Provides:
    - Registration and lookup of subflows
    - Version management
    - Hot reload capability
    - Dependency tracking
    """

    def __init__(self):
        self._subflows: Dict[str, Dict[int, SubflowDefinition]] = {}
        self._name_to_id: Dict[str, str] = {}
        self._dependencies: Dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        name: str,
        flow_definition: Dict[str, Any],
        description: str = "",
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        is_public: bool = False,
        tags: Optional[List[str]] = None,
    ) -> SubflowDefinition:
        """
        Register a new subflow or new version.

        Args:
            name: Unique name for the subflow
            flow_definition: React Flow nodes/edges definition
            description: What this subflow does
            input_schema: JSON Schema for expected input
            output_schema: JSON Schema for output
            tenant_id: Owner tenant (None for global)
            is_public: Can other tenants use this
            tags: Categorization tags

        Returns:
            The registered SubflowDefinition
        """
        async with self._lock:
            # Get or create ID
            subflow_id = self._name_to_id.get(name) or str(uuid4())
            self._name_to_id[name] = subflow_id

            # Determine version
            versions = self._subflows.get(subflow_id, {})
            new_version = max(versions.keys(), default=0) + 1

            # Create definition
            definition = SubflowDefinition(
                id=subflow_id,
                name=name,
                description=description,
                version=new_version,
                input_schema=input_schema or {},
                output_schema=output_schema or {},
                flow_definition=flow_definition,
                is_public=is_public,
                tenant_id=tenant_id,
                tags=tags or [],
            )

            # Register
            if subflow_id not in self._subflows:
                self._subflows[subflow_id] = {}
            self._subflows[subflow_id][new_version] = definition

            # Track dependencies
            self._dependencies[subflow_id] = self._extract_dependencies(flow_definition)

            logger.info(f"Registered subflow '{name}' version {new_version}")
            return definition

    def _extract_dependencies(self, flow_definition: Dict[str, Any]) -> Set[str]:
        """Extract subflow dependencies from flow definition"""
        dependencies = set()
        for node in flow_definition.get("nodes", []):
            if node.get("type") == "subflow":
                ref_id = node.get("data", {}).get("subflow_id")
                if ref_id:
                    dependencies.add(ref_id)
        return dependencies

    async def get(
        self,
        subflow_id: str,
        version: Optional[int] = None,
        tenant_id: Optional[str] = None,
    ) -> Optional[SubflowDefinition]:
        """
        Get a subflow definition.

        Args:
            subflow_id: ID or name of the subflow
            version: Specific version (None = latest)
            tenant_id: For tenant-scoped lookup

        Returns:
            SubflowDefinition or None
        """
        # Try by ID first
        actual_id = subflow_id
        if subflow_id in self._name_to_id:
            actual_id = self._name_to_id[subflow_id]

        versions = self._subflows.get(actual_id, {})
        if not versions:
            return None

        if version:
            return versions.get(version)

        # Get latest version
        latest_version = max(versions.keys())
        definition = versions[latest_version]

        # Check tenant access
        if definition.tenant_id and definition.tenant_id != tenant_id:
            if not definition.is_public:
                return None

        return definition

    async def list(
        self,
        tenant_id: Optional[str] = None,
        include_public: bool = True,
        tags: Optional[List[str]] = None,
    ) -> List[SubflowDefinition]:
        """
        List available subflows.

        Args:
            tenant_id: Filter by tenant
            include_public: Include public subflows
            tags: Filter by tags

        Returns:
            List of SubflowDefinition (latest versions only)
        """
        results = []
        for subflow_id, versions in self._subflows.items():
            latest_version = max(versions.keys())
            definition = versions[latest_version]

            # Tenant filter
            if definition.tenant_id:
                if definition.tenant_id != tenant_id:
                    if not (include_public and definition.is_public):
                        continue

            # Tag filter
            if tags:
                if not any(t in definition.tags for t in tags):
                    continue

            results.append(definition)

        return results

    async def delete(self, subflow_id: str, version: Optional[int] = None):
        """Delete a subflow or specific version"""
        actual_id = self._name_to_id.get(subflow_id, subflow_id)

        if actual_id in self._subflows:
            if version:
                self._subflows[actual_id].pop(version, None)
            else:
                del self._subflows[actual_id]
                # Remove name mapping
                name_to_remove = None
                for name, id_ in self._name_to_id.items():
                    if id_ == actual_id:
                        name_to_remove = name
                        break
                if name_to_remove:
                    del self._name_to_id[name_to_remove]

    def check_circular_dependency(self, subflow_id: str, visited: Optional[Set[str]] = None) -> bool:
        """Check for circular dependencies"""
        if visited is None:
            visited = set()

        if subflow_id in visited:
            return True

        visited.add(subflow_id)
        deps = self._dependencies.get(subflow_id, set())

        for dep_id in deps:
            if self.check_circular_dependency(dep_id, visited.copy()):
                return True

        return False


class SubflowExecutor:
    """
    Executes subflows with proper isolation and variable mapping.

    Features:
    - Input/output variable mapping with JSONPath
    - Execution isolation (separate context)
    - Timeout handling
    - Error propagation/handling
    - Circular dependency detection
    """

    MAX_DEPTH = 10  # Maximum subflow nesting depth

    def __init__(
        self,
        registry: SubflowRegistry,
        flow_executor: Optional[Callable] = None,
    ):
        """
        Initialize executor.

        Args:
            registry: SubflowRegistry for lookups
            flow_executor: Function to execute flow definitions
        """
        self.registry = registry
        self._flow_executor = flow_executor

    def set_flow_executor(self, executor: Callable):
        """Set the flow executor (for dependency injection)"""
        self._flow_executor = executor

    async def execute(
        self,
        reference: SubflowReference,
        parent_context: Dict[str, Any],
        execution_context: Optional[SubflowExecutionContext] = None,
    ) -> SubflowExecutionResult:
        """
        Execute a subflow.

        Args:
            reference: SubflowReference with configuration
            parent_context: Parent flow's variable context
            execution_context: Execution tracking context

        Returns:
            SubflowExecutionResult
        """
        start_time = datetime.now()

        # Initialize context if not provided
        if execution_context is None:
            execution_context = SubflowExecutionContext()

        # Check depth limit
        if execution_context.depth >= self.MAX_DEPTH:
            return SubflowExecutionResult(
                success=False,
                output={},
                execution_time_ms=0,
                error=f"Maximum subflow depth ({self.MAX_DEPTH}) exceeded",
            )

        # Check circular dependency
        if reference.subflow_id in execution_context.call_stack:
            return SubflowExecutionResult(
                success=False,
                output={},
                execution_time_ms=0,
                error=f"Circular dependency detected: {' -> '.join(execution_context.call_stack)} -> {reference.subflow_id}",
            )

        try:
            # Get subflow definition
            definition = await self.registry.get(
                reference.subflow_id,
                version=reference.version,
            )

            if not definition:
                return SubflowExecutionResult(
                    success=False,
                    output={},
                    execution_time_ms=0,
                    error=f"Subflow not found: {reference.subflow_id}",
                )

            # Map input variables
            subflow_input = self._map_variables(
                reference.input_mapping,
                parent_context,
            )

            # Create child context
            child_context = SubflowExecutionContext(
                parent_execution_id=execution_context.execution_id,
                parent_flow_id=execution_context.parent_flow_id,
                depth=execution_context.depth + 1,
                call_stack=execution_context.call_stack + [reference.subflow_id],
                variables=subflow_input,
                timeout_seconds=reference.timeout_seconds,
            )

            # Execute based on mode
            if reference.execution_mode == SubflowExecutionMode.ASYNC:
                # Fire and forget
                asyncio.create_task(
                    self._execute_flow(definition, child_context)
                )
                return SubflowExecutionResult(
                    success=True,
                    output={},
                    execution_time_ms=0,
                )

            # Sync execution with timeout
            result = await asyncio.wait_for(
                self._execute_flow(definition, child_context),
                timeout=reference.timeout_seconds,
            )

            # Map output variables
            mapped_output = self._map_variables(
                reference.output_mapping,
                result,
                reverse=True,
            )

            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return SubflowExecutionResult(
                success=True,
                output=mapped_output,
                execution_time_ms=execution_time,
            )

        except asyncio.TimeoutError:
            return SubflowExecutionResult(
                success=False,
                output={},
                execution_time_ms=reference.timeout_seconds * 1000,
                error=f"Subflow execution timeout after {reference.timeout_seconds}s",
            )

        except Exception as e:
            logger.exception(f"Subflow execution error: {e}")
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            return SubflowExecutionResult(
                success=False,
                output={},
                execution_time_ms=execution_time,
                error=str(e),
                error_details={"exception_type": type(e).__name__},
            )

    async def _execute_flow(
        self,
        definition: SubflowDefinition,
        context: SubflowExecutionContext,
    ) -> Dict[str, Any]:
        """Execute the flow definition"""
        if not self._flow_executor:
            raise RuntimeError("Flow executor not configured")

        return await self._flow_executor(
            definition.flow_definition,
            context.variables,
            execution_context=context,
        )

    def _map_variables(
        self,
        mapping: Dict[str, str],
        source: Dict[str, Any],
        reverse: bool = False,
    ) -> Dict[str, Any]:
        """
        Map variables using JSONPath-like expressions.

        Args:
            mapping: Variable mapping (target: source_path)
            source: Source data
            reverse: If True, source becomes target

        Returns:
            Mapped variables
        """
        result = {}

        for target, source_path in mapping.items():
            try:
                value = self._extract_value(source_path, source)
                result[target] = value
            except Exception as e:
                logger.warning(f"Variable mapping failed for {target}: {e}")
                result[target] = None

        return result

    def _extract_value(self, path: str, data: Dict[str, Any]) -> Any:
        """Extract value from data using JSONPath-like syntax"""
        if not path:
            return data

        # Simple path extraction ($.field.subfield)
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


# Global registry instance
_subflow_registry: Optional[SubflowRegistry] = None


def get_subflow_registry() -> SubflowRegistry:
    """Get the global subflow registry"""
    global _subflow_registry
    if _subflow_registry is None:
        _subflow_registry = SubflowRegistry()
    return _subflow_registry

"""
Multi-Agent Orchestration System

Manages collaboration between multiple AI agents with:
- Agent handoffs
- Parallel execution
- Result aggregation
- Conflict resolution
"""

import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Callable, Awaitable
from uuid import UUID, uuid4
from enum import Enum

logger = logging.getLogger(__name__)


class HandoffReason(str, Enum):
    """Reasons for agent handoff."""
    CAPABILITY_REQUIRED = "capability_required"
    USER_REQUEST = "user_request"
    TASK_COMPLETE = "task_complete"
    ERROR_ESCALATION = "error_escalation"
    APPROVAL_NEEDED = "approval_needed"


class AgentRole(str, Enum):
    """Agent roles in multi-agent system."""
    ORCHESTRATOR = "orchestrator"
    FLOW_BUILDER = "flow-builder"
    ADMIN = "admin"
    ANALYSIS = "analysis"
    QUERY = "query"
    SPECIALIST = "specialist"


@dataclass
class AgentHandoff:
    """Represents a handoff between agents."""
    id: UUID
    from_agent: str
    to_agent: str
    reason: HandoffReason
    context: Dict[str, Any]
    message: str
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "from_agent": self.from_agent,
            "to_agent": self.to_agent,
            "reason": self.reason.value,
            "context": self.context,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class AgentTask:
    """A task for an agent to execute."""
    id: UUID
    agent_type: str
    task_type: str
    input_data: Dict[str, Any]
    priority: int = 1
    timeout_seconds: int = 300
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    depends_on: List[UUID] = field(default_factory=list)


@dataclass
class AgentResult:
    """Result from an agent execution."""
    task_id: UUID
    agent_type: str
    success: bool
    output: Any
    error: Optional[str]
    tokens_used: int
    execution_time_ms: int
    handoff: Optional[AgentHandoff] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": str(self.task_id),
            "agent_type": self.agent_type,
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "tokens_used": self.tokens_used,
            "execution_time_ms": self.execution_time_ms,
            "handoff": self.handoff.to_dict() if self.handoff else None,
        }


# ============================================================================
# Agent Capabilities Registry
# ============================================================================

AGENT_CAPABILITIES = {
    AgentRole.FLOW_BUILDER: {
        "description": "Creates and modifies integration flows",
        "can_handle": [
            "create_flow", "update_flow", "validate_flow", "deploy_flow",
            "flow_design", "node_configuration", "edge_routing",
        ],
        "tools": [
            "create_flow", "get_flow", "update_flow", "validate_flow",
            "list_connectors", "deploy_flow",
        ],
    },
    AgentRole.ADMIN: {
        "description": "Manages API keys, users, and system configuration",
        "can_handle": [
            "api_key_management", "user_management", "connector_configuration",
            "permissions", "security", "access_control",
        ],
        "tools": [
            "create_api_key", "list_api_keys", "revoke_api_key",
            "invite_user", "list_users", "update_user_role",
            "create_connector", "test_connector",
        ],
    },
    AgentRole.ANALYSIS: {
        "description": "Parses and analyzes HL7/FHIR messages",
        "can_handle": [
            "hl7_parsing", "fhir_parsing", "message_validation",
            "performance_analysis", "error_diagnostics", "data_comparison",
        ],
        "tools": [
            "parse_hl7", "validate_hl7", "parse_fhir", "validate_fhir",
            "analyze_flow_performance", "get_error_summary", "compare_messages",
        ],
    },
    AgentRole.QUERY: {
        "description": "Searches and queries data, monitors CDC pipelines",
        "can_handle": [
            "message_search", "audit_log_query", "statistics",
            "trend_analysis", "compliance_reports",
            "cdc_monitoring", "cdc_statistics", "delivery_tracking",
            "schema_drift_detection", "cdc_health_check",
        ],
        "tools": [
            "search_messages", "get_message_details", "get_audit_logs",
            "get_phi_access_logs", "get_statistics", "get_trend_analysis",
            "list_cdc_monitors", "get_cdc_stats", "get_cdc_health",
            "list_cdc_deliveries", "list_cdc_schema_drifts",
        ],
    },
}


class MultiAgentOrchestrator:
    """
    Orchestrates multiple AI agents for complex tasks.

    Features:
    - Route tasks to appropriate agents
    - Handle agent handoffs
    - Execute tasks in parallel when possible
    - Aggregate results from multiple agents
    """

    def __init__(self, agent_executor: Callable[[AgentTask], Awaitable[AgentResult]] = None):
        self._executor = agent_executor
        self._task_queue: asyncio.Queue[AgentTask] = asyncio.Queue()
        self._results: Dict[UUID, AgentResult] = {}
        self._pending_handoffs: List[AgentHandoff] = []

    def find_best_agent(self, task_type: str) -> Optional[AgentRole]:
        """Find the best agent for a given task type."""
        for role, capabilities in AGENT_CAPABILITIES.items():
            if task_type in capabilities["can_handle"]:
                return role

        # Check if any tool matches
        for role, capabilities in AGENT_CAPABILITIES.items():
            if task_type in capabilities["tools"]:
                return role

        return None

    async def route_task(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        context: Dict[str, Any] = None,
    ) -> AgentRole:
        """Route a task to the appropriate agent."""
        agent = self.find_best_agent(task_type)

        if not agent:
            # Default to query agent for unknown tasks
            logger.warning(f"No agent found for task type: {task_type}, defaulting to query")
            agent = AgentRole.QUERY

        logger.info(
            f"Routed task to {agent.value}",
            extra={
                "event_type": "ai.task_routed",
                "task_type": task_type,
                "agent": agent.value,
            }
        )

        return agent

    async def execute_parallel(
        self,
        tasks: List[AgentTask],
        max_concurrent: int = 3,
    ) -> List[AgentResult]:
        """Execute multiple tasks in parallel."""
        semaphore = asyncio.Semaphore(max_concurrent)
        results = []

        async def execute_with_semaphore(task: AgentTask) -> AgentResult:
            async with semaphore:
                return await self._execute_task(task)

        # Execute all tasks concurrently
        coroutines = [execute_with_semaphore(task) for task in tasks]
        results = await asyncio.gather(*coroutines, return_exceptions=True)

        # Handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(AgentResult(
                    task_id=tasks[i].id,
                    agent_type=tasks[i].agent_type,
                    success=False,
                    output=None,
                    error=str(result),
                    tokens_used=0,
                    execution_time_ms=0,
                ))
            else:
                processed_results.append(result)

        return processed_results

    async def execute_with_dependencies(
        self,
        tasks: List[AgentTask],
    ) -> List[AgentResult]:
        """Execute tasks respecting dependencies."""
        results: Dict[UUID, AgentResult] = {}
        pending = {task.id: task for task in tasks}

        while pending:
            # Find tasks with satisfied dependencies
            ready = [
                task for task in pending.values()
                if all(dep in results for dep in task.depends_on)
            ]

            if not ready:
                # Circular dependency or unsatisfied deps
                for task in pending.values():
                    results[task.id] = AgentResult(
                        task_id=task.id,
                        agent_type=task.agent_type,
                        success=False,
                        output=None,
                        error="Unsatisfied dependencies",
                        tokens_used=0,
                        execution_time_ms=0,
                    )
                break

            # Execute ready tasks in parallel
            batch_results = await self.execute_parallel(ready)

            for result in batch_results:
                results[result.task_id] = result
                if result.task_id in pending:
                    del pending[result.task_id]

        return list(results.values())

    async def handle_handoff(
        self,
        from_agent: str,
        to_agent: str,
        reason: HandoffReason,
        context: Dict[str, Any],
        message: str,
    ) -> AgentHandoff:
        """Handle handoff between agents."""
        handoff = AgentHandoff(
            id=uuid4(),
            from_agent=from_agent,
            to_agent=to_agent,
            reason=reason,
            context=context,
            message=message,
            timestamp=datetime.now(timezone.utc),
        )

        self._pending_handoffs.append(handoff)

        logger.info(
            f"Agent handoff: {from_agent} -> {to_agent}",
            extra={
                "event_type": "ai.agent_handoff",
                "from_agent": from_agent,
                "to_agent": to_agent,
                "reason": reason.value,
            }
        )

        return handoff

    async def _execute_task(self, task: AgentTask) -> AgentResult:
        """Execute a single agent task."""
        start_time = datetime.now(timezone.utc)

        try:
            if self._executor:
                result = await self._executor(task)
                return result
            else:
                # Default mock execution
                return AgentResult(
                    task_id=task.id,
                    agent_type=task.agent_type,
                    success=True,
                    output={"status": "completed"},
                    error=None,
                    tokens_used=100,
                    execution_time_ms=int(
                        (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                    ),
                )

        except Exception as e:
            logger.exception(f"Task execution failed: {task.id}")
            return AgentResult(
                task_id=task.id,
                agent_type=task.agent_type,
                success=False,
                output=None,
                error=str(e),
                tokens_used=0,
                execution_time_ms=int(
                    (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                ),
            )

    def get_pending_handoffs(self) -> List[AgentHandoff]:
        """Get pending handoffs that need processing."""
        return self._pending_handoffs

    def clear_handoffs(self):
        """Clear processed handoffs."""
        self._pending_handoffs = []


# ============================================================================
# Agent Collaboration Patterns
# ============================================================================

class CollaborationPattern(str, Enum):
    """Common agent collaboration patterns."""
    SEQUENTIAL = "sequential"  # A -> B -> C
    PARALLEL = "parallel"  # A, B, C run together
    HIERARCHICAL = "hierarchical"  # Orchestrator delegates to workers
    ROUND_ROBIN = "round_robin"  # Distribute load
    SPECIALIST = "specialist"  # Route to specialized agent


@dataclass
class CollaborationPlan:
    """Plan for multi-agent collaboration."""
    id: UUID
    pattern: CollaborationPattern
    agents: List[str]
    tasks: List[AgentTask]
    estimated_time_seconds: int
    created_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "pattern": self.pattern.value,
            "agents": self.agents,
            "task_count": len(self.tasks),
            "estimated_time_seconds": self.estimated_time_seconds,
            "created_at": self.created_at.isoformat(),
        }


class CollaborationPlanner:
    """
    Plans multi-agent collaboration for complex tasks.

    Analyzes tasks and creates execution plans that optimize
    for speed, quality, and resource usage.
    """

    def create_plan(
        self,
        objective: str,
        requirements: List[str],
        constraints: Dict[str, Any] = None,
    ) -> CollaborationPlan:
        """Create a collaboration plan for an objective."""
        # Analyze requirements to determine agents needed
        agents_needed = set()
        tasks = []

        for req in requirements:
            agent = self._find_agent_for_requirement(req)
            if agent:
                agents_needed.add(agent.value)
                tasks.append(AgentTask(
                    id=uuid4(),
                    agent_type=agent.value,
                    task_type=req,
                    input_data={"requirement": req},
                ))

        # Determine pattern based on task relationships
        pattern = self._determine_pattern(tasks, constraints)

        # Add dependencies for sequential patterns
        if pattern == CollaborationPattern.SEQUENTIAL and len(tasks) > 1:
            for i in range(1, len(tasks)):
                tasks[i].depends_on = [tasks[i-1].id]

        plan = CollaborationPlan(
            id=uuid4(),
            pattern=pattern,
            agents=list(agents_needed),
            tasks=tasks,
            estimated_time_seconds=len(tasks) * 30,  # Rough estimate
            created_at=datetime.now(timezone.utc),
        )

        logger.info(
            f"Created collaboration plan",
            extra={
                "event_type": "ai.plan_created",
                "pattern": pattern.value,
                "agents": list(agents_needed),
                "task_count": len(tasks),
            }
        )

        return plan

    def _find_agent_for_requirement(self, requirement: str) -> Optional[AgentRole]:
        """Find the best agent for a requirement."""
        req_lower = requirement.lower()

        # Keywords to agent mapping
        keyword_map = {
            AgentRole.FLOW_BUILDER: ["flow", "node", "edge", "integration", "pipeline"],
            AgentRole.ADMIN: ["user", "api key", "permission", "access", "security"],
            AgentRole.ANALYSIS: ["hl7", "fhir", "parse", "validate", "analyze", "error"],
            AgentRole.QUERY: ["search", "query", "find", "list", "statistics", "audit"],
        }

        for agent, keywords in keyword_map.items():
            if any(kw in req_lower for kw in keywords):
                return agent

        return AgentRole.QUERY  # Default

    def _determine_pattern(
        self,
        tasks: List[AgentTask],
        constraints: Dict[str, Any] = None,
    ) -> CollaborationPattern:
        """Determine the best collaboration pattern."""
        if not tasks:
            return CollaborationPattern.SEQUENTIAL

        # Check for explicit constraint
        if constraints and "pattern" in constraints:
            return CollaborationPattern(constraints["pattern"])

        # If all tasks are independent, use parallel
        if len(set(t.agent_type for t in tasks)) == len(tasks):
            return CollaborationPattern.PARALLEL

        # If tasks require same agent, use sequential
        if len(set(t.agent_type for t in tasks)) == 1:
            return CollaborationPattern.SEQUENTIAL

        # Default to hierarchical for mixed agents
        return CollaborationPattern.HIERARCHICAL


# ============================================================================
# Singleton Factories
# ============================================================================

_orchestrator: Optional[MultiAgentOrchestrator] = None
_planner: Optional[CollaborationPlanner] = None


async def get_multi_agent_orchestrator() -> MultiAgentOrchestrator:
    """Get singleton instance of multi-agent orchestrator."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = MultiAgentOrchestrator()
    return _orchestrator


def get_collaboration_planner() -> CollaborationPlanner:
    """Get singleton instance of collaboration planner."""
    global _planner
    if _planner is None:
        _planner = CollaborationPlanner()
    return _planner

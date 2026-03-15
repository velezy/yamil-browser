"""
Async HTTP client for Evaluation Service (port 8033).

Usage:
    from assemblyline_common.evaluation_client import EvaluationClient

    client = EvaluationClient()
    run = await client.trigger_ragas(tenant_id, sample_size=50)
    results = await client.get_ragas_results(tenant_id)
    trajectory = await client.submit_trajectory(tenant_id, agent_name="rag", ...)
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

DEFAULT_EVALUATION_URL = "http://localhost:8033"


class EvaluationClient:
    """Async HTTP client for the Evaluation Service."""

    def __init__(self, base_url: str = None, timeout: float = 60.0):
        import os
        self.base_url = base_url or os.getenv("EVALUATION_SERVICE_URL", DEFAULT_EVALUATION_URL)
        self.timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)

    # ── RAGAS ────────────────────────────────────────────────────────────

    async def trigger_ragas(
        self, tenant_id: UUID, sample_size: int = 50, run_type: str = "manual"
    ) -> Dict[str, Any]:
        """Trigger a RAGAS evaluation run."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/eval/ragas/run",
                json={
                    "tenant_id": str(tenant_id),
                    "sample_size": sample_size,
                    "run_type": run_type,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_ragas_results(
        self, tenant_id: UUID, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get RAGAS evaluation results."""
        async with self._client() as client:
            resp = await client.get(
                "/api/v1/eval/ragas/results",
                params={"tenant_id": str(tenant_id), "limit": limit},
            )
            resp.raise_for_status()
            return resp.json().get("runs", [])

    async def get_ragas_run(self, run_id: UUID) -> Dict[str, Any]:
        """Get a specific RAGAS run with per-query results."""
        async with self._client() as client:
            resp = await client.get(f"/api/v1/eval/ragas/results/{run_id}")
            resp.raise_for_status()
            return resp.json()

    # ── Trajectories ─────────────────────────────────────────────────────

    async def submit_trajectory(
        self,
        tenant_id: UUID,
        agent_name: str,
        task_description: str,
        steps: List[Dict[str, Any]],
        optimal_steps: int = None,
        outcome: str = "unknown",
        latency_ms: int = None,
        token_count: int = None,
        cost_usd: float = None,
    ) -> Dict[str, Any]:
        """Submit an agent trajectory for scoring."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/eval/trajectories",
                json={
                    "tenant_id": str(tenant_id),
                    "agent_name": agent_name,
                    "task_description": task_description,
                    "steps": steps,
                    "optimal_steps": optimal_steps,
                    "outcome": outcome,
                    "latency_ms": latency_ms,
                    "token_count": token_count,
                    "cost_usd": cost_usd,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_trajectories(
        self, tenant_id: UUID, agent_name: str = None, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """List agent trajectories."""
        async with self._client() as client:
            params = {"tenant_id": str(tenant_id), "limit": limit}
            if agent_name:
                params["agent_name"] = agent_name
            resp = await client.get("/api/v1/eval/trajectories", params=params)
            resp.raise_for_status()
            return resp.json().get("trajectories", [])

    # ── A/B Testing ──────────────────────────────────────────────────────

    async def create_ab_test(
        self,
        tenant_id: UUID,
        name: str,
        variant_a: Dict,
        variant_b: Dict,
        metric_name: str,
        traffic_split: float = 0.5,
    ) -> Dict[str, Any]:
        """Create an A/B test."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/eval/ab-test",
                json={
                    "tenant_id": str(tenant_id),
                    "name": name,
                    "variant_a": variant_a,
                    "variant_b": variant_b,
                    "metric_name": metric_name,
                    "traffic_split": traffic_split,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def record_ab_result(
        self, test_id: UUID, tenant_id: UUID, variant: str, metric_value: float
    ) -> Dict[str, Any]:
        """Record an A/B test observation."""
        async with self._client() as client:
            resp = await client.post(
                f"/api/v1/eval/ab-test/{test_id}/result",
                json={
                    "tenant_id": str(tenant_id),
                    "variant": variant,
                    "metric_value": metric_value,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def analyze_ab_test(self, test_id: UUID) -> Dict[str, Any]:
        """Get statistical analysis of an A/B test."""
        async with self._client() as client:
            resp = await client.post(f"/api/v1/eval/ab-test/{test_id}/analyze")
            resp.raise_for_status()
            return resp.json()

    # ── Benchmarks ───────────────────────────────────────────────────────

    async def run_benchmark(
        self, tenant_id: UUID, benchmark_name: str, suite_config: Dict
    ) -> Dict[str, Any]:
        """Run a custom benchmark suite."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/eval/benchmarks/custom",
                json={
                    "tenant_id": str(tenant_id),
                    "benchmark_name": benchmark_name,
                    "suite_config": suite_config,
                },
            )
            resp.raise_for_status()
            return resp.json()

    # ── Regression ───────────────────────────────────────────────────────

    async def run_regression(
        self, suite_id: UUID, tenant_id: UUID
    ) -> Dict[str, Any]:
        """Run a regression test suite."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/eval/regression/run",
                json={"suite_id": str(suite_id), "tenant_id": str(tenant_id)},
            )
            resp.raise_for_status()
            return resp.json()

    # ── Health ───────────────────────────────────────────────────────────

    async def health(self) -> Dict[str, Any]:
        """Check service health."""
        async with self._client() as client:
            resp = await client.get("/health")
            resp.raise_for_status()
            return resp.json()

"""
Async HTTP client for Feedback Service (port 8035).

Usage:
    from assemblyline_common.feedback_client import FeedbackClient

    client = FeedbackClient()
    await client.thumbs_up(tenant_id, user_id, interaction_id="...")
    await client.record_retry(tenant_id, user_id, original_query="...", revised_query="...")
    reflection = await client.reflect(tenant_id, task_description="...", original_response="...")
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

DEFAULT_FEEDBACK_URL = "http://localhost:8035"


class FeedbackClient:
    """Async HTTP client for the Feedback Service."""

    def __init__(self, base_url: str = None, timeout: float = 60.0):
        import os
        self.base_url = base_url or os.getenv("FEEDBACK_SERVICE_URL", DEFAULT_FEEDBACK_URL)
        self.timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)

    # ── Explicit Feedback ────────────────────────────────────────────────

    async def submit_feedback(
        self,
        tenant_id: UUID,
        user_id: UUID,
        feedback_type: str,
        interaction_id: UUID = None,
        agent_name: str = None,
        rating: int = None,
        query_text: str = None,
        response_text: str = None,
        corrected_text: str = None,
        tags: List[str] = None,
    ) -> Dict[str, Any]:
        """Submit explicit feedback."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/feedback/explicit",
                json={
                    "tenant_id": str(tenant_id),
                    "user_id": str(user_id),
                    "feedback_type": feedback_type,
                    "interaction_id": str(interaction_id) if interaction_id else None,
                    "agent_name": agent_name,
                    "rating": rating,
                    "query_text": query_text,
                    "response_text": response_text,
                    "corrected_text": corrected_text,
                    "tags": tags or [],
                    "metadata": {},
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def thumbs_up(
        self, tenant_id: UUID, user_id: UUID, interaction_id: UUID = None, agent_name: str = None
    ) -> Dict[str, Any]:
        """Quick thumbs up."""
        return await self.submit_feedback(
            tenant_id, user_id, "thumbs_up", interaction_id=interaction_id, agent_name=agent_name
        )

    async def thumbs_down(
        self, tenant_id: UUID, user_id: UUID, interaction_id: UUID = None, agent_name: str = None
    ) -> Dict[str, Any]:
        """Quick thumbs down."""
        return await self.submit_feedback(
            tenant_id, user_id, "thumbs_down", interaction_id=interaction_id, agent_name=agent_name
        )

    async def submit_correction(
        self,
        tenant_id: UUID,
        user_id: UUID,
        query_text: str,
        response_text: str,
        corrected_text: str,
        interaction_id: UUID = None,
    ) -> Dict[str, Any]:
        """Submit a user correction."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/feedback/correction",
                json={
                    "tenant_id": str(tenant_id),
                    "user_id": str(user_id),
                    "interaction_id": str(interaction_id) if interaction_id else None,
                    "query_text": query_text,
                    "response_text": response_text,
                    "corrected_text": corrected_text,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_quality_score(
        self, tenant_id: UUID, interaction_id: UUID
    ) -> Optional[Dict[str, Any]]:
        """Get aggregated quality score for an interaction."""
        async with self._client() as client:
            resp = await client.get(
                f"/api/v1/feedback/quality/{interaction_id}",
                params={"tenant_id": str(tenant_id)},
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    # ── Implicit Feedback ────────────────────────────────────────────────

    async def record_implicit(
        self,
        tenant_id: UUID,
        user_id: UUID,
        signal_type: str,
        signal_value: float = None,
        interaction_id: UUID = None,
        agent_name: str = None,
        original_query: str = None,
        revised_query: str = None,
    ) -> Dict[str, Any]:
        """Record an implicit feedback signal."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/feedback/implicit",
                json={
                    "tenant_id": str(tenant_id),
                    "user_id": str(user_id),
                    "signal_type": signal_type,
                    "signal_value": signal_value,
                    "interaction_id": str(interaction_id) if interaction_id else None,
                    "agent_name": agent_name,
                    "original_query": original_query,
                    "revised_query": revised_query,
                    "metadata": {},
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def record_retry(
        self, tenant_id: UUID, user_id: UUID, original_query: str, revised_query: str = None, interaction_id: UUID = None
    ) -> Dict[str, Any]:
        """Record a user retry."""
        return await self.record_implicit(
            tenant_id, user_id, "retry",
            original_query=original_query, revised_query=revised_query, interaction_id=interaction_id
        )

    # ── Preferences ──────────────────────────────────────────────────────

    async def rank_preference(
        self,
        tenant_id: UUID,
        query_text: str,
        response_a: str,
        response_b: str,
        winner: str,
        model_a: str = None,
        model_b: str = None,
        user_id: UUID = None,
    ) -> Dict[str, Any]:
        """Submit a preference ranking (A > B or B > A)."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/feedback/preferences/rank",
                json={
                    "tenant_id": str(tenant_id),
                    "user_id": str(user_id) if user_id else None,
                    "query_text": query_text,
                    "response_a": response_a,
                    "response_b": response_b,
                    "model_a": model_a,
                    "model_b": model_b,
                    "winner": winner,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_leaderboard(self, tenant_id: UUID) -> List[Dict[str, Any]]:
        """Get model Elo leaderboard."""
        async with self._client() as client:
            resp = await client.get(
                "/api/v1/feedback/preferences/leaderboard",
                params={"tenant_id": str(tenant_id)},
            )
            resp.raise_for_status()
            return resp.json().get("leaderboard", [])

    # ── Reflection ───────────────────────────────────────────────────────

    async def reflect(
        self,
        tenant_id: UUID,
        task_description: str,
        original_response: str = None,
        agent_name: str = None,
        interaction_id: UUID = None,
    ) -> Dict[str, Any]:
        """Trigger self-critique on a task."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/feedback/reflection",
                json={
                    "tenant_id": str(tenant_id),
                    "task_description": task_description,
                    "original_response": original_response,
                    "agent_name": agent_name,
                    "interaction_id": str(interaction_id) if interaction_id else None,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_patterns(self, tenant_id: UUID) -> List[Dict[str, Any]]:
        """Get detected reflection patterns."""
        async with self._client() as client:
            resp = await client.get(
                "/api/v1/feedback/reflection/patterns",
                params={"tenant_id": str(tenant_id)},
            )
            resp.raise_for_status()
            return resp.json().get("patterns", [])

    # ── Reward Model ─────────────────────────────────────────────────────

    async def trigger_training(
        self,
        tenant_id: UUID,
        model_name: str,
        training_type: str = "dpo",
        training_config: Dict = None,
    ) -> Dict[str, Any]:
        """Trigger reward model training."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/feedback/reward-model/train",
                json={
                    "tenant_id": str(tenant_id),
                    "model_name": model_name,
                    "training_type": training_type,
                    "training_config": training_config or {},
                },
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

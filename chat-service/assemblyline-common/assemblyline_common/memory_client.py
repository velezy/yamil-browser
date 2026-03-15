"""
Async HTTP client for Memory Service (port 8032).

Usage:
    from assemblyline_common.memory_client import MemoryClient

    client = MemoryClient()
    episode = await client.record_episode(tenant_id, summary="User asked about auth", outcome="success")
    results = await client.search_episodes(tenant_id, query="authentication")
    await client.set_session(session_id, {"context": "working on auth"})
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

DEFAULT_MEMORY_URL = "http://localhost:8032"


class MemoryClient:
    """Async HTTP client for the Memory Service."""

    def __init__(self, base_url: str = None, timeout: float = 30.0):
        import os
        self.base_url = base_url or os.getenv("MEMORY_SERVICE_URL", DEFAULT_MEMORY_URL)
        self.timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)

    # ── Episodic Memory ──────────────────────────────────────────────────

    async def record_episode(
        self,
        tenant_id: UUID,
        summary: str,
        user_id: UUID = None,
        session_id: UUID = None,
        agent_name: str = None,
        category: str = "general",
        details: Dict[str, Any] = None,
        outcome: str = "neutral",
        tags: List[str] = None,
    ) -> Dict[str, Any]:
        """Record a new episode."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/episodic",
                json={
                    "tenant_id": str(tenant_id),
                    "summary": summary,
                    "user_id": str(user_id) if user_id else None,
                    "session_id": str(session_id) if session_id else None,
                    "agent_name": agent_name,
                    "category": category,
                    "details": details or {},
                    "outcome": outcome,
                    "tags": tags or [],
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def search_episodes(
        self,
        tenant_id: UUID,
        query: str,
        agent_name: str = None,
        category: str = None,
        outcome: str = None,
        memory_tier: str = None,
        limit: int = 10,
        min_similarity: float = 0.3,
    ) -> List[Dict[str, Any]]:
        """Search episodes by semantic similarity or text match."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/episodic/search",
                json={
                    "tenant_id": str(tenant_id),
                    "query": query,
                    "agent_name": agent_name,
                    "category": category,
                    "outcome": outcome,
                    "memory_tier": memory_tier,
                    "limit": limit,
                    "min_similarity": min_similarity,
                },
            )
            resp.raise_for_status()
            return resp.json().get("episodes", [])

    async def get_episode(self, episode_id: UUID) -> Optional[Dict[str, Any]]:
        """Get a single episode by ID."""
        async with self._client() as client:
            resp = await client.get(f"/api/v1/memory/episodic/{episode_id}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    # ── Procedural Memory ────────────────────────────────────────────────

    async def save_procedure(
        self,
        tenant_id: UUID,
        title: str,
        description: str,
        steps: List[Dict[str, Any]],
        triggers: List[str] = None,
        applies_to: List[str] = None,
        tags: List[str] = None,
        source: str = "learned",
    ) -> Dict[str, Any]:
        """Save a learned procedure."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/procedural",
                json={
                    "tenant_id": str(tenant_id),
                    "title": title,
                    "description": description,
                    "steps": steps,
                    "triggers": triggers or [],
                    "applies_to": applies_to or [],
                    "tags": tags or [],
                    "source": source,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def search_procedures(
        self,
        tenant_id: UUID,
        query: str,
        limit: int = 5,
        min_similarity: float = 0.3,
    ) -> List[Dict[str, Any]]:
        """Search procedures."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/procedural/search",
                json={
                    "tenant_id": str(tenant_id),
                    "query": query,
                    "limit": limit,
                    "min_similarity": min_similarity,
                },
            )
            resp.raise_for_status()
            return resp.json().get("procedures", [])

    async def record_execution(
        self, procedure_id: UUID, success: bool = True
    ) -> Dict[str, Any]:
        """Record procedure execution result."""
        async with self._client() as client:
            resp = await client.post(
                f"/api/v1/memory/procedural/{procedure_id}/execute",
                json={"success": success},
            )
            resp.raise_for_status()
            return resp.json()

    # ── Semantic Memory ──────────────────────────────────────────────────

    async def store_fact(
        self,
        tenant_id: UUID,
        subject: str,
        predicate: str,
        object_val: str,
        confidence: float = 0.8,
        source: str = None,
    ) -> Dict[str, Any]:
        """Store a semantic fact."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/semantic",
                json={
                    "tenant_id": str(tenant_id),
                    "subject": subject,
                    "predicate": predicate,
                    "object": object_val,
                    "confidence": confidence,
                    "source": source,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def search_facts(
        self, tenant_id: UUID, query: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Search semantic facts."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/semantic/search",
                json={"tenant_id": str(tenant_id), "query": query, "limit": limit},
            )
            resp.raise_for_status()
            return resp.json().get("facts", [])

    # ── Short-Term Memory ────────────────────────────────────────────────

    async def set_session(
        self, session_id: UUID, data: Dict[str, Any], ttl: int = None
    ):
        """Set session context."""
        async with self._client() as client:
            body = {"data": data}
            if ttl:
                body["ttl"] = ttl
            resp = await client.post(
                f"/api/v1/memory/short-term/{session_id}", json=body
            )
            resp.raise_for_status()

    async def get_session(self, session_id: UUID) -> Optional[Dict[str, Any]]:
        """Get session context."""
        async with self._client() as client:
            resp = await client.get(f"/api/v1/memory/short-term/{session_id}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json().get("data")

    async def update_session(self, session_id: UUID, updates: Dict[str, Any]):
        """Merge updates into session context."""
        async with self._client() as client:
            resp = await client.patch(
                f"/api/v1/memory/short-term/{session_id}",
                json={"updates": updates},
            )
            resp.raise_for_status()

    # ── Consolidation ────────────────────────────────────────────────────

    async def consolidate(self, tenant_id: UUID) -> Dict[str, Any]:
        """Trigger memory consolidation."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/consolidate",
                json={"tenant_id": str(tenant_id)},
            )
            resp.raise_for_status()
            return resp.json()

    async def get_stats(self, tenant_id: UUID) -> Dict[str, Any]:
        """Get memory statistics."""
        async with self._client() as client:
            resp = await client.get(
                "/api/v1/memory/consolidate/stats",
                params={"tenant_id": str(tenant_id)},
            )
            resp.raise_for_status()
            return resp.json()

    # ── Preferences ──────────────────────────────────────────────────────

    async def record_preference(
        self,
        tenant_id: UUID,
        user_id: UUID,
        preference_key: str,
        preference_value: Any,
        source: str = "inferred",
    ) -> Dict[str, Any]:
        """Record a user preference."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/memory/preferences",
                json={
                    "tenant_id": str(tenant_id),
                    "user_id": str(user_id),
                    "preference_key": preference_key,
                    "preference_value": preference_value,
                    "source": source,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_preferences(
        self, tenant_id: UUID, user_id: UUID
    ) -> List[Dict[str, Any]]:
        """Get user preferences."""
        async with self._client() as client:
            resp = await client.get(
                "/api/v1/memory/preferences",
                params={"tenant_id": str(tenant_id), "user_id": str(user_id)},
            )
            resp.raise_for_status()
            return resp.json().get("preferences", [])

    # ── Health ───────────────────────────────────────────────────────────

    async def health(self) -> Dict[str, Any]:
        """Check service health."""
        async with self._client() as client:
            resp = await client.get("/health")
            resp.raise_for_status()
            return resp.json()

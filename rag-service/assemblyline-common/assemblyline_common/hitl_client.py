"""
Async HTTP client for HITL Service (port 8034).

Usage:
    from assemblyline_common.hitl_client import HITLClient

    client = HITLClient()
    request = await client.submit_for_approval(tenant_id, agent_name="rag", action_type="delete", ...)
    result = await client.approve(request_id, reviewer_id)
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

DEFAULT_HITL_URL = "http://localhost:8034"


class HITLClient:
    """Async HTTP client for the HITL (Human-in-the-Loop) Service."""

    def __init__(self, base_url: str = None, timeout: float = 30.0):
        import os
        self.base_url = base_url or os.getenv("HITL_SERVICE_URL", DEFAULT_HITL_URL)
        self.timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)

    # ── Approvals ────────────────────────────────────────────────────────

    async def submit_for_approval(
        self,
        tenant_id: UUID,
        agent_name: str,
        action_type: str,
        action_summary: str,
        action_payload: Dict[str, Any] = None,
        priority: str = "medium",
        confidence_score: float = None,
        risk_level: str = "medium",
        queue_name: str = "default",
        compliance_flags: List[str] = None,
    ) -> Dict[str, Any]:
        """Submit an action for human approval."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/hitl/approvals",
                json={
                    "tenant_id": str(tenant_id),
                    "agent_name": agent_name,
                    "action_type": action_type,
                    "action_summary": action_summary,
                    "action_payload": action_payload or {},
                    "priority": priority,
                    "confidence_score": confidence_score,
                    "risk_level": risk_level,
                    "queue_name": queue_name,
                    "compliance_flags": compliance_flags or [],
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def get_approval(self, request_id: UUID) -> Optional[Dict[str, Any]]:
        """Get approval request details."""
        async with self._client() as client:
            resp = await client.get(f"/api/v1/hitl/approvals/{request_id}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    async def approve(
        self, request_id: UUID, reviewer_id: UUID, notes: str = None
    ) -> Dict[str, Any]:
        """Approve a request."""
        async with self._client() as client:
            body = {"reviewer_id": str(reviewer_id)}
            if notes:
                body["reviewer_notes"] = notes
            resp = await client.post(
                f"/api/v1/hitl/approvals/{request_id}/approve", json=body
            )
            resp.raise_for_status()
            return resp.json()

    async def reject(
        self, request_id: UUID, reviewer_id: UUID, reason: str, notes: str = None
    ) -> Dict[str, Any]:
        """Reject a request."""
        async with self._client() as client:
            body = {"reviewer_id": str(reviewer_id), "reason": reason}
            if notes:
                body["reviewer_notes"] = notes
            resp = await client.post(
                f"/api/v1/hitl/approvals/{request_id}/reject", json=body
            )
            resp.raise_for_status()
            return resp.json()

    async def batch_approve(
        self, ids: List[UUID], reviewer_id: UUID, notes: str = None
    ) -> Dict[str, Any]:
        """Batch approve multiple requests."""
        async with self._client() as client:
            body = {
                "ids": [str(i) for i in ids],
                "reviewer_id": str(reviewer_id),
            }
            if notes:
                body["reviewer_notes"] = notes
            resp = await client.post(
                "/api/v1/hitl/approvals/batch-approve", json=body
            )
            resp.raise_for_status()
            return resp.json()

    # ── Queues ───────────────────────────────────────────────────────────

    async def get_queue(
        self, tenant_id: UUID, queue_name: str = "default", priority: str = None, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get pending reviews in a queue."""
        async with self._client() as client:
            params = {"tenant_id": str(tenant_id), "limit": limit}
            if priority:
                params["priority"] = priority
            resp = await client.get(f"/api/v1/hitl/queues/{queue_name}", params=params)
            resp.raise_for_status()
            return resp.json().get("items", [])

    async def get_queue_stats(self, tenant_id: UUID) -> Dict[str, Any]:
        """Get queue statistics."""
        async with self._client() as client:
            resp = await client.get(
                "/api/v1/hitl/queues/stats",
                params={"tenant_id": str(tenant_id)},
            )
            resp.raise_for_status()
            return resp.json()

    # ── Escalation ───────────────────────────────────────────────────────

    async def escalate(
        self, request_id: UUID, reason: str, escalate_to: str
    ) -> Dict[str, Any]:
        """Manually escalate a request."""
        async with self._client() as client:
            resp = await client.post(
                f"/api/v1/hitl/escalate/{request_id}",
                json={"reason": reason, "escalate_to": escalate_to},
            )
            resp.raise_for_status()
            return resp.json()

    # ── Policies ─────────────────────────────────────────────────────────

    async def create_policy(
        self,
        tenant_id: UUID,
        name: str,
        conditions: Dict[str, Any],
        description: str = None,
    ) -> Dict[str, Any]:
        """Create an auto-approval policy."""
        async with self._client() as client:
            resp = await client.post(
                "/api/v1/hitl/policies",
                json={
                    "tenant_id": str(tenant_id),
                    "name": name,
                    "description": description,
                    "conditions": conditions,
                },
            )
            resp.raise_for_status()
            return resp.json()

    # ── Compliance ───────────────────────────────────────────────────────

    async def get_compliance_report(
        self, tenant_id: UUID, start_date: str = None, end_date: str = None
    ) -> Dict[str, Any]:
        """Generate EU AI Act compliance report."""
        async with self._client() as client:
            params = {"tenant_id": str(tenant_id)}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            resp = await client.get("/api/v1/hitl/compliance/report", params=params)
            resp.raise_for_status()
            return resp.json()

    # ── Convenience: Check-and-Wait ──────────────────────────────────────

    async def require_approval(
        self,
        tenant_id: UUID,
        agent_name: str,
        action_type: str,
        action_summary: str,
        confidence_score: float = None,
        risk_level: str = "medium",
    ) -> Dict[str, Any]:
        """
        Submit for approval and return immediately.
        The caller should poll get_approval() to check status.
        Returns the created approval request.
        """
        return await self.submit_for_approval(
            tenant_id=tenant_id,
            agent_name=agent_name,
            action_type=action_type,
            action_summary=action_summary,
            confidence_score=confidence_score,
            risk_level=risk_level,
        )

    # ── Health ───────────────────────────────────────────────────────────

    async def health(self) -> Dict[str, Any]:
        """Check service health."""
        async with self._client() as client:
            resp = await client.get("/health")
            resp.raise_for_status()
            return resp.json()

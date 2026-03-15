"""
CDC Tools

Tools for managing CDC monitors, subscribers, deliveries, schemas,
and health via the CDC Hub Service (port 8016).
"""

import logging
from typing import Optional, Dict, List, Any

from assemblyline_common.ai.tools.base import Tool, ToolResult, ToolDefinition
from assemblyline_common.ai.authorization import AuthorizationContext, Permission

logger = logging.getLogger(__name__)

CDC_HUB_BASE = "http://cdc-hub:8016"


# ============================================================================
# CDC Monitor Tools
# ============================================================================

class ListCDCMonitorsTool(Tool):
    """List all CDC monitors with status, table info, and subscriber counts."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="list_cdc_monitors",
            description="List all CDC monitors showing table FQN, connector, poll interval, status, last version, rows, and subscriber count",
            parameters={
                "type": "object",
                "properties": {
                    "is_active": {"type": "boolean", "description": "Filter by active status"},
                    "is_paused": {"type": "boolean", "description": "Filter by paused status"},
                    "connector_id": {"type": "string", "description": "Filter by connector UUID"},
                },
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        is_active: Optional[bool] = None,
        is_paused: Optional[bool] = None,
        connector_id: Optional[str] = None,
        **kwargs,
    ) -> ToolResult:
        import httpx
        try:
            params: Dict[str, Any] = {}
            if is_active is not None:
                params["is_active"] = is_active
            if is_paused is not None:
                params["is_paused"] = is_paused
            if connector_id:
                params["connector_id"] = connector_id

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{CDC_HUB_BASE}/api/v1/cdc/monitors", params=params)
                resp.raise_for_status()
                data = resp.json()

            monitors = data.get("monitors", [])
            total = data.get("total", len(monitors))
            lines = [f"**{total} CDC monitor(s)**\n"]
            for m in monitors:
                status = "Active" if not m.get("is_paused") else "Paused"
                if m.get("consecutive_errors", 0) > 0:
                    status = f"Error ({m['consecutive_errors']})"
                lines.append(
                    f"- **{m['table_fqn']}** | {m.get('connector_name', '?')} | "
                    f"every {m.get('poll_interval_seconds', '?')}s | {status} | "
                    f"v{m.get('last_version', '—')} | {m.get('rows_total', 0)} rows | "
                    f"{m.get('subscriber_count', 0)} subs | ID: {m['id']}"
                )
            return ToolResult(success=True, data="\n".join(lines))
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to list CDC monitors: {e}")


class GetCDCStatsTool(Tool):
    """Get CDC Hub dashboard statistics."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_cdc_stats",
            description="Get CDC Hub stats: active/paused/error monitors, delivery counts, delivery engine metrics (circuit breaker, rate limiter, exactly-once dedup), coordinator queue depth",
            parameters={"type": "object", "properties": {}},
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(self, auth_context: AuthorizationContext, **kwargs) -> ToolResult:
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{CDC_HUB_BASE}/api/v1/cdc/stats")
                resp.raise_for_status()
                data = resp.json()

            m = data.get("monitors", {})
            d = data.get("deliveries_last_hour", {})
            de = data.get("delivery_engine", {})
            c = data.get("coordinator", {})
            cb = de.get("circuit_breakers", {})

            lines = [
                "**CDC Hub Stats**",
                f"Monitors: {m.get('active', 0)} active, {m.get('paused', 0)} paused, {m.get('total', 0)} total",
                f"Deliveries (last hour): {d.get('total', 0)} total, {d.get('delivered', 0)} delivered, {d.get('failed', 0)} failed, {d.get('dead_letter', 0)} dead letter",
                f"Delivery Engine: {de.get('total_delivered', 0)} delivered, {de.get('total_deduped', 0)} deduped (exactly-once), {de.get('total_circuit_broken', 0)} circuit broken, {de.get('total_rate_limited', 0)} rate limited",
                f"Circuit Breakers: {cb.get('closed', 0)} closed, {cb.get('open', 0)} open, {cb.get('half_open', 0)} half-open",
                f"Coordinator: {c.get('worker_count', 0)} workers, queue depth {c.get('queue_depth', 0)}, {c.get('warehouse_concurrency', 0)} warehouse concurrency",
            ]
            return ToolResult(success=True, data="\n".join(lines))
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to get CDC stats: {e}")


class GetCDCHealthTool(Tool):
    """Get CDC Hub health status."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_cdc_health",
            description="Get CDC Hub health: Redis connection, worker count, coordinator running, leader election status",
            parameters={"type": "object", "properties": {}},
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(self, auth_context: AuthorizationContext, **kwargs) -> ToolResult:
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{CDC_HUB_BASE}/api/v1/cdc/health")
                resp.raise_for_status()
                data = resp.json()

            lines = [
                "**CDC Hub Health**",
                f"Status: {data.get('status', 'unknown')}",
                f"Redis: {data.get('redis', 'unknown')}",
                f"Workers: {data.get('workers', 0)}",
                f"Coordinator: {'Running' if data.get('running') else 'Stopped'}",
                f"Leader: {'This instance' if data.get('is_leader') else 'Follower'}",
            ]
            return ToolResult(success=True, data="\n".join(lines))
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to get CDC health: {e}")


class ListCDCDeliveriesTool(Tool):
    """List recent CDC delivery events."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="list_cdc_deliveries",
            description="List recent CDC delivery events with status, subscriber, table, version range, HTTP status, and duration. Filter by status (delivered/failed/dead_letter/retrying)",
            parameters={
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["delivered", "failed", "dead_letter", "retrying"], "description": "Filter by delivery status"},
                    "subscriber_id": {"type": "string", "description": "Filter by subscriber UUID"},
                    "table_fqn": {"type": "string", "description": "Filter by table FQN"},
                    "limit": {"type": "integer", "description": "Max results (1-100)", "default": 20},
                },
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        status: Optional[str] = None,
        subscriber_id: Optional[str] = None,
        table_fqn: Optional[str] = None,
        limit: int = 20,
        **kwargs,
    ) -> ToolResult:
        import httpx
        try:
            params: Dict[str, Any] = {"page_size": min(max(limit, 1), 100)}
            if status:
                params["status"] = status
            if subscriber_id:
                params["subscriber_id"] = subscriber_id
            if table_fqn:
                params["table_fqn"] = table_fqn

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{CDC_HUB_BASE}/api/v1/cdc/deliveries", params=params)
                resp.raise_for_status()
                data = resp.json()

            deliveries = data.get("deliveries", [])
            lines = [f"**{data.get('total', len(deliveries))} delivery record(s)**\n"]
            for d in deliveries[:limit]:
                dur = f"{d.get('delivery_ms', '?')}ms" if d.get("delivery_ms") else "—"
                http = d.get("http_status") or "—"
                lines.append(
                    f"- [{d['status']}] {d.get('subscriber_name', '?')} → {d.get('table_fqn', '?')} "
                    f"v{d.get('from_version', '?')}→{d.get('to_version', '?')} | "
                    f"{d.get('change_count', 0)} changes | HTTP {http} | {dur} | "
                    f"{d.get('created_at', '?')} | ID: {d['id']}"
                )
            return ToolResult(success=True, data="\n".join(lines))
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to list CDC deliveries: {e}")


class ListCDCSchemaDriftsTool(Tool):
    """List schema drift events from the CDC schema registry."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="list_cdc_schema_drifts",
            description="List schema drift events detected by the CDC schema registry. Shows column changes (added/removed/changed) between Delta table versions",
            parameters={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max results", "default": 20},
                },
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(self, auth_context: AuthorizationContext, limit: int = 20, **kwargs) -> ToolResult:
        import httpx
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{CDC_HUB_BASE}/api/v1/cdc/schemas", params={"limit": limit})
                resp.raise_for_status()
                data = resp.json()

            drifts = data.get("drifts", [])
            if not drifts:
                return ToolResult(success=True, data="**No schema drifts detected.** All table schemas are stable.")

            lines = [f"**{data.get('total', len(drifts))} schema event(s)**\n"]
            for d in drifts:
                drift = d.get("drift_details")
                if drift:
                    lines.append(
                        f"- **{d['table_fqn']}** v{d.get('version', '?')} | {drift.get('summary', '')} | "
                        f"{d.get('column_count', '?')} cols | {d.get('detected_at', '?')}"
                    )
                else:
                    lines.append(
                        f"- **{d['table_fqn']}** v{d.get('version', '?')} | Initial capture | "
                        f"{d.get('column_count', '?')} cols"
                    )
            return ToolResult(success=True, data="\n".join(lines))
        except Exception as e:
            return ToolResult(success=False, error=f"Failed to list schema drifts: {e}")


# ============================================================================
# Public API
# ============================================================================

class CDCTools:
    """All CDC-related tools for the AI assistant."""

    @staticmethod
    def get_all_tools() -> list:
        return [
            ListCDCMonitorsTool(),
            GetCDCStatsTool(),
            GetCDCHealthTool(),
            ListCDCDeliveriesTool(),
            ListCDCSchemaDriftsTool(),
        ]

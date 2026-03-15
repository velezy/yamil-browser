"""
AI Flow Monitor — Background agent that monitors flow executions for failures.

Detects persistent failures, classifies root causes, escalates via notifications,
and auto-creates Jira tickets for critical recurring issues.

Uses APScheduler (same pattern as FlowScheduler) — no Celery/broker dependency.

Escalation ladder:
  1 failure (transient): log only
  2-3 failures: insert notification into common.notifications
  4+ consecutive failures: create Jira ticket via JiraTool
  Deduplication: no duplicate Jira tickets within 24 hours per flow
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import text

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False
    logger.warning("apscheduler not installed. FlowMonitor will not run.")


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------

TRANSIENT_PATTERNS = [
    "timeout", "timed out", "rate_limit", "rate limit", "429",
    "503", "504", "connection reset", "temporary",
]

PERSISTENT_PATTERNS = [
    "401", "403", "unauthorized", "forbidden",
    "circuitopenerror", "circuit open", "circuit breaker",
    "connection refused", "name resolution", "dns",
    "invalid credentials", "authentication failed",
]


def classify_failure(error_message: str) -> str:
    """Classify a failure as 'transient' or 'persistent'."""
    if not error_message:
        return "persistent"
    lower = error_message.lower()
    for pattern in TRANSIENT_PATTERNS:
        if pattern in lower:
            return "transient"
    for pattern in PERSISTENT_PATTERNS:
        if pattern in lower:
            return "persistent"
    return "persistent"  # default to persistent for unknown errors


# ---------------------------------------------------------------------------
# FlowMonitor
# ---------------------------------------------------------------------------

class FlowMonitor:
    """Background monitor that watches flow executions for failures and escalates.

    Args:
        db_func: async callable(schema) returning an async DB session context manager
        check_interval_minutes: how often to check for failures (default 5)
        jira_project_key: Jira project for auto-created tickets (default "DAT")
    """

    def __init__(
        self,
        db_func,
        check_interval_minutes: int = 5,
        jira_project_key: str = "DAT",
    ):
        self._db_func = db_func
        self._check_interval = check_interval_minutes
        self._jira_project_key = jira_project_key
        self._scheduler: Optional[AsyncIOScheduler] = None

        # Tracking state
        self._last_check: datetime = datetime.now(timezone.utc) - timedelta(minutes=check_interval_minutes)
        self._failure_counts: Dict[str, int] = defaultdict(int)  # flow_id -> consecutive failure count
        self._last_jira_ticket: Dict[str, datetime] = {}  # flow_id -> last Jira ticket timestamp
        self._flow_names: Dict[str, str] = {}  # flow_id -> flow name cache

    async def start(self):
        """Initialize and start the monitor scheduler."""
        if not HAS_APSCHEDULER:
            logger.warning("APScheduler not available — FlowMonitor disabled")
            return

        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._scheduler.add_listener(self._on_job_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

        self._scheduler.add_job(
            self._check_failures,
            trigger=IntervalTrigger(minutes=self._check_interval),
            id="flow_monitor_check",
            name="FlowMonitor: check for failures",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.info(f"FlowMonitor started (checking every {self._check_interval} minutes)")

    async def stop(self):
        """Shutdown the monitor."""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("FlowMonitor stopped")

    def _on_job_event(self, event):
        """Handle APScheduler job events."""
        if hasattr(event, "exception") and event.exception:
            logger.error(f"FlowMonitor job failed: {event.exception}")

    # ------------------------------------------------------------------
    # Core check loop
    # ------------------------------------------------------------------

    async def _check_failures(self):
        """Query for recent failures and escalate as needed."""
        check_since = self._last_check
        now = datetime.now(timezone.utc)
        self._last_check = now

        try:
            async with self._db_func("common") as db:
                # Get failed executions since last check
                result = await db.execute(
                    text("""
                        SELECT
                            fe.id, fe.flow_id, fe.tenant_id, fe.status,
                            fe.error_message, fe.error_step_id, fe.started_at,
                            f.name as flow_name
                        FROM common.flow_executions fe
                        JOIN common.logic_weaver_flows f ON f.id = fe.flow_id
                        WHERE fe.status = 'failed'
                          AND fe.started_at > :since
                        ORDER BY fe.started_at ASC
                    """),
                    {"since": check_since},
                )
                failures = result.fetchall()

                if not failures:
                    # Reset counters for flows that succeeded (no failures in this window)
                    # Query for successful executions to clear counters
                    success_result = await db.execute(
                        text("""
                            SELECT DISTINCT flow_id::text
                            FROM common.flow_executions
                            WHERE status = 'completed'
                              AND started_at > :since
                        """),
                        {"since": check_since},
                    )
                    for row in success_result.fetchall():
                        flow_id = row[0]
                        if flow_id in self._failure_counts:
                            logger.info(f"[FlowMonitor] Flow {flow_id} recovered — resetting failure count")
                            self._failure_counts[flow_id] = 0
                    return

                logger.info(f"[FlowMonitor] Found {len(failures)} failed execution(s) since {check_since.isoformat()}")

                # Process each failure
                for row in failures:
                    flow_id = str(row.flow_id)
                    tenant_id = str(row.tenant_id)
                    error_message = row.error_message or ""
                    flow_name = row.flow_name or flow_id
                    self._flow_names[flow_id] = flow_name

                    # Classify and increment
                    failure_type = classify_failure(error_message)
                    self._failure_counts[flow_id] += 1
                    count = self._failure_counts[flow_id]

                    logger.info(
                        f"[FlowMonitor] Flow '{flow_name}' ({flow_id}) — "
                        f"failure #{count} ({failure_type}): {error_message[:100]}"
                    )

                    # Escalation ladder
                    if count == 1 and failure_type == "transient":
                        # Log only
                        continue

                    if count >= 2:
                        await self._create_notification(
                            db, tenant_id, flow_id, flow_name, count, error_message, failure_type
                        )

                    if count >= 4:
                        await self._create_jira_ticket(
                            db, tenant_id, flow_id, flow_name, count, error_message, failure_type
                        )

                # Also reset counters for flows that had successes in this window
                success_result = await db.execute(
                    text("""
                        SELECT DISTINCT flow_id::text
                        FROM common.flow_executions
                        WHERE status = 'completed'
                          AND started_at > :since
                    """),
                    {"since": check_since},
                )
                for row in success_result.fetchall():
                    flow_id = row[0]
                    if flow_id in self._failure_counts and self._failure_counts[flow_id] > 0:
                        logger.info(f"[FlowMonitor] Flow {flow_id} had success — resetting failure count")
                        self._failure_counts[flow_id] = 0

        except Exception as e:
            logger.error(f"[FlowMonitor] Check failed: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Notification
    # ------------------------------------------------------------------

    async def _create_notification(
        self, db, tenant_id: str, flow_id: str, flow_name: str,
        count: int, error_message: str, failure_type: str,
    ):
        """Insert a notification into common.notifications."""
        try:
            # Ensure table exists (idempotent)
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS common.notifications (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    tenant_id UUID NOT NULL,
                    type VARCHAR(50) NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    message TEXT,
                    metadata JSONB DEFAULT '{}',
                    is_read BOOLEAN DEFAULT false,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    read_at TIMESTAMPTZ
                )
            """))

            import json
            await db.execute(
                text("""
                    INSERT INTO common.notifications (tenant_id, type, title, message, metadata)
                    VALUES (:tid, 'flow_failure', :title, :message, :metadata::jsonb)
                """),
                {
                    "tid": tenant_id,
                    "title": f"Flow '{flow_name}' — {count} consecutive failure(s)",
                    "message": f"Error ({failure_type}): {error_message[:500]}",
                    "metadata": json.dumps({
                        "flow_id": flow_id,
                        "flow_name": flow_name,
                        "failure_count": count,
                        "failure_type": failure_type,
                    }),
                },
            )
            await db.commit()
            logger.info(f"[FlowMonitor] Notification created for flow '{flow_name}' ({count} failures)")
        except Exception as e:
            logger.warning(f"[FlowMonitor] Failed to create notification: {e}")

    # ------------------------------------------------------------------
    # Jira ticket creation
    # ------------------------------------------------------------------

    async def _create_jira_ticket(
        self, db, tenant_id: str, flow_id: str, flow_name: str,
        count: int, error_message: str, failure_type: str,
    ):
        """Create a Jira ticket for persistent failures (dedup within 24h)."""
        # Deduplication: skip if we created a ticket for this flow in last 24h
        now = datetime.now(timezone.utc)
        last_ticket = self._last_jira_ticket.get(flow_id)
        if last_ticket and (now - last_ticket) < timedelta(hours=24):
            logger.info(
                f"[FlowMonitor] Skipping Jira ticket for flow '{flow_name}' — "
                f"last ticket created {(now - last_ticket).total_seconds() / 3600:.1f}h ago"
            )
            return

        try:
            from assemblyline_common.ai.tools.jira_tool import JiraTool
            from assemblyline_common.ai.authorization import AuthorizationContext, Permission

            jira = JiraTool()

            # Create a minimal auth context for the tool
            auth_context = AuthorizationContext(
                user_id="system-flow-monitor",
                tenant_id=tenant_id,
                permissions=[Permission.READ, Permission.WRITE],
            )

            summary = f"[Auto] Flow '{flow_name}' — {count} consecutive failures"
            description = (
                f"The AI Flow Monitor detected {count} consecutive failures for flow '{flow_name}' "
                f"(ID: {flow_id}).\n\n"
                f"Failure type: {failure_type}\n"
                f"Latest error: {error_message[:500]}\n\n"
                f"This ticket was automatically created by the YAMIL Flow Monitor.\n"
                f"Investigate the flow execution logs for root cause analysis."
            )

            result = await jira.execute(
                auth_context=auth_context,
                action="create_issue",
                project_key=self._jira_project_key,
                summary=summary,
                description=description,
                issue_type="Bug",
                priority="High",
                labels=["flow-error", "automated", "flow-monitor"],
                _db=db,
                _tenant_id=tenant_id,
            )

            if result.success:
                self._last_jira_ticket[flow_id] = now
                logger.info(f"[FlowMonitor] Jira ticket created for flow '{flow_name}': {result.message}")
            else:
                logger.warning(f"[FlowMonitor] Jira ticket creation failed: {result.error}")

        except Exception as e:
            logger.error(f"[FlowMonitor] Jira ticket creation error: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Return monitor status for the status endpoint."""
        return {
            "running": bool(self._scheduler and self._scheduler.running),
            "last_check": self._last_check.isoformat() if self._last_check else None,
            "check_interval_minutes": self._check_interval,
            "tracked_flows": len(self._failure_counts),
            "failure_counts": {
                fid: {"flow_name": self._flow_names.get(fid, fid), "consecutive_failures": count}
                for fid, count in self._failure_counts.items()
                if count > 0
            },
            "recent_jira_tickets": {
                fid: ts.isoformat()
                for fid, ts in self._last_jira_ticket.items()
            },
        }

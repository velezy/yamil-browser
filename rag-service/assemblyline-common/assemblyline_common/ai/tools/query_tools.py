"""
Query Tools

Tools for searching messages, viewing audit logs, and generating statistics.
"""

import logging
from typing import Optional, Dict, List, Any
from uuid import UUID
from datetime import datetime, timezone, timedelta

from assemblyline_common.ai.tools.base import Tool, ToolResult, ToolDefinition
from assemblyline_common.ai.authorization import AuthorizationContext, Permission

logger = logging.getLogger(__name__)


# ============================================================================
# Message Search Tools
# ============================================================================

class SearchMessagesTool(Tool):
    """Tool to search through processed messages."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="search_messages",
            description="Search through processed HL7/FHIR messages",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "message_type": {"type": "string", "enum": ["hl7", "fhir", "all"], "description": "Message type filter"},
                    "flow_id": {"type": "string", "description": "Filter by flow UUID"},
                    "status": {"type": "string", "enum": ["success", "failed", "pending", "all"], "description": "Processing status"},
                    "date_from": {"type": "string", "description": "Start date (ISO 8601)"},
                    "date_to": {"type": "string", "description": "End date (ISO 8601)"},
                    "limit": {"type": "integer", "description": "Max results (1-100)", "default": 20},
                },
                "required": ["query"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        query: str,
        message_type: str = "all",
        flow_id: Optional[str] = None,
        status: str = "all",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 20,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_MESSAGES):
            return ToolResult(success=False, error="Not authorized to search messages")

        limit = min(max(limit, 1), 100)

        # In production, query from database with full-text search
        results = [
            {
                "id": "msg-001",
                "type": "hl7",
                "message_type": "ADT^A01",
                "flow_id": "flow-123",
                "status": "success",
                "timestamp": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                "preview": "MSH|^~\\&|HOSPITAL|...",
                "matched_field": "PID-5",
            },
            {
                "id": "msg-002",
                "type": "fhir",
                "resource_type": "Patient",
                "flow_id": "flow-456",
                "status": "success",
                "timestamp": (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat(),
                "preview": '{"resourceType": "Patient", "id": "...',
                "matched_field": "name.family",
            },
        ]

        return ToolResult(
            success=True,
            data={
                "query": query,
                "total_found": len(results),
                "results": results[:limit],
                "filters_applied": {
                    "message_type": message_type,
                    "flow_id": flow_id,
                    "status": status,
                    "date_range": f"{date_from or 'any'} to {date_to or 'any'}",
                },
            },
            message=f"Found {len(results)} messages matching '{query}'",
        )


class GetMessageDetailsTool(Tool):
    """Tool to get full details of a specific message."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_message_details",
            description="Get full details of a processed message",
            parameters={
                "type": "object",
                "properties": {
                    "message_id": {"type": "string", "description": "Message UUID"},
                    "include_raw": {"type": "boolean", "description": "Include raw message content", "default": True},
                    "include_processing": {"type": "boolean", "description": "Include processing history", "default": True},
                },
                "required": ["message_id"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        message_id: str,
        include_raw: bool = True,
        include_processing: bool = True,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_MESSAGES):
            return ToolResult(success=False, error="Not authorized to view messages")

        # In production, fetch from database
        message = {
            "id": message_id,
            "type": "hl7",
            "message_type": "ADT^A01",
            "version": "2.5.1",
            "flow_id": "flow-123",
            "flow_name": "Epic ADT Processor",
            "status": "success",
            "received_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            "processed_at": (datetime.now(timezone.utc) - timedelta(hours=2, seconds=-1)).isoformat(),
            "processing_time_ms": 145,
        }

        if include_raw:
            message["raw_content"] = "MSH|^~\\&|HOSPITAL|DEPT|EPIC|PROD|20231215120000||ADT^A01|MSG001|P|2.5.1\\rPID|1||123456^^^MRN||DOE^JOHN^A||19800101|M"

        if include_processing:
            message["processing_history"] = [
                {"node": "hl7-receiver", "status": "success", "duration_ms": 5, "timestamp": message["received_at"]},
                {"node": "hl7-parser", "status": "success", "duration_ms": 25, "timestamp": message["received_at"]},
                {"node": "filter", "status": "success", "duration_ms": 2, "output": "matched"},
                {"node": "fhir-mapper", "status": "success", "duration_ms": 85},
                {"node": "http-output", "status": "success", "duration_ms": 28, "response_code": 200},
            ]

        return ToolResult(
            success=True,
            data=message,
            message=f"Retrieved details for message {message_id}",
        )


# ============================================================================
# Audit Log Tools
# ============================================================================

class GetAuditLogsTool(Tool):
    """Tool to query audit logs."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_audit_logs",
            description="Query audit logs for security and compliance",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string", "description": "Filter by user UUID"},
                    "action_type": {"type": "string", "description": "Filter by action type"},
                    "resource_type": {"type": "string", "description": "Filter by resource type"},
                    "date_from": {"type": "string", "description": "Start date (ISO 8601)"},
                    "date_to": {"type": "string", "description": "End date (ISO 8601)"},
                    "limit": {"type": "integer", "description": "Max results (1-500)", "default": 50},
                },
            },
            required_permission=Permission.VIEW_AUDIT_LOGS,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        user_id: Optional[str] = None,
        action_type: Optional[str] = None,
        resource_type: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 50,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_AUDIT_LOGS):
            return ToolResult(success=False, error="Not authorized to view audit logs")

        limit = min(max(limit, 1), 500)

        # In production, query from audit_trail table
        logs = [
            {
                "id": "audit-001",
                "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
                "user_id": "user-123",
                "user_email": "admin@example.com",
                "action": "user.login",
                "resource_type": "session",
                "resource_id": "session-456",
                "ip_address": "10.0.1.50",
                "success": True,
            },
            {
                "id": "audit-002",
                "timestamp": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                "user_id": "user-789",
                "user_email": "dev@example.com",
                "action": "flow.update",
                "resource_type": "flow",
                "resource_id": "flow-123",
                "ip_address": "10.0.1.55",
                "success": True,
                "changes": {"nodes": "modified", "version": "1 -> 2"},
            },
            {
                "id": "audit-003",
                "timestamp": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                "user_id": "user-456",
                "user_email": "analyst@example.com",
                "action": "message.view",
                "resource_type": "message",
                "resource_id": "msg-001",
                "ip_address": "10.0.1.60",
                "success": True,
                "phi_accessed": True,
            },
        ]

        return ToolResult(
            success=True,
            data={
                "total_count": len(logs),
                "logs": logs[:limit],
                "filters": {
                    "user_id": user_id,
                    "action_type": action_type,
                    "resource_type": resource_type,
                    "date_range": f"{date_from or 'any'} to {date_to or 'any'}",
                },
            },
            message=f"Retrieved {len(logs)} audit log entries",
        )


class GetPHIAccessLogsTool(Tool):
    """Tool to query PHI access logs for compliance."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_phi_access_logs",
            description="Query PHI access logs for HIPAA compliance",
            parameters={
                "type": "object",
                "properties": {
                    "patient_id": {"type": "string", "description": "Filter by patient identifier"},
                    "user_id": {"type": "string", "description": "Filter by accessing user"},
                    "date_from": {"type": "string", "description": "Start date (ISO 8601)"},
                    "date_to": {"type": "string", "description": "End date (ISO 8601)"},
                    "limit": {"type": "integer", "description": "Max results (1-500)", "default": 50},
                },
            },
            required_permission=Permission.VIEW_PHI_AUDIT,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        patient_id: Optional[str] = None,
        user_id: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 50,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_PHI_AUDIT):
            return ToolResult(success=False, error="Not authorized to view PHI access logs")

        # In production, query from phi_access_log table
        logs = [
            {
                "id": "phi-001",
                "timestamp": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                "user_id": "user-123",
                "user_email": "clinician@hospital.com",
                "patient_id": "MRN-456789",
                "access_type": "view",
                "phi_fields": ["name", "dob", "ssn"],
                "purpose": "treatment",
                "message_id": "msg-001",
            },
            {
                "id": "phi-002",
                "timestamp": (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat(),
                "user_id": "user-456",
                "user_email": "billing@hospital.com",
                "patient_id": "MRN-456789",
                "access_type": "view",
                "phi_fields": ["name", "insurance"],
                "purpose": "billing",
                "message_id": "msg-002",
            },
        ]

        return ToolResult(
            success=True,
            data={
                "total_count": len(logs),
                "logs": logs[:limit],
                "patient_id_filter": patient_id,
                "compliance_note": "All PHI access is logged per HIPAA requirements",
            },
            message=f"Retrieved {len(logs)} PHI access log entries",
        )


# ============================================================================
# Statistics Tools
# ============================================================================

class GetStatisticsTool(Tool):
    """Tool to get aggregate statistics."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_statistics",
            description="Get aggregate statistics for messages, flows, and system health",
            parameters={
                "type": "object",
                "properties": {
                    "category": {"type": "string", "enum": ["messages", "flows", "users", "system"], "description": "Statistics category"},
                    "time_range": {"type": "string", "enum": ["1h", "24h", "7d", "30d"], "description": "Time range"},
                    "group_by": {"type": "string", "enum": ["hour", "day", "week"], "description": "Grouping period"},
                },
                "required": ["category"],
            },
            required_permission=Permission.VIEW_ANALYTICS,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        category: str,
        time_range: str = "24h",
        group_by: str = "hour",
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_ANALYTICS):
            return ToolResult(success=False, error="Not authorized to view analytics")

        stats: Dict[str, Any] = {"category": category, "time_range": time_range}

        if category == "messages":
            stats["summary"] = {
                "total_processed": 15420,
                "successful": 14985,
                "failed": 435,
                "success_rate": 97.2,
                "average_processing_ms": 142,
            }
            stats["by_type"] = {
                "hl7_adt": 5200,
                "hl7_oru": 4800,
                "hl7_orm": 3100,
                "fhir_patient": 1200,
                "fhir_observation": 1120,
            }
            stats["trend"] = [
                {"period": "0-6h", "count": 2400},
                {"period": "6-12h", "count": 3800},
                {"period": "12-18h", "count": 4500},
                {"period": "18-24h", "count": 4720},
            ]

        elif category == "flows":
            stats["summary"] = {
                "total_flows": 12,
                "active": 8,
                "draft": 3,
                "disabled": 1,
            }
            stats["top_flows"] = [
                {"id": "flow-1", "name": "Epic ADT Processor", "executions": 5200, "success_rate": 98.5},
                {"id": "flow-2", "name": "Lab Results Router", "executions": 4800, "success_rate": 97.8},
                {"id": "flow-3", "name": "Order Entry Handler", "executions": 3100, "success_rate": 96.2},
            ]

        elif category == "users":
            stats["summary"] = {
                "total_users": 24,
                "active_today": 12,
                "active_week": 18,
            }
            stats["by_role"] = {
                "admin": 2,
                "developer": 5,
                "operator": 8,
                "viewer": 9,
            }
            stats["activity"] = {
                "logins": 45,
                "flow_edits": 12,
                "message_views": 234,
            }

        elif category == "system":
            stats["health"] = {
                "status": "healthy",
                "uptime_hours": 720,
                "last_restart": (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
            }
            stats["resources"] = {
                "cpu_percent": 45,
                "memory_percent": 62,
                "disk_percent": 38,
                "queue_depth": 12,
            }
            stats["services"] = [
                {"name": "auth-service", "status": "healthy", "latency_ms": 12},
                {"name": "gateway", "status": "healthy", "latency_ms": 8},
                {"name": "message-processor", "status": "healthy", "latency_ms": 145},
                {"name": "ai-orchestra", "status": "healthy", "latency_ms": 350},
            ]

        return ToolResult(
            success=True,
            data=stats,
            message=f"{category.capitalize()} statistics for {time_range}",
        )


class GetTrendAnalysisTool(Tool):
    """Tool to get trend analysis over time."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_trend_analysis",
            description="Get trend analysis for message volumes, errors, or performance",
            parameters={
                "type": "object",
                "properties": {
                    "metric": {"type": "string", "enum": ["volume", "errors", "latency", "success_rate"], "description": "Metric to analyze"},
                    "flow_id": {"type": "string", "description": "Filter by flow UUID"},
                    "time_range": {"type": "string", "enum": ["24h", "7d", "30d", "90d"], "description": "Time range"},
                    "compare_previous": {"type": "boolean", "description": "Compare with previous period", "default": True},
                },
                "required": ["metric"],
            },
            required_permission=Permission.VIEW_ANALYTICS,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        metric: str,
        flow_id: Optional[str] = None,
        time_range: str = "7d",
        compare_previous: bool = True,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_ANALYTICS):
            return ToolResult(success=False, error="Not authorized to view analytics")

        # Generate trend data
        trend = {
            "metric": metric,
            "time_range": time_range,
            "flow_id": flow_id,
        }

        if metric == "volume":
            trend["current_period"] = {
                "total": 45000,
                "daily_average": 6428,
                "peak_day": "2023-12-15",
                "peak_value": 8500,
            }
            trend["data_points"] = [
                {"date": "2023-12-10", "value": 5800},
                {"date": "2023-12-11", "value": 6200},
                {"date": "2023-12-12", "value": 6500},
                {"date": "2023-12-13", "value": 6800},
                {"date": "2023-12-14", "value": 7200},
                {"date": "2023-12-15", "value": 8500},
                {"date": "2023-12-16", "value": 4000},  # Weekend
            ]
            if compare_previous:
                trend["previous_period"] = {
                    "total": 42000,
                    "daily_average": 6000,
                }
                trend["change"] = {
                    "absolute": 3000,
                    "percent": 7.1,
                    "direction": "up",
                }

        elif metric == "errors":
            trend["current_period"] = {
                "total": 450,
                "daily_average": 64,
                "error_rate": 1.0,
            }
            trend["by_type"] = {
                "validation": 180,
                "timeout": 120,
                "auth_failure": 90,
                "parse_error": 60,
            }
            if compare_previous:
                trend["previous_period"] = {"total": 380, "error_rate": 0.9}
                trend["change"] = {"absolute": 70, "percent": 18.4, "direction": "up"}
                trend["alert"] = "Error rate increased - investigate validation errors"

        elif metric == "latency":
            trend["current_period"] = {
                "p50_ms": 145,
                "p95_ms": 420,
                "p99_ms": 890,
            }
            trend["data_points"] = [
                {"date": "2023-12-10", "p50": 140, "p95": 400},
                {"date": "2023-12-11", "p50": 142, "p95": 410},
                {"date": "2023-12-12", "p50": 145, "p95": 415},
                {"date": "2023-12-13", "p50": 148, "p95": 425},
                {"date": "2023-12-14", "p50": 150, "p95": 440},
                {"date": "2023-12-15", "p50": 155, "p95": 460},
                {"date": "2023-12-16", "p50": 138, "p95": 380},
            ]
            if compare_previous:
                trend["previous_period"] = {"p50_ms": 138, "p95_ms": 390}
                trend["change"] = {"p50_change": 5.1, "p95_change": 7.7, "direction": "up"}

        elif metric == "success_rate":
            trend["current_period"] = {"rate": 97.2, "successful": 43650, "failed": 1350}
            trend["data_points"] = [
                {"date": "2023-12-10", "rate": 97.5},
                {"date": "2023-12-11", "rate": 97.8},
                {"date": "2023-12-12", "rate": 97.2},
                {"date": "2023-12-13", "rate": 96.8},
                {"date": "2023-12-14", "rate": 97.0},
                {"date": "2023-12-15", "rate": 97.4},
                {"date": "2023-12-16", "rate": 97.6},
            ]
            if compare_previous:
                trend["previous_period"] = {"rate": 97.8}
                trend["change"] = {"absolute": -0.6, "direction": "down"}

        return ToolResult(
            success=True,
            data=trend,
            message=f"{metric.replace('_', ' ').title()} trend for {time_range}",
        )


# ============================================================================
# Query Tools Collection
# ============================================================================

class QueryTools:
    """Collection of all query-related tools."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [
            # Message Search
            SearchMessagesTool(),
            GetMessageDetailsTool(),
            # Audit
            GetAuditLogsTool(),
            GetPHIAccessLogsTool(),
            # Statistics
            GetStatisticsTool(),
            GetTrendAnalysisTool(),
        ]

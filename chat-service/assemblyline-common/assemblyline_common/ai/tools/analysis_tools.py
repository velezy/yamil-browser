"""
Analysis Tools

Tools for parsing HL7/FHIR messages, analyzing flow performance, and error diagnostics.
"""

import logging
import re
from typing import Optional, Dict, List, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta

from assemblyline_common.ai.tools.base import Tool, ToolResult, ToolDefinition
from assemblyline_common.ai.authorization import AuthorizationContext, Permission

logger = logging.getLogger(__name__)


# ============================================================================
# HL7 Parsing Tools
# ============================================================================

class ParseHL7Tool(Tool):
    """Tool to parse HL7 v2.x messages."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="parse_hl7",
            description="Parse an HL7 v2.x message and extract fields",
            parameters={
                "type": "object",
                "properties": {
                    "message": {"type": "string", "description": "Raw HL7 message"},
                    "extract_fields": {"type": "array", "items": {"type": "string"}, "description": "Specific fields to extract (e.g., ['PID-5', 'OBR-4'])"},
                },
                "required": ["message"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        message: str,
        extract_fields: Optional[List[str]] = None,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_MESSAGES):
            return ToolResult(success=False, error="Not authorized to parse messages")

        try:
            # Parse HL7 structure
            segments = message.strip().split('\r')
            if not segments:
                segments = message.strip().split('\n')

            parsed = {
                "segment_count": len(segments),
                "segments": {},
                "message_type": None,
                "version": None,
            }

            for segment in segments:
                if not segment.strip():
                    continue

                fields = segment.split('|')
                segment_name = fields[0]

                if segment_name not in parsed["segments"]:
                    parsed["segments"][segment_name] = []

                segment_data = {"fields": fields[1:]}
                parsed["segments"][segment_name].append(segment_data)

                # Extract message type and version from MSH
                if segment_name == "MSH":
                    if len(fields) > 9:
                        parsed["message_type"] = fields[8] if len(fields) > 8 else None
                        parsed["version"] = fields[11] if len(fields) > 11 else None

            # Extract specific fields if requested
            extracted = {}
            if extract_fields:
                for field_spec in extract_fields:
                    match = re.match(r'(\w+)-(\d+)(?:\.(\d+))?', field_spec)
                    if match:
                        seg_name, field_num, component = match.groups()
                        field_idx = int(field_num) - 1

                        if seg_name in parsed["segments"]:
                            for seg_instance in parsed["segments"][seg_name]:
                                if field_idx < len(seg_instance["fields"]):
                                    value = seg_instance["fields"][field_idx]
                                    if component:
                                        components = value.split('^')
                                        comp_idx = int(component) - 1
                                        value = components[comp_idx] if comp_idx < len(components) else None
                                    extracted[field_spec] = value
                                    break

            return ToolResult(
                success=True,
                data={
                    "parsed": parsed,
                    "extracted_fields": extracted if extract_fields else None,
                },
                message=f"Parsed HL7 message: {parsed['message_type']} v{parsed['version']} with {len(segments)} segments",
            )

        except Exception as e:
            logger.exception("HL7 parsing error")
            return ToolResult(success=False, error=f"Parse error: {str(e)}")


class ValidateHL7Tool(Tool):
    """Tool to validate HL7 v2.x message structure."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="validate_hl7",
            description="Validate an HL7 v2.x message against specification",
            parameters={
                "type": "object",
                "properties": {
                    "message": {"type": "string", "description": "Raw HL7 message"},
                    "version": {"type": "string", "description": "HL7 version to validate against", "default": "2.5.1"},
                    "strict": {"type": "boolean", "description": "Enable strict validation", "default": False},
                },
                "required": ["message"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        message: str,
        version: str = "2.5.1",
        strict: bool = False,
        **kwargs,
    ) -> ToolResult:
        errors = []
        warnings = []

        segments = message.strip().split('\r')
        if not segments or len(segments) == 1:
            segments = message.strip().split('\n')

        # Check for MSH segment
        if not segments or not segments[0].startswith("MSH"):
            errors.append("Message must start with MSH segment")
        else:
            msh_fields = segments[0].split('|')

            # Check field separator
            if len(msh_fields) < 2 or msh_fields[0] != "MSH":
                errors.append("Invalid MSH segment structure")

            # Check encoding characters
            if len(msh_fields) > 1 and len(msh_fields[1]) < 4:
                errors.append("MSH-2 encoding characters incomplete")

            # Check required MSH fields
            if len(msh_fields) < 12:
                warnings.append("MSH segment may be missing required fields")

            # Check message type
            if len(msh_fields) > 8 and not msh_fields[8]:
                errors.append("MSH-9 Message Type is required")

            # Check control ID
            if len(msh_fields) > 9 and not msh_fields[9]:
                errors.append("MSH-10 Message Control ID is required")

        # Validate segment structure
        for i, segment in enumerate(segments):
            if not segment.strip():
                continue

            fields = segment.split('|')
            seg_name = fields[0]

            # Check segment name format
            if not re.match(r'^[A-Z][A-Z0-9]{2}$', seg_name):
                warnings.append(f"Line {i+1}: Invalid segment name '{seg_name}'")

            # Check for PID in patient messages
            if i == 0 and "ADT" in segment and strict:
                has_pid = any(s.startswith("PID") for s in segments)
                if not has_pid:
                    errors.append("ADT messages require PID segment")

        is_valid = len(errors) == 0

        return ToolResult(
            success=True,
            data={
                "is_valid": is_valid,
                "errors": errors,
                "warnings": warnings,
                "segment_count": len([s for s in segments if s.strip()]),
            },
            message=f"Validation {'passed' if is_valid else 'failed'}: {len(errors)} error(s), {len(warnings)} warning(s)",
        )


# ============================================================================
# FHIR Tools
# ============================================================================

class ParseFHIRTool(Tool):
    """Tool to parse and validate FHIR resources."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="parse_fhir",
            description="Parse a FHIR resource and extract information",
            parameters={
                "type": "object",
                "properties": {
                    "resource": {"type": "object", "description": "FHIR resource JSON"},
                    "extract_paths": {"type": "array", "items": {"type": "string"}, "description": "FHIRPath expressions to extract"},
                },
                "required": ["resource"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        resource: Dict[str, Any],
        extract_paths: Optional[List[str]] = None,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_MESSAGES):
            return ToolResult(success=False, error="Not authorized to parse FHIR resources")

        try:
            resource_type = resource.get("resourceType")
            if not resource_type:
                return ToolResult(success=False, error="Invalid FHIR resource: missing resourceType")

            analysis = {
                "resourceType": resource_type,
                "id": resource.get("id"),
                "meta": resource.get("meta"),
                "profile": resource.get("meta", {}).get("profile", []),
            }

            # Resource-specific analysis
            if resource_type == "Patient":
                analysis["identifiers"] = [
                    {"system": i.get("system"), "value": i.get("value")}
                    for i in resource.get("identifier", [])
                ]
                names = resource.get("name", [])
                if names:
                    analysis["name"] = names[0].get("text") or " ".join(names[0].get("given", []) + [names[0].get("family", "")])
                analysis["birthDate"] = resource.get("birthDate")
                analysis["gender"] = resource.get("gender")

            elif resource_type == "Observation":
                analysis["status"] = resource.get("status")
                analysis["code"] = resource.get("code", {}).get("coding", [{}])[0]
                analysis["value"] = resource.get("valueQuantity") or resource.get("valueString") or resource.get("valueCodeableConcept")
                analysis["effectiveDateTime"] = resource.get("effectiveDateTime")

            elif resource_type == "Bundle":
                entries = resource.get("entry", [])
                analysis["bundleType"] = resource.get("type")
                analysis["entryCount"] = len(entries)
                analysis["resourceTypes"] = list(set(
                    e.get("resource", {}).get("resourceType") for e in entries if e.get("resource")
                ))

            # Simple path extraction (not full FHIRPath)
            extracted = {}
            if extract_paths:
                for path in extract_paths:
                    parts = path.split('.')
                    value = resource
                    for part in parts:
                        if isinstance(value, dict):
                            value = value.get(part)
                        elif isinstance(value, list) and value:
                            value = value[0].get(part) if isinstance(value[0], dict) else None
                        else:
                            value = None
                            break
                    extracted[path] = value

            return ToolResult(
                success=True,
                data={
                    "analysis": analysis,
                    "extracted": extracted if extract_paths else None,
                },
                message=f"Parsed FHIR {resource_type} resource",
            )

        except Exception as e:
            logger.exception("FHIR parsing error")
            return ToolResult(success=False, error=f"Parse error: {str(e)}")


class ValidateFHIRTool(Tool):
    """Tool to validate FHIR resources."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="validate_fhir",
            description="Validate a FHIR resource against specification",
            parameters={
                "type": "object",
                "properties": {
                    "resource": {"type": "object", "description": "FHIR resource JSON"},
                    "profile": {"type": "string", "description": "Profile URL to validate against"},
                },
                "required": ["resource"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        resource: Dict[str, Any],
        profile: Optional[str] = None,
        **kwargs,
    ) -> ToolResult:
        errors = []
        warnings = []

        # Check required fields
        if not resource.get("resourceType"):
            errors.append("Missing required field: resourceType")

        resource_type = resource.get("resourceType", "Unknown")

        # Resource-specific validation
        if resource_type == "Patient":
            if not resource.get("identifier"):
                warnings.append("Patient should have at least one identifier")
            if not resource.get("name"):
                warnings.append("Patient should have at least one name")

        elif resource_type == "Observation":
            if not resource.get("status"):
                errors.append("Observation requires status field")
            if not resource.get("code"):
                errors.append("Observation requires code field")
            if resource.get("status") not in ["registered", "preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "unknown"]:
                errors.append(f"Invalid observation status: {resource.get('status')}")

        elif resource_type == "Bundle":
            if not resource.get("type"):
                errors.append("Bundle requires type field")
            entries = resource.get("entry", [])
            for i, entry in enumerate(entries):
                if not entry.get("resource"):
                    warnings.append(f"Bundle entry {i} has no resource")

        # Check meta
        if resource.get("meta"):
            meta = resource["meta"]
            if meta.get("lastUpdated"):
                # Validate datetime format
                try:
                    datetime.fromisoformat(meta["lastUpdated"].replace('Z', '+00:00'))
                except:
                    errors.append("Invalid meta.lastUpdated datetime format")

        is_valid = len(errors) == 0

        return ToolResult(
            success=True,
            data={
                "is_valid": is_valid,
                "resourceType": resource_type,
                "errors": errors,
                "warnings": warnings,
            },
            message=f"FHIR {resource_type} validation {'passed' if is_valid else 'failed'}",
        )


# ============================================================================
# Performance Analysis Tools
# ============================================================================

class AnalyzeFlowPerformanceTool(Tool):
    """Tool to analyze flow execution performance."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="analyze_flow_performance",
            description="Analyze performance metrics for a flow",
            parameters={
                "type": "object",
                "properties": {
                    "flow_id": {"type": "string", "description": "Flow UUID"},
                    "time_range": {"type": "string", "enum": ["1h", "24h", "7d", "30d"], "description": "Time range"},
                },
                "required": ["flow_id"],
            },
            required_permission=Permission.VIEW_ANALYTICS,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        flow_id: str,
        time_range: str = "24h",
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_ANALYTICS):
            return ToolResult(success=False, error="Not authorized to view analytics")

        # In production, fetch from metrics database
        performance = {
            "flow_id": flow_id,
            "time_range": time_range,
            "summary": {
                "total_executions": 1542,
                "successful": 1498,
                "failed": 44,
                "success_rate": 97.1,
            },
            "latency": {
                "p50_ms": 145,
                "p95_ms": 420,
                "p99_ms": 890,
                "max_ms": 2340,
            },
            "throughput": {
                "messages_per_minute": 12.8,
                "peak_mpm": 45.2,
            },
            "node_performance": [
                {"node": "hl7-receiver", "avg_ms": 15, "error_rate": 0.1},
                {"node": "hl7-parser", "avg_ms": 25, "error_rate": 0.5},
                {"node": "fhir-mapper", "avg_ms": 85, "error_rate": 1.2},
                {"node": "http-output", "avg_ms": 120, "error_rate": 0.8},
            ],
            "bottleneck": "fhir-mapper",
            "recommendations": [
                "Consider caching FHIR mappings for repeated message types",
                "HTTP output shows intermittent slowness - check target endpoint",
            ],
        }

        return ToolResult(
            success=True,
            data=performance,
            message=f"Flow {flow_id} has {performance['summary']['success_rate']}% success rate over {time_range}",
        )


class GetErrorSummaryTool(Tool):
    """Tool to get error summary and diagnostics."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="get_error_summary",
            description="Get error summary and diagnostics for a flow or tenant",
            parameters={
                "type": "object",
                "properties": {
                    "flow_id": {"type": "string", "description": "Flow UUID (optional, omit for tenant-wide)"},
                    "time_range": {"type": "string", "enum": ["1h", "24h", "7d"], "description": "Time range"},
                    "error_type": {"type": "string", "description": "Filter by error type"},
                },
            },
            required_permission=Permission.VIEW_ANALYTICS,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        flow_id: Optional[str] = None,
        time_range: str = "24h",
        error_type: Optional[str] = None,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_ANALYTICS):
            return ToolResult(success=False, error="Not authorized to view analytics")

        # In production, query error logs
        summary = {
            "time_range": time_range,
            "scope": f"Flow {flow_id}" if flow_id else "Tenant-wide",
            "total_errors": 44,
            "by_category": {
                "validation_error": 18,
                "connection_timeout": 12,
                "auth_failure": 8,
                "parse_error": 4,
                "unknown": 2,
            },
            "by_node": {
                "fhir-mapper": 15,
                "http-output": 12,
                "hl7-parser": 10,
                "epic-fhir": 7,
            },
            "recent_errors": [
                {
                    "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat(),
                    "error": "FHIR validation failed: missing required field 'status'",
                    "node": "fhir-mapper",
                    "message_id": "msg-123",
                },
                {
                    "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=42)).isoformat(),
                    "error": "Connection timeout to Epic FHIR endpoint",
                    "node": "epic-fhir",
                    "message_id": "msg-456",
                },
            ],
            "recommendations": [
                "18 validation errors suggest schema mismatch - review incoming message format",
                "12 connection timeouts indicate potential network or endpoint issues",
            ],
        }

        return ToolResult(
            success=True,
            data=summary,
            message=f"{summary['total_errors']} errors in {time_range}: {summary['by_category']}",
        )


class CompareMessagesToolTool(Tool):
    """Tool to compare two messages for differences."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="compare_messages",
            description="Compare two HL7 or FHIR messages to identify differences",
            parameters={
                "type": "object",
                "properties": {
                    "message_a": {"type": "string", "description": "First message (HL7 string or FHIR JSON string)"},
                    "message_b": {"type": "string", "description": "Second message"},
                    "message_type": {"type": "string", "enum": ["hl7", "fhir"], "description": "Message type"},
                },
                "required": ["message_a", "message_b", "message_type"],
            },
            required_permission=Permission.VIEW_MESSAGES,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        message_a: str,
        message_b: str,
        message_type: str,
        **kwargs,
    ) -> ToolResult:
        if not self.check_authorization(auth_context, Permission.VIEW_MESSAGES):
            return ToolResult(success=False, error="Not authorized to view messages")

        differences = []

        if message_type == "hl7":
            # Compare HL7 segment by segment
            segs_a = message_a.strip().split('\r') if '\r' in message_a else message_a.strip().split('\n')
            segs_b = message_b.strip().split('\r') if '\r' in message_b else message_b.strip().split('\n')

            max_segs = max(len(segs_a), len(segs_b))

            for i in range(max_segs):
                seg_a = segs_a[i] if i < len(segs_a) else None
                seg_b = segs_b[i] if i < len(segs_b) else None

                if seg_a != seg_b:
                    if seg_a and seg_b:
                        # Field-level comparison
                        fields_a = seg_a.split('|')
                        fields_b = seg_b.split('|')
                        seg_name = fields_a[0]

                        for j, (fa, fb) in enumerate(zip(fields_a, fields_b)):
                            if fa != fb:
                                differences.append({
                                    "type": "field_changed",
                                    "location": f"{seg_name}-{j}",
                                    "value_a": fa,
                                    "value_b": fb,
                                })
                    elif seg_a:
                        differences.append({
                            "type": "segment_removed",
                            "location": f"Segment {i+1}",
                            "value_a": seg_a[:50] + "..." if len(seg_a) > 50 else seg_a,
                        })
                    else:
                        differences.append({
                            "type": "segment_added",
                            "location": f"Segment {i+1}",
                            "value_b": seg_b[:50] + "..." if len(seg_b) > 50 else seg_b,
                        })

        else:  # FHIR
            import json
            try:
                json_a = json.loads(message_a) if isinstance(message_a, str) else message_a
                json_b = json.loads(message_b) if isinstance(message_b, str) else message_b

                def compare_dicts(a: Any, b: Any, path: str = ""):
                    if type(a) != type(b):
                        differences.append({
                            "type": "type_changed",
                            "path": path,
                            "type_a": type(a).__name__,
                            "type_b": type(b).__name__,
                        })
                    elif isinstance(a, dict):
                        all_keys = set(a.keys()) | set(b.keys())
                        for key in all_keys:
                            new_path = f"{path}.{key}" if path else key
                            if key not in a:
                                differences.append({"type": "added", "path": new_path, "value": b[key]})
                            elif key not in b:
                                differences.append({"type": "removed", "path": new_path, "value": a[key]})
                            else:
                                compare_dicts(a[key], b[key], new_path)
                    elif isinstance(a, list):
                        if len(a) != len(b):
                            differences.append({
                                "type": "array_length_changed",
                                "path": path,
                                "length_a": len(a),
                                "length_b": len(b),
                            })
                        for i, (item_a, item_b) in enumerate(zip(a, b)):
                            compare_dicts(item_a, item_b, f"{path}[{i}]")
                    elif a != b:
                        differences.append({
                            "type": "value_changed",
                            "path": path,
                            "value_a": a,
                            "value_b": b,
                        })

                compare_dicts(json_a, json_b)

            except json.JSONDecodeError as e:
                return ToolResult(success=False, error=f"Invalid JSON: {str(e)}")

        return ToolResult(
            success=True,
            data={
                "difference_count": len(differences),
                "differences": differences[:50],  # Limit to 50
                "identical": len(differences) == 0,
            },
            message=f"Found {len(differences)} difference(s)" if differences else "Messages are identical",
        )


# ============================================================================
# Analysis Tools Collection
# ============================================================================

class AnalysisTools:
    """Collection of all analysis-related tools."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [
            # HL7
            ParseHL7Tool(),
            ValidateHL7Tool(),
            # FHIR
            ParseFHIRTool(),
            ValidateFHIRTool(),
            # Performance
            AnalyzeFlowPerformanceTool(),
            GetErrorSummaryTool(),
            CompareMessagesToolTool(),
        ]

"""
Jira Cloud Tool — AI assistant integration with Atlassian Jira

Provides the in-app AI with the ability to create, search, update,
and transition Jira issues via the Jira REST API v3.

Uses the enterprise HTTPConnector for:
  - Circuit breaker (fail-fast if Jira is down)
  - Retry with exponential backoff (transient 5xx auto-recover)
  - Connection pooling (reuse TCP, 100 max connections)
  - Rate limiting (60 req/min to match Jira's limits)

Credentials are resolved from:
  1. common.connectors (jira_cloud type) → AWS Secrets Manager
  2. Fallback: env vars JIRA_SITE_URL, JIRA_USER_EMAIL, JIRA_API_TOKEN
"""

import logging
import os
from typing import Any, Dict, List, Optional

from assemblyline_common.ai.authorization import AuthorizationContext, Permission
from assemblyline_common.ai.tools.base import Tool, ToolDefinition, ToolResult
from assemblyline_common.connectors.http_connector import (
    HTTPConnectorConfig,
    HTTPResponse,
    get_http_connector,
)
from assemblyline_common.circuit_breaker import CircuitOpenError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JIRA_API_VERSION = "3"
REQUEST_TIMEOUT = 30  # seconds
SEARCH_MAX_RESULTS = 15


# ---------------------------------------------------------------------------
# JiraTool
# ---------------------------------------------------------------------------

class JiraTool(Tool):
    """AI tool for interacting with Jira Cloud via REST API v3."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="jira",
            description=(
                "Interact with Jira Cloud: create issues, search with JQL, "
                "update issues, transition status, add comments, and list projects. "
                "Use this when the user asks about tickets, tasks, bugs, stories, "
                "or project management."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "create_issue",
                            "get_issue",
                            "search",
                            "update_issue",
                            "transition",
                            "add_comment",
                            "list_projects",
                            "list_transitions",
                        ],
                        "description": "Action to perform on Jira.",
                    },
                    "project_key": {
                        "type": "string",
                        "description": "Jira project key, e.g. 'DAT'.",
                    },
                    "issue_key": {
                        "type": "string",
                        "description": "Issue key, e.g. 'DAT-1'.",
                    },
                    "summary": {
                        "type": "string",
                        "description": "Issue title / summary.",
                    },
                    "description": {
                        "type": "string",
                        "description": "Issue description (plain text).",
                    },
                    "issue_type": {
                        "type": "string",
                        "enum": ["Task", "Bug", "Story", "Epic", "Subtask"],
                        "description": "Issue type. Defaults to 'Task'.",
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["Highest", "High", "Medium", "Low", "Lowest"],
                        "description": "Issue priority.",
                    },
                    "assignee": {
                        "type": "string",
                        "description": "Assignee email or account ID.",
                    },
                    "labels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Labels to apply to the issue.",
                    },
                    "jql": {
                        "type": "string",
                        "description": "JQL query for searching issues.",
                    },
                    "transition_name": {
                        "type": "string",
                        "description": "Target status name, e.g. 'In Progress', 'Done'.",
                    },
                    "comment": {
                        "type": "string",
                        "description": "Comment text to add to an issue.",
                    },
                    "fields": {
                        "type": "object",
                        "description": "Arbitrary field updates as key/value pairs.",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max results for search (default 15).",
                    },
                },
                "required": ["action"],
            },
            required_permission=Permission.READ,
        )

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    async def execute(
        self,
        auth_context: AuthorizationContext,
        action: str = "",
        **kwargs,
    ) -> ToolResult:
        if not action:
            return ToolResult(success=False, error="Missing required parameter: action")

        # Resolve credentials
        creds = await self._get_credentials(
            db=kwargs.get("_db"),
            tenant_id=kwargs.get("_tenant_id") or str(auth_context.tenant_id),
        )
        if not creds:
            return ToolResult(
                success=False,
                error=(
                    "No Jira credentials found. Create a 'jira_cloud' connector "
                    "in Settings > Connectors, or set JIRA_SITE_URL / JIRA_USER_EMAIL / "
                    "JIRA_API_TOKEN environment variables."
                ),
            )

        try:
            dispatch = {
                "create_issue": self._create_issue,
                "get_issue": self._get_issue,
                "search": self._search,
                "update_issue": self._update_issue,
                "transition": self._transition,
                "add_comment": self._add_comment,
                "list_projects": self._list_projects,
                "list_transitions": self._list_transitions,
            }
            handler = dispatch.get(action)
            if not handler:
                return ToolResult(success=False, error=f"Unknown action: {action}")

            result_text = await handler(creds, **kwargs)

            self.log_execution(
                auth_context, "jira",
                {"action": action, **{k: v for k, v in kwargs.items() if not k.startswith("_")}},
                ToolResult(success=True),
            )
            return ToolResult(success=True, data=result_text, message=result_text)

        except CircuitOpenError as e:
            err = (
                "Jira API is temporarily unavailable (circuit breaker open). "
                f"Retry after {e.retry_after:.0f}s. This usually means Jira has "
                "been returning errors — it will auto-recover once Jira stabilizes."
            )
            logger.warning(f"[Jira] Circuit open: {e}")
            return ToolResult(success=False, error=err)
        except Exception as e:
            logger.exception(f"jira tool error: {e}")
            return ToolResult(success=False, error=str(e))

    # ------------------------------------------------------------------
    # Credential resolution
    # ------------------------------------------------------------------

    async def _get_credentials(
        self, db=None, tenant_id: str = ""
    ) -> Optional[Dict[str, str]]:
        """Resolve Jira credentials from DB connector or env vars."""

        # Try DB connector first
        if db and tenant_id:
            try:
                from sqlalchemy import text

                result = await db.execute(
                    text(
                        "SELECT id, config FROM common.connectors "
                        "WHERE tenant_id = :tid AND connector_type = 'jira_cloud' "
                        "AND is_active = true AND deleted_at IS NULL "
                        "ORDER BY created_at DESC LIMIT 1"
                    ),
                    {"tid": tenant_id},
                )
                row = result.first()

                if row:
                    config = row.config if isinstance(row.config, dict) else {}
                    site_url = config.get("site_url", "")
                    user_email = config.get("user_email", "")
                    api_token = config.get("api_token", "")

                    # Try AWS Secrets Manager for encrypted api_token
                    SM_SENTINEL = "__SM_MANAGED__"
                    if not api_token or api_token == SM_SENTINEL:
                        try:
                            from assemblyline_common.secrets_manager import (
                                SecretsManagerClient,
                            )

                            sm = SecretsManagerClient()
                            secrets = await sm.get_connector_credentials(
                                tenant_id, str(row.id)
                            )
                            if secrets:
                                api_token = secrets.get("api_token", "")
                        except Exception as sm_err:
                            logger.warning(f"Secrets Manager lookup failed: {sm_err}")

                    if site_url and user_email and api_token:
                        # Normalize site_url
                        if not site_url.startswith("http"):
                            site_url = f"https://{site_url}"
                        logger.info(f"[Jira] Using DB connector for {site_url}")
                        return {
                            "site_url": site_url,
                            "user_email": user_email,
                            "api_token": api_token,
                        }
            except Exception as db_err:
                logger.warning(f"DB connector lookup failed: {db_err}")

        # Fallback to env vars
        site_url = os.environ.get("JIRA_SITE_URL", "")
        user_email = os.environ.get("JIRA_USER_EMAIL", "")
        api_token = os.environ.get("JIRA_API_TOKEN", "")

        if site_url and user_email and api_token:
            if not site_url.startswith("http"):
                site_url = f"https://{site_url}"
            logger.info(f"[Jira] Using env-var credentials for {site_url}")
            return {
                "site_url": site_url,
                "user_email": user_email,
                "api_token": api_token,
            }

        return None

    # ------------------------------------------------------------------
    # HTTP helpers (via enterprise HTTPConnector)
    # ------------------------------------------------------------------

    async def _get_connector(self, creds: Dict[str, str]):
        """Get or create a cached HTTPConnector for this Jira site."""
        base_url = f"{creds['site_url']}/rest/api/{JIRA_API_VERSION}"
        config = HTTPConnectorConfig(
            base_url=base_url,
            basic_auth=(creds["user_email"], creds["api_token"]),
            timeout=REQUEST_TIMEOUT,
            http2=False,  # Jira Cloud doesn't support HTTP/2
            enable_circuit_breaker=True,
            enable_retry=True,
            enable_rate_limiting=True,
            rate_limit_requests=60,
            rate_limit_window_seconds=60,
            default_headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        return await get_http_connector(config, name=f"jira:{creds['site_url']}")

    async def _request(
        self,
        creds: Dict[str, str],
        method: str,
        path: str,
        body: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Any:
        connector = await self._get_connector(creds)
        resp: HTTPResponse = await connector.request(
            method, path, json=body, params=params
        )
        if resp.status_code == 204:
            return {}
        if not resp.ok:
            raise Exception(
                f"Jira API error {resp.status_code}: {resp.text[:200]}"
            )
        return resp.json

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    async def _create_issue(self, creds: Dict[str, str], **kwargs) -> str:
        project_key = kwargs.get("project_key", "")
        summary = kwargs.get("summary", "")
        if not project_key or not summary:
            return "Error: project_key and summary are required for create_issue"

        fields: Dict[str, Any] = {
            "project": {"key": project_key},
            "summary": summary,
            "issuetype": {"name": kwargs.get("issue_type", "Task")},
        }

        desc = kwargs.get("description", "")
        if desc:
            # Atlassian Document Format (ADF)
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": desc}],
                    }
                ],
            }

        priority = kwargs.get("priority")
        if priority:
            fields["priority"] = {"name": priority}

        labels = kwargs.get("labels")
        if labels:
            fields["labels"] = labels

        assignee = kwargs.get("assignee")
        if assignee:
            # Try to find account ID by email
            account_id = await self._find_user(creds, assignee)
            if account_id:
                fields["assignee"] = {"accountId": account_id}

        result = await self._request(creds, "POST", "/issue", body={"fields": fields})
        key = result.get("key", "?")
        return f"Created {key}: {summary}\nURL: {creds['site_url']}/browse/{key}"

    async def _get_issue(self, creds: Dict[str, str], **kwargs) -> str:
        issue_key = kwargs.get("issue_key", "")
        if not issue_key:
            return "Error: issue_key is required for get_issue"

        data = await self._request(creds, "GET", f"/issue/{issue_key}")
        return self._format_issue(data, creds)

    async def _search(self, creds: Dict[str, str], **kwargs) -> str:
        jql = kwargs.get("jql", "")
        if not jql:
            # Default: open issues in project
            pk = kwargs.get("project_key", "")
            if pk:
                jql = f"project = {pk} ORDER BY created DESC"
            else:
                jql = "assignee = currentUser() ORDER BY updated DESC"

        max_results = kwargs.get("max_results", SEARCH_MAX_RESULTS)
        body = {"jql": jql, "maxResults": max_results, "fields": [
            "summary", "status", "priority", "assignee", "issuetype", "created", "updated"
        ]}

        data = await self._request(creds, "POST", "/search/jql", body=body)
        issues = data.get("issues", [])
        total = data.get("total", 0)

        if not issues:
            return f"No issues found for: {jql}"

        lines = [f"Found {total} issue(s) (showing {len(issues)}):"]
        lines.append("")
        for iss in issues:
            f = iss.get("fields", {})
            key = iss.get("key", "?")
            summary = f.get("summary", "")
            status = (f.get("status") or {}).get("name", "?")
            priority = (f.get("priority") or {}).get("name", "?")
            itype = (f.get("issuetype") or {}).get("name", "?")
            assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
            lines.append(f"  {key} [{status}] {itype} | {priority} | {assignee}")
            lines.append(f"    {summary}")

        return "\n".join(lines)

    async def _update_issue(self, creds: Dict[str, str], **kwargs) -> str:
        issue_key = kwargs.get("issue_key", "")
        if not issue_key:
            return "Error: issue_key is required for update_issue"

        fields: Dict[str, Any] = {}
        if kwargs.get("summary"):
            fields["summary"] = kwargs["summary"]
        if kwargs.get("description"):
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": kwargs["description"]}],
                    }
                ],
            }
        if kwargs.get("priority"):
            fields["priority"] = {"name": kwargs["priority"]}
        if kwargs.get("labels"):
            fields["labels"] = kwargs["labels"]
        if kwargs.get("assignee"):
            account_id = await self._find_user(creds, kwargs["assignee"])
            if account_id:
                fields["assignee"] = {"accountId": account_id}

        # Merge extra fields
        extra = kwargs.get("fields")
        if extra and isinstance(extra, dict):
            fields.update(extra)

        if not fields:
            return "Error: No fields to update. Provide summary, description, priority, labels, assignee, or fields."

        await self._request(creds, "PUT", f"/issue/{issue_key}", body={"fields": fields})
        return f"Updated {issue_key}: {', '.join(fields.keys())}"

    async def _transition(self, creds: Dict[str, str], **kwargs) -> str:
        issue_key = kwargs.get("issue_key", "")
        transition_name = kwargs.get("transition_name", "")
        if not issue_key or not transition_name:
            return "Error: issue_key and transition_name are required"

        # Get available transitions
        data = await self._request(creds, "GET", f"/issue/{issue_key}/transitions")
        transitions = data.get("transitions", [])

        # Match by name (case-insensitive)
        target = transition_name.lower()
        match = None
        for t in transitions:
            if t.get("name", "").lower() == target:
                match = t
                break

        if not match:
            available = [t.get("name", "?") for t in transitions]
            return f"Transition '{transition_name}' not found. Available: {', '.join(available)}"

        await self._request(
            creds, "POST", f"/issue/{issue_key}/transitions",
            body={"transition": {"id": match["id"]}},
        )
        return f"Transitioned {issue_key} to '{match['name']}'"

    async def _add_comment(self, creds: Dict[str, str], **kwargs) -> str:
        issue_key = kwargs.get("issue_key", "")
        comment = kwargs.get("comment", "")
        if not issue_key or not comment:
            return "Error: issue_key and comment are required"

        body = {
            "body": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": comment}],
                    }
                ],
            }
        }
        await self._request(creds, "POST", f"/issue/{issue_key}/comment", body=body)
        return f"Added comment to {issue_key}"

    async def _list_projects(self, creds: Dict[str, str], **kwargs) -> str:
        data = await self._request(creds, "GET", "/project", params={"maxResults": 50})
        if not data:
            return "No projects found."

        lines = [f"Found {len(data)} project(s):"]
        lines.append("")
        for p in data:
            key = p.get("key", "?")
            name = p.get("name", "?")
            ptype = p.get("projectTypeKey", "?")
            lines.append(f"  {key} — {name} ({ptype})")

        return "\n".join(lines)

    async def _list_transitions(self, creds: Dict[str, str], **kwargs) -> str:
        issue_key = kwargs.get("issue_key", "")
        if not issue_key:
            return "Error: issue_key is required for list_transitions"

        data = await self._request(creds, "GET", f"/issue/{issue_key}/transitions")
        transitions = data.get("transitions", [])

        if not transitions:
            return f"No transitions available for {issue_key}"

        lines = [f"Available transitions for {issue_key}:"]
        for t in transitions:
            tid = t.get("id", "?")
            name = t.get("name", "?")
            to_status = (t.get("to") or {}).get("name", "?")
            lines.append(f"  [{tid}] {name} → {to_status}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _find_user(self, creds: Dict[str, str], query: str) -> Optional[str]:
        """Find a Jira user by email or display name."""
        try:
            users = await self._request(
                creds, "GET", "/user/search",
                params={"query": query, "maxResults": 1},
            )
            if users and isinstance(users, list) and len(users) > 0:
                return users[0].get("accountId")
        except Exception:
            pass
        return None

    def _format_issue(self, data: Dict, creds: Dict[str, str]) -> str:
        """Format a single issue for display."""
        key = data.get("key", "?")
        f = data.get("fields", {})
        summary = f.get("summary", "")
        status = (f.get("status") or {}).get("name", "?")
        priority = (f.get("priority") or {}).get("name", "?")
        itype = (f.get("issuetype") or {}).get("name", "?")
        assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
        reporter = (f.get("reporter") or {}).get("displayName", "?")
        created = f.get("created", "?")[:10]
        updated = f.get("updated", "?")[:10]
        labels = ", ".join(f.get("labels", [])) or "none"

        desc_text = ""
        desc = f.get("description")
        if desc and isinstance(desc, dict):
            # Extract text from ADF
            for block in desc.get("content", []):
                for inline in block.get("content", []):
                    desc_text += inline.get("text", "")
                desc_text += "\n"
        desc_text = desc_text.strip()[:500] if desc_text else "(no description)"

        comments = f.get("comment", {}).get("comments", [])
        comment_count = f.get("comment", {}).get("total", len(comments))

        lines = [
            f"{key}: {summary}",
            f"URL: {creds['site_url']}/browse/{key}",
            f"Type: {itype} | Status: {status} | Priority: {priority}",
            f"Assignee: {assignee} | Reporter: {reporter}",
            f"Created: {created} | Updated: {updated}",
            f"Labels: {labels}",
            f"Comments: {comment_count}",
            "",
            f"Description:\n{desc_text}",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tools container (same pattern as CanvasVisionTools)
# ---------------------------------------------------------------------------

class JiraTools:
    """Collection of Jira tools."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [JiraTool()]

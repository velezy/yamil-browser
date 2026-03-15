"""
General Agent Tools

Tools available to the General AI agent, including URL fetching
for API spec import workflows.
"""

import ipaddress
import logging
import re
from typing import Any, Dict, List
from urllib.parse import urlparse

import httpx
import yaml

from assemblyline_common.ai.authorization import AuthorizationContext, Permission
from assemblyline_common.ai.tools.base import Tool, ToolDefinition, ToolResult

logger = logging.getLogger(__name__)

# Maximum response size: 500KB
MAX_RESPONSE_SIZE = 500 * 1024
# Request timeout: 10 seconds
REQUEST_TIMEOUT = 10


def _is_private_ip(hostname: str) -> bool:
    """Check if hostname resolves to a private/localhost IP."""
    try:
        addr = ipaddress.ip_address(hostname)
        return addr.is_private or addr.is_loopback or addr.is_reserved
    except ValueError:
        # Not a raw IP — check for obvious localhost names
        return hostname.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0")


def _strip_html_tags(html: str) -> str:
    """Strip HTML tags and return plain text content."""
    # Remove script and style blocks
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


class FetchUrlTool(Tool):
    """Fetch content from a URL (API specs, documentation pages)."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="fetch_url",
            description=(
                "Fetch content from a URL. Use to read OpenAPI/Swagger specs, "
                "FHIR IGs, or API documentation."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to fetch",
                    },
                    "format_hint": {
                        "type": "string",
                        "enum": ["auto", "json", "yaml", "html"],
                        "description": "Expected format (default: auto-detect)",
                    },
                },
                "required": ["url"],
            },
            required_permission=Permission.USE_AI,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        **kwargs: Any,
    ) -> ToolResult:
        url = kwargs.get("url", "")
        format_hint = kwargs.get("format_hint", "auto")

        if not url:
            return ToolResult(success=False, error="URL is required")

        # Validate URL scheme
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return ToolResult(
                success=False,
                error=f"Only http/https URLs are supported, got: {parsed.scheme}",
            )

        # Block private/localhost IPs
        hostname = parsed.hostname or ""
        if _is_private_ip(hostname):
            return ToolResult(
                success=False,
                error="Cannot fetch from private or localhost addresses",
            )

        try:
            async with httpx.AsyncClient(
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
                max_redirects=5,
            ) as client:
                resp = await client.get(
                    url,
                    headers={"Accept": "application/json, application/yaml, text/yaml, text/html, */*"},
                )

                if resp.status_code != 200:
                    return ToolResult(
                        success=False,
                        error=f"HTTP {resp.status_code}: {resp.reason_phrase}",
                    )

                content_type = resp.headers.get("content-type", "")
                raw = resp.content

                # Enforce size limit
                truncated = False
                if len(raw) > MAX_RESPONSE_SIZE:
                    raw = raw[:MAX_RESPONSE_SIZE]
                    truncated = True

                text = raw.decode("utf-8", errors="replace")

                # Detect and parse format
                detected_format = format_hint
                if detected_format == "auto":
                    if "json" in content_type or text.lstrip().startswith("{") or text.lstrip().startswith("["):
                        detected_format = "json"
                    elif "yaml" in content_type or "yml" in content_type:
                        detected_format = "yaml"
                    elif "html" in content_type:
                        detected_format = "html"
                    else:
                        # Try JSON first, then YAML
                        detected_format = "text"

                result_data: Dict[str, Any] = {
                    "url": url,
                    "format": detected_format,
                    "truncated": truncated,
                }

                if detected_format == "json":
                    try:
                        import json
                        parsed_content = json.loads(text)
                        result_data["content"] = json.dumps(parsed_content, indent=2)
                        result_data["parsed"] = True
                    except (json.JSONDecodeError, ValueError):
                        result_data["content"] = text
                        result_data["parsed"] = False
                elif detected_format == "yaml":
                    try:
                        parsed_content = yaml.safe_load(text)
                        import json
                        result_data["content"] = json.dumps(parsed_content, indent=2)
                        result_data["parsed"] = True
                    except yaml.YAMLError:
                        result_data["content"] = text
                        result_data["parsed"] = False
                elif detected_format == "html":
                    result_data["content"] = _strip_html_tags(text)
                    result_data["parsed"] = True
                else:
                    result_data["content"] = text
                    result_data["parsed"] = False

                warning = ""
                if truncated:
                    warning = " (truncated to 500KB)"

                return ToolResult(
                    success=True,
                    data=result_data,
                    message=f"Fetched {detected_format} content from {url}{warning}",
                )

        except httpx.HTTPError as e:
            return ToolResult(success=False, error=f"Request failed: {str(e)}")
        except TimeoutError:
            return ToolResult(success=False, error=f"Request timed out after {REQUEST_TIMEOUT}s")
        except Exception as e:
            logger.exception(f"Unexpected error fetching URL: {url}")
            return ToolResult(success=False, error=f"Unexpected error: {str(e)}")


class GeneralTools:
    """Collection of tools for the General agent."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [
            FetchUrlTool(),
        ]

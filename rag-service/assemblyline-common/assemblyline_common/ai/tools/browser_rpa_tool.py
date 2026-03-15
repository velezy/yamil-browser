"""
Browser RPA Tool

Full RPA browser automation for the AI assistant. Uses Playwright to:
- Navigate to JS-rendered pages
- Fill forms and click buttons
- Handle login flows with stored credentials
- Extract content from rendered pages
- Take screenshots

Session management: one browser session per conversation,
auto-closes after inactivity timeout.
"""

import asyncio
import ipaddress
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from uuid import UUID

from assemblyline_common.ai.authorization import AuthorizationContext, Permission
from assemblyline_common.ai.tools.base import Tool, ToolDefinition, ToolResult

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

MAX_CONTENT_SIZE = 500 * 1024  # 500KB extracted content per step
SESSION_IDLE_TIMEOUT = 300  # 5 minutes
SESSION_MAX_LIFETIME = 900  # 15 minutes
MAX_PAGE_NAVIGATIONS = 20
MAX_SESSIONS_PER_TENANT = 3
MAX_SESSIONS_PER_HOUR = 5
DEFAULT_STEP_TIMEOUT = 10  # seconds
MAX_STEPS_PER_CALL = 10


# ============================================================================
# Session Management
# ============================================================================

class BrowserSession:
    """A managed Playwright browser session tied to a conversation."""

    def __init__(self, conversation_id: str, tenant_id: str):
        self.conversation_id = conversation_id
        self.tenant_id = tenant_id
        self.browser = None
        self.context = None
        self.page = None
        self.created_at = time.time()
        self.last_activity = time.time()
        self.page_navigation_count = 0
        self._closed = False

    @property
    def is_expired(self) -> bool:
        now = time.time()
        idle = now - self.last_activity > SESSION_IDLE_TIMEOUT
        lifetime = now - self.created_at > SESSION_MAX_LIFETIME
        return idle or lifetime

    def touch(self):
        self.last_activity = time.time()

    async def start(self):
        """Launch browser and create a new page."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright is not installed. Install with: pip install playwright && playwright install chromium"
            )

        self._pw = await async_playwright().start()
        self.browser = await self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
            ],
        )
        self.context = await self.browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        self.page = await self.context.new_page()
        self.touch()
        logger.info(f"Browser session started for conversation {self.conversation_id}")

    async def close(self):
        """Close browser and clean up resources."""
        if self._closed:
            return
        self._closed = True
        try:
            if self.page and not self.page.is_closed():
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, '_pw') and self._pw:
                await self._pw.stop()
        except Exception as e:
            logger.warning(f"Error closing browser session: {e}")
        logger.info(f"Browser session closed for conversation {self.conversation_id}")


class SessionManager:
    """Manages browser sessions across conversations."""

    def __init__(self):
        self._sessions: Dict[str, BrowserSession] = {}
        self._tenant_session_counts: Dict[str, int] = {}
        self._tenant_hourly_counts: Dict[str, List[float]] = {}

    def _check_rate_limit(self, tenant_id: str) -> Optional[str]:
        """Check tenant rate limits. Returns error message or None."""
        # Check concurrent sessions
        active_count = sum(
            1 for s in self._sessions.values()
            if s.tenant_id == tenant_id and not s.is_expired
        )
        if active_count >= MAX_SESSIONS_PER_TENANT:
            return f"Maximum {MAX_SESSIONS_PER_TENANT} concurrent browser sessions per tenant"

        # Check hourly rate
        now = time.time()
        hour_ago = now - 3600
        hourly = self._tenant_hourly_counts.get(tenant_id, [])
        hourly = [t for t in hourly if t > hour_ago]
        self._tenant_hourly_counts[tenant_id] = hourly
        if len(hourly) >= MAX_SESSIONS_PER_HOUR:
            return f"Maximum {MAX_SESSIONS_PER_HOUR} browser sessions per hour per tenant"

        return None

    async def get_or_create(
        self, conversation_id: str, tenant_id: str, action: str
    ) -> tuple[BrowserSession, Optional[str]]:
        """Get existing session or create new one. Returns (session, error)."""
        # Clean expired sessions
        await self._cleanup_expired()

        if action == "start":
            # Close existing session if any
            if conversation_id in self._sessions:
                await self._sessions[conversation_id].close()
                del self._sessions[conversation_id]

            # Rate limit check
            rate_error = self._check_rate_limit(tenant_id)
            if rate_error:
                return None, rate_error

            session = BrowserSession(conversation_id, tenant_id)
            await session.start()
            self._sessions[conversation_id] = session

            # Track hourly count
            hourly = self._tenant_hourly_counts.get(tenant_id, [])
            hourly.append(time.time())
            self._tenant_hourly_counts[tenant_id] = hourly

            return session, None

        elif action == "continue":
            session = self._sessions.get(conversation_id)
            if not session:
                return None, "No active browser session. Use session_action='start' first."
            if session.is_expired:
                await session.close()
                del self._sessions[conversation_id]
                return None, "Browser session expired due to inactivity. Start a new session."
            session.touch()
            return session, None

        elif action == "close":
            session = self._sessions.get(conversation_id)
            if session:
                await session.close()
                del self._sessions[conversation_id]
            return None, None  # No error, but no session to return (that's OK for close)

        return None, f"Unknown session_action: {action}"

    async def _cleanup_expired(self):
        """Clean up expired sessions."""
        expired = [
            cid for cid, s in self._sessions.items() if s.is_expired
        ]
        for cid in expired:
            logger.info(f"Cleaning up expired browser session: {cid}")
            await self._sessions[cid].close()
            del self._sessions[cid]


# Singleton session manager
_session_manager: Optional[SessionManager] = None


def get_session_manager() -> SessionManager:
    global _session_manager
    if _session_manager is None:
        _session_manager = SessionManager()
    return _session_manager


# ============================================================================
# Safety Checks
# ============================================================================

def _is_blocked_url(url: str) -> Optional[str]:
    """Check if URL targets a blocked network. Returns error message or None."""
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""

        # Block non-HTTP schemes
        if parsed.scheme not in ("http", "https"):
            return f"Only http/https URLs supported, got: {parsed.scheme}"

        # Block private/internal IPs
        try:
            addr = ipaddress.ip_address(hostname)
            if addr.is_private or addr.is_loopback or addr.is_reserved:
                return "Cannot navigate to private or internal network addresses"
        except ValueError:
            # Not a raw IP — check for obvious localhost names
            if hostname.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
                return "Cannot navigate to localhost"

        # Block common internal hostnames
        if any(hostname.endswith(suffix) for suffix in [".internal", ".local", ".corp"]):
            return "Cannot navigate to internal network addresses"

        return None
    except Exception:
        return "Invalid URL"


# ============================================================================
# Step Execution
# ============================================================================

async def _execute_step(
    session: BrowserSession,
    step: Dict[str, Any],
    credential_values: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Execute a single browser action step. Returns step result dict."""
    action = step.get("action", "")
    page = session.page
    timeout_ms = int(step.get("timeout", DEFAULT_STEP_TIMEOUT) * 1000)
    result = {"action": action, "success": True}

    try:
        if action == "navigate":
            url = step.get("url", "")
            if not url:
                return {"action": action, "success": False, "error": "URL is required for navigate"}

            blocked = _is_blocked_url(url)
            if blocked:
                return {"action": action, "success": False, "error": blocked}

            session.page_navigation_count += 1
            if session.page_navigation_count > MAX_PAGE_NAVIGATIONS:
                return {
                    "action": action,
                    "success": False,
                    "error": f"Maximum {MAX_PAGE_NAVIGATIONS} page navigations per session exceeded",
                }

            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            result["url"] = page.url
            result["title"] = await page.title()

        elif action == "click":
            selector = step.get("selector", "")
            if not selector:
                return {"action": action, "success": False, "error": "selector required for click"}

            # Try CSS selector first, then text-based
            try:
                await page.click(selector, timeout=timeout_ms)
            except Exception:
                # Fallback: try finding by text
                await page.get_by_text(selector, exact=False).first.click(timeout=timeout_ms)
            result["clicked"] = selector

        elif action == "fill":
            selector = step.get("selector", "")
            value = step.get("value", "")

            # If credential_values provided, check for placeholder substitution
            if credential_values and value.startswith("{{") and value.endswith("}}"):
                key = value[2:-2].strip()
                if key in credential_values:
                    value = credential_values[key]

            if not selector:
                return {"action": action, "success": False, "error": "selector required for fill"}

            try:
                await page.fill(selector, value, timeout=timeout_ms)
            except Exception:
                # Fallback: try finding by label/placeholder text
                try:
                    await page.get_by_label(selector).fill(value, timeout=timeout_ms)
                except Exception:
                    await page.get_by_placeholder(selector).fill(value, timeout=timeout_ms)
            result["filled"] = selector
            # Never return the actual value (could be a credential)
            result["value_applied"] = True

        elif action == "select":
            selector = step.get("selector", "")
            value = step.get("value", "")
            if not selector:
                return {"action": action, "success": False, "error": "selector required for select"}
            await page.select_option(selector, value, timeout=timeout_ms)
            result["selected"] = selector

        elif action == "wait":
            wait_for = step.get("wait_for", "")
            # Recognize page load states (often sent by LLM as wait_for values)
            load_states = {"networkidle", "load", "domcontentloaded"}
            if not wait_for or wait_for.lower().strip() in load_states:
                state = wait_for.lower().strip() if wait_for else "networkidle"
                await page.wait_for_load_state(state, timeout=timeout_ms)
            else:
                try:
                    # Try as selector first
                    await page.wait_for_selector(wait_for, timeout=timeout_ms)
                except Exception:
                    # Try as text
                    await page.get_by_text(wait_for, exact=False).first.wait_for(
                        state="visible", timeout=timeout_ms
                    )
            result["waited_for"] = wait_for or "networkidle"

        elif action == "screenshot":
            screenshot_bytes = await page.screenshot(type="png")
            import base64
            result["screenshot_base64"] = base64.b64encode(screenshot_bytes).decode()

        elif action == "extract":
            # Get page text content via accessibility snapshot
            try:
                snapshot = await page.accessibility.snapshot()
                text_content = _flatten_accessibility_tree(snapshot) if snapshot else ""
            except Exception:
                text_content = await page.inner_text("body")

            # Enforce size limit
            if len(text_content) > MAX_CONTENT_SIZE:
                text_content = text_content[:MAX_CONTENT_SIZE] + "\n...(truncated)"

            result["content"] = text_content
            result["url"] = page.url
            result["title"] = await page.title()

        elif action == "scroll":
            direction = step.get("value", "down")
            if direction == "down":
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
            elif direction == "up":
                await page.evaluate("window.scrollBy(0, -window.innerHeight)")
            elif direction == "top":
                await page.evaluate("window.scrollTo(0, 0)")
            elif direction == "bottom":
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            result["scrolled"] = direction

        else:
            return {"action": action, "success": False, "error": f"Unknown action: {action}"}

    except Exception as e:
        result["success"] = False
        result["error"] = str(e)

    return result


def _flatten_accessibility_tree(node: Dict[str, Any], depth: int = 0) -> str:
    """Flatten Playwright accessibility tree into readable text."""
    parts = []
    role = node.get("role", "")
    name = node.get("name", "")
    value = node.get("value", "")

    # Build text representation
    if name:
        indent = "  " * min(depth, 4)
        label = f"[{role}]" if role and role not in ("none", "generic", "text") else ""
        text = f"{indent}{label} {name}"
        if value:
            text += f": {value}"
        parts.append(text.strip())

    # Process children
    for child in node.get("children", []):
        child_text = _flatten_accessibility_tree(child, depth + 1)
        if child_text:
            parts.append(child_text)

    return "\n".join(parts)


# ============================================================================
# Credential Loading
# ============================================================================

async def _load_credentials(
    credential_id: str, tenant_id: str, db
) -> Optional[Dict[str, str]]:
    """
    Load RPA credentials.

    Tries AWS Secrets Manager first, falls back to Fernet-encrypted DB columns
    if SM is unavailable (local dev, SM down).
    """
    # First, load the non-secret metadata from DB (site_url, mfa_type, login_steps)
    try:
        from sqlalchemy import text as sql_text
        result = await db.execute(
            sql_text("""
                SELECT username_encrypted, password_encrypted, totp_secret_encrypted,
                       login_steps, site_url, mfa_type
                FROM common.rpa_credentials
                WHERE id = :id AND tenant_id = :tenant_id
            """),
            {"id": credential_id, "tenant_id": tenant_id},
        )
        row = result.fetchone()
        if not row:
            return None
    except Exception as e:
        logger.error(f"Failed to load RPA credential metadata: {e}")
        return None

    creds = {
        "site_url": row.site_url,
        "mfa_type": row.mfa_type or "none",
    }

    if row.login_steps:
        creds["login_steps"] = row.login_steps

    # Try AWS Secrets Manager first
    sm_loaded = False
    try:
        from assemblyline_common.secrets_manager import get_secrets_manager
        sm = get_secrets_manager()
        secret_name = f"yamil/{tenant_id}/rpa-credentials/{credential_id}"
        sm_data = await sm.get_credentials(secret_name)
        if sm_data:
            creds["username"] = sm_data.get("username", "")
            creds["password"] = sm_data.get("password", "")
            if sm_data.get("totp_secret"):
                creds["totp_secret"] = sm_data["totp_secret"]
            sm_loaded = True
            logger.debug(f"Loaded RPA credentials from AWS SM for {credential_id}")
    except Exception as e:
        logger.warning(f"AWS SM unavailable for RPA credentials, falling back to DB: {e}")

    # Fallback to Fernet-encrypted DB columns
    if not sm_loaded:
        try:
            from assemblyline_common.encryption import decrypt_value
            creds["username"] = decrypt_value(row.username_encrypted)
            creds["password"] = decrypt_value(row.password_encrypted)
            if row.totp_secret_encrypted:
                creds["totp_secret"] = decrypt_value(row.totp_secret_encrypted)
            logger.debug(f"Loaded RPA credentials from DB (Fernet) for {credential_id}")
        except Exception as e:
            logger.error(f"Failed to decrypt RPA credentials from DB: {e}")
            return None

    return creds


# ============================================================================
# BrowserRPATool
# ============================================================================

class BrowserRPATool(Tool):
    """
    Full RPA browser automation tool.

    Opens a real browser, navigates pages, fills forms, clicks buttons,
    and extracts content. Supports login flows with stored credentials.
    """

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="browser_rpa",
            description=(
                "Open a browser, navigate pages, fill forms, click buttons, "
                "and extract content. Use for JS-rendered pages or sites requiring login. "
                "When fetch_url returns only an HTML shell without meaningful content, "
                "use this tool instead."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "session_action": {
                        "type": "string",
                        "enum": ["start", "continue", "close"],
                        "description": (
                            "start=new browser session, "
                            "continue=use existing session, "
                            "close=end session and free resources"
                        ),
                    },
                    "steps": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "action": {
                                    "type": "string",
                                    "enum": [
                                        "navigate", "click", "fill", "select",
                                        "wait", "screenshot", "extract", "scroll",
                                    ],
                                },
                                "selector": {
                                    "type": "string",
                                    "description": "CSS selector or text content to find element",
                                },
                                "value": {
                                    "type": "string",
                                    "description": "Value for fill/select actions, or direction for scroll",
                                },
                                "url": {
                                    "type": "string",
                                    "description": "URL for navigate action",
                                },
                                "wait_for": {
                                    "type": "string",
                                    "description": "Text or selector to wait for after action",
                                },
                                "timeout": {
                                    "type": "number",
                                    "description": "Timeout in seconds (default 10)",
                                },
                            },
                            "required": ["action"],
                        },
                        "description": "Sequence of browser actions to execute",
                    },
                    "credential_id": {
                        "type": "string",
                        "description": "ID of stored RPA credential for auto-login",
                    },
                },
                "required": ["session_action", "steps"],
            },
            required_permission=Permission.USE_AI,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        **kwargs: Any,
    ) -> ToolResult:
        session_action = kwargs.get("session_action", "start")
        steps = kwargs.get("steps", [])
        credential_id = kwargs.get("credential_id")
        conversation_id = kwargs.get("_conversation_id", "default")
        db = kwargs.get("_db")

        tenant_id = str(auth_context.tenant_id)

        # Validate steps count
        if len(steps) > MAX_STEPS_PER_CALL:
            return ToolResult(
                success=False,
                error=f"Maximum {MAX_STEPS_PER_CALL} steps per call",
            )

        # Handle close action
        if session_action == "close":
            mgr = get_session_manager()
            await mgr.get_or_create(conversation_id, tenant_id, "close")
            return ToolResult(
                success=True,
                data={"status": "session_closed"},
                message="Browser session closed.",
            )

        # Get or create session
        mgr = get_session_manager()
        session, error = await mgr.get_or_create(conversation_id, tenant_id, session_action)
        if error:
            return ToolResult(success=False, error=error)
        if not session:
            return ToolResult(success=False, error="Failed to create browser session")

        # Load credentials if needed
        credential_values = None
        if credential_id and db:
            credential_values = await _load_credentials(credential_id, tenant_id, db)
            if not credential_values:
                return ToolResult(
                    success=False,
                    error=f"Credential '{credential_id}' not found or could not be decrypted",
                )

        # Execute steps
        step_results = []
        for i, step in enumerate(steps):
            logger.info(
                f"RPA step {i + 1}/{len(steps)}: {step.get('action')} "
                f"(conversation={conversation_id})"
            )
            step_result = await _execute_step(session, step, credential_values)
            step_results.append(step_result)

            # Log failures but continue with remaining steps (partial results are better than none)
            if not step_result.get("success", False):
                logger.warning(f"RPA step {i + 1} failed: {step_result.get('error')}")
                # Only abort on navigate/click failures (page state is unreliable)
                if step.get("action") in ("navigate", "click", "fill"):
                    break

        # Build final page state snapshot
        page_state = {}
        try:
            if session.page and not session.page.is_closed():
                page_state = {
                    "url": session.page.url,
                    "title": await session.page.title(),
                }
                # Get accessibility snapshot for page content
                try:
                    snapshot = await session.page.accessibility.snapshot()
                    if snapshot:
                        page_text = _flatten_accessibility_tree(snapshot)
                        if len(page_text) > MAX_CONTENT_SIZE:
                            page_text = page_text[:MAX_CONTENT_SIZE] + "\n...(truncated)"
                        page_state["page_content"] = page_text
                except Exception:
                    pass
        except Exception:
            pass

        all_success = all(r.get("success", False) for r in step_results)
        # Consider partial success if at least navigate succeeded and we have page content
        has_content = bool(page_state.get("page_content"))
        effective_success = all_success or has_content

        # Check for MFA prompt indicators
        mfa_detected = False
        if page_state.get("page_content"):
            content_lower = page_state["page_content"].lower()
            mfa_keywords = [
                "verification code", "mfa", "two-factor", "2fa",
                "authenticator", "one-time", "otp", "security code",
            ]
            mfa_detected = any(kw in content_lower for kw in mfa_keywords)

        result_data = {
            "session_action": session_action,
            "steps_executed": len(step_results),
            "steps_succeeded": sum(1 for r in step_results if r.get("success")),
            "step_results": step_results,
            "page_state": page_state,
        }

        if mfa_detected and credential_values and credential_values.get("totp_secret"):
            # Auto-generate TOTP code and fill it
            try:
                import pyotp
                totp = pyotp.TOTP(credential_values["totp_secret"])
                code = totp.now()

                auto_fill_steps = [
                    {
                        "action": "fill",
                        "selector": "input[type='text'], input[name*='code'], input[name*='otp'], input[name*='mfa'], input[name*='token'], input[name*='verification']",
                        "value": code,
                    },
                    {
                        "action": "click",
                        "selector": "button[type='submit'], button:has-text('Verify'), button:has-text('Submit'), button:has-text('Continue')",
                    },
                    {"action": "wait", "timeout": 3},
                ]

                for step in auto_fill_steps:
                    auto_result = await _execute_step(session, step, None)
                    step_results.append(auto_result)

                # Re-snapshot the page after auto-fill
                try:
                    if session.page and not session.page.is_closed():
                        page_state["url"] = session.page.url
                        page_state["title"] = await session.page.title()
                        snapshot = await session.page.accessibility.snapshot()
                        if snapshot:
                            page_text = _flatten_accessibility_tree(snapshot)
                            if len(page_text) > MAX_CONTENT_SIZE:
                                page_text = page_text[:MAX_CONTENT_SIZE] + "\n...(truncated)"
                            page_state["page_content"] = page_text
                except Exception:
                    pass

                result_data["mfa_auto_filled"] = True
                result_data["mfa_message"] = "TOTP code was auto-generated and submitted."
                logger.info(f"TOTP auto-filled for conversation {conversation_id}")
            except Exception as e:
                logger.warning(f"TOTP auto-fill failed, falling back to manual: {e}")
                result_data["mfa_required"] = True
                result_data["mfa_message"] = (
                    "The page appears to require MFA/2FA verification. "
                    "TOTP auto-fill failed. Ask the user for the verification code, "
                    "then use browser_rpa with session_action='continue' to fill the code and submit."
                )
        elif mfa_detected:
            result_data["mfa_required"] = True
            result_data["mfa_message"] = (
                "The page appears to require MFA/2FA verification. "
                "Ask the user for the verification code, then use browser_rpa "
                "with session_action='continue' to fill the code and submit."
            )

        self.log_execution(auth_context, "browser_rpa", {
            "session_action": session_action,
            "steps_count": len(steps),
            "success": effective_success,
        }, ToolResult(success=effective_success))

        return ToolResult(
            success=effective_success,
            data=result_data,
            message=(
                f"Executed {len(step_results)} browser steps. "
                f"Current page: {page_state.get('title', 'unknown')}"
            ),
        )


class BrowserRPATools:
    """Collection of browser RPA tools for the General agent."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [
            BrowserRPATool(),
        ]

"""
Canvas Vision Tool

AI's own browser for viewing and learning the app's internal UI.
Unlike browser_rpa (which browses external sites), this tool navigates to
internal Docker URLs (envoy-internal) with auto-bridged auth to:
- View flow canvases and take screenshots
- Extract accessibility snapshots for UI reasoning
- Verify canvas layout (disconnected nodes, label truncation)
- Learn page structure and suggest orchestrator improvements

Screenshots are returned as base64 image UIBlocks so the user
sees exactly what the AI sees in the chat panel.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from assemblyline_common.ai.authorization import AuthorizationContext, Permission, Role
from assemblyline_common.ai.tools.base import Tool, ToolDefinition, ToolResult

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

# Internal app URL — the Docker-internal Envoy proxy
INTERNAL_APP_URL = os.environ.get("INTERNAL_APP_URL", "http://envoy-internal:9080")

# Session limits (lighter than browser_rpa since this is internal only)
VISION_SESSION_IDLE_TIMEOUT = 300   # 5 minutes
VISION_SESSION_MAX_LIFETIME = 600   # 10 minutes
MAX_VISION_SESSIONS = 2             # max concurrent per tenant

# Browser pool settings
BROWSER_POOL_SIZE = int(os.environ.get("VISION_POOL_SIZE", "3"))
BROWSER_POOL_WARMUP = os.environ.get("VISION_POOL_WARMUP", "false").lower() == "true"

# Screenshot settings
VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 1080
MAX_SCREENSHOT_SIZE = 2 * 1024 * 1024  # 2MB limit for base64

# Screenshot encryption + TTL
SCREENSHOT_TTL_SECONDS = int(os.environ.get("SCREENSHOT_TTL", "300"))  # 5 min default
SCREENSHOT_ENCRYPTION_KEY = os.environ.get("SCREENSHOT_ENCRYPTION_KEY", "")  # Fernet key

# OmniParser settings
OMNIPARSER_ENDPOINT = os.environ.get("OMNIPARSER_ENDPOINT", "")  # HuggingFace inference API URL
OMNIPARSER_API_KEY = os.environ.get("OMNIPARSER_API_KEY", "")  # HF API token

# CloudWatch audit settings
CW_LOG_GROUP = os.environ.get("VISION_AUDIT_LOG_GROUP", "/yamil/ai/canvas-vision")
CW_LOG_STREAM_PREFIX = os.environ.get("VISION_AUDIT_LOG_STREAM", "audit")


# ============================================================================
# RBAC — Role-Based Access Control for Canvas Touch Actions
# ============================================================================

class VisionPermissionLevel(str, Enum):
    """Permission tiers for canvas_vision actions."""
    VIEW = "view"           # screenshot, view_flow, view_page, verify_canvas, learn_page
    INTERACT = "interact"   # click, fill, select, hover, scroll
    MODIFY = "modify"       # drag (moves nodes), destructive actions

# Map roles to their maximum vision permission level
_ROLE_VISION_PERMISSIONS: Dict[str, VisionPermissionLevel] = {
    "viewer": VisionPermissionLevel.VIEW,
    "operator": VisionPermissionLevel.VIEW,
    "developer": VisionPermissionLevel.INTERACT,
    "editor": VisionPermissionLevel.INTERACT,
    "user": VisionPermissionLevel.INTERACT,
    "admin": VisionPermissionLevel.MODIFY,
    "super_admin": VisionPermissionLevel.MODIFY,
    "compliance": VisionPermissionLevel.VIEW,
}

# Map actions to the minimum permission level required
_ACTION_PERMISSION_LEVEL: Dict[str, VisionPermissionLevel] = {
    # View actions — anyone can see
    "view_flow": VisionPermissionLevel.VIEW,
    "view_page": VisionPermissionLevel.VIEW,
    "screenshot": VisionPermissionLevel.VIEW,
    "verify_canvas": VisionPermissionLevel.VIEW,
    "learn_page": VisionPermissionLevel.VIEW,
    "close": VisionPermissionLevel.VIEW,
    # Interact actions — editors and above
    "click": VisionPermissionLevel.INTERACT,
    "fill": VisionPermissionLevel.INTERACT,
    "select": VisionPermissionLevel.INTERACT,
    "hover": VisionPermissionLevel.INTERACT,
    "scroll": VisionPermissionLevel.INTERACT,
    # Modify actions — admin only
    "drag": VisionPermissionLevel.MODIFY,
}

# Permission level ordering for comparison
_PERMISSION_ORDER = {
    VisionPermissionLevel.VIEW: 0,
    VisionPermissionLevel.INTERACT: 1,
    VisionPermissionLevel.MODIFY: 2,
}


def check_vision_rbac(auth_context: AuthorizationContext, action: str) -> Optional[str]:
    """Check if user's role allows the requested canvas_vision action.

    Returns None if allowed, or an error message if denied.
    """
    role_str = auth_context.role.value if isinstance(auth_context.role, Enum) else str(auth_context.role)
    user_level = _ROLE_VISION_PERMISSIONS.get(role_str, VisionPermissionLevel.VIEW)
    required_level = _ACTION_PERMISSION_LEVEL.get(action, VisionPermissionLevel.VIEW)

    if _PERMISSION_ORDER[user_level] < _PERMISSION_ORDER[required_level]:
        return (
            f"Permission denied: '{action}' requires {required_level.value} level, "
            f"but your role '{role_str}' has {user_level.value} level. "
            f"Contact an admin to upgrade your permissions."
        )
    return None


# ============================================================================
# Screenshot Encryption + TTL Cache
# ============================================================================

class ScreenshotCache:
    """Encrypted screenshot cache with TTL-based auto-expiry.

    Screenshots may contain PHI (healthcare data visible on screen).
    This cache:
    - Encrypts screenshots at rest using Fernet symmetric encryption
    - Auto-expires entries after TTL_SECONDS
    - Never persists to disk — memory only
    - Limits total cache size to prevent memory bloat
    """

    def __init__(self, ttl_seconds: int = SCREENSHOT_TTL_SECONDS, max_entries: int = 50):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._cache: Dict[str, Dict[str, Any]] = {}  # key -> {data, expires_at}
        self._fernet = None

        # Initialize Fernet encryption if key is provided
        if SCREENSHOT_ENCRYPTION_KEY:
            try:
                from cryptography.fernet import Fernet
                self._fernet = Fernet(SCREENSHOT_ENCRYPTION_KEY.encode())
                logger.info("Screenshot encryption enabled (Fernet)")
            except ImportError:
                logger.warning("cryptography package not installed — screenshots stored unencrypted")
            except Exception as e:
                logger.warning(f"Invalid encryption key — screenshots stored unencrypted: {e}")

    def store(self, key: str, screenshot_b64: str) -> str:
        """Store an encrypted screenshot. Returns the cache key."""
        self._evict_expired()

        # Enforce max entries
        if len(self._cache) >= self.max_entries:
            oldest_key = min(self._cache, key=lambda k: self._cache[k]["expires_at"])
            del self._cache[oldest_key]

        # Encrypt if available
        if self._fernet:
            encrypted = self._fernet.encrypt(screenshot_b64.encode()).decode()
        else:
            encrypted = screenshot_b64

        self._cache[key] = {
            "data": encrypted,
            "encrypted": self._fernet is not None,
            "expires_at": time.time() + self.ttl,
            "stored_at": time.time(),
        }
        return key

    def retrieve(self, key: str) -> Optional[str]:
        """Retrieve and decrypt a screenshot. Returns None if expired or not found."""
        self._evict_expired()
        entry = self._cache.get(key)
        if not entry:
            return None

        if entry["encrypted"] and self._fernet:
            try:
                return self._fernet.decrypt(entry["data"].encode()).decode()
            except Exception:
                logger.warning(f"Failed to decrypt screenshot {key}")
                return None
        return entry["data"]

    def invalidate(self, key: str):
        """Manually invalidate a cached screenshot."""
        self._cache.pop(key, None)

    def clear(self):
        """Clear all cached screenshots."""
        self._cache.clear()

    def _evict_expired(self):
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, v in self._cache.items() if v["expires_at"] <= now]
        for k in expired:
            del self._cache[k]

    @property
    def stats(self) -> Dict[str, Any]:
        self._evict_expired()
        return {
            "entries": len(self._cache),
            "max_entries": self.max_entries,
            "ttl_seconds": self.ttl,
            "encrypted": self._fernet is not None,
        }


# Singleton
_screenshot_cache: Optional[ScreenshotCache] = None

def get_screenshot_cache() -> ScreenshotCache:
    global _screenshot_cache
    if _screenshot_cache is None:
        _screenshot_cache = ScreenshotCache()
    return _screenshot_cache


# ============================================================================
# Circuit Breaker — prevents runaway retries on persistent failures
# ============================================================================

class CircuitState(str, Enum):
    CLOSED = "closed"       # Normal operation — requests pass through
    OPEN = "open"           # Tripped — all requests fail fast
    HALF_OPEN = "half_open" # Testing — allow one request to check if recovered


class CircuitBreaker:
    """Circuit breaker for canvas_vision browser operations.

    Prevents hammering a dead browser with retries:
    - CLOSED: normal operation, failures increment counter
    - OPEN: after N failures, all requests fail immediately
    - HALF_OPEN: after cooldown, allow one probe request
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,  # seconds before trying again
        name: str = "canvas_vision",
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float = 0
        self._success_count = 0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            # Check if cooldown has elapsed
            if time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(f"[CircuitBreaker:{self.name}] OPEN -> HALF_OPEN (cooldown elapsed)")
        return self._state

    def allow_request(self) -> bool:
        """Check if a request should be allowed through."""
        current = self.state
        if current == CircuitState.CLOSED:
            return True
        if current == CircuitState.HALF_OPEN:
            return True  # Allow probe request
        return False  # OPEN — fail fast

    def record_success(self):
        """Record a successful operation."""
        self._success_count += 1
        if self._state == CircuitState.HALF_OPEN:
            # Probe succeeded — recover
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            logger.info(f"[CircuitBreaker:{self.name}] HALF_OPEN -> CLOSED (recovered)")
        elif self._failure_count > 0:
            self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self, error: str = ""):
        """Record a failed operation."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Probe failed — back to open
            self._state = CircuitState.OPEN
            logger.warning(f"[CircuitBreaker:{self.name}] HALF_OPEN -> OPEN (probe failed: {error})")
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                f"[CircuitBreaker:{self.name}] CLOSED -> OPEN "
                f"(threshold {self.failure_threshold} reached: {error})"
            )

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "failures": self._failure_count,
            "threshold": self.failure_threshold,
            "successes": self._success_count,
            "recovery_timeout": self.recovery_timeout,
        }


# Singleton
_circuit_breaker: Optional[CircuitBreaker] = None

def get_circuit_breaker() -> CircuitBreaker:
    global _circuit_breaker
    if _circuit_breaker is None:
        _circuit_breaker = CircuitBreaker()
    return _circuit_breaker


# ============================================================================
# CloudWatch Audit Trail
# ============================================================================

class CloudWatchAuditWriter:
    """Sends canvas_vision audit entries to CloudWatch Logs for enterprise compliance.

    Falls back gracefully if boto3/CloudWatch is unavailable.
    Batches log events and flushes periodically to reduce API calls.
    """

    def __init__(self, log_group: str = CW_LOG_GROUP, stream_prefix: str = CW_LOG_STREAM_PREFIX):
        self.log_group = log_group
        self.stream_prefix = stream_prefix
        self._client = None
        self._stream_name: Optional[str] = None
        self._sequence_token: Optional[str] = None
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_limit = 25
        self._initialized = False
        self._available = False

    def _ensure_init(self):
        """Lazy-initialize CloudWatch client and log stream."""
        if self._initialized:
            return
        self._initialized = True
        try:
            import boto3
            self._client = boto3.client("logs")
            # Create stream name with date prefix for easy partitioning
            from datetime import datetime
            date_str = datetime.utcnow().strftime("%Y/%m/%d")
            self._stream_name = f"{self.stream_prefix}/{date_str}/{os.getpid()}"

            # Ensure log group exists
            try:
                self._client.create_log_group(logGroupName=self.log_group)
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass
            except Exception:
                pass  # May not have permission — that's fine, stream may already exist

            # Ensure log stream exists
            try:
                self._client.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=self._stream_name,
                )
            except Exception:
                pass

            self._available = True
            logger.info(f"CloudWatch audit trail initialized: {self.log_group}/{self._stream_name}")
        except ImportError:
            logger.debug("boto3 not available — CloudWatch audit trail disabled")
        except Exception as e:
            logger.warning(f"CloudWatch audit trail init failed: {e}")

    def log(self, entry: Dict[str, Any]):
        """Buffer an audit entry for CloudWatch. Auto-flushes when buffer is full."""
        self._ensure_init()
        if not self._available:
            return

        self._buffer.append({
            "timestamp": int(time.time() * 1000),
            "message": json.dumps(entry, default=str),
        })

        if len(self._buffer) >= self._buffer_limit:
            self.flush()

    def flush(self):
        """Send buffered entries to CloudWatch Logs."""
        if not self._available or not self._buffer:
            return

        try:
            params = {
                "logGroupName": self.log_group,
                "logStreamName": self._stream_name,
                "logEvents": sorted(self._buffer, key=lambda e: e["timestamp"]),
            }
            if self._sequence_token:
                params["sequenceToken"] = self._sequence_token

            response = self._client.put_log_events(**params)
            self._sequence_token = response.get("nextSequenceToken")
            self._buffer.clear()
        except Exception as e:
            logger.warning(f"CloudWatch flush failed: {e}")
            # Keep buffer — will retry next flush
            if len(self._buffer) > 100:
                self._buffer = self._buffer[-50:]  # Prevent unbounded growth


# Singleton
_cw_writer: Optional[CloudWatchAuditWriter] = None

def get_cw_audit_writer() -> CloudWatchAuditWriter:
    global _cw_writer
    if _cw_writer is None:
        _cw_writer = CloudWatchAuditWriter()
    return _cw_writer


# ============================================================================
# Screenshot Diffing — verify actions had expected effect
# ============================================================================

class ScreenshotDiffer:
    """Compares before/after screenshots to verify canvas_touch actions.

    Uses pixel-based comparison to detect changes. Returns a summary
    the AI can use to confirm its action worked.
    """

    @staticmethod
    def compute_hash(screenshot_b64: str) -> str:
        """Compute a fast hash of a screenshot for quick change detection."""
        return hashlib.md5(screenshot_b64.encode()).hexdigest()

    @staticmethod
    def compare(before_b64: str, after_b64: str) -> Dict[str, Any]:
        """Compare two screenshots and return diff summary.

        Returns:
            {changed: bool, change_percentage: float, hash_before, hash_after, summary}
        """
        hash_before = ScreenshotDiffer.compute_hash(before_b64)
        hash_after = ScreenshotDiffer.compute_hash(after_b64)

        if hash_before == hash_after:
            return {
                "changed": False,
                "change_percentage": 0.0,
                "hash_before": hash_before,
                "hash_after": hash_after,
                "summary": "No visual change detected — screenshot is identical before and after the action.",
            }

        # Try pixel-level comparison if PIL is available
        try:
            from PIL import Image
            import io

            img_before = Image.open(io.BytesIO(base64.b64decode(before_b64)))
            img_after = Image.open(io.BytesIO(base64.b64decode(after_b64)))

            # Resize to same dimensions if needed
            if img_before.size != img_after.size:
                img_after = img_after.resize(img_before.size)

            # Convert to RGB
            img_before = img_before.convert("RGB")
            img_after = img_after.convert("RGB")

            # Pixel-by-pixel comparison (downsampled for speed)
            thumb_size = (192, 108)  # 1/10th of 1920x1080
            t_before = img_before.resize(thumb_size)
            t_after = img_after.resize(thumb_size)

            pixels_before = list(t_before.getdata())
            pixels_after = list(t_after.getdata())
            total_pixels = len(pixels_before)
            changed_pixels = 0
            for pb, pa in zip(pixels_before, pixels_after):
                # Pixel is "changed" if any channel differs by more than 30
                if any(abs(a - b) > 30 for a, b in zip(pb, pa)):
                    changed_pixels += 1

            change_pct = round(100 * changed_pixels / total_pixels, 1) if total_pixels > 0 else 0

            if change_pct < 1:
                summary = "Minimal change detected (<1%) — possibly a cursor blink or subtle animation."
            elif change_pct < 10:
                summary = f"Small change detected ({change_pct}%) — likely a button state change, tooltip, or field update."
            elif change_pct < 40:
                summary = f"Moderate change detected ({change_pct}%) — likely a modal opened, panel toggled, or content loaded."
            else:
                summary = f"Major change detected ({change_pct}%) — likely a page navigation, large modal, or full content refresh."

            return {
                "changed": True,
                "change_percentage": change_pct,
                "hash_before": hash_before,
                "hash_after": hash_after,
                "summary": summary,
            }
        except ImportError:
            # PIL not available — fall back to hash-only comparison
            return {
                "changed": True,
                "change_percentage": -1,  # unknown
                "hash_before": hash_before,
                "hash_after": hash_after,
                "summary": "Visual change detected (screenshots differ). Install Pillow for detailed pixel analysis.",
            }
        except Exception as e:
            return {
                "changed": True,
                "change_percentage": -1,
                "hash_before": hash_before,
                "hash_after": hash_after,
                "summary": f"Visual change detected. Pixel comparison failed: {e}",
            }


# ============================================================================
# OmniParser Fallback — Microsoft's vision-based element detection
# ============================================================================

class OmniParserFallback:
    """Fallback element detection using Microsoft OmniParser (Hugging Face).

    When DOM extraction fails (canvas-rendered content, SVGs, third-party
    iframes), this sends the screenshot to OmniParser for vision-based
    element detection.

    OmniParser returns bounding boxes and labels for detected UI elements
    in the screenshot — similar to our DOM extraction but works on any image.
    """

    def __init__(self, endpoint: str = OMNIPARSER_ENDPOINT, api_key: str = OMNIPARSER_API_KEY):
        self.endpoint = endpoint
        self.api_key = api_key
        self._available = bool(endpoint and api_key)

    @property
    def is_available(self) -> bool:
        return self._available

    async def detect_elements(self, screenshot_b64: str) -> List[Dict[str, Any]]:
        """Send screenshot to OmniParser and get element detections.

        Returns element map in same format as DOM extraction:
        [{id, type, text, bbox, selector, confidence}]
        """
        if not self._available:
            return []

        try:
            import aiohttp

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            # OmniParser expects base64 image
            payload = {
                "inputs": screenshot_b64,
                "parameters": {
                    "task": "ui-element-detection",
                    "return_bboxes": True,
                    "return_labels": True,
                },
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"OmniParser API returned {resp.status}")
                        return []

                    result = await resp.json()

            # Parse OmniParser response into our element map format
            elements = []
            detections = result if isinstance(result, list) else result.get("detections", [])
            for i, det in enumerate(detections, start=1):
                bbox = det.get("bbox", det.get("box", [0, 0, 0, 0]))
                # OmniParser returns [x1, y1, x2, y2], we want [x, y, w, h]
                if len(bbox) == 4:
                    x1, y1, x2, y2 = bbox
                    w, h = x2 - x1, y2 - y1
                else:
                    x1, y1, w, h = 0, 0, 0, 0

                elements.append({
                    "id": i,
                    "type": det.get("label", det.get("type", "unknown")),
                    "text": det.get("text", det.get("content", "")),
                    "bbox": [int(x1), int(y1), int(w), int(h)],
                    "selector": "",  # OmniParser doesn't have CSS selectors
                    "confidence": det.get("confidence", det.get("score", 0)),
                    "source": "omniparser",
                })

            logger.info(f"OmniParser detected {len(elements)} elements")
            return elements

        except ImportError:
            logger.debug("aiohttp not available — OmniParser fallback disabled")
            return []
        except Exception as e:
            logger.warning(f"OmniParser detection failed: {e}")
            return []


# Singleton
_omniparser: Optional[OmniParserFallback] = None

def get_omniparser() -> OmniParserFallback:
    global _omniparser
    if _omniparser is None:
        _omniparser = OmniParserFallback()
    return _omniparser


# ============================================================================
# Vision Session Management
# ============================================================================

class VisionSession:
    """A managed Playwright browser session for internal app browsing."""

    def __init__(self, conversation_id: str, tenant_id: str):
        self.conversation_id = conversation_id
        self.tenant_id = tenant_id
        self.browser = None
        self.context = None
        self.page = None
        self.created_at = time.time()
        self.last_activity = time.time()
        self._closed = False
        self._pw = None

    @property
    def is_expired(self) -> bool:
        now = time.time()
        idle = now - self.last_activity > VISION_SESSION_IDLE_TIMEOUT
        lifetime = now - self.created_at > VISION_SESSION_MAX_LIFETIME
        return idle or lifetime

    def touch(self):
        self.last_activity = time.time()

    async def start(self, jwt_token: str, headed: bool = False):
        """Launch browser, inject auth, navigate to app."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright is not installed. Install with: pip install playwright && playwright install chromium"
            )

        self._pw = await async_playwright().start()

        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            # The SPA uses <script type="module" crossorigin> which requires CORS
            # headers. envoy-internal doesn't set Access-Control-Allow-Origin on
            # static assets, causing Chromium to silently block the JS bundle.
            # Safe to disable since we only browse internal Docker URLs.
            "--disable-web-security",
        ]

        # Headed mode: connect to VNC display for live viewing
        if headed and os.environ.get("DISPLAY"):
            self.browser = await self._pw.chromium.launch(
                headless=False,
                args=launch_args,
            )
        else:
            self.browser = await self._pw.chromium.launch(
                headless=True,
                args=launch_args,
            )

        self.context = await self.browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 "
                "YAMIL-AI-Vision/1.0"
            ),
        )

        # Inject auth cookie for internal envoy routing
        envoy_domain = INTERNAL_APP_URL.replace("http://", "").replace("https://", "").split(":")[0]
        await self.context.add_cookies([{
            "name": "access_token",
            "value": jwt_token,
            "domain": envoy_domain,
            "path": "/",
        }])

        self.page = await self.context.new_page()

        # Polyfill crypto.randomUUID — only available in secure contexts (HTTPS/localhost).
        # The headless browser navigates to http://envoy-internal:9080 (not secure),
        # so crypto.randomUUID is undefined and the SPA crashes before React mounts.
        await self.page.add_init_script("""
            if (typeof crypto !== 'undefined' && !crypto.randomUUID) {
                crypto.randomUUID = function() {
                    return '10000000-1000-4000-8000-100000000000'.replace(/[018]/g, c =>
                        (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
                    );
                };
            }
        """)

        # Prevent accidental node dragging on React Flow canvases.
        # When the AI browser session loads a flow, pointer events can
        # inadvertently trigger drag-and-drop on canvas nodes, moving them
        # off-screen. This script intercepts pointermove after pointerdown
        # on a node, preventing drags while still allowing clicks.
        # The global __aiAllowDrag flag lets CanvasTouch "drag" action
        # temporarily bypass the guard for intentional node moves.
        await self.page.add_init_script("""
            (function() {
                window.__aiAllowDrag = false;
                var _nodeDrag = false;
                document.addEventListener('pointerdown', function(e) {
                    if (e.target.closest && e.target.closest('.react-flow__node')) {
                        _nodeDrag = true;
                    }
                }, true);
                document.addEventListener('pointermove', function(e) {
                    if (_nodeDrag && !window.__aiAllowDrag) {
                        e.stopImmediatePropagation();
                        e.preventDefault();
                    }
                }, true);
                document.addEventListener('pointerup', function() {
                    _nodeDrag = false;
                }, true);
                document.addEventListener('pointercancel', function() {
                    _nodeDrag = false;
                }, true);
                document.addEventListener('dragstart', function(e) {
                    if (e.target.closest && e.target.closest('.react-flow__node') && !window.__aiAllowDrag) {
                        e.preventDefault();
                    }
                }, true);
            })();
        """)

        # Auto-accept all browser dialogs (beforeunload, alert, confirm, prompt).
        # The SPA fires beforeunload when navigating away from pages with unsaved
        # changes (e.g., the flow builder). Without this handler, page.goto()
        # hangs until the dialog times out.
        self.page.on("dialog", lambda dialog: asyncio.ensure_future(dialog.accept()))

        # Auth injection strategy:
        # 1. Navigate to the app (sets the correct origin for localStorage)
        # 2. Fetch /api/v1/auth/me to get the user object the SPA needs
        # 3. Set the full Zustand persist store (token + user) in localStorage
        # 4. Reload — SPA rehydrates from localStorage and renders authenticated
        #
        # CRITICAL: The SPA's ProtectedRoute checks _hasHydrated + isAuthenticated,
        # but pages (Users, Settings, etc.) also need user.tenantId, user.role to
        # make API calls. Without a valid user object, pages render empty.
        await self.page.goto(INTERNAL_APP_URL, wait_until="domcontentloaded", timeout=15000)

        # Step 1: Fetch user data from the API using the JWT token
        logger.info(f"JWT token length: {len(jwt_token) if jwt_token else 0}, starts with: {jwt_token[:20] if jwt_token else 'EMPTY'}")
        user_data = await self.page.evaluate(f"""async () => {{
            try {{
                const resp = await fetch('/api/v1/auth/me', {{
                    headers: {{ 'Authorization': 'Bearer ' + {json.dumps(jwt_token)} }}
                }});
                if (resp.ok) {{
                    return await resp.json();
                }}
                return {{ error: resp.status + ' ' + resp.statusText }};
            }} catch (e) {{
                return {{ error: e.message }};
            }}
        }}""")

        if isinstance(user_data, dict) and "error" in user_data:
            logger.error(f"Auth /me call failed: {user_data['error']}. SPA may not render correctly.")
            user_obj = None
        else:
            logger.info(f"Auth /me succeeded: id={user_data.get('id')}, email={user_data.get('email')}, role={user_data.get('role')}")
            full_name = user_data.get("full_name", "User")
            name_parts = full_name.split(" ", 1) if full_name else ["User"]
            user_obj = {
                "id": user_data.get("id", ""),
                "email": user_data.get("email", ""),
                "firstName": name_parts[0],
                "lastName": name_parts[1] if len(name_parts) > 1 else "",
                "role": user_data.get("role", "user"),
                "tenantId": user_data.get("tenant_id", ""),
                "tenantName": user_data.get("tenant_name", ""),
                "tenantSlug": user_data.get("tenant_slug", ""),
            }

        # Step 2: Set the full Zustand persist store in localStorage
        auth_store_value = json.dumps({
            "state": {
                "user": user_obj,
                "token": jwt_token,
                "refreshToken": None,
                "isAuthenticated": True,
            },
            "version": 0,
        })
        await self.page.evaluate(f"""() => {{
            localStorage.setItem('logic-weaver-auth', {json.dumps(auth_store_value)});
            localStorage.setItem('token', {json.dumps(jwt_token)});
            localStorage.setItem('auth_token', {json.dumps(jwt_token)});
        }}""")

        # Step 3: Reload so the SPA rehydrates with full auth state
        await self.page.reload(wait_until="domcontentloaded", timeout=15000)

        # Wait for SPA to redirect away from /login (ProtectedRoute allows when isAuthenticated=true)
        try:
            await self.page.wait_for_url(
                lambda url: "/login" not in url,
                timeout=10000,
            )
            logger.info(f"Auth succeeded — redirected to: {self.page.url}")
        except Exception:
            # Still on /login — try force-navigating to home
            logger.warning(f"Still on login after 10s — trying force navigation. URL: {self.page.url}")
            await self.page.goto(INTERNAL_APP_URL + "/", wait_until="networkidle", timeout=15000)
            await self.page.wait_for_timeout(2000)
            current_url = self.page.url
            if "/login" in current_url:
                logger.error(f"Auth injection FAILED — stuck on {current_url}. JWT may be expired or invalid.")
            else:
                logger.info(f"Force-navigated past login, now at: {current_url}")

        # Step 4: Wait for SPA content to fully render (React hydration + data fetching)
        await self.page.wait_for_timeout(2000)
        try:
            # Wait for any major content to appear (nav, sidebar, main content)
            await self.page.wait_for_selector(
                "nav, [role='navigation'], aside, main, [role='main']",
                timeout=8000,
            )
            logger.info("SPA content rendered — navigation elements detected")
        except Exception:
            logger.warning("SPA content not fully rendered after 8s wait")

        self.touch()
        logger.info(f"Vision session started for conversation {self.conversation_id}")

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
            if self._pw:
                await self._pw.stop()
        except Exception as e:
            logger.warning(f"Error closing vision session: {e}")
        logger.info(f"Vision session closed for conversation {self.conversation_id}")


class VisionSessionManager:
    """Manages vision browser sessions."""

    def __init__(self):
        self._sessions: Dict[str, VisionSession] = {}

    async def get_or_create(
        self, conversation_id: str, tenant_id: str, jwt_token: str, headed: bool = False
    ) -> tuple:
        """Get existing session or create new one. Returns (session, error)."""
        await self._cleanup_expired()

        # Reuse existing session if available
        session = self._sessions.get(conversation_id)
        if session and not session.is_expired:
            session.touch()
            return session, None

        # Clean up expired session
        if session:
            await session.close()
            del self._sessions[conversation_id]

        # Check tenant limit — evict oldest session if at capacity
        tenant_sessions = [
            (cid, s) for cid, s in self._sessions.items()
            if s.tenant_id == tenant_id and not s.is_expired
        ]
        if len(tenant_sessions) >= MAX_VISION_SESSIONS:
            # Evict the oldest (least recently active) session
            tenant_sessions.sort(key=lambda x: x[1].last_activity)
            evict_cid, evict_session = tenant_sessions[0]
            logger.info(f"Evicting oldest vision session {evict_cid} to make room for {conversation_id}")
            await evict_session.close()
            del self._sessions[evict_cid]

        # Create new session
        session = VisionSession(conversation_id, tenant_id)
        try:
            await session.start(jwt_token, headed=headed)
        except Exception as e:
            logger.exception("Failed to start vision session")
            return None, f"Failed to start browser: {str(e)}"

        self._sessions[conversation_id] = session
        return session, None

    async def close_session(self, conversation_id: str):
        """Close a specific session."""
        session = self._sessions.pop(conversation_id, None)
        if session:
            await session.close()

    async def _cleanup_expired(self):
        """Clean up expired sessions."""
        expired = [cid for cid, s in self._sessions.items() if s.is_expired]
        for cid in expired:
            logger.info(f"Cleaning up expired vision session: {cid}")
            await self._sessions[cid].close()
            del self._sessions[cid]


# Singleton
_vision_session_manager: Optional[VisionSessionManager] = None


def get_vision_session_manager() -> VisionSessionManager:
    global _vision_session_manager
    if _vision_session_manager is None:
        _vision_session_manager = VisionSessionManager()
    return _vision_session_manager


# ============================================================================
# Accessibility Snapshot Helper
# ============================================================================

def _flatten_accessibility_tree(node: Dict[str, Any], depth: int = 0) -> str:
    """Flatten Playwright accessibility tree into readable text."""
    parts = []
    role = node.get("role", "")
    name = node.get("name", "")
    value = node.get("value", "")

    if name:
        indent = "  " * min(depth, 4)
        label = f"[{role}]" if role and role not in ("none", "generic", "text") else ""
        text = f"{indent}{label} {name}"
        if value:
            text += f": {value}"
        parts.append(text.strip())

    for child in node.get("children", []):
        child_text = _flatten_accessibility_tree(child, depth + 1)
        if child_text:
            parts.append(child_text)

    return "\n".join(parts)


# ============================================================================
# DOM Element Map Extraction (OmniParser-style)
# ============================================================================

# JavaScript to extract all interactive elements with bounding boxes
EXTRACT_ELEMENT_MAP_JS = """
() => {
    const elements = [];
    let id = 1;

    // Selectors for interactive elements
    const selectors = [
        'button', '[role="button"]',
        'a[href]', '[role="link"]',
        'input', 'textarea', 'select', '[role="textbox"]', '[role="combobox"]',
        '[role="tab"]', '[role="menuitem"]', '[role="option"]',
        '[role="checkbox"]', '[role="radio"]', '[role="switch"]',
        '.react-flow__node',
        '[data-radix-collection-item]',
    ];

    const seen = new Set();

    for (const selector of selectors) {
        for (const el of document.querySelectorAll(selector)) {
            // Skip if already processed or invisible
            if (seen.has(el)) continue;
            seen.add(el);

            const rect = el.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) continue;
            if (rect.bottom < 0 || rect.top > window.innerHeight) continue;
            if (rect.right < 0 || rect.left > window.innerWidth) continue;

            // Skip elements inside the AI chat panel
            if (el.closest('[data-ai-chat-panel]')) continue;

            const tag = el.tagName.toLowerCase();
            const role = el.getAttribute('role') || '';
            const ariaLabel = el.getAttribute('aria-label') || '';

            // Determine type
            let type = 'unknown';
            if (tag === 'button' || role === 'button') type = 'button';
            else if (tag === 'a' || role === 'link') type = 'link';
            else if (tag === 'input') type = 'input:' + (el.type || 'text');
            else if (tag === 'textarea') type = 'textarea';
            else if (tag === 'select' || role === 'combobox') type = 'dropdown';
            else if (role === 'tab') type = 'tab';
            else if (role === 'menuitem') type = 'menuitem';
            else if (role === 'checkbox') type = 'checkbox';
            else if (role === 'radio') type = 'radio';
            else if (role === 'switch') type = 'switch';
            else if (el.classList.contains('react-flow__node')) type = 'node';
            else type = role || tag;

            // Get text content
            let text = '';
            if (tag === 'input' || tag === 'textarea') {
                text = el.placeholder || ariaLabel || el.name || '';
            } else if (tag === 'select') {
                const selected = el.options?.[el.selectedIndex];
                text = selected?.text || ariaLabel || '';
            } else {
                // For buttons/links, get direct text content (not deep children)
                const directText = el.textContent?.trim() || '';
                text = directText.slice(0, 80);
            }
            if (!text && ariaLabel) text = ariaLabel;

            // Build a CSS selector for this element
            let cssSelector = '';
            if (el.id) {
                cssSelector = '#' + el.id;
            } else if (el.classList.length > 0) {
                const classes = Array.from(el.classList).slice(0, 3).join('.');
                cssSelector = tag + '.' + classes;
            } else {
                cssSelector = tag;
            }

            // Node-specific: get node data-id
            const nodeId = el.getAttribute('data-id') || '';

            elements.push({
                id: id++,
                type,
                text: text.slice(0, 80),
                bbox: [
                    Math.round(rect.left),
                    Math.round(rect.top),
                    Math.round(rect.width),
                    Math.round(rect.height),
                ],
                selector: cssSelector,
                nodeId: nodeId || undefined,
                disabled: el.disabled || el.getAttribute('aria-disabled') === 'true',
            });
        }
    }

    return elements;
}
"""

# JavaScript to inject numbered overlay labels on interactive elements
INJECT_ELEMENT_LABELS_JS = """
(elementMap) => {
    // Create overlay container
    const overlay = document.createElement('div');
    overlay.id = '__ai_element_labels';
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:99999;';

    for (const el of elementMap) {
        const [x, y, w, h] = el.bbox;
        // Create label
        const label = document.createElement('div');
        label.style.cssText = `
            position:absolute;
            left:${x}px;
            top:${y}px;
            background:rgba(255,50,50,0.85);
            color:white;
            font-size:10px;
            font-weight:bold;
            padding:1px 4px;
            border-radius:3px;
            line-height:14px;
            font-family:monospace;
            pointer-events:none;
            z-index:99999;
        `;
        label.textContent = '[' + el.id + ']';
        overlay.appendChild(label);

        // Create subtle border around element
        const border = document.createElement('div');
        border.style.cssText = `
            position:absolute;
            left:${x}px;
            top:${y}px;
            width:${w}px;
            height:${h}px;
            border:1px solid rgba(255,50,50,0.4);
            border-radius:2px;
            pointer-events:none;
        `;
        overlay.appendChild(border);
    }

    document.body.appendChild(overlay);
    return true;
}
"""

REMOVE_ELEMENT_LABELS_JS = """
() => {
    const overlay = document.getElementById('__ai_element_labels');
    if (overlay) overlay.remove();
    return true;
}
"""


# ============================================================================
# Rate Limiting & Action Audit
# ============================================================================

class ActionRateLimiter:
    """Rate limiter for canvas_touch actions — prevents runaway loops."""

    def __init__(self, max_actions_per_minute: int = 30):
        self.max_per_minute = max_actions_per_minute
        self._timestamps: List[float] = []

    def check(self) -> bool:
        """Return True if action is allowed, False if rate limited."""
        now = time.time()
        # Remove timestamps older than 60 seconds
        self._timestamps = [t for t in self._timestamps if now - t < 60]
        if len(self._timestamps) >= self.max_per_minute:
            return False
        self._timestamps.append(now)
        return True

    @property
    def remaining(self) -> int:
        now = time.time()
        recent = sum(1 for t in self._timestamps if now - t < 60)
        return max(0, self.max_per_minute - recent)


class ActionAuditLog:
    """Audit trail for canvas_touch interactions — enterprise compliance.

    Logs to both in-memory buffer AND CloudWatch Logs for durable storage.
    """

    def __init__(self, max_entries: int = 200):
        self.max_entries = max_entries
        self._entries: List[Dict[str, Any]] = []

    def log(
        self,
        action: str,
        target: str,
        result: str,
        conversation_id: str,
        extra: Optional[Dict] = None,
        tenant_id: str = "",
        user_email: str = "",
        user_role: str = "",
    ):
        entry = {
            "timestamp": time.time(),
            "action": action,
            "target": target,
            "result": result,
            "conversation_id": conversation_id,
            "tenant_id": tenant_id,
            "user_email": user_email,
            "user_role": user_role,
        }
        if extra:
            entry.update(extra)
        self._entries.append(entry)
        if len(self._entries) > self.max_entries:
            self._entries = self._entries[-self.max_entries:]
        logger.info(f"[AuditTrail] {action} on '{target}' -> {result} (conv={conversation_id[:8]})")

        # Also send to CloudWatch for durable enterprise compliance
        try:
            cw = get_cw_audit_writer()
            cw.log(entry)
        except Exception:
            pass  # CloudWatch is best-effort — never block the main flow

    def get_recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self._entries[-limit:]

    def flush_to_cloudwatch(self):
        """Force-flush any buffered CloudWatch entries."""
        try:
            cw = get_cw_audit_writer()
            cw.flush()
        except Exception:
            pass


# Singletons
_rate_limiter: Optional[ActionRateLimiter] = None
_audit_log: Optional[ActionAuditLog] = None

def get_rate_limiter() -> ActionRateLimiter:
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = ActionRateLimiter()
    return _rate_limiter

def get_audit_log() -> ActionAuditLog:
    global _audit_log
    if _audit_log is None:
        _audit_log = ActionAuditLog()
    return _audit_log


# ============================================================================
# Canvas Vision Tool
# ============================================================================

class CanvasVisionTool(Tool):
    """AI's own browser for viewing and learning the app's UI."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="canvas_vision",
            description=(
                "View the app's UI through a headless browser. Use this to take screenshots "
                "of flow canvases, verify node connections visually, extract page structure, "
                "and learn UI patterns. Screenshots are shown to the user in chat."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "view_flow", "view_page", "screenshot", "verify_canvas",
                            "learn_page", "close",
                            "click", "fill", "select", "hover", "scroll", "drag",
                        ],
                        "description": (
                            "Action to perform: "
                            "'view_flow' — navigate to a flow by ID, take screenshot + snapshot; "
                            "'view_page' — navigate to any app page; "
                            "'screenshot' — capture current page; "
                            "'verify_canvas' — check node connections and layout issues; "
                            "'learn_page' — extract full UI structure for learning; "
                            "'close' — close the browser session; "
                            "--- CanvasTouch (interaction) --- "
                            "'click' — click a button, link, tab, or any element; "
                            "'fill' — type text into an input field or textarea; "
                            "'select' — choose an option from a dropdown; "
                            "'hover' — hover over an element to reveal tooltips or menus; "
                            "'scroll' — scroll the page or a specific container; "
                            "'drag' — drag a node/element by dx/dy pixels (e.g., move a canvas node)"
                        ),
                    },
                    "flow_id": {
                        "type": "string",
                        "description": "Flow UUID to view (for view_flow action)",
                    },
                    "path": {
                        "type": "string",
                        "description": "App path to navigate to, e.g. '/settings', '/flows', '/users'. Required for view_page and CanvasTouch actions (click, fill, select, hover, scroll) when the target is on a different page.",
                    },
                    "headed": {
                        "type": "boolean",
                        "description": "If true, launch in headed mode (visible via noVNC). Default: false",
                    },
                    "selector": {
                        "type": "string",
                        "description": "CSS selector to target an element (e.g., 'button.save', '#email', '.nav-link'). Used by CanvasTouch actions.",
                    },
                    "text": {
                        "type": "string",
                        "description": "Visible text to find an element (e.g., 'Save', 'AI Learnings', 'Submit'). Used by CanvasTouch actions.",
                    },
                    "value": {
                        "type": "string",
                        "description": "Value to type into a form field (fill) or option text to select (select).",
                    },
                    "scroll_direction": {
                        "type": "string",
                        "enum": ["up", "down", "left", "right"],
                        "description": "Direction to scroll (default: down). Used by scroll action.",
                    },
                    "scroll_amount": {
                        "type": "number",
                        "description": "Pixels to scroll (default: 500). Used by scroll action.",
                    },
                    "dx": {
                        "type": "number",
                        "description": "Horizontal pixels to drag (positive=right, negative=left). Used by drag action.",
                    },
                    "dy": {
                        "type": "number",
                        "description": "Vertical pixels to drag (positive=down, negative=up). Used by drag action.",
                    },
                    "element_id": {
                        "type": "integer",
                        "description": "Element ID from the element map to target (e.g., 3 to click element [3]). Alternative to selector/text for CanvasTouch actions.",
                    },
                    "highlight": {
                        "type": "boolean",
                        "description": "If true, overlay numbered labels [1], [2], [3] on interactive elements before taking the screenshot. Default: false.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, describe what the action would do without executing it. For safety review.",
                    },
                },
                "required": ["action"],
            },
            required_permission=Permission.READ,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        **kwargs,
    ) -> ToolResult:
        action = kwargs.get("action", "")
        conversation_id = kwargs.get("_conversation_id", "")
        jwt_token = kwargs.get("_jwt_token", "")
        headed = kwargs.get("headed", False)
        highlight = kwargs.get("highlight", False)
        dry_run = kwargs.get("dry_run", False)
        element_id = kwargs.get("element_id")

        if not conversation_id:
            return ToolResult(success=False, error="No conversation context available")

        # Extract user info for enriched audit logging
        user_email = getattr(auth_context, "email", "")
        user_role = auth_context.role.value if isinstance(auth_context.role, Enum) else str(getattr(auth_context, "role", ""))
        tenant_id_str = str(auth_context.tenant_id)

        manager = get_vision_session_manager()
        audit = get_audit_log()
        breaker = get_circuit_breaker()

        # ── RBAC Check ──
        logger.info(f"[Phase2] RBAC check: user={user_email}, role={user_role}, action={action}")
        rbac_error = check_vision_rbac(auth_context, action)
        if rbac_error:
            audit.log(
                action, "(rbac_denied)", "denied", conversation_id,
                tenant_id=tenant_id_str, user_email=user_email, user_role=user_role,
                extra={"reason": rbac_error},
            )
            return ToolResult(success=False, error=rbac_error)

        # ── Circuit Breaker Check ──
        logger.info(f"[Phase2] Circuit breaker state: {breaker.state.value}, failures={breaker._failure_count}")
        if not breaker.allow_request():
            audit.log(
                action, "(circuit_open)", "blocked", conversation_id,
                tenant_id=tenant_id_str, user_email=user_email, user_role=user_role,
            )
            return ToolResult(
                success=False,
                error=(
                    f"Canvas vision is temporarily unavailable (circuit breaker OPEN after "
                    f"{breaker.failure_threshold} consecutive failures). "
                    f"Will auto-recover in {breaker.recovery_timeout}s. "
                    f"Current state: {breaker.stats}"
                ),
            )

        # Handle close action
        if action == "close":
            await manager.close_session(conversation_id)
            audit.log("close", "session", "success", conversation_id,
                       tenant_id=tenant_id_str, user_email=user_email, user_role=user_role)
            return ToolResult(
                success=True,
                message="Vision browser session closed",
                data={"closed": True},
            )

        if not jwt_token:
            return ToolResult(success=False, error="No auth token available for vision session")

        # Rate limit CanvasTouch actions
        if action in ("click", "fill", "select", "hover", "scroll", "drag"):
            limiter = get_rate_limiter()
            if not limiter.check():
                audit.log(action, "rate_limited", "blocked", conversation_id,
                           tenant_id=tenant_id_str, user_email=user_email, user_role=user_role)
                return ToolResult(
                    success=False,
                    error=f"Rate limited: too many actions per minute ({limiter.max_per_minute}/min). "
                          f"Wait a moment before trying again. Remaining: {limiter.remaining}",
                )

        # Get or create session
        try:
            session, error = await manager.get_or_create(
                conversation_id,
                tenant_id_str,
                jwt_token,
                headed=headed,
            )
            if error:
                breaker.record_failure(error)
                return ToolResult(success=False, error=error)
        except Exception as e:
            breaker.record_failure(str(e))
            raise

        # Store element_id and highlight on session for use by sub-methods
        session._element_id = element_id
        session._highlight = highlight
        session._dry_run = dry_run

        try:
            if action == "view_flow":
                result = await self._view_flow(session, kwargs.get("flow_id", ""))
            elif action == "view_page":
                result = await self._view_page(session, kwargs.get("path", "/"))
            elif action == "screenshot":
                result = await self._take_screenshot(session)
            elif action == "verify_canvas":
                result = await self._verify_canvas(session, kwargs.get("flow_id"))
            elif action == "learn_page":
                result = await self._learn_page(session, kwargs.get("path"))
            # ── CanvasTouch interactions ──
            elif action in ("click", "fill", "select", "hover", "scroll", "drag"):
                # Dry run mode — describe what would happen without executing
                if dry_run:
                    target = kwargs.get("text", "") or kwargs.get("selector", "") or (f"element #{element_id}" if element_id else "(none)")
                    desc = f"Would {action} on '{target}'"
                    if action == "fill":
                        desc += f" with value '{kwargs.get('value', '')[:50]}'"
                    elif action == "select":
                        desc += f" selecting '{kwargs.get('value', '')}'"
                    elif action == "drag":
                        desc += f" by ({kwargs.get('dx', 0)}, {kwargs.get('dy', 0)})px"
                    audit.log(action, target, "dry_run", conversation_id,
                               tenant_id=tenant_id_str, user_email=user_email, user_role=user_role)
                    return ToolResult(success=True, message=f"[DRY RUN] {desc}", data={"dry_run": True, "description": desc})

                # Navigate to path first if provided
                touch_path = kwargs.get("path", "") or kwargs.get("page_route", "") or kwargs.get("page", "") or kwargs.get("route", "")
                if touch_path:
                    await self._navigate_to(session, touch_path)

                # Resolve element_id to selector/text if provided
                selector = kwargs.get("selector", "")
                text = kwargs.get("text", "")
                if element_id and not selector and not text:
                    resolved = await self._resolve_element_id(session, element_id)
                    if resolved:
                        selector = resolved

                # ── Screenshot Diffing: capture BEFORE screenshot ──
                before_screenshot = await self._capture_screenshot(session.page)

                if action == "click":
                    result = await self._touch_click(session, selector, text)
                elif action == "fill":
                    result = await self._touch_fill(session, selector, text, kwargs.get("value", ""))
                elif action == "select":
                    result = await self._touch_select(session, selector, text, kwargs.get("value", ""))
                elif action == "hover":
                    result = await self._touch_hover(session, selector, text)
                elif action == "drag":
                    result = await self._touch_drag(session, selector, text, kwargs.get("dx", 0), kwargs.get("dy", 0))
                else:  # scroll
                    result = await self._touch_scroll(
                        session, selector, kwargs.get("scroll_direction", "down"), kwargs.get("scroll_amount", 500),
                    )

                # ── Screenshot Diffing: compare BEFORE vs AFTER ──
                if result.success and before_screenshot and result.data and result.data.get("screenshot_base64"):
                    diff = ScreenshotDiffer.compare(before_screenshot, result.data["screenshot_base64"])
                    result.data["screenshot_diff"] = diff

                # Enriched audit log
                target_desc = text or selector or (f"element #{element_id}" if element_id else "(none)")
                audit.log(
                    action, target_desc, "success" if result.success else "failed",
                    conversation_id,
                    tenant_id=tenant_id_str, user_email=user_email, user_role=user_role,
                )
            else:
                return ToolResult(success=False, error=f"Unknown action: {action}")

            # ── Screenshot Encryption + TTL ──
            if result.success and result.data and result.data.get("screenshot_base64"):
                cache = get_screenshot_cache()
                cache_key = f"{conversation_id}:{action}:{int(time.time())}"
                cache.store(cache_key, result.data["screenshot_base64"])
                result.data["screenshot_cache_key"] = cache_key
                result.data["screenshot_ttl"] = SCREENSHOT_TTL_SECONDS
                logger.info(f"[Phase2] Screenshot cached: key={cache_key}, ttl={SCREENSHOT_TTL_SECONDS}s, encrypted={cache._fernet is not None}")

            # Audit log for all successful actions (view, screenshot, touch, etc.)
            audit.log(
                action, kwargs.get("flow_id", "") or kwargs.get("path", "") or "(canvas)",
                "success", conversation_id,
                tenant_id=tenant_id_str, user_email=user_email, user_role=user_role,
            )

            # Circuit breaker: record success
            breaker.record_success()

            # Flush audit trail to CloudWatch after each successful action
            audit.flush_to_cloudwatch()

            return result

        except Exception as e:
            logger.exception(f"canvas_vision action '{action}' failed")
            breaker.record_failure(str(e))
            audit.log(
                action, str(kwargs)[:200], f"error: {e}",
                conversation_id,
                tenant_id=tenant_id_str, user_email=user_email, user_role=user_role,
            )
            audit.flush_to_cloudwatch()
            return ToolResult(success=False, error=f"Vision action failed: {str(e)}")

    # ====================================================================
    # Element Map & Highlighting Helpers
    # ====================================================================

    async def _extract_element_map(self, page) -> List[Dict[str, Any]]:
        """Extract structured element map from the current page DOM."""
        try:
            elements = await page.evaluate(EXTRACT_ELEMENT_MAP_JS)
            return elements if isinstance(elements, list) else []
        except Exception as e:
            logger.warning(f"Element map extraction failed: {e}")
            return []

    async def _highlight_elements(self, page, element_map: List[Dict[str, Any]]) -> bool:
        """Inject numbered overlay labels onto the page for visual identification."""
        try:
            await page.evaluate(INJECT_ELEMENT_LABELS_JS, element_map)
            return True
        except Exception as e:
            logger.warning(f"Element highlighting failed: {e}")
            return False

    async def _remove_highlights(self, page):
        """Remove injected element labels from the page."""
        try:
            await page.evaluate(REMOVE_ELEMENT_LABELS_JS)
        except Exception:
            pass

    async def _capture_with_element_map(
        self, session: VisionSession, include_map: bool = True
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        Capture screenshot with optional element map and highlighting.
        Falls back to OmniParser if DOM extraction yields few results.
        Returns (screenshot_b64, element_map).
        """
        page = session.page
        element_map = []

        if include_map:
            element_map = await self._extract_element_map(page)

            # OmniParser fallback: if DOM extraction found very few elements,
            # try vision-based detection for canvas-rendered/SVG content
            if len(element_map) < 3:
                omni = get_omniparser()
                if omni.is_available:
                    # Take a quick screenshot for OmniParser analysis
                    temp_screenshot = await self._capture_screenshot(page)
                    if temp_screenshot:
                        omni_elements = await omni.detect_elements(temp_screenshot)
                        if omni_elements:
                            # Merge: DOM elements first, then OmniParser detections
                            # Renumber OmniParser elements to avoid ID collisions
                            max_id = max((e.get("id", 0) for e in element_map), default=0)
                            for el in omni_elements:
                                el["id"] = max_id + el["id"]
                            element_map.extend(omni_elements)
                            logger.info(f"OmniParser added {len(omni_elements)} elements (DOM had {len(element_map) - len(omni_elements)})")

        # Highlighting: inject labels before screenshot, then remove
        highlight = getattr(session, '_highlight', False)
        if highlight and element_map:
            await self._highlight_elements(page, element_map)
            # Brief pause to let overlays render
            await page.wait_for_timeout(200)

        screenshot_b64 = await self._capture_screenshot(page)

        if highlight and element_map:
            await self._remove_highlights(page)

        return screenshot_b64, element_map

    async def _resolve_element_id(self, session: VisionSession, element_id: int) -> Optional[str]:
        """Resolve an element ID from the element map to a CSS selector."""
        element_map = await self._extract_element_map(session.page)
        for el in element_map:
            if el.get("id") == element_id:
                selector = el.get("selector", "")
                if selector:
                    logger.info(f"Resolved element_id={element_id} to selector='{selector}' (text='{el.get('text', '')}')")
                    return selector
                break
        logger.warning(f"Could not resolve element_id={element_id} — element not found in map")
        return None

    async def _wait_for_stable(self, page, timeout_ms: int = 3000):
        """Wait for DOM to stabilize (no mutations for 500ms). Used before interactions."""
        try:
            await page.evaluate("""
                () => new Promise((resolve) => {
                    let timer = null;
                    const observer = new MutationObserver(() => {
                        if (timer) clearTimeout(timer);
                        timer = setTimeout(() => {
                            observer.disconnect();
                            resolve(true);
                        }, 500);
                    });
                    observer.observe(document.body, {
                        childList: true, subtree: true, attributes: true
                    });
                    // Start initial timer in case no mutations happen
                    timer = setTimeout(() => {
                        observer.disconnect();
                        resolve(true);
                    }, 500);
                    // Hard timeout
                    setTimeout(() => {
                        observer.disconnect();
                        resolve(false);
                    }, %d);
                })
            """ % timeout_ms)
        except Exception:
            # If stability check fails, just wait a fixed amount
            await page.wait_for_timeout(500)

    # ====================================================================
    # Vision Actions
    # ====================================================================

    async def _view_flow(self, session: VisionSession, flow_id: str) -> ToolResult:
        """Navigate to a flow and capture screenshot + accessibility snapshot + element map."""
        if not flow_id:
            return ToolResult(success=False, error="flow_id is required for view_flow action")

        url = f"{INTERNAL_APP_URL}/flows/{flow_id}"
        await session.page.goto(url, wait_until="networkidle", timeout=20000)

        # Wait for React canvas to render (flow data fetch + layout)
        await session.page.wait_for_timeout(3000)

        # Click "Fit view" so the screenshot shows all nodes centered,
        # matching what the user sees in their browser.
        try:
            fit_btn = session.page.get_by_role("button", name="Fit view")
            await fit_btn.click(timeout=3000)
            await session.page.wait_for_timeout(1000)
            logger.info("Fit view applied before screenshot")
        except Exception:
            logger.debug("Fit view button not found, skipping")

        # Take screenshot with element map
        screenshot_b64, element_map = await self._capture_with_element_map(session)

        # Get accessibility snapshot
        snapshot_text = ""
        try:
            snapshot = await session.page.accessibility.snapshot()
            if snapshot:
                snapshot_text = _flatten_accessibility_tree(snapshot)
                if len(snapshot_text) > 50000:
                    snapshot_text = snapshot_text[:50000] + "\n...(truncated)"
        except Exception as e:
            snapshot_text = f"(accessibility snapshot unavailable: {e})"

        data = {
            "url": session.page.url,
            "title": await session.page.title(),
            "flow_id": flow_id,
            "accessibility_snapshot": snapshot_text,
        }

        # Include element map so AI can reference elements by ID
        if element_map:
            data["element_map"] = element_map
            data["element_map_summary"] = f"{len(element_map)} interactive elements found. Use element_id to target them."

        # Add screenshot as ui_block
        ui_blocks = []
        if screenshot_b64:
            ui_blocks.append({
                "type": "image",
                "title": f"Flow Canvas: {flow_id[:8]}...",
                "src": f"data:image/png;base64,{screenshot_b64}",
                "alt": f"Screenshot of flow {flow_id}",
            })
            data["screenshot_base64"] = screenshot_b64

        data["ui_blocks"] = ui_blocks

        return ToolResult(
            success=True,
            message=f"Viewing flow {flow_id} ({len(element_map)} interactive elements)",
            data=data,
        )

    async def _view_page(self, session: VisionSession, path: str) -> ToolResult:
        """Navigate to an app page and capture it with element map."""
        # Ensure path starts with /
        if not path.startswith("/"):
            path = f"/{path}"

        url = f"{INTERNAL_APP_URL}{path}"
        await session.page.goto(url, wait_until="networkidle", timeout=20000)
        await session.page.wait_for_timeout(1500)

        # Take screenshot with element map
        screenshot_b64, element_map = await self._capture_with_element_map(session)

        snapshot_text = ""
        try:
            snapshot = await session.page.accessibility.snapshot()
            if snapshot:
                snapshot_text = _flatten_accessibility_tree(snapshot)
                if len(snapshot_text) > 50000:
                    snapshot_text = snapshot_text[:50000] + "\n...(truncated)"
        except Exception:
            snapshot_text = "(accessibility snapshot unavailable)"

        data = {
            "url": session.page.url,
            "title": await session.page.title(),
            "path": path,
            "accessibility_snapshot": snapshot_text,
        }

        # Include element map
        if element_map:
            data["element_map"] = element_map
            data["element_map_summary"] = f"{len(element_map)} interactive elements found. Use element_id to target them."

        ui_blocks = []
        if screenshot_b64:
            ui_blocks.append({
                "type": "image",
                "title": f"Page: {path}",
                "src": f"data:image/png;base64,{screenshot_b64}",
                "alt": f"Screenshot of {path}",
            })
            data["screenshot_base64"] = screenshot_b64

        data["ui_blocks"] = ui_blocks

        return ToolResult(
            success=True,
            message=f"Viewing page {path} ({len(element_map)} interactive elements)",
            data=data,
        )

    async def _take_screenshot(self, session: VisionSession) -> ToolResult:
        """Capture screenshot of current page with element map."""
        # If on a flow page, fit view first so the screenshot matches user's view
        current_url = session.page.url
        if "/flows/" in current_url:
            try:
                fit_btn = session.page.get_by_role("button", name="Fit view")
                await fit_btn.click(timeout=3000)
                await session.page.wait_for_timeout(1000)
            except Exception:
                pass

        # Take screenshot with element map
        screenshot_b64, element_map = await self._capture_with_element_map(session)

        data = {
            "url": session.page.url,
            "title": await session.page.title(),
        }

        if element_map:
            data["element_map"] = element_map
            data["element_map_summary"] = f"{len(element_map)} interactive elements found."

        ui_blocks = []
        if screenshot_b64:
            ui_blocks.append({
                "type": "image",
                "title": "Current Page",
                "src": f"data:image/png;base64,{screenshot_b64}",
                "alt": f"Screenshot of {session.page.url}",
            })
            data["screenshot_base64"] = screenshot_b64

        data["ui_blocks"] = ui_blocks

        return ToolResult(
            success=True,
            message=f"Screenshot captured ({len(element_map)} interactive elements)",
            data=data,
        )

    async def _verify_canvas(self, session: VisionSession, flow_id: Optional[str] = None) -> ToolResult:
        """Verify canvas layout — check for visual issues via JS inspection."""
        # Navigate to flow if flow_id provided and not already there
        if flow_id:
            current_path = session.page.url.split(INTERNAL_APP_URL)[-1] if INTERNAL_APP_URL in session.page.url else ""
            if f"/flows/{flow_id}" not in current_path:
                url = f"{INTERNAL_APP_URL}/flows/{flow_id}"
                await session.page.goto(url, wait_until="networkidle", timeout=20000)
                await session.page.wait_for_timeout(2000)

        # Extract React Flow state via JS
        canvas_state = await session.page.evaluate("""
            () => {
                // Try to access React Flow store from the DOM
                const reactFlowWrapper = document.querySelector('.react-flow');
                if (!reactFlowWrapper) return { error: 'No React Flow canvas found' };

                // Count visible nodes and edges
                const nodeElements = document.querySelectorAll('.react-flow__node');
                const edgeElements = document.querySelectorAll('.react-flow__edge');

                const nodes = [];
                nodeElements.forEach(el => {
                    const rect = el.getBoundingClientRect();
                    const label = el.querySelector('.node-label, [data-label], h3, .font-semibold');
                    nodes.push({
                        id: el.getAttribute('data-id') || '',
                        label: label ? label.textContent?.trim() : '',
                        x: Math.round(rect.left),
                        y: Math.round(rect.top),
                        width: Math.round(rect.width),
                        height: Math.round(rect.height),
                        visible: rect.width > 0 && rect.height > 0,
                        truncated: label ? label.scrollWidth > label.clientWidth : false,
                    });
                });

                return {
                    nodeCount: nodeElements.length,
                    edgeCount: edgeElements.length,
                    nodes: nodes,
                    viewportWidth: window.innerWidth,
                    viewportHeight: window.innerHeight,
                };
            }
        """)

        # Take verification screenshot
        screenshot_b64 = await self._capture_screenshot(session.page)

        # Analyze results
        issues = []
        if isinstance(canvas_state, dict) and "error" not in canvas_state:
            for node in canvas_state.get("nodes", []):
                if not node.get("visible"):
                    issues.append(f"Node '{node.get('label', node.get('id'))}' is not visible (off-screen or zero-size)")
                if node.get("truncated"):
                    issues.append(f"Node '{node.get('label', node.get('id'))}' has truncated label text")

            if canvas_state.get("nodeCount", 0) > 0 and canvas_state.get("edgeCount", 0) == 0:
                issues.append("Canvas has nodes but no edges — all nodes are disconnected")

        data = {
            "canvas_state": canvas_state,
            "issues": issues,
            "issue_count": len(issues),
        }

        ui_blocks = []
        if screenshot_b64:
            issue_label = f" ({len(issues)} issues)" if issues else " (no issues)"
            ui_blocks.append({
                "type": "image",
                "title": f"Canvas Verification{issue_label}",
                "src": f"data:image/png;base64,{screenshot_b64}",
                "alt": f"Canvas verification screenshot - {len(issues)} issues found",
            })
            data["screenshot_base64"] = screenshot_b64

        data["ui_blocks"] = ui_blocks

        return ToolResult(
            success=True,
            message=f"Canvas verified: {len(issues)} issue(s) found" if issues else "Canvas verified: no issues",
            data=data,
        )

    async def _learn_page(self, session: VisionSession, path: Optional[str] = None) -> ToolResult:
        """Extract comprehensive UI structure for AI learning."""
        if path:
            if not path.startswith("/"):
                path = f"/{path}"
            url = f"{INTERNAL_APP_URL}{path}"
            await session.page.goto(url, wait_until="networkidle", timeout=20000)
            await session.page.wait_for_timeout(1500)

        # Deep UI structure extraction
        ui_structure = await session.page.evaluate("""
            () => {
                const result = {
                    url: window.location.pathname,
                    title: document.title,
                    headings: [],
                    forms: [],
                    buttons: [],
                    links: [],
                    tables: [],
                    interactiveElements: [],
                };

                // Extract headings
                document.querySelectorAll('h1, h2, h3, h4').forEach(h => {
                    result.headings.push({
                        level: h.tagName,
                        text: h.textContent?.trim().slice(0, 100),
                    });
                });

                // Extract forms and their fields
                document.querySelectorAll('form, [role="form"]').forEach(form => {
                    const fields = [];
                    form.querySelectorAll('input, select, textarea').forEach(el => {
                        fields.push({
                            type: el.tagName.toLowerCase() + (el.type ? `:${el.type}` : ''),
                            name: el.name || el.id || el.placeholder || '',
                            label: el.closest('label')?.textContent?.trim().slice(0, 50) || '',
                        });
                    });
                    if (fields.length > 0) {
                        result.forms.push({ fields });
                    }
                });

                // Extract buttons
                document.querySelectorAll('button, [role="button"]').forEach(btn => {
                    const text = btn.textContent?.trim().slice(0, 50);
                    if (text) result.buttons.push(text);
                });

                // Extract navigation links
                document.querySelectorAll('nav a, [role="navigation"] a').forEach(a => {
                    result.links.push({
                        text: a.textContent?.trim().slice(0, 50),
                        href: a.getAttribute('href') || '',
                    });
                });

                // Extract tabs
                document.querySelectorAll('[role="tab"]').forEach(tab => {
                    result.interactiveElements.push({
                        type: 'tab',
                        text: tab.textContent?.trim().slice(0, 50),
                        selected: tab.getAttribute('aria-selected') === 'true',
                    });
                });

                return result;
            }
        """)

        # Get accessibility snapshot
        snapshot_text = ""
        try:
            snapshot = await session.page.accessibility.snapshot()
            if snapshot:
                snapshot_text = _flatten_accessibility_tree(snapshot)
                if len(snapshot_text) > 30000:
                    snapshot_text = snapshot_text[:30000] + "\n...(truncated)"
        except Exception:
            snapshot_text = "(accessibility snapshot unavailable)"

        data = {
            "ui_structure": ui_structure,
            "accessibility_snapshot": snapshot_text,
            "page_path": ui_structure.get("url", path or session.page.url),
        }

        return ToolResult(
            success=True,
            message=f"Page structure extracted for {ui_structure.get('url', 'current page')}",
            data=data,
        )

    async def _capture_screenshot(self, page) -> Optional[str]:
        """Capture page screenshot as base64, with size limit."""
        try:
            screenshot_bytes = await page.screenshot(type="png", full_page=False)
            if len(screenshot_bytes) > MAX_SCREENSHOT_SIZE:
                # Retry with JPEG for smaller size
                screenshot_bytes = await page.screenshot(type="jpeg", quality=70, full_page=False)
            return base64.b64encode(screenshot_bytes).decode()
        except Exception as e:
            logger.warning(f"Screenshot capture failed: {e}")
            return None

    # ====================================================================
    # CanvasTouch — Interaction Actions
    # ====================================================================

    async def _navigate_to(self, session: VisionSession, path: str):
        """Navigate to a page before performing a CanvasTouch action."""
        if not path.startswith("/"):
            path = f"/{path}"
        current_path = ""
        try:
            from urllib.parse import urlparse
            current_path = urlparse(session.page.url).path
        except Exception:
            pass
        if current_path != path:
            url = f"{INTERNAL_APP_URL}{path}"
            logger.info(f"CanvasTouch: navigating to {path} before interaction")
            await session.page.goto(url, wait_until="networkidle", timeout=15000)
            # Wait for SPA content to render (settings, flows pages load async data)
            await session.page.wait_for_timeout(3000)
            # Extra wait: if page shows a loading indicator, wait for it to disappear
            try:
                loading = session.page.locator("[role='status']:has-text('Loading')")
                if await loading.count() > 0:
                    await loading.first.wait_for(state="hidden", timeout=10000)
                    await session.page.wait_for_timeout(500)
            except Exception:
                pass

    async def _find_element(self, page, selector: str = "", text: str = ""):
        """
        Find an interactive element by CSS selector or visible text.

        Fallback chain: exact CSS → ARIA role+name → exact text → fuzzy text →
        contextual (scoped to node subtree) → dialog-scoped → label/placeholder.

        Returns (locator, description_string).  locator is None when nothing
        matched and description_string explains why.
        """
        logger.info(f"_find_element: selector='{selector}', text='{text}', url={page.url}")
        # Auto-detect: if selector looks like plain text (no CSS special chars) and text is empty,
        # treat selector as text. The AI sometimes puts the target label in "selector" instead of "text".
        import re as _re
        if selector and not text and not _re.search(r'[.#\[\]>+~:=(){}@]', selector):
            logger.info(f"_find_element: selector '{selector}' looks like text, treating as text search")
            text = selector
            selector = ""

        # Strategy 1: CSS selector
        if selector:
            loc = page.locator(selector)
            try:
                count = await loc.count()
                if count > 0:
                    first = loc.first
                    try:
                        if await first.is_visible(timeout=2000):
                            return first, f"CSS '{selector}' ({count} match{'es' if count > 1 else ''})"
                    except Exception:
                        # Visible check timed out — still return the locator
                        return first, f"CSS '{selector}' (not confirmed visible)"
            except Exception:
                pass

        # Strategy 2: ARIA role + name (buttons, links, tabs are the most
        #             common interactive targets)
        if text:
            for role in ("button", "link", "tab", "menuitem", "checkbox", "radio", "option"):
                loc = page.get_by_role(role, name=text)
                try:
                    count = await loc.count()
                    if count > 0:
                        el = loc.first
                        try:
                            if await el.is_visible(timeout=1500):
                                return el, f"role={role} name='{text}'"
                        except Exception:
                            return el, f"role={role} name='{text}' (visibility unconfirmed)"
                except Exception:
                    continue

            # Strategy 2b: Form field roles (textbox, combobox, spinbutton)
            # — catches inputs whose accessible name comes from <label htmlFor>
            for role in ("textbox", "combobox", "spinbutton"):
                loc = page.get_by_role(role, name=text)
                try:
                    count = await loc.count()
                    if count > 0:
                        el = loc.first
                        try:
                            if await el.is_visible(timeout=1500):
                                return el, f"role={role} name='{text}'"
                        except Exception:
                            return el, f"role={role} name='{text}' (visibility unconfirmed)"
                except Exception:
                    continue

            # Strategy 3: Exact text match
            loc = page.get_by_text(text, exact=True)
            try:
                count = await loc.count()
                if count > 0:
                    el = loc.first
                    try:
                        if await el.is_visible(timeout=1500):
                            return el, f"exact text '{text}'"
                    except Exception:
                        return el, f"exact text '{text}' (visibility unconfirmed)"
            except Exception:
                pass

            # Strategy 4: Contains text (looser)
            loc = page.get_by_text(text)
            try:
                count = await loc.count()
                if count > 0:
                    return loc.first, f"text containing '{text}'"
            except Exception:
                pass

            # Strategy 5: Label / placeholder (useful for form fields)
            loc = page.get_by_label(text)
            try:
                count = await loc.count()
                if count > 0:
                    return loc.first, f"label '{text}'"
            except Exception:
                pass

            loc = page.get_by_placeholder(text)
            try:
                count = await loc.count()
                if count > 0:
                    return loc.first, f"placeholder '{text}'"
            except Exception:
                pass

            # Strategy 6: Fuzzy text matching — "Save" matches "Save Flow", "Save Changes", etc.
            # Uses JS to find elements where text starts with or contains the search term
            try:
                fuzzy_result = await page.evaluate("""
                    (searchText) => {
                        const lower = searchText.toLowerCase();
                        const candidates = [];
                        // Check buttons, links, tabs
                        for (const el of document.querySelectorAll('button, a, [role="button"], [role="tab"], [role="link"], [role="menuitem"]')) {
                            const elText = (el.textContent || '').trim().toLowerCase();
                            if (elText && elText !== lower && elText.includes(lower)) {
                                const rect = el.getBoundingClientRect();
                                if (rect.width > 0 && rect.height > 0) {
                                    candidates.push({
                                        text: el.textContent.trim().slice(0, 80),
                                        tag: el.tagName.toLowerCase(),
                                        id: el.id || '',
                                        classes: Array.from(el.classList).slice(0, 3).join('.'),
                                        score: elText.startsWith(lower) ? 1 : 0,
                                    });
                                }
                            }
                        }
                        // Sort by best match (starts-with > contains)
                        candidates.sort((a, b) => b.score - a.score);
                        return candidates.length > 0 ? candidates[0] : null;
                    }
                """, text)
                if fuzzy_result:
                    # Build selector from the fuzzy match result
                    if fuzzy_result.get("id"):
                        fuzzy_loc = page.locator(f"#{fuzzy_result['id']}")
                    elif fuzzy_result.get("classes"):
                        fuzzy_loc = page.locator(f"{fuzzy_result['tag']}.{fuzzy_result['classes']}")
                    else:
                        fuzzy_loc = page.get_by_text(fuzzy_result["text"], exact=True)
                    if await fuzzy_loc.count() > 0:
                        matched_text = fuzzy_result.get("text", "")
                        logger.info(f"_find_element: fuzzy match '{text}' -> '{matched_text}'")
                        return fuzzy_loc.first, f"fuzzy match '{text}' -> '{matched_text}'"
            except Exception as e:
                logger.debug(f"Fuzzy matching failed: {e}")

            # Strategy 6b: Contextual targeting — scope to a React Flow node subtree
            # "the delete button on S3_Archive node" → finds button inside that node's DOM
            if " on " in text.lower() or " in " in text.lower():
                try:
                    # Split "X on Y" or "X in Y" to extract action target and scope
                    for sep in (" on ", " in "):
                        if sep in text.lower():
                            parts = text.lower().split(sep, 1)
                            action_text = parts[0].strip()
                            scope_text = parts[1].strip()
                            # Find the scope element (e.g., a React Flow node)
                            scope_loc = page.get_by_text(scope_text)
                            if await scope_loc.count() > 0:
                                scope_el = scope_loc.first
                                # Search within the scope for the action target
                                for role in ("button", "link", "tab"):
                                    inner = scope_el.get_by_role(role, name=action_text)
                                    if await inner.count() > 0:
                                        return inner.first, f"'{action_text}' scoped to '{scope_text}'"
                                # Fallback: text within scope
                                inner = scope_el.get_by_text(action_text)
                                if await inner.count() > 0:
                                    return inner.first, f"text '{action_text}' within '{scope_text}'"
                            break
                except Exception as e:
                    logger.debug(f"Contextual targeting failed: {e}")

        # Strategy 7: Dialog-scoped search — Radix dialogs render in portals.
        # If a dialog is open, search within it using label + id association.
        if text:
            try:
                dialog = page.locator("[role='dialog']")
                if await dialog.count() > 0:
                    # Look for input/textarea associated with a label inside the dialog
                    dialog_el = dialog.first
                    # Try label-linked input
                    label_loc = dialog_el.locator(f"label:has-text('{text}')")
                    if await label_loc.count() > 0:
                        label_for = await label_loc.first.get_attribute("for")
                        if label_for:
                            input_loc = dialog_el.locator(f"#{label_for}")
                            if await input_loc.count() > 0:
                                return input_loc.first, f"dialog label '{text}' → #{label_for}"
                    # Try any input/textarea/select near the label text
                    for input_tag in ("input", "textarea", "select", "[role='combobox']"):
                        inputs = dialog_el.locator(input_tag)
                        input_count = await inputs.count()
                        for i in range(input_count):
                            inp = inputs.nth(i)
                            try:
                                if await inp.is_visible(timeout=500):
                                    # Check if this input's label or placeholder matches
                                    ph = await inp.get_attribute("placeholder") or ""
                                    aria = await inp.get_attribute("aria-label") or ""
                                    if text.lower() in ph.lower() or text.lower() in aria.lower():
                                        return inp, f"dialog {input_tag} placeholder/aria '{text}'"
                            except Exception:
                                continue
            except Exception as e:
                logger.debug(f"Dialog-scoped search failed: {e}")

        # Log what we tried for debugging
        target = selector or text or "(none)"
        # Check if page has any dialog open
        try:
            _dialog_count = await page.locator("[role='dialog']").count()
            _textbox_count = await page.locator("input, textarea").count()
            logger.warning(
                f"_find_element FAILED for '{target}': "
                f"url={page.url}, dialogs={_dialog_count}, inputs={_textbox_count}"
            )
        except Exception:
            logger.warning(f"_find_element FAILED for '{target}'")
        return None, f"Element not found for: {target}"

    def _make_touch_result(
        self, action_name: str, description: str, screenshot_b64: Optional[str],
        url: str, extra_data: Optional[Dict[str, Any]] = None,
    ) -> ToolResult:
        """Build a ToolResult with screenshot UI block for a CanvasTouch action."""
        data: Dict[str, Any] = {
            "touch_action": action_name,
            "target": description,
            "url_after": url,
        }
        if extra_data:
            data.update(extra_data)

        ui_blocks: List[Dict[str, Any]] = []
        if screenshot_b64:
            ui_blocks.append({
                "type": "image",
                "title": f"CanvasTouch {action_name}: {description[:40]}",
                "src": f"data:image/png;base64,{screenshot_b64}",
                "alt": f"Screenshot after {action_name} on {description}",
            })
            data["screenshot_base64"] = screenshot_b64
        data["ui_blocks"] = ui_blocks

        return ToolResult(success=True, message=f"CanvasTouch {action_name}: {description}", data=data)

    # ── click ──────────────────────────────────────────────────────────

    async def _touch_click(self, session: VisionSession, selector: str, text: str) -> ToolResult:
        """Click a button, link, tab, or any visible element. Includes retry on transient failures."""
        if not selector and not text:
            return ToolResult(success=False, error="Provide 'selector' or 'text' to identify what to click")

        page = session.page

        # Wait for DOM stability before interacting
        await self._wait_for_stable(page)

        element, desc = await self._find_element(page, selector, text)
        if element is None:
            # Retry once after a brief wait (element may still be loading)
            await page.wait_for_timeout(1000)
            element, desc = await self._find_element(page, selector, text)
            if element is None:
                return ToolResult(success=False, error=desc)

        # Capture element info before clicking
        try:
            el_info = await element.evaluate(
                "el => ({ tag: el.tagName, text: (el.textContent || '').trim().slice(0, 100), "
                "type: el.type || '', href: el.href || '' })"
            )
        except Exception:
            el_info = {}

        logger.info(f"_touch_click: found={desc}, tag={el_info.get('tag','?')}, text='{el_info.get('text','')[:40]}'")

        url_before = page.url
        try:
            await element.click(timeout=5000)
        except Exception as e:
            # Retry once on transient failure (animation in progress, element intercepted)
            logger.warning(f"_touch_click: first click failed ({e}), retrying after 1s")
            await page.wait_for_timeout(1000)
            await element.click(timeout=5000)
        # Wait for page to react (navigation, modal, animation)
        await page.wait_for_timeout(1500)

        # If a dialog/modal appeared after the click, wait for it to stabilize
        try:
            dialog = page.locator("[role='dialog'], [data-state='open'], .modal, .dialog")
            dialog_count = await dialog.count()
            logger.info(f"_touch_click: after click, dialog count={dialog_count}")
            if dialog_count > 0:
                await dialog.first.wait_for(state="visible", timeout=3000)
                await page.wait_for_timeout(500)  # extra settle time for animation
                logger.info("_touch_click: dialog detected after click, waited for stability")
        except Exception as e:
            logger.warning(f"_touch_click: dialog detection error: {e}")

        screenshot_b64 = await self._capture_screenshot(page)
        navigated = page.url != url_before

        return self._make_touch_result("click", desc, screenshot_b64, page.url, {
            "element_tag": el_info.get("tag", ""),
            "element_text": el_info.get("text", ""),
            "navigated": navigated,
            "url_before": url_before,
        })

    # ── fill ───────────────────────────────────────────────────────────

    async def _touch_fill(self, session: VisionSession, selector: str, text: str, value: str) -> ToolResult:
        """Type text into a form field (input, textarea). Includes retry on transient failures."""
        if not value:
            return ToolResult(success=False, error="Provide 'value' — the text to type into the field")
        if not selector and not text:
            return ToolResult(success=False, error="Provide 'selector' or 'text' to identify the field")

        page = session.page
        await self._wait_for_stable(page)

        element, desc = await self._find_element(page, selector, text)
        if element is None:
            await page.wait_for_timeout(1000)
            element, desc = await self._find_element(page, selector, text)
            if element is None:
                return ToolResult(success=False, error=desc)

        # Clear and fill
        await element.click(timeout=3000)
        await element.fill(value, timeout=5000)
        await page.wait_for_timeout(500)

        screenshot_b64 = await self._capture_screenshot(page)

        return self._make_touch_result("fill", f"{desc} → '{value[:50]}'", screenshot_b64, page.url, {
            "filled_value": value,
        })

    # ── select ─────────────────────────────────────────────────────────

    async def _touch_select(self, session: VisionSession, selector: str, text: str, value: str) -> ToolResult:
        """Select an option from a dropdown (native <select> or custom). Includes retry."""
        if not value:
            return ToolResult(success=False, error="Provide 'value' — the option text to select")
        if not selector and not text:
            return ToolResult(success=False, error="Provide 'selector' or 'text' to identify the dropdown")

        page = session.page
        await self._wait_for_stable(page)

        element, desc = await self._find_element(page, selector, text)
        if element is None:
            await page.wait_for_timeout(1000)
            element, desc = await self._find_element(page, selector, text)
            if element is None:
                return ToolResult(success=False, error=desc)

        # Determine if it's a native <select> or custom dropdown
        tag = await element.evaluate("el => el.tagName.toLowerCase()")
        role = await element.get_attribute("role") or ""

        # If we found a label/text instead of the actual dropdown trigger,
        # look for the nearest combobox or select element.
        # Radix UI pattern: label is a sibling/preceding element to combobox trigger.
        if tag not in ("select", "button") and role != "combobox":
            logger.info(f"_touch_select: found label ({tag}/{role}), searching for associated combobox")
            try:
                # Strategy A: Look for combobox in the same container (e.g., dialog)
                # Find a combobox that comes after this label in the DOM
                parent = element.locator("xpath=..")
                sibling_cb = parent.locator("[role='combobox']")
                if await sibling_cb.count() > 0:
                    element = sibling_cb.first
                    desc = f"combobox near '{text}' (sibling)"
                    tag = await element.evaluate("el => el.tagName.toLowerCase()")
                    role = "combobox"
                    logger.info(f"_touch_select: found sibling combobox")
                else:
                    # Strategy B: Search in dialog or page for combobox by aria-labelledby
                    container = page.locator("[role='dialog']") if await page.locator("[role='dialog']").count() > 0 else page
                    if await container.locator("[role='dialog']").count() > 0:
                        container = container.locator("[role='dialog']").first
                    all_comboboxes = container.locator("[role='combobox']")
                    cb_count = await all_comboboxes.count()
                    # Find the combobox that's closest to our label text
                    for i in range(cb_count):
                        cb = all_comboboxes.nth(i)
                        # Check aria-label, aria-labelledby, or preceding text
                        aria = await cb.get_attribute("aria-label") or ""
                        if text and text.lower() in aria.lower():
                            element = cb
                            desc = f"combobox aria-label '{aria}'"
                            tag = await element.evaluate("el => el.tagName.toLowerCase()")
                            role = "combobox"
                            logger.info(f"_touch_select: found combobox by aria-label")
                            break
            except Exception as e:
                logger.debug(f"_touch_select: combobox search failed: {e}")

        if tag == "select":
            # Native <select> — use Playwright's select_option
            await element.select_option(label=value, timeout=5000)
            await page.wait_for_timeout(500)
        else:
            # Custom dropdown — click to open, then click the matching option
            await element.click(timeout=3000)
            await page.wait_for_timeout(800)  # wait for dropdown animation

            # Try to find the option in the dropdown
            option_found = False
            try:
                # Radix UI Select: options appear in a [role="listbox"] portal
                option = page.get_by_role("option", name=value)
                count = await option.count()
                if count > 0:
                    await option.first.click(timeout=3000)
                    option_found = True
                else:
                    # Try exact text match inside the listbox
                    listbox = page.locator("[role='listbox']")
                    if await listbox.count() > 0:
                        lb_opt = listbox.first.get_by_text(value, exact=True)
                        if await lb_opt.count() > 0:
                            await lb_opt.first.click(timeout=3000)
                            option_found = True

                if not option_found:
                    # Fallback: try listbox item, menuitem, or plain text inside dropdown
                    for role in ("listitem", "menuitem"):
                        opt = page.get_by_role(role, name=value)
                        if await opt.count() > 0:
                            await opt.first.click(timeout=3000)
                            option_found = True
                            break
                if not option_found:
                    # Last resort: click text that appeared after opening
                    opt = page.get_by_text(value, exact=True)
                    if await opt.count() > 0:
                        await opt.first.click(timeout=3000)
                        option_found = True
                    else:
                        # Try partial text match (e.g., "14 days" when user says "14")
                        opt = page.get_by_text(value)
                        if await opt.count() > 0:
                            await opt.first.click(timeout=3000)
                            option_found = True
                        else:
                            return ToolResult(
                                success=False,
                                error=f"Opened dropdown but could not find option '{value}'",
                            )
            except Exception as e:
                return ToolResult(success=False, error=f"Failed to select option '{value}': {e}")

            await page.wait_for_timeout(500)

        screenshot_b64 = await self._capture_screenshot(page)

        return self._make_touch_result("select", f"{desc} → '{value}'", screenshot_b64, page.url, {
            "selected_value": value,
            "dropdown_type": "native" if tag == "select" else "custom",
        })

    # ── hover ──────────────────────────────────────────────────────────

    async def _touch_hover(self, session: VisionSession, selector: str, text: str) -> ToolResult:
        """Hover over an element to reveal tooltips, submenus, or popover content."""
        if not selector and not text:
            return ToolResult(success=False, error="Provide 'selector' or 'text' to identify what to hover")

        page = session.page
        await self._wait_for_stable(page)

        element, desc = await self._find_element(page, selector, text)
        if element is None:
            return ToolResult(success=False, error=desc)

        await element.hover(timeout=5000)
        # Wait for tooltip/popover animation
        await page.wait_for_timeout(1000)

        screenshot_b64 = await self._capture_screenshot(page)

        # Check if any tooltip/popover appeared
        tooltip_text = ""
        try:
            for tip_sel in ('[role="tooltip"]', ".tooltip", "[data-tooltip]", ".popover", '[role="dialog"]'):
                tip = page.locator(tip_sel)
                if await tip.count() > 0:
                    tooltip_text = await tip.first.text_content() or ""
                    tooltip_text = tooltip_text.strip()[:200]
                    break
        except Exception:
            pass

        return self._make_touch_result("hover", desc, screenshot_b64, page.url, {
            "tooltip_text": tooltip_text,
            "tooltip_found": bool(tooltip_text),
        })

    # ── scroll ─────────────────────────────────────────────────────────

    async def _touch_scroll(
        self, session: VisionSession, selector: str, direction: str, amount: int
    ) -> ToolResult:
        """Scroll the page or a specific container."""
        page = session.page
        amount = max(50, min(amount or 500, 5000))  # clamp between 50-5000px

        dx, dy = 0, 0
        if direction == "down":
            dy = amount
        elif direction == "up":
            dy = -amount
        elif direction == "right":
            dx = amount
        elif direction == "left":
            dx = -amount

        if selector:
            # Scroll a specific element/container
            element, desc = await self._find_element(page, selector, "")
            if element is None:
                return ToolResult(success=False, error=desc)
            await element.evaluate(
                f"el => el.scrollBy({{ left: {dx}, top: {dy}, behavior: 'smooth' }})"
            )
            target_desc = f"container '{selector}' {direction} {amount}px"
        else:
            # Scroll the whole page
            await page.evaluate(
                f"window.scrollBy({{ left: {dx}, top: {dy}, behavior: 'smooth' }})"
            )
            target_desc = f"page {direction} {amount}px"

        # Wait for scroll animation + lazy-loaded content
        await page.wait_for_timeout(800)

        screenshot_b64 = await self._capture_screenshot(page)

        # Get scroll position
        scroll_pos = await page.evaluate(
            "() => ({ x: Math.round(window.scrollX), y: Math.round(window.scrollY), "
            "maxY: document.body.scrollHeight - window.innerHeight })"
        )

        return self._make_touch_result("scroll", target_desc, screenshot_b64, page.url, {
            "direction": direction,
            "amount": amount,
            "scroll_x": scroll_pos.get("x", 0),
            "scroll_y": scroll_pos.get("y", 0),
            "scroll_max_y": scroll_pos.get("maxY", 0),
        })

    # ── drag ──────────────────────────────────────────────────────────

    async def _touch_drag(
        self, session: VisionSession, selector: str, text: str, dx: float, dy: float
    ) -> ToolResult:
        """Drag an element (e.g., a canvas node) by dx/dy pixels.

        Temporarily enables the __aiAllowDrag flag so the drag-prevention
        init script lets this intentional move through.
        """
        if not selector and not text:
            return ToolResult(success=False, error="Provide 'selector' or 'text' to identify what to drag")
        if not dx and not dy:
            return ToolResult(success=False, error="Provide 'dx' and/or 'dy' — pixels to drag (positive=right/down)")

        page = session.page
        element, desc = await self._find_element(page, selector, text)
        if element is None:
            return ToolResult(success=False, error=desc)

        # Get the element's bounding box to calculate start position
        box = await element.bounding_box()
        if not box:
            return ToolResult(success=False, error=f"Could not get position of element: {desc}")

        start_x = box["x"] + box["width"] / 2
        start_y = box["y"] + box["height"] / 2
        end_x = start_x + (dx or 0)
        end_y = start_y + (dy or 0)

        logger.info(
            f"_touch_drag: {desc} from ({start_x:.0f},{start_y:.0f}) "
            f"to ({end_x:.0f},{end_y:.0f}) delta=({dx},{dy})"
        )

        # Enable drag bypass so the init script lets this through
        await page.evaluate("window.__aiAllowDrag = true")

        try:
            # Perform the drag: mouse down → move → up
            await page.mouse.move(start_x, start_y)
            await page.mouse.down()
            # Move in steps for smooth drag (React Flow needs intermediate events)
            steps = max(5, int(max(abs(dx or 0), abs(dy or 0)) / 20))
            for i in range(1, steps + 1):
                t = i / steps
                await page.mouse.move(
                    start_x + (dx or 0) * t,
                    start_y + (dy or 0) * t,
                )
                await page.wait_for_timeout(20)
            await page.mouse.up()
        finally:
            # Always re-enable drag prevention
            await page.evaluate("window.__aiAllowDrag = false")

        await page.wait_for_timeout(500)
        screenshot_b64 = await self._capture_screenshot(page)

        return self._make_touch_result("drag", f"{desc} by ({dx},{dy})px", screenshot_b64, page.url, {
            "start_x": round(start_x),
            "start_y": round(start_y),
            "end_x": round(end_x),
            "end_y": round(end_y),
            "dx": dx,
            "dy": dy,
        })


# ============================================================================
# Tool Registration Helper
# ============================================================================

class CanvasVisionTools:
    """Container for canvas vision tool registration."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [CanvasVisionTool()]

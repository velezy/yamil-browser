"""
yamil_browser_client.py — Python SDK for the yamil-browser microservice.

Copy this single file into any Python project that needs stealth browser
automation.  No additional dependencies beyond httpx.

Environment variable:
    YAMIL_BROWSER_URL  — base URL of the running service
                         (default: http://localhost:4000)

Usage:
    from yamil_browser_client import YamilBrowserClient

    async with YamilBrowserClient() as browser:
        await browser.navigate("https://example.com")
        html  = await browser.content()
        png   = await browser.screenshot()   # base64 PNG
        title = (await browser.get_url())["title"]
"""

import base64
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

YAMIL_BROWSER_URL = os.getenv("YAMIL_BROWSER_URL", "http://localhost:4000")


class YamilBrowserClient:
    """Async HTTP client for the yamil-browser REST API.

    Each instance represents one isolated browser session (its own Playwright
    browser context).  Sessions are automatically destroyed on close.
    """

    def __init__(self, base_url: str = YAMIL_BROWSER_URL, timeout: float = 30.0):
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._session_id: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self):
        """Create a new browser session on the service."""
        self._client = httpx.AsyncClient(timeout=self._timeout)
        resp = await self._client.post(f"{self._base}/sessions")
        resp.raise_for_status()
        self._session_id = resp.json()["id"]
        logger.info("YamilBrowserClient: session %s created", self._session_id)

    async def close(self):
        """Destroy the session and close the HTTP connection pool."""
        if self._session_id and self._client:
            try:
                await self._client.delete(
                    f"{self._base}/sessions/{self._session_id}"
                )
            except Exception as exc:
                logger.warning("YamilBrowserClient close error: %s", exc)
        if self._client:
            await self._client.aclose()
        self._session_id = None
        self._client = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *_):
        await self.close()

    # ── internal helpers ──────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self._base}/sessions/{self._session_id}{path}"

    async def _post(self, path: str, body: dict = None) -> dict:
        resp = await self._client.post(self._url(path), json=body or {})
        resp.raise_for_status()
        return resp.json()

    async def _get(self, path: str) -> dict:
        resp = await self._client.get(self._url(path))
        resp.raise_for_status()
        return resp.json()

    # ── navigation ────────────────────────────────────────────────────────────

    async def navigate(self, url: str, wait_until: str = "domcontentloaded") -> dict:
        """Navigate to *url*. Returns {url, title}."""
        return await self._post("/navigate", {"url": url, "waitUntil": wait_until})

    async def go_back(self) -> dict:
        return await self._post("/back")

    async def press(self, key: str) -> dict:
        """Press a keyboard key (Enter, F5, Escape, Tab, ArrowDown, …)."""
        return await self._post("/press", {"key": key})

    async def reload(self) -> dict:
        return await self.press("F5")

    # ── page data ─────────────────────────────────────────────────────────────

    async def get_url(self) -> dict:
        """Return {url, title} for the current page."""
        return await self._get("/url")

    async def content(self) -> str:
        """Return the full HTML source of the current page."""
        data = await self._get("/content")
        return data.get("content", "")

    async def evaluate(self, script: str) -> Any:
        """Evaluate *script* in the page context and return the result."""
        data = await self._post("/evaluate", {"script": script})
        return data.get("result")

    async def cookies(self) -> List[dict]:
        """Return all cookies for the current page."""
        data = await self._get("/cookies")
        return data.get("cookies", [])

    # ── screenshot ────────────────────────────────────────────────────────────

    async def screenshot(self, full_page: bool = False) -> str:
        """Return a base64-encoded JPEG screenshot of the current viewport."""
        resp = await self._client.get(self._url("/screenshot"))
        resp.raise_for_status()
        return base64.b64encode(resp.content).decode()

    async def screenshot_bytes(self, full_page: bool = False) -> bytes:
        """Return raw JPEG bytes."""
        resp = await self._client.get(self._url("/screenshot"))
        resp.raise_for_status()
        return resp.content

    # ── interactions ──────────────────────────────────────────────────────────

    async def click(self, selector: str) -> dict:
        """Click the first element matching *selector*."""
        return await self._post("/click", {"selector": selector})

    async def fill(self, selector: str, value: str) -> dict:
        """Clear and fill an input matching *selector* with *value*."""
        return await self._post("/fill", {"selector": selector, "value": value})

    async def select(self, selector: str, value: str) -> dict:
        """Select an <option> by value in a <select> element."""
        return await self._post("/select", {"selector": selector, "value": value})

    async def hover(self, selector: str) -> dict:
        """Hover the mouse over an element."""
        return await self._post("/hover", {"selector": selector})

    async def scroll(self, direction: str = "down", amount: int = 500) -> dict:
        """Scroll the page. direction: 'up' | 'down', amount: pixels."""
        return await self._post("/scroll", {"direction": direction, "amount": amount})

    async def wait_for(self, selector: str, timeout: int = 10000) -> dict:
        """Wait until *selector* is visible (timeout in ms)."""
        return await self._post("/wait", {"selector": selector, "timeout": timeout})

    # ── mouse / keyboard (coordinate-based) ───────────────────────────────────

    async def mouse_click(self, x: int, y: int) -> dict:
        return await self._post("/mouse/click", {"x": x, "y": y})

    async def mouse_move(self, x: int, y: int) -> dict:
        return await self._post("/mouse/move", {"x": x, "y": y})

    async def keyboard_type(self, text: str) -> dict:
        return await self._post("/keyboard/type", {"text": text})

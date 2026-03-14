# YAMIL Browser - Claude Code Rules

## CRITICAL: Use YAMIL Browser MCP Tools, Not Playwright or curl

This is the **YAMIL Browser** project — an Electron desktop browser application. When interacting with pages displayed in YAMIL Browser, you MUST use the `yamil_browser_*` MCP tools. **NEVER** use:

1. **Playwright MCP tools** (`browser_click`, `browser_fill_form`, `browser_snapshot`, `browser_navigate`, etc.) — These control a separate Playwright browser instance, NOT the YAMIL Browser Electron app. Using them will fail or control the wrong browser.

2. **Raw curl commands** to `http://127.0.0.1:9300/*` — The MCP tools have built-in validation, image size guards, fallback chains, and error handling. Bypassing them with curl skips all of that and causes crashes (e.g., "Could not process image" errors).

3. **Read tool on screenshot files** — Never save a screenshot to a file with curl then try to Read it. The MCP tools return images directly with proper validation.

## Available MCP Tools (always use these)

### Navigation & Window
- `yamil_browser_navigate` — Go to a URL
- `yamil_browser_focus` — Bring window to foreground
- `yamil_browser_back` / `yamil_browser_forward` — History navigation

### Observation (reading page state)
- `yamil_browser_screenshot` — Take a validated screenshot (has fallback chain + size guards)
- `yamil_browser_screenshot_element` — Screenshot a specific element
- `yamil_browser_dom` — Get URL, title, text, inputs, buttons
- `yamil_browser_a11y_snapshot` — Accessibility tree (best for complex pages)
- `yamil_browser_observe` — List interactive elements
- `yamil_browser_content` — Get visible text content
- `yamil_browser_get_html` — Get raw HTML
- `yamil_browser_eval` — Run JavaScript in page context
- `yamil_browser_console_logs` — Get console messages

### Interaction
- `yamil_browser_click` — Click elements
- `yamil_browser_fill` — Fill form fields
- `yamil_browser_press` — Press keyboard keys
- `yamil_browser_scroll` — Scroll the page
- `yamil_browser_select` — Select dropdown options
- `yamil_browser_hover` — Hover over elements
- `yamil_browser_dialog` — Handle alert/confirm/prompt dialogs

### Tab Management
- `yamil_browser_tabs` — List open tabs
- `yamil_browser_new_tab` — Open a new tab
- `yamil_browser_switch_tab` — Switch tabs
- `yamil_browser_close_tab` — Close a tab

### Data & Knowledge
- `yamil_browser_bookmarks` — Manage bookmarks
- `yamil_browser_history` — Browse history
- `yamil_browser_cookies` — Cookie management
- `yamil_browser_credentials` — Credential store

## Architecture Quick Reference

- **Electron app** (`electron-app/main.js`): HTTP control server on port 9300, window management
- **Browser service** (`browser-service/`): Playwright-based headless sessions on port 4000 (for stealth tabs)
- **MCP server** (`mcp-server/`): MCP tool definitions that wrap the above APIs with validation
- Regular tabs use Electron webview; stealth tabs use browser-service Playwright sessions

## Screenshot Pipeline

The `yamil_browser_screenshot` MCP tool does:
1. Try webview screenshot via GET `/screenshot?quality=35&maxBytes=350000`
2. If invalid, fall back to whole-window capture via GET `/window-screenshot`
3. Validate image bytes (check JPEG/PNG magic bytes, minimum 200 bytes)
4. Reject if > 400KB (advise using `yamil_browser_dom` or `yamil_browser_a11y_snapshot`)
5. Return as base64 image content to Claude

**Never bypass this pipeline.**

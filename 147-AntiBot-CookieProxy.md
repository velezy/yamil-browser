# 147 - Anti-Bot Cookie Proxy via Headful Chrome

## Problem

Sites with aggressive bot detection (PerimeterX, Akamai) block all non-homepage pages in the YAMIL Browser Electron app. Home Depot was the primary example — the homepage loaded fine, but any search, product, or category page returned a custom "Error Page" with HTTP 403.

### Root Cause

PerimeterX performs **server-side TLS fingerprinting (JA3/JA4)**. Electron's bundled Chromium (even when upgraded to v36/Chrome 146) has a different BoringSSL build with different cipher suite ordering and TLS extension sets compared to Google's official Chrome binary. This fingerprint difference is detected at the network level before any JavaScript executes.

### What Didn't Work

| Approach | Result |
|----------|--------|
| User-Agent spoofing | Already in place, not sufficient |
| Stealth script (navigator.webdriver, canvas noise, WebGL spoofing) | No effect — server-side check |
| Sec-Ch-Ua client hints headers | No effect — TLS fingerprint is the gate |
| Upgrading Electron 33→36 (Chromium 130→146) | Same TLS fingerprint family, still blocked |
| `--disable-blink-features=AutomationControlled` | No effect on server-side checks |
| Ad blocker whitelist for the domain | No effect — PX scripts loaded but verdict was still "bot" |
| Playwright with real Chrome `executablePath` | Blocked — Playwright's CDP control is detectable |
| Raw Chrome `--headless=new` with CDP | Blocked — headless mode is detectable |
| Cookie transfer from Playwright session | Cookies carried "bot" verdict |

### What Worked

**Headful real Chrome** (visible window, no Playwright) launched with `--remote-debugging-port` and controlled via raw CDP WebSocket. This produces a genuine Chrome TLS fingerprint that passes PerimeterX validation.

## Solution: Cookie Proxy Flow

### Architecture

```
1. Electron detects "Error Page" on did-finish-load
2. Launches headful Chrome subprocess with --remote-debugging-port
3. Chrome visits homepage (PX sensor initializes)
4. Chrome navigates to target URL (PX validates, page loads)
5. Cookies exported via CDP Network.getAllCookies
6. Cookies imported into Electron's persist:yamil session
7. Headful Chrome killed
8. Electron reloads page — PX cookies pass server check
```

### Auto-Detection

In `TabManager._wireEvents()`, the `did-finish-load` event checks `document.title`. If it equals "Error Page" or "Access Denied", `_attemptCookieBypass(tab)` is triggered automatically (once per tab via `_bypassAttempted` flag).

### Cookie Import Endpoint

Added `POST /cookies/import` to Electron's control server (port 9300):

```json
POST http://127.0.0.1:9300/cookies/import
Body: { "cookies": [{ "url", "name", "value", "domain", "path", "secure", "httpOnly", "expirationDate", "sameSite" }] }
Response: { "ok": true, "imported": 30, "failed": 21, "total": 51 }
```

Uses `session.fromPartition('persist:yamil').cookies.set()` which operates at the Chromium network stack level, supporting `httpOnly` cookies (critical for PX cookies).

### Cookie Format Mapping

| CDP (Chrome) | Electron |
|-------------|----------|
| `expires` (Unix seconds) | `expirationDate` (Unix seconds) |
| `sameSite: "Strict"` | `sameSite: "strict"` |
| `sameSite: "Lax"` | `sameSite: "lax"` |
| `sameSite: "None"` | `sameSite: "no_restriction"` |
| (no url field) | `url` = `https://{domain}{path}` |

## Other Changes in This Phase

### Stealth Script Hardening

- Wrapped in IIFE with `window.__yamil_stealth__` guard to prevent redeclaration errors when injected multiple times
- Changed `const`/`let` to `var` for safety
- Injected on `did-navigate` event only (removed redundant `did-start-navigation`)
- Removed from console errors: `Identifier '_origQuery' has already been declared`

### Electron Upgrade

- Electron 33.4.11 → 36.9.5 (Chromium 130 → 146)
- UA string updated to Chrome/146
- Sec-Ch-Ua header added via `onBeforeSendHeaders`

### Browser-Service Improvements

- Uses real Chrome via `executablePath` (auto-detected from standard install paths)
- Removed `--no-sandbox` and `--disable-gpu` flags (flagged by bot detection)
- UA updated to Windows Chrome 146

### Bug Fixes

- Fixed `PROJECT_ROOT` in `mcp-server/src/services/browser-client.mjs` — was 2 levels up from `src/services/`, needed 3 to reach repo root
- Fixed stealth tab session creation — now actually calls `POST /sessions` on browser-service and sets `tab.sessionId`
- Fixed `.mcp.json` cwd path from Mac (`/Users/yaml/...`) to Windows (`C:/project/yamil-browser`)

## Files Modified

| File | Changes |
|------|---------|
| `electron-app/main.js` | Stealth script, cookie bypass, cookie import endpoint, Electron 36 UA |
| `electron-app/package.json` | Electron 33→36 upgrade |
| `electron-app/preload-stealth.js` | New file (preload webdriver cleanup) |
| `browser-service/src/sessions.js` | Real Chrome executablePath, auto-detection |
| `browser-service/src/stealth.js` | Removed sandbox/gpu flags, Windows UA |
| `mcp-server/src/services/browser-client.mjs` | Fixed PROJECT_ROOT path |
| `.mcp.json` | Fixed cwd to Windows path |

## Limitations

- Cookie bypass takes ~20 seconds (Chrome launch + homepage PX + navigate + cookie export)
- PX cookies expire (typically 30 minutes) — bypass runs again automatically on next "Error Page"
- Requires Google Chrome installed at standard path
- Headful Chrome window briefly appears during bypass (could be minimized in future)
- Some sites may re-validate TLS on every request even with valid cookies (would need screencast fallback)

## Future Improvements

1. Run bypass Chrome minimized/offscreen (`--window-position=-9999,-9999`)
2. Cache domain bypass cookies and refresh proactively before expiry
3. Screencast fallback for sites that check TLS on every request
4. Global MCP config (`~/.claude/.mcp.json`) for cross-project YAMIL Browser access

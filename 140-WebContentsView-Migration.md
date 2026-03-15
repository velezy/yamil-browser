# 140 — Migrate from `<webview>` to `WebContentsView`

**Status**: Planning
**Created**: 2026-03-15
**Scope**: Full architecture migration of tab rendering in YAMIL Browser

---

## Problem

The YAMIL Browser uses Electron's `<webview>` DOM element to render web pages. This causes:
- **Clipping** on high-DPI/Retina displays (content cut off)
- **FOUC** (Flash of Unstyled Content) — white flash before pages render
- **Rendering differences** from Chrome — extra compositor layer distorts output
- **CSS sizing bugs** — flexbox/percentage dimensions break internal viewport

## Solution

Migrate from `<webview>` (deprecated DOM element) to `WebContentsView` (native Chromium view). This is what production Electron browsers like Min use. Pages render identically to Chrome because they use the same native compositor — no iframe/embed layer.

## Key Architecture Change

**Before:** `BrowserWindow` → single renderer → `<webview>` DOM elements per tab
**After:** `BaseWindow` → toolbar `WebContentsView` (UI) + one `WebContentsView` per tab (native)

Main process manages tabs directly via `view.webContents` — no IPC chain through the renderer.

## 10-Phase Implementation

1. Create TabManager class in main.js
2. Replace BrowserWindow with BaseWindow + toolbar view
3. Add IPC bridge (preload.js) for toolbar ↔ main
4. Strip webview code from renderer.js, replace with IPC calls
5. Update HTML/CSS — transparent viewport area
6. Simplify HTTP control server (port 9300) — direct webContents access
7. Move credential auto-save/autofill to main.js
8. Move session/ad-blocker setup to main.js
9. Add resize/layout handler for sidebar + window resize
10. Replace custom context menu with native Menu.popup()

## What Claude Gains

- `webContents.capturePage()` captures native compositor pixels — same as what's on screen
- No iframe/embed distortion in screenshots
- Shorter code path: MCP → HTTP → main.js → webContents (skip renderer IPC)
- Better accessibility tree access

## Files Changed

- `electron-app/main.js` — TabManager, BaseWindow, IPC handlers, control server
- `electron-app/renderer/renderer.js` — strip webview code, use IPC
- `electron-app/renderer/index.html` — remove webview container
- `electron-app/renderer/styles.css` — remove webview styles, add viewport
- `electron-app/preload.js` — add IPC bridge for tab actions

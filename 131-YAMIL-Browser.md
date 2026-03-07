# 131 — YAMIL Browser: Architecture, Tech & Industry Comparison

**Status**: Production
**Created**: 2026-03-05
**Vision**: Project Atlas — Unified browser where AI sees and controls everything

---

## 1. What is YAMIL Browser?

YAMIL Browser is an enterprise-grade, AI-powered desktop browser that combines two tab types in a single window:

- **Yamil tabs** — Electron webview, logged into yamil-ai.com, full JavaScript DOM access
- **Stealth tabs** — Canvas-rendered via Playwright screencast, anti-detection evasion for external sites

The AI (via MCP tools) can see, navigate, click, type, extract data, and complete multi-step tasks in both tab types. The LLM sees screenshots and accessibility trees; the browser acts as the AI's hands.

### Why It Exists

Every iPaaS/integration platform (MuleSoft, Kong, Apigee) has a web UI but none let AI operate the UI natively. YAMIL Browser solves this:

1. AI assistant builds flows, configures connectors, tests APIs — all through the browser
2. Stealth tabs let AI interact with external sites (scrape docs, fill forms, test webhooks) without being blocked
3. One tool surface (`yamil_browser_*`) for both internal YAMIL work and external web automation

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    MCP Server (index.mjs)                     │
│                     88 MCP tools total                        │
│          43 browser_* + 45 yamil_browser_* tools             │
│                                                              │
│  ┌──────────────────────┐  ┌─────────────────────────────┐   │
│  │  LLM Provider Chain  │  │      Action Cache            │   │
│  │  1. Ollama qwen3-vl  │  │  (30 min TTL, 500 entries)  │   │
│  │  2. Gemini Flash     │  │  hostname|instruction → act  │   │
│  │  3. AWS Bedrock      │  └─────────────────────────────┘   │
│  │  4. Anthropic Direct │                                    │
│  └──────────────────────┘                                    │
└───────────┬────────────────────────────┬─────────────────────┘
            │                            │
   ┌────────▼────────┐         ┌────────▼─────────────┐
   │  Electron App   │         │  Browser Service      │
   │  (port 9300)    │         │  (port 4000)          │
   │                 │         │                       │
   │  ┌───────────┐  │         │  Fastify 5 +          │
   │  │ yamil tab │  │         │  Playwright 1.58      │
   │  │ (webview) │  │         │  + WebSocket streams   │
   │  ├───────────┤  │         │                       │
   │  │stealth tab│◄─┼── JPEG ─┤  Screencast → Canvas  │
   │  │ (canvas)  │  │  stream │  Input ← forwarded    │
   │  └───────────┘  │         │                       │
   │                 │         │  Anti-detection:       │
   │  Tab bar, AI    │         │  stealth.js injected   │
   │  sidebar, nav   │         │  on every page         │
   └─────────────────┘         └───────────────────────┘
```

### Component Breakdown

| Component | Stack | Lines | Purpose |
|-----------|-------|-------|---------|
| MCP Server | Node.js ESM, MCP SDK, Zod | 3,699 | 88 tools, LLM chain, vision, action cache |
| Electron App | Electron 33, IPC bridge | ~3,000 | Desktop window, tab bar, AI sidebar, control server |
| Browser Service | Fastify 5, Playwright 1.58 | 851 | Headless Chromium, sessions, stealth, screencast |

---

## 3. The Hybrid Tab Model

| Aspect | Yamil Tab | Stealth Tab |
|--------|-----------|-------------|
| Technology | Electron `<webview>` | HTML Canvas + Playwright |
| Rendering | Native HTML/CSS | JPEG stream via WebSocket |
| JS Access | Direct DOM | `evaluate()` through Playwright |
| Session | Persistent (Electron profile) | Ephemeral or profile-based |
| Use Case | YAMIL platform work | External sites, anti-detect |
| Visual Cue | Normal tab | Shield icon |
| Shortcut | `Ctrl+T` | `Ctrl+Shift+N` |
| Anti-detection | N/A (our own site) | Full stealth stack |

### Smart Routing

`main.js` inspects the active tab type and routes MCP calls transparently:

- Active tab is **yamil** → execute in webview via IPC
- Active tab is **stealth** → forward to browser-service HTTP API

The AI never needs to know which tab type is active. Same tools work everywhere.

---

## 4. Vision: Ollama qwen3-vl (Local, Free)

### LLM Provider Chain (Priority Order)

| # | Provider | Model | Cost | Requires |
|---|----------|-------|------|----------|
| 1 | **Ollama** (local) | `qwen3-vl:8b` | Free | Ollama running on localhost:11434 |
| 2 | Gemini | `gemini-2.0-flash` | Free tier | `GEMINI_API_KEY` |
| 3 | AWS Bedrock | Claude (cross-region) | Pay-per-use | Bedrock credentials |
| 4 | Anthropic Direct | Claude | Pay-per-use | `ANTHROPIC_API_KEY` |

### Why qwen3-vl?

- **Free** — runs locally, zero API cost
- **Fast** — 8B parameter model, runs on consumer GPU
- **Accurate** — 61.8% on ScreenSpot Pro (GUI navigation benchmark), rivals GPT-4o
- **Apache 2.0** — fully self-hostable, no vendor lock-in
- **Think tags** — outputs `<think>...</think>` reasoning blocks; our `extractJSON()` strips them automatically

### How Vision Works

1. Screenshot taken (PNG/JPEG)
2. Screenshot + instruction sent to LLM (Ollama first, fallback chain)
3. LLM returns structured JSON: `{ action, selector, text, ... }`
4. MCP server executes the action (click, fill, navigate, etc.)
5. Result cached (hostname + instruction → action, 30 min TTL)

Vision-powered tools: `browser_act`, `browser_observe`, `browser_extract`, `browser_run_task` (and yamil_browser equivalents)

---

## 5. Anti-Detection (stealth.js)

Injected on every page in stealth tabs. Defeats major bot detection services:

| Technique | What It Does |
|-----------|-------------|
| **WebDriver flag** | `navigator.webdriver` → `undefined` |
| **Chrome runtime** | Spoofed to look like real Chrome |
| **AutomationControlled** | Chromium flag disabled |
| **Languages** | `navigator.languages` → `['en-US', 'en']` |
| **Plugins** | Fake plugin array (5 entries) |
| **Hardware concurrency** | `navigator.hardwareConcurrency` → 8 |
| **WebRTC leak prevention** | RTCPeerConnection proxied, STUN servers filtered |
| **Canvas fingerprint** | `toDataURL()` + `toBlob()` → random pixel noise (±1%) |
| **WebGL fingerprint** | `getParameter()` → "Intel Inc." + "Intel Iris OpenGL Engine" |

### Chromium Launch Args

```
--disable-blink-features=AutomationControlled
--disable-automation
--disable-infobars
--window-size=1920,1080
--lang=en-US
--disable-background-timer-throttling
--disable-backgrounding-occluded-windows
--disable-renderer-backgrounding
```

---

## 6. All 88 MCP Tools

### Standard Playwright Tools (43 `browser_*`)

**Navigation**: navigate, go_back, go_forward, get_url, network_idle
**Interaction**: click, type, fill, press, hover, double_click, right_click, select, drag, scroll, dialog, set_files
**Observation**: screenshot, screenshot_element, content, get_html, head, observe, extract
**Tabs**: new_tab, switch_tab, list_tabs, close_tab
**Data**: get_cookies, clear_cookies, download, pdf, evaluate
**AI Vision**: act, extract, observe, run_task
**Session**: status, close, resize

### YAMIL Desktop Tools (45 `yamil_browser_*`)

All of the above **plus**:
**Bookmarks**: bookmark, bookmarks, bookmark_search
**History**: history
**AI Privacy**: ai_privacy (block AI from seeing certain domains)
**Context**: tab_context (get all tab info for multi-tab reasoning)
**Skills**: skills (summarize, extract, translate, explain, custom)
**Zoom**: zoom (per-tab zoom level)
**Lifecycle**: start, stop, status

### Monaco Editor Detection

Problem: Monaco editors (used in YAMIL's Python Transform, YQL, etc.) ignore DOM events.
Solution: Auto-detected and handled via `window.monaco.editor.getEditors()[0].setValue(code)`.
Built into: `yamil_browser_fill`, `yamil_browser_type`, `browser_fill`, `browser_type`

---

## 7. Browser Service API

Port 4000. Fastify + Playwright. Full REST + WebSocket.

### Sessions
```
POST   /sessions                    Create session (profile, viewport, stealth)
GET    /sessions                    List all sessions
DELETE /sessions/:id                Close session
```

### Navigation & Interaction (33 endpoints)
```
POST   /sessions/:id/navigate       Go to URL
GET    /sessions/:id/url            Current URL + title
POST   /sessions/:id/click          Click (selector or text)
POST   /sessions/:id/fill           Fill input
POST   /sessions/:id/press          Press key
POST   /sessions/:id/scroll         Scroll
POST   /sessions/:id/hover          Hover
POST   /sessions/:id/dblclick       Double-click
POST   /sessions/:id/rightclick     Right-click
POST   /sessions/:id/drag           Drag and drop
POST   /sessions/:id/select         Select dropdown
POST   /sessions/:id/evaluate       Execute JS
POST   /sessions/:id/wait           Wait for selector
POST   /sessions/:id/network-idle   Wait for network idle
GET    /sessions/:id/screenshot     Screenshot (JPEG 85%)
GET    /sessions/:id/content        HTML content
GET    /sessions/:id/html           Raw HTML
GET    /sessions/:id/head           <head> HTML
GET    /sessions/:id/text           Text content
GET    /sessions/:id/cookies        Get cookies
POST   /sessions/:id/clear-cookies  Clear cookies
POST   /sessions/:id/dialog         Handle alert/confirm/prompt
POST   /sessions/:id/screenshot-element  Element screenshot
POST   /sessions/:id/pdf            Generate PDF
POST   /sessions/:id/resize         Resize viewport
POST   /sessions/:id/set-files      Set file input
POST   /sessions/:id/upload         Upload files (base64)
POST   /sessions/:id/download       Download file
POST   /sessions/:id/mouse/click    Raw mouse click (x,y)
POST   /sessions/:id/mouse/move     Raw mouse move
POST   /sessions/:id/keyboard/type  Raw keyboard type
```

### Tabs
```
GET    /sessions/:id/tabs           List tabs
POST   /sessions/:id/new-tab        New tab
POST   /sessions/:id/switch-tab     Switch tab
POST   /sessions/:id/close-tab      Close tab
```

### WebSocket Streams
```
GET    /sessions/:id/screencast     Live JPEG frames (canvas rendering)
GET    /sessions/:id/events         CDP event stream (network, DOM, console)
```

### Session Lifecycle

| Setting | Default | Electron-managed |
|---------|---------|-----------------|
| Idle timeout | 5 min | 1 hour |
| Max age | 30 min | 24 hours |
| Profile | Ephemeral | Persistent |

---

## 8. Electron Desktop App

Port 9300 (control server). Single-instance lock.

### Control Server Endpoints
```
GET    /ping                        Health check
POST   /focus                       Bring window to foreground
GET    /active-tab-info             { type, sessionId, id, url, title }
GET    /url                         Current URL
POST   /navigate                    Navigate active tab
GET    /screenshot                  PNG screenshot
GET    /dom                         DOM snapshot
POST   /eval                        Execute JS
POST   /new-stealth-tab             Create stealth tab
GET    /tabs                        List all tabs
POST   /new-tab                     Create yamil tab
POST   /close-tab                   Close tab
POST   /switch-tab                  Switch tab
POST   /clear-cache                 Clear cache + storage
```

### Renderer Features (2,600+ lines)

- **Tab management** — create/switch/close, webview wiring (yamil), canvas+WS (stealth)
- **Address bar** — URL autocomplete, SSL lock icon, back/forward/refresh
- **Bookmarks** — Add/remove/search, category + tags, bookmark bar
- **History** — Per-tab, searchable, max 5,000 entries
- **AI Sidebar** — Chat with streaming, task queue, skills, `@tab:0` / `@all-tabs` references
- **Settings** — Homepage, search engine, sidebar position, AI endpoint, shortcuts
- **Find in Page** — Ctrl+F with prev/next
- **Zoom** — Ctrl+/- per-tab (10% increments)

### Data Persistence (localStorage)

```
yamil_tabs              Tab state (IDs, URLs, types)
yamil_bookmarks         Bookmarks array
yamil_history           Navigation history (max 5,000)
yamil_chat_history      AI chat messages (max 200)
yamil_ai_memory         AI context/memory
yamil_ai_skills         Custom skills
yamil_ai_blocked_domains  Domains hidden from AI
yamil_sidebar_open      Sidebar state
yamil_bookmarkbar_visible  Bookmark bar visibility
```

---

## 9. Gemini Computer Use Integration

Model: `gemini-2.5-computer-use-preview-10-2025`

Translates Gemini Computer Use actions to Playwright:

| Gemini CU Action | Browser Action |
|-------------------|---------------|
| `click_at(x, y)` | Click at normalized 0-999 coordinates → pixel coords |
| `type_text_at(x, y, text)` | Type with optional clear/enter |
| `scroll_document(direction)` | up/down/left/right |
| `scroll_at(x, y, direction)` | Scroll at specific point |
| `key_combination(keys)` | e.g. "Control+A", "Control+C" |
| `hover_at(x, y)` | Hover |
| `navigate(url)` | Go to URL |
| `drag_and_drop(x, y, dx, dy)` | Drag from source to dest |
| `wait_5_seconds()` | Pause |

---

## 10. Dependencies

### Browser Service
```
fastify            ^5.0.0      HTTP server
@fastify/websocket ^11.0.0     WebSocket (screencast, events)
playwright         1.58.0      Headless Chromium automation
```

### Electron App
```
electron           ^33.0.0     Desktop app shell
electron-builder   ^25.0.0     Packaging (NSIS/DMG/AppImage)
```

### MCP Server (index.mjs)
```
@modelcontextprotocol/sdk      MCP protocol
@anthropic-ai/sdk              Anthropic API (fallback)
@anthropic-ai/bedrock-sdk      AWS Bedrock (fallback)
@google/generative-ai          Gemini (fallback)
playwright                     Browser automation
zod                            Schema validation
```

---

## 11. Industry Comparison

### Feature Matrix

| Feature | YAMIL Browser | Stagehand / Browserbase | Playwright MCP | Browser Use | Steel | Browserless |
|---------|:---:|:---:|:---:|:---:|:---:|:---:|
| **Open source** | Yes | SDK yes / Infra no | Yes (MIT) | Yes (MIT) | Yes (Apache 2.0) | Dual (SSPL) |
| **Self-hosted** | Yes | No | Yes | Yes (local) | Yes (Docker) | Yes (commercial) |
| **Free** | Yes | 1 hr/mo free | Yes | Framework free | Yes | 1K units free |
| **Vision model** | Ollama qwen3-vl (local, free) | Gemini Flash (cloud) | None (a11y tree) | ChatBrowserUse (cloud) | None | None |
| **Anti-detection** | Built-in (stealth.js) | Paid plans only | None | Cloud only | Built-in | BrowserQL only |
| **MCP support** | 88 tools | Yes | Yes (IS the MCP) | Yes | Yes | Yes |
| **Dual tab model** | Yamil + Stealth | No | No | No | No | No |
| **Desktop app** | Electron | No (cloud) | No | No | No | No |
| **Tab management** | Full tab bar UI | Cloud sessions | Multi-browser | Multi-tab (buggy) | CDP control | WebSocket |
| **Session persistence** | LocalStorage + profiles | 7-90 days (cloud) | Per-workspace | Cloud profiles | 24 hours | Yes |
| **AI sidebar** | Built-in chat + skills | No | No | No | No | No |
| **Bookmarks/history** | Yes | No | No | No | No | No |
| **Monaco editor** | Auto-detected | No | No | No | No | No |
| **Action caching** | Yes (30 min TTL) | Yes (auto-caching) | No | No | No | No |
| **Computer Use** | Gemini CU integration | No | No | No | No | No |
| **Multi-LLM fallback** | 4-provider chain | Single provider | N/A | Single provider | N/A | N/A |

### What YAMIL Browser Has That Nobody Else Does

1. **Dual tab model** — No other browser combines logged-in webview tabs with stealth canvas tabs in the same window
2. **Zero-cost vision** — Ollama qwen3-vl runs locally, no API keys, no cloud dependency
3. **88 MCP tools** — Largest tool surface (Stagehand: ~10, Playwright MCP: ~20, Browser Use: ~15)
4. **AI sidebar** — Built-in chat, task queue, skills, multi-tab context (`@tab:0`)
5. **Monaco editor awareness** — Auto-detects and handles Monaco editors (critical for code-heavy platforms)
6. **4-provider LLM chain** — Graceful degradation from local Ollama → Gemini → Bedrock → Anthropic
7. **Smart routing** — AI doesn't need to know tab type; same tools work transparently
8. **Full desktop app** — Not just a headless API or cloud service; actual browser with tab bar, bookmarks, history

### What Competitors Have That We Could Consider

| Feature | Who Has It | Priority |
|---------|-----------|----------|
| Auto CAPTCHA solving | Browserbase, Browserless, Browser Use | Low (stealth avoids CAPTCHAs) |
| Residential proxies | Browserbase ($39+/mo), Browserless | Low (not needed for internal YAMIL work) |
| Session replay/recording | Browserbase, Browserless | Medium (useful for debugging) |
| SOC 2 / HIPAA compliance cert | Browserbase Scale | Future (when deploying to healthcare customers) |
| TLS/JA3 fingerprint masking | Advanced stealth tools | Low (stealth.js covers main vectors) |

### Pricing Comparison

| Tool | What You Pay | What YAMIL Browser Costs |
|------|-------------|------------------------|
| Browserbase Startup | $99/mo (500 hrs, 50 concurrent) | $0 (self-hosted) |
| Browser Use Cloud | $10 free + pay-per-use | $0 (local Ollama) |
| AgentQL Pro | $99/mo (15K API calls) | $0 |
| Browserless Starter | $50/mo | $0 |
| YAMIL Browser | **$0** | Electricity + GPU for Ollama |

---

## 12. Qwen3-VL vs Other Vision Models

| Benchmark | Qwen3-VL 8B | GPT-4o | Gemini 2.5 Pro |
|-----------|:-----------:|:------:|:--------------:|
| ScreenSpot Pro (GUI nav) | 61.8% | — | — |
| AndroidWorld (mobile GUI) | 63.7% (32B) | — | — |
| Design2Code | 92.0 | — | — |
| Cost per 100K queries | ~$0 (local) | ~$1,464 | Free tier limited |
| License | Apache 2.0 | Proprietary | Proprietary |
| Self-hostable | Yes | No | No |
| Runs on consumer GPU | Yes (8B) | No | No |

Qwen3-VL is the best open-source vision model for browser automation in 2026. The 8B parameter version runs on a single GPU, provides competitive accuracy for GUI tasks, and costs nothing to operate.

---

## 13. File Structure

```
C:\project\yamil-browser\
├── browser-service\
│   ├── src\
│   │   ├── index.js           (21 lines — Fastify server entry)
│   │   ├── routes.js          (441 lines — REST API, 35+ endpoints)
│   │   ├── sessions.js        (221 lines — session lifecycle, CDP, profiles)
│   │   ├── stealth.js         (92 lines — anti-detection injection)
│   │   └── ws.js              (76 lines — WebSocket screencast + events)
│   └── package.json           (fastify 5, playwright 1.58)
│
├── electron-app\
│   ├── main.js                (~400 lines — control server, window mgmt)
│   ├── preload.js             (IPC bridge, context isolation)
│   ├── renderer\
│   │   ├── renderer.js        (2,600+ lines — tabs, AI sidebar, bookmarks, history)
│   │   └── index.html         (UI structure, CSS)
│   ├── package.json           (electron 33, electron-builder 25)
│   └── assets\                (logo, icons)
│
├── docker-compose.yml         (browser-service container config)
├── yamil_browser_client.py    (Python client for programmatic control)
└── README.md
                                                        Total: ~7,000+ lines

C:\project\Ai-Tools\src\
└── index.mjs                  (3,699 lines — 88 MCP tools, LLM chain)
                                                        Grand total: ~10,700 lines
```

---

## 14. Environment Variables

### MCP Server
```env
OLLAMA_URL=http://127.0.0.1:11434         # Ollama endpoint (default)
OLLAMA_VISION_MODEL=qwen3-vl:8b           # Vision model (default)
GEMINI_API_KEY=                            # Google Gemini (fallback)
ANTHROPIC_API_KEY=                         # Direct Anthropic (last resort)
AWS_BEDROCK_ACCESS_KEY_ID=                 # Bedrock (fallback)
AWS_BEDROCK_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1
```

### Browser Service
```env
PORT=4000                    # HTTP port
HOST=0.0.0.0                 # Bind address
SESSION_IDLE_MS=300000       # 5 min idle timeout
SESSION_MAX_AGE_MS=1800000   # 30 min max session
PROFILES_DIR=data/profiles   # Persistent profile storage
```

### Electron App
```env
APP_TITLE=YAMIL Browser
START_URL=https://yamil-ai.com
CTRL_PORT=9300               # Control server port
BROWSER_SERVICE=http://127.0.0.1:4000
AI_ENDPOINT=http://localhost:8015/api/v1/builder-orchestra/browser-chat
```

---

## 15. Summary

YAMIL Browser is a **full desktop browser** with AI vision, anti-detection, and 88 MCP tools — built with zero recurring cost using local Ollama. It uniquely combines logged-in YAMIL tabs with stealth Playwright tabs, has the largest MCP tool surface in the industry, and is the only AI browser with a built-in AI sidebar, bookmarks, history, and Monaco editor awareness.

No other tool in the market spans all three tiers: SDK (MCP tools), infrastructure (Electron + Playwright), and product (desktop browser with full UX).

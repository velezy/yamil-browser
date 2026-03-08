# 133 — YAMIL Browser: MCP Server Microservices Refactor

**Status**: In Progress
**Created**: 2026-03-07
**Vision**: AI-First UX — The browser is the AI's eyes and hands, humans come second

---

## 1. Philosophy: AI-First, User-Second

YAMIL Browser is not a browser with AI bolted on. It is an **AI browser** where:

- The **AI is the primary user** — it sees pages (screenshots, a11y trees), navigates, clicks, fills, extracts, and learns
- The **human is the secondary user** — they watch, guide, and override when needed
- Every action the AI takes feeds back into a **RAG knowledge pipeline** so the browser gets smarter over time
- The browser provides **eyes for the LLM** — whether it's Claude Code, the AI Builder assemblyline, or any other agent

This is the difference between YAMIL Browser and every other browser automation tool: we don't automate browsers for humans, we give browsers to AIs and let humans supervise.

### What "AI-First UX" Means in Practice

1. **Every MCP tool returns structured data optimized for LLM consumption** — not HTML, not pixels, but semantic trees and action results
2. **The RAG pipeline learns navigation patterns** — "how do I get to the HBS 3 backup wizard on QNAP?" becomes a learned recipe
3. **Error recovery is learned** — when a click fails, the browser remembers what worked and suggests it next time
4. **The AI Builder (assemblyline) can call YAMIL Browser as its eyes** — browse docs, test APIs, verify deployments, fill forms
5. **Page schemas are auto-discovered** — the browser learns what inputs exist on every page it visits

---

## 2. The Problem: 3,045-Line Monolith

The MCP server (`mcp-server/src/index.mjs`) is a single 3,045-line file containing:
- 88 MCP tool definitions
- 4-provider LLM fallback chain (Ollama, Gemini, Bedrock, Anthropic)
- Gemini Computer Use integration
- Action cache (hostname|instruction -> action)
- RAG knowledge lookup client
- Vision pipeline
- HTTP client to browser-service
- Error logging and formatting

This violates every microservice principle. A single bug in the Gemini provider crashes all 88 tools. Testing one tool means loading all 3,045 lines. Adding a new tool means editing a massive file.

### Current Architecture (What's Already Good)

| Service | Port | Lines | Status |
|---------|------|-------|--------|
| browser-service | 4000 | 1,803 (7 files) | Already a proper microservice |
| browser-db (pgvector) | 5433 | Docker container | Already separate |
| electron-app | 9300 | 1,023 + renderer | Desktop app, separate process |
| **mcp-server** | stdio | **3,045 (1 file)** | **Monolith — needs refactor** |

---

## 3. Target Architecture

```
mcp-server/src/
├── index.mjs                    Entry point — MCP server setup, tool registration
│
├── tools/                       One file per tool category
│   ├── navigation.mjs           navigate, go_back, go_forward, network_idle
│   ├── interaction.mjs          click, fill, type, press, hover, scroll, select, drag,
│   │                            double_click, right_click, dialog, focus
│   ├── observation.mjs          screenshot, a11y_snapshot, dom, content, html, head,
│   │                            screenshot_element, console_logs
│   ├── tabs.mjs                 new_tab, switch_tab, close_tab, list_tabs, tab_context
│   ├── data.mjs                 cookies, clear_cookies, download, pdf, set_files, upload,
│   │                            eval, get_html
│   ├── ai-vision.mjs            act, observe, extract, run_task, batch
│   ├── knowledge.mjs            knowledge_search, knowledge_stats
│   └── browser-mgmt.mjs         start, stop, status, resize, zoom, bookmarks, bookmark,
│                                bookmark_search, history, ai_privacy, skills
│
├── providers/                   LLM provider implementations
│   ├── ollama.mjs               qwen3-vl vision + qwen3 extraction + nomic embeddings
│   ├── gemini.mjs               Gemini Flash + Computer Use (gemini-2.5-computer-use)
│   ├── bedrock.mjs              AWS Bedrock Claude (cross-region)
│   └── anthropic.mjs            Direct Anthropic API
│
├── services/                    Shared services
│   ├── browser-client.mjs       HTTP client to browser-service (yamilGet, yamilPost, yamilPing)
│   ├── electron-client.mjs      HTTP client to electron-app (port 9300)
│   ├── action-cache.mjs         hostname|instruction -> cached action (30 min TTL, 500 entries)
│   ├── rag.mjs                  ragLookup — knowledge search via browser-service API
│   └── llm-chain.mjs            Provider fallback chain: Ollama -> Gemini -> Bedrock -> Anthropic
│
└── utils/
    ├── json-parser.mjs          extractJSON (strips think tags, markdown fences)
    ├── dom-helpers.mjs          Text extraction, selector building, Monaco detection
    └── errors.mjs               logToolError, error formatting, tool result builders
```

### Module Dependency Graph

```
index.mjs
  ├── tools/*.mjs          (each tool file registers its tools on the MCP server)
  │     ├── services/browser-client.mjs    (talk to browser-service)
  │     ├── services/electron-client.mjs   (talk to electron-app)
  │     ├── services/rag.mjs              (knowledge lookup)
  │     └── services/action-cache.mjs     (cached actions)
  │
  ├── tools/ai-vision.mjs
  │     └── services/llm-chain.mjs        (provider fallback)
  │           ├── providers/ollama.mjs
  │           ├── providers/gemini.mjs
  │           ├── providers/bedrock.mjs
  │           └── providers/anthropic.mjs
  │
  └── utils/*.mjs          (shared utilities, no external deps)
```

---

## 4. How the AI Builder (Assemblyline) Uses YAMIL Browser

The AI Builder assemblyline (`C:\project\assemblyline`) orchestrates multi-agent workflows. YAMIL Browser serves as its **web interaction layer**:

```
AI Builder Assemblyline
  │
  ├── Agent: "Research"     → yamil_browser_navigate, yamil_browser_extract
  ├── Agent: "Test"         → yamil_browser_navigate, yamil_browser_act
  ├── Agent: "Deploy"       → yamil_browser_fill, yamil_browser_click
  └── Agent: "Monitor"      → yamil_browser_screenshot, yamil_browser_dom
        │
        ▼
  YAMIL Browser MCP Server (88 tools)
        │
        ├── browser-service (Playwright, sessions, stealth)
        ├── electron-app (desktop UI, tab management)
        └── knowledge-db (pgvector — learns from every action)
```

The browser **learns from every agent's actions** and feeds that knowledge back. When Agent "Deploy" fills a form on QNAP, the browser remembers the field selectors, the navigation path, and what errors occurred. Next time any agent needs to do the same thing, RAG provides the recipe.

---

## 5. Learning Pipeline Improvements (Done in This Session)

### 5.1 Screenshot Fix (Root Cause of Crashes)

| Bug | Fix |
|-----|-----|
| `scale` passed as number — Playwright ignores it, images stay 1920px | Changed to `scale: 'css'` |
| No width cap on screenshots | Capped clip to 1280x900 |
| Element screenshots have zero size protection | Added adaptive quality + `scale: 'css'` |
| MIME type says PNG but data is JPEG | Fixed to `image/jpeg` |

### 5.2 New Action Logging (Browser Learns More)

Added `withLog` to previously unlogged actions:
- `scroll` — learns page layout patterns
- `rightclick` — learns context menu workflows
- `forward` — learns navigation sequences
- `drag` — learns drag-and-drop patterns
- `set-files` — learns file upload workflows
- `a11y-click` — learns accessibility tree interactions
- `a11y-fill` — learns form filling via a11y refs

### 5.3 Richer Knowledge Distillation

- Navigate actions now log page title (not just URL)
- `logAction` accepts result parameter (success/failure tracking)
- Distillation includes page titles for richer context
- New `navigation_maps` category — learns how to get from page A to page B

---

## 6. Refactoring Rules

1. **Zero behavior change** — every tool must work exactly as before
2. **Same tool names** — MCP clients see no difference
3. **Each tool file exports a register function** — `export function registerNavigationTools(server, deps)`
4. **Shared deps passed via `deps` object** — `{ yamilGet, yamilPost, yamilPing, ragLookup, cache, ... }`
5. **No circular imports** — utils depend on nothing, services depend on utils, tools depend on services
6. **Each file under 400 lines** — if it's bigger, split it further

---

## 7. Implementation Order

- [x] Phase 0: Fix screenshot crashes + learning gaps
- [x] Phase 1: Extract `utils/` (json-parser, dom-helpers, errors) — 3 files, 227 lines
- [x] Phase 2: Extract `services/` (browser-client, action-cache, llm-chain) — 3 files, 307 lines
- [x] Phase 3: Extract `providers/` (gemini-cu) — 1 file, 182 lines
- [x] Phase 4: Extract `tools/` (8 tool files) — 8 files, 2,395 lines
- [x] Phase 5: Slim down `index.mjs` to pure registration — 114 lines (was 3,045)
- [ ] Phase 6: Runtime test all tools, remove `index.old.mjs` backup

---

## 8. File Line Count Targets

| File | Target Lines | Contents |
|------|-------------|----------|
| index.mjs | ~100 | Server setup, imports, register all tool groups |
| tools/navigation.mjs | ~150 | 4 tools |
| tools/interaction.mjs | ~400 | 12 tools (largest group) |
| tools/observation.mjs | ~350 | 8 tools |
| tools/tabs.mjs | ~200 | 5 tools |
| tools/data.mjs | ~250 | 8 tools |
| tools/ai-vision.mjs | ~400 | 5 tools (complex LLM logic) |
| tools/knowledge.mjs | ~100 | 2 tools |
| tools/browser-mgmt.mjs | ~350 | 12 tools |
| providers/ollama.mjs | ~100 | Vision + extraction calls |
| providers/gemini.mjs | ~150 | Flash + Computer Use |
| providers/bedrock.mjs | ~80 | Bedrock Claude |
| providers/anthropic.mjs | ~80 | Direct Anthropic |
| services/browser-client.mjs | ~100 | yamilGet, yamilPost, yamilPing, yamilPageUrl |
| services/electron-client.mjs | ~50 | Electron control server client |
| services/action-cache.mjs | ~80 | LRU cache with TTL |
| services/rag.mjs | ~60 | ragLookup wrapper |
| services/llm-chain.mjs | ~150 | Provider fallback orchestration |
| utils/json-parser.mjs | ~50 | extractJSON, think tag stripping |
| utils/dom-helpers.mjs | ~100 | Text helpers, Monaco detection |
| utils/errors.mjs | ~60 | logToolError, result builders |
| **Total** | **~3,360** | Same code, 21 focused files |

# 138 - YAMIL Browser Feature Roadmap & RAG API Expansion

**Created:** 2026-03-12
**Status:** In Progress
**Goal:** Close the gap between YAMIL Browser and production browsers (Chrome/Edge) while expanding the agentic RAG system for external integrations.

---

## Part A: RAG API Expansion (Webhooks & External Access)

### Current State
- 200 knowledge entries, 511 actions logged
- PostgreSQL + pgvector (nomic-embed-text 768-dim embeddings)
- REST pull-only: `/knowledge/search`, `/knowledge/stats`, `/knowledge/contribute`
- No push notifications, no webhooks, no event streaming

### Planned Enhancements

#### A1. Webhooks — Push knowledge events to external systems
- `POST /knowledge/webhooks` — Register a webhook (URL, events, domain filter)
- `DELETE /knowledge/webhooks/:id` — Remove a webhook
- `GET /knowledge/webhooks` — List registered webhooks
- Events: `knowledge.created`, `knowledge.updated`, `action.logged`, `session.flushed`
- Payload: `{ event, timestamp, entry, domain, category }`
- Retry logic: 3 attempts with exponential backoff

#### A2. SSE Event Stream — Real-time knowledge feed
- `GET /knowledge/stream` — Server-Sent Events stream
- Filter by domain/category via query params
- Heartbeat every 30s to keep connection alive

#### A3. Bulk Export & Import
- `GET /knowledge/export?domain=&category=&format=json` — Export knowledge
- `POST /knowledge/import` — Bulk import from another YAMIL instance

#### A4. API Authentication
- API key system for non-localhost access
- `X-YAMIL-API-Key` header
- Key management via settings UI

---

## Part B: Browser Features (Chromium Gaps)

### Priority 1 — High Impact, Moderate Effort

#### B1. Context Menus (Right-Click)
- Right-click on page: Back, Forward, Reload, View Source, Inspect, Copy, Select All
- Right-click on link: Open in New Tab, Open in Stealth Tab, Copy Link
- Right-click on image: Save Image, Copy Image, Open in New Tab
- Right-click on text selection: Copy, Search with [engine], Ask AI

#### B2. Password Autofill
- On page load, check credential store for matching domain
- Show autofill dropdown on username/password focus
- One-click fill from saved credentials
- "Save password?" prompt bar after login detection (upgrade current silent save)

#### B3. Address Bar Autocomplete & Search Suggestions
- History-based suggestions as user types
- Bookmark matches
- Search engine suggestions (Google Suggest API)
- Keyboard navigation (arrow keys, Enter to select)

#### B4. Tab Enhancements
- Tab pinning (smaller fixed-width tabs, persist across restarts)
- Tab groups (color-coded, collapsible)
- Drag tabs to reorder
- Tab preview on hover (thumbnail tooltip)
- Restore closed tab (Ctrl+Shift+T)

### Priority 2 — Medium Impact

#### B5. Cookie Management UI
- Settings panel: list cookies by domain
- Delete individual cookies or all for a domain
- Block third-party cookies toggle

#### B6. Ad/Tracker Blocking
- EasyList filter integration
- Block count badge on address bar
- Per-site whitelist
- Cosmetic filtering (hide ad elements)

#### B7. Reader Mode
- Detect article pages (readability score)
- Strip ads/nav, show clean text
- Font size, theme (light/dark/sepia) controls
- Reading time estimate

#### B8. Picture-in-Picture
- Detect video elements on page
- PiP button overlay on videos
- Floating mini-player window

### Priority 3 — Nice to Have

#### B9. Service Workers & PWA
- Enable service worker registration in webview
- PWA install button when manifest detected
- Offline page support

#### B10. Translation
- Language detection on page load
- Translation bar: "Translate this page?"
- Use LLM for translation (already have Ollama)

#### B11. Multi-Profile
- Profile switcher in title bar
- Separate history, bookmarks, credentials per profile
- Partition webview sessions by profile

#### B12. Download Manager Enhancements
- Pause/resume downloads
- Download progress in status bar
- Auto-open option per file type

---

## Implementation Order

| Phase | Items | Focus |
|-------|-------|-------|
| **Phase 1** | B1 (Context Menus), B2 (Autofill), A1 (Webhooks) | Core UX + API |
| **Phase 2** | B3 (Autocomplete), B4 (Tab Enhancements), A2 (SSE Stream) | Navigation + Real-time |
| **Phase 3** | B6 (Ad Blocking), B5 (Cookies), A3 (Export/Import) | Privacy + Data |
| **Phase 4** | B7 (Reader), B8 (PiP), B10 (Translation) | Content |
| **Phase 5** | B9 (PWA), B11 (Multi-Profile), A4 (API Auth) | Advanced |

---

## Technical Notes

- **Electron 33.4.11 / Chromium 131** — current stack
- **Webview tag** — used for all yamil tabs (not BrowserView)
- **Browser-service** (port 4000) — Fastify + PostgreSQL + Ollama
- **MCP server** — 16 modules, Claude integration
- **Knowledge DB** — pgvector cosine similarity, qwen3:8b extraction

# 142 — AI Settings Panel + Fix Agentic RAG

## Overview

Two things need to happen:

1. **Fix the Agentic RAG** — the rag-service is running but degraded. Vector store never initialized because `DocumentRepository` can't be imported from `assemblyline_common.database`. The DB connection works fine (pgvector enabled, pool created). Fix the import gate so the RAG is fully operational.

2. **AI Settings Panel** — add LLM provider/model selection, voice settings, and API key management to the existing Electron settings panel (Ctrl+,). Currently the only way to change LLM or voice is editing `.env` files.

3. **Wire RAG to AI Sidebar + MCP** — the whole point of having a RAG in the browser is so:
   - The AI sidebar can search RAG knowledge when answering questions
   - MCP tools can read from RAG (so Claude Code can pull knowledge when programming)
   - Other apps (MemoBytes, Drive-Sentinel, YAMIL Application) can query the RAG API
   - When building the AI Orchestra layer, all accumulated knowledge is already there

---

## Architecture

```
YAMIL Browser (Electron)
├── Settings Panel (Ctrl+,)
│   ├── AI Provider: [Ollama ▼] Model: [qwen3:8b ▼]
│   ├── API Keys: OpenAI, Anthropic, Gemini, Grok
│   ├── Voice: [af_heart ▼] Speed: [1.0]
│   └── RAG Status: Connected (142 documents)
│
├── AI Sidebar Chat
│   ├── Sends { provider, model } per user selection
│   ├── Searches RAG for context before answering
│   └── Ingests page content to RAG on demand
│
└── MCP Tools (yamil_browser_*)
    ├── yamil_browser_rag_search  → POST :8022/search
    ├── yamil_browser_rag_ingest  → POST :8022/ingest/text
    └── yamil_browser_rag_status  → GET  :8022/health

External Apps (MemoBytes, Drive-Sentinel, YAMIL App)
└── HTTP API → :8022/search, /agents/query, /ingest/text
```

---

## Phase 1: Fix Agentic RAG (Critical)

### Problem
`rag-service/src/app/main.py` imports `DocumentRepository` and `DocumentStatus` from `assemblyline_common.database`. These classes don't exist in the bundled version. This sets `DB_AVAILABLE = False`, which prevents:
- Vector store initialization
- Agent pipeline initialization
- All search/ingest endpoints

### Fix
Decouple vector store init from the `DocumentRepository` import. The vector store connects to postgres on its own via `get_vector_store()` — it doesn't need `DocumentRepository`.

**File:** `rag-service/src/app/main.py`
- Remove `DocumentRepository`/`DocumentStatus` from the import that gates `DB_AVAILABLE`
- Or create stub classes if the code references them downstream
- Ensure `get_vector_store()` initializes on startup

### Verify
```bash
curl http://localhost:8022/health
# Should show: "database": true, "vector_store_initialized": true

curl -X POST http://localhost:8022/search \
  -H "Content-Type: application/json" \
  -d '{"query": "test", "top_k": 3}'
# Should return results (empty array is fine, no error)

curl -X POST http://localhost:8022/ingest/text \
  -H "Content-Type: application/json" \
  -d '{"text": "YAMIL Browser is an Electron-based desktop browser", "metadata": {"source": "test"}}'
# Should return success with document ID
```

---

## Phase 2: AI Settings Panel

### Extend existing settings (Ctrl+,)

The settings panel already exists with an "AI" section containing `#set-ai-endpoint` and `#set-clear-memory`. Add:

**File:** `electron-app/renderer/index.html` (inside `#settings-body`, AI section)

```
AI Section (extended):
├── AI Endpoint: [http://localhost:8020/browser-chat] (existing)
├── LLM Provider: [Auto ▼] (new select: auto, ollama, openai, anthropic, gemini, grok)
├── Model: [qwen3:8b ▼] (new select: populated dynamically from provider)
├── Cloud API Keys: (new expandable section)
│   ├── OpenAI: [sk-... ] (password input)
│   ├── Anthropic: [sk-ant-... ]
│   ├── Gemini: [AIza... ]
│   └── Grok/xAI: [xai-... ]
├── Voice: (new section)
│   ├── TTS Voice: [af_heart ▼] (populated from /voice/voices)
│   ├── Speed: [1.0] (range slider 0.5-2.0)
│   └── Test Voice: [Play ▶] button
├── RAG Status: (new display)
│   ├── Status dot + "Connected" / "Disconnected"
│   ├── Document count
│   └── Index current page button
└── Clear AI Memory (existing)
```

**File:** `electron-app/renderer/renderer.js`
- `loadAISettings()` — fetch `/llm/status`, `/voice/voices`, RAG `/health` on panel open
- `saveAISettings()` — persist provider/model/voice to localStorage
- `sendChat()` — include selected provider/model from settings
- API keys saved to localStorage (encrypted? or just local-only)
- API keys sent to chat-service via new endpoint `POST /config/api-keys`

**File:** `electron-app/renderer/styles.css`
- Styles for API key inputs (password fields)
- Voice test button
- RAG status indicator

---

## Phase 3: Wire RAG to AI Sidebar

### Chat-service reads from RAG before answering

**File:** `chat-service/src/app/browser_chat.py`

Before calling LLMRouter, search the RAG for relevant context:

```python
# In browser_chat():
rag_context = ""
if RAG_URL:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{RAG_URL}/search", json={
                "query": request.message,
                "top_k": 3
            }, timeout=5.0)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    rag_context = "\n\nRelevant knowledge from RAG:\n"
                    for r in results:
                        rag_context += f"- {r.get('content', '')[:500]}\n"
    except Exception:
        pass  # RAG is optional

system += rag_context
```

### Sidebar can ingest current page to RAG

Add "Save to Knowledge" button in sidebar that POSTs page content to `/ingest/text`:

```javascript
async function savePageToRAG() {
    const pageCtx = await getPageContext()
    const resp = await fetch(RAG_ENDPOINT + '/ingest/text', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            text: pageCtx.text,
            metadata: { url: pageCtx.url, title: pageCtx.title, source: 'yamil-browser' }
        })
    })
}
```

---

## Phase 4: MCP Tools for RAG

### Add RAG tools to MCP server

**File:** `mcp-server/src/index.mjs`

Add three new MCP tools so Claude Code can interact with the RAG:

1. `yamil_browser_rag_search` — search the knowledge base
2. `yamil_browser_rag_ingest` — add text/documents to the knowledge base
3. `yamil_browser_rag_status` — check RAG health and stats

These let Claude Code (and the AI Orchestra layer) read from and write to the RAG programmatically.

---

## Phase 5: Chat-Service Config Endpoint

### Dynamic config without container restart

**File:** `chat-service/src/app/browser_chat.py`

Add endpoint to update LLM config at runtime:

```
POST /config/llm
{ "provider": "openai", "model": "gpt-4o", "api_key": "sk-..." }

POST /config/voice
{ "voice": "am_adam", "speed": 1.2 }

GET /config
→ current provider, model, voice, available models per provider
```

This way the settings panel can change the LLM without restarting Docker.

---

## Files Summary

| Action | File | Change |
|--------|------|--------|
| FIX | `rag-service/src/app/main.py` | Decouple vector store from DocumentRepository import |
| EDIT | `chat-service/src/app/browser_chat.py` | Add RAG search before LLM call, add /config endpoints |
| EDIT | `electron-app/renderer/index.html` | Extend AI settings section |
| EDIT | `electron-app/renderer/renderer.js` | Settings load/save, send provider/model with chat |
| EDIT | `electron-app/renderer/styles.css` | Settings panel styles |
| EDIT | `mcp-server/src/index.mjs` | Add yamil_browser_rag_* tools |

---

## Verification

1. RAG health: `curl http://localhost:8022/health` → `vector_store_initialized: true`
2. RAG ingest: POST text → get document ID back
3. RAG search: POST query → get relevant results
4. Settings panel: Ctrl+, → see LLM/voice/RAG settings
5. Change provider in settings → next chat uses new provider
6. Change voice in settings → TTS uses new voice
7. "Save to Knowledge" button → page content ingested to RAG
8. Chat references RAG knowledge when relevant
9. MCP: `yamil_browser_rag_search` returns results from Claude Code
10. External: `curl http://localhost:8022/search` works from other apps

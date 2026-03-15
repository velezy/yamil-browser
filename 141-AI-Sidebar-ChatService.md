# 141 — Connect YAMIL Browser AI Sidebar to AssemblyLine Chat-Service

## Overview

Deploy the AssemblyLine chat-service as a standalone Docker microservice to power the YAMIL Browser AI sidebar with:

- **Multi-provider LLM routing** — Ollama (local default) + OpenAI, Claude, Gemini, Grok (cloud options)
- **Voice I/O** — STT via Faster-Whisper, TTS via Edge TTS/Kokoro
- **Conversation persistence** — PostgreSQL + pgvector
- **Agentic RAG API** — shared endpoint for MemoBytes, Drive-Sentinel, and YAMIL apps
- First standalone deployment of the AssemblyLine common library

---

## Architecture

```
YAMIL Browser (Electron)         Chat-Service (Docker :8020)         LLM Providers
┌─────────────────────┐         ┌──────────────────────────┐       ┌────────────────┐
│ renderer.js sidebar  │──POST──│ /browser-chat            │──────▶│ Ollama (local)  │
│ sendChatStreaming()  │  :8020 │   → LLMRouter.generate() │       │ OpenAI (cloud)  │
│                      │        │   → stream SSE tokens     │       │ Anthropic       │
│ Voice button (STT)   │──POST──│ /voice/transcribe (STT)  │       │ Gemini          │
│ speakText() (TTS)    │──POST──│ /voice/synthesize (TTS)  │       │ Grok (xAI)      │
└─────────────────────┘         │                          │       └────────────────┘
                                │ postgres:5433 (pgvector)  │
                                │ redis:6380               │
                                └──────────────────────────┘

External Apps (API consumers)
┌─────────────────────┐
│ MemoBytes            │──POST──┐
│ Drive-Sentinel       │──POST──├─→ /api/rag/search  (agentic RAG)
│ YAMIL Application    │──POST──┘   /api/rag/ingest
└─────────────────────┘
```

---

## LLM Provider Configuration

### Default: Ollama (Local)

Ollama runs on the host machine. The chat-service container accesses it via `host.docker.internal:11434`.

**Default models (from AssemblyLine common lib):**
| Role | Model | Notes |
|------|-------|-------|
| Default | `llama3.1:8b` | General purpose |
| Fast | `llama3.2:3b` | Quick responses |
| Quality | `gemma3:4b` | Higher quality |
| Vision | `qwen2.5-vl:3b` | Image understanding |
| Code | `qwen2.5-coder:7b` | Code generation |
| Embedding | `nomic-embed-text:latest` | RAG embeddings |

### Cloud Providers (Optional — add API keys to `.env`)

| Provider | Env Variable | Models |
|----------|-------------|--------|
| **OpenAI** | `OPENAI_API_KEY=sk-...` | gpt-4o, gpt-4o-mini |
| **Anthropic (Claude)** | `ANTHROPIC_API_KEY=sk-ant-...` | claude-3.5-sonnet, claude-3-opus, claude-3.5-haiku |
| **Google (Gemini)** | `GOOGLE_API_KEY=AIza...` | gemini-2.0-flash, gemini-1.5-pro, gemini-1.5-flash |
| **xAI (Grok)** | `XAI_API_KEY=xai-...` | grok-2, grok-2-mini (OpenAI-compatible API) |

**Smart Routing (LLMRouter):**
- Vision tasks → Gemini (best cost/quality for images)
- Code tasks → OpenAI or Ollama qwen2.5-coder
- Reasoning tasks → Anthropic Claude
- Fast/simple → Ollama (zero cost, lowest latency)
- Fallback chain: `ollama → gemini → openai → anthropic`

---

## Agentic RAG API

The chat-service exposes a RAG API that any application can use to store and retrieve knowledge:

### Endpoints

```
POST /api/rag/search    — Search the knowledge base
POST /api/rag/ingest    — Ingest documents into the knowledge base
GET  /api/rag/status    — RAG system health and stats
DELETE /api/rag/documents/{id}  — Remove a document
```

### Search Request
```json
POST /api/rag/search
{
  "query": "How do I configure the S3 connector?",
  "top_k": 5,
  "app_id": "memobytes",           // Filter by source app
  "namespace": "docs",              // Optional namespace
  "include_metadata": true
}
```

### Search Response
```json
{
  "results": [
    {
      "content": "To configure the S3 connector, go to Settings...",
      "score": 0.92,
      "metadata": {
        "source": "memobytes",
        "document_id": "doc-123",
        "title": "S3 Configuration Guide"
      }
    }
  ],
  "query_embedding_model": "nomic-embed-text:latest"
}
```

### Ingest Request
```json
POST /api/rag/ingest
{
  "documents": [
    {
      "content": "Document text content...",
      "metadata": {
        "title": "Setup Guide",
        "source": "drive-sentinel",
        "type": "documentation"
      }
    }
  ],
  "namespace": "docs",
  "chunk_size": 512,
  "chunk_overlap": 50
}
```

### Consumer Apps

| App | Use Case |
|-----|----------|
| **MemoBytes** | Store/retrieve personal notes, flashcards, study materials for AI-powered recall |
| **Drive-Sentinel** | Index file metadata, search patterns, security alerts for AI context |
| **YAMIL Application** | Flow documentation, connector configs, error history for AI assistant context |
| **YAMIL Browser** | Page bookmarks, browsing history, user preferences for personalized AI |

---

## Implementation Phases

### Phase 1: Deploy Chat-Service (Docker)

**1.1 — Copy chat-service into yamil-browser repo**

Copy from AssemblyLine, adapt for standalone:
```
yamil-browser/
  chat-service/
    docker-compose.yml       ← adapted (Ollama host access, port offsets)
    Dockerfile               ← copied from AssemblyLine
    setup.sh                 ← copied from AssemblyLine
    requirements.txt         ← copied from AssemblyLine
    .env                     ← new (Ollama default, cloud key placeholders)
    .env.example             ← new
    migrations/
      V001__chat_schema.sql  ← copied from AssemblyLine
    src/
      main.py                ← entry point
      app/
        main.py              ← adapted (CORS, mount browser_chat, skip JWT)
        browser_chat.py      ← NEW — direct LLMRouter endpoint
        rag_api.py           ← NEW — agentic RAG API for external apps
        voice/               ← copied from AssemblyLine
```

**1.2 — Create `browser_chat.py` (core endpoint)**

Direct LLMRouter calls replacing orchestrator proxy. Supports:
- Streaming (SSE) and non-streaming modes
- Page context injection for browser-aware responses
- Provider selection per request

**1.3 — Create `rag_api.py` (agentic RAG API)**

Shared RAG endpoint for MemoBytes, Drive-Sentinel, YAMIL:
- `/api/rag/search` — vector similarity search with pgvector
- `/api/rag/ingest` — document chunking and embedding
- App-scoped namespaces for data isolation

**1.4 — Adapt `main.py` for standalone use**

- Add CORS middleware (Electron renderer needs `Access-Control-Allow-Origin: *`)
- Mount browser_chat + rag_api routers
- Add `/llm/status` endpoint (provider health)
- No JWT required (local desktop app, single user)

**1.5 — Create docker-compose.yml**

- `extra_hosts: ["host.docker.internal:host-gateway"]` for Ollama
- Postgres on port **5433** (avoid local conflicts)
- Redis on port **6380** (avoid local conflicts)
- `OLLAMA_URL: http://host.docker.internal:11434`

### Phase 2: Connect Sidebar to Chat-Service

**2.1 — Update default AI_ENDPOINT**

`electron-app/preload.js` line 4:
```js
AI_ENDPOINT: process.env.AI_ENDPOINT || 'http://localhost:8020/browser-chat',
```

**2.2 — Wire streaming in sendChat()**

Replace the inline `fetch()` in `sendChat()` with a call to `sendChatStreaming()` by default, keeping non-streaming as fallback.

### Phase 3: Voice Integration

**3.1 — Voice input (STT):** Push-to-hold recording via MediaRecorder → POST to `/voice/transcribe`
**3.2 — Server TTS:** Replace `window.speechSynthesis` with POST to `/voice/synthesize`
**3.3 — Voice button:** Add microphone button to sidebar with recording state styling

### Phase 4: LLM Provider Status UI

Fetch `GET /llm/status` on sidebar open, show provider health indicator.

### Phase 5: Launch Script

`start-with-ai.sh` — starts chat-service Docker containers, waits for health, launches Electron app.

---

## Files Summary

| Action | File | Change |
|--------|------|--------|
| NEW | `chat-service/` directory | Entire standalone chat-service |
| NEW | `chat-service/src/app/browser_chat.py` | Direct LLMRouter endpoint |
| NEW | `chat-service/src/app/rag_api.py` | Agentic RAG API for external apps |
| NEW | `chat-service/.env` / `.env.example` | Local config with provider key placeholders |
| NEW | `chat-service/docker-compose.yml` | Adapted with Ollama host access |
| NEW | `start-with-ai.sh` | Unified launch script |
| EDIT | `electron-app/preload.js:4` | Default AI_ENDPOINT → `:8020/browser-chat` |
| EDIT | `electron-app/renderer/renderer.js` | Streaming default, voice recording, server TTS, LLM status |
| EDIT | `electron-app/renderer/index.html` | Voice button + status indicator |
| EDIT | `electron-app/renderer/styles.css` | Voice button + recording styles |
| EDIT | `electron-app/package.json` | Add `start:ai` script |

---

## Verification

1. `curl http://localhost:8020/health` → `{"status":"healthy"}`
2. `curl http://localhost:8020/llm/status` → shows Ollama healthy
3. `curl -X POST http://localhost:8020/browser-chat -H "Content-Type: application/json" -d '{"message":"Hello","stream":false}'` → returns response
4. Open YAMIL Browser sidebar → type "Hello" → streamed response from Ollama
5. Hold mic button → speak → transcription → AI response
6. AI response plays server TTS audio
7. Add cloud API key to `.env`, restart → provider shows in `/llm/status`
8. Navigate to page, ask "what is this page about?" → AI references page content
9. `curl -X POST http://localhost:8020/api/rag/search -d '{"query":"test","app_id":"memobytes"}'` → RAG search works

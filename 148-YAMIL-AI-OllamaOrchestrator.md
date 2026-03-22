# 148 - YAMIL AI Powered by Ollama Model Tiers via AI Orchestrator

## Goal

Power the YAMIL Browser's AI sidebar with a local Ollama-based AI orchestrator, copied and adapted from the AssemblyLine project's `ai-orchestrator-service`. This gives the browser intelligent, multi-model AI capabilities without relying on cloud APIs.

## Current State

- YAMIL Browser sidebar sends `POST AI_ENDPOINT` with `{ message, pageContext, stream }` format
- Default `AI_ENDPOINT` is `http://localhost:8020/browser-chat` (not running)
- AssemblyLine project has a production-ready AI orchestrator at `C:/project/AssemblyLine/services/ai-orchestrator-service/`
- The orchestrator already has a `/browser-chat` endpoint designed for YAMIL Browser
- Shared library `assemblyline_common` provides config, observability, database, auth

## Ollama Model Tier System

| Slot | Env Var | Model | Params | VRAM (4-bit) | Role |
|------|---------|-------|--------|-------------|------|
| Fast | `OLLAMA_FAST_MODEL` | `llama3.2:3b` | 3.2B | ~2.0 GB | Greetings, simple queries, chitchat |
| Quality | `OLLAMA_QUALITY_MODEL` | `gemma3:4b` | 4.3B | ~3.3 GB | Balanced general chat, tool LLM calls |
| Deep | `OLLAMA_DEEP_MODEL` | `qwen3:8b` | 8.2B | ~5.9 GB | Complex reasoning, logic puzzles |
| Math | `OLLAMA_MATH_MODEL` | `qwen3:8b` | 8.2B | ~5.9 GB | Math reasoning (shared with Deep) |
| Science | `OLLAMA_SCIENCE_MODEL` | `qwen3:8b` | 8.2B | ~5.9 GB | Science questions (shared with Deep) |
| Code | `OLLAMA_CODE_MODEL` | `qwen2.5-coder:7b` | 7.6B | ~5.5 GB | Code generation, Mermaid diagrams |
| Vision | `OLLAMA_VISION_MODEL` | `qwen2.5vl:3b` | 3.8B | ~2.8 GB | Image understanding, screenshots |
| Embedding | `OLLAMA_EMBEDDING_MODEL` | `nomic-embed-text:latest` | 137M | ~0.3 GB | Vector embeddings for RAG |

## Architecture

```
┌─────────────────────┐
│   YAMIL Browser     │
│   (Electron App)    │
│                     │
│  ┌───────────────┐  │     POST /browser-chat
│  │  AI Sidebar   │──┼──────────────────────────┐
│  └───────────────┘  │                          │
│                     │                          ▼
│  ┌───────────────┐  │     ┌─────────────────────────────┐
│  │ Browser Svc   │◄─┼─────│   AI Orchestrator (FastAPI) │
│  │ (port 4000)   │  │     │   Port 8024                 │
│  └───────────────┘  │     │                             │
└─────────────────────┘     │  ┌─────────┐ ┌──────────┐  │
                            │  │ Agents  │ │ RAG      │  │
                            │  │ (30+)   │ │ Search   │  │
                            │  └────┬────┘ └────┬─────┘  │
                            │       │           │        │
                            │  ┌────▼───────────▼─────┐  │
                            │  │   Model Router       │  │
                            │  │   (Swarm Router)     │  │
                            │  └────┬─────────────────┘  │
                            └───────┼─────────────────────┘
                                    │
                            ┌───────▼─────────────────────┐
                            │   Ollama (port 11434)       │
                            │   Local LLM Models          │
                            │   Fast/Quality/Deep/Code/   │
                            │   Math/Vision/Embedding     │
                            └─────────────────────────────┘
                                    │
                     ┌──────────────┼──────────────┐
                     ▼              ▼              ▼
              ┌──────────┐  ┌──────────┐  ┌──────────┐
              │PostgreSQL│  │  Redis   │  │  Ollama  │
              │(pgvector)│  │ (cache)  │  │ (models) │
              │ port 5432│  │port 6379 │  │port 11434│
              └──────────┘  └──────────┘  └──────────┘
```

## Source Files to Copy

### From AssemblyLine
| Source | Description |
|--------|------------|
| `services/ai-orchestrator-service/app/main.py` | Main FastAPI app (~9,600 lines) |
| `services/ai-orchestrator-service/app/agents/` | 30+ specialized agents |
| `services/ai-orchestrator-service/app/agents/coordinator.py` | Agent coordination |
| `services/ai-orchestrator-service/app/agents/model_factory.py` | Ollama/Bedrock model abstraction |
| `services/ai-orchestrator-service/app/agents/yamil_browser_client.py` | Browser integration client |
| `services/ai-orchestrator-service/app/utils/swarm_router.py` | Model tier routing |
| `services/ai-orchestrator-service/app/mcp/plugins/` | 14 MCP tool plugins |
| `services/ai-orchestrator-service/docker-compose.yml` | PostgreSQL + Redis + orchestrator |
| `services/ai-orchestrator-service/Dockerfile` | Python 3.13 build |
| `services/ai-orchestrator-service/requirements.txt` | Dependencies |
| `shared/python/assemblyline_common/` | Shared library (config, observability, DB) |

### Key Shared Library Modules
| Module | Purpose |
|--------|---------|
| `assemblyline_common/config.py` | Settings, DB/Redis config |
| `assemblyline_common/observability.py` | Tracing, metrics, PII masking |
| `assemblyline_common/database.py` | AuditLog, ErrorLog repositories |
| `assemblyline_common/ai/orchestrator.py` | Orchestrator patterns, guardrails |
| `assemblyline_common/ai/phi_guard.py` | PHI detection and masking |
| `assemblyline_common/utils/cloud_llm_provider.py` | Cloud LLM wrapper |
| `assemblyline_common/utils/redis_cache.py` | AI response cache |

## Implementation Plan

### Phase 1: Copy & Adapt Orchestrator

1. Create `C:/project/yamil-browser/ai-orchestrator/` directory
2. Copy `ai-orchestrator-service` from AssemblyLine
3. Copy `assemblyline_common` shared library
4. Rename references from `assemblyline` to `yamil` where needed
5. Update `docker-compose.yml`:
   - PostgreSQL container (pgvector)
   - Redis container
   - AI Orchestrator container on port 8024
   - Ollama connection to host `http://host.docker.internal:11434`

### Phase 2: Configure Model Tiers

1. Set environment variables in `docker-compose.yml`:
   ```yaml
   environment:
     OLLAMA_URL: http://host.docker.internal:11434
     OLLAMA_FAST_MODEL: llama3.2:3b
     OLLAMA_QUALITY_MODEL: gemma3:4b
     OLLAMA_DEEP_MODEL: qwen3:8b
     OLLAMA_MATH_MODEL: qwen3:8b
     OLLAMA_SCIENCE_MODEL: qwen3:8b
     OLLAMA_CODE_MODEL: qwen2.5-coder:7b
     OLLAMA_VISION_MODEL: qwen2.5vl:3b
     OLLAMA_EMBEDDING_MODEL: nomic-embed-text:latest
   ```
2. Ensure all models are pulled in Ollama locally
3. Test model routing via `/llm/status` endpoint

### Phase 3: Wire YAMIL Browser to Orchestrator

1. Update Electron app default `AI_ENDPOINT`:
   ```
   AI_ENDPOINT=http://localhost:8024/browser-chat
   ```
2. Update `preload.js` default endpoint
3. Set `YAMIL_BROWSER_URL=http://host.docker.internal:4000` in orchestrator config so it can talk back to the browser-service
4. Test streaming SSE responses in the AI sidebar

### Phase 4: Docker Compose for Full Stack

Create `docker-compose.yml` at project root that brings up:
```yaml
services:
  postgres:
    image: pgvector/pgvector:pg17
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: yamil
      POSTGRES_USER: yamil
      POSTGRES_PASSWORD: yamil
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --requirepass yamil

  ai-orchestrator:
    build: ./ai-orchestrator
    ports: ["8024:8024"]
    depends_on: [postgres, redis]
    environment:
      PORT: 8024
      DATABASE_URL: postgresql+asyncpg://yamil:yamil@postgres:5432/yamil
      REDIS_URL: redis://yamil@redis:6379/0
      OLLAMA_URL: http://host.docker.internal:11434
      YAMIL_BROWSER_URL: http://host.docker.internal:4000
      # Model tiers
      OLLAMA_FAST_MODEL: llama3.2:3b
      OLLAMA_QUALITY_MODEL: gemma3:4b
      OLLAMA_DEEP_MODEL: qwen3:8b
      OLLAMA_MATH_MODEL: qwen3:8b
      OLLAMA_CODE_MODEL: qwen2.5-coder:7b
      OLLAMA_VISION_MODEL: qwen2.5vl:3b
      OLLAMA_EMBEDDING_MODEL: nomic-embed-text:latest

volumes:
  pgdata:
```

### Phase 5: LLM Provider Settings UI

1. Add settings panel in YAMIL Browser sidebar for:
   - AI endpoint URL (already exists as `set-ai-endpoint` input)
   - Model tier selection (fast/quality/deep/code)
   - Provider toggle (Ollama local vs Cloud)
2. Save settings to `persist:yamil` session storage
3. Show LLM status indicator (green/red dot already exists in sidebar)

### Phase 6: Test End-to-End

1. Start Docker stack: `docker compose up -d`
2. Launch YAMIL Browser with: `AI_ENDPOINT=http://localhost:8024/browser-chat npx electron .`
3. Test AI sidebar:
   - Simple greeting → should use Fast model (llama3.2:3b)
   - "Explain this page" → should use Quality model (gemma3:4b)
   - Complex question → should use Deep model (qwen3:8b)
   - Code question → should use Code model (qwen2.5-coder:7b)
4. Verify streaming responses work
5. Verify page context is passed correctly

## API Request/Response Format

### Browser → Orchestrator
```json
POST /browser-chat
{
  "message": "What batteries are on this page?",
  "pageContext": {
    "url": "https://www.homedepot.com/s/battery",
    "title": "Search Results",
    "text": "page visible text..."
  },
  "stream": true
}
```

### Orchestrator → Browser (Streaming SSE)
```
data: {"token": "The"}
data: {"token": " page"}
data: {"token": " shows"}
data: {"token": " several"}
data: [DONE]
```

## Dependencies

- **Docker Desktop** (already installed, data on D: drive)
- **Ollama** (already running on localhost:11434)
- **Python 3.13** (for orchestrator container)
- **PostgreSQL with pgvector** (Docker container)
- **Redis** (Docker container)

## Key Risks

1. **VRAM management** — Multiple models loaded simultaneously may exceed GPU memory. The orchestrator should load/unload models as needed (Ollama handles this automatically with LRU eviction).
2. **Cold start latency** — First query to a model tier may take 10-30s while Ollama loads the model into VRAM.
3. **assemblyline_common coupling** — The shared library may have dependencies on AssemblyLine-specific services. Need to audit and remove/stub those.
4. **Database migrations** — The orchestrator may expect specific tables. Need to run migrations on first start.

## Files to Modify in YAMIL Browser

| File | Change |
|------|--------|
| `electron-app/preload.js` | Update default AI_ENDPOINT to port 8024 |
| `electron-app/main.js` | Add AI_ENDPOINT env var in stealth tab browser-service launch |
| `electron-app/renderer/renderer.js` | No changes needed (already supports streaming SSE) |

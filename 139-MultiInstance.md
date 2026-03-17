# 139 — Multi-Instance YAMIL Browser Support

## Overview
Enables two (or more) Claude Code sessions to use YAMIL Browser simultaneously by running isolated Electron + browser-service instances that share the same AI/RAG backends (Ollama, PostgreSQL, Redis).

## Architecture

```
                       ┌─────────────────────────────────┐
                       │  Shared Services (Docker)        │
                       │  chat-service :8020              │
                       │  rag-service  :8022              │
                       │  ai-db        :5432              │
                       │  redis        :6379              │
                       │  ollama       :11434 (host)      │
                       └────────┬──────────┬──────────────┘
                                │          │
              ┌─────────────────┘          └──────────────────┐
              ▼                                               ▼
   ┌──────────────────────┐                     ┌──────────────────────┐
   │  Instance 1 (default) │                     │  Instance 2           │
   │  Electron     :9300   │                     │  Electron     :9301   │
   │  browser-svc  :4000   │                     │  browser-svc  :4001   │
   │  user-data: default   │                     │  user-data: instance-2│
   └──────────────────────┘                     └──────────────────────┘
        ▲                                               ▲
        │                                               │
   Claude Code (global)                          Claude Code (project)
   ~/.claude/settings.json                       project/.mcp.json
```

## Port Allocation

| Component | Instance 1 (default) | Instance 2 |
|---|---|---|
| Electron control server | 9300 | 9301 |
| browser-service (Playwright) | 4000 | 4001 |
| chat-service | 8020 (shared) | 8020 (shared) |
| rag-service | 8022 (shared) | 8022 (shared) |
| PostgreSQL | 5432 (shared) | 5432 (shared) |
| Redis | 6379 (shared) | 6379 (shared) |

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `YAMIL_CTRL_URL` | `http://127.0.0.1:9300` | MCP server target for Electron control |
| `YAMIL_BROWSER_URL` | `http://127.0.0.1:4000` | MCP server target for browser-service |
| `CTRL_PORT` | `9300` | Electron main process listen port |
| `BROWSER_SERVICE` | `http://127.0.0.1:4000` | Electron/renderer target for browser-service |

## Files Changed

### MCP Server
- **`mcp-server/src/services/browser-client.mjs`** — `YAMIL_CTRL` reads `YAMIL_CTRL_URL` env var
- **`mcp-server/src/tools/browser-mgmt.mjs`** — Dynamic port in messages; passes `CTRL_PORT` + `BROWSER_SERVICE` to spawned Electron
- **`mcp-server/src/tools/data.mjs`** — Replaced hardcoded `http://127.0.0.1:4000` with `BROWSER_SVC_URL`

### Electron App
- **`electron-app/preload.js`** — Exposes `CTRL_PORT` and `BROWSER_SERVICE` to renderer via `YAMIL_CONFIG`
- **`electron-app/renderer/renderer.js`** — All ~15 hardcoded URLs replaced with dynamic `CTRL_URL` / `BROWSER_SERVICE`
- **`electron-app/main.js`** — Injected credential script interpolates `CTRL_PORT` instead of hardcoded `9300`

### Docker
- **`docker-compose.yml`** — Added `browser-service-2` with `profiles: [multi-instance]`, port `4001:4000`

### New Files
- **`start-instance-2.sh`** — Launcher for instance 2

## How to Use

### Instance 1 (no changes needed)
```bash
./start-with-ai.sh              # or: docker compose up -d && cd electron-app && npm start
```

### Instance 2
```bash
./start-instance-2.sh           # starts browser-service-2 + Electron on 9301
```

### Per-Project MCP Config (Instance 2)
Create `.mcp.json` in the project root:
```json
{
  "mcpServers": {
    "yamil-browser": {
      "command": "node",
      "args": ["mcp-server/src/index.mjs"],
      "cwd": "/Users/yaml/Project/Git/yamil-browser",
      "env": {
        "YAMIL_BROWSER_URL": "http://127.0.0.1:4001",
        "YAMIL_CTRL_URL": "http://127.0.0.1:9301"
      }
    }
  }
}
```

### Global MCP Config (Instance 1 — default)
`~/.claude/settings.json` stays as-is (no `YAMIL_CTRL_URL` needed since it defaults to 9300).

## Backward Compatibility
All env vars have defaults matching the current single-instance values. Instance 1 works with zero config changes.

## Safety / Security Notes
- Each Electron instance uses a separate `--user-data-dir` to avoid profile corruption
- Both browser-service containers share the same database — browsing history, credentials, and knowledge are shared (this is intentional so both instances benefit from the same AI context)
- No ports are exposed beyond localhost (`127.0.0.1`)

## Verification Checklist
- [ ] Start instance 1 normally — confirm works as before
- [ ] Run `./start-instance-2.sh` — Electron opens on 9301, browser-service on 4001
- [ ] Two Claude Code sessions with different `.mcp.json` configs can navigate simultaneously
- [ ] Both share the same Ollama/RAG/knowledge base
- [ ] Credential auto-save works on both instances
- [ ] Ad blocker, cookies, bookmarks all route to the correct Electron instance

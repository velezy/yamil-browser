# 135 — YAMIL Browser: AI-First Credential Manager

**Status**: In Progress
**Created**: 2026-03-08
**Philosophy**: AI-First — the AI manages credentials, humans provide them once

---

## 1. The Problem

YAMIL Browser has no credential storage. Every time the AI needs to log into a site (bank, QNAP, Synology, etc.), it has to ask the human for credentials. This breaks the AI-first workflow — the AI should be able to log into any previously-visited site autonomously.

## 2. Design: AI-First, Not Password-Manager-First

This is NOT a traditional password manager with popups and autofill UI. The AI is the primary consumer:

1. **AI needs to log in** → checks `browser_credentials` table by domain
2. **Credentials exist** → decrypts password, fills form, logs in
3. **Credentials don't exist** → AI asks human once, saves for future
4. **Human never sees a password manager UI** — it's all MCP tools

### MCP Tools

| Tool | Description |
|------|-------------|
| `yamil_browser_credential_save` | Store credentials (domain, username, encrypted password) |
| `yamil_browser_credential_get` | Retrieve credentials by domain, optionally autofill |
| `yamil_browser_credential_list` | List saved domains + usernames (no passwords) |
| `yamil_browser_credential_delete` | Remove saved credentials |

### Security

- Passwords encrypted with **Electron `safeStorage`** (Windows DPAPI) before storage
- Encryption/decryption happens in the Electron app (port 9300), NOT in browser-service
- Database stores only encrypted blobs — useless without the OS keychain
- The `credential_get` tool returns the decrypted password only to the AI (never displayed to human)

## 3. Architecture

```
AI (Claude Code / Assemblyline)
  │
  ├── yamil_browser_credential_save(domain, username, password)
  │     └── MCP Server → POST /credentials to Electron (port 9300)
  │           └── Electron: safeStorage.encryptString(password)
  │                 └── INSERT into browser_credentials (pgvector DB, port 5433)
  │
  ├── yamil_browser_credential_get(domain)
  │     └── MCP Server → GET /credentials?domain=... from Electron
  │           └── Electron: SELECT from DB → safeStorage.decryptString(password)
  │                 └── Returns { domain, username, password } to AI
  │
  └── yamil_browser_credential_list()
        └── MCP Server → GET /credentials/list from Electron
              └── Returns [{ domain, username, lastUsed }] (no passwords)
```

## 4. Database Schema

Added to the existing `yamil_browser` pgvector database (port 5433):

```sql
CREATE TABLE browser_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain TEXT NOT NULL,
    username TEXT NOT NULL,
    password_encrypted TEXT NOT NULL,
    label TEXT,
    form_url TEXT,
    notes TEXT,
    last_used TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(domain, username)
);
CREATE INDEX idx_credentials_domain ON browser_credentials(domain);
```

## 5. Implementation Files

| File | Change |
|------|--------|
| `electron-app/main.js` | Add `/credentials`, `/credentials/list`, `/credentials/delete` HTTP endpoints |
| `browser-service/sql/init.sql` | Add `browser_credentials` table |
| `mcp-server/src/tools/credentials.mjs` | New — 4 MCP tools |
| `mcp-server/src/index.mjs` | Register credential tools |

## 6. Implementation Order

- [x] Phase 1: Database schema (migration + init.sql)
- [x] Phase 2: Electron endpoints (encrypt/decrypt via safeStorage)
- [x] Phase 3: MCP tools (credential_save, credential_get, credential_list, credential_delete)
- [x] Phase 4: Wire into index.mjs
- [x] Phase 5: Test with real credentials (QNAP + Synology saved, autonomous QNAP login tested)
- [x] Phase 6: Auto-save credentials on login form submission

## 7. Auto-Save (Phase 6)

The browser now automatically detects login forms and saves credentials on submission — no manual `credential_save` call needed.

### How it works

1. **Observer script injected** into every webview on page load
2. Detects `<input type="password">` fields on the page
3. Watches for form submit, Enter key, or login button click
4. Captures domain + username + password at submission time
5. Sends to `POST /credentials/auto-save` on Electron (port 9300)
6. Electron encrypts via safeStorage → stores in pgvector DB
7. MutationObserver re-scans for dynamically added password fields (SPA support)

### Flow

```
User types credentials → clicks Login
  │
  └── Observer script captures { domain, username, password }
        └── POST http://127.0.0.1:9300/credentials/auto-save
              └── Electron: safeStorage.encryptString(password)
                    └── POST http://127.0.0.1:4000/credentials (browser-service)
                          └── UPSERT into browser_credentials
```

### Files changed

| File | Change |
|------|--------|
| `electron-app/renderer/renderer.js` | Inject credential observer into webviews on `did-stop-loading` |
| `electron-app/main.js` | Add `POST /credentials/auto-save` endpoint (encrypt + store in one call) |

## 8. Additional Improvements

- **Chrome user agent spoof** — `session.setUserAgent()` on both default and `persist:yamil` sessions to prevent "Incompatible browser" blocks
- **SPA-resilient a11y_click** — Uses direct DOM Map references instead of `data-yamil-ref` attributes (which SPAs strip on re-render)

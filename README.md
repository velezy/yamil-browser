# YAMIL Browser

Stealth Playwright browser microservice + Electron desktop shell.
Used by YAMIL, DriveSentinel, and Memobytes for all browser automation.

```
┌──────────────────────────────────────────────────────┐
│              yamil-browser                           │
│                                                      │
│  browser-service/   ←── Fastify + Playwright         │
│    REST API  :4000  ←── sessions, navigate, click…   │
│    WebSocket :4000  ←── live screencast + CDP events │
│                                                      │
│  electron-app/      ←── Desktop shell (Electron 33)  │
│    canvas screencast, address bar, AI sidebar        │
│                                                      │
│  yamil_browser_client.py  ←── Python SDK (copy-able) │
└──────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Run the browser service

```bash
cd browser-service
npm install
npm start          # starts Fastify on :4000
```

### 2. Launch the Electron desktop app

```bash
cd electron-app
npm install

# Pick the launcher for your project:
./launch-yamil.sh           # Mac/Linux
./launch-drivesentinel.sh
./launch-memobytes.sh

launch-yamil.bat            # Windows
launch-drivesentinel.bat
launch-memobytes.bat
```

### 3. Or run via Docker

```bash
# Build
docker build -t yamil-browser ./browser-service

# Run
docker run -p 4000:4000 --shm-size=2gb yamil-browser
```

Pre-built image (public):
```bash
docker pull ghcr.io/velezy/yamil-browser:latest
```

---

## API Reference

All endpoints are prefixed with `/sessions/:id`.

### Session lifecycle

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sessions` | Create new session → `{id}` |
| `DELETE` | `/sessions/:id` | Destroy session |
| `GET` | `/sessions` | List all sessions |

### Navigation

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/sessions/:id/navigate` | `{url, waitUntil?}` | Navigate to URL |
| `POST` | `/sessions/:id/back` | — | Go back |
| `POST` | `/sessions/:id/press` | `{key}` | Press keyboard key |

### Page data

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/sessions/:id/url` | Current `{url, title}` |
| `GET` | `/sessions/:id/content` | Full HTML source |
| `GET` | `/sessions/:id/screenshot` | JPEG screenshot (raw bytes) |
| `GET` | `/sessions/:id/cookies` | All cookies |
| `POST` | `/sessions/:id/evaluate` | `{script}` → `{result}` |

### Interactions

| Method | Path | Body | Description |
|--------|------|------|-------------|
| `POST` | `/sessions/:id/click` | `{selector}` | Click element |
| `POST` | `/sessions/:id/fill` | `{selector, value}` | Fill input |
| `POST` | `/sessions/:id/hover` | `{selector}` | Hover element |
| `POST` | `/sessions/:id/select` | `{selector, value}` | Select option |
| `POST` | `/sessions/:id/scroll` | `{direction, amount}` | Scroll page |
| `POST` | `/sessions/:id/wait` | `{selector, timeout}` | Wait for element |
| `POST` | `/sessions/:id/mouse/click` | `{x, y}` | Click by coordinate |
| `POST` | `/sessions/:id/mouse/move` | `{x, y}` | Move by coordinate |
| `POST` | `/sessions/:id/keyboard/type` | `{text}` | Type text |

### WebSocket streams

| Path | Description |
|------|-------------|
| `ws://.../sessions/:id/screencast` | JPEG frames `{frame, metadata}` |
| `ws://.../sessions/:id/events` | CDP event stream |

---

## Python SDK

Copy `yamil_browser_client.py` into any Python project (requires `httpx`):

```python
from yamil_browser_client import YamilBrowserClient

async with YamilBrowserClient("http://localhost:4000") as browser:
    await browser.navigate("https://example.com")
    html  = await browser.content()
    png   = await browser.screenshot_bytes()
    await browser.click("button#submit")
    await browser.fill("input[name=email]", "user@example.com")
```

---

## Electron App

The desktop shell renders the browser's screencast on a `<canvas>` and lets you:

- Click / scroll / type directly on the canvas (coordinates mapped to page)
- Navigate via the address bar
- Chat with your app's AI in the sidebar (set `AI_ENDPOINT` env var)

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `BROWSER_SERVICE_URL` | `http://localhost:4000` | Browser service URL |
| `AI_ENDPOINT` | — | Orchestrator chat URL for the sidebar |
| `APP_TITLE` | `YAMIL Browser` | Window / sidebar title |

---

## Wiring into your project

### docker-compose.yml

```yaml
services:
  yamil-browser:
    image: ghcr.io/velezy/yamil-browser:latest
    ports:
      - "4000:4000"
    volumes:
      - yamil-browser-profile:/data/chrome-profile
    shm_size: "2gb"

  your-app:
    environment:
      - YAMIL_BROWSER_URL=http://yamil-browser:4000

volumes:
  yamil-browser-profile:
```

### Projects already wired

| Project | Compose | Client |
|---------|---------|--------|
| YAMIL (parser_lite/logic-weaver) | `docker-compose.yml` | `services/ai-builder-orchestra-service/browser_service_client.py` |
| DriveSentinel (ai-bot) | `docker/docker-compose.yml` | `services/orchestrator/app/agents/yamil_browser_client.py` |
| Memobytes (flashcard-app) | `docker-compose.yml` | `app/services/yamil_browser_client.py` |

---

## CI/CD

GitHub Actions (`.github/workflows/docker.yml`) builds and pushes to GHCR on every push to `main`.

```
ghcr.io/velezy/yamil-browser:latest
ghcr.io/velezy/yamil-browser:sha-<short>
```

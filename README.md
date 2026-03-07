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


___________________________________

 Databricks CDC: Optimal Approach for YAMIL

  The Bottom Line

  Your current polling approach is the correct enterprise pattern.   
  Databricks does NOT offer native push notifications when a Delta   
  table changes. Every integration platform (MuleSoft, etc.) uses the
   same polling pattern you're using.

  ---
  What Databricks Actually Offers

  ┌────────────────┬───────────┬──────────────┬────────────────┐     
  │    Approach    │ Real-time │  Practical?  │     Notes      │     
  │                │     ?     │              │                │     
  ├────────────────┼───────────┼──────────────┼────────────────┤     
  │ Polling via ta │ 30-60s    │ Best for     │ No Databricks- │     
  │ ble_changes()  │ latency   │ external     │ side infra     │     
  │ (your current) │           │ platforms    │ needed         │     
  ├────────────────┼───────────┼──────────────┼────────────────┤     
  │ Structured     │           │ Only if      │ Requires       │     
  │ Streaming +    │ 10s - sub │ running code │ always-on      │     
  │ CDF            │ -second   │  inside      │ Spark cluster  │     
  │                │           │ Databricks   │                │     
  ├────────────────┼───────────┼──────────────┼────────────────┤     
  │                │           │              │ Notifications  │     
  │ Databricks Job │ 1 min+    │ Marginal     │ are about job  │     
  │  webhooks      │           │ improvement  │ status, not    │     
  │                │           │              │ table changes  │     
  ├────────────────┼───────────┼──────────────┼────────────────┤     
  │                │           │              │ 1 alert per    │     
  │ SQL Alerts     │ 1-5 min   │ Not scalable │ table, max ~50 │     
  │                │           │              │  alerts        │     
  ├────────────────┼───────────┼──────────────┼────────────────┤     
  │                │           │              │ Tells you      │     
  │ Unity Catalog  │ Minutes   │ Supplementar │ something      │     
  │ audit logs     │ to hours  │ y only       │ changed, not   │     
  │                │           │              │ what changed   │     
  ├────────────────┼───────────┼──────────────┼────────────────┤     
  │                │           │              │ S3 file        │     
  │ AWS            │ Near      │ File-level   │ events, not    │     
  │ EventBridge    │ real-time │ only         │ Delta          │     
  │                │           │              │ operations     │     
  └────────────────┴───────────┴──────────────┴────────────────┘     

  There is no Databricks webhook that fires "table X changed, here's 
  the data."

  ---
  Scalability of Your Current Architecture

  ┌────────────────┬────────────────────┬────────────────────────┐   
  │   Dimension    │  Current Capacity  │ Max with Optimization  │   
  ├────────────────┼────────────────────┼────────────────────────┤   
  │ Tables         │ ~400 (4 workers x  │ ~1,000 (increase       │   
  │ monitored      │ 20/batch)          │ workers, batch         │   
  │                │                    │ queries)               │   
  ├────────────────┼────────────────────┼────────────────────────┤   
  │ Polling        │ 30-60s             │ 10-15s (keep warehouse │   
  │ latency        │                    │  warm)                 │   
  ├────────────────┼────────────────────┼────────────────────────┤   
  │ Downstream API │ Unlimited per flow │ Hundreds per change    │   
  │  recipients    │  (fan-out in       │ event                  │   
  │                │ nodes)             │                        │   
  ├────────────────┼────────────────────┼────────────────────────┤   
  │ Concurrent     │ 4 Python           │ Scale with more        │   
  │ flows          │ Transforms         │ flow-execution         │   
  │                │                    │ containers             │   
  └────────────────┴────────────────────┴────────────────────────┘   

  ---
  Recommended Optimizations (Short-term)

  1. Keep warehouse warm — Set auto-stop to 10+ minutes (eliminates  
  5-15s cold start)
  2. Batch DESCRIBE HISTORY — Check multiple tables in one SQL call  
  with UNION ALL before fetching actual changes
  3. Adaptive polling — Reduce interval for frequently-changing      
  tables, increase for stable ones (your backoff already does this   
  for errors)

  Future Enhancement (Medium-term): Push-Triggered Pull

  Add a lightweight Databricks Job (runs every 1-2 min) that:        
  1. Checks DESCRIBE HISTORY for all monitored tables
  2. POSTs a webhook to YAMIL with only the table names that changed 
  3. YAMIL immediately polls only those tables

  This reduces unnecessary polling by ~80% and cuts latency to ~1-2  
  minutes.

  Long-term (Only if Premium/Enterprise Tier)

  Deploy Structured Streaming inside Databricks → push to Kafka →    
  YAMIL consumes. Only worth it at 500+ tables with sub-30s latency  
  requirements.

  ---
  Your cdc_coordinator.py is well-architected and production-grade.  
  The industry standard for external platforms monitoring Databricks 
  is exactly what you're doing — intelligent polling with
  checkpointing, batching, and error recovery.




```

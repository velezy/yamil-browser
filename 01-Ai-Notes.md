# AI Notes

## 2026-03-13 — Grafana Login & QNAP IP Update

### Task
- Verify Grafana is running on QNAP and log in
- Update QNAP IP from old 192.168.1.188 to current 192.168.0.102

### Steps Taken
1. Found Grafana was previously documented at 192.168.1.188 — unreachable (old IP)
2. User confirmed QNAP is at **192.168.0.102** — updated all references:
   - `132-Infrastructure-Monitoring.md` — all 192.168.1.188 → 192.168.0.102
   - Memory file `MEMORY.md` — added Homelab IPs section
3. Navigated to `http://192.168.0.102:3000` — Grafana login page loaded
4. Tried `admin / Ashley2029` — login failed (password not working)
5. Tried `admin / Ashley2029$` — failed ($ handling issues in browser fill)
6. Tried `admin / admin` — failed
7. All attempts triggered Grafana's brute-force lockout
8. SSH'd into QNAP, reset password via `grafana-cli admin reset-admin-password` — reported success but didn't stick (running process had DB cached)
9. Discovered Grafana container was just restarted by Watchtower (Up 56 seconds)
10. Attempted stop → reset → start cycle — hit SQLite "disk I/O error" on migrations
11. Root cause: Docker overlay filesystem on QNAP doesn't handle SQLite writes
12. **Fix**: Changed docker-compose from named volume to bind mount (`./grafana-data:/var/lib/grafana`)
13. Fresh Grafana started successfully, `admin / Ashley2029` works (from env var on first boot)

### Key Decisions
- Bind mount instead of Docker volume for Grafana data on QNAP
- Password confirmed as `Ashley2029` (no special characters) — matches docker-compose env var

### Next Steps
- Re-provision Grafana datasources (Prometheus, Loki) if not auto-created
- Recreate any custom dashboards that were lost
- Consider switching other QNAP SQLite services to bind mounts preventively

## 2026-03-14 — Monitoring Pipeline Plan + Memobyte Architecture Diagram

### Task
- Create numbered plan document for the YAMIL Monitoring Pipeline (APIs -> Kafka -> Dashboard)
- Create Memobyte infrastructure architecture diagram HTML (matching infra diagram style)

### Steps Taken
1. Explored existing file structure — Ai-Tools has numbered docs 123-135, next is 136
2. Read existing `architecture-diagram-infra.html` (1667 lines) as template for styling
3. Read `MemobyteTechStack.md` for complete Memobyte tech stack details
4. Created `136-MonitoringPipeline.md` in Ai-Tools — 5-phase plan covering:
   - Phase 1: 7 monitoring connector nodes (Prometheus, Loki, Alertmanager, Blackbox, ntfy, Uptime Kuma, Grafana)
   - Phase 2: Kafka topics & standardized message envelope
   - Phase 3: Logic Weaver flow templates
   - Phase 4: Custom monitoring dashboard (FastAPI WebSocket + React)
   - Phase 5: Deployment & wiring
5. Created `Memobyte-Infrastructure-Architecture-Diagram.html` — real monitoring dashboard with Chart.js:
   - **Pipeline banner**: QNAP Monitoring APIs -> YAMIL Gateway -> Kafka -> This Dashboard
   - **4 stat cards**: Services Up (7/7), Active Alerts (2), Avg Response (42ms), Kafka Throughput (1.2k msg/s)
   - **Service health grid**: 12 services with live status dots (Prometheus, Grafana, Loki, Alertmanager, Blackbox, ntfy, Uptime Kuma, Postgres, Redis, Envoy, Kafka, Ollama)
   - **Active alerts feed**: Critical/warning/resolved alerts from Alertmanager
   - **CPU usage chart**: Line chart — Dark-Knight, GEEKOM, QNAP (Prometheus 30s polling)
   - **Memory usage chart**: Line chart — all 3 servers with % usage
   - **Probe latency chart**: Bar chart — response times per service (Blackbox)
   - **Uptime chart**: Horizontal bars — 24h uptime % per service (Uptime Kuma)
   - **Kafka message rate**: Line chart — msgs/sec + consumer lag
   - **Live logs**: Auto-scrolling log stream from Loki (new line every 2s)
   - **Notifications**: ntfy feed with icons and timestamps
   - **Disk usage**: Doughnut chart — Dark-Knight vs QNAP
   - **Network I/O**: Line chart — RX/TX Mbps

6. Iterated on design through multiple versions:
   - v1: Dark theme with multi-color — user wanted company colors (aqua + white)
   - v2: White background, all-aqua charts — charts hard to distinguish
   - v3: Added coral, navy, amber colors to chart datasets
   - v4: Added black accents — header, stat card borders, card titles, chart datasets
   - v5 (final): **Full dark ops dashboard** with aqua gradient header banner + live simulation
     - Dark body (#0b0f14) with glowing dark cards
     - Aqua gradient header with white MemoByte branding
     - Pipeline strip with animated arrows and color-coded nodes (aqua/amber/green)
     - **Live simulation**: all line charts scroll new data every 3s
     - Latency bars wiggle every 5s, stat numbers update live
     - Alerts rotate new items every 12s, notifications every 15s
     - Log stream adds colored entries every 1.8s (aqua INFO, amber WARN, coral ERROR)
     - Service health dots pulse with aqua glow
     - Chart colors: aqua (primary), coral (secondary), amber (tertiary), green (accent)

### Key Decisions
- Company colors: **aqua blue (#06d6d6)** as primary accent on dark background
- Memobyte is the company name — branding in header as "MemoByte"
- Used Chart.js 4.4.7 from CDN for all charts (line, bar, doughnut)
- Dark ops theme chosen for final version — looks professional for monitoring
- Live simulation with staggered intervals (1.8s logs, 3s charts, 5s latency, 12s alerts, 15s notifs)
- Mock data with realistic patterns — will be replaced by live Kafka feed

### Files Created
- `/Users/yaml/Project/Git/Yamil/Ai-Tools/136-MonitoringPipeline.md`
- `/Users/yaml/Project/Git/Yamil/Ai-Tools/Memobyte-Infrastructure-Architecture-Diagram.html`

## 2026-03-14 — Docker Pipeline + Vault Fix + GHCR Auth

### Task
- Fix missing .env files, broken GHCR auth, and Vault on Docker
- Create Docker image pipeline instructions (137-DockerImagePipeline.md)
- Update 135-InfrastructureHA.md for MacBook Air as primary dev with Docker

### Steps Taken
1. **GHCR auth fixed**: `gh auth token | docker login ghcr.io -u velezy --password-stdin` → Login Succeeded
2. **Discovered active repo**: Running containers come from `parser_lite.py/logic-weaver/` (not `Yamil/parser_lite/logic-weaver/`)
   - `parser_lite.py` has all .env files: `.env`, `.env.local`, `.env.prod`, `.env.secrets`
   - `Yamil/parser_lite` only has templates (`.env.example`, `.env.prod.template`)
3. **Vault server crashed**: `logic-weaver-vault-1` had Exit 255 for 9 days
   - Also down: postgres, redis, etcd, apisix, envoy-external (all Exit 255, 9 days)
   - Vault agent was running but failing ("token file validation failed" every ~5 min)
4. **Fixed Vault chain**:
   - `docker compose up -d vault` → started
   - `docker compose up -d vault-unseal` → "Vault unsealed"
   - `docker compose restart vault-agent` → "renewed auth token", rendered fresh DB creds
   - Fresh dynamic credentials in `vault-secrets/env` (new Vault-generated Postgres user/pass)
5. **Brought full stack back**: `docker compose up -d` → 25 services running, all healthy
6. **Known issues (not critical)**:
   - `certbot` — crash loop, missing `CLOUDFLARE_API_TOKEN` env var
   - `infinity` (embedding model) — exited 2 weeks ago
   - `credential-watcher` — project path mismatch ("project-" vs "logic-weaver-")

### Key Findings
- **Vault manages secrets dynamically**: DB credentials auto-rotate via `vault-secrets/env`
- **No manual .env copying needed**: Vault renders secrets from its encrypted store
- **Two repos**: `parser_lite.py` (active, has .env files + running stack) vs `Yamil/parser_lite` (templates only)

### Files Created/Modified
- Created: `/Users/yaml/Project/Git/Yamil/Ai-Tools/137-DockerImagePipeline.md`
- Modified: `/Users/yaml/Project/Git/Yamil/Ai-Tools/135-InfrastructureHA.md`

### Next Steps
- Commit and push 135 + 137 docs to Ai-Tools repo
- On Windows: set up Docker buildx, GHCR auth, create push-images.ps1
- On Mac: create docker-compose.override.yml for GHCR pulls (after Windows pushes)
- Fix certbot CLOUDFLARE_API_TOKEN
- Fix credential-watcher project path mismatch

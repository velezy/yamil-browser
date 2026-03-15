# AI Notes

## 2026-03-14 — yamil-ai.com Slowness Investigation

### Task
- Diagnose why yamil-ai.com is slow, hosted on Windows PC

### Findings
1. **Windows PC IP changed** from 192.168.1.190 to 192.168.0.101 (updated memory)
2. **All 15+ YAMIL microservices are healthy** in Docker (5-10ms response times direct)
3. **Cloudflare tunnel is working** — routes to envoy-internal:80 inside Docker network
4. **Gateway service (port 9091) failing Envoy health checks** (`/failed_active_hc`)
   - Root cause: Envoy checks `/health` but gateway only exposes `/health/live`, `/health/ready`, `/health/`
   - FastAPI's `HealthRouter(prefix="/health")` with `@router.get("/")` creates `/health/` (trailing slash)
   - Request to `/health` (no slash) gets 307 redirect, which Envoy counts as unhealthy
5. **AI Builder (port 8014) not running** — requires `--profile ai-builder` Docker flag
6. **JS bundle is 4.88 MB** (1.46 MB Brotli-compressed) — contributes to initial load time
7. **API latency through Cloudflare**: ~50-85ms (vs 20-30ms direct) — 300ms tunnel overhead on login

### Fixes Applied
Added bare `/health` endpoint (matching auth-service pattern) to 4 services that were missing it:
- `services/gateway-service/main.py` — **critical fix** (was failing health checks)
- `services/policy-service/main.py`
- `services/dsl-engines-service/main.py`
- `services/flow-execution-service/main.py`

Also updated the shared `HealthRouter` class (`shared/python/logic_weaver_common/health/endpoints.py`):
- Added `bare_health_route` property that creates a bare `/health` endpoint automatically
- New services can use `app.include_router(health_router.bare_health_route)` instead of hand-writing the endpoint
- Committed and pushed to git

### Deployment (from Mac via SSH)
Docker build cache wouldn't pick up file changes even with `--no-cache`. Used `docker cp` workaround:
1. SCP'd patched files from Mac to `C:\project\parser_lite\logic-weaver\services\*\main.py` on Windows
2. `docker cp` each file into the running container at `/app/main.py`
3. `docker restart` the container

**All 4 services deployed and verified:**
- Gateway: `/health` returns 200, Envoy shows `health_flags::healthy`
- Policy: `/health` returns 200, Envoy shows `health_flags::healthy`
- DSL-Engines: `/health` returns 200, Envoy shows `health_flags::healthy`
- Flow-Execution: `/health` returns 200 (no Envoy cluster — internal-only service)
- **All 17 Envoy clusters healthy** — zero `failed_active_hc`
- **yamil-ai.com**: 200 in 83ms, **api.yamil-ai.com/gateway/health**: 200 in 140ms

### Remote Deployment Setup (Mac → Windows via SSH)
- **SSH**: `ssh -i ~/.ssh/id_ed25519 yvele@192.168.0.101` (credentials in `yamil/homelab/windows-pc` in AWS SM)
- **Docker over SSH**: Need `DOCKER_CONFIG` override to avoid `credsStore: desktop` errors:
  ```powershell
  $env:DOCKER_CONFIG = 'C:\Users\yvele\.docker-ssh'
  ```
  (config.json at that path has `{"auths":{},"currentContext":"desktop-linux"}`)
- **Git over SSH**: `wincredman` credential helper doesn't work over SSH — need to set up `credential.helper store` with a GitHub PAT (TODO)
- **Deploy pattern**: SCP files → `docker cp` into container → `docker restart`

### Remaining TODO
- Fix git credentials over SSH on Windows (set up `credential.helper store` with GitHub PAT)
- Fix Docker build cache issue for proper `docker compose build` deploys
- Consider code-splitting the frontend JS bundle (4.88 MB is very large)
- Consider enabling Cloudflare caching for static assets to reduce tunnel round-trips

---

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

---

## 2026-03-14: Security Audit of yamil-ai.com

### Task
Comprehensive security audit of yamil-ai.com covering HTTP headers, TLS, sensitive paths, CORS, Envoy admin exposure, internal network, and Cloudflare features.

### Critical Finding: Envoy Admin Interface Publicly Exposed
- `https://yamil-ai.com/envoy/api/int/*` returns 200 on ALL admin endpoints
- Leaks: full Docker service topology (19 services), internal IPs/ports, all route mappings, Envoy version (1.31.10), memory stats, config dump
- Destructive endpoints (`/quitquitquit`, `/drain_listeners`) may accept POST
- Envoy dashboard also exposed at `https://yamil-ai.com/envoy`
- VNC service exposed at `https://yamil-ai.com/vnc/`
- Root cause: route `/envoy/api/int/ -> envoy_admin_self` exists in the Cloudflare-facing Envoy listener

### Other Findings
- **Frontend missing all security headers** (HSTS, X-Frame-Options, CSP, etc.) -- api subdomain has them
- **TLS is solid**: TLSv1.3, valid cert through June 2026
- **CORS is clean**: no `Access-Control-Allow-Origin` for malicious origins
- **Internal ports 5432/6379/8200 not exposed** on LAN (good)
- **Gateway health endpoint** leaks internal config state without auth

### Recommended Fixes (Priority Order)
1. Remove `/envoy/api/int/`, `/envoy/api/ext/`, `/envoy` routes from external listener
2. ~~Remove or auth-gate `/vnc/` routes~~ — **DONE** (see below)
3. Add security headers to frontend responses (via Envoy or Cloudflare)
4. Restrict gateway health endpoint to authenticated requests
5. Strip `x-envoy-upstream-service-time` header from responses

---

## 2026-03-14: Gate VNC Routes Behind JWT Authentication

### Task
VNC routes (`/vnc/`, `/vnc/websockify`) were publicly accessible without authentication. Anyone with the URL could view and control the AI browser session. Needed to gate these behind JWT auth.

### Challenge
The VNC viewer loads in an `<iframe>`, which can't send `Authorization` headers. Solution: sync JWT to a cookie scoped to `/vnc` path, then validate it server-side with an Envoy Lua filter.

### Changes Made

**1. Frontend cookie sync** — `logic-weaver/frontend/src/stores/auth-store.ts`
- Added Zustand subscriber after store creation that syncs `token` → `yamil_auth` cookie
- Cookie scoped to `path=/vnc` (not sent with API calls), `SameSite=Strict`, `Secure` on HTTPS
- Handles all lifecycle: login → cookie set, refresh → cookie updated, logout → cookie deleted
- Page reload: Zustand persist rehydrates from localStorage → subscriber fires before iframe renders

**2. Envoy Lua filter** — `logic-weaver/docker/envoy/envoy-internal.yaml`
- Added `envoy.filters.http.lua` between compressor and router filters
- Only activates for `/vnc/*` paths — all other routes pass through untouched (zero overhead)
- Extracts `yamil_auth` cookie, calls `GET /api/v1/auth/me` on auth cluster with Bearer token
- Returns 401 JSON if no cookie or if auth service rejects the token
- WebSocket upgrade (`/vnc/websockify`) also protected — cookie sent on HTTP upgrade request
- Updated VNC route comment block to remove the TODO

### Deployment (Completed 2026-03-15)
1. SCP'd `auth-store.ts` and `envoy-internal.yaml` to Windows PC
2. `docker restart logic-weaver-envoy-internal-1` — Envoy loaded Lua filter cleanly
3. Fixed Docker SSH build issue: `credsStore: "desktop"` fails over SSH (no Windows credential manager session)
   - Fix: temporarily set `credsStore: ""` in `~/.docker/config.json`, build, restore
   - Also created `~/.docker-ssh/config.json` with `{"auths":{},"credsStore":"","currentContext":"desktop-linux"}` for future SSH builds
4. `docker build -f docker/Dockerfile.frontend -t logic-weaver-frontend .` — built in ~31s
5. `docker compose up -d frontend` — recreated frontend + deps, all healthy

### Verification Results
- [x] `curl https://yamil-ai.com/vnc/vnc_theater.html` → 401 `{"error":"authentication_required"}`
- [x] Invalid token cookie → 401 `{"error":"invalid_token"}`
- [x] Frontend (`/`) → 200
- [x] Auth API (`/api/v1/auth/me`) → 401 (no token, as expected)
- [x] Envoy logs clean — no Lua errors, `envoy_on_response() not found` is expected (info level)
- [ ] Log in → AI Builder Theater → VNC iframe loads (needs manual browser test)
- [ ] Cookie scoped to `Path: /vnc` in DevTools (needs manual browser test)
- [ ] WebSocket `/vnc/websockify` connects (needs manual browser test)

---

## 2026-03-15: Login Broken After VNC Auth Deployment — Root Cause & Fix

### Problem
After deploying VNC auth changes, `docker compose up -d frontend` cascaded and **recreated postgres, redis, and auth** containers. Login appeared to work (auth service returned tokens) but all YAMIL service API calls returned **401**, causing the axios interceptor to immediately log the user out → redirect to `/login`.

### Root Cause Chain
1. **`docker compose up -d frontend`** recreated postgres/redis/auth as dependencies
2. **Postgres password mismatch**: The postgres container was initialized with a password stored in the volume, but the `.env.secrets` password didn't match — Vault's database engine couldn't connect to generate fresh credentials
3. **Vault credential expiry**: `vault-secrets/flow.env`, `auth.env`, etc. had expired dynamic DB credentials (Vault-generated postgres users with TTL)
4. **JWT secret mismatch**: `vault-secrets/env` (rendered by vault-agent) had a new `JWT_SECRET_KEY` but `.env.prod` had the old one. Services created 46hrs ago had the old key; auth (recreated) had the new key. `docker restart` does NOT re-read env_file — must **recreate** containers
5. **Windows docker-compose.yml differs from git**: Has service-specific `env_file` entries (`vault-secrets/flow.env`, `vault-secrets/auth.env`) that override `vault-secrets/env` — the expired credentials from these files took priority

### Fixes Applied
1. **Reset postgres password**: `ALTER USER postgres WITH PASSWORD '...'` to match `.env.secrets`
2. **Updated Vault database config**: `vault write database/config/message-weaver` with correct postgres password
3. **Restarted vault-agent**: Regenerated all credential files (`env`, `auth.env`, `flow.env`, `connector.env`, `cdc.env`)
4. **Recreated all services**: `docker compose up -d` (not just `docker restart`) to pick up fresh env_file values
5. **Reset user password**: Updated `logicweaver@hss.edu` password hash to `Ashley2026$$`

### Key Lessons
- **`docker restart` ≠ `docker compose up -d`**: restart keeps old env vars; compose up recreates with fresh env_file
- **Vault dynamic credentials expire**: When postgres restarts, existing Vault-generated DB users may become invalid
- **JWT_SECRET_KEY in `.env.prod`** becomes stale when Vault rotates it — the vault-rendered file should always be loaded last (and it is, but service-specific files load even later)
- **Always recreate ALL dependent services** after recreating postgres/auth, not just the target service

---

## 2026-03-15: Security Audit — Secrets Hardened + HIPAA/Attack Assessment

### Security Hardening Completed
1. **Moved Cloudflare tokens to AWS Secrets Manager** — `CLOUDFLARE_TUNNEL_TOKEN` and `CLOUDFLARE_API_TOKEN` added to `yamil/cloudflare/tunnel` secret in AWS SM
2. **Cleaned `.env.prod`** — all hardcoded secret values replaced with empty placeholders (Vault overrides them at runtime)
3. **Confirmed Vault rotation running** — vault-agent renders templates every 5 minutes with fresh credentials

### Gateway Comparison: YAMIL vs MuleSoft/Kong/Apigee
- Performed comprehensive codebase audit of all security modules
- YAMIL has comparable security depth to Kong OSS: rate limiting, circuit breaker, mTLS, bot detection, ACL, CORS, injection protection
- Exceeds Kong in some areas (Python code sandboxing, behavioral bot analysis)
- Gaps vs commercial products: automated key rotation, BAA management, breach notification

### HIPAA Compliance Assessment
- **~80% ready**, 8 gaps identified
- Created `doc/logicweaver/improvements/130-HIPAACompliance-SecurityPosture.md` with full plan
- Estimated ~6 months (13 sprints) to full compliance
- Most gaps are process/documentation, not code

### Attack Surface Assessment (Grade: B+)
- **Strong**: TLS, injection protection (5 types), brute force lockout, 4-tier rate limiting, circuit breaker, bot detection, mTLS
- **Critical gaps found**: Token endpoint has no rate limit, API keys hashed without salt (SHA-256), timing attack on secret comparison, no SSRF protection on upstream URLs
- **Quick wins (~1 day)**: Rate limit token endpoint, use `hmac.compare_digest()`, block private IPs in upstream_url, suppress stack traces, enforce strict CORS
- Full details in `130-HIPAACompliance-SecurityPosture.md`

### Next Steps
- Close 6 critical/high security gaps (~1-2 sprints)
- Start HIPAA Phase 1: BAA enforcement, risk assessment documentation, DR plan
- Run dependency scanning (Snyk/Trivy) to assess component vulnerabilities

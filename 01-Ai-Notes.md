# AI Notes

## 2026-03-17 — Multi-Instance YAMIL Browser Support

### Task
Enable two Claude Code sessions to use YAMIL Browser simultaneously via isolated instances that share AI/RAG backends.

### Changes Made
1. **`mcp-server/src/services/browser-client.mjs`** — `YAMIL_CTRL` now reads from `process.env.YAMIL_CTRL_URL` (defaults to `http://127.0.0.1:9300`)
2. **`mcp-server/src/tools/browser-mgmt.mjs`** — Dynamic port in user-facing messages; passes `CTRL_PORT` and `BROWSER_SERVICE` env vars when spawning Electron
3. **`mcp-server/src/tools/data.mjs`** — Replaced hardcoded `http://127.0.0.1:4000` with `BROWSER_SVC_URL` for session resize
4. **`electron-app/preload.js`** — Exposes `CTRL_PORT` and `BROWSER_SERVICE` to renderer via `YAMIL_CONFIG`
5. **`electron-app/renderer/renderer.js`** — All ~15 hardcoded `http://127.0.0.1:9300` and `http://127.0.0.1:4000` replaced with dynamic `CTRL_URL` / `BROWSER_SERVICE` constants
6. **`electron-app/main.js`** — Injected credential auto-save script now interpolates `CTRL_PORT` (already had env var support for `CTRL_PORT` and `BROWSER_SVC`)
7. **`docker-compose.yml`** — Added `browser-service-2` with `profiles: [multi-instance]`, port `4001:4000`
8. **`start-instance-2.sh`** (new) — Launches instance 2 with `CTRL_PORT=9301`, `BROWSER_SERVICE=http://127.0.0.1:4001`, separate user-data-dir
9. **`FlashCards/.mcp.json`** — Updated to use instance 2 ports (`4001`/`9301`)

### Port Allocation
| Component | Instance 1 | Instance 2 |
|---|---|---|
| Electron control | 9300 | 9301 |
| browser-service | 4000 | 4001 |
| chat/rag/db/redis/ollama | shared | shared |

### Backward Compatibility
All env vars default to current values — instance 1 works with zero config changes. Global `~/.claude/settings.json` keeps instance 1 defaults.

### Verification Steps
1. Start instance 1 normally (`start-with-ai.sh`) — confirm works as before
2. Run `./start-instance-2.sh` — confirm Electron opens on 9301, browser-service on 4001
3. Launch two Claude Code sessions with different `.mcp.json` configs
4. Both navigate simultaneously without conflicting

---

## 2026-03-16 — IoT Zone Isolation, Network Architecture Diagram, Address Bar Search Fix

### Tasks
1. Created network-diagram.html — 5-tab architecture diagram for all devices on Route10 + TRENDnet
2. Designed IoT security segmentation: Bitdefender BOX as inline IoT gateway on Route10 Port 4
3. Configured Route10 IoT zone isolation via SSH/UCI
4. Fixed YAMIL Browser address bar to support search queries (like Chrome/Edge)
5. Updated 136-NetworkUpgrade-OmadaSDN.md with security architecture and phases 14-24

### IoT Zone Configuration (SSH/UCI on Route10)
**Method**: SSH to root@192.168.0.1 + UCI commands (OpenWrt has no REST API)

**Changes applied:**
1. Removed eth2 from br-lan bridge: `uci set network.@device[1].ports="eth1 eth0 eth5"`
2. Created iot interface on eth2: 192.168.2.1/24 static
3. Created iot firewall zone: input=ACCEPT, output=ACCEPT, forward=REJECT
4. Added forwarding rules: iot→wan ACCEPT, lan→iot ACCEPT (iot→lan blocked by zone default)
5. Enabled DHCP on iot interface: 192.168.2.10-250, 1hr lease

**Port mapping after changes:**
| Route10 Port | Interface | Bridge | Subnet | Device |
|-------------|-----------|--------|--------|--------|
| W1 | eth3 | — | WAN | CR1000A |
| L1 (Port 2) | eth0 | br-lan | 192.168.0.0/24 | TRENDnet switch |
| L2 (Port 3) | eth1 | br-lan | 192.168.0.0/24 | MacBook Air |
| L3 (Port 4) | eth2 | **standalone** | **192.168.2.0/24** | Bitdefender BOX (IoT) |

**Firewall result:**
- IoT → Internet: ACCEPT
- IoT → LAN: REJECT (isolated)
- LAN → IoT: ACCEPT (can manage IoT from trusted side)

### Address Bar Search Fix
- **Problem**: Typing search queries like "weather" in address bar prepended "https://" instead of searching
- **Fix**: Modified Enter key handler in renderer.js (~line 814) to detect search queries (has spaces or no dots) and call `navigateToSearch()` instead
- Also fixed `resolveUrl()` helper to properly detect search vs URL input

### Files Created/Modified
- `network-diagram.html` (NEW) — 5-tab network architecture diagram
- `136-NetworkUpgrade-OmadaSDN.md` — Updated security zones and phases 14-24
- `electron-app/renderer/renderer.js` — Address bar search fix
- Commit cc3b975 pushed to main

### IoT Isolation Attempt — Reverted
- Created IoT zone on eth2 (192.168.2.0/24), removed eth2 from br-lan
- Plugged Bitdefender BOX into Route10 Port 4
- **Problem:** CLOAK (Aruba switch) was on TRENDnet Port 8 AND connected to something on the Bitdefender/IoT side, creating a L2 bridge between TRENDnet and the IoT subnet
- **Result:** All TRENDnet devices (Windows PC, QNAP, Kain, brainiac7) picked up 192.168.2.x instead of 192.168.0.x. TRENDnet management (192.168.0.200) became unreachable.
- Also discovered eero mesh bridge issue: eero Pro 6E on TRENDnet meshing with IoT eero behind Bitdefender creates another L2 bridge loop
- **Fix:** Reverted IoT isolation (eth2 back in br-lan), devices recovered to 192.168.0.x
- Added static DHCP reservation for Windows PC (Dark-Knight) at 192.168.0.11

### Lessons Learned
1. **CLOAK cannot bridge two subnets** — if on TRENDnet, it cannot also connect to IoT side
2. **eero mesh bridges L2** — two eeros on different subnets will bridge them together, defeating isolation
3. **Test IoT isolation with minimal devices first** — before connecting the full chain
4. **Bitdefender runs its own DHCP/NAT** on 192.168.7.x internally

### Next Steps
- Re-plan IoT physical topology: CLOAK on ONE side only, eeros not meshing across subnets
- Phase 14: Install 10G SFP+ DAC cable (arriving Monday 2026-03-17)
- Phase 15+: Move trusted devices to TRENDnet, connect eero Pro 6E to Port 7

---

## 2026-03-16 — Route10 Troubleshooting, Factory Reset, Full Configuration via SSH

### Task
Fix slow internet on Alta Labs Route10, identify root cause, factory reset to stable firmware, and fully configure via SSH/CLI (DHCP reservations, DNS, WireGuard VPN, firewall).

### Problem
After cloud re-adoption, Route10 speeds dropped to near-zero (~28 kb/s). Dashboard showed minimal traffic even with devices connected.

### Root Cause
1. **Subnet conflict**: Route10 LAN was 192.168.1.0/24, same as upstream CR1000A — Route10 couldn't distinguish WAN from LAN traffic
2. **Cloud re-adoption**: manage.alta.inc pushed old site config (wrong subnet + phantom WAN2)
3. **Firmware 1.4 bugs**: Hardware acceleration broken in 1.4 series (30-50% throughput drop, DPI regression, UDP disabled)

### Solution
1. Factory reset Route10 — reverted to firmware **1.3z** (stable, pre-bug)
2. Selected "Setup Router" (local mode, NOT "Connect to Controller")
3. Set LAN to **192.168.0.1/24** during initial setup wizard
4. Connected to manage.alta.inc cloud AFTER local setup
5. Immediately disabled automatic firmware updates (pin at 1.3z)
6. Added Mac's SSH key via cloud dashboard
7. Configured everything via SSH/UCI:

### Configuration Applied (via SSH as root@192.168.0.1)

**DHCP Reservations:**
| Device | MAC | IP |
|--------|-----|-----|
| DARK-KNIGHT (Windows PC) | 04:d9:f5:81:10:74 | 192.168.0.11 |
| MacBook Air | 00:e0:4c:b3:3d:fe | 192.168.0.10 |
| FridayAI (QNAP) | 24:5e:be:00:e3:fb | 192.168.0.102 |
| brainiac7 (Synology) | 00:11:32:2d:3c:3c | 192.168.0.103 |

**DNS**: 1.1.1.1, 8.8.8.8 (via DHCP option)

**WireGuard VPN:**
- Interface: wg0, subnet 10.0.0.0/24, port 51820
- Server: 10.0.0.1 (Route10)
- Client peer: yamil-mobile (10.0.0.2)
- Firewall zones: vpn→lan, vpn→wan, lan→vpn
- UDP 51820 allowed from WAN

**Firewall**: WAN DROP, LAN ACCEPT, NAT/Masquerade ON

**Hostname**: Changed from "Memobytes" to "Route10"

### Speed Test Results
- Download: **1561.92 Mbps**
- Upload: **2313.75 Mbps**
- Through double NAT (CR1000A → Route10)

### Health Check (All Passed)
- Internet: 5.2ms ping to 1.1.1.1
- DNS: resolving correctly
- WAN: connected at 192.168.1.227
- LAN: 192.168.0.1/24
- WireGuard: listening on port 51820
- Hardware acceleration: QCA NSS + ECM active
- System load: 0.04

### AWS Secrets Created
- `yamil/homelab/route10-ssh` — SSH access details (root, ed25519 key, firmware 1.3z)
- `yamil/homelab/route10-wireguard` — All WireGuard keys + config

### Key Decisions
- **Stay on 1.3z**: All 1.4 firmware versions have confirmed bugs. Auto-update OFF.
- **SSH over cloud UI**: Cloud management caused subnet conflicts and phantom WANs. SSH/UCI is reliable.
- **Zyxel removed**: XMG1915-10EP locked in Nebula cloud mode, web UI unreachable. Replaced by TRENDnet TEG-30284 (arriving Tuesday).
- **Flat subnet for now**: Single 192.168.0.0/24 instead of multi-VLAN. VLANs can be added later via TRENDnet switch.

### Completed This Session (Continued)

**Port Forward**: Created UDP 51820 → 192.168.1.227 on CR1000A for external WireGuard VPN access.

**AWS Secret Updated**: `yamil/homelab/windows-pc` host changed from 192.168.0.101 → 192.168.0.11.

**WireGuard Mobile Config**: Added full mobile client config (Interface + Peer) to 136-NetworkUpgrade doc.

**Device Port Identification (Route10)**:
| Port | Interface | Device | Speed |
|------|-----------|--------|-------|
| W1 | eth3 | CR1000A (WAN) | 2500 Mb/s |
| L1 | eth0 | Aruba CLOAK switch | 2500 Mb/s |
| L2 | eth1 | DARK-KNIGHT / Windows PC | 2500 Mb/s |
| L3 | eth2 | MacBook Air | 1000 Mb/s |
| W2/SFP+ | eth4/5 | Empty | — |

**Prometheus Fix**: All 14 logic-weaver-services targets + 2 blackbox targets had old IP 192.168.0.101. Updated to 192.168.0.11, SCP'd to QNAP, hot-reloaded. Result: 28/28 targets UP, alerts dropped from 17 → 0.

**api.yamil-ai.com Fix**: Root cause chain: `logic-weaver-etcd-1` crashed (Exit 255) → `logic-weaver-apisix-1` couldn't connect to etcd → `envoy-external` had no upstream → 503. Fixed by starting etcd → APISIX → re-running init routes. All 5 external routes loaded. Blackbox probe now passing (`probe_success 1`).

**SFP+ DAC Cable**: Ordered 10Gtek 0.5m SFP+ DAC ($9.99 Amazon) for Route10 ↔ TRENDnet 10G backbone.

### Next Steps
- [ ] TRENDnet TEG-30284 arrives Tuesday — connect to Route10 L1, 10G SFP+ DAC backbone
- [ ] Move all devices to TRENDnet switch (QNAP, Synology, Windows PC, MacBook, Aruba)
- [ ] Configure VLANs on TRENDnet if needed
- [ ] CR1000A bridge mode (do last, after all devices migrated)
- [ ] DDNS setup for stable WireGuard endpoint (public IP may change)
- [ ] 7 monitoring connectors not yet created (prometheus, loki, alertmanager, blackbox, ntfy, uptime_kuma, grafana)
- [ ] Investigate why etcd + APISIX crashed after 12 hours — may need restart policy

---

## 2026-03-16 — Alta Labs Route10 Firmware Bug Research: Slow LAN Throughput

### Task
Comprehensive web research on Alta Labs Route10 firmware bugs causing slow LAN throughput, specifically after cloud re-adoption pushing conflicting subnet config (192.168.1.0/24 matching upstream CR1000A).

### Problem Statement
After factory reset, Route10 works perfectly with fast speeds. After re-adopting to Alta Labs cloud (manage.alta.inc), speeds drop to near-zero. Cloud pushes LAN subnet to 192.168.1.0/24, conflicting with upstream CR1000A router on same subnet. Even after manually fixing subnet to 192.168.0.0/24 via cloud dashboard, traffic drops to 0.

### Key Findings

#### 1. Hardware Acceleration Bug (CONFIRMED)
- With hardware acceleration enabled, download speeds DROP 30-50% (confirmed by multiple users)
- CPU usage INCREASES from 32-33% to 52-53% when acceleration is on (opposite of expected)
- UDP acceleration was intentionally disabled in firmware 1.4l because "BitTorrents waste too many flow slots"
- Three modes: Enabled, Alternate, Disabled
- CLI to re-enable UDP acceleration: `echo 4 >/cfg/alta_bits` then reboot
- CLI to disable: `echo 0 >/cfg/alta_bits` then reboot
- Flow control commands: `ssdk_sh flow status set 0` and `ssdk_sh port flowCtrl set [port] disable`
- Port mapping: WAN1=port 4, WAN2=port 5, LAN ports 1-4
- Disabling flow control on SFP+ WAN port resolved throughput bottleneck for one user

#### 2. Cloud Re-Adoption Config Conflict (CONFIRMED)
- After factory reset, cloud controller automatically restores previous configuration
- Old WAN/subnet settings cause immediate connectivity loss
- Device shows red light on Alta logo when old config conflicts with new network
- WORKAROUND: Delete device entirely from cloud site before re-adopting. Also delete all WAN profiles. Alternatively, add to a NEW site (not existing site)
- Subnet change from cloud UI doesn't always stick (reported by multiple users with mobile app)

#### 3. Latest Firmware: 1.4v (March 6, 2026)
- Only fixes firewall group stability regression from 1.4u
- NO fix for NAT throughput, hardware acceleration, or cloud config push issues
- Full recent history: 1.4v (Mar 6), 1.4u (Mar 5), 1.4t (Feb 24), 1.4s (Feb 4), 1.4r (Jan 29), 1.4q (Jan 28)
- 1.4o added "NAT disabling option for WAN connections" - may be useful

#### 4. Local Management Options
- **Self-hosted controller (Docker)**: Free at manage.alta.inc/control. Run locally, no cloud needed. Requires Docker knowledge. Must `apt update && apt upgrade` inside container for Route10 support.
- **SSH access**: Root via SSH key only (no password). Add key via Settings > System in controller. User is root.
- **Underlying OS**: OpenWrt-derived, supports UCI (Unified Configuration Interface) commands
- **post-cfg.sh**: Script at `/cfg/post-cfg.sh` runs AFTER cloud config pushes, survives reboots. Can override cloud-pushed settings. Must be LF line endings, `chmod +x`.
- **No standalone mode**: Route10 REQUIRES a controller (cloud, local hardware, or self-hosted Docker) for initial provisioning. No true standalone/offline setup exists.
- **No bridge mode**: Not planned as a feature. DMZ via port forwarding is the official workaround.

#### 5. Workarounds for Subnet Conflict
- Route10 has built-in conflict detection: if upstream is 192.168.1.x, it auto-switches to 192.168.0.1
- Can set subnet via cloud UI: Network > Route10 > edit VLAN1
- Via SSH/UCI: `uci set` commands for network config, `uci commit`, `/etc/init.d/network restart`
- Via post-cfg.sh: Persistent config that re-applies after every cloud push
- 1.3z firmware added LAN subnet selection to initial setup wizard

#### 6. Preventing Cloud Config Override
- `/cfg/post-cfg.sh` is the PRIMARY method - runs after every cloud config push
- Can set network config, firewall rules, DHCP, DNS, WireGuard, QoS
- Modular approach: `/cfg/post-cfg.d/` directory for subscripts
- Example patterns: check current config before applying (`uci get` + conditional)
- UCI commands persist via `uci commit`

### Recommended Action Plan
1. Factory reset Route10
2. Delete device + all WAN profiles from manage.alta.inc cloud site
3. Set up self-hosted Docker controller on QNAP or Mac
4. Adopt Route10 to local controller (not cloud)
5. Set LAN subnet to 192.168.0.0/24 during initial setup
6. Disable hardware acceleration OR use `echo 4 >/cfg/alta_bits` for UDP fix
7. Create `/cfg/post-cfg.sh` to persist subnet + acceleration settings after any config push
8. If using cloud: create new site, adopt as new device (not re-adopt to existing site)

### Alternative: Nuclear Option
- Factory reset, do NOT re-adopt to any controller
- SSH in, configure everything via UCI commands
- Use post-cfg.sh to persist
- Limitation: no web UI for management, CLI only

---

## 2026-03-15 — 136: Network Upgrade Phase 6 — Route10 Port VLAN Assignments

### Task
- Assign VLANs to Route10 ports via Alta Labs dashboard (manage.alta.inc)
- Create connection profiles for Main (VLAN 10) and IoT (VLAN 20)
- Configure per-port Native VLAN and Allowed VLANs

### Steps Taken
1. Created connection profiles in Settings > Networks > Profiles:
   - **Main** profile: VLAN 10, Standard type
   - **IoT** profile: VLAN 20, Standard type
2. Navigated to Network page, opened Memobytes device panel (click device image)
3. Opened L3 (Bitdefender port) per-port config by clicking `.ports-col:nth-child(4) .port-content`
4. Set L3 Native VLAN to 20 (IoT) via Custom input — saved successfully
5. Verified L3 shows "T" (Tagged) on VLAN 1 and "U" (Untagged) on VLAN 20
6. Opened L1 (Zyxel switch uplink) per-port config — verified already correct:
   - Native VLAN: Default (VLAN 1)
   - Allowed VLANs: Default (All — 1, 10, 20 checked)
7. Verified all three VLAN views show correct U/T tagging

### Route10 Port Summary

| Port | Device | Native VLAN | VLAN 1 | VLAN 10 | VLAN 20 |
|------|--------|-------------|--------|---------|---------|
| W1 | CR1000A (WAN) | 1 | U | T | T |
| L1 | Zyxel switch | 1 (Mgmt) | U | T | T |
| L2 | spare | 1 | - | - | - |
| L3 | Bitdefender | 20 (IoT) | T | T | U |
| L4 | spare | 1 | - | - | - |
| W2 | SFP+ (empty) | 1 | - | - | - |

### Key Learnings
- Per-port config opens by clicking `.port-content` inside `.ports-col`
- Native VLAN "Custom" input found via a11y_snapshot — NOT the search bar
- Alta Labs has NO public REST API — all config through UI only
- "Default" for Allowed VLANs = all VLANs allowed

### Phase 9 — Firewall / VLAN Isolation
- VLAN 20 (IoT) already had **Isolation** enabled from VLAN creation
- IoT devices (e.g., Bitdefender at 192.168.20.10) can reach internet but NOT VLAN 1 or VLAN 10
- No additional firewall rules needed for basic inter-VLAN isolation

### Phase 7 — Zyxel XMG1915-10EP — BLOCKED
- Switch visible on Alta Labs Devices page: **XMG1915** at 192.168.1.19, MAC 7049a26e82d4, Link L1
- MacBook (192.168.1.17, en7) is on same 192.168.1.0/24 subnet but **cannot reach** the switch
- ARP entry for 192.168.1.19 is "(incomplete)" — no L2 response
- Ports 80, 443, 22 all unreachable (nc connection refused or timeout)
- Possible causes: Nebula cloud mode (disables local web UI), management VLAN restriction, or L2 segmentation
- **Zyxel Nebula portal** (nebula.zyxel.com) is available but needs Zyxel account credentials

### VPN — Explored (Not Configured)
- WireGuard Server available on Route10 VPN tab
- DDNS hostname already assigned: `4h5jm0towhl.ddns...`
- Needs: private key, VPN subnet (e.g., 192.168.5.0/24), DNS servers, client configs
- IPsec Server also available as alternative

### Next Steps
- **Phase 7**: Need physical access to Zyxel switch or Nebula cloud credentials to configure VLANs
  - Port 1 (Route10 uplink): trunk, all VLANs (1, 10, 20)
  - Ports 2-8: access on VLAN 10 (Main)
- **Phase 8**: Move devices to Zyxel switch (after Phase 7 is complete)
- **Phase 10**: Set up WireGuard VPN — needs user decisions on subnet, clients, access policies
- **Phase 13**: Put CR1000A in bridge mode — do this LAST after all devices migrated

---

## 2026-03-15 — 141: Connect AI Sidebar to AssemblyLine Chat-Service

### Task
- Deploy chat-service and rag-service as standalone Docker microservices
- Power YAMIL Browser AI sidebar with multi-provider LLM routing
- Expose agentic RAG API for MemoBytes, Drive-Sentinel, YAMIL apps

### Architecture
- **Hybrid**: Electron app runs native on desktop, AI services run in Docker
- **chat-service** (:8020) — LLM routing via assemblyline-common, voice I/O, streaming SSE
- **rag-service** (:8022) — agentic RAG, vector search, knowledge graph (pgvector)
- **Shared Postgres** (pgvector:pg17) + **Redis** for all services
- **Default LLM**: Ollama (local), with cloud fallback (OpenAI, Claude, Gemini, Grok)

### Changes Made
1. **Created `chat-service/`** — copied from AssemblyLine, adapted for standalone
   - `browser_chat.py` — direct LLMRouter endpoint (no orchestrator needed)
   - `app/main.py` — added CORS, SKIP_AUTH mode, mounted browser_chat router
   - `docker-compose.yml`, `.env`, `.env.example` — Ollama host access, port offsets
2. **Copied `rag-service/`** — full agentic RAG from AssemblyLine (118+ endpoints)
3. **Updated root `docker-compose.yml`** — unified stack with shared ai-db and redis
   - browser-service :4000, chat-service :8020, rag-service :8022
   - Single Postgres instance with per-service databases
4. **Created `init-databases.sql`** — creates yamil_chat, yamil_rag databases + pgvector
5. **Updated `electron-app/preload.js`** — default AI_ENDPOINT to :8020/browser-chat
6. **Updated `renderer.js`**:
   - sendChat() now uses streaming (SSE) by default
   - Server TTS via /voice/synthesize (fallback to browser speechSynthesis)
   - Push-to-talk voice input via MediaRecorder + /voice/transcribe
   - LLM status indicator (polls /llm/status every 30s)
7. **Updated `index.html`** — voice button + LLM status dot
8. **Updated `styles.css`** — voice button, recording animation, status indicator
9. **Created `setup-all.sh`** — bundles assemblyline-common into both services
10. **Created `start-with-ai.sh`** — starts Docker + waits for health + launches Electron

### Key Decisions
- Agentic RAG writes to its own Postgres DB — apps pull via API, not direct DB access
- No JWT auth for local desktop use (SKIP_AUTH=true)
- assemblyline-common bundled at build time into each container (setup.sh)
- Cloud API keys optional — add to .env for fallback providers

### Fixes Applied During Deployment
1. **`setup_observability()` TypeError** — bundled assemblyline_common had different function signature than what chat-service/rag-service expected. Fixed by removing `setup_observability()` calls, using `get_logger()` directly.
2. **Ollama model not found (404)** — default model `llama3.1:8b` not installed locally. Added `OLLAMA_MODEL=gemma3:4b` to `chat-service/.env`.
3. **`file://` URLs broken in YAMIL Browser** — regex in `renderer.js` only allowed `http/https`, prepending `https://` to `file://` URLs. Fixed regex to `/^(https?|file):\/\//` in both occurrences (lines 795 and 1783).

### Verification Results
| Test | Status |
|------|--------|
| `curl http://localhost:8020/health` | Pass — `{"status":"healthy"}` |
| `curl http://localhost:8022/health` | Pass — `{"status":"degraded"}` (DB not initialized, but running) |
| `curl http://localhost:8020/llm/status` | Pass — Ollama healthy, providers configured |
| Non-streaming browser-chat | Pass — gemma3:4b responds in ~2s, $0 cost |
| Streaming browser-chat (SSE) | Pass — tokens stream correctly |
| LLM status indicator in sidebar | Pass — green dot, `title="AI connected: ollama"` |
| Chat via sidebar | Pass — sent "Hello YAMIL, who are you?" → got contextual response about the page |
| Voice status `/voice/status` | Pass — STT available (lazy), TTS loaded (af_heart) |
| TTS synthesis `/voice/synthesize` | Pass — generated 16KB audio file |
| Docker containers stable | Pass — no crash loops |

### How to Change LLM
1. **Change Ollama model**: Edit `chat-service/.env` → `OLLAMA_MODEL=qwen3:8b` → `docker compose restart chat-service`
2. **Add cloud providers**: Add API keys to `chat-service/.env` (OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, XAI_API_KEY)
3. **Per-request**: POST to `/browser-chat` with `{"provider":"openai","model":"gpt-4o"}`
4. **Fallback chain**: `LLM_FALLBACK_CHAIN=ollama,gemini,openai,anthropic` in `.env`

### Next Steps
1. Add provider/model selector dropdown to sidebar UI
2. Initialize RAG database (currently degraded)
3. Test voice input (push-to-talk) in the actual browser
4. Push Docker containers to Windows PC (192.168.0.101)

---

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

---

## 2026-03-15: Chrome Rendering Feature Flags Research for YAMIL Browser

### Task
Research what Chrome rendering features and command-line flags production Chrome uses that the Electron YAMIL Browser app is missing. Provide a comprehensive list covering font rendering, CSS/layout, GPU/hardware acceleration, media codecs, web platform features, V8 performance, and FOUC prevention.

### Context
- Electron 33 = Chromium 130
- Current flags: `SharedArrayBuffer`, `enable-gpu-rasterization`, `enable-zero-copy`, `ignore-gpu-blocklist`
- Current webview: `contextIsolation=yes, sandbox=no`, Chrome 131 UA string
- macOS (darwin) is the primary platform

### Research Approach
1. Searched Electron docs for supported command-line switches
2. Searched Chromium source for `--enable-features` flag names and defaults
3. Checked Peter Beverloo's Chromium switches database
4. Investigated font rendering issues specific to Electron on macOS
5. Researched FOUC prevention (show:false + ready-to-show pattern)
6. Verified feature availability against Chromium 130 (Electron 33's engine)

### Key Findings
See the comprehensive list in the response. Organized into 7 categories with specific `app.commandLine.appendSwitch()` calls, BrowserWindow config changes, and webview FOUC prevention techniques.

---

## 2026-03-15: WebContentsView Migration Research

### Task
Research Electron's `WebContentsView` API as a replacement for `<webview>` tags in the YAMIL Browser app. Cover API surface, migration patterns, open-source browser examples, overlay UI handling, tab switching, resize, screenshots, JS execution, navigation events, and version compatibility.

### Context
- YAMIL Browser currently uses `<webview>` tags inside a BrowserWindow renderer process
- Electron version: ^33.0.0 (Chromium 130)
- ~86 references to `webview` across 4 files (main.js, renderer.js, index.html, styles.css)
- The `<webview>` tag is officially deprecated in Electron docs in favor of WebContentsView

### Key Findings
1. **WebContentsView** introduced in Electron v30 as stable API, replaces both `<webview>` and deprecated `BrowserView`
2. Uses **BaseWindow** (not BrowserWindow) as container — BaseWindow has no renderer of its own
3. The **View** base class provides: `setBounds()`, `getBounds()`, `setVisible()`, `addChildView()`, `removeChildView()`, `setBackgroundColor()`, `setBorderRadius()`, `children` property
4. **No auto-resize** — must listen to window `resize` event and manually update bounds
5. **No built-in z-index** — child view ordering controlled by `addChildView(view, index)` parameter; calling `addChildView()` on existing child reorders to top
6. **Tab switching**: use `setVisible(false)` to hide inactive tabs, `setVisible(true)` + bring to top for active
7. **Screenshots**: `view.webContents.capturePage([rect])` returns Promise<NativeImage>
8. **JS execution**: `view.webContents.executeJavaScript(code)` returns Promise with result
9. **Navigation events**: `did-navigate`, `did-start-navigation`, `will-navigate`, `page-title-updated`, `page-favicon-updated` all on `view.webContents`
10. **Memory management critical**: When BaseWindow closes, webContents are NOT auto-destroyed — must call `view.webContents.close()` manually in `closed` event
11. **Min Browser** (minbrowser/min) uses one BrowserView per tab with IPC communication pattern
12. **Overlay UI** (autofill bar, context menus): must be a separate WebContentsView layered on top with higher z-order

### Architecture Decision: BaseWindow vs BrowserWindow
- **BaseWindow + WebContentsView**: Recommended path. UI toolbar is a WebContentsView, each tab is a WebContentsView. All views are siblings under `win.contentView`.
- **BrowserWindow + WebContentsView**: Also works but BrowserWindow's own webContents conflicts with the view hierarchy. Possible but not recommended.

### Migration Complexity for YAMIL Browser
- HIGH: 86 webview references across 4 files
- Renderer process (renderer.js) creates/manages webview DOM elements — all of this moves to main process
- IPC bridge between renderer and main process needs redesign
- HTTP control server (port 9300) endpoints that eval in webview need to use `webContents.executeJavaScript()` instead
- Screenshot pipeline changes from webview.capturePage to view.webContents.capturePage

### Sources
- Electron official: WebContentsView API, View API, webContents API, Migration blog post
- Mamezou-tech: WebContentsView implementation guide, App structure visualization
- Ika.im: Building a Browser in Electron (Yoga layout, React portals for overlay UI)
- GitHub: mamezou-tech/electron-example-browserview, minbrowser/min Architecture wiki

## 2026-03-15 — WebContentsView Migration Implementation

### Task
Implement the full migration from `<webview>` to `WebContentsView` as planned in `140-WebContentsView-Migration.md`.

### Changes Made

#### `electron-app/main.js` (Complete rewrite)
- Replaced `BrowserWindow` with `BaseWindow` + `WebContentsView`
- Created `TabManager` class managing WebContentsView tabs directly
- All HTTP control server endpoints now access `tabManager.getActiveView().webContents` directly
- Screenshots use `view.webContents.capturePage()` (no more IPC chain)
- Native context menus via `Menu.popup()`
- Credential watcher injection from main process
- Layout management with `layoutViews()` on resize/sidebar toggle

#### `electron-app/preload.js` (Extended)
- Added `window.yamil` IPC bridge namespace with 20+ methods:
  - Tab lifecycle: `createTab`, `switchTab`, `closeTab`
  - Navigation: `navigate`, `goBack`, `goForward`, `reload`
  - Page interaction: `eval`, `zoom`, `find`, `stopFind`, `print`, `devtools`
  - Queries: `getInfo`, `list`, `getUrl`
  - Actions: `savePageAs`, `copyUrl`, `viewSource`
  - Layout: `sidebarToggled`, `bookmarkBarToggled`
  - Events: `onTabEvent` listener for main→toolbar events

#### `electron-app/renderer/renderer.js` (30+ surgical edits)
- `createTab()`: Calls `window.yamil.createTab()` instead of creating `<webview>` DOM elements
- `switchTab()`: Calls `window.yamil.switchTab()` to swap native views
- `closeTab()`: Calls `window.yamil.closeTab()` instead of `webview.remove()`
- Removed `wireWebviewEvents()` (~200 lines) — replaced with `window.yamil.onTabEvent()` listener
- Removed `showContextMenu()` (~100 lines) — now native in main.js
- All `tab.webview.executeJavaScript()` → `window.yamil.eval()`
- All `tab.webview.loadURL()` → `window.yamil.navigate()`
- All `tab.webview.goBack/goForward/reload` → `window.yamil.goBack/goForward/reload()`
- All `tab.webview.findInPage/stopFindInPage` → `window.yamil.find/stopFind()`
- All `tab.webview.setZoomLevel()` → `window.yamil.zoom()`
- Autofill now triggered by main process `check-autofill` event
- `setSidebarOpen()` notifies main via `window.yamil.sidebarToggled()`
- `setBookmarkBarVisible()` notifies main via `window.yamil.bookmarkBarToggled()`
- Stealth tab code (canvas, WebSocket screencast) unchanged

#### `electron-app/renderer/index.html`
- `#webview-container` comment updated (now transparent viewport)
- Removed `#context-menu` div (native menus)

#### `electron-app/renderer/styles.css`
- `#webview-container`: Made transparent with `pointer-events: none` pass-through
- Removed webview CSS rules, kept stealth-canvas rules
- Removed `#context-menu` styles (~50 lines)

### What's Preserved
- Stealth tabs (canvas + browser-service) — fully unchanged
- Sidebar chat UI, bookmarks, history, settings, downloads — all in toolbar DOM
- MCP server — still calls port 9300 HTTP endpoints (no changes needed)
- Tab bar drag-reorder, tab groups, pinning — all in toolbar DOM

### Bug Fix: Missing `/console-logs` endpoint
- The MCP tool `yamil_browser_console_logs` calls `GET /console-logs` on port 9300
- This endpoint was missing from the new main.js — returned 404
- **Fix**: Added `consoleLogs` circular buffer (max 500) to TabManager, wired `console-message` webContents event, added `/console-logs` HTTP endpoint with `level`, `last`, `clear` query params
- Verified: all three console levels (info, warning, error) captured and returned correctly

### Verification Results (All Passed)
| Test | Status |
|------|--------|
| Window opens with tab bar, navbar, status bar | Pass |
| Pages render in native WebContentsView (no clipping) | Pass |
| Tab switching between multiple tabs | Pass |
| Navigation (forward/back/URL bar) | Pass |
| `yamil_browser_screenshot` | Pass |
| `yamil_browser_navigate` | Pass |
| `yamil_browser_dom` | Pass |
| `yamil_browser_eval` | Pass |
| `yamil_browser_click` | Pass |
| `yamil_browser_fill` | Pass |
| `yamil_browser_go_back` | Pass |
| `yamil_browser_list_tabs` | Pass |
| `yamil_browser_switch_tab` | Pass |
| `yamil_browser_scroll` | Pass |
| `yamil_browser_console_logs` | Pass (fixed) |
| Layout resize (sidebar closed = full-width) | Pass |

### Remaining Tests (not yet verified)
- Self-signed cert handling (QNAP at 192.168.0.102)
- Autofill on a login page
- Find-in-page UI (Cmd+F from toolbar)

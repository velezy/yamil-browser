# 137 — Server Infrastructure: Production, Dev & HA

**Status**: Implemented
**Created**: 2026-03-12
**Updated**: 2026-03-20
**Philosophy**: Dark-Knight runs production, GEEKOM and Mac Mini are HA secondaries, QNAP monitors everything. Consul orchestrates failover. Cloudflare Tunnels handle external access.

---

## 1. The Problem (Solved)

Everything used to run on one machine (Dark-Knight) with test config only. No separation between dev and prod, no failover, no monitoring. If Dark-Knight went down, everything went offline.

**What we built**: A 5-node homelab with centralized monitoring on QNAP, Consul-based HA failover, automated backups, and Cloudflare Tunnel for external access — no cloud compute needed.

## 2. Server Roles

| Server | Role | IP | Specs | OS |
|--------|------|----|-------|----|
| **Dark-Knight** (Windows PC) | Production | 192.168.0.101 | Ryzen 9 3900X, 64GB DDR4, 1TB SSD | Windows 11 + Docker Desktop |
| **GEEKOM A8 Max** | HA Secondary #1 | 192.168.0.113 | Ryzen 9 8945HS, 32GB DDR5, 1TB SSD, dual 2.5G LAN | Windows 11 + Docker Desktop |
| **Mac Mini M4** (Kain) | HA Secondary #2 | 192.168.0.119 | M4, 16GB, 256GB + 2TB Samsung 990 EVO (USB4) | macOS |
| **MacBook Air M3** | Development | 192.168.0.120 | M3, 24GB | macOS |
| **QNAP NAS** (FridayAI) | Monitoring / Infrastructure Hub | 192.168.0.102 | Intel Celeron 4C, 8GB, TS-251+ | QTS + Container Station |

### Why This Layout

- **Dark-Knight as Production**: 64GB RAM, Ryzen 9 — handles Postgres, Redis, Ollama, all YAMIL services, Cloudflare Tunnel
- **GEEKOM as HA #1**: 32GB DDR5, dual 2.5G LAN, all 3 project Docker images pre-built, ready for instant failover
- **Mac Mini as HA #2**: M4, Docker on external SSD, all repos cloned, Ollama running — second failover option
- **MacBook Air as Dev**: Primary development machine, runs YAMIL Browser Electron app, Claude Code sessions
- **QNAP as Observer**: Always-on, independent from what it monitors — runs Prometheus, Grafana, Loki, backups, Consul server

## 3. What Runs Where

### Production (Dark-Knight — 192.168.0.101)
- YAMIL Browser + all LogicWeaver services (ports 8001-8016)
- PostgreSQL + pgvector (port 5432)
- Redis (port 6379)
- Ollama (port 11434)
- Cloudflare Tunnel (`yamil-local`) — routes yamil-ai.com, api.yamil-ai.com
- Consul client — registers: yamil-frontend, docker-engine, ollama, postgres, redis
- Bitdefender with QNAP (192.168.0.102) whitelisted

### HA Secondary #1 (GEEKOM — 192.168.0.113)
- All 3 project Docker images pre-built (YAMIL, FlashCards, ai-bot)
- Ollama v0.18.2 (4 models)
- Docker Desktop with metrics on :9323, auto-start enabled
- Windows Exporter on :9182
- Portainer on :9443
- Consul client — registers: docker-engine, ollama, windows-exporter
- Sleep mode disabled (always-on standby)
- SSH: `ssh geekom` (yvele user, `~/.ssh/geekom_ha` key)

### HA Secondary #2 (Mac Mini — 192.168.0.119)
- All repos cloned to external SSD (`/Volumes/External/project/`)
- Docker Desktop with socket fix via LaunchDaemon
- Docker.raw on external SSD (`/Volumes/External/docker-data/`)
- Ollama running (5 models)
- node_exporter on :9100
- Portainer on :9443
- Consul client — registers: docker-engine, ollama, node-exporter
- SSH: `ssh macmini` (yaml user, `~/.ssh/macmini_ha` key)

### Development (MacBook Air — 192.168.0.120)
- YAMIL Browser Electron app (control API on :9300)
- Claude Code sessions
- Docker Desktop (local development)
- Consul client — registers: yamil-browser

### Infrastructure Hub (QNAP — 192.168.0.102)
- **Monitoring**: Prometheus (:9090), Grafana (:3000), Loki (:3100), Blackbox Exporter (:9115), Alertmanager (:9093), Uptime Kuma (:3001)
- **Notifications**: ntfy (:8090) — push alerts to phone
- **Backups**: db-backup (pg_dump every 6h), git-mirror (5 repos hourly)
- **Infrastructure**: Consul server (:8500), Cloudflare Tunnel (Grafana + ntfy), Watchtower (daily auto-update)
- **External access**: grafana.yamil-ai.com, ntfy.yamil-ai.com via Cloudflare Tunnel
- 13 containers total, ~2.5GB RAM, leaves ~5GB for QTS

## 4. Deployment Flow

```
MacBook (Dev) ──git push──> GitHub ──git mirror──> QNAP (hourly bare clones)
                                    ──git pull──> Dark-Knight (manual deploy)
                                    ──git pull──> GEEKOM (manual deploy)
                                    ──git pull──> Mac Mini (manual deploy)
```

### Git-Based Workflow
1. Develop & test on MacBook Air
2. `git push` to GitHub (main branch)
3. QNAP git-mirror fetches bare clones hourly (5 repos)
4. On Dark-Knight: `git pull && docker compose up -d --build`
5. On GEEKOM/Mac Mini: `git pull && docker compose up -d --build`
6. (Future) Webhook auto-deploy on push

## 5. External Access (Cloudflare Tunnels)

No Nginx/Certbot needed — Cloudflare handles TLS and routing.

### yamil-local Tunnel (`47748839-...`) — on Dark-Knight
| Domain | Routes to |
|--------|-----------|
| `yamil-ai.com` | `http://localhost:9080` |
| `www.yamil-ai.com` | `http://localhost:9080` |
| `api.yamil-ai.com` | `http://localhost:9082` |
| `ntfy.yamil-ai.com` | `http://192.168.0.102:8090` (backup) |

### QNAP Tunnel (`31e10393-...`) — on QNAP
| Domain | Routes to |
|--------|-----------|
| `grafana.yamil-ai.com` | `http://grafana:3000` |
| `ntfy.yamil-ai.com` | `http://ntfy:80` |

## 6. HA Strategy (Consul-Based)

### Consul Cluster (5/5 nodes alive)
- **Server**: QNAP (192.168.0.102:8500) — single server, v1.22.5
- **Clients**: MacBook, Mac Mini, GEEKOM, Windows PC — all v1.22.5
- **Services registered**: yamil-frontend, yamil-browser, docker-engine (x3), ollama (x3), windows-exporter, node-exporter, postgres, redis
- **Health checks**: 16/17 passing (docker-engine on Windows PC requires :9323 metrics)
- **DNS**: `dig @192.168.0.102 -p 8600 <service>.service.consul`
- **KV Store**: `failover/active-node`, `failover/last-failover`, `failover/previous-node`
- See `141-ConsulHA.md` for full details

### Failover Detection
- Consul watch on `yamil-frontend` service health
- When service goes critical → triggers `failover-handler.sh` on QNAP
- Failover handler updates Consul KV, sends ntfy notification
- RTO: <2 minutes

### Database Backup (not streaming replication)
- QNAP runs `pg_dump` against Dark-Knight Postgres every 6 hours
- 90-day retention with daily deduplication after 7 days
- Success/failure notifications via ntfy
- For failover: restore latest dump on secondary node

### Failback
- Once Dark-Knight is back, restore latest backup
- Consul automatically detects service recovery
- Update KV store to mark Dark-Knight as active

## 7. Network

### Hardware
- **Router**: Alta Labs Route10 (192.168.0.1) — SSH as root
- **Switch**: TRENDnet TEG-3102WS (192.168.0.200) — 10-port 2.5G managed switch
- **AP**: TP-Link Omada (managed by Omada Controller)
- All on VLAN 1 (single flat network), no port isolation, no ACLs

### IP Assignments (DHCP reservations on Omada)
| Device | IP | MAC |
|--------|----|-----|
| Dark-Knight (Windows PC) | 192.168.0.101 | — |
| QNAP FridayAI | 192.168.0.102 | — |
| GEEKOM A8 Max | 192.168.0.113 | 38:f7:cd:d5:35:86 |
| Mac Mini M4 | 192.168.0.119 | d0:11:e5:35:9a:b3 |
| MacBook Air M3 | 192.168.0.120 | — |
| TRENDnet Switch | 192.168.0.200 | — |

### Docker Contexts (from MacBook)
- `docker --context geekom ps` — ssh://geekom
- `docker --context macmini ps` — ssh://macmini
- Default: `desktop-linux` (local Docker Desktop)

## 8. Monitoring & Alerting

### Prometheus Targets (19/33 UP)
- Infrastructure services: all UP (node exporters, Docker metrics, Consul, Blackbox, Grafana, etc.)
- Logic Weaver services (14 targets on ports 8001-8016): DOWN — these services don't expose `/metrics` endpoints (expected)

### Alert Rules
| Rule | Trigger | Severity |
|------|---------|----------|
| ServiceDown | `up == 0` for 2 min | Critical |
| EndpointDown | `probe_success == 0` for 2 min | Critical |
| SSLCertExpiringSoon | cert expiry < 14 days | Warning |
| HighLatency | response > 5s for 5 min | Warning |
| HighMemoryUsage | RSS > 512MB for 5 min | Warning |

### Alert Flow
Prometheus → Alertmanager → ntfy webhook → phone push notification (`yamil-alerts` topic)

## 9. AWS Resources (Minimal)

Most infrastructure is on-premises now. AWS resources kept as safety net:

| Resource | ID | Status | Cost |
|----------|----|--------|------|
| EC2 `yamil-prod` | i-0d4a9036d9f189e6a | **Stopped** | $0 (stopped) |
| RDS `yamil-dev-v2` | db.t4g.micro, postgres | **Stopped** | ~$0.90/mo (storage) |
| Secrets Manager | 12 secrets | Active | ~$4.50/mo |
| VPC / EIP | — | Active | ~$3.95/mo |
| Lambda `rds-auto-stop` | — | Active | $0 (free tier) |

- **RDS auto-stop automation**: Lambda + EventBridge rule catches AWS 7-day auto-restart and immediately stops the instance again
- **Total AWS cost**: ~$9/mo (down from $148/mo in Feb 2026)
- Consider deleting RDS/EC2 entirely once homelab Postgres is validated long-term

## 10. Implementation Status

| Phase | What | Status |
|-------|------|--------|
| 1 | Grafana + Prometheus on QNAP | **DONE** |
| 2 | Blackbox exporter + uptime probes | **DONE** |
| 3 | ntfy for phone notifications | **DONE** |
| 4 | Alert rules in Prometheus + Alertmanager | **DONE** |
| 5 | Scheduled database backups (pg_dump every 6h) | **DONE** |
| 6 | Loki + log aggregation | **DONE** |
| 7 | Watchtower auto-updates | **DONE** |
| 8 | Uptime Kuma | **DONE** |
| 9 | Git mirror (hourly, 5 repos) | **DONE** |
| 10 | Email alerts (Gmail SMTP in Grafana) | **DONE** |
| 11 | HA machine monitoring (GEEKOM + Mac Mini + Windows PC) | **DONE** |
| 12 | Consul HA cluster — 5/5 nodes | **DONE** |
| 13 | HIPAA compliance plan | TODO |
| 14 | Full failover drill | TODO |
| 15 | Auto-deploy on git push (webhook) | TODO |

## 11. Related Documents

| Doc | Covers |
|-----|--------|
| `134-QNAPInfrastructureHub.md` | Full QNAP monitoring stack details, container inventory, configs |
| `135-InfrastructureHA.md` | HA architecture, failover scripts, machine setup phases |
| `136-NetworkUpgrade-OmadaSDN.md` | Network hardware upgrade (Alta Labs, TRENDnet) |
| `139-GEEKOM-HA-Setup.md` | GEEKOM 10-phase setup guide |
| `140-MacMini-HA-Setup.md` | Mac Mini 11-phase setup guide |
| `141-ConsulHA.md` | Consul cluster deployment, services, DNS, KV failover |
| `144-FixBitdefender-WindowsPC.md` | Bitdefender WFP fix for Windows PC ↔ QNAP connectivity |

## 12. Notes

- GEEKOM runs Windows 11 (not Ubuntu as originally planned) — Docker Desktop works fine
- Mac Mini Docker uses external SSD to save internal storage
- Mac Mini SSH needs `PermitUserEnvironment yes` for Docker context over SSH
- GEEKOM Docker builds require `schtasks /it` (DPAPI credential helper limitation over SSH)
- Bitdefender on Windows PC blocks IPs via WFP filters — must whitelist QNAP (192.168.0.102)
- All machines have SSH key auth (no passwords)
- Secrets stored in AWS Secrets Manager (source of truth)

# 132 — Infrastructure, Monitoring & Alerting

**Status**: Production
**Created**: 2026-03-07

---

## 1. Network Topology

```
                    Internet
                       |
               Cloudflare Edge
              (DDoS, TLS, WAF)
                 /           \
    yamil-local tunnel    yamil-qnap tunnel
         |                      |
   Windows Server          QNAP NAS
   192.168.0.101           192.168.0.102
   (LW + YAMIL App)       (Storage + Monitoring)
```

| Host | IP | Role | Always On |
|------|-----|------|-----------|
| Windows Server (Logic Weaver) | 192.168.0.101 | YAMIL app, all LW microservices, Grafana, Loki | Yes |
| QNAP NAS (FridayAI / TS-251+) | 192.168.0.102 | Storage, Uptime Kuma, ntfy, cloudflared | Yes |

---

## 2. Cloudflare Tunnels

### yamil-local (Windows Server — 192.168.0.101)

| Hostname | Service | Purpose |
|----------|---------|---------|
| yamil-ai.com | http://localhost:9080 | YAMIL frontend + API (via Envoy) |
| www.yamil-ai.com | http://localhost:9080 | YAMIL frontend (www redirect) |
| api.yamil-ai.com | http://localhost:9082 | API gateway (external clients) |

- **Tunnel ID**: 47748839-2815-42ce-9db0-00a09cf26a47
- **Config**: `C:\Users\yvele\.cloudflared\config.yml`
- **Binary**: `C:\project\Ai-Tools\cloudflared.exe`
- **Status**: Windows service (currently DISABLED — start manually or re-enable)

### yamil-qnap (QNAP NAS — 192.168.0.102)

| Hostname | Service | Purpose |
|----------|---------|---------|
| ntfy.yamil-ai.com | http://localhost:8090 | Push notifications (Uptime Kuma alerts) |

- **Tunnel ID**: 31e10393-d998-4310-9b67-fda16be24109
- **Runs as**: Docker container (cloudflared) in `yamil-monitor` stack
- **Config**: Remotely managed via Cloudflare dashboard (token-based)

---

## 3. Monitoring Stack

### Uptime Kuma (QNAP :3001)

- **URL**: http://192.168.0.102:3001
- **Monitors**: 23 active, all services
- **Alerts**: Sends to ntfy topic `yamil-alerts`

### ntfy Push Notifications (QNAP :8090)

- **URL**: http://192.168.0.102:8090/yamil-alerts
- **External**: https://ntfy.yamil-ai.com/yamil-alerts (via Cloudflare tunnel)
- **Purpose**: Receive Uptime Kuma alerts on phone when away from home network
- **Docker**: Part of `yamil-monitor` stack on QNAP

### Grafana (QNAP :3000)

- **URL**: http://192.168.0.102:3000
- **Credentials**: admin / Ashley2029
- **Data source**: Loki
- **Purpose**: Log visualization, dashboards

### Loki (QNAP :3100)

- **URL**: http://192.168.0.102:3100
- **Purpose**: Log aggregation from all services

### Prometheus (QNAP :9090)

- **URL**: http://192.168.0.102:9090
- **Purpose**: Metrics scraping from all LW services on Windows

---

## 4. QNAP Docker Stack (yamil-monitor)

**Path**: `/share/CACHEDEV1_DATA/Container/yamil-monitor/docker-compose.yml`
**Docker binary**: `/share/CACHEDEV1_DATA/.qpkg/container-station/usr/bin/.libs/docker`

```
┌─────────────┬───────┬────────────────────────────┐
│ Service     │ Port  │ Notes                      │
├─────────────┼───────┼────────────────────────────┤
│ Uptime Kuma │ :3001 │ Running, 23 monitors       │
├─────────────┼───────┼────────────────────────────┤
│ ntfy        │ :8090 │ Running, push notifications│
├─────────────┼───────┼────────────────────────────┤
│ cloudflared │  —    │ Running, yamil-qnap tunnel │
├─────────────┼───────┼────────────────────────────┤
│ Prometheus  │ :9090 │ Running, metrics scraping  │
├─────────────┼───────┼────────────────────────────┤
│ Loki        │ :3100 │ Running, log aggregation   │
├─────────────┼───────┼────────────────────────────┤
│ Grafana     │ :3000 │ Running (admin/Ashley2029) │
└─────────────┴───────┴────────────────────────────┘
```

---

## 5. Windows Server Services (192.168.0.101)

| Service | Port | Purpose |
|---------|------|---------|
| Envoy (YAMIL proxy) | 9080 | Frontend + API routing |
| API Gateway | 9082 | External API access |
| APISIX | 9081 | LW API gateway (TCP) |
| Auth Service | — | LW authentication |
| Redis | 127.0.0.1 only | Cache (localhost-bound) |
| PostgreSQL | 127.0.0.1 only | Database (localhost-bound) |
| Promtail | 3101 | Ships Docker logs to QNAP Loki |

---

## 6. QNAP Access

- **Admin UI**: https://192.168.0.102:8443
- **SSH**: Port 2222, user: admin
- **Container Station**: Installed, manages Docker
- **QuFirewall**: Installed, v2.5.0

### QuFirewall API

**Base URL**: `https://192.168.0.102:8443/qufirewall/api/`
**Auth**: Pass `sid=<NAS_SID>` as query parameter (get SID from QTS login cookie)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/qufirewall/api/dashboard?sid=` | GET | Firewall status, deny counts, config flags |
| `/qufirewall/api/profile?sid=` | GET | All firewall profiles with full rule definitions |
| `/qufirewall/api/application_rules?sid=` | GET | Auto-created application rules (Virtual Switch etc.) |

**Dashboard response keys**:
- `firewall_status` — "0" disabled, "1" enabled
- `enable_profile` — name of active profile (empty if none)
- `deny_count` — denied events count

**Profile structure** (from GET /profile):
```json
{
  "name": "Profile Name",
  "psirt_flag": 1,        // PSIRT deny rules
  "tor_flag": 1,          // Tor deny rules
  "botnet_flag": 1,       // Botnet deny rules
  "app_rules": 1,         // Application auto-rules
  "auto_lan_rules_flag": 1, // Auto-LAN discovery allow
  "dhcp_packet_flag": 0,  // DHCP allow
  "rules": [              // Custom IPv4 rules (priority order)
    {"interface":"All","protocol":"Any","src_ip":"192.168.1.0/24","permission":"Allow"},
    {"interface":"All","protocol":"Any","src_ip":"Any","permission":"Deny"}
  ],
  "rulesv6": []           // Custom IPv6 rules
}
```

**Built-in profiles**: Basic protection (14 rules, geo-US), Include subnets only (8 rules, auto-LAN), Restricted security (24 rules, port-specific + geo-US)

**Important**: All built-in profiles with deny-all rules trigger "will block your computer" warning. To safely enable, ensure an explicit `Allow` rule for `192.168.1.0/24` exists above the deny-all rule.

---

## 7. Security Layers

1. **Cloudflare Edge** — DDoS protection, TLS termination, IP hiding, WAF
2. **Cloudflare Tunnel** — Outbound-only connections, no open ports on router
3. **QuFirewall** — QNAP firewall with geo-based IP filtering
4. **Localhost-only services** — Redis, PostgreSQL bound to 127.0.0.1
5. **Let's Encrypt cert** — TLS for yamil-ai.com (Cloudflare handles edge TLS)

---

## 8. Alert Flow

```
Service Down
    ↓
Uptime Kuma (QNAP :3001) detects failure
    ↓
Sends to ntfy (QNAP :8090) topic "yamil-alerts"
    ↓
ntfy exposed via Cloudflare tunnel (ntfy.yamil-ai.com)
    ↓
Phone receives push notification (ntfy app)
```

This works even when away from home network because ntfy is exposed externally via Cloudflare tunnel on the always-on QNAP NAS.

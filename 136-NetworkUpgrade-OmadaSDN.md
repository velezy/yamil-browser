# 136 — Network Upgrade: Alta Labs Route10

**Status**: In Progress
**Created**: 2026-03-08
**Updated**: 2026-03-18
**Philosophy**: AI-Managed Network — the AI controls the entire network via Alta Labs dashboard + SSH through YAMIL Browser

---

## 1. The Problem

Current network is bottlenecked at 1G everywhere:

- **Verizon CR1000A** has a 10GE LAN port wasted on a 1G Bitdefender BOX
- **Two Aruba Instant On 1930 switches** (CLOAK + DAGGER) — 1G only
- **2 Gbps Verizon Fios** plan capped at 1G on LAN
- No VLANs, no proper firewall, no VPN, no AI-manageable network

## 2. Hardware

| Device | Role | Status |
|--------|------|--------|
| **Alta Labs Route10** | 10G Multi-WAN Router — primary router | Active, firmware 1.3z |
| ~~Zyxel XMG1915-10EP~~ | ~~8-port 2.5G PoE++ switch~~ | **Removed** — web UI unreachable, Nebula cloud lock |
| **TRENDnet TEG-3102WS** | 10-port 2.5G Web Smart Switch (8x 2.5G RJ45 + 2x 10G SFP+) | Active, firmware v1.00.14, IP 192.168.0.200 |
| **10Gtek SFP+ DAC 0.5m** | 10G Direct Attach Cable (Route10 SFP+ 2 → TRENDnet Port 9) | **Installed** — verified 10Gbps link up |

### Alta Labs Route10 Specs
- 2x 10G SFP+ ports (WAN/LAN)
- 4x 2.5G RJ45 ports (WAN/LAN)
- Qualcomm Quad-Core processor (aarch64, OpenWrt 21.02.1)
- Hardware-accelerated VPN (WireGuard, IPsec, OpenVPN)
- Hardware acceleration: QCA NSS PPE + ECM (active on 1.3z)
- Multi-WAN load balancing / failover
- 40W PoE+ output
- Real-time stats dashboard
- Stateful firewall, ACL, DoS defense

### TRENDnet TEG-3102WS Specs (Installing Today)
- 8x 2.5GBASE-T RJ45 ports
- 2x 10G SFP+ slots
- Web smart managed (standalone — no cloud lock-in)
- VLAN, QoS, IGMP snooping, link aggregation, port mirroring
- Metal housing, NDAA & TAA compliant
- Lifetime protection warranty

### Zyxel XMG1915-10EP (Removed)
- Was 8x 2.5G PoE++ ports + 2x 10G SFP+
- **Problem**: Nebula cloud mode locked out local web UI (192.168.1.19 unreachable, ARP incomplete)
- Could not configure VLANs without Zyxel Nebula account or physical console access
- Replaced by TRENDnet TEG-30284

## 3. Current Network Topology (Verified 2026-03-20 — CR1000A in Bridge Mode)

```
Fiber ONT (black box)
  └── CR1000A (Verizon Router) — BRIDGE MODE (passthrough, no NAT)
        └── Alta Labs Route10 (Router) — WAN: 70.111.193.92 (public IP via DHCP)
              │
              │  Route10 LAN: 192.168.0.1/24
              │  WireGuard VPN: 10.0.0.0/24 (port 51820, direct — no port forward needed)
              │
              ├── SFP+ 2 (eth5): TRENDnet TEG-3102WS via 10G DAC ← 10Gbps active
              │     ├── Port 1: (down — was Route10 RJ45 uplink, removed)
              │     ├── Port 2: Dark-Knight / Windows PC (.101) — 2.5G
              │     ├── Port 3: Apple device (.109) — 1G
              │     ├── Port 4: Unknown (.113) — 2.5G
              │     ├── Port 5: brainiac7 / Synology (.111) — 1G
              │     ├── Port 6: FridayAI / QNAP (.102) — 1G
              │     ├── Port 7: eero/Aruba (.104) — 1G
              │     ├── Port 8: (down)
              │     ├── Port 9 (SFP+ 1): Route10 via 10G DAC ← 10Gbps active
              │     └── Port 10 (SFP+ 2): empty
              │
              ├── L1 (eth0): connected — 1G (Bitdefender BOX / IoT zone)
              ├── L2 (eth1): connected — 2.5G (MacBook Air)
              ├── L3 (eth2): down
              ├── L4: spare
              └── SFP+ 1 (eth4): empty
```

**Speed Test Results**: 1561.92 Mbps down / 2313.75 Mbps up (was double NAT; now single NAT after bridge mode 2026-03-20)

### Route10 Port Map (verified via SSH ethtool)

| Physical Label | Interface | Type | Speed | Status | Zone |
|----------------|-----------|------|-------|--------|------|
| WAN | eth3 | 2.5G RJ45 | 2.5G | UP | wan (CR1000A bridge → public IP) |
| L1 | eth0 | 2.5G RJ45 | 1G | UP | **iot** (Bitdefender BOX) |
| L2 | eth1 | 2.5G RJ45 | 2.5G | UP | br-lan (MacBook Air) |
| L3 | eth2 | 2.5G RJ45 | — | DOWN | br-lan (empty) |
| L4 | — | 2.5G RJ45 | — | DOWN | — |
| **SFP+ 1** | eth4 | 10G FIBRE | — | **DOWN** | not in bridge |
| **SFP+ 2** | eth5 | 10G FIBRE | **10G** | **UP** | **br-lan** (DAC to TRENDnet) |

### Device IP Assignments (from ARP table + DHCP reservations)

| Device | MAC | IP | Status |
|--------|-----|----|----|
| Route10 (LAN) | bc:b9:23:81:9e:3c | 192.168.0.1 | Gateway |
| Route10 (WAN) | — | 70.111.193.92 | Public IP via DHCP (CR1000A bridge mode) |
| MacBook Air | 00:e0:4c:b3:3d:fe | 192.168.0.10 | Reserved |
| Dark-Knight / Windows PC | 04:d9:f5:81:10:74 | 192.168.0.101 | Reserved |
| FridayAI / QNAP | 24:5e:be:00:e3:fb | 192.168.0.102 | Reserved |
| brainiac7 / Synology | 00:11:32:2d:3c:3c | 192.168.0.111 | Reserved |
| TRENDnet TEG-3102WS | 78:2d:7e:27:e8:b8 | 192.168.0.200 | Static IP |
| eero | fc:3f:a6:80:9f:12 | 192.168.0.115 | DHCP |
| eero Pro 6E | fc:3f:a6:90:e0:b2 | 192.168.0.17 | DHCP |
| **Bitdefender BOX** | e8:44:7e:04:ed:f8 | **192.168.2.10** | **IoT zone** (DHCP from iot pool) |

## 4. Target Network Topology (Final)

```
Fiber ONT (black box)
  └── CR1000A (BRIDGE MODE — passthrough only, WiFi disabled)
        └── Alta Labs Route10 (Router) — WAN from CR1000A
              │
              ├── SFP+ 2 (eth5): TRENDnet TEG-3102WS (core switch, 10G DAC uplink)
              │     │
              │     │  ── TRUSTED ZONE (192.168.0.0/24) ──
              │     ├── Port 1: spare (was RJ45 uplink, now using SFP+ DAC)
              │     ├── Port 2: Dark-Knight / Windows PC (2.5G Realtek)
              │     ├── Port 3: MacBook Air (1G)
              │     ├── Port 4: spare
              │     ├── Port 5: brainiac7 / Synology NAS (1G)
              │     ├── Port 6: FridayAI / QNAP NAS (1G)
              │     ├── Port 7: eero Pro 6E — "eero+" trusted WiFi (1G)
              │     │     ├── iPhones, iPads, laptops
              │     │     └── personal devices (full LAN access)
              │     ├── Port 8: Aruba switch (wired IoT port expander)
              │     │     ├── SmartThings hub (needs ethernet)
              │     │     ├── Sony TV (wired)
              │     │     ├── Lutron hub (wired)
              │     │     └── other wired IoT devices
              │     ├── SFP+ 1 (Port 9): Route10 SFP+ 2 (10G DAC) ← ACTIVE
              │     └── SFP+ 2 (Port 10): future 10G device
              │
              ├── L1 (eth0): Bitdefender BOX (IoT security gateway)
              │     │
              │     │  ── IOT ZONE (192.168.2.0/24, firewall-isolated) ──
              │     └── eero — "eero" IoT WiFi
              ├── L2 (eth1): MacBook Air (2.5G, br-lan)
              ├── L3 (eth2): spare (br-lan)
              └── L4: spare
                          ├── Sony TV
                          ├── SmartThings hub
                          ├── Lutron hub
                          ├── Apple TV / HomePod
                          └── other IoT (all wireless, scanned by Bitdefender)
```

### Security Architecture

| Zone | Subnet | WiFi Network | Switch/Port | Access |
|------|--------|-------------|-------------|--------|
| **Trusted (lan)** | 192.168.0.0/24 | "eero+" (eero Pro 6E) | TRENDnet Port 7 | Full LAN + internet |
| **IoT (iot)** | **192.168.2.0/24** | "eero" (regular eero) | Route10 L1 (eth0) → Bitdefender | **Internet only**, firewall-isolated |
| **Servers** | 192.168.0.0/24 | Wired only | TRENDnet Ports 2-6 | Full LAN + internet |
| **VPN (vpn)** | 10.0.0.0/24 | N/A | WireGuard wg0 | Full LAN + internet |

- **Trusted devices** (phones, laptops) connect to "eero+" WiFi → TRENDnet → full access to servers, NAS, internet
- **IoT devices** (TV, SmartThings, Lutron) connect to "eero" WiFi → Bitdefender BOX scans all traffic → internet only, **firewall blocks all access to 192.168.0.0/24**
- **Firewall isolation** via Route10 UCI/iptables: IoT zone has separate subnet (192.168.2.0/24), REJECT input/forward, only allows DHCP/DNS to router and forwarding to WAN
- **LAN → IoT allowed** for management (SmartThings app, Chromecast setup, etc.)
- Bitdefender BOX provides: malware scanning, botnet detection, vulnerability assessment, device fingerprinting

## 5. Network Configuration

### Subnet: 192.168.0.0/24
- Gateway: 192.168.0.1 (Route10)
- DNS: 1.1.1.1, 8.8.8.8 (Cloudflare + Google)
- DHCP range: managed by Route10

### WireGuard VPN
- Interface: wg0
- VPN subnet: 10.0.0.0/24
- Server: 10.0.0.1 (Route10)
- Client: 10.0.0.2 (yamil-mobile)
- Port: 51820/UDP
- Hardware-accelerated on Qualcomm NPU
- Firewall zones: vpn→lan, vpn→wan, lan→vpn
- CR1000A port forward: UDP 51820 → 192.168.1.227 (Route10 WAN IP)
- Public IP: *(check current IP — Verizon Fios, may change, consider DDNS)*

#### Mobile Client Config (WireGuard App)

> **Keys stored in AWS Secrets Manager**: `yamil/homelab/route10-wireguard`
> Contains: server private/public keys, client private/public key, mac client key, preshared key

**Interface:**
- Name: `Home`
- Private Key: *(see AWS SM `yamil/homelab/route10-wireguard` → `client_private_key`)*
- Addresses: `10.0.0.2/32`
- DNS: `1.1.1.1`

**Peer:**
- Public Key: *(see AWS SM `yamil/homelab/route10-wireguard` → `server_public_key`)*
- Preshared Key: *(see AWS SM `yamil/homelab/route10-wireguard` → `preshared_key`)*
- Endpoint: `<your-public-ip>:51820` *(check current IP or set up DDNS)*
- Allowed IPs: `192.168.0.0/24, 10.0.0.0/24`
- Persistent Keepalive: `25`

#### Connection Notes
- Install WireGuard app (iOS App Store / Google Play) → + → Create from scratch
- Use `Allowed IPs: 0.0.0.0/0` for full tunnel (all traffic through VPN)
- If public IP changes, update the Endpoint in the app
- For permanent access: set up DDNS (DuckDNS, No-IP, or Cloudflare) to auto-update

### Firewall
- WAN: DROP (default deny inbound)
- LAN: ACCEPT
- **IoT: REJECT** input + forward (isolated subnet)
  - Allowed: DHCP (UDP 67-68), DNS (TCP/UDP 53) to router
  - Forwarding: IoT → WAN only (internet access)
  - Forwarding: LAN → IoT (management access)
  - NAT/Masquerade: ON (IoT zone, for internet NAT)
- NAT/Masquerade: ON (WAN zone)
- WireGuard: Allow UDP 51820 from WAN
- Default rules: DHCP, Ping, IGMP, IPsec, ICMPv6

### SSH Access
- Host: 192.168.0.1, Port: 22
- User: root
- Auth: SSH key (ed25519) — Mac's key added via manage.alta.inc
- AWS Secret: `yamil/homelab/route10-ssh`

## 6. Implementation Order

- [x] Phase 1: Unbox and firmware update Alta Labs Route10
- [x] Phase 2: Connect Route10 as primary router, WAN via CR1000A (DHCP)
- [x] Phase 3: Factory reset Route10 to firmware 1.3z (stable, pre-bug firmware)
- [x] Phase 4: Configure Route10 via "Setup Router" (local mode, 192.168.0.1/24)
- [x] Phase 5: Connect to Alta Labs cloud (manage.alta.inc) for initial config
- [x] Phase 6: Disable automatic firmware updates (pin at 1.3z)
- [x] Phase 7: Add SSH key, configure via CLI (UCI/OpenWrt)
- [x] Phase 8: DHCP reservations (4 devices: PC, MacBook, QNAP, Synology)
- [x] Phase 9: DNS configuration (1.1.1.1 + 8.8.8.8)
- [x] Phase 10: WireGuard VPN (10.0.0.0/24, port 51820, 1 client peer) — re-configured via SSH/UCI 2026-03-16
- [x] Phase 11: Firewall verified (WAN DROP, LAN ACCEPT, NAT ON, WireGuard zone + rules)
- [x] Phase 12: Port forward UDP 51820 on CR1000A → 192.168.1.227 — verified working over 5G cellular
- [x] Phase 13: Connect TRENDnet TEG-3102WS switch — configured IP 192.168.0.200, saved to startup
- [x] Phase 14: Install 10G SFP+ DAC cable — Route10 SFP+ 2 (eth5) ↔ TRENDnet Port 9 (SFP+ 1), verified 10Gbps link up
- [x] Phase 15: Move trusted devices to TRENDnet — PC port 2 (2.5G), Synology port 5 (1G), QNAP port 6 (1G) verified on TRENDnet UI
- [x] Phase 16: eero connected to TRENDnet Port 7 (1G link up verified)
- [x] Phase 17: Move Bitdefender BOX to Route10 L4 port — done
- [x] Phase 18: Connect eero ("eero" IoT WiFi) behind Bitdefender BOX — IoT isolated + scanned
- [x] Phase 19: Move all IoT devices to "eero" WiFi (Sony TV, SmartThings, Lutron, Apple TV)
- [x] Phase 19.5: **IoT firewall isolation** — eth0 removed from br-lan, IoT zone (192.168.2.0/24), Bitdefender BOX at 192.168.2.10, firewall REJECT IoT→LAN, allow IoT→WAN + LAN→IoT management (2026-03-18)
- [ ] Phase 20: Repurpose Aruba switches — keep 1 as wired IoT port expander on TRENDnet Port 8, retire the other
- [ ] Phase 21: Configure QoS / bandwidth control (if needed)
- [x] Phase 22: **CR1000A bridge mode enabled** — Broadband Connection bridged in Network (Home/Office) Settings, Route10 WAN now gets public IP (70.111.193.92) directly via DHCP, single NAT, CR1000A port forwards no longer needed (2026-03-20)
- [x] Phase 23: **All services verified post-bridge-mode** — Uptime Kuma (:3001), Grafana (:3000), YAMIL Browser (:9300), Route10 SSH, TRENDnet Switch (:80), QNAP QTS (:8443 HTTPS, Force SSL enabled, HTTP :8092 redirects), WireGuard VPN (:51820) — all working (2026-03-20)
- [ ] Phase 24: Test AI management — YAMIL Browser manages network via SSH + API

## 7. Critical Lessons Learned

### Firmware 1.3z is Stable — Do NOT Update
- Factory reset reverted Route10 from 1.4x to 1.3z
- 1.3z has working hardware acceleration (QCA NSS + ECM) — verified 1.5/2.3 Gbps throughput
- **1.4 series bugs** (confirmed by community):
  - Hardware acceleration drops throughput 30-50%
  - CPU usage INCREASES with acceleration enabled (opposite of expected)
  - DPI regression in 1.4g: 900 → 200 Mbps
  - UDP acceleration disabled in 1.4l ("BitTorrents waste too many flow slots")
  - SFP+ asymmetric throughput issues
  - QoS/CAKE bugs
- Automatic updates DISABLED in manage.alta.inc Settings > System

### Cloud Management Causes Problems
- Cloud re-adoption pushes old site config (wrong subnet, phantom WAN2)
- Subnet conflict: Route10 LAN 192.168.1.0/24 overlapped CR1000A LAN 192.168.1.0/24
- **Solution**: Factory reset → "Setup Router" (local mode first) → set 192.168.0.0/24 → THEN connect to cloud
- `/cfg/post-cfg.sh` can override cloud-pushed settings (runs after every cloud config push)
- SSH/UCI is the reliable configuration method — cloud UI sometimes doesn't persist changes

### Double NAT Eliminated (2026-03-20)
- ~~CR1000A (NAT) → Route10 (NAT) → devices~~ — **resolved**
- CR1000A now in bridge mode — Route10 gets public IP directly
- Single NAT: ONT → CR1000A (bridge) → Route10 (NAT) → devices
- Was achieving 1.5/2.3 Gbps through double NAT; should be same or better now

## 8. AI Management Capabilities

### Via SSH (Primary — UCI/OpenWrt)
- Full system configuration: network, firewall, DHCP, DNS, VPN, QoS
- UCI commands: `uci set/get/commit`, configs in `/etc/config/`
- Service management: `/etc/init.d/<service> restart`
- Real-time monitoring: `ifstat`, `top`, `wg show`, `iptables -L`
- Hardware acceleration: `echo N >/cfg/alta_bits` (0=off, 4=UDP enabled)

### Via Alta Labs Dashboard (manage.alta.inc)
- Visual device topology and status
- Real-time throughput graphs
- VLAN creation and assignment
- Firewall rule management
- VPN configuration UI

### Via TRENDnet Switch Web UI (after Phase 13)
- Port monitoring — speed, traffic, errors per port
- VLAN assignment — assign ports to VLANs
- QoS — prioritize traffic classes
- Link aggregation — bond ports for more bandwidth
- IGMP snooping — optimize multicast

## 9. AWS Secrets

| Secret Name | Contents |
|-------------|----------|
| `yamil/homelab/route10-ssh` | SSH access: root@192.168.0.1:22, ed25519 key, firmware 1.3z |
| `yamil/homelab/route10-wireguard` | WireGuard keys (server + client + preshared), port 51820, VPN subnet 10.0.0.0/24 |
| `yamil/homelab/trendnet-switch` | TRENDnet TEG-3102WS web UI: admin, IP 192.168.0.200 |

## 10. Rollback Plan

If anything goes wrong:
1. Unplug Route10 from CR1000A
2. Take CR1000A out of bridge mode (factory reset if needed)
3. Re-plug Aruba switches into CR1000A LAN ports
4. Network returns to original state — nothing lost

## 11. Notes

- CR1000A bridge mode may require calling Verizon or accessing hidden admin page
- Bitdefender BOX monitors IoT traffic — keep it on Route10 L4 port
- eero mesh may need reconfiguration after topology change
- Windows PC should use the 2.5G Realtek port (not the 1G Intel port)
- QNAP and Synology are 1G devices — they'll negotiate down but benefit from the faster backbone
- All configuration done through YAMIL Browser + SSH for RAG learning
- Route10 runs OpenWrt 21.02.1 (aarch64) — full Linux system with UCI
- Zyxel XMG1915-10EP removed from plan — Nebula cloud lock prevented local management
- TRENDnet TEG-3102WS chosen as replacement: standalone web UI, no cloud dependency, 8x 2.5G + 2x 10G SFP+
- WireGuard config was lost during Route10 factory reset (Phase 3) — re-applied via SSH/UCI on 2026-03-16, verified working over 5G cellular

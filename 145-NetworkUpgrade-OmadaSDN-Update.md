# 136 — Network Upgrade: TP-Link Omada SDN

**Status**: In Progress — Phases 1-8 Complete (except 2,4,5)
**Created**: 2026-03-08
**Philosophy**: AI-Managed Network — the AI controls the entire network via Omada dashboard through YAMIL Browser

---

## 1. The Problem

Current network is bottlenecked at 1G everywhere:

- **Verizon CR1000A** has a 10GE LAN port wasted on a 1G Bitdefender BOX
- **Two Aruba Instant On 1930 switches** (CLOAK + DAGGER) — 1G only
- **2 Gbps Verizon Fios** plan capped at 1G on LAN
- No VLANs, no proper firewall, no VPN, no AI-manageable network

## 2. Hardware Purchased

| Device | Price | Role |
|--------|-------|------|
| **TP-Link ER707-M2** | ~$120 | Omada VPN Router — replaces CR1000A as primary router |
| **TP-Link SG2210XMP-M2** | ~$250 | Omada 8-port 2.5G PoE+ switch + 2x 10G SFP+ |
| **Total** | **~$370** | |

### ER707-M2 Specs
- 2x 2.5G WAN/LAN ports
- 4x 1G WAN/LAN ports
- 1x SFP WAN/LAN slot
- VPN: 100 IPsec, 66 OpenVPN, 60 L2TP, 60 PPTP
- SPI Firewall, ACL, DoS defense
- Load balancing (multi-WAN)
- 500K concurrent sessions, 1000+ clients
- Omada SDN integrated

### SG2210XMP-M2 Specs
- 8x 2.5G 802.3at/af PoE+ ports (160W budget, 30W/port)
- 2x 10G SFP+ uplink slots
- Smart managed (L2)
- Omada SDN integrated
- Fanless / silent
- VLAN, QoS, IGMP snooping, link aggregation

## 3. Current Network Topology

```
Fiber ONT (black box)
  └── CR1000A (Verizon Router) — WAN: 10G, WiFi active
        ├── 10GE LAN port → Bitdefender BOX (1G) → ???
        ├── LAN Port 1 → Aruba CLOAK (1G switch)
        │     ├── Port 2: Kain (unknown)
        │     ├── Port 3: Lutron-032e8493
        │     ├── Port 5: Brainiac6 (FridayAI/QNAP - 24:5e:be:00:e3:fb)
        │     └── Port 7: Brainiac5 (brainiac7/Synology - 00:11:32:2d:3c:3c)
        ├── LAN Port 2 → Aruba DAGGER (1G switch)
        │     ├── Port 2: SmartThings hub (24:fd:5b:03:55:47)
        │     ├── Port 3: Sony TV (cc:98:8b:00:0b:e1)
        │     ├── Port 5: OfficeTheater3 (58:d3:49:e0:62:f2)
        │     ├── Port 6: eero (fc:3f:a6:90:e0:a0)
        │     ├── Port 7: Dark-Knight / Windows PC (04:d9:f5:81:10:74)
        │     └── Port 8: unknown (00:22:6c:3b:57:65)
        ├── LAN Port 3 → empty
        └── LAN Port 4 → empty
```

## 4. Target Network Topology

```
Fiber ONT (black box)
  └── CR1000A (BRIDGE MODE — passthrough only, WiFi disabled)
        ├── ER707-M2 (Omada Router) — WAN: 2.5G
        │     └── SG2210XMP-M2 (Omada Switch) — 2.5G backbone
        │           ├── Port 1: ER707-M2 (uplink to router, 2.5G)
        │           ├── Port 2: Dark-Knight / Windows PC (2.5G Realtek)
        │           ├── Port 3: GEEKOM A8 Max (2.5G port 1)
        │           ├── Port 4: FridayAI / QNAP TS-251+
        │           ├── Port 5: brainiac7 / Synology
        │           ├── Port 6: Mac Mini M4 (1G, negotiates down)
        │           ├── Port 7: eero Pro 6E (mesh WiFi)
        │           └── Port 8: spare
        │           SFP+ 1: future 10G uplink
        │           SFP+ 2: future 10G uplink
        │
        └── Bitdefender BOX (1G, monitoring only — not inline)
              └── Aruba DAGGER (1G) — IoT only
                    ├── Sony TV
                    ├── eero
                    ├── Lutron
                    ├── SmartThings hub
                    └── other IoT
```

## 5. VLAN Plan

| VLAN | Name | Devices | Purpose |
|------|------|---------|---------|
| 1 | Default/Management | ER707-M2, SG2210XMP-M2 | Network management |
| 10 | Production | Dark-Knight, GEEKOM, QNAP, Synology, Mac Mini | Docker Swarm, NAS, dev machines |
| 20 | IoT | Aruba DAGGER → TV, Lutron, SmartThings, eero | Isolated IoT — no access to Production VLAN |

## 6. Implementation Order

- [x] Phase 1: Unbox and firmware update both devices (ER707-M2 → fw 1.3.1)
- [x] Phase 2: CR1000A port forwarding for VPN (UDP 500, 4500, 1701 → ER707-M2 at 192.168.1.226; bridge mode skipped — double NAT)
- [x] Phase 3: Connect ER707-M2 as primary router, configure WAN (DHCP from CR1000A, 192.168.1.226)
- [x] Phase 4: Connect SG2210XMP-M2 to ER707-M2
- [x] Phase 5: Move devices from Aruba to TP-Link switch (PC, NAS units, GEEKOM)
- [x] Phase 6: Set up Omada SDN dashboard (controller on QNAP, ER707-M2 adopted)
- [x] Phase 7: Configure VLANs (Default=1, Production=10 192.168.10.0/24, IoT=20 192.168.20.0/24)
- [x] Phase 8: Configure firewall rules (Gateway ACL: Deny All, IoT→Production, LAN→LAN)
- [x] Phase 9: Set up VPN (L2TP/IPsec, policy: YAMIL_Remote_Access, pool: 192.168.30.0/24, user: yvelez)
- [x] Phase 10: Configure QoS (Bandwidth Control: Docker-NAS-Priority 1.5G/2G, IoT-Throttle 100M/200M, WAN1)
- [ ] Phase 11: Retire Aruba CLOAK, keep DAGGER for IoT sub-switch
- [ ] Phase 12: Verify all services (Uptime Kuma, Grafana, Logic Weaver, YAMIL Browser)
- [ ] Phase 13: Test AI management — YAMIL Browser manages Omada dashboard

## 7. AI Management Capabilities (via Omada Dashboard + YAMIL Browser)

### Router (ER707-M2)
- Firewall rules — block/allow traffic between devices or VLANs
- VPN — create/manage VPN tunnels for remote access
- VLANs — create/modify isolated networks
- Port forwarding — open/close ports for services
- Bandwidth control — throttle or prioritize per device/VLAN
- DHCP — static IP assignments, reservations
- Routing — static routes between subnets

### Switch (SG2210XMP-M2)
- Port monitoring — speed, traffic, errors per port
- PoE control — power cycle devices remotely (toggle PoE per port)
- VLAN assignment — assign ports to VLANs
- QoS — prioritize traffic classes
- Link aggregation — bond ports for more bandwidth
- IGMP snooping — optimize multicast

## 8. Rollback Plan

If anything goes wrong:
1. Unplug ER707-M2 from CR1000A
2. Take CR1000A out of bridge mode (factory reset if needed)
3. Re-plug Aruba switches into CR1000A LAN ports
4. Network returns to current state — nothing lost

## 9. Notes

- CR1000A bridge mode may require calling Verizon or accessing hidden admin page
- Bitdefender BOX monitors IoT traffic — keep it between switch and Aruba (IoT sub-network)
- eero mesh may need reconfiguration after topology change
- Windows PC should use the 2.5G Realtek port (not the 1G Intel port)
- QNAP and Synology are 1G devices — they'll negotiate down but benefit from the faster backbone
- All configuration done through YAMIL Browser for RAG learning

# 136 — Network Upgrade: Alta Labs + Zyxel

**Status**: In Progress
**Created**: 2026-03-08
**Updated**: 2026-03-15
**Philosophy**: AI-Managed Network — the AI controls the entire network via Alta Labs dashboard through YAMIL Browser

---

## 1. The Problem

Current network is bottlenecked at 1G everywhere:

- **Verizon CR1000A** has a 10GE LAN port wasted on a 1G Bitdefender BOX
- **Two Aruba Instant On 1930 switches** (CLOAK + DAGGER) — 1G only
- **2 Gbps Verizon Fios** plan capped at 1G on LAN
- No VLANs, no proper firewall, no VPN, no AI-manageable network

## 2. Hardware Purchased

| Device | Role |
|--------|------|
| **Alta Labs Route10** | 10G Multi-WAN Router — primary router replacing CR1000A |
| **Zyxel XMG1915-10EP** | 8-port 2.5G PoE++ switch — core switch |

### Alta Labs Route10 Specs
- 2x 10G SFP+ ports (WAN/LAN)
- 4x 2.5G RJ45 ports (WAN/LAN)
- Qualcomm Quad-Core processor
- Hardware-accelerated VPN (WireGuard, IPsec, OpenVPN)
- Multi-WAN load balancing / failover
- 40W PoE+ output
- Real-time stats dashboard
- Stateful firewall, ACL, DoS defense

### Zyxel XMG1915-10EP Specs
- 8x 2.5G RJ45 PoE++ ports (130W total budget)
- 2x 10G SFP+ uplink slots
- Cloud-managed (Nebula) or standalone smart-managed
- Desktop or wall mount
- VLAN, QoS, IGMP snooping, link aggregation
- Fanless / silent

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
        ├── Alta Labs Route10 (Router) — WAN: 10G SFP+
        │     ├── 2.5G Port → Zyxel XMG1915-10EP (Switch) — 2.5G backbone
        │     │     ├── Port 1: Route10 (uplink to router, 2.5G)
        │     │     ├── Port 2: Dark-Knight / Windows PC (2.5G Realtek)
        │     │     ├── Port 3: GEEKOM A8 Max (2.5G port 1)
        │     │     ├── Port 4: FridayAI / QNAP TS-251+
        │     │     ├── Port 5: brainiac7 / Synology
        │     │     ├── Port 6: Mac Mini M4 (1G, negotiates down)
        │     │     ├── Port 7: eero Pro 6E (mesh WiFi)
        │     │     └── Port 8: spare
        │     │     SFP+ 1: future 10G device
        │     │     SFP+ 2: future 10G device
        │     │
        │     └── 40W PoE+ port available for AP or camera
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
| 1 | Default/Management | Route10, XMG1915-10EP | Network management |
| 10 | Production | Dark-Knight, GEEKOM, QNAP, Synology, Mac Mini | Docker Swarm, NAS, dev machines |
| 20 | IoT | Aruba DAGGER → TV, Lutron, SmartThings, eero | Isolated IoT — no access to Production VLAN |

## 6. Implementation Order

- [ ] Phase 1: Unbox and firmware update Alta Labs Route10 + Zyxel XMG1915-10EP
- [ ] Phase 2: Connect Route10 as primary router, configure WAN (DHCP from CR1000A or bridge mode)
- [ ] Phase 3: Connect Zyxel XMG1915-10EP to Route10
- [ ] Phase 4: Move devices to Zyxel switch (PC, NAS units, GEEKOM, Mac Mini)
- [ ] Phase 5: Configure VLANs (Default=1, Production=10 192.168.10.0/24, IoT=20 192.168.20.0/24)
- [ ] Phase 6: Configure firewall rules (ACL: deny IoT→Production, allow Production→Internet)
- [ ] Phase 7: Set up VPN (WireGuard or IPsec — hardware-accelerated on Route10)
- [ ] Phase 8: Configure QoS / bandwidth control
- [ ] Phase 9: Retire Aruba CLOAK, keep DAGGER for IoT sub-switch
- [ ] Phase 10: Verify all services (Uptime Kuma, Grafana, Logic Weaver, YAMIL Browser)
- [ ] Phase 11: Test AI management — YAMIL Browser manages Alta Labs dashboard

## 7. AI Management Capabilities (via Alta Labs Dashboard + YAMIL Browser)

### Router (Alta Labs Route10)
- Firewall rules — block/allow traffic between devices or VLANs
- VPN — hardware-accelerated WireGuard/IPsec/OpenVPN tunnels
- VLANs — create/modify isolated networks
- Port forwarding — open/close ports for services
- Multi-WAN load balancing / failover
- Bandwidth control — throttle or prioritize per device/VLAN
- DHCP — static IP assignments, reservations
- Routing — static routes between subnets
- Real-time stats — live throughput, latency, session counts
- 40W PoE+ — power an AP or camera directly from router

### Switch (Zyxel XMG1915-10EP)
- Port monitoring — speed, traffic, errors per port
- PoE++ control — power cycle devices remotely (130W budget, per-port toggle)
- VLAN assignment — assign ports to VLANs
- QoS — prioritize traffic classes
- Link aggregation — bond ports for more bandwidth
- IGMP snooping — optimize multicast
- Cloud management — Zyxel Nebula or standalone web UI

## 8. Rollback Plan

If anything goes wrong:
1. Unplug Route10 from CR1000A
2. Take CR1000A out of bridge mode (factory reset if needed)
3. Re-plug Aruba switches into CR1000A LAN ports
4. Network returns to original state — nothing lost

## 9. Notes

- CR1000A bridge mode may require calling Verizon or accessing hidden admin page
- Bitdefender BOX monitors IoT traffic — keep it between switch and Aruba (IoT sub-network)
- eero mesh may need reconfiguration after topology change
- Windows PC should use the 2.5G Realtek port (not the 1G Intel port)
- QNAP and Synology are 1G devices — they'll negotiate down but benefit from the faster backbone
- All configuration done through YAMIL Browser for RAG learning
- Alta Labs Route10 default management IP: check documentation (typically 192.168.1.1)
- Zyxel XMG1915-10EP default management IP: typically 192.168.1.1 (may conflict — assign static IPs during setup)
- No controller dependency — both devices are self-contained (no external software like Omada Controller needed)

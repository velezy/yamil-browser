# Windows PC Fix: Bitdefender Blocking QNAP (192.168.0.102)

## Problem
Bitdefender IGNIS Firewall on the Windows PC has WFP (Windows Filtering Platform) filters that **block all traffic to/from 192.168.0.102** (QNAP NAS). This prevents:
- Consul cluster communication (ports 8300, 8301)
- Ping (ICMP)
- Any TCP connection to QNAP

Other IPs work fine (GEEKOM 192.168.0.113, MacBook 192.168.0.120, router 192.168.0.1).

## Root Cause
Two `FWP_ACTION_BLOCK` filters from "Bitdefender IGNIS Firewall" targeting `FWPM_CONDITION_IP_REMOTE_ADDRESS = 192.168.0.102` at `FWPM_LAYER_INBOUND_IPPACKET_V4` with max weight (`uint64: 18446744073709551615`).

These operate **below** the Windows Firewall — toggling Windows Firewall profiles has zero effect.

## What Needs To Be Done

### Option A: Bitdefender UI (preferred)
1. Open Bitdefender on the Windows PC
2. Go to **Protection > Firewall**
3. Change the network adapter profile from "Public" to **"Home/Office"** or **"Trusted"**
4. OR: Add a firewall rule to **Allow all traffic** to/from IP **192.168.0.102** (both directions, all protocols, all ports)
5. OR: Temporarily disable Bitdefender Firewall to confirm it fixes it, then create the proper rule

### Option B: PowerShell (if Bitdefender has CLI tools)
Check if `bdproduct` or Bitdefender command-line tools exist:
```
dir "C:\Program Files\Bitdefender\Bitdefender Security\product*.exe"
```

### Verification Steps
After making the change, run these commands to verify:

```powershell
# 1. Test ping
ping 192.168.0.102

# 2. Test TCP to Consul
Test-NetConnection 192.168.0.102 -Port 8301

# 3. Rejoin Consul cluster
C:\Users\yvele\consul\consul.exe join 192.168.0.102

# 4. Verify WFP BLOCK filters are gone
netsh wfp show state
Select-String -Path wfpstate.xml -Pattern '192.168.0.102' -Context 15,5 | Select-String 'BLOCK'
# Should return nothing

# 5. Verify Consul health checks
curl http://localhost:8500/v1/health/node/windows-pc
# All services should show "passing"
```

## Current State
- **Consul service**: Running as Windows Service (`sc query Consul`)
- **Consul config**: `C:\Users\yvele\consul\config\client.json` and `services.json`
- **Consul binary**: `C:\Users\yvele\consul\consul.exe` (v1.22.5)
- **Windows Firewall**: Currently DISABLED (was disabled during troubleshooting) — re-enable after Bitdefender is fixed
- **Services registered**: yamil-frontend (:9080), docker-engine (:9323), ollama (:11434), postgres (:5432), redis (:6379)

## After Fixing Bitdefender
1. Re-enable Windows Firewall: `Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled True`
2. Consul firewall rules already exist (check with `Get-NetFirewallRule | Where-Object { $_.DisplayName -match 'Consul' }`)
3. Verify all 5 Consul nodes are alive: `C:\Users\yvele\consul\consul.exe members`
4. Verify all service health checks pass: `curl http://localhost:8500/v1/health/state/any`

## Context
- QNAP NAS (192.168.0.102) runs the Consul server, Prometheus, Grafana, and all monitoring
- Windows PC is the primary Docker host running 37 containers (Y.A.M.I.L, DriveSentinel, Memobytes)
- The Consul `yamil-frontend` service on Windows PC is the critical HA failover trigger
- All other 4 Consul nodes are alive and healthy (qnap, macbook, macmini, geekom)

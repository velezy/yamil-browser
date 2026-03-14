# 139 — Fix Cloudflared on Windows PC

## Purpose

This document tells Claude Code on the **Windows PC** how to fix duplicate cloudflared instances causing intermittent 502 errors on `yamil-ai.com` and `api.yamil-ai.com`, enable SSH for remote management, and save credentials to AWS Secrets Manager.

Read this file and execute all sections in order.

---

## Context

- **Machine**: Windows PC at `192.168.0.101` (Ryzen 9 3900x, 64GB, RTX 3070 Ti)
- **Problem**: 4 cloudflared instances are running (should be 1), causing 16 connections and intermittent 502s
- **Tunnel**: `yamil-local` (ID: `47748839-2815-42ce-9db0-00a09cf26a47`)
- **Cloudflare Account**: `2c4d6f2c9a02c47a683b1cf5c0e61fcc`
- **Domain**: `yamil-ai.com`

---

## Section 1: Diagnose Cloudflared

Run these commands in PowerShell (as Administrator):

### 1a. Find all cloudflared processes

```powershell
# Check for standalone cloudflared processes
Get-Process cloudflared -ErrorAction SilentlyContinue | Format-Table Id, ProcessName, StartTime, Path

# Check for cloudflared Windows service
Get-Service cloudflared -ErrorAction SilentlyContinue | Format-Table Name, Status, StartType

# Check for cloudflared in Docker
docker ps --filter "name=cloudflared" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker ps --filter "ancestor=cloudflare/cloudflared" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### 1b. Identify the correct one

There should be exactly **one** cloudflared running. The correct configuration routes:
- `yamil-ai.com` → `http://localhost:9080`
- `www.yamil-ai.com` → `http://localhost:9080`
- `api.yamil-ai.com` → `http://localhost:9082`
- `ntfy.yamil-ai.com` → `http://192.168.0.102:8090`

Check which type is running:
- **Docker container**: Part of `parser_lite/logic-weaver/docker-compose.yml`
- **Windows service**: Installed via `cloudflared service install`
- **Standalone**: Running from `C:\Users\yvele\.cloudflared\` or similar

---

## Section 2: Kill All Cloudflared and Restart One

### 2a. Stop everything

```powershell
# Stop Windows service (if exists)
Stop-Service cloudflared -ErrorAction SilentlyContinue

# Kill all cloudflared processes
Get-Process cloudflared -ErrorAction SilentlyContinue | Stop-Process -Force

# Stop Docker cloudflared containers
docker ps --filter "ancestor=cloudflare/cloudflared" -q | ForEach-Object { docker stop $_ }
docker ps --filter "name=cloudflared" -q | ForEach-Object { docker stop $_ }
```

### 2b. Verify all stopped

```powershell
Get-Process cloudflared -ErrorAction SilentlyContinue
# Should return nothing

docker ps --filter "name=cloudflared" --format "{{.Names}}"
# Should return nothing
```

### 2c. Disable duplicate startup methods

If cloudflared was running as BOTH a service AND Docker, pick ONE:

**Option A — Keep as Docker container (RECOMMENDED if using docker-compose):**
```powershell
# Remove Windows service so it doesn't auto-start
cloudflared service uninstall
# Then restart via docker-compose:
cd C:\project\parser_lite\logic-weaver
docker compose up -d cloudflared
```

**Option B — Keep as Windows service (if NOT using docker-compose for it):**
```powershell
# Remove cloudflared from docker-compose.yml if it's there
# Then restart the service:
Start-Service cloudflared
```

**Option C — If running as standalone (Task Scheduler or startup script):**
```powershell
# Check Task Scheduler
schtasks /query /fo LIST /v | findstr /i "cloudflared"
# Disable duplicates, keep one

# Or check startup folder
explorer shell:startup
# Remove duplicate cloudflared shortcuts
```

### 2d. Start exactly one instance

If using Docker compose:
```powershell
cd C:\project\parser_lite\logic-weaver
docker compose up -d cloudflared
docker compose logs cloudflared --tail 20
```

If using the service:
```powershell
Start-Service cloudflared
Get-Service cloudflared
```

If standalone binary:
```powershell
cloudflared tunnel --config C:\Users\yvele\.cloudflared\config.yml run yamil-local
```

### 2e. Verify fix

```powershell
# Should show exactly 1 process
Get-Process cloudflared -ErrorAction SilentlyContinue | Measure-Object | Select-Object Count

# Test the tunnel
curl https://yamil-ai.com -o NUL -w "%{http_code}"
curl https://api.yamil-ai.com -o NUL -w "%{http_code}"
# Both should return 200

# Run multiple times to confirm no more intermittent 502s
1..10 | ForEach-Object { curl -s -o NUL -w "%{http_code} " https://yamil-ai.com }
# Should be all 200s
```

---

## Section 3: Enable OpenSSH Server on Windows

This allows remote management from the MacBook.

```powershell
# Install OpenSSH Server (run as Administrator)
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0

# Start and set to auto-start
Start-Service sshd
Set-Service -Name sshd -StartupType Automatic

# Verify it's running
Get-Service sshd

# Allow through firewall
New-NetFirewallRule -Name "OpenSSH-Server" -DisplayName "OpenSSH Server" -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22

# Test locally
ssh localhost whoami
```

Default auth: Windows username + password. To find your username:
```powershell
whoami
# Usually: MACHINENAME\username or just username
```

---

## Section 4: Generate SSH Key and Add to Authorized Keys

```powershell
# Generate ed25519 key if it doesn't exist
if (-not (Test-Path ~/.ssh/id_ed25519)) {
    ssh-keygen -t ed25519 -f $env:USERPROFILE\.ssh\id_ed25519 -N '""'
}

# Show the public key (save this for AWS SM)
Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub

# Add MacBook's public key to authorized_keys (paste the key below)
# The MacBook's public key is:
# ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILmhwsSQkhFzwQzuiEyK8dSlIi1fa+Id/1iVGPNsq1HR yaml@MacBook-Air.local
$macKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILmhwsSQkhFzwQzuiEyK8dSlIi1fa+Id/1iVGPNsq1HR yaml@MacBook-Air.local"

# For admin users, add to administrators_authorized_keys
$adminKeysFile = "$env:ProgramData\ssh\administrators_authorized_keys"
if (-not (Test-Path $adminKeysFile)) { New-Item -Path $adminKeysFile -ItemType File -Force }
Add-Content -Path $adminKeysFile -Value $macKey
# Fix permissions (required for OpenSSH on Windows)
icacls $adminKeysFile /inheritance:r /grant "Administrators:F" /grant "SYSTEM:F"

# Also add to user authorized_keys as fallback
$userKeysFile = "$env:USERPROFILE\.ssh\authorized_keys"
if (-not (Test-Path $userKeysFile)) { New-Item -Path $userKeysFile -ItemType File -Force }
Add-Content -Path $userKeysFile -Value $macKey

# Test SSH from the machine itself
ssh -o StrictHostKeyChecking=no localhost "echo SSH working"
```

---

## Section 5: Save Credentials to AWS Secrets Manager

### 5a. Configure AWS CLI (if not already done)

```powershell
# Check if AWS CLI is configured
aws sts get-caller-identity

# If not configured:
# aws configure
# Region: us-east-1
# Output: json
```

### 5b. Save Windows PC SSH credentials

```powershell
# Get values
$username = $env:USERNAME
$hostname = hostname
$privateKey = Get-Content $env:USERPROFILE\.ssh\id_ed25519 -Raw
$publicKey = Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub -Raw

# Build secret JSON
$secret = @{
    host = "192.168.0.101"
    port = 22
    username = $username
    auth = "ssh-key"
    private_key = $privateKey.Trim()
    public_key = $publicKey.Trim()
    key_path = "~/.ssh/id_ed25519"
    hostname = $hostname
    os = "Windows 11 Pro"
    notes = "Ryzen 9 3900x, 64GB, RTX 3070 Ti. Primary Docker host."
} | ConvertTo-Json -Compress

# Create or update the secret
aws secretsmanager create-secret --name "yamil/homelab/windows-pc" --secret-string $secret 2>$null
if ($LASTEXITCODE -ne 0) {
    aws secretsmanager update-secret --secret-id "yamil/homelab/windows-pc" --secret-string $secret
}
```

### 5c. Verify secrets stored correctly

```powershell
aws secretsmanager get-secret-value --secret-id "yamil/homelab/windows-pc" --query SecretString --output text | ConvertFrom-Json | Format-List host, port, username, os, notes
```

---

## Section 6: Verify Everything

### 6a. Tunnel health

```powershell
# Only 1 cloudflared process
(Get-Process cloudflared -ErrorAction SilentlyContinue).Count
# Expected: 1

# All endpoints working (run 10x each)
1..10 | ForEach-Object {
    $code = (Invoke-WebRequest -Uri "https://yamil-ai.com" -UseBasicParsing -ErrorAction SilentlyContinue).StatusCode
    Write-Host -NoNewline "$code "
}
Write-Host ""
# Expected: all 200

1..10 | ForEach-Object {
    $code = (Invoke-WebRequest -Uri "https://api.yamil-ai.com" -UseBasicParsing -ErrorAction SilentlyContinue).StatusCode
    Write-Host -NoNewline "$code "
}
Write-Host ""
# Expected: all 200 or 404 (depending on API path)
```

### 6b. SSH reachable from MacBook

After completing this guide, go back to the **MacBook** and verify:
```bash
ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_ed25519 <username>@192.168.0.101 "echo connected from MacBook"
```

### 6c. AWS secrets

```powershell
# List all homelab secrets
aws secretsmanager list-secrets --query "SecretList[?starts_with(Name, 'yamil/homelab/')].Name" --output table
```

Expected secrets:
| Secret | What |
|--------|------|
| `yamil/homelab/qnap-ssh` | QNAP NAS SSH (key + password) |
| `yamil/homelab/qnap-admin` | QNAP QTS web UI login |
| `yamil/homelab/windows-pc` | Windows PC SSH (created by this guide) |
| `yamil/homelab/cloudflare-access` | Cloudflare dashboard login |
| `yamil/homelab/grafana` | Grafana admin login |
| `yamil/homelab/omada-controller` | Omada SDN controller login |
| `yamil/homelab/er707-m2` | ER707-M2 router login |
| `yamil/homelab/vpn-l2tp` | L2TP VPN config |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `cloudflared` not found | Install: `winget install Cloudflare.cloudflared` |
| Multiple services registered | `cloudflared service uninstall` then re-install once |
| Docker cloudflared keeps restarting | Check `docker logs cloudflared --tail 20` |
| SSH connection refused | Verify `sshd` service is running: `Get-Service sshd` |
| SSH permission denied | Check `administrators_authorized_keys` permissions with `icacls` |
| AWS CLI not configured | `aws configure` with access key, region `us-east-1` |
| 502 persists after fix | Check `docker ps` on port 9080 — Envoy must be running |

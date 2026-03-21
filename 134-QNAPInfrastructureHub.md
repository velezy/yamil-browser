# 134 — QNAP Infrastructure Hub

**Status**: Production
**Created**: 2026-03-20
**QNAP**: 192.168.0.102 (FridayAI / TS-251+), SSH port 2222

---

## Phase 6: Log Shipping — Promtail → Loki ✅ DONE

**What**: Windows PC Docker logs ship to QNAP Loki via Promtail.

| Component | Location | Details |
|-----------|----------|---------|
| Promtail | Windows PC (Docker container) | Discovers all Docker containers, ships logs |
| Loki | QNAP :3100 | Log aggregation and storage |
| Grafana | QNAP :3000 | Log visualization |

**Config**: `C:\project\yamil-browser\promtail\promtail-config.yml`

```yaml
clients:
  - url: http://192.168.0.102:3100/loki/api/v1/push
```

**Label**: `host=windows-server` — all logs tagged with this label for Loki queries.

**Fix applied (2026-03-20)**: Updated Promtail config from old QNAP IP (`192.168.1.188`) to new IP (`192.168.0.102`). Restarted container. Logs now flowing successfully.

**Verification**: `curl -s "http://192.168.0.102:3100/loki/api/v1/query?query={host=\"windows-server\"}&limit=3"` returns recent logs.

---

## Phase 9: Git Mirror Cron ✅ DONE

**What**: Bare git mirror clones on QNAP with hourly fetch as a safety net.

| Item | Details |
|------|---------|
| Script | `/share/Container/git-mirrors/git-mirror.sh` |
| Log | `/share/Container/git-mirrors/mirror.log` |
| Cron | `0 * * * *` (hourly) |
| Docker image | `alpine/git:latest` (QNAP has no native git) |

### Repos

| Repo | Status | Notes |
|------|--------|-------|
| `velezy/yamil-browser` | ✅ Mirrored | Public repo, works without PAT |
| `velezy/parser_lite` | ✅ Mirrored | Private repo, uses PAT |
| `velezy/Ai-Tools` | ✅ Mirrored | Private repo, uses PAT |

### GitHub PAT

- **Token name**: `qnap-mirror` (fine-grained, Contents: Read-only)
- **Expires**: April 19, 2026
- **Saved to**: `/share/Container/git-mirrors/.github-pat` (chmod 600)
- **Scoped to**: `parser_lite` and `Ai-Tools` repos only

### How it works

- First run: `git clone --mirror` creates bare repos
- Subsequent runs: `git fetch --all --prune` updates existing mirrors
- Success/failure sent to ntfy (`yamil-alerts` topic)
- Only sends success notification at midnight (avoids hourly spam)

---

## Phase C4: Docker Volume Backups ✅ DONE

**What**: Daily backup of stateful Docker volumes from Windows PC to QNAP storage.

| Item | Details |
|------|---------|
| Script | `/share/Container/volume-backups/volume-backup.sh` |
| Log | `/share/Container/volume-backups/backup.log` |
| Cron | `0 3 * * *` (daily at 3am) |
| Retention | 30 days |
| SSH key | `/root/.ssh/id_ed25519_windows` (QNAP → Windows PC) |

### Volumes backed up

| Volume | Size | Contents |
|--------|------|----------|
| `logic-weaver_vault-data` | ~45K | Vault encrypted secrets (critical) |
| `logic-weaver_vault-logs` | ~888K | Vault audit logs |
| `logic-weaver_redis-data` | ~6.9M | Redis AOF/RDB data |
| `mb-redis-data` | ~2.8K | MemoByte Redis data |
| `mb-etcd-data` | ~1.6M | APISIX etcd configuration |

### How it works

1. SSHes from QNAP → Windows PC using ed25519 key
2. For each volume: `docker run --rm -v <vol>:/data busybox tar cf - /data` piped over SSH
3. Gzipped and saved as `<volume>_YYYYMMDD.tar.gz`
4. Old backups (>30 days) auto-deleted
5. Success/failure sent to ntfy (`yamil-alerts` topic)

### File naming

```
/share/Container/volume-backups/
  logic-weaver_vault-data_20260320.tar.gz
  logic-weaver_vault-logs_20260320.tar.gz
  logic-weaver_redis-data_20260320.tar.gz
  mb-redis-data_20260320.tar.gz
  mb-etcd-data_20260320.tar.gz
  volume-backup.sh
  backup.log
```

### Prerequisites

- `busybox:latest` Docker image must be pulled on the Windows PC
- Docker Desktop's `credsStore: desktop` prevents pulling images via SSH — must pull busybox interactively first
- SSH key from AWS SM `yamil/homelab/windows-pc` deployed to `/root/.ssh/id_ed25519_windows` on QNAP

---

## QNAP Cron Jobs (Custom)

```
0 * * * * /bin/bash /share/Container/git-mirrors/git-mirror.sh >> /share/Container/git-mirrors/mirror.log 2>&1
0 3 * * * /bin/bash /share/Container/volume-backups/volume-backup.sh >> /share/Container/volume-backups/backup.log 2>&1
```

**Note**: QNAP cron is managed via `/etc/config/crontab` and reloaded with `crontab /etc/config/crontab`. QNAP may reset crontab on firmware updates — verify after any QTS update.

---

## QNAP Directory Layout

```
/share/Container/
├── yamil-monitor/          # Docker Compose stack (Uptime Kuma, Grafana, Loki, Prometheus, ntfy)
├── git-mirrors/            # Bare git mirror clones
│   ├── git-mirror.sh
│   ├── mirror.log
│   ├── .github-pat         # (create this for private repos)
│   └── yamil-browser.git/  # Bare clone
└── volume-backups/         # Daily Docker volume backups from Windows PC
    ├── volume-backup.sh
    ├── backup.log
    └── *_YYYYMMDD.tar.gz   # Backup files (30-day retention)
```

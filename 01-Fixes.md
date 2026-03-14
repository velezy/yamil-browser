# Fixes Log

## 2026-03-13 — Grafana SQLite Disk I/O Errors on QNAP

**Error**: `migration failed: disk I/O error: no such file or directory` — Grafana repeatedly crashed during SQLite migrations after container restarts.

**Root Cause**: Docker's overlay filesystem on QNAP (Container Station) doesn't handle SQLite's write patterns correctly. Named Docker volumes use the overlay driver, which caused I/O errors during the 710+ database migrations.

**Solution**: Changed Grafana's data volume from a Docker named volume to a bind mount in `docker-compose.yml`:
```yaml
# Before (broken):
- grafana-data:/var/lib/grafana

# After (working):
- ./grafana-data:/var/lib/grafana
```

The bind mount writes directly to QNAP's ext4 filesystem (`/share/Container/yamil-monitor/grafana-data/`), bypassing Docker's overlay.

**Side Effects**:
- Fresh Grafana database — provisioned datasources (Prometheus, Loki) auto-recreate, but manually created dashboards are lost
- Password reset to `Ashley2029` (from `GF_SECURITY_ADMIN_PASSWORD` env var on fresh init)

**Also Fixed**: Account lockout from too many failed login attempts was cleared by container restart.

**Note**: If other QNAP Docker services using SQLite (Uptime Kuma, etc.) have similar issues, switching them to bind mounts would be the fix.

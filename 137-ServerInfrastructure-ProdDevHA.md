# 137 — Server Infrastructure: Production, Dev & HA

**Status**: Planning
**Created**: 2026-03-12
**Philosophy**: Develop on Mac Mini, push to Production (Dark-Knight), GEEKOM as HA failover. Git-based deployment across all three.

---

## 1. The Problem

Everything runs on one machine (Dark-Knight) with test config only. No separation between dev and prod, no failover, no CI/CD pipeline. If Dark-Knight goes down, YAMIL Browser and all services go offline.

## 2. Server Roles

| Server | Role | IP | Specs | OS |
|--------|------|----|-------|----|
| **Dark-Knight** (Windows PC) | Production | 192.168.0.101 | Ryzen 9 3900X, 64GB DDR4, 1TB SSD | Windows 11 + Docker Desktop |
| **GEEKOM A8 Max** | HA / Failover | TBD (192.168.0.103) | Ryzen 9 8945HS, 32GB DDR5, 1TB SSD, dual 2.5G LAN | Ubuntu Server 24.04 LTS |
| **Mac Mini M4** | Development | 192.168.0.104 | M4, 16GB, 256GB + 2TB Samsung 990 EVO (USB4) | macOS |

### Why This Layout

- **GEEKOM as HA** (not Dev): 32GB RAM, dual 2.5G LAN, native Linux Docker (no VM overhead), 1TB internal SSD for DB replica — best match for a production-mirror standby server
- **Mac Mini as Dev** (not HA): Great macOS IDE experience, 16GB fine for dev/test, 1G LAN doesn't matter for development, display support for coding

## 3. What Runs Where

### Production (Dark-Knight)
- YAMIL Browser (standard ports: 443, 5432, 6379, 9080)
- Nginx reverse proxy + Certbot TLS
- PostgreSQL + pgvector (production data)
- Ollama (qwen3:8b, nomic-embed-text)
- `docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d`

### Development (Mac Mini)
- YAMIL Browser (offset ports: 4000, 5433, 6380)
- Same docker-compose.yml (test config)
- Ollama (local models for testing)
- IDE / code editing environment
- `docker compose up -d`

### HA Failover (GEEKOM)
- Mirror of Production stack (standby)
- DB replication from Dark-Knight (PostgreSQL streaming replica)
- Auto-promotion if Dark-Knight goes down
- Uptime Kuma monitors Dark-Knight → triggers failover script
- `docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d`

## 4. Deployment Flow

```
Mac Mini (Dev) ──git push──> GitHub ──git pull──> Dark-Knight (Prod)
                                     ──git pull──> GEEKOM (HA)
```

### Git-Based Workflow
1. Develop & test on Mac Mini
2. `git push` to GitHub (main branch)
3. On Dark-Knight: `git pull && docker compose ... up -d --build`
4. On GEEKOM: `git pull && docker compose ... up -d --build`
5. (Future) Webhook auto-deploy on push

## 5. Files to Create

### docker-compose.prod.yml (overlay)
```yaml
services:
  browser-service:
    ports:
      - "9080:4000"      # Direct access (internal)
    environment:
      - DATABASE_URL=postgresql://${DB_USER}:${DB_PASS}@browser-db:5432/${DB_NAME}
      - OLLAMA_URL=http://host.docker.internal:11434
      - NODE_ENV=production

  browser-db:
    ports:
      - "127.0.0.1:5432:5432"
    environment:
      - POSTGRES_DB=${DB_NAME}
      - POSTGRES_USER=${DB_USER}
      - POSTGRES_PASSWORD=${DB_PASS}

  nginx:
    image: nginx:alpine
    container_name: yamil-browser-nginx
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certbot/conf:/etc/letsencrypt:ro
      - ./certbot/www:/var/www/certbot:ro
    depends_on:
      - browser-service
    restart: unless-stopped

  certbot:
    image: certbot/certbot
    container_name: yamil-browser-certbot
    volumes:
      - ./certbot/conf:/etc/letsencrypt
      - ./certbot/www:/var/www/certbot
```

### .env.prod (secrets — NOT in git)
```
DB_NAME=yamil_browser
DB_USER=yamil_prod
DB_PASS=<strong-random-password>
OLLAMA_URL=http://host.docker.internal:11434
```

## 6. Implementation Order

- [ ] Phase 1: Set up GEEKOM — install Ubuntu Server 24.04 LTS, Docker, clone repo
- [ ] Phase 2: Configure GEEKOM static IP (192.168.0.103) on Omada Controller
- [ ] Phase 3: Install Ollama on GEEKOM, pull models (qwen3:8b, nomic-embed-text)
- [ ] Phase 4: Create `docker-compose.prod.yml` overlay
- [ ] Phase 5: Create `.env.prod` with production secrets
- [ ] Phase 6: Create `nginx/nginx.conf` with reverse proxy + TLS config
- [ ] Phase 7: Deploy production stack on Dark-Knight
- [ ] Phase 8: Test production (HTTPS, standard ports, knowledge pipeline)
- [ ] Phase 9: Deploy HA stack on GEEKOM (same prod config, standby mode)
- [ ] Phase 10: Configure PostgreSQL streaming replication (Dark-Knight → GEEKOM)
- [ ] Phase 11: Create failover script (promote GEEKOM replica, swap DNS/IP)
- [ ] Phase 12: Add Uptime Kuma monitors for failover detection
- [ ] Phase 13: Test failover — stop Dark-Knight, verify GEEKOM takes over
- [ ] Phase 14: Set up Mac Mini — install Docker Desktop, clone repo, verify dev stack
- [ ] Phase 15: (Future) GitHub webhook for auto-deploy on push

## 7. Network Config (on ER707-M2 / Omada)

- Dark-Knight: 192.168.0.101 (already set)
- QNAP FridayAI: 192.168.0.102 (Omada Controller + monitoring)
- GEEKOM: Static IP reservation → 192.168.0.103
- Mac Mini: Static IP reservation → 192.168.0.104
- All on Default VLAN (VLAN 1)

## 8. HA Strategy

### PostgreSQL Replication
- Dark-Knight (primary) → GEEKOM (standby replica)
- Streaming replication with `pg_basebackup`
- Replica is read-only until promoted
- RPO: ~0 (synchronous optional), RTO: <2 minutes
- GEEKOM's dual 2.5G LAN: port 1 for traffic, port 2 dedicated to replication (optional)

### Failover Trigger
- Uptime Kuma on QNAP (192.168.0.102:3001) monitors Dark-Knight:443
- If down for >60 seconds → execute failover script on GEEKOM
- Failover script:
  1. Promote PostgreSQL replica to primary
  2. Start full production stack
  3. Update DNS / floating IP (or Nginx upstream on QNAP)
  4. Send notification (Grafana alert)

### Failback
- Once Dark-Knight is back, re-sync DB from GEEKOM
- Reverse replication direction
- Switch traffic back to Dark-Knight

## 9. Rollback Plan

- Mac Mini is dev only — nothing breaks if it goes down
- GEEKOM failover is optional — system works without it (just no HA)
- If production deploy fails on Dark-Knight, roll back with `git checkout` + `docker compose up -d`
- Test config (current) always works as baseline

## 10. Notes

- GEEKOM has dual 2.5G LAN — port 1 for network, port 2 for replication or bond for 5G
- GEEKOM on native Linux Docker = no VM overhead, ideal for HA mirror
- Mac Mini is 1G only — fine for development
- Samsung 990 EVO 2TB via USB4 gives Mac Mini plenty of dev workspace
- Dark-Knight has 64GB RAM — can run production + Ollama comfortably
- GEEKOM has 32GB DDR5 — matches production workload capacity for seamless failover
- All servers should have SSH key auth configured (no passwords)
- Consider Tailscale/WireGuard mesh between servers for secure internal comms

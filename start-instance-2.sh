#!/bin/bash
# =============================================================================
# YAMIL Browser — Start Instance 2
# Launches a second browser-service (port 4001) + Electron app (port 9301)
# that shares the same AI backends (chat, RAG, Ollama, DB, Redis).
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Starting YAMIL Browser instance 2..."

# Start browser-service-2 via the multi-instance profile
cd "$SCRIPT_DIR"
docker compose --profile multi-instance up -d browser-service-2

# Wait for browser-service-2 health
echo "Waiting for browser-service-2 on port 4001..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:4001/health > /dev/null 2>&1; then
        echo "browser-service-2 healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Warning: browser-service-2 not responding after 30s, starting Electron anyway"
    fi
    sleep 1
done

# Launch Electron with instance-2 ports and isolated user data
echo "Starting YAMIL Browser (instance 2) on port 9301..."
cd "$SCRIPT_DIR/electron-app"
CTRL_PORT=9301 \
BROWSER_SERVICE=http://127.0.0.1:4001 \
AI_ENDPOINT=http://localhost:8020/browser-chat \
APP_TITLE="YAMIL Browser (2)" \
npx electron . --user-data-dir="$HOME/.yamil-browser-instance-2"

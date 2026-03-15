#!/bin/bash
# =============================================================================
# YAMIL Browser — Start Everything
# Starts Docker services (chat, RAG, browser-service, postgres, redis)
# then launches the Electron desktop app.
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Starting YAMIL Browser services..."

# Start Docker containers
cd "$SCRIPT_DIR"
docker compose up -d

# Wait for chat service health
echo "Waiting for chat service..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8020/health > /dev/null 2>&1; then
        echo "Chat service healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Warning: Chat service not responding after 30s, starting browser anyway"
    fi
    sleep 1
done

# Wait for RAG service health
echo "Waiting for RAG service..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8022/health > /dev/null 2>&1; then
        echo "RAG service healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Warning: RAG service not responding after 30s, starting browser anyway"
    fi
    sleep 1
done

# Launch Electron app
echo "Starting YAMIL Browser..."
cd "$SCRIPT_DIR/electron-app"
AI_ENDPOINT=http://localhost:8020/browser-chat npm start

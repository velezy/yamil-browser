#!/usr/bin/env bash
# Launch YAMIL Browser pointed at the YAMIL AI Builder orchestrator
#
#  yamil-browser REST API   → port 4000 (YAMIL default)
#  YAMIL orchestra chat     → port 8015 (direct, local dev)
#  For Docker-hosted YAMIL: AI_ENDPOINT=http://localhost:9080/api/v1/builder-orchestra/browser-chat
export BROWSER_SERVICE_URL=${BROWSER_SERVICE_URL:-http://localhost:4000}
export AI_ENDPOINT=${AI_ENDPOINT:-http://localhost:8015/api/v1/builder-orchestra/browser-chat}
export APP_TITLE="YAMIL Browser"
npx electron .

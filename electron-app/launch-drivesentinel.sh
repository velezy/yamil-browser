#!/usr/bin/env bash
# Launch YAMIL Browser pointed at the DriveSentinel orchestrator
#
#  yamil-browser REST API  → port 4001 (DS uses 4001, YAMIL uses 4000 — no conflict)
#  DS orchestrator chat    → port 17003 (direct, local dev)
#  For Docker-hosted DS:   BROWSER_SERVICE_URL=http://localhost:4001
#                          AI_ENDPOINT=http://localhost:3078/browser-chat
export BROWSER_SERVICE_URL=${BROWSER_SERVICE_URL:-http://localhost:4001}
export AI_ENDPOINT=${AI_ENDPOINT:-http://localhost:17003/browser-chat}
export APP_TITLE="DriveSentinel Browser"
npx electron .

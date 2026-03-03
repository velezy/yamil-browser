#!/usr/bin/env bash
# Launch YAMIL Browser pointed at the YAMIL orchestrator
export BROWSER_SERVICE_URL=http://localhost:4000
export AI_ENDPOINT=http://localhost:8003/api/v1/builder-orchestra/browser-chat
export APP_TITLE="YAMIL Browser"
npx electron .

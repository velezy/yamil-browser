#!/usr/bin/env bash
# Launch YAMIL Browser pointed at the Memobytes app
export BROWSER_SERVICE_URL=http://localhost:4001
export AI_ENDPOINT=http://localhost:8347/api/ai/browser-chat
export APP_TITLE="Memobytes Browser"
npx electron .

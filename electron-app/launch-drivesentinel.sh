#!/usr/bin/env bash
# Launch YAMIL Browser pointed at the DriveSentinel orchestrator
export BROWSER_SERVICE_URL=http://localhost:4000
export AI_ENDPOINT=http://localhost:17003/browser-chat
export APP_TITLE="DriveSentinel Browser"
npx electron .

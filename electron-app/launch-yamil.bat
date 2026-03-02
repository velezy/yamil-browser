@echo off
REM Launch YAMIL Browser pointed at the YAMIL orchestrator
set BROWSER_SERVICE_URL=http://localhost:4000
set AI_ENDPOINT=http://localhost:8003/api/ai/chat
set APP_TITLE=YAMIL Browser
npx electron .

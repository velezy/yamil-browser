@echo off
REM Launch YAMIL Browser pointed at the YAMIL orchestrator
set BROWSER_SERVICE_URL=http://localhost:4000
set AI_ENDPOINT=http://localhost:8015/api/v1/builder-orchestra/browser-chat
set APP_TITLE=YAMIL Browser
npx electron .

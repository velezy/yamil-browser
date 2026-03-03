@echo off
REM Launch YAMIL Browser pointed at the YAMIL AI Builder orchestrator
REM
REM  yamil-browser REST API   → port 4000 (YAMIL default)
REM  YAMIL orchestra chat     → port 9080 (Docker stack, via envoy/APISIX)
REM  For direct local dev (no Docker): set AI_ENDPOINT=http://localhost:8015/api/v1/builder-orchestra/browser-chat
set BROWSER_SERVICE_URL=http://localhost:4000
set AI_ENDPOINT=http://localhost:9080/api/v1/builder-orchestra/browser-chat
set APP_TITLE=YAMIL Browser
npx electron .

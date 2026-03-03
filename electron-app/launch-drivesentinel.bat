@echo off
REM Launch YAMIL Browser pointed at the DriveSentinel orchestrator
REM
REM  yamil-browser REST API  → port 4001 (DS uses 4001, YAMIL uses 4000 — no conflict)
REM  DS orchestrator chat    → port 17003 (direct, local dev)
REM  For Docker-hosted DS:   set BROWSER_SERVICE_URL=http://localhost:4001
REM                          set AI_ENDPOINT=http://localhost:3078/browser-chat
set BROWSER_SERVICE_URL=http://localhost:4001
set AI_ENDPOINT=http://localhost:17003/browser-chat
set APP_TITLE=DriveSentinel Browser
npx electron .

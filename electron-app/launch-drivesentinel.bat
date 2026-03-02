@echo off
REM Launch YAMIL Browser pointed at the DriveSentinel orchestrator
set BROWSER_SERVICE_URL=http://localhost:4000
set AI_ENDPOINT=http://localhost:17003/api/browser/chat
set APP_TITLE=DriveSentinel Browser
npx electron .

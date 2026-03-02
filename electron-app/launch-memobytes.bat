@echo off
REM Launch YAMIL Browser pointed at the Memobytes app
set BROWSER_SERVICE_URL=http://localhost:4001
set AI_ENDPOINT=http://localhost:8347/api/ai/browser-chat
set APP_TITLE=Memobytes Browser
npx electron .

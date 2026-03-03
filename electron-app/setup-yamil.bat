@echo off
REM ╔══════════════════════════════════════════════════════════════════╗
REM ║  YAMIL Browser — First-time setup (run once per machine)        ║
REM ║                                                                  ║
REM ║  1. Adds app to Windows Startup (starts minimized to tray)      ║
REM ║  2. Starts the app NOW → app self-registers yamil-browser://    ║
REM ║     The protocol registration happens inside the Electron app   ║
REM ║     via app.setAsDefaultProtocolClient — this is correct.       ║
REM ╚══════════════════════════════════════════════════════════════════╝

setlocal EnableDelayedExpansion

set "DIR=%~dp0"
if "%DIR:~-1%"=="\" set "DIR=%DIR:~0,-1%"

set "ELECTRON=%DIR%\node_modules\.bin\electron.cmd"
set "STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"

echo.
echo [1/2] Installing startup entry (starts minimized to tray on login)...
(
  echo @echo off
  echo set BROWSER_SERVICE_URL=http://localhost:4000
  echo set AI_ENDPOINT=http://localhost:9080/api/v1/builder-orchestra/browser-chat
  echo set APP_TITLE=YAMIL Browser
  echo cd /d "%DIR%"
  echo start "" "%ELECTRON%" "." --minimized
) > "%STARTUP%\yamil-browser-startup.bat"
echo     Written to: %STARTUP%\yamil-browser-startup.bat

echo.
echo [2/2] Starting YAMIL Browser (this also registers yamil-browser:// protocol)...
set BROWSER_SERVICE_URL=http://localhost:4000
set AI_ENDPOINT=http://localhost:9080/api/v1/builder-orchestra/browser-chat
set APP_TITLE=YAMIL Browser
start "" "%ELECTRON%" "%DIR%"

echo.
echo ================================================================
echo  Setup complete!
echo.
echo  - Startup entry created (auto-starts on next Windows login)
echo  - App is starting now — yamil-browser:// registers on launch
echo  - Port 9300 will be available in a few seconds
echo.
echo  In YAMIL web UI: switch to Local mode and click Desktop button.
echo ================================================================
echo.
pause

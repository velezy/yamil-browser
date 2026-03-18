#!/bin/bash
# Debug wrapper — logs everything, then execs the real server
LOG="/tmp/yamil-mcp-debug.log"
echo "=== MCP WRAPPER START ===" >> "$LOG"
echo "TIME: $(date)" >> "$LOG"
echo "CWD: $(pwd)" >> "$LOG"
echo "NODE: /opt/homebrew/opt/node@22/bin/node" >> "$LOG"
echo "NODE_VERSION: $(/opt/homebrew/opt/node@22/bin/node --version)" >> "$LOG"
echo "YAMIL_BROWSER_URL: $YAMIL_BROWSER_URL" >> "$LOG"
echo "YAMIL_CTRL_URL: $YAMIL_CTRL_URL" >> "$LOG"
echo "PATH: $PATH" >> "$LOG"
echo "=== LAUNCHING ===" >> "$LOG"
exec /opt/homebrew/opt/node@22/bin/node mcp-server/src/index.mjs 2>>"$LOG"

/**
 * YAMIL Browser MCP Server — Entry Point
 *
 * AI-First UX: The browser is the AI's eyes and hands.
 * This file wires together all modules and registers 45 yamil_browser_* tools.
 */

// ── Global crash protection ─────────────────────────────────────────
// Prevent unhandled errors from killing the MCP server process,
// which would crash Claude Code entirely.
process.on("uncaughtException", (err) => {
  const msg = err?.message || String(err);
  console.error("[YAMIL MCP] Uncaught exception (caught by global handler):", msg);
  try { logToolError("__uncaughtException__", {}, msg, ""); } catch (_) {}
});
process.on("unhandledRejection", (reason) => {
  const msg = reason?.message || String(reason);
  console.error("[YAMIL MCP] Unhandled rejection (caught by global handler):", msg);
  try { logToolError("__unhandledRejection__", {}, msg, ""); } catch (_) {}
});

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { readFileSync } from "fs";
import { join } from "path";

// ── Utils ────────────────────────────────────────────────────────────
import { extractJSON } from "./utils/json-parser.mjs";
import { logToolError, PROJECT_ROOT } from "./utils/errors.mjs";
import { YAMIL_OBSERVER_SCRIPT, SELF_HEAL_SCRIPT, createDomHelpers } from "./utils/dom-helpers.mjs";

// ── Services ─────────────────────────────────────────────────────────
import {
  yamilPing, yamilGet, yamilPost, ye, yamilScreenshotBuf, yamilPageUrl,
  logMcpAction, ragLookup, extractDomain,
  YAMIL_CTRL, YAMIL_ELECTRON_DIR, BROWSER_SVC_URL,
  yamilElectronProc, setYamilElectronProc,
} from "./services/browser-client.mjs";

import {
  cacheGet, cacheSet, CACHEABLE_ACTIONS,
  selectorCacheGet, selectorCacheSet,
} from "./services/action-cache.mjs";

import {
  anthropic, isOllamaAvailable, isGeminiAvailable, isBedrockAvailable, getAnthropic,
  OLLAMA_URL, OLLAMA_VISION_MODEL,
} from "./services/llm-chain.mjs";

// ── Providers ────────────────────────────────────────────────────────
import {
  geminiComputerUse, convertCUCoords, buildCUFunctionResponse, createCUExecutor,
} from "./providers/gemini-cu.mjs";

// ── Tools ────────────────────────────────────────────────────────────
import { registerBrowserMgmtTools } from "./tools/browser-mgmt.mjs";
import { registerNavigationTools } from "./tools/navigation.mjs";
import { registerObservationTools } from "./tools/observation.mjs";
import { registerInteractionTools } from "./tools/interaction.mjs";
import { registerA11yTools } from "./tools/a11y.mjs";
import { registerDataTools } from "./tools/data.mjs";
import { registerAiVisionTools } from "./tools/ai-vision.mjs";
import { registerKnowledgeTools } from "./tools/knowledge.mjs";
import { registerCredentialTools } from "./tools/credentials.mjs";
import { registerPdfTools } from "./tools/pdf.mjs";
import { registerLearningTools } from "./tools/learning.mjs";

console.error("[YAMIL MCP] Loaded from: C:/project/yamil-browser/mcp-server/src/index.mjs");

// ── Load env vars from ~/.claude/mcp.json if not already set ─────────
try {
  const mcpPath = join(process.env.USERPROFILE || process.env.HOME || "", ".claude", "mcp.json");
  const mcpConf = JSON.parse(readFileSync(mcpPath, "utf8"));
  const envBlock = mcpConf?.mcpServers?.["playwright-browser"]?.env;
  if (envBlock) {
    for (const [k, v] of Object.entries(envBlock)) {
      if (!process.env[k]) process.env[k] = v;
    }
  }
} catch (_) { /* mcp.json not found or unparseable — ignore */ }

// ── Create bound helpers ─────────────────────────────────────────────
const domHelpers = createDomHelpers(ye);
const executeYamilCUAction = createCUExecutor(ye, yamilPost);

// ── MCP Server ───────────────────────────────────────────────────────
const server = new McpServer({
  name: "yamil-browser",
  version: "1.0.0",
});

// ── Shared dependency object for all tool modules ────────────────────
const deps = {
  // Browser client
  yamilPing, yamilGet, yamilPost, ye, yamilScreenshotBuf, yamilPageUrl,
  logMcpAction, ragLookup, extractDomain,
  YAMIL_CTRL, YAMIL_ELECTRON_DIR, BROWSER_SVC_URL,
  yamilElectronProc, setYamilElectronProc,

  // DOM helpers
  ...domHelpers, // getYamilA11yTree, monacoSetValue, yamilEnsureObserver, yamilWaitForDom
  YAMIL_OBSERVER_SCRIPT, SELF_HEAL_SCRIPT,

  // Caches
  cacheGet, cacheSet, CACHEABLE_ACTIONS,
  selectorCacheGet, selectorCacheSet,

  // LLM chain
  anthropic, isOllamaAvailable, isGeminiAvailable, isBedrockAvailable, getAnthropic,
  OLLAMA_URL, OLLAMA_VISION_MODEL,

  // Providers
  geminiComputerUse, convertCUCoords, buildCUFunctionResponse, executeYamilCUAction,

  // Utils
  extractJSON, logToolError, PROJECT_ROOT,
};

// ── Register all tool groups ─────────────────────────────────────────
registerBrowserMgmtTools(server, deps);
registerNavigationTools(server, deps);
registerObservationTools(server, deps);
registerInteractionTools(server, deps);
registerA11yTools(server, deps);
registerDataTools(server, deps);
registerAiVisionTools(server, deps);
registerKnowledgeTools(server, deps);
registerCredentialTools(server, deps);
registerPdfTools(server, deps);
registerLearningTools(server, deps);

// ── Start server ─────────────────────────────────────────────────────
const transport = new StdioServerTransport();
await server.connect(transport);

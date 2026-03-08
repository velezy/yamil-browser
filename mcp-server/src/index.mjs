import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import Anthropic from "@anthropic-ai/sdk";
import AnthropicBedrock from "@anthropic-ai/bedrock-sdk";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { spawn } from "child_process";
import { readFileSync, appendFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

console.error("[YAMIL MCP] Loaded from: C:/project/yamil-browser/mcp-server/src/index.mjs");

// ── Error logger — writes detection failures to markdown for review ───
const __dirname_mjs = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = join(__dirname_mjs, "..", "..");
const ERROR_LOG_PATH = join(PROJECT_ROOT, "YAMILBrowserErrors.md");
function logToolError(tool, params, error, pageUrl) {
  try {
    const dir = dirname(ERROR_LOG_PATH);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const ts = new Date().toISOString();
    const header = existsSync(ERROR_LOG_PATH) ? "" : "# YAMIL Browser Tool Errors\n\nDetection failures logged automatically for review and improvement.\n\n---\n\n";
    const entry = `${header}### ${ts} — \`${tool}\`\n- **Page**: ${pageUrl || "unknown"}\n- **Params**: \`${JSON.stringify(params)}\`\n- **Error**: ${error}\n\n---\n\n`;
    appendFileSync(ERROR_LOG_PATH, entry, "utf8");
  } catch (_) { /* don't break tools if logging fails */ }
}

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

// ── LLM Provider: Gemini CU → Ollama (local) → Gemini Flash → Bedrock → Anthropic ──
let _gemini = null;
let _anthropic = null;
let _usingBedrock = false;
let _usingGemini = false;
let _ollamaAvailable = false;
const OLLAMA_URL = process.env.OLLAMA_URL || "http://127.0.0.1:11434";
const OLLAMA_VISION_MODEL  = process.env.OLLAMA_VISION_MODEL  || "qwen3-vl:8b";

// Initialize Gemini if key available
if (process.env.GEMINI_API_KEY) {
  _gemini = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
  _usingGemini = true;
}

// Probe Ollama on startup (non-blocking)
fetch(`${OLLAMA_URL}/api/tags`, { signal: AbortSignal.timeout(2000) })
  .then(r => r.ok ? r.json() : null)
  .then(d => {
    if (!d?.models) return;
    const names = d.models.map(m => m.name);
    const hasModel = (target) => names.some(n => n === target || n.startsWith(target.split(":")[0]));
    if (hasModel(OLLAMA_VISION_MODEL)) {
      _ollamaAvailable = true;
      console.error(`[OLLAMA] Vision: "${OLLAMA_VISION_MODEL}" ready`);
    }
  })
  .catch(() => {});

async function ollamaCreate(params) {
  const messages = [];
  for (const msg of params.messages) {
    const content = Array.isArray(msg.content) ? msg.content : [{ type: "text", text: msg.content }];
    let text = "";
    const images = [];
    for (const part of content) {
      if (part.type === "text") text += part.text;
      else if (part.type === "image") images.push(part.source.data);
    }
    messages.push({ role: msg.role, content: text, ...(images.length ? { images } : {}) });
  }
  const res = await fetch(`${OLLAMA_URL}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: OLLAMA_VISION_MODEL, messages, stream: false }),
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`Ollama HTTP ${res.status}`);
  const data = await res.json();
  const text = data.message?.content || "";
  return { content: [{ type: "text", text }] };
}

async function geminiCreate(params) {
  const model = _gemini.getGenerativeModel({ model: "gemini-2.0-flash" });
  const parts = [];
  for (const msg of params.messages) {
    const content = Array.isArray(msg.content) ? msg.content : [{ type: "text", text: msg.content }];
    for (const part of content) {
      if (part.type === "text") {
        parts.push({ text: part.text });
      } else if (part.type === "image") {
        parts.push({ inlineData: { mimeType: part.source.media_type, data: part.source.data } });
      }
    }
  }
  const result = await model.generateContent(parts);
  const text = result.response.text();
  return { content: [{ type: "text", text }] };
}

const BEDROCK_MODELS = {
  "claude-sonnet-4-6":          "us.anthropic.claude-sonnet-4-6",
  "claude-opus-4-6":            "us.anthropic.claude-opus-4-6-v1",
  "claude-haiku-4-5-20251001":  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
  "claude-3-5-sonnet-20241022": "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
  "claude-3-5-haiku-20241022":  "us.anthropic.claude-3-5-haiku-20241022-v1:0",
  "claude-3-7-sonnet-20250219": "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
};
function resolveModel(m) {
  return _usingBedrock ? (BEDROCK_MODELS[m] || `us.anthropic.${m}`) : m;
}

function getAnthropic() {
  if (_anthropic) return _anthropic;
  if (process.env.ANTHROPIC_API_KEY) {
    _anthropic = new Anthropic();
    _usingBedrock = false;
    return _anthropic;
  }
  const akid = process.env.AWS_BEDROCK_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID;
  const skey = process.env.AWS_BEDROCK_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY;
  if (akid && skey) {
    _anthropic = new AnthropicBedrock({ awsAccessKey: akid, awsSecretKey: skey, awsRegion: process.env.AWS_DEFAULT_REGION || "us-east-1" });
    _usingBedrock = true;
    return _anthropic;
  }
  return null;
}

const anthropic = new Proxy({}, {
  get(_, prop) {
    if (prop === "messages") {
      return {
        create: async (params) => {
          if (_ollamaAvailable) {
            try {
              const hasImage = params.messages?.some(m => Array.isArray(m.content) && m.content.some(p => p.type === "image"));
              const imgLen = hasImage ? params.messages.flatMap(m => Array.isArray(m.content) ? m.content.filter(p => p.type === "image").map(p => (p.source?.data || "").length) : []).join(",") : "none";
              console.error(`[OLLAMA] Using ${OLLAMA_VISION_MODEL} (images: ${imgLen})`);
              return await ollamaCreate(params);
            } catch (e) {
              console.error(`[OLLAMA] Failed: ${e.message}`);
            }
          }
          if (_usingGemini && _gemini) {
            try { return await geminiCreate(params); } catch (e) { /* fall through */ }
          }
          const client = getAnthropic();
          if (!client) throw new Error("No LLM available — run Ollama, set GEMINI_API_KEY, ANTHROPIC_API_KEY, or AWS_BEDROCK_ACCESS_KEY_ID.");
          return client.messages.create({ ...params, model: resolveModel(params.model) });
        }
      };
    }
    const client = getAnthropic();
    if (!client) throw new Error("No LLM available.");
    return client[prop];
  }
});

// ── Gemini Computer Use API ─────────────────────────────────────────
const CU_MODEL = "gemini-2.5-computer-use-preview-10-2025";
const CU_ENDPOINT = `https://generativelanguage.googleapis.com/v1beta/models/${CU_MODEL}:generateContent`;

async function geminiComputerUse(screenshotBase64, instruction, history = []) {
  if (!process.env.GEMINI_API_KEY) return null;
  const contents = [
    ...history,
    { role: "user", parts: [
      { text: instruction },
      { inline_data: { mime_type: "image/png", data: screenshotBase64 } },
    ]},
  ];
  const res = await fetch(`${CU_ENDPOINT}?key=${process.env.GEMINI_API_KEY}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents,
      tools: [{ computer_use: { environment: "ENVIRONMENT_BROWSER" } }],
    }),
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    console.error(`[CU] Gemini CU error ${res.status}: ${errText.slice(0, 200)}`);
    return null;
  }
  const data = await res.json();
  const candidate = data.candidates?.[0]?.content;
  if (!candidate) return null;
  let action = null, args = null, reasoning = "", safetyDecision = null;
  for (const part of candidate.parts || []) {
    if (part.text) reasoning += part.text;
    if (part.function_call) {
      action = part.function_call.name;
      args = part.function_call.args || {};
      safetyDecision = part.function_call.safety_decision?.decision || null;
    }
  }
  return { action, args, reasoning, safetyDecision, _rawParts: candidate.parts };
}

function convertCUCoords(x, y, viewport) {
  return {
    px: Math.round((x / 1000) * viewport.width),
    py: Math.round((y / 1000) * viewport.height),
  };
}

function buildCUFunctionResponse(actionName, screenshotBase64, url, safetyAck = false) {
  const response = { url };
  if (safetyAck) response.safety_acknowledgement = "true";
  return {
    role: "user",
    parts: [{
      function_response: {
        name: actionName,
        response,
        parts: [{ inline_data: { mime_type: "image/png", data: screenshotBase64 } }],
      },
    }],
  };
}

async function executeYamilCUAction(action, args) {
  const vp = await ye("({ width: window.innerWidth, height: window.innerHeight })") || { width: 1440, height: 900 };
  try {
    switch (action) {
      case "click_at": {
        const { px, py } = convertCUCoords(args.x, args.y, vp);
        await ye(`(function(x,y){
          const el = document.elementFromPoint(x,y);
          if (!el) return;
          el.scrollIntoView({ block: "center", behavior: "instant" });
          el.dispatchEvent(new PointerEvent("pointerdown", { bubbles:true, clientX:x, clientY:y }));
          el.dispatchEvent(new MouseEvent("mousedown", { bubbles:true, clientX:x, clientY:y }));
          el.dispatchEvent(new MouseEvent("mouseup", { bubbles:true, clientX:x, clientY:y }));
          el.dispatchEvent(new MouseEvent("click", { bubbles:true, clientX:x, clientY:y }));
        })(${px},${py})`);
        return { ok: true, text: `Clicked at (${px},${py})` };
      }
      case "type_text_at": {
        const { px, py } = convertCUCoords(args.x, args.y, vp);
        const text = args.text || "";
        const clearFirst = args.clear_before_typing || false;
        const pressEnter = args.press_enter || false;
        await ye(`(function(x,y,txt,clear,enter){
          const el = document.elementFromPoint(x,y);
          if (!el) return;
          el.focus();
          if (clear) {
            const proto = el.tagName==="TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
            const setter = Object.getOwnPropertyDescriptor(proto,"value")?.set;
            if (setter) setter.call(el,""); else el.value="";
          }
          const proto = el.tagName==="TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto,"value")?.set;
          if (setter) setter.call(el, (clear ? "" : el.value) + txt); else el.value += txt;
          el.dispatchEvent(new InputEvent("input",{bubbles:true,inputType:"insertText"}));
          el.dispatchEvent(new Event("change",{bubbles:true}));
          if (enter) el.dispatchEvent(new KeyboardEvent("keydown",{key:"Enter",code:"Enter",bubbles:true}));
        })(${px},${py},${JSON.stringify(text)},${clearFirst},${pressEnter})`);
        return { ok: true, text: `Typed "${text.slice(0, 40)}" at (${px},${py})` };
      }
      case "scroll_document": {
        const dir = args.direction || "down";
        const amt = dir === "down" ? 500 : dir === "up" ? -500 : 0;
        const amtH = dir === "right" ? 500 : dir === "left" ? -500 : 0;
        await ye(`window.scrollBy(${amtH},${amt})`);
        return { ok: true, text: `Scrolled document ${dir}` };
      }
      case "scroll_at": {
        const { px, py } = convertCUCoords(args.x, args.y, vp);
        const dir = args.direction || "down";
        const mag = args.magnitude || 800;
        const amt = (dir === "down" || dir === "right") ? mag : -mag;
        await ye(`(function(x,y,v,h){
          const el = document.elementFromPoint(x,y) || document;
          el.scrollBy ? el.scrollBy(h,v) : window.scrollBy(h,v);
        })(${px},${py},${dir==="up"||dir==="down"?amt:0},${dir==="left"||dir==="right"?amt:0})`);
        return { ok: true, text: `Scrolled ${dir} at (${px},${py})` };
      }
      case "key_combination": {
        const keys = args.keys || "";
        await ye(`(function(k){
          const el = document.activeElement || document.body;
          const parts = k.split("+");
          const key = parts[parts.length-1];
          const ev = { key, code: key, bubbles:true, cancelable:true,
            ctrlKey: parts.includes("Control"), shiftKey: parts.includes("Shift"),
            altKey: parts.includes("Alt"), metaKey: parts.includes("Meta") };
          el.dispatchEvent(new KeyboardEvent("keydown", ev));
          el.dispatchEvent(new KeyboardEvent("keyup", ev));
        })(${JSON.stringify(keys)})`);
        return { ok: true, text: `Pressed keys: ${keys}` };
      }
      case "hover_at": {
        const { px, py } = convertCUCoords(args.x, args.y, vp);
        await ye(`(function(x,y){
          const el = document.elementFromPoint(x,y);
          if (!el) return;
          ["mouseover","mouseenter","mousemove"].forEach(t =>
            el.dispatchEvent(new MouseEvent(t, { bubbles:true, clientX:x, clientY:y })));
        })(${px},${py})`);
        return { ok: true, text: `Hovered at (${px},${py})` };
      }
      case "navigate":
        await yamilPost("/navigate", { url: args.url });
        return { ok: true, text: `Navigated to ${args.url}` };
      case "go_back":
        await ye("history.back()");
        return { ok: true, text: "Went back" };
      case "go_forward":
        await ye("history.forward()");
        return { ok: true, text: "Went forward" };
      case "wait_5_seconds":
        await new Promise(r => setTimeout(r, 5000));
        return { ok: true, text: "Waited 5 seconds" };
      case "drag_and_drop": {
        const src = convertCUCoords(args.x, args.y, vp);
        const dst = convertCUCoords(args.destination_x, args.destination_y, vp);
        await ye(`(function(sx,sy,dx,dy){
          const el = document.elementFromPoint(sx,sy);
          if (!el) return;
          el.dispatchEvent(new MouseEvent("mousedown",{bubbles:true,clientX:sx,clientY:sy}));
          el.dispatchEvent(new MouseEvent("mousemove",{bubbles:true,clientX:dx,clientY:dy}));
          el.dispatchEvent(new MouseEvent("mouseup",{bubbles:true,clientX:dx,clientY:dy}));
          el.dispatchEvent(new DragEvent("drop",{bubbles:true,clientX:dx,clientY:dy}));
        })(${src.px},${src.py},${dst.px},${dst.py})`);
        return { ok: true, text: `Dragged (${src.px},${src.py}) → (${dst.px},${dst.py})` };
      }
      default:
        return { ok: false, text: `Unknown CU action: ${action}` };
    }
  } catch (err) {
    return { ok: false, text: `CU action ${action} failed: ${err.message}` };
  }
}

// ── Accessibility Tree ──────────────────────────────────────────────
async function getYamilA11yTree() {
  try {
    const tree = await ye(`(function(){
      const lines = [];
      function walk(el, depth) {
        if (depth > 8 || lines.length > 500) return;
        const tag = el.tagName?.toLowerCase() || "";
        const role = el.getAttribute?.("role") || "";
        const aria = el.getAttribute?.("aria-label") || "";
        const text = (el.innerText || "").trim().slice(0, 60);
        const val = el.value || "";
        const focused = document.activeElement === el;
        const disabled = el.disabled || el.getAttribute("aria-disabled") === "true";
        if (tag === "script" || tag === "style" || tag === "noscript") return;
        const s = el.getBoundingClientRect?.();
        if (s && s.width === 0 && s.height === 0) return;
        const parts = [role || tag];
        if (aria) parts.push('"' + aria + '"');
        else if (text && text.length < 60) parts.push('"' + text.replace(/"/g, "'") + '"');
        if (val) parts.push('value="' + val.slice(0, 40) + '"');
        if (focused) parts.push("[focused]");
        if (disabled) parts.push("[disabled]");
        if (el.checked) parts.push("[checked]");
        lines.push("  ".repeat(depth) + parts.join(" "));
        for (const child of el.children || []) walk(child, depth + 1);
      }
      walk(document.body, 0);
      return lines.join("\\n");
    })()`);
    if (!tree) return "";
    return tree.length > 10000 ? tree.slice(0, 10000) + "\n...(truncated)" : tree;
  } catch {
    return "";
  }
}

// ── JSON extraction helper ──────────────────────────────────────────
function extractJSON(raw) {
  let cleaned = raw.replace(/<think>[\s\S]*?<\/think>/gi, "").trim();
  cleaned = cleaned.replace(/```json\s*/gi, "").replace(/```\s*/gi, "").trim();
  const start = cleaned.indexOf("{");
  if (start === -1) return null;
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let i = start; i < cleaned.length; i++) {
    const ch = cleaned[i];
    if (esc) { esc = false; continue; }
    if (ch === "\\") { esc = true; continue; }
    if (ch === '"') { inStr = !inStr; continue; }
    if (inStr) continue;
    if (ch === "{") depth++;
    else if (ch === "}") { depth--; if (depth === 0) return cleaned.slice(start, i + 1); }
  }
  const match = cleaned.match(/\{[\s\S]*\}/);
  return match ? match[0] : null;
}

// ── Action Cache ────────────────────────────────────────────────────
const actionCache = new Map();
const CACHE_TTL = 30 * 60 * 1000;
const MAX_CACHE = 500;
const CACHEABLE_ACTIONS = new Set(["click_at", "click", "navigate", "press", "key_combination", "scroll", "scroll_document", "scroll_at", "hover_at", "hover", "select", "go_back", "go_forward"]);

function cacheKey(pageUrl, instruction) {
  try { return new URL(pageUrl).hostname + "|" + instruction.trim().toLowerCase(); }
  catch { return pageUrl + "|" + instruction.trim().toLowerCase(); }
}

function cacheGet(pageUrl, instruction) {
  const key = cacheKey(pageUrl, instruction);
  const entry = actionCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > CACHE_TTL) {
    actionCache.delete(key);
    return null;
  }
  entry.hits++;
  actionCache.delete(key);
  actionCache.set(key, entry);
  return entry;
}

function cacheSet(pageUrl, instruction, action, args) {
  const actionName = action?.action || action;
  if (!CACHEABLE_ACTIONS.has(actionName)) return;
  const key = cacheKey(pageUrl, instruction);
  if (actionCache.size >= MAX_CACHE) {
    const oldest = actionCache.keys().next().value;
    actionCache.delete(oldest);
  }
  actionCache.set(key, { action, args, timestamp: Date.now(), hits: 0 });
}

// ── Selector Auto-Cache ────────────────────────────────────────────
const selectorCache = new Map();
const SELECTOR_CACHE_TTL = 60 * 60 * 1000;
const MAX_SELECTOR_CACHE = 200;

function selectorCacheKey(pageUrl, originalSelector) {
  try { return new URL(pageUrl).hostname + "|" + originalSelector; }
  catch { return pageUrl + "|" + originalSelector; }
}

function selectorCacheGet(pageUrl, originalSelector) {
  const key = selectorCacheKey(pageUrl, originalSelector);
  const entry = selectorCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > SELECTOR_CACHE_TTL) {
    selectorCache.delete(key);
    return null;
  }
  return entry;
}

function selectorCacheSet(pageUrl, originalSelector, healedDesc) {
  const key = selectorCacheKey(pageUrl, originalSelector);
  if (selectorCache.size >= MAX_SELECTOR_CACHE) {
    const oldest = selectorCache.keys().next().value;
    selectorCache.delete(oldest);
  }
  selectorCache.set(key, { healedDesc, timestamp: Date.now() });
}

// ── MCP Server ────────────────────────────────────────────────────────
const server = new McpServer({
  name: "yamil-browser",
  version: "1.0.0",
});

// ═══════════════════════════════════════════════════════════════════════
// ── YAMIL Browser Desktop — MCP tools ─────────────────────────────────
// Controls the native Electron app via its HTTP control server (port 9300)
// ═══════════════════════════════════════════════════════════════════════

const YAMIL_ELECTRON_DIR = join(PROJECT_ROOT, "electron-app");
const YAMIL_CTRL         = "http://127.0.0.1:9300";
const BROWSER_SVC_URL    = process.env.YAMIL_BROWSER_URL || "http://127.0.0.1:4000";

let yamilElectronProc = null;

// ── helpers ───────────────────────────────────────────────────────────

async function yamilPing () {
  try {
    const res = await fetch(`${YAMIL_CTRL}/ping`, { signal: AbortSignal.timeout(1500) });
    if (res.ok) { const d = await res.json(); return d.ok === true; }
  } catch (_) {}
  return false;
}

async function yamilGet (path) {
  const res = await fetch(`${YAMIL_CTRL}${path}`, { signal: AbortSignal.timeout(10000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res;
}

async function yamilPost (path, body = {}) {
  const res = await fetch(`${YAMIL_CTRL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res;
}

async function ye(script) {
  const res  = await yamilPost("/eval", { script });
  const data = await res.json();
  return data.result;
}

async function yamilScreenshotBuf() {
  const res = await yamilGet("/screenshot?quality=40&maxWidth=800&maxBytes=400000&scale=0.5");
  return Buffer.from(await res.arrayBuffer());
}

async function yamilPageUrl() {
  try { const r = await yamilGet("/url").then(r => r.json()); return r.url || "unknown"; } catch { return "unknown"; }
}

// ── Phase 1: MutationObserver backbone ─────────────────────────────────
const YAMIL_OBSERVER_SCRIPT = `(function(){
  if (window.__yamil_observer) return { already: true, version: window.__yamil_snapshot_version };
  let mutCount = 0, lastMutTime = 0, settleTimer = null, settleResolvers = [];
  let summary = { added: 0, removed: 0, attrChanged: 0 };
  window.__yamil_snapshot_version = 1;
  window.__yamil_refs = {};

  const obs = new MutationObserver((mutations) => {
    mutCount += mutations.length;
    lastMutTime = Date.now();
    window.__yamil_snapshot_version++;
    for (const m of mutations) {
      if (m.type === 'childList') { summary.added += m.addedNodes.length; summary.removed += m.removedNodes.length; }
      else if (m.type === 'attributes') summary.attrChanged++;
    }
    if (settleTimer) clearTimeout(settleTimer);
    settleTimer = setTimeout(() => {
      for (const r of settleResolvers) r(true);
      settleResolvers = [];
    }, 300);
  });
  obs.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['class','style','aria-expanded','aria-hidden','data-state','open','hidden'] });

  window.__yamil_observer = obs;
  window.__yamil_dom_settled = function(timeoutMs) {
    timeoutMs = timeoutMs || 3000;
    if (Date.now() - lastMutTime > 300 && mutCount > 0) return Promise.resolve(true);
    return new Promise((resolve) => {
      settleResolvers.push(resolve);
      setTimeout(() => resolve(false), timeoutMs);
    });
  };
  window.__yamil_last_mutations = function() {
    const result = { count: mutCount, summary: {...summary}, settledMs: Date.now() - lastMutTime, version: window.__yamil_snapshot_version };
    mutCount = 0; summary = { added: 0, removed: 0, attrChanged: 0 };
    return result;
  };
  return { injected: true, version: window.__yamil_snapshot_version };
})()`;

async function yamilEnsureObserver() {
  try { return await ye(YAMIL_OBSERVER_SCRIPT); } catch { return null; }
}

async function yamilWaitForDom(timeoutMs = 3000) {
  await yamilEnsureObserver();
  try {
    return await ye(`window.__yamil_dom_settled(${timeoutMs})`);
  } catch { return false; }
}

// ── Phase 5: Self-Healing Selector helper ──────────────────────────────
const SELF_HEAL_SCRIPT = (selector) => `(function(){
  const orig = ${JSON.stringify(selector)};
  const el = document.querySelector(orig);
  if (el) {
    const r = el.getBoundingClientRect();
    if (r.width > 0 || r.height > 0) return { found: true, healed: false };
  }
  const hints = {};
  const idMatch = orig.match(/#([\\w-]+)/);
  if (idMatch) hints.id = idMatch[1];
  const classMatch = orig.match(/\\.([\\w-]+)/g);
  if (classMatch) hints.classes = classMatch.map(c => c.slice(1));
  const attrMatch = orig.match(/\\[(\\w[\\w-]*)(?:=["']?([^"'\\]]+))?/g);
  if (attrMatch) {
    hints.attrs = attrMatch.map(a => {
      const m = a.match(/\\[(\\w[\\w-]*)(?:=["']?([^"'\\]]+))?/);
      return { name: m[1], value: m[2] || "" };
    });
  }
  const tagMatch = orig.match(/^(\\w+)/);
  if (tagMatch) hints.tag = tagMatch[1].toUpperCase();

  const candidates = [];
  const attrSearches = [];
  if (hints.attrs) {
    for (const a of hints.attrs) {
      if (a.value) attrSearches.push({ attr: a.name, val: a.value.toLowerCase() });
    }
  }
  if (hints.id) attrSearches.push({ attr: "id", val: hints.id.toLowerCase() });

  for (const el of document.querySelectorAll("*")) {
    const r = el.getBoundingClientRect();
    if (r.width === 0 && r.height === 0) continue;
    const s = getComputedStyle(el);
    if (s.display === "none" || s.visibility === "hidden") continue;
    let score = 0;

    if (hints.tag && el.tagName === hints.tag) score += 1;

    for (const { attr, val } of attrSearches) {
      const elVal = (el.getAttribute(attr) || "").toLowerCase();
      if (elVal === val) score += 5;
      else if (elVal.includes(val) || val.includes(elVal)) score += 3;
    }
    const ariaLabel = (el.getAttribute("aria-label") || "").toLowerCase();
    const placeholder = (el.getAttribute("placeholder") || "").toLowerCase();
    const name = (el.getAttribute("name") || "").toLowerCase();
    if (hints.id) {
      if (ariaLabel.includes(hints.id.toLowerCase())) score += 3;
      if (placeholder.includes(hints.id.toLowerCase())) score += 3;
      if (name.includes(hints.id.toLowerCase())) score += 3;
    }

    if (hints.classes) {
      for (const cls of hints.classes) {
        if (el.classList.contains(cls)) score += 2;
      }
    }

    if (score >= 3) candidates.push({ el, score });
  }
  candidates.sort((a, b) => b.score - a.score);
  if (candidates.length > 0) {
    const best = candidates[0].el;
    const desc = best.tagName.toLowerCase() + (best.id ? "#" + best.id : "") + (best.className ? "." + String(best.className).split(" ")[0] : "");
    best.setAttribute("data-yamil-healed", "true");
    return { found: true, healed: true, healedSelector: "[data-yamil-healed=true]", original: orig, healedDesc: desc, score: candidates[0].score };
  }
  return { found: false, healed: false, original: orig };
})()`;

// ── Monaco editor helper ──────────────────────────────────────────────
async function monacoSetValue(value, editorIndex = 0) {
  const r = await ye(`(function(){
    if (typeof window.monaco === "undefined" || !window.monaco.editor) return { monaco: false };
    const editors = window.monaco.editor.getEditors();
    if (!editors || !editors.length) return { monaco: false };
    const idx = Math.min(${editorIndex}, editors.length - 1);
    editors[idx].setValue(${JSON.stringify(value)});
    return { monaco: true, editorCount: editors.length, usedIndex: idx };
  })()`);
  return r;
}

// ── Tools are defined below ──────────────────────────────────────────
// (continued in this file)

// ── yamil_browser_start ───────────────────────────────────────────────
server.tool(
  "yamil_browser_start",
  "Start the YAMIL Browser native desktop Electron app. No-op if already running.",
  {
    startUrl:   z.string().optional().describe("URL to open on launch (default: https://yamil-ai.com)"),
    aiEndpoint: z.string().optional().describe("AI chat endpoint override"),
  },
  async ({ startUrl, aiEndpoint }) => {
    if (await yamilPing()) {
      return { content: [{ type: "text", text: "YAMIL Browser is already running on port 9300." }] };
    }
    const env = {
      ...process.env,
      AI_ENDPOINT: aiEndpoint || "http://localhost:9080/api/v1/builder-orchestra/browser-chat",
      START_URL:   startUrl   || "https://yamil-ai.com",
      APP_TITLE:   "YAMIL Browser",
    };
    let cmd, args;
    if (process.platform === "win32") {
      cmd  = "cmd.exe";
      args = ["/c", "npx electron ."];
    } else {
      cmd  = "npx";
      args = ["electron", "."];
    }
    yamilElectronProc = spawn(cmd, args, {
      cwd: YAMIL_ELECTRON_DIR,
      env,
      detached: true,
      stdio: "ignore",
    });
    yamilElectronProc.unref();
    for (let i = 0; i < 8; i++) {
      await new Promise(r => setTimeout(r, 1000));
      if (await yamilPing()) {
        return { content: [{ type: "text", text: `YAMIL Browser started (PID ${yamilElectronProc.pid}). Port 9300 ready.` }] };
      }
    }
    return { content: [{ type: "text", text: "YAMIL Browser process spawned but port 9300 not yet responding — it may still be loading." }] };
  }
);

// ── yamil_browser_stop ────────────────────────────────────────────────
server.tool(
  "yamil_browser_stop",
  "Close the YAMIL Browser desktop app gracefully.",
  {},
  async () => {
    if (!(await yamilPing())) {
      return { content: [{ type: "text", text: "YAMIL Browser is not running." }] };
    }
    try {
      await yamilPost("/eval", { script: "require('electron').app ? require('electron').app.quit() : window.close()" });
    } catch (_) {}
    if (yamilElectronProc) {
      try { process.kill(yamilElectronProc.pid); } catch (_) {}
      yamilElectronProc = null;
    }
    return { content: [{ type: "text", text: "YAMIL Browser closed." }] };
  }
);

// ── yamil_browser_status ──────────────────────────────────────────────
server.tool(
  "yamil_browser_status",
  "Show which browser MCP is active and the current CDP connection state",
  {},
  async () => {
    const alive = await yamilPing();
    if (!alive) {
      return { content: [{ type: "text", text: "YAMIL Browser: offline (port 9300 not responding)" }] };
    }
    try {
      const [urlRes, tabsRes, tabInfoRes] = await Promise.all([
        yamilGet("/url").catch(() => null),
        yamilGet("/tabs").catch(() => null),
        yamilGet("/active-tab-info").catch(() => null),
      ]);
      const urlData = urlRes ? await urlRes.json() : {};
      const tabsData = tabsRes ? await tabsRes.json() : {};
      const tabInfo = tabInfoRes ? await tabInfoRes.json() : {};
      const llmStatus = `LLM: Gemini=${_usingGemini} | Ollama Vision=${_ollamaAvailable} (${OLLAMA_VISION_MODEL}) | Bedrock=${!!getAnthropic()} | Anthropic=${!!process.env.ANTHROPIC_API_KEY}`;
      const lines = [
        "YAMIL Browser: running (unified stealth + logged-in)",
        `Active tab: ${tabInfo.type || "yamil"} | URL: ${urlData.url || "unknown"}`,
        `Tabs: ${(tabsData.tabs || []).length} (${(tabsData.tabs || []).filter(t => t.type === "stealth").length} stealth, ${(tabsData.tabs || []).filter(t => t.type !== "stealth").length} yamil)`,
        "Stealth: enabled (Playwright via browser-service:4000)",
        llmStatus,
      ];
      return { content: [{ type: "text", text: lines.join("\n") }] };
    } catch (_) {
      return { content: [{ type: "text", text: "YAMIL Browser: running (could not read status)" }] };
    }
  }
);

// ── yamil_browser_navigate ────────────────────────────────────────────
server.tool(
  "yamil_browser_navigate",
  "Navigate the YAMIL Browser desktop app to a URL.",
  { url: z.string().describe("URL to navigate to") },
  async ({ url }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running. Use yamil_browser_start first." }], isError: true };
    await yamilPost("/navigate", { url });
    const deadline = Date.now() + 15000;
    let ready = false;
    for (let i = 0; i < 30 && Date.now() < deadline; i++) {
      await new Promise(r => setTimeout(r, 500));
      try {
        const state = await ye("document.readyState");
        if (state === "complete") {
          await new Promise(r => setTimeout(r, 500));
          ready = true;
          break;
        }
      } catch (_) {}
    }
    const urlRes = await yamilGet("/url");
    const { url: finalUrl } = await urlRes.json();
    return { content: [{ type: "text", text: `Navigated → ${finalUrl}${ready ? "" : " (page may still be loading)"}` }] };
  }
);

// ── yamil_browser_screenshot (FIX: guard empty base64) ────────────────
server.tool(
  "yamil_browser_screenshot",
  "Take a screenshot of what the YAMIL Browser desktop app is currently showing.",
  {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const res = await yamilGet("/screenshot?quality=40&maxWidth=800&maxBytes=400000&scale=0.5");
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > 500_000) {
      return { content: [{ type: "text", text: `Screenshot too large for API (${(buf.length/1024).toFixed(0)}KB). Use yamil_browser_a11y_snapshot or yamil_browser_dom instead.` }], isError: true };
    }
    const b64 = buf.toString("base64");
    if (!b64 || b64.length < 100) {
      return { content: [{ type: "text", text: "Screenshot returned empty image. The page may not be loaded yet." }], isError: true };
    }
    return { content: [{ type: "image", data: b64, mimeType: "image/jpeg" }] };
  }
);

// ── yamil_browser_dom ─────────────────────────────────────────────────
server.tool(
  "yamil_browser_dom",
  "Get the current page context from the YAMIL Browser: URL, title, visible text, inputs, and buttons.",
  {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const res  = await yamilGet("/dom");
    const data = await res.json();
    const summary = [
      `URL:    ${data.url || ""}`,
      `Title:  ${data.title || ""}`,
      `Text:   ${(data.text || "").slice(0, 2000)}`,
      `Inputs: ${JSON.stringify((data.inputs || []).slice(0, 20))}`,
      `Buttons:${JSON.stringify((data.buttons || []).slice(0, 30))}`,
    ].join("\n");
    return { content: [{ type: "text", text: summary }] };
  }
);

// ── yamil_browser_eval ────────────────────────────────────────────────
server.tool(
  "yamil_browser_eval",
  "Execute JavaScript in the YAMIL Browser's active page and return the result.",
  { script: z.string().describe("JavaScript to run in the page context") },
  async ({ script }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res    = await yamilPost("/eval", { script });
      const data   = await res.json();
      if (data.error) return { content: [{ type: "text", text: `Eval error: ${data.error}` }], isError: true };
      return { content: [{ type: "text", text: JSON.stringify(data.result, null, 2) ?? "undefined" }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Eval failed: ${err.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_console_logs ────────────────────────────────────────
server.tool(
  "yamil_browser_console_logs",
  "Get console log messages (log/warn/error) from the active YAMIL Browser webview tab.",
  {
    level:  z.enum(["error", "warning", "info", "verbose"]).optional().describe("Filter by log level"),
    last:   z.number().optional().describe("Number of most recent messages to return (default 50)"),
    clear:  z.boolean().optional().describe("Clear the log buffer after reading"),
  },
  async ({ level, last, clear }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const params = new URLSearchParams();
      if (level) params.set("level", level);
      if (last)  params.set("last", String(last));
      if (clear) params.set("clear", "true");
      const qs  = params.toString();
      const res  = await yamilGet(`/console-logs${qs ? "?" + qs : ""}`);
      const data = await res.json();
      if (data.error) return { content: [{ type: "text", text: `Error: ${data.error}` }], isError: true };
      const logs = data.logs || [];
      if (logs.length === 0) return { content: [{ type: "text", text: "No console messages captured." }] };
      const formatted = logs.map(l => {
        const ts = new Date(l.ts).toISOString().slice(11, 23);
        const src = l.source ? ` (${l.source}:${l.line})` : "";
        return `[${ts}] [${l.level.toUpperCase()}] ${l.message}${src}`;
      }).join("\n");
      return { content: [{ type: "text", text: formatted }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Failed: ${err.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_focus ───────────────────────────────────────────────
server.tool(
  "yamil_browser_focus",
  "Bring the YAMIL Browser desktop window to the foreground.",
  {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await yamilPost("/focus");
    return { content: [{ type: "text", text: "YAMIL Browser focused." }] };
  }
);

// ── yamil_browser_click ───────────────────────────────────────────────
server.tool(
  "yamil_browser_click",
  "Click an element in the YAMIL Browser by CSS selector or visible text. Use 'near' to scope text search near another text.",
  {
    selector: z.string().optional().describe("CSS selector to click"),
    text:     z.string().optional().describe("Visible text to click (uses getByText)"),
    near:     z.string().optional().describe("Scope: only match text near this other text"),
  },
  async ({ selector, text, near }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const clickScript = `(function(){
      let el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : "null"};
      if (!el && ${JSON.stringify(text || "")}) {
        const searchText = ${JSON.stringify(text || "")}.toLowerCase().trim();
        const nearText = ${JSON.stringify(near || "")}.toLowerCase().trim();
        const candidates = [];
        const allEls = document.querySelectorAll("a, button, [role='button'], input[type='submit'], span, div, li, td, th, label, p, h1, h2, h3, h4, h5, h6");
        for (const e of allEls) {
          const directText = Array.from(e.childNodes).filter(n => n.nodeType === 3).map(n => n.textContent.trim()).join(" ").toLowerCase();
          const innerTxt = (e.innerText || "").trim().toLowerCase();
          const ariaLabel = (e.getAttribute("aria-label") || "").toLowerCase();
          const title = (e.getAttribute("title") || "").toLowerCase();
          const matchesText = directText === searchText || innerTxt === searchText || ariaLabel === searchText || ariaLabel.includes(searchText) || title === searchText || (innerTxt.includes(searchText) && innerTxt.length < searchText.length * 3);
          if (!matchesText) continue;
          const rect = e.getBoundingClientRect();
          if (rect.width === 0 && rect.height === 0) continue;
          const style = getComputedStyle(e);
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") continue;
          let nearScore = 0;
          if (nearText) {
            let parent = e.parentElement;
            let depth = 0;
            let foundNear = false;
            while (parent && depth < 10) {
              const parentText = (parent.innerText || "").toLowerCase();
              if (parentText.includes(nearText)) { foundNear = true; nearScore = 10 - depth; break; }
              parent = parent.parentElement;
              depth++;
            }
            if (!foundNear) continue;
          }
          const isClickable = ["A", "BUTTON"].includes(e.tagName) || e.getAttribute("role") === "button" ? 2 : 0;
          const isExact = (directText === searchText || innerTxt === searchText) ? 3 : 0;
          const textLen = innerTxt.length;
          const score = isClickable + isExact + nearScore;
          candidates.push({ el: e, score, textLen });
        }
        candidates.sort((a, b) => { if (b.score !== a.score) return b.score - a.score; return a.textLen - b.textLen; });
        el = candidates[0]?.el || null;
      }
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      el.focus();
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2;
      const cy = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy, button: 0 };
      el.dispatchEvent(new PointerEvent("pointerdown", opts));
      el.dispatchEvent(new MouseEvent("mousedown", opts));
      el.dispatchEvent(new MouseEvent("mouseup", opts));
      el.dispatchEvent(new MouseEvent("click", opts));
      return { found: true, tag: el.tagName, id: el.id || null, text: (el.innerText||"").trim().substring(0, 40) };
    })()`;
    for (let attempt = 0; attempt < 3; attempt++) {
      const r = await ye(clickScript);
      if (r?.found) { return { content: [{ type: "text", text: `Clicked ${r.tag}${r.id ? "#" + r.id : ""} (${selector || text}${near ? ` near "${near}"` : ""})` }] }; }
      if (attempt < 2) await new Promise(r => setTimeout(r, 500));
    }
    if (selector && !text) {
      const heal = await ye(SELF_HEAL_SCRIPT(selector));
      if (heal?.found && heal.healed) {
        const hr = await ye(`(function(){
          const el = document.querySelector("[data-yamil-healed=true]");
          if (!el) return { found: false };
          el.removeAttribute("data-yamil-healed");
          el.scrollIntoView({ block: "center", behavior: "instant" });
          el.focus();
          const rect = el.getBoundingClientRect();
          const cx = rect.left + rect.width / 2, cy = rect.top + rect.height / 2;
          const opts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy, button: 0 };
          el.dispatchEvent(new PointerEvent("pointerdown", opts));
          el.dispatchEvent(new MouseEvent("mousedown", opts));
          el.dispatchEvent(new MouseEvent("mouseup", opts));
          el.dispatchEvent(new MouseEvent("click", opts));
          return { found: true, tag: el.tagName, id: el.id || null, text: (el.innerText||"").trim().substring(0, 40) };
        })()`);
        if (hr?.found) {
          selectorCacheSet(await yamilPageUrl(), selector, heal.healedDesc);
          return { content: [{ type: "text", text: `[HEALED] Original selector "${selector}" failed. Found ${heal.healedDesc} (score: ${heal.score}). Clicked ${hr.tag}${hr.id ? "#" + hr.id : ""} "${hr.text}"` }] };
        }
      }
    }
    const errMsg = `Element not found after 3 attempts: ${selector || text}${near ? ` near "${near}"` : ""}`;
    logToolError("yamil_browser_click", { selector, text, near }, errMsg, await yamilPageUrl());
    return { content: [{ type: "text", text: errMsg }], isError: true };
  }
);

// ── yamil_browser_fill ────────────────────────────────────────────────
server.tool(
  "yamil_browser_fill",
  "Fill a form field in the YAMIL Browser (clears existing value first). Auto-detects Monaco editors.",
  {
    selector: z.string().describe("CSS selector of the input"),
    value:    z.string().describe("Value to fill"),
  },
  async ({ selector, value }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const monacoCheck = await ye(`(function(){
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el) return { found: false };
      if (el.closest && el.closest(".monaco-editor")) return { found: true, isMonaco: true };
      return { found: true, isMonaco: false };
    })()`);
    if (!monacoCheck?.found) {
      const mr = await monacoSetValue(value);
      if (mr?.monaco) { return { content: [{ type: "text", text: `Filled Monaco editor (${mr.editorCount} editor(s)) with content` }] }; }
      const heal = await ye(SELF_HEAL_SCRIPT(selector));
      if (heal?.found && heal.healed) {
        const hr = await ye(`(function(){
          const el = document.querySelector("[data-yamil-healed=true]");
          if (!el) return { found: false };
          el.removeAttribute("data-yamil-healed");
          el.scrollIntoView({ block: "center", behavior: "instant" });
          el.focus();
          el.dispatchEvent(new Event("focus", { bubbles: true }));
          if (el.isContentEditable) {
            el.textContent = ${JSON.stringify(value)};
            el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
            el.dispatchEvent(new Event("change", { bubbles: true }));
            return { found: true, value: el.textContent };
          }
          const proto = el.tagName === "TEXTAREA" ? window.HTMLTextAreaElement.prototype
                      : el.tagName === "SELECT"   ? window.HTMLSelectElement.prototype
                      : window.HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
          if (setter) setter.call(el, ${JSON.stringify(value)});
          else el.value = ${JSON.stringify(value)};
          var tracker = el._valueTracker; if (tracker) tracker.setValue("");
          el.dispatchEvent(new Event("input",  { bubbles: true }));
          el.dispatchEvent(new Event("change", { bubbles: true }));
          el.dispatchEvent(new Event("blur",   { bubbles: true }));
          return { found: true, value: el.value };
        })()`);
        if (hr?.found) {
          selectorCacheSet(await yamilPageUrl(), selector, heal.healedDesc);
          return { content: [{ type: "text", text: `[HEALED] Original selector "${selector}" failed. Found ${heal.healedDesc} (score: ${heal.score}). Filled with "${value}"` }] };
        }
      }
      logToolError("yamil_browser_fill", { selector, value: value.substring(0, 100) }, `Input not found: ${selector}`, await yamilPageUrl());
      return { content: [{ type: "text", text: `Input not found: ${selector}` }], isError: true };
    }
    if (monacoCheck.isMonaco) {
      const mr = await monacoSetValue(value);
      if (mr?.monaco) return { content: [{ type: "text", text: `Filled Monaco editor with content` }] };
    }
    const script = `(function(){
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      el.focus();
      el.dispatchEvent(new Event("focus", { bubbles: true }));
      if (el.isContentEditable) {
        el.textContent = ${JSON.stringify(value)};
        el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
        el.dispatchEvent(new Event("blur", { bubbles: true }));
        return { found: true, value: el.textContent };
      }
      const proto = el.tagName === "TEXTAREA" ? window.HTMLTextAreaElement.prototype
                  : el.tagName === "SELECT"   ? window.HTMLSelectElement.prototype
                  : window.HTMLInputElement.prototype;
      const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
      if (setter) setter.call(el, ${JSON.stringify(value)});
      else el.value = ${JSON.stringify(value)};
      var tracker = el._valueTracker; if (tracker) tracker.setValue("");
      el.dispatchEvent(new Event("input",  { bubbles: true }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
      el.dispatchEvent(new Event("blur",   { bubbles: true }));
      return { found: true, value: el.value };
    })()`;
    const r = await ye(script);
    if (!r?.found) {
      logToolError("yamil_browser_fill", { selector, value: value.substring(0, 100) }, `Input not found after DOM fill: ${selector}`, await yamilPageUrl());
      return { content: [{ type: "text", text: `Input not found: ${selector}` }], isError: true };
    }
    return { content: [{ type: "text", text: `Filled ${selector} with "${value}"` }] };
  }
);

// ── yamil_browser_type ────────────────────────────────────────────────
server.tool(
  "yamil_browser_type",
  "Type text into the focused element in the YAMIL Browser (key-by-key). Auto-detects Monaco editors.",
  {
    text:     z.string().describe("Text to type"),
    selector: z.string().optional().describe("CSS selector to focus before typing"),
  },
  async ({ text, selector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const isMonaco = await ye(`(function(){
      const el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : `document.activeElement`};
      if (el && el.closest && el.closest(".monaco-editor")) return true;
      if (typeof window.monaco !== "undefined" && window.monaco.editor) {
        const editors = window.monaco.editor.getEditors();
        if (editors && editors.length && editors[0].hasTextFocus()) return true;
      }
      return false;
    })()`);
    if (isMonaco) {
      const mr = await ye(`(function(){
        const editors = window.monaco.editor.getEditors();
        if (!editors || !editors.length) return { monaco: false };
        const ed = editors[0];
        const sel = ed.getSelection();
        ed.executeEdits("yamil-browser", [{ range: sel, text: ${JSON.stringify(text)} }]);
        return { monaco: true, typed: ${text.length} };
      })()`);
      if (mr?.monaco) return { content: [{ type: "text", text: `Typed ${text.length} characters into Monaco editor` }] };
    }
    if (selector) {
      const focused = await ye(`(function(){ const el = document.querySelector(${JSON.stringify(selector)}); if(el) { el.focus(); return true; } return false; })()`);
      if (!focused) {
        const heal = await ye(SELF_HEAL_SCRIPT(selector));
        if (heal?.found && heal.healed) {
          await ye(`(function(){ const el = document.querySelector("[data-yamil-healed=true]"); if(el) { el.removeAttribute("data-yamil-healed"); el.focus(); } })()`);
        }
      }
    }
    const script = `(function(txt){
      for (const ch of txt) {
        document.activeElement.dispatchEvent(new KeyboardEvent("keydown",  { key: ch, bubbles: true }));
        document.activeElement.dispatchEvent(new KeyboardEvent("keypress", { key: ch, bubbles: true }));
        document.execCommand("insertText", false, ch);
        document.activeElement.dispatchEvent(new KeyboardEvent("keyup",    { key: ch, bubbles: true }));
      }
      return { typed: txt.length };
    })(${JSON.stringify(text)})`;
    const r = await ye(script);
    return { content: [{ type: "text", text: `Typed ${r?.typed ?? 0} characters` }] };
  }
);

// ── yamil_browser_press ───────────────────────────────────────────────
server.tool(
  "yamil_browser_press",
  "Press a keyboard key in the YAMIL Browser (Enter, Escape, Tab, ArrowDown, etc.).",
  { key: z.string().describe("Key name, e.g. 'Enter', 'Escape', 'Tab', 'ArrowDown'") },
  async ({ key }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await ye(`(function(){
      const el = document.activeElement || document.body;
      const keyMap = {Enter:13,Escape:27,Tab:9,Backspace:8,Delete:46,ArrowUp:38,ArrowDown:40,ArrowLeft:37,ArrowRight:39,Home:36,End:35,PageUp:33,PageDown:34,Space:32," ":32};
      const keyName = ${JSON.stringify(key)};
      const code = keyName.length === 1 ? "Key" + keyName.toUpperCase() : keyName;
      const keyCode = keyMap[keyName] || keyName.charCodeAt(0) || 0;
      const opts = { key: keyName, code: code, keyCode: keyCode, which: keyCode, bubbles: true, cancelable: true };
      el.dispatchEvent(new KeyboardEvent("keydown", opts));
      el.dispatchEvent(new KeyboardEvent("keypress", opts));
      if (keyName === "Enter" && (el.tagName === "INPUT" || el.tagName === "TEXTAREA")) {
        const form = el.closest("form");
        if (form) form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
      }
      el.dispatchEvent(new KeyboardEvent("keyup", opts));
    })()`);
    return { content: [{ type: "text", text: `Pressed: ${key}` }] };
  }
);

// ── yamil_browser_scroll ─────────────────────────────────────────────
server.tool(
  "yamil_browser_scroll",
  "Scroll the YAMIL Browser page up or down. Returns delta — only NEW text that appeared after scrolling.",
  {
    direction: z.enum(["up", "down"]).describe("Scroll direction"),
    amount:    z.number().optional().describe("Pixels to scroll (default 500)"),
    selector:  z.string().optional().describe("CSS selector of a scrollable container (default: page)"),
  },
  async ({ direction, amount, selector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const px = (direction === "down" ? 1 : -1) * (amount ?? 500);
    const beforeText = await ye(`(function(){
      const els = document.querySelectorAll("h1,h2,h3,h4,h5,h6,p,li,td,th,a,button,span,label,input,textarea");
      const visible = new Set();
      for (const e of els) {
        const r = e.getBoundingClientRect();
        if (r.top >= 0 && r.bottom <= window.innerHeight && r.width > 0 && r.height > 0) {
          const t = (e.innerText || e.value || "").trim();
          if (t && t.length < 200) visible.add(t);
        }
      }
      return Array.from(visible);
    })()`) || [];
    const beforeSet = new Set(beforeText);
    if (selector) {
      const r = await ye(`(function(){
        const el = document.querySelector(${JSON.stringify(selector)});
        if (!el) return { found: false };
        el.scrollBy(0, ${px});
        return { found: true, scrollTop: el.scrollTop };
      })()`);
      if (!r?.found) return { content: [{ type: "text", text: `Scroll container not found: ${selector}` }], isError: true };
    } else {
      await ye(`(function(){
        const cx = window.innerWidth / 2, cy = window.innerHeight / 2;
        let el = document.elementFromPoint(cx, cy);
        while (el && el !== document.body && el !== document.documentElement) {
          const style = window.getComputedStyle(el);
          const overflowY = style.overflowY;
          if ((overflowY === 'auto' || overflowY === 'scroll' || overflowY === 'overlay') && el.scrollHeight > el.clientHeight) {
            el.scrollBy(0, ${px});
            return;
          }
          el = el.parentElement;
        }
        window.scrollBy(0, ${px});
      })()`);
    }
    await new Promise(r => setTimeout(r, 150));
    const afterText = await ye(`(function(){
      const els = document.querySelectorAll("h1,h2,h3,h4,h5,h6,p,li,td,th,a,button,span,label,input,textarea");
      const visible = new Set();
      for (const e of els) {
        const r = e.getBoundingClientRect();
        if (r.top >= 0 && r.bottom <= window.innerHeight && r.width > 0 && r.height > 0) {
          const t = (e.innerText || e.value || "").trim();
          if (t && t.length < 200) visible.add(t);
        }
      }
      return Array.from(visible);
    })()`) || [];
    const delta = afterText.filter(t => !beforeSet.has(t));
    const deltaText = delta.length > 0 ? `\n\nNew content:\n${delta.join("\n")}` : "\n\n(No new content appeared)";
    return { content: [{ type: "text", text: `Scrolled ${direction} ${Math.abs(px)}px${selector ? ` in ${selector}` : ""}${deltaText}` }] };
  }
);

// ── yamil_browser_scroll_until ────────────────────────────────────────
server.tool(
  "yamil_browser_scroll_until",
  "Scroll in a loop until a target selector or text is found, or the bottom is reached.",
  {
    target:     z.string().describe("CSS selector or text to search for"),
    direction:  z.enum(["up", "down"]).optional().describe("Scroll direction (default: down)"),
    maxScrolls: z.number().optional().describe("Maximum scroll iterations (default: 10)"),
    amount:     z.number().optional().describe("Pixels per scroll (default: 600)"),
  },
  async ({ target, direction, maxScrolls, amount }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await yamilEnsureObserver();
    const dir = direction || "down";
    const max = maxScrolls || 10;
    const px = (dir === "down" ? 1 : -1) * (amount || 600);
    const isSelector = /^[.#\[]|^\w+$/.test(target) && !target.includes(" ");
    const initialText = await ye(`(function(){
      const s = new Set();
      for (const e of document.querySelectorAll("h1,h2,h3,h4,h5,h6,p,li,td,th,a,span,label")) {
        const t = (e.innerText || "").trim();
        if (t && t.length < 200) s.add(t);
      }
      return Array.from(s);
    })()`) || [];
    const seenText = new Set(initialText);
    const allNewContent = [];
    for (let i = 0; i < max; i++) {
      const found = await ye(`(function(){
        const target = ${JSON.stringify(target)};
        const isSelector = ${isSelector};
        if (isSelector) {
          const el = document.querySelector(target);
          if (el) {
            const r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) {
              el.scrollIntoView({ block: "center" });
              return { found: true, tag: el.tagName, text: (el.innerText || "").trim().slice(0, 80) };
            }
          }
        }
        const searchLower = target.toLowerCase();
        for (const el of document.querySelectorAll("*")) {
          const txt = (el.innerText || "").trim().toLowerCase();
          if (txt.includes(searchLower) && txt.length < searchLower.length * 5) {
            const r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0 && r.top >= 0 && r.bottom <= window.innerHeight) {
              return { found: true, tag: el.tagName, text: txt.slice(0, 80) };
            }
          }
        }
        return { found: false };
      })()`);
      if (found?.found) {
        return { content: [{ type: "text", text: `Found "${target}" after ${i} scrolls. Element: ${found.tag} "${found.text}"${allNewContent.length ? `\n\nNew content discovered:\n${allNewContent.join("\n")}` : ""}` }] };
      }
      const scrollResult = await ye(`(function(){
        const cx = window.innerWidth / 2, cy = window.innerHeight / 2;
        let el = document.elementFromPoint(cx, cy);
        while (el && el !== document.body && el !== document.documentElement) {
          const style = window.getComputedStyle(el);
          const ov = style.overflowY;
          if ((ov === 'auto' || ov === 'scroll' || ov === 'overlay') && el.scrollHeight > el.clientHeight) {
            const before = el.scrollTop;
            el.scrollBy(0, ${px});
            return { scrolled: el.scrollTop !== before, scrollTop: el.scrollTop, scrollHeight: el.scrollHeight, clientHeight: el.clientHeight };
          }
          el = el.parentElement;
        }
        const before = window.scrollY;
        window.scrollBy(0, ${px});
        return { scrolled: window.scrollY !== before, scrollTop: window.scrollY, scrollHeight: document.documentElement.scrollHeight, clientHeight: window.innerHeight };
      })()`);
      await yamilWaitForDom(1500);
      const newText = await ye(`(function(){
        const s = new Set();
        for (const e of document.querySelectorAll("h1,h2,h3,h4,h5,h6,p,li,td,th,a,span,label")) {
          const r = e.getBoundingClientRect();
          if (r.top >= 0 && r.bottom <= window.innerHeight && r.width > 0 && r.height > 0) {
            const t = (e.innerText || "").trim();
            if (t && t.length < 200) s.add(t);
          }
        }
        return Array.from(s);
      })()`) || [];
      for (const t of newText) {
        if (!seenText.has(t)) { seenText.add(t); allNewContent.push(t); }
      }
      if (!scrollResult?.scrolled) {
        return { content: [{ type: "text", text: `Reached ${dir === "down" ? "bottom" : "top"} after ${i + 1} scrolls. Target "${target}" not found.${allNewContent.length ? `\n\nNew content discovered:\n${allNewContent.join("\n")}` : ""}` }] };
      }
    }
    return { content: [{ type: "text", text: `Reached max scrolls (${max}). Target "${target}" not found.${allNewContent.length ? `\n\nNew content discovered:\n${allNewContent.join("\n")}` : ""}` }] };
  }
);

// ── yamil_browser_hover ───────────────────────────────────────────────
server.tool(
  "yamil_browser_hover",
  "Hover over an element in the YAMIL Browser (triggers hover states, dropdowns, tooltips).",
  {
    selector: z.string().optional().describe("CSS selector to hover"),
    text:     z.string().optional().describe("Visible text to hover"),
  },
  async ({ selector, text }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const script = `(function(){
      let el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : "null"};
      if (!el && ${JSON.stringify(text || "")}) {
        const searchText = ${JSON.stringify(text || "")}.toLowerCase().trim();
        const allEls = document.querySelectorAll("a, button, [role='button'], [role='menuitem'], [role='tab'], span, div, li, td, th, label, p, h1, h2, h3, h4, h5, h6, img, svg");
        let best = null, bestScore = -1;
        for (const e of allEls) {
          const innerTxt = (e.innerText || "").trim().toLowerCase();
          const ariaLabel = (e.getAttribute("aria-label") || "").toLowerCase();
          const title = (e.getAttribute("title") || "").toLowerCase();
          if (innerTxt !== searchText && !innerTxt.includes(searchText) && ariaLabel !== searchText && !ariaLabel.includes(searchText) && title !== searchText) continue;
          const rect = e.getBoundingClientRect();
          if (rect.width === 0 && rect.height === 0) continue;
          const style = getComputedStyle(e);
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") continue;
          const exact = (innerTxt === searchText || ariaLabel === searchText) ? 3 : 0;
          const short = innerTxt.length < searchText.length * 3 ? 1 : 0;
          const score = exact + short;
          if (score > bestScore) { best = e; bestScore = score; }
        }
        el = best;
      }
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      ["pointerover","pointerenter","mouseover","mouseenter","mousemove"].forEach(t =>
        el.dispatchEvent(new MouseEvent(t, { bubbles: true, cancelable: true }))
      );
      return { found: true, tag: el.tagName, text: (el.innerText||"").trim().substring(0, 40) };
    })()`;
    for (let attempt = 0; attempt < 2; attempt++) {
      const r = await ye(script);
      if (r?.found) return { content: [{ type: "text", text: `Hovered: ${selector || text}` }] };
      if (attempt < 1) await new Promise(r => setTimeout(r, 400));
    }
    logToolError("yamil_browser_hover", { selector, text }, `Element not found: ${selector || text}`, await yamilPageUrl());
    return { content: [{ type: "text", text: `Element not found: ${selector || text}` }], isError: true };
  }
);

// ── yamil_browser_double_click ────────────────────────────────────────
server.tool(
  "yamil_browser_double_click",
  "Double-click an element in the YAMIL Browser.",
  {
    selector: z.string().optional().describe("CSS selector to double-click"),
    text:     z.string().optional().describe("Visible text to double-click"),
  },
  async ({ selector, text }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const script = `(function(){
      let el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : "null"};
      if (!el && ${JSON.stringify(text || "")}) {
        const searchText = ${JSON.stringify(text || "")}.toLowerCase().trim();
        const allEls = document.querySelectorAll("a, button, [role='button'], span, div, li, td, th, label, p, h1, h2, h3, h4, h5, h6");
        let best = null, bestScore = -1;
        for (const e of allEls) {
          const innerTxt = (e.innerText || "").trim().toLowerCase();
          const ariaLabel = (e.getAttribute("aria-label") || "").toLowerCase();
          if (innerTxt !== searchText && !innerTxt.includes(searchText) && ariaLabel !== searchText) continue;
          const rect = e.getBoundingClientRect();
          if (rect.width === 0 && rect.height === 0) continue;
          const style = getComputedStyle(e);
          if (style.display === "none" || style.visibility === "hidden") continue;
          const score = (innerTxt === searchText ? 3 : 0) + (innerTxt.length < searchText.length * 3 ? 1 : 0);
          if (score > bestScore) { best = e; bestScore = score; }
        }
        el = best;
      }
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2, cy = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy };
      el.dispatchEvent(new MouseEvent("mousedown", opts));
      el.dispatchEvent(new MouseEvent("mouseup", opts));
      el.dispatchEvent(new MouseEvent("click", opts));
      el.dispatchEvent(new MouseEvent("mousedown", opts));
      el.dispatchEvent(new MouseEvent("mouseup", opts));
      el.dispatchEvent(new MouseEvent("click", opts));
      el.dispatchEvent(new MouseEvent("dblclick", opts));
      return { found: true };
    })()`;
    for (let attempt = 0; attempt < 2; attempt++) {
      const r = await ye(script);
      if (r?.found) { return { content: [{ type: "text", text: `Double-clicked: ${selector || text}` }] }; }
      if (attempt < 1) await new Promise(r => setTimeout(r, 400));
    }
    logToolError("yamil_browser_double_click", { selector, text }, `Element not found: ${selector || text}`, await yamilPageUrl());
    return { content: [{ type: "text", text: `Element not found: ${selector || text}` }], isError: true };
  }
);

// ── yamil_browser_right_click ─────────────────────────────────────────
server.tool(
  "yamil_browser_right_click",
  "Right-click an element in the YAMIL Browser to open a context menu.",
  {
    selector: z.string().optional().describe("CSS selector to right-click"),
    text:     z.string().optional().describe("Visible text to right-click"),
  },
  async ({ selector, text }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const script = `(function(){
      let el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : "null"};
      if (!el && ${JSON.stringify(text || "")}) {
        const searchText = ${JSON.stringify(text || "")}.toLowerCase().trim();
        const allEls = document.querySelectorAll("a, button, [role='button'], span, div, li, td, th, label, p, h1, h2, h3, h4, h5, h6");
        let best = null, bestScore = -1;
        for (const e of allEls) {
          const innerTxt = (e.innerText || "").trim().toLowerCase();
          const ariaLabel = (e.getAttribute("aria-label") || "").toLowerCase();
          if (innerTxt !== searchText && !innerTxt.includes(searchText) && ariaLabel !== searchText) continue;
          const rect = e.getBoundingClientRect();
          if (rect.width === 0 && rect.height === 0) continue;
          const style = getComputedStyle(e);
          if (style.display === "none" || style.visibility === "hidden") continue;
          const score = (innerTxt === searchText ? 3 : 0) + (innerTxt.length < searchText.length * 3 ? 1 : 0);
          if (score > bestScore) { best = e; bestScore = score; }
        }
        el = best;
      }
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      const rect = el.getBoundingClientRect();
      const opts = { bubbles: true, cancelable: true, button: 2, clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2 };
      el.dispatchEvent(new MouseEvent("contextmenu", opts));
      return { found: true };
    })()`;
    for (let attempt = 0; attempt < 2; attempt++) {
      const r = await ye(script);
      if (r?.found) return { content: [{ type: "text", text: `Right-clicked: ${selector || text}` }] };
      if (attempt < 1) await new Promise(r => setTimeout(r, 400));
    }
    logToolError("yamil_browser_right_click", { selector, text }, `Element not found: ${selector || text}`, await yamilPageUrl());
    return { content: [{ type: "text", text: `Element not found: ${selector || text}` }], isError: true };
  }
);

// ── yamil_browser_select ──────────────────────────────────────────────
server.tool(
  "yamil_browser_select",
  "Select an option from a dropdown in the YAMIL Browser. Handles native <select>, Radix/shadcn Select, and custom dropdowns.",
  {
    selector: z.string().describe("CSS selector of the <select> or trigger element"),
    value:    z.string().optional().describe("Option value to select"),
    label:    z.string().optional().describe("Option visible label to select"),
    index:    z.number().optional().describe("Zero-based option index"),
  },
  async ({ selector, value, label, index }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const nativeScript = `(function(){
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el) return { found: false };
      if (el.tagName === "SELECT") {
        const opts = Array.from(el.options);
        let opt;
        if (${JSON.stringify(value || "")} !== "") opt = opts.find(o => o.value === ${JSON.stringify(value || "")});
        else if (${JSON.stringify(label || "")} !== "") opt = opts.find(o => o.text.trim() === ${JSON.stringify(label || "")}) || opts.find(o => o.text.trim().toLowerCase().includes(${JSON.stringify((label || "").toLowerCase())}));
        else if (${index !== undefined}) opt = opts[${index ?? 0}];
        if (!opt) return { found: true, native: true, error: "option not found in " + opts.length + " options" };
        el.value = opt.value;
        el.dispatchEvent(new Event("change", { bubbles: true }));
        el.dispatchEvent(new Event("input", { bubbles: true }));
        return { found: true, native: true, selected: opt.text };
      }
      return { found: true, native: false };
    })()`;
    const nr = await ye(nativeScript);
    if (!nr?.found) {
      logToolError("yamil_browser_select", { selector, value, label, index }, `Select not found: ${selector}`, await yamilPageUrl());
      return { content: [{ type: "text", text: `Select not found: ${selector}` }], isError: true };
    }
    if (nr.native && !nr.error) { return { content: [{ type: "text", text: `Selected "${nr.selected}" in ${selector}` }] }; }
    if (nr.native && nr.error) {
      logToolError("yamil_browser_select", { selector, value, label, index }, nr.error, await yamilPageUrl());
      return { content: [{ type: "text", text: nr.error }], isError: true };
    }
    await yamilEnsureObserver();
    const triggerScript = `(function(){
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      el.focus();
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2, cy = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy, button: 0 };
      el.dispatchEvent(new PointerEvent("pointerdown", opts));
      el.dispatchEvent(new MouseEvent("mousedown", opts));
      el.dispatchEvent(new MouseEvent("mouseup", opts));
      el.dispatchEvent(new MouseEvent("click", opts));
      el.dispatchEvent(new PointerEvent("pointerup", opts));
      return { found: true, tag: el.tagName, role: el.getAttribute("role") };
    })()`;
    await ye(triggerScript);
    const searchLabel = label || value || "";
    await yamilWaitForDom(2000);
    const optionSelectors = [
      "[role='option']", "[role='menuitem']", "[role='listbox'] [role='option']",
      "[data-radix-collection-item]", "[cmdk-item]",
      ".select-option", ".dropdown-item", "li[data-value]",
      "[role='menuitemradio']", "[role='menuitemcheckbox']",
      ".ant-select-item", ".MuiMenuItem-root", ".el-select-dropdown__item",
    ].join(",");
    let pr = null;
    for (let attempt = 0; attempt < 2; attempt++) {
      const pickScript = `(function(){
        const searchText = ${JSON.stringify(searchLabel)}.toLowerCase().trim();
        const options = document.querySelectorAll(${JSON.stringify(optionSelectors)});
        if (!options.length) return { found: false, optionCount: 0, waiting: true };
        for (const opt of options) {
          const txt = (opt.innerText || opt.textContent || "").trim().toLowerCase();
          const val = opt.getAttribute("data-value") || opt.getAttribute("value") || "";
          if (searchText && (txt === searchText || txt.includes(searchText) || val === searchText)) {
            opt.scrollIntoView({ block: "center" });
            opt.click();
            return { found: true, selected: (opt.innerText||"").trim() };
          }
        }
        if (${index !== undefined} && options.length > ${index ?? 0}) {
          const opt = options[${index ?? 0}];
          opt.scrollIntoView({ block: "center" });
          opt.click();
          return { found: true, selected: (opt.innerText||"").trim() };
        }
        return { found: false, optionCount: options.length };
      })()`;
      pr = await ye(pickScript);
      if (pr?.found || (pr && !pr.waiting)) break;
      if (attempt === 0) await yamilWaitForDom(1000);
    }
    if (pr?.found) { return { content: [{ type: "text", text: `Selected "${pr.selected}" in custom dropdown` }] }; }
    logToolError("yamil_browser_select", { selector, value, label, index }, `Option not found in custom dropdown (${pr?.optionCount || 0} options)`, await yamilPageUrl());
    return { content: [{ type: "text", text: `Option "${searchLabel}" not found in dropdown (${pr?.optionCount || 0} options visible)` }], isError: true };
  }
);

// ── yamil_browser_a11y_snapshot ─────────────────────────────────────
server.tool(
  "yamil_browser_a11y_snapshot",
  "Get a compact accessibility tree snapshot with element refs (@e1, @e2, ...). Use @refs with yamil_browser_a11y_click and yamil_browser_a11y_fill.",
  {
    selector: z.string().optional().describe("CSS selector to scope the snapshot (default: body)"),
  },
  async ({ selector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await yamilEnsureObserver();

    // For stealth tabs, use browser-service route (supports cross-origin iframes via Playwright)
    try {
      const infoRes = await yamilGet("/active-tab-info");
      const info = await infoRes.json();
      if (info && info.type === "stealth" && info.sessionId) {
        const qs = selector ? `?selector=${encodeURIComponent(selector)}` : "";
        const res = await fetch(`${BROWSER_SVC_URL}/sessions/${info.sessionId}/a11y-snapshot${qs}`, { signal: AbortSignal.timeout(15000) });
        if (res.ok) {
          const data = await res.json();
          if (!data.tree) return { content: [{ type: "text", text: "Empty page — no interactive or semantic elements found." }] };
          return { content: [{ type: "text", text: `A11y snapshot (v${data.version}, ${data.count} elements):\n${data.tree}` }] };
        }
      }
    } catch (_) { /* fall through to eval-based snapshot */ }

    const snapshot = await ye(`(function(){
      const root = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : `document.body`};
      if (!root) return { error: "Root not found" };
      const refs = {};
      const lines = [];
      let refId = 1;
      const INTERACTIVE = new Set(["A","BUTTON","INPUT","TEXTAREA","SELECT","DETAILS","SUMMARY"]);
      const SEMANTIC = new Set(["H1","H2","H3","H4","H5","H6","NAV","MAIN","ASIDE","HEADER","FOOTER","SECTION","ARTICLE","FORM","TABLE","THEAD","TBODY","TR","TH","TD","UL","OL","LI","LABEL","IMG","FIGURE","FIGCAPTION","DIALOG"]);
      const ROLES_INTERACTIVE = new Set(["button","link","textbox","combobox","listbox","option","menuitem","menuitemradio","menuitemcheckbox","checkbox","radio","switch","slider","spinbutton","searchbox","tab","tabpanel","dialog","alertdialog","tree","treeitem","grid","gridcell","row"]);
      const ROLES_SEMANTIC = new Set(["heading","navigation","main","complementary","banner","contentinfo","region","form","table","list","listitem","img","figure","alert","status","log","marquee","timer","toolbar","menu","menubar","tablist"]);
      const version = window.__yamil_snapshot_version || 1;
      function walk(el, depth, frameIndex) {
        if (depth > 20 || lines.length > 500) return;
        const tag = el.tagName;
        if (!tag) return;
        if (tag === "SCRIPT" || tag === "STYLE" || tag === "NOSCRIPT" || tag === "SVG" || tag === "PATH") return;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 && rect.height === 0) return;
        const style = getComputedStyle(el);
        if (style.display === "none" || style.visibility === "hidden") return;
        // Traverse into same-origin iframes
        if (tag === "IFRAME") {
          try {
            const iframeDoc = el.contentDocument || el.contentWindow?.document;
            if (iframeDoc && iframeDoc.body) {
              const ref = "@e" + refId++;
              const indent = "  ".repeat(Math.min(depth, 8));
              const src = el.src ? el.src.split("?")[0].split("/").pop() : "";
              lines.push(indent + ref + ' iframe' + (src ? ' "' + src.slice(0, 40) + '"' : ''));
              el.setAttribute("data-yamil-ref", ref);
              refs[ref] = { tag: "IFRAME", id: el.id || null, selector: el.id ? "#" + el.id : null, frame: true };
              for (const child of iframeDoc.body.children) walk(child, depth + 1, ref);
            }
          } catch(e) { /* cross-origin, skip */ }
          return;
        }
        const role = el.getAttribute("role") || "";
        const ariaLabel = el.getAttribute("aria-label") || "";
        const ariaExpanded = el.getAttribute("aria-expanded");
        const ariaSelected = el.getAttribute("aria-selected");
        const placeholder = el.getAttribute("placeholder") || "";
        const title = el.getAttribute("title") || "";
        const isInteractive = INTERACTIVE.has(tag) || ROLES_INTERACTIVE.has(role) || el.hasAttribute("onclick") || el.hasAttribute("tabindex");
        const isSemantic = SEMANTIC.has(tag) || ROLES_SEMANTIC.has(role);
        if (isInteractive || isSemantic) {
          const ref = "@e" + refId++;
          const indent = "  ".repeat(Math.min(depth, 8));
          const parts = [ref];
          if (role) parts.push(role);
          else parts.push(tag.toLowerCase());
          if (ariaLabel) parts.push('"' + ariaLabel.slice(0, 60) + '"');
          else if (title) parts.push('"' + title.slice(0, 60) + '"');
          else {
            const directText = Array.from(el.childNodes).filter(n => n.nodeType === 3).map(n => n.textContent.trim()).join(" ").slice(0, 60);
            if (directText) parts.push('"' + directText.replace(/"/g, "'") + '"');
            else if (tag === "IMG") {
              const imgLabel = el.alt || el.title || "";
              if (imgLabel) parts.push('"' + imgLabel.slice(0, 40) + '"');
            }
          }
          if (el.value && (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT")) parts.push("value=" + JSON.stringify(el.value.slice(0, 30)));
          if (placeholder) parts.push("placeholder=" + JSON.stringify(placeholder.slice(0, 30)));
          if (ariaExpanded !== null) parts.push(ariaExpanded === "true" ? "[expanded]" : "[collapsed]");
          if (ariaSelected === "true") parts.push("[selected]");
          if (el.disabled) parts.push("[disabled]");
          if (document.activeElement === el) parts.push("[focused]");
          if (el.checked) parts.push("[checked]");
          if (el.type) parts.push("type=" + el.type);
          if (tag === "A" && el.href) parts.push("href=" + JSON.stringify(el.href.slice(0, 60)));
          lines.push(indent + parts.join(" "));
          el.setAttribute("data-yamil-ref", ref);
          refs[ref] = { tag, id: el.id || null, selector: el.id ? "#" + el.id : null, frame: frameIndex || null };
        }
        for (const child of el.children) walk(child, depth + 1, frameIndex);
      }
      walk(root, 0, null);
      window.__yamil_refs = refs;
      window.__yamil_refs_version = version;
      return { tree: lines.join("\\n"), count: refId - 1, version: version };
    })()`);
    if (snapshot?.error) return { content: [{ type: "text", text: `Error: ${snapshot.error}` }], isError: true };
    if (!snapshot?.tree) return { content: [{ type: "text", text: "Empty page — no interactive or semantic elements found." }] };
    return { content: [{ type: "text", text: `A11y snapshot (v${snapshot.version}, ${snapshot.count} elements):\n${snapshot.tree}` }] };
  }
);

// ── yamil_browser_a11y_click ────────────────────────────────────────
server.tool(
  "yamil_browser_a11y_click",
  "Click an element by its @ref from a11y_snapshot (e.g. @e5).",
  {
    ref:     z.string().describe("Element ref from a11y snapshot, e.g. '@e5'"),
    version: z.number().optional().describe("Snapshot version for stale detection"),
  },
  async ({ ref, version }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };

    // For stealth tabs, use Playwright-based click (handles cross-origin iframes)
    try {
      const infoRes = await yamilGet("/active-tab-info");
      const info = await infoRes.json();
      if (info && info.type === "stealth" && info.sessionId) {
        const res = await fetch(`${BROWSER_SVC_URL}/sessions/${info.sessionId}/a11y-click`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ref }),
          signal: AbortSignal.timeout(10000),
        });
        const r = await res.json();
        if (!r?.found) return { content: [{ type: "text", text: `Ref ${ref} not found on page. Run a11y_snapshot again.` }], isError: true };
        return { content: [{ type: "text", text: `Clicked ${ref} → ${r.tag} "${r.text}"` }] };
      }
    } catch (_) { /* fall through to eval-based click */ }

    const r = await ye(`(function(){
      const refVersion = window.__yamil_refs_version;
      if (${version ?? -1} > 0 && refVersion !== ${version ?? -1}) return { stale: true, expected: ${version ?? -1}, actual: refVersion };
      // Search in main document and same-origin iframes
      function findRef(doc) {
        const el = doc.querySelector('[data-yamil-ref="${ref}"]');
        if (el) return el;
        const iframes = doc.querySelectorAll("iframe");
        for (const iframe of iframes) {
          try {
            const iDoc = iframe.contentDocument || iframe.contentWindow?.document;
            if (iDoc) { const found = findRef(iDoc); if (found) return found; }
          } catch(e) {}
        }
        return null;
      }
      const el = findRef(document);
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      el.focus();
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2, cy = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy, button: 0 };
      el.dispatchEvent(new PointerEvent("pointerdown", opts));
      el.dispatchEvent(new MouseEvent("mousedown", opts));
      el.dispatchEvent(new MouseEvent("mouseup", opts));
      el.dispatchEvent(new MouseEvent("click", opts));
      return { found: true, tag: el.tagName, text: (el.innerText||"").trim().slice(0, 40) };
    })()`);
    if (r?.stale) return { content: [{ type: "text", text: `Stale snapshot: expected v${r.expected}, page is now v${r.actual}. Run a11y_snapshot again.` }], isError: true };
    if (!r?.found) return { content: [{ type: "text", text: `Ref ${ref} not found on page. Run a11y_snapshot again.` }], isError: true };
    return { content: [{ type: "text", text: `Clicked ${ref} → ${r.tag} "${r.text}"` }] };
  }
);

// ── yamil_browser_a11y_fill ──────────────────────────────────────────
server.tool(
  "yamil_browser_a11y_fill",
  "Fill a form field by its @ref from a11y_snapshot (e.g. @e3).",
  {
    ref:     z.string().describe("Element ref from a11y snapshot, e.g. '@e3'"),
    value:   z.string().describe("Value to fill"),
    version: z.number().optional().describe("Snapshot version for stale detection"),
  },
  async ({ ref, value, version }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };

    // For stealth tabs, use Playwright-based fill (handles cross-origin iframes)
    try {
      const infoRes = await yamilGet("/active-tab-info");
      const info = await infoRes.json();
      if (info && info.type === "stealth" && info.sessionId) {
        const res = await fetch(`${BROWSER_SVC_URL}/sessions/${info.sessionId}/a11y-fill`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ref, value }),
          signal: AbortSignal.timeout(10000),
        });
        const r = await res.json();
        if (!r?.found) return { content: [{ type: "text", text: `Ref ${ref} not found on page. Run a11y_snapshot again.` }], isError: true };
        return { content: [{ type: "text", text: `Filled ${ref} → ${r.tag} with "${value}"` }] };
      }
    } catch (_) { /* fall through to eval-based fill */ }

    const r = await ye(`(function(){
      const refVersion = window.__yamil_refs_version;
      if (${version ?? -1} > 0 && refVersion !== ${version ?? -1}) return { stale: true, expected: ${version ?? -1}, actual: refVersion };
      function findRef(doc) {
        const el = doc.querySelector('[data-yamil-ref="${ref}"]');
        if (el) return el;
        const iframes = doc.querySelectorAll("iframe");
        for (const iframe of iframes) {
          try {
            const iDoc = iframe.contentDocument || iframe.contentWindow?.document;
            if (iDoc) { const found = findRef(iDoc); if (found) return found; }
          } catch(e) {}
        }
        return null;
      }
      const el = findRef(document);
      if (!el) return { found: false };
      el.scrollIntoView({ block: "center", behavior: "instant" });
      el.focus();
      el.dispatchEvent(new Event("focus", { bubbles: true }));
      if (el.isContentEditable) {
        el.textContent = ${JSON.stringify(value)};
        el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
        return { found: true, tag: el.tagName, value: el.textContent };
      }
      const proto = el.tagName === "TEXTAREA" ? window.HTMLTextAreaElement.prototype
                  : el.tagName === "SELECT"   ? window.HTMLSelectElement.prototype
                  : window.HTMLInputElement.prototype;
      const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
      if (setter) setter.call(el, ${JSON.stringify(value)});
      else el.value = ${JSON.stringify(value)};
      el.dispatchEvent(new InputEvent("input",  { bubbles: true, inputType: "insertText" }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
      el.dispatchEvent(new Event("blur",   { bubbles: true }));
      return { found: true, tag: el.tagName, value: el.value };
    })()`);
    if (r?.stale) return { content: [{ type: "text", text: `Stale snapshot: expected v${r.expected}, page is now v${r.actual}. Run a11y_snapshot again.` }], isError: true };
    if (!r?.found) return { content: [{ type: "text", text: `Ref ${ref} not found on page. Run a11y_snapshot again.` }], isError: true };
    return { content: [{ type: "text", text: `Filled ${ref} → ${r.tag} with "${value}"` }] };
  }
);

// ── yamil_browser_expand_and_list ────────────────────────────────────
server.tool(
  "yamil_browser_expand_and_list",
  "Click a dropdown/select trigger and return all available options as a structured list.",
  {
    selector: z.string().optional().describe("CSS selector of the trigger element"),
    text:     z.string().optional().describe("Visible text of the trigger to click"),
  },
  async ({ selector, text }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await yamilEnsureObserver();
    const clickScript = `(function(){
      let el = ${selector ? `document.querySelector(${JSON.stringify(selector)})` : "null"};
      if (!el && ${JSON.stringify(text || "")}) {
        const t = ${JSON.stringify(text || "")}.toLowerCase();
        for (const e of document.querySelectorAll("button, [role='combobox'], [role='listbox'], select, [data-radix-select-trigger], [class*='select'], [class*='dropdown'], input[type='text'][aria-haspopup]")) {
          if ((e.innerText || "").toLowerCase().trim().includes(t) || (e.getAttribute("aria-label") || "").toLowerCase().includes(t)) { el = e; break; }
        }
      }
      if (!el) return { found: false };
      if (el.tagName === "SELECT") {
        const opts = Array.from(el.options).map((o, i) => ({ index: i, text: o.text.trim(), value: o.value, selected: o.selected }));
        return { found: true, native: true, options: opts };
      }
      el.scrollIntoView({ block: "center", behavior: "instant" });
      el.focus();
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2, cy = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, clientX: cx, clientY: cy, button: 0 };
      el.dispatchEvent(new PointerEvent("pointerdown", opts));
      el.dispatchEvent(new MouseEvent("mousedown", opts));
      el.dispatchEvent(new MouseEvent("mouseup", opts));
      el.dispatchEvent(new MouseEvent("click", opts));
      el.dispatchEvent(new PointerEvent("pointerup", opts));
      return { found: true, native: false, tag: el.tagName, role: el.getAttribute("role") };
    })()`;
    const clickResult = await ye(clickScript);
    if (!clickResult?.found) {
      return { content: [{ type: "text", text: `Trigger not found: ${selector || text}` }], isError: true };
    }
    if (clickResult.native) {
      const optList = clickResult.options.map(o => `${o.selected ? ">" : " "} [${o.index}] ${o.text} (value="${o.value}")`).join("\n");
      return { content: [{ type: "text", text: `Native <select> with ${clickResult.options.length} options:\n${optList}` }] };
    }
    await yamilWaitForDom(2000);
    const scanScript = `(function(){
      const selectors = [
        "[role='option']", "[role='menuitem']", "[role='menuitemradio']", "[role='menuitemcheckbox']",
        "[data-radix-collection-item]", "[cmdk-item]",
        ".select-option", ".dropdown-item", "li[data-value]",
        ".ant-select-item", ".MuiMenuItem-root", ".el-select-dropdown__item",
        "[role='listbox'] > *", "[role='menu'] > *",
      ];
      const options = document.querySelectorAll(selectors.join(","));
      if (!options.length) {
        const containers = document.querySelectorAll("[role='listbox'], [role='menu'], [data-radix-popper-content-wrapper], [data-state='open'], .dropdown-menu, .select-menu, .popover");
        const fallbackOpts = [];
        for (const c of containers) {
          const rect = c.getBoundingClientRect();
          if (rect.width === 0 || rect.height === 0) continue;
          for (const child of c.children) {
            const txt = (child.innerText || "").trim();
            if (txt && txt.length < 200) fallbackOpts.push({ index: fallbackOpts.length, text: txt, value: child.getAttribute("data-value") || "", selected: child.getAttribute("aria-selected") === "true" || child.classList.contains("selected") });
          }
        }
        return { options: fallbackOpts };
      }
      return { options: Array.from(options).map((o, i) => ({
        index: i,
        text: (o.innerText || o.textContent || "").trim(),
        value: o.getAttribute("data-value") || o.getAttribute("value") || "",
        selected: o.getAttribute("aria-selected") === "true" || o.classList.contains("selected"),
      }))};
    })()`;
    const scanResult = await ye(scanScript);
    const opts = scanResult?.options || [];
    if (opts.length === 0) {
      return { content: [{ type: "text", text: "Dropdown opened but no options detected. Try yamil_browser_screenshot to see what appeared." }] };
    }
    const optList = opts.map(o => `${o.selected ? ">" : " "} [${o.index}] ${o.text}${o.value ? ` (value="${o.value}")` : ""}`).join("\n");
    return { content: [{ type: "text", text: `Dropdown expanded with ${opts.length} options:\n${optList}\n\nUse yamil_browser_select with index to pick one.` }] };
  }
);

// ── yamil_browser_batch ──────────────────────────────────────────────
server.tool(
  "yamil_browser_batch",
  "Execute multiple independent browser actions in a single call. Reduces round-trips 50-74%.",
  {
    actions: z.array(z.object({
      type:      z.enum(["click", "fill", "type", "press", "scroll", "check", "uncheck"]).describe("Action type"),
      selector:  z.string().optional().describe("CSS selector for the target element"),
      text:      z.string().optional().describe("Text to click or type"),
      value:     z.string().optional().describe("Value to fill"),
      key:       z.string().optional().describe("Key to press"),
      direction: z.enum(["up", "down"]).optional().describe("Scroll direction"),
      amount:    z.number().optional().describe("Scroll amount in pixels"),
    })).describe("Array of actions to execute"),
  },
  async ({ actions }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    if (!actions || actions.length === 0) return { content: [{ type: "text", text: "No actions provided." }], isError: true };
    const batchScript = `(function(){
      const actions = ${JSON.stringify(actions)};
      const results = [];
      for (const act of actions) {
        try {
          let el = act.selector ? document.querySelector(act.selector) : null;
          if (!el && act.text && act.type === "click") {
            const t = act.text.toLowerCase();
            for (const e of document.querySelectorAll("a, button, [role='button'], span, div, li, label")) {
              if ((e.innerText || "").trim().toLowerCase().includes(t)) {
                const r = e.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) { el = e; break; }
              }
            }
          }
          switch (act.type) {
            case "click":
              if (!el) { results.push({ type: "click", success: false, error: "Element not found: " + (act.selector || act.text) }); break; }
              el.scrollIntoView({ block: "center", behavior: "instant" });
              el.click();
              results.push({ type: "click", success: true, target: (el.innerText || "").trim().slice(0, 30) });
              break;
            case "fill":
              if (!el) { results.push({ type: "fill", success: false, error: "Element not found: " + act.selector }); break; }
              el.focus();
              if (el.isContentEditable) { el.textContent = act.value || ""; }
              else {
                const proto = el.tagName === "TEXTAREA" ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
                const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
                if (setter) setter.call(el, act.value || ""); else el.value = act.value || "";
              }
              el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
              el.dispatchEvent(new Event("change", { bubbles: true }));
              results.push({ type: "fill", success: true, target: act.selector });
              break;
            case "type":
              if (act.selector && el) el.focus();
              const txt = act.text || act.value || "";
              for (const ch of txt) {
                document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: ch, bubbles: true }));
                document.execCommand("insertText", false, ch);
                document.activeElement.dispatchEvent(new KeyboardEvent("keyup", { key: ch, bubbles: true }));
              }
              results.push({ type: "type", success: true, chars: txt.length });
              break;
            case "press":
              const keyEl = document.activeElement || document.body;
              keyEl.dispatchEvent(new KeyboardEvent("keydown", { key: act.key, bubbles: true }));
              keyEl.dispatchEvent(new KeyboardEvent("keyup", { key: act.key, bubbles: true }));
              results.push({ type: "press", success: true, key: act.key });
              break;
            case "scroll":
              const px = (act.direction === "up" ? -1 : 1) * (act.amount || 500);
              window.scrollBy(0, px);
              results.push({ type: "scroll", success: true, pixels: px });
              break;
            case "check":
            case "uncheck":
              if (!el) { results.push({ type: act.type, success: false, error: "Element not found: " + act.selector }); break; }
              el.checked = act.type === "check";
              el.dispatchEvent(new Event("change", { bubbles: true }));
              results.push({ type: act.type, success: true, checked: el.checked });
              break;
            default:
              results.push({ type: act.type, success: false, error: "Unknown action type" });
          }
        } catch (e) {
          results.push({ type: act.type, success: false, error: e.message });
        }
      }
      return results;
    })()`;
    const results = await ye(batchScript);
    if (!Array.isArray(results)) return { content: [{ type: "text", text: "Batch execution failed." }], isError: true };
    const summary = results.map((r, i) => `${i + 1}. ${r.type}: ${r.success ? "OK" : "FAIL"} ${r.success ? (r.target || r.key || r.chars + " chars" || "") : r.error}`).join("\n");
    const succeeded = results.filter(r => r.success).length;
    return { content: [{ type: "text", text: `Batch: ${succeeded}/${results.length} succeeded\n${summary}` }] };
  }
);

// ── yamil_browser_go_back ─────────────────────────────────────────────
server.tool("yamil_browser_go_back", "Navigate back in the YAMIL Browser history.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await ye("history.back()");
    await new Promise(r => setTimeout(r, 1000));
    const res = await yamilGet("/url"); const { url } = await res.json();
    return { content: [{ type: "text", text: `Back → ${url}` }] };
  }
);

// ── yamil_browser_go_forward ──────────────────────────────────────────
server.tool("yamil_browser_go_forward", "Navigate forward in the YAMIL Browser history.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await ye("history.forward()");
    await new Promise(r => setTimeout(r, 1000));
    const res = await yamilGet("/url"); const { url } = await res.json();
    return { content: [{ type: "text", text: `Forward → ${url}` }] };
  }
);

// ── yamil_browser_content ─────────────────────────────────────────────
server.tool("yamil_browser_content", "Get the visible text content of the YAMIL Browser page.",
  { selector: z.string().optional().describe("CSS selector (default: body)") },
  async ({ selector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const sel = selector || "body";
    const text = await ye(`(document.querySelector(${JSON.stringify(sel)}) || document.body).innerText`);
    return { content: [{ type: "text", text: (text || "").slice(0, 20000) }] };
  }
);

// ── yamil_browser_get_html ────────────────────────────────────────────
server.tool("yamil_browser_get_html", "Get the raw HTML of the YAMIL Browser page.",
  { selector: z.string().optional().describe("CSS selector (default: full page)") },
  async ({ selector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const html = selector
      ? await ye(`(document.querySelector(${JSON.stringify(selector)}) || document.body).innerHTML`)
      : await ye("document.documentElement.outerHTML");
    return { content: [{ type: "text", text: (html || "").slice(0, 30000) }] };
  }
);

// ── yamil_browser_head ────────────────────────────────────────────────
server.tool("yamil_browser_head", "Get the <head> HTML of the YAMIL Browser page.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const html = await ye("document.head.innerHTML");
    return { content: [{ type: "text", text: (html || "").slice(0, 20000) }] };
  }
);

// ── yamil_browser_wait ────────────────────────────────────────────────
server.tool("yamil_browser_wait", "Wait for a CSS selector to appear in the YAMIL Browser page.",
  {
    selector: z.string().describe("CSS selector to wait for"),
    timeout:  z.number().optional().describe("Max wait in ms (default 10000)"),
  },
  async ({ selector, timeout }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const deadline = Date.now() + (timeout ?? 10000);
    while (Date.now() < deadline) {
      const found = await ye(`(function(){
        const el = document.querySelector(${JSON.stringify(selector)});
        if (!el) return false;
        const s = getComputedStyle(el);
        return s.display !== "none" && s.visibility !== "hidden" && el.getBoundingClientRect().height > 0;
      })()`);
      if (found) return { content: [{ type: "text", text: `Selector visible: ${selector}` }] };
      await new Promise(r => setTimeout(r, 200));
    }
    logToolError("yamil_browser_wait", { selector, timeout }, `Timeout waiting for visible: ${selector}`, await yamilPageUrl());
    return { content: [{ type: "text", text: `Timeout waiting for visible: ${selector}` }], isError: true };
  }
);

// ── yamil_browser_observe ─────────────────────────────────────────────
server.tool("yamil_browser_observe", "List interactive elements on the YAMIL Browser page.",
  { instruction: z.string().optional().describe("Natural language filter (uses LLM if provided)") },
  async ({ instruction }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const elements = await ye(`(function(){
      const tags = ["a[href]","button","input:not([type='hidden'])","select","textarea","[role='button']","[role='link']","[role='checkbox']","[role='radio']","[role='tab']","[role='menuitem']","[role='switch']","[role='combobox']","[role='option']","[role='spinbutton']","[role='slider']","[onclick]","[tabindex]:not([tabindex='-1'])","summary","details>summary","[contenteditable='true']","[contenteditable='']"];
      const found = []; const seen = new Set();
      for (const sel of tags) {
        try {
          for (const el of document.querySelectorAll(sel)) {
            if (seen.has(el)) continue; seen.add(el);
            const r = el.getBoundingClientRect();
            if (r.width === 0 && r.height === 0) continue;
            const s = getComputedStyle(el);
            if (s.display === "none" || s.visibility === "hidden") continue;
            const role = el.getAttribute("role") || null;
            const ariaLabel = el.getAttribute("aria-label") || null;
            let uniqSel = el.tagName.toLowerCase();
            if (el.id) uniqSel = "#" + el.id;
            else if (el.name) uniqSel = el.tagName.toLowerCase() + '[name="' + el.name + '"]';
            else if (el.className && typeof el.className === "string") {
              const cls = el.className.trim().split(/\\s+/).slice(0,2).join(".");
              if (cls) uniqSel = el.tagName.toLowerCase() + "." + cls;
            }
            found.push({ tag: el.tagName.toLowerCase(), type: el.type||null, id: el.id||null, name: el.name||null, placeholder: el.placeholder||null, role, text: (el.innerText||el.value||ariaLabel||"").slice(0,80).trim(), href: el.href||null, ariaLabel, disabled: el.disabled || el.getAttribute("aria-disabled") === "true" || false, checked: el.checked ?? (el.getAttribute("aria-checked") === "true") ?? null, selector: uniqSel });
          }
        } catch(_) {}
      }
      return found.slice(0, 150);
    })()`);
    const text = JSON.stringify(elements, null, 2);
    const a11y = await getYamilA11yTree();
    if (!instruction) return { content: [{ type: "text", text: text }] };
    if (!_ollamaAvailable && !_usingGemini && !getAnthropic()) {
      return { content: [{ type: "text", text: `Instruction: ${instruction}\n\nInteractive elements on page:\n${text}${a11y ? `\n\nAccessibility Tree:\n${a11y}` : ""}` }] };
    }
    const a11ySection = a11y ? `\n\nAccessibility Tree:\n${a11y}` : "";
    const response = await anthropic.messages.create({
      model: "claude-haiku-4-5-20251001", max_tokens: 2048,
      messages: [{ role: "user", content: `From these page elements, ${instruction}:\n\n${text}${a11ySection}\n\nReturn only the relevant elements as a JSON array.` }],
    });
    return { content: [{ type: "text", text: response.content[0].text.trim() }] };
  }
);

// ── yamil_browser_get_cookies ─────────────────────────────────────────
server.tool("yamil_browser_get_cookies", "Get cookies from the YAMIL Browser current page.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const cookies = await ye(`document.cookie`);
    return { content: [{ type: "text", text: cookies || "(no cookies)" }] };
  }
);

// ── yamil_browser_clear_cookies ───────────────────────────────────────
server.tool("yamil_browser_clear_cookies", "Clear cookies for the current page in the YAMIL Browser.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await ye(`document.cookie.split(";").forEach(c => { document.cookie = c.replace(/^ +/, "").replace(/=.*/, "=;expires=Thu, 01 Jan 1970 00:00:00 UTC;path=/"); })`);
    return { content: [{ type: "text", text: "Cookies cleared." }] };
  }
);

// ── yamil_browser_network_idle ────────────────────────────────────────
server.tool("yamil_browser_network_idle", "Wait until all network requests have settled.",
  { timeout: z.number().optional().describe("Max wait in ms (default 15000)") },
  async ({ timeout }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    await ye(`(function(){
      if (window.__yamilNetIdle) return;
      window.__yamilNetIdle = { pending: 0, lastActivity: Date.now() };
      const orig = window.fetch;
      window.fetch = function(...args) {
        window.__yamilNetIdle.pending++;
        window.__yamilNetIdle.lastActivity = Date.now();
        return orig.apply(this, args).finally(() => {
          window.__yamilNetIdle.pending--;
          window.__yamilNetIdle.lastActivity = Date.now();
        });
      };
      const xhrOpen = XMLHttpRequest.prototype.open;
      const xhrSend = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(...a) { this.__yamilTracked = true; return xhrOpen.apply(this, a); };
      XMLHttpRequest.prototype.send = function(...a) {
        if (this.__yamilTracked) {
          window.__yamilNetIdle.pending++;
          window.__yamilNetIdle.lastActivity = Date.now();
          this.addEventListener("loadend", () => {
            window.__yamilNetIdle.pending--;
            window.__yamilNetIdle.lastActivity = Date.now();
          }, { once: true });
        }
        return xhrSend.apply(this, a);
      };
    })()`);
    const deadline = Date.now() + (timeout ?? 15000);
    while (Date.now() < deadline) {
      const state = await ye("window.__yamilNetIdle ? { p: window.__yamilNetIdle.pending, t: Date.now() - window.__yamilNetIdle.lastActivity } : null");
      if (state && state.p === 0 && state.t >= 500) {
        const res = await yamilGet("/url"); const { url } = await res.json();
        return { content: [{ type: "text", text: `Network idle (0 pending, ${state.t}ms quiet) at: ${url}` }] };
      }
      await new Promise(r => setTimeout(r, 300));
    }
    const url = await yamilPageUrl();
    logToolError("yamil_browser_network_idle", { timeout }, `Network idle timeout`, url);
    return { content: [{ type: "text", text: `Timeout — page: ${url}` }] };
  }
);

// ── yamil_browser_dialog ──────────────────────────────────────────────
server.tool("yamil_browser_dialog", "Handle the next browser dialog (alert, confirm, prompt).",
  {
    action:     z.enum(["accept", "dismiss"]).describe("Accept or dismiss the dialog"),
    promptText: z.string().optional().describe("Text to enter if the dialog is a prompt input"),
  },
  async ({ action, promptText }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const res = await yamilPost("/dialog", { action, promptText });
    const data = await res.json();
    if (data.error) {
      logToolError("yamil_browser_dialog", { action, promptText }, data.error, await yamilPageUrl());
      return { content: [{ type: "text", text: `Error: ${data.error}` }], isError: true };
    }
    return { content: [{ type: "text", text: `Dialog handler set: ${action}${promptText ? ` with text "${promptText}"` : ""}` }] };
  }
);

// ── yamil_browser_screenshot_element (with empty image guard) ─────────
server.tool("yamil_browser_screenshot_element", "Take a screenshot of a specific element.",
  {
    selector:      z.string().describe("CSS selector of the element to screenshot"),
    frameSelector: z.string().optional().describe("CSS selector of the iframe the element is inside"),
  },
  async ({ selector, frameSelector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res = await yamilPost("/screenshot-element", { selector });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        logToolError("yamil_browser_screenshot_element", { selector, frameSelector }, err.error || `HTTP ${res.status}`, await yamilPageUrl());
        return { content: [{ type: "text", text: `Element screenshot failed: ${err.error || res.status}` }], isError: true };
      }
      const buf = Buffer.from(await res.arrayBuffer());
      const b64 = buf.toString("base64");
      if (!b64 || b64.length < 100) {
        return { content: [{ type: "text", text: "Element screenshot returned empty image." }], isError: true };
      }
      return { content: [{ type: "image", data: b64, mimeType: "image/png" }] };
    } catch (e) {
      logToolError("yamil_browser_screenshot_element", { selector, frameSelector }, e.message, await yamilPageUrl());
      return { content: [{ type: "text", text: `Screenshot error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_drag ───────────────────────────────────────────────
server.tool("yamil_browser_drag", "Drag from one element to another.",
  {
    sourceSelector: z.string().describe("CSS selector of the element to drag from"),
    targetSelector: z.string().describe("CSS selector of the element to drop onto"),
  },
  async ({ sourceSelector, targetSelector }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const res = await yamilPost("/drag", { sourceSelector, targetSelector });
    const data = await res.json();
    if (data.error) {
      logToolError("yamil_browser_drag", { sourceSelector, targetSelector }, data.error, await yamilPageUrl());
      return { content: [{ type: "text", text: `Drag failed: ${data.error}` }], isError: true };
    }
    return { content: [{ type: "text", text: `Dragged ${sourceSelector} → ${targetSelector}` }] };
  }
);

// ── yamil_browser_set_files ──────────────────────────────────────────
server.tool("yamil_browser_set_files", "Set files on a file input element.",
  {
    selector:  z.string().describe("CSS selector of the file input"),
    filePaths: z.array(z.string()).describe("Array of absolute file paths to set"),
  },
  async ({ selector, filePaths }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const script = `(function(){
      const el = document.querySelector(${JSON.stringify(selector)});
      if (!el || el.type !== "file") return { error: "file input not found" };
      const dt = new DataTransfer();
      const fileData = ${JSON.stringify(filePaths)};
      for (const fp of fileData) {
        const name = fp.split(/[\\\\/]/).pop();
        dt.items.add(new File([""], name, { type: "application/octet-stream" }));
      }
      el.files = dt.files;
      el.dispatchEvent(new Event("change", { bubbles: true }));
      return { ok: true, count: dt.files.length };
    })()`;
    const r = await ye(script);
    if (r?.error) {
      logToolError("yamil_browser_set_files", { selector, filePaths }, r.error, await yamilPageUrl());
      return { content: [{ type: "text", text: r.error }], isError: true };
    }
    return { content: [{ type: "text", text: `Set ${r?.count || 0} file(s) on ${selector}` }] };
  }
);

// ── yamil_browser_list_tabs ──────────────────────────────────────────
server.tool("yamil_browser_list_tabs", "List all open browser tabs.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res = await yamilGet("/tabs");
      const data = await res.json();
      return { content: [{ type: "text", text: JSON.stringify(data.tabs || [], null, 2) }] };
    } catch (e) {
      const info = await ye(`({ url: location.href, title: document.title })`);
      const tabs = [{ index: 0, type: "yamil", url: info?.url || "", title: info?.title || "", active: true }];
      return { content: [{ type: "text", text: JSON.stringify(tabs, null, 2) }] };
    }
  }
);

// ── yamil_browser_close_tab ──────────────────────────────────────────
server.tool("yamil_browser_close_tab", "Close a browser tab by index or URL substring.",
  {
    index:       z.number().optional().describe("Zero-based tab index to close"),
    urlContains: z.string().optional().describe("Close the first tab whose URL contains this string"),
  },
  async ({ index, urlContains }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      if (urlContains) {
        const tabsRes = await yamilGet("/tabs");
        const tabsData = await tabsRes.json();
        const match = (tabsData.tabs || []).find(t => (t.url || "").includes(urlContains));
        if (!match) { logToolError("yamil_browser_close_tab", { urlContains }, `No tab found containing: ${urlContains}`, await yamilPageUrl()); return { content: [{ type: "text", text: `No tab found containing: ${urlContains}` }], isError: true }; }
        await yamilPost("/close-tab", { id: match.id });
      } else if (index !== undefined) {
        const tabsRes = await yamilGet("/tabs");
        const tabsData = await tabsRes.json();
        const target = (tabsData.tabs || [])[index];
        if (!target) { logToolError("yamil_browser_close_tab", { index }, `No tab at index ${index}`, await yamilPageUrl()); return { content: [{ type: "text", text: `No tab at index ${index}` }], isError: true }; }
        await yamilPost("/close-tab", { id: target.id });
      } else {
        await yamilPost("/close-tab", {});
      }
      const res = await yamilGet("/tabs");
      const data = await res.json();
      return { content: [{ type: "text", text: `Tab closed. ${(data.tabs || []).length} tab(s) remaining.` }] };
    } catch (e) {
      logToolError("yamil_browser_close_tab", { index, urlContains }, e.message, await yamilPageUrl());
      return { content: [{ type: "text", text: `Close tab error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_new_tab ─────────────────────────────────────────────
server.tool("yamil_browser_new_tab", "Open a new browser tab.",
  {
    url:  z.string().optional().describe("URL to navigate to in the new tab"),
    type: z.enum(["yamil", "stealth"]).optional().describe("Tab type (default: yamil)"),
  },
  async ({ url, type }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const tabType = type || "yamil";
    const res = await yamilPost("/new-tab", { url: url || "", type: tabType });
    const data = await res.json();
    if (data.error) { logToolError("yamil_browser_new_tab", { url, type: tabType }, data.error, await yamilPageUrl()); return { content: [{ type: "text", text: `Error: ${data.error}` }], isError: true }; }
    return { content: [{ type: "text", text: `New ${tabType} tab opened${url ? `: ${url}` : ""}` }] };
  }
);

// ── yamil_browser_switch_tab ──────────────────────────────────────────
server.tool("yamil_browser_switch_tab", "Switch to a different open tab.",
  {
    index:       z.number().optional().describe("Zero-based tab index"),
    urlContains: z.string().optional().describe("Switch to first tab whose URL contains this string"),
  },
  async ({ index, urlContains }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      if (urlContains) {
        const tabsRes = await yamilGet("/tabs");
        const tabsData = await tabsRes.json();
        const match = (tabsData.tabs || []).find(t => (t.url || "").includes(urlContains));
        if (!match) { logToolError("yamil_browser_switch_tab", { urlContains }, `No tab found containing: ${urlContains}`, await yamilPageUrl()); return { content: [{ type: "text", text: `No tab found containing: ${urlContains}` }], isError: true }; }
        await yamilPost("/switch-tab", { id: match.id });
        return { content: [{ type: "text", text: `Switched to ${match.type || "yamil"} tab: ${match.url}` }] };
      } else if (index !== undefined) {
        const tabsRes = await yamilGet("/tabs");
        const tabsData = await tabsRes.json();
        const target = (tabsData.tabs || [])[index];
        if (!target) { logToolError("yamil_browser_switch_tab", { index }, `No tab at index ${index}`, await yamilPageUrl()); return { content: [{ type: "text", text: `No tab at index ${index}` }], isError: true }; }
        await yamilPost("/switch-tab", { id: target.id });
        return { content: [{ type: "text", text: `Switched to ${target.type || "yamil"} tab: ${target.url}` }] };
      }
      return { content: [{ type: "text", text: "Provide index or urlContains" }], isError: true };
    } catch (e) {
      logToolError("yamil_browser_switch_tab", { index, urlContains }, e.message, await yamilPageUrl());
      return { content: [{ type: "text", text: `Switch tab error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_resize ─────────────────────────────────────────────
server.tool("yamil_browser_resize", "Resize the YAMIL Browser viewport.",
  {
    width:  z.number().describe("Viewport width in pixels"),
    height: z.number().describe("Viewport height in pixels"),
  },
  async ({ width, height }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const infoRes = await yamilGet("/active-tab-info");
      const info = await infoRes.json();
      if (info && info.type === "stealth" && info.sessionId) {
        await fetch(`http://127.0.0.1:4000/sessions/${info.sessionId}/resize`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ width, height }),
          signal: AbortSignal.timeout(10000),
        });
      }
    } catch (_) {}
    return { content: [{ type: "text", text: `Viewport resized to ${width}x${height}` }] };
  }
);

// ── yamil_browser_pdf ────────────────────────────────────────────────
server.tool("yamil_browser_pdf", "Save the current page as a PDF file.",
  { path: z.string().describe("Absolute file path to save the PDF") },
  async ({ path: pdfPath }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res = await fetch(`${YAMIL_CTRL}/print-pdf`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        signal: AbortSignal.timeout(30000),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        logToolError("yamil_browser_pdf", { path: pdfPath }, err.error || `HTTP ${res.status}`, await yamilPageUrl());
        return { content: [{ type: "text", text: `PDF failed: ${err.error || res.status}` }], isError: true };
      }
      const buf = Buffer.from(await res.arrayBuffer());
      writeFileSync(pdfPath, buf);
      return { content: [{ type: "text", text: `PDF saved: ${pdfPath} (${(buf.length / 1024).toFixed(1)} KB)` }] };
    } catch (e) {
      logToolError("yamil_browser_pdf", { path: pdfPath }, e.message, await yamilPageUrl());
      return { content: [{ type: "text", text: `PDF error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_download ───────────────────────────────────────────
server.tool("yamil_browser_download", "Wait for the next file download triggered by a click/navigation.",
  {
    saveDir: z.string().optional().describe("Directory to save the file"),
    timeout: z.number().optional().describe("Max wait time in ms (default 30000)"),
  },
  async ({ saveDir, timeout }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const setupScript = `(function(){
      const wv = document.getElementById('screen');
      if (!wv) return { error: 'no webview' };
      window.__yamilDownload = { pending: true, path: null, error: null };
      wv.session.once('will-download', (event, item) => {
        const savePath = ${JSON.stringify(saveDir || "")} ?
          require('path').join(${JSON.stringify(saveDir || "")}, item.getFilename()) :
          require('path').join(require('os').tmpdir(), item.getFilename());
        item.setSavePath(savePath);
        item.once('done', (e, state) => {
          window.__yamilDownload.pending = false;
          window.__yamilDownload.path = state === 'completed' ? savePath : null;
          window.__yamilDownload.error = state !== 'completed' ? state : null;
        });
      });
      return { listening: true };
    })()`;
    await yamilPost("/renderer-eval", { script: setupScript });
    return { content: [{ type: "text", text: `Download listener active. Trigger the download now, then check result.` }] };
  }
);


// ── Shared action executor for act and run_task ──────────────────────
async function executeYamilAction(act) {
  switch (act.action) {
    case "click": {
      const r = await ye(`(function(){
        let el = document.querySelector(${JSON.stringify(act.selector)});
        if (!el) return { found: false };
        const s = getComputedStyle(el);
        if (s.display === "none" || s.visibility === "hidden") return { found: false, reason: "hidden" };
        el.scrollIntoView({ block: "center", behavior: "instant" });
        el.focus();
        el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
        el.dispatchEvent(new MouseEvent("mousedown", { bubbles: true, cancelable: true }));
        el.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, cancelable: true }));
        el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
        return { found: true };
      })()`);
      return { ok: r?.found, text: r?.found ? `Clicked: ${act.selector}` : `Click target not found: ${act.selector}` };
    }
    case "fill": {
      const monacoFill = await ye(`(function(){
        const el = document.querySelector(${JSON.stringify(act.selector)});
        if (el && el.closest && el.closest(".monaco-editor")) return true;
        return false;
      })()`);
      if (monacoFill) {
        const mr = await monacoSetValue(act.value);
        if (mr?.monaco) return { ok: true, text: `Filled Monaco editor via API` };
      }
      await ye(`(function(){
        const el = document.querySelector(${JSON.stringify(act.selector)});
        if (!el) return;
        el.focus();
        el.dispatchEvent(new Event("focus", { bubbles: true }));
        if (el.isContentEditable) { el.textContent = ${JSON.stringify(act.value)}; }
        else {
          const proto = el.tagName === "TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
          const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
          if (setter) setter.call(el, ${JSON.stringify(act.value)}); else el.value = ${JSON.stringify(act.value)};
        }
        el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText" }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
        el.dispatchEvent(new Event("blur", { bubbles: true }));
      })()`);
      return { ok: true, text: `Filled: ${act.selector}` };
    }
    case "navigate":
      await yamilPost("/navigate", { url: act.url });
      return { ok: true, text: `Navigated to: ${act.url}` };
    case "press":
      await ye(`(function(){
        const el = document.activeElement || document.body;
        ["keydown","keypress","keyup"].forEach(t =>
          el.dispatchEvent(new KeyboardEvent(t, { key: ${JSON.stringify(act.key)}, code: ${JSON.stringify(act.key)}, bubbles: true, cancelable: true }))
        );
      })()`);
      return { ok: true, text: `Pressed: ${act.key}` };
    case "scroll":
      await ye(`window.scrollBy(0, ${act.direction === "down" ? act.amount || 500 : -(act.amount || 500)})`);
      return { ok: true, text: `Scrolled ${act.direction}` };
    case "select":
      await ye(`(function(){ const el=document.querySelector(${JSON.stringify(act.selector)}); if(el&&el.tagName==="SELECT"){el.value=${JSON.stringify(act.value)};el.dispatchEvent(new Event("change",{bubbles:true}));} })()`);
      return { ok: true, text: `Selected: ${act.value}` };
    case "wait":
      await new Promise(r => setTimeout(r, act.ms || 1000));
      return { ok: true, text: `Waited ${act.ms || 1000}ms` };
    case "none":
      return { ok: true, text: `No action: ${act.reason}` };
    case "fail":
      return { ok: false, text: `Failed: ${act.reason}` };
    default:
      return { ok: false, text: `Unknown action: ${act.action}` };
  }
}

// ── yamil_browser_act ─────────────────────────────────────────────────
server.tool(
  "yamil_browser_act",
  "Execute a browser action in the YAMIL Browser via natural language (uses Claude vision).",
  { instruction: z.string().describe("Natural language, e.g. 'click the Login button'") },
  async ({ instruction }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const urlRes = await yamilGet("/url").then(r => r.json());
    const pageUrl = urlRes.url || "";
    const cached = cacheGet(pageUrl, instruction);
    if (cached) {
      console.error(`[CACHE HIT] yamil_browser_act: "${instruction}"`);
      const result = cached.args
        ? await executeYamilCUAction(cached.action, cached.args)
        : await executeYamilAction(cached.action);
      if (!result.ok) logToolError("yamil_browser_act", { instruction, cache: "hit" }, result.text, pageUrl);
      return { content: [{ type: "text", text: `[CACHE HIT] ${result.text}` }], isError: !result.ok };
    }
    console.error(`[CACHE MISS] yamil_browser_act: "${instruction}"`);
    const [ssBuf, html] = await Promise.all([
      yamilScreenshotBuf(),
      ye("document.body.innerHTML").then(h => (h||"").slice(0, 20000)),
    ]);
    const ssBase64 = ssBuf.toString("base64");
    if (!_ollamaAvailable && !_usingGemini && !getAnthropic()) {
      if (!ssBase64 || ssBase64.length < 100) {
        return { content: [
          { type: "text", text: `No LLM available — returning context for you to interpret.\nPage: ${pageUrl}\nInstruction: "${instruction}"\nUse yamil_browser_click/fill/type/press tools to execute the action.` },
        ]};
      }
      return { content: [
        { type: "image", data: ssBase64, mimeType: "image/png" },
        { type: "text", text: `No LLM available — returning context for you to interpret.\nPage: ${pageUrl}\nInstruction: "${instruction}"\nUse yamil_browser_click/fill/type/press tools to execute the action.` },
      ]};
    }
    if (_usingGemini) {
      try {
        const a11y = await getYamilA11yTree();
        const cuInstruction = a11y ? `${instruction}\n\nAccessibility Tree:\n${a11y}` : instruction;
        console.error(`[CU] yamil_browser_act: "${instruction}"${a11y ? " [A11Y]" : ""}`);
        const cu = await geminiComputerUse(ssBase64, cuInstruction);
        if (cu?.action) {
          if (cu.safetyDecision === "blocked") {
            logToolError("yamil_browser_act", { instruction }, `Safety blocked: ${cu.reasoning}`, pageUrl);
            return { content: [{ type: "text", text: `Action blocked by safety: ${cu.reasoning}` }], isError: true };
          }
          if (cu.safetyDecision === "require_confirmation") {
            return { content: [{ type: "text", text: `Action requires confirmation: ${cu.reasoning}\nAction: ${cu.action} ${JSON.stringify(cu.args)}` }] };
          }
          const result = await executeYamilCUAction(cu.action, cu.args);
          if (result.ok) cacheSet(pageUrl, instruction, cu.action, cu.args);
          if (!result.ok) logToolError("yamil_browser_act", { instruction, cuAction: cu.action }, result.text, pageUrl);
          return { content: [{ type: "text", text: `[CU] ${result.text}${cu.reasoning ? `\n${cu.reasoning}` : ""}` }], isError: !result.ok };
        }
      } catch (e) {
        console.error(`[CU-FALLBACK] yamil_browser_act CU failed: ${e.message}`);
        logToolError("yamil_browser_act", { instruction }, `CU failed: ${e.message}`, pageUrl);
      }
    }
    console.error(`[CU-FALLBACK] yamil_browser_act: using prompt-based approach`);
    const a11y = await getYamilA11yTree();
    const prompt = `You are controlling a web browser. Analyze the screenshot and HTML to determine the best action.
Current page: ${pageUrl}
Page HTML (partial): ${html}
${a11y ? `\nAccessibility Tree:\n${a11y}\n` : ""}
User instruction: "${instruction}"
Return ONLY valid JSON (no markdown fences):
- {"action":"click","selector":"CSS_SELECTOR"}
- {"action":"fill","selector":"CSS_SELECTOR","value":"TEXT"}
- {"action":"navigate","url":"URL"}
- {"action":"press","key":"KEY_NAME"}
- {"action":"scroll","direction":"up|down","amount":500}
- {"action":"select","selector":"CSS_SELECTOR","value":"OPTION_VALUE"}
- {"action":"none","reason":"EXPLANATION"}
Use specific, unique CSS selectors. Prefer #id selectors, then [name=...], then tag.class.`;
    const msgContent = [];
    if (ssBase64 && ssBase64.length >= 100) {
      msgContent.push({ type: "image", source: { type: "base64", media_type: "image/png", data: ssBase64 } });
    }
    msgContent.push({ type: "text", text: prompt });
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-6", max_tokens: 512,
      messages: [{ role: "user", content: msgContent }],
    });
    const raw = response.content[0].text.trim();
    const act = JSON.parse(extractJSON(raw) || "{}");
    const result = await executeYamilAction(act);
    if (result.ok && CACHEABLE_ACTIONS.has(act.action)) cacheSet(pageUrl, instruction, act, null);
    if (!result.ok && act.action !== "none") logToolError("yamil_browser_act", { instruction, action: act }, result.text, pageUrl);
    return { content: [{ type: "text", text: result.text }], isError: !result.ok && act.action !== "none" };
  }
);

// ── yamil_browser_extract ─────────────────────────────────────────────
server.tool(
  "yamil_browser_extract",
  "Extract structured data from the current page using natural language. Returns JSON.",
  {
    instruction: z.string().describe("What to extract, e.g. 'all product names and prices as a JSON array'"),
    selector:    z.string().optional().describe("CSS selector to scope extraction (default: body)"),
    schema:      z.string().optional().describe("Optional JSON schema string describing expected output shape"),
  },
  async ({ instruction, selector, schema }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    const [html, ssBuf, a11y] = await Promise.all([
      ye(`(document.querySelector(${JSON.stringify(selector || "body")}) || document.body).innerHTML`),
      yamilScreenshotBuf(),
      getYamilA11yTree(),
    ]);
    const ssBase64 = ssBuf.toString("base64");
    if (!_ollamaAvailable && !_usingGemini && !getAnthropic()) {
      const content = [];
      if (ssBase64 && ssBase64.length >= 100) {
        content.push({ type: "image", data: ssBase64, mimeType: "image/png" });
      }
      content.push({ type: "text", text: `Extract: ${instruction}\n${selector ? `Scoped to: ${selector}\n` : ""}${schema ? `Schema: ${schema}\n` : ""}HTML:\n${(html||"").slice(0,8000)}${a11y ? `\n\nAccessibility Tree:\n${a11y}` : ""}` });
      return { content };
    }
    const schemaHint = schema ? `\nReturn data matching this JSON schema: ${schema}` : "";
    const a11ySection = a11y ? `\n\nAccessibility Tree:\n${a11y}` : "";
    const promptText = `Extract the following from this web page (use both the screenshot and HTML):\n\n${instruction}${schemaHint}\n\n${selector ? `Scoped to: ${selector}\n` : ""}HTML:\n${(html||"").slice(0,20000)}${a11ySection}\n\nReturn ONLY valid JSON, no explanation or markdown fences.`;
    const msgContent = [];
    if (ssBase64 && ssBase64.length >= 100) {
      msgContent.push({ type: "image", source: { type: "base64", media_type: "image/png", data: ssBase64 } });
    }
    msgContent.push({ type: "text", text: promptText });
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-6", max_tokens: 4096,
      messages: [{ role: "user", content: msgContent }],
    });
    let text = response.content[0].text.trim();
    if (schema && text) {
      try {
        const parsed = JSON.parse(text);
        const expectedKeys = Object.keys(JSON.parse(schema).properties || JSON.parse(schema));
        const hasKeys = expectedKeys.length === 0 || expectedKeys.some(k => k in parsed || (Array.isArray(parsed) && parsed.length > 0));
        if (!hasKeys) {
          const retryContent = [];
          if (ssBase64 && ssBase64.length >= 100) {
            retryContent.push({ type: "image", source: { type: "base64", media_type: "image/png", data: ssBase64 } });
          }
          retryContent.push({ type: "text", text: `The previous extraction didn't match the expected schema.\nSchema: ${schema}\nPrevious result: ${text}\n\nPlease re-extract:\n${instruction}\n\nReturn ONLY valid JSON matching the schema.` });
          const retry = await anthropic.messages.create({
            model: "claude-sonnet-4-6", max_tokens: 4096,
            messages: [{ role: "user", content: retryContent }],
          });
          text = retry.content[0].text.trim();
        }
      } catch { /* parse failed, return as-is */ }
    }
    return { content: [{ type: "text", text: text }] };
  }
);

// ── yamil_browser_run_task ────────────────────────────────────────────
server.tool(
  "yamil_browser_run_task",
  "Autonomously complete a high-level goal by looping: screenshot → decide → act → repeat.",
  {
    goal:     z.string().describe("High-level goal"),
    maxSteps: z.number().optional().describe("Maximum steps before giving up (default 15)"),
  },
  async ({ goal, maxSteps }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    let sessionId;
    try {
      const infoRes = await yamilGet("/active-tab-info");
      const info = await infoRes.json();
      sessionId = info?.sessionId;
    } catch {}
    if (!sessionId) {
      return { content: [{ type: "text", text: "No active stealth session. Navigate to a page first." }], isError: true };
    }
    try {
      const res = await fetch(`${BROWSER_SVC_URL}/sessions/${sessionId}/run-task`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ goal, maxSteps: maxSteps ?? 15 }),
        signal: AbortSignal.timeout(300000),
      });
      const data = await res.json();
      if (!res.ok) return { content: [{ type: "text", text: `run-task error: ${data.error || res.statusText}` }], isError: true };
      const stepsText = (data.steps || []).map((h, i) => `${i + 1}. ${h}`).join("\n");
      if (data.done) {
        return { content: [{ type: "text", text: `Done in ${data.stepCount} steps.\n${data.result}\n\nSteps:\n${stepsText}` }] };
      }
      return { content: [{ type: "text", text: `${data.result}\nSteps:\n${stepsText}` }], isError: true };
    } catch (e) {
      return { content: [{ type: "text", text: `run-task failed: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_bookmark ─────────────────────────────────────────────
server.tool("yamil_browser_bookmark", "Add or remove a bookmark in YAMIL Browser.",
  {
    action:   z.enum(["add", "remove"]).describe("Whether to add or remove a bookmark"),
    url:      z.string().optional().describe("URL to bookmark (defaults to current page for add)"),
    title:    z.string().optional().describe("Bookmark title"),
    tags:     z.array(z.string()).optional().describe("Tags for the bookmark"),
    category: z.string().optional().describe("Category for the bookmark"),
  },
  async ({ action, url, title, tags, category }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      if (action === "add") {
        let bmUrl = url;
        let bmTitle = title;
        if (!bmUrl) {
          const r = await yamilGet("/url");
          const d = await r.json();
          bmUrl = d.url;
          if (!bmTitle) bmTitle = d.title || bmUrl;
        }
        const res = await fetch(`${YAMIL_CTRL}/bookmarks`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: bmUrl, title: bmTitle || bmUrl, tags: tags || [], category: category || "" }),
          signal: AbortSignal.timeout(10000),
        });
        const data = await res.json();
        if (data.error) return { content: [{ type: "text", text: `Error: ${data.error}` }], isError: true };
        return { content: [{ type: "text", text: `Bookmarked: "${data.bookmark?.title || bmTitle}" — ${bmUrl}` }] };
      } else {
        if (!url) return { content: [{ type: "text", text: "URL required to remove a bookmark." }], isError: true };
        const res = await fetch(`${YAMIL_CTRL}/bookmarks`, {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url }),
          signal: AbortSignal.timeout(10000),
        });
        const data = await res.json();
        if (data.error) return { content: [{ type: "text", text: `Error: ${data.error}` }], isError: true };
        return { content: [{ type: "text", text: `Bookmark removed: ${url}` }] };
      }
    } catch (e) {
      return { content: [{ type: "text", text: `Bookmark error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_bookmarks ───────────────────────────────────────────
server.tool("yamil_browser_bookmarks", "List all bookmarks in YAMIL Browser.", {},
  async () => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res = await yamilGet("/bookmarks");
      const data = await res.json();
      const bookmarks = data.bookmarks || [];
      if (bookmarks.length === 0) return { content: [{ type: "text", text: "No bookmarks saved." }] };
      return { content: [{ type: "text", text: JSON.stringify(bookmarks, null, 2) }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Error listing bookmarks: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_bookmark_search ─────────────────────────────────────
server.tool("yamil_browser_bookmark_search", "Search bookmarks by query.",
  { query: z.string().describe("Search query") },
  async ({ query }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res = await yamilGet(`/bookmarks?query=${encodeURIComponent(query)}`);
      const data = await res.json();
      const bookmarks = data.bookmarks || [];
      if (bookmarks.length === 0) return { content: [{ type: "text", text: `No bookmarks matching "${query}".` }] };
      return { content: [{ type: "text", text: JSON.stringify(bookmarks, null, 2) }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Bookmark search error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_history ─────────────────────────────────────────────
server.tool("yamil_browser_history", "List browsing history from YAMIL Browser.",
  { query: z.string().optional().describe("Optional search query to filter history") },
  async ({ query }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const path = query ? `/history?query=${encodeURIComponent(query)}` : '/history';
      const res = await yamilGet(path);
      const data = await res.json();
      const history = data.history || [];
      if (history.length === 0) return { content: [{ type: "text", text: query ? `No history matching "${query}".` : "No browsing history." }] };
      const shown = history.slice(0, 50);
      return { content: [{ type: "text", text: JSON.stringify(shown, null, 2) + (history.length > 50 ? `\n... and ${history.length - 50} more entries` : '') }] };
    } catch (e) {
      return { content: [{ type: "text", text: `History error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_zoom ───────────────────────────────────────────────
server.tool("yamil_browser_zoom", "Zoom in, out, or reset the active tab's zoom level.",
  { action: z.enum(["in", "out", "reset"]).describe("Zoom action") },
  async ({ action }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      const res = await yamilPost('/zoom', { action });
      const data = await res.json();
      return { content: [{ type: "text", text: data.ok ? `Zoom ${action}: level ${data.zoom || 0}` : (data.error || 'Zoom failed') }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Zoom error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_skills ──────────────────────────────────────────────
server.tool("yamil_browser_skills", "List or run AI skills in YAMIL Browser.",
  {
    action: z.enum(["list", "run"]).describe("'list' to get all skills, 'run' to execute a skill by id"),
    skillId: z.string().optional().describe("Skill ID to run (required if action is 'run')"),
  },
  async ({ action, skillId }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      if (action === "list") {
        const res = await ye(`JSON.stringify(window._yamil.skills.getAll().map(s => ({id:s.id, name:s.name, prompt:s.prompt})))`);
        return { content: [{ type: "text", text: res || "[]" }] };
      } else {
        const res = await ye(`(async () => { const skills = window._yamil.skills.getAll(); const s = skills.find(x => x.id === '${(skillId || "").replace(/'/g, "\\'")}'); if (!s) return 'Skill not found'; await window._yamil.skills.run(s); return 'Running skill: ' + s.name; })()`);
        return { content: [{ type: "text", text: String(res) }] };
      }
    } catch (e) {
      return { content: [{ type: "text", text: `Skills error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_ai_privacy ─────────────────────────────────────────
server.tool("yamil_browser_ai_privacy", "Check or toggle AI page visibility for the current site.",
  {
    action: z.enum(["status", "toggle", "list-blocked"]).describe("'status' checks current site, 'toggle' flips visibility, 'list-blocked' shows all blocked domains"),
  },
  async ({ action }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      if (action === "status") {
        const res = await ye(`window._yamil.aiPrivacy.isBlocked() ? 'AI is BLOCKED for this page' : 'AI can see this page'`);
        return { content: [{ type: "text", text: String(res) }] };
      } else if (action === "toggle") {
        const res = await ye(`(function(){ window._yamil.aiPrivacy.toggle(); return window._yamil.aiPrivacy.isBlocked() ? 'AI is now BLOCKED' : 'AI is now ALLOWED'; })()`);
        return { content: [{ type: "text", text: String(res) }] };
      } else {
        const res = await ye(`JSON.stringify(window._yamil.aiPrivacy.getBlockedDomains())`);
        return { content: [{ type: "text", text: res || "[]" }] };
      }
    } catch (e) {
      return { content: [{ type: "text", text: `Privacy error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_tab_context ────────────────────────────────────────
server.tool("yamil_browser_tab_context", "Get content/context from a specific tab or all tabs.",
  {
    tabIndex: z.number().optional().describe("1-based tab index. Omit to get all tabs."),
  },
  async ({ tabIndex }) => {
    if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
    try {
      if (tabIndex) {
        const res = await ye(`(async () => { const t = window._yamil.tabs[${tabIndex - 1}]; if (!t) return 'Tab not found'; const ctx = await window._yamil.getTabContext(t.id); return JSON.stringify(ctx); })()`);
        return { content: [{ type: "text", text: String(res) }] };
      } else {
        const res = await ye(`(async () => { const results = []; for (let i = 0; i < Math.min(window._yamil.tabs.length, 10); i++) { const ctx = await window._yamil.getTabContext(window._yamil.tabs[i].id); results.push({ tab: i+1, title: window._yamil.tabs[i].title, url: window._yamil.tabs[i].url, text: ctx?.text?.slice(0,500) || '' }); } return JSON.stringify(results); })()`);
        return { content: [{ type: "text", text: String(res) }] };
      }
    } catch (e) {
      return { content: [{ type: "text", text: `Tab context error: ${e.message}` }], isError: true };
    }
  }
);

// ── yamil_browser_knowledge_search ────────────────────────────────────
server.tool(
  "yamil_browser_knowledge_search",
  "Search YAMIL Browser's learned knowledge base (RAG).",
  {
    query:    z.string().describe("Search query"),
    domain:   z.string().optional().describe("Filter by domain"),
    category: z.string().optional().describe("Filter by category"),
    topK:     z.number().optional().describe("Max results (default 5)"),
  },
  async ({ query, domain, category, topK }) => {
    try {
      const res = await fetch(`${BROWSER_SVC_URL}/knowledge/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, domain, category, topK: topK || 5 }),
        signal: AbortSignal.timeout(15000),
      });
      const data = await res.json();
      if (!data.entries?.length) {
        return { content: [{ type: "text", text: "No knowledge found. The browser hasn't learned about this topic yet." }] };
      }
      const formatted = data.entries.map((r, i) =>
        `${i + 1}. [${r.category}] ${r.title} (domain: ${r.domain}, score: ${(r.score || 0).toFixed(2)})\n   Source: "${r.source_goal}"\n   Content: ${JSON.stringify(r.content)}`
      ).join("\n\n");
      return { content: [{ type: "text", text: `Found ${data.entries.length} knowledge entries:\n\n${formatted}` }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Knowledge search failed: ${e.message}. Is the browser service running?` }], isError: true };
    }
  }
);

// ── yamil_browser_knowledge_stats ─────────────────────────────────────
server.tool("yamil_browser_knowledge_stats", "Show statistics about YAMIL Browser's knowledge base.", {},
  async () => {
    try {
      const res = await fetch(`${BROWSER_SVC_URL}/knowledge/stats`, { signal: AbortSignal.timeout(5000) });
      const stats = await res.json();
      if (!stats.total && !stats.actions) return { content: [{ type: "text", text: "Knowledge base is empty. Browse some pages — the browser learns passively from every action." }] };
      const domainList = Object.entries(stats.byDomain || {}).sort((a, b) => b[1] - a[1]).map(([d, c]) => `  ${d}: ${c}`).join("\n");
      const catList = Object.entries(stats.byCategory || {}).sort((a, b) => b[1] - a[1]).map(([c, n]) => `  ${c}: ${n}`).join("\n");
      return { content: [{ type: "text", text: `Knowledge base: ${stats.total} entries | ${stats.actions || 0} actions logged\n\nBy domain:\n${domainList}\n\nBy category:\n${catList}\n\nModels: Extract=${stats.extractAvailable ? stats.models.extract : "unavailable"} | Embed=${stats.embedAvailable ? stats.models.embed : "unavailable"}\nDB: ${stats.db ? "connected" : "disconnected"}` }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Knowledge stats failed: ${e.message}. Is the browser service running?` }], isError: true };
    }
  }
);

// ── Start server ──────────────────────────────────────────────────────
const transport = new StdioServerTransport();
await server.connect(transport);

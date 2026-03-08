import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname_mjs = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = join(__dirname_mjs, "..", "..");

export const YAMIL_ELECTRON_DIR = join(PROJECT_ROOT, "electron-app");
export const YAMIL_CTRL         = "http://127.0.0.1:9300";
export const BROWSER_SVC_URL    = process.env.YAMIL_BROWSER_URL || "http://127.0.0.1:4000";

export let yamilElectronProc = null;

/** Update the yamilElectronProc reference (used by start/stop tools) */
export function setYamilElectronProc(proc) {
  yamilElectronProc = proc;
}

// ── helpers ───────────────────────────────────────────────────────────

export async function yamilPing () {
  try {
    const res = await fetch(`${YAMIL_CTRL}/ping`, { signal: AbortSignal.timeout(1500) });
    if (res.ok) { const d = await res.json(); return d.ok === true; }
  } catch (_) {}
  return false;
}

export async function yamilGet (path) {
  const res = await fetch(`${YAMIL_CTRL}${path}`, { signal: AbortSignal.timeout(10000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res;
}

export async function yamilPost (path, body = {}) {
  const res = await fetch(`${YAMIL_CTRL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res;
}

export async function ye(script) {
  try {
    const res  = await yamilPost("/eval", { script });
    const data = await res.json();
    if (data.error) return undefined;
    return data.result;
  } catch (_) {
    // Retry with try/catch wrapper for pages that throw (ExtJS, QNAP, etc.)
    try {
      const safe = `(function(){try{return ${script}}catch(e){return undefined}})()`;
      const res  = await yamilPost("/eval", { script: safe });
      const data = await res.json();
      return data.error ? undefined : data.result;
    } catch (_) {
      return undefined;
    }
  }
}

export async function yamilScreenshotBuf() {
  const res = await yamilGet("/screenshot?quality=40&maxBytes=400000");
  return Buffer.from(await res.arrayBuffer());
}

export async function yamilPageUrl() {
  try { const r = await yamilGet("/url").then(r => r.json()); return r.url || "unknown"; } catch { return "unknown"; }
}

/** Log action to browser-service knowledge pipeline (non-blocking) */
export function logMcpAction(action, params = {}, pageUrl = '') {
  fetch(`${BROWSER_SVC_URL}/log-action`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sessionId: "mcp", action, params, pageUrl }),
    signal: AbortSignal.timeout(3000),
  }).catch(() => {});
}

/** Search knowledge base for relevant context (returns formatted string or null) */
export async function ragLookup(query, domain, category, topK = 3) {
  try {
    const res = await fetch(`${BROWSER_SVC_URL}/knowledge/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, domain, category, topK }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    if (!data.entries || data.entries.length === 0) return null;
    return data.entries
      .filter(e => (e.score || 0) > 0.3)
      .map(e => {
        const content = typeof e.content === 'string' ? JSON.parse(e.content) : e.content;
        return `[${e.category}] ${e.title}: ${JSON.stringify(content)}`;
      })
      .join('\n');
  } catch { return null; }
}

/** Get domain from URL */
export function extractDomain(url) {
  try { return new URL(url).hostname; } catch { return null; }
}

import { z } from "zod";
import { writeFileSync } from "fs";

export function registerDataTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, logToolError,
          BROWSER_SVC_URL, logMcpAction, YAMIL_CTRL } = deps;

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
        await fetch(`${BROWSER_SVC_URL}/sessions/${info.sessionId}/resize`, {
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

}

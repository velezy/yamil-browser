import { z } from "zod";
import { spawn } from "child_process";

export function registerBrowserMgmtTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, logToolError,
          YAMIL_ELECTRON_DIR, yamilElectronProc, setYamilElectronProc,
          BROWSER_SVC_URL, ragLookup, extractDomain,
          YAMIL_CTRL, OLLAMA_VISION_MODEL, getAnthropic,
          isGeminiAvailable, isOllamaAvailable, isBedrockAvailable } = deps;

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
        return { content: [{ type: "text", text: `YAMIL Browser is already running on ${YAMIL_CTRL}.` }] };
      }
      // Extract port from YAMIL_CTRL URL so Electron listens on the right port
      const ctrlPort = new URL(YAMIL_CTRL).port || "9300";
      const env = {
        ...process.env,
        AI_ENDPOINT:     aiEndpoint || "http://localhost:9080/api/v1/builder-orchestra/browser-chat",
        START_URL:       startUrl   || "https://yamil-ai.com",
        APP_TITLE:       "YAMIL Browser",
        CTRL_PORT:       ctrlPort,
        BROWSER_SERVICE: BROWSER_SVC_URL,
      };
      let cmd, args;
      if (process.platform === "win32") {
        cmd  = "cmd.exe";
        args = ["/c", "npx electron ."];
      } else {
        cmd  = "npx";
        args = ["electron", "."];
      }
      const proc = spawn(cmd, args, {
        cwd: YAMIL_ELECTRON_DIR,
        env,
        detached: true,
        stdio: "ignore",
      });
      proc.unref();
      setYamilElectronProc(proc);
      for (let i = 0; i < 8; i++) {
        await new Promise(r => setTimeout(r, 1000));
        if (await yamilPing()) {
          return { content: [{ type: "text", text: `YAMIL Browser started (PID ${proc.pid}). ${YAMIL_CTRL} ready.` }] };
        }
      }
      return { content: [{ type: "text", text: `YAMIL Browser process spawned but ${YAMIL_CTRL} not yet responding — it may still be loading.` }] };
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
      const proc = yamilElectronProc();
      if (proc) {
        try { process.kill(proc.pid); } catch (_) {}
        setYamilElectronProc(null);
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
        return { content: [{ type: "text", text: `YAMIL Browser: offline (${YAMIL_CTRL} not responding)` }] };
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
        const llmStatus = `LLM: Gemini=${isGeminiAvailable()} | Ollama Vision=${isOllamaAvailable()} (${OLLAMA_VISION_MODEL}) | Bedrock=${isBedrockAvailable()} | Anthropic=${!!process.env.ANTHROPIC_API_KEY}`;
        const lines = [
          "YAMIL Browser: running (unified stealth + logged-in)",
          `Active tab: ${tabInfo.type || "yamil"} | URL: ${urlData.url || "unknown"}`,
          `Tabs: ${(tabsData.tabs || []).length} (${(tabsData.tabs || []).filter(t => t.type === "stealth").length} stealth, ${(tabsData.tabs || []).filter(t => t.type !== "stealth").length} yamil)`,
          `Stealth: enabled (Playwright via ${BROWSER_SVC_URL})`,
          llmStatus,
        ];
        return { content: [{ type: "text", text: lines.join("\n") }] };
      } catch (_) {
        return { content: [{ type: "text", text: "YAMIL Browser: running (could not read status)" }] };
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
}

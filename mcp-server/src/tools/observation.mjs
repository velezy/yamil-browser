import { z } from "zod";

export function registerObservationTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, logToolError,
          ragLookup, extractDomain, yamilScreenshotBuf,
          getYamilA11yTree, yamilEnsureObserver, yamilWaitForDom,
          isOllamaAvailable, isGeminiAvailable, getAnthropic, anthropic } = deps;

  // ── yamil_browser_screenshot ──────────────────────────────────────────
  server.tool(
    "yamil_browser_screenshot",
    "Take a screenshot of what the YAMIL Browser desktop app is currently showing.",
    {},
    async () => {
      if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
      try {
        const isValidImage = (b) => b && b.length >= 200 &&
          ((b[0] === 0xFF && b[1] === 0xD8) || (b[0] === 0x89 && b[1] === 0x50));

        let buf;
        // Try webview screenshot first
        try {
          const res = await yamilGet("/screenshot?quality=35&maxBytes=350000");
          buf = Buffer.from(await res.arrayBuffer());
        } catch (_) { buf = null; }

        // Fall back to whole-window capture if webview failed or returned invalid data
        if (!isValidImage(buf)) {
          try {
            const wres = await yamilGet("/window-screenshot");
            buf = Buffer.from(await wres.arrayBuffer());
          } catch (e2) {
            return { content: [{ type: "text", text: `Screenshot failed (webview + window): ${e2.message}. Use yamil_browser_dom instead.` }], isError: true };
          }
        }

        if (!isValidImage(buf)) {
          return { content: [{ type: "text", text: "Screenshot returned invalid image data from both webview and window capture. Use yamil_browser_dom instead." }], isError: true };
        }
        if (buf.length > 400_000) {
          return { content: [{ type: "text", text: `Screenshot too large for API (${(buf.length/1024).toFixed(0)}KB). Use yamil_browser_a11y_snapshot or yamil_browser_dom instead. DO NOT attempt to take a raw screenshot via curl — always use MCP tools.` }], isError: true };
        }
        const mime = (buf[0] === 0x89 && buf[1] === 0x50) ? "image/png" : "image/jpeg";
        return { content: [{ type: "image", data: buf.toString("base64"), mimeType: mime }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Screenshot failed: ${e.message}` }], isError: true };
      }
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
      // RAG: append relevant knowledge about this page
      const domain = extractDomain(data.url);
      const knowledge = domain ? await ragLookup(data.url, domain, null, 2) : null;
      if (knowledge) return { content: [{ type: "text", text: `${summary}\n\n📚 Learned:\n${knowledge}` }] };
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
      const pageUrl = await yamilPageUrl();

      // Attempt 1: run script as-is
      try {
        const res  = await yamilPost("/eval", { script });
        const data = await res.json();
        if (!data.error) {
          return { content: [{ type: "text", text: JSON.stringify(data.result, null, 2) ?? "undefined" }] };
        }
      } catch (_) { /* fall through to recovery */ }

      // Attempt 2: wrap in try/catch — some pages (ExtJS/QNAP) throw on complex DOM ops
      try {
        const safeScript = `(function(){try{return ${script}}catch(e){return {__evalError:e.message}}})()`;
        const res  = await yamilPost("/eval", { script: safeScript });
        const data = await res.json();
        if (!data.error) {
          if (data.result?.__evalError) {
            logToolError("yamil_browser_eval", { script: script.slice(0, 200) }, `Page threw: ${data.result.__evalError}`, pageUrl);
            return { content: [{ type: "text", text: `Script threw on page: ${data.result.__evalError}\nDomain: ${extractDomain(pageUrl)}\nTip: This page may block complex DOM queries. Use simpler scripts or yamil_browser_a11y_snapshot instead.` }], isError: true };
          }
          return { content: [{ type: "text", text: JSON.stringify(data.result, null, 2) ?? "undefined" }] };
        }
      } catch (_) { /* fall through */ }

      // Attempt 3: minimal probe — if even wrapped fails, the page/webview is broken
      try {
        const res  = await yamilPost("/eval", { script: "document.title" });
        const data = await res.json();
        const title = data.result || "unknown";
        logToolError("yamil_browser_eval", { script: script.slice(0, 200) }, "Eval failed after all retries", pageUrl);
        return { content: [{ type: "text", text: `Eval failed on "${title}" (${extractDomain(pageUrl)}).\nThis page blocks JavaScript eval. Use yamil_browser_a11y_snapshot, yamil_browser_content, or yamil_browser_click instead.` }], isError: true };
      } catch (err) {
        logToolError("yamil_browser_eval", { script: script.slice(0, 200) }, `Complete eval failure: ${err.message}`, pageUrl);
        return { content: [{ type: "text", text: `Eval failed: ${err.message}. The webview may not be responding.` }], isError: true };
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
      // RAG: include learned page schema
      const pageUrl = await yamilPageUrl();
      const pageKnowledge = await ragLookup(pageUrl, extractDomain(pageUrl), "page_schemas", 2);
      if (!instruction) {
        const parts = [text];
        if (pageKnowledge) parts.push(`\n📚 Learned page knowledge:\n${pageKnowledge}`);
        return { content: [{ type: "text", text: parts.join("") }] };
      }
      if (!isOllamaAvailable() && !isGeminiAvailable() && !getAnthropic()) {
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

  // ── yamil_browser_screenshot_element (with empty/size/validity guards) ──
  server.tool("yamil_browser_screenshot_element", "Take a screenshot of a specific element.",
    {
      selector:      z.string().describe("CSS selector of the element to screenshot"),
      frameSelector: z.string().optional().describe("CSS selector of the iframe the element is inside"),
    },
    async ({ selector, frameSelector }) => {
      if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
      const isValidImage = (b) => b && b.length >= 200 &&
        ((b[0] === 0xFF && b[1] === 0xD8) || (b[0] === 0x89 && b[1] === 0x50));
      try {
        const res = await yamilPost("/screenshot-element", { selector });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          logToolError("yamil_browser_screenshot_element", { selector, frameSelector }, err.error || `HTTP ${res.status}`, await yamilPageUrl());
          return { content: [{ type: "text", text: `Element screenshot failed: ${err.error || res.status}` }], isError: true };
        }
        const buf = Buffer.from(await res.arrayBuffer());
        if (!isValidImage(buf)) {
          return { content: [{ type: "text", text: "Element screenshot returned invalid image data. Use yamil_browser_a11y_snapshot instead." }], isError: true };
        }
        if (buf.length > 400_000) {
          return { content: [{ type: "text", text: `Element screenshot too large for API (${(buf.length/1024).toFixed(0)}KB). Use yamil_browser_a11y_snapshot or yamil_browser_dom instead.` }], isError: true };
        }
        const mime = (buf[0] === 0x89 && buf[1] === 0x50) ? "image/png" : "image/jpeg";
        return { content: [{ type: "image", data: buf.toString("base64"), mimeType: mime }] };
      } catch (e) {
        logToolError("yamil_browser_screenshot_element", { selector, frameSelector }, e.message, await yamilPageUrl());
        return { content: [{ type: "text", text: `Screenshot error: ${e.message}` }], isError: true };
      }
    }
  );
}

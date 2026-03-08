import { z } from "zod";

export function registerAiVisionTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, yamilScreenshotBuf,
          logToolError, ragLookup, extractDomain, logMcpAction,
          anthropic, isOllamaAvailable, isGeminiAvailable, getAnthropic,
          geminiComputerUse, executeYamilCUAction,
          cacheGet, cacheSet, CACHEABLE_ACTIONS,
          getYamilA11yTree, monacoSetValue,
          extractJSON, BROWSER_SVC_URL } = deps;

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
      if (!isOllamaAvailable() && !isGeminiAvailable() && !getAnthropic()) {
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
      if (isGeminiAvailable()) {
        try {
          const a11y = await getYamilA11yTree();
          const ragCtx = await ragLookup(instruction + " " + pageUrl, extractDomain(pageUrl), null, 3);
          const cuInstruction = [instruction, a11y ? `\nAccessibility Tree:\n${a11y}` : "", ragCtx ? `\nLearned knowledge:\n${ragCtx}` : ""].join("");
          console.error(`[CU] yamil_browser_act: "${instruction}"${a11y ? " [A11Y]" : ""}${ragCtx ? " [RAG]" : ""}`);
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
      // RAG: search for relevant action recipes and page knowledge
      const domain = extractDomain(pageUrl);
      const ragContext = await ragLookup(instruction + " " + pageUrl, domain, null, 3);
      const prompt = `You are controlling a web browser. Analyze the screenshot and HTML to determine the best action.
Current page: ${pageUrl}
Page HTML (partial): ${html}
${a11y ? `\nAccessibility Tree:\n${a11y}\n` : ""}${ragContext ? `\nLearned knowledge from previous visits:\n${ragContext}\n` : ""}
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
      if (!isOllamaAvailable() && !isGeminiAvailable() && !getAnthropic()) {
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
}

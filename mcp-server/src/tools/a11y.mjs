import { z } from "zod";

export function registerA11yTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, logToolError,
          yamilEnsureObserver, yamilWaitForDom, BROWSER_SVC_URL, logMcpAction } = deps;

// ── yamil_browser_a11y_snapshot ────────────────────────────────────────
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
      // Use a Map for direct DOM element references — survives SPA re-renders
      if (!window.__yamil_ref_elements) window.__yamil_ref_elements = new Map();
      const refElements = window.__yamil_ref_elements;
      refElements.clear();
      const lines = [];
      let refId = 1;
      const INTERACTIVE = new Set(["A","BUTTON","INPUT","TEXTAREA","SELECT","DETAILS","SUMMARY"]);
      const SEMANTIC = new Set(["H1","H2","H3","H4","H5","H6","NAV","MAIN","ASIDE","HEADER","FOOTER","SECTION","ARTICLE","FORM","TABLE","THEAD","TBODY","TR","TH","TD","UL","OL","LI","LABEL","IMG","FIGURE","FIGCAPTION","DIALOG"]);
      const ROLES_INTERACTIVE = new Set(["button","link","textbox","combobox","listbox","option","menuitem","menuitemradio","menuitemcheckbox","checkbox","radio","switch","slider","spinbutton","searchbox","tab","tabpanel","dialog","alertdialog","tree","treeitem","grid","gridcell","row"]);
      const ROLES_SEMANTIC = new Set(["heading","navigation","main","complementary","banner","contentinfo","region","form","table","list","listitem","img","figure","alert","status","log","marquee","timer","toolbar","menu","menubar","tablist"]);
      const version = (window.__yamil_snapshot_version || 0) + 1;
      window.__yamil_snapshot_version = version;
      function walk(el, depth, frameIndex) {
        if (depth > 25 || lines.length > 600) return;
        const tag = el.tagName;
        if (!tag) return;
        if (tag === "SCRIPT" || tag === "STYLE" || tag === "NOSCRIPT" || tag === "SVG" || tag === "PATH") return;
        const style = getComputedStyle(el);
        if (style.display === "none" || style.visibility === "hidden") return;
        if (style.display !== "contents") {
          const rect = el.getBoundingClientRect();
          if (rect.width === 0 && rect.height === 0) return;
        }
        if (tag === "IFRAME") {
          try {
            const iframeDoc = el.contentDocument || el.contentWindow?.document;
            if (iframeDoc && iframeDoc.body) {
              const ref = "@e" + refId++;
              const indent = "  ".repeat(Math.min(depth, 8));
              const src = el.src ? el.src.split("?")[0].split("/").pop() : "";
              lines.push(indent + ref + ' iframe' + (src ? ' "' + src.slice(0, 40) + '"' : ''));
              refElements.set(ref, el);
              refs[ref] = { tag: "IFRAME", id: el.id || null, frame: true };
              for (const child of iframeDoc.body.children) walk(child, depth + 1, ref);
            }
          } catch(e) {}
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
            const dt = Array.from(el.childNodes).filter(n => n.nodeType === 3).map(n => n.textContent.trim()).join(" ").slice(0, 60);
            if (dt) parts.push('"' + dt.replace(/"/g, "'") + '"');
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
          // Store direct DOM reference in Map (survives SPA re-renders)
          refElements.set(ref, el);
          const labelText = ariaLabel || title || (el.innerText || "").trim().slice(0, 60);
          refs[ref] = { tag, label: labelText, role: role || null, frame: frameIndex || null };
        }
        for (const child of el.children) walk(child, depth + 1, frameIndex);
        if (el.shadowRoot) { for (const child of el.shadowRoot.children) walk(child, depth + 1, frameIndex); }
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
  async ({ ref: rawRef, version }) => {
    // Normalize ref: ensure it starts with @
    const ref = rawRef.startsWith("@") ? rawRef : "@" + rawRef;
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
        logMcpAction("click", { selector: ref, text: r.text }, await yamilPageUrl());
        return { content: [{ type: "text", text: `Clicked ${ref} → ${r.tag} "${r.text}"` }] };
      }
    } catch (_) { /* fall through to eval-based click */ }

    const r = await ye(`(function(){
      const refVersion = window.__yamil_refs_version;
      if (${version ?? -1} > 0 && refVersion !== ${version ?? -1}) return { stale: true, expected: ${version ?? -1}, actual: refVersion };
      // Primary: direct DOM reference from Map (like Chrome's node IDs)
      const refMap = window.__yamil_ref_elements;
      let el = refMap && refMap.get("${ref}");
      // Check element is still in the document (not detached by SPA)
      if (el && !el.isConnected) el = null;
      // Fallback: metadata-based search for re-rendered elements
      if (!el) {
        const meta = window.__yamil_refs && window.__yamil_refs["${ref}"];
        if (meta && meta.label) {
          const candidates = meta.role
            ? document.querySelectorAll("[role='" + meta.role + "'], " + (meta.tag || "div").toLowerCase())
            : document.querySelectorAll((meta.tag || "div").toLowerCase());
          const targetLabel = meta.label.toLowerCase();
          for (const c of candidates) {
            const cLabel = (c.getAttribute("aria-label") || c.getAttribute("title") || "").toLowerCase();
            const cText = (c.innerText || "").trim().toLowerCase().slice(0, 80);
            if (cLabel === targetLabel || cText === targetLabel || (targetLabel.length > 3 && cText.includes(targetLabel))) {
              const rect = c.getBoundingClientRect();
              if (rect.width > 0 && rect.height > 0) { el = c; break; }
            }
          }
        }
      }
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
  async ({ ref: rawRef, value, version }) => {
    // Normalize ref: ensure it starts with @
    const ref = rawRef.startsWith("@") ? rawRef : "@" + rawRef;
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
        logMcpAction("fill", { selector: ref, value: value.substring(0, 50) }, await yamilPageUrl());
        return { content: [{ type: "text", text: `Filled ${ref} → ${r.tag} with "${value}"` }] };
      }
    } catch (_) { /* fall through to eval-based fill */ }

    const r = await ye(`(function(){
      const refVersion = window.__yamil_refs_version;
      if (${version ?? -1} > 0 && refVersion !== ${version ?? -1}) return { stale: true, expected: ${version ?? -1}, actual: refVersion };
      // Primary: direct DOM reference from Map (like Chrome's node IDs)
      const refMap = window.__yamil_ref_elements;
      let el = refMap && refMap.get("${ref}");
      if (el && !el.isConnected) el = null;
      // Fallback: metadata-based search
      if (!el) {
        const meta = window.__yamil_refs && window.__yamil_refs["${ref}"];
        if (meta && meta.label) {
          const candidates = meta.role
            ? document.querySelectorAll("[role='" + meta.role + "'], " + (meta.tag || "div").toLowerCase())
            : document.querySelectorAll((meta.tag || "div").toLowerCase());
          const targetLabel = meta.label.toLowerCase();
          for (const c of candidates) {
            const cLabel = (c.getAttribute("aria-label") || c.getAttribute("title") || "").toLowerCase();
            const cText = (c.innerText || "").trim().toLowerCase().slice(0, 80);
            if (cLabel === targetLabel || cText === targetLabel || (targetLabel.length > 3 && cText.includes(targetLabel))) {
              const rect = c.getBoundingClientRect();
              if (rect.width > 0 && rect.height > 0) { el = c; break; }
            }
          }
        }
      }
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

}

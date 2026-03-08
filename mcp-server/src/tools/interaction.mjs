import { z } from "zod";

export function registerInteractionTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, logToolError,
          ragLookup, extractDomain, logMcpAction,
          yamilEnsureObserver, yamilWaitForDom, monacoSetValue,
          SELF_HEAL_SCRIPT, selectorCacheGet, selectorCacheSet } = deps;

// ── yamil_browser_click ─────────────────────────────────────────────
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
      if (r?.found) { logMcpAction("click", { selector: selector || text, near }, await yamilPageUrl()); return { content: [{ type: "text", text: `Clicked ${r.tag}${r.id ? "#" + r.id : ""} (${selector || text}${near ? ` near "${near}"` : ""})` }] }; }
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
    const pageUrl = await yamilPageUrl();
    const errMsg = `Element not found after 3 attempts: ${selector || text}${near ? ` near "${near}"` : ""}`;
    logToolError("yamil_browser_click", { selector, text, near }, errMsg, pageUrl);
    // RAG: check for error recovery knowledge
    const recovery = await ragLookup(`click failed ${selector || text}`, extractDomain(pageUrl), "error_recoveries", 2);
    const parts = [errMsg];
    if (recovery) parts.push(`\n📚 Known recovery steps:\n${recovery}`);
    return { content: [{ type: "text", text: parts.join("") }], isError: true };
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
    logMcpAction("fill", { selector, value: value.substring(0, 50) }, await yamilPageUrl());
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
    if (nr.native && !nr.error) { logMcpAction("select", { selector, value: nr.selected }, await yamilPageUrl()); return { content: [{ type: "text", text: `Selected "${nr.selected}" in ${selector}` }] }; }
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

} // end registerInteractionTools

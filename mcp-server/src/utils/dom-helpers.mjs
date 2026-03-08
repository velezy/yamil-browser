// ── Phase 1: MutationObserver backbone ─────────────────────────────────
export const YAMIL_OBSERVER_SCRIPT = `(function(){
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

// ── Phase 5: Self-Healing Selector helper ──────────────────────────────
export const SELF_HEAL_SCRIPT = (selector) => `(function(){
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

/**
 * Factory that creates DOM helper functions bound to a `ye` (evaluate) function.
 * @param {Function} ye - The browser evaluate function
 * @returns {{ getYamilA11yTree, monacoSetValue, yamilEnsureObserver, yamilWaitForDom }}
 */
export function createDomHelpers(ye) {
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

  async function yamilEnsureObserver() {
    try { return await ye(YAMIL_OBSERVER_SCRIPT); } catch { return null; }
  }

  async function yamilWaitForDom(timeoutMs = 3000) {
    await yamilEnsureObserver();
    try {
      return await ye(`window.__yamil_dom_settled(${timeoutMs})`);
    } catch { return false; }
  }

  return { getYamilA11yTree, monacoSetValue, yamilEnsureObserver, yamilWaitForDom };
}

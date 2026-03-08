// ── Action Cache ────────────────────────────────────────────────────
export const actionCache = new Map();
export const CACHE_TTL = 30 * 60 * 1000;
export const MAX_CACHE = 500;
export const CACHEABLE_ACTIONS = new Set(["click_at", "click", "navigate", "press", "key_combination", "scroll", "scroll_document", "scroll_at", "hover_at", "hover", "select", "go_back", "go_forward"]);

export function cacheKey(pageUrl, instruction) {
  try { return new URL(pageUrl).hostname + "|" + instruction.trim().toLowerCase(); }
  catch { return pageUrl + "|" + instruction.trim().toLowerCase(); }
}

export function cacheGet(pageUrl, instruction) {
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

export function cacheSet(pageUrl, instruction, action, args) {
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
export const selectorCache = new Map();
export const SELECTOR_CACHE_TTL = 60 * 60 * 1000;
export const MAX_SELECTOR_CACHE = 200;

export function selectorCacheKey(pageUrl, originalSelector) {
  try { return new URL(pageUrl).hostname + "|" + originalSelector; }
  catch { return pageUrl + "|" + originalSelector; }
}

export function selectorCacheGet(pageUrl, originalSelector) {
  const key = selectorCacheKey(pageUrl, originalSelector);
  const entry = selectorCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > SELECTOR_CACHE_TTL) {
    selectorCache.delete(key);
    return null;
  }
  return entry;
}

export function selectorCacheSet(pageUrl, originalSelector, healedDesc) {
  const key = selectorCacheKey(pageUrl, originalSelector);
  if (selectorCache.size >= MAX_SELECTOR_CACHE) {
    const oldest = selectorCache.keys().next().value;
    selectorCache.delete(oldest);
  }
  selectorCache.set(key, { healedDesc, timestamp: Date.now() });
}

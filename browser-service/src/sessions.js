import { chromium } from 'playwright'
import { randomUUID } from 'crypto'
import { mkdirSync } from 'fs'
import { join } from 'path'
import { STEALTH_SCRIPT, LAUNCH_ARGS, USER_AGENT } from './stealth.js'

const logger = { info: (...a) => console.log('[sessions]', ...a) }

const HUD_SCRIPT = `(function(){
  if(document.getElementById('__yamil_hud__'))return;
  const s=document.createElement('style');
  s.textContent='@keyframes yp{0%,100%{opacity:1}50%{opacity:.25}}';
  document.head.appendChild(s);
  const h=document.createElement('div');
  h.id='__yamil_hud__';
  h.style.cssText='position:fixed;bottom:20px;right:20px;z-index:2147483647;background:rgba(10,12,20,.82);color:#e2e8f0;font:600 11px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",monospace;padding:6px 10px 6px 8px;border-radius:7px;border:1px solid rgba(74,222,128,.35);box-shadow:0 2px 12px rgba(0,0,0,.5);display:flex;align-items:center;gap:7px;pointer-events:none;user-select:none;backdrop-filter:blur(6px)';
  const d=document.createElement('span');
  d.style.cssText='width:7px;height:7px;border-radius:50%;background:#4ade80;display:inline-block;flex-shrink:0;animation:yp 2s ease-in-out infinite';
  const l=document.createElement('span');
  l.textContent='⚡ YAMIL Stealth Browser';
  h.appendChild(d);h.appendChild(l);
  document.body.appendChild(h);
})()`

const SESSION_IDLE_MS     = parseInt(process.env.SESSION_IDLE_MS     || '300000')  // 5 min idle
const SESSION_MAX_AGE_MS  = parseInt(process.env.SESSION_MAX_AGE_MS  || '1800000') // 30 min absolute
// Legacy alias
const SESSION_TIMEOUT_MS  = parseInt(process.env.SESSION_TIMEOUT_MS  || String(SESSION_IDLE_MS))

// Electron-managed sessions get longer timeouts (1hr idle) and skip HUD overlay
const ELECTRON_IDLE_MS    = 3600000  // 1 hour
const ELECTRON_MAX_AGE_MS = 86400000 // 24 hours

// Root directory for persistent browser profiles.
// Each named profile gets its own subdirectory so cookies/localStorage/logins
// survive session expiry and service restarts — just like a real browser profile.
const PROFILES_DIR = process.env.PROFILES_DIR || join(process.cwd(), 'data', 'profiles')

// event → method name for CDP domains we want to enable
const CDP_DOMAINS = ['Network', 'Runtime', 'Page', 'DOM']

// Raw CDP events forwarded to event stream subscribers
const CDP_EVENTS = [
  'Network.requestWillBeSent',
  'Network.responseReceived',
  'Network.loadingFailed',
  'Runtime.consoleAPICalled',
  'Runtime.exceptionThrown',
  'Page.frameNavigated',
  'Page.domContentEventFired',
  'Page.loadEventFired',
  'DOM.documentUpdated',
]

/** @type {Map<string, Session>} */
const sessions = new Map()

/**
 * Create a new browser session.
 *
 * When opts.profile is set (or defaults to "default"), Playwright uses a
 * persistent user-data directory so cookies, localStorage, IndexedDB, and
 * browser login state are preserved across session expiry and service restarts.
 *
 * Pass opts.profile = null to get a fully ephemeral (incognito) session.
 *
 * Each named profile is shared across sessions that request it — this mirrors
 * how a real user's Chrome profile works: one set of stored credentials used
 * across many windows/tabs.
 */
export async function createSession(opts = {}) {
  const id = randomUUID()
  const electronManaged = !!opts.electronManaged

  // Resolve profile: "default" unless caller passes a different name or null
  const profileName = opts.profile === null ? null : (opts.profile || 'default')

  let context

  // Proxy support: pass { server, username, password } in opts.proxy
  const proxyConfig = opts.proxy || null

  if (profileName) {
    // ── Persistent profile ─────────────────────────────────────────────
    const userDataDir = join(PROFILES_DIR, profileName)
    mkdirSync(userDataDir, { recursive: true })
    logger.info(`Session ${id} using persistent profile "${profileName}" at ${userDataDir}${proxyConfig ? ' (proxy: ' + proxyConfig.server + ')' : ''}`)

    const launchOpts = {
      headless: opts.headless !== false,
      args: LAUNCH_ARGS,
      userAgent: USER_AGENT,
      viewport: { width: 1920, height: 1080 },
    }
    if (proxyConfig) launchOpts.proxy = proxyConfig

    context = await chromium.launchPersistentContext(userDataDir, launchOpts)
  } else {
    // ── Ephemeral (incognito) session ──────────────────────────────────
    logger.info(`Session ${id} using ephemeral (incognito) context${proxyConfig ? ' (proxy: ' + proxyConfig.server + ')' : ''}`)
    const browser = await chromium.launch({
      headless: opts.headless !== false,
      args: LAUNCH_ARGS,
    })
    const contextOpts = {
      userAgent: USER_AGENT,
      viewport: { width: 1920, height: 1080 },
    }
    if (proxyConfig) contextOpts.proxy = proxyConfig

    context = await browser.newContext(contextOpts)
    // Attach browser ref so closeSession can call browser.close()
    context._browser = browser
  }

  // Inject stealth on every page/navigation; skip HUD for electron-managed sessions
  await context.addInitScript(STEALTH_SCRIPT)
  if (!electronManaged) {
    await context.addInitScript(HUD_SCRIPT)
  }

  const page = await context.newPage()

  // Raw CDP session for event subscription + screencast
  const cdp = await context.newCDPSession(page)

  // Enable CDP domains
  for (const domain of CDP_DOMAINS) {
    await cdp.send(`${domain}.enable`).catch(() => {})
  }

  /** @type {Session} */
  const session = {
    id,
    profile: profileName,
    electronManaged,
    context,
    page,
    cdp,
    createdAt: Date.now(),
    lastUsedAt: Date.now(),
    eventSubs: new Set(),       // WS clients receiving CDP events
    screencastSubs: new Set(),  // WS clients receiving screencast frames
    screencastActive: false,
  }

  sessions.set(id, session)

  // Forward CDP events to all event stream subscribers
  for (const event of CDP_EVENTS) {
    cdp.on(event, (params) =>
      broadcast(session.eventSubs, { event, params, ts: Date.now() })
    )
  }

  // Expiry timer — closes session on idle or absolute max age.
  // For persistent profiles this only closes the Playwright context (the
  // browser process), not the on-disk profile data — that persists.
  const idleLimit = electronManaged ? ELECTRON_IDLE_MS : SESSION_IDLE_MS
  const maxAge    = electronManaged ? ELECTRON_MAX_AGE_MS : SESSION_MAX_AGE_MS
  session._timer = setInterval(() => {
    const now = Date.now()
    const idle    = now - session.lastUsedAt > idleLimit
    const tooOld  = now - session.createdAt  > maxAge
    if (idle || tooOld) {
      logger.info(`[sessions] expiring ${id} (profile="${profileName}") — idle=${idle} tooOld=${tooOld}`)
      closeSession(id).catch(() => {})
    }
  }, 60_000)

  return session
}

export function getSession(id) {
  return sessions.get(id)
}

export function listSessions() {
  return [...sessions.values()].map((s) => ({
    id: s.id,
    profile: s.profile || null,
    electronManaged: s.electronManaged || false,
    url: s.page.url(),
    createdAt: s.createdAt,
    lastUsedAt: s.lastUsedAt,
    eventSubscribers: s.eventSubs.size,
    screencastSubscribers: s.screencastSubs.size,
  }))
}

export async function closeSession(id) {
  const s = sessions.get(id)
  if (!s) return
  sessions.delete(id)
  clearInterval(s._timer)
  for (const ws of [...s.eventSubs, ...s.screencastSubs]) {
    try { ws.close() } catch (_) {}
  }
  // For persistent contexts, close() flushes state to disk then shuts the
  // browser process — the profile directory is preserved for next time.
  // For ephemeral contexts, also close the underlying browser.
  try { await s.context.close() } catch (_) {}
  if (s.context._browser) {
    try { await s.context._browser.close() } catch (_) {}
  }
}

export function touch(session) {
  session.lastUsedAt = Date.now()
}

function broadcast(subs, data) {
  const msg = JSON.stringify(data)
  for (const ws of subs) {
    try {
      if (ws.readyState === 1) ws.send(msg)
    } catch (_) {
      subs.delete(ws)
    }
  }
}

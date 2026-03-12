/* ── YAMIL Browser — Renderer with Tabs ────────────────────────────── */

const addrBar    = document.getElementById('address-bar')
const statusUrl  = document.getElementById('status-url')
const statusLoad = document.getElementById('status-loading')
const connDot    = document.getElementById('conn-dot')
const chatLog    = document.getElementById('chat-log')
const chatInput  = document.getElementById('chat-input')
const tabsList   = document.getElementById('tabs-list')
const container  = document.getElementById('webview-container')

const aiEndpoint = window.YAMIL_CONFIG?.AI_ENDPOINT || null
const startUrl   = window.YAMIL_CONFIG?.START_URL   || 'https://yamil-ai.com'

if (window.YAMIL_CONFIG?.APP_TITLE) document.title = window.YAMIL_CONFIG.APP_TITLE

// ── Persistence keys ──────────────────────────────────────────────────
// Global keys (shared across profiles)
const KEY_SIDEBAR_OPEN = 'yamil_sidebar_open'
const KEY_BMBAR_VIS    = 'yamil_bookmarkbar_visible'
const KEY_PROFILE      = 'yamil_current_profile'
// Profile-scoped keys — use pkey() wrapper when reading/writing these
const KEY_LAST_URL     = 'yamil_last_url'
const KEY_CHAT_HISTORY = 'yamil_chat_history'
const KEY_TABS         = 'yamil_tabs'
const KEY_BOOKMARKS    = 'yamil_bookmarks'
const KEY_HISTORY      = 'yamil_history'
const KEY_AI_MEMORY    = 'yamil_ai_memory'
const KEY_AI_SKILLS    = 'yamil_ai_skills'
const KEY_AI_BLOCKED   = 'yamil_ai_blocked_domains'
const MAX_STORED_MSGS  = 200
const MAX_HISTORY      = 5000

// ── Profile management ───────────────────────────────────────────────
let currentProfile = localStorage.getItem(KEY_PROFILE) || 'Default'

function getProfiles () {
  try { return JSON.parse(localStorage.getItem('yamil_profiles') || '["Default"]') } catch { return ['Default'] }
}

function saveProfiles (profiles) {
  localStorage.setItem('yamil_profiles', JSON.stringify(profiles))
}

function getPartition () {
  return currentProfile === 'Default' ? 'persist:yamil' : `persist:profile-${currentProfile.toLowerCase().replace(/\s+/g, '-')}`
}

/** Profile-scoped localStorage key. Default profile uses bare keys for backward compatibility. */
function pkey (key) {
  return currentProfile === 'Default' ? key : `${currentProfile}::${key}`
}

// Profile-scoped storage helpers — use these instead of raw localStorage for profile data
const PROFILE_KEYS = new Set([KEY_LAST_URL, KEY_CHAT_HISTORY, KEY_TABS, KEY_BOOKMARKS, KEY_HISTORY, KEY_AI_MEMORY, KEY_AI_SKILLS, KEY_AI_BLOCKED, 'yamil_downloads', 'yamil_settings'])

function pGet (key) { return localStorage.getItem(pkey(key)) }
function pSet (key, val) { localStorage.setItem(pkey(key), val) }
function pRemove (key) { localStorage.removeItem(pkey(key)) }

// ── Domain color helper ──────────────────────────────────────────────

function domainColor (domain) {
  if (!domain) return '#475569'
  let hash = 0
  for (let i = 0; i < domain.length; i++) hash = domain.charCodeAt(i) + ((hash << 5) - hash)
  const hue = Math.abs(hash) % 360
  return `hsl(${hue}, 55%, 45%)`
}

// ── Bookmark data model ──────────────────────────────────────────────

function getBookmarks () {
  try { return JSON.parse(pGet(KEY_BOOKMARKS) || '[]') } catch (_) { return [] }
}

function saveBookmarks (arr) {
  try { pSet(KEY_BOOKMARKS, JSON.stringify(arr)) } catch (_) {}
}

function addBookmark ({ url, title, favicon, tags, category }) {
  const bm = getBookmarks()
  const existing = bm.find(b => b.url === url)
  if (existing) return existing
  const bookmark = {
    id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
    url, title: title || url, favicon: favicon || '',
    tags: tags || [], category: category || '',
    createdAt: Date.now(), visits: 0,
  }
  bm.push(bookmark)
  saveBookmarks(bm)
  return bookmark
}

function removeBookmark (id) {
  const bm = getBookmarks().filter(b => b.id !== id)
  saveBookmarks(bm)
}

function removeBookmarkByUrl (url) {
  const bm = getBookmarks().filter(b => b.url !== url)
  saveBookmarks(bm)
}

function isBookmarked (url) {
  return getBookmarks().find(b => b.url === url) || null
}

function searchBookmarks (query) {
  const q = query.toLowerCase()
  return getBookmarks().filter(b =>
    b.title.toLowerCase().includes(q) ||
    b.url.toLowerCase().includes(q) ||
    b.tags.some(t => t.toLowerCase().includes(q)) ||
    b.category.toLowerCase().includes(q)
  )
}

// ── Browser-service config ────────────────────────────────────────────
const BROWSER_SERVICE = 'http://127.0.0.1:4000'

// ── Tab state ─────────────────────────────────────────────────────────
// Tab types: 'yamil' (webview) | 'stealth' (canvas + browser-service)
let tabs = []          // { id, type, title, url, webview?, tabEl, canvasEl?, sessionId?, ws?, titleSpan }
let activeTabId = null
let tabIdCounter = 0

// ── Tab management ────────────────────────────────────────────────────

function createTab (url, activate = true, type = 'yamil') {
  const id = ++tabIdCounter
  url = url || (type === 'stealth' ? 'about:blank' : startUrl)

  const tab = { id, type, title: 'New Tab', url, webview: null, canvasEl: null, sessionId: null, ws: null, tabEl: null, titleSpan: null, faviconEl: null, zoom: 0 }

  if (type === 'yamil') {
    // Create webview element
    const wv = document.createElement('webview')
    wv.setAttribute('allowpopups', '')
    wv.setAttribute('partition', getPartition())
    wv.setAttribute('webpreferences', 'contextIsolation=yes, sandbox=no')
    wv.setAttribute('disablewebsecurity', '')
    wv.src = url
    container.appendChild(wv)
    tab.webview = wv
  } else if (type === 'stealth') {
    // Create canvas element for stealth rendering
    const canvas = document.createElement('canvas')
    canvas.className = 'stealth-canvas'
    canvas.width = 1280
    canvas.height = 800
    container.appendChild(canvas)
    tab.canvasEl = canvas

    // Will be initialized async after tab is added to array
    initStealthTab(tab, url)
  }

  // Create tab bar element
  const tabEl = document.createElement('div')
  tabEl.className = 'tab' + (type === 'stealth' ? ' stealth' : '')
  tabEl.dataset.tabId = id

  // Stealth indicator icon
  if (type === 'stealth') {
    const icon = document.createElement('span')
    icon.className = 'stealth-icon'
    icon.textContent = '\uD83D\uDEE1\uFE0F'
    icon.title = 'Stealth tab'
    tabEl.appendChild(icon)
  }

  // Favicon
  const faviconEl = document.createElement('span')
  faviconEl.className = 'tab-favicon-letter'
  const domain = (() => { try { return new URL(url).hostname.replace('www.','') } catch(_) { return '' } })()
  faviconEl.textContent = domain ? domain[0].toUpperCase() : ''
  faviconEl.style.background = domainColor(domain)
  tabEl.appendChild(faviconEl)
  tab.faviconEl = faviconEl

  const titleSpan = document.createElement('span')
  titleSpan.className = 'tab-title'
  titleSpan.textContent = type === 'stealth' ? 'Stealth Tab' : 'New Tab'

  const closeBtn = document.createElement('button')
  closeBtn.className = 'tab-close'
  closeBtn.textContent = '\u00d7'
  closeBtn.title = 'Close tab'

  tabEl.appendChild(titleSpan)
  tabEl.appendChild(closeBtn)
  tabsList.appendChild(tabEl)

  tab.tabEl = tabEl
  tab.titleSpan = titleSpan
  tabs.push(tab)

  // Drag to reorder
  tabEl.setAttribute('draggable', 'true')
  tabEl.addEventListener('dragstart', (e) => {
    e.dataTransfer.setData('text/plain', String(id))
    tabEl.classList.add('dragging')
  })
  tabEl.addEventListener('dragend', () => { tabEl.classList.remove('dragging') })
  tabEl.addEventListener('dragover', (e) => {
    e.preventDefault()
    const dragging = tabsList.querySelector('.tab.dragging')
    if (!dragging || dragging === tabEl) return
    const rect = tabEl.getBoundingClientRect()
    const mid = rect.left + rect.width / 2
    if (e.clientX < mid) tabsList.insertBefore(dragging, tabEl)
    else tabsList.insertBefore(dragging, tabEl.nextSibling)
  })
  tabEl.addEventListener('drop', (e) => {
    e.preventDefault()
    // Reorder tabs array to match DOM order
    const order = Array.from(tabsList.querySelectorAll('.tab')).map(el => Number(el.dataset.tabId))
    tabs.sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id))
    saveTabs()
  })

  // Click tab to switch
  tabEl.addEventListener('click', (e) => {
    if (e.target === closeBtn) return
    switchTab(id)
  })

  // Close tab
  closeBtn.addEventListener('click', (e) => {
    e.stopPropagation()
    closeTab(id)
  })

  // Wire webview events for yamil tabs
  if (type === 'yamil') wireWebviewEvents(tab)

  if (activate) switchTab(id)
  saveTabs()
  return tab
}

// ── Stealth tab initialization (async) ───────────────────────────────

async function initStealthTab (tab, url) {
  try {
    // Create a browser-service session
    const res = await fetch(`${BROWSER_SERVICE}/sessions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ electronManaged: true }),
    })
    const { id: sessionId } = await res.json()
    tab.sessionId = sessionId

    // Navigate to url if not about:blank
    if (url && url !== 'about:blank') {
      await fetch(`${BROWSER_SERVICE}/sessions/${sessionId}/navigate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url }),
      })
    }

    // Connect WebSocket for screencast
    connectScreencast(tab)

    // Wire canvas input forwarding
    wireCanvasInput(tab)

    // Poll for title/url updates
    startStealthPoller(tab)
  } catch (e) {
    console.error('[stealth] init error:', e)
    tab.titleSpan.textContent = 'Stealth (error)'
  }
}

function connectScreencast (tab) {
  if (!tab.sessionId) return
  const ws = new WebSocket(`ws://127.0.0.1:4000/sessions/${tab.sessionId}/screencast`)
  tab.ws = ws

  const canvas = tab.canvasEl
  const ctx = canvas.getContext('2d')

  ws.onmessage = (ev) => {
    try {
      const data = JSON.parse(ev.data)
      if (data.frame) {
        const img = new Image()
        img.onload = () => {
          canvas.width = img.width
          canvas.height = img.height
          ctx.drawImage(img, 0, 0)
        }
        img.src = 'data:image/jpeg;base64,' + data.frame
      }
    } catch (_) {}
  }

  ws.onclose = () => {
    // Reconnect if tab still exists
    if (tabs.find(t => t.id === tab.id) && tab.sessionId) {
      setTimeout(() => connectScreencast(tab), 1000)
    }
  }

  ws.onerror = () => ws.close()
}

function wireCanvasInput (tab) {
  const canvas = tab.canvasEl
  if (!canvas) return

  // Mouse click
  canvas.addEventListener('click', (e) => {
    if (!tab.sessionId) return
    const rect = canvas.getBoundingClientRect()
    const scaleX = canvas.width / rect.width
    const scaleY = canvas.height / rect.height
    const x = Math.round((e.clientX - rect.left) * scaleX)
    const y = Math.round((e.clientY - rect.top) * scaleY)
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/mouse/click`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ x, y }),
    }).catch(() => {})
  })

  // Double click
  canvas.addEventListener('dblclick', (e) => {
    if (!tab.sessionId) return
    const rect = canvas.getBoundingClientRect()
    const scaleX = canvas.width / rect.width
    const scaleY = canvas.height / rect.height
    const x = Math.round((e.clientX - rect.left) * scaleX)
    const y = Math.round((e.clientY - rect.top) * scaleY)
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/mouse/click`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ x, y }),
    }).then(() =>
      fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/mouse/click`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ x, y }),
      })
    ).catch(() => {})
  })

  // Mouse move (throttled)
  let moveThrottled = false
  canvas.addEventListener('mousemove', (e) => {
    if (!tab.sessionId || moveThrottled) return
    moveThrottled = true
    setTimeout(() => { moveThrottled = false }, 50)
    const rect = canvas.getBoundingClientRect()
    const scaleX = canvas.width / rect.width
    const scaleY = canvas.height / rect.height
    const x = Math.round((e.clientX - rect.left) * scaleX)
    const y = Math.round((e.clientY - rect.top) * scaleY)
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/mouse/move`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ x, y }),
    }).catch(() => {})
  })

  // Scroll
  canvas.addEventListener('wheel', (e) => {
    if (!tab.sessionId) return
    e.preventDefault()
    const direction = e.deltaY > 0 ? 'down' : 'up'
    const amount = Math.min(Math.abs(e.deltaY), 1000)
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/scroll`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ direction, amount }),
    }).catch(() => {})
  }, { passive: false })

  // Keyboard input — capture when canvas is focused
  canvas.tabIndex = 0
  canvas.addEventListener('keydown', (e) => {
    if (!tab.sessionId) return
    e.preventDefault()
    // Map to Playwright key names
    const key = mapKey(e)
    if (key) {
      fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/press`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key }),
      }).catch(() => {})
    } else if (e.key.length === 1) {
      fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/keyboard/type`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: e.key }),
      }).catch(() => {})
    }
  })
}

function mapKey (e) {
  // Special keys that Playwright recognizes
  const map = {
    Enter: 'Enter', Backspace: 'Backspace', Tab: 'Tab', Escape: 'Escape',
    ArrowUp: 'ArrowUp', ArrowDown: 'ArrowDown', ArrowLeft: 'ArrowLeft', ArrowRight: 'ArrowRight',
    Delete: 'Delete', Home: 'Home', End: 'End', PageUp: 'PageUp', PageDown: 'PageDown',
    ' ': 'Space', F1: 'F1', F2: 'F2', F3: 'F3', F4: 'F4', F5: 'F5',
    F6: 'F6', F7: 'F7', F8: 'F8', F9: 'F9', F10: 'F10', F11: 'F11', F12: 'F12',
  }
  if (map[e.key]) {
    const parts = []
    if (e.ctrlKey) parts.push('Control')
    if (e.shiftKey) parts.push('Shift')
    if (e.altKey) parts.push('Alt')
    parts.push(map[e.key])
    return parts.join('+')
  }
  // Ctrl+key combos
  if (e.ctrlKey && e.key.length === 1) {
    return `Control+${e.key}`
  }
  return null
}

function startStealthPoller (tab) {
  const poll = async () => {
    if (!tabs.find(t => t.id === tab.id) || !tab.sessionId) return
    try {
      const res = await fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/url`)
      const { url, title } = await res.json()
      if (url) tab.url = url
      if (title) {
        tab.title = title
        tab.titleSpan.textContent = title
      }
      if (tab.id === activeTabId) {
        updateBar(tab.url)
        statusUrl.textContent = tab.title || ''
      }
    } catch (_) {}
    setTimeout(poll, 2000)
  }
  setTimeout(poll, 1000)
}

function switchTab (id) {
  const tab = tabs.find(t => t.id === id)
  if (!tab) return

  activeTabId = id

  // Update visibility for all tabs
  tabs.forEach(t => {
    // Yamil tabs
    if (t.webview) t.webview.classList.toggle('active', t.id === id)
    // Stealth tabs
    if (t.canvasEl) t.canvasEl.classList.toggle('active', t.id === id)
    // Tab bar
    t.tabEl.classList.toggle('active', t.id === id)
  })

  // Focus canvas for keyboard input on stealth tabs
  if (tab.type === 'stealth' && tab.canvasEl) {
    tab.canvasEl.focus()
  }

  // Update address bar and status
  updateBar(tab.url)
  statusUrl.textContent = tab.title || ''
  updateBookmarkStar()
  updateAiEyeIcon()
  saveTabs()
}

// ── Recently closed tabs (for Ctrl+Shift+T restore) ──────────────────
const recentlyClosed = [] // { url, title, type } — max 20
const MAX_CLOSED = 20

function closeTab (id) {
  const idx = tabs.findIndex(t => t.id === id)
  if (idx === -1) return

  // Don't close the last tab — create a new one first
  if (tabs.length === 1) {
    createTab(startUrl, true)
  }

  const tab = tabs[idx]

  // Remember for restore (non-blank tabs only)
  if (tab.url && tab.url !== 'about:blank' && !tab.url.startsWith('https://localhost:8444')) {
    recentlyClosed.push({ url: tab.url, title: tab.title, type: tab.type || 'yamil' })
    if (recentlyClosed.length > MAX_CLOSED) recentlyClosed.shift()
  }

  // Cleanup based on tab type
  if (tab.type === 'stealth') {
    // Close WebSocket
    if (tab.ws) { try { tab.ws.close() } catch (_) {} }
    // Close browser-service session
    if (tab.sessionId) {
      fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}`, { method: 'DELETE' }).catch(() => {})
    }
    // Remove canvas
    if (tab.canvasEl) tab.canvasEl.remove()
  } else {
    // Remove webview
    if (tab.webview) tab.webview.remove()
  }

  tab.tabEl.remove()
  tabs.splice(idx, 1)

  // If we closed the active tab, switch to nearest
  if (activeTabId === id) {
    const newIdx = Math.min(idx, tabs.length - 1)
    switchTab(tabs[newIdx].id)
  }
  saveTabs()
}

function restoreClosedTab () {
  const entry = recentlyClosed.pop()
  if (!entry) return
  createTab(entry.url, true, entry.type || 'yamil')
}

function getActiveWebview () {
  const tab = tabs.find(t => t.id === activeTabId)
  return (tab && tab.type === 'yamil') ? tab.webview : null
}

function getActiveTabType () {
  const tab = tabs.find(t => t.id === activeTabId)
  return tab ? tab.type : null
}

function getActiveSessionId () {
  const tab = tabs.find(t => t.id === activeTabId)
  return (tab && tab.type === 'stealth') ? tab.sessionId : null
}

function getActiveTabInfo () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab) return null
  return { type: tab.type, sessionId: tab.sessionId || null, id: tab.id, url: tab.url, title: tab.title }
}

// Expose for main process queries
window._yamil = {
  tabs, getActiveWebview, getActiveTabType, getActiveSessionId, getActiveTabInfo, createTab, switchTab, closeTab,
  bookmarks: { getAll: getBookmarks, add: addBookmark, remove: removeBookmark, removeByUrl: removeBookmarkByUrl, search: searchBookmarks, isBookmarked },
  history: { getAll: getHistory, search: searchHistory, clear: clearHistory },
  zoom: { zoomIn, zoomOut, zoomReset, getZoom: function () { const t = tabs.find(t => t.id === activeTabId); return t ? t.zoom : 0 } },
  toggleFullscreen,
  skills: { getAll: getAllSkills, getCustom: getCustomSkills, run: runSkill },
  aiPrivacy: { isBlocked: isAiBlockedForCurrentPage, toggle: toggleAiVisibility, getBlockedDomains },
  getTabContext,
}

// ── Wire webview events to a tab ──────────────────────────────────────

function wireWebviewEvents (tab) {
  const wv = tab.webview

  wv.addEventListener('did-start-loading', () => {
    if (tab.id === activeTabId) {
      statusLoad.textContent = 'Loading...'
      if (connDot) connDot.className = 'dot connecting'
      showProgressBar()
    }
  })

  wv.addEventListener('did-stop-loading', () => {
    if (tab.id === activeTabId) {
      statusLoad.textContent = ''
      if (connDot) connDot.className = 'dot connected'
      hideProgressBar()
    }
  })

  wv.addEventListener('did-navigate', (e) => {
    tab.url = e.url
    if (tab.id === activeTabId) { updateBar(e.url); updateBookmarkStar(); updateAiEyeIcon() }
    saveTabs()
    recordHistory(e.url, tab.title)
  })

  wv.addEventListener('did-navigate-in-page', (e) => {
    tab.url = e.url
    if (tab.id === activeTabId) { updateBar(e.url); updateBookmarkStar() }
    saveTabs()
  })

  wv.addEventListener('page-title-updated', (e) => {
    tab.title = e.title
    tab.titleSpan.textContent = e.title
    if (tab.id === activeTabId) statusUrl.textContent = e.title
  })

  wv.addEventListener('did-fail-load', (e) => {
    if (e.errorCode !== -3 && tab.id === activeTabId) {
      statusLoad.textContent = `Error: ${e.errorDescription}`
      if (connDot) connDot.className = 'dot disconnected'
    }
  })

  // ── Auto-save credentials: detect login form submissions ─────────────
  wv.addEventListener('did-stop-loading', () => {
    wv.executeJavaScript(`(function() {
      if (window.__yamil_cred_observer) return; // already injected
      window.__yamil_cred_observer = true;

      // Find password fields and watch for form submissions
      function watchForms() {
        const pwFields = document.querySelectorAll('input[type="password"]');
        if (!pwFields.length) return;

        pwFields.forEach(pw => {
          if (pw.__yamil_watched) return;
          pw.__yamil_watched = true;

          // Find the form or nearest container
          const form = pw.closest('form') || pw.parentElement?.closest('div');
          if (!form) return;

          // Capture credentials on submit
          function captureAndSave(e) {
            const password = pw.value;
            if (!password) return;

            // Find username: look for email/text input near the password field
            let username = '';
            const container = pw.closest('form') || document.body;
            const inputs = container.querySelectorAll('input[type="email"], input[type="text"], input[name*="user"], input[name*="email"], input[name*="login"], input[name*="account"], input[autocomplete="username"]');
            for (const inp of inputs) {
              if (inp.value && inp.value.trim()) { username = inp.value.trim(); break; }
            }
            // Fallback: any text/email input on the page with a value
            if (!username) {
              for (const inp of document.querySelectorAll('input[type="email"], input[type="text"]')) {
                if (inp.value && inp.value.trim() && inp !== pw && !inp.type.match(/hidden|search/)) {
                  username = inp.value.trim(); break;
                }
              }
            }
            if (!username || !password) return;

            const domain = location.hostname.replace(/^www\\./, '');
            // Build login form recipe — selectors the AI can use to replay login
            function bestSelector(el) {
              if (el.id) return '#' + el.id;
              if (el.name) return el.tagName.toLowerCase() + '[name="' + el.name + '"]';
              if (el.type) return el.tagName.toLowerCase() + '[type="' + el.type + '"]';
              return el.tagName.toLowerCase();
            }
            const usernameField = container.querySelector('input[type="email"], input[type="text"], input[name*="user"], input[name*="email"]');
            const submitBtn = container.querySelector('button[type="submit"], input[type="submit"], button:not([type])');
            const formRecipe = {
              usernameSelector: usernameField ? bestSelector(usernameField) : null,
              passwordSelector: bestSelector(pw),
              submitSelector: submitBtn ? bestSelector(submitBtn) : null,
            };
            // Send to Electron control server for encryption + storage
            fetch('http://127.0.0.1:9300/credentials/auto-save', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ domain, username, password, formUrl: location.href, formRecipe }),
            }).catch(() => {}); // fire and forget
          }

          // Watch form submit
          if (pw.closest('form')) {
            pw.closest('form').addEventListener('submit', captureAndSave, { once: true });
          }

          // Also watch Enter key on password field and button clicks
          pw.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') setTimeout(() => captureAndSave(e), 100);
          }, { once: true });

          // Watch submit/login buttons
          const btns = (pw.closest('form') || pw.parentElement?.closest('div') || document).querySelectorAll('button[type="submit"], button:not([type]), input[type="submit"], [role="button"]');
          btns.forEach(btn => {
            const txt = (btn.textContent || btn.value || '').toLowerCase();
            if (txt.match(/log.?in|sign.?in|submit|continue|next/)) {
              btn.addEventListener('click', (e) => setTimeout(() => captureAndSave(e), 100), { once: true });
            }
          });
        });
      }

      // Run immediately and re-run on DOM changes (SPAs add password fields dynamically)
      watchForms();
      let _yamilWatchTimer = null;
      new MutationObserver(() => {
        if (_yamilWatchTimer) return;
        _yamilWatchTimer = setTimeout(() => { _yamilWatchTimer = null; watchForms(); }, 2000);
      }).observe(document.body, { childList: true, subtree: true });
    })()`, true).catch(() => {});
  })

  // ── Autofill: check for saved credentials on page load ─────────
  wv.addEventListener('did-finish-load', () => {
    tryAutofill(tab)
    checkPWA(tab)
  })

  // Handle new-window requests (e.g. target="_blank") by opening in a new tab
  wv.addEventListener('new-window', (e) => {
    e.preventDefault()
    createTab(e.url, true)
  })

  // Favicon update
  wv.addEventListener('page-favicon-updated', (e) => {
    if (e.favicons && e.favicons.length > 0 && tab.faviconEl) {
      const img = new Image()
      img.onload = () => {
        const newEl = document.createElement('img')
        newEl.className = 'tab-favicon'
        newEl.src = e.favicons[0]
        tab.faviconEl.replaceWith(newEl)
        tab.faviconEl = newEl
      }
      img.src = e.favicons[0]
    }
  })

  // Find-in-page result count
  wv.addEventListener('found-in-page', (e) => {
    if (e.result && tab.id === activeTabId) {
      const { activeMatchOrdinal, matches } = e.result
      const fc = document.getElementById('find-count')
      if (fc) fc.textContent = matches > 0 ? `${activeMatchOrdinal} of ${matches}` : 'No matches'
    }
  })

  // Context menu (right-click)
  wv.addEventListener('context-menu', (e) => {
    e.preventDefault()
    showContextMenu(e.params, tab)
  })
}

// ── Context menu ─────────────────────────────────────────────────────

const ctxMenu = document.getElementById('context-menu')

function showContextMenu (params, tab) {
  const items = []
  const { x, y, linkURL, srcURL, mediaType, selectionText, isEditable, pageURL } = params

  // Link context
  if (linkURL) {
    const shortUrl = linkURL.length > 40 ? linkURL.slice(0, 40) + '...' : linkURL
    items.push({ type: 'label', text: shortUrl })
    items.push({ label: 'Open Link in New Tab', action: () => createTab(linkURL, true) })
    items.push({ label: 'Open Link in Stealth Tab', action: () => createTab(linkURL, true, 'stealth') })
    items.push({ label: 'Copy Link Address', action: () => navigator.clipboard.writeText(linkURL) })
    items.push({ type: 'sep' })
  }

  // Image context
  if (mediaType === 'image' && srcURL) {
    items.push({ label: 'Open Image in New Tab', action: () => createTab(srcURL, true) })
    items.push({ label: 'Copy Image Address', action: () => navigator.clipboard.writeText(srcURL) })
    items.push({ label: 'Save Image As...', action: () => downloadUrl(srcURL, 'image') })
    items.push({ type: 'sep' })
  }

  // Text selection context
  if (selectionText) {
    const shortSel = selectionText.length > 30 ? selectionText.slice(0, 30) + '...' : selectionText
    items.push({ label: `Copy "${shortSel}"`, action: () => { if (tab.webview) tab.webview.copy() } })
    items.push({ label: `Search for "${shortSel}"`, action: () => {
      const engine = localStorage.getItem('yamil_settings') ? JSON.parse(localStorage.getItem('yamil_settings')).searchEngine : 'google'
      const urls = { google: 'https://www.google.com/search?q=', bing: 'https://www.bing.com/search?q=', duckduckgo: 'https://duckduckgo.com/?q=', brave: 'https://search.brave.com/search?q=' }
      createTab((urls[engine] || urls.google) + encodeURIComponent(selectionText), true)
    }})
    items.push({ label: `Ask AI about "${shortSel}"`, action: () => {
      const chatInput = document.getElementById('chat-input')
      if (chatInput) { chatInput.value = selectionText; chatInput.focus() }
      setSidebarOpen(true)
    }})
    items.push({ type: 'sep' })
  }

  // Editable field context
  if (isEditable) {
    items.push({ label: 'Cut', kbd: 'Ctrl+X', action: () => { if (tab.webview) tab.webview.cut() } })
    items.push({ label: 'Copy', kbd: 'Ctrl+C', action: () => { if (tab.webview) tab.webview.copy() } })
    items.push({ label: 'Paste', kbd: 'Ctrl+V', action: () => { if (tab.webview) tab.webview.paste() } })
    items.push({ label: 'Select All', kbd: 'Ctrl+A', action: () => { if (tab.webview) tab.webview.selectAll() } })
    items.push({ type: 'sep' })
  }

  // General page actions
  items.push({ label: 'Back', kbd: 'Alt+\u2190', action: () => { if (tab.webview) tab.webview.goBack() }, disabled: !tab.webview?.canGoBack() })
  items.push({ label: 'Forward', kbd: 'Alt+\u2192', action: () => { if (tab.webview) tab.webview.goForward() }, disabled: !tab.webview?.canGoForward() })
  items.push({ label: 'Reload', kbd: 'Ctrl+R', action: () => { if (tab.webview) tab.webview.reload() } })
  items.push({ type: 'sep' })
  items.push({ label: 'Find in Page', kbd: 'Ctrl+F', action: () => openFindBar() })
  items.push({ label: 'View Page Source', kbd: 'Ctrl+U', action: () => viewPageSource() })
  items.push({ label: 'Inspect', kbd: 'F12', action: () => toggleDevTools() })

  // Build the menu DOM
  ctxMenu.innerHTML = ''
  for (const item of items) {
    if (item.type === 'sep') {
      const sep = document.createElement('div')
      sep.className = 'ctx-sep'
      ctxMenu.appendChild(sep)
    } else if (item.type === 'label') {
      const lbl = document.createElement('div')
      lbl.className = 'ctx-label'
      lbl.textContent = item.text
      ctxMenu.appendChild(lbl)
    } else {
      const el = document.createElement('div')
      el.className = 'ctx-item' + (item.disabled ? ' disabled' : '')
      el.textContent = item.label
      if (item.kbd) {
        const kbd = document.createElement('kbd')
        kbd.textContent = item.kbd
        el.appendChild(kbd)
      }
      el.addEventListener('click', () => {
        ctxMenu.style.display = 'none'
        if (item.action) item.action()
      })
      ctxMenu.appendChild(el)
    }
  }

  // Position: keep within viewport
  ctxMenu.style.display = 'block'
  const mw = ctxMenu.offsetWidth, mh = ctxMenu.offsetHeight
  const vw = window.innerWidth, vh = window.innerHeight
  ctxMenu.style.left = (x + mw > vw ? Math.max(0, x - mw) : x) + 'px'
  ctxMenu.style.top = (y + mh > vh ? Math.max(0, y - mh) : y) + 'px'
}

// Helper for context menu image save
function downloadUrl (url, type) {
  const a = document.createElement('a')
  a.href = url
  a.download = type || 'download'
  a.click()
}

// Close context menu on click anywhere or Escape
document.addEventListener('click', () => { ctxMenu.style.display = 'none' })
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && ctxMenu.style.display === 'block') ctxMenu.style.display = 'none'
})

// ── Autofill ─────────────────────────────────────────────────────────

const afBar = document.getElementById('autofill-bar')
const afUsername = document.getElementById('af-username')
const afFillBtn = document.getElementById('af-fill')
const afDismissBtn = document.getElementById('af-dismiss')
let _afPending = null // { tab, username, password, usernameSelector, passwordSelector }

// Dismissed domains for this session (don't nag)
const _afDismissed = new Set()

async function tryAutofill (tab) {
  if (!tab.webview || tab.type !== 'yamil') return
  try {
    // Step 1: Check if page has a password field
    const hasLogin = await tab.webview.executeJavaScript(
      `!!document.querySelector('input[type="password"]')`
    )
    if (!hasLogin) return

    // Step 2: Get domain
    const pageUrl = tab.webview.getURL()
    let domain
    try { domain = new URL(pageUrl).hostname.replace(/^www\./, '') } catch (_) { return }
    if (_afDismissed.has(domain)) return

    // Step 3: Fetch credentials from browser-service
    const credRes = await fetch(`http://127.0.0.1:4000/credentials?domain=${encodeURIComponent(domain)}`)
    const credData = await credRes.json()
    if (!credData.credentials || !credData.credentials.length) return

    const cred = credData.credentials[0]
    if (!cred.password_encrypted) return

    // Step 4: Decrypt password via Electron safeStorage
    const decRes = await fetch('http://127.0.0.1:9300/credentials/decrypt', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ encrypted: cred.password_encrypted })
    })
    const decData = await decRes.json()
    if (!decData.password) return

    // Step 5: Detect field selectors
    const selectors = await tab.webview.executeJavaScript(`(function() {
      var pw = document.querySelector('input[type="password"]');
      if (!pw) return null;
      var container = pw.closest('form') || document.body;
      var user = container.querySelector('input[type="email"], input[type="text"], input[name*="user"], input[name*="email"], input[name*="login"], input[autocomplete="username"]');
      if (!user) {
        var inputs = container.querySelectorAll('input[type="email"], input[type="text"]');
        for (var i = 0; i < inputs.length; i++) {
          if (inputs[i] !== pw && !inputs[i].type.match(/hidden|search/)) { user = inputs[i]; break; }
        }
      }
      function sel(el) {
        if (el.id) return '#' + el.id;
        if (el.name) return el.tagName.toLowerCase() + '[name="' + el.name + '"]';
        return el.tagName.toLowerCase() + '[type="' + el.type + '"]';
      }
      return { u: user ? sel(user) : null, p: sel(pw) };
    })()`)
    if (!selectors) return

    // Step 6: Show autofill bar
    _afPending = {
      tab,
      username: cred.username,
      password: decData.password,
      usernameSelector: selectors.u,
      passwordSelector: selectors.p
    }
    afUsername.textContent = cred.username
    afBar.style.display = 'flex'
  } catch (_) {
    // Silently fail — autofill is best-effort
  }
}

function doAutofill () {
  if (!_afPending) return
  const { tab, username, password, usernameSelector, passwordSelector } = _afPending
  if (!tab.webview) return

  const uSel = usernameSelector ? JSON.stringify(usernameSelector) : 'null'
  const pSel = JSON.stringify(passwordSelector)
  const uVal = JSON.stringify(username)
  const pVal = JSON.stringify(password)

  tab.webview.executeJavaScript(`(function() {
    function fillField(selector, value) {
      var el = document.querySelector(selector);
      if (!el) return;
      el.focus();
      el.value = value;
      el.dispatchEvent(new Event('input', {bubbles: true}));
      el.dispatchEvent(new Event('change', {bubbles: true}));
    }
    if (${uSel}) fillField(${uSel}, ${uVal});
    fillField(${pSel}, ${pVal});
  })()`, true).catch(() => {})

  afBar.style.display = 'none'
  _afPending = null
}

afFillBtn.addEventListener('click', () => doAutofill())
afDismissBtn.addEventListener('click', () => {
  afBar.style.display = 'none'
  if (_afPending) {
    try {
      const domain = new URL(_afPending.tab.webview.getURL()).hostname.replace(/^www\./, '')
      _afDismissed.add(domain)
    } catch (_) {}
  }
  _afPending = null
})

// ── Address bar ───────────────────────────────────────────────────────

function updateBar (url) {
  if (document.activeElement !== addrBar) addrBar.value = url || ''
  document.getElementById('lock-icon').textContent = url?.startsWith('https') ? '🔒' : '🔓'
}

addrBar.addEventListener('keydown', (e) => {
  // Autocomplete navigation
  const ac = document.getElementById('addr-autocomplete')
  if (ac && ac.style.display === 'block') {
    const items = ac.querySelectorAll('.ac-item')
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      const sel = ac.querySelector('.ac-item.selected')
      const idx = sel ? Math.min(parseInt(sel.dataset.idx) + 1, items.length - 1) : 0
      items.forEach((el, i) => el.classList.toggle('selected', i === idx))
      return
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault()
      const sel = ac.querySelector('.ac-item.selected')
      const idx = sel ? Math.max(parseInt(sel.dataset.idx) - 1, 0) : 0
      items.forEach((el, i) => el.classList.toggle('selected', i === idx))
      return
    }
    if (e.key === 'Enter') {
      const sel = ac.querySelector('.ac-item.selected')
      if (sel) { e.preventDefault(); sel.click(); return }
    }
    if (e.key === 'Escape') { ac.style.display = 'none'; return }
  }
  if (e.key !== 'Enter') return
  if (ac) ac.style.display = 'none'
  let url = addrBar.value.trim()
  if (!url) return
  if (!url.match(/^https?:\/\//)) url = 'https://' + url

  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.type === 'stealth' && tab.sessionId) {
    // Navigate via browser-service
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/navigate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }),
    }).catch(() => {})
  } else {
    const wv = getActiveWebview()
    if (wv) wv.loadURL(url)
  }
})

addrBar.addEventListener('focus', () => addrBar.select())

document.getElementById('btn-back').addEventListener('click', () => {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.type === 'stealth' && tab.sessionId) {
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/back`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }).catch(() => {})
  } else {
    const wv = getActiveWebview()
    if (wv && wv.canGoBack()) wv.goBack()
  }
})

document.getElementById('btn-forward').addEventListener('click', () => {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.type === 'stealth' && tab.sessionId) {
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/forward`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }).catch(() => {})
  } else {
    const wv = getActiveWebview()
    if (wv && wv.canGoForward()) wv.goForward()
  }
})

document.getElementById('btn-refresh').addEventListener('click', () => {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.type === 'stealth' && tab.sessionId) {
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/navigate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: tab.url }),
    }).catch(() => {})
  } else {
    const wv = getActiveWebview()
    if (wv) wv.reload()
  }
})

// ── New tab button ────────────────────────────────────────────────────

document.getElementById('btn-new-tab').addEventListener('click', () => {
  createTab(startUrl, true, 'yamil')
})

document.getElementById('btn-new-stealth-tab').addEventListener('click', () => {
  createTab(null, true, 'stealth')
})

// ── Keyboard shortcuts ────────────────────────────────────────────────

document.addEventListener('keydown', (e) => {
  // Ctrl+T → new yamil tab
  if (e.ctrlKey && !e.shiftKey && e.key === 't') { e.preventDefault(); createTab(startUrl, true, 'yamil') }
  // Ctrl+Shift+N → new stealth tab
  if (e.ctrlKey && e.shiftKey && (e.key === 'N' || e.key === 'n')) { e.preventDefault(); createTab(null, true, 'stealth') }
  // Ctrl+Shift+R → reader mode
  if (e.ctrlKey && e.shiftKey && (e.key === 'R' || e.key === 'r')) { e.preventDefault(); toggleReaderMode() }
  // Ctrl+W → close active tab
  if (e.ctrlKey && !e.shiftKey && e.key === 'w') { e.preventDefault(); if (activeTabId) closeTab(activeTabId) }
  // Ctrl+Shift+T → restore closed tab
  if (e.ctrlKey && e.shiftKey && (e.key === 'T' || e.key === 't')) { e.preventDefault(); restoreClosedTab() }
  // Ctrl+Tab / Ctrl+Shift+Tab → cycle tabs
  if (e.ctrlKey && e.key === 'Tab') {
    e.preventDefault()
    const idx = tabs.findIndex(t => t.id === activeTabId)
    const next = e.shiftKey
      ? (idx - 1 + tabs.length) % tabs.length
      : (idx + 1) % tabs.length
    switchTab(tabs[next].id)
  }
  // Ctrl+D → toggle bookmark
  if (e.ctrlKey && !e.shiftKey && (e.key === 'd' || e.key === 'D')) { e.preventDefault(); toggleBookmark() }
  // Ctrl+Shift+B → toggle bookmark bar
  if (e.ctrlKey && e.shiftKey && (e.key === 'B' || e.key === 'b')) { e.preventDefault(); setBookmarkBarVisible(bookmarkBar.style.display === 'none') }
  // Ctrl+Shift+O → open bookmark manager
  if (e.ctrlKey && e.shiftKey && (e.key === 'O' || e.key === 'o')) { e.preventDefault(); openBookmarkManager() }
  // Ctrl+F → find in page
  if (e.ctrlKey && !e.shiftKey && (e.key === 'f' || e.key === 'F')) { e.preventDefault(); openFindBar() }
  // Ctrl+H → history panel
  if (e.ctrlKey && !e.shiftKey && (e.key === 'h' || e.key === 'H')) { e.preventDefault(); openHistoryPanel() }
  // Ctrl+= → zoom in
  if (e.ctrlKey && (e.key === '=' || e.key === '+')) { e.preventDefault(); zoomIn() }
  // Ctrl+- → zoom out
  if (e.ctrlKey && e.key === '-') { e.preventDefault(); zoomOut() }
  // Ctrl+0 → zoom reset
  if (e.ctrlKey && e.key === '0') { e.preventDefault(); zoomReset() }
  // Ctrl+, → settings
  if (e.ctrlKey && e.key === ',') { e.preventDefault(); openSettingsPanel() }
  // Ctrl+J → downloads
  if (e.ctrlKey && !e.shiftKey && (e.key === 'j' || e.key === 'J')) { e.preventDefault(); openDownloadsPanel() }
  // Ctrl+P → print
  if (e.ctrlKey && !e.shiftKey && (e.key === 'p' || e.key === 'P')) { e.preventDefault(); printPage() }
  // Ctrl+S → save page
  if (e.ctrlKey && !e.shiftKey && (e.key === 's' || e.key === 'S')) { e.preventDefault(); savePageAs() }
  // Ctrl+U → view source
  if (e.ctrlKey && !e.shiftKey && (e.key === 'u' || e.key === 'U')) { e.preventDefault(); viewPageSource() }
  // F12 → developer tools
  if (e.key === 'F12') { e.preventDefault(); toggleDevTools() }
  // F11 → fullscreen
  if (e.key === 'F11') { e.preventDefault(); toggleFullscreen() }
  // Escape → close overlays
  if (e.key === 'Escape') {
    const findBarVisible = document.getElementById('find-bar')?.style.display !== 'none'
    const histVisible = document.getElementById('history-panel')?.style.display !== 'none'
    const settingsVisible = document.getElementById('settings-panel')?.style.display !== 'none'
    const dlVisible = document.getElementById('downloads-panel')?.style.display !== 'none'
    if (findBarVisible) { closeFindBar(); return }
    if (histVisible) { closeHistoryPanel(); return }
    if (settingsVisible) { closeSettingsPanel(); return }
    if (dlVisible) { closeDownloadsPanel(); return }
    if (bookmarkMgr.style.display !== 'none') { closeBookmarkManager(); return }
    const skillsEditor = document.getElementById('skills-editor')
    if (skillsEditor) { skillsEditor.remove(); return }
    if (document.body.classList.contains('fullscreen')) { toggleFullscreen(); return }
  }
})

// ── Sidebar toggle ────────────────────────────────────────────────────

const sidebar        = document.getElementById('sidebar')
const btnToggle      = document.getElementById('btn-sidebar-toggle')
const btnSidebarOpen = document.getElementById('btn-sidebar-open')

const navRight = document.getElementById('nav-right')

function setSidebarOpen (open) {
  if (open) {
    sidebar.classList.remove('collapsed')
    btnSidebarOpen.style.display = 'none'
    if (navRight) navRight.classList.remove('sidebar-collapsed')
  } else {
    sidebar.classList.add('collapsed')
    btnSidebarOpen.style.display = ''
    if (navRight) navRight.classList.add('sidebar-collapsed')
  }
  try { localStorage.setItem(KEY_SIDEBAR_OPEN, open ? '1' : '0') } catch (_) {}
}

btnToggle.addEventListener('click',      () => setSidebarOpen(false))
btnSidebarOpen.addEventListener('click', () => setSidebarOpen(true))

// ── Chat history persistence ──────────────────────────────────────────

function saveChatHistory () {
  try {
    const msgs = [...chatLog.children].map(el => ({
      role: el.dataset.role,
      text: el.textContent,
    }))
    pSet(KEY_CHAT_HISTORY, JSON.stringify(msgs.slice(-MAX_STORED_MSGS)))
  } catch (_) {}
}

function loadChatHistory () {
  try {
    const stored = JSON.parse(pGet(KEY_CHAT_HISTORY) || '[]')
    // Filter out old voice error messages and dedupe consecutive system messages
    const clean = stored
      .filter(m => !m.text?.startsWith('Voice error:') && !m.text?.startsWith('Voice input unavailable'))
      .filter((m, i, arr) => {
        if (i === 0) return true
        // Remove consecutive duplicate system messages (e.g. repeated "YAMIL Browser ready")
        const prev = arr[i - 1]
        return !(m.role === prev.role && m.text === prev.text && m.role === 'system')
      })
    if (clean.length !== stored.length) {
      try { pSet(KEY_CHAT_HISTORY, JSON.stringify(clean)) } catch (_) {}
    }
    if (clean.length) {
      clean.forEach(m => _appendMsg(m.role, m.text))
      chatLog.scrollTop = chatLog.scrollHeight
    }
  } catch (_) {}
}

// ── Message helpers ───────────────────────────────────────────────────

function _appendMsg (role, text) {
  const div = document.createElement('div')
  div.className = `chat-msg ${role}`
  div.dataset.role = role
  div.textContent = text
  chatLog.appendChild(div)
}

function addMsg (role, text) {
  _appendMsg(role, text)
  chatLog.scrollTop = chatLog.scrollHeight
  saveChatHistory()
}

const addSystemMsg = (t) => addMsg('system', t)
const addUserMsg   = (t) => addMsg('user', t)
const addAiMsg     = (t) => addMsg('ai', t)
const addErrorMsg  = (t) => addMsg('error', t)

// ── AI Chat ───────────────────────────────────────────────────────────

function resolveUrl (input) {
  input = input.trim().replace(/[.!?]+$/, '')
  if (input.match(/^https?:\/\//i)) return input
  if (!input.includes(' ') && input.includes('.')) return 'https://' + input
  return 'https://' + input.toLowerCase().replace(/\s+/g, '') + '.com'
}

function navigateWebview (url) {
  addrBar.value = url
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.type === 'stealth' && tab.sessionId) {
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/navigate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }),
    }).catch(() => {})
  } else {
    const wv = getActiveWebview()
    if (wv) wv.loadURL(url)
  }
}

function extractUrl (text) {
  const m = text.match(/https?:\/\/[^\s)"'\]]+/i)
  return m ? m[0].replace(/[.,;!?]+$/, '') : null
}

async function sendChat () {
  const text = chatInput.value.trim()
  if (!text) return
  chatInput.value = ''
  addUserMsg(text)

  // ── Bookmark chat commands ────────────────────────────────────
  // "bookmark this" / "save this page"
  if (/^(bookmark|save)\s+(this|current|page)/i.test(text)) {
    const tab = tabs.find(t => t.id === activeTabId)
    if (!tab || !tab.url) { addSystemMsg('No active page to bookmark.'); return }
    if (isBookmarked(tab.url)) { addSystemMsg(`Already bookmarked: "${tab.title}"`); return }
    const bm = addBookmark({ url: tab.url, title: tab.title || tab.url, favicon: '' })
    updateBookmarkStar()
    renderBookmarkBar()
    // Try AI auto-tagging if endpoint available
    if (aiEndpoint) {
      try {
        const tagRes = await fetch(aiEndpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: `Categorize this webpage. Return ONLY valid JSON: {"tags":["tag1","tag2"],"category":"one-word-category"}. Page: "${tab.title}" URL: ${tab.url}`, pageContext: { url: tab.url, title: tab.title } }),
        })
        const tagData = await tagRes.json()
        const reply = tagData.response || tagData.message || ''
        const jsonMatch = reply.match(/\{[^}]+\}/)
        if (jsonMatch) {
          const parsed = JSON.parse(jsonMatch[0])
          const bms = getBookmarks()
          const target = bms.find(b => b.id === bm.id)
          if (target) {
            if (parsed.tags) target.tags = parsed.tags
            if (parsed.category) target.category = parsed.category
            saveBookmarks(bms)
            renderBookmarkBar()
          }
          addAiMsg(`Bookmarked "${tab.title}" [${parsed.category || 'uncategorized'}] tags: ${(parsed.tags || []).join(', ') || 'none'}`)
          return
        }
      } catch (_) {}
    }
    addSystemMsg(`Bookmarked "${tab.title}"`)
    return
  }

  // "find bookmark X" / "open my saved X"
  const findBmMatch = text.match(/(?:open|find|show)\s+(?:my\s+)?(?:saved|bookmark)s?\s+(.+)/i)
  if (findBmMatch) {
    const query = findBmMatch[1].trim()
    const results = searchBookmarks(query)
    if (results.length === 0) { addSystemMsg(`No bookmarks matching "${query}"`) }
    else if (results.length === 1) {
      navigateWebview(results[0].url)
      addSystemMsg(`Opening "${results[0].title}"`)
    } else {
      addAiMsg(`Found ${results.length} bookmarks:\n` + results.map((b, i) => `${i + 1}. ${b.title} — ${b.url}`).join('\n'))
    }
    return
  }

  // "open all [category] bookmarks"
  const openAllMatch = text.match(/open\s+(?:all\s+)?(\w+)\s+bookmarks/i)
  if (openAllMatch) {
    const cat = openAllMatch[1].toLowerCase()
    const bm = getBookmarks().filter(b => b.category.toLowerCase() === cat || b.tags.some(t => t.toLowerCase() === cat))
    if (bm.length === 0) { addSystemMsg(`No bookmarks with category/tag "${cat}"`) }
    else {
      bm.forEach(b => createTab(b.url, false, 'yamil'))
      switchTab(tabs[tabs.length - bm.length].id)
      addSystemMsg(`Opened ${bm.length} bookmark(s) tagged "${cat}"`)
    }
    return
  }

  // "remember that..."
  const remMatch = text.match(/^remember\s+(?:that\s+)?(.+)/i)
  if (remMatch) {
    addAiMemoryFact(remMatch[1])
    addSystemMsg(`Remembered: "${remMatch[1]}"`)
    return
  }

  // "forget..."
  const forgetMatch = text.match(/^forget\s+(.+)/i)
  if (forgetMatch) {
    removeAiMemoryFact(forgetMatch[1])
    addSystemMsg(`Forgot memories matching "${forgetMatch[1]}"`)
    return
  }

  if (!aiEndpoint) {
    addAiMsg('No AI endpoint configured. Set AI_ENDPOINT env var.')
    return
  }

  // Background task mode
  if (bgCheck && bgCheck.checked) {
    bgCheck.checked = false
    const task = createAgentTask(text)
    taskQueue.style.display = 'block'
    addSystemMsg(`Running in background: "${text}"`)
    runBackgroundTask(task)
    return
  }

  const navMatch = text.match(/^(?:go\s+to|navigate\s+to|open|visit)\s+([^\s,]+)/i)
  if (navMatch) {
    const url = resolveUrl(navMatch[1])
    navigateWebview(url)
    addSystemMsg(`Navigating to ${url}...`)
  }

  // Resolve cross-tab references (@tab:N, @all-tabs)
  let resolvedText = text
  let extraContexts = []
  const { text: resolved, contexts } = await resolveTabReferences(text)
  resolvedText = resolved
  extraContexts = contexts

  let pageContext = {}
  // Only extract page context if AI is not blocked for this page
  if (!isAiBlockedForCurrentPage()) {
    const activeTab = tabs.find(t => t.id === activeTabId)
    if (activeTab && activeTab.type === 'stealth' && activeTab.sessionId) {
      try {
        const pcRes = await fetch(`${BROWSER_SERVICE}/sessions/${activeTab.sessionId}/evaluate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ script: '({ url: location.href, title: document.title, text: document.body.innerText.slice(0, 4000) })' }),
        })
        const pcData = await pcRes.json()
        pageContext = pcData.result || {}
      } catch (_) {}
    } else {
      const wv = getActiveWebview()
      if (wv) {
        try {
          pageContext = await wv.executeJavaScript(`({
            url:   location.href,
            title: document.title,
            text:  document.body.innerText.slice(0, 4000),
          })`)
        } catch (_) {}
      }
    }
  }

  // Build cross-tab context string
  let crossTabCtx = ''
  if (extraContexts.length > 0) {
    crossTabCtx = '\n\n[Cross-tab context:\n' + extraContexts.map(c =>
      `Tab ${c.tabIndex} "${c.title}" (${c.url}):\n${c.text?.slice(0, 2000) || 'No content'}`
    ).join('\n---\n') + ']'
  }

  // Inject AI memory context
  const memory = getAiMemory()
  const memCtx = memory.length > 0 ? '\n\n[User memories: ' + memory.map(m => m.fact).join('; ') + ']' : ''

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: resolvedText + memCtx + crossTabCtx, pageContext }),
    })
    const data = await res.json()
    const reply = data.response || data.message || JSON.stringify(data)
    addAiMsg(reply)

    if (data.navigatedUrl) {
      navigateWebview(data.navigatedUrl)
    } else if (navMatch) {
      const url = extractUrl(reply)
      if (url) navigateWebview(url)
    }
  } catch (err) {
    addErrorMsg('AI request failed: ' + err.message)
  }
}

document.getElementById('chat-send').addEventListener('click', sendChat)
chatInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat() }
})

// ── Tab persistence ───────────────────────────────────────────────────

function saveTabs () {
  try {
    const data = {
      activeTabId,
      tabs: tabs.map(t => ({ id: t.id, type: t.type || 'yamil', url: t.url, title: t.title, group: t.group || null, groupColor: t.groupColor || null, pinned: t.pinned || false })),
    }
    pSet(KEY_TABS, JSON.stringify(data))
  } catch (_) {}
}

function loadTabs () {
  try {
    const stored = JSON.parse(pGet(KEY_TABS) || 'null')
    if (stored && stored.tabs && stored.tabs.length > 0) {
      // Restore counter to avoid ID conflicts
      tabIdCounter = Math.max(...stored.tabs.map(t => t.id), 0)
      // Create tabs without activating
      stored.tabs.forEach(t => {
        const tab = createTab(t.url, false, t.type || 'yamil')
        if (t.group) { tab.group = t.group; tab.groupColor = t.groupColor; tab.tabEl.classList.add('grouped'); tab.tabEl.style.setProperty('--group-color', t.groupColor) }
        if (t.pinned) { tab.pinned = true; tab.tabEl.classList.add('pinned') }
      })
      // Activate the previously active tab (or first)
      const targetId = stored.activeTabId
      const match = tabs.find(t => t.id === targetId)
      switchTab(match ? match.id : tabs[0].id)
      return true
    }
  } catch (_) {}
  return false
}

// ── Tab context menu ──────────────────────────────────────────────────

tabsList.addEventListener('contextmenu', (e) => {
  const tabEl = e.target.closest('.tab')
  if (!tabEl) return
  e.preventDefault()
  const tabId = parseInt(tabEl.dataset.tabId)
  const tab = tabs.find(t => t.id === tabId)
  if (!tab) return

  // Remove existing context menu
  const existing = document.getElementById('tab-context-menu')
  if (existing) existing.remove()

  const menu = document.createElement('div')
  menu.id = 'tab-context-menu'
  menu.style.cssText = `position:fixed;left:${e.clientX}px;top:${e.clientY}px;z-index:10000;background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 0;min-width:180px;box-shadow:0 4px 16px rgba(0,0,0,.5);font-size:12px;color:var(--text);`

  const items = []
  // Pin/Unpin
  items.push({ label: tab.pinned ? 'Unpin Tab' : 'Pin Tab', action: () => togglePinTab(tabId) })
  items.push({ type: 'sep' })
  if (tab.type === 'yamil') {
    items.push({ label: 'Open URL as Stealth Tab', action: () => createTab(tab.url, true, 'stealth') })
  } else {
    items.push({ label: 'Open URL as YAMIL Tab', action: () => createTab(tab.url, true, 'yamil') })
  }
  items.push({ label: 'Duplicate Tab', action: () => createTab(tab.url, true, tab.type) })
  if (tab.group) {
    items.push({ label: 'Remove from Group', action: () => setTabGroup(tabId, null) })
  } else {
    items.push({ label: 'Add to Group...', action: () => {
      const name = prompt('Group name:')
      if (name && name.trim()) setTabGroup(tabId, name.trim())
    }})
  }
  items.push({ type: 'sep' })
  items.push({ label: 'Close Tab', action: () => closeTab(tabId) })
  if (recentlyClosed.length > 0) {
    items.push({ label: `Restore Closed Tab (${recentlyClosed.length})`, action: () => restoreClosedTab() })
  }
  items.push({ label: 'Close Other Tabs', action: () => {
    tabs.filter(t => t.id !== tabId && !t.pinned).forEach(t => closeTab(t.id))
  }})

  items.forEach(item => {
    if (item.type === 'sep') {
      const sep = document.createElement('div')
      sep.style.cssText = 'height:1px;background:var(--border);margin:4px 0;'
      menu.appendChild(sep)
      return
    }
    const el = document.createElement('div')
    el.textContent = item.label
    el.style.cssText = 'padding:6px 12px;cursor:pointer;'
    el.addEventListener('mouseenter', () => { el.style.background = 'var(--bg3)' })
    el.addEventListener('mouseleave', () => { el.style.background = 'transparent' })
    el.addEventListener('click', () => { menu.remove(); item.action() })
    menu.appendChild(el)
  })

  document.body.appendChild(menu)

  // Auto-close on click outside
  const close = (ev) => {
    if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close) }
  }
  setTimeout(() => document.addEventListener('click', close), 0)
})

// ── Bookmark UI ──────────────────────────────────────────────────

const btnBookmark    = document.getElementById('btn-bookmark')
const bookmarkBar    = document.getElementById('bookmark-bar')
const bookmarkChips  = document.getElementById('bookmark-chips')
const btnBmMore      = document.getElementById('btn-bm-more')
const bookmarkMgr    = document.getElementById('bookmark-manager')
const bmBody         = document.getElementById('bm-body')
const bmSearch       = document.getElementById('bm-search')
const mainEl         = document.getElementById('main')

function updateBookmarkStar () {
  const tab = tabs.find(t => t.id === activeTabId)
  const url = tab ? tab.url : ''
  if (isBookmarked(url)) {
    btnBookmark.innerHTML = '&#9733;'
    btnBookmark.classList.add('bookmarked')
  } else {
    btnBookmark.innerHTML = '&#9734;'
    btnBookmark.classList.remove('bookmarked')
  }
}

function toggleBookmark () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.url) return
  if (isBookmarked(tab.url)) {
    const bm = isBookmarked(tab.url)
    removeBookmark(bm.id)
  } else {
    addBookmark({ url: tab.url, title: tab.title || tab.url, favicon: '' })
  }
  updateBookmarkStar()
  renderBookmarkBar()
}

btnBookmark.addEventListener('click', toggleBookmark)

function renderBookmarkBar () {
  bookmarkChips.innerHTML = ''
  const bm = getBookmarks().sort((a, b) => (b.visits - a.visits) || (b.createdAt - a.createdAt))
  const show = bm.slice(0, 15)
  show.forEach(b => {
    const chip = document.createElement('div')
    chip.className = 'bm-chip'
    chip.title = b.url
    if (b.favicon) {
      const img = document.createElement('img')
      img.src = b.favicon
      img.onerror = () => { img.style.display = 'none' }
      chip.appendChild(img)
    }
    const span = document.createElement('span')
    span.textContent = b.title
    chip.appendChild(span)
    chip.addEventListener('click', () => {
      navigateWebview(b.url)
      const bms = getBookmarks()
      const target = bms.find(x => x.id === b.id)
      if (target) { target.visits++; saveBookmarks(bms) }
    })
    chip.addEventListener('contextmenu', (e) => {
      e.preventDefault()
      showBookmarkContextMenu(e, b)
    })
    bookmarkChips.appendChild(chip)
  })
}

function showBookmarkContextMenu (e, bm) {
  const existing = document.getElementById('bm-context-menu')
  if (existing) existing.remove()

  const menu = document.createElement('div')
  menu.id = 'bm-context-menu'
  menu.style.cssText = `position:fixed;left:${e.clientX}px;top:${e.clientY}px;z-index:10001;background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 0;min-width:180px;box-shadow:0 4px 16px rgba(0,0,0,.5);font-size:12px;color:var(--text);`

  const items = [
    { label: 'Open in New Tab', action: () => createTab(bm.url, true, 'yamil') },
    { label: 'Open in Stealth Tab', action: () => createTab(bm.url, true, 'stealth') },
    { label: 'Edit Title', action: () => {
      const newTitle = prompt('Edit bookmark title:', bm.title)
      if (newTitle !== null && newTitle.trim()) {
        const bms = getBookmarks()
        const target = bms.find(x => x.id === bm.id)
        if (target) { target.title = newTitle.trim(); saveBookmarks(bms); renderBookmarkBar() }
      }
    }},
    { label: 'Delete', action: () => { removeBookmark(bm.id); renderBookmarkBar(); updateBookmarkStar() } },
  ]

  items.forEach(item => {
    const el = document.createElement('div')
    el.textContent = item.label
    el.style.cssText = 'padding:6px 12px;cursor:pointer;'
    el.addEventListener('mouseenter', () => { el.style.background = 'var(--bg3)' })
    el.addEventListener('mouseleave', () => { el.style.background = 'transparent' })
    el.addEventListener('click', () => { menu.remove(); item.action() })
    menu.appendChild(el)
  })

  document.body.appendChild(menu)
  const close = (ev) => { if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close) } }
  setTimeout(() => document.addEventListener('click', close), 0)
}

function setBookmarkBarVisible (visible) {
  bookmarkBar.style.display = visible ? 'flex' : 'none'
  mainEl.classList.toggle('with-bmbar', visible)
  try { localStorage.setItem(KEY_BMBAR_VIS, visible ? '1' : '0') } catch (_) {}
  if (visible) renderBookmarkBar()
}

btnBmMore.addEventListener('click', () => openBookmarkManager())

// Bookmark Manager
function openBookmarkManager () {
  bookmarkMgr.style.display = 'flex'
  bmSearch.value = ''
  renderBookmarkManager()
  bmSearch.focus()
}

function closeBookmarkManager () {
  bookmarkMgr.style.display = 'none'
}

document.getElementById('bm-close').addEventListener('click', closeBookmarkManager)
bookmarkMgr.addEventListener('click', (e) => { if (e.target === bookmarkMgr) closeBookmarkManager() })

bmSearch.addEventListener('input', () => renderBookmarkManager(bmSearch.value.trim()))

function renderBookmarkManager (filter) {
  bmBody.innerHTML = ''
  let bm = filter ? searchBookmarks(filter) : getBookmarks()
  bm = bm.sort((a, b) => (b.visits - a.visits) || (b.createdAt - a.createdAt))

  // Group by category
  const groups = {}
  bm.forEach(b => {
    const cat = b.category || 'Uncategorized'
    if (!groups[cat]) groups[cat] = []
    groups[cat].push(b)
  })

  for (const [cat, items] of Object.entries(groups)) {
    const section = document.createElement('div')
    section.className = 'bm-category'
    const title = document.createElement('div')
    title.className = 'bm-category-title'
    title.textContent = cat
    section.appendChild(title)

    items.forEach(b => {
      const row = document.createElement('div')
      row.className = 'bm-item'

      const titleEl = document.createElement('span')
      titleEl.className = 'bm-item-title'
      titleEl.textContent = b.title

      const urlEl = document.createElement('span')
      urlEl.className = 'bm-item-url'
      urlEl.textContent = b.url

      const tagsEl = document.createElement('span')
      tagsEl.className = 'bm-item-tags'
      b.tags.forEach(t => {
        const tag = document.createElement('span')
        tag.className = 'bm-tag'
        tag.textContent = t
        tagsEl.appendChild(tag)
      })

      const del = document.createElement('button')
      del.className = 'bm-item-del'
      del.innerHTML = '&times;'
      del.title = 'Delete bookmark'
      del.addEventListener('click', (e) => {
        e.stopPropagation()
        removeBookmark(b.id)
        renderBookmarkManager(bmSearch.value.trim())
        renderBookmarkBar()
        updateBookmarkStar()
      })

      row.appendChild(titleEl)
      row.appendChild(urlEl)
      row.appendChild(tagsEl)
      row.appendChild(del)
      row.addEventListener('click', () => { navigateWebview(b.url); closeBookmarkManager() })
      section.appendChild(row)
    })

    bmBody.appendChild(section)
  }

  if (bm.length === 0) {
    bmBody.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:40px;font-size:12px;">No bookmarks yet. Press Ctrl+D to bookmark a page.</div>'
  }
}

// ── Progress bar ──────────────────────────────────────────────────────

const progressBar = document.getElementById('progress-bar')

function showProgressBar () {
  progressBar.classList.remove('done')
  progressBar.classList.add('loading')
}

function hideProgressBar () {
  progressBar.classList.remove('loading')
  progressBar.classList.add('done')
  setTimeout(() => { progressBar.classList.remove('done') }, 600)
}

// ── Find in page ─────────────────────────────────────────────────────

const findBarEl = document.getElementById('find-bar')
const findInput = document.getElementById('find-input')
const findCount = document.getElementById('find-count')

function openFindBar () {
  findBarEl.style.display = 'flex'
  findInput.focus()
  findInput.select()
}

function closeFindBar () {
  findBarEl.style.display = 'none'
  findInput.value = ''
  findCount.textContent = ''
  const wv = getActiveWebview()
  if (wv) wv.stopFindInPage('clearSelection')
}

function doFind (forward = true) {
  const text = findInput.value.trim()
  if (!text) { findCount.textContent = ''; return }
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab) return
  if (tab.type === 'yamil' && tab.webview) {
    tab.webview.findInPage(text, { forward })
  }
}

findInput.addEventListener('input', () => doFind())
findInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); doFind(!e.shiftKey) }
  if (e.key === 'Escape') { closeFindBar() }
})
document.getElementById('find-next').addEventListener('click', () => doFind(true))
document.getElementById('find-prev').addEventListener('click', () => doFind(false))
document.getElementById('find-close').addEventListener('click', closeFindBar)

// ── Page Zoom ────────────────────────────────────────────────────────

const statusZoom = document.getElementById('status-zoom')

function updateZoomDisplay () {
  const tab = tabs.find(t => t.id === activeTabId)
  const level = tab ? tab.zoom : 0
  if (level === 0) {
    statusZoom.style.display = 'none'
  } else {
    const pct = Math.round(Math.pow(1.2, level) * 100)
    statusZoom.textContent = pct + '%'
    statusZoom.style.display = ''
  }
}

function setZoom (level) {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab) return
  tab.zoom = level
  if (tab.type === 'yamil' && tab.webview) {
    tab.webview.setZoomLevel(level)
  }
  updateZoomDisplay()
}

function zoomIn () { const tab = tabs.find(t => t.id === activeTabId); setZoom((tab ? tab.zoom : 0) + 1) }
function zoomOut () { const tab = tabs.find(t => t.id === activeTabId); setZoom((tab ? tab.zoom : 0) - 1) }
function zoomReset () { setZoom(0) }

statusZoom.addEventListener('click', zoomReset)

// ── History ──────────────────────────────────────────────────────────

const historyPanel = document.getElementById('history-panel')
const histBody     = document.getElementById('hist-body')
const histSearch   = document.getElementById('hist-search')

function getHistory () {
  try { return JSON.parse(pGet(KEY_HISTORY) || '[]') } catch (_) { return [] }
}

function saveHistoryData (arr) {
  try { pSet(KEY_HISTORY, JSON.stringify(arr)) } catch (_) {}
}

function recordHistory (url, title) {
  if (!url || url === 'about:blank') return
  const hist = getHistory()
  if (hist.length > 0 && hist[0].url === url) return
  hist.unshift({ url, title: title || url, timestamp: Date.now() })
  if (hist.length > MAX_HISTORY) hist.length = MAX_HISTORY
  saveHistoryData(hist)
}

function searchHistory (query) {
  const q = query.toLowerCase()
  return getHistory().filter(h =>
    h.url.toLowerCase().includes(q) || (h.title && h.title.toLowerCase().includes(q))
  )
}

function clearHistory () {
  saveHistoryData([])
}

function openHistoryPanel () {
  historyPanel.style.display = 'flex'
  histSearch.value = ''
  renderHistory()
  histSearch.focus()
}

function closeHistoryPanel () {
  historyPanel.style.display = 'none'
}

function renderHistory (filter) {
  histBody.innerHTML = ''
  let items = filter ? searchHistory(filter) : getHistory()
  items = items.slice(0, 200)

  const groups = {}
  items.forEach(h => {
    const d = new Date(h.timestamp)
    const key = d.toLocaleDateString()
    if (!groups[key]) groups[key] = []
    groups[key].push(h)
  })

  for (const [date, entries] of Object.entries(groups)) {
    const dateEl = document.createElement('div')
    dateEl.className = 'hist-date'
    dateEl.textContent = date
    histBody.appendChild(dateEl)

    entries.forEach(h => {
      const row = document.createElement('div')
      row.className = 'hist-item'
      const titleEl = document.createElement('span')
      titleEl.className = 'hist-item-title'
      titleEl.textContent = h.title || h.url
      const urlEl = document.createElement('span')
      urlEl.className = 'hist-item-url'
      urlEl.textContent = h.url
      const timeEl = document.createElement('span')
      timeEl.className = 'hist-item-time'
      timeEl.textContent = new Date(h.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      row.appendChild(titleEl)
      row.appendChild(urlEl)
      row.appendChild(timeEl)
      row.addEventListener('click', () => { navigateWebview(h.url); closeHistoryPanel() })
      histBody.appendChild(row)
    })
  }

  if (items.length === 0) {
    histBody.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:40px;font-size:12px;">No history yet.</div>'
  }
}

document.getElementById('hist-close').addEventListener('click', closeHistoryPanel)
historyPanel.addEventListener('click', (e) => { if (e.target === historyPanel) closeHistoryPanel() })
histSearch.addEventListener('input', () => renderHistory(histSearch.value.trim()))

// ── Address bar autocomplete ─────────────────────────────────────────

const acDropdown = document.getElementById('addr-autocomplete')

let _acSuggestTimer = null

function showAutocomplete () {
  const query = addrBar.value.trim().toLowerCase()
  if (!query) { hideAutocomplete(); return }

  const bms = getBookmarks().filter(b =>
    b.url.toLowerCase().includes(query) || b.title.toLowerCase().includes(query)
  ).map(b => ({ url: b.url, title: b.title, _type: 'bm' }))

  const hist = getHistory().filter(h =>
    h.url.toLowerCase().includes(query) || (h.title && h.title.toLowerCase().includes(query))
  ).map(h => ({ url: h.url, title: h.title, _type: 'hist' }))

  const seen = new Set()
  const combined = []
  for (const item of [...bms, ...hist]) {
    if (!seen.has(item.url)) {
      seen.add(item.url)
      combined.push(item)
      if (combined.length >= 6) break
    }
  }

  // Fetch search suggestions async (debounced)
  if (_acSuggestTimer) clearTimeout(_acSuggestTimer)
  _acSuggestTimer = setTimeout(() => fetchSearchSuggestions(query, combined.length), 250)

  if (combined.length === 0 && !query.includes('.')) {
    // Show "Search for..." hint while waiting for suggestions
    acDropdown.innerHTML = ''
    const hint = document.createElement('div')
    hint.className = 'ac-item'
    hint.dataset.idx = '0'
    const t = document.createElement('span')
    t.className = 'ac-item-title'
    t.textContent = `Search for "${query}"`
    const ty = document.createElement('span')
    ty.className = 'ac-item-type search'
    ty.textContent = 'search'
    hint.appendChild(t)
    hint.appendChild(ty)
    hint.addEventListener('click', () => {
      hideAutocomplete()
      navigateToSearch(query)
    })
    acDropdown.appendChild(hint)
    acDropdown.style.display = 'block'
    return
  }

  if (combined.length === 0) { hideAutocomplete(); return }

  acDropdown.innerHTML = ''
  combined.forEach((item, i) => {
    const row = document.createElement('div')
    row.className = 'ac-item'
    row.dataset.idx = i
    const titleEl = document.createElement('span')
    titleEl.className = 'ac-item-title'
    titleEl.textContent = item.title || item.url
    const urlEl = document.createElement('span')
    urlEl.className = 'ac-item-url'
    urlEl.textContent = item.url
    const typeEl = document.createElement('span')
    typeEl.className = 'ac-item-type ' + item._type
    typeEl.textContent = item._type === 'bm' ? 'bookmark' : 'history'
    row.appendChild(titleEl)
    row.appendChild(urlEl)
    row.appendChild(typeEl)
    row.addEventListener('click', () => {
      addrBar.value = item.url
      hideAutocomplete()
      let url = item.url
      if (!url.match(/^https?:\/\//)) url = 'https://' + url
      const tab = tabs.find(t => t.id === activeTabId)
      if (tab && tab.type === 'stealth' && tab.sessionId) {
        fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/navigate`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url }),
        }).catch(() => {})
      } else {
        const wv = getActiveWebview()
        if (wv) wv.loadURL(url)
      }
    })
    acDropdown.appendChild(row)
  })
  acDropdown.style.display = 'block'
}

function hideAutocomplete () {
  if (acDropdown) acDropdown.style.display = 'none'
}

function navigateToSearch (query) {
  const settings = JSON.parse(localStorage.getItem('yamil_settings') || '{}')
  const engine = settings.searchEngine || 'google'
  const urls = { google: 'https://www.google.com/search?q=', bing: 'https://www.bing.com/search?q=', duckduckgo: 'https://duckduckgo.com/?q=', brave: 'https://search.brave.com/search?q=' }
  const searchUrl = (urls[engine] || urls.google) + encodeURIComponent(query)
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.type === 'stealth' && tab.sessionId) {
    fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/navigate`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: searchUrl }),
    }).catch(() => {})
  } else {
    const wv = getActiveWebview()
    if (wv) wv.loadURL(searchUrl)
  }
}

async function fetchSearchSuggestions (query, existingCount) {
  if (!query || query.length < 2) return
  try {
    // Google Suggest API (JSONP-style, returns array)
    const res = await fetch(`https://suggestqueries.google.com/complete/search?client=firefox&q=${encodeURIComponent(query)}`, {
      signal: AbortSignal.timeout(2000)
    })
    const data = await res.json()
    // data = ["query", ["suggestion1", "suggestion2", ...]]
    const suggestions = data[1] || []
    if (!suggestions.length) return
    // Don't update if input has changed
    if (addrBar.value.trim().toLowerCase() !== query) return

    // Append search suggestions to existing dropdown
    const maxSuggestions = Math.min(suggestions.length, 4)
    for (let i = 0; i < maxSuggestions; i++) {
      const s = suggestions[i]
      const idx = existingCount + i
      const row = document.createElement('div')
      row.className = 'ac-item'
      row.dataset.idx = idx
      const titleEl = document.createElement('span')
      titleEl.className = 'ac-item-title'
      titleEl.textContent = s
      const typeEl = document.createElement('span')
      typeEl.className = 'ac-item-type search'
      typeEl.textContent = 'search'
      row.appendChild(titleEl)
      row.appendChild(typeEl)
      row.addEventListener('click', () => {
        hideAutocomplete()
        navigateToSearch(s)
      })
      acDropdown.appendChild(row)
    }
    if (acDropdown.children.length > 0) acDropdown.style.display = 'block'
  } catch (_) {
    // Silently fail — suggestions are best-effort
  }
}

addrBar.addEventListener('input', showAutocomplete)
addrBar.addEventListener('blur', () => { setTimeout(hideAutocomplete, 150) })

// ── AI Summarize ─────────────────────────────────────────────────────

document.getElementById('btn-summarize').addEventListener('click', async () => {
  if (!aiEndpoint) { addSystemMsg('No AI endpoint configured.'); return }
  if (isAiBlockedForCurrentPage()) { addSystemMsg('AI is blocked for this page. Click the eye icon to allow.'); return }
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab) { addSystemMsg('No active tab.'); return }

  addSystemMsg('Summarizing page...')
  let pageText = ''
  if (tab.type === 'stealth' && tab.sessionId) {
    try {
      const r = await fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/evaluate`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ script: 'document.body.innerText.slice(0, 8000)' }),
      })
      const d = await r.json()
      pageText = d.result || ''
    } catch (_) {}
  } else if (tab.webview) {
    try { pageText = await tab.webview.executeJavaScript('document.body.innerText.slice(0, 8000)') } catch (_) {}
  }

  if (!pageText) { addSystemMsg('Could not extract page text.'); return }

  try {
    const r = await fetch(aiEndpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'Summarize this page concisely in 3-5 bullet points:\n\n' + pageText, pageContext: { url: tab.url, title: tab.title } }),
    })
    const d = await r.json()
    addAiMsg(d.response || d.message || 'No summary available.')
  } catch (e) { addErrorMsg('Summarize failed: ' + e.message) }
})

// ── AI Memory ────────────────────────────────────────────────────────

function getAiMemory () {
  try { return JSON.parse(pGet(KEY_AI_MEMORY) || '[]') } catch (_) { return [] }
}

function saveAiMemory (arr) {
  try { pSet(KEY_AI_MEMORY, JSON.stringify(arr)) } catch (_) {}
}

function addAiMemoryFact (fact) {
  const mem = getAiMemory()
  mem.push({ fact, timestamp: Date.now() })
  if (mem.length > 100) mem.shift()
  saveAiMemory(mem)
}

function removeAiMemoryFact (query) {
  const q = query.toLowerCase()
  const before = getAiMemory()
  const after = before.filter(m => !m.fact.toLowerCase().includes(q))
  saveAiMemory(after)
  return after.length !== before.length
}

// ── Fullscreen ───────────────────────────────────────────────────────

function toggleFullscreen () {
  if (window.YAMIL_IPC && window.YAMIL_IPC.toggleFullscreen) {
    window.YAMIL_IPC.toggleFullscreen()
  } else {
    document.body.classList.toggle('fullscreen')
  }
}

if (window.YAMIL_IPC && window.YAMIL_IPC.onFullscreenChange) {
  window.YAMIL_IPC.onFullscreenChange((isFs) => {
    document.body.classList.toggle('fullscreen', isFs)
  })
}

// ── Window control buttons ────────────────────────────────────────────
document.getElementById('wc-minimize')?.addEventListener('click', () => {
  if (window.YAMIL_IPC?.minimize) window.YAMIL_IPC.minimize()
})
document.getElementById('wc-maximize')?.addEventListener('click', () => {
  if (window.YAMIL_IPC?.maximize) window.YAMIL_IPC.maximize()
})
document.getElementById('wc-close')?.addEventListener('click', () => {
  if (window.YAMIL_IPC?.close) window.YAMIL_IPC.close()
})

// ── App menu (three-dot dropdown) ────────────────────────────────────

const appMenu = document.getElementById('app-menu')
const btnMenu = document.getElementById('btn-menu')

function toggleAppMenu () {
  if (!appMenu) return
  const showing = appMenu.style.display !== 'none'
  appMenu.style.display = showing ? 'none' : 'block'
  if (!showing) {
    // Position menu below the button, aligned right
    const rect = btnMenu.getBoundingClientRect()
    appMenu.style.top = (rect.bottom + 4) + 'px'
    appMenu.style.right = (window.innerWidth - rect.right) + 'px'
    // Update zoom level display
    const tab = tabs.find(t => t.id === activeTabId)
    const z = tab ? tab.zoom : 0
    const pct = Math.round(100 * Math.pow(1.2, z))
    const zl = document.getElementById('menu-zoom-level')
    if (zl) zl.textContent = pct + '%'
  }
}

if (btnMenu) btnMenu.addEventListener('click', (e) => { e.stopPropagation(); toggleAppMenu() })

// Close menu on outside click
document.addEventListener('click', (e) => {
  if (appMenu && appMenu.style.display !== 'none' && !appMenu.contains(e.target) && e.target !== btnMenu) {
    appMenu.style.display = 'none'
  }
})

// Menu item actions
if (appMenu) {
  appMenu.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-action]')
    if (!btn) return
    const action = btn.dataset.action
    switch (action) {
      case 'new-tab':    createTab(startUrl, true, 'yamil'); break
      case 'new-stealth': createTab(startUrl, true, 'stealth'); break
      case 'history':    openHistoryPanel(); break
      case 'bookmarks':  openBookmarkManager(); break
      case 'downloads':  openDownloadsPanel(); break
      case 'find':       openFindBar(); break
      case 'zoom-in':    zoomIn(); e.stopPropagation(); updateMenuZoom(); return
      case 'zoom-out':   zoomOut(); e.stopPropagation(); updateMenuZoom(); return
      case 'print':      printPage(); break
      case 'save-page':  savePageAs(); break
      case 'copy-url':   copyUrl(); break
      case 'view-source': viewPageSource(); break
      case 'dev-tools':  toggleDevTools(); break
      case 'fullscreen': toggleFullscreen(); break
      case 'settings':   openSettingsPanel(); break
    }
    appMenu.style.display = 'none'
  })
}

function updateMenuZoom () {
  const tab = tabs.find(t => t.id === activeTabId)
  const z = tab ? tab.zoom : 0
  const pct = Math.round(100 * Math.pow(1.2, z))
  const zl = document.getElementById('menu-zoom-level')
  if (zl) zl.textContent = pct + '%'
}

// ── Print, Save, DevTools ────────────────────────────────────────────

function printPage () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.webview) tab.webview.print()
}

function savePageAs () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.webview) return
  // Use Electron's save dialog via IPC
  if (window.electronAPI && window.electronAPI.savePageAs) {
    window.electronAPI.savePageAs(tab.webview.getURL())
  } else {
    // Fallback: download the page content
    tab.webview.executeJavaScript(`document.documentElement.outerHTML`).then(html => {
      const blob = new Blob([html], { type: 'text/html' })
      const a = document.createElement('a')
      a.href = URL.createObjectURL(blob)
      a.download = (tab.title || 'page') + '.html'
      a.click()
      URL.revokeObjectURL(a.href)
    })
  }
}

function copyUrl () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.webview) {
    const url = tab.webview.getURL()
    navigator.clipboard.writeText(url).then(() => {
      // Brief visual feedback in address bar
      const orig = addrBar.value
      addrBar.value = 'URL copied!'
      setTimeout(() => { addrBar.value = orig }, 1000)
    })
  }
}

function viewPageSource () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.webview) {
    const url = tab.webview.getURL()
    createTab('view-source:' + url, true, 'yamil')
  }
}

function toggleDevTools () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (tab && tab.webview) {
    if (tab.webview.isDevToolsOpened()) {
      tab.webview.closeDevTools()
    } else {
      tab.webview.openDevTools()
    }
  }
}

// ── Settings panel ───────────────────────────────────────────────────

const KEY_SETTINGS = 'yamil_settings'
const settingsPanel = document.getElementById('settings-panel')

function getSettings () {
  try { return JSON.parse(pGet(KEY_SETTINGS) || '{}') } catch (_) { return {} }
}

function saveSetting (key, value) {
  const s = getSettings()
  s[key] = value
  try { pSet(KEY_SETTINGS, JSON.stringify(s)) } catch (_) {}
}

function openSettingsPanel () {
  settingsPanel.style.display = 'flex'
  const s = getSettings()
  const hp = document.getElementById('set-homepage')
  const se = document.getElementById('set-search-engine')
  const sp = document.getElementById('set-sidebar-pos')
  const bb = document.getElementById('set-bmbar')
  const ae = document.getElementById('set-ai-endpoint')
  if (hp) hp.value = s.homepage || startUrl
  if (se) se.value = s.searchEngine || 'google'
  if (sp) sp.value = s.sidebarPos || 'right'
  if (bb) bb.checked = bookmarkBar.style.display !== 'none'
  if (ae) ae.value = s.aiEndpoint || aiEndpoint || ''
}

function closeSettingsPanel () {
  settingsPanel.style.display = 'none'
}

document.getElementById('settings-close').addEventListener('click', closeSettingsPanel)
settingsPanel.addEventListener('click', (e) => { if (e.target === settingsPanel) closeSettingsPanel() })

document.getElementById('set-homepage')?.addEventListener('change', (e) => saveSetting('homepage', e.target.value))
document.getElementById('set-search-engine')?.addEventListener('change', (e) => saveSetting('searchEngine', e.target.value))
document.getElementById('set-sidebar-pos')?.addEventListener('change', (e) => {
  saveSetting('sidebarPos', e.target.value)
  const s = document.getElementById('sidebar')
  const m = document.getElementById('main')
  if (e.target.value === 'left') {
    m.style.flexDirection = 'row-reverse'
    s.style.borderLeft = 'none'
    s.style.borderRight = '1px solid var(--border)'
  } else {
    m.style.flexDirection = ''
    s.style.borderLeft = '1px solid var(--border)'
    s.style.borderRight = ''
  }
})
document.getElementById('set-bmbar')?.addEventListener('change', (e) => setBookmarkBarVisible(e.target.checked))
document.getElementById('set-ai-endpoint')?.addEventListener('change', (e) => saveSetting('aiEndpoint', e.target.value))
document.getElementById('set-clear-history')?.addEventListener('click', () => {
  clearHistory()
  addSystemMsg('History cleared.')
})
document.getElementById('set-clear-chat')?.addEventListener('click', () => {
  chatLog.innerHTML = ''
  try { pRemove(KEY_CHAT_HISTORY) } catch (_) {}
  addSystemMsg('Chat history cleared.')
})
document.getElementById('set-clear-memory')?.addEventListener('click', () => {
  saveAiMemory([])
  addSystemMsg('AI memory cleared.')
})

// ── Reader mode ──────────────────────────────────────────────────────

const readerOverlay = document.getElementById('reader-overlay')
const readerContent = document.getElementById('reader-content')
let readerFontSize = 18

async function toggleReaderMode () {
  if (readerOverlay.style.display !== 'none') {
    readerOverlay.style.display = 'none'
    return
  }

  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.webview) return

  try {
    const data = await tab.webview.executeJavaScript(`(function() {
      // Simple article extraction
      var article = document.querySelector('article') || document.querySelector('[role="main"]') || document.querySelector('main') || document.querySelector('.post-content, .article-content, .entry-content, .content');
      var root = article || document.body;
      var title = document.querySelector('h1')?.innerText || document.title;

      // Get text content with structure
      function extract(el) {
        var html = '';
        var dominated = el.querySelectorAll('p, h1, h2, h3, h4, h5, h6, blockquote, pre, ul, ol, img, figure');
        if (dominated.length > 3) {
          dominated.forEach(function(n) {
            if (n.tagName === 'IMG') {
              html += '<img src="' + n.src + '" alt="' + (n.alt || '') + '">';
            } else {
              html += '<' + n.tagName.toLowerCase() + '>' + n.innerHTML + '</' + n.tagName.toLowerCase() + '>';
            }
          });
        } else {
          html = root.innerHTML;
        }
        return html;
      }

      var content = extract(root);
      var wordCount = root.innerText.split(/\\s+/).length;
      var readTime = Math.ceil(wordCount / 200);

      return { title: title, content: content, wordCount: wordCount, readTime: readTime, url: location.href };
    })()`)

    if (!data || !data.content) return

    readerContent.innerHTML = `
      <h1>${data.title}</h1>
      <div class="reader-meta">${data.wordCount} words &middot; ${data.readTime} min read &middot; ${data.url}</div>
      ${data.content}
    `
    document.getElementById('reader-time').textContent = `${data.readTime} min read`

    // Set theme
    const theme = document.getElementById('reader-theme').value
    readerOverlay.className = theme
    readerOverlay.style.display = 'block'
    readerContent.style.fontSize = readerFontSize + 'px'
  } catch (_) {}
}

document.getElementById('btn-reader')?.addEventListener('click', toggleReaderMode)
document.getElementById('reader-close')?.addEventListener('click', () => { readerOverlay.style.display = 'none' })
document.getElementById('reader-theme')?.addEventListener('change', (e) => {
  readerOverlay.className = e.target.value
})
document.getElementById('reader-font-inc')?.addEventListener('click', () => {
  readerFontSize = Math.min(readerFontSize + 2, 32)
  readerContent.style.fontSize = readerFontSize + 'px'
})
document.getElementById('reader-font-dec')?.addEventListener('click', () => {
  readerFontSize = Math.max(readerFontSize - 2, 12)
  readerContent.style.fontSize = readerFontSize + 'px'
})

// ── Picture-in-Picture ───────────────────────────────────────────────

async function togglePiP () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.webview) return

  try {
    await tab.webview.executeJavaScript(`(function() {
      // Find the largest playing video, or first video
      var videos = Array.from(document.querySelectorAll('video'));
      if (!videos.length) { alert('No video found on this page.'); return; }
      var video = videos.sort(function(a, b) {
        return (b.videoWidth * b.videoHeight) - (a.videoWidth * a.videoHeight);
      })[0];

      if (document.pictureInPictureElement) {
        document.exitPictureInPicture();
      } else if (video.requestPictureInPicture) {
        video.requestPictureInPicture().catch(function(e) {
          // Try playing first then PiP
          video.play().then(function() {
            video.requestPictureInPicture();
          }).catch(function() {
            alert('Could not enter Picture-in-Picture: ' + e.message);
          });
        });
      }
    })()`, true)
  } catch (_) {}
}

document.getElementById('btn-pip')?.addEventListener('click', togglePiP)

// ── Translation ──────────────────────────────────────────────────────

async function translatePage () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.webview) return

  // Get target language from user
  const lang = prompt('Translate page to (e.g., Spanish, French, Japanese):', 'Spanish')
  if (!lang) return

  try {
    // Extract page text
    const text = await tab.webview.executeJavaScript('document.body.innerText.slice(0, 6000)')
    if (!text) return

    // Use AI endpoint for translation
    if (!aiEndpoint) { addSystemMsg('No AI endpoint configured for translation.'); return }

    setSidebarOpen(true)
    addSystemMsg(`Translating page to ${lang}...`)

    const response = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: `Translate the following text to ${lang}. Preserve formatting. Only output the translation, no explanations:\n\n${text}`,
        context: { url: tab.url, title: tab.title },
      }),
      signal: AbortSignal.timeout(30000),
    })

    if (response.ok) {
      const data = await response.json()
      const translated = data.response || data.message || data.text || JSON.stringify(data)
      addAssistantMsg(translated)
    } else {
      addSystemMsg('Translation failed: ' + response.statusText)
    }
  } catch (e) {
    addSystemMsg('Translation error: ' + e.message)
  }
}

document.getElementById('btn-translate')?.addEventListener('click', translatePage)

// ── Ad blocker settings ──────────────────────────────────────────────

async function refreshAdblockStats () {
  try {
    const res = await fetch('http://127.0.0.1:9300/adblock/stats')
    const stats = await res.json()
    const countEl = document.getElementById('set-adblock-count')
    const toggleEl = document.getElementById('set-adblock-toggle')
    if (countEl) countEl.textContent = `${stats.totalBlocked} blocked`
    if (toggleEl) toggleEl.textContent = stats.enabled ? 'Enabled' : 'Disabled'
  } catch (_) {}
}

document.getElementById('set-adblock-toggle')?.addEventListener('click', async () => {
  try {
    await fetch('http://127.0.0.1:9300/adblock/toggle', { method: 'POST' })
    refreshAdblockStats()
  } catch (_) {}
})

document.getElementById('set-adblock-whitelist')?.addEventListener('click', async () => {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.webview) return
  try {
    const domain = new URL(tab.webview.getURL()).hostname.replace(/^www\./, '')
    await fetch('http://127.0.0.1:9300/adblock/whitelist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ domain })
    })
    addSystemMsg(`${domain} added to ad blocker whitelist.`)
  } catch (_) {}
})

// ── Cookie management ────────────────────────────────────────────────

const cookiePanel = document.getElementById('cookie-panel')
const cookieBody = document.getElementById('cookie-body')

async function refreshCookieCount () {
  try {
    const res = await fetch('http://127.0.0.1:9300/cookies')
    const data = await res.json()
    const el = document.getElementById('set-cookie-count')
    if (el) el.textContent = `${data.total} cookies`
  } catch (_) {}
}

async function openCookieManager () {
  cookiePanel.style.display = 'flex'
  try {
    const res = await fetch('http://127.0.0.1:9300/cookies')
    const data = await res.json()
    cookieBody.innerHTML = ''
    const domains = Object.keys(data.cookies).sort()
    if (domains.length === 0) {
      cookieBody.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:40px;font-size:12px;">No cookies stored.</div>'
      return
    }
    domains.forEach(domain => {
      const count = data.cookies[domain].length
      const row = document.createElement('div')
      row.className = 'cookie-domain'
      const nameEl = document.createElement('span')
      nameEl.className = 'cookie-domain-name'
      nameEl.textContent = domain
      const countEl = document.createElement('span')
      countEl.className = 'cookie-domain-count'
      countEl.textContent = `${count} cookie${count !== 1 ? 's' : ''}`
      const delBtn = document.createElement('button')
      delBtn.className = 'cookie-domain-delete'
      delBtn.textContent = 'Delete'
      delBtn.addEventListener('click', async (e) => {
        e.stopPropagation()
        await fetch('http://127.0.0.1:9300/cookies', {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ domain })
        })
        row.remove()
        refreshCookieCount()
      })
      row.appendChild(nameEl)
      row.appendChild(countEl)
      row.appendChild(delBtn)
      cookieBody.appendChild(row)
    })
  } catch (_) {
    cookieBody.innerHTML = '<div style="text-align:center;color:var(--red);padding:40px;">Failed to load cookies.</div>'
  }
}

document.getElementById('set-view-cookies')?.addEventListener('click', openCookieManager)
document.getElementById('cookie-close')?.addEventListener('click', () => { cookiePanel.style.display = 'none' })
cookiePanel?.addEventListener('click', (e) => { if (e.target === cookiePanel) cookiePanel.style.display = 'none' })

document.getElementById('set-clear-cookies')?.addEventListener('click', async () => {
  try {
    const res = await fetch('http://127.0.0.1:9300/cookies')
    const data = await res.json()
    for (const domain of Object.keys(data.cookies)) {
      await fetch('http://127.0.0.1:9300/cookies', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ domain })
      })
    }
    addSystemMsg('All cookies cleared.')
    refreshCookieCount()
  } catch (_) {}
})

// Third-party cookie blocking
const block3pCheckbox = document.getElementById('set-block-3p-cookies')
if (block3pCheckbox) {
  fetch('http://127.0.0.1:9300/cookies/block-third-party').then(r => r.json()).then(d => {
    block3pCheckbox.checked = !!d.blocking
  }).catch(() => {})
  block3pCheckbox.addEventListener('change', async () => {
    try {
      await fetch('http://127.0.0.1:9300/cookies/block-third-party', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: block3pCheckbox.checked })
      })
      addSystemMsg(block3pCheckbox.checked ? 'Third-party cookies blocked.' : 'Third-party cookies allowed.')
    } catch (_) {}
  })
}

// Refresh stats when settings opens
const _origOpenSettings = openSettingsPanel
openSettingsPanel = function () {
  _origOpenSettings()
  refreshAdblockStats()
  refreshCookieCount()
}

// ── Downloads manager ────────────────────────────────────────────────

const KEY_DOWNLOADS = 'yamil_downloads'
const downloadsPanel = document.getElementById('downloads-panel')
const dlBody = document.getElementById('dl-body')
let downloads = []
const liveDownloads = new Map() // id → { filename, received, totalBytes, state, paused }

function getDownloads () {
  try { return JSON.parse(pGet(KEY_DOWNLOADS) || '[]') } catch (_) { return [] }
}

function saveDownloads () {
  try { pSet(KEY_DOWNLOADS, JSON.stringify(downloads.slice(0, 100))) } catch (_) {}
}

function openDownloadsPanel () {
  downloadsPanel.style.display = 'flex'
  renderDownloads()
}

function closeDownloadsPanel () {
  downloadsPanel.style.display = 'none'
}

function renderDownloads () {
  dlBody.innerHTML = ''
  // Show live downloads first
  for (const [id, dl] of liveDownloads) {
    const row = document.createElement('div')
    row.className = 'dl-item'
    row.dataset.dlId = id
    const nameEl = document.createElement('span')
    nameEl.className = 'dl-item-name'
    nameEl.textContent = dl.filename
    const progressWrap = document.createElement('div')
    progressWrap.className = 'dl-progress-wrap'
    const bar = document.createElement('div')
    bar.className = 'dl-progress-bar'
    const pct = dl.totalBytes > 0 ? Math.round((dl.received / dl.totalBytes) * 100) : 0
    bar.style.width = pct + '%'
    progressWrap.appendChild(bar)
    const info = document.createElement('span')
    info.className = 'dl-item-size'
    info.textContent = `${formatBytes(dl.received)} / ${dl.totalBytes > 0 ? formatBytes(dl.totalBytes) : '?'} (${pct}%)`
    const actions = document.createElement('span')
    actions.className = 'dl-actions'
    if (dl.paused) {
      const resumeBtn = document.createElement('button')
      resumeBtn.textContent = '▶'
      resumeBtn.title = 'Resume'
      resumeBtn.addEventListener('click', () => window.YAMIL_IPC?.downloadResume(id))
      actions.appendChild(resumeBtn)
    } else {
      const pauseBtn = document.createElement('button')
      pauseBtn.textContent = '⏸'
      pauseBtn.title = 'Pause'
      pauseBtn.addEventListener('click', () => window.YAMIL_IPC?.downloadPause(id))
      actions.appendChild(pauseBtn)
    }
    const cancelBtn = document.createElement('button')
    cancelBtn.textContent = '✕'
    cancelBtn.title = 'Cancel'
    cancelBtn.addEventListener('click', () => window.YAMIL_IPC?.downloadCancel(id))
    actions.appendChild(cancelBtn)
    row.appendChild(nameEl)
    row.appendChild(progressWrap)
    row.appendChild(info)
    row.appendChild(actions)
    dlBody.appendChild(row)
  }
  // Show completed/historical downloads
  const items = downloads.length > 0 ? downloads : getDownloads()
  if (items.length === 0 && liveDownloads.size === 0) {
    dlBody.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:40px;font-size:12px;">No downloads yet.</div>'
    return
  }
  items.forEach(dl => {
    const row = document.createElement('div')
    row.className = 'dl-item'
    const nameEl = document.createElement('span')
    nameEl.className = 'dl-item-name'
    nameEl.textContent = dl.filename
    const sizeEl = document.createElement('span')
    sizeEl.className = 'dl-item-size'
    sizeEl.textContent = dl.size ? formatBytes(dl.size) : ''
    const statusEl = document.createElement('span')
    statusEl.className = 'dl-item-status ' + (dl.state === 'completed' ? 'complete' : dl.state === 'cancelled' ? 'failed' : '')
    statusEl.textContent = dl.state || 'unknown'
    row.appendChild(nameEl)
    row.appendChild(sizeEl)
    row.appendChild(statusEl)
    dlBody.appendChild(row)
  })
}

function formatBytes (b) {
  if (b < 1024) return b + ' B'
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB'
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB'
  return (b / 1073741824).toFixed(1) + ' GB'
}

// Wire IPC download events
window.YAMIL_IPC?.onDownloadStarted((d) => {
  liveDownloads.set(d.id, { filename: d.filename, received: 0, totalBytes: d.totalBytes, state: 'progressing', paused: false })
  // Show a non-intrusive status bar indicator instead of auto-opening the panel
  statusLoad.textContent = `↓ ${d.filename}`
})

window.YAMIL_IPC?.onDownloadProgress((d) => {
  const dl = liveDownloads.get(d.id)
  if (dl) {
    dl.received = d.received
    dl.totalBytes = d.totalBytes
    dl.paused = d.paused
    dl.state = d.state
  }
  if (downloadsPanel.style.display !== 'none') renderDownloads()
  // Update status bar
  statusLoad.textContent = dl ? `↓ ${dl.filename} ${dl.totalBytes > 0 ? Math.round((dl.received / dl.totalBytes) * 100) : 0}%` : ''
})

window.YAMIL_IPC?.onDownloadDone((d) => {
  liveDownloads.delete(d.id)
  downloads.unshift({ filename: d.filename, size: d.totalBytes, state: d.state, savePath: d.savePath, date: Date.now() })
  saveDownloads()
  statusLoad.textContent = ''
  if (downloadsPanel.style.display !== 'none') renderDownloads()
})

document.getElementById('dl-close').addEventListener('click', closeDownloadsPanel)
downloadsPanel.addEventListener('click', (e) => { if (e.target === downloadsPanel) closeDownloadsPanel() })

// ── Tab pinning ──────────────────────────────────────────────────────

function togglePinTab (tabId) {
  const tab = tabs.find(t => t.id === tabId)
  if (!tab) return
  tab.pinned = !tab.pinned
  tab.tabEl.classList.toggle('pinned', tab.pinned)
  // Move pinned tabs to the left
  reorderPinnedTabs()
  saveTabs()
}

function reorderPinnedTabs () {
  const pinned = tabs.filter(t => t.pinned)
  const unpinned = tabs.filter(t => !t.pinned)
  // Re-insert pinned tabs first in DOM
  pinned.forEach(t => tabsList.insertBefore(t.tabEl, tabsList.firstChild))
}

// ── Tab groups ───────────────────────────────────────────────────────

const GROUP_COLORS = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316']

function getTabGroup (tab) {
  return tab.group || null
}

function setTabGroup (tabId, groupName, color) {
  const tab = tabs.find(t => t.id === tabId)
  if (!tab) return
  if (!groupName) {
    delete tab.group
    delete tab.groupColor
    tab.tabEl.classList.remove('grouped')
    tab.tabEl.style.removeProperty('--group-color')
  } else {
    tab.group = groupName
    tab.groupColor = color || GROUP_COLORS[Math.abs(hashStr(groupName)) % GROUP_COLORS.length]
    tab.tabEl.classList.add('grouped')
    tab.tabEl.style.setProperty('--group-color', tab.groupColor)
  }
  saveTabs()
  renderTabGroups()
}

function hashStr (s) {
  let h = 0
  for (let i = 0; i < s.length; i++) h = s.charCodeAt(i) + ((h << 5) - h)
  return h
}

function renderTabGroups () {
  // Remove existing group labels
  tabsList.querySelectorAll('.tab-group-label').forEach(el => el.remove())

  // Find unique groups
  const groups = {}
  tabs.forEach(t => {
    if (t.group) {
      if (!groups[t.group]) groups[t.group] = { color: t.groupColor || GROUP_COLORS[0], tabs: [] }
      groups[t.group].tabs.push(t)
    }
  })

  // Insert group labels before first tab of each group
  for (const [name, g] of Object.entries(groups)) {
    const firstTab = g.tabs[0]
    if (firstTab && firstTab.tabEl) {
      const label = document.createElement('div')
      label.className = 'tab-group-label'
      label.style.background = g.color
      label.textContent = name
      label.title = `Group: ${name} (${g.tabs.length} tabs)`
      label.addEventListener('click', () => {
        // Toggle collapse
        g.tabs.forEach(t => {
          if (t.tabEl.style.display === 'none') {
            t.tabEl.style.display = ''
          } else if (t.id !== activeTabId) {
            t.tabEl.style.display = 'none'
          }
        })
      })
      tabsList.insertBefore(label, firstTab.tabEl)
    }
  }
}

// ── Streaming AI response helper ─────────────────────────────────────

async function sendChatStreaming (text, pageContext) {
  const memory = getAiMemory()
  const memCtx = memory.length > 0 ? '\n\n[User memories: ' + memory.map(m => m.fact).join('; ') + ']' : ''

  const msgDiv = document.createElement('div')
  msgDiv.className = 'chat-msg ai'
  msgDiv.dataset.role = 'ai'
  chatLog.appendChild(msgDiv)
  chatLog.scrollTop = chatLog.scrollHeight

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text + memCtx, pageContext, stream: true }),
    })

    if (res.headers.get('content-type')?.includes('text/event-stream')) {
      // SSE streaming
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6)
            if (data === '[DONE]') break
            try {
              const parsed = JSON.parse(data)
              const token = parsed.token || parsed.content || parsed.delta?.content || ''
              if (token) {
                msgDiv.textContent += token
                chatLog.scrollTop = chatLog.scrollHeight
              }
            } catch (_) {
              if (data.trim()) {
                msgDiv.textContent += data
                chatLog.scrollTop = chatLog.scrollHeight
              }
            }
          }
        }
      }
    } else {
      // Non-streaming fallback
      const data = await res.json()
      const reply = data.response || data.message || JSON.stringify(data)
      msgDiv.textContent = reply

      if (data.navigatedUrl) navigateWebview(data.navigatedUrl)
    }
  } catch (e) {
    msgDiv.className = 'chat-msg error'
    msgDiv.textContent = 'AI request failed: ' + e.message
  }

  saveChatHistory()
}

// ── Text-to-Speech (output only) ─────────────────────────────────────
function speakText (text) {
  if (!window.speechSynthesis) return
  const utt = new SpeechSynthesisUtterance(text.slice(0, 500))
  utt.rate = 1.1
  utt.pitch = 1
  window.speechSynthesis.speak(utt)
}

// ── AI Skills ─────────────────────────────────────────────────────────

const DEFAULT_SKILLS = [
  { id: 'summarize', name: 'Summarize', icon: '\u2211', prompt: 'Summarize this page concisely in 3-5 bullet points.' },
  { id: 'extract', name: 'Extract', icon: '\u2913', prompt: 'Extract all key data points from this page as a structured JSON object.' },
  { id: 'translate', name: 'Translate', icon: '\u2300', prompt: 'Translate this page content to Spanish.', svgClass: 'skill-icon-translate' },
  { id: 'explain', name: 'Explain', icon: '\u2604', prompt: 'Explain this page content simply, as if to a non-technical person.', svgClass: 'skill-icon-explain' },
]

function getCustomSkills () {
  try { return JSON.parse(pGet(KEY_AI_SKILLS) || '[]') } catch (_) { return [] }
}

function saveCustomSkills (arr) {
  try { pSet(KEY_AI_SKILLS, JSON.stringify(arr)) } catch (_) {}
}

function getAllSkills () {
  return [...DEFAULT_SKILLS, ...getCustomSkills()]
}

function renderSkillsTray () {
  const tray = document.getElementById('skills-tray')
  if (!tray) return
  tray.innerHTML = ''

  getAllSkills().forEach(skill => {
    const btn = document.createElement('button')
    btn.className = 'skill-btn' + (skill.custom ? ' custom' : '')
    btn.dataset.skill = skill.id
    btn.title = skill.prompt
    if (skill.svgClass) {
      const iconSpan = document.createElement('span')
      iconSpan.className = 'skill-icon ' + skill.svgClass
      btn.appendChild(iconSpan)
      btn.appendChild(document.createTextNode(' ' + skill.name))
    } else {
      btn.textContent = (skill.icon || '\u26A1') + ' ' + skill.name
    }
    btn.addEventListener('click', () => runSkill(skill))
    // Right-click to delete custom skills
    if (skill.custom) {
      btn.addEventListener('contextmenu', (e) => {
        e.preventDefault()
        if (confirm(`Delete skill "${skill.name}"?`)) {
          const cs = getCustomSkills().filter(s => s.id !== skill.id)
          saveCustomSkills(cs)
          renderSkillsTray()
        }
      })
    }
    tray.appendChild(btn)
  })

  // Add skill button
  const addBtn = document.createElement('button')
  addBtn.id = 'btn-add-skill'
  addBtn.title = 'Create custom skill'
  addBtn.textContent = '+'
  addBtn.addEventListener('click', openSkillEditor)
  tray.appendChild(addBtn)
}

async function runSkill (skill) {
  if (!aiEndpoint) { addSystemMsg('No AI endpoint configured.'); return }
  if (isAiBlockedForCurrentPage()) { addSystemMsg('AI is blocked for this page.'); return }

  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab) { addSystemMsg('No active tab.'); return }

  addSystemMsg(`Running skill: ${skill.name}...`)

  let pageText = ''
  if (tab.type === 'stealth' && tab.sessionId) {
    try {
      const r = await fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/evaluate`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ script: 'document.body.innerText.slice(0, 8000)' }),
      })
      const d = await r.json()
      pageText = d.result || ''
    } catch (_) {}
  } else if (tab.webview) {
    try { pageText = await tab.webview.executeJavaScript('document.body.innerText.slice(0, 8000)') } catch (_) {}
  }

  if (!pageText) { addSystemMsg('Could not extract page text.'); return }

  try {
    const r = await fetch(aiEndpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: skill.prompt + '\n\n' + pageText,
        pageContext: { url: tab.url, title: tab.title },
      }),
    })
    const d = await r.json()
    addAiMsg(d.response || d.message || 'No response.')
  } catch (e) { addErrorMsg('Skill failed: ' + e.message) }
}

function openSkillEditor () {
  const existing = document.getElementById('skills-editor')
  if (existing) existing.remove()

  const overlay = document.createElement('div')
  overlay.id = 'skills-editor'
  overlay.innerHTML = `
    <div id="skills-editor-dialog">
      <h3>Create Custom Skill</h3>
      <label>Name</label>
      <input id="skill-name" type="text" placeholder="e.g. Find Bugs" maxlength="30">
      <label>Icon (emoji or symbol)</label>
      <input id="skill-icon" type="text" placeholder="e.g. 🐛" maxlength="4">
      <label>Prompt Template</label>
      <textarea id="skill-prompt" rows="4" placeholder="e.g. Find potential bugs and security issues in this code..."></textarea>
      <div class="skills-editor-actions">
        <button class="btn-cancel" id="skill-cancel">Cancel</button>
        <button class="btn-save" id="skill-save">Save Skill</button>
      </div>
    </div>
  `
  document.body.appendChild(overlay)

  overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove() })
  document.getElementById('skill-cancel').addEventListener('click', () => overlay.remove())
  document.getElementById('skill-save').addEventListener('click', () => {
    const name = document.getElementById('skill-name').value.trim()
    const icon = document.getElementById('skill-icon').value.trim() || '\u26A1'
    const prompt = document.getElementById('skill-prompt').value.trim()
    if (!name || !prompt) { addSystemMsg('Skill needs a name and prompt.'); return }
    const cs = getCustomSkills()
    cs.push({ id: 'custom_' + Date.now().toString(36), name, icon, prompt, custom: true })
    saveCustomSkills(cs)
    renderSkillsTray()
    overlay.remove()
    addSystemMsg(`Skill "${name}" created!`)
  })
  document.getElementById('skill-name').focus()
}

// ── AI Page Visibility / Privacy ──────────────────────────────────────

const btnAiEye = document.getElementById('btn-ai-eye')

function getBlockedDomains () {
  try { return JSON.parse(pGet(KEY_AI_BLOCKED) || '[]') } catch (_) { return [] }
}

function saveBlockedDomains (arr) {
  try { pSet(KEY_AI_BLOCKED, JSON.stringify(arr)) } catch (_) {}
}

function getCurrentDomain () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab || !tab.url) return null
  try { return new URL(tab.url).hostname } catch (_) { return null }
}

function isAiBlockedForCurrentPage () {
  const domain = getCurrentDomain()
  if (!domain) return false
  return getBlockedDomains().includes(domain)
}

function toggleAiVisibility () {
  const domain = getCurrentDomain()
  if (!domain) { addSystemMsg('No active page.'); return }
  const blocked = getBlockedDomains()
  const idx = blocked.indexOf(domain)
  if (idx >= 0) {
    blocked.splice(idx, 1)
    addSystemMsg(`AI can now see pages on ${domain}`)
  } else {
    blocked.push(domain)
    addSystemMsg(`AI blocked from seeing pages on ${domain}`)
  }
  saveBlockedDomains(blocked)
  updateAiEyeIcon()
}

function updateAiEyeIcon () {
  if (isAiBlockedForCurrentPage()) {
    btnAiEye.classList.remove('ai-visible')
    btnAiEye.classList.add('ai-blocked')
    btnAiEye.title = 'AI is blocked for this site (click to allow)'
  } else {
    btnAiEye.classList.remove('ai-blocked')
    btnAiEye.classList.add('ai-visible')
    btnAiEye.title = 'AI can see this page (click to block)'
  }
}

btnAiEye.addEventListener('click', toggleAiVisibility)

// ── Cross-Tab Context ─────────────────────────────────────────────────

async function getTabContext (tabId) {
  const tab = tabs.find(t => t.id === tabId)
  if (!tab) return null
  try {
    if (tab.type === 'stealth' && tab.sessionId) {
      const r = await fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/evaluate`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ script: '({ url: location.href, title: document.title, text: document.body.innerText.slice(0, 3000) })' }),
      })
      const d = await r.json()
      return d.result || null
    } else if (tab.webview) {
      return await tab.webview.executeJavaScript(`({
        url: location.href, title: document.title, text: document.body.innerText.slice(0, 3000),
      })`)
    }
  } catch (_) {}
  return null
}

async function resolveTabReferences (text) {
  // Match @tab:N or @all-tabs
  const allTabsMatch = text.match(/@all[_-]?tabs/i)
  const tabRefMatches = [...text.matchAll(/@tab:(\d+)/gi)]

  if (!allTabsMatch && tabRefMatches.length === 0) return { text, contexts: [] }

  const contexts = []

  if (allTabsMatch) {
    for (let i = 0; i < Math.min(tabs.length, 5); i++) {
      const ctx = await getTabContext(tabs[i].id)
      if (ctx) contexts.push({ tabIndex: i + 1, ...ctx })
    }
    text = text.replace(/@all[_-]?tabs/gi, `[Content from ${contexts.length} tabs included below]`)
  }

  for (const match of tabRefMatches) {
    const idx = parseInt(match[1]) - 1
    if (idx >= 0 && idx < tabs.length) {
      const ctx = await getTabContext(tabs[idx].id)
      if (ctx) {
        contexts.push({ tabIndex: idx + 1, ...ctx })
        text = text.replace(match[0], `[Tab ${idx + 1}: "${ctx.title}"]`)
      }
    }
  }

  return { text, contexts }
}

// ── Background Agent Task Queue ───────────────────────────────────────

const KEY_TASKS = 'yamil_agent_tasks'
const taskQueue = document.getElementById('task-queue')
const tqList = document.getElementById('tq-list')
const tqCount = document.getElementById('tq-count')
const bgCheck = document.getElementById('bg-check')
let agentTasks = []
let taskIdCounter2 = 0

function loadAgentTasks () {
  try { agentTasks = JSON.parse(pGet(KEY_TASKS) || '[]') } catch (_) { agentTasks = [] }
  taskIdCounter2 = agentTasks.reduce((max, t) => Math.max(max, t.id || 0), 0)
}

function saveAgentTasks () {
  try { pSet(KEY_TASKS, JSON.stringify(agentTasks.slice(-50))) } catch (_) {}
}

function createAgentTask (goal) {
  const task = {
    id: ++taskIdCounter2,
    goal,
    status: 'queued',   // queued | running | done | failed
    steps: [],
    progress: 0,
    createdAt: Date.now(),
    result: null,
  }
  agentTasks.push(task)
  saveAgentTasks()
  renderTaskQueue()
  return task
}

function updateAgentTask (taskId, updates) {
  const task = agentTasks.find(t => t.id === taskId)
  if (!task) return
  Object.assign(task, updates)
  saveAgentTasks()
  renderTaskQueue()
}

function renderTaskQueue () {
  tqList.innerHTML = ''
  const active = agentTasks.filter(t => t.status === 'running' || t.status === 'queued')
  const recent = agentTasks.filter(t => t.status === 'done' || t.status === 'failed').slice(-5)
  const show = [...active, ...recent.reverse()]

  tqCount.textContent = active.length
  tqCount.classList.toggle('active', active.length > 0)

  if (show.length === 0) {
    tqList.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px;font-size:11px;">No background tasks. Check "BG" next to send to run tasks in background.</div>'
    return
  }

  show.forEach(task => {
    const el = document.createElement('div')
    el.className = 'tq-item ' + task.status

    // Header row with goal + elapsed time
    const elapsed = task.status === 'running' ? formatElapsed(Date.now() - task.createdAt) : ''
    el.innerHTML = `
      <div class="tq-item-header">
        <div class="tq-item-goal" title="${task.goal}">${task.goal}</div>
        ${elapsed ? `<span class="tq-item-elapsed">${elapsed}</span>` : ''}
      </div>
      <div class="tq-item-status">${task.status === 'running' ? 'Running...' : task.status === 'queued' ? 'Queued' : task.status === 'done' ? 'Completed' : 'Failed'}</div>
      ${task.status === 'running' ? `<div class="tq-item-progress"><div class="tq-item-progress-fill" style="width:${task.progress}%"></div></div>` : ''}
    `

    // Live screenshot preview for running tasks
    if (task.status === 'running' && task.screenshot) {
      const previewEl = document.createElement('div')
      previewEl.className = 'tq-item-preview'
      const img = document.createElement('img')
      img.src = task.screenshot
      img.className = 'tq-preview-img'
      img.title = 'Live agent view'
      previewEl.appendChild(img)
      el.appendChild(previewEl)
    }

    // Step plan with numbered steps
    if (task.plan && task.plan.length > 0) {
      const planEl = document.createElement('div')
      planEl.className = 'tq-item-plan'
      task.plan.forEach((step, i) => {
        const stepEl = document.createElement('div')
        const isActive = i === task.currentPlanStep
        const isDone = i < task.currentPlanStep
        stepEl.className = 'tq-plan-step' + (isActive ? ' active' : '') + (isDone ? ' done' : '')
        stepEl.innerHTML = `<span class="tq-plan-num">${isDone ? '✓' : i + 1}</span> ${step}`
        planEl.appendChild(stepEl)
      })
      el.appendChild(planEl)
    }

    // Live execution steps (last 3)
    if (task.steps.length > 0) {
      const stepsEl = document.createElement('div')
      stepsEl.className = 'tq-item-steps'
      task.steps.slice(-3).forEach((s, i, arr) => {
        const stepEl = document.createElement('div')
        stepEl.className = 'tq-item-step' + (i === arr.length - 1 && task.status === 'running' ? ' current' : '')
        stepEl.textContent = (i === arr.length - 1 && task.status === 'running' ? '▸ ' : '✓ ') + s
        stepsEl.appendChild(stepEl)
      })
      el.appendChild(stepsEl)
    }

    // Result actions
    if (task.status === 'done' && task.result) {
      const resultBtn = document.createElement('div')
      resultBtn.className = 'tq-item-actions'
      const btn = document.createElement('button')
      btn.textContent = 'Show Result'
      btn.addEventListener('click', () => addAiMsg(`[Task "${task.goal}"] ${task.result}`))
      resultBtn.appendChild(btn)
      el.appendChild(resultBtn)
    }

    // Cancel action
    if (task.status === 'running' || task.status === 'queued') {
      const actionsEl = document.createElement('div')
      actionsEl.className = 'tq-item-actions'
      const cancelBtn = document.createElement('button')
      cancelBtn.textContent = 'Cancel'
      cancelBtn.addEventListener('click', () => {
        updateAgentTask(task.id, { status: 'failed', result: 'Cancelled by user' })
        if (task._cancelToken) task._cancelToken.cancelled = true
      })
      actionsEl.appendChild(cancelBtn)
      el.appendChild(actionsEl)
    }

    tqList.appendChild(el)
  })
}

function formatElapsed (ms) {
  if (ms < 1000) return '<1s'
  const s = Math.floor(ms / 1000)
  if (s < 60) return s + 's'
  return Math.floor(s / 60) + 'm ' + (s % 60) + 's'
}

// Toggle task queue visibility
document.getElementById('btn-tasks-toggle').addEventListener('click', () => {
  const visible = taskQueue.style.display !== 'none'
  taskQueue.style.display = visible ? 'none' : 'block'
  if (!visible) renderTaskQueue()
})

// Capture a screenshot for live task preview
async function captureTaskScreenshot () {
  const tab = tabs.find(t => t.id === activeTabId)
  if (!tab) return null
  try {
    if (tab.type === 'stealth' && tab.sessionId) {
      const r = await fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/screenshot`)
      if (r.ok) {
        const buf = await r.arrayBuffer()
        return 'data:image/png;base64,' + btoa(String.fromCharCode(...new Uint8Array(buf)))
      }
    } else if (tab.webview) {
      const nativeImg = await tab.webview.capturePage()
      if (nativeImg) return nativeImg.toDataURL()
    }
  } catch (_) {}
  return null
}

// Run a task in the background with live progress view
async function runBackgroundTask (task) {
  const cancelToken = { cancelled: false }
  task._cancelToken = cancelToken

  updateAgentTask(task.id, { status: 'running', progress: 5 })
  updateAgentTask(task.id, { steps: ['Analyzing task...'] })

  // Generate a plan using AI (if available)
  if (aiEndpoint) {
    try {
      const planRes = await fetch(aiEndpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: `Break this task into 3-5 concise action steps (one line each, no numbering). Just list the steps, nothing else.\n\nTask: ${task.goal}`,
        }),
      })
      const planData = await planRes.json()
      const planText = planData.response || planData.message || ''
      const planSteps = planText.split('\n').map(l => l.replace(/^\d+[\.\)]\s*/, '').replace(/^[-•]\s*/, '').trim()).filter(l => l.length > 5).slice(0, 5)
      if (planSteps.length > 0) {
        updateAgentTask(task.id, { plan: planSteps, currentPlanStep: 0, progress: 10 })
      }
    } catch (_) {}
  }

  if (cancelToken.cancelled) return

  updateAgentTask(task.id, { steps: ['Preparing request...'], progress: 15 })

  // Start live screenshot polling
  let screenshotInterval = null
  screenshotInterval = setInterval(async () => {
    if (cancelToken.cancelled || task.status !== 'running') {
      clearInterval(screenshotInterval)
      return
    }
    const screenshot = await captureTaskScreenshot()
    if (screenshot) {
      task.screenshot = screenshot
      renderTaskQueue()
    }
  }, 3000) // Update every 3 seconds

  // Capture initial screenshot
  const initialScreenshot = await captureTaskScreenshot()
  if (initialScreenshot) {
    task.screenshot = initialScreenshot
    renderTaskQueue()
  }

  const tab = tabs.find(t => t.id === activeTabId)
  let pageContext = {}
  if (!isAiBlockedForCurrentPage()) {
    if (tab && tab.type === 'stealth' && tab.sessionId) {
      try {
        const r = await fetch(`${BROWSER_SERVICE}/sessions/${tab.sessionId}/evaluate`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ script: '({ url: location.href, title: document.title, text: document.body.innerText.slice(0, 4000) })' }),
        })
        const d = await r.json()
        pageContext = d.result || {}
      } catch (_) {}
    } else if (tab && tab.webview) {
      try {
        pageContext = await tab.webview.executeJavaScript(`({
          url: location.href, title: document.title, text: document.body.innerText.slice(0, 4000),
        })`)
      } catch (_) {}
    }
  }

  if (cancelToken.cancelled) { clearInterval(screenshotInterval); return }

  updateAgentTask(task.id, { steps: [...task.steps, 'Sending to AI...'], progress: 30, currentPlanStep: 1 })

  const memory = getAiMemory()
  const memCtx = memory.length > 0 ? '\n\n[User memories: ' + memory.map(m => m.fact).join('; ') + ']' : ''

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: task.goal + memCtx,
        pageContext,
        background: true,
      }),
    })

    if (cancelToken.cancelled) { clearInterval(screenshotInterval); return }

    updateAgentTask(task.id, { steps: [...task.steps, 'Processing response...'], progress: 70, currentPlanStep: Math.max((task.plan || []).length - 2, 2) })

    const data = await res.json()
    const reply = data.response || data.message || JSON.stringify(data)

    if (data.navigatedUrl) navigateWebview(data.navigatedUrl)

    clearInterval(screenshotInterval)
    // Final screenshot
    const finalScreenshot = await captureTaskScreenshot()

    updateAgentTask(task.id, {
      status: 'done',
      progress: 100,
      result: reply,
      steps: [...task.steps, 'Complete'],
      currentPlanStep: (task.plan || []).length,
      screenshot: finalScreenshot || task.screenshot,
    })

    // Desktop notification
    if (Notification.permission === 'granted') {
      new Notification('YAMIL Task Complete', { body: task.goal, icon: '../assets/yamil-logo.png' })
    }
    addSystemMsg(`Background task completed: "${task.goal}"`)
  } catch (e) {
    clearInterval(screenshotInterval)
    updateAgentTask(task.id, {
      status: 'failed',
      result: e.message,
      steps: [...task.steps, 'Failed: ' + e.message],
    })
    addSystemMsg(`Background task failed: "${task.goal}" — ${e.message}`)
  }
}

// Expose for window._yamil
window._yamil.agentTasks = {
  create: createAgentTask,
  list: () => agentTasks,
  get: (id) => agentTasks.find(t => t.id === id),
  cancel: (id) => updateAgentTask(id, { status: 'failed', result: 'Cancelled' }),
}

// ── Init ──────────────────────────────────────────────────────────────

loadAgentTasks()
renderTaskQueue()
renderSkillsTray()
loadChatHistory()

// Restore tabs or create initial tab
if (!loadTabs()) {
  const lastUrl = pGet(KEY_LAST_URL) || startUrl
  createTab(lastUrl, true)
}

// Restore sidebar state
const sidebarWasOpen = localStorage.getItem(KEY_SIDEBAR_OPEN) !== '0'
setSidebarOpen(sidebarWasOpen)

// Restore bookmark bar state
const bmBarVisible = localStorage.getItem(KEY_BMBAR_VIS) === '1'
setBookmarkBarVisible(bmBarVisible)
updateBookmarkStar()

// Apply saved settings
;(function applySettings () {
  const s = getSettings()
  if (s.sidebarPos === 'left') {
    const m = document.getElementById('main')
    const sb = document.getElementById('sidebar')
    m.style.flexDirection = 'row-reverse'
    sb.style.borderLeft = 'none'
    sb.style.borderRight = '1px solid var(--border)'
  }
  renderTabGroups()
  updateAiEyeIcon()
})()

downloads = getDownloads()

// ── Profile switcher ─────────────────────────────────────────────────

function initProfileSwitcher () {
  const select = document.getElementById('set-profile')
  const addBtn = document.getElementById('set-profile-add')
  const delBtn = document.getElementById('set-profile-delete')
  if (!select) return

  function renderProfiles () {
    const profiles = getProfiles()
    select.innerHTML = ''
    profiles.forEach(p => {
      const opt = document.createElement('option')
      opt.value = p
      opt.textContent = p
      if (p === currentProfile) opt.selected = true
      select.appendChild(opt)
    })
  }

  renderProfiles()

  select.addEventListener('change', () => {
    currentProfile = select.value
    localStorage.setItem(KEY_PROFILE, currentProfile)
    addSystemMsg(`Switched to profile: ${currentProfile}. New tabs will use this profile.`)
  })

  addBtn?.addEventListener('click', () => {
    const name = prompt('Profile name:')
    if (!name || !name.trim()) return
    const profiles = getProfiles()
    if (profiles.includes(name.trim())) { addSystemMsg('Profile already exists.'); return }
    profiles.push(name.trim())
    saveProfiles(profiles)
    currentProfile = name.trim()
    localStorage.setItem(KEY_PROFILE, currentProfile)
    renderProfiles()
    addSystemMsg(`Profile "${currentProfile}" created and activated.`)
  })

  delBtn?.addEventListener('click', () => {
    if (currentProfile === 'Default') { addSystemMsg('Cannot delete the Default profile.'); return }
    const profiles = getProfiles().filter(p => p !== currentProfile)
    saveProfiles(profiles)
    currentProfile = 'Default'
    localStorage.setItem(KEY_PROFILE, currentProfile)
    renderProfiles()
    addSystemMsg('Profile deleted. Switched to Default.')
  })
}

initProfileSwitcher()

// ── PWA install detection ────────────────────────────────────────────

function checkPWA (tab) {
  if (!tab || !tab.webview || tab.type !== 'yamil') return
  try {
    tab.webview.executeJavaScript(`(function() {
      var link = document.querySelector('link[rel="manifest"]');
      return link ? link.href : null;
    })()`)
    .then(manifestUrl => {
      if (manifestUrl) {
        const installBtn = document.getElementById('btn-pwa-install')
        if (installBtn) {
          installBtn.style.display = 'inline-flex'
          installBtn.dataset.manifest = manifestUrl
          installBtn.dataset.tabId = tab.id
        }
      } else {
        const installBtn = document.getElementById('btn-pwa-install')
        if (installBtn) installBtn.style.display = 'none'
      }
    })
    .catch(() => {})
  } catch {}
}

document.getElementById('btn-pwa-install')?.addEventListener('click', async () => {
  const btn = document.getElementById('btn-pwa-install')
  const manifestUrl = btn?.dataset.manifest
  if (!manifestUrl) return
  try {
    const tab = tabs.find(t => t.id === Number(btn.dataset.tabId))
    if (!tab || !tab.webview) return
    const manifest = await (await fetch(manifestUrl)).json()
    const name = manifest.name || manifest.short_name || 'App'
    const startUrl = manifest.start_url || tab.url
    // Create a bookmark as "installed PWA"
    let bm = JSON.parse(pGet(KEY_BOOKMARKS) || '[]')
    if (!bm.some(b => b.url === startUrl && b.pwa)) {
      bm.push({ url: startUrl, title: `[PWA] ${name}`, date: Date.now(), pwa: true, icon: manifest.icons?.[0]?.src || '' })
      pSet(KEY_BOOKMARKS, JSON.stringify(bm))
      renderBookmarkBar()
    }
    addSystemMsg(`"${name}" added as PWA bookmark.`)
    btn.style.display = 'none'
  } catch (e) {
    addSystemMsg('PWA install failed: ' + e.message)
  }
})

// Check for PWA on page load — wire into existing webview events
const origSwitchTab = switchTab
// Hook into tab switch to check PWA
const _origDidFinishLoadHandlers = new Map()

// ── API key management ───────────────────────────────────────────────

async function refreshApiKeys () {
  try {
    const res = await fetch(`${BROWSER_SERVICE}/api-keys`)
    const data = await res.json()
    const listEl = document.getElementById('api-key-list')
    const statusEl = document.getElementById('api-key-status')
    if (statusEl) statusEl.textContent = data.authEnabled ? 'Enabled (remote access requires key)' : 'Disabled (no keys configured)'
    if (!listEl) return
    listEl.innerHTML = ''
    if (data.keys.length === 0) {
      listEl.innerHTML = '<div style="font-size:11px;color:var(--text-muted);padding:4px 0;">No API keys. Remote access is unrestricted.</div>'
      return
    }
    data.keys.forEach(k => {
      const row = document.createElement('div')
      row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:4px 0;font-size:11px;'
      row.innerHTML = `<span style="flex:1">${k.name} <code>${k.prefix}</code></span>`
      const del = document.createElement('button')
      del.className = 'settings-btn'
      del.textContent = 'Revoke'
      del.style.fontSize = '10px'
      del.addEventListener('click', async () => {
        await fetch(`${BROWSER_SERVICE}/api-keys/${k.id}`, { method: 'DELETE' })
        refreshApiKeys()
      })
      row.appendChild(del)
      listEl.appendChild(row)
    })
  } catch {}
}

document.getElementById('set-create-api-key')?.addEventListener('click', async () => {
  const name = prompt('API key name:', 'my-key')
  if (!name) return
  try {
    const res = await fetch(`${BROWSER_SERVICE}/api-keys`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name })
    })
    const data = await res.json()
    // Show the key once — it won't be shown again
    addSystemMsg(`API key created: ${data.key.key}\nSave this key — it will not be shown again.`)
    refreshApiKeys()
  } catch (e) {
    addSystemMsg('Failed to create API key: ' + e.message)
  }
})

refreshApiKeys()

addSystemMsg('YAMIL Browser ready')

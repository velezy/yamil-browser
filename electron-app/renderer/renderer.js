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
const KEY_LAST_URL     = 'yamil_last_url'
const KEY_SIDEBAR_OPEN = 'yamil_sidebar_open'
const KEY_CHAT_HISTORY = 'yamil_chat_history'
const KEY_TABS         = 'yamil_tabs'
const KEY_BOOKMARKS    = 'yamil_bookmarks'
const KEY_BMBAR_VIS    = 'yamil_bookmarkbar_visible'
const KEY_HISTORY      = 'yamil_history'
const KEY_AI_MEMORY    = 'yamil_ai_memory'
const MAX_STORED_MSGS  = 200
const MAX_HISTORY      = 5000

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
  try { return JSON.parse(localStorage.getItem(KEY_BOOKMARKS) || '[]') } catch (_) { return [] }
}

function saveBookmarks (arr) {
  try { localStorage.setItem(KEY_BOOKMARKS, JSON.stringify(arr)) } catch (_) {}
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
    wv.setAttribute('partition', 'persist:yamil')
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
  saveTabs()
}

function closeTab (id) {
  const idx = tabs.findIndex(t => t.id === id)
  if (idx === -1) return

  // Don't close the last tab — create a new one first
  if (tabs.length === 1) {
    createTab(startUrl, true)
  }

  const tab = tabs[idx]

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
}

// ── Wire webview events to a tab ──────────────────────────────────────

function wireWebviewEvents (tab) {
  const wv = tab.webview

  wv.addEventListener('did-start-loading', () => {
    if (tab.id === activeTabId) {
      statusLoad.textContent = 'Loading...'
      connDot.className = 'dot connecting'
      showProgressBar()
    }
  })

  wv.addEventListener('did-stop-loading', () => {
    if (tab.id === activeTabId) {
      statusLoad.textContent = ''
      connDot.className = 'dot connected'
      hideProgressBar()
    }
  })

  wv.addEventListener('did-navigate', (e) => {
    tab.url = e.url
    if (tab.id === activeTabId) { updateBar(e.url); updateBookmarkStar() }
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
      connDot.className = 'dot disconnected'
    }
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
}

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
  // Ctrl+W → close active tab
  if (e.ctrlKey && e.key === 'w') { e.preventDefault(); if (activeTabId) closeTab(activeTabId) }
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
    if (document.body.classList.contains('fullscreen')) { toggleFullscreen(); return }
  }
})

// ── Sidebar toggle ────────────────────────────────────────────────────

const sidebar        = document.getElementById('sidebar')
const btnToggle      = document.getElementById('btn-sidebar-toggle')
const btnSidebarOpen = document.getElementById('btn-sidebar-open')

function setSidebarOpen (open) {
  if (open) {
    sidebar.classList.remove('collapsed')
    btnSidebarOpen.style.display = 'none'
  } else {
    sidebar.classList.add('collapsed')
    btnSidebarOpen.style.display = ''
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
    localStorage.setItem(KEY_CHAT_HISTORY, JSON.stringify(msgs.slice(-MAX_STORED_MSGS)))
  } catch (_) {}
}

function loadChatHistory () {
  try {
    const stored = JSON.parse(localStorage.getItem(KEY_CHAT_HISTORY) || '[]')
    if (stored.length) {
      stored.forEach(m => _appendMsg(m.role, m.text))
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

  let pageContext = {}
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

  // Inject AI memory context
  const memory = getAiMemory()
  const memCtx = memory.length > 0 ? '\n\n[User memories: ' + memory.map(m => m.fact).join('; ') + ']' : ''

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text + memCtx, pageContext }),
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
      tabs: tabs.map(t => ({ id: t.id, type: t.type || 'yamil', url: t.url, title: t.title, group: t.group || null, groupColor: t.groupColor || null })),
    }
    localStorage.setItem(KEY_TABS, JSON.stringify(data))
  } catch (_) {}
}

function loadTabs () {
  try {
    const stored = JSON.parse(localStorage.getItem(KEY_TABS) || 'null')
    if (stored && stored.tabs && stored.tabs.length > 0) {
      // Restore counter to avoid ID conflicts
      tabIdCounter = Math.max(...stored.tabs.map(t => t.id), 0)
      // Create tabs without activating
      stored.tabs.forEach(t => {
        const tab = createTab(t.url, false, t.type || 'yamil')
        if (t.group) { tab.group = t.group; tab.groupColor = t.groupColor; tab.tabEl.classList.add('grouped'); tab.tabEl.style.setProperty('--group-color', t.groupColor) }
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
  items.push({ label: 'Close Tab', action: () => closeTab(tabId) })

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
  try { return JSON.parse(localStorage.getItem(KEY_HISTORY) || '[]') } catch (_) { return [] }
}

function saveHistoryData (arr) {
  try { localStorage.setItem(KEY_HISTORY, JSON.stringify(arr)) } catch (_) {}
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
      if (combined.length >= 8) break
    }
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

addrBar.addEventListener('input', showAutocomplete)
addrBar.addEventListener('blur', () => { setTimeout(hideAutocomplete, 150) })

// ── AI Summarize ─────────────────────────────────────────────────────

document.getElementById('btn-summarize').addEventListener('click', async () => {
  if (!aiEndpoint) { addSystemMsg('No AI endpoint configured.'); return }
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
  try { return JSON.parse(localStorage.getItem(KEY_AI_MEMORY) || '[]') } catch (_) { return [] }
}

function saveAiMemory (arr) {
  try { localStorage.setItem(KEY_AI_MEMORY, JSON.stringify(arr)) } catch (_) {}
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

// ── Settings panel ───────────────────────────────────────────────────

const KEY_SETTINGS = 'yamil_settings'
const settingsPanel = document.getElementById('settings-panel')

function getSettings () {
  try { return JSON.parse(localStorage.getItem(KEY_SETTINGS) || '{}') } catch (_) { return {} }
}

function saveSetting (key, value) {
  const s = getSettings()
  s[key] = value
  try { localStorage.setItem(KEY_SETTINGS, JSON.stringify(s)) } catch (_) {}
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
  try { localStorage.removeItem(KEY_CHAT_HISTORY) } catch (_) {}
  addSystemMsg('Chat history cleared.')
})
document.getElementById('set-clear-memory')?.addEventListener('click', () => {
  saveAiMemory([])
  addSystemMsg('AI memory cleared.')
})

// ── Downloads manager ────────────────────────────────────────────────

const KEY_DOWNLOADS = 'yamil_downloads'
const downloadsPanel = document.getElementById('downloads-panel')
const dlBody = document.getElementById('dl-body')
let downloads = []

function getDownloads () {
  try { return JSON.parse(localStorage.getItem(KEY_DOWNLOADS) || '[]') } catch (_) { return [] }
}

function saveDownloads () {
  try { localStorage.setItem(KEY_DOWNLOADS, JSON.stringify(downloads.slice(0, 100))) } catch (_) {}
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
  const items = downloads.length > 0 ? downloads : getDownloads()
  if (items.length === 0) {
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
  return (b / 1048576).toFixed(1) + ' MB'
}

document.getElementById('dl-close').addEventListener('click', closeDownloadsPanel)
downloadsPanel.addEventListener('click', (e) => { if (e.target === downloadsPanel) closeDownloadsPanel() })

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

// ── Background Agent Task Queue ───────────────────────────────────────

const KEY_TASKS = 'yamil_agent_tasks'
const taskQueue = document.getElementById('task-queue')
const tqList = document.getElementById('tq-list')
const tqCount = document.getElementById('tq-count')
const bgCheck = document.getElementById('bg-check')
let agentTasks = []
let taskIdCounter2 = 0

function loadAgentTasks () {
  try { agentTasks = JSON.parse(localStorage.getItem(KEY_TASKS) || '[]') } catch (_) { agentTasks = [] }
  taskIdCounter2 = agentTasks.reduce((max, t) => Math.max(max, t.id || 0), 0)
}

function saveAgentTasks () {
  try { localStorage.setItem(KEY_TASKS, JSON.stringify(agentTasks.slice(-50))) } catch (_) {}
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
    el.innerHTML = `
      <div class="tq-item-goal" title="${task.goal}">${task.goal}</div>
      <div class="tq-item-status">${task.status === 'running' ? 'Running...' : task.status === 'queued' ? 'Queued' : task.status === 'done' ? 'Completed' : 'Failed'}</div>
      ${task.status === 'running' ? `<div class="tq-item-progress"><div class="tq-item-progress-fill" style="width:${task.progress}%"></div></div>` : ''}
    `
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

    if (task.status === 'done' && task.result) {
      const resultBtn = document.createElement('div')
      resultBtn.className = 'tq-item-actions'
      const btn = document.createElement('button')
      btn.textContent = 'Show Result'
      btn.addEventListener('click', () => addAiMsg(`[Task "${task.goal}"] ${task.result}`))
      resultBtn.appendChild(btn)
      el.appendChild(resultBtn)
    }

    if (task.status === 'running' || task.status === 'queued') {
      const actionsEl = document.createElement('div')
      actionsEl.className = 'tq-item-actions'
      const cancelBtn = document.createElement('button')
      cancelBtn.textContent = 'Cancel'
      cancelBtn.addEventListener('click', () => {
        updateAgentTask(task.id, { status: 'failed', result: 'Cancelled by user' })
      })
      actionsEl.appendChild(cancelBtn)
      el.appendChild(actionsEl)
    }

    tqList.appendChild(el)
  })
}

// Toggle task queue visibility
document.getElementById('btn-tasks-toggle').addEventListener('click', () => {
  const visible = taskQueue.style.display !== 'none'
  taskQueue.style.display = visible ? 'none' : 'block'
  if (!visible) renderTaskQueue()
})

// Run a task in the background
async function runBackgroundTask (task) {
  updateAgentTask(task.id, { status: 'running', progress: 10 })
  updateAgentTask(task.id, { steps: ['Preparing request...'] })

  const tab = tabs.find(t => t.id === activeTabId)
  let pageContext = {}
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

  updateAgentTask(task.id, { steps: [...task.steps, 'Sending to AI...'], progress: 30 })

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

    updateAgentTask(task.id, { steps: [...task.steps, 'Processing response...'], progress: 70 })

    const data = await res.json()
    const reply = data.response || data.message || JSON.stringify(data)

    if (data.navigatedUrl) navigateWebview(data.navigatedUrl)

    updateAgentTask(task.id, {
      status: 'done',
      progress: 100,
      result: reply,
      steps: [...task.steps, 'Complete'],
    })

    // Desktop notification
    if (Notification.permission === 'granted') {
      new Notification('YAMIL Task Complete', { body: task.goal, icon: '../assets/yamil-logo.png' })
    }
    addSystemMsg(`Background task completed: "${task.goal}"`)
  } catch (e) {
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
loadChatHistory()

// Restore tabs or create initial tab
if (!loadTabs()) {
  const lastUrl = localStorage.getItem(KEY_LAST_URL) || startUrl
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
})()

downloads = getDownloads()

addSystemMsg('YAMIL Browser ready')

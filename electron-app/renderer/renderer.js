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
const MAX_STORED_MSGS  = 200

// ── Tab state ─────────────────────────────────────────────────────────
let tabs = []          // { id, title, url, webview, tabEl }
let activeTabId = null
let tabIdCounter = 0

// ── Tab management ────────────────────────────────────────────────────

function createTab (url, activate = true) {
  const id = ++tabIdCounter
  url = url || startUrl

  // Create webview element
  const wv = document.createElement('webview')
  wv.setAttribute('allowpopups', '')
  wv.setAttribute('partition', 'persist:yamil')
  wv.src = url
  container.appendChild(wv)

  // Create tab bar element
  const tabEl = document.createElement('div')
  tabEl.className = 'tab'
  tabEl.dataset.tabId = id

  const titleSpan = document.createElement('span')
  titleSpan.className = 'tab-title'
  titleSpan.textContent = 'New Tab'

  const closeBtn = document.createElement('button')
  closeBtn.className = 'tab-close'
  closeBtn.textContent = '\u00d7'
  closeBtn.title = 'Close tab'

  tabEl.appendChild(titleSpan)
  tabEl.appendChild(closeBtn)
  tabsList.appendChild(tabEl)

  // Tab object
  const tab = { id, title: 'New Tab', url, webview: wv, tabEl, titleSpan }
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

  // Wire webview events
  wireWebviewEvents(tab)

  if (activate) switchTab(id)
  saveTabs()
  return tab
}

function switchTab (id) {
  const tab = tabs.find(t => t.id === id)
  if (!tab) return

  activeTabId = id

  // Update webview visibility
  tabs.forEach(t => {
    t.webview.classList.toggle('active', t.id === id)
    t.tabEl.classList.toggle('active', t.id === id)
  })

  // Update address bar and status
  updateBar(tab.url)
  statusUrl.textContent = tab.title || ''
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
  tab.webview.remove()
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
  return tab ? tab.webview : null
}

// Expose for main process queries
window._yamil = { tabs, getActiveWebview, createTab, switchTab, closeTab }

// ── Wire webview events to a tab ──────────────────────────────────────

function wireWebviewEvents (tab) {
  const wv = tab.webview

  wv.addEventListener('did-start-loading', () => {
    if (tab.id === activeTabId) {
      statusLoad.textContent = 'Loading...'
      connDot.className = 'dot connecting'
    }
  })

  wv.addEventListener('did-stop-loading', () => {
    if (tab.id === activeTabId) {
      statusLoad.textContent = ''
      connDot.className = 'dot connected'
    }
  })

  wv.addEventListener('did-navigate', (e) => {
    tab.url = e.url
    if (tab.id === activeTabId) updateBar(e.url)
    saveTabs()
  })

  wv.addEventListener('did-navigate-in-page', (e) => {
    tab.url = e.url
    if (tab.id === activeTabId) updateBar(e.url)
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
}

// ── Address bar ───────────────────────────────────────────────────────

function updateBar (url) {
  if (document.activeElement !== addrBar) addrBar.value = url || ''
  document.getElementById('lock-icon').textContent = url?.startsWith('https') ? '🔒' : '🔓'
}

addrBar.addEventListener('keydown', (e) => {
  if (e.key !== 'Enter') return
  let url = addrBar.value.trim()
  if (!url) return
  if (!url.match(/^https?:\/\//)) url = 'https://' + url
  const wv = getActiveWebview()
  if (wv) wv.loadURL(url)
})

addrBar.addEventListener('focus', () => addrBar.select())

document.getElementById('btn-back').addEventListener('click', () => {
  const wv = getActiveWebview()
  if (wv && wv.canGoBack()) wv.goBack()
})

document.getElementById('btn-forward').addEventListener('click', () => {
  const wv = getActiveWebview()
  if (wv && wv.canGoForward()) wv.goForward()
})

document.getElementById('btn-refresh').addEventListener('click', () => {
  const wv = getActiveWebview()
  if (wv) wv.reload()
})

// ── New tab button ────────────────────────────────────────────────────

document.getElementById('btn-new-tab').addEventListener('click', () => {
  createTab(startUrl, true)
})

// ── Keyboard shortcuts ────────────────────────────────────────────────

document.addEventListener('keydown', (e) => {
  if (e.ctrlKey && e.key === 't') { e.preventDefault(); createTab(startUrl, true) }
  if (e.ctrlKey && e.key === 'w') { e.preventDefault(); if (activeTabId) closeTab(activeTabId) }
  if (e.ctrlKey && e.key === 'Tab') {
    e.preventDefault()
    const idx = tabs.findIndex(t => t.id === activeTabId)
    const next = e.shiftKey
      ? (idx - 1 + tabs.length) % tabs.length
      : (idx + 1) % tabs.length
    switchTab(tabs[next].id)
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
  const wv = getActiveWebview()
  if (wv) wv.loadURL(url)
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

  if (!aiEndpoint) {
    addAiMsg('No AI endpoint configured. Set AI_ENDPOINT env var.')
    return
  }

  const navMatch = text.match(/^(?:go\s+to|navigate\s+to|open|visit)\s+([^\s,]+)/i)
  if (navMatch) {
    const url = resolveUrl(navMatch[1])
    navigateWebview(url)
    addSystemMsg(`Navigating to ${url}...`)
  }

  let pageContext = {}
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

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, pageContext }),
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
      tabs: tabs.map(t => ({ id: t.id, url: t.url, title: t.title })),
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
        const tab = createTab(t.url, false)
        // Override the auto-generated id to match stored id
        // (not needed since createTab increments, but we want to match activeTabId)
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

// ── Init ──────────────────────────────────────────────────────────────

loadChatHistory()

// Restore tabs or create initial tab
if (!loadTabs()) {
  const lastUrl = localStorage.getItem(KEY_LAST_URL) || startUrl
  createTab(lastUrl, true)
}

// Restore sidebar state
const sidebarWasOpen = localStorage.getItem(KEY_SIDEBAR_OPEN) !== '0'
setSidebarOpen(sidebarWasOpen)

addSystemMsg('YAMIL Browser ready')

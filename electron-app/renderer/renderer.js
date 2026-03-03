/* ── YAMIL Browser — Renderer (webview edition) ───────────────────── */

const webview    = document.getElementById('screen')
const addrBar    = document.getElementById('address-bar')
const statusUrl  = document.getElementById('status-url')
const statusLoad = document.getElementById('status-loading')
const connDot    = document.getElementById('conn-dot')
const chatLog    = document.getElementById('chat-log')
const chatInput  = document.getElementById('chat-input')

const aiEndpoint = window.YAMIL_CONFIG?.AI_ENDPOINT || null
const startUrl   = window.YAMIL_CONFIG?.START_URL   || 'https://yamil-ai.com'

if (window.YAMIL_CONFIG?.APP_TITLE) document.title = window.YAMIL_CONFIG.APP_TITLE

// ── Webview navigation events ─────────────────────────────────────────

webview.addEventListener('did-start-loading', () => {
  statusLoad.textContent = 'Loading...'
  connDot.className = 'dot connecting'
})

webview.addEventListener('did-stop-loading', () => {
  statusLoad.textContent = ''
  connDot.className = 'dot connected'
})

webview.addEventListener('did-navigate', (e) => {
  updateBar(e.url)
})

webview.addEventListener('did-navigate-in-page', (e) => {
  updateBar(e.url)
})

webview.addEventListener('page-title-updated', (e) => {
  statusUrl.textContent = e.title
})

webview.addEventListener('did-fail-load', (e) => {
  if (e.errorCode !== -3) { // -3 = aborted (user navigated away), ignore
    statusLoad.textContent = `Error: ${e.errorDescription}`
    connDot.className = 'dot disconnected'
  }
})

function updateBar (url) {
  if (document.activeElement !== addrBar) addrBar.value = url || ''
  document.getElementById('lock-icon').textContent = url?.startsWith('https') ? '🔒' : '🔓'
}

// ── Address bar ───────────────────────────────────────────────────────

addrBar.addEventListener('keydown', (e) => {
  if (e.key !== 'Enter') return
  let url = addrBar.value.trim()
  if (!url) return
  if (!url.match(/^https?:\/\//)) url = 'https://' + url
  webview.loadURL(url)
})

addrBar.addEventListener('focus', () => addrBar.select())

document.getElementById('btn-back').addEventListener('click', () => {
  if (webview.canGoBack()) webview.goBack()
})

document.getElementById('btn-forward').addEventListener('click', () => {
  if (webview.canGoForward()) webview.goForward()
})

document.getElementById('btn-refresh').addEventListener('click', () => {
  webview.reload()
})

// ── Sidebar toggle ────────────────────────────────────────────────────

document.getElementById('btn-sidebar-toggle').addEventListener('click', () => {
  document.getElementById('sidebar').classList.toggle('collapsed')
})

// ── AI Chat ───────────────────────────────────────────────────────────

function addMsg (role, text) {
  const div = document.createElement('div')
  div.className = `chat-msg ${role}`
  div.textContent = text
  chatLog.appendChild(div)
  chatLog.scrollTop = chatLog.scrollHeight
}

const addSystemMsg = (t) => addMsg('system', t)
const addUserMsg   = (t) => addMsg('user', t)
const addAiMsg     = (t) => addMsg('ai', t)
const addErrorMsg  = (t) => addMsg('error', t)

// Resolve a plain name like "twilio" or "google" to a URL
function resolveUrl (input) {
  input = input.trim().replace(/[.!?]+$/, '')
  if (input.match(/^https?:\/\//i)) return input
  // If it looks like a domain (contains a dot, no spaces) add https://
  if (!input.includes(' ') && input.includes('.')) return 'https://' + input
  // Otherwise treat as a search/site name — go straight to the .com
  return 'https://' + input.toLowerCase().replace(/\s+/g, '') + '.com'
}

// Navigate the webview and update the address bar
function navigateWebview (url) {
  addrBar.value = url
  webview.loadURL(url)
}

// Extract the first https URL mentioned in a string
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

  // ── Intercept navigation commands and move the webview immediately ──
  const navMatch = text.match(/^(?:go\s+to|navigate\s+to|open|visit)\s+(.+)/i)
  if (navMatch) {
    const url = resolveUrl(navMatch[1])
    navigateWebview(url)
    addSystemMsg(`Navigating to ${url}…`)
  }

  // Send current page context with every message
  let pageContext = {}
  try {
    const result = await webview.executeJavaScript(`({
      url:   location.href,
      title: document.title,
      text:  document.body.innerText.slice(0, 4000),
    })`)
    pageContext = result
  } catch (_) {}

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, pageContext }),
    })
    const data = await res.json()
    const reply = data.response || data.message || JSON.stringify(data)
    addAiMsg(reply)

    // If AI navigated/clicked to a new page, sync the webview to follow
    if (data.navigatedUrl) {
      navigateWebview(data.navigatedUrl)
    } else if (navMatch) {
      // Fallback: extract URL from reply text
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

// ── Init ──────────────────────────────────────────────────────────────

addrBar.value = startUrl
webview.src = startUrl
addSystemMsg('YAMIL Browser ready — ' + startUrl)

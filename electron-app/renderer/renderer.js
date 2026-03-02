/* ── YAMIL Browser — Renderer ─────────────────────────────────────── */

const canvas   = document.getElementById('screen')
const ctx      = canvas.getContext('2d')
const overlay  = document.getElementById('overlay-msg')
const addrBar  = document.getElementById('address-bar')
const statusUrl= document.getElementById('status-url')
const connDot  = document.getElementById('conn-dot')
const chatLog  = document.getElementById('chat-log')
const chatInput= document.getElementById('chat-input')
const evtStatus= document.getElementById('status-events')

let sessionId       = null
let screenW         = 1280
let screenH         = 800
let lastEventCount  = 0
let aiEndpoint      = window.AI_ENDPOINT || null // set per-app via env

// ── Screencast canvas ─────────────────────────────────────────────────

function setCanvasSize () {
  const area = document.getElementById('browser-area')
  const areaW = area.clientWidth
  const areaH = area.clientHeight
  const scale = Math.min(areaW / screenW, areaH / screenH)
  canvas.style.width  = Math.floor(screenW * scale) + 'px'
  canvas.style.height = Math.floor(screenH * scale) + 'px'
  canvas.width  = screenW
  canvas.height = screenH
}

window.addEventListener('resize', setCanvasSize)

function drawFrame (base64jpeg) {
  const img = new Image()
  img.onload = () => ctx.drawImage(img, 0, 0)
  img.src = 'data:image/jpeg;base64,' + base64jpeg
}

// ── Canvas → browser coordinate mapping ──────────────────────────────

function canvasToPage (clientX, clientY) {
  const rect  = canvas.getBoundingClientRect()
  const scaleX = screenW / rect.width
  const scaleY = screenH / rect.height
  return {
    x: Math.round((clientX - rect.left)  * scaleX),
    y: Math.round((clientY - rect.top)   * scaleY),
  }
}

// ── Canvas interaction ────────────────────────────────────────────────

canvas.addEventListener('click', async (e) => {
  if (!sessionId) return
  const pos = canvasToPage(e.clientX, e.clientY)
  await window.yamil.mouseClick(pos)
  syncUrl()
})

canvas.addEventListener('mousemove', (e) => {
  if (!sessionId) return
  const pos = canvasToPage(e.clientX, e.clientY)
  window.yamil.mouseMove(pos)
})

canvas.addEventListener('wheel', (e) => {
  if (!sessionId) return
  e.preventDefault()
  window.yamil.scroll({ direction: e.deltaY > 0 ? 'down' : 'up', amount: Math.abs(e.deltaY) })
}, { passive: false })

canvas.addEventListener('contextmenu', (e) => e.preventDefault())

// Keyboard — when canvas has focus
canvas.setAttribute('tabindex', '0')
canvas.addEventListener('keydown', async (e) => {
  if (!sessionId) return
  e.preventDefault()
  const named = ['Enter','Tab','Escape','Backspace','Delete','ArrowUp','ArrowDown','ArrowLeft','ArrowRight','Home','End','PageUp','PageDown','F5']
  if (named.includes(e.key)) {
    await window.yamil.pressKey(e.key)
    if (e.key === 'Enter' || e.key === 'F5') syncUrl()
  } else if (e.key.length === 1) {
    await window.yamil.keyboardType(e.key)
  }
})

// ── Address bar ───────────────────────────────────────────────────────

addrBar.addEventListener('keydown', async (e) => {
  if (e.key !== 'Enter') return
  let url = addrBar.value.trim()
  if (!url) return
  if (!url.startsWith('http')) url = 'https://' + url
  addSystemMsg('Navigating to ' + url)
  const res = await window.yamil.navigate(url)
  if (res?.url) addrBar.value = res.url
})

document.getElementById('btn-back').addEventListener('click', async () => {
  await window.yamil.goBack()
  syncUrl()
})

document.getElementById('btn-refresh').addEventListener('click', async () => {
  await window.yamil.pressKey('F5')
  syncUrl()
})

async function syncUrl () {
  try {
    const { url, title } = await window.yamil.getUrl()
    addrBar.value  = url || ''
    statusUrl.textContent = title || url || ''
    document.getElementById('lock-icon').textContent = url?.startsWith('https') ? '🔒' : '🔓'
  } catch (_) {}
}

// ── Sidebar toggle ────────────────────────────────────────────────────

document.getElementById('btn-sidebar-toggle').addEventListener('click', () => {
  const sb = document.getElementById('sidebar')
  sb.classList.toggle('collapsed')
  setCanvasSize()
})

// ── AI Chat sidebar ────────────────────────────────────────────────────

function addMsg (role, text) {
  const div = document.createElement('div')
  div.className = `chat-msg ${role}`
  div.textContent = text
  chatLog.appendChild(div)
  chatLog.scrollTop = chatLog.scrollHeight
}

function addSystemMsg (text) { addMsg('system', text) }
function addUserMsg   (text) { addMsg('user',   text) }
function addAiMsg     (text) { addMsg('ai',     text) }
function addErrorMsg  (text) { addMsg('error',  text) }

async function sendChat () {
  const text = chatInput.value.trim()
  if (!text) return
  chatInput.value = ''
  addUserMsg(text)

  if (!aiEndpoint) {
    addAiMsg('AI endpoint not configured. Set window.AI_ENDPOINT to your orchestrator URL.')
    return
  }

  try {
    const res = await fetch(aiEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, sessionId }),
    })
    const data = await res.json()
    addAiMsg(data.response || data.message || JSON.stringify(data))
  } catch (err) {
    addErrorMsg('AI request failed: ' + err.message)
  }
}

document.getElementById('chat-send').addEventListener('click', sendChat)
chatInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat() }
})

// ── CDP Event counter ─────────────────────────────────────────────────

window.yamil.onCdpEvent(() => {
  lastEventCount++
  evtStatus.textContent = `${lastEventCount} events`
})

// ── Service connection lifecycle ──────────────────────────────────────

window.yamil.onSessionReady(({ id, serviceUrl }) => {
  sessionId = id
  overlay.classList.add('hidden')
  connDot.className = 'dot connected'
  setCanvasSize()
  addSystemMsg('Connected to ' + serviceUrl)
  addSystemMsg('Session: ' + id)
  syncUrl()
})

window.yamil.onServiceError((msg) => {
  overlay.classList.remove('hidden')
  overlay.textContent = msg
  connDot.className = 'dot disconnected'
})

window.yamil.onScreencastFrame(({ frame, metadata }) => {
  if (metadata) {
    screenW = metadata.deviceWidth  || screenW
    screenH = metadata.deviceHeight || screenH
  }
  drawFrame(frame)
})

// ── Init ──────────────────────────────────────────────────────────────

connDot.className = 'dot connecting'
overlay.classList.remove('hidden')
setCanvasSize()
addSystemMsg('Starting YAMIL Stealth Browser...')

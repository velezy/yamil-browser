const { app, BrowserWindow, ipcMain, shell } = require('electron')
const path = require('path')
const { WebSocket } = require('ws')

const SERVICE_URL  = process.env.BROWSER_SERVICE_URL || 'http://localhost:4000'
const WS_URL       = SERVICE_URL.replace(/^http/, 'ws')
const AI_ENDPOINT  = process.env.AI_ENDPOINT || null   // e.g. http://localhost:8003/ai/chat
const APP_TITLE    = process.env.APP_TITLE    || 'YAMIL Browser'

let mainWindow
let sessionId     = null
let screencastWs  = null
let eventsWs      = null

// ── Window ────────────────────────────────────────────────────────────

function createWindow () {
  mainWindow = new BrowserWindow({
    width:  1440,
    height: 900,
    minWidth:  900,
    minHeight: 600,
    backgroundColor: '#0f172a',
    icon: path.join(__dirname, 'assets', 'yamil-logo.svg'),
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  })

  mainWindow.loadFile('renderer/index.html')
  mainWindow.setTitle(APP_TITLE)
  mainWindow.on('closed', () => { mainWindow = null })
}

// ── Service API helpers ────────────────────────────────────────────────

async function servicePost (path, body = {}) {
  const res = await fetch(`${SERVICE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return res.json()
}

async function serviceGet (path) {
  const res = await fetch(`${SERVICE_URL}${path}`)
  return res.json()
}

// ── Session lifecycle ─────────────────────────────────────────────────

async function initSession () {
  try {
    const { id } = await servicePost('/sessions')
    sessionId = id
    mainWindow?.webContents.send('session-ready', { id, serviceUrl: SERVICE_URL })
    connectScreencast(id)
    connectEvents(id)
  } catch (err) {
    console.error('[main] Could not create session:', err.message)
    mainWindow?.webContents.send('service-error', 'Cannot reach browser service at ' + SERVICE_URL)
    setTimeout(initSession, 3000)
  }
}

// ── Screencast WebSocket ───────────────────────────────────────────────

function connectScreencast (id) {
  if (screencastWs) { try { screencastWs.close() } catch (_) {} }

  screencastWs = new WebSocket(`${WS_URL}/sessions/${id}/screencast`)

  screencastWs.on('message', (raw) => {
    try {
      const { frame, metadata } = JSON.parse(raw)
      if (frame) mainWindow?.webContents.send('screencast-frame', { frame, metadata })
    } catch (_) {}
  })

  screencastWs.on('close',   () => setTimeout(() => connectScreencast(id), 1000))
  screencastWs.on('error',   (e) => console.error('[screencast]', e.message))
}

// ── CDP Event stream WebSocket ─────────────────────────────────────────

function connectEvents (id) {
  if (eventsWs) { try { eventsWs.close() } catch (_) {} }

  eventsWs = new WebSocket(`${WS_URL}/sessions/${id}/events`)

  eventsWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw)
      mainWindow?.webContents.send('cdp-event', msg)
    } catch (_) {}
  })

  eventsWs.on('close',   () => setTimeout(() => connectEvents(id), 1000))
  eventsWs.on('error',   (e) => console.error('[events]', e.message))
}

// ── IPC handlers ──────────────────────────────────────────────────────

ipcMain.handle('navigate',      (_, url)       => servicePost(`/sessions/${sessionId}/navigate`,       { url }))
ipcMain.handle('go-back',       ()             => servicePost(`/sessions/${sessionId}/back`))
ipcMain.handle('press-key',     (_, key)       => servicePost(`/sessions/${sessionId}/press`,          { key }))
ipcMain.handle('scroll',        (_, d)         => servicePost(`/sessions/${sessionId}/scroll`,         d))
ipcMain.handle('mouse-click',   (_, { x, y }) => servicePost(`/sessions/${sessionId}/mouse/click`,    { x, y }))
ipcMain.handle('mouse-move',    (_, { x, y }) => servicePost(`/sessions/${sessionId}/mouse/move`,     { x, y }))
ipcMain.handle('keyboard-type', (_, text)      => servicePost(`/sessions/${sessionId}/keyboard/type`,  { text }))
ipcMain.handle('evaluate',      (_, script)    => servicePost(`/sessions/${sessionId}/evaluate`,       { script }))
ipcMain.handle('get-url',       ()             => serviceGet (`/sessions/${sessionId}/url`))
ipcMain.handle('get-sessions',  ()             => serviceGet ('/sessions'))

// ── App lifecycle ─────────────────────────────────────────────────────

app.whenReady().then(() => {
  createWindow()
  initSession()
  app.on('activate', () => { if (!mainWindow) createWindow() })
})

app.on('window-all-closed', () => {
  if (sessionId) servicePost(`/sessions/${sessionId}`, {}).catch(() => {})
  if (process.platform !== 'darwin') app.quit()
})

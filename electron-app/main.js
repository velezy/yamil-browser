const { app, BrowserWindow, ipcMain, Tray, Menu, nativeImage } = require('electron')
const path = require('path')
const http  = require('http')

const APP_TITLE   = process.env.APP_TITLE || 'YAMIL Browser'
const CTRL_PORT   = parseInt(process.env.CTRL_PORT || '9300', 10)
const START_MINIMIZED = process.argv.includes('--minimized')


let mainWindow
let tray = null
let webviewRef = null   // set via IPC from renderer once <webview> is ready

// ── Custom URL protocol: yamil-browser:// ─────────────────────────────
// On Windows the default registration omits the app directory, so Electron
// receives just the URL as argv[1] and crashes trying to require() it.
// Explicitly pass __dirname so the registry entry becomes:
//   electron.exe "<app-dir>" "%1"
if (process.platform === 'win32') {
  app.setAsDefaultProtocolClient('yamil-browser', process.execPath, [__dirname])
} else {
  app.setAsDefaultProtocolClient('yamil-browser')
}

const gotLock = app.requestSingleInstanceLock()
if (!gotLock) {
  app.quit()
} else {
  app.on('second-instance', () => {
    focusWindow()
  })
}

function focusWindow () {
  if (mainWindow) {
    if (!mainWindow.isVisible()) mainWindow.show()
    if (mainWindow.isMinimized()) mainWindow.restore()
    mainWindow.focus()
  }
}

// ── HTTP control server on port 9300 ─────────────────────────────────
// Lets the YAMIL web app (and Claude Code MCP) control the desktop app
// without relying on protocol registration.
// CORS headers allow requests from any origin (same machine only).

function startControlServer () {
  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin',  '*')
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }

    const url = new URL(req.url, `http://localhost:${CTRL_PORT}`)

    // ── GET /ping ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/ping') {
      json(res, { ok: true, app: APP_TITLE })
      return
    }

    // ── POST /focus ────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/focus') {
      focusWindow()
      json(res, { ok: true })
      return
    }

    // ── GET /url ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/url') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(
        `(function(){ const wv = document.getElementById('screen'); return wv ? wv.getURL() : null })()`
      ).then(u => json(res, { url: u }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /navigate ────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/navigate') {
      readBody(req, body => {
        const { url: navUrl } = body
        if (!navUrl) { json(res, { error: 'url required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        focusWindow()
        mainWindow.webContents.executeJavaScript(
          `(function(){ const wv = document.getElementById('screen'); if(wv) wv.loadURL(${JSON.stringify(navUrl)}); return !!wv })()`
        ).then(ok => json(res, { ok }))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── GET /screenshot ────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/screenshot') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      // Capture the webview contents (NativeImage → PNG base64)
      mainWindow.webContents.executeJavaScript(
        `(function(){
          const wv = document.getElementById('screen')
          if (!wv) return Promise.resolve(null)
          return wv.capturePage().then(img => img.toDataURL())
        })()`
      ).then(dataUrl => {
        if (!dataUrl) { json(res, { error: 'webview not ready' }, 503); return }
        // Strip "data:image/png;base64," prefix
        const base64 = dataUrl.replace(/^data:image\/\w+;base64,/, '')
        res.setHeader('Content-Type', 'image/png')
        res.writeHead(200)
        res.end(Buffer.from(base64, 'base64'))
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── GET /dom ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/dom') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(
        `(function(){
          const wv = document.getElementById('screen')
          if (!wv) return Promise.resolve(null)
          return wv.executeJavaScript(\`({
            url:      location.href,
            title:    document.title,
            text:     document.body.innerText.slice(0, 8000),
            inputs:   Array.from(document.querySelectorAll('input,textarea,select')).slice(0,50).map(el=>({tag:el.tagName,type:el.type||null,name:el.name||null,placeholder:el.placeholder||null,id:el.id||null})),
            buttons:  Array.from(document.querySelectorAll('button,[role=button],a')).slice(0,80).map(el=>({tag:el.tagName,text:(el.innerText||el.getAttribute('aria-label')||'').slice(0,80),href:el.href||null,id:el.id||null})),
          })\`)
        })()`
      ).then(d => json(res, d || {}))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /eval ─────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/eval') {
      readBody(req, body => {
        const { script } = body
        if (!script) { json(res, { error: 'script required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(
          `(function(){
            const wv = document.getElementById('screen')
            if (!wv) return Promise.resolve({error:'no webview'})
            return wv.executeJavaScript(${JSON.stringify(script)})
          })()`
        ).then(result => json(res, { result }))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── GET /window-screenshot ────────────────────────────────────
    // Capture the full Electron window (sidebar + webview) as PNG.
    if (req.method === 'GET' && url.pathname === '/window-screenshot') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.capturePage().then(img => {
        const buf = img.toPNG()
        res.setHeader('Content-Type', 'image/png')
        res.writeHead(200)
        res.end(buf)
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /renderer-eval ────────────────────────────────────────
    // Run JS in the Electron renderer context (sidebar, address bar, etc.)
    // Unlike /eval which runs inside the webview, this runs in the shell UI.
    if (req.method === 'POST' && url.pathname === '/renderer-eval') {
      readBody(req, body => {
        const { script } = body
        if (!script) { json(res, { error: 'script required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(script)
          .then(result => json(res, { result }))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /sidebar-chat ─────────────────────────────────────────
    // Send a message through the Electron AI sidebar chat.
    if (req.method === 'POST' && url.pathname === '/sidebar-chat') {
      readBody(req, body => {
        const { message } = body
        if (!message) { json(res, { error: 'message required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        focusWindow()
        mainWindow.webContents.executeJavaScript(`
          (function() {
            const ta = document.getElementById('chat-input')
            if (!ta) return { error: 'no chat input found' }
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value').set
            nativeInputValueSetter.call(ta, ${JSON.stringify(message)})
            ta.dispatchEvent(new Event('input', { bubbles: true }))
            ta.dispatchEvent(new Event('change', { bubbles: true }))
            const btn = document.getElementById('chat-send')
            if (btn) { btn.click(); return { sent: true } }
            ta.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true }))
            return { sent: true, via: 'enter' }
          })()
        `).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    res.writeHead(404); res.end('Not found')
  })

  server.listen(CTRL_PORT, '127.0.0.1', () => {
    console.log(`[YAMIL ctrl] HTTP server listening on http://127.0.0.1:${CTRL_PORT}`)
  })
  server.on('error', e => console.error('[YAMIL ctrl] server error:', e.message))
}

function json (res, data, status = 200) {
  res.writeHead(status, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(data))
}

function readBody (req, cb) {
  let raw = ''
  req.on('data', chunk => { raw += chunk })
  req.on('end', () => {
    try { cb(JSON.parse(raw || '{}')) } catch { cb({}) }
  })
}

// ── Window ────────────────────────────────────────────────────────────

function createWindow () {
  mainWindow = new BrowserWindow({
    width:  1440,
    height: 900,
    minWidth:  900,
    minHeight: 600,
    backgroundColor: '#0f172a',
    icon: path.join(__dirname, 'assets', process.platform === 'darwin' ? 'icon.icns' : process.platform === 'win32' ? 'icon.ico' : 'icon.png'),
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      webviewTag: true,   // required for <webview>
      sandbox: false,     // webview requires sandbox off
    },
  })

  mainWindow.loadFile('renderer/index.html')
  mainWindow.setTitle(APP_TITLE)

  // Minimize to tray instead of quitting when tray is active
  mainWindow.on('close', (e) => {
    if (tray) {
      e.preventDefault()
      mainWindow.hide()
    }
  })
  mainWindow.on('closed', () => { mainWindow = null })
}

// ── App lifecycle ─────────────────────────────────────────────────────

app.on('open-url', (event, _url) => {
  event.preventDefault()
  focusWindow()
})

function createTray () {
  // Use icon.png (32×32 works cross-platform); fall back to logo
  const iconFile = path.join(__dirname, 'assets', 'icon.png')
  const icon = nativeImage.createFromPath(iconFile)
  tray = new Tray(icon.isEmpty() ? nativeImage.createFromPath(path.join(__dirname, 'assets', 'yamil-logo.png')) : icon)
  tray.setToolTip(APP_TITLE)

  const menu = Menu.buildFromTemplate([
    { label: 'Show YAMIL Browser', click: focusWindow },
    { type: 'separator' },
    { label: 'Quit', click: () => { tray.destroy(); tray = null; app.quit() } },
  ])
  tray.setContextMenu(menu)
  tray.on('click', focusWindow)
}

app.whenReady().then(() => {
  startControlServer()
  createWindow()
  createTray()
  if (START_MINIMIZED) mainWindow.hide()
  app.on('activate', () => {
    if (!mainWindow) createWindow()
    else focusWindow()
  })
})

app.on('window-all-closed', () => {
  // With tray active the window is hidden, not closed — so this only fires on real quit
  if (!tray && process.platform !== 'darwin') app.quit()
})

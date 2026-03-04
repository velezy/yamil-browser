const { app, BrowserWindow, ipcMain, Tray, Menu, nativeImage } = require('electron')
const path = require('path')
const http  = require('http')
const fs    = require('fs')

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

    // ── POST /dialog ─────────────────────────────────────────────
    // Set up a dialog handler (alert/confirm/prompt) for the next dialog
    if (req.method === 'POST' && url.pathname === '/dialog') {
      readBody(req, body => {
        const { action, promptText } = body // action: "accept" | "dismiss"
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            const wv = document.getElementById('screen')
            if (!wv) return { error: 'no webview' }
            // Inject dialog interceptor into webview
            return wv.executeJavaScript(\`(function(){
              window.__yamilDialogResult = null;
              const origAlert = window.alert;
              const origConfirm = window.confirm;
              const origPrompt = window.prompt;
              window.alert = function(msg) {
                window.__yamilDialogResult = { type: 'alert', message: msg };
                ${action === 'accept' ? '' : ''}
                return undefined;
              };
              window.confirm = function(msg) {
                window.__yamilDialogResult = { type: 'confirm', message: msg };
                return ${action === 'accept' ? 'true' : 'false'};
              };
              window.prompt = function(msg, def) {
                window.__yamilDialogResult = { type: 'prompt', message: msg, defaultValue: def };
                return ${action === 'accept' ? JSON.stringify(promptText || '') : 'null'};
              };
              // Auto-restore after 30s
              setTimeout(() => {
                window.alert = origAlert;
                window.confirm = origConfirm;
                window.prompt = origPrompt;
              }, 30000);
              return { handler: 'set', action: ${JSON.stringify(action || 'accept')} };
            })()\`)
          })()
        `).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /screenshot-element ──────────────────────────────────
    // Screenshot a specific element by CSS selector (returns PNG)
    if (req.method === 'POST' && url.pathname === '/screenshot-element') {
      readBody(req, body => {
        const { selector } = body
        if (!selector) { json(res, { error: 'selector required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        // Get element bounding rect, then capture page and crop
        mainWindow.webContents.executeJavaScript(`
          (function() {
            const wv = document.getElementById('screen')
            if (!wv) return Promise.resolve(null)
            return wv.executeJavaScript(\`(function(){
              const el = document.querySelector(${JSON.stringify(selector).replace(/\\/g, '\\\\').replace(/`/g, '\\`')});
              if (!el) return null;
              el.scrollIntoView({ block: 'center', behavior: 'instant' });
              const r = el.getBoundingClientRect();
              return { x: Math.round(r.x), y: Math.round(r.y), width: Math.round(r.width), height: Math.round(r.height) };
            })()\`).then(rect => {
              if (!rect) return null;
              return wv.capturePage({
                x: Math.max(0, rect.x),
                y: Math.max(0, rect.y),
                width: Math.max(1, rect.width),
                height: Math.max(1, rect.height)
              }).then(img => img.toDataURL());
            })
          })()
        `).then(dataUrl => {
          if (!dataUrl) { json(res, { error: 'element not found or capture failed' }, 404); return }
          const base64 = dataUrl.replace(/^data:image\/\w+;base64,/, '')
          res.setHeader('Content-Type', 'image/png')
          res.writeHead(200)
          res.end(Buffer.from(base64, 'base64'))
        }).catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /print-pdf ──────────────────────────────────────────
    // Generate PDF of the current webview page
    if (req.method === 'POST' && url.pathname === '/print-pdf') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(`
        (function() {
          const wv = document.getElementById('screen')
          if (!wv || !wv.getWebContentsId) return Promise.resolve(null)
          return wv.getWebContentsId()
        })()
      `).then(async wcId => {
        if (!wcId) { json(res, { error: 'webview not ready' }, 503); return }
        const { webContents } = require('electron')
        const wc = webContents.fromId(wcId)
        if (!wc) { json(res, { error: 'webcontents not found' }, 503); return }
        const pdfBuf = await wc.printToPDF({
          printBackground: true,
          preferCSSPageSize: true,
        })
        res.setHeader('Content-Type', 'application/pdf')
        res.writeHead(200)
        res.end(pdfBuf)
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /drag ────────────────────────────────────────────────
    // Execute drag-and-drop between two selectors via event dispatch
    if (req.method === 'POST' && url.pathname === '/drag') {
      readBody(req, body => {
        const { sourceSelector, targetSelector } = body
        if (!sourceSelector || !targetSelector) { json(res, { error: 'sourceSelector and targetSelector required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            const wv = document.getElementById('screen')
            if (!wv) return Promise.resolve({ error: 'no webview' })
            return wv.executeJavaScript(\`(function(){
              const src = document.querySelector(${JSON.stringify(sourceSelector).replace(/\\/g, '\\\\').replace(/`/g, '\\`')});
              const tgt = document.querySelector(${JSON.stringify(targetSelector).replace(/\\/g, '\\\\').replace(/`/g, '\\`')});
              if (!src) return { error: 'source not found' };
              if (!tgt) return { error: 'target not found' };
              const dt = new DataTransfer();
              src.dispatchEvent(new DragEvent('dragstart', { bubbles: true, cancelable: true, dataTransfer: dt }));
              tgt.dispatchEvent(new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer: dt }));
              tgt.dispatchEvent(new DragEvent('dragover',  { bubbles: true, cancelable: true, dataTransfer: dt }));
              tgt.dispatchEvent(new DragEvent('drop',      { bubbles: true, cancelable: true, dataTransfer: dt }));
              src.dispatchEvent(new DragEvent('dragend',   { bubbles: true, cancelable: true, dataTransfer: dt }));
              return { ok: true };
            })()\`)
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

// ── Window state persistence ──────────────────────────────────────────

function windowStatePath () {
  return path.join(app.getPath('userData'), 'window-state.json')
}

function loadWindowState () {
  try {
    return JSON.parse(fs.readFileSync(windowStatePath(), 'utf8'))
  } catch (_) {
    return { width: 1440, height: 900 }
  }
}

function saveWindowState () {
  if (!mainWindow || mainWindow.isMinimized() || mainWindow.isMaximized()) return
  try {
    const [x, y]         = mainWindow.getPosition()
    const [width, height] = mainWindow.getSize()
    fs.writeFileSync(windowStatePath(), JSON.stringify({ x, y, width, height }))
  } catch (_) {}
}

// ── Window ────────────────────────────────────────────────────────────

function createWindow () {
  const state = loadWindowState()

  mainWindow = new BrowserWindow({
    width:  state.width  || 1440,
    height: state.height || 900,
    x: state.x,
    y: state.y,
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

  // Save window bounds on resize and move
  mainWindow.on('resize', saveWindowState)
  mainWindow.on('move',   saveWindowState)

  // Minimize to tray instead of quitting when tray is active
  mainWindow.on('close', (e) => {
    saveWindowState()
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

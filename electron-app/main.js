const { app, BrowserWindow, ipcMain, Tray, Menu, nativeImage } = require('electron')
const path = require('path')
const http  = require('http')
const fs    = require('fs')

const APP_TITLE   = process.env.APP_TITLE || 'YAMIL Browser'
const CTRL_PORT   = parseInt(process.env.CTRL_PORT || '9300', 10)
const START_MINIMIZED = process.argv.includes('--minimized')


let mainWindow
let tray = null

// ── Custom URL protocol: yamil-browser:// ─────────────────────────────
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

// ── Helper: run JS in the active tab's webview ────────────────────────
// All endpoints that interact with the page go through this helper.
// It asks the renderer for the active webview and executes code inside it.

function execInActiveWebview (script) {
  if (!mainWindow) return Promise.reject(new Error('no window'))
  return mainWindow.webContents.executeJavaScript(
    `(function(){
      const wv = window._yamil && window._yamil.getActiveWebview()
      if (!wv) return Promise.resolve({error:'no webview'})
      return wv.executeJavaScript(${JSON.stringify(script)})
    })()`
  )
}

function captureActiveWebview () {
  if (!mainWindow) return Promise.reject(new Error('no window'))
  return mainWindow.webContents.executeJavaScript(
    `(function(){
      const wv = window._yamil && window._yamil.getActiveWebview()
      if (!wv) return Promise.resolve(null)
      return wv.capturePage().then(img => img.toDataURL())
    })()`
  )
}

function getActiveWebviewUrl () {
  if (!mainWindow) return Promise.reject(new Error('no window'))
  return mainWindow.webContents.executeJavaScript(
    `(function(){
      const wv = window._yamil && window._yamil.getActiveWebview()
      return wv ? wv.getURL() : null
    })()`
  )
}

// ── HTTP control server on port 9300 ─────────────────────────────────

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
      getActiveWebviewUrl()
        .then(u => json(res, { url: u }))
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
          `(function(){
            const wv = window._yamil && window._yamil.getActiveWebview()
            if(wv) wv.loadURL(${JSON.stringify(navUrl)})
            return !!wv
          })()`
        ).then(ok => json(res, { ok }))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── GET /screenshot ──────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/screenshot') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      captureActiveWebview().then(dataUrl => {
        if (!dataUrl) { json(res, { error: 'webview not ready' }, 503); return }
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
      execInActiveWebview(`({
        url:      location.href,
        title:    document.title,
        text:     document.body.innerText.slice(0, 8000),
        inputs:   Array.from(document.querySelectorAll('input,textarea,select')).slice(0,50).map(el=>({tag:el.tagName,type:el.type||null,name:el.name||null,placeholder:el.placeholder||null,id:el.id||null})),
        buttons:  Array.from(document.querySelectorAll('button,[role=button],a')).slice(0,80).map(el=>({tag:el.tagName,text:(el.innerText||el.getAttribute('aria-label')||'').slice(0,80),href:el.href||null,id:el.id||null})),
      })`)
        .then(d => json(res, d || {}))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /eval ─────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/eval') {
      readBody(req, body => {
        const { script } = body
        if (!script) { json(res, { error: 'script required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        execInActiveWebview(script)
          .then(result => json(res, { result }))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── GET /window-screenshot ────────────────────────────────────
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

    // ── POST /sidebar-chat ──────────────────────────────────────────
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
    if (req.method === 'POST' && url.pathname === '/dialog') {
      readBody(req, body => {
        const { action, promptText } = body
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        execInActiveWebview(`(function(){
          window.__yamilDialogResult = null;
          const origAlert = window.alert;
          const origConfirm = window.confirm;
          const origPrompt = window.prompt;
          window.alert = function(msg) {
            window.__yamilDialogResult = { type: 'alert', message: msg };
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
          setTimeout(() => {
            window.alert = origAlert;
            window.confirm = origConfirm;
            window.prompt = origPrompt;
          }, 30000);
          return { handler: 'set', action: ${JSON.stringify(action || 'accept')} };
        })()`)
          .then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /screenshot-element ──────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/screenshot-element') {
      readBody(req, body => {
        const { selector } = body
        if (!selector) { json(res, { error: 'selector required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            const wv = window._yamil && window._yamil.getActiveWebview()
            if (!wv) return Promise.resolve(null)
            return wv.executeJavaScript(${JSON.stringify(`(function(){
              const el = document.querySelector(${JSON.stringify(selector)});
              if (!el) return null;
              el.scrollIntoView({ block: 'center', behavior: 'instant' });
              const r = el.getBoundingClientRect();
              return { x: Math.round(r.x), y: Math.round(r.y), width: Math.round(r.width), height: Math.round(r.height) };
            })()`)}).then(rect => {
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
    if (req.method === 'POST' && url.pathname === '/print-pdf') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(`
        (function() {
          const wv = window._yamil && window._yamil.getActiveWebview()
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
    if (req.method === 'POST' && url.pathname === '/drag') {
      readBody(req, body => {
        const { sourceSelector, targetSelector } = body
        if (!sourceSelector || !targetSelector) { json(res, { error: 'sourceSelector and targetSelector required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        execInActiveWebview(`(function(){
          const src = document.querySelector(${JSON.stringify(sourceSelector)});
          const tgt = document.querySelector(${JSON.stringify(targetSelector)});
          if (!src) return { error: 'source not found' };
          if (!tgt) return { error: 'target not found' };
          const dt = new DataTransfer();
          src.dispatchEvent(new DragEvent('dragstart', { bubbles: true, cancelable: true, dataTransfer: dt }));
          tgt.dispatchEvent(new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer: dt }));
          tgt.dispatchEvent(new DragEvent('dragover',  { bubbles: true, cancelable: true, dataTransfer: dt }));
          tgt.dispatchEvent(new DragEvent('drop',      { bubbles: true, cancelable: true, dataTransfer: dt }));
          src.dispatchEvent(new DragEvent('dragend',   { bubbles: true, cancelable: true, dataTransfer: dt }));
          return { ok: true };
        })()`)
          .then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── TAB MANAGEMENT ENDPOINTS ─────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    // ── GET /tabs ─────────────────────────────────────────────────
    // List all open tabs with index, url, title, active status
    if (req.method === 'GET' && url.pathname === '/tabs') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(`
        (function() {
          if (!window._yamil) return []
          var activeId = null
          for (var i = 0; i < window._yamil.tabs.length; i++) {
            if (window._yamil.tabs[i].webview && window._yamil.tabs[i].webview.classList.contains('active')) {
              activeId = window._yamil.tabs[i].id
              break
            }
          }
          return window._yamil.tabs.map(function(t, i) {
            return { index: i, id: t.id, url: t.url || '', title: t.title || '', active: t.id === activeId }
          })
        })()
      `).then(tabs => json(res, { tabs: tabs || [] }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /new-tab ─────────────────────────────────────────────
    // Create a new tab, optionally with a URL
    if (req.method === 'POST' && url.pathname === '/new-tab') {
      readBody(req, body => {
        const tabUrl = body.url || ''
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            if (!window._yamil) return { error: 'tabs not ready' }
            const tab = window._yamil.createTab(${JSON.stringify(tabUrl)} || undefined, true)
            return { ok: true, id: tab.id, url: tab.url }
          })()
        `).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /switch-tab ──────────────────────────────────────────
    // Switch to a tab by index or id
    if (req.method === 'POST' && url.pathname === '/switch-tab') {
      readBody(req, body => {
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        const switchId = body.id != null ? body.id : null
        const switchIdx = body.index != null ? body.index : null
        mainWindow.webContents.executeJavaScript(`
          (function() {
            if (!window._yamil) return { error: 'tabs not ready' }
            var tabs = window._yamil.tabs
            var target = null
            var wantId = ${switchId != null ? JSON.stringify(switchId) : 'null'}
            var wantIdx = ${switchIdx != null ? JSON.stringify(switchIdx) : 'null'}
            if (wantId != null) {
              for (var i = 0; i < tabs.length; i++) { if (tabs[i].id === wantId) { target = tabs[i]; break } }
            } else if (wantIdx != null) {
              target = tabs[wantIdx] || null
            }
            if (!target) return { error: 'tab not found' }
            window._yamil.switchTab(target.id)
            return { ok: true, id: target.id, url: target.url, title: target.title }
          })()
        `).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /close-tab ───────────────────────────────────────────
    // Close a tab by index or id (defaults to active tab)
    if (req.method === 'POST' && url.pathname === '/close-tab') {
      readBody(req, body => {
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        const closeId = body.id != null ? body.id : null
        const closeIdx = body.index != null ? body.index : null
        mainWindow.webContents.executeJavaScript(`
          (function() {
            if (!window._yamil) return { error: 'tabs not ready' }
            var tabs = window._yamil.tabs
            var target = null
            var wantId = ${closeId != null ? JSON.stringify(closeId) : 'null'}
            var wantIdx = ${closeIdx != null ? JSON.stringify(closeIdx) : 'null'}
            if (wantId != null) {
              for (var i = 0; i < tabs.length; i++) { if (tabs[i].id === wantId) { target = tabs[i]; break } }
            } else if (wantIdx != null) {
              target = tabs[wantIdx] || null
            } else {
              for (var i = 0; i < tabs.length; i++) {
                if (tabs[i].webview && tabs[i].webview.classList.contains('active')) { target = tabs[i]; break }
              }
            }
            if (!target) return { error: 'tab not found' }
            window._yamil.closeTab(target.id)
            return { ok: true, remaining: window._yamil.tabs.length }
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
      webviewTag: true,
      sandbox: false,
    },
  })

  mainWindow.loadFile('renderer/index.html')
  mainWindow.setTitle(APP_TITLE)

  mainWindow.on('resize', saveWindowState)
  mainWindow.on('move',   saveWindowState)

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

// Accept self-signed certificates for local/private network addresses
app.on('certificate-error', (event, webContents, url, error, certificate, callback) => {
  try {
    const u = new URL(url)
    const host = u.hostname
    const isLocal = host === 'localhost' || host === '127.0.0.1' ||
      host.startsWith('192.168.') || host.startsWith('10.') ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(host)
    if (isLocal) {
      event.preventDefault()
      callback(true)
      return
    }
  } catch (_) { /* ignore parse errors */ }
  callback(false)
})

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
  if (!tray && process.platform !== 'darwin') app.quit()
})

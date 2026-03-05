const { app, BrowserWindow, ipcMain, Tray, Menu, nativeImage } = require('electron')
const path = require('path')
const http  = require('http')
const fs    = require('fs')

const APP_TITLE    = process.env.APP_TITLE || 'YAMIL Browser'
const CTRL_PORT    = parseInt(process.env.CTRL_PORT || '9300', 10)
const BROWSER_SVC  = process.env.BROWSER_SERVICE || 'http://127.0.0.1:4000'
const START_MINIMIZED = process.argv.includes('--minimized')


let mainWindow
let tray = null

// ── Window control IPC ──────────────────────────────────────────────
ipcMain.on('toggle-fullscreen', () => {
  if (mainWindow) mainWindow.setFullScreen(!mainWindow.isFullScreen())
})
ipcMain.on('window-minimize', () => { if (mainWindow) mainWindow.minimize() })
ipcMain.on('window-maximize', () => {
  if (mainWindow) {
    if (mainWindow.isMaximized()) mainWindow.unmaximize()
    else mainWindow.maximize()
  }
})
ipcMain.on('window-close', () => { if (mainWindow) mainWindow.close() })

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

// ── Browser-service proxy helpers ─────────────────────────────────────

function browserServiceGet (path) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, BROWSER_SVC)
    http.get(url.toString(), (res) => {
      let data = ''
      res.on('data', c => { data += c })
      res.on('end', () => {
        try { resolve({ status: res.statusCode, headers: res.headers, body: data, json: JSON.parse(data) }) }
        catch { resolve({ status: res.statusCode, headers: res.headers, body: data, json: null }) }
      })
    }).on('error', reject)
  })
}

function browserServicePost (path, body = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, BROWSER_SVC)
    const payload = JSON.stringify(body)
    const req = http.request(url.toString(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
    }, (res) => {
      let data = ''
      res.on('data', c => { data += c })
      res.on('end', () => {
        try { resolve({ status: res.statusCode, headers: res.headers, body: data, json: JSON.parse(data) }) }
        catch { resolve({ status: res.statusCode, headers: res.headers, body: data, json: null }) }
      })
    })
    req.on('error', reject)
    req.end(payload)
  })
}

function browserServiceDelete (path) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, BROWSER_SVC)
    const req = http.request(url.toString(), { method: 'DELETE' }, (res) => {
      let data = ''
      res.on('data', c => { data += c })
      res.on('end', () => resolve({ status: res.statusCode, body: data }))
    })
    req.on('error', reject)
    req.end()
  })
}

function browserServiceRaw (method, path, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, BROWSER_SVC)
    const opts = { method, headers: {} }
    let payload = null
    if (body !== undefined) {
      payload = JSON.stringify(body)
      opts.headers['Content-Type'] = 'application/json'
      opts.headers['Content-Length'] = Buffer.byteLength(payload)
    }
    const req = http.request(url.toString(), opts, (res) => {
      const chunks = []
      res.on('data', c => chunks.push(c))
      res.on('end', () => {
        const buf = Buffer.concat(chunks)
        resolve({ status: res.statusCode, headers: res.headers, buf })
      })
    })
    req.on('error', reject)
    if (payload) req.write(payload)
    req.end()
  })
}

// Get active tab info from renderer
function getActiveTabInfo () {
  if (!mainWindow) return Promise.resolve(null)
  return mainWindow.webContents.executeJavaScript(
    `(function(){
      return window._yamil && window._yamil.getActiveTabInfo ? window._yamil.getActiveTabInfo() : null
    })()`
  )
}

// ── HTTP control server on port 9300 ─────────────────────────────────

function startControlServer () {
  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin',  '*')
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
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

    // ── POST /clear-cache ───────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/clear-cache') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.session.clearCache()
        .then(() => mainWindow.webContents.session.clearStorageData({ storages: ['cachestorage'] }))
        .then(() => {
          // Reload the active webview without cache
          mainWindow.webContents.executeJavaScript(
            `(function(){
              const wv = window._yamil && window._yamil.getActiveWebview()
              if(wv) wv.reloadIgnoringCache()
              return !!wv
            })()`
          ).then(ok => json(res, { ok, cleared: true }))
            .catch(e => json(res, { error: e.message }, 500))
        })
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── GET /active-tab-info ─────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/active-tab-info') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      getActiveTabInfo()
        .then(info => json(res, info || { error: 'no tab info' }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /new-stealth-tab ─────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/new-stealth-tab') {
      readBody(req, body => {
        const tabUrl = body.url || ''
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            if (!window._yamil) return { error: 'tabs not ready' }
            const tab = window._yamil.createTab(${JSON.stringify(tabUrl)} || undefined, true, 'stealth')
            return { ok: true, id: tab.id, type: 'stealth', url: tab.url }
          })()
        `).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── GET /url ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/url') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      getActiveTabInfo().then(async (info) => {
        if (info && info.type === 'stealth' && info.sessionId) {
          try {
            const r = await browserServiceGet(`/sessions/${info.sessionId}/url`)
            json(res, r.json || { url: info.url })
          } catch { json(res, { url: info.url }) }
        } else {
          getActiveWebviewUrl()
            .then(u => json(res, { url: u }))
            .catch(e => json(res, { error: e.message }, 500))
        }
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /navigate ────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/navigate') {
      readBody(req, body => {
        const { url: navUrl } = body
        if (!navUrl) { json(res, { error: 'url required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        focusWindow()
        getActiveTabInfo().then(async (info) => {
          if (info && info.type === 'stealth' && info.sessionId) {
            try {
              const r = await browserServicePost(`/sessions/${info.sessionId}/navigate`, { url: navUrl })
              json(res, r.json || { ok: true })
            } catch (e) { json(res, { error: e.message }, 500) }
          } else {
            mainWindow.webContents.executeJavaScript(
              `(function(){
                const wv = window._yamil && window._yamil.getActiveWebview()
                if(wv) wv.loadURL(${JSON.stringify(navUrl)})
                return !!wv
              })()`
            ).then(ok => json(res, { ok }))
              .catch(e => json(res, { error: e.message }, 500))
          }
        }).catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── GET /screenshot ──────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/screenshot') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      getActiveTabInfo().then(async (info) => {
        if (info && info.type === 'stealth' && info.sessionId) {
          try {
            const r = await browserServiceRaw('GET', `/sessions/${info.sessionId}/screenshot`)
            res.setHeader('Content-Type', r.headers['content-type'] || 'image/jpeg')
            res.writeHead(r.status)
            res.end(r.buf)
          } catch (e) { json(res, { error: e.message }, 500) }
        } else {
          captureActiveWebview().then(dataUrl => {
            if (!dataUrl) { json(res, { error: 'webview not ready' }, 503); return }
            const base64 = dataUrl.replace(/^data:image\/\w+;base64,/, '')
            res.setHeader('Content-Type', 'image/png')
            res.writeHead(200)
            res.end(Buffer.from(base64, 'base64'))
          }).catch(e => json(res, { error: e.message }, 500))
        }
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── GET /dom ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/dom') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      getActiveTabInfo().then(async (info) => {
        if (info && info.type === 'stealth' && info.sessionId) {
          // For stealth tabs, use browser-service evaluate
          try {
            const r = await browserServicePost(`/sessions/${info.sessionId}/evaluate`, {
              script: `({
                url:      location.href,
                title:    document.title,
                text:     document.body.innerText.slice(0, 8000),
                inputs:   Array.from(document.querySelectorAll('input,textarea,select')).slice(0,50).map(el=>({tag:el.tagName,type:el.type||null,name:el.name||null,placeholder:el.placeholder||null,id:el.id||null})),
                buttons:  Array.from(document.querySelectorAll('button,[role=button],a')).slice(0,80).map(el=>({tag:el.tagName,text:(el.innerText||el.getAttribute('aria-label')||'').slice(0,80),href:el.href||null,id:el.id||null})),
              })`
            })
            json(res, r.json?.result || {})
          } catch (e) { json(res, { error: e.message }, 500) }
        } else {
          execInActiveWebview(`({
            url:      location.href,
            title:    document.title,
            text:     document.body.innerText.slice(0, 8000),
            inputs:   Array.from(document.querySelectorAll('input,textarea,select')).slice(0,50).map(el=>({tag:el.tagName,type:el.type||null,name:el.name||null,placeholder:el.placeholder||null,id:el.id||null})),
            buttons:  Array.from(document.querySelectorAll('button,[role=button],a')).slice(0,80).map(el=>({tag:el.tagName,text:(el.innerText||el.getAttribute('aria-label')||'').slice(0,80),href:el.href||null,id:el.id||null})),
          })`)
            .then(d => json(res, d || {}))
            .catch(e => json(res, { error: e.message }, 500))
        }
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /eval ─────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/eval') {
      readBody(req, body => {
        const { script } = body
        if (!script) { json(res, { error: 'script required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        getActiveTabInfo().then(async (info) => {
          if (info && info.type === 'stealth' && info.sessionId) {
            try {
              const r = await browserServicePost(`/sessions/${info.sessionId}/evaluate`, { script })
              json(res, { result: r.json?.result })
            } catch (e) { json(res, { error: e.message }, 500) }
          } else {
            execInActiveWebview(script)
              .then(result => json(res, { result }))
              .catch(e => json(res, { error: e.message }, 500))
          }
        }).catch(e => json(res, { error: e.message }, 500))
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
        getActiveTabInfo().then(async (info) => {
          if (info && info.type === 'stealth' && info.sessionId) {
            try {
              const r = await browserServicePost(`/sessions/${info.sessionId}/dialog`, { accept: action === 'accept', promptText })
              json(res, r.json || { ok: true })
            } catch (e) { json(res, { error: e.message }, 500) }
            return
          }
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
        }).catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /screenshot-element ──────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/screenshot-element') {
      readBody(req, body => {
        const { selector } = body
        if (!selector) { json(res, { error: 'selector required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        getActiveTabInfo().then(async (info) => {
          if (info && info.type === 'stealth' && info.sessionId) {
            try {
              const r = await browserServiceRaw('POST', `/sessions/${info.sessionId}/screenshot-element`, { selector })
              if (r.status >= 400) { json(res, { error: 'element not found' }, r.status); return }
              res.setHeader('Content-Type', r.headers['content-type'] || 'image/jpeg')
              res.writeHead(r.status)
              res.end(r.buf)
            } catch (e) { json(res, { error: e.message }, 500) }
          } else {
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
          }
        }).catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /print-pdf ──────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/print-pdf') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      getActiveTabInfo().then(async (info) => {
        if (info && info.type === 'stealth' && info.sessionId) {
          try {
            const r = await browserServiceRaw('POST', `/sessions/${info.sessionId}/pdf`, {})
            res.setHeader('Content-Type', 'application/pdf')
            res.writeHead(r.status)
            res.end(r.buf)
          } catch (e) { json(res, { error: e.message }, 500) }
        } else {
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
        }
      }).catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /drag ────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/drag') {
      readBody(req, body => {
        const { sourceSelector, targetSelector } = body
        if (!sourceSelector || !targetSelector) { json(res, { error: 'sourceSelector and targetSelector required' }, 400); return }
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        getActiveTabInfo().then(async (info) => {
          if (info && info.type === 'stealth' && info.sessionId) {
            try {
              const r = await browserServicePost(`/sessions/${info.sessionId}/drag`, { sourceSelector, targetSelector })
              json(res, r.json || { ok: true })
            } catch (e) { json(res, { error: e.message }, 500) }
          } else {
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
          }
        }).catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── TAB MANAGEMENT ENDPOINTS ─────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    // ── GET /tabs ─────────────────────────────────────────────────
    // List all open tabs with index, url, title, type, active status
    if (req.method === 'GET' && url.pathname === '/tabs') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(`
        (function() {
          if (!window._yamil) return []
          var info = window._yamil.getActiveTabInfo && window._yamil.getActiveTabInfo()
          var activeId = info ? info.id : null
          return window._yamil.tabs.map(function(t, i) {
            return { index: i, id: t.id, type: t.type || 'yamil', sessionId: t.sessionId || null, url: t.url || '', title: t.title || '', active: t.id === activeId }
          })
        })()
      `).then(tabs => json(res, { tabs: tabs || [] }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /new-tab ─────────────────────────────────────────────
    // Create a new tab, optionally with a URL and type ('yamil' | 'stealth')
    if (req.method === 'POST' && url.pathname === '/new-tab') {
      readBody(req, body => {
        const tabUrl = body.url || ''
        const tabType = body.type || 'yamil'
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            if (!window._yamil) return { error: 'tabs not ready' }
            const tab = window._yamil.createTab(${JSON.stringify(tabUrl)} || undefined, true, ${JSON.stringify(tabType)})
            return { ok: true, id: tab.id, type: tab.type, url: tab.url }
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

    // ═══════════════════════════════════════════════════════════════
    // ── BOOKMARK ENDPOINTS ──────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    // ── GET /bookmarks ─────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/bookmarks') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      const query = url.searchParams.get('query') || ''
      const script = query
        ? `(function(){ return window._yamil && window._yamil.bookmarks ? window._yamil.bookmarks.search(${JSON.stringify(query)}) : [] })()`
        : `(function(){ return window._yamil && window._yamil.bookmarks ? window._yamil.bookmarks.getAll() : [] })()`
      mainWindow.webContents.executeJavaScript(script)
        .then(bookmarks => json(res, { bookmarks: bookmarks || [] }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── POST /bookmarks ─────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/bookmarks') {
      readBody(req, body => {
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        const { url: bmUrl, title, tags, category, favicon } = body
        if (!bmUrl) { json(res, { error: 'url required' }, 400); return }
        mainWindow.webContents.executeJavaScript(`
          (function() {
            if (!window._yamil || !window._yamil.bookmarks) return { error: 'bookmarks not ready' }
            var bm = window._yamil.bookmarks.add(${JSON.stringify({ url: bmUrl, title: title || bmUrl, tags: tags || [], category: category || '', favicon: favicon || '' })})
            if (typeof updateBookmarkStar === 'function') updateBookmarkStar()
            if (typeof renderBookmarkBar === 'function') renderBookmarkBar()
            return { ok: true, bookmark: bm }
          })()
        `).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── DELETE /bookmarks ────────────────────────────────────────
    if (req.method === 'DELETE' && url.pathname === '/bookmarks') {
      readBody(req, body => {
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        const { id, url: bmUrl } = body
        if (!id && !bmUrl) { json(res, { error: 'id or url required' }, 400); return }
        const script = id
          ? `(function(){ if(!window._yamil||!window._yamil.bookmarks) return {error:'not ready'}; window._yamil.bookmarks.remove(${JSON.stringify(id)}); if(typeof updateBookmarkStar==='function') updateBookmarkStar(); if(typeof renderBookmarkBar==='function') renderBookmarkBar(); return {ok:true} })()`
          : `(function(){ if(!window._yamil||!window._yamil.bookmarks) return {error:'not ready'}; window._yamil.bookmarks.removeByUrl(${JSON.stringify(bmUrl)}); if(typeof updateBookmarkStar==='function') updateBookmarkStar(); if(typeof renderBookmarkBar==='function') renderBookmarkBar(); return {ok:true} })()`
        mainWindow.webContents.executeJavaScript(script)
          .then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── HISTORY ENDPOINTS ────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    // ── GET /history ─────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/history') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      const query = url.searchParams.get('query') || ''
      const script = query
        ? `(function(){ return window._yamil && window._yamil.history ? window._yamil.history.search(${JSON.stringify(query)}) : [] })()`
        : `(function(){ return window._yamil && window._yamil.history ? window._yamil.history.getAll() : [] })()`
      mainWindow.webContents.executeJavaScript(script)
        .then(history => json(res, { history: history || [] }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── DELETE /history ──────────────────────────────────────────
    if (req.method === 'DELETE' && url.pathname === '/history') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.webContents.executeJavaScript(
        `(function(){ if(window._yamil && window._yamil.history) { window._yamil.history.clear(); return {ok:true} } return {error:'not ready'} })()`
      ).then(result => json(res, result))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── ZOOM ENDPOINTS ──────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    // ── POST /zoom ──────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/zoom') {
      readBody(req, body => {
        if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
        const { action } = body // 'in', 'out', 'reset'
        const fn = action === 'in' ? 'zoomIn' : action === 'out' ? 'zoomOut' : 'zoomReset'
        mainWindow.webContents.executeJavaScript(
          `(function(){ if(window._yamil && window._yamil.zoom) { window._yamil.zoom.${fn}(); return {ok:true, zoom: window._yamil.zoom.getZoom()} } return {error:'not ready'} })()`
        ).then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ── POST /fullscreen ─────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/fullscreen') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      mainWindow.setFullScreen(!mainWindow.isFullScreen())
      json(res, { ok: true, fullscreen: mainWindow.isFullScreen() })
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

  const isMac = process.platform === 'darwin'
  const isWin = process.platform === 'win32'

  mainWindow = new BrowserWindow({
    width:  state.width  || 1440,
    height: state.height || 900,
    x: state.x,
    y: state.y,
    minWidth:  900,
    minHeight: 600,
    backgroundColor: '#0f172a',
    icon: path.join(__dirname, 'assets', isMac ? 'icon.icns' : isWin ? 'icon.ico' : 'icon.png'),
    titleBarStyle: isMac ? 'hiddenInset' : 'default',
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      webviewTag: true,
      sandbox: false,
    },
  })

  // Hide menu bar on Windows/Linux for a clean browser look (Alt shows it)
  if (!isMac) mainWindow.setMenuBarVisibility(false)

  mainWindow.loadFile('renderer/index.html')
  mainWindow.setTitle(APP_TITLE)

  mainWindow.on('resize', saveWindowState)
  mainWindow.on('move',   saveWindowState)

  mainWindow.on('enter-full-screen', () => {
    mainWindow.webContents.send('fullscreen-changed', true)
  })
  mainWindow.on('leave-full-screen', () => {
    mainWindow.webContents.send('fullscreen-changed', false)
  })

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

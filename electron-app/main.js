const { app, BaseWindow, WebContentsView, ipcMain, Tray, Menu, nativeImage, session, safeStorage } = require('electron')
const path = require('path')
const http  = require('http')
const fs    = require('fs')

const { AdBlocker } = require('./adblocker')
const adBlocker = new AdBlocker()

// Suppress Electron CSP warnings for third-party web pages loaded in tabs
// (our own toolbar renderer has a proper CSP via meta tag)
process.env.ELECTRON_DISABLE_SECURITY_WARNINGS = 'true'

const APP_TITLE    = process.env.APP_TITLE || 'YAMIL Browser'
const CTRL_PORT    = parseInt(process.env.CTRL_PORT || '9300', 10)
const BROWSER_SVC  = process.env.BROWSER_SERVICE || 'http://127.0.0.1:4000'
const START_MINIMIZED = process.argv.includes('--minimized')

// ── Chrome-compatible rendering flags ───────────────────────────────
// Match Chrome's rendering pipeline so pages look identical

// GPU & compositing
app.commandLine.appendSwitch('enable-gpu-rasterization')
app.commandLine.appendSwitch('enable-zero-copy')
app.commandLine.appendSwitch('ignore-gpu-blocklist')
app.commandLine.appendSwitch('force_high_performance_gpu')

// ANGLE backend: Metal on macOS (Chrome default since ~113)
if (process.platform === 'darwin') {
  app.commandLine.appendSwitch('use-angle', 'metal')
}

// Font rendering: LCD subpixel text on Windows/Linux
app.commandLine.appendSwitch('enable-lcd-text')

// Smooth scrolling (Chrome default)
app.commandLine.appendSwitch('enable-smooth-scrolling')

// Prevent background throttling (important for browser with multiple tabs)
app.commandLine.appendSwitch('disable-renderer-backgrounding')
app.commandLine.appendSwitch('disable-background-timer-throttling')

// Combined --enable-features (MUST be a single call, comma-separated)
app.commandLine.appendSwitch('enable-features', [
  'SharedArrayBuffer',
  'OverlayScrollbar',
  'OverlayScrollbarFlashAfterAnyScrollUpdate',
  'OverlayScrollbarFlashWhenMouseEnter',
  'BackForwardCache',
  'CanvasOopRasterization',
  'WebAssemblyLazyCompilation',
  'PlatformHEVCDecoderSupport',
].join(','))

// Disable features that hurt Electron browser apps
app.commandLine.appendSwitch('disable-features', [
  'WebContentsOcclusion',  // Prevent hidden window from freezing renderers
].join(','))

// ── Gracefully handle EPIPE (broken pipe when MCP/stdio closes) ─────
process.on('uncaughtException', (err) => {
  if (err.code === 'EPIPE' || err.message?.includes('EPIPE')) return
  console.error('[YAMIL] Uncaught exception:', err)
})
process.stdout?.on('error', (err) => { if (err.code !== 'EPIPE') throw err })
process.stderr?.on('error', (err) => { if (err.code !== 'EPIPE') throw err })

let mainWindow     // BaseWindow
let toolbarView    // WebContentsView for toolbar UI (tab bar, navbar, sidebar, status bar)
let tray = null

// ── Chrome UA for spoofing ──────────────────────────────────────────
const CHROME_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'

// ══════════════════════════════════════════════════════════════════════
// ── TAB MANAGER (WebContentsView-based) ─────────────────────────────
// ══════════════════════════════════════════════════════════════════════

class TabManager {
  constructor () {
    this.tabs = new Map()   // id → { view: WebContentsView, title, url, favicon, zoom, type }
    this.activeTabId = null
    this.nextId = 1
    this.sidebarOpen = true
    this.sidebarWidth = 300
    this.consoleLogs = []       // circular buffer of { ts, level, message, source, line }
    this.consoleLogsMax = 500
  }

  createTab (url, activate = true, type = 'yamil') {
    const id = this.nextId++
    url = url || 'https://yamil-ai.com'

    if (type === 'yamil') {
      const yamilSession = session.fromPartition('persist:yamil')

      const view = new WebContentsView({
        webPreferences: {
          session: yamilSession,
          contextIsolation: true,
          nodeIntegration: false,
          sandbox: false,
          webviewTag: false,
        },
      })

      const wc = view.webContents
      wc.setUserAgent(CHROME_UA)

      const tab = { id, view, title: 'New Tab', url, favicon: null, zoom: 0, type: 'yamil' }
      this.tabs.set(id, tab)

      // Wire webContents events
      this._wireEvents(tab)

      // Load URL
      wc.loadURL(url)

      // Add to window but hidden
      if (mainWindow) {
        mainWindow.contentView.addChildView(view)
        view.setVisible(false)
      }

      if (activate) this.switchTab(id)

      // Notify toolbar
      this._sendTabEvent('tab-created', { tabId: id, url, type: 'yamil' })

      return { id, url, type: 'yamil' }
    }

    if (type === 'stealth') {
      // Stealth tabs use browser-service (no WebContentsView)
      const tab = { id, view: null, title: 'Stealth Tab', url, favicon: null, zoom: 0, type: 'stealth', sessionId: null }
      this.tabs.set(id, tab)
      if (activate) this.switchTab(id)
      this._sendTabEvent('tab-created', { tabId: id, url, type: 'stealth' })
      return { id, url, type: 'stealth' }
    }

    return null
  }

  switchTab (id) {
    const tab = this.tabs.get(id)
    if (!tab) return

    const prevId = this.activeTabId
    this.activeTabId = id

    // Hide all tab views, show the active one
    for (const [tid, t] of this.tabs) {
      if (t.view) {
        if (tid === id) {
          t.view.setVisible(true)
          // Tab view on top — sized to viewport area only, so toolbar
          // (tab bar, navbar, sidebar, status bar) remains visible around it
          if (mainWindow) {
            mainWindow.contentView.removeChildView(t.view)
            mainWindow.contentView.addChildView(t.view) // append = topmost z-order
          }
        } else {
          t.view.setVisible(false)
        }
      }
    }

    this.layoutViews()

    // Notify toolbar
    this._sendTabEvent('tab-switched', {
      tabId: id,
      url: tab.url,
      title: tab.title,
      type: tab.type,
      canGoBack: tab.view ? tab.view.webContents.canGoBack() : false,
      canGoForward: tab.view ? tab.view.webContents.canGoForward() : false,
    })
  }

  closeTab (id) {
    const tab = this.tabs.get(id)
    if (!tab) return

    // If closing the last tab, create a new one first
    if (this.tabs.size === 1) {
      this.createTab('https://yamil-ai.com', true)
    }

    // Remove view
    if (tab.view) {
      if (mainWindow) mainWindow.contentView.removeChildView(tab.view)
      tab.view.webContents.close()
    }

    // Clean up stealth session
    if (tab.type === 'stealth' && tab.sessionId) {
      browserServiceDelete(`/sessions/${tab.sessionId}`).catch(() => {})
    }

    this.tabs.delete(id)

    // If we closed the active tab, switch to nearest
    if (this.activeTabId === id) {
      const ids = [...this.tabs.keys()]
      if (ids.length > 0) this.switchTab(ids[ids.length - 1])
    }

    this._sendTabEvent('tab-closed', { tabId: id, remaining: this.tabs.size })
  }

  getActiveView () {
    const tab = this.tabs.get(this.activeTabId)
    return (tab && tab.view) ? tab.view : null
  }

  getActiveTab () {
    return this.tabs.get(this.activeTabId) || null
  }

  getActiveTabInfo () {
    const tab = this.getActiveTab()
    if (!tab) return null
    return { id: tab.id, type: tab.type, url: tab.url, title: tab.title, sessionId: tab.sessionId || null }
  }

  getTabList () {
    const result = []
    for (const [id, tab] of this.tabs) {
      result.push({
        id,
        type: tab.type,
        url: tab.url,
        title: tab.title,
        active: id === this.activeTabId,
        sessionId: tab.sessionId || null,
      })
    }
    return result
  }

  // ── Layout: position tab views in the viewport area ──────────────

  layoutViews () {
    if (!mainWindow) return
    const bounds = mainWindow.getContentBounds()
    const { width, height } = bounds

    // Toolbar spans full window
    if (toolbarView) {
      toolbarView.setBounds({ x: 0, y: 0, width, height })
    }

    // Calculate viewport area (below tab bar + navbar, above status bar)
    const TAB_BAR_H = 34
    const NAVBAR_H = 42
    const STATUS_H = 24
    const topOffset = TAB_BAR_H + NAVBAR_H
    const sidebarW = this.sidebarOpen ? this.sidebarWidth : 0

    const tabBounds = {
      x: 0,
      y: topOffset,
      width: width - sidebarW,
      height: height - topOffset - STATUS_H,
    }

    // Position the active tab view
    for (const [id, tab] of this.tabs) {
      if (tab.view) {
        if (id === this.activeTabId) {
          tab.view.setBounds(tabBounds)
        }
      }
    }
  }

  setSidebarOpen (open) {
    this.sidebarOpen = open
    this.layoutViews()
  }

  setBookmarkBarVisible (visible) {
    // Adjust layout when bookmark bar is toggled
    this._bookmarkBarVisible = visible
    this.layoutViews()
  }

  // Override layoutViews to account for bookmark bar
  get _topOffset () {
    const TAB_BAR_H = 34
    const NAVBAR_H = 42
    const BMBAR_H = this._bookmarkBarVisible ? 28 : 0
    return TAB_BAR_H + NAVBAR_H + BMBAR_H
  }

  // ── Wire webContents events for a tab ────────────────────────────

  _wireEvents (tab) {
    const wc = tab.view.webContents

    wc.on('did-start-loading', () => {
      this._sendTabEvent('loading', { tabId: tab.id })
    })

    wc.on('did-stop-loading', () => {
      this._sendTabEvent('loaded', { tabId: tab.id })
      // Inject credential watcher
      this._injectCredentialWatcher(tab)
    })

    wc.on('did-navigate', (_e, url) => {
      tab.url = url
      this._sendTabEvent('navigated', {
        tabId: tab.id,
        url,
        canGoBack: wc.canGoBack(),
        canGoForward: wc.canGoForward(),
      })
    })

    wc.on('did-navigate-in-page', (_e, url) => {
      tab.url = url
      this._sendTabEvent('url-updated', {
        tabId: tab.id,
        url,
        canGoBack: wc.canGoBack(),
        canGoForward: wc.canGoForward(),
      })
    })

    wc.on('page-title-updated', (_e, title) => {
      tab.title = title
      this._sendTabEvent('title-updated', { tabId: tab.id, title })
    })

    wc.on('page-favicon-updated', (_e, favicons) => {
      if (favicons && favicons.length > 0) {
        tab.favicon = favicons[0]
        this._sendTabEvent('favicon-updated', { tabId: tab.id, favicon: favicons[0] })
      }
    })

    wc.on('did-fail-load', (_e, errorCode, errorDescription) => {
      if (errorCode !== -3) {
        this._sendTabEvent('load-error', { tabId: tab.id, errorCode, errorDescription })
      }
    })

    wc.on('did-finish-load', () => {
      // Check for autofill
      this._checkAutofill(tab)
    })

    // Handle target="_blank" links by opening in new tab
    wc.setWindowOpenHandler(({ url }) => {
      this.createTab(url, true)
      return { action: 'deny' }
    })

    // Find in page results
    wc.on('found-in-page', (_e, result) => {
      if (tab.id === this.activeTabId) {
        this._sendTabEvent('find-result', {
          tabId: tab.id,
          activeMatchOrdinal: result.activeMatchOrdinal,
          matches: result.matches,
        })
      }
    })

    // Intercept keyboard shortcuts from tab webContents and forward to toolbar
    // Mirrors Chrome's shortcuts so they work even when a web page has focus
    wc.on('before-input-event', (event, input) => {
      if (input.type !== 'keyDown') return
      const meta = input.meta || input.control  // Cmd on Mac, Ctrl on Win/Linux
      const { shift, alt, key } = input
      const k = key.toLowerCase()

      // Helper: prevent default and send menu-action to toolbar
      const send = (action) => { event.preventDefault(); toolbarView?.webContents.send('menu-action', action) }

      // ── Tab management ──────────────────────────────────────────
      if (meta && !shift && k === 't') { send('new-tab'); return }
      if (meta && shift && k === 'n') { send('new-stealth'); return }
      if (meta && !shift && k === 'w') { send('close-tab'); return }
      if (meta && shift && k === 't') { send('restore-tab'); return }

      // Mod+1-8 → switch to tab N, Mod+9 → last tab
      if (meta && !shift && key >= '1' && key <= '9') { send(`switch-tab-${key}`); return }

      // ── Navigation ──────────────────────────────────────────────
      if (meta && !shift && k === 'r') { event.preventDefault(); wc.reload(); return }
      if (meta && shift && k === 'r') { event.preventDefault(); wc.reloadIgnoringCache(); return }
      if (key === 'F5' && !shift) { event.preventDefault(); wc.reload(); return }
      if (key === 'F5' && shift) { event.preventDefault(); wc.reloadIgnoringCache(); return }
      if (meta && !shift && k === 'l') { event.preventDefault(); toolbarView?.webContents.executeJavaScript(`document.getElementById('address-bar')?.focus(); document.getElementById('address-bar')?.select()`); return }
      if (key === 'F6') { event.preventDefault(); toolbarView?.webContents.executeJavaScript(`document.getElementById('address-bar')?.focus(); document.getElementById('address-bar')?.select()`); return }
      if (meta && key === '[') { event.preventDefault(); if (wc.canGoBack()) wc.goBack(); return }
      if (meta && key === ']') { event.preventDefault(); if (wc.canGoForward()) wc.goForward(); return }
      if (alt && key === 'ArrowLeft') { event.preventDefault(); if (wc.canGoBack()) wc.goBack(); return }
      if (alt && key === 'ArrowRight') { event.preventDefault(); if (wc.canGoForward()) wc.goForward(); return }

      // ── Page operations ─────────────────────────────────────────
      if (meta && !shift && k === 'f') { send('find'); return }
      if (meta && !shift && k === 'g') { send('find-next'); return }
      if (meta && shift && k === 'g') { send('find-prev'); return }
      if (meta && !shift && k === 'p') { send('print'); return }
      if (meta && !shift && k === 's') { send('save-page'); return }
      if (meta && !shift && k === 'u') { send('view-source'); return }

      // ── Bookmarks ───────────────────────────────────────────────
      if (meta && !shift && k === 'd') { send('bookmark'); return }
      if (meta && shift && k === 'b') { send('toggle-bookmark-bar'); return }
      if (meta && shift && k === 'o') { send('bookmarks'); return }

      // ── History & Downloads ─────────────────────────────────────
      if (meta && !shift && k === 'h') { send('history'); return }
      if (meta && !shift && k === 'j') { send('downloads'); return }

      // ── Zoom ────────────────────────────────────────────────────
      if (meta && (key === '=' || key === '+')) { send('zoom-in'); return }
      if (meta && key === '-') { send('zoom-out'); return }
      if (meta && key === '0') { send('zoom-reset'); return }

      // ── Developer tools ─────────────────────────────────────────
      if (key === 'F12') { send('dev-tools'); return }
      if ((meta && shift && k === 'i') || (input.meta && alt && k === 'i')) { send('dev-tools'); return }
      if (meta && shift && k === 'j') { send('dev-tools'); return }

      // ── Fullscreen ──────────────────────────────────────────────
      if (key === 'F11') { send('fullscreen'); return }

      // ── Settings ────────────────────────────────────────────────
      if (meta && key === ',') { send('settings'); return }
    })

    // Console messages
    wc.on('console-message', (_e, level, message, line, sourceId) => {
      const levelMap = { 0: 'verbose', 1: 'info', 2: 'warning', 3: 'error' }
      this.consoleLogs.push({
        ts: Date.now(),
        level: levelMap[level] || 'info',
        message,
        source: sourceId || '',
        line: line || 0,
        tabId: tab.id,
      })
      if (this.consoleLogs.length > this.consoleLogsMax) {
        this.consoleLogs = this.consoleLogs.slice(-this.consoleLogsMax)
      }
    })

    // Context menu
    wc.on('context-menu', (_event, params) => {
      this._showContextMenu(tab, params)
    })
  }

  // ── Native context menu (Phase 10) ────────────────────────────────

  _showContextMenu (tab, params) {
    const wc = tab.view.webContents
    const menuItems = []

    if (params.linkURL) {
      menuItems.push({ label: 'Open Link in New Tab', click: () => this.createTab(params.linkURL, true) })
      menuItems.push({ label: 'Copy Link Address', click: () => { require('electron').clipboard.writeText(params.linkURL) } })
      menuItems.push({ type: 'separator' })
    }

    if (params.mediaType === 'image' && params.srcURL) {
      menuItems.push({ label: 'Open Image in New Tab', click: () => this.createTab(params.srcURL, true) })
      menuItems.push({ label: 'Copy Image Address', click: () => { require('electron').clipboard.writeText(params.srcURL) } })
      menuItems.push({ type: 'separator' })
    }

    if (params.selectionText) {
      menuItems.push({ label: 'Copy', click: () => wc.copy() })
      menuItems.push({ type: 'separator' })
    }

    if (params.isEditable) {
      menuItems.push({ label: 'Cut', click: () => wc.cut() })
      menuItems.push({ label: 'Copy', click: () => wc.copy() })
      menuItems.push({ label: 'Paste', click: () => wc.paste() })
      menuItems.push({ label: 'Select All', click: () => wc.selectAll() })
      menuItems.push({ type: 'separator' })
    }

    menuItems.push({ label: 'Back', enabled: wc.canGoBack(), click: () => wc.goBack() })
    menuItems.push({ label: 'Forward', enabled: wc.canGoForward(), click: () => wc.goForward() })
    menuItems.push({ label: 'Reload', click: () => wc.reload() })
    menuItems.push({ type: 'separator' })
    menuItems.push({ label: 'Inspect Element', click: () => wc.inspectElement(params.x, params.y) })

    const menu = Menu.buildFromTemplate(menuItems)
    menu.popup()
  }

  // ── Credential injection (Phase 7) ────────────────────────────────

  _injectCredentialWatcher (tab) {
    const wc = tab.view.webContents
    wc.executeJavaScript(`(function() {
      if (window.__yamil_cred_observer) return;
      window.__yamil_cred_observer = true;

      function watchForms() {
        const pwFields = document.querySelectorAll('input[type="password"]');
        if (!pwFields.length) return;

        pwFields.forEach(pw => {
          if (pw.__yamil_watched) return;
          pw.__yamil_watched = true;

          const form = pw.closest('form') || pw.parentElement?.closest('div');
          if (!form) return;

          function captureAndSave(e) {
            const password = pw.value;
            if (!password) return;
            let username = '';
            const container = pw.closest('form') || document.body;
            const inputs = container.querySelectorAll('input[type="email"], input[type="text"], input[name*="user"], input[name*="email"], input[name*="login"], input[name*="account"], input[autocomplete="username"]');
            for (const inp of inputs) {
              if (inp.value && inp.value.trim()) { username = inp.value.trim(); break; }
            }
            if (!username) {
              for (const inp of document.querySelectorAll('input[type="email"], input[type="text"]')) {
                if (inp.value && inp.value.trim() && inp !== pw && !inp.type.match(/hidden|search/)) {
                  username = inp.value.trim(); break;
                }
              }
            }
            if (!username || !password) return;

            const domain = location.hostname.replace(/^www\\\\./, '');
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
            fetch('http://127.0.0.1:${CTRL_PORT}/credentials/auto-save', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ domain, username, password, formUrl: location.href, formRecipe }),
            }).catch(() => {});
          }

          if (pw.closest('form')) {
            pw.closest('form').addEventListener('submit', captureAndSave, { once: true });
          }
          pw.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') setTimeout(() => captureAndSave(e), 100);
          }, { once: true });

          const btns = (pw.closest('form') || pw.parentElement?.closest('div') || document).querySelectorAll('button[type="submit"], button:not([type]), input[type="submit"], [role="button"]');
          btns.forEach(btn => {
            const txt = (btn.textContent || btn.value || '').toLowerCase();
            if (txt.match(/log.?in|sign.?in|submit|continue|next/)) {
              btn.addEventListener('click', (e) => setTimeout(() => captureAndSave(e), 100), { once: true });
            }
          });
        });
      }

      watchForms();
      let _yamilWatchTimer = null;
      new MutationObserver(() => {
        if (_yamilWatchTimer) return;
        _yamilWatchTimer = setTimeout(() => { _yamilWatchTimer = null; watchForms(); }, 2000);
      }).observe(document.body, { childList: true, subtree: true });
    })()`, true).catch(() => {})
  }

  async _checkAutofill (tab) {
    if (tab.type !== 'yamil' || !tab.view) return
    const wc = tab.view.webContents
    try {
      const hasLogin = await wc.executeJavaScript(`!!document.querySelector('input[type="password"]')`)
      if (!hasLogin) return
      const pageUrl = wc.getURL()
      let domain
      try { domain = new URL(pageUrl).hostname.replace(/^www\./, '') } catch { return }

      // Notify toolbar to check for autofill
      this._sendTabEvent('check-autofill', { tabId: tab.id, domain, url: pageUrl })
    } catch {}
  }

  // ── Send events to toolbar ────────────────────────────────────────

  _sendTabEvent (type, data) {
    if (toolbarView && !toolbarView.webContents.isDestroyed()) {
      toolbarView.webContents.send('tab:event', { type, ...data })
    }
  }
}

const tabManager = new TabManager()

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

// ── Tab IPC handlers (Phase 3) ─────────────────────────────────────

ipcMain.handle('tab:create', (_e, url, type) => {
  return tabManager.createTab(url, true, type || 'yamil')
})

ipcMain.handle('tab:switch', (_e, id) => {
  tabManager.switchTab(id)
  return { ok: true }
})

ipcMain.handle('tab:close', (_e, id) => {
  tabManager.closeTab(id)
  return { ok: true, remaining: tabManager.tabs.size }
})

ipcMain.handle('tab:navigate', (_e, url) => {
  const view = tabManager.getActiveView()
  if (!view) return { error: 'no active tab' }
  view.webContents.loadURL(url)
  return { ok: true }
})

ipcMain.handle('tab:goBack', () => {
  const view = tabManager.getActiveView()
  if (view && view.webContents.canGoBack()) view.webContents.goBack()
  return { ok: true }
})

ipcMain.handle('tab:goForward', () => {
  const view = tabManager.getActiveView()
  if (view && view.webContents.canGoForward()) view.webContents.goForward()
  return { ok: true }
})

ipcMain.handle('tab:reload', () => {
  const view = tabManager.getActiveView()
  if (view) view.webContents.reload()
  return { ok: true }
})

ipcMain.handle('tab:hardReload', () => {
  const view = tabManager.getActiveView()
  if (view) view.webContents.reloadIgnoringCache()
  return { ok: true }
})

ipcMain.handle('tab:eval', (_e, script) => {
  const view = tabManager.getActiveView()
  if (!view) return { error: 'no active tab' }
  return view.webContents.executeJavaScript(script)
})

ipcMain.handle('tab:zoom', (_e, level) => {
  const tab = tabManager.getActiveTab()
  if (!tab || !tab.view) return { error: 'no active tab' }
  tab.zoom = level
  tab.view.webContents.setZoomLevel(level)
  return { ok: true, zoom: level }
})

ipcMain.handle('tab:find', (_e, text, opts) => {
  const view = tabManager.getActiveView()
  if (!view) return { error: 'no active tab' }
  if (text) {
    view.webContents.findInPage(text, opts || {})
  }
  return { ok: true }
})

ipcMain.handle('tab:stopFind', () => {
  const view = tabManager.getActiveView()
  if (view) view.webContents.stopFindInPage('clearSelection')
  return { ok: true }
})

ipcMain.handle('tab:print', () => {
  const view = tabManager.getActiveView()
  if (view) view.webContents.print()
  return { ok: true }
})

ipcMain.handle('tab:devtools', () => {
  const view = tabManager.getActiveView()
  if (!view) return { error: 'no active tab' }
  if (view.webContents.isDevToolsOpened()) {
    view.webContents.closeDevTools()
  } else {
    view.webContents.openDevTools()
  }
  return { ok: true }
})

ipcMain.handle('tab:getInfo', () => {
  return tabManager.getActiveTabInfo()
})

ipcMain.handle('tab:list', () => {
  return tabManager.getTabList()
})

ipcMain.handle('tab:getUrl', () => {
  const tab = tabManager.getActiveTab()
  if (!tab) return { url: null }
  if (tab.view) return { url: tab.view.webContents.getURL() }
  return { url: tab.url }
})

ipcMain.handle('tab:savePageAs', () => {
  const view = tabManager.getActiveView()
  if (!view) return { error: 'no active tab' }
  return view.webContents.executeJavaScript('document.documentElement.outerHTML')
})

ipcMain.handle('tab:copyUrl', () => {
  const tab = tabManager.getActiveTab()
  if (!tab || !tab.view) return { error: 'no active tab' }
  const url = tab.view.webContents.getURL()
  require('electron').clipboard.writeText(url)
  return { ok: true, url }
})

ipcMain.handle('tab:viewSource', () => {
  const tab = tabManager.getActiveTab()
  if (!tab || !tab.view) return { error: 'no active tab' }
  const url = tab.view.webContents.getURL()
  return tabManager.createTab('view-source:' + url, true)
})

// Sidebar state from toolbar
ipcMain.on('sidebar-toggled', (_e, open) => {
  tabManager.setSidebarOpen(open)
})

ipcMain.on('bookmark-bar-toggled', (_e, visible) => {
  tabManager.setBookmarkBarVisible(visible)
})

// Native popup menu for the 3-dot button (avoids WebContentsView z-order issues)
ipcMain.on('show-app-menu', (_e, { x, y, zoomPct }) => {
  if (!mainWindow || !toolbarView) return
  const template = [
    { label: 'New Tab', accelerator: 'CmdOrCtrl+T', click: () => sendMenuAction('new-tab') },
    { label: 'New Stealth Tab', accelerator: 'CmdOrCtrl+Shift+N', click: () => sendMenuAction('new-stealth') },
    { type: 'separator' },
    { label: 'History', accelerator: 'CmdOrCtrl+H', click: () => sendMenuAction('history') },
    { label: 'Bookmarks', accelerator: 'CmdOrCtrl+Shift+O', click: () => sendMenuAction('bookmarks') },
    { label: 'Downloads', click: () => sendMenuAction('downloads') },
    { type: 'separator' },
    { label: 'Find in Page', accelerator: 'CmdOrCtrl+F', click: () => sendMenuAction('find') },
    { label: `Zoom (${zoomPct}%)`, enabled: false },
    { label: 'Zoom In', accelerator: 'CmdOrCtrl+Plus', click: () => sendMenuAction('zoom-in') },
    { label: 'Zoom Out', accelerator: 'CmdOrCtrl+-', click: () => sendMenuAction('zoom-out') },
    { type: 'separator' },
    { label: 'Print', accelerator: 'CmdOrCtrl+P', click: () => sendMenuAction('print') },
    { label: 'Save Page As', accelerator: 'CmdOrCtrl+S', click: () => sendMenuAction('save-page') },
    { label: 'Copy URL', click: () => sendMenuAction('copy-url') },
    { type: 'separator' },
    { label: 'View Page Source', accelerator: 'CmdOrCtrl+U', click: () => sendMenuAction('view-source') },
    { label: 'Developer Tools', accelerator: 'F12', click: () => sendMenuAction('dev-tools') },
    { label: 'Fullscreen', accelerator: 'F11', click: () => sendMenuAction('fullscreen') },
    { type: 'separator' },
    { label: 'Settings', accelerator: 'CmdOrCtrl+,', click: () => sendMenuAction('settings') },
  ]
  const menu = Menu.buildFromTemplate(template)
  menu.popup({ window: mainWindow, x, y })
})

function sendMenuAction (action) {
  if (toolbarView) {
    toolbarView.webContents.send('menu-action', action)
  }
}

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

// ── Helper: run JS in the active tab's webContents (direct!) ────────
// No more renderer IPC chain — this goes straight to the tab.

function execInActiveWebview (script) {
  const view = tabManager.getActiveView()
  if (!view) return Promise.reject(new Error('no active tab'))
  return view.webContents.executeJavaScript(script)
}

function captureActiveWebview ({ quality = 40, maxWidth = 1280, maxBytes = 400_000 } = {}) {
  const view = tabManager.getActiveView()
  if (!view) return Promise.resolve(null)
  return view.webContents.capturePage().then(img => {
    const sz = img.getSize()
    if (!sz.width || !sz.height) return null
    // Cap height
    const maxH = 768
    if (sz.height > maxH) {
      const cropScale = maxH / sz.height
      img = img.resize({ width: Math.round(sz.width * cropScale), height: maxH })
    }
    const cur = img.getSize()
    let w = maxWidth
    let q = quality
    for (let attempt = 0; attempt < 5; attempt++) {
      let ni = img
      if (cur.width > w) {
        const scale = w / cur.width
        ni = ni.resize({ width: w, height: Math.round(cur.height * scale) })
      }
      const jpegBuf = ni.toJPEG(q)
      if (jpegBuf.length <= maxBytes) {
        return 'data:image/jpeg;base64,' + jpegBuf.toString('base64')
      }
      if (q > 30) { q = Math.max(30, q - 20) }
      else { w = Math.round(w * 0.7) }
    }
    const fw = Math.min(w, 640)
    let ni = img.resize({ width: fw, height: Math.round(cur.height * (fw / cur.width)) })
    return 'data:image/jpeg;base64,' + ni.toJPEG(20).toString('base64')
  })
}

function getActiveWebviewUrl () {
  const view = tabManager.getActiveView()
  if (!view) return Promise.resolve(null)
  return Promise.resolve(view.webContents.getURL())
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

    // ── GET /debug-toolbar ─────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/debug-toolbar') {
      if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
      const wc = toolbarView.webContents
      const bounds = toolbarView.getBounds()
      const children = mainWindow ? mainWindow.contentView.children.map((c, i) => ({
        index: i,
        isToolbar: c === toolbarView,
        bounds: c.getBounds(),
        visible: c.getVisible ? c.getVisible() : 'unknown',
      })) : []
      wc.executeJavaScript(`JSON.stringify({
        title: document.title,
        tabBarH: document.getElementById('tab-bar')?.offsetHeight,
        navbarH: document.getElementById('navbar')?.offsetHeight,
        tabCount: document.querySelectorAll('.tab').length,
        bodyBg: getComputedStyle(document.body).background,
        error: window._lastError || null,
      })`).then(info => {
        json(res, { toolbar: { url: wc.getURL(), bounds }, children, rendererInfo: JSON.parse(info) })
      }).catch(err => {
        json(res, { toolbar: { url: wc.getURL(), bounds }, children, evalError: err.message })
      })
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
      const yamilSession = session.fromPartition('persist:yamil')
      yamilSession.clearCache()
        .then(() => yamilSession.clearStorageData({ storages: ['cachestorage'] }))
        .then(() => {
          const view = tabManager.getActiveView()
          if (view) view.webContents.reloadIgnoringCache()
          json(res, { ok: true, cleared: true })
        })
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ── GET /active-tab-info ─────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/active-tab-info') {
      const info = tabManager.getActiveTabInfo()
      json(res, info || { error: 'no tab info' })
      return
    }

    // ── POST /new-stealth-tab ─────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/new-stealth-tab') {
      readBody(req, body => {
        const result = tabManager.createTab(body.url || '', true, 'stealth')
        json(res, result || { error: 'failed to create tab' })
      })
      return
    }

    // ── GET /url ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/url') {
      const info = tabManager.getActiveTabInfo()
      if (info && info.type === 'stealth' && info.sessionId) {
        browserServiceGet(`/sessions/${info.sessionId}/url`)
          .then(r => json(res, r.json || { url: info.url }))
          .catch(() => json(res, { url: info.url }))
      } else {
        getActiveWebviewUrl()
          .then(u => json(res, { url: u }))
          .catch(e => json(res, { error: e.message }, 500))
      }
      return
    }

    // ── POST /navigate ────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/navigate') {
      readBody(req, body => {
        const { url: navUrl } = body
        if (!navUrl) { json(res, { error: 'url required' }, 400); return }
        focusWindow()
        const info = tabManager.getActiveTabInfo()
        if (info && info.type === 'stealth' && info.sessionId) {
          browserServicePost(`/sessions/${info.sessionId}/navigate`, { url: navUrl })
            .then(r => json(res, r.json || { ok: true }))
            .catch(e => json(res, { error: e.message }, 500))
        } else {
          const view = tabManager.getActiveView()
          if (view) {
            view.webContents.loadURL(navUrl)
            json(res, { ok: true })
          } else {
            json(res, { error: 'no active tab' }, 503)
          }
        }
      })
      return
    }

    // ── GET /screenshot ──────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/screenshot') {
      const info = tabManager.getActiveTabInfo()
      if (info && info.type === 'stealth' && info.sessionId) {
        const maxBytes = parseInt(url.searchParams.get('maxBytes')) || 400_000
        const qs = url.search || '?quality=40&scale=0.5'
        browserServiceRaw('GET', `/sessions/${info.sessionId}/screenshot${qs}`)
          .then(r => {
            if (r.buf && r.buf.length > maxBytes) {
              json(res, { error: `Screenshot too large (${(r.buf.length/1024).toFixed(0)}KB). Use yamil_browser_a11y_snapshot instead.` }, 413)
            } else {
              res.setHeader('Content-Type', r.headers['content-type'] || 'image/jpeg')
              res.writeHead(r.status)
              res.end(r.buf)
            }
          })
          .catch(e => json(res, { error: e.message }, 500))
      } else {
        const quality = parseInt(url.searchParams.get('quality')) || 40
        const maxWidth = parseInt(url.searchParams.get('maxWidth')) || 1280
        const maxBytes = parseInt(url.searchParams.get('maxBytes')) || 400_000
        captureActiveWebview({ quality, maxWidth, maxBytes }).then(dataUrl => {
          if (!dataUrl) { json(res, { error: 'webview not ready' }, 503); return }
          const base64 = dataUrl.replace(/^data:image\/\w+;base64,/, '')
          res.setHeader('Content-Type', 'image/jpeg')
          res.writeHead(200)
          res.end(Buffer.from(base64, 'base64'))
        }).catch(e => json(res, { error: e.message }, 500))
      }
      return
    }

    // ── GET /dom ──────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/dom') {
      const info = tabManager.getActiveTabInfo()
      const domScript = `(function(){
        function collectFromDoc(doc) {
          let text = (doc.body?.innerText || '').slice(0, 4000);
          let inputs = Array.from(doc.querySelectorAll('input,textarea,select')).slice(0,30).map(el=>({tag:el.tagName,type:el.type||null,name:el.name||null,placeholder:el.placeholder||null,id:el.id||null}));
          let buttons = Array.from(doc.querySelectorAll('button,[role=button],a')).slice(0,50).map(el=>({tag:el.tagName,text:(el.innerText||el.getAttribute('aria-label')||'').slice(0,80),href:el.href||null,id:el.id||null}));
          const iframes = doc.querySelectorAll('iframe');
          for (const iframe of iframes) {
            try {
              const iDoc = iframe.contentDocument || iframe.contentWindow?.document;
              if (iDoc && iDoc.body) {
                const sub = collectFromDoc(iDoc);
                text += '\\n' + sub.text;
                inputs = inputs.concat(sub.inputs);
                buttons = buttons.concat(sub.buttons);
              }
            } catch(e) {}
          }
          return { text, inputs, buttons };
        }
        const collected = collectFromDoc(document);
        return {
          url:      location.href,
          title:    document.title,
          text:     collected.text.slice(0, 8000),
          inputs:   collected.inputs.slice(0, 50),
          buttons:  collected.buttons.slice(0, 80),
        };
      })()`

      if (info && info.type === 'stealth' && info.sessionId) {
        browserServicePost(`/sessions/${info.sessionId}/evaluate`, { script: domScript })
          .then(r => json(res, r.json?.result || {}))
          .catch(e => json(res, { error: e.message }, 500))
      } else {
        execInActiveWebview(domScript)
          .then(d => json(res, d || {}))
          .catch(e => json(res, { error: e.message }, 500))
      }
      return
    }

    // ── POST /eval ─────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/eval') {
      readBody(req, body => {
        const { script } = body
        if (!script) { json(res, { error: 'script required' }, 400); return }
        const info = tabManager.getActiveTabInfo()
        if (info && info.type === 'stealth' && info.sessionId) {
          browserServicePost(`/sessions/${info.sessionId}/evaluate`, { script })
            .then(r => json(res, { result: r.json?.result }))
            .catch(e => json(res, { error: e.message }, 500))
        } else {
          execInActiveWebview(script)
            .then(result => json(res, { result }))
            .catch(e => json(res, { error: e.message }, 500))
        }
      })
      return
    }

    // ── GET /window-screenshot ────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/window-screenshot') {
      if (!mainWindow) { json(res, { error: 'no window' }, 503); return }
      let quality = parseInt(url.searchParams.get('quality')) || 40
      let maxWidth = parseInt(url.searchParams.get('maxWidth')) || 800
      const maxBytes = parseInt(url.searchParams.get('maxBytes')) || 400_000
      // For BaseWindow, capture the toolbar view
      const captureTarget = toolbarView || mainWindow
      const captureFn = captureTarget.webContents ? captureTarget.webContents.capturePage() : Promise.resolve(null)
      captureFn.then(img => {
        if (!img) { json(res, { error: 'empty capture' }, 503); return }
        const sz = img.getSize()
        if (!sz.width || !sz.height) { json(res, { error: 'empty capture' }, 503); return }
        const maxH = 768
        if (sz.height > maxH) {
          const cropScale = maxH / sz.height
          img = img.resize({ width: Math.round(sz.width * cropScale), height: maxH })
        }
        const cur = img.getSize()
        if (cur.width > maxWidth) {
          const scale = maxWidth / cur.width
          img = img.resize({ width: maxWidth, height: Math.round(cur.height * scale) })
        }
        let buf = img.toJPEG(quality)
        for (let attempt = 0; attempt < 4 && buf.length > maxBytes; attempt++) {
          if (quality > 25) { quality = Math.max(25, quality - 15) }
          else { maxWidth = Math.round(maxWidth * 0.7) ; img = img.resize({ width: maxWidth, height: Math.round(img.getSize().height * 0.7) }) }
          buf = img.toJPEG(quality)
        }
        res.setHeader('Content-Type', 'image/jpeg')
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
        if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
        toolbarView.webContents.executeJavaScript(script)
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
        if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
        focusWindow()
        toolbarView.webContents.executeJavaScript(`
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
        const info = tabManager.getActiveTabInfo()
        if (info && info.type === 'stealth' && info.sessionId) {
          browserServicePost(`/sessions/${info.sessionId}/dialog`, { accept: action === 'accept', promptText })
            .then(r => json(res, r.json || { ok: true }))
            .catch(e => json(res, { error: e.message }, 500))
          return
        }
        execInActiveWebview(`(function(){
          window.__yamilDialogResult = null;
          const origAlert = window.alert;
          const origConfirm = window.confirm;
          const origPrompt = window.prompt;
          window.alert = function(msg) { window.__yamilDialogResult = { type: 'alert', message: msg }; return undefined; };
          window.confirm = function(msg) { window.__yamilDialogResult = { type: 'confirm', message: msg }; return ${action === 'accept' ? 'true' : 'false'}; };
          window.prompt = function(msg, def) { window.__yamilDialogResult = { type: 'prompt', message: msg, defaultValue: def }; return ${action === 'accept' ? JSON.stringify(promptText || '') : 'null'}; };
          setTimeout(() => { window.alert = origAlert; window.confirm = origConfirm; window.prompt = origPrompt; }, 30000);
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
        const info = tabManager.getActiveTabInfo()
        if (info && info.type === 'stealth' && info.sessionId) {
          browserServiceRaw('POST', `/sessions/${info.sessionId}/screenshot-element`, { selector })
            .then(r => {
              if (r.status >= 400) { json(res, { error: 'element not found' }, r.status); return }
              if (r.buf && r.buf.length > 400_000) {
                json(res, { error: `Element screenshot too large (${(r.buf.length/1024).toFixed(0)}KB).` }, 413)
              } else {
                res.setHeader('Content-Type', r.headers['content-type'] || 'image/jpeg')
                res.writeHead(r.status)
                res.end(r.buf)
              }
            })
            .catch(e => json(res, { error: e.message }, 500))
        } else {
          const view = tabManager.getActiveView()
          if (!view) { json(res, { error: 'no active tab' }, 503); return }
          view.webContents.executeJavaScript(`(function(){
            const el = document.querySelector(${JSON.stringify(selector)});
            if (!el) return null;
            el.scrollIntoView({ block: 'center', behavior: 'instant' });
            const r = el.getBoundingClientRect();
            return { x: Math.round(r.x), y: Math.round(r.y), width: Math.round(r.width), height: Math.round(r.height) };
          })()`).then(rect => {
            if (!rect) { json(res, { error: 'element not found' }, 404); return }
            view.webContents.capturePage({
              x: Math.max(0, rect.x),
              y: Math.max(0, rect.y),
              width: Math.max(1, rect.width),
              height: Math.max(1, rect.height),
            }).then(img => {
              const sz = img.getSize()
              let ni = img
              if (sz.width > 1024) {
                const scale = 1024 / sz.width
                ni = ni.resize({ width: 1024, height: Math.round(sz.height * scale) })
              }
              const buf = ni.toJPEG(55)
              res.setHeader('Content-Type', 'image/jpeg')
              res.writeHead(200)
              res.end(buf)
            }).catch(e => json(res, { error: e.message }, 500))
          }).catch(e => json(res, { error: e.message }, 500))
        }
      })
      return
    }

    // ── POST /print-pdf ──────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/print-pdf') {
      const info = tabManager.getActiveTabInfo()
      if (info && info.type === 'stealth' && info.sessionId) {
        browserServiceRaw('POST', `/sessions/${info.sessionId}/pdf`, {})
          .then(r => {
            res.setHeader('Content-Type', 'application/pdf')
            res.writeHead(r.status)
            res.end(r.buf)
          })
          .catch(e => json(res, { error: e.message }, 500))
      } else {
        const view = tabManager.getActiveView()
        if (!view) { json(res, { error: 'no active tab' }, 503); return }
        view.webContents.printToPDF({ printBackground: true, preferCSSPageSize: true })
          .then(pdfBuf => {
            res.setHeader('Content-Type', 'application/pdf')
            res.writeHead(200)
            res.end(pdfBuf)
          })
          .catch(e => json(res, { error: e.message }, 500))
      }
      return
    }

    // ── POST /drag ────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/drag') {
      readBody(req, body => {
        const { sourceSelector, targetSelector } = body
        if (!sourceSelector || !targetSelector) { json(res, { error: 'sourceSelector and targetSelector required' }, 400); return }
        const info = tabManager.getActiveTabInfo()
        if (info && info.type === 'stealth' && info.sessionId) {
          browserServicePost(`/sessions/${info.sessionId}/drag`, { sourceSelector, targetSelector })
            .then(r => json(res, r.json || { ok: true }))
            .catch(e => json(res, { error: e.message }, 500))
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
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── TAB MANAGEMENT ENDPOINTS ─────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    // ── GET /tabs ─────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/tabs') {
      const tabs = tabManager.getTabList()
      json(res, { tabs: tabs.map((t, i) => ({ index: i, ...t })) })
      return
    }

    // ── POST /new-tab ─────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/new-tab') {
      readBody(req, body => {
        const result = tabManager.createTab(body.url || '', true, body.type || 'yamil')
        json(res, result || { error: 'failed to create tab' })
      })
      return
    }

    // ── POST /switch-tab ──────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/switch-tab') {
      readBody(req, body => {
        const tabs = tabManager.getTabList()
        let target = null
        if (body.id != null) target = tabs.find(t => t.id === body.id)
        else if (body.index != null) target = tabs[body.index]
        if (!target) { json(res, { error: 'tab not found' }, 404); return }
        tabManager.switchTab(target.id)
        json(res, { ok: true, id: target.id, url: target.url, title: target.title })
      })
      return
    }

    // ── POST /close-tab ───────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/close-tab') {
      readBody(req, body => {
        const tabs = tabManager.getTabList()
        let target = null
        if (body.id != null) target = tabs.find(t => t.id === body.id)
        else if (body.index != null) target = tabs[body.index]
        else target = tabs.find(t => t.active)
        if (!target) { json(res, { error: 'tab not found' }, 404); return }
        tabManager.closeTab(target.id)
        json(res, { ok: true, remaining: tabManager.tabs.size })
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── BOOKMARK ENDPOINTS ──────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'GET' && url.pathname === '/bookmarks') {
      if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
      const query = url.searchParams.get('query') || ''
      const script = query
        ? `(function(){ return window._yamil && window._yamil.bookmarks ? window._yamil.bookmarks.search(${JSON.stringify(query)}) : [] })()`
        : `(function(){ return window._yamil && window._yamil.bookmarks ? window._yamil.bookmarks.getAll() : [] })()`
      toolbarView.webContents.executeJavaScript(script)
        .then(bookmarks => json(res, { bookmarks: bookmarks || [] }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    if (req.method === 'POST' && url.pathname === '/bookmarks') {
      readBody(req, body => {
        if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
        const { url: bmUrl, title, tags, category, favicon } = body
        if (!bmUrl) { json(res, { error: 'url required' }, 400); return }
        toolbarView.webContents.executeJavaScript(`
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

    if (req.method === 'DELETE' && url.pathname === '/bookmarks') {
      readBody(req, body => {
        if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
        const { id, url: bmUrl } = body
        if (!id && !bmUrl) { json(res, { error: 'id or url required' }, 400); return }
        const script = id
          ? `(function(){ if(!window._yamil||!window._yamil.bookmarks) return {error:'not ready'}; window._yamil.bookmarks.remove(${JSON.stringify(id)}); if(typeof updateBookmarkStar==='function') updateBookmarkStar(); if(typeof renderBookmarkBar==='function') renderBookmarkBar(); return {ok:true} })()`
          : `(function(){ if(!window._yamil||!window._yamil.bookmarks) return {error:'not ready'}; window._yamil.bookmarks.removeByUrl(${JSON.stringify(bmUrl)}); if(typeof updateBookmarkStar==='function') updateBookmarkStar(); if(typeof renderBookmarkBar==='function') renderBookmarkBar(); return {ok:true} })()`
        toolbarView.webContents.executeJavaScript(script)
          .then(result => json(res, result))
          .catch(e => json(res, { error: e.message }, 500))
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── HISTORY ENDPOINTS ────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'GET' && url.pathname === '/history') {
      if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
      const query = url.searchParams.get('query') || ''
      const script = query
        ? `(function(){ return window._yamil && window._yamil.history ? window._yamil.history.search(${JSON.stringify(query)}) : [] })()`
        : `(function(){ return window._yamil && window._yamil.history ? window._yamil.history.getAll() : [] })()`
      toolbarView.webContents.executeJavaScript(script)
        .then(history => json(res, { history: history || [] }))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    if (req.method === 'DELETE' && url.pathname === '/history') {
      if (!toolbarView) { json(res, { error: 'no toolbar' }, 503); return }
      toolbarView.webContents.executeJavaScript(
        `(function(){ if(window._yamil && window._yamil.history) { window._yamil.history.clear(); return {ok:true} } return {error:'not ready'} })()`
      ).then(result => json(res, result))
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── COOKIE MANAGEMENT ENDPOINTS ───────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'GET' && url.pathname === '/cookies') {
      const domain = url.searchParams.get('domain') || ''
      const yamilSession = session.fromPartition('persist:yamil')
      yamilSession.cookies.get(domain ? { domain } : {})
        .then(cookies => {
          const grouped = {}
          cookies.forEach(c => {
            const d = c.domain.replace(/^\./, '')
            if (!grouped[d]) grouped[d] = []
            grouped[d].push({ name: c.name, value: c.value.slice(0, 100), domain: c.domain, path: c.path, secure: c.secure, httpOnly: c.httpOnly, expirationDate: c.expirationDate })
          })
          json(res, { cookies: grouped, total: cookies.length })
        })
        .catch(e => json(res, { error: e.message }, 500))
      return
    }

    if (req.method === 'DELETE' && url.pathname === '/cookies') {
      readBody(req, async body => {
        const yamilSession = session.fromPartition('persist:yamil')
        const { domain, name } = body
        if (domain && name) {
          const url = `https://${domain.replace(/^\./, '')}${body.path || '/'}`
          await yamilSession.cookies.remove(url, name)
          json(res, { ok: true, deleted: 1 })
        } else if (domain) {
          const cookies = await yamilSession.cookies.get({ domain: domain.replace(/^\./, '') })
          for (const c of cookies) {
            const u = `https://${c.domain.replace(/^\./, '')}${c.path || '/'}`
            await yamilSession.cookies.remove(u, c.name).catch(() => {})
          }
          json(res, { ok: true, deleted: cookies.length })
        } else {
          json(res, { error: 'domain required' }, 400)
        }
      })
      return
    }

    if (req.method === 'POST' && url.pathname === '/cookies/block-third-party') {
      readBody(req, body => {
        const enabled = !!body.enabled
        if (enabled) {
          const yamilSession = session.fromPartition('persist:yamil')
          yamilSession.cookies.flushStore().catch(() => {})
        }
        global._block3pCookies = enabled
        json(res, { ok: true, blocking: enabled })
      })
      return
    }

    if (req.method === 'GET' && url.pathname === '/cookies/block-third-party') {
      json(res, { blocking: !!global._block3pCookies })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── AD BLOCKER ENDPOINTS ──────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'GET' && url.pathname === '/adblock/stats') {
      json(res, adBlocker.getStats())
      return
    }

    if (req.method === 'POST' && url.pathname === '/adblock/toggle') {
      const enabled = adBlocker.toggle()
      json(res, { enabled })
      return
    }

    if (req.method === 'POST' && url.pathname === '/adblock/whitelist') {
      readBody(req, body => {
        const { domain, action } = body
        if (!domain) { json(res, { error: 'domain required' }, 400); return }
        if (action === 'remove') adBlocker.removeWhitelist(domain)
        else adBlocker.addWhitelist(domain)
        json(res, { ok: true, whitelist: [...adBlocker.whitelist] })
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── CONSOLE LOGS ENDPOINT ───────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'GET' && url.pathname === '/console-logs') {
      const level = url.searchParams.get('level')
      const last  = parseInt(url.searchParams.get('last') || '50', 10)
      const clear = url.searchParams.get('clear') === 'true'

      const levels = ['error', 'warning', 'info', 'verbose']
      const maxLevel = level ? levels.indexOf(level) : 2 // default: info
      let logs = tabManager.consoleLogs.filter(l => levels.indexOf(l.level) <= (maxLevel >= 0 ? maxLevel : 2))
      logs = logs.slice(-last)

      if (clear) tabManager.consoleLogs = []

      json(res, { logs })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── CREDENTIAL CRYPTO ENDPOINTS (safeStorage / OS keychain) ──
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'POST' && url.pathname === '/credentials/encrypt') {
      readBody(req, body => {
        if (!safeStorage.isEncryptionAvailable()) { json(res, { error: 'OS keychain not available' }, 503); return }
        const { password } = body
        if (!password) { json(res, { error: 'password required' }, 400); return }
        try {
          const encrypted = safeStorage.encryptString(password).toString('base64')
          json(res, { encrypted })
        } catch (e) { json(res, { error: e.message }, 500) }
      })
      return
    }

    if (req.method === 'POST' && url.pathname === '/credentials/decrypt') {
      readBody(req, body => {
        if (!safeStorage.isEncryptionAvailable()) { json(res, { error: 'OS keychain not available' }, 503); return }
        const { encrypted } = body
        if (!encrypted) { json(res, { error: 'encrypted required' }, 400); return }
        try {
          const password = safeStorage.decryptString(Buffer.from(encrypted, 'base64'))
          json(res, { password })
        } catch (e) { json(res, { error: e.message }, 500) }
      })
      return
    }

    if (req.method === 'POST' && url.pathname === '/credentials/auto-save') {
      readBody(req, async (body) => {
        const { domain, username, password, formUrl, formRecipe } = body
        if (!domain || !username || !password) { json(res, { error: 'domain, username, password required' }, 400); return }
        if (!safeStorage.isEncryptionAvailable()) { json(res, { error: 'OS keychain not available' }, 503); return }
        try {
          const encrypted = safeStorage.encryptString(password).toString('base64')
          const svcUrl = process.env.YAMIL_BROWSER_URL || 'http://127.0.0.1:4000'
          const saveRes = await fetch(`${svcUrl}/credentials`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain, username, passwordEncrypted: encrypted, formUrl }),
            signal: AbortSignal.timeout(5000),
          })
          const saveData = await saveRes.json()
          if (saveData.error) { json(res, { error: saveData.error }, 500); return }
          if (formRecipe) {
            fetch(`${svcUrl}/log-action`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ source: 'auto-credential', action: 'login_recipe_learned', details: { domain, username, formRecipe }, url: formUrl || `https://${domain}`, status: 'ok' }),
              signal: AbortSignal.timeout(3000),
            }).catch(() => {})
          }
          console.log(`[YAMIL cred] Auto-saved credentials for ${domain} (user: ${username})`)
          json(res, { saved: true, domain, username })
        } catch (e) { json(res, { error: e.message }, 500) }
      })
      return
    }

    // ═══════════════════════════════════════════════════════════════
    // ── ZOOM ENDPOINTS ──────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    if (req.method === 'POST' && url.pathname === '/zoom') {
      readBody(req, body => {
        const { action } = body
        const tab = tabManager.getActiveTab()
        if (!tab || !tab.view) { json(res, { error: 'no active tab' }, 503); return }
        let zoom = tab.zoom || 0
        if (action === 'in') zoom++
        else if (action === 'out') zoom--
        else zoom = 0
        tab.zoom = zoom
        tab.view.webContents.setZoomLevel(zoom)
        json(res, { ok: true, zoom })
      })
      return
    }

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

  // ── BaseWindow (replaces BrowserWindow) ──────────────────────
  mainWindow = new BaseWindow({
    width:  state.width  || 1440,
    height: state.height || 900,
    x: state.x,
    y: state.y,
    minWidth:  900,
    minHeight: 600,
    show: false,
    backgroundColor: '#0f172a',
    icon: path.join(__dirname, 'assets', isMac ? 'icon.icns' : isWin ? 'icon.ico' : 'icon.png'),
    titleBarStyle: isMac ? 'hiddenInset' : 'default',
    frame: isMac,
    autoHideMenuBar: true,
  })

  // Hide menu bar on Windows/Linux
  if (!isMac) mainWindow.setMenuBarVisibility(false)

  // ── Toolbar WebContentsView (UI: tab bar, navbar, sidebar, status bar) ──
  toolbarView = new WebContentsView({
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      webviewTag: false,
      sandbox: false,
    },
  })

  toolbarView.webContents.loadFile(path.join(__dirname, 'renderer', 'index.html'))
  mainWindow.contentView.addChildView(toolbarView)

  // Capture toolbar console messages so we can see renderer.js errors
  toolbarView.webContents.on('console-message', (_e, level, message, line, sourceId) => {
    const levelMap = { 0: 'verbose', 1: 'info', 2: 'warning', 3: 'error' }
    tabManager.consoleLogs.push({
      ts: Date.now(),
      level: levelMap[level] || 'info',
      message,
      source: sourceId ? `[toolbar] ${sourceId}` : '[toolbar]',
      line: line || 0,
      tabId: 'toolbar',
    })
    if (tabManager.consoleLogs.length > tabManager.consoleLogsMax) {
      tabManager.consoleLogs = tabManager.consoleLogs.slice(-tabManager.consoleLogsMax)
    }
  })

  // Set title
  mainWindow.setTitle(APP_TITLE)

  // Show window after toolbar is ready
  toolbarView.webContents.once('did-finish-load', () => {
    // Initial layout
    tabManager.layoutViews()
    if (!START_MINIMIZED) mainWindow.show()
  })

  mainWindow.on('resize', () => {
    saveWindowState()
    tabManager.layoutViews()
  })
  mainWindow.on('move', saveWindowState)

  mainWindow.on('enter-full-screen', () => {
    if (toolbarView) toolbarView.webContents.send('fullscreen-changed', true)
  })
  mainWindow.on('leave-full-screen', () => {
    if (toolbarView) toolbarView.webContents.send('fullscreen-changed', false)
  })

  mainWindow.on('close', (e) => {
    saveWindowState()
    if (tray) {
      e.preventDefault()
      mainWindow.hide()
    }
  })
  mainWindow.on('closed', () => { mainWindow = null; toolbarView = null })
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
  // Spoof user agent to Chrome
  session.defaultSession.setUserAgent(CHROME_UA)
  const yamilSession = session.fromPartition('persist:yamil')
  yamilSession.setUserAgent(CHROME_UA)

  // Install ad blocker on webview sessions
  adBlocker.install(yamilSession)
  console.log(`[YAMIL adblock] Installed — ${adBlocker.blockedDomains.size} domains blocked`)

  // Unified onHeadersReceived handler — strips frame-blocking headers and optionally blocks 3P cookies.
  yamilSession.webRequest.onHeadersReceived((details, callback) => {
    const h = Object.assign({}, details.responseHeaders)

    // 1. Strip frame-blocking and download-forcing headers
    for (const key of Object.keys(h)) {
      const lk = key.toLowerCase()
      if (lk === 'x-frame-options') { delete h[key]; continue }
      if (lk === 'content-disposition') {
        const ct = Object.keys(h).find(k => k.toLowerCase() === 'content-type')
        const contentType = ct ? (Array.isArray(h[ct]) ? h[ct][0] : h[ct]) : ''
        if (contentType.includes('text/html') || contentType.includes('application/xhtml')) { delete h[key] }
        continue
      }
      if (lk === 'content-security-policy') {
        h[key] = h[key].map(v => v.replace(/frame-ancestors\s+[^;]+;?/gi, ''))
      }
      if (lk === 'cross-origin-opener-policy' || lk === 'cross-origin-embedder-policy') {
        delete h[key]; continue
      }
    }

    // 2. Third-party cookie blocking (when enabled)
    if (global._block3pCookies) {
      try {
        const reqUrl = new URL(details.url)
        const frameUrl = details.frame?.url || details.referrer || ''
        if (frameUrl) {
          const frameHost = new URL(frameUrl).hostname.replace(/^www\./, '')
          const reqHost = reqUrl.hostname.replace(/^www\./, '')
          if (frameHost && reqHost !== frameHost && !reqHost.endsWith('.' + frameHost)) {
            delete h['set-cookie']
            delete h['Set-Cookie']
          }
        }
      } catch {}
    }

    callback({ responseHeaders: h })
  })

  // Auto-configure any new profile sessions
  app.on('session-created', (newSession) => {
    newSession.setUserAgent(CHROME_UA)
    adBlocker.install(newSession)
    wireDownloadHandler(newSession)
    newSession.webRequest.onHeadersReceived({ urls: ['*://*/*'] }, (details, callback) => {
      const h = Object.assign({}, details.responseHeaders)
      for (const key of Object.keys(h)) {
        const lk = key.toLowerCase()
        if (lk === 'x-frame-options') { delete h[key]; continue }
        if (lk === 'content-disposition') {
          const ct = Object.keys(h).find(k => k.toLowerCase() === 'content-type')
          const contentType = ct ? (Array.isArray(h[ct]) ? h[ct][0] : h[ct]) : ''
          if (contentType.includes('text/html') || contentType.includes('application/xhtml')) { delete h[key] }
          continue
        }
        if (lk === 'content-security-policy') {
          h[key] = h[key].map(v => v.replace(/frame-ancestors\s+[^;]+;?/gi, ''))
        }
        if (lk === 'cross-origin-opener-policy' || lk === 'cross-origin-embedder-policy') {
          delete h[key]; continue
        }
      }
      callback({ responseHeaders: h })
    })
    console.log('[YAMIL] Configured new session partition')
  })

  // ── Download manager ─────────────────────────────────────────────
  const activeDownloads = new Map()

  function wireDownloadHandler (sess) {
    sess.on('will-download', (event, item, webContents) => {
      const filename = item.getFilename()
      const mimeType = item.getMimeType() || ''
      const totalBytes = item.getTotalBytes()
      const downloadUrl = item.getURL() || ''

      console.log(`[YAMIL download] will-download: url=${downloadUrl}, mime=${mimeType}, file=${filename}, bytes=${totalBytes}`)

      const isPageDownload = mimeType.includes('text/html') || mimeType.includes('application/xhtml') ||
          filename === 'download' || !mimeType
      if (isPageDownload) {
        console.log('[YAMIL download] Cancelled — looks like a page navigation, not a real download')
        const tmpPath = path.join(app.getPath('temp'), 'yamil-cancelled-' + Date.now())
        item.setSavePath(tmpPath)
        item.cancel()
        fs.unlink(tmpPath, () => {})
        return
      }

      const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 6)
      let notified = false

      item.on('updated', (_e, state) => {
        const savePath = item.getSavePath()
        if (!savePath) return

        if (!notified) {
          notified = true
          activeDownloads.set(id, item)
          if (toolbarView) {
            toolbarView.webContents.send('download-started', {
              id, filename, totalBytes: item.getTotalBytes(), savePath, state: 'progressing', received: 0
            })
          }
        }

        if (toolbarView) {
          toolbarView.webContents.send('download-progress', {
            id, received: item.getReceivedBytes(), totalBytes: item.getTotalBytes(),
            state: state === 'interrupted' ? 'interrupted' : 'progressing',
            paused: item.isPaused()
          })
        }
      })

      item.once('done', (_e, state) => {
        activeDownloads.delete(id)
        if (notified && toolbarView) {
          toolbarView.webContents.send('download-done', {
            id, filename, state,
            savePath: item.getSavePath(),
            totalBytes: item.getTotalBytes()
          })
        }
      })
    })
  }

  wireDownloadHandler(yamilSession)
  wireDownloadHandler(session.defaultSession)

  ipcMain.on('download-pause', (_e, id) => { const item = activeDownloads.get(id); if (item) item.pause() })
  ipcMain.on('download-resume', (_e, id) => { const item = activeDownloads.get(id); if (item) item.resume() })
  ipcMain.on('download-cancel', (_e, id) => { const item = activeDownloads.get(id); if (item) item.cancel() })

  // Grant media permissions
  session.defaultSession.setPermissionRequestHandler((_wc, permission, callback) => {
    const allowed = ['media', 'audioCapture', 'microphone', 'display-capture']
    callback(allowed.includes(permission))
  })

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

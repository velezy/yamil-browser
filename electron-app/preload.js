const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('YAMIL_CONFIG', {
  AI_ENDPOINT: process.env.AI_ENDPOINT || 'http://localhost:8020/browser-chat',
  APP_TITLE:   process.env.APP_TITLE   || 'YAMIL Browser',
  START_URL:   process.env.START_URL   || 'https://yamil-ai.com',
  PLATFORM:    process.platform,  // 'darwin', 'win32', 'linux'
})

contextBridge.exposeInMainWorld('YAMIL_IPC', {
  toggleFullscreen: () => ipcRenderer.send('toggle-fullscreen'),
  onFullscreenChange: (cb) => ipcRenderer.on('fullscreen-changed', (_e, isFs) => cb(isFs)),
  minimize: () => ipcRenderer.send('window-minimize'),
  maximize: () => ipcRenderer.send('window-maximize'),
  close: () => ipcRenderer.send('window-close'),
  // Download manager
  onDownloadStarted: (cb) => ipcRenderer.on('download-started', (_e, d) => cb(d)),
  onDownloadProgress: (cb) => ipcRenderer.on('download-progress', (_e, d) => cb(d)),
  onDownloadDone: (cb) => ipcRenderer.on('download-done', (_e, d) => cb(d)),
  downloadPause: (id) => ipcRenderer.send('download-pause', id),
  downloadResume: (id) => ipcRenderer.send('download-resume', id),
  downloadCancel: (id) => ipcRenderer.send('download-cancel', id),
})

// ── Tab management IPC bridge ──────────────────────────────────────
// Main process manages WebContentsView tabs directly.
// Toolbar (renderer) communicates with main via these IPC channels.
contextBridge.exposeInMainWorld('yamil', {
  // Tab lifecycle
  createTab:   (url, type) => ipcRenderer.invoke('tab:create', url, type),
  switchTab:   (id)        => ipcRenderer.invoke('tab:switch', id),
  closeTab:    (id)        => ipcRenderer.invoke('tab:close', id),

  // Navigation
  navigate:    (url)       => ipcRenderer.invoke('tab:navigate', url),
  goBack:      ()          => ipcRenderer.invoke('tab:goBack'),
  goForward:   ()          => ipcRenderer.invoke('tab:goForward'),
  reload:      ()          => ipcRenderer.invoke('tab:reload'),

  // Page interaction
  eval:        (script)    => ipcRenderer.invoke('tab:eval', script),
  zoom:        (level)     => ipcRenderer.invoke('tab:zoom', level),
  find:        (text, opts) => ipcRenderer.invoke('tab:find', text, opts),
  stopFind:    ()          => ipcRenderer.invoke('tab:stopFind'),
  print:       ()          => ipcRenderer.invoke('tab:print'),
  devtools:    ()          => ipcRenderer.invoke('tab:devtools'),

  // Tab queries
  getInfo:     ()          => ipcRenderer.invoke('tab:getInfo'),
  list:        ()          => ipcRenderer.invoke('tab:list'),
  getUrl:      ()          => ipcRenderer.invoke('tab:getUrl'),

  // Misc actions
  savePageAs:  ()          => ipcRenderer.invoke('tab:savePageAs'),
  copyUrl:     ()          => ipcRenderer.invoke('tab:copyUrl'),
  viewSource:  ()          => ipcRenderer.invoke('tab:viewSource'),

  // Layout notifications (toolbar → main)
  sidebarToggled:    (open)    => ipcRenderer.send('sidebar-toggled', open),
  bookmarkBarToggled: (visible) => ipcRenderer.send('bookmark-bar-toggled', visible),

  // Events from main → toolbar
  onTabEvent: (callback) => ipcRenderer.on('tab:event', (_e, data) => callback(data)),
})

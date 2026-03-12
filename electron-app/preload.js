const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('YAMIL_CONFIG', {
  AI_ENDPOINT: process.env.AI_ENDPOINT || 'http://localhost:8015/api/v1/builder-orchestra/browser-chat',
  APP_TITLE:   process.env.APP_TITLE   || 'YAMIL Browser',
  START_URL:   process.env.START_URL   || 'https://yamil-ai.com',
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

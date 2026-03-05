const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('YAMIL_CONFIG', {
  AI_ENDPOINT: process.env.AI_ENDPOINT || 'http://localhost:8015/api/v1/builder-orchestra/browser-chat',
  APP_TITLE:   process.env.APP_TITLE   || 'YAMIL Browser',
  START_URL:   process.env.START_URL   || 'https://yamil-ai.com',
})

contextBridge.exposeInMainWorld('YAMIL_IPC', {
  toggleFullscreen: () => ipcRenderer.send('toggle-fullscreen'),
  onFullscreenChange: (cb) => ipcRenderer.on('fullscreen-changed', (_e, isFs) => cb(isFs)),
})

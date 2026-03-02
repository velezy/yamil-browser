const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('yamil', {
  // Navigation
  navigate:     (url)    => ipcRenderer.invoke('navigate',      url),
  goBack:       ()       => ipcRenderer.invoke('go-back'),
  pressKey:     (key)    => ipcRenderer.invoke('press-key',     key),
  scroll:       (d)      => ipcRenderer.invoke('scroll',        d),

  // Mouse + keyboard (canvas interactions)
  mouseClick:   (pos)    => ipcRenderer.invoke('mouse-click',   pos),
  mouseMove:    (pos)    => ipcRenderer.invoke('mouse-move',    pos),
  keyboardType: (text)   => ipcRenderer.invoke('keyboard-type', text),

  // Page data
  evaluate:     (script) => ipcRenderer.invoke('evaluate',      script),
  getUrl:       ()       => ipcRenderer.invoke('get-url'),
  getSessions:  ()       => ipcRenderer.invoke('get-sessions'),

  // Events from main process
  onSessionReady:    (cb) => ipcRenderer.on('session-ready',    (_, d) => cb(d)),
  onServiceError:    (cb) => ipcRenderer.on('service-error',    (_, m) => cb(m)),
  onScreencastFrame: (cb) => ipcRenderer.on('screencast-frame', (_, d) => cb(d)),
  onCdpEvent:        (cb) => ipcRenderer.on('cdp-event',        (_, d) => cb(d)),
})

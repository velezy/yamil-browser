import { getSession } from './sessions.js'

export async function registerWebSockets(app) {

  // ── CDP Event Stream ──────────────────────────────────────────────────
  // Every network request, DOM change, console log, JS error — live.
  // YAMIL: feeds AI context
  // DriveSentinel: triggers alerts on DOM changes
  // Memobytes: captures page content for flashcard generation
  app.get('/sessions/:id/events', { websocket: true }, (socket, req) => {
    const s = getSession(req.params.id)
    if (!s) {
      socket.send(JSON.stringify({ error: 'Session not found' }))
      socket.close()
      return
    }

    s.eventSubs.add(socket)
    socket.send(JSON.stringify({ event: 'connected', sessionId: s.id, ts: Date.now() }))

    socket.on('close', () => s.eventSubs.delete(socket))
    socket.on('error', () => s.eventSubs.delete(socket))
  })

  // ── Live Screencast ───────────────────────────────────────────────────
  // JPEG frames streamed over WebSocket — what the browser actually sees.
  // Electron app connects here to show the live browser view.
  // Starts on first subscriber, stops when last subscriber disconnects.
  app.get('/sessions/:id/screencast', { websocket: true }, async (socket, req) => {
    const s = getSession(req.params.id)
    if (!s) {
      socket.send(JSON.stringify({ error: 'Session not found' }))
      socket.close()
      return
    }

    s.screencastSubs.add(socket)

    // Start screencast on first subscriber
    if (!s.screencastActive) {
      s.screencastActive = true

      await s.cdp.send('Page.startScreencast', {
        format: 'jpeg',
        quality: 80,
        maxWidth: 1280,
        maxHeight: 800,
        everyNthFrame: 1,
      }).catch(() => {})

      s.cdp.on('Page.screencastFrame', async ({ data, sessionId: frameId, metadata }) => {
        const msg = JSON.stringify({ frame: data, metadata, ts: Date.now() })
        for (const ws of s.screencastSubs) {
          try {
            if (ws.readyState === 1) ws.send(msg)
          } catch (_) {
            s.screencastSubs.delete(ws)
          }
        }
        await s.cdp.send('Page.screencastFrameAck', { sessionId: frameId }).catch(() => {})
      })
    }

    socket.on('close', async () => {
      s.screencastSubs.delete(socket)

      // Stop screencast when nobody is watching
      if (s.screencastSubs.size === 0 && s.screencastActive) {
        s.screencastActive = false
        await s.cdp.send('Page.stopScreencast').catch(() => {})
      }
    })

    socket.on('error', () => s.screencastSubs.delete(socket))
  })
}

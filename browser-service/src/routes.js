import { createSession, getSession, listSessions, closeSession, touch } from './sessions.js'

function notFound(reply, id) {
  return reply.code(404).send({ error: `Session ${id} not found` })
}

export async function registerRoutes(app) {

  // ── Health ────────────────────────────────────────────────────────────
  app.get('/health', async () => ({ ok: true, sessions: listSessions().length }))

  // ── Session lifecycle ─────────────────────────────────────────────────
  app.get('/sessions', async () => listSessions())

  app.post('/sessions', async (req) => {
    const session = await createSession(req.body || {})
    return { id: session.id, createdAt: session.createdAt }
  })

  app.delete('/sessions/:id', async (req, reply) => {
    await closeSession(req.params.id)
    return { ok: true }
  })

  // ── Navigation ────────────────────────────────────────────────────────
  app.post('/sessions/:id/navigate', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const allowed = ['networkidle', 'load', 'domcontentloaded', 'commit']
    const waitUntil = allowed.includes(req.body.waitUntil) ? req.body.waitUntil : 'domcontentloaded'
    await s.page.goto(req.body.url, { waitUntil, timeout: 30000 })
    return { url: s.page.url(), title: await s.page.title() }
  })

  app.get('/sessions/:id/url', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    return { url: s.page.url(), title: await s.page.title() }
  })

  app.post('/sessions/:id/back', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.goBack()
    return { url: s.page.url() }
  })

  // ── Interaction ───────────────────────────────────────────────────────
  app.post('/sessions/:id/click', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, text, button = 'left', clickCount = 1 } = req.body
    const opts = { button, clickCount, timeout: 10000 }
    if (text) await s.page.getByText(text, { exact: false }).first().click(opts)
    else await s.page.click(selector, opts)
    return { ok: true }
  })

  app.post('/sessions/:id/fill', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.fill(req.body.selector, req.body.value, { timeout: 10000 })
    return { ok: true }
  })

  app.post('/sessions/:id/press', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.keyboard.press(req.body.key)
    return { ok: true }
  })

  app.post('/sessions/:id/scroll', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const delta = req.body.direction === 'down' ? (req.body.amount || 500) : -(req.body.amount || 500)
    await s.page.mouse.wheel(0, delta)
    return { ok: true }
  })

  app.post('/sessions/:id/hover', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.hover(req.body.selector, { timeout: 10000 })
    return { ok: true }
  })

  app.post('/sessions/:id/select', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.selectOption(req.body.selector, req.body.value, { timeout: 10000 })
    return { ok: true }
  })

  // ── Page data ─────────────────────────────────────────────────────────
  app.get('/sessions/:id/screenshot', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const buf = await s.page.screenshot({ type: 'jpeg', quality: 85 })
    reply.header('content-type', 'image/jpeg')
    return reply.send(buf)
  })

  app.get('/sessions/:id/content', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    return { html: await s.page.content() }
  })

  app.post('/sessions/:id/evaluate', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const result = await s.page.evaluate(req.body.script)
    return { result }
  })

  app.get('/sessions/:id/cookies', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    const cookies = await s.context.cookies()
    return { cookies }
  })

  app.post('/sessions/:id/wait', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, timeout = 10000 } = req.body
    await s.page.waitForSelector(selector, { timeout })
    return { ok: true }
  })

  // ── Mouse + keyboard (used by Electron canvas interactions) ──────────
  app.post('/sessions/:id/mouse/click', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.mouse.click(req.body.x, req.body.y)
    return { ok: true }
  })

  app.post('/sessions/:id/mouse/move', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    await s.page.mouse.move(req.body.x, req.body.y)
    return { ok: true }
  })

  app.post('/sessions/:id/keyboard/type', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.keyboard.type(req.body.text, { delay: req.body.delay || 0 })
    return { ok: true }
  })

  // ── Dialog handling ───────────────────────────────────────────────────

  app.post('/sessions/:id/dialog', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { accept = true, promptText = '' } = req.body || {}
    // Register a one-shot dialog handler then wait briefly for it to fire
    await new Promise((resolve) => {
      const handler = async (dialog) => {
        s.page.off('dialog', handler)
        if (accept) await dialog.accept(promptText || undefined)
        else await dialog.dismiss()
        resolve()
      }
      s.page.on('dialog', handler)
      // Auto-remove if no dialog appears within 5 s
      setTimeout(() => { s.page.off('dialog', handler); resolve() }, 5000)
    })
    return { ok: true, accepted: accept }
  })

  // ── Cookie management ─────────────────────────────────────────────────

  app.post('/sessions/:id/clear-cookies', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.context.clearCookies()
    return { ok: true }
  })

  // ── Tab management ────────────────────────────────────────────────────

  app.get('/sessions/:id/tabs', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const pages = s.context.pages()
    const tabs = await Promise.all(pages.map(async (p, i) => ({
      index: i,
      url: p.url(),
      title: await p.title().catch(() => ''),
      active: p === s.page,
    })))
    return { tabs, count: tabs.length }
  })

  app.post('/sessions/:id/new-tab', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const url = req.body?.url || 'about:blank'
    const newPage = await s.context.newPage()
    if (url !== 'about:blank') {
      await newPage.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {})
    }
    s.page = newPage
    const index = s.context.pages().indexOf(newPage)
    return { index, url: newPage.url(), title: await newPage.title().catch(() => '') }
  })

  app.post('/sessions/:id/switch-tab', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { index } = req.body || {}
    const pages = s.context.pages()
    if (index === undefined || index < 0 || index >= pages.length) {
      return reply.code(400).send({ error: `Invalid tab index ${index}` })
    }
    s.page = pages[index]
    await s.page.bringToFront().catch(() => {})
    return { index, url: s.page.url(), title: await s.page.title().catch(() => '') }
  })

  app.post('/sessions/:id/close-tab', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const pages = s.context.pages()
    if (pages.length <= 1) return reply.code(400).send({ error: 'Cannot close the last tab' })
    const { index } = req.body || {}
    const idx = (index !== undefined) ? index : pages.indexOf(s.page)
    if (idx < 0 || idx >= pages.length) return reply.code(400).send({ error: `Invalid tab index ${idx}` })
    const closing = pages[idx]
    const isCurrent = closing === s.page
    await closing.close()
    if (isCurrent) {
      const remaining = s.context.pages()
      s.page = remaining[Math.max(0, idx - 1)] || remaining[0]
    }
    const currentIdx = s.context.pages().indexOf(s.page)
    return { closed: idx, current: currentIdx, url: s.page.url() }
  })

  // ── File upload ───────────────────────────────────────────────────────

  app.post('/sessions/:id/upload', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, files } = req.body || {}
    if (!selector || !Array.isArray(files) || !files.length) {
      return reply.code(400).send({ error: 'selector and files[] required' })
    }
    const fileList = files.map(f => ({
      name: f.name,
      mimeType: f.mimeType || 'application/octet-stream',
      buffer: Buffer.from(f.content, 'base64'),
    }))
    await s.page.setInputFiles(selector, fileList)
    return { ok: true, uploaded: files.map(f => f.name) }
  })

  // ── File download ─────────────────────────────────────────────────────

  app.post('/sessions/:id/download', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, timeout = 30000 } = req.body || {}
    try {
      const downloadPromise = s.page.waitForEvent('download', { timeout })
      if (selector) await s.page.click(selector).catch(() => {})
      const dl = await downloadPromise
      const stream = await dl.createReadStream()
      const chunks = []
      await new Promise((res, rej) => {
        stream.on('data', c => chunks.push(c))
        stream.on('end', res)
        stream.on('error', rej)
      })
      const buf = Buffer.concat(chunks)
      return {
        ok: true,
        filename: dl.suggestedFilename(),
        content: buf.toString('base64'),
        size: buf.length,
      }
    } catch (e) {
      return reply.code(500).send({ error: e.message })
    }
  })
}

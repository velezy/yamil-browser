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
    await s.page.goto(req.body.url, { waitUntil: 'domcontentloaded', timeout: 30000 })
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
    const { selector, text } = req.body
    if (text) await s.page.getByText(text, { exact: false }).first().click({ timeout: 10000 })
    else await s.page.click(selector, { timeout: 10000 })
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
}

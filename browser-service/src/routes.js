import { createSession, getSession, listSessions, closeSession, touch } from './sessions.js'
import { logAction, cleanupSession, searchKnowledge, getKnowledgeStats, distillSession, flushSession } from './knowledge.js'
import { runTask, isVisionAvailable } from './vision.js'

function notFound(reply, id) {
  return reply.code(404).send({ error: `Session ${id} not found` })
}

/** Log action + return result (non-blocking) */
function withLog(sessionId, action, params, pageUrl) {
  try { logAction(sessionId, action, params, pageUrl) } catch {}
}

export async function registerRoutes(app) {

  // ── Health ────────────────────────────────────────────────────────────
  app.get('/health', async () => ({ ok: true, sessions: listSessions().length }))

  // ── External action logging (for MCP tools that bypass browser-service) ──
  app.post('/log-action', async (req) => {
    const { sessionId, action, params, pageUrl } = req.body || {}
    if (!action) return { ok: false, error: 'action required' }
    const sid = sessionId || 'mcp-' + Date.now().toString(36)
    logAction(sid, action, params || {}, pageUrl || '')
    return { ok: true }
  })

  // ── Session lifecycle ─────────────────────────────────────────────────
  app.get('/sessions', async () => listSessions())

  app.post('/sessions', async (req) => {
    const session = await createSession(req.body || {})
    return { id: session.id, createdAt: session.createdAt }
  })

  app.delete('/sessions/:id', async (req, reply) => {
    cleanupSession(req.params.id)  // flush learned knowledge before closing
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
    const result = { url: s.page.url(), title: await s.page.title() }
    withLog(req.params.id, 'navigate', { url: req.body.url }, result.url)
    return result
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
    withLog(req.params.id, 'click', { selector: selector || text }, s.page.url())
    return { ok: true }
  })

  app.post('/sessions/:id/fill', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.fill(req.body.selector, req.body.value, { timeout: 10000 })
    withLog(req.params.id, 'fill', { selector: req.body.selector, value: (req.body.value || '').substring(0, 50) }, s.page.url())
    return { ok: true }
  })

  app.post('/sessions/:id/press', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.keyboard.press(req.body.key)
    withLog(req.params.id, 'press', { value: req.body.key }, s.page.url())
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
    withLog(req.params.id, 'hover', { selector: req.body.selector }, s.page.url())
    return { ok: true }
  })

  app.post('/sessions/:id/select', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.selectOption(req.body.selector, req.body.value, { timeout: 10000 })
    withLog(req.params.id, 'select', { selector: req.body.selector, value: req.body.value }, s.page.url())
    return { ok: true }
  })

  // ── Page data ─────────────────────────────────────────────────────────
  app.get('/sessions/:id/screenshot', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    let quality = Math.min(100, Math.max(1, parseInt(req.query.quality) || 85))
    const scale = parseFloat(req.query.scale) || 1
    const maxBytes = parseInt(req.query.maxBytes) || 0
    let buf = await s.page.screenshot({ type: 'jpeg', quality, scale: Math.min(1, scale) })
    // Adaptive quality reduction to stay within maxBytes budget
    if (maxBytes > 0) {
      for (let attempt = 0; attempt < 4 && buf.length > maxBytes && quality > 10; attempt++) {
        quality = Math.max(10, Math.round(quality * 0.6))
        buf = await s.page.screenshot({ type: 'jpeg', quality, scale: Math.min(1, scale * 0.7) })
      }
    }
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

  // ── Accessibility snapshot with cross-origin frame traversal ────────
  app.get('/sessions/:id/a11y-snapshot', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const selectorScope = req.query.selector || null

    const SNAPSHOT_SCRIPT = (rootSelector) => `(function(){
      const root = ${rootSelector ? `document.querySelector(${JSON.stringify(rootSelector)})` : `document.body`};
      if (!root) return { elements: [] };
      const INTERACTIVE = new Set(["A","BUTTON","INPUT","TEXTAREA","SELECT","DETAILS","SUMMARY"]);
      const SEMANTIC = new Set(["H1","H2","H3","H4","H5","H6","NAV","MAIN","ASIDE","HEADER","FOOTER","SECTION","ARTICLE","FORM","TABLE","THEAD","TBODY","TR","TH","TD","UL","OL","LI","LABEL","IMG","FIGURE","FIGCAPTION","DIALOG"]);
      const ROLES_INTERACTIVE = new Set(["button","link","textbox","combobox","listbox","option","menuitem","menuitemradio","menuitemcheckbox","checkbox","radio","switch","slider","spinbutton","searchbox","tab","tabpanel","dialog","alertdialog","tree","treeitem","grid","gridcell","row"]);
      const ROLES_SEMANTIC = new Set(["heading","navigation","main","complementary","banner","contentinfo","region","form","table","list","listitem","img","figure","alert","status","log","marquee","timer","toolbar","menu","menubar","tablist"]);
      const elements = [];
      function walk(el, depth) {
        if (depth > 20 || elements.length > 400) return;
        const tag = el.tagName;
        if (!tag) return;
        if (tag === "SCRIPT" || tag === "STYLE" || tag === "NOSCRIPT" || tag === "SVG" || tag === "PATH" || tag === "IFRAME") return;
        try {
          const rect = el.getBoundingClientRect();
          if (rect.width === 0 && rect.height === 0) return;
          const style = getComputedStyle(el);
          if (style.display === "none" || style.visibility === "hidden") return;
        } catch(e) { return; }
        const role = el.getAttribute("role") || "";
        const ariaLabel = el.getAttribute("aria-label") || "";
        const title = el.getAttribute("title") || "";
        const placeholder = el.getAttribute("placeholder") || "";
        const ariaExpanded = el.getAttribute("aria-expanded");
        const ariaSelected = el.getAttribute("aria-selected");
        const isInteractive = INTERACTIVE.has(tag) || ROLES_INTERACTIVE.has(role) || el.hasAttribute("onclick") || el.hasAttribute("tabindex");
        const isSemantic = SEMANTIC.has(tag) || ROLES_SEMANTIC.has(role);
        if (isInteractive || isSemantic) {
          const parts = [];
          if (role) parts.push(role);
          else parts.push(tag.toLowerCase());
          if (ariaLabel) parts.push('"' + ariaLabel.slice(0, 60) + '"');
          else if (title) parts.push('"' + title.slice(0, 60) + '"');
          else {
            const directText = Array.from(el.childNodes).filter(n => n.nodeType === 3).map(n => n.textContent.trim()).join(" ").slice(0, 60);
            if (directText) parts.push('"' + directText.replace(/"/g, "'") + '"');
            else if (tag === "IMG") { const l = el.alt || el.title || ""; if (l) parts.push('"' + l.slice(0, 40) + '"'); }
          }
          if (el.value && (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT")) parts.push("value=" + JSON.stringify(el.value.slice(0, 30)));
          if (placeholder) parts.push("placeholder=" + JSON.stringify(placeholder.slice(0, 30)));
          if (ariaExpanded !== null) parts.push(ariaExpanded === "true" ? "[expanded]" : "[collapsed]");
          if (ariaSelected === "true") parts.push("[selected]");
          if (el.disabled) parts.push("[disabled]");
          if (el.checked) parts.push("[checked]");
          if (el.type) parts.push("type=" + el.type);
          if (tag === "A" && el.href) parts.push("href=" + JSON.stringify(el.href.slice(0, 60)));
          elements.push({ depth, desc: parts.join(" "), selector: el.id ? "#" + el.id : null });
        }
        for (const child of el.children) walk(child, depth + 1);
      }
      walk(root, 0);
      return { elements };
    })()`

    // Collect from all frames (including cross-origin) via Playwright frame API
    const lines = []
    let refId = 1
    const refs = {}

    for (const frame of s.page.frames()) {
      const frameUrl = frame.url()
      const isMain = frame === s.page.mainFrame()
      const baseDepth = isMain ? 0 : 1

      if (!isMain) {
        const ref = '@e' + refId++
        const name = frameUrl.split('?')[0].split('/').pop() || frameUrl.slice(0, 50)
        lines.push('  ' + ref + ` iframe "${name}"`)
        refs[ref] = { tag: 'IFRAME', frame: frameUrl }
      }

      try {
        const result = await frame.evaluate(SNAPSHOT_SCRIPT(isMain ? selectorScope : null)).catch(() => null)
        if (result && result.elements) {
          for (const el of result.elements) {
            const ref = '@e' + refId++
            const indent = '  '.repeat(Math.min(el.depth + baseDepth, 8))
            lines.push(indent + ref + ' ' + el.desc)
            refs[ref] = { selector: el.selector, frame: isMain ? null : frameUrl }
          }
        }
      } catch { /* frame navigated or crashed, skip */ }
    }

    // Store refs on main frame for click/fill
    await s.page.evaluate((data) => {
      window.__yamil_refs = data.refs
      window.__yamil_snapshot_version = (window.__yamil_snapshot_version || 0) + 1
      window.__yamil_refs_version = window.__yamil_snapshot_version
    }, { refs }).catch(() => {})

    const version = await s.page.evaluate(() => window.__yamil_snapshot_version).catch(() => 1)
    return { tree: lines.join('\n'), count: refId - 1, version, refs }
  })

  // ── Click element by ref (with cross-origin frame support) ──────────
  app.post('/sessions/:id/a11y-click', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { ref, frameUrl } = req.body
    if (!ref) return reply.code(400).send({ error: 'ref required' })

    // Find the right frame
    let targetFrame = s.page.mainFrame()
    if (frameUrl) {
      const match = s.page.frames().find(f => f.url() === frameUrl || f.url().includes(frameUrl))
      if (match) targetFrame = match
    }

    try {
      const el = await targetFrame.locator(`[data-yamil-ref="${ref}"]`).first()
      await el.scrollIntoViewIfNeeded({ timeout: 3000 }).catch(() => {})
      await el.click({ timeout: 5000 })
      const text = await el.innerText().catch(() => '')
      const tag = await el.evaluate(e => e.tagName).catch(() => '')
      return { found: true, tag, text: (text || '').trim().slice(0, 40) }
    } catch (e) {
      // Fallback: try all frames
      for (const frame of s.page.frames()) {
        try {
          const el = await frame.locator(`[data-yamil-ref="${ref}"]`).first()
          const visible = await el.isVisible().catch(() => false)
          if (!visible) continue
          await el.scrollIntoViewIfNeeded({ timeout: 3000 }).catch(() => {})
          await el.click({ timeout: 5000 })
          const text = await el.innerText().catch(() => '')
          const tag = await el.evaluate(e => e.tagName).catch(() => '')
          return { found: true, tag, text: (text || '').trim().slice(0, 40) }
        } catch { continue }
      }
      return { found: false, error: e.message }
    }
  })

  // ── Fill element by ref (with cross-origin frame support) ───────────
  app.post('/sessions/:id/a11y-fill', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { ref, value, frameUrl } = req.body
    if (!ref) return reply.code(400).send({ error: 'ref required' })

    for (const frame of s.page.frames()) {
      try {
        const el = await frame.locator(`[data-yamil-ref="${ref}"]`).first()
        const visible = await el.isVisible().catch(() => false)
        if (!visible) continue
        await el.scrollIntoViewIfNeeded({ timeout: 3000 }).catch(() => {})
        await el.fill(value || '', { timeout: 5000 })
        const tag = await el.evaluate(e => e.tagName).catch(() => '')
        return { found: true, tag, value }
      } catch { continue }
    }
    return { found: false, error: `Ref ${ref} not found in any frame` }
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
    withLog(req.params.id, 'type', { value: (req.body.text || '').substring(0, 50) }, s.page.url())
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

  // ── Double click ─────────────────────────────────────────────────
  app.post('/sessions/:id/dblclick', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, text } = req.body
    if (text) await s.page.getByText(text, { exact: false }).first().dblclick({ timeout: 10000 })
    else await s.page.dblclick(selector, { timeout: 10000 })
    withLog(req.params.id, 'dblclick', { selector: selector || text }, s.page.url())
    return { ok: true }
  })

  // ── Right click ─────────────────────────────────────────────────
  app.post('/sessions/:id/rightclick', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, text } = req.body
    const opts = { button: 'right', timeout: 10000 }
    if (text) await s.page.getByText(text, { exact: false }).first().click(opts)
    else await s.page.click(selector, opts)
    return { ok: true }
  })

  // ── Viewport resize ────────────────────────────────────────────
  app.post('/sessions/:id/resize', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { width, height } = req.body
    await s.page.setViewportSize({ width: width || 1920, height: height || 1080 })
    return { ok: true, width, height }
  })

  // ── Go forward ──────────────────────────────────────────────────
  app.post('/sessions/:id/forward', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    await s.page.goForward()
    return { url: s.page.url() }
  })

  // ── Network idle ────────────────────────────────────────────────
  app.post('/sessions/:id/network-idle', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const timeout = req.body?.timeout || 15000
    await s.page.waitForLoadState('networkidle', { timeout }).catch(() => {})
    return { ok: true, url: s.page.url() }
  })

  // ── Element screenshot ──────────────────────────────────────────
  app.post('/sessions/:id/screenshot-element', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector } = req.body
    if (!selector) return reply.code(400).send({ error: 'selector required' })
    const el = s.page.locator(selector).first()
    const quality = Math.min(100, Math.max(1, parseInt(req.body.quality) || 40))
    const buf = await el.screenshot({ type: 'jpeg', quality, timeout: 10000 })
    reply.header('content-type', 'image/jpeg')
    return reply.send(buf)
  })

  // ── PDF generation ──────────────────────────────────────────────
  app.post('/sessions/:id/pdf', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const buf = await s.page.pdf({ printBackground: true, preferCSSPageSize: true })
    reply.header('content-type', 'application/pdf')
    return reply.send(buf)
  })

  // ── Raw HTML ────────────────────────────────────────────────────
  app.get('/sessions/:id/html', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const selector = req.query.selector
    if (selector) {
      const html = await s.page.locator(selector).first().innerHTML({ timeout: 10000 })
      return { html }
    }
    return { html: await s.page.content() }
  })

  // ── Head HTML ───────────────────────────────────────────────────
  app.get('/sessions/:id/head', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const html = await s.page.locator('head').innerHTML({ timeout: 10000 })
    return { html }
  })

  // ── Text content ────────────────────────────────────────────────
  app.get('/sessions/:id/text', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const selector = req.query.selector || 'body'
    const text = await s.page.locator(selector).first().innerText({ timeout: 10000 })
    return { text }
  })

  // ── Drag and drop ──────────────────────────────────────────────
  app.post('/sessions/:id/drag', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { sourceSelector, targetSelector } = req.body
    if (!sourceSelector || !targetSelector) return reply.code(400).send({ error: 'sourceSelector and targetSelector required' })
    await s.page.dragAndDrop(sourceSelector, targetSelector, { timeout: 10000 })
    return { ok: true }
  })

  // ── Set files on input ─────────────────────────────────────────
  app.post('/sessions/:id/set-files', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    const { selector, filePaths } = req.body
    if (!selector || !Array.isArray(filePaths)) return reply.code(400).send({ error: 'selector and filePaths[] required' })
    await s.page.setInputFiles(selector, filePaths)
    return { ok: true }
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

  // ── Autonomous agent (vision + action loop) ──────────────────────────

  app.post('/sessions/:id/run-task', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    touch(s)
    if (!isVisionAvailable()) {
      return reply.code(503).send({ error: 'Vision model not available. Ensure Ollama is running with qwen3-vl:8b.' })
    }
    const { goal, maxSteps = 15 } = req.body || {}
    if (!goal) return reply.code(400).send({ error: 'goal required' })

    const result = await runTask(s.page, req.params.id, goal, maxSteps)
    return result
  })

  /** Check if vision/run-task is available */
  app.get('/vision/status', async () => {
    return { available: isVisionAvailable() }
  })

  // ── Knowledge API (RAG Learning Pipeline) ────────────────────────────
  // Used by AI sidebar, AI Builder Orchestra, and any client that wants
  // to query or contribute to the browser's learned knowledge.

  /** Search learned knowledge by similarity */
  app.post('/knowledge/search', async (req) => {
    const { query, domain, category, topK } = req.body || {}
    if (!query) return { error: 'query required', entries: [] }
    const results = await searchKnowledge(query, domain, category, topK || 5)
    return { entries: results, count: results.length }
  })

  /** Get knowledge base stats */
  app.get('/knowledge/stats', async () => {
    return getKnowledgeStats()
  })

  /** Manually contribute knowledge (e.g., from AI sidebar analysis) */
  app.post('/knowledge/contribute', async (req) => {
    const { goal, url, steps, outcome } = req.body || {}
    if (!goal || !url) return { error: 'goal and url required' }
    const entries = await distillSession({
      goal,
      url,
      steps: steps || [],
      outcome: outcome || 'contributed',
      durationMs: 0,
    })
    return { ok: true, entriesAdded: entries || 0 }
  })

  /** Flush a session's passive knowledge now (don't wait for idle timer) */
  app.post('/sessions/:id/knowledge/flush', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    flushSession(req.params.id, 'manual')
    return { ok: true }
  })

  /** Get knowledge relevant to the current page (for AI sidebar context) */
  app.get('/sessions/:id/knowledge/context', async (req, reply) => {
    const s = getSession(req.params.id)
    if (!s) return notFound(reply, req.params.id)
    const url = s.page.url()
    let domain
    try { domain = new URL(url).hostname } catch { domain = null }
    const results = await searchKnowledge(url, domain, null, 3)
    return { url, domain, entries: results, count: results.length }
  })
}

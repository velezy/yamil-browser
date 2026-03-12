/**
 * YAMIL Browser Webhooks — Push RAG events to external systems
 *
 * Supports: knowledge.created, action.logged, session.flushed, credential.saved
 * Webhooks are stored in PostgreSQL and dispatched async with retry logic.
 */

import pg from 'pg'
import crypto from 'crypto'

const { Pool } = pg

const DB_URL = process.env.DATABASE_URL || 'postgresql://yamil_browser:yamil_browser_secret@localhost:5433/yamil_browser'
const RETRY_MAX = parseInt(process.env.WEBHOOK_RETRY_MAX || '3', 10)
const RETRY_DELAYS = [1000, 5000, 15000] // exponential backoff
const TIMEOUT_MS = parseInt(process.env.WEBHOOK_TIMEOUT_MS || '10000', 10)

let pool = null
let dbReady = false

async function getPool () {
  if (!pool) {
    pool = new Pool({ connectionString: DB_URL, max: 3 })
    pool.on('error', () => {})
  }
  return pool
}

// ── Init: create webhooks table if needed ────────────────────────────

export async function initWebhooks () {
  try {
    const p = await getPool()
    await p.query(`
      CREATE TABLE IF NOT EXISTS browser_webhooks (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        url TEXT NOT NULL,
        events TEXT[] NOT NULL DEFAULT '{}',
        domain_filter TEXT,
        secret TEXT,
        active BOOLEAN DEFAULT true,
        description TEXT,
        failure_count INT DEFAULT 0,
        last_success TIMESTAMPTZ,
        last_failure TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now()
      )
    `)
    dbReady = true
    console.log('[WEBHOOKS] Initialized')
  } catch (e) {
    console.error(`[WEBHOOKS] Init failed: ${e.message}`)
  }
}

// ── CRUD ─────────────────────────────────────────────────────────────

export async function listWebhooks () {
  if (!dbReady) return []
  const p = await getPool()
  const { rows } = await p.query(
    'SELECT id, url, events, domain_filter, active, description, failure_count, last_success, last_failure, created_at FROM browser_webhooks ORDER BY created_at'
  )
  return rows
}

export async function createWebhook ({ url, events, domainFilter, secret, description }) {
  if (!dbReady) throw new Error('DB not ready')
  if (!url) throw new Error('url required')
  const p = await getPool()
  const evts = events && events.length ? events : ['knowledge.created', 'action.logged', 'session.flushed']
  const { rows } = await p.query(
    `INSERT INTO browser_webhooks (url, events, domain_filter, secret, description)
     VALUES ($1, $2, $3, $4, $5) RETURNING *`,
    [url, evts, domainFilter || null, secret || null, description || null]
  )
  return rows[0]
}

export async function deleteWebhook (id) {
  if (!dbReady) throw new Error('DB not ready')
  const p = await getPool()
  const { rowCount } = await p.query('DELETE FROM browser_webhooks WHERE id = $1', [id])
  return rowCount > 0
}

export async function updateWebhook (id, fields) {
  if (!dbReady) throw new Error('DB not ready')
  const p = await getPool()
  const sets = []
  const vals = [id]
  let idx = 2
  if (fields.url !== undefined) { sets.push(`url = $${idx++}`); vals.push(fields.url) }
  if (fields.events !== undefined) { sets.push(`events = $${idx++}`); vals.push(fields.events) }
  if (fields.domainFilter !== undefined) { sets.push(`domain_filter = $${idx++}`); vals.push(fields.domainFilter) }
  if (fields.active !== undefined) { sets.push(`active = $${idx++}`); vals.push(fields.active) }
  if (fields.description !== undefined) { sets.push(`description = $${idx++}`); vals.push(fields.description) }
  if (!sets.length) return null
  sets.push('updated_at = now()')
  const { rows } = await p.query(
    `UPDATE browser_webhooks SET ${sets.join(', ')} WHERE id = $1 RETURNING *`,
    vals
  )
  return rows[0] || null
}

// ── Dispatch ─────────────────────────────────────────────────────────

/** Fire-and-forget: dispatch event to all matching webhooks */
export function dispatch (event, payload) {
  // Run async, don't block caller
  _dispatchAll(event, payload).catch(e =>
    console.error(`[WEBHOOKS] Dispatch error: ${e.message}`)
  )
}

async function _dispatchAll (event, payload) {
  if (!dbReady) return
  const p = await getPool()
  const { rows: hooks } = await p.query(
    `SELECT id, url, events, domain_filter, secret FROM browser_webhooks WHERE active = true`
  )
  if (!hooks.length) return

  const body = {
    event,
    timestamp: new Date().toISOString(),
    ...payload,
  }

  for (const hook of hooks) {
    // Check event filter
    if (hook.events.length && !hook.events.includes(event) && !hook.events.includes('*')) continue
    // Check domain filter
    if (hook.domain_filter && payload.domain && payload.domain !== hook.domain_filter) continue

    _deliverWithRetry(hook, body, p)
  }
}

async function _deliverWithRetry (hook, body, p) {
  const bodyStr = JSON.stringify(body)

  // HMAC signature if secret is set
  const headers = { 'Content-Type': 'application/json', 'X-YAMIL-Event': body.event }
  if (hook.secret) {
    const sig = crypto.createHmac('sha256', hook.secret).update(bodyStr).digest('hex')
    headers['X-YAMIL-Signature'] = `sha256=${sig}`
  }

  for (let attempt = 0; attempt <= RETRY_MAX; attempt++) {
    try {
      const res = await fetch(hook.url, {
        method: 'POST',
        headers,
        body: bodyStr,
        signal: AbortSignal.timeout(TIMEOUT_MS),
      })
      if (res.ok) {
        // Success — reset failure count
        p.query(
          'UPDATE browser_webhooks SET failure_count = 0, last_success = now() WHERE id = $1',
          [hook.id]
        ).catch(() => {})
        return
      }
      // Non-2xx — retry
      console.warn(`[WEBHOOKS] ${hook.url} returned ${res.status} (attempt ${attempt + 1})`)
    } catch (e) {
      console.warn(`[WEBHOOKS] ${hook.url} failed: ${e.message} (attempt ${attempt + 1})`)
    }

    // Wait before retry
    if (attempt < RETRY_MAX) {
      await new Promise(r => setTimeout(r, RETRY_DELAYS[attempt] || 15000))
    }
  }

  // All retries exhausted — increment failure count
  p.query(
    'UPDATE browser_webhooks SET failure_count = failure_count + 1, last_failure = now() WHERE id = $1',
    [hook.id]
  ).catch(() => {})

  // Auto-disable after 10 consecutive failures
  p.query(
    'UPDATE browser_webhooks SET active = false WHERE id = $1 AND failure_count >= 10',
    [hook.id]
  ).catch(() => {})
}

// ── SSE Stream ───────────────────────────────────────────────────────

const sseClients = new Set()

export function addSSEClient (res, domain, category) {
  const client = { res, domain, category }
  sseClients.add(client)
  res.raw.on('close', () => sseClients.delete(client))

  // Send heartbeat every 30s
  const hb = setInterval(() => {
    try { res.raw.write(': heartbeat\n\n') } catch { clearInterval(hb); sseClients.delete(client) }
  }, 30000)
}

/** Also push to SSE clients */
export function dispatchSSE (event, payload) {
  if (!sseClients.size) return
  const data = JSON.stringify({ event, timestamp: new Date().toISOString(), ...payload })
  for (const client of sseClients) {
    if (client.domain && payload.domain && client.domain !== payload.domain) continue
    if (client.category && payload.category && client.category !== payload.category) continue
    try { client.res.raw.write(`event: ${event}\ndata: ${data}\n\n`) } catch {}
  }
}

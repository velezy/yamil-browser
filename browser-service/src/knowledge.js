/**
 * YAMIL Browser RAG Knowledge Pipeline — PostgreSQL + pgvector
 *
 * Passively learns from every browser action (navigate, click, fill, etc.).
 * Distills structured knowledge via Ollama (qwen3:8b) and generates embeddings
 * (nomic-embed-text) stored in pgvector for cosine similarity search.
 *
 * All clients (YAMIL, DriveSentinel, Memobytes) benefit automatically.
 *
 * SECURITY: Password fields are scrubbed before storage.
 */

import pg from 'pg'
import crypto from 'crypto'

const { Pool } = pg

// ── Config ───────────────────────────────────────────────────────────
const OLLAMA_URL = process.env.OLLAMA_URL || 'http://host.docker.internal:11434'
const OLLAMA_EXTRACT_MODEL = process.env.OLLAMA_EXTRACT_MODEL || 'qwen3:8b'
const OLLAMA_EMBED_MODEL   = process.env.OLLAMA_EMBED_MODEL   || 'nomic-embed-text'
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://yamil_browser:yamil_browser_secret@localhost:5433/yamil_browser'

const PASSIVE_IDLE_MS     = 30000  // 30s idle → auto distill
const PASSIVE_MIN_ACTIONS = 5     // minimum actions before auto-distill

// ── Database pool ────────────────────────────────────────────────────
let pool = null
let dbReady = false

async function getPool() {
  if (pool) return pool
  pool = new Pool({ connectionString: DATABASE_URL, max: 5 })
  pool.on('error', (err) => console.error('[KNOWLEDGE DB] Pool error:', err.message))
  // Test connection
  try {
    const client = await pool.connect()
    client.release()
    dbReady = true
    console.log('[KNOWLEDGE DB] Connected to PostgreSQL + pgvector')
  } catch (e) {
    console.error(`[KNOWLEDGE DB] Connection failed: ${e.message}`)
    dbReady = false
  }
  return pool
}

// ── Model availability (probed on startup) ───────────────────────────
let _extractAvailable = false
let _embedAvailable = false

export async function probeOllama() {
  try {
    const res = await fetch(`${OLLAMA_URL}/api/tags`, { signal: AbortSignal.timeout(3000) })
    if (!res.ok) return
    const data = await res.json()
    const names = (data.models || []).map(m => m.name)
    const has = (t) => names.some(n => n === t || n.startsWith(t.split(':')[0]))
    if (has(OLLAMA_EXTRACT_MODEL)) {
      _extractAvailable = true
      console.log(`[KNOWLEDGE] Extract model "${OLLAMA_EXTRACT_MODEL}" ready`)
    }
    if (has(OLLAMA_EMBED_MODEL)) {
      _embedAvailable = true
      console.log(`[KNOWLEDGE] Embed model "${OLLAMA_EMBED_MODEL}" ready`)
    }
  } catch {
    console.log('[KNOWLEDGE] Ollama not reachable — knowledge distillation disabled')
  }
}

export async function initDb() {
  await getPool()
}

// ── Ollama helpers ───────────────────────────────────────────────────
async function ollamaExtract(inputText, template) {
  const systemPrompt = `/no_think\nYou are a structured data extractor. Extract ONLY facts present in the input text into the JSON template below. Do not invent or hallucinate data — if a field has no matching data, leave it as empty string or empty array. Return ONLY valid JSON, no explanation.`
  const userPrompt = `Input text:\n${inputText}\n\nJSON template to fill:\n${JSON.stringify(template, null, 2)}`
  const res = await fetch(`${OLLAMA_URL}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: OLLAMA_EXTRACT_MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ],
      stream: false,
      options: { temperature: 0.0 },
    }),
    signal: AbortSignal.timeout(60000),
  })
  const data = await res.json()
  const raw = data?.message?.content || ''
  const jsonStr = extractJSON(raw)
  if (!jsonStr) return null
  try { return JSON.parse(jsonStr) } catch { return null }
}

async function ollamaEmbed(text) {
  const res = await fetch(`${OLLAMA_URL}/api/embed`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: OLLAMA_EMBED_MODEL, input: text }),
    signal: AbortSignal.timeout(15000),
  })
  const data = await res.json()
  return data?.embeddings?.[0] || null
}

function extractJSON(raw) {
  let cleaned = raw.replace(/<think>[\s\S]*?<\/think>/gi, '').trim()
  cleaned = cleaned.replace(/```json\s*/gi, '').replace(/```\s*/gi, '').trim()
  const start = cleaned.indexOf('{')
  if (start === -1) return null
  let depth = 0, inStr = false, esc = false
  for (let i = start; i < cleaned.length; i++) {
    const ch = cleaned[i]
    if (esc) { esc = false; continue }
    if (ch === '\\') { esc = true; continue }
    if (ch === '"') { inStr = !inStr; continue }
    if (inStr) continue
    if (ch === '{') depth++
    else if (ch === '}') { depth--; if (depth === 0) return cleaned.slice(start, i + 1) }
  }
  return null
}

// ── Security: scrub sensitive values ─────────────────────────────────
const SENSITIVE_PATTERNS = /password|passwd|secret|token|api.?key|authorization|credit.?card|ssn|cvv/i

function scrubValue(action, selector, value) {
  if (!value) return value
  // Scrub fill/type actions on password-like fields
  if ((action === 'fill' || action === 'type') && SENSITIVE_PATTERNS.test(selector || '')) {
    return '***'
  }
  return value.substring(0, 100)  // truncate long values
}

// ── Distillation template ────────────────────────────────────────────
const DISTILLATION_TEMPLATE = {
  page_schemas: [{ url_pattern: '', interactive_elements: [], form_fields: [] }],
  action_recipes: [{ goal: '', steps: [], preconditions: '' }],
  field_maps: [{ field_label: '', selector_hint: '', input_type: '', required: '' }],
  error_recoveries: [{ error_trigger: '', recovery_action: '', outcome: '' }],
  api_patterns: [{ endpoint_hint: '', method: '', auth_type: '', response_shape: '' }],
}

// ── Distill a session into knowledge ─────────────────────────────────
export async function distillSession(session) {
  if (!_extractAvailable) return null
  if (!session?.steps?.length) return null
  if (!dbReady) { console.error('[KNOWLEDGE] DB not ready, skipping distillation'); return null }

  try {
    console.log(`[KNOWLEDGE] Distilling: "${session.goal}" (${session.steps.length} steps)`)

    const inputText = [
      `Goal: ${session.goal}`,
      `URL: ${session.url}`,
      `Outcome: ${session.outcome}`,
      'Steps:',
      ...session.steps.map((s, i) =>
        `${i + 1}. ${s.action} ${s.selector || ''} ${scrubValue(s.action, s.selector, s.value) || ''} - ${s.result}`.replace(/\s+/g, ' ').trim()
      ),
    ].join('\n')

    const extracted = await ollamaExtract(inputText, DISTILLATION_TEMPLATE)
    if (!extracted || typeof extracted === 'string') {
      console.error('[KNOWLEDGE] Extract returned non-JSON, skipping')
      return null
    }

    const domain = (() => { try { return new URL(session.url).hostname } catch { return 'unknown' } })()
    const p = await getPool()
    let savedCount = 0

    const categories = ['page_schemas', 'action_recipes', 'field_maps', 'error_recoveries', 'api_patterns']
    for (const cat of categories) {
      const items = extracted[cat]
      if (!Array.isArray(items)) continue
      for (const item of items) {
        const values = Object.values(item)
        if (values.every(v => !v || (Array.isArray(v) && v.length === 0) || v === '')) continue

        const title = item.goal || item.url_pattern || item.field_label || item.error_trigger || item.endpoint_hint || cat
        const contentStr = JSON.stringify(item)

        let embedding = null
        if (_embedAvailable) {
          try { embedding = await ollamaEmbed(`${cat}: ${title} — ${contentStr}`) } catch {}
        }

        const confidence = session.outcome === 'success' ? 1.0 : session.outcome === 'passive' ? 0.7 : 0.5

        const embeddingStr = embedding ? `[${embedding.join(',')}]` : null

        await p.query(
          `INSERT INTO browser_knowledge (domain, category, title, content, source_goal, source_url, embedding, confidence)
           VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8)`,
          [domain, cat, title, JSON.stringify(item), session.goal, session.url, embeddingStr, confidence]
        )
        savedCount++
      }
    }

    if (savedCount === 0) {
      console.log('[KNOWLEDGE] No meaningful knowledge extracted')
      return null
    }

    console.log(`[KNOWLEDGE] Saved ${savedCount} entries from "${session.goal}" (domain: ${domain})`)
    return savedCount
  } catch (e) {
    console.error(`[KNOWLEDGE] Distillation failed: ${e.message}`)
    return null
  }
}

// ── Search knowledge (pgvector cosine similarity) ────────────────────
export async function searchKnowledge(query, domain, category, topK = 5) {
  if (!dbReady) return []
  const p = await getPool()

  let queryEmbedding = null
  if (_embedAvailable) {
    try { queryEmbedding = await ollamaEmbed(query) } catch {}
  }

  if (queryEmbedding) {
    // Vector similarity search
    const embStr = `[${queryEmbedding.join(',')}]`
    let sql = `SELECT *, 1 - (embedding <=> $1::vector) AS score FROM browser_knowledge WHERE embedding IS NOT NULL`
    const params = [embStr]
    let idx = 2
    if (domain) { sql += ` AND domain = $${idx++}`; params.push(domain) }
    if (category) { sql += ` AND category = $${idx++}`; params.push(category) }
    sql += ` ORDER BY embedding <=> $1::vector LIMIT $${idx}`
    params.push(topK)

    const { rows } = await p.query(sql, params)
    return rows.map(r => ({ ...r, content: r.content, score: parseFloat(r.score) || 0 }))
  } else {
    // Keyword fallback
    let sql = `SELECT *, 0.5 AS score FROM browser_knowledge WHERE title ILIKE $1 OR source_goal ILIKE $1`
    const params = [`%${query}%`]
    let idx = 2
    if (domain) { sql += ` AND domain = $${idx++}`; params.push(domain) }
    if (category) { sql += ` AND category = $${idx++}`; params.push(category) }
    sql += ` ORDER BY created_at DESC LIMIT $${idx}`
    params.push(topK)

    const { rows } = await p.query(sql, params)
    return rows
  }
}

export async function getKnowledgeStats() {
  if (!dbReady) return { total: 0, version: 0, byDomain: {}, byCategory: {}, models: { extract: OLLAMA_EXTRACT_MODEL, embed: OLLAMA_EMBED_MODEL }, extractAvailable: _extractAvailable, embedAvailable: _embedAvailable, db: false }

  const p = await getPool()
  const { rows: [{ count }] } = await p.query('SELECT COUNT(*) AS count FROM browser_knowledge')
  const { rows: domains } = await p.query('SELECT domain, COUNT(*) AS count FROM browser_knowledge GROUP BY domain ORDER BY count DESC')
  const { rows: cats } = await p.query('SELECT category, COUNT(*) AS count FROM browser_knowledge GROUP BY category ORDER BY count DESC')
  const { rows: actions } = await p.query('SELECT COUNT(*) AS count FROM browser_actions')

  const byDomain = {}
  for (const r of domains) byDomain[r.domain] = parseInt(r.count)
  const byCategory = {}
  for (const r of cats) byCategory[r.category] = parseInt(r.count)

  return {
    total: parseInt(count),
    actions: parseInt(actions[0]?.count || 0),
    byDomain,
    byCategory,
    models: { extract: OLLAMA_EXTRACT_MODEL, embed: OLLAMA_EMBED_MODEL },
    extractAvailable: _extractAvailable,
    embedAvailable: _embedAvailable,
    db: true,
  }
}

// ── Passive session tracker (per-session) ────────────────────────────
const sessionTrackers = new Map()

function getTracker(sessionId) {
  if (!sessionTrackers.has(sessionId)) {
    sessionTrackers.set(sessionId, {
      actions: [],
      startUrl: null,
      startDomain: null,
      idleTimer: null,
    })
  }
  return sessionTrackers.get(sessionId)
}

/** Log an action — writes to DB + in-memory tracker for batch distillation */
export async function logAction(sessionId, action, params = {}, pageUrl = '') {
  const tracker = getTracker(sessionId)
  const scrubbedValue = scrubValue(action, params.selector || params.text || '', params.value || params.url || '')

  const entry = {
    action,
    selector: params.selector || params.text || '',
    value: scrubbedValue,
    result: 'ok',
    timestamp: new Date().toISOString(),
  }
  tracker.actions.push(entry)

  // Write to DB (non-blocking)
  if (dbReady) {
    let domain = null
    try { domain = new URL(pageUrl).hostname } catch {}
    const p = await getPool()
    p.query(
      'INSERT INTO browser_actions (session_id, action, selector, value, page_url, domain) VALUES ($1, $2, $3, $4, $5, $6)',
      [sessionId, action, entry.selector, scrubbedValue, pageUrl, domain]
    ).catch(e => console.error(`[KNOWLEDGE] Action log DB error: ${e.message}`))
  }

  // Track domain changes
  try {
    const domain = new URL(pageUrl).hostname
    if (tracker.startDomain && tracker.startDomain !== domain) {
      flushSession(sessionId, 'domain-change')
    }
    tracker.startUrl = pageUrl
    tracker.startDomain = domain
  } catch {}

  // Reset idle timer
  if (tracker.idleTimer) clearTimeout(tracker.idleTimer)
  if (tracker.actions.length >= PASSIVE_MIN_ACTIONS) {
    tracker.idleTimer = setTimeout(() => flushSession(sessionId, 'idle'), PASSIVE_IDLE_MS)
  }
}

/** Flush accumulated actions → distillation */
export function flushSession(sessionId, trigger = 'manual') {
  const tracker = sessionTrackers.get(sessionId)
  if (!tracker) return
  if (tracker.idleTimer) clearTimeout(tracker.idleTimer)
  tracker.idleTimer = null

  const actions = tracker.actions.splice(0)
  if (actions.length < PASSIVE_MIN_ACTIONS) {
    tracker.actions.unshift(...actions)
    return
  }

  const url = tracker.startUrl || 'unknown'
  const goal = `Passive: ${actions.length} actions on ${url}`
  console.log(`[KNOWLEDGE] Passive flush (${trigger}): ${actions.length} actions`)

  distillSession({
    goal,
    url,
    steps: actions,
    outcome: 'passive',
    durationMs: actions.length > 0 ? (Date.now() - new Date(actions[0].timestamp).getTime()) : 0,
  }).catch(e => console.error(`[KNOWLEDGE] Passive distillation error: ${e.message}`))
}

export function cleanupSession(sessionId) {
  flushSession(sessionId, 'session-close')
  sessionTrackers.delete(sessionId)
}

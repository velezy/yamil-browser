/**
 * YAMIL Browser RAG Knowledge Pipeline
 *
 * Passively learns from every browser action (navigate, click, fill, etc.).
 * Distills structured knowledge via Ollama (qwen3:8b) and generates embeddings
 * (nomic-embed-text) for cosine similarity search.
 *
 * Knowledge persists to JSON on disk. All clients (YAMIL, DriveSentinel,
 * Memobytes) benefit automatically since this runs in the browser service.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs'
import { join } from 'path'
import crypto from 'crypto'

// ── Config ───────────────────────────────────────────────────────────
const OLLAMA_URL = process.env.OLLAMA_URL || 'http://host.docker.internal:11434'
const OLLAMA_EXTRACT_MODEL = process.env.OLLAMA_EXTRACT_MODEL || 'qwen3:8b'
const OLLAMA_EMBED_MODEL   = process.env.OLLAMA_EMBED_MODEL   || 'nomic-embed-text'

const KNOWLEDGE_DIR  = process.env.KNOWLEDGE_DIR || join(process.cwd(), 'data', 'knowledge')
const KNOWLEDGE_FILE = join(KNOWLEDGE_DIR, 'browser-knowledge.json')

const PASSIVE_IDLE_MS    = 30000  // 30s idle → auto distill
const PASSIVE_MIN_ACTIONS = 5    // minimum actions before auto-distill
const MAX_ENTRIES = 1000          // LRU cap

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

/** Balanced-brace JSON extractor — handles trailing text from LLMs */
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

// ── Knowledge store (JSON file) ──────────────────────────────────────
function loadKnowledge() {
  try {
    if (existsSync(KNOWLEDGE_FILE)) return JSON.parse(readFileSync(KNOWLEDGE_FILE, 'utf8'))
  } catch {}
  return { entries: [], version: 2 }
}

function saveKnowledge(store) {
  try {
    if (!existsSync(KNOWLEDGE_DIR)) mkdirSync(KNOWLEDGE_DIR, { recursive: true })
    writeFileSync(KNOWLEDGE_FILE, JSON.stringify(store, null, 2), 'utf8')
  } catch (e) { console.error(`[KNOWLEDGE] Save failed: ${e.message}`) }
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

  try {
    console.log(`[KNOWLEDGE] Distilling: "${session.goal}" (${session.steps.length} steps)`)

    const inputText = [
      `Goal: ${session.goal}`,
      `URL: ${session.url}`,
      `Outcome: ${session.outcome}`,
      'Steps:',
      ...session.steps.map((s, i) =>
        `${i + 1}. ${s.action} ${s.selector || ''} ${s.value || ''} - ${s.result}`.replace(/\s+/g, ' ').trim()
      ),
    ].join('\n')

    const extracted = await ollamaExtract(inputText, DISTILLATION_TEMPLATE)
    if (!extracted || typeof extracted === 'string') {
      console.error('[KNOWLEDGE] Extract returned non-JSON, skipping')
      return null
    }

    const knowledgeEntries = []
    const domain = (() => { try { return new URL(session.url).hostname } catch { return 'unknown' } })()
    const timestamp = new Date().toISOString()

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

        knowledgeEntries.push({
          id: crypto.randomUUID(),
          domain,
          category: cat,
          title,
          content: item,
          source_goal: session.goal,
          source_url: session.url,
          embedding,
          confidence: session.outcome === 'success' ? 1.0 : session.outcome === 'passive' ? 0.7 : 0.5,
          access_count: 0,
          created_at: timestamp,
        })
      }
    }

    if (knowledgeEntries.length === 0) {
      console.log('[KNOWLEDGE] No meaningful knowledge extracted')
      return null
    }

    const store = loadKnowledge()
    store.entries.push(...knowledgeEntries)
    if (store.entries.length > MAX_ENTRIES) store.entries = store.entries.slice(-MAX_ENTRIES)
    store.version++
    saveKnowledge(store)

    console.log(`[KNOWLEDGE] Saved ${knowledgeEntries.length} entries from "${session.goal}" (domain: ${domain})`)
    return knowledgeEntries
  } catch (e) {
    console.error(`[KNOWLEDGE] Distillation failed: ${e.message}`)
    return null
  }
}

// ── Search knowledge (cosine similarity) ─────────────────────────────
function cosineSim(a, b) {
  if (!a || !b || a.length !== b.length) return 0
  let dot = 0, na = 0, nb = 0
  for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i] }
  return dot / (Math.sqrt(na) * Math.sqrt(nb) || 1)
}

export async function searchKnowledge(query, domain, category, topK = 5) {
  const store = loadKnowledge()
  if (!store.entries.length) return []

  let candidates = store.entries
  if (domain) candidates = candidates.filter(e => e.domain === domain)
  if (category) candidates = candidates.filter(e => e.category === category)

  let queryEmbedding = null
  if (_embedAvailable) {
    try { queryEmbedding = await ollamaEmbed(query) } catch {}
  }

  const scored = candidates.map(entry => {
    let score = 0
    if (queryEmbedding && entry.embedding) {
      score = cosineSim(queryEmbedding, entry.embedding)
    } else {
      // Keyword fallback
      const lower = query.toLowerCase()
      const text = `${entry.title} ${entry.domain} ${entry.category} ${JSON.stringify(entry.content)}`.toLowerCase()
      const words = lower.split(/\s+/)
      score = words.filter(w => text.includes(w)).length / (words.length || 1)
    }
    return { ...entry, score }
  })

  scored.sort((a, b) => b.score - a.score)
  return scored.slice(0, topK)
}

export function getKnowledgeStats() {
  const store = loadKnowledge()
  const byDomain = {}, byCategory = {}
  for (const e of store.entries) {
    byDomain[e.domain] = (byDomain[e.domain] || 0) + 1
    byCategory[e.category] = (byCategory[e.category] || 0) + 1
  }
  return {
    total: store.entries.length,
    version: store.version,
    byDomain,
    byCategory,
    models: { extract: OLLAMA_EXTRACT_MODEL, embed: OLLAMA_EMBED_MODEL },
    extractAvailable: _extractAvailable,
    embedAvailable: _embedAvailable,
  }
}

// ── Passive session tracker (per-session) ────────────────────────────
// Each browser session gets its own action log. Distillation fires on:
//   - 5+ actions and 30s idle
//   - Domain change
//   - Session close

const sessionTrackers = new Map()  // sessionId → tracker

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

/** Log an action from a browser session */
export function logAction(sessionId, action, params = {}, pageUrl = '') {
  const tracker = getTracker(sessionId)
  tracker.actions.push({
    action,
    selector: params.selector || params.text || '',
    value: params.value || params.url || '',
    result: 'ok',
    timestamp: new Date().toISOString(),
  })

  // Track domain
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

/** Clean up tracker when session closes */
export function cleanupSession(sessionId) {
  flushSession(sessionId, 'session-close')
  sessionTrackers.delete(sessionId)
}

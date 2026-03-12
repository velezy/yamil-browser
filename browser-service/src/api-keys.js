import { randomBytes } from 'node:crypto'
import { readFileSync, writeFileSync, existsSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const KEYS_FILE = join(__dirname, '..', 'data', 'api-keys.json')

let keys = [] // [{ id, key, name, createdAt, lastUsed }]

function load () {
  try {
    if (existsSync(KEYS_FILE)) {
      keys = JSON.parse(readFileSync(KEYS_FILE, 'utf8'))
    }
  } catch { keys = [] }
}

function save () {
  try { writeFileSync(KEYS_FILE, JSON.stringify(keys, null, 2)) } catch {}
}

load()

export function createApiKey (name = 'default') {
  const key = 'yamil_' + randomBytes(24).toString('hex')
  const entry = { id: randomBytes(8).toString('hex'), key, name, createdAt: new Date().toISOString(), lastUsed: null }
  keys.push(entry)
  save()
  return entry
}

export function listApiKeys () {
  return keys.map(k => ({ id: k.id, name: k.name, prefix: k.key.slice(0, 10) + '...', createdAt: k.createdAt, lastUsed: k.lastUsed }))
}

export function deleteApiKey (id) {
  const idx = keys.findIndex(k => k.id === id)
  if (idx === -1) return false
  keys.splice(idx, 1)
  save()
  return true
}

export function validateApiKey (keyValue) {
  const entry = keys.find(k => k.key === keyValue)
  if (!entry) return false
  entry.lastUsed = new Date().toISOString()
  save()
  return true
}

export function isAuthEnabled () {
  return keys.length > 0
}

/** Fastify preHandler hook — skip auth for localhost, health, and when no keys exist */
export function authHook (request, reply, done) {
  // No keys configured = auth disabled
  if (keys.length === 0) return done()

  // Allow localhost without auth
  const ip = request.ip
  if (ip === '127.0.0.1' || ip === '::1' || ip === '::ffff:127.0.0.1') return done()

  const apiKey = request.headers['x-yamil-api-key']
  if (!apiKey || !validateApiKey(apiKey)) {
    reply.code(401).send({ error: 'Invalid or missing API key. Set X-YAMIL-API-Key header.' })
    return
  }
  done()
}

/**
 * YAMIL Browser Credential Store — encrypted credentials in pgvector DB
 *
 * Passwords are stored pre-encrypted (by Electron safeStorage / OS keychain).
 * This module only handles CRUD — encryption/decryption is done by the caller.
 */

import pg from 'pg'

const { Pool } = pg
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://yamil_browser:yamil_browser_secret@localhost:5433/yamil_browser'

let pool = null

async function getPool() {
  if (pool) return pool
  pool = new Pool({ connectionString: DATABASE_URL, max: 3 })
  pool.on('error', (err) => console.error('[CREDENTIALS] Pool error:', err.message))
  return pool
}

/** Save or update credentials for a domain+username */
export async function saveCredential({ domain, username, passwordEncrypted, label, formUrl, notes }) {
  const p = await getPool()
  const { rows } = await p.query(
    `INSERT INTO browser_credentials (domain, username, password_encrypted, label, form_url, notes)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (domain, username) DO UPDATE SET
       password_encrypted = EXCLUDED.password_encrypted,
       label = COALESCE(EXCLUDED.label, browser_credentials.label),
       form_url = COALESCE(EXCLUDED.form_url, browser_credentials.form_url),
       notes = COALESCE(EXCLUDED.notes, browser_credentials.notes),
       updated_at = now()
     RETURNING id, domain, username, label, created_at, updated_at`,
    [domain, username, passwordEncrypted, label || null, formUrl || null, notes || null]
  )
  return rows[0]
}

/** Get credentials for a domain (returns encrypted passwords) */
export async function getCredentials(domain) {
  const p = await getPool()
  const { rows } = await p.query(
    `UPDATE browser_credentials SET last_used = now() WHERE domain = $1
     RETURNING id, domain, username, password_encrypted, label, form_url, notes, last_used`,
    [domain]
  )
  return rows
}

/** List all saved credentials (no passwords) */
export async function listCredentials() {
  const p = await getPool()
  const { rows } = await p.query(
    `SELECT id, domain, username, label, form_url, last_used, created_at, updated_at
     FROM browser_credentials ORDER BY domain, username`
  )
  return rows
}

/** Delete credentials by domain+username or by id */
export async function deleteCredential({ domain, username, id }) {
  const p = await getPool()
  if (id) {
    const { rowCount } = await p.query('DELETE FROM browser_credentials WHERE id = $1', [id])
    return { deleted: rowCount > 0 }
  }
  if (domain && username) {
    const { rowCount } = await p.query(
      'DELETE FROM browser_credentials WHERE domain = $1 AND username = $2',
      [domain, username]
    )
    return { deleted: rowCount > 0 }
  }
  if (domain) {
    const { rowCount } = await p.query('DELETE FROM browser_credentials WHERE domain = $1', [domain])
    return { deleted: rowCount > 0, count: rowCount }
  }
  return { deleted: false, error: 'domain, username, or id required' }
}

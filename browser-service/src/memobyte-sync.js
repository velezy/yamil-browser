/**
 * MemoByte Sync — Syncs browser knowledge to MemoByte's episodic memory.
 *
 * After distillation extracts structured knowledge, this module POSTs
 * each entry to MemoByte's /memory/episodic/record endpoint as an episode.
 *
 * Fire-and-forget: all errors are caught and logged to memobyte_sync_log.
 */

const MEMOBYTE_URL = process.env.MEMOBYTE_ORCHESTRATOR_URL || 'http://host.docker.internal:8124'
const SYNC_USER_ID = 'yamil_browser'

/**
 * Sync extracted knowledge entries to MemoByte episodic memory.
 *
 * @param {object} extracted - The distilled knowledge object (categories → entries)
 * @param {object} session   - Session metadata (goal, url, outcome, etc.)
 * @param {string} domain    - The domain the knowledge was extracted from
 * @param {object} pool      - pg Pool for logging sync status
 */
export async function syncToMemoByte(extracted, session, domain, pool) {
  const categories = ['page_schemas', 'action_recipes', 'field_maps', 'error_recoveries', 'api_patterns', 'navigation_maps']
  let synced = 0
  let failed = 0

  for (const cat of categories) {
    const items = extracted[cat]
    if (!Array.isArray(items)) continue

    for (const item of items) {
      const values = Object.values(item)
      if (values.every(v => !v || (Array.isArray(v) && v.length === 0) || v === '')) continue

      const title = item.goal || item.url_pattern || item.field_label || item.error_trigger || item.endpoint_hint || cat
      const contentStr = JSON.stringify(item)

      // Map to MemoByte episodic memory schema
      const episode = {
        user_id: SYNC_USER_ID,
        query: `${cat}: ${title}`,
        response: contentStr,
        event_type: 'browser_learning',
        topic: `${domain}/${cat}`,
        context: {
          source: 'yamil_browser',
          domain,
          category: cat,
          goal: session.goal,
          url: session.url,
          outcome: session.outcome,
        },
        was_successful: session.outcome === 'success' || session.outcome === 'passive',
        impact_score: session.outcome === 'success' ? 1.0 : session.outcome === 'passive' ? 0.7 : 0.5,
      }

      try {
        const res = await fetch(`${MEMOBYTE_URL}/memory/episodic/record`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(episode),
          signal: AbortSignal.timeout(10000),
        })

        if (res.ok) {
          const data = await res.json()
          synced++
          // Log successful sync
          if (pool) {
            pool.query(
              `INSERT INTO memobyte_sync_log (episode_id, status) VALUES ($1, 'synced')`,
              [data.episode_id || 'unknown']
            ).catch(() => {})
          }
        } else {
          failed++
          const errText = await res.text().catch(() => 'unknown')
          if (pool) {
            pool.query(
              `INSERT INTO memobyte_sync_log (status, error) VALUES ('failed', $1)`,
              [`HTTP ${res.status}: ${errText.substring(0, 200)}`]
            ).catch(() => {})
          }
        }
      } catch (e) {
        failed++
        if (pool) {
          pool.query(
            `INSERT INTO memobyte_sync_log (status, error) VALUES ('failed', $1)`,
            [e.message.substring(0, 200)]
          ).catch(() => {})
        }
      }
    }
  }

  if (synced > 0 || failed > 0) {
    console.log(`[MEMOBYTE SYNC] Synced ${synced} entries, ${failed} failed (domain: ${domain})`)
  }
}

/**
 * YAMIL Browser Vision + Autonomous Agent (run_task)
 *
 * Provides the screenshot → vision AI → act → repeat loop.
 * Uses Ollama qwen3-vl for local vision, with Gemini/Anthropic fallback.
 *
 * Lives in the browser service so ALL clients benefit (YAMIL, DriveSentinel, Memobytes).
 */

import { distillSession, logAction } from './knowledge.js'

// ── Config ───────────────────────────────────────────────────────────
const OLLAMA_URL = process.env.OLLAMA_URL || 'http://host.docker.internal:11434'
const OLLAMA_VISION_MODEL = process.env.OLLAMA_VISION_MODEL || 'qwen3-vl:8b'

let _visionAvailable = false

export async function probeVision() {
  try {
    const res = await fetch(`${OLLAMA_URL}/api/tags`, { signal: AbortSignal.timeout(3000) })
    if (!res.ok) return
    const data = await res.json()
    const names = (data.models || []).map(m => m.name)
    if (names.some(n => n === OLLAMA_VISION_MODEL || n.startsWith(OLLAMA_VISION_MODEL.split(':')[0]))) {
      _visionAvailable = true
      console.log(`[VISION] Model "${OLLAMA_VISION_MODEL}" ready`)
    }
  } catch {
    console.log('[VISION] Ollama not reachable — vision disabled')
  }
}

export function isVisionAvailable() { return _visionAvailable }

// ── Ollama Vision Call ───────────────────────────────────────────────
async function ollamaVision(imageBase64, prompt) {
  const res = await fetch(`${OLLAMA_URL}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: OLLAMA_VISION_MODEL,
      messages: [{
        role: 'user',
        content: prompt,
        images: [imageBase64],
      }],
      stream: false,
    }),
    signal: AbortSignal.timeout(120000),
  })
  if (!res.ok) throw new Error(`Ollama vision HTTP ${res.status}`)
  const data = await res.json()
  return data.message?.content || ''
}

// ── JSON extractor (balanced brace) ──────────────────────────────────
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

// ── Action executor (runs against a Playwright page) ─────────────────
async function executeAction(page, act) {
  switch (act.action) {
    case 'click': {
      try {
        const el = page.locator(act.selector).first()
        await el.scrollIntoViewIfNeeded({ timeout: 3000 }).catch(() => {})
        await el.click({ timeout: 5000 })
        return { ok: true, text: `Clicked: ${act.selector}` }
      } catch (e) {
        return { ok: false, text: `Click failed: ${e.message}` }
      }
    }
    case 'fill': {
      try {
        await page.fill(act.selector, act.value || '', { timeout: 5000 })
        return { ok: true, text: `Filled: ${act.selector}` }
      } catch (e) {
        // Fallback: evaluate fill
        try {
          await page.evaluate(({ sel, val }) => {
            const el = document.querySelector(sel)
            if (!el) throw new Error('not found')
            el.focus()
            const proto = el.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype
            const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set
            if (setter) setter.call(el, val); else el.value = val
            el.dispatchEvent(new InputEvent('input', { bubbles: true }))
            el.dispatchEvent(new Event('change', { bubbles: true }))
          }, { sel: act.selector, val: act.value || '' })
          return { ok: true, text: `Filled (eval): ${act.selector}` }
        } catch (e2) {
          return { ok: false, text: `Fill failed: ${e2.message}` }
        }
      }
    }
    case 'navigate': {
      try {
        await page.goto(act.url, { waitUntil: 'domcontentloaded', timeout: 15000 })
        return { ok: true, text: `Navigated to: ${act.url}` }
      } catch (e) {
        return { ok: false, text: `Navigate failed: ${e.message}` }
      }
    }
    case 'press': {
      try {
        await page.keyboard.press(act.key)
        return { ok: true, text: `Pressed: ${act.key}` }
      } catch (e) {
        return { ok: false, text: `Press failed: ${e.message}` }
      }
    }
    case 'scroll': {
      const delta = act.direction === 'down' ? (act.amount || 500) : -(act.amount || 500)
      await page.mouse.wheel(0, delta)
      return { ok: true, text: `Scrolled ${act.direction}` }
    }
    case 'select': {
      try {
        await page.selectOption(act.selector, act.value, { timeout: 5000 })
        return { ok: true, text: `Selected: ${act.value}` }
      } catch (e) {
        return { ok: false, text: `Select failed: ${e.message}` }
      }
    }
    case 'wait': {
      await new Promise(r => setTimeout(r, act.ms || 1000))
      return { ok: true, text: `Waited ${act.ms || 1000}ms` }
    }
    case 'fail':
      return { ok: false, text: `Failed: ${act.reason}` }
    default:
      return { ok: false, text: `Unknown action: ${act.action}` }
  }
}

// ── run_task: autonomous vision loop ─────────────────────────────────
/**
 * @param {import('playwright').Page} page - Playwright page instance
 * @param {string} sessionId - Session ID for action logging
 * @param {string} goal - High-level goal
 * @param {number} maxSteps - Max steps before giving up
 * @param {function} onStep - Optional callback(step, action, result) for real-time updates
 * @returns {Promise<{done: boolean, result: string, steps: string[], stepCount: number}>}
 */
export async function runTask(page, sessionId, goal, maxSteps = 15, onStep = null) {
  if (!_visionAvailable) {
    return { done: false, result: 'Vision model not available. Ensure Ollama is running with qwen3-vl:8b.', steps: [], stepCount: 0 }
  }

  const history = []
  const transcript = []
  const sessionStart = Date.now()
  let sessionUrl = ''
  let lastUrlHash = ''
  let stuckCount = 0

  for (let step = 1; step <= maxSteps; step++) {
    // Take screenshot
    let ssBase64
    try {
      const buf = await page.screenshot({ type: 'jpeg', quality: 40, scale: 0.5 })
      ssBase64 = buf.toString('base64')
    } catch (e) {
      history.push(`[SCREENSHOT_ERROR] ${e.message}`)
      continue
    }
    if (!ssBase64) { history.push('[EMPTY_SCREENSHOT]'); continue }

    const currentUrl = page.url()
    const scrollY = await page.evaluate('window.scrollY').catch(() => 0)

    // Stuck detection
    const currentHash = `${currentUrl}|${scrollY}`
    if (currentHash === lastUrlHash && step > 1) {
      stuckCount++
      if (stuckCount >= 3) {
        distillSession({ goal, url: sessionUrl || currentUrl, steps: transcript, outcome: 'stuck_loop', durationMs: Date.now() - sessionStart }).catch(() => {})
        return { done: false, result: `Stuck loop after ${step} steps`, steps: history, stepCount: step }
      }
    } else {
      stuckCount = 0
    }
    lastUrlHash = currentHash

    // RAG: search for relevant knowledge on first step
    let ragContext = ''
    if (step === 1) {
      try {
        const domain = new URL(currentUrl).hostname
        const { searchKnowledge } = await import('./knowledge.js')
        const entries = await searchKnowledge(goal + ' ' + currentUrl, domain, null, 3)
        if (entries.length > 0) {
          const tips = entries
            .filter(e => (e.score || 0) > 0.3)
            .map(e => {
              const content = typeof e.content === 'string' ? JSON.parse(e.content) : e.content
              return `- [${e.category}] ${e.title}: ${JSON.stringify(content)}`
            }).join('\n')
          if (tips) ragContext = `\nLearned knowledge from previous visits:\n${tips}`
        }
      } catch {}
    }

    // Vision prompt
    const histText = history.length ? `\nCompleted steps:\n${history.map((h, i) => `${i + 1}. ${h}`).join('\n')}` : ''
    const stuckWarning = stuckCount > 0 ? `\nWARNING: Page state unchanged for ${stuckCount} steps — try a different approach!` : ''

    const prompt = `/no_think\nAutonomous browser agent. Goal: "${goal}"
Step ${step}/${maxSteps} | Page: ${currentUrl}${histText}${stuckWarning}${ragContext}
Analyze the screenshot carefully. Return ONLY valid JSON (no markdown fences):
If goal achieved: {"done":true,"result":"SUMMARY_OF_WHAT_WAS_ACCOMPLISHED"}
If impossible: {"action":"fail","reason":"WHY_IT_CANNOT_BE_DONE"}
Otherwise: {"action":"click|fill|navigate|press|scroll|select|wait","selector":"CSS","value":"TEXT","url":"URL","key":"KEY","direction":"up|down","amount":500,"ms":1000,"description":"BRIEF_STEP_DESCRIPTION"}
Use specific CSS selectors (#id preferred). For click, ensure the element is visible in the screenshot.`

    // Call vision model
    let rawResponse
    try {
      rawResponse = await ollamaVision(ssBase64, prompt)
    } catch (e) {
      history.push(`[VISION_ERROR] ${e.message}`)
      continue
    }

    // Parse decision
    let decision
    try {
      const jsonStr = extractJSON(rawResponse)
      decision = JSON.parse(jsonStr || '{}')
    } catch {
      history.push(`[PARSE_ERROR] ${rawResponse.substring(0, 100)}`)
      continue
    }

    // Check done
    if (decision.done) {
      distillSession({ goal, url: sessionUrl || currentUrl, steps: transcript, outcome: 'success', durationMs: Date.now() - sessionStart }).catch(() => {})
      return { done: true, result: decision.result, steps: history, stepCount: step }
    }

    // Execute action
    const desc = decision.description || `${decision.action} ${decision.selector || decision.url || decision.key || ''}`.trim()
    const result = await executeAction(page, decision)
    history.push(result.ok ? desc : `${desc} [FAILED: ${result.text}]`)
    transcript.push({
      action: decision.action,
      selector: decision.selector || '',
      value: decision.value || '',
      result: result.ok ? 'ok' : result.text,
      url: currentUrl,
      timestamp: new Date().toISOString(),
    })
    if (!sessionUrl) sessionUrl = currentUrl

    // Log to knowledge pipeline
    logAction(sessionId, decision.action, { selector: decision.selector, value: decision.value, url: decision.url }, currentUrl)

    if (onStep) onStep(step, desc, result)

    await new Promise(r => setTimeout(r, 500))
  }

  // Max steps reached
  distillSession({ goal, url: sessionUrl || page.url(), steps: transcript, outcome: 'max_steps', durationMs: Date.now() - sessionStart }).catch(() => {})
  return { done: false, result: `Reached max steps (${maxSteps})`, steps: history, stepCount: maxSteps }
}

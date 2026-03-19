# 143 — Browser Knowledge + AI Orchestrator Integration

**Created:** 2026-03-19
**Status:** Phase 1 MVP Complete
**Goal:** Bridge YAMIL Browser's learned interaction patterns into the MemoByte AI orchestrator so the browser agent uses prior knowledge instead of navigating from scratch.

---

## Overview

YAMIL Browser passively learns interaction patterns as users browse — action recipes, page schemas, field maps, navigation maps — stored in PostgreSQL with pgvector embeddings. The MemoByte AI orchestrator's `BrowserAgent` had **no awareness** of this knowledge, always starting fresh with LLM-driven navigation.

This integration adds a knowledge query step before browser automation, injecting learned recipes into the agent's system prompt so it can follow known paths and use exact selectors.

---

## Architecture

```
User Query → tool_agent.py (classify as BROWSER_WORKFLOW)
  ↓
BrowserKnowledgeClient.search(goal) → POST /knowledge/search (port 4000)
  ↓ (5s timeout, graceful degradation)
format_recipes_as_context(entries) → structured LLM prompt text
  ↓
_execute_browser_workflow() → navigate + capture page state
  ↓
Output includes: page context + learned knowledge context
  ↓
browser_agent.py (OllamaBrowserAgent) → system prompt with injected recipes
  ↓ (on success)
BrowserKnowledgeClient.contribute() → POST /knowledge/contribute (feedback loop)
```

---

## Files Changed

### New: `agents/browser_knowledge_client.py`
- `BrowserKnowledgeClient` — async HTTP client with 5s timeout
  - `search(query, domain?, category?, top_k=5)` → list of knowledge entries
  - `stats()` → knowledge statistics (total, by domain, by category)
  - `contribute(goal, url, steps, outcome)` → feed successful workflows back
- `format_recipes_as_context(entries)` — renders knowledge into LLM-readable text
  - Groups by category (Action Recipes, Field Maps, Navigation Maps, etc.)
  - Includes confidence scores, source URLs, step sequences
  - Renders field_maps with exact CSS selectors

### Modified: `agents/tool_agent.py`
- Import `BrowserKnowledgeClient` + `format_recipes_as_context`
- Knowledge lookup before `_execute_browser_workflow()`:
  - Searches by goal text
  - Passes `knowledge_context` string and `knowledge_entries` list in ToolResult data
  - Rendered in chat output as "Learned Knowledge (N entries)"
- Extended `BROWSER_WORKFLOW_KEYWORDS`:
  - Added: "start a study session", "navigate to the", "go to the page", "open the dashboard", "open the settings", "how do i get to", "show me how to navigate", "browse to"

### Modified: `agents/browser_agent.py`
- `OllamaBrowserAgent.__init__()` accepts `knowledge_context: str`
- `run_task()` injects knowledge into system prompt:
  - Appears before AVAILABLE TOOLS section
  - Instructions: "follow learned recipes first, use exact selectors from field_maps"
- Step tracking: `_executed_steps` list records every tool execution
  - Password scrubbing: fields matching "password/passwd/secret" get `***`
- `run_browser_task()` convenience function:
  - Accepts `knowledge_context` parameter
  - On success: contributes workflow back via `kb_client.contribute()`

---

## Graceful Degradation

- **Browser-service down:** `search()` returns `[]` after 5s timeout, workflow continues without knowledge
- **No matching entries:** Empty context string, agent operates normally (LLM-driven navigation)
- **Contribute fails:** Logged at DEBUG level, doesn't affect user-facing workflow
- **Import error:** If `BrowserKnowledgeClient` can't be imported, knowledge step is skipped silently

---

## Knowledge Content Categories

| Category | What it contains | How the agent uses it |
|----------|------------------|-----------------------|
| `action_recipes` | Goal + ordered steps + preconditions | Follow step sequence directly |
| `field_maps` | Field labels + CSS selectors + input types | Use exact selectors for form filling |
| `navigation_maps` | From-page → To-page + steps + UI path | Navigate via known menu paths |
| `page_schemas` | URL patterns + interactive elements + form fields | Understand page structure upfront |
| `error_recoveries` | Error triggers + recovery actions | Handle errors without retrying blindly |

---

## Also: TTS Auto-Play (StudyAssistant.tsx)

Separate but related change — the AI now speaks back responses automatically:
- Removed `voiceConversationMode` gate from `shouldAutoRead`
- Default changed to ON for new users
- Added speaker toggle button (volume-up/volume-mute) in chat input bar
- Works with Kokoro TTS (streaming or smooth mode)

---

## Verification Checklist

- [ ] Knowledge search returns results when browser-service is up
- [ ] Knowledge search returns `[]` gracefully when browser-service is down
- [ ] Browser agent system prompt includes learned knowledge when entries found
- [ ] Successful workflows contribute back to knowledge API
- [ ] Password values are scrubbed in step records
- [ ] "start a study session" triggers BROWSER_WORKFLOW tool type
- [ ] TTS auto-play speaks back AI responses in StudyAssistant
- [ ] Speaker toggle button mutes/unmutes TTS

---

## Remaining Work (Phase 2)

- [ ] Knowledge stats display in YAMIL Browser sidebar
- [ ] "Knowledge used" indicator in MemoByte chat UI when recipes are found
- [ ] Domain-specific search (extract domain from URL before searching)
- [ ] Field_maps-based smart form filling (auto-fill using exact selectors without LLM)
- [ ] Knowledge ranking: boost frequently-accessed entries

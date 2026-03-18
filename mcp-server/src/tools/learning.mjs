import { z } from "zod";

export function registerLearningTools(server, deps) {
  const { BROWSER_SVC_URL } = deps;

  // ── yamil_browser_learning_start ────────────────────────────────────
  server.tool(
    "yamil_browser_learning_start",
    "Start the YAMIL Browser passive learning agent. The browser will learn from every page visit, click, and form fill.",
    {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/learning/start`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        return { content: [{ type: "text", text: `Learning agent STARTED. The browser will now passively learn from all browsing activity.` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Failed to start learning: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_learning_stop ─────────────────────────────────────
  server.tool(
    "yamil_browser_learning_stop",
    "Stop the YAMIL Browser passive learning agent. No new knowledge will be captured until started again.",
    {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/learning/stop`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        return { content: [{ type: "text", text: `Learning agent STOPPED. Existing knowledge is preserved but no new learning will occur.` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Failed to stop learning: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_learning_status ───────────────────────────────────
  server.tool(
    "yamil_browser_learning_status",
    "Check whether the YAMIL Browser learning agent is active and whether MemoByte sync is enabled.",
    {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/learning/status`, {
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        const learningState = data.learning ? "ON (actively learning)" : "OFF (paused)";
        const syncState = data.sync ? "ON (syncing to MemoByte)" : "OFF";
        return { content: [{ type: "text", text: `Learning: ${learningState}\nMemoByte sync: ${syncState}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Failed to get learning status: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_memobyte_sync_enable ──────────────────────────────
  server.tool(
    "yamil_browser_memobyte_sync_enable",
    "Enable syncing learned knowledge to MemoByte's episodic memory. Knowledge entries will be sent to the AI orchestrator after distillation.",
    {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/learning/memobyte-sync`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: true }),
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        return { content: [{ type: "text", text: `MemoByte sync ENABLED. Learned knowledge will now be synced to MemoByte's episodic memory.` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Failed to enable sync: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_memobyte_sync_disable ─────────────────────────────
  server.tool(
    "yamil_browser_memobyte_sync_disable",
    "Disable syncing learned knowledge to MemoByte. Knowledge will still be stored locally but not sent to the AI orchestrator.",
    {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/learning/memobyte-sync`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: false }),
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        return { content: [{ type: "text", text: `MemoByte sync DISABLED. Knowledge will be stored locally only.` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Failed to disable sync: ${e.message}` }], isError: true };
      }
    }
  );
}

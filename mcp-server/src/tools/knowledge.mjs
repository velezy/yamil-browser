import { z } from "zod";

export function registerKnowledgeTools(server, deps) {
  const { yamilPing, BROWSER_SVC_URL } = deps;

  // ── yamil_browser_knowledge_search ────────────────────────────────────
  server.tool(
    "yamil_browser_knowledge_search",
    "Search YAMIL Browser's learned knowledge base (RAG).",
    {
      query:    z.string().describe("Search query"),
      domain:   z.string().optional().describe("Filter by domain"),
      category: z.string().optional().describe("Filter by category"),
      topK:     z.number().optional().describe("Max results (default 5)"),
    },
    async ({ query, domain, category, topK }) => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/knowledge/search`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query, domain, category, topK: topK || 5 }),
          signal: AbortSignal.timeout(15000),
        });
        const data = await res.json();
        if (!data.entries?.length) {
          return { content: [{ type: "text", text: "No knowledge found. The browser hasn't learned about this topic yet." }] };
        }
        const formatted = data.entries.map((r, i) =>
          `${i + 1}. [${r.category}] ${r.title} (domain: ${r.domain}, score: ${(r.score || 0).toFixed(2)})\n   Source: "${r.source_goal}"\n   Content: ${JSON.stringify(r.content)}`
        ).join("\n\n");
        return { content: [{ type: "text", text: `Found ${data.entries.length} knowledge entries:\n\n${formatted}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Knowledge search failed: ${e.message}. Is the browser service running?` }], isError: true };
      }
    }
  );

  // ── yamil_browser_knowledge_stats ─────────────────────────────────────
  server.tool("yamil_browser_knowledge_stats", "Show statistics about YAMIL Browser's knowledge base.", {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/knowledge/stats`, { signal: AbortSignal.timeout(5000) });
        const stats = await res.json();
        if (!stats.total && !stats.actions) return { content: [{ type: "text", text: "Knowledge base is empty. Browse some pages — the browser learns passively from every action." }] };
        const domainList = Object.entries(stats.byDomain || {}).sort((a, b) => b[1] - a[1]).map(([d, c]) => `  ${d}: ${c}`).join("\n");
        const catList = Object.entries(stats.byCategory || {}).sort((a, b) => b[1] - a[1]).map(([c, n]) => `  ${c}: ${n}`).join("\n");
        return { content: [{ type: "text", text: `Knowledge base: ${stats.total} entries | ${stats.actions || 0} actions logged\n\nBy domain:\n${domainList}\n\nBy category:\n${catList}\n\nModels: Extract=${stats.extractAvailable ? stats.models.extract : "unavailable"} | Embed=${stats.embedAvailable ? stats.models.embed : "unavailable"}\nDB: ${stats.db ? "connected" : "disconnected"}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Knowledge stats failed: ${e.message}. Is the browser service running?` }], isError: true };
      }
    }
  );
}

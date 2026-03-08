import { z } from "zod";

export function registerNavigationTools(server, deps) {
  const { yamilPing, yamilGet, yamilPost, ye, yamilPageUrl, logToolError,
          ragLookup, extractDomain, logMcpAction } = deps;

  // ── yamil_browser_navigate ────────────────────────────────────────────
  server.tool(
    "yamil_browser_navigate",
    "Navigate the YAMIL Browser desktop app to a URL.",
    { url: z.string().describe("URL to navigate to") },
    async ({ url }) => {
      if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running. Use yamil_browser_start first." }], isError: true };
      await yamilPost("/navigate", { url });
      const deadline = Date.now() + 15000;
      let ready = false;
      for (let i = 0; i < 30 && Date.now() < deadline; i++) {
        await new Promise(r => setTimeout(r, 500));
        try {
          const state = await ye("document.readyState");
          if (state === "complete") {
            await new Promise(r => setTimeout(r, 500));
            ready = true;
            break;
          }
        } catch (_) {}
      }
      const urlRes = await yamilGet("/url");
      const { url: finalUrl } = await urlRes.json();
      logMcpAction("navigate", { url }, finalUrl);
      // RAG: include learned knowledge about this domain
      const domain = extractDomain(finalUrl);
      const knowledge = domain ? await ragLookup(finalUrl, domain, null, 3) : null;
      const parts = [`Navigated → ${finalUrl}${ready ? "" : " (page may still be loading)"}`];
      if (knowledge) parts.push(`\n📚 Learned knowledge about ${domain}:\n${knowledge}`);
      return { content: [{ type: "text", text: parts.join("") }] };
    }
  );

  // ── yamil_browser_go_back ─────────────────────────────────────────────
  server.tool("yamil_browser_go_back", "Navigate back in the YAMIL Browser history.", {},
    async () => {
      if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
      await ye("history.back()");
      await new Promise(r => setTimeout(r, 1000));
      const res = await yamilGet("/url"); const { url } = await res.json();
      return { content: [{ type: "text", text: `Back → ${url}` }] };
    }
  );

  // ── yamil_browser_go_forward ──────────────────────────────────────────
  server.tool("yamil_browser_go_forward", "Navigate forward in the YAMIL Browser history.", {},
    async () => {
      if (!(await yamilPing())) return { content: [{ type: "text", text: "YAMIL Browser is not running." }], isError: true };
      await ye("history.forward()");
      await new Promise(r => setTimeout(r, 1000));
      const res = await yamilGet("/url"); const { url } = await res.json();
      return { content: [{ type: "text", text: `Forward → ${url}` }] };
    }
  );
}

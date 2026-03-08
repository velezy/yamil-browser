import { z } from "zod";

export function registerCredentialTools(server, deps) {
  const { yamilPing, yamilPost, YAMIL_CTRL, BROWSER_SVC_URL, extractDomain } = deps;

  // ── yamil_browser_credential_save ──────────────────────────────────
  server.tool(
    "yamil_browser_credential_save",
    "Save website login credentials. The AI stores credentials so it can log in autonomously next time.",
    {
      domain:   z.string().describe("Website domain (e.g. 'chase.com', '192.168.1.188')"),
      username: z.string().describe("Login username or email"),
      password: z.string().describe("Login password (will be encrypted via OS keychain before storage)"),
      label:    z.string().optional().describe("Friendly label (e.g. 'Chase Bank', 'QNAP Admin')"),
      formUrl:  z.string().optional().describe("URL of the login form"),
      notes:    z.string().optional().describe("Additional notes"),
    },
    async ({ domain, username, password, label, formUrl, notes }) => {
      // Step 1: Encrypt password via Electron's safeStorage (OS keychain)
      if (!(await yamilPing())) {
        return { content: [{ type: "text", text: "YAMIL Browser must be running to encrypt credentials (uses OS keychain)." }], isError: true };
      }
      let encrypted;
      try {
        const encRes = await fetch(`${YAMIL_CTRL}/credentials/encrypt`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ password }),
          signal: AbortSignal.timeout(5000),
        });
        const encData = await encRes.json();
        if (encData.error) {
          return { content: [{ type: "text", text: `Encryption failed: ${encData.error}` }], isError: true };
        }
        encrypted = encData.encrypted;
      } catch (e) {
        return { content: [{ type: "text", text: `Encryption error: ${e.message}` }], isError: true };
      }

      // Step 2: Store encrypted credential in DB via browser-service
      try {
        const saveRes = await fetch(`${BROWSER_SVC_URL}/credentials`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            domain, username, passwordEncrypted: encrypted,
            label, formUrl, notes,
          }),
          signal: AbortSignal.timeout(5000),
        });
        const saveData = await saveRes.json();
        if (saveData.error) {
          return { content: [{ type: "text", text: `Save failed: ${saveData.error}` }], isError: true };
        }
        return { content: [{ type: "text", text: `Credentials saved for ${domain} (user: ${username})${label ? ` — "${label}"` : ""}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Save error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_credential_get ───────────────────────────────────
  server.tool(
    "yamil_browser_credential_get",
    "Retrieve saved credentials for a domain. Returns decrypted username and password for the AI to use for login.",
    {
      domain: z.string().optional().describe("Domain to look up (defaults to current page domain)"),
    },
    async ({ domain }) => {
      if (!(await yamilPing())) {
        return { content: [{ type: "text", text: "YAMIL Browser must be running to decrypt credentials." }], isError: true };
      }

      // If no domain specified, use current page
      let lookupDomain = domain;
      if (!lookupDomain) {
        try {
          const urlRes = await fetch(`${YAMIL_CTRL}/url`, { signal: AbortSignal.timeout(3000) });
          const urlData = await urlRes.json();
          lookupDomain = extractDomain(urlData.url);
        } catch { }
      }
      if (!lookupDomain) {
        return { content: [{ type: "text", text: "No domain specified and could not detect current page domain." }], isError: true };
      }

      // Step 1: Get encrypted credentials from DB
      let creds;
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/credentials?domain=${encodeURIComponent(lookupDomain)}`, {
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        if (data.error) {
          return { content: [{ type: "text", text: `Lookup failed: ${data.error}` }], isError: true };
        }
        creds = data.credentials || [];
      } catch (e) {
        return { content: [{ type: "text", text: `Lookup error: ${e.message}` }], isError: true };
      }

      if (creds.length === 0) {
        return { content: [{ type: "text", text: `No saved credentials for "${lookupDomain}". Ask the user for credentials and use yamil_browser_credential_save to store them.` }] };
      }

      // Step 2: Decrypt each credential via Electron's safeStorage
      const results = [];
      for (const cred of creds) {
        try {
          const decRes = await fetch(`${YAMIL_CTRL}/credentials/decrypt`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ encrypted: cred.password_encrypted }),
            signal: AbortSignal.timeout(5000),
          });
          const decData = await decRes.json();
          if (decData.error) {
            results.push({ domain: cred.domain, username: cred.username, error: decData.error });
          } else {
            results.push({
              domain: cred.domain,
              username: cred.username,
              password: decData.password,
              label: cred.label,
              formUrl: cred.form_url,
            });
          }
        } catch (e) {
          results.push({ domain: cred.domain, username: cred.username, error: e.message });
        }
      }

      const text = results.map(r =>
        r.error
          ? `${r.domain} — ${r.username}: decrypt error (${r.error})`
          : `Domain: ${r.domain}\nUsername: ${r.username}\nPassword: ${r.password}${r.label ? `\nLabel: ${r.label}` : ""}${r.formUrl ? `\nForm: ${r.formUrl}` : ""}`
      ).join("\n---\n");

      return { content: [{ type: "text", text }] };
    }
  );

  // ── yamil_browser_credential_list ──────────────────────────────────
  server.tool(
    "yamil_browser_credential_list",
    "List all saved credential domains and usernames (no passwords shown).",
    {},
    async () => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/credentials/list`, {
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        if (data.error) {
          return { content: [{ type: "text", text: `List failed: ${data.error}` }], isError: true };
        }
        const creds = data.credentials || [];
        if (creds.length === 0) {
          return { content: [{ type: "text", text: "No saved credentials. Use yamil_browser_credential_save to store credentials." }] };
        }
        const text = creds.map(c =>
          `${c.domain} — ${c.username}${c.label ? ` (${c.label})` : ""}${c.last_used ? ` | last used: ${new Date(c.last_used).toLocaleDateString()}` : ""}`
        ).join("\n");
        return { content: [{ type: "text", text: `Saved credentials (${creds.length}):\n${text}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `List error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_credential_delete ────────────────────────────────
  server.tool(
    "yamil_browser_credential_delete",
    "Delete saved credentials for a domain (and optionally a specific username).",
    {
      domain:   z.string().describe("Domain to delete credentials for"),
      username: z.string().optional().describe("Specific username to delete (omit to delete all for domain)"),
    },
    async ({ domain, username }) => {
      try {
        const res = await fetch(`${BROWSER_SVC_URL}/credentials`, {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ domain, username }),
          signal: AbortSignal.timeout(5000),
        });
        const data = await res.json();
        if (data.error) {
          return { content: [{ type: "text", text: `Delete failed: ${data.error}` }], isError: true };
        }
        if (data.deleted) {
          return { content: [{ type: "text", text: `Credentials deleted for ${domain}${username ? ` (user: ${username})` : ""}` }] };
        }
        return { content: [{ type: "text", text: `No credentials found for ${domain}${username ? ` (user: ${username})` : ""}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `Delete error: ${e.message}` }], isError: true };
      }
    }
  );
}

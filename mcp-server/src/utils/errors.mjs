/**
 * Error logging — writes detection failures to markdown for review and improvement
 */

import { existsSync, mkdirSync, appendFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname_mjs = dirname(fileURLToPath(import.meta.url));
export const PROJECT_ROOT = join(__dirname_mjs, "..", "..", "..");
const ERROR_LOG_PATH = join(PROJECT_ROOT, "YAMILBrowserErrors.md");

export function logToolError(tool, params, error, pageUrl) {
  try {
    const dir = dirname(ERROR_LOG_PATH);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const ts = new Date().toISOString();
    const header = existsSync(ERROR_LOG_PATH) ? "" : "# YAMIL Browser Tool Errors\n\nDetection failures logged automatically for review and improvement.\n\n---\n\n";
    const entry = `${header}### ${ts} — \`${tool}\`\n- **Page**: ${pageUrl || "unknown"}\n- **Params**: \`${JSON.stringify(params)}\`\n- **Error**: ${error}\n\n---\n\n`;
    appendFileSync(ERROR_LOG_PATH, entry, "utf8");
  } catch (_) { /* don't break tools if logging fails */ }
}

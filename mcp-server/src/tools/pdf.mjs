import { z } from "zod";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import { join } from "path";

export function registerPdfTools(server, deps) {
  const { yamilPing, yamilGet, logToolError } = deps;

  // ── yamil_browser_fill_pdf ──────────────────────────────────────────
  server.tool(
    "yamil_browser_fill_pdf",
    "Fill a PDF form. Accepts a URL or local file path, a map of field names → values, and optional text overlays for non-form areas. Returns the saved file path.",
    {
      source:    z.string().describe("URL or absolute file path of the PDF to fill"),
      fields:    z.record(z.string(), z.string()).optional().describe("Map of form field names → values to fill"),
      overlays:  z.array(z.object({
        text: z.string().describe("Text to place"),
        x:    z.number().describe("X position in points from left"),
        y:    z.number().describe("Y position in points from bottom"),
        size: z.number().optional().describe("Font size (default 12)"),
        page: z.number().optional().describe("Page index, 0-based (default 0)"),
      })).optional().describe("Array of text overlays for non-form areas"),
      savePath:  z.string().describe("Absolute file path to save the filled PDF"),
    },
    async ({ source, fields, overlays, savePath }) => {
      try {
        // Load the PDF
        let pdfBytes;
        if (source.startsWith("http://") || source.startsWith("https://")) {
          const res = await fetch(source, { signal: AbortSignal.timeout(30000) });
          if (!res.ok) return { content: [{ type: "text", text: `Failed to download PDF: HTTP ${res.status}` }], isError: true };
          pdfBytes = new Uint8Array(await res.arrayBuffer());
        } else {
          const filePath = source.replace(/^file:\/\/\//, "");
          if (!existsSync(filePath)) return { content: [{ type: "text", text: `File not found: ${filePath}` }], isError: true };
          pdfBytes = readFileSync(filePath);
        }

        const pdfDoc = await PDFDocument.load(pdfBytes, { ignoreEncryption: true });
        const form = pdfDoc.getForm();
        const font = await pdfDoc.embedFont(StandardFonts.Helvetica);

        // Fill form fields
        const filled = [];
        const skipped = [];
        if (fields) {
          for (const [name, value] of Object.entries(fields)) {
            try {
              const field = form.getTextField(name);
              field.setText(value);
              field.updateAppearances(font);
              filled.push(name);
            } catch (e) {
              // Try checkbox/radio
              try {
                const cb = form.getCheckBox(name);
                if (value.toLowerCase() === "true" || value === "X" || value === "x" || value === "1") {
                  cb.check();
                } else {
                  cb.uncheck();
                }
                filled.push(name);
              } catch (_) {
                skipped.push(`${name}: ${e.message}`);
              }
            }
          }
        }

        // Apply text overlays
        const overlaid = [];
        if (overlays) {
          const pages = pdfDoc.getPages();
          for (const ov of overlays) {
            const pageIdx = ov.page ?? 0;
            if (pageIdx >= pages.length) {
              skipped.push(`overlay page ${pageIdx}: out of range`);
              continue;
            }
            const page = pages[pageIdx];
            page.drawText(ov.text, {
              x: ov.x,
              y: ov.y,
              size: ov.size ?? 12,
              font,
              color: rgb(0, 0, 0),
            });
            overlaid.push(`"${ov.text}" at (${ov.x}, ${ov.y})`);
          }
        }

        // Flatten form so fields are no longer editable (print-ready)
        form.flatten();

        // Save
        const filledBytes = await pdfDoc.save();
        writeFileSync(savePath, filledBytes);

        const summary = [
          `PDF saved: ${savePath} (${(filledBytes.length / 1024).toFixed(1)} KB)`,
          filled.length ? `Filled ${filled.length} fields: ${filled.join(", ")}` : null,
          overlaid.length ? `Overlaid ${overlaid.length} texts: ${overlaid.join("; ")}` : null,
          skipped.length ? `Skipped: ${skipped.join("; ")}` : null,
        ].filter(Boolean).join("\n");

        return { content: [{ type: "text", text: summary }] };
      } catch (e) {
        return { content: [{ type: "text", text: `PDF fill error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── yamil_browser_pdf_fields ────────────────────────────────────────
  server.tool(
    "yamil_browser_pdf_fields",
    "List all fillable form fields in a PDF, with their names, types, and positions. Use this to discover field names before filling.",
    {
      source: z.string().describe("URL or absolute file path of the PDF"),
    },
    async ({ source }) => {
      try {
        let pdfBytes;
        if (source.startsWith("http://") || source.startsWith("https://")) {
          const res = await fetch(source, { signal: AbortSignal.timeout(30000) });
          if (!res.ok) return { content: [{ type: "text", text: `Failed to download PDF: HTTP ${res.status}` }], isError: true };
          pdfBytes = new Uint8Array(await res.arrayBuffer());
        } else {
          const filePath = source.replace(/^file:\/\/\//, "");
          if (!existsSync(filePath)) return { content: [{ type: "text", text: `File not found: ${filePath}` }], isError: true };
          pdfBytes = readFileSync(filePath);
        }

        const pdfDoc = await PDFDocument.load(pdfBytes, { ignoreEncryption: true });
        const form = pdfDoc.getForm();
        const fields = form.getFields();

        if (fields.length === 0) {
          return { content: [{ type: "text", text: "No fillable form fields found in this PDF. Use overlays to place text at specific coordinates." }] };
        }

        const info = fields.map(f => {
          const name = f.getName();
          const type = f.constructor.name.replace("PDF", "");
          const widgets = f.acroField.getWidgets();
          const rects = widgets.map(w => {
            const r = w.getRectangle();
            return `(${r.x.toFixed(0)}, ${r.y.toFixed(0)}, ${r.width.toFixed(0)}x${r.height.toFixed(0)})`;
          });
          return `${name} [${type}] at ${rects.join(", ")}`;
        });

        return { content: [{ type: "text", text: `${fields.length} form fields:\n${info.join("\n")}` }] };
      } catch (e) {
        return { content: [{ type: "text", text: `PDF fields error: ${e.message}` }], isError: true };
      }
    }
  );
}

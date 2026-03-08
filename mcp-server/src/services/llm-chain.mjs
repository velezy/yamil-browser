import Anthropic from "@anthropic-ai/sdk";
import AnthropicBedrock from "@anthropic-ai/bedrock-sdk";
import { GoogleGenerativeAI } from "@google/generative-ai";

// ── LLM Provider: Gemini CU → Ollama (local) → Gemini Flash → Bedrock → Anthropic ──
let _gemini = null;
let _anthropic = null;
let _usingBedrock = false;
let _usingGemini = false;
let _ollamaAvailable = false;
export const OLLAMA_URL = process.env.OLLAMA_URL || "http://127.0.0.1:11434";
export const OLLAMA_VISION_MODEL  = process.env.OLLAMA_VISION_MODEL  || "qwen3-vl:8b";

// Initialize Gemini if key available
if (process.env.GEMINI_API_KEY) {
  _gemini = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
  _usingGemini = true;
}

// Probe Ollama on startup (non-blocking)
fetch(`${OLLAMA_URL}/api/tags`, { signal: AbortSignal.timeout(2000) })
  .then(r => r.ok ? r.json() : null)
  .then(d => {
    if (!d?.models) return;
    const names = d.models.map(m => m.name);
    const hasModel = (target) => names.some(n => n === target || n.startsWith(target.split(":")[0]));
    if (hasModel(OLLAMA_VISION_MODEL)) {
      _ollamaAvailable = true;
      console.error(`[OLLAMA] Vision: "${OLLAMA_VISION_MODEL}" ready`);
    }
  })
  .catch(() => {});

async function ollamaCreate(params) {
  const messages = [];
  for (const msg of params.messages) {
    const content = Array.isArray(msg.content) ? msg.content : [{ type: "text", text: msg.content }];
    let text = "";
    const images = [];
    for (const part of content) {
      if (part.type === "text") text += part.text;
      else if (part.type === "image") images.push(part.source.data);
    }
    messages.push({ role: msg.role, content: text, ...(images.length ? { images } : {}) });
  }
  const res = await fetch(`${OLLAMA_URL}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: OLLAMA_VISION_MODEL, messages, stream: false }),
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`Ollama HTTP ${res.status}`);
  const data = await res.json();
  const text = data.message?.content || "";
  return { content: [{ type: "text", text }] };
}

async function geminiCreate(params) {
  const model = _gemini.getGenerativeModel({ model: "gemini-2.0-flash" });
  const parts = [];
  for (const msg of params.messages) {
    const content = Array.isArray(msg.content) ? msg.content : [{ type: "text", text: msg.content }];
    for (const part of content) {
      if (part.type === "text") {
        parts.push({ text: part.text });
      } else if (part.type === "image") {
        parts.push({ inlineData: { mimeType: part.source.media_type, data: part.source.data } });
      }
    }
  }
  const result = await model.generateContent(parts);
  const text = result.response.text();
  return { content: [{ type: "text", text }] };
}

const BEDROCK_MODELS = {
  "claude-sonnet-4-6":          "us.anthropic.claude-sonnet-4-6",
  "claude-opus-4-6":            "us.anthropic.claude-opus-4-6-v1",
  "claude-haiku-4-5-20251001":  "us.anthropic.claude-haiku-4-5-20251001-v1:0",
  "claude-3-5-sonnet-20241022": "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
  "claude-3-5-haiku-20241022":  "us.anthropic.claude-3-5-haiku-20241022-v1:0",
  "claude-3-7-sonnet-20250219": "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
};
function resolveModel(m) {
  return _usingBedrock ? (BEDROCK_MODELS[m] || `us.anthropic.${m}`) : m;
}

function getAnthropic() {
  if (_anthropic) return _anthropic;
  if (process.env.ANTHROPIC_API_KEY) {
    _anthropic = new Anthropic();
    _usingBedrock = false;
    return _anthropic;
  }
  const akid = process.env.AWS_BEDROCK_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID;
  const skey = process.env.AWS_BEDROCK_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY;
  if (akid && skey) {
    _anthropic = new AnthropicBedrock({ awsAccessKey: akid, awsSecretKey: skey, awsRegion: process.env.AWS_DEFAULT_REGION || "us-east-1" });
    _usingBedrock = true;
    return _anthropic;
  }
  return null;
}

export const anthropic = new Proxy({}, {
  get(_, prop) {
    if (prop === "messages") {
      return {
        create: async (params) => {
          if (_ollamaAvailable) {
            try {
              const hasImage = params.messages?.some(m => Array.isArray(m.content) && m.content.some(p => p.type === "image"));
              const imgLen = hasImage ? params.messages.flatMap(m => Array.isArray(m.content) ? m.content.filter(p => p.type === "image").map(p => (p.source?.data || "").length) : []).join(",") : "none";
              console.error(`[OLLAMA] Using ${OLLAMA_VISION_MODEL} (images: ${imgLen})`);
              return await ollamaCreate(params);
            } catch (e) {
              console.error(`[OLLAMA] Failed: ${e.message}`);
            }
          }
          if (_usingGemini && _gemini) {
            try { return await geminiCreate(params); } catch (e) { /* fall through */ }
          }
          const client = getAnthropic();
          if (!client) throw new Error("No LLM available — run Ollama, set GEMINI_API_KEY, ANTHROPIC_API_KEY, or AWS_BEDROCK_ACCESS_KEY_ID.");
          return client.messages.create({ ...params, model: resolveModel(params.model) });
        }
      };
    }
    const client = getAnthropic();
    if (!client) throw new Error("No LLM available.");
    return client[prop];
  }
});

export function isOllamaAvailable() {
  return _ollamaAvailable;
}

export function isGeminiAvailable() {
  return _usingGemini;
}

export function isBedrockAvailable() {
  return _usingBedrock;
}

export { getAnthropic };

// ── Gemini Computer Use API ─────────────────────────────────────────
const CU_MODEL = "gemini-2.5-computer-use-preview-10-2025";
const CU_ENDPOINT = `https://generativelanguage.googleapis.com/v1beta/models/${CU_MODEL}:generateContent`;

async function geminiComputerUse(screenshotBase64, instruction, history = []) {
  if (!process.env.GEMINI_API_KEY) return null;
  const contents = [
    ...history,
    { role: "user", parts: [
      { text: instruction },
      { inline_data: { mime_type: "image/png", data: screenshotBase64 } },
    ]},
  ];
  const res = await fetch(`${CU_ENDPOINT}?key=${process.env.GEMINI_API_KEY}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents,
      tools: [{ computer_use: { environment: "ENVIRONMENT_BROWSER" } }],
    }),
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    console.error(`[CU] Gemini CU error ${res.status}: ${errText.slice(0, 200)}`);
    return null;
  }
  const data = await res.json();
  const candidate = data.candidates?.[0]?.content;
  if (!candidate) return null;
  let action = null, args = null, reasoning = "", safetyDecision = null;
  for (const part of candidate.parts || []) {
    if (part.text) reasoning += part.text;
    if (part.function_call) {
      action = part.function_call.name;
      args = part.function_call.args || {};
      safetyDecision = part.function_call.safety_decision?.decision || null;
    }
  }
  return { action, args, reasoning, safetyDecision, _rawParts: candidate.parts };
}

function convertCUCoords(x, y, viewport) {
  return {
    px: Math.round((x / 1000) * viewport.width),
    py: Math.round((y / 1000) * viewport.height),
  };
}

function buildCUFunctionResponse(actionName, screenshotBase64, url, safetyAck = false) {
  const response = { url };
  if (safetyAck) response.safety_acknowledgement = "true";
  return {
    role: "user",
    parts: [{
      function_response: {
        name: actionName,
        response,
        parts: [{ inline_data: { mime_type: "image/png", data: screenshotBase64 } }],
      },
    }],
  };
}

export function createCUExecutor(ye, yamilPost) {
  return async function executeYamilCUAction(action, args) {
    const vp = await ye("({ width: window.innerWidth, height: window.innerHeight })") || { width: 1440, height: 900 };
    try {
      switch (action) {
        case "click_at": {
          const { px, py } = convertCUCoords(args.x, args.y, vp);
          await ye(`(function(x,y){
            const el = document.elementFromPoint(x,y);
            if (!el) return;
            el.scrollIntoView({ block: "center", behavior: "instant" });
            el.dispatchEvent(new PointerEvent("pointerdown", { bubbles:true, clientX:x, clientY:y }));
            el.dispatchEvent(new MouseEvent("mousedown", { bubbles:true, clientX:x, clientY:y }));
            el.dispatchEvent(new MouseEvent("mouseup", { bubbles:true, clientX:x, clientY:y }));
            el.dispatchEvent(new MouseEvent("click", { bubbles:true, clientX:x, clientY:y }));
          })(${px},${py})`);
          return { ok: true, text: `Clicked at (${px},${py})` };
        }
        case "type_text_at": {
          const { px, py } = convertCUCoords(args.x, args.y, vp);
          const text = args.text || "";
          const clearFirst = args.clear_before_typing || false;
          const pressEnter = args.press_enter || false;
          await ye(`(function(x,y,txt,clear,enter){
            const el = document.elementFromPoint(x,y);
            if (!el) return;
            el.focus();
            if (clear) {
              const proto = el.tagName==="TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
              const setter = Object.getOwnPropertyDescriptor(proto,"value")?.set;
              if (setter) setter.call(el,""); else el.value="";
            }
            const proto = el.tagName==="TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
            const setter = Object.getOwnPropertyDescriptor(proto,"value")?.set;
            if (setter) setter.call(el, (clear ? "" : el.value) + txt); else el.value += txt;
            el.dispatchEvent(new InputEvent("input",{bubbles:true,inputType:"insertText"}));
            el.dispatchEvent(new Event("change",{bubbles:true}));
            if (enter) el.dispatchEvent(new KeyboardEvent("keydown",{key:"Enter",code:"Enter",bubbles:true}));
          })(${px},${py},${JSON.stringify(text)},${clearFirst},${pressEnter})`);
          return { ok: true, text: `Typed "${text.slice(0, 40)}" at (${px},${py})` };
        }
        case "scroll_document": {
          const dir = args.direction || "down";
          const amt = dir === "down" ? 500 : dir === "up" ? -500 : 0;
          const amtH = dir === "right" ? 500 : dir === "left" ? -500 : 0;
          await ye(`window.scrollBy(${amtH},${amt})`);
          return { ok: true, text: `Scrolled document ${dir}` };
        }
        case "scroll_at": {
          const { px, py } = convertCUCoords(args.x, args.y, vp);
          const dir = args.direction || "down";
          const mag = args.magnitude || 800;
          const amt = (dir === "down" || dir === "right") ? mag : -mag;
          await ye(`(function(x,y,v,h){
            const el = document.elementFromPoint(x,y) || document;
            el.scrollBy ? el.scrollBy(h,v) : window.scrollBy(h,v);
          })(${px},${py},${dir==="up"||dir==="down"?amt:0},${dir==="left"||dir==="right"?amt:0})`);
          return { ok: true, text: `Scrolled ${dir} at (${px},${py})` };
        }
        case "key_combination": {
          const keys = args.keys || "";
          await ye(`(function(k){
            const el = document.activeElement || document.body;
            const parts = k.split("+");
            const key = parts[parts.length-1];
            const ev = { key, code: key, bubbles:true, cancelable:true,
              ctrlKey: parts.includes("Control"), shiftKey: parts.includes("Shift"),
              altKey: parts.includes("Alt"), metaKey: parts.includes("Meta") };
            el.dispatchEvent(new KeyboardEvent("keydown", ev));
            el.dispatchEvent(new KeyboardEvent("keyup", ev));
          })(${JSON.stringify(keys)})`);
          return { ok: true, text: `Pressed keys: ${keys}` };
        }
        case "hover_at": {
          const { px, py } = convertCUCoords(args.x, args.y, vp);
          await ye(`(function(x,y){
            const el = document.elementFromPoint(x,y);
            if (!el) return;
            ["mouseover","mouseenter","mousemove"].forEach(t =>
              el.dispatchEvent(new MouseEvent(t, { bubbles:true, clientX:x, clientY:y })));
          })(${px},${py})`);
          return { ok: true, text: `Hovered at (${px},${py})` };
        }
        case "navigate":
          await yamilPost("/navigate", { url: args.url });
          return { ok: true, text: `Navigated to ${args.url}` };
        case "go_back":
          await ye("history.back()");
          return { ok: true, text: "Went back" };
        case "go_forward":
          await ye("history.forward()");
          return { ok: true, text: "Went forward" };
        case "wait_5_seconds":
          await new Promise(r => setTimeout(r, 5000));
          return { ok: true, text: "Waited 5 seconds" };
        case "drag_and_drop": {
          const src = convertCUCoords(args.x, args.y, vp);
          const dst = convertCUCoords(args.destination_x, args.destination_y, vp);
          await ye(`(function(sx,sy,dx,dy){
            const el = document.elementFromPoint(sx,sy);
            if (!el) return;
            el.dispatchEvent(new MouseEvent("mousedown",{bubbles:true,clientX:sx,clientY:sy}));
            el.dispatchEvent(new MouseEvent("mousemove",{bubbles:true,clientX:dx,clientY:dy}));
            el.dispatchEvent(new MouseEvent("mouseup",{bubbles:true,clientX:dx,clientY:dy}));
            el.dispatchEvent(new DragEvent("drop",{bubbles:true,clientX:dx,clientY:dy}));
          })(${src.px},${src.py},${dst.px},${dst.py})`);
          return { ok: true, text: `Dragged (${src.px},${src.py}) → (${dst.px},${dst.py})` };
        }
        default:
          return { ok: false, text: `Unknown CU action: ${action}` };
      }
    } catch (err) {
      return { ok: false, text: `CU action ${action} failed: ${err.message}` };
    }
  };
}

export { geminiComputerUse, convertCUCoords, buildCUFunctionResponse };

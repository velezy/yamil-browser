export const STEALTH_SCRIPT = `
Object.defineProperty(navigator, 'webdriver', {get: () => undefined, configurable: true});
window.chrome = window.chrome || {};
window.chrome.runtime = window.chrome.runtime || {};
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en'], configurable: true});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5], configurable: true});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8, configurable: true});
const _origQuery = navigator.permissions?.query?.bind(navigator.permissions);
if (_origQuery) {
  navigator.permissions.query = (p) =>
    p.name === 'notifications'
      ? Promise.resolve({state: Notification.permission})
      : _origQuery(p);
}

// ── WebRTC leak prevention ─────────────────────────────────────
const _RTC = window.RTCPeerConnection || window.webkitRTCPeerConnection || window.mozRTCPeerConnection;
if (_RTC) {
  const _origRTC = _RTC.bind(window);
  const handler = {
    construct(target, args) {
      const config = args[0] || {};
      config.iceServers = config.iceServers ? config.iceServers.filter(s =>
        !s.urls || (Array.isArray(s.urls) ? s.urls : [s.urls]).every(u => !u.startsWith('stun:'))
      ) : [];
      return new _origRTC(config);
    }
  };
  window.RTCPeerConnection = new Proxy(_RTC, handler);
  if (window.webkitRTCPeerConnection) window.webkitRTCPeerConnection = window.RTCPeerConnection;
}

// ── Canvas fingerprint noise ───────────────────────────────────
const _toDataURL = HTMLCanvasElement.prototype.toDataURL;
const _toBlob = HTMLCanvasElement.prototype.toBlob;
const _noise = () => (Math.random() - 0.5) * 0.01;
HTMLCanvasElement.prototype.toDataURL = function(...args) {
  const ctx = this.getContext('2d');
  if (ctx && this.width > 0 && this.height > 0) {
    try {
      const px = ctx.getImageData(0, 0, 1, 1);
      px.data[0] = Math.max(0, Math.min(255, px.data[0] + Math.floor(_noise() * 10)));
      ctx.putImageData(px, 0, 0);
    } catch(_) {}
  }
  return _toDataURL.apply(this, args);
};
HTMLCanvasElement.prototype.toBlob = function(...args) {
  const ctx = this.getContext('2d');
  if (ctx && this.width > 0 && this.height > 0) {
    try {
      const px = ctx.getImageData(0, 0, 1, 1);
      px.data[0] = Math.max(0, Math.min(255, px.data[0] + Math.floor(_noise() * 10)));
      ctx.putImageData(px, 0, 0);
    } catch(_) {}
  }
  return _toBlob.apply(this, args);
};

// ── WebGL renderer/vendor spoofing ──────────────────────────────
const _getParam = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(p) {
  if (p === 0x9245) return 'Intel Inc.';
  if (p === 0x9246) return 'Intel Iris OpenGL Engine';
  return _getParam.call(this, p);
};
if (typeof WebGL2RenderingContext !== 'undefined') {
  const _getParam2 = WebGL2RenderingContext.prototype.getParameter;
  WebGL2RenderingContext.prototype.getParameter = function(p) {
    if (p === 0x9245) return 'Intel Inc.';
    if (p === 0x9246) return 'Intel Iris OpenGL Engine';
    return _getParam2.call(this, p);
  };
}
`.trim()

export const LAUNCH_ARGS = [
  '--no-sandbox',
  '--disable-gpu',
  '--disable-dev-shm-usage',
  '--disable-blink-features=AutomationControlled',
  '--disable-automation',
  '--window-size=1920,1080',
  '--lang=en-US',
  '--disable-infobars',
  '--disable-background-timer-throttling',
  '--disable-backgrounding-occluded-windows',
  '--disable-renderer-backgrounding',
]

export const USER_AGENT =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'

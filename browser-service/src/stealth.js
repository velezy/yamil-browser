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

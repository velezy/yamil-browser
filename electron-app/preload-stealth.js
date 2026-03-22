/**
 * Stealth preload script — injected into every renderer BEFORE page scripts run.
 * This defeats bot detection (PerimeterX, Akamai, etc.) by patching browser APIs
 * before the sensor scripts can fingerprint the environment.
 */

// Must run in the page's world (world 0), not isolated world
const { contextBridge } = require('electron')

// We use executeJavaScript from the main process instead,
// but this preload ensures webdriver is hidden at the earliest possible moment.
// Electron sets navigator.webdriver = true by default in some builds.

// The contextBridge can't modify navigator directly, so we use a workaround:
// We'll signal the main process that the preload loaded, and the main process
// injects the stealth script via executeJavaScript at did-start-navigation.

// However, we CAN delete the webdriver property from this preload
// since it runs before page scripts:
try {
  delete Object.getPrototypeOf(navigator).webdriver
} catch (_) {}

/**
 * YAMIL Browser Ad Blocker
 *
 * Lightweight ad/tracker blocking using EasyList-derived domain blocklist.
 * Blocks requests at the Electron session level (webRequest API).
 *
 * Features:
 * - Domain-based blocking (fast hash lookup)
 * - URL pattern blocking for common ad paths
 * - Per-site whitelist
 * - Block count tracking
 */

const fs = require('fs')
const path = require('path')
const { app } = require('electron')

const BLOCKLIST_PATH = path.join(app.getPath('userData'), 'adblock-domains.txt')
const WHITELIST_PATH = path.join(app.getPath('userData'), 'adblock-whitelist.json')

// Common ad/tracker domains (built-in fallback — ~500 most common)
const BUILTIN_DOMAINS = `
doubleclick.net
googleadservices.com
googlesyndication.com
google-analytics.com
googletagmanager.com
googletagservices.com
adservice.google.com
pagead2.googlesyndication.com
ade.googlesyndication.com
facebook.net
connect.facebook.net
graph.facebook.com
pixel.facebook.com
analytics.facebook.com
ad.doubleclick.net
adclick.g.doubleclick.net
stats.g.doubleclick.net
cm.g.doubleclick.net
s0.2mdn.net
z.moatads.com
px.moatads.com
ads.pubmatic.com
image2.pubmatic.com
hbopenbid.pubmatic.com
prebid.adnxs.com
ib.adnxs.com
adnxs.com
secure.adnxs.com
ads.yahoo.com
analytics.yahoo.com
udc.yahoo.com
ads.yieldmo.com
cdn.taboola.com
trc.taboola.com
api.taboola.com
nr-data.net
bam.nr-data.net
bam-cell.nr-data.net
js-agent.newrelic.com
cdn.branch.io
app.link
bnc.lt
adsrvr.org
match.adsrvr.org
insight.adsrvr.org
ads.linkedin.com
px.ads.linkedin.com
analytics.twitter.com
t.co
syndication.twitter.com
static.ads-twitter.com
ads-api.twitter.com
advertising.amazon.com
aax.amazon-adsystem.com
fls-na.amazon.com
c.amazon-adsystem.com
s.amazon-adsystem.com
z-na.amazon-adsystem.com
rcm-na.amazon-adsystem.com
mads.amazon-adsystem.com
aan.amazon.com
unagi.amazon.com
device-metrics-us.amazon.com
amazonaax.com
cdn.krxd.net
beacon.krxd.net
usermatch.krxd.net
pixel.quantserve.com
rules.quantcount.com
secure.quantserve.com
b.scorecardresearch.com
sb.scorecardresearch.com
cdn.optimizely.com
logx.optimizely.com
pixel.wp.com
stats.wp.com
i0.wp.com
r.turn.com
ad.turn.com
cdn.segment.com
api.segment.io
cdn.mxpnl.com
api.mixpanel.com
decide.mixpanel.com
bat.bing.com
c.bing.com
clarity.ms
www.clarity.ms
tags.tiqcdn.com
collect.tealiumiq.com
datacloud.tealiumiq.com
cdn.heapanalytics.com
heapanalytics.com
cdn.mouseflow.com
o2.mouseflow.com
script.hotjar.com
static.hotjar.com
vars.hotjar.com
insights.hotjar.com
cdn.cookielaw.org
optanon.blob.core.windows.net
cdn.onetrust.com
geolocation.onetrust.com
bat.r.msn.com
flex.msn.com
a.ads.msn.com
adnexus.net
serving-sys.com
bs.serving-sys.com
ds.serving-sys.com
eyeblaster.com
media.net
contextual.media.net
static.media.net
hb.adscale.de
js.ad-score.com
ssp.lkqd.net
ad.lkqd.net
cdn.districtm.io
prebid.districtm.io
ads.stickyadstv.com
cdn.stickyadstv.com
match.prod.bidr.io
ads.rubiconproject.com
fastlane.rubiconproject.com
optimized-by.rubiconproject.com
pixel.rubiconproject.com
casalemedia.com
htlb.casalemedia.com
x.bidswitch.net
ssp.bidswitch.com
match.sharethrough.com
native.sharethrough.com
btloader.com
cdn.btloader.com
tpc.googlesyndication.com
fundingchoicesmessages.google.com
adserver.snapads.com
tr.snapchat.com
sc-static.net
ads.tiktok.com
analytics.tiktok.com
log.byteoversea.com
mon.byteoversea.com
`.trim().split('\n').map(d => d.trim()).filter(Boolean)

// Common ad URL patterns
const AD_PATH_PATTERNS = [
  /\/ads\//i,
  /\/ad\//i,
  /\/adserve/i,
  /\/doubleclick/i,
  /\/pagead\//i,
  /\/adview/i,
  /\/pixel\.gif/i,
  /\/beacon\?/i,
  /\/track(er|ing)?\?/i,
  /\/collect\?/i,
  /\.gif\?.*utm_/i,
  /\/prebid/i,
  /\/rtb/i,
]

class AdBlocker {
  constructor () {
    this.blockedDomains = new Set(BUILTIN_DOMAINS)
    this.whitelist = new Set()
    this.enabled = true
    this.blockCount = 0
    this.sessionBlockCounts = new Map() // domain → count
    this._loadWhitelist()
    this._loadCustomBlocklist()
  }

  _loadWhitelist () {
    try {
      const data = JSON.parse(fs.readFileSync(WHITELIST_PATH, 'utf8'))
      if (Array.isArray(data)) data.forEach(d => this.whitelist.add(d))
    } catch (_) {}
  }

  _saveWhitelist () {
    try {
      fs.writeFileSync(WHITELIST_PATH, JSON.stringify([...this.whitelist], null, 2))
    } catch (_) {}
  }

  _loadCustomBlocklist () {
    try {
      const data = fs.readFileSync(BLOCKLIST_PATH, 'utf8')
      data.split('\n').forEach(line => {
        line = line.trim()
        if (line && !line.startsWith('#')) this.blockedDomains.add(line)
      })
    } catch (_) {}
  }

  /** Check if a URL should be blocked */
  shouldBlock (url, pageUrl) {
    if (!this.enabled) return false

    let hostname
    try { hostname = new URL(url).hostname } catch { return false }

    // Check whitelist (by page domain, not request domain)
    if (pageUrl) {
      try {
        const pageDomain = new URL(pageUrl).hostname.replace(/^www\./, '')
        if (this.whitelist.has(pageDomain)) return false
      } catch {}
    }

    // Domain check
    const parts = hostname.split('.')
    for (let i = 0; i < parts.length - 1; i++) {
      const domain = parts.slice(i).join('.')
      if (this.blockedDomains.has(domain)) {
        this.blockCount++
        const pageDomain = pageUrl ? (() => { try { return new URL(pageUrl).hostname } catch { return 'unknown' } })() : 'unknown'
        this.sessionBlockCounts.set(pageDomain, (this.sessionBlockCounts.get(pageDomain) || 0) + 1)
        return true
      }
    }

    // URL path pattern check (only for third-party requests)
    if (pageUrl) {
      try {
        const pageHost = new URL(pageUrl).hostname
        if (hostname !== pageHost && AD_PATH_PATTERNS.some(p => p.test(url))) {
          this.blockCount++
          return true
        }
      } catch {}
    }

    return false
  }

  /** Install on an Electron session */
  install (ses) {
    ses.webRequest.onBeforeRequest((details, callback) => {
      if (this.shouldBlock(details.url, details.referrer || details.url)) {
        callback({ cancel: true })
      } else {
        callback({})
      }
    })
  }

  addWhitelist (domain) {
    this.whitelist.add(domain.replace(/^www\./, ''))
    this._saveWhitelist()
  }

  removeWhitelist (domain) {
    this.whitelist.delete(domain.replace(/^www\./, ''))
    this._saveWhitelist()
  }

  getStats () {
    return {
      enabled: this.enabled,
      totalBlocked: this.blockCount,
      blockedDomainCount: this.blockedDomains.size,
      whitelistCount: this.whitelist.size,
      whitelist: [...this.whitelist],
      topBlocked: [...this.sessionBlockCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([domain, count]) => ({ domain, count }))
    }
  }

  toggle () {
    this.enabled = !this.enabled
    return this.enabled
  }
}

module.exports = { AdBlocker }

import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import { registerRoutes } from './routes.js'
import { registerWebSockets } from './ws.js'
import { probeOllama, initDb } from './knowledge.js'
import { probeVision } from './vision.js'
import { initWebhooks } from './webhooks.js'
import { authHook } from './api-keys.js'

const PORT = parseInt(process.env.PORT || '4000')
const HOST = process.env.HOST || '0.0.0.0'

const isDev = process.env.NODE_ENV !== 'production'

const app = Fastify({
  logger: isDev
    ? { transport: { target: 'pino-pretty', options: { colorize: true } } }
    : true,
})

await app.register(websocket)

// API key auth — enforced for non-localhost when keys are configured
app.addHook('preHandler', authHook)

await registerRoutes(app)
await registerWebSockets(app)

// Initialize knowledge pipeline + vision + webhooks: database + Ollama models
initDb().catch(e => console.error('[KNOWLEDGE] DB init failed:', e.message))
initWebhooks().catch(e => console.error('[WEBHOOKS] Init failed:', e.message))
probeOllama().catch(() => {})
probeVision().catch(() => {})

await app.listen({ port: PORT, host: HOST })

import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import { registerRoutes } from './routes.js'
import { registerWebSockets } from './ws.js'
import { probeOllama } from './knowledge.js'

const PORT = parseInt(process.env.PORT || '4000')
const HOST = process.env.HOST || '0.0.0.0'

const isDev = process.env.NODE_ENV !== 'production'

const app = Fastify({
  logger: isDev
    ? { transport: { target: 'pino-pretty', options: { colorize: true } } }
    : true,
})

await app.register(websocket)
await registerRoutes(app)
await registerWebSockets(app)

// Probe Ollama for knowledge pipeline models (non-blocking)
probeOllama().catch(() => {})

await app.listen({ port: PORT, host: HOST })

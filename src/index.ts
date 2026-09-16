import { Hono } from 'hono'
import { granolaHandler } from './routes/granola'
import { ingestGranola, type IngestGranola } from './webhook'

export const createApp = (ingest: IngestGranola) => {
  const app = new Hono<{ Bindings: CloudflareBindings }>()
  app.post('/webhooks/granola', granolaHandler(ingest))
  app.onError((_error, c) => c.json({ error: 'internal_error' }, 500))
  return app
}

export default createApp(ingestGranola)

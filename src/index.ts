import { Hono } from 'hono'
import { Effect, Layer } from 'effect'
import { EventQueueLive } from './event-queue'
import { granolaHandler } from './routes/granola'
import { GranolaIngestionLive, type GranolaIngestion, type GranolaConfigurationError } from './granola'

// Secrets are supplied by Cloudflare, not checked into Wrangler configuration.
export type Bindings = CloudflareBindings & { GRANOLA_SIGNING_SECRET?: string }

export const createApp = (
  ingestion?: Layer.Layer<GranolaIngestion, GranolaConfigurationError>,
) => {
  const app = new Hono<{ Bindings: Bindings }>()
  app.get('/', (c) => c.text('Hello Hono!'))
  app.post('/webhooks/granola', (c) => {
    const dependencies = Layer.merge(
      ingestion ?? GranolaIngestionLive(c.env.GRANOLA_SIGNING_SECRET),
      EventQueueLive(c.env.EVENTS),
    )
    return Effect.runPromise(granolaHandler(c.req.raw).pipe(
      Effect.provide(dependencies),
      Effect.catchAllCause(() => Effect.succeed(c.json({ error: 'internal_error' }, 500))),
    ))
  })
  app.onError((_error, c) => c.json({ error: 'internal_error' }, 500))
  return app
}

export default createApp()

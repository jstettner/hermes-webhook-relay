import { Hono } from 'hono'
import { Effect, Layer } from 'effect'
import { EventQueueLive, type EventQueue } from './event-queue'
import { WebhookProvidersLive, type ProviderSecrets, type WebhookProviders } from './providers'
import { webhookHandler } from './routes/webhook'

// Secrets are supplied by Cloudflare, not checked into Wrangler configuration.
// WEBHOOK_PROVIDERS is validated at runtime, so widen its generated literal type.
export type Bindings = Omit<CloudflareBindings, 'WEBHOOK_PROVIDERS'> & ProviderSecrets & {
  WEBHOOK_PROVIDERS?: unknown
}

// The single service boundary: everything a request needs, built from the bindings.
export type AppLayer = (env: Bindings) => Layer.Layer<WebhookProviders | EventQueue>

export const AppLive: AppLayer = (env) =>
  Layer.merge(WebhookProvidersLive(env), EventQueueLive(env.EVENTS))

// Ingestion has its own 10 s timeout (INGESTION_TIMEOUT); this outer bound also
// covers the queue send, which has none, and leaves headroom for the inner one to fire first.
const REQUEST_TIMEOUT = '12 seconds'

export const createApp = (makeLayer: AppLayer) => {
  const app = new Hono<{ Bindings: Bindings }>()
  app.get('/', (c) => c.text('Hello Hono!'))
  app.post('/webhooks/:provider', (c) =>
    Effect.runPromise(webhookHandler(c.req.param('provider'))(c.req.raw).pipe(
      Effect.provide(makeLayer(c.env)),
      Effect.timeout(REQUEST_TIMEOUT),
      Effect.catchAllCause(() => Effect.succeed(c.json({ error: 'internal_error' }, 500))),
    )))
  app.onError((_error, c) => c.json({ error: 'internal_error' }, 500))
  return app
}

export default createApp(AppLive)

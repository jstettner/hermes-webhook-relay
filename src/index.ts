import { Hono } from 'hono'
import { ConfigProvider, Effect, Layer, type Config } from 'effect'
import { EventQueue } from './event-queue'
import { WebhookProviders, type ProviderSecrets } from './providers'
import { webhookHandler } from './routes/webhook'

// Per-deployment values are set as Cloudflare secrets (or in .dev.vars locally),
// not checked into Wrangler configuration. They are read through Effect Config.
export type Bindings = CloudflareBindings & ProviderSecrets & {
  WEBHOOK_PROVIDERS?: string | ReadonlyArray<string>
}

// The single service boundary: everything a request needs, built from the bindings.
// Construction fails on a malformed WEBHOOK_PROVIDERS, which the edge maps to 500.
export type AppLayer = (env: Bindings) => Layer.Layer<WebhookProviders | EventQueue, Config.ConfigError>

// Config reads the bindings directly, never process.env. Empty strings count as unset.
export const AppLive: AppLayer = (env) =>
  Layer.mergeAll(WebhookProviders.layer, EventQueue.layer(env.EVENTS)).pipe(
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(env))),
  )

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
      Effect.catchCause(() => Effect.succeed(c.json({ error: 'internal_error' }, 500))),
    )))
  app.onError((_error, c) => c.json({ error: 'internal_error' }, 500))
  return app
}

export default createApp(AppLive)

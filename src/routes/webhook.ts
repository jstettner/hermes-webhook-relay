import { Effect } from 'effect'
import { EventQueue } from '../event-queue'
import { WebhookProviders } from '../providers'
import { WebhookIngestionFailed } from '../webhook'

// Bounds header checks, body reading and verification. The queue send is
// bounded separately by the request timeout at the app boundary.
const INGESTION_TIMEOUT = '10 seconds'

const acceptWebhook = (provider: string, request: Request) =>
  Effect.gen(function* () {
    const ingestion = yield* (yield* WebhookProviders).resolve(provider)
    const event = yield* ingestion.ingest(request).pipe(
      Effect.timeoutFail({ duration: INGESTION_TIMEOUT, onTimeout: () => new WebhookIngestionFailed() }),
    )
    const queue = yield* EventQueue
    yield* queue.enqueue(event)
  })

// One error → HTTP mapping for every provider, so they cannot drift apart.
export const webhookHandler = (provider: string) => (request: Request) =>
  Effect.suspend(() => acceptWebhook(provider, request)).pipe(
    Effect.as(new Response(null, { status: 202 })),
    Effect.catchTags({
      WebhookProviderNotFound: () => Effect.succeed(Response.json({ error: 'not_found' }, { status: 404 })),
      WebhookConfigurationError: () => Effect.succeed(Response.json({ error: 'internal_error' }, { status: 500 })),
      WebhookUnauthorized: () => Effect.succeed(Response.json({ error: 'unauthorized' }, { status: 401 })),
      WebhookInvalidPayload: () => Effect.succeed(Response.json({ error: 'invalid_payload' }, { status: 400 })),
      WebhookBodyTooLarge: () => Effect.succeed(Response.json({ error: 'body_too_large' }, { status: 413 })),
      WebhookIngestionFailed: () => Effect.succeed(Response.json({ error: 'internal_error' }, { status: 500 })),
      EnqueueFailed: () => Effect.succeed(Response.json({ error: 'enqueue_failed' }, { status: 503 })),
    }),
  )

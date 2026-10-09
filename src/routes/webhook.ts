import { Context, Effect } from 'effect'
import { EventQueue } from '../event-queue'
import type { WebhookIngestion } from '../webhook'

const acceptWebhook = <I>(tag: Context.Tag<I, WebhookIngestion>, request: Request) =>
  Effect.gen(function* () {
    const ingestion = yield* tag
    const event = yield* ingestion.ingest(request)
    const queue = yield* EventQueue
    yield* queue.enqueue(event)
  })

// One error → HTTP mapping for every provider, so they cannot drift apart.
export const webhookHandler = <I>(tag: Context.Tag<I, WebhookIngestion>) => (request: Request) =>
  Effect.suspend(() => acceptWebhook(tag, request)).pipe(
    Effect.as(new Response(null, { status: 202 })),
    Effect.catchTags({
      WebhookUnauthorized: () => Effect.succeed(Response.json({ error: 'unauthorized' }, { status: 401 })),
      WebhookInvalidPayload: () => Effect.succeed(Response.json({ error: 'invalid_payload' }, { status: 400 })),
      WebhookBodyTooLarge: () => Effect.succeed(Response.json({ error: 'body_too_large' }, { status: 413 })),
      WebhookIngestionFailed: () => Effect.succeed(Response.json({ error: 'internal_error' }, { status: 500 })),
      EnqueueFailed: () => Effect.succeed(Response.json({ error: 'enqueue_failed' }, { status: 503 })),
    }),
  )

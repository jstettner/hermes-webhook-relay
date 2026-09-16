import { Effect } from 'effect'
import type { Handler } from 'hono'
import { EventQueueLive } from '../event-queue'
import { acceptWebhook, type IngestGranola } from '../webhook'

export const granolaHandler = (ingest: IngestGranola): Handler<{ Bindings: CloudflareBindings }> =>
  async (c) => {
    const program = Effect.suspend(() => acceptWebhook(c.req.raw, ingest)).pipe(
      Effect.provide(EventQueueLive(c.env.EVENTS)),
      Effect.as(new Response(null, { status: 202 })),
      Effect.catchTags({
        IngestionUnavailable: () => Effect.succeed(c.json({ error: 'ingestion_unavailable' }, 503)),
        EnqueueFailed: () => Effect.succeed(c.json({ error: 'enqueue_failed' }, 503)),
      }),
      Effect.catchAllCause(() => Effect.succeed(c.json({ error: 'internal_error' }, 500))),
    )
    return Effect.runPromise(program)
  }

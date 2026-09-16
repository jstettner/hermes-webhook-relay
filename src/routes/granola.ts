import { Effect } from 'effect'
import { GranolaIngestion } from '../granola'
import { EventQueue } from '../event-queue'

const acceptWebhook = (request: Request) =>
  Effect.gen(function* () {
    const ingestion = yield* GranolaIngestion
    const event = yield* ingestion.ingest(request)
    const queue = yield* EventQueue
    yield* queue.enqueue(event)
  })

export const granolaHandler = (request: Request) =>
  Effect.suspend(() => acceptWebhook(request)).pipe(
    Effect.as(new Response(null, { status: 202 })),
    Effect.catchTags({
      IngestionUnavailable: () => Effect.succeed(Response.json({ error: 'ingestion_unavailable' }, { status: 503 })),
      EnqueueFailed: () => Effect.succeed(Response.json({ error: 'enqueue_failed' }, { status: 503 })),
    }),
  )

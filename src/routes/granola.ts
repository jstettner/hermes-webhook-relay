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
      GranolaUnauthorized: () => Effect.succeed(Response.json({ error: 'unauthorized' }, { status: 401 })),
      GranolaInvalidPayload: () => Effect.succeed(Response.json({ error: 'invalid_payload' }, { status: 400 })),
      GranolaBodyTooLarge: () => Effect.succeed(Response.json({ error: 'body_too_large' }, { status: 413 })),
      GranolaIngestionFailed: () => Effect.succeed(Response.json({ error: 'internal_error' }, { status: 500 })),
      EnqueueFailed: () => Effect.succeed(Response.json({ error: 'enqueue_failed' }, { status: 503 })),
    }),
  )

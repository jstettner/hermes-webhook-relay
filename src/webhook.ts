import { Data, Effect } from 'effect'
import { EventQueue, type EventEnvelope } from './event-queue'

export class IngestionUnavailable extends Data.TaggedError('IngestionUnavailable')<{}> {}

// Implementations must authenticate and validate before yielding metadata.
export type IngestGranola = (request: Request) => Effect.Effect<EventEnvelope, IngestionUnavailable>

export const ingestGranola: IngestGranola = () => Effect.fail(new IngestionUnavailable())

export const acceptWebhook = (request: Request, ingest: IngestGranola) =>
  Effect.gen(function* () {
    const event = yield* ingest(request)
    const queue = yield* EventQueue
    yield* queue.enqueue(event)
  })

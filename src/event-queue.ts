import { Context, Data, DateTime, Effect, Either, Layer, Schema } from 'effect'

const Identifier = Schema.String.pipe(Schema.minLength(1), Schema.maxLength(512))
const decodeTimestamp = Schema.decodeUnknownEither(Schema.DateTimeUtc)
const SourceTimestamp = Schema.String.pipe(
  Schema.pattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/),
  Schema.filter((value) => {
    const decoded = decodeTimestamp(value)
    // Reject calendar rollover while preserving the provider's original string.
    return Either.isRight(decoded) && DateTime.formatIso(decoded.right) ===
      (value.length === 20 ? value.replace('Z', '.000Z') : value)
  }, { message: () => 'Expected a valid UTC ISO timestamp' }),
)

export const EventEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  provider: Schema.Literal('granola'),
  eventId: Identifier,
  eventType: Identifier,
  sourceRecordId: Identifier,
  sourceTimestamp: SourceTimestamp,
})

export type EventEnvelope = typeof EventEnvelope.Type

export class EnqueueFailed extends Data.TaggedError('EnqueueFailed')<{}> {}

export class EventQueue extends Context.Tag('EventQueue')<EventQueue, {
  readonly enqueue: (event: EventEnvelope) => Effect.Effect<void, EnqueueFailed>
}>() {}

export const EventQueueLive = (binding: Pick<Queue<EventEnvelope>, 'send'>) =>
  Layer.succeed(EventQueue, {
    enqueue: (event) => Effect.tryPromise({
      try: async () => { await binding.send({
        version: event.version,
        provider: event.provider,
        eventId: event.eventId,
        eventType: event.eventType,
        sourceRecordId: event.sourceRecordId,
        sourceTimestamp: event.sourceTimestamp,
      }, { contentType: 'json' }) },
      catch: () => new EnqueueFailed(),
    }),
  })

export const decodeEventEnvelope = Schema.decodeUnknown(EventEnvelope, {
  onExcessProperty: 'error',
})

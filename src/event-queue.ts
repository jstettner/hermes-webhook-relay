import { Context, DateTime, Effect, Exit, Layer, Schema } from 'effect'

const Identifier = Schema.String.pipe(Schema.check(Schema.isMinLength(1), Schema.isMaxLength(512)))
const decodeTimestamp = Schema.decodeUnknownExit(Schema.DateTimeUtcFromString)
const SourceTimestamp = Schema.String.pipe(Schema.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/),
  Schema.makeFilter((value: string) => {
    const decoded = decodeTimestamp(value)
    // Reject calendar rollover while preserving the provider's original string.
    return Exit.isSuccess(decoded) && DateTime.formatIso(decoded.value) ===
      (value.length === 20 ? value.replace('Z', '.000Z') : value)
  }, { message: 'Expected a valid UTC ISO timestamp' }),
))

export const EventEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  provider: Schema.Literals(['granola', 'pocket']),
  eventId: Identifier,
  eventType: Identifier,
  sourceRecordId: Identifier,
  sourceTimestamp: SourceTimestamp,
})

export type EventEnvelope = typeof EventEnvelope.Type

export class EnqueueFailed extends Schema.TaggedError<EnqueueFailed>()('EnqueueFailed', {}) {}

export class EventQueue extends Context.Service<EventQueue, {
  readonly enqueue: (event: EventEnvelope) => Effect.Effect<void, EnqueueFailed>
}>()('hermes-webhook-relay/EventQueue') {
  // A function of the binding rather than Config: the queue is an object, not a value.
  static readonly layer = (binding: Pick<Queue<EventEnvelope>, 'send'>) =>
    Layer.succeed(EventQueue, EventQueue.of({
      enqueue: Effect.fn('EventQueue.enqueue')(function* (event: EventEnvelope) {
        yield* Effect.tryPromise({
          try: async () => { await binding.send({
            version: event.version,
            provider: event.provider,
            eventId: event.eventId,
            eventType: event.eventType,
            sourceRecordId: event.sourceRecordId,
            sourceTimestamp: event.sourceTimestamp,
          }, { contentType: 'json' }) },
          catch: () => new EnqueueFailed(),
        })
      }),
    }))
}

export const decodeEventEnvelope = Schema.decodeUnknownEffect(EventEnvelope, {
  onExcessProperty: 'error',
})

import { Clock, Context, Data, Effect, Either, Encoding, Layer, Redacted, Schema, Stream } from 'effect'
import { EventEnvelope } from './event-queue'

export class GranolaConfig extends Context.Tag('GranolaConfig')<GranolaConfig, {
  readonly signingSecret: Redacted.Redacted<string>
}>() {}

export class GranolaConfigurationError extends Data.TaggedError('GranolaConfigurationError')<{}> {}
export class GranolaUnauthorized extends Data.TaggedError('GranolaUnauthorized')<{}> {}
export class GranolaInvalidPayload extends Data.TaggedError('GranolaInvalidPayload')<{}> {}
export class GranolaBodyTooLarge extends Data.TaggedError('GranolaBodyTooLarge')<{}> {}
export class GranolaIngestionFailed extends Data.TaggedError('GranolaIngestionFailed')<{}> {}

export type GranolaIngestionError = GranolaUnauthorized | GranolaInvalidPayload |
  GranolaBodyTooLarge | GranolaIngestionFailed

export const GranolaConfigLive = (secret: string | undefined) =>
  Layer.effect(GranolaConfig, Effect.suspend(() =>
    secret === undefined || secret.length === 0
      ? Effect.fail(new GranolaConfigurationError())
      : Effect.succeed({ signingSecret: Redacted.make(secret) }),
  ))

export class GranolaIngestion extends Context.Tag('GranolaIngestion')<GranolaIngestion, {
  readonly ingest: (request: Request) => Effect.Effect<EventEnvelope, GranolaIngestionError>
}>() {}

const MAX_BODY_BYTES = 64 * 1024
const Identifier = Schema.String.pipe(Schema.pattern(/^[A-Za-z0-9_-]{1,512}$/))
const fields = {
  event_id: Identifier,
  note_id: Schema.String.pipe(Schema.pattern(/^not_[a-zA-Z0-9]{14}$/)),
  occurred_at: EventEnvelope.fields.sourceTimestamp,
}
// Ignore additive provider fields, but only normalize explicitly selected metadata.
const Payload = Schema.Union(
  Schema.Struct({ ...fields, event_type: Schema.Literal('note.generated', 'note.access_granted') }),
  Schema.Struct({
    ...fields,
    event_type: Schema.Literal('note.edited'),
    data: Schema.Struct({ changed_fields: Schema.Tuple(Schema.Literal('summary')) }),
  }),
)

const readBody = (request: Request) => Effect.suspend(() => {
  const body = request.body
  if (!body) return Effect.succeed(new Uint8Array())
  if (body.locked) return Effect.fail(new GranolaInvalidPayload())
  const buffer = new Uint8Array(MAX_BODY_BYTES)
  return Stream.fromReadableStream({
    evaluate: () => body,
    onError: () => new GranolaInvalidPayload(),
    releaseLockOnEnd: true,
  }).pipe(
    Stream.runFoldEffect(0, (length, chunk) => {
      const nextLength = length + chunk.byteLength
      if (nextLength > MAX_BODY_BYTES) return Effect.fail(new GranolaBodyTooLarge())
      return Effect.sync(() => {
        buffer.set(chunk, length)
        return nextLength
      })
    }),
    // Stream releases its reader before this finalizer. Cancellation must not
    // block a timeout or replace the original error if the source rejects it.
    Effect.ensuring(Effect.sync(() => { void body.cancel().catch(() => {}) })),
    Effect.map((length) => buffer.slice(0, length)),
  )
})

export const GranolaIngestionFromConfig = Layer.effect(GranolaIngestion,
  Effect.gen(function* () {
    const config = yield* GranolaConfig
    // Decode and import once per layer construction; never attach secrets to errors.
    const key = yield* Effect.tryPromise({
      try: async () => {
        const secret = Redacted.value(config.signingSecret)
        if (!secret.startsWith('whsec_') || secret.length > 1024) throw new Error('Invalid secret')
        const encoded = secret.slice(6)
        const decoded = Encoding.decodeBase64(encoded)
        if (Either.isLeft(decoded) || decoded.right.length === 0 ||
          Encoding.encodeBase64(decoded.right) !== encoded) throw new Error('Invalid secret')
        return crypto.subtle.importKey('raw', decoded.right,
          { name: 'HMAC', hash: 'SHA-256' }, false, ['verify'])
      },
      catch: () => new GranolaConfigurationError(),
    })
    return {
      ingest: (request: Request) => Effect.gen(function* () {
        const id = request.headers.get('webhook-id')
        const timestamp = request.headers.get('webhook-timestamp')
        const signatures = request.headers.get('webhook-signature')
        if (!id || !/^[A-Za-z0-9_-]{1,512}$/.test(id) ||
          !timestamp || !/^\d{1,12}$/.test(timestamp) ||
          !signatures || signatures.length > 4096) {
          return yield* Effect.fail(new GranolaUnauthorized())
        }
        const entries = signatures.split(' ')
        if (entries.length > 16) return yield* Effect.fail(new GranolaUnauthorized())
        const now = yield* Clock.currentTimeMillis
        if (Math.abs(Math.floor(now / 1000) - Number(timestamp)) > 300) {
          return yield* Effect.fail(new GranolaUnauthorized())
        }
        const candidates = entries.flatMap((entry) => {
          const match = /^v1,([^,]+)$/.exec(entry)
          if (!match) return []
          const decoded = Encoding.decodeBase64(match[1])
          if (Either.isLeft(decoded) || decoded.right.length !== 32 ||
            Encoding.encodeBase64(decoded.right) !== match[1]) return []
          return [decoded.right]
        })
        if (!candidates.length) return yield* Effect.fail(new GranolaUnauthorized())
        const body = yield* readBody(request)
        const prefix = new TextEncoder().encode(`${id}.${timestamp}.`)
        const signed = new Uint8Array(prefix.length + body.length)
        signed.set(prefix)
        signed.set(body, prefix.length)
        const verified = yield* Effect.tryPromise({
          try: async () => {
            for (const signature of candidates) {
              if (await crypto.subtle.verify('HMAC', key, signature, signed)) return true
            }
            return false
          },
          catch: () => new GranolaIngestionFailed(),
        })
        if (!verified) return yield* Effect.fail(new GranolaUnauthorized())
        const text = yield* Effect.try({
          try: () => new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(body),
          catch: () => new GranolaInvalidPayload(),
        })
        const payload = yield* Schema.decodeUnknown(Schema.parseJson(Payload))(text).pipe(
          Effect.mapError(() => new GranolaInvalidPayload()),
        )
        if (payload.event_id !== id) return yield* Effect.fail(new GranolaInvalidPayload())
        return {
          version: 1 as const,
          provider: 'granola' as const,
          eventId: payload.event_id,
          eventType: payload.event_type,
          sourceRecordId: payload.note_id,
          sourceTimestamp: payload.occurred_at,
        }
      }).pipe(Effect.timeoutFail({ duration: '10 seconds', onTimeout: () => new GranolaIngestionFailed() })),
    }
  }),
)

// Encapsulate the live configuration wiring; callers provide only the binding.
export const GranolaIngestionLive = (secret: string | undefined) =>
  GranolaIngestionFromConfig.pipe(Layer.provide(GranolaConfigLive(secret)))

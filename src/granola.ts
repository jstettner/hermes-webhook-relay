import { Clock, Effect, Either, Encoding, Redacted, Schema } from 'effect'
import { EventEnvelope } from './event-queue'
import { readBody, WebhookConfigurationError, WebhookIngestionFailed, WebhookInvalidPayload, WebhookUnauthorized, type WebhookIngestion } from './webhook'

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

export const GranolaIngestionLive = (signingSecret: Redacted.Redacted<string>): Effect.Effect<WebhookIngestion, WebhookConfigurationError> =>
  Effect.gen(function* () {
    // Decode and import once per construction; never attach secrets to errors.
    const key = yield* Effect.tryPromise({
      try: async () => {
        const secret = Redacted.value(signingSecret)
        if (!secret.startsWith('whsec_') || secret.length > 1024) throw new Error('Invalid secret')
        const encoded = secret.slice(6)
        const decoded = Encoding.decodeBase64(encoded)
        if (Either.isLeft(decoded) || decoded.right.length === 0 ||
          Encoding.encodeBase64(decoded.right) !== encoded) throw new Error('Invalid secret')
        return crypto.subtle.importKey('raw', decoded.right,
          { name: 'HMAC', hash: 'SHA-256' }, false, ['verify'])
      },
      catch: () => new WebhookConfigurationError(),
    })
    return {
      ingest: (request: Request) => Effect.gen(function* () {
        const id = request.headers.get('webhook-id')
        const timestamp = request.headers.get('webhook-timestamp')
        const signatures = request.headers.get('webhook-signature')
        if (!id || !/^[A-Za-z0-9_-]{1,512}$/.test(id) ||
          !timestamp || !/^\d{1,12}$/.test(timestamp) ||
          !signatures || signatures.length > 4096) {
          return yield* Effect.fail(new WebhookUnauthorized())
        }
        const entries = signatures.split(' ')
        if (entries.length > 16) return yield* Effect.fail(new WebhookUnauthorized())
        const now = yield* Clock.currentTimeMillis
        if (Math.abs(Math.floor(now / 1000) - Number(timestamp)) > 300) {
          return yield* Effect.fail(new WebhookUnauthorized())
        }
        const candidates = entries.flatMap((entry) => {
          const match = /^v1,([^,]+)$/.exec(entry)
          if (!match) return []
          const decoded = Encoding.decodeBase64(match[1])
          if (Either.isLeft(decoded) || decoded.right.length !== 32 ||
            Encoding.encodeBase64(decoded.right) !== match[1]) return []
          return [decoded.right]
        })
        if (!candidates.length) return yield* Effect.fail(new WebhookUnauthorized())
        const body = yield* readBody(request, MAX_BODY_BYTES)
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
          catch: () => new WebhookIngestionFailed(),
        })
        if (!verified) return yield* Effect.fail(new WebhookUnauthorized())
        const text = yield* Effect.try({
          try: () => new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(body),
          catch: () => new WebhookInvalidPayload(),
        })
        const payload = yield* Schema.decodeUnknown(Schema.parseJson(Payload))(text).pipe(
          Effect.mapError(() => new WebhookInvalidPayload()),
        )
        if (payload.event_id !== id) return yield* Effect.fail(new WebhookInvalidPayload())
        return {
          version: 1 as const,
          provider: 'granola' as const,
          eventId: payload.event_id,
          eventType: payload.event_type,
          sourceRecordId: payload.note_id,
          sourceTimestamp: payload.occurred_at,
        }
      }),
    }
  })


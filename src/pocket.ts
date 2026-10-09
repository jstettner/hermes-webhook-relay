import { Clock, Effect, Encoding, Either, Redacted, Schema } from 'effect'
import { EventEnvelope } from './event-queue'
import { readBody, WebhookConfigurationError, WebhookIngestionFailed, WebhookInvalidPayload, WebhookUnauthorized, type WebhookIngestion } from './webhook'

// Pocket sends the full transcript and summaries, so allow long meetings.
const MAX_BODY_BYTES = 2 * 1024 * 1024
const FRESHNESS_MS = 300 * 1000
// Declare only the metadata we forward; excess keys (transcript, summaries,
// user email) are ignored by the decoder and never copied anywhere.
const Payload = Schema.Struct({
  event: Schema.Literal(
    'transcription.completed', 'summary.completed', 'summary.regenerated', 'summary.updated',
    'mind_map.completed', 'action_items.regenerated', 'speakers.labeled',
    'transcript.edited', 'action_items.updated', 'recording.created', 'recording.deleted',
    'recording.merged', 'translation.completed',
  ),
  timestamp: EventEnvelope.fields.sourceTimestamp,
  recording: Schema.Struct({ id: Schema.String.pipe(Schema.pattern(/^[A-Za-z0-9_-]{1,512}$/)) }),
})

export const PocketIngestionLive = (signingSecret: Redacted.Redacted<string>): Effect.Effect<WebhookIngestion, WebhookConfigurationError> =>
  Effect.gen(function* () {
    // Pocket uses the secret string itself as the HMAC key; never attach it to errors.
    const key = yield* Effect.tryPromise({
      try: async () => {
        const secret = Redacted.value(signingSecret)
        if (secret.length > 1024) throw new Error('Invalid secret')
        return crypto.subtle.importKey('raw', new TextEncoder().encode(secret),
          { name: 'HMAC', hash: 'SHA-256' }, false, ['verify'])
      },
      catch: () => new WebhookConfigurationError(),
    })
    return {
      ingest: (request: Request) => Effect.gen(function* () {
        const timestamp = request.headers.get('x-heypocket-timestamp')
        const signature = request.headers.get('x-heypocket-signature')
        // Missing headers also reject legacy webhooks that were created without a secret.
        if (!timestamp || !/^\d{1,15}$/.test(timestamp) || !signature) {
          return yield* Effect.fail(new WebhookUnauthorized())
        }
        const now = yield* Clock.currentTimeMillis
        if (Math.abs(now - Number(timestamp)) > FRESHNESS_MS) {
          return yield* Effect.fail(new WebhookUnauthorized())
        }
        const hex = /^(?:sha256=)?([0-9a-f]{64})$/i.exec(signature)
        const digest = hex && Encoding.decodeHex(hex[1].toLowerCase())
        if (!digest || Either.isLeft(digest)) return yield* Effect.fail(new WebhookUnauthorized())
        const body = yield* readBody(request, MAX_BODY_BYTES)
        const prefix = new TextEncoder().encode(`${timestamp}.`)
        const signed = new Uint8Array(prefix.length + body.length)
        signed.set(prefix)
        signed.set(body, prefix.length)
        const verified = yield* Effect.tryPromise({
          try: () => crypto.subtle.verify('HMAC', key, digest.right, signed),
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
        // Pocket sends no event ID. Derive one from signed body fields rather than the
        // delivery header, so a retry that is re-signed with a new header timestamp
        // keeps the same ID as long as Pocket reuses the body.
        const eventId = yield* Effect.tryPromise({
          try: async () => Encoding.encodeHex(new Uint8Array(await crypto.subtle.digest('SHA-256',
            new TextEncoder().encode(`${payload.event}\n${payload.recording.id}\n${payload.timestamp}`)))),
          catch: () => new WebhookIngestionFailed(),
        })
        return {
          version: 1 as const,
          provider: 'pocket' as const,
          eventId,
          eventType: payload.event,
          sourceRecordId: payload.recording.id,
          sourceTimestamp: payload.timestamp,
        }
      }),
    }
  })


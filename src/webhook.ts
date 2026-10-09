import { Data, Effect, Stream } from 'effect'
import { EventEnvelope } from './event-queue'

export class WebhookUnauthorized extends Data.TaggedError('WebhookUnauthorized')<{}> {}
export class WebhookInvalidPayload extends Data.TaggedError('WebhookInvalidPayload')<{}> {}
export class WebhookBodyTooLarge extends Data.TaggedError('WebhookBodyTooLarge')<{}> {}
export class WebhookIngestionFailed extends Data.TaggedError('WebhookIngestionFailed')<{}> {}

export type WebhookIngestionError = WebhookUnauthorized | WebhookInvalidPayload |
  WebhookBodyTooLarge | WebhookIngestionFailed

// Shape shared by every provider's ingestion service.
export interface WebhookIngestion {
  readonly ingest: (request: Request) => Effect.Effect<EventEnvelope, WebhookIngestionError>
}

export const readBody = (request: Request, maxBytes: number) => Effect.suspend(() => {
  const body = request.body
  if (!body) return Effect.succeed(new Uint8Array())
  if (body.locked) return Effect.fail(new WebhookInvalidPayload())
  const buffer = new Uint8Array(maxBytes)
  return Stream.fromReadableStream({
    evaluate: () => body,
    onError: () => new WebhookInvalidPayload(),
    releaseLockOnEnd: true,
  }).pipe(
    Stream.runFoldEffect(0, (length, chunk) => {
      const nextLength = length + chunk.byteLength
      if (nextLength > maxBytes) return Effect.fail(new WebhookBodyTooLarge())
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

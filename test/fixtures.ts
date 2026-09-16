import { Effect, Layer } from 'effect'
import type { EventEnvelope } from '../src/event-queue'
import { GranolaIngestion } from '../src/granola'

export const event: EventEnvelope = {
  version: 1, provider: 'granola', eventId: 'event-1', eventType: 'fixture',
  sourceRecordId: 'meeting-1', sourceTimestamp: '2026-09-16T12:00:00Z',
}
export const sendResponse = { metadata: { metrics: { backlogCount: 1, backlogBytes: 128 } } }
export const ingestFixture = Layer.succeed(GranolaIngestion, { ingest: () => Effect.succeed(event) })
export const request = () => new Request('https://example.com/webhooks/granola', { method: 'POST' })

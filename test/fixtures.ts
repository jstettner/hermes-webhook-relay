import { Effect, Layer } from 'effect'
import { EventQueue, type EventEnvelope } from '../src/event-queue'
import { createApp } from '../src/index'
import { WebhookProviders } from '../src/providers'
import { WebhookConfigurationError, WebhookProviderNotFound, type WebhookIngestion } from '../src/webhook'

export const event: EventEnvelope = {
  version: 1, provider: 'granola', eventId: 'event-1', eventType: 'fixture',
  sourceRecordId: 'meeting-1', sourceTimestamp: '2026-09-16T12:00:00Z',
}
export const sendResponse = { metadata: { metrics: { backlogCount: 1, backlogBytes: 128 } } }
export const request = () => new Request('https://example.com/webhooks/granola', { method: 'POST' })

// Resolves only the given providers; every other name is a 404, as when not enabled.
export const providersFixture = (
  ingestions: Record<string, Effect.Effect<WebhookIngestion, WebhookConfigurationError>>,
) => Layer.succeed(WebhookProviders, {
  resolve: (name) => Object.hasOwn(ingestions, name) ? ingestions[name] : Effect.fail(new WebhookProviderNotFound()),
})
export const ingestFixture = providersFixture({ granola: Effect.succeed({ ingest: () => Effect.succeed(event) }) })
// The app with fixture providers and the live queue adapter over the env binding.
export const fixtureApp = (providers: Layer.Layer<WebhookProviders>) =>
  createApp((env) => Layer.merge(providers, EventQueue.layer(env.EVENTS)))

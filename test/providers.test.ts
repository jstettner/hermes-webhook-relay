import { Effect, Either, Layer } from 'effect'
import { expect, it } from 'vitest'
import { WebhookProviders, WebhookProvidersFromEnv } from '../src/providers'
import type { WebhookIngestion } from '../src/webhook'

const secrets = { GRANOLA_SIGNING_SECRET: 'whsec_dGVzdC1zZWNyZXQ=', POCKET_SIGNING_SECRET: 'pocket-secret' }
const resolve = (env: object, name: string) => Effect.runPromise(Effect.either(
  Effect.flatMap(WebhookProviders, (registry) => registry.resolve(name)).pipe(
    Effect.provide(WebhookProvidersFromEnv(env)),
  ),
))
const tag = (result: Either.Either<WebhookIngestion, { _tag: string }>) =>
  Either.isLeft(result) ? result.left._tag : 'Resolved'

it.each([
  ['granola,pocket', 'granola', 'Resolved'],
  [' granola , pocket ', 'pocket', 'Resolved'],
  ['granola', 'pocket', 'WebhookProviderNotFound'],
  [undefined, 'granola', 'WebhookProviderNotFound'],
  ['', 'granola', 'WebhookProviderNotFound'],
  ['granola,pocket', 'other', 'WebhookProviderNotFound'],
  ['granola,pocket', 'toString', 'WebhookProviderNotFound'],
  ['granola,other', 'granola', 'WebhookConfigurationError'],
  ['["granola"]', 'granola', 'WebhookConfigurationError'],
])('WEBHOOK_PROVIDERS=%j resolves %s as %s', async (enabled, name, expected) => {
  expect(tag(await resolve({ ...secrets, WEBHOOK_PROVIDERS: enabled }, name))).toBe(expected)
})
it.each([undefined, ''])('treats a missing or empty secret (%j) as misconfiguration for that provider only', async (value) => {
  const env = { ...secrets, WEBHOOK_PROVIDERS: 'granola,pocket', POCKET_SIGNING_SECRET: value }
  expect(tag(await resolve(env, 'pocket'))).toBe('WebhookConfigurationError')
  expect(tag(await resolve(env, 'granola'))).toBe('Resolved')
})
it('ignores non-string bindings such as the queue', async () => {
  const env = { ...secrets, WEBHOOK_PROVIDERS: 'granola', EVENTS: { send: () => {} } }
  expect(tag(await resolve(env, 'granola'))).toBe('Resolved')
})
it('fails layer construction, not resolution, for a malformed list', async () => {
  const built = await Effect.runPromise(Effect.either(Layer.build(WebhookProvidersFromEnv({ WEBHOOK_PROVIDERS: 'nope' })).pipe(Effect.scoped)))
  expect(Either.isLeft(built) && built.left._tag).toBe('WebhookConfigurationError')
})

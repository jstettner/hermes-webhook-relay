import { ConfigProvider, Effect, Layer, Result } from 'effect'
import { expect, it } from 'vitest'
import { WebhookProviders } from '../src/providers'
import type { WebhookIngestion } from '../src/webhook'

const secrets = { GRANOLA_SIGNING_SECRET: 'whsec_dGVzdC1zZWNyZXQ=', POCKET_SIGNING_SECRET: 'pocket-secret' }
const fromEnv = (env: object) =>
  WebhookProviders.layer.pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(env))))
const resolve = (env: object, name: string) => Effect.runPromise(Effect.result(
  Effect.flatMap(WebhookProviders, (registry) => registry.resolve(name)).pipe(
    Effect.provide(fromEnv(env)),
  ),
))
const tag = (result: Result.Result<WebhookIngestion, { _tag: string }>) =>
  Result.isFailure(result) ? result.failure._tag : 'Resolved'

it.each([
  ['granola,pocket', 'granola', 'Resolved'],
  [' granola , pocket ', 'pocket', 'Resolved'],
  ['granola', 'pocket', 'WebhookProviderNotFound'],
  [undefined, 'granola', 'WebhookProviderNotFound'],
  ['', 'granola', 'WebhookProviderNotFound'],
  [['granola', 'pocket'], 'pocket', 'Resolved'],
  [['granola'], 'pocket', 'WebhookProviderNotFound'],
  ['granola,pocket', 'other', 'WebhookProviderNotFound'],
  ['granola,pocket', 'toString', 'WebhookProviderNotFound'],
  // A malformed list fails layer construction, so every name fails alike.
  ['granola,other', 'granola', 'ConfigError'],
  ['granola,pocket,', 'granola', 'ConfigError'],
  // Only a real JSON array (a JSON var) is a list; a JSON string is not parsed.
  ['["granola"]', 'granola', 'ConfigError'],
  [['granola', 'other'], 'granola', 'ConfigError'],
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
  const built = await Effect.runPromise(Effect.result(Layer.build(fromEnv({ WEBHOOK_PROVIDERS: 'nope' })).pipe(Effect.scoped)))
  expect(Result.isFailure(built) && built.failure._tag).toBe('ConfigError')
})

import { Effect, Either, Redacted } from 'effect'
import { expect, it } from 'vitest'
import { GranolaConfig, GranolaConfigLive } from '../src/granola'

it('provides a redacted signing secret', async () => {
  const config = await Effect.runPromise(GranolaConfig.pipe(
    Effect.provide(GranolaConfigLive('private-secret')),
  ))
  expect(Redacted.value(config.signingSecret)).toBe('private-secret')
  expect(String(config.signingSecret)).not.toContain('private-secret')
})

it.each([undefined, ''])('rejects missing configuration: %s', async (secret) => {
  const result = await Effect.runPromise(GranolaConfig.pipe(
    Effect.provide(GranolaConfigLive(secret)), Effect.either,
  ))
  expect(Either.isLeft(result) && result.left._tag).toBe('GranolaConfigurationError')
})

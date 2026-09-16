import { Effect, Either } from 'effect'
import { expect, it, vi } from 'vitest'
import { EnqueueFailed, EventQueue } from '../src/event-queue'
import { acceptWebhook, ingestGranola } from '../src/webhook'
import { event, ingestFixture, request } from './fixtures'

it('enqueues the ingested envelope', async () => {
  const enqueue = vi.fn(() => Effect.void)
  await Effect.runPromise(acceptWebhook(request(), ingestFixture).pipe(Effect.provideService(EventQueue, { enqueue })))
  expect(enqueue).toHaveBeenCalledExactlyOnceWith(event)
})
it('fails closed without calling the queue', async () => {
  const enqueue = vi.fn(() => Effect.void)
  const result = await Effect.runPromise(acceptWebhook(request(), ingestGranola).pipe(
    Effect.provideService(EventQueue, { enqueue }), Effect.either,
  ))
  expect(Either.isLeft(result) && result.left._tag).toBe('IngestionUnavailable')
  expect(enqueue).not.toHaveBeenCalled()
})
it('propagates enqueue failure', async () => {
  const result = await Effect.runPromise(acceptWebhook(request(), ingestFixture).pipe(
    Effect.provideService(EventQueue, { enqueue: () => Effect.fail(new EnqueueFailed()) }), Effect.either,
  ))
  expect(Either.isLeft(result) && result.left._tag).toBe('EnqueueFailed')
})

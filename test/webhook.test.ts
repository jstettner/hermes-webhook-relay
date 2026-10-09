import { Effect } from 'effect'
import { expect, it, vi } from 'vitest'
import { EnqueueFailed, EventQueue } from '../src/event-queue'
import { GranolaIngestionLive } from '../src/granola'
import { webhookHandler } from '../src/routes/webhook'
import { event, ingestFixture, providersFixture, request } from './fixtures'

const granolaHandler = webhookHandler('granola')

it('enqueues the ingested envelope and returns 202', async () => {
  const enqueue = vi.fn(() => Effect.void)
  const response = await Effect.runPromise(granolaHandler(request()).pipe(
    Effect.provide(ingestFixture),
    Effect.provideService(EventQueue, { enqueue }),
  ))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith(event)
})
it('fails closed without calling the queue', async () => {
  const enqueue = vi.fn(() => Effect.void)
  const response = await Effect.runPromise(granolaHandler(request()).pipe(
    Effect.provide(providersFixture({ granola: GranolaIngestionLive('whsec_dGVzdC1zZWNyZXQ=') })),
    Effect.provideService(EventQueue, { enqueue }),
  ))
  expect(response.status).toBe(401)
  expect(await response.json()).toEqual({ error: 'unauthorized' })
  expect(enqueue).not.toHaveBeenCalled()
})
it('maps enqueue failure to a sanitized 503', async () => {
  const response = await Effect.runPromise(granolaHandler(request()).pipe(
    Effect.provide(ingestFixture),
    Effect.provideService(EventQueue, { enqueue: () => Effect.fail(new EnqueueFailed()) }),
  ))
  expect(response.status).toBe(503)
  expect(await response.json()).toEqual({ error: 'enqueue_failed' })
})

import { Effect, Either } from 'effect'
import { describe, expect, it, vi } from 'vitest'
import { decodeEventEnvelope, EventQueue, EventQueueLive } from '../src/event-queue'
import { event, sendResponse } from './fixtures'

describe('envelope', () => {
  it('accepts UTC timestamps with optional milliseconds', async () => {
    expect(await Effect.runPromise(decodeEventEnvelope(event))).toEqual(event)
    await Effect.runPromise(decodeEventEnvelope({ ...event, sourceTimestamp: '2026-09-16T12:00:00.123Z' }))
  })
  it.each([
    { version: 2 }, { provider: 'other' }, { eventId: '' }, { eventType: 'x'.repeat(513) },
    { sourceRecordId: undefined }, { transcript: 'private' },
    { sourceTimestamp: '2026-02-30T12:00:00Z' }, { sourceTimestamp: 'not-a-date' },
    { sourceTimestamp: '2026-09-16T12:00:00+00:00' },
  ])('rejects invalid metadata %j', async (patch) => {
    const result = await Effect.runPromise(Effect.either(decodeEventEnvelope({ ...event, ...patch })))
    expect(Either.isLeft(result)).toBe(true)
  })
})

describe('live queue', () => {
  it('sends only explicitly selected metadata as JSON', async () => {
    const send = vi.fn(async () => sendResponse)
    const enriched = { ...event, transcript: 'must not leave worker' }
    await Effect.runPromise(Effect.flatMap(EventQueue, (queue) => queue.enqueue(enriched)).pipe(
      Effect.provide(EventQueueLive({ send })),
    ))
    expect(send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
  })
  it('sanitizes binding failures', async () => {
    const result = await Effect.runPromise(Effect.flatMap(EventQueue, (queue) => queue.enqueue(event)).pipe(
      Effect.provide(EventQueueLive({ send: async () => { throw new Error('secret') } })),
      Effect.either,
    ))
    expect(Either.isLeft(result) && result.left._tag).toBe('EnqueueFailed')
    expect(JSON.stringify(result)).not.toContain('secret')
  })
})

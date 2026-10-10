import { describe, expect, it, vi } from '@effect/vitest'
import { Effect, Result } from 'effect'
import { decodeEventEnvelope, EventQueue } from '../src/event-queue'
import { event, sendResponse } from './fixtures'

describe('envelope', () => {
  it.effect.each([
    event, { ...event, provider: 'pocket' },
    { ...event, sourceTimestamp: '2026-09-16T12:00:00.123Z' },
    { ...event, sourceTimestamp: '2024-02-29T12:00:00Z' },
  ])('accepts %j', (valid) => Effect.gen(function* () {
    expect(yield* decodeEventEnvelope(valid)).toEqual(valid)
  }))
  it.effect.each([
    { version: 2 }, { provider: 'other' }, { eventId: '' }, { eventType: 'x'.repeat(513) },
    { sourceRecordId: undefined }, { transcript: 'private' },
    { sourceTimestamp: '2026-02-30T12:00:00Z' }, { sourceTimestamp: 'not-a-date' },
    { sourceTimestamp: '2026-09-16T12:00:00+00:00' },
    { sourceTimestamp: '2026-02-29T12:00:00Z' },
    { sourceTimestamp: '2026-09-16T24:00:00Z' },
    { sourceTimestamp: '2026-09-16T12:00:00.12Z' },
    { sourceTimestamp: '2026-09-16T12:00:00.1234Z' },
  ])('rejects invalid metadata %j', (patch) => Effect.gen(function* () {
    const result = yield* Effect.result(decodeEventEnvelope({ ...event, ...patch }))
    expect(Result.isFailure(result)).toBe(true)
  }))
})

describe('live queue', () => {
  it.effect('sends only explicitly selected metadata as JSON', () => {
    const send = vi.fn(async () => sendResponse)
    return Effect.gen(function* () {
      yield* (yield* EventQueue).enqueue({ ...event, transcript: 'must not leave worker' } as typeof event)
      expect(send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
    }).pipe(Effect.provide(EventQueue.layer({ send })))
  })
  it.effect('sanitizes binding failures', () => Effect.gen(function* () {
    const result = yield* Effect.result((yield* EventQueue).enqueue(event))
    expect(Result.isFailure(result) && result.failure._tag).toBe('EnqueueFailed')
    expect(JSON.stringify(result)).not.toContain('secret')
  }).pipe(Effect.provide(EventQueue.layer({ send: async () => { throw new Error('secret') } }))))
})

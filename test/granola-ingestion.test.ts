import { Clock, Effect } from 'effect'
import { expect, it, vi } from 'vitest'
import { GranolaIngestion, GranolaIngestionLive } from '../src/granola'
import { EventQueue } from '../src/event-queue'
import { granolaHandler } from '../src/routes/granola'
import app from '../src/index'
import { sendResponse } from './fixtures'

const secret = 'whsec_dGVzdC1zZWNyZXQ='
const now = 1800000000
const clock = Object.assign(Clock.make(), { currentTimeMillis: Effect.succeed(now * 1000), unsafeCurrentTimeMillis: () => now * 1000 })
const payload = {
  event_id: 'event_123', event_type: 'note.generated',
  note_id: 'not_1d3tmYTlCICgjy', occurred_at: '2026-01-27T15:30:00Z',
}
const expected = {
  version: 1, provider: 'granola', eventId: payload.event_id,
  eventType: payload.event_type, sourceRecordId: payload.note_id, sourceTimestamp: payload.occurred_at,
}
async function signed(body: string | Uint8Array<ArrayBuffer> = JSON.stringify(payload), timestamp = String(now)) {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode('test-secret'),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const prefix = new TextEncoder().encode(`${payload.event_id}.${timestamp}.`)
  const bytes = typeof body === 'string' ? new TextEncoder().encode(body) : body
  const message = new Uint8Array(prefix.length + bytes.length)
  message.set(prefix)
  message.set(bytes, prefix.length)
  const signature = await crypto.subtle.sign('HMAC', key, message)
  return new Request('https://example.com/webhooks/granola', {
    method: 'POST', body,
    headers: {
      'webhook-id': payload.event_id, 'webhook-timestamp': timestamp,
      'webhook-signature': `v1,${btoa(String.fromCharCode(...new Uint8Array(signature)))}`,
    },
  })
}
async function handle(request: Request, configuredSecret = secret) {
  const enqueue = vi.fn(() => Effect.void)
  const response = await Effect.runPromise(granolaHandler(request).pipe(
    Effect.provide(GranolaIngestionLive(configuredSecret)),
    Effect.provideService(EventQueue, { enqueue }), Effect.withClock(clock),
  ))
  return { response, enqueue }
}
async function rejected(request: Request, status: number) {
  const { response, enqueue } = await handle(request)
  expect(response.status).toBe(status)
  expect(enqueue).not.toHaveBeenCalled()
  return response
}

it.each(['note.generated', 'note.edited', 'note.access_granted'])('normalizes signed %s and strips unknown fields', async (event_type) => {
  const body = JSON.stringify({ ...payload, event_type, data: { changed_fields: ['summary'] }, private: '秘密' }, null, 2)
  const { response, enqueue } = await handle(await signed(body))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith({ ...expected, eventType: event_type })
})
it.each([false, true])('runs live ingestion through the app (queue fails: %s)', async (fails) => {
  const send = vi.fn(async () => {
    if (fails) throw new Error('private queue error')
    return sendResponse
  })
  const request = await signed(undefined, String(Math.floor(Date.now() / 1000)))
  const response = await app.request(request, {}, {
    GRANOLA_SIGNING_SECRET: secret,
    EVENTS: { send, sendBatch: vi.fn(), metrics: vi.fn() },
  })
  expect(response.status).toBe(fails ? 503 : 202)
  expect(send).toHaveBeenCalledExactlyOnceWith(expected, { contentType: 'json' })
  if (fails) expect(await response.json()).toEqual({ error: 'enqueue_failed' })
})
it('preserves event IDs on duplicate deliveries', async () => {
  for (let i = 0; i < 2; i++) {
    const { enqueue } = await handle(await signed())
    expect(enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  }
})
it.each([-301, -300, 300, 301])('enforces freshness at %s seconds', async (offset) => {
  const { response, enqueue } = await handle(await signed(undefined, String(now + offset)))
  expect(response.status).toBe(Math.abs(offset) > 300 ? 401 : 202)
  expect(enqueue).toHaveBeenCalledTimes(Math.abs(offset) > 300 ? 0 : 1)
})
it.each(['webhook-id', 'webhook-timestamp', 'webhook-signature'])('rejects missing %s', async (header) => {
  const request = await signed()
  request.headers.delete(header)
  await rejected(request, 401)
})
it.each([
  ['webhook-id', 'event.123'], ['webhook-id', 'x'.repeat(513)],
  ['webhook-timestamp', '1800000000junk'], ['webhook-timestamp', '1.8e9'],
  ['webhook-signature', 'v1,%%%'], ['webhook-signature', 'v1,YQ=='],
  ['webhook-signature', 'v2,unknown'], ['webhook-signature', 'x'.repeat(4097)],
  ['webhook-signature', Array(17).fill('v2,unknown').join(' ')],
])('rejects malformed %s: %s', async (header, value) => {
  const request = await signed()
  request.headers.set(header, value)
  await rejected(request, 401)
})
it('accepts any matching v1 signature alongside other versions and nonmatching signatures', async () => {
  const request = await signed()
  request.headers.set('webhook-signature', `v2,unknown v1,%%% v1,${btoa('\0'.repeat(32))} ${request.headers.get('webhook-signature')}`)
  expect((await handle(request)).response.status).toBe(202)
})
it('rejects a wrong key and tampered raw bytes', async () => {
  const wrongKey = await handle(await signed(), 'whsec_d3Jvbmcta2V5')
  expect(wrongKey.response.status).toBe(401)
  expect(wrongKey.enqueue).not.toHaveBeenCalled()
  const request = await signed()
  await rejected(new Request(request, { body: JSON.stringify(payload) + ' ' }), 401)
})
it.each([
  'not json', 'null', '[]',
  JSON.stringify({ ...payload, event_id: 'different' }),
  JSON.stringify({ ...payload, event_type: 'unknown' }),
  JSON.stringify({ ...payload, event_type: 'note.edited' }),
  JSON.stringify({ ...payload, event_type: 'note.edited', data: { changed_fields: ['other'] } }),
  JSON.stringify({ ...payload, note_id: 'not_bad' }),
  JSON.stringify({ ...payload, occurred_at: '2026-02-30T15:30:00Z' }),
])('rejects authenticated invalid payload: %s', async (body) => {
  const response = await rejected(await signed(body), 400)
  expect(await response.json()).toEqual({ error: 'invalid_payload' })
})
it('rejects authenticated invalid UTF-8 rather than replacing invalid bytes', async () => {
  const bytes = new Uint8Array(new TextEncoder().encode(JSON.stringify({ ...payload, extra: 'x' })))
  bytes[bytes.length - 3] = 0xff
  const response = await rejected(await signed(bytes), 400)
  expect(await response.json()).toEqual({ error: 'invalid_payload' })
})
it('reads exact raw bytes across chunk boundaries and releases the reader', async () => {
  const text = JSON.stringify({ ...payload, extra: '秘密' }, null, 2)
  const request = await signed(text)
  const bytes = new TextEncoder().encode(text)
  const stream = new ReadableStream({
    start(controller) {
      for (const byte of bytes) controller.enqueue(Uint8Array.of(byte))
      controller.close()
    },
  })
  const { response, enqueue } = await handle(new Request(request, { body: stream, duplex: 'half' } as RequestInit))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  expect(stream.locked).toBe(false)
})
it('verifies before parsing JSON', async () => {
  const request = await signed()
  await rejected(new Request(request, { body: 'not json' }), 401)
})
it.each([false, true])('limits streamed bytes and cancels overflow (cancel rejects: %s)', async (cancelRejects) => {
  const request = await signed()
  request.headers.set('content-length', '1')
  const cancel = vi.fn(async () => {
    if (cancelRejects) throw new Error('private cancellation failure')
  })
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(32768))
      controller.enqueue(new Uint8Array(32769))
    }, cancel,
  })
  const streamed = new Request(request, { body: stream, duplex: 'half' } as RequestInit)
  await rejected(streamed, 413)
  expect(cancel).toHaveBeenCalledOnce()
  expect(stream.locked).toBe(false)
})
it('cancels a stalled body on ingestion timeout', async () => {
  const cancel = vi.fn()
  const request = await signed()
  const stream = new ReadableStream({ cancel })
  const enqueue = vi.fn(() => Effect.void)
  const response = await Effect.runPromise(granolaHandler(new Request(request, {
    body: stream, duplex: 'half',
  } as RequestInit)).pipe(
    Effect.provide(GranolaIngestionLive(secret)),
    Effect.provideService(EventQueue, { enqueue }),
    Effect.withClock(Object.assign(Clock.make(), {
      currentTimeMillis: Effect.succeed(now * 1000),
      sleep: () => Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 20))),
    })),
  ))
  expect(response.status).toBe(500)
  expect(cancel).toHaveBeenCalledOnce()
  expect(stream.locked).toBe(false)
  expect(enqueue).not.toHaveBeenCalled()
})
it('maps a failed body stream to a sanitized 400', async () => {
  const request = await signed()
  const stream = new ReadableStream({ start(controller) { controller.error(new Error('private')) } })
  await rejected(new Request(request, { body: stream, duplex: 'half' } as RequestInit), 400)
})
it('accepts the exact body-size limit', async () => {
  const body = JSON.stringify(payload)
  expect((await handle(await signed(body.padEnd(65536, ' ')))).response.status).toBe(202)
})
it.each([undefined, '', 'test-secret', 'whsec_', 'whsec_%%%', 'whsec_YR==', 'whsec_YQ'])('rejects invalid live configuration without exposing it: %s', async (value) => {
  const result = await Effect.runPromise(GranolaIngestion.pipe(
    Effect.provide(GranolaIngestionLive(value)), Effect.either,
  ))
  expect(result._tag).toBe('Left')
  if (result._tag === 'Left') {
    expect(result.left._tag).toBe('GranolaConfigurationError')
    expect(JSON.stringify(result.left)).not.toContain('whsec_')
  }
})

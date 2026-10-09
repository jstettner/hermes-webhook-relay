import { Clock, Effect, Redacted } from 'effect'
import { expect, it, vi } from 'vitest'
import { PocketIngestionLive } from '../src/pocket'
import { EventQueue } from '../src/event-queue'
import { webhookHandler } from '../src/routes/webhook'
import app from '../src/index'
import { providersFixture, sendResponse } from './fixtures'

const pocketHandler = webhookHandler('pocket')

const secret = 'pocket-test-secret'
const now = 1800000000000
const clock = Object.assign(Clock.make(), { currentTimeMillis: Effect.succeed(now), unsafeCurrentTimeMillis: () => now })
const payload = {
  event: 'summary.completed',
  timestamp: '2026-02-18T12:00:00.000Z',
  user: { id: 'user_abc123', email: 'alice@example.com' },
  recording: { id: 'rec_abc123', title: 'Team Standup', recordingAt: '2026-02-17T15:02:00.000Z' },
  summarizations: { sum_456: { v2: { summary: { markdown: '## Private notes' } } } },
  transcript: [{ speaker: 'SPEAKER_00', speakerName: 'Alice', text: 'Private words.', start: 0, end: 3.5 }],
  speakers: { SPEAKER_00: { name: 'Alice', speakerId: 'spk_abc123' } },
}
const eventId = async (event: string, recordingId: string, timestamp: string) =>
  [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(`${event}\n${recordingId}\n${timestamp}`)))]
    .map((byte) => byte.toString(16).padStart(2, '0')).join('')
const expected = {
  version: 1, provider: 'pocket', eventId: await eventId(payload.event, payload.recording.id, payload.timestamp),
  eventType: payload.event, sourceRecordId: payload.recording.id, sourceTimestamp: payload.timestamp,
}
async function sign(body: string | Uint8Array<ArrayBuffer>, timestamp: string, key = secret) {
  const hmac = await crypto.subtle.importKey('raw', new TextEncoder().encode(key),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const prefix = new TextEncoder().encode(`${timestamp}.`)
  const bytes = typeof body === 'string' ? new TextEncoder().encode(body) : body
  const message = new Uint8Array(prefix.length + bytes.length)
  message.set(prefix)
  message.set(bytes, prefix.length)
  return [...new Uint8Array(await crypto.subtle.sign('HMAC', hmac, message))]
    .map((byte) => byte.toString(16).padStart(2, '0')).join('')
}
async function signed(body: string | Uint8Array<ArrayBuffer> = JSON.stringify(payload), timestamp = String(now), key = secret) {
  return new Request('https://example.com/webhooks/pocket', {
    method: 'POST', body,
    headers: {
      'content-type': 'application/json',
      'x-heypocket-timestamp': timestamp,
      'x-heypocket-signature': await sign(body, timestamp, key),
    },
  })
}
async function handle(request: Request, configuredSecret = secret) {
  const enqueue = vi.fn(() => Effect.void)
  const response = await Effect.runPromise(pocketHandler(request).pipe(
    Effect.provide(providersFixture({ pocket: PocketIngestionLive(Redacted.make(configuredSecret)) })),
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

it('normalizes a signed delivery and forwards no meeting content', async () => {
  const { response, enqueue } = await handle(await signed())
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  const forwarded = JSON.stringify(enqueue.mock.calls)
  for (const secretText of ['Private', 'alice@example.com', 'Team Standup']) expect(forwarded).not.toContain(secretText)
})
it.each([
  'transcription.completed', 'summary.completed', 'summary.regenerated', 'summary.updated',
  'mind_map.completed', 'action_items.regenerated', 'speakers.labeled', 'transcript.edited',
  'action_items.updated', 'recording.created', 'recording.deleted', 'recording.merged', 'translation.completed',
])('accepts documented event %s', async (event) => {
  const { response, enqueue } = await handle(await signed(JSON.stringify({ ...payload, event })))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith({
    ...expected, eventType: event, eventId: await eventId(event, payload.recording.id, payload.timestamp),
  })
})
it('runs live ingestion through the app', async () => {
  const send = vi.fn(async () => sendResponse)
  const response = await app.request(await signed(undefined, String(Date.now())), {}, {
    WEBHOOK_PROVIDERS: 'pocket', POCKET_SIGNING_SECRET: secret,
    EVENTS: { send, sendBatch: vi.fn(), metrics: vi.fn() },
  })
  expect(response.status).toBe(202)
  expect(send).toHaveBeenCalledExactlyOnceWith(expected, { contentType: 'json' })
})
it('reads a secret containing commas from the bindings unsplit', async () => {
  const send = vi.fn(async () => sendResponse)
  const response = await app.request(await signed(undefined, String(Date.now()), 'a,b,c'), {}, {
    WEBHOOK_PROVIDERS: 'pocket', POCKET_SIGNING_SECRET: 'a,b,c',
    EVENTS: { send, sendBatch: vi.fn(), metrics: vi.fn() },
  })
  expect(response.status).toBe(202)
})
it('keeps the event ID when a retry is re-signed with a new header timestamp', async () => {
  const first = await handle(await signed(undefined, String(now)))
  const retry = await handle(await signed(undefined, String(now + 5000)))
  expect(first.enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  expect(retry.enqueue).toHaveBeenCalledExactlyOnceWith(expected)
})
it('derives different event IDs for different events on the same recording', async () => {
  const other = await handle(await signed(JSON.stringify({ ...payload, event: 'transcript.edited' })))
  expect(other.enqueue.mock.calls[0]).not.toContainEqual(expect.objectContaining({ eventId: expected.eventId }))
})
it.each([-300001, -300000, 300000, 300001])('enforces freshness at %s ms', async (offset) => {
  const { response, enqueue } = await handle(await signed(undefined, String(now + offset)))
  expect(response.status).toBe(Math.abs(offset) > 300000 ? 401 : 202)
  expect(enqueue).toHaveBeenCalledTimes(Math.abs(offset) > 300000 ? 0 : 1)
})
it('rejects a seconds timestamp as stale', async () => {
  await rejected(await signed(undefined, String(now / 1000)), 401)
})
it.each(['x-heypocket-timestamp', 'x-heypocket-signature'])('rejects missing %s (legacy unsigned webhooks)', async (header) => {
  const request = await signed()
  request.headers.delete(header)
  await rejected(request, 401)
})
it('rejects an unsigned delivery with a valid payload', async () => {
  await rejected(new Request('https://example.com/webhooks/pocket', { method: 'POST', body: JSON.stringify(payload) }), 401)
})
it.each([
  ['x-heypocket-timestamp', '1800000000000junk'], ['x-heypocket-timestamp', '1.8e12'],
  ['x-heypocket-signature', 'zz'.repeat(32)], ['x-heypocket-signature', 'ab'.repeat(31)],
  ['x-heypocket-signature', 'ab'.repeat(33)], ['x-heypocket-signature', `v1,${'ab'.repeat(32)}`],
  ['x-heypocket-signature', 'x'.repeat(4097)],
])('rejects malformed %s: %s', async (header, value) => {
  const request = await signed()
  request.headers.set(header, value)
  await rejected(request, 401)
})
it('accepts an optional sha256= prefix and uppercase hex', async () => {
  const request = await signed()
  request.headers.set('x-heypocket-signature', `sha256=${request.headers.get('x-heypocket-signature')!.toUpperCase()}`)
  expect((await handle(request)).response.status).toBe(202)
})
it('rejects a wrong key, tampered bytes, and a signature over the body alone', async () => {
  await rejected(await signed(undefined, String(now), 'wrong-secret'), 401)
  const request = await signed()
  await rejected(new Request(request, { body: JSON.stringify(payload) + ' ' }), 401)
  const bodyOnly = await signed()
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  bodyOnly.headers.set('x-heypocket-signature', [...new Uint8Array(await crypto.subtle.sign('HMAC', key,
    new TextEncoder().encode(JSON.stringify(payload))))].map((byte) => byte.toString(16).padStart(2, '0')).join(''))
  await rejected(bodyOnly, 401)
})
it('verifies before parsing JSON', async () => {
  const request = await signed()
  await rejected(new Request(request, { body: 'not json' }), 401)
})
it.each([
  'not json', 'null', '[]', '{}',
  JSON.stringify({ ...payload, event: 'unknown.event' }),
  JSON.stringify({ ...payload, recording: undefined }),
  JSON.stringify({ ...payload, recording: { id: 'rec.bad' } }),
  JSON.stringify({ ...payload, timestamp: '2026-02-30T12:00:00.000Z' }),
  JSON.stringify({ ...payload, timestamp: '2026-02-18T12:00:00+00:00' }),
])('rejects authenticated invalid payload: %s', async (body) => {
  const response = await rejected(await signed(body), 400)
  expect(await response.json()).toEqual({ error: 'invalid_payload' })
})
it('accepts a large transcript up to the 2 MiB limit and rejects one byte more', async () => {
  const body = JSON.stringify(payload)
  expect((await handle(await signed(body.padEnd(2 * 1024 * 1024, ' ')))).response.status).toBe(202)
  await rejected(await signed(body.padEnd(2 * 1024 * 1024 + 1, ' ')), 413)
})
it('rejects an oversized secret', async () => {
  const result = await Effect.runPromise(Effect.either(PocketIngestionLive(Redacted.make('x'.repeat(1025)))))
  expect(result._tag).toBe('Left')
  if (result._tag === 'Left') expect(result.left._tag).toBe('WebhookConfigurationError')
})

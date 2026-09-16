import { Effect, Layer } from 'effect'
import { GranolaIngestion } from '../src/granola'
import type { Bindings } from '../src/index'
import { expect, it, vi } from 'vitest'
import app, { createApp } from '../src/index'
import { event, ingestFixture, sendResponse } from './fixtures'

const environment = (send = vi.fn(async () => sendResponse)) => ({
  GRANOLA_SIGNING_SECRET: 'whsec_dGVzdC1zZWNyZXQ=',
  EVENTS: { send, sendBatch: vi.fn(async () => sendResponse), metrics: vi.fn(async () => sendResponse.metadata.metrics) },
}) satisfies Bindings

it.each(['{}', 'not json', '{"transcript":"private"}'])('production fails closed: %s', async (body) => {
  const env = environment()
  const response = await app.request('/webhooks/granola', { method: 'POST', body, headers: { 'signature': 'fake' } }, env)
  expect(response.status).toBe(401)
  expect(await response.json()).toEqual({ error: 'unauthorized' })
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it('returns sanitized 500 without a configured secret', async () => {
  const env = { ...environment(), GRANOLA_SIGNING_SECRET: undefined }
  const response = await app.request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(500)
  expect(await response.json()).toEqual({ error: 'internal_error' })
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it('awaits successful storage before 202', async () => {
  let complete!: () => void
  let started!: () => void
  const entered = new Promise<void>((resolve) => { started = resolve })
  const stored = new Promise<void>((resolve) => { complete = resolve })
  const send = vi.fn(async () => { started(); await stored; return sendResponse })
  let settled = false
  const response = createApp(ingestFixture).request('/webhooks/granola', { method: 'POST' }, environment(send))
  const pending = Promise.resolve(response).then((value) => { settled = true; return value })
  await entered
  expect(settled).toBe(false)
  complete()
  expect((await pending).status).toBe(202)
  expect(send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
})
it('fixture ingestion does not require live configuration', async () => {
  const env = { ...environment(), GRANOLA_SIGNING_SECRET: undefined }
  const response = await createApp(ingestFixture).request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(202)
  expect(env.EVENTS.send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
})
it('returns sanitized 503 on storage failure', async () => {
  const env = environment(vi.fn(async () => { throw new Error('secret') }))
  const response = await createApp(ingestFixture).request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(503)
  expect(await response.json()).toEqual({ error: 'enqueue_failed' })
})
it.each([
  () => Effect.die(new Error('secret')),
  () => { throw new Error('secret') },
])('sanitizes unexpected defects', async (ingest) => {
  const env = environment()
  const response = await createApp(Layer.succeed(GranolaIngestion, { ingest })).request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(500)
  expect(await response.json()).toEqual({ error: 'internal_error' })
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it('preserves root and does not enqueue for other routes or methods', async () => {
  const env = environment()
  expect(await (await app.request('/', {}, env)).text()).toBe('Hello Hono!')
  expect((await app.request('/webhooks/granola', {}, env)).status).toBe(404)
  expect((await app.request('/other', { method: 'POST' }, env)).status).toBe(404)
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})

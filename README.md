# hermes-webhook-relay

A Hono Cloudflare Worker with an Effect-based event queue foundation.

**Ingestion is not enabled.** `POST /webhooks/granola` always returns
`503 {"error":"ingestion_unavailable"}` without reading the body or enqueueing.
Do not register a real webhook yet. `GET /` retains the template greeting.

## Local verification

```sh
npm ci
npm run cf-typegen
npm run typecheck
npm test
npx wrangler deploy --dry-run
npm run dev
```

In another terminal:

```sh
curl -i http://localhost:8787/
curl -i -X POST http://localhost:8787/webhooks/granola \
  -H 'Content-Type: application/json' --data '{"synthetic":true}'
```

Expect 200 for the root and 503 for the webhook. Wrangler uses a local queue;
these checks do not require remote provisioning. Regenerate
`worker-configuration.d.ts` whenever Wrangler configuration changes.

## Structure

- `src/event-queue.ts`: bounded metadata schema/type, strict decoder, `EventQueue`
  service, typed `EnqueueFailed`, and live Cloudflare producer layer.
- `src/webhook.ts`: ingestion contract, fail-closed adapter, and sequential
  ingestion → enqueue Effect program.
- `src/routes/granola.ts`: per-request layer wiring, one Effect execution boundary,
  sanitized HTTP outcomes.
- `src/index.ts`: typed Hono app factory and production default export.

The envelope contains only version, provider, event ID/type, source record ID,
and UTC source timestamp. Identifiers are 1–512 characters; timestamps use
`YYYY-MM-DDTHH:mm:ssZ` or millisecond precision. No meeting content is forwarded.
The ingestion contract requires authenticated, validated metadata; the queue
adapter explicitly selects fields rather than forwarding arbitrary objects.

Tests inject a fixture ingestion function via the app factory and recording or
failing queue implementations. This seam has no environment flag or HTTP bypass.
The production export always uses the unavailable adapter.

Once ingestion is implemented, HTTP 202 means the queue send completed;
queue failure returns 503 `enqueue_failed`, and unexpected failures return 500
`internal_error`. No detached enqueue, automatic retries, raw payload logging,
or raw exception responses are used. Duplicate delivery remains possible.

## Before enabling ingestion

- Establish Granola's actual signing and payload contract; implement bounded
  raw-body reading, Web Crypto verification, timestamp checks with Effect Clock,
  schema validation, and metadata normalization.
- Provision `hermes-webhook-events` for the `EVENTS` producer binding. Configure
  HTTP pull consumption and verify free-plan 24-hour retention separately.
- Store the signing secret in Cloudflare's secret store, not source control.
- Implement the private Hermes consumer with durable deduplication and safe
  acknowledgment, outside this repository.
- Verify real events and deployed CPU limits. Dry-run bundle size alone does not
  establish free-plan CPU compliance.

No remote resources were created or deployed by this foundation setup.
`npm run deploy` is available only for a later, explicitly authorized deployment.

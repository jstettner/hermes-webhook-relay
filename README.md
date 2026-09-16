# hermes-webhook-relay

A Hono Cloudflare Worker with Effect-based Granola webhook ingestion and queueing.

`POST /webhooks/granola` verifies Standard Webhooks HMAC-SHA256 signatures over
bounded raw bytes before validating and enqueueing metadata. Configure
`GRANOLA_SIGNING_SECRET` with Granola's `whsec_`-prefixed base64 secret (locally in
`.dev.vars`; never commit secrets). Missing or malformed configuration returns
sanitized 500. The key is imported when constructing the live ingestion layer.
`GET /` retains the template greeting. Real Granola delivery interoperability
still needs verification before ongoing ingestion is enabled.

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

Expect 200 for the root and 401 for the unsigned webhook with valid configuration
(500 without configuration). Wrangler uses a local queue;
these checks do not require remote provisioning. Regenerate
`worker-configuration.d.ts` whenever Wrangler configuration changes.

## Structure

- `src/event-queue.ts`: bounded metadata schema/type, strict decoder, `EventQueue`
  service, typed `EnqueueFailed`, and live Cloudflare producer layer.
- `src/granola.ts`: `GranolaConfig` (redacted secret), `GranolaIngestion`, and
  their live layers, bounded body reading, signature verification, and payload schemas.
- `src/routes/granola.ts`: ingestion → enqueue orchestration, route Effect, and
  sanitized HTTP outcomes; no layer wiring.
- `src/index.ts`: typed Hono app factory, per-request layer composition, and the
  Effect execution boundary.

The envelope contains only version, provider, event ID/type, source record ID,
and UTC source timestamp. Identifiers are 1–512 characters; timestamps use
`YYYY-MM-DDTHH:mm:ssZ` or millisecond precision. No meeting content is forwarded.
The ingestion contract requires authenticated, validated metadata; the queue
adapter explicitly selects fields rather than forwarding arbitrary objects.

Tests provide a fixture `GranolaIngestion` layer via the app factory and recording
or failing queue implementations. The app boundary provides live ingestion and
queue layers. Live ingestion encapsulates configuration wiring from the secret
binding; fixture layers do not require live configuration. No ingestion dependency
is passed through the handler or orchestration. This seam has no environment flag or HTTP bypass.
The production export always uses the verifying adapter.

HTTP 202 means the queue send completed;
queue failure returns 503 `enqueue_failed`, and unexpected failures return 500
`internal_error`. No detached enqueue, automatic retries, raw payload logging,
or raw exception responses are used. Duplicate delivery remains possible.
Authentication/freshness failures return 401 `unauthorized`; invalid authenticated
payloads return 400 `invalid_payload`; oversized bodies return 413 `body_too_large`.

Local policy: 64 KiB streamed body limit, 512-character alphanumeric/underscore/hyphen
event IDs, 12-digit delivery timestamps, 4096-character signature headers, at most
16 space-separated signatures, and an inclusive ±300-second delivery window using
Effect Clock. Ingestion times out after 10 seconds; the request Effect is bounded
to 12 seconds including enqueue. Source timestamps use the envelope's strict UTC
format and are not subject to the freshness window. All three documented event
types are accepted; edited events require `data.changed_fields: ["summary"]`.
Unknown payload fields are stripped; unknown event types are rejected.
See `spec/granola.md` for the provider contract and deployment checklist.

## Before enabling ingestion

- Verify Granola UI test deliveries and real meeting events against the documented
  contract and local validation policy; synthetic tests do not prove interoperability.
- Provision `hermes-webhook-events` for the `EVENTS` producer binding. Configure
  HTTP pull consumption and verify free-plan 24-hour retention separately.
- Store the signing secret in Cloudflare's secret store, not source control.
- Implement the private Hermes consumer with durable deduplication and safe
  acknowledgment, outside this repository.
- Verify real events and deployed CPU limits. Dry-run bundle size alone does not
  establish free-plan CPU compliance.

No remote resources were created or deployed by this foundation setup.
`npm run deploy` is available only for a later, explicitly authorized deployment.

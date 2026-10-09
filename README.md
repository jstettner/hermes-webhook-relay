# hermes-webhook-relay

A Hono Cloudflare Worker with Effect-based webhook ingestion and queueing for
Granola and Pocket.

## Configuration

Providers are opt-in. List the ones this deployment accepts in `wrangler.jsonc`,
and store each enabled provider's signing secret as a Cloudflare secret
(`wrangler secret put …`; locally in `.dev.vars`; never commit secrets):

```jsonc
"vars": { "WEBHOOK_PROVIDERS": ["granola", "pocket"] }
```

| Provider | Route | Secret binding |
| --- | --- | --- |
| `granola` | `POST /webhooks/granola` | `GRANOLA_SIGNING_SECRET` |
| `pocket` | `POST /webhooks/pocket` | `POCKET_SIGNING_SECRET` |

The checked-in list is empty, so nothing is accepted until you opt in. A provider
that is unregistered or not listed returns 404 `not_found`. A listed provider whose
secret is missing or malformed, or a malformed `WEBHOOK_PROVIDERS` value, returns
sanitized 500. Each request resolves only its own provider, so one provider's
misconfiguration does not affect another's route.

To add a provider, implement a `WebhookIngestion` builder (see `src/granola.ts`)
and register it with its secret binding in `src/providers.ts`.

## Providers

`POST /webhooks/granola` verifies Standard Webhooks HMAC-SHA256 signatures over
bounded raw bytes before validating and enqueueing metadata.
`GRANOLA_SIGNING_SECRET` is Granola's `whsec_`-prefixed base64 secret. The key is
imported when the provider is resolved. `GET /` retains the template greeting. Real Granola delivery interoperability
still needs verification before ongoing ingestion is enabled.

`POST /webhooks/pocket` verifies `X-HeyPocket-Signature` (hex HMAC-SHA256 over
`{X-HeyPocket-Timestamp}.{rawBody}`, keyed with the secret string as-is) and
rejects timestamps more than 300 s (header is in milliseconds) from now. Configure
`POCKET_SIGNING_SECRET` with the secret Pocket shows when the webhook is created
or rotated. Unsigned deliveries, including those from legacy webhooks without a
secret, are rejected. Pocket bodies include the full transcript, so the read limit
is 2 MiB (Granola: 64 KiB); only `event`, `timestamp` and `recording.id` are
decoded. Pocket sends no event ID, so `eventId` is the SHA-256 hex of
`event`, `recording.id` and the payload `timestamp`. Real Pocket delivery
interoperability (signature format, retry behaviour) still needs verification.

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

Expect 200 for the root and, for the unsigned webhook, 401 when `granola` is
enabled with a secret, 404 when it is not enabled, and 500 when it is enabled
without a secret. Wrangler uses a local queue;
these checks do not require remote provisioning. Regenerate
`worker-configuration.d.ts` with `npm run cf-typegen` whenever Wrangler
configuration changes; it ignores local `.env` files so their variable names are
not written into the committed types.

## Structure

- `src/event-queue.ts`: bounded metadata schema/type, strict decoder, `EventQueue`
  service, typed `EnqueueFailed`, and live Cloudflare producer layer.
- `src/webhook.ts`: provider-neutral ingestion and resolution errors, the
  `WebhookIngestion` shape, and bounded body reading.
- `src/providers.ts`: the provider registry, the `WebhookProviders` service that
  resolves a provider name to its ingestion, and its live layer, which reads
  `WEBHOOK_PROVIDERS` and the secret bindings.
- `src/granola.ts`, `src/pocket.ts`: per-provider config (redacted secret),
  ingestion builders, signature verification, and payload schemas.
- `src/routes/webhook.ts`: resolve → ingest → enqueue orchestration and sanitized
  HTTP outcomes shared by every provider; no layer wiring.
- `src/index.ts`: typed Hono app factory, the `AppLive` layer built from bindings,
  and the Effect execution boundary.

The envelope contains only version, provider, event ID/type, source record ID,
and UTC source timestamp. Identifiers are 1–512 characters; timestamps use
`YYYY-MM-DDTHH:mm:ssZ` or millisecond precision. No meeting content is forwarded.
The ingestion contract requires authenticated, validated metadata; the queue
adapter explicitly selects fields rather than forwarding arbitrary objects.

The handler depends only on the `WebhookProviders` and `EventQueue` services.
`createApp` takes one function from bindings to the layer that provides them; the
production export passes `AppLive`, which always uses the verifying providers.
Tests pass fixture `WebhookProviders` layers through the same boundary, with
recording or failing queue bindings; fixtures do not require live configuration.
This seam has no environment flag or HTTP bypass.

HTTP 202 means the queue send completed;
queue failure returns 503 `enqueue_failed`, and unexpected failures return 500
`internal_error`. No detached enqueue, automatic retries, raw payload logging,
or raw exception responses are used. Duplicate delivery remains possible.
Authentication/freshness failures return 401 `unauthorized`; invalid authenticated
payloads return 400 `invalid_payload`; oversized bodies return 413 `body_too_large`.

Granola policy: 64 KiB streamed body limit, 512-character alphanumeric/underscore/hyphen
event IDs, 12-digit delivery timestamps, 4096-character signature headers, at most
16 space-separated signatures, and an inclusive ±300-second delivery window using
Effect Clock. For every provider, ingestion times out after 10 seconds and the
request Effect is bounded to 12 seconds including enqueue. Source timestamps use the envelope's strict UTC
format and are not subject to the freshness window. All three documented event
types are accepted; edited events require `data.changed_fields: ["summary"]`.
Unknown payload fields are stripped; unknown event types are rejected.
See `spec/granola.md` for the provider contract and deployment checklist.

## Before enabling ingestion

- Verify Granola UI test deliveries and real meeting events against the documented
  contract and local validation policy; synthetic tests do not prove interoperability.
- Provision `hermes-webhook-events` for the `EVENTS` producer binding. Configure
  HTTP pull consumption and verify free-plan 24-hour retention separately.
- Set `WEBHOOK_PROVIDERS` and store each enabled provider's signing secret in
  Cloudflare's secret store, not source control.
- Implement the private Hermes consumer with durable deduplication and safe
  acknowledgment, outside this repository.
- Verify real events and deployed CPU limits. Dry-run bundle size alone does not
  establish free-plan CPU compliance.

No remote resources were created or deployed by this foundation setup.
`npm run deploy` is available only for a later, explicitly authorized deployment.

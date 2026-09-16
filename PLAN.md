# Effect EventQueue and Hono boundary

## Context

- `src/index.ts` is a basic Hono app with only `GET /`.
- Effect 3.22 and Hono 4 are installed. No custom services or validation exist yet.
- `wrangler.jsonc` has no queue bindings. `package.json` has Wrangler scripts and binding type generation, but no tests or typecheck command.
- Goal: introduce a narrow Effect queue capability and a Hono execution boundary without creating an unauthenticated event-ingestion endpoint.

## Approach

- Define a bounded, versioned metadata envelope with Effect Schema directly in `src/event-queue.ts`, alongside its inferred type; no separate envelope module.
- Define `EventQueue` using `Context.Tag`, exposing `enqueue(event): Effect<void, EnqueueFailed>`.
- Implement a live layer wrapping the Cloudflare producer binding using `Effect.tryPromise`; await persistence before returning HTTP 202.
- Keep domain functions as Effects; run Effect only at the Hono boundary. Translate expected errors into sanitized HTTP responses and unexpected failures into 500s.
- Wire bindings per request; no global managed runtime, background enqueue, or automatic retry.
- Use Effect's built-in Clock when signature timestamp verification is implemented; no custom clock service.
- Foundation only: register `POST /webhooks/granola`, but its production ingestion function always fails with `IngestionUnavailable`. Return sanitized HTTP 503 and never read/enqueue unverified events. No permissive verifier, secret, or generic ingestion endpoint.
- Use an app factory accepting a narrow ingestion function for tests; the default Worker export always uses the fail-closed stub. This is a code-level testing seam, not an environment flag or request-controlled bypass.
- The ingestion function returns an Effect yielding a validated envelope or a typed ingestion error. Compose it with `EventQueue.enqueue` in the webhook program. Tests inject validated synthetic envelopes; real verification and decoding replace the stub in a later slice.
- Add producer binding `EVENTS` targeting `hermes-webhook-events`. Provisioning, pull-consumer configuration, retention settings, and deployment remain separate work.

## Files to modify

Planned files:
- `src/index.ts`: typed Hono app factory, route registration, and default fail-closed Worker export.
- `src/event-queue.ts`: inline metadata schema and inferred `EventEnvelope` type, queue tag with inline service shape, typed enqueue error, and live layer.
- `src/webhook.ts`: ingestion contract, `IngestionUnavailable`, fail-closed stub, and ingestion-then-enqueue Effect program.
- `src/routes/granola.ts`: Hono handler, request-scoped queue layer, typed error mapping, and sanitized defect handling.
- `wrangler.jsonc`: `EVENTS` queue producer binding for `hermes-webhook-events`.
- `worker-configuration.d.ts`: generated Worker binding/runtime types from `cf-typegen`.
- `package.json`, `package-lock.json`, `tsconfig.json`: test/typecheck tooling and generated binding type integration.
- `test/event-queue.test.ts`, `test/webhook.test.ts`, `test/routes.test.ts`: Vitest tests for schema, service, orchestration, and HTTP outcomes.
- `README.md`: usage, failure behavior, and readiness limitations.

## Reuse

- Keep the existing Hono app/export and `GET /` in `src/index.ts`.
- Reuse the existing `cf-typegen`, `dev`, and `deploy` scripts in `package.json`.
- Follow the existing typed Hono binding pattern documented in `README.md`.
- Reuse Effect Schema, Context, Layer, and Clock rather than adding a dependency-injection framework.

## Steps

- [x] Confirm foundation-only scope and proposed queue binding/name.
- [x] Define the schema and inferred `EventEnvelope` type directly in `src/event-queue.ts` with `version: 1`, `provider: "granola"`, bounded nonempty `eventId`, `eventType`, and `sourceRecordId` (maximum 512 characters each), and a validated UTC ISO `sourceTimestamp`. No raw payload or content fields. Explicitly construct outgoing metadata; test strict schema rejection of excess fields. Provider-specific event types remain deferred until the contract is known.
- [x] Define `EventQueue` with `Context.Tag` and `EnqueueFailed` with `Data.TaggedError`. Build `EventQueueLive(binding)` using `Layer.succeed` and `Effect.tryPromise` around awaited `binding.send(event, { contentType: "json" })`. Keep raw binding errors out of public responses and error logs.
- [x] Implement the ingestion-then-enqueue program. Add the typed fail-closed stub; do not parse or authenticate provider payloads in this slice. Use recording/failing queue substitutes and a fixture ingestion function only in tests.
- [x] Add the app factory and Hono handler. Provide the live layer from each request's `env.EVENTS`. Map success to 202, `IngestionUnavailable` to 503, and `EnqueueFailed` to 503 with distinct sanitized error codes. Map unexpected defects to sanitized 500s. Run Effect once at the route boundary; never use `waitUntil` or detached enqueue.
- [x] Configure the producer binding and generate runtime/binding types. Add TypeScript and Vitest dev dependencies, `typecheck` (`tsc --noEmit`) and `test` (`vitest run`) scripts; ensure generated runtime types are included in TypeScript checking. Avoid a Workers test-pool dependency for this foundation slice; use Hono `app.request` with fake bindings.
- [x] Document local verification and remaining deployment prerequisites, including the fact that all production webhook requests currently return 503. No remote resource creation or deployment in this slice.

## Verification

- Queue adapter sends only the exact normalized envelope and maps binding rejection to `EnqueueFailed`.
- Handler waits for enqueue completion before 202; rejection returns 503; unexpected failures return sanitized 500.
- The production stub returns 503 and never invokes the queue, regardless of body or signature headers. This verifies fail-closed behavior, not actual signature validation.
- Schema tests cover required fields, bounded strings, timestamps, version/provider literals, and exclusion of content fields.
- Keep `GET /` working. Verify nonmatching paths/methods cannot enqueue.
- Test the Hono boundary through HTTP requests using injected dependencies; no test bypass reachable through production configuration.
- Run `npm run cf-typegen`, `npm run typecheck`, `npm test`, and `npx wrangler deploy --dry-run`; inspect bundle size. Dry-run bundling does not prove production CPU-limit compliance.
- Run `npm run dev` locally and POST a synthetic request to `/webhooks/granola`; expect sanitized 503 with no queue write. Verify `GET /` still responds. No remote credentials should be required for the local checks.

## Deferred

- Granola signing contract, bounded raw-body reading, signature/replay checks using Effect Clock, and provider payload validation/normalization.
- Remote queue creation, free-plan retention and HTTP pull configuration, deployment, webhook registration, and real-event verification.
- Hermes consumer integration, durable deduplication, and acknowledgment policy.

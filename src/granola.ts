import { Context, Data, Effect, Layer, Redacted } from 'effect'
import type { EventEnvelope } from './event-queue'

export class GranolaConfig extends Context.Tag('GranolaConfig')<GranolaConfig, {
  readonly signingSecret: Redacted.Redacted<string>
}>() {}

export class GranolaConfigurationError extends Data.TaggedError('GranolaConfigurationError')<{}> {}
export class IngestionUnavailable extends Data.TaggedError('IngestionUnavailable')<{}> {}

export const GranolaConfigLive = (secret: string | undefined) =>
  Layer.effect(GranolaConfig, Effect.suspend(() =>
    secret === undefined || secret.length === 0
      ? Effect.fail(new GranolaConfigurationError())
      : Effect.succeed({ signingSecret: Redacted.make(secret) }),
  ))

export class GranolaIngestion extends Context.Tag('GranolaIngestion')<GranolaIngestion, {
  readonly ingest: (request: Request) => Effect.Effect<EventEnvelope, IngestionUnavailable>
}>() {}

// Fail closed until signature verification and payload validation are implemented.
export const GranolaIngestionFromConfig = Layer.effect(GranolaIngestion,
  Effect.gen(function* () {
    yield* GranolaConfig
    return { ingest: (_request: Request) => Effect.fail(new IngestionUnavailable()) }
  }),
)

// Encapsulate the live configuration wiring; callers provide only the binding.
export const GranolaIngestionLive = (secret: string | undefined) =>
  GranolaIngestionFromConfig.pipe(Layer.provide(GranolaConfigLive(secret)))


import { Config, ConfigProvider, Context, Effect, Layer, Option, type Redacted } from 'effect'
import { GranolaIngestionLive } from './granola'
import { PocketIngestionLive } from './pocket'
import { WebhookConfigurationError, WebhookProviderNotFound, type WebhookIngestion } from './webhook'

// Every provider the relay supports. Adding one means adding an entry here;
// deployments opt in through WEBHOOK_PROVIDERS plus the secret binding.
export const providers = {
  granola: { secretBinding: 'GRANOLA_SIGNING_SECRET', ingestion: GranolaIngestionLive },
  pocket: { secretBinding: 'POCKET_SIGNING_SECRET', ingestion: PocketIngestionLive },
} as const satisfies Record<string, {
  readonly secretBinding: string
  readonly ingestion: (secret: Redacted.Redacted<string>) => Effect.Effect<WebhookIngestion, WebhookConfigurationError>
}>

export type ProviderName = keyof typeof providers
export type ProviderSecrets = { readonly [P in ProviderName as typeof providers[P]['secretBinding']]?: string }

export class WebhookProviders extends Context.Tag('WebhookProviders')<WebhookProviders, {
  readonly resolve: (name: string) =>
    Effect.Effect<WebhookIngestion, WebhookProviderNotFound | WebhookConfigurationError>
}>() {}

// Vars, secrets and .dev.vars all arrive as strings. fromMap splits sequences on
// commas, so WEBHOOK_PROVIDERS=granola,pocket reads as a list. Empty values count
// as unset; non-string bindings (queues) are not configuration.
export const envConfigProvider = (env: object) => ConfigProvider.fromMap(new Map(
  Object.entries(env).filter((entry): entry is [string, string] =>
    typeof entry[1] === 'string' && entry[1].length > 0),
))

const names = Object.keys(providers) as [ProviderName, ...ProviderName[]]
// Unset enables nothing; unknown names are a misconfiguration.
const EnabledProviders = Config.array(Config.literal(...names)(), 'WEBHOOK_PROVIDERS').pipe(
  Config.withDefault([] as ReadonlyArray<ProviderName>),
)

// Reads all configuration when built, failing with WebhookConfigurationError if the
// enabled list is malformed. Each enabled provider's secret is optional here, so a
// missing secret fails only that provider's route rather than the whole layer.
export const WebhookProvidersLive = Layer.effect(WebhookProviders, Effect.gen(function* () {
  const enabled = yield* EnabledProviders
  const configured = new Map(yield* Effect.forEach(enabled, (name) =>
    Config.option(Config.redacted(providers[name].secretBinding)).pipe(
      Effect.map((secret) => [name as string, { provider: providers[name], secret }] as const),
    )))
  return WebhookProviders.of({
    resolve: (name) => {
      const entry = configured.get(name)
      if (entry === undefined) return Effect.fail(new WebhookProviderNotFound())
      return Option.match(entry.secret, {
        onNone: () => Effect.fail(new WebhookConfigurationError()),
        onSome: entry.provider.ingestion,
      })
    },
  })
}).pipe(Effect.mapError(() => new WebhookConfigurationError())))

// The live registry, reading configuration from the Worker bindings.
export const WebhookProvidersFromEnv = (env: object) =>
  WebhookProvidersLive.pipe(Layer.provide(Layer.setConfigProvider(envConfigProvider(env))))

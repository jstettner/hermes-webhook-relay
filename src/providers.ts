import { Context, Effect, Either, Layer, Schema } from 'effect'
import { GranolaIngestionLive } from './granola'
import { PocketIngestionLive } from './pocket'
import { WebhookConfigurationError, WebhookProviderNotFound, type WebhookIngestion } from './webhook'

// Every provider the relay supports. Adding one means adding an entry here;
// deployments opt in through the WEBHOOK_PROVIDERS var plus the secret binding.
export const providers = {
  granola: { secretBinding: 'GRANOLA_SIGNING_SECRET', ingestion: GranolaIngestionLive },
  pocket: { secretBinding: 'POCKET_SIGNING_SECRET', ingestion: PocketIngestionLive },
} as const satisfies Record<string, {
  readonly secretBinding: string
  readonly ingestion: (secret: string | undefined) => Effect.Effect<WebhookIngestion, WebhookConfigurationError>
}>

export type ProviderName = keyof typeof providers
export type ProviderSecrets = { readonly [P in ProviderName as typeof providers[P]['secretBinding']]?: string }
export type ProvidersEnv = ProviderSecrets & { readonly WEBHOOK_PROVIDERS?: unknown }

export class WebhookProviders extends Context.Tag('WebhookProviders')<WebhookProviders, {
  readonly resolve: (name: string) =>
    Effect.Effect<WebhookIngestion, WebhookProviderNotFound | WebhookConfigurationError>
}>() {}

const isProviderName = (name: string): name is ProviderName => Object.hasOwn(providers, name)
const decodeEnabled = Schema.decodeUnknownEither(Schema.Array(Schema.Literal(...Object.keys(providers) as ProviderName[])))

export const WebhookProvidersLive = (env: ProvidersEnv) =>
  Layer.succeed(WebhookProviders, {
    resolve: (name) => Effect.suspend((): Effect.Effect<WebhookIngestion, WebhookProviderNotFound | WebhookConfigurationError> => {
      if (!isProviderName(name)) return Effect.fail(new WebhookProviderNotFound())
      // A malformed list (unknown names, wrong type) is a misconfiguration, not a 404.
      const enabled = decodeEnabled(env.WEBHOOK_PROVIDERS ?? [])
      if (Either.isLeft(enabled)) return Effect.fail(new WebhookConfigurationError())
      if (!enabled.right.includes(name)) return Effect.fail(new WebhookProviderNotFound())
      const provider = providers[name]
      return provider.ingestion(env[provider.secretBinding])
    }),
  })

import type { Model } from '@earendil-works/pi-ai';
import { createModels, type MutableModels } from '@earendil-works/pi-ai/models';
import { vercelAIGatewayProvider } from '@earendil-works/pi-ai/providers/vercel-ai-gateway';
import { gatewayCredential } from 'harness-world-process/gateway';
import { gatewayModelsCatalog, gatewayTokenPrice, type GatewayModelInfo } from 'harness-world-process/gateway-models';

function piModel(model: GatewayModelInfo): Model<'anthropic-messages'> {
  return {
    id: model.id,
    name: model.name,
    api: 'anthropic-messages',
    provider: 'vercel-ai-gateway',
    baseUrl: 'https://ai-gateway.vercel.sh',
    input: model.modalities.input.includes('image') ? ['text', 'image'] : ['text'],
    reasoning: model.tags.includes('reasoning'),
    contextWindow: model.context_window,
    maxTokens: model.max_tokens,
    cost: {
      input: gatewayTokenPrice(model.pricing.input), output: gatewayTokenPrice(model.pricing.output),
      cacheRead: gatewayTokenPrice(model.pricing.input_cache_read), cacheWrite: gatewayTokenPrice(model.pricing.input_cache_write),
    },
  };
}

/** Keep pi-ai's compatibility metadata; add new Gateway IDs through its refresh API. */
export function createGatewayModels(options: { apiKey?: () => Promise<string | undefined> | string | undefined } = {}): MutableModels {
  const apiKey = options.apiKey ?? gatewayCredential;
  const models = createModels({
    authContext: {
      env: async (key: string) => key === 'AI_GATEWAY_API_KEY' ? apiKey() : process.env[key],
      fileExists: async () => false,
    },
  });
  const provider = vercelAIGatewayProvider();
  const bundled = new Set(provider.getModels().map((model) => model.id));
  let added: Model<'anthropic-messages'>[] = [];
  models.setProvider({
    ...provider,
    getModels: () => [...provider.getModels(), ...added],
    getAllModels: () => [...(provider.getAllModels?.() ?? provider.getModels()), ...added],
    async refreshModels(context) {
      if (!context.allowNetwork || context.signal.aborted) return;
      const discovered = await gatewayModelsCatalog({ signal: context.signal });
      const next = discovered.filter((model) => !bundled.has(model.id)).map(piModel);
      await context.publish({ update: () => { added = next; } });
    },
  });
  return models;
}

/** Resolve lazily so an installed catalog entry continues working while discovery is offline. */
export async function resolveGatewayModel(models: MutableModels, id: string, signal?: AbortSignal): Promise<Model<any>> {
  const provider = 'vercel-ai-gateway';
  const bundled = models.getModel(provider, id);
  if (bundled) return bundled;
  const refreshed = await models.refresh({ providers: [provider], signal });
  signal?.throwIfAborted();
  if (refreshed.aborted) throw new DOMException('Gateway model discovery was aborted', 'AbortError');
  const found = models.getModel(provider, id);
  if (!found) throw new Error(`durable-harness/pi: Gateway model ${id} is absent from the bundled and live catalogs`, { cause: refreshed.errors.get(provider) });
  return found;
}

import { gatewayModelsCatalog, gatewayTokenPrice } from 'harness-world-process/gateway-models';
import type { HostInit } from './protocol.ts';

/** Model metadata only: the provider's URL and credential remain the drive's relay. */
export async function gatewayModelConfig(init: Pick<HostInit, 'model' | 'gatewayBaseURL' | 'gatewayKey'>) {
  const id = init.model.replace(/^vercel\//, '');
  const catalog = await gatewayModelsCatalog({ baseURL: init.gatewayBaseURL, headers: { authorization: `Bearer ${init.gatewayKey}` } });
  const model = catalog.find((m) => m.id === id);
  if (!model) return undefined;
  const modalities = new Set(['text', 'image', 'audio', 'video', 'pdf']);
  return {
    [id]: {
      modelID: id,
      name: model.name,
      limit: { context: model.context_window, output: model.max_tokens },
      capabilities: {
        tools: model.tags.includes('tool-use'),
        input: model.modalities.input.filter((v) => modalities.has(v)),
        output: model.modalities.output.filter((v) => modalities.has(v)),
      },
      cost: {
        input: gatewayTokenPrice(model.pricing.input), output: gatewayTokenPrice(model.pricing.output),
        cache: { read: gatewayTokenPrice(model.pricing.input_cache_read), write: gatewayTokenPrice(model.pricing.input_cache_write) },
      },
    },
  };
}

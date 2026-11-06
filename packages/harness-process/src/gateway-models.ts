/** The metadata needed to add a Gateway language model to a harness's catalog. */
export interface GatewayModelInfo {
  id: string;
  name: string;
  context_window: number;
  max_tokens: number;
  tags: string[];
  modalities: { input: string[]; output: string[] };
  pricing: { input?: string; output?: string; input_cache_read?: string; input_cache_write?: string };
}

/**
 * Read Gateway's public catalog, rather than guessing a new model's limits
 * from its name. A loopback relay can supply its placeholder authorization.
 * Embedding, media, and batch models are not interactive chat models.
 */
export async function gatewayModelsCatalog(options: { baseURL?: string; headers?: Record<string, string>; signal?: AbortSignal } = {}): Promise<GatewayModelInfo[]> {
  const url = new URL('/v1/models', options.baseURL ?? 'https://ai-gateway.vercel.sh');
  const timeout = AbortSignal.timeout(10_000);
  const response = await fetch(url, {
    headers: options.headers,
    signal: options.signal ? AbortSignal.any([options.signal, timeout]) : timeout,
    redirect: 'error',
  });
  if (!response.ok) throw new Error(`AI Gateway model discovery failed (${response.status})`);
  const body = await response.json() as { data?: unknown };
  if (!Array.isArray(body?.data)) throw new Error('AI Gateway model discovery returned no model catalog');
  return body.data.filter((value): value is GatewayModelInfo => {
    if (!value || typeof value !== 'object') return false;
    const m = value as Record<string, any>;
    return m.type === 'language' && typeof m.id === 'string' && !m.id.endsWith('-batch') && typeof m.name === 'string'
      && Number.isSafeInteger(m.context_window) && m.context_window > 0 && Number.isSafeInteger(m.max_tokens) && m.max_tokens > 0
      && Array.isArray(m.tags) && m.tags.every((tag: unknown) => typeof tag === 'string')
      && Array.isArray(m.modalities?.input) && m.modalities.input.includes('text') && m.modalities.input.every((v: unknown) => typeof v === 'string')
      && Array.isArray(m.modalities?.output) && m.modalities.output.includes('text') && m.modalities.output.every((v: unknown) => typeof v === 'string')
      && m.pricing !== null && typeof m.pricing === 'object';
  });
}

/** Gateway quotes dollars per token; harness model registries use dollars per million tokens. */
export function gatewayTokenPrice(price: string | undefined): number {
  const value = Number(price);
  return Number.isFinite(value) && value >= 0 ? value * 1_000_000 : 0;
}

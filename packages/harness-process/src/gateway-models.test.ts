import { afterEach, describe, expect, it, vi } from 'vitest';
import { gatewayModelsCatalog, gatewayTokenPrice } from './gateway-models.ts';

const language = { id: 'example/new-model', name: 'New model', type: 'language', context_window: 200000, max_tokens: 16000, tags: ['tool-use'], modalities: { input: ['text', 'image'], output: ['text'] }, pricing: { input: '0.000001', output: '0.000002' } };
afterEach(() => vi.unstubAllGlobals());

describe('Gateway model discovery', () => {
  it('keeps interactive language models and preserves real limits and prices', async () => {
    const fetcher = vi.fn(async () => Response.json({ data: [language, { ...language, type: 'embedding' }, { ...language, id: 'example/new-model-batch' }, { ...language, max_tokens: -1 }, { ...language, modalities: { input: ['text'], output: ['image'] } }, null] }));
    vi.stubGlobal('fetch', fetcher);
    expect(await gatewayModelsCatalog({ baseURL: 'http://127.0.0.1:1234/v3/ai', headers: { authorization: 'Bearer placeholder' } })).toEqual([language]);
    expect(fetcher).toHaveBeenCalledWith(new URL('http://127.0.0.1:1234/v1/models'), expect.objectContaining({ headers: { authorization: 'Bearer placeholder' }, redirect: 'error', signal: expect.any(AbortSignal) }));
    expect(gatewayTokenPrice(language.pricing.input)).toBe(1);
    expect(gatewayTokenPrice(undefined)).toBe(0);
    expect(gatewayTokenPrice('invalid')).toBe(0);
  });

  it('reports a failed or malformed catalog without inventing model metadata', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('unavailable', { status: 503 })));
    await expect(gatewayModelsCatalog()).rejects.toThrow('503');
    vi.stubGlobal('fetch', vi.fn(async () => Response.json({ data: {} })));
    await expect(gatewayModelsCatalog()).rejects.toThrow('no model catalog');
  });

  it('passes caller cancellation to the discovery request', async () => {
    const controller = new AbortController();
    controller.abort();
    vi.stubGlobal('fetch', vi.fn(async (_url, init) => { init.signal.throwIfAborted(); return Response.json({ data: [] }); }));
    await expect(gatewayModelsCatalog({ signal: controller.signal })).rejects.toThrow();
  });
});

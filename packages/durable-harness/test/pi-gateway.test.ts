import { afterEach, describe, expect, it, vi } from 'vitest';
import { createGatewayModels, resolveGatewayModel } from '../src/pi-gateway.ts';

const provider = 'vercel-ai-gateway';
const model = { id: 'example/new-model', name: 'New model', type: 'language', context_window: 300000, max_tokens: 20000, tags: ['tool-use', 'reasoning'], modalities: { input: ['text', 'image'], output: ['text'] }, pricing: { input: '0.000001', output: '0.000002', input_cache_read: '0.0000001' } };
afterEach(() => vi.unstubAllGlobals());

describe('Pi Gateway catalog refresh', () => {
  it('discovers an unknown ID on demand and leaves known IDs independent of discovery', async () => {
    const fetcher = vi.fn(async () => Response.json({ data: [model] }));
    vi.stubGlobal('fetch', fetcher);
    const models = createGatewayModels({ apiKey: async () => 'test' });
    expect((await resolveGatewayModel(models, 'anthropic/claude-haiku-4.5')).id).toBe('anthropic/claude-haiku-4.5');
    expect(fetcher).not.toHaveBeenCalled();
    expect((await resolveGatewayModel(models, model.id)).id).toBe(model.id);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it('preserves a discovery failure as the missing-model error cause', async () => {
    const failure = new Error('discovery offline');
    vi.stubGlobal('fetch', vi.fn(async () => { throw failure; }));
    const models = createGatewayModels({ apiKey: async () => 'test' });
    await expect(resolveGatewayModel(models, model.id)).rejects.toMatchObject({ cause: failure });
  });

  it('reports caller cancellation instead of classifying a cancelled lookup as an unknown model', async () => {
    const controller = new AbortController();
    controller.abort();
    const models = createGatewayModels({ apiKey: async () => 'test' });
    await expect(resolveGatewayModel(models, model.id, controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });

  it('adds new IDs with live limits and costs while keeping bundled compatibility metadata', async () => {
    const fetcher = vi.fn(async () => Response.json({ data: [model, { ...model, id: 'anthropic/claude-haiku-4.5' }] }));
    vi.stubGlobal('fetch', fetcher);
    const models = createGatewayModels({ apiKey: async () => 'not-used-for-public-discovery' });
    const bundled = models.getModel(provider, 'anthropic/claude-haiku-4.5');
    expect(bundled).toBeDefined();
    expect(models.getModel(provider, model.id)).toBeUndefined();
    expect(fetcher).not.toHaveBeenCalled();
    expect((await models.refresh({ providers: [provider] })).errors.size).toBe(0);
    expect(models.getModel(provider, model.id)).toMatchObject({ api: 'anthropic-messages', provider, contextWindow: 300000, maxTokens: 20000, reasoning: true, input: ['text', 'image'], cost: { input: 1, output: 2, cacheWrite: 0 } });
    expect(models.getModel(provider, model.id)!.cost.cacheRead).toBeCloseTo(0.1);
    expect(models.getModel(provider, 'anthropic/claude-haiku-4.5')).toEqual(bundled);
    expect(models.getAllModels(provider).some((m) => m.id === model.id)).toBe(true);
  });

  it('retains the previous catalog on refresh failure and makes offline refresh network-free', async () => {
    const fetcher = vi.fn(async () => Response.json({ data: [model] }));
    vi.stubGlobal('fetch', fetcher);
    const models = createGatewayModels({ apiKey: async () => 'test' });
    await models.refresh();
    fetcher.mockImplementation(async () => { throw new Error('offline'); });
    expect((await models.refresh()).errors.get(provider)?.message).toBe('offline');
    expect(models.getModel(provider, model.id)).toBeDefined();
    fetcher.mockClear();
    await models.refresh({ allowNetwork: false });
    expect(fetcher).not.toHaveBeenCalled();
  });
});

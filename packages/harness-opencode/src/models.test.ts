import { afterEach, describe, expect, it, vi } from 'vitest';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { gatewayProxy } from 'harness-world-process/gateway';
import { gatewayModelConfig } from './models.ts';

afterEach(() => vi.unstubAllGlobals());

describe('OpenCode Gateway model metadata', () => {
  it('discovers through the real relay with the driver credential replacing the placeholder', async () => {
    let authorization: string | undefined;
    let path: string | undefined;
    const upstream = createServer((req, res) => {
      authorization = req.headers.authorization;
      path = req.url;
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ data: [{ id: 'example/new-model', name: 'New model', type: 'language', context_window: 300000, max_tokens: 16000, tags: ['tool-use'], modalities: { input: ['text'], output: ['text'] }, pricing: {} }] }));
    });
    await new Promise<void>((resolve, reject) => {
      upstream.once('error', reject);
      upstream.listen(0, '127.0.0.1', resolve);
    });
    const proxy = await gatewayProxy({ target: `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`, token: 'placeholder', credential: async () => 'driver-test-credential' });
    try {
      const config = await gatewayModelConfig({ model: 'vercel/example/new-model', gatewayBaseURL: `${proxy.url}/v3/ai`, gatewayKey: 'placeholder' });
      expect(config?.['example/new-model'].modelID).toBe('example/new-model');
      expect(path).toBe('/v1/models');
      expect(authorization).toBe('Bearer driver-test-credential');
    } finally {
      await proxy.close();
      await new Promise<void>((resolve, reject) => upstream.close((error) => error ? reject(error) : resolve()));
    }
  });

  it('registers an exact new ID without changing the relay URL or credential', async () => {
    const model = { id: 'example/new-model', name: 'New model', type: 'language', context_window: 300000, max_tokens: 16000, tags: ['tool-use'], modalities: { input: ['text', 'image'], output: ['text'] }, pricing: { input: '0.000001', output: '0.000002' } };
    const fetcher = vi.fn(async () => Response.json({ data: [model] }));
    vi.stubGlobal('fetch', fetcher);
    const config = await gatewayModelConfig({ model: 'vercel/example/new-model', gatewayBaseURL: 'http://127.0.0.1:1234/v3/ai', gatewayKey: 'placeholder' });
    expect(config).toEqual({ 'example/new-model': { modelID: model.id, name: model.name, limit: { context: 300000, output: 16000 }, capabilities: { tools: true, input: ['text', 'image'], output: ['text'] }, cost: { input: 1, output: 2, cache: { read: 0, write: 0 } } } });
    expect(fetcher).toHaveBeenCalledWith(new URL('http://127.0.0.1:1234/v1/models'), expect.objectContaining({ headers: { authorization: 'Bearer placeholder' } }));
  });

  it('leaves the bundled catalog intact when an ID is absent from Gateway', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => Response.json({ data: [] })));
    expect(await gatewayModelConfig({ model: 'vercel/example/missing', gatewayBaseURL: 'http://127.0.0.1:1234/v3/ai', gatewayKey: 'placeholder' })).toBeUndefined();
  });
});

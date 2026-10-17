import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fxAgent } from './fx-durable';

const captured = vi.hoisted(() => ({ fetch: undefined as typeof fetch | undefined }));
vi.mock('libfx', () => ({ createFxAgent: async (options: { fetch: typeof fetch }) => {
  captured.fetch = options.fetch;
  return {};
} }));
vi.mock('./model', () => ({ MODEL: 'test', INSTRUCTIONS: '' }));
vi.mock('./fx-durable-world', () => ({ fxDurability: async () => undefined, fxDurableDisabledReason: () => undefined, refuseDisabled: vi.fn() }));
vi.mock('./world', () => ({ getWorld: async () => ({ events: { list: async () => ({ data: [] }) }, streams: { write: async () => {} } }) }));

beforeEach(() => { vi.stubEnv('HARNESS_CHAOS', '0'); });
afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.clearAllMocks();
  delete (globalThis as { __fxDurableAgent?: unknown }).__fxDurableAgent;
  captured.fetch = undefined;
});

async function modelRequest() {
  await fxAgent();
  return captured.fetch!('https://gateway.example.test/v3/ai/language-model', {
    method: 'POST', headers: { 'x-session-id': 'test-session' }, body: JSON.stringify({ prompt: [] }),
  });
}

describe("the libfx lane's model fetch", () => {
  it.each([
    { status: 200, body: 'data: {"type":"text-delta","delta":"hi"}\n\n' },
    { status: 204, body: null },
    { status: 429, body: '{"error":"rate limited"}' },
    { status: 500, body: null },
    { status: 302, body: null },
  ])('passes HTTP $status and its body through', async ({ status, body }) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body, { status })));
    const response = await modelRequest();
    expect(response.status).toBe(status);
    expect(await response.text()).toBe(body ?? '');
  });
  it('rejects with the fetch rejection', async () => {
    const error = new Error('network down');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(error));
    await expect(modelRequest()).rejects.toBe(error);
  });
  it("fails the response's stream with the stream's own error", async () => {
    const error = new Error('stream interrupted');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(new ReadableStream({
      pull(controller) { controller.error(error); },
    }), { status: 500 })));
    const response = await modelRequest();
    await expect(response.text()).rejects.toBe(error);
  });
});

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fxAgent, withFxTraces } from './fx-durable';

const traced = vi.hoisted(() => ({ complete: vi.fn(async () => {}), fail: vi.fn(async () => {}), fetch: undefined as typeof fetch | undefined }));
vi.mock('libfx', () => ({ createFxAgent: async (options: { fetch: typeof fetch }) => {
  traced.fetch = options.fetch;
  return {};
} }));
vi.mock('./model', () => ({ MODEL: 'test', INSTRUCTIONS: '' }));
vi.mock('./fx-durable-world', () => ({ fxDurability: async () => undefined, fxDurableDisabledReason: () => undefined, refuseDisabled: vi.fn() }));
vi.mock('./world', () => ({ getWorld: async () => ({ events: { list: async () => ({ data: [] }) }, streams: { write: async () => {} } }) }));
vi.mock('harness-world/agent-runs', () => {
  const step = { modelCall: async () => ({ complete: traced.complete, fail: traced.fail }), attributes() {}, complete: async () => {}, tool: async () => undefined };
  const turn = { findTool: () => undefined, complete: async () => {} };
  return { agentRunsHook: () => ({ session: () => ({ turn: async () => turn, step: async () => step, flush: async () => {} }) }) };
});

beforeEach(() => { vi.stubEnv('HARNESS_CHAOS', '0'); });
afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.clearAllMocks();
  delete (globalThis as { __fxDurableAgent?: unknown }).__fxDurableAgent;
  traced.fetch = undefined;
});

async function modelRequest() {
  await fxAgent();
  return withFxTraces(() => traced.fetch!('https://gateway.example.test/v3/ai/language-model', {
    method: 'POST', headers: { 'x-session-id': 'test-session' }, body: JSON.stringify({ prompt: [] }),
  }));
}

describe('native libfx model response tracing', () => {
  it.each([
    { status: 200, body: 'data: {"type":"text-delta","delta":"hi"}\n\n' },
    { status: 204, body: null },
    { status: 429, body: '{"error":"rate limited"}' },
    { status: 500, body: null },
    { status: 302, body: null },
  ])('preserves HTTP $status and marks non-success responses as failures', async ({ status, body }) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body, { status })));
    const response = await modelRequest();
    expect(response.status).toBe(status);
    expect(await response.text()).toBe(body ?? '');
    if (status >= 200 && status < 300) {
      expect(traced.complete).toHaveBeenCalledTimes(1);
      expect(traced.fail).not.toHaveBeenCalled();
    } else {
      expect(traced.complete).not.toHaveBeenCalled();
      expect(traced.fail).toHaveBeenCalledWith(new Error(`Model request failed (${status})`));
    }
  });
  it('preserves a fetch rejection as the trace error', async () => {
    const error = new Error('network down');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(error));
    await expect(modelRequest()).rejects.toBe(error);
    expect(traced.fail).toHaveBeenCalledTimes(1);
    expect(traced.fail).toHaveBeenCalledWith(error);
  });
  it('preserves a response stream failure instead of masking it with the HTTP error', async () => {
    const error = new Error('stream interrupted');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(new ReadableStream({
      pull(controller) { controller.error(error); },
    }), { status: 500 })));
    const response = await modelRequest();
    await expect(response.text()).rejects.toBe(error);
    expect(traced.fail).toHaveBeenCalledTimes(1);
    expect(traced.fail).toHaveBeenCalledWith(error);
  });
});

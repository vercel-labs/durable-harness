import type { AgentRun } from '@vercel/agent-runs';
import type { DriveContext, Harness, SessionState } from 'harness-world';
import { describe, expect, it, vi } from 'vitest';
import type { TraceStreams } from '../src/instrumentation/checkpoints.ts';
import { traceDrive } from '../src/instrumentation/drive.ts';

function setup(failure: 'start' | 'run' | 'complete' | 'fail') {
  const unavailable = new Error('tracing unavailable');
  const model = {
    run: <T>(execute: () => T): T => { if (failure === 'run') throw unavailable; return execute(); },
    complete: async () => { if (failure === 'complete') throw unavailable; },
    fail: async () => { if (failure === 'fail') throw unavailable; },
  };
  const step = { complete: async () => {}, fail: async () => {}, modelCall: async () => { if (failure === 'start') throw unavailable; return model; } };
  const turn = { finished: false, attempt: async () => step, complete: async () => {} };
  const run = { turn: async () => turn, resume: async () => turn } as unknown as AgentRun;
  const streams = { forget() {}, setDeadline() {}, finished: async () => false, flush: async () => {} } as unknown as TraceStreams;
  const prompt = { id: 'prompt', kind: 'prompt', text: 'hello' };
  const state = { status: 'running', submissions: [prompt], placed: new Set([prompt.id]), effects: new Map(), effectOrder: [] } as unknown as SessionState;
  const ctx = {
    runId: 'run', config: { traceAudience: 'public' }, signal: new AbortController().signal,
    state: () => state, pending: () => [], effect: async (_key: string, _options: unknown, execute: (io: unknown) => Promise<unknown>) => execute({}),
  } as unknown as DriveContext;
  return { options: { run, streams, audience: 'public' as const, capture: () => ({ emit: true, recordInputs: true, recordOutputs: true }), version: 'test' }, ctx };
}

describe('model effects with failing tracing', () => {
  it.each(['start', 'run', 'complete'] as const)('preserves the model result and executes once when tracing fails during %s', async failure => {
    const { options, ctx } = setup(failure);
    const execute = vi.fn(async () => 'model reply');
    const harness: Harness = { name: 'test', async drive(ctx) {
      expect(await ctx.effect('model:1', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, execute)).toBe('model reply');
    } };
    await traceDrive(options, harness, ctx);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('preserves the model error when recording that error fails', async () => {
    const { options, ctx } = setup('fail');
    const original = new Error('model unavailable');
    const execute = vi.fn(async () => { throw original; });
    const harness: Harness = { name: 'test', async drive(ctx) {
      await ctx.effect('model:1', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, execute);
    } };
    await expect(traceDrive(options, harness, ctx)).rejects.toBe(original);
    expect(execute).toHaveBeenCalledTimes(1);
  });
});

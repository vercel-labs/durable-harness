import type { DriveContext, EffectIO, EffectOptions, SessionState } from 'harness-world';
import { describe, expect, it } from 'vitest';
import { isModelRequest, modelCrashArmed, ModelSteps, settleAbandonedModelSteps, unfinishedModelStep } from './model.ts';

/** Just enough of a fold for the gate: an armed crash, and the steps so far. */
function stateWith(opts: { armed?: 'tool' | 'model' | 'any' | 'sandbox'; kind?: string; pending?: string[] } = {}): SessionState {
  return {
    submissions: opts.armed ? [{ id: 'c1', kind: 'chaos', chaos: { at: opts.armed, ...(opts.kind ? { kind: opts.kind } : {}) } }] : [],
    placed: new Set(),
    withdrawn: new Set(),
    effectOrder: (opts.pending ?? []).map((key) => ({ key, name: 'model', status: 'pending' })),
  } as unknown as SessionState;
}

/** A drive context whose effects only record that they ran and what they returned. */
function stubCtx(state: SessionState) {
  const abort = new AbortController();
  const effects: { key: string; options: EffectOptions; output?: unknown; error?: unknown }[] = [];
  const ctx = {
    runId: 'run',
    state: () => state,
    signal: abort.signal,
    halt: (err: unknown) => abort.abort(err),
    effect: async <T>(key: string, options: EffectOptions, fn: (io: EffectIO) => Promise<T>) => {
      const e: (typeof effects)[number] = { key, options };
      effects.push(e);
      try {
        e.output = await fn({ signal: abort.signal, stepId: `step_${key}` } as EffectIO);
        return e.output as T;
      } catch (err) {
        e.error = err;
        throw err;
      }
    },
  } as unknown as DriveContext;
  return { ctx, effects, abort };
}

const tick = () => new Promise((r) => setTimeout(r, 0));
const post = { method: 'POST', path: '/v1/messages' };

describe('ModelSteps', () => {
  it('counts POSTs as model requests, not token counts or reads', () => {
    expect(isModelRequest(post)).toBe(true);
    expect(isModelRequest({ method: 'POST', path: '/v1/messages/count_tokens' })).toBe(false);
    expect(isModelRequest({ method: 'GET', path: '/v1/models' })).toBe(false);
    expect(isModelRequest({ method: 'HEAD', path: '/api/hello' })).toBe(false);
  });

  it('is armed by a process crash at the model or any step', () => {
    expect(modelCrashArmed(stateWith())).toBe(false);
    expect(modelCrashArmed(stateWith({ armed: 'tool' }))).toBe(false);
    expect(modelCrashArmed(stateWith({ armed: 'model' }))).toBe(true);
    expect(modelCrashArmed(stateWith({ armed: 'any' }))).toBe(true);
    expect(modelCrashArmed(stateWith({ armed: 'sandbox', kind: 'sandbox-vm' }))).toBe(false);
    // A dropped relay on the session's sandbox (D4) fires in a model step too; a stopped VM (D1) only in a tool call.
    expect(modelCrashArmed(stateWith({ armed: 'model', kind: 'relay-drop' }))).toBe(true);
    expect(modelCrashArmed(stateWith({ armed: 'tool', kind: 'relay-drop' }))).toBe(false);
    expect(modelCrashArmed(stateWith({ armed: 'tool', kind: 'sandbox-vm' }))).toBe(false);
    expect(unfinishedModelStep(stateWith({ pending: ['model:a', 'model:b'] }), new Set(['model:a']))).toBe('model:b');
  });

  it('marks every model request on the live stream, drill or not: request, first token, done', async () => {
    const { ctx } = stubCtx(stateWith());
    const marks: string[] = [];
    (ctx as unknown as { mark: (name: string, f: { key: string; status?: number }) => void }).mark = (name, f) => marks.push(`${name} ${f.key}${f.status ? ` ${f.status}` : ''}`);
    const steps = new ModelSteps(ctx);
    const end = steps.request(post)!;
    end.firstToken?.();
    end.firstToken?.();
    end({ status: 200 });
    end({ status: 200 });
    expect(steps.request({ method: 'GET', path: '/v1/models' })).toBeUndefined();
    steps.request(post)!({ error: 'cut' });
    await steps.close();
    expect(marks).toEqual(['model.request request:0', 'model.first_token request:0', 'model.done request:0 200', 'model.request request:1', 'model.done request:1']);
  });

  it('writes nothing when no drill is armed', async () => {
    const { ctx, effects } = stubCtx(stateWith({ armed: 'tool' }));
    const steps = new ModelSteps(ctx);
    steps.request(post)!({ status: 200 });
    await steps.close();
    expect(effects).toEqual([]);
  });

  it('one step for overlapping requests, closed when the last ends; a later request gets its own', async () => {
    const { ctx, effects } = stubCtx(stateWith({ armed: 'model' }));
    const steps = new ModelSteps(ctx);
    const endA = steps.request(post)!;
    const endB = steps.request({ method: 'POST', path: '/v1/messages?beta=true' })!;
    endA({ status: 200 });
    await tick();
    expect(effects).toHaveLength(1);
    expect(effects[0]!.output).toBeUndefined();
    endB({ error: 'the connection closed before the response ended' });
    await tick();
    expect(effects[0]).toMatchObject({ options: { name: 'model', replay: 'safe', input: post }, output: { requests: 2, error: 'the connection closed before the response ended' } });
    expect(effects[0]!.key).toMatch(/^model:/);
    steps.request(post)!({ status: 200 });
    await steps.close();
    expect(effects).toHaveLength(2);
    expect(effects[1]!.key).not.toBe(effects[0]!.key);
  });

  it('retries the step a crash left unfinished, once', async () => {
    const { ctx, effects } = stubCtx(stateWith({ pending: ['model:killed'] }));
    const steps = new ModelSteps(ctx);
    steps.request(post)!({ status: 200 });
    await tick();
    steps.request(post)!({ status: 200 });
    await steps.close();
    // The first request retried it; nothing is armed, so the second wrote nothing.
    expect(effects.map((e) => e.key)).toEqual(['model:killed']);
  });

  it('a drive that stops mid-step leaves the outcome to the kernel (the step rejects with the reason)', async () => {
    const { ctx, effects, abort } = stubCtx(stateWith({ armed: 'any' }));
    const steps = new ModelSteps(ctx);
    steps.request(post);
    await tick();
    abort.abort(new Error('preempted'));
    await steps.close();
    expect(effects[0]!.error).toEqual(new Error('preempted'));
    // Closed: later requests are not tracked.
    expect(steps.request(post)).toBeUndefined();
  });

  it('closes the model steps a crash left unfinished that no request of the drive retried, as abandoned', async () => {
    const { ctx, effects } = stubCtx(stateWith({ pending: ['model:a', 'model:b'] }));
    // `model:a` was retried by this drive's request; `model:b` never will be.
    expect(await settleAbandonedModelSteps(ctx, new Set(['model:a']))).toBe(1);
    expect(effects.map((e) => [e.key, e.options.name, e.options.replay, e.output])).toEqual([['model:b', 'model', 'safe', { requests: 0, abandoned: true }]]);
    // Nothing unfinished: nothing written.
    const none = stubCtx(stateWith());
    expect(await settleAbandonedModelSteps(none.ctx)).toBe(0);
    expect(none.effects).toEqual([]);
  });

  it('settleAbandoned skips the steps its own requests retried', async () => {
    const { ctx, effects } = stubCtx(stateWith({ armed: 'model', pending: ['model:a'] }));
    const steps = new ModelSteps(ctx);
    // The drive's request retries `model:a` (attempt 2) and ends.
    steps.request(post)?.({ status: 200 });
    await tick();
    expect(await steps.settleAbandoned()).toBe(0);
    expect(effects.map((e) => e.key)).toEqual(['model:a']);
    await steps.close();
  });
});

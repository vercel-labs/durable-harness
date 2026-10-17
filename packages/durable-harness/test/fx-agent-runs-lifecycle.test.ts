import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { AGENT_RUNS, agentRunsHook } from 'harness-world/agent-runs';
import { readJsonLines } from 'harness-world/streams';
import type { FxHookEvent } from 'libfx';
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { agentRunsHooks } from '../src/fx.ts';
import { agentRuns } from '../src/instrumentation/otel.ts';

const exporter = new InMemorySpanExporter();
const errors: unknown[] = [];
const runs = agentRuns({ audience: 'public', spanProcessors: [new SimpleSpanProcessor(exporter)], onError: error => { errors.push(error); } });
runs.register();
const worlds: LocalWorld[] = [];
beforeEach(() => { errors.length = 0; });
afterEach(() => vi.restoreAllMocks());
afterAll(async () => { await runs.forceFlush(); for (const world of worlds) await world.close?.(); });
let number = 0;
const call = { callId: 'call_1', name: 'lookup', provenance: 'fx_local', argumentsJson: '{"query":"hello"}' };

async function fixture(audience: 'public' | 'private' | 'unknown' = 'public') {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'fx-trace-lifecycle-')), recoverActiveRuns: false });
  worlds.push(world);
  const runId = `wrun_fx_lifecycle_${++number}`;
  const hooks = agentRunsHooks({ world, name: 'fx-lifecycle', audience });
  const event = (executionId: string | null, type: string, data: unknown, stepIndex = 0, turnId: string | null = 'turn') => ({
    version: 1, sessionId: runId, rootSessionId: runId, parentSessionId: null, sourceSessionId: null, workId: null,
    turnId, executionId, stepIndex, timestamp: Date.now(), contentCaptured: true, contentTruncated: false, type, data,
  }) as unknown as FxHookEvent;
  const send = (executionId: string | null, type: string, data: unknown, stepIndex = 0, turnId: string | null = 'turn') => hooks.onEvent(event(executionId, type, data, stepIndex, turnId));
  const model = (executionId: string, output: string, stepIndex = 1, turnId = 'turn') => {
    send(executionId, 'step.start', {}, stepIndex, turnId);
    send(executionId, 'model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [{ role: 'user', content: output, toolCalls: [] }], tools: [] }, stepIndex, turnId);
    send(executionId, 'model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output, provider: 'test-provider', usage: { inputTokens: 4, outputTokens: 2, cacheReadTokens: 0, cacheWriteTokens: 0, reasoningTokens: 1 } }, stepIndex, turnId);
  };
  const spans = () => exporter.getFinishedSpans().filter(span => span.attributes['agent.run.id'] === runId);
  return { world, runId, hooks, event, send, model, spans };
}

describe('fx handoff and terminal outcomes', () => {
  it.each(['completed', 'cancelled', 'interrupted', 'failed'])('settles a recovered tool that is not rerun when its turn ends %s', async outcome => {
    const f = await fixture();
    f.send('one', 'turn.start', { resumed: false, input: 'Look up the record' });
    f.model('one', 'Looking up the record');
    f.send('one', 'tool.start', { call, rerun: false }, 1);
    f.send('one', 'wait.start', { requestId: 'approval', kind: 'approval', callId: call.callId, prompt: 'Continue?' }, 1);
    f.send('one', 'wait.end', { requestId: 'approval', outcome: 'cancelled', error: null }, 1);
    f.send('one', 'tool.end', { call, status: 'cancelled', error: null }, 1);
    f.send('one', 'step.end', {}, 1);
    f.send('one', 'turn.end', { outcome: 'interrupted', handoff: true, error: null });
    await f.hooks.flush();
    expect(f.spans().filter(span => span.name === 'execute_tool lookup')).toHaveLength(0);

    // A fresh worker can hand off again without ever receiving another event for the ambiguous call.
    for (const execution of ['two', 'three']) {
      const hooks = agentRunsHooks({ world: new Proxy(f.world, {}), name: 'fx-lifecycle', audience: 'public' });
      hooks.onEvent(f.event(execution, 'turn.start', { resumed: true }));
      hooks.onEvent(f.event(execution, 'turn.end', execution === 'two'
        ? { outcome: 'interrupted', handoff: true, error: null }
        : { outcome, error: outcome === 'failed' ? 'turn failed' : null }));
      await hooks.flush();
    }
    await runs.forceFlush();
    const spans = f.spans();
    expect(spans.filter(span => span.name === 'invoke_agent fx-lifecycle')).toHaveLength(1);
    const tools = spans.filter(span => span.name === 'execute_tool lookup');
    expect(tools).toHaveLength(1);
    expect(spans.filter(span => span.name === 'agent.approval')).toHaveLength(1);
    const names = (await f.world.streams.list(f.runId)).filter(name => name.includes('_agent-runs_') && !name.endsWith('_sequence'));
    expect(names).toHaveLength(1);
    expect((await readJsonLines(f.world, f.runId, names[0]!)).at(-1)).toMatchObject({ v: null });
    expect(await agentRunsHook()!.session({ world: new Proxy(f.world, {}), runId: f.runId, agentName: 'fx-lifecycle' }).resume('turn')).toBeUndefined();
    expect(new Set(spans.map(span => span.spanContext().spanId)).size).toBe(spans.length);
  });

  it.each([
    { outcome: 'cancelled', handoff: true, fenced: false },
    { outcome: 'interrupted', handoff: true, fenced: false },
    { outcome: 'yielded', handoff: false, fenced: false },
    { outcome: 'paused', handoff: false, fenced: false },
    { outcome: 'fenced', handoff: false, fenced: false },
    { outcome: 'failed', handoff: false, fenced: true },
  ])('continues $outcome (handoff=$handoff, journal fenced=$fenced) without settling its cancelled tool', async ({ outcome, handoff, fenced }) => {
    const f = await fixture();
    f.send('one', 'turn.start', { resumed: false, input: 'Look up the record' });
    f.model('one', 'Looking up the record');
    f.send('one', 'tool.start', { call, rerun: false }, 1);
    f.send('one', 'wait.start', { requestId: 'approval', kind: 'approval', callId: call.callId, prompt: 'Continue?' }, 1);
    if (fenced) f.send(null, 'journal.error', { fenced: true, error: 'FX_FENCED' }, 0, null);
    f.send('one', 'wait.end', { requestId: 'approval', outcome: 'cancelled', error: null }, 1);
    f.send('one', 'tool.end', { call, status: 'cancelled', error: 'execution stopped' }, 1);
    f.send('one', 'step.end', {}, 1);
    f.send('one', 'turn.end', { outcome, handoff, error: fenced ? 'fenced' : null });
    await f.hooks.flush();
    expect(f.spans().filter(span => span.name === 'invoke_agent fx-lifecycle')).toHaveLength(0);
    expect(f.spans().filter(span => span.name === 'execute_tool lookup')).toHaveLength(0);

    f.send('two', 'turn.start', { resumed: true });
    f.send('two', 'step.start', {}, 1);
    f.send('two', 'tool.start', { call, rerun: true }, 1);
    f.send('two', 'wait.start', { requestId: 'approval', kind: 'approval', callId: call.callId, prompt: 'Continue?' }, 1);
    f.send('two', 'wait.end', { requestId: 'approval', outcome: 'accepted', error: null }, 1);
    f.send('two', 'tool.end', { call, status: 'success', output: '"found"', error: null }, 1);
    f.send('two', 'step.end', {}, 1);
    f.model('two', 'The record was found', 2);
    f.send('two', 'step.end', {}, 2);
    f.send('two', 'turn.end', { outcome: 'completed', error: null });
    await f.hooks.flush();
    await runs.forceFlush();
    const spans = f.spans();
    const roots = spans.filter(span => span.name === 'invoke_agent fx-lifecycle');
    expect(roots).toHaveLength(1);
    expect(roots[0]!.attributes['agent.turn.outcome']).toBe('completed');
    const tools = spans.filter(span => span.name === 'execute_tool lookup');
    expect(tools).toHaveLength(1);
    expect(tools[0]!.attributes['gen_ai.tool.call.result']).toContain('found');
    const approvals = spans.filter(span => span.name === 'agent.approval');
    expect(approvals).toHaveLength(1);
    expect(approvals[0]!.attributes['agent.approval.outcome']).toBe('approved');
    expect(spans.filter(span => span.status.code === 2)).toEqual([]);
  });

  it.each([
    { outcome: 'interrupted', expected: 'cancelled' },
    { outcome: 'cancelled', expected: 'cancelled' },
    { outcome: 'failed', expected: 'failed' },
    { outcome: 'completed', expected: 'completed' },
  ])('finishes a non-handoff $outcome turn and its pending children', async ({ outcome, expected }) => {
    const f = await fixture();
    f.send('one', 'turn.start', { resumed: false, input: 'Look up the record' });
    f.model('one', 'Looking up the record');
    f.send('one', 'tool.start', { call, rerun: false }, 1);
    f.send('one', 'wait.start', { requestId: 'approval', kind: 'approval', callId: call.callId, prompt: 'Continue?' }, 1);
    f.send('one', 'tool.end', { call, status: 'cancelled', error: null }, 1);
    f.send('one', 'step.end', {}, 1);
    f.send('one', 'turn.end', { outcome, handoff: false, error: outcome === 'failed' ? 'turn failed' : null });
    await f.hooks.flush();
    await runs.forceFlush();
    const roots = f.spans().filter(span => span.name === 'invoke_agent fx-lifecycle');
    expect(roots).toHaveLength(1);
    expect(roots[0]!.attributes['agent.turn.outcome']).toBe(expected);
    expect(f.spans().filter(span => span.name === 'execute_tool lookup')).toHaveLength(1);
    expect(f.spans().filter(span => span.name === 'agent.approval')).toHaveLength(1);
    expect(await agentRunsHook()!.session({ world: f.world, runId: f.runId, agentName: 'fx-lifecycle' }).resume('turn')).toBeUndefined();
  });
});

describe('fx execution isolation and bookkeeping', () => {
  it('ignores old execution completions without closing the current execution or losing metadata', async () => {
    const f = await fixture();
    f.send('old', 'turn.start', { resumed: false, input: 'first' });
    f.send('old', 'step.start', {}, 1);
    f.send('old', 'model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [], tools: [] }, 1);
    f.send('new', 'turn.start', { resumed: true });
    f.send('new', 'step.start', {}, 1);
    f.send('new', 'model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [], tools: [] }, 1);
    f.send('old', 'model.end', { modelCallId: 1, error: null, cancelled: false, output: 'stale answer', usage: {} }, 1);
    f.send('old', 'step.end', {}, 1);
    f.send('old', 'turn.end', { outcome: 'cancelled', error: null });
    await f.hooks.flush();
    expect(f.spans().filter(span => span.name.startsWith('invoke_agent'))).toHaveLength(0);
    f.send('new', 'model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output: 'current answer', provider: 'actual-provider', usage: { inputTokens: 20, outputTokens: 5, reasoningTokens: 7 } }, 1);
    f.send('new', 'step.end', {}, 1);
    f.send('new', 'turn.end', { outcome: 'completed', error: null });
    await f.hooks.flush();
    await runs.forceFlush();
    const spans = f.spans();
    expect(spans.filter(span => span.name.startsWith('invoke_agent'))).toHaveLength(1);
    const model = spans.find(span => String(span.attributes['gen_ai.output.messages']).includes('current answer'))!;
    expect(model.attributes['gen_ai.provider.name']).toBe('actual-provider');
    expect(spans.find(span => span.spanContext().spanId === model.parentSpanContext?.spanId)!.attributes['agent.usage.reasoning_tokens']).toBe(7);
    expect(JSON.stringify(spans.map(span => span.attributes))).not.toContain('stale answer');
    const steps = spans.filter(span => span.name === 'agent.step');
    expect(new Set(steps.map(span => span.spanContext().spanId)).size).toBe(2);
  });

  it.each(['completed', 'yielded'])('evicts a drained %s acquisition and ignores late receipts and title calls', async outcome => {
    const f = await fixture();
    const acquire = vi.spyOn(agentRunsHook()!, 'session');
    f.send('one', 'turn.start', { resumed: false, input: 'first' });
    f.send('one', 'turn.end', { outcome, error: null });
    await f.hooks.flush();
    expect(acquire).toHaveBeenCalledTimes(1);
    f.send(null, 'journal.commit', { cursor: '1', firstSeq: 1, lastSeq: 1, eventCount: 1 }, 0, null);
    f.send(null, 'journal.restore', { cursor: '1', lastSeq: 1, resumable: true, openTurnId: null, lastTurnId: 'turn' }, 0, null);
    f.send(null, 'model.start', { modelCallId: 9, purpose: 'title', model: 'test/model', tools: [] }, 0, null);
    f.send('one', 'model.start', { modelCallId: 10, purpose: 'title', model: 'test/model', tools: [] });
    await f.hooks.flush();
    expect(acquire).toHaveBeenCalledTimes(1);
    const nextTurn = outcome === 'completed' ? 'second' : 'turn';
    f.send('two', 'turn.start', { resumed: outcome !== 'completed', input: 'second' }, 0, nextTurn);
    f.send('two', 'turn.end', { outcome: 'completed', error: null }, 0, nextTurn);
    await f.hooks.flush();
    expect(acquire).toHaveBeenCalledTimes(2);
    await runs.forceFlush();
    const turns = f.spans().filter(span => span.name.startsWith('invoke_agent'));
    expect(turns.map(span => span.attributes['agent.turn.sequence']).sort()).toEqual(outcome === 'completed' ? [0, 1] : [0]);
  });

  it('forwards hook failures to agentRuns onError', async () => {
    const f = await fixture();
    const error = new Error('hook unavailable');
    f.hooks.onError?.(error);
    expect(errors).toEqual([error]);
  });

  it('does nothing until tracing is registered', async () => {
    const current = (globalThis as Record<symbol, unknown>)[AGENT_RUNS];
    delete (globalThis as Record<symbol, unknown>)[AGENT_RUNS];
    try {
      const f = await fixture();
      expect(f.hooks.captureContent).toBe(false);
      f.send('one', 'turn.start', { resumed: false, input: 'ignored' });
      f.send('one', 'turn.end', { outcome: 'completed', error: null });
      await f.hooks.flush();
      expect(f.spans()).toEqual([]);
    } finally { (globalThis as Record<symbol, unknown>)[AGENT_RUNS] = current; }
  });
});

describe('fx default private capture', () => {
  it.each(['private', 'unknown'] as const)('does not transport or record content for %s sessions', async audience => {
    const f = await fixture(audience);
    const marker = 'restricted fx content';
    expect(f.hooks.captureContent).toBe(false);
    f.send('one', 'turn.start', { resumed: false, input: marker });
    f.model('one', marker);
    const privateCall = { ...call, argumentsJson: JSON.stringify({ query: marker }) };
    f.send('one', 'tool.start', { call: privateCall, rerun: false }, 1);
    f.send('one', 'tool.end', { call: privateCall, status: 'success', output: JSON.stringify(marker), error: null }, 1);
    f.send('one', 'step.end', {}, 1);
    f.send('one', 'turn.end', { outcome: 'completed', error: null });
    await f.hooks.flush();
    await runs.forceFlush();
    const spans = f.spans();
    expect(spans.filter(span => span.name.startsWith('invoke_agent'))).toHaveLength(1);
    expect(spans.filter(span => span.name === 'execute_tool lookup')).toHaveLength(1);
    expect(JSON.stringify(spans.map(span => ({ attributes: span.attributes, events: span.events, status: span.status })))).not.toContain(marker);
    const names = (await f.world.streams.list(f.runId)).filter(name => name.includes('_agent-runs_'));
    expect(names.length).toBeGreaterThan(0);
    for (const name of names) {
      const lines = await readJsonLines(f.world, f.runId, name);
      expect(lines.length).toBeGreaterThan(0);
      expect(JSON.stringify(lines)).not.toContain(marker);
    }
  });
});

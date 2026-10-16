import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SpanStatusCode } from '@opentelemetry/api';
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FxHookEvent } from 'libfx';
import { agentRunsHooks } from '../src/fx.ts';
import { agentRuns } from '../src/instrumentation/otel.ts';

const exporter = new InMemorySpanExporter();
const runs = agentRuns({ audience: 'public', spanProcessors: [new SimpleSpanProcessor(exporter)] });
runs.register();

const SESSION = 'wrun_01JZFXHOOKS0000000000000000';
const TURN = 'turn_1';
const worlds: LocalWorld[] = [];
afterAll(async () => {
  for (const w of worlds) await w.close?.();
});

/** An event as libfx sends it: the envelope, then the event's own fields. */
const at = (executionId: string, stepIndex: number) => (type: string, data: unknown): FxHookEvent =>
  ({ version: 1, sessionId: SESSION, rootSessionId: SESSION, turnId: TURN, executionId, stepIndex, timestamp: Date.now(), contentCaptured: true, contentTruncated: false, type, data }) as unknown as FxHookEvent;

const call = { callId: 'call_1', name: 'shout', provenance: 'fx_local', argumentsJson: '{"text":"hi"}' };
const user = { role: 'user', content: 'shout hi', toolCallId: null, toolName: null, toolResultStatus: null, imageCount: 0, toolCalls: [] };

describe('Agent Runs from libfx hooks', () => {
  let spans: ReadableSpan[];
  beforeAll(async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'dh-fx-hooks-'));
    // The first execution: a model call asks for `shout`, which starts, waits on an approval, and the turn yields.
    const first = createWorld({ dataDir, recoverActiveRuns: false });
    worlds.push(first);
    const one = agentRunsHooks({ world: first, name: 'fx-agent' });
    const e1 = at('exec_1', 1);
    one.onEvent(at('exec_1', 0)('turn.start', { resumed: false, input: 'shout hi' }));
    one.onEvent(e1('step.start', {}));
    one.onEvent(e1('model.start', { modelCallId: 1, purpose: 'agent', model: 'anthropic/claude-haiku-4.5', messages: [user], tools: [{ name: 'shout', description: 'Uppercase.', inputSchemaJson: '{"type":"object"}' }] }));
    one.onEvent(e1('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'tool-calls', usage: { inputTokens: 10, outputTokens: 3, cacheReadTokens: 4, cacheWriteTokens: 0 }, toolCalls: [call] }));
    one.onEvent(e1('tool.start', { call, rerun: false }));
    one.onEvent(e1('wait.start', { requestId: 7, kind: 'approval', callId: 'call_1', prompt: 'Shout?' }));
    one.onEvent(e1('wait.end', { requestId: 7, outcome: 'accepted', error: null }));
    one.onEvent(e1('step.end', {}));
    one.onEvent(at('exec_1', 0)('turn.end', { outcome: 'yielded', error: null }));
    await one.flush();

    // Another process resumes the turn: the call runs again (a rerun), and a second model call answers.
    const second = createWorld({ dataDir, recoverActiveRuns: false });
    worlds.push(second);
    const two = agentRunsHooks({ world: second, name: 'fx-agent' });
    const e2 = (step: number) => at('exec_2', step);
    two.onEvent(e2(0)('turn.start', { resumed: true }));
    two.onEvent(e2(1)('step.start', {}));
    two.onEvent(e2(1)('tool.start', { call, rerun: true }));
    two.onEvent(e2(1)('tool.end', { call, status: 'success', output: '"HI"', error: null }));
    two.onEvent(e2(1)('step.end', {}));
    two.onEvent(e2(2)('step.start', {}));
    two.onEvent(e2(2)('model.start', { modelCallId: 1, purpose: 'agent', model: 'anthropic/claude-haiku-4.5', messages: [user], tools: [] }));
    two.onEvent(e2(2)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output: 'Shouted HI.', usage: { inputTokens: 20, outputTokens: 4, cacheReadTokens: 0, cacheWriteTokens: 0 } }));
    two.onEvent(e2(2)('step.end', {}));
    two.onEvent(e2(0)('turn.end', { outcome: 'completed', error: null }));
    await two.flush();
    await runs.forceFlush();
    spans = exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === SESSION);
  }, 30_000);

  it('records one turn across the two executions, in the eve.agent scope', () => {
    const turns = spans.filter((s) => s.name === 'invoke_agent fx-agent');
    expect(turns).toHaveLength(1);
    expect(turns[0]!.attributes).toMatchObject({ 'agent.turn.id': TURN, 'agent.turn.outcome': 'completed', 'agent.session.title': 'shout hi' });
    expect(new Set(spans.map((s) => s.spanContext().traceId)).size).toBe(1);
    for (const span of spans) expect(span.instrumentationScope.name).toBe('eve.agent');
  });

  it('records a step per model call, numbered by the execution that ran it', () => {
    const steps = spans.filter((s) => s.name === 'agent.step');
    // The first execution's step, and the second's two (the rerun's and the answer's).
    expect(steps).toHaveLength(3);
    expect(new Set(steps.map((s) => s.spanContext().spanId)).size).toBe(3);
    const models = spans.filter((s) => s.name === 'chat anthropic/claude-haiku-4.5');
    expect(models).toHaveLength(2);
    expect(models.map((m) => m.attributes['gen_ai.usage.input_tokens']).sort()).toEqual([10, 20]);
    expect(String(models.find((m) => m.attributes['gen_ai.usage.input_tokens'] === 10)!.attributes['gen_ai.input.messages'])).toContain('shout hi');
  });

  it("keeps the tool call the first execution started, ended by the second, under the step that asked for it", () => {
    const tools = spans.filter((s) => s.name === 'execute_tool shout');
    expect(tools).toHaveLength(1);
    const tool = tools[0]!;
    expect(tool.attributes).toMatchObject({ 'gen_ai.tool.call.id': 'call_1', 'agent.action.outcome': 'completed' });
    expect(String(tool.attributes['gen_ai.tool.call.result'])).toContain('HI');
    const asker = spans.find((s) => s.name === 'chat anthropic/claude-haiku-4.5' && s.attributes['gen_ai.usage.input_tokens'] === 10)!;
    expect(tool.parentSpanContext?.spanId).toBe(asker.parentSpanContext?.spanId);
    const approval = spans.find((s) => s.name === 'agent.approval')!;
    expect(approval.parentSpanContext?.spanId).toBe(tool.spanContext().spanId);
    expect(approval.attributes['agent.approval.outcome']).toBe('approved');
  });
});

describe('Agent Runs from libfx hooks, turn after turn', () => {
  it("records a turn that starts while the previous one's events are still being recorded", async () => {
    const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-fx-next-')), recoverActiveRuns: false });
    worlds.push(world);
    const hooks = agentRunsHooks({ world, name: 'fx-next' });
    const session = 'wrun_01JZFXHOOKSNEXT00000000000';
    const ev = (turnId: string, stepIndex: number) => (type: string, data: unknown): FxHookEvent =>
      ({ version: 1, sessionId: session, rootSessionId: session, turnId, executionId: `exec_${turnId}`, stepIndex, timestamp: Date.now(), contentCaptured: true, contentTruncated: false, type, data }) as unknown as FxHookEvent;
    const model = (turnId: string) => {
      hooks.onEvent(ev(turnId, 1)('step.start', {}));
      hooks.onEvent(ev(turnId, 1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [user], tools: [] }));
      hooks.onEvent(ev(turnId, 1)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output: 'ok', usage: { inputTokens: 1, outputTokens: 1, cacheReadTokens: 0, cacheWriteTokens: 0 } }));
      hooks.onEvent(ev(turnId, 1)('step.end', {}));
    };
    hooks.onEvent(ev('turn_a', 0)('turn.start', { resumed: false, input: 'first' }));
    model('turn_a');
    hooks.onEvent(ev('turn_a', 0)('turn.end', { outcome: 'completed', error: null }));
    // The next turn starts before the first turn's end is recorded; its other events come after.
    hooks.onEvent(ev('turn_b', 0)('turn.start', { resumed: false, input: 'second' }));
    await hooks.flush();
    model('turn_b');
    hooks.onEvent(ev('turn_b', 0)('turn.end', { outcome: 'completed', error: null }));
    await hooks.flush();
    await runs.forceFlush();

    const own = exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === session);
    const turns = own.filter((s) => s.name === 'invoke_agent fx-next');
    expect(turns.map((t) => [t.attributes['agent.turn.id'], t.attributes['agent.turn.outcome']]).sort()).toEqual([['turn_a', 'completed'], ['turn_b', 'completed']]);
    const b = turns.find((t) => t.attributes['agent.turn.id'] === 'turn_b')!;
    expect(own.filter((s) => s.name === 'agent.step' && s.parentSpanContext?.spanId === b.spanContext().spanId)).toHaveLength(1);
  }, 30_000);
});

describe('Agent Runs from a real libfx durable agent', () => {
  it('records its turn, its model calls and the tool call their reply asked for', async () => {
    const { createFxAgent } = await import('libfx');
    const { world: fxWorld } = await import('libfx/durable-world');
    const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-fx-real-')), recoverActiveRuns: false });
    worlds.push(world);
    const usage = { inputTokens: { total: 12, cacheRead: 2 }, outputTokens: { total: 3 } };
    const sse = (parts: Record<string, unknown>[]) =>
      new Response([...parts.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
    let requests = 0;
    const fetch = async (_url: string | URL | Request, init?: RequestInit) => {
      if ((init?.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
      return requests++ === 0
        ? sse([{ type: 'tool-call', toolCallId: 'call_shout', toolName: 'shout', input: '{"text":"hi"}' }, { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage }])
        : sse([{ type: 'text-delta', id: 't', delta: 'Shouted HI.' }, { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage }]);
    };
    const hooks = agentRunsHooks({ world, name: 'fx-real' });
    const agent = createFxAgent({
      durability: fxWorld(world, { name: 'fx-real', livenessKnown: true, alive: () => true, holderInfo: () => ({ pid: process.pid, host: 'test' }), pollMs: 50 }),
      apiKey: 'test',
      model: 'test/model',
      fetch: fetch as typeof globalThis.fetch,
      modelCatalog: [],
      hooks,
      tools: [{ name: 'shout', description: 'Uppercase some text.', inputSchema: { type: 'object', properties: { text: { type: 'string' } } }, idempotent: true, execute: async (input: { text: string }) => input.text.toUpperCase() }],
    });
    try {
      const turn = agent.session().prompt('shout hi');
      const { sessionId } = await turn.accepted;
      for await (const _ of turn);
      await turn.result;
      await hooks.flush();
      await runs.forceFlush();
      const own = exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === sessionId);
      expect(own.filter((s) => s.name === 'invoke_agent fx-real')).toHaveLength(1);
      expect(own.find((s) => s.name === 'invoke_agent fx-real')!.attributes['agent.turn.outcome']).toBe('completed');
      const models = own.filter((s) => s.name === 'chat test/model');
      expect(models.length).toBeGreaterThanOrEqual(2);
      const tool = own.find((s) => s.name === 'execute_tool shout')!;
      expect(tool.attributes['gen_ai.tool.call.id']).toBe('call_shout');
      expect(String(tool.attributes['gen_ai.tool.call.result'])).toContain('HI');
      // The tool span is under the step whose model call asked for it.
      const asker = models.find((m) => String(m.attributes['gen_ai.output.messages']).includes('call_shout'))!;
      expect(tool.parentSpanContext?.spanId).toBe(asker.parentSpanContext?.spanId);
      expect(new Set(own.map((s) => s.spanContext().traceId)).size).toBe(1);
    } finally {
      await agent.close();
    }
  }, 60_000);
});

/** A session's events from one execution, as libfx sends them. */
const fx = (session: string, executionId: string | null, turnId: string | null = TURN) => (stepIndex: number) => (type: string, data: unknown): FxHookEvent =>
  ({ version: 1, sessionId: session, rootSessionId: session, turnId, executionId, stepIndex, timestamp: Date.now(), contentCaptured: true, contentTruncated: false, type, data }) as unknown as FxHookEvent;
const usage = { inputTokens: 1, outputTokens: 1, cacheReadTokens: 0, cacheWriteTokens: 0 };

/** An execution whose model call asks for `shout`, which libfx cuts off (`tool.end` cancelled) before the turn ends with `end`. */
async function cutOff(hooks: ReturnType<typeof agentRunsHooks>, session: string, end: Record<string, unknown>, fence = false) {
  const ev = fx(session, 'exec_1');
  hooks.onEvent(ev(0)('turn.start', { resumed: false, input: 'shout hi' }));
  hooks.onEvent(ev(1)('step.start', {}));
  hooks.onEvent(ev(1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [user], tools: [] }));
  hooks.onEvent(ev(1)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'tool-calls', usage, toolCalls: [call] }));
  hooks.onEvent(ev(1)('tool.start', { call, rerun: false }));
  // What libfx 0.0.14 sends when another worker's write fences this one's journal out: a receipt naming no turn or execution.
  if (fence) hooks.onEvent(fx(session, null, null)(0)('journal.error', { error: 'FxFencedError', fenced: true }));
  hooks.onEvent(ev(1)('tool.end', { call, status: 'cancelled', error: 'Cancelled' }));
  hooks.onEvent(ev(1)('step.end', {}));
  hooks.onEvent(ev(0)('turn.end', { error: null, ...end }));
  await hooks.flush();
}

/** The execution that continues the turn: it runs the call again, and the turn completes. */
async function continued(hooks: ReturnType<typeof agentRunsHooks>, session: string) {
  const ev = fx(session, 'exec_2');
  hooks.onEvent(ev(0)('turn.start', { resumed: true }));
  hooks.onEvent(ev(1)('step.start', {}));
  hooks.onEvent(ev(1)('tool.start', { call, rerun: true }));
  hooks.onEvent(ev(1)('tool.end', { call, status: 'success', output: '"HI"', error: null }));
  hooks.onEvent(ev(1)('step.end', {}));
  hooks.onEvent(ev(0)('turn.end', { outcome: 'completed', error: null }));
  await hooks.flush();
}

async function spansOf(session: string): Promise<ReadableSpan[]> {
  await runs.forceFlush();
  return exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === session);
}

async function newWorld(dataDir?: string): Promise<LocalWorld> {
  const world = createWorld({ dataDir: dataDir ?? (await mkdtemp(join(tmpdir(), 'dh-fx-end-'))), recoverActiveRuns: false });
  worlds.push(world);
  return world;
}

describe('Agent Runs from libfx hooks, as a turn ends', () => {
  it.each([
    { how: 'hands it to another worker', end: { outcome: 'interrupted', handoff: true }, fence: false, session: 'wrun_01JZFXHOOKSHANDOFF000000000' },
    { how: "is fenced out by another worker's write", end: { outcome: 'interrupted' }, fence: true, session: 'wrun_01JZFXHOOKSFENCED0000000000' },
  ])('keeps the turn open when its execution $how, and the call it cut off runs again as the same call', async ({ end, fence, session }) => {
    const dataDir = await mkdtemp(join(tmpdir(), 'dh-fx-handoff-'));
    await cutOff(agentRunsHooks({ world: await newWorld(dataDir), name: 'fx-end' }), session, end, fence);
    expect((await spansOf(session)).filter((s) => s.name === 'invoke_agent fx-end' || s.name === 'execute_tool shout')).toEqual([]);

    // Another process continues it.
    await continued(agentRunsHooks({ world: await newWorld(dataDir), name: 'fx-end' }), session);
    const spans = await spansOf(session);
    const turns = spans.filter((s) => s.name === 'invoke_agent fx-end');
    expect(turns).toHaveLength(1);
    expect(turns[0]!.attributes['agent.turn.outcome']).toBe('completed');
    const tools = spans.filter((s) => s.name === 'execute_tool shout');
    expect(tools).toHaveLength(1);
    expect(tools[0]!.attributes['agent.action.outcome']).toBe('completed');
    expect(String(tools[0]!.attributes['gen_ai.tool.call.result'])).toContain('HI');
  }, 30_000);

  it.each([
    { outcome: 'interrupted', recorded: 'cancelled', session: 'wrun_01JZFXHOOKSINTERRUPTED00000' },
    { outcome: 'cancelled', recorded: 'cancelled', session: 'wrun_01JZFXHOOKSCANCELLED000000' },
  ])("ends a turn the user cancelled ($outcome, no handoff) as $recorded, with the call it cut off", async ({ outcome, recorded, session }) => {
    await cutOff(agentRunsHooks({ world: await newWorld(), name: 'fx-end' }), session, { outcome });
    const spans = await spansOf(session);
    const turns = spans.filter((s) => s.name === 'invoke_agent fx-end');
    expect(turns).toHaveLength(1);
    expect(turns[0]!.attributes['agent.turn.outcome']).toBe(recorded);
    const tool = spans.find((s) => s.name === 'execute_tool shout')!;
    expect(tool.attributes['agent.action.outcome']).toBe('cancelled');
  }, 30_000);

  it('ends a failed turn as failed, with its error', async () => {
    const session = 'wrun_01JZFXHOOKSFAILED0000000000';
    await cutOff(agentRunsHooks({ world: await newWorld(), name: 'fx-end' }), session, { outcome: 'failed', error: 'the model refused' });
    const turn = (await spansOf(session)).find((s) => s.name === 'invoke_agent fx-end')!;
    expect(turn.attributes['agent.turn.outcome']).toBe('failed');
    expect(turn.status.code).toBe(SpanStatusCode.ERROR);
  }, 30_000);

  it('ignores the end of an execution this process ran before a takeover', async () => {
    const session = 'wrun_01JZFXHOOKSTAKEOVER00000000';
    const hooks = agentRunsHooks({ world: await newWorld(), name: 'fx-end' });
    const old = fx(session, 'exec_old');
    const now = fx(session, 'exec_new');
    hooks.onEvent(old(0)('turn.start', { resumed: false, input: 'shout hi' }));
    hooks.onEvent(old(1)('step.start', {}));
    hooks.onEvent(old(1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [user], tools: [] }));
    // Another delivery takes the session over in this process while the first execution stops.
    hooks.onEvent(now(0)('turn.start', { resumed: true }));
    hooks.onEvent(now(1)('step.start', {}));
    hooks.onEvent(old(1)('model.end', { modelCallId: 1, error: null, cancelled: true, usage }));
    hooks.onEvent(old(1)('step.end', {}));
    hooks.onEvent(old(0)('turn.end', { outcome: 'interrupted', error: null }));
    hooks.onEvent(now(1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [user], tools: [] }));
    hooks.onEvent(now(1)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output: 'HI', usage }));
    hooks.onEvent(now(1)('step.end', {}));
    hooks.onEvent(now(0)('turn.end', { outcome: 'completed', error: null }));
    await hooks.flush();
    const spans = await spansOf(session);
    const turns = spans.filter((s) => s.name === 'invoke_agent fx-end');
    expect(turns.map((t) => t.attributes['agent.turn.outcome'])).toEqual(['completed']);
    const answered = spans.filter((s) => s.name === 'chat test/model' && String(s.attributes['gen_ai.output.messages']).includes('HI'));
    expect(answered).toHaveLength(1);
    // Each execution's step, the answer in the one the new execution started.
    const steps = spans.filter((s) => s.name === 'agent.step');
    expect(steps).toHaveLength(2);
    expect(steps.map((s) => s.spanContext().spanId)).toContain(answered[0]!.parentSpanContext?.spanId);
  }, 30_000);
});

describe('Agent Runs from libfx hooks, between turns', () => {
  it('numbers its turns, keeps no session between them, and makes none for receipts or calls that outlive a turn', async () => {
    const session = 'wrun_01JZFXHOOKSBETWEEN000000000';
    const hook = (globalThis as Record<symbol, { session: (input: unknown) => unknown }>)[Symbol.for('durable-harness.agent-runs')]!;
    const session_ = hook.session;
    let made = 0;
    hook.session = (input) => (made++, session_(input));
    try {
      const hooks = agentRunsHooks({ world: await newWorld(), name: 'fx-between' });
      const turn = (turnId: string) => {
        const ev = fx(session, `exec_${turnId}`, turnId);
        hooks.onEvent(ev(0)('turn.start', { resumed: false, input: turnId }));
        hooks.onEvent(ev(1)('step.start', {}));
        hooks.onEvent(ev(1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [user], tools: [] }));
        hooks.onEvent(ev(1)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output: 'ok', usage }));
        hooks.onEvent(ev(1)('step.end', {}));
        hooks.onEvent(ev(0)('turn.end', { outcome: 'completed', error: null }));
      };
      turn('turn_a');
      await hooks.flush();
      // After the turn: its journal receipt, and a title call that outlived it.
      hooks.onEvent(fx(session, null, null)(0)('journal.commit', { cursor: 'c1', firstSeq: 1, lastSeq: 3, eventCount: 3 }));
      const late = fx(session, 'exec_turn_a', 'turn_a');
      hooks.onEvent(late(0)('model.start', { modelCallId: 2, purpose: 'title', model: 'test/model', messages: [user], tools: [] }));
      hooks.onEvent(late(0)('model.end', { modelCallId: 2, error: null, cancelled: false, finishReason: 'stop', output: 'a title', usage }));
      await hooks.flush();
      expect(made).toBe(1);
      turn('turn_b');
      await hooks.flush();
      expect(made).toBe(2);
      const spans = await spansOf(session);
      const turns = spans.filter((s) => s.name === 'invoke_agent fx-between');
      expect(turns.map((t) => [t.attributes['agent.turn.id'], t.attributes['agent.turn.outcome'], t.attributes['agent.turn.sequence']])).toEqual([
        ['turn_a', 'completed', 0],
        ['turn_b', 'completed', 1],
      ]);
      expect(spans.filter((s) => s.name === 'chat test/model')).toHaveLength(2);
    } finally {
      hook.session = session_;
    }
  }, 30_000);
});

describe('Agent Runs from libfx hooks, by audience', () => {
  it('records no content for a private audience, and asks libfx for none', async () => {
    const session = 'wrun_01JZFXHOOKSPRIVATE000000000';
    const marker = 'private-marker-7d1c';
    const hooks = agentRunsHooks({ world: await newWorld(), name: 'fx-private', audience: 'private' });
    expect(hooks.captureContent).toBe(false);
    expect(agentRunsHooks({ world: await newWorld(), audience: 'public' }).captureContent).toBe(true);
    const secret = { ...call, argumentsJson: JSON.stringify({ text: marker }) };
    const ev = fx(session, 'exec_1');
    hooks.onEvent(ev(0)('turn.start', { resumed: false, input: marker }));
    hooks.onEvent(ev(1)('step.start', {}));
    hooks.onEvent(ev(1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', instructions: [{ ...user, role: 'system', content: marker }], messages: [{ ...user, content: marker }], tools: [{ name: 'shout', description: marker, inputSchemaJson: '{}' }] }));
    hooks.onEvent(ev(1)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'tool-calls', output: marker, usage, toolCalls: [secret] }));
    hooks.onEvent(ev(1)('tool.start', { call: secret, rerun: false }));
    hooks.onEvent(ev(1)('tool.end', { call: secret, status: 'success', output: JSON.stringify(marker), error: null }));
    hooks.onEvent(ev(1)('step.end', {}));
    hooks.onEvent(ev(0)('turn.end', { outcome: 'completed', error: null }));
    await hooks.flush();
    const spans = await spansOf(session);
    expect(spans.map((s) => s.name).sort()).toEqual(['agent.step', 'chat test/model', 'execute_tool shout', 'invoke_agent fx-private']);
    expect(spans.find((s) => s.name === 'invoke_agent fx-private')!.attributes['agent.channel.audience']).toBe('private');
    for (const span of spans) expect(JSON.stringify([span.name, span.attributes, span.events, span.status])).not.toContain(marker);
  }, 30_000);

  it('records nothing until agentRuns() is registered, and asks libfx for content meanwhile', async () => {
    const session = 'wrun_01JZFXHOOKSUNREGISTERED0000';
    const key = Symbol.for('durable-harness.agent-runs');
    const global = globalThis as Record<symbol, unknown>;
    const hook = global[key];
    delete global[key];
    try {
      const hooks = agentRunsHooks({ world: await newWorld(), name: 'fx-off' });
      expect(hooks.captureContent).toBe(true);
      const ev = fx(session, 'exec_1');
      hooks.onEvent(ev(0)('turn.start', { resumed: false, input: 'hi' }));
      hooks.onEvent(ev(1)('step.start', {}));
      hooks.onEvent(ev(1)('model.start', { modelCallId: 1, purpose: 'agent', model: 'test/model', messages: [user], tools: [] }));
      global[key] = hook;
      // Registered mid-turn: the turn it missed the start of stays unrecorded.
      hooks.onEvent(ev(1)('model.end', { modelCallId: 1, error: null, cancelled: false, finishReason: 'stop', output: 'ok', usage }));
      hooks.onEvent(ev(1)('step.end', {}));
      hooks.onEvent(ev(0)('turn.end', { outcome: 'completed', error: null }));
      await hooks.flush();
      expect(await spansOf(session)).toEqual([]);
    } finally {
      global[key] = hook;
    }
  }, 30_000);
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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

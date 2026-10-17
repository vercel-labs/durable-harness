import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import type { DriveContext, EffectRecord, Harness, SessionState, Submission } from 'harness-world';
import { agentRunsHook } from 'harness-world/agent-runs';
import { Parked, Yield } from 'harness-world/errors';
import { readJsonLines } from 'harness-world/streams';
import { gatewayModelOptions } from '../../harness-process/src/agent-runs.ts';
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest';
import { checkpointStreamName } from '../src/instrumentation/checkpoints.ts';
import { agentRuns } from '../src/instrumentation/otel.ts';

const exporter = new InMemorySpanExporter();
const runs = agentRuns({ spanProcessors: [new SimpleSpanProcessor(exporter)], onError: () => {} });
runs.register();
const hook = agentRunsHook()!;

const worlds: LocalWorld[] = [];
afterEach(() => { vi.useRealTimers(); });
afterAll(async () => {
  for (const w of worlds) await w.close?.();
});

async function localWorld(): Promise<LocalWorld> {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-agent-runs-recovery-')), recoverActiveRuns: false });
  worlds.push(world);
  return world;
}

/** The same World as another process sees it: its own Agent Runs state. With `write`, its stream writes go there. */
function processOf(world: World, write?: World['streams']['write'], overrides: Partial<World['streams']> = {}): World {
  const bound = (target: object, key: string | symbol) => {
    const value = Reflect.get(target, key);
    return typeof value === 'function' ? value.bind(target) : value;
  };
  const streams = new Proxy(world.streams, { get: (target, key) => key in overrides ? Reflect.get(overrides, key) : key === 'write' && write ? write : bound(target, key) });
  return new Proxy(world, { get: (target, key) => (key === 'streams' ? streams : bound(target, key)) });
}

/** A drive of one prompt's session, its effects run as they come (the kernel's log is not under test). */
function driveContext(world: World, runId: string): DriveContext {
  const prompt: Submission = { id: 'sub_1', kind: 'prompt', text: 'hello', receivedAt: new Date() };
  const state = { status: 'running', submissions: [prompt], placed: new Set([prompt.id]), effects: new Map(), effectOrder: [] } as unknown as SessionState;
  return {
    runId,
    world,
    config: { traceAudience: 'public' },
    signal: new AbortController().signal,
    state: () => state,
    pending: () => [],
    async effect(key: string, effect: { name: string; placed?: string[] }, fn: (io: never) => Promise<unknown>) {
      const record = { key, name: effect.name, placed: effect.placed ?? [], status: 'running', attempt: 1 } as unknown as EffectRecord;
      state.effects.set(key, record);
      state.effectOrder.push(record);
      return fn({} as never);
    },
  } as unknown as DriveContext;
}

const harness = (drive: (ctx: DriveContext) => Promise<void>): Harness => ({ name: 'recovery', drive }) as Harness;
const of = (runId: string) => exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === runId);

describe('Agent Runs across drives and processes', () => {
  it('checkpoints a model cut by the soft deadline before another process continues its turn', async () => {
    const world = await localWorld();
    const runId = 'wrun_soft_deadline';
    const abort = new AbortController();
    const ctx = driveContext(world, runId);
    const deadline = Date.now() + 5000;
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      await expect(hook.drive(harness(async ctx => {
        await ctx.effect('model:cut', { name: 'model', replay: 'safe', placed: ['sub_1'], input: { model: 'test/model' } }, async () => {
          await runs.forceFlush();
          vi.setSystemTime(deadline + 1);
          const reason = new Yield('budget');
          abort.abort(reason);
          throw reason;
        });
      }), { ...ctx, deadline, signal: abort.signal })).rejects.toBeInstanceOf(Yield);
    } finally { vi.useRealTimers(); }
    await runs.forceFlush();
    const cut = of(runId);
    expect(cut.filter(span => span.name === 'agent.step')).toHaveLength(1);
    expect(cut.filter(span => span.attributes['gen_ai.operation.name'] === 'chat')).toHaveLength(1);
    expect(cut.every(span => span.status.code === 2)).toBe(true);

    const continued = driveContext(processOf(world), runId);
    continued.state().effectOrder.push(...ctx.state().effectOrder);
    await hook.drive(harness(async ctx => {
      await ctx.effect('model:next', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, async () => 'continued answer');
    }), continued);
    await runs.forceFlush();
    const spans = of(runId);
    expect(spans.filter(span => span.name === 'invoke_agent recovery')).toHaveLength(1);
    expect(spans.filter(span => span.name === 'agent.step')).toHaveLength(2);
    expect(spans.filter(span => span.attributes['gen_ai.operation.name'] === 'chat')).toHaveLength(2);
    expect(new Set(spans.map(span => span.spanContext().spanId)).size).toBe(spans.length);
  });

  it('stores the finished marker after a slow write instead of exporting the completed turn again', async () => {
    const original = await localWorld();
    const runId = 'wrun_slow_finished';
    let release!: () => void;
    const delayed = new Promise<void>(resolve => { release = resolve; });
    let writes = 0;
    const world = processOf(original, async (...args) => {
      if (++writes === 1) await delayed;
      await original.streams.write(...args);
    });
    try {
      await hook.drive(harness(async ctx => {
        await ctx.effect('model:0', { name: 'model', replay: 'safe', placed: ['sub_1'], input: { model: 'test/model' } }, async () => 'finished answer');
      }), driveContext(world, runId));
    } finally { release(); }
    await runs.forceFlush();
    const lines = await readJsonLines(original, runId, checkpointStreamName(runId, 'sub_1'));
    expect(lines.at(-1)).toMatchObject({ v: null });
    await hook.drive(harness(async () => {}), driveContext(processOf(original), runId));
    await runs.forceFlush();
    const spans = of(runId);
    expect(spans.filter(span => span.name === 'invoke_agent recovery')).toHaveLength(1);
    expect(new Set(spans.map(span => span.spanContext().spanId)).size).toBe(spans.length);
  });

  it('records public tool definitions without persisting provider credentials', async () => {
    const world = await localWorld();
    const runId = 'wrun_tool_definitions';
    const session = hook.session({ world, runId, agentName: 'public-tools', audience: 'public' });
    const turn = (await session.turn({ turnId: 'turn', sequence: 0 }))!;
    const step = await session.step(turn, 'turn');
    const options = gatewayModelOptions({ path: '/v1/responses', body: {
      model: 'test/model', input: 'Look up the record',
      tools: [{ type: 'mcp', name: 'remote_lookup', authorization: 'test-only-credential', headers: { 'x-api-key': 'test-only-credential' }, parameters: { type: 'object' } }],
    } });
    const model = await step.modelCall(options);
    await session.flush();
    const lines = await readJsonLines(world, runId, checkpointStreamName(runId, 'turn'));
    expect(JSON.stringify(lines)).toContain('remote_lookup');
    expect(JSON.stringify(lines)).not.toContain('test-only-credential');
    await model.complete({ outcome: 'completed', result: { usage: {}, finishReason: 'stop', content: [{ type: 'text', text: 'done' }] } });
    await step.complete();
    await turn.complete({ outcome: 'completed' });
    await session.flush();
    await runs.forceFlush();
    const recorded = of(runId).find(span => span.attributes['gen_ai.operation.name'] === 'chat')!;
    expect(recorded.attributes['gen_ai.tool.definitions']).toContain('remote_lookup');
    expect(JSON.stringify(recorded.attributes)).not.toContain('test-only-credential');
  });

  it('runs untraced without overwriting a checkpoint when its read fails', async () => {
    const original = await localWorld();
    let writes = 0;
    const world = processOf(original, async () => { writes++; }, { getInfo: async () => { throw new Error('checkpoint unavailable'); } });
    let ran = false;
    await hook.drive(harness(async ctx => {
      expect(await ctx.effect('model:0', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, async () => { ran = true; return 'model reply'; })).toBe('model reply');
    }), driveContext(world, 'wrun_read_failed'));
    expect(ran).toBe(true);
    expect(writes).toBe(0);
  });

  it('lets a drive finish when its checkpoint read never answers', async () => {
    const world = processOf(await localWorld(), undefined, { getInfo: () => new Promise(() => {}) });
    let ran = false;
    const ctx = { ...driveContext(world, 'wrun_read_stuck'), deadline: Date.now() + 10 };
    await hook.drive(harness(async ctx => {
      await ctx.effect('model:0', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, async () => { ran = true; return 'model reply'; });
    }), ctx);
    expect(ran).toBe(true);
  }, 1000);

  it('continues the placed turn before switching to a queued follow-up', async () => {
    const world = await localWorld();
    const runId = 'wrun_followup';
    const session = hook.session({ world, runId, agentName: 'recovery', audience: 'public' });
    await session.turn({ turnId: 'sub_1', sequence: 0, title: 'hello' });
    await session.flush();
    const ctx = driveContext(processOf(world), runId);
    const followUp: Submission = { id: 'sub_2', kind: 'followUp', text: 'next prompt', receivedAt: new Date() };
    ctx.state().submissions.push(followUp);
    await hook.drive(harness(async ctx => {
      await ctx.effect('model:continued', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, async () => 'continued first answer');
      await ctx.effect('model:next', { name: 'model', replay: 'safe', input: { model: 'test/model' }, placed: ['sub_2'] }, async () => 'follow-up answer');
    }), { ...ctx, pending: () => [followUp] });
    await runs.forceFlush();
    const turns = of(runId).filter(span => span.name === 'invoke_agent recovery');
    expect(turns).toHaveLength(2);
    const first = turns.find(span => span.attributes['agent.turn.id'] === 'sub_1')!;
    const second = turns.find(span => span.attributes['agent.turn.id'] === 'sub_2')!;
    expect(first.attributes['gen_ai.output.messages']).toContain('continued first answer');
    expect(second.attributes['gen_ai.output.messages']).toContain('follow-up answer');
    expect(first.attributes['agent.turn.outcome']).toBe('completed');
    expect(second.attributes['agent.turn.outcome']).toBe('completed');
  });

  it.each([
    { audience: 'private' as const, freshProcess: false },
    { audience: 'private' as const, freshProcess: true },
    { audience: 'unknown' as const, freshProcess: true },
  ])('narrows a resumed public turn and its children for $audience capture (fresh process: $freshProcess)', async ({ audience, freshProcess }) => {
    const world = await localWorld();
    const runId = `wrun_capture_${audience}_${freshProcess}`;
    const initial = hook.session({ world, runId, agentName: 'pi', audience: 'public' });
    const turn = (await initial.turn({ turnId: 't1', sequence: 0, title: 'initial public title' }))!;
    const step = await initial.step(turn, 't1');
    await step.tool({ callId: 'call_1', name: 'lookup', arguments: { q: 'initial public argument' } });
    await initial.flush();

    // Pi recovery resumes before it records fresh generations or settles an earlier tool.
    const resumedSession = hook.session({ world: freshProcess ? processOf(world) : world, runId, agentName: 'pi', audience });
    const resumed = (await resumedSession.resume('t1'))!;
    const tool = resumed.findTool('call_1')!;
    const metadata = { emit: true, recordInputs: false, recordOutputs: false };
    expect(resumed.reference).toEqual(turn.reference);
    expect(resumed.capture).toEqual(metadata);
    expect(tool.capture).toEqual(metadata);

    const markers = ['restricted model input', 'restricted model output', 'restricted tool result', 'restricted new tool input', 'restricted new tool result'];
    await tool.complete({ outcome: 'completed', output: markers[2] });
    const next = await resumedSession.step(resumed, 't1');
    const model = await next.modelCall({ provider: 'test', modelId: 'model', messages: [{ role: 'user', content: markers[0] }] });
    expect(model.capture).toEqual(metadata);
    await model.complete({ outcome: 'completed', result: { usage: { inputTokens: 2, outputTokens: 3 }, finishReason: 'stop', content: [{ type: 'text', text: markers[1]! }] } });
    const nextTool = await next.tool({ callId: 'call_2', name: 'lookup', arguments: { q: markers[3] } });
    await nextTool.complete({ outcome: 'completed', output: markers[4] });
    await resumedSession.flush();

    const lines = await readJsonLines(world, runId, checkpointStreamName(runId, 't1'));
    const snapshot = (lines.filter((line) => line && typeof line === 'object' && 'v' in line).at(-1) as { v: unknown }).v;
    expect(snapshot).toMatchObject({ capture: metadata });
    const stored = JSON.stringify(snapshot);
    for (const marker of [...markers, 'initial public title', 'initial public argument']) expect(stored).not.toContain(marker);

    await next.complete();
    await resumed.complete({ outcome: 'completed' });
    await resumedSession.flush();
    await runs.forceFlush();
    const spans = of(runId);
    expect(spans.filter((span) => span.attributes['gen_ai.operation.name'] === 'chat')).toHaveLength(1);
    expect(spans.filter((span) => span.attributes['gen_ai.operation.name'] === 'execute_tool')).toHaveLength(2);
    for (const marker of markers) expect(JSON.stringify(spans.map((span) => ({ attributes: span.attributes, events: span.events, status: span.status })))).not.toContain(marker);
    const root = spans.find((span) => span.name === 'invoke_agent pi')!;
    expect(root.attributes).toMatchObject({ 'agent.channel.audience': audience, 'agent.trace.content.input': false, 'agent.trace.content.output': false });
  });

  it('does not start a turn when resume finds no checkpoint', async () => {
    const world = await localWorld();
    const runId = 'wrun_resume_missing';
    const session = hook.session({ world, runId, agentName: 'pi', audience: 'private' });
    expect(await session.resume('missing')).toBeUndefined();
    await session.flush();
    await runs.forceFlush();
    expect(of(runId)).toEqual([]);
  });

  it('narrows restored tools settled from memoized kernel effects', async () => {
    const world = await localWorld();
    const runId = 'wrun_memoized_capture';
    const initial = hook.session({ world, runId, agentName: 'recovery', audience: 'public' });
    const turn = (await initial.turn({ turnId: 'sub_1', sequence: 0, title: 'hello' }))!;
    await (await initial.step(turn, 'sub_1')).tool({ callId: 'call_1', name: 'lookup', arguments: {} });
    await initial.flush();

    const ctx = driveContext(processOf(world), runId);
    const marker = 'restricted recovered tool result';
    const resumed = { ...ctx, config: { traceAudience: 'private' }, effect: async () => marker } as unknown as DriveContext;
    await hook.drive(harness(async (ctx) => {
      expect(await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: {} }, async () => { throw new Error('memoized work must not run'); })).toBe(marker);
    }), resumed);
    await runs.forceFlush();
    const spans = of(runId);
    const tool = spans.find((span) => span.name === 'execute_tool lookup')!;
    expect(tool).toBeDefined();
    expect(tool.attributes['gen_ai.tool.call.result']).toBeUndefined();
    expect(JSON.stringify(spans.map((span) => ({ attributes: span.attributes, events: span.events, status: span.status })))).not.toContain(marker);
    expect(spans.find((span) => span.name === 'invoke_agent recovery')!.attributes).toMatchObject({ 'agent.trace.content.input': false, 'agent.trace.content.output': false });
  });

  it('keeps a parked tool call open for the drive that settles it', async () => {
    const world = await localWorld();
    const runId = 'wrun_parked';
    const model = (ctx: DriveContext, key: string) => ctx.effect(key, { name: 'model', replay: 'safe', input: { model: 'test/model' }, placed: ['sub_1'] }, async () => 'ok');
    const first = harness(async (ctx) => {
      await model(ctx, 'model:0');
      await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: { q: 'x' } }, async () => {
        throw new Parked();
      });
    });
    await expect(hook.drive(first, driveContext(world, runId))).rejects.toBeInstanceOf(Parked);
    const second = harness(async (ctx) => {
      await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: { q: 'x' } }, async () => 'found');
      await model(ctx, 'model:1');
    });
    await hook.drive(second, driveContext(world, runId));
    await runs.forceFlush();

    const spans = of(runId);
    const tools = spans.filter((s) => s.name === 'execute_tool lookup');
    expect(tools).toHaveLength(1);
    expect(tools[0]!.status.code).not.toBe(2);
    expect(tools[0]!.attributes['gen_ai.tool.call.result']).toContain('found');
    // Under the step whose model call asked for it, in the first drive.
    const firstStep = spans.find((s) => s.name === 'agent.step' && s.spanContext().spanId === tools[0]!.parentSpanContext?.spanId);
    expect(firstStep?.attributes['agent.step.index']).toBe(0);
    expect(spans.find((s) => s.name === 'invoke_agent recovery')?.attributes['agent.turn.outcome']).toBe('completed');
  }, 30_000);

  it('runs a tool call untraced when its step cannot be recorded', async () => {
    const world = processOf(await localWorld(), async () => {
      throw new Error('stream store unavailable');
    });
    let ran = false;
    const drive = harness(async (ctx) => {
      const value = await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: {} }, async () => ((ran = true), 'found'));
      expect(value).toBe('found');
    });
    await hook.drive(drive, driveContext(world, 'wrun_store_down'));
    expect(ran).toBe(true);
  }, 30_000);

  it("continues a turn from another process's checkpoint when the session comes back", async () => {
    const world = await localWorld();
    const a = processOf(world);
    const b = processOf(world);
    const runId = 'wrun_handoff';
    const session = (w: World) => hook.session({ world: w, runId, agentName: 'pi', audience: 'public' });

    const onA = session(a);
    const turn = (await onA.turn({ turnId: 't1', sequence: 0, title: 'hello' }))!;
    await (await (await onA.step(turn, 't1')).modelCall({ provider: 'test', modelId: 'model' })).complete({ outcome: 'completed' });
    await onA.flush();

    const onB = session(b);
    const resumed = (await onB.resume('t1'))!;
    await (await onB.step(resumed, 't1')).tool({ callId: 'call_b', name: 'lookup' });
    await onB.flush();

    // The same session object, asked for the turn again (libfx's next execution on a warm worker).
    const again = (await onA.turn({ turnId: 't1', sequence: 0 }))!;
    expect(again.findTool('call_b')).toBeDefined();

    // A new acquisition on the warm worker: its steps are numbered after B's.
    const back = session(a);
    const latest = (await back.resume('t1'))!;
    expect(latest.findTool('call_b')).toBeDefined();
    await (await back.step(latest, 't1')).modelCall({ provider: 'test', modelId: 'model' }).then((m) => m.complete({ outcome: 'completed' }));
    await latest.findTool('call_b')!.complete({ outcome: 'completed' });
    await latest.complete({ outcome: 'completed' });
    await back.flush();
    await runs.forceFlush();

    const steps = of(runId).filter((s) => s.name === 'agent.step');
    expect(steps).toHaveLength(3);
    expect(new Set(steps.map((s) => s.spanContext().spanId)).size).toBe(3);
    expect(new Set(steps.map((s) => s.attributes['agent.step.attempt'])).size).toBe(3);
  }, 30_000);
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import type { DriveContext, EffectRecord, Harness, SessionState, Submission } from 'harness-world';
import { agentRunsHook } from 'harness-world/agent-runs';
import { Parked } from 'harness-world/errors';
import { readJsonLines } from 'harness-world/streams';
import { afterAll, describe, expect, it } from 'vitest';
import { checkpointStreamName, TRACE_IO_MS } from '../src/instrumentation/checkpoints.ts';
import { agentRuns } from '../src/instrumentation/otel.ts';

const exporter = new InMemorySpanExporter();
const runs = agentRuns({ spanProcessors: [new SimpleSpanProcessor(exporter)], onError: () => {} });
runs.register();
const hook = agentRunsHook()!;

const worlds: LocalWorld[] = [];
afterAll(async () => {
  for (const w of worlds) await w.close?.();
});

async function localWorld(): Promise<LocalWorld> {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-agent-runs-recovery-')), recoverActiveRuns: false });
  worlds.push(world);
  return world;
}

/** The same World as another process sees it: its own Agent Runs state. With `write` or `getChunks`, its stream writes or reads go there. */
function processOf(world: World, write?: World['streams']['write'], getChunks?: World['streams']['getChunks']): World {
  const bound = (target: object, key: string | symbol) => {
    const value = Reflect.get(target, key);
    return typeof value === 'function' ? value.bind(target) : value;
  };
  const streams = new Proxy(world.streams, { get: (target, key) => (key === 'write' && write ? write : key === 'getChunks' && getChunks ? getChunks : bound(target, key)) });
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

  it('continues a turn under its own span when a follow-up waits behind it', async () => {
    const world = await localWorld();
    const runId = 'wrun_followup';
    const first = harness(async (ctx) => {
      await ctx.effect('model:0', { name: 'model', replay: 'safe', input: { model: 'test/model' }, placed: ['sub_1'] }, async () => 'ok');
      await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: { q: 'x' } }, async () => {
        throw new Parked();
      });
    });
    const ctx = driveContext(world, runId);
    await expect(hook.drive(first, ctx)).rejects.toBeInstanceOf(Parked);

    // The next drive, on the same log, goes on with sub_1's turn; sub_2 waits until its own model call places it.
    const state = ctx.state();
    const followUp: Submission = { id: 'sub_2', kind: 'prompt', text: 'and then?', receivedAt: new Date() };
    state.submissions.push(followUp);
    const placing = ctx.effect.bind(ctx);
    const next = {
      ...ctx,
      pending: () => (state.placed.has(followUp.id) ? [] : [followUp]),
      effect: (key: string, effect: { name: string; placed?: string[] }, fn: (io: never) => Promise<unknown>) => {
        for (const id of effect.placed ?? []) state.placed.add(id);
        return placing(key, effect as never, fn as never);
      },
    } as unknown as DriveContext;
    await hook.drive(harness(async (ctx) => {
      await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: { q: 'x' } }, async () => 'found');
      await ctx.effect('model:1', { name: 'model', replay: 'safe', input: { model: 'test/model' } }, async () => 'done');
      await ctx.effect('model:2', { name: 'model', replay: 'safe', input: { model: 'test/model' }, placed: ['sub_2'] }, async () => 'next');
    }), next);
    await runs.forceFlush();

    const spans = of(runId);
    const turns = spans.filter((s) => s.name === 'invoke_agent recovery');
    expect(turns.map((s) => s.attributes['agent.turn.id']).sort()).toEqual(['sub_1', 'sub_2']);
    const turnOf = (span: (typeof spans)[number]): string | undefined => {
      const parent = spans.find((s) => s.spanContext().spanId === span.parentSpanContext?.spanId);
      return !parent ? undefined : parent.name === 'invoke_agent recovery' ? (parent.attributes['agent.turn.id'] as string) : turnOf(parent);
    };
    const tools = spans.filter((s) => s.name === 'execute_tool lookup');
    expect(tools).toHaveLength(1);
    expect(turnOf(tools[0]!)).toBe('sub_1');
    expect(spans.filter((s) => s.name === 'agent.step').map(turnOf).sort()).toEqual(['sub_1', 'sub_1', 'sub_2']);
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

  it('goes untraced in a drive whose checkpoint read failed, and runs its tool', async () => {
    const runId = 'wrun_read_down';
    const world = processOf(await localWorld(), undefined, async () => {
      throw Object.assign(new Error('the stream store is down'), { status: 503 });
    });
    let ran = false;
    await hook.drive(harness(async (ctx) => {
      await ctx.effect('model:0', { name: 'model', replay: 'safe', input: { model: 'test/model' }, placed: ['sub_1'] }, async () => 'ok');
      expect(await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: {} }, async () => ((ran = true), 'found'))).toBe('found');
    }), driveContext(world, runId));
    await runs.forceFlush();
    expect(ran).toBe(true);
    // Not a fresh tree under the turn's span ids, which would orphan what another drive recorded.
    expect(of(runId)).toEqual([]);
  }, 30_000);

  it('ends a drive whose checkpoint writes never answer, within the bound', async () => {
    const world = processOf(await localWorld(), () => new Promise<never>(() => {}));
    const started = Date.now();
    await hook.drive(harness(async (ctx) => {
      await ctx.effect('tool:call_1', { name: 'tool.lookup', replay: 'safe', input: {} }, async () => 'found');
    }), driveContext(world, 'wrun_write_hangs'));
    expect(Date.now() - started).toBeLessThan(3 * TRACE_IO_MS);
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

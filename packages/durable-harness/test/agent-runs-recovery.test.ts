import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import type { DriveContext, EffectRecord, Harness, SessionState, Submission } from 'harness-world';
import { agentRunsHook } from 'harness-world/agent-runs';
import { Parked } from 'harness-world/errors';
import { afterAll, describe, expect, it } from 'vitest';
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

/** The same World as another process sees it: its own Agent Runs state. With `write`, its stream writes go there. */
function processOf(world: World, write?: World['streams']['write']): World {
  const bound = (target: object, key: string | symbol) => {
    const value = Reflect.get(target, key);
    return typeof value === 'function' ? value.bind(target) : value;
  };
  const streams = new Proxy(world.streams, { get: (target, key) => (key === 'write' && write ? write : bound(target, key)) });
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

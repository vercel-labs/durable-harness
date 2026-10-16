import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { type DriveContext, durableSessions, type Harness, QUEUE_PREFIX, type Submission, Yield } from 'harness-world';
import { readJsonLines } from 'harness-world/streams';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { checkpointStreamName } from '../src/instrumentation/checkpoints.ts';
import { agentRuns } from '../src/instrumentation/otel.ts';

const exporter = new InMemorySpanExporter();
const runs = agentRuns({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
runs.register();

/**
 * Per prompt: a model call that asks for `lookup`, the tool call, and a model
 * call that answers. With `yieldAfterTool`, its first drive stops (out of
 * time) after the tool call, so the turn goes on in a second drive.
 */
function scripted(opts: { yieldAfterTool?: boolean } = {}): Harness & { drives: number } {
  const harness = {
    name: 'scripted',
    drives: 0,
    async drive(ctx: DriveContext) {
      harness.drives++;
      const prompts = ctx.state().submissions.filter((s): s is Submission & { text: string } => s.text !== undefined && s.kind !== 'steer');
      for (const s of prompts) {
        await ctx.effect(`model:${s.id}:0`, { name: 'model', replay: 'safe', input: { model: 'test/model' }, placed: [s.id] }, async () => ({ finishReason: 'tool-calls' }));
        await ctx.effect(`tool:call_${s.id}`, { name: 'tool.lookup', replay: 'safe', input: { q: s.text } }, async () => `found ${s.text}`);
        if (opts.yieldAfterTool && harness.drives === 1) throw new Yield('budget');
        await ctx.effect(`model:${s.id}:1`, { name: 'model', replay: 'safe', input: { model: 'test/model' } }, async () => `answer to ${s.text}`);
      }
    },
  };
  return harness;
}

const worlds: LocalWorld[] = [];
afterAll(async () => {
  for (const w of worlds) await w.close?.();
});

async function session(harness: Harness, config: Record<string, unknown> = {}) {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-agent-runs-')), recoverActiveRuns: false });
  worlds.push(world);
  const sessions = durableSessions({ world, harness });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  return { world, sessions, config };
}

async function idle(sessions: ReturnType<typeof durableSessions>, id: string, models: number) {
  for (let i = 0; i < 300; i++) {
    const state = await sessions.inspect(id);
    if (state.effectOrder.filter((e) => e.name === 'model' && e.status === 'completed').length >= models) return state;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('timed out');
}

const of = (runId: string) => exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === runId);
const parentOf = (span: ReadableSpan) => span.parentSpanContext?.spanId;

describe('Agent Runs with @vercel/agent-runs', () => {
  let first: { runId: string; spans: ReadableSpan[] };
  beforeAll(async () => {
    const harness = scripted({ yieldAfterTool: true });
    const { sessions, world } = await session(harness);
    const { sessionId } = await sessions.start({ prompt: 'hello', config: { traceAudience: 'public' } });
    await idle(sessions, sessionId, 2);
    await sessions.send(sessionId, 'again');
    await idle(sessions, sessionId, 4);
    await runs.forceFlush();
    expect(harness.drives).toBeGreaterThan(1);
    first = { runId: sessionId, spans: of(sessionId) };
    // Both turns finished, so their checkpoints were deleted.
    const state = await sessions.inspect(sessionId);
    for (const s of state.submissions.filter((x) => x.text !== undefined)) {
      const lines = (await readJsonLines(world, sessionId, checkpointStreamName(sessionId, s.id))) as { v?: unknown }[];
      expect(lines.filter((l) => 'v' in l).at(-1)).toMatchObject({ v: null });
    }
  }, 60_000);

  it('writes the spans Agent Runs reads: the eve.agent scope at schema 4, keyed by the session', () => {
    expect(first.spans.length).toBeGreaterThan(0);
    for (const span of first.spans) {
      expect(span.instrumentationScope.name).toBe('eve.agent');
      expect(span.attributes['agent.trace.schema.version']).toBe(4);
      expect(span.attributes['agent.run.id']).toBe(first.runId);
    }
    const names = first.spans.map((s) => s.name).sort();
    expect(names.filter((n) => n === 'invoke_agent scripted')).toHaveLength(2);
    expect(names.filter((n) => n === 'agent.step')).toHaveLength(4);
    expect(names.filter((n) => n === 'chat model')).toHaveLength(4);
    expect(names.filter((n) => n === 'execute_tool lookup')).toHaveLength(2);
  });

  it('keeps a turn that went on in a second drive one invoke_agent span, with its steps and tools under it', () => {
    const turns = first.spans.filter((s) => s.name === 'invoke_agent scripted');
    const ids = new Set(turns.map((t) => t.spanContext().spanId));
    expect(ids.size).toBe(2);
    for (const step of first.spans.filter((s) => s.name === 'agent.step')) expect(ids.has(parentOf(step)!)).toBe(true);
    const steps = new Set(first.spans.filter((s) => s.name === 'agent.step').map((s) => s.spanContext().spanId));
    for (const span of first.spans.filter((s) => s.name === 'chat model' || s.name === 'execute_tool lookup')) expect(steps.has(parentOf(span)!)).toBe(true);
    // Every span of a turn is in the turn's trace.
    for (const turn of turns) {
      const trace = turn.spanContext().traceId;
      const under = first.spans.filter((s) => s.spanContext().traceId === trace);
      expect(under.map((s) => s.name).sort()).toEqual(['agent.step', 'agent.step', 'chat model', 'chat model', 'execute_tool lookup', 'invoke_agent scripted']);
    }
    for (const turn of turns) expect(turn.attributes['agent.turn.outcome']).toBe('completed');
  });

  it("records a public session's content, and the tool call's id", () => {
    const tool = first.spans.find((s) => s.name === 'execute_tool lookup')!;
    expect(tool.attributes['gen_ai.tool.call.id']).toMatch(/^call_/);
    expect(tool.attributes['gen_ai.tool.call.arguments']).toContain('"q"');
    const turn = first.spans.find((s) => s.name === 'invoke_agent scripted' && s.attributes['agent.turn.sequence'] === 0)!;
    expect(turn.attributes['agent.trace.content.input']).toBe(true);
    expect(turn.attributes['agent.trace.content.output']).toBe(true);
    expect(turn.attributes['agent.session.title']).toBe('hello');
    expect(turn.attributes['agent.channel.audience']).toBe('public');
  });

  it("records only metadata for a session that is not public", async () => {
    const { sessions } = await session(scripted());
    const { sessionId } = await sessions.start({ prompt: 'a secret', config: { traceAudience: 'private' } });
    await idle(sessions, sessionId, 2);
    await runs.forceFlush();
    const spans = of(sessionId);
    expect(spans.map((s) => s.name)).toContain('execute_tool lookup');
    expect(JSON.stringify(spans.map((s) => s.attributes))).not.toContain('secret');
    const turn = spans.find((s) => s.name === 'invoke_agent scripted')!;
    expect(turn.attributes['agent.trace.content.input']).toBe(false);
    expect(turn.attributes['agent.session.title']).toBeUndefined();
  }, 30_000);
});

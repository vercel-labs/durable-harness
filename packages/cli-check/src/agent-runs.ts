import { appendFileSync } from 'node:fs';
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { type AgentRuns, agentRuns } from 'durable-harness/instrumentation/otel';
import { durableSessions, type Harness, QUEUE_PREFIX, type SessionState } from 'harness-world';
import type { World } from '@workflow/world';
import { expect } from 'vitest';
import { settled, until } from './scenario.ts';
import { type OpenWorld, storageFor } from './worlds.ts';

/**
 * Vercel Agent Runs checks: durable-harness's `agentRuns()` (on
 * `@vercel/agent-runs`) with an in-memory span processor, and what one tool
 * turn of a harness must give it.
 *
  * Sessions use an unknown audience. Each scripted harness runs once with
  * content enabled and once with the default metadata-only policy.
 */

export interface Tracing {
  readonly content: boolean;
  integration: AgentRuns;
  exporter: InMemorySpanExporter;
  /** The finished spans of one session (every span carries `agent.run.id`). */
  spansOf(sessionId: string): ReadableSpan[];
  /** Wait until the session has a finished root for `harness` (the drive that ran its turn returned). */
  finishedTurn(sessionId: string, harness: string, timeoutMs?: number): Promise<ReadableSpan[]>;
  /** Unregister (the integration installs itself on `globalThis`). */
  close(): Promise<void>;
}

export function tracing(content = true): Tracing {
  const exporter = new InMemorySpanExporter();
  const integration = agentRuns({ ...(content ? { capture: () => ({ emit: true, recordInputs: true, recordOutputs: true }) } : {}), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  integration.register();
  const spansOf = (sessionId: string) => exporter.getFinishedSpans().filter((s) => s.attributes['agent.run.id'] === sessionId);
  return {
    content,
    integration,
    exporter,
    spansOf,
    async finishedTurn(sessionId, harness, timeoutMs = 60_000) {
      await until(() => Promise.resolve(spansOf(sessionId).some((s) => s.name === `invoke_agent ${harness}`)), timeoutMs, `the ${harness} turn's root span`);
      await integration.forceFlush();
      const spans = spansOf(sessionId);
      for (const root of spans.filter(span => span.name.startsWith('invoke_agent'))) {
        expect(root.attributes['agent.trace.content.input']).toBe(content);
        expect(root.attributes['agent.trace.content.output']).toBe(content);
      }
      return spans;
    },
    async close() {
      await integration.forceFlush();
      delete (globalThis as Record<symbol, unknown>)[Symbol.for('durable-harness.agent-runs')];
    },
  };
}

export async function expectNoCheckpointContent(world: World, runId: string, marker: string): Promise<void> {
  const names = (await world.streams.list(runId)).filter(name => name.includes('_agent-runs_'));
  expect(names.length, 'durable trace checkpoints exist').toBeGreaterThan(0);
  for (const name of names) {
    let cursor: string | undefined;
    let text = '';
    const decoder = new TextDecoder();
    for (;;) {
      const page = await world.streams.getChunks(runId, name, { ...(cursor ? { cursor } : {}) });
      for (const chunk of page.data) text += decoder.decode(chunk.data, { stream: true });
      if (!page.hasMore || !page.cursor) break;
      cursor = page.cursor;
    }
    expect(text).not.toContain(marker);
  }
}

export interface TurnExpect {
  sessionId: string;
  /** The root's agent name: the harness's name (`invoke_agent <harness>`). */
  harness: string;
  /** A string in the prompt and in the tool call's arguments. */
  marker: string;
  /** The tool the turn calls. Undefined: any tool, as long as its arguments carry the marker. */
  tool?: string;
  /** The audience the session recorded. Default `unknown`. */
  audience?: 'public' | 'private' | 'unknown';
  /** The model's prompt reaches its span (a model that speaks a provider's format). */
  modelContent?: boolean;
  /** The model id every model span names (`chat <model>`). */
  model?: string | RegExp;
  /** Model spans carry token usage on their step. */
  usage?: boolean;
}

/** What one tool turn must give Agent Runs: the span tree and its content. */
export function expectToolTurn(spans: ReadableSpan[], o: TurnExpect): void {
  // `AGENT_RUNS_DUMP=<file>`: append the span tree (names and ids, no content) for a look by eye.
  if (process.env.AGENT_RUNS_DUMP) appendFileSync(process.env.AGENT_RUNS_DUMP, `${o.harness} ${o.sessionId}\n${outline(spans)}\n`);
  const byId = new Map(spans.map((s) => [s.spanContext().spanId, s]));
  const parentOf = (s: ReadableSpan) => (s.parentSpanContext ? byId.get(s.parentSpanContext.spanId) : undefined);
  const roots = spans.filter((s) => s.name === `invoke_agent ${o.harness}`);
  expect(roots.length, 'an invoke_agent root per turn').toBeGreaterThanOrEqual(1);
  const root = roots[0]!;
  expect(root.parentSpanContext, 'the root has no parent').toBeUndefined();
  expect(root.instrumentationScope.name).toBe('eve.agent');
  expect(root.attributes).toMatchObject({
    'agent.framework.name': 'durable-harness',
    'agent.name': o.harness,
    'agent.run.id': o.sessionId,
    'gen_ai.operation.name': 'invoke_agent',
    'agent.trace.schema.version': 4,
    'agent.channel.audience': o.audience ?? 'unknown',
    'agent.turn.outcome': 'completed',
  });
  const content = root.attributes['agent.trace.content.input'] === true;
  if (content) expect(String(root.attributes['agent.session.title'])).toContain(o.marker);
  else {
    expect(root.attributes['agent.session.title']).toBeUndefined();
    expect(JSON.stringify(spans.map(span => ({ attributes: span.attributes, events: span.events, status: span.status })))).not.toContain(o.marker);
  }
  expect(new Set(spans.map((s) => s.spanContext().traceId)), 'one trace per turn').toEqual(new Set([root.spanContext().traceId]));

  // Steps hang off a root; model and tool spans off a step.
  const steps = spans.filter((s) => s.name === 'agent.step');
  expect(steps.length).toBeGreaterThanOrEqual(2);
  for (const step of steps) expect(parentOf(step)?.name).toBe(`invoke_agent ${o.harness}`);
  const ofRoot = (s: ReadableSpan) => parentOf(parentOf(s)!)?.spanContext().spanId === root.spanContext().spanId;
  const models = spans.filter((s) => s.attributes['gen_ai.operation.name'] === 'chat');
  expect(models.length, 'a model span').toBeGreaterThanOrEqual(1);
  for (const m of models) expect(parentOf(m)?.name).toBe('agent.step');
  expect(models.some(ofRoot), "a model span in the tool turn's trace").toBe(true);
  if (content) expect(models[0]!.attributes['gen_ai.output.messages']).toBeDefined();
  if (o.modelContent && content) expect(models.map((m) => String(m.attributes['gen_ai.input.messages'])).join(''), "the prompt in a model span's input").toContain(o.marker);
  if (o.model) for (const m of models) expect(m.name).toMatch(typeof o.model === 'string' ? new RegExp(`^chat ${o.model.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) : o.model);
  if (o.usage) {
    const used = models.map((m) => parentOf(m)!).filter((step) => typeof step.attributes['agent.usage.input_tokens'] === 'number');
    expect(used.length, 'token usage on a model step').toBeGreaterThanOrEqual(1);
  }
  const tools = spans.filter((s) => s.attributes['gen_ai.operation.name'] === 'execute_tool' && (o.tool === undefined || s.name === `execute_tool ${o.tool}`));
  const calls = content ? tools.filter((s) => String(s.attributes['gen_ai.tool.call.arguments']).includes(o.marker)) : tools;
  expect(calls.length, `an execute_tool ${o.tool ?? '*'} span with the marker in its arguments (tools: ${tools.map((s) => s.name).join(', ') || 'none'})`).toBeGreaterThanOrEqual(1);
  if (o.tool) expect(tools).toHaveLength(1);
  const call = calls[0]!;
  expect(parentOf(call)?.name).toBe('agent.step');
  expect(ofRoot(call)).toBe(true);
  expect(call.attributes['gen_ai.tool.name']).toBe(call.name.slice('execute_tool '.length));
  if (content) expect(call.attributes['gen_ai.tool.call.result']).toBeDefined();
  // Where the model's reply names its tool calls, the tool span carries that
  // call's id: Agent Runs joins the two into one tool call by it.
  const asked = models.flatMap((m) => {
    try {
      return (JSON.parse(String(m.attributes['gen_ai.output.messages'])) as { parts?: { type?: string; id?: unknown }[] }[])
        .flatMap((msg) => msg.parts ?? []).filter((p) => p.type === 'tool_call').map((p) => p.id);
    } catch {
      return [];
    }
  });
  if (process.env.AGENT_RUNS_DUMP) appendFileSync(process.env.AGENT_RUNS_DUMP, `${o.harness} asked ${JSON.stringify(asked)} ran ${String(call.attributes['gen_ai.tool.call.id'])}\n`);
  if (asked.length) expect(asked, "the tool span's call id is the model's").toContain(call.attributes['gen_ai.tool.call.id']);
  expect(
    spans.filter((s) => s.status.code === 2).map((s) => `${s.name}: ${s.status.message}`),
    'no span failed'
  ).toEqual([]);
}

/**
 * A kernel session on a fresh world-local data directory: its first prompt
 * in (with `traceAudience: 'unknown'`), the queue served in this process,
 * settled once `done`. Returns its spans once its turn's root has ended.
 */
export async function kernelTurn(
  t: Tracing,
  disposers: (() => Promise<void> | void)[],
  name: string,
  harness: Harness,
  prompt: string,
  done: (s: SessionState) => boolean,
  timeoutMs = 90_000
): Promise<{ sessionId: string; state: SessionState; spans: ReadableSpan[]; world: World }> {
  const open: OpenWorld = await storageFor('local', `agent-runs-${name}`).open();
  disposers.push(() => open.close());
  const sessions = durableSessions({ world: open.world, harness });
  await open.serve(QUEUE_PREFIX, sessions.handler);
  const { sessionId } = await sessions.start({ prompt, config: { traceAudience: 'unknown' } });
  const state = await settled(open.world, sessions, sessionId, done, { what: `the ${name} tool turn`, timeoutMs });
  const spans = await t.finishedTurn(sessionId, harness.name);
  return { sessionId, state, spans, world: open.world };
}

/** The span tree, one line per span, indented under its parent: names and step ids, no content. */
export function outline(spans: readonly ReadableSpan[]): string {
  const children = new Map<string | undefined, ReadableSpan[]>();
  for (const s of spans) children.set(s.parentSpanContext?.spanId, [...(children.get(s.parentSpanContext?.spanId) ?? []), s]);
  const ids = new Set(spans.map((s) => s.spanContext().spanId));
  const lines: string[] = [];
  const walk = (s: ReadableSpan, depth: number) => {
    const usage = s.attributes['agent.usage.input_tokens'] !== undefined ? ` usage ${s.attributes['agent.usage.input_tokens']}/${s.attributes['agent.usage.output_tokens']}` : '';
    lines.push(`${'  '.repeat(depth)}${s.name}${usage}${s.status.code === 2 ? ' ERROR' : ''}`);
    for (const c of (children.get(s.spanContext().spanId) ?? []).sort((a, b) => a.startTime[0] - b.startTime[0] || a.startTime[1] - b.startTime[1])) walk(c, depth + 1);
  };
  for (const s of spans.filter((x) => !x.parentSpanContext || !ids.has(x.parentSpanContext.spanId))) walk(s, 1);
  return lines.join('\n');
}

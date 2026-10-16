import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AgentRun,
  AttemptOperation,
  CaptureDecision,
  ModelOperation,
  ToolOperation,
  TurnInput,
  TurnOperation,
} from '@vercel/agent-runs';
import type { DriveContext, EffectOptions, Harness, SessionState, Submission } from 'harness-world';
import { type DriveTracing, type ModelOptions, type ModelResult, type TraceAudience, type TracedModelCall, traceAudienceOf } from 'harness-world/agent-runs';
import { Fatal, Parked } from 'harness-world/errors';
import type { TraceStreams } from './checkpoints.ts';

export interface DriveTracerOptions {
  readonly run: AgentRun;
  readonly streams: TraceStreams;
  /** For a session whose config records no `traceAudience`. */
  readonly audience: TraceAudience;
  readonly capture: (audience: TraceAudience) => CaptureDecision;
  readonly version: string;
}

/** A `model` effect running in this drive: the step it is, created when its model call is recorded. */
interface Slot {
  readonly drive: object;
  readonly turn: TurnTrace;
  readonly stepIndex: number;
  readonly attempt: number;
  step?: Promise<Step | undefined>;
}

/** One step (attempt) of a turn: a model call, and the tool calls its response asks for. */
interface Step {
  readonly op: AttemptOperation;
  /** The tool calls the step's model responses asked for, not yet matched to a tool effect. */
  readonly asked: { id: string; name: string; args: string }[];
  /** Model calls still running: a step another one starts beside is not ended under them. */
  running: number;
  /** Its tool calls not settled, a parked one among them: ending the step would close them. */
  open: number;
}

interface TurnTrace {
  readonly submission: Submission;
  op?: Promise<TurnOperation | undefined>;
  steps: Step[];
  /** Steps of model calls recorded outside a `model` effect, numbered in this drive. */
  requests: number;
  /** This drive's number for the turn, for those steps' attempt numbers. */
  drive?: Promise<number>;
}

const slots = new AsyncLocalStorage<Slot>();
const record = (v: unknown): Record<string, unknown> => (v !== null && typeof v === 'object' ? (v as Record<string, unknown>) : {});
const isPrompt = (s: Submission | undefined): s is Submission & { text: string } => s?.text !== undefined && s.kind !== 'steer';

/**
 * Drive `harness` with each turn of the session traced: a turn per prompt
 * (steers join the open one), a step per model call with the tool calls it
 * asks for under it. A turn that goes on in a later drive (out of time, a
 * crash, a handoff) is continued there from its checkpoint, so it stays one
 * `invoke_agent` span; work already in the log is never traced again.
 */
export async function traceDrive(options: DriveTracerOptions, harness: Harness, ctx: DriveContext): Promise<void> {
  const { run, streams } = options;
  streams.forget(ctx.runId);
  const audience = traceAudienceOf(record(ctx.config).traceAudience) ?? options.audience;
  const capture = options.capture(audience);
  const self = harness.modelCalls === 'self';
  const drive = {};
  const turns = new Map<string, TurnTrace>();
  const prompts = () => ctx.state().submissions.filter(isPrompt);

  let current: TurnTrace | undefined;
  const turnOf = (submission: Submission) => {
    let turn = turns.get(submission.id);
    if (!turn) turns.set(submission.id, (turn = { submission, steps: [], requests: 0 }));
    return turn;
  };
  const initial = ctx.pending().find(isPrompt) ?? prompts().filter((s) => ctx.state().placed.has(s.id)).at(-1);
  if (initial) current = turnOf(initial);

  function input(submission: Submission): TurnInput {
    const first = prompts()[0];
    const title = capture.recordInputs && first?.text ? first.text.slice(0, 160) : undefined;
    return {
      identity: { conversationId: ctx.runId, runId: ctx.runId, turnId: submission.id },
      agentName: harness.name,
      sequence: Math.max(0, prompts().findIndex((s) => s.id === submission.id)),
      framework: { name: 'durable-harness', version: options.version },
      capture,
      channel: { kind: 'http', origin: 'channel' },
      attributes: { 'agent.channel.audience': audience, ...(title ? { 'agent.session.title': title } : {}) },
    };
  }

  /** The turn's operation, started if it never was. A turn that already finished is not started again. */
  function started(turn: TurnTrace): Promise<TurnOperation | undefined> {
    turn.op ??= (async () => {
      if (await streams.finished(ctx.runId, turn.submission.id)) return undefined;
      return run.turn(input(turn.submission));
    })();
    return turn.op;
  }

  /** The turn's operation if this or an earlier drive started it and it is still open. */
  async function saved(turn: TurnTrace): Promise<TurnOperation | undefined> {
    if (turn.op) return turn.op;
    const data = input(turn.submission);
    if (!(await run.resume({ identity: data.identity }))) return undefined;
    // Re-enter before exposing restored children so recovery cannot retain an older, wider capture policy.
    const resumed = await run.turn(data);
    resumed.attributes({ 'agent.channel.audience': audience });
    turn.op = Promise.resolve(resumed);
    return resumed;
  }

  async function endSteps(turn: TurnTrace, error?: unknown, which: (step: Step) => boolean = () => true): Promise<void> {
    const ending = turn.steps.filter(which);
    turn.steps = turn.steps.filter((s) => !ending.includes(s));
    for (const step of ending) await (error === undefined ? step.op.complete() : step.op.fail(error));
  }

  async function endTurn(turn: TurnTrace, outcome: 'completed' | 'failed' | 'cancelled' | 'running', error?: unknown): Promise<void> {
    const op = await saved(turn);
    // A turn that goes on keeps the steps of its open tool calls: the drive that settles one finds it in them.
    await endSteps(turn, outcome === 'failed' || outcome === 'running' ? error : undefined, outcome === 'running' ? (s) => s.open === 0 : undefined);
    if (!op || outcome === 'running') return;
    if (outcome === 'failed') await op.complete({ outcome, failed: true, error, ...(error instanceof Error ? { errorType: error.name } : {}) });
    else await op.complete({ outcome });
  }

  /** A newly placed prompt starts its turn; the one before it is over. */
  async function place(placed: readonly string[] | undefined): Promise<void> {
    const submission = ctx.state().submissions.find((s) => placed?.includes(s.id) && isPrompt(s));
    if (!submission || submission.id === current?.submission.id) return;
    const previous = current;
    current = turnOf(submission);
    if (previous) await endTurn(previous, 'completed');
  }

  async function newStep(turn: TurnTrace, at: { stepIndex: number; attempt: number }): Promise<Step | undefined> {
    const op = await started(turn);
    if (!op || op.finished) return undefined;
    await endSteps(turn, undefined, (s) => s.running === 0 && s.open === 0);
    const step: Step = { op: await op.attempt(at), asked: [], running: 0, open: 0 };
    turn.steps.push(step);
    return step;
  }

  /** A step for a model call made outside a `model` effect (a CLI's request through the Gateway proxy). */
  async function requestStep(turn: TurnTrace): Promise<Step | undefined> {
    if (!turn.drive) {
      const drive = (turn.drive = streams.nextDrive(ctx.runId, turn.submission.id));
      // Not recorded: a later step asks again.
      drive.catch(() => turn.drive === drive && (turn.drive = undefined));
    }
    return newStep(turn, { stepIndex: turn.requests++, attempt: await turn.drive });
  }

  /** Watches a recorded model call: the step's tool calls are the ones its response asks for. */
  function watch(model: ModelOperation, step: Step): ModelOperation {
    step.running++;
    let settled = false;
    const settle = (result?: ModelResult) => {
      if (settled) return;
      settled = true;
      step.running--;
      for (const part of result?.content ?? []) {
        if (part.type === 'tool-call') step.asked.push({ id: part.callId, name: part.toolName, args: canonical(part.input) });
      }
    };
    return new Proxy(model, {
      get(target, key, receiver) {
        if (key === 'complete') return (result?: Parameters<ModelOperation['complete']>[0]) => (settle(result?.result ?? result?.model), target.complete(result));
        if (key === 'fail') return (error: unknown) => (settle(), target.fail(error));
        const value = Reflect.get(target, key, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
  }

  async function modelCall(options: ModelOptions): Promise<TracedModelCall | undefined> {
    const slot = slots.getStore();
    const inEffect = slot?.drive === drive;
    const turn = inEffect ? slot.turn : current;
    if (!turn) return undefined;
    const step = inEffect ? await (slot.step ??= newStep(turn, slot)) : await requestStep(turn);
    if (!step) return undefined;
    return { model: watch(await step.op.modelCall(options), step), attempt: step.op };
  }

  /** The tool call's id as the model gave it, so Agent Runs joins the call in the reply with its execution. */
  function callIdOf(turn: TurnTrace, key: string, name: string, input: unknown): string {
    const fromKey = key.startsWith('tool:') && !key.slice(5).includes(':') ? key.slice(5).split('@')[0]! : undefined;
    const args = canonical(input);
    for (const step of [...turn.steps].reverse()) {
      const index = [
        (c: Step['asked'][number]) => c.id === fromKey,
        (c: Step['asked'][number]) => fromKey === undefined && c.name === name && c.args === args,
        (c: Step['asked'][number]) => fromKey === undefined && c.name === name,
      ].reduce((found, test) => (found >= 0 ? found : step.asked.findIndex(test)), -1);
      if (index >= 0) return step.asked.splice(index, 1)[0]!.id;
    }
    return fromKey ?? key;
  }

  /** The tool call's operation: one an earlier drive started, else a new one under the step that asked for it. */
  async function openTool(key: string, effect: EffectOptions): Promise<{ tool: ToolOperation; step?: Step } | undefined> {
    const turn = current;
    const op = turn && (await started(turn));
    if (!turn || !op || op.finished) return undefined;
    const name = effect.name.slice('tool.'.length);
    const callId = callIdOf(turn, key, name, effect.input);
    const found = op.findTool(callId);
    if (found) return { tool: found };
    const step = turn.steps.at(-1) ?? (await requestStep(turn));
    const tool = await step?.op.tool({ callId, name, arguments: effect.input });
    return tool && { tool, step };
  }

  async function traceTool<T>(key: string, effect: EffectOptions, execute: () => Promise<T>): Promise<T> {
    // Tracing never fails a tool call: one it cannot record runs untraced.
    const traced = await openTool(key, effect).catch(() => undefined);
    if (!traced) return execute();
    const { tool, step } = traced;
    if (step) step.open++;
    let value: T;
    try {
      value = await tool.run(execute);
    } catch (error) {
      // A call the drive stopped under, or handed off, goes on in a later drive: its span stays open for it.
      if (!(error instanceof Parked) && !ctx.signal.aborted) {
        if (step) step.open--;
        await tool.fail(error).catch(() => {});
      }
      throw error;
    }
    if (step) step.open--;
    await tool.complete({ outcome: 'completed', output: value }).catch(() => {});
    return value;
  }

  /** A tool call an earlier drive started, which the kernel settled without running it again (interrupted, or parked work collected). */
  async function settleOpenTool(key: string, effect: EffectOptions, outcome: { value?: unknown; error?: unknown }): Promise<void> {
    const turn = current;
    const op = turn && (await saved(turn));
    if (!turn || !op) return;
    const tool = op.findTool(callIdOf(turn, key, effect.name.slice('tool.'.length), effect.input));
    if (!tool) return;
    if (outcome.error !== undefined) await tool.fail(outcome.error);
    else await tool.complete({ outcome: 'completed', output: outcome.value });
  }

  /** A `model` effect of a harness whose model calls are its effects: one call, from what the effect records. */
  async function effectModelCall<T>(step: Step | undefined, effect: EffectOptions, execute: () => Promise<T>): Promise<T> {
    if (!step) return execute();
    const id = String(record(effect.input).model ?? 'unknown');
    const slash = id.indexOf('/');
    const model = watch(await step.op.modelCall({ provider: slash > 0 ? id.slice(0, slash) : 'unknown', modelId: slash > 0 ? id.slice(slash + 1) : id }), step);
    try {
      const value = await model.run(execute);
      const output = record(value);
      await model.complete({
        outcome: 'completed',
        result: {
          finishReason: typeof output.finishReason === 'string' ? output.finishReason : 'unknown',
          usage: {},
          content: typeof value === 'string' ? [{ type: 'text', text: value }] : undefined,
        },
      });
      return value;
    } catch (error) {
      await model.fail(error);
      throw error;
    }
  }

  const tracing: DriveTracing = { modelCall: (o) => modelCall(o).catch(() => undefined) };
  const traced: DriveContext = {
    ...ctx,
    tracing,
    async effect(key, effect, fn) {
      await place(effect.placed).catch(() => {});
      const model = effect.name === 'model';
      const tool = effect.name.startsWith('tool.');
      if ((!model && !tool) || !current) return ctx.effect(key, effect, fn);
      const turn = current;
      let ran = false;
      const body = async (io: Parameters<typeof fn>[0]) => {
        ran = true;
        if (tool) return traceTool(key, effect, () => fn(io));
        const slot: Slot = { drive, turn, ...stepOf(ctx.state(), turn.submission.id, key) };
        if (!self) slot.step = newStep(turn, slot).catch(() => undefined);
        return slots.run(slot, async () => (self ? fn(io) : effectModelCall(await slot.step, effect, () => fn(io))));
      };
      const result = ctx.effect(key, effect, body);
      if (tool) {
        void result.then(
          (value) => (ran ? undefined : settleOpenTool(key, effect, { value })),
          (error: unknown) => (ran ? undefined : settleOpenTool(key, effect, { error }))
        ).catch(() => {});
      }
      return result;
    },
  };

  let outcome: 'completed' | 'failed' | 'cancelled' | 'running' = 'completed';
  let reason: unknown;
  try {
    await harness.drive(traced);
  } catch (error) {
    reason = error;
    throw error;
  } finally {
    const state = ctx.state();
    const stop = reason instanceof Fatal ? reason : ctx.signal.aborted ? ctx.signal.reason : reason;
    outcome =
      state.status === 'cancelled' ? 'cancelled'
      : stop instanceof Fatal || state.status === 'failed' ? 'failed'
      : stop !== undefined ? 'running'
      : 'completed';
    try {
      if (current) await endTurn(current, outcome, outcome === 'cancelled' ? undefined : (stop ?? (state.failure ? Object.assign(new Error(state.failure.message), { name: 'RunFailed' }) : undefined)));
    } catch {
      // Tracing never fails a drive.
    }
    await streams.flush();
  }
}

/**
 * A `model` effect's place among its turn's model calls, and its attempt
 * (a retry after a crash is the same step again): the same in every drive,
 * so a step an earlier drive started is the same step.
 */
function stepOf(state: SessionState, turnId: string, key: string): { stepIndex: number; attempt: number } {
  const prompts = new Set(state.submissions.filter(isPrompt).map((s) => s.id));
  const seen = new Set<string>();
  let turn: string | undefined;
  for (const effect of state.effectOrder) {
    const placed = effect.placed.find((id) => prompts.has(id));
    if (placed) turn = placed;
    if (effect.name !== 'model' || turn !== turnId || seen.has(effect.key)) continue;
    if (effect.key === key) break;
    seen.add(effect.key);
  }
  return { stepIndex: seen.size, attempt: Math.max(0, (state.effects.get(key)?.attempt ?? 1) - 1) };
}

/** Arguments as comparable text: a model may send them as a JSON string or an object. */
function canonical(args: unknown): string {
  let value = args;
  if (typeof args === 'string') {
    try {
      value = JSON.parse(args);
    } catch {
      return args;
    }
  }
  const sort = (v: unknown): unknown =>
    Array.isArray(v) ? v.map(sort)
    : v !== null && typeof v === 'object' ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, x]) => [k, sort(x)]))
    : v;
  return JSON.stringify(sort(value)) ?? '';
}

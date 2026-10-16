import type { World } from '@workflow/world';
import type { ApprovalOperation, AttemptOperation, ModelOperation, ToolOperation, TurnOperation } from '@vercel/agent-runs';
import { type AgentRunsHook, agentRunsHook, type ContentPart, type ModelOptions, type SessionTracing, type TraceAudience } from 'harness-world/agent-runs';
import type { FxHookEvent, FxHooks } from 'libfx';

type FxHookMessage = NonNullable<Extract<FxHookEvent, { type: 'model.start' }>['data']['messages']>[number];
type FxWaitOutcome = Extract<FxHookEvent, { type: 'wait.end' }>['data']['outcome'];

export interface FxAgentRunsOptions {
  /** The World the agent's sessions live on (`createFxAgent({ durability })`'s). Default: the environment's (`getWorld()`). */
  readonly world?: World;
  /** Names the agent in Agent Runs (`invoke_agent <name>`). Default `fx`. */
  readonly name?: string;
  /** Who may read its sessions' content. Default: `agentRuns({ audience })`'s. */
  readonly audience?: TraceAudience;
}

/** libfx's `FxHooks`, for `createFxAgent({ hooks })`. */
export interface FxAgentRunsHooks extends FxHooks {
  /** Whether libfx passes the hooks content: if the audience records any, or `true` while `agentRuns()` isn't registered yet. */
  readonly captureContent: boolean;
  /** Settles once every event so far is recorded and its turn's checkpoint stored. */
  flush(): Promise<void>;
}

/** One turn of a session as one execution's events arrive: its operation and what is open in it. */
interface TurnTrace {
  readonly id: string;
  /** The execution running it. Another's events (one this process ran before a takeover) are not this turn's. */
  readonly execution: string | null;
  readonly op: TurnOperation | undefined;
  /** Another worker's write fenced this execution's journal out: its end is not the turn's. */
  fenced: boolean;
  /** By step and model call: both are numbered within the execution. */
  readonly steps: Map<number, AttemptOperation>;
  readonly models: Map<number, { op: ModelOperation; own?: AttemptOperation }>;
  readonly tools: Map<string, ToolOperation>;
  /** Tool calls that ended cancelled, settled by the turn's end: a handoff leaves them open for the execution that runs them again. */
  readonly cancelled: Map<string, ToolOperation>;
  readonly waits: Map<string, ApprovalOperation>;
}

interface SessionTrace {
  readonly tracing: Promise<SessionTracing | undefined>;
  /** The turn an execution is running, until its end. */
  turn?: TurnTrace;
  /** Its events, recorded one at a time in order. */
  queue: Promise<void>;
}

const parse = (json: string | null | undefined): unknown => {
  if (json === undefined || json === null) return undefined;
  try {
    return JSON.parse(json);
  } catch {
    return json;
  }
};

/** A model request's messages in AI SDK form, which `@vercel/agent-runs` reads; system messages are its instructions. */
function modelMessages(messages: readonly FxHookMessage[]): { role: string; content: Record<string, unknown>[] }[] {
  return messages.flatMap((m): { role: string; content: Record<string, unknown>[] }[] => {
    if (m.role === 'system') return [];
    if (m.role === 'tool') {
      return [{ role: 'tool', content: [{ type: 'tool-result', toolCallId: m.toolCallId, toolName: m.toolName, output: { type: m.toolResultStatus === 'failure' ? 'error-text' : 'text', value: m.content ?? '' } }] }];
    }
    const calls = m.toolCalls.map((c) => ({ type: 'tool-call', toolCallId: c.callId, toolName: c.name, input: parse(c.argumentsJson) }));
    const text = m.content ? [{ type: 'text', text: m.content }] : [];
    return [{ role: m.role, content: [...text, ...calls] }];
  });
}

function modelOptions(data: Extract<FxHookEvent, { type: 'model.start' }>['data']): ModelOptions {
  const instructions = [...(data.instructions ?? []), ...(data.messages ?? []).filter((m) => m.role === 'system')]
    .map((m) => m.content)
    .filter((c): c is string => !!c);
  return {
    // libfx calls AI Gateway: the model id stays whole, as Agent Runs links it.
    provider: 'gateway',
    modelId: data.model,
    ...(data.messages ? { messages: modelMessages(data.messages) } : {}),
    ...(instructions.length ? { instructions: instructions.join('\n\n') } : {}),
    ...(data.tools.length ? { tools: data.tools.map((t) => ({ type: 'function', name: t.name, description: t.description ?? undefined, inputSchema: parse(t.inputSchemaJson) })) } : {}),
  };
}

function modelContent(data: Extract<FxHookEvent, { type: 'model.end' }>['data']): ContentPart[] {
  return [
    ...(data.output ? [{ type: 'text' as const, text: data.output }] : []),
    ...(data.toolCalls ?? []).map((c) => ({ type: 'tool-call' as const, callId: c.callId, toolName: c.name, input: parse(c.argumentsJson) })),
  ];
}

const APPROVAL: Record<FxWaitOutcome, string> = { accepted: 'approved', denied: 'denied', cancelled: 'cancelled', failed: 'failed', timed_out: 'timed_out' };

/** Settles `work` past the response on Vercel (the request context's `waitUntil`). */
function keepAlive(work: Promise<unknown>): void {
  const settled = work.then(() => {}, () => {});
  (globalThis as { [k: symbol]: { get?: () => { waitUntil?: (p: Promise<unknown>) => void } | undefined } | undefined })[Symbol.for('@vercel/request-context')]?.get?.()?.waitUntil?.(settled);
}

/**
 * Agent Runs for libfx's own durable agents, from libfx's semantic hooks:
 *
 * ```ts
 * import { createFxAgent } from 'libfx'
 * import { agentRunsHooks } from 'durable-harness/fx'
 * export const agent = createFxAgent({ durability, model, hooks: agentRunsHooks() })
 * ```
 *
 * Each libfx turn is a turn, each step of it a step with its model call, and
 * the tool calls (and the approvals they wait on) are under the step. A turn
 * that libfx yields, pauses, hands to another worker (at its deadline, or
 * when it lost its lease) or that another worker fenced out stays open: the
 * execution that resumes it (in any process) continues the same turn from
 * its checkpoint, its steps numbered by that execution, and a call the
 * handoff cut off runs again as the same call. Nothing is recorded until
 * `agentRuns()` is registered.
 *
 * libfx doesn't wait for its hooks, so a turn is recorded just after it
 * runs: an execution that resumes it at once elsewhere may read its
 * checkpoint before this process stores the last of it.
 */
export function agentRunsHooks(options: FxAgentRunsOptions = {}): FxAgentRunsHooks {
  const sessions = new Map<string, SessionTrace>();
  let world: Promise<World> | undefined;
  let pending: Promise<void> = Promise.resolve();
  const report = (error: unknown, phase: 'start' | 'complete' = 'complete') => {
    const hook = agentRunsHook();
    if (hook) hook.onError(error, { phase });
    else console.warn('[durable-harness] Agent Runs: an fx hook event was not recorded', error instanceof Error ? error.message : error);
  };

  /** A session's trace, made at once on its turn's start, so its events queue in the order they came. */
  function open(id: string, hook: AgentRunsHook): SessionTrace {
    world ??= options.world ? Promise.resolve(options.world) : import('./world.ts').then((m) => m.getWorld());
    const tracing = world.then((w) => hook.session({ world: w, runId: id, agentName: options.name ?? 'fx', ...(options.audience ? { audience: options.audience } : {}) }));
    const session: SessionTrace = { tracing: tracing.catch((error: unknown) => (report(error, 'start'), undefined)), queue: Promise.resolve() };
    sessions.set(id, session);
    return session;
  }

  /** Ends what the execution left open in its steps: a model call still running is abandoned with them. */
  async function settleSteps(turn: TurnTrace | undefined): Promise<void> {
    if (!turn) return;
    for (const step of turn.steps.values()) await step.complete();
    for (const model of turn.models.values()) await model.own?.complete();
    turn.steps.clear();
    turn.models.clear();
  }

  async function record(session: SessionTrace, tracing: SessionTracing, event: FxHookEvent): Promise<void> {
    if (event.type === 'turn.start') {
      const e = event;
      if (!e.turnId) return;
      // A takeover in this process: the execution it replaces is done with its steps.
      await settleSteps(session.turn);
      // Each execution asks again: the library continues the turn from its checkpoint, which another process may have moved on.
      // libfx's hooks carry no turn number: the session numbers its turns.
      const op = await tracing.turn({ turnId: e.turnId, ...(e.data.input ? { title: e.data.input } : {}) });
      session.turn = { id: e.turnId, execution: e.executionId, op, fenced: false, steps: new Map(), models: new Map(), tools: new Map(), cancelled: new Map(), waits: new Map() };
      return;
    }
    const turn = session.turn;
    if (event.type === 'journal.error') {
      // A journal receipt names no execution: the one recording now is the one another worker's write fenced out.
      if (turn && event.data.fenced) turn.fenced = true;
      return;
    }
    if (!turn || turn.id !== event.turnId || turn.execution !== event.executionId) return;
    const op = turn.op;
    if (event.type === 'turn.end') {
      const e = event;
      session.turn = undefined;
      await settleSteps(turn);
      if (!op || op.finished) return;
      const { outcome, error, handoff } = e.data;
      // Handed to another worker, fenced out by one (libfx then ends it `interrupted`), yielded or paused:
      // the execution that resumes it continues the turn, with its cut-off calls still open.
      if (!handoff && !turn.fenced && outcome !== 'yielded' && outcome !== 'paused' && outcome !== 'fenced') {
        for (const tool of turn.cancelled.values()) await tool.complete({ outcome: 'cancelled' });
        if (outcome === 'failed') await op.complete({ outcome, failed: true, error: new Error(error ?? 'the turn failed') });
        // `interrupted` with no handoff is the user's cancel, which the session's history keeps.
        else await op.complete({ outcome: outcome === 'completed' ? 'completed' : 'cancelled' });
      }
      await tracing.flush();
      return;
    }
    if (!op || op.finished) return;
    switch (event.type) {
      case 'step.start': {
        if (!turn.execution || event.stepIndex < 1) return;
        turn.steps.set(event.stepIndex, await tracing.step(op, turn.id, { execution: turn.execution, stepIndex: event.stepIndex - 1 }));
        return;
      }
      case 'step.end': {
        await turn.steps.get(event.stepIndex)?.complete();
        turn.steps.delete(event.stepIndex);
        return;
      }
      case 'model.start': {
        const e = event;
        // A call outside a step (a title, a compaction) is a step of its own.
        const step = turn.steps.get(event.stepIndex);
        const own = step ? undefined : await tracing.step(op, turn.id);
        const model = await (step ?? own)!.modelCall(modelOptions(e.data));
        turn.models.set(e.data.modelCallId, { op: model, ...(own ? { own } : {}) });
        return;
      }
      case 'model.end': {
        const e = event;
        const model = turn.models.get(e.data.modelCallId);
        turn.models.delete(e.data.modelCallId);
        if (!model) return;
        if (e.data.error !== null) await model.op.fail(new Error(e.data.error));
        else if (e.data.cancelled) await model.op.complete({ outcome: 'cancelled' });
        else {
          const u = e.data.usage;
          await model.op.complete({
            outcome: 'completed',
            result: {
              finishReason: e.data.finishReason ?? 'unknown',
              usage: {
                inputTokens: u?.inputTokens ?? undefined,
                outputTokens: u?.outputTokens ?? undefined,
                inputTokenDetails: { cacheReadTokens: u?.cacheReadTokens ?? undefined, cacheWriteTokens: u?.cacheWriteTokens ?? undefined },
              },
              ...(e.data.generationId ? { responseId: e.data.generationId } : {}),
              content: modelContent(e.data),
            },
          });
        }
        await model.own?.complete();
        return;
      }
      case 'tool.start': {
        const e = event;
        const { callId, name } = e.data.call;
        turn.cancelled.delete(callId);
        // A call an earlier execution started (a rerun after a crash or a handoff) is the same call.
        const tool = op.findTool(callId) ?? (await (turn.steps.get(event.stepIndex) ?? [...turn.steps.values()].at(-1))?.tool({ callId, name, arguments: parse(e.data.call.argumentsJson) }));
        if (tool) turn.tools.set(callId, tool);
        return;
      }
      case 'tool.end': {
        const e = event;
        const { callId } = e.data.call;
        const tool = turn.tools.get(callId) ?? op.findTool(callId);
        turn.tools.delete(callId);
        if (!tool) return;
        if (e.data.status === 'success') await tool.complete({ outcome: 'completed', output: parse(e.data.output) });
        // The turn's end tells whether it was the user's cancel or a handoff, after which the call runs again.
        else if (e.data.status === 'cancelled') turn.cancelled.set(callId, tool);
        else await tool.fail(new Error(e.data.error ?? 'the tool call failed'));
        return;
      }
      case 'wait.start': {
        const e = event;
        const tool = e.data.callId ? (turn.tools.get(e.data.callId) ?? op.findTool(e.data.callId)) : undefined;
        const approval = await tool?.approval({ requestId: String(e.data.requestId), ...(e.data.prompt ? { request: e.data.prompt } : {}) });
        if (approval) turn.waits.set(String(e.data.requestId), approval);
        return;
      }
      case 'wait.end': {
        const e = event;
        const approval = turn.waits.get(String(e.data.requestId));
        turn.waits.delete(String(e.data.requestId));
        await approval?.complete({ outcome: APPROVAL[e.data.outcome] ?? e.data.outcome });
        return;
      }
    }
  }

  return {
    // libfx reads this once, as it makes the agent. With agentRuns() registered by then, content crosses only where the audience records it.
    get captureContent() {
      const decision = agentRunsHook()?.capture(options.audience);
      return decision ? decision.emit && (decision.recordInputs || decision.recordOutputs) : true;
    },
    onEvent(event) {
      const hook = agentRunsHook();
      if (!hook) return;
      // A session's trace starts with a turn. Journal receipts, and calls that outlive their turn (a title), find none open.
      if (event.turnId === null && event.type !== 'journal.error') return;
      const session = sessions.get(event.sessionId) ?? (event.type === 'turn.start' && event.turnId ? open(event.sessionId, hook) : undefined);
      if (!session) return;
      // libfx does not wait for a hook: each session's events are recorded one at a time, in order.
      const next: Promise<void> = session.queue
        .then(async () => {
          const tracing = await session.tracing;
          if (tracing) await record(session, tracing, event);
        })
        .catch((error: unknown) => report(error, event.type.endsWith('.start') ? 'start' : 'complete'))
        .then(() => {
          // Between turns, with nothing queued behind, the session keeps nothing: its next turn reads its checkpoint again.
          if (session.queue === next && !session.turn && sessions.get(event.sessionId) === session) sessions.delete(event.sessionId);
        });
      session.queue = next;
      pending = pending.then(() => next);
      keepAlive(next);
    },
    onError: (error) => report(error),
    async flush() {
      for (;;) {
        const at = pending;
        await at;
        if (at === pending) break;
      }
      await Promise.all([...sessions.values()].map(async (s) => (await s.tracing)?.flush()));
    },
  };
}

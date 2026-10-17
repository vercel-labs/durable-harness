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
  readonly captureContent: boolean;
  /** Wait for already queued events and bounded, best-effort checkpoint writes. */
  flush(): Promise<void>;
}

/** One turn of a session as its events arrive: its operation and what is open in it. */
interface TurnTrace {
  readonly id: string;
  readonly executionId: string;
  op?: TurnOperation;
  fenced: boolean;
  readonly steps: Map<string, AttemptOperation>;
  readonly models: Map<string, { op: ModelOperation; step: AttemptOperation; own?: AttemptOperation }>;
  readonly tools: Map<string, ToolOperation>;
  readonly waits: Map<string, ApprovalOperation>;
}

interface SessionTrace {
  readonly tracing: Promise<SessionTracing | undefined>;
  turn?: TurnTrace;
  /** Its events, recorded one at a time in order. */
  queue: Promise<void>;
  ended: boolean;
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
 * that libfx yields, pauses or hands to another worker stays open: the
 * execution that resumes it (in any process) continues the same turn from
 * its checkpoint, its steps numbered by that execution. Nothing is recorded
 * until `agentRuns()` is registered.
 * Requires libfx 0.0.14 or later.
 * Register tracing before creating the agent when content capture is needed.
 * libfx does not await hooks: checkpoints and waitUntil are best effort. A
 * new delivery can see an older checkpoint if it starts before this queue
 * flushes; await flush() when the host controls the handoff.
 */
export function agentRunsHooks(options: FxAgentRunsOptions = {}): FxAgentRunsHooks {
  const sessions = new Map<string, SessionTrace>();
  let world: Promise<World> | undefined;
  const pending = new Set<Promise<void>>();
  const report = (error: unknown) => { try { agentRunsHook()?.onError(error, { phase: 'complete' }); } catch {} };

  /** The session's trace, found or made at once, so its events queue in the order they came. */
  function sessionFor(event: FxHookEvent, hook: AgentRunsHook): SessionTrace | undefined {
    let session = sessions.get(event.sessionId);
    if (!session) {
      if (event.type !== 'turn.start' || !event.turnId || !event.executionId) return undefined;
      world ??= options.world ? Promise.resolve(options.world) : import('./world.ts').then((m) => m.getWorld());
      const tracing = world.then((w) => hook.session({ world: w, runId: event.sessionId, agentName: options.name ?? 'fx', ...(options.audience ? { audience: options.audience } : {}) }));
      session = { tracing: tracing.catch((error: unknown) => (report(error), undefined)), queue: Promise.resolve(), ended: false };
      sessions.set(event.sessionId, session);
    }
    return session;
  }

  function parkTools(tracing: SessionTracing, turn: TurnTrace): Promise<void> {
    return tracing.parkTools(turn.id, [...turn.tools].map(([callId, tool]) => ({
      callId,
      approvals: [...turn.waits].filter(([, wait]) => wait.parent?.spanId === tool.reference.spanId).map(([id]) => id),
    })));
  }

  async function record(session: SessionTrace, tracing: SessionTracing, event: FxHookEvent): Promise<void> {
    if (event.type === 'journal.error' && event.data.fenced) {
      // libfx's journal receipts have no turn/execution id. Preserve the active turn conservatively.
      if (session.turn && !session.ended) session.turn.fenced = true;
      return;
    }
    if (event.type === 'turn.start' && event.turnId && event.executionId) {
      const e = event;
      if (session.turn?.id === e.turnId && session.turn.executionId === e.executionId && !session.ended) return;
      const turn: TurnTrace = { id: e.turnId!, executionId: e.executionId!, fenced: false, steps: new Map(), models: new Map(), tools: new Map(), waits: new Map() };
      session.turn = turn;
      session.ended = false;
      await tracing.flush();
      // Each execution asks again: the library continues the turn from its checkpoint, which another process may have moved on.
      turn.op = await tracing.turn({ turnId: e.turnId!, ...(e.data.input ? { title: e.data.input } : {}) });
      if (turn.op) for (const pending of await tracing.parkedTools(turn.id)) {
        const tool = turn.op.findTool(pending.callId);
        if (!tool) continue;
        turn.tools.set(pending.callId, tool);
        for (const id of pending.approvals) {
          const wait = tool.findApproval(id);
          if (wait) turn.waits.set(id, wait);
        }
      }
      return;
    }
    const turn = session.turn;
    if (!turn || turn.id !== event.turnId || turn.executionId !== event.executionId) return;
    if (event.type === 'turn.end') {
      session.ended = true;
      const outcome = event.data.outcome;
      const continuing = event.data.handoff === true || turn.fenced || outcome === 'yielded' || outcome === 'paused' || outcome === 'fenced';
      const final = outcome === 'interrupted' ? 'cancelled' : outcome;
      if (!continuing) {
        for (const wait of turn.waits.values()) await wait.complete({ outcome: final === 'cancelled' ? 'cancelled' : 'abandoned' });
        turn.waits.clear();
        for (const tool of turn.tools.values()) {
          if (final === 'failed') await tool.fail(new Error(event.data.error ?? 'the turn failed'));
          else await tool.complete({ outcome: final === 'cancelled' ? 'cancelled' : 'abandoned' });
        }
        turn.tools.clear();
      }
      for (const model of turn.models.values()) {
        if (continuing) await model.op.complete({ outcome: 'cancelled' });
        else if (final === 'failed') await model.op.fail(new Error(event.data.error ?? 'the turn failed'));
        else await model.op.complete({ outcome: final === 'cancelled' ? 'cancelled' : 'abandoned' });
        await model.own?.complete();
      }
      turn.models.clear();
      for (const step of turn.steps.values()) await step.complete();
      turn.steps.clear();
      if (!continuing && turn.op && !turn.op.finished) {
        if (final === 'failed') await turn.op.complete({ outcome: final, failed: true, error: new Error(event.data.error ?? 'the turn failed') });
        else await turn.op.complete({ outcome: final });
      }
      if (continuing && turn.op && !turn.op.finished) await parkTools(tracing, turn);
      await tracing.flush();
      return;
    }
    if (!turn.op || turn.op.finished || session.ended) return;
    const op = turn.op;
    const stepKey = `${event.executionId}:${event.stepIndex}`;
    switch (event.type) {
      case 'step.start': {
        if (!event.executionId || event.stepIndex < 1) return;
        turn.steps.set(stepKey, await tracing.step(op, turn.id, { execution: event.executionId, stepIndex: event.stepIndex - 1 }));
        return;
      }
      case 'step.end': {
        await turn.steps.get(stepKey)?.complete();
        turn.steps.delete(stepKey);
        return;
      }
      case 'model.start': {
        const e = event;
        // A call outside a step (a title, a compaction) is a step of its own.
        const step = turn.steps.get(stepKey);
        const own = step ? undefined : await tracing.step(op, turn.id);
        const model = await (step ?? own)!.modelCall(modelOptions(e.data));
        turn.models.set(`${event.executionId}:${e.data.modelCallId}`, { op: model, step: (step ?? own)!, ...(own ? { own } : {}) });
        return;
      }
      case 'model.end': {
        const e = event;
        const key = `${event.executionId}:${e.data.modelCallId}`;
        const model = turn.models.get(key);
        turn.models.delete(key);
        if (!model) return;
        if (e.data.provider) model.op.attributes({ 'gen_ai.provider.name': e.data.provider });
        if (typeof e.data.usage?.reasoningTokens === 'number' && Number.isFinite(e.data.usage.reasoningTokens)) model.step.attributes({ 'agent.usage.reasoning_tokens': e.data.usage.reasoningTokens });
        if (e.data.cancelled) await model.op.complete({ outcome: 'cancelled' });
        else if (e.data.error !== null) await model.op.fail(new Error(e.data.error ?? 'the model call failed'));
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
        // A call an earlier execution started (a rerun after a crash) is the same call.
        const tool = op.findTool(callId) ?? (await (turn.steps.get(stepKey) ?? [...turn.steps.values()].at(-1))?.tool({ callId, name, arguments: parse(e.data.call.argumentsJson) }));
        if (tool) turn.tools.set(callId, tool);
        await parkTools(tracing, turn);
        return;
      }
      case 'tool.end': {
        const e = event;
        const tool = turn.tools.get(e.data.call.callId) ?? op.findTool(e.data.call.callId);
        if (!tool) return;
        if (e.data.status === 'cancelled') {
          // Only turn.end knows whether cancellation was a user action or a handoff.
          turn.tools.set(e.data.call.callId, tool);
          return;
        }
        turn.tools.delete(e.data.call.callId);
        if (e.data.status === 'success') await tool.complete({ outcome: 'completed', output: parse(e.data.output) });
        else await tool.fail(new Error(e.data.error ?? 'the tool call failed'));
        await parkTools(tracing, turn);
        return;
      }
      case 'wait.start': {
        const e = event;
        const tool = e.data.callId ? (turn.tools.get(e.data.callId) ?? op.findTool(e.data.callId)) : undefined;
        const approval = await tool?.approval({ requestId: String(e.data.requestId), ...(e.data.prompt ? { request: e.data.prompt } : {}) });
        if (approval) turn.waits.set(String(e.data.requestId), approval);
        await parkTools(tracing, turn);
        return;
      }
      case 'wait.end': {
        const e = event;
        const approval = turn.waits.get(String(e.data.requestId));
        if (e.data.outcome === 'cancelled') return;
        turn.waits.delete(String(e.data.requestId));
        await approval?.complete({ outcome: APPROVAL[e.data.outcome] ?? e.data.outcome });
        await parkTools(tracing, turn);
        return;
      }
    }
  }

  return {
    get captureContent() {
      const decision = agentRunsHook()?.capture(options.audience);
      return decision?.recordInputs === true || decision?.recordOutputs === true;
    },
    onEvent(event) {
      const hook = agentRunsHook();
      if (!hook) return;
      if (event.turnId === null && !(event.type === 'journal.error' && event.data.fenced)) return;
      const session = sessionFor(event, hook);
      if (!session) return;
      // libfx does not wait for a hook: each session's events are recorded one at a time, in order.
      const next: Promise<void> = session.queue
        .then(async () => {
          try {
            if (event.type === 'turn.end' && (!session.turn || (session.turn.id === event.turnId && session.turn.executionId === event.executionId))) session.ended = true;
            const tracing = await session.tracing;
            if (tracing) await record(session, tracing, event);
            else if (event.type === 'turn.start') session.ended = false;
          } finally {
            // A handed-off turn is checkpointed too: a later execution reacquires it.
            if (session.queue === next && session.ended && sessions.get(event.sessionId) === session) sessions.delete(event.sessionId);
          }
        })
        .catch(report);
      session.queue = next;
      pending.add(next);
      void next.then(() => pending.delete(next));
      keepAlive(next);
    },
    onError: report,
    async flush() {
      const current = [...sessions.values()];
      await Promise.all([...pending]);
      await Promise.all(current.map(async (s) => (await s.tracing)?.flush()));
    },
  };
}

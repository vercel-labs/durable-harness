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
  /** Settles once every event so far is recorded and its turn's checkpoint stored. */
  flush(): Promise<void>;
}

/** One turn of a session as its events arrive: its operation and what is open in it. */
interface TurnTrace {
  readonly id: string;
  readonly op: TurnOperation | undefined;
  readonly steps: Map<string, AttemptOperation>;
  readonly models: Map<string, { op: ModelOperation; own?: AttemptOperation }>;
  readonly tools: Map<string, ToolOperation>;
  readonly waits: Map<string, ApprovalOperation>;
}

interface SessionTrace {
  readonly tracing: Promise<SessionTracing | undefined>;
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
 * that libfx yields, pauses or hands to another worker stays open: the
 * execution that resumes it (in any process) continues the same turn from
 * its checkpoint, its steps numbered by that execution. Nothing is recorded
 * until `agentRuns()` is registered.
 */
export function agentRunsHooks(options: FxAgentRunsOptions = {}): FxAgentRunsHooks {
  const sessions = new Map<string, SessionTrace>();
  let world: Promise<World> | undefined;
  let pending: Promise<void> = Promise.resolve();
  const report = (error: unknown) => console.warn('[durable-harness] Agent Runs: an fx hook event was not recorded', error instanceof Error ? error.message : error);

  /** The session's trace, found or made at once, so its events queue in the order they came. */
  function sessionFor(id: string, hook: AgentRunsHook): SessionTrace {
    let session = sessions.get(id);
    if (!session) {
      world ??= options.world ? Promise.resolve(options.world) : import('./world.ts').then((m) => m.getWorld());
      const tracing = world.then((w) => hook.session({ world: w, runId: id, agentName: options.name ?? 'fx', ...(options.audience ? { audience: options.audience } : {}) }));
      session = { tracing: tracing.catch((error: unknown) => (report(error), undefined)), queue: Promise.resolve() };
      sessions.set(id, session);
    }
    return session;
  }

  async function record(session: SessionTrace, tracing: SessionTracing, event: FxHookEvent): Promise<void> {
    if (event.type === 'turn.start' && event.turnId) {
      const e = event;
      // Each execution asks again: the library continues the turn from its checkpoint, which another process may have moved on.
      const op = await tracing.turn({ turnId: e.turnId!, sequence: 0, ...(e.data.input ? { title: e.data.input } : {}) });
      session.turn = { id: e.turnId!, op, steps: new Map(), models: new Map(), tools: new Map(), waits: new Map() };
      return;
    }
    const turn = session.turn;
    if (!turn || turn.id !== event.turnId || !turn.op || turn.op.finished) return;
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
        turn.models.set(`${event.executionId}:${e.data.modelCallId}`, { op: model, ...(own ? { own } : {}) });
        return;
      }
      case 'model.end': {
        const e = event;
        const key = `${event.executionId}:${e.data.modelCallId}`;
        const model = turn.models.get(key);
        turn.models.delete(key);
        if (!model) return;
        if (e.data.error !== null || e.data.cancelled) await model.op.fail(new Error(e.data.error ?? 'the model call was cancelled'));
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
        return;
      }
      case 'tool.end': {
        const e = event;
        const tool = turn.tools.get(e.data.call.callId) ?? op.findTool(e.data.call.callId);
        turn.tools.delete(e.data.call.callId);
        if (!tool) return;
        if (e.data.status === 'success') await tool.complete({ outcome: 'completed', output: parse(e.data.output) });
        else await tool.fail(new Error(e.data.error ?? `the tool call ${e.data.status === 'cancelled' ? 'was cancelled' : 'failed'}`));
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
      case 'turn.end': {
        const e = event;
        for (const step of turn.steps.values()) await step.complete();
        turn.steps.clear();
        const outcome = e.data.outcome;
        // Yielded, paused, handed off or fenced: the turn goes on in another execution.
        if (outcome === 'completed' || outcome === 'cancelled') await op.complete({ outcome });
        else if (outcome === 'failed') await op.complete({ outcome, failed: true, error: new Error(e.data.error ?? 'the turn failed') });
        await tracing.flush();
        return;
      }
    }
  }

  return {
    // The library records content only where the session's audience allows.
    captureContent: true,
    onEvent(event) {
      const hook = agentRunsHook();
      if (!hook) return;
      const session = sessionFor(event.sessionId, hook);
      // libfx does not wait for a hook: each session's events are recorded one at a time, in order.
      const next: Promise<void> = session.queue
        .then(async () => {
          const tracing = await session.tracing;
          if (tracing) await record(session, tracing, event);
          // A finished turn with nothing queued behind it leaves nothing to keep: the session's next turn starts afresh.
          if (session.queue === next && session.turn?.op?.finished && sessions.get(event.sessionId) === session) sessions.delete(event.sessionId);
        })
        .catch(report);
      session.queue = next;
      pending = pending.then(() => next);
      keepAlive(next);
    },
    onError: report,
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

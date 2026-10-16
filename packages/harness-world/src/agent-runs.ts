import type { World } from '@workflow/world';
import type { AttemptOperation, ModelCallReturn, ModelOperation, TurnOperation } from '@vercel/agent-runs';
import type { DriveContext, Harness } from './types.ts';

export type { ContentPart } from '@vercel/agent-runs';

/** What a model call records when it starts: the model, and (when content is captured) the request. */
export type ModelOptions = Parameters<AttemptOperation['modelCall']>[0];
/** What a model call records when it completes (`model.complete({ outcome, result })`). */
export type ModelResult = Omit<ModelCallReturn<unknown>, 'result'>;

/** Who may read a session's trace content in Agent Runs: `public` records model and tool content, the others metadata only. */
export type TraceAudience = 'public' | 'private' | 'unknown';

/**
 * A request's tool definitions as a trace keeps them: each tool's type, id,
 * name, description and input schema, whichever API it came in (Anthropic's
 * `input_schema`, OpenAI's `parameters` or `function`, the AI SDK's
 * `inputSchema`). Nothing else is kept: a provider's tool can carry its own
 * credentials (an MCP tool's `authorization` and `headers`), which belong in
 * no span and no checkpoint.
 */
export function toolDefinitions(tools: readonly unknown[]): Record<string, unknown>[] {
  const text = (v: unknown) => (typeof v === 'string' ? v : undefined);
  return tools.flatMap((tool) => {
    if (tool === null || typeof tool !== 'object') return [];
    const t = tool as Record<string, unknown>;
    const fn = t.function !== null && typeof t.function === 'object' ? (t.function as Record<string, unknown>) : undefined;
    const name = text(t.name) ?? text(fn?.name) ?? text(t.id);
    if (name === undefined) return [];
    const description = text(t.description) ?? text(fn?.description);
    const inputSchema = t.inputSchema ?? t.input_schema ?? t.parameters ?? fn?.parameters;
    return [{ ...(text(t.type) ? { type: t.type } : {}), ...(text(t.id) ? { id: t.id } : {}), name, ...(description ? { description } : {}), ...(inputSchema !== undefined ? { inputSchema } : {}) }];
  });
}

export const traceAudienceOf = (value: unknown): TraceAudience | undefined =>
  value === 'public' || value === 'private' || value === 'unknown' ? value : undefined;

/** A model call recorded as its own step of the open turn (an Agent Runs `agent.step`). */
export interface TracedModelCall {
  readonly model: ModelOperation;
  /** The step the call belongs to: the tool calls its response asks for join it. */
  readonly attempt: AttemptOperation;
}

/**
 * Agent Runs tracing for one drive (`DriveContext.tracing`), when
 * durable-harness's `agentRuns()` is registered in this process.
 */
export interface DriveTracing {
  /**
   * Record a model call. Inside a `model` effect's body it is that effect's
   * step, which is then not recorded again from the effect; anywhere else
   * (a CLI's request through the Gateway proxy) it starts a step of its own.
   * Undefined when the drive has no open turn, or the turn is not traced.
   * Complete or fail `model`; the step ends with the next model call or the
   * drive.
   */
  modelCall(options: ModelOptions): Promise<TracedModelCall | undefined>;
}

export interface SessionTracingInput {
  readonly world: World;
  readonly runId: string;
  /** Names the agent (`invoke_agent <agentName>`). */
  readonly agentName: string;
  readonly audience?: TraceAudience;
}

/**
 * Agent Runs turns for a session that is not driven by the kernel (pi-durable,
 * libfx's own durable agent), checkpointed with the session.
 */
export interface SessionTracing {
  /** The turn, started if this is its first operation, else continued where an earlier process left it. */
  turn(input: { readonly turnId: string; readonly sequence: number; readonly title?: string }): Promise<TurnOperation | undefined>;
  /** A saved turn under the current capture policy, without starting one. */
  resume(turnId: string): Promise<TurnOperation | undefined>;
  /**
   * A new step of `turn`, for a model call: numbered in this process, its
   * attempt this process's number for the turn (recorded before it is
   * used), so no step of another process, before or after, is the same one.
   */
  step(turn: TurnOperation, turnId: string): Promise<AttemptOperation>;
  /** Wait until every checkpoint this process wrote is stored. */
  flush(): Promise<void>;
}

/** What `agentRuns().register()` installs. The kernel and adapters reach it only through this symbol, so they load no tracing code. */
export interface AgentRunsHook {
  drive(harness: Harness, ctx: DriveContext): Promise<void>;
  session(input: SessionTracingInput): SessionTracing;
}

export const AGENT_RUNS = Symbol.for('durable-harness.agent-runs');

export function agentRunsHook(): AgentRunsHook | undefined {
  return (globalThis as { [AGENT_RUNS]?: AgentRunsHook })[AGENT_RUNS];
}

export function tracedDrive(harness: Harness, ctx: DriveContext): Promise<void> {
  return agentRunsHook()?.drive(harness, ctx) ?? harness.drive(ctx);
}

import type { World } from '@workflow/world';
import type { AttemptOperation, CaptureDecision, ModelCallReturn, ModelOperation, TraceErrorHandler, TurnOperation } from '@vercel/agent-runs';
import type { DriveContext, Harness } from './types.ts';

export type { ContentPart } from '@vercel/agent-runs';

/** What a model call records when it starts: the model, and (when content is captured) the request. */
export type ModelOptions = Parameters<AttemptOperation['modelCall']>[0];
/** What a model call records when it completes (`model.complete({ outcome, result })`). */
export type ModelResult = Omit<ModelCallReturn<unknown>, 'result'>;

/** Who may read a session's trace content in Agent Runs: `public` records model and tool content, the others metadata only. */
export type TraceAudience = 'public' | 'private' | 'unknown';

export const traceAudienceOf = (value: unknown): TraceAudience | undefined =>
  value === 'public' || value === 'private' || value === 'unknown' ? value : undefined;

/** Only tool definitions belong in traces, never provider credentials or transport options. */
export function modelTools(input: unknown): { name: string; description?: string; inputSchema?: unknown }[] {
  if (!Array.isArray(input)) return [];
  return input.flatMap((value) => {
    if (!value || typeof value !== 'object') return [];
    const tool = value as Record<string, unknown>;
    const definition = (tool.function && typeof tool.function === 'object' ? tool.function : tool) as Record<string, unknown>;
    if (typeof definition.name !== 'string') return [];
    const schema = definition.inputSchema ?? definition.input_schema ?? definition.parameters;
    return [{ name: definition.name, ...(typeof definition.description === 'string' ? { description: definition.description } : {}), ...(schema !== undefined ? { inputSchema: schema } : {}) }];
  });
}

/** Provider options and replay metadata are not conversation content. */
export function modelMessages(input: readonly unknown[]): unknown[] {
  const copy = (value: unknown): unknown => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return value;
    const result = { ...value as Record<string, unknown> };
    delete result.providerOptions;
    delete result.providerMetadata;
    return result;
  };
  return input.map(value => {
    const message = copy(value) as Record<string, unknown>;
    if (message && Array.isArray(message.content)) message.content = message.content.map(copy);
    return message;
  });
}

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

/** An interrupted tool and approvals that a native engine may not mention again after recovery. */
export interface ParkedTraceTool {
  readonly callId: string;
  readonly approvals: readonly string[];
}

/**
 * Agent Runs turns for a session that is not driven by the kernel (pi-durable,
 * libfx's own durable agent), checkpointed with the session.
 */
export interface SessionTracing {
  /** The turn, started if this is its first operation, else continued where an earlier process left it. */
  turn(input: { readonly turnId: string; readonly sequence?: number; readonly title?: string }): Promise<TurnOperation | undefined>;
  /** A saved turn under the current capture policy, without starting one. */
  resume(turnId: string): Promise<TurnOperation | undefined>;
  /**
   * A new step of `turn`, for a model call. Its attempt is a number for the
   * turn recorded before it is used, so no step of another process, before
   * or after, is the same one: this process's number, the step numbered in
   * this process; or, with `at`, the number of the execution that runs it
   * (an engine's run or resume of the turn), the step numbered in it.
   */
  step(turn: TurnOperation, turnId: string, at?: { readonly execution: string; readonly stepIndex: number }): Promise<AttemptOperation>;
  /** Tool identities retained with the turn's checkpoint, without their content. */
  parkedTools(turnId: string): Promise<readonly ParkedTraceTool[]>;
  /** Replace the tool identities a resumed engine may silently skip. */
  parkTools(turnId: string, tools: readonly ParkedTraceTool[]): Promise<void>;
  /** Wait for this session's queued checkpoint writes, within the tracing I/O budget. */
  flush(): Promise<void>;
}

/** What `agentRuns().register()` installs. The kernel and adapters reach it only through this symbol, so they load no tracing code. */
export interface AgentRunsHook {
  capture(audience?: TraceAudience): CaptureDecision;
  readonly onError: TraceErrorHandler;
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

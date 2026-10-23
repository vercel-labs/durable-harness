import {
  Agent,
  type AgentEvent,
  type AgentMessage,
  type AgentTool,
  type AgentToolResult,
  type StreamFn,
  type ThinkingLevel,
} from '@earendil-works/pi-agent-core';
import {
  type AssistantMessage,
  createAssistantMessageEventStream,
  type Message,
  type Model,
  type ToolCall,
  type ToolResultMessage,
  type UserMessage,
} from '@earendil-works/pi-ai';
import {
  type DriveContext,
  type EffectError,
  EffectFailure,
  type Harness,
  type HostTool,
  type HostToolIO,
  type Lane,
  lastUnavailable,
  type PromptDocs,
  promptDocsText,
  Restart,
  type SessionState,
  type Submission,
  type UnavailableInput,
  Yield,
} from 'harness-world';
import { piModelOptions, piModelResult, piStepAttributes } from './agent-runs.ts';

export interface PiHarnessOptions {
  /** The pi-ai model to drive. */
  model: Model<any>;
  /** The provider stream function, e.g. `models.streamSimple.bind(models)`. */
  streamFn: StreamFn;
  systemPrompt?: string;
  /**
   * Tools: Pi `AgentTool`s or harness-neutral `HostTool`s. `replay`
   * (`'safe' | 'never'`, Pi's own field) is the crash policy for an
   * interrupted call; it defaults to `'never'`.
   */
  tools?: (AgentTool<any> | HostTool)[];
  thinkingLevel?: ThinkingLevel;
  /** Attempts per model call when the provider returns an error. */
  modelAttempts?: number;
  /** Resolve the provider credential per call (e.g. a short-lived OIDC token). */
  getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
  /**
   * How often the inbox is read while the agent runs. Default 500ms; 5s
   * when the kernel offers live hints (`ctx.onHint`), which trigger a read
   * the moment a submission commits, so the poll is only a fallback.
   */
  inboxPollMs?: number;
  /**
   * Documents to show the model, at the start of the latest user message of
   * every model call as they are at that call (`docsPrompt`), or a function
   * of the call's documents that returns the text to show. Each earlier user
   * message keeps what the last call that had it as its latest showed (the
   * model step records it), so every request extends the one before it: the
   * provider's prompt cache keeps the conversation up to the latest user
   * message when a document changes, and all of it when none did. Off by
   * default: documents are app state, and reach the model only where the
   * app puts them (here, or through tools such as `docTools`). Read inside
   * the model call's step, so a replay never depends on a later value.
   */
  promptDocs?: PromptDocs;
}

/**
 * What a model step's output holds beside the assistant message, with
 * `promptDocs`: the documents' text the call showed at the start of the user
 * message at index `at` of its request, when it differs from what that
 * message showed in the call before (none: `''`).
 */
interface ShownDocs {
  at: number;
  text: string;
}
type ModelOutput = AssistantMessage & { promptDocs?: ShownDocs };

/** A model step's assistant message, without what the step records beside it. */
function messageOf(output: unknown): AssistantMessage {
  const { promptDocs: _, ...message } = output as ModelOutput;
  return message as AssistantMessage;
}

/** What a `model` step records: the messages this call placed, and its size. */
interface ModelInput {
  /** Messages appended to the transcript since the previous model call. */
  appended: Message[];
  /** Total messages in the request context (a replay divergence check). */
  contextLength: number;
  model: string;
  /** First model call after `agent.prompt`, after the preceding `idle`. */
  turnStart?: boolean;
}

/**
 * Pi on a World.
 *
 * The transcript is an exact fold of the log: each `model` step records the
 * messages it placed (`appended`: user submissions, tool results, system
 * messages) and outputs the assistant message, so
 * `transcript = concat(appended_i, output_i)`. On every invocation the
 * adapter rebuilds Pi's `Agent` at the context of the last model call and
 * continues, so the last turn replays against memoized effects and the rest
 * of it runs for real.
 */
export function piHarness(options: PiHarnessOptions): Harness {
  return {
    name: 'pi',
    modelCalls: 'self',
    drive: (ctx) => new PiDrive(options, ctx).run(),
  };
}

class PiDrive {
  private agent!: Agent;
  /** Messages already recorded by committed model steps. */
  private recorded = 0;
  private modelOrdinal = 0;
  private turnStart?: boolean;
  /** Why this drive must stop, set from inside Pi callbacks that cannot throw. */
  private stop?: Error;
  /** The drive-end hooks of its host tools (`afterDrive`), each once. */
  private readonly afterDrive = new Set<(ctx: DriveContext) => Promise<void>>();
  /** Submissions handed to Pi but not yet placed by a model step. */
  private handed = new Map<string, Submission>();

  constructor(
    private readonly options: PiHarnessOptions,
    private readonly ctx: DriveContext
  ) {}

  async run(): Promise<void> {
    const state = this.ctx.state();
    const models = state.effectOrder.filter((e) => e.name === 'model');
    // A turn that an `unavailable` fact failed (harness-world's
    // unavailable.ts: the harness could not run, or kept failing) is over:
    // the next message starts a turn of its own instead of resuming it.
    const failedAt = lastUnavailable(state);
    const last = models.at(-1);
    const ended = last !== undefined && failedAt > state.effectOrder.indexOf(last);
    const restore: AgentMessage[] = [];
    let lastAppended = 0;
    for (const [i, record] of models.entries()) {
      const input = record.input as ModelInput;
      restore.push(...input.appended);
      lastAppended = input.appended.length;
      // Every model step but the last contributes its output; the last is
      // replayed so Pi re-enters its turn and runs whatever is left of it.
      if (i < models.length - 1 && record.output !== undefined) restore.push(messageOf(record.output));
    }
    // The tool calls the failed turn's last response asked for, answered:
    // what a call recorded, else that it never ran. A provider refuses a
    // tool call without its result, and the next message comes after them.
    let answered: ToolResultMessage[] = [];
    if (ended && last.status === 'completed' && last.output !== undefined) {
      const response = messageOf(last.output);
      restore.push(response);
      answered = toolResultsOf(response, state, state.effectOrder[failedAt]!);
    }
    // The last model step is re-entered, so what it appended belongs to it
    // again: a memoized replay records the same messages, and a retry of a
    // released call (a takeover) re-records them instead of an empty list,
    // which would drop them from every later fold of the transcript. After a
    // failed turn nothing is re-entered: the next model call is a new step,
    // and the tool results above are the first of what it appends.
    this.recorded = ended ? restore.length : restore.length - lastAppended;
    restore.push(...answered);
    this.modelOrdinal = ended ? models.length : Math.max(0, models.length - 1);
    this.turnStart = !ended && last ? (last.input as ModelInput).turnStart : undefined;

    this.agent = new Agent({
      initialState: {
        systemPrompt: this.options.systemPrompt ?? '',
        model: this.options.model,
        thinkingLevel: this.options.thinkingLevel ?? 'off',
        tools: (this.options.tools ?? []).map((t) => {
          if (isHostTool(t) && (t as PiHostTool).afterDrive) this.afterDrive.add((t as PiHostTool).afterDrive!);
          return this.wrapTool(isHostTool(t) ? fromHostTool(t, this.ctx) : t);
        }),
        messages: restore,
      },
      streamFn: (model, context, opts) => this.stream(model, context, opts),
      getApiKey: this.options.getApiKey,
      toolExecution: 'sequential',
      followUpMode: 'all',
      steeringMode: 'all',
    });
    this.agent.subscribe((event) => this.onEvent(event));

    // A submission's hint reads the inbox at once; the poll is the fallback.
    const offHint = this.ctx.onHint?.((hint) => {
      if (hint.eventType === 'hook_received' || hint.eventType === 'reconnect') void this.pullInbox();
    });
    const poll = setInterval(() => void this.pullInbox(), this.options.inboxPollMs ?? (offHint ? 5000 : 500));
    // Another driver took the session (a takeover after this one looked
    // dead): stop now, mid-stream or mid-tool, and write nothing more. A
    // budget abort only stops new effects (the kernel refuses them).
    const onAbort = () => {
      const reason = this.ctx.signal.reason;
      // Lost the session, lost a first-writer race (`contended`), or a
      // run-ahead write was rejected (`Restart`): the response in flight is
      // not this drive's to record, so stop it now rather than let a stale
      // model call hold the process's drive slot until it ends.
      if ((reason instanceof Yield && reason.reason !== 'budget') || reason instanceof Restart) this.halt(reason);
    };
    this.ctx.signal.addEventListener('abort', onAbort);
    if (this.ctx.signal.aborted) onAbort();
    try {
      if (models.length > 0 && !ended) {
        const restart = restartOf(state);
        if (restart) this.ctx.live(restart);
        await this.agent.continue();
      }
      this.throwIfStopped();
      for (;;) {
        await this.ctx.refresh();
        const next = this.ctx.pending().filter((s) => !this.handed.has(s.id));
        if (next.length === 0) break;
        for (const s of next) this.handed.set(s.id, s);
        this.turnStart = true;
        await this.agent.prompt(next.map((s) => userMessage(s)));
        this.throwIfStopped();
      }
    } finally {
      clearInterval(poll);
      offHint?.();
      this.ctx.signal.removeEventListener('abort', onAbort);
      // What the tools hold for this drive (a sandbox's link) goes with it.
      for (const end of this.afterDrive) await end(this.ctx).catch(() => {});
    }
  }

  private throwIfStopped(): void {
    if (this.stop) throw this.stop;
  }

  private halt(reason: Error): void {
    this.stop ??= reason;
    this.agent.abort();
  }

  /** Hand newly committed submissions to Pi while it is running. */
  private async pullInbox(): Promise<void> {
    if (!this.agent.state.isStreaming) return;
    try {
      await this.ctx.refresh();
    } catch {
      return;
    }
    for (const s of this.ctx.pending()) {
      if (this.handed.has(s.id)) continue;
      this.handed.set(s.id, s);
      // Pi's own semantics: steering lands at the next model boundary,
      // follow-ups (and prompts sent mid-run) when the agent would stop.
      if (s.kind === 'steer') this.agent.steer(userMessage(s));
      else this.agent.followUp(userMessage(s));
    }
  }

  private stream: StreamFn = (model, context, opts) => {
    const out = createAssistantMessageEventStream();
    const key = `model:${this.modelOrdinal++}`;
    const messages = context.messages;
    const appended = messages.slice(this.recorded);
    const placed = appended.flatMap((m) =>
      m.role === 'user' ? this.claimSubmission(m) : []
    );
    const replaying = this.ctx.isMemoized(key);
    const turnStart = this.turnStart;
    this.turnStart = false;

    const run = async (): Promise<AssistantMessage> => {
      if (this.stop) throw this.stop;
      return this.ctx.effect(
        key,
        {
          name: 'model',
          replay: 'safe',
          placed,
          input: {
            appended,
            contextLength: messages.length,
            model: `${model.provider}/${model.id}`,
            ...(turnStart !== undefined ? { turnStart } : {}),
          } satisfies ModelInput,
        },
        async (io): Promise<ModelOutput> => {
          const attempts = this.options.modelAttempts ?? 3;
          // The prompt hook: the documents as this call pinned them at the
          // start of the latest prompt, and what earlier calls showed on the
          // earlier ones (so the request extends the previous one).
          const hook = this.options.promptDocs;
          const shown = hook ? this.docsShown() : new Map<number, string>();
          const at = messages.findLastIndex((m) => m.role === 'user');
          const docs = promptDocsText(hook, io.docs) ?? '';
          const request = hook ? withDocuments(context, shown, at, docs) : context;
          const record = (message: AssistantMessage): ModelOutput =>
            hook && at >= 0 && docs !== (shown.get(at) ?? '') ? { ...message, promptDocs: { at, text: docs } } : message;
          const traced = await this.ctx.tracing?.modelCall(piModelOptions(model, request.messages));
          let last: AssistantMessage | undefined;
          try {
            for (let attempt = 1; attempt <= attempts; attempt++) {
              const inner = await this.options.streamFn(model, request, opts);
              let first = attempt > 1;
              for await (const event of inner) {
                if (!first && FIRST_OUTPUT.has(event.type)) {
                  first = true;
                  this.ctx.mark?.('model.first_token', { key, kind: event.type });
                }
                // The terminal event is pushed once the step commits, so Pi
                // never sees a response finish that the log does not have.
                // Only the first attempt streams: a retry must not open a
                // second partial message in Pi's context.
                if (event.type === 'done' || event.type === 'error' || attempt > 1) continue;
                out.push(event);
                if (event.type === 'text_delta') this.ctx.live({ type: 'text', delta: event.delta });
              }
              last = await inner.result();
              // Halted mid-stream: the cut-off response is not this step's outcome.
              if (this.stop) throw this.stop;
              if (last.stopReason !== 'error') {
                traced?.attempt.attributes(piStepAttributes(last));
                await traced?.model.complete({ outcome: 'completed', result: piModelResult(last) });
                return record(last);
              }
              await sleep(500 * attempt);
            }
            throw new Error(last?.errorMessage ?? 'model call failed');
          } catch (error) {
            await traced?.model.fail(error);
            throw error;
          }
        }
      );
    };

    run().then(
      (output) => {
        const message = messageOf(output);
        if (replaying) {
          const recorded = (
            this.ctx.state().effects.get(key)?.input as ModelInput | undefined
          )?.contextLength;
          if (recorded !== undefined && recorded !== messages.length) {
            this.halt(new Restart(`${key} replayed with a different context`));
          }
        }
        this.recorded = messages.length + 1;
        out.push({ type: 'done', reason: doneReason(message), message });
      },
      (err) => {
        if (err instanceof Yield || err instanceof Restart) this.halt(err);
        const message = errorMessage(model, err);
        out.push({ type: 'error', reason: 'error', error: message });
      }
    );
    return out;
  };

  /**
   * The documents each user message of the transcript showed, by index: what
   * the last model call that showed any there recorded (`ShownDocs`).
   */
  private docsShown(): Map<number, string> {
    const shown = new Map<number, string>();
    for (const r of this.ctx.state().effectOrder) {
      if (r.name !== 'model' || r.status !== 'completed') continue;
      const d = (r.output as ModelOutput | undefined)?.promptDocs;
      if (d && Number.isSafeInteger(d.at) && typeof d.text === 'string') shown.set(d.at, d.text);
    }
    return shown;
  }

  private claimSubmission(message: Message): string[] {
    const text = typeof message.content === 'string' ? message.content : undefined;
    for (const [id, s] of this.handed) {
      if (s.text === text) {
        this.handed.delete(id);
        return [id];
      }
    }
    return [];
  }

  private wrapTool(tool: AgentTool<any>): AgentTool<any> {
    return {
      ...tool,
      execute: async (toolCallId, params, signal, onUpdate) => {
        if (this.stop) throw this.stop;
        const key = `tool:${toolCallId}`;
        const fresh = !this.ctx.isMemoized(key);
        if (fresh) this.ctx.live({ type: 'tool_start', name: tool.name, args: params });
        // The pre-intent phase (a sandbox's attach): before the call's intent
        // is written. What it resolves with is the call's outcome at once (it
        // did not run); a rejection stops the drive.
        const before = fresh ? (tool as HostBacked).beforeIntent : undefined;
        let notRun: Error | undefined;
        if (before) {
          try {
            notRun = await before(this.ctx);
          } catch (err) {
            this.halt(err instanceof Error ? err : new Error(String(err)));
            throw err;
          }
        }
        try {
          const result = await this.ctx.effect(
            key,
            {
              name: `tool.${tool.name}`,
              replay: tool.replay ?? 'never',
              input: params,
              ...((tool as { requires?: Lane }).requires ? { requires: (tool as { requires?: Lane }).requires } : {}),
            },
            (io) => {
              if (notRun) throw notRun;
              const combined = signal ? AbortSignal.any([signal, io.signal]) : io.signal;
              const update = (partial: AgentToolResult<any>) => {
                io.progress(partial);
                onUpdate?.(partial);
              };
              // A host tool gets the effect's io (park, settlement) as well.
              const host = (tool as HostBacked).executeHost;
              return host
                ? host(params, {
                    signal: combined,
                    stepId: io.stepId,
                    park: io.park,
                    ...(io.settlement ? { settlement: io.settlement } : {}),
                    ...(io.chaosArmedMs !== undefined ? { chaosArmedMs: io.chaosArmedMs } : {}),
                    ...(io.docs ? { docs: io.docs } : {}),
                    onUpdate: update,
                  })
                : tool.execute(toolCallId, params, combined, update);
            }
          );
          if (fresh) this.ctx.live({ type: 'tool_end', name: tool.name, result });
          return result as AgentToolResult<any>;
        } catch (err) {
          if (err instanceof Yield || err instanceof Restart) {
            this.halt(err);
            throw err;
          }
          if (err instanceof EffectFailure) throw new Error(describeFailure(err));
          // Pi turns the message into the tool result, and providers reject
          // an empty error result, which would poison every later request.
          if (err instanceof Error && !err.message) {
            throw new Error(`${tool.name} failed: ${err.name || 'error'} with no message`);
          }
          throw err;
        }
      },
    };
  }

  private onEvent(event: AgentEvent): void {
    if (event.type === 'agent_end') this.ctx.live({ type: 'idle' });
  }
}

/** Replace only the interrupted turn's text, keeping its committed model steps. */
function restartOf(state: SessionState): { type: 'restart'; text: string; parts: string[] } | undefined {
  const models = state.effectOrder.slice(lastUnavailable(state) + 1).filter((record) => record.name === 'model');
  const finished = (record: typeof models[number]) =>
    record.status === 'completed' && record.output !== undefined &&
    !messageOf(record.output).content.some((part) => part.type === 'toolCall');
  const last = models.at(-1);
  if (!last || finished(last)) return;
  let parts: string[] = [];
  for (const [i, record] of models.entries()) {
    const { turnStart } = record.input as ModelInput;
    // Older logs predate the explicit boundary: a tool-less answer ended
    // their previous turn. New logs also retain text before a steer or
    // follow-up that continued the same Agent invocation without an idle.
    if (turnStart === true || (turnStart === undefined && i > 0 && finished(models[i - 1]!))) parts = [];
    if (record.status !== 'completed' || record.output === undefined) continue;
    const text = messageOf(record.output).content.flatMap((part) => part.type === 'text' ? [part.text] : []).join('');
    if (text) parts.push(text);
  }
  // Pi's live text deltas concatenate across model calls without separators.
  return { type: 'restart', text: parts.join(''), parts };
}

/** Stream events that count as the model's first output (TTFT). */
const FIRST_OUTPUT = new Set(['text_delta', 'thinking_delta', 'toolcall_start', 'toolcall_delta']);

function isHostTool(tool: AgentTool<any> | HostTool): tool is HostTool {
  return 'inputSchema' in tool && !('parameters' in tool);
}

/**
 * What a `HostTool` may carry for Pi, beside its fields (harness-world-hands'
 * Pi coding tools do): `prepareArguments`, pi-agent-core's, run on the
 * model's arguments before they are validated; `isErrorResult`, whether a
 * call that succeeded is an error result for the model (Pi's bash: a
 * nonzero exit; the call's changes are kept either way); and a runtime's
 * phases (harness-world-hands' `PhasedTool`, on a sandbox):
 * `beforeIntent(ctx)`, run before the call's intent is written (not for a
 * call the log already answers), resolving with an error the call then fails
 * with without running, or rejecting to stop the drive; `afterDrive(ctx)`,
 * run once when the drive ends.
 */
interface PiHostTool extends HostTool {
  prepareArguments?: (args: unknown) => unknown;
  isErrorResult?: (text: string) => boolean;
  beforeIntent?: (ctx: DriveContext) => Promise<Error | undefined>;
  afterDrive?: (ctx: DriveContext) => Promise<void>;
}

/** A Pi tool built from a `HostTool`: callable with the effect's io too, with its pre-intent phase. */
type HostBacked = AgentTool<any> & {
  beforeIntent?: PiHostTool['beforeIntent'];
  executeHost?: (
    params: unknown,
    io: Omit<HostToolIO, 'ctx' | 'progress'> & { onUpdate?: (partial: AgentToolResult<any>) => void }
  ) => Promise<AgentToolResult<any>>;
};

function fromHostTool(tool: PiHostTool, ctx: DriveContext): AgentTool<any> {
  const run = async (
    params: unknown,
    io: Omit<HostToolIO, 'ctx' | 'progress'> & { onUpdate?: (partial: AgentToolResult<any>) => void }
  ): Promise<AgentToolResult<any>> => {
    const { onUpdate, ...rest } = io;
    const text = await tool.execute(params, {
      ...rest,
      progress: (chunk) => onUpdate?.({ content: [{ type: 'text', text: JSON.stringify(chunk) }], details: chunk }),
      ctx,
    });
    return { content: [{ type: 'text', text }], details: undefined, ...(tool.isErrorResult?.(text) ? { isError: true } : {}) };
  };
  const backed: HostBacked = {
    name: tool.name,
    label: tool.label ?? tool.name,
    description: tool.description,
    parameters: tool.inputSchema as AgentTool<any>['parameters'],
    replay: tool.replay,
    ...(tool.requires ? { requires: tool.requires } : {}),
    ...(tool.prepareArguments ? { prepareArguments: tool.prepareArguments } : {}),
    ...(tool.beforeIntent ? { beforeIntent: tool.beforeIntent } : {}),
    execute: (_id, params, signal, onUpdate) =>
      run(params, { signal: signal ?? new AbortController().signal, ...(onUpdate ? { onUpdate } : {}) }),
    executeHost: run,
  };
  return backed;
}

function userMessage(s: Submission): Message {
  return { role: 'user', content: s.text ?? '', timestamp: s.receivedAt.getTime() };
}

/**
 * The tool results for `response`'s tool calls, after the turn it is in
 * failed (`fact`, the `unavailable` step): a call's recorded outcome as Pi
 * would have placed it, or, for a call that has none, that it never ran.
 */
function toolResultsOf(response: AssistantMessage, state: SessionState, fact: { output?: unknown; startedAt?: Date }): ToolResultMessage[] {
  const failure = fact.output as Partial<UnavailableInput> | undefined;
  const why = failure?.code ? `${failure.code}: ${failure.message ?? ''}`.trim() : 'the turn failed';
  const timestamp = fact.startedAt?.getTime() ?? 0;
  const calls = response.content.filter((c): c is ToolCall => c.type === 'toolCall');
  return calls.map((call) => {
    const step = state.effects.get(`tool:${call.id}`);
    const base = { role: 'toolResult' as const, toolCallId: call.id, toolName: call.name, timestamp };
    if (step?.status === 'completed') {
      const result = step.output as Partial<AgentToolResult<any>> & { isError?: boolean } | undefined;
      return { ...base, content: result?.content ?? [], details: result?.details, isError: result?.isError === true } as ToolResultMessage;
    }
    const text = step?.status === 'failed' && step.error ? describeError(step.error) : `Not run: the turn ended before this call ran (${why}).`;
    return { ...base, content: [{ type: 'text', text }], details: {}, isError: true } as ToolResultMessage;
  });
}

function describeFailure(err: EffectFailure): string {
  return describeError(err.error);
}

function describeError(error: EffectError): string {
  const { message, interrupted, partial } = error;
  if (!interrupted) return message || 'the tool failed with no message';
  const tail = partial?.length
    ? `\nPartial output before the interruption:\n${JSON.stringify(partial.at(-1))}`
    : '';
  return `${message}${tail}`;
}

function doneReason(message: AssistantMessage) {
  return message.stopReason === 'length' ||
    message.stopReason === 'toolUse' ||
    message.stopReason === 'deferred'
    ? message.stopReason
    : 'stop';
}

function errorMessage(model: Model<any>, err: unknown): AssistantMessage {
  return {
    role: 'assistant',
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: 'error',
    errorMessage: err instanceof Error ? err.message : String(err),
    timestamp: Date.now(),
  } as AssistantMessage;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * The request with the documents at the start of user messages: `latest` on
 * the latest one (index `at`; a user message at the end when there is none),
 * and on each earlier one what an earlier call showed there (`shown`). Near
 * the end, not in the system prompt, where a change would invalidate the
 * provider's cache of the whole conversation; and never taken off a message
 * once a request carried it, so each request starts with the previous one
 * (a prefix cache keyed on the last message, as Anthropic's, otherwise
 * misses every earlier turn). Part of a user message, not a message of its
 * own, so no provider sees two user messages in a row.
 */
function withDocuments(context: Parameters<StreamFn>[1], shown: Map<number, string>, at: number, latest: string): Parameters<StreamFn>[1] {
  const messages = context.messages.map((m, i) => {
    const text = i === at ? latest : shown.get(i);
    if (m.role !== 'user' || !text) return m;
    const user = m as UserMessage;
    return { ...user, content: [{ type: 'text' as const, text }, ...(typeof user.content === 'string' ? [{ type: 'text' as const, text: user.content }] : user.content)] };
  });
  if (at < 0 && latest) messages.push({ role: 'user', content: [{ type: 'text', text: latest }], timestamp: 0 });
  return { ...context, messages };
}

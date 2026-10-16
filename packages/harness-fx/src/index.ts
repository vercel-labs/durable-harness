import {
  type DriveContext,
  EffectFailure,
  type EffectRecord,
  Fatal,
  type Harness,
  type HostTool,
  type HostToolIO,
  type Lane,
  Restart,
  type Submission,
  Yield,
} from 'harness-world';
import type { FxEngineOptions } from 'libfx';

/**
 * libfx's `createFxEngine` (from `libfx` on Node, `libfx/browser` in a tab):
 * one engine in this process, which this adapter drives and records. Not
 * libfx's `createFxAgent`, which since vercel-labs/fx#1180 is libfx's own
 * durable agent: `fxHarness` refuses it (see `createEngine`).
 */
export type CreateFxEngine = (options: FxEngineOptions) => Promise<FxAgent>;
/** @deprecated The old name of `CreateFxEngine`. */
export type CreateFxAgent = CreateFxEngine;

export interface FxAgent {
  prompt(input: string): FxTurn;
  checkpoint(): Promise<Uint8Array>;
  close(): Promise<void>;
}

export interface FxTurn extends AsyncIterable<FxEvent> {
  result: Promise<{ stopReason: string; usage?: unknown }>;
  steer(text: string): Promise<unknown>;
  cancel(): void;
}

export type FxEvent =
  | { type: 'text_delta'; delta: string }
  | { type: 'reasoning_delta'; delta: string }
  | { type: 'tool_start'; id: string; name: string; input?: unknown }
  | { type: 'tool_end'; id: string; name: string; output?: unknown; isError?: boolean }
  | { type: 'user_message'; text: string };

/**
 * A host tool. `HostTool`s from `harness-world-hands` fit as they are,
 * including a sandbox runtime's (`sandboxRuntime`), whose phases the harness
 * runs: `beforeIntent(ctx)` before the call's intent is written (not for a
 * call the log already answers), resolving with an error the call then
 * fails with, without running, or rejecting to stop the drive; and
 * `afterDrive(ctx)` once when the drive ends.
 */
export interface FxTool {
  name: string;
  description: string;
  inputSchema: object;
  /** Crash policy for an interrupted call. Defaults to `'never'`. */
  replay?: 'safe' | 'never';
  /**
   * The least capable lane that can run this tool, for sessions that move
   * between lanes (see `harness-world` lanes). Omitted: anywhere.
   */
  requires?: Lane;
  execute(
    input: any,
    io: { signal: AbortSignal; progress: (chunk: unknown) => void; ctx: DriveContext } & Partial<
      Pick<HostToolIO, 'stepId' | 'park' | 'settlement' | 'chaosArmedMs' | 'docs'>
    >
  ): Promise<unknown>;
  /** The call's pre-intent phase (a sandbox's attach): see the interface comment. */
  beforeIntent?(ctx: DriveContext): Promise<Error | undefined>;
  /** The drive is over (a sandbox's link closes). */
  afterDrive?(ctx: DriveContext): Promise<void>;
}

/** A tool's phases, on an `FxTool` or a `HostTool` that carries them. */
type Phased = Pick<FxTool, 'beforeIntent' | 'afterDrive'>;

/**
 * Where an fx harness gets its engines: `createFxEngine`, or the deprecated
 * `createFxAgent` (the same factory under the name libfx gave it before
 * vercel-labs/fx#1180, which now names libfx's durable agent).
 */
export type FxEngineFactory =
  | { createFxEngine: CreateFxEngine; createFxAgent?: undefined }
  | {
      /** @deprecated Pass `createFxEngine`. Since vercel-labs/fx#1180, libfx's own `createFxAgent` is its durable agent, which this option refuses. */
      createFxAgent: CreateFxEngine;
      createFxEngine?: undefined;
    };

export type FxHarnessOptions = FxEngineFactory & FxHarnessSettings;

export interface FxHarnessSettings {
  /** The AI Gateway credential, or a function resolving one per invocation. */
  apiKey: string | (() => Promise<string> | string);
  model?: string | { id: string; effort?: string; fast?: boolean };
  instructions?: string;
  tools?: (FxTool | HostTool)[];
  /** The network fetch the kernel's model requests ultimately go to. */
  fetch?: typeof fetch;
  /** Extra libfx options (`backend`, `wasm`, `gatewayChatUrl`, ...). */
  agentOptions?: Record<string, unknown>;
  /**
   * Extra libfx options, over `agentOptions`, for an engine that restores a
   * checkpoint from the log: `{ backend: 'wasm' }` where a checkpoint may
   * have been written by a process this one does not trust (a hybrid
   * session's browser tab writes the same run), so its bytes never reach
   * the native addon.
   */
  restoreAgentOptions?: Record<string, unknown>;
  /**
   * How often the inbox is read for steers while a turn runs. Default
   * 500ms; 5s when the kernel offers live hints (`ctx.onHint`), which read
   * the moment a submission commits.
   */
  steeringPollMs?: number;
  /**
   * The libfx version `createFxEngine` comes from. Recorded on every turn and
   * checkpoint, so a process restoring a checkpoint or replaying a tape
   * written by another libfx can say so instead of failing obscurely.
   */
  kernelVersion?: string;
  /** How many times a model request that fails before streaming is retried. Default 3 attempts. */
  modelAttempts?: number;
}

/** What a `model` step records. */
interface ModelInput {
  url: string;
  /** The request body the kernel sent (the AI SDK language-model call). */
  request: unknown;
  digest: string;
  /**
   * The model the request asked for. libfx sends it as a header
   * (`ai-language-model-id`), not in the body, so the digest cannot see it;
   * recording it lets a replay in another process (another lane of a hybrid
   * session) notice it was configured with a different model instead of
   * silently continuing the turn on it. Absent on tapes recorded before it.
   */
  model?: string;
  /**
   * Keys of the `steer` steps this request was the first to carry: where
   * each steer actually landed, observed in the body, rather than where the
   * adapter predicted it would. A steer handed to the kernel while it was
   * already building a request lands in the one after. Absent on older
   * tapes, which fall back to the steer's `beforeRequest`.
   */
  steers?: string[];
}

/** What a `model` step outputs: the raw response plus a readable summary. */
interface ModelOutput {
  status: number;
  /** The complete server-sent-event body, replayed verbatim to the kernel. */
  body: string;
  text: string;
  toolCalls: { toolCallId: string; toolName: string; input: unknown }[];
  finishReason?: string;
}

interface TurnInput {
  submissions: { id: string; text: string }[];
  /**
   * `fxConfigHash` of the process that started the turn: what shapes the
   * request bodies the tape digests (instructions, tools, model, libfx). A
   * replay of the turn in a process with another hash cannot match its
   * tape, so it fails with that reason instead of as a bare divergence.
   * Absent on older logs.
   */
  config?: string;
}

/** What a `checkpoint` step outputs. */
interface CheckpointOutput {
  turn: number;
  stopReason: string;
  usage?: unknown;
  data: string;
  /** `FxHarnessOptions.kernelVersion` of the process that wrote it. */
  kernel?: string;
  /** Set when the turn ended in an error rather than a stop reason. */
  error?: string;
  /**
   * Steers taken during the turn that no model request carried (the turn
   * ended first, or the kernel refused them): the next turn's prompt.
   */
  dropped?: { id: string; text: string }[];
}

interface SteerInput {
  text: string;
  turn: number;
  /**
   * Ordinal (within the turn) of the model request this steer was expected
   * to land before. Where it actually landed is recorded on the model step
   * that carried it (`ModelInput.steers`).
   */
  beforeRequest: number;
}

/**
 * fx on a World.
 *
 * fx's kernel is opaque (Zig, native or Wasm), so the log records its I/O
 * tape instead of its transcript: every model request the kernel makes
 * through `fetch` is a `model` step holding the response body, every host
 * tool call is a `tool.*` step, and each turn ends with a `checkpoint` step
 * holding the kernel's own `checkpoint()` bytes. An invocation restores the
 * last checkpoint and re-runs any unfinished turn against the tape, which
 * the kernel consumes without touching the network. A request that does not
 * match the tape (by body digest) is a divergence and fails loudly.
 */
export function fxHarness(options: FxHarnessOptions): Harness {
  engineFactory(options);
  let config: Promise<string> | undefined;
  return {
    name: 'fx',
    drive: async (ctx) => new FxDrive(options, ctx, await (config ??= fxConfigHash(options))).run(),
  };
}

/** The engine factory in `options`: `createFxEngine`, else the deprecated `createFxAgent`. */
export function engineFactory(options: { createFxEngine?: CreateFxEngine | undefined; createFxAgent?: CreateFxEngine | undefined }): CreateFxEngine {
  const factory = options.createFxEngine ?? options.createFxAgent;
  if (typeof factory !== 'function') throw new TypeError("fxHarness needs createFxEngine: libfx's createFxEngine (from 'libfx', or 'libfx/browser' in a tab)");
  return factory;
}

const DURABLE_AGENT =
  "the engine factory returned libfx's durable agent (libfx's createFxAgent since vercel-labs/fx#1180, whose sessions libfx keeps in a World of its own) instead of an engine: pass libfx's createFxEngine as createFxEngine";

/** libfx's durable agent (`createFxAgent` since vercel-labs/fx#1180) has these; its engine has neither. */
const isDurableAgent = (value: unknown) => !!value && typeof value === 'object' && ('wakeHandler' in value || 'session' in value);

/**
 * One engine from `factory`, refusing what is not an engine. libfx's
 * `createFxAgent` used to be the engine factory; since vercel-labs/fx#1180 it
 * returns, synchronously, a durable agent whose turns run in a session libfx
 * keeps in a World of its own. Driven as an engine, every turn would run
 * there instead of here, and nothing would say so: so a factory that does
 * not return a promise, or whose result looks like that agent, is a TypeError.
 */
export async function createEngine(factory: CreateFxEngine, options: FxEngineOptions): Promise<FxAgent> {
  let made: unknown;
  try {
    made = factory(options);
  } catch (err) {
    // Only a durable agent has a name to collide on (libfx 0.0.14: one configuration per name on a store, in a process).
    if ((err as { code?: unknown } | null)?.code === 'FX_AGENT_NAME_IN_USE') throw new TypeError(`fx: ${DURABLE_AGENT}`);
    throw err;
  }
  if (isDurableAgent(made)) {
    // Closing it lets its name go at once, so the next agent of that name is not refused.
    void (made as { close?: () => Promise<void> }).close?.().catch(() => {});
    throw new TypeError(`fx: ${DURABLE_AGENT}`);
  }
  if (!made || typeof (made as { then?: unknown }).then !== 'function') {
    throw new TypeError("fx: the engine factory must return a promise of an engine, as libfx's createFxEngine does");
  }
  const engine = await (made as Promise<unknown>);
  if (isDurableAgent(engine)) throw new TypeError(`fx: ${DURABLE_AGENT}`);
  return engine as FxAgent;
}

/**
 * A hash of everything in these options that shapes the request bodies
 * fx's kernel sends (and so the tape's digests): instructions, the tool
 * list (names, descriptions, schemas, in order), the model, and the libfx
 * version. Two processes that can replay each other's tapes have the same
 * hash; a hybrid session's lanes compare it before one continues the other.
 */
export async function fxConfigHash(options: FxHarnessSettings): Promise<string> {
  return sha256(
    JSON.stringify({
      instructions: options.instructions ?? null,
      model: options.model ?? null,
      tools: (options.tools ?? []).map((t) => [t.name, t.description, t.inputSchema]),
      kernel: options.kernelVersion ?? null,
    })
  );
}

/** fx's steering envelope: the kernel wraps a steer in the request that first carries it. */
const STEERING = /^<user_steering>\n[^\n]*\n\n([\s\S]*)\n<\/user_steering>$/;

/** Why a drive stops: the kernel's own, or what a tool's pre-intent phase stopped it with (`Unavailable`, the log's). */
type Stop = Yield | Restart | Fatal | Error;

class FxDrive {
  private agent?: FxAgent;
  private turn = 0;
  private request = 0;
  private toolSeen = new Map<string, number>();
  private active?: FxTurn;
  private replayingOutput = false;
  private stop?: Stop;
  private steering = new Set<string>();
  /** Keys of this turn's steer steps whose text this drive has handed to the kernel. */
  private handed = new Set<string>();
  /** Live model requests, aborted when the drive halts. */
  private inFlight = new Set<AbortController>();
  /** A steering pass in progress, and whether it may still take steers for the running turn. */
  private steeringTask?: Promise<void>;
  private accepting = false;
  /** Live model effects, settled before the drive returns (a halted one is still writing its release). */
  private liveEffects = new Set<Promise<unknown>>();
  /** Start a steering pass unless one is running (set up by `run`). */
  private pull?: () => void;
  /** The drive-end hooks of its tools (`afterDrive`), each once. */
  private readonly afterDrive = new Set<(ctx: DriveContext) => Promise<void>>();

  constructor(
    private readonly options: FxHarnessOptions,
    private readonly ctx: DriveContext,
    private readonly config: string
  ) {}

  async run(): Promise<void> {
    const effects = this.ctx.state().effectOrder;
    const turns = effects.filter((e) => e.name === 'turn');
    const checkpoints = effects.filter(
      (e) => e.name === 'checkpoint' && e.status === 'completed'
    );
    const last = checkpoints.at(-1)?.output as CheckpointOutput | undefined;
    const restoredThrough = last ? last.turn : -1;

    // Turns recorded after the last checkpoint re-run against their tape,
    // which only a process with the same configuration can reproduce.
    for (const record of turns) {
      const n = Number(record.key.split(':')[1]);
      const recordedWith = (record.input as TurnInput).config;
      if (n <= restoredThrough || !recordedWith || recordedWith === this.config) continue;
      if (!this.ctx.state().effects.has(`model:${n}:0`)) continue;
      throw new Fatal(
        `fx: turn ${n} of this session was recorded by a build with a different fx configuration ` +
          `(system prompt, tools, model or libfx version: ${recordedWith} here ${this.config}), so its tape cannot be replayed here. ` +
          'This happens when a tab from an older deployment and the server (or two deployments) share a session.'
      );
    }

    const kernel = this.options.kernelVersion;
    const model = this.options.model;
    try {
      this.agent = await createEngine(engineFactory(this.options), {
        apiKey:
          typeof this.options.apiKey === 'function'
            ? await this.options.apiKey()
            : this.options.apiKey,
        // libfx types `model` as an id or as options, so each branch passes one.
        ...(typeof model === 'object' ? { model } : model ? { model } : {}),
        ...(this.options.instructions ? { instructions: this.options.instructions } : {}),
        tools: (this.options.tools ?? []).map((t) => {
          const end = (t as Phased).afterDrive;
          if (end) this.afterDrive.add(end);
          return this.wrapTool(t);
        }),
        fetch: (url: RequestInfo | URL, init?: RequestInit) => this.fetch(url, init),
        ...(last ? { checkpoint: fromBase64(last.data) } : {}),
        ...this.options.agentOptions,
        ...(last ? this.options.restoreAgentOptions : {}),
      });
    } catch (err) {
      if (last?.kernel && kernel && last.kernel !== kernel) {
        throw new Fatal(
          `fx: this session's checkpoint was written by libfx ${last.kernel} and this process runs libfx ${kernel}, which cannot restore it: ${err instanceof Error ? err.message : String(err)}`
        );
      }
      throw err;
    }

    const pull = (this.pull = () => {
      this.steeringTask ??= this.pullSteering().finally(() => {
        this.steeringTask = undefined;
      });
    });
    // A submission's hint reads the inbox at once; the poll is the fallback.
    const offHint = this.ctx.onHint?.((hint) => {
      if (hint.eventType === 'hook_received' || hint.eventType === 'reconnect') pull();
    });
    const poll = setInterval(pull, this.options.steeringPollMs ?? (offHint ? 5000 : 500));
    // Cancel the turn and any in-flight request when the drive loses the
    // session. A budget abort only stops new effects.
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
      for (const record of turns) {
        const n = Number(record.key.split(':')[1]);
        if (n <= restoredThrough) continue;
        await this.runTurn(n, (record.input as TurnInput).submissions);
      }
      for (let n = turns.length; ; n++) {
        await this.ctx.refresh();
        const next = this.ctx.pending();
        // Steers a finished turn never delivered become the next turn.
        const carried = this.carryOver();
        if (next.length === 0 && carried.length === 0) break;
        const submissions = [...carried, ...next.map((s) => ({ id: s.id, text: s.text ?? '' }))];
        await this.ctx.effect(
          `turn:${n}`,
          {
            name: 'turn',
            replay: 'safe',
            placed: next.map((s) => s.id),
            input: { submissions, config: this.config } satisfies TurnInput,
          },
          async () => ({ accepted: submissions.length })
        );
        await this.runTurn(n, submissions);
      }
    } finally {
      clearInterval(poll);
      offHint?.();
      this.ctx.signal.removeEventListener('abort', onAbort);
      await Promise.allSettled([...this.liveEffects]);
      await this.agent.close().catch(() => {});
      // What the tools hold for this drive (a sandbox's link) goes with it.
      for (const end of this.afterDrive) await end(this.ctx).catch(() => {});
    }
  }

  private async runTurn(n: number, submissions: TurnInput['submissions']): Promise<void> {
    this.turn = n;
    this.request = 0;
    this.toolSeen.clear();
    this.handed.clear();
    this.accepting = true;
    const text = submissions.map((s) => s.text).join('\n\n');
    const turn = this.agent!.prompt(text);
    this.active = turn;
    this.replaySteers(0);
    try {
      let result: { stopReason: string; usage?: unknown };
      let error: string | undefined;
      try {
        for await (const event of turn) {
          if (this.replayingOutput) continue;
          if (event.type === 'text_delta') this.ctx.live({ type: 'text', delta: event.delta });
          else if (event.type === 'tool_start') this.ctx.live({ type: 'tool_start', name: event.name, args: event.input });
          else if (event.type === 'tool_end') this.ctx.live({ type: 'tool_end', name: event.name, isError: event.isError });
        }
        result = await turn.result;
      } catch (err) {
        if (this.stop) throw this.stop;
        // The turn itself failed (a model step committed as failed, which a
        // replay reproduces exactly). End it like any other turn, so the
        // session is idle again instead of failing every delivery after.
        error = err instanceof Error ? err.message || err.name : String(err);
        result = { stopReason: 'error' };
        this.ctx.live({ type: 'error', message: error });
      }
      // No more steers for this turn; let one being taken right now land
      // (or be refused) before deciding what the turn dropped.
      this.accepting = false;
      await this.steeringTask;
      if (this.stop) throw this.stop;
      const carried = this.carriedBy();
      const dropped = this.turnSteers()
        .filter((e) => !carried.has(e.key))
        .map((e) => ({ id: e.placed[0] ?? e.key, text: (e.input as SteerInput).text }));
      const bytes = await this.agent!.checkpoint();
      await this.ctx.effect(
        `checkpoint:${n}`,
        { name: 'checkpoint', replay: 'safe', input: { turn: n } },
        async (): Promise<CheckpointOutput> => ({
          turn: n,
          stopReason: result.stopReason,
          usage: result.usage,
          data: toBase64(bytes),
          ...(this.options.kernelVersion ? { kernel: this.options.kernelVersion } : {}),
          ...(error ? { error } : {}),
          ...(dropped.length ? { dropped } : {}),
        })
      );
      this.ctx.live({ type: 'idle' });
    } finally {
      this.active = undefined;
    }
  }

  private halt(reason: Stop): void {
    this.stop ??= reason;
    for (const request of this.inFlight) request.abort(reason);
    this.active?.cancel();
  }

  /**
   * The kernel sent a request its tape does not have. A fresh read of the
   * log may explain it (another driver moved it), so the first time the
   * drive restarts; a rebuilt drive that diverges again would do so on
   * every delivery, so it fails the session with the reason.
   */
  private diverge(key: string, why: string): Error {
    const message = `fx diverged from its tape at ${key}: ${why}`;
    this.halt(this.ctx.attempt === 0 ? new Restart(message) : new Fatal(`${message} (again on a fresh read of the log)`));
    return new Error(message);
  }

  /** Steering: hand new `steer` submissions to the running turn, durably. */
  private async pullSteering(): Promise<void> {
    const turn = this.active;
    // Out of budget: the next effect stops the drive anyway. Starting a steer
    // now would stop it in the middle of a live model stream instead.
    if (!turn || !this.accepting || this.stop || this.ctx.signal.aborted) return;
    try {
      await this.ctx.refresh();
    } catch {
      return;
    }
    // A steer is recorded with the ordinal of the request it lands before.
    // While the kernel is still fast-forwarding through the tape, that
    // request is already recorded, and it was recorded *without* this steer:
    // placing the steer there would make the kernel's next request differ
    // from the tape, and since the steer step is durable, every later replay
    // would diverge at the same spot (a permanent wedge). So a steer is only
    // accepted where the next model request is live. It waits a tick (or
    // becomes the next turn) otherwise. A session that just moved to another
    // lane replays its turn from the tape first, which makes this the common
    // case, not a corner. "Recorded" means any status: a request being
    // retried after a crash must be rebuilt exactly as its first attempt
    // recorded it, steers and all.
    if (this.stop || this.ctx.signal.aborted || this.ctx.state().effects.has(`model:${this.turn}:${this.request}`)) return;
    for (const s of this.ctx.pending()) {
      if (this.stop || !this.accepting) return;
      if (s.kind !== 'steer' || this.steering.has(s.id)) continue;
      this.steering.add(s.id);
      await this.steer(turn, s).catch((err) => {
        if (err instanceof Yield || err instanceof Restart) this.halt(err);
      });
    }
  }

  private async steer(turn: FxTurn, s: Submission): Promise<void> {
    const key = `steer:${s.id}`;
    if (this.ctx.isMemoized(key)) return;
    await this.ctx.effect(
      key,
      {
        name: 'steer',
        replay: 'safe',
        placed: [s.id],
        input: { text: s.text ?? '', turn: this.turn, beforeRequest: this.request } satisfies SteerInput,
      },
      async () => {
        this.handed.add(key);
        try {
          await turn.steer(s.text ?? '');
        } catch {
          // The turn ended as the steer arrived. The checkpoint lists it as
          // dropped, and it becomes the next turn.
          return { steered: false };
        }
        return { steered: true };
      }
    );
  }

  /**
   * This turn's steer steps. A steer counts from its committed intent: the
   * kernel may already put it in a request before the step's outcome is
   * written, and that request must record that it carried it.
   */
  private turnSteers() {
    return this.ctx
      .state()
      .effectOrder.filter(
        (e) => e.name === 'steer' && e.status !== 'failed' && (e.input as SteerInput).turn === this.turn
      );
  }

  /** Steers recorded as dropped by a checkpoint that no later turn took up yet. */
  private carryOver(): { id: string; text: string }[] {
    const effects = this.ctx.state().effectOrder;
    const taken = new Set(
      effects.filter((e) => e.name === 'turn').flatMap((e) => (e.input as TurnInput).submissions.map((s) => s.id))
    );
    return effects
      .filter((e) => e.name === 'checkpoint' && e.status === 'completed')
      .flatMap((e) => (e.output as CheckpointOutput).dropped ?? [])
      .filter((d) => !taken.has(d.id));
  }

  /**
   * This turn's recorded model requests in ordinal order, whatever their
   * status: a request being retried after a crash must carry exactly the
   * steers its first attempt recorded, so it counts as on the tape too.
   */
  private turnRequests(): EffectRecord[] {
    const out: EffectRecord[] = [];
    for (let i = 0; ; i++) {
      const r = this.ctx.state().effects.get(`model:${this.turn}:${i}`);
      if (!r) return out;
      out.push(r);
    }
  }

  /** Steer key → ordinal of the recorded request that first carried it. */
  private carriedBy(): Map<string, number> {
    const carried = new Map<string, number>();
    this.turnRequests().forEach((r, i) => {
      for (const key of (r.input as ModelInput).steers ?? []) carried.set(key, i);
    });
    return carried;
  }

  private hand(e: EffectRecord): void {
    if (this.handed.has(e.key)) return;
    this.handed.add(e.key);
    void this.active?.steer((e.input as SteerInput).text).catch(() => {});
  }

  /**
   * Hand the kernel this turn's steers that belong before request
   * `ordinal`, so it builds the same request it built the first time:
   * - a steer a recorded request carried (`ModelInput.steers`) goes before
   *   exactly that request;
   * - a steer no recorded request carried goes before `ordinal` only if
   *   that request is not recorded (it is live, so whatever it carries is
   *   observed and recorded); otherwise it waits for the first live request
   *   (see `fetch`);
   * - on older tapes (a request without a `steers` field), a steer no
   *   request is known to carry goes before its `beforeRequest`.
   */
  private replaySteers(ordinal: number): void {
    if (!this.active) return;
    const requests = this.turnRequests();
    const carried = this.carriedBy();
    const legacy = requests.some((r) => !Array.isArray((r.input as ModelInput).steers));
    const live = ordinal >= requests.length;
    for (const e of this.turnSteers()) {
      const at = carried.get(e.key);
      if (at !== undefined) {
        if (at === ordinal) this.hand(e);
      } else if (legacy) {
        if ((e.input as SteerInput).beforeRequest === ordinal && e.status === 'completed') this.hand(e);
      } else if (live) this.hand(e);
    }
  }

  /**
   * The steers of this turn request `ordinal` carries for the first time,
   * read from its structure: the user messages the kernel appended after
   * the previous response (its latest assistant message), matched exactly
   * against steers this drive has handed to the kernel. Request 0's first
   * such message is the turn's own prompt.
   */
  private newlyCarried(request: unknown, ordinal: number): string[] {
    const carried = this.carriedBy();
    return steersCarriedBy(
      request,
      ordinal,
      this.turnSteers()
        .filter((e) => this.handed.has(e.key) && !carried.has(e.key))
        .map((e) => ({ key: e.key, text: (e.input as SteerInput).text }))
    );
  }

  private async fetch(url: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    // A halted drive (it handed the session to another lane, lost it to
    // another driver, or must restart) sends nothing more: the kernel is
    // being cancelled, and anything it asked for now would be recorded in a
    // log this drive no longer owns.
    if (this.stop) throw this.stop;
    const realFetch = this.options.fetch ?? globalThis.fetch;
    const href = String(url instanceof Request ? url.url : url);
    if ((init?.method ?? 'GET') === 'GET') return realFetch(url, init);

    const ordinal = this.request++;
    const key = `model:${this.turn}:${ordinal}`;
    const bodyText = await bodyToText(init?.body);
    const request = JSON.parse(bodyText);
    const digest = await sha256(bodyText);
    const model = headerOf(init?.headers, 'ai-language-model-id');

    const recorded = this.ctx.state().effects.get(key);
    // A recorded request, finished or being retried, must be the one the
    // kernel sends now: a retry goes out under its first attempt's input.
    if (recorded && recorded.status !== 'failed') {
      const tape = recorded.input as ModelInput;
      const why =
        tape.digest !== digest
          ? 'the kernel sent a different request than the log recorded'
          : tape.model && model && tape.model !== model
            ? `the log recorded model ${tape.model}, this process is configured for ${model}`
            : undefined;
      if (why) throw this.diverge(key, why);
    }
    if (recorded?.status === 'completed') {
      this.replayingOutput = true;
      this.replaySteers(ordinal + 1);
      const out = recorded.output as ModelOutput;
      return new Response(out.body, { status: out.status, headers: { 'content-type': 'text/event-stream' } });
    }
    this.replayingOutput = false;
    // A live request is a steering boundary: look at the inbox now, for a
    // steer that came in before this drive was listening (its hint went to
    // nobody) or before the turn was running, instead of waiting for a poll
    // that may only come after the turn ends.
    this.pull?.();
    // The first live request of a (re-run) turn: steers committed earlier
    // that no recorded request carried (one taken while a request that was
    // then interrupted was streaming) go to the kernel now. This request is
    // already built, so they land in the next one, which records them.
    if (recorded) {
      const carried = this.carriedBy();
      for (const e of this.turnSteers()) if (!carried.has(e.key)) this.hand(e);
    }

    // Live: stream text deltas to the kernel as they arrive, but hold back
    // everything from the first non-delta part (tool calls, finish) until
    // the step commits, so the kernel never acts on a response the log does
    // not have.
    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    let held = '';
    // Aborted by halt(): the model effect then stops with the drive's own
    // reason, which releases its intent for a retry instead of recording
    // the cut-off stream as a failed model call.
    const abort = new AbortController();
    let sinkGone = false;
    const forward = async (event: string) => {
      if (sinkGone) return;
      try {
        await writer.write(encoder.encode(event));
      } catch {
        // The kernel stopped reading. If the drive is halting, stop too;
        // otherwise keep draining, so the log still gets the whole response.
        if (this.stop) throw this.stop;
        sinkGone = true;
      }
    };
    const committed = this.ctx.effect(
      key,
      {
        name: 'model',
        replay: 'safe',
        input: { url: href, request, digest, ...(model ? { model } : {}), steers: this.newlyCarried(request, ordinal) } satisfies ModelInput,
      },
      async (io) => {
        if (this.stop) throw this.stop;
        this.inFlight.add(abort);
        try {
          const attempts = this.options.modelAttempts ?? 3;
          let response: Response | undefined;
          for (let attempt = 1; ; attempt++) {
            try {
              response = await abortable(realFetch(url, { ...init, body: bodyText, signal: abort.signal }), abort.signal);
            } catch (err) {
              if (this.stop) throw this.stop;
              if (attempt >= attempts) throw err;
              await sleep(500 * attempt);
              continue;
            }
            if (response.ok && response.body) break;
            const detail = await response.text().catch(() => '');
            if (this.stop) throw this.stop;
            if (attempt >= attempts || !(response.status === 429 || response.status >= 500)) {
              throw new Error(`model request failed: ${response.status} ${detail}`);
            }
            await sleep(500 * attempt);
          }
          const decoder = new TextDecoder();
          const reader = response.body!.getReader();
          let first = false;
          let body = '';
          let pending = '';
          let holding = false;
          for (;;) {
            let read: ReadableStreamReadResult<Uint8Array>;
            try {
              read = await abortable(reader.read(), abort.signal);
            } catch (err) {
              if (this.stop) throw this.stop;
              throw err;
            }
            if (this.stop) throw this.stop;
            const { value, done } = read;
            if (done) break;
            const chunk = decoder.decode(value, { stream: true });
            body += chunk;
            pending += chunk;
            // TTFT: the first model output event of this request (text, reasoning or a tool call).
            if (!first && hasOutput(pending)) {
              first = true;
              this.ctx.mark?.('model.first_token', { key });
            }
            // Forward whole events only, until the first one worth holding.
            let boundary = pending.indexOf('\n\n');
            while (!holding && boundary !== -1) {
              const event = pending.slice(0, boundary + 2);
              if (!isDelta(event)) {
                holding = true;
                break;
              }
              pending = pending.slice(boundary + 2);
              await forward(event);
              io.progress(event);
              boundary = pending.indexOf('\n\n');
            }
          }
          held = pending;
          return summarize(response!.status, body);
        } finally {
          this.inFlight.delete(abort);
        }
      }
    );
    this.liveEffects.add(committed);
    void committed.catch(() => {}).finally(() => this.liveEffects.delete(committed));
    committed.then(
      async () => {
        if (sinkGone) return;
        try {
          if (held) await writer.write(encoder.encode(held));
          await writer.close();
        } catch {}
      },
      async (err) => {
        if (err instanceof Yield || err instanceof Restart || err instanceof Fatal) this.halt(err);
        await writer.abort(err).catch(() => {});
      }
    );
    return new Response(readable, { status: 200, headers: { 'content-type': 'text/event-stream' } });
  }

  private wrapTool(tool: FxTool | HostTool) {
    return {
      name: tool.name,
      description: tool.description,
      inputSchema: tool.inputSchema,
      // The kernel records a turn's tool calls in order and can hand the turn
      // to another lane from inside one, so they run one at a time: libfx
      // runs the tools of one model response in parallel unless they write.
      writes: true,
      execute: async (input: unknown, { signal }: { signal: AbortSignal }) => {
        const base = `tool:${this.turn}:${tool.name}:${await sha256(JSON.stringify(input))}`;
        const occurrence = this.toolSeen.get(base) ?? 0;
        this.toolSeen.set(base, occurrence + 1);
        const key = `${base}:${occurrence}`;
        try {
          if (this.stop) throw this.stop;
          // The pre-intent phase (a sandbox's attach), before the call's
          // intent is written: what it resolves with is the call's outcome at
          // once (it did not run); a rejection stops the drive.
          const before = (tool as Phased).beforeIntent && !this.ctx.isMemoized(key) ? (tool as Phased).beforeIntent : undefined;
          let notRun: Error | undefined;
          if (before) {
            try {
              notRun = await before(this.ctx);
            } catch (err) {
              const stop = err instanceof Error ? err : new Error(String(err));
              this.halt(stop);
              throw stop;
            }
          }
          return await this.ctx.effect(
            key,
            {
              name: `tool.${tool.name}`,
              replay: tool.replay ?? 'never',
              input,
              // Mid-turn escalation: in a lane that cannot run this tool the
              // kernel commits a handoff instead and throws Yield('handoff').
              // halt() below cancels the turn *before* the rethrow, so the
              // kernel never reports the Yield to the model as a tool error
              // (it would POST it as a tool result otherwise). The next lane
              // restores the last checkpoint, replays this turn's model
              // responses from the tape, reaches this same call under the
              // same key, and runs it there.
              ...(tool.requires ? { requires: tool.requires } : {}),
            },
            (io) => {
              if (notRun) throw notRun;
              return tool.execute(input, {
                signal: AbortSignal.any([signal, io.signal]),
                progress: io.progress,
                ctx: this.ctx,
                stepId: io.stepId,
                park: io.park,
                ...(io.settlement ? { settlement: io.settlement } : {}),
                ...(io.chaosArmedMs !== undefined ? { chaosArmedMs: io.chaosArmedMs } : {}),
                ...(io.docs ? { docs: io.docs } : {}),
              });
            }
          );
        } catch (err) {
          if (err instanceof Yield || err instanceof Restart || err instanceof Fatal) {
            this.halt(err);
            throw err;
          }
          if (err instanceof EffectFailure) {
            const { message, interrupted, partial } = err.error;
            throw new Error(
              interrupted && partial?.length
                ? `${message}\nPartial output before the interruption:\n${JSON.stringify(partial.at(-1))}`
                : message
            );
          }
          throw err;
        }
      },
    };
  }
}

/** `promise`, or a rejection as soon as `signal` aborts (for fetches that ignore it). */
function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (v) => {
        signal.removeEventListener('abort', onAbort);
        resolve(v);
      },
      (e) => {
        signal.removeEventListener('abort', onAbort);
        reject(e);
      }
    );
  });
}

/**
 * Which of `candidates` (steers handed to the kernel that no earlier
 * request carried, in commit order) the model request `request`, the
 * `ordinal`-th of its turn, carries: the user messages it appends after the
 * previous response (its latest assistant message), each matched exactly,
 * in order, to one candidate's text. Request 0's first such message is the
 * turn's own prompt. Structural on purpose: a steer's text also appearing in
 * the system prompt, a tool description or the history proves nothing.
 */
export function steersCarriedBy(request: unknown, ordinal: number, candidates: { key: string; text: string }[]): string[] {
  const prompt = (request as { prompt?: { role?: string; content?: unknown }[] } | null)?.prompt;
  if (!Array.isArray(prompt)) return [];
  const lastReply = prompt.findLastIndex((m) => m?.role === 'assistant');
  const added = prompt
    .slice(lastReply + 1)
    .filter((m) => m?.role === 'user')
    .map((m) => messageText(m.content));
  if (ordinal === 0) added.shift();
  const out: string[] = [];
  for (const text of added) {
    const steer = STEERING.exec(text)?.[1] ?? text;
    const hit = candidates.find((c) => !out.includes(c.key) && c.text === steer);
    if (hit) out.push(hit.key);
  }
  return out;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** The text of a prompt message's content (a string, or text parts). */
function messageText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .filter((p): p is { type: 'text'; text: string } => p?.type === 'text' && typeof p.text === 'string')
    .map((p) => p.text)
    .join('');
}

function headerOf(headers: HeadersInit | undefined, name: string): string | undefined {
  if (!headers) return undefined;
  if (headers instanceof Headers) return headers.get(name) ?? undefined;
  const entries = Array.isArray(headers) ? headers : Object.entries(headers);
  const hit = entries.find(([k]) => k.toLowerCase() === name);
  return hit?.[1] === undefined ? undefined : String(hit[1]);
}

/** Whether a chunk of SSE holds a model output part: text, reasoning, or a tool call (TTFT). */
function hasOutput(sse: string): boolean {
  return /"type":\s*"(text-delta|reasoning-delta|tool-input-start|tool-input-delta|tool-call)"/.test(sse);
}

/**
 * Stream parts the kernel only renders, never acts on, so they may reach it
 * before the step commits. The stream opens with `stream-start` and
 * `response-metadata`: holding those back held back every delta behind
 * them, so durable fx text arrived all at once when the model call ended.
 */
const FORWARDABLE = new Set([
  'stream-start',
  'response-metadata',
  'text-start',
  'text-delta',
  'text-end',
  'reasoning-start',
  'reasoning-delta',
  'reasoning-end',
  'raw',
]);

export function isDelta(event: string): boolean {
  const data = event.trim().replace(/^data: /, '');
  if (!data.startsWith('{')) return false;
  try {
    const type = JSON.parse(data).type as string;
    return FORWARDABLE.has(type);
  } catch {
    return false;
  }
}

function summarize(status: number, body: string): ModelOutput {
  let text = '';
  let finishReason: string | undefined;
  const toolCalls: ModelOutput['toolCalls'] = [];
  for (const block of body.split('\n\n')) {
    const data = block.trim().replace(/^data: /, '');
    if (!data.startsWith('{')) continue;
    try {
      const part = JSON.parse(data);
      if (part.type === 'text-delta') text += part.delta ?? '';
      else if (part.type === 'tool-call') {
        toolCalls.push({ toolCallId: part.toolCallId, toolName: part.toolName, input: part.input });
      } else if (part.type === 'finish') finishReason = part.finishReason?.unified ?? part.finishReason;
    } catch {}
  }
  return { status, body, text, toolCalls, finishReason };
}

async function bodyToText(body: BodyInit | null | undefined): Promise<string> {
  if (body == null) return '';
  if (typeof body === 'string') return body;
  if (body instanceof Uint8Array) return new TextDecoder().decode(body);
  if (body instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(body));
  return new Response(body).text();
}

async function sha256(text: string): Promise<string> {
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return Array.from(new Uint8Array(hash), (b) => b.toString(16).padStart(2, '0')).join('').slice(0, 32);
}

function toBase64(bytes: Uint8Array): string {
  let binary = '';
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}

function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

export { createDurableFxAgent, turnOutcome } from './durable.ts';
export type { DurableFxAgent, DurableFxAgentOptions, DurableFxEvent, DurableFxTurn, DurableFxTurnResult } from './durable.ts';

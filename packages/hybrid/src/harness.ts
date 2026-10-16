import type { StreamFn, ThinkingLevel } from '@earendil-works/pi-agent-core';
import { createModels, type Model } from '@earendil-works/pi-ai';
import { vercelAIGatewayProvider } from '@earendil-works/pi-ai/providers/vercel-ai-gateway';
import type { DriveContext, Harness, HostTool, Lane, LaneOptions } from 'harness-world';
import { engineFactory, type FxEngineFactory, type FxHarnessOptions, fxConfigHash, fxHarness } from 'harness-world-fx';
import { piHarness } from 'harness-world-pi';
import { type HybridHands, hybridTools, isHybridHands } from './tools.ts';

/** Sessions run as `harness:hybrid`, so a deployment can host them next to plain Pi sessions. */
export const HYBRID_HARNESS = 'hybrid';
/** Hybrid fx sessions run as `harness:hybrid-fx`: same lanes and tools, fx's kernel. */
export const HYBRID_FX_HARNESS = 'hybrid-fx';
/** Every harness name that moves between lanes. */
export const HYBRID_HARNESSES: readonly string[] = [HYBRID_HARNESS, HYBRID_FX_HARNESS];
export const isHybridHarness = (name: string | undefined) => !!name && HYBRID_HARNESSES.includes(name);
export const HYBRID_MODEL = 'anthropic/claude-opus-5.5-fast';
/**
 * The libfx every lane runs (pinned to this exact version in the pnpm
 * catalog, pnpm-workspace.yaml; a test checks this constant against the
 * installed one).
 * Recorded on fx turns and checkpoints, and part of `hybridFxConfigHash`.
 */
export const LIBFX_VERSION = '0.0.14';

/**
 * The same in every lane, and for both harnesses: it is part of the
 * replayed context (Pi's transcript, fx's digested request body), so a session
 * that continues in another lane must see exactly what it saw before.
 */
export const HYBRID_SYSTEM_PROMPT = `You are a coding agent in a hybrid session. The session is one durable log: it runs in the user's browser tab whenever it can, and moves on its own to a server function when a tool needs one, then comes back. You never need to manage where you run.

Files live in the workspace at /workspace and persist for the whole session, wherever it runs.

Tools:
- read_file, write_file, edit_file, list_files, bash: instant, for text and file work. bash is an emulated shell with coreutils, grep, sed, awk, jq: there is no node, npm, python, git, or network in it.
- lookup_customer: look up a customer in the CRM. It needs a backend credential, so the session moves to a server function to run it.
- run_on_machine: a real Linux VM with node, npm, python, git, and network access, for running programs or tests, installing packages, git, and fetching URLs. The session moves to a server function, which runs the command on the machine and waits for it; the workspace files are synced in and back.

Prefer the lightest tool that can do the job. Keep answers short.`;

const PROMPT_HEAD = `You are a coding agent in a hybrid session. The session is one durable log: it runs in the user's browser tab whenever it can, and moves on its own to a server function when a tool needs one, then comes back. You never need to manage where you run.`;
const LOOKUP_LINE = '- lookup_customer: look up a customer in the CRM. It needs a backend credential, so the session moves to a server function to run it.';
const PROMPT_TAIL = 'Prefer the lightest tool that can do the job. Keep answers short.';

/**
 * The system prompt for a session's hands (`HybridHands`). Part of the
 * replayed context, so the same in every lane for one session; `machine`'s
 * is `HYBRID_SYSTEM_PROMPT`, unchanged, so sessions started before the
 * hands axis replay as they were.
 */
export function hybridSystemPrompt(hands: HybridHands = 'machine'): string {
  if (hands === 'machine') return HYBRID_SYSTEM_PROMPT;
  const files =
    hands === 'none'
      ? 'There is no workspace in this session: no files and no shell.'
      : hands === 'ephemeral'
        ? 'Files live in a workspace at /workspace in memory only: they are cleared at the end of every turn, and sooner if the turn moves to another lane.'
        : 'Files live in the workspace at /workspace and persist for the whole session, wherever it runs.';
  const tools =
    hands === 'none'
      ? ['Tools:', LOOKUP_LINE]
      : ['Tools:', '- read_file, write_file, edit_file, list_files, bash: instant, for text and file work. bash is an emulated shell with coreutils, grep, sed, awk, jq: there is no node, npm, python, git, or network in it, and no real machine in this session.', LOOKUP_LINE];
  return [PROMPT_HEAD, files, tools.join('\n'), PROMPT_TAIL].join('\n\n');
}

/** A session's hands, as its `run_created` recorded them (`config.hands`); `machine` for one that recorded none. */
export function hybridHandsOf(ctx: Pick<DriveContext, 'config'>): HybridHands {
  const hands = (ctx.config as { hands?: unknown } | undefined)?.hands;
  return isHybridHands(hands) ? hands : 'machine';
}

/** One harness per hands, built on a session's first drive with them, each drive going to its session's. */
function byHands(name: string, lanes: LaneOptions | undefined, build: (hands: HybridHands) => Harness): Harness {
  const built = new Map<HybridHands, Harness>();
  return {
    name,
    ...(lanes ? { lanes } : {}),
    drive(ctx) {
      const hands = hybridHandsOf(ctx);
      let harness = built.get(hands);
      if (!harness) built.set(hands, (harness = build(hands)));
      return harness.drive(ctx);
    },
  };
}

export interface HybridHarnessOptions {
  /** The most capable lane this process drives; picks the default tools. */
  lane: Lane;
  model: Model<any>;
  streamFn: StreamFn;
  getApiKey?: (provider: string) => Promise<string | undefined> | string | undefined;
  /**
   * Default: `hybridTools({ lane, hands })`. Pass your own to give it
   * secrets or a sandbox: a function of the session's hands (each session
   * gets the tools for the hands its `run_created` recorded), or one list
   * for every session.
   */
  tools?: HostTool[] | ((hands: HybridHands) => HostTool[]);
  thinkingLevel?: ThinkingLevel;
  /**
   * Which lanes this process drives and how it wakes the others. Carried on
   * the harness, so a route hosting plain Pi/fx sessions too only moves the
   * hybrid ones.
   */
  lanes?: LaneOptions;
}

/** The tools for `hands`, from what the options give. */
function toolsFor(tools: HybridHarnessOptions['tools'], lane: Lane, hands: HybridHands): HostTool[] {
  if (typeof tools === 'function') return tools(hands);
  return tools ?? hybridTools({ lane, hands });
}

/**
 * The hybrid Pi harness. One definition for every lane: same system prompt,
 * same tool list (for the session's hands), so the transcript folded from
 * the log is the same context whichever lane continues it.
 */
export function hybridHarness(options: HybridHarnessOptions): Harness {
  return byHands(HYBRID_HARNESS, options.lanes, (hands) => {
    const pi = piHarness({
      model: options.model,
      streamFn: options.streamFn,
      ...(options.getApiKey ? { getApiKey: options.getApiKey } : {}),
      ...(options.thinkingLevel ? { thinkingLevel: options.thinkingLevel } : {}),
      systemPrompt: hybridSystemPrompt(hands),
      tools: toolsFor(options.tools, options.lane, hands),
    });
    return { ...pi, name: HYBRID_HARNESS };
  });
}

/**
 * A pi-ai model on AI Gateway. `baseUrl` points the provider somewhere else
 * (the demo app's `/api/gateway` proxy, with a placeholder key). `fetch`
 * carries every provider request instead of the global fetch: the tab passes
 * `gatewayFetch` (see `gateway.ts`), which goes to the Gateway directly with
 * a minted token and falls back to the proxy. The server passes neither.
 */
export function createGatewayModel(baseUrl?: string, modelId: string = HYBRID_MODEL, fetch?: typeof globalThis.fetch) {
  const models = createModels();
  models.setProvider(vercelAIGatewayProvider());
  const base = models.getModel('vercel-ai-gateway', modelId);
  if (!base) throw new Error(`pi-ai does not know ${modelId}`);
  const model: Model<any> = baseUrl ? { ...base, baseUrl: baseUrl.replace(/\/+$/, '') } : base;
  const stream = models.streamSimple.bind(models) as StreamFn;
  return { model, streamFn: fetch ? withFetch(stream, fetch) : stream };
}

/** `streamFn` with every provider request sent through `fetch`. */
export function withFetch(streamFn: StreamFn, fetch: typeof globalThis.fetch): StreamFn {
  return ((model, context, options) => streamFn(model, context, { ...options, fetch } as typeof options)) as StreamFn;
}

/**
 * `createFxEngine`: libfx's engine factory, injected so this package never
 * imports libfx: the server passes the native addon's (`libfx`), the tab
 * `libfx/browser`'s (Wasm + JSPI). Their checkpoints and tapes are
 * interchangeable: every build sends byte-identical request bodies for the
 * same state, and each restores the others' checkpoints, so one session can
 * move between them mid-turn. `createFxAgent` is its deprecated name (since
 * vercel-labs/fx#1180, libfx's own `createFxAgent` is its durable agent,
 * which the fx harness refuses).
 */
export type HybridFxHarnessOptions = FxEngineFactory & HybridFxSettings;

export interface HybridFxSettings {
  /** The most capable lane this process drives; picks the default tools. */
  lane: Lane;
  /** The AI Gateway credential (the tab passes a placeholder its proxy replaces). */
  apiKey: string | (() => Promise<string> | string);
  /** Default `HYBRID_MODEL`. Must be the same in every lane (the tape checks it). */
  model?: string;
  /** Default: `hybridTools({ lane, hands })`. Pass your own to give it secrets or a sandbox: per hands, or one list (see `HybridHarnessOptions.tools`). */
  tools?: HostTool[] | ((hands: HybridHands) => HostTool[]);
  /** Where the kernel's model requests go (the tab rewrites them to its proxy). */
  fetch?: typeof fetch;
  /** Extra libfx options, e.g. `{ backend: 'wasm' }` in the tab. */
  agentOptions?: Record<string, unknown>;
  /**
   * Extra libfx options for an engine that restores a checkpoint. Default:
   * `{ backend: 'wasm' }` outside the browser lane. The tab writes the same
   * run as the server, so a checkpoint in the log may be the tab's, and its
   * bytes never reach the native addon on a shared instance: the server
   * restores on libfx's wasm build (it needs JSPI: Node 24 has it), whose
   * checkpoints and tapes are the native one's.
   */
  restoreAgentOptions?: Record<string, unknown>;
  lanes?: LaneOptions;
  /**
   * Whether this process can drive fx at all right now. Returns a reason
   * when it cannot (a browser without JSPI, a tab built from another
   * deployment than the server's): a session with work to do in the
   * `browser` lane is then handed to the `function` lane, which runs the
   * turn on libfx's native addon, instead of failing to start a kernel here.
   */
  cannotDrive?: () => string | undefined | Promise<string | undefined>;
  /** Default 500ms. */
  steeringPollMs?: number;
}

/** The fx options every lane derives from the same inputs (for one session's hands). */
function hybridFxOptions(options: HybridFxHarnessOptions, hands: HybridHands = 'machine'): FxHarnessOptions {
  return {
    createFxEngine: engineFactory(options),
    apiKey: options.apiKey,
    model: options.model ?? HYBRID_MODEL,
    instructions: hybridSystemPrompt(hands),
    tools: toolsFor(options.tools, options.lane, hands),
    kernelVersion: LIBFX_VERSION,
    ...(options.fetch ? { fetch: options.fetch } : {}),
    ...(options.agentOptions ? { agentOptions: options.agentOptions } : {}),
    ...(restoreOptions(options) ? { restoreAgentOptions: restoreOptions(options) } : {}),
    ...(options.steeringPollMs ? { steeringPollMs: options.steeringPollMs } : {}),
  };
}

/** How a lane restores a checkpoint: the tab as it likes, every other lane never on the native addon. */
function restoreOptions(options: HybridFxHarnessOptions): Record<string, unknown> | undefined {
  return options.restoreAgentOptions ?? (options.lane === 'browser' ? undefined : { backend: 'wasm' });
}

/**
 * What a hybrid fx process's tapes depend on (see `fxConfigHash`), for the
 * hands given (default `machine`). Lanes with the same hash can continue
 * each other's turns; the server publishes its own so a tab from another
 * deployment can tell it would diverge (one hands' hash tells: every hands'
 * set is built from the same code).
 */
export function hybridFxConfigHash(options: Pick<HybridFxSettings, 'lane' | 'model' | 'tools'> & { hands?: HybridHands }): Promise<string> {
  const { hands, ...rest } = options;
  return fxConfigHash(hybridFxOptions({ ...rest, createFxEngine: async () => { throw new Error('unused'); }, apiKey: '' }, hands));
}

/**
 * The hybrid session on fx. Same shape as `hybridHarness`: one system
 * prompt and one tool list for every lane, so the request body fx's kernel
 * builds (which the tape digests) is the same whichever lane builds it.
 *
 * A tool that needs a more capable lane escalates *mid-turn*: the lane that
 * reached it commits a handoff and cancels its turn, and the next lane
 * restores the last checkpoint and re-runs the turn against the tape (model
 * responses and earlier tool results come from the log, not the network),
 * reaching the same tool call under the same key and running it there.
 */
export function hybridFxHarness(options: HybridFxHarnessOptions): Harness {
  const fx = byHands(HYBRID_FX_HARNESS, undefined, (hands) => fxHarness(hybridFxOptions(options, hands)));
  return {
    name: HYBRID_FX_HARNESS,
    ...(options.lanes ? { lanes: options.lanes } : {}),
    async drive(ctx) {
      const why = ctx.lane === 'browser' ? await options.cannotDrive?.() : undefined;
      if (why && hasWork(ctx)) {
        // Escalate like a tool that needs the function lane: commit a
        // handoff (no intent), wake the server, stop this drive.
        await ctx.effect(
          `fx-unavailable:${ctx.state().eventCount}`,
          { name: 'fx', replay: 'safe', input: null, requires: 'function', reason: `fx cannot run in this tab: ${why}` },
          async () => null
        );
      }
      if (why) return;
      await fx.drive(ctx);
    },
  };
}

/** A submission to take, or a turn left unfinished (no checkpoint yet). */
function hasWork(ctx: DriveContext): boolean {
  if (ctx.pending().length > 0) return true;
  const { effects, effectOrder } = ctx.state();
  return effectOrder.some((e) => e.name === 'turn' && effects.get(`checkpoint:${e.key.split(':')[1]}`)?.status !== 'completed');
}

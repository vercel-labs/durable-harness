/**
 * durable-harness/pi-durable: your pi-durable code, on a World-backed
 * storage. Pi 1.0's own durable `Harness` (`@earendil-works/pi-durable`)
 * keeps its API; the World adds a storage whose every commit is an event in
 * the session's run, and a backstop that finishes the work a dead process
 * left.
 *
 * ```ts
 * // lib/pi.ts
 * import { Harness, type Storage } from '@earendil-works/pi-durable'
 * import { resumeWith } from 'durable-harness/pi-durable'
 * export const open = resumeWith((storage: Storage) => Harness.open(storage, { models, registry }, ctx))
 *
 * // where you opened SQLite, JSONL or memory storage before
 * import { openWorldStorage } from 'durable-harness/pi-durable'
 * const harness = await open(await openWorldStorage(sessionId))
 * ```
 *
 * `openWorldStorage` is the storage swap; `resumeWith` is the one line that
 * tells durable-harness how to reopen a session a process left unfinished.
 * Everything else is Pi's. The rest of this module is optional helpers:
 * `acquireHarness` (one open Harness per session per process),
 * `chatRoute` (a chat route on Pi's submissions), `gatewayModels` (pi-ai
 * models through the Vercel AI Gateway) and `openWorldReader` (a read-only
 * follower for UIs).
 *
 * A Harness has no tools until you install them. Pi's coding tools (read,
 * write, edit, bash) go where the model's commands and files belong in a
 * shared Function: a virtual workspace, or the session's Vercel Sandbox,
 * never the Function's own disk, processes or environment:
 *
 * ```ts
 * import { CodingTools, virtualEnv } from 'durable-harness/pi-durable'
 * registry.install(CodingTools)
 * Harness.open(storage, { models, registry, env: virtualEnv() }, ctx)
 * ```
 */
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { createModels, type MutableModels } from '@earendil-works/pi-ai/models';
import { vercelAIGatewayProvider } from '@earendil-works/pi-ai/providers/vercel-ai-gateway';
import type { AgentChange, Harness, HarnessOptions } from '@earendil-works/pi-durable';
import type { World } from '@workflow/world';
import type { ChatRoute } from 'harness-world';
import { defaultAnchors } from 'harness-world/anchor';
import { type ChatAuthorize, chatGate } from 'harness-world/authorize';
import { harnessOfQueue, workflowNameFor } from 'harness-world/names';
import { gatewayCredential } from 'harness-world-process';
import { type Backstop, type PiDurability, piDurability, StorageFenced, untilSettled, type WorldStorage } from 'pi-world/durability';
import { JournalReader, type PiCommit, type PiEntry, type PiSnapshot, type PiTask } from 'pi-world/reader';
import { NOT_RUN, sandboxEnvBuilder } from 'pi-world/sandbox';
import { CodingTools, MEMORY_NOTE, virtualEnvBuilder } from 'pi-world/workspace';
import { waitUntil } from './agent.ts';
import { resolveEnvironment } from './environments.ts';
import { register } from './registry.ts';
import { publicSandboxConfig } from './sandbox-harness.ts';
import { getWorld } from './world.ts';
import { isWorkspace, sandbox, type SandboxOptions, type SandboxWorkspace, virtual, type VirtualOptions, type VirtualWorkspace } from './workspace.ts';

export { StorageFenced };
export type { WorldStorage };

/** pi-ai's provider id for the Vercel AI Gateway: `{ provider: GATEWAY_PROVIDER, modelId: 'anthropic/claude-sonnet-4.5' }`. */
export const GATEWAY_PROVIDER = 'vercel-ai-gateway';

/** The name sessions carry on their runs, and the opener that resumes them, when you name none. */
const DEFAULT_NAME = 'pi-durable';

/** The default for `recoverAfterMs`. */
const RECOVER_AFTER_MS = 120_000;

/**
 * How durable-harness reopens a session: your own `Harness.open` on the
 * storage it hands you (`(storage) => Harness.open(storage, { models, registry }, ctx)`).
 * It must return a new Harness on that storage, every time.
 */
export type Opener = (storage: WorldStorage) => Promise<Harness>;

export interface WorldStorageOptions {
  /** The World the session lives on. Default: the environment's, chosen the way the Workflow SDK chooses it (`getWorld()`). */
  world?: World;
  /**
   * The name of the opener that resumes the session (`resumeWith(open, { name })`),
   * recorded on a new session's run. Only for two Pi setups in one app. Default `pi-durable`.
   */
  name?: string;
  /**
   * How long this process may stay silent (no Pi commit) while the session
   * has unfinished work before the backstop may take it over: never before
   * half of this after its last commit, at the latest all of it. A silent
   * crash on Vercel is resumed within this plus 5 s. A tool that runs longer
   * than this without output is taken over too: raise it for long tools.
   * Default 120 000.
   */
  recoverAfterMs?: number;
  /**
   * Who may read the session's trace content in Vercel Agent Runs
   * (`durable-harness/instrumentation/otel`). Recorded on a new session, so
   * every later storage of it, a wake's included, traces the same way; on a
   * session that exists, it applies to this storage only. Default: what the
   * session recorded, else the integration's default (`agentRuns({ audience })`).
   */
  traceAudience?: 'public' | 'private' | 'unknown';
}

export interface ResumeOptions {
  /** The World it serves. Default: the environment's. An opener with its own World is served only by that World's queue consumer. */
  world?: World;
  /** The name it resumes (the `name` you give `openWorldStorage`). Default `pi-durable`. */
  name?: string;
  /** The longest one wake drives a session before it hands it to the next one, in ms. Default 10 minutes. */
  maxDriveMs?: number;
  /** The trace audience of the turns a wake drives in a session that recorded none (`openWorldStorage`'s `traceAudience`). Default: the integration's. */
  traceAudience?: 'public' | 'private' | 'unknown';
}

const ctx = BACKGROUND_CONTEXT;

// ---------- the World's sessions, shared by every caller in this process ----------

const DURABILITIES = Symbol.for('durable-harness//pi-durable//durabilities');

/** One pi-world durability per World and name in this process: the wake and `acquireHarness` share its open Harnesses. */
function durabilityOf(world: World, name: string): PiDurability {
  const g = globalThis as { [DURABILITIES]?: WeakMap<World, Map<string, PiDurability>> };
  const byWorld = (g[DURABILITIES] ??= new WeakMap());
  let byName = byWorld.get(world);
  if (!byName) byWorld.set(world, (byName = new Map()));
  let d = byName.get(name);
  if (!d) byName.set(name, (d = piDurability({ world, name, anchors: defaultAnchors() })));
  return d;
}

const REQUESTS = Symbol.for('durable-harness//pi-durable//kept-alive');
type RequestContext = { waitUntil?: (p: Promise<unknown>) => void };

/** The current request's `waitUntil` (Vercel's request context), once per request and promise; a no-op elsewhere. */
function keepAlive(work: Promise<void>): void {
  const request = (globalThis as { [k: symbol]: { get?: () => RequestContext | undefined } | undefined })[Symbol.for('@vercel/request-context')]?.get?.();
  if (!request?.waitUntil) return;
  const g = globalThis as { [REQUESTS]?: WeakMap<object, WeakSet<Promise<void>>> };
  const kept = (g[REQUESTS] ??= new WeakMap());
  let promises = kept.get(request);
  if (!promises) kept.set(request, (promises = new WeakSet()));
  if (promises.has(work)) return;
  promises.add(work);
  waitUntil(work);
}

const log = (message: string, fields?: Record<string, unknown>) => console.warn(`[durable-harness] ${message}`, fields ?? {});

function backstopOf(d: PiDurability, options: WorldStorageOptions): Backstop {
  return { anchors: d.anchors, recoverAfterMs: options.recoverAfterMs ?? RECOVER_AFTER_MS, keepAlive, log };
}

/** What a session this module creates records in its config besides its trace audience (a public chat route's mark). */
type CreatedConfig = Readonly<Record<string, unknown>>;

/** What a storage this module opens is opened with (`config`: recorded only if this open creates the session). */
const storageOf = (d: PiDurability, options: WorldStorageOptions, config?: CreatedConfig) => ({
  backstop: backstopOf(d, options),
  ...(options.traceAudience ? { traceAudience: options.traceAudience } : {}),
  ...(config ? { config } : {}),
});

/** What a new session is created with. */
const creationOf = (options: WorldStorageOptions, config?: CreatedConfig) => ({ ...(options.traceAudience ? { traceAudience: options.traceAudience } : {}), ...(config ? { config } : {}) });

const OPEN = Symbol.for('durable-harness//pi-durable//open-storages');
const WARNED_SECOND = Symbol.for('durable-harness//pi-durable//warned-second-open');

/**
 * Not in production: a second open storage of one session in this process
 * means two Harnesses, and the second takes the session over from the first
 * at its open (Pi's one-owner rule). Warned once.
 */
function noteOpen(world: World, storage: WorldStorage): void {
  if (process.env.NODE_ENV === 'production') return;
  const g = globalThis as { [OPEN]?: WeakMap<World, Map<string, WorldStorage>>; [WARNED_SECOND]?: boolean };
  const byWorld = (g[OPEN] ??= new WeakMap());
  let open = byWorld.get(world);
  if (!open) byWorld.set(world, (open = new Map()));
  const other = open.get(storage.sessionId);
  if (other && !other.closed && !other.isFenced && !g[WARNED_SECOND]) {
    g[WARNED_SECOND] = true;
    console.warn(
      `[durable-harness] pi-durable session ${storage.sessionId} is open twice in this process: the second Harness takes it over from the first at its open, and the first one's next commit is refused. Keep one Harness per session per process, or share it with acquireHarness() from durable-harness/pi-durable.`
    );
  }
  open.set(storage.sessionId, storage);
}

// ---------- the storage swap ----------

/**
 * A Pi `Storage` for one session, on the World: pass it to your
 * `Harness.open`, where you passed SQLite, JSONL or memory storage before.
 * Without `sessionId`, a new session (its id is `storage.sessionId`: `wrun_`
 * and a ULID). Each Pi commit is one event in the session's run, durable
 * before Pi goes on.
 *
 * The storage is its own backstop: before a commit that may leave work
 * unfinished, it makes sure a wake stands behind it (on Vercel, a queue
 * message delayed past the commit's deadline; on world-local and
 * world-postgres, the next start's recovery). If this process dies, the wake
 * reopens the session with the opener you registered with `resumeWith`, and
 * Pi resumes the turn. A request that returns while Pi still works is kept
 * alive (`waitUntil`) until the session is idle; closing the Harness with
 * work left hands it to a wake at once.
 *
 * Opening a session is taking it over (Pi's own rule): keep one open Harness
 * per session per process (`acquireHarness` shares one), and read a session
 * that may be running with `openWorldReader`.
 */
export async function openWorldStorage(sessionId?: string, options: WorldStorageOptions = {}): Promise<WorldStorage> {
  const world = options.world ?? (await getWorld());
  const d = durabilityOf(world, options.name ?? DEFAULT_NAME);
  const id = sessionId ?? (await d.create(creationOf(options)));
  const storage = await d.storage(id, undefined, storageOf(d, options));
  noteOpen(world, storage);
  return storage;
}

// ---------- the one line ----------

const isPiWake = (message: unknown) => (message as { input?: { kind?: unknown } } | undefined)?.input?.kind === 'pi-wake';

/**
 * Register how durable-harness reopens a session this or another process
 * left unfinished: `open(storage)` returns your `Harness.open(storage, ...)`.
 * Returns `open` unchanged, so it is also what you open sessions with.
 *
 * Call it once, at the top of a module the flow route loads: in Next.js,
 * any module (`withDurableHarness` finds the call and imports the module in
 * the flow route it writes); on Vercel Functions, import the module in
 * `api/flow.ts`; on a server of your own, in the process that consumes the
 * queue. A wake opens a fresh storage under its queue message, calls `open`,
 * `resume()`s the Harness, waits until it is idle and closes it.
 */
export function resumeWith<O extends Opener>(open: O, options: ResumeOptions = {}): O {
  const name = options.name ?? DEFAULT_NAME;
  register({
    name,
    workflowName: workflowNameFor(name),
    ...(options.world ? { world: options.world } : {}),
    // A wake pi-world queued, or a World's recovery delivery for one of these sessions (on its workflow's queue).
    claims: (message, meta) => isPiWake(message) || harnessOfQueue(meta.queueName) === name,
    async consume(message, meta) {
      const world = options.world ?? (await getWorld());
      const d = durabilityOf(world, name);
      // A wake's storage is written under its queue message; if a request of
      // this process keeps the Harness open after the wake lets go, the
      // storage's own backstop stands behind what it commits next.
      const reopen = async (sessionId: string, anchor?: Parameters<PiDurability['storage']>[1]) => {
        const storage = await d.storage(sessionId, anchor, {
          ...storageOf(d, {}),
          ...(options.traceAudience ? { defaultTraceAudience: options.traceAudience } : {}),
        });
        noteOpen(world, storage);
        return open(storage);
      };
      const handled = d.route(reopen, options.maxDriveMs !== undefined ? { maxDriveMs: options.maxDriveMs } : {})(message, meta);
      // Not a pi-durable message (another harness's, whose module has not loaded here): it comes back.
      if (!handled) return { timeoutSeconds: 5 };
      return (await handled) ?? undefined;
    },
  });
  return open;
}

// ---------- the tools' environment ----------

/** What `HarnessOptions.env` takes: the environment each tool call gets. */
export type EnvBuilder = NonNullable<HarnessOptions['env']>;

export { CodingTools, MEMORY_NOTE, NOT_RUN };

/**
 * The environment Pi's tools run on, for `Harness.open(storage, { env })`:
 * a virtual workspace (just-bash on a virtual filesystem at `/workspace`,
 * in a worker thread leased to the session). Commands can't reach the
 * Function's files, processes or environment; there is no network and no
 * real binaries (node, python, git).
 *
 * Install `CodingTools` from this module with it (pi-durable's own read,
 * write, edit and bash: the same names, schemas, descriptions and code, each
 * call bound to the workspace):
 *
 * ```ts
 * registry.install(CodingTools)
 * Harness.open(storage, { models, registry, env: virtualEnv() }, ctx)
 * ```
 *
 * - `virtualEnv()` (`virtual()`, durable): each call runs on the files as
 *   committed, and what it changed commits with its result, in the
 *   session's log (files over 4 KB in its blob stream). A call a crash cut
 *   off changed nothing, and the files follow the session to any machine.
 *   Needs a World storage (`openWorldStorage`). Outside `CodingTools` (a
 *   tool of yours that uses `api.env`), the workspace is read-only.
 * - `virtualEnv(virtual({ durable: false }))` (or `virtualEnv({ durable:
 *   false })`): the same tools on files in memory, for the turn; they are
 *   cleared when the next turn starts, and sooner if the session moves to
 *   another invocation. The first tool result of each turn tells the model
 *   (`MEMORY_NOTE`).
 *
 * Never `NodeExecutionEnv` in a Function: it runs the model's commands on
 * the Function's own disk and processes, with its environment, where every
 * session on the instance can reach them. `CodingTools` refuses one on Vercel.
 */
export function virtualEnv(workspace?: VirtualWorkspace | VirtualOptions): EnvBuilder {
  if (isWorkspace(workspace) && workspace.kind !== 'virtual') throw new TypeError('durable-harness: virtualEnv() takes virtual(); for a sandbox() workspace use sandboxEnv()');
  const ws = isWorkspace(workspace) ? workspace : virtual(workspace);
  const { isolation } = resolveEnvironment(ws);
  return virtualEnvBuilder({ durable: ws.durable, machine: { maxBytes: ws.maxBytes, maxFileBytes: ws.maxFileBytes, timeoutMs: ws.timeoutMs, isolation } });
}

/**
 * The environment Pi's tools run on in the session's Vercel Sandbox: the
 * VM's `/workspace`, on a Drive per session, as the VM's unprivileged user
 * (commands get only the variables they are given; the Function's files,
 * processes and environment are out of reach). Files on the Drive are
 * durable but not transactional: they outlive every VM, and a command a
 * crash interrupted may have half-run (pi-durable reports such a call
 * interrupted and never runs it again).
 *
 * ```ts
 * registry.install(CodingTools)
 * Harness.open(storage, { models, registry, env: sandboxEnv() }, ctx)
 * ```
 *
 * The VM boots at the first call that reaches the workspace (a session that
 * never runs a tool never boots one), from `sandbox({ image })` or the
 * project's base snapshot, and the storage is its owner: it arms the VM with
 * the session's own fencing token (the count of its first accepted write in
 * the session's log, which rises from owner to owner), so an invocation that
 * lost the session can't run a command there. The session's sandbox record
 * (its Drive, the VM time granted against `sandbox({ budget: { vmSeconds }
 * })`) is in its log. A call whose VM could not be attached answers
 * `NOT_RUN` ("Not run: the sandbox is starting, try again"): nothing reached
 * the VM. With this module's `CodingTools`, a call also tells the model when
 * the session's files were lost (a new, empty Drive), once. pi-durable's own
 * `CodingTools` work on it too. Needs a World storage (`openWorldStorage`)
 * on a World with the in-band writer fence (world-vercel, world-local). A
 * session a public `chatRoute` started boots a VM only with that route's
 * `allowPublicSandbox: true` and a finite `budget.sessionUsd`; otherwise its
 * calls fail, saying why, before any VM boots.
 */
export function sandboxEnv(workspace?: SandboxWorkspace | SandboxOptions): EnvBuilder {
  if (isWorkspace(workspace) && workspace.kind !== 'sandbox') throw new TypeError('durable-harness: sandboxEnv() takes sandbox(); for a virtual() workspace use virtualEnv()');
  const ws = isWorkspace(workspace) ? workspace : sandbox(workspace);
  // The sandbox tier's acquireMachine for this descriptor: each storage hands it the session, its own fence, its signal and its record.
  return sandboxEnvBuilder(resolveEnvironment(ws).acquire);
}

// ---------- optional: one Harness per session per process ----------

/** A shared open Harness (`acquireHarness`). */
export interface HarnessLease {
  /** Pi's own Harness. Release it, don't close it. */
  harness: Harness;
  /** The storage under it (`storage.sessionId`). */
  storage: WorldStorage;
  /** Let go of it; the last release closes the Harness. */
  release(): Promise<void>;
}

/**
 * The session's open Harness in this process, shared: opened with `open` on
 * `openWorldStorage(sessionId)` if this process holds none (or holds one
 * another process took the session from), else the one already open,
 * including one a wake of this process is driving. Pi allows one owner per
 * session, so every request that submits to a session should go through
 * this. Without `sessionId`, a new session.
 */
export async function acquireHarness(sessionId: string | undefined, open: Opener, options: WorldStorageOptions = {}): Promise<HarnessLease> {
  return leaseHarness(sessionId, open, options);
}

/** `acquireHarness`, recording `config` on a session it creates (`chatRoute`'s public mark). */
async function leaseHarness(sessionId: string | undefined, open: Opener, options: WorldStorageOptions, config?: CreatedConfig): Promise<HarnessLease> {
  const world = options.world ?? (await getWorld());
  const d = durabilityOf(world, options.name ?? DEFAULT_NAME);
  const id = sessionId ?? (await d.create(creationOf(options, config)));
  const lease = await d.acquire(id, async (sid, anchor) => {
    // An id the World has never seen is created by this open: it records `config` too.
    const storage = await d.storage(sid, anchor, storageOf(d, options, config));
    noteOpen(world, storage);
    return open(storage);
  });
  if (!lease.storage) {
    await lease.release();
    throw new Error(`durable-harness: the Harness open for ${id} in this process was not opened on a World storage`);
  }
  return { harness: lease.harness, storage: lease.storage, release: lease.release };
}

// ---------- optional: models through the AI Gateway ----------

/**
 * pi-ai `Models` with the Vercel AI Gateway provider, its credential asked
 * for at every request: `AI_GATEWAY_API_KEY`, else the Vercel OIDC token (on
 * Vercel, and locally after `vercel env pull`). Model refs are pi-ai's:
 * `{ provider: GATEWAY_PROVIDER, modelId: 'anthropic/claude-sonnet-4.5' }`.
 * Any other pi-ai provider works the same; this is only the Gateway's.
 */
export function gatewayModels(options: { apiKey?: () => Promise<string> } = {}): MutableModels {
  const apiKey = options.apiKey ?? gatewayCredential;
  const models = createModels({
    // The Gateway provider reads AI_GATEWAY_API_KEY from its auth context: a fresh credential per request.
    authContext: {
      env: async (key: string) => (key === 'AI_GATEWAY_API_KEY' ? apiKey() : process.env[key]),
      fileExists: async () => false,
    },
  });
  models.setProvider(vercelAIGatewayProvider());
  return models;
}

// ---------- optional: reading a session ----------

/** A session as its log has it: entries, Pi's live partial, tasks, commits, and whether work is unfinished. */
export type SessionSnapshot = PiSnapshot;
export type { PiCommit as SessionCommit, PiEntry as SessionEntry, PiTask as SessionTask };

/** A read-only follower of a session's log. */
export interface SessionReader {
  readonly sessionId: string;
  /** Read what the log gained since the last refresh. Resolves true when it gained anything. */
  refresh(): Promise<boolean>;
  /** The session as of the last refresh. */
  snapshot(): Promise<SessionSnapshot>;
}

/**
 * A read-only follower of a session for UIs. It never opens the session, so
 * it never takes it from the process driving it (opening a Harness does).
 */
export function openWorldReader(sessionId: string, options: Pick<WorldStorageOptions, 'world'> = {}): SessionReader {
  let reader: Promise<JournalReader> | undefined;
  const made = () => (reader ??= (options.world ? Promise.resolve(options.world) : getWorld()).then((world) => new JournalReader(world, sessionId)));
  return {
    sessionId,
    refresh: async () => (await made()).refresh(),
    snapshot: async () => (await made()).snapshot(),
  };
}

// ---------- optional: a chat route ----------

export interface PiChatRouteOptions extends WorldStorageOptions {
  /** Your opener (what `resumeWith` returned). */
  open: Opener;
  /** The root conversation's agent (`{ model, instructions }`), applied when the root is created. */
  agent?: AgentChange;
  /**
   * Who may use the route: a function that checks the caller may use the
   * session (undefined: a POST is about to start one) and throws a `Response`
   * to refuse, or `'public'` for anyone. Required in production, as for every
   * durable-harness chat route: without it, every request there is a 401.
   */
  authorize?: ChatAuthorize;
  /** A comment this often keeps proxies from closing a quiet response. Default 15 s. */
  keepaliveMs?: number;
  /**
   * Serve `authorize: 'public'` when the app's tools run in the session's
   * Vercel Sandbox (`sandboxEnv()`): each session then boots a VM and
   * creates a Drive, for anyone who can reach the route, within its
   * `sandbox({ budget })`. Without it, in every session this route starts
   * a call that would boot one fails instead, saying why, before any VM
   * boots, and so it does with it while the workspace's
   * `budget.sessionUsd` is `Infinity` (the default). The route records
   * itself in each session's config, and `sandboxEnv()` checks that at
   * every attach: this route can't check it when it is made, since the
   * Harness (and its `env`) is the app's own, opened in `open`.
   */
  allowPublicSandbox?: boolean;
}

const READERS = Symbol.for('durable-harness//pi-durable//readers');

/** A snapshot of the session from its log, read incrementally by one reader per session in this process. */
function snapshotter(world: () => Promise<World>): (sessionId: string) => Promise<PiSnapshot> {
  return async (sessionId) => {
    const w = await world();
    const g = globalThis as { [READERS]?: WeakMap<World, Map<string, { reader: JournalReader; busy: Promise<unknown> }>> };
    const byWorld = (g[READERS] ??= new WeakMap());
    let readers = byWorld.get(w);
    if (!readers) byWorld.set(w, (readers = new Map()));
    let entry = readers.get(sessionId);
    if (!entry) {
      entry = { reader: new JournalReader(w, sessionId), busy: Promise.resolve() };
      readers.set(sessionId, entry);
      while (readers.size > 64) readers.delete(readers.keys().next().value as string);
    }
    const e = entry;
    const next = e.busy.then(async () => {
      await e.reader.refresh();
      return e.reader.snapshot();
    });
    e.busy = next.catch(() => {});
    try {
      return await next;
    } catch (error) {
      readers.delete(sessionId);
      throw error;
    }
  };
}

/**
 * A chat route on Pi's own API (optional: your own route with
 * `acquireHarness` and `root.submit` is the same thing).
 *
 * POST `{ sessionId?, text, kind?, requestId? }` submits the text to the
 * session's root conversation (`kind: 'steer'` lands after the current tool
 * round, Pi's `whenBusy: 'steer'`; a retried request with the same
 * `requestId` finds the same submission), drives the turn in this invocation,
 * and streams it as server-sent events until the session is idle: `text`
 * deltas, `tool_start` / `tool_end`, `idle`, then `event: end`, the events
 * every durable-harness chat route sends. GET `?sessionId=` follows a turn
 * that is under way from the log, without opening the session (a reconnect
 * sends the in-flight response again from its start: Pi's live partial has
 * no cursor), and answers 204 when the session is idle. A `rewind` event
 * means the model call whose text came before it restarted (a crash): drop
 * that call's text.
 */
export function chatRoute(options: PiChatRouteOptions): ChatRoute {
  const gate = chatGate(options.authorize);
  const { open, agent, keepaliveMs, authorize, allowPublicSandbox, ...given } = options;
  // As every durable-harness chat route: a public route's traces are public unless you say otherwise.
  const storage: WorldStorageOptions = { ...given, traceAudience: given.traceAudience ?? (authorize === 'public' ? 'public' : 'unknown') };
  // A public route records itself in each session it creates (sandbox-harness.ts): sandbox() boots no VM for one without
  // allowPublicSandbox: true and a finite budget.sessionUsd. The other agents' routes also refuse that when they are
  // made, from their harness's sandbox mark; pi-durable has no harness object to mark (the app opens its own Pi Harness,
  // with its own env, in `open`), so here the acquire's check is the only one.
  const created = authorize === 'public' ? publicSandboxConfig(allowPublicSandbox) : undefined;
  const read = snapshotter(() => (storage.world ? Promise.resolve(storage.world) : getWorld()));
  async function submit(sessionId: string | undefined, text: string, kind: 'prompt' | 'steer' | 'followUp', requestId: string) {
    for (let attempt = 0; ; attempt++) {
      const lease = await leaseHarness(sessionId, open, storage, created);
      try {
        const before = await read(lease.storage.sessionId);
        const root = await lease.harness.root(ctx, agent ? { agent } : {});
        const submission = await root.submit({ type: 'input', content: text, requestId, whenBusy: kind === 'steer' ? 'steer' : 'followUp' }, ctx);
        const driven = (async (): Promise<Drive> => {
          try {
            // Background work the turn started (a compaction Pi began on its own, a background subagent) too:
            // releasing the last lease closes the Harness, which would abort it and leave it to a wake.
            const left = await untilSettled(lease.harness);
            // Nothing this process can run is left (work no deployment here can run may be): the turn is over.
            return !left && !lease.storage.isFenced ? 'settled' : 'lost';
          } catch {
            // Fenced (another process took the session over): the stream follows the log until idle.
            return 'lost';
          } finally {
            await lease.release().catch(() => {});
          }
        })();
        waitUntil(driven);
        return { sessionId: lease.storage.sessionId, before, submissionId: String(submission.id), driven };
      } catch (error) {
        await lease.release().catch(() => {});
        // The Harness this process held lost the session to another process: open a fresh one, once.
        if (attempt === 0 && lease.storage.isFenced) {
          sessionId = lease.storage.sessionId;
          continue;
        }
        throw error;
      }
    }
  }
  return {
    async POST(request) {
      const closed = gate.unconfigured();
      if (closed) return closed;
      let body: { sessionId?: unknown; text?: unknown; kind?: unknown; requestId?: unknown };
      try {
        body = await request.json();
      } catch {
        return new Response('expected a JSON body', { status: 400 });
      }
      const { text, kind = 'prompt', requestId } = body ?? {};
      if (typeof text !== 'string' || !text) return new Response('text is required', { status: 400 });
      if (kind !== 'prompt' && kind !== 'steer' && kind !== 'followUp') return new Response('kind must be one of prompt, steer, followUp', { status: 400 });
      if (body.sessionId !== undefined && typeof body.sessionId !== 'string') return new Response('sessionId must be a string', { status: 400 });
      if (requestId !== undefined && typeof requestId !== 'string') return new Response('requestId must be a string', { status: 400 });
      const refused = await gate.check(request, body.sessionId);
      if (refused) return refused;
      const turn = await submit(body.sessionId, text, kind, requestId ?? crypto.randomUUID());
      return turnStream(read, turn.sessionId, turn.before, request.signal, turn.driven, keepaliveMs, { 'x-session-id': turn.sessionId, 'x-submission-id': turn.submissionId });
    },
    async GET(request) {
      const closed = gate.unconfigured();
      if (closed) return closed;
      const sessionId = new URL(request.url).searchParams.get('sessionId');
      if (!sessionId) return new Response('sessionId is required', { status: 400 });
      const refused = await gate.check(request, sessionId);
      if (refused) return refused;
      const now = await read(sessionId);
      if (!now.unfinished) return new Response(null, { status: 204 });
      return turnStream(read, sessionId, now, request.signal, undefined, keepaliveMs, { 'x-session-id': sessionId });
    },
  };
}

/** How often a turn's stream reads the log. pi-durable commits its streamed partial about every 100 ms. */
const POLL_MS = 150;

/** How a chat route's own drive of a turn ended: nothing it could run was left (`settled`), or it lost the session (`lost`). */
type Drive = 'settled' | 'lost';

/** A Pi tool task's tool name: from the assistant entry whose tool call it runs (`input.callId`). */
function toolNameOf(snap: PiSnapshot, input: unknown): string | undefined {
  const i = input as { name?: unknown; toolName?: unknown; callId?: unknown } | undefined;
  if (typeof i?.name === 'string') return i.name;
  if (typeof i?.toolName === 'string') return i.toolName;
  if (typeof i?.callId !== 'string') return undefined;
  for (const e of snap.entries) {
    if (e.kind !== 'pi.assistant') continue;
    const content = (e.model?.[0] as { content?: { type?: string; id?: string; name?: string }[] } | undefined)?.content;
    const call = Array.isArray(content) ? content.find((c) => c?.type === 'toolCall' && c.id === i.callId) : undefined;
    if (typeof call?.name === 'string') return call.name;
  }
  return undefined;
}

const textOf = (content: unknown): string =>
  Array.isArray(content) ? content.filter((c) => c?.type === 'text').map((c) => String(c.text ?? '')).join('') : '';

/**
 * A turn as server-sent events, read from the journal: `text` deltas (of the
 * live partial while a model call streams, then of the response it
 * committed), `tool_start` / `tool_end`, and `idle` once nothing is left to
 * do. `before`: the session as it was before the message (or at a
 * reconnect), so only what follows is streamed; the in-flight response's
 * partial is sent from its start.
 */
function turnStream(
  read: (id: string) => Promise<PiSnapshot>,
  sessionId: string,
  before: PiSnapshot,
  signal: AbortSignal,
  driven: Promise<Drive> | undefined,
  keepaliveMs = 15_000,
  headers: Record<string, string> = {}
): Response {
  const encoder = new TextEncoder();
  let done = !driven;
  let drive: Drive | undefined;
  void driven?.then((d) => {
    drive = d;
    done = true;
  });
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      let open = true;
      const send = (s: string) => {
        if (!open) return;
        try {
          controller.enqueue(encoder.encode(s));
        } catch {
          open = false;
        }
      };
      let seq = 0;
      const event = (e: unknown) => send(`id: ${++seq}\ndata: ${JSON.stringify(e)}\n\n`);
      const keepalive = setInterval(() => send(': keepalive\n\n'), keepaliveMs);
      const oldTasks = new Set(before.tasks.filter((t) => t.status === 'terminal').map((t) => t.id));
      const seenEntries = new Set(before.entries.map((e) => e.id));
      const tools = new Map<string, 'started' | 'ended'>();
      /** The live partial's attempt, and how much of it this response has sent. */
      let live: { attempt: number; text: string } | undefined;
      let busy = false;
      send('retry: 1000\n\n');
      try {
        while (open && !signal.aborted) {
          // This invocation's drive settled before this read: the log has all of it, so this read is the last.
          const last = drive === 'settled';
          const snap = await read(sessionId).catch(() => undefined);
          if (snap) {
            // Text: the in-flight response's committed partial while it streams,
            // then the response once it is an entry. A new attempt of the call
            // (a rerun after a crash) starts over: a `rewind` says so first.
            // A resumed request first records its old partial as an aborted
            // assistant entry. Process entries before the current partial: both
            // the replacement's partial and its final entry can arrive in one poll.
            for (const [index, e] of snap.entries.entries()) {
              if (seenEntries.has(e.id)) continue;
              seenEntries.add(e.id);
              if (e.kind !== 'pi.assistant') continue;
              const message = e.model?.[0] as { content?: unknown; stopReason?: string } | undefined;
              // Keep a terminal cancellation's partial; discard it only when
              // the same generation task has restarted or written a later answer.
              const superseded = message?.stopReason === 'aborted' && e.byTaskId && (
                (snap.live?.generation && e.byTaskId === String(snap.live.run?.taskId)) ||
                snap.entries.slice(index + 1).some((next) => next.kind === 'pi.assistant' && next.byTaskId === e.byTaskId)
              );
              if (superseded) {
                if (live?.text) event({ type: 'rewind' });
                live = undefined;
                continue;
              }
              const text = textOf(message?.content);
              const prior = live?.text ?? '';
              if (!text.startsWith(prior)) event({ type: 'rewind' });
              const rest = text.startsWith(prior) ? text.slice(prior.length) : text;
              if (rest) event({ type: 'text', delta: rest });
              live = undefined;
            }
            const generation = snap.live?.generation;
            if (generation?.message) {
              const text = textOf(generation.message.content);
              if (live && live.attempt !== generation.attempt && live.text) {
                event({ type: 'rewind' });
                live = undefined;
              }
              live ??= { attempt: generation.attempt, text: '' };
              if (text.length > live.text.length && text.startsWith(live.text)) {
                event({ type: 'text', delta: text.slice(live.text.length) });
                live.text = text;
              }
            }
            for (const t of snap.tasks) {
              if (oldTasks.has(t.id) || t.kind !== 'pi.tool') continue;
              const name = toolNameOf(snap, t.input);
              if (t.starts.length && !tools.has(t.id)) {
                tools.set(t.id, 'started');
                event({ type: 'tool_start', id: t.id, ...(typeof name === 'string' ? { name } : {}) });
              }
              if (t.status === 'terminal' && tools.get(t.id) === 'started') {
                tools.set(t.id, 'ended');
                event({ type: 'tool_end', id: t.id, ...(typeof name === 'string' ? { name } : {}) });
              }
            }
            if (snap.unfinished) busy = true;
            // Idle: nothing left to do, once this turn's work was seen (or the drive that ran it is over); or
            // nothing left that this deployment can run (a task whose definition is not installed here).
            if ((!snap.unfinished && (busy || done)) || last) {
              event({ type: 'idle' });
              send('event: end\ndata: {}\n\n');
              break;
            }
          }
          await new Promise((r) => setTimeout(r, POLL_MS));
        }
      } finally {
        clearInterval(keepalive);
        if (open) {
          open = false;
          try {
            controller.close();
          } catch {}
        }
      }
    },
  });
  return new Response(body, {
    headers: { ...headers, 'content-type': 'text/event-stream', 'cache-control': 'no-cache, no-transform', 'x-accel-buffering': 'no' },
  });
}

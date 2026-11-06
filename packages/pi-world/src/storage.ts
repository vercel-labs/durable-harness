import type { Context } from '@earendil-works/chord';
import { createContextKey } from '@earendil-works/chord/context';
import {
  type ConversationId,
  type ConversationQuery,
  type Cursor,
  type DocumentAddress,
  type DocumentId,
  type DocumentPoint,
  type DocumentQuery,
  type EntryId,
  type EntryQuery,
  type Id,
  MemoryStorage,
  type Seq,
  type Storage,
  type StorageWrite,
  type SubmissionId,
  type SubmissionQuery,
  type TaskId,
  type TaskQuery,
} from '@earendil-works/pi-durable';
import type { Event, EventListResponse, ValidQueueName, World } from '@workflow/world';
import { eventIdToSlot } from '@workflow/world';
// Subpaths, not the barrel: a bundle of pi-world then carries only these.
import { decode, encode } from 'harness-world/codec';
import { type Anchor, type AnchorPolicy, anchorKey, holdAnchor } from 'harness-world/anchor';
import { covers, type InBandCounters, inBandFenceSupported, isDefinitiveRefusal, isInBandSuperseded, probeInBandCounters } from 'harness-world/fence';
import { workflowNameFor } from 'harness-world/names';
import { agentRunsHook, type TraceAudience, traceAudienceOf } from 'harness-world/agent-runs';
import { EXECUTION_CONTEXT, PI_WAKE_QUEUE } from 'harness-world/queue';
import { monotonicFactory } from 'ulid';
import { PiTracing } from './agent-runs.ts';
import { SessionFiles } from './files.ts';
import { type SandboxRecordValue, SessionSandbox } from './sandbox-record.ts';
import type { DriveFence } from 'harness-world';

const ulid = monotonicFactory();

/** The queue a session's wakes go to, on the kernel's prefix: any `__harness_wkf_workflow_` handler receives them. */
export const WAKE_QUEUE = PI_WAKE_QUEUE as ValidQueueName;

export const journalToken = (runId: string) => `pi:${runId}:journal`;

/** A run id that is `wrun_` and a ULID; the capture is the ULID. */
const RUN_ULID = /^wrun_([0-7][0-9A-HJKMNP-TV-Z]{25})$/;

/**
 * The correlation id of a session's journal hook: `hook_` and the run's own
 * ULID, so a run's journal hook is always the same id.
 *
 * It has to be `hook_<ULID>`: world-vercel rejects any other correlation id
 * with a 400 (`validation-error`; its backend validates every correlation id
 * as `step_`, `hook_`, `wait_` or `attr_` followed by a ULID). Sessions
 * written before this (`hook_journal_<ULID>`, accepted by world-local,
 * world-postgres and world-browser) still read: readers find the journal hook
 * by its token (`journalToken`), never by its id. A run id that is not
 * `wrun_<ULID>` (other Worlds accept any string) gets a fresh ULID.
 */
export const journalHookId = (runId: string) => `hook_${RUN_ULID.exec(runId)?.[1] ?? ulid()}`;

/**
 * Another process committed to this session after this one last read it.
 * Nothing from the rejected commit is visible to anyone (see `WorldStorage`),
 * and every later operation on this storage rejects the same way: the session
 * belongs to the other process now. Reopen it to continue.
 */
export class StorageFenced extends Error {
  constructor(readonly sessionId: string) {
    super(`pi-world: session ${sessionId} was committed to by another process; this storage no longer owns it, reopen it`);
    this.name = 'StorageFenced';
  }
}

/**
 * Who a Harness's storage is, for code that holds only what pi-durable hands
 * it (an `ExecutionEnv` builder gets the Harness as a `DocumentReader`, and
 * no session id): a read through the Harness with this key in its context
 * reaches the storage, which calls the value (`(storage) => void`) with
 * itself. Reading a document nobody writes reaches the storage on every call
 * (pi-durable caches only documents that exist). durable-harness's
 * `virtualEnv()` finds the session's files this way.
 */
export const STORAGE_PROBE = createContextKey('pi-world.storage-probe');

/** One Pi commit as it sits in the run's event log. */
interface JournalEntry {
  v: 1;
  /** Slot of the newest valid journal entry the writer had seen (the hook's slot for the first). */
  after: number;
  /** The Pi sequence the commit was given. */
  seq: number;
  /** The commit's writes as `MemoryStorage.prepareCommit` detached them. */
  writes: StorageWrite[];
  /** The writing process (one id per open storage): tells a rerun after a crash from the first run. */
  by?: string;
  /**
   * What the commit was written under: the queue delivery (or deadline drive)
   * holding the session when it was made (harness-world's `Anchor`). A wake
   * that finds the session unfinished judges this, and nothing else, to tell
   * whether its writer may still be at work (see `runWake`).
   */
  anchor?: Anchor;
}

/**
 * A storage that closed with tasks still live says so (the pi-world twin of
 * harness-world's `close` fact): its writer will commit nothing more, so a
 * wake may take the session at once. Written with `after: -1`, so no reader
 * ever counts it as a commit (readers written before it skip it as a lost
 * race), and so it never fences anyone.
 */
interface CloseEntry {
  v: 1;
  close: true;
  after: -1;
  by: string;
  anchor?: Anchor;
}

/**
 * The session's sandbox record (sandbox-record.ts), written by the owner
 * (`writeSandbox`): in-band at the World's count like a commit, so a writer
 * that lost the session is refused, and with `after: -1`, so no reader ever
 * counts it as a commit (readers written before it skip it as a lost race)
 * and it never fences anyone's commit.
 */
interface SandboxEntry {
  v: 1;
  sandbox: Record<string, unknown>;
  after: -1;
  by: string;
  anchor?: Anchor;
}

export type { TraceAudience } from 'harness-world/agent-runs';

export interface WorldStorageOptions {
  /**
   * What this storage's commits are written under (see `JournalEntry.anchor`):
   * the queue message of the delivery that opens it, or a deadline anchor for
   * a drive that holds none. Whoever shares an open storage later sets its own
   * with `setAnchor`. Without one, commits carry no anchor and a wake judges
   * them by its World's rule for anchorless intents.
   */
  anchor?: Anchor;
  /** Workflow name recorded on a run this creates. Default `pi-harness`. */
  name?: string;
  /**
   * Content classification for Agent Runs. Recorded on a session this
   * storage creates, so every later storage of it (a wake's too) traces the
   * same way; on a session that exists, it overrides the recorded one for
   * this storage. Omitted: the session's recorded audience, else
   * `defaultTraceAudience`, else the integration's default.
   */
  traceAudience?: TraceAudience;
  /** The audience for a session that recorded none (a wake's opener's, say). */
  defaultTraceAudience?: TraceAudience;
  /**
   * More of the session's config, recorded on a session this storage
   * creates, next to `traceAudience` (durable-harness's chat route records
   * the route that started it), and read back by every storage of it
   * (`WorldStorage.config`). Ignored for a session that exists.
   */
  config?: Readonly<Record<string, unknown>>;
  /**
   * Make this storage its own backstop, for a writer that holds no queue
   * message (a request that opened the session itself): see `Backstop`.
   * Without it, commits queue nothing and carry `anchor` (a queue delivery's
   * storage: its message is its backstop, redelivered if its handler dies).
   */
  backstop?: Backstop;
}

/**
 * A storage that stands behind its own commits. Every commit that may leave
 * work unfinished (a live task once it lands) is written under a deadline
 * anchor of this storage's (`anchors.direct`), and where the World's queue
 * outlives this process (`companion`: Vercel) a wake delayed past that
 * deadline is queued first, so a process that dies after any commit is
 * resumed by the wake behind it. A holder that keeps committing renews its
 * anchor once it is less than half of `recoverAfterMs` from its deadline (Pi
 * commits a streamed partial about every 100 ms, and every task step):
 * nothing is written for liveness. While a delivery shares the open storage
 * (`setAnchor`), its message is the backstop instead. The anchor is held in
 * this process (`holdAnchor`) until it is replaced, the storage closes or a
 * commit is refused, so a wake here acks while this writer lives. A storage
 * that closes with work left queues a continuation at once (after its close
 * entry). Where the queue dies with the process (world-local, world-postgres)
 * nothing is queued: the anchor names the process, and the next start's
 * recovery finds it gone.
 */
export interface Backstop {
  /** How the World holds deliveries (harness-world's `AnchorPolicy`, `defaultAnchors()` for the environment's). */
  anchors: AnchorPolicy;
  /**
   * How long this writer may stay silent with work unfinished before a wake
   * may take the session over: never before half of this after its last
   * commit, at the latest all of it. A silent crash is resumed within this
   * plus 5 s (Vercel), or at the next start (world-local, world-postgres).
   */
  recoverAfterMs: number;
  /** Called with a promise that settles when this storage has no unfinished work (or closes, or is fenced): a request's `waitUntil`. */
  keepAlive?: (work: Promise<void>) => void;
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

const TASK_LIVE = ['pending', 'running', 'waiting', 'completing'] as const;

/** Everything a replay of a run's log produces. */
interface Replayed {
  memory: MemoryStorage;
  /** The tasks whose newest record is live (`TASK_LIVE`). */
  live: Set<string>;
  hookId: string | undefined;
  /** Highest slot in the log. */
  eventCount: number;
  /** Slot of the newest valid journal entry (or of the journal hook). */
  lastValid: number;
  /** The World's in-band count for this log (its first page's snapshot), on a World with the in-band fence. */
  seqInBand?: number;
  /** The trace audience the session recorded when it was created (its run's input). */
  traceAudience?: TraceAudience;
  /** The whole config the session recorded when it was created (its run's input's `config`), frozen. */
  config?: unknown;
  /** The session's virtual workspace: the fold of its tool results' file changes (files.ts). */
  files: SessionFiles;
  /** The session's sandbox record (sandbox-record.ts). */
  sandbox: SessionSandbox;
}

async function listEvents(world: World, runId: string): Promise<Event[]> {
  return (await loadEvents(world, runId)).events;
}

/** Worlds seen to fence without listing a snapshot (world-vercel): an open there probes before it lists. */
const listsNoSnapshot = new WeakSet<object>();

/**
 * The whole log, and the in-band count the World read before listing it:
 * the first page's `snapshot`, or with `probe` the counters of a write the
 * fence refused just before (`probeInBandCounters`, for a World that fences
 * but lists no snapshot). The count is kept only when the listing covers
 * every position it may count (`covers`).
 */
async function loadEvents(world: World, runId: string, probe = false): Promise<{ events: Event[]; seqInBand?: number; snapshot: boolean }> {
  const probed = probe ? await probeInBandCounters(world, runId) : undefined;
  const events: Event[] = [];
  let cursor: string | undefined;
  let counted: InBandCounters | undefined;
  let snapshot = false;
  for (let first = true; ; first = false) {
    const page = (await world.events.list({ runId, pagination: { sortOrder: 'asc', ...(cursor ? { cursor } : {}) } } as never)) as EventListResponse;
    if (first) {
      snapshot = !!page.snapshot;
      counted = page.snapshot ?? probed;
    }
    events.push(...(page.data as Event[]));
    if (!page.hasMore || !page.cursor) break;
    cursor = page.cursor;
  }
  const seqInBand = counted && covers(events.length, counted) ? counted.seqInBand : undefined;
  return { events, snapshot, ...(seqInBand !== undefined ? { seqInBand } : {}) };
}

/**
 * A journal entry as a `hook_received` payload: its JSON text, as one string
 * in the kernel's payload codec (`devl`). JSON because Pi's records are JSON
 * values and JSON keeps everything conformance asks for (`__proto__` as a
 * plain key, lone surrogates, which it escapes), where devalue refuses
 * `__proto__` keys. Bytes because world-vercel takes nothing else as an event
 * payload (a string throws in its client before anything is sent); `devl` is
 * the Workflow runtime's own wire shape, so tools that decode runtime payloads
 * (`workflow inspect`, the dashboard) can show it as text.
 */
export const encodeJournalEntry = (entry: unknown): Uint8Array => encode(JSON.stringify(entry));

/**
 * The journal entry in a `hook_received` payload. Reads both shapes: bytes
 * (`encodeJournalEntry`), and the bare JSON text that sessions written
 * before 2026-10-07 carry on world-local, world-postgres and world-browser.
 */
export function decodeJournalPayload(payload: unknown): unknown {
  const text = payload instanceof Uint8Array ? decode(payload) : payload;
  if (typeof text !== 'string') throw new Error('pi-world: a journal payload is not JSON text');
  return JSON.parse(text);
}

const readEntry = (event: Event) => decodeJournalPayload((event.eventData as { payload: unknown }).payload) as JournalEntry;

const slotOf = (event: Event, fallback: number) => eventIdToSlot(event.eventId) ?? fallback;

/**
 * Rebuild Pi's state from the log. A journal entry counts only when it was
 * written on top of the newest entry that counted (`after`): a writer that
 * lost a race still lands an event (Worlds bump a contended append to the
 * next slot), and this rule makes every reader, including the writer itself,
 * ignore it.
 */
async function replay(world: World, runId: string, options: { fence?: boolean } = {}): Promise<Replayed> {
  const memory = new MemoryStorage();
  const live = new Set<string>();
  const files = new SessionFiles();
  const sandbox = new SessionSandbox();
  let hookId: string | undefined;
  let eventCount = 0;
  let lastValid = 0;
  // A writer's replay needs the World's count. A World that fences but lists
  // no snapshot is asked with a probe; the first time, that costs a second load.
  const fenceable = !!options.fence && inBandFenceSupported(world);
  let loaded = await loadEvents(world, runId, fenceable && listsNoSnapshot.has(world));
  if (fenceable && !loaded.snapshot && loaded.seqInBand === undefined && !listsNoSnapshot.has(world)) {
    listsNoSnapshot.add(world);
    loaded = await loadEvents(world, runId, true);
  }
  const { events, seqInBand } = loaded;
  let traceAudience: TraceAudience | undefined;
  let config: unknown;
  for (const event of events) {
    const slot = slotOf(event, eventCount + 1);
    eventCount = Math.max(eventCount, slot);
    if (event.eventType === 'run_created') {
      config = recordedConfig(event);
      traceAudience = traceAudienceOf((config as { traceAudience?: unknown } | undefined)?.traceAudience);
    }
    else if (event.eventType === 'hook_created' && (event.eventData as { token?: string }).token === journalToken(runId)) {
      hookId = event.correlationId;
      lastValid = slot;
    } else if (hookId && event.eventType === 'hook_received' && event.correlationId === hookId) {
      const entry = readEntry(event) as JournalEntry | SandboxEntry;
      // A sandbox record that landed was its owner's: written in-band (sandbox() needs the World's fence).
      if ('sandbox' in entry) sandbox.set(entry.sandbox);
      if (entry.after !== lastValid) continue;
      const commit = entry as JournalEntry;
      memory.prepareCommit(commit.writes, commit.seq as Seq).apply();
      trackLive(live, commit.writes);
      files.fold(commit.writes);
      sandbox.fold(commit.writes);
      lastValid = slot;
    }
  }
  return {
    memory,
    live,
    files,
    sandbox,
    hookId,
    eventCount,
    lastValid,
    ...(seqInBand !== undefined && fenceable ? { seqInBand } : {}),
    ...(traceAudience ? { traceAudience } : {}),
    ...(config !== undefined ? { config } : {}),
  };
}

/** The config a session's `run_created` recorded (`createRun`: its input's `config`), deep-frozen; undefined when it recorded none. */
function recordedConfig(event: Event): unknown {
  try {
    const input = (event.eventData as { input?: unknown } | undefined)?.input;
    const decoded = input instanceof Uint8Array ? decode(input) : input;
    const config = (decoded as { config?: unknown } | undefined)?.config;
    return config === undefined ? undefined : frozen(config);
  } catch {
    return undefined;
  }
}

/** `value`, frozen all the way down (a decoded config is the storage's own copy). */
function frozen<T>(value: T): T {
  if (typeof value === 'object' && value !== null && !ArrayBuffer.isView(value) && !Object.isFrozen(value)) {
    for (const v of Object.values(value)) frozen(v);
    Object.freeze(value);
  }
  return value;
}

const isLive = (status: unknown) => (TASK_LIVE as readonly unknown[]).includes(status);

/** Apply a commit's task writes to the set of live tasks. */
function trackLive(live: Set<string>, writes: readonly StorageWrite[]): void {
  for (const w of writes) {
    if (w.type !== 'task') continue;
    const id = String(w.value.id);
    const status = w.value.state.status;
    if (isLive(status)) live.add(id);
    else live.delete(id);
  }
}

/** Whether a task would be live once `writes` land on top of `live`. */
function liveAfter(live: ReadonlySet<string>, writes: readonly StorageWrite[]): boolean {
  const changed = new Map<string, boolean>();
  for (const w of writes) {
    if (w.type !== 'task') continue;
    const id = String(w.value.id);
    const status = w.value.state.status;
    changed.set(id, isLive(status));
  }
  for (const v of changed.values()) if (v) return true;
  for (const id of live) if (!changed.has(id)) return true;
  return false;
}

/** The newest commit that counted, and whether its writer closed after it. */
export interface JournalHead {
  /** Some task has not reached `terminal`. */
  unfinished: boolean;
  /** The newest valid commit's writer and anchor. */
  last?: { by?: string; anchor?: Anchor; slot: number };
  /** Its writer closed the storage after it (a close entry), so it writes nothing more. */
  closed: boolean;
  /** Highest slot in the log. */
  eventCount: number;
}

/** Read where a session stands without opening it: what a wake judges (see `runWake`). */
export async function journalHead(world: World, runId: string, context: Context): Promise<JournalHead> {
  const memory = new MemoryStorage();
  let hookId: string | undefined;
  let eventCount = 0;
  let lastValid = 0;
  let last: JournalHead['last'];
  let closed = false;
  for (const event of await listEvents(world, runId)) {
    const slot = slotOf(event, eventCount + 1);
    eventCount = Math.max(eventCount, slot);
    if (event.eventType === 'hook_created' && (event.eventData as { token?: string }).token === journalToken(runId)) {
      hookId = event.correlationId;
      lastValid = slot;
    } else if (hookId && event.eventType === 'hook_received' && event.correlationId === hookId) {
      const entry = readEntry(event) as JournalEntry | CloseEntry;
      if ('close' in entry && entry.close) {
        if (last && entry.by === last.by) closed = true;
        continue;
      }
      if (entry.after !== lastValid) continue;
      const commit = entry as JournalEntry;
      memory.prepareCommit(commit.writes, commit.seq as Seq).apply();
      lastValid = slot;
      last = { ...(entry.by ? { by: entry.by } : {}), ...(entry.anchor ? { anchor: entry.anchor } : {}), slot };
      closed = false;
    }
  }
  return { unfinished: hookId !== undefined && (await hasLiveTasks(memory, context)), ...(last ? { last } : {}), closed, eventCount };
}

async function hasLiveTasks(memory: Storage, context: Context): Promise<boolean> {
  for (const status of TASK_LIVE) {
    if ((await memory.scanTasks({ status }, 1, undefined, context)).items.length > 0) return true;
  }
  return false;
}

/**
 * A Pi `Storage` whose source of truth is a Workflow run's event log: the
 * session is the run.
 *
 * Every `commit()` is one `hook_received` event on the run's journal hook
 * carrying the commit's writes and Pi sequence. Reads are served by Pi's own
 * `MemoryStorage`, rebuilt on open by replaying the journal, so sequences and
 * IDs come out identical in every process. `mintId()` is `MemoryStorage`'s:
 * replay moves its counter past every ID a commit used, so a reopened storage
 * never hands out one already in the log.
 *
 * A commit is durable before it is visible: it is validated, written to the
 * log, and only then applied. Commits are Durable Strict: each resolves once
 * its event is stored.
 *
 * One owner at a time, like every Pi storage, but enforced: each write tells
 * the World how many events this process has seen (bump and report). If
 * another process committed in between, the commit rejects with
 * `StorageFenced`, nothing of it is applied (here or by any later reader),
 * and the storage refuses all further commits.
 *
 * On a World with the in-band writer fence (`capabilities.inBandFence`) every
 * commit is also an in-band write at the count the World gave with the replay
 * this storage opened from: a commit decided from a stale log is refused by
 * the World (`InBandSupersededError`) and never lands, and the storage is
 * fenced the same way. The `after` chain still decides for every reader, so
 * logs written without the fence (an older World, another release) replay as
 * before. The close entry is out-of-band: it never fences anyone.
 *
 * Nothing about liveness is written: no beats. Each commit records the
 * anchor it was written under (the queue delivery holding the session, see
 * `setAnchor`, or this storage's own deadline anchor with `backstop`), and a
 * storage that closes with tasks still live writes one close entry. A wake
 * (`runWake`) judges those. For a delivery's storage, the queue's own
 * redelivery of a delivery that died is what finishes the work a dead process
 * left behind; a storage with `backstop` queues the wake that stands behind
 * its commits itself (see `Backstop`).
 */
export class WorldStorage implements Storage {
  /** Agent Runs for the commits this storage makes, when `agentRuns()` is registered (undefined until the first commit, null without it). */
  private tracing?: PiTracing | null;
  private readonly traceName: string;
  private readonly traceAudience: WorldStorageOptions['traceAudience'];
  private queue: Promise<unknown> = Promise.resolve();
  private closing?: Promise<void>;
  private fenced?: StorageFenced;
  private dead = false;
  /** Whether a commit of this storage counted (so a close entry means something). */
  private committed = false;
  /** The anchors of whoever holds this storage open, newest last (`setAnchor`). */
  private readonly anchors: Anchor[] = [];
  /** This process's id as a writer: on every journal entry. */
  readonly writer = `pi_${ulid()}`;
  private readonly backstop?: Backstop;
  /** With `backstop`: this storage's own deadline anchor, and its hold in this process. */
  private own?: { anchor: Anchor; release: () => void };
  /** With `backstop`: settles when no work is left, or when this storage's own deadline passes unrenewed (`keepAlive`). */
  private busy?: { done: Promise<void>; settle: () => void; timer?: ReturnType<typeof setTimeout> };
  /** The count this storage's first accepted in-band write was checked against (`writerFence`). */
  private acceptedAt?: number;
  /** Aborted once this storage writes nothing more (`signal`). */
  private readonly ended = new AbortController();

  private constructor(
    /** The World the session lives on. */
    readonly world: World,
    readonly sessionId: string,
    private readonly hookId: string,
    private readonly memory: MemoryStorage,
    /** The tasks that are live as of this storage's view. */
    private readonly live: Set<string>,
    private eventCount: number,
    private lastValid: number,
    options: WorldStorageOptions,
    /** The World's in-band count as of this storage's view (undefined: commits are unfenced). */
    private fenceCount?: number,
    /** The session's virtual workspace as of this storage's view: the fold of its tool results' file changes (files.ts). */
    readonly files: SessionFiles = new SessionFiles(),
    /** The session's sandbox record as of this storage's view (sandbox-record.ts; `writeSandbox`). */
    readonly sandbox: SessionSandbox = new SessionSandbox(),
    /**
     * The session's config as its `run_created` recorded it (its trace
     * audience, and what `WorldStorageOptions.config` added when it was
     * created), deep-frozen; undefined when it recorded none. The sandbox
     * tier gets it with every acquire, for its admission checks.
     */
    readonly config?: unknown
  ) {
    this.traceName = options.name ?? 'pi-durable';
    this.traceAudience = options.traceAudience;
    if (options.anchor) this.anchors.push(options.anchor);
    if (options.backstop) this.backstop = options.backstop;
  }

  /**
   * Commits from now on are written under `anchor` (a delivery that shares
   * this open storage: Pi allows one owner per session, so a process's routes
   * share one). Returns its release: the anchor of whoever still holds the
   * storage applies again.
   */
  setAnchor(anchor: Anchor): () => void {
    this.anchors.push(anchor);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.releaseAnchor(anchor);
    };
  }

  /**
   * Stop writing under `anchor` (the one this storage was opened under, when
   * the delivery that opened it lets go while another holder of this process
   * keeps it open): the anchor below it applies again, or this storage's own
   * (`backstop`).
   */
  releaseAnchor(anchor: Anchor): void {
    const at = this.anchors.lastIndexOf(anchor);
    if (at !== -1) this.anchors.splice(at, 1);
  }

  /** The anchor the next commit is written under. */
  get anchor(): Anchor | undefined {
    return this.anchors.at(-1);
  }

  /** Another process committed to the session after this storage's view: it commits nothing more (reopen to continue). */
  get isFenced(): boolean {
    return this.fenced !== undefined;
  }

  /** Closed, or abandoned (`abandon()`): it commits nothing more. */
  get closed(): boolean {
    return this.closing !== undefined || this.dead;
  }

  /** Create a new session: a workflow run with an empty Pi journal. Returns its id. */
  static async create(world: World, options: Pick<WorldStorageOptions, 'name' | 'traceAudience' | 'config'> = {}): Promise<string> {
    // The World's own id when it mints one (world-vercel tags the region its data lives in), as the SDK's start() does.
    const sessionId = `wrun_${world.createRunId?.({}) ?? ulid()}`;
    await createRun(world, sessionId, options.name, options.traceAudience, options.config);
    return sessionId;
  }

  /** Open a session, creating its run first when the World has never seen it. */
  static async open(world: World, sessionId: string, options: WorldStorageOptions = {}): Promise<WorldStorage> {
    let replayed = await replay(world, sessionId, { fence: true });
    if (!replayed.hookId) {
      if (replayed.eventCount > 0) throw new Error(`pi-world: ${sessionId} is not a Pi session (no journal hook)`);
      await createRun(world, sessionId, options.name, options.traceAudience, options.config);
      replayed = await replay(world, sessionId, { fence: true });
      if (!replayed.hookId) throw new Error(`pi-world: could not create session ${sessionId}`);
    }
    const { memory, live, files, sandbox, hookId, eventCount, lastValid, seqInBand, config } = replayed;
    const traceAudience = options.traceAudience ?? replayed.traceAudience ?? options.defaultTraceAudience;
    return new WorldStorage(world, sessionId, hookId, memory, live, eventCount, lastValid, { ...options, ...(traceAudience ? { traceAudience } : {}) }, seqInBand, files, sandbox, config);
  }

  /**
   * Aborted once this storage writes nothing more: closed, abandoned, fenced
   * (another process committed), or after a commit whose outcome it cannot
   * know. What it holds for the session (the sandbox's link) ends with it.
   */
  get signal(): AbortSignal {
    return this.ended.signal;
  }

  /**
   * This storage's fencing token, for the sandbox tier (docs/workspaces.md
   * §4.1, P0.12): the in-band count its first accepted write was checked
   * against. The World accepts an in-band write only at its current count,
   * so accepted counts are unique and in log order: a later owner's first
   * accepted write comes after every write of the owner before it, and its
   * count is higher; within one storage the count stays the same. `fenced`
   * while its writes are still in-band: not once one had an unknown outcome,
   * the World refused one, or the storage let go (closed, abandoned).
   * Undefined before its first accepted in-band write (`claim`), and on a
   * World without the in-band fence.
   */
  get writerFence(): DriveFence | undefined {
    if (this.acceptedAt === undefined) return undefined;
    return Object.freeze({ count: this.acceptedAt, fenced: this.fenceCount !== undefined && !this.fenced && !this.closed && !this.ended.signal.aborted });
  }

  /**
   * Make sure this storage owns the session before its token is read: if it
   * has no accepted in-band write yet, write the sandbox record as it stands
   * (one in-band write; refused, `StorageFenced`, when another process
   * committed since this storage's view). Nothing on a World without the
   * in-band fence.
   */
  async claim(): Promise<void> {
    if (this.acceptedAt !== undefined || this.fenceCount === undefined) return;
    await this.writeSandbox(this.sandbox.record ?? {});
  }

  /**
   * Write the session's sandbox record (sandbox-record.ts): in-band, after
   * every commit already admitted, so a storage that lost the session is
   * refused (`StorageFenced`) and writes nothing more. A storage whose writes
   * are no longer in-band (an earlier write's outcome is unknown) keeps it in
   * memory only: out of band it could land after a newer owner's, and a
   * token that is not fenced is granted no VM time anyway.
   */
  writeSandbox(record: SandboxRecordValue): Promise<void> {
    if (this.dead) return new Promise<void>(() => {});
    if (this.closing) return Promise.reject(new Error('pi-world: storage is closed'));
    const run = this.queue.then(async () => {
      if (this.dead) return new Promise<void>(() => {});
      if (this.fenced) throw this.fenced;
      const fenceAt = this.fenceCount;
      if (fenceAt === undefined) {
        this.sandbox.set(record);
        return;
      }
      const anchor = this.anchor;
      const entry: SandboxEntry = { v: 1, sandbox: { ...record }, after: -1, by: this.writer, ...(anchor ? { anchor } : {}) };
      const expected = this.eventCount + 1;
      let result: Awaited<ReturnType<World['events']['create']>>;
      try {
        result = await this.world.events.create(
          this.sessionId,
          { eventType: 'hook_received', correlationId: this.hookId, eventData: { token: journalToken(this.sessionId), payload: encodeJournalEntry(entry) } },
          { eventCount: this.eventCount, inBand: true, expectedSeqInBand: fenceAt } as never
        );
      } catch (err) {
        if (isInBandSuperseded(err)) {
          this.fenceCount = undefined;
          throw this.fence();
        }
        if (!isDefinitiveRefusal(err)) this.fenceCount = undefined;
        throw err;
      }
      const allocated = (result as { allocated?: unknown }).allocated;
      this.fenceCount = fenceAt + (typeof allocated === 'number' ? allocated : result.event ? 1 : 0);
      this.acceptedAt ??= fenceAt;
      const slot = result.event ? eventIdToSlot(result.event.eventId) : null;
      if (slot === null) throw new Error('pi-world: the World did not report the slot of a sandbox record');
      if (slot !== expected && (await this.supersededBefore(expected, slot))) {
        this.eventCount = Math.max(this.eventCount, slot);
        throw this.fence();
      }
      this.eventCount = slot;
      this.sandbox.set(record);
    });
    this.queue = run.catch(() => {});
    return run;
  }

  /** Whether some task is live as of this storage's view (pending, running, waiting or completing). */
  get unfinished(): boolean {
    return this.live.size > 0;
  }

  /**
   * With `backstop`, before a commit that leaves work: make sure it is
   * written under a deadline anchor of this storage's that is at least half a
   * window away, and that the wake behind it is queued (where the queue
   * outlives the process). Not while a delivery shares the storage: its
   * message stands behind the commit.
   */
  private async renew(now = Date.now()): Promise<void> {
    const b = this.backstop;
    if (!b) return;
    const top = this.anchors.at(-1);
    if (top && top !== this.own?.anchor) return;
    const current = this.own?.anchor;
    if (current?.at !== undefined && current.at - now >= b.recoverAfterMs / 2) return;
    const { anchor, companion } = b.anchors.direct(now + b.recoverAfterMs);
    if (companion) {
      try {
        // The wake goes out before the commit it stands behind: queued first, the worst case is a wake with nothing to do.
        // A queue that refuses once is asked again (the key makes a retry one message): failing here refuses the commit, which poisons Pi's session.
        const send = () => wake(this.world, this.sessionId, { delaySeconds: Math.ceil(b.recoverAfterMs / 1000) + 5, idempotencyKey: `${this.sessionId}:pi-companion:${anchor.m}` });
        await send().catch(async () => {
          await new Promise((r) => setTimeout(r, 200));
          return send();
        });
      } catch (error) {
        // The anchor this storage holds still has a wake behind it: keep it, and renew at the next commit.
        if (current?.at !== undefined && current.at > now + 1_000) {
          b.log?.('pi-world: could not queue the backstop wake; the current anchor stands', { sessionId: this.sessionId, error: String(error) });
          return;
        }
        throw error;
      }
    }
    const previous = this.own;
    this.own = { anchor, release: holdAnchor(this.world, anchor) };
    const at = previous ? this.anchors.indexOf(previous.anchor) : -1;
    if (at === -1) this.anchors.unshift(anchor);
    else this.anchors[at] = anchor;
    previous?.release();
  }

  /** Let go of this storage's own anchor and of the request it kept alive: it writes nothing more. */
  private letGo(): void {
    this.own?.release();
    this.endBusy();
    this.ended.abort(new Error('pi-world: this storage writes nothing more'));
  }

  /** Let go of the request this storage keeps alive. */
  private endBusy(): void {
    const busy = this.busy;
    if (!busy) return;
    this.busy = undefined;
    if (busy.timer !== undefined) clearTimeout(busy.timer);
    busy.settle();
  }

  /**
   * With `backstop`, after a commit landed: keep the request alive while work
   * is left, let it go once none is. Never past this storage's own deadline
   * unrenewed: a holder silent that long is taken over by the wake behind its
   * anchor anyway, and work nobody here can run (a task whose definition is
   * not installed) would otherwise hold the request until the platform ends it.
   */
  private settled(unfinished: boolean): void {
    const b = this.backstop;
    if (!b) return;
    if (!unfinished) {
      this.endBusy();
      return;
    }
    if (!this.busy) {
      let settle!: () => void;
      const done = new Promise<void>((r) => (settle = r));
      this.busy = { done, settle };
    }
    const busy = this.busy;
    const at = this.anchor === this.own?.anchor ? this.own?.anchor.at : undefined;
    if (at !== undefined) {
      if (busy.timer !== undefined) clearTimeout(busy.timer);
      busy.timer = setTimeout(() => {
        if (this.busy === busy) this.endBusy();
      }, Math.max(0, at - Date.now()));
      (busy.timer as { unref?: () => void }).unref?.();
    }
    b.keepAlive?.(busy.done);
  }

  commit(writes: readonly StorageWrite[], context: Context): Promise<Seq> {
    // A dead process's work never lands, and nothing it waits on settles.
    if (this.dead) return new Promise<Seq>(() => {});
    if (this.closing) return Promise.reject(new Error('pi-world: storage is closed'));
    const run = this.queue.then(async () => {
      // Abandoned while this one waited its turn: it never lands either.
      if (this.dead) return new Promise<Seq>(() => {});
      if (this.fenced) throw this.fenced;
      // Validate before anything is written: a commit Pi's own storage would
      // refuse never reaches the log.
      const prepared = this.memory.prepareCommit(writes);
      const unfinished = liveAfter(this.live, prepared.writes);
      if (unfinished && this.backstop) await this.renew();
      const anchor = this.anchor;
      const entry: JournalEntry = { v: 1, after: this.lastValid, seq: prepared.seq, writes: [...prepared.writes], by: this.writer, ...(anchor ? { anchor } : {}) };
      const expected = this.eventCount + 1;
      // A commit is the owner's progress: in-band, at the World's count, when the World fences.
      const fenceAt = this.fenceCount;
      let result: Awaited<ReturnType<World['events']['create']>>;
      try {
        result = await this.world.events.create(
          this.sessionId,
          {
            eventType: 'hook_received',
            correlationId: this.hookId,
            eventData: { token: journalToken(this.sessionId), payload: encodeJournalEntry(entry) },
          },
          { eventCount: this.eventCount, ...(fenceAt !== undefined ? { inBand: true, expectedSeqInBand: fenceAt } : {}) } as never
        );
      } catch (err) {
        if (fenceAt !== undefined && isInBandSuperseded(err)) {
          // Another process committed in-band since this storage's view: nothing of this commit landed.
          this.fenceCount = undefined;
          throw this.fence();
        }
        // The outcome is unknown (it may have been counted): commit unfenced from here, by the `after` chain alone.
        if (fenceAt !== undefined && !isDefinitiveRefusal(err)) this.fenceCount = undefined;
        throw err;
      }
      if (fenceAt !== undefined) {
        const allocated = (result as { allocated?: unknown }).allocated;
        this.fenceCount = fenceAt + (typeof allocated === 'number' ? allocated : result.event ? 1 : 0);
        this.acceptedAt ??= fenceAt;
      }
      const slot = result.event ? eventIdToSlot(result.event.eventId) : null;
      if (slot === null) throw new Error('pi-world: the World did not report the slot of a journal write');
      if (slot !== expected && (await this.supersededBefore(expected, slot))) {
        this.eventCount = Math.max(this.eventCount, slot);
        throw this.fence();
      }
      this.eventCount = slot;
      this.lastValid = slot;
      this.committed = true;
      const seq = prepared.apply();
      trackLive(this.live, prepared.writes);
      this.files.fold(prepared.writes);
      this.sandbox.fold(prepared.writes);
      this.settled(unfinished);
      try { await this.traceWrites(prepared.writes, context); } catch { /* Telemetry never rejects a durable commit. */ }
      return seq;
    });
    // A refused commit (fenced, or an outcome this storage cannot know): Pi
    // has poisoned the session, so this writer holds nothing any more.
    run.catch(() => this.letGo());
    this.queue = run.catch(() => {});
    return run;
  }

  private async traceWrites(writes: readonly StorageWrite[], context: Context): Promise<void> {
    if (this.tracing === undefined) {
      const session = agentRunsHook()?.session({ world: this.world, runId: this.sessionId, agentName: this.traceName, ...(this.traceAudience ? { audience: this.traceAudience } : {}) });
      this.tracing = session ? new PiTracing(session, this.memory) : null;
    }
    // Only accepted writes are observed. Replaying a journal emits no spans.
    await this.tracing?.commit(writes, context);
  }

  /**
   * Whether a valid commit landed in slots `[from, to)`, written by someone
   * else while this one was in flight. Entries that themselves lost a race
   * (written on top of something older than our newest) do not count.
   */
  private async supersededBefore(from: number, to: number): Promise<boolean> {
    for (const event of await listEvents(this.world, this.sessionId)) {
      const slot = slotOf(event, 0);
      if (slot < from || slot >= to) continue;
      if (event.eventType !== 'hook_received' || event.correlationId !== this.hookId) continue;
      const entry = readEntry(event);
      // A close entry (`after: -1`) is never a commit.
      if (entry.after === this.lastValid) return true;
    }
    return false;
  }

  private fence(): StorageFenced {
    this.fenced ??= new StorageFenced(this.sessionId);
    this.ended.abort(this.fenced);
    return this.fenced;
  }

  /**
   * Stop as a crashed process would: no more commits (each one hangs
   * forever, so nothing waiting on it moves on), no close entry, nothing
   * settled. For tests and simulations; `close()` is the normal way out.
   */
  abandon(): void {
    this.dead = true;
    this.letGo();
  }

  async mintId<I extends Id<string>>(): Promise<I> {
    return this.read().mintId<I>();
  }
  async conversation(id: ConversationId, context: Context) {
    return this.read().conversation(id, context);
  }
  async scanConversations(query: ConversationQuery, limit: number, cursor: Cursor | undefined, context: Context) {
    return this.read().scanConversations(query, limit, cursor, context);
  }
  entry(id: EntryId, context: Context): ReturnType<Storage['entry']>;
  entry(conversationId: ConversationId, id: EntryId, context: Context): ReturnType<Storage['entry']>;
  async entry(a: ConversationId | EntryId, b: EntryId | Context, c?: Context): ReturnType<Storage['entry']> {
    const memory = this.read();
    return c === undefined ? memory.entry(a as EntryId, b as Context) : memory.entry(a as ConversationId, b as EntryId, c);
  }
  async findLatestHeadMarker(conversationId: ConversationId, atOrBeforeEntryId: EntryId | undefined, context: Context) {
    return this.read().findLatestHeadMarker(conversationId, atOrBeforeEntryId, context);
  }
  async scanEntries(query: EntryQuery, limit: number, cursor: Cursor | undefined, context: Context) {
    return this.read().scanEntries(query, limit, cursor, context);
  }
  async task(id: TaskId, context: Context) {
    return this.read().task(id, context);
  }
  async scanTasks(query: TaskQuery, limit: number, cursor: Cursor | undefined, context: Context) {
    return this.read().scanTasks(query, limit, cursor, context);
  }
  async submission(id: SubmissionId, context: Context) {
    return this.read().submission(id, context);
  }
  async scanSubmissions(query: SubmissionQuery, limit: number, cursor: Cursor | undefined, context: Context) {
    return this.read().scanSubmissions(query, limit, cursor, context);
  }
  async submissionByRequest(conversationId: ConversationId, requestId: string, context: Context) {
    return this.read().submissionByRequest(conversationId, requestId, context);
  }
  async findDocument(address: DocumentAddress, at: DocumentPoint, context: Context) {
    // Asked who it is (`STORAGE_PROBE`): a read that came through a Harness on this storage.
    (context.value(STORAGE_PROBE) as ((storage: WorldStorage) => void) | undefined)?.(this);
    return this.read().findDocument(address, at, context);
  }
  async document(id: DocumentId, at: DocumentPoint, context: Context) {
    return this.read().document(id, at, context);
  }
  async scanDocuments(query: DocumentQuery, limit: number, cursor: Cursor | undefined, context: Context) {
    return this.read().scanDocuments(query, limit, cursor, context);
  }

  /** Reads, like every Pi storage's, reject once closed. A fenced storage still reads its own last state. */
  private read(): MemoryStorage {
    if (this.closing) throw new Error('pi-world: storage is closed');
    return this.memory;
  }

  /**
   * Seal admission, settle commits already admitted, close once. A storage
   * whose commits left a task live says it closed (a close entry), so a wake
   * need not wait for anything to take the session.
   */
  close(context: Context): Promise<void> {
    // A crashed process closes nothing: it writes no close entry.
    if (this.dead) return (this.closing ??= Promise.resolve());
    this.closing ??= this.queue.then(async () => {
      try {
        if (this.committed && !this.fenced && (await hasLiveTasks(this.memory, context).catch(() => true))) {
          const anchor = this.anchor;
          const entry: CloseEntry = { v: 1, close: true, after: -1, by: this.writer, ...(anchor ? { anchor } : {}) };
          // Best effort: without it a wake judges the last commit's anchor, which
          // a dead holder's redelivery (or the World's proof) still settles.
          await this.world.events
            .create(
              this.sessionId,
              { eventType: 'hook_received', correlationId: this.hookId, eventData: { token: journalToken(this.sessionId), payload: encodeJournalEntry(entry) } },
              { eventCount: this.eventCount } as never
            )
            .catch(() => {});
          // A writer of its own that lets go with work left hands it on now
          // (a delivery's storage leaves that to its wake, `runWake`).
          if (this.backstop && (!anchor || anchor === this.own?.anchor)) {
            await wake(this.world, this.sessionId, { ...(anchor ? { continues: anchorKey(anchor) } : {}), idempotencyKey: `${this.sessionId}:pi-close:${this.writer}` }).catch((error: unknown) =>
              this.backstop?.log?.('pi-world: could not queue the continuation of a closed storage', { sessionId: this.sessionId, error: String(error) })
            );
          }
        }
      } finally {
        this.letGo();
      }
      // A turn with work left goes on in the storage a wake opens, from its checkpoint.
      await this.tracing?.flush().catch(() => {});
      await this.memory.close(context);
    });
    return this.closing;
  }
}

/**
 * The run and its journal hook, written the way the kernel starts a session
 * (`durableSessions().start`, the shape world-vercel accepts): each write
 * after `run_created` says how many events precede it.
 */
async function createRun(world: World, runId: string, name?: string, traceAudience?: TraceAudience, more?: Readonly<Record<string, unknown>>): Promise<void> {
  const deploymentId = await world.getDeploymentId();
  const config = { ...more, ...(traceAudience ? { traceAudience } : {}) };
  await world.events.create(runId, {
    eventType: 'run_created',
    specVersion: world.specVersion,
    // `executionContext`: a World's restart recovery re-enqueues the run on
    // the kernel's namespace, where these sessions' wakes are consumed. The
    // input's `config` (the kernel's place for it: `traceAudience`, and what
    // the opener added) travels with the session to every storage of it.
    eventData: { deploymentId, workflowName: workflowNameFor(name ?? 'pi-harness'), executionContext: { ...EXECUTION_CONTEXT }, input: encode({ harness: 'pi-harness', ...(Object.keys(config).length ? { config } : {}) }) },
  } as never);
  await world.events.create(runId, { eventType: 'run_started' } as never, { eventCount: 1 } as never);
  // Not a webhook: Worlds treat a hook that does not say so as public, and
  // the journal's token is guessable from the session id.
  await world.events.create(
    runId,
    { eventType: 'hook_created', correlationId: journalHookId(runId), eventData: { token: journalToken(runId), isWebhook: false } } as never,
    { eventCount: 2 } as never
  );
}

/** Queue a wake for a session now (or after `delaySeconds`); `continues` hands it the anchor (`anchorKey`) of the delivery that sent it. */
export async function wake(
  world: World,
  sessionId: string,
  options: { delaySeconds?: number; idempotencyKey?: string; continues?: string } = {}
): Promise<void> {
  await world.queue(WAKE_QUEUE, { runId: sessionId, input: { kind: 'pi-wake', ...(options.continues ? { continues: options.continues } : {}) } } as never, {
    idempotencyKey: options.idempotencyKey ?? `${sessionId}:pi-wake:${Date.now()}:${ulid()}`,
    ...(options.delaySeconds ? { delaySeconds: options.delaySeconds } : {}),
  });
}

/** Whether a session has a task that has not reached `terminal`, read from its log without opening it. */
export async function hasUnfinishedWork(world: World, sessionId: string, context: Context): Promise<boolean> {
  const { memory, hookId } = await replay(world, sessionId);
  return hookId !== undefined && (await hasLiveTasks(memory, context));
}

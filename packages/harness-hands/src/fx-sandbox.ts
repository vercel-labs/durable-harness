import { decodeLine, type DriveContext, type DriveFence, encodeLine } from 'harness-world';
import type { FxCallContext, FxResult, FxToolDef } from './fx-tools.ts';
import { WORKSPACE_ROOT } from './paths.ts';
import { type AttachedMachine, CallMachine, IDLE_CLOSE_MS, NotRun, stopsTheDrive } from './sandbox-runtime.ts';

/**
 * fx's tools for libfx's own durable agent (`createFxAgent`) on the
 * session's Vercel Sandbox (durable-harness's `fxTools(sandbox())`,
 * docs/workspaces.md §4.1, §5.6): the files on the session's Drive at
 * `/workspace`, the commands in its VM, as the VM's unprivileged user, never
 * on the disk, processes or environment of the server running the agent.
 * libfx's agent has no kernel drive: it keeps its own log in a World run per
 * session, and one worker at a time runs a session under a lease. So the VM
 * comes from the sandbox tier's `acquireMachine` with libfx's own fence
 * (P0.12), the epoch of the lease its worker holds:
 *
 * - **Whose call it is.** Every tool here is `idempotent: false`, so libfx
 *   stores each call's intent (a `tool_intent` record of its log's chain)
 *   before it runs the call, and only the worker that stored it does (a
 *   worker that was taken over is fenced at that write and never starts the
 *   call). A call reads the log and finds its own intent by `executionId`:
 *   the lease before it in the chain is the worker running the call, and
 *   its epoch the token. Epochs only rise over a session's life and stay the
 *   same through one worker's renewals, so the token rises from owner to
 *   owner and stays the same within one. A lease with a higher epoch later
 *   in the chain (another worker took the session) or this worker's release
 *   means its writes are no longer the session's: the call is not run, and
 *   the token says `fenced: false` to the attach it held.
 * - **One attach per worker** (a tenure), at its first call that reaches the
 *   workspace, again after a lost link; closed once no call ran on it for
 *   `IDLE_CLOSE_MS`, or when a later worker of the session calls here.
 * - **Before the call does anything.** An attach that fails or is busy
 *   answers the call "not run: the sandbox is starting, try again" (nothing
 *   reached the VM); one that no retry fixes (no fencing token, the VM-time
 *   budget, the link lost past its re-attaches) answers with its reason.
 * - **The record** (the sandbox tier's `MachineRecord`: the Drive's id, the
 *   VM time granted) is a stream of the session's run (`<sessionId>_fxsbx`),
 *   each line with the writer's epoch, a reader keeping no line older than
 *   one it has seen (as libfx's own readers do).
 * - **The files-lost note** goes before the first result of a tenure whose
 *   attach carries it, and the record acknowledges it once libfx stored that
 *   result (its tool result in the log, or the call behind a checkpoint), so
 *   a call cut off before it was stored tells the next tenure again.
 *
 * Durable, not transactional: the Drive keeps what a call wrote even if a
 * crash cut it off, and libfx never runs such a call again (it tells the
 * model it may have partly run). Each write is flushed to the Drive before
 * it resolves (the machine's own `FSYNC`).
 */

type World = DriveContext['world'];

/** The sandbox tier's `MachineRecord`, as the runtime handles it (opaque but for the note it may carry). */
export type FxMachineRecord = object;

/** Where the session factory reads and writes the session's record (harness-world-process' `MachineRecordStore`, structurally). */
export interface FxRecordStore<Record extends FxMachineRecord> {
  read(): Promise<Record | undefined>;
  write(record: Record): Promise<void>;
}

/** What fx's tools pass the sandbox tier's `acquireMachine` (harness-world-process' `AcquireMachineRequest`, structurally). */
export interface FxAcquireRequest<Record extends FxMachineRecord> {
  sessionId: string;
  fence: () => DriveFence | undefined;
  signal: AbortSignal;
  record: FxRecordStore<Record>;
  /**
   * The session's recorded config, for the factory's admission checks
   * (durable-harness's public-route guard). Always `undefined` here: an fx
   * agent's chat route is libfx's own, so no route of durable-harness's
   * records one, and the key is passed so the guard sees it was not forgotten.
   */
  config: undefined;
}

/** The sandbox tier's `AcquireMachine`, as fx's tools call it. */
export type FxAcquire<Record extends FxMachineRecord> = (request: FxAcquireRequest<Record>) => Promise<AttachedMachine>;

/** The name of a session's sandbox record stream (in its run). */
export const fxSandboxStreamName = (sessionId: string) => `${sessionId}_fxsbx`;

/** What a call answers when libfx's log holds no open intent for it (it was not called by libfx's agent, or by an agent on another World, or its result is stored). */
export const FX_NO_INTENT =
  "not run: libfx's log for this session holds no open intent for this call, so it can't tell which worker runs it (fxTools(sandbox()) is for libfx's createFxAgent, with { world } the World its sessions live in)";

/** What a call answers when its worker no longer holds the session: nothing ran, and libfx stores nothing more for that worker. */
export const FX_SUPERSEDED = 'not run: another server took this session over, so this one runs nothing more in its sandbox';

// ---------------------------------------------------------------------------
// libfx's session log, as far as the fence needs it (libfx durable.js)

/** A World run's events are a session's log; libfx's entries are `fx.log` steps. */
const LOG_STEP = 'fx.log';
/** A World event id holds its slot in the run (libfx reads it the same way). */
const SLOT = /^[a-z]+_(\d{26})$/;
const decoder = new TextDecoder();
const encoder = new TextEncoder();

/** One entry of libfx's log, kept small: its chain link, and what the fence and the note need of it. */
interface LogEntry {
  slot: number;
  k: string;
  /** The chained entry it continues (a cursor), for a lease, release or record. */
  a: string | null;
  holder?: string;
  epoch?: number;
  /** A record's tool intents (the calls about to run) and tool results (calls whose outcome libfx stored), by call id. */
  intents?: string[];
  results?: string[];
  /** A checkpoint's cover and the lease it carries. */
  through?: string | null;
  lease?: { holder: string; epoch: number };
}

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);
const isChained = (k: string) => k === 'lease' || k === 'release' || k === 'record';

/** The call ids a libfx journal record holds (`libfx-journal-v1`): intents, and results. */
function journalCalls(data: unknown): { intents: string[]; results: string[] } {
  const intents: string[] = [];
  const results: string[] = [];
  if (typeof data !== 'string') return { intents, results };
  let journal: unknown;
  try {
    journal = JSON.parse(decoder.decode(Uint8Array.from(atob(data), (c) => c.charCodeAt(0))));
  } catch {
    return { intents, results };
  }
  if (!isRecord(journal) || journal.format !== 'libfx-journal-v1' || !Array.isArray(journal.events)) return { intents, results };
  for (const event of journal.events) {
    if (!isRecord(event)) continue;
    if (event.type === 'tool_intent' && Array.isArray(event.data)) {
      for (const call of event.data) if (isRecord(call) && typeof call.id === 'string') intents.push(call.id);
      continue;
    }
    const execution = isRecord(event.data) ? event.data.execution : undefined;
    const steps = isRecord(execution) && Array.isArray(execution.tool_steps) ? execution.tool_steps : [];
    for (const step of steps) {
      for (const r of isRecord(step) && Array.isArray(step.tool_results) ? step.tool_results : []) {
        if (isRecord(r) && typeof r.tool_call_id === 'string') results.push(r.tool_call_id);
      }
    }
  }
  return { intents, results };
}

function compact(slot: number, entry: Record<string, unknown>): LogEntry | undefined {
  const k = entry.k;
  if (typeof k !== 'string') return undefined;
  const out: LogEntry = { slot, k, a: typeof entry.a === 'string' ? entry.a : null };
  if (typeof entry.holder === 'string') out.holder = entry.holder;
  if (Number.isSafeInteger(entry.epoch)) out.epoch = entry.epoch as number;
  if (k === 'record') Object.assign(out, journalCalls(entry.data));
  if (k === 'checkpoint') {
    out.through = typeof entry.through === 'string' ? entry.through : entry.through === null || entry.through === undefined ? null : String(entry.through);
    const lease = entry.lease;
    if (isRecord(lease) && typeof lease.holder === 'string' && Number.isSafeInteger(lease.epoch)) out.lease = { holder: lease.holder, epoch: lease.epoch as number };
  }
  return out;
}

/** Who runs a call, as the log says: the lease its intent was stored under. */
interface Owner {
  holder: string;
  epoch: number;
  /** The intent's slot (for the note's acknowledgement). */
  slot: number;
}

/** The log folded from its latest checkpoint, as libfx folds it (`foldSessionLog`): the chain only. */
interface LogState {
  intents: Map<string, Owner>;
  results: Set<string>;
  /** The latest lease of the chain, standing or not. */
  last: { holder: string; epoch: number } | null;
  released: Set<string>;
  /** What the latest checkpoint covers (a slot), or -1 with none. */
  checkpointed: number;
}

function fold(entries: readonly LogEntry[]): LogState {
  let checkpoint: LogEntry | undefined;
  for (const e of entries) if (e.k === 'checkpoint') checkpoint = e;
  let head = checkpoint ? (checkpoint.through ?? null) : null;
  let lease = checkpoint?.lease ?? null;
  const state: LogState = { intents: new Map(), results: new Set(), last: lease, released: new Set(), checkpointed: checkpoint ? Number(checkpoint.through ?? 0) : -1 };
  for (const e of entries) {
    if (!isChained(e.k) || e.a !== head) continue;
    // A chained entry stands only when it continues the chain: the first writer to continue a cursor wins.
    head = String(e.slot);
    if (e.k === 'lease' && e.holder !== undefined && e.epoch !== undefined) lease = state.last = { holder: e.holder, epoch: e.epoch };
    else if (e.k === 'release' && e.holder !== undefined) state.released.add(e.holder);
    else if (e.k === 'record') {
      for (const id of e.intents ?? []) if (lease) state.intents.set(id, { ...lease, slot: e.slot });
      for (const id of e.results ?? []) state.results.add(id);
    }
  }
  return state;
}

/** A session's log as this process has read it: the entries from its latest checkpoint, and the newest slot seen. */
class SessionLog {
  entries: LogEntry[] = [];
  slot = 0;
  state: LogState = fold([]);

  private reading: Promise<unknown> = Promise.resolve();

  /** Read what the run gained since (newest first, down to the slot seen or the latest checkpoint), and fold: one read at a time. */
  refresh(world: World, runId: string): Promise<LogState> {
    const next = this.reading.then(
      () => this.read(world, runId),
      () => this.read(world, runId)
    );
    this.reading = next.catch(() => {});
    return next;
  }

  private async read(world: World, runId: string): Promise<LogState> {
    const fresh: LogEntry[] = [];
    let through: number | undefined;
    let newest = this.slot;
    let cursor: string | undefined;
    pages: for (let limit = 10; ; limit = 100) {
      let page: Awaited<ReturnType<World['events']['list']>>;
      try {
        page = await world.events.list({ runId, pagination: { sortOrder: 'desc', limit, ...(cursor ? { cursor } : {}) }, resolveData: 'all' });
      } catch (err) {
        if ((err as { status?: unknown })?.status === 404 || /not found/i.test(String((err as Error)?.message))) break;
        throw err;
      }
      for (const event of page.data) {
        const match = SLOT.exec(String(event.eventId ?? ''));
        if (!match) throw new Error(`harness-world-hands: the World returned event id ${JSON.stringify(event.eventId ?? null)}, which holds no slot (libfx needs one)`);
        const slot = Number(match[1]);
        if (slot <= this.slot || (through !== undefined && slot <= through)) break pages;
        newest = Math.max(newest, slot);
        const data = event.eventType === 'step_created' ? (event as { eventData?: { stepName?: unknown; input?: unknown } }).eventData : undefined;
        if (data?.stepName !== LOG_STEP || !(data.input instanceof Uint8Array)) continue;
        let entry: unknown;
        try {
          entry = JSON.parse(decoder.decode(data.input));
        } catch {
          continue;
        }
        const e = isRecord(entry) ? compact(slot, entry) : undefined;
        if (!e) continue;
        fresh.push(e);
        if (through === undefined && e.k === 'checkpoint') through = e.through === null || e.through === undefined ? 0 : Number(e.through);
      }
      if (!page.hasMore || !page.cursor) break;
      cursor = page.cursor;
    }
    fresh.reverse();
    // From a newer checkpoint on, what it covers is not needed.
    const kept = through === undefined ? this.entries : this.entries.filter((e) => e.slot > through!);
    this.entries = [...kept, ...fresh];
    this.slot = newest;
    this.state = fold(this.entries);
    return this.state;
  }
}

// ---------------------------------------------------------------------------
// The record, in a stream of the session's run

interface RecordLine<Record> {
  v: 1;
  epoch: number;
  record?: Record;
  /** A call whose result told the model the record's note (`key`), with its intent's slot. */
  told?: { executionId: string; key: string; slot: number };
}

async function readLines<Record>(world: World, runId: string): Promise<RecordLine<Record>[]> {
  const name = fxSandboxStreamName(runId);
  let buffer = '';
  let cursor: string | undefined;
  let chunks = 0;
  for (;;) {
    let page: Awaited<ReturnType<World['streams']['getChunks']>>;
    try {
      page = await world.streams.getChunks(runId, name, cursor ? { cursor } : {});
    } catch (err) {
      if (chunks === 0 && !(await world.streams.list(runId).catch(() => [name])).includes(name)) return [];
      throw err;
    }
    for (const chunk of page.data) {
      buffer += decoder.decode(chunk.data, { stream: true });
      chunks++;
    }
    if (!page.hasMore || !page.cursor) break;
    cursor = page.cursor;
  }
  const lines: RecordLine<Record>[] = [];
  for (const line of buffer.split('\n')) {
    if (!line) continue;
    const value = decodeLine(line) as Partial<RecordLine<Record>> | null;
    if (value?.v === 1 && Number.isSafeInteger(value.epoch)) lines.push(value as RecordLine<Record>);
  }
  return lines;
}

/** The record the lines leave: the newest record line, skipping any line older than one before it; and the told lines since. */
function foldLines<Record>(lines: readonly RecordLine<Record>[]): { record: Record | undefined; told: NonNullable<RecordLine<Record>['told']>[] } {
  let high = -Infinity;
  let record: Record | undefined;
  const told: NonNullable<RecordLine<Record>['told']>[] = [];
  for (const line of lines) {
    if (line.epoch < high) continue;
    high = line.epoch;
    if (line.record !== undefined) record = line.record;
    if (line.told && typeof line.told.executionId === 'string' && typeof line.told.key === 'string') told.push(line.told);
  }
  return { record, told };
}

/** The record with the note acknowledged (the sandbox tier's `acknowledgeNotes`): its `filesLost`, when it is `key`, cleared. */
function acknowledged<Record extends FxMachineRecord>(record: Record, key: string): Record {
  const { filesLost, ...rest } = record as { filesLost?: unknown };
  return filesLost === key ? (rest as Record) : record;
}

// ---------------------------------------------------------------------------
// One worker's attach

/** Why an attach failed, as the call's outcome. */
function attachFailure(err: unknown): FxResult {
  const e = err as { name?: unknown; reason?: unknown; message?: unknown; hint?: unknown; code?: unknown } | undefined;
  if (typeof e?.code === 'string' && (e.code === 'ERR_MODULE_NOT_FOUND' || e.code === 'MODULE_NOT_FOUND')) return { text: String(e.message), isError: true };
  if (e?.name === 'Yield' && e.reason !== 'contended') return { text: `not run: another server holds this session's sandbox now (${String(e.reason)}), so this one runs nothing more there`, isError: true };
  if (stopsTheDrive(err)) {
    const message = typeof e?.message === 'string' ? e.message : String(err);
    return { text: typeof e?.hint === 'string' && e.hint ? `${message} ${e.hint}` : message, isError: true };
  }
  return { text: (err instanceof NotRun ? err : new NotRun(err)).message, isError: true };
}

/** One worker's tenure of a session in this process: its token, its attach, the note it showed. */
class Tenure<Record extends FxMachineRecord> {
  readonly controller = new AbortController();
  fenced = true;
  shown = false;
  private machine: AttachedMachine | undefined;
  private pending: Promise<AttachedMachine> | undefined;
  private active = 0;
  private idle: ReturnType<typeof setTimeout> | undefined;

  constructor(
    readonly sessionId: string,
    readonly owner: Owner,
    private readonly world: World,
    private readonly log: SessionLog,
    private readonly acquire: FxAcquire<Record>,
    private readonly onIdle: (t: Tenure<Record>) => void
  ) {}

  /** The token, as the session factory reads it at each acquire. */
  readonly fence = (): DriveFence => ({ count: this.owner.epoch, fenced: this.fenced });

  /**
   * The record store: the stream's newest record, its note acknowledged for
   * each call that told it whose result libfx has stored (the call's tool
   * result in the log, or the call behind the latest checkpoint), as the log
   * was read for the call that attaches.
   */
  private readonly store: FxRecordStore<Record> = {
    read: async () => {
      const { record, told } = foldLines<Record>(await readLines<Record>(this.world, this.sessionId));
      let r = record;
      const state = this.log.state;
      for (const t of told) if (r && (state.results.has(t.executionId) || t.slot <= state.checkpointed)) r = acknowledged(r, t.key);
      return r;
    },
    write: (record) => this.append({ v: 1, epoch: this.owner.epoch, record }),
  };

  private async append(line: RecordLine<Record>): Promise<void> {
    await this.world.streams.write(this.sessionId, fxSandboxStreamName(this.sessionId), encoder.encode(`${encodeLine(line)}\n`));
  }

  /** The attached machine, attaching now if there is none (or its link was lost). One attach at a time. */
  private attach(): Promise<AttachedMachine> {
    const m = this.machine;
    if (m && !m.signal.aborted && !m.workspace.lost) return Promise.resolve(m);
    if (this.pending) return this.pending;
    const pending = (async () => {
      this.machine = undefined;
      if (m) void m.close().catch(() => {});
      if (!this.fenced) throw new Error(FX_SUPERSEDED);
      const machine = await this.acquire({ sessionId: this.sessionId, fence: this.fence, signal: this.controller.signal, record: this.store, config: undefined });
      if (machine.workspace.root !== WORKSPACE_ROOT || !this.fenced) {
        void machine.close().catch(() => {});
        if (!this.fenced) throw new Error(FX_SUPERSEDED);
        throw Object.assign(new Error(`harness-world-hands: the session's sandbox has its files at ${machine.workspace.root}, not ${WORKSPACE_ROOT}, where fx's tools work`), { name: 'Unavailable' });
      }
      this.machine = machine;
      return machine;
    })();
    this.pending = pending;
    const clear = () => {
      if (this.pending === pending) this.pending = undefined;
    };
    pending.then(clear, clear);
    return pending;
  }

  /** Run a call (`slot`: where libfx stored its intent) on the attach's machine. */
  async run(def: FxToolDef, input: unknown, context: FxCallContext, slot: number): Promise<FxResult> {
    this.active++;
    clearTimeout(this.idle);
    try {
      let machine: AttachedMachine;
      try {
        machine = await this.attach();
      } catch (err) {
        if (!this.fenced) return { text: FX_SUPERSEDED, isError: true };
        return attachFailure(err);
      }
      const note = !this.shown && machine.notes.length ? machine.notes.join('\n') : undefined;
      if (note) this.shown = true;
      const stop = context.signal ? AbortSignal.any([context.signal, this.controller.signal]) : this.controller.signal;
      let result: FxResult;
      try {
        result = await def.run(input, new CallMachine(machine.workspace, stop));
      } catch (err) {
        // On a Drive a call that failed part way may have changed files: it says what failed, and stays failed.
        result = { text: String((err as Error)?.message ?? err), isError: true };
      }
      if (!note) return result;
      // Said with this call's result: the record acknowledges it once libfx has stored that result.
      if (machine.notesKey !== undefined) await this.append({ v: 1, epoch: this.owner.epoch, told: { executionId: context.executionId, key: machine.notesKey, slot } }).catch(() => {});
      return { ...result, text: `${note}\n\n${result.text}` };
    } finally {
      if (--this.active === 0 && this.fenced) {
        this.idle = setTimeout(() => this.onIdle(this), IDLE_CLOSE_MS);
        (this.idle as { unref?: () => void }).unref?.();
      }
    }
  }

  /** The tenure is over (another worker took the session, or it sat idle): its token stops being fenced, its link closes. */
  end(): void {
    this.fenced = false;
    clearTimeout(this.idle);
    const m = this.machine;
    this.machine = undefined;
    if (m) void m.close().catch(() => {});
    this.controller.abort(new Error('harness-world-hands: this worker no longer runs the session here'));
  }
}

// ---------------------------------------------------------------------------
// The store fx's tools call

/** fx's tools on the session's sandbox, for libfx's agent (see the module comment). */
export class FxSandbox<Record extends FxMachineRecord> {
  private static readonly MAX_LOGS = 256;
  private readonly logs = new Map<string, SessionLog>();
  private readonly tenures = new Map<string, Tenure<Record>>();
  private world?: Promise<World>;

  constructor(
    private readonly worldOf: () => World | Promise<World>,
    private readonly acquire: FxAcquire<Record>,
    private readonly checkSession: (world: World, sessionId: string) => Promise<void>
  ) {}

  async call(def: FxToolDef, input: unknown, context: FxCallContext): Promise<FxResult> {
    const world = await (this.world ??= Promise.resolve()
      .then(this.worldOf)
      .catch((err) => {
        this.world = undefined;
        throw err;
      }));
    const sessionId = context.sessionId;
    let log = this.logs.get(sessionId);
    if (!log) {
      await this.checkSession(world, sessionId);
      this.logs.set(sessionId, (log = new SessionLog()));
      // A bounded number of sessions keep their log read here; the others read it again.
      for (const id of this.logs.keys()) {
        if (this.logs.size <= FxSandbox.MAX_LOGS) break;
        if (!this.tenures.has(id)) this.logs.delete(id);
      }
    }
    const state = await log.refresh(world, sessionId);
    // An intent is open until libfx stores its result: a checkpoint need not have covered it yet.
    const owner = state.results.has(context.executionId) ? undefined : state.intents.get(context.executionId);
    if (!owner) return { text: FX_NO_INTENT, isError: true };
    let tenure = this.tenures.get(sessionId);
    if (tenure && tenure.owner.holder !== owner.holder) {
      // A worker this process ran before the one now in the log (its token is lower): it holds nothing.
      if (tenure.owner.epoch > owner.epoch) return { text: FX_SUPERSEDED, isError: true };
      tenure.end();
      this.tenures.delete(sessionId);
      tenure = undefined;
    }
    // A later worker took the session, or this one let it go: its writes are no longer the session's.
    const later = state.last && state.last.holder !== owner.holder && state.last.epoch > owner.epoch;
    if (later || state.released.has(owner.holder)) {
      if (tenure) {
        tenure.end();
        this.tenures.delete(sessionId);
      }
      return { text: FX_SUPERSEDED, isError: true };
    }
    if (!tenure) {
      tenure = new Tenure<Record>(sessionId, owner, world, log, this.acquire, (t) => {
        if (this.tenures.get(sessionId) === t) this.tenures.delete(sessionId);
        t.end();
        if (!this.tenures.has(sessionId)) this.logs.delete(sessionId);
      });
      this.tenures.set(sessionId, tenure);
    }
    return tenure.run(def, input, context, owner.slot);
  }
}

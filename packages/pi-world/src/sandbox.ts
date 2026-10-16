import type { ToolDiagnostic } from '@earendil-works/pi-durable';
import type { CallMode, WorkspaceMachine } from 'harness-world-hands/runtime';
import { WORKSPACE_ROOT } from 'harness-world-hands/tools';
import type { AcquireMachine, MachineRecord, MachineRecordStore, SessionMachine } from 'harness-world-process';
import { SANDBOX_DETAILS_KEY } from './sandbox-record.ts';
import type { WorldStorage } from './storage.ts';
import { Binding, cwdOf, type EnvBuilder, envOn, type Ran, storageOf } from './workspace.ts';

/**
 * A pi-durable session's tools in its Vercel Sandbox (durable-harness's
 * `sandboxEnv()`, docs/workspaces.md §4.1, §5.6): Pi's own tools on the VM's
 * `/workspace` (the session's Drive), through the relay's helper as the VM's
 * unprivileged user, never on the Function's disk, processes or
 * environment. Files on the Drive are durable but not transactional: they
 * outlive every VM, and a command a crash cut off may have half-run.
 *
 * The VM comes from the sandbox tier's `acquireMachine`, with this runtime's
 * own fence (P0.12): pi-durable has no kernel drive, so its `WorldStorage` is
 * the owner. One attach per open storage (an invocation of the session):
 *
 * - **Lazily**, at the first call that reaches the workspace, so a session
 *   that never runs a tool never boots a VM; again after the link was lost.
 * - **As the owner.** The storage makes sure it has an accepted in-band write
 *   (`claim`), then hands the session factory its token (`writerFence`: the
 *   count its first accepted write was checked against, which rises from
 *   owner to owner and stays the same within one; `fenced` turns false once
 *   its writes are no longer behind the World's fence) and its `signal`
 *   (aborted when it closes, is abandoned or fenced: the links close).
 * - **With the session's recorded config** (`WorldStorage.config`, its
 *   `run_created`'s), for the factory's admission checks: durable-harness's
 *   `sandbox()` refuses a session a public chat route started without its
 *   opt-in, before any VM boots.
 * - **With the record in the session's log.** The factory reads and writes
 *   the session's `MachineRecord` (the Drive's id, the VM time granted) as
 *   in-band sandbox entries of the storage's journal (`writeSandbox`), so the
 *   VM-time budget holds across invocations and a lost Drive is noticed.
 * - **Before the call does anything.** An attach that fails or is contended
 *   answers the call "Not run: the sandbox is starting, try again" (a
 *   definite outcome: nothing reached the VM), so the model may simply
 *   retry; an error that no retry in this invocation can fix (the session's
 *   VM-time budget, a World without the fence, the storage fenced, the link
 *   lost past its re-attaches) fails the call with its own message.
 *
 * With durable-harness's `CodingTools` (`bindTool`), a call also tells the
 * model what the attach says (today, that the session's files were lost: a
 * new, empty Drive), once per invocation, and its result names the Drive it
 * told about (`hw.sandbox` details), so the record acknowledges the note
 * when that result commits (sandbox-record.ts). pi-durable's own tools and
 * an app's tool on `api.env` run on the same workspace, with no notes.
 *
 * Calls run side by side, as on a disk; a write is flushed to the Drive
 * (`FSYNC`) before it resolves, so a result never claims a write the Drive
 * could still lose.
 */

/** What a call answers when its sandbox could not be attached for it: nothing reached the VM. */
export const NOT_RUN = 'Not run: the sandbox is starting, try again';

/** What a call says on a storage that is not a World storage. */
export const SANDBOX_NO_WORLD =
  "durable-harness: sandboxEnv() runs in the session's Vercel Sandbox, and this Harness's storage is not a World storage: open it on openWorldStorage() from durable-harness/pi-durable";

/** What a call says once its storage writes nothing more (closed, or another process took the session). */
export const SANDBOX_ENDED = "durable-harness: this invocation of the session holds nothing more (it closed, or another one took the session over): it runs nothing in the session's sandbox";

/**
 * A call that did not run: its sandbox could not be attached. Nothing reached
 * the VM. Busy, booting or contended (no cause, a `Yield`, or `retry`):
 * `NOT_RUN`, so the model may retry. Anything else says what failed (a
 * missing project link, a refused credential), as harness-hands' `NotRun`
 * does, so the model isn't told to retry what no retry fixes.
 */
export class NotRunError extends Error {
  readonly code = 'ENOTRUN';
  constructor(cause?: unknown, retry = cause === undefined || (cause as { name?: unknown }).name === 'Yield') {
    const reason = retry ? '' : String((cause as Error)?.message ?? cause).replace(/\s+/g, ' ').slice(0, 300);
    super(retry ? NOT_RUN : `Not run: the sandbox could not be attached (${reason})`, cause !== undefined ? { cause } : undefined);
    this.name = 'NotRunError';
  }
}

/**
 * The attach errors a call reports as they are, not as "not run": no retry
 * in this invocation can fix them (the session's VM-time budget, a World
 * without the in-band fence, an image without the relay, the storage lost
 * the session, the link lost more often than an invocation re-attaches, a
 * module the sandbox tier needs that this install lacks).
 */
const FINAL = new Set(['Unavailable', 'Fatal', 'Restart', 'StorageFenced', 'InBandSupersededError', 'LogFenced', 'LogSuperseded', 'SandboxLost']);
const MISSING_MODULE = new Set(['ERR_MODULE_NOT_FOUND', 'MODULE_NOT_FOUND']);

function finalError(err: unknown): Error | undefined {
  const e = err as { name?: unknown; reason?: unknown; message?: unknown; hint?: unknown; code?: unknown } | undefined;
  if (typeof e?.code === 'string' && MISSING_MODULE.has(e.code)) return err as Error;
  if (e?.name === 'Yield') {
    if (e.reason === 'contended') return undefined;
    return new Error(`durable-harness: another invocation of the session holds its sandbox now (${String(e.reason)}): this one runs nothing more there`, { cause: err });
  }
  if (typeof e?.name !== 'string' || !FINAL.has(e.name)) return undefined;
  const message = typeof e.message === 'string' ? e.message : String(err);
  return typeof e.hint === 'string' && e.hint ? new Error(`${message} ${e.hint}`, { cause: err }) : (err as Error);
}

/** The session's `MachineRecord`, in its journal: the record as the log has it, written in-band. */
function recordStore(storage: WorldStorage): MachineRecordStore {
  return {
    read: () => (storage.sandbox.record ? ({ ...storage.sandbox.record } as MachineRecord) : undefined),
    write: (record) => storage.writeSandbox(record as Record<string, unknown>),
  };
}

/** One open storage's attach to the session's sandbox (see the module comment). */
export class SandboxSession {
  #machine: SessionMachine | undefined;
  #pending: Promise<SessionMachine> | undefined;
  /** The notes this invocation showed the model (or is showing it, in a call still running). */
  readonly #shown = new Set<string>();
  /** How many times this storage acquired a machine (attach and re-attaches). */
  attaches = 0;

  constructor(
    readonly storage: WorldStorage,
    private readonly acquire: AcquireMachine
  ) {
    storage.signal.addEventListener('abort', () => this.#close(), { once: true });
  }

  /** The attached machine, attaching now if there is none or its link was lost. Throws `NotRunError` when it can't, or an error no retry fixes. */
  attach(): Promise<SessionMachine> {
    const m = this.#machine;
    if (m && !m.signal.aborted && !m.workspace.lost) return Promise.resolve(m);
    if (this.#pending) return this.#pending;
    const pending = this.#attachNow();
    this.#pending = pending;
    const clear = () => {
      if (this.#pending === pending) this.#pending = undefined;
    };
    pending.then(clear, clear);
    return pending;
  }

  async #attachNow(): Promise<SessionMachine> {
    // A lost machine is closed (best effort) before it is replaced: nothing reaches it from here again.
    const old = this.#machine;
    this.#machine = undefined;
    if (old) void old.close().catch(() => {});
    const { storage } = this;
    if (storage.signal.aborted) throw new Error(SANDBOX_ENDED);
    let machine: SessionMachine;
    try {
      // The owner's token: read by the factory once this storage holds the session.
      await storage.claim();
      this.attaches++;
      // With the session's recorded config, for the factory's admission checks (a public route's session without its opt-in boots no VM).
      machine = await this.acquire({ sessionId: storage.sessionId, config: storage.config, fence: () => storage.writerFence, signal: storage.signal, record: recordStore(storage) });
    } catch (err) {
      if (storage.signal.aborted) throw new Error(SANDBOX_ENDED, { cause: err });
      throw finalError(err) ?? new NotRunError(err);
    }
    if (storage.signal.aborted || machine.signal.aborted || machine.workspace.lost) {
      void machine.close().catch(() => {});
      if (storage.signal.aborted) throw new Error(SANDBOX_ENDED);
      throw new NotRunError(new Error('the sandbox link was lost as it was attached'), true);
    }
    if (machine.workspace.root !== WORKSPACE_ROOT) {
      void machine.close().catch(() => {});
      throw new Error(`durable-harness: the session's sandbox has its workspace at ${machine.workspace.root}, not ${WORKSPACE_ROOT}: boot it from the base (prepareSandboxBase) or an image built by the same recipe`);
    }
    this.#machine = machine;
    return machine;
  }

  /**
   * What the attach says that this invocation has not shown the model yet,
   * reserved for one call (`release` it if that call never returns a result):
   * its notes, and the Drive whose loss its result tells about.
   */
  reserve(machine: SessionMachine): { notes: ToolDiagnostic[]; details?: Record<string, unknown>; release: () => void } {
    const fresh = machine.notes.filter((n) => !this.#shown.has(n));
    for (const n of fresh) this.#shown.add(n);
    const release = () => {
      for (const n of fresh) this.#shown.delete(n);
    };
    if (!fresh.length) return { notes: [], release };
    const lost = this.storage.sandbox.record?.filesLost;
    return {
      notes: fresh.map((message) => ({ severity: 'warn' as const, code: 'sandbox', message })),
      ...(typeof lost === 'string' ? { details: { [SANDBOX_DETAILS_KEY]: { told: lost } } } : {}),
      release,
    };
  }

  #close(): void {
    const m = this.#machine;
    this.#machine = undefined;
    if (m) void m.close().catch(() => {});
  }
}

const sessions = new WeakMap<WorldStorage, WeakMap<AcquireMachine, SandboxSession>>();

/** The sandbox attach of one open storage (one per storage and `acquire`). */
export function sandboxSessionOf(storage: WorldStorage, acquire: AcquireMachine): SandboxSession {
  let byAcquire = sessions.get(storage);
  if (!byAcquire) sessions.set(storage, (byAcquire = new WeakMap()));
  let session = byAcquire.get(acquire);
  if (!session) byAcquire.set(acquire, (session = new SandboxSession(storage, acquire)));
  return session;
}

class SandboxBinding extends Binding {
  constructor(private readonly session: SandboxSession) {
    super();
  }

  async run<T>(inner: () => Promise<T>, signal: AbortSignal | undefined, mode: CallMode): Promise<Ran<T>> {
    // The pre-intent phase: a failed attach is the call's outcome, before anything reaches the VM.
    let machine: SessionMachine;
    try {
      machine = await this.session.attach();
    } catch (error) {
      return { outcome: { ok: false, error } };
    }
    const told = this.session.reserve(machine);
    let outcome: Ran<T>['outcome'];
    try {
      outcome = await this.on(machine.workspace, mode, inner, signal);
    } catch (error) {
      told.release();
      throw error;
    }
    return { outcome, notes: told.notes, ...(told.details ? { details: told.details } : {}) };
  }

  protected async bare<T>(_mode: CallMode, work: (m: WorkspaceMachine) => Promise<T>): Promise<T> {
    return work((await this.session.attach()).workspace);
  }
}

/**
 * `HarnessOptions.env` for the session's Vercel Sandbox, over the sandbox
 * tier's `acquire` (durable-harness's `sandboxEnv()` passes the one its
 * `sandbox()` descriptor resolves to). Needs a World storage
 * (`openWorldStorage`).
 */
export function sandboxEnvBuilder(acquire: AcquireMachine): EnvBuilder {
  return async (target, context) => {
    const storage = await storageOf(target.read, context);
    if (!storage) throw new Error(SANDBOX_NO_WORLD);
    // Not transactional: a command the machine stopped may have half-run, and its error says so (not that nothing changed).
    return envOn(`sandbox:${storage.sessionId}`, cwdOf(target.cwd), new SandboxBinding(sandboxSessionOf(storage, acquire)), { transactional: false });
  };
}

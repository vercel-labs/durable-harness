/**
 * `RemoteMachine` (docs/workspaces.md §4.1 read with the §4 note, §5.4, §5.6):
 * the session's `/workspace` in its VM as a harness-world-hands
 * `WorkspaceMachine`, the same interface as `virtual()`'s, for the tools of a
 * Function-tier runtime (OpenCode, Pi, pi-durable, fx with `sandbox()`). Every
 * operation is one `ENV_CALL` over the relay link, served by the relay's
 * per-attach helper as the unprivileged user (`relay/helper.ts`): files under
 * `/workspace`, commands as that user in the helper's cgroup leaf with the
 * relay's allowlist as their environment, never this process's. Paths are not
 * confined to the root (its `exec` reaches whatever that user can, so a path
 * check would guard nothing); the CLIs' own state dirs are hidden from the
 * helper by the relay instead (a mount namespace, privileged tier).
 *
 * - **The fence.** While the attach holds no fenced token, every operation that
 *   can change the VM (`exec`, `writeFile`, `mkdir`, `remove`, `rename`,
 *   `symlink`, `chmod`) is refused here, before anything is sent (§5.4); reads
 *   are not.
 * - **The env.** A command's `env` passes `checkSpawnEnv` first (§3.1's rules
 *   and the `process.env` equality rule), as every spawn's does: a `TypeError`
 *   naming the key. `SessionMachine.provision`'s workspace passes only the
 *   structural rules (`checkProvisionEnv`), before anything else runs.
 * - **Durable, not transactional.** A Drive keeps what a call wrote, even a
 *   call a crash cut off: so there is no `checkpoint`, `diff` or `rollback`
 *   here (they are the in-memory machine's, for the document-backed
 *   workspace; on a sandbox a mutating call is `replay: 'never'`, §7). What
 *   a write needs is to be on the Drive before its outcome is recorded: with
 *   `flush: 'each'` (the default) every mutating operation sends `FSYNC` and
 *   waits before it resolves; with `'manual'` the runtime calls `fsync()` once
 *   before the call's outcome.
 * - **Deadlines.** A command stops at its own `timeoutMs` (exit 124, as
 *   `timeout` does), at the machine's `timeoutMs` (`ETIMEDOUT`), at the time
 *   left in the invocation (`timeLeft`, the §5.6 clamp: `ETIMEDOUT`, "partial
 *   effects may exist"), or when its `signal` aborts. A file operation waits
 *   at most `callTimeoutMs`, and never past the time left either; with none
 *   left, no operation is sent ("did not run"). A stopped or given-up mutating
 *   operation settles only once the relay confirmed it stopped (the command's
 *   group is dead), so its outcome is definite, never an open intent.
 * - **Loss.** An operation on a lost link is refused before anything is sent
 *   (`EGONE`: it did not run); one cut off by the loss fails with `EINTR`, and
 *   a mutating one says it may have half-run. So does a mutating one that got
 *   no answer in time (`ETIMEDOUT`): with `flush: 'each'` every mutating
 *   operation that failed after it was sent is flushed too, before it
 *   rejects, since part of it may be on the Drive.
 * - **Untrusted bytes.** Every result is checked against its operation's shape
 *   (types, bounds, sizes) before it is returned; a result that doesn't fit is
 *   the VM's fault (`EPROTO`).
 *
 * Frozen with the branded `SessionMachine` that carries it (`workspace`), so its
 * state is in private fields only.
 *
 * `LazyRemoteMachine` is the §5.6 lazy attach: a `WorkspaceMachine` that
 * acquires the session's VM on first use, with `attach()` as the pre-intent
 * phase a runtime runs before it writes a call's intent: a failed or busy
 * attach is `NotRunError` ("not run"), a definite outcome, never an
 * interrupted call.
 */
import type { FenceToken, SessionMachine } from 'harness-world-process';
import type { DirEntry, ExecOptions, ExecResult, PathStat, PathType, WalkEntry, WorkspaceMachine } from 'harness-world-hands/runtime';
import { checkSpawnEnv } from './env-check.ts';
import { EnvCallError, type EnvReply } from './relay/dial.ts';
import { LIMITS, MAX_TIMER_MS, MUTATING_ENV_OPS, isCount, isObject, type EnvOp } from './relay/protocol.ts';

/** What `RemoteMachine` asks of the relay link (`RelayLink`): an `ENV_CALL`, `FSYNC`, and its loss. */
export interface EnvLink {
  envCall(op: EnvOp, args: unknown[], options?: { input?: Uint8Array; signal?: AbortSignal; timeoutMs?: number; maxBytes?: number }): Promise<EnvReply>;
  fsync(): Promise<void>;
  isLost(): boolean;
  /** Drop the link as a protocol violation (a result that doesn't fit its operation). */
  violation?(reason: string): void;
}

export interface RemoteMachineOptions {
  link: EnvLink;
  /** `/workspace` (the stand-in's is a temp dir). */
  root: string;
  /** The attach's token (`SessionMachine.fence`): without a fenced one, nothing that can change the VM is sent. */
  fence: FenceToken | undefined;
  /** A command's longest run (`sandbox({ commandTimeoutMs })`). Default 10 min; at most 2^31 - 1 ms. */
  timeoutMs?: number;
  /** The ms left in the invocation, minus its margin (§5.6): a command is stopped there. Undefined: no clamp. */
  timeLeft?: () => number | undefined;
  /**
   * Whether the attach may still change the VM, asked at every mutating
   * operation on top of `fence` (the session factory's: the invocation's token
   * as later acquires narrowed it). Default: `fence` alone.
   */
  mutable?: () => boolean;
  /** `each` (default): every mutating operation waits for `FSYNC` before it resolves. `manual`: the runtime calls `fsync()`. */
  flush?: 'each' | 'manual';
  /** How long a file operation waits for its answer. Default 60 s; at most 2^31 - 1 ms. */
  callTimeoutMs?: number;
  /**
   * Runs before a command is sent, with its `env`: throws to refuse it.
   * Default `checkSpawnEnv` (every rule). The session machine's own: the same
   * rules for `workspace`, the structural ones for `provision`'s workspace.
   */
  checkEnv?: (env: Readonly<Record<string, string>> | undefined) => void;
}

/** Whether a token lets an attach mutate the VM: present and still behind the World's fence (§5.4). */
export function fencedToken(fence: FenceToken | undefined): fence is FenceToken {
  return fence !== undefined && fence.fenced === true;
}

/** The default longest command (`SANDBOX_DEFAULTS.commandTimeoutMs`). */
export const REMOTE_COMMAND_TIMEOUT_MS = 10 * 60_000;
/** How long past a command's own deadline the driver waits for the helper's answer before it gives up on it. */
const ANSWER_GRACE_MS = 5_000;
const PATH_TYPES: readonly PathType[] = ['file', 'directory', 'symlink'];
const MAX_ENTRIES = 100_000;

const error = (code: string, message: string) => Object.assign(new Error(message), { code });

/** A timeout option: a positive number, clamped to what a timer can hold. */
function timerMs(v: number | undefined, fallback: number, what: string): number {
  if (v === undefined) return fallback;
  if (typeof v !== 'number' || !(v > 0)) throw new RangeError(`harness-sandbox: ${what} must be a positive number of ms`);
  return Math.min(MAX_TIMER_MS, v);
}

function stoppedError(reason: unknown): Error {
  const r = reason as { code?: unknown; message?: unknown; reason?: unknown } | undefined;
  if (r && typeof r.code === 'string' && typeof r.message === 'string') return error(r.code, r.message);
  if (r?.reason === 'budget') return error('EINTR', 'the command was stopped: this invocation of the session ran out of time');
  return error('EINTR', 'the command was stopped: its call was cancelled');
}

/** Normalize a path as the in-memory machine does: relative to the root, `.` and `..` folded. Refuses NUL and over-long paths. */
export function workspacePath(root: string, path: string): string {
  if (typeof path !== 'string' || path.includes('\0')) throw error('EINVAL', 'a path must be a string without NUL');
  const p = path.startsWith('/') ? path : `${root}/${path}`;
  const out: string[] = [];
  for (const seg of p.split('/')) {
    if (seg === '' || seg === '.') continue;
    if (seg === '..') out.pop();
    else out.push(seg);
  }
  const abs = `/${out.join('/')}`;
  if (abs.length > 4096) throw error('ENAMETOOLONG', 'a path over 4096 bytes');
  return abs;
}

const isMode = (v: unknown): v is number => isCount(v) && v <= 0o7777;
const isPathText = (v: unknown, max = 4096): v is string => typeof v === 'string' && v.length > 0 && v.length <= max && !v.includes('\0');

/** A result that doesn't fit its operation: the VM's fault (the machine drops the link as a violation). */
class BadResult extends Error {
  readonly code = 'EPROTO';
  constructor(op: string, what: string) {
    super(`harness-sandbox: the sandbox answered ${op} with ${what}`);
  }
}
const badResult = (op: string, what: string) => new BadResult(op, what);

function checkStat(op: string, v: unknown): PathStat {
  if (!isObject(v) || !PATH_TYPES.includes(v.type as PathType) || !isCount(v.size) || !isMode(v.mode) || typeof v.mtimeMs !== 'number' || !Number.isFinite(v.mtimeMs) || v.mtimeMs < 0) throw badResult(op, 'a malformed stat');
  return { type: v.type as PathType, size: v.size, mode: v.mode, mtimeMs: v.mtimeMs };
}

const byName = (a: { name: string }, b: { name: string }) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
const byPath = (a: { path: string }, b: { path: string }) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0);

/** The session's `/workspace` in its VM, over the relay's helper (see the module comment). */
export class RemoteMachine implements WorkspaceMachine {
  readonly root: string;
  /** Commands run in the session's own VM. */
  readonly isolation = 'sandbox';
  readonly #link: EnvLink;
  readonly #mutable: boolean;
  readonly #still: (() => boolean) | undefined;
  readonly #timeoutMs: number;
  readonly #timeLeft: (() => number | undefined) | undefined;
  readonly #flushEach: boolean;
  readonly #callTimeoutMs: number;
  readonly #checkEnv: (env: Readonly<Record<string, string>> | undefined) => void;
  #disposed = false;

  constructor(options: RemoteMachineOptions) {
    this.root = options.root;
    this.#link = options.link;
    this.#mutable = fencedToken(options.fence);
    this.#still = options.mutable;
    this.#timeoutMs = timerMs(options.timeoutMs, REMOTE_COMMAND_TIMEOUT_MS, 'timeoutMs');
    this.#timeLeft = options.timeLeft;
    this.#flushEach = (options.flush ?? 'each') === 'each';
    this.#callTimeoutMs = timerMs(options.callTimeoutMs, 60_000, 'callTimeoutMs');
    this.#checkEnv = options.checkEnv ?? ((env) => checkSpawnEnv(env));
  }

  /** The link is gone (or the machine was disposed): get a new machine from the session (re-attach). */
  get lost(): boolean {
    return this.#disposed || this.#link.isLost();
  }

  /** Flush the Drive's writes (`FSYNC`, `sync -f /workspace`): before a mutating call's outcome is recorded. */
  async fsync(): Promise<void> {
    if (this.lost) throw error('EGONE', 'the sandbox link was lost before the Drive was flushed: re-attach');
    try {
      await this.#link.fsync();
    } catch (err) {
      throw Object.assign(error('EIO', `the Drive could not be flushed: ${String((err as Error)?.message ?? err)}`), { cause: err });
    }
  }

  /**
   * One operation: refused unfenced, lost or out of time (nothing sent), sent
   * with a deadline no later than the invocation's, its errors given their
   * codes, and (mutating) flushed, after a failure too.
   */
  async #call(op: EnvOp, args: unknown[], options: { input?: Uint8Array; signal?: AbortSignal; timeoutMs?: number; maxBytes?: number } = {}): Promise<EnvReply> {
    const mutating = MUTATING_ENV_OPS.has(op);
    if (this.lost) throw error('EGONE', `${op} did not run: the sandbox link was lost (re-attach)`);
    if (mutating && !(this.#mutable && (this.#still ? this.#still() : true))) throw error('EPERM', `harness-sandbox: ${op} refused: this attach holds no fenced token (docs/workspaces.md §5.4: nothing that can change the VM without one)`);
    let timeoutMs = options.timeoutMs ?? this.#callTimeoutMs;
    if (op !== 'exec') {
      // `exec` clamps itself (its deadline is the command's); every other op waits no longer than the invocation has left.
      const left = this.#timeLeft?.();
      if (left !== undefined && left <= 0) throw error('ETIMEDOUT', `${op} did not run: this invocation of the session has no time left for it`);
      if (left !== undefined) timeoutMs = Math.min(timeoutMs, left);
    }
    let reply: EnvReply;
    try {
      reply = await this.#link.envCall(op, args, { ...options, timeoutMs });
    } catch (err) {
      // Sent, then failed: part of a mutating op may be on the Drive, so it is flushed before its outcome.
      if (mutating && this.#flushEach && !(err instanceof RangeError) && !this.lost) await this.fsync().catch(() => {});
      if (err instanceof EnvCallError) throw error(err.code, err.message);
      if (this.#link.isLost()) {
        throw Object.assign(error('EINTR', mutating ? `${op} was cut off: the sandbox link was lost while it ran, so it may have half-run` : `${op} was cut off: the sandbox link was lost (re-attach)`), { cause: err });
      }
      if ((err as { code?: unknown })?.code === 'ETIMEDOUT' && !options.signal?.aborted) {
        throw Object.assign(error('ETIMEDOUT', mutating ? `${op} got no answer within ${Math.floor(timeoutMs)} ms, so it was stopped: it may have run (partial effects may exist)` : `${op} got no answer within ${Math.floor(timeoutMs)} ms`), { cause: err });
      }
      throw err;
    }
    if (mutating && this.#flushEach) await this.fsync();
    return reply;
  }

  /** A result that doesn't fit its operation drops the link as a violation (the VM broke the protocol), then fails the call. */
  async #checked<T>(body: () => Promise<T>): Promise<T> {
    try {
      return await body();
    } catch (err) {
      if (err instanceof BadResult) this.#link.violation?.(err.message);
      throw err;
    }
  }

  exec(command: string, options?: ExecOptions): Promise<ExecResult> {
    return this.#checked(() => this.#exec(command, options));
  }

  readFile(path: string): Promise<Uint8Array> {
    return this.#checked(() => this.#readFile(path));
  }

  stat(path: string): Promise<PathStat | undefined> {
    return this.#checked(() => this.#stat(path));
  }

  lstat(path: string): Promise<PathStat | undefined> {
    return this.#checked(() => this.#lstat(path));
  }

  list(dir: string): Promise<DirEntry[]> {
    return this.#checked(() => this.#list(dir));
  }

  walk(dir?: string): Promise<WalkEntry[]> {
    return this.#checked(() => this.#walk(dir));
  }

  async #exec(command: string, options: ExecOptions = {}): Promise<ExecResult> {
    if (typeof command !== 'string' || command.includes('\0')) throw new TypeError('harness-sandbox: a command must be a string without NUL');
    if (command.length > LIMITS.frameBytes / 2) throw error('E2BIG', 'harness-sandbox: a command over 512 KiB');
    this.#checkEnv(options.env);
    const args = options.args ?? [];
    if (!Array.isArray(args) || !args.every((a) => typeof a === 'string' && !a.includes('\0'))) throw new TypeError('harness-sandbox: exec args must be strings without NUL');
    const stdin = options.stdin === undefined ? undefined : new TextEncoder().encode(options.stdin);
    const left = this.#timeLeft?.();
    const clamped = left !== undefined && left < this.#timeoutMs;
    const limitMs = Math.max(1, Math.floor(clamped ? left : this.#timeoutMs));
    if (clamped && left! <= 0) throw error('ETIMEDOUT', 'the command did not run: this invocation of the session has no time left for it');
    // Its own deadline exits 124; one at or past the machine's limit is that limit (`ETIMEDOUT`), as on the in-memory machine.
    const timeoutMs = options.timeoutMs !== undefined && options.timeoutMs > 0 && options.timeoutMs < limitMs ? Math.max(1, Math.floor(options.timeoutMs)) : undefined;
    const signal = options.signal;
    if (signal?.aborted) throw stoppedError(signal.reason);
    let reply: EnvReply;
    try {
      reply = await this.#call(
        'exec',
        [command, { cwd: workspacePath(this.root, options.cwd ?? this.root), ...(options.env ? { env: options.env } : {}), ...(args.length ? { args } : {}), ...(timeoutMs !== undefined ? { timeoutMs } : {}), limitMs }],
        { ...(stdin ? { input: stdin } : {}), ...(signal ? { signal } : {}), timeoutMs: Math.min(MAX_TIMER_MS, limitMs + ANSWER_GRACE_MS) }
      );
    } catch (err) {
      // Flushed already (`#call`), once the relay confirmed the command stopped (or the link dropped, and the session with it).
      if (signal?.aborted) throw stoppedError(signal.reason);
      throw err;
    }
    const v = reply.value;
    const data = reply.data ?? new Uint8Array();
    if (!isObject(v) || typeof v.exitCode !== 'number' || !Number.isInteger(v.exitCode) || v.exitCode < 0 || v.exitCode > 255 || !isCount(v.stdoutBytes) || !isCount(v.stderrBytes) || v.stdoutBytes + v.stderrBytes !== data.length || v.stdoutBytes > LIMITS.execOutputBytes || v.stderrBytes > LIMITS.execOutputBytes) {
      throw badResult('exec', 'a malformed result');
    }
    if (v.stopped === 'limit') {
      throw error(
        'ETIMEDOUT',
        clamped ? 'the command ran out of time in this invocation, so it was stopped: partial effects may exist' : `the command ran past its time limit (${this.#timeoutMs / 1000} s), so it was stopped: partial effects may exist`
      );
    }
    const decoder = new TextDecoder();
    return { exitCode: v.exitCode, stdout: decoder.decode(data.subarray(0, v.stdoutBytes)), stderr: decoder.decode(data.subarray(v.stdoutBytes)) };
  }

  async #readFile(path: string): Promise<Uint8Array> {
    const reply = await this.#call('readFile', [workspacePath(this.root, path)]);
    if (!reply.data) throw badResult('readFile', 'no bytes');
    return reply.data;
  }

  async writeFile(path: string, content: string | Uint8Array, options: { mode?: number; append?: boolean } = {}): Promise<void> {
    const bytes = typeof content === 'string' ? new TextEncoder().encode(content) : content;
    if (!(bytes instanceof Uint8Array)) throw new TypeError('harness-sandbox: file content must be a string or bytes');
    if (bytes.length > LIMITS.envBytes) throw error('EFBIG', `EFBIG: ${path} would be over the largest file a call moves (${LIMITS.envBytes} bytes)`);
    if (options.mode !== undefined && !isMode(options.mode)) throw new RangeError('harness-sandbox: a mode must be 0..0o7777');
    await this.#call('writeFile', [workspacePath(this.root, path), { ...(options.mode !== undefined ? { mode: options.mode } : {}), ...(options.append ? { append: true } : {}) }], { input: bytes });
  }

  async #stat(path: string): Promise<PathStat | undefined> {
    const { value } = await this.#call('stat', [workspacePath(this.root, path)]);
    return value === null ? undefined : checkStat('stat', value);
  }

  async #lstat(path: string): Promise<PathStat | undefined> {
    const { value } = await this.#call('lstat', [workspacePath(this.root, path)]);
    return value === null ? undefined : checkStat('lstat', value);
  }

  async #list(dir: string): Promise<DirEntry[]> {
    const { value } = await this.#call('list', [workspacePath(this.root, dir)]);
    if (!Array.isArray(value) || value.length > MAX_ENTRIES) throw badResult('list', 'a malformed listing');
    return value
      .map((e) => {
        if (!isObject(e) || !isPathText(e.name, 255) || e.name.includes('/') || e.name === '.' || e.name === '..' || !PATH_TYPES.includes(e.type as PathType)) throw badResult('list', 'a malformed entry');
        return { name: e.name, type: e.type as PathType };
      })
      .sort(byName);
  }

  async #walk(dir?: string): Promise<WalkEntry[]> {
    const root = workspacePath(this.root, dir ?? this.root);
    const prefix = root === '/' ? '/' : `${root}/`;
    const { value } = await this.#call('walk', [root]);
    if (!Array.isArray(value) || value.length > MAX_ENTRIES) throw badResult('walk', 'a malformed listing');
    return value
      .map((e) => {
        if (!isObject(e) || !isPathText(e.path) || !e.path.startsWith(prefix) || workspacePath('/', e.path) !== e.path || (e.link !== undefined && e.link !== true)) throw badResult('walk', 'a malformed entry');
        const s = checkStat('walk', e);
        return { path: e.path, ...s, ...(e.link ? { link: true as const } : {}) };
      })
      .sort(byPath);
  }

  async mkdir(path: string, options: { recursive?: boolean } = {}): Promise<void> {
    await this.#call('mkdir', [workspacePath(this.root, path), { recursive: options.recursive === true }]);
  }

  async remove(path: string, options: { recursive?: boolean; force?: boolean } = {}): Promise<void> {
    await this.#call('remove', [workspacePath(this.root, path), { recursive: options.recursive === true, force: options.force === true }]);
  }

  async rename(from: string, to: string): Promise<void> {
    await this.#call('rename', [workspacePath(this.root, from), workspacePath(this.root, to)]);
  }

  async symlink(target: string, path: string): Promise<void> {
    if (!isPathText(target)) throw new TypeError('harness-sandbox: a link target must be a non-empty string without NUL');
    await this.#call('symlink', [target, workspacePath(this.root, path)]);
  }

  readlink(path: string): Promise<string> {
    return this.#checked(async () => {
      const { value } = await this.#call('readlink', [workspacePath(this.root, path)]);
      if (!isPathText(value)) throw badResult('readlink', 'a malformed target');
      return value;
    });
  }

  realpath(path: string): Promise<string> {
    return this.#checked(async () => {
      const { value } = await this.#call('realpath', [workspacePath(this.root, path)]);
      if (!isPathText(value) || !value.startsWith('/')) throw badResult('realpath', 'a malformed path');
      return value;
    });
  }

  async chmod(path: string, mode: number): Promise<void> {
    if (!isMode(mode)) throw new RangeError('harness-sandbox: a mode must be 0..0o7777');
    await this.#call('chmod', [workspacePath(this.root, path), mode]);
  }

  /** Stop using it (the link and the VM stay: they are the `SessionMachine`'s). */
  dispose(): void {
    this.#disposed = true;
  }
}

// ---------------------------------------------------------------------------
// The lazy attach (§5.6)

/**
 * A call that did not run because the session's sandbox could not be attached
 * for it (busy, still booting, a 409 loop, the link lost again): a definite
 * outcome for the call, recorded as its result, never an interrupted call.
 * Nothing reached the VM.
 */
export class NotRunError extends Error {
  readonly code = 'ENOTRUN';
  constructor(cause?: unknown) {
    super('not run: the sandbox is starting, try again', cause !== undefined ? { cause } : undefined);
    this.name = 'NotRunError';
  }
}

/**
 * The errors an attach passes on as they are, never as `NotRunError`, because
 * they stop the drive or fail the turn: a yield other than `contended` (A3's
 * refusal to arm a fresh VM with an unfenced token is `Yield('superseded')`:
 * this drive can't get one), `Unavailable`, `Fatal`, `Restart`, and the log's
 * own stop conditions (fenced, superseded, parked, interrupted), which a "not
 * run" outcome written on a log the drive no longer owns would hide.
 */
const DRIVE_STOPPING = new Set(['Unavailable', 'Fatal', 'Restart', 'LogFenced', 'LogSuperseded', 'InBandSupersededError', 'Parked', 'EffectInterrupted']);
function stopsTheDrive(err: unknown): boolean {
  const e = err as { name?: unknown; reason?: unknown } | undefined;
  if (e?.name === 'Yield') return e.reason !== 'contended';
  return typeof e?.name === 'string' && DRIVE_STOPPING.has(e.name);
}

export interface LazyRemoteMachineOptions {
  /** `/workspace`. */
  root?: string;
  /** The drive's own signal (`ctx.signal`): once it aborted, an attach throws its reason (the drive is stopping), never `NotRunError`. */
  signal?: AbortSignal;
}

/**
 * The session's `/workspace`, attached on first use (§5.6: a session that
 * never runs a command never boots a VM). `acquire` is the runtime's
 * `acquireMachine` bound to its session and fence.
 *
 * A runtime calls `attach()` in the **pre-intent phase** of a sandbox call,
 * before it writes the call's intent: a failure there is `NotRunError`, which
 * the runtime records as the call's definite outcome ("not run: the sandbox
 * is starting, try again"); the intent is written only for the operation that
 * then runs. An error that stops the drive (its signal, `stopsTheDrive`'s
 * list) is passed on unchanged. Every other method attaches first too (as
 * `attach()`), so a caller that skipped the phase still gets `NotRunError`,
 * inside its intent. A lost machine is closed and re-acquired on the next
 * attach; one attach runs at a time.
 */
export class LazyRemoteMachine implements WorkspaceMachine {
  readonly root: string;
  readonly isolation = 'sandbox';
  readonly #acquire: () => Promise<SessionMachine>;
  readonly #signal: AbortSignal | undefined;
  #session: SessionMachine | undefined;
  #pending: Promise<WorkspaceMachine> | undefined;
  #disposed = false;

  constructor(acquire: () => Promise<SessionMachine>, options: LazyRemoteMachineOptions = {}) {
    this.#acquire = acquire;
    this.root = options.root ?? '/workspace';
    this.#signal = options.signal;
  }

  /** Whether a machine is attached and its link still up. */
  get attached(): boolean {
    return !!this.#session && !this.#session.signal.aborted && !this.#session.workspace.lost;
  }

  /** Never lost: a lost VM is attached again on the next call. False only once disposed. */
  get lost(): boolean {
    return this.#disposed;
  }

  /** The attached session machine, if any (for its `fsync` and its fence). */
  get session(): SessionMachine | undefined {
    return this.attached ? this.#session : undefined;
  }

  /** The pre-intent phase: the attached machine, attaching now if needed; `NotRunError` if it can't. */
  attach(): Promise<WorkspaceMachine> {
    if (this.#disposed) return Promise.reject(new NotRunError(new Error('the workspace was disposed')));
    if (this.attached) return Promise.resolve(this.#session!.workspace);
    if (this.#pending) return this.#pending;
    // Stored before it can settle (an `acquire` that throws at once included), and cleared only if it is still this one.
    const pending = this.#attachNow();
    this.#pending = pending;
    const clear = () => {
      if (this.#pending === pending) this.#pending = undefined;
    };
    pending.then(clear, clear);
    return pending;
  }

  async #attachNow(): Promise<WorkspaceMachine> {
    // A lost machine is closed (best effort) before it is replaced: nothing reaches it through this workspace again.
    const old = this.#session;
    this.#session = undefined;
    if (old) void old.close().catch(() => {});
    try {
      if (this.#signal?.aborted) throw this.#signal.reason;
      const session = await this.#acquire();
      if (this.#disposed || session.signal.aborted || session.workspace.lost) {
        // Disposed while it attached: nothing else can reach this machine, so it is closed here.
        void session.close().catch(() => {});
        throw new Error(this.#disposed ? 'the workspace was disposed as it attached' : 'the sandbox link was lost as it was attached');
      }
      this.#session = session;
      return session.workspace;
    } catch (err) {
      if (this.#signal?.aborted) throw this.#signal.reason;
      if (stopsTheDrive(err)) throw err;
      throw new NotRunError(err);
    }
  }

  async exec(command: string, options?: ExecOptions): Promise<ExecResult> {
    return (await this.attach()).exec(command, options);
  }
  async readFile(path: string): Promise<Uint8Array> {
    return (await this.attach()).readFile(path);
  }
  async writeFile(path: string, content: string | Uint8Array, options?: { mode?: number; append?: boolean }): Promise<void> {
    return (await this.attach()).writeFile(path, content, options);
  }
  async stat(path: string): Promise<PathStat | undefined> {
    return (await this.attach()).stat(path);
  }
  async lstat(path: string): Promise<PathStat | undefined> {
    return (await this.attach()).lstat(path);
  }
  async list(dir: string): Promise<DirEntry[]> {
    return (await this.attach()).list(dir);
  }
  async walk(dir?: string): Promise<WalkEntry[]> {
    return (await this.attach()).walk(dir);
  }
  async mkdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    return (await this.attach()).mkdir(path, options);
  }
  async remove(path: string, options?: { recursive?: boolean; force?: boolean }): Promise<void> {
    return (await this.attach()).remove(path, options);
  }
  async rename(from: string, to: string): Promise<void> {
    return (await this.attach()).rename(from, to);
  }
  async symlink(target: string, path: string): Promise<void> {
    return (await this.attach()).symlink(target, path);
  }
  async readlink(path: string): Promise<string> {
    return (await this.attach()).readlink(path);
  }
  async realpath(path: string): Promise<string> {
    return (await this.attach()).realpath(path);
  }
  async chmod(path: string, mode: number): Promise<void> {
    return (await this.attach()).chmod(path, mode);
  }

  /**
   * Stop using it. An attached session machine stays the runtime's (it closes
   * it at the invocation's end); one an attach still pending resolves with
   * is closed as it arrives.
   */
  dispose(): void {
    this.#disposed = true;
    this.#session = undefined;
  }
}

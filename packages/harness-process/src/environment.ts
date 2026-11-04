import type { ChaosKind, Drill, DrillTarget, DriveFence } from 'harness-world';
import type { WorkspaceMachine } from 'harness-world-hands/runtime';

/**
 * Where a process harness's CLI runs, as types (docs/workspaces.md §3.1,
 * §4.1, §5.4). Shared by the harness packages, which can't import
 * durable-harness, and by the sandbox tier that implements them; nothing
 * here runs a process.
 *
 * - `SessionMachine`: one session's VM, as the driver holds it for one
 *   invocation: spawn the CLI on it, upload and wipe its cache files, tail a
 *   file the CLI writes, flush the Drive, and its `/workspace` as a
 *   `WorkspaceMachine` (files and commands, for a Function-tier runtime's
 *   tools). Implemented by the sandbox tier's session factory over the in-VM
 *   relay, and branded and frozen there (`harness-world-process/machine-brand`),
 *   so a harness that must run only in a sandbox (Claude Code, Codex) can
 *   refuse a hand-written one by provenance, not by shape.
 * - `AcquireMachine`: how a runtime gets one, with the fencing token it
 *   arms the relay with (the kernel's `DriveContext.fence`; pi-durable and
 *   libfx supply their own).
 * - `LineChannel`: a child's stdio as line-oriented harness code uses it
 *   (Codex's app-server client, the reference harness): a `HostedProcess`
 *   on this machine and a `SessionProcess` in a VM are both one.
 * - `ProcessHostLike`: what a process harness asks of its host, so the host
 *   can be `ProcessHost` (this machine: the reference harness, tests) or
 *   one in a VM.
 * - `GatewayHandler`: the driver's side of the VM's model port: a relayed
 *   model request in, the Gateway's response out (§5.5).
 *
 * The files and commands a Function-tier harness's tools use are
 * harness-world-hands' `WorkspaceMachine` (its `/runtime` entry), not these.
 */

// ---------------------------------------------------------------------------
// Fencing

/**
 * A token that only rises across the owners of a session, and stays the
 * same for one owner: a newer owner's is higher, and no two owners hold the
 * same one. The relay records the highest it was armed with, refuses a
 * lower one, and lets a higher one kill the session's processes (§5.4).
 * The kernel's is `DriveContext.fence` (the count of the drive's first
 * accepted in-band write); a runtime with its own log supplies its own
 * (pi-world `WorldStorage`'s fence, libfx's lease). `fenced: false` says the
 * owner's own writes are no longer fenced (see `DriveFence.fenced`).
 */
export type FenceToken = DriveFence;

// ---------------------------------------------------------------------------
// A child's stdio

/** How a child exited (`signal` is the signal's name). */
export interface ChannelExit {
  code: number | null;
  signal: string | null;
}

/** A child's stdio, line by line: what line-oriented harness code reads and writes. */
export interface LineChannel {
  /** Write one line to the child's stdin (a value is sent as JSON). False once stdin is closed. */
  send(line: unknown): boolean;
  /** Close the child's stdin. */
  end(): void;
  /** The child's stdout, line by line. One reader per process. */
  lines(): AsyncIterable<string>;
  /** Resolves when the child exits (never rejects). */
  readonly exited: Promise<ChannelExit>;
  /** The last few KB of stderr. */
  stderrTail(): string;
}

/** The signals a harness may send a child (the ones the relay's `KILL` accepts). */
export type KillSignal = 'SIGTERM' | 'SIGKILL' | 'SIGINT';

/** A running child, wherever it runs: its stdio, and a way to stop it with everything it started. */
export interface ProcessLike extends LineChannel {
  /** Whether it is still running. */
  alive(): boolean;
  /** Kill it and everything it started (its process group here; the session's cgroup in a VM). Default `SIGTERM`. */
  kill(signal?: KillSignal): void;
}

// ---------------------------------------------------------------------------
// The host a process harness runs its child on

/** What a process harness spawns. */
export interface ProcessSpawn {
  command: string;
  args?: string[];
  /** The child's whole environment, on top of what the host sets itself: nothing else is inherited. */
  env?: Record<string, string | undefined>;
  /** Default: the host's directory for the session. */
  cwd?: string;
  /** Kill the child when this aborts. */
  signal?: AbortSignal;
  /**
   * Runs with the session claimed, after a child a previous drive left is
   * killed and before this one starts: rebuild the harness's native session
   * files here (from the journal), so no other drive of this host starts a
   * child, and no stale one writes, in between. On a host over a
   * `SessionMachine` that is `files.upload` before the `SPAWN`, and the
   * claim is the relay's fence (the arm this driver holds).
   */
  prepare?: () => Promise<void> | void;
}

/**
 * What a process harness asks of the host its child runs on. `ProcessHost`
 * (this machine) is one; a host the sandbox tier builds over a branded
 * `SessionMachine` is another. The session is claimed for one child at a
 * time: a spawn while another drive of this host runs the session's child
 * throws `Yield('contended')`. stderr is always kept as a tail
 * (`stderrTail()`), as the relay keeps it.
 *
 * Not interchangeable for callers: a ProcessHostLike carries no brand, so
 * Claude Code and Codex never take one from their options (that would let
 * a host `child_process` in). They take a branded `SessionMachine` and build
 * their host from it internally. `ProcessHost` is for the reference
 * harness, the relay and tests only (docs/workspaces.md §6 rules 1 and 3).
 */
export interface ProcessHostLike {
  /** Start the session's child (after killing one a previous drive left running). */
  spawn(runId: string, options: ProcessSpawn): Promise<ProcessLike>;
  /** Whether a drive of this host runs (or is starting) the session's child. */
  busy(runId: string): boolean;
  /** Kill a child (and what it started) a previous drive left running, before a resume. Returns what it killed. */
  killStale(runId: string): Promise<unknown>;
  /** Kill what the session's tools started that is still running (at the drive's end). Returns what it killed. */
  killGroups(runId: string): Promise<unknown>;
}

// ---------------------------------------------------------------------------
// A session's VM

/** The per-session cache directories in the VM, wiped before every spawn (§5.2): HOME, the CLIs' config homes, TMPDIR, XDG caches. */
export type CacheDir = 'home' | 'config' | 'codex-home' | 'tmp' | 'cache';

/** One file to upload into a cache directory, at a path relative to it (no `..`, not absolute). */
export interface CacheFile {
  path: string;
  bytes: Uint8Array;
  /** Default 0o600. */
  mode?: number;
}

/** What a harness spawns on a `SessionMachine`. The relay rebuilds the child's environment from its own allowlist plus `env`. */
export interface MachineSpawn {
  command: string;
  args?: string[];
  /**
   * Variables for the child, on top of the relay's allowlist: the one
   * channel through which a credential could reach the VM (decision 6).
   * The relay can't judge a secret, so the session factory checks every
   * spawn's `env` in the Function before it is sent: §3.1's name and value
   * rules (`sandbox({ env })`'s), and no value equal to a `process.env`
   * value of 16 or more characters. A failing key is a `TypeError` naming
   * the key, never the value. Every harness inherits the check (A3 tests
   * it once).
   */
  env?: Record<string, string>;
  /** Default the workspace root (`/workspace`). */
  cwd?: string;
  /** Kill the session's processes when this aborts. */
  signal?: AbortSignal;
  /**
   * The child's own commands run in a pid namespace of their own (Claude
   * Code's sandbox), or it runs none (its built-ins off). The model port
   * admits a CLI that can be traced from the VM only when this is set: a
   * command beside a traceable CLI could read its key from its environ, or
   * take its model socket (§5.5). Codex needs no claim: its binary is
   * installed execute-only, so the process is not dumpable.
   */
  isolatedCommands?: boolean;
}

/** A child on a `SessionMachine`: its stdio over the relay. `kill` always takes the session's whole cgroup. */
export interface SessionProcess extends ProcessLike {}

/**
 * A line-complete tail of a file the CLI appends to under its cache
 * directory (a Codex rollout), served in the VM by the unprivileged helper.
 * The same shape as `FileTail`: `read()` returns what has arrived since.
 */
export interface MachineTail {
  /** Byte offset consumed so far. */
  readonly offset: number;
  /** The complete lines that arrived since the last read. Throws once the file shrank or was replaced. */
  read(): string[];
  /** Wait until every line the file holds now has arrived (`SYNC`). */
  sync(): Promise<void>;
  close(): void;
}

/**
 * One session's VM, for one driver: what a process harness spawns its CLI
 * on. Every byte it returns came from an untrusted VM: callers bound and
 * parse it strictly, and never use a path it reports on their own fs.
 */
export interface SessionMachine {
  /** Provenance: set only by the sandbox tier's session factory (`machineBrand`); checked with `isBrandedMachine`. */
  readonly brand: MachineBrand;
  /** The session (`runId`). */
  readonly sessionId: string;
  /**
   * The token the relay was armed with for this attach, read once when the
   * runtime owned the session and reused for every arm and re-arm of it
   * (a dropped socket, a relay restart), so the relay sees one token per
   * owner. Undefined: none (a World without the in-band fence).
   */
  readonly fence: FenceToken | undefined;
  /** The workspace root: `/workspace`, the Drive. */
  readonly root: string;
  /**
   * The id of the Drive at `root` (the `hw.sandbox` record's `driveId`): a
   * different id on a later attach means the session's files were lost.
   * Undefined on a machine with no Drive record.
   */
  readonly driveId?: string;
  /**
   * The session's `/workspace` as files and commands (`RemoteMachine`: the
   * relay's helper over `ENV_CALL`, as the unprivileged user, rooted at
   * `/workspace`): what a Function-tier runtime's tools run on in a sandbox,
   * the same interface as `virtual()`'s.
   */
  readonly workspace: WorkspaceMachine;
  /**
   * Run `fn` on the session's workspace before anything else runs on this
   * attach, with commands whose `env` may carry credentials (a token for a
   * private clone): only the structural rules apply to it (valid names, no
   * NUL, no loader variables), not the credential rules of `MachineSpawn.env`.
   *
   * The values reach the VM only in the commands `fn` runs. Nothing the
   * model drives is running yet (an attach kills what an earlier one left,
   * and this one has started nothing), so no process of the same user is
   * there to read a command's environment. When `fn` settles, the session's
   * processes are killed (the helper that carried the values, and anything
   * the commands left running) and the workspace it was given refuses every
   * operation. Refused with a `TypeError` once anything started on this
   * attach (a `spawn`, or a command through `workspace`); `spawn` and
   * `workspace`'s commands are refused while `fn` runs. What the commands
   * write stays on the Drive: keep credentials out of files (a remote URL,
   * the npm config).
   */
  provision?<T>(fn: (workspace: WorkspaceMachine) => Promise<T>): Promise<T>;
  /** The cache directories, as paths in the VM. */
  readonly dirs: Readonly<Record<CacheDir, string>>;
  /** Aborted when the link to the VM is lost (a socket dropped, the relay superseded or gone): re-attach. */
  readonly signal: AbortSignal;
  /**
   * The model port in the VM, as the CLI's base URL (`http://127.0.0.1:4100`):
   * every request the CLI sends there is relayed over `/model` to the
   * `GatewayHandler` the acquire passed (§5.5).
   */
  readonly modelUrl: string;
  /**
   * The CLI's model key `P` for this attach (§5.5): give it to the CLI as its
   * API key (`ANTHROPIC_AUTH_TOKEN`, Codex's `env_key`). The relay admits a
   * model request only with it, and only from a process this attach spawned,
   * so it is a placeholder, not a credential: it authorizes nothing outside
   * this VM's model port. The spawn env check lets its variable name through
   * only with exactly this value.
   */
  readonly modelKey: string;
  /**
   * What the model must be told about this attach (§5.7): today only that
   * the session's files were lost (a new Drive). Frozen; empty when nothing
   * changed. Every machine of the invocation carries it, and every later
   * attach does too until the runtime acknowledges it in the turn that
   * told the model (the record's `filesLost`; the kernel's harness clears it
   * with `acknowledgeNotes` on the `hw.sandbox` document), so a turn that
   * failed before the model saw it shows it again.
   */
  readonly notes: readonly string[];
  /**
   * What `notes` describe (the new Drive's id while the "files lost" note is
   * shown), for the runtime to acknowledge exactly them: an acknowledgement
   * of an older key leaves a loss recorded since in place. Undefined when
   * there are no notes.
   */
  readonly notesKey?: string;
  /** Start a process in the session's cgroup, as the unprivileged user. Rejects with a `TypeError` for an `env` the checks refuse (`MachineSpawn.env`). */
  spawn(options: MachineSpawn): Promise<SessionProcess>;
  /** The session's cache files (`FILES`, `WIPE`). */
  readonly files: {
    /** Unpack `files` into `dir` (freshly wiped), as the unprivileged user. */
    upload(dir: CacheDir, files: readonly CacheFile[]): Promise<void>;
    /** Delete every cache directory and make them again, empty. */
    wipe(): Promise<void>;
  };
  /** Follow a file under `dir` from `offset` (default 0): `path` is relative to it, and the helper refuses anything outside. */
  tail(dir: CacheDir, path: string, options?: { offset?: number }): Promise<MachineTail>;
  /** Flush the Drive's writes (`sync -f /workspace`): before a mutating call's outcome is recorded. */
  fsync(): Promise<void>;
  /** Kill the session's processes and close the link. The VM and the Drive stay. */
  close(): Promise<void>;
}

/**
 * What a runtime keeps about its session's sandbox (docs/workspaces.md §5.7;
 * the kernel's is the `hw.sandbox` document). The Drive is the record: its
 * name comes from the session, and it carries its own region and size, so
 * this exists only to notice change. A `driveId` other than the stored one
 * means the files were lost: `filesLost` keeps the new id until the runtime
 * acknowledges the note (`SessionMachine.notes`); `versions` only move
 * forward (a newer recorded CLI than the image's is
 * `Unavailable('cli_version')`), and hold only versions the driver pins
 * itself, never one a VM merely reported; `vmSeconds` is the VM time granted
 * so far, against `sandbox({ budget: { vmSeconds } })`, written as soon as it
 * is granted (before the arm), so a delivery that fails after a grant still
 * counts it. The rest is for diagnosis.
 */
export interface MachineRecord {
  readonly sandboxName?: string;
  readonly driveName?: string;
  readonly driveId?: string;
  readonly region?: string;
  readonly driveSize?: number;
  readonly image?: string;
  readonly versions?: Readonly<Record<string, string>>;
  readonly vmSeconds?: number;
  /** The `driveId` of a Drive the model has not been told is new yet (the "files lost" note is in `notes` while it is set). */
  readonly filesLost?: string;
}

/** Where a runtime reads and writes its `MachineRecord`, in a document it already has (the kernel: `hw.sandbox`, in the attach fact's body). */
export interface MachineRecordStore {
  read(): MachineRecord | undefined | Promise<MachineRecord | undefined>;
  write(record: MachineRecord): void | Promise<void>;
}

/** How a runtime gets its session's machine. */
export interface AcquireMachineRequest {
  sessionId: string;
  /**
   * The runtime's fencing token, read once per acquire, after the runtime
   * owns the session (for the kernel: inside the body of the `sandbox.attach`
   * fact, `DriveContext.fence`), and reused for every arm and re-arm of
   * that acquire (`SessionMachine.fence`). Within one invocation its count
   * stays the same; a later read may only narrow it (`fenced` turned false),
   * and the session factory then stops every machine of the invocation from
   * mutating the VM.
   *
   * Undefined (no token: a World without the in-band fence) or not
   * `fenced`: the token never preempts a recorded one, and is admitted only
   * once the old sockets are dead (the relay refuses a second driver at the
   * same token instead of letting it preempt). A token that is not `fenced`
   * also gets no fresh VM armed, and no `WIPE`, `FILES` or mutating
   * `SPAWN`/`ENV_CALL` from the session factory: on a fresh VM the World's
   * fence is the only guard, and that drive is no longer behind it.
   */
  fence: () => FenceToken | undefined;
  /**
   * The invocation's own signal: the session factory keeps one attach per
   * session per signal, so an acquire after a loss within the same
   * invocation re-attaches with the count it read the first time; when the
   * signal aborts, the invocation's links close.
   */
  signal: AbortSignal;
  /**
   * The session's config as its log recorded it (its `run_created`'s:
   * the kernel's `DriveContext.config`, pi-world's `WorldStorage.config`),
   * for the factory's admission checks: durable-harness's `sandbox()`
   * refuses a session a public chat route started without its opt-in
   * before any VM boots. Every runtime passes it, `undefined` when the
   * session recorded none; durable-harness's acquire refuses a request
   * without the key at all (a runtime that forgot it fails closed).
   */
  config?: unknown;
  /**
   * Where the session's `MachineRecord` lives. Without one, a lost Drive is
   * never noticed; the session factory refuses (a `TypeError`) to go without
   * one when `budget.vmSeconds` is finite, since the budget would then hold
   * per invocation only. pi-durable and libfx pass a store backed by a
   * document they already have.
   */
  record?: MachineRecordStore;
  /**
   * Serves the CLI's model calls (`/model`, §5.5): the harness's
   * `gatewayHandler`, with its steps and budget. Without one, every model
   * call fails `internal`. A re-acquire within the invocation that passes
   * another handler rebinds the link to it.
   */
  model?: GatewayHandler;
  /** When this invocation ends (epoch ms), so the VM stays up at least that long plus `idleMs`. Default: 800 s from now (the longest Function). */
  deadline?: number;
  /**
   * The drive's drills on the session's sandbox (docs/workspaces.md §8
   * "Drills"; for the kernel: `attachInDrive` builds it from the attach
   * fact's `io.drill` and the drive's `ctx.drills`). Absent: the runtime runs
   * no drills, and the session factory carries none out.
   */
  drills?: AcquireDrills;
}

/** What a session factory needs to carry out the drills armed for a session (`AcquireMachineRequest.drills`). */
export interface AcquireDrills {
  /**
   * Claim, now, an armed attach drill of one of `kinds` (`sandbox-new-vm`,
   * `sandbox-zombie`): its chaos step is committed against the attach. The
   * drill, for the factory to carry out before it attaches; undefined when
   * none is armed.
   */
  claim(kinds: readonly ChaosKind[]): Promise<Drill | undefined>;
  /** Register what carries out the mid-step drills (`sandbox-vm` at a tool, `relay-drop`) for the drive: once per drive, however often it re-attaches. */
  register(target: DrillTarget): void;
}

/** The sandbox tier's machine acquisition (docs/workspaces.md §4.1), for runtimes with and without the kernel. */
export type AcquireMachine = (request: AcquireMachineRequest) => Promise<SessionMachine>;

// ---------------------------------------------------------------------------
// The brand

declare const brandType: unique symbol;

/**
 * An opaque provenance mark (`SessionMachine.brand`). Only the minter in
 * `harness-world-process/machine-brand` makes one; `isBrandedMachine`
 * checks it.
 */
export interface MachineBrand {
  readonly [brandType]: true;
}

// ---------------------------------------------------------------------------
// The model port

/**
 * One model request the CLI sent to its loopback port, relayed by the VM
 * (`/model`). `method` and `path` are VM-chosen text: the handler only
 * matches them against its fixed table, never joins them into a URL.
 */
export interface RelayedModelRequest {
  method: string;
  /** Pathname and query, as the CLI sent them. */
  path: string;
  headers: Readonly<Record<string, string>>;
  body: Uint8Array;
}

/** The handler's answer: allowlisted headers, and the body as it streams from the Gateway. */
export interface RelayedModelResponse {
  status: number;
  headers: Readonly<Record<string, string>>;
  body: AsyncIterable<Uint8Array>;
}

/**
 * Why the handler refused or failed a request. Fixed codes: the details go
 * to the Function's log, never to the VM. `budget` is a spend cap (the turn
 * or the session is out of money: no retry helps), `busy` the concurrency
 * cap (a retry soon may pass); the relay answers them 403 and 429.
 */
export type GatewayErrorCode = 'bad_request' | 'forbidden' | 'budget' | 'busy' | 'upstream' | 'internal';

/**
 * The driver's side of the model port (§5.5): match the request against the
 * harness's fixed table, validate its body, model and budget, call the
 * Gateway at a fixed URL with the credential, and record the step. A
 * request it refuses rejects with an error whose `code` is a
 * `GatewayErrorCode`. `signal` aborts the upstream call (`/model` closed).
 */
export type GatewayHandler = (request: RelayedModelRequest, signal: AbortSignal) => Promise<RelayedModelResponse>;

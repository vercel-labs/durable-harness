/**
 * The session factory (docs/workspaces.md §5.4, §5.5, §5.7, §6 rule 1): a
 * session's Vercel Sandbox VM and Drive, armed with the runtime's fence, as a
 * branded `SessionMachine`. This is the **one** module in the repo that imports
 * `harness-world-process/machine-brand`: it claims the brand once, at load, and
 * keeps the minter module-private, so a hand-written object with a `spawn` can
 * never pass `isBrandedMachine` and Claude Code or Codex refuse it by
 * provenance. The stand-in (`localMachine`, §5.8) brands through the guarded
 * `brandForTests` below, never a second claim.
 *
 * `sandboxSession(options)` returns an `AcquireMachine`. One acquire:
 *
 * 1. reads the runtime's fence (for the kernel: inside the `sandbox.attach`
 *    fact's body, `attachInDrive`): the first acquire of an invocation keeps
 *    its token for every arm, and each later one can only narrow it (a
 *    `fenced` that turned false stays false; another count is an error);
 * 2. finds the session's Drive by name (`dh-<hash>`), or creates it in the
 *    descriptor's region; on a race it adopts the winner's, region and size
 *    included; a `driveId` other than the recorded one gives the model a
 *    "files lost" note until the runtime acknowledges it (`acknowledgeNotes`);
 * 3. finds the session's named sandbox: running, it is reused; not found, it
 *    is created with the Drive at `/workspace` (and its VM time recorded at
 *    once); stopped, failed or aborted, the owner deletes it (after
 *    re-reading it and finding the same VM session it saw) and creates a
 *    fresh one, retrying while the Drive is still attached (409, P0.5);
 * 4. arms the relay through the control plane (`runCommand`, sudo) with
 *    `sha256(T)`, the fence and the session, refusing a degraded or
 *    cgroup-less VM, a model port that can't tell the CLI's calls from other
 *    processes' (`modelAuth`: the attach's key `P` and the peer's pid) and a
 *    CLI downgrade (`Unavailable('cli_version')`). The arm is the proof of
 *    ownership: only once the relay accepted it does the owner rotate a VM
 *    too close to the plan's cap (between turns), re-apply the network
 *    policy (`update({ networkPolicy })`, which raises the VM's `dh-fence`
 *    tag) and extend the VM's timeout to the invocation's end plus `idleMs`,
 *    within the plan's cap and the session's `budget.vmSeconds` (recorded at
 *    once, before the sockets);
 * 5. dials `/ctl` and `/model` (the model calls go to the acquire's
 *    `GatewayHandler` through `tunnel.ts`), re-arming with the same `T` if an
 *    upgrade fails, and returns the branded machine.
 *
 * Who may touch a VM. While the token is missing or `fenced` is false the
 * factory arms no fresh VM, deletes nothing, re-applies nothing and extends
 * nothing, and the machine refuses `WIPE`, `FILES` and `SPAWN` (`machine.ts`);
 * a token that is not fenced never preempts at the relay. A fenced token is
 * not proof of current ownership either (a drive that hasn't written since a
 * newer owner took over still reads `fenced`), so a running VM is deleted only
 * after the relay accepted this driver's arm (rotation, an anomaly) or, when
 * the VM can't be asked, after two probes went unanswered and its `dh-fence`
 * watermark is no higher than this driver's count (loss). A Drive held past
 * the busy budget, a contended arm, a transient Sandbox API error (429, 5xx,
 * the Function's network) and an unfenced token that would need a fresh or
 * replaced VM yield (`Yield('contended')`). What the VM says against this
 * driver (a `stale` arm, a 4409 close, a watermark above its count) is
 * checked against the World: `Yield('preempted')` only when the World agrees
 * (the token is no longer fenced), else `Yield('contended')`, which wakes the
 * session again, so a compromised VM can't park a session for good.
 *
 * A lost link (a socket dropped, the relay gone) is re-attached by the next
 * acquire in the same invocation, with the same token: the relay re-armed on
 * the same VM if it answers, else (owner only) a fresh VM on the same Drive, a
 * bounded number of times; past that, a transport error. A protocol violation
 * is not re-attached: it fails the delivery, so a misbehaving VM can't loop
 * the queue for free. An invocation's links are closed when its signal aborts.
 *
 * The drills (docs/workspaces.md §8, `AcquireMachineRequest.drills`), carried
 * out by an owner only: at the invocation's first attach, an armed
 * `sandbox-new-vm` deletes the session's running VM and waits for the Drive
 * to let go of it (D2: the turn boots a new VM on the same Drive), and
 * `sandbox-zombie` stops it and leaves it holding the Drive (D3: this attach
 * finds it, deletes it once stopped, waits out the 409 and resumes); once
 * attached, the drive's target stops the VM mid-step (`sandbox-vm`, D1) or
 * drops one of the link's sockets (`relay-drop`, D4: `/model` in a model
 * step, else `/ctl`), and the loss is re-attached as any other.
 */
import { ATTACH_DRILLS, type DrillTarget, Unavailable, Yield } from 'harness-world';
import { machineBrand, type BrandedMachine } from 'harness-world-process/machine-brand';
import type { AcquireDrills, AcquireMachine, AcquireMachineRequest, FenceToken, GatewayHandler, MachineRecord, SessionMachine } from 'harness-world-process';
import { ApiConflict, apiErrorOf, type ApiCredentials, type BootSource, type DriveInfo, FENCE_TAG, type SandboxApi, type SandboxNetworkPolicy, type VmHandle, vercelSandboxApi } from './api.ts';
import { CLAUDE_CLI, CODEX_CLI } from './clis.ts';
import { fencedToken, sessionMachine } from './machine.ts';
import { defaultRegion, sessionName } from './names.ts';
import { dialRelay, type RelayLink } from './relay/dial.ts';
import { armArgs, armRequest, type ArmReply, isObject, keyHash, mintModelKey, mintToken, tokenHash } from './relay/protocol.ts';
import { modelTunnel } from './tunnel.ts';
import { CACHE_ROOT, MODEL_PORT, RELAY_COMMAND, RELAY_CONFIG, RELAY_PORT, vmDirs, WORKSPACE } from './vm.ts';

// The kernel's side (the attach fact, the record, the note acknowledgement) is in its own module, so a CLI harness loads it without the factory.
export { acknowledgeInDrive, acknowledgeNotes, attachInDrive, kernelSandbox, SANDBOX_DOC } from './attach.ts';
export type { KernelSandbox } from './attach.ts';

/** The module-private minter: claimed once at load, never handed out. */
const brandMachine = machineBrand();

/** Whether the test-only seams may run: the stand-in's guard (§5.8). Throws with what is missing. */
function testGuard(what: string): void {
  if (process.env.DH_TEST_LOCAL_SANDBOX !== '1') throw new Error(`harness-sandbox: ${what} needs DH_TEST_LOCAL_SANDBOX=1 (the stand-in, §5.8)`);
  if (!process.env.VITEST && process.env.NODE_ENV !== 'test') throw new Error(`harness-sandbox: ${what} runs only under a test runner (VITEST or NODE_ENV=test)`);
  if (process.env.VERCEL) throw new Error(`harness-sandbox: ${what} refuses to run with VERCEL set`);
  if (process.env.NODE_ENV === 'production') throw new Error(`harness-sandbox: ${what} refuses to run in production`);
}

/**
 * Brand a stand-in machine for tests only (`localMachine`, §5.8). Guarded like
 * the stand-in itself so it can never run in a published build or a real
 * deployment: the test flag must be set, a test runner must be running
 * (`VITEST`, or `NODE_ENV=test`), `VERCEL` must be unset, and `NODE_ENV` must
 * not be `production`. The flag alone is not enough: on a host that is not
 * Vercel (self-hosted, Cloudflare) an operator could set it. It brands through
 * the one private minter, so a process that loads both the factory and the
 * stand-in never claims the brand twice. Not re-exported from the package index
 * or any published entry; A7's release gate asserts it and `test/local-machine`
 * are absent from every published bundle (§6 rule 2).
 */
export function brandForTests<M extends Omit<SessionMachine, 'brand'>>(machine: M): BrandedMachine<M> {
  testGuard('brandForTests');
  return brandMachine(machine);
}

// ---------------------------------------------------------------------------
// Options

/** The descriptor fields the factory reads: durable-harness's `SandboxWorkspace`, structurally (this package can't import it). */
export interface SandboxSpec {
  readonly region?: string;
  readonly image?: string | { readonly snapshot: string };
  readonly vcpus: number;
  readonly network: 'allow-all' | 'deny-all' | { readonly allow: readonly string[] };
  readonly idleMs: number;
  readonly driveSize: number;
  /** A command's longest run (`sandbox({ commandTimeoutMs })`). Default `RemoteMachine`'s. */
  readonly commandTimeoutMs?: number;
  readonly budget: { readonly vmSeconds: number };
  readonly credentials?: ApiCredentials;
}

export interface SandboxSessionOptions {
  spec: SandboxSpec;
  /**
   * What a VM boots from when `spec.image` is unset: the project's base
   * snapshot (`prepareBase`). Required then. Called with `refresh` once when a
   * create fails because the snapshot is gone (expired, or rebuilt
   * elsewhere), so a cache behind it can drop the stale id.
   */
  base?: (region: string, options?: { refresh?: boolean }) => Promise<BootSource>;
  /**
   * One VM session's cap on the plan (45 min on Hobby, 24 h on Pro and
   * Enterprise). Default: learned (once per factory) from the platform's
   * answers, assumed 45 min (the smallest) until then. A create asks for at
   * most the cap as known (until an extension proved more, 45 min), so it
   * never fails on it; at the assumed cap the VM is extended past 45 min
   * rather than rotated: granted means the larger cap, refused with
   * `sandbox_timeout_invalid` means 45 min (and a create past a learned cap
   * that the plan refuses, by its message or any 400, falls back to it). A VM
   * is rotated between turns before the cap.
   */
  planCapMs?: number;
  /** How long a booting VM or a Drive another VM session still holds is waited for, before `Yield('contended')`. Default 20 s (P0.5: 5-8 s after a delete). */
  busyMs?: number;
  /** Re-attaches after a lost link within one invocation. Default 3. */
  reattaches?: number;
  /**
   * The CLI versions this deployment pins (default Claude Code's and Codex's,
   * `CLAUDE_CLI`, `CODEX_CLI`). Only a version the arm reports that equals
   * one of these is recorded: a VM's report alone never moves the record,
   * so a compromised VM can't claim a version no image has and lock the
   * session out (`cli_version`) for good.
   */
  cliVersions?: Readonly<Record<string, string>>;
  /** Tags on the sandboxes it creates (at most 4: the factory adds `dh-fence`). */
  tags?: Record<string, string>;
  /** Where lifecycle events are described (the Function's log). Never a token. */
  log?: (message: string, fields?: Record<string, unknown>) => void;
  /**
   * Test seams (guarded like the stand-in): the Sandbox API, the names,
   * whether a degraded or cgroup-less relay is accepted, whether a model
   * handler may be served on a model port that can't check its caller's pid
   * (the stand-in on macOS has no `/proc`), and the pause before a second
   * loss probe.
   */
  test?: { api?: SandboxApi; name?: (projectId: string, sessionId: string) => string; allowDegraded?: boolean; allowUnauthenticatedModel?: boolean; probeMs?: number };
}

/** The note the model gets when the session's Drive is not the one its log knew (§5.7), until the runtime acknowledges it. */
export const FILES_LOST_NOTE =
  "The session's files were lost: /workspace is a new, empty Drive (the previous one was deleted). Files written in earlier turns are gone; the conversation is intact.";

/** A VM-time budget the session ran out of (`sandbox({ budget: { vmSeconds } })`): `Unavailable`, so the turn ends at once saying so (no redelivery grants more). */
export class SandboxBudgetExceeded extends Unavailable {
  readonly cap = 'vmSeconds' as const;
  constructor(seconds: number) {
    super(`This session used its sandbox VM time (${seconds} s, sandbox({ budget: { vmSeconds } })): no more VM time is granted.`, {
      hint: 'Raise sandbox({ budget: { vmSeconds } }) to give the session more VM time.',
    });
  }
}

/** The link to the VM was lost more often than an invocation re-attaches: a transport error, so it never counts toward the kernel's failing limit (§5.3). */
export class SandboxLost extends Error {
  readonly code = 'ECONNRESET';
  constructor(reason: string) {
    super(`harness-sandbox: the link to the session's sandbox was lost (${reason})`);
    this.name = 'SandboxLost';
  }
}

// ---------------------------------------------------------------------------
// The network policy (§5.7 Egress, P0.14)

/** Hosts the VM is answered 403 for even when everything else is open: the VM holds no credential for them (HTTPS only, P0.14). */
function forcedHosts(env: Readonly<Record<string, string | undefined>> = process.env): string[] {
  const hosts = new Set(['ai-gateway.vercel.sh', 'api.vercel.com']);
  for (const k of ['VERCEL_URL', 'VERCEL_BRANCH_URL', 'VERCEL_PROJECT_PRODUCTION_URL']) {
    const v = env[k]?.replace(/^https?:\/\//, '').replace(/\/.*$/, '');
    if (v) hosts.add(v);
  }
  return [...hosts];
}

/** The descriptor's `network` as the platform's policy: custom mode with the forced 403s for `allow-all`. */
export function networkPolicy(network: SandboxSpec['network'], env?: Readonly<Record<string, string | undefined>>): SandboxNetworkPolicy {
  if (network === 'deny-all') return 'deny-all';
  if (network === 'allow-all') return { allow: { ...Object.fromEntries(forcedHosts(env).map((h) => [h, [{ response: { statusCode: 403 } }]])), '*': [] } };
  return { allow: Object.fromEntries(network.allow.map((d) => [d, []])) };
}

// ---------------------------------------------------------------------------
// The arm reply (untrusted: it came from the VM)

const ARM_CODES = new Set(['stale', 'contended', 'session', 'listener', 'bad_request', 'internal']);

/** The arm's JSON reply (its last stdout line), checked field by field; undefined for anything else. */
export function parseArmReply(stdout: string): ArmReply | undefined {
  const line = stdout.trim().split('\n').at(-1) ?? '';
  if (line.length > 64 * 1024 || !line.startsWith('{')) return undefined;
  let raw: unknown;
  try {
    raw = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (!isObject(raw) || typeof raw.ok !== 'boolean') return undefined;
  const str = (v: unknown, max = 256) => typeof v === 'string' && v.length > 0 && v.length <= max;
  if (!raw.ok) return typeof raw.code === 'string' && ARM_CODES.has(raw.code) ? ({ ok: false, code: raw.code as never, error: String(raw.error ?? '').slice(0, 500) } as ArmReply) : undefined;
  if (!str(raw.R, 128) || !/^[A-Za-z0-9_-]+$/.test(raw.R as string) || !str(raw.bootId) || !str(raw.relayId) || !str(raw.sessionId) || !str(raw.relayVersion)) return undefined;
  if (raw.tier !== 'privileged' && raw.tier !== 'degraded') return undefined;
  if (typeof raw.cgroup !== 'boolean' || typeof raw.platformGuard !== 'boolean') return undefined;
  if (raw.modelAuth !== undefined && typeof raw.modelAuth !== 'boolean') return undefined;
  if (!isObject(raw.cliVersions) || Object.keys(raw.cliVersions).length > 16 || !Object.entries(raw.cliVersions).every(([k, v]) => k.length <= 64 && str(v, 64))) return undefined;
  return {
    ok: true,
    R: raw.R as string,
    bootId: raw.bootId as string,
    relayId: raw.relayId as string,
    sessionId: raw.sessionId as string,
    relayVersion: raw.relayVersion as string,
    cliVersions: { ...(raw.cliVersions as Record<string, string>) },
    tier: raw.tier,
    cgroup: raw.cgroup,
    platformGuard: raw.platformGuard,
    modelAuth: raw.modelAuth === true,
  };
}

/** `a` newer than `b`, comparing dotted numbers (a non-numeric part compares as text). */
function newer(a: string, b: string): boolean {
  const pa = a.split(/[.-]/);
  const pb = b.split(/[.-]/);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const x = pa[i] ?? '0';
    const y = pb[i] ?? '0';
    if (x === y) continue;
    const nx = Number(x);
    const ny = Number(y);
    if (Number.isFinite(nx) && Number.isFinite(ny)) return nx > ny;
    return x > y;
  }
  return false;
}

// ---------------------------------------------------------------------------
// The factory

/** One session's attach within one invocation: what is read once and reused for every re-attach. */
interface Attach {
  readonly sessionId: string;
  /** The token, as first read; a later read can only narrow it (`fenced` to false). */
  fence: FenceToken | undefined;
  /** The handler the model socket serves: the latest acquire's (the tunnel reads it per request). */
  model?: GatewayHandler;
  /** What the model must be told, for every machine of this invocation. */
  readonly notes: string[];
  /** What `notes` describe: the new Drive's id while the "files lost" note is in them. */
  notesKey?: string;
  /** The CLI's model key `P` for this invocation's attaches (§5.5): the relay is armed with its hash. */
  readonly modelKey: string;
  name?: string;
  drive?: DriveInfo;
  vm?: VmHandle;
  link?: RelayLink;
  machine?: SessionMachine;
  /** Builds a machine over the current link with a token (a narrowed one, after the fact). */
  build?: (fence: FenceToken) => SessionMachine;
  /** Re-attaches after a loss so far. */
  lost: number;
  /** Whether this invocation attached once already: a VM is rotated only before that (between turns). */
  attached: boolean;
  /** Serializes acquires of this attach. */
  busy?: Promise<unknown>;
}

/** A VM's answer to an arm, checked: the reply and the token it was armed with. */
interface Armed {
  reply: Extract<ArmReply, { ok: true }>;
  token: string;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const BOOTING = new Set(['pending', 'stopping', 'snapshotting']);
/** Margin kept between the plan's cap and what an invocation needs. */
const CAP_MARGIN_MS = 60_000;
/** The plan caps on one VM session: Hobby's (the smallest, assumed until learned) and the largest any plan allows (P0: a create past it is refused with "`timeout` should be <= 1d"). */
export const HOBBY_CAP_MS = 45 * 60_000;
export const PLAN_CAP_MAX_MS = 24 * 60 * 60_000;

/** The cap a refused create names (400, "`timeout` should be <= 45m" / "<= 1d"), in ms; undefined for any other error. */
export function createCapOf(err: unknown): number | undefined {
  const e = err as { message?: unknown; response?: { status?: number }; status?: number };
  const status = e?.response?.status ?? e?.status;
  if (status !== undefined && status !== 400) return undefined;
  const m = /`?timeout`?\s+should\s+be\s+<=\s*(\d+(?:\.\d+)?)\s*(ms|s|m|min|h|d)\b/i.exec(String(e?.message ?? ''));
  if (!m) return undefined;
  const unit = m[2]!.toLowerCase();
  const scale = unit === 'ms' ? 1 : unit === 's' ? 1_000 : unit === 'h' ? 3_600_000 : unit === 'd' ? 86_400_000 : 60_000;
  return Math.round(Number(m[1]) * scale);
}

/**
 * Whether a Drive create was refused for its size against the plan's quota:
 * a 400 (or 403, a plan limit) whose message is the one P0.15 measured,
 * "must not exceed the Drive quota of N bytes". Anything else (another
 * validation error, a conflict) is not.
 */
export function driveQuotaRefusal(err: unknown): boolean {
  if (err instanceof ApiConflict) return false;
  const { status, message } = apiErrorOf(err);
  if (status !== undefined && status !== 400 && status !== 403) return false;
  return /must not exceed the Drive quota of \d+ bytes/i.test(message);
}

/** Whether an `extendTimeout` was refused for going past the plan's maximum (400 `sandbox_timeout_invalid`, "would exceed maximum execution timeout"). */
export function extendPastCap(err: unknown): boolean {
  const e = err as { message?: unknown; json?: { error?: { code?: string } }; code?: unknown };
  return e?.json?.error?.code === 'sandbox_timeout_invalid' || e?.code === 'sandbox_timeout_invalid' || /exceed maximum execution timeout/i.test(String(e?.message ?? ''));
}
/** The longest a Function invocation runs: the default deadline. */
const MAX_INVOCATION_MS = 800_000;
/** What a command leaves of the invocation for its outcome to be recorded (the kernel's end-of-drive margin). */
const INVOCATION_MARGIN_MS = 15_000;
/** Network errors of the Function's own (the Sandbox API unreachable for now): never a sign the VM is gone. */
const NETWORK_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ECONNABORTED', 'EPIPE', 'EAI_AGAIN', 'ENOTFOUND', 'ENETUNREACH', 'EHOSTUNREACH', 'UND_ERR_SOCKET', 'UND_ERR_CONNECT_TIMEOUT']);

/** Drop undefined fields, so a record is strict JSON (a document refuses `undefined`). */
function clean(record: MachineRecord): MachineRecord {
  return Object.fromEntries(Object.entries(record).filter(([, v]) => v !== undefined)) as MachineRecord;
}

/** A token, frozen. */
function frozen(fence: FenceToken): FenceToken {
  return Object.freeze({ count: fence.count, fenced: fence.fenced });
}

/**
 * The token of a later acquire of the same invocation: the same count, and
 * `fenced` only while both reads say so (a drive whose writes stopped being
 * fenced mid-invocation never regains it). A different count is a runtime
 * bug, refused.
 */
function narrowed(held: FenceToken | undefined, now: FenceToken | undefined): FenceToken | undefined {
  if (!held) return now ? frozen(now) : undefined;
  if (now && now.count !== held.count) throw new Error(`harness-sandbox: the runtime's fence changed within one invocation (${held.count}, then ${now.count}); a drive's token stays the same (docs/workspaces.md §5.4)`);
  const fenced = held.fenced && now?.fenced === true;
  return fenced === held.fenced ? held : frozen({ count: held.count, fenced });
}

/**
 * Why a control-plane command failed: `busy` when the Sandbox API refused or
 * failed for now (429, 5xx) or the Function's own network did (nothing says
 * the VM is gone), else `unanswered` (a timeout, an abort, a 4xx: the VM may
 * be gone, P0.5).
 */
function runFailure(err: unknown): 'busy' | 'unanswered' {
  const { status } = apiErrorOf(err);
  if (status === 429 || (status !== undefined && status >= 500)) return 'busy';
  for (let e: unknown = err, i = 0; e && i < 4; e = (e as { cause?: unknown }).cause, i++) {
    const code = (e as { code?: unknown }).code;
    if (typeof code === 'string' && NETWORK_CODES.has(code)) return 'busy';
  }
  return 'unanswered';
}

/** The base snapshot is gone (expired, or rebuilt and deleted elsewhere): the create's 404 or 400 naming it. */
function snapshotGone(err: unknown): boolean {
  const e = apiErrorOf(err);
  return (e.status === 404 || e.status === 400) && /snapshot/i.test(`${e.code ?? ''} ${e.message}`);
}

/**
 * The sandbox tier's `AcquireMachine` for one descriptor. Keep one per
 * descriptor (an agent): it holds each invocation's attach, keyed by the
 * invocation's `signal`, so a re-acquire after a loss re-attaches with the
 * token it read the first time; the invocation's links close when the signal
 * aborts.
 */
export function sandboxSession(options: SandboxSessionOptions): AcquireMachine {
  const { spec } = options;
  if (options.test) testGuard('sandboxSession({ test })');
  // The platform takes 5 tags; the factory's own watermark is one of them.
  if (options.tags && (Object.keys(options.tags).length > 4 || Object.hasOwn(options.tags, FENCE_TAG))) throw new TypeError(`harness-sandbox: sandboxSession({ tags }) takes at most 4 tags, none named ${FENCE_TAG} (the factory's own)`);
  const api = options.test?.api ?? vercelSandboxApi(spec.credentials);
  const nameOf = options.test?.name ?? sessionName;
  /** The plan cap: given, learned for the project, or assumed (the smallest) until an answer says. */
  const cap = { ms: options.planCapMs ?? HOBBY_CAP_MS, known: options.planCapMs !== undefined };
  const learnCap = (ms: number, why: string) => {
    if (cap.known && cap.ms === ms) return;
    cap.ms = ms;
    cap.known = true;
    log('learned the plan cap', { planCapMs: ms, why });
  };
  const busyMs = options.busyMs ?? 20_000;
  const maxLost = options.reattaches ?? 3;
  const probeMs = options.test?.probeMs ?? 1_000;
  const trusted = options.cliVersions ?? { claude: CLAUDE_CLI.version, codex: CODEX_CLI.version };
  const log = options.log ?? (() => {});
  /** `idleMs` within the plan cap (as known now). */
  const idle = () => Math.min(spec.idleMs, cap.ms);
  let clampLogged = false;
  const attaches = new WeakMap<AbortSignal, Map<string, Attach>>();

  return async (request: AcquireMachineRequest) => {
    if (request.signal.aborted) throw request.signal.reason ?? new Error('harness-sandbox: acquire aborted');
    let perInvocation = attaches.get(request.signal);
    if (!perInvocation) {
      const map = (perInvocation = new Map<string, Attach>());
      attaches.set(request.signal, map);
      // The invocation is over: its links close, whether or not the harness closed its machine.
      request.signal.addEventListener(
        'abort',
        () => {
          for (const a of map.values()) a.link?.close();
          attaches.delete(request.signal);
        },
        { once: true }
      );
    }
    let attach = perInvocation.get(request.sessionId);
    // The token: read in every acquire, inside the runtime's ownership barrier (the attach fact's body); the first read's count is kept.
    const fence = request.fence();
    if (!attach) {
      attach = { sessionId: request.sessionId, fence: fence ? frozen(fence) : undefined, notes: [], modelKey: mintModelKey(), lost: 0, attached: false };
      perInvocation.set(request.sessionId, attach);
    } else attach.fence = narrowed(attach.fence, fence);
    const a = attach;
    if (request.model) a.model = request.model;
    const run = (a.busy ?? Promise.resolve()).catch(() => {}).then(() => acquire(a, request));
    a.busy = run;
    return run;
  };

  /** What a VM said against this driver (a stale arm, a 4409 close, a watermark above its count), checked against the World before the drive stops for good. */
  function againstUs(a: Attach, why: string): Yield {
    log('the VM says a newer owner holds the session', { why, fence: a.fence?.count, fenced: a.fence?.fenced });
    return fencedToken(a.fence) ? new Yield('contended', 5) : new Yield('preempted');
  }

  async function acquire(a: Attach, request: AcquireMachineRequest): Promise<SessionMachine> {
    if (a.machine && a.link && !a.link.isLost()) {
      // The same link; a token that stopped being fenced gets a machine that refuses what it must not run.
      if (a.fence && a.machine.fence?.fenced !== a.fence.fenced && a.build) a.machine = a.build(a.fence);
      return a.machine;
    }
    if (a.link?.isLost()) {
      const loss = a.link.lossReason()!;
      if (/superseded/.test(loss.reason)) throw againstUs(a, 'a 4409 close');
      if (loss.protocol) throw new Error(`harness-sandbox: the relay broke the protocol (${loss.reason}); this delivery fails`);
      if (loss.reason !== 'closed by driver' && ++a.lost > maxLost) throw new SandboxLost(loss.reason);
      a.link = a.machine = a.build = undefined;
    }
    if (a.fence === undefined) {
      throw new Unavailable('harness-sandbox: sandbox() needs a World with the in-band writer fence: without a fencing token the relay could let a stale driver run commands in the session (docs/workspaces.md §5.4)', {
        hint: 'Use a World that supports the in-band fence (world-local, world-vercel), or virtual() for the harnesses that take it.',
      });
    }
    if (Number.isFinite(spec.budget.vmSeconds) && !request.record) {
      throw new TypeError('harness-sandbox: sandbox({ budget: { vmSeconds } }) needs a record to count VM time across invocations: pass AcquireMachineRequest.record (the kernel passes its hw.sandbox document)');
    }
    const fence = a.fence;
    const fenced = fencedToken(fence);
    let record = clean({ ...((await request.record?.read()) ?? {}) });
    const deadline = request.deadline ?? Date.now() + MAX_INVOCATION_MS;
    let next: MachineRecord = record;
    /** Write the record now (VM time is written as soon as it is granted, so a delivery that fails after a grant still counts it). */
    const save = async () => {
      next = clean(next);
      if (request.record && JSON.stringify(next) !== JSON.stringify(record)) {
        await request.record.write(next);
        record = next;
      }
    };

    // 1. The Drive: found by name, or created; its region and size win.
    const projectId = await api.projectId();
    if (!clampLogged && idle() < spec.idleMs && cap.known) {
      clampLogged = true;
      log('idleMs clamped to the plan cap', { idleMs: idle(), planCapMs: cap.ms });
    }
    const name = (a.name ??= nameOf(projectId, a.sessionId));
    const onDrive = (drive: DriveInfo) => {
      a.drive = drive;
      if (spec.region && spec.region !== drive.region) log('the Drive keeps its region', { wanted: spec.region, region: drive.region });
      if (spec.driveSize !== drive.maxSize) log('the Drive keeps its size', { wanted: spec.driveSize, size: drive.maxSize });
      const lost = next.driveId && next.driveId !== drive.driveId ? drive.driveId : next.filesLost;
      next = { ...next, sandboxName: name, driveName: name, driveId: drive.driveId, region: drive.region, driveSize: drive.maxSize, ...(lost !== undefined ? { filesLost: lost } : {}) };
      if (next.filesLost === drive.driveId && !a.notes.includes(FILES_LOST_NOTE)) {
        a.notes.push(FILES_LOST_NOTE);
        a.notesKey = drive.driveId;
      }
    };
    onDrive(a.drive ?? (await driveFor(name)));
    next = { ...next, image: imageLabel() };
    // A drill armed for a turn's start (D2, D3): on the session's running VM, before this invocation's first attach.
    if (fenced && !a.attached && request.drills) await attachDrill(a, request.drills);

    // 2. The VM, 3. the arm (the proof of ownership), then its rotation, policy and timeout, and 4. the sockets. A VM that turns out lost or anomalous is replaced once.
    let replaced = false;
    let rotated = false;
    for (;;) {
      if (request.signal.aborted) throw request.signal.reason ?? new Error('harness-sandbox: acquire aborted');
      const { vm, fresh } = await vmFor(a, deadline, fenced, () => budgetLeftMs(next), onDrive);
      a.vm = vm;
      if (fresh) {
        next = { ...next, vmSeconds: charged(next, vm.timeout) };
        await save();
      } else if (vm.fenceMark !== undefined && vm.fenceMark > fence.count) throw againstUs(a, `the VM's dh-fence is ${vm.fenceMark}`);
      if (!fresh && vm.startedAt + vm.timeout <= Date.now() && budgetLeftMs(next) <= 0) throw new SandboxBudgetExceeded(spec.budget.vmSeconds);
      const armed = await arm(a, vm, fresh);
      if (armed === 'replace' || armed === 'lost') {
        // Only an owner replaces a VM; an unfenced delivery leaves it to one, and wakes later.
        if (!fenced) throw new Yield('contended', 5);
        if (replaced) {
          if (fresh) await deleteOwned(vm, 'anomaly', fence.count).catch(() => {});
          // A fresh VM that never answered is no lost link: a failed delivery, with the kernel's backoff, so VMs that don't boot can't loop the queue.
          if (armed === 'lost' && fresh) throw new Error('harness-sandbox: a fresh session VM answered no command (twice in a row): this delivery fails');
          if (armed === 'lost') throw new SandboxLost('the VM did not answer a re-arm');
          throw new Unavailable("harness-sandbox: the session VM did not arm twice (a listener held the relay port, or the relay did not start): the image has no working relay", {
            hint: 'Boot from the base (prepareSandboxBase) or an image built by the same recipe.',
          });
        }
        replaced = true;
        await deleteOwned(vm, armed === 'lost' ? 'lost' : 'anomaly', fence.count);
        continue;
      }
      let ok: Armed;
      try {
        ok = { reply: checkReply(a, armed.reply, record, request), token: armed.token };
      } catch (err) {
        // A refusal after the relay accepted: a fresh VM of this driver's goes; a reused one is let go (its pending arm closed).
        if (fresh && fenced) await deleteOwned(vm, 'anomaly', fence.count).catch(() => {});
        else await abandon(vm, armed);
        throw err;
      }
      // The relay accepted the arm: no higher fence holds this VM, so the owner may rotate it, re-apply its policy and extend it.
      if (fenced && !fresh && !a.attached && !rotated && cap.ms - (Date.now() - vm.startedAt) < deadline - Date.now() + CAP_MARGIN_MS && !(await pastAssumedCap(vm, deadline))) {
        log('rotating the VM before the plan cap', { age: Date.now() - vm.startedAt, planCapMs: cap.ms });
        rotated = true;
        await deleteOwned(vm, 'rotate', fence.count);
        continue;
      }
      if (fenced) {
        const kept = await keepAlive(vm, fresh, next, deadline, fence.count);
        next = kept.record;
        await save();
        if (kept.error !== undefined) {
          // The policy (and the watermark with it) wasn't re-applied: the VM isn't used without them. A Sandbox API error of the moment is a retry later.
          if (runFailure(kept.error) === 'busy') throw new Yield('contended', 2);
          throw kept.error;
        }
      }
      const versions = Object.fromEntries(Object.entries(ok.reply.cliVersions).filter(([cli, v]) => trusted[cli] === v));
      if (Object.keys(versions).length) next = { ...next, versions: { ...(record.versions ?? {}), ...versions } };
      const link = await dial(a, vm, ok, request);
      a.link = link;
      await save();
      const layout = vm.layout;
      const root = layout?.workspace ?? WORKSPACE;
      const notes = [...a.notes];
      const notesKey = a.notesKey;
      a.build = (token) =>
        brandMachine(
          sessionMachine({
            link,
            sessionId: a.sessionId,
            fence: token,
            mutable: () => fencedToken(a.fence),
            root,
            ...(a.drive?.driveId ? { driveId: a.drive.driveId } : {}),
            dirs: vmDirs(layout?.cacheRoot ?? CACHE_ROOT),
            // A command ends before the invocation does (§5.6).
            workspaceOptions: { ...(spec.commandTimeoutMs ? { timeoutMs: spec.commandTimeoutMs } : {}), timeLeft: () => deadline - Date.now() - INVOCATION_MARGIN_MS },
            modelUrl: `http://127.0.0.1:${layout?.modelPort ?? MODEL_PORT}`,
            modelKey: a.modelKey,
            notes,
            ...(notesKey !== undefined ? { notesKey } : {}),
          })
        );
      a.machine = a.build(fence);
      a.attached = true;
      request.drills?.register(drillTarget(a));
      log('attached', { sandbox: name, vmSession: vm.sessionId, fresh, fence: fence.count, bootId: ok.reply.bootId });
      return a.machine;
    }
  }

  /**
   * D2 (`sandbox-new-vm`) and D3 (`sandbox-zombie`), claimed only when the
   * session has a running VM to act on (else the drill stays armed). A drill
   * that fails is said in the log; the attach goes on.
   */
  async function attachDrill(a: Attach, drills: AcquireDrills): Promise<void> {
    const vm = await api.getSandbox(a.name!);
    if (!vm || vm.status !== 'running') return;
    const drill = await drills.claim(ATTACH_DRILLS);
    if (!drill) return;
    try {
      if (drill.kind === 'sandbox-new-vm') {
        log('drill: deleting the VM between turns, keeping the Drive', { vmSession: vm.sessionId });
        await vm.delete();
        // Between turns: the old VM session lets go of the Drive before the next one boots.
        const until = Date.now() + busyMs;
        while (Date.now() < until && (await api.getDrive(a.name!))?.currentSessionId === vm.sessionId) await sleep(250);
      } else {
        log('drill: stopping the VM and leaving it holding the Drive', { vmSession: vm.sessionId });
        await vm.stop();
      }
    } catch (err) {
      log('drill: it did not run', { kind: drill.kind, error: apiErrorOf(err).message.slice(0, 200) });
    }
  }

  /** The drive's target for D1 (`sandbox-vm` at a tool: the VM stopped mid-step) and D4 (`relay-drop`), on this attach's current VM and link. */
  function drillTarget(a: Attach): DrillTarget {
    return {
      kinds: ['sandbox-vm', 'relay-drop'],
      fire: async (drill) => {
        if (!fencedToken(a.fence)) return;
        if (drill.kind === 'relay-drop') {
          const socket = drill.effect === 'model' ? 'model' : 'ctl';
          log('drill: dropping a socket of the relay link', { socket });
          a.link?.drop(socket);
          return;
        }
        const vm = a.vm ?? (await api.getSandbox(a.name!));
        log('drill: stopping the VM mid-step', { vmSession: vm?.sessionId, effect: drill.effect });
        await vm?.stop();
      },
    };
  }

  /** VM time left in the session's budget, ms. */
  function budgetLeftMs(record: MachineRecord): number {
    return (spec.budget.vmSeconds - (record.vmSeconds ?? 0)) * 1000;
  }

  /** `vmSeconds` with `ms` more granted (unchanged when there is no budget). */
  function charged(record: MachineRecord, ms: number): number | undefined {
    if (!Number.isFinite(spec.budget.vmSeconds)) return record.vmSeconds;
    return Math.round((record.vmSeconds ?? 0) + Math.max(ms, 0) / 1000);
  }

  function imageLabel(): string {
    const image = spec.image;
    return typeof image === 'string' ? image : image ? `snapshot:${image.snapshot}` : 'base';
  }

  async function driveFor(name: string): Promise<DriveInfo> {
    const found = await api.getDrive(name);
    if (found) return found;
    const region = spec.region ?? defaultRegion();
    const create = async (maxSize: number | undefined) => {
      try {
        return await api.createDrive({ name, region, ...(maxSize !== undefined ? { maxSize } : {}) });
      } catch (err) {
        // Another invocation created it first: adopt it, region and size included.
        if (err instanceof ApiConflict) {
          const won = await api.getDrive(name);
          if (won) return won;
        }
        throw err;
      }
    };
    try {
      return await create(spec.driveSize);
    } catch (err) {
      // A plan whose Drive quota is below the size asked for (Hobby: 1 GiB a Drive, 15 GB an account): the platform's default size instead, said once in the log.
      if (!driveQuotaRefusal(err)) throw err;
      const drive = await create(undefined);
      log("the Drive is the plan's default size: the plan's Drive quota is below driveSize", { wanted: spec.driveSize, size: drive.maxSize });
      return drive;
    }
  }

  /** The VM to arm: reused while running, created when there is none, replaced (owner only) when stopped, failed or aborted. */
  async function vmFor(a: Attach, deadline: number, fenced: boolean, budgetLeft: () => number, onDrive: (drive: DriveInfo) => void): Promise<{ vm: VmHandle; fresh: boolean }> {
    const until = Date.now() + busyMs;
    let wait = 250;
    let heldLogged = false;
    /** The base is asked for again (`refresh`) once, on the create after one found it gone. */
    let refreshBase: 'no' | 'next' | 'done' = 'no';
    const busy = async (why: string) => {
      if (Date.now() > until) {
        log('the sandbox stayed busy', { why });
        throw new Yield('contended', 5);
      }
      await sleep(wait);
      wait = Math.min(wait * 2, 2_000);
    };
    for (;;) {
      const vm = await api.getSandbox(a.name!);
      if (!vm) {
        // A fresh VM is armed only by a driver behind the World's fence: on a fresh VM it is the only guard (§5.4).
        if (!fenced) throw new Yield('contended', 5);
        try {
          const refresh = refreshBase === 'next';
          if (refresh) refreshBase = 'done';
          return { vm: await create(a, deadline, budgetLeft(), refresh), fresh: true };
        } catch (err) {
          if (err instanceof ApiConflict && err.kind === 'name') continue; // another driver just created it: reuse its VM
          if (err instanceof ApiConflict && err.kind === 'drive_attached') {
            if (!heldLogged) log('the Drive is still attached to an earlier VM session; waiting for it to detach');
            heldLogged = true;
            await busy('the Drive is still attached to an earlier VM session');
            continue;
          }
          if (err instanceof ApiConflict && err.kind === 'drive_changed') {
            // The Drive was deleted and made again since it was checked: check the new one (the files-lost note follows).
            onDrive(await driveFor(a.name!));
            continue;
          }
          if (refreshBase === 'no' && !spec.image && options.base && snapshotGone(err)) {
            log('the base snapshot is gone; preparing it again', { error: apiErrorOf(err).message });
            refreshBase = 'next';
            continue;
          }
          throw err;
        }
      }
      if (vm.status === 'running') return { vm, fresh: false };
      if (BOOTING.has(vm.status)) {
        await busy(`the VM is ${vm.status}`);
        continue;
      }
      // Stopped, failed or aborted: it can't be resumed (P0.4); only the owner deletes it.
      if (!fenced) throw new Yield('contended', 5);
      await deleteOwned(vm, 'stopped', a.fence!.count);
    }
  }

  async function create(a: Attach, deadline: number, budgetLeftMs: number, refreshBase: boolean): Promise<VmHandle> {
    if (budgetLeftMs < 1_000) throw new SandboxBudgetExceeded(spec.budget.vmSeconds);
    const region = a.drive!.region;
    const source: BootSource | undefined =
      typeof spec.image === 'string' ? { image: spec.image } : spec.image ? { snapshot: spec.image.snapshot } : await options.base?.(region, refreshBase ? { refresh: true } : undefined);
    if (!source) throw new Error('harness-sandbox: no image for the session VM: pass sandbox({ image }) or a base');
    // Within the cap as known: until an extension proved a larger one, the assumed (smallest) cap, so a create never fails on it (extending later is cheap, ~270 ms).
    const wanted = Math.max(deadline - Date.now(), 0) + spec.idleMs;
    const timeout = Math.min(wanted, cap.ms, budgetLeftMs);
    const make = (t: number) =>
      api.createSandbox({
        name: a.name!,
        region,
        source,
        drive: a.drive!.name,
        driveId: a.drive!.driveId,
        ports: [RELAY_PORT],
        vcpus: spec.vcpus,
        timeout: t,
        network: networkPolicy(spec.network),
        fenceMark: a.fence!.count,
        ...(options.tags ? { tags: options.tags } : {}),
      });
    try {
      return await make(timeout);
    } catch (err) {
      // A create past the plan's cap (a learned cap the plan no longer has): the cap its message names, else, for any 400, Hobby's (the smallest), once.
      if (timeout <= HOBBY_CAP_MS) throw err;
      const named = createCapOf(err);
      const learned = named ?? (apiErrorOf(err).status === 400 ? HOBBY_CAP_MS : undefined);
      if (learned === undefined || learned >= timeout) throw err;
      if (named !== undefined) {
        learnCap(named, 'a create past the cap was refused');
        return make(Math.min(timeout, named));
      }
      // A bare 400 may be about something else (a network policy, the source): the cap is learned only once the shorter create succeeds, and a retry that fails the same way leaves it as it was.
      let vm: VmHandle;
      try {
        vm = await make(Math.min(timeout, learned));
      } catch {
        throw err;
      }
      learnCap(learned, 'a create past 45 min was refused, and one within it made');
      return vm;
    }
  }

  /**
   * At the assumed cap, before rotating a VM: while the cap isn't known, ask
   * for the time this invocation needs. Granted: the plan allows more than
   * the assumed 45 min (it is 24 h), and the VM stays. Refused as past the
   * plan's maximum: the cap is 45 min, and the VM rotates.
   */
  async function pastAssumedCap(vm: VmHandle, deadline: number): Promise<boolean> {
    if (cap.known) return false;
    const left = vm.startedAt + vm.timeout - Date.now();
    const need = deadline - Date.now() + CAP_MARGIN_MS + idle();
    try {
      if (need > left) await vm.extendTimeout(need - left);
      learnCap(PLAN_CAP_MAX_MS, 'an extension past 45 min was granted');
      return true;
    } catch (err) {
      if (extendPastCap(err)) learnCap(HOBBY_CAP_MS, 'an extension past 45 min was refused');
      return false;
    }
  }

  /**
   * Delete a VM this driver owns (P0.4's guards): re-read it and delete only
   * if it is still the VM session observed, no newer owner's watermark is on
   * it, and (unless it is replaced for loss, an anomaly or the plan cap,
   * after the arm decided) it is no longer running. Callers hold a fenced
   * token.
   */
  async function deleteOwned(observed: VmHandle, why: 'stopped' | 'lost' | 'anomaly' | 'rotate', count: number): Promise<void> {
    const now = await api.getSandbox(observed.name);
    if (!now || now.sessionId !== observed.sessionId) return;
    if (why === 'stopped' && now.status === 'running') return;
    if (why !== 'stopped' && now.fenceMark !== undefined && now.fenceMark > count) throw new Yield('contended', 5);
    log('deleting the VM', { why, vmSession: observed.sessionId });
    await now.delete();
  }

  /**
   * After the arm: the policy, re-applied (with the `dh-fence` watermark),
   * and the timeout: the invocation's end plus `idleMs`, within the plan's
   * cap and the session's VM budget. Both API calls run together, and both
   * are awaited before either's failure is looked at (an unawaited rejection
   * would take the Function's process down). Returns the record with the
   * time granted, and the policy's failure, if any, for the caller to throw
   * once the grant is saved.
   */
  async function keepAlive(vm: VmHandle, fresh: boolean, record: MachineRecord, deadline: number, count: number): Promise<{ record: MachineRecord; error?: unknown }> {
    const now = Date.now();
    const age = now - vm.startedAt;
    const leftBudgetMs = budgetLeftMs(record);
    const left = vm.startedAt + vm.timeout - now;
    const want = Math.min(Math.max(deadline - now, 0) + idle(), cap.ms - age, Math.max(left, 0) + Math.max(leftBudgetMs, 0));
    let granted = 0;
    const policy = fresh ? Promise.resolve() : vm.setNetwork(networkPolicy(spec.network), count);
    const extend =
      want > left + 1_000
        ? vm.extendTimeout(want - left).then(
            () => void (granted = want - left),
            // At the plan's cap: the next attach rotates (the cap is smaller than this deployment assumed).
            (err) => {
              log('extendTimeout refused', { error: String((err as Error)?.message ?? err).slice(0, 200) });
              if (extendPastCap(err) && age + want > HOBBY_CAP_MS && cap.ms > HOBBY_CAP_MS) learnCap(HOBBY_CAP_MS, 'an extension past 45 min was refused');
            }
          )
        : Promise.resolve();
    const [p] = await Promise.allSettled([policy, extend]);
    const out = { ...record, vmSeconds: charged(record, granted) };
    return p.status === 'rejected' ? { record: out, error: p.reason ?? new Error('harness-sandbox: the network policy update failed') } : { record: out };
  }

  /** Arm the relay: `'lost'` if the VM didn't answer two probes, `'replace'` if it answered with no relay or a foreign listener. */
  async function arm(a: Attach, vm: VmHandle, fresh: boolean): Promise<{ reply: Extract<ArmReply, { ok: true }>; token: string } | 'lost' | 'replace'> {
    const token = mintToken();
    const args = ['arm', vm.layout?.config ?? RELAY_CONFIG, ...armArgs(armRequest(tokenHash(token), a.sessionId, a.fence, keyHash(a.modelKey)))];
    let out: { exitCode: number | null; stdout: string };
    try {
      out = await vm.run(RELAY_COMMAND, args, { sudo: true, timeoutMs: fresh ? 30_000 : 15_000 });
    } catch (err) {
      log('arm did not run', { error: apiErrorOf(err).message.slice(0, 200) });
      if (runFailure(err) === 'busy') throw new Yield('contended', 2);
      // One unanswered command is not a lost VM (P0.5's test is "no command answered"): ask once more, after a pause.
      await sleep(probeMs);
      try {
        await vm.run('/bin/true', [], { timeoutMs: 5_000 });
      } catch (again) {
        if (runFailure(again) === 'busy') throw new Yield('contended', 2);
        return 'lost';
      }
      // It answers: the relay was slow or restarting. Try again later.
      throw new Yield('contended', 2);
    }
    const reply = parseArmReply(out.stdout);
    if (!reply) return 'replace';
    if (!reply.ok) {
      if (reply.code === 'listener') return 'replace';
      throw refusal(a, reply);
    }
    return { reply, token };
  }

  /** A refused arm, as what the acquire throws. */
  function refusal(a: Attach, reply: Extract<ArmReply, { ok: false }>): Error {
    if (reply.code === 'stale') return againstUs(a, 'a stale arm');
    if (reply.code === 'contended') return new Yield('contended', 2);
    return new Error(`harness-sandbox: the relay refused the arm (${reply.code})`);
  }

  /**
   * Check an accepted arm (an untrusted reply): the session, the tier, the
   * model port, the CLI versions. Throws what the acquire throws; returns the
   * reply when this VM may be used.
   */
  function checkReply(a: Attach, reply: Extract<ArmReply, { ok: true }>, record: MachineRecord, request: AcquireMachineRequest): Extract<ArmReply, { ok: true }> {
    if (reply.sessionId !== a.sessionId) throw new Error('harness-sandbox: the VM is armed for another session');
    if (!options.test?.allowDegraded && (reply.tier !== 'privileged' || !reply.cgroup)) {
      throw new Unavailable(`harness-sandbox: the session VM's relay runs ${reply.tier}${reply.cgroup ? '' : ' without its cgroup'}: model commands would not be confined (docs/workspaces.md §5.4)`, {
        hint: 'Boot from the base (prepareSandboxBase) or an image built by the same recipe.',
      });
    }
    if (request.model && !reply.modelAuth && !options.test?.allowUnauthenticatedModel) {
      throw new Unavailable("harness-sandbox: the session VM's relay can't tell the CLI's model calls from any other process's (it was armed without the model key, or has no /proc to check the caller), so it can't serve the session's Gateway budget", {
        hint: 'Boot from the base (prepareSandboxBase) or an image built by the same recipe, with this release of the relay.',
      });
    }
    if (!reply.platformGuard) log('no owner-match guard on the platform listener (P0.20: the VM kernel has no nf_tables or xt_owner)');
    for (const [cli, recorded] of Object.entries(record.versions ?? {})) {
      const image = reply.cliVersions[cli];
      if (image && newer(recorded, image)) {
        throw new Unavailable(`The session was last run with ${cli} ${recorded}, newer than this image's ${image}: resuming would downgrade it`, {
          code: 'cli_version',
          hint: 'Deploy a version of the app whose sandbox image carries the same CLI or a newer one.',
        });
      }
    }
    return reply;
  }

  /** Let go of an arm the factory won't use: dial its sockets and close them, so the relay doesn't hold the pending arm against the next driver. */
  async function abandon(vm: VmHandle, armed: { reply: Extract<ArmReply, { ok: true }>; token: string }): Promise<void> {
    try {
      const link = await open(vm, armed.token, armed.reply.R, undefined);
      link.close();
    } catch {}
  }

  function open(vm: VmHandle, token: string, nonce: string, a: Attach | undefined): Promise<RelayLink> {
    const base = vm.domain(vm.layout?.port ?? RELAY_PORT).replace(/^http/, 'ws').replace(/\/+$/, '');
    const current: GatewayHandler = (req, signal) => (a?.model ? a.model(req, signal) : Promise.reject(Object.assign(new Error('internal'), { code: 'internal' })));
    return dialRelay({ ctlUrl: `${base}/ctl`, modelUrl: `${base}/model`, token, nonce, model: modelTunnel(current), onLost: (reason, protocol) => log('relay link lost', { reason, protocol }) });
  }

  /** Dial both sockets; a transient upgrade failure re-arms once with the same `T` (the relay replaces its own pending arm), the reply checked as the first was. */
  async function dial(a: Attach, vm: VmHandle, armed: Armed, request: AcquireMachineRequest): Promise<RelayLink> {
    try {
      return await open(vm, armed.token, armed.reply.R, a);
    } catch (err) {
      log('relay dial failed; re-arming once', { error: String((err as Error)?.message ?? err).slice(0, 200) });
      let out: { stdout: string };
      try {
        out = await vm.run(RELAY_COMMAND, ['arm', vm.layout?.config ?? RELAY_CONFIG, ...armArgs(armRequest(tokenHash(armed.token), a.sessionId, a.fence, keyHash(a.modelKey)))], { sudo: true, timeoutMs: 15_000 });
      } catch (again) {
        if (runFailure(again) === 'busy') throw new Yield('contended', 2);
        throw new SandboxLost(`the re-arm did not run (${apiErrorOf(again).message.slice(0, 120)})`);
      }
      const again = parseArmReply(out.stdout);
      if (!again) throw new SandboxLost('the relay did not answer the re-arm');
      if (!again.ok) throw again.code === 'listener' ? new SandboxLost('a listener took the relay port') : refusal(a, again);
      checkReply(a, again, {}, request);
      return open(vm, armed.token, again.R, a);
    }
  }
}

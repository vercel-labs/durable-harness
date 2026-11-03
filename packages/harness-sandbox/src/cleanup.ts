/**
 * Deleting what the sandbox tier made for a session (docs/workspaces.md
 * §5.7 "Cleanup"): its named sandbox and its Drive. **Nothing here boots a
 * VM**: it reads (`getSandbox`, which never resumes one, and the Drive
 * list), and changes only through a VM handle's `stop` and `delete` and
 * `deleteDrive`. The SDK's `runCommand`, `extendTimeout`, `update` and
 * `writeFiles` would boot a stopped sandbox to answer, so none is called.
 *
 * - `deleteSession(name)`: stop the VM if it runs, delete the sandbox, then
 *   delete the Drive, retried while the platform still has it attached
 *   (409 `drive_attached`: some seconds after a VM is deleted without a
 *   stop; after a stop, which waits for the VM to end, at once, measured).
 * - `cleanupIdle`: every session Drive of the project whose session was
 *   idle for `idleDays`, skipping a session whose VM is running (or still
 *   holds its Drive), whose log was written within that time, or that no
 *   session of the app's World names (unless `orphans`). Each delete
 *   checks again that the session is idle (`ifIdle`): one that resumed
 *   since the decision is kept, nothing of it stopped.
 * - `stopSession(name)`: stop the VM if it runs, and nothing else (what a
 *   closed session's sandbox gets by default: its sandbox and Drive kept).
 *
 * Every `stop`, `delete` and `deleteDrive` waits its turn on one limiter per
 * process: at most 20 a second, the platform's delete limit.
 */
import { ApiConflict, apiErrorOf, type CleanupApi, type VmStatus } from './api.ts';

/** A session's sandbox and Drive name (`names.ts`): `dh-` and 20 base32 characters. A probe's (`dh-probe-…`) or a base builder's (`dh-base-…`) never matches. */
export const SESSION_NAME = /^dh-[a-z2-7]{20}$/;

/** The VM statuses of a sandbox that is running, or about to: its session is not idle. */
const LIVE: ReadonlySet<VmStatus> = new Set(['pending', 'running', 'stopping', 'snapshotting']);

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * At most `perSecond` calls begin in any second, for every caller of the
 * returned function: each call waits for its slot, one every
 * `1000 / perSecond` ms.
 */
export function rateLimiter(perSecond = 20, clock: { now(): number; sleep(ms: number): Promise<void> } = { now: Date.now, sleep }): () => Promise<void> {
  if (!(perSecond > 0)) throw new TypeError('harness-sandbox: rateLimiter needs a positive rate');
  const gap = 1000 / perSecond;
  let next = 0;
  return async () => {
    const now = clock.now();
    const at = Math.max(now, next);
    next = at + gap;
    if (at > now) await clock.sleep(at - now);
  };
}

/** The process's limiter for the platform's deletes (20 a second). */
const DELETES = rateLimiter(20);

export interface DeleteOptions {
  api: CleanupApi;
  /** How long a Drive the platform still has attached is retried. Default 60 s. */
  detachMs?: number;
  /** Waits for a delete's turn. Default: the process's, 20 a second. */
  limit?: () => Promise<void>;
  /** Default `setTimeout`. */
  sleep?: (ms: number) => Promise<void>;
  log?: (message: string, fields?: Record<string, unknown>) => void;
}

/** What `deleteSession` did. */
export interface DeletedSession {
  name: string;
  /** The named sandbox: deleted, or there was none. */
  sandbox: 'deleted' | 'none';
  /** The Drive: deleted, or there was none. */
  drive: 'deleted' | 'none';
}

/** A refusal of the moment (429, 5xx): retried, for any of the deletes. */
function transient(err: unknown): boolean {
  const { status } = apiErrorOf(err);
  return status === 429 || (status !== undefined && status >= 500);
}

/** A Drive delete refused while a VM session still holds it (409 `drive_attached`, measured), or a refusal of the moment: retried. */
function retryable(err: unknown): boolean {
  if (err instanceof ApiConflict) return err.kind === 'drive_attached';
  return transient(err);
}

/** A VM that is ending, or saving its state: waited for before its sandbox is deleted (a stop or a delete then is refused). */
const SETTLING: ReadonlySet<VmStatus> = new Set(['stopping', 'snapshotting']);

/**
 * `deleteSession({ ifIdle })` found the session in use (its VM running or
 * booting, a VM session newer than the one the caller saw, or its Drive
 * held): nothing of it was stopped, and its Drive is still there.
 */
export class SessionInUse extends Error {
  constructor(readonly sessionName: string) {
    super(`harness-sandbox: ${sessionName} is in use; it was kept`);
    this.name = 'SessionInUse';
  }
}

/** `deleteSession`'s idle-only mode (`cleanupIdle`'s): what the caller saw when it decided the session was idle. */
export interface IfIdle {
  /** The `startedAt` of the VM session the caller saw, or `undefined` when it saw no VM. A different one (or a VM where it saw none) is a session that resumed since. */
  startedAt: number | undefined;
}

/**
 * Delete the named sandbox and the Drive of one session (`name`, from
 * `sessionName`), without booting a VM: `stop` if it is running (a VM that
 * is stopping or snapshotting is waited for), `delete`, then the Drive,
 * retried for up to `detachMs` while it is still attached. A 429 or 5xx is
 * retried at each step. A sandbox or Drive that isn't there is fine.
 *
 * With `ifIdle`, it deletes only an idle session: when the VM it reads now
 * runs or boots, or is not the VM session the caller saw, or the Drive is
 * still held when its delete is tried, it throws `SessionInUse` and stops
 * nothing (`cleanupIdle`, which decided from an earlier read). Without it
 * the session is over: a running VM is stopped, whatever it was doing.
 */
export async function deleteSession(name: string, options: DeleteOptions & { ifIdle?: IfIdle }): Promise<DeletedSession> {
  if (!SESSION_NAME.test(name)) throw new TypeError(`harness-sandbox: ${JSON.stringify(name)} is not a session's sandbox name`);
  const { api, ifIdle } = options;
  const limit = options.limit ?? DELETES;
  const wait = options.sleep ?? sleep;
  const gone = (err: unknown) => apiErrorOf(err).status === 404;
  const deadline = Date.now() + (options.detachMs ?? 60_000);
  /** One call, retried on a refusal `again` allows until the deadline, each try on the limiter. */
  const retried = async <T>(what: string, call: () => Promise<T>, again: (err: unknown) => boolean = transient): Promise<T> => {
    for (let pause = 1000; ; pause = Math.min(pause * 2, 5000)) {
      await limit();
      try {
        return await call();
      } catch (err) {
        if (!again(err) || Date.now() + pause > deadline) throw err;
        options.log?.(`harness-sandbox: ${what} was refused for now; retrying`, { name, ...apiErrorOf(err) });
        await wait(pause);
      }
    }
  };
  let sandbox: DeletedSession['sandbox'] = 'none';
  let vm = await api.getSandbox(name);
  if (ifIdle && (vm ? LIVE.has(vm.status) || vm.startedAt !== ifIdle.startedAt : ifIdle.startedAt !== undefined)) throw new SessionInUse(name);
  if (vm && SETTLING.has(vm.status)) {
    // Ending or saving: wait for it to stop (never stopped mid-snapshot).
    for (let pause = 1000; vm && SETTLING.has(vm.status); pause = Math.min(pause * 2, 5000)) {
      if (Date.now() + pause > deadline) throw new Error(`harness-sandbox: ${name}'s VM is still ${vm.status}`);
      await wait(pause);
      vm = await api.getSandbox(name);
    }
  }
  if (vm) {
    if (LIVE.has(vm.status)) {
      const running = vm;
      await retried('the stop', () =>
        running.stop().catch((err) => {
          if (!gone(err)) throw err;
        })
      );
    }
    const doomed = vm;
    await retried('the sandbox delete', () =>
      doomed.delete().then(
        () => void (sandbox = 'deleted'),
        (err) => {
          if (!gone(err)) throw err;
        }
      )
    );
  }
  try {
    // Idle-only: a Drive still held is a VM session this read didn't see, never waited out.
    const drive: DeletedSession['drive'] = (await retried('the Drive delete', () => api.deleteDrive(name), ifIdle ? transient : retryable)) ? 'deleted' : 'none';
    options.log?.('harness-sandbox: deleted a session sandbox', { name, sandbox, drive });
    return { name, sandbox, drive };
  } catch (err) {
    if (ifIdle && err instanceof ApiConflict && err.kind === 'drive_attached') throw new SessionInUse(name);
    throw err;
  }
}

/** What `stopSession` did: the session's VM stopped, or there was none running. */
export interface StoppedSession {
  name: string;
  sandbox: 'stopped' | 'none';
}

/**
 * Stop a session's VM (`name`, from `sessionName`) if it is running or
 * booting, without booting one and without deleting anything: its sandbox
 * and its Drive stay, so a later `deleteSession` (or `cleanupIdle`) still
 * finds them. A VM that is already stopping or saving its state, or
 * stopped, or not there, is left as it is. A 429 or 5xx is retried, on the
 * shared limiter, for up to `detachMs`. Calling it again is harmless: the
 * second call finds nothing running.
 */
export async function stopSession(name: string, options: DeleteOptions): Promise<StoppedSession> {
  if (!SESSION_NAME.test(name)) throw new TypeError(`harness-sandbox: ${JSON.stringify(name)} is not a session's sandbox name`);
  const limit = options.limit ?? DELETES;
  const wait = options.sleep ?? sleep;
  const deadline = Date.now() + (options.detachMs ?? 60_000);
  const vm = await options.api.getSandbox(name);
  if (!vm || (vm.status !== 'pending' && vm.status !== 'running')) return { name, sandbox: 'none' };
  for (let pause = 1000; ; pause = Math.min(pause * 2, 5000)) {
    await limit();
    try {
      await vm.stop();
      break;
    } catch (err) {
      // Gone in between: nothing left to stop.
      if (apiErrorOf(err).status === 404) return { name, sandbox: 'none' };
      if (!transient(err) || Date.now() + pause > deadline) throw err;
      options.log?.('harness-sandbox: the stop was refused for now; retrying', { name, ...apiErrorOf(err) });
      await wait(pause);
    }
  }
  options.log?.('harness-sandbox: stopped a session sandbox', { name });
  return { name, sandbox: 'stopped' };
}

export interface CleanupOptions extends DeleteOptions {
  /** Delete only sessions idle this many days (> 0). */
  idleDays: number;
  /**
   * When the session whose sandbox is `name` last wrote its log (epoch ms),
   * or `undefined` when no session the app's World knows has that name.
   */
  lastActivity(name: string): Promise<number | undefined>;
  /**
   * Also delete a Drive no session of the World names (its session deleted,
   * or older than what the World lists), once its VM, if any, is idle too.
   * Default false: kept, and reported. Every app and environment that
   * shares the project (production, previews, a laptop's `vercel link`)
   * names its sessions from the same project, each in its own World: a
   * session of another one is an orphan to this one.
   */
  orphans?: boolean;
  /** Stop looking at sessions at this time (epoch ms): the rest are `unreached`. Default none. */
  deadline?: number;
  /** Look at this many sessions at most: the rest are `unreached`. Default all. */
  maxSessions?: number;
  /**
   * Begin at the first session name at or after this one, wrapping around
   * (a previous result's `resumeAt`). Default: a random point, so that runs
   * stopped by `deadline` or `maxSessions` cover every session over time.
   */
  resumeAt?: string;
  /** Default `Date.now`. */
  now?: () => number;
}

/** Why `cleanupIdle` kept a session's sandbox. */
export type CleanupSkip = 'running' | 'recent' | 'unknown' | 'failed';

export interface CleanupResult {
  deleted: DeletedSession[];
  kept: { name: string; reason: CleanupSkip; error?: string }[];
  /** Sessions not looked at (the `deadline` or `maxSessions` came first), in the order they would have been. */
  unreached: string[];
  /** Where the next run should begin (`resumeAt`): the first of `unreached`, when there is one. */
  resumeAt?: string;
}

/**
 * Delete the sandbox and Drive of every session of the project idle for
 * `idleDays` (opt-in, from a cron route: deleting a Drive breaks "files are
 * durable" for a session resumed later). Sessions are taken from the
 * project's session Drives; each is kept when its VM is running or holds
 * the Drive, when its log or its VM was active within `idleDays`, or when
 * no session of the World names it (unless `orphans`). The decision is
 * checked again at the delete (`deleteSession({ ifIdle })`): a session that
 * resumed in between is kept as `running`, nothing of it stopped. One at a
 * time, each delete on the shared limiter, until `deadline` or
 * `maxSessions`. A failure keeps that session and goes on.
 */
export async function cleanupIdle(options: CleanupOptions): Promise<CleanupResult> {
  if (typeof options.idleDays !== 'number' || !(options.idleDays > 0)) throw new TypeError('harness-sandbox: idleDays must be a number of days above 0');
  if (options.maxSessions !== undefined && !(Number.isInteger(options.maxSessions) && options.maxSessions > 0)) throw new TypeError('harness-sandbox: maxSessions must be a positive integer');
  const now = options.now ?? Date.now;
  const idleMs = options.idleDays * 86_400_000;
  // The whole list first: deleting while paging could skip a page.
  const drives = [];
  for await (const d of options.api.listDrives('dh-')) if (SESSION_NAME.test(d.name)) drives.push(d);
  drives.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  // Rotate: from `resumeAt` (or a random point), wrapping around.
  let from = options.resumeAt !== undefined ? drives.findIndex((d) => d.name >= options.resumeAt!) : Math.floor(Math.random() * drives.length);
  if (from < 0) from = 0;
  const order = [...drives.slice(from), ...drives.slice(0, from)];
  const result: CleanupResult = { deleted: [], kept: [], unreached: [] };
  const max = options.maxSessions ?? Infinity;
  for (const [i, drive] of order.entries()) {
    if (i >= max || (options.deadline !== undefined && now() >= options.deadline)) {
      result.unreached = order.slice(i).map((d) => d.name);
      result.resumeAt = result.unreached[0];
      break;
    }
    const { name } = drive;
    try {
      const vm = await options.api.getSandbox(name);
      // A VM that runs (or boots), or a Drive held by a VM session this name doesn't show: in use.
      if ((vm && LIVE.has(vm.status)) || (!vm && drive.currentSessionId)) {
        result.kept.push({ name, reason: 'running' });
        continue;
      }
      const logged = await options.lastActivity(name);
      if (logged === undefined && !options.orphans) {
        result.kept.push({ name, reason: 'unknown' });
        continue;
      }
      const last = Math.max(logged ?? 0, vm ? vm.startedAt : 0);
      if (now() - last < idleMs) {
        result.kept.push({ name, reason: 'recent' });
        continue;
      }
      result.deleted.push(await deleteSession(name, { ...options, ifIdle: { startedAt: vm?.startedAt } }));
    } catch (err) {
      if (err instanceof SessionInUse) result.kept.push({ name, reason: 'running' });
      else result.kept.push({ name, reason: 'failed', error: apiErrorOf(err).message });
    }
  }
  return result;
}

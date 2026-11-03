/**
 * The `SessionMachine` over one relay link (docs/workspaces.md §5.4), before it
 * is branded: what the session factory (A3, `session.ts`) and the stand-in
 * (`localMachine`) both build, so the rules below hold on both and A3 tests
 * them once.
 *
 * - **The fence.** Read by the caller once per attach (inside the
 *   `sandbox.attach` fact's body) and kept frozen as `machine.fence`. While it
 *   is missing or `fenced: false` (or the caller's `mutable` says the token
 *   has narrowed since), the machine refuses `files.upload` (`FILES`),
 *   `files.wipe` (`WIPE`) and `spawn` (`SPAWN`): the relay can't tell a
 *   mutating spawn from a read, so every spawn counts as mutating. The
 *   workspace (`RemoteMachine`, `env.ts`) refuses a mutating `ENV_CALL` the
 *   same way; arming a fresh VM is A3's.
 * - **The env.** Every `spawn`'s `env` passes `checkSpawnEnv` first (§3.1 and
 *   the `process.env` equality rule): a `TypeError` naming the key. The model
 *   key's name (`PLACEHOLDER_NAMES`) passes only with this attach's key.
 * - **Loss.** `signal` is the link's: aborted once the link is lost. A
 *   `spawn`'s own `signal` aborting kills the session's processes.
 * - **Frozen state.** The brand freezes the machine, so everything that
 *   changes (the link, its loss) is read through closures and getters, never
 *   stored in a property.
 */
import type { CacheDir, CacheFile, FenceToken, MachineNetwork, MachineSpawn, MachineTail, SessionMachine } from 'harness-world-process';
import { checkSpawnEnv } from './env-check.ts';
import { RemoteMachine, fencedToken, type RemoteMachineOptions } from './env.ts';
import type { RelayLink } from './relay/dial.ts';
import { ustar } from './tar.ts';

export interface MachineParts {
  link: RelayLink;
  sessionId: string;
  /** The token the relay was armed with, read once per attach. */
  fence: FenceToken | undefined;
  /** `/workspace`. */
  root: string;
  dirs: Readonly<Record<CacheDir, string>>;
  /**
   * The workspace's command limit, invocation clamp and flush mode
   * (`RemoteMachineOptions`). The workspace itself is always a `RemoteMachine`
   * over `link` holding `fence`, never one the caller supplies, so the fence
   * gate on mutating `ENV_CALL`s can't be swapped out.
   */
  workspaceOptions?: Pick<RemoteMachineOptions, 'timeoutMs' | 'timeLeft' | 'flush' | 'callTimeoutMs'>;
  /** The model port in the VM, as the CLI's base URL. */
  modelUrl: string;
  /** The attach's model key `P` (§5.5): the relay admits a model request only with it. */
  modelKey: string;
  /** What the model must be told about this attach (a lost Drive). */
  notes?: readonly string[];
  /** What `notes` describe (the new Drive's id), for the acknowledgement. */
  notesKey?: string;
  /** The network policy the attach applied, as `networkSummary` reduces it (`SessionMachine.network`). */
  network?: MachineNetwork;
  /**
   * Whether the attach may still mutate the VM, asked at every `spawn`,
   * `files.upload` and `files.wipe` on top of `fence` (the session factory's:
   * the invocation's token as later acquires narrowed it, so a machine handed
   * out before the drive's `fenced` turned false refuses too). Default: `fence`
   * alone.
   */
  mutable?: () => boolean;
}

export { fencedToken };

function unfenced(op: string): Error {
  return new Error(`harness-sandbox: ${op} refused: this attach holds no fenced token (docs/workspaces.md §5.4: no WIPE, FILES or SPAWN without one)`);
}

/** Build the unbranded `SessionMachine` (the caller brands it through `session.ts`). */
export function sessionMachine(parts: MachineParts): Omit<SessionMachine, 'brand'> {
  const { link } = parts;
  const fence = parts.fence ? Object.freeze({ count: parts.fence.count, fenced: parts.fence.fenced }) : undefined;
  const fenced = fencedToken(fence);
  const still = parts.mutable;
  const mutable = () => fenced && (still ? still() : true);
  return {
    sessionId: parts.sessionId,
    fence,
    root: parts.root,
    workspace: new RemoteMachine({ link, root: parts.root, fence, ...parts.workspaceOptions, ...(still ? { mutable: still } : {}) }),
    dirs: Object.freeze({ ...parts.dirs }),
    signal: link.signal,
    modelUrl: parts.modelUrl,
    modelKey: parts.modelKey,
    notes: Object.freeze([...(parts.notes ?? [])]),
    ...(parts.notes?.length && parts.notesKey !== undefined ? { notesKey: parts.notesKey } : {}),
    ...(parts.network !== undefined ? { network: parts.network } : {}),
    async spawn(options: MachineSpawn) {
      if (!mutable()) throw unfenced('spawn');
      checkSpawnEnv(options.env, process.env, parts.modelKey);
      if (options.signal?.aborted) throw options.signal.reason ?? new Error('harness-sandbox: spawn aborted');
      const proc = await link.spawn({ command: options.command, args: options.args, cwd: options.cwd ?? parts.root, env: options.env, ...(options.isolatedCommands ? { isolatedCommands: true } : {}) });
      const signal = options.signal;
      if (signal) {
        const onAbort = () => link.killSession('SIGKILL');
        if (signal.aborted) onAbort();
        else {
          signal.addEventListener('abort', onAbort, { once: true });
          void proc.exited.then(() => signal.removeEventListener('abort', onAbort));
        }
      }
      return proc;
    },
    files: {
      async upload(dir: CacheDir, files: readonly CacheFile[]) {
        if (!mutable()) throw unfenced('files.upload');
        await link.files(dir, ustar(files));
      },
      async wipe() {
        if (!mutable()) throw unfenced('files.wipe');
        await link.wipe();
      },
    },
    async tail(dir: CacheDir, path: string, options?: { offset?: number }): Promise<MachineTail> {
      const t = link.tail(dir, path, options);
      return {
        get offset() {
          return t.offset;
        },
        read: () => t.read(),
        sync: () => t.sync(),
        close: () => t.close(),
      };
    },
    fsync: () => link.fsync(),
    async close() {
      link.killSession('SIGKILL');
      link.close();
    },
  };
}

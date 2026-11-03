/**
 * The part of the Vercel Sandbox API the session factory uses (docs/workspaces.md
 * §5.7), as one small interface, so the lifecycle is written once and tested on
 * the stand-in (`test/fake-api.ts`, local relays over temp dirs) as well as on
 * the platform (`vercelSandboxApi`, `@vercel/sandbox` 3.7.0).
 *
 * Only what the probes measured is relied on (docs/sandbox-probes.md):
 *
 * - a stopped non-persistent sandbox can't be resumed (P0.4): `get`, then
 *   create, or an owner's delete and create;
 * - a Drive held by another VM session answers 409 `drive_attached` for 5-8 s
 *   after that session goes (P0.5);
 * - a VM that crashed inside stays `running` until its own timeout (P0.5), so
 *   whether it answers is asked with a command, not read from its status;
 * - `runCommand`, `extendTimeout` and `writeFiles` resume a stopped persistent
 *   sandbox; on a non-persistent one they fail, so cleanup never calls them.
 *
 * Besides the VM, the named sandbox carries one tag of the factory's own,
 * `dh-fence`: the highest fence count an owner armed the VM with (set at
 * create and with the policy at every warm attach). It is the watermark a
 * driver reads before it arms, rotates or replaces a VM, so a driver whose
 * fence is stale (but still reads `fenced`) never deletes the newer owner's
 * VM, and a VM it can't ask (lost) still says who armed it last.
 */
import type { NetworkPolicy } from '@vercel/sandbox';
import { resolveProjectId } from './names.ts';

export { resolveProjectId };

/** A Drive, as the session factory needs it. */
export interface DriveInfo {
  readonly name: string;
  readonly driveId: string;
  readonly region: string;
  /** Bytes. */
  readonly maxSize: number;
  /** The VM session the Drive is attached to, if any. */
  readonly currentSessionId?: string;
}

/** The status of a VM session, as the platform reports it. */
export type VmStatus = 'pending' | 'running' | 'stopping' | 'stopped' | 'failed' | 'aborted' | 'snapshotting';

/** What a command run through the control plane returned. */
export interface RunResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
}

/** Where the relay and the cache dirs are in a VM, when not where the base puts them (`vm.ts`): the stand-in's temp dirs and ports. */
export interface VmLayout {
  port: number;
  modelPort: number;
  workspace: string;
  cacheRoot: string;
  config: string;
}

/** A named sandbox and its current VM session. */
export interface VmHandle {
  readonly name: string;
  /** The current VM session. */
  readonly sessionId: string;
  readonly status: VmStatus;
  readonly region: string;
  /** When the current VM session started (epoch ms). */
  readonly startedAt: number;
  /** The current VM session's timeout, ms from `startedAt`. */
  readonly timeout: number;
  /** The `dh-fence` tag: the highest fence count an owner armed this sandbox with, if any did. */
  readonly fenceMark?: number;
  /** The public URL of an exposed port (`https://…`). Throws for a port the sandbox doesn't expose. */
  domain(port: number): string;
  /** Run a command through the control plane (authenticated with the Function's credential). */
  run(command: string, args: string[], options?: { sudo?: boolean; timeoutMs?: number; signal?: AbortSignal }): Promise<RunResult>;
  /** Extend the VM session's timeout by `ms`, up to the plan's cap (past it the platform refuses). */
  extendTimeout(ms: number): Promise<void>;
  /**
   * Apply a network policy (`update({ networkPolicy })`, never the deprecated
   * `updateNetworkPolicy`), and in the same call raise the `dh-fence` tag to
   * `fenceMark` (never lower it).
   */
  setNetwork(policy: SandboxNetworkPolicy, fenceMark?: number): Promise<void>;
  stop(): Promise<void>;
  delete(): Promise<void>;
  /** Only the stand-in sets it. */
  readonly layout?: VmLayout;
}

/** The network policy the factory applies: `@vercel/sandbox`'s `NetworkPolicy`, rules (header transforms, forwards, responses) and subnets included. */
export type SandboxNetworkPolicy = NetworkPolicy;

/** What the VM boots from. */
export type BootSource = { image: string } | { snapshot: string };

export interface CreateSandbox {
  name: string;
  region: string;
  source?: BootSource;
  /** Mounted read-write at `/workspace`. */
  drive: string;
  /** The Drive's id as the factory checked it: a Drive fetched for the mount with another id (deleted and made again in between) is `ApiConflict('drive_changed')`. */
  driveId: string;
  /** The fence count of the driver that creates it: the `dh-fence` tag. */
  fenceMark?: number;
  ports: number[];
  vcpus: number;
  /** ms. */
  timeout: number;
  network: SandboxNetworkPolicy;
  tags?: Record<string, string>;
}

/** An API call refused for a reason the lifecycle handles. */
export class ApiConflict extends Error {
  constructor(
    /** `name`: the sandbox name is taken; `drive_attached`: the Drive is held by another VM session; `exists`: the Drive exists; `drive_changed`: the Drive is not the one checked (another id under its name). */
    readonly kind: 'name' | 'drive_attached' | 'exists' | 'drive_changed',
    message: string
  ) {
    super(message);
    this.name = 'ApiConflict';
  }
}

export interface SandboxApi {
  /** The project the names are derived from. */
  projectId(): Promise<string>;
  getDrive(name: string): Promise<DriveInfo | undefined>;
  /** Throws `ApiConflict('exists')` when another invocation created it first. */
  /** Create a Drive; without `maxSize`, the platform's default size for the plan (1 GiB on Hobby). */
  createDrive(options: { name: string; region: string; maxSize?: number }): Promise<DriveInfo>;
  getSandbox(name: string): Promise<VmHandle | undefined>;
  /** Throws `ApiConflict('name')` or `ApiConflict('drive_attached')`. */
  createSandbox(options: CreateSandbox): Promise<VmHandle>;
}

/**
 * What deleting a session's sandbox and Drive takes (`cleanup.ts`): reads,
 * and the one delete the VM's handle doesn't have. Nothing here boots a VM:
 * `getSandbox` never resumes one, and a `VmHandle`'s `stop` and `delete`
 * act on the named sandbox without a session (P0.11: `runCommand`,
 * `extendTimeout`, `update` and `writeFiles` would boot a stopped one).
 */
export interface CleanupApi {
  getSandbox(name: string): Promise<VmHandle | undefined>;
  /** Delete a Drive by name: `false` when there is none. `ApiConflict('drive_attached')` while a VM session still holds it (a few seconds after a VM is deleted without a stop). */
  deleteDrive(name: string): Promise<boolean>;
  /** Every Drive of the project whose name starts with `prefix`, every page. */
  listDrives(prefix: string): AsyncIterable<DriveInfo>;
}

/** Credentials for a driver outside Vercel (`sandbox({ credentials })`); never logged, never sent to the VM. */
export interface ApiCredentials {
  token: string;
  teamId: string;
  projectId: string;
}

/** An SDK error's status and code, never its body or headers. */
export function apiErrorOf(err: unknown): { status?: number; code?: string; message: string } {
  const e = err as { message?: unknown; response?: { status?: unknown }; json?: { error?: { code?: unknown } } };
  const status = typeof e?.response?.status === 'number' ? e.response.status : undefined;
  const code = typeof e?.json?.error?.code === 'string' ? e.json.error.code : undefined;
  return { ...(status !== undefined ? { status } : {}), ...(code !== undefined ? { code } : {}), message: String(e?.message ?? err).slice(0, 300) };
}

type Sdk = typeof import('@vercel/sandbox');
type SdkSandbox = InstanceType<Sdk['Sandbox']>;

const STATUSES = new Set<VmStatus>(['pending', 'running', 'stopping', 'stopped', 'failed', 'aborted', 'snapshotting']);
/** The tag that carries the fence watermark. */
export const FENCE_TAG = 'dh-fence';

/** The watermark a tag map carries, or undefined. */
export function fenceMarkOf(tags: Readonly<Record<string, string>> | undefined): number | undefined {
  const v = tags?.[FENCE_TAG];
  if (v === undefined || !/^\d{1,15}$/.test(v)) return undefined;
  return Number(v);
}

/** `@vercel/sandbox` over the interface. The SDK is imported on first use, so a session that never acquires a sandbox never loads it. */
export function vercelSandboxApi(credentials?: ApiCredentials): SandboxApi & CleanupApi {
  let sdk: Promise<Sdk> | undefined;
  const load = () => (sdk ??= import('@vercel/sandbox'));
  const creds = credentials ? { token: credentials.token, teamId: credentials.teamId, projectId: credentials.projectId } : {};
  const drive = (d: InstanceType<Sdk['Drive']>): DriveInfo => ({ name: d.name, driveId: d.driveId, region: d.region, maxSize: d.maxSize, ...(d.currentSessionId ? { currentSessionId: d.currentSessionId } : {}) });
  const handle = (sb: SdkSandbox): VmHandle => {
    const session = sb.currentSession();
    // A status this SDK doesn't name (a state added later) is waited on as booting, never deleted as failed.
    const status = STATUSES.has(sb.status as VmStatus) ? (sb.status as VmStatus) : 'pending';
    const fenceMark = fenceMarkOf(sb.tags);
    return {
      name: sb.name,
      sessionId: session.sessionId,
      status,
      region: sb.region,
      startedAt: (session.startedAt ?? session.createdAt).getTime(),
      timeout: session.timeout,
      ...(fenceMark !== undefined ? { fenceMark } : {}),
      domain: (port) => sb.domain(port),
      async run(command, args, options = {}) {
        const c = await sb.runCommand({ cmd: command, args, sudo: options.sudo ?? false, ...(options.timeoutMs ? { timeoutMs: options.timeoutMs } : {}), ...(options.signal ? { signal: options.signal } : {}) });
        return { exitCode: c.exitCode, stdout: await c.stdout(), stderr: await c.stderr() };
      },
      extendTimeout: (ms) => sb.extendTimeout(ms),
      async setNetwork(policy, mark) {
        const tags = sb.tags ?? {};
        const raise = mark !== undefined && mark > (fenceMarkOf(tags) ?? -1);
        await sb.update({ networkPolicy: policy, ...(raise ? { tags: { ...tags, [FENCE_TAG]: String(mark) } } : {}) });
      },
      async stop() {
        await sb.stop();
      },
      delete: () => sb.delete(),
    };
  };
  return {
    projectId: async () => resolveProjectId(credentials),
    async getDrive(name) {
      const { Drive } = await load();
      try {
        return drive(await Drive.get({ name, ...creds }));
      } catch (err) {
        if (apiErrorOf(err).status === 404) return undefined;
        throw err;
      }
    },
    async createDrive({ name, region, maxSize }) {
      const { Drive } = await load();
      try {
        return drive(await Drive.getOrCreate({ name, region, ...(maxSize !== undefined ? { maxSize } : {}), ...creds }));
      } catch (err) {
        const e = apiErrorOf(err);
        if (e.status === 409) throw new ApiConflict('exists', e.message);
        throw err;
      }
    },
    async getSandbox(name) {
      const { Sandbox } = await load();
      try {
        return handle(await Sandbox.get({ name, ...creds }));
      } catch (err) {
        if (apiErrorOf(err).status === 404) return undefined;
        throw err;
      }
    },
    async deleteDrive(name) {
      const { Drive } = await load();
      let found;
      try {
        found = await Drive.get({ name, ...creds });
      } catch (err) {
        if (apiErrorOf(err).status === 404) return false;
        throw err;
      }
      try {
        await found.delete();
        return true;
      } catch (err) {
        const e = apiErrorOf(err);
        if (e.status === 404) return false;
        // Measured (A7): 409 `drive_attached` while a VM session holds it, and for some seconds after that VM is deleted without a stop.
        if (e.status === 409 && e.code === 'drive_attached') throw new ApiConflict('drive_attached', e.message);
        throw err;
      }
    },
    async *listDrives(prefix) {
      const { Drive } = await load();
      for await (const d of await Drive.list({ namePrefix: prefix, sortBy: 'name', limit: 50, ...creds })) yield drive(d);
    },
    async createSandbox(o) {
      const { Sandbox, Drive } = await load();
      const mounted = await Drive.get({ name: o.drive, ...creds });
      if (mounted.driveId !== o.driveId) throw new ApiConflict('drive_changed', 'the Drive under this name is not the one checked');
      const tags = { ...(o.tags ?? {}), ...(o.fenceMark !== undefined ? { [FENCE_TAG]: String(o.fenceMark) } : {}) };
      const source = o.source && 'snapshot' in o.source ? { source: { type: 'snapshot' as const, snapshotId: o.source.snapshot } } : o.source ? { image: o.source.image } : {};
      try {
        const sb = await Sandbox.create({
          name: o.name,
          region: o.region,
          persistent: false,
          mounts: { '/workspace': mounted },
          ports: o.ports,
          resources: { vcpus: o.vcpus },
          timeout: o.timeout,
          networkPolicy: o.network,
          ...(Object.keys(tags).length ? { tags } : {}),
          ...source,
          ...creds,
        } as Parameters<Sdk['Sandbox']['create']>[0]);
        return handle(sb);
      } catch (err) {
        const e = apiErrorOf(err);
        if (e.status === 409 && e.code === 'drive_attached') throw new ApiConflict('drive_attached', e.message);
        // A taken name: 409, or (P0.4) a 400 "already exists" while the old one is stopped.
        if (e.status === 409 || (e.status === 400 && /already exists/i.test(e.message))) throw new ApiConflict('name', e.message);
        throw err;
      }
    },
  };
}

/**
 * durable-harness/sandbox: a session's Vercel Sandbox (docs/workspaces.md
 * §3.3, §5.7). `sandbox()` is the descriptor every harness takes (also
 * exported from the root); the rest manages what it creates: a VM per
 * session and a Drive per session at `/workspace`, both named from the
 * session (`sandboxName`, `driveName`).
 *
 * ```ts
 * import { sandbox } from 'durable-harness/sandbox'
 * export const coder = claudeCode({ model, workspace: sandbox(), builtins: true })
 * ```
 *
 * `sandboxName` and `driveName` are the deterministic names the sandbox
 * tier uses (`dh-<hash of the project and the session>`), and
 * `prepareSandboxBase` builds the project's base snapshot ahead of the
 * first session (`npx durable-harness sandbox prepare`).
 * `deleteSessionSandbox` deletes one session's VM and Drive, and
 * `sandboxCleanup` those of sessions idle for some days; neither ever boots
 * a VM.
 *
 * Importing this entry loads only the names (`harness-world-sandbox/names`):
 * the session factory, the relay link and `@vercel/sandbox` load on the first
 * acquire, or the first call of one of these.
 */
import type { World } from '@workflow/world';
import { defaultRegion, resolveProjectId, sessionName } from 'harness-world-sandbox/names';
import { relayArtifact, sessionActivity } from './environments.ts';
import type { SandboxCredentials } from './workspace.ts';

export {
  sandbox,
  SANDBOX_DEFAULTS,
  type SandboxBudget,
  type SandboxCredentials,
  type SandboxNetwork,
  type SandboxNetworkPolicy,
  type SandboxNetworkSession,
  type SandboxOptions,
  type SandboxRecord,
  type SandboxWorkspace,
} from './workspace.ts';

/** Where the names' project comes from: `projectId`, else the deployment's `VERCEL_PROJECT_ID`, else the project `vercel link` linked. */
export interface SandboxNameOptions {
  projectId?: string;
}

/**
 * The Vercel Sandbox name of a session's VM: `dh-` and 20 base32 characters
 * of `sha256(projectId, runId)`. Every VM session of the session's sandbox
 * shares it.
 */
export function sandboxName(runId: string, options: SandboxNameOptions = {}): string {
  return sessionName(options.projectId ?? resolveProjectId(), runId);
}

/** The name of a session's Drive: the same hash as its VM's (the two live in separate namespaces). */
export function driveName(runId: string, options: SandboxNameOptions = {}): string {
  return sessionName(options.projectId ?? resolveProjectId(), runId);
}

/** Which project's sandboxes, and with which credentials. */
export interface SandboxManageOptions {
  /** `sandbox({ credentials })`'s, for a driver outside Vercel. Default: the Function's own (or `vercel link`'s on a laptop). */
  credentials?: SandboxCredentials;
}

/** What `deleteSessionSandbox` did: the session's named sandbox (`sandboxName`) and its Drive, each deleted or not there. */
export interface DeletedSessionSandbox {
  name: string;
  sandbox: 'deleted' | 'none';
  drive: 'deleted' | 'none';
}

export interface DeleteSessionSandboxOptions extends SandboxManageOptions {
  /** The World the session lives on. Default the one durable-harness uses (`getWorld()`). */
  world?: World;
  /** Delete even when the session's run is not over (completed, failed or cancelled). Default false. */
  force?: boolean;
}

/**
 * Delete a session's VM and its Drive (§5.7), for a session that is over.
 * Close the session first (`sessions.close`, or cancel its run): the run
 * must be completed, failed or cancelled on the World (or gone from it),
 * else this throws, unless `force: true`. A session still open would
 * otherwise come back on its next delivery with a new, empty Drive (and be
 * told its files were lost). It never boots a VM: a running VM is stopped
 * (whatever it was doing), then the named sandbox is deleted, then the
 * Drive, retried for up to a minute while the platform still has it
 * attached. Deleting the Drive deletes the session's files for good. A
 * sandbox or Drive that isn't there is fine. Deletes keep to the platform's
 * 20 a second, across the process.
 */
export async function deleteSessionSandbox(runId: string, options: DeleteSessionSandboxOptions = {}): Promise<DeletedSessionSandbox> {
  if (!options.force) {
    const world = options.world ?? (await (await import('./world.ts')).getWorld());
    const run = await world.runs.get(runId, { resolveData: 'none' } as Parameters<World['runs']['get']>[1]).catch((err: unknown) => {
      // Gone from the World: over. Any other failure is not an answer.
      if ((err as { status?: number })?.status === 404 || /not found/i.test(String((err as Error)?.message ?? ''))) return undefined;
      throw err;
    });
    if (run && !['completed', 'failed', 'cancelled'].includes(run.status)) {
      throw new Error(`durable-harness: deleteSessionSandbox(${JSON.stringify(runId)}): the session is still ${run.status}; close it first (sessions.close), or pass force: true`);
    }
  }
  const { deleteSession, vercelSandboxApi } = await import('harness-world-sandbox');
  const name = sessionName(options.credentials?.projectId ?? resolveProjectId(options.credentials), runId);
  return deleteSession(name, { api: vercelSandboxApi(options.credentials) });
}

/** What `sandboxCleanup` did. */
export interface SandboxCleanupResult {
  deleted: DeletedSessionSandbox[];
  /** Kept: `running` (its VM runs or holds the Drive, or the session resumed before its delete), `recent` (its log or its VM was active within `idleDays`), `unknown` (no session of the World names it; see `orphans`), `failed` (an error, in `error`). */
  kept: { name: string; reason: 'running' | 'recent' | 'unknown' | 'failed'; error?: string }[];
  /** Session Drives not looked at: the time budget (`deadline`) or `maxSessions` came first. */
  unreached: string[];
  /** Pass it as the next run's `resumeAt` to begin where this one stopped. */
  resumeAt?: string;
}

export interface SandboxCleanupOptions extends SandboxManageOptions {
  /** Delete the VMs and Drives of sessions idle this many days. */
  idleDays: number;
  /** The World the sessions live on. Default the one durable-harness uses (`getWorld()`). */
  world?: World;
  /**
   * Also delete a Drive no session of the World names (a session deleted
   * from the World, or one older than what the World lists), once its VM is
   * idle as long. Default false: kept, as `unknown`. **Every app and
   * environment that shares the Vercel project shares its session names**
   * (production, preview deployments, `vercel link` on a laptop), each with
   * its own World: with `orphans: true`, this World's cron deletes the idle
   * Drives of all the others' sessions too.
   */
  orphans?: boolean;
  /** Stop looking at sessions at this time (epoch ms). Default 4 minutes from the call (within a Function's default 300 s). */
  deadline?: number;
  /** Look at this many sessions at most. Default all (within `deadline`). */
  maxSessions?: number;
  /** Begin at this session name (a previous result's `resumeAt`). Default a random point, so that runs cut short cover every Drive over time. */
  resumeAt?: string;
}

/**
 * Delete the VMs and Drives of sessions idle for `idleDays` (opt-in, §5.7:
 * from a cron route of yours; deleting a Drive deletes the session's files,
 * so a session resumed later starts with an empty `/workspace` and is told
 * so). It never boots a VM. It lists the project's session Drives, and keeps
 * one whose VM is running or holds it, whose session wrote its log within
 * `idleDays` (the newest event of the run whose sandbox has that name), or
 * that no session of the World names (unless `orphans`). Each delete checks
 * again that the session is still idle: one that resumed in between is
 * kept, nothing of it stopped. Deletes keep to the platform's 20 a second;
 * a Drive still attached is retried, and so is a refusal of the moment.
 * Sessions are looked at one at a time, from a random point (or
 * `resumeAt`), until `deadline` (4 minutes by default) or `maxSessions`:
 * what wasn't reached is in `unreached`, so a project with many sessions is
 * covered over several runs.
 *
 * ```ts
 * // app/api/cron/sandboxes/route.ts (a Vercel Cron Job)
 * export const GET = async () => Response.json(await sandboxCleanup({ idleDays: 30 }))
 * ```
 */
export async function sandboxCleanup(options: SandboxCleanupOptions): Promise<SandboxCleanupResult> {
  if (typeof options?.idleDays !== 'number' || !(options.idleDays > 0)) throw new TypeError('durable-harness: sandboxCleanup({ idleDays }) needs a number of days above 0');
  const deadline = options.deadline ?? Date.now() + CLEANUP_BUDGET_MS;
  const { cleanupIdle, vercelSandboxApi } = await import('harness-world-sandbox');
  const world = options.world ?? (await (await import('./world.ts')).getWorld());
  const projectId = options.credentials?.projectId ?? resolveProjectId(options.credentials);
  return cleanupIdle({
    api: vercelSandboxApi(options.credentials),
    idleDays: options.idleDays,
    lastActivity: await sessionActivity(world, projectId),
    deadline,
    ...(options.maxSessions !== undefined ? { maxSessions: options.maxSessions } : {}),
    ...(options.resumeAt !== undefined ? { resumeAt: options.resumeAt } : {}),
    ...(options.orphans ? { orphans: true } : {}),
  });
}

/** How long `sandboxCleanup` looks at sessions by default: within a Function's default 300 s, with time to answer. */
const CLEANUP_BUDGET_MS = 4 * 60_000;

/**
 * Build the project's base snapshot for `region` ahead of the first session
 * (also `npx durable-harness sandbox prepare`): the relay, the pinned Claude
 * Code and Codex binaries, rg, bwrap and the unprivileged user (docs/workspaces.md
 * §5.7). Idempotent: an existing base that isn't about to expire is returned.
 * `region` defaults as a session's does (`VERCEL_REGION` when Vercel Sandbox
 * lists it, else `iad1`), so the base prepared is the one the first session
 * boots. Uses the Function's credential (or `vercel link`'s on a laptop),
 * unless `credentials` are given (`sandbox({ credentials })`'s).
 */
export async function prepareSandboxBase(options: { region?: string; credentials?: SandboxCredentials } = {}): Promise<{ snapshotId: string; built: boolean }> {
  const { prepareBase } = await import('harness-world-sandbox');
  const { snapshotId, built } = await prepareBase({ region: options.region ?? defaultRegion(), relay: await relayArtifact(), ...(options.credentials ? { credentials: options.credentials } : {}) });
  return { snapshotId, built };
}


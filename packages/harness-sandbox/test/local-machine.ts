/**
 * `localMachine()` — the private stand-in VM (docs/workspaces.md §5.8, §11.4
 * A2). It builds the relay bundle and runs it as a local child over temp dirs,
 * arms it, dials both sockets, and returns a branded `SessionMachine` built by
 * the same builder the session factory uses (`src/machine.ts`: the fence gate,
 * the spawn env check, loss as `signal`). It is a test helper: never a
 * published entry, and it refuses to run outside a test (`DH_TEST_LOCAL_SANDBOX=1`,
 * a test runner, not on Vercel, not in production — the same guard as
 * `brandForTests`).
 *
 * Degraded tier (macOS, unprivileged Linux): one uid, process-group kill, no
 * cgroups. As root (privileged CI) it runs the privileged tier: `runAs` is
 * `DH_TEST_RUN_AS` (default `nobody`) and the cgroup leaf is on, so the
 * privileged tests exercise the uid drop and cgroup kills.
 */
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { chmodSync, chownSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { CacheDir, FenceToken, SessionMachine } from 'harness-world-process';
import type { WsRequestHandler } from 'harness-world-remote/ws';
import { buildRelayBundle, type RelayBundle } from '../src/relay/bundle.ts';
import { armRequest, requestArm } from '../src/relay/arm.ts';
import { RelayLink, dialRelay, type DialOptions } from '../src/relay/dial.ts';
import { keyHash, mintModelKey, mintToken, tokenHash } from '../src/relay/protocol.ts';
import type { ArmReply, RelayConfig } from '../src/relay/relay.ts';
import { sessionMachine } from '../src/machine.ts';
import { brandForTests } from '../src/session.ts';

/** The session the stand-in arms for. */
export const SESSION = 'session-1';

/** One stand-in: the relay child, its config, and helpers for the fence and auth tests. */
export interface LocalSandbox {
  config: RelayConfig;
  /** The default attach's branded machine (fence 1, fenced). */
  machine: SessionMachine;
  /** The default attach's link, for direct `/ctl` tests. */
  link: RelayLink;
  /** The default attach's token and nonce. */
  token: string;
  nonce: string;
  /** The default attach's model key `P` (the relay is armed with its hash). */
  modelKey: string;
  /** Arm the relay with a fence token (default: the next higher one, fenced), returning the token and the relay's reply. `modelKey` defaults to the default attach's. */
  arm(options?: { fence?: FenceToken | null; session?: string; token?: string; modelKey?: string | null }): Promise<{ token: string; reply: ArmReply }>;
  /** Dial both sockets with a given token and nonce. */
  dial(options: { token: string; nonce: string } & Partial<DialOptions>): Promise<RelayLink>;
  /** A branded machine over `link`, holding `fence` (for the fence-gate tests). */
  machineFor(link: RelayLink, fence: FenceToken | undefined): SessionMachine;
  /** Kill the relay child (as an OOM kill would) and start a new one on the same config and state dir. */
  restart(): Promise<void>;
  /** Kill the relay child and delete the temp dirs. */
  dispose(): Promise<void>;
}

let bundlePromise: Promise<RelayBundle> | undefined;
/** Build the relay bundle once per test process. */
export function relayBundle(): Promise<RelayBundle> {
  return (bundlePromise ??= buildRelayBundle());
}

/**
 * A port nothing listens on right now. Another test file can take it before
 * the relay binds it (the relay binds `::`, this checks `127.0.0.1`), so
 * `startRelay` retries with fresh ports when the relay exits on `EADDRINUSE`.
 */
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.listen(0, '127.0.0.1', () => {
      const port = (s.address() as { port: number }).port;
      s.close(() => resolve(port));
    });
    s.on('error', reject);
  });
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** The stand-in's guard, the same as `brandForTests`'s. */
export function standInGuard(): void {
  if (process.env.DH_TEST_LOCAL_SANDBOX !== '1') throw new Error('localMachine: set DH_TEST_LOCAL_SANDBOX=1');
  if (!process.env.VITEST && process.env.NODE_ENV !== 'test') throw new Error('localMachine: runs only under a test runner');
  if (process.env.VERCEL) throw new Error('localMachine: refuses to run with VERCEL set');
  if (process.env.NODE_ENV === 'production') throw new Error('localMachine: refuses to run in production');
}

/** A relay child over temp dirs. */
export interface RelayChild {
  config: RelayConfig;
  configPath: string;
  bundle: RelayBundle;
  child(): ChildProcess;
  /** Start the relay child again on the same config (after `kill`). */
  restart(): Promise<void>;
  kill(): Promise<void>;
  stop(): Promise<void>;
}

/**
 * The relay child's own environment: `PATH` and the flags its stand-in guard
 * reads (`underTest`), never this process's. In a VM the relay starts with
 * the VM's environment, nothing of the Function's; the stand-in's relay must
 * not hold the test process's either, since on its one-uid tier the model's
 * commands can read the relay's `/proc/<pid>/environ` (the canary test reads
 * every process of the stand-in VM).
 */
function relayEnv(): NodeJS.ProcessEnv {
  const env: Record<string, string> = {};
  for (const key of ['PATH', 'DH_TEST_LOCAL_SANDBOX', 'VITEST', 'NODE_ENV']) {
    const value = process.env[key];
    if (value !== undefined) env[key] = value;
  }
  return env as NodeJS.ProcessEnv;
}

/** Start a relay child over temp dirs; arm and dial are the caller's. */
export async function startRelay(overrides: Partial<RelayConfig> = {}): Promise<RelayChild> {
  standInGuard();
  const bundle = await relayBundle();
  const dir = mkdtempSync(join(tmpdir(), 'dh-local-'));
  const root = process.getuid?.() === 0;
  const config: RelayConfig = {
    port: await freePort(),
    modelPort: await freePort(),
    stateDir: join(dir, 'state'),
    workspace: join(dir, 'workspace'),
    cacheRoot: join(dir, 'cache'),
    relayVersion: bundle.version.slice(0, 16),
    cliVersions: { 'fake-cli': '0.0.0' },
    ...(root ? { runAs: process.env.DH_TEST_RUN_AS ?? 'nobody', cgroup: true } : {}),
    ...overrides,
  };
  const configPath = join(dir, 'relay.json');
  writeFileSync(configPath, JSON.stringify(config));
  if (root && config.runAs) {
    // The agent must reach its cache dirs, and own the workspace (A3's first-mount chown does it in a VM).
    chmodSync(dir, 0o755);
    mkdirSync(config.workspace, { recursive: true });
    const [uid, gid] = ['-u', '-g'].map((f) => Number(execFileSync('id', [f, config.runAs!]).toString().trim()));
    chownSync(config.workspace, uid!, gid!);
  }
  let child!: ChildProcess;
  const launch = async () => {
    rmSync(join(config.stateDir, 'control.sock'), { force: true });
    child = spawn(process.execPath, [bundle.path, 'serve', configPath], { env: relayEnv(), stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    child.stderr?.on('data', (d) => (stderr += d));
    for (let i = 0; i < 200; i++) {
      if (existsSync(join(config.stateDir, 'control.sock'))) return;
      if (child.exitCode !== null) throw Object.assign(new Error(`relay exited early (${child.exitCode}): ${stderr}`), { inUse: stderr.includes('EADDRINUSE') });
      await sleep(25);
    }
    throw new Error(`relay control socket never appeared: ${stderr}`);
  };
  // The first start: a port another test file took between `freePort` and the bind is picked again.
  const firstLaunch = async () => {
    for (let attempt = 1; ; attempt++) {
      try {
        return await launch();
      } catch (err) {
        if (!(err as { inUse?: boolean }).inUse || attempt >= 5 || overrides.port !== undefined || overrides.modelPort !== undefined) throw err;
        config.port = await freePort();
        config.modelPort = await freePort();
        writeFileSync(configPath, JSON.stringify(config));
      }
    }
  };
  const kill = async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const gone = new Promise((r) => child.once('exit', r));
      child.kill('SIGKILL');
      await gone;
    }
  };
  await firstLaunch();
  return {
    config,
    configPath,
    bundle,
    child: () => child,
    restart: launch,
    kill,
    stop: async () => {
      await kill();
      // A command the relay started can outlive its SIGKILL for a moment and
      // still be writing under `dir` (ENOTEMPTY): rmSync retries those.
      rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 });
    },
  };
}

/** Build a branded `SessionMachine` over a link (the stand-in's attach), through the shared builder. */
export function machineOf(link: RelayLink, config: RelayConfig, sessionId: string, fence: FenceToken | undefined, modelKey: string): SessionMachine {
  const dirs = Object.fromEntries((['home', 'config', 'codex-home', 'tmp', 'cache'] as const).map((d) => [d, join(config.cacheRoot, d)])) as Record<CacheDir, string>;
  return brandForTests(sessionMachine({ link, sessionId, fence, root: config.workspace, dirs, modelUrl: `http://127.0.0.1:${config.modelPort}`, modelKey }));
}

/**
 * Start a stand-in, do the default attach (fence 1, fenced), and return a handle.
 * `modelKey: null` arms it without a model key (the model port then checks no
 * caller: tests that call it from this process).
 */
export async function localMachine(options: { model?: WsRequestHandler; relay?: Partial<RelayConfig>; modelKey?: null } = {}): Promise<LocalSandbox> {
  const relay = await startRelay(options.relay);
  const { config } = relay;
  let fenceSeq = 0;
  const modelKey = mintModelKey();

  const arm: LocalSandbox['arm'] = async ({ fence, session = SESSION, token = mintToken(), modelKey: key = modelKey } = {}) => {
    const f = fence === null ? undefined : (fence ?? { count: ++fenceSeq, fenced: true });
    const reply = await requestArm(config.stateDir, armRequest(tokenHash(token), session, f, key === null ? undefined : keyHash(key)));
    if (!reply) throw new Error('relay did not answer arm');
    return { token, reply };
  };

  const dial: LocalSandbox['dial'] = (o) =>
    dialRelay({
      ctlUrl: `ws://127.0.0.1:${config.port}/ctl`,
      modelUrl: `ws://127.0.0.1:${config.port}/model`,
      ...o,
      model: o.model ?? options.model,
    });

  const first = await arm({ fence: { count: 1, fenced: true }, ...(options.modelKey === null ? { modelKey: null } : {}) });
  fenceSeq = 1;
  if (!first.reply.ok) throw new Error(`default arm failed: ${JSON.stringify(first.reply)}`);
  const link = await dial({ token: first.token, nonce: first.reply.R });
  const machine = machineOf(link, config, first.reply.sessionId, { count: 1, fenced: true }, modelKey);

  return {
    config,
    machine,
    link,
    token: first.token,
    nonce: first.reply.R,
    modelKey,
    arm,
    dial,
    machineFor: (l, fence) => machineOf(l, config, SESSION, fence, modelKey),
    restart: async () => {
      await relay.kill();
      await relay.restart();
    },
    dispose: async () => {
      try {
        link.close();
      } catch {}
      await relay.stop();
    },
  };
}

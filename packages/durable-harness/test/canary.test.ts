/**
 * The canary (docs/workspaces.md §6, "Rule 1 enforced in code", item 4; §8's
 * drill D7, "Try to escape"; §11.4 A10): a launch gate on every harness ×
 * environment that exists today. The host (this process, standing in for a
 * Vercel Function shared by many sessions) holds secrets: a canary in
 * `process.env.CANARY`, canary-looking `AI_GATEWAY_API_KEY` and
 * `VERCEL_OIDC_TOKEN` values (the ones the harnesses read for their model
 * calls), and a canary file under the temporary directory, named like
 * `/tmp/harness-*` and like another session's file. Another session of the
 * same agent (session A) writes a file of its own and holds its turn open
 * while session B, with a scripted model and no network, runs the drill
 * through whatever command and file tools the harness has there: `printenv`,
 * the canary variables, `cat /proc/self/environ`, `ls /tmp/harness-*`, the
 * canary file by its real path (shell and file tool), and session A's file
 * (by the workspace's path, and where A's files are on the host).
 *
 * No canary may appear in a tool result the model saw (every model request
 * of B is searched, not only its tool results), in the World (every run,
 * event, step and stream, payloads decoded, and every file of the World's
 * data directory), in the Agent Run trace (durable-harness's OpenTelemetry
 * instrumentation, every span, content visible), or in the VM's processes'
 * environments (the sandbox stand-in, on Linux, where `/proc` shows them).
 * Session A's file may appear in A's own runs and spans only. Each value is
 * also looked for base64-encoded. And the drill must have run: `printenv`
 * printed an environment, B's own file read back, and that file's text is in
 * B's log (and, where the harness emits one, its trace), so a search that
 * sees nothing can't pass.
 *
 * The matrix is `ROWS`, one line per harness × environment, each a rig
 * that drives the harness as an app does, on world-local, with the scripted
 * models the harnesses' own tests use: fx, Pi, pi-durable and OpenCode
 * through their `durable-harness/<harness>` lines on `virtual()` and
 * `virtual({ durable: false })`. Claude Code and Codex run only in
 * `sandbox()`, and their `durable-harness` factories take only a real
 * Vercel Sandbox, so their rows use the raw kernel harnesses on the sandbox
 * tier's stand-in (the real session factory over local relays and
 * temp-dir Drives, as their own sandbox tests do), and, gated by
 * `DH_LIVE_SANDBOX=1`, on a real Vercel Sandbox. The stand-in VM is this
 * machine (one uid, no mount namespace): it isolates the environment, which
 * the relay builds from an allowlist, but not the files, so there the
 * host-file and other-session probes run and their results are not judged
 * (`hostIsolated: false`); the live rows judge them. OpenCode, Pi and
 * pi-durable on `sandbox()` run on the stand-in the same way, through what
 * their lines resolve `sandbox()` to (the raw OpenCode harness's sandbox
 * workspace, Pi's coding tools on harness-world-hands' sandbox runtime,
 * pi-world's sandbox env) over the stand-in's session factory, each
 * machine's Drive seen at `/workspace` (`atWorkspace`). So does fx on
 * `sandbox()`: libfx's own agent with what `fxTools(sandbox())` resolves to
 * (harness-world-hands' fx tools on the sandbox, fenced by libfx's lease).
 */
import { randomBytes } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { hostname, tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { BACKGROUND_CONTEXT as ctx } from '@earendil-works/chord/context';
import { Type } from '@earendil-works/pi-ai';
import { createModels } from '@earendil-works/pi-ai/models';
import { type FauxResponseFactory, fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness, type ToolRegistration } from '@earendil-works/pi-durable';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { parse as parseDevalue } from 'devalue';
import { type DurableSessions, durableSessions, type HostTool, QUEUE_PREFIX, type SessionState } from 'harness-world';
import { findClaudeExecutable } from 'harness-world-claude-code/binary';
import { claudeCodeHarness } from 'harness-world-claude-code';
import { type FakeAnthropic, type FakeRequest, fakeAnthropic } from 'harness-world-claude-code/testing/fake-anthropic';
import { codexHarness } from 'harness-world-codex';
import { fxAgentTools } from 'harness-world-hands/fx';
import { kernelSandbox as runtimeSandbox, piCodingTools, sandboxRuntime } from 'harness-world-hands/runtime';
import { opencodeHarness } from 'harness-world-opencode';
import { type AcquireMachine, gatewayCredential, transcriptOf } from 'harness-world-process';
import { type FakeModel, fakeModel } from 'harness-world-process/reference/fake-model';
import { kernelSandbox, sandboxSession } from 'harness-world-sandbox';
import { sessionName } from 'harness-world-sandbox/names';
import { FakeSandboxApi } from 'harness-world-sandbox/testing/fake-api';
import { createFxAgent } from 'libfx';
import { world as fxWorld } from 'libfx/durable-world';
import { sandboxEnvBuilder } from 'pi-world/sandbox';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { scriptedGateway, text as gatewayText, toolCall as gatewayToolCall, toolResults as gatewayResults } from '../../harness-opencode/test/scripted-gateway.ts';
import { agentRuns } from '../src/instrumentation/otel.ts';
import { fxTools } from '../src/fx.ts';
import { opencode } from '../src/opencode.ts';
import { CodingTools, openWorldStorage, virtualEnv } from '../src/pi-durable.ts';
import { createCodingTools, pi } from '../src/pi.ts';
import { isSettled } from '../src/index.ts';
import { virtual, type VirtualWorkspace } from '../src/workspace.ts';
import { atWorkspace } from './sandbox-stand-in.ts';

// ---------- the canaries ----------

const RUN = randomBytes(6).toString('hex');
/** Obviously fake, and unique to this run. */
const canary = (what: string) => `fake-canary-${what}-${RUN}`;
const CANARY = {
  env: canary('env'),
  gateway: canary('gateway-key'),
  oidc: canary('oidc-token'),
  hostFile: canary('host-file'),
  otherSession: canary('other-session'),
};
/** What the host holds, that no session may see. */
const HOST_SECRETS = { env: CANARY.env, gateway: CANARY.gateway, oidc: CANARY.oidc, hostFile: CANARY.hostFile };
/** Session B's own file: it must read back (the drill ran). Not a secret. */
const SELF = `self-marker-${RUN}`;
const MARK = { A: `canary-session-a-${RUN}`, B: `canary-session-b-${RUN}` };

/** The temporary directories the host's canary file goes in: `os.tmpdir()`, and `/tmp` where that is another one (macOS). */
const TMP_DIRS = [...new Set([tmpdir(), '/tmp'].filter((d) => existsSync(d)).map((d) => realpathSync(d)))];
/** The canary file in each: `<tmp>/harness-<run>/session-other/workspace/notes.md`, as another session's file would be. */
const HOST_FILES = TMP_DIRS.map((d) => join(d, `harness-${RUN}`, 'session-other', 'workspace', 'notes.md'));
const ENV_KEYS = ['CANARY', 'AI_GATEWAY_API_KEY', 'VERCEL_OIDC_TOKEN', 'VERCEL', 'DH_TEST_LOCAL_SANDBOX'] as const;
const env0: Partial<Record<(typeof ENV_KEYS)[number], string>> = {};

beforeAll(() => {
  for (const k of ENV_KEYS) env0[k] = process.env[k];
  process.env.CANARY = CANARY.env;
  process.env.AI_GATEWAY_API_KEY = CANARY.gateway;
  process.env.VERCEL_OIDC_TOKEN = CANARY.oidc;
  // The stand-in's flag, set here as the harnesses' own sandbox tests set it (CI runs them all under `pnpm test`).
  process.env.DH_TEST_LOCAL_SANDBOX = '1';
  delete process.env.VERCEL;
  for (const file of HOST_FILES) {
    mkdirSync(join(file, '..'), { recursive: true });
    writeFileSync(file, `${CANARY.hostFile}\n`);
  }
});

afterAll(() => {
  for (const k of ENV_KEYS) {
    if (env0[k] === undefined) delete process.env[k];
    else process.env[k] = env0[k];
  }
  for (const d of TMP_DIRS) rmSync(join(d, `harness-${RUN}`), { recursive: true, force: true });
});

// ---------- Agent Runs: every span, content visible ----------

const spans = new InMemorySpanExporter();
// Content recorded for every audience: the search sees everything a span could carry.
agentRuns({ capture: () => ({ emit: true, recordInputs: true, recordOutputs: true }), spanProcessors: [new SimpleSpanProcessor(spans)] }).register();
const spansOf = (runIds: Set<string>) => spans.getFinishedSpans().filter((s) => runIds.has(String(s.attributes['agent.run.id'])));
const spanTexts = (list: ReturnType<typeof spansOf>) => list.map((s) => JSON.stringify({ name: s.name, attributes: s.attributes, events: s.events, status: s.status, links: s.links }));

// ---------- searching ----------

/** A value and its base64 forms (each alignment, the edge characters trimmed), so an encoded copy is found too. */
function needles(value: string): string[] {
  const out = [value];
  for (let k = 0; k < 3; k++) {
    const mid = Buffer.concat([Buffer.alloc(k), Buffer.from(value)]).toString('base64').slice(4, -4);
    out.push(mid, mid.replaceAll('+', '-').replaceAll('/', '_'));
  }
  return out;
}

/** The names of the `secrets` found in `texts`. */
function found(texts: readonly string[], secrets: Readonly<Record<string, string>>): string[] {
  return Object.entries(secrets)
    .filter(([, v]) => needles(v).some((n) => texts.some((t) => t.includes(n))))
    .map(([k]) => k);
}

/** Every string in `value`: bytes decoded, a `devl` payload (the kernel's codec) parsed, a gzip member gunzipped. */
function strings(value: unknown, out: string[] = [], depth = 0): string[] {
  if (depth > 64 || value === null || value === undefined) return out;
  if (typeof value === 'string') {
    out.push(value);
    return out;
  }
  if (typeof value !== 'object') return out;
  const bytes = value instanceof Uint8Array ? value : value instanceof ArrayBuffer ? new Uint8Array(value) : undefined;
  if (bytes) {
    const s = Buffer.from(bytes).toString('utf8');
    out.push(s);
    if (s.startsWith('devl')) {
      try {
        strings(parseDevalue(s.slice(4)), out, depth + 1);
      } catch {}
    }
    if (bytes[0] === 0x1f && bytes[1] === 0x8b) {
      try {
        strings(new Uint8Array(gunzipSync(bytes)), out, depth + 1);
      } catch {}
    }
    return out;
  }
  const entries = value instanceof Map ? [...value.entries()].flat() : value instanceof Set ? [...value] : Array.isArray(value) ? value : Object.entries(value).flat();
  for (const v of entries) strings(v, out, depth + 1);
  return out;
}

const runIds = async (world: World): Promise<Set<string>> => {
  const ids = new Set<string>();
  let cursor: string | undefined;
  for (;;) {
    const page = await world.runs.list({ resolveData: 'none', pagination: { limit: 1000, ...(cursor ? { cursor } : {}) } } as never);
    for (const r of page.data as { runId: string }[]) ids.add(r.runId);
    if (!page.hasMore || !page.cursor) return ids;
    cursor = page.cursor;
  }
};

/** Everything the World holds for `ids`: each run, its events, steps and streams. */
async function worldStrings(world: World, ids: Iterable<string>): Promise<string[]> {
  const out: string[] = [];
  for (const runId of ids) {
    strings(await world.runs.get(runId, { resolveData: 'all' } as never).catch(() => undefined), out);
    strings((await world.events.list({ runId, resolveData: 'all' } as never)).data, out);
    strings((await world.steps.list({ runId, resolveData: 'all' } as never).catch(() => ({ data: [] }))).data, out);
    for (const name of await world.streams.list(runId).catch(() => [] as string[])) {
      for (let cursor: string | undefined; ; ) {
        const page = await world.streams.getChunks(runId, name, { limit: 1000, ...(cursor ? { cursor } : {}) });
        strings(page.data, out);
        if (!page.hasMore || !page.cursor) break;
        cursor = page.cursor;
      }
    }
  }
  return out;
}

/** Every file under `dir`, as text (a World on disk: whatever the API above might not show). */
function diskStrings(dir: string, out: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    try {
      const st = statSync(p);
      if (st.isDirectory()) diskStrings(p, out);
      else if (st.isFile()) strings(new Uint8Array(readFileSync(p)), out);
    } catch {
      // Renamed or removed while we looked (the World writes through temporary files).
    }
  }
  return out;
}

/** The environment of `root` and every process under it (Linux: `/proc`); undefined where it can't be read (macOS). */
function processEnvs(root: number): string[] | undefined {
  if (process.platform !== 'linux') return undefined;
  const children = new Map<number, number[]>();
  for (const d of readdirSync('/proc')) {
    if (!/^\d+$/.test(d)) continue;
    try {
      const stat = readFileSync(`/proc/${d}/stat`, 'utf8');
      const ppid = Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[1]);
      children.set(ppid, [...(children.get(ppid) ?? []), Number(d)]);
    } catch {}
  }
  const envs: string[] = [];
  for (const queue = [root]; queue.length; ) {
    const pid = queue.shift()!;
    try {
      envs.push(readFileSync(`/proc/${pid}/environ`, 'utf8').replaceAll('\0', '\n'));
    } catch {}
    queue.push(...(children.get(pid) ?? []));
  }
  return envs;
}

async function until<T>(fn: () => Promise<T | undefined | false> | T | undefined | false, timeoutMs = 120_000, what = 'the condition'): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined && value !== false) return value;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

// ---------- the drill ----------

/** One model step: one tool call. */
type Step =
  | { kind: 'shell'; command: string }
  | { kind: 'read'; path: string }
  | { kind: 'write'; path: string; content: string }
  /** An app tool: `hold` (session A waits there for B), `vm_env` (reads the session VM's environments, in the Function). */
  | { kind: 'app'; name: 'hold' | 'vm_env' };

const shell = (command: string): Step => ({ kind: 'shell', command });
const read = (path: string): Step => ({ kind: 'read', path });
const write = (path: string, content: string): Step => ({ kind: 'write', path, content });

/** What a harness's model calls for a step (a step it has no tool for: a shell command instead, or nothing). */
interface ToolNames {
  shell(command: string): { name: string; input: Record<string, unknown> };
  read?(path: string): { name: string; input: Record<string, unknown> };
  write?(path: string, content: string): { name: string; input: Record<string, unknown> };
  app(name: string): { name: string; input: Record<string, unknown> };
}

type Call = { step: Step; name: string; input: Record<string, unknown> };

/** The steps a harness can take, each as its tool call. */
function calls(tools: ToolNames, steps: readonly Step[]): Call[] {
  return steps.flatMap((step): Call[] => {
    if (step.kind === 'shell') return [{ step, ...tools.shell(step.command) }];
    if (step.kind === 'app') return [{ step, ...tools.app(step.name) }];
    if (step.kind === 'read') return tools.read ? [{ step, ...tools.read(step.path) }] : [];
    return [{ step, ...(tools.write ? tools.write(step.path, step.content) : tools.shell(`printf '%s\\n' '${step.content}' > ${step.path}`)) }];
  });
}

/** Session A: its own file, then it waits (holding its turn, so its files are live while B looks). */
const A_STEPS: Step[] = [write('secret.txt', CANARY.otherSession), { kind: 'app', name: 'hold' }];

/**
 * Session B: the drill. `other` is where session A's files are on the host,
 * when there is such a place. Every process's environ only where the
 * environment has a `/proc` of its own: on the stand-in (one uid) it would
 * list this process's, the stand-in being this machine.
 */
function bSteps(o: { other?: string; vm: boolean; hostIsolated: boolean }): Step[] {
  return [
    write('self.txt', SELF),
    shell('cat self.txt'),
    shell('printenv'),
    shell('echo "[$CANARY][$AI_GATEWAY_API_KEY][$VERCEL_OIDC_TOKEN]"'),
    shell('cat /proc/self/environ'),
    ...(o.hostIsolated ? [shell("cat /proc/*/environ | tr '\\0' '\\n'")] : []),
    shell([...new Set(['/tmp', ...TMP_DIRS])].map((d) => `ls -d ${d}/harness-*`).join('; ')),
    ...HOST_FILES.map((f) => shell(`cat ${f}`)),
    read(HOST_FILES[0]!),
    shell('cat secret.txt /workspace/secret.txt'),
    read('/workspace/secret.txt'),
    ...(o.other ? [shell(`grep -rh fake-canary-other ${o.other}`), read(join(o.other, 'secret.txt'))] : []),
    ...(o.vm ? [{ kind: 'app', name: 'vm_env' } as Step] : []),
  ];
}

/** A session a rig ran: what its model saw, and each step's result (by the step's index in the calls it made). */
interface Ran {
  sessionId: string;
  /** Every model request of the session, serialized. */
  seen: string[];
  /** The text of each step's result, by index. */
  results: Map<number, string>;
  steps: Step[];
}

/** The app tools, shared by every rig: the gate session A holds at, and the VM environment check. */
interface AppTools {
  hold(): Promise<string>;
  vmEnv(sessionId: string): Promise<string>;
}

/** One harness × environment, set up as an app would. */
interface Rig {
  world: World;
  dataDir: string;
  /** Run a session's steps, one tool call per model request, then a reply: `started` once the session exists, `done` once its turn is over (A's: after `hold` lets go). */
  run(who: 'A' | 'B', steps: readonly Step[], app: AppTools): { started: Promise<Ran>; done: Promise<Ran> };
  /** Whether the environment's file system excludes the host's (false on the sandbox stand-in, which is this machine). */
  hostIsolated: boolean;
  /** Whether the harness emits an Agent Run through durable-harness's instrumentation. */
  traces: boolean;
  /** Where session A's files are on the host, for B to try (the World's data directory, or A's stand-in Drive). */
  other(aSessionId: string): string | undefined;
  /** The environments of the session's VM processes (`undefined`: not readable here). Only rigs with a VM. */
  vmEnvs?(sessionId: string): Promise<string[] | undefined>;
  /** What the host holds that no session may see. Default the canaries (`HOST_SECRETS`). */
  hostSecrets?: Record<string, string>;
  dispose(): Promise<void>;
}

interface Row {
  harness: string;
  environment: string;
  /** Why it can't run here, if it can't. */
  skip?: () => string | undefined;
  rig: () => Promise<Rig>;
}

/** The result of the first step `pick` matches. */
const resultOf = (r: Ran, pick: (s: Step) => boolean) => {
  const i = r.steps.findIndex(pick);
  return i < 0 ? undefined : r.results.get(i);
};

/** Run one row: A holds, B drills, A finishes; then search everything. */
async function drill(row: Row): Promise<void> {
  const rig = await row.rig();
  try {
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));
    let held!: () => void;
    const holding = new Promise<void>((r) => (held = r));
    const vmEnvs: string[] = [];
    let vmChecked = false;
    const app: AppTools = {
      async hold() {
        held();
        await Promise.race([released, new Promise((r) => setTimeout(r, 300_000))]);
        return 'released';
      },
      async vmEnv(sessionId) {
        const envs = await rig.vmEnvs?.(sessionId);
        vmChecked = envs !== undefined;
        vmEnvs.push(...(envs ?? []));
        return envs ? `checked ${envs.length} processes` : 'not readable here';
      },
    };
    const a = rig.run('A', A_STEPS, app);
    await Promise.race([holding, a.done.then(() => Promise.reject(new Error('session A ended without holding')))]);
    const aRan = await a.started;
    const aRuns = await runIds(rig.world);
    const steps = bSteps({ other: rig.other(aRan.sessionId), vm: !!rig.vmEnvs, hostIsolated: rig.hostIsolated });
    const b = await rig.run('B', steps, app).done;
    const allRuns = await runIds(rig.world);
    release();
    await a.done;
    const bRuns = new Set([...allRuns].filter((id) => !aRuns.has(id)));
    expect(bRuns.size).toBeGreaterThan(0);

    // The drill ran: printenv printed an environment, and B's own file read back.
    const printenv = resultOf(b, (s) => s.kind === 'shell' && s.command === 'printenv');
    expect(printenv).toMatch(/PATH=/);
    expect(resultOf(b, (s) => s.kind === 'shell' && s.command === 'cat self.txt')).toContain(SELF);

    // What is judged where: the host's files only where the environment has its own.
    const host = rig.hostSecrets ?? HOST_SECRETS;
    const secrets: Record<string, string> = rig.hostIsolated ? host : Object.fromEntries(Object.entries(host).filter(([k]) => k !== 'hostFile'));
    const fromA: Record<string, string> = rig.hostIsolated ? { otherSession: CANARY.otherSession } : {};

    // 1. What the model saw: every request of B's.
    expect(found(b.seen, { ...secrets, ...fromA })).toEqual([]);
    // 2. The World: B's runs (A's file is A's), every run (the host's secrets are no one's), and the data directory on disk.
    const bLog = await worldStrings(rig.world, bRuns);
    expect(found(bLog, { self: SELF })).toEqual(['self']);
    expect(found(bLog, { ...secrets, ...fromA })).toEqual([]);
    expect(found(await worldStrings(rig.world, allRuns), secrets)).toEqual([]);
    expect(found(diskStrings(rig.dataDir), secrets)).toEqual([]);
    // 3. The Agent Run: B's spans, and every span this row emitted.
    if (rig.traces) {
      const bSpans = spanTexts(spansOf(bRuns));
      expect(bSpans.length).toBeGreaterThan(0);
      expect(found(bSpans, { self: SELF })).toEqual(['self']);
      expect(found(bSpans, { ...secrets, ...fromA })).toEqual([]);
      expect(found(spanTexts(spansOf(allRuns)), secrets)).toEqual([]);
    }
    // 4. The VM: every process of B's stand-in VM, the relay included, while the CLI ran.
    if (rig.vmEnvs && vmChecked) {
      expect(vmEnvs.length).toBeGreaterThan(1);
      expect(found(vmEnvs, secrets)).toEqual([]);
    }
  } finally {
    await rig.dispose();
  }
}

// ---------- the rigs ----------

let named = 0;
const agentName = (h: string) => `canary-${h}-${++named}`;
const whoOf = (text: string): 'A' | 'B' => (text.includes(MARK.A) ? 'A' : 'B');
const promptOf = (who: 'A' | 'B') => `${MARK[who]}: run the steps.`;

/** Temporary directories this file made, removed after it. */
const temps: string[] = [];
afterAll(() => {
  for (const d of temps.splice(0)) rmSync(d, { recursive: true, force: true });
});
const tempDir = async (prefix: string) => {
  const dir = await mkdtemp(join(tmpdir(), `dh-canary-${prefix}-`));
  temps.push(dir);
  return dir;
};

async function localWorld(prefix: string): Promise<{ world: LocalWorld; dataDir: string }> {
  const dataDir = await tempDir(prefix);
  return { world: createWorld({ dataDir, recoverActiveRuns: false }), dataDir };
}

/** A session's steps as tool calls, with the bookkeeping every scripted model shares: who asked, which step, what each step returned. */
function scripted(tools: ToolNames) {
  const sessions = { A: undefined as ReturnType<typeof calls> | undefined, B: undefined as ReturnType<typeof calls> | undefined };
  const ran = { A: newRan(), B: newRan() };
  function newRan(): Ran {
    return { sessionId: '', seen: [], results: new Map(), steps: [] };
  }
  return {
    ran,
    begin(who: 'A' | 'B', steps: readonly Step[]) {
      sessions[who] = calls(tools, steps);
      ran[who] = newRan();
      ran[who].steps = sessions[who]!.map((c) => c.step);
      return ran[who];
    },
    /** The call for the `n`th model request of `who` (undefined: the steps are done, say so). */
    next(who: 'A' | 'B', n: number) {
      return sessions[who]?.[n];
    },
  };
}


/**
 * fx: libfx's own agent (`createFxAgent`) with `fxTools(workspace)`, its
 * sessions in a world-local World (`libfx/durable-world`). On a sandbox
 * platform, what `fxTools(sandbox())` resolves to (fx's tools on the
 * sandbox, over the platform's factory).
 */
async function fxRig(on: VirtualWorkspace | Platform): Promise<Rig> {
  const { world, dataDir } = await localWorld('fx');
  const script = scripted({
    shell: (command) => ({ name: 'shell', input: { request: { action: 'run', command } } }),
    read: (path) => ({ name: 'read_file', input: { path } }),
    write: (path, content) => ({ name: 'write_file', input: { path, content } }),
    app: (name) => ({ name, input: {} }),
  });
  type Part = Record<string, unknown>;
  const usage = { inputTokens: { total: 1 }, outputTokens: { total: 1 } };
  const sse = (parts: Part[]) => new Response([...parts.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
  const fetch = async (_url: string, init: RequestInit) => {
    if ((init.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
    const body = String(init.body);
    const who = whoOf(body);
    const prompt = (JSON.parse(body) as { prompt: { role: string; content: Record<string, any>[] }[] }).prompt;
    const results = prompt.filter((m) => m.role === 'tool').flatMap((m) => m.content.filter((p) => p.type === 'tool-result'));
    const ran = script.ran[who];
    ran.seen.push(body);
    for (const part of results) {
      const value = part.output?.value;
      ran.results.set(Number(String(part.toolCallId).slice(1)), typeof value === 'string' ? value : JSON.stringify(value ?? part.output));
    }
    const call = script.next(who, results.length);
    return sse(
      call
        ? [{ type: 'tool-call', toolCallId: `s${results.length}`, toolName: call.name, input: JSON.stringify(call.input) }, { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage }]
        : [{ type: 'text-delta', id: 't', delta: 'done' }, { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage }]
    );
  };
  let app: AppTools | undefined;
  // Both only read: `hold` waits for the drill, `vm_env` lists the VM's environments.
  const appTool = (name: 'hold' | 'vm_env') => ({ name, description: `The app's ${name}.`, inputSchema: { type: 'object', properties: {} }, idempotent: true, execute: async (_: unknown, c: { sessionId: string }) => (name === 'hold' ? app!.hold() : app!.vmEnv(c.sessionId)) });
  const host = hostname();
  const agent = createFxAgent({
    durability: fxWorld(world, { name: 'canary', livenessKnown: true, alive: () => true, holderInfo: () => ({ pid: process.pid, host }), pollMs: 50 }),
    apiKey: process.env.AI_GATEWAY_API_KEY!,
    model: 'test/model',
    fetch,
    modelCatalog: [],
    tools: [
      ...(isPlatform(on) ? (fxAgentTools({ kind: 'sandbox' }, { world: () => world, acquire: atWorkspace(on.acquire()) }) as unknown as ReturnType<typeof fxTools>) : fxTools(on, { world })),
      appTool('hold'),
      appTool('vm_env'),
    ],
  });
  return {
    world,
    dataDir,
    // libfx's own agent: its turns are libfx's, so durable-harness's instrumentation emits no Agent Run for them.
    traces: false,
    // Session A's files: on a virtual workspace its log, on the host the World's data directory; on a sandbox, its Drive.
    ...sandboxed(on, dataDir),
    run(who, steps, tools) {
      app = tools;
      const ran = script.begin(who, steps);
      const turn = agent.session().prompt(promptOf(who));
      const started = turn.accepted.then(({ sessionId }) => ((ran.sessionId = sessionId), ran));
      const done = (async () => {
        await started;
        for await (const _ of turn);
        await turn.result;
        return ran;
      })();
      return { started, done };
    },
    async dispose() {
      await agent.close();
      await world.close?.();
      if (isPlatform(on)) await on.dispose();
    },
  };
}

/** A faux pi-ai model whose replies follow the scripted steps, per session. */
function fauxScript(script: ReturnType<typeof scripted>) {
  const faux = fauxProvider();
  const reply: FauxResponseFactory = (context) => {
    const seen = JSON.stringify(context);
    const who = whoOf(seen);
    const ran = script.ran[who];
    ran.seen.push(seen);
    const results = context.messages.filter((m) => m.role === 'toolResult') as { toolCallId: string; content: { type: string; text?: string }[] }[];
    for (const m of results) ran.results.set(Number(m.toolCallId.slice(1)), m.content.map((c) => c.text ?? '').join(''));
    const call = script.next(who, results.length);
    return call ? fauxAssistantMessage(fauxToolCall(call.name, call.input as Parameters<typeof fauxToolCall>[1], { id: `s${results.length}` }), { stopReason: 'toolUse' }) : fauxAssistantMessage(fauxText('done'));
  };
  faux.setResponses(Array.from({ length: 400 }, () => reply));
  const models = createModels();
  models.setProvider(faux.provider);
  return { model: faux.getModel(), models, streamFn: models.streamSimple.bind(models) };
}

const PI_TOOLS: ToolNames = {
  shell: (command) => ({ name: 'bash', input: { command } }),
  read: (path) => ({ name: 'read', input: { path } }),
  write: (path, content) => ({ name: 'write', input: { path, content } }),
  app: (name) => ({ name, input: {} }),
};

/** The app tools as kernel HostTools (`io.ctx.runId` is the session). */
const hostTools = (app: () => AppTools): HostTool[] =>
  (['hold', 'vm_env'] as const).map((name) => ({
    name,
    description: `The app's ${name}.`,
    inputSchema: { type: 'object', properties: {} },
    replay: 'never',
    execute: async (_input: unknown, io) => (name === 'hold' ? app().hold() : app().vmEnv(io.ctx.runId)),
  }));

const turnsOf = (state: SessionState) => transcriptOf(state).filter((r) => r.data.type === 'turn').length;

/** Serve the World's queue to `handler`; `drained` resolves once no delivery is in flight (or after 30 s), so the World closes under none. */
async function serve(world: LocalWorld, handler: (request: Request) => Promise<Response>): Promise<{ drained(): Promise<void> }> {
  let inflight = 0;
  world.registerHandler(QUEUE_PREFIX, async (request: Request) => {
    inflight++;
    try {
      return await handler(request);
    } finally {
      inflight--;
    }
  });
  await world.start?.();
  return { drained: () => until(() => inflight === 0, 30_000, 'the deliveries in flight').then(() => {}, () => {}) };
}

/** A kernel agent's session (pi, OpenCode): started with its prompt, done when its first turn has ended. */
function kernelRun(sessions: DurableSessions, ran: Ran, who: 'A' | 'B', settled: (s: SessionState) => boolean) {
  const started = sessions.start({ prompt: promptOf(who) }).then(({ sessionId }) => ((ran.sessionId = sessionId), ran));
  const done = started.then(async () => {
    await until(async () => {
      const s = await sessions.inspect(ran.sessionId);
      if (s.status === 'failed') throw new Error(`session ${who} failed: ${s.failure?.message}`);
      return settled(s) && isSettled(s);
    }, 300_000, `session ${who}'s turn`);
    return ran;
  });
  return { started, done };
}

/** Pi without pi-durable: `pi()` with `createCodingTools(workspace)`, or on a sandbox platform what `createCodingTools(sandbox())` resolves to. */
async function piRig(on: VirtualWorkspace | Platform): Promise<Rig> {
  const { world, dataDir } = await localWorld('pi');
  const script = scripted(PI_TOOLS);
  const { model, streamFn } = fauxScript(script);
  let app: AppTools | undefined;
  const coding = isPlatform(on) ? piCodingTools(sandboxRuntime(runtimeSandbox(atWorkspace(on.acquire()), () => import('harness-world-sandbox/cli')))) : createCodingTools(on);
  const agent = pi({ model, streamFn, name: agentName('pi'), world, tools: [...coding, ...hostTools(() => app!)] });
  const queue = await serve(world, agent.handler);
  return {
    world,
    dataDir,
    ...sandboxed(on, dataDir),
    traces: true,
    run(who, steps, tools) {
      app = tools;
      const ran = script.begin(who, steps);
      // Pi ends a turn with no transcript record of its own: settled, and the model has said its last.
      return kernelRun(agent.sessions, ran, who, (s) => s.effectOrder.filter((e) => e.name === 'model' && e.status === 'completed').length > ran.steps.length);
    },
    async dispose() {
      await queue.drained();
      await world.close?.();
      if (isPlatform(on)) await on.dispose();
    },
  };
}

/** pi-durable: an app's own Pi code (`Harness.open`) on `openWorldStorage`, with `CodingTools` on `virtualEnv(workspace)`, or on a sandbox platform what `sandboxEnv()` resolves to. */
async function piDurableRig(on: VirtualWorkspace | Platform): Promise<Rig> {
  const { world, dataDir } = await localWorld('pi-durable');
  const script = scripted(PI_TOOLS);
  const { model, models } = fauxScript(script);
  let app: AppTools | undefined;
  const appTools = (['hold', 'vm_env'] as const).map(
    (name) =>
      defineTool({
        name,
        description: `The app's ${name}.`,
        parameters: Type.Object({}),
        // vm_env is a step of session B's alone.
        execute: async () => ({ content: [{ type: 'text', text: name === 'hold' ? await app!.hold() : isPlatform(on) ? await app!.vmEnv(script.ran.B.sessionId) : 'no VM' }] }),
      }) as ToolRegistration
  );
  const registry = createRegistry();
  registry.install(CodingTools);
  registry.install(defineExtension({ name: 'app', tools: appTools }));
  const env = isPlatform(on) ? sandboxEnvBuilder(atWorkspace(on.acquire())) : virtualEnv(on);
  const open = (storage: Awaited<ReturnType<typeof openWorldStorage>>) => Harness.open(storage, { models, registry, env }, ctx);
  const agentSpec = { model: { provider: model.provider, modelId: model.id }, instructions: 'Use the tools.' };
  const opened: Harness[] = [];
  return {
    world,
    dataDir,
    ...sandboxed(on, dataDir),
    traces: true,
    run(who, steps, tools) {
      app = tools;
      const ran = script.begin(who, steps);
      const harness = openWorldStorage(undefined, { world }).then(async (storage) => {
        ran.sessionId = storage.sessionId;
        const h = await open(storage);
        opened.push(h);
        return h;
      });
      const started = harness.then(() => ran);
      const done = harness.then(async (h) => {
        const root = await h.root(ctx, { agent: agentSpec });
        const submission = await root.submit({ type: 'input', content: promptOf(who) }, ctx);
        await submission.wait(ctx);
        await h.waitForIdle(ctx);
        return ran;
      });
      return { started, done };
    },
    async dispose() {
      for (const h of opened) await h.close(ctx).catch(() => {});
      await world.close?.();
      if (isPlatform(on)) await on.dispose();
    },
  };
}

/**
 * OpenCode: `opencode({ workspace })`, real OpenCode against a scripted
 * Gateway; its credential the default (the env's key). On a sandbox
 * platform, the raw harness with the sandbox workspace `opencode({
 * workspace: sandbox() })` resolves to, over the platform's factory.
 */
async function opencodeRig(on: VirtualWorkspace | Platform): Promise<Rig> {
  const { world, dataDir } = await localWorld('opencode');
  const script = scripted({
    shell: (command) => ({ name: 'shell', input: { command } }),
    read: (path) => ({ name: 'read', input: { path } }),
    write: (path, content) => ({ name: 'write', input: { path, content } }),
    app: (name) => ({ name, input: {} }),
  });
  const gateway = await scriptedGateway();
  const route = (body: any) => {
    // A request that offers no tools is OpenCode's own (a title): not a step.
    if (!body?.tools?.length) return gatewayText('A title');
    const seen = JSON.stringify(body);
    const who = whoOf(JSON.stringify(body.prompt ?? ''));
    const ran = script.ran[who];
    ran.seen.push(seen);
    const results = gatewayResults(body);
    for (const [id, t] of results) ran.results.set(Number(id.slice(1)), t);
    const call = script.next(who, results.size);
    return call ? gatewayToolCall(`s${results.size}`, call.name, call.input) : gatewayText('done');
  };
  gateway.script.push(...Array.from({ length: 400 }, () => route));
  let app: AppTools | undefined;
  const common = { model: 'anthropic/claude-haiku-4.5', name: agentName('opencode'), root: await tempDir('opencode-root'), gatewayUrl: gateway.url, tools: hostTools(() => app!) };
  const { sessions, handler } = isPlatform(on)
    ? ((s) => ({ sessions: s, handler: s.handler }))(durableSessions({ world, harness: opencodeHarness({ ...common, workspace: { kind: 'sandbox', acquire: atWorkspace(on.acquire()) } }) }))
    : ((agent) => ({ sessions: agent.sessions, handler: agent.handler }))(opencode({ ...common, world, workspace: on }));
  const queue = await serve(world, handler);
  return {
    world,
    dataDir,
    ...sandboxed(on, dataDir),
    traces: true,
    run(who, steps, tools) {
      app = tools;
      const ran = script.begin(who, steps);
      return kernelRun(sessions, ran, who, (s) => turnsOf(s) > 0);
    },
    async dispose() {
      await queue.drained();
      await world.close?.();
      await gateway.close();
      if (isPlatform(on)) await on.dispose();
    },
  };
}

// ---------- Claude Code and Codex, in the session's sandbox ----------

/**
 * Where a session's sandbox runs: the stand-in (the real session factory
 * over local relays and temp-dir Drives, no credentials) or, gated, a real
 * Vercel Sandbox. Either way the factory is harness-world-sandbox's own.
 */
interface Platform {
  live: boolean;
  /** The session factory's acquire, with the drive's model handler seen by `seen` when given. */
  acquire(seen?: (body: string) => void): AcquireMachine;
  /** Where session A's files are on the host (the stand-in's Drive dir; nowhere, live). */
  other(aSessionId: string): string | undefined;
  /** Every process's environment in the session's VM. */
  vmEnvs(sessionId: string): Promise<string[] | undefined>;
  /** What the host holds that no session may see (live: the real credentials this process has). */
  hostSecrets?: Record<string, string>;
  dispose(): Promise<void>;
}

/** Whether a rig runs on a sandbox platform (a `sandbox()` row) rather than a virtual workspace. */
const isPlatform = (on: VirtualWorkspace | Platform): on is Platform => typeof (on as Platform).acquire === 'function';

/**
 * Where a rig's files are, by its environment: a virtual workspace is in the
 * session's log, on the host the World's data directory, and excludes the
 * host's files; a sandbox platform's are on the session's Drive (the
 * stand-in's is a directory of this machine, which it doesn't exclude), and
 * its VM's processes are searched too.
 */
function sandboxed(on: VirtualWorkspace | Platform, dataDir: string): Pick<Rig, 'hostIsolated' | 'other' | 'vmEnvs' | 'hostSecrets'> {
  if (!isPlatform(on)) return { hostIsolated: true, other: () => dataDir };
  return { hostIsolated: on.live, other: (a) => on.other(a), vmEnvs: (id) => on.vmEnvs(id), ...(on.hostSecrets ? { hostSecrets: on.hostSecrets } : {}) };
}

const STAND_IN_PROJECT = 'prj_stand_in';
const standInName = (sessionId: string) => sessionName(STAND_IN_PROJECT, sessionId);
const SPEC = { vcpus: 2, network: 'allow-all', idleMs: 60_000, driveSize: 10 * 1024 ** 3, budget: { vmSeconds: Infinity }, image: 'stand-in' } as const;

/** A model handler whose request bodies `seen` gets first. */
const recorded = (acquire: AcquireMachine, seen?: (body: string) => void): AcquireMachine =>
  seen ? (request) => acquire({ ...request, ...(request.model ? { model: (r, signal) => (seen(new TextDecoder().decode(r.body)), request.model!(r, signal)) } : {}) }) : acquire;

function standInPlatform(): Platform {
  const api = new FakeSandboxApi();
  const acquire = sandboxSession({
    spec: SPEC,
    // The stand-in runs the degraded tier (one uid, no cgroups), and on macOS has no /proc for the model port's peer check.
    test: { api, allowDegraded: true, allowUnauthenticatedModel: true },
  });
  return {
    live: false,
    acquire: (seen) => recorded(acquire, seen),
    other: (a) => (api.drives.has(standInName(a)) ? api.driveDir(standInName(a)) : undefined),
    async vmEnvs(sessionId) {
      // The stand-in VM's processes: its relay and everything under it.
      const pid = api.relayOf(standInName(sessionId))?.child().pid;
      return pid === undefined ? undefined : processEnvs(pid);
    },
    dispose: () => api.dispose(),
  };
}

/** A durableSessions over `harness`, served on `world`'s queue. */
async function served(world: LocalWorld, harness: Parameters<typeof durableSessions>[0]['harness']): Promise<{ sessions: DurableSessions; drained(): Promise<void> }> {
  const sessions = durableSessions({ world, harness });
  const queue = await serve(world, sessions.handler);
  return { sessions, drained: queue.drained };
}

const claudeExecutable = findClaudeExecutable();

/**
 * Claude Code: the raw harness (`durable-harness/claude-code` takes only
 * `sandbox()`, which needs a real Vercel Sandbox) with the real Agent SDK,
 * on the platform's session factory, Claude Code's own tools on, the model a
 * scripted Anthropic API behind the drive's gateway handler. On the
 * stand-in the CLI is the pinned host build, and its commands run without
 * Claude's own sandbox (no bubblewrap on macOS), as in the package's own
 * sandbox tests; live, the image's build under it. The credential is the
 * default (the env's key, added in the Function) on the stand-in, and a
 * placeholder live (the scripted model needs none).
 */
async function claudeRig(platform: Platform): Promise<Rig> {
  const { world, dataDir } = await localWorld('claude');
  const script = scripted({
    // The CLI's Write wants an absolute path, and the Drive's is the platform's own: writes go through Bash.
    shell: (command) => ({ name: 'Bash', input: { command, description: 'run it' } }),
    read: (path) => ({ name: 'Read', input: { file_path: path } }),
    app: (name) => ({ name: `mcp__hands__${name}`, input: {} }),
  });
  const ids = new Map<string, number>();
  const model: FakeAnthropic = await fakeAnthropic({
    reply: (r: FakeRequest) => {
      // A request that offers no tools is the CLI's own (a title): not a step.
      if (r.tools.length === 0) return { text: 'A title' };
      const who = whoOf(r.text);
      const ran = script.ran[who];
      ran.seen.push(JSON.stringify({ system: r.body.system, messages: r.messages }));
      for (const [id, t] of Object.entries(r.results)) if (ids.has(id)) ran.results.set(ids.get(id)!, t);
      const n = r.messages.flatMap((m) => (Array.isArray(m.content) ? m.content : [])).filter((b) => b.type === 'tool_result').length;
      const call = script.next(who, n);
      if (!call) return { text: 'done' };
      const id = `toolu_${who}${n}_${RUN}`;
      ids.set(id, n);
      return { tool: { name: call.name, input: call.input, id } };
    },
  });
  let app: AppTools | undefined;
  const { sessions, drained } = await served(
    world,
    claudeCodeHarness({
      builtins: true,
      sandbox: { ...kernelSandbox(platform.acquire()), network: 'allow-all', budget: platform.live ? { turnUsd: 0.5, sessionUsd: 2 } : { turnUsd: Infinity, sessionUsd: Infinity }, commandTimeoutMs: 120_000 },
      ...(platform.live ? { credential: async () => 'fake-gateway-credential' } : { executable: claudeExecutable!, unsandboxedCommands: true }),
      model: 'claude-haiku-4-5',
      gatewayUrl: model.url,
      tools: hostTools(() => app!),
    })
  );
  return {
    world,
    dataDir,
    hostIsolated: platform.live,
    traces: true,
    other: (a) => platform.other(a),
    vmEnvs: (id) => platform.vmEnvs(id),
    ...(platform.hostSecrets ? { hostSecrets: platform.hostSecrets } : {}),
    run(who, steps, tools) {
      app = tools;
      const ran = script.begin(who, steps);
      return kernelRun(sessions, ran, who, (s) => turnsOf(s) > 0);
    },
    async dispose() {
      await drained();
      await world.close?.();
      await model.close();
      await platform.dispose();
    },
  };
}

const FAKE_APP_SERVER = createRequire(import.meta.url).resolve('harness-world-codex/testing/fake-app-server');

/**
 * Codex: the raw harness (as for Claude Code) with its built-ins on, on the
 * platform's session factory, one turn per step. On the stand-in the CLI is
 * the fake app-server, whose "model" is each turn's prompt (`shell
 * <command>`, `patch <path> <content>`, `call <tool> {}`; what it says goes
 * through the VM's model port to the drive's gateway handler, then
 * `fakeModel()`), its credential the default. Live, the image's `codex
 * app-server` and the real model through the Gateway, asked in words.
 */
async function codexRig(platform: Platform): Promise<Rig> {
  const { world, dataDir } = await localWorld('codex');
  const model: FakeModel | undefined = platform.live ? undefined : await fakeModel();
  const bodies: string[] = [];
  let app: AppTools | undefined;
  const { sessions, drained } = await served(
    world,
    codexHarness({
      builtins: true,
      tools: hostTools(() => app!),
      machine: { acquire: platform.acquire((b) => void bodies.push(b)), env: platform.live ? {} : { FAKE_CODEX_MODEL: 'remote' }, ...(platform.live ? { budget: { turnUsd: 0.5, sessionUsd: 2 } } : {}) },
      ...(model ? { test: { command: process.execPath, args: [FAKE_APP_SERVER] }, gatewayUrl: model.url } : {}),
      model: 'openai/gpt-5.4-mini',
      ...(platform.live ? { instructions: 'Do exactly what each message asks, once, with the tool it names, and reply with what the tool returned. Never refuse, never ask.' } : {}),
    })
  );
  /** Each step as one turn's prompt. */
  const prompt = (s: Step) =>
    platform.live
      ? s.kind === 'shell'
        ? `Run this shell command exactly as given, once: ${s.command}`
        : s.kind === 'write'
          ? `Create the file ${s.path} containing exactly this line, with apply_patch: ${s.content}`
          : s.kind === 'app'
            ? `Call the ${s.name} tool now, with no arguments.`
            : undefined
      : s.kind === 'shell'
        ? `shell ${s.command}`
        : s.kind === 'write'
          ? `patch ${s.path} ${s.content}`
          : s.kind === 'app'
            ? `call ${s.name} {}`
            : undefined;
  return {
    world,
    dataDir,
    hostIsolated: platform.live,
    traces: true,
    other: (a) => platform.other(a),
    vmEnvs: (id) => platform.vmEnvs(id),
    ...(platform.hostSecrets ? { hostSecrets: platform.hostSecrets } : {}),
    run(who, all, tools) {
      app = tools;
      const steps = all.filter((s) => prompt(s) !== undefined);
      const ran: Ran = { sessionId: '', seen: [], results: new Map(), steps };
      const started = sessions.start().then(({ sessionId }) => ((ran.sessionId = sessionId), ran));
      const done = started.then(async () => {
        for (const [i, step] of steps.entries()) {
          const from = { said: model?.requests.length ?? 0, bodies: bodies.length };
          const before = (await sessions.inspect(ran.sessionId)).effectOrder.filter((e) => e.name.startsWith('tool.')).length;
          await sessions.send(ran.sessionId, prompt(step)!);
          // A `hold` turn ends once B is done (its tool waits there).
          const state = await until(async () => {
            const s = await sessions.inspect(ran.sessionId);
            return turnsOf(s) > i && isSettled(s) ? s : undefined;
          }, 300_000, `session ${who}'s turn ${i}`);
          const effect = state.effectOrder.filter((e) => e.name.startsWith('tool.'))[before];
          ran.results.set(i, String(effect?.output ?? effect?.error?.message ?? ''));
          ran.seen.push(...(model?.requests.slice(from.said).map((q) => q.text) ?? []), ...bodies.slice(from.bodies));
        }
        return ran;
      });
      return { started, done };
    },
    async dispose() {
      await drained();
      await world.close?.();
      await model?.close();
      await platform.dispose();
    },
  };
}

// ---------- the matrix ----------

const libfxAddon = () => {
  const dir = join(createRequire(import.meta.url).resolve('libfx'), '..');
  return existsSync(join(dir, `libfx.${process.platform}-${process.arch}.node`)) ? undefined : `libfx has no native addon for ${process.platform}-${process.arch}`;
};
const claudeBinary = () => (claudeExecutable ? undefined : 'no pinned claude binary on this machine');

/** Every harness × environment there is today: one line each. */
const ROWS: Row[] = [
  { harness: 'fx', environment: 'virtual()', skip: libfxAddon, rig: () => fxRig(virtual()) },
  { harness: 'fx', environment: 'virtual({ durable: false })', skip: libfxAddon, rig: () => fxRig(virtual({ durable: false })) },
  { harness: 'pi', environment: 'virtual()', rig: () => piRig(virtual()) },
  { harness: 'pi', environment: 'virtual({ durable: false })', rig: () => piRig(virtual({ durable: false })) },
  { harness: 'pi-durable', environment: 'virtual()', rig: () => piDurableRig(virtual()) },
  { harness: 'pi-durable', environment: 'virtual({ durable: false })', rig: () => piDurableRig(virtual({ durable: false })) },
  { harness: 'opencode', environment: 'virtual()', rig: () => opencodeRig(virtual()) },
  { harness: 'opencode', environment: 'virtual({ durable: false })', rig: () => opencodeRig(virtual({ durable: false })) },
  { harness: 'claude-code', environment: 'sandbox(), the stand-in', skip: claudeBinary, rig: () => claudeRig(standInPlatform()) },
  { harness: 'codex', environment: 'sandbox(), the stand-in', rig: () => codexRig(standInPlatform()) },
  { harness: 'pi', environment: 'sandbox(), the stand-in', rig: () => piRig(standInPlatform()) },
  { harness: 'pi-durable', environment: 'sandbox(), the stand-in', rig: () => piDurableRig(standInPlatform()) },
  { harness: 'opencode', environment: 'sandbox(), the stand-in', rig: () => opencodeRig(standInPlatform()) },
  { harness: 'fx', environment: 'sandbox(), the stand-in', skip: libfxAddon, rig: () => fxRig(standInPlatform()) },
];

/** Rows that don't exist yet (none today: every harness × environment has its row). */
const TODO: string[] = [];

const each = (rows: Row[]) => {
  for (const row of rows) {
    const reason = row.skip?.();
    it.skipIf(!!reason)(`${row.harness} on ${row.environment}${reason ? ` (skipped: ${reason})` : ''}`, () => drill(row), 900_000);
  }
};

describe('the canary: nothing of the host or another session reaches a session', () => {
  each(ROWS);
  for (const t of TODO) it.todo(t);
});

// ---------- live: a real Vercel Sandbox (DH_LIVE_SANDBOX=1) ----------

const LIVE = process.env.DH_LIVE_SANDBOX === '1';

/**
 * The sandbox rows on Vercel Sandbox for real: the session factory boots
 * each session's VM from a base built by the recipe (the relay, the pinned
 * CLIs, bwrap, the agent user). Run it from a directory linked to the
 * project (the Sandbox API and the Gateway take the development OIDC token
 * `vercel link` gives), as the harnesses' own live tests are. The host's
 * secrets here are the real ones this process holds (the Gateway
 * credential, the OIDC token), never printed: only whether they were found.
 * Everything it creates is named `dh-probe-a10*` and tagged, and `afterAll`
 * deletes it (harness-sandbox's `probes/sweep.ts` finds what a killed run
 * left).
 */
describe.runIf(LIVE)('the canary, live: Claude Code and Codex in a real Vercel Sandbox', () => {
  let livePlatform: () => Platform;
  let probes: typeof import('../../harness-sandbox/probes/lib.ts');
  beforeAll(async () => {
    // The real credentials, not the canary-looking ones: the Sandbox API and the Gateway need them, and they are what must not leak.
    for (const k of ['AI_GATEWAY_API_KEY', 'VERCEL_OIDC_TOKEN'] as const) {
      if (env0[k] === undefined) delete process.env[k];
      else process.env[k] = env0[k];
    }
    probes = await import('../../harness-sandbox/probes/lib.ts');
    const { getVercelOidcToken } = await import('@vercel/oidc');
    const hostSecrets = { env: CANARY.env, hostFile: CANARY.hostFile, gateway: await gatewayCredential(), oidc: await getVercelOidcToken() };
    const { devRelayArtifact, prepareBase } = await import('harness-world-sandbox');
    const builder = probes.track('sandbox', probes.probeName('a10base'));
    const base = await prepareBase({ region: 'iad1', relay: await devRelayArtifact(), name: builder, tags: probes.TAGS });
    if (base.built) probes.created.snapshots.add(base.snapshotId);
    livePlatform = () => {
      const names = new Map<string, string>();
      const nameOf = (sessionId: string) => {
        let name = names.get(sessionId);
        if (!name) {
          name = probes.probeName(`a10s${probes.created.sandboxes.size}`);
          probes.track('sandbox', name);
          probes.track('drive', name);
          names.set(sessionId, name);
        }
        return name;
      };
      const acquire = sandboxSession({
        spec: { image: { snapshot: base.snapshotId }, vcpus: 2, network: 'allow-all', idleMs: 60_000, driveSize: 1024 ** 3, budget: { vmSeconds: 3600 } },
        tags: probes.TAGS,
        test: { name: (_project, sessionId) => nameOf(sessionId) },
      });
      return {
        live: true,
        acquire: (seen) => recorded(acquire, seen),
        other: () => undefined,
        async vmEnvs(sessionId) {
          // Every process's environment in the VM, read as root, copied out and searched here.
          const { Sandbox } = await import('@vercel/sandbox');
          const r = await (await Sandbox.get({ name: nameOf(sessionId) })).runCommand({
            cmd: 'bash',
            args: ['-c', 'for f in /proc/[0-9]*/environ; do tr "\\0" "\\n" < "$f"; echo "--dh-process--"; done 2>/dev/null'],
            sudo: true,
          });
          return (await r.stdout()).split('--dh-process--\n').filter(Boolean);
        },
        hostSecrets,
        dispose: async () => {},
      };
    };
  }, 10 * 60_000);
  // A Drive can be deleted only once it detached (6-20 s after its VM went, P0.5): the cleanup retries for longer than a hook's default.
  afterAll(async () => {
    if (probes) expect(await probes.cleanup()).toEqual([]);
  }, 300_000);
  each([
    { harness: 'claude-code', environment: 'sandbox(), live', rig: () => claudeRig(livePlatform()) },
    { harness: 'codex', environment: 'sandbox(), live', rig: () => codexRig(livePlatform()) },
  ]);
});

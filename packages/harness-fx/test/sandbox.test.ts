import { type ChildProcess, spawn } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { hostname, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { durableSessions, type HostTool, QUEUE_PREFIX, readDoc, type SessionState } from 'harness-world';
import { createFxAgent, createFxEngine, memory } from 'libfx';
import { world as fxWorld } from 'libfx/durable-world';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { FX_NO_INTENT, FX_SCHEMAS, FX_SUPERSEDED, FX_TOOL_NAMES, fxDescription, fxSandboxStreamName } from '../../harness-hands/src/fx-exports.ts';
import { isWorkspaceTool, type PhasedTool, SANDBOX_NOTE, VIRTUAL_NOTE } from '../../harness-hands/src/runtime-exports.ts';
import type { AcquireMachine } from '../../harness-process/src/index.ts';
import { devRelayArtifact, FILES_LOST_NOTE, prepareBase, SANDBOX_DOC, sandboxSession, sessionName, vercelSandboxApi } from '../../harness-sandbox/src/index.ts';
import { fxHarness } from '../src/index.ts';
import { agentTools, FakeSandboxApi, kernelTools, relayBundle, seededStandIn, STAND_IN_PROJECT, standIn } from './stand-in.ts';

/**
 * B7 for fx (docs/workspaces.md §11.4): the suites fx's Function-tier item
 * (B5) added for its tools on `virtual()` (fx's definitions, its output
 * formats, each call kept with its outcome, a call never reaching a kernel
 * built-in, a process killed mid-call), run on a sandbox instead, through
 * the sandbox tier's real session factory (`sandboxSession`, A3) and
 * `acquireMachine` with each fx runtime's own fence (P0.12):
 *
 * - libfx's own durable agent (`createFxAgent`) with `fxTools(sandbox())`
 *   (harness-world-hands' `fxAgentTools` on a sandbox): the token is the
 *   epoch of the libfx lease under which the call's intent was stored, read
 *   from the session's log in the World its sessions live in (world-local,
 *   `libfx/durable-world`);
 * - the kernel's fx harness (`fxHarness` on durableSessions) with
 *   `fxHostTools` on harness-world-hands' sandbox runtime: the kernel's
 *   `attachInDrive`, a `sandbox.attach` fact with the drive's own fence.
 *
 * The sandbox is the stand-in (§5.8, always on: `DH_TEST_LOCAL_SANDBOX=1`,
 * set in vitest.config.ts before libfx loads): each "VM" a local relay
 * child over temp dirs (`localMachine()`'s relay, through harness-sandbox's
 * fake Sandbox API), each "Drive" a temp dir that outlives the VMs mounted
 * on it, each machine's workspace seen at /workspace (`test/stand-in.ts`). In
 * the degraded tier (macOS, unprivileged Linux) its commands run as this
 * user on this machine.
 *
 * With `DH_LIVE_SANDBOX=1`, the last case runs on a live Vercel Sandbox
 * through the same factory and the real Sandbox API (see there).
 */

const LIVE = process.env.DH_LIVE_SANDBOX === '1';
const CHILD = fileURLToPath(new URL('./sandbox-child.ts', import.meta.url));
const PACKAGE = fileURLToPath(new URL('..', import.meta.url));

// Nothing in this file changes process.env. libfx's native code reads the environment
// through a pointer to the environment array that it took earlier; setting a variable
// that is not set yet can move that array and free the old one (a SIGSEGV on a libfx
// thread). So the stand-in's flag comes from vitest.config.ts, before libfx loads. The one
// exception is the live case's: the Sandbox SDK's development token (@vercel/oidc sets
// VERCEL_OIDC_TOKEN), minted in its beforeAll, before any case loads libfx's addon.
const ENV_AT_LOAD = new Map(Object.entries(process.env));
/** The names (never the values) of the variables that changed since the file loaded. */
function envChanges(): string[] {
  const now = new Map(Object.entries(process.env));
  const names = new Set([...ENV_AT_LOAD.keys(), ...now.keys()]);
  return [...names].filter((k) => ENV_AT_LOAD.get(k) !== now.get(k) && !(LIVE && k === 'VERCEL_OIDC_TOKEN')).sort();
}

// ---------------------------------------------------------------------------
// A scripted AI Gateway (the AI SDK stream parts libfx's kernel reads)

type Part = Record<string, unknown>;
const usage = { inputTokens: { total: 1 }, outputTokens: { total: 1 } };
const sse = (parts: Part[]) =>
  new Response([...parts.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
const call = (id: string, name: string, input: unknown): Part[] => [
  { type: 'tool-call', toolCallId: id, toolName: name, input: JSON.stringify(input) },
  { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage },
];
/** Several tool calls in one model response (libfx runs the ones that only read side by side). */
const calls = (...list: [id: string, name: string, input: unknown][]): Part[] => [
  ...list.map(([id, name, input]) => ({ type: 'tool-call', toolCallId: id, toolName: name, input: JSON.stringify(input) })),
  { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage },
];
const say = (text: string): Part[] => [
  { type: 'text-delta', id: 't', delta: text },
  { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage },
];
const run = (command: string, extra: Record<string, unknown> = {}) => ({ request: { action: 'run', command, ...extra } });

function gateway(script: Part[][]) {
  const bodies: string[] = [];
  const fetch = async (_url: string | URL | Request, init?: RequestInit) => {
    if ((init?.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
    bodies.push(String(init?.body));
    const next = script.shift();
    if (!next) throw new Error('unexpected model request');
    return sse(next);
  };
  /** What the model was sent back for each tool call, by call id. */
  const results = () => {
    const out = new Map<string, { text: string; isError: boolean }>();
    for (const body of bodies) {
      for (const message of (JSON.parse(body) as { prompt: { role: string; content: Record<string, any>[] }[] }).prompt) {
        if (message.role !== 'tool') continue;
        for (const part of message.content) {
          const value = part.output?.value;
          out.set(part.toolCallId, { text: typeof value === 'string' ? value : JSON.stringify(value), isError: /error/.test(String(part.output?.type)) });
        }
      }
    }
    return out;
  };
  return { fetch, bodies, results };
}

let base: string;
const apis: FakeSandboxApi[] = [];
const closers: (() => unknown)[] = [];

beforeAll(async () => {
  // The stand-in's guard (§5.8): the test flag, a test runner, not on Vercel.
  if (process.env.DH_TEST_LOCAL_SANDBOX !== '1') throw new Error('run with DH_TEST_LOCAL_SANDBOX=1 (vitest.config.ts sets it)');
  if (process.env.VERCEL) throw new Error('the sandbox stand-in refuses to run with VERCEL set');
  // The relay bundle, built once (esbuild, in a temp dir of its own).
  await relayBundle();
  base = realpathSync(mkdtempSync(join(tmpdir(), 'fx-sandbox-')));
}, 120_000);

afterEach(async () => {
  for (const c of closers.splice(0).reverse()) await c();
  for (const api of apis.splice(0)) await api.dispose();
});

afterAll(() => {
  rmSync(base, { recursive: true, force: true });
  expect(envChanges(), 'this file must leave process.env as it found it (libfx)').toEqual([]);
});

let worldCount = 0;
/** A world-local World; another instance on the same `dataDir` is another server on the same store. */
function localWorld(dataDir = join(base, `world-${worldCount++}`)): LocalWorld {
  const w = createWorld({ dataDir, recoverActiveRuns: false });
  closers.push(() => w.close?.());
  return w;
}

/** libfx's durability on a world-local World, as the demo builds it: a lease's holder is alive while its process is. */
function durabilityOn(w: LocalWorld) {
  const host = hostname();
  return fxWorld(w, {
    name: 'local',
    livenessKnown: true,
    queueDurable: false,
    pollMs: 50,
    holderInfo: () => ({ pid: process.pid, host }),
    alive(lease) {
      if (lease.host !== host || typeof lease.pid !== 'number') return null;
      try {
        process.kill(lease.pid, 0);
        return true;
      } catch (error) {
        return (error as NodeJS.ErrnoException).code === 'EPERM';
      }
    },
  });
}

/** A stand-in sandbox (a fake Sandbox API, the session factory over it), and every token its acquires were given. */
function sandbox(options: Parameters<typeof standIn>[0] = {}) {
  const s = standIn(options);
  if (!options.api) apis.push(s.api);
  const fences: { count: number; fenced: boolean }[] = [];
  const acquire: AcquireMachine = (request) => {
    const fence = request.fence();
    if (fence) fences.push({ count: fence.count, fenced: fence.fenced });
    return s.acquire(request);
  };
  return { ...s, acquire, fences };
}

/** libfx's own agent on `w`, with `tools` and a scripted model. */
function agentOn(w: LocalWorld, tools: unknown[], script: Part[][]) {
  const gw = gateway(script);
  const agent = createFxAgent({ durability: durabilityOn(w), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: tools as never });
  closers.push(() => agent.close());
  return { agent, gw };
}

async function finish(turn: { result: Promise<unknown>; [Symbol.asyncIterator](): AsyncIterator<unknown> }) {
  for await (const _ of turn as AsyncIterable<unknown>);
  return turn.result;
}

/** The epochs of the leases in a session's libfx log, in order (renewals included). */
async function leaseEpochs(w: LocalWorld, sessionId: string): Promise<number[]> {
  const page = await w.events.list({ runId: sessionId, pagination: { sortOrder: 'asc' }, resolveData: 'all' });
  const out: number[] = [];
  for (const e of page.data) {
    const data = (e as { eventData?: { stepName?: string; input?: Uint8Array } }).eventData;
    if (e.eventType !== 'step_created' || data?.stepName !== 'fx.log' || !(data.input instanceof Uint8Array)) continue;
    const entry = JSON.parse(new TextDecoder().decode(data.input)) as { k: string; epoch?: number };
    if (entry.k === 'lease' && out.at(-1) !== entry.epoch) out.push(entry.epoch!);
  }
  return out;
}

/** The session's sandbox (and Drive) name on the stand-in's project. */
const nameOf = (sessionId: string) => sessionName(STAND_IN_PROJECT, sessionId);
const onDrive = (api: FakeSandboxApi, sessionId: string, path: string) => {
  const p = join(api.driveDir(nameOf(sessionId)), path);
  return existsSync(p) ? readFileSync(p, 'utf8') : undefined;
};
const snapshot = (text: string | undefined) => JSON.parse(text ?? 'null') as Record<string, unknown>;

async function until<T>(fn: () => Promise<T | undefined> | T | undefined, ms = 90_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v !== undefined) return v;
    if (Date.now() - start > ms) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

// ---------------------------------------------------------------------------

/**
 * The live case (`DH_LIVE_SANDBOX=1`, the gate harness-sandbox's live tests
 * use): libfx's own agent on a real Vercel Sandbox, through the session
 * factory and `@vercel/sandbox` (no stand-in, no path view: the VM's Drive
 * is at /workspace), armed with its lease's epoch: a turn on VM A writes with
 * fx's write_file and shell as the VM's unprivileged user; VM A is deleted
 * (the Drive stays); the next turn, by the next worker, on VM B, reads the
 * file. Run it from a directory linked to a Vercel project
 * (`DH_LIVE_SANDBOX_DIR`, default the directory the test started in), with
 * the Vercel CLI logged in: the SDK mints the linked project's development
 * token through `@vercel/oidc`. Every sandbox, Drive and snapshot is named
 * `dh-probe-*` and tagged (harness-sandbox's `probes/sweep.ts` finds what a
 * killed run left), and `afterAll` deletes them. No secret is printed. It
 * comes first in the file, so its token is minted before libfx loads.
 */
describe.runIf(LIVE)('live: fx on a Vercel Sandbox', () => {
  let lib: typeof import('../../harness-sandbox/probes/lib.ts');
  let cwd0: string;
  beforeAll(async () => {
    cwd0 = process.cwd();
    process.chdir(process.env.DH_LIVE_SANDBOX_DIR ?? cwd0);
    lib = await import('../../harness-sandbox/probes/lib.ts');
    // The SDK's development token, minted now (it lands in process.env), before any case loads libfx's addon.
    expect(await vercelSandboxApi().getSandbox(lib.probeName('b7fx-none'))).toBeUndefined();
  }, 60_000);
  // A Drive can be deleted only once it detached (6-20 s after its VM went): the cleanup retries for longer than a hook's default.
  afterAll(async () => {
    try {
      expect(await lib.cleanup()).toEqual([]);
    } finally {
      process.chdir(cwd0);
    }
  }, 240_000);

  it("a turn on VM A writes with fx's tools; VM A is deleted; the next worker's turn, on VM B over the same Drive, reads them", { timeout: 15 * 60_000 }, async () => {
    const api = vercelSandboxApi();
    // A base with the relay only (fx's tools need bash and coreutils), built in a builder named for the sweep.
    const snap = await prepareBase({ region: 'iad1', relay: await devRelayArtifact(), clis: [], name: lib.track('sandbox', lib.probeName('b7fxbase')), tags: lib.TAGS });
    lib.created.snapshots.add(snap.snapshotId);
    const name = lib.probeName('b7fx');
    lib.track('sandbox', name);
    lib.track('drive', name);
    const fences: number[] = [];
    const factory = (): AcquireMachine => {
      const acquire = sandboxSession({
        spec: { image: { snapshot: snap.snapshotId }, vcpus: 2, network: 'allow-all', idleMs: 60_000, driveSize: 1024 ** 3, budget: { vmSeconds: 3600 } },
        tags: lib.TAGS,
        test: { name: () => name },
      });
      return (request) => {
        const f = request.fence();
        if (f) fences.push(f.count);
        return acquire(request);
      };
    };
    const w = localWorld();
    const first = agentOn(w, agentTools(factory(), w), [
      call('a1', 'write_file', { path: 'notes/live.txt', content: 'written on VM A\n' }),
      call('a2', 'shell', run('cat /workspace/notes/live.txt; pwd; id -un; echo "[$AI_GATEWAY_API_KEY][$VERCEL_OIDC_TOKEN]"')),
      say('done'),
    ]);
    const t1 = first.agent.session().prompt('Write it.');
    const sessionId = (await t1.accepted).sessionId;
    await finish(t1);
    expect(first.gw.results().get('a1')?.text).toBe('wrote notes/live.txt (16 bytes)');
    expect(snapshot(first.gw.results().get('a2')?.text)).toMatchObject({ exit_code: 0, backend: 'sandbox', output_delta: 'written on VM A\n/workspace\nagent\n[][]\n' });
    await first.agent.close();
    const vmA = (await api.getSandbox(name))!.sessionId;

    // VM A goes; the Drive stays. Another factory (another process's) and the session's next worker run the next turn.
    await (await api.getSandbox(name))!.delete();
    const second = agentOn(w, agentTools(factory(), w), [call('b1', 'read_file', { path: 'notes/live.txt' }), call('b2', 'shell', run('ls /workspace/notes; stat -c %U /workspace/notes/live.txt')), say('read it')]);
    await finish(second.agent.session(sessionId).prompt('Read it.'));
    expect(second.gw.results().get('b1')?.text).toBe('<path>notes/live.txt</path>\n<content>\n1\twritten on VM A\n</content>');
    expect(snapshot(second.gw.results().get('b2')?.text).output_delta).toBe('live.txt\nagent\n');
    expect((await api.getSandbox(name))!.sessionId).not.toBe(vmA);
    expect(JSON.stringify([...second.gw.results().values()])).not.toContain(FILES_LOST_NOTE);
    expect(await leaseEpochs(w, sessionId)).toEqual([1, 2]);
    expect(new Set(fences)).toEqual(new Set([1, 2]));
  });
});

describe("fx's tools for libfx's agent on a sandbox: fx's definitions", () => {
  it("the same names and input schemas byte for byte, fx's descriptions with the reviewed edits, the shell's saying where it runs; none idempotent, so libfx stores each call's intent first", () => {
    const { api, acquire } = sandbox();
    const tools = agentTools(acquire, localWorld());
    expect(tools.map((t) => t.name)).toEqual([...FX_TOOL_NAMES]);
    for (const tool of tools) {
      expect(JSON.stringify(tool.inputSchema)).toBe(JSON.stringify(FX_SCHEMAS[tool.name]));
      expect(tool.description).toBe(tool.name === 'shell' ? `${fxDescription('shell')} ${SANDBOX_NOTE}` : fxDescription(tool.name));
      expect(tool.description).not.toContain(VIRTUAL_NOTE);
      // On a Drive a call cut off may have half-run: libfx never runs one again, and stores each call's intent before it runs it (which names the worker, and so the token).
      expect(tool.idempotent).toBe(false);
      expect(tool.writes ?? false).toBe(['edit_file', 'write_file', 'shell'].includes(tool.name));
      expect(isWorkspaceTool(tool)).toBe(true);
    }
    // Making them touches no Sandbox API.
    expect(api.calls).toEqual([]);
  });
});

describe("fx's tools for libfx's agent on a sandbox (the stand-in, world-local)", () => {
  it("answer as fx's do, in the session's VM, attached at the first call, armed with the epoch of the call's lease", { timeout: 120_000 }, async () => {
    const w = localWorld();
    const { api, acquire, fences } = sandbox();
    const { agent, gw } = agentOn(w, agentTools(acquire, w), [
      call('c1', 'write_file', { path: 'notes/today.txt', content: 'one\ntwo\nthree\n' }),
      call('c2', 'read_file', { path: 'notes/today.txt', start_line: 2, line_count: 1 }),
      call('c3', 'edit_file', { path: '/workspace/notes/today.txt', old_string: 'two', new_string: 'TWO' }),
      call('c4', 'glob_files', { pattern: '**/*.txt' }),
      call('c5', 'grep_files', { pattern: 'TWO' }),
      call('c6', 'shell', run('cat notes/today.txt; pwd; printenv | grep -c VITEST_ || true')),
      call('c7', 'write_file', { path: '/etc/passwd', content: 'x' }),
      call('c8', 'shell', run('echo partial > p.txt; exit 3')),
      call('c9', 'shell', { request: { action: 'interact', session_id: 'abc' } }),
      call('c10', 'edit_file', { path: 'notes/today.txt', old_string: 'nope', new_string: 'x' }),
      calls(['c11', 'read_file', { path: 'p.txt' }], ['c12', 'grep_files', { pattern: 'three' }], ['c13', 'glob_files', { pattern: '*.txt' }]),
      say('done'),
    ]);
    const turn = agent.session().prompt('write, read, edit, find and run');
    const sessionId = (await turn.accepted).sessionId;
    expect(await finish(turn)).toMatchObject({ stopReason: 'end_turn' });
    const r = gw.results();
    expect(r.get('c1')).toEqual({ text: 'wrote notes/today.txt (14 bytes)', isError: false });
    expect(r.get('c2')?.text).toBe('<path>notes/today.txt</path>\n<content>\n2\ttwo\n... [showing 1 of 3 lines; use start_line/line_count to read more.]\n</content>');
    expect(r.get('c3')?.text).toBe('edited notes/today.txt (14 bytes)');
    expect(r.get('c4')?.text).toBe('[glob] 1 matches for **/*.txt\n - notes/today.txt\n');
    expect(r.get('c5')?.text).toBe('[grep] 1 matches for TWO\n - notes/today.txt:2: TWO\n');
    // The VM's shell: the session's files at /workspace, none of this process's environment.
    expect(snapshot(r.get('c6')?.text)).toMatchObject({ session_id: null, state: 'completed', backend: 'sandbox', exit_code: 0, output_delta: 'one\nTWO\nthree\n/workspace\n0\n' });
    expect(r.get('c7')).toMatchObject({ isError: true, text: expect.stringContaining('outside the workspace') });
    // A command that exits non-zero ran: an error result, its change kept.
    expect(r.get('c8')?.isError).toBe(true);
    expect(snapshot(r.get('c8')?.text)).toMatchObject({ exit_code: 3, backend: 'sandbox' });
    expect(r.get('c9')).toMatchObject({ isError: true, text: expect.stringContaining('files are on /workspace') });
    expect(r.get('c10')).toMatchObject({ isError: true, text: expect.stringContaining('old_string not found in file') });
    // Reads in one response, side by side, on the same attach.
    expect(r.get('c11')?.text).toBe('<path>p.txt</path>\n<content>\n1\tpartial\n</content>');
    expect(r.get('c12')?.text).toBe('[grep] 1 matches for three\n - notes/today.txt:3: three\n');
    expect(r.get('c13')?.text).toBe('[glob] 2 matches for *.txt\n - notes/today.txt\n - p.txt\n');
    // The files are on the session's Drive; the World holds no file, only the sandbox's record.
    expect(onDrive(api, sessionId, 'notes/today.txt')).toBe('one\nTWO\nthree\n');
    expect(onDrive(api, sessionId, 'p.txt')).toBe('partial\n');
    const streams = await w.streams.list(sessionId);
    expect(streams).toContain(fxSandboxStreamName(sessionId));
    expect(streams).not.toContain(`${sessionId}_fxws`);
    // One VM, one attach, armed with the lease of the worker that ran the calls (the session's first: epoch 1).
    expect(await leaseEpochs(w, sessionId)).toEqual([1]);
    expect(fences.every((f) => f.count === 1 && f.fenced)).toBe(true);
    expect(fences.length).toBeGreaterThanOrEqual(1);
    expect(api.calls.filter((c) => c.startsWith('createSandbox:'))).toHaveLength(1);
  });

  it('a turn that runs no tool boots no sandbox and writes no record', { timeout: 60_000 }, async () => {
    const w = localWorld();
    const { api, acquire } = sandbox();
    const { agent } = agentOn(w, agentTools(acquire, w), [say('Hi.')]);
    const turn = agent.session().prompt('Hello.');
    const sessionId = (await turn.accepted).sessionId;
    await finish(turn);
    expect(api.calls).toEqual([]);
    expect(await w.streams.list(sessionId)).not.toContain(fxSandboxStreamName(sessionId));
  });

  it('files last across VMs: written on VM A, VM A deleted, read on VM B over the same Drive by the next worker, whose token is higher', { timeout: 180_000 }, async () => {
    const w = localWorld();
    const a = sandbox();
    const first = agentOn(w, agentTools(a.acquire, w), [
      call('a1', 'write_file', { path: 'notes/a.txt', content: 'written on VM A\n' }),
      call('a2', 'shell', run('echo "$(cat notes/a.txt) and by its shell" > notes/shell.txt; cat notes/shell.txt')),
      say('done'),
    ]);
    const t1 = first.agent.session().prompt('Write the notes.');
    const sessionId = (await t1.accepted).sessionId;
    await finish(t1);
    expect(snapshot(first.gw.results().get('a2')?.text).output_delta).toBe('written on VM A and by its shell\n');
    await first.agent.close();
    const name = nameOf(sessionId);
    const vmA = a.api.vms.get(name)!.sessionId;
    const driveId = a.api.drives.get(name)!.info.driveId;

    // VM A goes (its relay, rootfs and cache dirs with it); the Drive stays. Another process's agent runs the next turn: its own factory.
    await (await a.api.getSandbox(name))!.delete();
    expect(a.api.vms.has(name)).toBe(false);
    const b = sandbox({ api: a.api });
    const toolsB = agentTools(b.acquire, w);
    const second = agentOn(w, toolsB, [call('b1', 'read_file', { path: 'notes/a.txt' }), call('b2', 'shell', run('cat notes/shell.txt; ls notes')), say('read them')]);
    const t2 = second.agent.session(sessionId).prompt('Read the notes.');
    await finish(t2);
    const rb = second.gw.results();
    expect(rb.get('b1')?.text).toBe('<path>notes/a.txt</path>\n<content>\n1\twritten on VM A\n</content>');
    expect(snapshot(rb.get('b2')?.text).output_delta).toBe('written on VM A and by its shell\na.txt\nshell.txt\n');
    // A new VM, the same Drive: no "files lost" note.
    expect(a.api.vms.get(name)!.sessionId).not.toBe(vmA);
    expect(a.api.drives.get(name)!.info.driveId).toBe(driveId);
    expect(JSON.stringify([...rb.values()])).not.toContain(FILES_LOST_NOTE);
    // Each worker armed its VM with its own lease's epoch: the later one higher.
    expect(await leaseEpochs(w, sessionId)).toEqual([1, 2]);
    expect(new Set(a.fences.map((f) => f.count))).toEqual(new Set([1]));
    expect(new Set(b.fences.map((f) => f.count))).toEqual(new Set([2]));

    // A call libfx holds no open intent for (an earlier turn's, behind its checkpoint, or one never made) runs nothing.
    const shellB = toolsB.find((t) => t.name === 'shell')!;
    for (const executionId of ['a2', 'never-called']) {
      expect(await shellB.execute(run('touch stale.txt'), { executionId, sessionId, signal: new AbortController().signal })).toMatchObject({ isError: true, text: FX_NO_INTENT });
    }
    expect(onDrive(a.api, sessionId, 'stale.txt')).toBeUndefined();
  });

  // libfx 0.0.14 runs one configuration per agent name on a store in a process, and routes a World's deliveries over its
  // agents by name, so this in-process stand-in for two servers no longer gets B the session. Open in docs/followups.md:
  // P0.12 across two processes (the fence it checks, FxSandbox's epochs, is unchanged; the test above covers the next
  // worker's higher token).
  it.skip("P0.12: a worker taken over mid-call runs nothing more: the next worker's token is higher, and the old one's call, when it goes on, is not run", { timeout: 180_000 }, async () => {
    const dataDir = join(base, `world-${worldCount++}`);
    const w = localWorld(dataDir);
    const a = sandbox();
    // Worker A's shell call is held at its start (after libfx stored its intent), as a process that froze there would be.
    let reached!: () => void;
    const atGate = new Promise<void>((r) => (reached = r));
    let open!: () => void;
    const gate = new Promise<void>((r) => (open = r));
    let staleResult: unknown;
    const toolsA = agentTools(a.acquire, w).map((t) =>
      t.name !== 'shell'
        ? t
        : {
            ...t,
            async execute(input: unknown, context: Parameters<typeof t.execute>[1]) {
              reached();
              await gate;
              staleResult = await t.execute(input, context);
              return staleResult as string;
            },
          }
    );
    const first = agentOn(w, toolsA, [call('p1', 'write_file', { path: 'a.txt', content: 'by A\n' }), call('p2', 'shell', run('touch stale.txt'))]);
    // On a failure, A is let go before it is closed.
    closers.push(() => open());
    const turn = first.agent.session().prompt('go');
    const sessionId = (await turn.accepted).sessionId;
    void turn.result.catch(() => {});
    await atGate;
    expect(new Set(a.fences.map((f) => f.count))).toEqual(new Set([1]));

    // Worker B (another server, which judges A gone) takes the session over and goes on with the turn on its own attach.
    // Its leases name another process (libfx judges a lease of its own process by whether that worker still runs).
    // Another server: its own World on the same store. In one process an agent name runs one configuration on a store,
    // so A lets its name go, its frozen call still in flight.
    const closingA = first.agent.close();
    const wB = localWorld(dataDir);
    const b = sandbox({ api: a.api });
    const gwB = gateway([call('q1', 'shell', run('cat a.txt; echo by B > b.txt')), say('ok')]);
    // The same app's store ('local'), named apart in errors.
    const durabilityB = fxWorld(wB, { name: 'other-server', store: 'local', livenessKnown: true, queueDurable: false, pollMs: 50, holderInfo: () => ({ pid: -1, host: 'other-server' }), alive: () => false });
    const agentB = createFxAgent({ durability: durabilityB, apiKey: 'test', model: 'test/model', fetch: gwB.fetch, modelCatalog: [], tools: agentTools(b.acquire, wB) as never });
    closers.push(() => agentB.close());
    expect(await finish(agentB.session(sessionId).resume())).toMatchObject({ stopReason: 'end_turn' });
    // A's call never answered: B tells the model it may have partly run, and never runs it again.
    expect(gwB.results().get('p2')).toMatchObject({ isError: true });
    expect(snapshot(gwB.results().get('q1')?.text).output_delta).toBe('by A\n');
    expect(await leaseEpochs(w, sessionId)).toEqual([1, 2]);
    expect(new Set(b.fences.map((f) => f.count))).toEqual(new Set([2]));

    // A wakes and its call goes on: its lease is no longer the session's, so nothing runs.
    open();
    await until(() => (staleResult !== undefined ? true : undefined));
    expect(staleResult).toMatchObject({ isError: true, text: FX_SUPERSEDED });
    expect(onDrive(a.api, sessionId, 'stale.txt')).toBeUndefined();
    expect(onDrive(a.api, sessionId, 'b.txt')).toBe('by B\n');
    await closingA;
  });

  it('a lost Drive: the first result after it says so, once; the record acknowledges it once libfx stored that result', { timeout: 180_000 }, async () => {
    const w = localWorld();
    const { api, acquire } = sandbox();
    const first = agentOn(w, agentTools(acquire, w), [call('k1', 'write_file', { path: 'keep.txt', content: 'kept\n' }), say('done')]);
    const t1 = first.agent.session().prompt('Keep it.');
    const sessionId = (await t1.accepted).sessionId;
    await finish(t1);
    await first.agent.close();
    const name = nameOf(sessionId);
    const lost = api.drives.get(name)!.info.driveId;
    // The VM and the Drive both go.
    await (await api.getSandbox(name))!.delete();
    api.dropDrive(name);
    const second = agentOn(w, agentTools(sandbox({ api }).acquire, w), [call('l1', 'shell', run("ls -A | awk 'END { print NR }'")), call('l2', 'read_file', { path: 'keep.txt' }), say('gone')]);
    await finish(second.agent.session(sessionId).prompt('Is it there?'));
    const r = second.gw.results();
    const [note, rest] = r.get('l1')!.text.split('\n\n');
    expect(note).toBe(FILES_LOST_NOTE);
    expect(snapshot(rest).output_delta).toBe('0\n');
    expect(r.get('l2')).toMatchObject({ isError: true, text: expect.stringContaining('FileNotFound') });
    expect(r.get('l2')!.text).not.toContain(FILES_LOST_NOTE);
    expect(api.drives.get(name)!.info.driveId).not.toBe(lost);
    await second.agent.close();
    // The next worker (another process): told already.
    const third = agentOn(w, agentTools(sandbox({ api }).acquire, w), [call('m1', 'shell', run('echo again')), say('ok')]);
    await finish(third.agent.session(sessionId).prompt('Again.'));
    expect(third.gw.results().get('m1')!.text).not.toContain(FILES_LOST_NOTE);
    expect(snapshot(third.gw.results().get('m1')!.text).output_delta).toBe('again\n');
  });

  it('an attach that fails: the call is "not run", a definite outcome, nothing ran; the next call attaches and runs', { timeout: 120_000 }, async () => {
    const w = localWorld();
    const { api, acquire } = sandbox();
    // The arm's control-plane command fails for now (a 503): the attach yields `contended`.
    api.failRuns = { count: 1, error: () => Object.assign(new Error('Service Unavailable'), { response: { status: 503 } }) };
    const { agent, gw } = agentOn(w, agentTools(acquire, w), [call('f1', 'shell', run('echo first > first.txt')), call('f2', 'shell', run('echo second > second.txt; cat second.txt')), say('done')]);
    const turn = agent.session().prompt('run them');
    const sessionId = (await turn.accepted).sessionId;
    await finish(turn);
    const r = gw.results();
    expect(r.get('f1')).toEqual({ isError: true, text: 'not run: the sandbox is starting, try again' });
    expect(snapshot(r.get('f2')?.text).output_delta).toBe('second\n');
    expect(onDrive(api, sessionId, 'first.txt')).toBeUndefined();
    expect(onDrive(api, sessionId, 'second.txt')).toBe('second\n');
  });

  it("on memory() durability there is no lease to fence a sandbox with: each call says what to pass, and nothing boots", { timeout: 60_000 }, async () => {
    const w = localWorld();
    const { api, acquire } = sandbox();
    const gw = gateway([call('x1', 'write_file', { path: 'a.txt', content: 'A' }), say('done')]);
    const agent = createFxAgent({ durability: memory(), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: agentTools(acquire, w) as never });
    closers.push(() => agent.close());
    await finish(agent.session().prompt('go'));
    const result = gw.results().get('x1')!;
    expect(result.isError).toBe(true);
    expect(result.text).toContain('fxTools(sandbox(), { world })');
    expect(result.text).toContain('memory()');
    expect(api.calls).toEqual([]);
  });

  it('never a kernel built-in: a missing or throwing tool is an error, and the host is untouched', { timeout: 120_000 }, async () => {
    const w = localWorld();
    const { acquire } = sandbox();
    const hostDir = mkdtempSync(join(tmpdir(), 'fx-sandbox-host-'));
    closers.push(() => rmSync(hostDir, { recursive: true, force: true }));
    const hostFile = join(hostDir, 'pwned.txt');
    // fx's tools with write_file removed and shell's executor throwing: what an app that drops or breaks one registers.
    const broken = agentTools(acquire, w)
      .filter((t) => t.name !== 'write_file')
      .map((t) => (t.name === 'shell' ? { ...t, execute: async () => Promise.reject(new Error('shell is broken')) } : t));
    const { agent, gw } = agentOn(w, broken, [
      call('w1', 'write_file', { path: hostFile, content: 'pwned' }),
      call('w2', 'edit_file', { path: hostFile, old_string: 'a', new_string: 'b' }),
      call('s1', 'shell', run(`touch ${hostFile}`)),
      say('done'),
    ]);
    await finish(agent.session().prompt('go'));
    const r = gw.results();
    expect(r.get('w1')).toMatchObject({ isError: true });
    expect(r.get('w2')).toMatchObject({ isError: true, text: expect.stringContaining('outside the workspace') });
    expect(r.get('s1')).toMatchObject({ isError: true, text: expect.stringContaining('shell is broken') });
    expect(existsSync(hostFile)).toBe(false);
  });

  it('a crash mid-command: a fresh process continues the turn on a new VM over the same Drive; the command is never run again, and what it wrote before the cut stays', { timeout: 180_000 }, async () => {
    const dataDir = join(base, 'crash-world');
    const drive = { name: 'dh-fx-crash-session', dir: mkdtempSync(join(tmpdir(), 'dh-fx-crash-drive-')), driveId: 'drv_fxcrash1' };
    closers.push(() => rmSync(drive.dir, { recursive: true, force: true }));
    // Process A: libfx's agent runs a command that is still running when the process is SIGKILLed.
    const a = childProcess([dataDir, drive.name, drive.dir, drive.driveId]);
    const sessionId = (await a.next((l) => typeof l.sessionId === 'string')).sessionId as string;
    const vm = (await a.next((l) => l.type === 'vm')) as unknown as { relayPid: number; relayDir: string };
    await until(() => (existsSync(join(drive.dir, 'a.txt')) && existsSync(join(drive.dir, 'shell.pid')) ? true : undefined), 60_000);
    a.child.kill('SIGKILL');
    expect((await a.exited).signal).toBe('SIGKILL');
    // VM A goes with the process: its relay (and with it the helper, which kills the commands it runs), then the command's group, in case.
    kill(vm.relayPid);
    kill(-Number(readFileSync(join(drive.dir, 'shell.pid'), 'utf8').trim()));
    closers.push(() => rmSync(vm.relayDir, { recursive: true, force: true }));
    expect(readFileSync(join(drive.dir, 'a.txt'), 'utf8')).toBe('one\n');

    // Process B (this one), from the World alone, on a new VM over the same Drive.
    const b = seededStandIn(drive);
    apis.push(b.api);
    const w = createWorld({ dataDir, recoverActiveRuns: false });
    closers.push(() => w.close?.());
    const { agent, gw } = agentOn(w, agentTools(b.acquire, w), [call('r1', 'read_file', { path: 'a.txt' }), call('r2', 'write_file', { path: 'a.txt', content: 'one\ntwo\n' }), say('done')]);
    expect(await finish(agent.session(sessionId).resume())).toMatchObject({ stopReason: 'end_turn' });
    const r = gw.results();
    // The cut-off call is answered as possibly run, never run again (non-idempotent: libfx stored its intent): one line, not two.
    expect(r.get('call_crash')?.isError).toBe(true);
    expect(r.get('call_crash')?.text).toMatch(/partly|interrupted|may have/i);
    expect(r.get('r1')?.text).toBe('<path>a.txt</path>\n<content>\n1\tone\n</content>');
    expect(r.get('r2')?.text).toBe('wrote a.txt (8 bytes)');
    expect(readFileSync(join(drive.dir, 'a.txt'), 'utf8')).toBe('one\ntwo\n');
    // Each process armed the VM with its own worker's lease: B's is higher than A's.
    expect(await leaseEpochs(w, sessionId)).toEqual([1, 2]);
  });
});

// ---------------------------------------------------------------------------
// The kernel's fx harness

/** The kernel fx harness on `world`, with `tools` and a scripted model. */
function serveKernel(world: LocalWorld, tools: HostTool[], script: Part[][]) {
  const gw = gateway(script);
  const sessions = durableSessions({ world, harness: fxHarness({ createFxEngine: createFxEngine as never, apiKey: 'test', model: 'test/model', tools, fetch: gw.fetch as typeof fetch }) });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  return { sessions, gw };
}

async function kernelTurn({ sessions }: ReturnType<typeof serveKernel>, prompt: string, sessionId?: string): Promise<{ sessionId: string; state: SessionState }> {
  let id = sessionId;
  let done = 0;
  if (id) {
    done = (await sessions.inspect(id)).effectOrder.filter((e) => e.name === 'checkpoint' && e.status === 'completed').length;
    await sessions.send(id, prompt);
  } else id = (await sessions.start({ prompt })).sessionId;
  const state = await until(async () => {
    const s = await sessions.inspect(id!);
    if (s.status === 'failed') throw new Error(`session failed: ${s.failure?.message}`);
    return s.effectOrder.filter((e) => e.name === 'checkpoint' && e.status === 'completed').length > done ? s : undefined;
  });
  return { sessionId: id, state };
}

const toolEffects = (s: SessionState) => s.effectOrder.filter((e) => e.name.startsWith('tool.'));
const attaches = (s: SessionState) => s.effectOrder.filter((e) => e.name === 'sandbox.attach');

describe("the kernel's fx harness on a sandbox (fxHostTools on the sandbox runtime, the stand-in)", () => {
  it("each call runs in the session's VM, attached once before the first call's intent with the drive's own fence; reads safe to rerun, the rest never", { timeout: 120_000 }, async () => {
    const w = localWorld();
    const { api, acquire } = sandbox();
    const tools = kernelTools(acquire);
    for (const t of tools) {
      expect(typeof (t as PhasedTool).beforeIntent).toBe('function');
      expect(typeof (t as PhasedTool).afterDrive).toBe('function');
    }
    const served = serveKernel(w, tools, [
      call('k1', 'write_file', { path: 'a.txt', content: 'hello\n' }),
      call('k2', 'shell', run('echo world >> a.txt; cat a.txt')),
      call('k3', 'read_file', { path: 'a.txt' }),
      call('k4', 'edit_file', { path: 'a.txt', old_string: 'missing', new_string: 'x' }),
      say('done'),
    ]);
    const { sessionId, state } = await kernelTurn(served, 'write, run and read');
    const r = served.gw.results();
    expect(r.get('k1')?.text).toBe('wrote a.txt (6 bytes)');
    expect(snapshot(r.get('k2')?.text)).toMatchObject({ exit_code: 0, backend: 'sandbox', output_delta: 'hello\nworld\n' });
    expect(r.get('k3')?.text).toBe('<path>a.txt</path>\n<content>\n1\thello\n2\tworld\n</content>');
    expect(r.get('k4')).toMatchObject({ isError: true, text: expect.stringContaining('old_string not found in file') });
    expect(onDrive(api, sessionId, 'a.txt')).toBe('hello\nworld\n');
    expect(state.docs.has('hw.workspace')).toBe(false);
    expect(readDoc(state, SANDBOX_DOC).value).toMatchObject({ driveId: api.drives.get(nameOf(sessionId))!.info.driveId });
    const [attach] = attaches(state);
    expect(attaches(state)).toHaveLength(1);
    expect(attach).toMatchObject({ status: 'completed', output: { attached: true, fence: expect.any(Number), notes: 0 } });
    expect(state.effectOrder.indexOf(attach!)).toBeLessThan(state.effectOrder.indexOf(toolEffects(state)[0]!));
    expect(toolEffects(state).map((e) => [e.name, e.replay])).toEqual([
      ['tool.write_file', 'never'],
      ['tool.shell', 'never'],
      ['tool.read_file', 'safe'],
      ['tool.edit_file', 'never'],
    ]);
  });

  it('a turn that runs no tool boots no sandbox', { timeout: 60_000 }, async () => {
    const { api, acquire } = sandbox();
    const { state } = await kernelTurn(serveKernel(localWorld(), kernelTools(acquire), [say('Hi.')]), 'Hello.');
    expect(attaches(state)).toEqual([]);
    expect(api.calls).toEqual([]);
  });

  it('written on VM A, VM A deleted, read on VM B over the same Drive; the later drive armed with a higher token', { timeout: 180_000 }, async () => {
    const w = localWorld();
    const { api, acquire } = sandbox();
    const a = await kernelTurn(serveKernel(w, kernelTools(acquire), [call('a1', 'write_file', { path: 'notes/a.txt', content: 'written on VM A\n' }), say('done')]), 'Write it.');
    const name = nameOf(a.sessionId);
    const vmA = api.vms.get(name)!.sessionId;
    await (await api.getSandbox(name))!.delete();
    // Another process's harness and factory, on the same World and platform.
    const servedB = serveKernel(w, kernelTools(sandbox({ api }).acquire), [call('b1', 'read_file', { path: 'notes/a.txt' }), say('read it')]);
    const b = await kernelTurn(servedB, 'Read it.', a.sessionId);
    expect(servedB.gw.results().get('b1')?.text).toBe('<path>notes/a.txt</path>\n<content>\n1\twritten on VM A\n</content>');
    expect(api.vms.get(name)!.sessionId).not.toBe(vmA);
    const fences = attaches(b.state).map((e) => (e.output as { fence?: number }).fence!);
    expect(fences).toHaveLength(2);
    expect(fences[1]!).toBeGreaterThan(fences[0]!);
  });
});

/** Kill a process (or, negative, a process group) that may already be gone. */
function kill(pid: number): void {
  try {
    process.kill(pid, 'SIGKILL');
  } catch {}
}

/** test/sandbox-child.ts in a process of its own, whose stdout lines are JSON. */
function childProcess(args: string[]) {
  const child: ChildProcess = spawn(process.execPath, ['--import', 'tsx', CHILD, ...args], { cwd: PACKAGE, env: process.env, stdio: ['ignore', 'pipe', 'pipe'] });
  closers.push(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  });
  const lines: Record<string, unknown>[] = [];
  let stderr = '';
  child.stderr!.on('data', (d) => (stderr = (stderr + d).slice(-4000)));
  createInterface({ input: child.stdout! }).on('line', (l) => {
    try {
      lines.push(JSON.parse(l));
    } catch {}
  });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r) => child.once('exit', (code, signal) => r({ code, signal })));
  const next = (pred: (l: Record<string, unknown>) => boolean) =>
    until(() => {
      const hit = lines.find(pred);
      if (hit) return hit;
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`the child exited (${child.exitCode ?? child.signalCode}): ${stderr}`);
      return undefined;
    }, 90_000);
  return { child, next, exited, stderr: () => stderr };
}

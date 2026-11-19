import { type ChildProcess, spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { durableSessions, QUEUE_PREFIX, readDoc, type SessionState } from 'harness-world';
import { filesFromState } from 'harness-world-hands';
import { transcriptOf } from 'harness-world-process';
import { CODEX_CLI, devRelayArtifact, FILES_LOST_NOTE, prepareBase, SANDBOX_DOC, sandboxSession, sessionName, vercelSandboxApi } from 'harness-world-sandbox';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { COMMAND_ENV_KEYS, opencodeHarness, type OpenCodeHarnessOptions, type OpenCodeWorkspace } from '../src/index.ts';
import { SANDBOX_INSTRUCTIONS, SANDBOX_SHELL_NOTE } from '../src/sandbox.ts';
import { type HostAccess, type HostTrace, traceHost } from './host-access.ts';
import { scriptedGateway, type ScriptedGateway, text, toolCall, toolResults } from './scripted-gateway.ts';
import { FakeSandboxApi, relayBundle, seededStandIn, STAND_IN_PROJECT, standIn } from './stand-in.ts';

/**
 * B7 for OpenCode (docs/workspaces.md §11.4): `test/boot-and-tools.test.ts`'s
 * suite on a sandbox instead of `virtual()`. OpenCode 2.0.22 for real, booted
 * the way a Function boots it (in this process, from a directory with a
 * canary AGENTS.md, a planted `~/.claude/CLAUDE.md` and skills in HOME,
 * canary values in `process.env`, every `node:fs` and `node:child_process`
 * call of this process traced), a scripted model driving it, and its own
 * tools on the session's sandbox through the sandbox tier's real session
 * factory (`sandboxSession`, the kernel's `attachInDrive`: the `sandbox.attach`
 * fact with the drive's own fencing token read inside it, the `hw.sandbox`
 * record).
 *
 * The sandbox is the stand-in (§5.8, always on: `DH_TEST_LOCAL_SANDBOX=1`):
 * each "VM" a local relay child over temp dirs (`localMachine()`'s relay,
 * through harness-sandbox's fake Sandbox API), each "Drive" a temp dir that
 * outlives the VMs mounted on it, each machine's workspace seen at
 * /workspace (`test/stand-in.ts`). In the degraded tier (macOS, unprivileged
 * Linux) its commands run as this user on this machine, so what is checked
 * of the commands is what the sandbox tier promises there: their
 * environment is the relay's allowlist and the harness's, never this
 * process's. What OpenCode's own process does (this process) is checked as
 * on a virtual workspace: no process started but the stand-in's VMs, nothing
 * read or written outside the session's own directories, no canary and no
 * path of this machine in what the model sees.
 *
 * With `DH_LIVE_SANDBOX=1`, the last case runs on a live Vercel Sandbox
 * through the same factory and the real Sandbox API (see there).
 */

const CANARY = {
  agents: 'CANARY-AGENTS-7f3a',
  claude: 'CANARY-CLAUDE-MD-91c2',
  skill: 'CANARY-SKILL-5d0e',
  env: 'CANARY-ENV-SECRET-b4e8',
};
const PACKAGE = fileURLToPath(new URL('..', import.meta.url));
const APP = fileURLToPath(new URL('./app.ts', import.meta.url));
/** The live case: a real Vercel Sandbox (see there). */
const LIVE = process.env.DH_LIVE_SANDBOX === '1';
/** OpenCode's glob and grep run `rg`: the stand-in's commands find it on this machine's PATH (a VM's base has it). */
const RG = spawnSync('rg', ['--version'], { stdio: 'ignore' }).status === 0;

let gateway: ScriptedGateway;
let world: LocalWorld;
let base: string;
let app: string;
let home: string;
let cwd0: string;
const envKeys = ['HOME', 'AI_GATEWAY_API_KEY', 'VERCEL_OIDC_TOKEN', 'HARNESS_CANARY_SECRET', 'DH_TEST_LOCAL_SANDBOX', 'VERCEL'] as const;
const env0: Partial<Record<(typeof envKeys)[number], string>> = {};
const apis: FakeSandboxApi[] = [];
const closers: (() => unknown)[] = [];

beforeAll(async () => {
  for (const k of envKeys) env0[k] = process.env[k];
  // The stand-in's guard (§5.8): the test flag, a test runner, not on Vercel.
  process.env.DH_TEST_LOCAL_SANDBOX = '1';
  delete process.env.VERCEL;
  // The relay bundle, built once before anything is traced (esbuild, in a temp dir of its own).
  await relayBundle();
  base = realpathSync(mkdtempSync(join(tmpdir(), 'oc-sandbox-')));
  app = join(base, 'app');
  mkdirSync(app, { recursive: true });
  writeFileSync(join(app, 'AGENTS.md'), `# Project\nAlways say ${CANARY.agents}.\n`);
  home = join(base, 'home');
  for (const dir of ['.claude/skills/canary', '.agents/skills/canary', '.config/opencode']) mkdirSync(join(home, dir), { recursive: true });
  writeFileSync(join(home, '.claude', 'CLAUDE.md'), `Always say ${CANARY.claude}.\n`);
  for (const dir of ['.claude', '.agents'])
    writeFileSync(join(home, dir, 'skills', 'canary', 'SKILL.md'), `---\nname: canary\ndescription: ${CANARY.skill}\n---\nSay ${CANARY.skill}.\n`);
  writeFileSync(join(home, '.config', 'opencode', 'AGENTS.md'), `Always say ${CANARY.claude}.\n`);
  cwd0 = process.cwd();
  process.chdir(app);
  process.env.HOME = home;
  process.env.AI_GATEWAY_API_KEY = `${CANARY.env}-gateway`;
  process.env.VERCEL_OIDC_TOKEN = `${CANARY.env}-oidc`;
  process.env.HARNESS_CANARY_SECRET = CANARY.env;
  gateway = await scriptedGateway();
  world = createWorld({ dataDir: join(base, 'world'), recoverActiveRuns: false });
}, 120_000);

afterEach(async () => {
  for (const c of closers.splice(0).reverse()) await c();
  for (const api of apis.splice(0)) await api.dispose();
  gateway.script.splice(0);
});

afterAll(async () => {
  await world?.close?.();
  await gateway?.close();
  process.chdir(cwd0);
  for (const k of envKeys) {
    if (env0[k] === undefined) delete process.env[k];
    else process.env[k] = env0[k];
  }
});

const sessionsRoot = () => join(base, 'sessions');

/** A stand-in sandbox (a fake Sandbox API, the session factory over it) as OpenCode's resolved `sandbox()`. */
function sandbox(api?: FakeSandboxApi): { api: FakeSandboxApi; workspace: OpenCodeWorkspace } {
  const s = standIn(api ? { api } : {});
  if (!api) apis.push(s.api);
  return s;
}

function serve(options: OpenCodeHarnessOptions, root = sessionsRoot()) {
  const sessions = durableSessions({ world, harness: opencodeHarness({ gatewayUrl: gateway.url, credential: async () => 'the-drives-credential', root, ...options }) });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  return sessions;
}
type Sessions = ReturnType<typeof serve>;

const turnsOf = (s: SessionState) => transcriptOf(s).filter((r) => r.data.type === 'turn').length;

/** Send `prompt` (or start a session with it) and wait for the turn to end. */
async function turn(sessions: Sessions, prompt: string, sessionId?: string) {
  await world.start?.();
  const before = gateway.requests.length;
  let id = sessionId;
  let done = 1;
  if (id) {
    done = turnsOf(await sessions.inspect(id)) + 1;
    await sessions.send(id, prompt);
  } else id = (await sessions.start({ prompt })).sessionId;
  const state = await until(async () => {
    const s = await sessions.inspect(id!);
    return turnsOf(s) >= done || s.effectOrder.some((e) => e.name === 'unavailable' && (e.status === 'completed' || e.status === 'failed')) ? s : undefined;
  });
  return { sessionId: id, state, requests: gateway.requests.slice(before) };
}

async function until<T>(fn: () => Promise<T | undefined> | T | undefined, ms = 90_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v !== undefined) return v;
    if (Date.now() - start > ms) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 100));
  }
}

const forms = (dir: string) => {
  const abs = resolve(dir);
  try {
    return [...new Set([abs, realpathSync(abs)])];
  } catch {
    return [abs];
  }
};
const under = (p: string, roots: string[]) => roots.some((r) => p === r || p.startsWith(r + sep));
const CODE = [join(PACKAGE, '..', '..'), dirname(dirname(process.execPath))].flatMap(forms);
const libcProbe = (a: HostAccess) => a.path === '/proc/self/exe' && a.stack.includes('detect-libc');
/** The stand-in's own work on this machine: its "VMs" (relay children over temp dirs) and "Drives" (temp dirs), the platform a VM would be. */
const standInPlatform = (stack: string) => /harness-sandbox\/(test|src)\/|harness-opencode\/test\/stand-in\.ts/.test(stack);

/** Every host access of this process outside the session's own directories but those `boot-and-tools.test.ts` allows, and the stand-in platform's. */
function hostViolations(trace: HostTrace, allowed: string[]): HostAccess[] {
  const roots = allowed.flatMap(forms);
  const ancestor = (p: string) => roots.some((r) => r.startsWith(p.endsWith(sep) ? p : p + sep));
  return trace.paths.filter((a) => {
    if (a.path.startsWith('node:')) return false;
    const p = resolve(a.path);
    if (under(p, roots) || under(p, CODE)) return false;
    if (/\b(LOOP|gotStat)\b|node:fs:30\d\d/.test(a.stack) && ancestor(p)) return false;
    if (p === '/workspace' || p.startsWith('/workspace/')) return false;
    if (libcProbe(a)) return false;
    if (standInPlatform(a.stack)) return false;
    return true;
  });
}

const allText = (requests: unknown[]) => JSON.stringify(requests);
const toolNames = (body: any) => (body?.tools ?? []).map((t: { name: string }) => t.name).sort();
const toolDef = (body: any, name: string) => (body?.tools ?? []).find((t: { name: string }) => t.name === name) as { description: string; inputSchema: any };
const systemOf = (body: any) => (body?.prompt ?? []).filter((m: any) => m.role === 'system').map((m: any) => m.content).join('\n');
const tools = (state: SessionState) => state.effectOrder.filter((e) => e.name.startsWith('tool.'));
const attaches = (state: SessionState) => state.effectOrder.filter((e) => e.name === 'sandbox.attach');
/** The session's sandbox (and Drive) name on the stand-in's project. */
const nameOf = (sessionId: string) => sessionName(STAND_IN_PROJECT, sessionId);
const onDrive = (api: FakeSandboxApi, sessionId: string, path: string) => {
  const p = join(api.driveDir(nameOf(sessionId)), path);
  return existsSync(p) ? readFileSync(p, 'utf8') : undefined;
};

/**
 * What a command's environment may hold on the stand-in: the relay's base
 * (PATH, the session's HOME and TMPDIR in its cache dirs), the harness's
 * allowlist (`COMMAND_ENV_KEYS`), what bash sets itself, and on macOS the
 * text encoding the OS adds to every process it starts.
 */
const COMMAND_VARS = ['PATH', 'HOME', 'TMPDIR', 'PWD', 'OLDPWD', 'SHLVL', '_', ...COMMAND_ENV_KEYS, ...(process.platform === 'darwin' ? ['__CF_USER_TEXT_ENCODING'] : [])];

describe('OpenCode on a sandbox (boot and tools, on the stand-in)', () => {
  it("runs its own write, read, edit, grep, glob and shell in the session's sandbox, attached at its first call; nothing of this machine reaches the model or the commands", { timeout: 180_000 }, async () => {
    expect(existsSync('/workspace')).toBe(false);
    const { api, workspace } = sandbox();
    gateway.script.push(
      () => toolCall('c1', 'write', { path: 'src/hello.ts', content: 'export const hello = "world"\n' }),
      () => toolCall('c2', 'read', { path: 'src/hello.ts' }),
      () => toolCall('c3', 'edit', { path: 'src/hello.ts', oldString: 'world', newString: 'there' }),
      ...(RG ? [() => toolCall('c4', 'grep', { pattern: 'there' }), () => toolCall('c5', 'glob', { pattern: '**/*.ts' })] : []),
      () => toolCall('c6', 'shell', { command: 'cat src/hello.ts; env; echo "[$HARNESS_CANARY_SECRET][$AI_GATEWAY_API_KEY][$VERCEL_OIDC_TOKEN]"', background: true }),
      () => toolCall('c7', 'read', { path: '/etc/hosts' }),
      () => toolCall('c8', 'read', { path: join(app, 'AGENTS.md') }),
      () => toolCall('c9', 'shell', { command: 'seq 1 5000' }),
      () => text('done')
    );
    if (!RG) console.warn('[sandbox.test] rg is not on PATH here: OpenCode glob and grep are not exercised on the stand-in (a VM base has rg)');
    const trace = traceHost();
    let run: Awaited<ReturnType<typeof turn>>;
    try {
      run = await turn(serve({ workspace }), 'Do the work.');
    } finally {
      trace.stop();
    }
    const { state, requests, sessionId } = run;
    expect(requests).toHaveLength(RG ? 10 : 8);
    const r = toolResults(requests.at(-1));

    // The tools work: OpenCode's own code on the VM's files.
    expect(r.get('c1')).toBe('Created file successfully: src/hello.ts');
    expect(r.get('c2')).toBe('Read file src/hello.ts, lines 1-1\n1: export const hello = "world"');
    expect(r.get('c3')).toBe('Edited src/hello.ts (1 replacement)');
    if (RG) {
      expect(r.get('c4')).toContain('/workspace/src/hello.ts:\n  Line 1: export const hello = "there"');
      expect(r.get('c5')).toBe('/workspace/src/hello.ts');
    }
    const shell = r.get('c6')!;
    expect(shell).toContain('export const hello = "there"');
    // The commands' environment: the relay's base and the allowlist, nothing of this process (whose environment holds the canaries).
    const vars = shell.split('\n').filter((l) => /^[A-Za-z_][A-Za-z0-9_]*=/.test(l)).map((l) => l.slice(0, l.indexOf('=')));
    expect(vars).toEqual(expect.arrayContaining(['PATH', 'HOME', 'OPENCODE']));
    expect(vars.filter((v) => !COMMAND_VARS.includes(v))).toEqual([]);
    expect(shell).toContain('[][][]');
    expect(shell).not.toContain(CANARY.env);
    // Paths outside the workspace are refused before anything looks at them.
    for (const id of ['c7', 'c8']) expect(r.get(id)).toContain('Permission denied: external_directory');
    // A truncated result names no path of this machine.
    expect(r.get('c9')).toMatch(/\[showing lines \d+-5000 of 5000; the rest is not shown\]/);

    // What the model saw: OpenCode's tools only, edited for the sandbox, and no canary or path of this machine anywhere.
    const first = requests[0];
    expect(toolNames(first)).toEqual(['edit', 'glob', 'grep', 'read', 'shell', 'write']);
    const sh = toolDef(first, 'shell');
    expect(sh.description).toContain('Commands run on Linux using bash.');
    expect(sh.description).toContain(SANDBOX_SHELL_NOTE);
    expect(sh.description).not.toMatch(/macOS|zsh|background|saved to a file|virtual workspace/i);
    expect(Object.keys(sh.inputSchema.properties).sort()).toEqual(['command', 'timeout', 'workdir']);
    expect(systemOf(first)).toContain('Working directory: /workspace');
    expect(systemOf(first)).toContain('Platform: linux');
    expect(systemOf(first)).toContain(SANDBOX_INSTRUCTIONS);
    const seen = allText(requests);
    for (const canary of Object.values(CANARY)) expect(seen).not.toContain(canary);
    // No path of this machine but the one the test had the model ask for (c8), and c6's dump of the VM's own environment (its HOME and TMPDIR, cache dirs that are temp dirs of this machine on the stand-in), which holds none of the app's or the Function's.
    const told = allText([systemOf(first), first.tools, ...[...r].filter(([id]) => id !== 'c8' && id !== 'c6').map(([, t]) => t)]);
    for (const path of [app, home, base, tmpdir(), realpathSync(tmpdir())]) expect(told).not.toContain(path);
    for (const path of [app, home, base]) expect(shell).not.toContain(path);

    // Durable on the Drive, not transactional: the calls that change files or run commands are `never`, the others `safe`; nothing in the log's manifest.
    const calls = tools(state);
    expect(calls.map((e) => [e.name, e.replay])).toEqual([
      ['tool.write', 'never'],
      ['tool.read', 'safe'],
      ['tool.edit', 'never'],
      ...(RG
        ? [
            ['tool.grep', 'safe'],
            ['tool.glob', 'safe'],
          ]
        : []),
      ['tool.shell', 'never'],
      ['tool.read', 'safe'],
      ['tool.read', 'safe'],
      ['tool.shell', 'never'],
    ]);
    expect(filesFromState(state)).toEqual({});
    expect(onDrive(api, sessionId, 'src/hello.ts')).toBe('export const hello = "there"\n');
    // One attach for the drive, a fact before the first call's intent, armed with the drive's token; the record names the Drive.
    expect(attaches(state)).toHaveLength(1);
    expect(attaches(state)[0]).toMatchObject({ status: 'completed', replay: 'never', output: { attached: true } });
    expect((attaches(state)[0]!.output as { fence: unknown }).fence).toEqual(expect.any(Number));
    expect(state.effectOrder.findIndex((e) => e.name === 'sandbox.attach')).toBeLessThan(state.effectOrder.findIndex((e) => e.name.startsWith('tool.')));
    expect(readDoc(state, SANDBOX_DOC).value).toMatchObject({ sandboxName: nameOf(sessionId), driveId: api.drives.get(nameOf(sessionId))!.info.driveId });
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });

    // This machine: no process started but the stand-in's VM (its relay), nothing touched outside the session's own directories (and the stand-in's), nothing read where the canaries are.
    expect(trace.spawns.filter((s) => !standInPlatform(s.stack)).map((s) => `${s.method} ${s.command} :: ${s.stack}`)).toEqual([]);
    expect(trace.spawns.filter((s) => standInPlatform(s.stack))).toHaveLength(1);
    expect(trace.paths.filter((a) => /\b(which|isexe)\b/.test(a.stack)).map((a) => `${a.method} ${a.path}`)).toEqual([]);
    // The stand-in's own: its "VM" (a relay's temp dir, config and control socket) and its "Drive" (a temp dir), nothing else.
    expect(trace.paths.filter((a) => standInPlatform(a.stack) && !under(resolve(a.path), forms(tmpdir()))).map((a) => `${a.method} ${a.path}`)).toEqual([]);
    const violations = hostViolations(trace, [sessionsRoot(), join(base, 'world'), join(tmpdir(), 'harness-opencode')]);
    expect(violations.map((v) => `${v.method} ${v.path} :: ${v.stack}`)).toEqual([]);
    expect(trace.paths.filter((a) => !a.path.startsWith('node:') && !libcProbe(a) && under(resolve(a.path), [...forms(app), ...forms(home), '/etc', '/proc'])).map((a) => `${a.method} ${a.path} :: ${a.stack}`)).toEqual([]);
    await until(() => (readdirSync(join(sessionsRoot(), sessionId)).length ? undefined : true), 10_000);
    expect(existsSync('/workspace')).toBe(false);
  });

  it('a turn that runs no built-in call boots no sandbox', { timeout: 120_000 }, async () => {
    const { api, workspace } = sandbox();
    gateway.script.push(() => text('nothing to run'));
    const { state, requests } = await turn(serve({ workspace }), 'Just say hi.');
    expect(toolNames(requests[0])).toEqual(['edit', 'glob', 'grep', 'read', 'shell', 'write']);
    expect(attaches(state)).toEqual([]);
    expect(api.calls.filter((c) => /^create/.test(c))).toEqual([]);
    expect(api.vms.size).toBe(0);
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });
  });

  it('each call sees what the calls before it changed: a shell right after a write reads the file', { timeout: 120_000 }, async () => {
    const { workspace } = sandbox();
    gateway.script.push(
      () => toolCall('w1', 'write', { path: 'notes/next.txt', content: 'written just before\n' }),
      () => toolCall('w2', 'shell', { command: 'cat notes/next.txt' }),
      () => toolCall('w3', 'write', { path: 'notes/next.txt', content: 'and again\n' }),
      () => (RG ? toolCall('w4', 'grep', { pattern: 'again' }) : toolCall('w4', 'shell', { command: 'cat notes/next.txt' })),
      () => text('done')
    );
    const { requests } = await turn(serve({ workspace }), 'Write and read.');
    const r = toolResults(requests.at(-1));
    expect(r.get('w2')).toContain('written just before');
    expect(r.get('w4')).toContain('and again');
  });

  it('files last across VMs: written on VM A, VM A deleted, read on VM B over the same Drive', { timeout: 180_000 }, async () => {
    const { api, workspace } = sandbox();
    gateway.script.push(
      () => toolCall('a1', 'write', { path: 'notes/a.txt', content: 'written on VM A\n' }),
      () => toolCall('a2', 'shell', { command: 'echo "$(cat notes/a.txt) and by its shell" > notes/shell.txt; cat notes/shell.txt' }),
      () => text('done')
    );
    const a = await turn(serve({ workspace }), 'Write the notes.');
    expect(toolResults(a.requests.at(-1)).get('a2')).toContain('written on VM A and by its shell');
    const name = nameOf(a.sessionId);
    const vmA = api.vms.get(name)!.sessionId;
    const driveId = api.drives.get(name)!.info.driveId;

    // VM A goes (its relay, rootfs and cache dirs with it); the Drive stays. Another process drives the next turn: its own factory, its own root.
    await (await api.getSandbox(name))!.delete();
    expect(api.vms.has(name)).toBe(false);
    const b = sandbox(api);
    gateway.script.push(
      () => toolCall('b1', 'read', { path: 'notes/a.txt' }),
      () => toolCall('b2', 'shell', { command: 'cat notes/shell.txt; ls notes' }),
      () => text('read them')
    );
    const second = await turn(serve({ workspace: b.workspace }, join(base, 'sessions-b')), 'Read the notes.', a.sessionId);
    const r = toolResults(second.requests.at(-1));
    expect(r.get('b1')).toContain('1: written on VM A');
    expect(r.get('b2')).toContain('written on VM A and by its shell');
    expect(r.get('b2')).toContain('a.txt\nshell.txt');
    // A new VM, the same Drive: no "files lost" note, the record unchanged.
    expect(api.vms.get(name)!.sessionId).not.toBe(vmA);
    expect(api.drives.get(name)!.info.driveId).toBe(driveId);
    expect(JSON.stringify(second.requests)).not.toContain(FILES_LOST_NOTE);
    expect(readDoc(second.state, SANDBOX_DOC).value).toMatchObject({ driveId });
    expect(attaches(second.state).map((e) => e.status)).toEqual(['completed', 'completed']);
  });

  it('a lost Drive: the first call after it says so, once, and the record is told', { timeout: 180_000 }, async () => {
    const { api, workspace } = sandbox();
    gateway.script.push(() => toolCall('l1', 'write', { path: 'keep.txt', content: 'kept\n' }), () => text('done'));
    const sessions = serve({ workspace });
    const a = await turn(sessions, 'Keep it.');
    const name = nameOf(a.sessionId);
    const lost = api.drives.get(name)!.info.driveId;
    // The VM and the Drive both go.
    await (await api.getSandbox(name))!.delete();
    api.dropDrive(name);
    gateway.script.push(
      () => toolCall('l2', 'shell', { command: 'ls -A | wc -l' }),
      () => toolCall('l3', 'read', { path: 'keep.txt' }),
      () => text('gone')
    );
    const b = await turn(sessions, 'Is it there?', a.sessionId);
    const r = toolResults(b.requests.at(-1));
    expect(r.get('l2')!.startsWith(FILES_LOST_NOTE)).toBe(true);
    expect(r.get('l3')).toContain('File not found: keep.txt');
    expect(r.get('l3')).not.toContain(FILES_LOST_NOTE);
    const record = readDoc(b.state, SANDBOX_DOC).value;
    expect(record.driveId).not.toBe(lost);
    expect(record.filesLost).toBeUndefined();
    // The next turn: told already.
    gateway.script.push(() => toolCall('l4', 'shell', { command: 'echo again' }), () => text('ok'));
    const c = await turn(sessions, 'Again.', a.sessionId);
    expect(toolResults(c.requests.at(-1)).get('l4')).toBe('again\n');
  });

  it('an attach that fails: the call is "not run", a definite outcome, nothing ran; the next call attaches and runs', { timeout: 120_000 }, async () => {
    const { api, workspace } = sandbox();
    // The arm's control-plane command fails for now (a 503): the attach yields `contended`.
    api.failRuns = { count: 1, error: () => Object.assign(new Error('Service Unavailable'), { response: { status: 503 } }) };
    gateway.script.push(
      () => toolCall('n1', 'shell', { command: 'echo first > first.txt' }),
      () => toolCall('n2', 'shell', { command: 'echo second > second.txt; cat second.txt' }),
      () => text('done')
    );
    const { state, requests, sessionId } = await turn(serve({ workspace }), 'Run them.');
    const r = toolResults(requests.at(-1));
    expect(r.get('n1')).toContain('Not run: the sandbox is starting, try again');
    expect(r.get('n2')).toBe('second\n');
    const [n1, n2] = tools(state);
    expect(n1).toMatchObject({ name: 'tool.shell', status: 'failed', error: { message: 'not run: the sandbox is starting, try again' } });
    expect(n1!.error?.interrupted).toBeFalsy();
    expect(n2).toMatchObject({ name: 'tool.shell', status: 'completed' });
    expect(onDrive(api, sessionId, 'first.txt')).toBeUndefined();
    expect(onDrive(api, sessionId, 'second.txt')).toBe('second\n');
    expect(attaches(state).map((e) => e.output)).toEqual([
      { attached: false, yield: 'contended' },
      expect.objectContaining({ attached: true }),
    ]);
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });
  });

  it('builtins narrows them: only the listed tools are offered, a call to another is refused before it attaches', { timeout: 120_000 }, async () => {
    const { api, workspace } = sandbox();
    gateway.script.push(
      () => toolCall('b1', 'write', { path: 'x.txt', content: 'no' }),
      () => toolCall('b2', 'read', { path: 'x.txt' }),
      () => text('done')
    );
    const { state, requests, sessionId } = await turn(serve({ workspace, builtins: ['read'] }), 'Try.');
    expect(toolNames(requests[0])).toEqual(['read']);
    const r = toolResults(requests.at(-1));
    expect(r.get('b1')).toMatch(/not (currently )?available|No tool named/);
    expect(r.get('b2')).toContain('File not found: x.txt');
    expect(tools(state).map((e) => e.name)).toEqual(['tool.read']);
    expect(attaches(state)).toHaveLength(1);
    expect(onDrive(api, sessionId, 'x.txt')).toBeUndefined();
  });

  it('refuses to start on a machine that has a /workspace, and boots no sandbox', { timeout: 60_000 }, async () => {
    const { api, workspace } = sandbox();
    const { state, requests } = await turn(serve({ workspace, hostHas: (p) => p === '/workspace' }), 'Hello.');
    expect(requests).toEqual([]);
    const failed = state.effectOrder.find((e) => e.name === 'unavailable');
    expect(JSON.stringify(failed?.output)).toContain('refuses to start');
    expect(api.vms.size).toBe(0);
  });

  it('a crash mid-command: a fresh process resumes the turn from the log on a new VM over the same Drive; the command is never run again, and what it wrote before the cut stays', { timeout: 180_000 }, async () => {
    const dataDir = join(base, 'crash-world');
    const drive = { name: 'dh-crash-session', dir: mkdtempSync(join(tmpdir(), 'dh-crash-drive-')), driveId: 'drv_crash01' };
    closers.push(() => rmSync(drive.dir, { recursive: true, force: true }));
    // Process A: a command that runs long enough for the armed crash to kill the app mid-call.
    gateway.script.push(() => toolCall('k1', 'shell', { command: 'echo $$ > shell.pid; echo one > a.txt; sleep 30; echo two >> a.txt' }));
    const a = appProcess(dataDir, 'chaos-tool', 'Make a.txt.', {
      CRASH_AFTER_MS: '1500',
      SANDBOX: '1',
      SANDBOX_NAME: drive.name,
      SANDBOX_DRIVE_DIR: drive.dir,
      SANDBOX_DRIVE_ID: drive.driveId,
    });
    const exit = await Promise.race([a.exited, new Promise<never>((_, no) => setTimeout(() => no(new Error(`the app did not crash: ${a.stderr()}`)), 90_000))]);
    expect(exit.signal, a.stderr()).toBe('SIGKILL');
    const sessionId = a.lines[0].sessionId as string;
    expect(a.lines.some((l) => l.type === 'chaos' && l.effect === 'tool.shell')).toBe(true);
    // VM A goes with the app's process: its relay (and with it the helper, which kills the commands it runs), then the command's group, in case.
    const vm = a.lines.find((l) => l.type === 'vm') as { relayPid: number; relayDir: string };
    expect(vm).toBeDefined();
    kill(vm.relayPid);
    const pid = Number(readFileSync(join(drive.dir, 'shell.pid'), 'utf8').trim());
    kill(-pid);
    closers.push(() => rmSync(vm.relayDir, { recursive: true, force: true }));
    expect(readFileSync(join(drive.dir, 'a.txt'), 'utf8')).toBe('one\n');

    // Process B, from the log alone, on a new VM over the same Drive: OpenCode's restart sweep settles the call, the model goes on.
    gateway.script.push(
      () => toolCall('k2', 'read', { path: 'a.txt' }),
      () => toolCall('k3', 'write', { path: 'a.txt', content: 'one\ntwo\n' }),
      () => text('done')
    );
    const b = seededStandIn(drive);
    apis.push(b.api);
    const crashWorld = createWorld({ dataDir, recoverActiveRuns: true });
    closers.push(() => crashWorld.close?.());
    const sessions = durableSessions({
      world: crashWorld,
      harness: opencodeHarness({ workspace: b.workspace, gatewayUrl: gateway.url, credential: async () => 'k', root: join(base, 'crash-b') }),
      chaos: true,
      crash: () => {
        throw new Error('a chaos crash in the test runner');
      },
    });
    crashWorld.registerHandler(QUEUE_PREFIX, sessions.handler);
    await crashWorld.start?.();
    const state = await until(async () => {
      const s = await sessions.inspect(sessionId);
      return transcriptOf(s).at(-1)?.data.type === 'turn' ? s : undefined;
    });
    // The command's intent is in the log, and it never reported: `never`, never run again by itself.
    const shells = state.effectOrder.filter((e) => e.name === 'tool.shell');
    expect(shells).toHaveLength(1);
    expect(shells[0]).toMatchObject({ replay: 'never' });
    expect(shells[0]!.status).not.toBe('completed');
    // The model is told it may have partly run; the Drive kept what it wrote before the cut (durable, not transactional).
    const note = transcriptOf(state)
      .map((r) => r.data)
      .find((e) => e.type === 'message' && e.role === 'user' && (e as { synthetic?: string }).synthetic === 'recovery') as { text: string };
    expect(note.text).toContain('call k1: interrupted before it reported a result. It is not safe to repeat, and it may have taken effect');
    const k2 = toolResults(gateway.requests.at(-1)).get('k2')!;
    expect(k2).toContain('1: one');
    expect(k2).not.toContain('two');
    expect(readFileSync(join(drive.dir, 'a.txt'), 'utf8')).toBe('one\ntwo\n');
    // Each process attached with its drive's own token: B's is higher than A's.
    const fences = attaches(state).map((e) => (e.output as { fence?: number }).fence);
    expect(fences).toHaveLength(2);
    expect(fences[1]!).toBeGreaterThan(fences[0]!);
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });
  });
});

/**
 * The live case (`DH_LIVE_SANDBOX=1`): the same harness on a real Vercel
 * Sandbox, through the session factory and `@vercel/sandbox` (no stand-in, no
 * path view: the VM's Drive is at /workspace), with the drive's own fencing
 * token: a turn on VM A runs OpenCode's write and shell as the VM's
 * unprivileged user; VM A is deleted (the Drive stays); the next turn, on
 * VM B, reads the files. Run it from a directory linked to a Vercel project
 * (`DH_LIVE_SANDBOX_DIR`, default the directory the test started in), with the
 * Vercel CLI logged in: the SDK mints the linked project's development token
 * through `@vercel/oidc`, so HOME and `VERCEL_OIDC_TOKEN` are the real ones
 * for this case, not the canaries. Every sandbox, Drive and snapshot is named
 * `dh-probe-*` and tagged (harness-sandbox's `probes/sweep.ts` finds what a
 * killed run left), and `afterAll` deletes them. No secret is printed.
 */
describe.runIf(LIVE)('live: OpenCode on a Vercel Sandbox', () => {
  let lib: typeof import('../../harness-sandbox/probes/lib.ts');
  beforeAll(async () => {
    for (const k of ['HOME', 'VERCEL_OIDC_TOKEN', 'AI_GATEWAY_API_KEY'] as const) {
      if (env0[k] === undefined) delete process.env[k];
      else process.env[k] = env0[k];
    }
    process.chdir(process.env.DH_LIVE_SANDBOX_DIR ?? cwd0);
    lib = await import('../../harness-sandbox/probes/lib.ts');
  });
  // A Drive can be deleted only once it detached (6-20 s after its VM went): the cleanup retries for longer than a hook's default.
  afterAll(async () => {
    try {
      expect(await lib.cleanup()).toEqual([]);
    } finally {
      process.chdir(app);
      process.env.HOME = home;
      process.env.AI_GATEWAY_API_KEY = `${CANARY.env}-gateway`;
      process.env.VERCEL_OIDC_TOKEN = `${CANARY.env}-oidc`;
    }
  }, 240_000);

  it('a turn on VM A writes with OpenCode\'s own tools; VM A is deleted; the next turn, on VM B over the same Drive, reads them', { timeout: 15 * 60_000 }, async () => {
    const api = vercelSandboxApi();
    // A base with the relay and Codex's tarball (for its rg, OpenCode's glob and grep), built in a builder named for the sweep.
    const snapshot = await prepareBase({ region: 'iad1', relay: await devRelayArtifact(), clis: [CODEX_CLI], name: lib.track('sandbox', lib.probeName('b7ocbase')), tags: lib.TAGS });
    lib.created.snapshots.add(snapshot.snapshotId);
    const name = lib.probeName('b7oc');
    lib.track('sandbox', name);
    lib.track('drive', name);
    const factory = () =>
      sandboxSession({
        spec: { image: { snapshot: snapshot.snapshotId }, vcpus: 2, network: 'allow-all', idleMs: 60_000, driveSize: 1024 ** 3, budget: { vmSeconds: 3600 } },
        cliVersions: { codex: CODEX_CLI.version },
        tags: lib.TAGS,
        test: { name: () => name },
      });
    gateway.script.push(
      () => toolCall('v1', 'write', { path: 'notes/live.txt', content: 'written on VM A\n' }),
      () => toolCall('v2', 'shell', { command: 'cat /workspace/notes/live.txt; id -un; echo "[$AI_GATEWAY_API_KEY][$VERCEL_OIDC_TOKEN]"' }),
      () => toolCall('v3', 'grep', { pattern: 'VM A' }),
      () => text('done')
    );
    const a = await turn(serve({ workspace: { kind: 'sandbox', acquire: factory() } }, join(base, 'live-a')), 'Write it.');
    let r = toolResults(a.requests.at(-1));
    expect(r.get('v1')).toBe('Created file successfully: notes/live.txt');
    expect(r.get('v2')).toBe('written on VM A\nagent\n[][]\n');
    expect(r.get('v3')).toContain('/workspace/notes/live.txt:\n  Line 1: written on VM A');
    expect(attaches(a.state)).toEqual([expect.objectContaining({ status: 'completed', output: expect.objectContaining({ attached: true, fence: expect.any(Number) }) })]);
    const vmA = (await api.getSandbox(name))!.sessionId;

    // VM A goes; the Drive stays. Another factory (another process's) drives the next turn.
    await (await api.getSandbox(name))!.delete();
    gateway.script.push(
      () => toolCall('v4', 'read', { path: 'notes/live.txt' }),
      () => toolCall('v5', 'shell', { command: 'ls /workspace/notes; stat -c %U /workspace/notes/live.txt' }),
      () => text('read it')
    );
    const b = await turn(serve({ workspace: { kind: 'sandbox', acquire: factory() } }, join(base, 'live-b')), 'Read it.', a.sessionId);
    r = toolResults(b.requests.at(-1));
    expect(r.get('v4')).toContain('1: written on VM A');
    expect(r.get('v5')).toBe('live.txt\nagent\n');
    expect((await api.getSandbox(name))!.sessionId).not.toBe(vmA);
    expect(JSON.stringify(b.requests)).not.toContain(FILES_LOST_NOTE);
    const fences = attaches(b.state).map((e) => (e.output as { fence?: number }).fence);
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

/** The app's process (test/app.ts) on the scripted Gateway, whose stdout lines are JSON. */
function appProcess(dataDir: string, mode: string, arg: string, env: Record<string, string> = {}) {
  const child: ChildProcess = spawn(process.execPath, ['--import', 'tsx', APP, dataDir, join(base, 'crash-a'), mode, arg], {
    cwd: PACKAGE,
    env: { ...process.env, GATEWAY_URL: gateway.url, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  closers.push(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  });
  const lines: any[] = [];
  let stderr = '';
  child.stderr!.on('data', (d) => (stderr = (stderr + d).slice(-4000)));
  createInterface({ input: child.stdout! }).on('line', (l) => {
    try {
      lines.push(JSON.parse(l));
    } catch {}
  });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r) => child.once('exit', (code, signal) => r({ code, signal })));
  return { child, lines, exited, stderr: () => stderr };
}

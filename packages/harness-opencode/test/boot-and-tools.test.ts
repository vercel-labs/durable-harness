import { type ChildProcess, spawn } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { durableSessions, QUEUE_PREFIX } from 'harness-world';
import { filesFromState } from 'harness-world-hands';
import { EPHEMERAL_NOTE, VIRTUAL_NOTE, workspaceRuntime } from 'harness-world-hands/runtime';
import { transcriptOf } from 'harness-world-process';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { COMMAND_ENV_KEYS, opencodeHarness, type OpenCodeHarnessOptions } from '../src/index.ts';
import { type HostAccess, traceHost, type HostTrace } from './host-access.ts';
import { scriptedGateway, type ScriptedGateway, text, toolCall, toolResults } from './scripted-gateway.ts';

/**
 * OpenCode 2.0.22 for real, booted the way a Function boots it
 * (docs/workspaces.md §4.5): in this process, from a working directory that
 * holds a canary AGENTS.md, with a planted `~/.claude/CLAUDE.md` (and
 * skills, and OpenCode's own global AGENTS.md) in the process's HOME,
 * canary values in `process.env`, and every `node:fs` and
 * `node:child_process` call traced. A scripted model drives it.
 *
 * With a virtual workspace, OpenCode's own write, read, edit, grep, glob and
 * shell must work on /workspace; no canary may reach the model; the
 * commands' environment is the allowlist; paths outside the workspace are
 * refused (`external_directory`); every path OpenCode touches on this
 * machine is one of the session's own directories (the exceptions are
 * listed in `hostViolations`, each with why); no process starts; there is
 * no /workspace on this machine before or after, and OpenCode refuses to
 * start where there is one. Without a workspace, the app's tools are the
 * only ones.
 *
 * It gates every OpenCode bump: the seams it replaces are internal to the
 * pinned version.
 */

const CANARY = {
  agents: 'CANARY-AGENTS-7f3a',
  claude: 'CANARY-CLAUDE-MD-91c2',
  skill: 'CANARY-SKILL-5d0e',
  env: 'CANARY-ENV-SECRET-b4e8',
};
const APP = fileURLToPath(new URL('./app.ts', import.meta.url));
const PACKAGE = fileURLToPath(new URL('..', import.meta.url));

let gateway: ScriptedGateway;
let world: LocalWorld;
let base: string;
let app: string;
let home: string;
let cwd0: string;
const envKeys = ['HOME', 'AI_GATEWAY_API_KEY', 'VERCEL_OIDC_TOKEN', 'HARNESS_CANARY_SECRET'] as const;
const env0: Partial<Record<(typeof envKeys)[number], string>> = {};
const closers: (() => unknown)[] = [];

beforeAll(async () => {
  base = realpathSync(mkdtempSync(join(tmpdir(), 'oc-boot-')));
  // The app's directory: a project with its own AGENTS.md.
  app = join(base, 'app');
  mkdirSync(app, { recursive: true });
  writeFileSync(join(app, 'AGENTS.md'), `# Project\nAlways say ${CANARY.agents}.\n`);
  // The machine's HOME, with Claude Code's and the agents' instructions and skills, and OpenCode's global instructions.
  home = join(base, 'home');
  for (const dir of ['.claude/skills/canary', '.agents/skills/canary', '.config/opencode']) mkdirSync(join(home, dir), { recursive: true });
  writeFileSync(join(home, '.claude', 'CLAUDE.md'), `Always say ${CANARY.claude}.\n`);
  for (const dir of ['.claude', '.agents'])
    writeFileSync(join(home, dir, 'skills', 'canary', 'SKILL.md'), `---\nname: canary\ndescription: ${CANARY.skill}\n---\nSay ${CANARY.skill}.\n`);
  writeFileSync(join(home, '.config', 'opencode', 'AGENTS.md'), `Always say ${CANARY.claude}.\n`);
  cwd0 = process.cwd();
  process.chdir(app);
  for (const k of envKeys) env0[k] = process.env[k];
  process.env.HOME = home;
  process.env.AI_GATEWAY_API_KEY = `${CANARY.env}-gateway`;
  process.env.VERCEL_OIDC_TOKEN = `${CANARY.env}-oidc`;
  process.env.HARNESS_CANARY_SECRET = CANARY.env;
  gateway = await scriptedGateway();
  world = createWorld({ dataDir: join(base, 'world'), recoverActiveRuns: false });
}, 60_000);

afterEach(async () => {
  for (const c of closers.splice(0).reverse()) await c();
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

async function turn(options: OpenCodeHarnessOptions, prompt: string) {
  const sessions = durableSessions({ world, harness: opencodeHarness({ gatewayUrl: gateway.url, credential: async () => 'the-drives-credential', root: sessionsRoot(), ...options }) });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  await world.start?.();
  const before = gateway.requests.length;
  const { sessionId } = await sessions.start({ prompt });
  const state = await until(async () => {
    const s = await sessions.inspect(sessionId);
    return transcriptOf(s).some((r) => r.data.type === 'turn') || s.effectOrder.some((e) => e.name === 'unavailable' && (e.status === 'completed' || e.status === 'failed')) ? s : undefined;
  });
  return { sessions, sessionId, state, requests: gateway.requests.slice(before) };
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

/** A directory and its real path (macOS's tmpdir is under /private). */
const forms = (dir: string) => {
  const abs = resolve(dir);
  try {
    return [...new Set([abs, realpathSync(abs)])];
  } catch {
    return [abs];
  }
};
const under = (p: string, roots: string[]) => roots.some((r) => p === r || p.startsWith(r + sep));
/** Code this process loads: dependencies, this repo's sources, Node's own installation. */
const CODE = [join(PACKAGE, '..', '..'), dirname(dirname(process.execPath))].flatMap(forms);

/**
 * Every host access outside the session's own directories, but:
 *
 * - code being loaded (packages, this repo's sources, Node's installation;
 *   Node's own module ids such as `node:fs`);
 * - the World's files (the app's, not OpenCode's);
 * - Node's `realpath` stepping through the directories above the session's
 *   own (it lstats each component);
 * - paths under /workspace: a lookup of a workspace path that OpenCode
 *   still makes on this machine (its file locks resolve their paths), which
 *   finds nothing, because OpenCode refuses to start where /workspace exists
 *   (the harness's own check is one of these);
 * - on Linux, a native addon's loader reading `/proc/self/exe` to tell
 *   glibc from musl as the module loads (`detect-libc`, under
 *   `node-gyp-build`): code being loaded, picking its prebuilt binary; a
 *   fixed path, never one the model chose (`libcProbe`).
 */
/** A native addon's loader telling glibc from musl (see `hostViolations`). */
const libcProbe = (a: HostAccess) => a.path === '/proc/self/exe' && a.stack.includes('detect-libc');

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
    return true;
  });
}

const allText = (requests: unknown[]) => JSON.stringify(requests);
const toolNames = (body: any) => (body?.tools ?? []).map((t: { name: string }) => t.name).sort();
const toolDef = (body: any, name: string) => (body?.tools ?? []).find((t: { name: string }) => t.name === name) as { description: string; inputSchema: any };
const systemOf = (body: any) => (body?.prompt ?? []).filter((m: any) => m.role === 'system').map((m: any) => m.content).join('\n');

/** just-bash's own variables (the virtual shell's), which every command sees. */
const SHELL_VARS = ['HOME', 'PATH', 'IFS', 'OSTYPE', 'MACHTYPE', 'HOSTTYPE', 'HOSTNAME', 'PWD', 'OLDPWD', 'OPTIND', 'SHELLOPTS', 'BASHOPTS', '?'];

describe('OpenCode on a virtual workspace (boot and tools)', () => {
  it('runs its own write, read, edit, grep, glob and shell on /workspace; nothing of this machine reaches the model or is touched', { timeout: 180_000 }, async () => {
    expect(existsSync('/workspace')).toBe(false);
    gateway.script.push(
      () => toolCall('c1', 'write', { path: 'src/hello.ts', content: 'export const hello = "world"\n' }),
      () => toolCall('c2', 'read', { path: 'src/hello.ts' }),
      () => toolCall('c3', 'edit', { path: 'src/hello.ts', oldString: 'world', newString: 'there' }),
      () => toolCall('c4', 'grep', { pattern: 'there' }),
      () => toolCall('c5', 'glob', { pattern: '**/*.ts' }),
      () => toolCall('c6', 'shell', { command: 'cat src/hello.ts; env; echo "[$HARNESS_CANARY_SECRET][$AI_GATEWAY_API_KEY][$VERCEL_OIDC_TOKEN]"; ls /', background: true }),
      () => toolCall('c7', 'read', { path: '/etc/hosts' }),
      () => toolCall('c8', 'read', { path: join(app, 'AGENTS.md') }),
      () => toolCall('c9', 'shell', { command: `cat ~/.claude/CLAUDE.md ${home}/.claude/CLAUDE.md ${app}/AGENTS.md /proc/self/environ; ls ~` }),
      () => toolCall('c10', 'shell', { command: 'seq 1 5000' }),
      () => text('done')
    );
    const trace = traceHost();
    let run: Awaited<ReturnType<typeof turn>>;
    try {
      run = await turn({ workspace: { kind: 'virtual', runtime: workspaceRuntime({ kind: 'virtual', durable: true }) } }, 'Do the work.');
    } finally {
      trace.stop();
    }
    const { state, requests, sessionId } = run;
    expect(requests).toHaveLength(11);
    const r = toolResults(requests.at(-1));

    // The tools work, OpenCode's own code on the workspace's files.
    expect(r.get('c1')).toBe('Created file successfully: src/hello.ts');
    expect(r.get('c2')).toBe('Read file src/hello.ts, lines 1-1\n1: export const hello = "world"');
    expect(r.get('c3')).toBe('Edited src/hello.ts (1 replacement)');
    expect(r.get('c4')).toContain('/workspace/src/hello.ts:\n  Line 1: export const hello = "there"');
    expect(r.get('c5')).toBe('/workspace/src/hello.ts');
    const shell = r.get('c6')!;
    expect(shell).toContain('export const hello = "there"');
    // The commands' environment: the virtual shell's own variables and the allowlist, nothing of this process.
    const vars = shell.split('\n').filter((l) => /^[A-Za-z_?][A-Za-z0-9_]*=/.test(l)).map((l) => l.slice(0, l.indexOf('=')));
    expect(vars.length).toBeGreaterThan(5);
    expect(vars.filter((v) => !SHELL_VARS.includes(v) && !(COMMAND_ENV_KEYS as readonly string[]).includes(v))).toEqual([]);
    expect(shell).toContain('[][][]');
    expect(shell).toMatch(/\nbin\ndev\nproc\nusr\nworkspace\n/);
    // Paths outside the workspace are refused before anything looks at them.
    for (const id of ['c7', 'c8']) expect(r.get(id)).toContain('Permission denied: external_directory');
    // This machine's files are not the workspace's, even by absolute path.
    expect(r.get('c9')).toContain('No such file or directory');
    expect(r.get('c9')).not.toContain(CANARY.claude);
    // A truncated result names no path of this machine.
    expect(r.get('c10')).toMatch(/\[showing lines \d+-5000 of 5000; the rest is not shown\]/);

    // What the model saw: OpenCode's tools only, edited for the workspace, and no canary or path of this machine anywhere.
    const first = requests[0];
    expect(toolNames(first)).toEqual(['edit', 'glob', 'grep', 'read', 'shell', 'write']);
    const sh = toolDef(first, 'shell');
    expect(sh.description).toContain('Commands run on Linux using bash.');
    expect(sh.description).toContain(VIRTUAL_NOTE);
    expect(sh.description).not.toMatch(/macOS|zsh|background|saved to a file/i);
    expect(Object.keys(sh.inputSchema.properties).sort()).toEqual(['command', 'timeout', 'workdir']);
    expect(toolDef(first, 'read').description).toMatch(/^Read the contents of a file or directory\./);
    expect(systemOf(first)).toContain('Working directory: /workspace');
    expect(systemOf(first)).toContain('Platform: linux');
    expect(systemOf(first)).toContain('virtual workspace at /workspace');
    const seen = allText(requests);
    for (const canary of Object.values(CANARY)) expect(seen).not.toContain(canary);
    // No path of this machine but the ones the test itself had the model ask for (c8, c9).
    const told = allText([systemOf(first), first.tools, ...[...r].filter(([id]) => id !== 'c8' && id !== 'c9').map(([, t]) => t)]);
    for (const path of [app, home, base, tmpdir(), realpathSync(tmpdir())]) expect(told).not.toContain(path);

    // Durable: every call in the log, the file in the session's manifest.
    const calls = state.effectOrder.filter((e) => e.name.startsWith('tool.'));
    expect(calls.map((e) => e.name)).toEqual(['tool.write', 'tool.read', 'tool.edit', 'tool.grep', 'tool.glob', 'tool.shell', 'tool.read', 'tool.read', 'tool.shell', 'tool.shell']);
    expect(calls.every((e) => e.replay === 'safe')).toBe(true);
    expect(filesFromState(state)).toEqual({ '/workspace/src/hello.ts': 'export const hello = "there"\n' });
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });

    // This machine: nothing started, nothing read outside the session's own directories, the drive's files gone, still no /workspace.
    expect(trace.spawns).toEqual([]);
    // Nothing searched this machine's PATH (OpenCode's provider plugins that would, Azure's for `az`, are off).
    expect(trace.paths.filter((a) => /\b(which|isexe)\b/.test(a.stack)).map((a) => `${a.method} ${a.path}`)).toEqual([]);
    const violations = hostViolations(trace, [sessionsRoot(), join(base, 'world')]);
    expect(violations.map((v) => `${v.method} ${v.path} :: ${v.stack}`)).toEqual([]);
    expect(trace.paths.filter((a) => !a.path.startsWith('node:') && !libcProbe(a) && under(resolve(a.path), [...forms(app), ...forms(home), '/etc', '/proc'])).map((a) => `${a.method} ${a.path} :: ${a.stack}`)).toEqual([]);
    // The drive's own directory goes once the drive has ended (just after the turn's last record).
    await until(() => (readdirSync(join(sessionsRoot(), sessionId)).length ? undefined : true), 10_000);
    expect(existsSync('/workspace')).toBe(false);
  });

  it('each call sees what the calls before it changed: a shell right after a write reads the file', { timeout: 120_000 }, async () => {
    gateway.script.push(
      () => toolCall('w1', 'write', { path: 'notes/next.txt', content: 'written just before\n' }),
      () => toolCall('w2', 'shell', { command: 'cat notes/next.txt' }),
      () => toolCall('w3', 'write', { path: 'notes/next.txt', content: 'and again\n' }),
      () => toolCall('w4', 'grep', { pattern: 'again' }),
      () => text('done')
    );
    const { requests } = await turn({ workspace: { kind: 'virtual', runtime: workspaceRuntime({ kind: 'virtual', durable: true }) } }, 'Write and read.');
    const r = toolResults(requests.at(-1));
    expect(r.get('w2')).toContain('written just before');
    expect(r.get('w4')).toContain('Line 1: and again');
  });

  it('in memory (durable: false): the same tools, nothing in the log, each description saying how long its files last', { timeout: 120_000 }, async () => {
    gateway.script.push(
      () => toolCall('m1', 'write', { path: 'notes.txt', content: 'kept for this turn\n' }),
      () => toolCall('m2', 'shell', { command: 'cat notes.txt' }),
      () => text('done')
    );
    const runtime = workspaceRuntime({ kind: 'virtual', durable: false });
    const { state, requests, sessions, sessionId } = await turn({ workspace: { kind: 'virtual', runtime } }, 'Note it.');
    const r = toolResults(requests.at(-1));
    expect(r.get('m2')).toContain('kept for this turn');
    for (const name of ['write', 'read', 'shell']) expect(toolDef(requests[0], name).description).toContain(EPHEMERAL_NOTE);
    expect(systemOf(requests[0])).toContain('kept in memory');
    expect(filesFromState(state)).toEqual({});
    // The next turn starts with an empty workspace.
    gateway.script.push(() => toolCall('m3', 'read', { path: 'notes.txt' }), () => text('gone'));
    const before = gateway.requests.length;
    await sessions.send(sessionId, 'Read it again.');
    await until(async () => (transcriptOf(await sessions.inspect(sessionId)).filter((e) => e.data.type === 'turn').length >= 2 ? true : undefined));
    expect(toolResults(gateway.requests.at(-1)).get('m3')).toContain('File not found: notes.txt');
    expect(gateway.requests.length - before).toBe(2);
  });

  it('a call whose changes cannot be kept: the model hears the failure, not the result OpenCode had, and nothing is kept', { timeout: 120_000 }, async () => {
    gateway.script.push(
      () => toolCall('f1', 'write', { path: 'keep.txt', content: 'kept\n' }),
      () => toolCall('f2', 'shell', { command: 'for i in $(seq 1 4000); do echo x > f$i.txt; done; echo made' }),
      () => toolCall('f3', 'shell', { command: 'ls | wc -l' }),
      () => text('done')
    );
    const { state, requests } = await turn({ workspace: { kind: 'virtual', runtime: workspaceRuntime({ kind: 'virtual', durable: true }) } }, 'Fill it.');
    const r = toolResults(requests.at(-1));
    expect(r.get('f2')).toMatch(/^Error: the workspace is full/);
    expect(r.get('f3')!.trim()).toMatch(/^1\b/);
    expect(state.effects.get('tool:f2')).toMatchObject({ status: 'failed' });
    expect(filesFromState(state)).toEqual({ '/workspace/keep.txt': 'kept\n' });
  });

  it('builtins narrows them: only the listed tools are offered, a call to another is refused', { timeout: 120_000 }, async () => {
    gateway.script.push(
      () => toolCall('n1', 'write', { path: 'x.txt', content: 'no' }),
      () => toolCall('n2', 'glob', { pattern: '*' }),
      () => text('done')
    );
    const { state, requests } = await turn({ workspace: { kind: 'virtual', runtime: workspaceRuntime({ kind: 'virtual', durable: true }) }, builtins: ['read', 'glob'] }, 'Try.');
    expect(toolNames(requests[0])).toEqual(['glob', 'read']);
    expect(toolResults(requests.at(-1)).get('n1')).toMatch(/not (currently )?available|No tool named/);
    expect(filesFromState(state)).toEqual({});
    expect(state.effectOrder.filter((e) => e.name.startsWith('tool.')).map((e) => e.name)).toEqual(['tool.glob']);
  });

  it('refuses to start on a machine that has a /workspace', { timeout: 60_000 }, async () => {
    const { state, requests } = await turn({ workspace: { kind: 'virtual', runtime: workspaceRuntime() }, hostHas: (p) => p === '/workspace' }, 'Hello.');
    expect(requests).toEqual([]);
    const failed = state.effectOrder.find((e) => e.name === 'unavailable');
    expect(JSON.stringify(failed?.output)).toContain('refuses to start');
  });

  it('a crash mid-tool: a fresh process resumes the turn from the log; the cut-off call changed nothing', { timeout: 180_000 }, async () => {
    const dataDir = join(base, 'crash-world');
    // Process A: a shell call that runs long enough for the armed crash to kill the app mid-call.
    gateway.script.push(() => toolCall('k1', 'shell', { command: 'echo one > a.txt; sleep 8; echo two >> a.txt' }));
    const a = appProcess(dataDir, 'chaos-tool', 'Make a.txt.', { CRASH_AFTER_MS: '1000' });
    const exit = await Promise.race([a.exited, new Promise<never>((_, no) => setTimeout(() => no(new Error(`the app did not crash: ${a.stderr()}`)), 90_000))]);
    expect(exit.signal, a.stderr()).toBe('SIGKILL');
    const sessionId = a.lines[0].sessionId as string;
    expect(a.lines.some((l) => l.type === 'chaos' && l.effect === 'tool.shell')).toBe(true);

    // Process B, from the log alone: OpenCode's own restart sweep settles the call as interrupted, the model goes on.
    gateway.script.push(
      () => toolCall('k2', 'read', { path: 'a.txt' }),
      () => toolCall('k3', 'write', { path: 'a.txt', content: 'one\ntwo\n' }),
      () => text('done')
    );
    const crashWorld = createWorld({ dataDir, recoverActiveRuns: true });
    closers.push(() => crashWorld.close?.());
    const sessions = durableSessions({
      world: crashWorld,
      harness: opencodeHarness({ workspace: { kind: 'virtual', runtime: workspaceRuntime() }, gatewayUrl: gateway.url, credential: async () => 'k', root: join(base, 'crash-b') }),
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
    const shell = state.effectOrder.find((e) => e.name === 'tool.shell')!;
    // Its intent is in the log, and it never reported: it is open in the log (a `safe` call, nothing to settle), never rerun.
    expect(shell).toMatchObject({ replay: 'safe' });
    expect(shell.status).not.toBe('completed');
    expect(state.effectOrder.filter((e) => e.name === 'tool.shell')).toHaveLength(1);
    // The call was rolled back with the process: its first write never reached the log.
    expect(toolResults(gateway.requests.at(-1)).get('k2')).toContain('File not found: a.txt');
    const note = transcriptOf(state)
      .map((r) => r.data)
      .find((e) => e.type === 'message' && e.role === 'user' && (e as { synthetic?: string }).synthetic === 'recovery') as { text: string };
    expect(note.text).toContain('safe to repeat');
    expect(filesFromState(state)).toEqual({ '/workspace/a.txt': 'one\ntwo\n' });
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });
  });
});

/** The app's process (test/app.ts) on the scripted Gateway, whose stdout lines are JSON. */
function appProcess(dataDir: string, mode: string, arg: string, env: Record<string, string> = {}) {
  const child: ChildProcess = spawn(process.execPath, ['--import', 'tsx', APP, dataDir, join(base, 'crash-a'), mode, arg], {
    cwd: PACKAGE,
    env: { ...process.env, BUILTINS: '1', GATEWAY_URL: gateway.url, ...env },
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

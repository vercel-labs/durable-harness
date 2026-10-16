import { existsSync, readFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { hostname, tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { durableSessions, QUEUE_PREFIX } from 'harness-world';
import { createFxAgent, createFxEngine, type FxEngineEvent, memory } from 'libfx';
import { world as fxWorld } from 'libfx/durable-world';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { readWorkspace } from './files.ts';
import { FX_DESCRIPTIONS, FX_EDITS, FX_SCHEMAS, FX_SOURCE, FX_TOOL_NAMES, fxDescription } from './fx-specs.ts';
import { FX_MEMORY_NOTE, FX_TOOL_DEFS, fxAgentTools, fxHostTools, globMatches, readFxRecords, readFxWorkspace } from './fx-tools.ts';
import { createMachine, type VirtualMachine } from './isolation.ts';
import { EPHEMERAL_NOTE, VIRTUAL_NOTE } from './runtime.ts';
import { scripted, until } from './test-harness.ts';

/**
 * libfx's kernel tools on a virtual workspace (B5): the names and input
 * schemas are the pinned libfx kernel's, byte for byte; the descriptions are
 * fx's with the reviewed edits; the output formats are fx's. For libfx's own
 * agent, a durable workspace records each call by `executionId`, so a call
 * libfx runs again returns its first outcome and changes nothing twice; it
 * refuses a session with no World run (an agent on `memory()`); and a
 * missing or failing tool never falls back to one of the kernel's own
 * built-ins, before or after a restore under another tool set.
 */

const libfxDir = dirname(createRequire(import.meta.url).resolve('libfx'));
const addon = join(libfxDir, `libfx.${process.platform}-${process.arch}.node`);

// ---------------------------------------------------------------------------
// A scripted AI Gateway (the AI SDK stream parts libfx's kernel reads)

type Part = Record<string, unknown>;
const usage = { inputTokens: { total: 1 }, outputTokens: { total: 1 } };
const sse = (parts: Part[]) =>
  new Response([...parts.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
const calls = (...list: [id: string, name: string, input: unknown][]): Part[] => [
  ...list.map(([id, name, input]) => ({ type: 'tool-call', toolCallId: id, toolName: name, input: JSON.stringify(input) })),
  { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage },
];
const say = (text: string): Part[] => [
  { type: 'text-delta', id: 't', delta: text },
  { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage },
];
function gateway(script: Part[][]) {
  const bodies: string[] = [];
  const fetch = async (_url: string, init: RequestInit) => {
    if ((init.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
    bodies.push(String(init.body));
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

async function run(turn: { result: Promise<unknown>; [Symbol.asyncIterator](): AsyncIterator<unknown> }) {
  for await (const _ of turn as AsyncIterable<unknown>);
  return turn.result;
}

// Nothing in this file changes process.env. libfx's native code reads the environment
// through a pointer to the environment array that it took earlier; setting a variable
// that is not set yet can move that array and free the old one, and libfx's next lookup
// of a variable that is not set then reads freed memory: a SIGSEGV on a libfx thread, at
// any moment, that ends this file's process (and fails the whole vitest run).
const ENV_AT_LOAD = JSON.stringify(Object.entries(process.env).sort());
afterAll(() => {
  expect(JSON.stringify(Object.entries(process.env).sort()), 'this file must leave process.env as it found it (libfx)').toBe(ENV_AT_LOAD);
});

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});
async function localWorld(): Promise<LocalWorld> {
  const w = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-fx-')), recoverActiveRuns: false });
  worlds.push(w);
  return w;
}

/** libfx's durability on a world-local World, as the demo's `fxDurability()` builds it. */
function durabilityOn(w: LocalWorld) {
  const host = hostname();
  return fxWorld(w, {
    name: 'local',
    livenessKnown: true,
    queueDurable: false,
    pollMs: 50,
    holderInfo: () => ({ pid: process.pid, host }),
    alive: (lease) => (lease.host === host && lease.pid === process.pid ? true : null),
  });
}

// ---------------------------------------------------------------------------

describe('vendored from fx', () => {
  it("pins libfx's own build of the source commit", () => {
    const pkg = JSON.parse(readFileSync(join(libfxDir, 'package.json'), 'utf8')) as { version: string };
    expect(pkg.version).toBe(FX_SOURCE.libfx);
    // A dev build's version names the commit it is built from (a release's is its tag's).
    if (FX_SOURCE.libfx.includes('-dev.')) expect(FX_SOURCE.commit.startsWith(FX_SOURCE.libfx.split('.g').at(-1)!)).toBe(true);
    expect(FX_TOOL_DEFS.map((d) => d.name)).toEqual([...FX_TOOL_NAMES]);
  });

  it("every name, field, enum value and description is in the pinned kernel's native addon, byte for byte", () => {
    const binary = readFileSync(addon).toString('latin1');
    const has = (text: string) => binary.includes(Buffer.from(text, 'utf8').toString('latin1'));
    const strings = new Set<string>();
    const walk = (schema: any) => {
      for (const alternative of schema.oneOf ?? []) walk(alternative);
      for (const [key, prop] of Object.entries<any>(schema.properties ?? {})) {
        strings.add(key);
        if (prop.description) strings.add(prop.description);
        for (const value of prop.enum ?? []) strings.add(value);
        if (prop.properties || prop.oneOf) walk(prop);
      }
      for (const key of schema.required ?? []) strings.add(key);
    };
    for (const name of FX_TOOL_NAMES) {
      strings.add(name);
      strings.add(FX_DESCRIPTIONS[name]);
      walk(FX_SCHEMAS[name]);
    }
    const missing = [...strings].filter((s) => !has(s));
    expect(missing).toEqual([]);
    // The kernel's schema writer, whose key order the vendored schemas follow (model_tool_schema.zig).
    for (const fragment of ['{"oneOf":[', '{"type":"object","properties":{', ',"additionalProperties":', ',"required":[', ',"enum":[']) expect(has(fragment)).toBe(true);
  });

  it('the kernel advertises the tools exactly as fx defines them: name and schema byte for byte', async () => {
    // On Node the engine is the pinned native addon.
    const libfx = (await import('libfx')) as unknown as { getBackendInfo(): Promise<{ backend: string }> };
    expect((await libfx.getBackendInfo()).backend).toBe('native');
    const gw = gateway([say('hi')]);
    const tools = fxAgentTools({ kind: 'virtual', durable: false });
    const engine = await createFxEngine({ apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: tools as never });
    await run(engine.prompt('hello'));
    await engine.close();
    const body = gw.bodies[0]!;
    let last = -1;
    for (const name of FX_TOOL_NAMES) {
      const tool = tools.find((t) => t.name === name)!;
      const advertised = `{"type":"function","name":${JSON.stringify(name)},"description":${JSON.stringify(tool.description)},"inputSchema":${JSON.stringify(FX_SCHEMAS[name])}}`;
      const at = body.indexOf(advertised);
      expect(at, name).toBeGreaterThan(last);
      last = at;
    }
  });

  it("descriptions are fx's, with the reviewed edits and where the tools run", () => {
    for (const name of FX_TOOL_NAMES) {
      const text = fxDescription(name);
      expect(text).not.toMatch(/external|~\/|image|zsh|full_output_handle|startup files applied/);
      for (const edit of FX_EDITS[name]) {
        if (edit.to) expect(text).toContain(edit.to);
        expect(text).not.toContain(edit.from);
      }
    }
    const durable = fxAgentTools({ kind: 'virtual', durable: true }, { world: () => Promise.reject(new Error('unused')) });
    expect(durable.find((t) => t.name === 'shell')!.description.endsWith(VIRTUAL_NOTE)).toBe(true);
    expect(durable.find((t) => t.name === 'read_file')!.description).toBe(fxDescription('read_file'));
    const inMemory = fxAgentTools({ kind: 'virtual', durable: false });
    for (const tool of inMemory) expect(tool.description.endsWith(FX_MEMORY_NOTE)).toBe(true);
    const kernel = fxHostTools({ kind: 'virtual', durable: false });
    for (const tool of kernel) expect(tool.description.endsWith(EPHEMERAL_NOTE)).toBe(true);
    expect(durable.map((t) => [t.name, t.idempotent, t.writes ?? false])).toEqual([
      ['read_file', true, false],
      ['glob_files', true, false],
      ['grep_files', true, false],
      ['edit_file', true, true],
      ['write_file', true, true],
      ['shell', true, true],
    ]);
  });
});

describe("fx's tools on a machine", () => {
  let machine: VirtualMachine;
  const def = (name: string) => FX_TOOL_DEFS.find((d) => d.name === name)!;
  const call = (name: string, input: unknown) => def(name).run(input, machine);
  beforeAll(async () => {
    machine = await createMachine();
  });
  afterAll(() => machine.dispose());

  it("write_file, read_file and edit_file answer as fx's do", async () => {
    expect(await call('write_file', { path: 'notes/today.txt', content: 'one\ntwo\nthree\n' })).toEqual({ text: 'wrote notes/today.txt (14 bytes)' });
    expect(await call('write_file', { path: '/workspace/notes/today.txt', content: 'one\ntwo\nthree\n' })).toEqual({ text: 'No changes to notes/today.txt; it already contains the requested content' });
    expect((await call('read_file', { path: 'notes/today.txt' })).text).toBe('<path>notes/today.txt</path>\n<content>\n1\tone\n2\ttwo\n3\tthree\n</content>');
    expect((await call('read_file', { path: 'notes/today.txt', start_line: 2, line_count: 1 })).text).toBe(
      '<path>notes/today.txt</path>\n<content>\n2\ttwo\n... [showing 1 of 3 lines; use start_line/line_count to read more.]\n</content>'
    );
    expect((await call('read_file', { path: 'notes/today.txt', start_line: 9 })).text).toBe('<path>notes/today.txt</path>\n<content>\n... [start_line 9 is beyond end of file; total lines 3]\n</content>');
    await call('write_file', { path: 'ten.txt', content: Array.from({ length: 10 }, (_, i) => `l${i + 1}`).join('\n') });
    expect((await call('read_file', { path: 'ten.txt', start_line: 9 })).text).toBe(
      '<path>ten.txt</path>\n<content>\n9 \tl9\n10\tl10\n... [showing 2 of 10 lines; use start_line/line_count to read more.]\n</content>'
    );
    expect(await call('edit_file', { path: 'notes/today.txt', old_string: 'two', new_string: 'TWO' })).toEqual({ text: 'edited notes/today.txt (14 bytes)' });
    expect(await call('edit_file', { path: 'notes/today.txt', old_string: 'nope', new_string: 'x' })).toMatchObject({ isError: true, text: expect.stringContaining('old_string not found in file') });
    await call('write_file', { path: 'twice.txt', content: 'a a' });
    expect(await call('edit_file', { path: 'twice.txt', old_string: 'a', new_string: 'b' })).toMatchObject({ isError: true, text: 'edit_file failed: old_string is not unique (found 2 occurrences), provide more context' });
    const missing = await call('read_file', { path: 'missing.txt' });
    expect(missing.isError).toBe(true);
    expect(JSON.parse(missing.text)).toMatchObject({ error: { type: 'tool_execution_failed', tool_name: 'read_file', details: { error: 'FileNotFound' } } });
    await machine.writeFile('/workspace/bin.dat', new Uint8Array([0, 1, 2, 255]));
    expect((await call('read_file', { path: 'bin.dat' })).text).toBe('<path>bin.dat</path>\n<content>binary or non-utf8 file omitted (4 bytes)</content>');
  });

  it('refuses every path outside /workspace', async () => {
    for (const path of ['/etc/passwd', '../x', '~/x', '/workspace/../tmp/x', '/tmp/x']) {
      const result = await call('write_file', { path, content: 'x' });
      expect(result.isError, path).toBe(true);
      expect(result.text, path).toContain('outside the workspace');
    }
    expect((await call('read_file', { path: '/workspace/../etc/passwd' })).text).toContain('outside the workspace');
    expect((await call('glob_files', { pattern: '*', path: '/' })).text).toContain('outside the workspace');
  });

  it("glob_files and grep_files list as fx's do", async () => {
    await call('write_file', { path: 'src/a.ts', content: 'export const a = 1; // TODO\n' });
    await call('write_file', { path: 'src/core/b.ts', content: 'const b = 2;\n// todo later\nexport { b };\n' });
    expect((await call('glob_files', { pattern: '**/*.ts' })).text).toBe('[glob] 2 matches for **/*.ts\n - src/a.ts\n - src/core/b.ts\n');
    expect((await call('glob_files', { pattern: '*.ts', path: 'src/core' })).text).toBe('[glob] 1 matches for *.ts\n - src/core/b.ts\n');
    expect((await call('glob_files', { pattern: 'src/*.ts', mode: 'count' })).text).toBe('[glob] count 1 matches for src/*.ts\n');
    expect((await call('glob_files', { pattern: '*.md' })).text).toBe('[glob] no matches for *.md\n');
    expect((await call('grep_files', { pattern: 'TODO' })).text).toBe('[grep] 1 matches for TODO\n - src/a.ts:1: export const a = 1; // TODO\n');
    expect((await call('grep_files', { pattern: 'todo', case_insensitive: true, mode: 'files_with_matches' })).text).toBe('[grep] 2 files with matches for todo\n - src/a.ts\n - src/core/b.ts\n');
    expect((await call('grep_files', { pattern: 'todo', include: '*.ts', path: 'src/core', context_lines: 1 })).text).toBe(
      '[grep] 1 matches for todo\n   src/core/b.ts:1- const b = 2;\n - src/core/b.ts:2: // todo later\n   src/core/b.ts:3- export { b };\n'
    );
    expect((await call('grep_files', { pattern: 'o', mode: 'count' })).text).toMatch(/^\[grep\] count \d+ matching lines in \d+ files for o\n$/);
    expect((await call('grep_files', { pattern: 'const', head_limit: 1 })).text).toBe(
      '[grep] 1 matches for const (showing 1-1 of 2)\n - src/a.ts:1: export const a = 1; // TODO\n... more matches available; use offset 1 to continue\n'
    );
  });

  it("matches globs as fx's glob_pattern.zig does", () => {
    expect(globMatches('**/*.zig', 'main.zig')).toBe(true);
    expect(globMatches('**/*.zig', 'src/core/main.zig')).toBe(true);
    expect(globMatches('*.zig', 'src/main.zig')).toBe(true);
    expect(globMatches('file?.zig', 'dir/file1.zig')).toBe(true);
    expect(globMatches('file?.zig', 'dir/file12.zig')).toBe(false);
    expect(globMatches('src/*.zig', 'src/main.zig')).toBe(true);
    expect(globMatches('src/*.zig', 'src/core/main.zig')).toBe(false);
    expect(globMatches('src/*.zig', 'lib/src/main.zig')).toBe(false);
    expect(globMatches('src/**', 'src/core/main.zig')).toBe(true);
    expect(globMatches('src/**/main.zig', 'src/main.zig')).toBe(true);
    expect(globMatches('src/**/main.zig', 'lib/src/core/main.zig')).toBe(false);
  });

  it("shell runs in the virtual workspace and answers with fx's snapshot", async () => {
    // The host's own variables (vitest's, set in this process before the file loaded) never reach the shell.
    expect(process.env.VITEST_POOL_ID).toBeDefined();
    const result = await call('shell', { request: { action: 'run', command: 'echo hi > made.txt && cat made.txt && printenv | grep -c VITEST_ || true' } });
    const snapshot = JSON.parse(result.text);
    expect(result.isError).toBeUndefined();
    expect(snapshot).toMatchObject({ session_id: null, state: 'completed', exit_code: 0, output_terminal_safe: true, output_delta: 'hi\n0\n' });
    const failed = await call('shell', { request: { action: 'run', command: 'echo partial > p.txt; exit 3' } });
    expect(failed).toMatchObject({ isError: true, ran: true });
    expect(JSON.parse(failed.text).exit_code).toBe(3);
    const cwd = JSON.parse((await call('shell', { request: { action: 'run', command: 'pwd', cwd: 'src' } })).text);
    expect(cwd.output_delta).toBe('/workspace/src\n');
    const usage = await call('shell', { request: { action: 'run', command: 'echo "unterminated' } });
    expect(JSON.parse(usage.text).exit_code).not.toBe(0);
    for (const action of ['interact', 'stop']) {
      const r = await call('shell', { request: { action, session_id: 'abc' } });
      expect(r).toMatchObject({ isError: true, text: expect.stringContaining('files are on /workspace') });
      expect(r.ran).toBeUndefined();
    }
  });
});

describe('fxHostTools: the kernel fx harness', () => {
  it("each call's change commits with its outcome, in the session's hw.workspace", async () => {
    const world = await localWorld();
    const tools = fxHostTools();
    const sessions = durableSessions({ world, harness: scripted(tools) });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    const { sessionId: runId } = await sessions.start({
      prompt: JSON.stringify({
        seq: [
          { tool: 'write_file', input: { path: 'a.txt', content: 'hello\n' } },
          { tool: 'shell', input: { request: { action: 'run', command: 'echo world >> a.txt; exit 1' } } },
          { tool: 'edit_file', input: { path: 'a.txt', old_string: 'missing', new_string: 'x' } },
        ],
      }),
    });
    await until(async () => (await sessions.inspect(runId)).effectOrder.filter((e) => e.name.startsWith('tool.') && (e.status === 'completed' || e.status === 'failed')).length === 3, 'three calls');
    const state = await sessions.inspect(runId);
    const effects = state.effectOrder.filter((e) => e.name.startsWith('tool.'));
    expect(effects.map((e) => [e.name, e.status])).toEqual([
      ['tool.write_file', 'completed'],
      ['tool.shell', 'completed'],
      ['tool.edit_file', 'failed'],
    ]);
    expect(await readWorkspace(world, state)).toEqual({ '/workspace/a.txt': 'hello\nworld\n' });
  });
});

describe("fxAgentTools: libfx's own agent", () => {
  it('a durable workspace records each call by executionId: libfx running a call again changes nothing twice', async () => {
    const w = await localWorld();
    const tools = fxAgentTools({ kind: 'virtual', durable: true }, { world: () => w });
    const command = { request: { action: 'run', command: 'echo once >> log.txt && cat log.txt' } };
    const gw = gateway([calls(['c1', 'write_file', { path: 'a.txt', content: 'A\n' }]), calls(['c2', 'shell', command]), calls(['c3', 'read_file', { path: 'log.txt' }]), say('done')]);
    const agent = createFxAgent({ durability: durabilityOn(w), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: tools as never });
    try {
      const turn = agent.session().prompt('go');
      const sessionId = (await turn.accepted).sessionId;
      expect(await run(turn)).toMatchObject({ stopReason: 'end_turn' });
      const results = gw.results();
      expect(results.get('c1')?.text).toBe('wrote a.txt (2 bytes)');
      expect(JSON.parse(results.get('c2')!.text)).toMatchObject({ exit_code: 0, output_delta: 'once\n' });
      expect(results.get('c3')?.text).toBe('<path>log.txt</path>\n<content>\n1\tonce\n</content>');

      // libfx runs c2 again under the same executionId (its result never reached libfx's log): the first outcome, and no second line.
      const shell = tools.find((t) => t.name === 'shell')!;
      const again = await shell.execute(command, { executionId: 'c2', sessionId, signal: new AbortController().signal });
      expect(again).toBe(results.get('c2')!.text);
      // A new call (another executionId) does run: the record above was what stopped the rerun.
      const next = JSON.parse((await shell.execute(command, { executionId: 'c4', sessionId })) as string);
      expect(next.output_delta).toBe('once\nonce\n');

      const { files, records } = await readFxWorkspace(w, sessionId);
      expect(Object.fromEntries([...files].map(([p, f]) => [p, f.content]))).toEqual({ '/workspace/a.txt': 'A\n', '/workspace/log.txt': 'once\nonce\n' });
      // Reads record nothing; each changing call one record, of what it changed.
      expect(records.map((r) => [r.executionId, r.tool, Object.keys(r.files)])).toEqual([
        ['c1', 'write_file', ['/workspace/a.txt']],
        ['c2', 'shell', ['/workspace/log.txt']],
        ['c4', 'shell', ['/workspace/log.txt']],
      ]);

      // Another process (a new workspace object) rebuilds from the records alone.
      const fresh = fxAgentTools({ kind: 'virtual', durable: true }, { world: () => w });
      const read = fresh.find((t) => t.name === 'read_file')!;
      expect(await read.execute({ path: 'log.txt' }, { executionId: 'c5', sessionId })).toBe('<path>log.txt</path>\n<content>\n1\tonce\n2\tonce\n</content>');
      expect(await fresh.find((t) => t.name === 'shell')!.execute(command, { executionId: 'c2', sessionId })).toBe(results.get('c2')!.text);
      expect((await readFxRecords(w, sessionId)).records).toHaveLength(3);
    } finally {
      await agent.close();
    }
  });

  it('a large or binary file goes to the blob stream, and comes back byte for byte', async () => {
    const w = await localWorld();
    const tools = fxAgentTools({ kind: 'virtual', durable: true }, { world: () => w });
    const big = 'x'.repeat(10_000);
    const gw = gateway([
      calls(['c1', 'write_file', { path: 'big.txt', content: big }]),
      calls(['c2', 'shell', { request: { action: 'run', command: 'echo hello | gzip > bin.gz && chmod 755 bin.gz' } }]),
      say('done'),
    ]);
    const agent = createFxAgent({ durability: durabilityOn(w), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: tools as never });
    try {
      const turn = agent.session().prompt('go');
      const sessionId = (await turn.accepted).sessionId;
      await run(turn);
      const { files, records } = await readFxWorkspace(w, sessionId);
      expect(files.get('/workspace/big.txt')?.content).toBe(big);
      const gz = files.get('/workspace/bin.gz')?.content;
      expect(gz).toBeInstanceOf(Uint8Array);
      expect([...(gz as Uint8Array).subarray(0, 2)]).toEqual([0x1f, 0x8b]);
      expect(files.get('/workspace/bin.gz')?.mode).toBe(0o755);
      expect(records[0]!.files['/workspace/big.txt']).toMatchObject({ n: 10_000, h: expect.any(String) });
    } finally {
      await agent.close();
    }
  });

  it('refuses a session with no World run (an agent on memory()), with what to use instead', async () => {
    const w = await localWorld();
    const tools = fxAgentTools({ kind: 'virtual', durable: true }, { world: () => w });
    const gw = gateway([calls(['c1', 'write_file', { path: 'a.txt', content: 'A' }]), say('done')]);
    const agent = createFxAgent({ durability: memory(), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: tools as never });
    try {
      const turn = agent.session().prompt('go');
      const sessionId = (await turn.accepted).sessionId;
      await run(turn);
      const result = gw.results().get('c1')!;
      expect(result.text).toContain('virtual({ durable: false })');
      expect(result.text).toContain('memory()');
      await expect(tools.find((t) => t.name === 'write_file')!.execute({ path: 'a.txt', content: 'A' }, { executionId: 'x', sessionId })).rejects.toThrow(TypeError);
      expect(await w.streams.list(sessionId).catch(() => [])).toEqual([]);
    } finally {
      await agent.close();
    }
  });

  it('an in-memory workspace works on memory(): files last while this process runs the session', async () => {
    const tools = fxAgentTools({ kind: 'virtual', durable: false });
    const gw = gateway([calls(['c1', 'write_file', { path: 'a.txt', content: 'A\n' }]), calls(['c2', 'read_file', { path: 'a.txt' }]), say('done')]);
    const agent = createFxAgent({ durability: memory(), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: tools as never });
    try {
      await run(agent.session().prompt('go'));
      expect(gw.results().get('c2')?.text).toBe('<path>a.txt</path>\n<content>\n1\tA\n</content>');
    } finally {
      await agent.close();
    }
  });
});

describe('never a kernel built-in', () => {
  let hostDir: string;
  beforeAll(async () => {
    hostDir = await mkdtemp(join(tmpdir(), 'hw-fx-host-'));
  });
  afterAll(async () => {
    await rm(hostDir, { recursive: true, force: true });
  });

  /** fx's tools with write_file removed and shell's executor throwing: what an app that drops or breaks one registers. */
  const broken = () =>
    fxAgentTools({ kind: 'virtual', durable: false })
      .filter((t) => t.name !== 'write_file')
      .map((t) => (t.name === 'shell' ? { ...t, execute: async () => Promise.reject(new Error('shell is broken')) } : t));

  const attack = (hostFile: string) =>
    calls(
      ['w1', 'write_file', { path: hostFile, content: 'pwned' }],
      ['w2', 'edit_file', { path: hostFile, old_string: 'a', new_string: 'b' }],
      ['s1', 'shell', { request: { action: 'run', command: `touch ${hostFile}` } }]
    );

  function expectRefused(results: Map<string, { text: string; isError: boolean }>, hostFile: string) {
    expect(results.get('w1')).toMatchObject({ isError: true });
    expect(results.get('s1')).toMatchObject({ isError: true, text: expect.stringContaining('shell is broken') });
    expect(results.get('w2')).toMatchObject({ isError: true });
    expect(existsSync(hostFile)).toBe(false);
  }

  it('a missing or throwing tool is an error, and the host is untouched', async () => {
    const hostFile = join(hostDir, 'first.txt');
    const gw = gateway([attack(hostFile), say('done')]);
    const engine = await createFxEngine({ apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: broken() as never });
    await run(engine.prompt('go'));
    await engine.close();
    expectRefused(gw.results(), hostFile);
  });

  it('the same after a restore that reports checkpoint.mismatch', async () => {
    const hostFile = join(hostDir, 'second.txt');
    // A session saved under the full tool set...
    const first = gateway([say('saved')]);
    const before = await createFxEngine({ apiKey: 'test', model: 'test/model', fetch: first.fetch, modelCatalog: [], tools: fxAgentTools({ kind: 'virtual', durable: false }) as never });
    await run(before.prompt('hello'));
    const checkpoint = await before.checkpoint();
    await before.close();
    // ...restored under one without write_file and with a broken shell.
    const events: FxEngineEvent[] = [];
    const gw = gateway([attack(hostFile), say('done')]);
    const after = await createFxEngine({ apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], checkpoint, tools: broken() as never, onEvent: (e) => events.push(e) });
    await run(after.prompt('go'));
    await after.close();
    expect(events.find((e) => e.type === 'checkpoint.mismatch')).toMatchObject({ changed: expect.arrayContaining(['toolSchemaHash']) });
    expectRefused(gw.results(), hostFile);
  });
});

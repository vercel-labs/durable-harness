import { type ChildProcess, spawn } from 'node:child_process';
import { mkdtemp } from 'node:fs/promises';
import { hostname, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { readFxWorkspace } from 'harness-world-hands/fx';
import { createFxAgent, memory } from 'libfx';
import { world as fxWorld } from 'libfx/durable-world';
import { afterAll, afterEach, describe, expect, expectTypeOf, it } from 'vitest';
import { fxTools } from '../src/fx.ts';
import { sandbox, virtual } from '../src/workspace.ts';

/**
 * durable-harness/fx (docs/workspaces.md §3.3, §11.4 B5): `fxTools(virtual())`
 * for libfx's own agent records its calls in the World the agent's sessions
 * live in, which can be any World an app gives libfx (never `getWorld()`
 * unless it is that one); on `memory()` a durable workspace refuses, saying
 * what to use; and a process killed mid-call, before or after the call's
 * record, resumes in a new process with the change landing once.
 */

type Part = Record<string, unknown>;
const usage = { inputTokens: { total: 1 }, outputTokens: { total: 1 } };
const sse = (parts: Part[]) =>
  new Response([...parts.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
const toolCall = (id: string, name: string, input: unknown): Part[] => [
  { type: 'tool-call', toolCallId: id, toolName: name, input: JSON.stringify(input) },
  { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage },
];
const say = (text: string): Part[] => [
  { type: 'text-delta', id: 't', delta: text },
  { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage },
];
function gateway(script: Part[][]) {
  const bodies: string[] = [];
  return {
    bodies,
    fetch: async (_url: string, init: RequestInit) => {
      if ((init.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
      bodies.push(String(init.body));
      const next = script.shift();
      if (!next) throw new Error('unexpected model request');
      return sse(next);
    },
  };
}
const toolResults = (bodies: string[]) => {
  const out = new Map<string, string>();
  for (const body of bodies) {
    for (const m of (JSON.parse(body) as { prompt: { role: string; content: Record<string, any>[] }[] }).prompt) {
      if (m.role === 'tool') for (const part of m.content) out.set(part.toolCallId, String(part.output?.value));
    }
  }
  return out;
};

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

describe('fxTools()', () => {
  it("takes virtual() or sandbox() itself, and is libfx's six tools", () => {
    expect(() => fxTools({ ...virtual() } as never)).toThrow(/copy of a descriptor/);
    expect(() => fxTools({ ...sandbox() } as never)).toThrow(/copy of a descriptor/);
    const tools = fxTools(virtual());
    expect(tools.map((t) => t.name)).toEqual(['read_file', 'glob_files', 'grep_files', 'edit_file', 'write_file', 'shell']);
    expect(tools.every((t) => (t as unknown as Record<symbol, unknown>)[Symbol.for('durable-harness.workspace-tool')] === true)).toBe(true);
    expect(tools.every((t) => t.idempotent === true)).toBe(true);
    // On sandbox() the same six (harness-fx's test/sandbox.test.ts runs them, B7), none idempotent: libfx stores each call's intent first, whose lease arms the VM.
    const onSandbox = fxTools(sandbox());
    expect(onSandbox.map((t) => t.name)).toEqual(tools.map((t) => t.name));
    expect(onSandbox.every((t) => t.idempotent === false && (t as unknown as Record<symbol, unknown>)[Symbol.for('durable-harness.workspace-tool')] === true)).toBe(true);
    expect(onSandbox.find((t) => t.name === 'shell')!.description).toMatch(/in this session's own sandbox, a Linux VM/);
    // Typed with their idempotent, as createFxAgent() requires of its tools (libfx's FxAgentHostTool).
    expectTypeOf<ReturnType<typeof fxTools>[number]['idempotent']>().toEqualTypeOf<boolean>();
  });

  it('records in the durable-world World the agent uses, not in getWorld()', async () => {
    const other = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-fx-other-')), recoverActiveRuns: false });
    worlds.push(other);
    const host = hostname();
    const gw = gateway([toolCall('c1', 'write_file', { path: 'notes.md', content: '# hi\n' }), toolCall('c2', 'read_file', { path: 'notes.md' }), say('done')]);
    const agent = createFxAgent({
      durability: fxWorld(other, { name: 'other', livenessKnown: true, alive: () => true, holderInfo: () => ({ pid: process.pid, host }), pollMs: 50 }),
      apiKey: 'test',
      model: 'test/model',
      fetch: gw.fetch,
      modelCatalog: [],
      tools: [...fxTools(virtual(), { world: other })],
    });
    try {
      const turn = agent.session().prompt('write notes');
      const sessionId = (await turn.accepted).sessionId;
      for await (const _ of turn);
      expect(await turn.result).toMatchObject({ stopReason: 'end_turn' });
      expect(toolResults(gw.bodies).get('c2')).toBe('<path>notes.md</path>\n<content>\n1\t# hi\n</content>');
      const { files } = await readFxWorkspace(other, sessionId);
      expect(Object.fromEntries([...files].map(([p, f]) => [p, f.content]))).toEqual({ '/workspace/notes.md': '# hi\n' });
      // Neither durable-harness's World nor the default one was ever made.
      expect((globalThis as Record<symbol, unknown>)[Symbol.for('durable-harness//world')]).toBeUndefined();
      expect((globalThis as Record<symbol, unknown>)[Symbol.for('durable-harness//fx-local-world')]).toBeUndefined();
    } finally {
      await agent.close();
    }
  });

  it('with memory() durability a durable workspace refuses (a TypeError), and virtual({ durable: false }) works', async () => {
    const other = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-fx-mem-')), recoverActiveRuns: false });
    worlds.push(other);
    const durable = fxTools(virtual(), { world: other });
    await expect(durable[4]!.execute({ path: 'a.txt', content: 'A' }, { executionId: 'c1', sessionId: 'ses_123', signal: new AbortController().signal })).rejects.toThrow(TypeError);
    // In an agent on memory(), the model gets the error as the call's result, saying what to use instead, and nothing is written.
    const refused = gateway([toolCall('c1', 'write_file', { path: 'a.txt', content: 'A\n' }), say('ok')]);
    const onMemory = createFxAgent({ durability: memory(), apiKey: 'test', model: 'test/model', fetch: refused.fetch, modelCatalog: [], tools: fxTools(virtual(), { world: other }) });
    try {
      const turn = onMemory.session().prompt('go');
      for await (const _ of turn);
      await turn.result;
      expect(toolResults(refused.bodies).get('c1')).toMatch(/virtual\(\{ durable: false \}\)/);
    } finally {
      await onMemory.close();
    }
    const gw = gateway([toolCall('c1', 'write_file', { path: 'a.txt', content: 'A\n' }), toolCall('c2', 'read_file', { path: 'a.txt' }), say('done')]);
    const agent = createFxAgent({ durability: memory(), apiKey: 'test', model: 'test/model', fetch: gw.fetch, modelCatalog: [], tools: fxTools(virtual({ durable: false })) });
    try {
      const turn = agent.session().prompt('go');
      for await (const _ of turn);
      await turn.result;
      expect(toolResults(gw.bodies).get('c2')).toBe('<path>a.txt</path>\n<content>\n1\tA\n</content>');
    } finally {
      await agent.close();
    }
  });
});

const CHILD = new URL('./fx-sigkill-child.ts', import.meta.url).pathname;
const children = new Set<ChildProcess>();
afterAll(() => {
  for (const c of children) c.kill('SIGKILL');
});

function child(args: string[]) {
  const proc = spawn(process.execPath, ['--import', 'tsx', CHILD, ...args], { cwd: new URL('..', import.meta.url).pathname, stdio: ['ignore', 'pipe', 'pipe'], env: process.env });
  children.add(proc);
  let out = '';
  let err = '';
  proc.stdout!.on('data', (d) => (out += d));
  proc.stderr!.on('data', (d) => (err = (err + d).slice(-4000)));
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r) => proc.once('exit', (code, signal) => (children.delete(proc), r({ code, signal }))));
  const next = async (pred: (l: Record<string, any>) => boolean, timeoutMs = 60_000) => {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const hit = out
        .split('\n')
        .filter((l) => l.startsWith('{'))
        .map((l) => JSON.parse(l) as Record<string, any>)
        .find(pred);
      if (hit) return hit;
      if (proc.exitCode !== null || proc.signalCode !== null) throw new Error(`child exited (${proc.exitCode ?? proc.signalCode}): ${err}`);
      if (Date.now() > deadline) throw new Error(`timed out: ${err}`);
      await new Promise((r) => setTimeout(r, 50));
    }
  };
  return { proc, next, exit };
}

describe('SIGKILL in the middle of an fx tool call', () => {
  async function drill(afterRecord: boolean) {
    const dataDir = await mkdtemp(join(tmpdir(), 'dh-fx-sigkill-'));
    const victim = child([dataDir, ...(afterRecord ? ['--after-record'] : [])]);
    const { sessionId, pid } = await victim.next((l) => typeof l.sessionId === 'string');
    await victim.next((l) => (afterRecord ? l.recorded === true : l.running === true));
    // Before the record: mid-command (it sleeps 4 s). After it: libfx has not heard how the call ended.
    if (!afterRecord) await new Promise((r) => setTimeout(r, 500));
    victim.proc.kill('SIGKILL');
    expect((await victim.exit).signal).toBe('SIGKILL');
    const recoverer = child([dataDir, '--recover', sessionId]);
    const report = await recoverer.next((l) => l.pid !== undefined && l.result !== undefined, 120_000);
    expect((await recoverer.exit).code).toBe(0);
    expect(report.pid).not.toBe(pid);
    return report as { result: { stopReason: string }; files: Record<string, string>; records: { executionId: string; tool: string; files: string[] }[] };
  }

  it('before its record: libfx runs it again, on the files as they were, and the change lands once', async () => {
    const r = await drill(false);
    expect(r.result.stopReason).toBe('end_turn');
    expect(r.files).toEqual({ '/workspace/log.txt': 'once\n' });
    expect(r.records).toEqual([{ executionId: 'call_1', tool: 'shell', files: ['/workspace/log.txt'] }]);
  }, 180_000);

  it('after its record: the rerun returns the recorded outcome, and runs nothing', async () => {
    const r = await drill(true);
    expect(r.result.stopReason).toBe('end_turn');
    expect(r.files).toEqual({ '/workspace/log.txt': 'once\n' });
    expect(r.records).toEqual([{ executionId: 'call_1', tool: 'shell', files: ['/workspace/log.txt'] }]);
  }, 180_000);
});

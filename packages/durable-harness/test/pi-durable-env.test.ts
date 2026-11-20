import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Context } from '@earendil-works/chord';
import { BACKGROUND_CONTEXT as ctx } from '@earendil-works/chord/context';
import { Type } from '@earendil-works/pi-ai';
import { createModels } from '@earendil-works/pi-ai/models';
import { type FauxResponseStep, fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness, type HarnessOptions, MemoryStorage, ROOT_CONVERSATION_ID, type Storage, type StorageWrite, type ToolRegistration } from '@earendil-works/pi-durable';
import { NodeExecutionEnv } from '@earendil-works/pi-durable/env/node';
import { registerEnvConformance } from '@earendil-works/pi-durable/testing';
import { CodingTools as PiCodingTools } from '@earendil-works/pi-durable/tools';
import { createMachine } from 'harness-world-hands/runtime';
import { MachineEnv } from 'pi-world/env';
import { deltaOf } from 'pi-world/files';
import { HOST_ENV_REFUSED, MOVED_NOTE } from 'pi-world/workspace';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import * as vitest from 'vitest';
import { CodingTools, MEMORY_NOTE, openWorldReader, openWorldStorage, sandboxEnv, virtualEnv } from '../src/pi-durable.ts';
import { virtual } from '../src/workspace.ts';
import { getWorld } from '../src/world.ts';

/**
 * pi-durable's own tools (`CodingTools`) on a virtual workspace
 * (`virtualEnv()`): the environment pi-durable builds for each call, the
 * call's file changes committed with its result, and the in-memory variant.
 */

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function until<T>(fn: () => Promise<T | undefined | false> | T | undefined | false, timeoutMs = 30_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined && value !== false) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await sleep(50);
  }
}

type Env = NonNullable<HarnessOptions['env']>;

/** A Pi app's own setup: a scripted faux model, `registry.install(...)` of `extensions`, and an opener with `env`. */
function piApp(answers: FauxResponseStep[], extensions: Parameters<ReturnType<typeof createRegistry>['install']>[0][], env?: Env, settings?: HarnessOptions['settings']) {
  const faux = fauxProvider();
  faux.setResponses(answers);
  const models = createModels();
  models.setProvider(faux.provider);
  const model = faux.getModel();
  const registry = createRegistry();
  for (const e of extensions) registry.install(e);
  const open = (storage: Storage) => Harness.open(storage, { models, registry, ...(env ? { env } : {}), ...(settings ? { settings } : {}) }, ctx);
  return { faux, open, agent: { model: { provider: model.provider, modelId: model.id }, instructions: 'Use the tools.' } };
}

/** One prompt, until its submission settles. */
async function ask(harness: Harness, agent: ReturnType<typeof piApp>['agent'], text: string): Promise<void> {
  const root = await harness.root(ctx, { agent });
  const submission = await root.submit({ type: 'input', content: text }, ctx);
  await submission.wait(ctx);
  await harness.waitForIdle(ctx);
}

const toolCall = (name: string, args: Parameters<typeof fauxToolCall>[1], id?: string) => fauxToolCall(name, args, id ? { id } : undefined);
const calls = (...c: ReturnType<typeof fauxToolCall>[]) => fauxAssistantMessage(c, { stopReason: 'toolUse' });
const done = (text = 'Done.') => fauxAssistantMessage([fauxText(text)]);

interface ResultEntry {
  name: string;
  text: string;
  isError: boolean;
  details: unknown;
}

/** The session's tool results, as the log has them (what the model saw, and their details). */
async function results(sessionId: string): Promise<ResultEntry[]> {
  const reader = openWorldReader(sessionId);
  await reader.refresh();
  const snap = await reader.snapshot();
  return snap.entries
    .filter((e) => e.kind === 'pi.tool-result')
    .map((e) => {
      const m = e.model?.[0] as { toolName: string; content: { type: string; text?: string }[]; isError: boolean; details?: unknown };
      return { name: m.toolName, text: m.content.map((c) => c.text ?? '').join(''), isError: m.isError, details: m.details };
    });
}

const filesOf = (r: ResultEntry) => Object.keys(deltaOf(r.details)?.files ?? {}).sort();

describe('virtualEnv(): pi-durable CodingTools on a virtual workspace (world-local)', () => {
  beforeAll(() => {
    process.env.WORKFLOW_TARGET_WORLD = 'local';
    process.env.WORKFLOW_LOCAL_DATA_DIR = mkdtempSync(join(tmpdir(), 'dh-pi-durable-env-'));
  });
  afterAll(async () => {
    await ((await getWorld()) as { close?: () => Promise<void> }).close?.();
  });

  it("P0.9: the call's Context reaches runtime.commit, but not Storage.commit; EnvTarget names no call", async () => {
    // The env builder's context per call, and the context of each Storage.commit that settles a tool call.
    const built: { conversationId: string; keys: string[]; context: Context; env: object }[] = [];
    const used: object[] = [];
    const settled: Context[] = [];
    class Recording extends MemoryStorage {
      override async commit(writes: readonly StorageWrite[], context: Context) {
        if (writes.some((w) => w.type === 'entry' && w.value.kind === 'pi.tool-result')) settled.push(context);
        return super.commit(writes, context);
      }
    }
    const echo = defineTool({
      name: 'echo',
      description: 'Echo.',
      parameters: Type.Object({ text: Type.String() }),
      execute: async (args: { text: string }, api) => (used.push(api.env as object), { content: [{ type: 'text', text: args.text }] }),
    }) as ToolRegistration;
    // An environment is built for each tool call and for each model request's preparation; tell them apart by which one a call got.
    const env: Env = (target, context) => {
      const made = { id: `probe:${built.length}` };
      built.push({ conversationId: String(target.conversationId), keys: Object.keys(target).sort(), context, env: made });
      return made as never;
    };
    const app = piApp([calls(toolCall('echo', { text: 'a' }), toolCall('echo', { text: 'b' })), done()], [defineExtension({ name: 'app', tools: [echo] })], env);
    const harness = await app.open(new Recording());
    await ask(harness, app.agent, 'echo twice');
    await harness.close(ctx);
    const forCalls = built.filter((b) => used.includes(b.env));
    expect(forCalls).toHaveLength(2);
    expect(settled).toHaveLength(2);
    // Nothing in the target says which call it is for.
    expect(forCalls[0]!.keys).toEqual(['conversationId', 'read']);
    // Each call's env context is its own invocation's (its abort signal); the Storage.commit of its result gets a
    // derived context with that signal replaced: neither identity nor the one per-call value reaches the storage.
    expect(forCalls[0]!.context.abortSignal).toBeInstanceOf(AbortSignal);
    expect(forCalls[0]!.context.abortSignal).not.toBe(forCalls[1]!.context.abortSignal);
    for (const c of settled) {
      expect(built.map((b) => b.context)).not.toContain(c);
      expect(c.abortSignal).toBeUndefined();
    }
  });

  it('commits each call’s file changes with its own result: two calls in parallel, then a read of both', async () => {
    const app = piApp(
      [
        calls(toolCall('write', { path: 'a.txt', content: 'alpha\n' }, 'w1'), toolCall('bash', { command: 'printf beta > b.txt && mkdir -p sub && printf gamma > sub/c.txt' }, 'b1')),
        calls(toolCall('bash', { command: 'cat a.txt b.txt sub/c.txt' }, 'b2')),
        done(),
      ],
      [CodingTools],
      virtualEnv()
    );
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    await ask(harness, app.agent, 'write two files at once');
    await harness.close(ctx);
    const [write, bash, cat] = await results(storage.sessionId);
    expect(write!.name).toBe('write');
    expect(write!.isError).toBe(false);
    expect(filesOf(write!)).toEqual(['/workspace/a.txt']);
    expect(bash!.name).toBe('bash');
    expect(filesOf(bash!)).toEqual(['/workspace/b.txt', '/workspace/sub/c.txt']);
    expect(cat!.text).toBe('alpha\nbetagamma');
    // A call that only read committed nothing.
    expect(deltaOf(cat!.details)).toBeUndefined();
    // The session's files are the fold of its results: a new storage (another process) has them.
    const again = await openWorldStorage(storage.sessionId);
    expect([...again.files.entries.keys()].sort()).toEqual(['/workspace/a.txt', '/workspace/b.txt', '/workspace/sub/c.txt']);
    await again.close(ctx);
  });

  it('a crash before the result loses the call’s changes: the recovered call is interrupted and its file is not there', async () => {
    const app = piApp(
      [calls(toolCall('write', { path: 'kept.txt', content: 'kept' }, 'w1')), calls(toolCall('bash', { command: 'printf lost > lost.txt && sleep 3' }, 'b1')), done('Crashed.')],
      [CodingTools],
      virtualEnv()
    );
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    const root = await harness.root(ctx, { agent: app.agent });
    await root.submit({ type: 'input', content: 'write then crash' }, ctx);
    // The bash call is running (its intent is in the log): the process dies (nothing more it does lands).
    await until(async () => {
      const reader = openWorldReader(storage.sessionId);
      await reader.refresh();
      const snap = await reader.snapshot();
      return snap.tasks.some((t) => t.kind === 'pi.tool' && t.status === 'running' && t.starts.length > 0 && JSON.stringify(t).includes('b1'));
    });
    await sleep(300);
    storage.abandon();

    // Another process resumes the session with the same opener.
    const app2 = piApp([done('Recovered.')], [CodingTools], virtualEnv());
    const storage2 = await openWorldStorage(storage.sessionId);
    const harness2 = await app2.open(storage2);
    harness2.resume();
    await harness2.waitForIdle(ctx);
    await harness2.close(ctx);
    const out = await results(storage.sessionId);
    const bash = out.find((r) => r.name === 'bash')!;
    expect(bash.isError).toBe(true);
    expect(bash.text).toContain('interrupted');
    expect(deltaOf(bash.details)).toBeUndefined();
    const reopened = await openWorldStorage(storage.sessionId);
    expect([...reopened.files.entries.keys()]).toEqual(['/workspace/kept.txt']);
    await reopened.close(ctx);
  }, 60_000);

  it('two sessions in one process get distinct env ids and distinct files', async () => {
    const ids: string[] = [];
    const whoami = defineTool({
      name: 'whoami',
      description: 'The workspace id.',
      parameters: Type.Object({}),
      execute: async (_args, api) => (ids.push(String(api.env?.id)), { content: [{ type: 'text', text: String(api.env?.id) }] }),
    }) as ToolRegistration;
    const run = async (file: string) => {
      const app = piApp(
        [calls(toolCall('whoami', {}), toolCall('write', { path: file, content: file })), calls(toolCall('bash', { command: 'ls' })), done()],
        [CodingTools, defineExtension({ name: 'app', tools: [whoami] })],
        virtualEnv()
      );
      const storage = await openWorldStorage();
      const harness = await app.open(storage);
      await ask(harness, app.agent, 'who');
      await harness.close(ctx);
      return { storage, out: await results(storage.sessionId) };
    };
    const [a, b] = await Promise.all([run('one.txt'), run('two.txt')]);
    expect(ids).toHaveLength(2);
    expect(new Set(ids).size).toBe(2);
    expect(ids.sort()).toEqual([`virtual:${a.storage.sessionId}`, `virtual:${b.storage.sessionId}`].sort());
    expect(a.out.find((r) => r.name === 'bash')!.text.trim()).toBe('one.txt');
    expect(b.out.find((r) => r.name === 'bash')!.text.trim()).toBe('two.txt');
  });

  it('virtual({ durable: false }): files for the turn, cleared when the next prompt starts one; the model hears so', async () => {
    const seen: string[] = [];
    const sees: FauxResponseStep = (context) => {
      seen.push(JSON.stringify(context.messages));
      return done('Noted.');
    };
    const app = piApp(
      [
        calls(toolCall('write', { path: 'note.txt', content: 'remember' })),
        calls(toolCall('bash', { command: 'cat note.txt' })),
        sees,
        calls(toolCall('bash', { command: 'cat note.txt' })),
        done('Gone.'),
      ],
      [CodingTools],
      virtualEnv(virtual({ durable: false }))
    );
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    await ask(harness, app.agent, 'turn one');
    await ask(harness, app.agent, 'turn two');
    await harness.close(ctx);
    const [write, cat1, cat2] = await results(storage.sessionId);
    // The first result of each turn says the files are in memory; later ones in the turn do not.
    expect(write!.text).toContain(`<harness>\n[info] ${MEMORY_NOTE}\n</harness>`);
    expect(cat1!.text).toBe('remember');
    expect(cat2!.text).toContain(MEMORY_NOTE);
    // The model's context carries the sentence.
    expect(seen[0]).toContain(MEMORY_NOTE);
    // A new turn (a new pi.user): the file is gone.
    expect(cat2!.isError).toBe(true);
    expect(cat2!.text).toContain('No such file');
    // Nothing of the files is in the log.
    expect(filesOf(write!)).toEqual([]);
    const again = await openWorldStorage(storage.sessionId);
    expect(again.files.entries.size).toBe(0);
    await again.close(ctx);
  });

  it('virtual({ durable: false }): a turn picked up by another invocation starts empty, and says its files are gone', async () => {
    const app = piApp([calls(toolCall('write', { path: 'a.txt', content: 'a' })), calls(toolCall('bash', { command: 'printf b > b.txt; sleep 3' }, 'b1')), done()], [CodingTools], virtualEnv({ durable: false }));
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    const root = await harness.root(ctx, { agent: app.agent });
    await root.submit({ type: 'input', content: 'go' }, ctx);
    await until(async () => (await results(storage.sessionId)).length === 1);
    await sleep(500);
    storage.abandon();
    const app2 = piApp([calls(toolCall('bash', { command: 'ls' })), done()], [CodingTools], virtualEnv({ durable: false }));
    const storage2 = await openWorldStorage(storage.sessionId);
    const harness2 = await app2.open(storage2);
    harness2.resume();
    await harness2.waitForIdle(ctx);
    await harness2.close(ctx);
    const out = await results(storage.sessionId);
    const ls = out.at(-1)!;
    expect(ls.name).toBe('bash');
    expect(ls.text).toContain(MOVED_NOTE);
    expect(ls.text).toContain(MEMORY_NOTE);
    expect(ls.text).not.toContain('a.txt');
  }, 60_000);

  it('a command past the workspace time limit fails and changes nothing', async () => {
    const app = piApp([calls(toolCall('bash', { command: 'printf x > t.txt; sleep 5' })), done()], [CodingTools], virtualEnv({ timeoutMs: 1000 }));
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    await ask(harness, app.agent, 'slow');
    await harness.close(ctx);
    const [bash] = await results(storage.sessionId);
    expect(bash!.isError).toBe(true);
    expect(bash!.text).toContain('time limit');
    expect(bash!.text).toContain('nothing this call changed was kept');
    expect(deltaOf(bash!.details)).toBeUndefined();
  }, 30_000);

  it('a command that exits nonzero keeps what it wrote, as on a disk; binary files and modes survive the log', async () => {
    const app = piApp(
      [
        calls(toolCall('bash', { command: "printf partial > p.txt; printf '#!/bin/sh\\necho hi\\n' > run.sh; chmod 755 run.sh; echo AP8B | base64 -d > bin.dat; exit 3" })),
        calls(toolCall('bash', { command: 'cat p.txt; echo; test -x run.sh && echo executable; base64 bin.dat' })),
        done(),
      ],
      [CodingTools],
      virtualEnv()
    );
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    await ask(harness, app.agent, 'fail');
    await harness.close(ctx);
    const [failed, check] = await results(storage.sessionId);
    expect(failed!.isError).toBe(true);
    expect(failed!.text).toContain('Command exited with code 3');
    expect(filesOf(failed!)).toEqual(['/workspace/bin.dat', '/workspace/p.txt', '/workspace/run.sh']);
    expect(check!.text).toContain('partial');
    expect(check!.text).toContain('executable');
    expect(check!.text).toContain('AP8B');
  });

  it('a file over 4 KB goes to the blob stream and comes back from it in another process', async () => {
    const big = 'line of text\n'.repeat(2000);
    const app = piApp([calls(toolCall('write', { path: 'big.txt', content: big })), done()], [CodingTools], virtualEnv());
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    await ask(harness, app.agent, 'big');
    await harness.close(ctx);
    const [write] = await results(storage.sessionId);
    const entry = deltaOf(write!.details)!.files!['/workspace/big.txt'] as { h: string; n: number };
    expect(entry.h).toMatch(/^[0-9a-f]{64}$/);
    expect(entry.n).toBe(big.length);
    const app2 = piApp([calls(toolCall('bash', { command: 'wc -l big.txt' })), done()], [CodingTools], virtualEnv());
    const storage2 = await openWorldStorage(storage.sessionId);
    const harness2 = await app2.open(storage2);
    await ask(harness2, app2.agent, 'count');
    await harness2.close(ctx);
    expect((await results(storage.sessionId)).at(-1)!.text).toContain('2000');
  });

  it("outside durable-harness's CodingTools the durable workspace is read-only, and a write names the import", async () => {
    const seed = piApp([calls(toolCall('write', { path: 'seed.txt', content: 'seeded' })), done()], [CodingTools], virtualEnv());
    const storage = await openWorldStorage();
    const h1 = await seed.open(storage);
    await ask(h1, seed.agent, 'seed');
    await h1.close(ctx);
    const app = piApp([calls(toolCall('read', { path: 'seed.txt' }), toolCall('write', { path: 'x.txt', content: 'x' })), done()], [PiCodingTools], virtualEnv());
    const storage2 = await openWorldStorage(storage.sessionId);
    const h2 = await app.open(storage2);
    await ask(h2, app.agent, 'try');
    await h2.close(ctx);
    const out = await results(storage.sessionId);
    const read = out.find((r) => r.name === 'read')!;
    const write = out.filter((r) => r.name === 'write').at(-1)!;
    expect(read.text).toBe('seeded');
    expect(write.isError).toBe(true);
    expect(write.text).toContain("import { CodingTools } from 'durable-harness/pi-durable'");
  });

  it('a durable workspace needs a World storage: on another storage a call says so', async () => {
    const app = piApp([calls(toolCall('bash', { command: 'echo hi' })), done()], [CodingTools], virtualEnv());
    const memory = new MemoryStorage();
    const harness = await app.open(memory);
    await ask(harness, app.agent, 'hi');
    const entries = await memory.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 100, undefined, ctx);
    await harness.close(ctx);
    const result = entries.items.find((e) => e.kind === 'pi.tool-result');
    expect(JSON.stringify(result?.model)).toContain('openWorldStorage()');
  });

  it('NodeExecutionEnv is refused on a Vercel Function, and the host disk stays untouched', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'dh-node-env-'));
    const app = piApp([calls(toolCall('write', { path: 'escape.txt', content: 'x' })), done()], [CodingTools], () => new NodeExecutionEnv({ cwd: dir }));
    const storage = await openWorldStorage();
    const harness = await app.open(storage);
    const before = process.env.VERCEL;
    process.env.VERCEL = '1';
    try {
      await ask(harness, app.agent, 'escape');
      await harness.close(ctx);
      const [write] = await results(storage.sessionId);
      expect(write!.isError).toBe(true);
      expect(write!.text).toContain(HOST_ENV_REFUSED);
      expect(existsSync(join(dir, 'escape.txt'))).toBe(false);
    } finally {
      if (before === undefined) delete process.env.VERCEL;
      else process.env.VERCEL = before;
    }
  });

  it('the descriptor forms: a copy is refused, the other environment is refused by name', () => {
    expect(() => virtualEnv({ ...virtual() } as never)).toThrow(/copy of a descriptor/);
    expect(() => virtualEnv({ store: 'blob' } as never)).toThrow(/store/);
    expect(() => sandboxEnv(virtual() as never)).toThrow(/virtualEnv\(\)/);
  });

  it("sandboxEnv() takes sandbox()'s full network policy and a per-session callback, and checks a static one when it is built", () => {
    const transform = { allow: { 'api.github.com': [{ transform: [{ headers: { authorization: 'Bearer x' } }] }] } };
    expect(() => sandboxEnv({ network: transform })).not.toThrow();
    expect(() => sandboxEnv({ network: async () => transform })).not.toThrow();
    expect(() => sandboxEnv({ network: { allow: { 'api.github.com': [{ transform: [] }] } } as never })).toThrow(/transform must be a non-empty list/);
  });
});

// pi-durable's own conformance suite, on the environment `virtualEnv()` gives each call (over a fresh machine per case).
registerEnvConformance(
  vitest,
  'MachineEnv on a virtual workspace (pi-durable env conformance)',
  async (use) => {
    const machine = await createMachine({ isolation: 'worker' });
    try {
      await use(new MachineEnv({ id: 'virtual:conformance', cwd: '/workspace', access: { use: (_mode, work) => work(machine) } }));
    } finally {
      machine.dispose();
    }
  }
);

import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseWorkflowName } from '@workflow/utils/parse-name';
import type { DriveContext, Harness } from 'harness-world';
import { QUEUE_PREFIX as KERNEL_QUEUE_PREFIX } from 'harness-world';
import { legacyWorkflowNameFor, workflowNameFor } from 'harness-world/names';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { kernelAgent } from '../src/agent.ts';
import { defineDoc, DocConflict, durableAgent, hostToolIO, type HostToolIO } from '../src/index.ts';
import { chatRoute as piChatRoute, resumeWith } from '../src/pi-durable.ts';
import { dispatch, MAX_UNOWNED_DELIVERIES, register, registered, UNOWNED_MAX_DELAY_SECONDS } from '../src/registry.ts';
import { QUEUE_PREFIX } from '../src/routing.ts';
import { docTools, readWorkspace, workspaceManifest, workspaceTools } from '../src/tools.ts';
import { getWorld } from '../src/world.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Per message: one `model` effect that streams `<name>:<text>` and answers. */
function echo(name: string): Harness {
  return {
    name,
    async drive(ctx: DriveContext) {
      let turn = ctx.state().effectOrder.length;
      for (const s of ctx.pending()) {
        await ctx.effect(`model:${turn++}`, { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => {
          ctx.live({ type: 'text', delta: `${name}:${s.text}` });
          await sleep(20);
          return 'ok';
        });
      }
      ctx.live({ type: 'idle' });
    },
  };
}

async function sse(response: Response): Promise<{ id?: string; event?: string; data?: unknown }[]> {
  const text = await response.text();
  return text
    .split('\n\n')
    .filter(Boolean)
    .map((block) => {
      const m: { id?: string; event?: string; data?: unknown } = {};
      for (const line of block.split('\n')) {
        if (line.startsWith('id: ')) m.id = line.slice(4);
        if (line.startsWith('event: ')) m.event = line.slice(7);
        if (line.startsWith('data: ')) m.data = JSON.parse(line.slice(6));
      }
      return m;
    });
}

const post = (body: unknown) => new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify(body), headers: { 'content-type': 'application/json' } });

describe('agents on the environment World', () => {
  beforeAll(() => {
    process.env.WORKFLOW_TARGET_WORLD = 'local';
    process.env.WORKFLOW_LOCAL_DATA_DIR = mkdtempSync(join(tmpdir(), 'durable-harness-'));
  });
  afterAll(async () => {
    await ((await getWorld()) as { close?: () => Promise<void> }).close?.();
  });

  it("spells the kernel queue prefix, durable-harness's own namespace", () => {
    expect(QUEUE_PREFIX).toBe(KERNEL_QUEUE_PREFIX);
    expect(QUEUE_PREFIX).toBe('__harness_wkf_workflow_');
  });

  it('serves each agent its own sessions over one in-process queue (no flow route)', async () => {
    const a = kernelAgent({ harness: echo('echo-a') });
    const b = kernelAgent({ harness: echo('echo-b') });
    for (const [agent, name] of [[a, 'echo-a'], [b, 'echo-b']] as const) {
      const route = agent.chatRoute({ drive: 'queue' });
      const res = await route.POST(post({ text: 'hi' }));
      expect(res.status).toBe(200);
      const events = await sse(res);
      expect(events.some((e) => (e.data as { delta?: string } | undefined)?.delta === `${name}:hi`)).toBe(true);
      expect(events.at(-1)?.event).toBe('end');
      const sessionId = res.headers.get('x-session-id')!;
      const state = await agent.sessions.inspect(sessionId);
      expect(state.effectOrder.map((e) => e.status)).toEqual(['completed']);
    }
  }, 30_000);

  it('drives a turn in the chat route itself, with a second message on the same session', async () => {
    const agent = kernelAgent({ harness: echo('echo-c'), durability: 'on' });
    const route = agent.chatRoute();
    const first = await route.POST(post({ text: 'one' }));
    const sessionId = first.headers.get('x-session-id')!;
    expect((await sse(first)).some((e) => (e.data as { delta?: string } | undefined)?.delta === 'echo-c:one')).toBe(true);
    const second = await route.POST(post({ sessionId, text: 'two' }));
    const events = await sse(second);
    const deltas = events.map((e) => (e.data as { delta?: string } | undefined)?.delta).filter(Boolean);
    expect(deltas).toEqual(['echo-c:two']);
    // Reconnecting with the end id says the turn is over.
    const end = events.find((e) => e.id?.endsWith(';end'))!.id!;
    const again = await route.GET(new Request(`http://local/api/chat?sessionId=${sessionId}`, { headers: { 'last-event-id': end } }));
    expect(again.status).toBe(204);
  }, 30_000);

  it('documents: a harness tool edits one with its outcome, the app reads and updates it on agent.sessions, and the chat route forwards only the listed kinds', async () => {
    const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
    const Private = defineDoc<{ note: string }>({ kind: 'app.private', version: 1, initial: () => ({ note: '' }) });
    const add = async (input: { item: string }, io: Pick<HostToolIO, 'docs'>) => {
      const todos = io.docs!.update(Todos, (d) => void d.items.push(input.item));
      io.docs!.update(Private, (d) => void (d.note = `saw ${input.item}`));
      return `added; ${todos.items.length} todos`;
    };
    const harness: Harness = {
      name: 'docs-agent',
      async drive(ctx) {
        let turn = ctx.state().effectOrder.length;
        for (const s of ctx.pending()) {
          await ctx.effect(`tool:${turn++}`, { name: 'tool.add_todo', replay: 'safe', input: s.text, placed: [s.id] }, (io) => add({ item: s.text! }, io));
        }
        ctx.live({ type: 'idle' });
      },
    };
    const agent = durableAgent({ harness });
    const route = agent.chatRoute({ drive: 'queue', docs: [Todos] });
    const first = await route.POST(post({ text: 'milk' }));
    const sessionId = first.headers.get('x-session-id')!;
    const events = (await sse(first)).map((e) => e.data as { type?: string; kind?: string; value?: unknown } | undefined);
    expect(events.filter((e) => e?.type === 'doc').map((e) => [e!.kind, e!.value])).toEqual([['app.todos', { items: ['milk'] }]]);
    expect(await agent.sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['milk'] }, revision: 1 });
    expect((await agent.sessions.doc(sessionId, Private)).value).toEqual({ note: 'saw milk' });
    // The app's own edit, with optimistic concurrency.
    expect(await agent.sessions.updateDoc(sessionId, Todos, (d) => void d.items.push('eggs'), { expectRevision: 1 })).toEqual({ value: { items: ['milk', 'eggs'] }, revision: 2 });
    await expect(agent.sessions.updateDoc(sessionId, Todos, (d) => void d.items.push('late'), { expectRevision: 1 })).rejects.toBeInstanceOf(DocConflict);
    expect((await agent.sessions.inspect(sessionId)).docs.get('app.todos')?.revision).toBe(2);
  }, 30_000);

  it('durable-harness/tools: the workspace tools keep their files in a document, and docTools show an app document to a model', async () => {
    const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
    const tools = [...workspaceTools(), ...docTools([Todos])];
    expect(tools.map((t) => t.name)).toEqual(['bash', 'read_file', 'write_file', 'edit_file', 'list_files', 'read_todos', 'update_todos']);
    // Each message is one tool call, as JSON: what a model would ask for.
    const harness: Harness = {
      name: 'tools-agent',
      async drive(ctx) {
        let turn = ctx.state().effectOrder.length;
        for (const s of ctx.pending()) {
          const { tool, input } = JSON.parse(s.text!) as { tool: string; input: unknown };
          const t = tools.find((x) => x.name === tool)!;
          await ctx.effect(`tool:${turn++}`, { name: `tool.${tool}`, replay: t.replay, input, placed: [s.id] }, (io) => t.execute(input, hostToolIO(io, ctx)));
        }
        ctx.live({ type: 'idle' });
      },
    };
    const agent = durableAgent({ harness });
    const route = agent.chatRoute({ drive: 'queue' });
    const send = async (sessionId: string | undefined, call: object) => {
      const res = await route.POST(post({ ...(sessionId ? { sessionId } : {}), text: JSON.stringify(call) }));
      await sse(res);
      return res.headers.get('x-session-id')!;
    };
    const big = `${'z'.repeat(5000)}\n`;
    const sessionId = await send(undefined, { tool: 'update_todos', input: { value: { items: ['milk'] }, expectRevision: 0 } });
    await send(sessionId, { tool: 'write_file', input: { path: 'notes.md', content: '# notes\n' } });
    await send(sessionId, { tool: 'write_file', input: { path: 'big.txt', content: big } });
    expect(await agent.sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['milk'] }, revision: 1 });
    const state = await agent.sessions.inspect(sessionId);
    expect(workspaceManifest(state).map((f) => [f.path, f.inline])).toEqual([
      ['/workspace/big.txt', false],
      ['/workspace/notes.md', true],
    ]);
    expect(await readWorkspace(await getWorld(), state)).toEqual({ '/workspace/big.txt': big, '/workspace/notes.md': '# notes\n' });
  }, 30_000);

  it('refuses two agents of one name made at once, and lets a module evaluated again replace its agent', async () => {
    expect(() => {
      kernelAgent({ harness: echo('echo-dup') });
      kernelAgent({ harness: echo('echo-dup') });
    }).toThrow(/two agents are named 'echo-dup'/);
    await Promise.resolve();
    // A later evaluation (a hot reload): the new agent replaces the old one.
    expect(() => kernelAgent({ harness: echo('echo-dup') })).not.toThrow();
  });
});

describe('dispatch', () => {
  it('gives a message several agents claim to the one its run names', async () => {
    const seen: string[] = [];
    const world = { runs: { get: async (runId: string) => ({ workflowName: workflowNameFor(`pd-${runId.split('-')[1]}`) }) } } as never;
    for (const name of ['pd-one', 'pd-two']) {
      register({ name, workflowName: workflowNameFor(name), world, claims: () => true, consume: async () => void seen.push(name) });
      await Promise.resolve();
    }
    const meta = { attempt: 1, queueName: '__harness_wkf_workflow_pi', messageId: 'm' };
    await dispatch(world, { runId: 'r-two', input: { kind: 'pi-wake' } }, meta);
    await dispatch(world, { runId: 'r-one', input: { kind: 'pi-wake' } }, meta);
    expect(seen).toEqual(['pd-two', 'pd-one']);
  });

  it('gives a session an earlier version started (the older harness-world name, another version) to its harness', async () => {
    const seen: string[] = [];
    const runs: Record<string, string> = { 'r-legacy': legacyWorkflowNameFor('disp-b'), 'r-older': 'workflow//durable-harness@0.0.0-older//disp-a', 'r-other': 'workflow//./src/x//disp-a' };
    const world = { runs: { get: async (runId: string) => ({ workflowName: runs[runId] }) } } as never;
    for (const name of ['disp-a', 'disp-b']) {
      register({ name, workflowName: workflowNameFor(name), world, consume: async (message) => void seen.push(`${name}:${(message as { runId: string }).runId}`) });
      await Promise.resolve();
    }
    const meta = { attempt: 1, deliveryCount: 1, queueName: '__harness_wkf_workflow_session', messageId: 'm' };
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    await dispatch(world, { runId: 'r-legacy' }, meta);
    await dispatch(world, { runId: 'r-older' }, meta);
    expect(await dispatch(world, { runId: 'r-other' }, meta)).toEqual({ timeoutSeconds: 5 });
    // A recovery delivery, on the run's workflow's queue (durable-harness's prefix), in either form.
    await dispatch(world, { runId: 'r-x' }, { ...meta, queueName: `__harness_wkf_workflow_${legacyWorkflowNameFor('disp-a')}` });
    await dispatch(world, { runId: 'r-y' }, { ...meta, queueName: '__harness_wkf_workflow_workflow//durable-harness@0.0.0-older//disp-b' });
    expect(seen).toEqual(['disp-b:r-legacy', 'disp-a:r-older', 'disp-a:r-x', 'disp-b:r-y']);
    vi.restoreAllMocks();
  });
});

describe('dispatch of a message no agent here owns', () => {
  afterEach(() => vi.restoreAllMocks());

  /** A World with one agent of its own that owns no run (its `runs.get` names another harness); nothing else, so a write would throw. */
  const worldWithOther = async (name: string) => {
    const world = { runs: { get: async () => ({ workflowName: workflowNameFor('someone-else') }) } } as never;
    for (const n of [name, `${name}-2`]) {
      register({ name: n, workflowName: workflowNameFor(n), world, consume: async () => 'consumed' });
      await Promise.resolve();
    }
    return world;
  };
  const meta = (delivery: Partial<{ attempt: number; deliveryCount: number }>, messageId = 'm') =>
    ({ queueName: '__harness_wkf_workflow_session', messageId, ...delivery }) as { attempt: number; queueName: string; messageId: string };

  it('is redelivered, later each time, while under the limit, and acknowledged with one error at the limit, the run left as it is', async () => {
    const world = await worldWithOther('unowned-a');
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const delays: unknown[] = [];
    for (let n = 1; n < MAX_UNOWNED_DELIVERIES; n++) delays.push(await dispatch(world, { runId: 'r-gone' }, meta({ attempt: 1, deliveryCount: n })));
    expect(delays.slice(0, 4)).toEqual([{ timeoutSeconds: 5 }, { timeoutSeconds: 10 }, { timeoutSeconds: 20 }, { timeoutSeconds: 40 }]);
    expect(delays.at(-1)).toEqual({ timeoutSeconds: UNOWNED_MAX_DELAY_SECONDS });
    expect(warn).toHaveBeenCalledTimes(MAX_UNOWNED_DELIVERIES - 1);
    expect(error).not.toHaveBeenCalled();

    // The last delivery: acknowledged (nothing to redeliver), logged once with the run and the agent it names.
    expect(await dispatch(world, { runId: 'r-gone' }, meta({ attempt: 1, deliveryCount: MAX_UNOWNED_DELIVERIES }))).toBeUndefined();
    expect(error).toHaveBeenCalledTimes(1);
    expect(error.mock.calls[0]![0]).toMatch(/acknowledged and not redelivered/);
    expect(error.mock.calls[0]![1]).toMatchObject({ runId: 'r-gone', workflowName: workflowNameFor('someone-else'), delivery: MAX_UNOWNED_DELIVERIES });
  });

  it("fails the delivery with a retryAfter on a World that counts only `attempt` (a `{ timeoutSeconds }` there is a new message), and acknowledges at the limit", async () => {
    const world = await worldWithOther('unowned-b');
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    await expect(dispatch(world, { runId: 'r-gone' }, meta({ attempt: 1 }))).rejects.toMatchObject({ retryAfter: 5 });
    await expect(dispatch(world, { runId: 'r-gone' }, meta({ attempt: 3 }))).rejects.toMatchObject({ retryAfter: 20 });
    expect(await dispatch(world, { runId: 'r-gone' }, meta({ attempt: MAX_UNOWNED_DELIVERIES }))).toBeUndefined();
    expect(error).toHaveBeenCalledTimes(1);
    expect(error.mock.calls[0]![1]).toMatchObject({ runId: 'r-gone' });
  });

  it('is still bounded when the World reports no count: this process counts the deliveries of the message', async () => {
    const world = await worldWithOther('unowned-c');
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const results: unknown[] = [];
    for (let n = 1; n <= MAX_UNOWNED_DELIVERIES; n++) results.push(await dispatch(world, { runId: 'r-uncounted' }, meta({}, `m-${n}`)));
    expect(results[0]).toEqual({ timeoutSeconds: 5 });
    expect(results.slice(0, -1).every((r) => typeof (r as { timeoutSeconds?: unknown }).timeoutSeconds === 'number')).toBe(true);
    expect(results.at(-1)).toBeUndefined();
    expect(error).toHaveBeenCalledTimes(1);
    expect(error.mock.calls[0]![1]).toMatchObject({ runId: 'r-uncounted', delivery: MAX_UNOWNED_DELIVERIES });
    // The count starts over for a later message of the run.
    expect(await dispatch(world, { runId: 'r-uncounted' }, meta({}))).toEqual({ timeoutSeconds: 5 });
  });

  it('bounds the message a pi-durable agent is given that is not a pi-durable session\'s, too', async () => {
    const world = { runs: { get: async () => undefined } } as never;
    resumeWith(async () => {
      throw new Error('never opened');
    }, { name: 'unowned-pi', world });
    await Promise.resolve();
    const agent = registered().find((a) => a.name === 'unowned-pi')!;
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const m = { queueName: '__harness_wkf_workflow_session', messageId: 'm', attempt: 1 };
    expect(await agent.consume({ runId: 'r-not-pi' }, { ...m, deliveryCount: 1 })).toEqual({ timeoutSeconds: 5 });
    expect(await agent.consume({ runId: 'r-not-pi' }, { ...m, deliveryCount: MAX_UNOWNED_DELIVERIES })).toBeUndefined();
    expect(error).toHaveBeenCalledTimes(1);
    expect(error.mock.calls[0]![1]).toMatchObject({ runId: 'r-not-pi' });
  });
});

describe('run names', () => {
  it("parse with the Workflow SDK's parser: the harness is the short name, durable-harness at its version the module", () => {
    const { version } = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { version: string };
    for (const name of ['claude-code', 'pi-durable', 'my-agent']) {
      expect(parseWorkflowName(workflowNameFor(name))).toEqual({ shortName: name, moduleSpecifier: `durable-harness@${version}`, functionName: name });
    }
  });
});

describe('chat routes in production', () => {
  const warned = Symbol.for('durable-harness//chat-route-no-authorize-warned');
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
    delete (globalThis as { [warned]?: boolean })[warned];
  });

  /** A pi-durable opener these tests never reach: the gate refuses first. */
  const unreachable = async (): Promise<never> => {
    throw new Error('the chat route opened a session it should have refused');
  };

  it("agent.chatRoute() and durable-harness/pi-durable's chatRoute() refuse every request without authorize, with a 401 that says what to pass", async () => {
    vi.stubEnv('NODE_ENV', 'production');
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const agent = kernelAgent({ harness: echo('echo-prod') });
    for (const route of [agent.chatRoute(), agent.chatRoute({ drive: 'queue' }), piChatRoute({ open: unreachable })]) {
      for (const res of [await route.POST(post({ text: 'hi' })), await route.GET(new Request('http://local/api/chat?sessionId=wrun_x&cursor=0.0'))]) {
        expect(res.status).toBe(401);
        const body = (await res.json()) as { message: string };
        expect(body.message).toMatch(/authorize/);
        expect(body.message).toMatch(/'public'/);
      }
    }
    expect(warn).toHaveBeenCalledTimes(1);
    expect(() => agent.chatRoute({ authorize: 'open' as never })).toThrow(/authorize must be a function or 'public'/);
    expect(() => piChatRoute({ open: unreachable, authorize: 'open' as never })).toThrow(/authorize must be a function or 'public'/);
  });

  it("chatRoute({ authorize: 'public' }) for an agent whose sessions each get a Vercel Sandbox throws without allowPublicSandbox: true", async () => {
    const { claudeCode } = await import('../src/claude-code.ts');
    const { codex } = await import('../src/codex.ts');
    const { sandbox } = await import('../src/workspace.ts');
    const capped = sandbox({ budget: { sessionUsd: 2 } });
    for (const agent of [claudeCode({ name: 'cc-public', model: 'anthropic/claude-haiku-4.5', workspace: capped }), codex({ name: 'codex-public', model: 'openai/gpt-5.4-mini', workspace: capped })]) {
      expect(() => agent.chatRoute({ authorize: 'public' })).toThrow(/Vercel Sandbox VM.*allowPublicSandbox: true/);
      expect(() => agent.chatRoute({ authorize: 'public', allowPublicSandbox: false })).toThrow(TypeError);
      // An opt-in, your own authorize, or (under next dev) none: built, no I/O.
      expect(() => agent.chatRoute({ authorize: 'public', allowPublicSandbox: true })).not.toThrow();
      expect(() => agent.chatRoute({ authorize: async () => {} })).not.toThrow();
      expect(() => agent.chatRoute()).not.toThrow();
    }
    // With the opt-in, an uncapped session spend (the default budget) is still refused: nothing caps how many sessions a public route opens.
    for (const agent of [claudeCode({ name: 'cc-public-uncapped', model: 'anthropic/claude-haiku-4.5' }), codex({ name: 'codex-public-uncapped', model: 'openai/gpt-5.4-mini' })]) {
      expect(() => agent.chatRoute({ authorize: 'public', allowPublicSandbox: true })).toThrow(/needs a finite sandbox\(\{ budget: \{ sessionUsd \} \}\)/);
      expect(() => agent.chatRoute({ authorize: async () => {} })).not.toThrow();
    }
    // A harness that boots no VM (here a kernel harness of the app's own) needs no opt-in.
    expect(() => kernelAgent({ harness: echo('echo-public') }).chatRoute({ authorize: 'public' })).not.toThrow();
    // A copy of a sandbox harness keeps its mark: spreading it into a new object doesn't skip the opt-in.
    const { claudeCodeHarness } = await import('../src/claude-code.ts');
    const copied = kernelAgent({ harness: { ...claudeCodeHarness({ name: 'cc-public-copy', workspace: capped }) } });
    expect(() => copied.chatRoute({ authorize: 'public' })).toThrow(/Vercel Sandbox VM.*allowPublicSandbox: true/);
    expect(() => copied.chatRoute({ authorize: 'public', allowPublicSandbox: true })).not.toThrow();
  });

  it("a public route records itself in each session's config, so a harness rebuilt without its sandbox mark still can't boot a VM for it", async () => {
    process.env.WORKFLOW_TARGET_WORLD = 'local';
    process.env.WORKFLOW_LOCAL_DATA_DIR ??= mkdtempSync(join(tmpdir(), 'durable-harness-'));
    vi.stubEnv('NODE_ENV', 'production');
    const configs: unknown[] = [];
    const recording = (name: string): Harness => {
      const inner = echo(name);
      return { name, drive: (ctx) => (configs.push(ctx.config), inner.drive(ctx)) };
    };
    await sse(await kernelAgent({ harness: recording('echo-route-public') }).chatRoute({ authorize: 'public', drive: 'queue' }).POST(post({ text: 'hi' })));
    await sse(await kernelAgent({ harness: recording('echo-route-allowed') }).chatRoute({ authorize: 'public', allowPublicSandbox: true, drive: 'queue' }).POST(post({ text: 'hi' })));
    await sse(await kernelAgent({ harness: recording('echo-route-own') }).chatRoute({ authorize: async () => {}, drive: 'queue' }).POST(post({ text: 'hi' })));
    expect(configs[0]).toMatchObject({ route: 'public' });
    expect(configs[0]).not.toHaveProperty('allowPublicSandbox');
    expect(configs[1]).toMatchObject({ route: 'public', allowPublicSandbox: true });
    expect(configs[2]).not.toHaveProperty('route');
    const { publicSandboxRefusal } = await import('../src/sandbox-harness.ts');
    expect(publicSandboxRefusal(configs[0], { sessionUsd: 2 })).toMatch(/without allowPublicSandbox: true/);
    expect(publicSandboxRefusal(configs[1], { sessionUsd: 2 })).toBeUndefined();
    expect(publicSandboxRefusal(configs[1], { sessionUsd: Infinity })).toMatch(/finite budget\.sessionUsd/);
    expect(publicSandboxRefusal(configs[2], { sessionUsd: Infinity })).toBeUndefined();
    expect(publicSandboxRefusal(undefined, { sessionUsd: Infinity })).toBeUndefined();
  });

  it("serve with authorize: 'public' or a function, and refuse what the function refuses", async () => {
    process.env.WORKFLOW_TARGET_WORLD = 'local';
    process.env.WORKFLOW_LOCAL_DATA_DIR ??= mkdtempSync(join(tmpdir(), 'durable-harness-'));
    vi.stubEnv('NODE_ENV', 'production');
    const agent = kernelAgent({ harness: echo('echo-prod-open') });
    const res = await agent.chatRoute({ authorize: 'public', drive: 'queue' }).POST(post({ text: 'hi' }));
    expect(res.status).toBe(200);
    expect((await sse(res)).some((e) => (e.data as { delta?: string } | undefined)?.delta === 'echo-prod-open:hi')).toBe(true);
    const refusing = agent.chatRoute({
      authorize: () => {
        throw new Response('no', { status: 403 });
      },
    });
    expect((await refusing.POST(post({ text: 'hi' }))).status).toBe(403);
    expect(
      (
        await piChatRoute({
          open: unreachable,
          authorize: () => {
            throw new Response('no', { status: 404 });
          },
        }).POST(post({ text: 'hi' }))
      ).status
    ).toBe(404);
  }, 30_000);
});

import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT as ctx } from '@earendil-works/chord/context';
import { Type } from '@earendil-works/pi-ai';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness, type Storage, type ToolRegistration } from '@earendil-works/pi-durable';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { recoverSessions } from 'harness-world';
import { journalHead } from 'pi-world';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { acquireHarness, chatRoute, gatewayModels, GATEWAY_PROVIDER, openWorldReader, openWorldStorage, resumeWith } from '../src/pi-durable.ts';
import { registerQueue } from '../src/registry.ts';
import { getWorld } from '../src/world.ts';

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

/** A Pi app's own setup: a scripted faux model, a registry with `tools`, and its opener. */
function piApp(answers: Parameters<ReturnType<typeof fauxProvider>['setResponses']>[0], tools: ToolRegistration[] = []) {
  const faux = fauxProvider();
  faux.setResponses(answers);
  const models = createModels();
  models.setProvider(faux.provider);
  const model = faux.getModel();
  const registry = createRegistry();
  if (tools.length) registry.install(defineExtension({ name: 'app', tools }));
  const opened: string[] = [];
  const open = (storage: Storage) => (opened.push(String((storage as { sessionId?: string }).sessionId)), Harness.open(storage, { models, registry }, ctx));
  return { faux, open, opened, agent: { model: { provider: model.provider, modelId: model.id }, instructions: 'Use the tools.' } };
}

/** A tool that waits until `release()` (or its call is aborted). */
function gate(name: string, replay?: 'safe') {
  let release!: () => void;
  const released = new Promise<void>((r) => (release = r));
  let started = 0;
  const tool = defineTool({
    name,
    description: `${name}.`,
    parameters: Type.Object({}),
    ...(replay ? { replay } : {}),
    execute: async (_args, _api, context) => {
      started++;
      await Promise.race([released, new Promise((r) => context.abortSignal?.addEventListener('abort', r, { once: true }))]);
      return { content: [{ type: 'text', text: `${name} done` }] };
    },
  }) as ToolRegistration;
  return {
    tool,
    started: () => started,
    release: () => release(),
  };
}

async function sse(response: Response): Promise<{ event?: string; data?: any }[]> {
  const text = await response.text();
  return text
    .split('\n\n')
    .filter(Boolean)
    .map((block) => {
      const m: { event?: string; data?: any } = {};
      for (const line of block.split('\n')) {
        if (line.startsWith('event: ')) m.event = line.slice(7);
        if (line.startsWith('data: ')) m.data = JSON.parse(line.slice(6));
      }
      return m;
    })
    .filter((m) => m.event || m.data);
}

const post = (body: unknown) => new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify(body), headers: { 'content-type': 'application/json' } });

describe('durable-harness/pi-durable on the environment World (world-local)', () => {
  beforeAll(() => {
    process.env.WORKFLOW_TARGET_WORLD = 'local';
    process.env.WORKFLOW_LOCAL_DATA_DIR = mkdtempSync(join(tmpdir(), 'dh-pi-durable-'));
  });
  afterAll(async () => {
    await ((await getWorld()) as { close?: () => Promise<void> }).close?.();
  });

  it("runs a Pi app's own code on a World storage: Harness.open, root, submit, wait", async () => {
    const app = piApp([fauxAssistantMessage([fauxText('Paris.')])]);
    const open = resumeWith(app.open, { name: 'pi-own-code' });
    expect(open).toBe(app.open);
    const storage = await openWorldStorage(undefined, { name: 'pi-own-code' });
    expect(storage.sessionId).toMatch(/^wrun_[0-9A-HJKMNP-TV-Z]{26}$/);
    const harness = await open(storage);
    const root = await harness.root(ctx, { agent: app.agent });
    const submission = await root.submit({ type: 'input', content: 'The capital of France?', requestId: 'q1' }, ctx);
    expect((await submission.wait(ctx)).status).toBe('done');
    // The same request id finds the same submission.
    expect((await root.submit({ type: 'input', content: 'The capital of France?', requestId: 'q1' }, ctx)).id).toBe(submission.id);
    await harness.close(ctx);
    const reader = openWorldReader(storage.sessionId);
    await reader.refresh();
    const snap = await reader.snapshot();
    expect(snap.unfinished).toBe(false);
    expect(JSON.stringify(snap.entries)).toContain('Paris.');
    // The run records the opener's name, so a wake goes to it.
    const run = await (await getWorld()).runs.get(storage.sessionId);
    expect((run as { workflowName: string }).workflowName).toMatch(/\/\/pi-own-code$/);
  });

  it('closing the Harness mid-turn hands the session to a wake at once, which finishes it with the registered opener', async () => {
    const work = gate('work', 'safe');
    const app = piApp([fauxAssistantMessage([fauxToolCall('work', {})], { stopReason: 'toolUse' }), fauxAssistantMessage([fauxText('Worked.')])], [work.tool]);
    const open = resumeWith(app.open, { name: 'pi-close' });
    const storage = await openWorldStorage(undefined, { name: 'pi-close' });
    const harness = await open(storage);
    await (await harness.root(ctx, { agent: app.agent })).submit({ type: 'input', content: 'Work.' }, ctx);
    await until(() => work.started() === 1);
    // Closing aborts the tool call; its outcome is never committed: the work is left, and the close says so.
    await harness.close(ctx);
    expect(await journalHead(await getWorld(), storage.sessionId, ctx)).toMatchObject({ unfinished: true, closed: true });
    // The wake reruns the safe tool: let that run finish.
    work.release();
    const reader = openWorldReader(storage.sessionId);
    const snap = await until(async () => {
      await reader.refresh();
      const s = await reader.snapshot();
      return !s.unfinished && JSON.stringify(s.entries).includes('Worked.') ? s : undefined;
    });
    expect(snap.tasks.find((t) => t.kind === 'pi.tool')?.starts.length).toBe(2);
    expect(work.started()).toBe(2);
    expect(app.opened.filter((id) => id === storage.sessionId).length).toBe(2); // the route's open, then the wake's
  });

  it('acquireHarness shares one Harness per session in this process, and a second raw open is warned about', async () => {
    const app = piApp([fauxAssistantMessage([fauxText('One.')])]);
    const first = await acquireHarness(undefined, app.open, { name: 'pi-shared' });
    const second = await acquireHarness(first.storage.sessionId, app.open, { name: 'pi-shared' });
    expect(second.harness).toBe(first.harness);
    expect(app.opened).toHaveLength(1);
    const root = await first.harness.root(ctx, { agent: app.agent });
    expect((await (await root.submit({ type: 'input', content: 'One?' }, ctx)).wait(ctx)).status).toBe('done');
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const raw = await openWorldStorage(first.storage.sessionId, { name: 'pi-shared' });
    expect(warn.mock.calls.some(([m]) => String(m).includes('is open twice in this process'))).toBe(true);
    warn.mockRestore();
    await raw.close(ctx);
    await first.release();
    await second.release();
    // Released by both: the next acquire opens a fresh Harness.
    const again = await acquireHarness(first.storage.sessionId, app.open, { name: 'pi-shared' });
    expect(app.opened).toHaveLength(2);
    await again.release();
  });

  it('chatRoute streams a turn from Pi, follows it on a GET from the log, and a retried POST finds the same submission', async () => {
    const work = gate('lookup', 'safe');
    const app = piApp([fauxAssistantMessage([fauxText('Looking. '), fauxToolCall('lookup', {})], { stopReason: 'toolUse' }), fauxAssistantMessage([fauxText('It is 42.')])], [work.tool]);
    const open = resumeWith(app.open, { name: 'pi-chat' });
    const route = chatRoute({ open, agent: app.agent, authorize: 'public', name: 'pi-chat' });
    const res = await route.POST(post({ text: 'Look it up.', requestId: 'chat-1' }));
    expect(res.status).toBe(200);
    const sessionId = res.headers.get('x-session-id')!;
    const submissionId = res.headers.get('x-submission-id')!;
    const events = sse(res);
    await until(() => work.started() === 1);
    // A reconnect while the tool runs: it follows the session from the log, without opening it.
    const opens = app.opened.length;
    const followed = await route.GET(new Request(`http://local/api/chat?sessionId=${sessionId}`));
    expect(followed.status).toBe(200);
    const followedEvents = sse(followed);
    await sleep(300);
    work.release();
    const all = await events;
    expect(all.filter((e) => e.data?.type === 'text').map((e) => e.data.delta).join('')).toBe('Looking. It is 42.');
    expect(all.some((e) => e.data?.type === 'tool_start' && e.data.name === 'lookup')).toBe(true);
    expect(all.some((e) => e.data?.type === 'tool_end')).toBe(true);
    expect(all.at(-2)?.data).toEqual({ type: 'idle' });
    expect(all.at(-1)?.event).toBe('end');
    const after = await followedEvents;
    expect(after.filter((e) => e.data?.type === 'text').map((e) => e.data.delta).join('')).toContain('It is 42.');
    expect(after.at(-1)?.event).toBe('end');
    expect(app.opened.length).toBe(opens);
    // Idle now: a GET has nothing to follow.
    expect((await route.GET(new Request(`http://local/api/chat?sessionId=${sessionId}`))).status).toBe(204);
    // The same request id again: Pi finds the submission it already has.
    const retried = await route.POST(post({ sessionId, text: 'Look it up.', requestId: 'chat-1' }));
    expect(retried.headers.get('x-submission-id')).toBe(submissionId);
    await sse(retried);
  }, 60_000);

  it("a prompt POSTed mid-turn joins the running turn (Pi's whenBusy: 'followUp'): the first stream answers it before its idle", async () => {
    const work = gate('lookup', 'safe');
    const app = piApp(
      [fauxAssistantMessage([fauxText('Looking. '), fauxToolCall('lookup', {})], { stopReason: 'toolUse' }), fauxAssistantMessage([fauxText('It is 42. ')]), fauxAssistantMessage([fauxText('Noted.')])],
      [work.tool]
    );
    const open = resumeWith(app.open, { name: 'pi-mid-prompt' });
    const route = chatRoute({ open, agent: app.agent, authorize: 'public', name: 'pi-mid-prompt' });
    const res = await route.POST(post({ text: 'Look it up.' }));
    const sessionId = res.headers.get('x-session-id')!;
    const events = sse(res);
    await until(() => work.started() === 1);
    const second = await route.POST(post({ sessionId, text: 'Then say noted.' }));
    expect(second.status).toBe(200);
    const secondEvents = sse(second);
    work.release();
    const all = await events;
    // One turn: both answers, then a single idle.
    expect(all.filter((e) => e.data?.type === 'text').map((e) => e.data.delta).join('')).toBe('Looking. It is 42. Noted.');
    expect(all.filter((e) => e.data?.type === 'idle')).toHaveLength(1);
    expect(all.at(-1)?.event).toBe('end');
    await secondEvents;
    expect(app.faux.state.callCount).toBe(3);
  }, 60_000);

  it('gatewayModels knows the Gateway models', () => {
    const models = gatewayModels({ apiKey: async () => 'not-used' });
    expect(models.getModel(GATEWAY_PROVIDER, 'anthropic/claude-haiku-4.5')).toBeDefined();
  });
});

describe('a holder that died mid-tool', () => {
  const worlds: LocalWorld[] = [];
  afterAll(async () => {
    for (const w of worlds) await w.close?.();
  });

  it('is finished through the queue by the opener resumeWith registered, when another World instance on the same data starts', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'dh-pi-abandoned-'));
    const a = createWorld({ dataDir, recoverActiveRuns: false });
    worlds.push(a);
    const hang = gate('lookup', 'safe');
    const first = piApp([fauxAssistantMessage([fauxToolCall('lookup', {})], { stopReason: 'toolUse' })], [hang.tool]);
    const storage = await openWorldStorage(undefined, { world: a, name: 'pi-abandoned' });
    const harness = await first.open(storage);
    await (await harness.root(ctx, { agent: first.agent })).submit({ type: 'input', content: 'Look it up.' }, ctx);
    await until(() => hang.started() === 1);
    // The process dies: no more commits, no close entry.
    storage.abandon();
    expect((await journalHead(a, storage.sessionId, ctx)).last?.anchor).toMatchObject({ k: 'deadline', proc: expect.objectContaining({ pid: process.pid }) });

    // "Another process": a World instance of its own on the same data, the app's opener registered for it.
    const b = createWorld({ dataDir, recoverActiveRuns: false });
    worlds.push(b);
    const done = gate('lookup', 'safe');
    done.release();
    const second = piApp([fauxAssistantMessage([fauxText('It is 42.')])], [done.tool]);
    resumeWith(second.open, { world: b, name: 'pi-abandoned' });
    registerQueue(b);
    await b.start?.();
    expect(await recoverSessions(b as World)).toBeGreaterThan(0);
    const reader = openWorldReader(storage.sessionId, { world: b });
    await until(async () => {
      await reader.refresh();
      const s = await reader.snapshot();
      return !s.unfinished && JSON.stringify(s.entries).includes('It is 42.');
    });
    expect(done.started()).toBe(1);
    expect(second.opened).toEqual([storage.sessionId]);
    hang.release();
  }, 60_000);
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { Type } from '@earendil-works/pi-ai';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness, type ToolRegistration } from '@earendil-works/pi-durable';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { type Anchor, claimDrive, localAnchors, releaseDrive, vercelAnchors } from 'harness-world/anchor';
import { leasedWorld } from 'harness-world/testing';
import { hasUnfinishedWork, journalHead, QUEUE_PREFIX, runWake, StorageFenced, WAKE_QUEUE, type WorldStorage, type WorldStorageOptions, worldStorage } from './index.ts';

const ctx = BACKGROUND_CONTEXT;

async function until<T>(fn: () => Promise<T | undefined | false>, timeoutMs = 20_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined && value !== false) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** A tool call that runs until its drive stops it (a closing Harness aborts it). */
const untilAborted = (signal?: AbortSignal) =>
  new Promise<never>((_, reject) => signal?.addEventListener('abort', () => reject(signal.reason ?? new Error('aborted')), { once: true }));

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

async function newWorld(dataDir?: string): Promise<{ world: LocalWorld; dataDir: string }> {
  const dir = dataDir ?? (await mkdtemp(join(tmpdir(), 'pi-world-')));
  const world = createWorld({ dataDir: dir, recoverActiveRuns: false });
  worlds.push(world);
  return { world, dataDir: dir };
}

/** One process: a faux model with scripted answers, these tools, and an `openAgent` over `world`. */
function agent(world: World, answers: Parameters<ReturnType<typeof fauxProvider>['setResponses']>[0], tools: ToolRegistration[], storageOptions: WorldStorageOptions = {}) {
  const faux = fauxProvider();
  faux.setResponses(answers);
  const models = createModels();
  models.setProvider(faux.provider);
  const model = faux.getModel();
  const registry = createRegistry();
  registry.install(defineExtension({ name: 'test', tools }));
  const storages: WorldStorage[] = [];
  const openAgent = async (sessionId: string, anchor?: Anchor) => {
    const storage = await worldStorage(world, sessionId, { ...storageOptions, ...(anchor ? { anchor } : {}) });
    storages.push(storage);
    return Harness.open(storage, { models, registry, settings: { retry: { maxRetries: 0 }, toolExecution: 'parallel' } }, ctx);
  };
  return { openAgent, storages, faux, agentChoice: { model: { provider: model.provider, modelId: model.id } } };
}

async function transcript(harness: Harness): Promise<string> {
  const root = await harness.root(ctx);
  return JSON.stringify((await root.entries({}, 1000, undefined, ctx)).items);
}

describe('Pi Harness on worldStorage', () => {
  it('answers an input, and a fresh process reads the same session back', async () => {
    const { world, dataDir } = await newWorld();
    const a = agent(world, [fauxAssistantMessage([fauxText('Paris.')])], []);
    const sessionId = await worldStorage.create(world);
    const harness = await a.openAgent(sessionId);
    const root = await harness.root(ctx, { agent: a.agentChoice });
    const submission = await root.submit({ type: 'input', content: 'What is the capital of France?', requestId: 'q1' }, ctx);
    const settled = await submission.wait(ctx);
    expect(settled.status).toBe('done');
    await harness.close(ctx);

    const { world: other } = await newWorld(dataDir);
    const b = agent(other, [], []);
    const reopened = await b.openAgent(sessionId);
    const text = await transcript(reopened);
    expect(text).toContain('What is the capital of France?');
    expect(text).toContain('Paris.');
    // The request id finds the same submission instead of submitting twice.
    const again = await (await reopened.root(ctx)).submit({ type: 'input', content: 'What is the capital of France?', requestId: 'q1' }, ctx);
    expect(again.id).toBe(submission.id);
    expect(await hasUnfinishedWork(other, sessionId, ctx)).toBe(false);
    await reopened.close(ctx);
  });

  it("records the kernel's queue namespace on the run, and wakes on the kernel's prefix", async () => {
    const { world } = await newWorld();
    const sessionId = await worldStorage.create(world);
    const run = await world.runs.get(sessionId);
    expect(run.executionContext).toEqual({ queueNamespace: 'harness' });
    expect(QUEUE_PREFIX).toBe('__harness_wkf_workflow_');
    expect(WAKE_QUEUE).toBe('__harness_wkf_workflow_pi');
    const sent: string[] = [];
    world.registerHandler(QUEUE_PREFIX, async (request) => {
      sent.push(request.headers.get('x-vqs-queue-name') ?? '');
      return Response.json({ ok: true });
    });
    await worldStorage.wake(world, sessionId);
    await until(async () => sent.length > 0);
    expect(sent).toEqual([WAKE_QUEUE]);
  });

  it('a wake in another process finishes a run whose process died mid-tool', async () => {
    const { world: worldA, dataDir } = await newWorld();
    const started = { safe: 0, unsafe: 0 };
    const hang = () => new Promise<never>(() => {}); // The process dies inside both calls.
    const a = agent(
      worldA,
      [fauxAssistantMessage([fauxToolCall('lookup', { q: 'x' }), fauxToolCall('pay', { amount: 5 })], { stopReason: 'toolUse' })],
      [
        defineTool({ name: 'lookup', description: 'Read something.', parameters: Type.Object({ q: Type.String() }), replay: 'safe', execute: () => (started.safe++, hang()) }),
        defineTool({ name: 'pay', description: 'Move money.', parameters: Type.Object({ amount: Type.Number() }), execute: () => (started.unsafe++, hang()) }),
      ],
    );
    const sessionId = await worldStorage.create(worldA);
    // Process A drives under its delivery's anchor: on world-local, the message and this process.
    const harnessA = await a.openAgent(sessionId, localAnchors().own({ messageId: 'msg_a' }));
    const rootA = await harnessA.root(ctx, { agent: a.agentChoice });
    const submission = await rootA.submit({ type: 'input', content: 'Look up x and pay 5.' }, ctx);
    await until(async () => started.safe === 1 && started.unsafe === 1);
    for (const s of a.storages) s.abandon(); // Crash: no more commits, no close entry.
    await worldA.close?.();

    // Process B: same log, its own World instance, the wake route registered.
    const { world: worldB } = await newWorld(dataDir);
    const ran = { safe: 0, unsafe: 0 };
    const b = agent(
      worldB,
      [fauxAssistantMessage([fauxText('Looked it up; the payment may not have gone through.')])],
      [
        defineTool({ name: 'lookup', description: 'Read something.', parameters: Type.Object({ q: Type.String() }), replay: 'safe', execute: async () => (ran.safe++, { content: [{ type: 'text', text: 'x is 42' }] }) }),
        defineTool({ name: 'pay', description: 'Move money.', parameters: Type.Object({ amount: Type.Number() }), execute: async () => (ran.unsafe++, { content: [{ type: 'text', text: 'paid' }] }) }),
      ]
    );
    const openAgent = vi.fn(b.openAgent);
    // Here both "processes" share a pid: A's anchor names this process, whose drive of it no longer runs, so it is gone.
    worldB.registerHandler(QUEUE_PREFIX, worldStorage.wakeHandler(worldB, openAgent));
    expect(await hasUnfinishedWork(worldB, sessionId, ctx)).toBe(true);
    await worldStorage.wake(worldB, sessionId);

    await until(async () => !(await hasUnfinishedWork(worldB, sessionId, ctx)) && openAgent.mock.calls.length > 0, 30_000);
    expect(ran).toEqual({ safe: 1, unsafe: 0 }); // The safe call reran; the unsafe one did not.
    const reader = await b.openAgent(sessionId);
    const text = await transcript(reader);
    expect(text).toContain('x is 42');
    expect(text).toContain('interrupted');
    if (process.env.PI_WORLD_DEBUG) console.log(text);
    expect(text).toContain('the payment may not have gone through');
    const settled = await (await reader.submission(submission.id, ctx))!.wait(ctx);
    expect(settled.status).toBe('done');
    await reader.close(ctx);
  });

  it('fences a commit when another process committed first, and ignores it on replay', async () => {
    const { world } = await newWorld();
    const sessionId = await worldStorage.create(world);
    const p1 = agent(world, [], []);
    const p2 = agent(world, [], []);
    const one = await p1.openAgent(sessionId);
    const two = await p2.openAgent(sessionId);

    await (await two.root(ctx)).submit({ type: 'write', entry: { kind: 'app.note', data: 'from two' } }, ctx);
    const lost = await one.root(ctx).then((root) => root.submit({ type: 'write', entry: { kind: 'app.note', data: 'from one' } }, ctx)).catch((e: unknown) => e);
    const fenced = lost instanceof StorageFenced ? lost : (lost as { cause?: unknown })?.cause;
    expect(fenced).toBeInstanceOf(StorageFenced);
    await expect(p1.storages[0]!.commit([], ctx)).rejects.toBeInstanceOf(StorageFenced);

    // The loser's commit counts for nobody (on a World with the in-band
    // fence, as world-local is, the World refused it, so it is not even in
    // the log; on one without, it lands and the `after` chain skips it), and
    // it does not fence the winner, which keeps committing.
    await (await two.root(ctx)).submit({ type: 'write', entry: { kind: 'app.note', data: 'two again' } }, ctx);
    await two.close(ctx);
    const reader = await agent(world, [], []).openAgent(sessionId);
    const text = await transcript(reader);
    expect(text).toContain('from two');
    expect(text).toContain('two again');
    expect(text).not.toContain('from one');
    await reader.close(ctx);
    await one.close(ctx).catch(() => {});
  });

  it('a storage without a backstop queues nothing on commits; a wake of an idle session does nothing', async () => {
    const { world } = await newWorld();
    const queued: { name: string; delaySeconds?: number }[] = [];
    const queue = world.queue.bind(world);
    world.queue = (async (name, message, opts) => {
      queued.push({ name, delaySeconds: opts?.delaySeconds });
      return queue(name, message, opts);
    }) as World['queue'];
    const a = agent(world, [fauxAssistantMessage([fauxText('Hi.')])], []);
    const sessionId = await worldStorage.create(world);
    const harness = await a.openAgent(sessionId);
    const root = await harness.root(ctx, { agent: a.agentChoice });
    await (await root.submit({ type: 'input', content: 'Hello' }, ctx)).wait(ctx);
    await harness.close(ctx);
    // No backstop arm, no heartbeat: the queue is only ever written by whoever needs a wake.
    expect(queued).toEqual([]);

    const openAgent = vi.fn(a.openAgent);
    expect(await runWake(world, sessionId, openAgent)).toEqual({ kind: 'idle' });
    expect(openAgent).not.toHaveBeenCalled();
  });

  it("acks a wake while the holder's delivery is not proven gone, and takes it once it is", async () => {
    const { world } = await newWorld();
    const sessionId = await worldStorage.create(world);
    let waiting = false;
    const a = agent(world, [], [defineTool({ name: 'wait', description: 'Wait.', parameters: Type.Object({}), execute: (_args, _api, context) => ((waiting = true), untilAborted(context.abortSignal)) })]);
    a.faux.setResponses([fauxAssistantMessage([fauxToolCall('wait', {})], { stopReason: 'toolUse' })]);
    const holder: Anchor = { k: 'msg', m: 'msg_holder', proc: localAnchors().own({}).proc! };
    const harness = await a.openAgent(sessionId, holder);
    const root = await harness.root(ctx, { agent: a.agentChoice });
    await root.submit({ type: 'input', content: 'Wait.' }, ctx);
    // The holder is in its tool call, its commits so far landed.
    await until(async () => waiting);
    await new Promise((r) => setTimeout(r, 100));
    expect((await journalHead(world, sessionId, ctx)).last?.anchor).toMatchObject({ k: 'msg', m: 'msg_holder' });
    const openAgent = vi.fn(a.openAgent);
    const own = { k: 'msg' as const, m: 'msg_wake' };
    // On Vercel another message's anchor is its holder's, and comes back if it dies.
    expect(await runWake(world, sessionId, openAgent, { anchors: vercelAnchors(), anchor: own })).toMatchObject({ kind: 'covered' });
    // Here the holder is this process: covered while its drive runs here.
    const claim = claimDrive(world, sessionId, { drv: 'drv_holder', anchor: holder })!;
    expect(await runWake(world, sessionId, openAgent, { anchors: localAnchors(), anchor: own })).toMatchObject({ kind: 'covered' });
    expect(openAgent).not.toHaveBeenCalled();
    // Its drive let go without closing (the process "died"): gone, so the wake takes it.
    releaseDrive(world, sessionId, claim);
    for (const s of a.storages) s.abandon();
    a.faux.setResponses([fauxAssistantMessage([fauxText('Done waiting.')])]);
    const resumed = await runWake(world, sessionId, openAgent, { anchors: localAnchors(), anchor: own, maxDriveMs: 5_000 });
    expect(resumed.kind).toBe('resumed');
    expect(openAgent).toHaveBeenCalledWith(sessionId, own);
  });

  it('a storage that closes with work live says so, and the next wake takes the session at once', async () => {
    const { world } = await newWorld();
    const sessionId = await worldStorage.create(world);
    const a = agent(world, [], [defineTool({ name: 'wait', description: 'Wait.', parameters: Type.Object({}), execute: (_args, _api, context) => untilAborted(context.abortSignal) })]);
    a.faux.setResponses([fauxAssistantMessage([fauxToolCall('wait', {})], { stopReason: 'toolUse' })]);
    const harness = await a.openAgent(sessionId, { k: 'msg', m: 'msg_holder' });
    const root = await harness.root(ctx, { agent: a.agentChoice });
    await root.submit({ type: 'input', content: 'Wait.' }, ctx);
    await until(() => hasUnfinishedWork(world, sessionId, ctx));
    await harness.close(ctx);
    const head = await journalHead(world, sessionId, ctx);
    expect(head).toMatchObject({ unfinished: true, closed: true });
    // Even where another message's anchor is never proven gone (Vercel), a closed writer is.
    a.faux.setResponses([fauxAssistantMessage([fauxText('Done.')])]);
    const outcome = await runWake(world, sessionId, a.openAgent, { anchors: vercelAnchors(), anchor: { k: 'msg', m: 'msg_wake' }, maxDriveMs: 5_000 });
    expect(outcome.kind).toBe('resumed');
  });

  it('a wake that runs out of drive budget with work left sends a continuation that takes its anchor', async () => {
    const { world } = await newWorld();
    const sessionId = await worldStorage.create(world);
    let calls = 0;
    const slow = defineTool({ name: 'slow', description: 'Slow.', parameters: Type.Object({}), replay: 'safe', execute: (_args, _api, context) => (calls++, untilAborted(context.abortSignal)) });
    const a = agent(world, [fauxAssistantMessage([fauxToolCall('slow', {})], { stopReason: 'toolUse' })], [slow]);
    const writer = await a.openAgent(sessionId, { k: 'msg', m: 'msg_first' });
    await (await writer.root(ctx, { agent: a.agentChoice })).submit({ type: 'input', content: 'Go.' }, ctx);
    await until(async () => calls === 1);
    for (const s of a.storages) s.abandon();
    const queued: unknown[] = [];
    const queue = world.queue.bind(world);
    world.queue = (async (name, message, opts) => (queued.push({ name, message, key: opts?.idempotencyKey }), queue(name, message, opts))) as World['queue'];
    const outcome = await runWake(world, sessionId, a.openAgent, { anchors: vercelAnchors(), anchor: { k: 'msg', m: 'msg_first' }, maxDriveMs: 300 });
    expect(outcome).toEqual({ kind: 'resumed', unfinished: true, continued: true });
    expect(queued).toEqual([{ name: WAKE_QUEUE, message: { runId: sessionId, input: { kind: 'pi-wake', continues: 'msg:msg_first' } }, key: expect.stringContaining(':pi-cont:msg:msg_first:') }]);
  });

  it('on a leased queue, a wake that dies mid-tool is redelivered and finishes the run (its own message: mine)', async () => {
    const { world: base } = await newWorld();
    const world = leasedWorld(base, { visibilityMs: 200 });
    const sessionId = await worldStorage.create(world);
    let started = 0;
    let hang = true;
    const a = agent(
      world,
      [fauxAssistantMessage([fauxToolCall('lookup', { q: 'x' })], { stopReason: 'toolUse' }), fauxAssistantMessage([fauxText('x is 42.')])],
      [
        defineTool({
          name: 'lookup',
          description: 'Read something.',
          parameters: Type.Object({ q: Type.String() }),
          replay: 'safe',
          execute: async () => (started++, hang ? new Promise<never>(() => {}) : { content: [{ type: 'text', text: '42' }] }),
        }),
      ]
    );
    // A request commits the prompt and its process dies with the tool running.
    const opener = await a.openAgent(sessionId, localAnchors().own({ messageId: 'msg_request' }));
    await (await opener.root(ctx, { agent: a.agentChoice })).submit({ type: 'input', content: 'Look up x.' }, ctx);
    await until(async () => started === 1);
    for (const s of a.storages) s.abandon();
    worldStorage.wakeHandler(world, a.openAgent, { anchors: localAnchors() });
    await worldStorage.wake(world, sessionId);
    // The wake: the request's process is this one, its drive is not running here, so it is gone.
    const first = world.broker.deliver()!;
    await until(async () => started === 2);
    first.kill(); // The wake's own process dies mid-tool: its lease lapses.
    hang = false;
    await new Promise((r) => setTimeout(r, 300));
    // The queue redelivers the same message: the commits carry its anchor, so it is this delivery's own.
    const redelivered = world.broker.deliver(first.messageId)!;
    expect(redelivered.attempt).toBe(2);
    await redelivered.done;
    expect(redelivered.state).toBe('acked');
    expect(started).toBe(3);
    expect(await hasUnfinishedWork(base, sessionId, ctx)).toBe(false);
    expect((await journalHead(base, sessionId, ctx)).last?.anchor).toMatchObject({ k: 'msg', m: first.messageId });
  });
});

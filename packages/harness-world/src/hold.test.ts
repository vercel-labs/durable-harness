import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import type { World } from '@workflow/world';
import { afterEach, describe, expect, it } from 'vitest';
import { durableSessions, vercelAnchors, worldHolds } from './index.ts';
import { type BrokerMessage, hostProcess, type LeasedWorldOptions, leasedWorld } from './testing/index.ts';
import { Unavailable, UNAVAILABLE_STEP } from './unavailable.ts';
import type { DriveContext, Harness } from './types.ts';

/**
 * Send-and-hold (hold.ts): on a World whose queue holds a message for its
 * sender, a `wake: 'backstop'` submission's wake goes out undelayed and held,
 * and the process that sent it runs it as that message's delivery (its `msg`
 * anchor) instead of driving under a deadline with a delayed companion. A
 * wake the queue does not hold is the queue's to deliver; a World without
 * the capability keeps the companion.
 *
 * Process A takes the requests (the chat route's Function); process B serves
 * the queue (the flow route's). Queue seconds are 50 ms here.
 */

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

async function until<T>(fn: () => Promise<T | undefined> | T | undefined, timeoutMs = 5000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 20));
  }
}

/** One `model` effect per submission; `gate`, when set, holds the first run of each until it resolves. */
function harness(tag: string[], gate?: { wait?: Promise<void> }): Harness {
  return {
    name: 'h',
    async drive(ctx: DriveContext) {
      let n = ctx.state().effectOrder.filter((e) => e.name === 'model').length;
      for (;;) {
        const next = ctx.pending();
        if (next.length === 0) break;
        for (const s of next) {
          await ctx.effect(`model:${n++}`, { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => {
            tag.push(`ran:${s.text}`);
            if (gate?.wait) await gate.wait;
            return 'ok';
          });
        }
        await ctx.refresh();
      }
    },
  };
}

/** One `model` effect per submission, keyed by it, so a recovery that released it runs it again. */
function replaying(tag: string[], gate?: { wait?: Promise<void> }): Harness {
  return {
    name: 'h',
    async drive(ctx: DriveContext) {
      const open = () => {
        const state = ctx.state();
        const done = new Set(state.effectOrder.filter((e) => e.name === 'model' && e.status === 'completed').flatMap((e) => e.placed ?? []));
        return state.submissions.filter((s) => s.kind !== 'withdraw' && !done.has(s.id));
      };
      for (let next = open(); next.length > 0; next = open()) {
        for (const s of next) {
          await ctx.effect(`model:${s.id}`, { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => {
            tag.push(`ran:${s.text}`);
            if (gate?.wait) await gate.wait;
            return 'ok';
          });
        }
        await ctx.refresh();
      }
    },
  };
}

/** Every `world.queue()` call's options, as the sender passed them. */
function spyQueue<W extends World>(world: W): { world: W; opts: Array<Record<string, unknown> | undefined> } {
  const opts: Array<Record<string, unknown> | undefined> = [];
  const spied = new Proxy(world, {
    get(t, key) {
      const value = (t as unknown as Record<string | symbol, unknown>)[key];
      if (key === 'queue') {
        return (name: string, payload: unknown, o?: Record<string, unknown>) => {
          opts.push(o);
          return (value as World['queue']).call(t, name as never, payload as never, o as never);
        };
      }
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(t) : value;
    },
  });
  return { world: spied, opts };
}

async function setup(opts: { hold?: LeasedWorldOptions['hold']; harness?: Harness; flowHarness?: Harness; runAhead?: boolean; queueHold?: boolean } = {}) {
  const base = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-hold-')), recoverActiveRuns: false });
  worlds.push(base);
  const leased = leasedWorld(base, { visibilityMs: 300, secondMs: 50, ...(opts.hold ? { hold: opts.hold } : {}) });
  const proc = hostProcess(leased);
  const spy = spyQueue(proc.world);
  const tag: string[] = [];
  const common = { anchors: vercelAnchors(), directDriveMs: 400, deadlineMarginMs: 100, contendedDelaySeconds: 1, marks: false, ...(opts.runAhead ? { runAhead: true } : {}) };
  const aLog: string[] = [];
  const a = durableSessions({ world: spy.world, harness: opts.harness ?? harness(tag), ...common, log: (m) => aLog.push(m), ...(opts.queueHold !== undefined ? { queueHold: opts.queueHold } : {}) });
  // Registered last, so it serves the queue.
  const b = durableSessions({ world: leased, harness: opts.flowHarness ?? opts.harness ?? harness(tag), ...common });
  const { sessionId } = await b.start({});
  return { a, b, broker: leased.broker, sessionId, tag, aLog, queueOpts: spy.opts, proc, leased, base };
}

const inputOf = (m: BrokerMessage) => (m.payload as { input?: { companion?: boolean; continues?: string } }).input ?? {};
const companions = (sent: BrokerMessage[]) => sent.filter((m) => inputOf(m).companion);
const models = async (b: { inspect(id: string): Promise<{ effectOrder: Array<{ name: string; status: string; anchor?: { k: string; m: string } }> }> }, id: string) =>
  (await b.inspect(id)).effectOrder.filter((e) => e.name === 'model');

describe('send-and-hold: the wake is held and driven here', () => {
  it('held: consume runs the session handler on the wake, under its msg anchor, with no companion', async () => {
    const { a, b, broker, sessionId, tag, queueOpts } = await setup({ hold: 'held' });
    await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    const [opts] = queueOpts;
    expect(opts).toMatchObject({ idempotencyKey: expect.stringMatching(new RegExp(`^${sessionId}:`)), experimental_hold: {} });
    expect(opts).not.toHaveProperty('delaySeconds');
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:hi']);
    // One message: the wake itself, undelayed, consumed by its sender. No companion, nothing delayed.
    const sent = broker.sent();
    expect(sent).toHaveLength(1);
    expect(companions(sent)).toEqual([]);
    expect(inputOf(sent[0]!)).toEqual({});
    const [hold] = broker.holds();
    expect(hold).toMatchObject({ messageId: sent[0]!.messageId, consumed: true });
    expect(hold!.delivery?.state).toBe('acked');
    // Driven under the held message's own anchor, not a deadline.
    const [model] = await models(b, sessionId);
    expect(model).toMatchObject({ status: 'completed', anchor: { k: 'msg', m: sent[0]!.messageId } });
    // Nothing is left for the queue: no second run.
    await broker.drain({ timeoutMs: 500 });
    expect(broker.deliveries()).toHaveLength(1);
    expect(tag).toEqual(['ran:hi']);
  });

  it('held, with waitUntil: send returns once the submission is durable; the held drive is handed to waitUntil', async () => {
    let open!: () => void;
    const gate: { wait?: Promise<void> } = { wait: new Promise<void>((r) => (open = r)) };
    const tag: string[] = [];
    const { a, broker, sessionId } = await setup({ hold: 'held', harness: harness(tag, gate) });
    const background: Promise<unknown>[] = [];
    await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop', waitUntil: (p) => background.push(p) });
    expect(background).toHaveLength(1);
    await until(() => (tag.includes('ran:hi') ? true : undefined));
    const driving = a.drive(sessionId);
    expect(broker.holds()[0]!.delivery?.state).toBe('running');
    open();
    await Promise.all([driving, ...background]);
    expect(broker.holds()[0]!.delivery?.state).toBe('acked');
    expect(tag).toEqual(['ran:hi']);
  });

  it('held, run ahead: the drive starts from the submit\'s pipeline and places the submission', async () => {
    const { a, b, broker, sessionId, tag } = await setup({ hold: 'held', runAhead: true });
    const id = await a.send(sessionId, 'fast', 'prompt', { wake: 'backstop' });
    expect(id).toMatch(/^sub_/);
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:fast']);
    const state = await b.inspect(sessionId);
    expect(state.placed.has(id)).toBe(true);
    expect(companions(broker.sent())).toEqual([]);
    expect((await models(b, sessionId))[0]?.anchor?.k).toBe('msg');
  });

  it('a held drive and a second held wake in the same process: one drive, which runs once more', async () => {
    const gate: { wait?: Promise<void> } = {};
    let open!: () => void;
    gate.wait = new Promise<void>((r) => (open = r));
    const tag: string[] = [];
    const { a, b, broker, sessionId } = await setup({ hold: 'held', harness: harness(tag, gate) });
    await a.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    const first = a.drive(sessionId);
    await until(() => (tag.includes('ran:one') ? true : undefined));
    gate.wait = undefined;
    await a.send(sessionId, 'two', 'prompt', { wake: 'backstop' });
    // The second held delivery finds the session driving here and acks.
    await a.drive(sessionId);
    expect(broker.holds().map((h) => h.delivery?.state)).toEqual(['running', 'acked']);
    open();
    await first;
    expect(tag).toEqual(['ran:one', 'ran:two']);
    expect((await models(b, sessionId)).map((m) => m.status)).toEqual(['completed', 'completed']);
  });

  it('a held drive that dies is recovered when its lease lapses, by the queue redelivering the same message', async () => {
    const gate: { wait?: Promise<void> } = { wait: new Promise<void>(() => {}) };
    const tagA: string[] = [];
    const tagB: string[] = [];
    const { a, b, broker, sessionId, proc } = await setup({ hold: 'held', harness: replaying(tagA, gate), flowHarness: replaying(tagB) });
    await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    void a.drive(sessionId);
    await until(() => (tagA.includes('ran:hi') ? true : undefined));
    // The chat Function dies mid-turn.
    proc.kill();
    broker.holds()[0]!.delivery!.kill();
    await broker.drain({ timeoutMs: 3000 });
    expect(tagB).toEqual(['ran:hi']);
    const redelivery = broker.deliveries()[1]!;
    expect(redelivery).toMatchObject({ messageId: broker.holds()[0]!.messageId, attempt: 2 });
    expect((await models(b, sessionId)).map((m) => m.status)).toEqual(['completed']);
  });

  it('held, harness unavailable here: the turn goes to the queue, not failed', async () => {
    const seen: string[] = [];
    const missing: Harness = {
      name: 'h',
      async drive(ctx) {
        if (ctx.pending().length > 0) throw new Unavailable('Native CLI binary for linux-x64 not found.', { code: 'binary_missing' });
      },
    };
    const { a, b, broker, sessionId } = await setup({ hold: 'held', harness: missing, flowHarness: harness(seen) });
    const id = await a.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    let state = await b.inspect(sessionId);
    expect(state.effectOrder.some((e) => e.name === UNAVAILABLE_STEP)).toBe(false);
    expect(state.placed.has(id)).toBe(false);
    const continuation = broker.sent().find((m) => inputOf(m).continues);
    expect(continuation && inputOf(continuation).continues).toBe(`msg:${broker.holds()[0]!.messageId}`);
    await broker.drain({ timeoutMs: 2000 });
    expect(seen).toEqual(['ran:one']);
    state = await b.inspect(sessionId);
    expect(state.placed.has(id)).toBe(true);
  });
});

describe('send-and-hold: not held, or not offered', () => {
  it('is detected from the World\'s capabilities', async () => {
    const { leased, base } = await setup({ hold: 'held' });
    expect(worldHolds(leased)).toBe(true);
    expect(worldHolds(base)).toBe(false);
  });

  for (const reason of ['unsupported', 'lost', 'concurrency_limited'] as const) {
    it(`not held (${reason}): nothing drives here; the queue delivers the wake, one hop`, async () => {
      const { a, broker, sessionId, tag, aLog } = await setup({ hold: reason });
      await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
      await a.drive(sessionId);
      expect(tag).toEqual([]);
      expect(aLog).toContain('submit: the queue did not hold the wake; it delivers it');
      const sent = broker.sent();
      expect(sent).toHaveLength(1);
      expect(companions(sent)).toEqual([]);
      expect(sent[0]!.availableAt).toBeLessThanOrEqual(Date.now());
      await broker.drain({ timeoutMs: 2000 });
      expect(tag).toEqual(['ran:hi']);
      expect(broker.deliveries()).toHaveLength(1);
    });
  }

  it('unsupported: later sends from this process go back to the deadline drive and its companion', async () => {
    const { a, broker, sessionId, tag, queueOpts } = await setup({ hold: 'unsupported' });
    await a.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    await broker.drain({ timeoutMs: 2000 });
    expect(tag).toEqual(['ran:one']);
    await a.send(sessionId, 'two', 'prompt', { wake: 'backstop' });
    expect(queueOpts[1]).not.toHaveProperty('experimental_hold');
    expect(companions(broker.sent())).toHaveLength(1);
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:one', 'ran:two']);
  });

  it('lost (one message, not the queue): the next send asks for a hold again', async () => {
    let answer: 'lost' | 'held' = 'lost';
    const { a, broker, sessionId, tag, queueOpts } = await setup({ hold: () => answer });
    await a.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    await broker.drain({ timeoutMs: 2000 });
    answer = 'held';
    await a.send(sessionId, 'two', 'prompt', { wake: 'backstop' });
    expect(queueOpts[1]).toHaveProperty('experimental_hold');
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:one', 'ran:two']);
    expect(companions(broker.sent())).toEqual([]);
  });

  it('not held, run ahead: one more wake once the submission is durable', async () => {
    const { a, b, broker, sessionId, tag } = await setup({ hold: 'lost', runAhead: true });
    const id = await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    expect(tag).toEqual([]);
    const keys = await until(() => {
      const k = broker.sent().map((m) => m.idempotencyKey);
      return k.length === 2 ? k : undefined;
    });
    expect(keys).toEqual([`${sessionId}:${id}`, `${sessionId}:${id}:durable`]);
    await broker.drain({ timeoutMs: 2000 });
    expect(tag).toEqual(['ran:hi']);
    expect((await b.inspect(sessionId)).placed.has(id)).toBe(true);
  });

  it('no capability: the deadline drive and its delayed companion, and no hold asked for', async () => {
    const { a, b, broker, sessionId, tag, queueOpts } = await setup();
    await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    expect(queueOpts).toHaveLength(1);
    expect(queueOpts[0]).not.toHaveProperty('experimental_hold');
    expect(queueOpts[0]).toHaveProperty('delaySeconds');
    expect(companions(broker.sent())).toHaveLength(1);
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:hi']);
    expect(broker.holds()).toEqual([]);
    expect((await models(b, sessionId))[0]?.anchor?.k).toBe('deadline');
  });

  it('queueHold: false keeps the companion on a World that holds', async () => {
    const { a, broker, sessionId, tag, queueOpts } = await setup({ hold: 'held', queueHold: false });
    await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:hi']);
    expect(queueOpts[0]).not.toHaveProperty('experimental_hold');
    expect(companions(broker.sent())).toHaveLength(1);
    expect(broker.holds()).toEqual([]);
  });

  it('a chaos submission with wake: backstop wakes no drive now: its companion only', async () => {
    const { a, broker, sessionId, queueOpts } = await setup({ hold: 'held' });
    await a.chaos(sessionId, { at: 'tool', afterMs: 100 }, { wake: 'backstop' });
    expect(queueOpts[0]).not.toHaveProperty('experimental_hold');
    expect(companions(broker.sent())).toHaveLength(1);
    expect(broker.holds()).toEqual([]);
  });

  it('wake: queue never asks for a hold', async () => {
    const { a, broker, sessionId, tag, queueOpts } = await setup({ hold: 'held' });
    await a.send(sessionId, 'hi', 'prompt');
    expect(queueOpts[0]).not.toHaveProperty('experimental_hold');
    await broker.drain({ timeoutMs: 2000 });
    expect(tag).toEqual(['ran:hi']);
    expect(broker.holds()).toEqual([]);
  });
});

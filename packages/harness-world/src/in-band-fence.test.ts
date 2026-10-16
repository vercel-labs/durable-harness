import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { CreateEventParams, Event, World } from '@workflow/world';
import { eventIdToSlot } from '@workflow/world';
import { createWorld as createLocalWorld } from '@workflow/world-local';
import { monotonicFactory } from 'ulid';
import { afterEach, describe, expect, it } from 'vitest';
import { encode } from './codec.ts';
import {
  type DriveContext,
  type DriverOptions,
  deliver,
  durableSessions,
  EffectFailure,
  type Harness,
  LogSuperseded,
  SessionLog,
  vercelAnchors,
} from './index.ts';
import { isInBandSuperseded } from './fence.ts';
import { stepNameFor } from './names.ts';
import { PipelinedLog } from './pipelined-log.ts';
import type { StepEnvelope } from './fold.ts';
import { hostProcess, leasedWorld } from './testing/index.ts';

/**
 * The World's in-band writer fence (vercel/workflow #4733, #4821) as the
 * kernel uses it: a drive's own writes are in-band and carry the World's
 * count; a write decided from a stale log is refused by the World, never
 * lands, and its drive stops; inputs are out-of-band and never fenced; a
 * World without the fence is never sent it. On world-local always, and on
 * world-postgres when `WORKFLOW_POSTGRES_URL` names a database with the World
 * migrations applied.
 */

const ulid = monotonicFactory();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until<T>(fn: () => Promise<T | undefined> | T | undefined, timeoutMs = 10_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await sleep(20);
  }
}

interface Created {
  runId: string;
  eventType: string;
  params?: CreateEventParams;
}

/** `base` with its event writes recorded and its queue kept in memory (no queue process in these tests). */
function recorded(base: World, options: { fence?: boolean } = {}) {
  const creates: Created[] = [];
  const queued: Array<{ name: string; payload: unknown; opts?: { idempotencyKey?: string; delaySeconds?: number } }> = [];
  const events = new Proxy(base.events, {
    get(target, key) {
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      if (key === 'create') {
        return (runId: string, data: { eventType: string }, params?: CreateEventParams) => {
          creates.push({ runId, eventType: data.eventType, ...(params ? { params } : {}) });
          return (value as (...a: unknown[]) => unknown).call(target, runId, data, params);
        };
      }
      // A World without the fence: no snapshot on any page.
      if (key === 'list' && options.fence === false) {
        return async (...args: unknown[]) => {
          const { snapshot: _snapshot, ...page } = (await (value as (...a: unknown[]) => Promise<{ snapshot?: unknown }>).apply(target, args)) as Record<string, unknown>;
          return page;
        };
      }
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  });
  const world = new Proxy(base, {
    get(target, key) {
      if (key === 'events') return events;
      if (key === 'queue') {
        return async (name: string, payload: unknown, opts?: { idempotencyKey?: string; delaySeconds?: number }) => {
          queued.push({ name, payload, ...(opts ? { opts } : {}) });
          return { messageId: `msg_${ulid()}` };
        };
      }
      if (key === 'capabilities' && options.fence === false) {
        const { inBandFence: _fence, ...rest } = (target.capabilities ?? {}) as Record<string, unknown>;
        return rest;
      }
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  }) as World;
  return { world, creates, queued };
}

/**
 * `base` as a World that fences but lists no snapshot (world-vercel today:
 * its backend fences, its list reports no counters). Its refusals keep their
 * counters unless `counters: false` (a World that gives no count at all).
 */
function noSnapshot(base: World, options: { counters?: boolean } = {}): World {
  const events = new Proxy(base.events, {
    get(target, key) {
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      if (key === 'list') {
        return async (...args: unknown[]) => {
          const { snapshot: _snapshot, ...page } = (await (value as (...a: unknown[]) => Promise<Record<string, unknown>>).apply(target, args)) as Record<string, unknown>;
          return page;
        };
      }
      if (key === 'create' && options.counters === false) {
        return async (...args: unknown[]) => {
          try {
            return await (value as (...a: unknown[]) => Promise<unknown>).apply(target, args);
          } catch (err) {
            if (!isInBandSuperseded(err)) throw err;
            const bare = new Error((err as Error).message);
            bare.name = 'InBandSupersededError';
            throw bare;
          }
        };
      }
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  });
  return new Proxy(base, {
    get(target, key) {
      if (key === 'events') return events;
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  }) as World;
}

/** `base` whose listings end after slot `limit()` (as world-vercel's sealed log ends a read at a slot still in flight); the snapshot is the World's, read before. */
function truncated(base: World, limit: () => number): World {
  const events = new Proxy(base.events, {
    get(target, key) {
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      if (key === 'list') {
        return async (...args: unknown[]) => {
          const page = (await (value as (...a: unknown[]) => Promise<{ data: Event[] }>).apply(target, args)) as { data: Event[] } & Record<string, unknown>;
          return { ...page, data: page.data.filter((e) => (eventIdToSlot(e.eventId) ?? 0) <= limit()), cursor: null, hasMore: false };
        };
      }
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  });
  return new Proxy(base, {
    get(target, key) {
      if (key === 'events') return events;
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  }) as World;
}

type Closable = World & { close?(): Promise<void> };

interface WorldCase {
  name: string;
  /** A World on a fresh store, and a way to open another instance on the same store (another host's). */
  make(): Promise<Closable & { reopen(): Promise<Closable> }>;
}

const WORLDS: WorldCase[] = [
  {
    name: 'world-local',
    make: async () => {
      const dataDir = await mkdtemp(join(tmpdir(), 'hw-fence-'));
      const open = () => createLocalWorld({ dataDir, recoverActiveRuns: false });
      // Another instance in this process shares world-local's in-process count; another OS process would not.
      return Object.assign(open(), { reopen: async () => open() });
    },
  },
];
if (process.env.WORKFLOW_POSTGRES_URL) {
  WORLDS.push({
    name: 'world-postgres',
    make: async () => {
      const { createWorld } = await import('@workflow/world-postgres');
      const open = () => createWorld({ connectionString: process.env.WORKFLOW_POSTGRES_URL!, jobPrefix: `hw_fence_${ulid().toLowerCase()}_` }) as unknown as Closable;
      // Another instance has its own pool: the count it fences with is the database's.
      return Object.assign(open(), { reopen: async () => open() });
    },
  });
}

let opened: Array<{ close?(): Promise<void> }> = [];
afterEach(async () => {
  for (const w of opened.splice(0)) await w.close?.().catch(() => {});
});

/** Every event of a run, in log order. */
async function allEvents(world: World, runId: string): Promise<Event[]> {
  const out: Event[] = [];
  let cursor: string | undefined;
  for (;;) {
    const page = await world.events.list({ runId, pagination: { sortOrder: 'asc', limit: 1000, ...(cursor ? { cursor } : {}) }, resolveData: 'none' });
    out.push(...page.data);
    if (!page.hasMore || !page.cursor) return out;
    cursor = page.cursor;
  }
}

/** The World's in-band count for a run, as a fresh reader sees it. */
async function worldCount(world: World, runId: string): Promise<number | undefined> {
  const page = await world.events.list({ runId, pagination: { sortOrder: 'asc', limit: 1 }, resolveData: 'none' });
  return (page as { snapshot?: { seqInBand: number } }).snapshot?.seqInBand;
}

const fenced = (c: Created) => c.params?.inBand === true;

/** One turn per submission: a replay-safe `model`, then a `tool.slow` that must not run twice. */
function turnHarness(s: { model: (ctx: DriveContext) => Promise<unknown>; tool?: () => Promise<unknown>; calls: { model: number; tool: number } }): Harness {
  return {
    name: 'turns',
    async drive(ctx) {
      // Every prompt, in order: an answered turn replays from the log (memoized), an unfinished one resumes.
      const pending = new Set(ctx.pending().map((p) => p.id));
      for (const sub of ctx.state().submissions) {
        if (sub.kind !== 'prompt' && sub.kind !== 'steer') continue;
        if (!pending.has(sub.id) && !ctx.state().placed.has(sub.id)) continue;
        await ctx.effect(`model:${sub.id}`, { name: 'model', replay: 'safe', input: sub.text, placed: [sub.id] }, async () => {
          s.calls.model++;
          return s.model(ctx);
        });
        try {
          await ctx.effect(`tool:${sub.id}`, { name: 'tool.slow', replay: 'never', input: sub.text }, async () => {
            s.calls.tool++;
            return (await s.tool?.()) ?? 'ran';
          });
        } catch (err) {
          if (!(err instanceof EffectFailure)) throw err;
        }
      }
    },
  };
}

/**
 * A World that answers like world-vercel's backend, for the count alone:
 * its list carries a snapshot only with `snapshot: true`, its creates report
 * no `allocated`, a born-running intent comes back with `stepCreated`, and a
 * probe is refused with the counters.
 */
function vercelShaped(options: { snapshot: boolean; seq: number; seqInBand: number }): World {
  const events: Event[] = [];
  let slot = 0;
  const mint = (eventType: string, correlationId?: string): Event => ({ eventId: `evnt_${String(++slot).padStart(26, '0')}`, runId: 'wrun_x', eventType, ...(correlationId ? { correlationId } : {}), eventData: {}, createdAt: new Date() }) as unknown as Event;
  for (let i = 0; i < options.seq; i++) events.push(mint(i === 0 ? 'run_created' : 'run_started'));
  return {
    capabilities: { inBandFence: true },
    events: {
      async list() {
        return { data: [...events], cursor: null, hasMore: false, ...(options.snapshot ? { snapshot: { seq: options.seq, seqInBand: options.seqInBand } } : {}) };
      },
      async create(_runId: string, data: { eventType: string; correlationId?: string }, params?: CreateEventParams) {
        if (params?.inBand && params.expectedSeqInBand === Number.MAX_SAFE_INTEGER) {
          throw Object.assign(new Error('superseded'), { name: 'InBandSupersededError', status: 412, code: 'in-band-superseded', seq: options.seq, seqInBand: options.seqInBand });
        }
        if (data.eventType === 'step_started') events.push(mint('step_created', data.correlationId));
        const event = mint(data.eventType, data.correlationId);
        events.push(event);
        return { event, ...(data.eventType === 'step_started' ? { stepCreated: true } : {}) };
      },
    },
  } as unknown as World;
}

describe('in-band count without `allocated`', () => {
  const intent = () => ({ eventType: 'step_started' as const, correlationId: `step_${ulid()}`, eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: `k${ulid()}`, replay: 'safe', input: null, epoch: 0, drv: 'drv_x' } satisfies StepEnvelope) } });
  it("on a World that lists no snapshot (world-vercel: one position per event), a born-running intent takes two positions and an outcome one", async () => {
    const log = new SessionLog(vercelShaped({ snapshot: false, seq: 2, seqInBand: 1 }), 'wrun_x');
    await log.refresh();
    expect(await log.probe()).toBe(true);
    expect(log.inBandCount).toBe(1);
    expect(log.arm('drv_x')).toBe(true);
    const step = intent();
    await log.write(step);
    expect(log.inBandCount).toBe(3);
    await log.write({ eventType: 'step_completed', correlationId: step.correlationId, eventData: { stepName: stepNameFor('tool.x'), result: encode('ok') } });
    expect(log.inBandCount).toBe(4);
  });
  it('on a World that lists a snapshot and reports no `allocated` (world-postgres: one position per create), a born-running intent takes one', async () => {
    const log = new SessionLog(vercelShaped({ snapshot: true, seq: 2, seqInBand: 1 }), 'wrun_x');
    await log.refresh();
    expect(log.inBandCount).toBe(1);
    expect(log.arm('drv_x')).toBe(true);
    await log.write(intent());
    expect(log.inBandCount).toBe(2);
  });
});

/**
 * A World that fences for real and counts the way it says: it lists a
 * snapshot, reports no `allocated`, and advances its in-band count by one
 * per event (`perEvent`: world-vercel's backend, should it list snapshots
 * without `allocated`) or one per create (world-postgres). It refuses a stale
 * in-band write with both counters and writes nothing.
 */
function fencing(options: { perEvent: boolean }) {
  const events: Event[] = [];
  let slot = 0;
  let seqInBand = 1;
  const mint = (eventType: string, correlationId?: string): Event => ({ eventId: `evnt_${String(++slot).padStart(26, '0')}`, runId: 'wrun_y', eventType, ...(correlationId ? { correlationId } : {}), eventData: {}, createdAt: new Date() }) as unknown as Event;
  events.push(mint('run_created'), mint('run_started'));
  const refusals: number[] = [];
  const world = {
    capabilities: { inBandFence: true },
    events: {
      async list() {
        return { data: [...events], cursor: null, hasMore: false, snapshot: { seq: slot, seqInBand } };
      },
      async create(_runId: string, data: { eventType: string; correlationId?: string }, params?: CreateEventParams) {
        if (params?.inBand) {
          if (params.expectedSeqInBand !== seqInBand) {
            refusals.push(params.expectedSeqInBand ?? -1);
            throw Object.assign(new Error('superseded'), { name: 'InBandSupersededError', status: 412, code: 'in-band-superseded', seq: slot, seqInBand });
          }
        }
        const bornRunning = data.eventType === 'step_started';
        if (bornRunning) events.push(mint('step_created', data.correlationId));
        const event = mint(data.eventType, data.correlationId);
        events.push(event);
        if (params?.inBand) seqInBand += bornRunning && options.perEvent ? 2 : 1;
        return { event, ...(bornRunning ? { stepCreated: true } : {}) };
      },
    },
  } as unknown as World;
  /** Another writer's in-band write, at the World's count (a second drive). */
  const sibling = () => {
    events.push(mint('step_created', `step_${ulid()}`));
    seqInBand += 1;
  };
  return { world, refusals, sibling, count: () => seqInBand };
}

describe('in-band count from a snapshot, without `allocated`, on a World that counts per event', () => {
  const intent = () => ({ eventType: 'step_started' as const, correlationId: `step_${ulid()}`, eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: `k${ulid()}`, replay: 'safe', input: null, epoch: 0, drv: 'drv_y' } satisfies StepEnvelope) } });
  const outcome = (correlationId: string) => ({ eventType: 'step_completed' as const, correlationId, eventData: { stepName: stepNameFor('tool.x'), result: encode('ok') } });

  it("a born-running intent the log counted as one is corrected by the World's refusal of the next write (nothing else written), and the write goes through: no superseded", async () => {
    const w = fencing({ perEvent: true });
    const log = new SessionLog(w.world, 'wrun_y');
    const notes: string[] = [];
    log.onFence = (n) => notes.push(n.kind);
    await log.refresh();
    expect(log.arm('drv_y')).toBe(true);
    const first = intent();
    const startedAt = w.count();
    await log.write(first);
    await log.write(outcome(first.correlationId));
    expect(log.superseded).toBeUndefined();
    expect(log.inBandCount).toBe(w.count());
    expect(w.refusals).toHaveLength(1);
    expect(notes).toContain('recount');
    // The drive's fencing token is its first accepted write's count, and the recount and resend leave it there.
    expect(log.fenceOf('drv_y')).toEqual({ count: startedAt, fenced: true });
    // From then on it counts a born-running intent as the World does: no refusal at all.
    const second = intent();
    await log.write(second);
    await log.write(outcome(second.correlationId));
    expect(w.refusals).toHaveLength(1);
    expect(log.inBandCount).toBe(w.count());
    expect(log.state.effectOrder.map((e) => e.status)).toEqual(['completed', 'completed']);
  });

  it("a drive whose first write is resent at the World's recounted count holds that count as its token, exact although this log guessed", async () => {
    const w = fencing({ perEvent: true });
    const log = new SessionLog(w.world, 'wrun_y');
    await log.refresh();
    expect(log.arm('drv_x')).toBe(true);
    const first = intent();
    await log.write(first);
    // drv_x lets go after a guessed advance; drv_y arms at the guess (one behind the World).
    log.disarm('drv_x');
    expect(log.inBandCount).toBe(w.count() - 1);
    expect(log.arm('drv_y')).toBe(true);
    const recounted = w.count();
    await log.write(outcome(first.correlationId));
    expect(w.refusals).toHaveLength(1);
    expect(log.fenceOf('drv_y')).toEqual({ count: recounted, fenced: true });
    // Later writes move the World's count, not the token.
    const second = intent();
    await log.write(second);
    expect(w.count()).toBeGreaterThan(recounted + 1);
    expect(log.fenceOf('drv_y')).toEqual({ count: recounted, fenced: true });
    expect(log.fenceOf('drv_x')).toBeUndefined();
  });

  it('a refusal after a guessed advance still supersedes the drive when another writer wrote in between', async () => {
    const w = fencing({ perEvent: true });
    const log = new SessionLog(w.world, 'wrun_y');
    await log.refresh();
    expect(log.arm('drv_y')).toBe(true);
    const first = intent();
    await log.write(first);
    w.sibling();
    await expect(log.write(outcome(first.correlationId))).rejects.toBeInstanceOf(LogSuperseded);
    expect(log.superseded).toBeInstanceOf(LogSuperseded);
  });

  it('a World that counts per create (world-postgres) is never refused for it', async () => {
    const w = fencing({ perEvent: false });
    const log = new SessionLog(w.world, 'wrun_y');
    await log.refresh();
    expect(log.arm('drv_y')).toBe(true);
    for (let i = 0; i < 2; i++) {
      const step = intent();
      await log.write(step);
      await log.write(outcome(step.correlationId));
    }
    expect(w.refusals).toEqual([]);
    expect(log.inBandCount).toBe(w.count());
  });
});

for (const wc of WORLDS) {
  describe(`in-band fence on ${wc.name}`, () => {
    async function open(options: { fence?: boolean } = {}) {
      const base = await wc.make();
      opened.push(base);
      expect(base.capabilities?.inBandFence).toBe(true);
      return { base, ...recorded(base, options) };
    }

    it("tracks the World's count exactly through every kind of in-band write, and sends inputs unfenced", async () => {
      const { world, creates } = await open();
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      // `sessions.start` wrote nothing in-band: the run is still at its creation count.
      expect(await worldCount(world, sessionId)).toBe(1);
      expect(creates.filter(fenced)).toEqual([]);

      const log = new SessionLog(world, sessionId);
      await log.refresh();
      expect(log.inBandCount).toBe(1);
      expect(log.arm('drv_test')).toBe(true);
      const step = `step_${ulid()}`;
      const envelope: StepEnvelope = { key: 'k', replay: 'safe', input: null, epoch: 0, drv: 'drv_test' };
      // A born-running intent (two events, one in-band position on these Worlds), its retry, the retry's intent and outcome.
      await log.write({ eventType: 'step_started', correlationId: step, eventData: { stepName: stepNameFor('tool.x'), input: encode(envelope) } });
      expect(log.inBandCount).toBe(await worldCount(world, sessionId));
      await log.write({ eventType: 'step_retrying', correlationId: step, eventData: { stepName: stepNameFor('tool.x'), error: encode({ message: 'interrupted' }) } });
      expect(log.inBandCount).toBe(await worldCount(world, sessionId));
      // An input through the armed log itself (a submit sharing the drive's log): out-of-band.
      await sessions.send(sessionId, 'hello');
      await log.write({ eventType: 'hook_received', correlationId: log.state.inbox!.hookId, eventData: { token: log.state.inbox!.token, payload: encode({ kind: 'prompt', text: 'via the drive log', nonce: ulid() }) } });
      expect(log.inBandCount).toBe(await worldCount(world, sessionId));
      const retry = `step_${ulid()}`;
      await log.write({ eventType: 'step_started', correlationId: retry, eventData: { stepName: stepNameFor('tool.x'), input: encode({ ...envelope, retryOf: step }) } });
      await log.write({ eventType: 'step_completed', correlationId: retry, eventData: { stepName: stepNameFor('tool.x'), result: encode('ok') } });
      expect(log.inBandCount).toBe(await worldCount(world, sessionId));
      expect(log.inBandCount).toBe(5);
      // A definitive refusal (a second outcome for the same step) is not counted.
      await expect(log.write({ eventType: 'step_completed', correlationId: retry, eventData: { stepName: stepNameFor('tool.x'), result: encode('again') } })).rejects.toThrow();
      expect(log.inBandCount).toBe(await worldCount(world, sessionId));

      const sent = creates.filter((c) => c.runId === sessionId && c.eventType !== 'run_created');
      expect(sent.filter((c) => c.eventType === 'hook_received').every((c) => !fenced(c))).toBe(true);
      expect(sent.filter((c) => c.eventType.startsWith('step_')).every(fenced)).toBe(true);
    });

    it('two drives decide from one log: the stale one is refused at its first write, never starts its effect, writes nothing, and stops', async () => {
      const { world, creates, queued } = await open();
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      await sessions.send(sessionId, 'go');
      const logs: Array<{ m: string; f?: Record<string, unknown> }> = [];
      // Two processes (one World object each, so each has its own drive registry; the store, and its count, is shared).
      const a = hostProcess(world);
      const b = hostProcess(world);
      const base: DriverOptions = { world, harness, anchors: vercelAnchors(), contendedDelaySeconds: 1, log: (m, f) => logs.push({ m, ...(f ? { f } : {}) }) };
      // B loads, then (inside its admission) A drives the whole turn; B goes on from the log it loaded.
      const bDone = deliver(
        {
          ...base,
          world: b.world,
          admit: async () => {
            await deliver({ ...base, world: a.world }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } });
            return { admit: true };
          },
        },
        sessionId,
        { anchor: { k: 'msg', m: 'msg_b' } }
      );
      expect(await bDone).toBeUndefined();

      const state = await sessions.inspect(sessionId);
      const sub = state.submissions[0]!.id;
      expect(state.effects.get(`model:${sub}`)).toMatchObject({ status: 'completed', output: 'plan' });
      expect(state.effects.get(`tool:${sub}`)).toMatchObject({ status: 'completed', output: 'ran' });
      // The refused intent never started B's effect.
      expect(calls).toEqual({ model: 1, tool: 1 });
      expect(logs.map((l) => l.m)).toContain('in-band fence: a write was refused');
      expect(logs.map((l) => l.m)).toContain('yield: superseded');
      // Nothing B decided landed: every step event in the log is A's.
      const drives = new Set(state.effectOrder.map((r) => r.driver).filter(Boolean));
      expect(drives.size).toBe(1);
      const refused = creates.filter((c) => fenced(c) && c.params!.expectedSeqInBand === 1);
      expect(refused.length).toBe(2); // A's first intent, and B's (refused)
      // B had no open work and no grant: it asked for a look later (as `contended`), nothing more.
      expect(queued.filter((q) => q.opts?.idempotencyKey?.includes(':superseded:'))).toHaveLength(1);
      // The session goes on: a second prompt is answered by a later drive at the World's count.
      await sessions.send(sessionId, 'again');
      await deliver(base, sessionId, { anchor: { k: 'msg', m: 'msg_c' } });
      const after = await sessions.inspect(sessionId);
      expect(after.effects.get(`tool:${after.submissions[1]!.id}`)?.status).toBe('completed');
    });

    it('without the fence (a World that does not declare it) the same race lands the stale intent, and the fold has to make it inert', async () => {
      const { world, creates } = await open({ fence: false });
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      await sessions.send(sessionId, 'go');
      const a = hostProcess(world);
      const b = hostProcess(world);
      const base: DriverOptions = { world, harness, anchors: vercelAnchors(), contendedDelaySeconds: 1 };
      await deliver(
        { ...base, world: b.world, admit: async () => (await deliver({ ...base, world: a.world }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } }), { admit: true }) },
        sessionId,
        { anchor: { k: 'msg', m: 'msg_b' } }
      );
      // Never sent the fence.
      expect(creates.filter((c) => c.params?.inBand !== undefined || c.params?.expectedSeqInBand !== undefined)).toEqual([]);
      const state = await sessions.inspect(sessionId);
      // B's intent landed (and its "superseded" cleanup with it); the first-writer rule made it inert.
      expect(state.superseded.size).toBeGreaterThan(0);
      expect(state.effects.get(`tool:${state.submissions[0]!.id}`)?.status).toBe('completed');
      expect(calls.tool).toBe(1);
    });

    for (const [fence, runAhead, probe] of [[true, false, false], [true, true, false], [false, false, false], ...(wc.name === 'world-local' ? [[true, false, true] as const] : [])] as const) {
      it(
        (probe ? 'no snapshot (world-vercel), counts from probes: ' : '') +
          (runAhead ? 'run-ahead: ' : '') +
          (fence
            ? "a frozen holder's late outcome is refused by the World after its redelivery took over; the takeover's result stands and the session goes on"
            : "without the fence the frozen holder's late outcome lands (the fold ignores it)"),
        async () => {
          const made = await open();
          const recordedWorld = probe ? recorded(noSnapshot(made.base)).world : made.world;
          const leased = leasedWorld(recordedWorld, { visibilityMs: 200 });
          let releaseFirst!: (v: string) => void;
          const firstModel = new Promise<string>((r) => (releaseFirst = r));
          const calls = { model: 0, tool: 0 };
          const harness = turnHarness({ model: async () => (calls.model === 1 ? firstModel : 'from the takeover'), calls });
          const logs: string[] = [];
          // No hints: the frozen holder learns of the takeover only from its own write (or a periodic read, 15 s away).
          const sessions = durableSessions({ world: leased, harness, anchors: vercelAnchors(), inBandFence: fence, hints: false, runAhead, log: (m) => logs.push(m) });
          const { sessionId } = await sessions.start({ harness: 'turns' });
          await sessions.send(sessionId, 'go');
          const first = leased.broker.deliver()!;
          const modelRunning = async () => ((await sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'model')?.status === 'running' ? true : undefined);
          await until(modelRunning);
          const staleStep = (await sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'model')!.stepId;
          // The holder freezes (its World calls wait), its model call returns meanwhile, and its lease lapses.
          first.freeze();
          releaseFirst('from the frozen holder');
          const again = await until(() => leased.broker.deliver());
          expect(again).toMatchObject({ messageId: first.messageId, attempt: 2 });
          await again.done;
          let state = await sessions.inspect(sessionId);
          const sub = state.submissions[0]!.id;
          expect(state.effectOrder.find((e) => e.name === 'recover')).toMatchObject({ status: 'completed' });
          expect(state.effects.get(`model:${sub}`)).toMatchObject({ status: 'completed', output: 'from the takeover' });
          expect(state.effects.get(`tool:${sub}`)).toMatchObject({ status: 'completed' });
          // The holder thaws: its outcome write goes out now.
          first.thaw();
          await until(async () => (logs.includes(fence ? 'in-band fence: a write was refused' : 'preempted') || (await allEvents(recordedWorld, sessionId)).some((e) => e.correlationId === staleStep && e.eventType === 'step_completed') ? true : undefined), 5000);
          await sleep(300);
          const late = (await allEvents(recordedWorld, sessionId)).filter((e) => e.correlationId === staleStep && e.eventType === 'step_completed');
          expect(late).toHaveLength(fence ? 0 : 1);
          // Fenced, it was the World that refused it (not the fold after the fact), and the holder stopped as preempted (the epoch had moved).
          if (fence) {
            expect(logs).toContain('in-band fence: a write was refused');
            expect(logs).not.toContain('commit.retry');
          }
          state = await sessions.inspect(sessionId);
          expect(state.effects.get(`model:${sub}`)).toMatchObject({ status: 'completed', output: 'from the takeover' });
          expect(calls.tool).toBe(1);
          // Recovery was correct: the next turn runs on the World's count, refused by nothing.
          await sessions.send(sessionId, 'next');
          const next = await until(() => leased.broker.deliver());
          await next.done;
          state = await sessions.inspect(sessionId);
          expect(state.effects.get(`tool:${state.submissions[1]!.id}`)?.status).toBe('completed');
        }
      );
    }

    it('inputs are never fenced: prompts and steers land while a drive is in an effect, from any writer, and the drive is not refused for them', async () => {
      const { world, creates } = await open();
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      let started!: () => void;
      const inModel = new Promise<void>((r) => (started = r));
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => {
        if (calls.model !== 1) return 'plan 2';
        started();
        await gate;
        return 'plan';
      }, calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      // A writer that loaded first and never refreshes: stale for any in-band purpose.
      const stale = new SessionLog(world, sessionId);
      await stale.refresh();
      await sessions.send(sessionId, 'go');
      const logs: string[] = [];
      const driving = deliver({ world, harness, anchors: vercelAnchors(), log: (m) => logs.push(m) }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } });
      await until(async () => ((await sessions.inspect(sessionId)).effectOrder.some((e) => e.name === 'model' && e.status === 'running') ? true : undefined));
      // A running record may be visible before the effect's start writes finish.
      // Read the count only once the held model function is actually entered.
      await inModel;
      const before = await worldCount(world, sessionId);
      await sessions.send(sessionId, 'a steer from another instance', 'steer');
      await sessions.send(sessionId, 'a second prompt');
      await stale.write({ eventType: 'hook_received', correlationId: stale.state.inbox!.hookId, eventData: { token: stale.state.inbox!.token, payload: encode({ kind: 'prompt', text: 'from a stale writer', nonce: ulid() }) } });
      expect(await worldCount(world, sessionId)).toBe(before);
      release();
      await driving;
      expect(logs).not.toContain('in-band fence: a write was refused');
      const state = await sessions.inspect(sessionId);
      expect(state.submissions).toHaveLength(4);
      for (const s of state.submissions.filter((x) => x.kind === 'prompt')) expect(state.effects.get(`tool:${s.id}`)?.status).toBe('completed');
      expect(creates.filter((c) => c.eventType === 'hook_received').every((c) => !fenced(c))).toBe(true);
      expect(creates.filter((c) => c.eventType === 'step_started').some(fenced)).toBe(true);
    });

    it('a refusal mid run-ahead pipeline drops the queued in-band writes unsent, and the submission queued behind them still lands', async () => {
      const { world } = await open();
      const sessions = durableSessions({ world, harness: turnHarness({ model: async () => 'x', calls: { model: 0, tool: 0 } }) });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      const log = new SessionLog(world, sessionId);
      await log.refresh();
      expect(log.arm('drv_a')).toBe(true);
      // Another drive wrote in-band since this log's count.
      const other = new SessionLog(world, sessionId);
      await other.refresh();
      expect(other.arm('drv_b')).toBe(true);
      const intent = (drv: string) => ({
        eventType: 'step_started' as const,
        correlationId: `step_${ulid()}`,
        eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: `k:${drv}:${ulid()}`, replay: 'safe', input: null, epoch: 0, drv } satisfies StepEnvelope) },
      });
      await other.write(intent('drv_b'));
      const pipe = PipelinedLog.for(log);
      const first = pipe.enqueue(intent('drv_a'));
      const second = pipe.enqueue(intent('drv_a'));
      const cid = `sub_${ulid()}`;
      const submission = pipe.enqueue(
        { eventType: 'hook_received', correlationId: log.state.inbox!.hookId, eventData: { token: log.state.inbox!.token, payload: encode({ kind: 'prompt', text: 'queued behind', nonce: ulid(), cid }) } },
        { survives: true, landed: (st) => st.submissions.some((x) => x.id === cid) }
      );
      await expect(first).rejects.toThrow(/in-band fence/);
      await expect(second).rejects.toThrow();
      await submission;
      expect(log.superseded).toBeInstanceOf(LogSuperseded);
      // A later in-band write of this drive is refused here, unsent.
      await expect(log.write(intent('drv_a'))).rejects.toBeInstanceOf(LogSuperseded);
      const events = await allEvents(world, sessionId);
      const drvs = events.filter((e) => e.eventType === 'step_created').length;
      expect(drvs).toBe(1); // drv_b's intent only
      const fresh = new SessionLog(world, sessionId);
      await fresh.refresh();
      expect(fresh.state.submissions.some((x) => x.id === cid)).toBe(true);
    });

    it('a World that declares the fence but gives no count (no snapshot, no counters on a refusal) is written to unfenced', async () => {
      const base = await wc.make();
      opened.push(base);
      const { world, creates } = recorded(noSnapshot(base, { counters: false }));
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns', prompt: 'go' });
      const logs: string[] = [];
      await deliver({ world, harness, anchors: vercelAnchors(), log: (m) => logs.push(m) }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } });
      // One probe (refused, carrying nothing), then nothing more is sent in-band.
      expect(creates.filter((c) => c.params?.inBand !== undefined).map((c) => c.eventType)).toEqual(['attr_set']);
      expect(logs).not.toContain('in-band fence: armed');
      expect(calls).toEqual({ model: 1, tool: 1 });
      expect((await allEvents(base, sessionId)).some((e) => e.eventType === 'attr_set')).toBe(false);
    });

    // world-local's refusals carry both counters; world-postgres's carry no `seq`, so a probe cannot show its count is covered (it lists snapshots).
    const probes = wc.name === 'world-local';

    it(`a World that fences but lists no snapshot (world-vercel): ${probes ? 'a drive reads the count from a probe the fence refuses, then fences every write at the World\'s count; the probe writes nothing' : 'a refusal without `seq` gives no count, and the drive writes unfenced'}`, async () => {
      const base = await wc.make();
      opened.push(base);
      const { world, creates } = recorded(noSnapshot(base));
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns', prompt: 'go' });
      const log = new SessionLog(world, sessionId);
      await log.refresh();
      expect(log.inBandCount).toBeUndefined();
      expect(log.needsProbe).toBe(true);
      expect(await log.probe()).toBe(probes);
      if (probes) expect(log.inBandCount).toBe(await worldCount(base, sessionId));
      const logs: string[] = [];
      await deliver({ world, harness, anchors: vercelAnchors(), log: (m) => logs.push(m) }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } });
      expect(calls).toEqual({ model: 1, tool: 1 });
      const probesSent = creates.filter((c) => c.eventType === 'attr_set');
      expect(probesSent.every((c) => c.params?.inBand === true && c.params.expectedSeqInBand === Number.MAX_SAFE_INTEGER)).toBe(true);
      // Nothing of a probe landed, and it moved no count.
      expect((await allEvents(base, sessionId)).some((e) => e.eventType === 'attr_set')).toBe(false);
      const steps = creates.filter((c) => c.runId === sessionId && c.eventType.startsWith('step_'));
      expect(steps.length).toBeGreaterThan(0);
      if (probes) {
        expect(logs).toContain('in-band fence: armed');
        expect(steps.every(fenced)).toBe(true);
        // Each drive write was at the World's count: the last one leaves the World where the kernel thinks it is.
        expect(steps.at(-1)!.params!.expectedSeqInBand).toBe((await worldCount(base, sessionId))! - 1);
      } else {
        expect(steps.some(fenced)).toBe(false);
      }
    });

    if (probes) it('a World that fences but lists no snapshot: two drives that each probed and decided race the same turn, and the one that writes second is refused at its first write and stops', async () => {
      const base = await wc.make();
      opened.push(base);
      const { world, creates } = recorded(noSnapshot(base));
      const calls = { model: 0, tool: 0 };
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      // Both drives get as far as their first write (each having probed, read and decided) before either writes.
      let arrived = 0;
      const harness: Harness = {
        name: 'turns',
        async drive(ctx) {
          for (const sub of ctx.pending()) {
            if (++arrived === 2) release();
            await gate;
            await ctx.effect(`model:${sub.id}`, { name: 'model', replay: 'safe', input: sub.text, placed: [sub.id] }, async () => (calls.model++, 'plan'));
            await ctx.effect(`tool:${sub.id}`, { name: 'tool.slow', replay: 'never', input: sub.text }, async () => (calls.tool++, 'ran'));
          }
        },
      };
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      await sessions.send(sessionId, 'go');
      const logs: string[] = [];
      const a = hostProcess(world);
      const b = hostProcess(world);
      const opts = (w: World): DriverOptions => ({ world: w, harness, anchors: vercelAnchors(), contendedDelaySeconds: 1, log: (m) => logs.push(m) });
      await Promise.all([
        deliver(opts(a.world), sessionId, { anchor: { k: 'msg', m: 'msg_a' } }),
        deliver(opts(b.world), sessionId, { anchor: { k: 'msg', m: 'msg_b' } }),
      ]);
      expect(creates.filter((c) => c.eventType === 'attr_set')).toHaveLength(2);
      expect(logs.filter((m) => m === 'in-band fence: armed')).toHaveLength(2);
      expect(logs.filter((m) => m === 'in-band fence: a write was refused')).toHaveLength(1);
      expect(logs).toContain('yield: superseded');
      expect(calls).toEqual({ model: 1, tool: 1 });
      const state = await sessions.inspect(sessionId);
      expect(new Set(state.effectOrder.map((r) => r.driver)).size).toBe(1);
      expect(state.superseded.size).toBe(0);
    });

    if (probes) it('a World that fences but lists no snapshot: a log kept between turns (a route\'s) probes again once another process drove, so its next drive is not refused at its first write', async () => {
      const base = await wc.make();
      opened.push(base);
      const { world } = recorded(noSnapshot(base));
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      // The route's log, with a count it probed.
      const kept = new SessionLog(world, sessionId);
      await kept.refresh();
      expect(await kept.probe()).toBe(true);
      // Another process drives a turn: in-band writes the kept log does not count.
      await sessions.send(sessionId, 'one');
      const other = hostProcess(world);
      await deliver({ world: other.world, harness, anchors: vercelAnchors() }, sessionId, { anchor: { k: 'msg', m: 'msg_1' } });
      expect(calls.model).toBe(1);
      // The next prompt reaches the route: its log folds the other process's turn and lets its count go.
      await sessions.send(sessionId, 'two');
      await kept.refresh();
      expect(kept.needsProbe).toBe(true);
      const logs: string[] = [];
      await deliver({ world, harness, anchors: vercelAnchors(), log: (m) => logs.push(m) }, sessionId, { anchor: { k: 'msg', m: 'msg_2' }, log: kept });
      expect(logs).toContain('in-band fence: armed');
      expect(logs).not.toContain('in-band fence: a write was refused');
      expect(logs).not.toContain('yield: superseded');
      expect(calls).toEqual({ model: 2, tool: 2 });
      expect(kept.inBandCount).toBe(await worldCount(base, sessionId));
    });

    it('a read that stops short of its snapshot (a slot still in flight) adopts no count: the count would include an in-band write the reader never folded', async () => {
      const { world } = await open();
      const sessions = durableSessions({ world, harness: turnHarness({ model: async () => 'x', calls: { model: 0, tool: 0 } }) });
      const { sessionId } = await sessions.start({ harness: 'turns', prompt: 'go' });
      const before = (await allEvents(world, sessionId)).length;
      // Another drive writes an intent in-band (two slots: its step_created and step_started).
      const other = new SessionLog(world, sessionId);
      await other.refresh();
      expect(other.arm('drv_other')).toBe(true);
      await other.write({ eventType: 'step_started', correlationId: `step_${ulid()}`, eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: 'other', replay: 'safe', input: null, epoch: 0, drv: 'drv_other' } satisfies StepEnvelope) } });
      let limit = before;
      const log = new SessionLog(truncated(world, () => limit), sessionId);
      await log.refresh();
      expect(log.state.eventCount).toBe(before);
      expect(log.inBandCount).toBeUndefined();
      expect(log.arm('drv_stale')).toBe(false);
      log.disarm('drv_stale');
      // Once a read covers it, the count is the World's.
      limit = Number.MAX_SAFE_INTEGER;
      await log.refresh();
      expect(log.inBandCount).toBe(await worldCount(world, sessionId));
    });

    it("another drive's in-band write that the delivery reads after deciding (re-reading the log when a fact it finishes was finished first) does not lend it the count: its first write is refused", async () => {
      const { base, world } = await open();
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => 'plan', calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      await sessions.send(sessionId, 'go');
      // A half-written fact (a drive's close), which the delivery finishes while it decides.
      const seed = new SessionLog(base, sessionId);
      await seed.refresh();
      const closeStep = `step_${ulid()}`;
      await seed.write({ eventType: 'step_started', correlationId: closeStep, eventData: { stepName: stepNameFor('close'), input: encode({ key: 'close:drv_gone', replay: 'safe', input: { drv: 'drv_gone' }, epoch: 0 } satisfies StepEnvelope) } });
      // Just before that write goes out, another drive opens work of its own in-band at the same epoch.
      let injected = false;
      const racing = new Proxy(world, {
        get(t, key) {
          if (key !== 'events') return (t as unknown as Record<string | symbol, unknown>)[key];
          return new Proxy(t.events, {
            get(et, ek) {
              const v = (et as unknown as Record<string | symbol, unknown>)[ek];
              if (ek !== 'create') return typeof v === 'function' ? (v as (...a: unknown[]) => unknown).bind(et) : v;
              return async (runId: string, data: { correlationId?: string }, params?: CreateEventParams) => {
                if (!injected && data.correlationId === closeStep) {
                  injected = true;
                  const other = new SessionLog(base, sessionId);
                  await other.refresh();
                  expect(other.arm('drv_other')).toBe(true);
                  await other.write({ eventType: 'step_started', correlationId: `step_${ulid()}`, eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: 'held', replay: 'safe', input: null, epoch: 0, drv: 'drv_other', anchor: { k: 'msg', m: 'msg_other' } } satisfies StepEnvelope) } });
                  // And someone finishes the fact first, so this write is refused and the delivery re-reads the log.
                  await seed.write({ eventType: 'step_completed', correlationId: closeStep, eventData: { stepName: stepNameFor('close'), result: encode({ drv: 'drv_gone' }) } });
                }
                return (v as (...a: unknown[]) => Promise<unknown>).call(et, runId, data, params);
              };
            },
          });
        },
      }) as World;
      const logs: string[] = [];
      await deliver({ world: racing, harness, anchors: vercelAnchors(), contendedDelaySeconds: 1, log: (m) => logs.push(m) }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } });
      expect(injected).toBe(true);
      // It decided from a view without the other drive's open work; the World refused the decision, so its harness never ran.
      expect(logs).toContain('in-band fence: a write was refused');
      expect(calls).toEqual({ model: 0, tool: 0 });
      const state = await sessions.inspect(sessionId);
      expect(state.effectOrder.filter((r) => r.name === 'model')).toEqual([]);
    });

    it('two drives on two World instances (two hosts on one store) race the same turn: one of them is refused', async () => {
      const first = await open();
      const other = await first.base.reopen();
      opened.push(other);
      const second = recorded(other);
      const calls = { model: 0, tool: 0 };
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      // Both drives get as far as their first write before either writes.
      let arrived = 0;
      const harness: Harness = {
        name: 'turns',
        async drive(ctx) {
          for (const sub of ctx.pending()) {
            if (++arrived === 2) release();
            await gate;
            await ctx.effect(`model:${sub.id}`, { name: 'model', replay: 'safe', input: sub.text, placed: [sub.id] }, async () => (calls.model++, 'plan'));
            await ctx.effect(`tool:${sub.id}`, { name: 'tool.slow', replay: 'never', input: sub.text }, async () => (calls.tool++, 'ran'));
          }
        },
      };
      const sessions = durableSessions({ world: first.world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      await sessions.send(sessionId, 'go');
      const logs: string[] = [];
      const opts = (world: World): DriverOptions => ({ world, harness, anchors: vercelAnchors(), contendedDelaySeconds: 1, log: (m) => logs.push(m) });
      await Promise.all([
        deliver(opts(first.world), sessionId, { anchor: { k: 'msg', m: 'msg_host1' } }),
        deliver(opts(second.world), sessionId, { anchor: { k: 'msg', m: 'msg_host2' } }),
      ]);
      expect(logs.filter((m) => m === 'in-band fence: a write was refused')).toHaveLength(1);
      expect(calls).toEqual({ model: 1, tool: 1 });
      const state = await sessions.inspect(sessionId);
      expect(new Set(state.effectOrder.map((r) => r.driver)).size).toBe(1);
      expect(state.superseded.size).toBe(0);
    });

    for (const runAhead of [false, true]) it(`${runAhead ? 'run-ahead: ' : ''}a drive refused at the same epoch with an intent of its own open hands it to a continuation, writing nothing (no release, no outcome); the continuation recovers it`, async () => {
      const { world, queued } = await open();
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const calls = { model: 0, tool: 0 };
      const harness = turnHarness({ model: async () => (calls.model === 1 ? (await gate, 'stale plan') : 'plan'), calls });
      const sessions = durableSessions({ world, harness });
      const { sessionId } = await sessions.start({ harness: 'turns' });
      await sessions.send(sessionId, 'go');
      const logs: string[] = [];
      const base: DriverOptions = { world, harness, anchors: vercelAnchors(), hints: false, runAhead, log: (m) => logs.push(m) };
      const driving = deliver(base, sessionId, { anchor: { k: 'msg', m: 'msg_d' } });
      await until(async () => ((await sessions.inspect(sessionId)).effectOrder.some((e) => e.name === 'model' && e.status === 'running') ? true : undefined));
      const stale = (await sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'model')!;
      // Another drive writes in-band at the same epoch (here: a step of its own, opened and closed). The drive can still land an
      // in-band write of its own after the fold shows its step running; a refusal of this stand-in's first write then means it
      // read too early, so it reads again (a fresh log: a refused one stays refused).
      for (let attempt = 1; ; attempt++) {
        const other = new SessionLog(world, sessionId);
        await other.refresh();
        expect(other.arm('drv_other')).toBe(true);
        const step = `step_${ulid()}`;
        try {
          await other.write({ eventType: 'step_started', correlationId: step, eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: 'other', replay: 'safe', input: null, epoch: 0, drv: 'drv_other', anchor: { k: 'msg', m: 'msg_other' } } satisfies StepEnvelope) } });
        } catch (err) {
          if (attempt < 5 && err instanceof LogSuperseded) continue;
          throw err;
        }
        await other.write({ eventType: 'step_completed', correlationId: step, eventData: { stepName: stepNameFor('tool.x'), result: encode('done') } });
        break;
      }
      const before = (await allEvents(world, sessionId)).length;
      release();
      await driving;
      expect(logs).toContain('in-band fence: a write was refused');
      expect(logs).toContain('yield: superseded');
      // The refused drive wrote nothing more: not its outcome, not a release, not a close.
      expect((await allEvents(world, sessionId)).length).toBe(before);
      expect((await sessions.inspect(sessionId)).effects.get(stale.key)).toMatchObject({ stepId: stale.stepId, status: 'running' });
      // Its open intent went to a continuation carrying its anchor.
      const cont = queued.find((q) => (q.payload as { input?: { continues?: string } }).input?.continues === 'msg:msg_d');
      expect(cont).toBeDefined();
      await deliver(base, sessionId, { anchor: { k: 'msg', m: 'msg_cont' }, continues: 'msg:msg_d' });
      const state = await sessions.inspect(sessionId);
      const sub = state.submissions[0]!.id;
      expect(state.effectOrder.find((e) => e.name === 'recover')).toMatchObject({ status: 'completed', input: { continues: 'msg:msg_d', keys: [stale.key] } });
      expect(state.effects.get(`model:${sub}`)).toMatchObject({ status: 'completed', output: 'plan' });
      expect(state.effects.get(`tool:${sub}`)?.status).toBe('completed');
      // The stale result was never recorded.
      expect((await allEvents(world, sessionId)).some((e) => e.correlationId === stale.stepId && e.eventType === 'step_completed')).toBe(false);
      expect(calls).toEqual({ model: 2, tool: 1 });
    });
  });
}

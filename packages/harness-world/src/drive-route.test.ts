import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterEach, describe, expect, it } from 'vitest';
import { createDriveRoute, durableSessions, isAmbiguous, publishHint, QUEUE_PREFIX, SessionLog, vercelAnchors, watchHints, type Hint } from './index.ts';
import { hostProcess, leasedWorld } from './testing/index.ts';
import type { DriveContext, Harness } from './types.ts';

let world: LocalWorld | undefined;
afterEach(async () => {
  await world?.close?.();
  world = undefined;
});

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 10_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 25));
  }
}

/**
 * One `model` effect per submission. With `hold`, a turn waits (reading
 * the inbox only on hints, polling never) until a `steer` shows up, the way
 * Pi and fx pick up steering mid-turn.
 */
function harness(opts: { hold?: boolean; tag: string[] }): Harness {
  return {
    name: 'h',
    async drive(ctx: DriveContext) {
      let n = ctx.state().effectOrder.filter((e) => e.name === 'model').length;
      for (;;) {
        const next = ctx.pending();
        if (next.length === 0) break;
        for (const s of next) {
          await ctx.effect(`model:${n++}`, { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => {
            opts.tag.push(`ran:${s.text}`);
            if (opts.hold && s.kind !== 'steer') {
              await new Promise<void>((resolve) => {
                // Two hints can land together (the steer's own and a
                // companion's): each refreshes, so only the first to see the
                // steer ends the turn.
                let steered = false;
                const off = ctx.onHint?.(async () => {
                  await ctx.refresh();
                  if (!steered && ctx.pending().some((p) => p.kind === 'steer')) {
                    steered = true;
                    off?.();
                    opts.tag.push('steered');
                    resolve();
                  }
                });
              });
            }
            return 'ok';
          });
        }
        await ctx.refresh();
      }
    },
  };
}

async function setup(h: Harness) {
  const dataDir = await mkdtemp(join(tmpdir(), 'harness-drive-'));
  world = createWorld({ dataDir, recoverActiveRuns: false });
  const delivered: string[] = [];
  const sessions = durableSessions({ world, harness: h, contendedDelaySeconds: 1, log: (m) => delivered.push(m) });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  await world.start?.();
  return { sessions, world, delivered };
}

/**
 * The drive route on a World whose queue outlives the process (Vercel): the
 * submit's companion, delayed past the direct drive's deadline, stands
 * behind it. Queue "seconds" are 50 ms here; the drive's deadline is 400 ms.
 */
async function setupVercel(h: Harness) {
  const dataDir = await mkdtemp(join(tmpdir(), 'harness-drive-'));
  world = createWorld({ dataDir, recoverActiveRuns: false });
  const leased = leasedWorld(world, { visibilityMs: 300, secondMs: 50 });
  const delivered: string[] = [];
  const common = { harness: h, anchors: vercelAnchors(), directDriveMs: 400, deadlineMarginMs: 100, contendedDelaySeconds: 1 };
  // Process A takes requests and drives directly; process B serves the queue.
  const proc = hostProcess(leased);
  const a = durableSessions({ world: proc.world, ...common });
  const b = durableSessions({ world: leased, ...common, log: (m) => delivered.push(m) });
  return { a, b, broker: leased.broker, delivered };
}

const companions = (broker: ReturnType<typeof leasedWorld>['broker']) =>
  broker.sent().filter((m) => (m.payload as { input?: { companion?: boolean } }).input?.companion);

describe('drive route: drive in this process, a companion past its deadline as the backstop', () => {
  it('drives a submission directly; the companion lands after the deadline, finds the turn done, and runs nothing twice', async () => {
    const tag: string[] = [];
    const { a, b, broker } = await setupVercel(harness({ tag }));
    const { sessionId } = await b.start({});
    const sentAt = Date.now();
    await a.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    expect(companions(broker)).toHaveLength(1);
    // Delayed by the deadline's whole seconds (1) plus 5: 6 queue seconds of
    // 50 ms, from when it was queued (measured from before send(), not after
    // it returns, which a slow runner can make tens of milliseconds later).
    expect(companions(broker)[0]!.availableAt - sentAt).toBeGreaterThanOrEqual(300);
    const t0 = Date.now();
    await a.drive(sessionId);
    expect(Date.now() - t0).toBeLessThan(400); // did not wait for anything
    expect(tag).toEqual(['ran:hi']);
    await broker.drain({ timeoutMs: 3000 });
    expect(broker.deliveries().length).toBeGreaterThanOrEqual(1);
    expect(tag).toEqual(['ran:hi']);
    const state = await b.inspect(sessionId);
    expect(state.effectOrder.filter((e) => e.name === 'model').map((e) => e.status)).toEqual(['completed']);
  });

  it('a companion that lands while the direct drive is still within its deadline goes back to the queue', async () => {
    const tag: string[] = [];
    const { a, b, broker, delivered } = await setupVercel(harness({ tag, hold: true }));
    const { sessionId } = await b.start({});
    await a.send(sessionId, 'long', 'prompt', { wake: 'backstop' });
    const drive = a.drive(sessionId);
    await until(async () => (tag.includes('ran:long') ? true : undefined), 3000);
    // Deliver it early, as a queue may.
    const companion = companions(broker)[0]!;
    companion.availableAt = 0;
    await broker.deliver(companion.messageId)!.done;
    expect(delivered).toContain('defer: a direct drive is within its deadline');
    expect(companions(broker)).toHaveLength(2);
    // The steer's hint ends the held turn without any poll.
    await a.send(sessionId, 'now', 'steer', { wake: 'backstop' });
    await drive;
    expect(tag).toEqual(['ran:long', 'steered', 'ran:now']);
  });
});

describe('companions stand behind their own turn only', () => {
  it('a companion whose turn is answered stands down when a newer submission (with a companion of its own) waits', async () => {
    const tag: string[] = [];
    const { a, b, broker, delivered } = await setupVercel(harness({ tag }));
    const { sessionId } = await b.start({});
    await a.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    // The next turn's submission is durable, but its direct drive has not started.
    await a.send(sessionId, 'two', 'prompt', { wake: 'backstop' });
    const first = companions(broker)[0]!;
    // A timer can fire a millisecond before Date.now() reaches its target
    // (libuv's cached loop time), and the broker delivers only an available message.
    await new Promise((r) => setTimeout(r, Math.max(0, first.availableAt - Date.now()) + 20));
    await broker.deliver(first.messageId)!.done;
    expect(delivered).toContain("skip: the companion's turn is answered");
    expect(tag).toEqual(['ran:one']);
    await a.drive(sessionId);
    expect(tag).toEqual(['ran:one', 'ran:two']);
  });

  it('a companion whose submission is still pending (its drive never ran) drives it', async () => {
    const tag: string[] = [];
    const { a, b, broker } = await setupVercel(harness({ tag }));
    const { sessionId } = await b.start({});
    await a.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    await a.drive(sessionId);
    // This prompt's direct drive "died" before it started: only its companion comes.
    await a.send(sessionId, 'two', 'prompt', { wake: 'backstop' });
    await broker.drain({ timeoutMs: 4000 });
    expect(tag).toEqual(['ran:one', 'ran:two']);
  });

  it('on world-local no companion is sent: the queue dies with the process, and the next process recovers by pid', async () => {
    const tag: string[] = [];
    const { sessions, world: w } = await setup(harness({ tag }));
    const { sessionId } = await sessions.start({});
    const queued: unknown[] = [];
    const queue = w.queue.bind(w);
    (w as { queue: typeof queue }).queue = async (...args: Parameters<typeof queue>) => (queued.push(args[1]), queue(...args));
    await sessions.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    await sessions.drive(sessionId);
    (w as { queue: typeof queue }).queue = queue;
    expect(queued).toEqual([]);
    expect(tag).toEqual(['ran:hi']);
  });
});

describe('hints', () => {
  it('reach listeners in this process at once and other processes through the stream', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'harness-hints-'));
    world = createWorld({ dataDir, recoverActiveRuns: false });
    const runId = 'wrun_01HINTS0000000000000000000';
    const local: Hint[] = [];
    const abort = new AbortController();
    watchHints(world, runId, (h) => local.push(h), { signal: abort.signal, stream: false });
    await publishHint(world, runId, { eventType: 'hook_received', slot: 3, id: '3' });
    expect(local.map((h) => h.eventType)).toEqual(['hook_received']);

    // A reader that only sees the stream (as another process would).
    const remote: Hint[] = [];
    const other = { ...world, streams: world.streams } as typeof world;
    watchHints(other, runId, (h) => remote.push(h), { signal: abort.signal, startIndex: 0 });
    await until(async () => (remote.length >= 1 ? true : undefined), 5000);
    expect(remote[0]).toMatchObject({ type: 'hint', eventType: 'hook_received', slot: 3 });
    abort.abort();
  });
});

describe('submit returns once durable', () => {
  it('hands the post-commit work to waitUntil instead of waiting for it', async () => {
    const tag: string[] = [];
    const { sessions, world: w } = await setup(harness({ tag }));
    const { sessionId } = await sessions.start({});
    const queue = w.queue.bind(w);
    (w as { queue: typeof queue }).queue = async (...args: Parameters<typeof queue>) => {
      await new Promise((r) => setTimeout(r, 400));
      return queue(...args);
    };
    const background: Promise<unknown>[] = [];
    const t0 = Date.now();
    await sessions.send(sessionId, 'hi', 'prompt', { wake: 'backstop', waitUntil: (p) => background.push(p) });
    expect(Date.now() - t0).toBeLessThan(300);
    expect(background.length).toBe(1);
    await Promise.all(background);
    (w as { queue: typeof queue }).queue = queue;
  });

  it('a drive right after a submit starts from its log, not a fresh listing', async () => {
    const tag: string[] = [];
    const { sessions, world: w } = await setup(harness({ tag }));
    const { sessionId } = await sessions.start({});
    await sessions.send(sessionId, 'hi', 'prompt', { wake: 'backstop' });
    const list = w.events.list.bind(w.events);
    let listedBeforeRun = -1;
    let lists = 0;
    (w.events as { list: typeof list }).list = (...args: Parameters<typeof list>) => {
      lists++;
      return list(...args);
    };
    await sessions.drive(sessionId);
    listedBeforeRun = lists;
    (w.events as { list: typeof list }).list = list;
    expect(tag).toEqual(['ran:hi']);
    // Without the handover the delivery lists the log once more before it
    // starts (3 lists in all); with it only the refreshes after the turn remain.
    expect(listedBeforeRun).toBe(2);
  });
});

describe('resending after an ambiguous failure', () => {
  it('a submission whose write landed but whose answer was lost is counted once', async () => {
    const tag: string[] = [];
    const { sessions, world: w } = await setup(harness({ tag }));
    const { sessionId } = await sessions.start({});
    const create = w.events.create.bind(w.events);
    let failNext = true;
    (w.events as { create: typeof create }).create = async (...args: Parameters<typeof create>) => {
      const result = await create(...args);
      if (failNext && args[1].eventType === 'hook_received') {
        failNext = false;
        throw Object.assign(new Error('harness-world-remote: WebSocket closed (code 1006)'), { ambiguous: true });
      }
      return result;
    };
    const id = await sessions.send(sessionId, 'once', 'prompt', { wake: 'backstop' });
    (w.events as { create: typeof create }).create = create;
    const state = await sessions.inspect(sessionId);
    expect(state.submissions.map((s) => s.text)).toEqual(['once']);
    expect(state.submissions[0]!.id).toBe(id);
  });

  it('a submission whose write never landed is resent, and duplicates of one nonce fold once', async () => {
    const tag: string[] = [];
    const { sessions, world: w } = await setup(harness({ tag }));
    const { sessionId } = await sessions.start({});
    const create = w.events.create.bind(w.events);
    let failNext = true;
    (w.events as { create: typeof create }).create = async (...args: Parameters<typeof create>) => {
      if (failNext && args[1].eventType === 'hook_received') {
        failNext = false;
        throw new TypeError('fetch failed');
      }
      return create(...args);
    };
    await sessions.send(sessionId, 'retry', 'prompt', { wake: 'backstop' });
    (w.events as { create: typeof create }).create = create;
    let state = await sessions.inspect(sessionId);
    expect(state.submissions.map((s) => s.text)).toEqual(['retry']);
    // A second copy of the same nonce (a late landing of the lost write) is ignored.
    const log = new SessionLog(w, sessionId);
    await log.refresh();
    const nonce = state.submissions[0]!.nonce!;
    const { encode } = await import('./codec.ts');
    await log.write({
      eventType: 'hook_received',
      correlationId: log.state.inbox!.hookId,
      eventData: { token: log.state.inbox!.token, payload: encode({ kind: 'prompt', text: 'retry', nonce }) },
    });
    state = await sessions.inspect(sessionId);
    expect(state.submissions.map((s) => s.text)).toEqual(['retry']);
    expect(isAmbiguous(new Error('409 conflict'))).toBe(false);
  });
});

describe('createDriveRoute', () => {
  it('drives after the response, and reports the session as driving here until it lets go', async () => {
    const calls: string[] = [];
    let release!: () => void;
    const route = createDriveRoute({
      after: (work) => void work(),
      drive: async (runId) => {
        calls.push(runId);
        if (calls.length === 1) await new Promise<void>((r) => (release = r));
        return undefined;
      },
    });
    expect(route.drive('wrun_a', { why: 'submit' })).toBe(true);
    await new Promise((r) => setTimeout(r, 0));
    expect(route.driving('wrun_a')).toBe(true);
    // A second request while it drives: the kernel's own registry makes the
    // drive under way run once more (see the next test); here it just runs.
    expect(route.drive('wrun_a', { why: 'steer' })).toBe(false);
    release();
    await new Promise((r) => setTimeout(r, 10));
    expect(calls).toEqual(['wrun_a', 'wrun_a']);
    expect(route.driving('wrun_a')).toBe(false);
  });

  it('a drive that lands while the session drives in this process acks, and the drive under way runs once more', async () => {
    const tag: string[] = [];
    let release!: () => void;
    let held = false;
    const h: Harness = {
      name: 'h',
      async drive(ctx) {
        for (const s of ctx.pending()) {
          await ctx.effect(`model:${s.id}`, { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => {
            tag.push(`ran:${s.text}`);
            if (!held) {
              held = true;
              await new Promise<void>((r) => (release = r));
            }
            return 'ok';
          });
        }
      },
    };
    const { sessions, delivered } = await setup(h);
    const { sessionId } = await sessions.start({});
    await sessions.send(sessionId, 'one', 'prompt', { wake: 'backstop' });
    const first = sessions.drive(sessionId);
    await until(async () => (tag.length ? true : undefined));
    // A second submission, and its drive, land while the first still drives
    // (past its inbox read: it would never see this one).
    await sessions.send(sessionId, 'two', 'prompt', { wake: 'backstop' });
    await sessions.drive(sessionId);
    expect(delivered).toContain('skip: this session is driving in this process; it drives once more');
    release();
    await first;
    expect(delivered).toContain('rerun: a delivery landed while this drove');
    expect(tag).toEqual(['ran:one', 'ran:two']);
  });

  it('seeds the drive with the prepared log, and skips when prepare says so', async () => {
    const seen: unknown[] = [];
    const route = createDriveRoute({
      after: (w) => void w(),
      drive: async (_r, meta) => (seen.push(meta.log), undefined),
    });
    const fake = { runId: 'wrun_d' } as unknown as SessionLog;
    route.drive('wrun_d', { why: 'x', prepare: async () => ({ log: fake }) });
    await new Promise((r) => setTimeout(r, 10));
    expect(seen).toEqual([fake]);
    const skipped = createDriveRoute({ after: (w) => void w(), drive: async () => (seen.push('ran'), undefined) });
    skipped.drive('wrun_e', { why: 'x', prepare: async () => false });
    await new Promise((r) => setTimeout(r, 10));
    expect(seen).toEqual([fake]);
  });
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Event, World } from '@workflow/world';
import { createWorld as createLocalWorld } from '@workflow/world-local';
import { monotonicFactory } from 'ulid';
import { afterEach, describe, expect, it } from 'vitest';
import {
  chatRoute,
  defineDoc,
  deliver,
  type DriveContext,
  type DriverOptions,
  DocConflict,
  DocTooLarge,
  DocVersionError,
  DOC_MAX_BYTES,
  durableSessions,
  EffectFailure,
  type EffectIO,
  type Harness,
  hostToolIO,
  type HostWorkspace,
  liveStreamName,
  rawSessions,
  readDoc,
  readJsonLines,
  type Submission,
  SessionLog,
  vercelAnchors,
} from './index.ts';
import { encode } from './codec.ts';
import { hostProcess, leasedWorld } from './testing/index.ts';

/**
 * Documents in the kernel (docs.ts), on world-local always and on
 * world-postgres when `WORKFLOW_POSTGRES_URL` names a database with the World
 * migrations applied: an edit commits with its effect's outcome (a crash, a
 * throw, a refused stale writer leave none), replays fold to the same state,
 * out-of-band edits race under optimistic concurrency, the size limit and
 * schema versions hold, and the live `doc` event reaches a chat route.
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

type Closable = World & { close?(): Promise<void> };

interface WorldCase {
  name: string;
  /** A World on a fresh store, and a way to open another instance on the same store (another process's). */
  make(): Promise<Closable & { reopen(): Promise<Closable> }>;
}

const WORLDS: WorldCase[] = [
  {
    name: 'world-local',
    make: async () => {
      const dataDir = await mkdtemp(join(tmpdir(), 'hw-docs-'));
      const open = () => createLocalWorld({ dataDir, recoverActiveRuns: false });
      return Object.assign(open(), { reopen: async () => open() });
    },
  },
];
if (process.env.WORKFLOW_POSTGRES_URL) {
  WORLDS.push({
    name: 'world-postgres',
    make: async () => {
      const { createWorld } = await import('@workflow/world-postgres');
      const open = () => createWorld({ connectionString: process.env.WORKFLOW_POSTGRES_URL!, jobPrefix: `hw_docs_${ulid().toLowerCase()}_` }) as unknown as Closable;
      return Object.assign(open(), { reopen: async () => open() });
    },
  });
}

let opened: Array<{ close?(): Promise<void> }> = [];
afterEach(async () => {
  for (const w of opened.splice(0)) await w.close?.().catch(() => {});
});

/** `base` with its queue kept in memory (no queue process in these tests: deliveries are explicit) and its event writes counted. */
function memQueue(base: World) {
  const created: Array<{ runId: string; eventType: string; inBand?: boolean }> = [];
  const events = new Proxy(base.events, {
    get(target, key) {
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      if (key === 'create') {
        return (runId: string, data: { eventType: string }, ...rest: unknown[]) => {
          created.push({ runId, eventType: data.eventType, ...((rest[0] as { inBand?: boolean } | undefined)?.inBand ? { inBand: true } : {}) });
          return (value as (...a: unknown[]) => unknown).call(target, runId, data, ...rest);
        };
      }
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  });
  const world = new Proxy(base, {
    get(target, key) {
      if (key === 'events') return events;
      if (key === 'queue') return async () => ({ messageId: `msg_${ulid()}` });
      const value = (target as unknown as Record<string | symbol, unknown>)[key];
      return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  }) as World;
  return { world, created };
}

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

const liveDocs = async (world: World, runId: string) =>
  ((await readJsonLines(world, runId, liveStreamName(runId))) as Array<{ type?: string }>).filter((e) => e?.type === 'doc') as Array<{ type: 'doc'; kind: string; revision: number; value: unknown; by: unknown }>;

/** One turn per prompt: a `model` effect that places it, then `turn` (tool calls). Answered turns replay from the log. */
function scripted(turn: (ctx: DriveContext, sub: Submission) => Promise<void>, name = 'docs'): Harness {
  return {
    name,
    async drive(ctx) {
      const pending = new Set(ctx.pending().map((p) => p.id));
      for (const sub of ctx.state().submissions) {
        if (sub.kind !== 'prompt') continue;
        if (!pending.has(sub.id) && !ctx.state().placed.has(sub.id)) continue;
        await ctx.effect(`model:${sub.id}`, { name: 'model', replay: 'safe', input: sub.text, placed: [sub.id] }, async () => 'ok');
        await turn(ctx, sub);
      }
      ctx.live({ type: 'idle' });
    },
  };
}

/** A tool call: its result, or the committed failure. */
async function call<T>(ctx: DriveContext, key: string, fn: (io: EffectIO) => Promise<T>, replay: 'safe' | 'never' = 'safe', name = 'tool.edit'): Promise<T | EffectFailure> {
  try {
    return await ctx.effect(key, { name, replay, input: key }, fn);
  } catch (err) {
    if (err instanceof EffectFailure) return err;
    throw err;
  }
}

const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
const Board = defineDoc<{ cards: Record<string, string> }>({ kind: 'app.board', version: 1, initial: () => ({ cards: {} }) });

for (const wc of WORLDS) {
  describe(`documents on ${wc.name}`, () => {
    async function open() {
      const base = await wc.make();
      opened.push(base);
      return { base, ...memQueue(base) };
    }
    const msg = (m: string) => ({ anchor: { k: 'msg' as const, m } });

    it('a tool edit commits with its outcome: the fold, inspect, sessions.doc and the live stream agree', async () => {
      const { world } = await open();
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `add:${sub.id}`, async (io) => {
          const todos = io.docs!.update(Todos, (d) => void d.items.push(sub.text!));
          // The call sees its own update; nothing is committed yet.
          expect(io.docs!.read(Todos)).toEqual({ value: todos, revision: ctx.state().docs.get('app.todos')?.revision ?? 0 });
          return `added; ${todos.items.length}`;
        });
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, 'one');
      await deliver({ world, harness, marks: false }, sessionId, msg('m1'));
      await sessions.send(sessionId, 'two');
      await deliver({ world, harness, marks: false }, sessionId, msg('m2'));
      const state = await sessions.inspect(sessionId);
      const [one, two] = state.submissions;
      expect(state.effects.get(`add:${one!.id}`)).toMatchObject({ status: 'completed', output: 'added; 1', docs: [{ kind: 'app.todos', revision: 1 }] });
      expect(state.effects.get(`add:${two!.id}`)).toMatchObject({ status: 'completed', output: 'added; 2', docs: [{ kind: 'app.todos', revision: 2 }] });
      expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['one', 'two'] }, revision: 2 });
      expect(readDoc(state, Todos).value.items).toEqual(['one', 'two']);
      // Never written: initial at revision 0.
      expect(await sessions.doc(sessionId, Board)).toEqual({ value: { cards: {} }, revision: 0 });
      const live = await liveDocs(world, sessionId);
      expect(live.map((e) => [e.kind, e.revision, e.value])).toEqual([
        ['app.todos', 1, { items: ['one'] }],
        ['app.todos', 2, { items: ['one', 'two'] }],
      ]);
      expect(live[0]!.by).toEqual({ effect: `add:${one!.id}`, step: state.effects.get(`add:${one!.id}`)!.stepId });
    });

    for (const replay of ['never', 'safe'] as const) {
      it(`a crash mid-tool leaves no edit (replay: '${replay}'): ${replay === 'never' ? 'the call is interrupted and the document unchanged' : 'the retry reads the document unchanged and its edit lands once'}`, async () => {
        const { world } = await open();
        const reads: number[] = [];
        let attempts = 0;
        const harness = (slowMs: number) =>
          scripted(async (ctx, sub) => {
            await call(
              ctx,
              `add:${sub.id}`,
              async (io) => {
                attempts++;
                reads.push(io.docs!.read(Todos).revision);
                io.docs!.update(Todos, (d) => void d.items.push(`attempt ${attempts}`));
                await sleep(slowMs);
                return 'added';
              },
              replay
            );
          });
        const sessions = durableSessions({ world, harness: harness(10), marks: false });
        const { sessionId } = await sessions.start();
        await sessions.chaos(sessionId, { at: 'tool', afterMs: 150 });
        await sessions.send(sessionId, 'go');
        // Process A drives, and is killed mid-tool, after its update.
        const a = hostProcess(world);
        let crashed!: () => void;
        const died = new Promise<void>((r) => (crashed = r));
        void deliver({ world: a.world, harness: harness(60_000), marks: false, anchors: vercelAnchors(), chaos: true, crash: () => (a.kill(), crashed()) }, sessionId, msg('m_a'));
        await died;
        expect(attempts).toBe(1);
        expect((await sessions.doc(sessionId, Todos)).revision).toBe(0);
        // Process B: the crash record proves A's drive dead, so it recovers at once.
        await deliver({ world, harness: harness(10), marks: false, anchors: vercelAnchors() }, sessionId, msg('m_b'));
        const state = await sessions.inspect(sessionId);
        const key = `add:${state.submissions.find((s) => s.kind === 'prompt')!.id}`;
        if (replay === 'never') {
          expect(state.effects.get(key)).toMatchObject({ status: 'failed', error: { interrupted: true } });
          expect(attempts).toBe(1);
          expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: [] }, revision: 0 });
          expect(state.docs.size).toBe(0);
        } else {
          expect(state.effects.get(key)).toMatchObject({ status: 'completed', output: 'added', attempt: 2 });
          expect(reads).toEqual([0, 0]);
          expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['attempt 2'] }, revision: 1 });
        }
      });
    }

    it('a tool that throws after its update changes nothing', async () => {
      const { world } = await open();
      const harness = scripted(async (ctx, sub) => {
        const failed = await call(ctx, `bad:${sub.id}`, async (io) => {
          io.docs!.update(Todos, (d) => void d.items.push('never'));
          throw new Error('the tool failed after updating');
        });
        expect(failed).toBeInstanceOf(EffectFailure);
        await call(ctx, `good:${sub.id}`, async (io) => (io.docs!.update(Board, (d) => void (d.cards.a = 'kept')), 'ok'));
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, 'go');
      await deliver({ world, harness, marks: false }, sessionId, msg('m1'));
      const state = await sessions.inspect(sessionId);
      expect(state.effectOrder.find((e) => e.key.startsWith('bad:'))).toMatchObject({ status: 'failed', error: { message: 'the tool failed after updating' } });
      expect(state.docs.has('app.todos')).toBe(false);
      expect((await sessions.doc(sessionId, Board)).value).toEqual({ cards: { a: 'kept' } });
    });

    it('replay gives the same state: a fresh process folds the same documents, and memoized calls do not run their mutators again', async () => {
      const { base, world } = await open();
      let mutations = 0;
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `add:${sub.id}`, async (io) => {
          io.docs!.update(Todos, (d) => {
            mutations++;
            d.items.push(sub.text!);
          });
          io.docs!.update(Board, (d) => void (d.cards[sub.text!] = `card ${sub.text}`));
          return 'ok';
        });
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      for (const text of ['a', 'b', 'c']) {
        await sessions.send(sessionId, text);
        await deliver({ world, harness, marks: false }, sessionId, msg(`m_${text}`));
        // An app's edit between turns.
        await sessions.updateDoc(sessionId, Board, (d) => void (d.cards[`app-${text}`] = 'from the app'));
      }
      expect(mutations).toBe(3);
      const before = await sessions.inspect(sessionId);
      // Another process, another World instance on the same store.
      const other = await base.reopen();
      opened.push(other);
      const log = new SessionLog(other, sessionId);
      await log.refresh();
      expect([...log.state.docs]).toEqual([...before.docs]);
      expect([...log.state.docEdits]).toEqual([...before.docEdits]);
      expect(readDoc(log.state, Todos)).toEqual({ value: { items: ['a', 'b', 'c'] }, revision: 3 });
      expect(readDoc(log.state, Board).revision).toBe(6);
      // A later drive replays every earlier turn from the log: no mutator runs again.
      await sessions.send(sessionId, 'd');
      await deliver({ world: memQueue(other).world, harness, marks: false }, sessionId, msg('m_d'));
      expect(mutations).toBe(4);
      expect((await sessions.doc(sessionId, Todos)).value.items).toEqual(['a', 'b', 'c', 'd']);
    });

    it('concurrent out-of-band updates: without expectRevision every one lands (re-reading after a lost race); with it one wins each revision and the rest throw DocConflict; one docs hook', async () => {
      const { world, created } = await open();
      const harness = scripted(async () => {});
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      // Separate sessions objects: separate logs and caches, as separate instances would have.
      const writers = Array.from({ length: 5 }, () => durableSessions({ world, harness, marks: false }));
      let runs = 0;
      const results = await Promise.all(
        writers.map((w, i) =>
          w.updateDoc(sessionId, Board, (d) => {
            runs++;
            d.cards[`w${i}`] = `writer ${i}`;
          })
        )
      );
      const after = await sessions.doc(sessionId, Board);
      expect(after.revision).toBe(5);
      expect(Object.keys(after.value.cards).sort()).toEqual(['w0', 'w1', 'w2', 'w3', 'w4']);
      expect(results.map((r) => r.revision).sort()).toEqual([1, 2, 3, 4, 5]);
      expect(runs).toBeGreaterThan(5); // some lost a race and ran their mutator again
      let state = await sessions.inspect(sessionId);
      expect(state.docsHook?.token).toBe(`harness:${sessionId}:docs`);
      expect([...state.docEdits.values()].filter((e) => e.applied)).toHaveLength(5);
      // Every first edit raced to create the hook. The first `:docs` hook in log order is the docs hook, and
      // every edit went to it. The losers were answered with a conflict (world-local logs a hook_conflict;
      // world-postgres may refuse the write, or, racing, accept a second hook with the same token, which
      // the fold ignores) and used the winner's.
      const log = await allEvents(world, sessionId);
      const docsHooks = log.filter((e) => e.eventType === 'hook_created' && e.correlationId !== state.inbox?.hookId);
      expect(docsHooks[0]!.correlationId).toBe(state.docsHook!.hookId);
      expect(log.filter((e) => e.eventType === 'hook_received').every((e) => e.correlationId === state.docsHook!.hookId)).toBe(true);
      if (wc.name === 'world-local') {
        expect(docsHooks).toHaveLength(1);
        expect(log.filter((e) => e.eventType === 'hook_conflict').length).toBeGreaterThan(0);
      }

      // With expectRevision: all five expect revision 5; exactly one lands.
      const settled = await Promise.allSettled(writers.map((w, i) => w.updateDoc(sessionId, Board, (d) => void (d.cards.winner = `writer ${i}`), { expectRevision: 5 })));
      expect(settled.filter((s) => s.status === 'fulfilled')).toHaveLength(1);
      for (const s of settled.filter((x) => x.status === 'rejected')) {
        expect((s as PromiseRejectedResult).reason).toBeInstanceOf(DocConflict);
        expect((s as PromiseRejectedResult).reason).toMatchObject({ kind: 'app.board', expected: 5 });
      }
      expect((await sessions.doc(sessionId, Board)).revision).toBe(6);
      // A stale expectRevision is refused before anything is written.
      const writes = created.filter((c) => c.runId === sessionId && c.eventType === 'hook_received').length;
      await expect(sessions.updateDoc(sessionId, Board, (d) => void (d.cards.late = 'x'), { expectRevision: 2 })).rejects.toMatchObject({ name: 'DocConflict', revision: 6, expected: 2 });
      expect(created.filter((c) => c.runId === sessionId && c.eventType === 'hook_received').length).toBe(writes);
      // No change, no write.
      await sessions.updateDoc(sessionId, Board, () => {});
      expect(created.filter((c) => c.runId === sessionId && c.eventType === 'hook_received').length).toBe(writes);
      state = await sessions.inspect(sessionId);
      expect(state.submissions).toHaveLength(0); // an edit is never a submission
      // Every applied edit published its live event.
      expect((await liveDocs(world, sessionId)).map((e) => e.revision).sort((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6]);
    });

    it("out-of-band edits are inputs: never fenced, even through a log a drive armed, and a drive's own writes still are", async () => {
      const { world, created } = await open();
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `t:${sub.id}`, async (io) => (io.docs!.update(Todos, (d) => void d.items.push('tool')), 'ok'));
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      // A log armed by a drive (the drive route hands the submit's log to its drive; a route's updateDoc may share it).
      const log = new SessionLog(world, sessionId);
      await log.refresh();
      expect(log.arm('drv_test')).toBe(true);
      await log.write({ eventType: 'hook_created', correlationId: `hook_${ulid()}`, eventData: { token: `harness:${sessionId}:docs` } });
      await log.write({
        eventType: 'hook_received',
        correlationId: log.state.docsHook!.hookId,
        eventData: { token: log.state.docsHook!.token, payload: encode({ $docs: 1, id: `dedit_${ulid()}`, docs: [{ kind: 'app.todos', base: 0, schema: 1, ops: [{ op: 'set', path: [], value: { items: ['app'] } }] }] }) },
      });
      expect(log.superseded).toBeUndefined();
      await sessions.send(sessionId, 'go');
      await deliver({ world, harness, marks: false }, sessionId, msg('m1'));
      const mine = created.filter((c) => c.runId === sessionId);
      expect(mine.filter((c) => c.eventType.startsWith('hook_')).every((c) => !c.inBand)).toBe(true);
      expect(mine.filter((c) => c.eventType.startsWith('step_')).every((c) => c.inBand)).toBe(true);
      expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['app', 'tool'] }, revision: 2 });
    });

    it('an out-of-band edit whose answer was lost is looked up before it is sent again: it lands once', async () => {
      const { world } = await open();
      let dropped = 0;
      // The World commits the edit, then the answer is lost (a transport failure).
      const lossy = new Proxy(world, {
        get(target, key) {
          if (key !== 'events') return (target as unknown as Record<string | symbol, unknown>)[key];
          return new Proxy(target.events, {
            get(events, k) {
              const value = (events as unknown as Record<string | symbol, unknown>)[k];
              if (k !== 'create') return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(events) : value;
              return async (runId: string, data: { eventType: string }, ...rest: unknown[]) => {
                const result = await (value as (...a: unknown[]) => Promise<unknown>).call(events, runId, data, ...rest);
                if (data.eventType === 'hook_received' && dropped++ === 0) throw Object.assign(new Error('the relay dropped'), { ambiguous: true });
                return result;
              };
            },
          });
        },
      }) as World;
      const sessions = durableSessions({ world, harness: scripted(async () => {}), marks: false });
      const { sessionId } = await sessions.start();
      const logs: string[] = [];
      const writer = durableSessions({ world: lossy, harness: scripted(async () => {}), marks: false, log: (m) => logs.push(m) });
      expect(await writer.updateDoc(sessionId, Todos, (d) => void d.items.push('once'))).toEqual({ value: { items: ['once'] }, revision: 1 });
      expect(logs).toContain('updateDoc: ambiguous write, looking it up');
      const received = (await allEvents(world, sessionId)).filter((e) => e.eventType === 'hook_received');
      expect(received).toHaveLength(1);
      expect((await sessions.inspect(sessionId)).docEdits.size).toBe(1);
    });

    it('an out-of-band edit racing an in-band one: the app edit lands, the tool edit rebases onto it (both keys survive), and the drive does not yield contended for it', async () => {
      const { world } = await open();
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const app = durableSessions({ world, harness: scripted(async () => {}), marks: false });
      const logs: string[] = [];
      let sessionId = '';
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `tool:${sub.id}`, async (io) => {
          io.docs!.update(Board, (d) => void (d.cards.tool = 'from the tool'));
          await gate;
          return 'ok';
        });
        // Between this drive's last read and its next intent: an app's edit (and the docs hook's creation).
        await app.updateDoc(sessionId, Todos, (d) => void d.items.push('between'));
        await call(ctx, `next:${sub.id}`, async () => 'next ran');
      });
      const sessions = durableSessions({ world, harness, marks: false });
      ({ sessionId } = await sessions.start());
      await sessions.send(sessionId, 'go');
      const driving = deliver({ world, harness, marks: false, log: (m) => logs.push(m) }, sessionId, msg('m1'));
      await until(async () => ((await sessions.inspect(sessionId)).effectOrder.some((e) => e.key.startsWith('tool:') && e.status === 'running') ? true : undefined));
      // The tool pinned revision 0; the app writes revision 1 meanwhile.
      await app.updateDoc(sessionId, Board, (d) => void (d.cards.app = 'from the app'));
      release();
      await driving;
      const state = await sessions.inspect(sessionId);
      const tool = state.effectOrder.find((e) => e.key.startsWith('tool:'))!;
      expect(tool.docs).toEqual([{ kind: 'app.board', revision: 2, rebased: true }]);
      expect(await sessions.doc(sessionId, Board)).toEqual({ value: { cards: { app: 'from the app', tool: 'from the tool' } }, revision: 2 });
      expect(state.effectOrder.find((e) => e.key.startsWith('next:'))).toMatchObject({ status: 'completed', output: 'next ran' });
      expect(logs.some((m) => m.includes('contended'))).toBe(false);
      expect((await sessions.doc(sessionId, Todos)).value.items).toEqual(['between']);
    });

    it('parallel sibling calls (an AI SDK-style step) edit different keys of one document: both survive', async () => {
      const { world } = await open();
      let arrived = 0;
      let release!: () => void;
      const both = new Promise<void>((r) => (release = r));
      const harness = scripted(async (ctx, sub) => {
        await Promise.all(
          ['left', 'right'].map((side) =>
            call(ctx, `${side}:${sub.id}`, async (io) => {
              io.docs!.update(Board, (d) => void (d.cards[side] = `${side} card`));
              if (++arrived === 2) release();
              await both;
              return side;
            })
          )
        );
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, 'go');
      await deliver({ world, harness, marks: false }, sessionId, msg('m1'));
      const doc = await sessions.doc(sessionId, Board);
      expect(doc).toEqual({ value: { cards: { left: 'left card', right: 'right card' } }, revision: 2 });
      const records = (await sessions.inspect(sessionId)).effectOrder.filter((e) => e.name === 'tool.edit');
      expect(records.flatMap((r) => r.docs ?? []).filter((d) => d.rebased)).toHaveLength(1);
    });

    it('run-ahead: the next call sees an edit at once, before it is durable', async () => {
      const { world } = await open();
      const seen: number[] = [];
      const harness = scripted(async (ctx, sub) => {
        for (const n of [1, 2, 3]) {
          await call(ctx, `t${n}:${sub.id}`, async (io) => {
            seen.push(io.docs!.read(Todos).revision);
            io.docs!.update(Todos, (d) => void d.items.push(`t${n}`));
            return n;
          });
        }
      });
      const sessions = durableSessions({ world, harness, marks: false, runAhead: true });
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, 'go');
      await deliver({ world, harness, marks: false, runAhead: true }, sessionId, msg('m1'));
      expect(seen).toEqual([0, 1, 2]);
      expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['t1', 't2', 't3'] }, revision: 3 });
    });

    for (const fence of [true, false]) {
      it(
        fence
          ? "a fenced stale writer's edit is refused: a frozen holder's late outcome is refused by the World after its redelivery took over, and only the takeover's edit is in the document"
          : "without the fence the frozen holder's late outcome lands, inert, and its edit is not applied",
        async () => {
          const { world } = await open();
          const leased = leasedWorld(world, { visibilityMs: 200 });
          let releaseFirst!: () => void;
          const firstGate = new Promise<void>((r) => (releaseFirst = r));
          let attempts = 0;
          const harness = scripted(async (ctx, sub) => {
            await call(ctx, `edit:${sub.id}`, async (io) => {
              const mine = ++attempts;
              io.docs!.update(Todos, (d) => void d.items.push(mine === 1 ? 'from the frozen holder' : 'from the takeover'));
              if (mine === 1) await firstGate;
              return `attempt ${mine}`;
            });
          });
          const logs: string[] = [];
          const sessions = durableSessions({ world: leased, harness, anchors: vercelAnchors(), inBandFence: fence, hints: false, marks: false, log: (m) => logs.push(m) });
          const { sessionId } = await sessions.start();
          await sessions.send(sessionId, 'go');
          const first = leased.broker.deliver()!;
          const toolRunning = async () => ((await sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'tool.edit')?.status === 'running' ? true : undefined);
          await until(toolRunning);
          const staleStep = (await sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'tool.edit')!.stepId;
          // The holder freezes (its World calls wait), its tool returns meanwhile, its lease lapses.
          first.freeze();
          releaseFirst();
          const again = await until(() => leased.broker.deliver());
          await again.done;
          expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['from the takeover'] }, revision: 1 });
          // The holder thaws: its outcome, edit and all, goes out now.
          first.thaw();
          await until(async () => (logs.includes(fence ? 'in-band fence: a write was refused' : 'preempted') || (await allEvents(world, sessionId)).some((e) => e.correlationId === staleStep && e.eventType === 'step_completed') ? true : undefined), 5000);
          await sleep(300);
          const late = (await allEvents(world, sessionId)).filter((e) => e.correlationId === staleStep && e.eventType === 'step_completed');
          expect(late).toHaveLength(fence ? 0 : 1);
          const state = await sessions.inspect(sessionId);
          expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['from the takeover'] }, revision: 1 });
          expect(state.effects.get(`edit:${state.submissions[0]!.id}`)).toMatchObject({ status: 'completed', output: 'attempt 2' });
          expect(attempts).toBe(2);
        }
      );
    }

    it('the size limit: a tool update past 64 KB fails the call and commits nothing; updateDoc refuses before writing; exactly 64 KB is allowed', async () => {
      const { world, created } = await open();
      const Notes = defineDoc<{ text: string }>({ kind: 'app.notes', version: 1, initial: () => ({ text: '' }) });
      const fill = (bytes: number) => 'n'.repeat(bytes - JSON.stringify({ text: '' }).length);
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `big:${sub.id}`, async (io) => {
          io.docs!.update(Notes, (d) => void (d.text = fill(DOC_MAX_BYTES + Number(sub.text))));
          return 'wrote';
        });
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, '1');
      await deliver({ world, harness, marks: false }, sessionId, msg('m1'));
      let state = await sessions.inspect(sessionId);
      expect(state.effectOrder.find((e) => e.key.startsWith('big:'))).toMatchObject({ status: 'failed', error: { message: 'harness-world: document app.notes would be 65,537 bytes; the limit is 65,536' } });
      expect(state.docs.has('app.notes')).toBe(false);
      await sessions.send(sessionId, '0');
      await deliver({ world, harness, marks: false }, sessionId, msg('m2'));
      expect((await sessions.doc(sessionId, Notes)).revision).toBe(1);
      const writes = created.filter((c) => c.runId === sessionId && c.eventType.startsWith('hook_')).length;
      await expect(sessions.updateDoc(sessionId, Notes, (d) => void (d.text += '!'))).rejects.toBeInstanceOf(DocTooLarge);
      expect(created.filter((c) => c.runId === sessionId && c.eventType.startsWith('hook_')).length).toBe(writes);
      // Shrinking it is fine.
      state = await sessions.inspect(sessionId);
      await sessions.updateDoc(sessionId, Notes, (d) => void (d.text = d.text.slice(1000)));
      expect((await sessions.doc(sessionId, Notes)).revision).toBe(2);
    });

    it('schema versions: an older stored document migrates on read, a write after migrating stores the new schema, a newer one fails loudly', async () => {
      const { world } = await open();
      const V2 = defineDoc<{ items: { text: string; done: boolean }[] }>({
        kind: 'app.todos',
        version: 2,
        initial: () => ({ items: [] }),
        migrate: (old, from) => {
          if (from !== 1) throw new Error(`no migration from ${from}`);
          return { items: (old.items as string[]).map((text) => ({ text, done: false })) };
        },
      });
      const NoMigrate = defineDoc<{ items: { text: string; done: boolean }[] }>({ kind: 'app.todos', version: 2, initial: () => ({ items: [] }) });
      let def: typeof Todos | typeof V2 = Todos;
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `t:${sub.id}`, async (io) => {
          if (def === Todos) io.docs!.update(Todos, (d) => void d.items.push(sub.text!));
          else io.docs!.update(V2, (d) => void d.items.push({ text: sub.text!, done: true }));
          return 'ok';
        });
      });
      const sessions = durableSessions({ world, harness, marks: false });
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, 'old');
      await deliver({ world, harness, marks: false }, sessionId, msg('m1'));
      // New code reads the v1 document through migrate; the log is unchanged.
      expect(await sessions.doc(sessionId, V2)).toEqual({ value: { items: [{ text: 'old', done: false }] }, revision: 1 });
      expect((await sessions.inspect(sessionId)).docs.get('app.todos')).toMatchObject({ schema: 1 });
      await expect(sessions.doc(sessionId, NoMigrate)).rejects.toBeInstanceOf(DocVersionError);
      // New code writes: a root set at schema 2.
      def = V2;
      await sessions.send(sessionId, 'new');
      await deliver({ world, harness, marks: false }, sessionId, msg('m2'));
      const state = await sessions.inspect(sessionId);
      expect(state.docs.get('app.todos')).toMatchObject({ schema: 2, revision: 2, value: { items: [{ text: 'old', done: false }, { text: 'new', done: true }] } });
      // Old code (a deploy behind) refuses to read it rather than silently downgrade.
      await expect(sessions.doc(sessionId, Todos)).rejects.toThrow(/newer than this code's 1/);
      await expect(sessions.updateDoc(sessionId, Todos, (d) => void d.items.push('v1'))).rejects.toBeInstanceOf(DocVersionError);
      expect(await sessions.doc(sessionId, V2)).toMatchObject({ revision: 2 });
    });

    it('chatRoute({ docs }) starts each response with the current value of each listed document (no id) and forwards their live events; others are dropped, and by default none are sent', async () => {
      const { world } = await open();
      const Secret = defineDoc<{ token: string }>({ kind: 'app.secret', version: 1, initial: () => ({ token: '' }) });
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `t:${sub.id}`, async (io) => {
          io.docs!.update(Todos, (d) => void d.items.push(sub.text!));
          io.docs!.update(Secret, (d) => void (d.token = `secret ${sub.text}`));
          return 'ok';
        });
      });
      const options: DriverOptions = { world, harness, marks: false };
      const sessions = durableSessions(options);
      const send = async (id: string, text: string, kind: 'prompt' | 'steer' | 'followUp') => {
        const submission = await sessions.send(id, text, kind);
        void deliver(options, id, msg(`m_${ulid()}`));
        return submission;
      };
      const read = async (response: Response) => {
        const text = await response.text();
        return text
          .split('\n\n')
          .filter((b) => b.includes('data:'))
          .map((b) => ({ id: /^id: (.*)$/m.exec(b)?.[1], event: /^event: (.*)$/m.exec(b)?.[1], data: JSON.parse(/^data: (.*)$/m.exec(b)![1]!) }));
      };
      const listed = chatRoute(sessions, { send, docs: [Todos] });
      const post = (route: typeof listed, body: unknown) => route.POST(new Request('http://x/api/chat', { method: 'POST', body: JSON.stringify(body) }));
      const first = await post(listed, { text: 'one' });
      const sessionId = first.headers.get('x-session-id')!;
      const one = await read(first);
      const docs = one.filter((m) => m.data.type === 'doc');
      expect(docs.map((m) => [m.data.kind, m.data.revision, m.data.value])).toEqual([['app.todos', 1, { items: ['one'] }]]);
      expect(docs[0]!.id).toBeDefined(); // a live event keeps its cursor
      // The second response opens with the current value, then streams the turn.
      const two = await read(await post(listed, { sessionId, text: 'two' }));
      expect(two[0]).toEqual({ id: undefined, event: undefined, data: { type: 'doc', kind: 'app.todos', revision: 1, schema: 1, value: { items: ['one'] }, by: { effect: expect.stringMatching(/^t:/), step: expect.stringMatching(/^step_/) }, t: expect.any(Number) } });
      expect(two.filter((m) => m.data.type === 'doc').map((m) => m.data.revision)).toEqual([1, 2]);
      expect(two.some((m) => m.data.kind === 'app.secret')).toBe(false);
      // Default: no documents at all.
      const plain = chatRoute(sessions, { send });
      const three = await read(await post(plain, { sessionId, text: 'three' }));
      expect(three.some((m) => m.data.type === 'doc')).toBe(false);
      expect(three.some((m) => m.data.type === 'idle')).toBe(true);
      // The live stream had them all; the routes chose.
      expect((await liveDocs(world, sessionId)).filter((e) => e.kind === 'app.secret')).toHaveLength(3);
    });

    it("a document defined with live: 'revision' is published without its value (a tool's edit, an app's, the chat route's opening values); the log has the value", async () => {
      const { world } = await open();
      const Manifest = defineDoc<{ files: Record<string, string> }>({ kind: 'app.manifest', version: 1, live: 'revision', initial: () => ({ files: {} }) });
      const harness = scripted(async (ctx, sub) => {
        await call(ctx, `t:${sub.id}`, async (io) => {
          io.docs!.update(Manifest, (d) => void (d.files[sub.text!] = 'x'.repeat(1000)));
          io.docs!.update(Todos, (d) => void d.items.push(sub.text!));
          return 'ok';
        });
      });
      const options: DriverOptions = { world, harness, marks: false };
      const sessions = durableSessions(options);
      const { sessionId } = await sessions.start();
      await sessions.send(sessionId, 'a');
      await deliver(options, sessionId, msg('m1'));
      await sessions.updateDoc(sessionId, Manifest, (d) => void (d.files.app = 'y'));
      const live = await liveDocs(world, sessionId);
      expect(live.filter((e) => e.kind === 'app.manifest').map((e) => [e.revision, 'value' in e])).toEqual([
        [1, false],
        [2, false],
      ]);
      expect(live.find((e) => e.kind === 'app.manifest')).toMatchObject({ type: 'doc', kind: 'app.manifest', revision: 1, schema: 1, by: { effect: expect.stringMatching(/^t:/) } });
      // Other documents keep the default: the whole value.
      expect(live.filter((e) => e.kind === 'app.todos').map((e) => e.value)).toEqual([{ items: ['a'] }]);
      expect(await sessions.doc(sessionId, Manifest)).toEqual({ value: { files: { a: 'x'.repeat(1000), app: 'y' } }, revision: 2 });
      // The chat route: the opening value and the turn's events say the revision only.
      const send = async (id: string, text: string, kind: 'prompt' | 'steer' | 'followUp') => {
        const submission = await sessions.send(id, text, kind);
        void deliver(options, id, msg(`m_${ulid()}`));
        return submission;
      };
      const route = chatRoute(sessions, { send, docs: [Manifest, Todos] });
      const response = await route.POST(new Request('http://x/api/chat', { method: 'POST', body: JSON.stringify({ sessionId, text: 'b' }) }));
      const events = (await response.text())
        .split('\n\n')
        .filter((b) => b.includes('data:'))
        .map((b) => JSON.parse(/^data: (.*)$/m.exec(b)![1]!) as { type?: string; kind?: string; revision?: number; value?: unknown });
      const manifest = events.filter((e) => e.type === 'doc' && e.kind === 'app.manifest');
      expect(manifest.map((e) => [e.revision, 'value' in e])).toEqual([
        [2, false],
        [3, false],
      ]);
      expect(events.filter((e) => e.type === 'doc' && e.kind === 'app.todos').map((e) => e.value)).toEqual([{ items: ['a'] }, { items: ['a', 'b'] }]);
    });
  });
}

describe('documents without durability (rawSessions)', () => {
  it('tools edit documents in memory, updateDoc applies, live events go out, and a snapshot round-trips them', async () => {
    const harness = scripted(async (ctx, sub) => {
      await call(ctx, `t:${sub.id}`, async (io) => {
        io.docs!.update(Todos, (d) => void d.items.push(sub.text!));
        return 'ok';
      });
    });
    const raw = rawSessions({ harness, marks: false });
    const { sessionId } = await raw.start();
    const seen: unknown[] = [];
    raw.subscribe(sessionId, (e) => seen.push(e));
    await raw.send(sessionId, 'one');
    await raw.idle(sessionId);
    await raw.updateDoc(sessionId, Todos, (d) => void d.items.push('app'));
    await expect(raw.updateDoc(sessionId, Todos, (d) => void d.items.push('stale'), { expectRevision: 1 })).rejects.toBeInstanceOf(DocConflict);
    expect(await raw.doc(sessionId, Todos)).toEqual({ value: { items: ['one', 'app'] }, revision: 2 });
    expect(seen.filter((e) => (e as { type?: string }).type === 'doc')).toHaveLength(2);
    // A document defined with live: 'revision': its events carry no value, in a tool and from updateDoc.
    const Quiet = defineDoc<{ n: number }>({ kind: 'app.quiet', version: 1, live: 'revision', initial: () => ({ n: 0 }) });
    await raw.updateDoc(sessionId, Quiet, (d) => void (d.n = 1));
    expect(seen.at(-1)).toMatchObject({ type: 'doc', kind: 'app.quiet', revision: 1 });
    expect(seen.at(-1)).not.toHaveProperty('value');
    const snapshot = JSON.parse(JSON.stringify(raw.snapshot(sessionId)));
    raw.close(sessionId);
    const again = raw.restore(snapshot);
    expect(await raw.doc(again, Todos)).toEqual({ value: { items: ['one', 'app'] }, revision: 2 });
    await raw.send(again, 'two');
    await raw.idle(again);
    expect(await raw.doc(again, Todos)).toEqual({ value: { items: ['one', 'app', 'two'] }, revision: 3 });
    expect((await raw.inspect(again)).effectOrder.at(-1)?.docs).toEqual([{ kind: 'app.todos', revision: 3 }]);
  });
});

describe('hostToolIO', () => {
  it("passes every field of the effect's io a tool may use, documents included", () => {
    const docs = { read: () => ({ value: {}, revision: 0 }), update: () => ({}) } as unknown as EffectIO['docs'];
    const signal = new AbortController().signal;
    const io: EffectIO = { signal, progress: () => {}, stepId: 'step_1', park: async () => ({ stepId: 'p', settle: async () => true }), chaosArmedMs: 50, docs };
    const ctx = {} as DriveContext;
    const mine = new AbortController().signal;
    const out = hostToolIO(io, ctx, { signal: mine });
    expect(out).toMatchObject({ signal: mine, ctx, stepId: 'step_1', park: io.park, chaosArmedMs: 50, docs });
    expect(hostToolIO({ ...io, docs: undefined, chaosArmedMs: undefined }, ctx)).not.toHaveProperty('docs');
  });

  it("passes the session's workspace when the harness gives one, and leaves it out otherwise", () => {
    const io: EffectIO = { signal: new AbortController().signal, progress: () => {}, stepId: 'step_1', park: async () => ({ stepId: 'p', settle: async () => true }) };
    const ctx = {} as DriveContext;
    const workspace = { root: '/workspace' } as HostWorkspace;
    expect(hostToolIO(io, ctx, { workspace }).workspace).toBe(workspace);
    expect(hostToolIO(io, ctx)).not.toHaveProperty('workspace');
  });
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { CreateEventRequest } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterEach, describe, expect, it } from 'vitest';
import { effectIdempotencyKey } from './idempotency.ts';
import { durableSessions, QUEUE_PREFIX, rawSessions } from './index.ts';
import { stepNameFor } from './names.ts';
import { SessionLog } from './session-log.ts';
import type { DriveContext, Harness } from './types.ts';

/**
 * `EffectIO.idempotencyKey()`: what lets a tool that creates something be
 * `replay: 'safe'`. A rerun of the call gets the same key (so the system it
 * calls can return what the first attempt made), and a run-ahead drive hands
 * the key out only once the model response that asked for the call is
 * durable (so a crash cannot rerun the model and mint a second call).
 */

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 15_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 25));
  }
}

/** An external service with idempotent creates: one record per key. */
interface Service {
  created: Map<string, number>;
  /** Keys a create was asked with, in order (reruns included). */
  asked: string[];
  /** Was model:0's outcome durable each time the key resolved? */
  durableAtKey: boolean[];
}

/** model:0 (safe) → tool:0 (safe, creates under its idempotency key) → model:1 (safe). */
function agent(service: Service, world: () => LocalWorld): Harness {
  return {
    name: 'agent',
    async drive(ctx: DriveContext) {
      const st = ctx.state();
      const s = st.submissions.find((x) => x.kind !== 'withdraw' && x.kind !== 'chaos' && !st.withdrawn.has(x.id));
      if (!s || ctx.isMemoized('model:1')) {
        if (ctx.pending().length) await ctx.effect(`ack:${ctx.pending()[0]!.id}`, { name: 'model', replay: 'safe', input: 0, placed: ctx.pending().map((p) => p.id) }, async () => 'ok');
        return;
      }
      await ctx.effect('model:0', { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => 'call create');
      await ctx.effect('tool:0', { name: 'tool.create', replay: 'safe', input: s.text }, async (io) => {
        const key = await io.idempotencyKey!();
        const durable = new SessionLog(world(), ctx.runId);
        await durable.refresh();
        service.durableAtKey.push(durable.state.effects.get('model:0')?.status === 'completed');
        service.asked.push(key);
        service.created.set(key, (service.created.get(key) ?? 0) + 1);
        return 'created';
      });
      const rest = ctx.pending().map((p) => p.id);
      await ctx.effect('model:1', { name: 'model', replay: 'safe', input: 'created', placed: rest }, async () => 'done');
    },
  };
}

type Fault = (req: CreateEventRequest, go: () => Promise<unknown>) => Promise<unknown> | undefined;

async function setup(service: Service, opts: { fault?: Fault; dataDir?: string; delayMs?: number; runAhead?: boolean } = {}) {
  const dataDir = opts.dataDir ?? (await mkdtemp(join(tmpdir(), 'hw-idem-')));
  const world = createWorld({ dataDir, recoverActiveRuns: !!opts.dataDir });
  worlds.push(world);
  const create = world.events.create.bind(world.events);
  world.events.create = (async (runId: string, req: any, params?: any) => {
    if (opts.delayMs) await new Promise((r) => setTimeout(r, opts.delayMs));
    const go = () => create(runId, req, params);
    return (opts.fault?.(req, go) ?? go()) as ReturnType<typeof create>;
  }) as typeof world.events.create;
  const sessions = durableSessions({
    world,
    harness: agent(service, () => world),
    runAhead: opts.runAhead ?? true,
    contendedDelaySeconds: 1,
  });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  if (opts.dataDir) await world.start?.();
  return { world, sessions, dataDir };
}

const fresh = (): Service => ({ created: new Map(), asked: [], durableAtKey: [] });
const never = () => new Promise<never>(() => {});
const isStep = (req: CreateEventRequest, type: string, effect: string) =>
  req.eventType === type && (req as { eventData?: { stepName?: string } }).eventData?.stepName === stepNameFor(effect);

describe('effectIdempotencyKey', () => {
  it('is the same for the same call, different for another call or session, and fits as a name', async () => {
    const a = await effectIdempotencyKey('wrun_1', 'tool:call_1');
    expect(a).toBe(await effectIdempotencyKey('wrun_1', 'tool:call_1'));
    expect(a).not.toBe(await effectIdempotencyKey('wrun_1', 'tool:call_2'));
    expect(a).not.toBe(await effectIdempotencyKey('wrun_2', 'tool:call_1'));
    // No separator ambiguity: ("a\nb", "c") is not ("a", "b\nc").
    expect(await effectIdempotencyKey('a\nb', 'c')).not.toBe(await effectIdempotencyKey('a', 'b\nc'));
    expect(a).toMatch(/^dh-[0-9a-f]{64}$/);
  });
});

describe('EffectIO.idempotencyKey', () => {
  it('running ahead, resolves only once the model response that asked for the call is durable', async () => {
    const service = fresh();
    const { sessions } = await setup(service, { delayMs: 60 });
    const { sessionId } = await sessions.start({ prompt: 'remind me' });
    const state = await until(async () => {
      const s = await sessions.inspect(sessionId);
      return s.effects.get('model:1')?.status === 'completed' ? s : undefined;
    });
    expect(service.durableAtKey).toEqual([true]);
    expect(service.asked).toEqual([await effectIdempotencyKey(sessionId, 'tool:0')]);
    expect(state.effects.get('tool:0')?.status).toBe('completed');
  });

  it('a crash after the create, before its outcome lands: the safe tool reruns with the same key, and the create lands once', async () => {
    const service = fresh();
    let dead = false;
    const a = await setup(service, {
      fault: (req) => {
        if (dead) return never();
        if (isStep(req, 'step_completed', 'tool.create')) {
          dead = true; // the create went out; the process dies before its result is in the log
          return never();
        }
        return undefined;
      },
    });
    const { sessionId } = await a.sessions.start({ prompt: 'remind me' });
    await until(async () => (dead ? true : undefined));
    expect(service.asked).toHaveLength(1);
    await a.world.close?.();

    const b = await setup(service, { dataDir: a.dataDir });
    await b.sessions.send(sessionId, 'nudge', 'followUp').catch(() => {});
    const state = await until(async () => {
      const s = await b.sessions.inspect(sessionId);
      return s.effects.get('model:1')?.status === 'completed' ? s : undefined;
    });
    // The call ran twice (it is `safe`), under one key: the service made one record.
    expect(service.asked).toHaveLength(2);
    expect(new Set(service.asked).size).toBe(1);
    expect([...service.created.values()]).toEqual([2]);
    expect(service.created.size).toBe(1);
    expect(state.effects.get('tool:0')?.status).toBe('completed');
    expect(state.effects.get('tool:0')?.attempt).toBe(2);
  });

  it('a strict drive hands out the same key', async () => {
    const service = fresh();
    const { sessions } = await setup(service, { runAhead: false });
    const { sessionId } = await sessions.start({ prompt: 'remind me' });
    await until(async () => ((await sessions.inspect(sessionId)).effects.get('model:1')?.status === 'completed' ? true : undefined));
    expect(service.durableAtKey).toEqual([true]);
    expect(service.asked).toEqual([await effectIdempotencyKey(sessionId, 'tool:0')]);
  });

  it('raw sessions (durability off) hand out the same key', async () => {
    let key: string | undefined;
    const sessions = rawSessions({
      harness: {
        name: 'agent',
        async drive(ctx) {
          if (!ctx.pending().length) return;
          await ctx.effect('tool:0', { name: 'tool.create', replay: 'safe', input: 0, placed: ctx.pending().map((p) => p.id) }, async (io) => {
            key = await io.idempotencyKey!();
            return 'created';
          });
        },
      },
    });
    const { sessionId } = await sessions.start({ prompt: 'remind me' });
    await until(async () => key);
    expect(key).toBe(await effectIdempotencyKey(sessionId, 'tool:0'));
  });
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterEach, describe, expect, it } from 'vitest';
import { encode } from './codec.ts';
import {
  commitClose,
  commitHandoff,
  currentLane,
  type DriveContext,
  type DriverOptions,
  deliver,
  durableSessions,
  EffectFailure,
  type EffectError,
  type HandoffOutput,
  type Harness,
  type Lane,
  type LaneOptions,
  resolveAbandoned,
  SessionLog,
  vercelAnchors,
} from './index.ts';
import { stepNameFor } from './names.ts';
import { hostProcess, leasedWorld } from './testing/index.ts';

/**
 * The queue is the only backstop (anchor.ts): a delivery that finds open
 * work either recovers it at once (its own message came back, a
 * continuation handed it over, the log or the World proves the holder gone)
 * or acks, because the holder takes it if it lives and its message comes
 * back if it died. These are the old lease and backstop scenarios, ported.
 */

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const forever = () => new Promise<never>(() => {});

let world: LocalWorld | undefined;
afterEach(async () => {
  await world?.close?.();
  world = undefined;
});

interface Script {
  model(io: { progress(c: unknown): void; signal: AbortSignal }, ctx: DriveContext): Promise<unknown>;
  tool(io: { progress(c: unknown): void; signal: AbortSignal }, ctx: DriveContext): Promise<unknown>;
  modelCalls: number;
  toolCalls: number;
  failures: EffectError[];
  requires?: Lane;
}

function script(overrides: Partial<Script> = {}): Script {
  return { model: async () => 'plan', tool: async () => 'ran', modelCalls: 0, toolCalls: 0, failures: [], ...overrides };
}

/** Each prompt: a `model` effect (safe), then a `tool.slow` effect (never). Resumes unfinished turns first. */
function scripted(s: Script): Harness {
  async function turn(ctx: DriveContext, id: string, text: string | undefined, placed: string[]) {
    await ctx.effect(`model:${id}`, { name: 'model', replay: 'safe', input: text, placed }, async (io) => {
      s.modelCalls++;
      return s.model(io, ctx);
    });
    try {
      await ctx.effect(`tool:${id}`, { name: 'tool.slow', replay: 'never', input: text, ...(s.requires ? { requires: s.requires } : {}) }, async (io) => {
        s.toolCalls++;
        return s.tool(io, ctx);
      });
    } catch (err) {
      if (!(err instanceof EffectFailure)) throw err;
      s.failures.push(err.error);
    }
  }
  return {
    name: 'scripted',
    async drive(ctx) {
      for (const e of ctx.state().effectOrder.filter((r) => r.name === 'model')) {
        await turn(ctx, e.key.slice('model:'.length), e.input as string, []);
      }
      for (const sub of ctx.pending()) await turn(ctx, sub.id, sub.text, [sub.id]);
    },
  };
}

interface Wake {
  to: Lane;
  handoff: HandoffOutput;
  key?: string;
}

function lanes(serves: Lane[], wakes: Wake[]): LaneOptions {
  return {
    serves,
    settle: 'browser',
    async wake(_runId, to, handoff, opts) {
      wakes.push({ to, handoff, ...(opts?.idempotencyKey ? { key: opts.idempotencyKey } : {}) });
    },
  };
}

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 10_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await sleep(20);
  }
}

async function setup(s: Script) {
  world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-anchor-')), recoverActiveRuns: false });
  const harness = scripted(s);
  const sessions = durableSessions({ world, harness });
  const { sessionId } = await sessions.start({ harness: 'scripted' });
  const inspect = () => sessions.inspect(sessionId);
  const logs: string[] = [];
  const server: DriverOptions = { world, harness, anchors: vercelAnchors(), contendedDelaySeconds: 1, log: (m) => logs.push(m) };
  return { sessions, sessionId, inspect, logs, server, harness };
}

const toolRunning = (state: Awaited<ReturnType<ReturnType<typeof durableSessions>['inspect']>>) =>
  state.effectOrder.find((e) => e.name === 'tool.slow')?.status === 'running' ? true : undefined;

describe('a delivery that finds open work', () => {
  it("recovers a dead driver through its own message's redelivery: at once, fenced, no lease to wait out", async () => {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-anchor-')), recoverActiveRuns: false });
    const leased = leasedWorld(world, { visibilityMs: 200 });
    const s = script({ tool: forever });
    const logs: string[] = [];
    const sessions = durableSessions({ world: leased, harness: scripted(s), anchors: vercelAnchors(), log: (m) => logs.push(m) });
    const { sessionId } = await sessions.start({ harness: 'scripted' });
    await sessions.send(sessionId, 'go');
    const first = leased.broker.deliver()!;
    await until(async () => toolRunning(await sessions.inspect(sessionId)));
    first.kill();
    const killedAt = Date.now();
    s.tool = async () => 'unused';
    const again = await until(async () => leased.broker.deliver());
    expect(again).toMatchObject({ messageId: first.messageId, attempt: 2 });
    await again.done;
    const state = await sessions.inspect(sessionId);
    expect(state.effectOrder.find((e) => e.name === 'tool.slow')?.error?.interrupted).toBe(true);
    expect(state.effectOrder.find((e) => e.name === 'recover')).toMatchObject({ status: 'completed' });
    // One lease (200 ms) and the redelivery: nothing else.
    expect(Date.now() - killedAt).toBeLessThan(1500);
  });

  it('acks, and interrupts nothing, while a live driver elsewhere runs a long tool', async () => {
    let release!: () => void;
    const s = script({ tool: () => new Promise((r) => (release = () => r('ok'))) });
    const { sessions, sessionId, server, inspect, logs } = await setup(s);
    // Process A drives; process B gets every other delivery.
    const a = hostProcess(world!);
    await sessions.send(sessionId, 'go');
    const first = deliver({ ...server, world: a.world }, sessionId, { anchor: { k: 'msg', m: 'msg_a' } });
    await until(async () => toolRunning(await inspect()));
    for (let i = 0; i < 4; i++) {
      expect(await deliver(server, sessionId, { anchor: { k: 'msg', m: `msg_b${i}` } })).toBeUndefined();
    }
    expect(logs.filter((l) => l === 'skip: open work is held elsewhere')).toHaveLength(4);
    release();
    await first;
    expect((await inspect()).effectOrder.find((e) => e.name === 'tool.slow')).toMatchObject({ status: 'completed', output: 'ok' });
  });

  it('resolves what is provably abandoned before it acks for what another holder runs (no fence)', async () => {
    const { sessionId, server, inspect, logs } = await setup(script());
    const log = new SessionLog(world!, sessionId);
    await log.refresh();
    const intent = (key: string, m: string, drv: string) =>
      log.write({
        eventType: 'step_started',
        correlationId: `step_${key.replace(':', '')}0000000000000000000000`,
        eventData: { stepName: stepNameFor('tool.x'), input: encode({ key, replay: 'safe', input: null, epoch: 0, drv, anchor: { k: 'msg', m } }) },
      });
    // Two drives at one epoch: this delivery's own message's dead one, and a live one.
    await intent('a:mine', 'msg_redelivered', 'drv_dead');
    await intent('b:theirs', 'msg_live', 'drv_live');
    expect(await deliver(server, sessionId, { anchor: { k: 'msg', m: 'msg_redelivered' }, attempt: 2 })).toBeUndefined();
    const state = await inspect();
    expect(state.effects.get('a:mine')?.status).toBe('pending');
    expect(state.effects.get('b:theirs')?.status).toBe('running');
    expect(state.effectOrder.some((e) => e.name === 'recover')).toBe(false);
    expect(logs).toContain('skip: open work is held elsewhere');
  });

  it('abandons an intent of a drive that closed: it landed after its drive gave up on the write', async () => {
    const { sessionId, server, inspect } = await setup(script());
    const log = new SessionLog(world!, sessionId);
    await log.refresh();
    await commitClose(log, 'drv_gone');
    // The write the closed drive gave up on lands now, anchored to a message that acked.
    await log.write({
      eventType: 'step_started',
      correlationId: 'step_late00000000000000000000000',
      eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: 'late', replay: 'safe', input: null, epoch: 0, drv: 'drv_gone', anchor: { k: 'msg', m: 'msg_acked' } }) },
    });
    await deliver(server, sessionId, { anchor: { k: 'msg', m: 'msg_other' } });
    const state = await inspect();
    expect(state.effects.get('late')?.status).toBe('pending');
    // Provably abandoned by the log: no fence needed.
    expect(state.effectOrder.some((e) => e.name === 'recover')).toBe(false);
  });

  it('a redelivery fences even when its predecessor left nothing durable', async () => {
    const { sessions, sessionId, server, inspect } = await setup(script());
    await sessions.send(sessionId, 'go');
    await deliver(server, sessionId, { anchor: { k: 'msg', m: 'msg_x' }, attempt: 2 });
    const state = await inspect();
    expect(state.effectOrder.find((e) => e.name === 'recover')).toMatchObject({ input: { reason: 'recover: a redelivery', keys: [] } });
    expect(state.epoch).toBe(1);
    expect(state.effects.get(`tool:${state.submissions[0]!.id}`)?.status).toBe('completed');
  });

  it('a continuation is spent once a recovery took it', async () => {
    const { sessionId, server, inspect } = await setup(script());
    const log = new SessionLog(world!, sessionId);
    await log.refresh();
    await log.write({
      eventType: 'step_started',
      correlationId: 'step_cont0000000000000000000000',
      eventData: { stepName: stepNameFor('tool.x'), input: encode({ key: 'held', replay: 'safe', input: null, epoch: 0, drv: 'drv_x', anchor: { k: 'msg', m: 'msg_x' } }) },
    });
    // The first continuation takes it over.
    await deliver(server, sessionId, { anchor: { k: 'msg', m: 'msg_c1' }, continues: 'msg:msg_x' });
    expect((await inspect()).effects.get('held')?.status).toBe('pending');
    // A copy of it (a World that re-sends a payload) finds the grant spent: a new intent of msg_x is not its.
    const again = new SessionLog(world!, sessionId);
    await again.refresh();
    await again.write({
      eventType: 'step_started',
      correlationId: 'step_cont1000000000000000000000',
      eventData: { stepName: stepNameFor('tool.y'), input: encode({ key: 'held2', replay: 'safe', input: null, epoch: again.state.epoch, drv: 'drv_y', anchor: { k: 'msg', m: 'msg_x' } }) },
    });
    await deliver(server, sessionId, { anchor: { k: 'msg', m: 'msg_c2' }, continues: 'msg:msg_x' });
    expect((await inspect()).effects.get('held2')?.status).toBe('running');
  });

  it('releases its own intent when another live driver wrote in between (no dangling intent)', async () => {
    const { sessions, sessionId, inspect } = await setup(script());
    await sessions.send(sessionId, 'go');
    const other = new SessionLog(world!, sessionId);
    await other.refresh();
    const sneaky: Harness = {
      name: 'scripted',
      async drive(ctx) {
        await other.write({
          eventType: 'step_started',
          correlationId: 'step_foreign',
          eventData: { stepName: 'step//harness-world//tool.x', input: encode({ key: 'x', replay: 'safe', input: null, epoch: 0 }) },
        });
        await ctx.effect('mine', { name: 'tool.mine', replay: 'safe', input: null }, async () => 'no');
      },
    };
    const r = await deliver({ world: world!, harness: sneaky, contendedDelaySeconds: 1 }, sessionId);
    expect(r).toBeUndefined();
    expect((await inspect()).effects.get('mine')?.status).toBe('pending');
  });
});

describe('fencing', () => {
  for (const replay of ['safe', 'never'] as const) {
    it(`never hands the harness a ${replay} result the log rejected in favour of a recovery's`, async () => {
      const { sessions, sessionId, inspect } = await setup(script());
      await sessions.send(sessionId, 'go');
      let calls = 0;
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const got: unknown[] = [];
      const failures: EffectError[] = [];
      const h: Harness = {
        name: 'scripted',
        async drive(ctx) {
          try {
            got.push(
              await ctx.effect('k', { name: 'tool.k', replay, input: null, placed: ctx.pending().map((p) => p.id) }, async () => {
                calls++;
                if (calls === 1) await gate;
                return `call-${calls}`;
              })
            );
          } catch (err) {
            if (!(err instanceof EffectFailure)) throw err;
            failures.push(err.error);
          }
        },
      };
      const driving = deliver({ world: world!, harness: h }, sessionId);
      await until(async () => ((await inspect()).effects.get('k')?.status === 'running' ? true : undefined));
      // A recovery that does not move the epoch (a chaos-proven one) resolves
      // the intent while its driver is still running.
      const other = new SessionLog(world!, sessionId);
      await other.refresh();
      await resolveAbandoned(world!, other, other.state.effects.get('k')!);
      release();
      await driving;
      if (replay === 'safe') {
        expect(got).toEqual(['call-2']);
        expect(calls).toBe(2);
        expect((await inspect()).effects.get('k')).toMatchObject({ status: 'completed', output: 'call-2' });
      } else {
        expect(got).toEqual([]);
        expect(failures).toEqual([expect.objectContaining({ interrupted: true })]);
        expect(calls).toBe(1);
      }
    });
  }

  it('a frozen holder is fenced out when its lease lapses: its redelivery recovers, and the thawed holder stops writing', async () => {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-anchor-')), recoverActiveRuns: false });
    const leased = leasedWorld(world, { visibilityMs: 200 });
    let aborted = false;
    const s = script({
      tool: (io) =>
        new Promise((_, reject) =>
          io.signal.addEventListener('abort', () => {
            aborted = true;
            reject(io.signal.reason);
          })
        ),
    });
    const logs: string[] = [];
    const sessions = durableSessions({ world: leased, harness: scripted(s), anchors: vercelAnchors(), log: (m) => logs.push(m) });
    const { sessionId } = await sessions.start({ harness: 'scripted' });
    await sessions.send(sessionId, 'go');
    const first = leased.broker.deliver()!;
    // The intent is in the log before the tool's body runs: wait for both, so
    // the frozen call is the one listening for its abort, not the replacement.
    await until(async () => (s.toolCalls === 1 ? toolRunning(await sessions.inspect(sessionId)) : undefined));
    first.freeze();
    s.tool = async () => 'ran';
    const again = await until(async () => leased.broker.deliver());
    expect(again.messageId).toBe(first.messageId);
    await again.done;
    const state = await sessions.inspect(sessionId);
    expect(state.effectOrder.find((e) => e.name === 'recover')).toMatchObject({ status: 'completed' });
    expect(state.effectOrder.find((e) => e.name === 'tool.slow')?.error?.interrupted).toBe(true);
    first.thaw();
    await until(async () => (aborted ? true : undefined), 5000);
    expect(first.state).toBe('lost');
  });

  it('does not drive a session whose epoch moved while it recovered', async () => {
    const s = script({ tool: forever });
    const { sessions, sessionId, inspect, logs, harness } = await setup(s);
    const wakes: Wake[] = [];
    const tabProcess = hostProcess(world!);
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    await sessions.chaos(sessionId, { at: 'tool', afterMs: 50 });
    await sessions.send(sessionId, 'go');
    const tab = (extra: Partial<DriverOptions>): DriverOptions => ({ world: world!, harness, lanes: lanes(['browser'], wakes), log: (m) => logs.push(m), ...extra });
    void deliver(tab({ world: tabProcess.world, chaos: true, crash: () => (tabProcess.kill(), crashed()) }), sessionId);
    await died;
    // The reloaded tab is admitted at epoch 0; a takeover lands before it writes its recovery.
    const reloaded = tab({
      admit: async () => {
        const o = new SessionLog(world!, sessionId);
        await o.refresh();
        expect((await commitHandoff(o, { from: 'browser', to: 'function', reason: 'takeover' }, 0)).won).toBe(true);
        return { admit: true };
      },
    });
    expect(await deliver(reloaded, sessionId)).toBeUndefined();
    expect(logs).toContain('skip: the session moved during recovery');
    expect(currentLane(await inspect())).toBe('function');
  });
});

describe('lanes', () => {
  it("an escalation keeps its drive's sibling effect covered in the next lane, then wakes that lane again once it lands", async () => {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-anchor-')), recoverActiveRuns: false });
    let releaseSibling!: () => void;
    const sibling = new Promise<string>((r) => (releaseSibling = () => r('sibling done')));
    const harness: Harness = {
      name: 'parallel',
      async drive(ctx) {
        const p = ctx.pending();
        if (!p.length) return;
        await ctx.effect('model:0', { name: 'model', replay: 'safe', input: null, placed: p.map((x) => x.id) }, async () => 'two tools');
        // Two tool calls at once: one the browser runs, one that needs a function.
        await Promise.all([
          ctx.effect('tool:local', { name: 'tool.local', replay: 'never', input: null }, () => sibling),
          ctx.effect('tool:secret', { name: 'tool.secret', replay: 'safe', input: null, requires: 'function' }, async () => 'secret'),
        ]);
      },
    };
    const sessions = durableSessions({ world, harness });
    const { sessionId } = await sessions.start({ prompt: 'go' });
    const wakes: Wake[] = [];
    const logs: string[] = [];
    const tabProcess = hostProcess(world);
    const browser = deliver({ world: tabProcess.world, harness, lanes: lanes(['browser'], wakes), anchors: vercelAnchors() }, sessionId, { anchor: { k: 'tab', m: 'tab_1' } });
    await until(async () => (wakes.length ? true : undefined));
    expect(wakes[0]).toMatchObject({ to: 'function' });
    // The function lane's first delivery: the sibling still runs in the tab.
    const fn: DriverOptions = { world, harness, lanes: lanes(['function'], wakes), anchors: vercelAnchors(), log: (m) => logs.push(m) };
    await deliver(fn, sessionId, { anchor: { k: 'msg', m: 'msg_fn1' } });
    expect(logs).toContain('skip: open work is held elsewhere');
    let state = await sessions.inspect(sessionId);
    expect(state.effects.get('tool:local')?.status).toBe('running');
    releaseSibling();
    await browser;
    // The escalating drive let go only once its sibling landed, and woke the function lane again.
    state = await sessions.inspect(sessionId);
    expect(state.effects.get('tool:local')).toMatchObject({ status: 'completed', output: 'sibling done' });
    expect(wakes.at(-1)).toMatchObject({ to: 'function', key: expect.stringMatching(/:function:1:settled:drv_/) });
  });

  it('a delivery that finds work in a lane it does not serve (its wake lost with the mover) wakes that lane again, keyed by itself', async () => {
    const s = script({ requires: 'sandbox' });
    const { sessions, sessionId, harness } = await setup(s);
    await sessions.send(sessionId, 'go');
    const log = new SessionLog(world!, sessionId);
    await log.refresh();
    // A mover handed the session to the sandbox lane and died before its wake went out.
    expect((await commitHandoff(log, { from: 'browser', to: 'sandbox', reason: 'machine' }, 0)).won).toBe(true);
    const wakes: Wake[] = [];
    await deliver({ world: world!, harness, lanes: lanes(['function'], wakes), anchors: vercelAnchors() }, sessionId, { anchor: { k: 'msg', m: 'msg_r1' } });
    await deliver({ world: world!, harness, lanes: lanes(['function'], wakes), anchors: vercelAnchors() }, sessionId, { anchor: { k: 'msg', m: 'msg_r2' } });
    expect(wakes.map((w) => [w.to, w.key])).toEqual([
      ['sandbox', `${sessionId}:sandbox:1:redeliver:msg:msg_r1`],
      ['sandbox', `${sessionId}:sandbox:1:redeliver:msg:msg_r2`],
    ]);
  });

  it('a server delivery takes a remote (tab) lane over when a submission waits there and nothing runs', async () => {
    const { sessions, sessionId, harness, inspect } = await setup(script());
    await sessions.send(sessionId, 'from the server');
    const wakes: Wake[] = [];
    await deliver({ world: world!, harness, lanes: { ...lanes(['function'], wakes), remote: ['browser'] }, anchors: vercelAnchors() }, sessionId, { anchor: { k: 'msg', m: 'msg_s' } });
    const state = await inspect();
    expect(state.effectOrder[0]).toMatchObject({ name: 'handoff', input: { from: 'browser', to: 'function' } });
    expect(state.effects.get(`tool:${state.submissions[0]!.id}`)?.status).toBe('completed');
    // Idle: handed back to the tab's lane.
    expect(currentLane(state)).toBe('browser');
  });
});

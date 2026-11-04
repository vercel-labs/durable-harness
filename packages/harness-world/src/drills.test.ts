/**
 * The drills on a session's own sandbox (docs/workspaces.md §8 "Drills"), as
 * the kernel fires them: a target the harness registers for a drive
 * (`ctx.drills`) carries out `sandbox-vm` at a tool and `relay-drop` mid-step,
 * `afterMs` into the step, and the drive goes on; the attach drills
 * (`sandbox-new-vm`, `sandbox-zombie`) are claimed by the attach step itself
 * (`io.drill`). Each fires once: its chaos step places the armed submission.
 * The sandbox tier's side (what each drill does to a VM) is
 * harness-sandbox's `test/drills.test.ts`.
 */
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterEach, describe, expect, it } from 'vitest';
import { armedChaos, type ChaosDriveOptions, type ChaosSpec, chaosMatches, type Drill, type DriveContext, durableSessions, type Harness, isSessionDrill, QUEUE_PREFIX, type SessionState } from './index.ts';

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
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
 * Per prompt: an attach step that claims an attach drill, then a slow tool
 * (or a model step, for a prompt that says `model`). `register` registers a
 * target for the drive's mid-step drills.
 */
function drilled(fired: Drill[], claimed: (Drill | undefined)[], register = true): Harness {
  return {
    name: 'drilled',
    async drive(ctx: DriveContext) {
      if (register) ctx.drills?.({ kinds: ['sandbox-vm', 'relay-drop'], fire: async (d) => void fired.push(d) });
      for (const s of ctx.pending()) {
        await ctx.effect(`sandbox.attach:${s.id}`, { name: 'sandbox.attach', replay: 'never', input: null }, async (io) => {
          claimed.push(await io.drill?.(['sandbox-new-vm', 'sandbox-zombie']));
          return { attached: true };
        });
        const model = s.text?.includes('model');
        await ctx.effect(`${model ? 'model' : 'tool'}:${s.id}`, { name: model ? 'model' : 'tool.slow', replay: 'never', input: s.text, placed: [s.id] }, async () => {
          await new Promise((r) => setTimeout(r, 600));
          return 'done';
        });
      }
    },
  };
}

async function session(harness: Harness, chaos: boolean | ChaosDriveOptions = true) {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-drills-')), recoverActiveRuns: false });
  worlds.push(world);
  const sessions = durableSessions({ world, harness, chaos });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  await world.start?.();
  const { sessionId } = await sessions.start();
  const done = (n: number) =>
    until(async () => {
      const s = await sessions.inspect(sessionId);
      return s.effectOrder.filter((e) => (e.name === 'tool.slow' || e.name === 'model') && e.status === 'completed').length >= n ? s : undefined;
    });
  return { sessions, sessionId, done };
}

const chaosSteps = (state: SessionState) => state.effectOrder.filter((e) => e.name === 'chaos');

describe('drills on the session sandbox', () => {
  it('match their steps: D1 a tool call, D4 a tool or a model stream, D2 and D3 the attach', () => {
    const at = (spec: ChaosSpec) => ['tool.Bash', 'model', 'sandbox.attach', 'tool.sandbox_exec'].filter((n) => chaosMatches(spec, n));
    expect(at({ at: 'tool', kind: 'sandbox-vm' })).toEqual(['tool.Bash', 'tool.sandbox_exec']);
    expect(at({ at: 'sandbox', kind: 'sandbox-vm' })).toEqual(['tool.sandbox_exec']);
    expect(at({ at: 'model', kind: 'relay-drop' })).toEqual(['model']);
    expect(at({ at: 'any', kind: 'relay-drop' })).toEqual(['tool.Bash', 'model', 'tool.sandbox_exec']);
    expect(at({ at: 'sandbox', kind: 'sandbox-new-vm' })).toEqual(['sandbox.attach']);
    expect(at({ at: 'tool', kind: 'sandbox-zombie' })).toEqual(['sandbox.attach']);
    // The parked kinds stay a hybrid sandbox_exec's; D1 at a sandbox_exec is the parked kill.
    expect(isSessionDrill({ at: 'sandbox', kind: 'sandbox-vm' })).toBe(false);
    expect(isSessionDrill({ at: 'tool', kind: 'sandbox-vm' })).toBe(true);
    expect(isSessionDrill({ at: 'sandbox', kind: 'sandbox-command' })).toBe(false);
    expect(isSessionDrill({ at: 'tool', kind: 'process' })).toBe(false);
  });

  it("a process crash at 'sandbox' also lands in a sandbox harness's prepare run; the parked and session kinds don't", () => {
    expect(chaosMatches({ at: 'sandbox' }, 'sandbox.prepare')).toBe(true);
    expect(chaosMatches({ at: 'sandbox', kind: 'process' }, 'sandbox.prepare')).toBe(true);
    expect(chaosMatches({ at: 'sandbox', kind: 'sandbox-command' }, 'sandbox.prepare')).toBe(false);
    expect(chaosMatches({ at: 'sandbox', kind: 'relay-drop' }, 'sandbox.prepare')).toBe(false);
    expect(chaosMatches({ at: 'tool' }, 'sandbox.prepare')).toBe(false);
    expect(chaosMatches({ at: 'any' }, 'sandbox.prepare')).toBe(false);
  });

  it('D1: a stopped VM mid-tool is fired afterMs into the step by the drive\'s target, recorded, and the drive goes on', async () => {
    const fired: Drill[] = [];
    const { sessions, sessionId, done } = await session(drilled(fired, []));
    await sessions.chaos(sessionId, { at: 'tool', kind: 'sandbox-vm', afterMs: 200 });
    await sessions.send(sessionId, 'run it');
    const state = await done(1);
    expect(fired).toEqual([{ kind: 'sandbox-vm', spec: { at: 'tool', kind: 'sandbox-vm', afterMs: 200 }, effect: 'tool.slow', key: expect.stringMatching(/^tool:/) }]);
    const [step] = chaosSteps(state);
    // A v2 step against the tool's step, naming no driver: nothing was killed.
    expect(step?.input).toMatchObject({ v: 2, kind: 'sandbox-vm', effect: 'tool.slow', afterMs: 200 });
    expect((step?.input as { driver?: string }).driver).toBeUndefined();
    expect(armedChaos(state)).toBeUndefined();
    // Fired once: the next turn's tool runs with nothing armed.
    await sessions.send(sessionId, 'again');
    await done(2);
    expect(fired).toHaveLength(1);
  });

  it('D4: a dropped relay fires in a model step', async () => {
    const fired: Drill[] = [];
    const { sessions, sessionId, done } = await session(drilled(fired, []));
    await sessions.chaos(sessionId, { at: 'model', kind: 'relay-drop', afterMs: 150 });
    await sessions.send(sessionId, 'a model call');
    await done(1);
    expect(fired.map((d) => [d.kind, d.effect])).toEqual([['relay-drop', 'model']]);
  });

  it('D2, D3: the attach step claims its drill at once, and the claim is the chaos step', async () => {
    for (const kind of ['sandbox-new-vm', 'sandbox-zombie'] as const) {
      const claimed: (Drill | undefined)[] = [];
      const { sessions, sessionId, done } = await session(drilled([], claimed));
      await sessions.chaos(sessionId, { at: 'sandbox', kind });
      await sessions.send(sessionId, 'first');
      await sessions.send(sessionId, 'second');
      const state = await done(2);
      expect(claimed.map((d) => d?.kind)).toEqual([kind, undefined]);
      expect(chaosSteps(state).map((e) => e.input)).toEqual([expect.objectContaining({ v: 2, kind, effect: 'sandbox.attach', afterMs: 0 })]);
    }
  });

  it('with no target for its kind, or chaos off for the driver, a drill stays armed; a drill-only driver fires one armed before it began', async () => {
    const none: Drill[] = [];
    const a = await session(drilled(none, [], false));
    await a.sessions.chaos(a.sessionId, { at: 'tool', kind: 'sandbox-vm', afterMs: 100 });
    await a.sessions.send(a.sessionId, 'run it');
    expect(armedChaos(await a.done(1))).toBeDefined();

    const off: (Drill | undefined)[] = [];
    const b = await session(drilled(none, off), { fire: false });
    await b.sessions.chaos(b.sessionId, { at: 'sandbox', kind: 'sandbox-new-vm' });
    await b.sessions.send(b.sessionId, 'run it');
    expect(armedChaos(await b.done(1))).toBeDefined();
    expect(off).toEqual([undefined]);
    expect(none).toEqual([]);

    // `'armed-in-drive'` keeps a Kill Function armed before the drive for a drill route; a drill kills no process, so it fires here.
    const fired: Drill[] = [];
    const c = await session(drilled(fired, []), { fire: 'armed-in-drive' });
    await c.sessions.chaos(c.sessionId, { at: 'tool', kind: 'sandbox-vm', afterMs: 100 });
    await c.sessions.send(c.sessionId, 'run it');
    await c.done(1);
    expect(fired).toHaveLength(1);
  });
});

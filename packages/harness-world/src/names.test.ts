import { readFileSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { ulid } from 'ulid';
import { afterEach, describe, expect, it } from 'vitest';
import { durableSessions, encode, QUEUE_PREFIX, SessionLog } from './index.ts';
import { harnessOf, harnessOfQueue, legacyWorkflowNameFor, MODULE, sameHarness, stepNameFor, VERSION, workflowNameFor } from './names.ts';
import type { DriveContext, Harness } from './types.ts';

const { version } = JSON.parse(readFileSync(new URL('../../durable-harness/package.json', import.meta.url), 'utf8')) as { version: string };

describe('run names', () => {
  it("are the Workflow SDK's form for a package's workflow: workflow//durable-harness@<its package.json version>//<harness>", () => {
    expect(VERSION).toBe(version);
    expect(MODULE).toBe(`durable-harness@${version}`);
    expect(workflowNameFor('pi')).toBe(`workflow//durable-harness@${version}//pi`);
    expect(workflowNameFor('claude-code')).toMatch(/^workflow\/\/durable-harness@\d+\.\d+\.\d+[^/]*\/\/claude-code$/);
  });

  it('keep step names on the kernel module', () => {
    expect(stepNameFor('model')).toBe('step//harness-world//model');
  });

  it('read the harness from the new form at any version, the older harness-world form, and the bare prototype form', () => {
    expect(harnessOf(workflowNameFor('pi'))).toBe('pi');
    expect(harnessOf('workflow//durable-harness@0.0.1//codex')).toBe('codex');
    expect(harnessOf('workflow//durable-harness@1.2.3-beta.4//pi-durable')).toBe('pi-durable');
    expect(harnessOf('workflow//durable-harness//opencode')).toBe('opencode');
    expect(harnessOf(legacyWorkflowNameFor('pi'))).toBe('pi');
    expect(legacyWorkflowNameFor('pi')).toBe('workflow//harness-world//harness:pi');
    expect(harnessOf('harness:echo')).toBe('echo');
    // A harness whose own name starts like the older form is still itself.
    expect(harnessOf(workflowNameFor('harness:x'))).toBe('harness:x');
  });

  it('say nothing of runs that are not harness sessions', () => {
    for (const name of [undefined, '', 'libfx/session', 'workflow//./src/jobs/order//processOrder', 'workflow//eve@0.76.0//runAgentRouterTask', 'workflow//durable-harness@0.0.1//', 'workflow//durable-harness-other@1.0.0//pi', 'step//durable-harness@0.0.1//pi']) {
      expect(harnessOf(name)).toBeUndefined();
    }
  });

  it('match one harness across versions and forms, and only it', () => {
    expect(sameHarness('workflow//durable-harness@0.0.1//pi', workflowNameFor('pi'))).toBe(true);
    expect(sameHarness(legacyWorkflowNameFor('pi'), workflowNameFor('pi'))).toBe(true);
    expect(sameHarness(workflowNameFor('pi'), workflowNameFor('pi-durable'))).toBe(false);
    expect(sameHarness('workflow//./src/a//pi', 'workflow//./src/a//pi')).toBe(false);
    expect(sameHarness(undefined, undefined)).toBe(false);
  });

  it("read a World's recovery delivery (the run's workflow's queue), in either form, and no other queue", () => {
    expect(harnessOfQueue(`__wkf_workflow_${workflowNameFor('pi-durable')}`)).toBe('pi-durable');
    expect(harnessOfQueue('__wkf_workflow_workflow//durable-harness@0.0.1//pi')).toBe('pi');
    expect(harnessOfQueue(`__wkf_workflow_${legacyWorkflowNameFor('pi-durable')}`)).toBe('pi-durable');
    expect(harnessOfQueue(`__team_wkf_workflow_${workflowNameFor('codex')}`)).toBe('codex');
    for (const queue of ['__wkf_workflow_harness', '__wkf_workflow_harness_owner', '__wkf_workflow_workflow//./src/x//pi', `__libfx_wkf_workflow_session`, 'pi']) {
      expect(harnessOfQueue(queue)).toBeUndefined();
    }
  });
});

/** Each message: one `model` effect that answers its text. */
const answer: Harness = {
  name: 'answer',
  async drive(ctx: DriveContext) {
    let turn = ctx.state().effectOrder.length;
    for (const s of ctx.pending()) {
      await ctx.effect(`model:${turn++}`, { name: 'model', replay: 'safe', input: s.text, placed: [s.id] }, async () => `re: ${s.text}`);
    }
  },
};

let world: LocalWorld | undefined;
afterEach(async () => {
  await world?.close?.();
  world = undefined;
});

/** A session's run as `sessions.start` writes it, under `workflowName`: one an earlier version started. */
async function startNamed(w: LocalWorld, workflowName: string, harness: string): Promise<string> {
  const runId = `wrun_${ulid()}`;
  await w.events.create(runId, {
    eventType: 'run_created',
    specVersion: w.specVersion,
    eventData: { deploymentId: await w.getDeploymentId(), workflowName, input: encode({ harness }) },
  } as never);
  const log = new SessionLog(w, runId);
  await log.refresh();
  await log.write({ eventType: 'run_started' });
  await log.write({ eventType: 'hook_created', correlationId: `hook_${ulid()}`, eventData: { token: `harness:${runId}:inbox`, isWebhook: false } } as never);
  return runId;
}

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 10_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe('run ids', () => {
  it("are the World's own when it mints them (world-vercel's carry the region), a plain ULID otherwise", async () => {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'harness-world-names-')), recoverActiveRuns: false });
    const sessions = durableSessions({ world, harness: answer });
    const plain = (await sessions.start()).sessionId;
    expect(plain).toMatch(/^wrun_[0-9A-HJKMNP-TV-Z]{26}$/);
    const minted = ulid();
    const asked: unknown[] = [];
    Object.assign(world, { createRunId: (options?: unknown) => (asked.push(options), minted) });
    const { sessionId } = await sessions.start();
    expect(sessionId).toBe(`wrun_${minted}`);
    expect(asked).toEqual([{}]);
  });
});

describe('sessions under an older name', () => {
  it('start under the new name, and drive sessions an earlier version started (the harness-world form, another version) alike', async () => {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'harness-world-names-')), recoverActiveRuns: false });
    const sessions = durableSessions({ world, harness: answer });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    await world.start?.();

    const fresh = (await sessions.start()).sessionId;
    expect((await sessions.inspect(fresh)).workflowName).toBe(workflowNameFor('answer'));
    const legacy = await startNamed(world, legacyWorkflowNameFor('answer'), 'answer');
    const older = await startNamed(world, 'workflow//durable-harness@0.0.0-older//answer', 'answer');
    for (const id of [fresh, legacy, older]) await sessions.send(id, `hi ${id}`);
    for (const id of [fresh, legacy, older]) {
      const state = await until(async () => {
        const s = await sessions.inspect(id);
        return s.effectOrder.some((e) => e.status === 'completed') ? s : undefined;
      });
      expect(state.effectOrder.map((e) => e.output)).toEqual([`re: hi ${id}`]);
    }
    // The name a run was started under stays its name.
    expect((await sessions.inspect(legacy)).workflowName).toBe('workflow//harness-world//harness:answer');
  });

  it('leave alone a run of another workflow', async () => {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'harness-world-names-')), recoverActiveRuns: false });
    const skipped: string[] = [];
    const sessions = durableSessions({ world, harness: answer, log: (message) => void (message.startsWith('skip: no harness') && skipped.push(message)) });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    await world.start?.();
    const other = await startNamed(world, 'workflow//./src/other//answer', 'answer');
    await sessions.send(other, 'hi');
    await until(async () => (skipped.length > 0 ? true : undefined));
    expect((await sessions.inspect(other)).effectOrder).toEqual([]);
  });
});

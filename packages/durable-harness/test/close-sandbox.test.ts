import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import type { CleanupApi, VmHandle, VmStatus } from 'harness-world-sandbox';
import { sessionName } from 'harness-world-sandbox/names';
import type { DriveContext, Harness } from 'harness-world';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { durableAgent } from '../src/agent.ts';
import { claudeCodeHarness } from '../src/claude-code.ts';
import { markSandboxHarness, sandboxHarnessMark, sandboxMarkOf } from '../src/sandbox-harness.ts';
import { sandbox, type SandboxOptions } from '../src/workspace.ts';

/**
 * `agent.sessions.close` and the session's sandbox (`sandbox({ onClose })`,
 * `close(id, { sandbox })`): the Vercel Sandbox API is a stub here
 * (`vercelSandboxApi`, replaced) that records every call and fails on any
 * that would boot a VM, on world-local.
 */

const platform = vi.hoisted(() => ({
  vms: {} as Record<string, { status: string }>,
  drives: new Set<string>(),
  calls: [] as string[],
  /** Thrown by every call while set: the Sandbox API refusing. */
  down: undefined as Error | undefined,
  /** The credentials the close reached the API with. */
  credentials: [] as unknown[],
}));

vi.mock('harness-world-sandbox', async (importOriginal) => {
  const real = await importOriginal<typeof import('harness-world-sandbox')>();
  const boots = (what: string) => async () => {
    platform.calls.push(`BOOT:${what}`);
    throw new Error(`close called ${what}, which boots a stopped VM`);
  };
  const api: CleanupApi = {
    async getSandbox(name) {
      platform.calls.push(`get:${name}`);
      if (platform.down) throw platform.down;
      const vm = platform.vms[name];
      if (!vm) return undefined;
      return {
        name,
        sessionId: `sbx_${name}`,
        status: vm.status as VmStatus,
        region: 'iad1',
        startedAt: 1,
        timeout: 300_000,
        domain: () => {
          throw new Error('no ports');
        },
        run: boots('run'),
        extendTimeout: boots('extendTimeout'),
        setNetwork: boots('setNetwork'),
        async stop() {
          platform.calls.push(`stop:${name}`);
          vm.status = 'stopped';
        },
        async delete() {
          platform.calls.push(`delete:${name}`);
          delete platform.vms[name];
        },
      } satisfies VmHandle;
    },
    async deleteDrive(name) {
      platform.calls.push(`deleteDrive:${name}`);
      return platform.drives.delete(name);
    },
    async *listDrives() {},
  };
  return {
    ...real,
    vercelSandboxApi: (credentials?: unknown) => {
      platform.credentials.push(credentials);
      return api;
    },
  };
});

const PROJECT = 'prj_close';
const credentials = { token: 'tok_close_test', teamId: 'team_close', projectId: PROJECT };

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
  platform.vms = {};
  platform.drives.clear();
  platform.calls.length = 0;
  platform.credentials.length = 0;
  platform.down = undefined;
  vi.restoreAllMocks();
});

/** A harness that answers nothing: these tests only start and close sessions. */
const idle = (name: string): Harness => ({ name, async drive(_ctx: DriveContext) {} });

/** An agent whose harness is marked as giving each session a sandbox (as `claudeCode()` and the others mark theirs), on its own world-local. */
async function agentWith(options: { workspace?: SandboxOptions; marked?: boolean; log?: (message: string, fields?: Record<string, unknown>) => void } = {}) {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-close-')), recoverActiveRuns: false });
  worlds.push(world);
  const harness = idle(`close-${Math.random().toString(36).slice(2, 8)}`);
  const marked = options.marked === false ? harness : markSandboxHarness(harness, sandboxMarkOf(sandbox({ credentials, ...options.workspace })));
  return durableAgent({ world, harness: marked, ...(options.log ? { log: options.log } : {}) });
}

/** A session whose VM is running and whose Drive exists. */
async function running(agent: Awaited<ReturnType<typeof agentWith>>) {
  const { sessionId } = await agent.sessions.start();
  const name = sessionName(PROJECT, sessionId);
  platform.vms[name] = { status: 'running' };
  platform.drives.add(name);
  return { sessionId, name };
}

describe('sessions.close and the session sandbox', () => {
  it('stops the VM by default, once, and keeps the sandbox and the Drive; nothing boots', async () => {
    const agent = await agentWith();
    const { sessionId, name } = await running(agent);
    await agent.sessions.close(sessionId);
    expect((await agent.sessions.inspect(sessionId)).status).toBe('completed');
    expect(platform.calls).toEqual([`get:${name}`, `stop:${name}`]);
    expect(platform.vms[name]).toEqual({ status: 'stopped' });
    expect(platform.drives.has(name)).toBe(true);
    // With the workspace's credentials.
    expect(platform.credentials).toEqual([credentials]);
  });

  it("a close tried again does the sandbox's part again harmlessly (nothing left to stop), and throws as before", async () => {
    const agent = await agentWith();
    const { sessionId, name } = await running(agent);
    await agent.sessions.close(sessionId);
    await expect(agent.sessions.close(sessionId)).rejects.toThrow(/terminal/i);
    expect(platform.calls.filter((c) => c.startsWith('stop:'))).toEqual([`stop:${name}`]);
    expect(platform.calls).toEqual([`get:${name}`, `stop:${name}`, `get:${name}`]);
  });

  it('a VM left running by a close that died after run_completed is stopped by the close tried again', async () => {
    const agent = await agentWith({ workspace: { onClose: 'keep' } });
    const { sessionId, name } = await running(agent);
    // As if the process died between the log's close and the sandbox's.
    await agent.sessions.close(sessionId);
    expect(platform.calls).toEqual([]);
    await expect(agent.sessions.close(sessionId, { sandbox: 'stop' })).rejects.toThrow(/terminal/i);
    expect(platform.calls).toEqual([`get:${name}`, `stop:${name}`]);
  });

  it("onClose: 'delete' (or close(id, { sandbox: 'delete' })) deletes the VM and the Drive", async () => {
    const byOption = await agentWith({ workspace: { onClose: 'delete' } });
    const a = await running(byOption);
    await byOption.sessions.close(a.sessionId);
    expect(platform.calls).toEqual([`get:${a.name}`, `stop:${a.name}`, `delete:${a.name}`, `deleteDrive:${a.name}`]);
    expect(platform.vms[a.name]).toBeUndefined();
    expect(platform.drives.has(a.name)).toBe(false);

    platform.calls.length = 0;
    const byCall = await agentWith();
    const b = await running(byCall);
    await byCall.sessions.close(b.sessionId, { sandbox: 'delete' });
    expect(platform.calls).toEqual([`get:${b.name}`, `stop:${b.name}`, `delete:${b.name}`, `deleteDrive:${b.name}`]);
    expect(platform.drives.has(b.name)).toBe(false);
  });

  it("keep (the workspace's or the call's) leaves the sandbox alone", async () => {
    const kept = await agentWith({ workspace: { onClose: 'keep' } });
    const a = await running(kept);
    await kept.sessions.close(a.sessionId);
    const byCall = await agentWith();
    const b = await running(byCall);
    await byCall.sessions.close(b.sessionId, { sandbox: 'keep' });
    expect(platform.calls).toEqual([]);
    expect(platform.vms[a.name]).toEqual({ status: 'running' });
    expect(platform.vms[b.name]).toEqual({ status: 'running' });
    expect(platform.drives.has(a.name) && platform.drives.has(b.name)).toBe(true);
  });

  it('a Sandbox API failure does not fail the close: it is logged, and the session is closed', async () => {
    const lines: { message: string; fields?: Record<string, unknown> }[] = [];
    const agent = await agentWith({ log: (message, fields) => lines.push({ message, ...(fields ? { fields } : {}) }) });
    const { sessionId } = await running(agent);
    platform.down = Object.assign(new Error('Status code 403 is not ok'), { response: { status: 403 } });
    await agent.sessions.close(sessionId);
    expect((await agent.sessions.inspect(sessionId)).status).toBe('completed');
    const failed = lines.filter((l) => /left its sandbox as it was/.test(l.message));
    expect(failed).toHaveLength(1);
    expect(failed[0]!.fields).toMatchObject({ runId: sessionId, error: expect.stringContaining('403') });

    // Without a log: console.warn, and never the token.
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const quiet = await agentWith();
    const other = await running(quiet);
    await quiet.sessions.close(other.sessionId);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(warn.mock.calls)).not.toContain(credentials.token);
  });

  it('a harness whose sessions have no sandbox: close touches no Sandbox API, whatever it is asked', async () => {
    const agent = await agentWith({ marked: false });
    const { sessionId } = await agent.sessions.start();
    await agent.sessions.close(sessionId, { sandbox: 'delete' });
    expect((await agent.sessions.inspect(sessionId)).status).toBe('completed');
    expect(platform.calls).toEqual([]);
    expect(platform.credentials).toEqual([]);
  });

  it('an unknown sandbox value is refused before the session is closed', async () => {
    const agent = await agentWith();
    const { sessionId } = await agent.sessions.start();
    await expect(agent.sessions.close(sessionId, { sandbox: 'destroy' as never })).rejects.toThrow(TypeError);
    expect((await agent.sessions.inspect(sessionId)).status).toBe('running');
    expect(() => sandbox({ onClose: 'nope' as never })).toThrow(/onClose/);
  });
});

describe("sandbox({ onClose }) on the factories' marks", () => {
  it("defaults to stop, carries the workspace's onClose and credentials, and never prints the token", () => {
    expect(sandbox().onClose).toBe('stop');
    expect(sandbox(sandbox({ onClose: 'delete' })).onClose).toBe('delete');
    const harness = claudeCodeHarness({ model: 'anthropic/claude-haiku-4.5', workspace: sandbox({ onClose: 'delete', credentials }) });
    const mark = sandboxHarnessMark(harness)!;
    expect(mark.onClose).toBe('delete');
    expect(mark.credentials?.projectId).toBe(PROJECT);
    // A spread of the harness keeps the mark; the token is on no enumerable property.
    expect(sandboxHarnessMark({ ...harness })?.credentials).toBe(mark.credentials);
    expect(JSON.stringify(mark)).not.toContain(credentials.token);
    expect(sandboxHarnessMark(claudeCodeHarness({ model: 'anthropic/claude-haiku-4.5' }))?.onClose).toBe('stop');
  });
});

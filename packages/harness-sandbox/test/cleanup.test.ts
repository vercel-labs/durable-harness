/**
 * Deleting a session's sandbox and Drive (`cleanup.ts`, docs/workspaces.md
 * §5.7 "Cleanup", §11.4 A7) against a stub Sandbox API that fails on every
 * call that would boot a VM (`run`, `extendTimeout`, `setNetwork`, a
 * create), so passing proves neither path ever boots one.
 */
import { describe, expect, it } from 'vitest';
import { ApiConflict, type CleanupApi, type DriveInfo, type VmHandle, type VmStatus } from '../src/api.ts';
import { cleanupIdle, deleteSession, rateLimiter, SessionInUse, stopSession } from '../src/cleanup.ts';
import { sessionName } from '../src/names.ts';

const DAY = 86_400_000;
const NOW = 100 * DAY;

interface StubVm {
  status: VmStatus;
  startedAt: number;
}

/** The platform as the cleanup sees it: VMs by name, Drives by name, a log of the calls, and how many more times a Drive refuses its delete as attached. */
function platform(vms: Record<string, StubVm>, drives: Record<string, Partial<DriveInfo>>, options: { attachedFor?: number; driveError?: Error } = {}) {
  const calls: string[] = [];
  let attached = options.attachedFor ?? 0;
  const boots = (what: string) => async () => {
    calls.push(`BOOT:${what}`);
    throw new Error(`the cleanup called ${what}, which boots a stopped VM`);
  };
  const handle = (name: string, vm: StubVm): VmHandle => ({
    name,
    sessionId: `sbx_${name}`,
    status: vm.status,
    region: 'iad1',
    startedAt: vm.startedAt,
    timeout: 300_000,
    domain: () => {
      throw new Error('no ports');
    },
    run: boots('run'),
    extendTimeout: boots('extendTimeout'),
    setNetwork: boots('setNetwork'),
    async stop() {
      calls.push(`stop:${name}`);
      vm.status = 'stopped';
    },
    async delete() {
      calls.push(`delete:${name}`);
      delete vms[name];
    },
  });
  const api: CleanupApi & { createSandbox(): Promise<never> } = {
    async getSandbox(name) {
      calls.push(`get:${name}`);
      const vm = vms[name];
      return vm ? handle(name, vm) : undefined;
    },
    async deleteDrive(name) {
      calls.push(`deleteDrive:${name}`);
      if (options.driveError) throw options.driveError;
      if (!drives[name]) return false;
      if (attached > 0) {
        attached--;
        throw new ApiConflict('drive_attached', 'the Drive is still attached');
      }
      delete drives[name];
      return true;
    },
    async *listDrives(prefix) {
      calls.push(`list:${prefix}`);
      for (const [name, d] of Object.entries(drives).sort()) if (name.startsWith(prefix)) yield { name, driveId: `drv_${name}`, region: 'iad1', maxSize: 1 << 30, ...d } as DriveInfo;
    },
    createSandbox: boots('createSandbox'),
  };
  return { api, calls, vms, drives };
}

const name = (s: string) => sessionName('prj_1', s);
const instant = { limit: async () => {}, sleep: async () => {} };

describe('deleteSession: a session\'s VM and Drive, never booting a VM', () => {
  it('a running VM is stopped, then deleted, then its Drive, retried while still attached', async () => {
    const n = name('wrun_1');
    const p = platform({ [n]: { status: 'running', startedAt: NOW } }, { [n]: {} }, { attachedFor: 2 });
    const slept: number[] = [];
    expect(await deleteSession(n, { api: p.api, limit: async () => {}, sleep: async (ms) => void slept.push(ms) })).toEqual({ name: n, sandbox: 'deleted', drive: 'deleted' });
    expect(p.calls).toEqual([`get:${n}`, `stop:${n}`, `delete:${n}`, `deleteDrive:${n}`, `deleteDrive:${n}`, `deleteDrive:${n}`]);
    expect(slept).toEqual([1000, 2000]);
    expect(p.calls.some((c) => c.startsWith('BOOT'))).toBe(false);
  });

  it('a stopped VM is deleted without a stop; nothing there is fine', async () => {
    const n = name('wrun_2');
    const p = platform({ [n]: { status: 'stopped', startedAt: 0 } }, {});
    expect(await deleteSession(n, { api: p.api, ...instant })).toEqual({ name: n, sandbox: 'deleted', drive: 'none' });
    expect(p.calls).toEqual([`get:${n}`, `delete:${n}`, `deleteDrive:${n}`]);
    expect(await deleteSession(n, { api: p.api, ...instant })).toEqual({ name: n, sandbox: 'none', drive: 'none' });
  });

  it('a Drive still attached past detachMs is an error; another refusal is thrown at once; a name that is not a session\'s is refused', async () => {
    const n = name('wrun_3');
    const stuck = platform({}, { [n]: {} }, { attachedFor: 1000 });
    await expect(deleteSession(n, { api: stuck.api, ...instant, detachMs: 0 })).rejects.toBeInstanceOf(ApiConflict);
    const forbidden = platform({}, { [n]: {} }, { driveError: Object.assign(new Error('Forbidden'), { response: { status: 403 } }) });
    await expect(deleteSession(n, { api: forbidden.api, ...instant })).rejects.toThrow('Forbidden');
    expect(forbidden.calls.filter((c) => c.startsWith('deleteDrive'))).toHaveLength(1);
    await expect(deleteSession('dh-probe-abc', { api: stuck.api, ...instant })).rejects.toBeInstanceOf(TypeError);
  });

  it('a 429 or 5xx from stop or delete is retried; a VM snapshotting is waited for, never stopped', async () => {
    const n = name('wrun_flaky');
    const p = platform({ [n]: { status: 'running', startedAt: NOW } }, { [n]: {} });
    const vm = (await p.api.getSandbox(n))!;
    let stops = 0;
    let deletes = 0;
    const busy = (status: number) => Object.assign(new Error(`Status code ${status}`), { response: { status } });
    const api: CleanupApi = {
      ...p.api,
      getSandbox: async () => ({
        ...vm,
        stop: async () => (++stops < 2 ? Promise.reject(busy(429)) : vm.stop()),
        delete: async () => (++deletes < 3 ? Promise.reject(busy(503)) : vm.delete()),
      }),
    };
    expect(await deleteSession(n, { api, ...instant })).toEqual({ name: n, sandbox: 'deleted', drive: 'deleted' });
    expect([stops, deletes]).toEqual([2, 3]);

    const m = name('wrun_snap');
    const q = platform({ [m]: { status: 'snapshotting', startedAt: NOW } }, { [m]: {} });
    let reads = 0;
    const settling: CleanupApi = {
      ...q.api,
      getSandbox: async (x) => {
        // Saving its state for two reads, then stopped.
        if (++reads === 3) q.vms[m]!.status = 'stopped';
        return q.api.getSandbox(x);
      },
    };
    expect(await deleteSession(m, { api: settling, ...instant })).toEqual({ name: m, sandbox: 'deleted', drive: 'deleted' });
    expect(q.calls.filter((c) => c.startsWith('stop'))).toEqual([]);
    expect(q.calls.filter((c) => c.startsWith('delete:'))).toEqual([`delete:${m}`]);
  });

  it('ifIdle: a session that resumed since the caller looked (its VM running, a newer VM session, or its Drive held) is SessionInUse, nothing stopped or deleted', async () => {
    const n = name('wrun_resumed');
    // The caller saw a stopped VM; it runs now.
    const p = platform({ [n]: { status: 'running', startedAt: NOW - 40 * DAY } }, { [n]: {} });
    await expect(deleteSession(n, { api: p.api, ...instant, ifIdle: { startedAt: NOW - 40 * DAY } })).rejects.toBeInstanceOf(SessionInUse);
    // A new VM session, already stopped again.
    p.vms[n] = { status: 'stopped', startedAt: NOW };
    await expect(deleteSession(n, { api: p.api, ...instant, ifIdle: { startedAt: NOW - 40 * DAY } })).rejects.toBeInstanceOf(SessionInUse);
    // A VM where the caller saw none.
    await expect(deleteSession(n, { api: p.api, ...instant, ifIdle: { startedAt: undefined } })).rejects.toBeInstanceOf(SessionInUse);
    expect(p.calls.filter((c) => !c.startsWith('get:'))).toEqual([]);
    // The Drive held by a VM session the read didn't show: not waited out, kept.
    const m = name('wrun_held');
    const q = platform({}, { [m]: {} }, { attachedFor: 1000 });
    await expect(deleteSession(m, { api: q.api, ...instant, ifIdle: { startedAt: undefined } })).rejects.toBeInstanceOf(SessionInUse);
    expect(q.calls.filter((c) => c.startsWith('deleteDrive'))).toHaveLength(1);
  });
});

describe('stopSession: a closed session\'s VM stopped, its sandbox and Drive kept, never booting a VM', () => {
  it('a running or booting VM is stopped once; calling it again finds nothing running', async () => {
    for (const status of ['running', 'pending'] as const) {
      const n = name(`wrun_stop_${status}`);
      const p = platform({ [n]: { status, startedAt: NOW } }, { [n]: {} });
      expect(await stopSession(n, { api: p.api, ...instant })).toEqual({ name: n, sandbox: 'stopped' });
      expect(await stopSession(n, { api: p.api, ...instant })).toEqual({ name: n, sandbox: 'none' });
      expect(p.calls).toEqual([`get:${n}`, `stop:${n}`, `get:${n}`]);
      // Nothing deleted: the sandbox and the Drive are still there.
      expect(Object.keys(p.vms)).toEqual([n]);
      expect(Object.keys(p.drives)).toEqual([n]);
    }
  });

  it('a VM stopped, stopping, snapshotting or not there is left alone; a name that is not a session\'s is refused', async () => {
    for (const status of ['stopped', 'stopping', 'snapshotting', 'failed'] as const) {
      const n = name(`wrun_left_${status}`);
      const p = platform({ [n]: { status, startedAt: NOW } }, { [n]: {} });
      expect(await stopSession(n, { api: p.api, ...instant })).toEqual({ name: n, sandbox: 'none' });
      expect(p.calls).toEqual([`get:${n}`]);
    }
    const none = platform({}, {});
    expect(await stopSession(name('wrun_none'), { api: none.api, ...instant })).toEqual({ name: name('wrun_none'), sandbox: 'none' });
    await expect(stopSession('dh-base-abc', { api: none.api, ...instant })).rejects.toBeInstanceOf(TypeError);
  });

  it('a 429 or 5xx from the stop is retried; a 404 is nothing to stop; another refusal is thrown', async () => {
    const n = name('wrun_stop_flaky');
    const p = platform({ [n]: { status: 'running', startedAt: NOW } }, { [n]: {} });
    const vm = (await p.api.getSandbox(n))!;
    const refusal = (status: number) => Object.assign(new Error(`Status code ${status}`), { response: { status } });
    let stops = 0;
    const flaky: CleanupApi = { ...p.api, getSandbox: async () => ({ ...vm, stop: async () => (++stops < 3 ? Promise.reject(refusal(stops === 1 ? 429 : 502)) : vm.stop()) }) };
    expect(await stopSession(n, { api: flaky, ...instant })).toEqual({ name: n, sandbox: 'stopped' });
    expect(stops).toBe(3);
    const gone: CleanupApi = { ...p.api, getSandbox: async () => ({ ...vm, status: 'running', stop: async () => Promise.reject(refusal(404)) }) };
    expect(await stopSession(n, { api: gone, ...instant })).toEqual({ name: n, sandbox: 'none' });
    const forbidden: CleanupApi = { ...p.api, getSandbox: async () => ({ ...vm, status: 'running', stop: async () => Promise.reject(refusal(403)) }) };
    await expect(stopSession(n, { api: forbidden, ...instant })).rejects.toThrow('Status code 403');
    expect(p.calls.some((c) => c.startsWith('BOOT'))).toBe(false);
  });
});

describe('cleanupIdle: sessions idle for idleDays', () => {
  it('deletes only idle sessions: a running VM, recent log activity, a recent VM and a session the World doesn\'t name are kept', async () => {
    const [idle, running, recentLog, recentVm, orphan, held] = ['idle', 'running', 'log', 'vm', 'orphan', 'held'].map((s) => name(`wrun_${s}`)) as [string, string, string, string, string, string];
    const p = platform(
      { [running]: { status: 'running', startedAt: 0 }, [recentVm]: { status: 'stopped', startedAt: NOW - 2 * DAY }, [idle]: { status: 'stopped', startedAt: NOW - 40 * DAY } },
      { [idle]: {}, [running]: {}, [recentLog]: {}, [recentVm]: {}, [orphan]: {}, [held]: { currentSessionId: 'sbx_other' }, 'dh-probe-x1': {}, 'dh-base-iad1-0123': {} }
    );
    const activity: Record<string, number> = { [idle]: NOW - 31 * DAY, [running]: NOW - 90 * DAY, [recentLog]: NOW - DAY, [recentVm]: NOW - 60 * DAY, [held]: NOW - 90 * DAY };
    const result = await cleanupIdle({ api: p.api, ...instant, idleDays: 30, now: () => NOW, lastActivity: async (n) => activity[n] });
    expect(result.deleted).toEqual([{ name: idle, sandbox: 'deleted', drive: 'deleted' }]);
    expect(Object.fromEntries(result.kept.map((k) => [k.name, k.reason]))).toEqual({ [running]: 'running', [recentLog]: 'recent', [recentVm]: 'recent', [orphan]: 'unknown', [held]: 'running' });
    // Not a session's: never looked at.
    expect(p.calls.some((c) => c.includes('dh-probe') || c.includes('dh-base'))).toBe(false);
    expect(p.calls.some((c) => c.startsWith('BOOT'))).toBe(false);
    expect(p.calls.filter((c) => c.startsWith('stop'))).toEqual([]);
  });

  it('orphans: a Drive no session names goes once its VM is idle too; a failure keeps that one and goes on', async () => {
    const [orphan, busy, other] = ['o', 'b', 'c'].map((s) => name(`wrun_${s}`)) as [string, string, string];
    const p = platform({ [busy]: { status: 'stopped', startedAt: NOW - 50 * DAY } }, { [orphan]: {}, [busy]: {}, [other]: {} });
    const api = { ...p.api, deleteDrive: async (n: string) => (n === busy ? Promise.reject(Object.assign(new Error('Forbidden'), { response: { status: 403 } })) : p.api.deleteDrive(n)) };
    const result = await cleanupIdle({ api, ...instant, idleDays: 7, now: () => NOW, orphans: true, lastActivity: async () => undefined });
    expect(result.deleted.map((d) => d.name).sort()).toEqual([orphan, other].sort());
    expect(result.kept).toEqual([{ name: busy, reason: 'failed', error: 'Forbidden' }]);
  });

  it('a session that resumes between the decision and the delete is kept as running: no stop, no Drive delete', async () => {
    const n = name('wrun_race');
    const p = platform({ [n]: { status: 'stopped', startedAt: NOW - 40 * DAY } }, { [n]: {} });
    let reads = 0;
    const api: CleanupApi = {
      ...p.api,
      getSandbox: async (x) => {
        // The driver resumes the session right after cleanupIdle's read (its lastActivity round trip is the window).
        if (++reads === 2) p.vms[n] = { status: 'running', startedAt: NOW };
        return p.api.getSandbox(x);
      },
    };
    const result = await cleanupIdle({ api, ...instant, idleDays: 30, now: () => NOW, lastActivity: async () => NOW - 40 * DAY });
    expect(result.deleted).toEqual([]);
    expect(result.kept).toEqual([{ name: n, reason: 'running' }]);
    expect(p.calls.filter((c) => c.startsWith('stop') || c.startsWith('delete'))).toEqual([]);
    expect(p.vms[n]?.status).toBe('running');
  });

  it('deadline and maxSessions: what was not reached is returned, and resumeAt begins the next run there, wrapping around', async () => {
    const names = ['a', 'b', 'c', 'd', 'e'].map((s) => name(`wrun_${s}`)).sort();
    const p = platform({}, Object.fromEntries(names.map((n) => [n, {}])));
    const base = { api: p.api, ...instant, idleDays: 30, now: () => NOW, lastActivity: async () => NOW - DAY };
    const first = await cleanupIdle({ ...base, maxSessions: 2, resumeAt: names[3] });
    expect(first.kept.map((k) => k.name)).toEqual([names[3], names[4]]);
    expect(first.unreached).toEqual([names[0], names[1], names[2]]);
    expect(first.resumeAt).toBe(names[0]);
    const second = await cleanupIdle({ ...base, maxSessions: 10, resumeAt: first.resumeAt });
    expect(second.kept.map((k) => k.name)).toEqual([...names.slice(0, 5)]);
    expect(second.unreached).toEqual([]);
    expect(second.resumeAt).toBeUndefined();
    // A deadline already past: nothing is looked at.
    const late = await cleanupIdle({ ...base, deadline: NOW - 1, resumeAt: names[0] });
    expect(late.unreached).toEqual(names);
    expect(late.kept).toEqual([]);
    await expect(cleanupIdle({ ...base, maxSessions: 0 })).rejects.toBeInstanceOf(TypeError);
  });

  it('refuses idleDays that is not a positive number, before listing anything', async () => {
    const p = platform({}, {});
    for (const idleDays of [0, -1, Number.NaN, '30' as never]) await expect(cleanupIdle({ api: p.api, idleDays, lastActivity: async () => 0 })).rejects.toBeInstanceOf(TypeError);
    expect(p.calls).toEqual([]);
  });
});

describe('rateLimiter: the platform\'s 20 deletes a second', () => {
  it('starts at most 20 calls in any second, across callers', async () => {
    let now = 0;
    const started: number[] = [];
    const limit = rateLimiter(20, { now: () => now, sleep: async (ms) => void (now += ms) });
    for (let i = 0; i < 60; i++) {
      await limit();
      started.push(now);
    }
    for (let i = 20; i < started.length; i++) expect(started[i]! - started[i - 20]!).toBeGreaterThanOrEqual(1000);
    expect(started.at(-1)).toBeLessThanOrEqual(3000);
  });

  it('the deletes of one session wait their turns on it', async () => {
    const n = name('wrun_rate');
    const p = platform({ [n]: { status: 'running', startedAt: NOW } }, { [n]: {} });
    let turns = 0;
    await deleteSession(n, { api: p.api, limit: async () => void turns++, sleep: async () => {} });
    // stop, delete, the Drive.
    expect(turns).toBe(3);
  });
});

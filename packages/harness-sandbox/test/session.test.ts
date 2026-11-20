/**
 * The session factory's lifecycle (docs/workspaces.md §5.7, §11.4 A3) on the
 * stand-in: the Sandbox API is `FakeSandboxApi` (a local relay child per "VM",
 * a temp dir per "Drive"), the relay and the link are the real ones. What the
 * live suite (`live-session.test.ts`) does on the platform, this does here, plus
 * the races and failures a live VM can't be made to show on demand.
 */
import { Unavailable, Yield } from 'harness-world';
import { isBrandedMachine, type AcquireMachineRequest, type FenceToken, type MachineRecord, type SessionMachine } from 'harness-world-process';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { sessionName } from '../src/names.ts';
import { ApiConflict } from '../src/api.ts';
import { acknowledgeNotes, createCapOf, driveQuotaRefusal, extendPastCap, FILES_LOST_NOTE, HOBBY_CAP_MS, networkPolicy, PLAN_CAP_MAX_MS, parseArmReply, policySecrets, redactedError, SandboxBudgetExceeded, SandboxLost, type SandboxNetworkValue, type SandboxSessionOptions, sandboxSession } from '../src/session.ts';
import { FakeSandboxApi } from './fake-api.ts';

beforeAll(() => {
  process.env.DH_TEST_LOCAL_SANDBOX = '1';
  delete process.env.VERCEL;
});

const apis: FakeSandboxApi[] = [];
const machines: SessionMachine[] = [];
afterEach(async () => {
  for (const m of machines.splice(0)) await m.close().catch(() => {});
  for (const a of apis.splice(0)) await a.dispose();
});

const SESSION = 'wrun_session_1';
const NAME = sessionName('prj_stand_in', SESSION);

function fakeApi(...args: ConstructorParameters<typeof FakeSandboxApi>): FakeSandboxApi {
  const api = new FakeSandboxApi(...args);
  apis.push(api);
  return api;
}

function factory(api: FakeSandboxApi, overrides: Partial<Omit<SandboxSessionOptions, 'spec'>> & { spec?: Partial<SandboxSessionOptions['spec']> } = {}) {
  const { spec, ...rest } = overrides;
  return sandboxSession({
    spec: { vcpus: 2, network: 'allow-all', idleMs: 60_000, driveSize: 10 * 1024 ** 3, budget: { vmSeconds: Infinity }, image: 'stand-in', ...spec },
    busyMs: 1_500,
    cliVersions: { 'fake-cli': '0.0.0' },
    ...rest,
    test: { api, allowDegraded: true, allowUnauthenticatedModel: true, probeMs: 50, ...rest.test },
  });
}

/** An in-memory record store (the kernel's is the `hw.sandbox` document). */
function memoryRecord(initial?: MachineRecord) {
  let record = initial;
  return { store: { read: () => record, write: (r: MachineRecord) => void (record = r) }, get: () => record };
}

/** One invocation's request: its own signal, and a fence the test can narrow (`set`), as a drive's `fenced` turns false. */
function invocation(fence: FenceToken | undefined, extra: Partial<AcquireMachineRequest> = {}) {
  const controller = new AbortController();
  let reads = 0;
  let current = fence;
  const request: AcquireMachineRequest = {
    sessionId: SESSION,
    fence: () => (reads++, current),
    signal: controller.signal,
    ...extra,
  };
  return { request, controller, reads: () => reads, set: (f: FenceToken | undefined) => void (current = f) };
}

async function track(p: Promise<SessionMachine>): Promise<SessionMachine> {
  const m = await p;
  machines.push(m);
  return m;
}

async function run(machine: SessionMachine, script: string): Promise<string> {
  const proc = await machine.spawn({ command: 'bash', args: ['-c', script] });
  let out = '';
  for await (const l of proc.lines()) out += `${l}\n`;
  await proc.exited;
  return out.trim();
}

async function rejection(p: Promise<unknown>): Promise<unknown> {
  return p.then(
    () => undefined,
    (e: unknown) => e
  );
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe('the session factory: attach, and restart on a new VM', () => {
  it('creates the Drive and the VM, arms with the fence, and returns a branded machine; a new VM on the same Drive keeps the files', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    const rec = memoryRecord();
    const first = invocation({ count: 3, fenced: true }, { record: rec.store });
    const m1 = await track(acquire(first.request));
    expect(isBrandedMachine(m1)).toBe(true);
    expect(m1.fence).toEqual({ count: 3, fenced: true });
    expect(m1.notes).toEqual([]);
    expect(api.calls.filter((c) => c.startsWith('createSandbox') || c.startsWith('createDrive'))).toEqual([`createDrive:${NAME}:iad1`, `createSandbox:${NAME}:iad1`]);
    const vm1 = api.vms.get(NAME)!.sessionId;
    await run(m1, 'echo kept > note.txt');
    expect(rec.get()).toMatchObject({ sandboxName: NAME, driveName: NAME, region: 'iad1', image: 'stand-in', versions: { 'fake-cli': '0.0.0' } });
    // The same invocation, while the link lives: the same machine; the fence is read in each acquire (each attach fact's body).
    expect(await acquire(first.request)).toBe(m1);
    expect(first.reads()).toBe(2);
    await m1.close();

    // Tear the VM down between invocations; the Drive stays.
    await (await api.getSandbox(NAME))!.delete();
    const second = invocation({ count: 9, fenced: true }, { record: rec.store });
    const m2 = await track(acquire(second.request));
    expect(api.vms.get(NAME)!.sessionId).not.toBe(vm1);
    expect(await run(m2, 'cat note.txt')).toBe('kept');
    expect(m2.notes).toEqual([]);
  });

  it('reuses a running VM, re-applies the network policy and extends its timeout to the invocation plus idleMs', async () => {
    const api = fakeApi();
    const acquire = factory(api, { spec: { idleMs: 120_000 } });
    const m1 = await track(acquire(invocation({ count: 1, fenced: true }, { deadline: Date.now() + 10_000 }).request));
    await m1.close();
    const vmSession = api.vms.get(NAME)!.sessionId;
    api.calls.length = 0;
    const m2 = await track(acquire(invocation({ count: 2, fenced: true }, { deadline: Date.now() + 300_000 }).request));
    expect(api.vms.get(NAME)!.sessionId).toBe(vmSession);
    expect(api.calls).toContain(`setNetwork:${NAME}`);
    expect(api.calls.some((c) => c.startsWith(`extendTimeout:${NAME}`))).toBe(true);
    expect(api.calls.some((c) => c.startsWith('createSandbox') || c.startsWith('delete'))).toBe(false);
    expect(await run(m2, 'echo up')).toBe('up');
  });

  it('a stopped sandbox is deleted by its owner and created again (P0.4), retrying while the Drive is still attached (P0.5)', async () => {
    const api = fakeApi({ detachMs: 600 });
    const acquire = factory(api);
    await (await track(acquire(invocation({ count: 1, fenced: true }).request))).close();
    await api.stopVm(NAME);
    const t0 = Date.now();
    const m = await track(acquire(invocation({ count: 2, fenced: true }).request));
    expect(Date.now() - t0).toBeGreaterThanOrEqual(400);
    expect(api.calls.filter((c) => c.startsWith('delete:')).length).toBe(1);
    expect(await run(m, 'echo again')).toBe('again');
  });

  it('adopts the Drive another invocation created first, in its region and size', async () => {
    const api = fakeApi();
    api.beforeCreateDrive = (name) => {
      api.beforeCreateDrive = undefined;
      api.addDrive(name, 'fra1', 20 * 1024 ** 3);
    };
    const logs: string[] = [];
    const acquire = factory(api, { spec: { region: 'iad1' }, log: (m) => logs.push(m) });
    const rec = memoryRecord();
    await track(acquire(invocation({ count: 1, fenced: true }, { record: rec.store }).request));
    expect(api.calls).toContain(`createSandbox:${NAME}:fra1`);
    expect(rec.get()).toMatchObject({ region: 'fra1', driveSize: 20 * 1024 ** 3 });
    expect(logs).toContain('the Drive keeps its region');
  });

  it('a Drive with another id than the record\'s gives the "files lost" note, on every attach until the runtime acknowledges it', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    const rec = memoryRecord();
    await (await track(acquire(invocation({ count: 1, fenced: true }, { record: rec.store }).request))).close();
    const firstId = rec.get()!.driveId;
    await (await api.getSandbox(NAME))!.delete();
    api.dropDrive(NAME);
    const inv2 = invocation({ count: 2, fenced: true }, { record: rec.store });
    const m2 = await track(acquire(inv2.request));
    expect(m2.notes).toEqual([FILES_LOST_NOTE]);
    expect(Object.isFrozen(m2.notes)).toBe(true);
    expect(rec.get()!.driveId).not.toBe(firstId);
    expect(rec.get()!.filesLost).toBe(rec.get()!.driveId);
    // The link drops later in the same invocation: the re-attached machine still carries it.
    await api.relayOf(NAME)!.kill();
    await api.relayOf(NAME)!.restart();
    for (let i = 0; i < 40 && !m2.signal.aborted; i++) await sleep(50);
    const m2b = await track(acquire(inv2.request));
    expect(m2b).not.toBe(m2);
    expect(m2b.notes).toEqual([FILES_LOST_NOTE]);
    await m2b.close();
    // The turn failed before the model saw it (nothing acknowledged): the next invocation shows it again.
    const m3 = await track(acquire(invocation({ count: 3, fenced: true }, { record: rec.store }).request));
    expect(m3.notes).toEqual([FILES_LOST_NOTE]);
    await m3.close();
    // Acknowledged for another Drive (a loss told earlier): this one is kept.
    expect(m3.notesKey).toBe(rec.get()!.filesLost);
    expect(acknowledgeNotes(rec.get()!, 'drv_an_older_one')).toEqual(rec.get());
    // Acknowledged in the turn that told the model (the key the machine carried): gone.
    rec.store.write(acknowledgeNotes(rec.get()!, m3.notesKey));
    const m4 = await track(acquire(invocation({ count: 4, fenced: true }, { record: rec.store }).request));
    expect(m4.notes).toEqual([]);
  });

  it('a Drive deleted and made again between the check and the mount is noticed: the new one is mounted, with the note', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    const rec = memoryRecord();
    await (await track(acquire(invocation({ count: 1, fenced: true }, { record: rec.store }).request))).close();
    await (await api.getSandbox(NAME))!.delete();
    api.beforeCreateSandbox = (name) => {
      api.beforeCreateSandbox = undefined;
      api.dropDrive(name);
      api.addDrive(name);
    };
    const m2 = await track(acquire(invocation({ count: 2, fenced: true }, { record: rec.store }).request));
    expect(m2.notes).toEqual([FILES_LOST_NOTE]);
    expect(rec.get()!.driveId).toBe(api.drives.get(NAME)!.info.driveId);
  });

  it('rotates a VM near the plan cap between turns, onto the same Drive, and never inside an invocation', async () => {
    const api = fakeApi();
    const acquire = factory(api, { planCapMs: 10 * 60_000, spec: { idleMs: 60_000 } });
    const inv1 = invocation({ count: 1, fenced: true }, { deadline: Date.now() + 60_000 });
    const m1 = await track(acquire(inv1.request));
    await run(m1, 'echo before > f.txt');
    const vm1 = api.vms.get(NAME)!.sessionId;
    // Within the invocation, a long-running call is never rotated away: the same machine comes back.
    const sleeper = await m1.spawn({ command: 'bash', args: ['-c', 'sleep 1; echo done'] });
    api.age(NAME, 9 * 60_000);
    expect(await acquire(inv1.request)).toBe(m1);
    let out = '';
    for await (const l of sleeper.lines()) out += l;
    expect(out).toBe('done');
    await m1.close();
    // The next invocation needs more than the VM has left: a fresh VM, same Drive.
    const m2 = await track(acquire(invocation({ count: 2, fenced: true }, { deadline: Date.now() + 120_000 }).request));
    expect(api.vms.get(NAME)!.sessionId).not.toBe(vm1);
    expect(await run(m2, 'cat f.txt')).toBe('before');
  });
});

describe("the session factory: the plan's limits, learned", () => {
  const logs = () => {
    const lines: { message: string; fields?: Record<string, unknown> }[] = [];
    return { lines, log: (message: string, fields?: Record<string, unknown>) => void lines.push({ message, ...(fields ? { fields } : {}) }) };
  };

  it('Hobby (a 45 min cap): at the assumed cap, the extension is refused, the cap is learned, and the VM rotates', async () => {
    const api = fakeApi({ planCapMs: HOBBY_CAP_MS });
    const l = logs();
    const acquire = factory(api, { spec: { idleMs: 60_000 }, log: l.log });
    await (await track(acquire(invocation({ count: 1, fenced: true }, { deadline: Date.now() + 60_000 }).request))).close();
    const vm1 = api.vms.get(NAME)!.sessionId;
    api.age(NAME, 44 * 60_000);
    await track(acquire(invocation({ count: 2, fenced: true }, { deadline: Date.now() + 120_000 }).request));
    expect(api.vms.get(NAME)!.sessionId).not.toBe(vm1);
    expect(l.lines.find((x) => x.message === 'learned the plan cap')?.fields).toMatchObject({ planCapMs: HOBBY_CAP_MS });
  });

  it('Pro (a 24 h cap): at the assumed cap, the extension is granted, the larger cap is learned, and the VM stays', async () => {
    const api = fakeApi({ planCapMs: PLAN_CAP_MAX_MS });
    const l = logs();
    const acquire = factory(api, { spec: { idleMs: 60_000 }, log: l.log });
    await (await track(acquire(invocation({ count: 1, fenced: true }, { deadline: Date.now() + 60_000 }).request))).close();
    const vm1 = api.vms.get(NAME)!.sessionId;
    api.age(NAME, 44 * 60_000);
    await track(acquire(invocation({ count: 2, fenced: true }, { deadline: Date.now() + 120_000 }).request));
    expect(api.vms.get(NAME)!.sessionId).toBe(vm1);
    expect(l.lines.find((x) => x.message === 'learned the plan cap')?.fields).toMatchObject({ planCapMs: PLAN_CAP_MAX_MS });
  });

  it('an idleMs past the assumed cap: until an extension proved more, a create asks for 45 min at most, and boots at once (on any plan, whatever its refusal says)', async () => {
    for (const createRefusal of [undefined, 'Status code 400 is not ok: timeout is too long for this plan']) {
      const api = fakeApi({ planCapMs: HOBBY_CAP_MS, ...(createRefusal ? { createRefusal } : {}) });
      const l = logs();
      await track(factory(api, { spec: { idleMs: 2 * 60 * 60_000 }, log: l.log })(invocation({ count: 1, fenced: true }).request));
      expect(api.vms.get(NAME)!.timeout).toBeLessThanOrEqual(HOBBY_CAP_MS);
      expect(api.calls.filter((c) => c.startsWith('createSandbox'))).toHaveLength(1);
      await api.dispose();
    }
  });

  it('a cap past 45 min that the plan refuses at create, with a message the factory can\'t read: any 400 falls back to 45 min, once, and the cap is learned', async () => {
    const api = fakeApi({ planCapMs: HOBBY_CAP_MS, createRefusal: 'Status code 400 is not ok: invalid timeout' });
    const l = logs();
    // A 24 h cap given (or learned on another plan): the create asks for more than Hobby allows.
    await track(factory(api, { planCapMs: PLAN_CAP_MAX_MS, spec: { idleMs: 2 * 60 * 60_000 }, log: l.log })(invocation({ count: 1, fenced: true }).request));
    expect(api.vms.get(NAME)!.timeout).toBeLessThanOrEqual(HOBBY_CAP_MS);
    expect(api.calls.filter((c) => c.startsWith('createSandbox'))).toHaveLength(2);
    expect(l.lines.find((x) => x.message === 'learned the plan cap')?.fields).toMatchObject({ planCapMs: HOBBY_CAP_MS });
  });

  it('a bare 400 that the shorter create meets too is not the cap: the original error is thrown, and the cap is left as it was', async () => {
    const api = fakeApi();
    const l = logs();
    const acquire = factory(api, { planCapMs: PLAN_CAP_MAX_MS, spec: { idleMs: 2 * 60 * 60_000 }, log: l.log });
    let n = 0;
    api.beforeCreateSandbox = () => {
      throw Object.assign(new Error(`Status code 400 is not ok: bad network policy (${++n})`), { response: { status: 400 } });
    };
    await expect(acquire(invocation({ count: 1, fenced: true }).request)).rejects.toThrow(/bad network policy \(1\)/);
    expect(api.calls.filter((c) => c.startsWith('createSandbox'))).toHaveLength(2);
    expect(l.lines.some((x) => x.message === 'learned the plan cap')).toBe(false);
    // The next create still asks for more than 45 min: the 24 h cap stands.
    api.beforeCreateSandbox = undefined;
    await track(acquire(invocation({ count: 2, fenced: true }).request));
    expect(api.vms.get(NAME)!.timeout).toBeGreaterThan(HOBBY_CAP_MS);
  });

  it('a create refused for something else is not taken for the cap: the error is thrown', async () => {
    const api = fakeApi();
    const acquire = factory(api, { planCapMs: PLAN_CAP_MAX_MS, spec: { idleMs: 2 * 60 * 60_000 } });
    // Not a 400: no fallback.
    api.beforeCreateSandbox = () => {
      throw Object.assign(new Error('Status code 500 is not ok: internal'), { response: { status: 500 } });
    };
    await expect(acquire(invocation({ count: 1, fenced: true }).request)).rejects.toThrow(/internal/);
    expect(api.calls.filter((c) => c.startsWith('createSandbox'))).toHaveLength(1);
  });

  it("a plan whose Drive quota is below driveSize (Hobby: 1 GiB): the Drive gets the plan's default size, and the log says so", async () => {
    const api = fakeApi({ driveQuota: 1024 ** 3 });
    const l = logs();
    await track(factory(api, { spec: { driveSize: 10 * 1024 ** 3 }, log: l.log })(invocation({ count: 1, fenced: true }).request));
    expect(api.drives.get(NAME)!.info.maxSize).toBe(1024 ** 3);
    expect(api.calls.filter((c) => c.startsWith('createDrive'))).toEqual([`createDrive:${NAME}:iad1`, `createDrive:${NAME}:iad1:default`]);
    expect(l.lines.some((x) => /plan's default size/.test(x.message))).toBe(true);
  });

  it('driveQuotaRefusal is the measured quota refusal only (P0.15), never another size error or a server error', () => {
    const e = (status: number, message: string) => Object.assign(new Error(message), { response: { status } });
    expect(driveQuotaRefusal(e(400, 'Status code 400 is not ok: `maxSizeBytes` must not exceed the Drive quota of 1073741824 bytes. Request a higher quota'))).toBe(true);
    expect(driveQuotaRefusal(e(400, 'Status code 400 is not ok: `maxSizeBytes` should be >= 1073741824'))).toBe(false);
    expect(driveQuotaRefusal(e(400, 'maxSize exceeds what the plan allows'))).toBe(false);
    expect(driveQuotaRefusal(e(500, 'must not exceed the Drive quota of 1073741824 bytes'))).toBe(false);
  });

  it('a Drive create refused for another reason is thrown, not retried at the default size', async () => {
    const api = fakeApi();
    api.beforeCreateDrive = () => {
      throw Object.assign(new Error('Status code 400 is not ok: `maxSizeBytes` should be >= 1073741824'), { response: { status: 400 } });
    };
    await expect(factory(api)(invocation({ count: 1, fenced: true }).request)).rejects.toThrow(/should be >= 1073741824/);
    expect(api.calls.filter((c) => c.startsWith('createDrive'))).toHaveLength(1);
  });

  it('createCapOf and extendPastCap read the platform\'s answers (P0: "`timeout` should be <= 1d", sandbox_timeout_invalid)', () => {
    const e = (message: string, extra: object = {}) => Object.assign(new Error(message), { response: { status: 400 } }, extra);
    expect(createCapOf(e('Status code 400 is not ok: `timeout` should be <= 1d'))).toBe(PLAN_CAP_MAX_MS);
    expect(createCapOf(e('Status code 400 is not ok: `timeout` should be <= 45m'))).toBe(HOBBY_CAP_MS);
    expect(createCapOf(e('drive_attached'))).toBeUndefined();
    expect(extendPastCap(e('Failed to extend timeout: extension would exceed maximum execution timeout', { json: { error: { code: 'sandbox_timeout_invalid' } } }))).toBe(true);
    expect(extendPastCap(e('internal'))).toBe(false);
  });
});

describe('the session factory: fences and racing deliveries', () => {
  it('a token that is not fenced never arms a fresh VM, never deletes, and never preempts the owner: the owner\'s process survives', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    // No VM yet: an unfenced delivery creates nothing.
    const e1 = await rejection(acquire(invocation({ count: 4, fenced: false }).request));
    expect(e1).toBeInstanceOf(Yield);
    expect((e1 as Yield).reason).toBe('contended');
    expect(api.calls.some((c) => c.startsWith('createSandbox'))).toBe(false);

    const owner = await track(acquire(invocation({ count: 4, fenced: true }).request));
    const proc = await owner.spawn({ command: 'bash', args: ['-c', 'echo $$; sleep 30'] });
    const pid = Number((await proc.lines()[Symbol.asyncIterator]().next()).value);
    // A racing delivery at the same count, not behind the fence: contended at the relay; nothing deleted.
    const e2 = await rejection(acquire(invocation({ count: 4, fenced: false }).request));
    expect((e2 as Yield).reason).toBe('contended');
    expect(() => process.kill(pid, 0)).not.toThrow();
    expect(proc.alive()).toBe(true);
    // A stopped VM and an unfenced delivery: it is not deleted (only an owner deletes).
    await owner.close();
    await api.stopVm(NAME);
    const e3 = await rejection(acquire(invocation({ count: 5, fenced: false }).request));
    expect((e3 as Yield).reason).toBe('contended');
    expect(api.calls.some((c) => c.startsWith('delete:'))).toBe(false);
    expect(api.vms.get(NAME)!.status).toBe('stopped');
  });

  it('a stale (lower) fence never deletes, re-applies or extends the newer owner\'s VM; it stops (preempted) only once the World agrees', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    const owner = await track(acquire(invocation({ count: 7, fenced: true }).request));
    api.calls.length = 0;
    // Still fenced by its own World read (it hasn't written since): the VM's word alone never parks the session for good.
    const stale = invocation({ count: 5, fenced: true });
    expect(((await rejection(acquire(stale.request))) as Yield).reason).toBe('contended');
    // Its writes are refused now: the World agrees, and it stops.
    stale.set({ count: 5, fenced: false });
    expect(((await rejection(acquire(stale.request))) as Yield).reason).toBe('preempted');
    expect(await run(owner, 'echo still')).toBe('still');
    expect(api.calls.some((c) => /^(delete|setNetwork|extendTimeout|createSandbox):/.test(c))).toBe(false);
  });

  it('a stale driver that still reads fenced, meeting the owner\'s VM near the plan cap, never rotates it: the owner\'s call survives', async () => {
    const api = fakeApi();
    const owner = await track(factory(api, { planCapMs: 10 * 60_000 })(invocation({ count: 7, fenced: true }).request));
    const proc = await owner.spawn({ command: 'bash', args: ['-c', 'sleep 20'] });
    api.age(NAME, 9 * 60_000);
    // Refused by the VM's watermark, and (with no watermark on it) by the relay's arm, which comes before any rotation.
    for (const mark of [7, undefined]) {
      api.mark(NAME, mark);
      api.calls.length = 0;
      const e = await rejection(factory(api, { planCapMs: 10 * 60_000 })(invocation({ count: 5, fenced: true }, { deadline: Date.now() + 120_000 }).request));
      expect(e).toBeInstanceOf(Yield);
      expect(api.calls.some((c) => c.startsWith('delete:') || c.startsWith('createSandbox:'))).toBe(false);
      expect(api.calls.some((c) => c.startsWith('run:')) === (mark === undefined)).toBe(true);
      expect(proc.alive()).toBe(true);
      expect(owner.signal.aborted).toBe(false);
    }
  });

  it('a VM that can\'t be asked is never deleted while its watermark names a newer owner', async () => {
    const api = fakeApi();
    await (await track(factory(api)(invocation({ count: 7, fenced: true }).request))).close();
    await api.crash(NAME);
    api.calls.length = 0;
    const e = await rejection(factory(api)(invocation({ count: 5, fenced: true }).request));
    expect(e).toBeInstanceOf(Yield);
    expect(api.calls.some((c) => c.startsWith('delete:'))).toBe(false);
  });

  it('a newer owner supersedes: the old driver\'s next acquire yields, deleting nothing (preempted once the World refuses it)', async () => {
    const api = fakeApi();
    const old = invocation({ count: 1, fenced: true });
    const acquireOld = factory(api);
    const m1 = await track(acquireOld(old.request));
    const acquireNew = factory(api);
    await track(acquireNew(invocation({ count: 2, fenced: true }).request));
    for (let i = 0; i < 40 && !m1.signal.aborted; i++) await sleep(50);
    expect(m1.signal.aborted).toBe(true);
    expect(((await rejection(acquireOld(old.request))) as Yield).reason).toBe('contended');
    old.set({ count: 1, fenced: false });
    expect(((await rejection(acquireOld(old.request))) as Yield).reason).toBe('preempted');
    expect(api.calls.some((c) => c.startsWith('delete:'))).toBe(false);
  });

  it('a token that stops being fenced mid-invocation narrows the machine: no VM made or deleted on re-attach, and no spawn or files', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    const inv = invocation({ count: 3, fenced: true });
    const m1 = await track(acquire(inv.request));
    expect(await run(m1, 'echo ok')).toBe('ok');
    inv.set({ count: 3, fenced: false });
    // The same live link: a machine that refuses what a stale drive must not run, and so does the one handed out before.
    const narrowed = await track(acquire(inv.request));
    expect(narrowed.fence).toEqual({ count: 3, fenced: false });
    await expect(narrowed.spawn({ command: 'true' })).rejects.toThrow(/no fenced token/);
    await expect(narrowed.files.upload('home', [])).rejects.toThrow(/no fenced token/);
    await expect(m1.spawn({ command: 'true' })).rejects.toThrow(/no fenced token/);
    await expect(m1.files.wipe()).rejects.toThrow(/no fenced token/);
    // Its workspace too: the RemoteMachine handed out while fenced refuses to change the VM, and still reads.
    await expect(m1.workspace.writeFile(`${m1.workspace.root}/x`, 'x')).rejects.toMatchObject({ code: 'EPERM' });
    await expect(m1.workspace.exec('echo no')).rejects.toMatchObject({ code: 'EPERM' });
    await expect(narrowed.workspace.mkdir(`${narrowed.workspace.root}/d`)).rejects.toMatchObject({ code: 'EPERM' });
    expect(await m1.workspace.stat(m1.workspace.root)).toBeDefined();
    // The link drops: the re-attach arms the same VM (no create, no delete), and the machine still refuses.
    api.calls.length = 0;
    await api.relayOf(NAME)!.kill();
    await api.relayOf(NAME)!.restart();
    for (let i = 0; i < 40 && !m1.signal.aborted; i++) await sleep(50);
    const m2 = await track(acquire(inv.request));
    expect(m2.fence).toEqual({ count: 3, fenced: false });
    await expect(m2.spawn({ command: 'true' })).rejects.toThrow(/no fenced token/);
    expect(api.calls.some((c) => /^(delete|createSandbox|setNetwork|extendTimeout):/.test(c))).toBe(false);
    // A count that changes within an invocation is refused.
    inv.set({ count: 4, fenced: true });
    await expect(acquire(inv.request)).rejects.toThrow(/fence changed/);
  });

  it('a transient Sandbox API failure on the arm (a 503, the Function\'s network) yields contended and deletes nothing', async () => {
    const api = fakeApi();
    await (await track(factory(api)(invocation({ count: 1, fenced: true }).request))).close();
    api.calls.length = 0;
    api.failRuns = { count: 1, error: () => Object.assign(new Error('Service Unavailable'), { response: { status: 503 } }) };
    expect(((await rejection(factory(api)(invocation({ count: 2, fenced: true }).request))) as Yield).reason).toBe('contended');
    api.failRuns = { count: 1, error: () => Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }) };
    expect(((await rejection(factory(api)(invocation({ count: 2, fenced: true }).request))) as Yield).reason).toBe('contended');
    // One unanswered command that the next probe answers is no loss either.
    api.failRuns = { count: 1, error: () => new Error('command timed out') };
    expect(((await rejection(factory(api)(invocation({ count: 2, fenced: true }).request))) as Yield).reason).toBe('contended');
    expect(api.calls.some((c) => c.startsWith('delete:'))).toBe(false);
    expect(await run(await track(factory(api)(invocation({ count: 2, fenced: true }).request)), 'echo alive')).toBe('alive');
  });

  it('a policy update that fails while the timeout extension is slow is thrown (a 5xx: contended), never left unhandled, and the extension is still counted', async () => {
    const api = fakeApi();
    const rec = memoryRecord();
    const acquire = factory(api, { spec: { budget: { vmSeconds: 36_000 } } });
    await (await track(acquire(invocation({ count: 1, fenced: true }, { record: rec.store }).request))).close();
    const unhandled: unknown[] = [];
    const onUnhandled = (e: unknown) => unhandled.push(e);
    process.on('unhandledRejection', onUnhandled);
    try {
      api.failNetwork = { delayMs: 0, error: () => Object.assign(new Error('update failed'), { response: { status: 400 } }) };
      api.extendMs = 200;
      const before = rec.get()!.vmSeconds!;
      const e = await rejection(acquire(invocation({ count: 2, fenced: true }, { record: rec.store, deadline: Date.now() + 1_200_000 }).request));
      expect(String((e as Error)?.message)).toMatch(/update failed/);
      expect(rec.get()!.vmSeconds!).toBeGreaterThan(before);
      api.failNetwork = { delayMs: 0, error: () => Object.assign(new Error('update failed'), { response: { status: 503 } }) };
      expect(((await rejection(acquire(invocation({ count: 3, fenced: true }, { record: rec.store }).request))) as Yield).reason).toBe('contended');
      await sleep(300);
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandled);
      api.failNetwork = undefined;
      api.extendMs = 0;
    }
  });

  it('a re-acquire with another model handler rebinds the socket; a refusal reaches the CLI as its status (a spend cap 403, the concurrency cap 429)', async () => {
    const api = fakeApi();
    const acquire = factory(api);
    const inv = invocation({ count: 1, fenced: true });
    const refuse = (code: string) => async () => {
      throw Object.assign(new Error(code), { code });
    };
    const m = await track(acquire({ ...inv.request, model: refuse('budget') }));
    const status = async () => {
      // The CLI's key (the attach's model key), as the CLI sends it.
      const proc = await m.spawn({ command: process.execPath, args: ['-e', `fetch('${m.modelUrl}/v1/messages',{method:'POST',body:'{}',headers:{authorization:'Bearer '+process.env.ANTHROPIC_AUTH_TOKEN}}).then(r=>r.text().then(t=>console.log(r.status, JSON.parse(t).error.type)))`], env: { ANTHROPIC_AUTH_TOKEN: m.modelKey } });
      let out = '';
      for await (const l of proc.lines()) out += l;
      await proc.exited;
      return out;
    };
    expect(await status()).toBe('403 budget');
    expect(await acquire({ ...inv.request, model: refuse('busy') })).toBe(m);
    expect(await status()).toBe('429 busy');
  });

  it('the invocation\'s end closes its links, even when the harness never closed its machine', async () => {
    const api = fakeApi();
    const inv = invocation({ count: 1, fenced: true });
    const m = await factory(api)(inv.request);
    expect(m.signal.aborted).toBe(false);
    inv.controller.abort();
    for (let i = 0; i < 20 && !m.signal.aborted; i++) await sleep(25);
    expect(m.signal.aborted).toBe(true);
  });

  it('without a token (a World without the in-band fence), the sandbox is Unavailable', async () => {
    const api = fakeApi();
    const e = await rejection(factory(api)(invocation(undefined).request));
    expect(e).toBeInstanceOf(Unavailable);
    expect(api.calls.some((c) => c.startsWith('createSandbox'))).toBe(false);
  });

  it('a Drive another VM session holds past the busy budget yields contended', async () => {
    const api = fakeApi({ detachMs: 60_000 });
    const acquire = factory(api, { busyMs: 800 });
    await (await track(acquire(invocation({ count: 1, fenced: true }).request))).close();
    await (await api.getSandbox(NAME))!.delete();
    const t0 = Date.now();
    const e = await rejection(acquire(invocation({ count: 2, fenced: true }).request));
    expect((e as Yield).reason).toBe('contended');
    expect(Date.now() - t0).toBeLessThan(5_000);
  });
});

describe('the session factory: losing the link within an invocation', () => {
  it('a dropped link re-attaches to the same VM with the same token; a crashed VM is replaced on the same Drive', async () => {
    const api = fakeApi({ detachMs: 300 });
    const acquire = factory(api);
    const inv = invocation({ count: 2, fenced: true });
    const m1 = await track(acquire(inv.request));
    await run(m1, 'echo one > a.txt');
    const vm1 = api.vms.get(NAME)!.sessionId;
    // A dropped socket (the relay restarts): the next acquire re-arms the same VM.
    await api.relayOf(NAME)!.kill();
    await api.relayOf(NAME)!.restart();
    for (let i = 0; i < 40 && !m1.signal.aborted; i++) await sleep(50);
    expect(m1.signal.aborted).toBe(true);
    const m2 = await track(acquire(inv.request));
    expect(m2).not.toBe(m1);
    expect(m2.fence).toEqual({ count: 2, fenced: true });
    expect(api.vms.get(NAME)!.sessionId).toBe(vm1);
    expect(await run(m2, 'cat a.txt')).toBe('one');
    // A crash inside the VM: the platform still says running, nothing answers: the owner replaces it.
    await api.crash(NAME);
    for (let i = 0; i < 40 && !m2.signal.aborted; i++) await sleep(50);
    const m3 = await track(acquire(inv.request));
    expect(api.vms.get(NAME)!.sessionId).not.toBe(vm1);
    expect(await run(m3, 'cat a.txt')).toBe('one');
    expect(inv.reads()).toBe(3);
  });

  it('past its re-attach budget, a lost link is a transport error (never a failed turn)', async () => {
    const api = fakeApi();
    const acquire = factory(api, { reattaches: 1 });
    const inv = invocation({ count: 2, fenced: true });
    let m = await track(acquire(inv.request));
    for (let i = 0; i < 2; i++) {
      await api.relayOf(NAME)!.kill();
      await api.relayOf(NAME)!.restart();
      for (let j = 0; j < 40 && !m.signal.aborted; j++) await sleep(50);
      if (i === 0) m = await track(acquire(inv.request));
    }
    const e = await rejection(acquire(inv.request));
    expect(e).toBeInstanceOf(SandboxLost);
    expect((e as SandboxLost).code).toBe('ECONNRESET');
  });
});

describe('the session factory: what it refuses', () => {
  it('refuses a degraded relay outside tests that allow it: Unavailable, the fresh VM deleted, its time counted', async () => {
    const api = fakeApi();
    const rec = memoryRecord();
    const e = await rejection(factory(api, { spec: { budget: { vmSeconds: 3600 } }, test: { api, allowDegraded: false } })(invocation({ count: 1, fenced: true }, { record: rec.store }).request));
    expect(e).toBeInstanceOf(Unavailable);
    expect(String((e as Error)?.message)).toMatch(/runs degraded/);
    expect(api.vms.has(NAME)).toBe(false);
    expect(rec.get()!.vmSeconds).toBeGreaterThan(0);
  });

  it("refuses a model handler on a relay that can't check the CLI's model calls (no /proc for the peer check), outside tests that allow it", async () => {
    const api = fakeApi({ noPeerCheck: true });
    const model = async () => ({ status: 200, headers: {}, body: (async function* () {})() });
    const e = await rejection(factory(api, { test: { api, allowUnauthenticatedModel: false } })(invocation({ count: 1, fenced: true }, { model }).request));
    expect(e).toBeInstanceOf(Unavailable);
    expect(String((e as Error)?.message)).toMatch(/model calls/);
  });

  it('a VM that never arms is replaced once and then deleted, and each delivery counts the VM time it was granted', async () => {
    const api = fakeApi({ relayless: true });
    const rec = memoryRecord();
    const acquire = factory(api, { spec: { budget: { vmSeconds: 36_000 } } });
    let before = 0;
    for (let delivery = 0; delivery < 2; delivery++) {
      api.calls.length = 0;
      const e = await rejection(acquire(invocation({ count: 1 + delivery, fenced: true }, { record: rec.store }).request));
      expect(e).toBeInstanceOf(Unavailable);
      expect(api.calls.filter((c) => c.startsWith('createSandbox:'))).toHaveLength(2);
      expect(api.vms.has(NAME)).toBe(false);
      expect(rec.get()!.vmSeconds!).toBeGreaterThan(before);
      before = rec.get()!.vmSeconds!;
    }
  });

  it('records only CLI versions this deployment pins: a VM\'s report alone never moves the record', async () => {
    const api = fakeApi({ cliVersions: { claude: '999.0.0', codex: '0.160.0' } });
    const rec = memoryRecord();
    await track(factory(api, { cliVersions: { claude: '2.1.289', codex: '0.160.0' } })(invocation({ count: 1, fenced: true }, { record: rec.store }).request));
    expect(rec.get()!.versions).toEqual({ codex: '0.160.0' });
  });

  it('a finite VM budget without a record is a TypeError (it would hold per invocation only)', async () => {
    const api = fakeApi();
    await expect(factory(api, { spec: { budget: { vmSeconds: 600 } } })(invocation({ count: 1, fenced: true }).request)).rejects.toThrow(TypeError);
    expect(api.calls).toEqual([]);
  });

  it('refuses a CLI downgrade with Unavailable(cli_version)', async () => {
    const api = fakeApi();
    const rec = memoryRecord({ versions: { 'fake-cli': '1.2.0' } });
    const e = await rejection(factory(api)(invocation({ count: 1, fenced: true }, { record: rec.store }).request));
    expect(e).toBeInstanceOf(Unavailable);
    expect((e as Unavailable).code).toBe('cli_version');
  });

  it('stops granting VM time past budget.vmSeconds: Unavailable, so the turn ends at once', async () => {
    const api = fakeApi();
    const rec = memoryRecord({ vmSeconds: 600 });
    const e = await rejection(factory(api, { spec: { budget: { vmSeconds: 600 } } })(invocation({ count: 1, fenced: true }, { record: rec.store }).request));
    expect(e).toBeInstanceOf(SandboxBudgetExceeded);
    expect(e).toBeInstanceOf(Unavailable);
    expect(api.calls.some((c) => c.startsWith('createSandbox'))).toBe(false);
    // Within budget: the VM's timeout is capped by what is left, and the grant is recorded.
    const rec2 = memoryRecord({ vmSeconds: 590 });
    await track(factory(api, { spec: { budget: { vmSeconds: 600 } } })(invocation({ count: 1, fenced: true }, { record: rec2.store }).request));
    expect(api.vms.get(NAME)!.timeout).toBeLessThanOrEqual(10_000);
    expect(rec2.get()!.vmSeconds).toBe(600);
  });

  it('a spawn env carrying a Function value is refused before it leaves', async () => {
    const api = fakeApi();
    process.env.DH_TEST_FUNCTION_SECRET = 'function-only-value-0123456789';
    try {
      const m = await track(factory(api)(invocation({ count: 1, fenced: true }).request));
      await expect(m.spawn({ command: 'true', env: { HARMLESS: process.env.DH_TEST_FUNCTION_SECRET } })).rejects.toThrow(TypeError);
    } finally {
      delete process.env.DH_TEST_FUNCTION_SECRET;
    }
  });

  it("the machine's workspace is a RemoteMachine on the VM (its output cap is env.test.ts's), and honours timeoutMs", async () => {
    const api = fakeApi();
    const m = await track(factory(api)(invocation({ count: 1, fenced: true }).request));
    expect(m.workspace.isolation).toBe('sandbox');
    expect((await m.workspace.exec('echo hi')).stdout).toBe('hi\n');
    const t0 = Date.now();
    const slow = await m.workspace.exec('sleep 10', { timeoutMs: 300 });
    expect(slow.exitCode).toBe(124);
    expect(Date.now() - t0).toBeLessThan(5_000);
  });

  it('the test seams are refused outside a test (the stand-in\'s guard)', () => {
    const saved = process.env.DH_TEST_LOCAL_SANDBOX;
    delete process.env.DH_TEST_LOCAL_SANDBOX;
    try {
      expect(() => sandboxSession({ spec: { vcpus: 2, network: 'allow-all', idleMs: 1, driveSize: 1, budget: { vmSeconds: 1 } }, test: {} })).toThrow(/DH_TEST_LOCAL_SANDBOX/);
    } finally {
      process.env.DH_TEST_LOCAL_SANDBOX = saved;
    }
  });
});

describe('the session factory: the network policy', () => {
  const SECRET = 'Bearer sk-live-0123456789abcdef';
  const brokered = (token = SECRET): SandboxNetworkValue => ({
    allow: {
      'api.github.com': [{ match: { method: ['GET'] }, transform: [{ headers: { authorization: token } }] }],
      'registry.npmjs.org': [],
    },
    subnets: { deny: ['10.0.0.0/8'] },
  });
  const collect = () => {
    const lines: string[] = [];
    return { lines, log: (message: string, fields?: Record<string, unknown>) => void lines.push(`${message} ${JSON.stringify(fields ?? {})}`) };
  };

  it('passes a full policy through on create and re-applies it on every attach to the running VM', async () => {
    const api = fakeApi();
    const acquire = factory(api, { spec: { network: brokered() } });
    const m1 = await track(acquire(invocation({ count: 1, fenced: true }).request));
    expect(api.vms.get(NAME)!.create.network).toEqual(brokered());
    expect(m1.network).toEqual({ allow: ['api.github.com', 'registry.npmjs.org'] });
    await m1.close();
    // Something else changed the VM's policy between turns (an out-of-band update): the next attach puts the descriptor's back.
    api.vms.get(NAME)!.network = 'allow-all';
    api.calls.length = 0;
    await track(acquire(invocation({ count: 2, fenced: true }).request));
    expect(api.calls).toContain(`setNetwork:${NAME}`);
    expect(api.vms.get(NAME)!.network).toEqual(brokered());
  });

  it("calls a callback once per attach with the session, and applies its result at create and at each attach", async () => {
    const api = fakeApi();
    const seen: string[] = [];
    let n = 0;
    const acquire = factory(api, {
      spec: {
        network: async (session) => {
          seen.push(session.sessionId);
          n++;
          return n === 1 ? brokered(`Bearer t${n}`) : { allow: ['example.com'] };
        },
      },
    });
    const first = invocation({ count: 1, fenced: true });
    const m1 = await track(acquire(first.request));
    expect(api.vms.get(NAME)!.create.network).toEqual(brokered('Bearer t1'));
    // The same link, the same invocation: no attach, no call.
    expect(await acquire(first.request)).toBe(m1);
    expect(seen).toEqual([SESSION]);
    await m1.close();
    const m2 = await track(acquire(invocation({ count: 2, fenced: true }).request));
    expect(seen).toEqual([SESSION, SESSION]);
    expect(api.vms.get(NAME)!.network).toEqual({ allow: { 'example.com': [] } });
    expect(m2.network).toEqual({ allow: ['example.com'] });
  });

  it('a callback that throws fails the attach with nothing applied; one that is not fenced is not called', async () => {
    const api = fakeApi();
    let calls = 0;
    let fail = false;
    const acquire = factory(api, {
      spec: {
        network: () => {
          calls++;
          if (fail) throw new Error('token service down');
          return 'deny-all';
        },
      },
    });
    await (await track(acquire(invocation({ count: 1, fenced: true }).request))).close();
    fail = true;
    api.calls.length = 0;
    const e = (await rejection(acquire(invocation({ count: 2, fenced: true }).request))) as Error;
    expect(e.message).toMatch(/sandbox\(\{ network \}\) callback failed/);
    expect((e.cause as Error).message).toBe('token service down');
    expect(api.calls.some((c) => c.startsWith('setNetwork:'))).toBe(false);
    const before = calls;
    // A delivery whose token is not fenced re-applies nothing, so it doesn't ask.
    await rejection(acquire(invocation({ count: 2, fenced: false }).request));
    expect(calls).toBe(before);
  });

  it("never puts a header value in a log line or an error: a refused update's and a refused create's are redacted", async () => {
    const api = fakeApi();
    const l = collect();
    const acquire = factory(api, { spec: { network: () => brokered(), image: undefined }, base: async () => ({ snapshot: 'snap_base' }), log: l.log });
    await (await track(acquire(invocation({ count: 1, fenced: true }).request))).close();
    api.failNetwork = { error: () => Object.assign(new Error(`Status code 400 is not ok: header authorization=${SECRET} is invalid`), { response: { status: 400 }, json: { error: { code: 'bad_request', detail: SECRET } }, text: SECRET }) };
    const e = await rejection(acquire(invocation({ count: 2, fenced: true }).request));
    expect((e as Error).message).toMatch(/header authorization=\[redacted\] is invalid/);
    expect(JSON.stringify(e, Object.getOwnPropertyNames(e))).not.toContain('sk-live');
    expect(e as object).toMatchObject({ response: { status: 400 }, json: { error: { code: 'bad_request' } } });
    // A 5xx is still a retry later.
    api.failNetwork = { error: () => Object.assign(new Error(`upstream echoed ${SECRET}`), { response: { status: 503 } }) };
    expect(((await rejection(acquire(invocation({ count: 3, fenced: true }).request))) as Yield).reason).toBe('contended');
    api.failNetwork = undefined;
    // A create refused (the base gone, which the factory logs) names the value: neither the log nor the error carries it.
    await (await api.getSandbox(NAME))!.delete();
    api.beforeCreateSandbox = () => {
      throw Object.assign(new Error(`Status code 404 is not ok: snapshot gone (policy ${SECRET})`), { response: { status: 404 } });
    };
    const c = await rejection(acquire(invocation({ count: 4, fenced: true }).request));
    expect(String((c as Error).message)).toContain('[redacted]');
    expect(String((c as Error).message)).not.toContain('sk-live');
    expect(l.lines.some((x) => x.startsWith('the base snapshot is gone') && x.includes('[redacted]'))).toBe(true);
    expect(l.lines.join('\n')).not.toContain('sk-live');
  });

  it("a short header value doesn't hide the cap a refused create names: it is read before the error is redacted", async () => {
    // The plan allows 3 h; a transform sets a header to "3", which the refusal's "<= 3h" contains.
    const api = fakeApi({ planCapMs: 3 * 3_600_000 });
    const l = collect();
    const network = { allow: { 'api.example.com': [{ transform: [{ headers: { 'x-api-version': '3' } }] }] } };
    await track(factory(api, { planCapMs: PLAN_CAP_MAX_MS, spec: { network, idleMs: 4 * 60 * 60_000 }, log: l.log })(invocation({ count: 1, fenced: true }).request));
    // The cap the message names, not the 45 min fallback for a 400 it can't read.
    expect(l.lines.find((x) => x.startsWith('learned the plan cap'))).toContain(`"planCapMs":${3 * 3_600_000}`);
    expect(api.vms.get(NAME)!.timeout).toBeGreaterThan(HOBBY_CAP_MS);
    expect(api.calls.filter((c) => c.startsWith('createSandbox'))).toHaveLength(2);
  });

  it("a short header value doesn't hide a gone base snapshot: the base is prepared again, and the error that leaves is still redacted", async () => {
    const api = fakeApi();
    const l = collect();
    let bases = 0;
    // A header value that is a word of the platform's refusal.
    const network = { allow: { 'api.example.com': [{ transform: [{ headers: { 'x-mode': 'snapshot' } }] }] } };
    const acquire = factory(api, { spec: { network, image: undefined }, base: async () => ({ snapshot: `snap_base_${++bases}` }), log: l.log });
    let refused = 0;
    api.beforeCreateSandbox = () => {
      if (refused++ === 0) throw Object.assign(new Error('Status code 404 is not ok: snapshot not found'), { response: { status: 404 } });
    };
    await track(acquire(invocation({ count: 1, fenced: true }).request));
    expect(bases).toBe(2);
    expect(l.lines.some((x) => x.startsWith('the base snapshot is gone'))).toBe(true);
    expect(l.lines.join('\n')).not.toContain('snapshot not found');
  });

  it('maps a full policy: rules and subnets as given, the forced 403s only with * and only for hosts it does not name', () => {
    const env = { VERCEL_URL: 'app.example.com' };
    expect(networkPolicy(brokered(), env)).toEqual(brokered());
    const open = networkPolicy({ allow: { '*': [], 'ai-gateway.vercel.sh': [{ transform: [{ headers: { authorization: SECRET } }] }] } }, env) as { allow: Record<string, unknown[]> };
    expect(open.allow['ai-gateway.vercel.sh']).toEqual([{ transform: [{ headers: { authorization: SECRET } }] }]);
    expect(open.allow['api.vercel.com']).toEqual([{ response: { statusCode: 403 } }]);
    expect(open.allow['app.example.com']).toEqual([{ response: { statusCode: 403 } }]);
    expect(networkPolicy({ allow: ['*', 'github.com'] }, env)).toMatchObject({ allow: { '*': [], 'github.com': [], 'api.vercel.com': [{ response: { statusCode: 403 } }] } });
    expect(networkPolicy({ subnets: { allow: ['10.0.0.0/8'] } }, env)).toEqual({ allow: {}, subnets: { allow: ['10.0.0.0/8'] } });
    expect(policySecrets(networkPolicy(brokered()))).toEqual([SECRET]);
    expect(policySecrets(networkPolicy('allow-all'))).toEqual([]);
    const conflict = redactedError(new ApiConflict('name', `taken ${SECRET}`), [SECRET]);
    expect(conflict).toBeInstanceOf(ApiConflict);
    expect((conflict as Error).message).toBe('taken [redacted]');
    const original = new Error('no secret here');
    expect(redactedError(original, [])).toBe(original);
  });
});

describe('the session factory: pure parts', () => {
  it('names a session\'s sandbox and Drive dh-<20 base32> from the project and the session', () => {
    const a = sessionName('prj_a', 'wrun_1');
    expect(a).toMatch(/^dh-[a-z2-7]{20}$/);
    expect(sessionName('prj_a', 'wrun_1')).toBe(a);
    expect(sessionName('prj_b', 'wrun_1')).not.toBe(a);
    expect(sessionName('prj_a', 'wrun_2')).not.toBe(a);
    expect(a.startsWith('dh-probe-')).toBe(false);
  });

  it('a wildcard short of * passes as written: the forced 403s come only with *', () => {
    const env = { VERCEL_URL: 'app-git-x.vercel.app' };
    const p = (allow: string[] | Record<string, unknown[]>) => (networkPolicy({ allow } as never, env) as { allow: Record<string, unknown[]> }).allow;
    expect(p(['*.vercel.app', 'api.*.example.com'])).toEqual({ '*.vercel.app': [], 'api.*.example.com': [] });
    expect(p({ '*.vercel.sh': [] })).toEqual({ '*.vercel.sh': [] });
  });

  it('collects every value that may be a credential, and its JSON-escaped form', () => {
    const secrets = policySecrets({
      allow: {
        'api.example.com': [{ transform: [{ headers: { authorization: 'tok"en' } }] }],
        'broker.example.com': [{ forwardURL: 'https://u:SECRETURL@proxy.example.com/p' }],
        'blocked.example.com': [{ response: { statusCode: 403, body: 'SECRETBODY', contentType: 'text/plain' } }],
      },
    } as never);
    expect(secrets).toEqual(expect.arrayContaining(['tok"en', 'tok\\"en', 'https://u:SECRETURL@proxy.example.com/p', 'SECRETBODY']));
    const err = redactedError(Object.assign(new Error('400 body {"authorization":"tok\\"en","forwardURL":"https://u:SECRETURL@proxy.example.com/p","body":"SECRETBODY"}'), { status: 400 }), secrets) as Error;
    for (const v of ['tok', 'SECRETURL', 'SECRETBODY']) expect(err.message).not.toContain(v);
  });

  it('maps the network option: allow-all is custom mode with the forced 403s; deny-all and an allow list pass', () => {
    const policy = networkPolicy('allow-all', { VERCEL_URL: 'app-git-x.vercel.app', VERCEL_PROJECT_PRODUCTION_URL: 'app.example.com' }) as { allow: Record<string, unknown[]> };
    expect(policy.allow['*']).toEqual([]);
    for (const h of ['ai-gateway.vercel.sh', 'api.vercel.com', 'app-git-x.vercel.app', 'app.example.com']) expect(policy.allow[h]).toEqual([{ response: { statusCode: 403 } }]);
    expect(networkPolicy('deny-all')).toBe('deny-all');
    expect(networkPolicy({ allow: ['github.com'] })).toEqual({ allow: { 'github.com': [] } });
  });

  it('reads the arm reply strictly: anything off-shape from the VM is no reply', () => {
    const ok = { ok: true, R: 'abc_-1', bootId: 'b', relayId: 'r', sessionId: 's', relayVersion: 'v', cliVersions: { claude: '2.1.289' }, tier: 'privileged', cgroup: true, platformGuard: false };
    expect(parseArmReply(`noise\n${JSON.stringify(ok)}\n`)).toMatchObject({ ok: true, R: 'abc_-1' });
    expect(parseArmReply(JSON.stringify(ok))).toMatchObject({ modelAuth: false });
    for (const bad of [{ ...ok, R: 'a b' }, { ...ok, tier: 'root' }, { ...ok, cgroup: 'yes' }, { ...ok, cliVersions: { x: 1 } }, { ...ok, modelAuth: 'yes' }, { ok: false, code: 'other' }, 'x']) expect(parseArmReply(JSON.stringify(bad))).toBeUndefined();
    expect(parseArmReply('')).toBeUndefined();
    expect(parseArmReply(JSON.stringify({ ok: false, code: 'stale', error: 'x' }))).toMatchObject({ ok: false, code: 'stale' });
  });
});


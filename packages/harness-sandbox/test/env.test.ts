/**
 * `RemoteMachine` and the lazy attach (docs/workspaces.md §4.1, §5.4, §5.6,
 * §11.4 A4) on the stand-in (`localMachine()`): what the shared cases
 * (`machine-conformance.test.ts`) can't say because the in-memory machine has
 * no VM behind it.
 *
 * - a command's environment is the relay's allowlist and its own `env` only,
 *   never this process's; its `env` passes the spawn checks;
 * - without a fenced token nothing that can change the VM is sent, reads
 *   still work;
 * - deadlines (its own, the machine's, the invocation's), cancellation (before
 *   the command started, and while it runs: settled only once it is dead), a
 *   background job, the output cap, and a `KILL` that takes the helper with
 *   the session: each a definite outcome;
 * - `FSYNC` before every mutating call resolves (or once, by hand), and after
 *   one that failed once sent;
 * - a lost link: refused before sending (`EGONE`), or cut off (`EINTR`);
 * - the relay's own gate: an unfenced attach's mutating `ENV_CALL`, `SPAWN`,
 *   `FILES` and `WIPE` are refused there too, sent straight over the link;
 * - a helper that breaks the pipe's rules (`hostile-helper.mjs`), ignores a
 *   `CANCEL`, or never starts (`failing-helper.mjs`): its calls fail, the
 *   link stays up, and the next call gets a new helper;
 * - a walk whose JSON would pass the bound is `E2BIG`, not a dropped link;
 * - the lazy attach: a failed or busy attach is "not run" (a definite
 *   outcome, recorded as the call's result through the kernel), no intent is
 *   written while it attaches, and a drive-stopping error passes through.
 *
 * The privilege cases (the helper as the unprivileged user, a symlink to a
 * root-owned file refused with `EACCES`) need root: they run in the last block
 * as root, and in the live suite (`live-env.test.ts`).
 */
import { existsSync, mkdirSync, readFileSync, symlinkSync, writeFileSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { type DriveContext, durableSessions, EffectFailure, type Harness, QUEUE_PREFIX, type SessionState, Yield } from 'harness-world';
import type { SessionMachine } from 'harness-world-process';
import { LazyRemoteMachine, NotRunError, RemoteMachine, type EnvLink } from '../src/env.ts';
import { EnvCallError } from '../src/relay/dial.ts';
import { localMachine, type LocalSandbox } from './local-machine.ts';
import { codeOf } from './machine-cases.ts';

beforeAll(() => {
  process.env.DH_TEST_LOCAL_SANDBOX = '1';
  delete process.env.VERCEL;
});

const PRIV = process.getuid?.() === 0;
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

const sandboxes: LocalSandbox[] = [];
const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const sb of sandboxes.splice(0)) await sb.dispose().catch(() => {});
  for (const w of worlds.splice(0)) await w.close?.().catch(() => {});
});
async function standIn(options: Parameters<typeof localMachine>[0] = {}): Promise<LocalSandbox> {
  const sb = await localMachine(options);
  sandboxes.push(sb);
  return sb;
}
const fixture = (name: string) => join(dirname(fileURLToPath(import.meta.url)), 'fixtures', name);

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** A machine over `sb`'s link with its own options, and a count of the `FSYNC`s it sent. */
function counted(sb: LocalSandbox, options: Partial<ConstructorParameters<typeof RemoteMachine>[0]> = {}) {
  let fsyncs = 0;
  const link: EnvLink = {
    envCall: (op, args, o) => sb.link.envCall(op, args, o),
    fsync: () => (fsyncs++, sb.link.fsync()),
    isLost: () => sb.link.isLost(),
  };
  const machine = new RemoteMachine({ link, root: sb.config.workspace, fence: { count: 1, fenced: true }, ...options });
  return { machine, fsyncs: () => fsyncs };
}

describe("a command's environment", () => {
  it("is the relay's allowlist plus its own env, never this process's", async () => {
    const sb = await standIn();
    process.env.DH_TEST_CANARY = 'canary-value-in-the-function';
    try {
      const r = await sb.machine.workspace.exec('env', { env: { GIT_AUTHOR_NAME: 'someone' } });
      const vars = Object.fromEntries(r.stdout.trim().split('\n').map((l) => [l.slice(0, l.indexOf('=')), l.slice(l.indexOf('=') + 1)]));
      expect(r.stdout).not.toContain('canary-value-in-the-function');
      // bash adds PWD, SHLVL and _ itself; the relay gives PATH, HOME and TMPDIR (its cache dirs). macOS adds __CF_USER_TEXT_ENCODING to every process it starts.
      const own = ['PATH', 'HOME', 'TMPDIR', 'PWD', 'SHLVL', '_', 'OLDPWD', 'GIT_AUTHOR_NAME', ...(process.platform === 'darwin' ? ['__CF_USER_TEXT_ENCODING'] : [])];
      expect(Object.keys(vars).filter((k) => !own.includes(k))).toEqual([]);
      expect(vars.HOME).toBe(join(sb.config.cacheRoot, 'home'));
      expect(vars.TMPDIR).toBe(join(sb.config.cacheRoot, 'tmp'));
      expect(vars.GIT_AUTHOR_NAME).toBe('someone');
    } finally {
      delete process.env.DH_TEST_CANARY;
    }
  });

  it("passes the spawn checks: a credential name or value, or a value of the Function's own env, is a TypeError naming the key only", async () => {
    const sb = await standIn();
    process.env.DH_TEST_HOST_SECRET = 'a-host-value-of-some-length';
    try {
      for (const env of [{ GITHUB_TOKEN: 'value-of-a-token' }, { NOTE: 'sk-abcdefghijklmnopqrstuvwxyz' }, { COPY: 'a-host-value-of-some-length' }, { LD_PRELOAD: '/x.so' }] as Record<string, string>[]) {
        const err = await sb.machine.workspace.exec('true', { env }).catch((e: Error) => e);
        expect(err).toBeInstanceOf(TypeError);
        expect((err as Error).message).toContain(Object.keys(env)[0]);
        expect((err as Error).message).not.toContain(Object.values(env)[0]);
      }
    } finally {
      delete process.env.DH_TEST_HOST_SECRET;
    }
  });

  it("exempts the CLIs' model key from the name rule only on a spawn, with exactly the attach's key: anything else under the name, and any exec, is refused", async () => {
    const sb = await standIn();
    const key = sb.machine.modelKey;
    for (const name of ['HARNESS_CODEX_GATEWAY_KEY', 'ANTHROPIC_AUTH_TOKEN']) {
      const proc = await sb.machine.spawn({ command: '/bin/sh', args: ['-c', 'true'], env: { [name]: key } });
      await proc.exited;
      // Another value under the name, even one shaped like the key, is a credential-like name.
      for (const value of ['harness-world-codex-0b3f1a52-6f0e-4d8e-9a43-0c1e2b3a4d5f', 'sk-abcdefghijklmnopqrstuvwxyz']) {
        const err = await sb.machine.spawn({ command: '/bin/sh', args: ['-c', 'true'], env: { [name]: value } }).catch((e: Error) => e);
        expect(err).toBeInstanceOf(TypeError);
        expect((err as Error).message).not.toContain(value);
      }
      // A command's exec env is never given the key.
      const err = await sb.machine.workspace.exec('true', { env: { [name]: key } }).catch((e: Error) => e);
      expect(err).toBeInstanceOf(TypeError);
      expect((err as Error).message).not.toContain(key);
    }
    // Another key-like name is still refused, even with the key.
    expect(await sb.machine.spawn({ command: '/bin/sh', args: ['-c', 'true'], env: { HARNESS_OTHER_KEY: key } }).catch((e: Error) => e)).toBeInstanceOf(TypeError);
  });
});

describe('provisioning (SessionMachine.provision)', () => {
  const TOKEN = `ghp_${'a'.repeat(36)}`;
  /** Whether the command saw the credential: its length only, so nothing prints it. */
  const CARRIED = `[ "\${#GITHUB_TOKEN}" -eq ${TOKEN.length} ] && echo carried`;

  it("its commands take a credential in their env, even one of the Function's own; afterwards its workspace refuses everything, and the plain workspace still refuses the credential", async () => {
    const sb = await standIn();
    process.env.DH_TEST_PROVISION_TOKEN = TOKEN;
    try {
      let given: { exec: (command: string) => Promise<unknown> } | undefined;
      const out = await sb.machine.provision!(async (ws) => {
        given = ws;
        return (await ws.exec(CARRIED, { env: { GITHUB_TOKEN: TOKEN } })).stdout;
      });
      expect(out).toBe('carried\n');
      expect(await given!.exec('true').catch((e: Error) => e)).toMatchObject({ code: 'EGONE' });
      const err = await sb.machine.workspace.exec('true', { env: { GITHUB_TOKEN: TOKEN } }).catch((e: Error) => e);
      expect(err).toBeInstanceOf(TypeError);
      expect((err as Error).message).toContain('GITHUB_TOKEN');
      expect((err as Error).message).not.toContain(TOKEN);
    } finally {
      delete process.env.DH_TEST_PROVISION_TOKEN;
    }
  });

  it('is closed once anything ran on the attach: a workspace command, or a spawn', async () => {
    const a = await standIn();
    await a.machine.workspace.exec('true');
    expect(await a.machine.provision!(async () => 'ran').catch((e: Error) => e)).toBeInstanceOf(TypeError);
    const b = await standIn();
    await (await b.machine.spawn({ command: '/bin/sh', args: ['-c', 'true'] })).exited;
    expect(await b.machine.provision!(async () => 'ran').catch((e: Error) => e)).toBeInstanceOf(TypeError);
  });

  it("while it runs nothing else starts, and the structural rules still apply; afterwards the machine is the session's again", async () => {
    const sb = await standIn();
    await sb.machine.provision!(async (ws) => {
      expect(await sb.machine.spawn({ command: '/bin/sh', args: ['-c', 'true'] }).catch((e: Error) => e)).toBeInstanceOf(Error);
      expect(await sb.machine.workspace.exec('true').catch((e: Error) => e)).toBeInstanceOf(Error);
      expect(await sb.machine.provision!(async () => 'nested').catch((e: Error) => e)).toBeInstanceOf(TypeError);
      const err = await ws.exec('true', { env: { LD_PRELOAD: '/x.so' } }).catch((e: Error) => e);
      expect(err).toBeInstanceOf(TypeError);
      expect((err as Error).message).toContain('LD_PRELOAD');
    });
    expect((await sb.machine.workspace.exec('echo ok')).stdout).toBe('ok\n');
  });

  // The degraded stand-in kills by process group, which a command in a session of its own escapes; the privileged tier's cgroup doesn't let it.
  it.skipIf(!PRIV)('a process its commands leave running does not outlive it', async () => {
    const sb = await standIn();
    const pid = await sb.machine.provision!(async (ws) => Number((await ws.exec('setsid sleep 60 </dev/null >/dev/null 2>&1 & echo $!', { env: { GITHUB_TOKEN: TOKEN } })).stdout.trim()));
    expect(pid).toBeGreaterThan(0);
    for (let i = 0; i < 50 && alive(pid); i++) await sleep(100);
    expect(alive(pid)).toBe(false);
  });
});

describe('the fence', () => {
  it('without a fenced token nothing that can change the VM is sent; reads still work', async () => {
    const sb = await standIn();
    writeFileSync(join(sb.config.workspace, 'there.txt'), 'kept');
    for (const fence of [undefined, { count: 1, fenced: false }]) {
      const ws = sb.machineFor(sb.link, fence).workspace;
      const root = ws.root;
      for (const [op, call] of [
        ['exec', () => ws.exec('touch made-by-exec')],
        ['writeFile', () => ws.writeFile(`${root}/w.txt`, 'x')],
        ['mkdir', () => ws.mkdir(`${root}/d`)],
        ['remove', () => ws.remove(`${root}/there.txt`)],
        ['rename', () => ws.rename(`${root}/there.txt`, `${root}/moved.txt`)],
        ['symlink', () => ws.symlink('there.txt', `${root}/l`)],
        ['chmod', () => ws.chmod(`${root}/there.txt`, 0o600)],
      ] as const) {
        const err = await call().catch((e: Error & { code?: string }) => e);
        expect(err, op).toMatchObject({ code: 'EPERM' });
        expect((err as Error).message).toMatch(/fenced token/);
      }
      expect(new TextDecoder().decode(await ws.readFile(`${root}/there.txt`))).toBe('kept');
      expect((await ws.stat(`${root}/there.txt`))?.mode).toBe(0o644);
      expect((await ws.list(root)).map((e) => e.name)).toEqual(['there.txt']);
    }
  });

  it("the relay refuses them too: an unfenced attach's mutating ENV_CALL, SPAWN, FILES and WIPE, sent straight over the link", async () => {
    const sb = await standIn();
    writeFileSync(join(sb.config.workspace, 'there.txt'), 'kept');
    sb.link.close();
    await sleep(300);
    for (const fence of [null, { count: 5, fenced: false }]) {
      const armed = await sb.arm({ fence });
      if (!armed.reply.ok) throw new Error(JSON.stringify(armed.reply));
      const link = await sb.dial({ token: armed.token, nonce: armed.reply.R });
      const path = join(sb.config.workspace, 'w.txt');
      const write = await link.envCall('writeFile', [path, {}], { input: new TextEncoder().encode('x') }).catch((e: unknown) => e);
      expect(write).toBeInstanceOf(EnvCallError);
      expect(write).toMatchObject({ code: 'EPERM' });
      expect(await link.envCall('exec', ['touch made', { cwd: sb.config.workspace }]).catch((e: unknown) => e)).toMatchObject({ code: 'EPERM' });
      await expect(link.spawn({ command: 'touch', args: ['spawned'] })).rejects.toThrow(/fenced token/);
      await expect(link.files('config', new Uint8Array())).rejects.toThrow(/fenced token/);
      await expect(link.wipe()).rejects.toThrow(/fenced token/);
      // Reads are served, and the link is up.
      expect(await link.envCall('stat', [join(sb.config.workspace, 'there.txt')])).toMatchObject({ value: { type: 'file' } });
      expect(link.isLost()).toBe(false);
      expect(['w.txt', 'made', 'spawned'].some((f) => existsSync(join(sb.config.workspace, f)))).toBe(false);
      link.close();
      await sleep(300);
    }
  });
});

describe('a command, stopped', () => {
  it("at the machine's limit: ETIMEDOUT, not an exit code", async () => {
    const sb = await standIn();
    const { machine } = counted(sb, { timeoutMs: 500 });
    expect(await codeOf(machine.exec('sleep 5'))).toBe('ETIMEDOUT');
  });

  it('at the time left in the invocation (§5.6): ETIMEDOUT, saying partial effects may exist; with none left it never runs', async () => {
    const sb = await standIn();
    let left = 400;
    const { machine } = counted(sb, { timeLeft: () => left });
    const err = await machine.exec('touch began; sleep 5').catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'ETIMEDOUT' });
    expect((err as Error).message).toMatch(/ran out of time in this invocation.*partial effects may exist/);
    expect(existsSync(join(sb.config.workspace, 'began'))).toBe(true);
    left = 0;
    const none = await machine.exec('touch never').catch((e: Error & { code?: string }) => e);
    expect(none).toMatchObject({ code: 'ETIMEDOUT' });
    expect(existsSync(join(sb.config.workspace, 'never'))).toBe(false);
  });

  it('when its signal aborts: EINTR, settled only once its processes are gone and the Drive is flushed', async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    let flushed = 0;
    const link: EnvLink = { envCall: (op, args, o) => sb.link.envCall(op, args, o), fsync: async () => void (await sb.link.fsync(), flushed++), isLost: () => sb.link.isLost() };
    const machine = new RemoteMachine({ link, root: ws.root, fence: { count: 1, fenced: true } });
    const controller = new AbortController();
    const run = machine.exec('echo $$ > pid; sleep 30 & echo $! > child; wait', { signal: controller.signal });
    for (let i = 0; i < 200 && !existsSync(join(ws.root, 'child')); i++) await sleep(10);
    await sleep(20);
    const pids = ['pid', 'child'].map((f) => Number(readFileSync(join(ws.root, f), 'utf8').trim()));
    controller.abort();
    expect(await codeOf(run)).toBe('EINTR');
    // No wait: by the time the call settled, the command and its job were dead and FSYNC had answered.
    for (const pid of pids) expect(alive(pid)).toBe(false);
    expect(flushed).toBe(1);
  });

  it('when its signal aborts right after the call was sent: it never runs on', async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    const controller = new AbortController();
    const run = ws.exec('sleep 1; touch marker', { signal: controller.signal });
    controller.abort();
    expect(await codeOf(run)).toBe('EINTR');
    await sleep(1_500);
    expect(existsSync(join(ws.root, 'marker'))).toBe(false);
    expect((await ws.exec('echo next')).stdout).toBe('next\n');
  });

  it("once it exits: a background job it started doesn't outlive the call", async () => {
    const sb = await standIn();
    const r = await sb.machine.workspace.exec('sleep 30 >/dev/null 2>&1 & echo $!');
    await sleep(300);
    expect(alive(Number(r.stdout.trim()))).toBe(false);
  });

  it('when its output passes 10 MiB: EFBIG, and the Drive is flushed (the command ran)', async () => {
    const sb = await standIn();
    const { machine, fsyncs } = counted(sb);
    expect(await codeOf(machine.exec('head -c 11000000 /dev/zero'))).toBe('EFBIG');
    expect(fsyncs()).toBe(1);
    // The helper is still serving.
    expect((await sb.machine.workspace.exec('echo ok')).stdout).toBe('ok\n');
  });

  it('timeouts past what a timer holds are clamped, not fired at once', async () => {
    const sb = await standIn();
    const { machine } = counted(sb, { timeoutMs: 2 ** 40, callTimeoutMs: 2 ** 40 });
    expect((await machine.exec('sleep 0.2; echo ok')).stdout).toBe('ok\n');
    expect((await machine.stat(machine.root))?.type).toBe('directory');
  });

  it("by a KILL of the session (the CLI harness's kill): the call says it may have half-run, and the next call gets a new helper", async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    const run = ws.exec('sleep 30');
    await sleep(500);
    sb.link.killSession('SIGKILL');
    const err = await run.catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'EINTR' });
    expect((err as Error).message).toMatch(/half-run/);
    expect((await ws.exec('echo again')).stdout).toBe('again\n');
  });
});

describe('the Drive is flushed', () => {
  it('flush: each sends FSYNC before every mutating call resolves, never for a read', async () => {
    const sb = await standIn();
    const { machine, fsyncs } = counted(sb);
    const at = (p: string) => `${machine.root}/${p}`;
    await machine.writeFile(at('a.txt'), 'a');
    await machine.mkdir(at('d'));
    await machine.exec('true');
    expect(fsyncs()).toBe(3);
    await machine.readFile(at('a.txt'));
    await machine.stat(at('a.txt'));
    await machine.list(machine.root);
    expect(fsyncs()).toBe(3);
  });

  it('flush: manual leaves it to the runtime (one fsync() before the outcome)', async () => {
    const sb = await standIn();
    const { machine, fsyncs } = counted(sb, { flush: 'manual' });
    await machine.writeFile(`${machine.root}/a.txt`, 'a');
    await machine.exec('echo b > b.txt');
    expect(fsyncs()).toBe(0);
    await machine.fsync();
    expect(fsyncs()).toBe(1);
  });
});

describe('a call the sandbox does not answer in time', () => {
  /** A link that never answers in time: every call rejects as the driver's no-answer timer does. */
  function silent() {
    const seen: Array<{ op: string; timeoutMs?: number }> = [];
    let fsyncs = 0;
    const link: EnvLink = {
      envCall: async (op, _args, o) => {
        seen.push({ op, ...(o?.timeoutMs !== undefined ? { timeoutMs: o.timeoutMs } : {}) });
        throw Object.assign(new Error(`harness-sandbox: ${op} got no answer from the sandbox within ${o?.timeoutMs} ms`), { code: 'ETIMEDOUT' });
      },
      fsync: async () => void fsyncs++,
      isLost: () => false,
    };
    return { link, seen, fsyncs: () => fsyncs };
  }

  it('a mutating one says it may have run, and is flushed; a read just times out', async () => {
    const { link, fsyncs } = silent();
    const machine = new RemoteMachine({ link, root: '/workspace', fence: { count: 1, fenced: true }, callTimeoutMs: 50 });
    const write = await machine.writeFile('/workspace/a', 'a').catch((e: Error & { code?: string }) => e);
    expect(write).toMatchObject({ code: 'ETIMEDOUT' });
    expect((write as Error).message).toMatch(/may have run \(partial effects may exist\)/);
    expect(fsyncs()).toBe(1);
    const read = await machine.readFile('/workspace/a').catch((e: Error & { code?: string }) => e);
    expect(read).toMatchObject({ code: 'ETIMEDOUT' });
    expect((read as Error).message).not.toMatch(/may have run/);
    expect(fsyncs()).toBe(1);
  });

  it("a file op waits no longer than the invocation's time left, and with none left is never sent", async () => {
    const { link, seen } = silent();
    let left = 1_234;
    const machine = new RemoteMachine({ link, root: '/workspace', fence: { count: 1, fenced: true }, timeLeft: () => left });
    await machine.stat('/workspace/a').catch(() => {});
    expect(seen).toEqual([{ op: 'stat', timeoutMs: 1_234 }]);
    left = 0;
    const err = await machine.writeFile('/workspace/a', 'a').catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'ETIMEDOUT' });
    expect((err as Error).message).toMatch(/did not run/);
    expect(seen).toHaveLength(1);
  });
});

describe('a file of 20 MB', () => {
  it('is written and read back whole, in chunks', async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    const big = Uint8Array.from({ length: 20_000_000 }, (_, i) => (i * 7) % 251);
    await ws.writeFile(`${ws.root}/big.bin`, big);
    expect((await ws.stat(`${ws.root}/big.bin`))?.size).toBe(big.length);
    expect(Buffer.from(await ws.readFile(`${ws.root}/big.bin`)).equals(Buffer.from(big))).toBe(true);
  });
});

describe('a lost link', () => {
  it('an operation after the loss is refused before anything is sent (EGONE: it did not run)', async () => {
    const sb = await standIn();
    sb.link.close();
    expect(sb.machine.workspace.lost).toBe(true);
    expect(await codeOf(sb.machine.workspace.exec('touch x'))).toBe('EGONE');
    expect(await codeOf(sb.machine.workspace.readFile(`${sb.machine.root}/x`))).toBe('EGONE');
  });

  it('a command cut off by the loss is EINTR, and says it may have half-run', async () => {
    const sb = await standIn();
    const run = sb.machine.workspace.exec('sleep 30');
    await sleep(300);
    sb.link.close();
    const err = await run.catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'EINTR' });
    expect((err as Error).message).toMatch(/may have half-run/);
  });

  it('its errors carry their POSIX code', async () => {
    const sb = await standIn();
    const err = await sb.machine.workspace.readFile(`${sb.machine.root}/missing`).catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'ENOENT' });
  });
});

describe('a walk past the result bound', () => {
  it('is E2BIG from the helper, not an oversize result that drops the link', async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    // Names of control characters cost six bytes each as JSON: 15,000 paths of about 800 bytes are about 70 MB as a listing.
    const deep = join(ws.root, '\x01'.repeat(250), '\x01'.repeat(250));
    mkdirSync(deep, { recursive: true });
    for (let i = 0; i < 15_000; i++) writeFileSync(join(deep, `${'\x01'.repeat(240)}${i}`), '');
    expect(await codeOf(ws.walk(ws.root))).toBe('E2BIG');
    expect(sb.link.isLost()).toBe(false);
    expect((await ws.exec('echo still')).stdout).toBe('still\n');
  }, 60_000);
});

describe('a helper that breaks the rules', () => {
  it('each broken pipe rule ends that helper (its call EINTR), never the relay or the link; the next call gets a new helper', async () => {
    const sb = await standIn({ relay: { testHelperEntry: fixture('hostile-helper.mjs') } });
    const ws = sb.machine.workspace;
    let pid = (await ws.exec('pid')).stdout;
    for (const mode of ['garbage', 'big-data', 'over-limit', 'unknown-hid', 'big-result', 'bad-type', 'bad-code', 'long-message', 'stray-bytes', 'bad-kind']) {
      const err = await ws.exec(mode).catch((e: Error & { code?: string }) => e);
      expect(err, mode).toMatchObject({ code: 'EINTR' });
      expect((err as Error).message, mode).toMatch(/helper ended while this call ran/);
      expect(sb.link.isLost(), mode).toBe(false);
      const next = (await ws.exec('pid')).stdout;
      expect(next, mode).not.toBe(pid);
      pid = next;
    }
  }, 60_000);

  it('one that ignores a CANCEL is ended within seconds, and the cancelled call settles then', async () => {
    const sb = await standIn({ relay: { testHelperEntry: fixture('hostile-helper.mjs') } });
    const ws = sb.machine.workspace;
    const pid = (await ws.exec('pid')).stdout;
    const controller = new AbortController();
    const run = ws.exec('silent', { signal: controller.signal });
    await sleep(100);
    const at = Date.now();
    controller.abort();
    expect(await codeOf(run)).toBe('EINTR');
    expect(Date.now() - at).toBeGreaterThan(1_500);
    expect(sb.link.isLost()).toBe(false);
    expect((await ws.exec('pid')).stdout).not.toBe(pid);
  });

  it('one that never starts: its calls did not run (EIO), and after three the attach starts no more', async () => {
    const sb = await standIn({ relay: { testHelperEntry: fixture('failing-helper.mjs') } });
    const ws = sb.machine.workspace;
    for (let i = 0; i < 3; i++) {
      const err = await ws.exec('true').catch((e: Error & { code?: string }) => e);
      expect(err).toMatchObject({ code: 'EIO' });
      expect((err as Error).message).toMatch(/did not run: the session helper could not start/);
    }
    const err = await ws.stat(ws.root).catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'EIO' });
    expect((err as Error).message).toMatch(/again and again/);
    expect(readFileSync(join(ws.root, 'spawned'), 'utf8').trim().split('\n')).toHaveLength(3);
    expect(sb.link.isLost()).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// The lazy attach (§5.6)

describe('LazyRemoteMachine', () => {
  it('attaches on first use, once for concurrent calls, and again after the link is lost', async () => {
    const sb = await standIn();
    let acquired = 0;
    let current: SessionMachine = sb.machine;
    const lazy = new LazyRemoteMachine(async () => (acquired++, current));
    expect(lazy.attached).toBe(false);
    await Promise.all([lazy.exec('true'), lazy.stat(`${sb.machine.root}`), lazy.list(sb.machine.root)]);
    expect(acquired).toBe(1);
    sb.link.close();
    expect(lazy.attached).toBe(false);
    // The next attach of the session (the relay admits it: the old sockets are dead).
    const again = await sb.arm({ fence: { count: 2, fenced: true } });
    if (!again.reply.ok) throw new Error(JSON.stringify(again.reply));
    const link2 = await sb.dial({ token: again.token, nonce: again.reply.R });
    current = sb.machineFor(link2, { count: 2, fenced: true });
    expect((await lazy.exec('echo back')).stdout).toBe('back\n');
    expect(acquired).toBe(2);
    link2.close();
  });

  it('a failed or busy attach is NotRunError ("not run"); one that stops the drive passes through', async () => {
    for (const cause of [new Error('the VM did not boot'), new Yield('contended')]) {
      const lazy = new LazyRemoteMachine(() => Promise.reject(cause));
      const err = await lazy.attach().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NotRunError);
      expect(err).toMatchObject({ code: 'ENOTRUN', message: 'not run: the sandbox is starting, try again', cause });
      // Every method takes the same phase first.
      expect(await lazy.exec('true').catch((e: unknown) => e)).toBeInstanceOf(NotRunError);
    }
    const named = (name: string) => Object.assign(new Error(name), { name });
    // `Yield('superseded')` is A3's refusal to arm a fresh VM with an unfenced token: it can't succeed in this drive, so it is not "try again".
    for (const stop of [new Yield('budget'), new Yield('preempted'), new Yield('superseded'), ...['Unavailable', 'Fatal', 'Restart', 'LogFenced', 'LogSuperseded', 'InBandSupersededError', 'Parked', 'EffectInterrupted'].map(named)]) {
      const lazy = new LazyRemoteMachine(() => Promise.reject(stop));
      expect(await lazy.attach().catch((e: unknown) => e)).toBe(stop);
    }
    const controller = new AbortController();
    const reason = new Yield('budget');
    controller.abort(reason);
    const lazy = new LazyRemoteMachine(() => Promise.reject(new Error('busy')), { signal: controller.signal });
    expect(await lazy.attach().catch((e: unknown) => e)).toBe(reason);
  });

  /** A session machine that is only a shape: its workspace, its signal and a count of its closes. */
  function fakeSession(workspace: Partial<RemoteMachine> = {}) {
    const controller = new AbortController();
    let closed = 0;
    const session = { signal: controller.signal, workspace: { lost: false, ...workspace }, close: async () => void closed++ } as unknown as SessionMachine;
    return { session, lose: () => controller.abort(), closed: () => closed };
  }

  it('an acquire that throws at once is NotRunError, and the next attach calls it again', async () => {
    const { session } = fakeSession();
    let calls = 0;
    const lazy = new LazyRemoteMachine((() => {
      calls++;
      if (calls === 1) throw new Error('booting');
      return Promise.resolve(session);
    }) as () => Promise<SessionMachine>);
    expect(await lazy.attach().catch((e: unknown) => e)).toBeInstanceOf(NotRunError);
    expect(await lazy.attach()).toBe(session.workspace);
    expect(calls).toBe(2);
  });

  it('disposed while it attaches: the machine it gets is closed, and the attach is "not run"', async () => {
    const { session, closed } = fakeSession();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const lazy = new LazyRemoteMachine(async () => (await gate, session));
    const attaching = lazy.attach();
    lazy.dispose();
    release();
    expect(await attaching.catch((e: unknown) => e)).toBeInstanceOf(NotRunError);
    await sleep(0);
    expect(closed()).toBe(1);
  });

  it('a lost machine is closed as it is replaced', async () => {
    const first = fakeSession();
    const second = fakeSession();
    const sessions = [first.session, second.session];
    const lazy = new LazyRemoteMachine(async () => sessions.shift()!);
    expect(await lazy.attach()).toBe(first.session.workspace);
    first.lose();
    expect(await lazy.attach()).toBe(second.session.workspace);
    await sleep(0);
    expect(first.closed()).toBe(1);
    expect(second.closed()).toBe(0);
  });

  describe('through the kernel: the attach is a pre-intent phase', () => {
    /**
     * One `bash` call per prompt, the way a Function-tier runtime runs a
     * sandbox call (§5.6): attach first, outside any effect; then the call's
     * effect (`replay: 'never'`): its body runs the command, or, when the attach
     * failed, throws the `NotRunError` it got, so the call's outcome is
     * recorded as "not run".
     */
    function sandboxBash(lazy: LazyRemoteMachine, attaching: () => void = () => {}): Harness {
      return {
        name: 'sandbox-bash',
        async drive(ctx: DriveContext) {
          for (const s of ctx.pending()) {
            let notRun: NotRunError | undefined;
            attaching();
            try {
              await lazy.attach();
            } catch (err) {
              if (!(err instanceof NotRunError)) throw err;
              notRun = err;
            }
            try {
              await ctx.effect(`tool:${s.id}`, { name: 'tool.bash', replay: 'never', input: s.text, placed: [s.id] }, async () => {
                if (notRun) throw notRun;
                return (await lazy.exec(s.text!)).stdout;
              });
            } catch (err) {
              if (!(err instanceof EffectFailure)) throw err;
            }
          }
        },
      };
    }

    async function serve(harness: Harness) {
      const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'dh-a4-')), recoverActiveRuns: false });
      worlds.push(world);
      const s = durableSessions({ world, harness });
      world.registerHandler(QUEUE_PREFIX, s.handler);
      return s;
    }

    const tools = (state: SessionState) => state.effectOrder.filter((e) => e.name === 'tool.bash');
    async function settled(s: Awaited<ReturnType<typeof serve>>, sessionId: string, n: number): Promise<SessionState> {
      for (let i = 0; i < 400; i++) {
        const state = await s.inspect(sessionId);
        const t = tools(state);
        if (t.length >= n && t.every((r) => r.status === 'completed' || r.status === 'failed')) return state;
        await sleep(50);
      }
      throw new Error('the calls never settled');
    }

    it('a failed attach records the call as "not run", a definite outcome (not interrupted), and nothing ran; the next call attaches and runs', async () => {
      const sb = await standIn();
      let fail = true;
      const lazy = new LazyRemoteMachine(async () => {
        if (fail) throw new Yield('contended');
        return sb.machine;
      });
      const s = await serve(sandboxBash(lazy));
      const { sessionId } = await s.start();
      await s.send(sessionId, 'touch ran-1; echo one');
      let state = await settled(s, sessionId, 1);
      expect(tools(state)[0]).toMatchObject({ status: 'failed', error: { message: 'not run: the sandbox is starting, try again' } });
      expect(tools(state)[0]!.error?.interrupted).toBeFalsy();
      expect(existsSync(join(sb.config.workspace, 'ran-1'))).toBe(false);
      fail = false;
      await s.send(sessionId, 'echo two');
      state = await settled(s, sessionId, 2);
      expect(tools(state)[1]).toMatchObject({ status: 'completed', output: 'two\n' });
    });

    it('while it attaches, the call has no intent in the log: a crash then leaves nothing to interrupt', async () => {
      const sb = await standIn();
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      let attachStarted!: () => void;
      const started = new Promise<void>((r) => (attachStarted = r));
      const lazy = new LazyRemoteMachine(async () => {
        attachStarted();
        await gate;
        return sb.machine;
      });
      const s = await serve(sandboxBash(lazy));
      const { sessionId } = await s.start();
      await s.send(sessionId, 'echo attached');
      await started;
      await sleep(200);
      expect(tools(await s.inspect(sessionId))).toEqual([]);
      release();
      const state = await settled(s, sessionId, 1);
      expect(tools(state)).toEqual([expect.objectContaining({ status: 'completed', output: 'attached\n', attempt: 1 })]);
    });
  });
});

// ---------------------------------------------------------------------------
// The privilege split (root only here; the live suite runs them in a VM)

describe.skipIf(!PRIV)('the helper, privileged tier (root)', () => {
  it('runs as the unprivileged user, so a symlink to a root-owned file gets EACCES', async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    expect((await ws.exec('id -un')).stdout.trim()).toBe(sb.config.runAs);
    const owned = join(sb.config.stateDir, '..', 'root-owned');
    mkdirSync(owned, { recursive: true, mode: 0o755 });
    writeFileSync(join(owned, 'relay.mjs'), 'root');
    symlinkSync(join(owned, 'relay.mjs'), join(sb.config.workspace, 'x'));
    expect(await codeOf(ws.writeFile(`${ws.root}/x`, 'pwn'))).toBe('EACCES');
    expect(await codeOf(ws.chmod(`${ws.root}/x`, 0o777))).toBe('EPERM');
  });

  it("doesn't see the CLIs' state dirs (codex-home, config): each is an empty read-only tmpfs in its mount namespace", async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    const codex = join(sb.config.cacheRoot, 'codex-home');
    writeFileSync(join(codex, 'rollout.jsonl'), 'real\n');
    symlinkSync(join(codex, 'rollout.jsonl'), join(sb.config.workspace, 'r'));
    expect((await ws.exec(`ls -A ${codex} ${join(sb.config.cacheRoot, 'config')}`)).stdout.trim().split('\n').filter((l) => l && !l.endsWith(':'))).toEqual([]);
    expect(await codeOf(ws.readFile(join(codex, 'rollout.jsonl')))).toBe('ENOENT');
    expect(await codeOf(ws.readFile(`${ws.root}/r`))).toBe('ENOENT');
    expect(['EROFS', 'EACCES']).toContain(await codeOf(ws.writeFile(join(codex, 'rollout.jsonl'), 'forged', { append: true })));
    expect((await ws.exec(`echo forged >> ${codex}/rollout.jsonl`)).exitCode).not.toBe(0);
    expect(readFileSync(join(codex, 'rollout.jsonl'), 'utf8')).toBe('real\n');
  });

  it("killed outright, it takes its commands with it (its cgroup leaf) before the call's EINTR settles", async () => {
    const sb = await standIn();
    const ws = sb.machine.workspace;
    const run = ws.exec('echo $PPID > helper; echo $$ > pid; exec sleep 30');
    for (let i = 0; i < 200 && !existsSync(join(ws.root, 'pid')); i++) await sleep(10);
    await sleep(20);
    const [helper, pid] = ['helper', 'pid'].map((f) => Number(readFileSync(join(ws.root, f), 'utf8').trim()));
    process.kill(helper!, 'SIGKILL');
    const err = await run.catch((e: Error & { code?: string }) => e);
    expect(err).toMatchObject({ code: 'EINTR' });
    expect((err as Error).message).toMatch(/commands it started were killed/);
    expect(alive(pid!)).toBe(false);
    expect((await ws.exec('echo again')).stdout).toBe('again\n');
  });
});

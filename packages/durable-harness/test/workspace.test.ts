import { describe, expect, it } from 'vitest';
import { isWorkspace, sandbox, SANDBOX_DEFAULTS, virtual, VIRTUAL_DEFAULTS } from '../src/index.ts';

describe('virtual()', () => {
  it('fills the defaults and freezes the descriptor', () => {
    const ws = virtual();
    expect(ws).toEqual({ kind: 'virtual', ...VIRTUAL_DEFAULTS });
    expect(Object.isFrozen(ws)).toBe(true);
    expect(isWorkspace(ws)).toBe(true);
  });

  it('caps maxFileBytes at maxBytes', () => {
    expect(virtual({ durable: false, maxBytes: 2048 })).toMatchObject({ durable: false, maxBytes: 2048, maxFileBytes: 2048 });
  });

  it('refuses bad and unknown options, naming what to do instead', () => {
    expect(() => virtual({ durable: 'no' } as never)).toThrow(/durable.*true or false/);
    expect(() => virtual({ timeoutMs: 10 })).toThrow(/timeoutMs/);
    expect(() => virtual({ store: 'blob' } as never)).toThrow(/planned after launch/);
    expect(() => virtual({ machine: {} } as never)).toThrow(/hybrid/);
  });
});

describe('sandbox()', () => {
  it('fills the defaults and leaves the region to the first invocation', () => {
    const ws = sandbox();
    expect(ws).toEqual({ kind: 'sandbox', env: {}, ...SANDBOX_DEFAULTS });
    expect(ws.region).toBeUndefined();
    expect(ws.driveSize).toBe(10 * 1024 ** 3);
    expect(isWorkspace(ws)).toBe(true);
  });

  it('derives one descriptor from another, keeping credentials, and refuses copies', () => {
    const base = sandbox({ vcpus: 4, credentials: { token: 'SECRET-VALUE', teamId: 'team_1', projectId: 'prj_1' } });
    const derived = sandbox(base, { idleMs: 0 });
    expect(derived).toMatchObject({ vcpus: 4, idleMs: 0 });
    expect(derived.credentials?.projectId).toBe('prj_1');
    expect(() => sandbox({ ...base } as never)).toThrow(/copy of a descriptor/);
    expect(() => sandbox(virtual() as never)).toThrow(/sandbox\(\) descriptor/);
    expect(virtual(virtual({ durable: false }), { timeoutMs: 5_000 })).toMatchObject({ durable: false, timeoutMs: 5_000 });
  });

  it('caps the budget and the timeouts', () => {
    // Decided (docs/workspaces.md §12, question 10): no dollar caps by default, 4 VM-hours per session.
    expect(SANDBOX_DEFAULTS.budget).toEqual({ turnUsd: Infinity, sessionUsd: Infinity, vmSeconds: 4 * 60 * 60 });
    expect(sandbox({ budget: { turnUsd: 1, vmSeconds: Infinity } }).budget).toEqual({ ...SANDBOX_DEFAULTS.budget, turnUsd: 1, vmSeconds: Infinity });
    expect(() => sandbox({ budget: { turnUsd: 0 } })).toThrow(/budget.turnUsd/);
    expect(() => sandbox({ budget: { tokens: 1 } } as never)).toThrow(/budget.tokens/);
    expect(() => sandbox({ idleMs: 24 * 60 * 60_000 })).toThrow(/idleMs/);
    expect(() => sandbox({ commandTimeoutMs: 5 * 60 * 60_000 })).toThrow(/commandTimeoutMs/);
    expect(() => virtual({ maxBytes: 1024 ** 3 })).toThrow(/maxBytes/);
    expect(() => virtual({ timeoutMs: 60 * 60_000 })).toThrow(/timeoutMs/);
  });

  it('keeps credentials off the enumerable properties', () => {
    const ws = sandbox({ credentials: { token: 'SECRET-VALUE', teamId: 'team_1', projectId: 'prj_1' } });
    expect(ws.credentials?.teamId).toBe('team_1');
    expect(JSON.stringify(ws)).not.toContain('SECRET-VALUE');
    expect({ ...ws }).not.toHaveProperty('credentials');
  });

  it('refuses secrets and names the harness sets in env', () => {
    expect(() => sandbox({ env: { GITHUB_TOKEN: 'x' } })).toThrow(/holds no secrets/);
    expect(() => sandbox({ env: { API_KEY: 'x' } })).toThrow(/holds no secrets/);
    expect(() => sandbox({ env: { PATH: '/bin' } })).toThrow(/the harness sets it/);
    expect(() => sandbox({ env: { ANTHROPIC_BASE_URL: 'x' } })).toThrow(/the harness sets it/);
    expect(() => sandbox({ env: { SENTRY_DSN: 'x' } })).toThrow(/holds no secrets/);
    expect(() => sandbox({ env: { GITHUB_PAT: 'x' } })).toThrow(/holds no secrets/);
    expect(() => sandbox({ env: { STRIPE_SK: 'x' } })).toThrow(/holds no secrets/);
    expect(() => sandbox({ env: { DATABASE_URL: 'postgres://u:hunter2@db.example.com/app' } })).toThrow(/URL with a user/);
    expect(() => sandbox({ env: { MODEL_HINT: 'sk-ant-abcdefghijklmnopqrstuvwxyz' } })).toThrow(/sk-/);
    expect(() => sandbox({ env: { X: 'ghp_abcdefghijklmnopqrstuvwxyz0123' } })).toThrow(/GitHub/);
    try {
      sandbox({ env: { DATABASE_URL: 'postgres://u:hunter2@db.example.com/app' } });
    } catch (error) {
      expect(String(error)).not.toContain('hunter2');
    }
    expect(sandbox({ env: { GIT_AUTHOR_NAME: 'a', MONKEY: 'b', TASK_QUEUE: 'q', SITE_URL: 'https://example.com/x@y' } }).env).toEqual({ GIT_AUTHOR_NAME: 'a', MONKEY: 'b', TASK_QUEUE: 'q', SITE_URL: 'https://example.com/x@y' });
  });

  it('validates the rest', () => {
    expect(() => sandbox({ region: 'us-east-1' })).toThrow(/region/);
    expect(() => sandbox({ vcpus: 3 } as never)).toThrow(/1, 2, 4 or 8/);
    expect(() => sandbox({ driveSize: 1024 ** 3 + 1 })).toThrow(/multiple of 4096/);
    expect(() => sandbox({ network: { allow: ['not a domain'] } })).toThrow(/network.allow/);
    expect(sandbox({ network: { allow: ['*.npmjs.org'] } }).network).toEqual({ allow: ['*.npmjs.org'] });
    expect(() => sandbox({ image: { snapshot: '' } })).toThrow(/image/);
    expect(() => sandbox({ persistent: true } as never)).toThrow(/never persistent/);
    expect(() => sandbox({ credentials: { token: '', teamId: 't', projectId: 'p' } })).toThrow(/non-empty token/);
  });

  it('takes image as a function of the session, checking what it returns as a static image is checked', async () => {
    const seen: unknown[] = [];
    const ws = sandbox({
      image: (session) => {
        seen.push(session);
        return session.sessionId === 'wrun_a' ? 'team/base:a' : session.sessionId === 'wrun_s' ? { snapshot: 'snap_1' } : session.sessionId === 'wrun_async' ? Promise.resolve('team/base:async') : ({ snapshot: '' } as never);
      },
    });
    const hook = ws.image as Exclude<typeof ws.image, string | { snapshot: string } | undefined>;
    expect(typeof hook).toBe('function');
    expect(await hook({ sessionId: 'wrun_a', region: 'iad1' })).toBe('team/base:a');
    expect(await hook({ sessionId: 'wrun_s', region: 'iad1' })).toEqual({ snapshot: 'snap_1' });
    expect(await hook({ sessionId: 'wrun_async', region: 'iad1' })).toBe('team/base:async');
    await expect(hook({ sessionId: 'wrun_bad', region: 'iad1' })).rejects.toThrow(/sandbox\(\{ image \}\) hook must return a VCR image name or \{ snapshot: id \}/);
    expect(seen).toEqual([
      { sessionId: 'wrun_a', region: 'iad1' },
      { sessionId: 'wrun_s', region: 'iad1' },
      { sessionId: 'wrun_async', region: 'iad1' },
      { sessionId: 'wrun_bad', region: 'iad1' },
    ]);
    for (const bad of ['', 42, null, { snapshot: 'x', image: 'y' }]) {
      const badHook = sandbox({ image: () => bad as never }).image as typeof hook;
      await expect(badHook({ sessionId: 's', region: 'iad1' })).rejects.toThrow(/image/);
    }
    // A derived descriptor keeps the same checked hook (not wrapped again); a static image is unchanged.
    expect(sandbox(ws, { vcpus: 4 }).image).toBe(hook);
    expect(sandbox({ image: 'team/base:v1' }).image).toBe('team/base:v1');
    expect(sandbox({ image: { snapshot: 'snap_2' } }).image).toEqual({ snapshot: 'snap_2' });
  });

  it('is not fooled by look-alikes', () => {
    expect(isWorkspace({ kind: 'sandbox' })).toBe(false);
    expect(isWorkspace(null)).toBe(false);
  });
});

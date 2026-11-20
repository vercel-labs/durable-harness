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

  describe('network', () => {
    const SECRET = 'Bearer sk-live-0123456789abcdef';
    const policy = {
      allow: {
        'api.github.com': [{ match: { method: ['GET', 'POST'], path: { startsWith: '/repos/' } }, transform: [{ headers: { authorization: SECRET } }] }],
        'proxy.example.com': [{ forwardURL: 'https://broker.example.com/forward' }],
        'docs.example.com': [{ match: { path: { startsWith: '/private/' } }, response: { statusCode: 403, body: 'no', contentType: 'text/plain' } }],
        '*': [],
      },
      subnets: { allow: ['10.0.0.0/8'], deny: ['10.1.0.0/16'] },
    };
    const message = (fn: () => unknown) => {
      try {
        fn();
      } catch (error) {
        return String(error);
      }
      throw new Error('did not throw');
    };

    it('keeps the shorthands as they were', () => {
      expect(sandbox().network).toBe('allow-all');
      expect(sandbox({ network: 'deny-all' }).network).toBe('deny-all');
      expect(sandbox({ network: { allow: ['github.com', '*.npmjs.org'] } }).network).toEqual({ allow: ['github.com', '*.npmjs.org'] });
    });

    it('takes a full NetworkPolicy, copied and frozen', () => {
      const given = structuredClone(policy);
      const ws = sandbox({ network: given });
      expect(ws.network).toEqual(policy);
      given.allow['api.github.com'][0]!.transform![0]!.headers.authorization = 'changed';
      expect(ws.network).toEqual(policy);
      expect(Object.isFrozen((ws.network as typeof policy).allow['api.github.com'][0]!.transform![0]!.headers)).toBe(true);
      expect(sandbox({ network: { subnets: { allow: ['192.168.0.0/16'] } } }).network).toEqual({ subnets: { allow: ['192.168.0.0/16'] } });
    });

    it('takes a whole-label wildcard anywhere, as the firewall does', () => {
      const allow = ['api.*.example.com', '*.example.com', 'example.*', '*.*', 'localhost', '*'];
      expect(sandbox({ network: { allow } }).network).toEqual({ allow });
      expect(sandbox({ network: { allow: { 'api.*.example.com': [] } } }).network).toEqual({ allow: { 'api.*.example.com': [] } });
    });

    it('refuses bad shapes, naming where, never a header value', () => {
      const bad: [unknown, RegExp][] = [
        [{}, /needs allow or subnets/],
        [{ allow: ['github.com'], deny: ['x.com'] }, /no key "deny"/],
        [{ allow: { 'not a domain': [] } }, /network\.allow must list domains/],
        [{ allow: ['*example.com'] }, /network\.allow must list domains/],
        [{ allow: { 'exa*mple.com': [] } }, /network\.allow must list domains/],
        [{ allow: ['example..com'] }, /network\.allow must list domains/],
        [{ allow: { 'github.com': {} } }, /must be a list of rules/],
        [{ allow: { 'github.com': [{ match: { method: ['GET'] } }] } }, /exactly one of transform, forwardURL or response/],
        [{ allow: { 'github.com': [{ transform: [{ headers: { authorization: SECRET } }], response: { statusCode: 403 } }] } }, /exactly one/],
        [{ allow: { 'github.com': [{ transform: [{ headers: { authorization: 42 } }] }] } }, /\["authorization"\] must be a string on one line/],
        [{ allow: { 'github.com': [{ transform: [{ headers: { authorization: `${SECRET}\r\nx-injected: 1` } }] }] } }, /must be a string on one line/],
        [{ allow: { 'github.com': [{ transform: [{ headers: { 'bad name': SECRET } }] }] } }, /not an HTTP header name/],
        [{ allow: { 'github.com': [{ transform: [{ cookies: SECRET }] }] } }, /must be \{ headers/],
        [{ allow: { 'github.com': [{ forwardURL: `http://broker.example.com/?key=${SECRET}` }] } }, /https URL with no query string/],
        [{ allow: { 'github.com': [{ response: { statusCode: 99 } }] } }, /statusCode must be a whole number from 200 to 599/],
        [{ allow: { 'github.com': [{ response: { statusCode: 403, body: SECRET } }] } }, /needs contentType with a body/],
        [{ allow: { 'github.com': [{ response: { statusCode: 403, headers: { 'x-key': 7 } }, match: {} }] } }, /must be a string on one line/],
        [{ allow: { 'github.com': [{ match: { method: ['get'] }, response: { statusCode: 403 } }] } }, /HTTP methods/],
        [{ subnets: { allow: '10.0.0.0/8' } }, /network\.subnets\.allow must be a list of CIDRs/],
        ['allow-some', /must be 'allow-all', 'deny-all'/],
      ];
      for (const [network, rule] of bad) {
        const text = message(() => sandbox({ network: network as never }));
        expect(text).toMatch(rule);
        expect(text).not.toContain('sk-live');
      }
      expect(() => sandbox({ networkPolicy: 'deny-all' } as never)).toThrow(/use network: .*per-session callback/);
    });

    it('takes a per-session callback, and checks what it returns when it is called', async () => {
      const seen: unknown[] = [];
      const ws = sandbox({
        network: async (session) => {
          seen.push(session);
          return session.sessionId === 'wrun_bad' ? ({ allow: { 'github.com': [{ transform: [{ headers: { authorization: 7 } }] }] } } as never) : policy;
        },
      });
      expect(typeof ws.network).toBe('function');
      const resolve = ws.network as (session: { sessionId: string }) => Promise<unknown>;
      expect(await resolve({ sessionId: 'wrun_1' })).toEqual(policy);
      expect(seen).toEqual([{ sessionId: 'wrun_1' }]);
      await expect(resolve({ sessionId: 'wrun_bad' })).rejects.toThrow(/the sandbox\(\{ network \}\) callback's policy: network\.allow\["github\.com"\]\[0\]\.transform\[0\]\.headers\["authorization"\] must be a string/);
      // A derived descriptor keeps the callback, checked once.
      const derived = sandbox(ws, { vcpus: 4 });
      expect(derived.network).toBe(ws.network);
    });
  });

  it('is not fooled by look-alikes', () => {
    expect(isWorkspace({ kind: 'sandbox' })).toBe(false);
    expect(isWorkspace(null)).toBe(false);
  });
});

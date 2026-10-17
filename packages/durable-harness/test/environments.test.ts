import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { Unavailable } from 'harness-world';
import { describe, expect, it } from 'vitest';
import { defaultIsolation, kernelSandboxOf, relayArtifact, resolveEnvironment, sessionActivity, shippedArtifact } from '../src/environments.ts';
import { RELAY_MARKER, SHIPPED_RELAY } from '../src/relay-bundle.ts';
import { deleteSessionSandbox, driveName, prepareSandboxBase, sandboxCleanup, sandbox as sandboxFromEntry, sandboxName } from '../src/sandbox.ts';
import { UNRELEASED } from '../src/unreleased.ts';
import { sandbox, virtual } from '../src/workspace.ts';

/** The io of one tool call, as far as an in-memory workspace reads it: the drive (its scope) and the log's turn. */
function callIO() {
  const state = { effectOrder: [], submissions: [], superseded: new Set() };
  const ctx = { state: () => state };
  return { signal: new AbortController().signal, progress: () => {}, ctx } as never;
}

describe('resolveEnvironment', () => {
  it('virtual() resolves to the workspace runtime its tools run on', async () => {
    const workspace = virtual({ durable: false });
    const env = resolveEnvironment(workspace, { isolation: 'process' });
    expect(env.kind).toBe('virtual');
    expect(env.workspace).toBe(workspace);
    expect(env.runtime.durable).toBe(false);
    expect(env.runtime.root).toBe('/workspace');
    // A call on it: the files and the shell are the workspace's, not this machine's.
    const io = callIO();
    const out = await env.runtime.run(io, 'write', async (machine) => {
      await machine.writeFile('/workspace/note.txt', 'hello');
      return (await machine.exec('cat note.txt; echo; pwd')).stdout;
    });
    expect(out).toBe('hello\n/workspace\n');
    expect(await env.runtime.run(io, 'read', async (machine) => new TextDecoder().decode(await machine.readFile('/workspace/note.txt')))).toBe('hello');
    expect(resolveEnvironment(virtual()).runtime.durable).toBe(true);
  });

  it("on Node with worker threads, a virtual workspace's shell runs in a worker or not at all: it never falls back to this realm", () => {
    expect(defaultIsolation()).toBe('worker');
    expect(resolveEnvironment(virtual()).isolation).toBe('worker');
    // The options still choose (tests).
    expect(resolveEnvironment(virtual(), { isolation: 'auto' }).isolation).toBe('auto');
  });

  it('where there are no worker threads (a browser tab, workerd), auto', () => {
    const g = globalThis as { navigator?: unknown };
    const had = Object.getOwnPropertyDescriptor(g, 'navigator');
    Object.defineProperty(g, 'navigator', { value: { userAgent: 'Cloudflare-Workers' }, configurable: true });
    try {
      expect(defaultIsolation()).toBe('auto');
    } finally {
      if (had) Object.defineProperty(g, 'navigator', had);
      else delete g.navigator;
    }
  });

  it('sandbox() resolves to the sandbox tier\'s session factory, which refuses a session without a fencing token before it touches the Sandbox API', async () => {
    const workspace = sandbox();
    const env = resolveEnvironment(workspace);
    expect(env.kind).toBe('sandbox');
    expect(env.workspace).toBe(workspace);
    // No token (a World without the in-band fence): the relay could not tell a stale driver from the owner.
    const acquiring = env.acquire({ sessionId: 'wrun_1', config: undefined, fence: () => undefined, signal: new AbortController().signal });
    await expect(acquiring).rejects.toBeInstanceOf(Unavailable);
    await expect(acquiring).rejects.toThrow(/in-band writer fence/);
  });

  it("a kernel harness's attach refuses a public route's session the route didn't allow, or one with no finite sessionUsd, before it loads the sandbox tier", async () => {
    const attach = (workspace: ReturnType<typeof sandbox>, config: unknown) => kernelSandboxOf(resolveEnvironment(workspace)).attach({ config } as never, {});
    await expect(attach(sandbox({ budget: { sessionUsd: 2 } }), { route: 'public' })).rejects.toThrow(/without allowPublicSandbox: true/);
    await expect(attach(sandbox(), { route: 'public', allowPublicSandbox: true })).rejects.toThrow(/finite budget\.sessionUsd/);
    // Unavailable: the turn fails at once, the session stays (no redelivery repeats it).
    await expect(attach(sandbox(), { route: 'public' })).rejects.toBeInstanceOf(Unavailable);
  });

  it("sandbox()'s acquire refuses a request without the session's config (a runtime that forgot it), and a public route's session, before it loads the factory", async () => {
    const env = resolveEnvironment(sandbox({ budget: { sessionUsd: 2 } }));
    const request = { sessionId: 'wrun_1', fence: () => undefined, signal: new AbortController().signal };
    await expect(env.acquire(request)).rejects.toThrow(/without config: pass AcquireMachineRequest\.config/);
    await expect(env.acquire(request)).rejects.toBeInstanceOf(TypeError);
    const refused = env.acquire({ ...request, config: { route: 'public', traceAudience: 'public' } });
    await expect(refused).rejects.toBeInstanceOf(Unavailable);
    await expect(refused).rejects.toThrow(/without allowPublicSandbox: true/);
    // Allowed, or not a public route's: on to the factory (which refuses no token, as above).
    await expect(env.acquire({ ...request, config: { route: 'public', allowPublicSandbox: true } })).rejects.toThrow(/in-band writer fence/);
    await expect(env.acquire({ ...request, config: { traceAudience: 'private' } })).rejects.toThrow(/in-band writer fence/);
  });

  it('refuses a copy of a descriptor, and anything that is not one', () => {
    expect(() => resolveEnvironment({ ...virtual() } as never)).toThrow(/copy of a descriptor/);
    expect(() => resolveEnvironment({ ...sandbox() } as never)).toThrow(/copy of a descriptor/);
    expect(() => resolveEnvironment({ durable: true } as never)).toThrow(/takes virtual\(\) or sandbox\(\)/);
    expect(() => resolveEnvironment(undefined as never)).toThrow(TypeError);
  });
});

describe('the /fx and /sandbox entries', () => {
  it('/sandbox re-exports sandbox() and names a session\'s VM and Drive; no stub is left in it', async () => {
    expect(sandboxFromEntry).toBe(sandbox);
    expect(sandboxName('wrun_1', { projectId: 'prj_1' })).toMatch(/^dh-[a-z2-7]{20}$/);
    expect(driveName('wrun_1', { projectId: 'prj_1' })).toBe(sandboxName('wrun_1', { projectId: 'prj_1' }));
    expect(sandboxName('wrun_2', { projectId: 'prj_1' })).not.toBe(sandboxName('wrun_1', { projectId: 'prj_1' }));
    const saved = process.env.VERCEL_PROJECT_ID;
    process.env.VERCEL_PROJECT_ID = 'prj_1';
    try {
      expect(sandboxName('wrun_1')).toBe(sandboxName('wrun_1', { projectId: 'prj_1' }));
    } finally {
      if (saved === undefined) delete process.env.VERCEL_PROJECT_ID;
      else process.env.VERCEL_PROJECT_ID = saved;
    }
    expect(typeof prepareSandboxBase).toBe('function');
    expect(typeof deleteSessionSandbox).toBe('function');
    // A session still open is refused before anything is deleted: it would come back on a new, empty Drive.
    for (const status of ['pending', 'running']) {
      const world = { runs: { get: async () => ({ runId: 'wrun_1', status }) } } as never;
      await expect(deleteSessionSandbox('wrun_1', { world, credentials: { token: 't', teamId: 'team_1', projectId: 'prj_1' } })).rejects.toThrow(new RegExp(`still ${status}; close it first`));
    }
    // Checked before anything is loaded or listed.
    await expect(sandboxCleanup({ idleDays: 0 })).rejects.toBeInstanceOf(TypeError);
    await expect(sandboxCleanup({} as never)).rejects.toThrow(/idleDays/);
    expect(readFileSync(join(import.meta.dirname, '../src/sandbox.ts'), 'utf8')).not.toMatch(/unreleased/);
    // scripts/check-dist.mjs looks for the stubs' text.
    expect(readFileSync(join(import.meta.dirname, '../scripts/check-dist.mjs'), 'utf8')).toContain(`'${UNRELEASED}'`);
  });

  it('/sandbox loads only the names at import (the factory, which claims the brand, and the relay link load on first use); a deployed Function never builds the relay', async () => {
    const src = readFileSync(join(import.meta.dirname, '../src/sandbox.ts'), 'utf8');
    expect(src).not.toMatch(/from 'harness-world-sandbox'/);
    expect(src).toMatch(/from 'harness-world-sandbox\/names'/);
    const names = readFileSync(join(import.meta.dirname, '../../harness-sandbox/src/names.ts'), 'utf8');
    expect([...names.matchAll(/^import .* from '([^']+)'/gm)].map((m) => m[1])).toEqual(['node:crypto', 'node:fs', 'node:path']);
    const saved = process.env.VERCEL_ENV;
    process.env.VERCEL_ENV = 'production';
    try {
      await expect(relayArtifact()).rejects.toBeInstanceOf(Unavailable);
    } finally {
      if (saved === undefined) delete process.env.VERCEL_ENV;
      else process.env.VERCEL_ENV = saved;
    }
  });

  it('the relay bundle: none in the source (the build fills relay-bundle.ts in); a shipped one is checked against its hash', () => {
    expect(SHIPPED_RELAY).toBeUndefined();
    const bytes = new TextEncoder().encode('// a relay\n');
    const version = createHash('sha256').update(bytes).digest('hex');
    const gzip = gzipSync(bytes).toString('base64');
    expect(shippedArtifact({ marker: RELAY_MARKER, version, gzip })).toEqual({ bytes: new Uint8Array(bytes), version });
    expect(() => shippedArtifact({ marker: RELAY_MARKER, version: '0'.repeat(64), gzip })).toThrow(/does not match its hash/);
    // tsup.config.ts fills the module in, and check-dist.mjs finds it by its marker.
    expect(readFileSync(join(import.meta.dirname, '../tsup.config.ts'), 'utf8')).toMatch(/relay-bundle/);
    expect(readFileSync(join(import.meta.dirname, '../scripts/check-dist.mjs'), 'utf8')).toContain(RELAY_MARKER);
  });

  it("sandboxCleanup's activity: by each run's sandbox name, its newest event; a name no run has is unknown", async () => {
    const day = 86_400_000;
    const runs = [
      { runId: 'wrun_a', createdAt: new Date(10 * day), updatedAt: new Date(11 * day) },
      { runId: 'wrun_b', createdAt: new Date(10 * day), updatedAt: new Date(10 * day) },
    ];
    const events: Record<string, { createdAt: Date }[]> = { wrun_a: [{ createdAt: new Date(20 * day) }], wrun_b: [] };
    const pages: unknown[] = [];
    const world = {
      runs: {
        list: async (params: { pagination?: { cursor?: string } }) => {
          pages.push(params);
          return params.pagination?.cursor ? { data: [runs[1]], hasMore: false, cursor: null } : { data: [runs[0]], hasMore: true, cursor: 'next' };
        },
      },
      events: { list: async ({ runId, pagination }: { runId: string; pagination: { limit: number; sortOrder: string } }) => ({ data: pagination.limit === 1 && pagination.sortOrder === 'desc' ? events[runId] : [], hasMore: false, cursor: null }) },
    };
    const activity = await sessionActivity(world as never, 'prj_1');
    expect(pages).toHaveLength(2);
    expect(await activity(sandboxName('wrun_a', { projectId: 'prj_1' }))).toBe(20 * day);
    expect(await activity(sandboxName('wrun_b', { projectId: 'prj_1' }))).toBe(10 * day);
    expect(await activity(sandboxName('wrun_c', { projectId: 'prj_1' }))).toBeUndefined();
  });

  it("libfx's optional peer range admits the libfx the workspace catalog pins", () => {
    const pkg = JSON.parse(readFileSync(join(import.meta.dirname, '../package.json'), 'utf8')) as { peerDependencies: Record<string, string> };
    // The builds with durable sessions: a 0.0.13 dev build at or after 1432, or
    // 0.0.14 (its dev builds too) and up. libfx 0.0.13 itself has no
    // durable-world exports.
    expect(pkg.peerDependencies.libfx).toBe('>=0.0.14 <0.1.0');
    const catalog = /^\s+libfx:\s*(\S+)\s*$/m.exec(readFileSync(join(import.meta.dirname, '../../../pnpm-workspace.yaml'), 'utf8'))?.[1];
    const m = /^0\.0\.(\d+)(?:-dev\.(\d+)(?:\.\S+)?)?$/.exec(catalog ?? '');
    expect(m, `catalog libfx ${catalog}`).not.toBeNull();
    const patch = Number(m![1]);
    if (patch === 13) expect(m![2] !== undefined && Number(m![2]) >= 1432, `catalog libfx ${catalog}`).toBe(true);
    else expect(patch, `catalog libfx ${catalog}`).toBeGreaterThanOrEqual(14);
  });
});

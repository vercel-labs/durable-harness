import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import { registerStorageConformance } from '@earendil-works/pi-durable/testing';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { Yield } from 'harness-world';
import { ulid } from 'ulid';
import type { AcquireMachine, AcquireMachineRequest } from 'harness-world-process';
import { afterAll, describe, expect, it } from 'vitest';
import { worldStorage } from './index.ts';
import { NotRunError, sandboxSessionOf } from './sandbox.ts';
import { WorldStorage } from './storage.ts';

const worlds: LocalWorld[] = [];
afterAll(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

let world: LocalWorld | undefined;
async function sharedWorld(): Promise<LocalWorld> {
  if (!world) {
    world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'pi-world-')), recoverActiveRuns: false });
    worlds.push(world);
  }
  return world;
}

// Pi's own Storage conformance suite, run against a Workflow World. Each case
// gets a fresh session (run) on one world-local data directory.
registerStorageConformance({ describe, expect, it }, 'worldStorage on world-local', async (use) => {
  const w = await sharedWorld();
  const storage = await worldStorage(w, await worldStorage.create(w));
  try {
    await use(storage);
  } finally {
    await storage.close(BACKGROUND_CONTEXT).catch(() => {});
  }
});

// The same cases again, and afterwards a second process reopens the session
// from the log and must read back exactly what the first one holds.
registerStorageConformance({ describe, expect, it }, 'worldStorage, replayed after every case', async (use) => {
  const w = await sharedWorld();
  const id = await worldStorage.create(w);
  const storage = await worldStorage(w, id);
  await use(storage);
  const before = await dump(storage).catch(() => undefined); // Some cases close the storage themselves.
  await storage.close(BACKGROUND_CONTEXT).catch(() => {});
  const reopened = await worldStorage(w, id);
  try {
    if (before) expect(await dump(reopened)).toEqual(before);
  } finally {
    await reopened.close(BACKGROUND_CONTEXT);
  }
});

async function dump(storage: Awaited<ReturnType<typeof worldStorage>>) {
  const c = BACKGROUND_CONTEXT;
  return {
    conversations: (await storage.scanConversations({}, 10_000, undefined, c)).items,
    tasks: (await storage.scanTasks({}, 10_000, undefined, c)).items,
    submissions: (await storage.scanSubmissions({}, 10_000, undefined, c)).items,
  };
}

describe("the session's recorded config", () => {
  it('is recorded on run_created with the trace audience, and every storage of the session reads it back, frozen', async () => {
    const w = await sharedWorld();
    const id = await WorldStorage.create(w, { traceAudience: 'public', config: { route: 'public', allowPublicSandbox: true } });
    const storage = await worldStorage(w, id);
    const reopened = await worldStorage(w, id, { traceAudience: 'private' });
    try {
      for (const s of [storage, reopened]) {
        expect(s.config).toEqual({ route: 'public', allowPublicSandbox: true, traceAudience: 'public' });
        expect(Object.isFrozen(s.config)).toBe(true);
        expect(() => {
          (s.config as Record<string, unknown>).route = 'private';
        }).toThrow(TypeError);
      }
    } finally {
      await storage.close(BACKGROUND_CONTEXT).catch(() => {});
      await reopened.close(BACKGROUND_CONTEXT).catch(() => {});
    }
  });

  it("create mints the session's id with the World's createRunId when it has one", async () => {
    const w = await sharedWorld();
    const minted = ulid();
    expect(await worldStorage.create(w)).toMatch(/^wrun_[0-9A-HJKMNP-TV-Z]{26}$/);
    Object.assign(w, { createRunId: () => minted });
    try {
      expect(await worldStorage.create(w)).toBe(`wrun_${minted}`);
    } finally {
      delete (w as { createRunId?: unknown }).createRunId;
    }
  });

  it('an open that creates the session records it too; a session that recorded none has none', async () => {
    const w = await sharedWorld();
    const created = await worldStorage(w, `wrun_${ulid()}`, { config: { route: 'public' } });
    const plain = await worldStorage(w, await worldStorage.create(w));
    try {
      expect(created.config).toEqual({ route: 'public' });
      expect((await worldStorage(w, created.sessionId)).config).toEqual({ route: 'public' });
      // An existing session keeps what it recorded: config given to a later open is not written.
      expect((await worldStorage(w, plain.sessionId, { config: { route: 'public' } })).config).toBeUndefined();
      expect(plain.config).toBeUndefined();
    } finally {
      await created.close(BACKGROUND_CONTEXT).catch(() => {});
      await plain.close(BACKGROUND_CONTEXT).catch(() => {});
    }
  });

  it("the sandbox attach hands the factory the session's recorded config, with every acquire", async () => {
    const w = await sharedWorld();
    const storage = await worldStorage(w, await WorldStorage.create(w, { config: { route: 'public' } }));
    const requests: AcquireMachineRequest[] = [];
    const acquire: AcquireMachine = async (request) => {
      requests.push(request);
      throw new Yield('contended', 1);
    };
    try {
      await expect(sandboxSessionOf(storage, acquire).attach()).rejects.toBeInstanceOf(NotRunError);
      expect(requests).toHaveLength(1);
      expect(requests[0]!.config).toBe(storage.config);
      expect(requests[0]!.config).toEqual({ route: 'public' });
    } finally {
      await storage.close(BACKGROUND_CONTEXT).catch(() => {});
    }
  });
});

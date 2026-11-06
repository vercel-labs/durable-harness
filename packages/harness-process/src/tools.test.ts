import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { defineDoc, type DriveContext, durableSessions, type Harness, type HostTool, readDoc, Yield } from 'harness-world';
import type { WorkspaceMachine } from 'harness-world-hands/runtime';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { placeSubmissions } from './inbox.ts';
import { failureText, toolBridge } from './tools.ts';
import { zodFromJsonSchema, zodShapeFromJsonSchema } from './zod.ts';

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

async function drive(body: (ctx: DriveContext) => Promise<void>) {
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hwp-tools-')), recoverActiveRuns: false });
  worlds.push(world);
  const harness: Harness = {
    name: 'bridged',
    async drive(ctx) {
      if (ctx.pending().length === 0) return;
      await placeSubmissions(ctx, ctx.pending());
      await body(ctx);
    },
  };
  const sessions = durableSessions({ world, harness });
  const { sessionId } = await sessions.start();
  await sessions.send(sessionId, 'go', 'prompt', { wake: 'backstop' });
  const result = sessions.drive(sessionId);
  return { result, inspect: async () => (await result.catch(() => {}), sessions.inspect(sessionId)) };
}

const counter = (name: string, replay: 'safe' | 'never', runs: string[], delayMs = 0): HostTool<{ x: number }> => ({
  name,
  description: `${name} tool`,
  inputSchema: { type: 'object', properties: { x: { type: 'number' } }, required: ['x'] },
  replay,
  async execute({ x }) {
    runs.push(`${name}:${x}`);
    if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
    return `${name} got ${x}`;
  },
});

describe('toolBridge', () => {
  it('keys each call by the harness call id: a repeated id answers from the log', async () => {
    const runs: string[] = [];
    const { inspect } = await drive(async (ctx) => {
      const bridge = toolBridge(ctx, [counter('add', 'safe', runs)]);
      expect(bridge.tools.map((t) => [t.name, t.replay])).toEqual([['add', 'safe']]);
      expect(await bridge.call('add', { x: 1 }, 'c1')).toEqual({ text: 'add got 1', isError: false });
      // A resumed harness re-issuing the same call gets the recorded result.
      expect(await bridge.call('add', { x: 1 }, 'c1')).toEqual({ text: 'add got 1', isError: false });
      expect(await bridge.call('nope', {}, 'c2')).toEqual({ text: 'unknown tool: nope', isError: true });
      await expect(bridge.call('add', { x: 2 }, '')).rejects.toThrow(/no call id/);
    });
    const state = await inspect();
    expect(runs).toEqual(['add:1']);
    expect(state.effects.get('tool:c1')).toMatchObject({ name: 'tool.add', status: 'completed', output: 'add got 1' });
  });

  it("passes the effect's io through: a HostTool edits a document with its outcome", async () => {
    const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
    const add: HostTool<{ item: string }> = {
      name: 'add_todo',
      description: 'Add a todo.',
      inputSchema: { type: 'object', properties: { item: { type: 'string' } }, required: ['item'] },
      replay: 'safe',
      async execute({ item }, io) {
        expect(io.stepId).toMatch(/^step_/);
        const todos = io.docs!.update(Todos, (d) => void d.items.push(item));
        return `added; ${todos.items.length} todos`;
      },
    };
    const { inspect } = await drive(async (ctx) => {
      const bridge = toolBridge(ctx, [add]);
      expect(await bridge.call('add_todo', { item: 'milk' }, 'c1')).toEqual({ text: 'added; 1 todos', isError: false });
      expect(ctx.docs!.read(Todos).value.items).toEqual(['milk']);
    });
    const state = await inspect();
    expect(readDoc(state, Todos)).toEqual({ value: { items: ['milk'] }, revision: 1 });
    expect(state.effects.get('tool:c1')).toMatchObject({ output: 'added; 1 todos', docs: [{ kind: 'app.todos', revision: 1 }] });
  });

  it('runs the barrier before a never tool, and serializes calls', async () => {
    const runs: string[] = [];
    const { inspect } = await drive(async (ctx) => {
      const bridge = toolBridge(ctx, [counter('pay', 'never', runs, 30), counter('read', 'safe', runs, 30)], {
        beforeNever: async ({ callId }) => {
          await ctx.effect(`barrier:${callId}`, { name: 'barrier', replay: 'safe', input: callId }, async () => true);
        },
      });
      await Promise.all([bridge.call('pay', { x: 1 }, 'p1'), bridge.call('read', { x: 2 }, 'r1'), bridge.call('pay', { x: 3 }, 'p2')]);
    });
    const names = (await inspect()).effectOrder.map((e) => e.key);
    expect(names).toEqual([expect.stringMatching(/^place:/), 'barrier:p1', 'tool:p1', 'tool:r1', 'barrier:p2', 'tool:p2']);
    expect(runs).toEqual(['pay:1', 'read:2', 'pay:3']);
  });

  it('a failing tool is an error result; a stopped drive halts and rethrows', async () => {
    const failing: HostTool = { name: 'boom', description: '', inputSchema: {}, replay: 'safe', execute: async () => Promise.reject(new Error('no disk')) };
    const { result } = await drive(async (ctx) => {
      const bridge = toolBridge(ctx, [failing]);
      expect(await bridge.call('boom', {}, 'b1')).toEqual({ text: 'no disk', isError: true });
      const stopping: HostTool = { name: 'stop', description: '', inputSchema: {}, replay: 'safe', execute: async () => Promise.reject(new Yield('contended', 1)) };
      await expect(toolBridge(ctx, [stopping]).call('stop', {}, 's1')).rejects.toBeInstanceOf(Yield);
      expect(ctx.signal.aborted).toBe(true);
    });
    // The drive gives its delivery up through a continuation, never by asking the queue to come back.
    await expect(result).resolves.toBeUndefined();
  });

  it("gives each call the session's workspace when the harness has one, and none otherwise", async () => {
    const seen: unknown[] = [];
    const probe: HostTool = { name: 'probe', description: '', inputSchema: {}, replay: 'safe', execute: async (_input, io) => (seen.push(io.workspace), 'ok') };
    const workspace = { root: '/workspace' } as unknown as WorkspaceMachine;
    const { inspect } = await drive(async (ctx) => {
      expect(await toolBridge(ctx, [probe], { workspace }).call('probe', {}, 'w1')).toEqual({ text: 'ok', isError: false });
      expect(await toolBridge(ctx, [probe]).call('probe', {}, 'w2')).toEqual({ text: 'ok', isError: false });
    });
    await inspect();
    expect(seen).toHaveLength(2);
    expect(seen[0]).toBe(workspace);
    expect(seen[1]).toBeUndefined();
  });

  it('describes an interrupted call with its last progress', () => {
    expect(failureText({ message: 'interrupted: it may or may not have taken effect', interrupted: true, partial: [{ a: 1 }, 'tail'] })).toBe(
      'interrupted: it may or may not have taken effect\nPartial output before the interruption:\ntail'
    );
    expect(failureText({ message: 'plain' })).toBe('plain');
  });
});

describe('zod from JSON Schema', () => {
  const schema = {
    type: 'object',
    properties: {
      path: { type: 'string', description: 'where' },
      mode: { enum: ['a', 'b'] },
      lines: { type: 'array', items: { type: 'integer' } },
      opts: { type: 'object', properties: { deep: { type: ['boolean', 'null'] } }, additionalProperties: false },
    },
    required: ['path'],
  };
  it('converts objects, enums, arrays and nullables', () => {
    const t = zodFromJsonSchema(schema, z);
    expect(t.parse({ path: 'x', mode: 'a', lines: [1, 2], opts: { deep: null }, extra: 1 })).toEqual({ path: 'x', mode: 'a', lines: [1, 2], opts: { deep: null }, extra: 1 });
    expect(() => t.parse({ mode: 'a' })).toThrow();
    expect(() => t.parse({ path: 'x', mode: 'c' })).toThrow();
    expect(() => t.parse({ path: 'x', opts: { deep: true, nope: 1 } })).toThrow();
    const shape = zodShapeFromJsonSchema(schema, z);
    expect(Object.keys(shape)).toEqual(['path', 'mode', 'lines', 'opts']);
    expect(shape.path.description).toBe('where');
  });
});

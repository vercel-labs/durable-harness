import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT as ctx } from '@earendil-works/chord/context';
import { Type } from '@earendil-works/pi-ai';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness, type Storage } from '@earendil-works/pi-durable';
import type { World } from '@workflow/world';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { untilSettled } from 'pi-world';
import { afterAll, describe, expect, it } from 'vitest';
import { chatRoute, openWorldReader, openWorldStorage } from '../src/pi-durable.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function until<T>(read: () => T | Promise<T>, test: (value: T) => boolean): Promise<T> {
  const deadline = Date.now() + 15_000;
  for (;;) {
    const value = await read();
    if (test(value)) return value;
    if (Date.now() > deadline) throw new Error('timed out');
    await sleep(10);
  }
}

function app(text: string, tokensPerSecond: number, before?: Promise<void>) {
  const faux = fauxProvider({ tokensPerSecond, tokenSize: { min: 1, max: 1 } });
  faux.setResponses([async () => { await before; return fauxAssistantMessage(text); }]);
  const models = createModels();
  models.setProvider(faux.provider);
  const model = faux.getModel();
  return {
    agent: { model: { provider: model.provider, modelId: model.id } },
    open: (storage: Storage) => Harness.open(storage, { models, registry: createRegistry() }, ctx),
  };
}

type Record = { event?: string; data?: { type: string; delta?: string } };
function collect(response: Response) {
  const records: Record[] = [];
  const ended = (async () => {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let pending = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      pending += decoder.decode(value, { stream: true });
      let end: number;
      while ((end = pending.indexOf('\n\n')) !== -1) {
        const block = pending.slice(0, end);
        pending = pending.slice(end + 2);
        const record: Record = {};
        for (const line of block.split('\n')) {
          if (line.startsWith('data: ')) record.data = JSON.parse(line.slice(6));
          if (line.startsWith('event: ')) record.event = line.slice(7);
        }
        if (record.data || record.event) records.push(record);
      }
    }
    return records;
  })();
  return { records, ended };
}

function displayed(records: Record[]) {
  let text = '';
  for (const r of records) {
    if (r.data?.type === 'rewind') text = '';
    if (r.data?.type === 'text') text += r.data.delta;
  }
  return text;
}

/** Hold the route's polls after GET's initial snapshot and first streamed snapshot. */
function pollingWorld(world: World, after = 2) {
  let reads = 0;
  let pauseAfter = after;
  let release!: () => void;
  const blocked = new Promise<void>((r) => (release = r));
  return {
    release,
    pause: () => { pauseAfter = reads; },
    world: {
      ...world,
      events: {
        ...world.events,
        list: async (...args: Parameters<World['events']['list']>) => {
          if (++reads > pauseAfter) await blocked;
          return world.events.list(...args);
        },
      },
    } as World,
  };
}

describe('Pi-durable chat stream recovery', () => {
  const worlds: LocalWorld[] = [];
  afterAll(async () => {
    for (const world of worlds) await world.close?.();
  });

  for (const recovery of ['completed between polls', 'still streaming', 'waiting for the first replacement token'] as const) {
    it(`replaces an interrupted partial when recovery is ${recovery}`, async () => {
      const world = createWorld({ dataDir: mkdtempSync(join(tmpdir(), 'dh-pi-stream-')), recoverActiveRuns: false });
      worlds.push(world);
      const first = app('Same prefix, abandoned answer.', 15);
      const initial = await openWorldStorage(undefined, { world, name: `stream-${recovery}` });
      const old = await first.open(initial);
      await (await old.root(ctx, { agent: first.agent })).submit({ type: 'input', content: 'Answer.' }, ctx);
      const journal = openWorldReader(initial.sessionId, { world });
      const snapshot = () => journal.refresh().then(() => journal.snapshot());
      await until(snapshot, (s) => !!s.live?.generation?.message?.content.length);

      const polls = pollingWorld(world);
      const route = chatRoute({ open: first.open, world: polls.world, authorize: 'public' });
      const followed = collect(await route.GET(new Request(`http://local/api/chat?sessionId=${initial.sessionId}`)));
      await until(() => followed.records, (records) => records.some((r) => r.data?.type === 'text'));
      // Stop exactly where a killed writer leaves the persisted generation partial.
      // abandon() prevents every further commit and writes no graceful-close entry.
      initial.abandon();
      const answer = 'Same prefix, replacement answer.';
      let startReplacement!: () => void;
      const waiting = new Promise<void>((r) => (startReplacement = r));
      const second = app(answer, recovery === 'still streaming' ? 15 : 10000, recovery === 'waiting for the first replacement token' ? waiting : undefined);
      const replacement = await openWorldStorage(initial.sessionId, { world, name: `stream-${recovery}` });
      const resumed = await second.open(replacement);
      await resumed.resume();
      if (recovery === 'completed between polls') {
        await untilSettled(resumed);
      } else if (recovery === 'still streaming') {
        await until(snapshot, (s) => s.entries.some((e) => JSON.stringify(e.model).includes('aborted')) && !!s.live?.generation?.message?.content.length);
      } else {
        await until(snapshot, (s) => s.entries.some((e) => JSON.stringify(e.model).includes('aborted')) && !!s.live?.generation && !s.live.generation.message);
      }
      polls.release();
      if (recovery === 'waiting for the first replacement token') {
        await until(() => followed.records, (records) => records.some((r) => r.data?.type === 'rewind'));
        startReplacement();
      }
      const records = await followed.ended;
      await untilSettled(resumed);
      await resumed.close(ctx);
      const final = await snapshot();
      expect(final.unfinished).toBe(false);
      expect(final.entries.filter((e) => e.kind === 'pi.assistant')).toHaveLength(2);
      expect(displayed(records)).toBe(answer);
      expect(records.some((r) => r.data?.type === 'rewind')).toBe(true);
      expect(records.at(-2)?.data).toEqual({ type: 'idle' });
      expect(records.at(-1)?.event).toBe('end');
    });
  }

  it('preserves the stored partial when a turn is explicitly aborted without a replacement', async () => {
    const world = createWorld({ dataDir: mkdtempSync(join(tmpdir(), 'dh-pi-stream-abort-')), recoverActiveRuns: false });
    worlds.push(world);
    const first = app('Same prefix, a cancelled response that should remain visible.', 15);
    const storage = await openWorldStorage(undefined, { world });
    const harness = await first.open(storage);
    const root = await harness.root(ctx, { agent: first.agent });
    await root.submit({ type: 'input', content: 'Answer.' }, ctx);
    const journal = openWorldReader(storage.sessionId, { world });
    const snapshot = () => journal.refresh().then(() => journal.snapshot());
    await until(snapshot, (s) => !!s.live?.generation?.message?.content.length);
    const polls = pollingWorld(world);
    const route = chatRoute({ open: first.open, world: polls.world, authorize: 'public' });
    const followed = collect(await route.GET(new Request(`http://local/api/chat?sessionId=${storage.sessionId}`)));
    await until(() => followed.records, (records) => records.some((r) => r.data?.type === 'text'));
    await root.abort(ctx);
    polls.release();
    const records = await followed.ended;
    const final = await snapshot();
    await harness.close(ctx);
    const message = final.entries.find((e) => e.kind === 'pi.assistant')?.model?.[0] as { stopReason: string; content: { type: string; text?: string }[] };
    expect(message.stopReason).toBe('aborted');
    expect(displayed(records)).toBe(message.content.filter((c) => c.type === 'text').map((c) => c.text).join(''));
    expect(displayed(records).length).toBeGreaterThan(0);
    expect(records.some((r) => r.data?.type === 'rewind')).toBe(false);
    expect(records.at(-1)?.event).toBe('end');
  });

  it('keeps a completed tool round when a later model call is interrupted', async () => {
    const world = createWorld({ dataDir: mkdtempSync(join(tmpdir(), 'dh-pi-stream-round-')), recoverActiveRuns: false });
    worlds.push(world);
    const prefix = 'Looking. ';
    let releaseTool!: () => void;
    const toolGate = new Promise<void>((r) => (releaseTool = r));
    const faux = fauxProvider({ tokensPerSecond: 15, tokenSize: { min: 1, max: 1 } });
    faux.setResponses([
      fauxAssistantMessage([fauxText(prefix), fauxToolCall('lookup', {})], { stopReason: 'toolUse' }),
      fauxAssistantMessage('Same prefix, abandoned answer.'),
    ]);
    const models = createModels();
    models.setProvider(faux.provider);
    const registry = createRegistry();
    registry.install(defineExtension({ name: 'test', tools: [defineTool({
      name: 'lookup', description: 'Look up.', parameters: Type.Object({}), replay: 'safe',
      execute: async () => { await toolGate; return { content: [{ type: 'text', text: 'result' }] }; },
    })] }));
    const model = faux.getModel();
    const initial = await openWorldStorage(undefined, { world });
    const old = await Harness.open(initial, { models, registry }, ctx);
    await (await old.root(ctx, { agent: { model: { provider: model.provider, modelId: model.id } } })).submit({ type: 'input', content: 'Look.' }, ctx);
    const polls = pollingWorld(world, Infinity);
    const route = chatRoute({ open: (s) => Harness.open(s, { models, registry }, ctx), world: polls.world, authorize: 'public' });
    const followed = collect(await route.GET(new Request(`http://local/api/chat?sessionId=${initial.sessionId}`)));
    await until(() => followed.records, (records) => records.some((r) => r.data?.type === 'tool_start'));
    const boundary = followed.records.length;
    expect(displayed(followed.records)).toBe(prefix);
    releaseTool();
    await until(() => followed.records.slice(boundary), (records) => records.some((r) => r.data?.type === 'text'));
    polls.pause();
    initial.abandon();
    const answer = 'Same prefix, replacement answer.';
    const second = app(answer, 10000);
    const resumed = await second.open(await openWorldStorage(initial.sessionId, { world }));
    await resumed.resume();
    await untilSettled(resumed);
    polls.release();
    const records = await followed.ended;
    await resumed.close(ctx);
    // Rewind drops the current model call, as chatRoute documents; it must
    // neither resend nor remove text that already completed before the tool.
    expect(prefix + displayed(records.slice(boundary))).toBe(prefix + answer);
    expect(records.filter((r) => r.data?.type === 'rewind')).toHaveLength(1);
    expect(records.filter((r) => r.data?.type === 'tool_start')).toHaveLength(1);
    expect(records.filter((r) => r.data?.type === 'tool_end')).toHaveLength(1);
    expect(records.at(-1)?.event).toBe('end');
  });

  it('streams an uninterrupted turn with a tool round once, without rewinds', async () => {
    const world = createWorld({ dataDir: mkdtempSync(join(tmpdir(), 'dh-pi-stream-control-')), recoverActiveRuns: false });
    worlds.push(world);
    const faux = fauxProvider({ tokensPerSecond: 10000 });
    faux.setResponses([
      fauxAssistantMessage([fauxText('Looking. '), fauxToolCall('lookup', {})], { stopReason: 'toolUse' }),
      fauxAssistantMessage('Found it.'),
    ]);
    const models = createModels();
    models.setProvider(faux.provider);
    const registry = createRegistry();
    registry.install(defineExtension({ name: 'test', tools: [defineTool({
      name: 'lookup', description: 'Look up.', parameters: Type.Object({}), replay: 'safe',
      execute: async () => ({ content: [{ type: 'text', text: 'result' }] }),
    })] }));
    const model = faux.getModel();
    const route = chatRoute({
      open: (storage) => Harness.open(storage, { models, registry }, ctx),
      agent: { model: { provider: model.provider, modelId: model.id } },
      world, authorize: 'public',
    });
    const response = await route.POST(new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify({ text: 'Look.' }), headers: { 'content-type': 'application/json' } }));
    const records = await collect(response).ended;
    expect(displayed(records)).toBe('Looking. Found it.');
    expect(records.some((r) => r.data?.type === 'rewind')).toBe(false);
    expect(records.filter((r) => r.data?.type === 'tool_start')).toHaveLength(1);
    expect(records.filter((r) => r.data?.type === 'tool_end')).toHaveLength(1);
    expect(records.at(-1)?.event).toBe('end');
  });
});

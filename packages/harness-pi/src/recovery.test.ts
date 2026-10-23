import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createAssistantMessageEventStream, createModels } from '@earendil-works/pi-ai';
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { chatRoute, durableSessions, type Harness, type HostTool, liveResponse, QUEUE_PREFIX, Unavailable, vercelAnchors } from 'harness-world';
import { leasedWorld } from 'harness-world/testing';
import { afterEach, describe, expect, it } from 'vitest';
import { piHarness, type PiHarnessOptions } from './index.ts';
import { Type } from 'typebox';

type LiveEvent = { type: string; text?: string; delta?: string; parts?: string[] };
const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const world of worlds.splice(0)) await world.close?.();
});

async function until<T>(read: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const result = await read();
    if (result !== undefined) return result;
    if (Date.now() > deadline) throw new Error('timed out');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function model(responses: Parameters<ReturnType<typeof fauxProvider>['setResponses']>[0]) {
  const faux = fauxProvider();
  faux.setResponses(responses);
  const models = createModels();
  models.setProvider(faux.provider);
  return { model: faux.getModel(), streamFn: models.streamSimple.bind(models), faux };
}

/** Emit a complete text delta, then remain in flight until the process dies. */
function interrupted(): ReturnType<typeof createAssistantMessageEventStream> {
  const stream = createAssistantMessageEventStream();
  const partial = fauxAssistantMessage(fauxText('cut off'));
  stream.push({ type: 'start', partial });
  stream.push({ type: 'text_start', contentIndex: 0, partial });
  stream.push({ type: 'text_delta', contentIndex: 0, delta: 'cut off', partial });
  return stream;
}

function events(sse: string): LiveEvent[] {
  return sse.split('\n').flatMap((line) => line.startsWith('data: ') ? [JSON.parse(line.slice(6)) as LiveEvent] : []);
}

function display(initial: string, live: LiveEvent[]): string {
  return live.reduce((text, event) => event.type === 'restart' ? event.text ?? '' : event.type === 'text' ? text + event.delta : text, initial);
}

/** Exercise existing sessions whose recorded inputs predate turn boundaries. */
function legacyLog(harness: Harness): Harness {
  return {
    ...harness,
    drive: (ctx) => harness.drive({ ...ctx, effect(key, options, run) {
      if (options.name !== 'model') return ctx.effect(key, options, run);
      const { turnStart: _, ...input } = options.input as Record<string, unknown>;
      return ctx.effect(key, { ...options, input }, run);
    } }),
  };
}

/** Read through the crash point, retaining exactly the SSE cursor a client saw. */
async function beforeCrash(response: Response) {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let sse = '';
  for (;;) {
    const next = await reader.read();
    if (next.done) throw new Error('stream ended before the crash point');
    sse += decoder.decode(next.value, { stream: true });
    if (!events(sse).some((event) => event.delta === 'cut off')) continue;
    const cursor = [...sse.matchAll(/^id: (.+)$/gm)].at(-1)![1]!;
    await reader.cancel();
    return { text: display('', events(sse)), cursor };
  }
}

async function setup() {
  const storage = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hw-pi-restart-')), recoverActiveRuns: false });
  worlds.push(storage);
  const world = leasedWorld(storage, { visibilityMs: 60, secondMs: 10 });
  const sessions = (options: PiHarnessOptions, wrap: (harness: Harness) => Harness = (harness) => harness) =>
    durableSessions({ world, anchors: vercelAnchors(), marks: false, harness: wrap(piHarness(options)) });
  return { world, sessions };
}

describe('Pi cursor reconnect after a crash', () => {
  it.each([0, 1, 2].flatMap((kept) => [false, true].map((legacy) => ({ kept, legacy }))))('replaces an interrupted model reply, keeping $kept committed steps (legacy log: $legacy)', async ({ kept, legacy }) => {
    const { world, sessions } = await setup();
    let sideEffects = 0;
    const tool = {
      name: 'act', label: 'Act', description: 'Perform an action.', parameters: Type.Object({}), replay: 'never' as const,
      async execute() {
        sideEffects++;
        return { content: [{ type: 'text' as const, text: 'done' }], details: undefined };
      },
    };
    const parts = Array.from({ length: kept }, (_, i) => `Step ${i + 1}. `);
    const first = model([
      fauxAssistantMessage(fauxText('An earlier turn.')),
      ...parts.map((text, i) => fauxAssistantMessage([fauxText(text), fauxToolCall('act', {}, { id: `call_${i}` })], { stopReason: 'toolUse' })),
    ]);
    let calls = 0;
    const a = sessions({ ...first, tools: [tool], streamFn: (...args) => ++calls === kept + 2 ? interrupted() : first.streamFn(...args) }, legacy ? legacyLog : undefined);
    world.registerHandler!(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start({ prompt: 'earlier' });
    await world.broker.deliver()!.done;
    const startIndex = await a.liveEnd(sessionId);
    await a.send(sessionId, 'perform the actions');
    const delivery = world.broker.deliver()!;
    const before = await beforeCrash(await liveResponse(a, sessionId, { startIndex }));
    expect(before.text).toBe(parts.join('') + 'cut off');
    delivery.kill();

    const second = model([fauxAssistantMessage(fauxText('Replacement answer.'))]);
    const b = sessions({ ...second, tools: [tool] });
    world.registerHandler!(QUEUE_PREFIX, b.handler);
    const resumed = await chatRoute(b, { authorize: 'public' }).GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${before.cursor}`));
    await until(async () => world.broker.deliver(delivery.messageId));
    const sse = await resumed.text();
    const live = events(sse);
    expect(display(before.text, live)).toBe(parts.join('') + 'Replacement answer.');
    expect(live.filter((event) => event.type === 'restart')).toEqual([{ type: 'restart', text: parts.join(''), parts }]);
    expect(live.findIndex((event) => event.type === 'restart')).toBeLessThan(live.findIndex((event) => event.type === 'text'));
    expect(sse).toContain('event: end');
    expect(second.faux.state.callCount).toBe(1);
    expect(sideEffects).toBe(kept);
    const state = await b.inspect(sessionId);
    expect(state.effects.get(`model:${kept + 1}`)).toMatchObject({ attempt: 2, status: 'completed', output: { content: [{ type: 'text', text: 'Replacement answer.' }] } });
    expect(state.effectOrder.filter((effect) => effect.name === 'tool.act')).toHaveLength(kept);

    // A completed turn followed by a fresh prompt must not clear that new reply.
    second.faux.setResponses([fauxAssistantMessage(fauxText('The next turn.'))]);
    const route = chatRoute(b, { authorize: 'public' });
    const next = await route.POST(new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify({ sessionId, text: 'again' }) }));
    await until(async () => world.broker.deliver());
    const nextSse = await next.text();
    expect(display('', events(nextSse))).toBe('The next turn.');
    expect(events(nextSse).some((event) => event.type === 'restart')).toBe(false);
    expect(nextSse).toContain('event: end');
    // Reconnecting at the terminal cursor must stop, without replaying the answer.
    const end = [...sse.matchAll(/^id: (.+;end)$/gm)].at(-1)![1]!;
    expect((await route.GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${end}`))).status).toBe(204);
  });

  it('leaves an uninterrupted answer alone and reconnects from its text cursor without replay', async () => {
    const { world, sessions } = await setup();
    const m = model([fauxAssistantMessage(fauxText('cut off'))]);
    const a = sessions(m);
    world.registerHandler!(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start();
    const route = chatRoute(a, { authorize: 'public' });
    const response = await route.POST(new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify({ sessionId, text: 'hello' }) }));
    await world.broker.deliver()!.done;
    const sse = await response.text();
    expect(display('', events(sse))).toBe('cut off');
    expect(events(sse).some((event) => event.type === 'restart')).toBe(false);
    const textRecord = sse.split('\n\n').find((record) => record.includes('"type":"text"'))!;
    const cursor = /^id: (.+)$/m.exec(textRecord)![1]!;
    const continuation = await route.GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${cursor}`));
    expect(display('cut off', events(await continuation.text()))).toBe('cut off');
    expect(m.faux.state.callCount).toBe(1);
  });

  it('retains the text before an interrupted tool and does not repeat its side effect', async () => {
    const { world, sessions } = await setup();
    let sideEffects = 0;
    const tool = (hang: boolean) => ({
      name: 'act', label: 'Act', description: 'Perform an action.', parameters: Type.Object({}), replay: 'never' as const,
      async execute() {
        sideEffects++;
        if (hang) await new Promise(() => {});
        return { content: [{ type: 'text' as const, text: 'done' }], details: undefined };
      },
    });
    const a = sessions({ ...model([fauxAssistantMessage([fauxText('cut off'), fauxToolCall('act', {}, { id: 'call_1' })], { stopReason: 'toolUse' })]), tools: [tool(true)] });
    world.registerHandler!(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start({ prompt: 'act' });
    const delivery = world.broker.deliver()!;
    const before = await beforeCrash(await liveResponse(a, sessionId, { startIndex: 0 }));
    await until(async () => (await a.inspect(sessionId)).effects.get('tool:call_1')?.status === 'running' ? true : undefined);
    delivery.kill();

    const replacement = model([fauxAssistantMessage(fauxText(' Recovered.'))]);
    const b = sessions({ ...replacement, tools: [tool(false)] });
    world.registerHandler!(QUEUE_PREFIX, b.handler);
    const response = await chatRoute(b, { authorize: 'public' }).GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${before.cursor}`));
    await until(async () => world.broker.deliver(delivery.messageId));
    const sse = await response.text();
    expect(display(before.text, events(sse))).toBe('cut off Recovered.');
    expect(events(sse).find((event) => event.type === 'restart')).toEqual({ type: 'restart', text: 'cut off', parts: ['cut off'] });
    expect(sideEffects).toBe(1);
    expect((await b.inspect(sessionId)).effects.get('tool:call_1')).toMatchObject({ status: 'failed', error: { interrupted: true } });
    expect(replacement.faux.state.callCount).toBe(1);
    expect(sse).toContain('event: end');
  });

  it('retains completed text before steering continued the same turn without an idle', async () => {
    const { world, sessions } = await setup();
    const first = model([]);
    const stream = createAssistantMessageEventStream();
    const message = fauxAssistantMessage(fauxText('Before steering. '));
    stream.push({ type: 'start', partial: message });
    stream.push({ type: 'text_start', contentIndex: 0, partial: message });
    stream.push({ type: 'text_delta', contentIndex: 0, delta: 'Before steering. ', partial: message });
    let calls = 0;
    let readSteer!: () => void;
    const steered = new Promise<void>((resolve) => (readSteer = resolve));
    const a = sessions({ ...first, inboxPollMs: 1, streamFn: () => ++calls === 1 ? stream : interrupted() }, (harness) => ({
      ...harness,
      drive: (ctx) => harness.drive({ ...ctx, pending() {
        const pending = ctx.pending();
        if (pending.some((submission) => submission.kind === 'steer')) readSteer();
        return pending;
      } }),
    }));
    world.registerHandler!(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start({ prompt: 'start' });
    const delivery = world.broker.deliver()!;
    await until(async () => (await a.inspect(sessionId)).effects.get('model:0')?.status === 'running' ? true : undefined);
    await a.send(sessionId, 'continue with this guidance', 'steer');
    await steered;
    stream.push({ type: 'done', reason: 'stop', message });
    const before = await beforeCrash(await liveResponse(a, sessionId, { startIndex: 0 }));
    expect(before.text).toBe('Before steering. cut off');
    delivery.kill();

    const b = sessions(model([fauxAssistantMessage(fauxText('After steering.'))]));
    world.registerHandler!(QUEUE_PREFIX, b.handler);
    const response = await chatRoute(b, { authorize: 'public' }).GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${before.cursor}`));
    await until(async () => world.broker.deliver(delivery.messageId));
    const sse = await response.text();
    expect(display(before.text, events(sse))).toBe('Before steering. After steering.');
    expect(events(sse).find((event) => event.type === 'restart')).toEqual({ type: 'restart', text: 'Before steering. ', parts: ['Before steering. '] });
    expect(sse).toContain('event: end');
  });

  it('excludes an earlier unavailable turn, including in a legacy log', async () => {
    const { world, sessions } = await setup();
    const tool: HostTool = {
      name: 'unavailable', description: 'Fail this turn.', inputSchema: Type.Object({}), replay: 'safe',
      async execute(_input, io) {
        io.ctx.halt?.(new Unavailable('offline fixture is unavailable', { code: 'module_missing' }));
        throw io.signal.reason;
      },
    };
    const first = model([fauxAssistantMessage([fauxText('Earlier failed turn. '), fauxToolCall('unavailable', {})], { stopReason: 'toolUse' })]);
    let calls = 0;
    const a = sessions({ ...first, tools: [tool], streamFn: (...args) => ++calls === 1 ? first.streamFn(...args) : interrupted() }, legacyLog);
    world.registerHandler!(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start({ prompt: 'fail this turn' });
    await world.broker.deliver()!.done;
    expect((await a.inspect(sessionId)).effectOrder.some((effect) => effect.name === 'unavailable')).toBe(true);
    const startIndex = await a.liveEnd(sessionId);
    await a.send(sessionId, 'a new turn');
    const delivery = world.broker.deliver()!;
    const before = await beforeCrash(await liveResponse(a, sessionId, { startIndex }));
    delivery.kill();

    const b = sessions(model([fauxAssistantMessage(fauxText('Replacement answer.'))]));
    world.registerHandler!(QUEUE_PREFIX, b.handler);
    const response = await chatRoute(b, { authorize: 'public' }).GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${before.cursor}`));
    await until(async () => world.broker.deliver(delivery.messageId));
    const sse = await response.text();
    expect(display(before.text, events(sse))).toBe('Replacement answer.');
    expect(events(sse).find((event) => event.type === 'restart')).toEqual({ type: 'restart', text: '', parts: [] });
    expect(sse).toContain('event: end');
  });

  it.each([false, true].flatMap((legacy) => [false, true].map((crash) => ({ legacy, crash }))))('excludes a terminal model failure from the next turn (legacy: $legacy, crash: $crash)', async ({ legacy, crash }) => {
    const { world, sessions } = await setup();
    let sideEffects = 0;
    const tool = {
      name: 'act', label: 'Act', description: 'Perform an action.', parameters: Type.Object({}), replay: 'never' as const,
      async execute() {
        sideEffects++;
        return { content: [{ type: 'text' as const, text: 'done' }], details: undefined };
      },
    };
    const first = model([
      fauxAssistantMessage([fauxText('Earlier tool text. '), fauxToolCall('act', {}, { id: 'failed_turn_call' })], { stopReason: 'toolUse' }),
      fauxAssistantMessage(fauxText(''), { stopReason: 'error', errorMessage: 'scripted terminal failure' }),
      fauxAssistantMessage(fauxText('Fresh answer.')),
    ]);
    let calls = 0;
    const a = sessions({ ...first, tools: [tool], modelAttempts: 1, streamFn: (...args) => ++calls > 2 && crash ? interrupted() : first.streamFn(...args) }, legacy ? legacyLog : undefined);
    world.registerHandler!(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start({ prompt: 'act, then fail' });
    await world.broker.deliver()!.done;
    expect((await a.inspect(sessionId)).effects.get('model:1')).toMatchObject({ status: 'failed', error: { message: 'scripted terminal failure' } });
    const response = await chatRoute(a, { authorize: 'public' }).POST(new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify({ sessionId, text: 'a fresh prompt' }) }));
    const delivery = world.broker.deliver()!;
    if (!crash) {
      await delivery.done;
      const sse = await response.text();
      expect(display('', events(sse))).toBe('Fresh answer.');
      expect(events(sse).some((event) => event.type === 'restart')).toBe(false);
      expect(sse).toContain('event: end');
    } else {
      const before = await beforeCrash(response);
      delivery.kill();
      const b = sessions({ ...model([fauxAssistantMessage(fauxText('Fresh answer.'))]), tools: [tool] });
      world.registerHandler!(QUEUE_PREFIX, b.handler);
      const reconnect = await chatRoute(b, { authorize: 'public' }).GET(new Request(`http://local/api/chat?sessionId=${sessionId}&cursor=${before.cursor}`));
      await until(async () => world.broker.deliver(delivery.messageId));
      const sse = await reconnect.text();
      expect(display(before.text, events(sse))).toBe('Fresh answer.');
      expect(events(sse).find((event) => event.type === 'restart')).toEqual({ type: 'restart', text: '', parts: [] });
      expect(sse).toContain('event: end');
    }
    expect(sideEffects).toBe(1);
  });
});

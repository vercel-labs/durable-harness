import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { AgentTool } from '@earendil-works/pi-agent-core';
import { createModels } from '@earendil-works/pi-ai';
import type { Message, TranscriptContext } from '@earendil-works/pi-ai';
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxText,
  fauxToolCall,
} from '@earendil-works/pi-ai/providers/faux';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { chatRoute, defineDoc, durableSessions, type HostTool, isMark, liveStreamName, QUEUE_PREFIX, rawSessions, readJsonLines, Unavailable, UNAVAILABLE_STEP, vercelAnchors, workRemains } from 'harness-world';
import { hostProcess, leasedWorld } from 'harness-world/testing';
import { Type } from 'typebox';
import { afterEach, describe, expect, it } from 'vitest';
import { type PiHarnessOptions, piHarness } from './index.ts';

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 15_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

function shoutTool(onRun: (text: string) => Promise<void> = async () => {}): AgentTool<any> {
  return {
    name: 'shout',
    label: 'Shout',
    description: 'Uppercase some text.',
    parameters: Type.Object({ text: Type.String() }),
    replay: 'never',
    async execute(_id, params: any, _signal, onUpdate) {
      onUpdate?.({ content: [{ type: 'text', text: 'working' }], details: undefined });
      await onRun(params.text);
      return { content: [{ type: 'text', text: params.text.toUpperCase() }], details: undefined };
    },
  };
}

function setupModel() {
  const faux = fauxProvider();
  const models = createModels();
  models.setProvider(faux.provider);
  return { faux, model: faux.getModel(), streamFn: models.streamSimple.bind(models) };
}

const worlds: LocalWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

async function localWorld(dataDir: string, recoverActiveRuns = false) {
  const world = createWorld({ dataDir, recoverActiveRuns });
  worlds.push(world);
  return world;
}

describe('pi timing marks', () => {
  it('marks the first model output, durable and with durability off', async () => {
    const script = () => [fauxAssistantMessage(fauxToolCall('shout', { text: 'hi' }), { stopReason: 'toolUse' }), fauxAssistantMessage(fauxText('I shouted HI.'))];
    const a = setupModel();
    a.faux.setResponses(script());
    const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
    const sessions = durableSessions({ world, harness: piHarness({ model: a.model, streamFn: a.streamFn, tools: [shoutTool()] }) });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    const { sessionId } = await sessions.start({ prompt: 'shout hi' });
    const lines = await until(async () => {
      const l = await readJsonLines(world, sessionId, liveStreamName(sessionId));
      return l.some((e) => isMark(e) && e.name === 'idle') ? l : undefined;
    });
    const firsts = (events: unknown[]) => events.filter((e) => isMark(e) && e.name === 'model.first_token').map((e) => (e as { key?: string }).key);
    expect(firsts(lines)).toEqual(['model:0', 'model:1']);

    const b = setupModel();
    b.faux.setResponses(script());
    const raw = rawSessions({ harness: piHarness({ model: b.model, streamFn: b.streamFn, tools: [shoutTool()] }) });
    const r = await raw.start();
    const seen: unknown[] = [];
    raw.subscribe(r.sessionId, (e) => seen.push(e));
    await raw.send(r.sessionId, 'shout hi');
    await raw.idle(r.sessionId);
    const state = await raw.inspect(r.sessionId);
    expect(state.effectOrder.map((e) => e.name)).toEqual(['model', 'tool.shout', 'model']);
    expect(firsts(seen)).toEqual(['model:0', 'model:1']);
    expect(seen.some((e) => (e as { type?: string }).type === 'idle')).toBe(true);
  });
});

describe('pi on world-local', () => {
  it('a HostTool edits a document through its io, with its outcome', async () => {
    const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
    const add: HostTool<{ item: string }> = {
      name: 'add_todo',
      description: 'Add a todo.',
      inputSchema: Type.Object({ item: Type.String() }),
      replay: 'safe',
      async execute({ item }, io) {
        const todos = io.docs!.update(Todos, (d) => void d.items.push(item));
        return `added; ${todos.items.length} todos`;
      },
    };
    const { faux, model, streamFn } = setupModel();
    faux.setResponses([fauxAssistantMessage(fauxToolCall('add_todo', { item: 'milk' }), { stopReason: 'toolUse' }), fauxAssistantMessage(fauxText('Added milk.'))]);
    const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
    const sessions = durableSessions({ world, harness: piHarness({ model, streamFn, tools: [add] }) });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    const { sessionId } = await sessions.start({ prompt: 'add milk' });
    const state = await until(async () => {
      const s = await sessions.inspect(sessionId);
      return s.effectOrder.filter((e) => e.status === 'completed').length === 3 ? s : undefined;
    });
    expect(state.effectOrder.find((e) => e.name === 'tool.add_todo')?.docs).toEqual([{ kind: 'app.todos', revision: 1 }]);
    expect(await sessions.doc(sessionId, Todos)).toEqual({ value: { items: ['milk'] }, revision: 1 });
  });

  it('shows documents to the model only with promptDocs, as each model call pinned them, at the start of the latest prompt (the system prompt unchanged)', async () => {
    const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
    const add: HostTool<{ item: string }> = {
      name: 'add_todo',
      description: 'Add a todo.',
      inputSchema: Type.Object({ item: Type.String() }),
      replay: 'safe',
      async execute({ item }, io) {
        io.docs!.update(Todos, (d) => void d.items.push(item));
        return 'added';
      },
    };
    const systems: unknown[] = [];
    const run = async (promptDocs?: PiHarnessOptions['promptDocs']) => {
      const { faux, model, streamFn } = setupModel();
      const seen: unknown[] = [];
      const spy: typeof streamFn = (m, context, opts) => {
        // The leading system message is the same with the hook as without it (a cached prefix).
        systems.push(JSON.stringify(context.messages.filter((x) => x.role === 'system')));
        // The documents are the first part of the latest user message; the messages before it are untouched.
        const users = context.messages.flatMap((x, i) => (x.role === 'user' ? [i] : []));
        const latest = context.messages[users.at(-1)!] as { content: string | { type: string; text?: string }[] };
        const parts = typeof latest.content === 'string' ? [{ type: 'text', text: latest.content }] : latest.content;
        seen.push(parts.length > 1 ? parts[0]!.text : undefined);
        expect(parts.at(-1)).toEqual({ type: 'text', text: 'add milk' });
        expect(context.messages).toHaveLength(users.at(-1)! + (seen.length === 1 ? 1 : 3));
        return streamFn(m, context, opts);
      };
      faux.setResponses([fauxAssistantMessage(fauxToolCall('add_todo', { item: 'milk' }), { stopReason: 'toolUse' }), fauxAssistantMessage(fauxText('Added milk.'))]);
      const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
      const sessions = durableSessions({ world, harness: piHarness({ model, streamFn: spy, systemPrompt: 'Be brief.', tools: [add], ...(promptDocs ? { promptDocs } : {}) }) });
      world.registerHandler(QUEUE_PREFIX, sessions.handler);
      const { sessionId } = await sessions.start({ prompt: 'add milk' });
      await until(async () => ((await sessions.inspect(sessionId)).effectOrder.filter((e) => e.status === 'completed').length === 3 ? true : undefined));
      return seen;
    };
    // Off by default.
    expect(await run()).toEqual([undefined, undefined]);
    // On: the first call sees the document never written; the second, the tool's edit.
    expect(await run([Todos])).toEqual([
      'The session\'s documents, as of this model call (JSON; revision 0: never written):\n\n<document name="todos" kind="app.todos" revision="0">\n{"items":[]}\n</document>',
      'The session\'s documents, as of this model call (JSON; revision 0: never written):\n\n<document name="todos" kind="app.todos" revision="1">\n{"items":["milk"]}\n</document>',
    ]);
    expect(await run((docs) => `todos: ${docs.read(Todos).value.items.join(', ') || '(none)'}`)).toEqual(['todos: (none)', 'todos: milk']);
    expect(new Set(systems).size).toBe(1);
    expect(systems[0]).not.toContain('document');
  });

  // A prompt cache keyed on the request's last message (Anthropic's, which
  // pi-ai marks on the last user message) hits only when the request starts
  // with an earlier one: documents taken off a message once it is no longer
  // the latest would make every new turn miss the whole conversation.
  it('with promptDocs, each request starts with the one before it, across turns, edits and a fresh process', async () => {
    const Todos = defineDoc<{ items: string[] }>({ kind: 'app.todos', version: 1, initial: () => ({ items: [] }) });
    const add: HostTool<{ item: string }> = {
      name: 'add_todo',
      description: 'Add a todo.',
      inputSchema: Type.Object({ item: Type.String() }),
      replay: 'safe',
      async execute({ item }, io) {
        io.docs!.update(Todos, (d) => void d.items.push(item));
        return 'added';
      },
    };
    const requests: { system: string; messages: unknown[] }[] = [];
    const harness = () => {
      const { faux, model, streamFn } = setupModel();
      const spy: typeof streamFn = (m, context, opts) => {
        // Pi's Agent marks the assistant messages it holds with their thinking level, which the log does not record: not part of the request a provider sees.
        const messages = context.messages.map((x) => (x.role === 'assistant' ? { ...x, thinkingLevel: undefined } : x));
        requests.push(JSON.parse(JSON.stringify({ system: context.systemPrompt ?? '', messages })));
        return streamFn(m, context, opts);
      };
      return { faux, harness: piHarness({ model, streamFn: spy, systemPrompt: 'Be brief.', tools: [add, shoutTool()], promptDocs: [Todos] }) };
    };
    const dir = await mkdtemp(join(tmpdir(), 'hw-pi-'));
    const models = (s: Awaited<ReturnType<ReturnType<typeof durableSessions>['inspect']>>) => s.effectOrder.filter((e) => e.name === 'model' && e.status === 'completed');
    const first = harness();
    first.faux.setResponses([
      fauxAssistantMessage(fauxToolCall('add_todo', { item: 'milk' }), { stopReason: 'toolUse' }),
      fauxAssistantMessage(fauxText('Added milk.')),
      fauxAssistantMessage(fauxToolCall('shout', { text: 'hi' }), { stopReason: 'toolUse' }),
      fauxAssistantMessage(fauxText('HI.')),
    ]);
    const worldA = await localWorld(dir);
    const a = durableSessions({ world: worldA, harness: first.harness });
    worldA.registerHandler(QUEUE_PREFIX, a.handler);
    const { sessionId } = await a.start({ prompt: 'add milk' });
    await until(async () => (models(await a.inspect(sessionId)).length === 2 ? true : undefined));
    await a.send(sessionId, 'shout hi');
    let state = await until(async () => {
      const st = await a.inspect(sessionId);
      return models(st).length === 4 ? st : undefined;
    });
    // Recorded: a new latest message (calls 1 and 3) and a change on it (call 2); not an unchanged one (call 4).
    expect(models(state).map((r) => (r.output as { promptDocs?: { at: number } }).promptDocs?.at)).toEqual([1, 1, 5, undefined]);
    await worldA.close?.();

    // A fresh process, after an app edit: its request starts with the last one too.
    const second = harness();
    second.faux.setResponses([fauxAssistantMessage(fauxText('Noted.'))]);
    const worldB = await localWorld(dir);
    const b = durableSessions({ world: worldB, harness: second.harness });
    worldB.registerHandler(QUEUE_PREFIX, b.handler);
    await b.updateDoc(sessionId, Todos, (d) => void d.items.push('eggs'));
    await b.send(sessionId, 'thanks');
    state = await until(async () => {
      const st = await b.inspect(sessionId);
      return models(st).length === 5 ? st : undefined;
    });
    expect(requests).toHaveLength(5);
    for (let i = 1; i < requests.length; i++) {
      expect(requests[i]!.system).toBe(requests[0]!.system);
      // The tool's edit in the first turn changed the latest message's documents
      // (index 1, after the system message): the second request keeps what comes before it.
      const kept = i === 1 ? 1 : requests[i - 1]!.messages.length;
      expect(requests[i]!.messages.slice(0, kept)).toEqual(requests[i - 1]!.messages.slice(0, kept));
    }
    const docsOn = (r: { messages: unknown[] }) =>
      (r.messages as { role: string; content: string | { type: string; text?: string }[] }[]).flatMap((m) =>
        m.role === 'user' && Array.isArray(m.content) && m.content.length > 1 ? [m.content[0]!.text!.match(/"items":(\[[^\]]*\])/)![1]] : m.role === 'user' ? ['-'] : []
      );
    // Each user message keeps what it showed last; the latest shows the documents now.
    expect(requests.map(docsOn)).toEqual([['[]'], ['["milk"]'], ['["milk"]', '["milk"]'], ['["milk"]', '["milk"]'], ['["milk"]', '["milk"]', '["milk","eggs"]']]);
  });

  it('records a prompt, a tool call and the answer as the transcript', async () => {
    const { faux, model, streamFn } = setupModel();
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall('shout', { text: 'hi' }), { stopReason: 'toolUse' }),
      fauxAssistantMessage(fauxText('I shouted HI.')),
    ]);
    const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
    const sessions = durableSessions({
      world,
      harness: piHarness({ model, streamFn, systemPrompt: 'Be loud.', tools: [shoutTool()] }),
    });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    const { sessionId } = await sessions.start({ prompt: 'shout hi' });

    const state = await until(async () => {
      const s = await sessions.inspect(sessionId);
      return s.effectOrder.filter((e) => e.status === 'completed').length === 3 ? s : undefined;
    });
    expect(state.effectOrder.map((e) => e.name)).toEqual(['model', 'tool.shout', 'model']);
    const [first, , second] = state.effectOrder;
    expect((first!.input as any).appended.map((m: any) => m.role)).toEqual(['system', 'user']);
    expect((second!.input as any).appended.map((m: any) => m.role)).toEqual(['toolResult']);
    expect((second!.output as any).content[0].text).toBe('I shouted HI.');
    expect(state.submissions.every((s) => state.placed.has(s.id))).toBe(true);
  });

  it('continues the conversation across deliveries from the log alone', async () => {
    const { faux, model, streamFn } = setupModel();
    faux.setResponses([
      fauxAssistantMessage(fauxText('one')),
      (context) => fauxAssistantMessage(fauxText(`seen ${context.messages.length}`)),
    ]);
    const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
    const sessions = durableSessions({ world, harness: piHarness({ model, streamFn }) });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    const { sessionId } = await sessions.start({ prompt: 'first' });
    await until(async () => ((await sessions.inspect(sessionId)).effects.get('model:0')?.status === 'completed' ? true : undefined));
    await sessions.send(sessionId, 'second');
    const state = await until(async () => {
      const s = await sessions.inspect(sessionId);
      return s.effects.get('model:1')?.status === 'completed' ? s : undefined;
    });
    // user, assistant, user: the second delivery rebuilt Pi from the log.
    expect((state.effects.get('model:1')!.output as any).content[0].text).toBe('seen 3');
    expect(faux.state.callCount).toBe(2);
  });

  it('gives an interrupted tool call an interrupted result after a crash', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hw-pi-'));
    const a = setupModel();
    a.faux.setResponses([
      fauxAssistantMessage(fauxToolCall('shout', { text: 'hi' }), { stopReason: 'toolUse' }),
    ]);
    const worldA = await localWorld(dataDir);
    const sessionsA = durableSessions({
      world: worldA,
      harness: piHarness({ ...a, tools: [shoutTool(() => new Promise(() => {}))] }),
    });
    worldA.registerHandler(QUEUE_PREFIX, sessionsA.handler);
    const { sessionId } = await sessionsA.start({ prompt: 'shout hi' });
    await until(async () => {
      const s = await sessionsA.inspect(sessionId);
      return s.effectOrder.find((e) => e.name === 'tool.shout')?.status === 'running' ? true : undefined;
    });
    await worldA.close?.();

    // A new process. Its model has never seen this session.
    const b = setupModel();
    let sawInterruption = '';
    b.faux.setResponses([
      (context) => {
        const last = context.messages.at(-1) as any;
        sawInterruption = last.content[0].text;
        return fauxAssistantMessage(fauxText('The shout may not have happened; retrying is unsafe.'));
      },
    ]);
    await new Promise((r) => setTimeout(r, 400));
    const worldB = await localWorld(dataDir, true);
    const sessionsB = durableSessions({ world: worldB, harness: piHarness({ ...b, tools: [shoutTool()] }) });
    worldB.registerHandler(QUEUE_PREFIX, sessionsB.handler);
    await worldB.start?.();

    const state = await until(async () => {
      const s = await sessionsB.inspect(sessionId);
      return s.effects.get('model:1')?.status === 'completed' ? s : undefined;
    });
    expect(state.effectOrder.map((e) => [e.name, e.status])).toEqual([
      ['model', 'completed'],
      ['tool.shout', 'failed'],
      // The new process fenced the old one out before resolving its intent.
      ['recover', 'completed'],
      ['model', 'completed'],
    ]);
    expect(sawInterruption).toContain('interrupted');
    expect(sawInterruption).toContain('working');
    // The first model call was replayed from the log, not re-requested.
    expect(b.faux.state.callCount).toBe(1);
  });
  it('keeps the transcript when a released model call is retried by another process', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hw-pi-'));
    const a = setupModel();
    const worldA = await localWorld(dataDir);
    const sessionsA = durableSessions({
      world: worldA,
      // The first process's model call hangs (a stalled stream).
      harness: piHarness({ ...a, streamFn: () => new Promise(() => {}) }),
    });
    worldA.registerHandler(QUEUE_PREFIX, sessionsA.handler);
    const { sessionId } = await sessionsA.start({ prompt: 'first' });
    await until(async () => ((await sessionsA.inspect(sessionId)).effects.get('model:0')?.status === 'running' ? true : undefined));
    await worldA.close?.();

    const b = setupModel();
    b.faux.setResponses([
      (context) => fauxAssistantMessage(fauxText(`one ${context.messages.length}`)),
      (context) => fauxAssistantMessage(fauxText(`seen ${context.messages.length}`)),
    ]);
    await new Promise((r) => setTimeout(r, 400));
    const worldB = await localWorld(dataDir, true);
    const sessionsB = durableSessions({ world: worldB, harness: piHarness(b) });
    worldB.registerHandler(QUEUE_PREFIX, sessionsB.handler);
    await worldB.start?.();
    await until(async () => ((await sessionsB.inspect(sessionId)).effects.get('model:0')?.status === 'completed' ? true : undefined));
    // The retry records the submission it re-placed, so the fold still has it.
    const retried = (await sessionsB.inspect(sessionId)).effects.get('model:0')!;
    expect((retried.output as any).content[0].text).toBe('one 1');
    expect((retried.input as any).appended).toHaveLength(1);

    await sessionsB.send(sessionId, 'second');
    const state = await until(async () => {
      const s = await sessionsB.inspect(sessionId);
      return s.effects.get('model:1')?.status === 'completed' ? s : undefined;
    });
    expect((state.effects.get('model:1')!.output as any).content[0].text).toBe('seen 3');
    expect(state.effectOrder.filter((e) => e.name === 'restart')).toHaveLength(0);
  });
});

describe('a turn an unavailable fact failed', () => {
  /**
   * A tool that, while `broken.on`, finds the drive cannot run here (a module
   * it loads is missing) and halts it with `Unavailable`: the kernel fails
   * the turn mid-way, after a model response that asked for tools.
   */
  function probeTool(broken: { on: boolean }, replay: 'safe' | 'never'): HostTool {
    return {
      name: 'probe',
      description: 'Probe the machine.',
      inputSchema: Type.Object({}),
      replay,
      async execute(_input, io) {
        if (!broken.on) return 'probed';
        io.ctx.halt?.(new Unavailable("Cannot find module 'probe-native'", { code: 'module_missing' }));
        if (!io.signal.aborted) await new Promise((r) => io.signal.addEventListener('abort', r, { once: true }));
        throw io.signal.reason;
      },
    };
  }

  async function failedTurn(replay: 'safe' | 'never') {
    const m = setupModel();
    const contexts: Message[][] = [];
    const answer = (text: string) => (context: TranscriptContext) => {
      contexts.push([...context.messages]);
      return fauxAssistantMessage(fauxText(text));
    };
    m.faux.setResponses([
      fauxAssistantMessage(fauxToolCall('probe', {}, { id: 'call_1' }), { stopReason: 'toolUse' }),
      answer('the second message, answered'),
    ]);
    const broken = { on: true };
    const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
    const sessions = durableSessions({ world, marks: false, harness: piHarness({ model: m.model, streamFn: m.streamFn, tools: [probeTool(broken, replay)] }) });
    const { sessionId } = await sessions.start({});
    const first = await sessions.send(sessionId, 'probe it', 'prompt', { wake: 'backstop' });
    await sessions.drive(sessionId);
    const failed = await sessions.inspect(sessionId);
    const fact = failed.effectOrder.find((e) => e.name === UNAVAILABLE_STEP)!;
    expect(fact.output).toMatchObject({ code: 'module_missing', harness: 'pi' });
    // The model asked for the tool; the turn failed before its result reached the model.
    expect(failed.effectOrder.map((e) => e.name)).toEqual(['model', 'tool.probe', UNAVAILABLE_STEP]);
    expect(failed.placed.has(first)).toBe(true);
    broken.on = false;
    const second = await sessions.send(sessionId, 'and now?', 'prompt', { wake: 'backstop' });
    await sessions.drive(sessionId);
    return { sessions, sessionId, second, contexts, faux: m.faux, state: await sessions.inspect(sessionId) };
  }

  it.each(['never', 'safe'] as const)('is over: the next message starts its own turn, the unanswered tool call closed (replay %s)', async (replay) => {
    const { second, contexts, faux, state } = await failedTurn(replay);
    // One model call for the new message; the failed turn was not resumed (its tool not rerun).
    expect(faux.state.callCount).toBe(2);
    expect(faux.getPendingResponseCount()).toBe(0);
    const models = state.effectOrder.filter((e) => e.name === 'model');
    expect(models.map((e) => [e.key, e.status])).toEqual([
      ['model:0', 'completed'],
      ['model:1', 'completed'],
    ]);
    expect(models[1]!.placed).toEqual([second]);
    // What the new call placed: the tool call's result (it never reported one), then the message.
    const appended = (models[1]!.input as { appended: Message[] }).appended;
    expect(appended.map((m) => m.role)).toEqual(['toolResult', 'user']);
    expect(appended[0]).toMatchObject({ role: 'toolResult', toolCallId: 'call_1', toolName: 'probe', isError: true });
    expect(JSON.stringify(appended[0])).toMatch(replay === 'never' ? /interrupted/ : /Not run: the turn ended before this call ran \(module_missing: Cannot find module 'probe-native'\)/);
    // The provider saw the whole transcript: the prompt, the tool call, its result, the new message.
    expect(contexts[0]!.filter((m) => m.role !== 'system').map((m) => m.role)).toEqual(['user', 'assistant', 'toolResult', 'user']);
    // A tool step the failed turn left released is nobody's work now.
    expect(workRemains(state)).toBe(false);
  });

  it('in a chat Function that cannot run it, mid-turn, the turn goes to the queue’s Function, and the chat response streams it to its end', async () => {
    const m = setupModel();
    m.faux.setResponses([
      fauxAssistantMessage(fauxToolCall('probe', {}, { id: 'call_1' }), { stopReason: 'toolUse' }),
      fauxAssistantMessage(fauxText('Probed it.')),
    ]);
    const world = await localWorld(await mkdtemp(join(tmpdir(), 'hw-pi-')));
    const leased = leasedWorld(world, { visibilityMs: 300, secondMs: 50 });
    const common = { anchors: vercelAnchors(), directDriveMs: 60_000, marks: false, runAhead: false };
    // The chat Function: the tool's module is not in its bundle. The queue's Function has it.
    const chat = durableSessions({ world: hostProcess(leased).world, ...common, harness: piHarness({ model: m.model, streamFn: m.streamFn, tools: [probeTool({ on: true }, 'safe')] }) });
    const flow = durableSessions({ world: leased, ...common, harness: piHarness({ model: m.model, streamFn: m.streamFn, tools: [probeTool({ on: false }, 'safe')] }) });
    const { sessionId } = await flow.start({});
    const route = chatRoute(chat, {
      keepaliveMs: 60_000,
      async send(id, text, kind) {
        const submissionId = await chat.send(id, text, kind, { wake: 'backstop' });
        void chat.drive(id);
        return submissionId;
      },
    });
    const res = await route.POST(new Request('http://local/api/chat', { method: 'POST', body: JSON.stringify({ sessionId, text: 'probe it' }) }));
    let streaming = true;
    const serving = (async () => {
      while (streaming) {
        await leased.broker.drain({ timeoutMs: 200, filter: (msg) => !(msg.payload as { input?: { companion?: boolean } }).input?.companion });
        await new Promise((r) => setTimeout(r, 20));
      }
    })();
    const text = await res.text();
    streaming = false;
    await serving;
    const events = text.split('\n').filter((l) => l.startsWith('data: ')).map((l) => JSON.parse(l.slice(6)) as { type: string; delta?: string });
    expect(events.some((e) => e.type === 'error')).toBe(false);
    expect(events.filter((e) => e.type === 'text').map((e) => e.delta).join('')).toBe('Probed it.');
    expect(text).toMatch(/;end\n/);
    const state = await flow.inspect(sessionId);
    expect(state.effectOrder.some((e) => e.name === UNAVAILABLE_STEP)).toBe(false);
    expect(state.effectOrder.filter((e) => e.name !== 'recover').map((e) => [e.name, e.status])).toEqual([
      ['model', 'completed'],
      ['tool.probe', 'completed'],
      ['model', 'completed'],
    ]);
  });

  it('a later drive rebuilds the same transcript, and the turn after goes on from it', async () => {
    const { sessions, sessionId, faux, state } = await failedTurn('safe');
    const contextLength = (state.effectOrder.find((e) => e.key === 'model:1')!.input as { contextLength: number }).contextLength;
    const seen: Message[][] = [];
    faux.setResponses([
      (context: TranscriptContext) => {
        seen.push([...context.messages]);
        return fauxAssistantMessage(fauxText('third'));
      },
    ]);
    await sessions.send(sessionId, 'once more', 'prompt', { wake: 'backstop' });
    await sessions.drive(sessionId);
    const after = await sessions.inspect(sessionId);
    expect(after.effectOrder.filter((e) => e.name === 'model').map((e) => [e.key, e.status])).toEqual([
      ['model:0', 'completed'],
      ['model:1', 'completed'],
      ['model:2', 'completed'],
    ]);
    // model:1 replayed against its memo (same context), then one new call.
    expect(seen).toHaveLength(1);
    expect(seen[0]!.length).toBe(contextLength + 2);
  });
});

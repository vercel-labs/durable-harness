import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { durableSessions, QUEUE_PREFIX, type SessionState } from 'harness-world';
import { type FxTool, fxHarness } from 'harness-world-fx';
import { createFxEngine } from 'libfx';
import { KERNEL_EFFECTS, kernelSession, type Scenario, settled } from '../src/scenario.ts';

/**
 * fx (harness-world-fx on libfx 0.0.14): the real libfx engine, its model
 * requests answered by a scripted AI Gateway (`fetch`, SSE parts).
 */

type Part = Record<string, unknown>;
const usage = { inputTokens: { total: 1 }, outputTokens: { total: 1 } };
const sse = (parts: Part[]) =>
  new Response([...parts.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
const toolCall = (id: string, name: string, input: unknown): Part[] => [
  { type: 'tool-call', toolCallId: id, toolName: name, input },
  { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage },
];
const text = (...deltas: string[]): Part[] => [
  ...deltas.map((delta) => ({ type: 'text-delta', delta })),
  { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage },
];

/** A scripted AI Gateway: each model request takes the next answer. */
function fakeGateway(script: Part[][]): typeof fetch {
  return async (_url, init) => {
    if ((init?.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
    const next = script.shift();
    if (!next) throw new Error('fake gateway: unexpected model request');
    return sse(next);
  };
}

const shout = (ms = 0): FxTool => ({
  name: 'shout',
  description: 'Uppercase some text.',
  inputSchema: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] },
  replay: 'never',
  async execute(input: { text: string }) {
    if (ms) await new Promise((r) => setTimeout(r, ms));
    return input.text.toUpperCase();
  },
});

const harness = (script: Part[][], tools: FxTool[]) =>
  fxHarness({ createFxEngine: createFxEngine as never, apiKey: 'test', model: 'test/model', tools, fetch: fakeGateway(script) });

const vocabulary = new RegExp(`^(turn|model|tool\\..+|checkpoint|${KERNEL_EFFECTS})$`);
const checkpointed = (n: number) => (s: SessionState) => s.effects.get(`checkpoint:${n}`)?.status === 'completed';

/** fx's checkpoint is libfx's own snapshot (`FXCP…`, base64): opaque by design; the transcript is in its turn and model steps. */
const opaqueOk = (stepName: string, path: string) => stepName === 'checkpoint' && path.startsWith('output.');

export const fx: Scenario = {
  name: 'fx',
  title: 'fx (harness-world-fx, libfx 0.0.14)',
  blocking: true,
  skip() {
    // libfx ships one native addon per platform; without it there is no fx here.
    const dir = dirname(createRequire(import.meta.url).resolve('libfx'));
    const addon = join(dir, `libfx.${process.platform}-${process.arch}.node`);
    return existsSync(addon) ? undefined : `libfx has no native addon for ${process.platform}-${process.arch} (${addon})`;
  },
  sessions: [
    {
      label: 'basic',
      async run(storage) {
        const open = await storage.open();
        const sessions = durableSessions({
          world: open.world,
          harness: harness([toolCall('c1', 'shout', { text: 'hi' }), text('I shouted ', 'HI.'), text('Still loud.')], [shout()]),
        });
        await open.serve(QUEUE_PREFIX, sessions.handler);
        const { sessionId } = await sessions.start({ prompt: 'shout hi' });
        await settled(open.world, sessions, sessionId, checkpointed(0), { what: 'turn 0' });
        await sessions.send(sessionId, 'and now?');
        await settled(open.world, sessions, sessionId, checkpointed(1), { what: 'turn 1' });
        return {
          session: kernelSession(() => open.world, () => sessions, sessionId, {
            harness: 'fx',
            submissions: 2,
            calls: ['model', 'tool.shout', 'model', 'model'],
            callsMatch: 'exact',
            vocabulary,
            opaqueOk,
          }),
          dispose: () => open.close(),
        };
      },
    },
    {
      label: 'recovered',
      async run(storage) {
        const a = await storage.open();
        let crashed!: () => void;
        const died = new Promise<void>((r) => (crashed = r));
        const sessionsA = durableSessions({
          world: a.world,
          chaos: true,
          crash: () => crashed(),
          harness: harness([toolCall('c1', 'shout', { text: 'hi' })], [shout(2000)]),
        });
        await a.serve(QUEUE_PREFIX, sessionsA.handler);
        const { sessionId } = await sessionsA.start();
        await sessionsA.chaos(sessionId, { at: 'tool', afterMs: 200 });
        await sessionsA.send(sessionId, 'shout hi');
        await died;
        await a.close();

        const open = await storage.open({ recover: true });
        const sessions = durableSessions({
          world: open.world,
          chaos: true,
          crash: () => {},
          harness: harness([text('The shout may not have happened.'), text('Quiet now.')], [shout()]),
        });
        await open.serve(QUEUE_PREFIX, sessions.handler);
        await settled(open.world, sessions, sessionId, checkpointed(0), { what: 'recovery' });
        await sessions.send(sessionId, 'and now?');
        await settled(open.world, sessions, sessionId, checkpointed(1), { what: 'the turn after recovery', drain: () => open.drained() });
        return {
          session: kernelSession(() => open.world, () => sessions, sessionId, {
            harness: 'fx',
            submissions: 3,
            calls: ['model', 'tool.shout', 'model', 'model'],
            callsMatch: 'exact',
            vocabulary,
            failedSteps: /interrupted/i,
            opaqueOk,
          }),
          dispose: () => open.close(),
        };
      },
    },
  ],
};

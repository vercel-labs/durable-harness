import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { BACKGROUND_CONTEXT } from '@earendil-works/chord/context';
import type { AgentTool } from '@earendil-works/pi-agent-core';
import { createModels } from '@earendil-works/pi-ai';
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness as PiHarness, type Storage } from '@earendil-works/pi-durable';
import { tool } from 'ai';
import { aiSdkHarness } from 'durable-ai-sdk';
import { scriptedModel, text as aiText, toolCall as aiToolCall } from 'durable-ai-sdk/scripted';
import { openWorldStorage, resumeWith } from 'durable-harness/pi-durable';
import type { Harness, SessionState } from 'harness-world';
import { claudeCodeHarness } from 'harness-world-claude-code';
import { fakeSdk } from 'harness-world-claude-code/testing';
import { fakeAnthropic, lastUserText } from 'harness-world-claude-code/testing/fake-anthropic';
import { standIn } from 'harness-world-claude-code/testing/stand-in';
import { type FxTool, fxHarness } from 'harness-world-fx';
import { workspaceTools } from 'harness-world-hands';
import { opencodeHarness } from 'harness-world-opencode';
import { type FakeModel, fakeModel, streamFromModel } from 'harness-world-process/reference/fake-model';
import { piHarness } from 'harness-world-pi';
import { createFxEngine } from 'libfx';
import { Type } from 'typebox';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { codexStandIn } from '../scenarios/codex.ts';
import { admit, ready, ScriptedHost } from '../scenarios/opencode.ts';
import { newHost, type TrackedHost, turnsEnded } from '../scenarios/process.ts';
import { expectToolTurn, kernelTurn, type Tracing, tracing } from '../src/agent-runs.ts';
import { storageFor } from '../src/worlds.ts';

/**
 * Vercel Agent Runs coverage: one tool turn per server harness, each with a
 * scripted model on world-local, through durable-harness's OpenTelemetry
 * integration with an in-memory span processor (src/agent-runs.ts says what
 * each turn must give it: the span tree, its content, and the redaction of a
 * non-public session's spans).
 *
 * The process harnesses run their scripted CLIs with the model behind the
 * drive's gateway proxy (a `fakeModel()`), so their model spans come from
 * the proxy, as a real CLI's do. Their fakes speak no provider's wire format,
 * so those spans carry no model id or text; `agent-runs-live.test.ts` checks
 * them against real models (`AGENT_RUNS_LIVE=1`).
 */

const MARKER = 'zebra-7f3a';
let t: Tracing;
beforeAll(() => {
  t = tracing();
});
afterAll(() => t.close());

const disposers: (() => Promise<void> | void)[] = [];
afterEach(async () => {
  for (const d of disposers.splice(0).reverse()) await Promise.resolve(d()).catch(() => {});
});

const turn = (name: string, harness: Harness, prompt: string, done: (s: SessionState) => boolean) => kernelTurn(t, disposers, name, harness, prompt, done);

const SHOUT_PROMPT = `shout ${MARKER}`;
const modelDone = (n: number) => (s: SessionState) => s.effectOrder.filter((e) => e.name === 'model' && e.status === 'completed').length >= n;
const checkpointed = (s: SessionState) => s.effects.get('checkpoint:0')?.status === 'completed';

describe('Agent Runs: one tool turn per server harness', () => {
  it('Pi (harness-world-pi)', async () => {
    const faux = fauxProvider();
    faux.setResponses([fauxAssistantMessage(fauxToolCall('shout', { text: MARKER }), { stopReason: 'toolUse' }), fauxAssistantMessage(fauxText('Shouted.'))]);
    const models = createModels();
    models.setProvider(faux.provider);
    const shout: AgentTool<any> = {
      name: 'shout',
      label: 'Shout',
      description: 'Uppercase some text.',
      parameters: Type.Object({ text: Type.String() }),
      async execute(_id, params: any) {
        return { content: [{ type: 'text', text: params.text.toUpperCase() }], details: undefined };
      },
    };
    const harness = piHarness({ model: faux.getModel(), streamFn: models.streamSimple.bind(models), systemPrompt: 'Be loud.', tools: [shout] });
    const { sessionId, spans } = await turn('pi', harness, SHOUT_PROMPT, modelDone(2));
    expectToolTurn(spans, { sessionId, marker: MARKER, harness: 'pi', tool: 'shout', modelContent: true, model: faux.getModel().id, usage: true });
  });

  const libfxAddon = join(dirname(createRequire(import.meta.url).resolve('libfx')), `libfx.${process.platform}-${process.arch}.node`);
  it.skipIf(!existsSync(libfxAddon))('fx (harness-world-fx on libfx)', async () => {
    const usage = { inputTokens: { total: 1 }, outputTokens: { total: 1 } };
    const script = [
      [{ type: 'tool-call', toolCallId: 'c1', toolName: 'shout', input: { text: MARKER } }, { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool-calls' }, usage }],
      [{ type: 'text-delta', delta: 'Shouted.' }, { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage }],
    ];
    const fetch: typeof globalThis.fetch = async (_url, init) => {
      if ((init?.method ?? 'GET') === 'GET') return Response.json({ object: 'list', data: [] });
      const next = script.shift();
      if (!next) throw new Error('fake gateway: unexpected model request');
      return new Response([...next.map((p) => `data: ${JSON.stringify(p)}`), 'data: [DONE]', ''].join('\n\n'), { headers: { 'content-type': 'text/event-stream' } });
    };
    const shout: FxTool = {
      name: 'shout',
      description: 'Uppercase some text.',
      inputSchema: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] },
      async execute(input: { text: string }) {
        return input.text.toUpperCase();
      },
    };
    const harness = fxHarness({ createFxEngine: createFxEngine as never, apiKey: 'test', model: 'test/model', tools: [shout], fetch });
    const { sessionId, spans } = await turn('fx', harness, SHOUT_PROMPT, checkpointed);
    expectToolTurn(spans, { sessionId, marker: MARKER, harness: 'fx', tool: 'shout', modelContent: true, model: 'test/model', usage: true });
  });

  it('AI SDK ToolLoopAgent (durable-ai-sdk)', async () => {
    const model = scriptedModel([aiToolCall('shout', { text: MARKER }, 'call_1'), aiText('Shouted.')]);
    const shout = {
      ...tool({ description: 'Uppercase some text.', inputSchema: z.object({ text: z.string() }), execute: async ({ text }) => text.toUpperCase() }),
      replay: 'never' as const,
    };
    const harness = aiSdkHarness({ model, instructions: 'Be loud.', tools: { shout } });
    const { sessionId, spans } = await turn('ai-sdk', harness, SHOUT_PROMPT, checkpointed);
    expectToolTurn(spans, { sessionId, marker: MARKER, harness: 'ai-sdk', tool: 'shout', modelContent: true, usage: true });
  });

  it('Pi with pi-durable (your own Harness on openWorldStorage)', async () => {
    const faux = fauxProvider();
    faux.setResponses([fauxAssistantMessage([fauxToolCall('shout', { text: MARKER })], { stopReason: 'toolUse' }), fauxAssistantMessage([fauxText('Shouted.')])]);
    const models = createModels();
    models.setProvider(faux.provider);
    const registry = createRegistry();
    const execute = async (args: { text: string }) => ({ content: [{ type: 'text' as const, text: args.text.toUpperCase() }] });
    registry.install(defineExtension({ name: 'shout', tools: [defineTool({ name: 'shout', description: 'Uppercase some text.', parameters: Type.Object({ text: Type.String() }), replay: 'safe', execute })] }));
    const open = await storageFor('local', 'agent-runs-pi-durable').open();
    disposers.push(() => open.close());
    // The app's own code: its opener, registered once; a storage from the World where it had SQLite or memory.
    const openHarness = resumeWith((storage: Storage) => PiHarness.open(storage, { models, registry }, BACKGROUND_CONTEXT), { world: open.world });
    const storage = await openWorldStorage(undefined, { world: open.world, traceAudience: 'unknown' });
    const harness = await openHarness(storage);
    const root = await harness.root(BACKGROUND_CONTEXT, { agent: { model: { provider: faux.getModel().provider, modelId: faux.getModel().id } } });
    expect((await (await root.submit({ type: 'input', content: SHOUT_PROMPT }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT)).status).toBe('done');
    await harness.close(BACKGROUND_CONTEXT);
    const spans = await t.finishedTurn(storage.sessionId, 'pi-durable');
    expectToolTurn(spans, { sessionId: storage.sessionId, marker: MARKER, harness: 'pi-durable', tool: 'shout', modelContent: true, model: faux.getModel().id, usage: true });
  });

  /** The scripted process CLIs: `call <tool> <json>` makes one bridged tool call; what they say goes through the proxy to the model. */
  const WRITE_PROMPT = `call write_file {"path":"notes.txt","content":"${MARKER}"}`;
  const processTurn = async (name: string, make: (host: TrackedHost, model: FakeModel) => Harness) => {
    const model = await fakeModel();
    disposers.push(() => model.close());
    const host = await newHost(`agent-runs-${name}`);
    disposers.push(() => host.killAll());
    return turn(name, make(host, model), WRITE_PROMPT, (s) => turnsEnded(s) >= 1);
  };

  it('Claude Code (scripted Agent SDK on the stand-in VM, its model behind the gateway handler)', async () => {
    // What the fake says is a Messages API call through the VM's model port; the model echoes it.
    const model = await fakeAnthropic({ reply: (r) => ({ text: lastUserText(r) }) });
    disposers.push(() => model.close());
    const { api, sandbox } = standIn();
    disposers.push(() => api.dispose());
    const harness = claudeCodeHarness({ builtins: false, tools: workspaceTools({ confirmMs: 3000 }), sandbox, executable: process.execPath, sdk: fakeSdk({ remoteModel: true }), credential: async () => 'test-key', gatewayUrl: model.url });
    const { sessionId, spans } = await turn('claude-code', harness, WRITE_PROMPT, (s) => turnsEnded(s) >= 1);
    expectToolTurn(spans, { sessionId, marker: MARKER, harness: 'claude-code', tool: 'write_file' });
  });

  it('Codex (fake app-server in the stand-in sandbox, its model behind the gateway handler)', async () => {
    const { sessionId, spans } = await processTurn('codex', (_host, model) => {
      const { harness, dispose } = codexStandIn({
        builtins: false,
        tools: workspaceTools({ confirmMs: 3000 }),
        credential: async () => 'test-key',
        gatewayUrl: model.url,
        model: 'openai/gpt-5.4-mini',
        env: { FAKE_CODEX_MODEL: 'remote' },
      });
      disposers.push(dispose);
      return harness;
    });
    expectToolTurn(spans, { sessionId, marker: MARKER, harness: 'codex', tool: 'write_file' });
  });

  it('OpenCode (scripted host, its model behind the proxy)', async () => {
    const errors: unknown[] = [];
    /** OpenCode's model request: through the drive's proxy, after the barrier. */
    const ask = async (h: ScriptedHost, said: string) => {
      await h.request({ type: 'barrier', kind: 'primary' });
      await streamFromModel(`${h.init.gatewayBaseURL}/language-model`, said, (delta) => h.send({ type: 'text', delta }), { authorization: `Bearer ${h.init.gatewayKey}` });
    };
    const toolTurn = async (h: ScriptedHost) => {
      ready(h);
      await admit(h);
      await ask(h, 'Writing it.');
      const input = { path: 'notes.txt', content: MARKER };
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Writing it.' }], ['session.tool.input.started', { id: 'toolu_1', name: 'write_file' }]);
      h.commit(['session.tool.called', { id: 'toolu_1', input }]);
      const r = await h.request({ type: 'tool', callId: 'toolu_1', name: 'write_file', input });
      h.commit(['session.tool.success', { id: 'toolu_1', content: [{ type: 'text', text: r.ok ? r.text : '' }] }], ['session.step.ended']);
      await ask(h, 'Done.');
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Done.' }], ['session.step.ended'], ['session.execution.succeeded']);
      await h.take('close');
    };
    const { sessionId, spans } = await processTurn('opencode', (host, model) =>
      opencodeHarness({
        tools: workspaceTools({ confirmMs: 3000 }),
        root: host.root,
        gatewayUrl: model.url,
        credential: async () => 'test-key',
        connect: async (ctx, init) => {
          const h = new ScriptedHost(ctx, init);
          void toolTurn(h).catch((err) => errors.push(err));
          return h.conn;
        },
      })
    );
    expect(errors).toEqual([]);
    expectToolTurn(spans, { sessionId, marker: MARKER, harness: 'opencode', tool: 'write_file' });
  });
});

import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT as ctx } from '@earendil-works/chord/context';
import { Type } from '@earendil-works/pi-ai';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxAssistantMessage, fauxProvider, fauxText, fauxToolCall } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, defineExtension, defineTool, Harness, type ToolRegistration } from '@earendil-works/pi-durable';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { createWorld } from '@workflow/world-local';
import { readJsonLines } from 'harness-world/streams';
import { untilSettled, worldStorage } from 'pi-world';
import { describe, expect, it } from 'vitest';
import { agentRuns } from '../src/instrumentation/otel.ts';

describe('pi-durable capture policy on actual storage recovery', () => {
  it('reopens an unfinished public turn privately before fresh tool and model commits', async () => {
    const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'pi-private-recovery-')), recoverActiveRuns: false });
    const exporter = new InMemorySpanExporter();
    const runs = agentRuns({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
    runs.register();
    const privateTool = 'restricted recovered tool output';
    const privateModel = 'restricted recovered model output';
    let started!: () => void;
    const running = new Promise<void>(resolve => { started = resolve; });
    let calls = 0;
    const lookup = defineTool({
      name: 'lookup', description: 'Look up a record', replay: 'safe', parameters: Type.Object({}),
      async execute(_args, _api, context) {
        calls++;
        if (calls === 1) {
          started();
          await new Promise<never>((_, reject) => context.abortSignal?.addEventListener('abort', () => reject(context.abortSignal?.reason), { once: true }));
        }
        return { content: [{ type: 'text', text: privateTool }] };
      },
    }) as ToolRegistration;
    const faux = fauxProvider();
    faux.setResponses([
      fauxAssistantMessage([fauxToolCall('lookup', {}, { id: 'call_1' })], { stopReason: 'toolUse' }),
      context => { expect(JSON.stringify(context)).toContain(privateTool); return fauxAssistantMessage([fauxText(privateModel)]); },
    ]);
    const models = createModels();
    models.setProvider(faux.provider);
    const registry = createRegistry();
    registry.install(defineExtension({ name: 'test', tools: [lookup] }));
    const sessionId = await worldStorage.create(world, { name: 'pi-private' });
    const first = await Harness.open(await worldStorage(world, sessionId, { traceAudience: 'public' }), { models, registry }, ctx);
    let second: Harness | undefined;
    try {
      const model = faux.getModel();
      const root = await first.root(ctx, { agent: { model: { provider: model.provider, modelId: model.id } } });
      await root.submit({ type: 'input', content: 'Look up the record.' }, ctx);
      await running;
      await first.close(ctx);
      second = await Harness.open(await worldStorage(world, sessionId, { traceAudience: 'private' }), { models, registry }, ctx);
      expect(await untilSettled(second, Date.now() + 5000)).toBe(false);
      await second.close(ctx);
      await runs.forceFlush();
      expect(calls).toBe(2);
      const spans = exporter.getFinishedSpans().filter(span => span.attributes['agent.run.id'] === sessionId);
      expect(spans.filter(span => span.name === 'execute_tool lookup')).toHaveLength(1);
      const turn = spans.find(span => span.name === 'invoke_agent pi-durable')!;
      expect(turn.attributes).toMatchObject({ 'agent.trace.content.input': false, 'agent.trace.content.output': false, 'agent.turn.outcome': 'completed' });
      const content = JSON.stringify(spans.map(span => ({ attributes: span.attributes, events: span.events, status: span.status })));
      for (const marker of [privateTool, privateModel]) expect(content).not.toContain(marker);
      const streams = (await world.streams.list(sessionId)).filter(name => name.includes('_agent-runs_'));
      expect(streams.length).toBeGreaterThan(0);
      for (const name of streams) {
        const lines = await readJsonLines(world, sessionId, name);
        expect(lines.length).toBeGreaterThan(0);
        for (const marker of [privateTool, privateModel]) expect(JSON.stringify(lines)).not.toContain(marker);
      }
    } finally { await first.close(ctx); await second?.close(ctx); await runs.shutdown(); await world.close?.(); }
  }, 15_000);
});

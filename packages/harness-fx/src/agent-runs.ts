import type { Attributes } from '@vercel/agent-runs';
import type { ContentPart, ModelOptions, ModelResult } from 'harness-world/agent-runs';

/**
 * fx's model calls as Agent Runs records them: libfx's kernel sends AI
 * Gateway language-model calls (an AI SDK prompt, which `@vercel/agent-runs`
 * reads as it is) and reads back the Gateway's stream of AI SDK parts.
 */

const record = (v: unknown): Record<string, unknown> => (v !== null && typeof v === 'object' ? (v as Record<string, unknown>) : {});

/** The call on AI Gateway: its model id (`provider/model`, as Agent Runs links it), and the call's prompt and tools. */
export function fxModelOptions(model: string | undefined, request: unknown): ModelOptions {
  const body = record(request);
  const prompt = Array.isArray(body.prompt) ? (body.prompt as Record<string, unknown>[]) : [];
  const instructions = prompt.filter((m) => m.role === 'system').map((m) => (typeof m.content === 'string' ? m.content : '')).filter(Boolean);
  return {
    provider: 'gateway',
    modelId: model ?? 'unknown',
    messages: prompt.filter((m) => m.role !== 'system'),
    ...(instructions.length ? { instructions: instructions.join('\n\n') } : {}),
    ...(Array.isArray(body.tools) ? { tools: body.tools } : {}),
  };
}

/** The stream's parts (`data: {...}` events). */
function parts(body: string): Record<string, unknown>[] {
  return body.split('\n\n').flatMap((block) => {
    const data = block.trim().replace(/^data: /, '');
    if (!data.startsWith('{')) return [];
    try {
      return [JSON.parse(data) as Record<string, unknown>];
    } catch {
      return [];
    }
  });
}

const tokens = (v: unknown): number | undefined => (typeof v === 'number' ? v : typeof record(v).total === 'number' ? (record(v).total as number) : undefined);

export function fxModelResult(output: { body: string; text: string; toolCalls: { toolCallId: string; toolName: string; input: unknown }[]; finishReason?: string }): ModelResult {
  const all = parts(output.body);
  const finish = record(all.findLast((p) => p.type === 'finish'));
  const usage = record(finish.usage);
  const meta = record(all.find((p) => p.type === 'response-metadata'));
  let reasoning = '';
  for (const p of all) if (p.type === 'reasoning-delta' && typeof p.delta === 'string') reasoning += p.delta;
  const content: ContentPart[] = [
    ...(reasoning ? [{ type: 'reasoning' as const, text: reasoning }] : []),
    ...(output.text ? [{ type: 'text' as const, text: output.text }] : []),
    ...output.toolCalls.map((c) => ({ type: 'tool-call' as const, callId: c.toolCallId, toolName: c.toolName, input: typeof c.input === 'string' ? safeJson(c.input) : c.input })),
  ];
  const cacheRead = record(usage.inputTokens).cacheRead;
  const cacheWrite = record(usage.inputTokens).cacheWrite;
  return {
    finishReason: output.finishReason ?? 'unknown',
    usage: {
      inputTokens: tokens(usage.inputTokens),
      outputTokens: tokens(usage.outputTokens),
      inputTokenDetails: {
        ...(typeof cacheRead === 'number' ? { cacheReadTokens: cacheRead } : {}),
        ...(typeof cacheWrite === 'number' ? { cacheWriteTokens: cacheWrite } : {}),
      },
    },
    ...(typeof meta.id === 'string' ? { responseId: meta.id } : {}),
    ...(typeof meta.modelId === 'string' ? { responseModelId: meta.modelId } : {}),
    content,
  };
}

/** What the Gateway charged for the call (its `finish` part's provider metadata), where Agent Runs reads it. */
export function fxStepAttributes(body: string): Attributes {
  const gateway = record(record(record(parts(body).findLast((p) => p.type === 'finish')).providerMetadata).gateway);
  const usd = (v: unknown) => (v === undefined || v === null || v === '' || !Number.isFinite(Number(v)) ? undefined : Number(v));
  return { 'gen_ai.usage.cost': usd(gateway.cost), 'gen_ai.usage.gateway_cost': usd(gateway.gatewayCost) };
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

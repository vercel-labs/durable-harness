import type { AssistantMessage, Message, Model } from '@earendil-works/pi-ai';
import type { Attributes, ContentPart } from '@vercel/agent-runs';
import type { ModelOptions, ModelResult } from 'harness-world/agent-runs';

/**
 * Pi's messages and responses as Agent Runs records them
 * (`@vercel/agent-runs`, which reads AI SDK model messages). Shared by the
 * kernel harness (harness-pi) and pi-durable's storage (pi-world).
 */

type ModelMessage = { role: string; content: string | Record<string, unknown>[] };

const text = (content: string | readonly { type: string; text?: string }[]) =>
  typeof content === 'string' ? content : content.flatMap((p) => (p.type === 'text' && typeof p.text === 'string' ? [{ type: 'text', text: p.text }] : []));

/** The request's messages, in AI SDK form; its system messages are the instructions and the tools it offers. */
export function piModelMessages(messages: readonly Message[]): { messages: ModelMessage[]; instructions?: string; tools?: unknown[] } {
  const out: ModelMessage[] = [];
  const system: string[] = [];
  const tools = new Map<string, unknown>();
  for (const m of messages) {
    if (m.role === 'system') {
      const content = typeof m.content === 'string' ? m.content : m.content.map((p) => p.text).join('\n');
      if (content) system.push(content);
      for (const section of Object.values(m.sections ?? {})) if (section) system.push(section);
      for (const tool of m.toolsAdded ?? []) tools.set(tool.name, { type: 'function', name: tool.name, description: tool.description, inputSchema: tool.parameters });
      for (const tool of m.toolsRemoved ?? []) tools.delete(tool.name);
    } else if (m.role === 'user') {
      out.push({ role: 'user', content: text(m.content) });
    } else if (m.role === 'assistant') {
      out.push({ role: 'assistant', content: m.content.map(piPart).map((p) => (p.type === 'tool-call' ? { type: p.type, toolCallId: p.callId, toolName: p.toolName, input: p.input } : p)) });
    } else {
      const value = m.content.flatMap((p) => (p.type === 'text' ? [p.text] : [])).join('\n');
      out.push({ role: 'tool', content: [{ type: 'tool-result', toolCallId: m.toolCallId, toolName: m.toolName, output: { type: m.isError ? 'error-text' : 'text', value } }] });
    }
  }
  return { messages: out, ...(system.length ? { instructions: system.join('\n\n') } : {}), ...(tools.size ? { tools: [...tools.values()] } : {}) };
}

function piPart(p: AssistantMessage['content'][number]): ContentPart {
  if (p.type === 'text') return { type: 'text', text: p.text };
  if (p.type === 'thinking') return { type: 'reasoning', text: p.thinking };
  return { type: 'tool-call', callId: p.id, toolName: p.name, input: p.arguments };
}

/** A model call's options: the model, and the request's messages. */
export function piModelOptions(model: Pick<Model<never>, 'provider' | 'id'> | { provider: string; modelId: string }, messages: readonly Message[]): ModelOptions {
  return { provider: model.provider, modelId: 'id' in model ? model.id : model.modelId, ...piModelMessages(messages) };
}

const FINISH: Record<string, string> = { stop: 'stop', length: 'length', toolUse: 'tool-calls', error: 'error', aborted: 'other' };

/**
 * A response as a model call's result. Pi counts input without the cached
 * part; Agent Runs counts it with it (and subtracts the cache).
 */
export function piModelResult(message: AssistantMessage): ModelResult {
  const u = message.usage;
  return {
    finishReason: FINISH[message.stopReason] ?? message.stopReason,
    usage: {
      inputTokens: u.input + u.cacheRead + u.cacheWrite,
      outputTokens: u.output,
      inputTokenDetails: { cacheReadTokens: u.cacheRead, cacheWriteTokens: u.cacheWrite },
      ...(u.cost.total ? { costUsd: u.cost.total } : {}),
    },
    ...(message.responseId ? { responseId: message.responseId } : {}),
    responseModelId: message.responseModel ?? message.model,
    content: message.content.map(piPart),
  };
}

/** The step's cost, where Agent Runs reads it. */
export function piStepAttributes(message: AssistantMessage): Attributes {
  return message.usage.cost.total ? { 'gen_ai.usage.cost': message.usage.cost.total } : {};
}

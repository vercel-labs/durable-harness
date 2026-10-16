import type { Attributes } from '@vercel/agent-runs';
import { type ContentPart, type ModelOptions, type ModelResult, toolDefinitions } from 'harness-world/agent-runs';

/**
 * A CLI's model requests, as Agent Runs records them: the CLIs speak
 * Anthropic's Messages API (Claude Code), OpenAI's Responses API and chat
 * completions (Codex), and AI Gateway's language-model protocol (OpenCode),
 * all through the Gateway proxy. Requests become AI SDK messages, which
 * `@vercel/agent-runs` reads; responses become its content parts and usage.
 */

const record = (v: unknown): Record<string, unknown> => (v !== null && typeof v === 'object' ? (v as Record<string, unknown>) : {});
const list = (v: unknown): Record<string, unknown>[] => (Array.isArray(v) ? v.map(record) : []);
/** An AI SDK model message, as the library reads it. */
type Msg = { role: string; content: string | Record<string, unknown>[] };
const num = (v: unknown): number | undefined => (typeof v === 'number' && Number.isFinite(v) ? v : undefined);

/** A response's text as it streams: its start, and its end (where usage and the finish are), within bounds. */
export class ResponseCapture {
  private head = '';
  private tail = '';
  private truncated = false;
  constructor(
    private readonly headBytes = 1_000_000,
    private readonly tailBytes = 65_536
  ) {}
  push(text: string): void {
    if (!this.truncated && this.head.length + text.length <= this.headBytes) {
      this.head += text;
      return;
    }
    this.truncated = true;
    this.tail = (this.tail + text).slice(-this.tailBytes);
  }
  text(): string {
    if (!this.truncated) return this.head;
    // The tail from its first whole event.
    const at = this.tail.indexOf('\n\n');
    return `${this.head}\n\n${at < 0 ? '' : this.tail.slice(at + 2)}`;
  }
}

function parseJson(text: unknown): unknown {
  if (typeof text !== 'string') return text;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

/** The protocol a request path speaks. */
function protocolOf(path: string): 'anthropic' | 'responses' | 'chat' | 'gateway' | undefined {
  if (/\/messages(\?|$)/.test(path)) return 'anthropic';
  if (/\/responses(\?|$)/.test(path)) return 'responses';
  if (/\/chat\/completions(\?|$)/.test(path)) return 'chat';
  if (/\/ai\/language-model(\?|$)/.test(path)) return 'gateway';
  return undefined;
}

const textParts = (content: unknown, types: readonly string[]) =>
  typeof content === 'string' ? content : list(content).flatMap((p) => (types.includes(String(p.type)) && typeof p.text === 'string' ? [{ type: 'text', text: p.text }] : []));

function anthropicMessages(body: Record<string, unknown>): Msg[] {
  return list(body.messages).map((m): Msg => {
    if (typeof m.content === 'string') return { role: String(m.role), content: m.content };
    const parts = list(m.content).flatMap((p): Record<string, unknown>[] => {
      if (p.type === 'text') return [{ type: 'text', text: p.text }];
      if (p.type === 'thinking') return [{ type: 'reasoning', text: p.thinking }];
      if (p.type === 'tool_use') return [{ type: 'tool-call', toolCallId: p.id, toolName: p.name, input: p.input }];
      if (p.type === 'tool_result') {
        const value = typeof p.content === 'string' ? p.content : list(p.content).flatMap((c) => (c.type === 'text' ? [String(c.text)] : [])).join('\n');
        return [{ type: 'tool-result', toolCallId: p.tool_use_id, output: { type: p.is_error ? 'error-text' : 'text', value } }];
      }
      return [];
    });
    return { role: parts.every((p) => p.type === 'tool-result') && parts.length ? 'tool' : String(m.role), content: parts };
  });
}

function responsesMessages(body: Record<string, unknown>): Msg[] {
  const input = typeof body.input === 'string' ? [{ type: 'message', role: 'user', content: body.input }] : list(body.input);
  return input.flatMap((item): Msg[] => {
    if (item.type === 'function_call') return [{ role: 'assistant', content: [{ type: 'tool-call', toolCallId: item.call_id, toolName: item.name, input: parseJson(item.arguments) }] }];
    if (item.type === 'function_call_output') return [{ role: 'tool', content: [{ type: 'tool-result', toolCallId: item.call_id, output: { type: 'text', value: typeof item.output === 'string' ? item.output : JSON.stringify(item.output) } }] }];
    if (item.type === 'message' || item.role !== undefined) {
      if (item.role === 'system' || item.role === 'developer') return [];
      return [{ role: String(item.role), content: textParts(item.content, ['input_text', 'output_text', 'text']) }];
    }
    return [];
  });
}

function chatMessages(body: Record<string, unknown>): Msg[] {
  return list(body.messages).flatMap((m): Msg[] => {
    if (m.role === 'system' || m.role === 'developer') return [];
    if (m.role === 'tool') return [{ role: 'tool', content: [{ type: 'tool-result', toolCallId: m.tool_call_id, output: { type: 'text', value: typeof m.content === 'string' ? m.content : JSON.stringify(m.content) } }] }];
    const calls = list(m.tool_calls).map((c) => ({ type: 'tool-call', toolCallId: c.id, toolName: record(c.function).name, input: parseJson(record(c.function).arguments) }));
    const text = textParts(m.content, ['text']);
    return [{ role: String(m.role), content: calls.length ? [...(typeof text === 'string' ? (text ? [{ type: 'text', text }] : []) : text), ...calls] : text }];
  });
}

function instructionsOf(protocol: ReturnType<typeof protocolOf>, body: Record<string, unknown>): string | undefined {
  const text = (v: unknown) => (typeof v === 'string' ? v : list(v).flatMap((p) => (typeof p.text === 'string' ? [p.text] : [])).join('\n'));
  if (protocol === 'anthropic') return text(body.system) || undefined;
  if (protocol === 'responses') return [text(body.instructions), ...list(body.input).filter((i) => i.role === 'system' || i.role === 'developer').map((i) => text(i.content))].filter(Boolean).join('\n\n') || undefined;
  if (protocol === 'chat') return list(body.messages).filter((m) => m.role === 'system' || m.role === 'developer').map((m) => text(m.content)).join('\n\n') || undefined;
  if (protocol === 'gateway') return list(body.prompt).filter((m) => m.role === 'system').map((m) => text(m.content)).join('\n\n') || undefined;
  return undefined;
}

/** A request's model call: `model` is the body's, or the Gateway protocol's header. */
export function gatewayModelOptions(request: { path: string; model?: string; body?: unknown }): ModelOptions {
  const body = record(request.body);
  const protocol = protocolOf(request.path);
  // pi-ai names a Gateway model `vercel-ai-gateway/<id>`; the Gateway and the CLIs name it `<provider>/<id>` or `<id>`.
  // The model id stays whole: Agent Runs links a Gateway id (`<provider>/<model>`) to its model page.
  const id = String(body.model ?? request.model ?? 'unknown').replace(/^vercel-ai-gateway\//, '');
  const provider = protocol === 'anthropic' ? 'anthropic' : protocol === 'responses' || protocol === 'chat' ? 'openai' : 'gateway';
  const messages =
    protocol === 'anthropic' ? anthropicMessages(body)
    : protocol === 'responses' ? responsesMessages(body)
    : protocol === 'chat' ? chatMessages(body)
    : protocol === 'gateway' ? list(body.prompt).filter((m) => m.role !== 'system')
    : undefined;
  const instructions = instructionsOf(protocol, body);
  return {
    provider,
    modelId: id,
    ...(messages ? { messages } : {}),
    ...(instructions ? { instructions } : {}),
    ...(Array.isArray(body.tools) ? { tools: toolDefinitions(body.tools) } : {}),
  };
}

/** A response body's events: server-sent events, or one JSON body. */
function events(text: string): Record<string, unknown>[] {
  const whole = parseJson(text.trim());
  if (whole !== null && typeof whole === 'object') return [record(whole)];
  return text.split('\n').flatMap((line) => {
    if (!line.startsWith('data:')) return [];
    const value = parseJson(line.slice(5).trim());
    return value !== null && typeof value === 'object' ? [record(value)] : [];
  });
}

/** A model call's result, from the response the CLI got, and its step's cost where the Gateway reports one. */
export function gatewayModelResult(path: string, response: string): { result: ModelResult; attributes: Attributes } {
  const protocol = protocolOf(path);
  const content: ContentPart[] = [];
  let text = '';
  let reasoning = '';
  let finishReason = 'unknown';
  let responseId: string | undefined;
  let responseModelId: string | undefined;
  let input: number | undefined;
  let output: number | undefined;
  let cacheRead: number | undefined;
  let cacheWrite: number | undefined;
  let gateway: Record<string, unknown> = {};
  const anthropicTools = new Map<number, { callId: string; toolName: string; json: string; input: unknown }>();
  const chatTools = new Map<number, { callId: string; toolName: string; json: string }>();
  for (const e of events(response)) {
    const meta = record(e.providerMetadata ?? record(e.response).providerMetadata);
    if (meta.gateway) gateway = record(meta.gateway);
    if (protocol === 'anthropic') {
      const message = record(e.type === 'message_start' ? e.message : e.type === undefined ? e : undefined);
      if (typeof message.id === 'string') responseId = message.id;
      if (typeof message.model === 'string') responseModelId = message.model;
      const usage = record(message.usage ?? e.usage);
      const base = num(usage.input_tokens);
      cacheRead = num(usage.cache_read_input_tokens) ?? cacheRead;
      cacheWrite = num(usage.cache_creation_input_tokens) ?? cacheWrite;
      if (base !== undefined) input = base + (cacheRead ?? 0) + (cacheWrite ?? 0);
      output = num(usage.output_tokens) ?? output;
      const stop = record(e.delta).stop_reason ?? message.stop_reason;
      if (typeof stop === 'string') finishReason = stop === 'tool_use' ? 'tool-calls' : stop === 'end_turn' ? 'stop' : stop;
      for (const block of list(message.content)) {
        if (block.type === 'text') text += String(block.text ?? '');
        if (block.type === 'tool_use') content.push({ type: 'tool-call', callId: String(block.id), toolName: String(block.name), input: block.input });
      }
      if (e.type === 'content_block_start' && record(e.content_block).type === 'tool_use' && typeof e.index === 'number') {
        const block = record(e.content_block);
        anthropicTools.set(e.index, { callId: String(block.id), toolName: String(block.name), json: '', input: block.input });
      }
      const delta = record(e.delta);
      if (e.type === 'content_block_delta' && delta.type === 'text_delta') text += String(delta.text ?? '');
      if (e.type === 'content_block_delta' && delta.type === 'thinking_delta') reasoning += String(delta.thinking ?? '');
      if (e.type === 'content_block_delta' && delta.type === 'input_json_delta' && typeof e.index === 'number') {
        const tool = anthropicTools.get(e.index);
        if (tool) tool.json += String(delta.partial_json ?? '');
      }
    } else if (protocol === 'responses') {
      const done = record(e.type === 'response.completed' || e.type === 'response.incomplete' || e.type === 'response.failed' ? e.response : e.type === undefined ? e : undefined);
      if (e.type === 'response.output_text.delta' && typeof e.delta === 'string') text += e.delta;
      if (e.type === 'response.reasoning_summary_text.delta' && typeof e.delta === 'string') reasoning += e.delta;
      if (e.type === 'response.output_item.done' && record(e.item).type === 'function_call') {
        const item = record(e.item);
        content.push({ type: 'tool-call', callId: String(item.call_id), toolName: String(item.name), input: parseJson(item.arguments) });
      }
      if (Object.keys(done).length) {
        if (typeof done.id === 'string') responseId = done.id;
        if (typeof done.model === 'string') responseModelId = done.model;
        const usage = record(done.usage);
        input = num(usage.input_tokens) ?? input;
        output = num(usage.output_tokens) ?? output;
        cacheRead = num(record(usage.input_tokens_details).cached_tokens) ?? cacheRead;
        const calls = list(done.output).filter((i) => i.type === 'function_call');
        finishReason = calls.length ? 'tool-calls' : done.status === 'completed' ? 'stop' : String(done.status ?? finishReason);
        if (e.type === undefined) {
          for (const item of list(done.output)) {
            if (item.type === 'message') for (const p of list(item.content)) if (typeof p.text === 'string') text += p.text;
            if (item.type === 'function_call') content.push({ type: 'tool-call', callId: String(item.call_id), toolName: String(item.name), input: parseJson(item.arguments) });
          }
        }
      }
    } else if (protocol === 'chat') {
      if (typeof e.id === 'string') responseId = e.id;
      if (typeof e.model === 'string') responseModelId = e.model;
      const usage = record(e.usage);
      input = num(usage.prompt_tokens) ?? input;
      output = num(usage.completion_tokens) ?? output;
      cacheRead = num(record(usage.prompt_tokens_details).cached_tokens) ?? cacheRead;
      for (const choice of list(e.choices)) {
        const delta = record(choice.delta ?? choice.message);
        if (typeof delta.content === 'string') text += delta.content;
        for (const call of list(delta.tool_calls)) {
          const fn = record(call.function);
          const index = typeof call.index === 'number' ? call.index : chatTools.size;
          const tool = chatTools.get(index);
          if (tool) tool.json += String(fn.arguments ?? '');
          else chatTools.set(index, { callId: String(call.id ?? index), toolName: String(fn.name ?? 'unknown'), json: String(fn.arguments ?? '') });
        }
        if (typeof choice.finish_reason === 'string') finishReason = choice.finish_reason === 'tool_calls' ? 'tool-calls' : choice.finish_reason;
      }
    } else {
      if (e.type === 'text-delta') text += String(e.delta ?? e.textDelta ?? '');
      if (e.type === 'reasoning-delta') reasoning += String(e.delta ?? '');
      if (e.type === 'tool-call') content.push({ type: 'tool-call', callId: String(e.toolCallId), toolName: String(e.toolName), input: parseJson(e.input) });
      if (e.type === 'response-metadata') {
        if (typeof e.id === 'string') responseId = e.id;
        if (typeof e.modelId === 'string') responseModelId = e.modelId;
      }
      if (e.type === 'finish') {
        const reason = e.finishReason;
        finishReason = typeof reason === 'string' ? reason : String(record(reason).unified ?? finishReason);
        const usage = record(e.usage ?? e.totalUsage);
        input = num(usage.inputTokens) ?? num(record(usage.inputTokens).total) ?? input;
        output = num(usage.outputTokens) ?? num(record(usage.outputTokens).total) ?? output;
        cacheRead = num(record(usage.inputTokens).cacheRead) ?? num(usage.cachedInputTokens) ?? cacheRead;
        cacheWrite = num(record(usage.inputTokens).cacheWrite) ?? cacheWrite;
      }
    }
  }
  for (const tool of anthropicTools.values()) content.push({ type: 'tool-call', callId: tool.callId, toolName: tool.toolName, input: tool.json ? parseJson(tool.json) : tool.input });
  for (const tool of chatTools.values()) content.push({ type: 'tool-call', callId: tool.callId, toolName: tool.toolName, input: parseJson(tool.json) });
  const usd = (v: unknown) => (v === undefined || v === null || v === '' || !Number.isFinite(Number(v)) ? undefined : Number(v));
  return {
    result: {
      finishReason,
      usage: { inputTokens: input, outputTokens: output, inputTokenDetails: { cacheReadTokens: cacheRead, cacheWriteTokens: cacheWrite } },
      ...(responseId ? { responseId } : {}),
      ...(responseModelId ? { responseModelId } : {}),
      content: [...(reasoning ? [{ type: 'reasoning' as const, text: reasoning }] : []), ...(text ? [{ type: 'text' as const, text }] : []), ...content],
    },
    attributes: { 'gen_ai.usage.cost': usd(gateway.cost), 'gen_ai.usage.gateway_cost': usd(gateway.gatewayCost) },
  };
}

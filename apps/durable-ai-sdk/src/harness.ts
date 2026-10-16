import type {
  LanguageModelV4,
  LanguageModelV4CallOptions,
  LanguageModelV4Content,
  LanguageModelV4FinishReason,
  LanguageModelV4GenerateResult,
  LanguageModelV4ResponseMetadata,
  LanguageModelV4Usage,
  SharedV4ProviderMetadata,
} from '@ai-sdk/provider';
import {
  APICallError,
  gateway,
  type LanguageModel,
  type ModelMessage,
  type Tool,
  type ToolExecutionOptions,
  ToolLoopAgent,
  type ToolLoopAgentSettings,
  type ToolSet,
  wrapLanguageModel,
} from 'ai';
import { modelContent } from '@vercel/agent-runs/ai-sdk';
import { toolDefinitions } from 'harness-world/agent-runs';
import { type DriveContext, EffectFailure, Fatal, type Harness, type HostToolIO, type PromptDocs, promptDocsText, type ReplayPolicy, type SessionState, Yield } from 'durable-harness';

/**
 * The AI SDK's `ToolLoopAgent`, durable on a Workflow World.
 *
 * The agent loop is the AI SDK's own. Its model goes through a middleware
 * that makes every model call an effect (`model:<turn>:<step>`, replay
 * `safe`), and every tool's `execute` is an effect keyed by the tool call id
 * (`tool:<toolCallId>`, the tool's own replay rule). A turn starts with a
 * `turn` step that places its prompts and ends with a `checkpoint` step that
 * holds every message the turn generated. A drive folds the finished turns
 * back into `ModelMessage[]` and runs the agent again for the unfinished
 * one: every call the log already has returns its recorded result, and the
 * rest run for real.
 *
 * The AI SDK hands the model whatever a tool's `execute` throws, as the
 * tool's error result. A committed failure is a result. A kernel stop (the
 * session belongs to another driver, a write was refused) is not: it halts
 * the drive, so no model call is made with it and no checkpoint records it.
 */

/** An AI SDK tool plus its crash policy (default `'never'`). */
export type DurableTool = Tool & { replay?: ReplayPolicy };
/** A durable tool's `execute` gets the kernel's io for the call as a third argument. */
type Execute = (input: unknown, options: ToolExecutionOptions<any>, io: HostToolIO) => unknown;

export interface AiSdkHarnessOptions extends Omit<ToolLoopAgentSettings<never, ToolSet>, 'model' | 'tools'> {
  /** A model, or a `'provider/model'` id for the AI Gateway. */
  model: LanguageModel;
  tools?: Record<string, DurableTool>;
  /**
   * Documents to show the model: the start of the latest user message of
   * every model call, with the documents as that call pinned them
   * (`docsPrompt`), or a function of the call's documents. Each earlier user
   * message keeps what the last call that had it as its latest showed (the
   * model step records it), so every prompt extends the one before it: the
   * provider's prompt cache keeps the conversation up to the latest user
   * message when a document changes, and all of it when none did. Off by
   * default.
   */
  promptDocs?: PromptDocs;
}

/**
 * What a model step's output holds beside the model's result, with
 * `promptDocs`: the documents' text the call showed at the start of the user
 * message at index `at` of its prompt, when it differs from what that message
 * showed in the call before (none: `''`).
 */
interface ShownDocs {
  at: number;
  text: string;
}

interface TurnInput {
  submissions: { id: string; text: string }[];
}
export interface Checkpoint {
  turn: number;
  stopReason: string;
  usage?: unknown;
  /**
   * Every message the turn generated (assistant and tool): `generate()`'s
   * `responseMessages`, or, when a model call failed for good, the messages
   * of the steps that finished before it.
   */
  messages: ModelMessage[];
  /** Set when a model call failed for good and ended the turn. */
  error?: string;
}

export function aiSdkHarness({ model, tools = {}, promptDocs, ...settings }: AiSdkHarnessOptions): Harness {
  const base = typeof model === 'string' ? gateway(model) : model;
  const attempts = (settings.maxRetries ?? 2) + 1;

  async function runTurn(ctx: DriveContext, n: number): Promise<void> {
    let step = 0;
    let stop: unknown; // a kernel stop a tool's effect threw: this drive is over
    const done: ModelMessage[] = []; // the messages of the turn's finished steps
    const agent = new ToolLoopAgent<never, ToolSet>({
      ...settings,
      maxRetries: 0, // retries happen inside the model effect, so a retry is never a new step
      model: wrapLanguageModel({
        model: base,
        middleware: {
          specificationVersion: 'v4',
          async wrapGenerate({ params, model }) {
            if (stop) throw stop;
            const key = `model:${n}:${step++}`;
            sameConversation(ctx, key, params);
            const input = { model: model.modelId, messages: params.prompt.length };
            const output = await ctx.effect(key, { name: 'model', replay: 'safe', input }, async (io) => {
              // The prompt hook: the documents as this call pinned them at the
              // start of the latest prompt, and what earlier calls showed on
              // the earlier ones (so the prompt extends the previous one).
              if (!promptDocs) return tracedGenerate(ctx, model, { ...params, abortSignal: io.signal }, attempts, key);
              const shown = docsShown(ctx.state());
              const at = params.prompt.findLastIndex((m) => m.role === 'user');
              const docs = promptDocsText(promptDocs, io.docs) ?? '';
              const prompt = withDocuments(params.prompt, shown, at, docs);
              const result = await tracedGenerate(ctx, model, { ...params, prompt, abortSignal: io.signal }, attempts, key);
              return at >= 0 && docs !== (shown.get(at) ?? '') ? { ...result, promptDocs: { at, text: docs } satisfies ShownDocs } : result;
            });
            const { promptDocs: _, ...result } = output as typeof output & { promptDocs?: ShownDocs };
            return result;
          },
        },
      }),
      tools: Object.fromEntries(
        Object.entries(tools).map(([name, t]) => [name, {
          ...t,
          execute: async (input: unknown, options: ToolExecutionOptions<any>) => {
            try {
              return await ctx.effect(`tool:${options.toolCallId}`, { name: `tool.${name}`, replay: t.replay ?? 'never', input }, async (io) => {
                ctx.live({ type: 'tool_start', name, args: input });
                return (t.execute as Execute)(input, { ...options, abortSignal: io.signal }, { ...io, ctx });
              });
            } catch (err) {
              // A committed failure (an interrupted call too) is a result: the AI SDK hands it to the model.
              // Anything else is the drive stopping, so halt it. Out of time, the calls in flight may finish.
              if (!(err instanceof EffectFailure)) {
                stop ??= err;
                if (!(err instanceof Yield && err.reason === 'budget')) ctx.halt?.(err);
              }
              throw err;
            }
          },
        }])
      ) as ToolSet,
    });

    let outcome: Omit<Checkpoint, 'turn'>;
    try {
      const result = await agent.generate({ messages: transcript(ctx.state(), n), onStepEnd: (s) => void done.push(...s.response.messages) });
      outcome = { stopReason: result.finishReason, usage: result.totalUsage, messages: result.responseMessages };
    } catch (err) {
      if (!(err instanceof EffectFailure)) throw err; // the drive must stop: the kernel's business
      outcome = { stopReason: 'error', messages: done, error: err.message }; // a model call failed for good: keep the steps before it
    }
    await ctx.effect(`checkpoint:${n}`, { name: 'checkpoint', replay: 'safe', input: { turn: n } }, async () => ({ turn: n, ...outcome }));
  }

  return {
    name: 'ai-sdk',
    modelCalls: 'self',
    async drive(ctx) {
      for (let n = 0; ; n++) {
        const recorded = ctx.state().effects.get(`turn:${n}`)?.input as TurnInput | undefined;
        if (!recorded) await ctx.refresh();
        const submissions = recorded?.submissions ?? ctx.pending().map((s) => ({ id: s.id, text: s.text ?? '' }));
        if (submissions.length === 0) break;
        // The turn's first step places its prompts: once it commits, they are this turn's.
        await ctx.effect(`turn:${n}`, { name: 'turn', replay: 'safe', placed: submissions.map((s) => s.id), input: { submissions } }, async () => ({}));
        if (!ctx.isMemoized(`checkpoint:${n}`)) await runTurn(ctx, n);
      }
      ctx.live({ type: 'idle' });
    },
  };
}

/** The conversation, folded from the log: each turn's prompt, then the messages its checkpoint holds. */
export function transcript(state: SessionState, through = Infinity): ModelMessage[] {
  const messages: ModelMessage[] = [];
  for (let n = 0; n <= through; n++) {
    const turn = state.effects.get(`turn:${n}`);
    if (!turn) break;
    messages.push({ role: 'user', content: (turn.input as TurnInput).submissions.map((s) => s.text).join('\n\n') });
    const end = state.effects.get(`checkpoint:${n}`);
    if (n < through && end?.status === 'completed') messages.push(...(end.output as Checkpoint).messages);
  }
  return messages;
}

/**
 * A replayed model step must be asked with the same conversation it was
 * recorded with. If not, this build drives the session differently from the
 * one that wrote the log (other tools, other instructions), and no later
 * delivery can do better: fail the session instead of answering from a
 * recording of a different question. A cheap check: the message count.
 */
function sameConversation(ctx: DriveContext, key: string, params: LanguageModelV4CallOptions): void {
  const seen = ctx.state().effects.get(key)?.input as { messages: number } | undefined;
  if (seen && seen.messages !== params.prompt.length) {
    throw new Fatal(`${key}: the agent sent a different conversation than the log recorded`);
  }
}

/**
 * One model call, streamed: text goes to the live stream as the model writes
 * it, and its first output (text, reasoning or a tool call) is marked
 * (`model.first_token`, keyed by the model step, next to the step's own
 * `model.request` and `model.done` marks), so a page can take the model's
 * time out of the turn. The stream is folded back into what `doGenerate`
 * returns, which is what the step records. What the AI SDK's own
 * `maxRetries` would retry is retried here, inside the step, while nothing
 * has streamed yet; the raw HTTP request and response bodies are left out
 * of the log.
 */
/** `generate`, recorded as the model call of its step in Agent Runs (when `agentRuns()` is registered). */
async function tracedGenerate(ctx: DriveContext, model: LanguageModelV4, params: LanguageModelV4CallOptions, attempts: number, key: string) {
  const system = params.prompt.flatMap((m) => (m.role === 'system' ? [m.content] : []));
  const traced = await ctx.tracing?.modelCall({
    provider: model.provider,
    modelId: model.modelId,
    messages: params.prompt.filter((m) => m.role !== 'system'),
    ...(system.length ? { instructions: system.join('\n\n') } : {}),
    ...(params.tools ? { tools: toolDefinitions(params.tools) } : {}),
  });
  try {
    const result = await generate(ctx, model, params, attempts, key);
    const usage = result.usage;
    await traced?.model.complete({
      outcome: 'completed',
      result: {
        finishReason: result.finishReason.unified,
        usage: {
          inputTokens: usage.inputTokens.total,
          outputTokens: usage.outputTokens.total,
          inputTokenDetails: { cacheReadTokens: usage.inputTokens.cacheRead, cacheWriteTokens: usage.inputTokens.cacheWrite },
        },
        ...(result.response.id ? { responseId: result.response.id } : {}),
        ...(result.response.modelId ? { responseModelId: result.response.modelId } : {}),
        content: modelContent(result.content.map((p) => (p.type === 'tool-call' ? { ...p, input: parseJson(p.input) } : p))),
      },
    });
    const gatewayMeta = (result.providerMetadata?.gateway ?? {}) as Record<string, unknown>;
    const usd = (v: unknown) => (v === undefined || v === null || v === '' || !Number.isFinite(Number(v)) ? undefined : Number(v));
    traced?.attempt.attributes({ 'gen_ai.usage.cost': usd(gatewayMeta.cost), 'gen_ai.usage.gateway_cost': usd(gatewayMeta.gatewayCost) });
    return result;
  } catch (error) {
    await traced?.model.fail(error);
    throw error;
  }
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function generate(ctx: DriveContext, model: LanguageModelV4, params: LanguageModelV4CallOptions, attempts: number, key: string) {
  for (let attempt = 1; ; attempt++) {
    const progress = { streamed: false };
    try {
      return await streamed(ctx, model, params, key, progress);
    } catch (err) {
      if (progress.streamed || attempt >= attempts || !APICallError.isInstance(err) || !err.isRetryable) throw err;
      await new Promise((r) => setTimeout(r, 1000 * 2 ** attempt));
    }
  }
}

async function streamed(ctx: DriveContext, model: LanguageModelV4, params: LanguageModelV4CallOptions, key: string, progress: { streamed: boolean }) {
  const { stream } = await model.doStream(params);
  const content: LanguageModelV4Content[] = [];
  const open = new Map<string, { type: 'text' | 'reasoning'; text: string; providerMetadata?: SharedV4ProviderMetadata }>();
  let warnings: LanguageModelV4GenerateResult['warnings'] = [];
  let response: LanguageModelV4ResponseMetadata = {};
  let finish: { finishReason: LanguageModelV4FinishReason; usage: LanguageModelV4Usage; providerMetadata?: SharedV4ProviderMetadata } | undefined;
  const first = () => {
    if (progress.streamed) return;
    progress.streamed = true;
    ctx.mark?.('model.first_token', { key });
  };
  const part = (id: string, type: 'text' | 'reasoning', providerMetadata?: SharedV4ProviderMetadata) => {
    let p = open.get(id);
    if (!p) {
      p = { type, text: '', ...(providerMetadata ? { providerMetadata } : {}) };
      open.set(id, p);
      content.push(p);
    }
    return p;
  };
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    switch (value.type) {
      case 'stream-start':
        warnings = value.warnings;
        break;
      case 'response-metadata': {
        const { type: _, ...meta } = value;
        response = { ...response, ...meta };
        break;
      }
      case 'text-start':
      case 'reasoning-start':
        part(value.id, value.type === 'text-start' ? 'text' : 'reasoning', value.providerMetadata);
        break;
      case 'text-delta':
      case 'reasoning-delta': {
        first();
        part(value.id, value.type === 'text-delta' ? 'text' : 'reasoning').text += value.delta;
        if (value.type === 'text-delta') ctx.live({ type: 'text', delta: value.delta });
        break;
      }
      case 'text-end':
      case 'reasoning-end': {
        const p = open.get(value.id);
        if (p && value.providerMetadata) p.providerMetadata = value.providerMetadata;
        break;
      }
      case 'tool-input-start':
        first();
        break;
      case 'tool-input-delta':
      case 'tool-input-end':
      case 'raw':
        break;
      case 'finish':
        finish = { finishReason: value.finishReason, usage: value.usage, ...(value.providerMetadata ? { providerMetadata: value.providerMetadata } : {}) };
        break;
      case 'error':
        throw value.error;
      default:
        // A tool call, a provider's tool result, a file, a source: content as is.
        if (value.type === 'tool-call') first();
        content.push(value);
    }
  }
  if (!finish) throw new Error('the model stream ended without finishing');
  return { content, ...finish, warnings, response: { ...response, headers: undefined, body: undefined } };
}

/** The documents each user message of the conversation showed, by index: what the last model call that showed any there recorded (`ShownDocs`). */
function docsShown(state: SessionState): Map<number, string> {
  const shown = new Map<number, string>();
  for (const r of state.effectOrder) {
    if (r.name !== 'model' || r.status !== 'completed') continue;
    const d = (r.output as { promptDocs?: ShownDocs } | undefined)?.promptDocs;
    if (d && Number.isSafeInteger(d.at) && typeof d.text === 'string') shown.set(d.at, d.text);
  }
  return shown;
}

/**
 * `prompt` with the documents at the start of user messages: `latest` on the
 * latest one (index `at`; a user message at the end when there is none), and
 * on each earlier one what an earlier call showed there (`shown`). Near the
 * end, not with the instructions, where a change would invalidate the
 * provider's cache of the whole conversation; and never taken off a message
 * once a prompt carried it, so each prompt starts with the previous one (a
 * prefix cache keyed on the last message, as Anthropic's, otherwise misses
 * every earlier turn). Part of a user message, not a message of its own (or a
 * system message, which some providers refuse after the conversation starts).
 */
function withDocuments(prompt: LanguageModelV4CallOptions['prompt'], shown: Map<number, string>, at: number, latest: string): LanguageModelV4CallOptions['prompt'] {
  const out = prompt.map((m, i) => {
    const text = i === at ? latest : shown.get(i);
    return m.role === 'user' && text ? { ...m, content: [{ type: 'text' as const, text }, ...m.content] } : m;
  });
  return at === -1 && latest ? [...out, { role: 'user', content: [{ type: 'text', text: latest }] }] : out;
}

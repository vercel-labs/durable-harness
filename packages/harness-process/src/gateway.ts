import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Readable } from 'node:stream';
import { TransformStream } from 'node:stream/web';
import type { GatewayErrorCode, GatewayHandler } from './environment.ts';

/**
 * AI Gateway credentials for process harnesses: a fresh credential per call,
 * and a loopback proxy that adds it to every request, so a child process
 * never holds the secret (and a rotated OIDC token needs no restart).
 */

/** The AI Gateway's base URL (`AI_GATEWAY_BASE_URL` overrides it). */
export const gatewayUrl = () => process.env.AI_GATEWAY_BASE_URL ?? 'https://ai-gateway.vercel.sh';

/**
 * The Gateway credential: `AI_GATEWAY_API_KEY` if set, else the Vercel OIDC
 * token (`@vercel/oidc`: the deployment's runtime token on Vercel,
 * `VERCEL_OIDC_TOKEN` from `vercel env pull` locally, refreshed when it is
 * close to expiring). Read fresh on every call: never cache it.
 */
export async function gatewayCredential(): Promise<string> {
  const key = process.env.AI_GATEWAY_API_KEY;
  if (key) return key;
  try {
    const { getVercelOidcToken } = await import('@vercel/oidc');
    return await getVercelOidcToken();
  } catch (err) {
    throw new Error(`No AI Gateway credential: set AI_GATEWAY_API_KEY or run \`vercel env pull\` for VERCEL_OIDC_TOKEN (${(err as Error).message})`);
  }
}

export interface GatewayProxyOptions {
  /** Default `gatewayCredential`, called per request. */
  credential?: () => Promise<string>;
  /** Default `gatewayUrl()`. */
  target?: string;
  /**
   * The headers that carry the credential. Default: `authorization: Bearer`
   * and `x-api-key` (Anthropic-style clients send the latter), replacing
   * whatever placeholder the child sent.
   */
  headers?: (credential: string) => Record<string, string>;
  /** Called per proxied request (method, path, status), e.g. for usage facts. */
  onRequest?: (info: { method: string; path: string; status: number }) => void;
  /**
   * Called when a request the proxy will forward comes in, before it is
   * forwarded. The function it returns, if any, is called once that request
   * is over: its response ended, failed, or its connection closed (the child
   * died, the proxy closed). `ModelSteps.request` is one: the drive's model
   * steps.
   */
  track?: (request: { method: string; path: string; model?: string }) => TrackEnd | void;
  /**
   * Forward only requests that carry this value (`authorization: Bearer
   * <token>` or `x-api-key: <token>`), or one of these values; anything
   * else gets a 401. Give the child the token as its placeholder key
   * (`gatewayToken()`: a fresh random one per proxy). Then a process can
   * use the credential through the loopback port only if it holds the
   * child's key, which it gets only by reading the child's environment
   * (the same uid can, from `/proc/<pid>/environ`). So the check stops
   * other uids and guessing, not a co-tenant command of the same uid: that
   * is why a CLI's own tools (`builtins: true`) belong only where the CLI
   * is alone in its sandbox, and the VM path replaces this proxy with the
   * relay (docs/workspaces.md §5.5). Required: the proxy refuses to start
   * without one.
   */
  token: string | readonly string[];
}

/** A fresh placeholder key for a child, to give `gatewayProxy` as its `token`: one per proxy, never reused. */
export const gatewayToken = (prefix = 'harness-world'): string => `${prefix}-${randomUUID()}`;

/**
 * What `track` returns for a request: called once the request is over. With
 * `firstToken`, the proxy also calls that when the response streams its
 * first output (text, reasoning or a tool call: `FIRST_OUTPUT`).
 */
export type TrackEnd = ((end: { status?: number; error?: string; requestBody?: unknown; responseBody?: string }) => void) & { firstToken?: () => void; capture?: boolean };

/**
 * A response's first output, in any of the APIs the CLIs speak through the
 * Gateway: Anthropic's messages (`content_block_delta`), OpenAI's responses
 * (`response.*.delta`) and chat completions (a delta with content or tool
 * calls), and the Gateway's own stream (`text-delta`, `tool-input-start`).
 */
export const FIRST_OUTPUT =
  /"type":\s*"(content_block_delta|response\.output_text\.delta|response\.reasoning(?:_summary)?_text\.delta|response\.function_call_arguments\.delta|text-delta|reasoning-delta|tool-input-start|tool-input-delta|tool-call)"|"delta":\s*\{[^{}]*"(?:content|reasoning_content|reasoning)":\s*"[^"]|"delta":\s*\{[^{}]*"tool_calls"/;

export interface GatewayProxy {
  /** `http://127.0.0.1:<port>`: give it to the child as its base URL. */
  url: string;
  port: number;
  close(): Promise<void>;
}

/** Headers never forwarded in either direction (hop-by-hop, or recomputed). */
const HOP = new Set(['connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'upgrade', 'host', 'content-length', 'te', 'trailer']);

/**
 * A passthrough proxy on 127.0.0.1 that forwards every request carrying
 * `token` (or one of its values) to the Gateway, with the credential added
 * in its place. The child is configured with the proxy's URL and `token` as
 * its key. Responses stream through (SSE).
 */
export async function gatewayProxy(options: GatewayProxyOptions): Promise<GatewayProxy> {
  const tokens = typeof options?.token === 'string' ? [options.token] : Array.isArray(options?.token) ? (options.token as readonly unknown[]) : [];
  if (!tokens.length || tokens.some((t) => typeof t !== 'string' || !t))
    throw new TypeError('harness-world-process gateway proxy: `token` is required (a fresh `gatewayToken()` the child sends as its key), so only a process that holds the child\'s key can use the credential through the loopback port');
  const credential = options.credential ?? gatewayCredential;
  const target = (options.target ?? gatewayUrl()).replace(/\/$/, '');
  const auth = options.headers ?? ((c: string) => ({ authorization: `Bearer ${c}`, 'x-api-key': c }));
  const server = createServer((req, res) => {
    void forward(req, res).catch((err) => {
      if (!res.headersSent) res.writeHead(502, { 'content-type': 'text/plain' });
      res.end(`harness-world-process gateway proxy: ${(err as Error).message}`);
    });
  });
  async function forward(req: IncomingMessage, res: ServerResponse) {
    if (!tokens.some((t) => req.headers.authorization === `Bearer ${t}` || req.headers['x-api-key'] === t)) {
      res.writeHead(401, { 'content-type': 'text/plain' });
      return void res.end('harness-world-process gateway proxy: the request does not carry the key this proxy gave its child');
    }
    // The AI SDK Gateway protocol (used by OpenCode) sends the model in
    // this header, not the JSON body. Pass only that identifier to tracing;
    // never capture the request's credential or other headers.
    const model = req.headers['ai-language-model-id'];
    const ended = options.track?.({ method: req.method ?? 'GET', path: req.url ?? '/', ...(typeof model === 'string' && model ? { model } : {}) });
    let requestBody = '';
    let responseBody = '';
    const capture = !!ended && !!ended.capture;
    const requestDecoder = new TextDecoder();
    const captureLimit = 256_000;
    // `close` comes after the response ended, or when its connection went first.
    if (ended) res.once('close', () => ended({ ...(res.writableFinished ? { status: res.statusCode } : { error: 'the connection closed before the response ended' }), ...(capture ? { requestBody: parseBody(requestBody), responseBody } : {}) }));
    let firstToken = ended ? ended.firstToken : undefined;
    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (v === undefined || HOP.has(k) || k === 'authorization' || k === 'x-api-key') continue;
      headers.set(k, Array.isArray(v) ? v.join(', ') : v);
    }
    for (const [k, v] of Object.entries(auth(await credential()))) headers.set(k, v);
    const abort = new AbortController();
    res.on('close', () => abort.abort());
    const hasBody = req.method !== 'GET' && req.method !== 'HEAD';
    const upstream = await fetch(`${target}${req.url ?? '/'}`, {
      method: req.method ?? 'GET',
      headers,
      ...(hasBody ? { body: Readable.toWeb(req).pipeThrough(new TransformStream({
        transform(chunk, controller) {
          if (capture && requestBody.length < captureLimit) requestBody += requestDecoder.decode(chunk, { stream: true }).slice(0, captureLimit - requestBody.length);
          controller.enqueue(chunk);
        },
        flush() { if (capture) requestBody += requestDecoder.decode().slice(0, captureLimit - requestBody.length); },
      })) as ReadableStream, duplex: 'half' } : {}),
      signal: abort.signal,
    } as RequestInit);
    const out: Record<string, string> = {};
    upstream.headers.forEach((v, k) => {
      // fetch already decoded the body.
      if (!HOP.has(k) && k !== 'content-encoding') out[k] = v;
    });
    res.writeHead(upstream.status, out);
    options.onRequest?.({ method: req.method ?? 'GET', path: req.url ?? '/', status: upstream.status });
    if (!upstream.body) return void res.end();
    // Decode once for policy-controlled capture and the first-output mark.
    // A short tail carries a first-output match across chunk boundaries.
    const decoder = capture || firstToken ? new TextDecoder() : undefined;
    let tail = '';
    for await (const chunk of upstream.body as unknown as AsyncIterable<Uint8Array>) {
      if (decoder && (capture || firstToken)) {
        const text = decoder.decode(chunk, { stream: true });
        if (capture && responseBody.length < captureLimit) responseBody += text.slice(0, captureLimit - responseBody.length);
        if (firstToken) {
          const output = tail + text;
          if (FIRST_OUTPUT.test(output)) {
            firstToken();
            firstToken = undefined;
          } else tail = output.slice(-512);
        }
      }
      res.write(chunk);
    }
    if (capture && decoder) responseBody += decoder.decode().slice(0, captureLimit - responseBody.length);
    res.end();
  }
  await new Promise<void>((ok) => server.listen(0, '127.0.0.1', ok));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    port,
    close: () =>
      new Promise<void>((done) => {
        server.closeAllConnections?.();
        server.close(() => done());
      }),
  };
}

function parseBody(text: string): unknown { try { return JSON.parse(text); } catch { return undefined; } }

// ---------------------------------------------------------------------------
// gatewayHandler: the VM path (docs/workspaces.md §5.5)

/**
 * A request the handler refuses or fails, with one of the fixed codes the
 * VM may see (`GatewayErrorCode`). The message is for the Function's log
 * only: the model tunnel sends the code alone back to the VM.
 */
export class GatewayError extends Error {
  constructor(
    readonly code: GatewayErrorCode,
    message: string,
    /** The cap a `budget` refusal hit, so the harness can end the turn saying which. */
    readonly cap?: GatewayCap
  ) {
    super(message);
    this.name = 'GatewayError';
  }
}

/** The caps `sandbox({ budget })` sets on model spend, and the concurrency cap. */
export type GatewayCap = 'turnUsd' | 'sessionUsd' | 'concurrency';

/** A provider-hosted tool an app can opt into (`claudeCode({ providerTools })`, §5.5 step 4). */
export type ProviderTool = 'web_search' | 'web_fetch';

/**
 * What a route's body policy may check and say. `model` checks the request's
 * model id against `{ model, smallModel }`; `providerTool` checks an opt-in;
 * `clampTokens` clamps an output-token limit; `fetches` checks a content
 * source the provider would fetch itself (an image or file by URL), allowed
 * only when the VM's network is `allow-all` (under `deny-all` or an allow
 * list it would be a way out through the Gateway).
 */
export interface BodyCheck {
  model(id: unknown): string;
  providerTool(tool: ProviderTool): void;
  clampTokens(value: unknown): number;
  fetches(what: string): void;
}

/**
 * A route's body policy: validates the parsed request body (a plain JSON
 * object, duplicate keys already refused) and returns what to forward,
 * re-serialized by the handler. Throws `GatewayError` (`bad_request` or
 * `forbidden`) to refuse.
 */
export type BodyPolicy = (body: Record<string, unknown>, check: BodyCheck) => Record<string, unknown>;

/** One entry of a harness's fixed table: an exact method and pathname, never a pattern. */
export interface GatewayRoute {
  method: 'GET' | 'HEAD' | 'POST';
  /** The exact pathname, also the upstream pathname. */
  path: string;
  /** Exact query strings allowed besides none (`beta=true`); anything else is refused. */
  query?: readonly string[];
  /** The body policy (`POST`); a route without one takes no body. */
  body?: BodyPolicy;
  /** Answered by the handler itself with this status and no body, never forwarded (Claude's connectivity check). */
  local?: number;
  /** Request headers passed through (lowercase), besides `content-type` and `accept`. */
  headers?: readonly string[];
  /** For `anthropic-beta`: the values passed through; others are dropped. */
  betas?: ReadonlySet<string>;
  /** A route the Gateway doesn't bill (token counting): a response without a cost is charged nothing. */
  free?: boolean;
  /**
   * The request header that names the model, for a protocol that sends it
   * there and not in the body (AI Gateway's language-model protocol,
   * `ai-language-model-id`): checked against the session's models as a
   * body's `model` is. List it in `headers` too, so it goes upstream.
   */
  modelHeader?: string;
}

/** What a handler reads and records spend through: the totals live in the session's log, so they survive invocations and redeliveries. */
export interface GatewayLedger {
  /** Spend so far, in US dollars, this turn and over the session. */
  spent(): { turnUsd: number; sessionUsd: number };
  /**
   * Record one response's cost: the Gateway's own figure (P0.18), or, for a
   * response that ended without one (cut off, cancelled, a 2xx with no
   * metadata), an estimate at the handler's `ceiling` prices (`estimated`).
   */
  add(usd: number, info: { method: string; path: string; model?: string; estimated?: boolean }): void;
}

export interface GatewayHandlerOptions {
  /** The harness's fixed table (`CLAUDE_ROUTES`, `CODEX_ROUTES`, or a test's). */
  routes: readonly GatewayRoute[];
  /** The model ids the session may call: the app's `model` and `smallModel`. */
  models: { model: string; smallModel?: string };
  /** Caps from the session's descriptor (`sandbox({ budget })`); `Infinity` turns one off. Never defaulted here. */
  budget: { turnUsd: number; sessionUsd: number };
  /** Where spend is read and recorded. */
  ledger: GatewayLedger;
  /** Provider-hosted tools the app opted into. Default none. */
  providerTools?: readonly ProviderTool[];
  /** The VM's network: provider-hosted tools are refused under `deny-all`, whatever `providerTools` says. */
  network?: 'allow-all' | 'deny-all' | 'custom';
  /** Clamp output-token limits to this. Default 128 000. */
  maxOutputTokens?: number;
  /** Default `gatewayCredential`, called per request. */
  credential?: () => Promise<string>;
  /** Default `gatewayUrl()`. Only its origin is used. */
  target?: string;
  /** The headers that carry the credential. Default `authorization: Bearer` and `x-api-key`. */
  auth?: (credential: string) => Record<string, string>;
  /** As `gatewayProxy`'s: `ModelSteps.request`. */
  track?: (request: { method: string; path: string; model?: string }) => TrackEnd | void;
  /** Requests in flight at once. Default 8 (§5.5). */
  concurrency?: number;
  /**
   * The prices a response is charged at when the Gateway gave no cost for
   * it (the VM cancelled it, the stream was cut off, a 2xx had no metadata),
   * US dollars per million tokens: its tokens as the stream reported them
   * (`usage`), else estimated from the bytes (the request's at 3 bytes a
   * token, the response's at 8, at most the clamped output limit). A
   * ceiling, not a price list, so that cancelling every response before its
   * last event can't spend without limit. Default $15 in, $75 out.
   */
  ceiling?: { inputUsdPerMTok: number; outputUsdPerMTok: number };
  /** Where refusals and failures are described: the Function's log, never the VM. Default `console.warn`. */
  log?: (message: string, fields?: Record<string, unknown>) => void;
  /** Called when a cap refuses a request, so the harness ends the turn saying which. */
  onCap?: (cap: GatewayCap) => void;
  /** Test seam for the upstream call. Default the global `fetch`. */
  fetch?: typeof fetch;
}

/** The largest request body the handler buffers (§5.5 step 2). */
export const MODEL_BODY_MAX_BYTES = 4 << 20;

/** Response headers passed back to the VM. */
const RESPONSE_HEADERS = new Set(['content-type', 'cache-control', 'retry-after', 'x-should-retry', 'request-id', 'x-request-id']);

const ANTHROPIC_HEADERS = ['anthropic-version', 'anthropic-beta'] as const;
/**
 * The `anthropic-beta` values Claude Code may send on: what 2.1.296 sends
 * (P0.7, logged through the tunnel and checked against the Gateway:
 * docs/sandbox-probes.md), and the earlier pins' values. Any other value is
 * dropped, not refused, and logged.
 */
export const CLAUDE_BETAS: ReadonlySet<string> = new Set([
  'claude-code-20250219',
  'interleaved-thinking-2025-05-14',
  'thinking-token-count-2026-05-13',
  'prompt-caching-scope-2026-01-05',
  'thinking-display-updates-2026-08-18',
  'fine-grained-tool-streaming-2025-05-14',
  'context-management-2025-06-27',
  'token-efficient-tools-2025-02-19',
  'prompt-caching-2024-07-31',
  'context-1m-2025-08-07',
]);

/** Top-level keys a Messages request may carry: no `mcp_servers`, `container`, `service_tier` or provider options. */
const MESSAGES_KEYS = new Set(['model', 'messages', 'system', 'max_tokens', 'metadata', 'stop_sequences', 'stream', 'temperature', 'top_k', 'top_p', 'tools', 'tool_choice', 'thinking', 'context_management', 'output_config']);
const COUNT_TOKENS_KEYS = new Set(['model', 'messages', 'system', 'tools', 'tool_choice', 'thinking', 'context_management', 'output_config']);
/**
 * What `output_config` may carry: the effort, a structured output's format
 * and a task budget, as Claude Code 2.1.296 sends them (the effort on every
 * request to a model that takes one). None makes the provider fetch or run
 * anything.
 */
const OUTPUT_CONFIG_KEYS = new Set(['effort', 'format', 'task_budget']);
/** Top-level keys a Responses request may carry. */
const RESPONSES_KEYS = new Set(['model', 'instructions', 'input', 'tools', 'tool_choice', 'parallel_tool_calls', 'reasoning', 'store', 'stream', 'include', 'prompt_cache_key', 'text', 'max_output_tokens', 'temperature', 'top_p']);

const refuse = (code: 'bad_request' | 'forbidden', message: string) => new GatewayError(code, message);

function onlyKeys(body: Record<string, unknown>, keys: ReadonlySet<string>, what: string): void {
  for (const k of Object.keys(body)) if (!keys.has(k)) throw refuse('forbidden', `${what}: the key ${JSON.stringify(k.slice(0, 64))} is not allowed`);
}

/**
 * Anthropic tools: a client tool (no `type`, or `custom`) passes; `web_search_*`
 * and `web_fetch_*` only with the opt-in; every other server or Anthropic-defined
 * tool (`code_execution_*`, `bash_*`, `computer_*`, ...) is refused.
 */
function anthropicTools(tools: unknown, check: BodyCheck): void {
  if (tools === undefined) return;
  if (!Array.isArray(tools)) throw refuse('bad_request', 'tools must be an array');
  for (const tool of tools) {
    if (typeof tool !== 'object' || tool === null || Array.isArray(tool)) throw refuse('bad_request', 'a tool must be an object');
    const type = (tool as { type?: unknown }).type;
    if (type === undefined || type === 'custom') continue;
    if (typeof type === 'string' && /^web_search_\d{8}$/.test(type)) check.providerTool('web_search');
    else if (typeof type === 'string' && /^web_fetch_\d{8}$/.test(type)) check.providerTool('web_fetch');
    else throw refuse('forbidden', `the tool type ${JSON.stringify(String(type).slice(0, 64))} is not a client tool`);
  }
}

const isPlain = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** Content blocks the provider runs on its own (MCP, its containers, code execution): refused, whatever the network or the opt-ins. */
const ANTHROPIC_SERVER_BLOCKS = /^(?:mcp_tool_use|mcp_tool_result|container_upload|.*code_execution.*)$/;
/** The result blocks of a provider-hosted tool, which a conversation carries back once the tool is enabled. */
const ANTHROPIC_TOOL_RESULTS: Readonly<Record<string, ProviderTool>> = { web_search_tool_result: 'web_search', web_fetch_tool_result: 'web_fetch' };

/** An image's or a document's `source`: inline bytes or text pass; a URL only with `fetches`; a Files API reference never. */
function anthropicSource(source: unknown, check: BodyCheck): void {
  if (!isPlain(source)) return;
  if (source.type === 'url') check.fetches('a content block with a url source');
  else if (source.type === 'file') throw refuse('forbidden', 'a content block with a file source (the Files API) is not allowed');
}

/**
 * Walk a message's content (or `system`, a tool result's content, a
 * document's): a block the provider would run or fetch on its own is
 * refused, as `anthropicTools` refuses the tool. A tool call's `input` is the
 * model's own arguments and is not walked.
 */
function anthropicBlocks(content: unknown, check: BodyCheck, where: string): void {
  if (content === undefined || typeof content === 'string') return;
  if (!Array.isArray(content)) throw refuse('bad_request', `${where} must be a string or an array of blocks`);
  for (const block of content) {
    if (!isPlain(block) || typeof block.type !== 'string') throw refuse('bad_request', `a block in ${where} must be an object with a type`);
    const type = block.type;
    if (ANTHROPIC_SERVER_BLOCKS.test(type)) throw refuse('forbidden', `the content block type ${JSON.stringify(type.slice(0, 64))} runs on the provider`);
    if (type === 'server_tool_use') {
      if (block.name === 'web_search' || block.name === 'web_fetch') check.providerTool(block.name);
      else throw refuse('forbidden', `the server tool ${JSON.stringify(String(block.name).slice(0, 64))} is not allowed`);
    } else if (Object.hasOwn(ANTHROPIC_TOOL_RESULTS, type)) check.providerTool(ANTHROPIC_TOOL_RESULTS[type]!);
    if (type === 'image' || type === 'document') anthropicSource(block.source, check);
    if (type === 'document' && isPlain(block.source) && block.source.type === 'content') anthropicBlocks(block.source.content, check, "a document's content");
    if (type === 'tool_result') anthropicBlocks(block.content, check, "a tool result's content");
  }
}

function anthropicConversation(body: Record<string, unknown>, check: BodyCheck): void {
  if (!Array.isArray(body.messages)) throw refuse('bad_request', 'messages must be an array');
  for (const m of body.messages) {
    if (!isPlain(m)) throw refuse('bad_request', 'a message must be an object');
    anthropicBlocks(m.content, check, "a message's content");
  }
  anthropicBlocks(body.system, check, 'system');
}

/**
 * Extended thinking's budget must stay below `max_tokens`, which the handler
 * may have clamped: a budget at or past it is lowered to just under it (the
 * provider refuses below 1024, so a clamp that leaves less is a refusal).
 */
function anthropicThinking(thinking: unknown, maxTokens: number): unknown {
  if (!isPlain(thinking) || thinking.type !== 'enabled') return thinking;
  const budget = thinking.budget_tokens;
  if (typeof budget !== 'number' || !Number.isSafeInteger(budget) || budget <= 0) throw refuse('bad_request', 'thinking.budget_tokens must be a positive integer');
  if (budget < maxTokens) return thinking;
  if (maxTokens <= 1024) throw refuse('bad_request', `thinking.budget_tokens must be below max_tokens (${maxTokens} once clamped) and at least 1024`);
  return { ...thinking, budget_tokens: maxTokens - 1 };
}

function anthropicOutputConfig(config: unknown): void {
  if (config === undefined) return;
  if (!isPlain(config)) throw refuse('bad_request', 'output_config must be an object');
  onlyKeys(config, OUTPUT_CONFIG_KEYS, 'output_config');
  if (config.effort !== undefined && typeof config.effort !== 'string') throw refuse('bad_request', 'output_config.effort must be a string');
  for (const k of ['format', 'task_budget']) if (config[k] !== undefined && !isPlain(config[k])) throw refuse('bad_request', `output_config.${k} must be an object`);
}

/** Claude's `POST /v1/messages`. */
export const anthropicMessages: BodyPolicy = (body, check) => {
  onlyKeys(body, MESSAGES_KEYS, 'messages');
  anthropicOutputConfig(body.output_config);
  anthropicConversation(body, check);
  anthropicTools(body.tools, check);
  const max = check.clampTokens(body.max_tokens);
  return { ...body, model: check.model(body.model), max_tokens: max, ...(body.thinking !== undefined ? { thinking: anthropicThinking(body.thinking, max) } : {}) };
};

/** Claude's `POST /v1/messages/count_tokens`. */
export const anthropicCountTokens: BodyPolicy = (body, check) => {
  onlyKeys(body, COUNT_TOKENS_KEYS, 'count_tokens');
  anthropicOutputConfig(body.output_config);
  anthropicConversation(body, check);
  anthropicTools(body.tools, check);
  return { ...body, model: check.model(body.model) };
};

/** Responses tools: `function`, `custom` and `local_shell` pass; `web_search*` only with the opt-in; `mcp`, `code_interpreter`, `file_search`, ... are refused. */
function responsesTools(tools: unknown, check: BodyCheck): void {
  if (tools === undefined) return;
  if (!Array.isArray(tools)) throw refuse('bad_request', 'tools must be an array');
  for (const tool of tools) {
    if (typeof tool !== 'object' || tool === null || Array.isArray(tool)) throw refuse('bad_request', 'a tool must be an object');
    const type = (tool as { type?: unknown }).type;
    if (type === 'function' || type === 'custom' || type === 'local_shell') continue;
    if (type === 'web_search' || type === 'web_search_preview') check.providerTool('web_search');
    else throw refuse('forbidden', `the tool type ${JSON.stringify(String(type).slice(0, 64))} is not a client tool`);
  }
}

/** Responses input items the provider runs on its own: refused, whatever the network or the opt-ins. */
const RESPONSES_SERVER_ITEMS = /^(?:mcp_.*|code_interpreter_call|file_search_call|image_generation_call|computer_call|computer_call_output)$/;

/** A message's content parts (or a tool output's): an image or a file by URL only with `fetches`, by inline data always, by a Files API id never. */
function responsesContent(content: unknown, check: BodyCheck): void {
  if (content === undefined || typeof content === 'string') return;
  if (!Array.isArray(content)) throw refuse('bad_request', 'content must be a string or an array of parts');
  for (const part of content) {
    if (!isPlain(part)) throw refuse('bad_request', 'a content part must be an object');
    if (part.file_id !== undefined) throw refuse('forbidden', 'a file_id reference (the Files API) is not allowed');
    if (part.type === 'input_image' && part.image_url !== undefined) {
      if (typeof part.image_url !== 'string') throw refuse('bad_request', 'image_url must be a string');
      if (!part.image_url.startsWith('data:')) check.fetches('an input_image by URL');
    }
    if (part.type === 'input_file' && part.file_url !== undefined) check.fetches('an input_file by URL');
  }
}

/** Walk `input`: items the provider runs refused, `web_search_call` only with the opt-in, every message's and tool output's parts checked. */
function responsesInput(input: unknown, check: BodyCheck): void {
  if (input === undefined || typeof input === 'string') return;
  if (!Array.isArray(input)) throw refuse('bad_request', 'input must be a string or an array of items');
  for (const item of input) {
    if (!isPlain(item)) throw refuse('bad_request', 'an input item must be an object');
    const type = item.type;
    if (typeof type === 'string' && RESPONSES_SERVER_ITEMS.test(type)) throw refuse('forbidden', `the input item type ${JSON.stringify(type.slice(0, 64))} runs on the provider`);
    if (type === 'web_search_call') check.providerTool('web_search');
    if (type === undefined || type === 'message') responsesContent(item.content, check);
    if (Array.isArray(item.output)) responsesContent(item.output, check);
  }
}

/**
 * What Codex 0.160 puts in a Responses body that the provider doesn't need,
 * dropped rather than refused (P0.7, A6): `client_metadata`, its own ids for
 * the installation, thread, turn and window, and the turn's sandbox settings.
 */
const RESPONSES_DROPPED = new Set(['client_metadata']);

/**
 * Codex's `POST /v1/responses`. Nothing is stored on the shared Gateway
 * account (`store: false`, whatever the caller sent), and the output limit is
 * always set, clamped (Codex sends none, so the clamp would otherwise never
 * apply).
 */
export const openaiResponses: BodyPolicy = (raw, check) => {
  const body = Object.fromEntries(Object.entries(raw).filter(([k]) => !RESPONSES_DROPPED.has(k)));
  onlyKeys(body, RESPONSES_KEYS, 'responses');
  responsesInput(body.input, check);
  responsesTools(body.tools, check);
  return { ...body, model: check.model(body.model), store: false, max_output_tokens: check.clampTokens(body.max_output_tokens ?? Number.MAX_SAFE_INTEGER) };
};

/**
 * Top-level keys of an AI Gateway language-model call (the AI SDK's call
 * options, as libfx's kernel sends them): no call-level `providerOptions`
 * (the Gateway's routing and fallbacks, a provider's MCP servers or
 * containers), no `headers`.
 */
const LANGUAGE_MODEL_KEYS = new Set([
  'prompt', 'tools', 'toolChoice', 'maxOutputTokens', 'temperature', 'stopSequences', 'topP', 'topK',
  'presencePenalty', 'frequencyPenalty', 'responseFormat', 'seed', 'includeRawChunks',
]);

/** A language-model call's tools: `function` tools only; a provider-defined tool runs on the provider and is refused. */
function languageModelTools(tools: unknown): void {
  if (tools === undefined) return;
  if (!Array.isArray(tools)) throw refuse('bad_request', 'tools must be an array');
  for (const tool of tools) {
    if (!isPlain(tool)) throw refuse('bad_request', 'a tool must be an object');
    if (tool.type !== 'function') throw refuse('forbidden', `the tool type ${JSON.stringify(String(tool.type).slice(0, 64))} is not a client tool`);
  }
}

/**
 * Provider options on a prompt message or part: what a provider needs back
 * (a reasoning part's signature, a cache breakpoint), never the Gateway's
 * own (`gateway`: routing, fallbacks).
 */
function partOptions(options: unknown, where: string): void {
  if (options === undefined) return;
  if (!isPlain(options)) throw refuse('bad_request', `provider options on ${where} must be an object`);
  if (Object.hasOwn(options, 'gateway')) throw refuse('forbidden', `Gateway options on ${where} are not allowed`);
}

/**
 * A language-model call's prompt: messages with a role, their parts
 * objects; a file part whose `data` is a URL the provider would fetch
 * passes only with `fetches`, and no message or part carries Gateway options.
 */
function languageModelPrompt(prompt: unknown, check: BodyCheck): void {
  if (!Array.isArray(prompt)) throw refuse('bad_request', 'prompt must be an array of messages');
  for (const m of prompt) {
    if (!isPlain(m) || typeof m.role !== 'string') throw refuse('bad_request', 'a prompt message must be an object with a role');
    partOptions(m.providerOptions, 'a prompt message');
    if (typeof m.content === 'string' || m.content === undefined) continue;
    if (!Array.isArray(m.content)) throw refuse('bad_request', "a prompt message's content must be a string or an array of parts");
    for (const part of m.content) {
      if (!isPlain(part) || typeof part.type !== 'string') throw refuse('bad_request', 'a content part must be an object with a type');
      partOptions(part.providerOptions, 'a content part');
      if (part.type === 'file' && typeof part.data === 'string' && /^[a-z][a-z0-9+.-]*:\/\//i.test(part.data)) {
        check.fetches('a file part by URL');
      }
    }
  }
}

/**
 * AI Gateway's language-model call (`POST /v4/ai/language-model`, what
 * libfx's kernel and the AI SDK's Gateway provider send; the model is in
 * the `ai-language-model-id` header, `GatewayRoute.modelHeader`). An output
 * limit, when there is one, is clamped.
 */
export const aiGatewayLanguageModel: BodyPolicy = (body, check) => {
  onlyKeys(body, LANGUAGE_MODEL_KEYS, 'language-model');
  languageModelPrompt(body.prompt, check);
  languageModelTools(body.tools);
  return { ...body, ...(body.maxOutputTokens !== undefined ? { maxOutputTokens: check.clampTokens(body.maxOutputTokens) } : {}) };
};

/** AI Gateway's language-model route: the body policy above, the model in a header, the protocol's headers passed on. */
export const AI_GATEWAY_LANGUAGE_MODEL_ROUTE: GatewayRoute = Object.freeze({
  method: 'POST',
  path: '/v4/ai/language-model',
  body: aiGatewayLanguageModel,
  modelHeader: 'ai-language-model-id',
  headers: Object.freeze([
    'ai-language-model-id',
    'ai-language-model-specification-version',
    'ai-language-model-streaming',
    'ai-gateway-protocol-version',
    'x-session-affinity',
  ]),
});

/** Claude Code's fixed table (§5.5 step 1; P0.7 extends it from the tunnel's log only). */
export const CLAUDE_ROUTES: readonly GatewayRoute[] = Object.freeze([
  { method: 'HEAD', path: '/api/hello', local: 200 },
  { method: 'POST', path: '/v1/messages', query: ['beta=true'], body: anthropicMessages, headers: ANTHROPIC_HEADERS, betas: CLAUDE_BETAS },
  { method: 'POST', path: '/v1/messages/count_tokens', query: ['beta=true'], body: anthropicCountTokens, headers: ANTHROPIC_HEADERS, betas: CLAUDE_BETAS, free: true },
]);

/**
 * Codex's fixed table (§5.5 step 1). P0.7 (A6, the real 0.160 through the
 * tunnel): `POST /v1/responses` with no query is all it sends, streamed. Of
 * its headers only `originator` and `x-client-request-id` go upstream; its
 * `session-id`, `thread-id`, `x-codex-window-id`, `x-codex-turn-metadata`
 * (its installation and turn ids, the sandbox mode) and
 * `x-codex-beta-features` stay in the Function.
 */
export const CODEX_ROUTES: readonly GatewayRoute[] = Object.freeze([{ method: 'POST', path: '/v1/responses', body: openaiResponses, headers: ['originator', 'x-client-request-id'] }]);

/** Path text the handler never matches, whatever the table says: anything that could steer URL parsing. */
const UNSAFE_PATH = /[^A-Za-z0-9/_.\-?=&]|\.\.|\/\/|^[^/]|\?.*\?/;

/**
 * Match the VM's method and path against the table. The path is only ever
 * compared with the table's text, never parsed as a URL: an absolute form, a
 * `//host`, `@host`, `:port`, `..`, any `%` escape, an empty path or a query
 * the table doesn't list is refused.
 */
export function matchRoute(routes: readonly GatewayRoute[], method: string, path: string): { route: GatewayRoute; query: string } {
  if (typeof path !== 'string' || path.length === 0 || path.length > 2048 || UNSAFE_PATH.test(path)) throw refuse('forbidden', 'the path is not one the table can match');
  const at = path.indexOf('?');
  const pathname = at < 0 ? path : path.slice(0, at);
  const query = at < 0 ? '' : path.slice(at + 1);
  const route = routes.find((r) => r.method === method && r.path === pathname);
  if (!route) throw refuse('forbidden', `${method.slice(0, 16)} ${pathname.slice(0, 128)} is not in the table`);
  if (query !== '' && !route.query?.includes(query)) throw refuse('forbidden', `the query on ${pathname} is not in the table`);
  return { route, query };
}

/**
 * The upstream URL for a matched route: the table's path on the target's
 * origin, never text from the frame. Asserts the origin and the path before
 * the credential is added (§5.5 step 1).
 */
export function upstreamUrl(target: string, route: GatewayRoute, query: string): URL {
  const base = new URL(target);
  const url = new URL(route.path, base.origin);
  if (query) url.search = `?${query}`;
  if (url.origin !== base.origin || url.pathname !== route.path) throw new GatewayError('internal', 'the upstream URL left the target');
  return url;
}

/**
 * Parse JSON strictly: UTF-8 (fatal), one value, no duplicate object keys at
 * any depth (`{"model":"cheap","model":"opus"}` is refused, so a check on one
 * value can't pass while upstream reads the other), at most 64 levels deep.
 */
export function parseStrictJson(bytes: Uint8Array): unknown {
  let text: string;
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    throw refuse('bad_request', 'the body is not UTF-8');
  }
  let i = 0;
  const fail = (why: string): never => {
    throw refuse('bad_request', `the body is not strict JSON: ${why} at ${i}`);
  };
  const ws = () => {
    while (i < text.length && (text[i] === ' ' || text[i] === '\t' || text[i] === '\n' || text[i] === '\r')) i++;
  };
  const str = (): string => {
    const start = i;
    i++;
    for (;;) {
      if (i >= text.length) fail('an unterminated string');
      const c = text.charCodeAt(i);
      if (c === 0x22) break;
      if (c < 0x20) fail('a control character in a string');
      if (c === 0x5c) i++;
      i++;
    }
    i++;
    try {
      return JSON.parse(text.slice(start, i)) as string;
    } catch {
      return fail('a bad escape');
    }
  };
  const value = (depth: number): unknown => {
    if (depth > 64) fail('too deep');
    ws();
    const c = text[i];
    if (c === '{') {
      i++;
      const out: Record<string, unknown> = {};
      const seen = new Set<string>();
      ws();
      if (text[i] === '}') return i++, out;
      for (;;) {
        ws();
        if (text[i] !== '"') fail('a key that is not a string');
        const key = str();
        if (seen.has(key)) fail(`a duplicate key ${JSON.stringify(key.slice(0, 64))}`);
        seen.add(key);
        ws();
        if (text[i] !== ':') fail('a missing colon');
        i++;
        Object.defineProperty(out, key, { value: value(depth + 1), enumerable: true, writable: true, configurable: true });
        ws();
        if (text[i] === ',') {
          i++;
          continue;
        }
        if (text[i] === '}') return i++, out;
        fail('a missing comma');
      }
    }
    if (c === '[') {
      i++;
      const out: unknown[] = [];
      ws();
      if (text[i] === ']') return i++, out;
      for (;;) {
        out.push(value(depth + 1));
        ws();
        if (text[i] === ',') {
          i++;
          continue;
        }
        if (text[i] === ']') return i++, out;
        fail('a missing comma');
      }
    }
    if (c === '"') return str();
    const m = /^(?:-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)/.exec(text.slice(i, i + 64));
    if (!m) return fail('an unexpected character');
    i += m[0].length;
    return JSON.parse(m[0]) as unknown;
  };
  const out = value(0);
  ws();
  if (i !== text.length) fail('trailing text');
  return out;
}

/** The Gateway's own cost figure for a response (P0.18: `provider_metadata.gateway`, in the JSON body or the stream's last events). */
const GATEWAY_COST = /"gatewayCost"\s*:\s*"?(\d+(?:\.\d+)?)"?/g;
const PLAIN_COST = /"gateway"\s*:\s*\{[^]*?"cost"\s*:\s*"?(\d+(?:\.\d+)?)"?/g;
function costOf(text: string): number | undefined {
  const last = (re: RegExp) => {
    let found: number | undefined;
    for (const m of text.matchAll(re)) found = Number(m[1]);
    return found;
  };
  const usd = last(GATEWAY_COST) ?? last(PLAIN_COST);
  return usd !== undefined && Number.isFinite(usd) && usd >= 0 ? usd : undefined;
}

/** What a response showed of itself: its first and last text (for `usage` and the cost), and how many bytes streamed. */
interface Seen {
  head: string;
  window: string;
  streamed: number;
}

/** The largest count a `usage` field shows anywhere in `text` (0 for none). */
function usageOf(text: string, field: string): number {
  let max = 0;
  for (const m of text.matchAll(new RegExp(`"${field}"\\s*:\\s*(\\d{1,12})`, 'g'))) max = Math.max(max, Number(m[1]));
  return max;
}

/**
 * The charge for a response the Gateway gave no cost for, at the ceiling
 * prices: the tokens its `usage` showed (Anthropic's `message_start` has the
 * input's at once), else estimates from the bytes (the request's at 3 a
 * token, the response's at 8, capped at the clamped output limit). Cache
 * writes at 1.25 times the input price, reads at 0.1.
 */
function estimateUsd(seen: Seen, requestBytes: number, maxTokens: number | undefined, ceiling: { inputUsdPerMTok: number; outputUsdPerMTok: number }): number {
  const text = `${seen.head}\n${seen.window}`;
  const input = usageOf(text, 'input_tokens') || Math.ceil(requestBytes / 3);
  const cacheWrite = usageOf(text, 'cache_creation_input_tokens');
  const cacheRead = usageOf(text, 'cache_read_input_tokens');
  const output = Math.max(usageOf(text, 'output_tokens'), Math.min(Math.ceil(seen.streamed / 8), maxTokens ?? Infinity));
  return (input * ceiling.inputUsdPerMTok + cacheWrite * ceiling.inputUsdPerMTok * 1.25 + cacheRead * ceiling.inputUsdPerMTok * 0.1 + output * ceiling.outputUsdPerMTok) / 1e6;
}

/** The error `type` the VM gets for an upstream status (Anthropic's names; OpenAI clients read the same `error.type`). */
function errorType(status: number): string {
  if (status === 400) return 'invalid_request_error';
  if (status === 401) return 'authentication_error';
  if (status === 402) return 'billing_error';
  if (status === 403) return 'permission_error';
  if (status === 404) return 'not_found_error';
  if (status === 413) return 'request_too_large';
  if (status === 429) return 'rate_limit_error';
  if (status === 529) return 'overloaded_error';
  return 'api_error';
}

/** The error types whose message the VM may read: the provider's own complaint about the request (Claude Code compacts on "prompt is too long"), never the Gateway's about the credential, the team or billing. */
const REQUEST_ERRORS = new Set(['invalid_request_error', 'request_too_large']);

/**
 * An upstream error, rebuilt for the VM: `{ type: 'error', error: { type,
 * message?, code? } }`, the type from the status; the message (at most 500
 * characters) and an identifier-like `code` (OpenAI's
 * `context_length_exceeded`, which Codex reads) only for a request error the
 * provider reported. Everything else in the Gateway's body (it can describe
 * the Function's credential, team or billing) stays in the Function's log.
 */
function errorBody(status: number, text: string): string {
  const type = errorType(status);
  let message: string | undefined;
  let code: string | undefined;
  try {
    const parsed = JSON.parse(text) as { error?: { type?: unknown; message?: unknown; code?: unknown } };
    const e = parsed?.error;
    if (e && typeof e.type === 'string' && REQUEST_ERRORS.has(e.type)) {
      if (typeof e.message === 'string') message = e.message.slice(0, 500);
      if (typeof e.code === 'string' && /^[a-z][a-z0-9_]{0,63}$/.test(e.code)) code = e.code;
    }
  } catch {}
  return JSON.stringify({ type: 'error', error: { type, ...(message !== undefined ? { message } : {}), ...(code !== undefined ? { code } : {}) } });
}

/** The most of an upstream error body the handler reads. */
const ERROR_BODY_MAX = 64 * 1024;

async function readCapped(stream: ReadableStream<Uint8Array> | null, max: number): Promise<string> {
  if (!stream) return '';
  const decoder = new TextDecoder();
  let text = '';
  const reader = stream.getReader();
  try {
    while (text.length < max) {
      const { done, value } = await reader.read();
      if (done) break;
      text += decoder.decode(value, { stream: true });
    }
  } finally {
    void reader.cancel().catch(() => {});
  }
  return text.slice(0, max);
}

/**
 * The driver's side of the VM's model port (docs/workspaces.md §5.5, A3):
 * every relayed request, in order, before anything is sent anywhere:
 *
 * 1. the exact method and pathname against the harness's fixed table, and
 *    the upstream URL built from the table, its origin asserted;
 * 2. the body buffered (at most 4 MiB), parsed strictly (duplicate keys
 *    refused), checked by the route's policy (its keys, its tools, every
 *    content block the provider would run or fetch) and re-serialized;
 * 3. the model against `{ model, smallModel }`;
 * 4. provider-hosted tools only with the opt-in and a network;
 * 5. request headers from the route's allowlist, response headers from a
 *    fixed one, and an upstream error's body rebuilt (`errorBody`);
 * 6. at most `concurrency` in flight, and spend under the descriptor's caps
 *    (read from `ledger`, which keeps them in the log; each response's cost,
 *    the Gateway's own figure, is added once it ends, and one that ends
 *    without it, cancelled or cut off, is charged at the `ceiling`);
 * 7. the credential read per request and added last.
 *
 * A request's slot is released, and its step ended and its spend settled,
 * exactly once, on whichever comes first: the body read to its end or
 * dropped, the request's `signal` aborting (also when the response is never
 * read), or a failure.
 *
 * A refusal rejects with a `GatewayError` (`bad_request`, `forbidden`,
 * `budget`, `busy`); an upstream failure with `upstream`. The detail goes to
 * `log`.
 */
export function gatewayHandler(options: GatewayHandlerOptions): GatewayHandler {
  const { routes, ledger } = options;
  if (!routes?.length) throw new TypeError('harness-world-process gatewayHandler: `routes` (the harness\'s fixed table) is required');
  const budget = options.budget;
  for (const cap of ['turnUsd', 'sessionUsd'] as const) {
    const v = budget?.[cap];
    if (typeof v !== 'number' || Number.isNaN(v) || v < 0) throw new TypeError(`harness-world-process gatewayHandler: budget.${cap} must be a number (from sandbox({ budget }))`);
  }
  if (typeof options.models?.model !== 'string' || !options.models.model) throw new TypeError('harness-world-process gatewayHandler: `models.model` is required');
  const allowed = new Set([options.models.model, ...(options.models.smallModel ? [options.models.smallModel] : [])]);
  const providerTools = new Set(options.network === 'deny-all' ? [] : (options.providerTools ?? []));
  const maxTokens = options.maxOutputTokens ?? 128_000;
  const credential = options.credential ?? gatewayCredential;
  const target = options.target ?? gatewayUrl();
  const auth = options.auth ?? ((c: string) => ({ authorization: `Bearer ${c}`, 'x-api-key': c }));
  const limit = options.concurrency ?? 8;
  const ceiling = options.ceiling ?? { inputUsdPerMTok: 15, outputUsdPerMTok: 75 };
  const log = options.log ?? ((m: string, f?: Record<string, unknown>) => console.warn(`[gatewayHandler] ${m}`, f ?? {}));
  const doFetch = options.fetch ?? fetch;
  let inflight = 0;

  const check: BodyCheck = {
    model(id) {
      if (typeof id !== 'string' || !allowed.has(id)) throw refuse('forbidden', `the model ${JSON.stringify(String(id).slice(0, 128))} is not this session's`);
      return id;
    },
    providerTool(tool) {
      if (!providerTools.has(tool)) throw refuse('forbidden', `the provider-hosted tool ${tool} is not enabled${options.network === 'deny-all' ? " (network: 'deny-all')" : ''}`);
    },
    clampTokens(value) {
      if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) throw refuse('bad_request', 'an output token limit must be a positive integer');
      return Math.min(value, maxTokens);
    },
    fetches(what) {
      if (options.network !== 'allow-all') throw refuse('forbidden', `${what} makes the provider fetch it, which the session's network (${options.network ?? 'unset'}) doesn't allow`);
    },
  };

  const capped = (cap: GatewayCap, message: string) => {
    options.onCap?.(cap);
    return new GatewayError(cap === 'concurrency' ? 'busy' : 'budget', message, cap);
  };

  return async (request, signal) => {
    /** Set once the request holds a slot: releases it (and ends its step, and settles its spend) exactly once. */
    let release: ((outcome: { status?: number; error?: string }) => void) | undefined;
    try {
      const method = String(request.method);
      const { route, query } = matchRoute(routes, method, request.path);
      const body = request.body ?? new Uint8Array();
      if (route.local !== undefined) {
        if (body.length) throw refuse('bad_request', `${route.path} takes no body`);
        return { status: route.local, headers: {}, body: (async function* () {})() };
      }
      if (body.length > MODEL_BODY_MAX_BYTES) throw refuse('bad_request', 'the body is over 4 MiB');
      let forward: string | undefined;
      let model: string | undefined;
      let outputLimit: number | undefined;
      if (route.body) {
        const parsed = parseStrictJson(body);
        if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) throw refuse('bad_request', 'the body must be a JSON object');
        const out = route.body(parsed as Record<string, unknown>, check);
        model = typeof out.model === 'string' ? out.model : undefined;
        const limitOf = out.max_tokens ?? out.max_output_tokens;
        outputLimit = typeof limitOf === 'number' ? limitOf : undefined;
        forward = JSON.stringify(out);
      } else if (body.length) throw refuse('bad_request', `${route.path} takes no body`);
      if (route.modelHeader) {
        const named = Object.entries(request.headers).find(([k]) => k.toLowerCase() === route.modelHeader)?.[1];
        model = check.model(named);
      }
      const spent = ledger.spent();
      if (!(spent.turnUsd < budget.turnUsd)) throw capped('turnUsd', `this turn's model spend reached its cap ($${budget.turnUsd})`);
      if (!(spent.sessionUsd < budget.sessionUsd)) throw capped('sessionUsd', `this session's model spend reached its cap ($${budget.sessionUsd})`);
      if (inflight >= limit) throw capped('concurrency', `more than ${limit} model requests at once`);
      const url = upstreamUrl(target, route, query);
      const headers = new Headers({ 'content-type': 'application/json' });
      const lower = Object.fromEntries(Object.entries(request.headers).map(([k, v]) => [k.toLowerCase(), String(v)]));
      if (lower.accept) headers.set('accept', lower.accept);
      for (const name of route.headers ?? []) {
        const v = lower[name];
        if (v === undefined) continue;
        if (name === 'anthropic-beta') {
          const betas = v.split(',').map((b) => b.trim()).filter(Boolean);
          const kept = betas.filter((b) => route.betas?.has(b));
          if (kept.length < betas.length) log('dropped anthropic-beta values', { dropped: betas.filter((b) => !route.betas?.has(b)).map((b) => b.slice(0, 64)) });
          if (kept.length) headers.set(name, kept.join(','));
        } else headers.set(name, v);
      }

      inflight++;
      const info = { method, path: route.path, ...(model ? { model } : {}) };
      const end = options.track?.(info) || undefined;
      const billed = !!route.body && !route.free;
      const requestBytes = forward === undefined ? 0 : Buffer.byteLength(forward);
      const seen: Seen = { head: '', window: '', streamed: 0 };
      /** The request left for the Gateway (it may bill from here on). */
      let sent = false;
      /** The Gateway answered 2xx. */
      let ok = false;
      let upstreamBody: ReadableStream<Uint8Array> | null = null;
      let done = false;
      const onAbort = () => {
        release?.({ error: 'the request was cancelled' });
        void upstreamBody?.cancel().catch(() => {});
      };
      release = (outcome) => {
        if (done) return;
        done = true;
        inflight--;
        signal.removeEventListener('abort', onAbort);
        // This can run from the signal's 'abort' listener, where a throw would be an uncaught exception: the ledger's and track's failures are logged.
        try {
          // Charged when the Gateway may have billed it: a 2xx, or a request the VM cancelled once it was sent.
          if (billed && sent && (ok || signal.aborted)) {
            const usd = costOf(seen.window);
            if (usd !== undefined) ledger.add(usd, info);
            else {
              const estimate = estimateUsd(seen, requestBytes, outputLimit, ceiling);
              log('a response with no Gateway cost: charged an estimate', { path: route.path, usd: Number(estimate.toFixed(6)), ...outcome });
              ledger.add(estimate, { ...info, estimated: true });
            }
          }
        } catch (err) {
          log('the ledger refused a charge', { path: route.path, error: String((err as Error)?.message ?? err).slice(0, 200) });
        }
        try {
          end?.(outcome);
        } catch (err) {
          log("the model step's end failed", { error: String((err as Error)?.message ?? err).slice(0, 200) });
        }
      };
      signal.addEventListener('abort', onAbort, { once: true });
      // The credential last: after the URL is fixed and checked.
      for (const [k, v] of Object.entries(auth(await credential()))) headers.set(k, v);
      if (signal.aborted) throw new GatewayError('upstream', 'the request was cancelled');
      let upstream: Response;
      try {
        sent = true;
        upstream = await doFetch(url, { method, headers, ...(forward !== undefined ? { body: forward } : {}), signal, redirect: 'manual' });
      } catch (err) {
        throw new GatewayError('upstream', `the Gateway call failed: ${(err as Error)?.message ?? err}`);
      }
      ok = upstream.ok;
      upstreamBody = upstream.body;
      const out: Record<string, string> = {};
      upstream.headers.forEach((v, k) => {
        if (RESPONSE_HEADERS.has(k)) out[k] = v;
      });
      if (signal.aborted) throw new GatewayError('upstream', 'the request was cancelled');
      if (upstream.status >= 400) {
        // The Gateway's own words stay here: the VM gets the status and a fixed type.
        const text = await readCapped(upstream.body, ERROR_BODY_MAX);
        log('the Gateway answered an error', { path: route.path, status: upstream.status, body: text.slice(0, 500) });
        release({ status: upstream.status });
        const rebuilt = new TextEncoder().encode(errorBody(upstream.status, text));
        return {
          status: upstream.status,
          headers: { ...out, 'content-type': 'application/json' },
          body: (async function* () {
            yield rebuilt;
          })(),
        };
      }
      const stream = upstream.body;
      if (!stream) {
        release({ status: upstream.status });
        return { status: upstream.status, headers: out, body: (async function* () {})() };
      }
      const finish = release;
      return {
        status: upstream.status,
        headers: out,
        body: (async function* () {
          const decoder = new TextDecoder();
          let firstToken = end ? end.firstToken : undefined;
          let complete = false;
          try {
            for await (const chunk of stream as unknown as AsyncIterable<Uint8Array>) {
              const text = decoder.decode(chunk, { stream: true });
              seen.streamed += chunk.length;
              if (seen.head.length < 16_384) seen.head += text.slice(0, 16_384 - seen.head.length);
              seen.window = (seen.window + text).slice(-65_536);
              if (firstToken && FIRST_OUTPUT.test(seen.window.slice(-4096))) {
                firstToken();
                firstToken = undefined;
              }
              yield chunk;
            }
            complete = true;
          } finally {
            finish(complete ? { status: upstream.status } : { error: 'the response was cut off' });
          }
        })(),
      };
    } catch (err) {
      release?.({ error: String((err as Error)?.message ?? err) });
      const e = err instanceof GatewayError ? err : new GatewayError('internal', String((err as Error)?.message ?? err));
      log(`refused or failed: ${e.message}`, { code: e.code });
      throw e;
    }
  };
}

import { createServer, type IncomingHttpHeaders } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import type { RelayedModelRequest } from '../src/environment.ts';
import { AI_GATEWAY_LANGUAGE_MODEL_ROUTE, CLAUDE_ROUTES, CODEX_ROUTES, GatewayError, type GatewayLedger, gatewayHandler, type GatewayHandlerOptions, matchRoute, parseStrictJson } from '../src/gateway.ts';

/**
 * `gatewayHandler`'s policy (docs/workspaces.md §5.5, §11.4 A3): the VM is
 * untrusted, so every method, path, body and header it relays is checked
 * before the Function adds its credential, and the upstream URL never comes
 * from the frame.
 */

const closers: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const c of closers.splice(0)) await c();
});

const CREDENTIAL = 'fn-credential-do-not-leak';

/** A stand-in for the Gateway: records every request, answers like `/v1/messages` with the Gateway's cost metadata (P0.18). */
async function gateway(options: { cost?: string; status?: number } = {}) {
  const seen: { method?: string; url?: string; headers: IncomingHttpHeaders; body: string }[] = [];
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    seen.push({ method: req.method, url: req.url, headers: req.headers, body });
    res.writeHead(options.status ?? 200, { 'content-type': 'text/event-stream', 'set-cookie': 'a=b', 'x-internal': 'secret-config', 'request-id': 'req_1' });
    res.write('event: content_block_delta\ndata: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}\n\n');
    res.end(`event: message_delta\ndata: {"type":"message_delta","provider_metadata":{"gateway":{"routing":{},"cost":"0.5","gatewayCost":"${options.cost ?? '1.25'}"}}}\n\n`);
  });
  await new Promise<void>((ok) => server.listen(0, '127.0.0.1', ok));
  closers.push(() => new Promise((done) => server.close(() => done())));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, seen };
}

/** An in-memory ledger standing in for the log's totals (shared between handlers to model a redelivery). */
function ledger(): GatewayLedger & { turn: number; session: number; newTurn(): void } {
  const l = {
    turn: 0,
    session: 0,
    spent: () => ({ turnUsd: l.turn, sessionUsd: l.session }),
    add: (usd: number) => {
      l.turn += usd;
      l.session += usd;
    },
    newTurn: () => {
      l.turn = 0;
    },
  };
  return l;
}

const MODELS = { model: 'anthropic/claude-sonnet-4.5', smallModel: 'anthropic/claude-haiku-4.5' };

function handler(target: string, overrides: Partial<GatewayHandlerOptions> = {}) {
  const logs: { message: string; fields?: Record<string, unknown> }[] = [];
  const h = gatewayHandler({
    routes: CLAUDE_ROUTES,
    models: MODELS,
    budget: { turnUsd: 100, sessionUsd: 1000 },
    ledger: ledger(),
    target,
    credential: async () => CREDENTIAL,
    log: (message, fields) => logs.push({ message, fields }),
    ...overrides,
  });
  return { h, logs };
}

/** The default handler, no overrides. */
const h0 = (target: string) => handler(target).h;

const body = (value: unknown) => new TextEncoder().encode(typeof value === 'string' ? value : JSON.stringify(value));
const messages = (extra: Record<string, unknown> = {}) => ({ model: MODELS.model, max_tokens: 1024, messages: [{ role: 'user', content: 'hi' }], stream: true, ...extra });
/** A relayed request; `null` for no body. */
const req = (path: string, b: unknown = messages(), headers: Record<string, string> = {}, method = 'POST'): RelayedModelRequest => ({ method, path, headers: { 'content-type': 'application/json', ...headers }, body: b === null ? new Uint8Array() : body(b) });
const signal = () => new AbortController().signal;

async function drain(body: AsyncIterable<Uint8Array>): Promise<string> {
  let text = '';
  for await (const c of body) text += new TextDecoder().decode(c);
  return text;
}

async function refused(p: Promise<unknown>, code: string): Promise<GatewayError> {
  const err = await p.then(
    () => undefined,
    (e: unknown) => e
  );
  expect(err).toBeInstanceOf(GatewayError);
  expect((err as GatewayError).code).toBe(code);
  return err as GatewayError;
}

describe('gatewayHandler: the fixed table, never a URL from the frame', () => {
  const tricks = [
    ['@host', '@attacker.example/v1/messages'],
    ['.host', '.attacker.example/v1/messages'],
    ['//host', '//attacker.example/v1/messages'],
    [':port@host', ':443@attacker.example/v1/messages'],
    ['/@host', '/@attacker.example/v1/messages'],
    ['..', '/v1/messages/../../v1/models'],
    ['dot segment', '/v1/./messages'],
    ['%2e%2e', '/v1/messages/%2e%2e/%2e%2e/v1/models'],
    ['%2F', '/v1%2Fmessages'],
    ['absolute-form', 'https://attacker.example/v1/messages'],
    ['absolute-form, the Gateway', 'https://ai-gateway.vercel.sh/v1/messages'],
    ['empty path', ''],
    ['backslash', '/v1\\messages'],
    ['a query the table does not list', '/v1/messages?beta=true&x=1'],
    ['a second query', '/v1/messages?beta=true?x'],
    ['a fragment', '/v1/messages#x'],
    ['a path off the table', '/v1/models'],
    ['a trailing slash', '/v1/messages/'],
    ['whitespace', '/v1/messages '],
  ] as const;

  for (const [name, path] of tricks) {
    it(`refuses ${name} (${JSON.stringify(path)}) with forbidden, and sends nothing upstream`, async () => {
      const gw = await gateway();
      const { h } = handler(gw.url);
      await refused(h(req(path), signal()), 'forbidden');
      expect(gw.seen).toHaveLength(0);
    });
  }

  it('refuses a method the table does not have for the path', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    await refused(h(req('/v1/messages', messages(), {}, 'GET'), signal()), 'forbidden');
    await refused(h(req('/v1/messages', messages(), {}, 'PUT'), signal()), 'forbidden');
    expect(gw.seen).toHaveLength(0);
  });

  it('matches only the exact text: the table is never a pattern', () => {
    expect(matchRoute(CLAUDE_ROUTES, 'POST', '/v1/messages?beta=true').route.path).toBe('/v1/messages');
    expect(() => matchRoute(CLAUDE_ROUTES, 'POST', '/V1/messages')).toThrow(GatewayError);
    expect(() => matchRoute(CLAUDE_ROUTES, 'post', '/v1/messages')).toThrow(GatewayError);
  });

  it('forwards to the table path on the target origin, with the query the table allows, the credential added', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    const res = await h(req('/v1/messages?beta=true'), signal());
    expect(res.status).toBe(200);
    await drain(res.body);
    expect(gw.seen).toHaveLength(1);
    expect(gw.seen[0]).toMatchObject({ method: 'POST', url: '/v1/messages?beta=true' });
    expect(gw.seen[0]!.headers.authorization).toBe(`Bearer ${CREDENTIAL}`);
    expect(gw.seen[0]!.headers['x-api-key']).toBe(CREDENTIAL);
  });

  it('answers Claude\'s connectivity check itself, never upstream and never with the credential', async () => {
    const gw = await gateway();
    let reads = 0;
    const { h } = handler(gw.url, { credential: async () => (reads++, CREDENTIAL) });
    const res = await h(req('/api/hello', null, {}, 'HEAD'), signal());
    expect(res.status).toBe(200);
    expect(await drain(res.body)).toBe('');
    expect(gw.seen).toHaveLength(0);
    expect(reads).toBe(0);
  });

  it('a target with a path keeps only its origin', async () => {
    const gw = await gateway();
    const { h } = handler(`${gw.url}/some/base`);
    await drain((await h(req('/v1/messages'), signal())).body);
    expect(gw.seen[0]!.url).toBe('/v1/messages');
  });
});

describe('gatewayHandler: the body', () => {
  it('refuses duplicate keys at any depth', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    await refused(h(req('/v1/messages', `{"model":"${MODELS.smallModel}","model":"anthropic/claude-opus-4.1","max_tokens":1,"messages":[]}`), signal()), 'bad_request');
    await refused(h(req('/v1/messages', `{"model":"${MODELS.model}","max_tokens":1,"messages":[{"role":"user","role":"assistant","content":"x"}]}`), signal()), 'bad_request');
    expect(() => parseStrictJson(body('{"a":1,"a":2}'))).toThrow(GatewayError);
    expect(parseStrictJson(body('{"a":{"b":[1,{"c":null}]},"d":"\\u0041"}'))).toEqual({ a: { b: [1, { c: null }] }, d: 'A' });
    expect(gw.seen).toHaveLength(0);
  });

  it('refuses a body that is not strict JSON or not an object, or is over 4 MiB', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    for (const b of ['{"model":', '[1]', '"x"', '{"a":1} trailing', "{'a':1}", '{"a":NaN}', '{"__proto__":{"x":1},"model":"m"}']) {
      const err = await h(req('/v1/messages', b), signal()).then(() => undefined, (e: GatewayError) => e);
      expect(err?.code === 'bad_request' || err?.code === 'forbidden').toBe(true);
    }
    const big = new Uint8Array((4 << 20) + 1).fill(0x20);
    await refused(h({ method: 'POST', path: '/v1/messages', headers: {}, body: big }, signal()), 'bad_request');
    await refused(h({ method: 'POST', path: '/v1/messages', headers: {}, body: new Uint8Array([0xff, 0xfe]) }, signal()), 'bad_request');
    expect(gw.seen).toHaveLength(0);
  });

  it('refuses a top-level key outside the schema (mcp_servers, container, service_tier, provider options)', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    for (const extra of [{ mcp_servers: [] }, { container: 'c' }, { service_tier: 'priority' }, { providerOptions: { gateway: { order: ['x'] } } }, { betas: ['x'] }]) {
      await refused(h(req('/v1/messages', messages(extra)), signal()), 'forbidden');
    }
    expect(gw.seen).toHaveLength(0);
  });

  it("passes Claude Code 2.1.296's output_config (effort, format, task_budget), and refuses anything else in it", async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    const config = { effort: 'high', format: { type: 'json_schema', schema: { type: 'object' } }, task_budget: { type: 'tokens', total: 64_000 } };
    await drain((await h(req('/v1/messages', messages({ output_config: config })), signal())).body);
    await drain((await h(req('/v1/messages/count_tokens', { model: MODELS.model, messages: [{ role: 'user', content: 'hi' }], output_config: { effort: 'low' } }), signal())).body);
    expect(JSON.parse(gw.seen[0]!.body).output_config).toEqual(config);
    expect(JSON.parse(gw.seen[1]!.body).output_config).toEqual({ effort: 'low' });
    await refused(h(req('/v1/messages', messages({ output_config: { effort: 'high', mcp_servers: [] } })), signal()), 'forbidden');
    for (const output_config of ['high', { effort: 3 }, { format: 'json' }, { task_budget: 64_000 }]) {
      await refused(h(req('/v1/messages', messages({ output_config })), signal()), 'bad_request');
    }
    expect(gw.seen).toHaveLength(2);
  });

  it('refuses a tool type that is not a client tool, and a provider-hosted tool without the opt-in or with network deny-all', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    for (const tool of [{ type: 'code_execution_20250825', name: 'x' }, { type: 'bash_20250124', name: 'bash' }, { type: 'computer_20250124', name: 'c' }, { type: 'web_search_20250305', name: 'web_search' }, { type: 'web_fetch_20250910', name: 'web_fetch' }]) {
      await refused(h(req('/v1/messages', messages({ tools: [tool] })), signal()), 'forbidden');
    }
    const opted = handler(gw.url, { providerTools: ['web_search'] }).h;
    await drain((await opted(req('/v1/messages', messages({ tools: [{ type: 'web_search_20250305', name: 'web_search' }, { name: 'mine', input_schema: {} }] })), signal())).body);
    await refused(opted(req('/v1/messages', messages({ tools: [{ type: 'web_fetch_20250910', name: 'web_fetch' }] })), signal()), 'forbidden');
    const denied = handler(gw.url, { providerTools: ['web_search'], network: 'deny-all' }).h;
    await refused(denied(req('/v1/messages', messages({ tools: [{ type: 'web_search_20250305', name: 'web_search' }] })), signal()), 'forbidden');
    expect(gw.seen).toHaveLength(1);
  });

  it('allows only { model, smallModel }, and clamps max_tokens', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { maxOutputTokens: 2048 });
    await refused(h(req('/v1/messages', messages({ model: 'anthropic/claude-opus-4.1' })), signal()), 'forbidden');
    await refused(h(req('/v1/messages', messages({ model: undefined })), signal()), 'forbidden');
    await drain((await h(req('/v1/messages', messages({ model: MODELS.smallModel, max_tokens: 1_000_000 })), signal())).body);
    expect(JSON.parse(gw.seen[0]!.body)).toMatchObject({ model: MODELS.smallModel, max_tokens: 2048 });
  });

  it('forwards the re-serialized body, never the raw bytes', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    const raw = `{ "model" : "${MODELS.model}" ,\n "max_tokens": 10, "messages": [ ], "stream": true }`;
    await drain((await h(req('/v1/messages', raw), signal())).body);
    expect(gw.seen[0]!.body).toBe(JSON.stringify(JSON.parse(raw)));
  });

  it('Codex: /v1/responses with function tools; mcp, code_interpreter and an unknown key refused', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: CODEX_ROUTES, models: { model: 'openai/gpt-5-codex' } });
    const ok = { model: 'openai/gpt-5-codex', input: [], tools: [{ type: 'function', name: 'shell', parameters: {} }, { type: 'custom', name: 'apply_patch' }], stream: true };
    await drain((await h(req('/v1/responses', ok), signal())).body);
    for (const tool of [{ type: 'mcp', server_url: 'https://x' }, { type: 'code_interpreter' }, { type: 'file_search' }, { type: 'web_search' }]) {
      await refused(h(req('/v1/responses', { ...ok, tools: [tool] }), signal()), 'forbidden');
    }
    await refused(h(req('/v1/responses', { ...ok, background: true }), signal()), 'forbidden');
    await refused(h(req('/v1/messages', messages()), signal()), 'forbidden');
    expect(gw.seen).toHaveLength(1);
  });

  it('Codex: nothing is stored on the shared account, and the output limit is always clamped', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: CODEX_ROUTES, models: { model: 'openai/gpt-5-codex' }, maxOutputTokens: 4096 });
    const base = { model: 'openai/gpt-5-codex', input: [], stream: true };
    await drain((await h(req('/v1/responses', { ...base, store: true }), signal())).body);
    await drain((await h(req('/v1/responses', { ...base, max_output_tokens: 1_000_000 }), signal())).body);
    await drain((await h(req('/v1/responses', { ...base, max_output_tokens: 100 }), signal())).body);
    expect(gw.seen.map((s) => JSON.parse(s.body)).map((b) => [b.store, b.max_output_tokens])).toEqual([
      [false, 4096],
      [false, 4096],
      [false, 100],
    ]);
    await refused(h(req('/v1/responses', { ...base, max_output_tokens: 0 }), signal()), 'bad_request');
  });

  it("Codex 0.160's request as P0.7 logged it: client_metadata dropped, its own ids and turn metadata kept in the Function, originator and the request id passed", async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: CODEX_ROUTES, models: { model: 'openai/gpt-5.4-mini' } });
    const sent = {
      model: 'openai/gpt-5.4-mini',
      instructions: 'You are Codex.',
      input: [{ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'hi' }] }],
      tools: [{ type: 'function', name: 'write_file', parameters: {} }],
      tool_choice: 'auto',
      parallel_tool_calls: true,
      reasoning: { summary: 'auto' },
      store: false,
      stream: true,
      include: ['reasoning.encrypted_content'],
      prompt_cache_key: 'thread-1',
      client_metadata: { thread_id: 'thread-1', 'x-codex-installation-id': 'inst-1', 'x-codex-turn-metadata': '{"sandbox":"seatbelt"}' },
    };
    const headers = {
      accept: 'text/event-stream',
      authorization: 'Bearer harness-world-codex-placeholder',
      originator: 'harness-world-codex',
      'x-client-request-id': 'thread-1',
      'session-id': 'thread-1',
      'thread-id': 'thread-1',
      'x-codex-window-id': 'thread-1:0',
      'x-codex-turn-metadata': '{"installation_id":"inst-1"}',
      'x-codex-beta-features': 'remote_compaction_v2',
      'user-agent': 'harness-world-codex/0.160.0',
    };
    await drain((await h(req('/v1/responses', sent, headers), signal())).body);
    const upstream = gw.seen[0]!;
    const { client_metadata: _dropped, ...rest } = sent;
    // The output limit Codex never sends is set, at the clamp.
    expect(JSON.parse(upstream.body)).toEqual({ ...rest, max_output_tokens: 128_000 });
    expect(upstream.headers.originator).toBe('harness-world-codex');
    expect(upstream.headers['x-client-request-id']).toBe('thread-1');
    for (const name of ['session-id', 'thread-id', 'x-codex-window-id', 'x-codex-turn-metadata', 'x-codex-beta-features']) expect(upstream.headers[name]).toBeUndefined();
    // The credential, not the VM's placeholder.
    expect(upstream.headers.authorization).toBe(`Bearer ${CREDENTIAL}`);
  });
});

describe("gatewayHandler: AI Gateway's language-model route (libfx's kernel)", () => {
  const LM = [AI_GATEWAY_LANGUAGE_MODEL_ROUTE];
  /** A call as libfx 0.0.13 sends it: prompt, function tools, a tool choice; the model in a header. */
  const call = (extra: Record<string, unknown> = {}) => ({
    prompt: [{ role: 'system', content: 'You are fx.' }, { role: 'user', content: [{ type: 'text', text: 'hello' }] }],
    tools: [{ type: 'function', name: 'bash', description: 'Run bash.', inputSchema: { type: 'object' } }],
    toolChoice: { type: 'auto' },
    ...extra,
  });
  const lmHeaders = (model = MODELS.model) => ({
    'ai-language-model-id': model,
    'ai-language-model-specification-version': '4',
    'ai-language-model-streaming': 'true',
    'ai-gateway-protocol-version': '0.0.1',
    'x-session-affinity': 'abc',
    'http-referer': 'https://example.test',
    authorization: 'Bearer from-the-tab',
  });

  it('passes the call with its protocol headers, the model from its header, nothing else of the caller\'s', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: LM });
    const res = await h(req('/v4/ai/language-model', call(), lmHeaders()), signal());
    expect(res.status).toBe(200);
    await drain(res.body);
    expect(gw.seen[0]).toMatchObject({ method: 'POST', url: '/v4/ai/language-model' });
    expect(JSON.parse(gw.seen[0]!.body)).toEqual(call());
    const sent = gw.seen[0]!.headers;
    expect(sent['ai-language-model-id']).toBe(MODELS.model);
    expect(sent['ai-language-model-specification-version']).toBe('4');
    expect(sent['x-session-affinity']).toBe('abc');
    expect(sent['http-referer']).toBeUndefined();
    expect(sent.authorization).toBe(`Bearer ${CREDENTIAL}`);
  });

  it('refuses a model header outside { model, smallModel }, or none', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: LM });
    await refused(h(req('/v4/ai/language-model', call(), lmHeaders('openai/gpt-expensive')), signal()), 'forbidden');
    const { 'ai-language-model-id': _, ...none } = lmHeaders();
    await refused(h(req('/v4/ai/language-model', call(), none), signal()), 'forbidden');
    expect(gw.seen).toHaveLength(0);
  });

  it('refuses a body outside the schema: provider options (Gateway routing, fallbacks), a provider-defined tool, an unknown key, a bad prompt', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: LM });
    for (const body of [
      call({ providerOptions: { gateway: { order: ['bedrock'], models: ['openai/gpt-expensive'] } } }),
      call({ providerOptions: { anthropic: { mcpServers: [{ url: 'https://attacker.example' }] } } }),
      call({ tools: [{ type: 'provider', id: 'anthropic.web_search_20250305', name: 'web_search', args: {} }] }),
      call({ tools: [{ type: 'provider-defined', id: 'openai.code_interpreter', name: 'code', args: {} }] }),
      call({ headers: { 'x-api-key': 'k' } }),
      call({ mcpServers: [] }),
      call({ prompt: [{ role: 'user', content: [{ type: 'text', text: 'x', providerOptions: { gateway: { order: ['bedrock'] } } }] }] }),
      call({ prompt: [{ role: 'user', providerOptions: { gateway: { models: ['openai/gpt-expensive'] } }, content: 'x' }] }),
    ]) {
      await refused(h(req('/v4/ai/language-model', body, lmHeaders()), signal()), 'forbidden');
    }
    await refused(h(req('/v4/ai/language-model', call({ prompt: 'not an array' }), lmHeaders()), signal()), 'bad_request');
    expect(gw.seen).toHaveLength(0);
  });

  it("passes what a provider needs back on a part (a reasoning part's signature, a cache breakpoint)", async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: LM });
    const prompt = [
      { role: 'user', content: [{ type: 'text', text: 'x', providerOptions: { anthropic: { cacheControl: { type: 'ephemeral' } } } }] },
      { role: 'assistant', content: [{ type: 'reasoning', text: 'hmm', providerOptions: { anthropic: { signature: 'sig' } } }, { type: 'text', text: 'ok' }] },
    ];
    await drain((await h(req('/v4/ai/language-model', call({ prompt }), lmHeaders()), signal())).body);
    expect(JSON.parse(gw.seen[0]!.body).prompt).toEqual(prompt);
  });

  it('clamps an output limit, adds none, and a file by URL passes only under allow-all', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { routes: LM, maxOutputTokens: 1000 });
    await drain((await h(req('/v4/ai/language-model', call({ maxOutputTokens: 5000 }), lmHeaders()), signal())).body);
    expect(JSON.parse(gw.seen[0]!.body).maxOutputTokens).toBe(1000);
    await drain((await h(req('/v4/ai/language-model', call(), lmHeaders()), signal())).body);
    expect(JSON.parse(gw.seen[1]!.body).maxOutputTokens).toBeUndefined();
    const byUrl = call({ prompt: [{ role: 'user', content: [{ type: 'file', mediaType: 'image/png', data: 'https://example.test/x.png' }] }] });
    await refused(h(req('/v4/ai/language-model', byUrl, lmHeaders()), signal()), 'forbidden');
    const open = handler(gw.url, { routes: LM, network: 'allow-all' }).h;
    await drain((await open(req('/v4/ai/language-model', byUrl, lmHeaders()), signal())).body);
    expect(gw.seen).toHaveLength(3);
  });
});

describe('gatewayHandler: content the provider would run or fetch', () => {
  const image = (url: string) => ({ role: 'user', content: [{ type: 'image', source: { type: 'url', url } }] });

  it('Anthropic: a URL source (image, document, inside a tool result) only under allow-all; a Files API source, MCP, container and code-execution blocks never', async () => {
    const gw = await gateway();
    for (const network of [undefined, 'deny-all', 'custom'] as const) {
      const { h } = handler(gw.url, { ...(network ? { network } : {}) });
      await refused(h(req('/v1/messages', messages({ messages: [image('https://attacker.example/?d=secret')] })), signal()), 'forbidden');
      await refused(h(req('/v1/messages', messages({ messages: [{ role: 'user', content: [{ type: 'document', source: { type: 'url', url: 'https://x.example/a.pdf' } }] }] })), signal()), 'forbidden');
      await refused(h(req('/v1/messages', messages({ messages: [{ role: 'user', content: [{ type: 'tool_result', tool_use_id: 't', content: [{ type: 'image', source: { type: 'url', url: 'https://x.example/i.png' } }] }] }] })), signal()), 'forbidden');
    }
    const open = handler(gw.url, { network: 'allow-all' }).h;
    await drain((await open(req('/v1/messages', messages({ messages: [image('https://example.com/cat.png')] })), signal())).body);
    for (const block of [
      { type: 'image', source: { type: 'file', file_id: 'file_1' } },
      { type: 'mcp_tool_use', id: 'm', name: 'x', server_name: 's', input: {} },
      { type: 'mcp_tool_result', tool_use_id: 'm', content: [] },
      { type: 'container_upload', file_id: 'file_1' },
      { type: 'code_execution_tool_result', tool_use_id: 'c', content: {} },
      { type: 'server_tool_use', id: 's', name: 'code_execution', input: {} },
      { type: 'server_tool_use', id: 's', name: 'web_search', input: { query: 'q' } },
      { type: 'web_search_tool_result', tool_use_id: 's', content: [] },
    ]) {
      await refused(open(req('/v1/messages', messages({ messages: [{ role: 'assistant', content: [block] }] })), signal()), 'forbidden');
    }
    // A tool call's input is the model's own arguments, not content: never walked.
    await drain((await open(req('/v1/messages', messages({ messages: [{ role: 'assistant', content: [{ type: 'tool_use', id: 't', name: 'fetch', input: { source: { type: 'url', url: 'https://x' } } }] }] })), signal())).body);
    // Base64 images and text documents pass anywhere; so does a web search the app opted into.
    const denied = handler(gw.url, { network: 'deny-all' }).h;
    await drain((await denied(req('/v1/messages', messages({ system: [{ type: 'text', text: 's' }], messages: [{ role: 'user', content: [{ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AA==' } }, { type: 'document', source: { type: 'text', media_type: 'text/plain', data: 'x' } }] }] })), signal())).body);
    const searching = handler(gw.url, { network: 'allow-all', providerTools: ['web_search'] }).h;
    await drain((await searching(req('/v1/messages', messages({ messages: [{ role: 'assistant', content: [{ type: 'server_tool_use', id: 's', name: 'web_search', input: { query: 'q' } }, { type: 'web_search_tool_result', tool_use_id: 's', content: [] }] }] })), signal())).body);
    await refused(h0(gw.url)(req('/v1/messages/count_tokens?beta=true', { model: MODELS.model, messages: [image('https://x.example/i.png')] }), signal()), 'forbidden');
    expect(gw.seen).toHaveLength(4);
  });

  it('Responses: an image or file by URL only under allow-all; a Files API id and the items the provider runs never', async () => {
    const gw = await gateway();
    const codex = (network?: 'allow-all' | 'deny-all') => handler(gw.url, { routes: CODEX_ROUTES, models: { model: 'openai/gpt-5-codex' }, ...(network ? { network } : {}) }).h;
    const base = { model: 'openai/gpt-5-codex', stream: true };
    const msg = (part: Record<string, unknown>) => ({ ...base, input: [{ type: 'message', role: 'user', content: [part] }] });
    await refused(codex('deny-all')(req('/v1/responses', msg({ type: 'input_image', image_url: 'https://attacker.example/?d=x' })), signal()), 'forbidden');
    await refused(codex('deny-all')(req('/v1/responses', msg({ type: 'input_file', file_url: 'https://attacker.example/f' })), signal()), 'forbidden');
    await refused(codex('allow-all')(req('/v1/responses', msg({ type: 'input_image', file_id: 'file_1' })), signal()), 'forbidden');
    await refused(codex('deny-all')(req('/v1/responses', { ...base, input: [{ type: 'function_call_output', call_id: 'c', output: [{ type: 'input_image', image_url: 'https://x.example/i' }] }] }), signal()), 'forbidden');
    for (const type of ['mcp_call', 'mcp_list_tools', 'code_interpreter_call', 'file_search_call', 'image_generation_call', 'computer_call', 'web_search_call']) {
      await refused(codex('allow-all')(req('/v1/responses', { ...base, input: [{ type, id: 'x' }] }), signal()), 'forbidden');
    }
    await drain((await codex('deny-all')(req('/v1/responses', msg({ type: 'input_image', image_url: 'data:image/png;base64,AA==' })), signal())).body);
    await drain((await codex('allow-all')(req('/v1/responses', msg({ type: 'input_image', image_url: 'https://example.com/cat.png' })), signal())).body);
    await drain((await codex('deny-all')(req('/v1/responses', { ...base, input: [{ role: 'user', content: 'hi' }, { type: 'function_call', call_id: 'c', name: 'shell', arguments: '{}' }, { type: 'function_call_output', call_id: 'c', output: 'ok' }] }), signal())).body);
    expect(gw.seen).toHaveLength(3);
  });

  it('keeps extended thinking under the clamped max_tokens, or refuses when no room is left', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url, { maxOutputTokens: 8000 });
    await drain((await h(req('/v1/messages', messages({ max_tokens: 64_000, thinking: { type: 'enabled', budget_tokens: 32_000 } })), signal())).body);
    expect(JSON.parse(gw.seen[0]!.body)).toMatchObject({ max_tokens: 8000, thinking: { type: 'enabled', budget_tokens: 7999 } });
    await drain((await h(req('/v1/messages', messages({ max_tokens: 4096, thinking: { type: 'enabled', budget_tokens: 2048 } })), signal())).body);
    expect(JSON.parse(gw.seen[1]!.body).thinking.budget_tokens).toBe(2048);
    const tiny = handler(gw.url, { maxOutputTokens: 1024 }).h;
    await refused(tiny(req('/v1/messages', messages({ max_tokens: 4096, thinking: { type: 'enabled', budget_tokens: 2048 } })), signal()), 'bad_request');
    await refused(h(req('/v1/messages', messages({ thinking: { type: 'enabled', budget_tokens: -1 } })), signal()), 'bad_request');
  });
});

describe('gatewayHandler: headers', () => {
  it('passes only allowlisted request headers (and known betas), never the VM\'s credential headers, cookies or ai-* headers', async () => {
    const gw = await gateway();
    const { h, logs } = handler(gw.url);
    await drain(
      (
        await h(
          req('/v1/messages', messages(), {
            'anthropic-version': '2023-06-01',
            'anthropic-beta': 'claude-code-20250219, mcp-client-2025-04-04,interleaved-thinking-2025-05-14',
            authorization: 'Bearer vm-placeholder',
            'x-api-key': 'vm-placeholder',
            cookie: 'a=b',
            'ai-gateway-auth-method': 'oidc',
            'ai-language-model-id': 'other',
            'x-forwarded-host': 'attacker.example',
            host: 'attacker.example',
          }),
          signal()
        )
      ).body
    );
    const headers = gw.seen[0]!.headers;
    expect(headers['anthropic-version']).toBe('2023-06-01');
    expect(headers['anthropic-beta']).toBe('claude-code-20250219,interleaved-thinking-2025-05-14');
    expect(headers.authorization).toBe(`Bearer ${CREDENTIAL}`);
    expect(headers.cookie).toBeUndefined();
    expect(headers['ai-gateway-auth-method']).toBeUndefined();
    expect(headers['ai-language-model-id']).toBeUndefined();
    expect(headers['x-forwarded-host']).toBeUndefined();
    expect(headers.host).not.toBe('attacker.example');
    expect(logs.some((l) => l.message.includes('anthropic-beta'))).toBe(true);
  });

  it('logs a dropped anthropic-beta value once, not on every call', async () => {
    const gw = await gateway();
    const { h, logs } = handler(gw.url);
    const call = (beta: string) => h(req('/v1/messages', messages(), { 'anthropic-beta': beta }), signal()).then((r) => drain(r.body));
    await call('claude-code-20250219,mcp-client-2025-04-04');
    await call('claude-code-20250219,mcp-client-2025-04-04');
    await call('mcp-client-2025-04-04,files-api-2025-04-14');
    const dropped = logs.filter((l) => l.message === 'dropped anthropic-beta values').map((l) => l.fields?.dropped);
    expect(dropped).toEqual([['mcp-client-2025-04-04'], ['files-api-2025-04-14']]);
  });

  it('passes only allowlisted response headers back', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    const res = await h(req('/v1/messages'), signal());
    await drain(res.body);
    expect(res.headers).toEqual({ 'content-type': 'text/event-stream', 'request-id': 'req_1' });
  });
});

describe('gatewayHandler: budget and concurrency', () => {
  it('records the Gateway\'s cost and refuses past the turn cap, then past the session cap across a redelivery (a new handler on the same ledger)', async () => {
    const gw = await gateway({ cost: '1.25' });
    const shared = ledger();
    const caps: string[] = [];
    const first = handler(gw.url, { ledger: shared, budget: { turnUsd: 2, sessionUsd: 3 }, onCap: (c) => caps.push(c) }).h;
    await drain((await first(req('/v1/messages'), signal())).body);
    expect(shared.session).toBe(1.25);
    await drain((await first(req('/v1/messages'), signal())).body);
    const turn = await refused(first(req('/v1/messages'), signal()), 'budget');
    expect(turn.cap).toBe('turnUsd');
    // A redelivery in a new invocation: a new handler, the totals from the log.
    shared.newTurn();
    const second = handler(gw.url, { ledger: shared, budget: { turnUsd: 2, sessionUsd: 3 }, onCap: (c) => caps.push(c) }).h;
    await drain((await second(req('/v1/messages'), signal())).body);
    const session = await refused(second(req('/v1/messages'), signal()), 'budget');
    expect(session.cap).toBe('sessionUsd');
    expect(caps).toEqual(['turnUsd', 'sessionUsd']);
    expect(gw.seen).toHaveLength(3);
  });

  it('takes its caps from the descriptor only: Infinity turns one off, and a missing one is a TypeError', async () => {
    const gw = await gateway({ cost: '1000' });
    const free = handler(gw.url, { budget: { turnUsd: Infinity, sessionUsd: Infinity } }).h;
    for (let i = 0; i < 3; i++) await drain((await free(req('/v1/messages'), signal())).body);
    expect(() => gatewayHandler({ routes: CLAUDE_ROUTES, models: MODELS, budget: {} as never, ledger: ledger() })).toThrow(TypeError);
  });

  it('caps requests in flight at 8', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    const open = await Promise.all(Array.from({ length: 8 }, () => h(req('/v1/messages'), signal())));
    const err = await refused(h(req('/v1/messages'), signal()), 'busy');
    expect(err.cap).toBe('concurrency');
    for (const r of open) await drain(r.body);
    await drain((await h(req('/v1/messages'), signal())).body);
  });
});

/** A Gateway stand-in whose every answer the test writes: `respond(res, n)` for the n-th request. */
async function stub(respond: (res: import('node:http').ServerResponse, n: number) => void | Promise<void>) {
  let n = 0;
  const server = createServer(async (req, res) => {
    for await (const _ of req);
    await respond(res, n++);
  });
  await new Promise<void>((ok) => server.listen(0, '127.0.0.1', ok));
  closers.push(() => new Promise((done) => (server.closeAllConnections(), server.close(() => done()))));
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

describe('gatewayHandler: every slot is released and every billed response charged', () => {
  it('without a track, failures after the slot is taken never keep it: a throwing credential, a cancelled request, a fetch that rejects', async () => {
    const gw = await gateway();
    let fail = true;
    const { h } = handler(gw.url, {
      credential: async () => {
        if (fail) throw new Error('no credential');
        return CREDENTIAL;
      },
    });
    for (let i = 0; i < 9; i++) await refused(h(req('/v1/messages'), signal()), 'internal');
    fail = false;
    for (let i = 0; i < 9; i++) {
      const ac = new AbortController();
      ac.abort();
      await refused(h(req('/v1/messages'), ac.signal), 'upstream');
    }
    const rejecting = handler(gw.url, { fetch: async () => Promise.reject(new Error('ECONNRESET')) }).h;
    for (let i = 0; i < 9; i++) await refused(rejecting(req('/v1/messages'), signal()), 'upstream');
    expect(await drain((await h(req('/v1/messages'), signal())).body)).toContain('gatewayCost');
  });

  it('a response cancelled before it is read (the CLI hung up before the head) releases its slot and is charged', async () => {
    const gw = await gateway({ cost: '0.01' });
    const shared = ledger();
    const { h } = handler(gw.url, { ledger: shared });
    for (let i = 0; i < 9; i++) {
      const ac = new AbortController();
      const res = await h(req('/v1/messages'), ac.signal);
      ac.abort();
      await res.body[Symbol.asyncIterator]().return?.().catch(() => {});
    }
    // Never read, so the Gateway's figure never arrived: each was charged an estimate, and every slot came back.
    expect(shared.session).toBeGreaterThan(0);
    await drain((await h(req('/v1/messages'), signal())).body);
  });

  it('a stream cut off after its first delta (a mid-stream CANCEL) is charged at the ceiling, from the usage it showed', async () => {
    const url = await stub(async (res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write('event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":100000,"cache_read_input_tokens":0,"output_tokens":1}}}\n\n');
      res.write('event: content_block_delta\ndata: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}\n\n');
      // ... and never the message_delta with the Gateway's cost.
    });
    const shared = ledger();
    const added: { usd: number; estimated?: boolean }[] = [];
    const { h } = handler(url, { ledger: { ...shared, add: (usd, info) => (added.push({ usd, estimated: info.estimated }), shared.add(usd, info)) } });
    const ac = new AbortController();
    const res = await h(req('/v1/messages'), ac.signal);
    const it = res.body[Symbol.asyncIterator]();
    await it.next();
    ac.abort();
    // The aborted upstream stream may reject the return with the abort's reason.
    await it.return?.().catch(() => {});
    expect(added).toHaveLength(1);
    expect(added[0]!.estimated).toBe(true);
    // 100 000 input tokens at $15 a million, at least: the budget can't be dodged by cancelling.
    expect(added[0]!.usd).toBeGreaterThanOrEqual(1.5);
  });

  it('a ledger that throws while a cancelled request is charged (from the abort listener) is logged, never an uncaught exception', async () => {
    const gw = await gateway();
    const uncaught: unknown[] = [];
    const onUncaught = (e: unknown) => uncaught.push(e);
    process.on('uncaughtException', onUncaught);
    try {
      const { h, logs } = handler(gw.url, { ledger: { spent: () => ({ turnUsd: 0, sessionUsd: 0 }), add: () => { throw new Error('the effect ended'); } }, track: () => () => { throw new Error('step closed'); } });
      for (let i = 0; i < 9; i++) {
        const ac = new AbortController();
        const res = await h(req('/v1/messages'), ac.signal);
        ac.abort();
        await res.body[Symbol.asyncIterator]().return?.().catch(() => {});
      }
      await new Promise((r) => setTimeout(r, 20));
      expect(uncaught).toEqual([]);
      expect(logs.filter((l) => l.message === 'the ledger refused a charge')).toHaveLength(9);
      // Every slot came back.
      await drain((await h(req('/v1/messages'), signal())).body);
    } finally {
      process.off('uncaughtException', onUncaught);
    }
  });

  it('a 2xx with no cost is charged an estimate from its bytes; token counting is free; an upstream error is not charged', async () => {
    const url = await stub(async (res, n) => {
      if (n === 2) return void res.writeHead(500, { 'content-type': 'application/json' }).end('{"error":{"type":"api_error"}}');
      res.writeHead(200, { 'content-type': 'application/json' }).end('{"type":"message","content":[]}');
    });
    const added: { path: string; usd: number }[] = [];
    const { h, logs } = handler(url, { ledger: { spent: () => ({ turnUsd: 0, sessionUsd: 0 }), add: (usd, info) => added.push({ path: info.path, usd }) } });
    await drain((await h(req('/v1/messages', messages({ stream: false })), signal())).body);
    await drain((await h(req('/v1/messages/count_tokens?beta=true', { model: MODELS.model, messages: [] }), signal())).body);
    await drain((await h(req('/v1/messages'), signal())).body);
    expect(added.map((a) => a.path)).toEqual(['/v1/messages']);
    expect(added[0]!.usd).toBeGreaterThan(0);
    expect(logs.some((l) => l.message.startsWith('a response with no Gateway cost'))).toBe(true);
  });
});

describe('gatewayHandler: an upstream error reaches the VM as a status and a fixed type', () => {
  it('the Gateway\'s own words (credential, team, billing) stay in the Function; a request error keeps its message (Claude Code compacts on it)', async () => {
    const url = await stub(async (res, n) => {
      if (n === 0) return void res.writeHead(401, { 'content-type': 'application/json', 'set-cookie': 'a=b' }).end('{"error":{"type":"authentication_error","message":"OIDC token for team_secret expired"}}');
      if (n === 1) return void res.writeHead(402, { 'content-type': 'application/json' }).end('{"error":{"type":"insufficient_funds","message":"team_secret has no credit"}}');
      if (n === 2) return void res.writeHead(400, { 'content-type': 'application/json' }).end('{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 250000 tokens > 200000 maximum"}}');
      if (n === 4) return void res.writeHead(400, { 'content-type': 'application/json' }).end('{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"Your input exceeds the context window"}}');
      if (n === 5) return void res.writeHead(400, { 'content-type': 'application/json' }).end('{"error":{"type":"invalid_request_error","code":"team_secret has no credit","message":"x"}}');
      res.writeHead(429, { 'content-type': 'text/plain', 'retry-after': '3' }).end('rate limited for team_secret');
    });
    const { h, logs } = handler(url);
    const seen: { status: number; headers: Record<string, string>; text: string }[] = [];
    for (let i = 0; i < 6; i++) {
      const res = await h(req('/v1/messages'), signal());
      seen.push({ status: res.status, headers: { ...res.headers }, text: await drain(res.body) });
    }
    expect(seen.map((r) => r.status)).toEqual([401, 402, 400, 429, 400, 400]);
    expect(seen.map((r) => JSON.parse(r.text))).toEqual([
      { type: 'error', error: { type: 'authentication_error' } },
      { type: 'error', error: { type: 'billing_error' } },
      { type: 'error', error: { type: 'invalid_request_error', message: 'prompt is too long: 250000 tokens > 200000 maximum' } },
      { type: 'error', error: { type: 'rate_limit_error' } },
      // OpenAI's request errors keep an identifier-like code (Codex reads context_length_exceeded); any other code is dropped.
      { type: 'error', error: { type: 'invalid_request_error', message: 'Your input exceeds the context window', code: 'context_length_exceeded' } },
      { type: 'error', error: { type: 'invalid_request_error', message: 'x' } },
    ]);
    expect(seen.map((r) => r.text).join()).not.toContain('team_secret');
    expect(seen[3]!.headers).toMatchObject({ 'retry-after': '3', 'content-type': 'application/json' });
    expect(logs.filter((l) => l.message === 'the Gateway answered an error')).toHaveLength(6);
  });
});

describe('gatewayHandler: errors carry no detail, and the credential stays in the Function', () => {
  it('an upstream failure is `upstream` with no host, URL or credential in what the VM sees', async () => {
    const { h, logs } = handler('http://127.0.0.1:1');
    const err = await refused(h(req('/v1/messages'), signal()), 'upstream');
    // The tunnel sends `code` alone; the message is the Function log's.
    expect(Object.keys(err)).not.toContain('credential');
    expect(JSON.stringify({ code: err.code })).not.toContain('127.0.0.1');
    expect(logs.map((l) => JSON.stringify(l)).join()).not.toContain(CREDENTIAL);
  });

  it('never reads the credential for a refused request', async () => {
    const gw = await gateway();
    let reads = 0;
    const { h } = handler(gw.url, { credential: async () => (reads++, CREDENTIAL) });
    await refused(h(req('@attacker.example/v1/messages'), signal()), 'forbidden');
    await refused(h(req('/v1/messages', messages({ model: 'x' })), signal()), 'forbidden');
    expect(reads).toBe(0);
  });

  it('the credential reaches only the upstream request, never the response', async () => {
    const gw = await gateway();
    const { h } = handler(gw.url);
    const res = await h(req('/v1/messages'), signal());
    const text = await drain(res.body);
    expect(JSON.stringify(res.headers) + text).not.toContain(CREDENTIAL);
  });
});

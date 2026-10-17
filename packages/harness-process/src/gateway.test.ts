import { createServer, request as httpRequest, type IncomingHttpHeaders } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { FIRST_OUTPUT, gatewayCredential, gatewayProxy, gatewayToken, MODEL_BODY_MAX_BYTES } from './gateway.ts';

const closers: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const c of closers.splice(0)) await c();
});

/** A stand-in for the Gateway: echoes what it got, streaming the reply in pieces. */
async function upstream(parts = ['data: one\n\n', 'data: two\n\n']) {
  const seen: { method?: string; url?: string; headers?: IncomingHttpHeaders; body?: string }[] = [];
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    seen.push({ method: req.method, url: req.url, headers: req.headers, body });
    res.writeHead(req.url === '/missing' ? 404 : 200, { 'content-type': 'text/event-stream', 'x-upstream': 'yes' });
    for (const part of parts) {
      res.write(part);
      await new Promise((r) => setTimeout(r, 10));
    }
    res.end();
  });
  await new Promise<void>((ok) => server.listen(0, '127.0.0.1', ok));
  closers.push(() => new Promise((done) => server.close(() => done())));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, seen };
}

/** The key the tests' proxies give their "child", and the header that carries it. */
const KEY = 'child-key';
const keyed = { authorization: `Bearer ${KEY}` };

describe('gatewayProxy', () => {
  it.each([
    { captureRequest: false, path: '/v1/messages' },
    { captureRequest: true, path: '/v1/messages/count_tokens' },
  ])('streams request bytes before the body ends when no trace payload is needed: $path ($captureRequest)', async ({ captureRequest, path }) => {
    let arrived!: () => void;
    const first = new Promise<void>(resolve => { arrived = resolve; });
    let received = '';
    const server = createServer((req, res) => {
      req.on('data', chunk => { received += chunk; arrived(); });
      req.on('end', () => res.end('ok'));
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    closers.push(() => { server.closeAllConnections(); return new Promise(resolve => server.close(() => resolve())); });
    const tracked: unknown[] = [];
    const proxy = await gatewayProxy({ target: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, credential: async () => 'test', token: KEY, captureRequest, track: info => { tracked.push(info); } });
    closers.push(proxy.close);
    const outgoing = httpRequest(`${proxy.url}${path}`, { method: 'POST', headers: keyed });
    const response = new Promise<string>((resolve, reject) => {
      outgoing.on('error', reject);
      outgoing.on('response', res => { let text = ''; res.on('data', chunk => { text += chunk; }); res.on('end', () => resolve(text)); });
    });
    response.catch(() => {});
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      outgoing.write('first');
      await Promise.race([first, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('request was buffered')), 1000); })]);
      outgoing.end('second');
      expect(await response).toBe('ok');
      expect(received).toBe('firstsecond');
      expect(tracked).toEqual([{ method: 'POST', path }]);
    } finally { clearTimeout(timer); outgoing.destroy(); }
  });

  it('forwards a model request beyond the capture limit without retaining or parsing its body', async () => {
    const target = await upstream(['ok']);
    const tracked: { body?: unknown }[] = [];
    const proxy = await gatewayProxy({ target: target.url, credential: async () => 'test', token: KEY, captureRequest: true, track: info => { tracked.push(info); } });
    closers.push(proxy.close);
    const body = JSON.stringify({ model: 'test', messages: 'x'.repeat(MODEL_BODY_MAX_BYTES + 100) });
    const response = await fetch(`${proxy.url}/v1/messages`, { method: 'POST', headers: keyed, body });
    expect(await response.text()).toBe('ok');
    expect(target.seen[0]!.body).toBe(body);
    expect(tracked).toHaveLength(1);
    expect(tracked[0]!.body).toBeUndefined();
  });

  it('forwards with a fresh credential per request, replacing the placeholder, and streams the reply', async () => {
    const target = await upstream();
    let n = 0;
    const proxy = await gatewayProxy({ target: target.url, credential: async () => `token-${++n}`, token: KEY });
    closers.push(proxy.close);
    const res = await fetch(`${proxy.url}/v1/messages?beta=1`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ hello: 'world' }),
    });
    expect(res.status).toBe(200);
    expect(res.headers.get('x-upstream')).toBe('yes');
    expect(await res.text()).toBe('data: one\n\ndata: two\n\n');
    expect(target.seen[0]).toMatchObject({ method: 'POST', url: '/v1/messages?beta=1', body: '{"hello":"world"}' });
    expect(target.seen[0]!.headers).toMatchObject({ authorization: 'Bearer token-1', 'x-api-key': 'token-1', 'anthropic-version': '2023-06-01' });
    const again = await fetch(`${proxy.url}/missing`, { headers: keyed });
    expect(again.status).toBe(404);
    await again.text();
    expect(target.seen[1]!.headers?.authorization).toBe('Bearer token-2');
  });

  it('forwards only requests that carry its token', async () => {
    const target = await upstream();
    const proxy = await gatewayProxy({ target: target.url, credential: async () => 'secret', token: 'tok-1' });
    closers.push(proxy.close);
    for (const headers of [{}, { authorization: 'Bearer other' }, { 'x-api-key': 'other' }] as Record<string, string>[]) {
      const res = await fetch(`${proxy.url}/v1/models`, { headers });
      expect(res.status).toBe(401);
      await res.text();
    }
    expect(target.seen).toHaveLength(0);
    for (const headers of [{ authorization: 'Bearer tok-1' }, { 'x-api-key': 'tok-1' }] as Record<string, string>[]) {
      const res = await fetch(`${proxy.url}/v1/models`, { headers });
      expect(res.status).toBe(200);
      await res.text();
    }
    expect(target.seen.map((s) => s.headers?.authorization)).toEqual(['Bearer secret', 'Bearer secret']);
  });

  it('refuses to start without a token', async () => {
    for (const options of [{}, { token: '' }, { token: undefined }, { token: [] }, { token: ['ok', ''] }, { token: Symbol('unchecked') }] as Record<string, unknown>[])
      await expect(gatewayProxy({ credential: async () => 'secret', ...options } as never)).rejects.toThrow(/`token` is required/);
    await expect((gatewayProxy as (o?: unknown) => Promise<unknown>)()).rejects.toThrow(/`token` is required/);
  });

  it('gatewayToken: a fresh key per call', () => {
    const a = gatewayToken();
    expect(a).toMatch(/^harness-world-[0-9a-f-]{36}$/);
    expect(gatewayToken()).not.toBe(a);
    expect(gatewayToken('harness-world-codex')).toMatch(/^harness-world-codex-/);
  });

  it('a set of tokens: forwards a request that carries any of them, refuses the rest', async () => {
    const target = await upstream();
    const proxy = await gatewayProxy({ target: target.url, credential: async () => 'secret', token: ['tok-1', 'tok-2'] });
    closers.push(proxy.close);
    for (const headers of [{}, { authorization: 'Bearer tok-3' }, { 'x-api-key': 'tok' }] as Record<string, string>[]) {
      const res = await fetch(`${proxy.url}/v3/ai/language-model`, { headers });
      expect(res.status).toBe(401);
      await res.text();
    }
    for (const headers of [{ authorization: 'Bearer tok-1' }, { 'x-api-key': 'tok-2' }] as Record<string, string>[]) {
      const res = await fetch(`${proxy.url}/v3/ai/language-model`, { headers });
      expect(res.status).toBe(200);
      await res.text();
    }
    expect(target.seen.map((s) => s.headers?.authorization)).toEqual(['Bearer secret', 'Bearer secret']);
  });

  it('tells `track` when each request starts and when it is over', async () => {
    const target = await upstream();
    const seen: string[] = [];
    const proxy = await gatewayProxy({
      target: target.url,
      credential: async () => 'secret',
      token: KEY,
      track: (r) => {
        seen.push(`start ${r.method} ${r.path}`);
        return (end) => seen.push(`end ${r.path} ${JSON.stringify(end)}`);
      },
    });
    closers.push(proxy.close);
    const res = await fetch(`${proxy.url}/v1/responses`, { method: 'POST', headers: keyed, body: '{}' });
    await res.text();
    await new Promise((r) => setTimeout(r, 20));
    expect(seen).toEqual(['start POST /v1/responses', 'end /v1/responses {"status":200}']);
    // A client that goes away mid-stream: over, with an error.
    const abort = new AbortController();
    const cut = await fetch(`${proxy.url}/v1/cut`, { method: 'POST', headers: keyed, body: '{}', signal: abort.signal });
    abort.abort();
    await cut.text().catch(() => {});
    await new Promise((r) => setTimeout(r, 50));
    expect(seen.slice(2)).toEqual(['start POST /v1/cut', 'end /v1/cut {"error":"the connection closed before the response ended"}']);
  });

  it.each([false, true])("calls firstToken once across chunks with capture=%s", async (capture) => {
    // A stand-in streaming Anthropic-style events: a start, then the first delta split across two chunks.
    const server = createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const parts = ['event: message_start\ndata: {"type":"message_start"}\n\n', 'data: {"type":"content_bl', 'ock_delta","delta":{"type":"text_delta","text":"hi"}}\n\n', 'data: {"type":"content_block_delta"}\n\n'];
      let i = 0;
      const next = () => (i < parts.length ? (res.write(parts[i++]), setTimeout(next, 10)) : res.end());
      next();
    });
    await new Promise<void>((ok) => server.listen(0, '127.0.0.1', ok));
    closers.push(() => new Promise((done) => server.close(() => done())));
    const seen: string[] = [];
    let finish!: (outcome: { response?: string }) => void;
    const finished = new Promise<{ response?: string }>((resolve) => { finish = resolve; });
    const proxy = await gatewayProxy({
      target: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
      credential: async () => 'secret',
      token: KEY,
      track: () => Object.assign((outcome: { response?: string }) => { seen.push('end'); finish(outcome); }, { capture, firstToken: () => seen.push('first') }),
    });
    closers.push(proxy.close);
    const res = await fetch(`${proxy.url}/v1/messages`, { method: 'POST', headers: keyed, body: '{}' });
    const response = await res.text();
    const outcome = await finished;
    expect(outcome.response).toBe(capture ? response : undefined);
    expect(seen).toEqual(['first', 'end']);
  });

  it('knows a first output in each API the CLIs speak', () => {
    expect(FIRST_OUTPUT.test('data: {"type":"content_block_delta","index":0}')).toBe(true);
    expect(FIRST_OUTPUT.test('data: {"type":"response.output_text.delta","delta":"Hi"}')).toBe(true);
    expect(FIRST_OUTPUT.test('data: {"type":"response.function_call_arguments.delta"}')).toBe(true);
    expect(FIRST_OUTPUT.test('data: {"choices":[{"delta":{"content":"Hi"}}]}')).toBe(true);
    expect(FIRST_OUTPUT.test('data: {"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}')).toBe(true);
    // Not yet: a start event, a role-only delta, an empty content delta.
    expect(FIRST_OUTPUT.test('data: {"type":"message_start"}')).toBe(false);
    expect(FIRST_OUTPUT.test('data: {"choices":[{"delta":{"role":"assistant","content":""}}]}')).toBe(false);
    expect(FIRST_OUTPUT.test('data: {"type":"response.created"}')).toBe(false);
  });

  it('prefers AI_GATEWAY_API_KEY', async () => {
    const before = process.env.AI_GATEWAY_API_KEY;
    process.env.AI_GATEWAY_API_KEY = 'key-from-env';
    try {
      expect(await gatewayCredential()).toBe('key-from-env');
    } finally {
      if (before === undefined) delete process.env.AI_GATEWAY_API_KEY;
      else process.env.AI_GATEWAY_API_KEY = before;
    }
  });

  it('captures opted-in model content without credentials or changing the streamed response', async () => {
    const target = await upstream();
    let finish!: (value: Record<string, unknown>) => void;
    const finished = new Promise<Record<string, unknown>>((resolve) => { finish = resolve; });
    const tracked: unknown[] = [];
    const proxy = await gatewayProxy({
      target: target.url,
      credential: async () => 'secret-upstream-key',
      token: KEY,
      captureRequest: true,
      track: (request) => (tracked.push(request), Object.assign(finish, { capture: true })),
    });
    closers.push(proxy.close);
    const response = await fetch(`${proxy.url}/v1/messages`, {
      method: 'POST',
      headers: keyed,
      body: JSON.stringify({ model: 'test', messages: [{ role: 'user', content: 'hello 🌍' }] }),
    });
    expect(await response.text()).toBe('data: one\n\ndata: two\n\n');
    const capture = await finished;
    expect(tracked).toEqual([{ method: 'POST', path: '/v1/messages', body: { model: 'test', messages: [{ role: 'user', content: 'hello 🌍' }] } }]);
    expect(capture).toMatchObject({ status: 200, response: 'data: one\n\ndata: two\n\n' });
    expect(JSON.stringify([tracked, capture])).not.toContain('secret-upstream-key');
    expect(JSON.stringify([tracked, capture])).not.toContain(KEY);
  });
  it('forwards a large request and response whole while it tracks them', async () => {
    const text = 'x'.repeat(300_000);
    const target = await upstream([text]);
    let finish!: (value: { response?: string }) => void;
    const finished = new Promise<{ response?: string }>(resolve => { finish = resolve; });
    const proxy = await gatewayProxy({ target: target.url, credential: async () => 'secret', token: KEY, track: () => Object.assign(finish, { capture: true }) });
    closers.push(proxy.close);
    const body = JSON.stringify({ messages: text });
    const response = await fetch(`${proxy.url}/v1/messages`, { method: 'POST', headers: keyed, body });
    expect(await response.text()).toBe(text);
    expect((await finished).response).toBe(text);
    expect(target.seen[0]!.body).toBe(body);
  });
  it('passes the Gateway model header to tracking without exposing other headers', async () => {
    const target = await upstream();
    const starts: unknown[] = [];
    const proxy = await gatewayProxy({
      target: target.url,
      credential: async () => 'upstream-secret',
      token: 'child-secret',
      captureRequest: true,
      track: request => { starts.push(request); },
    });
    closers.push(proxy.close);
    const response = await fetch(`${proxy.url}/v3/ai/language-model`, {
      method: 'POST',
      headers: { authorization: 'Bearer child-secret', 'ai-language-model-id': 'anthropic/claude-haiku-4.5', 'x-private': 'private-header' },
      body: JSON.stringify({ prompt: [{ role: 'user', content: [{ type: 'text', text: 'hello' }] }] }),
    });
    await response.text();
    expect(starts).toEqual([{ method: 'POST', path: '/v3/ai/language-model', model: 'anthropic/claude-haiku-4.5', body: { prompt: [{ role: 'user', content: [{ type: 'text', text: 'hello' }] }] } }]);
    expect(JSON.stringify(starts)).not.toContain('private-header');
    expect(target.seen[0]!.headers?.['ai-language-model-id']).toBe('anthropic/claude-haiku-4.5');
  });
});

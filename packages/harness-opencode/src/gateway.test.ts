import { mkdtemp } from 'node:fs/promises';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { durableSessions, QUEUE_PREFIX } from 'harness-world';
import { workspaceTools } from 'harness-world-hands';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { opencodeHarness } from './harness.ts';

/**
 * The credential path with real OpenCode 2.0.22 and no network: a turn
 * against a stand-in Gateway that records what reached it and refuses it
 * (OpenCode retries; the first request is enough). It must arrive with the
 * drive's credential, whatever key the host sent: OpenCode, in this
 * process, sends this process's own `AI_GATEWAY_API_KEY` when it is set,
 * and the proxy's per-drive key otherwise. Either way the proxy refuses a
 * request that carries neither.
 */

let gateway: Server;
let gatewayUrl: string;
const seen: { url: string; authorization?: string; model?: string | string[] }[] = [];
const worlds: LocalWorld[] = [];
const envKey = process.env.AI_GATEWAY_API_KEY;

beforeAll(async () => {
  gateway = createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/v1/models') {
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ data: [] }));
      return;
    }
    seen.push({ url: req.url ?? '', ...(req.headers.authorization ? { authorization: req.headers.authorization } : {}), model: req.headers['ai-language-model-id'] });
    res.writeHead(401, { 'content-type': 'application/json' }).end(JSON.stringify({ error: { message: 'no model calls in this test', type: 'authentication_error' } }));
  });
  await new Promise<void>((r) => gateway.listen(0, '127.0.0.1', r));
  gatewayUrl = `http://127.0.0.1:${(gateway.address() as AddressInfo).port}`;
  // What a Function's environment may hold: OpenCode, embedded, reads it.
  process.env.AI_GATEWAY_API_KEY = 'a-key-in-the-environment';
});
afterAll(() => {
  gateway.close();
  if (envKey === undefined) delete process.env.AI_GATEWAY_API_KEY;
  else process.env.AI_GATEWAY_API_KEY = envKey;
});
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
});

/**
 * Start a turn; resolve with the first model request that reaches the
 * Gateway, and what the drive's proxy (OpenCode's base URL, seen by
 * `fetch` in this process) answered a request with no key and with a
 * guessed one.
 */
async function firstRequest() {
  const fetch0 = globalThis.fetch;
  let proxyUrl: string | undefined;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input instanceof Request ? input.url : input);
    if (!proxyUrl && url.startsWith('http://127.0.0.1:') && url.includes('/v3/ai/') && !url.startsWith(gatewayUrl)) proxyUrl = new URL(url).origin;
    return fetch0(input, init);
  }) as typeof fetch;
  const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'oc-gw-')), recoverActiveRuns: false });
  worlds.push(world);
  const root = await mkdtemp(join(tmpdir(), 'oc-gw-root-'));
  const sessions = durableSessions({
    world,
    harness: opencodeHarness({ tools: workspaceTools(), root, gatewayUrl, credential: async () => 'the-drives-credential' }),
  });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  await world.start?.();
  const before = seen.length;
  const { sessionId } = await sessions.start({ prompt: 'hello' });
  const start = Date.now();
  while (seen.length === before) {
    if (Date.now() - start > 60_000) throw new Error('no model request reached the Gateway');
    await new Promise((r) => setTimeout(r, 50));
  }
  globalThis.fetch = fetch0;
  void sessionId;
  const refused = proxyUrl
    ? await Promise.all(
        ([{}, { authorization: 'Bearer harness-world-proxy' }] as Record<string, string>[]).map(async (headers) => {
          const res = await fetch(`${proxyUrl}/v3/ai/language-model`, { method: 'POST', headers, body: '{}' });
          await res.text();
          return res.status;
        })
      )
    : undefined;
  return { request: seen[before]!, refused };
}

describe('the Gateway proxy, with real OpenCode', () => {
  const request = { url: '/v3/ai/language-model', authorization: 'Bearer the-drives-credential', model: 'anthropic/claude-haiku-4.5' };
  for (const env of ['with', 'without'] as const) {
    it(`${env} AI_GATEWAY_API_KEY in the environment: the model request reaches the Gateway with the drive's credential, a request without the key gets a 401`, { timeout: 90_000 }, async () => {
      if (env === 'without') delete process.env.AI_GATEWAY_API_KEY;
      try {
        expect(await firstRequest()).toEqual({ request, refused: [401, 401] });
      } finally {
        process.env.AI_GATEWAY_API_KEY = 'a-key-in-the-environment';
      }
    });
  }
});

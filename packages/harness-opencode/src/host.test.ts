import { mkdtempSync, readFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runHost } from './host.ts';
import { type FromHost, type HostInit, pipePair, type ToHost } from './protocol.ts';
import { blobHashes, eventName, type OpenCodeEvent } from './records.ts';

/**
 * The OpenCode host in this process, against real OpenCode 2.0.22 and no
 * network: rebuilding a session from recorded events (the fixtures), and
 * the outbox and the model barrier on a new one. The Gateway is a local
 * server that counts requests: a refused barrier means none reaches it.
 */

interface Fixture {
  runId: string;
  events: OpenCodeEvent[];
  blobs: { hash: string; value: unknown }[];
}
const fixture = (name: string) => JSON.parse(readFileSync(new URL(`../test/fixtures/${name}.json`, import.meta.url), 'utf8')) as Fixture;

let root: string;
let gateway: Server;
let gatewayUrl: string;
let gatewayHits = 0;
beforeAll(async () => {
  root = mkdtempSync(join(tmpdir(), 'oc-host-'));
  gateway = createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/v1/models') {
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ data: [] }));
      return;
    }
    gatewayHits++;
    res.writeHead(500).end('no model calls in this test');
  });
  await new Promise<void>((r) => gateway.listen(0, '127.0.0.1', r));
  gatewayUrl = `http://127.0.0.1:${(gateway.address() as AddressInfo).port}/v3/ai`;
});
afterAll(() => gateway.close());

let n = 0;
function init(overrides: Partial<HostInit>): HostInit {
  const dir = join(root, `s${++n}`);
  return {
    sessionId: `ses_test_${n}`,
    directory: join(dir, 'workspace'),
    dbPath: join(dir, 'opencode.db'),
    configDir: join(dir, 'config'),
    global: join(dir, 'global'),
    model: 'vercel/anthropic/claude-haiku-4.5',
    gatewayBaseURL: gatewayUrl,
    gatewayKey: 'placeholder',
    tools: [{ name: 'echo', description: 'Echo the input.', inputSchema: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] } }],
    builtins: [],
    system: '',
    title: 'test',
    events: [],
    blobs: [],
    claim: false,
    restarts: 0,
    ...overrides,
  };
}

function start(i: HostInit) {
  const { left, right } = pipePair<FromHost, ToHost>();
  const done = runHost(i, right).finally(() => right.close());
  const messages = left.messages[Symbol.asyncIterator]();
  const next = async (): Promise<FromHost> => {
    const r = await messages.next();
    if (r.done) throw new Error('the host ended');
    return r.value;
  };
  const close = async () => {
    left.send({ type: 'close' });
    await done;
  };
  return { left, next, close, done };
}

const fromFixture = (f: Fixture, upTo = Number.POSITIVE_INFINITY): Partial<HostInit> => {
  const created = f.events[0]!;
  return {
    sessionId: String(created.data.sessionID),
    directory: String(created.data.location.directory),
    events: f.events.filter((e) => e.seq <= upTo),
    blobs: f.blobs,
  };
};

function inspect(dbPath: string, sessionId: string) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const session = db.prepare('select time_suspended, resume_attempts from session_v2 where id = ?').get(sessionId) as
      | { time_suspended: number | null; resume_attempts: number }
      | undefined;
    const messages = db.prepare('select type, data from session_message where session_id = ? order by seq').all(sessionId) as { type: string; data: string }[];
    const blobs = (db.prepare('select count(*) as n from instruction_blob').get() as { n: number }).n;
    const events = (db.prepare('select count(*) as n from event where aggregate_id = ?').get(sessionId) as { n: number }).n;
    return { session, messages, blobs, events };
  } finally {
    db.close();
  }
}

describe('the OpenCode host', () => {
  it('rebuilds a finished session from its journaled events and blobs alone', async () => {
    const f = fixture('turn');
    const i = init(fromFixture(f));
    const h = start(i);
    expect(await h.next()).toEqual({ type: 'ready', seq: 28 });
    await h.close();
    const db = inspect(i.dbPath, i.sessionId);
    // Every event replayed, through OpenCode's own projectors.
    expect(db.events).toBe(29);
    expect(db.blobs).toBe(f.blobs.length);
    expect(db.session).toEqual({ time_suspended: null, resume_attempts: 0 });
    const types = db.messages.map((m) => m.type);
    expect(types[0]).toBe('user');
    expect(JSON.stringify(db.messages[0])).toContain('haiku');
    expect(types.filter((t) => t === 'assistant').length).toBeGreaterThanOrEqual(3);
    expect(gatewayHits).toBe(0);
  });

  it('rebuilds a turn a crash cut off with its execution claim and resume count', async () => {
    const f = fixture('crash');
    const i = init({ ...fromFixture(f, 9), claim: true, restarts: 2 });
    const h = start(i);
    expect(await h.next()).toEqual({ type: 'ready', seq: 9 });
    await h.close();
    const db = inspect(i.dbPath, i.sessionId);
    expect(db.events).toBe(10);
    expect(db.session!.time_suspended).toEqual(expect.any(Number));
    expect(db.session!.resume_attempts).toBe(2);
    // The interrupted call is in OpenCode's history: its restart sweep settles it.
    expect(JSON.stringify(db.messages)).toContain(String(f.events.find((e) => eventName(e) === 'session.tool.called')!.data.id));
  });

  it('a session created with a working directory of the machine that drove it (before /workspace) takes the next prompt: OpenCode reaches its model barrier', async () => {
    const f = fixture('turn');
    const i = init(fromFixture(f));
    expect(i.directory).toMatch(/^\/tmp\/harness-opencode\//);
    const h = start(i);
    expect(await h.next()).toEqual({ type: 'ready', seq: 28 });
    h.left.send({ type: 'prompt', id: 1, messageId: 'msg_test_next', text: 'and again', delivery: 'queue' });
    let barrier: Extract<FromHost, { type: 'barrier' }> | undefined;
    let reply: Extract<FromHost, { type: 'reply' }> | undefined;
    while (!barrier) {
      const m = await h.next();
      if (m.type === 'reply' && m.id === 1) reply = m;
      else if (m.type === 'barrier') barrier = m;
      else if (m.type === 'error') throw new Error(m.message);
    }
    expect(reply).toMatchObject({ ok: true });
    h.left.send({ type: 'reply', id: barrier.id, ok: false, error: 'refused' });
    await h.close();
  });

  it('ships what OpenCode commits, with its blobs, before the model barrier; a refused barrier makes no model call', async () => {
    const i = init({});
    const h = start(i);
    const created = await h.next();
    expect(created).toMatchObject({ type: 'events', events: [{ seq: 0, type: 'session.created.1' }] });
    expect(await h.next()).toEqual({ type: 'ready', seq: 0 });

    h.left.send({ type: 'prompt', id: 1, messageId: 'msg_test_1', text: 'hello', delivery: 'queue' });
    const shipped: OpenCodeEvent[] = [];
    const blobs: string[] = [];
    let barrier: Extract<FromHost, { type: 'barrier' }> | undefined;
    let replied = false;
    while (!barrier) {
      const m = await h.next();
      if (m.type === 'events') {
        shipped.push(...m.events);
        blobs.push(...m.blobs.map((b) => b.hash));
      } else if (m.type === 'reply') replied = m.id === 1 && m.ok;
      else if (m.type === 'barrier') barrier = m;
    }
    expect(replied).toBe(true);
    expect(barrier.kind).toBe('primary');
    // Contiguous from seq 1, the prompt admitted under our id, its instruction blobs alongside.
    expect(shipped.map((e) => e.seq)).toEqual(shipped.map((_, k) => k + 1));
    expect(shipped.map((e) => eventName(e))).toEqual(
      expect.arrayContaining(['session.inbox.enqueued', 'session.execution.started', 'session.instructions.updated', 'session.inbox.delivered'])
    );
    expect(shipped.find((e) => eventName(e) === 'session.inbox.enqueued')!.data.inboxID).toBe('msg_test_1');
    const wanted = shipped.flatMap(blobHashes);
    expect(wanted.length).toBeGreaterThan(0);
    expect(blobs.sort()).toEqual([...new Set(wanted)].sort());

    h.left.send({ type: 'reply', id: barrier.id, ok: false, error: 'refused' });
    await new Promise((r) => setTimeout(r, 1500));
    expect(gatewayHits).toBe(0);
    await h.close();
  });
});

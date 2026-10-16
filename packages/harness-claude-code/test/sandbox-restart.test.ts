/**
 * Restart on a new sandbox, in CI (docs/workspaces.md §11.4, R1-R8): the
 * **real Agent SDK and the pinned `claude` binary** (the host build of the
 * pin, `findClaudeExecutable()`), on the sandbox tier's stand-in VM
 * (`stand-in.ts`: the real session factory over local relays, a temp dir per
 * Drive), with the model a scripted Anthropic Messages API that enforces the
 * API's structure (`fake-anthropic.ts`). No credentials, no network.
 *
 * Each invocation is its own World and its own harness (a new Function: no
 * Function-side cache survives, the CLI's config dir is the drive's own temp
 * dir); a crash closes the World the drive ran in and a second one recovers
 * the session from the log alone. The platform (the stand-in's VMs and Drives)
 * outlives them, as Vercel's does. The design ported `real.test.ts`'s worker
 * processes; in one process a "crash" is the kernel's chaos hook plus the
 * World closing under the drive, which is what the recovery sees either way
 * (the log, the VM, the Drive), and the stand-in platform can't be shared
 * across processes.
 */
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { type DurableSessions, durableSessions, QUEUE_PREFIX, readDoc, type SessionState, UNAVAILABLE_STEP } from 'harness-world';
import { journalRecords, type SessionMachine, SPEND_STREAM, transcriptOf } from 'harness-world-process';
import { FILES_LOST_NOTE, readDriveManifest, SANDBOX_DOC, sessionName } from 'harness-world-sandbox';
import type { FakeSandboxApi } from 'harness-world-sandbox/testing/fake-api';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { findClaudeExecutable } from '../src/binary.ts';
import { durableEnv, PROJECT_DIR_NAME } from '../src/env.ts';
import { CLAUDE_BUILTINS, CLAUDE_BUILTINS_OFF, claudeCodeHarness } from '../src/harness.ts';
import { uuidFrom } from '../src/ids.ts';
import { CLAUDE_STREAM, type TranscriptLine } from '../src/store.ts';
import { CLAUDE_CODE_VERSION } from '../src/versions.ts';
import { type FakeAnthropic, fakeAnthropic, type FakeReply, type FakeRequest, lastUserText } from './fake-anthropic.ts';
import { standIn } from './stand-in.ts';

const executable = findClaudeExecutable();
const MODEL = 'claude-haiku-4-5';

const worlds: LocalWorld[] = [];
const apis: FakeSandboxApi[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.().catch(() => {});
  for (const a of apis.splice(0)) await a.dispose();
});

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 60_000, what = 'the condition'): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

const turns = (state: SessionState) => transcriptOf(state).filter((r) => r.data.type === 'turn').map((r) => r.data as { type: 'turn'; status: string });
const waitTurns = (sessions: DurableSessions, id: string, n: number) =>
  until(
    async () => {
      const s = await sessions.inspect(id);
      return turns(s).length >= n ? s : undefined;
    },
    90_000,
    `${n} turns`
  );
const native = (state: SessionState) => journalRecords<TranscriptLine>(state, CLAUDE_STREAM).map((r) => r.data);
const entries = (state: SessionState) => transcriptOf(state).map((r) => r.data as { type: string; role?: string; text?: string; synthetic?: string; status?: string });

/** The model's main requests (the CLI's own: they offer its tools); side calls (titles) offer none. */
const main = (r: FakeRequest) => r.tools.length > 0;
const hasResults = (r: FakeRequest) => Object.keys(r.results).length > 0;

interface Invocation {
  world: LocalWorld;
  sessions: DurableSessions;
  drained: () => Promise<void>;
  logs: { message: string; fields?: Record<string, unknown> }[];
  /** Every delivery that failed (a non-2xx answer to the queue: a redelivery, which counts toward the kernel's failing limit). */
  failed: { status: number; body: string }[];
}

/**
 * One invocation: a World on `dataDir`, a fresh harness with Claude Code's own
 * tools on, the real SDK, the pinned binary on the stand-in VM, the model at
 * `model`. `crash` makes the kernel's chaos hook available.
 */
async function invocation(
  dataDir: string,
  api: FakeSandboxApi,
  model: FakeAnthropic,
  opts: {
    recover?: boolean;
    crash?: () => void;
    commandTimeoutMs?: number;
    builtins?: boolean;
    executable?: string;
    budget?: { turnUsd: number; sessionUsd: number };
    /** The session's model (default `MODEL`). */
    model?: string;
    /** Sees each machine the invocation's drives attach. */
    onAttach?: (machine: SessionMachine) => void;
  } = {}
): Promise<Invocation> {
  const world = createWorld({ dataDir, recoverActiveRuns: opts.recover ?? false });
  worlds.push(world);
  const logs: Invocation['logs'] = [];
  const log = (message: string, fields?: Record<string, unknown>) => void logs.push({ message, ...(fields ? { fields } : {}) });
  const { sandbox } = standIn({ api, log, ...(opts.commandTimeoutMs ? { commandTimeoutMs: opts.commandTimeoutMs } : {}), ...(opts.budget ? { budget: opts.budget } : {}) });
  const sessions = durableSessions({
    world,
    harness: claudeCodeHarness({
      builtins: opts.builtins ?? true,
      sandbox: opts.onAttach
        ? {
            ...sandbox,
            attach: async (ctx, request) => {
              const machine = await sandbox.attach(ctx, request);
              opts.onAttach!(machine);
              return machine;
            },
          }
        : sandbox,
      executable: opts.executable ?? executable!,
      model: opts.model ?? MODEL,
      gatewayUrl: model.url,
      credential: async () => 'fake-gateway-credential',
      // The stand-in VM is this machine: no bubblewrap on macOS.
      unsandboxedCommands: true,
      log,
    }),
    ...(opts.crash ? { chaos: true, crash: opts.crash } : {}),
  });
  let inflight = 0;
  const waiters: (() => void)[] = [];
  const failed: Invocation['failed'] = [];
  world.registerHandler(QUEUE_PREFIX, async (request: Request) => {
    inflight++;
    try {
      const response = await sessions.handler(request);
      if (response.status >= 300) failed.push({ status: response.status, body: (await response.clone().text()).slice(0, 300) });
      return response;
    } finally {
      if (--inflight === 0) for (const r of waiters.splice(0)) r();
    }
  });
  await world.start?.();
  return { world, sessions, logs, failed, drained: () => (inflight === 0 ? Promise.resolve() : new Promise<void>((r) => waiters.push(r))) };
}

function platform(): FakeSandboxApi {
  const { api } = standIn();
  apis.push(api);
  return api;
}

const nameOf = (sessionId: string) => sessionName('prj_stand_in', sessionId);
/** Tear the stand-in VM down: its relay killed, its rootfs and cache dirs gone; the Drive dir stays (a deleted Vercel Sandbox). */
async function deleteVm(api: FakeSandboxApi, sessionId: string): Promise<void> {
  const vm = await api.getSandbox(nameOf(sessionId));
  await vm?.delete();
}

/** A script from prompt phrases: a tool result is answered with its text, a prompt by the first rule it contains, anything else with `ok`. */
function respond(rules: [phrase: string, reply: FakeReply | ((r: FakeRequest) => FakeReply | Promise<FakeReply>)][]): (r: FakeRequest) => FakeReply | Promise<FakeReply> {
  return (r) => {
    if (hasResults(r)) return { text: `Result: ${Object.values(r.results).join(' | ').trim() || 'empty'}` };
    const text = lastUserText(r);
    for (const [phrase, reply] of rules) if (text.includes(phrase)) return typeof reply === 'function' ? reply(r) : reply;
    return { text: 'ok' };
  };
}

/**
 * A TCP proxy between the Function and the stand-in VM's relay port, so a
 * test can drop one of the link's two sockets (`/ctl`, `/model`) and leave
 * the other, or hold a `/ctl` request (`holdCtl`: that frame and everything
 * after it on its socket never reaches the relay). It forwards to whatever
 * `target` is when a connection opens.
 */
async function relayProxy(): Promise<{ url: string; target: number; drop(path: '/ctl' | '/model'): number; holdCtl(type: number): Promise<void>; close(): Promise<void> }> {
  const { createServer, connect } = await import('node:net');
  interface Conn {
    path?: string;
    client: import('node:net').Socket;
    upstream: import('node:net').Socket;
    /** Function-to-relay bytes not yet forwarded (an incomplete WebSocket frame), and whether the upgrade request is past. */
    pending: Buffer;
    upgraded: boolean;
    held: boolean;
  }
  const conns: Conn[] = [];
  let hold: { type: number; seen: () => void } | undefined;
  /** Forward what is complete of `c.pending`: the upgrade request, then whole frames; a held frame stops the socket's forwarding for good. */
  const pump = (c: Conn) => {
    for (;;) {
      if (c.held) return;
      if (!c.upgraded) {
        const end = c.pending.indexOf('\r\n\r\n');
        if (end < 0) return;
        c.path = /^GET (\/[a-z]+)/.exec(c.pending.subarray(0, end).toString('latin1'))?.[1];
        c.upstream.write(c.pending.subarray(0, end + 4));
        c.pending = c.pending.subarray(end + 4);
        c.upgraded = true;
        continue;
      }
      const b = c.pending;
      if (b.length < 2) return;
      let len = b[1]! & 0x7f;
      let off = 2;
      if (len === 126) {
        if (b.length < 4) return;
        len = b.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (b.length < 10) return;
        len = Number(b.readBigUInt64BE(2));
        off = 10;
      }
      const mask = b[1]! & 0x80 ? 4 : 0;
      if (b.length < off + mask + len) return;
      const first = len > 0 ? b[off + mask]! ^ (mask ? b[off]! : 0) : -1;
      if (c.path === '/ctl' && hold && (b[0]! & 0x0f) === 2 && first === hold.type) {
        c.held = true;
        const h = hold;
        hold = undefined;
        h.seen();
        return;
      }
      c.upstream.write(b.subarray(0, off + mask + len));
      c.pending = b.subarray(off + mask + len);
    }
  };
  const state = {
    target: 0,
    url: '',
    drop(path: '/ctl' | '/model') {
      let n = 0;
      for (const c of conns) {
        if (c.path !== path || c.client.destroyed) continue;
        c.client.destroy();
        c.upstream.destroy();
        n++;
      }
      return n;
    },
    holdCtl: (type: number) => new Promise<void>((seen) => (hold = { type, seen })),
    close: () => new Promise<void>((r) => {
      for (const c of conns) c.client.destroy(), c.upstream.destroy();
      server.close(() => r());
    }),
  };
  const server = createServer((client) => {
    const upstream = connect(state.target, '127.0.0.1');
    const conn: Conn = { client, upstream, pending: Buffer.alloc(0), upgraded: false, held: false };
    conns.push(conn);
    client.on('data', (chunk: Buffer) => {
      conn.pending = Buffer.concat([conn.pending, chunk]);
      pump(conn);
    });
    upstream.pipe(client);
    client.on('error', () => upstream.destroy());
    upstream.on('error', () => client.destroy());
    client.on('close', () => upstream.destroy());
    upstream.on('close', () => client.destroy());
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  state.url = `http://127.0.0.1:${(server.address() as import('node:net').AddressInfo).port}`;
  return state;
}

/** The pids of the pinned CLI running a session now (the stand-in runs it on this machine). */
async function cliPids(sessionId: string): Promise<number[]> {
  const id = uuidFrom(sessionId);
  const { execFileSync } = await import('node:child_process');
  const out = execFileSync('ps', ['-axo', 'pid=,command='], { encoding: 'utf8' });
  return out
    .split('\n')
    .filter((l) => l.includes(executable!) && l.includes(id))
    .map((l) => Number(l.trim().split(/\s+/)[0]));
}

const bashCalls = (state: SessionState) => state.effectOrder.filter((e) => e.name === 'tool.Bash');
const commandOf = (e: { input?: unknown }) => String((e.input as { command?: unknown } | undefined)?.command ?? '');
/** The relay protocol's `FSYNC` request (harness-sandbox's `CTL.FSYNC`). */
const CTL_FSYNC = 21;
const refused = (from: number, m: FakeAnthropic) => m.requests.slice(from).filter((r) => r.status === 400).map((r) => r.refused);

describe.runIf(!!executable)('restart on a new sandbox: the real Agent SDK and the pinned CLI on the stand-in VM', () => {
  let model: FakeAnthropic;
  let script: (r: FakeRequest) => FakeReply | Promise<FakeReply>;
  beforeAll(async () => {
    model = await fakeAnthropic({ reply: (r) => (main(r) ? script(r) : { text: 'A title' }) });
  });
  afterAll(async () => {
    await model?.close();
  });

  it("a model that takes an effort (claude-sonnet-5-5): the CLI's output_config passes the gateway, and the turn completes", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-effort-'));
    const api = platform();
    script = () => ({ text: 'ok' });
    const from = model.requests.length;
    const inv = await invocation(dataDir, api, model, { model: 'claude-sonnet-5-5' });
    const { sessionId } = await inv.sessions.start();
    await inv.sessions.send(sessionId, 'Say ok.');
    expect(turns(await waitTurns(inv.sessions, sessionId, 1))).toEqual([{ type: 'turn', status: 'completed' }]);
    await inv.drained();
    expect(model.messages().slice(from).find(main)?.body.output_config).toMatchObject({ effort: expect.any(String) });
  });

  it('R1: a file and a code word on VM A; VM A deleted, the Drive kept; VM B reads the file and recalls the word from the log', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r1-'));
    const api = platform();
    script = (r) => {
      const prompt = lastUserText(r);
      if (hasResults(r)) return { text: `Done: ${Object.values(r.results).join(' | ').trim() || 'ok'}` };
      if (prompt.includes('Write it')) return { tool: { name: 'Bash', input: { command: 'echo heron > note.txt', description: 'write the word' } } };
      if (prompt.includes('What is the code word')) return { tool: { name: 'Bash', input: { command: 'cat note.txt', description: 'read the file' } } };
      return { text: 'ok' };
    };
    const from = model.requests.length;
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'The code word is heron. Write it to note.txt.');
    const one = await waitTurns(a.sessions, sessionId, 1);
    expect(turns(one)).toEqual([{ type: 'turn', status: 'completed' }]);
    await a.drained();
    await a.world.close?.();
    const drive = api.driveDir(nameOf(sessionId));
    expect(readFileSync(join(drive, 'note.txt'), 'utf8')).toBe('heron\n');
    const journal = native(one);
    expect(journal.length).toBeGreaterThan(0);
    // The first request of a fresh session: no resume, the CLI's own tools offered (the allowlist only).
    const first = model.messages().slice(from).find(main)!;
    expect(first.messages).toHaveLength(1);
    expect(first.tools.filter((t) => !t.startsWith('mcp__')).sort()).toEqual([...CLAUDE_BUILTINS].sort());

    await deleteVm(api, sessionId);
    const turn2From = model.requests.length;
    const b = await invocation(dataDir, api, model);
    // The Files panel's snapshot of the Drive (docs/workspaces.md §8): in the log after turn 1, readable with VM A gone and no VM yet.
    expect(readDriveManifest(await b.sessions.inspect(sessionId))?.files.find((f) => f.path === '/workspace/note.txt')).toMatchObject({ text: 'heron\n', bytes: 6 });
    let upload: string | undefined;
    // What the new VM's config dir holds when the CLI starts: the resume files, from the log.
    const sessionFile = () => {
      const relay = api.relayOf(nameOf(sessionId));
      const p = relay && join(relay.config.cacheRoot, 'config', 'projects', PROJECT_DIR_NAME, `${uuidFrom(sessionId)}.jsonl`);
      return p && existsSync(p) ? p : undefined;
    };
    await b.sessions.send(sessionId, 'What is the code word? Check note.txt.');
    const watch = until(async () => {
      const p = sessionFile();
      if (p) upload = readFileSync(p, 'utf8');
      return p ? true : undefined;
    });
    const two = await waitTurns(b.sessions, sessionId, 2);
    await watch;
    expect(turns(two).at(-1)).toEqual({ type: 'turn', status: 'completed' });
    const resumed = model.messages().slice(turn2From).filter(main);
    // The resumed request carries turn 1: the code word, the Bash call and its result.
    expect(resumed[0]!.text).toContain('The code word is heron');
    expect(resumed[0]!.messages.length).toBeGreaterThan(2);
    expect(resumed.every((r) => r.status === 200)).toBe(true);
    expect(Object.values(resumed.at(-1)!.results).join('')).toContain('heron');
    // The upload was the journal's transcript (the lines the log held before turn 2), line for line: the CLI only appends to it.
    // (A large entry, the prompt snapshot, is a blob in the log, not inline in the state: that line is checked for its type.)
    const records = journalRecords<TranscriptLine>(one, CLAUDE_STREAM);
    const lines = upload!.split('\n');
    records.forEach((r, i) => {
      if (r.blob) expect(JSON.parse(lines[i]!)).toMatchObject({ type: expect.any(String) });
      else expect(lines[i]).toBe(JSON.stringify(r.data));
    });
    // Turn 2's frames were mapped into the log (the store has turn 2's entries).
    expect(native(two).length).toBeGreaterThan(journal.length);
    expect(b.logs.filter((l) => /mirror_unmapped/.test(l.message))).toEqual([]);
    expect(entries(two).filter((e) => e.type === 'message' && e.role === 'assistant').at(-1)?.text).toContain('heron');
  }, 180_000);

  it('R2: the VM killed mid-Bash: the intent is in the log, the call comes back interrupted with the half-run note, never rerun', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r2-'));
    const api = platform();
    script = respond([['Run the slow job', { tool: { name: 'Bash', input: { command: 'echo started >> runs.txt; sleep 5; echo done >> runs.txt', description: 'slow job' } } }]]);
    const from = model.requests.length;
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Run the slow job.');
    const drive = await until(async () => (api.drives.has(nameOf(sessionId)) ? api.driveDir(nameOf(sessionId)) : undefined), 60_000, 'the Drive');
    await until(async () => (existsSync(join(drive, 'runs.txt')) ? true : undefined), 60_000, 'the command to start');
    // The intent was committed before the command ran.
    expect(bashCalls(await a.sessions.inspect(sessionId)).map((e) => e.status)).toEqual(['running']);
    await api.crash(nameOf(sessionId));
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(bashCalls(state)).toHaveLength(1);
    expect(bashCalls(state)[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
    expect(readFileSync(join(drive, 'runs.txt'), 'utf8').match(/started/g)).toHaveLength(1);
    // The model heard it: the recovery note names the call as interrupted, not safe to repeat.
    const later = model.messages().slice(from).filter(main);
    expect(later.some((r) => r.text.includes('interrupted before it reported a result') && r.text.includes('may have taken effect'))).toBe(true);
    expect(refused(from, model)).toEqual([]);
    expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
  }, 180_000);

  it('R3: the driver killed while the VM lives: the next invocation takes the VM over with a higher fence, the call is interrupted, never rerun', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r3-'));
    const api = platform();
    script = respond([['Run the slow job', { tool: { name: 'Bash', input: { command: 'echo started >> runs.txt; sleep 6; echo done >> runs.txt', description: 'slow job' } } }]]);
    const from = model.requests.length;
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const a = await invocation(dataDir, api, model, { crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 1500 });
    await a.sessions.send(sessionId, 'Run the slow job.');
    await died;
    await a.world.close?.();
    const before = await cliPids(sessionId);
    expect(before.length).toBeGreaterThan(0);
    const vm = (await api.getSandbox(nameOf(sessionId)))!;

    const b = await invocation(dataDir, api, model, { recover: true, crash: () => {} });
    const state = await waitTurns(b.sessions, sessionId, 1);
    // The same VM, taken over: its old CLI killed by the higher fence's arm.
    expect((await api.getSandbox(nameOf(sessionId)))!.sessionId).toBe(vm.sessionId);
    for (const pid of before) expect(alive(pid)).toBe(false);
    expect(bashCalls(state)).toHaveLength(1);
    expect(bashCalls(state)[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
    const drive = api.driveDir(nameOf(sessionId));
    expect(readFileSync(join(drive, 'runs.txt'), 'utf8').match(/started/g)).toHaveLength(1);
    const note = entries(state).find((e) => e.type === 'message' && e.role === 'user' && e.synthetic === 'recovery');
    expect(note?.text).toContain('interrupted before it reported a result');
    expect(refused(from, model)).toEqual([]);
    expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
  }, 180_000);

  it('R5: the driver killed mid model stream: the next invocation resumes, the conversation the API sees stays well formed', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r5a-'));
    const api = platform();
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    let first = true;
    script = respond([
      [
        'Tell me a story',
        () => {
          if (!first) return { text: 'Once upon a time, again.' };
          first = false;
          return { hold: held, then: { text: 'Once upon a time.' } };
        },
      ],
    ]);
    const from = model.requests.length;
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const a = await invocation(dataDir, api, model, { crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'model', afterMs: 500 });
    await a.sessions.send(sessionId, 'Tell me a story.');
    await died;
    await a.world.close?.();
    release();
    const b = await invocation(dataDir, api, model, { recover: true, crash: () => {} });
    const state = await waitTurns(b.sessions, sessionId, 1);
    expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
    expect(refused(from, model)).toEqual([]);
    // The recovery's request still carries the prompt the crash cut off.
    expect(model.messages().slice(from).filter(main).at(-1)!.text).toContain('Tell me a story');
  }, 180_000);

  for (const path of ['/ctl', '/model'] as const) {
    it(`R5: drop ${path} only, mid-turn: the drive re-attaches in the same invocation, and the turn completes`, async () => {
      const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r5-'));
      const api = platform();
      const proxy = await relayProxy();
      api.route = (_vm, url) => {
        proxy.target = Number(new URL(url).port);
        return proxy.url;
      };
      let release!: () => void;
      const held = new Promise<void>((r) => (release = r));
      let asked = 0;
      script = respond([
        [
          'Say hello',
          () => (++asked === 1 ? { hold: held, then: { text: 'Hello there.' } } : { text: 'Hello again.' }),
        ],
      ]);
      const from = model.requests.length;
      const a = await invocation(dataDir, api, model);
      try {
        const { sessionId } = await a.sessions.start();
        await a.sessions.send(sessionId, 'Say hello.');
        await until(async () => (asked > 0 ? true : undefined), 60_000, 'the model call');
        expect(proxy.drop(path)).toBeGreaterThan(0);
        setTimeout(release, 300);
        const state = await waitTurns(a.sessions, sessionId, 1);
        expect(turns(state)).toEqual([{ type: 'turn', status: 'completed' }]);
        expect(state.effectOrder.some((e) => e.name === UNAVAILABLE_STEP)).toBe(false);
        expect(refused(from, model)).toEqual([]);
        expect(a.logs.some((l) => l.message === 'relay link lost')).toBe(true);
        // Recovered in the same delivery: no failed delivery, no redelivery (each would count toward the kernel's failing limit).
        expect(a.failed).toEqual([]);
        expect(a.logs.some((l) => l.message.includes('the drive is rebuilt from the log'))).toBe(true);
      } finally {
        release();
        await proxy.close();
      }
    }, 180_000);
  }

  it('R5: /model dropped right after a charged response: its spend, recorded before the drop, is still counted after the drive is rebuilt', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r5s-'));
    const api = platform();
    const proxy = await relayProxy();
    api.route = (_vm, url) => {
      proxy.target = Number(new URL(url).port);
      return proxy.url;
    };
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    let results = 0;
    // The first response (a Read) is charged; the request after its result is held, and the link drops under it.
    script = (r) => {
      if (hasResults(r)) return ++results === 1 ? { hold: held, then: { text: 'Read it.' } } : { text: 'Read it again.' };
      return lastUserText(r).includes('Read the file') ? { tool: { name: 'Read', input: { file_path: 'missing.txt' } } } : { text: 'ok' };
    };
    const a = await invocation(dataDir, api, model);
    try {
      const { sessionId } = await a.sessions.start();
      await a.sessions.send(sessionId, 'Read the file missing.txt.');
      await until(async () => (results > 0 ? true : undefined), 60_000, 'the request after the tool result');
      expect(proxy.drop('/model')).toBeGreaterThan(0);
      setTimeout(release, 300);
      const state = await waitTurns(a.sessions, sessionId, 1);
      expect(turns(state)).toEqual([{ type: 'turn', status: 'completed' }]);
      expect(a.logs.some((l) => l.message.includes('the drive is rebuilt from the log'))).toBe(true);
      // The Read's response before the drop, and the recovery's after it: both charged in the log the caps read.
      expect(journalRecords<{ usd: number }>(state, SPEND_STREAM).length).toBeGreaterThanOrEqual(2);
    } finally {
      release();
      await proxy.close();
    }
  }, 180_000);

  it('R5: the CLI killed (an OOM kill) mid-turn: the drive starts it again from the log in the same invocation, and the turn completes', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r5o-'));
    const api = platform();
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    let asked = 0;
    script = respond([['Say hello', () => (++asked === 1 ? { hold: held, then: { text: 'Hello there.' } } : { text: 'Hello again.' })]]);
    const from = model.requests.length;
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Say hello.');
    await until(async () => (asked > 0 ? true : undefined), 60_000, 'the model call');
    const pids = await cliPids(sessionId);
    expect(pids.length).toBeGreaterThan(0);
    for (const pid of pids) process.kill(pid, 'SIGKILL');
    release();
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(turns(state)).toEqual([{ type: 'turn', status: 'completed' }]);
    expect(refused(from, model)).toEqual([]);
    expect(a.failed).toEqual([]);
    expect(a.logs.some((l) => l.message.includes('the drive is rebuilt from the log'))).toBe(true);
  }, 180_000);

  it('R6: the old VM session still holds the Drive (detach lag): the next VM waits for it, then resumes', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r6-'));
    const { api } = standIn({ api: new (await import('harness-world-sandbox/testing/fake-api')).FakeSandboxApi({ detachMs: 4000 }) });
    apis.push(api);
    script = respond([
      ['Write the word', { tool: { name: 'Write', input: { file_path: 'word.txt', content: 'kestrel\n' } } }],
      ['Read the word', { tool: { name: 'Read', input: { file_path: 'word.txt' } } }],
    ]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Write the word kestrel to word.txt.');
    const one = await waitTurns(a.sessions, sessionId, 1);
    expect(one.effectOrder.find((e) => e.name === 'tool.Write')).toMatchObject({ status: 'completed' });
    await a.drained();
    await a.world.close?.();
    await deleteVm(api, sessionId);
    const b = await invocation(dataDir, api, model);
    const at = Date.now();
    await b.sessions.send(sessionId, 'Read the word back from word.txt.');
    const two = await waitTurns(b.sessions, sessionId, 2);
    expect(turns(two).at(-1)).toEqual({ type: 'turn', status: 'completed' });
    expect(Date.now() - at).toBeGreaterThan(2000);
    expect(api.calls.some((c) => c.startsWith('createSandbox'))).toBe(true);
    expect(two.effectOrder.find((e) => e.name === 'tool.Read')).toMatchObject({ status: 'completed' });
    expect(entries(two).filter((e) => e.type === 'message' && e.role === 'assistant').at(-1)?.text).toContain('kestrel');
  }, 180_000);

  it('R4: a zombie driver (stuck in a model call while a newer one took the session over) is refused when it wakes: its CLI is dead, its machine runs nothing, and it writes no intent', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r4-'));
    const api = platform();
    let release!: () => void;
    const stuck = new Promise<void>((r) => (release = r));
    let asked = 0;
    script = respond([
      [
        'Append a line',
        () => (++asked === 1 ? { hold: stuck, then: { tool: { name: 'Bash', input: { command: 'echo zombie >> lines.txt', description: 'append' } } } } : { tool: { name: 'Bash', input: { command: 'echo owner >> lines.txt', description: 'append' } } }),
      ],
    ]);
    const from = model.requests.length;
    const machines: SessionMachine[] = [];
    const a = await invocation(dataDir, api, model, { onAttach: (m) => void machines.push(m) });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Append a line.');
    await until(async () => (asked > 0 ? true : undefined), 60_000, 'the model call');
    const zombie = machines.at(-1)!;
    const before = await cliPids(sessionId);
    expect(before.length).toBeGreaterThan(0);
    // A newer driver takes the session over (its writes win the World's in-band fence, its arm a higher relay fence) while A is stuck.
    const b = await invocation(dataDir, api, model);
    await b.sessions.drive(sessionId);
    const state = await waitTurns(b.sessions, sessionId, 1);
    expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
    // The takeover's arm killed A's CLI and closed A's link; A's machine runs nothing now (no SPAWN, WIPE or FILES).
    for (const pid of before) expect(alive(pid)).toBe(false);
    expect(zombie.signal.aborted).toBe(true);
    await expect(zombie.spawn({ command: '/bin/sh', args: ['-c', 'echo zombie >> lines.txt'] })).rejects.toThrow();
    await expect(zombie.files.wipe()).rejects.toThrow();
    await expect(zombie.files.upload('config', [{ path: 'projects/x.jsonl', bytes: new Uint8Array([1]) }])).rejects.toThrow();
    // A wakes: the model answers its stuck call. Nothing it does reaches the VM or the log.
    release();
    await new Promise((r) => setTimeout(r, 3000));
    const after = await b.sessions.inspect(sessionId);
    const drive = api.driveDir(nameOf(sessionId));
    expect(readFileSync(join(drive, 'lines.txt'), 'utf8')).toBe('owner\n');
    expect(bashCalls(after).map((e) => commandOf(e))).toEqual(['echo owner >> lines.txt']);
    expect(refused(from, model)).toEqual([]);
    // B armed with the higher fence; A, rebuilt when its CLI died, never armed again (the World refused its drive first; the relay refuses a lower fence's arm: harness-sandbox's session tests).
    const fences = (inv: Invocation) => inv.logs.filter((l) => l.message === 'attached').map((l) => l.fields?.fence as number);
    expect(fences(a)).toHaveLength(1);
    expect(Math.min(...fences(b))).toBeGreaterThan(fences(a)[0]!);
    expect(a.failed).toEqual([]);
  }, 180_000);

  it('R6: a crash after the Write ran and its FSYNC went out, before its outcome was committed: the call comes back interrupted, never rerun', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r6f-'));
    const api = platform();
    const proxy = await relayProxy();
    api.route = (_vm, url) => {
      proxy.target = Number(new URL(url).port);
      return proxy.url;
    };
    script = respond([['Write the word', { tool: { name: 'Write', input: { file_path: 'word.txt', content: 'plover\n' } } }]]);
    const from = model.requests.length;
    const a = await invocation(dataDir, api, model);
    try {
      const { sessionId } = await a.sessions.start();
      const flushing = proxy.holdCtl(CTL_FSYNC);
      await a.sessions.send(sessionId, 'Write the word plover to word.txt.');
      await flushing;
      // The Write ran (its file is on the Drive) and its FSYNC is on its way: the driver dies before the outcome is committed.
      const drive = api.driveDir(nameOf(sessionId));
      expect(readFileSync(join(drive, 'word.txt'), 'utf8')).toBe('plover\n');
      expect((await a.sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'tool.Write')).toMatchObject({ status: 'running' });
      await a.world.close?.();
      const b = await invocation(dataDir, api, model, { recover: true });
      const state = await waitTurns(b.sessions, sessionId, 1);
      const writes = state.effectOrder.filter((e) => e.name === 'tool.Write');
      expect(writes).toHaveLength(1);
      expect(writes[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
      const later = model.messages().slice(from).filter(main);
      expect(later.some((r) => r.text.includes('interrupted before it reported a result') && r.text.includes('may have taken effect'))).toBe(true);
      expect(refused(from, model)).toEqual([]);
      expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
    } finally {
      await proxy.close();
    }
  }, 180_000);

  it('R7: three restarts across three VMs: every resume carries every earlier turn', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r7-'));
    const api = platform();
    const words = ['amber', 'basalt', 'cobalt'];
    script = respond(words.map((w, i) => [`Word ${i + 1} is ${w}`, { tool: { name: 'Bash', input: { command: `echo ${w} >> words.txt`, description: 'note it' } } }]));
    const vms: string[] = [];
    let sessionId = '';
    for (let i = 0; i < words.length; i++) {
      const inv = await invocation(dataDir, api, model);
      if (!sessionId) sessionId = (await inv.sessions.start()).sessionId;
      const from = model.requests.length;
      await inv.sessions.send(sessionId, `Word ${i + 1} is ${words[i]}. Note it in words.txt.`);
      const state = await waitTurns(inv.sessions, sessionId, i + 1);
      expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
      const sent = model.messages().slice(from).filter(main);
      for (const w of words.slice(0, i + 1)) expect(sent[0]!.text).toContain(`is ${w}`);
      expect(refused(from, model)).toEqual([]);
      vms.push((await api.getSandbox(nameOf(sessionId)))!.sessionId);
      await inv.drained();
      await inv.world.close?.();
      await deleteVm(api, sessionId);
    }
    expect(new Set(vms).size).toBe(3);
    expect(readFileSync(join(api.driveDir(nameOf(sessionId)), 'words.txt'), 'utf8')).toBe('amber\nbasalt\ncobalt\n');
  }, 240_000);

  it('R7: a resume past a compaction boundary, on a new VM: the API sees the summary, not the compacted turns', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r7c-'));
    const api = platform();
    const saved = script;
    script = respond([
      // The CLI's compaction call (it offers the tools, and asks for none): the summary.
      ['Respond with plain text only', { text: '<analysis>The user gave a code word.</analysis>\n<summary>SUMMARY-OSPREY: the user said the code word is osprey.</summary>' }],
      ['The code word is', { text: 'Noted.' }],
      ['What was the code word', (r) => ({ text: r.text.includes('SUMMARY-OSPREY') ? 'From the summary: osprey.' : 'I do not know.' })]]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'The code word is osprey.');
    await waitTurns(a.sessions, sessionId, 1);
    const side = model.requests.length;
    await a.sessions.send(sessionId, '/compact');
    const two = await waitTurns(a.sessions, sessionId, 2);
    expect(model.requests.slice(side).some((r) => r.path === '/v1/messages' && r.status === 200)).toBe(true);
    const compacted = native(two) as ({ isCompactSummary?: boolean; subtype?: string } | undefined)[];
    expect(compacted.some((e) => e?.isCompactSummary === true || e?.subtype === 'compact_boundary')).toBe(true);
    await a.drained();
    await a.world.close?.();
    await deleteVm(api, sessionId);
    const b = await invocation(dataDir, api, model);
    const from = model.requests.length;
    await b.sessions.send(sessionId, 'What was the code word?');
    const three = await waitTurns(b.sessions, sessionId, 3);
    expect(turns(three).at(-1)).toEqual({ type: 'turn', status: 'completed' });
    const resumed = model.messages().slice(from).filter(main)[0]!;
    expect(resumed.text).toContain('SUMMARY-OSPREY');
    expect(resumed.text).not.toContain('The code word is osprey');
    expect(refused(from, model)).toEqual([]);
    script = saved;
  }, 180_000);

  it("a planted /workspace/.claude (settings, CLAUDE.md) is ignored: no setting sources", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-plant-'));
    const api = platform();
    script = respond([
      ['Say hi', { text: 'hi' }],
      ['Run env', { tool: { name: 'Bash', input: { command: 'env | grep PLANTED || echo none', description: 'env' } } }],
    ]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Say hi.');
    await waitTurns(a.sessions, sessionId, 1);
    await a.drained();
    await a.world.close?.();
    await deleteVm(api, sessionId);
    // Planted on the Drive (by the model, a tool, or anyone with the files): settings that deny Bash and set an env var, and instructions.
    const drive = api.driveDir(nameOf(sessionId));
    await mkdir(join(drive, '.claude'), { recursive: true });
    await writeFile(join(drive, '.claude', 'settings.json'), JSON.stringify({ permissions: { deny: ['Bash'] }, env: { PLANTED: 'yes' } }));
    await writeFile(join(drive, '.claude', 'settings.local.json'), JSON.stringify({ permissions: { deny: ['Bash'] } }));
    await writeFile(join(drive, 'CLAUDE.md'), 'PLANTED-INSTRUCTIONS\n');
    await writeFile(join(drive, '.claude', 'CLAUDE.md'), 'PLANTED-LOCAL\n');
    const from = model.requests.length;
    const b = await invocation(dataDir, api, model);
    await b.sessions.send(sessionId, 'Run env.');
    const state = await waitTurns(b.sessions, sessionId, 2);
    const sent = model.messages().slice(from).filter(main);
    // Bash still ran (the planted deny did nothing), the planted env didn't reach it, and no planted instructions reached the model.
    expect(state.effectOrder.filter((e) => e.name === 'tool.Bash').map((e) => e.status)).toEqual(['completed']);
    expect(Object.values(sent.at(-1)!.results).join('')).toContain('none');
    expect(JSON.stringify(sent.map((r) => r.body.system))).not.toContain('PLANTED-');
    expect(sent.map((r) => r.text).join('\n')).not.toContain('PLANTED-INSTRUCTIONS\n');
  }, 180_000);

  it('a planted /workspace/.git/config (a clean filter, a monitor) never runs in the CLI: it gathers no git status of its own', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-plantgit-'));
    const api = platform();
    script = respond([
      ['Say hi', { text: 'hi' }],
      ['Say bye', { text: 'bye' }],
    ]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Say hi.');
    await waitTurns(a.sessions, sessionId, 1);
    await a.drained();
    await a.world.close?.();
    await deleteVm(api, sessionId);
    // Planted on the Drive (the model can write .git/config from Bash, inside Claude's sandbox): a clean filter
    // and a monitor that git runs on `git status`, which the CLI would run itself, outside that sandbox, for the
    // system prompt's git context. A tracked file changed in place (same size) makes status run the filter.
    const drive = api.driveDir(nameOf(sessionId));
    const { execFileSync } = await import('node:child_process');
    const git = (...args: string[]) => execFileSync('git', args, { cwd: drive, env: { PATH: process.env.PATH, HOME: drive, GIT_CONFIG_NOSYSTEM: '1' } });
    git('init', '-q');
    await writeFile(join(drive, 'tracked.txt'), 'a');
    git('add', 'tracked.txt');
    await writeFile(join(drive, '.gitattributes'), '* filter=planted\n');
    const marker = join(drive, 'planted-ran');
    await writeFile(join(drive, '.git', 'config'), `${readFileSync(join(drive, '.git', 'config'), 'utf8')}[core]\n\tfsmonitor = "touch ${marker}; false"\n[filter "planted"]\n\tclean = "touch ${marker}; cat"\n`);
    await new Promise((r) => setTimeout(r, 1100));
    await writeFile(join(drive, 'tracked.txt'), 'b');
    const b = await invocation(dataDir, api, model);
    const from = model.requests.length;
    await b.sessions.send(sessionId, 'Say hi.');
    await waitTurns(b.sessions, sessionId, 2);
    await b.sessions.send(sessionId, 'Say bye.');
    const state = await waitTurns(b.sessions, sessionId, 3);
    expect(turns(state).every((t) => t.status === 'completed')).toBe(true);
    expect(existsSync(marker)).toBe(false);
    expect(JSON.stringify(model.messages().slice(from).filter(main).map((r) => r.body.system))).not.toContain('git status at the start');
    // The vector is real: the same git status, run directly, runs the planted command.
    git('--no-optional-locks', 'status', '--short');
    expect(existsSync(marker)).toBe(true);
  }, 180_000);

  it('builtins: true matches the pinned CLI: every allowlisted tool is one of its system/init tools, and every init tool is known (a bump that adds one fails here)', async () => {
    const { query } = await import('@anthropic-ai/claude-agent-sdk');
    const root = await mkdtemp(join(tmpdir(), 'hwcc-init-'));
    for (const d of ['home', 'config', 'tmp', 'cache', 'workspace']) await mkdir(join(root, d), { recursive: true });
    const env = durableEnv({ home: join(root, 'home'), configDir: join(root, 'config'), tmpDir: join(root, 'tmp'), cacheDir: join(root, 'cache'), baseUrl: model.url, token: 'dhm-init-test-token-0000', model: MODEL });
    const q = query({
      prompt: 'hi',
      options: { pathToClaudeCodeExecutable: executable!, cwd: join(root, 'workspace'), env, settingSources: [], strictMcpConfig: true, tools: { type: 'preset', preset: 'claude_code' }, permissionMode: 'dontAsk', model: MODEL },
    });
    let tools: string[] = [];
    try {
      for await (const m of q) {
        if (m.type === 'system' && m.subtype === 'init') {
          tools = m.tools;
          break;
        }
      }
    } finally {
      q.close?.();
      await rm(root, { recursive: true, force: true });
    }
    expect(tools.length).toBeGreaterThan(0);
    for (const t of CLAUDE_BUILTINS) expect(tools).toContain(t);
    // Every tool this CLI has is either allowlisted, off for good (Task and Agent with them: subagents' sidechain resume is not proven), or one the harness decided about (P0.17): Skill is not on by default, WebSearch is provider-hosted.
    expect(CLAUDE_BUILTINS_OFF).toEqual(expect.arrayContaining(['Task', 'Agent']));
    const known = new Set<string>([...CLAUDE_BUILTINS, ...CLAUDE_BUILTINS_OFF, 'Skill', 'WebSearch']);
    expect(tools.filter((t) => !known.has(t))).toEqual([]);
  }, 60_000);

  it(`a golden transcript the previous pin (2.1.289) wrote resumes on the current one (${CLAUDE_CODE_VERSION}), and the API sees it well formed`, async () => {
    const golden = JSON.parse(readFileSync(new URL('./fixtures/golden-2.1.289.json', import.meta.url), 'utf8')) as { cli: string; projectKey: string; sessionId: string; entries: unknown[] };
    expect(golden.cli).toBe('2.1.289');
    const { query } = await import('@anthropic-ai/claude-agent-sdk');
    const root = await mkdtemp(join(tmpdir(), 'hwcc-golden-'));
    for (const d of ['home', 'tmp', 'cache', 'workspace']) await mkdir(join(root, d), { recursive: true });
    const projects = join(root, 'config', 'projects', golden.projectKey);
    await mkdir(projects, { recursive: true });
    await writeFile(join(projects, `${golden.sessionId}.jsonl`), golden.entries.map((e) => `${JSON.stringify(e)}\n`).join(''));
    const saved = script;
    script = (r) => ({ text: r.text.includes('heron') ? 'The code word was heron.' : 'I do not know.' });
    const from = model.requests.length;
    const env = durableEnv({ home: join(root, 'home'), configDir: join(root, 'config'), tmpDir: join(root, 'tmp'), cacheDir: join(root, 'cache'), baseUrl: model.url, token: 'dhm-golden-test-token-00', model: MODEL });
    const q = query({
      prompt: 'What was the code word?',
      options: { pathToClaudeCodeExecutable: executable!, cwd: join(root, 'workspace'), env, settingSources: [], strictMcpConfig: true, tools: [...CLAUDE_BUILTINS], permissionMode: 'dontAsk', model: MODEL, resume: golden.sessionId },
    });
    let result: { subtype?: string; result?: string; is_error?: boolean } | undefined;
    try {
      for await (const m of q) if (m.type === 'result') result = m as typeof result;
    } finally {
      script = saved;
      await rm(root, { recursive: true, force: true });
    }
    expect(result).toMatchObject({ subtype: 'success', is_error: false });
    expect(result!.result).toContain('heron');
    const sent = model.messages().slice(from).filter(main);
    expect(sent.length).toBeGreaterThan(0);
    expect(sent.every((r) => r.status === 200)).toBe(true);
    // The old pin's turn is in the conversation: its prompt, its Bash call and that call's result.
    expect(sent[0]!.text).toContain('The code word is heron');
    expect(sent[0]!.messages.some((m) => Array.isArray(m.content) && m.content.some((b) => b.type === 'tool_use' && b.name === 'Bash'))).toBe(true);
  }, 60_000);

  it('a session last run with a newer CLI than the image\'s is refused: Unavailable(cli_version), and nothing runs', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-ver-'));
    const api = new (await import('harness-world-sandbox/testing/fake-api')).FakeSandboxApi({ cliVersions: { claude: CLAUDE_CODE_VERSION } });
    apis.push(api);
    script = respond([['Say hi', { text: 'hi' }]]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Say hi.');
    const one = await waitTurns(a.sessions, sessionId, 1);
    expect(readDoc(one, SANDBOX_DOC).value.versions).toMatchObject({ claude: CLAUDE_CODE_VERSION });
    await a.drained();
    await a.world.close?.();
    await deleteVm(api, sessionId);
    // A deployment rolled back to an image with the previous CLI.
    (api.options as { cliVersions?: Record<string, string> }).cliVersions = { claude: '2.1.289' };
    const from = model.requests.length;
    const b = await invocation(dataDir, api, model);
    await b.sessions.send(sessionId, 'Say hi.');
    const refusal = await until(async () => {
      const s = await b.sessions.inspect(sessionId);
      return [...s.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { code: string; message: string } | undefined;
    });
    expect(refusal).toMatchObject({ code: 'cli_version' });
    expect(refusal.message).toContain(`${CLAUDE_CODE_VERSION}, newer than this image's 2.1.289`);
    expect(model.messages().slice(from)).toEqual([]);
  }, 120_000);

  it('a mirror frame that does not map ends the turn with Unavailable (mirror_unmapped); the session is kept and the next turn works', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-unmap-'));
    const api = platform();
    // The pinned CLI behind a wrapper that, while `.unmap-once` is on the Drive, points its first mirror frame outside the config dir (a faulty or hostile VM).
    // The wrapper execs the CLI, so the CLI is still the process the relay spawned (its model port answers that process alone,
    // where /proc can tell), and only its stdout goes through the filter.
    const dir = await mkdtemp(join(tmpdir(), 'hwcc-wrap-'));
    const wrapper = join(dir, 'claude');
    const filter = join(dir, 'unmap-once.cjs');
    await writeFile(
      filter,
      `const fs = require('node:fs');
const marker = require('node:path').join(process.cwd(), '.unmap-once');
let unmap = fs.existsSync(marker);
require('node:readline').createInterface({ input: process.stdin }).on('line', (line) => {
  if (unmap && line.includes('"transcript_mirror"')) {
    const frame = JSON.parse(line);
    frame.filePath = '/etc/elsewhere/' + require('node:path').basename(frame.filePath);
    line = JSON.stringify(frame);
    unmap = false;
    fs.rmSync(marker, { force: true });
  }
  process.stdout.write(line + '\\n');
});
`
    );
    await writeFile(wrapper, `#!/bin/bash\nexec ${JSON.stringify(executable)} "$@" > >(${JSON.stringify(process.execPath)} ${JSON.stringify(filter)})\n`);
    await (await import('node:fs/promises')).chmod(wrapper, 0o755);
    script = respond([['Say hi', { text: 'hi' }], ['Say bye', { text: 'bye' }]]);
    const a = await invocation(dataDir, api, model, { executable: wrapper });
    const { sessionId } = await a.sessions.start();
    // The marker goes on the Drive once it exists (before the CLI's first frame: the turn waits for the model).
    let release!: () => void;
    const held = new Promise<void>((r) => (release = r));
    script = respond([['Say hi', { hold: held, then: { text: 'hi' } }], ['Say bye', { text: 'bye' }]]);
    await a.sessions.send(sessionId, 'Say hi.');
    const drive = await until(async () => (api.drives.has(nameOf(sessionId)) ? api.driveDir(nameOf(sessionId)) : undefined), 60_000, 'the Drive');
    await writeFile(join(drive, '.unmap-once'), '');
    release();
    const refusal = await until(async () => {
      const st = await a.sessions.inspect(sessionId);
      return [...st.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { code: string; message: string } | undefined;
    });
    expect(refusal.message).toContain('mirror_unmapped');
    expect(refusal.message).not.toContain('/etc/elsewhere');
    await a.sessions.send(sessionId, 'Say bye.');
    const state = await until(async () => {
      const st = await a.sessions.inspect(sessionId);
      return entries(st).some((e) => e.type === 'message' && e.role === 'assistant' && e.text === 'bye') ? st : undefined;
    }, 60_000, 'the next turn');
    expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
  }, 180_000);

  it('a Bash timeout past what the session allows is clamped, and the call it ends has a definite outcome (not an interrupted one)', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-clamp-'));
    const api = platform();
    script = respond([['Wait a while', { tool: { name: 'Bash', input: { command: 'echo begun; sleep 8; echo finished', description: 'wait', timeout: 60_000 } } }]]);
    const a = await invocation(dataDir, api, model, { commandTimeoutMs: 2000 });
    const { sessionId } = await a.sessions.start();
    const at = Date.now();
    await a.sessions.send(sessionId, 'Wait a while.');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(Date.now() - at).toBeLessThan(8000);
    const [bash] = bashCalls(state);
    // Committed as what ran (the clamped timeout), with an outcome: never an open intent or an interrupted call.
    expect(bash).toMatchObject({ status: expect.stringMatching(/^(completed|failed)$/) });
    expect((bash as { error?: { interrupted?: boolean } }).error?.interrupted).not.toBe(true);
    expect(bash!.input).toMatchObject({ timeout: 2000 });
    const said = JSON.stringify(state.effects.get(bash!.key)?.output ?? (bash as { error?: unknown }).error);
    expect(said).toMatch(/Ran out of time|timed out|Command timed out/i);
    expect(turns(state).at(-1)).toEqual({ type: 'turn', status: 'completed' });
  }, 120_000);

  it("a spend cap reached mid-turn ends the turn with an Unavailable naming the cap; the next model call is never forwarded", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-cap-'));
    const api = platform();
    // Each response costs the fake's $0.0001: a cap is checked before each request, so after the first the turn is past a $0.00005 cap.
    script = respond([['Count the files', { tool: { name: 'Bash', input: { command: 'ls | wc -l', description: 'count' } } }]]);
    const a = await invocation(dataDir, api, model, { budget: { turnUsd: 0.00005, sessionUsd: Infinity } });
    const { sessionId } = await a.sessions.start();
    const from = model.requests.length;
    await a.sessions.send(sessionId, 'Count the files.');
    const refusal = await until(async () => {
      const s = await a.sessions.inspect(sessionId);
      return [...s.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { code: string; message: string } | undefined;
    });
    expect(refusal.message).toMatch(/turn's model spend reached its cap \(sandbox\(\{ budget: \{ turnUsd \} \}\)\)/);
    // The first call (the Bash call) went out; the one after its result was refused in the Function.
    expect(model.messages().slice(from).filter(main)).toHaveLength(1);
  }, 120_000);

  it("the session's spend carries across invocations: a later invocation's first request is refused once the log holds the session cap's worth", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-capx-'));
    const api = platform();
    script = respond([['Say hi', { text: 'hi' }], ['Say bye', { text: 'bye' }]]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Say hi.');
    const one = await waitTurns(a.sessions, sessionId, 1);
    await a.drained();
    await a.world.close?.();
    const spent = journalRecords<{ usd: number }>(await a.sessions.inspect(sessionId), SPEND_STREAM).reduce((n, r) => n + r.data.usd, 0);
    expect(spent).toBeGreaterThan(0);
    expect(turns(one)).toEqual([{ type: 'turn', status: 'completed' }]);
    // A new invocation (no Function-side state) whose session cap is what the log already holds.
    const b = await invocation(dataDir, api, model, { budget: { turnUsd: Infinity, sessionUsd: spent } });
    const from = model.requests.length;
    await b.sessions.send(sessionId, 'Say bye.');
    const refusal = await until(async () => {
      const st = await b.sessions.inspect(sessionId);
      return [...st.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { message: string } | undefined;
    });
    expect(refusal.message).toMatch(/session's model spend reached its cap \(sandbox\(\{ budget: \{ sessionUsd \} \}\)\)/);
    expect(model.messages().slice(from).filter(main)).toHaveLength(0);
  }, 120_000);

  it("a turn's spend keeps its key across a redelivery, even when the recovery carries a new submission: the turn cap holds", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-capr-'));
    const api = platform();
    script = respond([['Run the slow job', { tool: { name: 'Bash', input: { command: 'echo started >> runs.txt; sleep 5; echo done >> runs.txt', description: 'slow job' } } }]]);
    // One response ($0.0001) puts the turn past its cap: the first request goes, the next is refused.
    const budget = { turnUsd: 0.00005, sessionUsd: Infinity };
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const a = await invocation(dataDir, api, model, { crash: () => crashed(), budget });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 2000 });
    await a.sessions.send(sessionId, 'Run the slow job.');
    const drive = await until(async () => (api.drives.has(nameOf(sessionId)) ? api.driveDir(nameOf(sessionId)) : undefined), 60_000, 'the Drive');
    await until(async () => (existsSync(join(drive, 'runs.txt')) ? true : undefined), 60_000, 'the command to start');
    // A new prompt waits behind the turn; the recovery note will carry it.
    await a.sessions.send(sessionId, 'And then say hello.');
    await died;
    await a.world.close?.();
    const from = model.requests.length;
    const b = await invocation(dataDir, api, model, { recover: true, crash: () => {}, budget });
    const refusal = await until(async () => {
      const st = await b.sessions.inspect(sessionId);
      return [...st.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { message: string } | undefined;
    });
    // The recovered turn is the turn that spent: its first request is refused, not counted from zero under the new submission.
    expect(refusal.message).toMatch(/turn's model spend reached its cap/);
    expect(model.messages().slice(from).filter(main)).toHaveLength(0);
  }, 180_000);

  it('a FSYNC that fails: the outcome is committed only after it, as a failed call that says its writes may not be on the Drive, never as a success', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-fsync-'));
    const api = platform();
    const proxy = await relayProxy();
    api.route = (_vm, url) => {
      proxy.target = Number(new URL(url).port);
      return proxy.url;
    };
    script = respond([['Write the word', { tool: { name: 'Write', input: { file_path: 'word.txt', content: 'avocet\n' } } }]]);
    const a = await invocation(dataDir, api, model);
    try {
      const { sessionId } = await a.sessions.start();
      const flushing = proxy.holdCtl(CTL_FSYNC);
      await a.sessions.send(sessionId, 'Write the word avocet to word.txt.');
      await flushing;
      // The outcome waits for the flush: still open while the FSYNC is out.
      await new Promise((r) => setTimeout(r, 500));
      expect((await a.sessions.inspect(sessionId)).effectOrder.find((e) => e.name === 'tool.Write')).toMatchObject({ status: 'running' });
      // The link drops under the FSYNC: it fails.
      expect(proxy.drop('/ctl')).toBeGreaterThan(0);
      const state = await waitTurns(a.sessions, sessionId, 1);
      const writes = state.effectOrder.filter((e) => e.name === 'tool.Write');
      expect(writes).toHaveLength(1);
      expect(writes[0]).toMatchObject({ status: 'failed' });
      expect((writes[0] as { error?: { interrupted?: boolean } }).error?.interrupted).not.toBe(true);
      expect(JSON.stringify((writes[0] as { error?: unknown }).error)).toContain("didn't confirm this call's writes");
      expect(a.logs.some((l) => l.message === 'claude-code: the Drive flush after a built-in failed')).toBe(true);
    } finally {
      await proxy.close();
    }
  }, 180_000);

  it('R8: the Drive deleted: the model hears its files were lost, exactly once', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-r8-'));
    const api = platform();
    script = respond([
      ['Write a note', { tool: { name: 'Write', input: { file_path: 'note.txt', content: 'one\n' } } }],
      ['Is the note there', { tool: { name: 'Bash', input: { command: 'ls', description: 'list' } } }],
    ]);
    const a = await invocation(dataDir, api, model);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'Write a note to note.txt.');
    await waitTurns(a.sessions, sessionId, 1);
    await a.drained();
    await a.world.close?.();
    await deleteVm(api, sessionId);
    api.dropDrive(nameOf(sessionId));
    const b = await invocation(dataDir, api, model);
    const from = model.requests.length;
    await b.sessions.send(sessionId, 'Is the note there?');
    await waitTurns(b.sessions, sessionId, 2);
    await until(async () => (readDoc(await b.sessions.inspect(sessionId), SANDBOX_DOC).value.filesLost === undefined ? true : undefined), 30_000, 'the note acknowledged');
    await b.sessions.send(sessionId, 'Thanks.');
    const state = await waitTurns(b.sessions, sessionId, 3);
    const sent = model.messages().slice(from).filter(main);
    const last = sent.at(-1)!;
    // Said once: in turn 2's prompt, and the conversation of turn 3 holds that one copy only.
    expect(last.text.split(FILES_LOST_NOTE).length - 1).toBe(1);
    expect(existsSync(join(api.driveDir(nameOf(sessionId)), 'note.txt'))).toBe(false);
    expect(turns(state).every((t) => t.status === 'completed')).toBe(true);
  }, 180_000);
});

const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

import { existsSync, mkdirSync } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { type DriveContext, durableSessions, QUEUE_PREFIX, type SessionState, Yield } from 'harness-world';
import { filesFromState, readLedger, unsafeTools, workspaceTools } from 'harness-world-hands';
import { journalRecords, transcriptOf, WORKSPACE_STREAM } from 'harness-world-process';
import { RESET_NOTE, workspaceRuntime } from 'harness-world-hands/runtime';
import { type FakeModel, fakeModel, streamFromModel } from 'harness-world-process/reference/fake-model';
import { afterEach, describe, expect, it } from 'vitest';
import { OPENCODE_TOOLS } from './environment.ts';
import { type HostConnection, opencodeHarness } from './harness.ts';
import { type FromHost, type HostInit, pipePair, type ToHost } from './protocol.ts';
import { EVENTS, type OpenCodeEvent } from './records.ts';

/**
 * The drive's side, against a scripted host (no OpenCode, no network): what
 * it mirrors into the log, how it maps OpenCode's tool calls onto the tool
 * bridge, the model barrier, recovery, and stopping the host when a write
 * is refused. `host.test.ts` covers OpenCode's side; `test/e2e.test.ts` runs
 * both for real.
 */

const worlds: LocalWorld[] = [];
const models: FakeModel[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
  for (const m of models.splice(0)) await m.close();
});

async function until<T>(fn: () => Promise<T | undefined> | T | undefined, timeoutMs = 20_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v !== undefined) return v;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 25));
  }
}

/** A scripted OpenCode host: the test writes the events it commits and the requests it makes. */
class FakeHost {
  seq: number;
  readonly sent: OpenCodeEvent[] = [];
  readonly inbound: ToHost[] = [];
  ended = false;
  private next = 0;
  private wakers: (() => void)[] = [];
  readonly conn: HostConnection;
  /** Set by a script: what it still does after the drive closes it, before its channel ends (a real host's shutdown). */
  linger: Promise<void> | undefined;
  private readonly out: (m: FromHost) => void;
  constructor(
    readonly ctx: DriveContext,
    readonly init: HostInit
  ) {
    this.seq = init.events.at(-1)?.seq ?? -1;
    const { left, right } = pipePair<FromHost, ToHost>();
    this.out = (m) => right.send(m);
    void (async () => {
      for await (const m of right.messages) {
        this.inbound.push(m);
        this.wake();
      }
      this.ended = true;
      this.wake();
    })();
    this.conn = {
      send: (m) => left.send(m),
      messages: left.messages,
      close: async () => {
        left.send({ type: 'close' });
        await this.linger;
        right.close();
      },
      kill: () => {
        left.close();
        right.close();
      },
      tail: () => '',
      exited: Promise.resolve(),
    };
  }
  private wake() {
    for (const w of this.wakers.splice(0)) w();
  }
  /** The next message from the drive of this type. */
  async take<T extends ToHost['type']>(type: T): Promise<Extract<ToHost, { type: T }>> {
    for (;;) {
      const at = this.inbound.findIndex((m) => m.type === type);
      if (at >= 0) return this.inbound.splice(at, 1)[0] as Extract<ToHost, { type: T }>;
      if (this.ended) throw new Error(`the drive closed before sending ${type}`);
      await new Promise<void>((r) => this.wakers.push(r));
    }
  }
  /** Commit (and ship) events. */
  commit(...events: [string, Record<string, unknown>?][]) {
    const out = events.map(([type, data]) => ({
      id: `evt_${this.seq + 1}`,
      seq: ++this.seq,
      created: Date.now(),
      type: `${type}.1`,
      data: { sessionID: this.init.sessionId, ...data },
    }));
    this.sent.push(...out);
    this.out({ type: 'events', events: out, blobs: [] });
  }
  send(m: FromHost) {
    this.out(m);
  }
  /** A request (barrier or tool) and the drive's reply. */
  async request(
    m:
      | { type: 'barrier'; kind: string }
      | { type: 'tool'; callId: string; name: string; input: unknown }
      | { type: 'builtin'; phase: 'begin'; callId: string; name: string; input: unknown }
      | { type: 'builtin'; phase: 'end'; callId: string; name: string; text: string; isError: boolean }
  ) {
    const id = ++this.next + 1000;
    this.out({ ...m, id } as FromHost);
    for (;;) {
      const at = this.inbound.findIndex((x) => x.type === 'reply' && x.id === id);
      if (at >= 0) return this.inbound.splice(at, 1)[0] as Extract<ToHost, { type: 'reply' }>;
      if (this.ended) throw new Error('the drive closed before replying');
      await new Promise<void>((r) => this.wakers.push(r));
    }
  }
}

type Script = (h: FakeHost) => Promise<void>;

async function session(
  dataDir: string,
  scripts: Script[],
  opts: { chaos?: boolean; crash?: () => void; recover?: boolean; contended?: () => boolean; gatewayUrl?: string; confirmMs?: number; builtins?: boolean; durable?: boolean } = {}
) {
  const world = createWorld({ dataDir, recoverActiveRuns: opts.recover ?? false });
  worlds.push(world);
  const hosts: FakeHost[] = [];
  /** What the scripts threw (their expectations included). */
  const errors: unknown[] = [];
  const harness = opencodeHarness({
    // Built-ins: OpenCode's own tools, on a virtual workspace.
    ...(opts.builtins ? { workspace: { kind: 'virtual' as const, runtime: workspaceRuntime({ kind: 'virtual', durable: opts.durable ?? true }) } } : {}),
    tools: opts.builtins ? unsafeTools({ confirmMs: opts.confirmMs ?? 3000 }) : workspaceTools({ confirmMs: opts.confirmMs ?? 3000 }),
    root: await mkdtemp(join(tmpdir(), 'oc-fake-root-')),
    // The scripted host's model requests (if it makes any) go to a fake model, with no real credential.
    ...(opts.gatewayUrl ? { gatewayUrl: opts.gatewayUrl, credential: async () => 'test-key' } : {}),
    connect: async (ctx, init) => {
      if (opts.contended?.()) throw new Yield('contended', 1);
      const h = new FakeHost(ctx, init);
      hosts.push(h);
      const script = scripts[hosts.length - 1];
      if (!script) throw new Error(`no script for drive ${hosts.length}`);
      void script(h).catch((err) => errors.push(err));
      return h.conn;
    },
  });
  const sessions = durableSessions({
    world,
    harness,
    ...(opts.chaos ? { chaos: true, crash: opts.crash ?? (() => {}) } : {}),
  });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  await world.start?.();
  return { world, sessions, harness, hosts, errors };
}

/** A new session: `session.created`, then ready. */
function open(h: FakeHost) {
  if (h.init.events.length === 0) h.commit(['session.created', { location: { directory: h.init.directory }, projectID: 'p' }]);
  h.send({ type: 'ready', seq: h.seq });
}

/** Admit the next prompt and start its execution (OpenCode's order). */
async function admit(h: FakeHost) {
  const p = await h.take('prompt');
  h.send({ type: 'reply', id: p.id, ok: true });
  h.commit(['session.inbox.enqueued', { inboxID: p.messageId, item: { type: 'user', payload: { text: p.text } } }], ['session.execution.started'], [
    'session.inbox.delivered',
    { inboxID: p.messageId },
  ]);
  return p;
}

const turnsEnded = (s: SessionState) => transcriptOf(s).filter((r) => r.data.type === 'turn').length;
const waitTurns = (sessions: Awaited<ReturnType<typeof session>>['sessions'], id: string, n: number) =>
  until(async () => {
    const s = await sessions.inspect(id);
    return turnsEnded(s) >= n ? s : undefined;
  });

describe('the OpenCode drive', () => {
  it('mirrors what OpenCode commits, maps its tool calls onto the bridge, and holds model requests for the log', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    const seen: { placedBeforeSend?: boolean; journaledAtBarrier?: number[]; sentAtBarrier?: number[]; init?: HostInit[] } = { init: [] };
    const turn: Script = async (h) => {
      seen.init!.push(h.init);
      open(h);
      const p = await admit(h);
      const sub = h.ctx.state().submissions.find((s) => `msg_${s.id}` === p.messageId)!;
      seen.placedBeforeSend ??= h.ctx.state().placed.has(sub.id);
      const b1 = await h.request({ type: 'barrier', kind: 'primary' });
      expect(b1.ok).toBe(true);
      // Everything shipped before the barrier is in the drive's log once it is answered.
      seen.sentAtBarrier ??= h.sent.map((e) => e.seq);
      seen.journaledAtBarrier ??= journalRecords<OpenCodeEvent>(h.ctx.state(), EVENTS).map((r) => r.data.seq);
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Writing it.' }], ['session.tool.input.started', { id: 'toolu_1', name: 'write_file' }]);
      h.commit(['session.tool.called', { id: 'toolu_1', input: { path: 'a.txt', content: 'hi' } }]);
      h.send({ type: 'text', delta: 'Writing it.' });
      const r = await h.request({ type: 'tool', callId: 'toolu_1', name: 'write_file', input: { path: 'a.txt', content: 'hi' } });
      expect(r).toMatchObject({ ok: true, isError: false, text: expect.stringContaining('wrote 2 bytes') });
      h.commit(['session.tool.success', { id: 'toolu_1', content: [{ type: 'text', text: r.ok ? r.text : '' }] }], ['session.step.ended']);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Done.' }], ['session.step.ended'], ['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [turn, turn]);
    const { sessionId } = await a.sessions.start({ prompt: 'write a.txt' });
    let state = await waitTurns(a.sessions, sessionId, 1);

    // The call ran once, as a kernel effect keyed by OpenCode's call id.
    expect(state.effects.get('tool:toolu_1')).toMatchObject({ name: 'tool.write_file', status: 'completed' });
    expect(filesFromState(state)).toEqual({ '/workspace/a.txt': 'hi' });
    // The journal is OpenCode's events, in order, one record each.
    const host1 = a.hosts[0]!;
    expect(journalRecords<OpenCodeEvent>(state, EVENTS).map((r) => [r.id, r.data])).toEqual(host1.sent.map((e) => [`seq:${e.seq}`, e]));
    // The normalized transcript.
    expect(transcriptOf(state).map((r) => r.data)).toEqual([
      { type: 'message', role: 'assistant', text: 'Writing it.', toolCalls: [{ id: 'toolu_1', name: 'write_file' }] },
      { type: 'message', role: 'assistant', text: 'Done.' },
      { type: 'turn', status: 'completed' },
    ]);
    // Placed before OpenCode saw it; the barrier answered only once the log had it all.
    expect(seen.placedBeforeSend).toBe(true);
    expect(seen.journaledAtBarrier).toEqual(seen.sentAtBarrier);
    expect(seen.init![0]).toMatchObject({ events: [], claim: false, restarts: 0, sessionId: `ses_${sessionId}`, model: 'vercel/anthropic/claude-haiku-4.5' });
    expect(seen.init![0]!.tools.map((t) => t.name)).toEqual(workspaceTools().map((t) => t.name));

    // The next drive rebuilds OpenCode from exactly what was journaled.
    await a.sessions.send(sessionId, 'again');
    state = await waitTurns(a.sessions, sessionId, 2);
    expect(seen.init![1]!.events).toEqual(host1.sent);
    expect(seen.init![1]).toMatchObject({ claim: false, restarts: 0 });
    // A call id OpenCode reuses (it never does) would answer from the log, not run again.
    expect(state.effectOrder.filter((e) => e.name === 'tool.write_file')).toHaveLength(1);
    expect(a.errors).toEqual([]);
  });

  it('an execution OpenCode fails before it delivers the prompt ends the turn, failed, with its error', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    const failing: Script = async (h) => {
      open(h);
      const p = await h.take('prompt');
      h.send({ type: 'reply', id: p.id, ok: true });
      // OpenCode's order when its drain fails at once: admitted, started, failed, the prompt still in its inbox.
      h.commit(['session.inbox.enqueued', { inboxID: p.messageId, item: { type: 'user', payload: { text: p.text } } }], ['session.execution.started'], [
        'session.execution.failed',
        { error: { type: 'unknown', message: 'the provider failed' } },
      ]);
      await h.take('close');
      closed = Date.now();
    };
    let closed: number | undefined;
    const a = await session(dataDir, [failing]);
    const started = Date.now();
    const { sessionId } = await a.sessions.start({ prompt: 'hi' });
    const state = await waitTurns(a.sessions, sessionId, 1);
    // The drive is over at once: it does not wait for the prompt OpenCode never delivered.
    expect((await until(() => closed, 10_000)) - started).toBeLessThan(10_000);
    expect(transcriptOf(state).at(-1)?.data).toMatchObject({ type: 'turn', status: 'failed', error: expect.stringContaining('the provider failed') });
    expect(a.errors).toEqual([]);
  });

  it('a crash mid never-tool: the next drive restores the claim and resumes with a recovery note; the side effect happened once', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const pay = { to: 'acme@example.com', amount_usd: 5, memo: 'order 7' };
    const first: Script = async (h) => {
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.send({ type: 'text', delta: 'Paying acme now.' });
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Paying acme now.' }], ['session.tool.input.started', { id: 'toolu_pay', name: 'unsafe_make_payment' }], ['session.tool.called', { id: 'toolu_pay', input: pay }]);
      await h.request({ type: 'tool', callId: 'toolu_pay', name: 'unsafe_make_payment', input: pay });
    };
    let note = '';
    let resumedInit: HostInit | undefined;
    const second: Script = async (h) => {
      resumedInit = h.init;
      open(h);
      const r = await h.take('recover');
      note = r.note;
      h.send({ type: 'reply', id: r.id, ok: true });
      h.commit(
        ['session.synthetic', { text: 'The server restarted while you were working.', metadata: { notice: 'restart' } }],
        ['session.tool.failed', { id: 'toolu_pay', error: { type: 'aborted', message: 'Tool execution interrupted: unsafe_make_payment' } }]
      );
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.text.ended', { text: 'The payment may have gone through; not repeating it.' }], ['session.step.ended'], [
        'session.execution.succeeded',
      ]);
      await h.take('close');
    };
    const a = await session(dataDir, [first], { chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 300 });
    await a.sessions.send(sessionId, 'pay acme $5 once');
    await died;
    await a.world.close?.();

    const b = await session(dataDir, [second], { chaos: true, recover: true });
    const state = await waitTurns(b.sessions, sessionId, 1);
    expect(state.effects.get('tool:toolu_pay')).toMatchObject({ name: 'tool.unsafe_make_payment', status: 'failed', error: { interrupted: true } });
    expect((await readLedger(b.world, sessionId)).map((l) => l.callKey)).toEqual(['tool:toolu_pay']);
    // The host was rebuilt with OpenCode's claim held, then told to resume with the note.
    expect(resumedInit).toMatchObject({ claim: true, restarts: 0 });
    expect(resumedInit!.events.map((e) => e.type)).toContain('session.tool.called.1');
    expect(note).toContain('[Recovery]');
    expect(note).toContain('unsafe_make_payment');
    expect(note).toContain('may have taken effect');
    const entries = transcriptOf(state).map((r) => r.data);
    expect(entries).toContainEqual({ type: 'message', role: 'user', text: note, synthetic: 'recovery' });
    expect(entries.at(-1)).toEqual({ type: 'turn', status: 'completed' });
    // The live stream: the first drive's text, then `restart` with what the log kept of the turn, before the resumed reply.
    const abort = new AbortController();
    const live: { type?: string; text?: string; delta?: string }[] = [];
    for await (const ev of b.sessions.live(sessionId, { signal: abort.signal }) as AsyncIterable<{ type?: string }>) {
      live.push(ev);
      if (ev.type === 'idle' && live.some((e) => e.type === 'restart')) abort.abort();
    }
    expect(live.filter((e) => e.type === 'text' || e.type === 'restart')).toEqual([
      { type: 'text', delta: 'Paying acme now.' },
      { type: 'restart', text: 'Paying acme now.', parts: ['Paying acme now.'] },
    ]);
    expect(b.errors).toEqual([]);
  });

  it('a crash while the model streams, after a never tool: the chaos step kills the model step, the resumed turn makes the call again, the email went out once', async () => {
    // The model behind the drive's proxy types the reply to the tool's result slowly: the stream the crash cuts.
    const model = await fakeModel({ pieceMs: (text) => (text.startsWith('unsafe_send_email returned') ? 150 : 5) });
    models.push(model);
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const mail = { to: 'a@b.c', subject: 'hi', body: 'x' };
    /** OpenCode's model request: through the drive's proxy (`gatewayBaseURL`), after the barrier. */
    const ask = async (h: FakeHost, text: string) => {
      await h.request({ type: 'barrier', kind: 'primary' });
      await streamFromModel(`${h.init.gatewayBaseURL}/language-model`, text, (delta) => h.send({ type: 'text', delta }), { authorization: `Bearer ${h.init.gatewayKey}` });
    };
    const first: Script = async (h) => {
      open(h);
      await admit(h);
      await ask(h, 'Sending it.');
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Sending it.' }], ['session.tool.input.started', { id: 'toolu_mail', name: 'unsafe_send_email' }], [
        'session.tool.called',
        { id: 'toolu_mail', input: mail },
      ]);
      const r = await h.request({ type: 'tool', callId: 'toolu_mail', name: 'unsafe_send_email', input: mail });
      h.commit(['session.tool.success', { id: 'toolu_mail', content: [{ type: 'text', text: r.ok ? r.text : '' }] }], ['session.step.ended']);
      await ask(h, `unsafe_send_email returned: ${r.ok ? r.text : ''}`);
    };
    let note = '';
    const second: Script = async (h) => {
      open(h);
      const r = await h.take('recover');
      note = r.note;
      h.send({ type: 'reply', id: r.id, ok: true });
      h.commit(['session.synthetic', { text: 'The server restarted while you were working.', metadata: { notice: 'restart' } }]);
      await ask(h, 'Recovered from the log: the email went out.');
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Recovered from the log: the email went out.' }], ['session.step.ended'], [
        'session.execution.succeeded',
      ]);
      await h.take('close');
    };
    const a = await session(dataDir, [first], { chaos: true, crash: () => crashed(), gatewayUrl: model.url, confirmMs: 500 });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'model', afterMs: 400 });
    await a.sessions.send(sessionId, 'email a@b.c once');
    await died;
    let state = await a.sessions.inspect(sessionId);
    // "Sending it." streamed in under 400 ms; the crash fired in the reply's model step.
    const steps = state.effectOrder.filter((e) => e.name === 'model');
    expect(steps.map((e) => e.status)).toEqual(['completed', 'running']);
    expect(steps[0]!.input).toEqual({ method: 'POST', path: '/v3/ai/language-model' });
    expect(state.effectOrder.find((e) => e.name === 'chaos')).toMatchObject({ output: { effect: 'model', crashedDuring: steps[1]!.key } });
    expect(state.effects.get('tool:toolu_mail')).toMatchObject({ status: 'completed' });
    await a.world.close?.();

    const b = await session(dataDir, [second], { chaos: true, recover: true, gatewayUrl: model.url });
    state = await waitTurns(b.sessions, sessionId, 1);
    expect(state.effects.get(steps[1]!.key)).toMatchObject({ status: 'completed', attempt: 2 });
    expect(state.effectOrder.filter((e) => e.status === 'running' || e.status === 'pending')).toEqual([]);
    expect(state.effectOrder.filter((e) => e.name === 'tool.unsafe_send_email')).toHaveLength(1);
    expect((await readLedger(b.world, sessionId)).map((l) => l.callKey)).toEqual(['tool:toolu_mail']);
    expect(note).toContain('call toolu_mail: completed');
    const entries = transcriptOf(state).map((r) => r.data);
    expect(entries).toContainEqual({ type: 'message', role: 'user', text: note, synthetic: 'recovery' });
    expect(entries.at(-1)).toEqual({ type: 'turn', status: 'completed' });
    // The cut-off reply streamed live and was never in the log: `restart` keeps only "Sending it.", then the resumed reply streams.
    const abort = new AbortController();
    const live: { type?: string; text?: string; delta?: string }[] = [];
    for await (const ev of b.sessions.live(sessionId, { signal: abort.signal }) as AsyncIterable<{ type?: string }>) {
      live.push(ev);
      if (ev.type === 'idle' && live.some((e) => e.type === 'restart')) abort.abort();
    }
    const restart = live.findIndex((e) => e.type === 'restart');
    expect(live[restart]).toEqual({ type: 'restart', text: 'Sending it.', parts: ['Sending it.'] });
    const text = (from: number, to: number) => live.slice(from, to).flatMap((e) => (e.type === 'text' ? [e.delta] : [])).join('');
    expect(text(0, restart)).toMatch(/^Sending it\..+/);
    // The stand-in's crashed host can still stream a few deltas as the next drive starts (a real crash writes nothing more).
    expect(text(restart + 1, live.length)).toMatch(/Recovered from the log: the email went out\.$/);
    expect(b.errors).toEqual([]);
  });

  it('admits a steer into the running turn', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let steered: Extract<ToHost, { type: 'prompt' }> | undefined;
    let first!: () => void;
    const admitted = new Promise<void>((r) => (first = r));
    const script: Script = async (h) => {
      open(h);
      await admit(h);
      first();
      // The turn runs until the steer arrives: OpenCode delivers it at its next step.
      steered = await h.take('prompt');
      h.send({ type: 'reply', id: steered.id, ok: true });
      h.commit(['session.inbox.enqueued', { inboxID: steered.messageId, item: { type: 'user', payload: { text: steered.text } } }], [
        'session.inbox.delivered',
        { inboxID: steered.messageId },
      ]);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Both done.' }], ['session.step.ended'], ['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [script]);
    const { sessionId } = await a.sessions.start({ prompt: 'do one thing' });
    await admitted;
    const steerId = await a.sessions.send(sessionId, 'and another', 'steer');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(steered).toMatchObject({ delivery: 'steer', text: 'and another', messageId: `msg_${steerId}` });
    expect(state.placed.has(steerId)).toBe(true);
    expect(transcriptOf(state).map((r) => r.data)).toEqual([
      { type: 'message', role: 'assistant', text: 'Both done.' },
      { type: 'turn', status: 'completed' },
    ]);
    expect(a.errors).toEqual([]);
  });

  it('a refused journal write halts the drive and stops the host', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let host!: FakeHost;
    const script: Script = async (h) => {
      host = h;
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' }).catch(() => {});
    };
    const a = await session(dataDir, [script]);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello', 'prompt', { wake: 'backstop' });
    const create = a.world.events.create.bind(a.world.events);
    const refused = new Error('the World refused the write');
    a.world.events.create = (async (runId: string, request: any, params?: any) => {
      if (request?.eventType === 'step_started' && String(request.eventData?.stepName).endsWith('//journal')) throw refused;
      return create(runId, request, params);
    }) as typeof a.world.events.create;
    await expect(a.sessions.drive(sessionId)).rejects.toBe(refused);
    // OpenCode's input ended: the host stops, and nothing it committed is in the log.
    await until(() => (host.ended ? true : undefined), 5000);
    const state = await a.sessions.inspect(sessionId);
    expect(state.effectOrder.some((e) => e.name === 'journal')).toBe(false);
  });
  it('a call id OpenCode reuses in a later step is a new call: it runs, keyed by its message; re-issued in its own message, it answers from the log', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    const replies: string[] = [];
    // Kimi-style ids: `functions.write_file:0` in every step.
    const id = 'functions.write_file:0';
    const call = (message: string, input: { path: string; content: string }): Script => async (h) => {
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started', { assistantMessageID: message }], ['session.tool.input.started', { assistantMessageID: message, id, name: 'write_file' }], [
        'session.tool.called',
        { assistantMessageID: message, id, input },
      ]);
      const r = await h.request({ type: 'tool', callId: id, name: 'write_file', input });
      replies.push(r.ok ? (r.text ?? '') : `error: ${r.error}`);
      // OpenCode re-issuing the same call (same message): the log's answer, no second run.
      const again = await h.request({ type: 'tool', callId: id, name: 'write_file', input });
      replies.push(again.ok ? (again.text ?? '') : `error: ${again.error}`);
      h.commit(['session.tool.success', { assistantMessageID: message, id }], ['session.step.ended', { assistantMessageID: message }], ['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [call('msg_a1', { path: 'a.txt', content: 'one' }), call('msg_a2', { path: 'b.txt', content: 'two' })]);
    const { sessionId } = await a.sessions.start({ prompt: 'write a.txt' });
    await waitTurns(a.sessions, sessionId, 1);
    await a.sessions.send(sessionId, 'now b.txt');
    const state = await waitTurns(a.sessions, sessionId, 2);
    expect(filesFromState(state)).toEqual({ '/workspace/a.txt': 'one', '/workspace/b.txt': 'two' });
    expect(state.effectOrder.filter((e) => e.name === 'tool.write_file').map((e) => [e.key, e.status])).toEqual([
      [`tool:${id}`, 'completed'],
      [`tool:${id}@msg_a2`, 'completed'],
    ]);
    expect(replies[0]).toBe(replies[1]);
    expect(replies[2]).toBe(replies[3]);
    // The transcript names the calls as the log keys them.
    const calls = transcriptOf(state).flatMap((r) => (r.data.type === 'message' && 'toolCalls' in r.data ? (r.data.toolCalls ?? []) : []));
    expect(calls.map((c) => c.id)).toEqual([id, `${id}@msg_a2`]);
    expect(a.errors).toEqual([]);
  });

  it('a drive that lost the race to start a host leaves the drive that runs it its files', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let second: unknown;
    let presentAfter: boolean | undefined;
    const harness = () => a.harness;
    const script: Script = async (h) => {
      mkdirSync(dirname(h.init.dbPath), { recursive: true });
      open(h);
      await admit(h);
      // Another drive of the session on this machine, while this one runs: it cannot start a host.
      second = await harness()
        .drive(h.ctx)
        .then(() => 'returned', (err) => err);
      presentAfter = existsSync(dirname(h.init.dbPath));
      h.commit(['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [script]);
    const { sessionId } = await a.sessions.start({ prompt: 'one thing' });
    await waitTurns(a.sessions, sessionId, 1);
    expect(second).toBeInstanceOf(Yield);
    expect(presentAfter).toBe(true);
    expect(a.errors).toEqual([]);
  });

  it('once the drive is over, nothing the host still sends reaches the log, and its tool calls are refused', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let late: Extract<ToHost, { type: 'reply' }> | undefined;
    let journaledAtEnd = 0;
    const script: Script = async (h) => {
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Done.' }], ['session.step.ended'], ['session.execution.succeeded']);
      // Its shutdown: an event committed and a tool called after the drive closed it.
      h.linger = (async () => {
        await h.take('close');
        journaledAtEnd = journalRecords(h.ctx.state(), EVENTS).length;
        h.commit(['session.step.started']);
        late = await h.request({ type: 'tool', callId: 'toolu_late', name: 'write_file', input: { path: 'late.txt', content: 'x' } });
      })();
      await h.linger;
    };
    const a = await session(dataDir, [script]);
    const { sessionId } = await a.sessions.start({ prompt: 'hi' });
    await waitTurns(a.sessions, sessionId, 1);
    await until(() => late);
    await new Promise((r) => setTimeout(r, 300));
    const state = await a.sessions.inspect(sessionId);
    expect(late).toMatchObject({ ok: false });
    expect(state.effects.has('tool:toolu_late')).toBe(false);
    expect(filesFromState(state)).toEqual({});
    expect(journalRecords(state, EVENTS)).toHaveLength(journaledAtEnd);
    expect(a.errors).toEqual([]);
  });
});

describe('the OpenCode drive, built-ins on', () => {
  it("records OpenCode's own tool calls: each one's intent before it runs (after its event is in the log), its outcome after", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    const seen: { init?: HostInit; begin?: Extract<ToHost, { type: 'reply' }>; intentBeforeReply?: boolean; eventFirst?: boolean } = {};
    const turn: Script = async (h) => {
      seen.init = h.init;
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.tool.input.started', { id: 'call_sh', name: 'shell' }], ['session.tool.called', { id: 'call_sh', input: { command: 'date > d.txt' } }]);
      seen.begin = await h.request({ type: 'builtin', phase: 'begin', callId: 'call_sh', name: 'shell', input: { command: 'date > d.txt' } });
      const state = h.ctx.state();
      seen.intentBeforeReply = state.effects.get('tool:call_sh')?.status === 'running';
      // OpenCode's record of the call was in the log before the call's intent.
      const event = journalRecords<OpenCodeEvent>(state, EVENTS).find((r) => r.data.type === 'session.tool.called.1')!;
      seen.eventFirst = event.at < state.effectOrder.indexOf(state.effects.get('tool:call_sh')!);
      await h.request({ type: 'builtin', phase: 'end', callId: 'call_sh', name: 'shell', text: 'ok', isError: false });
      h.commit(['session.tool.succeeded', { id: 'call_sh', result: 'ok' }]);
      // A read: replay-safe.
      h.commit(['session.tool.input.started', { id: 'call_rd', name: 'read' }], ['session.tool.called', { id: 'call_rd', input: { path: 'd.txt' } }]);
      await h.request({ type: 'builtin', phase: 'begin', callId: 'call_rd', name: 'read', input: { path: 'd.txt' } });
      await h.request({ type: 'builtin', phase: 'end', callId: 'call_rd', name: 'read', text: 'ENOENT', isError: true });
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.text.ended', { text: 'done' }], ['session.step.ended'], ['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [turn], { builtins: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'run date');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(a.errors).toEqual([]);
    expect(seen.init).toMatchObject({ builtins: [...OPENCODE_TOOLS], directory: '/workspace' });
    expect(seen.init!.system).toContain('virtual workspace at /workspace');
    expect(seen.begin).toMatchObject({ ok: true });
    expect(seen.intentBeforeReply).toBe(true);
    expect(seen.eventFirst).toBe(true);
    // On a virtual workspace every built-in is `safe`: a call that never reported changed nothing.
    expect(state.effects.get('tool:call_sh')).toMatchObject({ name: 'tool.shell', replay: 'safe', status: 'completed', output: 'ok' });
    expect(state.effects.get('tool:call_rd')).toMatchObject({ name: 'tool.read', replay: 'safe', status: 'failed', error: { message: 'ENOENT' } });
    // No working directory of this machine: nothing recorded about one.
    expect(journalRecords(state, WORKSPACE_STREAM)).toHaveLength(0);
  });

  it('a crash mid built-in: the next drive resumes with a note that says it was interrupted and is safe to repeat; a re-dispatch of the call runs it again', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const first: Script = async (h) => {
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.tool.input.started', { id: 'call_x', name: 'shell' }], ['session.tool.called', { id: 'call_x', input: { command: 'make build' } }]);
      await h.request({ type: 'builtin', phase: 'begin', callId: 'call_x', name: 'shell', input: { command: 'make build' } });
      // The command runs; the crash comes before OpenCode reports it.
      await new Promise(() => {});
    };
    let note = '';
    let again: Extract<ToHost, { type: 'reply' }> | undefined;
    const second: Script = async (h) => {
      open(h);
      const r = await h.take('recover');
      note = r.note;
      h.send({ type: 'reply', id: r.id, ok: true });
      h.commit(['session.tool.failed', { id: 'call_x', error: { type: 'aborted', message: 'Tool execution interrupted: shell' } }]);
      // A dispatch of the same call again runs it: the cut-off attempt changed nothing on the workspace.
      again = await h.request({ type: 'builtin', phase: 'begin', callId: 'call_x', name: 'shell', input: { command: 'make build' } });
      await h.request({ type: 'builtin', phase: 'end', callId: 'call_x', name: 'shell', text: 'built', isError: false });
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started'], ['session.text.ended', { text: 'Built.' }], ['session.step.ended'], ['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [first], { builtins: true, chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 300 });
    await a.sessions.send(sessionId, 'build');
    await died;
    await a.world.close?.();
    const b = await session(dataDir, [second], { builtins: true, chaos: true, recover: true });
    const state = await waitTurns(b.sessions, sessionId, 1);
    expect(b.errors).toEqual([]);
    expect(note).toContain('shell({"command":"make build"}), call call_x: interrupted before it reported a result. It is safe to repeat.');
    expect(note).not.toContain('[Environment]');
    expect(again).toMatchObject({ ok: true });
    expect(state.effects.get('tool:call_x')).toMatchObject({ name: 'tool.shell', replay: 'safe', status: 'completed', output: 'built' });
    expect(state.effectOrder.filter((e) => e.key === 'tool:call_x' && !state.superseded.has(e.stepId)).map((e) => e.status)).toEqual(['completed']);
  });

  it('in memory, a turn picked up by a new drive: the first call after it hears its workspace was reset', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'oc-fake-'));
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const call = async (h: FakeHost, id: string, name: string, input: unknown, text?: string) => {
      h.commit(['session.tool.input.started', { id, name }], ['session.tool.called', { id, input }]);
      await h.request({ type: 'builtin', phase: 'begin', callId: id, name, input });
      return text === undefined ? undefined : h.request({ type: 'builtin', phase: 'end', callId: id, name, text, isError: false });
    };
    const first: Script = async (h) => {
      open(h);
      await admit(h);
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.step.started']);
      expect(await call(h, 'call_a', 'write', { path: 'a.txt', content: 'a' }, 'Created file successfully: a.txt')).toMatchObject({ ok: true });
      await call(h, 'call_b', 'shell', { command: 'sleep 5' });
      await new Promise(() => {});
    };
    let after: Extract<ToHost, { type: 'reply' }> | undefined;
    const second: Script = async (h) => {
      open(h);
      const r = await h.take('recover');
      h.send({ type: 'reply', id: r.id, ok: true });
      h.commit(['session.tool.failed', { id: 'call_b', error: { type: 'aborted', message: 'Tool execution interrupted: shell' } }]);
      after = await call(h, 'call_c', 'read', { path: 'a.txt' }, 'File not found: a.txt');
      await h.request({ type: 'barrier', kind: 'primary' });
      h.commit(['session.text.ended', { text: 'It is gone.' }], ['session.step.ended'], ['session.execution.succeeded']);
      await h.take('close');
    };
    const a = await session(dataDir, [first], { builtins: true, durable: false, chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 300 });
    await a.sessions.send(sessionId, 'go');
    await died;
    await a.world.close?.();
    const b = await session(dataDir, [second], { builtins: true, durable: false, chaos: true, recover: true });
    const state = await waitTurns(b.sessions, sessionId, 1);
    expect(b.errors).toEqual([]);
    expect(after).toMatchObject({ ok: true, note: RESET_NOTE });
    expect(state.effects.get('tool:call_c')?.output).toBe(`${RESET_NOTE}\n\nFile not found: a.txt`);
    expect(filesFromState(state)).toEqual({});
  });
});

describe('the options', () => {
  const virtual = () => ({ kind: 'virtual' as const, runtime: workspaceRuntime() });
  it("no workspace: tool-less; `builtins` other than `false` is a TypeError, since OpenCode's own tools never run on this machine", () => {
    expect(() => opencodeHarness({})).not.toThrow();
    expect(() => opencodeHarness({ builtins: false })).not.toThrow();
    for (const builtins of [true, ['shell'] as const]) expect(() => opencodeHarness({ builtins })).toThrow(/needs a `workspace`/);
  });
  it('with a workspace: `true` and the default are all seven, a list narrows them, `false` and an unknown tool are TypeErrors', () => {
    expect(() => opencodeHarness({ workspace: virtual() })).not.toThrow();
    expect(() => opencodeHarness({ workspace: virtual(), builtins: true })).not.toThrow();
    expect(() => opencodeHarness({ workspace: virtual(), builtins: ['read', 'glob'] })).not.toThrow();
    expect(() => opencodeHarness({ workspace: virtual(), builtins: false })).toThrow(/`builtins: false` with a `workspace`/);
    expect(() => opencodeHarness({ workspace: virtual(), builtins: ['webfetch'] as never })).toThrow(/not one of OpenCode's own tools/);
  });
  it('a workspace that is not resolved (a bare descriptor) is a TypeError naming durable-harness/opencode; a resolved sandbox is the sandbox tier\'s acquire', () => {
    expect(() => opencodeHarness({ workspace: { kind: 'virtual', durable: true } as never })).toThrow(/durable-harness\/opencode resolves/);
    expect(() => opencodeHarness({ workspace: { kind: 'sandbox', vcpus: 2 } as never })).toThrow(/durable-harness\/opencode resolves virtual\(\) and sandbox\(\)/);
    const acquire = () => Promise.reject(new Error('not in this test'));
    expect(() => opencodeHarness({ workspace: { kind: 'sandbox', acquire } })).not.toThrow();
    expect(() => opencodeHarness({ workspace: { kind: 'sandbox', acquire }, builtins: ['read', 'grep'] })).not.toThrow();
  });
  it('the options that are gone are TypeErrors naming what replaces them', () => {
    for (const [key, replacement] of [
      ['mode', /drop `mode`/],
      ['directory', /workspace: virtual\(\)/],
      ['host', /pass `root`/],
      ['workspaceNote', /drop `workspaceNote`/],
    ] as const)
      expect(() => opencodeHarness({ [key]: 'x' } as never)).toThrow(replacement);
  });
});

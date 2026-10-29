import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorld, type LocalWorld } from '@workflow/world-local';
import { type DriveContext, type DurableSessions, durableSessions, type HostTool, QUEUE_PREFIX, readDoc, type SessionState, UNAVAILABLE_STEP, vercelAnchors } from 'harness-world';
import { filesFromState, readLedger, unsafeTools, workspaceTools } from 'harness-world-hands';
import { existsSync, readFileSync } from 'node:fs';
import { groupAlive, journalRecords, type SessionMachine, transcriptOf } from 'harness-world-process';
import { FILES_LOST_NOTE, SANDBOX_DOC, sessionName } from 'harness-world-sandbox';
import type { FakeSandboxApi } from 'harness-world-sandbox/testing/fake-api';
import { afterEach, describe, expect, it } from 'vitest';
import { CLAUDE_BUILTINS, CLAUDE_BUILTINS_OFF, claudeCodeHarness, DRIVE_NOTE, lostPrompts, NO_TOOLS_NOTE, protectedPath, TOOLS_NOTE } from '../src/harness.ts';
import { uuidFrom } from '../src/ids.ts';
import { CLAUDE_STREAM, JournalSessionStore, type TranscriptLine, toolUseIds } from '../src/store.ts';
import { type FakeSdk, fakeSdk } from './fake-sdk.ts';
import { standIn } from './stand-in.ts';

/**
 * The harness end to end on world-local, with a scripted stand-in for the
 * Agent SDK (`fake-sdk.ts`) and the sandbox tier's stand-in VM (`stand-in.ts`:
 * the real session factory over local relays): no network, no `claude`
 * binary. What it checks is the harness's own logic: the attach and its
 * refusals, the mirror, the tool bridge mapping, the barrier,
 * place-before-send, steering, the hooks, and recovery from the log. The real
 * CLI is `sandbox-restart.test.ts`'s.
 */

const worlds: LocalWorld[] = [];
const apis: FakeSandboxApi[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close?.();
  for (const a of apis.splice(0)) await a.dispose();
});

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 20_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
}

const turnsEnded = (state: SessionState) => transcriptOf(state).filter((r) => r.data.type === 'turn').length;
const waitTurns = (sessions: DurableSessions, id: string, n: number) =>
  until(async () => {
    const s = await sessions.inspect(id);
    return turnsEnded(s) >= n ? s : undefined;
  });

/**
 * The turns' end, and the lines the CLI writes after its result (last-prompt):
 * the drive's last batch commits those a moment after the turn's end does.
 */
const waitFlushed = (sessions: DurableSessions, id: string, n: number) =>
  until(async () => {
    const s = await sessions.inspect(id);
    return turnsEnded(s) >= n && native(s).at(-1)?.type === 'last-prompt' ? s : undefined;
  });

/** The stand-in platform a test shares across its sessions (another process, the same VMs and Drives). */
function platform(): FakeSandboxApi {
  const { api } = standIn();
  apis.push(api);
  return api;
}

async function session(
  dataDir: string,
  sdk: FakeSdk,
  opts: {
    chaos?: boolean;
    recover?: boolean;
    crash?: () => void;
    confirmMs?: number;
    idleMs?: number;
    /** Claude Code's own tools (no line: the harness's default, off). */
    builtins?: boolean | string[];
    /** The app's tools. Default the workspace tools, or (built-ins on) the side-effect tools. */
    tools?: HostTool[];
    /** The platform (default a fresh one). */
    api?: FakeSandboxApi;
    /** Vercel's anchors: another process's open effect is held there, whatever runs on this machine. */
    vercel?: boolean;
    /** The deliveries' log lines. */
    log?: (message: string) => void;
  } = {}
) {
  const world = createWorld({ dataDir, recoverActiveRuns: opts.recover ?? false });
  worlds.push(world);
  const api = opts.api ?? platform();
  const { sandbox } = standIn({ api });
  const sessions = durableSessions({
    world,
    harness: claudeCodeHarness({
      ...(opts.builtins !== undefined ? { builtins: opts.builtins } : {}),
      tools: opts.tools ?? (opts.builtins ? unsafeTools({ confirmMs: opts.confirmMs ?? 3000 }) : workspaceTools({ confirmMs: opts.confirmMs ?? 3000 })),
      sandbox,
      // The fake SDK's "CLI" is node, on the stand-in VM.
      executable: process.execPath,
      sdk,
      credential: async () => 'test-key',
      // The stand-in VM is this machine: no bubblewrap on macOS.
      unsandboxedCommands: true,
      ...(opts.idleMs ? { idleMs: opts.idleMs } : {}),
    }),
    ...(opts.chaos ? { chaos: true, crash: opts.crash ?? (() => {}) } : {}),
    ...(opts.vercel ? { anchors: vercelAnchors() } : {}),
    ...(opts.log ? { log: opts.log } : {}),
  });
  // The deliveries this process is running: a drive lives until its delivery's handler returns.
  let inflight = 0;
  const drainWaiters: (() => void)[] = [];
  world.registerHandler(QUEUE_PREFIX, async (request: Request) => {
    inflight++;
    try {
      return await sessions.handler(request);
    } finally {
      if (--inflight === 0) for (const resolve of drainWaiters.splice(0)) resolve();
    }
  });
  await world.start?.();
  /** Resolves once no delivery runs here (no drive of this process can take a later message). */
  const drained = () => (inflight === 0 ? Promise.resolve() : new Promise<void>((resolve) => drainWaiters.push(resolve)));
  return { world, sessions, api, drained };
}

const native = (state: SessionState) => journalRecords<TranscriptLine>(state, CLAUDE_STREAM).map((r) => r.data);
/** The session's Drive on the stand-in platform: the CLI's `/workspace`. */
/** Whether `bwrap` is installed here (what the stand-in VM's self-test runs). */
function bwrapOnPath(): boolean {
  return (process.env.PATH ?? '').split(':').some((d) => d && existsSync(join(d, 'bwrap')));
}

const driveOf = (api: FakeSandboxApi, sessionId: string) => api.driveDir(sessionName('prj_stand_in', sessionId));

describe('claudeCodeHarness (scripted SDK, stand-in VM)', () => {
  it('runs a turn: built-ins off, the workspace tools over MCP keyed by the tool_use id, the transcript in the log, the CLI in the VM', async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk);
    const { sessionId } = await a.sessions.start();
    const sub = await a.sessions.send(sessionId, 'call write_file {"path":"notes.txt","content":"hi"}');
    // The turn's end, and the lines the CLI writes after it (the drive's last flush).
    const state = await waitFlushed(a.sessions, sessionId, 1);

    // The query: built-ins off, our MCP server, the durable options, a new session whose id comes from the run.
    const opts = sdk.queries[0]!;
    expect(opts).toMatchObject({
      tools: [],
      strictMcpConfig: true,
      settingSources: [],
      permissionMode: 'dontAsk',
      persistSession: true,
      sessionStoreFlush: 'eager',
      includePartialMessages: true,
      sessionId: uuidFrom(sessionId),
      systemPrompt: { type: 'preset', preset: 'claude_code', snapshot: true },
    });
    expect(opts.resume).toBeUndefined();
    expect(opts.allowedTools).toEqual(workspaceTools().map((t) => `mcp__hands__${t.name}`));
    // The CLI's env: the VM's model port and the attach's key, the VM's cache dirs; the Function's own config dir for the SDK.
    expect(opts.env).toMatchObject({ CLAUDE_CODE_DISABLE_AUTO_MEMORY: '1', CLAUDE_CODE_PROJECT_DIR_NAME: 'session', PATH: '/usr/local/bin:/usr/bin:/bin' });
    expect(opts.env.ANTHROPIC_AUTH_TOKEN).toMatch(/^dhm-[A-Za-z0-9_-]{32}$/);
    expect(opts.env.ANTHROPIC_BASE_URL).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
    expect(opts.env.CLAUDE_CONFIG_DIR).toMatch(/dh-claude-/);
    expect(opts.cwd).toBe(driveOf(a.api, sessionId));
    expect(opts.pathToClaudeCodeExecutable).toBe(process.execPath);
    // Its record: the attach was a fact, and the Drive is named.
    expect(state.effectOrder.filter((e) => e.name === 'sandbox.attach').every((e) => e.status === 'completed')).toBe(true);
    expect(readDoc(state, SANDBOX_DOC).value).toMatchObject({ sandboxName: sessionName('prj_stand_in', sessionId) });

    // The tool ran once, as an effect keyed by the CLI's tool_use id.
    const tools = state.effectOrder.filter((e) => e.name.startsWith('tool.'));
    expect(tools).toHaveLength(1);
    const call = toolUseIds(native(state).find((e) => toolUseIds(e).length)!)[0]!;
    expect(tools[0]).toMatchObject({ key: `tool:${call}`, name: 'tool.write_file', status: 'completed', input: { path: 'notes.txt', content: 'hi' } });
    expect(filesFromState(state)).toEqual({ '/workspace/notes.txt': 'hi' });
    // The normalized transcript.
    expect(transcriptOf(state).map((r) => r.data)).toEqual([
      { type: 'message', role: 'assistant', text: 'Calling write_file.', toolCalls: [{ id: call, name: 'write_file' }] },
      { type: 'message', role: 'assistant', text: 'write_file returned: wrote 2 bytes to /workspace/notes.txt' },
      { type: 'turn', status: 'completed' },
    ]);
    // The prompt was placed before the CLI saw it, and its transcript entry carries the id derived from it.
    const place = state.effectOrder.find((e) => e.name === 'place')!;
    expect(place.placed).toEqual([sub]);
    expect(native(state).find((e) => e.type === 'user')?.uuid).toBe(uuidFrom(sub));
    expect(native(state).map((e) => e.type)).toEqual(['queue-operation', 'user', 'assistant', 'user', 'assistant', 'last-prompt']);
  });

  it('a never tool runs only once the assistant entry that asked for it is in the log', async () => {
    const sdk = fakeSdk({ mirrorLagMs: 300 });
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'call unsafe_send_email {"to":"a@b.c","subject":"hi","body":"x"}');
    const state = await waitTurns(a.sessions, sessionId, 1);
    const tool = state.effectOrder.findIndex((e) => e.name === 'tool.unsafe_send_email');
    const call = state.effectOrder[tool]!.key.slice('tool:'.length);
    const batch = journalRecords<TranscriptLine>(state, CLAUDE_STREAM).find((r) => toolUseIds(r.data).includes(call))!;
    expect(batch.at).toBeLessThan(tool);
    expect(await readLedger(a.world, sessionId)).toHaveLength(1);
  });

  it('a crash mid never-tool: a fresh process resumes the session from the log, with the recovery note, and the side effect happened once', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-'));
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    const first = fakeSdk();
    const a = await session(dataDir, first, { chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 300 });
    await a.sessions.send(sessionId, 'call unsafe_make_payment {"to":"acme","amount_usd":5,"memo":"order 7"}');
    await died;
    await a.world.close?.();

    // Another process, the same VM and Drive: only the log.
    const second = fakeSdk();
    const b = await session(dataDir, second, { chaos: true, recover: true, api: a.api });
    const state = await waitTurns(b.sessions, sessionId, 1);

    // It resumed the same Claude Code session, loaded from the journal.
    expect(second.queries[0]).toMatchObject({ resume: uuidFrom(sessionId) });
    expect(second.queries[0]!.sessionId).toBeUndefined();
    // The payment: interrupted, never rerun; one ledger line.
    const pay = state.effectOrder.filter((e) => e.name === 'tool.unsafe_make_payment');
    expect(pay).toHaveLength(1);
    expect(pay[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
    expect((await readLedger(b.world, sessionId)).map((l) => l.tool)).toEqual(['unsafe_make_payment']);
    // The call that made it was in the log before it ran (the barrier), so the resumed CLI knew of it.
    expect(native(state).some((e) => toolUseIds(e).includes(pay[0]!.key.slice('tool:'.length)))).toBe(true);
    // The recovery note told the model what the log knows.
    const entries = transcriptOf(state).map((r) => r.data);
    const note = entries.find((e) => e.type === 'message' && e.role === 'user') as { text: string; synthetic: string };
    expect(note.synthetic).toBe('recovery');
    expect(note.text).toContain('unsafe_make_payment');
    expect(note.text).toContain('may have taken effect');
    // The prompt reached the transcript before the crash: the note does not quote it again.
    expect(note.text).not.toContain("The user's message in that turn");
    expect(entries.at(-2)).toEqual({ type: 'message', role: 'assistant', text: 'Recovered from the log.' });
    expect(entries.at(-1)).toEqual({ type: 'turn', status: 'completed' });
  });

  it('the CLI dies mid-call: the drive waits for the call (nothing judges it by silence), so the call completes rather than being taken for abandoned', async () => {
    const sdk = fakeSdk();
    // A long call (6 s): a delivery landing meanwhile finds the drive running and is covered.
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { confirmMs: 12000 });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'exit-during unsafe_send_email {"to":"a@b.c","subject":"hi","body":"x"}');
    // The redelivery's recovery turn (the fake echoes the note).
    const state = await waitTurns(a.sessions, sessionId, 1);
    const email = state.effectOrder.filter((e) => e.name === 'tool.unsafe_send_email');
    expect(email).toHaveLength(1);
    expect(email[0]).toMatchObject({ status: 'completed' });
    expect(await readLedger(a.world, sessionId)).toHaveLength(1);
    const note = transcriptOf(state).map((r) => r.data).find((e) => e.type === 'message' && e.role === 'user') as { text: string };
    expect(note.text).toContain('completed. Its result');
  });

  it('a steer sent while a tool runs goes in mid-turn, placed first, at priority next', async () => {
    const seen: { text: string; priority?: string }[] = [];
    const sdk = fakeSdk({ onMessage: (m) => seen.push(m) });
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'call bash {"command":"sleep 2; echo done"}');
    await until(async () => ((await a.sessions.inspect(sessionId)).effectOrder.some((e) => e.name === 'tool.bash') ? true : undefined));
    const steer = await a.sessions.send(sessionId, 'also say hi', 'steer');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(seen.map((m) => [m.text, m.priority])).toEqual([
      ['call bash {"command":"sleep 2; echo done"}', undefined],
      ['also say hi', 'next'],
    ]);
    expect(state.placed.has(steer)).toBe(true);
    // A recovery would find it in the transcript (as the queued_command it became).
    expect(lostPrompts(state, [...state.placed], new JournalSessionStore({} as never, { records: journalRecords(state) }))).toEqual([]);
    // One turn consumed both.
    expect(turnsEnded(state)).toBe(1);
    expect(transcriptOf(state).at(-2)?.data).toMatchObject({ text: expect.stringContaining('read your steer') });
  });

  it('idleMs: a follow-up within the grace goes to the same child, without a resume', async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { idleMs: 4000 });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello');
    await waitTurns(a.sessions, sessionId, 1);
    await a.sessions.send(sessionId, 'again');
    const state = await waitTurns(a.sessions, sessionId, 2);
    expect(sdk.queries).toHaveLength(1);
    expect(transcriptOf(state).map((r) => r.data).filter((e) => e.type === 'message').map((e) => (e as { text: string }).text)).toEqual(['echo: hello', 'echo: again']);
  });

  it('idleMs: a follow-up whose delivery lands on another process goes to the idle child, not to a second drive', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-'));
    const sdk = fakeSdk();
    const a = await session(dataDir, sdk, { idleMs: 4000, vercel: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello');
    await waitTurns(a.sessions, sessionId, 1);
    // Another instance of the app, on the same log and VMs: the follow-up's delivery lands there.
    const lines: string[] = [];
    const other = fakeSdk();
    const b = await session(dataDir, other, { idleMs: 4000, vercel: true, api: a.api, log: (m) => lines.push(m) });
    await b.sessions.send(sessionId, 'again');
    const state = await waitTurns(a.sessions, sessionId, 2);
    await b.drained();
    // That instance leaves it to the idle drive: no second drive to fence the first off the VM.
    expect(lines).toContain('skip: open work is held elsewhere');
    expect(lines).not.toContain('in-band fence: armed');
    expect(other.queries).toHaveLength(0);
    expect(sdk.queries).toHaveLength(1);
    expect(transcriptOf(state).map((r) => r.data).filter((e) => e.type === 'message').map((e) => (e as { text: string }).text)).toEqual(['echo: hello', 'echo: again']);
  });

  it('an idle wake starts nothing: no VM is attached', async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello');
    await waitTurns(a.sessions, sessionId, 1);
    const attaches = (await a.sessions.inspect(sessionId)).effectOrder.filter((e) => e.name === 'sandbox.attach').length;
    await a.sessions.drive(sessionId);
    expect(sdk.queries).toHaveLength(1);
    expect((await a.sessions.inspect(sessionId)).effectOrder.filter((e) => e.name === 'sandbox.attach')).toHaveLength(attaches);
    // A follow-up resumes the session.
    await a.sessions.send(sessionId, 'again');
    await waitTurns(a.sessions, sessionId, 2);
    expect(sdk.queries[1]).toMatchObject({ resume: uuidFrom(sessionId) });
  });
});

describe('the refusal by provenance (docs/workspaces.md §6 rule 1)', () => {
  const failedTurn = async (harness: ReturnType<typeof claudeCodeHarness>) => {
    const world = createWorld({ dataDir: await mkdtemp(join(tmpdir(), 'hwcc-')), recoverActiveRuns: false });
    worlds.push(world);
    const sessions = durableSessions({ world, harness });
    world.registerHandler(QUEUE_PREFIX, sessions.handler);
    await world.start?.();
    const { sessionId } = await sessions.start();
    await sessions.send(sessionId, 'hello');
    return until(async () => {
      const s = await sessions.inspect(sessionId);
      return [...s.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { code: string; message: string } | undefined;
    });
  };

  it('a harness with no sandbox fails the turn with sandbox_required, and starts no CLI', async () => {
    const sdk = fakeSdk();
    const failed = await failedTurn(claudeCodeHarness({ sdk, credential: async () => 'k' }));
    expect(failed).toMatchObject({ code: 'sandbox_required', message: 'Claude Code runs only in a Vercel Sandbox: pass workspace: sandbox()' });
    expect(sdk.queries).toHaveLength(0);
  });

  // Where bubblewrap runs on this machine (Linux CI), the stand-in VM passes the self-test.
  it.skipIf(bwrapOnPath())("built-ins on, and Claude Code's own sandbox can't start in the VM: the turn fails with an Unavailable, and no CLI starts", async () => {
    const sdk = fakeSdk();
    const { api, sandbox } = standIn();
    apis.push(api);
    const failed = await failedTurn(claudeCodeHarness({ sdk, credential: async () => 'k', builtins: true, sandbox, executable: process.execPath }));
    expect(failed.message).toContain("Claude Code's own sandbox (bubblewrap) doesn't start in this session's VM");
    expect(sdk.queries).toHaveLength(0);
  });

  it('a hand-written machine with a spawn of its own is refused, whatever its shape', async () => {
    const sdk = fakeSdk();
    const spawned: string[] = [];
    const fake = {
      sessionId: 's',
      fence: { count: 1, fenced: true },
      root: '/tmp',
      dirs: { home: '/tmp', config: '/tmp', 'codex-home': '/tmp', tmp: '/tmp', cache: '/tmp' },
      signal: new AbortController().signal,
      modelUrl: 'http://127.0.0.1:1',
      modelKey: 'dhm-hand-written-key-000000000000',
      notes: [],
      spawn: async (o: { command: string }) => void spawned.push(o.command),
      files: { upload: async () => {}, wipe: async () => {} },
      tail: async () => ({}),
      fsync: async () => {},
      close: async () => {},
      brand: {},
    } as unknown as SessionMachine;
    const failed = await failedTurn(
      claudeCodeHarness({ sdk, credential: async () => 'k', sandbox: { attach: async (_ctx: DriveContext) => fake, network: 'allow-all', budget: { turnUsd: Infinity, sessionUsd: Infinity } } })
    );
    expect(failed.code).toBe('sandbox_required');
    expect(spawned).toEqual([]);
    expect(sdk.queries).toHaveLength(0);
  });
});

describe('claudeCodeHarness defaults (scripted SDK)', () => {
  it("no builtins line: tool-less, the CLI is offered only the app's tools, and the system prompt says so", async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello');
    await waitTurns(a.sessions, sessionId, 1);
    const opts = sdk.queries[0]!;
    // No built-in at all (`tools: []`), no hooks to let one through, nothing permitted but the app's tools.
    expect(opts.tools).toEqual([]);
    expect(opts.hooks).toBeUndefined();
    expect(opts.sandbox).toBeUndefined();
    expect(opts.disallowedTools).toEqual([...CLAUDE_BUILTINS_OFF, 'WebSearch']);
    expect(opts.allowedTools).toEqual(workspaceTools().map((t) => `mcp__hands__${t.name}`));
    expect(Object.keys(opts.mcpServers)).toEqual(['hands']);
    expect(opts.permissionMode).toBe('dontAsk');
    expect(opts.systemPrompt.append).toBe(TOOLS_NOTE);
  });

  it('no builtins line and no tools: no tools at all, and the note says the session has none', async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { tools: [] });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello');
    await waitTurns(a.sessions, sessionId, 1);
    const opts = sdk.queries[0]!;
    expect(opts).toMatchObject({ tools: [], allowedTools: [] });
    expect(opts.mcpServers).toBeUndefined();
    expect(opts.systemPrompt.append).toBe(NO_TOOLS_NOTE);
  });
});

describe('claudeCodeHarness, built-ins on (scripted SDK)', () => {
  it("runs a turn with Claude Code's own Bash and Write on the Drive: each call an effect keyed by its tool_use id, committed before it ran, the app's tools next to them", async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { builtins: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'builtin Bash {"command":"echo built-in > b.txt && cat b.txt"}');
    let state = await waitTurns(a.sessions, sessionId, 1);
    await a.sessions.send(sessionId, 'builtin Write {"file_path":"notes.md","content":"durable"}');
    state = await waitTurns(a.sessions, sessionId, 2);

    // The query: the pinned allowlist, the ones whose state the log does not carry off, the hooks, the app's tools over MCP.
    const opts = sdk.queries[0]!;
    expect(opts.tools).toEqual([...CLAUDE_BUILTINS]);
    expect(opts.disallowedTools).toEqual(expect.arrayContaining(['CronCreate', 'ScheduleWakeup', 'EnterWorktree', 'AskUserQuestion', 'TaskCreate', 'WebSearch']));
    expect(Object.keys(opts.hooks)).toEqual(['PreToolUse', 'PostToolUse', 'PostToolUseFailure']);
    expect(opts.allowedTools).toEqual(unsafeTools().map((t) => `mcp__hands__${t.name}`));
    expect(opts.systemPrompt.append).toBe(DRIVE_NOTE);
    // Each built-in call: one effect, its intent before the call ran, completed with the result.
    const calls = state.effectOrder.filter((e) => e.name.startsWith('tool.'));
    expect(calls.map((e) => [e.name, e.replay, e.status])).toEqual([
      ['tool.Bash', 'never', 'completed'],
      ['tool.Write', 'never', 'completed'],
    ]);
    expect(calls[0]).toMatchObject({ key: expect.stringMatching(/^tool:toolu_/), input: { command: 'echo built-in > b.txt && cat b.txt' }, output: 'built-in' });
    // The assistant entry that made each call was in the log before the call's intent (the barrier).
    for (const c of calls) {
      const at = state.effectOrder.indexOf(c);
      expect(journalRecords<TranscriptLine>(state, CLAUDE_STREAM).some((r) => r.at < at && toolUseIds(r.data).includes(c.key.slice('tool:'.length)))).toBe(true);
    }
    // The files are on the session's Drive, not in the log.
    const drive = driveOf(a.api, sessionId);
    expect(readFileSync(join(drive, 'b.txt'), 'utf8')).toBe('built-in\n');
    expect(readFileSync(join(drive, 'notes.md'), 'utf8')).toBe('durable');
    expect(filesFromState(state)).toEqual({});
  });

  it('Bash: its timeout is clamped to what is left of the invocation, and a background command is refused before it runs', async () => {
    const decisions: { name: string; decision: string; updatedInput?: Record<string, unknown>; reason?: string }[] = [];
    const sdk = fakeSdk({ onDecision: (d) => decisions.push(d) });
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { builtins: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'builtin Bash {"command":"echo hi","timeout":5000000}');
    await waitTurns(a.sessions, sessionId, 1);
    await a.sessions.send(sessionId, 'builtin Bash {"command":"sleep 100","run_in_background":true}');
    const state = await waitTurns(a.sessions, sessionId, 2);
    expect(decisions[0]).toMatchObject({ name: 'Bash', decision: 'allow', updatedInput: { command: 'echo hi' } });
    const timeout = decisions[0]!.updatedInput!.timeout as number;
    // The invocation's budget (10 min by default), less the margin the outcome needs.
    expect(timeout).toBeGreaterThan(0);
    expect(timeout).toBeLessThan(10 * 60_000);
    // The call's intent records the clamped input.
    expect(state.effectOrder.find((e) => e.name === 'tool.Bash')?.input).toMatchObject({ timeout });
    expect(decisions[1]).toMatchObject({ decision: 'deny', reason: expect.stringContaining("can't run in the background") });
    expect(state.effectOrder.filter((e) => e.name === 'tool.Bash')).toHaveLength(1);
  });

  it("the file tools don't write the repo's or Claude's config on the Drive (.git/, .claude/), however the path is spelled", async () => {
    const decisions: { name: string; decision: string; reason?: string }[] = [];
    const sdk = fakeSdk({ onDecision: (d) => decisions.push(d) });
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { builtins: true });
    const { sessionId } = await a.sessions.start();
    const paths = ['.git/config', './.git/hooks/pre-commit', 'src/../.claude/settings.json', '.claude', 'notes/.gitkeep'];
    for (const [i, path] of paths.entries()) {
      await a.sessions.send(sessionId, `builtin Write ${JSON.stringify({ file_path: path, content: 'x' })}`);
      await waitTurns(a.sessions, sessionId, i + 1);
    }
    expect(decisions.map((d) => d.decision)).toEqual(['deny', 'deny', 'deny', 'deny', 'allow']);
    expect(decisions[0]!.reason).toContain('.git and .claude');
    const state = await a.sessions.inspect(sessionId);
    // A refused call never became an intent.
    expect(state.effectOrder.filter((e) => e.name === 'tool.Write')).toHaveLength(1);
    expect(existsSync(join(driveOf(a.api, sessionId), '.git'))).toBe(false);
    // In the VM, absolute paths too.
    expect(['/workspace/.git/config', '/workspace/.claude', '/workspace/x/../.git'].every((p) => protectedPath('/workspace', p))).toBe(true);
    expect(['/workspace/.github/ci.yml', '/workspace/.gitignore', '/workspace/a/.git/config', '/tmp/.git/config'].some((p) => protectedPath('/workspace', p))).toBe(false);
  });

  it('a mirror batch the SDK dropped ends the turn with Unavailable (mirror_unmapped), and the session is kept', async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { builtins: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'mirror-error');
    const refusal = await until(async () => {
      const st = await a.sessions.inspect(sessionId);
      return [...st.effects.values()].find((e) => e.name === UNAVAILABLE_STEP)?.input as { message: string } | undefined;
    });
    expect(refusal.message).toContain('mirror_unmapped');
    await a.sessions.send(sessionId, 'hello again');
    const state = await until(async () => {
      const st = await a.sessions.inspect(sessionId);
      return transcriptOf(st).some((r) => (r.data as { role?: string; text?: string }).role === 'assistant' && (r.data as { text?: string }).text?.includes('hello again')) ? st : undefined;
    });
    expect(state.failure).toBeUndefined();
  });

  it("a built-in that fails is a failed step with the CLI's error text (not interrupted)", async () => {
    const sdk = fakeSdk();
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { builtins: true });
    const { sessionId } = await a.sessions.start();
    // A failing command: a failed step with the CLI's error text.
    await a.sessions.send(sessionId, 'builtin Bash {"command":"echo nope >&2; exit 3"}');
    const state = await waitTurns(a.sessions, sessionId, 1);
    const call = state.effectOrder.find((e) => e.name === 'tool.Bash')!;
    expect(call).toMatchObject({ status: 'failed', error: { message: 'Exit code 3\nnope' } });
    expect(call.error?.interrupted).toBeUndefined();
  });

  it('a crash mid built-in Bash, recovered by another process on the same VM: the call is reported interrupted and never rerun', async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-'));
    let crashed!: () => void;
    const died = new Promise<void>((r) => (crashed = r));
    let shell = 0;
    const first = fakeSdk({ onBuiltin: (c) => (shell = c.pid ?? 0) });
    const a = await session(dataDir, first, { builtins: true, chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 500 });
    await a.sessions.send(sessionId, 'builtin Bash {"command":"echo started >> runs.txt; sleep 4; echo done > out.txt"}');
    await died;
    await a.world.close?.();
    const drive = driveOf(a.api, sessionId);
    expect(shell).toBeGreaterThan(0);

    // The next process: it arms the same VM with a higher fence, which kills the session's processes there first.
    const second = fakeSdk();
    const b = await session(dataDir, second, { builtins: true, chaos: true, recover: true, api: a.api });
    const state = await waitTurns(b.sessions, sessionId, 1);
    const bash = state.effectOrder.filter((e) => e.name === 'tool.Bash');
    expect(bash).toHaveLength(1);
    expect(bash[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
    expect(readFileSync(join(drive, 'runs.txt'), 'utf8')).toBe('started\n');
    // The degraded stand-in kills by process group, which a command in a group of its own escapes; the cgroup of the privileged tier (a VM) doesn't let it.
    if (process.getuid?.() === 0) {
      expect(groupAlive(shell)).toBe(false);
      await new Promise((r) => setTimeout(r, 4500));
      expect(existsSync(join(drive, 'out.txt'))).toBe(false);
    }
    const entries = transcriptOf(state).map((r) => r.data);
    const note = entries.find((e) => e.type === 'message' && e.role === 'user') as { text: string; synthetic: string };
    expect(note.synthetic).toBe('recovery');
    expect(note.text).toContain(`Bash({"command":"echo started >> runs.txt; sleep 4; echo done > out.txt"}), call ${bash[0]!.key.slice('tool:'.length)}: interrupted before it reported a result. It is not safe to repeat, and it may have taken effect`);
    expect(entries.at(-1)).toEqual({ type: 'turn', status: 'completed' });
  });

  it("the Drive was deleted: the model hears its files were lost, once, and the record forgets it once that turn committed", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), 'hwcc-'));
    const a = await session(dataDir, fakeSdk(), { builtins: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'builtin Write {"file_path":"a.txt","content":"one"}');
    await waitTurns(a.sessions, sessionId, 1);
    await a.drained();
    // The session's VM and Drive go (an operator deleted them); the log stays.
    const name = sessionName('prj_stand_in', sessionId);
    await (await a.api.getSandbox(name))!.delete();
    a.api.dropDrive(name);
    const seen: string[] = [];
    const b = await session(dataDir, fakeSdk({ onMessage: (m) => seen.push(m.text) }), { builtins: true, api: a.api });
    await b.sessions.send(sessionId, 'builtin Read {"file_path":"a.txt"}');
    let state = await waitTurns(b.sessions, sessionId, 2);
    expect(seen[0]).toBe(`${FILES_LOST_NOTE}\n\nbuiltin Read {"file_path":"a.txt"}`);
    expect(state.effectOrder.filter((e) => e.name === 'tool.Read').map((e) => e.status)).toEqual(['failed']);
    await until(async () => (readDoc(await b.sessions.inspect(sessionId), SANDBOX_DOC).value.filesLost === undefined ? true : undefined));
    // The next turn doesn't say it again.
    await b.sessions.send(sessionId, 'hello');
    state = await waitTurns(b.sessions, sessionId, 3);
    expect(seen.at(-1)).toBe('hello');
    expect(transcriptOf(state).map((r) => r.data).filter((e) => e.type === 'message' && e.role === 'user').map((e) => (e as { synthetic: string }).synthetic)).toEqual(['environment']);
  });

  it('the CLI dies mid built-in (the drive lives): the call is committed interrupted', async () => {
    let shell = 0;
    const sdk = fakeSdk({ onBuiltin: (c) => (shell = c.pid ?? 0) });
    const a = await session(await mkdtemp(join(tmpdir(), 'hwcc-')), sdk, { builtins: true });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'exit-during-builtin Bash {"command":"sleep 20"}');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(state.effectOrder.find((e) => e.name === 'tool.Bash')).toMatchObject({ status: 'failed', error: { interrupted: true } });
    expect(shell).toBeGreaterThan(0);
  });
});

describe('lostPrompts', () => {
  const store = (lines: TranscriptLine[]) => new JournalSessionStore({} as never, { records: lines.map((data, seq) => ({ stream: CLAUDE_STREAM, seq, data, batch: 0, stepId: 's', at: 0 })) });
  const state = {
    submissions: [
      { id: 'p1', kind: 'prompt' },
      { id: 's1', kind: 'steer' },
      { id: 'p2', kind: 'prompt' },
      { id: 'p3', kind: 'followUp' },
      { id: 'c1', kind: 'chaos' },
    ],
    effectOrder: [
      { key: 'place:p1', name: 'place', placed: ['p1'] },
      { key: 'place:s1', name: 'place', placed: ['s1'] },
      // A recovery note carried these two.
      { key: 'place:p2', name: 'place', placed: ['p2', 'p3'] },
      { key: 'chaos:c1', name: 'chaos', placed: ['c1'] },
    ],
  } as unknown as SessionState;
  const all = ['p1', 's1', 'p2', 'p3', 'c1'];

  it('a prompt is lost until its own message, the queued_command a steer becomes, or the note that carried it is in the transcript', () => {
    expect(lostPrompts(state, all, store([]))).toEqual(['p1', 's1', 'p2', 'p3']);
    const heard = store([
      { type: 'user', uuid: uuidFrom('p1') },
      { type: 'attachment', uuid: 'a1', attachment: { type: 'queued_command', source_uuid: uuidFrom('s1') } },
      { type: 'user', uuid: uuidFrom('note:p2') },
    ]);
    expect(lostPrompts(state, all, heard)).toEqual([]);
  });
});

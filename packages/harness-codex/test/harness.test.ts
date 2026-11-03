import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { createWorld } from '@workflow/world-local';
import { durableSessions, QUEUE_PREFIX } from 'harness-world';
import { filesFromState, readLedger, workspaceTools } from 'harness-world-hands';
import { journalRecords, transcriptOf } from 'harness-world-process';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { rolloutStream, summarizeRollout, THREAD_STREAM } from '../src/rollout.ts';
import { cacheRootOf, driveOf, fileLines, nameOf, type Rig, rig, rolloutFile, rolloutLines, session, standInEnv, tearDown, threadOf, tmp, turnsEnded, until, vmOf, waitTurns } from './stand-in.ts';

/**
 * The Codex harness end to end on world-local, on the sandbox tier's
 * stand-in (`stand-in.ts`: the session factory over the fake Sandbox API,
 * local relays for VMs, temp dirs for Drives) against the fake app-server
 * (`fake-app-server.mjs`: Codex's protocol subset and its rollout format, a
 * scripted model behind the drive's gateway handler, no network). "A fresh
 * machine" is a new world over the same log with the session's VM torn
 * down: a new VM on the same Drive. The A6 cases (the barrier, tampering,
 * escalation, the placeholder, the self-test) are in
 * `sandbox-restart.test.ts`; the real binary and the Gateway in
 * `live.test.ts`.
 */

beforeAll(standInEnv);

const rigs: Rig[] = [];
afterEach(async () => {
  for (const r of rigs.splice(0)) await r.dispose();
});
const newRig = async (...args: Parameters<typeof rig>) => {
  const r = await rig(...args);
  rigs.push(r);
  return r;
};

describe('the Codex harness (fake app-server, stand-in sandbox)', () => {
  it('runs a turn with a tool, mirrors the rollout byte for byte, and resumes it on a fresh VM', async () => {
    const r = await newRig();
    const dataDir = await tmp('hwc-');
    const a = await session(dataDir, r);
    const { sessionId } = await a.sessions.start({ prompt: 'call write_file {"path":"notes.txt","content":"hi"}' });
    let state = await waitTurns(a.sessions, sessionId, 1);

    // One tool effect, keyed by Codex's call id.
    const calls = state.effectOrder.filter((e) => e.name === 'tool.write_file');
    expect(calls).toHaveLength(1);
    expect(calls[0]!.key).toMatch(/^tool:call_/);
    expect(calls[0]).toMatchObject({ status: 'completed' });
    expect(filesFromState(state)).toEqual({ '/workspace/notes.txt': 'hi' });
    // The normalized transcript.
    const callId = calls[0]!.key.slice('tool:'.length);
    expect(transcriptOf(state).map((x) => x.data)).toEqual([
      { type: 'message', role: 'assistant', text: 'Calling write_file.', toolCalls: [{ id: callId, name: 'write_file' }] },
      { type: 'message', role: 'assistant', text: expect.stringContaining('write_file returned: wrote 2 bytes') },
      { type: 'turn', status: 'completed' },
    ]);
    // The rollout is in the log line for line.
    const fileA = fileLines(rolloutFile(r, sessionId, state));
    expect(rolloutLines(state)).toEqual(fileA);
    const summary = summarizeRollout(rolloutLines(state));
    expect(summary.calls.map((c) => c.callId)).toEqual([callId]);
    expect(summary.clientIds).toEqual(new Set([state.submissions[0]!.id]));
    expect(summary.idle).toBe(true);
    await a.sessions.send(sessionId, 'hello');
    state = await waitTurns(a.sessions, sessionId, 2);
    expect(transcriptOf(state).at(-2)?.data).toEqual({ type: 'message', role: 'assistant', text: 'echo: hello' });
    await a.world.close?.();

    // Another machine, and a new VM on the same Drive: the rollout comes back
    // from the log and the thread resumes by id; new lines append to it.
    const vmA = vmOf(r, sessionId)!.sessionId;
    await tearDown(r, sessionId);
    const b = await session(dataDir, r);
    await b.sessions.send(sessionId, 'call read_file {"path":"notes.txt"}');
    state = await waitTurns(b.sessions, sessionId, 3);
    expect(vmOf(r, sessionId)!.sessionId).not.toBe(vmA);
    expect(state.effectOrder.find((e) => e.name === 'tool.read_file')).toMatchObject({ status: 'completed', output: 'hi' });
    const fileB = fileLines(rolloutFile(r, sessionId, state));
    expect(fileB).toEqual(rolloutLines(state));
    expect(fileB.slice(0, fileA.length)).toEqual(fileA);
    // Same thread throughout.
    expect(journalRecords(state, THREAD_STREAM)).toHaveLength(1);
    // Only the rollout and the config were uploaded into CODEX_HOME: no SQLite, nothing else.
    expect(walk(join(cacheRootOf(r, sessionId), 'codex-home')).filter((p) => !p.startsWith('sessions/'))).toEqual(['config.toml']);
  });

  it('a crash mid never-tool on a fresh VM: the dangling call gets an interrupted output, a recovery note, and the side effect happened once', async () => {
    const r = await newRig();
    const dataDir = await tmp('hwc-');
    let crashed!: () => void;
    const died = new Promise<void>((res) => (crashed = res));
    const a = await session(dataDir, r, { chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 300 });
    await a.sessions.send(sessionId, 'call unsafe_make_payment {"to":"acme","amount_usd":5,"memo":"order 7"}');
    await died;
    await a.world.close?.();
    // The machine is lost with the VM: a fresh VM on the same Drive.
    await tearDown(r, sessionId);

    const b = await session(dataDir, r, { chaos: true, recover: true });
    const state = await waitTurns(b.sessions, sessionId, 1);

    const pay = state.effectOrder.filter((e) => e.name === 'tool.unsafe_make_payment');
    expect(pay).toHaveLength(1);
    expect(pay[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
    const callId = pay[0]!.key.slice('tool:'.length);
    // A never tool waits for the journal: Codex's record of the call was in the log before the call's intent.
    const callLine = journalRecords<string>(state, rolloutStream(threadOf(state)!.threadId)).find((x) => x.data.includes('"function_call"') && x.data.includes(callId))!;
    expect(callLine.at).toBeLessThan(state.effectOrder.indexOf(pay[0]!));
    const ledger = await readLedger(b.world, sessionId);
    expect(ledger.map((l) => [l.tool, l.callKey])).toEqual([['unsafe_make_payment', `tool:${callId}`]]);

    // The dangling call got its output injected from the log, once.
    const lines = rolloutLines(state);
    const outputs = lines.filter((l) => l.includes('"function_call_output"') && l.includes(callId));
    expect(outputs).toHaveLength(1);
    expect(outputs[0]).toContain('may or may not have taken effect');
    expect(summarizeRollout(lines).dangling).toEqual([]);
    // The recovery note, then Codex's reply to it.
    const entries = transcriptOf(state).map((x) => x.data);
    const note = entries.find((e) => e.type === 'message' && e.role === 'user') as { text: string; synthetic: string };
    expect(note.synthetic).toBe('recovery');
    expect(note.text).toContain('unsafe_make_payment');
    expect(note.text).toContain('may have taken effect');
    expect(entries.at(-2)).toEqual({ type: 'message', role: 'assistant', text: expect.stringContaining('Recovered from the log') });
    expect(entries.at(-1)).toEqual({ type: 'turn', status: 'completed' });
    expect(fileLines(rolloutFile(r, sessionId, state))).toEqual(lines);
  });

  it('a crash while the model streams, after a never tool: the chaos step kills the model step, the resumed turn makes the call again, the email went out once', async () => {
    // The model behind the handler types the reply to the tool's result slowly: the stream the crash cuts.
    const r = await newRig({ model: { pieceMs: (text) => (text.startsWith('unsafe_send_email returned') ? 150 : 5) } });
    const dataDir = await tmp('hwc-');
    let crashed!: () => void;
    const died = new Promise<void>((res) => (crashed = res));
    const a = await session(dataDir, r, { chaos: true, crash: () => crashed(), confirmMs: 500 });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'model', afterMs: 400 });
    await a.sessions.send(sessionId, 'call unsafe_send_email {"to":"a@b.c","subject":"hi","body":"x"}');
    await died;
    let state = await a.sessions.inspect(sessionId);
    // "Calling ..." streamed in under 400 ms; the crash fired in the reply's model step.
    const steps = state.effectOrder.filter((e) => e.name === 'model');
    expect(steps.map((e) => e.status)).toEqual(['completed', 'running']);
    expect(steps[0]!.input).toMatchObject({ method: 'POST', path: '/v1/responses' });
    expect(state.effectOrder.find((e) => e.name === 'chaos')).toMatchObject({ output: { effect: 'model', crashedDuring: steps[1]!.key } });
    const email = state.effectOrder.find((e) => e.name === 'tool.unsafe_send_email')!;
    expect(email.status).toBe('completed');
    await a.world.close?.();
    await tearDown(r, sessionId);

    const b = await session(dataDir, r, { chaos: true, recover: true });
    state = await waitTurns(b.sessions, sessionId, 1);
    expect(state.effects.get(steps[1]!.key)).toMatchObject({ status: 'completed', attempt: 2 });
    expect(state.effectOrder.filter((e) => e.status === 'running' || e.status === 'pending')).toEqual([]);
    expect(state.effectOrder.filter((e) => e.name === 'tool.unsafe_send_email')).toHaveLength(1);
    expect((await readLedger(b.world, sessionId)).map((l) => l.callKey)).toEqual([email.key]);
    // The call had its output in the rollout already: nothing was injected for it.
    const callId = email.key.slice('tool:'.length);
    expect(rolloutLines(state).filter((l) => l.includes('"function_call_output"') && l.includes(callId))).toHaveLength(1);
    const entries = transcriptOf(state).map((x) => x.data);
    const note = entries.find((e) => e.type === 'message' && e.role === 'user') as { text: string; synthetic: string };
    expect(note.synthetic).toBe('recovery');
    expect(note.text).toContain(`call ${callId}: completed`);
    expect(entries.at(-2)).toEqual({ type: 'message', role: 'assistant', text: expect.stringContaining('Recovered from the log') });
    expect(entries.at(-1)).toEqual({ type: 'turn', status: 'completed' });
  });

  it('a model step a crash left open after its turn ended: the next drive closes it as abandoned, without booting a VM', async () => {
    const r = await newRig();
    const dataDir = await tmp('hwc-');
    const a = await session(dataDir, r);
    const { sessionId } = await a.sessions.start({ prompt: 'hello' });
    await waitTurns(a.sessions, sessionId, 1);
    await a.world.close?.();
    await tearDown(r, sessionId);

    // A drive that dies in a model step after the turn's end is in the log
    // (a CLI's last request outlived it): a stand-in for the Codex drive,
    // which opens the step and is killed in it.
    const world = createWorld({ dataDir, recoverActiveRuns: false });
    r.worlds.push(world);
    let crashed!: () => void;
    const died = new Promise<void>((res) => (crashed = res));
    const dying = durableSessions({
      world,
      harness: {
        name: 'codex',
        async drive(ctx) {
          if (!ctx.state().submissions.some((s) => s.kind === 'chaos')) return;
          await ctx.effect('model:late', { name: 'model', replay: 'safe', input: { method: 'POST', path: '/v1/responses' } }, () => new Promise<never>(() => {}));
        },
      },
      chaos: true,
      crash: () => crashed(),
    });
    world.registerHandler(QUEUE_PREFIX, dying.handler);
    await world.start?.();
    await dying.chaos(sessionId, { at: 'model', afterMs: 100 });
    await died;
    let state = await dying.inspect(sessionId);
    expect(state.effects.get('model:late')?.status).toBe('running');
    await world.close?.();

    // The next drive: the turn is over, nothing is pending, so no VM is
    // acquired; the step no request will ever retry is closed.
    const created = r.api.calls.filter((c) => c.startsWith('createSandbox')).length;
    const b = await session(dataDir, r, { recover: true });
    state = await until(async () => {
      const s = await b.sessions.inspect(sessionId);
      return s.effects.get('model:late')?.status === 'completed' ? s : undefined;
    });
    expect(state.effects.get('model:late')).toMatchObject({ status: 'completed', output: { requests: 0, abandoned: true } });
    expect(state.effectOrder.filter((e) => e.status === 'running' || e.status === 'pending')).toEqual([]);
    expect(turnsEnded(state)).toBe(1);
    expect(r.api.calls.filter((c) => c.startsWith('createSandbox'))).toHaveLength(created);
  });

  it('a crash with the VM still up: the next driver arms it with a higher fence, the stale app-server is killed, and the thread resumes from the log', async () => {
    const r = await newRig();
    const dataDir = await tmp('hwc-');
    let crashed!: () => void;
    const died = new Promise<void>((res) => (crashed = res));
    const a = await session(dataDir, r, { chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 300 });
    await a.sessions.send(sessionId, 'call unsafe_send_email {"to":"a@b.c","subject":"hi","body":"x"}');
    await died;
    await a.world.close?.();
    const vm = vmOf(r, sessionId)!.sessionId;

    const b = await session(dataDir, r, { chaos: true, recover: true });
    const state = await waitTurns(b.sessions, sessionId, 1);
    // The same VM, armed by the newer owner.
    expect(vmOf(r, sessionId)!.sessionId).toBe(vm);
    expect(state.effectOrder.find((e) => e.name === 'tool.unsafe_send_email')).toMatchObject({ status: 'failed', error: { interrupted: true } });
    expect((await readLedger(b.world, sessionId)).length).toBe(1);
    // CODEX_HOME was rebuilt from the log: the file is the log's lines, nothing the stale process wrote after.
    expect(fileLines(rolloutFile(r, sessionId, state))).toEqual(rolloutLines(state));
  });

  it('a steer during a turn goes into it with its submission id', async () => {
    const r = await newRig();
    // The fake model thinks for a second after a tool returns: the next model boundary, where a steer lands.
    const a = await session(await tmp('hwc-'), r, { env: { FAKE_CODEX_THINK_MS: '1000' } });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'call unsafe_send_email {"to":"a@b.c","subject":"hi","body":"x"}');
    // While the email confirms (3 s), steer.
    await until(async () => ((await a.sessions.inspect(sessionId)).effectOrder.some((e) => e.name === 'tool.unsafe_send_email') ? true : undefined));
    const submissionId = await a.sessions.send(sessionId, 'also cc the team', 'steer');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(state.placed.has(submissionId)).toBe(true);
    expect(summarizeRollout(rolloutLines(state)).clientIds.has(submissionId)).toBe(true);
    expect(transcriptOf(state).at(-2)?.data).toMatchObject({ text: expect.stringContaining('steered: also cc the team') });
    expect(turnsEnded(state)).toBe(1);
  });

  it('a steer Codex refused (its turn had just ended) runs next in the same turn', async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r, { env: { FAKE_CODEX_STEER: 'reject', FAKE_CODEX_THINK_MS: '1500' } });
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'call unsafe_send_email {"to":"a@b.c","subject":"hi","body":"x"}');
    await until(async () => ((await a.sessions.inspect(sessionId)).effectOrder.some((e) => e.name === 'tool.unsafe_send_email') ? true : undefined));
    const steerId = await a.sessions.send(sessionId, 'also cc the team', 'steer');
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(state.placed.has(steerId)).toBe(true);
    const entries = transcriptOf(state).map((x) => x.data);
    // One turn holds the prompt, the email and the refused steer, sent as Codex's next turn.
    expect(entries.filter((e) => e.type === 'turn')).toEqual([{ type: 'turn', status: 'completed' }]);
    expect(summarizeRollout(rolloutLines(state)).clientIds.has(steerId)).toBe(true);
    expect(entries.at(-2)).toMatchObject({ text: 'echo: also cc the team' });
    expect(state.effectOrder.filter((e) => e.name === 'tool.unsafe_send_email')).toHaveLength(1);
  });

  it('a refused journal write halts the drive and kills app-server in the VM', async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r);
    const { sessionId } = await a.sessions.start();
    await a.sessions.send(sessionId, 'hello', 'prompt', { wake: 'backstop' });
    const create = a.world.events.create.bind(a.world.events);
    const refused = new Error('the World refused the write');
    a.world.events.create = (async (runId: string, request: any, params?: any) => {
      if (request?.eventType === 'step_started' && String(request.eventData?.stepName).endsWith('//journal')) throw refused;
      return create(runId, request, params);
    }) as typeof a.world.events.create;
    await expect(a.sessions.drive(sessionId)).rejects.toBe(refused);
    const state = await a.sessions.inspect(sessionId);
    expect(state.effectOrder.some((e) => e.name === 'journal')).toBe(false);
    // The drive closed its machine: the session's processes in the VM were killed, and its link let go.
    expect(r.logs.some((l) => l.message === 'relay link lost' && l.fields?.reason === 'closed by driver')).toBe(true);
  });

  it('a link to the VM lost mid-turn: the drive re-attaches in the same invocation and resumes the turn from the log', async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r, { env: { FAKE_CODEX_THINK_MS: '2000' } });
    const { sessionId } = await a.sessions.start({ prompt: 'hello' });
    // While it thinks, the relay goes (an OOM kill): the link drops, the VM stays.
    await until(async () => (vmOf(r, sessionId) && (await a.sessions.inspect(sessionId)).placed.size > 0 ? true : undefined));
    await new Promise((res) => setTimeout(res, 500));
    await r.api.relayOf(nameOf(sessionId))!.kill();
    const state = await waitTurns(a.sessions, sessionId, 1);
    expect(r.logs.some((l) => l.message.includes('re-attaching'))).toBe(true);
    expect(state.effectOrder.filter((e) => e.name === 'sandbox.attach').length).toBeGreaterThanOrEqual(2);
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });
    expect(turnsEnded(state)).toBe(1);
  });
});

describe('the Codex harness defaults (fake app-server, stand-in sandbox)', () => {
  it("no builtins line: tool-less, Codex is offered only the app's tools, its shell, apply_patch and images off, a read-only sandbox", async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r);
    const { sessionId } = await a.sessions.start({ prompt: 'hello' });
    const state = await waitTurns(a.sessions, sessionId, 1);
    const config = readFileSync(join(cacheRootOf(r, sessionId), 'codex-home', 'config.toml'), 'utf8');
    expect(config).toMatch(/^sandbox_mode = "read-only"$/m);
    const features = `\n${config}`.split('\n[').find((t) => t.startsWith('features]\n'))!;
    for (const feature of ['shell_tool', 'unified_exec', 'view_image', 'image_generation', 'multi_agent', 'plugins']) expect(features).toMatch(new RegExp(`^${feature} = false$`, 'm'));
    expect(features).not.toMatch(/^\w+ = true$/m);
    expect(config).toContain('[features.code_mode]\nenabled = false\ndirect_only_tool_namespaces = ["functions"]');
    expect(config).toMatch(/^web_search = "disabled"$/m);
    // The thread's tools are the app's alone.
    const meta = JSON.parse(rolloutLines(state)[0]!) as { payload: { dynamic_tools: { name: string }[] } };
    expect(meta.payload.dynamic_tools.map((t) => t.name)).toEqual(workspaceTools().map((t) => t.name));
  });

  it('a built-in approval with the built-ins off (a backstop: Codex 0.160 offers no patch tool without an environment) is declined, and nothing is written', async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r);
    const { sessionId } = await a.sessions.start({ prompt: 'patch pwned.txt hello' });
    const state = await waitTurns(a.sessions, sessionId, 1);
    const callId = summarizeRollout(rolloutLines(state)).calls[0]!.callId;
    expect(r.logs).toContainEqual({ message: 'codex: declined a built-in tool', fields: { method: 'item/fileChange/requestApproval', itemId: callId } });
    expect(transcriptOf(state).at(-2)?.data).toMatchObject({ text: expect.stringContaining('apply_patch declined: rejected by user') });
    expect(existsSync(join(driveOf(r, sessionId), 'pwned.txt'))).toBe(false);
    expect(state.effectOrder.filter((e) => e.name.startsWith('tool.'))).toEqual([]);
  });

  it("a tool Codex answers itself (not the app's, not a journaled built-in) is logged once, with its name and call id, never its arguments", async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r);
    const { sessionId } = await a.sessions.start({ prompt: 'call read_file {"path":"missing.txt"}' });
    await waitTurns(a.sessions, sessionId, 1);
    await a.sessions.send(sessionId, 'foreign custom exec');
    const state = await waitTurns(a.sessions, sessionId, 2);
    const calls = summarizeRollout(rolloutLines(state)).calls;
    expect(calls.map((c) => c.name)).toEqual(['read_file', 'exec']);
    const foreign = r.logs.filter((l) => l.message.includes('neither runs nor journals'));
    expect(foreign).toEqual([{ message: 'codex: the model called a tool the harness neither runs nor journals (Codex answered it itself)', fields: { tool: 'exec', callId: calls[1]!.callId } }]);
    // Neither is an effect: Codex answered them, the harness ran nothing.
    expect(state.effectOrder.filter((e) => e.name.startsWith('tool.')).map((e) => e.name)).toEqual(['tool.read_file']);
  });

  it("the provider key is a fresh placeholder per drive, which sandbox({ env }) can't replace", async () => {
    const r = await newRig();
    const a = await session(await tmp('hwc-'), r, { env: { HARNESS_CODEX_GATEWAY_KEY: 'user' } });
    const { sessionId } = await a.sessions.start({ prompt: 'hello' });
    const state = await waitTurns(a.sessions, sessionId, 1);
    // app-server's environment, as the relay ran it (the fake's own process).
    expect(r.model.requests.map((q) => q.text)).toEqual(['echo: hello']);
    expect(transcriptOf(state).at(-2)?.data).toMatchObject({ text: 'echo: hello' });
  });
});

describe('the Codex harness, built-ins on (fake app-server, stand-in sandbox)', () => {
  it("runs Codex's own shell and apply_patch on the Drive: each approved only once its intent is in the log, its outcome when the item completes; the files stay on the Drive", async () => {
    const r = await newRig();
    const dataDir = await tmp('hwc-');
    const a = await session(dataDir, r, { builtins: true });
    const { sessionId } = await a.sessions.start({ prompt: 'shell echo built-in > b.txt && cat b.txt' });
    await waitTurns(a.sessions, sessionId, 1);
    await a.sessions.send(sessionId, 'patch notes.md durable');
    const state = await waitTurns(a.sessions, sessionId, 2);
    const calls = state.effectOrder.filter((e) => e.name.startsWith('tool.'));
    expect(calls.map((e) => [e.name, e.replay, e.status])).toEqual([
      ['tool.shell', 'never', 'completed'],
      ['tool.apply_patch', 'never', 'completed'],
    ]);
    expect(calls[0]).toMatchObject({ key: expect.stringMatching(/^tool:call_/), input: { command: "/bin/bash -lc 'echo built-in > b.txt && cat b.txt'" }, output: 'exit code 0\nbuilt-in' });
    // Codex's record of each call (its rollout line) was in the log before the call's intent.
    for (const c of calls) {
      const id = c.key.slice('tool:'.length);
      const line = journalRecords<string>(state, rolloutStream(threadOf(state)!.threadId)).find((x) => x.data.includes('"function_call"') && x.data.includes(id))!;
      expect(line.at).toBeLessThan(state.effectOrder.indexOf(c));
    }
    // The files are on the Drive, not in the log; a later turn on a new VM sees them.
    expect(readFileSync(join(driveOf(r, sessionId), 'b.txt'), 'utf8')).toBe('built-in\n');
    expect(readFileSync(join(driveOf(r, sessionId), 'notes.md'), 'utf8')).toBe('durable');
    expect(filesFromState(state)).toEqual({});
    // The built-ins are the harness's own: no "a tool the harness neither runs nor journals" line for them.
    expect(r.logs.filter((l) => l.message.includes('neither runs nor journals'))).toEqual([]);
    await a.world.close?.();
    await tearDown(r, sessionId);
    const b = await session(dataDir, r, { builtins: true });
    await b.sessions.send(sessionId, 'shell cat notes.md');
    const later = await waitTurns(b.sessions, sessionId, 3);
    expect(later.effectOrder.filter((e) => e.name === 'tool.shell').at(-1)).toMatchObject({ status: 'completed', output: 'exit code 0\ndurable' });
    // No "your files are a fresh copy" note: the Drive is the same everywhere.
    expect(transcriptOf(later).map((x) => x.data).filter((e) => e.type === 'message' && e.role === 'user')).toEqual([]);
  });

  it('a crash mid built-in command, recovered on the same VM: the command is killed, its call answered interrupted from the log, never rerun', async () => {
    const r = await newRig();
    const dataDir = await tmp('hwc-');
    let crashed!: () => void;
    const died = new Promise<void>((res) => (crashed = res));
    const a = await session(dataDir, r, { builtins: true, chaos: true, crash: () => crashed() });
    const { sessionId } = await a.sessions.start();
    await a.sessions.chaos(sessionId, { at: 'tool', afterMs: 500 });
    await a.sessions.send(sessionId, 'shell echo started >> runs.txt; sleep 4; echo done > out.txt');
    await died;
    await a.world.close?.();

    const b = await session(dataDir, r, { builtins: true, chaos: true, recover: true });
    const state = await waitTurns(b.sessions, sessionId, 1);
    // The newer owner's arm killed the session's processes, the command with them.
    await new Promise((res) => setTimeout(res, 4500));
    const cwd = driveOf(r, sessionId);
    expect(existsSync(join(cwd, 'out.txt'))).toBe(false);
    expect(readFileSync(join(cwd, 'runs.txt'), 'utf8')).toBe('started\n');
    const shell = state.effectOrder.filter((e) => e.name === 'tool.shell');
    expect(shell).toHaveLength(1);
    expect(shell[0]).toMatchObject({ status: 'failed', error: { interrupted: true } });
    // The dangling exec_command got its output from the log; the note says it may have taken effect.
    const id = shell[0]!.key.slice('tool:'.length);
    const outputs = rolloutLines(state).filter((l) => l.includes('"function_call_output"') && l.includes(id));
    expect(outputs).toHaveLength(1);
    expect(outputs[0]).toContain('may or may not have taken effect');
    const note = transcriptOf(state).map((x) => x.data).find((e) => e.type === 'message' && e.role === 'user') as { text: string; synthetic: string };
    expect(note).toMatchObject({ synthetic: 'recovery', text: expect.stringContaining('may have taken effect') });
  });
});

function walk(dir: string, prefix = ''): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).flatMap((name) => {
    const p = join(dir, name);
    return statSync(p).isDirectory() ? walk(p, `${prefix}${name}/`) : [`${prefix}${name}`];
  });
}

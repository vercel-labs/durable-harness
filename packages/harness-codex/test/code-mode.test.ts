import { existsSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { join } from 'node:path';
import { createWorld } from '@workflow/world-local';
import { durableSessions, type HostTool, QUEUE_PREFIX, type SessionState } from 'harness-world';
import { transcriptOf } from 'harness-world-process';
import { sandboxSession } from 'harness-world-sandbox';
import { FakeSandboxApi } from 'harness-world-sandbox/testing/fake-api';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { codexBinary } from '../src/binary.ts';
import { codexHarness } from '../src/harness.ts';
import { parseLine } from '../src/rollout.ts';
import { driveOf, rolloutLines, standInEnv, tmp, waitTurns } from './stand-in.ts';

/**
 * What the pinned `codex app-server` (the real binary) offers the model and
 * how it dispatches the calls, under the harness's real drive on the
 * stand-in sandbox, with a local fake Responses server as the Gateway: no
 * network, no credential. Codex sends a code-mode-only model's tools in an
 * `additional_tools` input item (namespaces), every other model's in the
 * top-level `tools`. The fake answers each request with the next scripted
 * output item, so the model "calls" exactly the tools a test names. The tool
 * lists are pinned exactly: a Codex bump that changes what the model is
 * offered with the built-ins off fails here. Skipped without a binary.
 */

beforeAll(standInEnv);

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c().catch(() => {});
});

/** A model id Codex 0.160 treats as code-mode-only. */
const CODE_MODE_MODEL = 'openai/gpt-6.1-sol';
const PATCH = '*** Begin Patch\n*** Add File: pwned.txt\n+hello\n*** End Patch\n';

/** The app's one tool. `never`: its call runs only once its rollout line is in the log, so a call_id that is not the rollout's would be declined, never run. */
const echo: HostTool<{ x: string }> = {
  name: 'echo',
  description: 'Echo x back.',
  inputSchema: { type: 'object', properties: { x: { type: 'string' } }, required: ['x'], additionalProperties: false },
  replay: 'never',
  execute: async ({ x }) => `echo: ${x}`,
};

type Item = Record<string, unknown>;
const functionCall = (name: string, callId: string, args: object): Item => ({ type: 'function_call', id: `fc_${callId}`, call_id: callId, name, arguments: JSON.stringify(args), status: 'completed' });
const customCall = (name: string, callId: string, input: string): Item => ({ type: 'custom_tool_call', id: `ct_${callId}`, call_id: callId, name, input, status: 'completed' });

/** A Responses server that answers request n with `script[n]` (then an assistant message), as server-sent events. */
async function fakeResponses(script: Item[]): Promise<{ url: string; bodies: Record<string, any>[]; server: Server }> {
  const bodies: Record<string, any>[] = [];
  const server = createServer(async (req, res) => {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    if (req.method !== 'POST' || req.url !== '/v1/responses') {
      res.writeHead(404).end();
      return;
    }
    bodies.push(JSON.parse(raw));
    const n = bodies.length;
    const id = `resp_${n}`;
    const item = script[n - 1] ?? { type: 'message', id: `msg_${n}`, role: 'assistant', status: 'completed', content: [{ type: 'output_text', text: 'done', annotations: [] }] };
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    for (const e of [
      { type: 'response.created', response: { id } },
      { type: 'response.output_item.done', output_index: 0, item },
      { type: 'response.completed', response: { id, output: [item], usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } },
    ]) {
      res.write(`event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`);
    }
    res.end();
  });
  await new Promise<void>((ok) => server.listen(0, '127.0.0.1', ok));
  cleanups.push(() => new Promise<void>((done) => (server.closeAllConnections(), server.close(() => done()))));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, bodies, server };
}

/** One turn of a session on `model`, with the built-ins off (the default), the model making the scripted calls. */
async function turn(model: string, script: Item[]) {
  const gateway = await fakeResponses(script);
  const api = new FakeSandboxApi();
  cleanups.push(() => api.dispose());
  const world = createWorld({ dataDir: await tmp('hwc-cm-'), recoverActiveRuns: false });
  cleanups.push(async () => void (await world.close?.()));
  const logs: { message: string; fields?: Record<string, unknown> }[] = [];
  const sessions = durableSessions({
    world,
    harness: codexHarness({
      tools: [echo],
      model,
      machine: { acquire: sandboxSession({ spec: { vcpus: 2, network: 'allow-all', idleMs: 60_000, driveSize: 10 * 1024 ** 3, budget: { vmSeconds: Infinity }, image: 'stand-in' }, test: { api, allowDegraded: true, allowUnauthenticatedModel: true } }) },
      test: { command: codexBinary()! },
      gatewayUrl: gateway.url,
      credential: async () => 'test-key',
      log: (message, fields) => void logs.push({ message, ...(fields ? { fields } : {}) }),
    }),
  });
  world.registerHandler(QUEUE_PREFIX, sessions.handler);
  await world.start?.();
  const { sessionId } = await sessions.start({ prompt: 'Make the calls.' });
  const state = await waitTurns(sessions, sessionId, 1, 60_000);
  return { state, bodies: gateway.bodies, logs, drive: driveOf({ api } as never, sessionId) };
}

/** The rollout's call and output lines, by call id. */
function rolloutCalls(state: SessionState) {
  const calls = new Map<string, { type: string; name: string }>();
  const outputs = new Map<string, string>();
  for (const raw of rolloutLines(state)) {
    const p = parseLine(raw)?.payload;
    if (!p || typeof p.call_id !== 'string') continue;
    if (p.type === 'function_call' || p.type === 'custom_tool_call') calls.set(p.call_id, { type: p.type, name: String(p.name) });
    else if (p.type === 'function_call_output' || p.type === 'custom_tool_call_output') outputs.set(p.call_id, typeof p.output === 'string' ? p.output : JSON.stringify(p.output));
  }
  return { calls, outputs };
}

const names = (tools: { name?: string }[] | undefined) => (tools ?? []).map((t) => t.name);
const FOREIGN = 'codex: the model called a tool the harness neither runs nor journals (Codex answered it itself)';

/** The app's tool, called once: one effect keyed `tool:<item/tool/call callId>`, the same id as the rollout's function_call and function_call_output. */
function expectEchoDirect(state: SessionState, callId: string) {
  const effects = state.effectOrder.filter((e) => e.name.startsWith('tool.'));
  // The harness keys the effect by item/tool/call's callId (`toolBridge`'s `tool:<callId>`): it is the rollout's call_id.
  expect(effects).toEqual([expect.objectContaining({ name: 'tool.echo', key: `tool:${callId}`, status: 'completed', output: 'echo: hi' })]);
  const { calls, outputs } = rolloutCalls(state);
  expect(calls.get(callId)).toEqual({ type: 'function_call', name: 'echo' });
  expect(outputs.get(callId)).toBe('echo: hi');
}

describe.skipIf(!codexBinary())("Codex 0.160's tool surface with the built-ins off (real binary, fake Responses server, stand-in sandbox)", () => {
  it(`a code-mode-only model (${CODE_MODE_MODEL}): the app's tool is offered and called directly; apply_patch is not offered, and exec, the sub-agent tools and a stray apply_patch fail inside Codex, each logged`, async () => {
    const { state, bodies, logs, drive } = await turn(CODE_MODE_MODEL, [
      functionCall('echo', 'call_echo', { x: 'hi' }),
      customCall('apply_patch', 'call_patch', PATCH),
      customCall('exec', 'call_exec', 'text(await tools.echo({ x: "via-exec" }))'),
      functionCall('spawn_agent', 'call_spawn', { message: 'hi', task_name: 'x' }),
    ]);
    expect(transcriptOf(state).at(-1)?.data).toEqual({ type: 'turn', status: 'completed' });
    // Offered in an `additional_tools` item, not the top-level tools: the app's `echo` a plain function in `functions`, next to what Codex adds itself.
    expect(bodies[0]!.tools ?? []).toEqual([]);
    const offered = (bodies[0]!.input as { type: string; tools?: { type: string; name: string; tools?: { type: string; name: string }[] }[] }[]).find((i) => i.type === 'additional_tools');
    expect(offered?.tools?.map((ns) => [ns.type, ns.name, names(ns.tools)])).toEqual([
      // No `apply_patch`: with the built-ins off each turn starts with no environment (`environments: []`), and Codex offers no patch tool.
      ['namespace', 'functions', ['exec', 'wait', 'request_user_input', 'request_user_input_async', 'echo']],
      ['namespace', 'collaboration', ['followup_task', 'interrupt_agent', 'list_agents', 'send_message', 'spawn_agent', 'wait_agent']],
    ]);
    expect(offered!.tools![0]!.tools!.find((t) => t.name === 'echo')).toMatchObject({ type: 'function', name: 'echo' });

    expectEchoDirect(state, 'call_echo');
    const { outputs } = rolloutCalls(state);
    // A model that calls apply_patch anyway: Codex refuses it itself, with no environment to patch, and never asks the drive (the drive's
    // "declined a built-in tool" for an item/fileChange/requestApproval with the built-ins off is a backstop this Codex does not reach:
    // harness.test.ts covers it with the fake app-server). Nothing is written.
    expect(outputs.get('call_patch')).toContain('unsupported custom tool call');
    expect(logs.filter((l) => l.message === 'codex: declined a built-in tool')).toEqual([]);
    expect(existsSync(join(drive, 'pwned.txt'))).toBe(false);
    // exec and spawn_agent never reach the drive either: Codex answers them itself.
    expect(outputs.get('call_exec')).toContain('code-mode host is disabled');
    expect(outputs.get('call_spawn')).toContain('unsupported call');
    // Each of the three is one log line, with its name and call id.
    expect(logs.filter((l) => l.message === FOREIGN).map((l) => l.fields)).toEqual([
      { tool: 'apply_patch', callId: 'call_patch' },
      { tool: 'exec', callId: 'call_exec' },
      { tool: 'spawn_agent', callId: 'call_spawn' },
    ]);
    expect(state.effectOrder.filter((e) => e.name.startsWith('tool.')).map((e) => e.name)).toEqual(['tool.echo']);
  }, 90_000);

  it("openai/gpt-5.4-mini: the app's tool is in the top-level tools, called directly", async () => {
    const { state, bodies, logs } = await turn('openai/gpt-5.4-mini', [functionCall('echo', 'call_echo', { x: 'hi' })]);
    expect(names(bodies[0]!.tools)).toEqual(['request_user_input', 'echo']);
    expect(bodies[0]!.tools.find((t: { name: string }) => t.name === 'echo')).toMatchObject({ type: 'function', name: 'echo' });
    expect((bodies[0]!.input as { type: string }[]).some((i) => i.type === 'additional_tools')).toBe(false);
    expectEchoDirect(state, 'call_echo');
    expect(logs.filter((l) => l.message === FOREIGN)).toEqual([]);
  }, 90_000);
});

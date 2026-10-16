import { AsyncLocalStorage } from 'node:async_hooks';
import { fxPromptTraceIdentity, fxTraceIdentity, fxTraceRequestBody } from './fx-trace';
import { agentRunsHook, type SessionTracing } from 'harness-world/agent-runs';
import { gatewayModelOptions, gatewayModelResult, ResponseCapture } from 'harness-world-process';
import type { World } from '@workflow/world';
import type { ChaosSpec } from 'harness-world-demo-ui';
import { fxTools } from 'durable-harness/fx';
import { virtual } from 'durable-harness/tools';
import { type FxChaosLine, type FxWorkspace, LIBFX_WORKFLOW } from 'harness-world-demo-ui/fx-view';
import { readFxWorkspace } from 'harness-world-hands/fx';
import { TOOL_SPECS, UNSAFE_CONFIRM_MS, type UnsafeToolName, unsafeResultText } from 'harness-world-hands/specs';
import { PROCESS_ID } from 'harness-world/marks';
import { readJsonLines, tailJsonLines } from 'harness-world/streams';
import { createFxAgent, type FxAgentEvent, type FxToolContext } from 'libfx';
import { fxDurability, fxDurableDisabledReason } from './fx-durable-world';
import { INSTRUCTIONS, MODEL } from './model';
import { getWorld } from './world';

export { refuseDisabled } from './fx-durable-world';

/**
 * "fx (libfx durable)": libfx's own durable sessions, as the libfx
 * durability doc (vercel-labs/fx#1180) has an app write them. One agent per
 * process, with this demo's model, prompt and workspace tools; libfx keeps
 * each session in this app's World (see lib/fx-durable-world.ts), runs its
 * turns in the wake route (`agent.wakeHandler()`; in-process on world-local),
 * and serves its stream from any process. Nothing here runs a turn, leases
 * a session or replays a log: the routes call `session.prompt`,
 * `session.steer` and `session.stream`, and the rest is libfx.
 *
 * What the app adds, beside libfx:
 * - The workspace: libfx's own tools from durable-harness/fx
 *   (`fxTools(virtual())`: read_file, glob_files, grep_files, edit_file,
 *   write_file and shell, with fx's names and schemas, on a virtual
 *   workspace). A call that can change it records what it changed under
 *   the call's `executionId`, in a stream of the session's run. A call that
 *   libfx runs again after a crash (the tools are idempotent) finds its
 *   record and returns what it already did, so it never applies twice; a
 *   call that crashed before recording reruns on the files as they were
 *   before it.
 * - Timing marks, the same `model.*` and `tool.*` marks the other lanes write,
 *   so /compare can take the model's own time out of each turn. A model
 *   request reaches the agent's `fetch` with the session's id in its
 *   `x-session-id` header; a tool gets it as libfx's `sessionId`.
 * - Crashes, armed from the page, fired during a model request or a tool
 *   call (found the same two ways).
 */

export const FX_DURABLE = 'fx-durable';
export const chaosStreamName = (id: string) => `${id}_fxchaos`;
export const workspaceStreamName = (id: string) => `${id}_fxws`;
export const markStreamName = (id: string) => `${id}_fxmarks`;

// Function log lines carry no time of their own, so each one says when it happened.
export const log = (event: string, detail: Record<string, unknown>) => console.log(`fx-durable ${event} ${JSON.stringify({ at: new Date().toISOString(), ...detail })}`);

/** What libfx reports that is worth a log line: why a worker stopped, or what it could not do. */
const LOGGED = new Set(['journal.open', 'checkpoint.save', 'checkpoint.error', 'checkpoint.mismatch', 'session.deadline', 'session.fenced', 'session.error']);

type FxAgent = ReturnType<typeof createFxAgent>;
type Turn = NonNullable<Awaited<ReturnType<SessionTracing['turn']>>>;
type Step = Awaited<ReturnType<SessionTracing['step']>>;
/** A libfx turn in Agent Runs: its operation, and the step of its latest model request (the tool calls it asks for go under it). */
type FxTraceState = { session: SessionTracing; turn: Turn; turnId: string; step?: Step; outcome?: 'running' | 'failed' };
type FxTraceInvocation = { traces: Map<string, FxTraceState>; turns: Map<string, { turnId: string; prompt?: string }> };
const fxTraceInvocations = new AsyncLocalStorage<FxTraceInvocation>();

/**
 * Agent Runs for libfx's own durable sessions: a turn per libfx turn, a step
 * per model request, the tool calls under it. A turn that goes on in a later
 * wake delivery continues from its checkpoint; one delivery ends only the
 * turns it worked on.
 */
export async function withFxTraces<T>(run: () => Promise<T>): Promise<T> {
  return fxTraceInvocations.run({ traces: new Map(), turns: new Map() }, async () => {
    let error: unknown;
    try { return await run(); } catch (caught) { error = caught; throw caught; }
    finally {
      for (const state of fxTraceInvocations.getStore()!.traces.values()) await endFxTrace(state, error).catch(() => {});
    }
  });
}
async function endFxTrace(state: FxTraceState, error?: unknown): Promise<void> {
  const outcome = state.outcome ?? (error === undefined ? 'completed' : 'failed');
  await state.step?.complete();
  if (outcome === 'failed') await state.turn.complete({ outcome, failed: true, error: error ?? new Error('the libfx session failed') });
  else if (outcome === 'completed') await state.turn.complete({ outcome });
  await state.session.flush();
}
async function fxTrace(id: string, key: string, refreshTurn = false): Promise<FxTraceState | undefined> {
  const invocation = fxTraceInvocations.getStore();
  const hook = agentRunsHook();
  if (!invocation || !hook) return undefined;
  const { traces, turns } = invocation;
  const existing = traces.get(id);
  if (existing && !refreshTurn) return existing;
  // libfx's accepted journal marks preserve the same turn id across resumes.
  const current = turns.get(id);
  const identity = current ?? fxTraceIdentity((await (await getWorld()).events.list({
    runId: id, pagination: { sortOrder: 'desc', limit: 100 }, resolveData: 'all',
  })).data, existing?.turnId ?? key);
  const { turnId, prompt } = identity;
  if (existing?.turnId === turnId) return existing;
  if (existing) await endFxTrace(existing);
  const session = hook.session({ world: await getWorld(), runId: id, agentName: 'fx-durable' });
  const turn = await session.turn({ turnId, sequence: 0, ...(typeof prompt === 'string' ? { title: prompt } : {}) });
  if (!turn) return undefined;
  const state: FxTraceState = { session, turn, turnId };
  traces.set(id, state);
  return state;
}
const g = globalThis as { __fxDurableAgent?: Promise<FxAgent> };

/** The process's agent. Every process creates the same one, so any of them can run any session. */
export function fxAgent(): Promise<FxAgent> {
  g.__fxDurableAgent ??= (async () => {
    const reason = fxDurableDisabledReason();
    if (reason) throw new Error(reason);
    const durability = await fxDurability();
    return createFxAgent({
      ...(durability ? { durability } : {}),
      model: MODEL,
      instructions: INSTRUCTIONS,
      tools: await workspaceTools(),
      fetch: timedFetch,
      onEvent(event: FxAgentEvent) {
        if (event.type === 'acp.send') {
          const identity = fxPromptTraceIdentity(event.message);
          if (identity) fxTraceInvocations.getStore()?.turns.set(identity.sessionId, identity);
        }
        if ('sessionId' in event && event.sessionId) {
          const state = fxTraceInvocations.getStore()?.traces.get(event.sessionId);
          if (state && (event.type === 'session.deadline' || event.type === 'session.fenced')) state.outcome = 'running';
          if (state && event.type === 'session.error') state.outcome = 'failed';
        }
        if (!LOGGED.has(event.type)) return;
        const { type, timestamp: _timestamp, ...detail } = event;
        log(type, detail);
      },
    });
  })();
  return g.__fxDurableAgent;
}

/** A session id libfx's World accepts, chosen here so the page knows it before the first prompt. */
export async function newSessionId(): Promise<string> {
  return (await fxAgent()).newSessionId();
}

/** A session id libfx takes on a World: `wrun_` and 10 to 64 letters or digits (its README). */
const SESSION_ID = /^wrun_[0-9A-Za-z]{10,64}$/;
/** Runs this process has read and found to be libfx sessions (see `refuseSession`). */
const knownSessions = new Set<string>();

/**
 * Why a route must not touch the session `id`, as the response to send, or
 * undefined when it may. libfx appends its log entries to whatever run the
 * id names, so an id of another kind of run in this World (a harness-world
 * or pi-durable session) is refused before a prompt, steer, cancel or crash
 * reaches it. A run that does not exist yet is fine: libfx creates it with
 * its first prompt. One `runs.get`, until this process has seen the run is a
 * libfx session: a run's workflow never changes, so later prompts skip the
 * read (it sits in front of every turn, on the time /compare measures).
 */
export async function refuseSession(id: string): Promise<Response | undefined> {
  if (!SESSION_ID.test(id)) return Response.json({ error: `not a session id: ${id}` }, { status: 400 });
  if (knownSessions.has(id)) return undefined;
  try {
    const run = await (await getWorld()).runs.get(id, { resolveData: 'none' });
    if (run.workflowName === LIBFX_WORKFLOW) {
      if (knownSessions.size >= 10_000) knownSessions.clear();
      knownSessions.add(id);
      return undefined;
    }
    return Response.json({ error: `${id} is not an fx (libfx durable) session (it is a ${run.workflowName} run)` }, { status: 404 });
  } catch (error) {
    if ((error as { name?: unknown } | undefined)?.name === 'WorkflowRunNotFoundError') return undefined;
    throw error;
  }
}

/** The session, for one request. Its tools find their workspace and crash stream by libfx's `sessionId`. */
export async function fxSession(id: string) {
  return (await fxAgent()).session(id);
}

const encoder = new TextEncoder();
async function writeLine(world: World, id: string, name: string, line: unknown): Promise<void> {
  await world.streams.write(id, name, encoder.encode(`${JSON.stringify(line)}\n`));
}

export async function chaosLines(id: string): Promise<FxChaosLine[]> {
  return (await readJsonLines(await getWorld(), id, chaosStreamName(id))) as FxChaosLine[];
}

/** The workspace as the page reads it: the files the records leave, on the newest record, and the count. */
export async function workspaceSummary(id: string): Promise<FxWorkspace> {
  const { files, records } = await readFxWorkspace(await getWorld(), id);
  const last = records.at(-1);
  if (!last) return { count: 0 };
  const text = Object.fromEntries([...files].map(([path, f]) => [path, typeof f.content === 'string' ? f.content : `(binary file: ${f.content.length} bytes)`]));
  return { latest: { executionId: last.executionId, tool: last.tool, output: last.output, files: text, at: last.at }, count: records.length };
}

/**
 * Whether crashes fire on this server. `HARNESS_CHAOS=0` turns them off, as
 * for the kernel lanes (lib/sessions.ts): nothing is armed and nothing exits.
 */
export const chaosOn = () => process.env.HARNESS_CHAOS !== '0';

/** Arm a crash: the next tool call the session runs long enough fires it. False (and nothing armed) when crashes are off. */
export async function arm(id: string, spec: ChaosSpec): Promise<boolean> {
  if (!chaosOn()) return false;
  await writeLine(await getWorld(), id, chaosStreamName(id), { t: 'armed', at: Date.now(), spec } satisfies FxChaosLine);
  return true;
}

// ---------- timing marks ----------

const writes = new Map<string, Promise<void>>();

/**
 * Write a timing mark to the session's mark stream, in order and without
 * waiting: a mark never holds up the step it times. `t` is this process's
 * clock and `pid` names the process, as in harness-world's marks.
 */
function mark(id: string, name: 'model.request' | 'model.first_token' | 'model.done' | 'tool.start' | 'tool.end', detail: { key: string; effect: string }) {
  const line = { type: 'mark', name, t: performance.timeOrigin + performance.now(), pid: PROCESS_ID, runId: id, ...detail };
  const next = (writes.get(id) ?? Promise.resolve())
    .then(async () => (await getWorld()).streams.write(id, markStreamName(id), encoder.encode(`${JSON.stringify(line)}\n\n`)))
    .catch(() => {});
  writes.set(id, next);
  void next.then(() => {
    if (writes.get(id) === next) writes.delete(id);
  });
}

/** The session's timing marks as they are written, until `signal` aborts. */
export function marks(id: string, signal: AbortSignal): AsyncGenerator<unknown> {
  return (async function* () {
    yield* tailJsonLines(await getWorld(), id, markStreamName(id), { signal });
  })();
}

/** The first model output in a Gateway stream: text, reasoning or a tool call (as harness-fx reads it). */
const hasOutput = (sse: string) => /"type":\s*"(text-delta|reasoning-delta|tool-input-start|tool-input-delta|tool-call)"/.test(sse);

/**
 * The agent's `fetch`: every model request, timed, and a crash armed for a
 * model step fired while it is in flight. libfx sends each request with the
 * session's id in `x-session-id`; other requests pass straight through.
 */
async function timedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
  const id = headers.get('x-session-id');
  const method = (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
  if (!id || method === 'GET') return fetch(input, init);
  const key = `model:${crypto.randomUUID().slice(0, 12)}`;
  let requestBody: unknown;
  try { requestBody = fxTraceRequestBody(init?.body ?? (input instanceof Request ? await input.clone().text() : undefined)); } catch {}
  const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
  const modelTrace = await (async () => {
    const state = await fxTrace(id, key, true);
    if (!state) return undefined;
    await state.step?.complete();
    const step = (state.step = await state.session.step(state.turn, state.turnId));
    return { step, model: await step.modelCall(gatewayModelOptions({ path, model: headers.get('ai-language-model-id') ?? MODEL, body: requestBody })) };
  })().catch(() => undefined);
  const traceBody = new ResponseCapture();
  const traceDecoder = new TextDecoder();
  mark(id, 'model.request', { key, effect: 'model' });
  // Checked beside the request, so it adds nothing to the model's time.
  const armed = crashDuring(id, key, 'model', 'model');
  let ended = false;
  let response: Response | undefined;
  const end = (error?: unknown) => {
    if (ended) return;
    ended = true;
    const status = response?.status;
    const failed = error ?? (response && !response.ok ? new Error(`Model request failed (${status})`) : undefined);
    if (modelTrace && failed !== undefined) void modelTrace.model.fail(failed).catch(() => {});
    else if (modelTrace) {
      const { result, attributes } = gatewayModelResult(path, traceBody.text());
      modelTrace.step.attributes(attributes);
      void modelTrace.model.complete({ outcome: 'completed', result }).catch(() => {});
    }
    void armed.then((disarm) => disarm());
    mark(id, 'model.done', { key, effect: 'model' });
  };
  try {
    response = await fetch(input, init);
  } catch (error) {
    end(error);
    throw error;
  }
  if (!response.body) {
    end();
    return response;
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let seen = '';
  let first = false;
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { value, done } = await reader.read();
        if (done) {
          traceBody.push(traceDecoder.decode());
          end();
          controller.close();
          return;
        }
        if (!first) {
          seen = (seen + decoder.decode(value, { stream: true })).slice(-16_384);
          if (hasOutput(seen)) {
            first = true;
            mark(id, 'model.first_token', { key, effect: 'model' });
          }
        }
        traceBody.push(traceDecoder.decode(value, { stream: true }));
        controller.enqueue(value);
      } catch (error) {
        end(error);
        controller.error(error);
      }
    },
    cancel(reason) {
      end();
      return reader.cancel(reason);
    },
  });
  return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
}

// ---------- tools ----------

/**
 * If a crash is armed for this kind of step (`tool`, `model`, or `any`),
 * kill this process once the step has run for the crash's delay. On Vercel
 * the Function exits, exactly a crashed invocation; elsewhere (a local dev
 * server, which exiting would stop) it only logs. Returns what disarms the
 * timer when the step ends first.
 */
async function crashDuring(id: string, effectKey: string, effectName: string, step: 'tool' | 'model'): Promise<() => void> {
  // Off: not even a crash armed before the server started with HARNESS_CHAOS=0 fires.
  if (!chaosOn()) return () => {};
  const last = (await chaosLines(id).catch(() => [])).at(-1);
  if (last?.t !== 'armed' || (last.spec.at !== step && last.spec.at !== 'any')) return () => {};
  const spec = last.spec;
  const timer = setTimeout(async () => {
    await writeLine(await getWorld(), id, chaosStreamName(id), { t: 'fired', at: Date.now(), spec, effectKey, effectName } satisfies FxChaosLine).catch(() => {});
    log('crash', { id, step: effectName, key: effectKey, vercel: !!process.env.VERCEL });
    if (process.env.VERCEL) process.exit(1);
  }, spec.afterMs ?? 1500);
  return () => clearTimeout(timer);
}

/** Run one tool call with its timing marks and an armed crash. */
async function toolStep<T>(id: string, ctx: FxToolContext, name: string, input: unknown, run: () => Promise<T>): Promise<T> {
  const key = `tool:${ctx.executionId}`;
  const effect = `tool.${name}`;
  const toolTrace = await (async () => {
    const state = await fxTrace(id, key);
    if (!state) return undefined;
    // `executionId` is the model's id for the call: Agent Runs joins it with the call in the model's reply.
    return state.turn.findTool(ctx.executionId) ?? (await (state.step ??= await state.session.step(state.turn, state.turnId)).tool({ callId: ctx.executionId, name, arguments: input }));
  })().catch(() => undefined);
  mark(id, 'tool.start', { key, effect });
  const disarm = await crashDuring(id, key, effect, 'tool');
  try {
    const result = await run();
    await toolTrace?.complete({ outcome: 'completed', output: result }).catch(() => {});
    return result;
  } catch (error) { await toolTrace?.fail(error).catch(() => {}); throw error; } finally {
    disarm();
    mark(id, 'tool.end', { key, effect });
  }
}

type Spec = keyof typeof TOOL_SPECS;
const descriptor = (name: Spec) => ({ name, description: TOOL_SPECS[name].description, inputSchema: TOOL_SPECS[name].inputSchema });

/**
 * libfx's own tools (durable-harness/fx), each call with this lane's timing
 * marks, Agent Runs span and armed crash, and the two unsafe example tools
 * (not idempotent: never run twice).
 */
export async function workspaceTools() {
  const world = await getWorld();
  const traced = fxTools(virtual(), { world }).map((tool) => ({
    ...tool,
    execute: (input: unknown, ctx: FxToolContext) => toolStep(ctx.sessionId, ctx, tool.name, input, async () => tool.execute(input, ctx)),
  }));
  const unsafe = (name: UnsafeToolName) => ({
    ...descriptor(name),
    idempotent: false,
    writes: true,
    async execute(input: Record<string, unknown>, ctx: FxToolContext) {
      return toolStep(ctx.sessionId, ctx, name, input, async () => {
        const ref = `${name === 'unsafe_send_email' ? 'msg' : 'pay'}_${ctx.executionId.slice(-10)}`;
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, UNSAFE_CONFIRM_MS);
          ctx.signal?.addEventListener('abort', () => (clearTimeout(timer), resolve()), { once: true });
        });
        return unsafeResultText(name, input, ref);
      });
    },
  });
  return [
    ...traced,
    unsafe('unsafe_send_email'),
    unsafe('unsafe_make_payment'),
  ];
}

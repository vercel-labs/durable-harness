import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { type DriveContext, type Harness, type HostTool, type Submission, Unavailable } from 'harness-world';
import type { GatewayHandler, SessionMachine } from '../src/environment.ts';
import { type GatewayCap, GatewayError, type GatewayLedger, type GatewayProxy, gatewayHandler, gatewayProxy, gatewayToken, type GatewayRoute } from '../src/gateway.ts';
import { NativeJournal } from '../src/journal.ts';
import { ModelSteps, settleAbandonedModelSteps } from '../src/model.ts';
import { ProcessHost } from '../src/host.ts';
import { placeSubmissions } from '../src/inbox.ts';
import { recoveryNote } from '../src/recovery.ts';
import { FileTail } from '../src/tail.ts';
import { toolBridge } from '../src/tools.ts';

/**
 * The reference process harness: a kernel `Harness` that drives a child CLI
 * (`fake-cli.mjs`) the way the Claude Code, Codex, OpenCode and Cursor ports
 * drive theirs, built only from this package's pieces. It is the worked
 * example the README points at, and what the end-to-end tests run.
 *
 * Every drive:
 *
 * 1. rebuilds the CLI's native session file from the journal (the log wins;
 *    a stale child is killed first so it cannot write over the rebuild);
 * 2. spawns the CLI (own process group, explicit env, killed when the drive
 *    stops) and mirrors every line it appends into the journal;
 * 3. if the last turn never ended, opens with the recovery note;
 * 4. places each submission, then sends it; serves the CLI's tool calls
 *    through the tool bridge (a `never` tool waits for the journal first);
 * 5. writes the normalized transcript (assistant text, tool calls, turn ends)
 *    and live events (`text`, `tool_start`, `tool_end`, `idle`).
 *
 * With `modelUrl`, the CLI's "model" is remote, like a real CLI's: its
 * requests go through a gateway proxy, whose `track` hook gives the drive
 * its model steps (`ModelSteps`), so a "Crash during: model" drill fires.
 *
 * With `machine`, the CLI runs on a session machine instead of this one (the
 * sandbox tier, docs/workspaces.md §5): the session file is uploaded into the
 * machine's cache dir (`files.wipe` + `files.upload`, the claim being the
 * relay's fence), mirrored back through `tail` (synced before a tool call
 * and a turn's end, so the journal has the call before the tool runs), and
 * its model requests reach `modelUrl` through the machine's model port and
 * a `gatewayHandler` with the reference's fixed table (`REFERENCE_ROUTES`).
 */

/** The fake CLI script. */
export const FAKE_CLI = fileURLToPath(new URL('./fake-cli.mjs', import.meta.url));

export interface ReferenceHarnessOptions {
  /** The CLI's only tools. */
  tools: HostTool[];
  /** Where session directories go. Default `new ProcessHost()`. */
  host?: ProcessHost;
  /** The kernel name (its sessions' runs are `workflow//durable-harness@<version>//<name>`). Default `reference`. */
  name?: string;
  /** Extra environment for the child. */
  env?: Record<string, string>;
  /**
   * A model the CLI streams its replies from (tests start `fakeModel()`),
   * through the drive's gateway proxy. Default: the CLI types them itself.
   */
  modelUrl?: string;
  /**
   * Run the CLI on a session machine (the sandbox tier): given the drive and
   * the handler for its model calls, the drive's machine (for the kernel,
   * `attachInDrive` with the session factory's acquire).
   */
  machine?: (ctx: DriveContext, model: GatewayHandler | undefined) => Promise<SessionMachine>;
  /**
   * With `machine`: the spend caps and ledger its gateway handler checks
   * (default no caps, an in-memory ledger). A model call refused for a spend
   * cap ends the turn at once, saying which (an `Unavailable`: no
   * redelivery spends more).
   */
  budget?: { turnUsd: number; sessionUsd: number };
  ledger?: GatewayLedger;
}

/** The reference CLI's fixed table: one model route, a body of `{ text }` only. */
export const REFERENCE_ROUTES: readonly GatewayRoute[] = Object.freeze([
  {
    method: 'POST',
    path: '/v1/model',
    body: (body) => {
      if (Object.keys(body).some((k) => k !== 'text') || typeof body.text !== 'string') throw new GatewayError('bad_request', 'the reference model takes { text }');
      return { text: body.text };
    },
  },
]);

/** The native stream: one record per line of the CLI's session file. */
const NATIVE = 'native';

type CliEvent =
  | { type: 'ready'; records: number }
  | { type: 'text'; delta: string }
  | { type: 'tool_call'; id: string; name: string; args: unknown }
  | { type: 'turn_end' };

export function referenceHarness(options: ReferenceHarnessOptions): Harness {
  const host = options.host ?? new ProcessHost();
  return {
    name: options.name ?? 'reference',
    // Each model request reaches Agent Runs from the Gateway proxy (`ModelSteps.request`).
    modelCalls: 'self',
    async drive(ctx: DriveContext) {
      // Nothing to do (an idle wake, a chaos arming): no child. A model step a
      // crash left open after its turn ended is closed (no request will retry it).
      if (!recoveryNote(ctx.state()) && ctx.pending().length === 0) {
        await settleAbandonedModelSteps(ctx);
        return;
      }
      if (options.machine) return driveOnMachine(ctx, options, options.machine);
      const journal = new NativeJournal(ctx);
      const dir = await host.dir(ctx.runId);

      // 1. The session file is a cache of the log: rebuilt from the journal
      //    in `prepare`, once a child a dead drive left running is killed (it
      //    cannot write over the rebuild), unless it already matches.
      // 2. The child, mirrored into the journal line by line.
      const native = (await journal.read<string>(NATIVE)).map((r) => r.data);
      const text = native.map((l) => `${l}\n`).join('');
      const file = join(dir.path, 'session.jsonl');
      const tail = new FileTail(file, Buffer.byteLength(text));
      const mirror = () => {
        for (const line of tail.read()) if (line) journal.append(NATIVE, line);
      };
      // The model traffic goes through the proxy, which tells the drive when
      // each request starts and ends: its model steps.
      const steps = new ModelSteps(ctx);
      let proxy: GatewayProxy | undefined;
      // The child's key: a fresh one per drive, the only one the proxy forwards.
      const key = gatewayToken();
      if (options.modelUrl) proxy = await gatewayProxy({ target: options.modelUrl, credential: async () => 'reference', token: key, track: steps.request, captureRequest: ctx.tracing !== undefined });
      const env = { ...options.env, ...(proxy ? { FAKE_CLI_MODEL_URL: `${proxy.url}/v1/model`, FAKE_CLI_MODEL_KEY: key } : {}) };
      const proc = await host
        .spawn(ctx.runId, {
          command: process.execPath,
          args: [FAKE_CLI, file],
          env,
          signal: ctx.signal,
          prepare: () => {
            if (!existsSync(file) || readFileSync(file, 'utf8') !== text) writeFileSync(file, text);
          },
        })
        .catch(async (err) => {
          await proxy?.close();
          throw err;
        });
      const events = proc.lines()[Symbol.asyncIterator]();
      const next = async (): Promise<CliEvent> => {
        const r = await events.next();
        if (r.done) {
          if (ctx.signal.aborted) throw ctx.signal.reason;
          const exit = await proc.exited;
          throw new Error(`the CLI exited (${exit.signal ?? exit.code}) mid-turn: ${proc.stderrTail().slice(-500)}`);
        }
        return JSON.parse(r.value) as CliEvent;
      };
      const bridge = toolBridge(ctx, options.tools, { beforeNever: () => journal.flush() });

      const send = async (text: string, placed: Submission[], synthetic?: string) => {
        if (synthetic) journal.message({ type: 'message', role: 'user', text, synthetic });
        // Place before send: a withdraw that wins never reaches the CLI.
        await placeSubmissions(ctx, placed);
        proc.send({ type: 'user', id: placed[0]?.id ?? synthetic, text });
      };
      const runTurn = async () => {
        let said = '';
        for (;;) {
          const ev = await next();
          mirror();
          if (ev.type === 'text') {
            said += ev.delta;
            ctx.live({ type: 'text', delta: ev.delta });
          } else if (ev.type === 'tool_call') {
            journal.message({ type: 'message', role: 'assistant', text: said, toolCalls: [{ id: ev.id, name: ev.name }] });
            said = '';
            const result = await bridge.call(ev.name, ev.args, ev.id);
            proc.send({ type: 'tool_result', id: ev.id, text: result.text, isError: result.isError });
          } else if (ev.type === 'turn_end') {
            if (said) journal.message({ type: 'message', role: 'assistant', text: said });
            journal.message({ type: 'turn', status: 'completed' });
            await journal.flush();
            ctx.live({ type: 'idle' });
            return;
          }
        }
      };

      try {
        const ready = await next();
        if (ready.type !== 'ready') throw new Error(`the CLI said ${ready.type} before ready`);
        // Lines the CLI wrote on load (its own resume repairs) are native records too.
        mirror();
        // 3. A turn a crash cut off: resume it with what the log knows.
        const note = recoveryNote(ctx.state());
        if (note) {
          await send(note.text, note.pending, 'recovery');
          await runTurn();
        }
        // 4. The inbox, one submission per turn (this CLI does not steer).
        for (;;) {
          await ctx.refresh();
          const [s] = ctx.pending();
          if (!s) break;
          await send(s.text ?? '', [s]);
          await runTurn();
        }
        // A model step a crash left open that no request of this drive made again: closed.
        await steps.settleAbandoned();
      } finally {
        proc.end();
        await Promise.race([proc.exited, new Promise((r) => setTimeout(r, 1000))]);
        if (proc.alive()) proc.kill();
        // No model step outlives the drive.
        await steps.close();
        await proxy?.close();
      }
    },
  };
}

/** An in-memory ledger: spend per drive only (a real harness keeps it in the log). */
function memoryLedger(): GatewayLedger {
  let usd = 0;
  return { spent: () => ({ turnUsd: usd, sessionUsd: usd }), add: (cost) => void (usd += cost) };
}

/** The reference drive with its CLI on a session machine (see `ReferenceHarnessOptions.machine`). */
async function driveOnMachine(ctx: DriveContext, options: ReferenceHarnessOptions, acquire: NonNullable<ReferenceHarnessOptions['machine']>): Promise<void> {
  const journal = new NativeJournal(ctx);
  const native = (await journal.read<string>(NATIVE)).map((r) => r.data);
  const text = native.map((l) => `${l}\n`).join('');
  const steps = new ModelSteps(ctx);
  /** The spend cap a model call of this drive was refused for, if one was. */
  let capped: Exclude<GatewayCap, 'concurrency'> | undefined;
  const model = options.modelUrl
    ? gatewayHandler({
        routes: REFERENCE_ROUTES,
        models: { model: 'reference' },
        budget: options.budget ?? { turnUsd: Infinity, sessionUsd: Infinity },
        ledger: options.ledger ?? memoryLedger(),
        target: options.modelUrl,
        credential: async () => 'reference',
        auth: (c) => ({ authorization: `Bearer ${c}` }),
        track: steps.request,
        onCap: (cap) => {
          if (cap !== 'concurrency') capped = cap;
        },
        log: () => {},
      })
    : undefined;
  const machine = await acquire(ctx, model);
  try {
    // 1. The session file, rebuilt from the journal in the machine's cache dir.
    await machine.files.wipe();
    await machine.files.upload('config', text ? [{ path: 'session.jsonl', bytes: new TextEncoder().encode(text) }] : []);
    // 2. The child, mirrored into the journal through the machine's tail.
    const proc = await machine.spawn({
      command: process.execPath,
      args: [FAKE_CLI, `${machine.dirs.config}/session.jsonl`],
      // The model port admits the attach's key alone (the name a real CLI reads it from).
      env: { ...options.env, ...(model ? { FAKE_CLI_MODEL_URL: `${machine.modelUrl}/v1/model`, ANTHROPIC_AUTH_TOKEN: machine.modelKey } : {}) },
      signal: ctx.signal,
    });
    const tail = await machine.tail('config', 'session.jsonl', { offset: Buffer.byteLength(text) });
    const mirror = () => {
      for (const line of tail.read()) if (line) journal.append(NATIVE, line);
    };
    const synced = async () => {
      await tail.sync();
      mirror();
    };
    const events = proc.lines()[Symbol.asyncIterator]();
    const next = async (): Promise<CliEvent> => {
      const r = await events.next();
      if (r.done) {
        if (ctx.signal.aborted) throw ctx.signal.reason;
        const exit = await proc.exited;
        // The CLI gave up on a model call the budget refused: the turn ends here, saying which cap.
        if (capped) throw new Unavailable(`The turn stopped: the ${capped === 'turnUsd' ? "turn's" : "session's"} model spend reached its cap (sandbox({ budget: { ${capped} } })).`, { hint: `Raise sandbox({ budget: { ${capped} } }) to let the session spend more.` });
        throw new Error(`the CLI exited (${exit.signal ?? exit.code}) mid-turn: ${proc.stderrTail().slice(-500)}`);
      }
      return JSON.parse(r.value) as CliEvent;
    };
    const bridge = toolBridge(ctx, options.tools, { beforeNever: () => journal.flush() });
    const send = async (said: string, placed: Submission[], synthetic?: string) => {
      if (synthetic) journal.message({ type: 'message', role: 'user', text: said, synthetic });
      await placeSubmissions(ctx, placed);
      proc.send({ type: 'user', id: placed[0]?.id ?? synthetic, text: said });
    };
    const runTurn = async () => {
      let said = '';
      for (;;) {
        const ev = await next();
        if (ev.type === 'text') {
          mirror();
          said += ev.delta;
          ctx.live({ type: 'text', delta: ev.delta });
        } else if (ev.type === 'tool_call') {
          // The call's record is in the file before the event: synced, it is in the journal before the tool runs.
          await synced();
          journal.message({ type: 'message', role: 'assistant', text: said, toolCalls: [{ id: ev.id, name: ev.name }] });
          said = '';
          const result = await bridge.call(ev.name, ev.args, ev.id);
          proc.send({ type: 'tool_result', id: ev.id, text: result.text, isError: result.isError });
        } else if (ev.type === 'turn_end') {
          await synced();
          if (said) journal.message({ type: 'message', role: 'assistant', text: said });
          journal.message({ type: 'turn', status: 'completed' });
          await journal.flush();
          ctx.live({ type: 'idle' });
          return;
        }
      }
    };
    try {
      const ready = await next();
      if (ready.type !== 'ready') throw new Error(`the CLI said ${ready.type} before ready`);
      await synced();
      const note = recoveryNote(ctx.state());
      if (note) {
        await send(note.text, note.pending, 'recovery');
        await runTurn();
      }
      for (;;) {
        await ctx.refresh();
        const [s] = ctx.pending();
        if (!s) break;
        await send(s.text ?? '', [s]);
        await runTurn();
      }
      await steps.settleAbandoned();
    } finally {
      proc.end();
      await Promise.race([proc.exited, new Promise((r) => setTimeout(r, 1000))]);
      tail.close();
      await steps.close();
    }
  } finally {
    // Nothing the model started outlives the drive (§5.4 "Ending an invocation").
    await machine.close();
  }
}

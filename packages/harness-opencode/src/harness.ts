import { existsSync } from 'node:fs';
import { mkdir, readdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type DriveContext, type Harness, type HostTool, type HostToolIO, type ReplayPolicy, type SessionState, type Submission, Unavailable, Yield } from 'harness-world';
import type { WorkspaceRuntime } from 'harness-world-hands/runtime';
import {
  type AcquireMachine,
  BuiltinCalls,
  gatewayCredential,
  gatewayProxy,
  gatewayToken,
  type GatewayProxy,
  ModelSteps,
  settleAbandonedModelSteps,
  NativeJournal,
  placeSubmissions,
  type OpenTurn,
  openTurn as findOpenTurn,
  recoveryNote,
  restartEvent,
  replayBy,
  type ToolBridge,
  toolBridge,
  toolKey,
} from 'harness-world-process';
import { OPENCODE_READ_TOOLS, OPENCODE_TOOLS, type OpenCodeTool, WORKSPACE_ROOT, WorkspaceSlot } from './environment.ts';
import { SANDBOX_INSTRUCTIONS, SANDBOX_SHELL_NOTE, SandboxCalls } from './sandbox.ts';
import type { FromHost, HostInit, InstructionBlob, OpenCodeEvent, ToHost } from './protocol.ts';
import { pipePair } from './protocol.ts';
import { assertOpenCodeVersions } from './versions.ts';
import {
  applyStatus,
  BLOBS,
  blobRecordId,
  EVENTS,
  eventRecordId,
  isTerminal,
  type OpenCodeStatus,
  opencodeMessageId,
  opencodeSessionId,
  statusOf,
  TranscriptBuilder,
  eventName,
  eventsOf,
} from './records.ts';

/**
 * OpenCode v2 as a kernel `Harness`.
 *
 * Every drive rebuilds OpenCode's session from the log, runs it in this
 * process (OpenCode's embedded server: host.ts), and mirrors what it
 * commits back into the log:
 *
 * 1. Returns at once when there is nothing to do (no open turn, no pending
 *    submission): an idle wake starts no OpenCode.
 * 2. Reads the session's journaled OpenCode events and instruction blobs
 *    and starts the host with them: it replays them into an empty database
 *    and restores the claim and resume count they imply.
 * 3. Mirrors every event OpenCode commits into the journal (the outbox),
 *    with the normalized transcript built from them, and answers the host's
 *    model barriers (`journal.flush()`), tool calls (the tool bridge:
 *    `ctx.effect('tool:' + OpenCode's call id)`) and built-in calls (each a
 *    kernel effect too, run on the session's workspace: `BuiltinCalls`).
 * 4. If a crash cut the last turn off, resumes it the way OpenCode resumes
 *    its own: a recovery note built from the log, the execution claim, and
 *    OpenCode's restart sweep.
 * 5. Places each pending submission, then admits it (OpenCode's message id
 *    is `msg_<submission id>`, so a resend is a no-op), and waits until
 *    OpenCode is idle or its execution failed; steers are admitted mid-turn.
 * 6. Closes the host. A drive that is stopped (preempted, out of budget, a
 *    refused write) stops it. Once the drive is over, nothing the host sends
 *    reaches the log: no journal write, no tool call, no model request.
 *
 * Tool-less by default: the app's `tools` are the session's only tools.
 * With a `workspace`, OpenCode's own shell, read, write, edit, patch, glob
 * and grep (or the `builtins` listed) run on it: OpenCode's own tool code,
 * with its files and commands on the workspace (environment.ts), never on
 * this machine. On a sandbox, OpenCode's core stays in the drive and only
 * its tools' files and commands go to the session's VM (sandbox.ts).
 */

/**
 * A session's workspace, resolved from its descriptor (durable-harness's
 * `virtual()` or `sandbox()`): the runtime its calls run on (virtual), or
 * how to get the session's Vercel Sandbox, armed with the drive's fencing
 * token (sandbox: the sandbox tier's `AcquireMachine`, attached on the
 * first built-in call of a drive).
 */
export type OpenCodeWorkspace = { readonly kind: 'virtual'; readonly runtime: WorkspaceRuntime } | { readonly kind: 'sandbox'; readonly acquire: AcquireMachine };

export interface OpenCodeHarnessOptions {
  /**
   * The app's tools, as OpenCode plugin tools that run in the drive: each
   * call a kernel effect keyed by OpenCode's call id, with the tool's replay
   * policy. Without a `workspace`, the session's only tools. Default none.
   */
  tools?: HostTool[];
  /**
   * Where OpenCode's own tools run. With one, the session has OpenCode's
   * shell, read, write, edit, patch, glob and grep (`builtins` narrows
   * them), running OpenCode's own code on the workspace's files and shell.
   * Each call is in the log before it runs, and runs on the workspace as
   * the log has it: on a durable virtual workspace, what a call changed is
   * committed with its outcome; on a sandbox, it is on the session's Drive
   * before its outcome is recorded (a call that changes files or runs a
   * command is `never` replayed). Default none: no built-ins.
   */
  workspace?: OpenCodeWorkspace;
  /**
   * Which of OpenCode's own tools a session with a `workspace` has: `true`
   * or the default, all of them (`OPENCODE_TOOLS`); a list narrows them.
   * Without a `workspace`, `builtins` is a TypeError (`false` alone changes
   * nothing: the session is tool-less).
   */
  builtins?: boolean | readonly OpenCodeTool[];
  /** An AI Gateway model id, e.g. `anthropic/claude-haiku-4.5` (or OpenCode's `vercel/…` form). Default `anthropic/claude-haiku-4.5`. */
  model?: string;
  /** The kernel name (its sessions' runs are `workflow//durable-harness@<version>//<name>`). Default `opencode`. */
  name?: string;
  /**
   * Where each drive keeps OpenCode's own files on this machine (its
   * database, rebuilt from the log; its config and global directories,
   * empty): a 0700 directory per drive, removed when the drive ends.
   * Default `<os.tmpdir()>/harness-opencode`.
   */
  root?: string;
  /** The AI Gateway credential, read per request. Default `gatewayCredential` (`AI_GATEWAY_API_KEY`, else the Vercel OIDC token). */
  credential?: () => Promise<string>;
  /** The Gateway to proxy to. Default `AI_GATEWAY_BASE_URL` or `https://ai-gateway.vercel.sh`. */
  gatewayUrl?: string;
  /**
   * Added to every model request's system prompt, before what the
   * workspace adds (what it is, how long its files last). Default without a
   * workspace: a line saying the tools are the only ones
   * (`DEFAULT_INSTRUCTIONS`).
   */
  instructions?: string;
  /** The session's title (OpenCode's own title request is skipped). Default `harness-world session`. */
  title?: string;
  /** Start the host some other way (tests drive a scripted host). Default: in this process. */
  connect?: (ctx: DriveContext, init: HostInit) => Promise<HostConnection>;
  /** Tests: whether this machine has a path (default `existsSync`). */
  hostHas?: (path: string) => boolean;
}

/** Options the harness no longer takes, and what replaces them. */
const REMOVED: Record<string, string> = {
  mode: "OpenCode always runs in the drive's process: drop `mode`",
  directory: "its working directory is the workspace's /workspace: pass `workspace: virtual()` for its own tools",
  host: "each drive keeps OpenCode's own files in a directory of its own under `root`: pass `root` instead",
  workspaceNote: 'the workspace is the same on every machine: drop `workspaceNote`',
};

export const DEFAULT_MODEL = 'anthropic/claude-haiku-4.5';

/** The default `instructions` without a workspace. */
export const DEFAULT_INSTRUCTIONS = 'The tools you are given are your only tools.';

/** The calls of OpenCode's built-ins that change nothing: they only read the workspace. */
const readOnly = new Set<string>(OPENCODE_READ_TOOLS);

/** A built-in's replay policy on a virtual workspace: `safe` (a call that did not report changed nothing: it was rolled back). On a sandbox: the read-only ones `safe`, the rest `never` (the Drive keeps what a cut-off call did). */
export const opencodeBuiltinReplay = (workspace: 'virtual' | 'sandbox'): ((name: string) => ReplayPolicy) =>
  workspace === 'virtual' ? () => 'safe' : replayBy(OPENCODE_READ_TOOLS);

/** The workspace's built-ins a session gets, checked. */
function builtinsOf(options: OpenCodeHarnessOptions): OpenCodeTool[] {
  const raw = (options as { builtins?: unknown }).builtins;
  const all = OPENCODE_TOOLS.join(', ');
  if (raw === undefined) return options.workspace ? [...OPENCODE_TOOLS] : [];
  if (raw === false) {
    if (options.workspace) throw new TypeError("harness-world-opencode: `builtins: false` with a `workspace`: the workspace is where OpenCode's own tools run. Drop the workspace for a tool-less session, or `builtins` for its tools");
    return [];
  }
  if (!options.workspace)
    throw new TypeError("harness-world-opencode: `builtins` needs a `workspace` to run on (`workspace: virtual()`): OpenCode's own tools never run on the machine that drives the session");
  if (raw === true) return [...OPENCODE_TOOLS];
  if (!Array.isArray(raw)) throw new TypeError(`harness-world-opencode: \`builtins\` is \`true\` or a list of OpenCode's own tools: ${all}`);
  for (const b of raw) if (!(OPENCODE_TOOLS as readonly string[]).includes(b)) throw new TypeError(`harness-world-opencode: \`builtins\` has ${JSON.stringify(b)}, which is not one of OpenCode's own tools (${all})`);
  return [...new Set(raw as OpenCodeTool[])];
}

export function opencodeHarness(options: OpenCodeHarnessOptions = {}): Harness {
  for (const [key, why] of Object.entries(REMOVED)) if (key in options) throw new TypeError(`harness-world-opencode: \`${key}\` is gone: ${why}`);
  const builtins = builtinsOf(options);
  const workspace = options.workspace;
  if (workspace && !((workspace.kind === 'virtual' && typeof workspace.runtime?.run === 'function') || (workspace.kind === 'sandbox' && typeof workspace.acquire === 'function')))
    throw new TypeError(
      "harness-world-opencode: `workspace` takes a resolved workspace ({ kind: 'virtual', runtime } or { kind: 'sandbox', acquire }); durable-harness/opencode resolves virtual() and sandbox() for you"
    );
  const root = options.root ?? join(tmpdir(), 'harness-opencode');
  const model = (options.model ?? DEFAULT_MODEL).replace(/^(?!vercel\/)/, 'vercel/');
  const credential = options.credential ?? gatewayCredential;
  const tools = options.tools ?? [];
  const hostHas = options.hostHas ?? existsSync;
  const runtime = workspace?.kind === 'virtual' ? workspace.runtime : undefined;
  const onSandbox = workspace?.kind === 'sandbox';
  // What the workspace adds to each built-in's description, and to the system prompt.
  const notes = runtime ? { shell: runtime.describe('', 'shell').trim(), file: runtime.describe('', 'file').trim() } : onSandbox ? { shell: SANDBOX_SHELL_NOTE, file: '' } : { shell: '', file: '' };
  const system = [options.instructions ?? (workspace ? undefined : DEFAULT_INSTRUCTIONS), runtime?.instructions ?? (onSandbox ? SANDBOX_INSTRUCTIONS : undefined)].filter(Boolean).join('\n\n');
  // Drives of one session in this process.
  const running = new Set<string>();

  return {
    name: options.name ?? 'opencode',
    // Each model request reaches Agent Runs from the Gateway proxy (`ModelSteps.request`).
    modelCalls: 'self',
    async drive(ctx: DriveContext) {
      // Another OpenCode or effect than the pins fails the turn here, with both versions, instead of hanging it.
      assertOpenCodeVersions();
      const opening = openTurn(ctx.state());
      if (!opening && ctx.pending().length === 0) {
        // A model step a crash left open after its turn ended is closed (no request will retry it).
        await settleAbandonedModelSteps(ctx);
        return;
      }
      // Paths of this machine that OpenCode could still look at (a filesystem
      // lookup, a lock) are never the workspace's: a real /workspace here would be.
      if (!options.connect && hostHas(WORKSPACE_ROOT))
        throw new Unavailable(`harness-world-opencode: this machine has a ${WORKSPACE_ROOT} directory, which is the path of the session's workspace: OpenCode refuses to start here`, {
          hint: `Run the app where ${WORKSPACE_ROOT} does not exist (a Docker image's WORKDIR is often /workspace: use another).`,
        });
      if (running.has(ctx.runId)) throw new Yield('contended', 2);
      running.add(ctx.runId);
      let proxy: GatewayProxy | undefined;
      // The proxy also sees each model request start and end: the drive's
      // model steps, written around a "Crash during: model" drill.
      const steps = new ModelSteps(ctx);
      let conn: HostConnection | undefined;
      let init: HostInit | undefined;
      let drive: OpenCodeDrive | undefined;
      let calls: DriveBuiltins | undefined;
      const here = join(root, ctx.runId, randomUUID().slice(0, 8));
      try {
        const journal = new NativeJournal(ctx);
        const events = (await journal.read<OpenCodeEvent>(EVENTS)).map((r) => r.data);
        const blobs = (await journal.read<InstructionBlob>(BLOBS)).map((r) => r.data);
        const status = statusOf(events);
        // The session's built-ins, on its workspace: each call runs on the machine the workspace opens for it.
        const slot = workspace ? new WorkspaceSlot() : undefined;
        calls = builtinCalls(ctx, journal, workspace, slot);
        // The host's key, fresh per drive: the proxy forwards only requests
        // that carry it, so a process that doesn't hold it can't use the
        // proxy's credential. Embedded, OpenCode reads this process's
        // environment and sends `AI_GATEWAY_API_KEY` instead when it is set,
        // so the proxy takes that too (whoever holds it needs no proxy).
        const key = gatewayToken();
        const envKey = !options.connect ? process.env.AI_GATEWAY_API_KEY : undefined;
        proxy = await gatewayProxy({ credential, track: steps.request, token: envKey ? [key, envKey] : key, ...(options.gatewayUrl ? { target: options.gatewayUrl } : {}) });
        await mkdir(here, { recursive: true, mode: 0o700 });
        await writeFile(join(here, OWNER), JSON.stringify({ pid: process.pid }), { mode: 0o600 });
        init = {
          sessionId: opencodeSessionId(ctx.runId),
          directory: WORKSPACE_ROOT,
          // Per drive: a drive that follows this one at once never shares (or loses) its files.
          dbPath: join(here, 'opencode.db'),
          configDir: join(here, 'config'),
          global: join(here, 'global'),
          model,
          gatewayBaseURL: `${proxy.url}/v3/ai`,
          gatewayKey: key,
          tools: tools.map((t) => ({ name: t.name, description: t.description, inputSchema: t.inputSchema as Record<string, unknown> })),
          builtins,
          notes,
          system,
          title: options.title ?? 'harness-world session',
          events,
          blobs,
          claim: status.claimHeld,
          restarts: status.restarts,
        };
        if (options.connect) conn = await options.connect(ctx, init);
        else {
          await sweep(join(root, ctx.runId), here);
          conn = await startEmbedded(ctx, init, slot);
        }
        drive = new OpenCodeDrive(
          ctx,
          journal,
          conn,
          status,
          new TranscriptBuilder(events),
          toolBridge(ctx, tools, { beforeNever: () => journal.flush() }),
          calls.builtins,
          calls.outcome,
          calls.prepare
        );
        await drive.run(opening);
        // A model step a crash left open that no request of this drive made again: closed.
        await steps.settleAbandoned();
        drive.end();
        await conn.close();
      } finally {
        await drive?.interruptBuiltins().catch(() => {});
        drive?.end();
        conn?.kill();
        // No model step outlives the drive.
        await steps.close();
        await proxy?.close();
        // The database is rebuilt by every drive: nothing on disk outlives it
        // (on Vercel, /tmp is small and shared by every session on the
        // instance).
        if (conn) await exited(conn);
        // The sandbox's link: its commands killed, the VM and the Drive kept.
        await calls?.close?.();
        await rm(here, { recursive: true, force: true }).catch(() => {});
        running.delete(ctx.runId);
      }
    },
  };
}

/** What the log recorded for a built-in call, when it is not what OpenCode reported: said to the model with its result. */
export interface BuiltinOutcome {
  /** A line before the result (the in-memory workspace was reset). */
  note?: string;
  /** The failure recorded instead of OpenCode's result (what the call changed could not be kept). */
  failed?: string;
}

/** A drive's built-in calls, and what each needs besides `BuiltinCalls`. */
interface DriveBuiltins {
  builtins: BuiltinCalls;
  outcome: Map<string, BuiltinOutcome>;
  /** The pre-intent phase of a call (a sandbox's attach): the reason it won't run, if it won't. */
  prepare?: (callId: string) => Promise<{ message: string } | undefined>;
  /** At the drive's end. */
  close?: () => Promise<void>;
}

/**
 * The drive's built-in calls: on a virtual workspace each call's effect body
 * runs inside the workspace runtime's `run` (the files as the log has them,
 * what the call changed committed with its outcome, one call at a time); on
 * a sandbox, on the session's VM (sandbox.ts); either way with the call's
 * machine in `slot` for OpenCode's Environment.
 */
function builtinCalls(ctx: DriveContext, journal: NativeJournal, workspace: OpenCodeWorkspace | undefined, slot: WorkspaceSlot | undefined): DriveBuiltins {
  const outcome = new Map<string, BuiltinOutcome>();
  const beforeNever = () => journal.flush();
  if (!workspace || !slot) return { builtins: new BuiltinCalls(ctx, { beforeNever }), outcome };
  if (workspace.kind === 'sandbox') {
    const sandbox = new SandboxCalls(ctx, workspace.acquire, slot, outcome);
    return {
      builtins: new BuiltinCalls(ctx, { replay: opencodeBuiltinReplay('sandbox'), beforeNever, around: (call, run, io) => sandbox.around(call, run, io) }),
      outcome,
      prepare: (callId) => sandbox.prepare(callId),
      close: () => sandbox.close(),
    };
  }
  const runtime = workspace.runtime;
  const around = async (call: { callId: string; name: string }, run: () => Promise<string>, io: HostToolIO): Promise<string> => {
    let reported: string | undefined;
    try {
      const recorded = await runtime.run(io, readOnly.has(call.name) ? 'read' : 'write', (machine) => slot.with(machine, async () => (reported = await run())));
      if (reported !== undefined && recorded !== reported && recorded.endsWith(reported)) outcome.set(call.callId, { note: recorded.slice(0, recorded.length - reported.length).trim() });
      return recorded;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      // OpenCode reported the call done, and what it changed could not be kept: the model hears the failure.
      if (reported !== undefined) outcome.set(call.callId, { failed: message });
      return Promise.reject(err);
    }
  };
  return { builtins: new BuiltinCalls(ctx, { replay: opencodeBuiltinReplay('virtual'), beforeNever, around }), outcome };
}

/** The file in a drive's directory that names the process the drive runs in. */
const OWNER = 'owner.json';

/** Whether process `pid` of this machine is running. */
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as { code?: string }).code === 'EPERM';
  }
}

/**
 * Remove what drives that died left in the session's directory (their
 * databases, configs and global paths), keeping `keep`: a drive's directory
 * goes once the process it names has exited (or the directory has no owner
 * and is an hour old). Another process's live drive of the session keeps its own.
 */
async function sweep(dir: string, keep: string): Promise<void> {
  const before = Date.now() - 60 * 60_000;
  for (const name of await readdir(dir).catch(() => [] as string[])) {
    const path = join(dir, name);
    if (path === keep) continue;
    const owner = await readFile(join(path, OWNER), 'utf8')
      .then((t) => JSON.parse(t) as { pid?: unknown })
      .catch(() => undefined);
    const dead = typeof owner?.pid === 'number' ? owner.pid !== process.pid && !alive(owner.pid) : ((await stat(path).catch(() => undefined))?.mtimeMs ?? 0) < before;
    if (dead) await rm(path, { recursive: true, force: true }).catch(() => {});
  }
}

/** Wait for a host to go (briefly: one that never reports its exit must not hold the drive open forever). */
async function exited(conn: HostConnection): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    conn.exited.catch(() => {}),
    new Promise<void>((r) => {
      timer = setTimeout(r, 5000);
      timer.unref?.();
    }),
  ]);
  clearTimeout(timer);
}

/** The turn a crash cut off, if there is one with work in it: prompts placed in it, tool calls, transcript entries, or OpenCode mid-execution. */
interface OpenTurnWork {
  turn: OpenTurn;
  /** The prompts placed in it (a chaos arming is placed too, and is no prompt). */
  prompts: string[];
}

function openTurn(state: SessionState): OpenTurnWork | undefined {
  const turn = findOpenTurn(state);
  if (!turn) return undefined;
  const kinds = new Map(state.submissions.map((s) => [s.id, s.kind]));
  const prompts = turn.placed.filter((id) => kinds.get(id) !== 'chaos' && kinds.get(id) !== 'withdraw');
  // Execution events are small: always inline in the fold.
  const status = statusOf(eventsOf(state));
  if (!prompts.length && !turn.calls.length && !turn.entries.length && !status.claimHeld && !status.undelivered.size) return undefined;
  return { turn, prompts };
}

/** The drive's side of a running host. */
export interface HostConnection {
  send(m: ToHost): void;
  messages: AsyncIterable<FromHost>;
  /** Ask the host to close, and wait (briefly) for it to go. */
  close(): Promise<void>;
  /** Stop it now. */
  kill(): void;
  /** What it said on its way out (stderr). */
  tail(): string;
  /** Resolves once the host is gone. */
  exited: Promise<unknown>;
}

/**
 * OpenCode in this process. Nothing here, or in the host, changes the
 * process's environment (`process.env`): every path OpenCode uses comes
 * from the drive (host.ts), never from XDG_* variables. A new variable
 * would make libc move the environment's array and free the old one, and
 * native code that kept a pointer to it (libfx's addon does) reads freed
 * memory and can crash the process, so a Function that also runs libfx's
 * agents must never see OpenCode grow its environment.
 */
async function startEmbedded(ctx: DriveContext, init: HostInit, workspace: WorkspaceSlot | undefined): Promise<HostConnection> {
  const { runHost } = await import('./host.ts');
  const { left, right } = pipePair<FromHost, ToHost>();
  let error = '';
  const done = runHost(init, right, workspace ? { workspace } : {})
    .catch((err) => {
      error = err instanceof Error ? (err.stack ?? err.message) : String(err);
      right.send({ type: 'error', message: error });
    })
    .finally(() => right.close());
  // A stopped drive stops OpenCode: ending its input closes it.
  const stop = () => left.close();
  ctx.signal.addEventListener('abort', stop, { once: true });
  void done.finally(() => ctx.signal.removeEventListener('abort', stop));
  return {
    send: (m) => left.send(m),
    messages: left.messages,
    async close() {
      left.send({ type: 'close' });
      await Promise.race([done, new Promise((r) => setTimeout(r, 5000))]);
    },
    kill: stop,
    tail: () => error,
    exited: done,
  };
}

type Reply = Extract<FromHost, { type: 'reply' }>;

/** One drive's conversation with its host. */
class OpenCodeDrive {
  private ready: number | undefined;
  private ended = false;
  private hostError: string | undefined;
  private starts = 0;
  private terminals = 0;
  /** The last execution OpenCode ended failed (and none started since): what it had not delivered yet stays in its inbox. */
  private failed = false;
  /** OpenCode message ids this drive admitted (or re-sent): a turn is over once each is in and OpenCode is idle. */
  private readonly expected = new Set<string>();
  private readonly replies = new Map<number, (r: Reply) => void>();
  private nextId = 0;
  private wakers: (() => void)[] = [];
  /** The drive is over (`end`): what the host still sends is dropped or refused. */
  private over = false;

  constructor(
    private readonly ctx: DriveContext,
    private readonly journal: NativeJournal,
    private readonly conn: HostConnection,
    private readonly status: OpenCodeStatus,
    private readonly transcript: TranscriptBuilder,
    private readonly bridge: ToolBridge,
    private readonly builtins: BuiltinCalls,
    /** What the log recorded for a built-in, when it differs from what OpenCode reported (by call id). */
    private readonly outcome: Map<string, BuiltinOutcome>,
    /** A built-in call's pre-intent phase (a sandbox's attach), before `begin` writes its intent. */
    private readonly prepare?: (callId: string) => Promise<{ message: string } | undefined>
  ) {
    void this.read();
  }

  async run(opening: OpenTurnWork | undefined): Promise<void> {
    await this.until(() => this.ready !== undefined);
    if (opening) await this.recover(opening);
    // The inbox, one submission per turn (steers join the running turn).
    for (;;) {
      await this.ctx.refresh();
      const [s] = this.ctx.pending();
      if (!s) break;
      await this.admit(s, 'queue');
      await this.turn();
    }
  }

  /** The drive is over: from now on nothing the host sends is journaled, and every request it makes is refused. */
  end(): void {
    this.over = true;
  }

  /** Built-ins OpenCode never reported (its host died mid-call): interrupted. A stopped drive's are the kernel's. */
  async interruptBuiltins(): Promise<void> {
    if (this.builtins.size) await this.builtins.interruptAll();
  }

  /** Every host message, in order. */
  private async read(): Promise<void> {
    try {
      for await (const m of this.conn.messages) {
        if (this.over && (m.type === 'barrier' || m.type === 'tool' || m.type === 'builtin')) {
          this.conn.send({ type: 'reply', id: m.id, ok: false, error: 'the drive is over' });
          continue;
        }
        switch (m.type) {
          case 'ready':
            this.ready = m.seq;
            break;
          case 'events':
            if (!this.over) this.mirror(m.events, m.blobs);
            break;
          case 'barrier':
            // Nothing irreversible (a paid model call) runs ahead of the log.
            this.journal.flush().then(
              () => this.conn.send({ type: 'reply', id: m.id, ok: true }),
              (err) => this.conn.send({ type: 'reply', id: m.id, ok: false, error: message(err) })
            );
            break;
          case 'tool':
            // Keyed by the call's id as the log names it (scoped when OpenCode's id was reused; see `TranscriptBuilder.callId`).
            this.bridge.call(m.name, m.input, this.transcript.callId(m.callId)).then(
              (r) => this.conn.send({ type: 'reply', id: m.id, ok: true, text: r.text, isError: r.isError }),
              (err) => this.conn.send({ type: 'reply', id: m.id, ok: false, error: message(err) })
            );
            break;
          case 'builtin': {
            const callId = this.transcript.callId(m.callId);
            if (m.phase === 'begin')
              // OpenCode's record of the call (its event, shipped just before) is in the log before the call's intent, whatever its replay.
              this.beginBuiltin(callId, m.name, m.input).then(
                (refused) => this.conn.send(refused === undefined ? { type: 'reply', id: m.id, ok: true } : { type: 'reply', id: m.id, ok: false, error: refused }),
                (err) => this.conn.send({ type: 'reply', id: m.id, ok: false, error: message(err) })
              );
            else
              this.builtins.end(callId, { name: m.name, text: m.text, isError: m.isError }).then(
                () => {
                  const said = this.outcome.get(callId);
                  this.outcome.delete(callId);
                  this.conn.send({ type: 'reply', id: m.id, ok: true, ...said });
                },
                (err) => this.conn.send({ type: 'reply', id: m.id, ok: false, error: message(err) })
              );
            break;
          }
          case 'text':
            if (!this.over) this.ctx.live({ type: 'text', delta: m.delta });
            break;
          case 'reply':
            this.replies.get(m.id)?.(m);
            this.replies.delete(m.id);
            break;
          case 'error':
            this.hostError = m.message;
            break;
        }
        this.wake();
      }
    } finally {
      this.ended = true;
      this.wake();
    }
  }

  /**
   * A built-in is about to run: its pre-intent phase (a sandbox's attach),
   * then its intent. Undefined: let it run; else why it doesn't (OpenCode
   * says "Not run: …"): the log's answer for a call that already ran, or
   * the failure decided before it could run (its sandbox couldn't be
   * attached).
   */
  private async beginBuiltin(callId: string, name: string, input: unknown): Promise<string | undefined> {
    await this.journal.flush();
    const known = this.ctx.isMemoized(toolKey(callId));
    await this.prepare?.(callId);
    const start = await this.builtins.begin({ callId, name, input });
    if (start.run) return undefined;
    return known ? `this call already ran in this session. Its recorded result: ${start.result.text}` : start.result.text.replace(/^not run: /i, '');
  }

  /** Journal what OpenCode committed, and the transcript it completes (one batch: the same synchronous burst). */
  private mirror(events: OpenCodeEvent[], blobs: InstructionBlob[]): void {
    for (const b of blobs) this.journal.append(BLOBS, b, { id: blobRecordId(b.hash) });
    for (const e of events) {
      if (!this.journal.append(EVENTS, e, { id: eventRecordId(e.seq) })) continue;
      applyStatus(this.status, e);
      const name = eventName(e);
      if (name === 'session.execution.started') {
        this.starts++;
        this.failed = false;
      }
      if (isTerminal(e)) this.terminals++;
      if (name === 'session.execution.failed') this.failed = true;
      for (const entry of this.transcript.push(e)) this.journal.message(entry);
    }
  }

  private wake(): void {
    for (const w of this.wakers.splice(0)) w();
  }

  /** Resolves once `cond()` holds; rejects if the host ends first or the drive stops. */
  private async until(cond: () => boolean, signal?: AbortSignal): Promise<void> {
    for (;;) {
      if (cond()) return;
      if (this.ctx.signal.aborted) throw this.ctx.signal.reason;
      if (signal?.aborted) return;
      if (this.ended) throw new Error(`the OpenCode host exited: ${(this.hostError ?? this.conn.tail()).slice(-2000) || 'no output'}`);
      await new Promise<void>((r) => {
        this.wakers.push(r);
        signal?.addEventListener('abort', () => r(), { once: true });
      });
    }
  }

  private async request(m: { type: 'prompt'; messageId: string; text: string; delivery: 'queue' | 'steer' } | { type: 'recover'; note: string }): Promise<void> {
    const id = ++this.nextId;
    let reply = undefined as Reply | undefined;
    this.replies.set(id, (r) => (reply = r));
    this.conn.send({ ...m, id } as ToHost);
    await this.until(() => reply !== undefined);
    if (!reply!.ok) throw new Error(`OpenCode refused ${m.type}: ${reply!.error}`);
  }

  /** Place, then admit: a withdraw that wins never reaches OpenCode. */
  private async admit(s: Submission, delivery: 'queue' | 'steer'): Promise<void> {
    await placeSubmissions(this.ctx, [s]);
    const messageId = opencodeMessageId(s.id);
    this.expected.add(messageId);
    await this.request({ type: 'prompt', messageId, text: s.text ?? '', delivery });
  }

  /**
   * OpenCode is idle: everything this drive admitted is in, no execution
   * runs, and its inbox is empty, or the execution failed (OpenCode starts
   * no other for what it had not delivered: the turn is over, failed, and
   * the transcript says why).
   */
  private idle = () =>
    [...this.expected].every((id) => this.status.enqueued.has(id)) && !this.status.executing && (this.status.undelivered.size === 0 || this.failed);

  /** Wait for the turn to end, admitting steers meanwhile. */
  private async turn(started: () => boolean = () => true): Promise<void> {
    const cond = () => started() && this.idle();
    for (;;) {
      const stop = new AbortController();
      const steering = this.steer(stop.signal);
      try {
        await this.until(cond);
      } finally {
        stop.abort();
        await steering.catch(() => {});
      }
      // A steer admitted as the turn ended starts another execution: wait for it too.
      if (cond()) break;
    }
    await this.journal.flush();
    this.ctx.live({ type: 'idle' });
  }

  /** Admit steers while a turn runs (OpenCode delivers them at its next step). */
  private async steer(stop: AbortSignal): Promise<void> {
    while (!stop.aborted) {
      await new Promise<void>((r) => {
        const timer = setTimeout(done, this.ctx.onHint ? 10_000 : 1500);
        const off = this.ctx.onHint?.(() => done());
        stop.addEventListener('abort', done, { once: true });
        function done() {
          clearTimeout(timer);
          off?.();
          r();
        }
      });
      if (stop.aborted) return;
      await this.ctx.refresh();
      for (const s of this.ctx.pending()) {
        if (stop.aborted) return;
        if (s.kind === 'steer') await this.admit(s, 'steer');
      }
    }
  }

  /**
   * Resume a turn a crash cut off. If OpenCode never admitted its prompts
   * (the crash came between placing and admitting), admit them again;
   * otherwise resume it as OpenCode resumes its own: the recovery note (what
   * the log knows about every tool call since the turn began), the claim,
   * and OpenCode's restart sweep, which settles the calls that were in
   * flight as interrupted and continues the turn.
   */
  private async recover(opening: OpenTurnWork): Promise<void> {
    const state = this.ctx.state();
    const lost = opening.prompts.filter((id) => !this.status.enqueued.has(opencodeMessageId(id)));
    const touched =
      this.status.claimHeld ||
      this.status.undelivered.size > 0 ||
      lost.length < opening.prompts.length ||
      opening.turn.calls.length > 0 ||
      opening.turn.entries.length > 0;
    if (!touched) {
      for (const id of lost) {
        const messageId = opencodeMessageId(id);
        this.expected.add(messageId);
        await this.request({ type: 'prompt', messageId, text: state.submissions.find((s) => s.id === id)?.text ?? '', delivery: 'queue' });
      }
      return this.turn();
    }
    // The note quotes the turn's prompts when OpenCode may have lost them.
    const note = recoveryNote(state, { pending: [], prompts: lost.length > 0 })!.text;
    this.journal.message({ type: 'message', role: 'user', text: note, synthetic: 'recovery' });
    // OpenCode starts the step the crash cut off over: what a reader saw of its reply is replaced by what the log kept (`restartEvent`).
    this.ctx.live(restartEvent(opening.turn));
    const [s0, t0] = [this.starts, this.terminals];
    await this.request({ type: 'recover', note });
    await this.turn(() => this.starts > s0 || this.terminals > t0);
  }
}

const message = (err: unknown) => (err instanceof Error ? err.message : String(err));

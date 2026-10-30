import type { World } from '@workflow/world';
import {
  type ChatAuthorize,
  type ChatRoute,
  chatRoute as kernelChatRoute,
  createDriveRoute,
  type Doc,
  type DriveRoute,
  type DurableSessions,
  type DurableSessionsOptions,
  durableSessions,
  type Harness,
  workflowNameFor,
} from 'harness-world';
import { vercelChannel } from 'harness-world/vercel-channel';
import { unavailableHint } from './hint.ts';
import { register } from './registry.ts';
import { publicSandboxConfig, sandboxHarnessMark } from './sandbox-harness.ts';
import { getWorld } from './world.ts';

/**
 * How a session's writes relate to the agent.
 *
 * - `strict` (Durable Strict): every write is durable before the agent goes
 *   on, and a message is in the log before `send` returns.
 * - `on` (Durable On): a drive's writes trail the agent and it waits for
 *   them only at barriers (before a `replay: 'never'` tool, before it lets
 *   the session go). A message sent through the chat route is acknowledged
 *   before it is durable; a lost write is re-issued by the drive.
 */
export type Durability = 'on' | 'strict';

/** What every agent factory takes besides its harness's own options. */
export interface AgentOptions
  extends Pick<
    DurableSessionsOptions,
    'log' | 'chaos' | 'marks' | 'directDriveMs' | 'maxInvocationMs' | 'deadlineMarginMs' | 'writeStallMs' | 'hints' | 'anchors' | 'crash'
  > {
  /**
   * The World sessions live on. Default: the environment's, chosen the way
   * the Workflow SDK chooses it (Vercel's World on Vercel, world-local
   * elsewhere, `WORKFLOW_TARGET_WORLD` to pick). An agent given its own World
   * is served only through its own `handler`.
   */
  world?: World;
  /** See `Durability`. Default: the harness's (`on` for Pi, `strict` for the rest). */
  durability?: Durability;
  /**
   * Trace audience, persisted with the session. When omitted, public chat
   * routes use public and other chat routes use unknown. Direct
   * `agent.sessions.start()` calls use the integration's audience default
   * unless their session config declares `traceAudience`.
   */
  traceAudience?: 'public' | 'private' | 'unknown';
}

export interface ChatRouteOptions {
  /**
   * Who may use the route: a function that checks the caller may use the
   * session (`sessionId` is undefined when a POST is about to start one) and
   * throws a `Response` (401, 404) to refuse, or `'public'` to serve anyone.
   * Required in production: without it, a route with `NODE_ENV` `production`
   * (`next start`, every Vercel deployment) refuses every request with a 401
   * that says so. Under `next dev` the route is open.
   */
  authorize?: ChatAuthorize;
  /**
   * Where a message's turn runs:
   * - `route` (default): in the invocation that received it, right away,
   *   under a deadline (`directDriveMs`, default 60 s), with one queue
   *   message delayed past that deadline behind it. If the invocation dies,
   *   that message resumes the turn in the flow route; if the turn finished,
   *   it finds nothing to do. Where the World's queue holds a message for
   *   its sender (Vercel Queues' send-and-hold), the turn's queue message
   *   is sent undelayed and held instead, and this invocation runs it as
   *   that message's delivery: no deadline, as long as the invocation
   *   lives, and a lapsed hold redelivers it to the flow route. A message
   *   the queue does not hold goes to the flow route, as with `queue`.
   * - `queue`: one queue message, delivered to the flow route now.
   */
  drive?: 'route' | 'queue';
  /** A comment this often keeps proxies from closing a quiet response. Default 15 s. */
  keepaliveMs?: number;
  /**
   * Serve `authorize: 'public'` for an agent whose sessions each boot a
   * Vercel Sandbox VM and create a Drive (Claude Code, Codex, a harness given
   * `sandbox()`): anyone who can reach the route can then make you pay for
   * VM time and storage, within each session's `sandbox({ budget })`.
   * Without it such a route throws a `TypeError` when it is made, and so it
   * does with it while the workspace's `budget.sessionUsd` is `Infinity` (the
   * default): nothing caps how many sessions a public route opens, so each
   * one's model spend must be.
   */
  allowPublicSandbox?: boolean;
  /**
   * Documents (`defineDoc`) the browser may see: each response starts with
   * the current value of each listed document that has been written, and
   * forwards its live `doc` events (migrated by the definition given here).
   * Default none: documents are app state. Writes go through your own route
   * (`agent.sessions.updateDoc`, behind your own authorization).
   */
  docs?: Doc<any>[];
}

export interface Agent {
  /** The harness's kernel name, its sessions' run name's last part (`workflow//durable-harness@<version>//<name>`). */
  readonly name: string;
  /**
   * The chat route: `export const { GET, POST } = agent.chatRoute({ authorize })`.
   * POST `{ sessionId?, text, kind? }` sends a message (starting a session
   * when there is none) and streams its turn as server-sent events until it
   * settles; GET `?sessionId=…&cursor=…` (or `Last-Event-ID`) reconnects just
   * past the last event a client saw. The turn runs on the log, not in the
   * response: a response that ends early loses nothing.
   */
  chatRoute(options?: ChatRouteOptions): ChatRoute;
  /** The queue consumer for this agent alone (`withDurableHarness` mounts every agent's for you). */
  handler(request: Request): Promise<Response>;
  /** The sessions underneath: start, send, inspect, live, close, ... */
  readonly sessions: DurableSessions;
}

/** Settles a promise past the response on Vercel (the request context's `waitUntil`); elsewhere the server outlives it. */
export function waitUntil(work: Promise<unknown>): void {
  const settled = work.then(
    () => {},
    () => {}
  );
  const ctx = (globalThis as { [k: symbol]: { get?: () => { waitUntil?: (p: Promise<unknown>) => void } | undefined } | undefined })[
    Symbol.for('@vercel/request-context')
  ]?.get?.();
  ctx?.waitUntil?.(settled);
}

const AGENT_KEYS = ['world', 'durability', 'traceAudience', 'log', 'chaos', 'marks', 'directDriveMs', 'maxInvocationMs', 'deadlineMarginMs', 'writeStallMs', 'hints', 'anchors', 'crash'] as const;

/** A factory's options, split into the agent's and the harness's (everything else, passed through untouched). */
export function splitOptions<T extends AgentOptions>(options: T): [AgentOptions, Omit<T, (typeof AGENT_KEYS)[number]>] {
  const agent: Record<string, unknown> = {};
  const harness = { ...options } as Record<string, unknown>;
  for (const key of AGENT_KEYS) {
    if (harness[key] !== undefined) agent[key] = harness[key];
    delete harness[key];
  }
  return [agent as AgentOptions, harness as Omit<T, (typeof AGENT_KEYS)[number]>];
}

export interface DurableAgentOptions extends AgentOptions {
  /** The harness: one of this package's (`claudeCodeHarness`, `piHarness`, ...) or your own. */
  harness: Harness;
}

interface KernelAgentInput extends DurableAgentOptions {
  /** The durability when the caller names none. */
  defaultDurability?: Durability;
}

/**
 * An agent over a harness of your own (a kernel `Harness`: `name` and
 * `drive(ctx)`, its model and tool calls as `ctx.effect` steps), with the
 * same chat route, queue consumer and sessions as the built-in ones.
 *
 * ```ts
 * export const agent = durableAgent({ harness: myHarness() })
 * ```
 */
export function durableAgent(options: DurableAgentOptions): Agent {
  return kernelAgent(options);
}

/** An agent over the kernel (`durableSessions`) and one harness. */
export function kernelAgent(input: KernelAgentInput): Agent {
  const { harness, traceAudience, world: given, durability = input.defaultDurability, defaultDurability: _, ...kernel } = input;
  if (durability !== undefined && durability !== 'on' && durability !== 'strict') {
    throw new TypeError(`durable-harness: durability must be 'on' or 'strict' (got ${String(durability)})`);
  }
  let made: Promise<DurableSessions> | undefined;
  const real = (): Promise<DurableSessions> =>
    (made ??= (given ? Promise.resolve(given) : getWorld()).then((world) =>
      durableSessions({
        marks: false,
        ...kernel,
        world,
        harness,
        // A drive this process runs itself writes over world-vercel's socket
        // (a no-op on every other World).
        openChannel: vercelChannel(),
        // A harness that cannot run here fails its turn with an error event
        // saying what to install or include (harness-world's unavailable.ts).
        unavailableHint,
        ...(durability !== undefined ? { runAhead: durability === 'on' } : {}),
      })
    ));
  const sessions = lazySessions(real, traceAudience);
  const workflowName = workflowNameFor(harness.name);
  register({
    name: harness.name,
    workflowName,
    ...(given ? { world: given } : {}),
    consume: async (message, meta) => (await real()).consume(message, meta),
  });

  return {
    name: harness.name,
    sessions,
    handler: (request) => sessions.handler(request),
    chatRoute(options = {}) {
      const mark = options.authorize === 'public' ? sandboxHarnessMark(harness) : undefined;
      if (mark && options.allowPublicSandbox !== true) {
        throw new TypeError(
          `durable-harness: ${harness.name}'s sessions each boot a Vercel Sandbox VM and create a Drive, so chatRoute({ authorize: 'public' }) would let anyone make you pay for them: pass your own authorize, or allowPublicSandbox: true to serve it publicly anyway (each session's spend stays within sandbox({ budget }))`
        );
      }
      // Nothing caps how many sessions (and VMs) a public route opens yet: each one's model spend must be capped, at least.
      if (mark && !Number.isFinite(mark.sessionUsd)) {
        throw new TypeError(
          `durable-harness: chatRoute({ authorize: 'public', allowPublicSandbox: true }) for ${harness.name} needs a finite sandbox({ budget: { sessionUsd } }): anyone can open sessions on a public route, and each one's model spend is otherwise uncapped`
        );
      }
      const turns = options.drive === 'queue' ? undefined : driveRoute(workflowName, sessions, kernel.log);
      return kernelChatRoute(
        {
          start: () => sessions.start({ config: { ...(durability !== undefined ? { durability: durability === 'on' ? 'run-ahead' : 'strict' } : {}), traceAudience: traceAudience ?? (options.authorize === 'public' ? 'public' : 'unknown'), ...(options.authorize === 'public' ? publicSandboxConfig(options.allowPublicSandbox) : {}) } }),
          send: (id, text, kind) => sessions.send(id, text, kind),
          inspect: (id) => sessions.inspect(id),
          liveEnd: (id) => sessions.liveEnd(id),
          liveEntries: (id, o) => sessions.liveEntries(id, o),
        },
        {
          ...(options.authorize !== undefined ? { authorize: options.authorize } : {}),
          ...(options.keepaliveMs !== undefined ? { keepaliveMs: options.keepaliveMs } : {}),
          ...(options.docs ? { docs: options.docs } : {}),
          ...(turns
            ? {
                async send(id: string, text: string, kind: 'prompt' | 'steer' | 'followUp') {
                  const submissionId = await sessions.send(id, text, kind, { wake: 'backstop', waitUntil });
                  turns.drive(id, { why: kind });
                  return submissionId;
                },
              }
            : {}),
        }
      );
    },
  };
}

/**
 * One drive route per agent per process (on `globalThis`, so every module
 * instance of a route shares it): a message for a session this process is
 * driving makes that drive run once more instead of contending with it. The
 * drive starts at once, not after the response (the response is the turn's
 * stream, and Next runs `after()` work only once a response has ended);
 * `waitUntil` only keeps the invocation alive until it is done.
 */
function driveRoute(workflowName: string, sessions: DurableSessions, log?: DurableSessionsOptions['log']): DriveRoute {
  const g = globalThis as { [ROUTES]?: Map<string, { route: DriveRoute; sessions: DurableSessions }> };
  const routes = (g[ROUTES] ??= new Map());
  let entry = routes.get(workflowName);
  if (!entry) {
    const made: { route?: DriveRoute; sessions: DurableSessions } = { sessions };
    made.route = createDriveRoute({
      after: (work) => waitUntil(work()),
      // The newest agent of this name (a module evaluated again under `next dev` makes a new one).
      drive: (id, meta) => made.sessions.drive(id, meta),
      ...(log ? { log } : {}),
    });
    entry = made as { route: DriveRoute; sessions: DurableSessions };
    routes.set(workflowName, entry);
  }
  entry.sessions = sessions;
  return entry.route;
}
const ROUTES = Symbol.for('durable-harness//drive-routes');

/** `DurableSessions`, bound to its World on first use (a module that makes an agent does no I/O). */
function lazySessions(real: () => Promise<DurableSessions>, traceAudience?: 'public' | 'private' | 'unknown'): DurableSessions {
  return {
    handler: async (request) => (await real()).handler(request),
    consume: async (message, meta) => (await real()).consume(message, meta),
    start: async (o) => (await real()).start(traceAudience === undefined ? o : { ...o, config: { ...(o?.config && typeof o.config === 'object' ? o.config : {}), traceAudience } }),
    send: async (id, text, kind, o) => (await real()).send(id, text, kind, o),
    drive: async (id, meta) => (await real()).drive(id, meta),
    withdraw: async (id, submissionId) => (await real()).withdraw(id, submissionId),
    chaos: async (id, spec) => (await real()).chaos(id, spec),
    close: async (id) => (await real()).close(id),
    inspect: async (id) => (await real()).inspect(id),
    doc: async (id, doc) => (await real()).doc(id, doc),
    updateDoc: async (id, doc, mutate, o) => (await real()).updateDoc(id, doc, mutate, o),
    liveEnd: async (id) => (await real()).liveEnd(id),
    async *live(id, o) {
      yield* (await real()).live(id, o);
    },
    async *liveEntries(id, o) {
      yield* (await real()).liveEntries(id, o);
    },
  };
}

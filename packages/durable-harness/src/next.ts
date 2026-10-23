import { existsSync, mkdirSync, readdirSync, readFileSync, rmdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, sep } from 'node:path';
import { isVercelWorldTarget, resolveWorkflowTargetWorld } from '@workflow/utils';
import type { NextConfig } from 'next';
import { harnessFiles, hostTarget, resolves } from './native.ts';
import { FLOW_ROUTE, flowFunctionConfig, FX_ROUTE, type GeneratedRoute, LIBFX_QUEUE_PREFIX, LIBFX_TOPIC, QUEUE_TOPIC, WORKFLOW_FLOW_ROUTE } from './routing.ts';
import { type AppPlan, appPlan, exportsFxAgent, fxAgentNames, type FxModule, fxCalls, MARK, nameOf, nextFunctions, readText, ROUTE_EXTENSIONS, routeDir, routeKey, triggerConfigured } from './scan.ts';
import { excludeLocalWorld } from './trace.ts';

/**
 * durable-harness/next: `withDurableHarness` for next.config.
 *
 * ```ts
 * // next.config.ts
 * import { withDurableHarness } from 'durable-harness/next'
 * export default withDurableHarness({})
 * ```
 *
 * With the Workflow SDK in the same app: `withWorkflow(withDurableHarness({}))`,
 * and on Vercel one `functions` entry (`npx durable-harness vercel-json --write`).
 */

export interface DurableHarnessOptions {
  /**
   * The modules that make your agents, relative to the project: durable-harness
   * agents (`claudeCode()`, `pi()`, ...) and fx agents (libfx's
   * `createFxAgent()`). Default: modules that export an agent made at module
   * scope with `durableAgent`, `claudeCode`, `codex`, `opencode` or `pi` from
   * durable-harness (including import aliases and exported containers or
   * accessors for that value). Calls inside factories, strings and comments
   * do not count. A module that calls pi-durable's `resumeWith` at module
   * scope is also included: it registers an opener, with or without an
   * export. Use `agents` for custom factories or effect-only registrations.
   * Automatic discovery also includes every module that
   * imports `createFxAgent` (from `libfx`, or a module of yours that
   * re-exports it) and calls it. A listed module that calls `createFxAgent(`
   * is an fx agent's (the first one listed is the one the fx route mounts);
   * one that makes a durable-harness agent is that too. A listed module that
   * shows neither (it makes its agent through a function of your own) is taken
   * as both: the flow route imports it for its durable-harness agents and, when
   * no listed module calls `createFxAgent(`, finds the fx agent among its
   * exports.
   */
  agents?: string[];
  /**
   * Routes that drive sessions in their own invocation, as Next route paths
   * (`/api/chat`): each gets the harnesses' run-time files traced in (OpenCode's
   * grammars), as the flow route does. Default: every route file that calls
   * `chatRoute(`.
   */
  driveRoutes?: string[];
  /**
   * Write the routes (the flow route, the queue's consumer, and with the
   * Workflow SDK the fx route, libfx's) and their queue triggers, or check
   * them. Default true; a route is skipped by itself when the app has its own
   * file there.
   */
  flowRoute?: boolean;
}

type Phase = string;
type ConfigInput = NextConfig | ((phase: Phase, ctx: { defaultConfig: NextConfig }) => NextConfig | Promise<NextConfig>);

/**
 * Wraps a Next.js config for durable agents:
 *
 * - keeps durable-harness and each installed harness SDK out of the server
 *   bundle (`serverExternalPackages`: they read their own package files, or
 *   must be one module instance per process);
 * - traces OpenCode's tree-sitter grammars (native.ts, resolved from your
 *   `node_modules`) into the routes that drive sessions: the flow route and
 *   every route that calls `chatRoute(`. Claude Code and Codex need no
 *   binary in a Function: their CLIs run in the session's Vercel Sandbox,
 *   from its base image;
 * - writes the flow route, which mounts every agent the app makes: the
 *   consumer of durable-harness's queue (`__harness_wkf_workflow_*`, never the
 *   Workflow SDK's `__wkf_workflow_*`). A `.gitignore` keeps it out of git; it
 *   is written again on every build and dev start. Where:
 *   - in an app without the Workflow SDK, at the Workflow SDK's path,
 *     `<app>/.well-known/workflow/v1/flow/route.js`, with its queue trigger in
 *     `config.json` next to it, read by Vercel's Next.js builder: no
 *     `vercel.json`. It answers the Workflow tools' `HEAD ?__health` probe.
 *     For an fx agent (libfx's `createFxAgent()`, exported from its module)
 *     the same route takes libfx's wakes too (a second trigger on libfx's
 *     topic) and hands them to the agent's `wakeHandler()`, by each
 *     delivery's topic;
 *   - in an app that also uses the Workflow SDK (`workflow` or
 *     `@workflow/next` installed, or a route of its own at that path), which
 *     owns that path, at durable-harness's own,
 *     `<app>/.well-known/durable-harness/v1/flow/route.js`, and an fx agent's
 *     wakes at `.../v1/fx/route.js`. On Vercel the queue delivers to such a
 *     route only when a `functions` entry subscribes it (`vercel.json`,
 *     written by `npx durable-harness vercel-json --write`, or
 *     `harnessFunctions()` in `vercel.ts`): a build on Vercel without it
 *     fails, saying what to add, and anywhere else it is a warning (locally
 *     the World delivers in process);
 * - keeps `libfx` (a native addon) out of the bundle, for an fx agent;
 * - on a build on Vercel for Vercel's World, keeps world-local out of every
 *   route's Function, which never loads it (trace.ts).
 *
 * An app that also uses the Workflow SDK composes both:
 * `withWorkflow(withDurableHarness({}))`. They share no route and no queue.
 */
export function withDurableHarness(nextConfig: ConfigInput = {}, options: DurableHarnessOptions = {}) {
  return async function harnessesConfig(phase: Phase, ctx: { defaultConfig: NextConfig }): Promise<NextConfig> {
    const base = typeof nextConfig === 'function' ? await nextConfig(phase, ctx) : nextConfig;
    return applyDurableHarness({ ...base }, phase, process.cwd(), options);
  };
}

/** `withDurableHarness`'s work on a resolved config (exported for tests). */
export function applyDurableHarness(config: NextConfig, phase: Phase, projectDir: string, options: DurableHarnessOptions = {}): NextConfig {
  const installed = HARNESSES.filter((h) => resolves(projectDir, h.probe));
  config.serverExternalPackages = unique([
    ...(config.serverExternalPackages ?? []),
    'durable-harness',
    ...installed.flatMap((h) => h.external).filter((name) => resolves(projectDir, name)),
    // libfx (fx agents) loads a native addon. Next's trace finds the addon
    // and libfx's durable bundles by their `new URL()`s.
    ...(resolves(projectDir, 'libfx') ? ['libfx'] : []),
  ]);
  const writes = phase === 'phase-development-server' || phase === 'phase-production-build';
  const plan = appPlan(projectDir, options.agents ? { agents: options.agents } : {});
  if (!plan) return config;
  excludeLocalWorld(config, projectDir);
  if (writes) {
    for (const file of plan.fxUnresolved) {
      warnOnce(`fx-unresolved:${file}`, `[durable-harness] ${nameOf(projectDir, file)} makes an fx agent, but libfx does not resolve from it, so withDurableHarness writes no fx route for it. Install libfx in this app.`);
    }
  }
  const drives = unique([plan.flowRoute, ...(options.driveRoutes ?? plan.sources.filter((s) => s.route && s.chat).map((s) => s.route!))]);

  // The harnesses' run-time files for the machine that builds (Vercel builds
  // where its Functions run): OpenCode's tree-sitter grammars, which
  // Turbopack's trace finds and webpack's does not.
  const files = unique(harnessFiles(projectDir, hostTarget()).flatMap((h) => h.files)).map((file) => tracePath(projectDir, file));
  if (files.length > 0) {
    const includes: Record<string, string[]> = { ...(config.outputFileTracingIncludes ?? {}) };
    for (const route of drives) includes[globOf(route)] = unique([...(includes[globOf(route)] ?? []), ...files]);
    config.outputFileTracingIncludes = includes;
  }

  if (writes) checkPiOpener(projectDir, plan);
  if (writes && options.flowRoute !== false) {
    if (plan.fxModules.length > 0 && plan.fx.length > 0) checkFxAgents(projectDir, plan);
    if (plan.layout === 'workflow') writeWorkflowLayout(projectDir, plan);
    else writeOwnLayout(projectDir, plan);
  }
  return config;
}

/**
 * No Workflow SDK in the app: the flow route at the Workflow SDK's path, its
 * triggers in `config.json` (no `vercel.json`), libfx's wakes through it too.
 * Removes the routes an earlier layout wrote at durable-harness's own path.
 */
function writeWorkflowLayout(projectDir: string, plan: AppPlan): void {
  for (const route of [FLOW_ROUTE, FX_ROUTE] as const) writeRoute(plan.app, route, undefined);
  removeIfEmpty(join(plan.app, '.well-known', 'durable-harness', 'v1'));
  removeIfEmpty(join(plan.app, '.well-known', 'durable-harness'), ['.gitignore']);
  const consumers = { harness: plan.agents.length > 0, fx: plan.fx.length > 0 };
  const wrote = writeRoute(plan.app, WORKFLOW_FLOW_ROUTE, consumers.harness || consumers.fx ? flowRouteText(routeDir(plan.app, WORKFLOW_FLOW_ROUTE), WORKFLOW_FLOW_ROUTE, plan.agents, plan.fx, plan.fxByName) : undefined);
  const dir = dirname(routeDir(plan.app, WORKFLOW_FLOW_ROUTE));
  if (wrote === 'written') {
    writeIfChanged(join(dir, 'config.json'), configText(consumers));
    writeIfChanged(join(dir, '.gitignore'), '*\n');
  } else if (wrote === 'none') {
    removeOurConfig(dir);
  }
  if (plan.fx.length > 0) warnOwnWakeRoutes(projectDir, plan, WORKFLOW_FLOW_ROUTE);
}

/**
 * The app uses the Workflow SDK, which owns `/.well-known/workflow/v1/*`:
 * durable-harness's routes at its own path, a `functions` entry for each on
 * Vercel. Removes the route (and `config.json`) an earlier layout wrote at the
 * Workflow SDK's path, never a file of the Workflow SDK's.
 */
function writeOwnLayout(projectDir: string, plan: AppPlan): void {
  removeWorkflowPathRoute(plan.app);
  const flow = writeRoute(plan.app, FLOW_ROUTE, plan.agents.length > 0 ? flowRouteText(routeDir(plan.app, FLOW_ROUTE), FLOW_ROUTE, plan.agents, []) : undefined);
  const fx = writeRoute(plan.app, FX_ROUTE, plan.fx.length > 0 ? fxRouteText(routeDir(plan.app, FX_ROUTE), plan.fx, plan.fxByName) : undefined);
  if (flow === 'own' && plan.agents.length > 0) {
    warnOnce(`flow-own:${plan.app}`, `[durable-harness] This app has its own route at ${FLOW_ROUTE}, so withDurableHarness writes none there. It must mount flowHandler() from durable-harness (with every module that makes an agent imported), or durable-harness's queue reaches no agent.`);
  }
  if (fx === 'own' && plan.fx.length > 0) {
    warnOnce(
      `fx-no-route:${plan.fx[0]!.file}`,
      `[durable-harness] This app has its own route at ${FX_ROUTE}, so withDurableHarness writes no fx route for the fx agent in ${plan.fx.map((m) => m.name).join(', ')}: that route must mount \`export const POST = agent.wakeHandler()\`.`
    );
  } else if (fx === 'written') {
    warnOwnWakeRoutes(projectDir, plan, FX_ROUTE);
  }
  checkTriggers(projectDir, plan, { flow: flow !== 'none', fx: fx !== 'none' });
}

/** A wake route of the app's own that nothing subscribes, next to the route withDurableHarness writes for libfx's wakes. */
function warnOwnWakeRoutes(projectDir: string, plan: AppPlan, route: GeneratedRoute): void {
  for (const own of plan.sources.filter((s) => s.route && s.wake)) {
    warnOnce(
      `fx-unsubscribed:${own.file}`,
      `[durable-harness] ${nameOf(projectDir, own.file)} mounts wakeHandler(), but no vercel.json or vercel.ts subscribes a function to libfx's topic (${LIBFX_TOPIC}), so the queue delivers nothing there. The route withDurableHarness writes (${route}) takes libfx's wakes; that route can go.`
    );
  }
}

/**
 * The fx agents the fx route can reach. With a libfx that routes by name
 * (`plan.fxByName`), each session's turns run on the agent that created it,
 * so several agents need a name each: two without one are both "default",
 * copies of one agent. Before that, libfx queues every fx agent's wakes on
 * one topic and the message does not say whose session it is, so the agent
 * that consumes a wake runs it with its own model and tools: one fx agent
 * per app. Either way the route reaches its agents through their modules.
 */
function checkFxAgents(projectDir: string, plan: AppPlan): void {
  const modules = plan.fxByName ? plan.fx.map((m) => m.file) : plan.fxModules;
  if (plan.fxByName) {
    const names = modules.flatMap((file) => fxAgentNames(readText(file)).map((name) => ({ file, name })));
    const unnamed = names.filter((n) => n.name === null);
    const seen = new Map<string, string>();
    const twice = names.filter((n) => typeof n.name === 'string' && (seen.has(n.name) ? true : (seen.set(n.name, n.file), false)));
    if (names.length > 1 && (unnamed.length > 0 || twice.length > 0)) {
      const where = unique(names.map((n) => nameOf(projectDir, n.file))).join(', ');
      const what = [
        ...(unnamed.length > 0 ? [`${unnamed.length === 1 ? 'one has' : `${unnamed.length} have`} no name (${unique(unnamed.map((n) => nameOf(projectDir, n.file))).join(', ')})`] : []),
        ...twice.map((n) => `"${n.name}" is used twice (${unique([seen.get(n.name!)!, n.file].map((f) => nameOf(projectDir, f))).join(', ')})`),
      ].join(', and ');
      warnOnce(
        `fx-names:${where}`,
        `[durable-harness] This app makes ${names.length} fx agents (${where}), and ${what}. libfx runs each session's turns on the agent with the name it was created with, and an agent without a name is "default": two agents with one name are copies of one agent. Give each createFxAgent() a name of its own: createFxAgent({ name: 'support', ... }).`
      );
    }
  } else {
    const made = modules.map((file) => ({ file, calls: fxCalls(readText(file)) }));
    const count = made.reduce((n, m) => n + Math.max(1, m.calls), 0);
    if (count > 1) {
      const where = made.map((m) => (m.calls > 1 ? `${nameOf(projectDir, m.file)} (${m.calls})` : nameOf(projectDir, m.file))).join(', ');
      warnOnce(
        `fx-several:${where}`,
        `[durable-harness] This app makes ${count} fx agents (${where}). This libfx queues every fx agent's wakes on one topic, and a wake does not say whose session it is: each runs on the agent the fx route mounts, the first export (by name) with a wakeHandler() in ${nameOf(projectDir, modules[0]!)}. Keep one fx agent per app, or choose it with withDurableHarness's \`agents\` option. libfx 0.0.13-dev.1506 and later run each session on its own agent, by name: with one of those, give each agent a name and the route serves them all.`
      );
    }
  }
  const first = modules[0]!;
  if (!exportsFxAgent(readText(first))) {
    warnOnce(
      `fx-export:${first}`,
      `[durable-harness] ${nameOf(projectDir, first)} calls createFxAgent(), but no top-level export there holds the agent, as far as the build can tell. The generated fx route mounts the first export of that module with a wakeHandler(); without one, libfx's wakes fail. Export the agent from that module: \`export const agent = createFxAgent({ ... })\` (any export name, or \`export default\`). If another module makes it through a function of yours (\`export const agent = makeAgent()\`), list that module in withDurableHarness's \`agents\`.`
    );
  }
}

/**
 * Each generated route needs its queue trigger on Vercel: a `functions`
 * entry for its file (the Next.js builder applies `vercel.json` /
 * `vercel.ts` to a route by its source file). Without one, the queue never
 * delivers there and turns never run, so a build on Vercel with the Vercel
 * World fails; anywhere else (`next dev`, a server on world-local or
 * world-postgres, which deliver in process or over HTTP) it is a warning.
 * Read from `vercel.json`'s `functions`, or a `vercel.ts` that calls
 * `harnessFunctions(` or names the topic. `DURABLE_HARNESS_SKIP_TRIGGER_CHECK=1`
 * skips it (a config this cannot read).
 */
function checkTriggers(projectDir: string, plan: AppPlan, routes: { flow: boolean; fx: boolean }): void {
  if (process.env.DURABLE_HARNESS_SKIP_TRIGGER_CHECK === '1') return;
  const missing: string[] = [];
  if (routes.flow && !triggerConfigured(projectDir, routeKey(projectDir, plan.app, FLOW_ROUTE), QUEUE_TOPIC)) missing.push(routeKey(projectDir, plan.app, FLOW_ROUTE));
  if (routes.fx && !triggerConfigured(projectDir, routeKey(projectDir, plan.app, FX_ROUTE), LIBFX_TOPIC)) missing.push(routeKey(projectDir, plan.app, FX_ROUTE));
  if (missing.length === 0) return;
  const entries = Object.fromEntries(Object.entries(nextFunctions(projectDir, plan)).filter(([key]) => missing.includes(key)));
  const onVercel = !!process.env.VERCEL && isVercelWorldTarget(resolveWorkflowTargetWorld());
  const message = [
    `[durable-harness] This app uses the Workflow SDK (${plan.workflowSdk}), which owns /.well-known/workflow/v1/*, so durable-harness's queue is consumed at its own route, and ${missing.join(' and ')} ${missing.length === 1 ? 'has' : 'have'} no queue trigger in vercel.json or vercel.ts. On Vercel the queue delivers to a route only when its Function subscribes to the topic, so without it no turn would run there.${onVercel ? '' : ' (Locally nothing is needed: the World delivers in process. This matters on your next Vercel deploy.)'}`,
    'Write it with `npx durable-harness vercel-json --write`, or add to vercel.json:',
    JSON.stringify({ functions: entries }, null, 2),
    'In vercel.ts: `functions: harnessFunctions()` from durable-harness/vercel. DURABLE_HARNESS_SKIP_TRIGGER_CHECK=1 skips this check.',
  ].join('\n');
  if (onVercel) throw new Error(message);
  warnOnce(`trigger:${projectDir}:${missing.join(',')}`, message);
}

/**
 * pi-durable sessions opened on the World (`openWorldStorage`,
 * `acquireHarness`, `chatRoute` from `durable-harness/pi-durable`) need an
 * opener registered with `resumeWith(` in a module the flow route imports:
 * without one, the backstop's wakes reach nothing, and a session a process
 * left unfinished is never finished. A build on Vercel fails; anywhere else it
 * is a warning. `DURABLE_HARNESS_SKIP_TRIGGER_CHECK=1` skips it (an opener
 * registered in a way the build cannot read).
 */
function checkPiOpener(projectDir: string, plan: AppPlan): void {
  if (plan.piWithoutOpener.length === 0 || process.env.DURABLE_HARNESS_SKIP_TRIGGER_CHECK === '1') return;
  const where = plan.piWithoutOpener.map((file) => nameOf(projectDir, file)).join(', ');
  const message = [
    `[durable-harness] ${where} ${plan.piWithoutOpener.length === 1 ? 'opens' : 'open'} pi-durable sessions on the World, but no module of this app calls resumeWith() from durable-harness/pi-durable, so a session a process left unfinished (a crash, a closed Harness, a timeout) would never be finished.`,
    'Register your opener once, in a module of its own (the flow route imports every module that calls resumeWith):',
    '  // lib/pi.ts',
    "  export const open = resumeWith((storage) => Harness.open(storage, { models, registry }, ctx))",
    'and open sessions with it. DURABLE_HARNESS_SKIP_TRIGGER_CHECK=1 skips this check.',
  ].join('\n');
  if (process.env.VERCEL && isVercelWorldTarget(resolveWorkflowTargetWorld())) throw new Error(message);
  warnOnce(`pi-opener:${projectDir}:${where}`, message);
}

function warnOnce(key: string, message: string): void {
  const g = globalThis as { [WARNED]?: Set<string> };
  const seen = (g[WARNED] ??= new Set());
  if (seen.has(key)) return;
  seen.add(key);
  console.warn(message);
}
const WARNED = Symbol.for('durable-harness//warned-next');

// ---------- harnesses ----------

interface HarnessSupport {
  /** Installed when this resolves from the project. */
  probe: string;
  /** Kept out of the bundle when installed. */
  external: string[];
}

const HARNESSES: HarnessSupport[] = [
  // The Agent SDK's query() runs in the Function (the claude CLI in the session's sandbox); Codex has no package in a Function.
  { probe: '@anthropic-ai/claude-agent-sdk', external: ['@anthropic-ai/claude-agent-sdk'] },
  { probe: '@opencode/server/package.json', external: ['@opencode/client', '@opencode/core', '@opencode/plugin', '@opencode/schema', '@opencode/server'] },
  { probe: '@earendil-works/pi-ai', external: ['@earendil-works/pi-ai', '@earendil-works/pi-agent-core', '@earendil-works/pi-durable', '@earendil-works/chord'] },
  { probe: 'just-bash', external: ['just-bash'] },
  { probe: '@workflow/world-local', external: ['@workflow/world-local', '@workflow/world-vercel', '@workflow/world-postgres'] },
];

/** A path for `outputFileTracingIncludes`: relative to the project, `/`-separated. */
function tracePath(projectDir: string, file: string): string {
  const rel = relative(projectDir, file).split(sep).join('/');
  return rel.startsWith('.') ? rel : `./${rel}`;
}

/** An `outputFileTracingIncludes` key for a route path (dynamic segments are literal there, not globs). */
const globOf = (route: string) => route.replace(/[[\]]/g, (c) => `\\${c}`);

// ---------- the routes ----------


/**
 * Write a generated route (`text`), or remove the one we wrote when there is
 * nothing for it to serve (`text` undefined). Never over a route the app
 * owns there. `own`: the app's file is there; `written`: ours is; `none`:
 * no route.
 */
function writeRoute(app: string, route: GeneratedRoute, text: string | undefined): 'written' | 'own' | 'none' {
  const dir = routeDir(app, route);
  const existing = ROUTE_EXTENSIONS.map((ext) => join(dir, `route.${ext}`)).filter((f) => existsSync(f));
  const ours = existing.filter((f) => readText(f).includes(MARK));
  if (existing.length > ours.length) return 'own';
  if (text === undefined) {
    for (const f of ours) rmSync(f, { force: true });
    removeIfEmpty(dir);
    return 'none';
  }
  mkdirSync(dir, { recursive: true });
  // durable-harness's own directory is all generated; the Workflow SDK's path gets its .gitignore with config.json.
  if (route !== WORKFLOW_FLOW_ROUTE) writeIfChanged(join(app, '.well-known', 'durable-harness', '.gitignore'), '*\n');
  writeIfChanged(join(dir, 'route.js'), text);
  return 'written';
}

/** `config.json` for the flow route at the Workflow SDK's path. */
const configText = (consumers: { harness: boolean; fx: boolean }) => `${JSON.stringify(flowFunctionConfig(consumers), null, 2)}\n`;

/**
 * The `config.json` texts durable-harness writes (or wrote, before it had a
 * queue namespace of its own: `__wkf_workflow_*`) next to its flow route at
 * the Workflow SDK's path, each exactly: only these are ever removed.
 */
const OUR_CONFIGS = new Set([
  ...[
    [{ type: 'queue/v2beta', topic: '__wkf_workflow_*', retryAfterSeconds: 5, initialDelaySeconds: 0, consumer: 'default' }],
    [{ type: 'queue/v2beta', topic: '__libfx_wkf_workflow_session', consumer: 'default' }],
    [
      { type: 'queue/v2beta', topic: '__wkf_workflow_*', retryAfterSeconds: 5, initialDelaySeconds: 0, consumer: 'default' },
      { type: 'queue/v2beta', topic: '__libfx_wkf_workflow_session', consumer: 'default' },
    ],
  ].map((triggers) => `${JSON.stringify({ version: '0', workflows: { maxDuration: 'max', experimentalTriggers: triggers } }, null, 2)}\n`),
  configText({ harness: true, fx: false }),
  configText({ harness: false, fx: true }),
  configText({ harness: true, fx: true }),
]);

/** Remove `config.json` (and `.gitignore`) at the Workflow SDK's path when they are ours and no route is left there. */
function removeOurConfig(dir: string): void {
  if (!existsSync(dir)) return;
  const rest = safeList(dir).filter((name) => name !== 'config.json' && name !== '.gitignore');
  if (rest.length > 0) return;
  if (OUR_CONFIGS.has(readText(join(dir, 'config.json')))) rmSync(join(dir, 'config.json'), { force: true });
  if (!existsSync(join(dir, 'config.json')) && readText(join(dir, '.gitignore')) === '*\n') rmSync(join(dir, '.gitignore'), { force: true });
  removeIfEmpty(dir);
  removeIfEmpty(dirname(dir));
}

/**
 * Remove the flow route durable-harness wrote at the Workflow SDK's path
 * (`<app>/.well-known/workflow/v1/flow/route.js`, with our mark), and its
 * `config.json` and `.gitignore` when they are ours and nothing else is
 * there. Never a file of the Workflow SDK's (`@workflow/next` writes that
 * directory too) or of the app's.
 */
function removeWorkflowPathRoute(app: string): void {
  const flowDir = routeDir(app, WORKFLOW_FLOW_ROUTE);
  for (const ext of ROUTE_EXTENSIONS) {
    const file = join(flowDir, `route.${ext}`);
    if (readText(file).includes(MARK)) rmSync(file, { force: true });
  }
  removeIfEmpty(flowDir);
  removeOurConfig(dirname(flowDir));
}

function safeList(dir: string): string[] {
  try {
    return readdirSync(dir);
  } catch {
    return [];
  }
}

/** Remove `dir` when it holds nothing but `ignoring` (which go with it). */
function removeIfEmpty(dir: string, ignoring: string[] = []): void {
  if (!existsSync(dir)) return;
  const rest = safeList(dir);
  if (rest.some((name) => !ignoring.includes(name))) return;
  for (const name of rest) rmSync(join(dir, name), { force: true });
  if (safeList(dir).length === 0) {
    try {
      rmdirSync(dir);
    } catch {}
  }
}

/** A module specifier for `file`, from the route's directory. */
function specifier(routeDir: string, file: string): string {
  const rel = relative(routeDir, file).split(sep).join('/').replace(/\.(?:[cm]?[jt]sx?)$/, '');
  return JSON.stringify(rel.startsWith('.') ? rel : `./${rel}`);
}

/** The routes' answer to the Workflow tools' health probe (`?__health`, as the Workflow SDK's routes answer it), and to any other GET or HEAD. */
const HEALTH = [
  "/** The Workflow tools' health probe (`HEAD ?__health`), as the Workflow SDK's routes answer it. */",
  'function health(request) {',
  '  const url = new URL(request.url);',
  "  if (!url.searchParams.has('__health')) return new Response(null, { status: 405, headers: { Allow: 'POST' } });",
  "  return Response.json({ healthy: true, endpoint: url.pathname, consumer: 'durable-harness' });",
  '}',
  '',
];

/**
 * The flow route: imports every module that makes an agent (each registers
 * itself), then mounts them all; at the Workflow SDK's path with an fx agent,
 * hands libfx's wakes to its `wakeHandler()` too.
 */
function flowRouteText(routeDir: string, route: typeof FLOW_ROUTE | typeof WORKFLOW_FLOW_ROUTE, agents: string[], fx: FxModule[], byName = false): string {
  const ids = fx.length === 1 ? ['fx'] : fx.map((_, i) => `fx${i}`);
  const header =
    agents.length === 0
      ? [`// ${MARK} (withDurableHarness). Vercel Queues delivers libfx's wakes`, `// (${LIBFX_TOPIC}) here, to the fx agent's wakeHandler(); it is`, '// written again on every build and dev start.']
      : fx.length === 0
        ? [`// ${MARK} (withDurableHarness). The World's queue delivers`, "// durable-harness's messages (__harness_wkf_workflow_*) here; it is written", '// again on every build and dev start.']
        : [
            `// ${MARK} (withDurableHarness). The World's queue delivers`,
            "// durable-harness's messages (__harness_wkf_workflow_*) here, and Vercel",
            `// Queues libfx's wakes (${LIBFX_TOPIC}); each delivery goes to its consumer`,
            '// by topic. It is written again on every build and dev start.',
          ];
  const imports = [...agents.map((file) => `import ${specifier(routeDir, file)};`), ...fx.map((m, i) => `import * as ${ids[i]} from ${specifier(routeDir, m.file)};`)];
  const fxWakeCall = `fxWake([${ids.join(', ')}], ${JSON.stringify(fx.map((m) => m.name))})`;
  // By name: libfx's own wakeHandler() runs each turn on the agent its session
  // belongs to, among every agent the imports made (a libfx without one at
  // run time gets the first agent's, as before).
  const wakeCall = byName ? `typeof libfx.wakeHandler === 'function' ? libfx.wakeHandler() : ${fxWakeCall}` : fxWakeCall;
  const handler =
    agents.length === 0
      ? [`const wake = ${wakeCall};`, '', 'export function POST(request) {', "  return new URL(request.url).searchParams.has('__health') ? health(request) : wake(request);", '}']
      : fx.length === 0
        ? [`const flow = flowHandler({ route: ${JSON.stringify(route)} });`, '', 'export function POST(request) {', "  return new URL(request.url).searchParams.has('__health') ? health(request) : flow(request);", '}']
        : [
            `const flow = flowHandler({ route: ${JSON.stringify(route)} });`,
            `const wake = ${wakeCall};`,
            '',
            'export function POST(request) {',
            "  if (new URL(request.url).searchParams.has('__health')) return health(request);",
            "  // Vercel Queues names a delivery's topic in ce-vqsqueuename; a World that",
            '  // delivers over HTTP itself (world-local, world-postgres) in x-vqs-queue-name.',
            "  const topic = request.headers.get('ce-vqsqueuename') ?? request.headers.get('x-vqs-queue-name') ?? '';",
            `  return topic.startsWith(${JSON.stringify(LIBFX_QUEUE_PREFIX)}) ? wake(request) : flow(request);`,
            '}',
          ];
  return [
    ...header,
    ...imports,
    ...(agents.length > 0 ? ["import { flowHandler } from 'durable-harness';"] : []),
    ...(byName && fx.length > 0 ? ["import * as libfx from 'libfx';"] : []),
    '',
    ...handler,
    'export const GET = health;',
    'export const HEAD = health;',
    '',
    ...HEALTH,
    ...(fx.length > 0 ? FX_WAKE : []),
  ].join('\n');
}

/** The fx agent, the first export (by name) with a wakeHandler() in the first of its modules that has one, and its wake handler. */
const FX_WAKE = [
  '/** The fx agent, the first export (by name) with a wakeHandler() in the first of its modules that has one, and its wake handler. */',
  'function fxWake(modules, files) {',
  '  for (const module of modules) {',
  "    const agent = Object.values(module).find((value) => typeof value?.wakeHandler === 'function');",
  '    if (agent) return agent.wakeHandler();',
  '  }',
  "  const message = `[durable-harness] ${files.join(', ')} ${files.length === 1 ? 'exports' : 'export'} no fx agent, so libfx's wakes cannot reach it: export it from its module (export const agent = createFxAgent({ ... })).`;",
  '  return async () => {',
  '    console.error(message);',
  '    return new Response(message, { status: 500 });',
  '  };',
  '}',
  '',
];

/** The fx route (with the Workflow SDK): the app's fx agent's `wakeHandler()`, found among its module's exports. */
function fxRouteText(routeDir: string, fx: FxModule[], byName: boolean): string {
  return flowRouteText(routeDir, FLOW_ROUTE, [], fx, byName);
}

function writeIfChanged(file: string, text: string): void {
  try {
    if (readFileSync(file, 'utf8') === text) return;
  } catch {}
  writeFileSync(file, text);
}

const unique = <T>(xs: T[]): T[] => [...new Set(xs)];

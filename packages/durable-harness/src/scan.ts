import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, sep } from 'node:path';
import { harnessModule } from './discovery.ts';
import { packageDir, resolves } from './native.ts';
import { FLOW_MAX_DURATION, FLOW_ROUTE, flowTrigger, FX_ROUTE, fxTrigger, type GeneratedRoute, LIBFX_QUEUE_PREFIX, WORKFLOW_FLOW_ROUTE } from './routing.ts';

/**
 * What a Next.js app makes, read from its source: the agents
 * `withDurableHarness` mounts, and the routes it writes for them. Shared by
 * `durable-harness/next` (which writes the routes) and `durable-harness/vercel`
 * (which writes their `functions` entries), so both see the same app. No
 * `next` import: `vercel.ts` and the CLI load it too.
 */

/** The app directory: `app`, or `src/app`. */
export function appDir(projectDir: string): string | undefined {
  for (const dir of ['app', join('src', 'app')]) {
    const full = join(projectDir, dir);
    if (existsSync(full) && statSync(full).isDirectory()) return full;
  }
  return undefined;
}

export interface Source {
  file: string;
  /** It exports an agent made at module scope, or registers a pi-durable opener with `resumeWith(` there. */
  agent: boolean;
  /**
   * It opens pi-durable sessions on the World (`openWorldStorage(`,
   * `acquireHarness(` or `chatRoute(` from `durable-harness/pi-durable`):
   * their wakes need an opener registered with `resumeWith(` somewhere.
   */
  piSessions?: boolean;
  /** It imports `createFxAgent` (from libfx, or a module re-exporting it) and calls it (it makes an fx agent). */
  fx: boolean;
  /** Its Next route path, when it is a route file. */
  route?: string;
  /** It calls `chatRoute(`. */
  chat: boolean;
  /** It calls `wakeHandler(` (a wake route of the app's own, for libfx's queue). */
  wake: boolean;
}

/**
 * A module the fx route looks for the app's fx agent in (one; several only
 * when the build cannot read which makes it): its path, and its name relative
 * to the project (for messages).
 */
export interface FxModule {
  file: string;
  name: string;
}

/**
 * Where the flow route goes. `workflow`: at the Workflow SDK's flow path, its
 * trigger (and libfx's, for an fx agent) in `config.json` next to it, which
 * Vercel's Next.js builder applies: no `vercel.json`. `own`: the app uses the
 * Workflow SDK, which owns that path, so durable-harness's routes are at
 * `/.well-known/durable-harness/v1/flow` and `/v1/fx`, and on Vercel each
 * needs a `functions` entry for its trigger.
 */
export type Layout = 'workflow' | 'own';

/** What `withDurableHarness` serves in an app, and where. */
export interface AppPlan {
  /** The app directory. */
  app: string;
  /** Where the routes go (see `Layout`). */
  layout: Layout;
  /** Why the routes are at durable-harness's own path (`own`): what shows the app uses the Workflow SDK. */
  workflowSdk?: string;
  /** The flow route's path for this layout. */
  flowRoute: typeof FLOW_ROUTE | typeof WORKFLOW_FLOW_ROUTE;
  /** The source files that make agents or serve chat or wake routes. */
  sources: Source[];
  /** The modules that make durable-harness agents: the flow route imports each. Empty: no flow route. */
  agents: string[];
  /**
   * Modules that open pi-durable sessions on the World while no module of the
   * app calls `resumeWith(`: the backstop's wakes would reach no opener, so a
   * session a dead process left would never be finished.
   */
  piWithoutOpener: string[];
  /** The modules that make fx agents and resolve libfx, the one the fx route mounts first. */
  fxModules: string[];
  /**
   * The installed libfx runs each session's turns on the agent that created
   * it, by name (0.0.13-dev.1506 and later, vercel-labs/fx#1254): the fx route
   * imports every module that exports an fx agent and mounts libfx's own
   * `wakeHandler()`. Before that, the route mounts one agent's handler.
   */
  fxByName: boolean;
  /** Modules that make an fx agent but do not resolve libfx (no fx route for them). */
  fxUnresolved: string[];
  /** The modules the fx route looks in. Empty: no fx route (no fx agent, or the app subscribes a consumer of its own). */
  fx: FxModule[];
  /** A `vercel.json` / `vercel.ts` subscribes a function of the app's own to libfx's topic. */
  ownFxConsumer: boolean;
}

export interface PlanOptions {
  /** `withDurableHarness`'s `agents`: the modules that make agents, relative to the project. */
  agents?: string[];
}

/** The plan for a Next.js app, or undefined when the project has no app directory. */
export function appPlan(projectDir: string, options: PlanOptions = {}): AppPlan | undefined {
  const app = appDir(projectDir);
  if (!app) return undefined;
  const sources = scan(projectDir, app);
  const listed = options.agents?.map((file) => join(projectDir, file)).map((file) => ({ file, ...listedKind(readText(file)) }));
  // A listed module is a durable-harness agent's unless it only makes an fx agent.
  const agents = listed ? listed.filter((m) => m.harness || !m.fx).map((m) => m.file) : sources.filter((s) => s.agent).map((s) => s.file);
  // As a Function finds it: in a node_modules up from the module (not NODE_PATH).
  const withLibfx = (file: string) => packageDir(dirname(file), 'libfx') !== undefined;
  // The fx agent's module: the listed one, in the order given, or the first
  // found that exports its agent (a script that makes one for itself does not).
  const found = listed ? listed.filter((m) => m.fx).map((m) => m.file) : byExport(sources.filter((s) => s.fx).map((s) => s.file).sort());
  const fxModules = found.filter(withLibfx);
  const fxUnresolved = found.filter((file) => !withLibfx(file));
  const fxByName = fxModules.length > 0 && routesByName(packageVersion(dirname(fxModules[0]!), 'libfx'));
  // Listed modules the build cannot read as either kind: the route looks for
  // the fx agent among their exports, at run time, when none calls createFxAgent(.
  const unread = fxModules.length > 0 || !listed ? [] : listed.filter((m) => !m.harness && !m.fx && withLibfx(m.file)).map((m) => m.file);
  // An app that subscribes a function of its own to libfx's topic (a route
  // that mounts `wakeHandler()`, and its trigger in vercel.json) keeps that
  // consumer: a second one would deliver every turn twice. A wake route with
  // no trigger gets no deliveries, so it does not count.
  const workflowSdk = workflowSdkIn(projectDir, app);
  const layout: Layout = workflowSdk ? 'own' : 'workflow';
  const flowRoute = layout === 'own' ? FLOW_ROUTE : WORKFLOW_FLOW_ROUTE;
  // The fx agent's wakes come to the flow route (`workflow`) or the fx route (`own`).
  const ownFxConsumer = subscribesOwnFxConsumer(projectDir, routeKey(projectDir, app, layout === 'own' ? FX_ROUTE : WORKFLOW_FLOW_ROUTE));
  // By name, every module that exports its agent (a listed one is the app's
  // choice); a script that makes an agent for itself is never imported.
  const named = listed ? fxModules : fxModules.filter((file) => exportsFxAgent(readText(file)));
  const candidates = ownFxConsumer ? [] : fxModules.length > 0 ? (fxByName && named.length > 0 ? named : fxModules.slice(0, 1)) : unread;
  const resumes = (file: string) => isPiOpenerModule(readText(file));
  const piWithoutOpener = sources.some((s) => s.agent && resumes(s.file)) || agents.some(resumes) ? [] : sources.filter((s) => s.piSessions).map((s) => s.file);
  return {
    app,
    layout,
    ...(workflowSdk ? { workflowSdk } : {}),
    flowRoute,
    sources,
    agents,
    piWithoutOpener,
    fxModules,
    fxByName,
    fxUnresolved,
    fx: candidates.map((file) => ({ file, name: nameOf(projectDir, file) })),
    ownFxConsumer,
  };
}

/**
 * Whether the app uses the Workflow SDK, and what shows it: `workflow` or
 * `@workflow/next` among the project's dependencies, either resolving from
 * the project (`withWorkflow` comes from `workflow/next`), or a route at the
 * Workflow SDK's flow path that durable-harness did not write (the one
 * `@workflow/next` generates, or the app's own). Undefined: it does not.
 */
export function workflowSdkIn(projectDir: string, app: string): string | undefined {
  const pkg = readJson(join(projectDir, 'package.json')) as { dependencies?: Record<string, unknown>; devDependencies?: Record<string, unknown> } | undefined;
  for (const name of ['workflow', '@workflow/next']) {
    if (pkg?.dependencies?.[name] !== undefined || pkg?.devDependencies?.[name] !== undefined) return `${name} is in package.json`;
  }
  for (const request of ['workflow/next', '@workflow/next']) {
    if (resolves(projectDir, request)) return `${request} resolves from the project`;
  }
  const dir = routeDir(app, WORKFLOW_FLOW_ROUTE);
  const own = ROUTE_EXTENSIONS.map((ext) => join(dir, `route.${ext}`)).find((f) => existsSync(f) && !readText(f).includes(MARK));
  if (own) return `${nameOf(projectDir, own)} is not durable-harness's`;
  return undefined;
}

function readJson(file: string): unknown {
  try {
    return JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    return undefined;
  }
}

/** The extensions a route file may have. */
export const ROUTE_EXTENSIONS = ['ts', 'tsx', 'js', 'jsx', 'mjs', 'mts'];

/** Written into a generated route: a route without it is the app's own. */
export const MARK = 'generated by durable-harness/next';

/** A route's directory under the app directory: `<app>/.well-known/durable-harness/v1/flow` for `FLOW_ROUTE`. */
export const routeDir = (app: string, route: GeneratedRoute) => join(app, ...route.split('/').filter(Boolean));

/**
 * A route's `functions` key in `vercel.json` / `vercel.ts`: its file,
 * relative to the project (`app/.well-known/durable-harness/v1/flow/route.js`,
 * or under `src/app`; the app's own file there when it has one). Vercel's
 * Next.js builder matches it against the route's source file.
 */
export function routeKey(projectDir: string, app: string, route: GeneratedRoute): string {
  const dir = routeDir(app, route);
  const own = ROUTE_EXTENSIONS.map((ext) => join(dir, `route.${ext}`)).find((f) => existsSync(f) && !readText(f).includes(MARK));
  return relative(projectDir, own ?? join(dir, 'route.js')).split(sep).join('/');
}

/** A Next.js route's settings in `vercel.json` / `vercel.ts`. */
export interface NextFunctionConfig {
  maxDuration: typeof FLOW_MAX_DURATION;
  experimentalTriggers: (ReturnType<typeof flowTrigger> | ReturnType<typeof fxTrigger>)[];
}

/**
 * The `functions` entries a Next.js app's routes need on Vercel. None when
 * the flow route is at the Workflow SDK's path (`workflow`: its triggers are
 * in `config.json`). With the Workflow SDK (`own`): the flow route's
 * (durable-harness agents; `maxDuration: 'max'`, the trigger on
 * `__harness_wkf_workflow_*`) and the fx route's (an fx agent; the trigger
 * on libfx's topic). No `includeFiles`: `withDurableHarness` traces the
 * harnesses' files into the routes.
 */
export function nextFunctions(projectDir: string, plan: AppPlan): Record<string, NextFunctionConfig> {
  const functions: Record<string, NextFunctionConfig> = {};
  if (plan.layout === 'workflow') return functions;
  if (plan.agents.length > 0) functions[routeKey(projectDir, plan.app, FLOW_ROUTE)] = { maxDuration: FLOW_MAX_DURATION, experimentalTriggers: [flowTrigger()] };
  if (plan.fx.length > 0) functions[routeKey(projectDir, plan.app, FX_ROUTE)] = { maxDuration: FLOW_MAX_DURATION, experimentalTriggers: [fxTrigger()] };
  return functions;
}

/** The files Vercel reads a project's function config from (a queue trigger there names its topic). */
export const VERCEL_CONFIGS = ['vercel.json', 'vercel.ts', 'vercel.mts', 'vercel.js', 'vercel.mjs', 'vercel.cjs'];

/** The `functions` of the project's `vercel.json`, or undefined when there is none (or it does not parse). */
export function vercelJsonFunctions(projectDir: string): Record<string, { experimentalTriggers?: { type?: string; topic?: string }[] }> | undefined {
  const text = readText(join(projectDir, 'vercel.json'));
  if (!text) return undefined;
  try {
    return (JSON.parse(text) as { functions?: Record<string, { experimentalTriggers?: { type?: string; topic?: string }[] }> }).functions ?? {};
  } catch {
    return undefined;
  }
}

/** The `vercel.ts` (or `.mts`, `.js`, ...) the project configures Vercel with, if any: its text. */
export function vercelCode(projectDir: string): string | undefined {
  for (const file of VERCEL_CONFIGS.slice(1)) {
    const text = readText(join(projectDir, file));
    if (text) return text;
  }
  return undefined;
}

/**
 * Whether the project subscribes a Function of its own to libfx's topic: a
 * `vercel.json` function other than the generated fx route with a trigger
 * on libfx's prefix, or a `vercel.ts` that names the prefix itself.
 */
function subscribesOwnFxConsumer(projectDir: string, fxKey: string): boolean {
  const functions = vercelJsonFunctions(projectDir);
  if (functions && Object.entries(functions).some(([key, f]) => key !== fxKey && (f.experimentalTriggers ?? []).some((t) => t.topic?.startsWith(LIBFX_QUEUE_PREFIX)))) return true;
  const ts = vercelCode(projectDir);
  return !!ts && code(ts).includes(LIBFX_QUEUE_PREFIX);
}

/**
 * Whether the project's Vercel config gives a generated route its queue
 * trigger: a `vercel.json` `functions` entry whose key matches the route's
 * file (exactly, or as a glob) with a `queue/v2beta` trigger on `topic`; or
 * a `vercel.ts` that calls `harnessFunctions(` or names the topic.
 */
export function triggerConfigured(projectDir: string, key: string, topic: string): boolean {
  const functions = vercelJsonFunctions(projectDir);
  if (functions && Object.entries(functions).some(([pattern, f]) => matchesKey(pattern, key) && (f.experimentalTriggers ?? []).some((t) => t.type === 'queue/v2beta' && t.topic === topic))) return true;
  const ts = vercelCode(projectDir);
  return !!ts && (/\bharnessFunctions\s*\(/.test(code(ts)) || ts.includes(topic));
}

/** Whether a `functions` key (a path or a glob: `*`, `**`, `?`, `{a,b}`) names `file`. */
function matchesKey(pattern: string, file: string): boolean {
  if (pattern === file) return true;
  if (!/[*?{]/.test(pattern)) return false;
  let re = '';
  let depth = 0;
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i]!;
    if (c === '*' && pattern[i + 1] === '*') {
      re += '.*';
      i++;
      if (pattern[i + 1] === '/') i++;
    } else if (c === '*') re += '[^/]*';
    else if (c === '?') re += '[^/]';
    else if (c === '{') {
      re += '(?:';
      depth++;
    } else if (c === '}' && depth > 0) {
      re += ')';
      depth--;
    } else if (c === ',' && depth > 0) re += '|';
    else re += c.replace(/[.+^$()|[\]\\{}]/g, '\\$&');
  }
  try {
    return new RegExp(`^${re}$`).test(file);
  } catch {
    return false;
  }
}

// ---------- reading modules ----------

const SKIP = new Set(['node_modules', 'public', 'out', 'dist', 'build', 'coverage']);
const SOURCE = /\.(?:[cm]?[jt]sx?)$/;
const ROUTE_FILE = /^route\.(?:[cm]?[jt]sx?)$/;
const LIBFX = String.raw`['"]libfx(?:\/node)?['"]`;
const FX_CALL = /(?<!\bfunction\s*\*?\s*)\bcreateFxAgent\s*(?:<[^()]*>\s*)?\(/;

/** The text with its comments taken out, near enough for the checks below (a `//` after `:` is a URL's). */
export const code = (text: string) => text.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:\\'"`])\/\/.*$/gm, '$1');

/** A module's top-level statements, near enough: each starts a line, as formatted code has it. */
const statementsOf = (text: string) => code(text).split(/\n(?=[A-Za-z_$@])/);

/** How many fx agents a module makes, near enough: its top-level statements that call `createFxAgent(` (a conditional is one). */
export const fxCalls = (text: string) => statementsOf(text).filter((s) => FX_CALL.test(s)).length;

/** The clauses of a module's `import ... from` statements whose specifier matches `spec` (a regex source). */
const importClauses = (src: string, spec: string) =>
  [...src.matchAll(new RegExp(String.raw`\bimport\s+([^'";]*?)\s*\bfrom\s*${spec}`, 'g'))].map((m) => m[1]!.trim());

/** An import clause's braced specifiers (`{ a, type B, c as d }`), each trimmed. */
const specifiers = (clause: string) => /\{([^}]*)\}/.exec(clause)?.[1]!.split(',').map((n) => n.trim()).filter(Boolean) ?? [];

/**
 * Whether a module imports libfx as a value: `import ... from 'libfx'` (or
 * `libfx/node`) that is not `import type` and not only `type` specifiers, or
 * `import('libfx')` / `require('libfx')`.
 */
function importsLibfx(text: string): boolean {
  const src = code(text);
  if (new RegExp(String.raw`\b(?:import|require)\s*\(\s*${LIBFX}`).test(src)) return true;
  return importClauses(src, LIBFX).some((clause) => !/^type\s+\S/.test(clause) && !(/^\{[^}]*\}$/.test(clause) && specifiers(clause).every((n) => /^type\s/.test(n))));
}

/** Whether a module imports `createFxAgent` by that name as a value from any module: libfx, or a module of the app's own that re-exports it. */
const importsCreateFxAgent = (text: string) =>
  importClauses(code(text), String.raw`['"][^'"]+['"]`).some((clause) => !/^type\s+\S/.test(clause) && specifiers(clause).some((n) => /^createFxAgent(?:\s+as\s+createFxAgent)?$/.test(n)));

/** Whether a module makes an fx agent: a value import of libfx or of `createFxAgent`, and a `createFxAgent(` call. */
export const isFxModule = (text: string) => (importsLibfx(text) || importsCreateFxAgent(text)) && fxCalls(text) > 0;

/**
 * What a module listed in `agents` makes, as far as the build can tell: a
 * durable-harness agent, an fx agent (it calls `createFxAgent(`, wherever it
 * imports it from), both, or neither (an agent made through a function of
 * the app's own).
 */
const listedKind = (text: string) => {
  const module = harnessModule(text);
  return { harness: module.agentFactory || module.piOpener, fx: fxCalls(text) > 0 };
};

/**
 * Whether a module registers a pi-durable opener: a value import of
 * `durable-harness/pi-durable` and a `resumeWith(` call. A module that only
 * opens storages there (a route) is not one: the flow route never imports it.
 */
export const isPiOpenerModule = (text: string) => harnessModule(text).piOpener;

/** Whether a module opens pi-durable sessions on the World: a value import of `durable-harness/pi-durable` and a call that opens one. */
export const opensPiSessions = (text: string) => harnessModule(text).piSessions;

/**
 * Whether a module exports a durable-harness agent made at module scope, or
 * registers a pi-durable opener (`resumeWith(`) there.
 */
export const isHarnessModule = (text: string) => harnessModule(text).agent;

/** A function, not a value made now: an initializer an export would hold. */
const FUNCTION_VALUE = /^(?:async\s+)?(?:function\b|class\b|\([^)]*\)\s*(?::[^=]*)?=>|[\w$]+\s*=>)/;

/**
 * Whether a module exports the fx agent it makes, at its top level: an
 * `export const x = ...` or `export default ...` whose statement calls
 * `createFxAgent(` (`export const agent = createFxAgent(...)`, `= globalThis.fx
 * ??= createFxAgent(...)`, `= wrap(createFxAgent(...))`), not a function that
 * makes one; or a top-level `const` like that it exports by name (`export {
 * agent }`) or as its default. Near enough: each top-level statement starts
 * a line, as formatted code has it.
 */
export function exportsFxAgent(text: string): boolean {
  const statements = statementsOf(text);
  const calls = (s: string) => FX_CALL.test(s);
  const made = new Set<string>();
  for (const statement of statements) {
    const exported = /^export\s+(?:default\s+(?!(?:async\s+)?(?:function|class)\b)|(?:const|let|var)\s+[\w$]+\s*(?::[^=]+)?=\s*(?!\s))/.exec(statement);
    if (exported && calls(statement) && !FUNCTION_VALUE.test(statement.slice(exported[0].length).trimStart())) return true;
    const local = /^(?:const|let|var)\s+([\w$]+)\s*(?::[^=]+)?=\s*/.exec(statement);
    if (local && calls(statement) && !FUNCTION_VALUE.test(statement.slice(local[0].length))) made.add(local[1]!);
  }
  for (const name of made) {
    const id = name.replace(/\$/g, '\\$');
    if (statements.some((s) => new RegExp(String.raw`^export\s+default\s+${id}\s*;?\s*$`).test(s.trim()))) return true;
    if (statements.some((s) => new RegExp(String.raw`^export\s*\{(?:[^}]*[\s,])?${id}(?:\s+as\s+[\w$]+)?\s*[,}]`).test(s))) return true;
  }
  return false;
}

/**
 * Whether a libfx version runs each session's turns on its agent by name:
 * 0.0.13-dev.1506 or a later dev build, or 0.0.14 and up. (libfx 0.0.13
 * itself has no durable sessions.)
 */
export function routesByName(version: string | undefined): boolean {
  const m = /^0\.0\.(\d+)(?:-dev\.(\d+))?/.exec(version ?? '');
  if (!m) return false;
  const patch = Number(m[1]);
  return patch > 13 || (patch === 13 && m[2] !== undefined && Number(m[2]) >= 1506);
}

/** The version of the package `name` that resolves from `dir`, or undefined. */
function packageVersion(dir: string, name: string): string | undefined {
  const root = packageDir(dir, name);
  const pkg = root ? (readJson(join(root, 'package.json')) as { version?: unknown } | undefined) : undefined;
  return typeof pkg?.version === 'string' ? pkg.version : undefined;
}

/**
 * The `name` each top-level `createFxAgent(` call in a module gives its
 * agent, near enough: a string literal, `null` when the call names none
 * (libfx names it "default"), or `undefined` when the build can't tell (a
 * name from a variable, options from elsewhere).
 */
export function fxAgentNames(text: string): (string | null | undefined)[] {
  return statementsOf(text)
    .filter((s) => FX_CALL.test(s))
    .map((s) => {
      const args = callArgs(s, s.search(FX_CALL));
      if (args === undefined || !/^\s*(?:\{|$)/.test(args)) return undefined;
      if (/^\s*\{\s*\.\.\./.test(args)) return undefined;
      const named = /(?:^|[{,]\s*)name\s*:\s*(['"`])([^'"`$]*)\1/.exec(args);
      if (named) return named[2]!;
      return /(?:^|[{,]\s*)name\b/.test(args) || /\.\.\./.test(args) ? undefined : null;
    });
}

/** The text inside the parentheses of the call that starts at `at`, or undefined when they don't close. */
function callArgs(text: string, at: number): string | undefined {
  const open = text.indexOf('(', at);
  let depth = 0;
  for (let i = open; i < text.length; i++) {
    if (text[i] === '(') depth++;
    else if (text[i] === ')' && --depth === 0) return text.slice(open + 1, i);
  }
  return undefined;
}

/** Modules that export their fx agent first, each group in the order given. */
function byExport(modules: string[]): string[] {
  const exporting = modules.filter((file) => exportsFxAgent(readText(file)));
  return [...exporting, ...modules.filter((file) => !exporting.includes(file))];
}

export function readText(file: string): string {
  try {
    return readFileSync(file, 'utf8');
  } catch {
    return '';
  }
}

/** A file as messages name it: relative to the project. */
export const nameOf = (projectDir: string, file: string) => relative(projectDir, file).split(sep).join('/');

/** The project's source files that make agents or serve chat routes (dot directories, node_modules and build output skipped). */
function scan(projectDir: string, app: string): Source[] {
  const out: Source[] = [];
  const walk = (dir: string) => {
    let entries: import('node:fs').Dirent[];
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (entry.name.startsWith('.') || SKIP.has(entry.name)) continue;
      const full = join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && SOURCE.test(entry.name) && !/\.(?:test|spec|d)\.[cm]?[jt]sx?$/.test(entry.name)) {
        let text: string;
        try {
          if (statSync(full).size > 1_000_000) continue;
          text = readFileSync(full, 'utf8');
        } catch {
          continue;
        }
        const inApp = full.startsWith(app + sep);
        const route = inApp && ROUTE_FILE.test(entry.name) ? routeOf(app, full) : undefined;
        const { agent, piSessions } = harnessModule(text);
        const fx = isFxModule(text);
        const chat = /\bchatRoute\s*\(/.test(text);
        const wake = /\bwakeHandler\s*\(/.test(code(text));
        if (agent || fx || piSessions || (route && (chat || wake))) out.push({ file: full, agent, fx, chat, wake, ...(piSessions ? { piSessions } : {}), ...(route ? { route } : {}) });
      }
    }
  };
  walk(projectDir);
  return out;
}

/** `/api/chat` for `<app>/api/chat/route.ts` (route groups dropped). */
function routeOf(app: string, file: string): string {
  const segments = relative(app, dirname(file)).split(sep).filter((s) => s && !(s.startsWith('(') && s.endsWith(')')));
  return `/${segments.join('/')}`;
}

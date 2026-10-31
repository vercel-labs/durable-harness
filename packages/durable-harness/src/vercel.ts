import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { isBuiltin } from 'node:module';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { harnessFiles, includeGlob, packageDir, type Target, VERCEL_TARGET } from './native.ts';
import { FLOW_MAX_DURATION, flowTrigger, type fxTrigger, QUEUE_HOLD_FILE, vercelTriggerConsumerGroup } from './routing.ts';
import { appPlan, nextFunctions, writeQueueHold } from './scan.ts';

/**
 * durable-harness/vercel: the `functions` config durable-harness's queue
 * needs on Vercel, for `vercel.ts` or `vercel.json`.
 *
 * ```ts
 * // vercel.ts (`VercelConfig` is @vercel/config/v1's)
 * export const config: VercelConfig = {
 *   functions: harnessFunctions(),
 * }
 * ```
 *
 * or, for `vercel.json`: `npx durable-harness vercel-json --write`.
 *
 * In a Next.js app (an `app/` or `src/app/` directory and no `api/flow.ts`):
 *
 * - without the Workflow SDK: nothing (`{}`). `withDurableHarness` writes the
 *   flow route at the Workflow SDK's path with its trigger in `config.json`,
 *   which Vercel's Next.js builder applies.
 * - with the Workflow SDK (`workflow` or `@workflow/next` installed, or a
 *   route of its own at `/.well-known/workflow/v1/flow`): the routes
 *   `withDurableHarness` writes at durable-harness's own path, each with
 *   `maxDuration: 'max'` and its queue trigger: the flow route
 *   (`app/.well-known/durable-harness/v1/flow/route.js`, on
 *   `__harness_wkf_workflow_*`) when the app makes durable-harness agents,
 *   and the fx route (`.../v1/fx/route.js`, on libfx's
 *   `__libfx_wkf_workflow_session`) for an fx agent. `withDurableHarness`
 *   traces OpenCode's files, so no `includeFiles`.
 *
 * On Vercel Functions without Next.js (an `api/` directory):
 *
 * - The flow function (`api/flow.ts`, the queue's consumer: it imports every
 *   module that makes an agent, then `export const POST = flowHandler()`):
 *   `maxDuration: 'max'` and the queue trigger on `__harness_wkf_workflow_*`
 *   (without a `consumer`, which Vercel derives for a function and refuses in
 *   `vercel.json`).
 * - The flow function and every chat function: `includeFiles` for the
 *   files an installed harness loads at run time that the file tracer does
 *   not find (OpenCode's tree-sitter grammars), in an npm, yarn or pnpm
 *   `node_modules`. The flow function drives every agent, so it gets every
 *   harness's; a chat function gets those of the harnesses its imports reach
 *   (`durable-harness/opencode`), or every one when the scan can't tell.
 *   Claude Code and Codex need none: their CLIs run in the session's Vercel
 *   Sandbox, from the binaries its base image carries. In `vercel.ts` this
 *   runs on the machine that builds, after the install, so the paths it
 *   found installed are there too.
 */

/** A Vercel Function's settings, as `vercel.ts` (`@vercel/config`'s `FunctionConfig`) and `vercel.json` take them. */
export interface HarnessFunctionConfig {
  maxDuration?: number | 'max';
  /** One glob (`{a,b}` for several). */
  includeFiles?: string;
  experimentalTriggers?: (ReturnType<typeof flowTrigger> | ReturnType<typeof fxTrigger>)[];
}

export interface HarnessFunctionsOptions {
  /**
   * The flow function's file, on Vercel Functions without Next.js. Default
   * `api/flow.ts` (or `.js`, `.mjs`); without one, a Next.js app's routes.
   */
  flow?: string;
  /** In a Next.js app: `withDurableHarness`'s `agents`, when you pass it there. */
  agents?: string[];
  /**
   * The functions that serve `agent.chatRoute()` (each drives its turns in
   * its own invocation, so each needs the harness's files too). Default:
   * every file under `api/` that calls `chatRoute(`.
   */
  chat?: string | string[];
  /** More globs to include in every one of these functions. */
  includeFiles?: string[];
  /**
   * The project (its Root Directory, where `vercel.ts` is). Default: the
   * directory of the `vercel.ts` the Vercel CLI is loading, else the working
   * directory. (The CLI loads `vercel.ts` in a child that keeps its own
   * working directory, which in a monorepo is the repository's root.)
   */
  projectDir?: string;
  /** The machine the Functions run on. Default Vercel's (linux-x64, glibc). */
  target?: Target;
}

/** The `functions` entries for the flow function and the chat functions. */
export function harnessFunctions(options: HarnessFunctionsOptions = {}): Record<string, HarnessFunctionConfig> {
  return functionsFor(options, true);
}

/**
 * `harnessFunctions`. `record`: at build (`vercel.ts`), record the flow
 * function's consumer group for the chat functions' held wakes and trace it
 * into them; not for a `vercel.json` written ahead of time, where nothing
 * runs at build to record it (held wakes then need
 * `DURABLE_HARNESS_QUEUE_HOLD_GROUP`, see queue-hold.ts).
 */
function functionsFor(options: HarnessFunctionsOptions, record: boolean): Record<string, HarnessFunctionConfig> {
  const projectDir = options.projectDir ?? defaultProjectDir();
  // A file that is not there would fail the deploy later, and less clearly
  // (or, in the wrong project directory, leave every function without its files).
  const flow = options.flow ?? FLOW_FILES.find((f) => existsSync(join(projectDir, f)));
  if (!flow) {
    // A Next.js app: the routes withDurableHarness writes.
    const plan = appPlan(projectDir, options.agents ? { agents: options.agents } : {});
    if (plan) {
      // Without the Workflow SDK the route's triggers are in config.json: nothing here.
      if (plan.layout === 'workflow') return {};
      const functions = nextFunctions(projectDir, plan);
      if (Object.keys(functions).length === 0) {
        throw new Error(`durable-harness/vercel: the Next.js app in ${projectDir} makes no agent that withDurableHarness can find (a module that imports a harness subpath of durable-harness, calls durableAgent(, or makes an fx agent); pass agents, or projectDir if this is not the project)`);
      }
      return functions;
    }
  }
  if (!flow || !existsSync(join(projectDir, flow))) {
    throw new Error(`durable-harness/vercel: no flow function ${flow ?? 'api/flow.ts'} and no Next.js app directory in ${projectDir} (the file that exports flowHandler(); pass flow, or projectDir if this is not the project)`);
  }
  const chat = options.chat === undefined ? chatFunctions(projectDir) : [options.chat].flat();
  for (const file of chat) {
    if (!existsSync(join(projectDir, file))) throw new Error(`durable-harness/vercel: no chat function ${file} in ${projectDir}`);
  }
  const installed = harnessFiles(projectDir, options.target ?? VERCEL_TARGET);
  // The consumer group Vercel gives the flow function's trigger (named
  // after this `functions` key), for the chat functions' held wakes: recorded
  // here, at build, and traced into each chat function (queue-hold.ts).
  if (record) writeQueueHold(projectDir, vercelTriggerConsumerGroup(flow));
  const files = (only?: Set<string>, chatFunction = false) => {
    const include = checked(includeGlob(projectDir, installed.filter((h) => !only || only.has(h.harness)), [...(options.includeFiles ?? []), ...(chatFunction && record ? [QUEUE_HOLD_FILE] : [])]));
    return include ? { includeFiles: include } : {};
  };
  const functions: Record<string, HarnessFunctionConfig> = {
    [flow]: { maxDuration: FLOW_MAX_DURATION, experimentalTriggers: [flowTrigger()], ...files() },
  };
  for (const file of chat) {
    if (file === flow) continue;
    // Vercel refuses an entry with no settings: a chat function that needs no files (Pi's, outside a build) gets none.
    const config = files(harnessesOf(projectDir, file), true);
    if (Object.keys(config).length > 0) functions[file] = config;
  }
  return functions;
}

/**
 * The `includeFiles` glob for a function that drives sessions of every
 * installed harness (the flow function): their run-time files (OpenCode's
 * grammars), plus `includeFiles`. Undefined when there is nothing to include
 * (Claude Code, Codex, Pi and fx need nothing).
 */
export function harnessIncludes(options: Pick<HarnessFunctionsOptions, 'includeFiles' | 'projectDir' | 'target'> = {}): string | undefined {
  const projectDir = options.projectDir ?? defaultProjectDir();
  return checked(includeGlob(projectDir, harnessFiles(projectDir, options.target ?? VERCEL_TARGET), options.includeFiles));
}

/**
 * The project whose `vercel.ts` is being loaded. The Vercel CLI (`vercel
 * build`, `deploy`, `dev`, and the build on Vercel) bundles `vercel.ts` to
 * `<project>/.vercel/vercel-temp.mjs` and imports it from
 * `<project>/.vercel/vercel-loader.mjs` in a child process with the CLI's
 * working directory, which is the repository's root when the project is a
 * Root Directory in it (a monorepo). Elsewhere (`vercel-json`, a script): the
 * working directory.
 */
export function defaultProjectDir(argv: readonly string[] = process.argv, cwd: string = process.cwd()): string {
  for (const arg of argv.slice(1, 3)) {
    if (arg && /[\\/]\.vercel[\\/]vercel-(?:loader|temp)\.mjs$/.test(arg)) return dirname(dirname(resolve(cwd, arg)));
  }
  return cwd;
}

/** Vercel takes one `includeFiles` pattern of at most this many characters. */
export const INCLUDE_FILES_MAX = 256;

function checked(glob: string | undefined): string | undefined {
  if (glob && glob.length > INCLUDE_FILES_MAX) {
    throw new Error(`durable-harness/vercel: the includeFiles glob is ${glob.length} characters, and Vercel takes ${INCLUDE_FILES_MAX} at most: ${glob}`);
  }
  return glob;
}

const FLOW_FILES = ['api/flow.ts', 'api/flow.js', 'api/flow.mjs'];

/** The harness subpaths, and the harness whose run-time files each needs (OpenCode's grammars; Claude Code and Codex run their CLIs in the session's sandbox, Pi needs none). */
const SUBPATHS: Record<string, string | undefined> = { 'claude-code': undefined, codex: undefined, opencode: 'opencode', pi: undefined, 'pi-durable': undefined, fx: undefined, sandbox: undefined };
const IMPORT = /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s+|\brequire\s*\(\s*)['"]([^'"]+)['"]/g;
const EXTENSIONS = ['.ts', '.mts', '.cts', '.tsx', '.js', '.mjs', '.cjs', '.jsx'];

/** `import type …` / `export type …`: no module at run time. */
const TYPE_ONLY = /\b(?:import|export)\s+type\b[^'";]*?\bfrom\s*['"][^'"]+['"]/g;
/** A file that is not code (JSON, styles, …): nothing to follow. */
const ASSET = /\.(?:json|css|scss|sass|less|svg|png|jpe?g|gif|txt|md|html|wasm|node)$/;
/** An `import()` / `require()` of something other than a string literal. */
const DYNAMIC = /\b(?:import|require)\s*\(\s*[^'"\s)]/;

/**
 * The harnesses a function's agents are made with: the harness subpaths of
 * durable-harness its imports reach (relative ones, and the project's
 * tsconfig `paths`). Undefined (every harness) when the scan cannot tell:
 * they reach none, a harness of the app's own (`durableAgent`), or an
 * import it does not follow (an alias it cannot resolve, a workspace
 * package, a computed `import()`), which could make an agent of any harness.
 */
function harnessesOf(projectDir: string, file: string): Set<string> | undefined {
  const seen = new Set<string>();
  const found = new Set<string>();
  const paths = tsconfigPaths(projectDir);
  let known = false;
  const queue = [resolve(projectDir, file)];
  while (queue.length > 0) {
    const path = queue.shift()!;
    if (seen.has(path)) continue;
    if (seen.size >= 500) return undefined;
    seen.add(path);
    let text: string;
    try {
      text = readFileSync(path, 'utf8').replace(TYPE_ONLY, '');
    } catch {
      return undefined;
    }
    if (DYNAMIC.test(text)) return undefined;
    for (const [, spec] of text.matchAll(IMPORT)) {
      const sub = /^durable-harness\/([\w-]+)$/.exec(spec!)?.[1];
      if (sub !== undefined && sub in SUBPATHS) {
        known = true;
        if (SUBPATHS[sub]) found.add(SUBPATHS[sub]!);
      } else if (spec === 'durable-harness' || spec!.startsWith('durable-harness/')) {
        if (/\bdurableAgent\s*\(/.test(text)) return undefined;
      } else if (spec!.startsWith('.')) {
        if (ASSET.test(spec!)) continue;
        const next = sourceFile(dirname(path), spec!);
        if (!next) return undefined;
        queue.push(next);
      } else if (!isBuiltin(spec!)) {
        const aliased = paths(spec!);
        if (aliased) queue.push(aliased);
        // A package installed from the registry makes no agent of the app's;
        // anything else (an alias, a workspace package) might.
        else if (!fromRegistry(dirname(path), spec!)) return undefined;
      }
    }
  }
  return known ? found : undefined;
}

/** Whether `spec` is a package in a `node_modules` (not a link to a workspace package). */
function fromRegistry(dir: string, spec: string): boolean {
  if (spec.startsWith('#')) return false;
  const name = spec.split('/').slice(0, spec.startsWith('@') ? 2 : 1).join('/');
  const real = packageDir(dir, name);
  return !!real && real.split(sep).includes('node_modules');
}

/**
 * The project's tsconfig.json `compilerOptions.paths` (`"@/*": ["./*"]`),
 * as a resolver to a source file. Nothing when there is none (or it is not
 * plain JSON, or it `extends` another for them).
 */
function tsconfigPaths(projectDir: string): (spec: string) => string | undefined {
  let options: { baseUrl?: string; paths?: Record<string, string[]> } = {};
  try {
    options = (JSON.parse(readFileSync(join(projectDir, 'tsconfig.json'), 'utf8')) as { compilerOptions?: typeof options }).compilerOptions ?? {};
  } catch {}
  const base = resolve(projectDir, options.baseUrl ?? '.');
  const entries = Object.entries(options.paths ?? {});
  /** What `*` stands for when `spec` matches `pattern` (`''` for a pattern without one). */
  const matchOf = (pattern: string, spec: string): string | undefined => {
    const star = pattern.indexOf('*');
    if (star < 0) return spec === pattern ? '' : undefined;
    const [head, tail] = [pattern.slice(0, star), pattern.slice(star + 1)];
    return spec.length >= head.length + tail.length && spec.startsWith(head) && spec.endsWith(tail) ? spec.slice(head.length, spec.length - tail.length) : undefined;
  };
  return (spec) => {
    for (const [pattern, targets] of entries) {
      const rest = matchOf(pattern, spec);
      if (rest === undefined) continue;
      for (const target of targets) {
        const file = sourceFile(base, target.replace('*', rest));
        if (file) return file;
      }
    }
    return undefined;
  };
}

/** The source file a relative import names (`./agent.js` is `./agent.ts` in TypeScript). */
function sourceFile(dir: string, spec: string): string | undefined {
  const base = resolve(dir, spec);
  const stem = base.replace(/\.(?:[cm]?js|jsx)$/, '');
  const candidates = [base, ...EXTENSIONS.map((e) => stem + e), ...EXTENSIONS.map((e) => join(base, `index${e}`))];
  return candidates.find((c) => {
    try {
      return statSync(c).isFile();
    } catch {
      return false;
    }
  });
}

/** `{ functions }` for `vercel.json` (what `npx durable-harness vercel-json` prints). */
export function vercelJson(options: HarnessFunctionsOptions = {}): { functions: Record<string, HarnessFunctionConfig> } {
  return { functions: functionsFor(options, false) };
}

/** What Vercel makes a Node.js Function of under `api/` (not `.mts`, `.cjs`, …, nor a `.d.ts`). */
const FUNCTION = /\.(?:m?js|tsx?)$/;

/**
 * Every Function under `api/` that calls `chatRoute(`, project-relative.
 * Vercel makes none of a path with a segment that starts with `_` or `.`
 * (`api/_lib/…`): a `functions` entry for one fails the deploy.
 */
function chatFunctions(projectDir: string): string[] {
  const out: string[] = [];
  const walk = (dir: string) => {
    let names: string[];
    try {
      names = readdirSync(dir);
    } catch {
      return;
    }
    for (const name of names) {
      if (name.startsWith('.') || name.startsWith('_') || name === 'node_modules') continue;
      const full = join(dir, name);
      let st: ReturnType<typeof statSync>;
      try {
        st = statSync(full);
      } catch {
        continue;
      }
      if (st.isDirectory()) walk(full);
      else if (FUNCTION.test(name) && !/\.(?:test|spec|d)\.[cm]?[jt]sx?$/.test(name) && st.size < 1_000_000 && /\bchatRoute\s*\(/.test(readFileSync(full, 'utf8'))) {
        out.push(relative(projectDir, full).split(sep).join('/'));
      }
    }
  };
  const api = join(projectDir, 'api');
  if (existsSync(api)) walk(api);
  return out.sort();
}

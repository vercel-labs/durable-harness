#!/usr/bin/env node
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { vercelJson } from './vercel.ts';

/**
 * `durable-harness vercel-json`: the `functions` config durable-harness/vercel
 * makes, as JSON for `vercel.json` (an app without a `vercel.ts`): a Next.js
 * app's generated routes and their queue triggers, or, on Vercel Functions
 * without Next.js, the flow function's and the chat functions'.
 *
 *   npx durable-harness vercel-json [--flow api/flow.ts] [--chat api/chat.ts]... [--include <glob>]... [--agent <module>]... [--write]
 *
 * Prints `{ "functions": … }`; `--write` merges it into ./vercel.json (its
 * other settings, functions and function settings stay). Run it again after
 * adding a harness.
 *
 * `durable-harness sandbox prepare [--region <region>]`: build the project's
 * base snapshot for `sandbox()` ahead of the first session
 * (`prepareSandboxBase`, durable-harness/sandbox), with `vercel link`'s
 * credentials on a laptop or the deployment's. Prints the snapshot id.
 */

const USAGE = `usage: durable-harness vercel-json [--flow api/flow.ts] [--chat api/chat.ts]... [--include <glob>]... [--agent <module>]... [--write]
       durable-harness sandbox prepare [--region <region>]

sandbox prepare builds the project's base snapshot for sandbox() (the relay,
the pinned Claude Code and Codex CLIs) ahead of the first session, with
vercel link's credentials. --region: default VERCEL_REGION when Vercel Sandbox
offers it, else iad1 (where a session's Drive goes).

Prints the functions config for vercel.json that durable-harness's queue needs
on Vercel. --write merges it into ./vercel.json.

Next.js (an app/ or src/app/ directory, no api/flow.ts): nothing, unless the
app also uses the Workflow SDK (withWorkflow). Then the routes
withDurableHarness writes at its own path,
app/.well-known/durable-harness/v1/flow/route.js (the queue trigger on
__harness_wkf_workflow_*) and, for an fx agent, .../v1/fx/route.js (libfx's
topic), each with maxDuration 'max'. --agent: withDurableHarness's agents
option, if you pass it.

Vercel Functions without Next.js: the flow function's maxDuration and queue
trigger, and the run-time files an installed harness needs (OpenCode's
grammars, as includeFiles) for the flow function and every chat function
(default: every file under api/ that calls chatRoute()).

In vercel.ts, call harnessFunctions() from durable-harness/vercel instead.`;

/** `sandbox prepare [--region <region>]`. */
async function sandboxCommand(argv: string[]): Promise<number> {
  const [sub, ...rest] = argv;
  if (sub === '--help' || sub === '-h') {
    console.log(USAGE);
    return 0;
  }
  if (sub !== 'prepare') {
    console.error(USAGE);
    return 1;
  }
  let region: string | undefined;
  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i]!;
    if (arg === '--region' && rest[i + 1]) region = rest[++i];
    else if (arg === '--help' || arg === '-h') {
      console.log(USAGE);
      return 0;
    } else throw new Error(`unknown option ${arg}\n\n${USAGE}`);
  }
  const { prepareSandboxBase } = await import('./sandbox.ts');
  const { snapshotId, built } = await prepareSandboxBase(region ? { region } : {});
  console.log(`${built ? 'built' : 'already there'}: ${snapshotId}`);
  return 0;
}

function main(argv: string[]): number | Promise<number> {
  const [command, ...rest] = argv;
  if (command === '--help' || command === '-h') {
    console.log(USAGE);
    return 0;
  }
  if (command === 'sandbox') return sandboxCommand(rest);
  if (command !== 'vercel-json') {
    console.error(USAGE);
    return command === undefined ? 0 : 1;
  }
  const chat: string[] = [];
  const include: string[] = [];
  const agents: string[] = [];
  let flow: string | undefined;
  let write = false;
  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i]!;
    const value = () => {
      const v = rest[++i];
      if (!v) throw new Error(`${arg} needs a value`);
      return v;
    };
    if (arg === '--chat') chat.push(value());
    else if (arg === '--flow') flow = value();
    else if (arg === '--include') include.push(value());
    else if (arg === '--agent') agents.push(value());
    else if (arg === '--write') write = true;
    else if (arg === '--help' || arg === '-h') {
      console.log(USAGE);
      return 0;
    } else throw new Error(`unknown option ${arg}\n\n${USAGE}`);
  }
  const config = vercelJson({ ...(flow ? { flow } : {}), ...(chat.length ? { chat } : {}), ...(include.length ? { includeFiles: include } : {}), ...(agents.length ? { agents } : {}) });
  if (Object.keys(config.functions).length === 0) {
    console.error(
      "Nothing to add: this is a Next.js app without the Workflow SDK, so withDurableHarness writes the flow route at /.well-known/workflow/v1/flow with its queue trigger in config.json next to it, which Vercel's Next.js builder applies. No vercel.json entry is needed."
    );
    if (write) return 0;
  }
  if (!write) {
    console.log(JSON.stringify(config, null, 2));
    return 0;
  }
  for (const other of ['vercel.ts', 'vercel.mts', 'vercel.js', 'vercel.mjs']) {
    if (existsSync(other)) throw new Error(`${other} is the config here: call harnessFunctions() from durable-harness/vercel in it instead of writing vercel.json`);
  }
  const file = join(process.cwd(), 'vercel.json');
  const current = existsSync(file) ? (JSON.parse(readFileSync(file, 'utf8')) as { functions?: Record<string, object> }) : {};
  // Per function, so its other settings (memory, regions, ...) stay.
  const functions: Record<string, object> = { ...(current.functions ?? {}) };
  for (const [name, settings] of Object.entries(config.functions)) functions[name] = { ...(functions[name] ?? {}), ...settings };
  const next = { ...current, functions };
  writeFileSync(file, `${JSON.stringify(next, null, 2)}\n`);
  console.log(`vercel.json: ${Object.keys(config.functions).join(', ')}`);
  return 0;
}

try {
  const code = main(process.argv.slice(2));
  if (typeof code === 'number') process.exitCode = code;
  else
    code.then(
      (c) => (process.exitCode = c),
      (error: unknown) => {
        console.error(error instanceof Error ? error.message : String(error));
        process.exitCode = 1;
      }
    );
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}

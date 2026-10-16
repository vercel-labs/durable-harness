/**
 * durable-harness/fx: workspace tools for libfx's own durable agent
 * (`createFxAgent`), named and shaped like libfx's kernel built-ins:
 * `read_file`, `glob_files`, `grep_files`, `edit_file`, `write_file` and
 * `shell`, with fx's input schemas byte for byte and fx's descriptions with a
 * few reviewed edits (no paths outside `/workspace`, no image attachments, a
 * shell whose commands complete in one call). They run on a `virtual()`
 * workspace (docs/workspaces.md §3.3, §4.4): just-bash on a virtual
 * filesystem, in a worker thread leased to the session, with an empty
 * environment; or in the session's own Vercel Sandbox (`sandbox()`, §5.6).
 * Commands and paths the model picks never reach your Function's files,
 * processes or environment.
 *
 * ```ts
 * import { createFxAgent } from 'libfx'
 * import { virtual } from 'durable-harness'
 * import { fxTools } from 'durable-harness/fx'
 * export const agent = createFxAgent({ model, tools: [...fxTools(virtual()), ...appTools] })
 * ```
 *
 * libfx advertises no tools of its own, so without this line an agent has
 * only `appTools`. The names are the kernel's on purpose: the kernel routes a
 * call by the tool registered under its name, and these are host tools, so a
 * call never reaches a kernel built-in (one the app leaves out is "not
 * registered", never a fallback).
 *
 * With `virtual()` (durable, the default) every call that may change files
 * records what it changed and what it returned in the session's World run (a
 * stream of the run, small files inline and larger ones in the session's
 * blob stream), keyed by libfx's `executionId`. Every tool is `idempotent:
 * true`: a call a crash cut off before its record runs again on the files as
 * they were before it, and one libfx runs again after its record returns
 * that record's output, so no change lands twice. The changing tools are
 * `writes: true`, so libfx runs them one at a time. With `virtual({ durable:
 * false })` the files are in memory on the server running the session, and
 * the World is never written.
 *
 * With `sandbox()` the files are on the session's Drive at `/workspace` and
 * the commands run in its Linux VM, as an unprivileged user with none of the
 * Function's environment; the agent stays in the Function. The VM is
 * attached at the first call of a worker that runs one (a turn with no call
 * boots none), armed with the epoch of the libfx lease that worker holds,
 * which rises from worker to worker, so a server that was taken over can't
 * run anything there. The Drive keeps what a call wrote even if a crash cut
 * it off, so every tool is `idempotent: false`: libfx stores each call's
 * intent before it runs and never runs a cut-off call again (it tells the
 * model the call may have partly run). A call whose VM is still starting
 * comes back "not run: the sandbox is starting, try again". The World is
 * the one the agent keeps its sessions in, as for `virtual()`.
 */
import type { World } from '@workflow/world';
import { fxAgentTools } from 'harness-world-hands/fx';
import type { FxHostTool } from 'libfx';
import { resolveEnvironment } from './environments.ts';
import type { Workspace } from './workspace.ts';
import { getWorld } from './world.ts';

export interface FxToolsOptions {
  /**
   * The World the agent keeps its sessions in, where a durable workspace
   * records its calls (each session is a run there). Default: the World
   * libfx uses for an agent given no `durability`: Vercel's World on Vercel
   * (`getWorld()`), and elsewhere libfx's local files (`$FX_SESSIONS_DIR`,
   * or `libfx/sessions` in the temporary directory). An agent with a
   * durability of its own (`world(myWorld)` from `libfx/durable-world`)
   * passes the same World here. An agent on `memory()` has no run to record
   * in: its durable calls fail with a `TypeError` saying to use
   * `virtual({ durable: false })`. With `sandbox()` it is where libfx keeps
   * the lease the session's sandbox is armed with: on `memory()` there is
   * none, and each call fails with a `TypeError` saying so.
   */
  world?: World;
}

/**
 * One of `fxTools()`'s tools: a libfx host tool that says whether a call is
 * safe to run twice, as `createFxAgent()` requires of its tools (libfx's
 * `FxAgentHostTool`).
 */
export type FxKernelTool = FxHostTool & { idempotent: boolean };

/**
 * libfx's kernel tools on `workspace` (`virtual()` or `sandbox()`), for
 * `createFxAgent({ tools })`: six libfx host tools, in the order fx
 * advertises them. Spread them into the agent's `tools` array with the
 * app's own. Install `just-bash` for `virtual()`, `@vercel/sandbox` for
 * `sandbox()`.
 */
export function fxTools(workspace: Workspace, options: FxToolsOptions = {}): FxKernelTool[] {
  const environment = resolveEnvironment(workspace);
  const { world } = options;
  const worldOf = world ? () => world : defaultWorld;
  // Each tool carries the workspace-tool brand, so claudeCode() and codex() refuse them.
  if (environment.kind === 'sandbox') {
    // The session factory loads (with `@vercel/sandbox`) on the first acquire.
    return fxAgentTools({ kind: 'sandbox' }, { world: worldOf, acquire: environment.acquire }) as unknown as FxKernelTool[];
  }
  return fxAgentTools(environment.workspace, { isolation: environment.isolation, world: worldOf }) as unknown as FxKernelTool[];
}

const LOCAL = Symbol.for('durable-harness//fx-local-world');

/**
 * The World libfx keeps an agent's sessions in when it is given no
 * durability (libfx's `node.js`, `environmentDurability`): Vercel's on
 * Vercel, its local files elsewhere. The local one is libfx's own World
 * (`libfx/durable-local`) on the same directory, so the two agree on the
 * format of the files.
 */
async function defaultWorld(): Promise<World> {
  if (process.env.VERCEL) return getWorld();
  const g = globalThis as { [LOCAL]?: Promise<World> };
  g[LOCAL] ??= (async () => {
    const [{ local }, { join }, { tmpdir }] = await Promise.all([import('libfx/durable-local'), import('node:path'), import('node:os')]);
    const durability = local({ dir: process.env.FX_SESSIONS_DIR || join(tmpdir(), 'libfx', 'sessions') }) as unknown as { world?: () => World | Promise<World> };
    if (typeof durability.world !== 'function') {
      throw new Error("durable-harness/fx: this libfx's local() has no World to share: pass the agent's World as fxTools(virtual(), { world })");
    }
    return durability.world();
  })().catch((error) => {
    g[LOCAL] = undefined;
    throw error;
  });
  return g[LOCAL];
}

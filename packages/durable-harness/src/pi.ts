import type { Model } from '@earendil-works/pi-ai';
import type { Harness, HostTool } from 'harness-world';
import { isPiCodingAgentTool, kernelSandbox, piCodingTools, sandboxRuntime, workspaceRuntime } from 'harness-world-hands/runtime';
import { type PiHarnessOptions, piHarness as rawPiHarness } from 'harness-world-pi';
import { gatewayCredential } from 'harness-world-process';
import { type Agent, type AgentOptions, kernelAgent, splitOptions } from './agent.ts';
import { resolveEnvironment } from './environments.ts';
import { markSandboxHarness, markSandboxTools, sandboxToolsMark } from './sandbox-harness.ts';
import type { Workspace } from './workspace.ts';

/** pi-ai's provider id for the Vercel AI Gateway. */
export const GATEWAY_PROVIDER = 'vercel-ai-gateway';

export interface PiOptions extends Omit<PiHarnessOptions, 'model' | 'streamFn' | 'getApiKey'>, AgentOptions {
  /**
   * An AI Gateway model id (`anthropic/claude-sonnet-4.5`), served through
   * pi-ai's Gateway provider, or a pi-ai `Model` of your own (then pass
   * `streamFn` too).
   */
  model: string | Model<any>;
  /** Appended as Pi's system prompt (`systemPrompt` is the same thing). */
  instructions?: string;
  /** pi-ai's stream function. Default: the Gateway provider's (`models.streamSimple`). */
  streamFn?: PiHarnessOptions['streamFn'];
  /** The provider credential, per call. Default `AI_GATEWAY_API_KEY`, else the Vercel OIDC token. */
  getApiKey?: PiHarnessOptions['getApiKey'];
  /** The kernel name (its sessions' runs are `workflow//durable-harness@<version>//<name>`). Default `pi`. */
  name?: string;
}

/**
 * Pi without pi-durable: Pi's agent loop (`pi-agent-core`'s `Agent`) on the
 * durable kernel. Every model call and tool call is a step in the session's
 * log, intent before and outcome after; a crash resumes from the log in
 * another process (a `replay: 'safe'` tool reruns, any other comes back to
 * the model as interrupted). Durable On by default.
 *
 * Tool-less unless you give it tools: only `tools` reach the model. Pi's
 * own coding tools (`read`, `bash`, `edit`, `write`) on a workspace are one
 * line, `createCodingTools(virtual())` (or `createCodingTools(sandbox())`,
 * the session's own Vercel Sandbox); pi-coding-agent's own tools are
 * refused (they run on the machine that drives the session).
 *
 * ```ts
 * import { virtual } from 'durable-harness'
 * import { createCodingTools, pi } from 'durable-harness/pi'
 * export const agent = pi({ model: 'anthropic/claude-sonnet-4.5', tools: [...createCodingTools(virtual()), ...appTools] })
 * ```
 *
 * Install `@earendil-works/pi-agent-core` and `@earendil-works/pi-ai`
 * (and `just-bash` for `createCodingTools`).
 */
export function pi(options: PiOptions): Agent {
  refusePiCodingAgentTools(options.tools);
  const [agent, rest] = splitOptions(options);
  const { model, instructions, name = 'pi', ...harnessOptions } = rest;
  let built: Promise<Harness> | undefined;
  // pi-ai loads on the first drive: a route that only reads a session loads none of it.
  const build = async (): Promise<Harness> => {
    let resolved: Model<any>;
    let streamFn = harnessOptions.streamFn;
    if (typeof model === 'string') {
      const [{ createModels }, { vercelAIGatewayProvider }] = await Promise.all([
        import('@earendil-works/pi-ai'),
        import('@earendil-works/pi-ai/providers/vercel-ai-gateway'),
      ]);
      const models = createModels();
      models.setProvider(vercelAIGatewayProvider());
      const found = models.getModel(GATEWAY_PROVIDER, model);
      if (!found) throw new Error(`durable-harness/pi: pi-ai does not know the Gateway model ${model}`);
      resolved = found;
      streamFn ??= models.streamSimple.bind(models);
    } else {
      resolved = model;
      if (!streamFn) throw new Error('durable-harness/pi: pass streamFn with a pi-ai Model of your own');
    }
    return rawPiHarness({
      ...harnessOptions,
      model: resolved,
      streamFn,
      getApiKey: harnessOptions.getApiKey ?? (() => gatewayCredential()),
      ...(instructions !== undefined && harnessOptions.systemPrompt === undefined ? { systemPrompt: instructions } : {}),
    });
  };
  const harness: Harness = {
    name,
    modelCalls: 'self',
    async drive(ctx) {
      return (await (built ??= build().catch((error) => {
        built = undefined;
        throw error;
      }))).drive(ctx);
    },
  };
  return kernelAgent({ ...agent, harness: marked(harness, options.tools), defaultDurability: 'on' });
}

/** A harness given tools of a `sandbox()` workspace boots a VM per session: marked for its public route (sandbox-harness.ts). */
function marked<H extends object>(harness: H, tools: readonly unknown[] | undefined): H {
  const mark = sandboxToolsMark(tools);
  return mark ? markSandboxHarness(harness, mark) : harness;
}

/**
 * Pi without pi-durable, as a raw harness (`durableSessions({ harness })`):
 * the harness `pi()` builds, from a pi-ai `Model` and its stream function.
 * Like `pi()`, it refuses pi-coding-agent's own tools.
 */
export function piHarness(options: PiHarnessOptions): Harness {
  refusePiCodingAgentTools(options.tools);
  return marked(rawPiHarness(options), options.tools);
}

export type { PiHarnessOptions };

/**
 * Pi's coding tools, `read`, `bash`, `edit` and `write`, on `workspace`:
 * the tools pi-coding-agent's `createCodingTools(cwd)` gives Pi, with their
 * names, input schemas and descriptions, the same truncation (the last 2000
 * lines or 50 KB of a command's output, the first of a file) and the same
 * result texts, but on a workspace at `/workspace` instead of the disk, the
 * processes and the environment of the machine that drives the session.
 *
 * - `virtual()`: a virtual workspace. `bash` is just-bash (a bash-compatible
 *   shell with coreutils, grep, sed, awk, jq, and more; no network, no real
 *   binaries such as node or python), in a worker thread leased to the
 *   session. Each call's file changes commit with its outcome in the
 *   session's log, so every tool is `replay: 'safe'`: a crash mid-call
 *   leaves nothing behind and the call runs again, and the files move with
 *   the session to any machine. With durability off (no World) the files
 *   last the turn, as with `virtual({ durable: false })`.
 * - `virtual({ durable: false })`: the same tools, their files in memory,
 *   cleared at the end of every turn (the descriptions tell the model).
 * - `sandbox()`: the session's own Vercel Sandbox: the files on the
 *   session's Drive at `/workspace`, the commands in its Linux VM, as an
 *   unprivileged user with none of the Function's environment. Pi stays in
 *   the Function; the VM is attached on the first call of a turn that runs
 *   one (a turn with no call boots none), with the session's own fencing
 *   token. The Drive keeps what a call wrote even if a crash cut it off, so
 *   `bash`, `edit` and `write` are `replay: 'never'` (a cut-off call comes
 *   back as interrupted, never run again by itself) and `read` is `safe`; a
 *   call whose VM is still starting comes back "not run: the sandbox is
 *   starting, try again". Needs a World with the in-band fence. The tools
 *   carry the sandbox mark, so a public `chatRoute` of the agent given them
 *   needs `allowPublicSandbox: true` and a finite `budget.sessionUsd`.
 *
 * In `pi()`, a command that exits nonzero is an error result, as in Pi
 * (elsewhere, a result that ends with the exit code), and the files it
 * changed stay. `read` reads text files (an image is an error result).
 * Give them to `pi({ tools })` with your own tools, or to any loop that
 * takes HostTools. Claude Code and Codex don't take them: in their sandbox
 * they use their own tools.
 *
 * ```ts
 * import { virtual } from 'durable-harness'
 * import { createCodingTools, pi } from 'durable-harness/pi'
 * export const agent = pi({ model: 'anthropic/claude-sonnet-4.5', tools: [...createCodingTools(virtual()), ...appTools] })
 * ```
 *
 * Install `just-bash` for `virtual()`, `@vercel/sandbox` for `sandbox()`.
 */
export function createCodingTools(workspace: Workspace): HostTool[] {
  const env = resolveEnvironment(workspace);
  // The kernel's attach (harness-world-sandbox/cli) loads on the first attach; the factory and `@vercel/sandbox` with the first acquire.
  // Marked: the agent given them boots a VM per session, so its public route needs allowPublicSandbox (sandbox-harness.ts).
  if (env.kind === 'sandbox') return markSandboxTools(piCodingTools(sandboxRuntime(kernelSandbox(env.acquire, () => import('harness-world-sandbox/cli')))), env.workspace.budget);
  const { workspace: descriptor, isolation, runtime } = env;
  const inMemory = runtime.durable ? workspaceRuntime({ ...descriptor, durable: false }, { isolation }) : undefined;
  return piCodingTools(runtime, inMemory ? { inMemory } : {});
}

/** pi-coding-agent's own tools run on the machine that drives the session: refuse them, naming the replacement. */
function refusePiCodingAgentTools(tools: readonly unknown[] | undefined): void {
  const found = tools?.find(isPiCodingAgentTool) as { name: string } | undefined;
  if (!found) return;
  throw new TypeError(
    `durable-harness/pi: the tool "${found.name}" is pi-coding-agent's own: it reads, writes and runs commands on the machine that drives the session (its bash gets this process's environment). ` +
      "Use createCodingTools(virtual()) from durable-harness/pi: Pi's coding tools on a virtual workspace."
  );
}

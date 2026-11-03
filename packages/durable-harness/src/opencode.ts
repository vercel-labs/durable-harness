import type { Harness } from 'harness-world';
import { OPENCODE_TOOLS, type OpenCodeHarnessOptions as RawOptions, type OpenCodeTool, type OpenCodeWorkspace, opencodeHarness as rawOpencodeHarness } from 'harness-world-opencode';
import { type Agent, type AgentOptions, kernelAgent, splitOptions } from './agent.ts';
import { resolveEnvironment } from './environments.ts';
import { markSandboxHarness, sandboxMarkOf } from './sandbox-harness.ts';
import type { SandboxWorkspace, VirtualWorkspace } from './workspace.ts';

export { OPENCODE_TOOLS, type OpenCodeTool };
export { DEFAULT_INSTRUCTIONS } from 'harness-world-opencode';

/** The raw harness's test seams (a scripted host, a stand-in for the machine's files): not options of durable-harness's OpenCode. */
type TestSeam = 'connect' | 'hostHas';
const TEST_SEAMS: readonly TestSeam[] = ['connect', 'hostHas'];

export interface OpenCodeHarnessOptions extends Omit<RawOptions, 'workspace' | 'builtins' | TestSeam> {
  /**
   * Where OpenCode's own tools run: `virtual()` (a virtual workspace; its
   * files in the session's log, or with `virtual({ durable: false })` in
   * memory for one turn) or `sandbox()` (the session's own Vercel Sandbox:
   * its files on the session's Drive at `/workspace`, its commands in the
   * session's VM, attached on the first call of a turn that runs one). With
   * one, the session has OpenCode's own shell, read, write, edit, patch,
   * glob and grep, running OpenCode's own code on the workspace's files and
   * shell, never on the machine that drives the session. Default none: only
   * `tools`. With `sandbox()`, a public `chatRoute` needs
   * `allowPublicSandbox: true` and a finite `budget.sessionUsd`.
   */
  workspace?: VirtualWorkspace | SandboxWorkspace;
  /**
   * Narrow OpenCode's own tools (`OPENCODE_TOOLS`): default all seven when
   * `workspace` is set (`true` says so). Without a `workspace` it is a
   * TypeError.
   */
  builtins?: true | readonly OpenCodeTool[];
}

export interface OpenCodeOptions extends OpenCodeHarnessOptions, AgentOptions {
  /** An AI Gateway model id, e.g. `anthropic/claude-haiku-4.5`. */
  model: string;
}

/**
 * OpenCode v2 as a durable agent: OpenCode's embedded server in the
 * Function's process, rebuilt from the session's log on every drive. Its
 * committed events are mirrored into the log and every model request waits
 * until the log has them. `model` is an AI Gateway id
 * (`anthropic/claude-haiku-4.5`); the credential is `AI_GATEWAY_API_KEY`,
 * else the Vercel OIDC token. Every other option is the harness's, passed
 * through as is.
 *
 * Tool-less by default: the app's `tools` are the only ones the model is
 * offered, and OpenCode starts no process and reads no file of the machine
 * that drives the session for the model. One line gives it its own tools:
 * `workspace: virtual()` runs OpenCode's own shell, read, write, edit,
 * patch, glob and grep (their names, schemas and descriptions) on a virtual
 * workspace, each call in the log before it runs and its file changes
 * committed with its result; `workspace: sandbox()` runs them in the
 * session's own Vercel Sandbox (OpenCode's core stays in the Function), its
 * files on the session's Drive; `builtins` narrows them.
 *
 * ```ts
 * import { virtual } from 'durable-harness'
 * import { opencode } from 'durable-harness/opencode'
 * export const agent = opencode({ model: 'anthropic/claude-haiku-4.5', workspace: virtual(), tools: appTools })
 * ```
 *
 * Install OpenCode's packages at the versions this package pins:
 * `@opencode/client @opencode/core @opencode/schema @opencode/server effect`,
 * and `just-bash` for `virtual()` (`@vercel/sandbox` for `sandbox()`).
 */
export function opencode(options: OpenCodeOptions): Agent {
  const [agent, harness] = splitOptions(options);
  return kernelAgent({ ...agent, harness: opencodeHarness(harness) });
}

/**
 * The OpenCode harness itself, for `durableSessions({ harness })`: the same
 * options as `opencode()` but the agent's.
 */
export function opencodeHarness(options: OpenCodeHarnessOptions = {}): Harness {
  const { workspace, ...rest } = options;
  // OpenCode runs in the drive's process, on its workspace: nothing starts it elsewhere or looks at this machine's files for it.
  const seam = TEST_SEAMS.find((key) => key in rest);
  if (seam) throw new TypeError(`durable-harness/opencode: \`${seam}\` is not an option: OpenCode runs in the drive's process, its own tools only on the session's workspace`);
  let raw: Harness;
  let environment: ReturnType<typeof resolveEnvironment> | undefined;
  try {
    // A copy of a descriptor, or anything else, is a TypeError.
    environment = workspace !== undefined ? resolveEnvironment(workspace) : undefined;
    raw = rawOpencodeHarness({ ...rest, ...(environment ? { workspace: resolved(environment) } : {}) });
  } catch (err) {
    if (err instanceof TypeError) throw new TypeError(err.message.replace(/^harness-world-opencode:/, 'durable-harness/opencode:'));
    throw err;
  }
  // On sandbox() each session boots a VM and creates a Drive: marked, so a public route needs allowPublicSandbox (sandbox-harness.ts).
  return environment?.kind === 'sandbox' ? markSandboxHarness(raw, sandboxMarkOf(environment.workspace)) : raw;
}

/** A resolved descriptor, as the harness takes it. */
function resolved(environment: ReturnType<typeof resolveEnvironment>): OpenCodeWorkspace {
  return environment.kind === 'virtual' ? { kind: 'virtual', runtime: environment.runtime } : { kind: 'sandbox', acquire: environment.acquire };
}

import type { Harness } from 'harness-world';
import { type CodexHarnessOptions as RawCodexHarnessOptions, codexHarness as rawCodexHarness } from 'harness-world-codex';
import { isWorkspaceTool } from 'harness-world-hands/runtime';
import { type Agent, type AgentOptions, kernelAgent, splitOptions } from './agent.ts';
import { resolveEnvironment } from './environments.ts';
import { markSandboxHarness, sandboxMarkOf } from './sandbox-harness.ts';
import { isWorkspace, sandbox, type SandboxWorkspace } from './workspace.ts';

export interface CodexHarnessOptions extends Omit<RawCodexHarnessOptions, 'machine' | 'test'> {
  /**
   * The session's Vercel Sandbox: a VM per session with a Drive at
   * `/workspace`, where `codex app-server` runs (docs/workspaces.md §5.6).
   * Default `sandbox()`. Codex runs nowhere else: `virtual()` is a
   * `TypeError`.
   */
  workspace?: SandboxWorkspace;
}

export type CodexOptions = CodexHarnessOptions & AgentOptions;

/**
 * Codex's kernel harness on the session's Vercel Sandbox: `workspace`
 * (default `sandbox()`) resolved into the sandbox tier's acquire, and the
 * descriptor's `env`, `network`, `budget` and `commandTimeoutMs` applied. For
 * `durableSessions({ harness: codexHarness() })`; `codex()` is the agent.
 */
export function codexHarness(options: CodexHarnessOptions = {}): Harness {
  const { workspace = sandbox(), ...rest } = options;
  if (!isWorkspace(workspace) || workspace.kind !== 'sandbox') {
    throw new TypeError('durable-harness/codex: Codex runs only in a Vercel Sandbox: workspace takes sandbox() (the default), not virtual() or anything else');
  }
  for (const tool of options.tools ?? []) {
    if (isWorkspaceTool(tool)) {
      throw new TypeError(`durable-harness/codex: ${(tool as { name?: string }).name ?? 'a tool'} is a workspace tool: in its sandbox Codex uses its own shell and apply_patch, with builtins: true`);
    }
  }
  for (const key of ['machine', 'test'] as const) {
    if ((options as Record<string, unknown>)[key] !== undefined) throw new TypeError(`durable-harness/codex: ${key} is internal: pass workspace: sandbox()`);
  }
  const { acquire } = resolveEnvironment(workspace);
  return markSandboxHarness(
    rawCodexHarness({
      ...rest,
      machine: { acquire, env: workspace.env, network: workspace.network, budget: workspace.budget, commandTimeoutMs: workspace.commandTimeoutMs },
    }),
    sandboxMarkOf(workspace)
  );
}

/**
 * Codex (`codex app-server`) as a durable agent, in the session's Vercel
 * Sandbox VM (`workspace`, default `sandbox()`): its process, its shell and
 * its files are there, on `/workspace` (the session's Drive), never in the
 * Function that drives the session. Its rollout is mirrored into the
 * session's log and restored on whichever VM runs it next. Tool-less by
 * default: the app's `tools` are its only tools, run in the Function.
 * `builtins: true` turns its shell and `apply_patch` on, in the VM under
 * Codex's own sandbox (`sandboxMode`; by default `workspace-write` when
 * bubblewrap's self-test passes there): each command is approved once its
 * intent is in the log, and never rerun after a crash. `model` is an AI
 * Gateway id (`openai/gpt-5.4-mini`); the credential (`AI_GATEWAY_API_KEY`,
 * else the Vercel OIDC token) stays in the Function, which makes Codex's
 * model calls for it. Every other option is the harness's, passed through as
 * is.
 *
 * ```ts
 * export const agent = codex({ model: 'openai/gpt-5.4-mini' }) // no tools: it answers from the conversation
 *
 * // or with its own shell and apply_patch on the session's Drive:
 * export const coder = codex({ model: 'openai/gpt-5.4-mini', builtins: true })
 * ```
 *
 * Needs `@vercel/sandbox`, and the Vercel CLI's login (`vercel link`) or a
 * Vercel deployment for its credentials.
 */
export function codex(options: CodexOptions): Agent {
  const [agent, harness] = splitOptions(options);
  return kernelAgent({ ...agent, harness: codexHarness({ ...harness, ...(agent.log ? { log: agent.log } : {}) }) });
}

/** The Codex version the sandbox image pins (the binary lives in the VM image, not on this machine). */
export { CODEX_VERSION } from 'harness-world-codex';
